package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Tenant-aware state uses a distinct header so a rolling deployment can read
// v1 event-ID-only state without silently interpreting it as tenant-safe.
// Lines are JSON objects containing both identity components; JSON encoding is
// the durable injective representation, while the in-memory key is length
// framed so neither component can collide with the other.
const (
	replayTenantLogFormat     = "replay-log-tenant-v1"
	replayTenantLogHeaderLine = `{"format":"replay-log-tenant-v1"}`
	replayTenantLogHeader     = replayTenantLogHeaderLine + "\n"
	replayTenantLogPrefix     = `{"format":"replay-log-v2`
)

type tenantReplayMark struct {
	TenantID string `json:"tenant_id,omitempty"`
	EventID  string `json:"event_id"`
}

// initializeReplayMaps also supports same-package tests and older embedders
// that construct ReplayState directly instead of using LoadReplayState.
func (s *ReplayState) initializeReplayMaps() {
	if s.Replayed == nil {
		s.Replayed = map[string]bool{}
	}
	if s.scopedMarks == nil {
		s.scopedMarks = map[string]bool{}
	}
	if s.legacyMarks == nil {
		s.legacyMarks = map[string]bool{}
	}
	// A direct construction has no loader provenance. Treat its old public map
	// as legacy only when no scoped/legacy marks were loaded already. This does
	// not turn a v2 exact mark into an unscoped mark after restart.
	if len(s.scopedMarks) == 0 && len(s.legacyMarks) == 0 {
		for eventID := range s.Replayed {
			s.legacyMarks[eventID] = true
		}
	}
}

// replayIdentityKey is injective for arbitrary tenant and event strings:
// the byte length of tenantID frames the boundary before eventID. It is used
// only in memory; the persisted JSON line retains both fields explicitly.
func replayIdentityKey(tenantID, eventID string) string {
	return strconv.Itoa(len([]byte(tenantID))) + ":" + tenantID + eventID
}

func replayTenantFromIdentity(identity string) (string, bool) {
	colon := strings.IndexByte(identity, ':')
	if colon <= 0 {
		return "", false
	}
	length, err := strconv.Atoi(identity[:colon])
	if err != nil || length < 1 {
		return "", false
	}
	rest := identity[colon+1:]
	if len([]byte(rest)) < length {
		return "", false
	}
	return string([]byte(rest)[:length]), true
}

func replayEventIDFromIdentity(identity string) string {
	tenantID, ok := replayTenantFromIdentity(identity)
	if !ok {
		return ""
	}
	prefix := strconv.Itoa(len([]byte(tenantID))) + ":" + tenantID
	if !strings.HasPrefix(identity, prefix) || len(identity) == len(prefix) {
		return ""
	}
	return identity[len(prefix):]
}

func tenantMarkLine(tenantID, eventID string) ([]byte, error) {
	line, err := json.Marshal(tenantReplayMark{TenantID: tenantID, EventID: eventID})
	if err != nil {
		return nil, fmt.Errorf("marshal tenant replay mark: %w", err)
	}
	return append(line, '\n'), nil
}

// SetLegacyTenantScope opts in to honoring event-ID-only marks from a
// pre-tenant-aware state file for exactly one explicitly proven tenant. With
// no scope, v1 marks remain readable and visible but are never used to close a
// tenant-aware record. The scope is configuration, not a value read from a
// Failure payload.
func (s *ReplayState) SetLegacyTenantScope(tenantID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeReplayMaps()
	s.legacyTenantScope = strings.TrimSpace(tenantID)
}

// Marked reports whether the exact tenant/event identity has a durable replay
// mark. Legacy event-ID-only marks are honored only for the explicit scope set
// by SetLegacyTenantScope; they never suppress another tenant's same ID.
func (s *ReplayState) Marked(tenantID, eventID string) bool {
	if tenantID == "" || eventID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeReplayMaps()
	if s.scopedMarks[replayIdentityKey(tenantID, eventID)] {
		return true
	}
	return s.legacyTenantScope != "" && s.legacyTenantScope == tenantID && s.legacyMarks[eventID]
}

// MarkTenant durably records one verified replay using the canonical tenant
// recovered from the accepted event. It deliberately has no Failure argument:
// an unverified DLQ claim cannot enter replay state. The existing Mark method
// remains the event-ID-only compatibility API for old Kafka-mode embedders.
func (s *ReplayState) MarkTenant(tenantID, eventID string) error {
	if tenantID == "" || eventID == "" {
		return fmt.Errorf("tenant-aware replay mark requires non-empty tenant_id and event_id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeReplayMaps()
	identity := replayIdentityKey(tenantID, eventID)
	if s.path == "" {
		s.scopedMarks[identity] = true
		s.Replayed[eventID] = true // compatibility view only; Marked never trusts it
		return nil
	}
	lockWait := s.lockWait
	if lockWait <= 0 {
		lockWait = defaultLockWait
	}
	lock, err := lockReplayState(s.path, lockWait)
	if err != nil {
		return err
	}
	defer lock.Close()
	line, err := tenantMarkLine(tenantID, eventID)
	if err != nil {
		return err
	}
	kind, err := classifyStateFile(s.path)
	if err != nil {
		return fmt.Errorf("inspect replay state: %w", err)
	}
	if kind == stateTenantLog {
		if err := s.appendLogLine(line); err != nil {
			return err
		}
		lock.Close()
		if err := s.syncFileOr(s.path); err != nil {
			return err
		}
		s.scopedMarks[identity] = true
		s.Replayed[eventID] = true
		return nil
	}
	content, err := s.tenantRewriteContent(kind, line)
	if err != nil {
		return err
	}
	if err := s.rewriteBytes(content); err != nil {
		return err
	}
	s.scopedMarks[identity] = true
	s.Replayed[eventID] = true
	return nil
}

func (s *ReplayState) tenantRewriteContent(kind stateKind, newLine []byte) ([]byte, error) {
	scoped := map[string]bool{}
	legacy := map[string]bool{}
	for identity := range s.scopedMarks {
		scoped[identity] = true
	}
	for eventID := range s.legacyMarks {
		legacy[eventID] = true
	}
	if kind == stateLog {
		encoded, err := os.ReadFile(s.path)
		if err != nil {
			return nil, fmt.Errorf("read replay state: %w", err)
		}
		marks, err := parseLogMarks(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode replay state: %w", err)
		}
		for eventID := range marks {
			legacy[eventID] = true
		}
	}
	if kind == stateLegacy {
		marks, err := loadLegacyMarks(s.path)
		if err != nil {
			return nil, err
		}
		for eventID := range marks {
			legacy[eventID] = true
		}
	}
	return tenantLogBytes(scoped, legacy, newLine), nil
}

func tenantLogBytes(scoped map[string]bool, legacy map[string]bool, newLine []byte) []byte {
	identities := make([]string, 0, len(scoped))
	for identity := range scoped {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	legacyIDs := make([]string, 0, len(legacy))
	for eventID := range legacy {
		legacyIDs = append(legacyIDs, eventID)
	}
	sort.Strings(legacyIDs)
	buf := make([]byte, 0, len(replayTenantLogHeader)+(len(identities)+len(legacyIDs)+1)*64)
	buf = append(buf, replayTenantLogHeader...)
	for _, identity := range identities {
		tenantID, ok := replayTenantFromIdentity(identity)
		eventID := replayEventIDFromIdentity(identity)
		if !ok || eventID == "" {
			continue
		}
		line, _ := tenantMarkLine(tenantID, eventID)
		buf = append(buf, line...)
	}
	for _, eventID := range legacyIDs {
		line, _ := markLine(eventID)
		buf = append(buf, line...)
	}
	buf = append(buf, newLine...)
	return buf
}

// parseTenantLogMarks accepts both scoped v2 lines and retained unscoped v1
// lines. As with parseLogMarks, an unterminated final line is a torn append,
// never a committed mark; malformed non-final lines fail closed.
func parseTenantLogMarks(encoded []byte) (map[string]bool, map[string]bool, error) {
	scoped := map[string]bool{}
	legacy := map[string]bool{}
	tornTail := len(encoded) > 0 && encoded[len(encoded)-1] != '\n'
	segments := bytes.Split(encoded, []byte{'\n'})
	last := len(segments) - 1
	for index, segment := range segments {
		if index == last && tornTail {
			continue
		}
		trimmed := bytes.TrimRight(segment, "\r")
		if index == 0 && bytes.Equal(trimmed, []byte(replayTenantLogHeaderLine)) {
			continue
		}
		if len(trimmed) == 0 {
			continue
		}
		var mark tenantReplayMark
		if err := json.Unmarshal(segment, &mark); err != nil || mark.EventID == "" {
			return nil, nil, fmt.Errorf("malformed tenant replay log line %d", index+1)
		}
		if mark.TenantID == "" {
			legacy[mark.EventID] = true
			continue
		}
		scoped[replayIdentityKey(mark.TenantID, mark.EventID)] = true
	}
	return scoped, legacy, nil
}

func (s *ReplayState) rebuildTenantLog() error {
	encoded, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("read replay state: %w", err)
	}
	scoped, legacy, err := parseTenantLogMarks(encoded)
	if err != nil {
		return err
	}
	return s.rewriteBytes(tenantLogBytes(scoped, legacy, nil))
}
