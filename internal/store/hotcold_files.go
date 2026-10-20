package store

// Durable file IO for the split layout: writing a hot document and the control
// plane with fsync, appending and replacing a tenant ledger, the v1 to v2
// migration performed on first open, and the derived file paths. TenantView is
// the closure-facing view every tenant-scoped caller receives; Hot holds only
// the tenant's Streams and Events, while Global stays private to the store.

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

func loadTenantState(root, tenantID string) (*tenantState, error) {
	hot := emptyTenantHot(tenantID)
	hotPath := tenantFilePath(root, tenantID)
	if raw, err := fsutil.ReadDataFile(hotPath); err == nil {
		var disk tenantHotSnapshot
		if err := decodeJSON(raw, &disk); err != nil {
			return nil, fmt.Errorf("decode tenant hot file %s: %w", hotPath, err)
		}
		hot.Events = disk.Events
		hot.Streams = disk.Streams
		if hot.Events == nil {
			hot.Events = map[string]domain.Event{}
		}
		if hot.Streams == nil {
			hot.Streams = map[string]StreamState{}
		}
		if disk.TenantID != tenantID {
			return nil, fmt.Errorf("tenant hot identity mismatch")
		}
		state, err := loadTenantLedger(root, tenantID, hot, disk.Version)
		if err != nil {
			return nil, err
		}
		if err := validateTenantCommitRecords(tenantID, disk.PendingLedger); err != nil {
			return nil, err
		}
		state.pending = cloneLedgerRecords(disk.PendingLedger)
		unresolved, err := unresolvedTenantCommit(state)
		if err != nil {
			return nil, err
		}
		if !unresolved {
			state.pending = nil
		}
		return state, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return loadTenantLedger(root, tenantID, hot, 1)
}

func loadTenantLedger(root, tenantID string, hot *Snapshot, version int64) (*tenantState, error) {
	records, err := readLedgerFile(root, tenantID)
	if err != nil {
		return nil, err
	}
	return &tenantState{hot: hot, ledger: newTenantLedger(tenantID, records), version: version}, nil
}

func migrateV1File(path string, data *Snapshot) error {
	backup := path + ".v1"
	if _, err := fsutil.StatDataFile(backup); errors.Is(err, os.ErrNotExist) {
		raw, readErr := fsutil.ReadDataFile(path)
		if readErr != nil {
			return fmt.Errorf("backup v1 state: %w", readErr)
		}
		if err := writeDurableFile(backup, raw, fsutil.PrivateFileMode); err != nil {
			return fmt.Errorf("backup v1 state: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("inspect v1 backup: %w", err)
	}
	prepared, err := splitSnapshot(data)
	if err != nil {
		return err
	}
	root := filepath.Dir(path)
	ids := make([]string, 0, len(prepared.tenant))
	for tenantID := range prepared.tenant {
		ids = append(ids, tenantID)
	}
	sort.Strings(ids)
	for _, tenantID := range ids {
		state := prepared.tenant[tenantID]
		if err := writeTenantFile(root, tenantID, tenantHotSnapshot{TenantID: tenantID, Version: state.version, Events: state.hot.Events, Streams: state.hot.Streams}); err != nil {
			return err
		}
		if err := replaceLedgerFile(root, tenantID, state.ledger.committedRecords()); err != nil {
			return err
		}
	}
	if err := writeControlFile(path, prepared.control); err != nil {
		return err
	}
	*data = *prepared.control
	return nil
}

func tenantFilePath(root, tenantID string) string {
	return filepath.Join(root, "tenants", tenantFileName(tenantID)+".json")
}

func ledgerFilePath(root, tenantID string) string {
	return filepath.Join(root, "ledger", tenantFileName(tenantID)+".jsonl")
}

func tenantFileName(tenantID string) string {
	encoded := fsutil.EncodeKeyComponent(tenantID)
	if len(encoded)+len(".json.tmp") <= 255 {
		return encoded
	}
	digest := sha256.Sum256([]byte(tenantID))
	return "sha256-" + hex.EncodeToString(digest[:])
}

func writeTenantFile(root, tenantID string, disk tenantHotSnapshot) error {
	encoded, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}
	return writeDurableFile(tenantFilePath(root, tenantID), encoded, fsutil.PrivateFileMode)
}

func writeControlFile(path string, data *Snapshot) error {
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return writeDurableFile(path, encoded, fsutil.PrivateFileMode)
}

var writeDurableFile = writeDurableFileImpl

func writeDurableFileImpl(path string, data []byte, mode os.FileMode) error {
	return fsutil.AtomicDataFile(path, data, mode, nil)
}

var appendLedgerFile = appendLedgerFileImpl

func appendLedgerFileImpl(root, tenantID string, records []LedgerRecord) (err error) {
	path := ledgerFilePath(root, tenantID)
	parent := filepath.Dir(path)
	ancestor := fsutil.DeepestExistingAncestor(parent)
	dir, err := fsutil.OpenDataDir(parent, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	file, err := dir.OpenFile(filepath.Base(path), os.O_WRONLY|os.O_CREATE|os.O_APPEND, fsutil.PrivateFileMode)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return dir.SyncChain(ancestor, nil)
}

func replaceLedgerFile(root, tenantID string, records []LedgerRecord) error {
	if len(records) == 0 {
		// Keep an explicit empty ledger alongside every tenant hot file. The
		// marker is part of the v2 layout integrity check: a missing ledger
		// file is indistinguishable from lost cold evidence after restart.
		return writeDurableFile(ledgerFilePath(root, tenantID), nil, fsutil.PrivateFileMode)
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}
	return writeDurableFile(ledgerFilePath(root, tenantID), buffer.Bytes(), fsutil.PrivateFileMode)
}

func readLedgerFile(root, tenantID string) ([]LedgerRecord, error) {
	path := ledgerFilePath(root, tenantID)
	file, err := fsutil.OpenDataFile(path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var records []LedgerRecord
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record LedgerRecord
		if err := decodeJSON(line, &record); err != nil {
			return nil, fmt.Errorf("decode ledger %s: %w", path, err)
		}
		if record.TenantID != tenantID {
			return nil, fmt.Errorf("ledger %s contains tenant %q", path, record.TenantID)
		}
		if err := validateLedgerRecord(record); err != nil {
			return nil, fmt.Errorf("validate ledger %s: %w", path, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func decodeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(target)
}

// TenantView is the closure-facing view of one tenant. Hot contains only
// Streams and hot Events; global control-plane maps are intentionally absent
// from Hot. Global is a private control snapshot for authorization checks and
// tenant-scoped worker metadata mutations in the same store critical section.
type TenantView struct {
	Hot    *Snapshot
	Ledger *TenantLedger
	Global *Snapshot
	// PublicationConfirmed is a read-only, tenant-bound lookup under this
	// operation's context/lock. It performs no nested Store operation.
	PublicationConfirmed func(domain.EventReceipt) (bool, error)
}
