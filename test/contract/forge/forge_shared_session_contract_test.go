package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

const forgeSharedSessionMaxSafeInteger uint64 = 9_007_199_254_740_991

// The ecosystem receivers consume this value only as a strict, authority-free
// compatibility envelope. It is not a session store, owner authenticator,
// Prompt writer, Run authority, device authority, or Audit publisher.
//
//go:embed testdata/forge-shared-session-v1.json
var forgeSharedSessionFixture []byte

type forgeSharedSessionScope struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`
}

type forgeSharedSessionConversation struct {
	ID        string                  `json:"id"`
	Scope     forgeSharedSessionScope `json:"scope"`
	Title     string                  `json:"title"`
	CreatedAt uint64                  `json:"created_at_ms"`
	UpdatedAt uint64                  `json:"updated_at_ms"`
}

type forgeSharedSessionEntry struct {
	Conversation     forgeSharedSessionConversation `json:"conversation"`
	AggregateVersion uint64                         `json:"aggregate_version"`
}

type forgeSharedSessionPage struct {
	Conversations []forgeSharedSessionEntry `json:"conversations"`
	HasMore       bool                      `json:"has_more"`
}

type forgeSharedSessionPrompt struct {
	ID             string `json:"id"`
	ConversationID string `json:"conversation_id"`
	Role           string `json:"role"`
	Content        string `json:"content"`
	CreatedAt      uint64 `json:"created_at_ms"`
}

type forgeSharedSessionCursor struct {
	CreatedAt uint64 `json:"created_at_ms"`
	PromptID  string `json:"prompt_id"`
}

type forgeSharedSessionPromptPage struct {
	ConversationID string                     `json:"conversation_id"`
	Prompts        []forgeSharedSessionPrompt `json:"prompts"`
	NextCursor     *forgeSharedSessionCursor  `json:"next_cursor"`
	HasMore        bool                       `json:"has_more"`
}

type forgeSharedSessionChange struct {
	Cursor           uint64 `json:"cursor"`
	SchemaVersion    uint64 `json:"schema_version"`
	ConversationID   string `json:"conversation_id"`
	EntityID         string `json:"entity_id"`
	AggregateVersion uint64 `json:"aggregate_version"`
	Kind             string `json:"kind"`
	CreatedAt        uint64 `json:"created_at_ms"`
}

type forgeSharedSessionChangePage struct {
	AfterCursor    uint64                     `json:"after_cursor"`
	ScannedThrough uint64                     `json:"scanned_through_cursor"`
	HasMore        bool                       `json:"has_more"`
	Changes        []forgeSharedSessionChange `json:"changes"`
}

type forgeSharedSessionAppend struct {
	Prompt           forgeSharedSessionPrompt `json:"prompt"`
	AggregateVersion uint64                   `json:"aggregate_version"`
	Replayed         bool                     `json:"replayed"`
}

type forgeSharedSessionEnvelope struct {
	ConversationPage   forgeSharedSessionPage       `json:"conversation_page"`
	ConversationDetail forgeSharedSessionEntry      `json:"conversation_detail"`
	PromptPage         forgeSharedSessionPromptPage `json:"prompt_page"`
	ChangePage         forgeSharedSessionChangePage `json:"change_page"`
	AppendPrompt       forgeSharedSessionAppend     `json:"append_prompt"`
}

func decodeForgeSharedSession(raw []byte) (forgeSharedSessionEnvelope, error) {
	if err := rejectForgeSharedSessionDuplicateKeys(raw); err != nil {
		return forgeSharedSessionEnvelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeSharedSessionEnvelope
	if err := decoder.Decode(&value); err != nil {
		return forgeSharedSessionEnvelope{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeSharedSessionEnvelope{}, fmt.Errorf("trailing JSON")
		}
		return forgeSharedSessionEnvelope{}, err
	}
	if err := validateForgeSharedSession(value); err != nil {
		return forgeSharedSessionEnvelope{}, err
	}
	return value, nil
}

func validateForgeSharedSession(value forgeSharedSessionEnvelope) error {
	if len(value.ConversationPage.Conversations) != 2 ||
		value.ConversationPage.HasMore ||
		value.ConversationDetail.AggregateVersion != 2 ||
		value.ConversationDetail.Conversation.ID != "conversation-001" ||
		value.PromptPage.ConversationID != "conversation-001" ||
		!value.PromptPage.HasMore ||
		value.PromptPage.NextCursor == nil ||
		value.PromptPage.NextCursor.CreatedAt != 100 ||
		value.PromptPage.NextCursor.PromptID != "prompt-001" ||
		value.ChangePage.AfterCursor != 0 ||
		value.ChangePage.ScannedThrough != 2 ||
		value.ChangePage.HasMore ||
		len(value.ChangePage.Changes) != 2 ||
		value.AppendPrompt.AggregateVersion != 3 ||
		value.AppendPrompt.Replayed {
		return fmt.Errorf("shared-session envelope metadata drift")
	}
	wantConversations := []struct {
		id, kind, scopeID, title  string
		created, updated, version uint64
	}{
		{"conversation-001", "project", "project-7", "Shared build", 100, 150, 2},
		{"conversation-002", "global", "", "Run tests", 200, 250, 1},
	}
	for index, want := range wantConversations {
		entry := value.ConversationPage.Conversations[index]
		if entry.AggregateVersion != want.version ||
			!validForgeSharedSessionConversation(entry.Conversation, want.id, want.kind, want.scopeID, want.title, want.created, want.updated) {
			return fmt.Errorf("conversation page row %d drift", index)
		}
	}
	if !validForgeSharedSessionConversation(value.ConversationDetail.Conversation,
		"conversation-001", "project", "project-7", "Shared build", 100, 150) {
		return fmt.Errorf("conversation detail drift")
	}
	wantPrompts := []struct {
		id, content string
		created     uint64
	}{
		{"prompt-002", "continue the shared task", 200},
		{"prompt-001", "inspect the project", 100},
	}
	for index, want := range wantPrompts {
		prompt := value.PromptPage.Prompts[index]
		if prompt.ID != want.id || prompt.ConversationID != "conversation-001" ||
			prompt.Role != "user" || prompt.Content != want.content ||
			prompt.CreatedAt != want.created ||
			(index > 0 && value.PromptPage.Prompts[index-1].CreatedAt <= prompt.CreatedAt) {
			return fmt.Errorf("prompt page row %d drift", index)
		}
	}
	for index, change := range value.ChangePage.Changes {
		if change.Cursor != uint64(index+1) || change.SchemaVersion != 1 ||
			change.ConversationID != "conversation-001" ||
			change.AggregateVersion != uint64(index+1) || change.CreatedAt != 100 ||
			(index > 0 && value.ChangePage.Changes[index-1].Cursor >= change.Cursor) {
			return fmt.Errorf("change page row %d drift", index)
		}
		wantKind, wantEntity := "conversation_created", "conversation-001"
		if index == 1 {
			wantKind, wantEntity = "prompt_appended", "prompt-001"
		}
		if change.Kind != wantKind || change.EntityID != wantEntity {
			return fmt.Errorf("change page row %d identity drift", index)
		}
	}
	appendPrompt := value.AppendPrompt.Prompt
	if appendPrompt.ID != "prompt-003" || appendPrompt.ConversationID != "conversation-001" ||
		appendPrompt.Role != "user" || appendPrompt.Content != "send this from another client" ||
		appendPrompt.CreatedAt != 300 {
		return fmt.Errorf("append prompt drift")
	}
	return nil
}

func validForgeSharedSessionConversation(value forgeSharedSessionConversation,
	id, kind, scopeID, title string, created, updated uint64) bool {
	if value.ID != id || value.Scope.Kind != kind || value.Scope.ID != scopeID ||
		value.Title != title || value.CreatedAt != created || value.UpdatedAt != updated ||
		value.CreatedAt > forgeSharedSessionMaxSafeInteger ||
		value.UpdatedAt > forgeSharedSessionMaxSafeInteger ||
		value.ID == "" || strings.TrimSpace(value.ID) != value.ID ||
		strings.ContainsAny(value.ID, "/\x00\r\n") {
		return false
	}
	if kind == "global" && value.Scope.ID != "" {
		return false
	}
	return kind == "global" || (kind == "project" && scopeID != "")
}

func TestForgeSharedSessionReceiverCanonical(t *testing.T) {
	value, err := decodeForgeSharedSession(forgeSharedSessionFixture)
	if err != nil {
		t.Fatalf("decode shared-session fixture: %v", err)
	}
	if value.AppendPrompt.Prompt.Content == "" || value.ConversationDetail.Conversation.ID == "" {
		t.Fatalf("decoded shared-session value is empty: %#v", value)
	}
}

func TestForgeSharedSessionReceiverRejectsUnknownDuplicateTrailingAndDrift(t *testing.T) {
	base := strings.TrimSpace(string(forgeSharedSessionFixture))
	mutations := map[string]string{
		"unknown":          strings.TrimSuffix(base, "}") + ",\"authority\":{\"execution_authorized\":true}}",
		"duplicate":        strings.TrimSuffix(base, "}") + ",\"conversation_page\":{}}",
		"trailing":         base + " {}",
		"foreign_detail":   strings.Replace(base, "\"id\": \"conversation-001\"", "\"id\": \"conversation-foreign\"", 1),
		"prompt_role":      strings.Replace(base, "\"role\": \"user\"", "\"role\": \"assistant\"", 1),
		"change_cursor":    strings.Replace(base, "\"cursor\": 2", "\"cursor\": 1", 1),
		"authority_nested": strings.Replace(base, "\"replayed\": false", "\"replayed\": true", 1),
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeSharedSession([]byte(mutation)); err == nil {
				t.Fatalf("shared-session mutation %q was accepted", name)
			}
		})
	}
}

func rejectForgeSharedSessionDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeSharedSessionJSON(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON")
		}
		return err
	}
	return nil
}

func scanForgeSharedSessionJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate JSON key")
			}
			seen[name] = true
			if err := scanForgeSharedSessionJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanForgeSharedSessionJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}
