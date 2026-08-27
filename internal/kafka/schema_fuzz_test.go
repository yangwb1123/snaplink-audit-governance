package kafka

import "testing"

func FuzzValidateEventJSONNeverPanics(f *testing.F) {
	f.Add([]byte("{}"))
	f.Add([]byte("not-json"))
	f.Add([]byte(`{"event_id":"evt-1"}{"event_id":"evt-2"}`))

	f.Fuzz(func(t *testing.T, value []byte) {
		for _, schema := range []EventSchema{AcceptedEventSchema, LedgeredEventSchema} {
			_, _ = ValidateEventJSON(schema, value)
		}
	})
}
