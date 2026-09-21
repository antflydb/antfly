package oapi

import (
	"encoding/json"
	"testing"
)

// A minimal transcriber shorthand must serialize to exactly what the caller
// set. The schema used to compose the provider configuration, which made the
// generator emit a marshaler that wrote every field: an unset
// max_download_bytes went out as 0, a limit that rejects every recording.
func TestTranscriberEnrichmentConfigOmitsUnsetOptions(t *testing.T) {
	encoded, err := json.Marshal(TranscriberEnrichmentConfig{
		Provider: "antfly",
		Model:    "openai/whisper-base",
	})
	if err != nil {
		t.Fatalf("encode transcriber config: %v", err)
	}

	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode transcriber config: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("expected only the fields that were set, got %s", encoded)
	}
	for _, name := range []string{"max_download_bytes", "timestamps", "diarization", "language_code", "api_key"} {
		if _, present := fields[name]; present {
			t.Fatalf("%s must be omitted when unset, got %s", name, encoded)
		}
	}
	if fields["provider"] != "antfly" || fields["model"] != "openai/whisper-base" {
		t.Fatalf("unexpected payload %s", encoded)
	}
}

// What the caller does set survives, including the values that share Go's
// zero value with "unset": false is only sent when it was asked for.
func TestTranscriberEnrichmentConfigKeepsSetOptions(t *testing.T) {
	timestampsOff := false
	encoded, err := json.Marshal(TranscriberEnrichmentConfig{
		Provider:         "vertex",
		Model:            "long",
		LanguageCode:     "en",
		Diarization:      true,
		MaxDownloadBytes: 4096,
		Timestamps:       &timestampsOff,
	})
	if err != nil {
		t.Fatalf("encode transcriber config: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode transcriber config: %v", err)
	}
	if fields["diarization"] != true {
		t.Fatalf("diarization = %v in %s", fields["diarization"], encoded)
	}
	if fields["max_download_bytes"].(float64) != 4096 {
		t.Fatalf("max_download_bytes = %v in %s", fields["max_download_bytes"], encoded)
	}
	if fields["language_code"] != "en" {
		t.Fatalf("language_code = %v in %s", fields["language_code"], encoded)
	}
	if fields["timestamps"] != false {
		t.Fatalf("timestamps = %v in %s", fields["timestamps"], encoded)
	}
}

// timestamps defaults to true on the server, so a client that cannot tell
// false from unset can never turn segment timing off. The field is a
// pointer for exactly that reason.
func TestTranscriberEnrichmentConfigSendsExplicitTimestamps(t *testing.T) {
	no := false
	yes := true
	for _, tc := range []struct {
		name    string
		value   *bool
		present bool
		want    bool
	}{
		{name: "unset", value: nil, present: false},
		{name: "false", value: &no, present: true, want: false},
		{name: "true", value: &yes, present: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(TranscriberEnrichmentConfig{
				Provider:   "antfly",
				Model:      "openai/whisper-base",
				Timestamps: tc.value,
			})
			if err != nil {
				t.Fatalf("encode transcriber config: %v", err)
			}
			var fields map[string]any
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatalf("decode transcriber config: %v", err)
			}
			got, present := fields["timestamps"]
			if present != tc.present {
				t.Fatalf("timestamps present = %v, want %v in %s", present, tc.present, encoded)
			}
			if tc.present && got != tc.want {
				t.Fatalf("timestamps = %v, want %v in %s", got, tc.want, encoded)
			}
		})
	}
}
