// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package foreign

import (
	"encoding/json"
	"testing"
	"time"
)

func TestConvertValue_Bytes_JSON(t *testing.T) {
	input := []byte(`{"key": "value", "num": 42}`)
	result := convertValue(input)
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["key"] != "value" {
		t.Errorf("expected key=value, got %v", m["key"])
	}
}

func TestConvertValue_Bytes_NonJSON(t *testing.T) {
	input := []byte("plain text")
	result := convertValue(input)
	s, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if s != "plain text" {
		t.Errorf("expected 'plain text', got %q", s)
	}
}

func TestConvertValue_Bytes_JSONArray(t *testing.T) {
	input := []byte(`[1, 2, 3]`)
	result := convertValue(input)
	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d", len(arr))
	}
}

func TestConvertValue_Time(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	result := convertValue(ts)
	s, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if s != "2025-06-15T10:30:00Z" {
		t.Errorf("expected RFC3339 timestamp, got %q", s)
	}
}

func TestConvertValue_UUID(t *testing.T) {
	// UUID bytes: 550e8400-e29b-41d4-a716-446655440000
	uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	result := convertValue(uuid)
	s, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if s != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("expected UUID string, got %q", s)
	}
}

func TestConvertValue_Passthrough(t *testing.T) {
	// int64, float64, string, nil should pass through unchanged
	tests := []struct {
		name  string
		input any
	}{
		{"int64", int64(42)},
		{"float64", float64(3.14)},
		{"string", "hello"},
		{"nil", nil},
		{"bool", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.input)
			if result != tt.input {
				t.Errorf("expected %v (%T), got %v (%T)", tt.input, tt.input, result, result)
			}
		})
	}
}

func TestEscapeLike(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"100%", `100\%`},
		{"a_b", `a\_b`},
		{"100%_x", `100\%\_x`},
	}
	for _, tt := range tests {
		got := escapeLike(tt.input)
		if got != tt.want {
			t.Errorf("escapeLike(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestConvertValue_Bytes_EmptyJSON(t *testing.T) {
	// Empty JSON object
	input := []byte(`{}`)
	result := convertValue(input)
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

func TestConvertValue_Bytes_JSONString(t *testing.T) {
	// JSON string value should unmarshal to string
	input := []byte(`"hello world"`)
	result := convertValue(input)
	s, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if s != "hello world" {
		t.Errorf("expected 'hello world', got %q", s)
	}
}

func TestConvertValue_Bytes_InvalidJSON(t *testing.T) {
	// Invalid JSON should fall back to string
	input := []byte(`{not json`)
	result := convertValue(input)
	s, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if s != "{not json" {
		t.Errorf("expected '{not json', got %q", s)
	}
}

// TestScanRows_Integration is not feasible without a real DB connection,
// but we test the helper functions it relies on above.

// TestAggregateResult_JSON verifies AggregateResult serializes cleanly.
func TestAggregateResult_JSON(t *testing.T) {
	result := &AggregateResult{
		Results: map[string]any{
			"total_count": int64(100),
			"avg_price":   float64(29.99),
			"categories": []map[string]any{
				{"key": "electronics", "doc_count": int64(50)},
				{"key": "books", "doc_count": int64(30)},
			},
		},
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	results, ok := parsed["Results"].(map[string]any)
	if !ok {
		t.Fatal("expected Results map")
	}
	if results["total_count"] != float64(100) {
		t.Errorf("expected total_count=100, got %v", results["total_count"])
	}
}
