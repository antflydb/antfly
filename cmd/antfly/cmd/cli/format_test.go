/*
Copyright 2025 The Antfly Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cli

import (
	"bytes"
	"strings"
	"testing"

	antfly "github.com/antflydb/antfly/pkg/client"
)

func TestFormatTable(t *testing.T) {
	res := &antfly.QueryResponses{
		Responses: []antfly.QueryResult{
			{
				Hits: antfly.Hits{
					Total: 2,
					Hits: []antfly.Hit{
						{
							ID:    "doc-1",
							Score: 0.95,
							Source: map[string]any{
								"title": "Albert Einstein",
								"year":  float64(1905),
							},
						},
						{
							ID:    "doc-2",
							Score: 0.82,
							Source: map[string]any{
								"title": "Isaac Newton",
								"year":  float64(1687),
							},
						},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	err := formatQueryResults(&buf, res, outputTable)
	if err != nil {
		t.Fatalf("formatQueryResults: %v", err)
	}

	out := buf.String()

	// Should contain header
	if !strings.Contains(out, "_id") {
		t.Error("table output missing _id header")
	}
	if !strings.Contains(out, "_score") {
		t.Error("table output missing _score header")
	}
	if !strings.Contains(out, "title") {
		t.Error("table output missing title column")
	}

	// Should contain data
	if !strings.Contains(out, "doc-1") {
		t.Error("table output missing doc-1")
	}
	if !strings.Contains(out, "Albert Einstein") {
		t.Error("table output missing Albert Einstein")
	}
	if !strings.Contains(out, "0.950") {
		t.Error("table output missing score 0.950")
	}

	// Should show hit count
	if !strings.Contains(out, "Found 2 hit(s)") {
		t.Error("table output missing hit count")
	}
}

func TestFormatJSON(t *testing.T) {
	res := &antfly.QueryResponses{
		Responses: []antfly.QueryResult{
			{
				Hits: antfly.Hits{
					Total: 1,
					Hits: []antfly.Hit{
						{ID: "doc-1", Score: 0.5, Source: map[string]any{"title": "Test"}},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	err := formatQueryResults(&buf, res, outputJSON)
	if err != nil {
		t.Fatalf("formatQueryResults json: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `"_id"`) {
		t.Error("json output missing _id field")
	}
	if !strings.Contains(out, `"doc-1"`) {
		t.Error("json output missing doc-1 value")
	}
}

func TestFormatJSONL(t *testing.T) {
	res := &antfly.QueryResponses{
		Responses: []antfly.QueryResult{
			{
				Hits: antfly.Hits{
					Total: 2,
					Hits: []antfly.Hit{
						{ID: "doc-1", Score: 0.9, Source: map[string]any{"a": "1"}},
						{ID: "doc-2", Score: 0.8, Source: map[string]any{"a": "2"}},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	err := formatQueryResults(&buf, res, outputJSONL)
	if err != nil {
		t.Fatalf("formatQueryResults jsonl: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Errorf("expected 2 lines, got %d: %q", len(lines), buf.String())
	}
}

func TestFormatTableNoResults(t *testing.T) {
	res := &antfly.QueryResponses{
		Responses: []antfly.QueryResult{
			{Hits: antfly.Hits{Total: 0, Hits: nil}},
		},
	}

	var buf bytes.Buffer
	err := formatQueryResults(&buf, res, outputTable)
	if err != nil {
		t.Fatalf("formatQueryResults: %v", err)
	}
	if !strings.Contains(buf.String(), "No results found") {
		t.Error("expected 'No results found' for empty hits")
	}
}

func TestTruncPad(t *testing.T) {
	tests := []struct {
		input string
		width int
		want  string
	}{
		{"hello", 10, "hello     "},
		{"hello world", 5, "hell…"},
		{"hi", 2, "hi"},
		{"abc", 3, "abc"},
	}
	for _, tt := range tests {
		got := truncPad(tt.input, tt.width)
		if got != tt.want {
			t.Errorf("truncPad(%q, %d) = %q, want %q", tt.input, tt.width, got, tt.want)
		}
	}
}

func TestParseOutputFormat(t *testing.T) {
	tests := []struct {
		input   string
		want    outputFormat
		wantErr bool
	}{
		{"table", outputTable, false},
		{"json", outputJSON, false},
		{"jsonl", outputJSONL, false},
		{"TABLE", outputTable, false},
		{"", outputTable, false},
		{"xml", "", true},
	}
	for _, tt := range tests {
		got, err := parseOutputFormat(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseOutputFormat(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("parseOutputFormat(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
