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

package template

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatErrorDirective(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		message  string
		expected string
	}{
		{
			name:     "with status code",
			status:   404,
			message:  "Not Found",
			expected: "<<<error:status=404 message=Not Found>>>",
		},
		{
			name:     "without status code",
			status:   0,
			message:  "connection refused",
			expected: "<<<error:message=connection refused>>>",
		},
		{
			name:     "message containing >>> is sanitized",
			status:   500,
			message:  "bad response>>>injected",
			expected: `<<<error:status=500 message=bad response>>\>injected>>>`,
		},
		{
			name:     "negative status treated as zero",
			status:   -1,
			message:  "weird",
			expected: "<<<error:message=weird>>>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatErrorDirective(tt.status, tt.message)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestParseErrorDirectives(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []ErrorDirective
	}{
		{
			name:  "single directive with status",
			input: "<<<error:status=404 message=Not Found>>>",
			expected: []ErrorDirective{
				{Status: 404, Message: "Not Found"},
			},
		},
		{
			name:  "single directive without status",
			input: "<<<error:message=connection refused>>>",
			expected: []ErrorDirective{
				{Status: 0, Message: "connection refused"},
			},
		},
		{
			name:  "multiple directives",
			input: "<<<error:status=404 message=Not Found>>> some text <<<error:status=503 message=Service Unavailable>>>",
			expected: []ErrorDirective{
				{Status: 404, Message: "Not Found"},
				{Status: 503, Message: "Service Unavailable"},
			},
		},
		{
			name:     "no directives",
			input:    "just some normal text",
			expected: nil,
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:  "directive embedded in surrounding text",
			input: "prefix <<<error:status=401 message=Unauthorized>>> suffix",
			expected: []ErrorDirective{
				{Status: 401, Message: "Unauthorized"},
			},
		},
		{
			name:  "directive with special characters in message",
			input: `<<<error:message=failed: Get "http://example.com" timeout>>>`,
			expected: []ErrorDirective{
				{Status: 0, Message: `failed: Get "http://example.com" timeout`},
			},
		},
		{
			name:     "does not match dotprompt media markers",
			input:    "<<<dotprompt:media:url https://example.com/img.png>>>",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseErrorDirectives(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestFormatAndParseRoundTrip(t *testing.T) {
	tests := []struct {
		status  int
		message string
	}{
		{404, "Not Found"},
		{0, "connection refused"},
		{503, "Service Unavailable"},
		{410, "Gone"},
	}

	for _, tt := range tests {
		formatted := FormatErrorDirective(tt.status, tt.message)
		directives := ParseErrorDirectives(formatted)
		require.Len(t, directives, 1)
		assert.Equal(t, tt.status, directives[0].Status)
		assert.Equal(t, tt.message, directives[0].Message)
	}
}

func TestContainsErrorDirective(t *testing.T) {
	assert.True(t, ContainsErrorDirective("<<<error:status=404 message=Not Found>>>"))
	assert.True(t, ContainsErrorDirective("prefix <<<error:message=timeout>>> suffix"))
	assert.False(t, ContainsErrorDirective("clean text"))
	assert.False(t, ContainsErrorDirective(""))
	assert.False(t, ContainsErrorDirective("<<<dotprompt:media:url https://example.com>>>"))
}

func TestErrorDirective_IsPermanent(t *testing.T) {
	permanent := []int{401, 403, 404, 410}
	for _, code := range permanent {
		d := ErrorDirective{Status: code, Message: "test"}
		assert.True(t, d.IsPermanent(), "expected status %d to be permanent", code)
	}
	transient := []int{0, 200, 301, 400, 429, 500, 503}
	for _, code := range transient {
		d := ErrorDirective{Status: code, Message: "test"}
		assert.False(t, d.IsPermanent(), "expected status %d to not be permanent", code)
	}
}

func TestStripErrorDirectives(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "removes single directive",
			input:    "before <<<error:status=404 message=Not Found>>> after",
			expected: "before  after",
		},
		{
			name:     "removes multiple directives",
			input:    "<<<error:message=a>>> mid <<<error:status=500 message=b>>>",
			expected: " mid ",
		},
		{
			name:     "no directives unchanged",
			input:    "clean text",
			expected: "clean text",
		},
		{
			name:     "preserves dotprompt media markers",
			input:    "<<<dotprompt:media:url https://example.com>>> <<<error:message=fail>>>",
			expected: "<<<dotprompt:media:url https://example.com>>> ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, StripErrorDirectives(tt.input))
		})
	}
}
