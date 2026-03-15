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

package a2a

import (
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/antflydb/antfly/lib/ai"
)

func TestExtractTextFromMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  *a2a.Message
		want string
	}{
		{
			name: "nil message",
			msg:  nil,
			want: "",
		},
		{
			name: "single text part",
			msg: &a2a.Message{
				Parts: a2a.ContentParts{&a2a.TextPart{Text: "hello"}},
			},
			want: "hello",
		},
		{
			name: "multiple text parts",
			msg: &a2a.Message{
				Parts: a2a.ContentParts{
					&a2a.TextPart{Text: "hello"},
					&a2a.TextPart{Text: "world"},
				},
			},
			want: "hello\nworld",
		},
		{
			name: "mixed parts ignores non-text",
			msg: &a2a.Message{
				Parts: a2a.ContentParts{
					&a2a.TextPart{Text: "query"},
					&a2a.DataPart{Data: map[string]any{"key": "val"}},
				},
			},
			want: "query",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTextFromMessage(tt.msg)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractDataFromMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  *a2a.Message
		want map[string]any
	}{
		{
			name: "nil message",
			msg:  nil,
			want: nil,
		},
		{
			name: "no data part",
			msg: &a2a.Message{
				Parts: a2a.ContentParts{&a2a.TextPart{Text: "hello"}},
			},
			want: nil,
		},
		{
			name: "with data part",
			msg: &a2a.Message{
				Parts: a2a.ContentParts{
					&a2a.TextPart{Text: "query"},
					&a2a.DataPart{Data: map[string]any{"table": "docs"}},
				},
			},
			want: map[string]any{"table": "docs"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDataFromMessage(tt.msg)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if got["table"] != tt.want["table"] {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractConversationHistory(t *testing.T) {
	task := &a2a.Task{
		History: []*a2a.Message{
			{Role: a2a.MessageRoleUser, Parts: a2a.ContentParts{&a2a.TextPart{Text: "What is OAuth?"}}},
			{Role: a2a.MessageRoleAgent, Parts: a2a.ContentParts{&a2a.TextPart{Text: "OAuth is..."}}},
		},
	}

	messages := extractConversationHistory(task)
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	if messages[0].Role != ai.ChatMessageRoleUser {
		t.Errorf("expected user role, got %s", messages[0].Role)
	}
	if messages[0].Content != "What is OAuth?" {
		t.Errorf("unexpected content: %s", messages[0].Content)
	}

	if messages[1].Role != ai.ChatMessageRoleAssistant {
		t.Errorf("expected assistant role, got %s", messages[1].Role)
	}
}

func TestExtractConversationHistoryNil(t *testing.T) {
	messages := extractConversationHistory(nil)
	if messages != nil {
		t.Errorf("expected nil, got %v", messages)
	}
}

func TestStringFromMap(t *testing.T) {
	m := map[string]any{"key": "value", "num": 42}

	if got := stringFromMap(m, "key", "default"); got != "value" {
		t.Errorf("got %q, want %q", got, "value")
	}
	if got := stringFromMap(m, "missing", "default"); got != "default" {
		t.Errorf("got %q, want %q", got, "default")
	}
	if got := stringFromMap(m, "num", "default"); got != "default" {
		t.Errorf("got %q, want %q", got, "default")
	}
	if got := stringFromMap(nil, "key", "default"); got != "default" {
		t.Errorf("got %q, want %q", got, "default")
	}
}

func TestIntFromMap(t *testing.T) {
	m := map[string]any{"int": 5, "float": 3.14, "str": "hello"}

	if got := intFromMap(m, "int", 0); got != 5 {
		t.Errorf("got %d, want 5", got)
	}
	if got := intFromMap(m, "float", 0); got != 3 {
		t.Errorf("got %d, want 3", got)
	}
	if got := intFromMap(m, "str", 99); got != 99 {
		t.Errorf("got %d, want 99", got)
	}
	if got := intFromMap(m, "missing", 42); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}
