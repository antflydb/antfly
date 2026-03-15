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
	"strings"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/antflydb/antfly/lib/ai"
)

// extractTextFromMessage concatenates all TextParts in a message.
func extractTextFromMessage(msg *a2a.Message) string {
	if msg == nil {
		return ""
	}
	var parts []string
	for _, p := range msg.Parts {
		if tp, ok := p.(*a2a.TextPart); ok {
			parts = append(parts, tp.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// extractDataFromMessage returns the data from the first DataPart in a message.
func extractDataFromMessage(msg *a2a.Message) map[string]any {
	if msg == nil {
		return nil
	}
	for _, p := range msg.Parts {
		if dp, ok := p.(*a2a.DataPart); ok {
			return dp.Data
		}
	}
	return nil
}

// extractConversationHistory converts A2A task history to Antfly ChatMessages.
func extractConversationHistory(task *a2a.Task) []ai.ChatMessage {
	if task == nil || len(task.History) == 0 {
		return nil
	}
	var messages []ai.ChatMessage
	for _, msg := range task.History {
		role := ai.ChatMessageRoleUser
		if msg.Role == a2a.MessageRoleAgent {
			role = ai.ChatMessageRoleAssistant
		}
		text := extractTextFromMessage(msg)
		if text != "" {
			messages = append(messages, ai.ChatMessage{
				Role:    role,
				Content: text,
			})
		}
	}
	return messages
}

// stringFromMap extracts a string value from a map, returning fallback if not found.
func stringFromMap(m map[string]any, key, fallback string) string {
	if m == nil {
		return fallback
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return fallback
}

// intFromMap extracts an int value from a map, returning fallback if not found.
func intFromMap(m map[string]any, key string, fallback int) int {
	if m == nil {
		return fallback
	}
	v, ok := m[key]
	if !ok {
		return fallback
	}
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case int64:
		return int(n)
	default:
		return fallback
	}
}
