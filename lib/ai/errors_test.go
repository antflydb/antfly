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

package ai

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	openrouter "github.com/revrost/go-openrouter"
)

func TestAsGenerationError(t *testing.T) {
	tests := []struct {
		name         string
		provider     string
		err          error
		wantKind     GenerationErrorKind
		wantMessage  string // exact match; use wantContains for substring
		wantContains string
	}{
		{
			name:        "openrouter 401",
			provider:    "openrouter",
			err:         &openrouter.APIError{HTTPStatusCode: 401, Message: "Invalid API key"},
			wantKind:    GenerationErrorAuth,
			wantMessage: "Authentication failed for provider 'openrouter'. Check your API key.",
		},
		{
			name:     "openrouter 403",
			provider: "openrouter",
			err:      &openrouter.APIError{HTTPStatusCode: 403, Message: "Forbidden"},
			wantKind: GenerationErrorAuth,
		},
		{
			name:        "openrouter 402",
			provider:    "openrouter",
			err:         &openrouter.APIError{HTTPStatusCode: 402, Message: "Payment required"},
			wantKind:    GenerationErrorQuotaExceeded,
			wantMessage: "Quota exceeded for provider 'openrouter'. Check your billing or usage limits.",
		},
		{
			name:        "openrouter 404",
			provider:    "openrouter",
			err:         &openrouter.APIError{HTTPStatusCode: 404, Message: "model not found"},
			wantKind:    GenerationErrorModelNotFound,
			wantMessage: "Model not found on provider 'openrouter'. Check your model name.",
		},
		{
			name:     "openrouter 429",
			provider: "openrouter",
			err:      &openrouter.APIError{HTTPStatusCode: 429, Message: "Rate limit exceeded"},
			wantKind: GenerationErrorRateLimit,
		},
		{
			name:     "openrouter 500",
			provider: "openrouter",
			err:      &openrouter.APIError{HTTPStatusCode: 500, Message: "Internal server error"},
			wantKind: GenerationErrorServer,
		},
		{
			name:     "openrouter RequestError",
			provider: "openrouter",
			err:      &openrouter.RequestError{HTTPStatusCode: 401, HTTPStatus: "401 Unauthorized"},
			wantKind: GenerationErrorAuth,
		},
		{
			name:     "deep wrapping preserves type",
			provider: "openrouter",
			err: fmt.Errorf("executing prompt: %w",
				fmt.Errorf("OpenRouter API error: %w",
					&openrouter.APIError{HTTPStatusCode: 402, Message: "Payment required"})),
			wantKind: GenerationErrorQuotaExceeded,
		},
		{
			name:        "context deadline",
			provider:    "openrouter",
			err:         fmt.Errorf("calling LLM: %w", context.DeadlineExceeded),
			wantKind:    GenerationErrorTimeout,
			wantMessage: "Request to provider 'openrouter' timed out.",
		},
		{
			name:         "plain error falls through to unknown",
			provider:     "openai",
			err:          errors.New("something broke"),
			wantKind:     GenerationErrorUnknown,
			wantContains: "something broke",
		},
		{
			name:        "empty provider defaults to unknown",
			provider:    "",
			err:         &openrouter.APIError{HTTPStatusCode: 401, Message: "bad key"},
			wantKind:    GenerationErrorAuth,
			wantMessage: "Authentication failed for provider 'unknown'. Check your API key.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AsGenerationError(tt.provider, tt.err)
			if result.Kind != tt.wantKind {
				t.Errorf("Kind = %v, want %v", result.Kind, tt.wantKind)
			}
			if tt.wantMessage != "" && result.UserMessage != tt.wantMessage {
				t.Errorf("UserMessage = %q, want %q", result.UserMessage, tt.wantMessage)
			}
			if tt.wantContains != "" && !strings.Contains(result.UserMessage, tt.wantContains) {
				t.Errorf("UserMessage = %q, want substring %q", result.UserMessage, tt.wantContains)
			}
		})
	}
}

func TestAsGenerationErrorPanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil error")
		}
	}()
	AsGenerationError("openai", nil)
}

func TestGenerationErrorImplementsError(t *testing.T) {
	genErr := GenerationError{
		Kind:        GenerationErrorRateLimit,
		UserMessage: "Rate limit reached for provider 'openrouter'. Please wait and try again.",
	}

	var err error = genErr
	if err.Error() != genErr.UserMessage {
		t.Fatalf("Error() = %q, want %q", err.Error(), genErr.UserMessage)
	}
}
