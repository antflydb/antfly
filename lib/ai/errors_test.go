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
	"testing"

	openrouter "github.com/revrost/go-openrouter"
)

func TestClassifyGenerationError_OpenRouterAuth(t *testing.T) {
	err := &openrouter.APIError{HTTPStatusCode: 401, Message: "Invalid API key"}
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorAuth {
		t.Errorf("expected Auth, got %v", result.Kind)
	}
	if got := result.UserMessage; got != "Authentication failed for provider 'openrouter'. Check your API key." {
		t.Errorf("unexpected message: %s", got)
	}
}

func TestClassifyGenerationError_OpenRouterForbidden(t *testing.T) {
	err := &openrouter.APIError{HTTPStatusCode: 403, Message: "Forbidden"}
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorAuth {
		t.Errorf("expected Auth, got %v", result.Kind)
	}
}

func TestClassifyGenerationError_OpenRouterQuota(t *testing.T) {
	err := &openrouter.APIError{HTTPStatusCode: 402, Message: "Payment required"}
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorQuotaExceeded {
		t.Errorf("expected QuotaExceeded, got %v", result.Kind)
	}
	if got := result.UserMessage; got != "Quota exceeded for provider 'openrouter'. Check your billing or usage limits." {
		t.Errorf("unexpected message: %s", got)
	}
}

func TestClassifyGenerationError_OpenRouterModelNotFound(t *testing.T) {
	err := &openrouter.APIError{HTTPStatusCode: 404, Message: "model not found"}
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorModelNotFound {
		t.Errorf("expected ModelNotFound, got %v", result.Kind)
	}
	expected := "Model not found on provider 'openrouter'. Check your model name."
	if result.UserMessage != expected {
		t.Errorf("expected %q, got %q", expected, result.UserMessage)
	}
}

func TestClassifyGenerationError_OpenRouterRateLimit(t *testing.T) {
	err := &openrouter.APIError{HTTPStatusCode: 429, Message: "Rate limit exceeded"}
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorRateLimit {
		t.Errorf("expected RateLimit, got %v", result.Kind)
	}
}

func TestClassifyGenerationError_OpenRouterServerError(t *testing.T) {
	err := &openrouter.APIError{HTTPStatusCode: 500, Message: "Internal server error"}
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorServer {
		t.Errorf("expected ServerError, got %v", result.Kind)
	}
}

func TestClassifyGenerationError_OpenRouterRequestError(t *testing.T) {
	err := &openrouter.RequestError{HTTPStatusCode: 401, HTTPStatus: "401 Unauthorized"}
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorAuth {
		t.Errorf("expected Auth, got %v", result.Kind)
	}
}

func TestClassifyGenerationError_DeepWrapping(t *testing.T) {
	apiErr := &openrouter.APIError{HTTPStatusCode: 402, Message: "Payment required"}
	wrapped := fmt.Errorf("executing prompt: %w", fmt.Errorf("OpenRouter API error: %w", apiErr))
	result := ClassifyGenerationError("openrouter", wrapped)
	if result.Kind != GenerationErrorQuotaExceeded {
		t.Errorf("expected QuotaExceeded through wrapping, got %v", result.Kind)
	}
}

func TestClassifyGenerationError_Timeout(t *testing.T) {
	err := fmt.Errorf("calling LLM: %w", context.DeadlineExceeded)
	result := ClassifyGenerationError("openrouter", err)
	if result.Kind != GenerationErrorTimeout {
		t.Errorf("expected Timeout, got %v", result.Kind)
	}
	expected := "Request to provider 'openrouter' timed out."
	if result.UserMessage != expected {
		t.Errorf("expected %q, got %q", expected, result.UserMessage)
	}
}

func TestClassifyGenerationError_PlainError(t *testing.T) {
	err := errors.New("something broke")
	result := ClassifyGenerationError("openai", err)
	if result.Kind != GenerationErrorUnknown {
		t.Errorf("expected Unknown, got %v", result.Kind)
	}
	expected := "Generation failed (provider 'openai'): something broke"
	if result.UserMessage != expected {
		t.Errorf("expected %q, got %q", expected, result.UserMessage)
	}
}

func TestClassifyGenerationError_Nil(t *testing.T) {
	result := ClassifyGenerationError("openai", nil)
	if result.Kind != GenerationErrorUnknown {
		t.Errorf("expected Unknown for nil, got %v", result.Kind)
	}
	if result.UserMessage != "" {
		t.Errorf("expected empty message for nil, got %q", result.UserMessage)
	}
}

func TestClassifyGenerationError_EmptyProvider(t *testing.T) {
	err := &openrouter.APIError{HTTPStatusCode: 401, Message: "bad key"}
	result := ClassifyGenerationError("", err)
	if result.Kind != GenerationErrorAuth {
		t.Errorf("expected Auth, got %v", result.Kind)
	}
	expected := "Authentication failed for provider 'unknown'. Check your API key."
	if result.UserMessage != expected {
		t.Errorf("expected %q, got %q", expected, result.UserMessage)
	}
}
