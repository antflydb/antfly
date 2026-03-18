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
	"errors"
	"fmt"

	openai "github.com/openai/openai-go"
	openrouter "github.com/revrost/go-openrouter"
)

// GenerationErrorKind categorizes an LLM generation error.
type GenerationErrorKind int

const (
	GenerationErrorUnknown GenerationErrorKind = iota
	GenerationErrorAuth
	GenerationErrorQuotaExceeded
	GenerationErrorModelNotFound
	GenerationErrorRateLimit
	GenerationErrorTimeout
	GenerationErrorServer
)

// ClassifiedError holds a user-friendly message and the error kind.
type ClassifiedError struct {
	Kind        GenerationErrorKind
	UserMessage string
}

// ClassifyGenerationError inspects the error chain for known provider SDK types
// and returns a user-friendly message. The provider parameter is used to give
// context in the message (e.g. "openrouter", "openai").
func ClassifyGenerationError(provider string, err error) ClassifiedError {
	if err == nil {
		return ClassifiedError{Kind: GenerationErrorUnknown, UserMessage: ""}
	}

	if provider == "" {
		provider = "unknown"
	}

	// 1. OpenRouter APIError
	var orAPIErr *openrouter.APIError
	if errors.As(err, &orAPIErr) {
		return classifyByStatusCode(provider, orAPIErr.HTTPStatusCode)
	}

	// 2. OpenRouter RequestError
	var orReqErr *openrouter.RequestError
	if errors.As(err, &orReqErr) {
		return classifyByStatusCode(provider, orReqErr.HTTPStatusCode)
	}

	// 3. OpenAI Error (alias for apierror.Error)
	var oaiErr *openai.Error
	if errors.As(err, &oaiErr) {
		return classifyByStatusCode(provider, oaiErr.StatusCode)
	}

	// 4. Timeout
	if isTimeoutError(err) {
		return ClassifiedError{
			Kind:        GenerationErrorTimeout,
			UserMessage: fmt.Sprintf("Request to provider '%s' timed out.", provider),
		}
	}

	// 5. Rate limit (pre-existing string-based detection in chain.go)
	if isRateLimitError(err) {
		return ClassifiedError{
			Kind:        GenerationErrorRateLimit,
			UserMessage: fmt.Sprintf("Rate limit reached for provider '%s'. Please wait and try again.", provider),
		}
	}

	// 6. Unknown — pass through the inner message for context
	return ClassifiedError{
		Kind:        GenerationErrorUnknown,
		UserMessage: fmt.Sprintf("Generation failed (provider '%s'): %s", provider, err.Error()),
	}
}

func classifyByStatusCode(provider string, statusCode int) ClassifiedError {
	switch {
	case statusCode == 401 || statusCode == 403:
		return ClassifiedError{
			Kind:        GenerationErrorAuth,
			UserMessage: fmt.Sprintf("Authentication failed for provider '%s'. Check your API key.", provider),
		}
	case statusCode == 402:
		return ClassifiedError{
			Kind:        GenerationErrorQuotaExceeded,
			UserMessage: fmt.Sprintf("Quota exceeded for provider '%s'. Check your billing or usage limits.", provider),
		}
	case statusCode == 404:
		return ClassifiedError{
			Kind:        GenerationErrorModelNotFound,
			UserMessage: fmt.Sprintf("Model not found on provider '%s'. Check your model name.", provider),
		}
	case statusCode == 429:
		return ClassifiedError{
			Kind:        GenerationErrorRateLimit,
			UserMessage: fmt.Sprintf("Rate limit reached for provider '%s'. Please wait and try again.", provider),
		}
	case statusCode >= 500:
		return ClassifiedError{
			Kind:        GenerationErrorServer,
			UserMessage: fmt.Sprintf("Provider '%s' returned a server error. The provider may be experiencing issues.", provider),
		}
	default:
		return ClassifiedError{
			Kind:        GenerationErrorUnknown,
			UserMessage: fmt.Sprintf("Generation failed (provider '%s', status %d).", provider, statusCode),
		}
	}
}

