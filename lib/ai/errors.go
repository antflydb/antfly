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
	"net"
	"strings"

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

// GenerationError holds a user-friendly message and the error kind.
type GenerationError struct {
	Kind        GenerationErrorKind
	UserMessage string
	Cause       error
}

func (c *GenerationError) Error() string {
	return c.UserMessage
}

func (c *GenerationError) Unwrap() error {
	return c.Cause
}

// HTTPStatusCode returns the appropriate HTTP status code for this error kind.
func (c *GenerationError) HTTPStatusCode() int {
	switch c.Kind {
	case GenerationErrorAuth:
		return 401
	case GenerationErrorQuotaExceeded:
		return 402
	case GenerationErrorModelNotFound:
		return 404
	case GenerationErrorRateLimit:
		return 429
	case GenerationErrorTimeout:
		return 504
	case GenerationErrorServer:
		return 502
	default:
		return 500
	}
}

// AsGenerationError inspects the error chain for known provider SDK types
// and returns a user-friendly message. The provider parameter is used to give
// context in the message (e.g. "openrouter", "openai").
func AsGenerationError(provider string, err error) *GenerationError {
	if err == nil {
		panic("AsGenerationError called with nil error")
	}

	if provider == "" {
		provider = "unknown"
	}

	// 1. OpenRouter APIError
	var orAPIErr *openrouter.APIError
	if errors.As(err, &orAPIErr) {
		ge := generationErrorFromStatusCode(provider, orAPIErr.HTTPStatusCode)
		ge.Cause = err
		return ge
	}

	// 2. OpenRouter RequestError
	var orReqErr *openrouter.RequestError
	if errors.As(err, &orReqErr) {
		ge := generationErrorFromStatusCode(provider, orReqErr.HTTPStatusCode)
		ge.Cause = err
		return ge
	}

	// 3. OpenAI Error (alias for apierror.Error)
	var oaiErr *openai.Error
	if errors.As(err, &oaiErr) {
		ge := generationErrorFromStatusCode(provider, oaiErr.StatusCode)
		ge.Cause = err
		return ge
	}

	// 4. Timeout
	if isTimeoutError(err) {
		return &GenerationError{
			Kind:        GenerationErrorTimeout,
			UserMessage: fmt.Sprintf("Request to provider '%s' timed out.", provider),
			Cause:       err,
		}
	}

	// 5. Rate limit — catches rate limits from providers without typed SDK errors
	// (e.g. Ollama, Anthropic). Typed SDK errors with 429 are already caught above.
	if isRateLimitError(err) {
		return &GenerationError{
			Kind:        GenerationErrorRateLimit,
			UserMessage: fmt.Sprintf("Rate limit reached for provider '%s'. Please wait and try again.", provider),
			Cause:       err,
		}
	}

	// 6. Unknown — pass through the inner message for context
	return &GenerationError{
		Kind:        GenerationErrorUnknown,
		UserMessage: fmt.Sprintf("Generation failed (provider '%s'): %s", provider, err.Error()),
		Cause:       err,
	}
}

func generationErrorFromStatusCode(provider string, statusCode int) *GenerationError {
	switch {
	case statusCode == 401 || statusCode == 403:
		return &GenerationError{
			Kind:        GenerationErrorAuth,
			UserMessage: fmt.Sprintf("Authentication failed for provider '%s'. Check your API key.", provider),
		}
	case statusCode == 402:
		return &GenerationError{
			Kind:        GenerationErrorQuotaExceeded,
			UserMessage: fmt.Sprintf("Quota exceeded for provider '%s'. Check your billing or usage limits.", provider),
		}
	case statusCode == 404:
		return &GenerationError{
			Kind:        GenerationErrorModelNotFound,
			UserMessage: fmt.Sprintf("Model not found on provider '%s'. Check your model name.", provider),
		}
	case statusCode == 429:
		return &GenerationError{
			Kind:        GenerationErrorRateLimit,
			UserMessage: fmt.Sprintf("Rate limit reached for provider '%s'. Please wait and try again.", provider),
		}
	case statusCode >= 500:
		return &GenerationError{
			Kind:        GenerationErrorServer,
			UserMessage: fmt.Sprintf("Provider '%s' returned a server error. The provider may be experiencing issues.", provider),
		}
	default:
		return &GenerationError{
			Kind:        GenerationErrorUnknown,
			UserMessage: fmt.Sprintf("Generation failed (provider '%s', status %d).", provider, statusCode),
		}
	}
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "rate limit") ||
		strings.Contains(errStr, "429") ||
		strings.Contains(errStr, "too many requests") ||
		strings.Contains(errStr, "rate_limit")
}
