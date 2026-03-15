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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldTryNext(t *testing.T) {
	tests := []struct {
		name      string
		condition ChainCondition
		err       error
		expected  bool
	}{
		{
			name:      "always condition with no error",
			condition: ChainConditionAlways,
			err:       nil,
			expected:  true,
		},
		{
			name:      "always condition with error",
			condition: ChainConditionAlways,
			err:       errors.New("some error"),
			expected:  true,
		},
		{
			name:      "on_error condition with error",
			condition: ChainConditionOnError,
			err:       errors.New("some error"),
			expected:  true,
		},
		{
			name:      "on_error condition with no error",
			condition: ChainConditionOnError,
			err:       nil,
			expected:  false,
		},
		{
			name:      "on_timeout condition with timeout error",
			condition: ChainConditionOnTimeout,
			err:       context.DeadlineExceeded,
			expected:  true,
		},
		{
			name:      "on_timeout condition with non-timeout error",
			condition: ChainConditionOnTimeout,
			err:       errors.New("some error"),
			expected:  false,
		},
		{
			name:      "on_rate_limit condition with rate limit error",
			condition: ChainConditionOnRateLimit,
			err:       errors.New("429 Too Many Requests"),
			expected:  true,
		},
		{
			name:      "on_rate_limit condition with non-rate-limit error",
			condition: ChainConditionOnRateLimit,
			err:       errors.New("some error"),
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldTryNext(tt.condition, tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsTimeoutErr(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "wrapped deadline exceeded",
			err:      errors.New("failed: " + context.DeadlineExceeded.Error()),
			expected: false, // errors.Is checks for wrapped errors, not string matching
		},
		{
			name:     "generic error",
			err:      errors.New("some error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTimeoutError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// mockNetError implements net.Error for testing
type mockNetError struct {
	timeout bool
}

func (e *mockNetError) Error() string   { return "mock net error" }
func (e *mockNetError) Timeout() bool   { return e.timeout }
func (e *mockNetError) Temporary() bool { return false }

var _ net.Error = (*mockNetError)(nil)

func TestIsTimeoutErr_NetErr(t *testing.T) {
	t.Run("net.Error with timeout", func(t *testing.T) {
		err := &mockNetError{timeout: true}
		assert.True(t, isTimeoutError(err))
	})

	t.Run("net.Error without timeout", func(t *testing.T) {
		err := &mockNetError{timeout: false}
		assert.False(t, isTimeoutError(err))
	})
}

func TestIsRateLimitErr(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "rate limit in message",
			err:      errors.New("rate limit exceeded"),
			expected: true,
		},
		{
			name:     "429 status code",
			err:      errors.New("HTTP 429 error"),
			expected: true,
		},
		{
			name:     "too many requests",
			err:      errors.New("too many requests"),
			expected: true,
		},
		{
			name:     "rate_limit underscore",
			err:      errors.New("rate_limit_exceeded"),
			expected: true,
		},
		{
			name:     "generic error",
			err:      errors.New("connection refused"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRateLimitError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestResolveGeneratorOrChain(t *testing.T) {
	t.Run("with chain provided", func(t *testing.T) {
		chain := []ChainLink{
			{Generator: GeneratorConfig{Provider: GeneratorProviderOllama}},
			{Generator: GeneratorConfig{Provider: GeneratorProviderOpenai}},
		}
		result := ResolveGeneratorOrChain(GeneratorConfig{}, chain)
		require.Len(t, result, 2)
		assert.Equal(t, GeneratorProviderOllama, result[0].Generator.Provider)
		assert.Equal(t, GeneratorProviderOpenai, result[1].Generator.Provider)
	})

	t.Run("with generator provided", func(t *testing.T) {
		gen := GeneratorConfig{Provider: GeneratorProviderAnthropic}
		result := ResolveGeneratorOrChain(gen, nil)
		require.Len(t, result, 1)
		assert.Equal(t, GeneratorProviderAnthropic, result[0].Generator.Provider)
	})

	t.Run("chain takes precedence", func(t *testing.T) {
		gen := GeneratorConfig{Provider: GeneratorProviderAnthropic}
		chain := []ChainLink{
			{Generator: GeneratorConfig{Provider: GeneratorProviderOllama}},
		}
		result := ResolveGeneratorOrChain(gen, chain)
		require.Len(t, result, 1)
		assert.Equal(t, GeneratorProviderOllama, result[0].Generator.Provider)
	})
}

func TestExecuteChain_EmptyChain(t *testing.T) {
	_, err := ExecuteChain(context.Background(), []ChainLink{}, func(ctx context.Context, gen *GenKitModelImpl) (string, error) {
		return "success", nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chain is empty")
}

func TestExecuteChain_ContextCancellation(t *testing.T) {
	// Create a chain with retry that would take a long time
	maxAttempts := 5
	initialBackoff := 1000 // 1 second
	chain := []ChainLink{
		{
			Generator: GeneratorConfig{Provider: "invalid_provider"},
			Retry: &RetryConfig{
				MaxAttempts:      &maxAttempts,
				InitialBackoffMs: &initialBackoff,
			},
		},
	}

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := ExecuteChain(ctx, chain, func(ctx context.Context, gen *GenKitModelImpl) (string, error) {
		return "", errors.New("should not reach here")
	})
	elapsed := time.Since(start)

	// Should fail quickly due to context cancellation (in the generator creation)
	// or the retry wait should be interrupted
	assert.Less(t, elapsed, 500*time.Millisecond)
	// Error could be from generator creation or context cancellation
	require.Error(t, err)
}
