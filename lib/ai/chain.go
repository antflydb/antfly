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
	"time"

	"github.com/sethvargo/go-retry"
)

// ExecuteChain tries each generator in the chain according to conditions.
// Returns the result from the first successful generator.
// Uses generics to provide type-safe results without type assertions.
func ExecuteChain[T any](
	ctx context.Context,
	chain []ChainLink,
	fn func(ctx context.Context, gen *GenKitModelImpl) (T, error),
) (T, error) {
	var zero T
	if len(chain) == 0 {
		return zero, errors.New("chain is empty")
	}

	var lastErr error

	for i, link := range chain {
		// Create generator for this link
		gen, err := NewGenKitGenerator(ctx, link.Generator)
		if err != nil {
			lastErr = fmt.Errorf("chain[%d]: failed to create generator: %w", i, err)
			continue
		}

		// Execute with retry if configured
		result, err := executeWithRetry(ctx, gen, link.Retry, fn)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Check if we should try the next generator
		if i < len(chain)-1 {
			condition := ChainConditionOnError // default
			if link.Condition != nil {
				condition = *link.Condition
			}

			if !shouldTryNext(condition, err) {
				return zero, err
			}
		}
	}

	return zero, fmt.Errorf("all generators in chain failed: %w", lastErr)
}

// executeWithRetry handles retry logic for a single generator using go-retry.
func executeWithRetry[T any](
	ctx context.Context,
	gen *GenKitModelImpl,
	retryCfg *RetryConfig,
	fn func(ctx context.Context, gen *GenKitModelImpl) (T, error),
) (T, error) {
	var zero T

	// If no retry config or max_attempts <= 1, just execute once
	if retryCfg == nil || retryCfg.MaxAttempts == nil || *retryCfg.MaxAttempts <= 1 {
		return fn(ctx, gen)
	}

	// Build backoff from config
	initialBackoff := time.Second // default 1s
	if retryCfg.InitialBackoffMs != nil {
		initialBackoff = time.Duration(*retryCfg.InitialBackoffMs) * time.Millisecond
	}

	maxBackoff := 30 * time.Second // default 30s
	if retryCfg.MaxBackoffMs != nil {
		maxBackoff = time.Duration(*retryCfg.MaxBackoffMs) * time.Millisecond
	}

	// Create exponential backoff
	b := retry.NewExponential(initialBackoff)
	b = retry.WithMaxRetries(uint64(*retryCfg.MaxAttempts-1), b) // -1 because first attempt isn't a retry
	b = retry.WithCappedDuration(maxBackoff, b)
	b = retry.WithJitter(initialBackoff/10, b) // Add small jitter for better distribution

	var result T
	err := retry.Do(ctx, b, func(ctx context.Context) error {
		var fnErr error
		result, fnErr = fn(ctx, gen)
		if fnErr != nil {
			return retry.RetryableError(fnErr)
		}
		return nil
	})

	if err != nil {
		return zero, err
	}
	return result, nil
}

// shouldTryNext evaluates ChainCondition against error to determine
// if the next generator in the chain should be attempted.
func shouldTryNext(condition ChainCondition, err error) bool {
	switch condition {
	case ChainConditionAlways:
		return true
	case ChainConditionOnError:
		return err != nil
	case ChainConditionOnTimeout:
		return isTimeoutError(err)
	case ChainConditionOnRateLimit:
		return isRateLimitError(err)
	default:
		return err != nil
	}
}

// isTimeoutError checks if the error is a timeout error.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// isRateLimitError checks if the error is a rate limit error.
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

// ResolveGeneratorOrChain returns an effective chain for execution.
// If a generator is provided (non-empty provider), wraps it in a single-link chain.
// If a chain is provided, returns it as-is.
// This allows handlers to use chain execution for both single generators and chains.
func ResolveGeneratorOrChain(generator GeneratorConfig, chain []ChainLink) []ChainLink {
	if len(chain) > 0 {
		return chain
	}
	// Wrap single generator in a chain
	return []ChainLink{{Generator: generator}}
}

// defaultChain is the default chain configuration, set from config at startup.
var defaultChain []ChainLink

// SetDefaultChain sets the default chain configuration.
// This should be called during config initialization.
func SetDefaultChain(chain []ChainLink) {
	defaultChain = chain
}

// GetDefaultChain returns the current default chain configuration.
func GetDefaultChain() []ChainLink {
	return defaultChain
}
