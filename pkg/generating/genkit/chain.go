package genkit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	generating "github.com/antflydb/antfly/pkg/generating"
	"github.com/sethvargo/go-retry"
)

// ExecuteChain tries each generator in the chain according to conditions.
// Returns the result from the first successful generator.
func ExecuteChain[T any](
	ctx context.Context,
	chain []generating.ChainLink,
	fn func(ctx context.Context, model *Model) (T, error),
) (T, error) {
	return ExecuteChainWith(ctx, chain, NewModel, fn)
}

// ExecuteChainWith is like ExecuteChain but allows the model constructor to be injected for tests.
func ExecuteChainWith[T any](
	ctx context.Context,
	chain []generating.ChainLink,
	newModel func(context.Context, generating.GeneratorConfig) (*Model, error),
	fn func(ctx context.Context, model *Model) (T, error),
) (T, error) {
	var zero T
	if len(chain) == 0 {
		return zero, errors.New("chain is empty")
	}

	var lastErr error

	for i, link := range chain {
		model, err := newModel(ctx, link.Generator)
		if err != nil {
			lastErr = fmt.Errorf("chain[%d]: failed to create generator: %w", i, err)
		} else {
			result, execErr := executeWithRetry(ctx, model, link.Retry, fn)
			if execErr == nil {
				return result, nil
			}
			lastErr = execErr
		}

		if i < len(chain)-1 {
			condition := generating.ChainConditionOnError
			if link.Condition != nil {
				condition = *link.Condition
			}

			if !shouldTryNext(condition, lastErr) {
				return zero, lastErr
			}
		}
	}

	return zero, fmt.Errorf("all generators in chain failed: %w", lastErr)
}

func executeWithRetry[T any](
	ctx context.Context,
	model *Model,
	retryCfg *generating.RetryConfig,
	fn func(ctx context.Context, model *Model) (T, error),
) (T, error) {
	var zero T

	if retryCfg == nil || retryCfg.MaxAttempts == nil || *retryCfg.MaxAttempts <= 1 {
		return fn(ctx, model)
	}

	initialBackoff := time.Second
	if retryCfg.InitialBackoffMs != nil {
		initialBackoff = time.Duration(*retryCfg.InitialBackoffMs) * time.Millisecond
	}

	maxBackoff := 30 * time.Second
	if retryCfg.MaxBackoffMs != nil {
		maxBackoff = time.Duration(*retryCfg.MaxBackoffMs) * time.Millisecond
	}

	b := retry.NewExponential(initialBackoff)
	b = retry.WithMaxRetries(uint64(*retryCfg.MaxAttempts-1), b)
	b = retry.WithCappedDuration(maxBackoff, b)
	b = retry.WithJitter(initialBackoff/10, b)

	var result T
	err := retry.Do(ctx, b, func(ctx context.Context) error {
		var fnErr error
		result, fnErr = fn(ctx, model)
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

func shouldTryNext(condition generating.ChainCondition, err error) bool {
	switch condition {
	case generating.ChainConditionAlways:
		return true
	case generating.ChainConditionOnError:
		return err != nil
	case generating.ChainConditionOnTimeout:
		return isTimeoutError(err)
	case generating.ChainConditionOnRateLimit:
		return isRateLimitError(err)
	default:
		return err != nil
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
