package genkit

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	generating "github.com/antflydb/antfly/pkg/generating"
)

func TestShouldTryNext(t *testing.T) {
	tests := []struct {
		name      string
		condition generating.ChainCondition
		err       error
		expected  bool
	}{
		{name: "always condition with no error", condition: generating.ChainConditionAlways, expected: true},
		{name: "always condition with error", condition: generating.ChainConditionAlways, err: errors.New("some error"), expected: true},
		{name: "on_error condition with error", condition: generating.ChainConditionOnError, err: errors.New("some error"), expected: true},
		{name: "on_error condition with no error", condition: generating.ChainConditionOnError, expected: false},
		{name: "on_timeout condition with timeout error", condition: generating.ChainConditionOnTimeout, err: context.DeadlineExceeded, expected: true},
		{name: "on_timeout condition with non-timeout error", condition: generating.ChainConditionOnTimeout, err: errors.New("some error"), expected: false},
		{name: "on_rate_limit condition with rate limit error", condition: generating.ChainConditionOnRateLimit, err: errors.New("429 Too Many Requests"), expected: true},
		{name: "on_rate_limit condition with non-rate-limit error", condition: generating.ChainConditionOnRateLimit, err: errors.New("some error"), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldTryNext(tt.condition, tt.err); got != tt.expected {
				t.Fatalf("shouldTryNext() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsTimeoutError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil error", expected: false},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, expected: true},
		{name: "wrapped deadline exceeded string only", err: errors.New("failed: " + context.DeadlineExceeded.Error()), expected: false},
		{name: "generic error", err: errors.New("some error"), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTimeoutError(tt.err); got != tt.expected {
				t.Fatalf("isTimeoutError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

type mockNetError struct {
	timeout bool
}

func (e *mockNetError) Error() string   { return "mock net error" }
func (e *mockNetError) Timeout() bool   { return e.timeout }
func (e *mockNetError) Temporary() bool { return false }

var _ net.Error = (*mockNetError)(nil)

func TestIsTimeoutErrorNetError(t *testing.T) {
	if !isTimeoutError(&mockNetError{timeout: true}) {
		t.Fatal("expected timeout net error to be detected")
	}
	if isTimeoutError(&mockNetError{timeout: false}) {
		t.Fatal("expected non-timeout net error to be ignored")
	}
}

func TestIsRateLimitError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil error", expected: false},
		{name: "rate limit in message", err: errors.New("rate limit exceeded"), expected: true},
		{name: "429 status code", err: errors.New("HTTP 429 error"), expected: true},
		{name: "too many requests", err: errors.New("too many requests"), expected: true},
		{name: "rate_limit underscore", err: errors.New("rate_limit_exceeded"), expected: true},
		{name: "generic error", err: errors.New("connection refused"), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRateLimitError(tt.err); got != tt.expected {
				t.Fatalf("isRateLimitError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExecuteChainWithEmptyChain(t *testing.T) {
	_, err := ExecuteChainWith(context.Background(), nil, nil, func(ctx context.Context, model *Model) (string, error) {
		return "success", nil
	})
	if err == nil || err.Error() != "chain is empty" {
		t.Fatalf("expected empty chain error, got %v", err)
	}
}

func TestExecuteChainWithRetriesAndFallback(t *testing.T) {
	maxAttempts := 2
	attempts := 0
	firstCalls := 0
	secondCalls := 0
	var firstModel *Model

	chain := []generating.ChainLink{
		{
			Generator: generating.GeneratorConfig{Provider: generating.GeneratorProviderOpenai},
			Retry:     &generating.RetryConfig{MaxAttempts: &maxAttempts},
		},
		{
			Generator: generating.GeneratorConfig{Provider: generating.GeneratorProviderGemini},
		},
	}

	newModel := func(ctx context.Context, cfg generating.GeneratorConfig) (*Model, error) {
		attempts++
		model := &Model{}
		if firstModel == nil {
			firstModel = model
		}
		return model, nil
	}

	got, err := ExecuteChainWith(context.Background(), chain, newModel, func(ctx context.Context, model *Model) (string, error) {
		if model == firstModel {
			firstCalls++
			return "", errors.New("transient failure")
		}
		secondCalls++
		return "success", nil
	})
	if err != nil {
		t.Fatalf("ExecuteChainWith() error = %v", err)
	}
	if got != "success" {
		t.Fatalf("ExecuteChainWith() = %q, want %q", got, "success")
	}
	if attempts != 2 {
		t.Fatalf("newModel attempts = %d, want 2", attempts)
	}
	if firstCalls != 2 {
		t.Fatalf("first link calls = %d, want 2", firstCalls)
	}
	if secondCalls != 1 {
		t.Fatalf("second link calls = %d, want 1", secondCalls)
	}
}

func TestExecuteChainWithContextCancellation(t *testing.T) {
	maxAttempts := 5
	initialBackoff := 1000
	chain := []generating.ChainLink{
		{
			Generator: generating.GeneratorConfig{Provider: generating.GeneratorProviderOpenai},
			Retry: &generating.RetryConfig{
				MaxAttempts:      &maxAttempts,
				InitialBackoffMs: &initialBackoff,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := ExecuteChainWith(ctx, chain, func(ctx context.Context, cfg generating.GeneratorConfig) (*Model, error) {
		return &Model{}, nil
	}, func(ctx context.Context, model *Model) (string, error) {
		return "", errors.New("retry me")
	})
	elapsed := time.Since(start)

	if elapsed >= 500*time.Millisecond {
		t.Fatalf("expected fast cancellation, elapsed %v", elapsed)
	}
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}
