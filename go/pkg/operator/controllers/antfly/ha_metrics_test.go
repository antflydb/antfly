package controllers

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
)

func TestHAActionMetricsCountAttemptsAndRetriesWithoutUnboundedLabels(t *testing.T) {
	action := &antflyv1.HAPlannedActionStatus{
		Kind:         "MetricContractAction",
		AttemptCount: 3,
	}
	attempts := haActionAttempts.WithLabelValues(action.Kind, haMetricExecutorDirect)
	retries := haActionRetries.WithLabelValues(action.Kind, haMetricExecutorDirect)
	beforeAttempts := testutil.ToFloat64(attempts)
	beforeRetries := testutil.ToFloat64(retries)

	haObserveActionAttempts(action, haMetricExecutorDirect, 3)

	if got := testutil.ToFloat64(attempts) - beforeAttempts; got != 3 {
		t.Fatalf("expected three HA action attempts, got %v", got)
	}
	if got := testutil.ToFloat64(retries) - beforeRetries; got != 2 {
		t.Fatalf("expected two attempts after the first to be retries, got %v", got)
	}
}

func TestHAMetricErrorClassCollapsesUnboundedJobReasons(t *testing.T) {
	tests := map[string]string{
		"HTTP503":                     "http_5xx",
		"HTTP401":                     "http_4xx",
		"RetryBudgetExhausted":        "retry_budget_exhausted",
		"BackoffLimitExceeded":        "job_failed",
		"arbitrary admission message": "job_failed",
		"":                            "unknown",
	}
	for input, want := range tests {
		if got := haMetricErrorClass(input); got != want {
			t.Fatalf("haMetricErrorClass(%q) = %q, want %q", input, got, want)
		}
	}
}
