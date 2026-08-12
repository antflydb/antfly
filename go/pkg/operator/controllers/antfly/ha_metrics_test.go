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
	// Unknown or future action kinds must collapse to the bounded "unknown"
	// series instead of allowing status-derived strings to create unbounded
	// Prometheus label values.
	attempts := haActionAttempts.WithLabelValues("unknown", haMetricExecutorDirect)
	retries := haActionRetries.WithLabelValues("unknown", haMetricExecutorDirect)
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
		"HTTP503":                      "http_5xx",
		"HTTP401":                      "http_4xx",
		"RetryBudgetExhausted":         "retry_budget_exhausted",
		"ReservationExpired":           "reservation_expired",
		"PromotionPrerequisiteTimeout": "promotion_prerequisite_timeout",
		"BackoffLimitExceeded":         "job_failed",
		"arbitrary admission message":  "job_failed",
		"":                             "unknown",
	}
	for input, want := range tests {
		if got := haMetricErrorClass(input); got != want {
			t.Fatalf("haMetricErrorClass(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestHAActionWaitMetricUsesBoundedReasonLabels(t *testing.T) {
	action := &antflyv1.HAPlannedActionStatus{Kind: string(haActionAssessPromotion)}
	waits := haActionWaits.WithLabelValues(action.Kind, "promotion_boundary")
	before := testutil.ToFloat64(waits)

	haObserveActionWait(action, "promotion_boundary")

	if got := testutil.ToFloat64(waits) - before; got != 1 {
		t.Fatalf("expected one bounded promotion prerequisite wait, got %v", got)
	}
	if got := haMetricWaitReason("arbitrary runtime reason"); got != "unknown" {
		t.Fatalf("unexpected unbounded wait reason label %q", got)
	}
}
