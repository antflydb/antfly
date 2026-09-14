package controllers

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

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
	beforeAttemptsLegacy := testutil.ToFloat64(attempts.legacy)
	beforeAttemptsCanonical := testutil.ToFloat64(attempts.canonical)
	beforeRetriesLegacy := testutil.ToFloat64(retries.legacy)
	beforeRetriesCanonical := testutil.ToFloat64(retries.canonical)

	haObserveActionAttempts(action, haMetricExecutorDirect, 3)

	// Both the deprecated "ha" subsystem series and the canonical "standby"
	// subsystem series must observe every attempt/retry during the
	// deprecation window.
	if got := testutil.ToFloat64(attempts.legacy) - beforeAttemptsLegacy; got != 3 {
		t.Fatalf("expected three legacy HA action attempts, got %v", got)
	}
	if got := testutil.ToFloat64(attempts.canonical) - beforeAttemptsCanonical; got != 3 {
		t.Fatalf("expected three canonical standby action attempts, got %v", got)
	}
	if got := testutil.ToFloat64(retries.legacy) - beforeRetriesLegacy; got != 2 {
		t.Fatalf("expected two legacy attempts after the first to be retries, got %v", got)
	}
	if got := testutil.ToFloat64(retries.canonical) - beforeRetriesCanonical; got != 2 {
		t.Fatalf("expected two canonical attempts after the first to be retries, got %v", got)
	}
}

func TestHAAndStandbyMetricsAreBothRegistered(t *testing.T) {
	// A prometheus *Vec with no instantiated label children produces no
	// samples and is therefore absent from Gather's output entirely, so
	// force one child into existence for every dual-emitted metric before
	// gathering, independent of whether any other test in this package
	// happened to run first.
	haActionAttempts.WithLabelValues("probe", "probe").Inc()
	haActionRetries.WithLabelValues("probe", "probe").Inc()
	haActionFailures.WithLabelValues("probe", "probe", "probe", "probe").Inc()
	haActionWaits.WithLabelValues("probe", "probe").Inc()
	haActionDuration.WithLabelValues("probe", "probe", "probe").Observe(0)
	haSeedArtifactBytes.WithLabelValues("probe").Observe(0)
	haSeedArtifactFiles.WithLabelValues("probe").Observe(0)

	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather returned error: %v", err)
	}
	names := map[string]bool{}
	for _, family := range families {
		names[family.GetName()] = true
	}
	for _, name := range []string{"action_attempts_total", "action_retries_total", "action_failures_total", "action_waits_total", "action_duration_seconds", "seed_artifact_bytes", "seed_artifact_files"} {
		legacy := "antfly_operator_ha_" + name
		canonical := "antfly_operator_standby_" + name
		if !names[legacy] {
			t.Errorf("legacy metric %s is not registered", legacy)
		}
		if !names[canonical] {
			t.Errorf("canonical metric %s is not registered", canonical)
		}
	}
	// Sanity check the constant prefixes used above still describe every
	// registered "ha"/"standby" subsystem operator metric, so this test
	// notices if a future metric is added to ha_metrics.go without a match
	// here.
	var haCount, standbyCount int
	for name := range names {
		switch {
		case strings.HasPrefix(name, "antfly_operator_ha_"):
			haCount++
		case strings.HasPrefix(name, "antfly_operator_standby_"):
			standbyCount++
		}
	}
	if haCount != standbyCount {
		t.Fatalf("registered %d antfly_operator_ha_* metrics but %d antfly_operator_standby_* metrics, want equal counts", haCount, standbyCount)
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
	beforeLegacy := testutil.ToFloat64(waits.legacy)
	beforeCanonical := testutil.ToFloat64(waits.canonical)

	haObserveActionWait(action, "promotion_boundary")

	if got := testutil.ToFloat64(waits.legacy) - beforeLegacy; got != 1 {
		t.Fatalf("expected one bounded legacy promotion prerequisite wait, got %v", got)
	}
	if got := testutil.ToFloat64(waits.canonical) - beforeCanonical; got != 1 {
		t.Fatalf("expected one bounded canonical promotion prerequisite wait, got %v", got)
	}
	if got := haMetricWaitReason("arbitrary runtime reason"); got != "unknown" {
		t.Fatalf("unexpected unbounded wait reason label %q", got)
	}
}
