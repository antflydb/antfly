package controllers

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
)

const (
	haMetricExecutorDirect = "direct_api"
	haMetricExecutorJob    = "kubernetes_job"
)

var (
	haActionAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "antfly_operator",
		Subsystem: "ha",
		Name:      "action_attempts_total",
		Help:      "Number of HA control-plane action execution attempts.",
	}, []string{"action", "executor"})
	haActionRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "antfly_operator",
		Subsystem: "ha",
		Name:      "action_retries_total",
		Help:      "Number of HA control-plane action attempts after the first attempt.",
	}, []string{"action", "executor"})
	haActionFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "antfly_operator",
		Subsystem: "ha",
		Name:      "action_failures_total",
		Help:      "Number of retryable and terminal HA control-plane action failures.",
	}, []string{"action", "executor", "class", "terminal"})
	haActionWaits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "antfly_operator",
		Subsystem: "ha",
		Name:      "action_waits_total",
		Help:      "Number of successful HA control-plane observations that remain blocked on a bounded prerequisite.",
	}, []string{"action", "reason"})
	haActionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "antfly_operator",
		Subsystem: "ha",
		Name:      "action_duration_seconds",
		Help:      "Elapsed wall-clock time from the first attempt to terminal HA action completion.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"action", "executor", "outcome"})
	haSeedArtifactBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "antfly_operator",
		Subsystem: "ha",
		Name:      "seed_artifact_bytes",
		Help:      "Total bytes in successfully captured, published, restored, or activated HA seed artifacts.",
		Buckets:   prometheus.ExponentialBuckets(1024*1024, 4, 10),
	}, []string{"action"})
	haSeedArtifactFiles = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "antfly_operator",
		Subsystem: "ha",
		Name:      "seed_artifact_files",
		Help:      "File count in successfully captured, published, restored, or activated HA seed artifacts.",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 10),
	}, []string{"action"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		haActionAttempts,
		haActionRetries,
		haActionFailures,
		haActionWaits,
		haActionDuration,
		haSeedArtifactBytes,
		haSeedArtifactFiles,
	)
}

func haObserveActionWait(action *antflyv1.HAPlannedActionStatus, reason string) {
	if action == nil {
		return
	}
	haActionWaits.WithLabelValues(haMetricActionLabel(action.Kind), haMetricWaitReason(reason)).Inc()
}

func haObserveActionAttempts(action *antflyv1.HAPlannedActionStatus, executor string, count int32) {
	if action == nil || count <= 0 {
		return
	}
	actionLabel := haMetricActionLabel(action.Kind)
	haActionAttempts.WithLabelValues(actionLabel, executor).Add(float64(count))
	firstAttempt := action.AttemptCount - count + 1
	if firstAttempt < 1 {
		firstAttempt = 1
	}
	retryCount := action.AttemptCount - max(firstAttempt, 2) + 1
	if retryCount > 0 {
		haActionRetries.WithLabelValues(actionLabel, executor).Add(float64(retryCount))
	}
}

func haObserveActionFailure(action *antflyv1.HAPlannedActionStatus, executor string, terminal bool) {
	if action == nil {
		return
	}
	haActionFailures.WithLabelValues(
		haMetricActionLabel(action.Kind),
		executor,
		haMetricErrorClass(action.ErrorClass),
		strconvFormatBool(terminal),
	).Inc()
	if terminal {
		haObserveActionCompletion(action, executor, "failed")
	}
}

func haObserveActionSuccess(action *antflyv1.HAPlannedActionStatus, executor string) {
	if action == nil {
		return
	}
	haObserveActionCompletion(action, executor, "succeeded")
	if action.SeedArtifactReceipt != nil {
		haSeedArtifactBytes.WithLabelValues(haMetricActionLabel(action.Kind)).Observe(float64(action.SeedArtifactReceipt.TotalBytes))
		haSeedArtifactFiles.WithLabelValues(haMetricActionLabel(action.Kind)).Observe(float64(action.SeedArtifactReceipt.FileCount))
		return
	}
	if action.AdminResult != nil && action.AdminResult.SeedArtifactGeneration != "" {
		haSeedArtifactBytes.WithLabelValues(haMetricActionLabel(action.Kind)).Observe(float64(action.AdminResult.SeedTotalBytes))
		haSeedArtifactFiles.WithLabelValues(haMetricActionLabel(action.Kind)).Observe(float64(action.AdminResult.SeedFileCount))
	}
}

func haObserveActionCompletion(action *antflyv1.HAPlannedActionStatus, executor, outcome string) {
	if action == nil || action.FirstAttemptAt == nil || action.CompletedAt == nil {
		return
	}
	duration := action.CompletedAt.Sub(action.FirstAttemptAt.Time)
	if duration < 0 {
		duration = 0
	}
	haActionDuration.WithLabelValues(haMetricActionLabel(action.Kind), executor, outcome).Observe(duration.Seconds())
}

func haMetricActionLabel(kind string) string {
	switch label := strings.TrimSpace(kind); label {
	case string(haActionCreateSlot),
		string(haActionResumeSlot),
		string(haActionPauseSlot),
		string(haActionDropSlot),
		string(haActionSeedStandby),
		string(haActionFinishStandbySeed),
		string(haActionCaptureSeedArtifact),
		string(haActionPublishSeedArtifact),
		string(haActionRestoreSeedArtifact),
		string(haActionActivateSeedArtifact),
		string(haActionActivateSeededSlot),
		string(haActionBootstrapStandbySeed),
		string(haActionPruneSeedArtifacts),
		string(haActionMarkReseed),
		string(haActionAcquireFence),
		string(haActionAssessPromotion),
		string(haActionPromoteStandby),
		string(haActionUpdatePrimaryRoute),
		string(haActionFenceFormerPrimary),
		string(haActionIsolateFormerPrimary),
		string(haActionDemoteFormerPrimary),
		string(haActionRewindFormerPrimary),
		string(haActionReseedFormerPrimary):
		return label
	default:
		return "unknown"
	}
}

func haMetricErrorClass(class string) string {
	switch value := strings.TrimSpace(class); {
	case value == "RetryBudgetExhausted":
		return "retry_budget_exhausted"
	case value == "PromotionBoundaryNotApplied":
		return "promotion_boundary_not_applied"
	case value == "PromotionPrerequisiteTimeout":
		return "promotion_prerequisite_timeout"
	case value == "ReservationExpired":
		return "reservation_expired"
	case value == "PermanentAdminError":
		return "permanent_admin_error"
	case value == "RetryableAdminError":
		return "retryable_admin_error"
	case value == "UnsupportedAdminAction":
		return "unsupported_admin_action"
	case strings.HasPrefix(value, "HTTP4"):
		return "http_4xx"
	case strings.HasPrefix(value, "HTTP5"):
		return "http_5xx"
	case strings.HasPrefix(value, "HTTP"):
		return "http_other"
	case value == "":
		return "unknown"
	default:
		// Kubernetes Job condition reasons are intentionally collapsed to avoid
		// turning free-form controller or admission text into metric labels.
		return "job_failed"
	}
}

func haMetricWaitReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case "promotion_boundary":
		return "promotion_boundary"
	default:
		return "unknown"
	}
}

func strconvFormatBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
