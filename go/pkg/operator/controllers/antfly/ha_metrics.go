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

// The hot-standby admin surface is being renamed from "ha" to "standby"
// (see zig/HOT_STANDBY.md, "Naming"). Operator metrics dual-emit under both
// the legacy "ha" subsystem and the canonical "standby" subsystem for the
// deprecation window so existing dashboards and alerts keep working while
// new ones can move to the canonical names. dualCounterVec/dualHistogramVec
// hold one vector per subsystem with identical names, labels, and help
// text, and fan out every observation to both so call sites stay
// single-line and unaware of the duplication.

// dualCounterVec is a prometheus.CounterVec that writes every observation to
// a legacy "ha"-subsystem vector and a canonical "standby"-subsystem vector.
type dualCounterVec struct {
	legacy    *prometheus.CounterVec
	canonical *prometheus.CounterVec
}

func newDualCounterVec(name, help string, labels []string) *dualCounterVec {
	return &dualCounterVec{
		legacy: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "antfly_operator",
			Subsystem: "ha",
			Name:      name,
			Help:      help,
		}, labels),
		canonical: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "antfly_operator",
			Subsystem: "standby",
			Name:      name,
			Help:      help,
		}, labels),
	}
}

// WithLabelValues returns a dualCounter that increments both the legacy and
// canonical series for the given label values.
func (d *dualCounterVec) WithLabelValues(lvs ...string) dualCounter {
	return dualCounter{legacy: d.legacy.WithLabelValues(lvs...), canonical: d.canonical.WithLabelValues(lvs...)}
}

// dualCounter fans Inc/Add out to the legacy and canonical counters it wraps.
type dualCounter struct {
	legacy    prometheus.Counter
	canonical prometheus.Counter
}

func (d dualCounter) Inc() {
	d.legacy.Inc()
	d.canonical.Inc()
}

func (d dualCounter) Add(v float64) {
	d.legacy.Add(v)
	d.canonical.Add(v)
}

// dualHistogramVec is a prometheus.HistogramVec that writes every
// observation to a legacy "ha"-subsystem vector and a canonical
// "standby"-subsystem vector.
type dualHistogramVec struct {
	legacy    *prometheus.HistogramVec
	canonical *prometheus.HistogramVec
}

func newDualHistogramVec(name, help string, buckets []float64, labels []string) *dualHistogramVec {
	return &dualHistogramVec{
		legacy: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "antfly_operator",
			Subsystem: "ha",
			Name:      name,
			Help:      help,
			Buckets:   buckets,
		}, labels),
		canonical: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "antfly_operator",
			Subsystem: "standby",
			Name:      name,
			Help:      help,
			Buckets:   buckets,
		}, labels),
	}
}

// WithLabelValues returns a dualObserver that records into both the legacy
// and canonical series for the given label values.
func (d *dualHistogramVec) WithLabelValues(lvs ...string) dualObserver {
	return dualObserver{legacy: d.legacy.WithLabelValues(lvs...), canonical: d.canonical.WithLabelValues(lvs...)}
}

// dualObserver fans Observe out to the legacy and canonical observers it wraps.
type dualObserver struct {
	legacy    prometheus.Observer
	canonical prometheus.Observer
}

func (d dualObserver) Observe(v float64) {
	d.legacy.Observe(v)
	d.canonical.Observe(v)
}

var (
	haActionAttempts = newDualCounterVec(
		"action_attempts_total",
		"Number of HA control-plane action execution attempts.",
		[]string{"action", "executor"},
	)
	haActionRetries = newDualCounterVec(
		"action_retries_total",
		"Number of HA control-plane action attempts after the first attempt.",
		[]string{"action", "executor"},
	)
	haActionFailures = newDualCounterVec(
		"action_failures_total",
		"Number of retryable and terminal HA control-plane action failures.",
		[]string{"action", "executor", "class", "terminal"},
	)
	haActionWaits = newDualCounterVec(
		"action_waits_total",
		"Number of successful HA control-plane observations that remain blocked on a bounded prerequisite.",
		[]string{"action", "reason"},
	)
	haActionDuration = newDualHistogramVec(
		"action_duration_seconds",
		"Elapsed wall-clock time from the first attempt to terminal HA action completion.",
		prometheus.ExponentialBuckets(1, 2, 12),
		[]string{"action", "executor", "outcome"},
	)
	haSeedArtifactBytes = newDualHistogramVec(
		"seed_artifact_bytes",
		"Total bytes in successfully captured, published, restored, or activated HA seed artifacts.",
		prometheus.ExponentialBuckets(1024*1024, 4, 10),
		[]string{"action"},
	)
	haSeedArtifactFiles = newDualHistogramVec(
		"seed_artifact_files",
		"File count in successfully captured, published, restored, or activated HA seed artifacts.",
		prometheus.ExponentialBuckets(1, 4, 10),
		[]string{"action"},
	)
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		haActionAttempts.legacy, haActionAttempts.canonical,
		haActionRetries.legacy, haActionRetries.canonical,
		haActionFailures.legacy, haActionFailures.canonical,
		haActionWaits.legacy, haActionWaits.canonical,
		haActionDuration.legacy, haActionDuration.canonical,
		haSeedArtifactBytes.legacy, haSeedArtifactBytes.canonical,
		haSeedArtifactFiles.legacy, haSeedArtifactFiles.canonical,
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
