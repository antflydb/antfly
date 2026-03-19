package sim

import (
	"errors"
	"fmt"
	"strings"
)

type FailureCategory string

const (
	FailureCategoryUnknown           FailureCategory = "unknown"
	FailureCategoryLeaderUnavailable FailureCategory = "leader_unavailable"
	FailureCategoryProposalDropped   FailureCategory = "proposal_dropped"
	FailureCategoryLookupMismatch    FailureCategory = "lookup_mismatch"
	FailureCategorySplitLiveness     FailureCategory = "split_liveness_timeout"
	FailureCategorySplitStillActive  FailureCategory = "split_still_active"
	FailureCategorySnapshotTransfer  FailureCategory = "snapshot_transfer"
	FailureCategoryTxnUnresolved     FailureCategory = "txn_unresolved"
	FailureCategoryTxnOrphanedIntent FailureCategory = "txn_orphaned_intent"
	FailureCategoryTxnStuckPending   FailureCategory = "txn_stuck_pending"
	FailureCategoryTxnStatusMismatch FailureCategory = "txn_status_mismatch"
	FailureCategoryStoreStopped      FailureCategory = "store_stopped"
)

type ScenarioRunError struct {
	Kind           ScenarioKind
	Seed           int64
	Action         string
	Category       FailureCategory
	Actions        []ScenarioAction
	ReducedActions []ScenarioAction
	Trace          string
	Cause          error
}

func (e *ScenarioRunError) Error() string {
	if e == nil {
		return "<nil>"
	}
	base := fmt.Sprintf(
		"%s scenario seed=%d action=%s category=%s actions=%v",
		e.Kind,
		e.Seed,
		e.Action,
		e.Category,
		e.Actions,
	)
	if len(e.ReducedActions) > 0 {
		base += fmt.Sprintf(" reduced=%v", e.ReducedActions)
	}
	if e.Trace != "" {
		base += "\n" + e.Trace
	}
	if e.Cause != nil {
		base += ": " + e.Cause.Error()
	}
	return base
}

func (e *ScenarioRunError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func ClassifyFailure(err error) FailureCategory {
	if err == nil {
		return FailureCategoryUnknown
	}
	var runErr *ScenarioRunError
	if errors.As(err, &runErr) && runErr.Category != "" {
		return runErr.Category
	}
	msg := err.Error()
	if looksLikeSplitFailure(msg) {
		return FailureCategorySplitLiveness
	}
	switch {
	case strings.Contains(msg, "no leader elected for shard"),
		strings.Contains(msg, "no healthy leader found"),
		strings.Contains(msg, "leader for shard"):
		return FailureCategoryLeaderUnavailable
	case strings.Contains(msg, "raft proposal dropped"):
		return FailureCategoryProposalDropped
	case strings.Contains(msg, "lookup mismatch"),
		strings.Contains(msg, "missing key"),
		strings.Contains(msg, "returned no value"):
		return FailureCategoryLookupMismatch
	case strings.Contains(msg, "split state remained active"):
		return FailureCategorySplitLiveness
	case strings.Contains(msg, "still has active split state"):
		return FailureCategorySplitStillActive
	case strings.Contains(msg, "snapshot not found"),
		strings.Contains(msg, "reading snapshot"),
		strings.Contains(msg, "copying snapshot"),
		strings.Contains(msg, "opening snapshot store"):
		return FailureCategorySnapshotTransfer
	case strings.Contains(msg, "initializing transaction"),
		strings.Contains(msg, "committing transaction"),
		strings.Contains(msg, "resolving intent"):
		return FailureCategoryTxnUnresolved
	case strings.Contains(msg, "orphaned intents"):
		return FailureCategoryTxnOrphanedIntent
	case strings.Contains(msg, "pending transaction"),
		strings.Contains(msg, "pending coordinator record"):
		return FailureCategoryTxnStuckPending
	case strings.Contains(msg, "resolved_participants="),
		strings.Contains(msg, "finalized with status="):
		return FailureCategoryTxnStatusMismatch
	case strings.Contains(msg, "raft: stopped"):
		return FailureCategoryStoreStopped
	default:
		return FailureCategoryUnknown
	}
}

func ClassifyFailureWithTrace(err error, trace string) FailureCategory {
	category := ClassifyFailure(err)
	if category != FailureCategoryUnknown || strings.TrimSpace(trace) == "" {
		return category
	}
	return ClassifyFailure(fmt.Errorf("%s: %w", trace, err))
}

func looksLikeSplitFailure(msg string) bool {
	if !strings.Contains(msg, "[split]") &&
		!strings.Contains(msg, "PHASE_SPLITTING") &&
		!strings.Contains(msg, "SplitOffPreSnap") &&
		!strings.Contains(msg, "splitOffShardIsReady") &&
		!strings.Contains(msg, "active_splits=") {
		return false
	}
	return strings.Contains(msg, "range mismatch") ||
		strings.Contains(msg, "split did not recover") ||
		strings.Contains(msg, "parent split still active") ||
		strings.Contains(msg, "table still has 1 shards") ||
		strings.Contains(msg, "child shard metadata not published") ||
		strings.Contains(msg, "references missing split child") ||
		strings.Contains(msg, "active split state without child shard id") ||
		strings.Contains(msg, "metadata leader unavailable") ||
		strings.Contains(msg, "metadata leader not elected within") ||
		strings.Contains(msg, "saving store and shard statuses") ||
		strings.Contains(msg, "operation did not complete within")
}
