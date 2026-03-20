package sim

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyFailure_SplitTraceMetadataMismatch(t *testing.T) {
	err := errors.New("events:\n  [split] shard=2600\nshard 2600 range mismatch: metadata status=[,ff) table=[,302f)")
	require.Equal(t, FailureCategorySplitLiveness, ClassifyFailure(err))
}

func TestClassifyFailure_SplitTraceParentStillActive(t *testing.T) {
	err := errors.New("events:\n  [split] shard=2600\nparent split still active in phase PHASE_SPLITTING")
	require.Equal(t, FailureCategorySplitLiveness, ClassifyFailure(err))
}

func TestClassifyFailure_SplitTraceStatusRefreshTimeout(t *testing.T) {
	err := errors.New(
		"events:\n  [split] shard=4000\n" +
			"digests:\n  note=step-04-drop_next_msg active_splits=2\n" +
			"updating store statuses: saving store and shard statuses: operation did not complete within 5m0s",
	)
	require.Equal(t, FailureCategorySplitLiveness, ClassifyFailure(err))
}
