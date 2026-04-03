package db

import (
	"testing"

	"github.com/antflydb/antfly/src/common"
	"github.com/stretchr/testify/require"
)

func TestNewShardInfo_UsesTrueIdentityForAndMergedReadinessFields(t *testing.T) {
	base := NewShardInfo()
	require.True(t, base.HasSnapshot)
	require.True(t, base.SplitReplayCaughtUp)
	require.True(t, base.SplitCutoverReady)

	reported := NewShardInfo()
	reported.HasSnapshot = false
	reported.SplitReplayCaughtUp = false
	reported.SplitCutoverReady = false

	base.Merge(1, reported)

	require.False(t, base.HasSnapshot)
	require.False(t, base.SplitReplayCaughtUp)
	require.False(t, base.SplitCutoverReady)
}

func TestShardInfoMerge_PreservesReplicatedTransitionStateFromReplicaReports(t *testing.T) {
	base := NewShardInfo()

	splitState := &SplitState{}
	splitState.SetPhase(SplitState_PHASE_PREPARE)
	splitState.SetSplitKey([]byte("m"))

	mergeState := &MergeState{}
	mergeState.SetPhase(MergeState_PHASE_PREPARE)
	mergeState.SetDonorShardId(2)
	mergeState.SetReceiverShardId(1)
	mergeState.SetAcceptDonorRange(true)

	reported := NewShardInfo()
	reported.RaftStatus = &common.RaftStatus{Lead: 17}
	reported.SplitState = splitState
	reported.MergeState = mergeState

	base.Merge(21, reported)

	require.NotNil(t, base.SplitState)
	require.Equal(t, SplitState_PHASE_PREPARE, base.SplitState.GetPhase())
	require.NotNil(t, base.MergeState)
	require.Equal(t, MergeState_PHASE_PREPARE, base.MergeState.GetPhase())
	require.True(t, base.MergeState.GetAcceptDonorRange())
}

func TestShardInfoIsReadyForMergeCutover_DoesNotRequireSnapshot(t *testing.T) {
	state := &MergeState{}
	state.SetPhase(MergeState_PHASE_FINALIZING)
	state.SetCopyCompleted(true)
	state.SetReplaySeq(7)
	state.SetFinalSeq(7)

	info := &ShardInfo{
		RaftStatus: &common.RaftStatus{Lead: 1},
		MergeState: state,
	}

	require.True(t, info.IsReadyForMergeCutover())
}

func TestShardInfoDeepCopy_PreservesLocalReadinessFields(t *testing.T) {
	original := &ShardInfo{
		HasSnapshot:         true,
		Initializing:        true,
		Splitting:           true,
		SplitReplayRequired: true,
		SplitCutoverReady:   true,
		SplitParentShardID:  9,
		RaftStatus:          &common.RaftStatus{Lead: 3},
	}

	copy := original.DeepCopy()
	require.NotNil(t, copy)
	require.Equal(t, original.HasSnapshot, copy.HasSnapshot)
	require.Equal(t, original.Initializing, copy.Initializing)
	require.Equal(t, original.Splitting, copy.Splitting)
	require.Equal(t, original.SplitReplayRequired, copy.SplitReplayRequired)
	require.Equal(t, original.SplitCutoverReady, copy.SplitCutoverReady)
	require.Equal(t, original.SplitParentShardID, copy.SplitParentShardID)
	require.NotNil(t, copy.RaftStatus)
	require.Equal(t, original.RaftStatus.Lead, copy.RaftStatus.Lead)
}
