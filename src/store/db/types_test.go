package db

import (
	"testing"

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
