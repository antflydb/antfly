package sim

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/stretchr/testify/require"
)

func TestRandomScenario_WithMetadataFailoverActions(t *testing.T) {
	actions := []ScenarioAction{
		ActionWrite,
		ActionWrite,
		ActionWrite,
		ActionWrite,
		ActionReallocate,
		ActionTick,
		ActionMetadataCrash,
		ActionTick,
		ActionMetadataRestart,
		ActionTick,
	}

	result, err := RunRandomScenarioWithActions(context.Background(), RandomScenarioConfig{
		Seed:           521,
		BaseDir:        filepath.Join(t.TempDir(), "split-metadata-failover"),
		Start:          time.Unix(1_701_100_000, 0).UTC(),
		MetadataIDs:    []types.ID{100, 101, 102},
		Steps:          len(actions),
		ActionSettle:   1200 * time.Millisecond,
		StabilizeEvery: len(actions) + 1,
	}, ScenarioRecord{
		Kind:    ScenarioKindDocuments,
		Seed:    521,
		Actions: actions,
	})
	require.NoError(t, err)
	require.Equal(t, actions, result.Actions)
	require.True(t, hasEventKind(result.Events, "metadata_crash"))
	require.True(t, hasEventKind(result.Events, "metadata_restart"))
}

func TestRandomScenario_MetadataFailoverSplitSeedsReplayable(t *testing.T) {
	seeds := []int64{521, 523, 541}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			cfg := RandomScenarioConfig{
				Seed:                        seed,
				BaseDir:                     filepath.Join(t.TempDir(), fmt.Sprintf("split-failover-seed-%d", seed)),
				Start:                       time.Unix(1_701_200_000+seed, 0).UTC(),
				MetadataIDs:                 []types.ID{100, 101, 102},
				Steps:                       22,
				MaxShardSizeBytes:           256,
				MaxShardsPerTable:           3,
				SplitTimeout:                30 * time.Second,
				SplitFinalizeGracePeriod:    time.Second,
				CheckerSplitLivenessTimeout: 90 * time.Second,
				StabilizeTimeout:            180 * time.Second,
				ActionSettle:                1200 * time.Millisecond,
				StabilizeEvery:              5,
				PreferSplits:                true,
			}

			result, err := RunRandomScenario(context.Background(), cfg)

			var actions []ScenarioAction
			if err == nil {
				require.NotNil(t, result)
				require.Equal(t, seed, result.Seed)
				require.NotEmpty(t, result.Events)
				require.True(t, hasEventKind(result.Events, "split"))
				actions = result.Actions
			} else {
				var runErr *ScenarioRunError
				require.ErrorAs(t, err, &runErr)
				require.Equal(t, seed, runErr.Seed)
				require.NotEmpty(t, runErr.Actions)
				actions = runErr.Actions
			}

			replayCfg := cfg
			replayCfg.BaseDir = filepath.Join(t.TempDir(), fmt.Sprintf("split-failover-replay-seed-%d", seed))
			replay, replayErr := RunRandomScenarioWithActions(context.Background(), replayCfg, ScenarioRecord{
				Kind:    ScenarioKindDocuments,
				Seed:    seed,
				Actions: actions,
			})
			if replayErr == nil {
				require.NotNil(t, replay)
				require.Equal(t, actions, replay.Actions)
			} else {
				var replayRunErr *ScenarioRunError
				require.ErrorAs(t, replayErr, &replayRunErr)
				require.Equal(t, actions, replayRunErr.Actions)
			}

			if err != nil && replayErr != nil {
				var firstErr *ScenarioRunError
				var secondErr *ScenarioRunError
				require.True(t, errors.As(err, &firstErr))
				require.True(t, errors.As(replayErr, &secondErr))
				require.Equal(t, firstErr.Actions, secondErr.Actions)
			}
		})
	}
}
