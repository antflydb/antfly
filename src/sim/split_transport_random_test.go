package sim

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRandomScenario_WithSplitTransportFaultActions(t *testing.T) {
	actions := []ScenarioAction{
		ActionWrite,
		ActionWrite,
		ActionWrite,
		ActionWrite,
		ActionReallocate,
		ActionSetLinkLatency,
		ActionDropNextMsg,
		ActionDuplicateNextMsg,
		ActionTick,
		ActionResetLinkLatency,
		ActionTick,
	}

	result, err := RunRandomScenarioWithActions(context.Background(), RandomScenarioConfig{
		Seed:                     401,
		BaseDir:                  filepath.Join(t.TempDir(), "docs-split-transport"),
		Start:                    time.Unix(1_700_900_000, 0).UTC(),
		Steps:                    len(actions),
		MaxShardSizeBytes:        256,
		MaxShardsPerTable:        2,
		SplitTimeout:             30 * time.Second,
		SplitFinalizeGracePeriod: time.Second,
		ActionSettle:             1200 * time.Millisecond,
		StabilizeEvery:           len(actions) + 1,
		PreferSplits:             true,
	}, ScenarioRecord{
		Kind:    ScenarioKindDocuments,
		Seed:    401,
		Actions: actions,
	})
	require.NoError(t, err)
	require.Equal(t, actions, result.Actions)
	require.GreaterOrEqual(t, result.FinalShards, 2)
	require.True(t, hasEventKind(result.Events, "split"))
	requireTransportFaultEvents(t, result.Events, ActionSetLinkLatency, ActionDropNextMsg, ActionDuplicateNextMsg)
}

func TestRandomScenario_PreferSplitSeedsReplayable(t *testing.T) {
	seeds := []int64{411, 419}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			t.Logf("seed=%d", seed)
			cfg := RandomScenarioConfig{
				Seed:                     seed,
				BaseDir:                  filepath.Join(t.TempDir(), fmt.Sprintf("split-seed-%d", seed)),
				Start:                    time.Unix(1_701_000_000+seed, 0).UTC(),
				Steps:                    20,
				MaxShardSizeBytes:        256,
				MaxShardsPerTable:        3,
				SplitTimeout:             30 * time.Second,
				SplitFinalizeGracePeriod: time.Second,
				ActionSettle:             1200 * time.Millisecond,
				StabilizeEvery:           5,
				PreferSplits:             true,
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
				require.True(t, strings.Contains(runErr.Trace, "[split]"))
				actions = runErr.Actions
			}

			replayCfg := cfg
			replayCfg.BaseDir = filepath.Join(t.TempDir(), fmt.Sprintf("split-replay-seed-%d", seed))
			replay, replayErr := RunRandomScenarioWithActions(context.Background(), replayCfg, ScenarioRecord{
				Kind:    ScenarioKindDocuments,
				Seed:    seed,
				Actions: actions,
			})
			if replayErr == nil {
				require.NotNil(t, replay)
				require.Equal(t, actions, replay.Actions)
				require.True(t, hasEventKind(replay.Events, "split"))
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

func hasEventKind(events []TraceEvent, kind string) bool {
	for _, event := range events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}
