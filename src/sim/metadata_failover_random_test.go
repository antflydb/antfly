package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/stretchr/testify/require"
)

const metadataFailoverSplitSeedHelperEnv = "ANTFLY_SIM_METADATA_FAILOVER_SPLIT_SEED"

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
	if seedValue := os.Getenv(metadataFailoverSplitSeedHelperEnv); seedValue != "" {
		seed, err := strconv.ParseInt(seedValue, 10, 64)
		require.NoError(t, err)
		runMetadataFailoverSplitSeed(t, seed)
		return
	}

	seeds := []int64{521, 523, 541}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			runMetadataFailoverSplitSeedInSubprocess(t, seed)
		})
	}
}

func runMetadataFailoverSplitSeedInSubprocess(t *testing.T, seed int64) {
	t.Helper()
	t.Logf("seed=%d", seed)

	cmd := exec.Command(os.Args[0], "-test.run=^TestRandomScenario_MetadataFailoverSplitSeedsReplayable$")
	cmd.Env = append(
		os.Environ(),
		fmt.Sprintf("%s=%d", metadataFailoverSplitSeedHelperEnv, seed),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("seed=%d subprocess failed: %v\n%s", seed, err, output)
	}
}

func runMetadataFailoverSplitSeed(t *testing.T, seed int64) {
	t.Helper()
	t.Logf("seed=%d", seed)

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
	logScenarioAttempt(t, "initial", err)

	var actions []ScenarioAction
	if err == nil {
		require.NotNil(t, result)
		require.Equal(t, seed, result.Seed)
		require.NotEmpty(t, result.Events)
		require.True(t, hasEventKind(result.Events, "split"))
		actions = result.Actions
		t.Logf("initial succeeded: actions=%v finalShards=%d", actions, result.FinalShards)
	} else {
		var runErr *ScenarioRunError
		require.ErrorAs(t, err, &runErr)
		require.Equal(t, seed, runErr.Seed)
		require.NotEmpty(t, runErr.Actions)
		actions = runErr.Actions
		t.Logf("initial failed: category=%s action=%s actions=%v reduced=%v", runErr.Category, runErr.Action, runErr.Actions, runErr.ReducedActions)
	}

	replayCfg := cfg
	replayCfg.BaseDir = filepath.Join(t.TempDir(), fmt.Sprintf("split-failover-replay-seed-%d", seed))
	t.Logf("replay actions=%v", actions)
	replay, replayErr := RunRandomScenarioWithActions(context.Background(), replayCfg, ScenarioRecord{
		Kind:    ScenarioKindDocuments,
		Seed:    seed,
		Actions: actions,
	})
	logScenarioAttempt(t, "replay", replayErr)

	if replayErr == nil {
		require.NotNil(t, replay)
		require.Equal(t, actions, replay.Actions)
		t.Logf("replay succeeded: finalShards=%d", replay.FinalShards)
	} else {
		var replayRunErr *ScenarioRunError
		require.ErrorAs(t, replayErr, &replayRunErr)
		require.Equal(t, actions, replayRunErr.Actions)
		t.Logf("replay failed: category=%s action=%s actions=%v reduced=%v", replayRunErr.Category, replayRunErr.Action, replayRunErr.Actions, replayRunErr.ReducedActions)
	}

	if err != nil && replayErr != nil {
		var firstErr *ScenarioRunError
		var secondErr *ScenarioRunError
		require.True(t, errors.As(err, &firstErr))
		require.True(t, errors.As(replayErr, &secondErr))
		require.Equal(t, firstErr.Actions, secondErr.Actions)
	}
}

func logScenarioAttempt(t *testing.T, label string, err error) {
	t.Helper()
	if err == nil {
		t.Logf("%s run completed without error", label)
		return
	}
	var runErr *ScenarioRunError
	if errors.As(err, &runErr) {
		t.Logf("%s run error: category=%s action=%s cause=%v", label, runErr.Category, runErr.Action, runErr.Cause)
		if runErr.Trace != "" {
			t.Logf("%s trace:\n%s", label, runErr.Trace)
		}
		return
	}
	t.Logf("%s run error: %v", label, err)
}
