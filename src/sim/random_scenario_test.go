package sim

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRandomScenario_ReplayableSeedsRemainConsistent(t *testing.T) {
	seeds := []int64{11, 29, 47}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			t.Logf("seed=%d", seed)
			cfg := RandomScenarioConfig{
				Seed:                     seed,
				BaseDir:                  filepath.Join(t.TempDir(), fmt.Sprintf("seed-%d", seed)),
				Start:                    time.Unix(1_700_300_000+seed, 0).UTC(),
				Steps:                    28,
				SplitTimeout:             30 * time.Second,
				SplitFinalizeGracePeriod: time.Second,
				ActionSettle:             1200 * time.Millisecond,
				StabilizeEvery:           5,
			}
			result, err := RunRandomScenario(context.Background(), cfg)

			var actions []ScenarioAction
			if err == nil {
				require.NotNil(t, result)
				require.Equal(t, seed, result.Seed)
				require.NotEmpty(t, result.Trace)
				require.NotEmpty(t, result.Events)
				require.NotEmpty(t, result.Digests)
				actions = result.Actions
			} else {
				var runErr *ScenarioRunError
				require.ErrorAs(t, err, &runErr)
				require.Equal(t, seed, runErr.Seed)
				require.NotEmpty(t, runErr.Actions)
				actions = runErr.Actions
			}

			replayCfg := cfg
			replayCfg.BaseDir = filepath.Join(t.TempDir(), fmt.Sprintf("replay-seed-%d", seed))
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
				require.Equal(t, firstErr.Category, secondErr.Category)
			}
		})
	}
}
