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

func TestRandomTransactionScenario_ReplayableSeedsRemainConsistent(t *testing.T) {
	seeds := []int64{7, 19}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			cfg := RandomTransactionScenarioConfig{
				Seed:           seed,
				BaseDir:        filepath.Join(t.TempDir(), fmt.Sprintf("txn-seed-%d", seed)),
				Start:          time.Unix(1_700_500_000+seed, 0).UTC(),
				Steps:          16,
				ActionSettle:   time.Second,
				StabilizeEvery: 4,
			}
			result, err := RunRandomTransactionScenario(context.Background(), cfg)

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
			replayCfg.BaseDir = filepath.Join(t.TempDir(), fmt.Sprintf("txn-replay-seed-%d", seed))
			replay, replayErr := RunRandomTransactionScenarioWithActions(context.Background(), replayCfg, ScenarioRecord{
				Kind:    ScenarioKindTransactions,
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
