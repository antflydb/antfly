// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package sim

import (
	"context"
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
		})
	}
}
