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
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type scenarioReplayFn func(context.Context, []ScenarioAction) error

var failureReductionTimeout = 30 * time.Second

func reduceScenarioActions(
	ctx context.Context,
	actions []ScenarioAction,
	category FailureCategory,
	replay scenarioReplayFn,
) ([]ScenarioAction, error) {
	if len(actions) == 0 {
		return nil, nil
	}
	if replay == nil {
		return nil, fmt.Errorf("replay function is required")
	}
	if category == "" {
		category = FailureCategoryUnknown
	}

	best := append([]ScenarioAction(nil), actions...)

	if reduced, ok, err := tryReduce(ctx, best, category, replay, func(current []ScenarioAction) [][]ScenarioAction {
		candidates := make([][]ScenarioAction, 0, len(current))
		for keep := len(current) - 1; keep >= 1; keep-- {
			candidate := append([]ScenarioAction(nil), current[:keep]...)
			candidates = append(candidates, candidate)
		}
		return candidates
	}); err != nil {
		return nil, err
	} else if ok {
		best = reduced
	}

	changed := true
	for changed {
		changed = false
		if reduced, ok, err := tryReduce(ctx, best, category, replay, func(current []ScenarioAction) [][]ScenarioAction {
			candidates := make([][]ScenarioAction, 0, len(current))
			for i := range current {
				candidate := append([]ScenarioAction(nil), current[:i]...)
				candidate = append(candidate, current[i+1:]...)
				if len(candidate) == 0 {
					continue
				}
				candidates = append(candidates, candidate)
			}
			return candidates
		}); err != nil {
			return nil, err
		} else if ok {
			best = reduced
			changed = true
		}
	}

	return best, nil
}

func tryReduce(
	ctx context.Context,
	current []ScenarioAction,
	category FailureCategory,
	replay scenarioReplayFn,
	candidatesFn func([]ScenarioAction) [][]ScenarioAction,
) ([]ScenarioAction, bool, error) {
	for _, candidate := range candidatesFn(current) {
		err := replay(ctx, candidate)
		if err == nil {
			continue
		}
		if ClassifyFailure(err) == category {
			return candidate, true, nil
		}
	}
	return nil, false, nil
}

func ReduceRandomScenarioFailure(
	ctx context.Context,
	cfg RandomScenarioConfig,
	actions []ScenarioAction,
	category FailureCategory,
) ([]ScenarioAction, error) {
	parentDir, err := os.MkdirTemp("", fmt.Sprintf("sim-reduce-%d-*", cfg.Seed))
	if err != nil {
		return nil, fmt.Errorf("creating temp dir for reduction: %w", err)
	}
	defer os.RemoveAll(parentDir)
	var attempt atomic.Int64
	return reduceScenarioActions(ctx, actions, category, func(ctx context.Context, reduced []ScenarioAction) error {
		dir := filepath.Join(parentDir, fmt.Sprintf("attempt-%d", attempt.Add(1)))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("creating reduction attempt dir: %w", err)
		}
		defer os.RemoveAll(dir)
		replayCfg := cfg
		replayCfg.BaseDir = dir
		_, err := RunRandomScenarioWithActions(ctx, replayCfg, ScenarioRecord{
			Kind:    ScenarioKindDocuments,
			Seed:    cfg.Seed,
			Actions: reduced,
		})
		return err
	})
}

func ReduceRandomTransactionScenarioFailure(
	ctx context.Context,
	cfg RandomTransactionScenarioConfig,
	actions []ScenarioAction,
	category FailureCategory,
) ([]ScenarioAction, error) {
	parentDir, err := os.MkdirTemp("", fmt.Sprintf("sim-txn-reduce-%d-*", cfg.Seed))
	if err != nil {
		return nil, fmt.Errorf("creating temp dir for reduction: %w", err)
	}
	defer os.RemoveAll(parentDir)
	var attempt atomic.Int64
	return reduceScenarioActions(ctx, actions, category, func(ctx context.Context, reduced []ScenarioAction) error {
		dir := filepath.Join(parentDir, fmt.Sprintf("attempt-%d", attempt.Add(1)))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("creating reduction attempt dir: %w", err)
		}
		defer os.RemoveAll(dir)
		replayCfg := cfg
		replayCfg.BaseDir = dir
		_, err := RunRandomTransactionScenarioWithActions(ctx, replayCfg, ScenarioRecord{
			Kind:    ScenarioKindTransactions,
			Seed:    cfg.Seed,
			Actions: reduced,
		})
		return err
	})
}

func runFailureReduction(reducer func(context.Context) ([]ScenarioAction, error)) ([]ScenarioAction, error) {
	ctx, cancel := context.WithTimeout(context.Background(), failureReductionTimeout)
	defer cancel()
	return reducer(ctx)
}
