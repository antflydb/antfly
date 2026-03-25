package sim

import (
	"context"
	"fmt"
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
	return reduceScenarioActions(ctx, actions, category, func(ctx context.Context, reduced []ScenarioAction) error {
		_, err := RunRandomScenarioWithActions(ctx, cfg, ScenarioRecord{
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
	return reduceScenarioActions(ctx, actions, category, func(ctx context.Context, reduced []ScenarioAction) error {
		_, err := RunRandomTransactionScenarioWithActions(ctx, cfg, ScenarioRecord{
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
