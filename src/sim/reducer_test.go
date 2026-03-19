package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReduceScenarioActions_ShrinksFailingSchedule(t *testing.T) {
	actions := []ScenarioAction{
		ActionTick,
		ActionWrite,
		ActionCrash,
		ActionHeal,
		ActionTick,
	}

	reduced, err := reduceScenarioActions(
		context.Background(),
		actions,
		FailureCategoryProposalDropped,
		func(ctx context.Context, candidate []ScenarioAction) error {
			for _, action := range candidate {
				if action == ActionCrash {
					return &ScenarioRunError{
						Kind:     ScenarioKindDocuments,
						Seed:     1,
						Action:   string(action),
						Category: FailureCategoryProposalDropped,
						Cause:    fmt.Errorf("raft proposal dropped"),
					}
				}
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Less(t, len(reduced), len(actions))
	require.Equal(t, []ScenarioAction{ActionCrash}, reduced)
}

func TestRunRandomScenarioWithActions_UsesProvidedScheduleLength(t *testing.T) {
	result, err := RunRandomScenarioWithActions(context.Background(), RandomScenarioConfig{
		Seed:           101,
		BaseDir:        t.TempDir(),
		Start:          time.Unix(1_700_600_000, 0).UTC(),
		Steps:          20,
		ActionSettle:   500 * time.Millisecond,
		StabilizeEvery: 2,
	}, ScenarioRecord{
		Kind:    ScenarioKindDocuments,
		Seed:    101,
		Actions: []ScenarioAction{ActionTick, ActionTick, ActionTick},
	})
	require.NoError(t, err)
	require.Len(t, result.Actions, 3)
	require.Equal(t, []ScenarioAction{ActionTick, ActionTick, ActionTick}, result.Actions)
}
