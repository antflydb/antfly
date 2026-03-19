package sim

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunSoak_WritesFailureArtifacts(t *testing.T) {
	cfg := SoakConfig{
		BaseDir:     filepath.Join(t.TempDir(), "soak"),
		ArtifactDir: filepath.Join(t.TempDir(), "artifacts"),
		Scenarios: []SoakScenario{{
			Mode:  SoakModeSplits,
			Seeds: []int64{101, 103},
		}},
	}

	report, err := runSoakWithRunner(context.Background(), cfg, func(_ context.Context, scenario SoakScenario, seed int64, _ string) error {
		if seed == 101 {
			return nil
		}
		return &ScenarioRunError{
			Kind:           ScenarioKindDocuments,
			Seed:           seed,
			Action:         "tick",
			Category:       FailureCategorySplitLiveness,
			Actions:        []ScenarioAction{ActionWrite, ActionReallocate, ActionTick},
			ReducedActions: []ScenarioAction{ActionReallocate, ActionTick},
			Trace:          "events:\n  split",
			Cause:          errors.New("split state remained active"),
		}
	})
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Equal(t, 2, report.Runs)
	require.Equal(t, 1, report.Successes)
	require.Equal(t, 1, report.Failures)
	require.Len(t, report.FailureFiles, 1)
	require.Len(t, report.FailureStates, 1)

	payload, err := os.ReadFile(report.FailureFiles[0])
	require.NoError(t, err)

	var artifact SoakFailureArtifact
	require.NoError(t, json.Unmarshal(payload, &artifact))
	require.Equal(t, SoakModeSplits, artifact.Mode)
	require.Equal(t, int64(103), artifact.Seed)
	require.Equal(t, FailureCategorySplitLiveness, artifact.Category)
	require.Equal(t, []ScenarioAction{ActionReallocate, ActionTick}, artifact.ReducedActions)
}

func TestRunSoak_StopsAtMaxFailures(t *testing.T) {
	cfg := SoakConfig{
		BaseDir:     filepath.Join(t.TempDir(), "soak"),
		ArtifactDir: filepath.Join(t.TempDir(), "artifacts"),
		MaxFailures: 1,
		Scenarios: []SoakScenario{{
			Mode:  SoakModeDocuments,
			Seeds: []int64{1, 2, 3},
		}},
	}

	report, err := runSoakWithRunner(context.Background(), cfg, func(_ context.Context, scenario SoakScenario, seed int64, _ string) error {
		require.Equal(t, SoakModeDocuments, scenario.Mode)
		return &ScenarioRunError{
			Kind:     ScenarioKindDocuments,
			Seed:     seed,
			Action:   "write",
			Category: FailureCategoryProposalDropped,
			Actions:  []ScenarioAction{ActionWrite},
			Cause:    errors.New("raft proposal dropped"),
		}
	})
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Equal(t, 1, report.Runs)
	require.Equal(t, 1, report.Failures)
	require.Empty(t, report.Successes)
	require.Len(t, report.FailureFiles, 1)
}

func TestNewSoakFailureArtifact_ClassifiesNonScenarioErrors(t *testing.T) {
	artifact := newSoakFailureArtifact(SoakModeTransactions, 55, errors.New("opening snapshot store failed"))
	require.Equal(t, SoakModeTransactions, artifact.Mode)
	require.Equal(t, ScenarioKindTransactions, artifact.Kind)
	require.Equal(t, FailureCategorySnapshotTransfer, artifact.Category)
	require.Contains(t, artifact.Error, "snapshot")
}

func TestDefaultSoakScenarios_IncludeSplitFailover(t *testing.T) {
	scenarios := DefaultSoakScenarios()
	found := false
	for _, scenario := range scenarios {
		if scenario.Mode == SoakModeSplitFailover {
			found = true
			require.NotEmpty(t, scenario.Seeds)
			require.Positive(t, scenario.Steps)
		}
	}
	require.True(t, found)
}
