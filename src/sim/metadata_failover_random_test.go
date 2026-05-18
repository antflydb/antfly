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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/stretchr/testify/require"
)

const metadataFailoverSplitSeedHelperEnv = "ANTFLY_SIM_METADATA_FAILOVER_SPLIT_SEED"
const metadataFailoverSplitSeedArtifactEnv = "ANTFLY_SIM_METADATA_FAILOVER_SPLIT_ARTIFACT"

type metadataFailoverRunArtifact struct {
	Outcome        string           `json:"outcome"`
	Kind           ScenarioKind     `json:"kind,omitempty"`
	Category       FailureCategory  `json:"category,omitempty"`
	Action         string           `json:"action,omitempty"`
	Actions        []ScenarioAction `json:"actions,omitempty"`
	ReducedActions []ScenarioAction `json:"reduced_actions,omitempty"`
	FinalShards    int              `json:"final_shards,omitempty"`
	Error          string           `json:"error,omitempty"`
	Trace          string           `json:"trace,omitempty"`
}

type metadataFailoverSeedArtifact struct {
	Seed       int64                        `json:"seed"`
	Initial    *metadataFailoverRunArtifact `json:"initial,omitempty"`
	Replay     *metadataFailoverRunArtifact `json:"replay,omitempty"`
	Failure    string                       `json:"failure,omitempty"`
	Panic      string                       `json:"panic,omitempty"`
	Stack      string                       `json:"stack,omitempty"`
	RecordedAt time.Time                    `json:"recorded_at"`
}

type metadataFailoverSeedArtifactRecorder struct {
	path     string
	artifact metadataFailoverSeedArtifact
}

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

	artifactPath := filepath.Join(t.TempDir(), fmt.Sprintf("seed-%d-artifact.json", seed))
	cmd := exec.Command(os.Args[0], "-test.run=^TestRandomScenario_MetadataFailoverSplitSeedsReplayable$")
	cmd.Env = append(
		os.Environ(),
		fmt.Sprintf("%s=%d", metadataFailoverSplitSeedHelperEnv, seed),
		fmt.Sprintf("%s=%s", metadataFailoverSplitSeedArtifactEnv, artifactPath),
		"GOTRACEBACK=all",
		"ANTFLY_SIM_DEBUG_LOG=1",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		artifactDetails := readMetadataFailoverSeedArtifact(t, artifactPath)
		t.Fatalf("seed=%d subprocess failed: %v\nartifact:\n%s\noutput:\n%s", seed, err, artifactDetails, output)
	}
}

func runMetadataFailoverSplitSeed(t *testing.T, seed int64) {
	t.Helper()
	t.Logf("seed=%d", seed)
	recorder := newMetadataFailoverSeedArtifactRecorder(seed)
	if recorder != nil {
		defer func() {
			if r := recover(); r != nil {
				recorder.recordPanic(r)
				recorder.recordFailure(fmt.Sprintf("panic: %v", r))
				_ = recorder.write()
				panic(r)
			}
			recorder.finalize(t)
		}()
	}

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
	recorder.recordRun("initial", result, err)

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
	recorder.recordRun("replay", replay, replayErr)

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

func newMetadataFailoverSeedArtifactRecorder(seed int64) *metadataFailoverSeedArtifactRecorder {
	path := os.Getenv(metadataFailoverSplitSeedArtifactEnv)
	if path == "" {
		return nil
	}
	return &metadataFailoverSeedArtifactRecorder{
		path: path,
		artifact: metadataFailoverSeedArtifact{
			Seed:       seed,
			RecordedAt: time.Now().UTC(),
		},
	}
}

func (r *metadataFailoverSeedArtifactRecorder) recordRun(
	label string,
	result *RandomScenarioResult,
	err error,
) {
	if r == nil {
		return
	}
	runArtifact := metadataFailoverRunArtifact{}
	if err == nil {
		runArtifact.Outcome = "success"
		if result != nil {
			runArtifact.Actions = append([]ScenarioAction(nil), result.Actions...)
			runArtifact.ReducedActions = append([]ScenarioAction(nil), result.ReducedActions...)
			runArtifact.FinalShards = result.FinalShards
		}
	} else {
		failure := newSoakFailureArtifact(SoakModeSplitFailover, r.artifact.Seed, err)
		runArtifact.Outcome = "error"
		runArtifact.Kind = failure.Kind
		runArtifact.Category = failure.Category
		runArtifact.Action = failure.Action
		runArtifact.Actions = append([]ScenarioAction(nil), failure.Actions...)
		runArtifact.ReducedActions = append([]ScenarioAction(nil), failure.ReducedActions...)
		runArtifact.Error = failure.Error
		runArtifact.Trace = failure.Trace
	}
	switch label {
	case "initial":
		r.artifact.Initial = &runArtifact
	case "replay":
		r.artifact.Replay = &runArtifact
	}
}

func (r *metadataFailoverSeedArtifactRecorder) recordFailure(message string) {
	if r == nil || message == "" {
		return
	}
	r.artifact.Failure = message
}

func (r *metadataFailoverSeedArtifactRecorder) recordPanic(v any) {
	if r == nil {
		return
	}
	r.artifact.Panic = fmt.Sprint(v)
	r.artifact.Stack = string(debug.Stack())
}

func (r *metadataFailoverSeedArtifactRecorder) finalize(t *testing.T) {
	if r == nil {
		return
	}
	if !t.Failed() && r.artifact.Panic == "" && r.artifact.Failure == "" {
		return
	}
	if r.artifact.Failure == "" && t.Failed() {
		r.artifact.Failure = "test failed before emitting a structured scenario error"
	}
	_ = r.write()
}

func (r *metadataFailoverSeedArtifactRecorder) write() error {
	if r == nil || r.path == "" {
		return nil
	}
	payload, err := json.MarshalIndent(r.artifact, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling metadata failover artifact: %w", err)
	}
	if err := os.WriteFile(r.path, append(payload, '\n'), 0o600); err != nil {
		return fmt.Errorf("writing metadata failover artifact %s: %w", r.path, err)
	}
	return nil
}

func readMetadataFailoverSeedArtifact(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("artifact unavailable: %v", err)
	}
	return string(payload)
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

func TestMetadataFailoverSeedArtifactRecorderWritesStructuredFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seed-artifact.json")
	recorder := &metadataFailoverSeedArtifactRecorder{
		path: path,
		artifact: metadataFailoverSeedArtifact{
			Seed:       521,
			RecordedAt: time.Unix(1_701_200_521, 0).UTC(),
		},
	}

	recorder.recordRun("initial", nil, &ScenarioRunError{
		Kind:           ScenarioKindDocuments,
		Seed:           521,
		Action:         "tick",
		Category:       FailureCategorySplitLiveness,
		Actions:        []ScenarioAction{ActionWrite, ActionTick},
		ReducedActions: []ScenarioAction{ActionTick},
		Trace:          "events:\n  split",
		Cause:          errors.New("split state remained active"),
	})
	recorder.recordFailure("test failed before emitting a structured scenario error")
	require.NoError(t, recorder.write())

	payload, err := os.ReadFile(path)
	require.NoError(t, err)

	var artifact metadataFailoverSeedArtifact
	require.NoError(t, json.Unmarshal(payload, &artifact))
	require.Equal(t, int64(521), artifact.Seed)
	require.NotNil(t, artifact.Initial)
	require.Equal(t, "error", artifact.Initial.Outcome)
	require.Equal(t, FailureCategorySplitLiveness, artifact.Initial.Category)
	require.Equal(t, []ScenarioAction{ActionTick}, artifact.Initial.ReducedActions)
	require.Contains(t, artifact.Failure, "structured scenario error")
}
