package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/antflydb/antfly/lib/types"
)

type SoakMode string

const (
	SoakModeDocuments     SoakMode = "documents"
	SoakModeSplits        SoakMode = "splits"
	SoakModeSplitFailover SoakMode = "split_failover"
	SoakModeTransactions  SoakMode = "transactions"
)

type SoakScenario struct {
	Mode           SoakMode
	Seeds          []int64
	Steps          int
	ActionSettle   time.Duration
	StabilizeEvery int
}

type SoakConfig struct {
	BaseDir     string
	ArtifactDir string
	MaxFailures int
	Scenarios   []SoakScenario
}

type SoakFailureArtifact struct {
	Mode           SoakMode         `json:"mode"`
	Kind           ScenarioKind     `json:"kind"`
	Seed           int64            `json:"seed"`
	Category       FailureCategory  `json:"category"`
	Action         string           `json:"action,omitempty"`
	Actions        []ScenarioAction `json:"actions,omitempty"`
	ReducedActions []ScenarioAction `json:"reduced_actions,omitempty"`
	Trace          string           `json:"trace,omitempty"`
	Error          string           `json:"error"`
	RecordedAt     time.Time        `json:"recorded_at"`
}

type SoakReport struct {
	Runs          int                   `json:"runs"`
	Successes     int                   `json:"successes"`
	Failures      int                   `json:"failures"`
	FailureFiles  []string              `json:"failure_files,omitempty"`
	FailureStates []SoakFailureArtifact `json:"failure_states,omitempty"`
}

type soakRunner func(context.Context, SoakScenario, int64, string) error

func RunSoak(ctx context.Context, cfg SoakConfig) (*SoakReport, error) {
	return runSoakWithRunner(ctx, cfg, defaultSoakRunner)
}

func runSoakWithRunner(ctx context.Context, cfg SoakConfig, runner soakRunner) (*SoakReport, error) {
	if runner == nil {
		return nil, fmt.Errorf("runner is required")
	}
	if cfg.BaseDir == "" {
		cfg.BaseDir = filepath.Join(".", "tmp", "sim-soak")
	}
	if cfg.ArtifactDir == "" {
		cfg.ArtifactDir = filepath.Join(cfg.BaseDir, "artifacts")
	}
	if len(cfg.Scenarios) == 0 {
		cfg.Scenarios = DefaultSoakScenarios()
	}
	if err := os.MkdirAll(cfg.BaseDir, 0o750); err != nil {
		return nil, fmt.Errorf("creating soak base dir: %w", err)
	}
	if err := os.MkdirAll(cfg.ArtifactDir, 0o750); err != nil {
		return nil, fmt.Errorf("creating soak artifact dir: %w", err)
	}

	report := &SoakReport{}
	for _, scenario := range cfg.Scenarios {
		if len(scenario.Seeds) == 0 {
			continue
		}
		for _, seed := range scenario.Seeds {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			report.Runs++
			runDir := filepath.Join(cfg.BaseDir, string(scenario.Mode), fmt.Sprintf("seed-%d", seed))
			if err := os.RemoveAll(runDir); err != nil {
				return nil, fmt.Errorf("resetting soak run dir %s: %w", runDir, err)
			}
			if err := os.MkdirAll(runDir, 0o750); err != nil {
				return nil, fmt.Errorf("creating soak run dir %s: %w", runDir, err)
			}
			if err := runner(ctx, scenario, seed, runDir); err != nil {
				artifact := newSoakFailureArtifact(scenario.Mode, seed, err)
				filename, writeErr := writeSoakFailureArtifact(cfg.ArtifactDir, artifact)
				if writeErr != nil {
					return nil, writeErr
				}
				report.Failures++
				report.FailureFiles = append(report.FailureFiles, filename)
				report.FailureStates = append(report.FailureStates, artifact)
				if cfg.MaxFailures > 0 && report.Failures >= cfg.MaxFailures {
					return report, nil
				}
				continue
			}
			report.Successes++
		}
	}
	return report, nil
}

func DefaultSoakScenarios() []SoakScenario {
	return []SoakScenario{
		{
			Mode:           SoakModeDocuments,
			Seeds:          []int64{11, 29, 47},
			Steps:          28,
			ActionSettle:   1200 * time.Millisecond,
			StabilizeEvery: 5,
		},
		{
			Mode:           SoakModeSplits,
			Seeds:          []int64{401, 411, 419},
			Steps:          20,
			ActionSettle:   1200 * time.Millisecond,
			StabilizeEvery: 5,
		},
		{
			Mode:           SoakModeTransactions,
			Seeds:          []int64{7, 19, 302},
			Steps:          16,
			ActionSettle:   time.Second,
			StabilizeEvery: 4,
		},
		{
			Mode:           SoakModeSplitFailover,
			Seeds:          []int64{521, 523, 541},
			Steps:          22,
			ActionSettle:   1200 * time.Millisecond,
			StabilizeEvery: 5,
		},
	}
}

func defaultSoakRunner(ctx context.Context, scenario SoakScenario, seed int64, baseDir string) error {
	switch scenario.Mode {
	case SoakModeDocuments:
		_, err := RunRandomScenario(ctx, RandomScenarioConfig{
			Seed:                     seed,
			BaseDir:                  baseDir,
			Start:                    time.Unix(1_702_000_000+seed, 0).UTC(),
			Steps:                    defaultSoakSteps(scenario.Steps, 24),
			SplitTimeout:             30 * time.Second,
			SplitFinalizeGracePeriod: time.Second,
			ActionSettle:             defaultSoakDuration(scenario.ActionSettle, 1200*time.Millisecond),
			StabilizeEvery:           defaultSoakInterval(scenario.StabilizeEvery, 6),
		})
		return err
	case SoakModeSplits:
		_, err := RunRandomScenario(ctx, RandomScenarioConfig{
			Seed:                     seed,
			BaseDir:                  baseDir,
			Start:                    time.Unix(1_702_100_000+seed, 0).UTC(),
			Steps:                    defaultSoakSteps(scenario.Steps, 20),
			MaxShardSizeBytes:        256,
			MaxShardsPerTable:        3,
			SplitTimeout:             30 * time.Second,
			SplitFinalizeGracePeriod: time.Second,
			ActionSettle:             defaultSoakDuration(scenario.ActionSettle, 1200*time.Millisecond),
			StabilizeEvery:           defaultSoakInterval(scenario.StabilizeEvery, 5),
			PreferSplits:             true,
		})
		return err
	case SoakModeSplitFailover:
		_, err := RunRandomScenario(ctx, RandomScenarioConfig{
			Seed:                        seed,
			BaseDir:                     baseDir,
			Start:                       time.Unix(1_702_150_000+seed, 0).UTC(),
			MetadataIDs:                 []types.ID{100, 101, 102},
			Steps:                       defaultSoakSteps(scenario.Steps, 22),
			MaxShardSizeBytes:           256,
			MaxShardsPerTable:           3,
			SplitTimeout:                30 * time.Second,
			SplitFinalizeGracePeriod:    time.Second,
			CheckerSplitLivenessTimeout: 90 * time.Second,
			StabilizeTimeout:            180 * time.Second,
			ActionSettle:                defaultSoakDuration(scenario.ActionSettle, 1200*time.Millisecond),
			StabilizeEvery:              defaultSoakInterval(scenario.StabilizeEvery, 5),
			PreferSplits:                true,
		})
		return err
	case SoakModeTransactions:
		_, err := RunRandomTransactionScenario(ctx, RandomTransactionScenarioConfig{
			Seed:           seed,
			BaseDir:        baseDir,
			Start:          time.Unix(1_702_200_000+seed, 0).UTC(),
			Steps:          defaultSoakSteps(scenario.Steps, 16),
			ActionSettle:   defaultSoakDuration(scenario.ActionSettle, time.Second),
			StabilizeEvery: defaultSoakInterval(scenario.StabilizeEvery, 4),
		})
		return err
	default:
		return fmt.Errorf("unknown soak mode %q", scenario.Mode)
	}
}

func newSoakFailureArtifact(mode SoakMode, seed int64, err error) SoakFailureArtifact {
	artifact := SoakFailureArtifact{
		Mode:       mode,
		Seed:       seed,
		Category:   ClassifyFailure(err),
		Error:      err.Error(),
		RecordedAt: time.Now().UTC(),
	}
	var runErr *ScenarioRunError
	if errors.As(err, &runErr) {
		artifact.Kind = runErr.Kind
		artifact.Action = runErr.Action
		artifact.Category = runErr.Category
		artifact.Actions = append([]ScenarioAction(nil), runErr.Actions...)
		artifact.ReducedActions = append([]ScenarioAction(nil), runErr.ReducedActions...)
		artifact.Trace = runErr.Trace
	} else {
		switch mode {
		case SoakModeTransactions:
			artifact.Kind = ScenarioKindTransactions
		default:
			artifact.Kind = ScenarioKindDocuments
		}
	}
	return artifact
}

func writeSoakFailureArtifact(dir string, artifact SoakFailureArtifact) (string, error) {
	name := fmt.Sprintf(
		"%s-seed-%d-%s.json",
		artifact.Mode,
		artifact.Seed,
		artifact.Category,
	)
	path := filepath.Join(dir, name)
	payload, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling soak artifact: %w", err)
	}
	if err := os.WriteFile(path, append(payload, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("writing soak artifact %s: %w", path, err)
	}
	return path, nil
}

func defaultSoakSteps(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

func defaultSoakDuration(v, fallback time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return fallback
}

func defaultSoakInterval(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}
