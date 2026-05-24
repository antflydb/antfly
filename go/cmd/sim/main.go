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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/antflydb/antfly/src/sim"
)

type options struct {
	action         string
	scope          string
	modes          string
	seeds          string
	steps          int
	actionSettle   time.Duration
	stabilizeEvery int
	baseDir        string
	artifactDir    string
	maxFailures    int
	jsonOutput     bool
}

type validationResult struct {
	Scope    string   `json:"scope"`
	Commands []string `json:"commands"`
}

func main() {
	opts, err := parseFlags()
	if err != nil {
		fatal(err)
	}

	ctx := context.Background()
	switch opts.action {
	case "soak":
		if err := runSoak(ctx, opts); err != nil {
			fatal(err)
		}
	case "validate":
		if err := runValidate(ctx, opts); err != nil {
			fatal(err)
		}
	default:
		fatal(fmt.Errorf("unsupported action %q", opts.action))
	}
}

func parseFlags() (*options, error) {
	opts := &options{}
	flag.StringVar(&opts.action, "action", "soak", "Action to run: soak or validate")
	flag.StringVar(&opts.scope, "scope", "sim", "Validation scope: sim or repo")
	flag.StringVar(&opts.modes, "modes", "all", "Comma-separated soak modes: all,documents,splits,split_failover,transactions")
	flag.StringVar(&opts.seeds, "seeds", "", "Optional comma-separated seed override for all selected soak modes")
	flag.IntVar(&opts.steps, "steps", 0, "Optional step override for all selected soak modes")
	flag.DurationVar(&opts.actionSettle, "action-settle", 0, "Optional action settle duration override")
	flag.IntVar(&opts.stabilizeEvery, "stabilize-every", 0, "Optional stabilize interval override")
	flag.StringVar(&opts.baseDir, "base-dir", filepath.Join(".", "tmp", "sim-cli"), "Base directory for soak artifacts and temp data")
	flag.StringVar(&opts.artifactDir, "artifact-dir", "", "Optional soak artifact directory override")
	flag.IntVar(&opts.maxFailures, "max-failures", 10, "Maximum failures before soak stops")
	flag.BoolVar(&opts.jsonOutput, "json", false, "Emit JSON summary")
	flag.Parse()

	if opts.action != "soak" && opts.action != "validate" {
		return nil, fmt.Errorf("invalid action %q", opts.action)
	}
	if opts.scope != "sim" && opts.scope != "repo" {
		return nil, fmt.Errorf("invalid scope %q", opts.scope)
	}
	return opts, nil
}

func runSoak(ctx context.Context, opts *options) error {
	scenarios, err := selectScenarios(opts)
	if err != nil {
		return err
	}
	cfg := sim.SoakConfig{
		BaseDir:     opts.baseDir,
		ArtifactDir: opts.artifactDir,
		MaxFailures: opts.maxFailures,
		Scenarios:   scenarios,
	}
	report, err := sim.RunSoak(ctx, cfg)
	if err != nil {
		return err
	}
	return printJSON(report, opts.jsonOutput)
}

func runValidate(ctx context.Context, opts *options) error {
	commands := [][]string{
		{"go", "test", "-timeout", "30m", "-run", "^$", "./src/metadata", "./src/sim"},
		{"go", "test", "-json", "-count=1", "-timeout", "30m", "./src/sim"},
	}
	if opts.scope == "repo" {
		commands = append(commands, []string{"go", "test", "-timeout", "30m", "./..."})
	}
	ran := make([]string, 0, len(commands))
	for _, cmdArgs := range commands {
		if err := runCommand(ctx, cmdArgs...); err != nil {
			return err
		}
		ran = append(ran, strings.Join(cmdArgs, " "))
	}
	return printJSON(validationResult{
		Scope:    opts.scope,
		Commands: ran,
	}, opts.jsonOutput)
}

func runCommand(ctx context.Context, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("command is required")
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...) //nolint:gosec // args are constructed internally, not from user input
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "GOCACHE=/tmp/antfly-go-cache")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func selectScenarios(opts *options) ([]sim.SoakScenario, error) {
	scenarios := sim.DefaultSoakScenarios()
	selectedModes, err := parseModes(opts.modes)
	if err != nil {
		return nil, err
	}
	selectedSeeds, err := parseSeeds(opts.seeds)
	if err != nil {
		return nil, err
	}

	filtered := make([]sim.SoakScenario, 0, len(selectedModes))
	for _, mode := range selectedModes {
		idx := slices.IndexFunc(scenarios, func(s sim.SoakScenario) bool {
			return s.Mode == mode
		})
		if idx < 0 {
			return nil, fmt.Errorf("unknown soak mode %q", mode)
		}
		scenario := scenarios[idx]
		if len(selectedSeeds) > 0 {
			scenario.Seeds = append([]int64(nil), selectedSeeds...)
		}
		if opts.steps > 0 {
			scenario.Steps = opts.steps
		}
		if opts.actionSettle > 0 {
			scenario.ActionSettle = opts.actionSettle
		}
		if opts.stabilizeEvery > 0 {
			scenario.StabilizeEvery = opts.stabilizeEvery
		}
		filtered = append(filtered, scenario)
	}
	return filtered, nil
}

func parseModes(raw string) ([]sim.SoakMode, error) {
	if raw == "" || raw == "all" {
		return []sim.SoakMode{
			sim.SoakModeDocuments,
			sim.SoakModeSplits,
			sim.SoakModeSplitFailover,
			sim.SoakModeTransactions,
		}, nil
	}
	parts := strings.Split(raw, ",")
	modes := make([]sim.SoakMode, 0, len(parts))
	for _, part := range parts {
		mode := sim.SoakMode(strings.TrimSpace(part))
		if mode == "" {
			continue
		}
		modes = append(modes, mode)
	}
	if len(modes) == 0 {
		return nil, fmt.Errorf("at least one soak mode is required")
	}
	return modes, nil
}

func parseSeeds(raw string) ([]int64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	seeds := make([]int64, 0, len(parts))
	for _, part := range parts {
		seed, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing seed %q: %w", part, err)
		}
		seeds = append(seeds, seed)
	}
	return seeds, nil
}

func printJSON(v any, force bool) error {
	if !force {
		return nil
	}
	payload, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(os.Stdout, "%s\n", payload)
	return err
}

func fatal(err error) {
	if err == nil {
		os.Exit(0)
	}
	if !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	os.Exit(1)
}
