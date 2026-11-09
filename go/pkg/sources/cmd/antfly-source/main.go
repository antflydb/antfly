// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// antfly-source runs durable one-shot ingestion or its optional source companion.
// It does not require Colony or Cloud CLI.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	sources "github.com/antflydb/antfly/go/pkg/sources"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "antfly-source:", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) >= 2 && os.Args[1] == "knowledge" {
		return knowledgeCommand(os.Args[2:])
	}
	if len(os.Args) >= 2 && os.Args[1] == "search" {
		return searchCommand(os.Args[2:])
	}
	if len(os.Args) >= 2 && os.Args[1] == "processing" {
		return processingCommand(os.Args[2:])
	}
	if len(os.Args) >= 2 && os.Args[1] == "serve" {
		return serve(os.Args[2:])
	}
	if len(os.Args) >= 2 && os.Args[1] == "migrate-local" {
		return migrateLocal(os.Args[2:])
	}
	flags := flag.NewFlagSet("antfly-source sync", flag.ContinueOnError)
	knowledgeAudience := flags.String("knowledge-audience", "", "operator assertion of one native instance audience")
	profile := flags.String("config", "", "source profile JSON")
	state := flags.String("state-dir", "", "private durable state directory")
	catalog := flags.String("catalog-out", "", "portable search catalog path")
	restart := flags.Bool("restart", false, "discard a failed pinned selection and enumerate afresh (retain ownership)")
	if len(os.Args) < 2 || os.Args[1] != "sync" {
		return fmt.Errorf("usage: antfly-source sync --config FILE --state-dir DIR [--catalog-out FILE] [--restart]; or antfly-source serve --config FILE --state-dir DIR --public-url URL; or antfly-source migrate-local --source ID --from-connection NAME --to-connection NAME --to-bucket BUCKET [--server URL | (--config FILE --state-dir DIR --public-url URL)] [--from-prefix PREFIX] [--to-prefix PREFIX] [--dry-run]")
	}
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *profile == "" || *state == "" {
		return fmt.Errorf("config and state-dir are required")
	}
	configuration, err := loadProfile(*profile)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runner := sources.Runner{KnowledgeAudience: *knowledgeAudience, Profile: configuration, StateDir: *state, CatalogPath: *catalog, Restart: *restart}
	return runner.Sync(ctx)
}

func loadProfile(path string) (sources.Profile, error) {
	var configuration sources.Profile
	file, err := os.Open(path)
	if err != nil {
		return configuration, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil {
		return configuration, err
	}
	if len(data) > 1<<20 {
		return configuration, fmt.Errorf("source profile exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configuration); err != nil {
		return configuration, fmt.Errorf("invalid source profile: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return configuration, fmt.Errorf("source profile must contain exactly one JSON value")
	}
	return configuration, nil
}
