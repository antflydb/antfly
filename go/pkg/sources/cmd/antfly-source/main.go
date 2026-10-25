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

// antfly-source performs one durable sync; run it from cron/systemd/Kubernetes
// for unattended self-hosted ingestion. It does not require Colony or Cloud CLI.
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
	flags := flag.NewFlagSet("antfly-source sync", flag.ContinueOnError)
	profile := flags.String("config", "", "source profile JSON")
	state := flags.String("state-dir", "", "private durable state directory")
	catalog := flags.String("catalog-out", "", "portable search catalog path")
	restart := flags.Bool("restart", false, "discard a failed pinned selection and enumerate afresh (retain ownership)")
	if len(os.Args) < 2 || os.Args[1] != "sync" {
		return fmt.Errorf("usage: antfly-source sync --config FILE --state-dir DIR [--catalog-out FILE] [--restart]")
	}
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *profile == "" || *state == "" {
		return fmt.Errorf("config and state-dir are required")
	}
	file, err := os.Open(*profile)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("source profile exceeds 1 MiB")
	}
	var configuration sources.Profile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configuration); err != nil {
		return fmt.Errorf("invalid source profile: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("source profile must contain exactly one JSON value")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runner := sources.Runner{Profile: configuration, StateDir: *state, CatalogPath: *catalog, Restart: *restart}
	return runner.Sync(ctx)
}
