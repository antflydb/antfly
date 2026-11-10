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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	sources "github.com/antflydb/antfly/go/pkg/sources"
	"io"
	"os"
	"time"
)

// Search and processing use ordinary native APIs. Neither command installs a
// second retrieval endpoint or reuses provider credentials as data-plane auth.
func searchCommand(args []string) error {
	flags := flag.NewFlagSet("antfly-source search", flag.ContinueOnError)
	allowScan := flags.Bool("allow-scan", false, "permit bounded original-value verification on tables without indexed scope predicates")
	maxDocs := flags.Int("max-verify-documents", 10000, "maximum source documents verified per shard")
	maxBytes := flags.Int("max-verify-bytes", 64<<20, "maximum original bytes verified per shard")
	explain := flags.Bool("explain", false, "print the bounded native query without executing it")
	path := flags.String("catalog", "", "portable catalog JSON")
	sourceID := flags.String("source", "", "source identity")
	stream := flags.String("stream", "", "stream name (or unique per-mode default)")
	mode := flags.String("mode", "semantic", "semantic or full_text")
	query := flags.String("query", "", "query text")
	vectorFile := flags.String("vector", "", "JSON object containing vector and vector_space")
	tokenEnv := flags.String("token-env", "ANTFLY_TOKEN", "data-plane token environment reference")
	if err := flags.Parse(args); err != nil {
		return err
	}
	catalogFile, err := os.Open(*path)
	if err != nil {
		return err
	}
	defer catalogFile.Close()
	raw, err := io.ReadAll(io.LimitReader(catalogFile, (32<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 32<<20 {
		return fmt.Errorf("catalog exceeds budget")
	}
	var catalog sources.Catalog
	if err = json.Unmarshal(raw, &catalog); err != nil {
		return err
	}
	if catalog.Version != 2 {
		return fmt.Errorf("unsupported catalog version")
	}
	var binding *sources.Binding
	for i := range catalog.Sources {
		if catalog.Sources[i].ID == *sourceID {
			binding = &catalog.Sources[i]
			break
		}
	}
	if binding == nil {
		return fmt.Errorf("source not found")
	}
	request := sources.SearchRequest{Stream: *stream, Mode: *mode, Text: *query}
	if *allowScan {
		request.Verification = &sources.SearchVerification{AllowScan: true, MaxDocuments: *maxDocs, MaxBytes: *maxBytes}
	}
	if *vectorFile != "" {
		file, err := os.Open(*vectorFile)
		if err != nil {
			return err
		}
		defer file.Close()
		data, err := io.ReadAll(io.LimitReader(file, 1<<20))
		if err != nil {
			return err
		}
		if err = json.Unmarshal(data, &request); err != nil {
			return err
		}
		request.Stream = *stream
		request.Mode = *mode
	}
	if *explain {
		raw, err := sources.BuildSearchQuery(*binding, request)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(append(raw, '\n'))
		return err
	}
	publisher := &sources.Publisher{Endpoint: catalog.Endpoint, Token: os.Getenv(*tokenEnv)}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	result, err := publisher.Search(ctx, *binding, request)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(result, '\n'))
	return err
}
func processingCommand(args []string) error {
	flags := flag.NewFlagSet("antfly-source processing", flag.ContinueOnError)
	path := flags.String("config", "", "source profile JSON")
	state := flags.String("state", "", "optional existing runner state directory for discovery publication")
	sourceID := flags.String("source", "", "source identity")
	apply := flags.Bool("apply", false, "create missing native processing indexes and backfill without provider acquisition")
	if err := flags.Parse(args); err != nil {
		return err
	}
	profile, err := loadProfile(*path)
	if err != nil {
		return err
	}
	publisher := &sources.Publisher{Endpoint: profile.Endpoint, Token: os.Getenv(profile.DataPlaneTokenEnv)}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, source := range profile.Sources {
		if source.ID != *sourceID {
			continue
		}
		if *apply {
			if err = publisher.EnsureTable(ctx, source.Table); err != nil {
				return err
			}
			if err = sources.ReconcileSourceSearch(ctx, publisher, source); err != nil {
				return err
			}
			if *state != "" {
				runner := sources.Runner{Profile: profile, StateDir: *state, Publisher: publisher}
				if err := runner.RefreshSearchDiscovery(ctx, source.ID); err != nil {
					return err
				}
			}
		}
		return json.NewEncoder(os.Stdout).Encode(sources.ObserveSearch(ctx, publisher, sources.Binding{SearchVersion: source.SearchVersion, Table: source.Table, Streams: source.Streams, Artifacts: source.Artifacts}))
	}
	return fmt.Errorf("source not found")
}
