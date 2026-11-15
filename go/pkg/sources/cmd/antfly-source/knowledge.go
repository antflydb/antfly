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
	"io"
	"os"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources"
)

func knowledgeCommand(args []string) error {
	flags := flag.NewFlagSet("antfly-source knowledge", flag.ContinueOnError)
	catalogPath := flags.String("catalog", "", "portable catalog JSON")
	source := flags.String("source", "", "source identity")
	entity := flags.String("entity", "", "entity binding for namespaced canonical start keys")
	graph := flags.String("graph", "", "graph binding")
	start := flags.String("start", "", "exact native node key")
	audience := flags.String("knowledge-audience", "", "operator assertion of one native instance audience")
	valid := flags.String("valid-at", "", "relationship valid instant (RFC3339)")
	known := flags.String("known-at", "", "knowledge instant (RFC3339; current retained metadata only)")
	depth := flags.Int("depth", 1, "maximum hops, at most 8")
	limit := flags.Int("limit", 20, "maximum results, at most 100")
	token := flags.String("token-env", "ANTFLY_TOKEN", "native data-plane token environment reference")
	snapshot := flags.Bool("require-snapshot", false, "reject live/unretained results")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	file, err := os.Open(*catalogPath)
	if err != nil {
		return err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (32<<20)+1))
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
		return fmt.Errorf("unsupported catalog")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, binding := range catalog.Sources {
		if binding.ID == *source {
			p := &sources.Publisher{Endpoint: catalog.Endpoint, Token: os.Getenv(*token)}
			out, err := p.QueryKnowledge(ctx, binding, sources.KnowledgeRequest{Graph: *graph, Entity: *entity, Start: []string{*start}, Depth: *depth, Limit: *limit, ValidAt: *valid, KnownAt: *known, RequireSnapshot: *snapshot}, *audience)
			if err != nil {
				return err
			}
			_, err = os.Stdout.Write(append(out, '\n'))
			return err
		}
	}
	return fmt.Errorf("source not found")
}
