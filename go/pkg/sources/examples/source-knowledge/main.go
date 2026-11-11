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

// A deterministic native knowledge fixture; no provider credentials or models.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	endpoint := flag.String("endpoint", "http://127.0.0.1:8080/db/v1", "disposable loopback native owner")
	output := flag.String("catalog", "source-knowledge-catalog.json", "portable fixture catalog")
	flag.Parse()
	u, err := url.Parse(*endpoint)
	if err != nil {
		return err
	}
	if u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback() {
		return fmt.Errorf("fixture requires loopback HTTP")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p := &sources.Publisher{Endpoint: *endpoint, Token: os.Getenv("ANTFLY_TOKEN")}
	table := fmt.Sprintf("knowledge_fixture_%d", time.Now().UnixNano())
	if err = p.EnsureTable(ctx, table); err != nil {
		return err
	}
	entityTable := table + "_entities"
	if err = p.EnsureTable(ctx, entityTable); err != nil {
		return err
	}
	id := "11111111-1111-4111-8111-111111111111"
	plan, k, artifact, err := sources.BuildKnowledgeProcessing(sources.KnowledgePreset{Name: "facts", SourceID: id, Audience: "fixture", Field: "facts", EntityTable: entityTable, Source: map[string]any{"path": "$.relations[*]", "format": "extraction_relation", "edge": map[string]any{"edge_id": "{{ _item.id }}", "type": "WORKS_AT", "metadata": map[string]any{"valid_at": "{{ _item.valid_at }}", "created_at": "{{ _item.created_at }}", "provenance": "{{ _item.provenance }}"}}}})
	if err != nil {
		return err
	}
	if err = sources.ReconcileProcessing(ctx, p, table, &plan); err != nil {
		return err
	}
	parent := map[string]any{"source_id": id, "doc_type": "source_item", "schema_version": 3, "source": map[string]any{"id": id, "item_id": "claim", "uri": "local:///claim", "path": "claim", "revision": "r1", "indexed_at": time.Now().Unix()}, "content": map[string]any{"text": "Alice works at Acme."}, "permissions": map[string]any{"public": true}, "facts": map[string]any{"entities": []any{map[string]any{"id": "alice", "text": "Alice", "label": "person"}, map[string]any{"id": "acme", "text": "Acme", "label": "organization"}}, "relations": []any{map[string]any{"id": "employment-1", "source": "alice", "target": "acme", "valid_at": "2020-01-01T00:00:00Z", "created_at": "2021-01-01T00:00:00Z", "provenance": "source character range 0:20"}}}}
	if err = p.Batch(ctx, table, map[string]any{"claim": parent}, nil); err != nil {
		return err
	}
	until := time.Now().Add(30 * time.Minute).Unix()
	b := sources.Binding{ID: id, Alias: "knowledge fixture", Table: table, Knowledge: &k, Artifacts: []sources.ArtifactBinding{artifact}, Fields: sources.CanonicalDocumentFields(), DocumentVersion: sources.SourceRecordVersion, Filter: sources.SourceFilter(id), Status: "searchable", PublishedRevision: "fixture-r1", ValidUntil: &until}
	var result []byte
	for {
		result, err = p.QueryKnowledge(ctx, b, sources.KnowledgeRequest{Graph: "facts", Entity: "facts", Start: []string{k.Entities[0].Namespace + "person/alice"}, ValidAt: "2022-01-01T00:00:00Z", KnownAt: "2022-01-01T00:00:00Z"}, "fixture")
		if err == nil && strings.Contains(string(result), "employment-1") {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("native fixture did not converge: error=%v response=%s", err, result)
		case <-time.After(100 * time.Millisecond):
		}
	}
	catalog, err := json.MarshalIndent(sources.Catalog{Version: 2, Endpoint: *endpoint, Sources: []sources.Binding{b}, Principals: []string{}}, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(*output, append(catalog, '\n'), 0600); err != nil {
		return err
	}
	fmt.Printf("Table: %s\nCatalog: %s\nSynthetic facts qualify integration only.\n%s\n", table, *output, result)
	return nil
}
