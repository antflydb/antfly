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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Server mode sends the same schema, index configs, documents, and search
// requests that the embedded Lite path uses, over the public HTTP API
// (specs/openapi/antfly/metadata.yaml, served under /db/v1).
const (
	defaultTableName     = "antfly_docs"
	serverRequestTimeout = 2 * time.Minute
)

// defaultServerURL returns $ANTFLY_URL, which selects server mode when set.
func defaultServerURL() string {
	return os.Getenv("ANTFLY_URL")
}

type serverClient struct {
	base  string
	token string
	http  http.Client
}

func newServerClient(baseURL string) *serverClient {
	token := os.Getenv("ANTFLY_TOKEN")
	return &serverClient{
		base:  strings.TrimRight(baseURL, "/"),
		token: token,
		http:  http.Client{Timeout: serverRequestTimeout},
	}
}

// do sends a JSON request and returns the response body. A status listed in
// allowed is returned without error so callers can treat, for example, an
// existing table as success.
func (c *serverClient) do(method, path string, body any, allowed ...int) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("marshal %s %s: %w", method, path, err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return respBody, resp.StatusCode, nil
	}
	for _, status := range allowed {
		if resp.StatusCode == status {
			return respBody, resp.StatusCode, nil
		}
	}
	return respBody, resp.StatusCode, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBody)))
}

func tablePath(table string, suffix string) string {
	return "/tables/" + url.PathEscape(table) + suffix
}

// serverIndexConfigs returns the public create-table index map for the two
// model-backed indexes. The server provisions full_text_index_v0 itself.
func serverIndexConfigs(cfg indexBuildConfig) (map[string]any, error) {
	chunkVectors, err := innerIndexConfig(chunkVectorsIndexJSON(cfg.EmbedModel, cfg.InferenceURL, cfg.TargetTokens, cfg.OverlapTokens))
	if err != nil {
		return nil, err
	}
	knowledge := knowledgeGraphConfig(cfg.ExtractModel, cfg.InferenceURL, cfg.Metrics)
	knowledge["type"] = "graph"
	return map[string]any{
		chunkVectorsIndex:   chunkVectors,
		knowledgeGraphIndex: knowledge,
	}, nil
}

// innerIndexConfig unwraps the Lite add-index envelope into its public config.
func innerIndexConfig(envelope []byte, err error) (map[string]any, error) {
	if err != nil {
		return nil, err
	}
	var wrapped struct {
		ConfigJSON string `json:"config_json"`
	}
	if err := json.Unmarshal(envelope, &wrapped); err != nil {
		return nil, err
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(wrapped.ConfigJSON), &config); err != nil {
		return nil, err
	}
	return config, nil
}

// ensureServerTables creates the entities table (the resolver's promotion
// target) and the docs table with its indexes. Existing tables are kept;
// pass -reset to rebuild them with new settings.
func ensureServerTables(c *serverClient, table string, cfg indexBuildConfig, reset bool) error {
	if reset {
		for _, name := range []string{table, entitiesTable} {
			if _, _, err := c.do(http.MethodDelete, tablePath(name, ""), nil, http.StatusNotFound); err != nil {
				return fmt.Errorf("drop table %s: %w", name, err)
			}
		}
	}
	if _, status, err := c.do(http.MethodPost, tablePath(entitiesTable, ""), map[string]any{"num_shards": 1}, http.StatusConflict); err != nil {
		return fmt.Errorf("create table %s: %w", entitiesTable, err)
	} else if status == http.StatusConflict {
		fmt.Printf("using existing table %q\n", entitiesTable)
	}

	indexes, err := serverIndexConfigs(cfg)
	if err != nil {
		return err
	}
	var schema map[string]any
	if err := json.Unmarshal(schemaJSON(), &schema); err != nil {
		return err
	}
	// The server manages schema versions itself.
	delete(schema, "version")
	request := map[string]any{
		"num_shards": 1,
		"schema":     schema,
		"indexes":    indexes,
	}
	_, status, err := c.do(http.MethodPost, tablePath(table, ""), request, http.StatusConflict)
	if err != nil {
		return fmt.Errorf("create table %s: %w", table, err)
	}
	if status == http.StatusConflict {
		fmt.Printf("using existing table %q (pass -reset to rebuild it with new settings)\n", table)
	} else {
		fmt.Printf("created table %q with indexes %s and %s\n", table, chunkVectorsIndex, knowledgeGraphIndex)
	}
	return nil
}

// ingestSectionsServer writes sections with the batch API in bounded batches.
func ingestSectionsServer(c *serverClient, table string, sections []docSection) (int, error) {
	written := 0
	for start := 0; start < len(sections); start += ingestBatchSize {
		end := min(start+ingestBatchSize, len(sections))
		inserts := make(map[string]any, end-start)
		for _, section := range sections[start:end] {
			inserts[section.Key] = section.toDocument()
		}
		if _, _, err := c.do(http.MethodPost, tablePath(table, "/batch"), map[string]any{"inserts": inserts}); err != nil {
			return written, fmt.Errorf("write batch [%d:%d): %w", start, end, err)
		}
		written += end - start
		fmt.Printf("  wrote %d/%d documents\n", written, len(sections))
	}
	return written, nil
}

// runServerQuery sends the same hybrid request as the Lite path and prints
// the hits. Chunk hits are projected back to their section.
func runServerQuery(c *serverClient, table, text string, limit int) error {
	raw, _, err := c.do(http.MethodPost, tablePath(table, "/query"), hybridSearchRequest(text, limit))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	var envelope searchEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode search results: %w\nraw: %s", err, raw)
	}
	var results searchHits
	if len(envelope.Responses) > 0 {
		results = envelope.Responses[0].Hits
	}
	fmt.Printf("Query: %q (%d hits)\n\n", text, len(results.Hits))
	printHits(results.Hits)
	fmt.Printf("\nThe server builds embeddings and the %q graph in the background. Explore the graph with graph_queries\n(docs/guides/graph-navigation.mdx) or in Antfarm.\n", knowledgeGraphIndex)
	return nil
}
