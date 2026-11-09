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

// A small real-native fixture, with authored vectors explicitly labeled as
// deterministic fixture output. It requires no Colony deployment or models.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	sources "github.com/antflydb/antfly/go/pkg/sources"
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	endpoint := flag.String("endpoint", "http://127.0.0.1:8080/db/v1", "isolated loopback Antfly API")
	directory := flag.String("directory", ".source-search-fixture", "private fixture files and durable state")
	flag.Parse()
	u, err := url.Parse(*endpoint)
	if err != nil {
		return err
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("fixture requires an isolated loopback Antfly server")
	}
	root, err := filepath.Abs(*directory)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return err
	}
	objects := filepath.Join(root, "originals")
	if err = os.MkdirAll(objects, 0700); err != nil {
		return err
	}
	if err = os.Setenv(sources.LocalRootsEnv, objects); err != nil {
		return err
	}
	for _, text := range []string{"Canonical source text: the research launch is Tuesday.", "Transcript fixture: Alice discussed the launch at 04:12.", "Summary fixture: the launch plan is ready."} {
		if _, err = sources.PutLocalObject(objects, "", ".txt", strings.NewReader(text)); err != nil {
			return err
		}
	}
	png, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII=")
	if _, err = sources.PutLocalObject(objects, "", ".png", bytes.NewReader(png)); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	base := "http://" + listener.Addr().String()
	table := "source_fixture_" + fmt.Sprint(time.Now().UnixNano())
	plan, text, artifact, err := sources.BuildTextProcessing(sources.TextProcessingPreset{Name: "fixture_content", Field: "content.text", Purpose: "extraction"})
	if err != nil {
		return err
	}
	space := sources.VectorSpace{Id: "fixture-only-v1", Model: "deterministic-fixture", Revision: "v1", Dimension: 3, Metric: "cosine", QueryPreprocessing: "fixture-v1", DocumentPreprocessing: "fixture-v1", QueryModality: "text", DocumentModality: "multimodal"}
	external, err := sources.ExternalVectorIndex("fixture_vectors", space)
	if err != nil {
		return err
	}
	plan.Indexes = append(plan.Indexes, external)
	media := sources.SearchStream{Name: "original", ResourceField: "content.resource", Purpose: "original", Retrieval: &sources.RetrievalBindings{Semantic: &management.SemanticRetrieval{Index: "fixture_vectors", External: true, VectorSpace: space, QueryInputs: []management.SemanticRetrievalQueryInputs{"vector"}, Default: true}}}
	tokenEnv := ""
	if os.Getenv("ANTFLY_TOKEN") != "" {
		tokenEnv = "ANTFLY_TOKEN"
	}
	profile := sources.Profile{Version: 1, Endpoint: *endpoint, DataPlaneTokenEnv: tokenEnv, CatalogTable: table + "_catalog", Connections: map[string]sources.Connection{"local": {Provider: sources.ProviderLocal, LocalRoot: objects}}, Sources: []sources.Source{{ID: "11111111-1111-4111-8111-111111111111", Table: table, Connection: "local", Publication: "native", Config: json.RawMessage(`{}`), Permissions: &sources.Permissions{Public: true}, SearchVersion: 2, Streams: []sources.SearchStream{text, media}, Artifacts: []sources.ArtifactBinding{artifact}, Processing: &plan}}}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return err
	}
	admin := base64.RawURLEncoding.EncodeToString(key)
	service, err := sources.NewService(sources.ServiceOptions{Profile: profile, StateDir: filepath.Join(root, "state"), PublicURL: base, AdminToken: admin, MediaKey: key})
	if err != nil {
		return err
	}
	defer service.Close()
	server := &http.Server{Handler: service, ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	go server.Serve(listener)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "POST", base+"/v1/sync", strings.NewReader(`{"source_id":"11111111-1111-4111-8111-111111111111"}`))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+admin)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	var job struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(response.Body).Decode(&job)
	response.Body.Close()
	if err != nil {
		return err
	}
	if response.StatusCode != 202 {
		return fmt.Errorf("fixture sync rejected: HTTP %d", response.StatusCode)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("fixture sync did not complete")
		}
		request, _ = http.NewRequestWithContext(ctx, "GET", base+"/v1/jobs/"+job.ID, nil)
		request.Header.Set("Authorization", "Bearer "+admin)
		response, err = client.Do(request)
		if err != nil {
			return err
		}
		var state struct {
			State string `json:"state"`
		}
		err = json.NewDecoder(response.Body).Decode(&state)
		response.Body.Close()
		if err != nil {
			return err
		}
		if state.State == "completed" {
			break
		}
		if state.State == "failed" || state.State == "interrupted" {
			return fmt.Errorf("fixture sync failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	publisher := &sources.Publisher{Endpoint: *endpoint, Token: os.Getenv("ANTFLY_TOKEN")}
	query := []byte(`{"query":{"match_all":{}},"fields":["id","source","content","metadata","permissions","schema_version","source_id","doc_type"],"limit":10}`)
	raw, err := publisher.Query(ctx, table, query)
	if err != nil {
		return err
	}
	var rows struct {
		Responses []struct {
			Hits struct {
				Hits []struct {
					ID     string         `json:"_id"`
					Source map[string]any `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	if err = json.Unmarshal(raw, &rows); err != nil {
		return err
	}
	if len(rows.Responses) != 1 {
		return fmt.Errorf("unexpected native fixture response")
	}
	inserts := map[string]any{}
	for _, hit := range rows.Responses[0].Hits.Hits {
		vector := []float32{1, 0, 0}
		if content, ok := hit.Source["content"].(map[string]any); ok && content["resource"] != nil {
			vector = []float32{0, 1, 0}
		}
		record, err := sources.WithExternalVector(hit.Source, "fixture_vectors", space, vector)
		if err != nil {
			return err
		}
		inserts[hit.ID] = record
	}
	if err = publisher.Batch(ctx, table, inserts, nil); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(root, "profile.json"), profile); err != nil {
		return err
	}
	fmt.Printf("Source service: %s\nCatalog: %s\nTable: %s\nFixture vectors are synthetic; this is not model/relevance qualification.\nKeep this process running for original-media evidence. Ctrl-C stops it; tables remain for inspection.\n", base, filepath.Join(root, "state", "runner", "catalog.json"), table)
	<-ctx.Done()
	return nil
}
func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0600)
}
