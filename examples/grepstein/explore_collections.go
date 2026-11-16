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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/sync/errgroup"
)

type explorerCollection struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Table    string `json:"table"`
	Profile  string `json:"profile"`
	tableURL string
}

func (c explorerCollection) public() map[string]string {
	return map[string]string{"id": c.ID, "label": c.Label, "profile": c.Profile}
}

var explorerCollectionID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
var explorerTableName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func parseExplorerCollections(endpoint, table, raw string) ([]explorerCollection, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("explorer requires an HTTP(S) API URL without credentials or query")
	}
	cs := []explorerCollection{{ID: "pdf", Label: "PDF records", Table: table, Profile: "pdf"}}
	if raw != "" {
		cs = nil
		d := json.NewDecoder(strings.NewReader(raw))
		d.DisallowUnknownFields()
		if err := d.Decode(&cs); err != nil {
			return nil, fmt.Errorf("invalid explorer collections: %w", err)
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			return nil, fmt.Errorf("explorer collections must contain one JSON array")
		}
	}
	if len(cs) < 1 || len(cs) > 4 {
		return nil, fmt.Errorf("configure between one and four explorer collections")
	}
	ids, tables := map[string]bool{}, map[string]bool{}
	for i := range cs {
		c := &cs[i]
		if !explorerCollectionID.MatchString(c.ID) || c.ID == "all" || ids[c.ID] || !explorerTableName.MatchString(c.Table) || tables[c.Table] {
			return nil, fmt.Errorf("invalid or duplicate collection ID/table")
		}
		if c.Profile == "" {
			return nil, fmt.Errorf("collection %s requires an explicit processing profile", c.ID)
		}
		if _, err := processingProfile(c.Profile); err != nil {
			return nil, err
		}
		if len(c.Label) > 80 {
			return nil, fmt.Errorf("collection label exceeds 80 bytes")
		}
		if strings.TrimSpace(c.Label) == "" {
			c.Label = c.ID
		}
		ids[c.ID], tables[c.Table] = true, true
		c.tableURL = strings.TrimRight(endpoint, "/") + "/tables/" + url.PathEscape(c.Table)
	}
	return cs, nil
}
func explorerProvenanceFields(profile string) []string {
	if profile == "transcription" {
		return []string{"provenance.start_ms", "provenance.end_ms", "provenance.speaker"}
	}
	return []string{"provenance.page_number"}
}

// The configured collection allowlist bounds table access and search fan-out.
// Browsers cannot choose arbitrary native paths, artifacts or query bodies.
func explorerCollectionsHandler(cs []explorerCollection, client *http.Client, bucket string, storageClient func(context.Context) (*http.Client, error)) http.Handler {
	handlers := map[string]http.Handler{}
	for _, c := range cs {
		handlers[c.ID] = explorerCollectionHandler(c.tableURL, c.Profile, client, bucket, storageClient)
	}
	selectCollections := func(id string) ([]explorerCollection, error) {
		if id == "" || id == "all" {
			return cs, nil
		}
		for _, c := range cs {
			if c.ID == id {
				return []explorerCollection{c}, nil
			}
		}
		return nil, fmt.Errorf("unknown collection")
	}
	router := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/collections" && r.Method == http.MethodGet {
			out := make([]map[string]string, len(cs))
			for i, c := range cs {
				out[i] = c.public()
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(out)
			return
		}
		if r.URL.Path == "/api/search" && r.Method == http.MethodGet {
			q := strings.TrimSpace(r.URL.Query().Get("q"))
			mode := r.URL.Query().Get("mode")
			if q == "" || len(q) > 2048 {
				http.Error(w, "Enter a query of at most 2048 bytes", 400)
				return
			}
			selected, err := selectCollections(r.URL.Query().Get("collection"))
			if err != nil {
				http.Error(w, "Unknown collection", 400)
				return
			}
			payloads := make([]map[string]any, len(selected))
			for i, c := range selected {
				payloads[i], err = explorerProfileQuery(q, mode, c.Profile)
				if err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				if payloads[i]["limit"].(int) > 20/len(selected) {
					payloads[i]["limit"] = 20 / len(selected)
				}
			}
			// Rankings remain independent; total source results are bounded to twenty.
			results := make([][]map[string]json.RawMessage, len(selected))
			work, ctx := errgroup.WithContext(r.Context())
			for i, c := range selected {
				work.Go(func() error {
					raw, err := corpusRequestWithContext(ctx, client, http.MethodPost, c.tableURL+"/query", payloads[i])
					if err != nil {
						return err
					}
					var response map[string]json.RawMessage
					if err = json.Unmarshal(raw, &response); err != nil {
						return err
					}
					if response == nil {
						return fmt.Errorf("invalid query response")
					}
					if failures := bytes.TrimSpace(response["errors"]); len(failures) > 0 && !bytes.Equal(failures, []byte("null")) && !bytes.Equal(failures, []byte("[]")) {
						return fmt.Errorf("native query failed")
					}
					groups := []map[string]json.RawMessage{response}
					if batch, ok := response["responses"]; ok {
						if err = json.Unmarshal(batch, &groups); err != nil {
							return err
						}
					}
					if len(groups) == 0 {
						return fmt.Errorf("missing query response")
					}
					for _, g := range groups {
						if g == nil {
							return fmt.Errorf("invalid query response")
						}
						if failure := bytes.TrimSpace(g["error"]); len(failure) > 0 && !bytes.Equal(failure, []byte("null")) && !bytes.Equal(failure, []byte(`""`)) {
							return fmt.Errorf("native query failed")
						}
						if _, ok := g["hits"]; !ok {
							return fmt.Errorf("missing query hits")
						}
						g["collection"], _ = json.Marshal(c.public())
					}
					results[i] = groups
					return nil
				})
			}
			if err := work.Wait(); err != nil {
				http.Error(w, "Search unavailable; check index coverage and server logs", 502)
				return
			}
			groups := []map[string]json.RawMessage{}
			for _, result := range results {
				groups = append(groups, result...)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"responses": groups})
			return
		}
		if r.URL.Path == "/pdf" || r.URL.Path == "/original" || r.URL.Path == "/api/status" {
			id := r.URL.Query().Get("collection")
			if id == "" {
				id = cs[0].ID
			}
			selected, err := selectCollections(id)
			if err != nil || len(selected) != 1 {
				http.Error(w, "Select one configured collection", 400)
				return
			}
			if r.URL.Path == "/pdf" && selected[0].Profile != "pdf" {
				http.NotFound(w, r)
				return
			}
			handlers[selected[0].ID].ServeHTTP(w, r)
			return
		}
		// Child muxes reject unknown paths and all mutation methods.
		handlers[cs[0].ID].ServeHTTP(w, r)
	})
	return boundExplorerRequests(router, 4)
}
