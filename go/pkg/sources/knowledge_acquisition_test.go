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

package sources

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalKnowledgeFencesAcquisitionAndConnection(t *testing.T) {
	changes := map[string]func(*Service){
		"selection": func(s *Service) {
			source, err := (localManagement{s}).configuredSource(testSourceID)
			require.NoError(t, err)
			source.Config = json.RawMessage(`{"bucket":"other"}`)
			w := serviceCall(s, "PUT", "/v1/sources/"+testSourceID, source)
			require.Equal(t, 200, w.Code, w.Body.String())
		},
		"permissions": func(s *Service) {
			source, err := (localManagement{s}).configuredSource(testSourceID)
			require.NoError(t, err)
			source.Permissions = &Permissions{Public: true}
			w := serviceCall(s, "PUT", "/v1/sources/"+testSourceID, source)
			require.Equal(t, 200, w.Code, w.Body.String())
		},
		"connection": func(s *Service) {
			s.mu.Lock()
			connection := s.profile.Connections["docs"]
			connection.S3Endpoint = "https://different.example"
			s.profile.Connections["docs"] = connection
			s.mu.Unlock()
		},
		"destination": func(s *Service) { s.mu.Lock(); s.profile.Endpoint = "http://127.0.0.1:1"; s.mu.Unlock() },
		"keying":      func(s *Service) { s.mu.Lock(); s.profile.Sources[0].Identity.Namespace = "different"; s.mu.Unlock() },
	}
	for name, change := range changes {
		for _, during := range []bool{false, true} {
			suffix := "before"
			if during {
				suffix = "during"
			}
			t.Run(name+"/"+suffix, func(t *testing.T) {
				s, _, _ := serviceFixture(t)
				plan, k := knowledgeFixture(t)
				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if strings.HasSuffix(r.URL.Path, "/query") {
						calls++
						if during {
							change(s)
						}
						w.Write([]byte(`{"responses":[{"graph_results":{"source_knowledge":{"nodes":[]}}}],"live_read":true}`))
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"config": plan.Indexes[0].Config, "status": map[string]any{"resolver_replay": map[string]any{"resolvers": plan.Indexes[0].Config["resolvers"]}, "readiness": map[string]any{"complete": true, "queryable": true}}})
				}))
				defer server.Close()
				s.mu.Lock()
				s.knowledgeAudience = "research"
				s.profile.Endpoint = server.URL
				s.profile.Sources[0].Table = "docs"
				s.profile.Sources[0].Knowledge = &k
				s.mu.Unlock()
				snapshot, err := (localManagement{s}).knowledgeSnapshot(testSourceID)
				require.NoError(t, err)
				until := time.Now().Add(time.Minute).Unix()
				binding := Binding{ID: testSourceID, Table: "docs", Knowledge: &k, Status: "searchable", ValidUntil: &until, AcquisitionID: snapshot.acquisition}
				require.NoError(t, writeJSON(filepath.Join(s.runnerPath(), "catalog.json"), Catalog{Version: 2, Endpoint: server.URL, Sources: []Binding{binding}}, 32<<20))
				if !during {
					change(s)
				}
				raw, err := (localManagement{s}).QueryKnowledge(t.Context(), testSourceID, KnowledgeRequest{Graph: "facts", Start: []string{"alice"}})
				require.Error(t, err)
				require.Nil(t, raw)
				expected := "knowledge_acquisition_not_current"
				if during {
					expected = "knowledge_changed_during_query"
					require.Equal(t, 1, calls)
				} else {
					require.Zero(t, calls)
				}
				require.ErrorContains(t, err, expected)
			})
		}
	}
}

func TestKnowledgeDiscoveryAdoptsOnlyMatchingAcquisition(t *testing.T) {
	runner, _, _ := runnerFixture(t)
	require.NoError(t, runner.Sync(t.Context()))
	var original Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &original, 32<<20))
	require.Empty(t, original.Sources[0].AcquisitionID, "stream-only publications keep their existing wire shape")
	_, knowledge := knowledgeFixture(t)
	runner.Profile.Sources[0].Knowledge = &knowledge
	runner.KnowledgeAudience = "research"
	require.NoError(t, runner.RefreshSearchDiscovery(t.Context(), testSourceID))
	var published Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &published, 32<<20))
	expected, err := sourceAcquisitionIdentity(runner.Profile.Sources[0], runner.Profile.Connections["docs"])
	require.NoError(t, err)
	require.Equal(t, expected, published.Sources[0].AcquisitionID)
	require.Equal(t, original.Sources[0].PublishedRevision, published.Sources[0].PublishedRevision)
	require.Equal(t, original.Sources[0].ValidUntil, published.Sources[0].ValidUntil)
	runner.Profile.Sources[0].Config = json.RawMessage(`{"bucket":"other"}`)
	require.NoError(t, runner.RefreshSearchDiscovery(t.Context(), testSourceID))
	var stale Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &stale, 32<<20))
	require.Equal(t, published, stale, "changed acquisition cannot acquire a new publication witness without running")
}

func TestAcquisitionWitnessIgnoresJSONLayoutWithoutRounding(t *testing.T) {
	source := Source{ID: testSourceID, Config: json.RawMessage(`{"bucket":"docs","revision":9007199254740993}`)}
	connection := Connection{Provider: ProviderS3}
	before, err := sourceAcquisitionIdentity(source, connection)
	require.NoError(t, err)
	source.Config = json.RawMessage(`{ "revision": 9007199254740993, "bucket": "docs" }`)
	after, err := sourceAcquisitionIdentity(source, connection)
	require.NoError(t, err)
	require.Equal(t, before, after)
	source.Config = json.RawMessage(`{"bucket":"docs","revision":9007199254740992}`)
	different, err := sourceAcquisitionIdentity(source, connection)
	require.NoError(t, err)
	require.NotEqual(t, before, different)
}
