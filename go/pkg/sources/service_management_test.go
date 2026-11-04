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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
)

func managementCall(s *Service, method, path, key string, input any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(input)
	r := httptest.NewRequest(method, "/sources/v1"+path, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+serviceTestToken)
	r.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func TestManagementIdempotencyHistoryRestartAndIsolation(t *testing.T) {
	s, _, _ := serviceFixture(t)
	path := "/sources/" + testSourceID + "/runs"
	first := managementCall(s, "POST", path, "first", map[string]any{})
	require.Equal(t, 202, first.Code, first.Body.String())
	var run management.Run
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &run))
	require.Equal(t, testSourceID, run.SourceId)
	require.Equal(t, "completed", waitServiceJob(t, s, run.Id).State)
	again := managementCall(s, "POST", path, "first", map[string]any{})
	require.Equal(t, 202, again.Code, again.Body.String())
	var repeated management.Run
	require.NoError(t, json.Unmarshal(again.Body.Bytes(), &repeated))
	require.Equal(t, run.Id, repeated.Id)
	require.Equal(t, 409, managementCall(s, "POST", path, "first", map[string]any{"restart": true}).Code)
	next := managementCall(s, "POST", path, "second", map[string]any{})
	require.Equal(t, 202, next.Code, next.Body.String())
	var second management.Run
	require.NoError(t, json.Unmarshal(next.Body.Bytes(), &second))
	waitServiceJob(t, s, second.Id)
	require.NotEqual(t, run.Id, second.Id)
	require.Equal(t, 200, managementCall(s, "GET", path+"/"+run.Id, "", nil).Code)
	require.Equal(t, 404, managementCall(s, "GET", "/sources/00000000-0000-4000-8000-000000000099/runs/"+run.Id, "", nil).Code)
	var page management.RunPage
	response := managementCall(s, "GET", path+"?limit=1", "", nil)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
	require.Len(t, page.Data, 1)
	require.NotEmpty(t, page.NextCursor)
	require.Equal(t, second.Id, page.Data[0].Id)
	response = managementCall(s, "GET", path+"?cursor="+page.NextCursor, "", nil)
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
	require.Equal(t, run.Id, page.Data[0].Id)
	profile := cloneProfile(s.profile)
	directory := s.directory
	factory := s.factory
	require.NoError(t, s.Close())
	restarted, err := NewService(ServiceOptions{Profile: profile, StateDir: directory, PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: factory})
	require.NoError(t, err)
	defer restarted.Close()
	replay, err := (localManagement{restarted}).StartRun(t.Context(), testSourceID, "first", management.RunInput{Operation: "sync"})
	require.NoError(t, err)
	require.Equal(t, run.Id, replay.Id)
}
func TestSourceScopedRunPreservesOtherCatalogBindings(t *testing.T) {
	runner, _, _ := runnerFixture(t)
	sibling := "00000000-0000-4000-8000-000000000099"
	runner.Profile.Sources = append(runner.Profile.Sources, Source{ID: sibling, Alias: "sibling", Connection: "docs", Table: "sources", Config: json.RawMessage(`{"bucket":"docs"}`), Permissions: &Permissions{Readers: []string{"user:self"}}})
	previous := Catalog{Version: 2, Endpoint: runner.Profile.Endpoint, Principals: []string{"user:self"}, Sources: []Binding{{ID: sibling, Alias: "sibling", Table: "sources", DocumentVersion: SourceRecordVersion, Fields: CanonicalDocumentFields(), Filter: SourceFilter(sibling), Status: "searchable", PublishedRevision: "sibling-revision"}}}
	require.NoError(t, writeJSON(runner.CatalogPath, previous, 32<<20))
	runner.SourceID = testSourceID
	require.NoError(t, runner.Sync(context.Background()))
	var catalog Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &catalog, 32<<20))
	require.Len(t, catalog.Sources, 2)
	require.Equal(t, "sibling-revision", catalog.Sources[1].PublishedRevision)
	require.Equal(t, "searchable", catalog.Sources[1].Status)
	var state runState
	require.Error(t, loadJSON(filepath.Join(runner.StateDir, sibling+".json"), &state, 1<<20))
}
func TestManagementPurgeTargetsOnlyRequestedSource(t *testing.T) {
	s, _, _ := serviceFixture(t)
	source := s.profile.Sources[0]
	sibling := source
	sibling.ID = "00000000-0000-4000-8000-000000000099"
	s.mu.Lock()
	require.NoError(t, s.queuePurge(source))
	require.NoError(t, s.queuePurge(sibling))
	s.profile.Sources = nil
	s.mu.Unlock()
	response := managementCall(s, "POST", "/sources/"+testSourceID+"/runs", "purge", map[string]any{"operation": "purge"})
	require.Equal(t, 202, response.Code, response.Body.String())
	var run management.Run
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &run))
	require.Equal(t, "completed", waitServiceJob(t, s, run.Id).State)
	queue, err := s.readPurges()
	require.NoError(t, err)
	require.Len(t, queue.Sources, 1)
	require.Equal(t, sibling.ID, queue.Sources[0].ID)
}

func TestSourceScopedRunIgnoresSiblingCredentialRevocation(t *testing.T) {
	s, _, _ := serviceFixture(t)
	s.mu.Lock()
	source := s.profile.Sources[0]
	source.ID = "00000000-0000-4000-8000-000000000099"
	source.Connection = "sibling"
	s.profile.Sources = append(s.profile.Sources, source)
	s.profile.Connections["sibling"] = Connection{Provider: ProviderS3, SecretRefs: map[string]string{"token": "sibling-token"}}
	s.job = &SyncJob{SourceID: testSourceID, Operation: "sync", State: "running"}
	cancelled := 0
	s.cancel = func() { cancelled++ }
	s.cancelConnectionAcquisition("sibling")
	s.cancelSecretAcquisition("sibling-token")
	require.Zero(t, cancelled)
	s.cancelConnectionAcquisition("docs")
	require.Equal(t, 1, cancelled)
	s.cancel = nil
	s.mu.Unlock()
}

func TestMediaProtocolSeparatesRevocationFromProviderOutage(t *testing.T) {
	for _, tc := range []struct{ status, want int }{{403, 410}, {404, 410}, {429, 503}, {503, 503}} {
		w := httptest.NewRecorder()
		WriteMediaError(w, &GoogleHTTPError{Status: tc.status})
		require.Equal(t, tc.want, w.Code)
	}
}

func TestSourceScopedRunStartsAfterSiblingConnectionIsRevoked(t *testing.T) {
	s, _, _ := serviceFixture(t)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/sibling", Connection{Provider: ProviderS3}).Code)
	sibling := s.profile.Sources[0]
	sibling.ID = "00000000-0000-4000-8000-000000000099"
	sibling.Connection = "sibling"
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+sibling.ID, sibling).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/sibling", nil).Code)
	response := managementCall(s, "POST", "/sources/"+testSourceID+"/runs", "selected-only", map[string]any{})
	require.Equal(t, 202, response.Code, response.Body.String())
	var run management.Run
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &run))
	require.Equal(t, "completed", waitServiceJob(t, s, run.Id).State)
}

func TestManagementSourcePagesBoundEncodedBytes(t *testing.T) {
	s, _, _ := serviceFixture(t)
	s.mu.Lock()
	template := s.profile.Sources[0]
	s.profile.Sources = nil
	for i := range 30 {
		item := template
		item.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
		item.Config = json.RawMessage(`{"bucket":"` + strings.Repeat("x", 100000) + `"}`)
		s.profile.Sources = append(s.profile.Sources, item)
	}
	s.mu.Unlock()
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		response := managementCall(s, "GET", "/sources?limit=100&cursor="+cursor, "", nil)
		require.Equal(t, 200, response.Code, response.Body.String())
		require.LessOrEqual(t, response.Body.Len(), management.MaxResponseBytes)
		var page management.SourcePage
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
		require.NotEmpty(t, page.Data)
		for _, item := range page.Data {
			require.False(t, seen[item.Id])
			seen[item.Id] = true
		}
		pages++
		if page.NextCursor == "" {
			break
		}
		require.NotEqual(t, cursor, page.NextCursor)
		cursor = page.NextCursor
	}
	require.Len(t, seen, 30)
	require.Greater(t, pages, 1)
}

func TestSourceScopedRunDoesNotUpgradeUnpublishedSiblingLayout(t *testing.T) {
	runner, _, _ := runnerFixture(t)
	sibling := "00000000-0000-4000-8000-000000000099"
	runner.Profile.Sources = append(runner.Profile.Sources, Source{ID: sibling, Connection: "docs", Table: "sources", Config: json.RawMessage(`{"bucket":"docs"}`), Permissions: &Permissions{Readers: []string{"user:self"}}})
	prior := Catalog{Version: 2, Endpoint: runner.Profile.Endpoint, Principals: []string{"user:self"}, Sources: []Binding{{ID: sibling, Table: "sources", DocumentVersion: 1, DocumentField: "grep", Status: "searchable", PublishedRevision: "old-layout", Filter: SourceFilter(sibling)}}}
	require.NoError(t, writeJSON(runner.CatalogPath, prior, 32<<20))
	runner.SourceID = testSourceID
	require.NoError(t, runner.Sync(t.Context()))
	var catalog Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &catalog, 32<<20))
	require.Empty(t, catalog.Sources[1].PublishedRevision)
	require.NotEqual(t, "searchable", catalog.Sources[1].Status)
}
