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
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStandalonePermissionLeaseUsesAuditStartAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ttl     int
		delay   time.Duration
		expired bool
	}{
		{"within budget", 60, 12 * time.Second, false},
		{"exact budget", 60, time.Minute, true},
		{"exceeds budget", 60, 90 * time.Second, true},
		{"default within budget", 0, 12 * time.Second, false},
		{"default exceeds budget", 0, 901 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root := t.TempDir()
				backend := &publicationFixture{records: map[string]map[string]any{}}
				destination := &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
					recorder := httptest.NewRecorder()
					backend.ServeHTTP(recorder, r)
					response := recorder.Result()
					response.Request = r
					return response, nil
				})}
				labels, listings := 0, 0
				var profileAt, auditStarted time.Time
				provider := fixtureClient(t, func(r *http.Request) (int, any) {
					switch r.URL.Path {
					case "/gmail/profile":
						profileAt = time.Now()
						return 200, map[string]any{"emailAddress": "alice@example.com", "historyId": "10"}
					case "/gmail/labels/INBOX":
						labels++
						if labels == 3 {
							auditStarted = profileAt
							time.Sleep(tc.delay)
						}
						return 200, map[string]any{"id": "INBOX"}
					case "/gmail/messages":
						if r.URL.Query().Get("q") != "" {
							return 200, map[string]any{}
						}
						listings++
						return 200, map[string]any{"messages": []any{}}
					default:
						t.Fatalf("unexpected provider request %s", r.URL)
						return 500, nil
					}
				})
				endpoint := "https://destination.test/api/v1"
				runner := &Runner{Profile: Profile{Version: 1, Endpoint: endpoint, Connections: map[string]Connection{"workspace": {Provider: ProviderGoogleWorkspace, Services: []string{"gmail"}}}, Sources: []Source{{ID: testSourceID, Connection: "workspace", Table: "sources", Config: SourceConfig(`{"resource":"gmail","label_ids":["INBOX"]}`), Permissions: &Permissions{Readers: []string{"user:self"}}, PermissionTTLSeconds: tc.ttl}}}, StateDir: filepath.Join(root, "state"), CatalogPath: filepath.Join(root, "catalog.json"), Publisher: &Publisher{Endpoint: endpoint, Token: "database-test-token", Client: destination}, Factory: func(context.Context, Connection) (Connector, error) {
					return &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, GmailBase: "https://fixture.test/gmail", HTTPClient: provider}, nil
				}}
				err := runner.Sync(t.Context())
				var catalog Catalog
				require.NoError(t, loadJSON(runner.CatalogPath, &catalog, 32<<20))
				var state runState
				statePath := filepath.Join(runner.StateDir, testSourceID+".json")
				require.NoError(t, loadJSON(statePath, &state, 256<<20))
				require.Equal(t, 3, labels)
				if tc.expired {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Equal(t, "stale", catalog.Sources[0].Status)
					require.Nil(t, catalog.Sources[0].ValidUntil)
					require.NotNil(t, state.Pending, "failed audits must preserve pinned acquisition for retry")
					require.Nil(t, state.Published)
					require.NotEmpty(t, state.AcquisitionID)
					require.NoError(t, runner.Sync(t.Context()), "a later fresh audit must recover the same acquisition")
					require.Equal(t, 1, listings, "recovery must not enumerate again")
				} else {
					require.NoError(t, err)
					ttl := tc.ttl
					if ttl == 0 {
						ttl = 900
					}
					require.Equal(t, auditStarted.Unix()+int64(ttl), *catalog.Sources[0].ValidUntil)
					require.Equal(t, auditStarted.Unix(), state.StartedAt)
					require.Nil(t, state.Pending)
					require.NotNil(t, state.Published)
				}
			})
		})
	}
}
