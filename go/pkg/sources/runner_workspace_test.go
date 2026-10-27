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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type workspaceRunnerFixture struct {
	runner                                                *Runner
	backend                                               *publicationFixture
	account                                               string
	listings, histories, bodies, attachments, extractions int
}

func newWorkspaceRunnerFixture(t *testing.T) *workspaceRunnerFixture {
	t.Helper()
	r, _, backend := runnerFixture(t)
	f := &workspaceRunnerFixture{runner: r, backend: backend, account: "alice@example.com"}
	r.Profile.Connections["docs"] = Connection{Provider: ProviderGoogleWorkspace, Services: []string{"gmail"}, CredentialsEnv: "WORKSPACE_CREDENTIALS"}
	r.Profile.Sources[0].Config = json.RawMessage(`{"resource":"gmail","label_ids":["INBOX"],"attachments":true}`)
	client := fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/gmail/profile":
			return 200, map[string]any{"emailAddress": f.account, "historyId": "10"}
		case "/gmail/labels/INBOX":
			return 200, map[string]any{"id": "INBOX"}
		case "/gmail/messages":
			if r.URL.Query().Get("q") != "" {
				return 200, map[string]any{}
			}
			f.listings++
			return 200, map[string]any{"messages": []any{map[string]any{"id": "message1"}}}
		case "/gmail/history":
			f.histories++
			return 200, map[string]any{"historyId": "20", "history": []any{}}
		case "/gmail/messages/message1":
			f.bodies++
			return 200, map[string]any{"id": "message1", "labelIds": []string{"INBOX"}, "payload": map[string]any{"mimeType": "multipart/mixed", "parts": []any{
				map[string]any{"partId": "0", "mimeType": "text/plain", "body": map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte("body"))}},
				map[string]any{"partId": "1", "filename": "notes.txt", "mimeType": "text/plain", "body": map[string]any{"attachmentId": "attachment1", "size": 5}},
			}}}
		case "/gmail/messages/message1/attachments/attachment1":
			f.attachments++
			return 200, map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte("notes")), "size": 5}
		default:
			t.Fatalf("unexpected provider request %s", r.URL)
			return 500, nil
		}
	})
	r.Factory = func(_ context.Context, connection Connection) (Connector, error) {
		return &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: connection.Services, HTTPClient: client, GmailBase: "https://fixture.test/gmail", ExtractorVersion: "runner-workspace-test-v1", Extractor: func(_ context.Context, _, _ string, data []byte) ([]byte, error) { f.extractions++; return data, nil }}, nil
	}
	return f
}

func TestWorkspaceRunnerConsentExpansionRetainsProgressAndBaseline(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "normalized"
		if legacy {
			name = "legacy-adoption"
		}
		t.Run(name, func(t *testing.T) {
			f := newWorkspaceRunnerFixture(t)
			f.backend.failAfterWrite = true
			require.Error(t, f.runner.Sync(t.Context()))
			path := filepath.Join(f.runner.StateDir, testSourceID+".json")
			var state runState
			require.NoError(t, loadJSON(path, &state, 256<<20))
			require.NotNil(t, state.Pending)
			acquisition := state.AcquisitionID
			require.NotEmpty(t, acquisition)
			if legacy {
				oldHash, err := sourceConfigurationHash(f.runner.Profile.Sources[0], f.runner.Profile.Connections["docs"])
				require.NoError(t, err)
				require.NotEqual(t, state.ConfigHash, oldHash)
				state.ConfigHash = oldHash
				require.NoError(t, writeJSON(path, state, 256<<20))
				require.NoError(t, f.runner.Sync(t.Context()), "unchanged legacy checkpoints adopt without reacquisition")
			}
			connection := f.runner.Profile.Connections["docs"]
			connection.Services = []string{"drive", "gmail"}
			f.runner.Profile.Connections["docs"] = connection
			require.NoError(t, f.runner.Sync(t.Context()), "unrelated consent must preserve pinned Gmail progress")
			require.Equal(t, 1, f.listings)
			require.Equal(t, 1, f.bodies)
			require.Equal(t, 1, f.attachments)
			require.Equal(t, 1, f.extractions)
			require.NoError(t, loadJSON(path, &state, 256<<20))
			require.NotNil(t, state.Published)
			require.Nil(t, state.Pending)
			require.Empty(t, state.AcquisitionID)
			connection.Services = []string{"calendar", "gmail", "drive"}
			f.runner.Profile.Connections["docs"] = connection
			histories := f.histories
			require.NoError(t, f.runner.Sync(t.Context()))
			require.Greater(t, f.histories, histories, "published inventory must retain its incremental cursor")
			require.Equal(t, 1, f.listings)
			require.Equal(t, 1, f.bodies)
			require.Equal(t, 1, f.attachments)
			require.Equal(t, 1, f.extractions)
			connection.Services = []string{"drive"}
			f.runner.Profile.Connections["docs"] = connection
			require.ErrorIs(t, f.runner.Sync(t.Context()), ErrInvalidInput, "selected-service loss must still fail live authorization")
			connection.Services = []string{"drive", "gmail"}
			f.runner.Profile.Connections["docs"] = connection
			f.account = "bob@example.com"
			require.ErrorIs(t, f.runner.Sync(t.Context()), ErrInvalidInput, "account drift cannot reuse the previous mailbox cursor")
			require.Equal(t, 1, f.listings)
		})
	}
}

func TestWorkspaceRunnerPinnedIdentityStillFencesDrift(t *testing.T) {
	for _, change := range []string{"credentials", "selection", "legacy-consent-without-proof"} {
		t.Run(change, func(t *testing.T) {
			f := newWorkspaceRunnerFixture(t)
			f.backend.failAfterWrite = true
			require.Error(t, f.runner.Sync(t.Context()))
			path := filepath.Join(f.runner.StateDir, testSourceID+".json")
			var before runState
			require.NoError(t, loadJSON(path, &before, 256<<20))
			switch change {
			case "credentials":
				connection := f.runner.Profile.Connections["docs"]
				connection.CredentialsEnv = "OTHER_CREDENTIALS"
				f.runner.Profile.Connections["docs"] = connection
			case "selection":
				f.runner.Profile.Sources[0].Config = json.RawMessage(`{"resource":"gmail","label_ids":["OTHER"],"attachments":true}`)
			case "legacy-consent-without-proof":
				oldHash, err := sourceConfigurationHash(f.runner.Profile.Sources[0], f.runner.Profile.Connections["docs"])
				require.NoError(t, err)
				before.ConfigHash = oldHash
				require.NoError(t, writeJSON(path, before, 256<<20))
				connection := f.runner.Profile.Connections["docs"]
				connection.Services = []string{"drive", "gmail"}
				f.runner.Profile.Connections["docs"] = connection
			}
			require.ErrorContains(t, f.runner.Sync(t.Context()), "source configuration changed during a pinned run")
			var after runState
			require.NoError(t, loadJSON(path, &after, 256<<20))
			require.Equal(t, before, after, "unproven drift must leave the pinned checkpoint intact")
			require.Equal(t, 1, f.listings)
			require.Equal(t, 1, f.attachments)
			require.Equal(t, 1, f.extractions)
		})
	}
}
