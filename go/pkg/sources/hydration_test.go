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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHydrationResumesAcrossConnectorRestarts(t *testing.T) {
	pages, details := t.TempDir(), t.TempDir()
	reads := map[string]int{}
	attempt := 0
	member := true
	client := fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/auth.test":
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		case "/conversations.info":
			return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": member}}
		case "/conversations.history":
			id := r.URL.Query().Get("oldest")
			if id == "" {
				return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "100.000001"}, map[string]any{"ts": "100.000002"}}}
			}
			reads[id]++
			if id == "100.000002" && attempt == 0 {
				return 429, nil
			}
			return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": id, "text": "private normalized body"}}}
		case "/chat.getPermalink":
			return 200, map[string]any{"ok": true}
		default:
			t.Fatalf("unexpected request %s", r.URL)
			return 500, nil
		}
	})
	connector := func() *CollaborationConnector {
		return &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", HTTPClient: client, Cache: &FileAcquisitionCache{Directory: pages}, ContentStore: &FileAcquisitionContentStore{Directory: details}}
	}
	request := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"],"include_threads":false}`)}
	_, err := connector().Enumerate(t.Context(), request)
	require.ErrorAs(t, err, new(*ProviderHTTPError))
	attempt++
	c := connector()
	inventory, err := c.Enumerate(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, inventory.Objects, 2)
	require.Equal(t, 1, reads["100.000001"])
	require.Equal(t, 2, reads["100.000002"])
	for _, item := range inventory.Objects {
		body, err := c.Read(t.Context(), request, item)
		require.NoError(t, err)
		require.Equal(t, "private normalized body", string(body))
	}
	require.Equal(t, 1, reads["100.000001"])
	require.Equal(t, 2, reads["100.000002"])
	files, err := os.ReadDir(pages)
	require.NoError(t, err)
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(pages, file.Name()))
		require.NoError(t, err)
		require.NotContains(t, string(data), "private normalized body")
	}
	member = false
	require.Error(t, c.ValidateInventory(t.Context(), request, inventory))
	// A corrupted private blob must fail closed even when SQL metadata is valid.
	member = true
	item := inventory.Objects[0]
	require.NoError(t, os.WriteFile(filepath.Join(details, acquisitionKey(item.ContentRef)+".json"), []byte(`{}`), 0600))
	_, err = connector().Read(t.Context(), request, item)
	require.ErrorContains(t, err, "digest mismatch")
}

func TestSlackUsesBodiesFromHistoryPages(t *testing.T) {
	calls := 0
	c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", Cache: &FileAcquisitionCache{Directory: t.TempDir()}, ContentStore: &FileAcquisitionContentStore{Directory: t.TempDir()}}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/auth.test":
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		case "/conversations.info":
			return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": true}}
		case "/conversations.history":
			calls++
			require.Empty(t, r.URL.Query().Get("oldest"))
			return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "100.000001", "text": "one"}, map[string]any{"ts": "100.000002", "text": "two"}}}
		case "/chat.getPermalink":
			return 200, map[string]any{"ok": true}
		default:
			t.Fatalf("unexpected request %s", r.URL)
			return 500, nil
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"],"include_threads":false}`)}
	inv, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, inv.Objects, 2)
	for _, item := range inv.Objects {
		_, err := c.Read(t.Context(), r, item)
		require.NoError(t, err)
	}
	require.Equal(t, 1, calls)
}

func TestAttachmentPreviewKeepsParentAndBoundedChildren(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace}
	r := Request{SourceNamespace: "preview/test", Preview: true}
	files := make([]collaborationFile, 20)
	for i := range files {
		files[i] = collaborationFile{ID: string(rune('a' + i)), Name: "note.txt", MIME: "text/plain", Data: []byte("attachment phrase")}
	}
	inputs, err := c.materialItems(t.Context(), r, CollaborationConfig{Attachments: true, preview: true}, collaborationRef{Resource: "gmail", Container: "alice@example.com", ID: "mail1"}, collaborationMaterial{Text: []byte("parent"), Files: files})
	require.NoError(t, err)
	require.Len(t, inputs, PreviewItemLimit)
	require.Equal(t, "parent", string(inputs[0].Bytes))
	require.Equal(t, "attachment phrase", string(inputs[1].Bytes))
}

func TestSourceRecordSchemaAcceptsEveryProvider(t *testing.T) {
	data, err := os.ReadFile("../../../zig/pkg/antfly-grep/schema/source-record-v1.json")
	require.NoError(t, err)
	var schema any
	require.NoError(t, json.Unmarshal(data, &schema))
	var providers []string
	var walk func(any)
	walk = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			for k, child := range v {
				if k == "enum" {
					if values, ok := child.([]any); ok {
						for _, value := range values {
							if value == ProviderGitHub {
								for _, p := range values {
									providers = append(providers, p.(string))
								}
							}
						}
					}
				}
				walk(child)
			}
		case []any:
			for _, child := range v {
				walk(child)
			}
		}
	}
	walk(schema)
	for _, provider := range []string{ProviderGitHub, ProviderGoogleDrive, ProviderGoogleWorkspace, ProviderSlack, ProviderDiscord, ProviderS3, ProviderGCS} {
		require.Contains(t, providers, provider)
	}
	require.NotContains(t, strings.Join(providers, ","), "content_ref")
}
