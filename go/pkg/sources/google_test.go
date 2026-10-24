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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGCSSourcePinsGenerationAndNeverPromotesTruncatedInventory(t *testing.T) {
	changed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/docs/o":
			fmt.Fprint(w, `{"items":[{"name":"manual.txt","generation":"123","size":"5","contentType":"text/plain"}]}`)
		case "/b/docs/o/manual.txt":
			require.Equal(t, "123", r.URL.Query().Get("generation"))
			require.Equal(t, "123", r.URL.Query().Get("ifGenerationMatch"))
			version := "123"
			if changed {
				version = "124"
			}
			w.Header().Set("X-Goog-Generation", version)
			fmt.Fprint(w, "hello")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := &GoogleConnector{ProviderName: ProviderGCS, APIBase: server.URL, HTTPClient: server.Client()}
	req := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"bucket":"docs"}`)}
	snapshot, err := c.Enumerate(context.Background(), req)
	require.NoError(t, err)
	require.True(t, snapshot.Authoritative)
	require.Len(t, snapshot.Objects, 1)
	result, err := c.ReadBatch(context.Background(), req, snapshot.Objects)
	require.NoError(t, err)
	require.NoError(t, result[0].ReadError)
	require.Equal(t, "hello", string(result[0].Bytes))
	changed = true
	result, err = c.ReadBatch(context.Background(), req, snapshot.Objects)
	require.NoError(t, err)
	require.Error(t, result[0].ReadError)
	require.Empty(t, result[0].Bytes)
	req.Config = SourceConfig(`{"bucket":"docs","max_objects":1}`)
	snapshot, err = c.Enumerate(context.Background(), req)
	require.NoError(t, err)
	require.False(t, snapshot.Authoritative)
}
func TestDriveSourceExportsPinnedRevisionAndRejectsPermissionOrVersionFailure(t *testing.T) {
	for _, mime := range []string{"application/vnd.google-apps.document", "application/vnd.google-apps.presentation"} {
		t.Run(mime, func(t *testing.T) {
			version := "7"
			moveDuringExport := false
			incomplete := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/files/folder":
					fmt.Fprint(w, `{"id":"folder","mimeType":"application/vnd.google-apps.folder"}`)
				case "/files":
					_ = json.NewEncoder(w).Encode(map[string]any{"incompleteSearch": incomplete, "files": []map[string]any{{"id": "doc", "name": "Plan", "mimeType": mime, "version": "7", "webViewLink": "https://docs.google.com/document/d/doc/edit"}}})
				case "/files/doc":
					fmt.Fprintf(w, `{"id":"doc","version":%q}`, version)
				case "/files/doc/export":
					require.Equal(t, "text/plain", r.URL.Query().Get("mimeType"))
					fmt.Fprint(w, "Plan\nExact export\n")
					if moveDuringExport {
						version = "8"
					}
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			c := &GoogleConnector{ProviderName: ProviderGoogleDrive, APIBase: server.URL, HTTPClient: server.Client()}
			req := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"folder_id":"folder"}`)}
			snapshot, err := c.Enumerate(context.Background(), req)
			require.NoError(t, err)
			require.True(t, snapshot.Authoritative)
			require.Equal(t, "drive://job/id/doc", snapshot.Objects[0].ID)
			inputs, err := c.ReadBatch(context.Background(), req, snapshot.Objects)
			require.NoError(t, err)
			require.NoError(t, inputs[0].ReadError)
			require.Equal(t, "Plan\nExact export\n", string(inputs[0].Bytes))
			moveDuringExport = true
			inputs, err = c.ReadBatch(context.Background(), req, snapshot.Objects)
			require.NoError(t, err)
			require.Error(t, inputs[0].ReadError)
			require.Empty(t, inputs[0].Bytes)
			sheet := snapshot.Objects[0]
			sheet.Metadata = cloneSourceMetadata(sheet.Metadata)
			sheet.Metadata["mime_type"] = "application/vnd.google-apps.spreadsheet"
			inputs, err = c.ReadBatch(context.Background(), req, []Item{sheet})
			require.NoError(t, err)
			require.ErrorContains(t, inputs[0].ReadError, "whole-workbook")
			incomplete = true
			_, err = c.Enumerate(context.Background(), req)
			require.Error(t, err)
		})
	}
}

func TestGoogleSourceRetriesStayScopedAcrossPages(t *testing.T) {
	for _, provider := range []string{ProviderGCS, ProviderGoogleDrive} {
		t.Run(provider, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/files/folder" {
					fmt.Fprint(w, `{"id":"folder","mimeType":"application/vnd.google-apps.folder"}`)
					return
				}
				keys := []string{"before.txt", "failed.txt"}
				if r.URL.Query().Get("pageToken") == "next" {
					keys = []string{"after.txt"}
				}
				objects := []map[string]any{}
				for _, key := range keys {
					if provider == ProviderGCS {
						objects = append(objects, map[string]any{"name": key, "generation": "1", "size": "1"})
					} else {
						objects = append(objects, map[string]any{"id": "id-" + key[:len(key)-4], "name": key, "mimeType": "text/plain", "version": "1", "size": "1"})
					}
				}
				field := "items"
				if provider == ProviderGoogleDrive {
					field = "files"
				}
				response := map[string]any{field: objects}
				if r.URL.Query().Get("pageToken") == "" {
					response["nextPageToken"] = "next"
				}
				require.NoError(t, json.NewEncoder(w).Encode(response))
			}))
			defer server.Close()
			c := &GoogleConnector{ProviderName: provider, APIBase: server.URL, HTTPClient: server.Client()}
			req := Request{Mode: ImportModeSource, Config: SourceConfig(`{"bucket":"docs","folder_id":"folder"}`), RetrySourceKeys: []string{"failed.txt"}}
			snapshot, err := c.Enumerate(t.Context(), req)
			require.NoError(t, err)
			require.False(t, snapshot.Authoritative)
			require.Len(t, snapshot.Objects, 1)
			require.Equal(t, "failed.txt", snapshot.Objects[0].Path)

			req.RetrySourceKeys = []string{"before.txt", "after.txt"}
			snapshot, err = c.Enumerate(t.Context(), req)
			require.NoError(t, err)
			require.Len(t, snapshot.Objects, 2)
			require.Equal(t, "before.txt", snapshot.Objects[0].Path)
			require.Equal(t, "after.txt", snapshot.Objects[1].Path)

			req.RetrySourceKeys = []string{"missing.txt"}
			_, err = c.Enumerate(t.Context(), req)
			require.ErrorContains(t, err, "no longer accessible")
		})
	}
}

func TestDriveRetriesUseStableIdentityAcrossDuplicatePathsAndRenames(t *testing.T) {
	secondName := "Notes.txt"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/files/folder" {
			fmt.Fprint(w, `{"id":"folder","mimeType":"application/vnd.google-apps.folder"}`)
			return
		}
		requests++
		if r.URL.Query().Get("pageToken") == "next" {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"files": []map[string]any{{"id": "second", "name": secondName, "mimeType": "text/plain", "version": "2", "size": "1"}}}))
		} else {
			fmt.Fprint(w, `{"files":[{"id":"first","name":"Notes.txt","mimeType":"text/plain","version":"1","size":"1"}],"nextPageToken":"next"}`)
		}
	}))
	defer server.Close()
	c := &GoogleConnector{ProviderName: ProviderGoogleDrive, APIBase: server.URL, HTTPClient: server.Client()}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"folder_id":"folder"}`)}
	full, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, full.Objects, 2)
	require.Equal(t, full.Objects[0].Path, full.Objects[1].Path)
	first, second := full.Objects[0].ID, full.Objects[1].ID
	require.NotEqual(t, first, second)

	r.RetrySourceKeys = []string{second}
	retry, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, retry.Objects, 1)
	require.Equal(t, second, retry.Objects[0].ID)
	secondName = "Renamed.txt"
	retry, err = c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, retry.Objects, 1)
	require.Equal(t, second, retry.Objects[0].ID)
	require.Equal(t, "Renamed.txt", retry.Objects[0].Path)

	// Legacy unique paths remain usable, but a collision discovered on a later
	// page must reject the entire selection, even with a bounded source config.
	r.RetrySourceKeys = []string{"Renamed.txt"}
	retry, err = c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, retry.Objects, 1)
	require.Equal(t, second, retry.Objects[0].ID)
	secondName = "Notes.txt"
	r.Config = SourceConfig(`{"folder_id":"folder","max_objects":1}`)
	r.RetrySourceKeys = []string{"Notes.txt"}
	requests = 0
	_, err = c.Enumerate(t.Context(), r)
	require.ErrorContains(t, err, "ambiguous legacy Drive retry path")
	require.Equal(t, 2, requests, "must establish uniqueness beyond the first matching page")
	r.RetrySourceKeys = []string{first, "Notes.txt"}
	_, err = c.Enumerate(t.Context(), r)
	require.ErrorContains(t, err, "ambiguous legacy Drive retry path")
	r.RetrySourceKeys = []string{"drive://job/other/second"}
	_, err = c.Enumerate(t.Context(), r)
	require.ErrorContains(t, err, "no longer accessible")
}

func TestDriveNamesAreEncodedAsLogicalPathSegments(t *testing.T) {
	require.Equal(t, "Design%2F2026", DrivePathSegment("Design/2026"))
	require.Equal(t, "Design%252F2026", DrivePathSegment("Design%2F2026"))
	require.Equal(t, "Quarterly notes", DrivePathSegment("Quarterly notes"))
}

func TestGCSStagedAccessIsBoundToPinnedGeneration(t *testing.T) {
	for _, state := range []string{"readable", "revoked", "deleted", "wrong-generation"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, "1", r.URL.Query().Get("generation"))
				require.Equal(t, "1", r.URL.Query().Get("ifGenerationMatch"))
				switch state {
				case "revoked":
					w.WriteHeader(403)
				case "deleted":
					w.WriteHeader(404)
				case "wrong-generation":
					fmt.Fprint(w, `{"generation":"2"}`)
				default:
					fmt.Fprint(w, `{"generation":"1"}`)
				}
			}))
			defer server.Close()
			connector := GoogleConnector{ProviderName: ProviderGCS, APIBase: server.URL, HTTPClient: server.Client()}
			err := connector.CheckAccess(t.Context(), Request{Config: SourceConfig(`{"bucket":"docs"}`)}, Item{Path: "private.txt", Revision: "1", Metadata: map[string]any{"bucket": "docs"}})
			if state == "readable" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, 1, calls)
		})
	}
}
