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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

// TestGCSRelativeKeyStripsConfiguredPrefix pins that a GCS item's RelativeKey
// is relative to the source's own root (its configured prefix), not the
// bucket: Path keeps the full object name (unchanged, so canonical gs:// URIs
// are unaffected) while RelativeKey strips config.Prefix, and setting
// identity.namespace to today's provider-ID prefix must reconstruct the
// provider ID byte for byte (the invariant IdentityKey relies on). Drive,
// by contrast, has no path-like root of its own: its items get no explicit
// RelativeKey and fall back to their native Drive file ID (see
// relativeKeyFor in document.go), since moving a Drive item across
// providers is never a meaningful operation to begin with.
func TestGCSRelativeKeyStripsConfiguredPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"name":"team-a/manual.txt","generation":"123","size":"5","contentType":"text/plain"}]}`)
	}))
	defer server.Close()
	c := &GoogleConnector{ProviderName: ProviderGCS, APIBase: server.URL, HTTPClient: server.Client()}
	req := Request{Mode: ImportModeSource, SourceNamespace: "job/" + testSourceID, Config: SourceConfig(`{"bucket":"docs","prefix":"team-a/"}`)}
	inventory, err := c.Enumerate(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, inventory.Objects, 1)
	item := inventory.Objects[0]
	require.Equal(t, "team-a/manual.txt", item.Path, "Path keeps the full bucket object name")
	require.Equal(t, "manual.txt", item.RelativeKey, "RelativeKey strips the configured prefix")

	namespace := strings.TrimSuffix(item.ID, "/"+item.RelativeKey)
	require.Equal(t, item.ID, namespace+"/"+item.RelativeKey)
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

func TestGCSProjectedPageProvenance(t *testing.T) {
	text := []byte("one repaired page\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/projection/o":
			if r.URL.Query().Get("maxResults") != "100" {
				t.Error("provenance listing did not bound metadata page size")
			}
			if !strings.Contains(r.URL.Query().Get("fields"), "metadata") {
				t.Error("listing did not request page provenance")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
				"name": "pages/page.txt", "generation": "1", "metageneration": "7", "size": fmt.Sprint(len(text)), "contentType": "text/plain",
				"metadata": map[string]string{
					"original_pdf_uri": "gs://original/source.pdf?generation=42",
					"original_page":    "12", "extraction_revision": "3",
					"content_sha256": fmt.Sprintf("%x", sha256.Sum256(text)),
				},
			}}})
		case "/b/projection/o/pages/page.txt":
			if r.URL.Query().Get("generation") != "1" || r.URL.Query().Get("ifGenerationMatch") != "1" || r.URL.Query().Get("ifMetagenerationMatch") != "7" {
				t.Error("read did not pin the projection generation and metadata")
			}
			if r.URL.Query().Get("alt") != "media" {
				_, _ = w.Write([]byte(`{"generation":"1"}`))
				return
			}
			w.Header().Set("X-Goog-Generation", "1")
			_, _ = w.Write(text)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	connector := GoogleConnector{ProviderName: ProviderGCS, HTTPClient: server.Client(), APIBase: server.URL}
	request := Request{Mode: ImportModeSource, SourceNamespace: "test", Config: SourceConfig(`{"bucket":"projection","prefix":"pages/","page_provenance":true}`)}
	inventory, err := connector.Enumerate(t.Context(), request)
	if err != nil || len(inventory.Objects) != 1 {
		t.Fatalf("page listing: %d objects, error = %v", len(inventory.Objects), err)
	}
	item := inventory.Objects[0]
	if err := connector.CheckAccess(t.Context(), request, item); err != nil {
		t.Fatal(err)
	}
	data, err := connector.Read(t.Context(), request, item)
	if err != nil {
		t.Fatal(err)
	}

	doc, err := Normalize("test-source", item, data, Permissions{}, time.Time{})
	if err != nil || doc.URI != "gs://original/source.pdf?generation=42#page=12" || doc.Metadata["projection_uri"] != item.URL {
		t.Fatalf("projected page citation = %q, error = %v", doc.URI, err)
	}
	if _, err := Normalize("test-source", item, []byte("different text"), Permissions{}, time.Time{}); err == nil {
		t.Fatal("mismatched page text was accepted")
	}
	for _, uri := range []string{
		"gs://original/source.pdf",
		"gs://original/?generation=42",
		"gs://original/source.pdf?generation=0",
		"gs://original/source.pdf?generation=42&generation=43",
		"https://original.example/source.pdf",
	} {
		item.Metadata["original_pdf_uri"] = uri
		_, err := Normalize("test-source", item, text, Permissions{}, time.Time{})
		require.Error(t, err, uri)
	}
}

func TestGCSPreviewBoundsFilteredListing(t *testing.T) {
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages++
		if r.URL.Query().Get("maxResults") != "100" {
			t.Error("preview requested a full listing page")
		}
		fmt.Fprintf(w, `{"items":[{"name":"pages/skipped.pdf","size":"1","generation":"1"}],"nextPageToken":"%d"}`, pages)
	}))
	defer server.Close()
	connector := GoogleConnector{ProviderName: ProviderGCS, HTTPClient: server.Client(), APIBase: server.URL}
	request := Request{Mode: ImportModeSource, SourceNamespace: "test", Preview: true, Config: SourceConfig(`{"bucket":"docs","prefix":"pages/","suffixes":[".txt"]}`)}
	_, err := connector.Enumerate(t.Context(), request)
	if pages != 4 || err == nil || !strings.Contains(err.Error(), "preview sample exhausted") {
		t.Fatalf("expected bounded preview, got %d pages and %v", pages, err)
	}
}

func TestGCSPageProvenanceRequiresTextPublication(t *testing.T) {
	connector := &GoogleConnector{ProviderName: ProviderGCS}
	config := SourceConfig(`{"bucket":"projection","prefix":"pages/","page_provenance":true}`)
	for _, publication := range []string{"", "native", "media", "text"} {
		_, err := PublicationConnector(connector, Source{Config: config, Publication: publication})
		if (err == nil) != (publication == "text") {
			t.Errorf("publication %q returned %v", publication, err)
		}
	}
}

func TestGCSBatchTruncationIsDistinctFromMismatchedResponse(t *testing.T) {
	for _, tc := range []struct {
		name, id, tail string
		interrupted    bool
	}{
		{"truncated", "<response-0>", "", true},
		{"mismatched identity", "<response-1>", "\r\n--fixture--\r\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "multipart/mixed; boundary=fixture")
				fmt.Fprintf(w, "--fixture\r\nContent-Type: application/http\r\nContent-ID: %s\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 18\r\n\r\n%s%s", tc.id, `{"generation":"1"}`, tc.tail)
			}))
			defer server.Close()
			c := GoogleConnector{ProviderName: ProviderGCS, APIBase: server.URL, HTTPClient: server.Client()}
			_, err := c.CheckGCSAccessBatch(t.Context(), Request{Config: SourceConfig(`{"bucket":"docs"}`)},
				[]Item{{Path: "p.txt", Revision: "1", Metadata: map[string]any{"bucket": "docs"}}})
			require.Error(t, err)
			if tc.interrupted {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			} else {
				require.NotErrorIs(t, err, io.ErrUnexpectedEOF)
			}
		})
	}
}

func TestGCSProjectedListingSkipsMarkersAndPreservesLiteralObjectKeys(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.EscapedPath() {
		case "/b/docs/o":
			fmt.Fprint(w, `{"items":[{"name":"pages/","size":"0","generation":"1"},{"name":"pages/a//b.txt","size":"1","generation":"2","metageneration":"1"}]}`)
		case "/b/docs/o/pages%2Fa%2F%2Fb.txt":
			if r.URL.Query().Get("alt") == "media" {
				w.Header().Set("X-Goog-Generation", "2")
				fmt.Fprint(w, "x")
			} else {
				fmt.Fprint(w, `{"generation":"2"}`)
			}
		default:
			t.Errorf("unexpected escaped path %q", r.URL.EscapedPath())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := GoogleConnector{ProviderName: ProviderGCS, APIBase: server.URL, HTTPClient: server.Client()}
	for _, preview := range []bool{true, false} {
		request := Request{Mode: ImportModeSource, Preview: preview, SourceNamespace: "test",
			Config: SourceConfig(`{"bucket":"docs","prefix":"pages/","page_provenance":true}`)}
		inventory, err := c.Enumerate(t.Context(), request)
		require.NoError(t, err)
		require.Len(t, inventory.Objects, 1)
		item := inventory.Objects[0]
		require.Equal(t, "pages/a//b.txt", item.Path)
		payload, err := c.Read(t.Context(), request, item)
		require.NoError(t, err)
		require.Equal(t, "x", string(payload))
		require.NoError(t, c.CheckAccess(t.Context(), request, item))
	}
	exact := Request{Mode: ImportModeSource, SourceNamespace: "test",
		Config: SourceConfig(`{"bucket":"docs","key":"pages/","page_provenance":true}`)}
	inventory, err := c.Enumerate(t.Context(), exact)
	require.NoError(t, err)
	require.True(t, inventory.Authoritative)
	require.Len(t, inventory.Objects, 1)
	require.Equal(t, "pages/", inventory.Objects[0].Path)
	ordinary := exact
	ordinary.Config = SourceConfig(`{"bucket":"docs","prefix":"pages/"}`)
	inventory, err = c.Enumerate(t.Context(), ordinary)
	require.NoError(t, err)
	require.True(t, inventory.Authoritative)
	require.Len(t, inventory.Objects, 1)
	require.Equal(t, "pages/a//b.txt", inventory.Objects[0].Path)
}

func TestGCSBatchRejectsShortMemberBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "multipart/mixed; boundary=fixture")
		fmt.Fprint(w, "--fixture\r\nContent-Type: application/http\r\nContent-ID: <response-0>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\n")
		fmt.Fprint(w, `{"generation":"1"}`+"\r\n--fixture--\r\n")
	}))
	defer server.Close()
	c := GoogleConnector{ProviderName: ProviderGCS, APIBase: server.URL, HTTPClient: server.Client()}
	results, err := c.CheckGCSAccessBatch(t.Context(), Request{Config: SourceConfig(`{"bucket":"docs"}`)},
		[]Item{{Path: "p.txt", Revision: "1", Metadata: map[string]any{"bucket": "docs"}}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0], io.ErrUnexpectedEOF)
}

func TestGCSProjectedPublicationResumesThroughServiceADC(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	connection := Connection{Provider: ProviderGCS, GoogleADC: true}
	runner.Profile.Connections["docs"] = connection
	runner.Profile.Sources[0].Config = SourceConfig(`{"bucket":"docs","page_provenance":true}`)
	runner.Profile.Sources[0].Publication = "text"
	runner.Profile.DataPlaneTokenEnv = "SOURCE_SERVICE_TEST_DB_TOKEN"
	t.Setenv("SOURCE_SERVICE_TEST_DB_TOKEN", "database-test-token")

	const payload = "one projected page"
	const pageCount = projectedPageBatchCount + 1
	var reads, batches atomic.Int32
	var metadataChanged, failLastRead atomic.Bool
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"adc-fixture","token_type":"Bearer","expires_in":3600}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer adc-fixture" {
			http.Error(w, "missing ADC token", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/b/docs/o":
			if r.URL.Query().Get("maxResults") == "1" {
				fmt.Fprint(w, `{"items":[{"name":"page.txt"}]}`)
				return
			}
			items := make([]map[string]any, 0, pageCount)
			for i := 1; i <= pageCount; i++ {
				items = append(items, map[string]any{
					"name": fmt.Sprintf("page-%02d.txt", i), "generation": "1", "metageneration": "7",
					"size": fmt.Sprint(len(payload)), "contentType": "text/plain",
					"metadata": map[string]string{
						"original_pdf_uri": "gs://original/report.pdf?generation=42",
						"original_page":    "12", "extraction_revision": "3",
						"content_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(payload))),
					},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
		case "/batch/storage/v1":
			batches.Add(1)
			body, err := io.ReadAll(r.Body)
			count := strings.Count(string(body), "generation=1&ifGenerationMatch=1&fields=generation&ifMetagenerationMatch=7")
			if err != nil || count == 0 || count != strings.Count(string(body), "GET /storage/v1") {
				t.Error("batch did not check pinned content and metadata")
			}
			w.Header().Set("Content-Type", "multipart/mixed; boundary=fixture")
			status, proof := "200 OK", `{"generation":"1"}`
			if metadataChanged.Load() {
				status, proof = "412 Precondition Failed", `{"error":{"message":"metadata changed"}}`
			}
			for i := 0; i < count; i++ {
				fmt.Fprintf(w, "--fixture\r\nContent-Type: application/http\r\nContent-ID: <response-%d>\r\n\r\nHTTP/1.1 %s\r\nContent-Length: %d\r\n\r\n%s\r\n", i, status, len(proof), proof)
			}
			fmt.Fprint(w, "--fixture--\r\n")
		default:
			if !strings.HasPrefix(r.URL.Path, "/b/docs/o/page-") {
				http.NotFound(w, r)
				return
			}
			reads.Add(1)
			if failLastRead.Load() && r.URL.Path == fmt.Sprintf("/b/docs/o/page-%02d.txt", pageCount) {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if metadataChanged.Load() || r.URL.Query().Get("ifMetagenerationMatch") != "7" {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			w.Header().Set("X-Goog-Generation", "1")
			fmt.Fprint(w, payload)
		}
	}))
	defer storage.Close()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	credentials, err := json.Marshal(map[string]string{
		"type": "service_account", "client_email": "fixture@example.iam.gserviceaccount.com",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
		"token_uri":   storage.URL + "/token",
	})
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "adc.json")
	require.NoError(t, os.WriteFile(file, credentials, 0600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", file)

	service, err := NewService(ServiceOptions{Profile: runner.Profile, StateDir: t.TempDir(),
		PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner.Factory = func(ctx context.Context, connection Connection) (Connector, error) {
		connector, err := service.providerConnector(ctx, connection)
		if err != nil {
			return nil, err
		}
		google := *connector.(*GoogleConnector)
		google.APIBase = storage.URL
		return &google, nil
	}

	assertRestart := func(err error) {
		t.Helper()
		require.Error(t, err)
		failure := describeSyncFailure(err)
		require.Equal(t, "source_version_changed", failure.Code)
		require.True(t, failure.RestartRequired)
		require.False(t, failure.Retryable)
	}
	metadataChanged.Store(true)
	assertRestart(runner.Sync(t.Context()))
	require.EqualValues(t, 1, reads.Load())
	require.Zero(t, batches.Load())
	metadataChanged.Store(false)

	failLastRead.Store(true)
	require.Error(t, runner.Sync(t.Context()))
	require.EqualValues(t, 1+pageCount, reads.Load())
	require.Zero(t, batches.Load())
	backend.mu.Lock()
	require.Equal(t, 1, backend.contentMerges)
	require.Len(t, backend.records, projectedPageBatchCount)
	backend.mu.Unlock()
	failLastRead.Store(false)

	staleKey, err := RecordKey(testSourceID, "gcs://job/"+testSourceID+"/docs/removed.txt")
	require.NoError(t, err)
	backend.mu.Lock()
	backend.records[staleKey] = map[string]any{"source_id": testSourceID}
	backend.dropCleanupAck = true
	backend.mu.Unlock()
	require.Error(t, runner.Sync(t.Context()))
	require.EqualValues(t, 2+pageCount, reads.Load())
	require.EqualValues(t, 1, batches.Load())

	metadataChanged.Store(true)
	assertRestart(runner.Sync(t.Context()))
	require.EqualValues(t, 2, batches.Load())
	require.EqualValues(t, 2+pageCount, reads.Load())
	metadataChanged.Store(false)

	// A resumed acquisition may be older than its permission lease. Final
	// metadata validation, rather than enumeration, starts the published lease.
	statePath := filepath.Join(runner.StateDir, testSourceID+".json")
	var pending runState
	require.NoError(t, loadJSON(statePath, &pending, 256<<20))
	pending.StartedAt = time.Now().Add(-24 * time.Hour).Unix()
	require.NoError(t, writeJSON(statePath, pending, 256<<20))
	before := time.Now().Unix()
	require.NoError(t, runner.Sync(t.Context()))
	var published Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &published, 32<<20))
	require.NotNil(t, published.Sources[0].ValidUntil)
	require.Greater(t, *published.Sources[0].ValidUntil, before)
	require.EqualValues(t, 4, batches.Load())
	require.EqualValues(t, 2+pageCount, reads.Load())
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Len(t, backend.records, pageCount)
	require.Equal(t, 2, backend.contentMerges)
	for _, row := range backend.records {
		require.Equal(t, "gs://original/report.pdf?generation=42#page=12", row["source"].(map[string]any)["uri"])
	}
}
