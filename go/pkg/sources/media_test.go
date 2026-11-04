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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"testing"
	"time"
)

func mediaDocument(t *testing.T) Document {
	t.Helper()
	item := Item{ID: "s3://job/" + testSourceID + "/docs/photo.jpg", Path: "photo.jpg", URL: "s3://docs/photo.jpg", Revision: "rev-1", SourceKind: ProviderS3, Size: 123}
	doc, err := NormalizeMedia(testSourceID, item, MediaReference{ContentSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("fixture"))), Type: "source", URI: "https://source.example/v1/media/object/token", MIMEType: "image/jpeg", Version: `"etag"`, VersionKind: "strong_etag", Length: 123}, Permissions{Public: true}, time.Now())
	require.NoError(t, err)
	return doc
}
func TestMediaNormalizationRejectsAmbiguousOrUnpinnedContent(t *testing.T) {
	doc := mediaDocument(t)
	require.Equal(t, 2, doc.Version)
	require.Empty(t, doc.Content)
	require.True(t, validContentSHA(configString(doc.Metadata, "sha256")))
	cases := map[string]func(*Document){
		"text masquerading as media": func(d *Document) { d.Content = "binary" },
		"invented checksum":          func(d *Document) { d.Metadata["sha256"] = "hash" },
		"weak version":               func(d *Document) { d.Media.Version = `W/"version"` },
		"unknown length":             func(d *Document) { d.Media.Length = 0 },
		"inline credentials":         func(d *Document) { d.Media.URI = "https://user:password@example.com/file" },
		"wrong version":              func(d *Document) { d.Version = 1 },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) { d := mediaDocument(t); change(&d); require.Error(t, validateDocumentContent(d)) })
	}
}
func TestMediaReadinessIncludesCompleteDescriptor(t *testing.T) {
	doc := mediaDocument(t)
	require.Contains(t, QueryDocumentFields(), "grep.media")
	require.NoError(t, WaitForDocuments(t.Context(), func(context.Context, string, []byte) ([]byte, error) { return readinessResponse(doc), nil }, "docs", map[string]Document{"item": doc}))
	before, _ := projectionDigest(doc)
	changed := mediaDocument(t)
	changed.IndexedAt = doc.IndexedAt
	changed.Media.URI = "https://source.example/new"
	after, _ := projectionDigest(changed)
	require.NotEqual(t, before, after)
}
func TestRunnerMediaRetainsOwnershipReadinessAndDeletion(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	runner.Profile.Sources[0].Publication = "media"
	// Binary bytes deliberately cannot pass the text normalizer. Media sync must
	// never call Read: the fetch resolver owns acquisition after publication.
	connector.data = nil
	runner.MediaReference = func(_ context.Context, _ Source, _ Request, item Item) (MediaReference, error) {
		ref := *mediaDocument(t).Media
		ref.Length = item.Size
		return ref, nil
	}
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	for _, row := range backend.records {
		raw, _ := json.Marshal(row["grep"])
		var doc Document
		require.NoError(t, json.Unmarshal(raw, &doc))
		require.Equal(t, "media", doc.Kind)
		require.Equal(t, 2, doc.Version)
		require.True(t, validContentSHA(configString(doc.Metadata, "sha256")))
	}
	backend.mu.Unlock()
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	require.Equal(t, 1, backend.upserted)
	backend.mu.Unlock()
	connector.inventory.Objects = nil
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Empty(t, backend.records)
	require.Equal(t, 1, backend.deletes)
}

func TestWorkspaceMediaReadsOriginalBytesWithRevisionFences(t *testing.T) {
	binary := []byte{0xff, 0xd8, 0xff, 0x00, 0x01, 0x02}
	afterRead := false
	mediaChecks := 0
	client := withDriveScopes(&http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		var payload []byte
		switch {
		case r.URL.Path == "/files/folder":
			payload = []byte(`{"id":"folder","mimeType":"application/vnd.google-apps.folder","capabilities":{"canListChildren":true}}`)
		case r.URL.Path == "/files":
			payload = []byte(`{"files":[{"id":"photo","name":"photo.jpg","mimeType":"image/jpeg","size":"6","version":"7"}]}`)
		case r.URL.Path == "/files/photo" && r.URL.Query().Get("alt") == "media":
			payload = binary
			afterRead = true
		case r.URL.Path == "/files/photo":
			mediaChecks++
			version := "7"
			if afterRead {
				version = "8"
			}
			payload = []byte(fmt.Sprintf(`{"id":"photo","version":%q,"capabilities":{"canDownload":true}}`, version))
		default:
			t.Errorf("unexpected Drive request: %s", r.URL)
			return nil, fmt.Errorf("unexpected request")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload)), Request: r}, nil
	})}, "https://www.googleapis.com/auth/drive.readonly")
	source := Source{Publication: "media", Config: json.RawMessage(`{"resource":"drive","folder_id":"folder"}`)}
	c, err := publicationConnector(&CollaborationConnector{ProviderName: ProviderGoogleWorkspace, HTTPClient: client, DriveBase: "https://fixture.test"}, source)
	require.NoError(t, err)
	request := Request{SourceNamespace: "job/" + testSourceID, Mode: ImportModeSource, Config: source.Config}
	inventory, err := c.Enumerate(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, inventory.Objects, 1)
	require.Equal(t, ProviderGoogleWorkspace, inventory.Objects[0].SourceKind)
	require.NoError(t, ValidateMediaInventory(t.Context(), c, request, inventory))
	require.Equal(t, 1, mediaChecks, "one per-object audit, without repeating Drive inventory validation")
	_, err = c.Read(t.Context(), request, inventory.Objects[0])
	var changed *RevisionChangedError
	require.ErrorAs(t, err, &changed)
	require.Equal(t, "7", changed.Expected)
	require.Equal(t, "8", changed.Actual)
}

func TestMediaContentScopePinsProviderEndpoint(t *testing.T) {
	makeS3 := func(endpoint, key string) *S3Connector {
		client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(key, "secret", ""), Secure: true})
		require.NoError(t, err)
		return &S3Connector{Client: client}
	}
	first := makeS3("first.example", "first-key").MediaContentScope()
	require.Equal(t, first, makeS3("first.example", "rotated-key").MediaContentScope())
	require.NotEqual(t, first, makeS3("second.example", "first-key").MediaContentScope())
	gcs := &GoogleConnector{ProviderName: ProviderGCS, APIBase: "https://first.example"}
	before := gcs.MediaContentScope()
	gcs.APIBase = "https://second.example"
	require.NotEqual(t, before, gcs.MediaContentScope())
}
