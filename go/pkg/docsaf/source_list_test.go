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

package docsaf

import (
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/time/rate"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestS3ListNeverDownloadsAndPreservesVersions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Has("location") {
			fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
			return
		}
		if r.URL.Path != "/docs/" && r.URL.Path != "/docs" {
			t.Errorf("unexpected content request: %s", r.URL)
			http.Error(w, "no content GETs", 500)
			return
		}
		if r.URL.Query().Get("continuation-token") == "page2" {
			fmt.Fprint(w, `<ListBucketResult><Name>docs</Name><IsTruncated>false</IsTruncated><Contents><Key>prefix/audio.wav</Key><Size>99</Size><ETag>"second"</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></Contents></ListBucketResult>`)
			return
		}
		fmt.Fprint(w, `<ListBucketResult><Name>docs</Name><IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken><Contents><Key>prefix/photo #1.jpg</Key><Size>123</Size><ETag>"multipart-2"</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></Contents><Contents><Key>prefix/ignore.txt</Key><Size>3</Size><ETag>"ignored"</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></Contents></ListBucketResult>`)
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client, err := minio.New(u.Host, &minio.Options{Secure: false, Creds: credentials.NewStaticV4("access", "secret", "")})
	if err != nil {
		t.Fatal(err)
	}
	source := &S3Source{client: client, config: S3SourceConfig{Bucket: "docs", Prefix: "prefix/", ExcludePatterns: []string{"*.txt"}}}
	var _ MetadataSource = source
	items, errs := source.List(t.Context())
	var got []ContentItem
	for item := range items {
		got = append(got, item)
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 {
		t.Fatalf("items = %#v", got)
	}
	for _, item := range got {
		if item.Content != nil {
			t.Fatal("list downloaded content")
		}
		if _, ok := item.Metadata["sha256"]; ok {
			t.Fatal("fabricated checksum")
		}
	}
	if got[0].Metadata["etag"] != "multipart-2" || !strings.Contains(got[0].SourceURL, "photo%20%231.jpg") {
		t.Fatalf("metadata: %#v", got[0])
	}
}
func TestS3ListPropagatesListingFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(403)
		fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client, err := minio.New(u.Host, &minio.Options{Secure: false, Region: "us-east-1", Creds: credentials.NewStaticV4("access", "secret", "")})
	if err != nil {
		t.Fatal(err)
	}
	source := &S3Source{client: client, config: S3SourceConfig{Bucket: "docs"}}
	items, errs := source.List(t.Context())
	for range items {
		t.Fatal("unexpected object")
	}
	if err := <-errs; err == nil {
		t.Fatal("listing failure was swallowed")
	}
}
func TestDriveListNeverDownloadsOrExports(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files" || r.URL.Query().Get("alt") == "media" {
			t.Errorf("unexpected content request: %s", r.URL)
			http.Error(w, "no content", 500)
			return
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("pageToken") == "second" {
			_ = json.NewEncoder(w).Encode(map[string]any{"files": []any{map[string]any{"id": "sheet", "name": "sheet", "mimeType": "application/vnd.google-apps.spreadsheet", "version": "8"}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"nextPageToken": "second", "files": []any{map[string]any{"id": "photo", "name": "photo.jpg", "mimeType": "image/jpeg", "size": "123", "version": "7", "md5Checksum": "provider-md5"}}})
	}))
	defer server.Close()
	client, err := drive.NewService(t.Context(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	source := &GoogleDriveSource{config: GoogleDriveSourceConfig{FolderID: "root"}, service: client, limiter: rate.NewLimiter(rate.Inf, 1)}
	var _ MetadataSource = source
	items, errs := source.List(t.Context())
	var got []ContentItem
	for item := range items {
		got = append(got, item)
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || calls != 2 {
		t.Fatalf("got %d items across %d calls", len(got), calls)
	}
	if got[0].Content != nil || got[0].ContentType != "image/jpeg" || got[0].Metadata["version"] != "7" || got[0].Metadata["md5_checksum"] != "provider-md5" {
		t.Fatalf("wrong metadata: %#v", got[0])
	}
	if got[1].ContentType != "application/vnd.google-apps.spreadsheet" {
		t.Fatal("listing must preserve original Workspace metadata")
	}
}
