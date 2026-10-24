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
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestS3ObjectKeysRemainDistinctCanonicalURIs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "1")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(200)
	}))
	defer server.Close()
	target, _ := url.Parse(server.URL)
	client, err := minio.New(target.Host, &minio.Options{Creds: credentials.NewStaticV4("access", "secret", ""), Secure: false, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	c := S3Connector{Client: client}
	uris := map[string]string{}
	for _, key := range []string{"a b.txt", "a%20b.txt", "100%done.txt", "docs/report#1?.txt"} {
		raw, _ := json.Marshal(S3Config{Bucket: "docs", Key: key})
		inventory, err := c.Enumerate(context.Background(), Request{Mode: ImportModeSource, SourceNamespace: testSourceID, Config: raw})
		if err != nil {
			t.Fatal(err)
		}
		doc, err := Normalize(testSourceID, inventory.Objects[0], []byte("x"), Permissions{Public: true}, time.Now())
		if err != nil {
			t.Errorf("legal S3 key %q cannot be normalized: %v", key, err)
			continue
		}
		legacy := inventory.Objects[0]
		legacy.URL = "s3://docs/" + key
		replayed, err := Normalize(testSourceID, legacy, []byte("x"), Permissions{Public: true}, time.Now())
		if err != nil || replayed.URI != doc.URI {
			t.Errorf("legacy pinned key %q cannot recover canonical identity: %q %v", key, replayed.URI, err)
		}
		parsed, err := url.Parse(doc.URI)
		if err != nil || parsed.Path != "/"+key || parsed.RawQuery != "" || parsed.Fragment != "" {
			t.Errorf("canonical URI %q does not preserve literal key %q", doc.URI, key)
		}
		if prior, ok := uris[doc.URI]; ok {
			t.Errorf("distinct keys %q and %q collapse to canonical URI %q", prior, key, doc.URI)
		}
		uris[doc.URI] = key
	}
}

// Exercise real MinIO request construction: a readable replacement must never
// authorize bytes staged from a version whose access has since been revoked.
func TestS3PinnedVersionLifecycle(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		for _, version := range []string{"old-version", "null", ""} {
			t.Run(fmt.Sprintf("prefix=%v/version=%q", prefix, version), func(t *testing.T) {
				denied, replaced, ignoreVersion, changeListing := false, false, false, false
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("list-type") == "2" {
						w.Header().Set("Content-Type", "application/xml")
						fmt.Fprint(w, `<ListBucketResult><Name>docs</Name><IsTruncated>false</IsTruncated><Contents><Key>doc.txt</Key><ETag>"etag"</ETag><Size>1</Size></Contents></ListBucketResult>`)
						return
					}
					requested := r.URL.Query().Get("versionId")
					if denied && requested == version {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					actual, etag := version, "etag"
					if (replaced && (requested == "" || ignoreVersion)) || changeListing {
						actual, etag = "replacement", "new-etag"
					}
					if !replaced && !changeListing && r.Method == http.MethodGet && requested != version {
						t.Errorf("GET versionId = %q, want %q", requested, version)
					}
					w.Header().Set("ETag", `"`+etag+`"`)
					w.Header().Set("Content-Length", "1")
					w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
					if actual != "" {
						w.Header().Set("x-amz-version-id", actual)
					}
					if r.Method == http.MethodGet {
						fmt.Fprint(w, "x")
					}
				}))
				defer server.Close()
				target, _ := url.Parse(server.URL)
				client, err := minio.New(target.Host, &minio.Options{Creds: credentials.NewStaticV4("access", "secret", ""), Secure: false, Region: "us-east-1"})
				if err != nil {
					t.Fatal(err)
				}
				connector := S3Connector{Client: client}
				cfg := S3Config{Bucket: "docs", Key: "doc.txt"}
				if prefix {
					cfg.Key = ""
				}
				raw, _ := json.Marshal(cfg)
				req := Request{Mode: ImportModeSource, SourceNamespace: testSourceID, Config: raw}
				inventory, err := connector.Enumerate(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				if len(inventory.Objects) != 1 {
					t.Fatalf("inventory: %+v", inventory)
				}
				item := inventory.Objects[0]
				if got, ok := item.Metadata["version_id"]; !ok || got != version {
					t.Fatalf("pinned version = %v, want %q", got, version)
				}
				// The inventory survives a persisted JSON manifest round trip.
				encoded, _ := json.Marshal(item)
				if err := json.Unmarshal(encoded, &item); err != nil {
					t.Fatal(err)
				}
				check := func(wantOK bool) {
					t.Helper()
					if err := connector.CheckAccess(context.Background(), req, item); (err == nil) != wantOK {
						t.Errorf("CheckAccess = %v; wantOK %v", err, wantOK)
					}
					data, err := connector.Read(context.Background(), req, item)
					if (err == nil) != wantOK {
						t.Errorf("Read = %q, %v; wantOK %v", data, err, wantOK)
					}
				}
				check(true)
				replaced = true
				check(version != "") // Old versions remain usable only via exact-version access.
				denied = true
				check(false)
				denied, ignoreVersion = false, true
				check(false) // Fail closed for servers that ignore versionId.
				if prefix {
					replaced, ignoreVersion, changeListing = false, false, true
					if _, err := connector.Enumerate(context.Background(), req); err == nil {
						t.Error("accepted changed listing during version pinning")
					}
				}
			})
		}
	}
}

func TestS3PinnedResponseIdentity(t *testing.T) {
	item := Item{Path: "doc.txt", Revision: "etag", Size: 1, Metadata: map[string]any{"bucket": "docs"}}
	for _, tc := range []struct {
		name string
		info minio.ObjectInfo
		ok   bool
	}{
		{"legacy unversioned", minio.ObjectInfo{ETag: "etag", Size: 1}, true},
		{"legacy versioned same bytes", minio.ObjectInfo{ETag: "etag", Size: 1, VersionID: "replacement"}, false},
		{"changed etag", minio.ObjectInfo{ETag: "new", Size: 1}, false},
		{"changed size", minio.ObjectInfo{ETag: "etag", Size: 2}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateS3Response(item, tc.info); (err == nil) != tc.ok {
				t.Fatalf("validateS3Response = %v; wantOK %v", err, tc.ok)
			}
		})
	}
}
