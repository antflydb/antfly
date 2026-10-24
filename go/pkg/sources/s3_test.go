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
