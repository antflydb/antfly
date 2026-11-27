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

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestRepairGrepMetadataPreservesExistingACLAndFencesUpdates(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("conflict=%t", conflict), func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("POST /tables/corpus/documents", func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, `{"_id":"doc"}`)
			})
			mux.HandleFunc("GET /tables/corpus/documents/doc", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Antfly-Version", "9007199254740993")
				fmt.Fprintln(w, `{"version":"gcs-1","url":"gs://fixture/source","original_url":"gs://fixture/source","filename":"source.pdf","mime_type":"application/pdf","permissions":{"public":false,"readers":["user:existing"]}}`)
			})
			mux.HandleFunc("POST /transactions/commit", func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ReadSet []map[string]string `json:"read_set"`
					Tables  map[string]struct {
						Transforms []struct {
							Key        string
							Operations []map[string]any
						}
					}
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if want := []map[string]string{{"table": "corpus", "key": "doc", "version": "9007199254740993"}}; !reflect.DeepEqual(request.ReadSet, want) {
					t.Errorf("read set: %+v", request.ReadSet)
				}
				transforms := request.Tables["corpus"].Transforms
				want := []map[string]any{{"op": "$set", "path": "$.indexed_at", "value": float64(123)}}
				if len(transforms) != 1 || transforms[0].Key != "doc" || !reflect.DeepEqual(transforms[0].Operations, want) {
					t.Errorf("unexpected metadata patch: %+v", transforms)
				}
				if conflict {
					w.WriteHeader(http.StatusConflict)
					return
				}
				fmt.Fprintln(w, `{"status":"committed"}`)
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			result, err := repairGrepMetadataPage(server.Client(), server.URL, "corpus", grepMetadataOptions{Limit: 1, IndexedAt: 123, Reader: "user:new", Apply: true})
			if conflict {
				if err == nil || result.Patched != 0 || result.NextAfter != "" {
					t.Fatalf("conflict advanced page: %+v, %v", result, err)
				}
			} else if err != nil || result.Patched != 1 || result.NextAfter != "doc" {
				t.Fatalf("unexpected result: %+v, %v", result, err)
			}
		})
	}
}
