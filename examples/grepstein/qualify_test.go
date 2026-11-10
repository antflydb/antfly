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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractionQualificationDoesNotEquateServingWithOCRCoverage(t *testing.T) {
	for _, problem := range []string{"", "embedded_only", "ocr_failed", "unknown_ocr", "stale_artifact", "stale_source", "missing_page", "unqualified_source", "failed_reprocess", "in_progress", "last_error", "unknown_raw_ocr", "missing_manifest"} {
		t.Run(problem, func(t *testing.T) {
			state := t.TempDir()
			hash := strings.Repeat("a", 64)
			source := "gs://originals/objects/sha256/" + hash + ".pdf?generation=123"
			version := hash + ":123"
			if problem == "unqualified_source" {
				source = "s3://originals/objects/sha256/" + hash + ".pdf"
			}
			record := corpusRecord{ID: "source:1", Document: map[string]any{"url": source, "version": version, "metadata": map[string]any{"page_start": 1, "page_end": 2}}}
			raw, _ := json.Marshal(record)
			raw = append(raw, '\n')
			os.WriteFile(filepath.Join(state, "published.ndjson"), raw, 0600)
			digest := fmt.Sprintf("%x", sha256.Sum256(raw))
			atomicCorpusJSON(filepath.Join(state, "publication.json"), map[string]any{"sha256": digest, "records": 1})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.Contains(r.URL.Path, "/indexes/"):
					fmt.Fprint(w, `{"status":{"readiness":{"complete":true,"queryable":true}},"shard_status":{"one":{"readiness":{"complete":true,"queryable":true}}}}`)
				case strings.Contains(r.URL.Path, "/artifacts/"):
					artifact := map[string]any{"source_url": source, "generation": 1, "unit_count": 2, "ocr_attempted_count": 2, "ocr_failed_count": 0, "route_type": "pdf", "merge_status": "converged"}
					switch problem {
					case "embedded_only":
						artifact["ocr_attempted_count"] = 0
					case "ocr_failed":
						artifact["ocr_failed_count"] = 1
					case "unknown_ocr":
						delete(artifact, "ocr_failed_count")
					case "stale_artifact":
						artifact["source_url"] = source + "4"
					case "missing_page":
						artifact["unit_count"] = 1
					case "failed_reprocess":
						// Failed refreshes preserve prior pages and serving indexes.
						artifact["route_type"] = "error"
						artifact["merge_status"] = "failed"
						artifact["last_error_code"] = "RemoteDocumentFetchFailed"
						artifact["ocr_attempted_count"] = 0
					case "in_progress":
						artifact["merge_status"] = "in_progress"
					case "last_error":
						artifact["last_error_code"] = "InvalidPdf"
					}
					manifest := map[string]any{"ocr_attempted_count": artifact["ocr_attempted_count"], "ocr_failed_count": artifact["ocr_failed_count"]}
					if problem == "unknown_raw_ocr" {
						// The public summary normalizes absent raw counters to zero.
						artifact["ocr_attempted_count"] = 0
						artifact["ocr_failed_count"] = 0
						manifest = map[string]any{}
					}
					encoded, _ := json.Marshal(manifest)
					if problem != "missing_manifest" {
						artifact["manifest_json"] = string(encoded)
					}
					if r.URL.Query().Get("detail") != "raw" {
						t.Error("qualification must request raw OCR evidence")
					}
					json.NewEncoder(w).Encode(artifact)
				default:
					doc := map[string]any{"url": source, "version": version}
					if problem == "stale_source" {
						doc["version"] = "new-version"
					}
					json.NewEncoder(w).Encode(doc)
				}
			}))
			defer server.Close()
			q, err := qualifyCorpus(server.Client(), server.URL+"/tables/corpus", state)
			if problem == "ocr_failed" && (q.OCRFailed != 1 || q.OCRAttempted != 2) {
				t.Fatalf("failed OCR counts were hidden: %+v", q)
			}
			expectedAttempts := 2
			if problem == "embedded_only" {
				expectedAttempts = 0
			}
			if problem != "" && problem != "embedded_only" {
				if err == nil || q.Complete {
					t.Fatalf("incomplete extraction accepted: %+v", q)
				}
			} else if err != nil || !q.Complete || q.Pages != 2 || q.Records != 1 || q.OCRAttempted != expectedAttempts || q.PublicationSHA256 != digest {
				t.Fatalf("qualification failed: %+v %v", q, err)
			}
		})
	}
}
