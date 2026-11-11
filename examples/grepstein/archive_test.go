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
	"archive/zip"
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveIntegrityAndCacheRecovery(t *testing.T) {
	var buffer bytes.Buffer
	z := zip.NewWriter(&buffer)
	f, err := z.Create("DATA/EFTA00000001.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(createMinimalPDF()); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	data := buffer.Bytes()
	requests := 0
	served := data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++; w.Write(served) }))
	defer server.Close()
	receipt := archiveReceipt{Dataset: "doj-06", Item: archiveItem("doj-06"), URL: server.URL, OfficialStatus: "unresolved", File: archiveFile{Name: "DataSet 6.zip", Size: fmt.Sprint(len(data)), SHA1: fmt.Sprintf("%x", sha1.Sum(data)), MD5: fmt.Sprintf("%x", md5.Sum(data))}}
	dest := filepath.Join(t.TempDir(), receipt.File.Name)
	if err := downloadArchive(server.Client(), dest, &receipt, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	// Receipts carry the public acquisition identity, independently of this
	// test's local download transport.
	receipt.URL = "https://archive.org/download/" + receipt.Item + "/DataSet%206.zip"
	if err := atomicCorpusJSON(dest+".json", receipt); err != nil {
		t.Fatal(err)
	}
	if err := downloadArchive(server.Client(), dest, &receipt, int64(len(data))); err != nil || requests != 1 {
		t.Fatalf("verified cache: %v requests=%d", err, requests)
	}
	if _, err := archiveProvenance(corpusSource{Dataset: "doj-06", Path: dest, Zip: true}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := archiveProvenance(corpusSource{Dataset: "doj-06", Path: dest, Zip: true}); err == nil {
		t.Fatal("accepted changed archived bytes")
	}
	receipt.URL = server.URL
	if err := downloadArchive(server.Client(), dest, &receipt, int64(len(data))); err != nil || requests != 2 {
		t.Fatalf("repair: %v requests=%d", err, requests)
	}
	served = []byte("<html>access gate</html>")
	if err := downloadArchive(server.Client(), filepath.Join(t.TempDir(), "bad.zip"), &receipt, int64(len(data))); err == nil {
		t.Fatal("accepted interstitial")
	}
	if err := downloadArchive(server.Client(), filepath.Join(t.TempDir(), "bad.zip"), &receipt, int64(len(data)-1)); err == nil {
		t.Fatal("accepted size mismatch")
	}
}

func TestArchiveRejectsNonZIPWithMatchingChecksum(t *testing.T) {
	data := []byte("<html>unavailable</html>")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(data) }))
	defer server.Close()
	receipt := archiveReceipt{URL: server.URL, File: archiveFile{SHA1: fmt.Sprintf("%x", sha1.Sum(data)), MD5: fmt.Sprintf("%x", md5.Sum(data))}}
	dest := filepath.Join(t.TempDir(), "source.zip")
	if err := downloadArchive(server.Client(), dest, &receipt, int64(len(data))); err == nil {
		t.Fatal("accepted HTML with valid transport checksums")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("published rejected archive")
	}
}

func TestArchiveRejectsUnknownDatasets(t *testing.T) {
	for _, id := range []string{"court-2024", "doj-00", "doj-13"} {
		if archiveItem(id) != "" {
			t.Fatalf("allowed deferred dataset %s", id)
		}
		if err := fetchArchiveCmd([]string{"--dataset", id}); err == nil {
			t.Fatalf("fetch accepted %s", id)
		}
	}
}

func TestArchiveResumeAndRangeValidation(t *testing.T) {
	var data bytes.Buffer
	writer := zip.NewWriter(&data)
	member, err := writer.Create("DATA/EFTA00000001.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write(createMinimalPDF()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	wire := data.Bytes()
	for _, mode := range []string{"range", "restart", "invalid-range"} {
		t.Run(mode, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if requests == 1 {
					w.Header().Set("Content-Length", fmt.Sprint(len(wire)))
					w.Write(wire[:len(wire)/2])
					return
				}
				offset := len(wire) / 2
				if r.Header.Get("Range") != fmt.Sprintf("bytes=%d-", offset) {
					t.Errorf("missing resume range: %s", r.Header.Get("Range"))
				}
				if mode == "restart" {
					w.Write(wire)
					return
				}
				start := offset
				if mode == "invalid-range" {
					start++
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(wire)-1, len(wire)))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(wire[offset:])
			}))
			defer server.Close()
			receipt := archiveReceipt{URL: server.URL, File: archiveFile{SHA1: fmt.Sprintf("%x", sha1.Sum(wire)), MD5: fmt.Sprintf("%x", md5.Sum(wire))}}
			destination := filepath.Join(t.TempDir(), "release.zip")
			if err := downloadArchive(server.Client(), destination, &receipt, int64(len(wire))); err == nil {
				t.Fatal("partial archive published")
			}
			err := downloadArchive(server.Client(), destination, &receipt, int64(len(wire)))
			if mode == "invalid-range" {
				if err == nil {
					t.Fatal("invalid range accepted")
				}
				if _, e := os.Stat(destination); !os.IsNotExist(e) {
					t.Fatal("invalid range became source")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(destination)
			if err != nil || !bytes.Equal(got, wire) {
				t.Fatal("resume corrupted archive", err)
			}
			if requests != 2 {
				t.Fatal("unexpected requests", requests)
			}
		})
	}
}

func TestLargeArchiveMappingsAreExplicit(t *testing.T) {
	for _, id := range []string{"doj-08", "doj-09", "doj-10", "doj-11"} {
		if archiveItem(id) == "" {
			t.Fatal("missing raw archive", id)
		}
	}
	// Extending raw acquisition must not make prepare-small index large releases.
	for _, d := range smallDatasets() {
		if d.ID >= "doj-08" && d.ID <= "doj-11" && d.Stage != "deferred" {
			t.Fatal("large release enabled for v1 indexing")
		}
	}
}
