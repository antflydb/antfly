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

func TestArchiveOnlySmallDatasets(t *testing.T) {
	for _, id := range []string{"doj-08", "doj-09", "doj-10", "doj-11", "court-2024", "doj-00"} {
		if archiveItem(id) != "" {
			t.Fatalf("allowed deferred dataset %s", id)
		}
		if err := fetchArchiveCmd([]string{"--dataset", id}); err == nil {
			t.Fatalf("fetch accepted %s", id)
		}
	}
}
