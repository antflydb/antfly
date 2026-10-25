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
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func originalsFixture(t *testing.T, members map[string][]byte) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "originals.zip")
	file, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	for name, data := range members {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestOriginalsStagePreservesMediaWithoutCreatingAnIndexManifest(t *testing.T) {
	members := map[string][]byte{"IMAGES/file.pdf": createMinimalPDF(), "NATIVES/clip.avi": []byte("original video"), "NATIVES/audio.mp3": []byte("original audio"), "IMAGES/photo.png": []byte("original image"), "DATA/release.dat": []byte("metadata")}
	archive := originalsFixture(t, members)
	state := t.TempDir()
	objects := map[string][]byte{}
	put := func(_ context.Context, bucket, key string, reader io.ReadSeeker, record originalRecord) (string, error) {
		if bucket != "private-originals" {
			t.Fatal(bucket)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return "", err
		}
		checked, err := originalFileRecord(bytes.NewReader(data), record.MIME)
		if err != nil {
			return "", err
		}
		if record.Bytes != checked.Bytes || record.SHA256 != checked.SHA256 || record.CRC32C != checked.CRC32C || !bytes.Equal(record.MD5, checked.MD5) {
			t.Fatal("unverified upload")
		}
		objects[key] = data
		return "42", nil
	}
	if err := stageOriginals(context.Background(), state, "private-originals", []corpusSource{{Dataset: "doj-09", Path: archive, Zip: true}}, 1<<20, 1<<20, put); err != nil {
		t.Fatal(err)
	}
	if len(objects) != 6 {
		t.Fatalf("uploaded %d objects", len(objects))
	}
	file, err := os.Open(filepath.Join(state, "originals.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var record originalRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record.Dataset != "doj-09" || record.Generation != "42" || !bytes.Equal(members[record.Name], objects[record.Object]) {
			t.Fatalf("changed original: %+v", record)
		}
		count++
	}
	if scanner.Err() != nil || count != 5 {
		t.Fatalf("catalog count %d: %v", count, scanner.Err())
	}
	if err := validatePublication(state); err == nil {
		t.Fatal("raw catalog accepted as an Antfly ingest manifest")
	}
	entries, err := os.ReadDir(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".original") {
			t.Fatal("left spooled media behind")
		}
	}
}

func TestOriginalsInventoryNeedsNoCloudCredentialsAndReportsVideo(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "clip.mp4"), []byte("video"), 0600); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	if err := stageOriginalsCmd([]string{"--inventory-only", "--source", "doj-08=" + dir, "--state", state}); err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Uploaded bool           `json:"uploaded"`
		Types    map[string]int `json:"mime_types"`
	}
	if err := readCorpusJSON(filepath.Join(state, "originals.json"), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Uploaded || receipt.Types["video/mp4"] != 1 {
		t.Fatalf("receipt=%+v", receipt)
	}
}

func TestOriginalsRejectsUnsafeNamesAndBudgets(t *testing.T) {
	for _, name := range []string{"../escape.pdf", "/absolute.pdf", "a\\escape.pdf"} {
		source := originalsFixture(t, map[string][]byte{name: []byte("source")})
		if err := stageOriginals(context.Background(), t.TempDir(), "", []corpusSource{{Dataset: "doj-08", Path: source, Zip: true}}, 100, 100, nil); err == nil {
			t.Fatalf("accepted %s", name)
		}
	}
	source := originalsFixture(t, map[string][]byte{"clip.mov": bytes.Repeat([]byte("x"), 100)})
	for _, limits := range [][2]int64{{99, 100}, {100, 99}} {
		state := t.TempDir()
		if err := stageOriginals(context.Background(), state, "", []corpusSource{{Dataset: "doj-08", Path: source, Zip: true}}, limits[0], limits[1], nil); err == nil {
			t.Fatal("ignored byte cap")
		}
		if _, err := os.Stat(filepath.Join(state, "originals.json")); !os.IsNotExist(err) {
			t.Fatal("sealed an incomplete inventory")
		}
	}
}

func TestOriginalsRejectsStateInsideInput(t *testing.T) {
	dir := t.TempDir()
	if err := stageOriginalsCmd([]string{"--inventory-only", "--source", "doj-01=" + dir, "--state", filepath.Join(dir, "output")}); err == nil {
		t.Fatal("allowed staging to archive its own outputs")
	}
}

func TestOriginalGCSUploadIsCreateOnlyAndChecksExistingBytes(t *testing.T) {
	data := []byte("video bytes")
	record, err := originalFileRecord(bytes.NewReader(data), "video/mp4")
	if err != nil {
		t.Fatal(err)
	}
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], record.CRC32C)
	attrs := fmt.Sprintf(`{"name":"objects/test.mp4","size":%q,"generation":"42","md5Hash":%q,"crc32c":%q}`, fmt.Sprint(record.Bytes), base64.StdEncoding.EncodeToString(record.MD5), base64.StdEncoding.EncodeToString(checksum[:]))
	posts := 0
	exists := false
	bad := false
	transport := roundTrip(func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case "GET":
			if !exists {
				return response(404, `{"error":{"code":404,"message":"not found"}}`), nil
			}
			body := attrs
			if bad {
				body = strings.Replace(body, base64.StdEncoding.EncodeToString(record.MD5), base64.StdEncoding.EncodeToString([]byte("wrong-checksum")), 1)
			}
			return response(200, body), nil
		case "POST":
			posts++
			if r.URL.Query().Get("ifGenerationMatch") != "0" {
				t.Fatal("upload could replace an original")
			}
			if r.URL.Query().Get("uploadType") == "resumable" {
				result := response(200, "")
				result.Header.Set("Location", "https://storage.googleapis.com/upload/session")
				return result, nil
			}
			return response(200, attrs), nil
		case "PUT":
			return response(200, attrs), nil
		default:
			t.Fatalf("unexpected method %s", r.Method)
			return nil, nil
		}
	})
	client, err := storage.NewClient(context.Background(), option.WithHTTPClient(&http.Client{Transport: transport}), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	put := originalGCSWriter(client)
	if generation, err := put(context.Background(), "private", "objects/test.mp4", bytes.NewReader(data), record); err != nil || generation != "42" || posts != 1 {
		t.Fatalf("create: generation=%s posts=%d error=%v", generation, posts, err)
	}
	exists = true
	if _, err := put(context.Background(), "private", "objects/test.mp4", bytes.NewReader(data), record); err != nil || posts != 1 {
		t.Fatalf("reuse: %v", err)
	}
	bad = true
	if _, err := put(context.Background(), "private", "objects/test.mp4", bytes.NewReader(data), record); err == nil {
		t.Fatal("reused different existing bytes")
	}
}
