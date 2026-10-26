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
	"bufio"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
)

// Each published record contains only immutable remote locators. Extraction is
// performed by Antfly, never by the uploader. ADC is used only by this process;
// Antfly's separately scoped read credentials are configured on the instance.
func publishCmd(args []string) error {
	f := flag.NewFlagSet("publish", flag.ContinueOnError)
	state := f.String("state", "./grepstein-corpus", "Prepared corpus")
	bucket := f.String("bucket", "", "Private GCS bucket")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *bucket == "" || strings.ContainsAny(*bucket, "/?#:") {
		return fmt.Errorf("a GCS bucket name is required")
	}
	unlock, err := corpusLock(*state)
	if err != nil {
		return err
	}
	defer unlock()
	var progress corpusProgress
	if err = readCorpusJSON(filepath.Join(*state, "prepare.json"), &progress); err != nil {
		return err
	}
	if skipped, err := os.Stat(filepath.Join(*state, "skipped.ndjson")); err == nil && skipped.Size() > 0 {
		return fmt.Errorf("source preparation skipped files; repair them in a new state before publishing v1")
	}
	if !progress.Complete {
		return fmt.Errorf("finish preparation first")
	}
	var cfg corpusConfig
	if err = readCorpusJSON(filepath.Join(*state, "sources.json"), &cfg); err != nil {
		return err
	}
	client, err := google.DefaultClient(context.Background(), "https://www.googleapis.com/auth/devstorage.read_write")
	if err != nil {
		return err
	}
	client.Timeout = 15 * time.Minute
	store := newCorpusStore(cfg)
	store.pageCache = filepath.Join(*state, "page-windows")
	defer store.Close()
	input, err := os.Open(filepath.Join(*state, "records.ndjson"))
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.CreateTemp(*state, ".published-*")
	if err != nil {
		return err
	}
	defer os.Remove(output.Name())
	defer output.Close()
	reader := bufio.NewReader(input)
	originals := map[sourceLocator][2]string{}
	count := 0
	seen := map[string]bool{}
	// The manifest is replaced only after every upload succeeds. Re-running after
	// a crash reuses content-addressed objects, including shared originals.
	for {
		var record corpusRecord
		if _, err = readCorpusLine(reader, &record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if record.ID == "" || seen[record.ID] {
			return fmt.Errorf("invalid or duplicate source record")
		}
		seen[record.ID] = true
		count++
		if record.Document["mime_type"] != "application/pdf" {
			return fmt.Errorf("v1 publication supports PDFs only")
		}
		meta, ok := record.Document["metadata"].(map[string]any)
		if !ok {
			return fmt.Errorf("missing metadata")
		}
		raw, _ := json.Marshal(meta["source_locator"])
		var loc sourceLocator
		if err = json.Unmarshal(raw, &loc); err != nil {
			return err
		}
		original := loc
		original.First = 0
		original.Last = 0
		cached, ok := originals[original]
		if !ok {
			originalURL, originalVersion, err := publishLocator(client, store, *bucket, original)
			if err != nil {
				return fmt.Errorf("%s original: %w", record.ID, err)
			}
			cached = [2]string{originalURL, originalVersion}
			originals[original] = cached
		}
		originalURL, originalVersion := cached[0], cached[1]
		if expected, ok := meta["upstream_sha256"].(string); ok && !strings.HasPrefix(originalVersion, expected+":") {
			return fmt.Errorf("%s differs from the verified download receipt", record.ID)
		}
		sourceURL, version, err := publishLocator(client, store, *bucket, loc)
		if err != nil {
			return fmt.Errorf("%s: %w", record.ID, err)
		}
		meta["original_version"] = originalVersion
		record.Document["url"] = sourceURL
		record.Document["original_url"] = originalURL
		record.Document["version"] = version
		meta["gcs_bucket"] = *bucket
		meta["publication_version"] = 1
		if err = json.NewEncoder(output).Encode(record); err != nil {
			return err
		}
	}
	if count != progress.Records {
		return fmt.Errorf("prepared manifest record count changed")
	}
	if err = output.Sync(); err != nil {
		return err
	}
	if err = output.Close(); err != nil {
		return err
	}
	manifest, err := os.Open(output.Name())
	if err != nil {
		return err
	}
	defer manifest.Close()
	sha, md := sha256.New(), md5.New()
	size, err := io.Copy(io.MultiWriter(sha, md), manifest)
	if err != nil {
		return err
	}
	digest := hex.EncodeToString(sha.Sum(nil))
	if _, err = manifest.Seek(0, io.SeekStart); err != nil {
		return err
	}
	key := "manifests/sha256/" + digest + ".ndjson"
	object, err := putGCS(client, *bucket, key, "application/x-ndjson", manifest, size, base64.StdEncoding.EncodeToString(md.Sum(nil)))
	if err != nil {
		return err
	}
	if err = os.Rename(output.Name(), filepath.Join(*state, "published.ndjson")); err != nil {
		return err
	}
	return atomicCorpusJSON(filepath.Join(*state, "publication.json"), map[string]any{"version": 1, "manifest": "gs://" + *bucket + "/" + key, "generation": object.Generation, "sha256": digest, "records": progress.Records})

}

func publishLocator(client *http.Client, store *corpusStore, bucket string, loc sourceLocator) (string, string, error) {
	var source *os.File
	var cleanup func()
	var err error
	if loc.First > 0 {
		if err = store.preparePageWindow(loc); err != nil {
			return "", "", err
		}
		source, err = os.Open(store.pageCachePath(loc))
		cleanup = func() {
			if source != nil {
				source.Close()
			}
		}
	} else {
		source, cleanup, err = store.seekable(loc)
	}
	if err != nil {
		return "", "", err
	}
	defer cleanup()
	sha, md := sha256.New(), md5.New()
	size, err := io.Copy(io.MultiWriter(sha, md), source)
	if err != nil {
		return "", "", err
	}
	if size > corpusMaxBytes && loc.First > 0 {
		return "", "", fmt.Errorf("PDF window exceeds 64 MiB; prepare smaller windows")
	}
	digest := hex.EncodeToString(sha.Sum(nil))
	key := "objects/sha256/" + digest + strings.ToLower(filepath.Ext(loc.Name))
	if _, err = source.Seek(0, io.SeekStart); err != nil {
		return "", "", err
	}
	object, err := putGCS(client, bucket, key, corpusMIME(loc.Name), source, size, base64.StdEncoding.EncodeToString(md.Sum(nil)))
	if err != nil {
		return "", "", err
	}
	// GCS's XML/S3 endpoint is an existing Antfly remote-content transport.
	// Hash-addressed keys + create-only writes prevent an unpinned latest read
	// from changing underneath extraction. The GCS generation remains evidence.
	return "s3://" + bucket + "/" + key, digest + ":" + object.Generation, nil
}

type gcsObject struct {
	Generation string `json:"generation"`
	Size       string `json:"size"`
	MD5        string `json:"md5Hash"`
}

func putGCS(client *http.Client, bucket, key, mime string, source io.Reader, size int64, md5sum string) (gcsObject, error) {
	var object gcsObject
	endpoint := "https://storage.googleapis.com/upload/storage/v1/b/" + url.PathEscape(bucket) + "/o?uploadType=media&ifGenerationMatch=0&name=" + url.QueryEscape(key)
	req, err := http.NewRequest("POST", endpoint, source)
	if err != nil {
		return object, err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", mime)
	resp, err := client.Do(req)
	if err != nil {
		return object, err
	}
	if resp.StatusCode == http.StatusPreconditionFailed {
		resp.Body.Close()
		resp, err = client.Get("https://storage.googleapis.com/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.PathEscape(key))
		if err != nil {
			return object, err
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return object, fmt.Errorf("GCS HTTP %d", resp.StatusCode)
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&object); err != nil {
		return object, err
	}
	if object.Generation == "" || object.Size != strconv.FormatInt(size, 10) || object.MD5 != md5sum {
		return object, fmt.Errorf("GCS object integrity mismatch")
	}
	return object, nil
}
