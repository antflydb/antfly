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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
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
)

// Archive integrity establishes which archived bytes we acquired. It does not
// establish current official availability or authorize public republication.
type archiveFile struct {
	Name   string `json:"name"`
	Size   string `json:"size"`
	SHA1   string `json:"sha1"`
	MD5    string `json:"md5"`
	Source string `json:"source"`
}

type archiveReceipt struct {
	Dataset         string      `json:"dataset"`
	Item            string      `json:"item"`
	URL             string      `json:"url"`
	OfficialRelease string      `json:"official_release"`
	OfficialStatus  string      `json:"official_status"`
	File            archiveFile `json:"file"`
	SHA256          string      `json:"sha256"`
	RetrievedAt     string      `json:"retrieved_at"`
}

func archiveItem(dataset string) string {
	if dataset == "doj-12" {
		return "epstein_library_transparency_act_hr_4405_dataset12_202602"
	}
	for _, id := range []string{"doj-01", "doj-02", "doj-03", "doj-04", "doj-05", "doj-06", "doj-07"} {
		if dataset == id {
			return "data-set-1"
		}
	}
	return ""
}

func fetchArchiveCmd(args []string) error {
	f := flag.NewFlagSet("fetch-archive", flag.ContinueOnError)
	selected := f.String("dataset", "all-small", "all-small or doj-01 through doj-07 / doj-12")
	root := f.String("output", "./grepstein-archive", "Archived release ZIP directory")
	budget := f.Int64("max-bytes", 10<<30, "Maximum selected compressed archive bytes, including cached files")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *budget < 1 {
		return fmt.Errorf("max-bytes must be positive")
	}
	if *selected != "all-small" && archiveItem(*selected) == "" {
		return fmt.Errorf("dataset is not in the small v1 release set")
	}
	client := &http.Client{Timeout: 30 * time.Minute, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		host := req.URL.Hostname()
		if len(via) > 5 || req.URL.Scheme != "https" || req.URL.User != nil || (host != "archive.org" && !strings.HasSuffix(host, ".archive.org")) {
			return fmt.Errorf("unexpected archive redirect")
		}
		return nil
	}}
	if err := os.MkdirAll(*root, 0700); err != nil {
		return err
	}
	unlock, err := corpusLock(*root)
	if err != nil {
		return err
	}
	defer unlock()
	for _, dataset := range smallDatasets() {
		item := archiveItem(dataset.ID)
		if item == "" || (*selected != "all-small" && *selected != dataset.ID) {
			continue
		}
		response, err := client.Get("https://archive.org/metadata/" + item)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
		response.Body.Close()
		if err != nil {
			return err
		}
		if response.StatusCode != 200 || len(body) > 8<<20 {
			return fmt.Errorf("archive metadata unavailable for %s", dataset.ID)
		}
		var metadata struct {
			Files  []archiveFile `json:"files"`
			IsDark bool          `json:"is_dark"`
		}
		if err := json.Unmarshal(body, &metadata); err != nil {
			return err
		}
		if metadata.IsDark {
			return fmt.Errorf("archive item unavailable")
		}
		var number int
		fmt.Sscanf(dataset.ID, "doj-%d", &number)
		name := fmt.Sprintf("DataSet %d.zip", number)
		var matches []archiveFile
		for _, file := range metadata.Files {
			if file.Name == name && file.Source == "original" {
				matches = append(matches, file)
			}
		}
		if len(matches) != 1 {
			return fmt.Errorf("archive must contain exactly one original %s", name)
		}
		file := matches[0]
		size, err := strconv.ParseInt(file.Size, 10, 64)
		if err != nil || size < 1 || size > *budget {
			return fmt.Errorf("%s exceeds remaining archive byte budget", dataset.ID)
		}
		*budget -= size
		// Metadata revisions get distinct directories; an earlier copy is retained.
		raw, _ := json.Marshal(file)
		binding := sha256.Sum256(raw)
		dir := filepath.Join(*root, dataset.ID, hex.EncodeToString(binding[:]))
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "archive-metadata.json"), body, 0600); err != nil {
			return err
		}
		destination := filepath.Join(dir, name)
		receipt := archiveReceipt{Dataset: dataset.ID, Item: item, URL: "https://archive.org/download/" + item + "/" + url.PathEscape(name), OfficialRelease: dataset.Release, OfficialStatus: "unresolved", File: file, RetrievedAt: time.Now().UTC().Format(time.RFC3339)}
		fmt.Printf("%s: archived ZIP, %d bytes; current official status unresolved\n", dataset.ID, size)
		if err := downloadArchive(client, destination, &receipt, size); err != nil {
			return fmt.Errorf("%s: %w", dataset.ID, err)
		}
		if err := atomicCorpusJSON(destination+".json", receipt); err != nil {
			return err
		}
		fmt.Printf("prepare --source %s=%s\n", dataset.ID, destination)
	}
	return nil
}

func downloadArchive(client *http.Client, destination string, receipt *archiveReceipt, size int64) error {
	for _, checksum := range []struct {
		value string
		size  int
	}{{receipt.File.SHA1, 20}, {receipt.File.MD5, 16}} {
		decoded, err := hex.DecodeString(checksum.value)
		if err != nil || len(decoded) != checksum.size {
			return fmt.Errorf("missing or invalid archive checksum")
		}
	}
	verify := func(reader io.Reader) (string, error) {
		a, b, c := sha1.New(), md5.New(), sha256.New()
		n, err := io.Copy(io.MultiWriter(a, b, c), io.LimitReader(reader, size+1))
		if err != nil {
			return "", err
		}
		if n != size || hex.EncodeToString(a.Sum(nil)) != strings.ToLower(receipt.File.SHA1) || hex.EncodeToString(b.Sum(nil)) != strings.ToLower(receipt.File.MD5) {
			return "", fmt.Errorf("archive size/checksum mismatch")
		}
		return hex.EncodeToString(c.Sum(nil)), nil
	}
	if existing, err := os.Open(destination); err == nil {
		digest, err := verify(existing)
		existing.Close()
		if err == nil {
			z, zipErr := zip.OpenReader(destination)
			if zipErr != nil {
				return zipErr
			}
			count := len(z.File)
			z.Close()
			if count == 0 {
				return fmt.Errorf("empty archive")
			}
			receipt.SHA256 = digest
			var prior archiveReceipt
			if readCorpusJSON(destination+".json", &prior) == nil && prior.SHA256 == digest && prior.URL == receipt.URL {
				receipt.RetrievedAt = prior.RetrievedAt
			}
			return nil
		}
	}
	response, err := client.Get(receipt.URL)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("archive download HTTP %d", response.StatusCode)
	}
	file, err := os.CreateTemp(filepath.Dir(destination), ".archive-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	digest, err := verify(io.TeeReader(response.Body, file))
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Check the ZIP structure before making the download discoverable. HTML,
	// partial archives, and archive-site derivatives never become source files.
	z, err := zip.OpenReader(file.Name())
	if err != nil {
		return err
	}
	if len(z.File) == 0 {
		z.Close()
		return fmt.Errorf("empty archive")
	}
	z.Close()
	if err := os.Rename(file.Name(), destination); err != nil {
		return err
	}
	receipt.SHA256 = digest
	return nil
}

func archiveProvenance(source corpusSource) (*archiveReceipt, error) {
	if !source.Zip {
		return nil, nil
	}
	var receipt archiveReceipt
	if err := readCorpusJSON(source.Path+".json", &receipt); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	item := archiveItem(source.Dataset)
	expectedURL := "https://archive.org/download/" + item + "/" + url.PathEscape(receipt.File.Name)
	if item == "" || receipt.Dataset != source.Dataset || receipt.Item != item || receipt.URL != expectedURL || receipt.OfficialStatus != "unresolved" || receipt.File.Name != filepath.Base(source.Path) || len(receipt.SHA256) != 64 {
		return nil, fmt.Errorf("invalid archive provenance for %s", source.Dataset)
	}
	file, err := os.Open(source.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	h := sha256.New()
	n, err := io.Copy(h, file)
	if err != nil {
		return nil, err
	}
	if fmt.Sprint(n) != receipt.File.Size || hex.EncodeToString(h.Sum(nil)) != receipt.SHA256 {
		return nil, fmt.Errorf("archived source changed since acquisition")
	}
	return &receipt, nil
}
