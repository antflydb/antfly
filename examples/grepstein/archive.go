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
	MirrorStatus    string      `json:"mirror_reported_status,omitempty"`
}

func archiveItem(dataset string) string {
	switch dataset {
	case "doj-08":
		return "epstein_library_transparency_act_hr_4405_dataset8"
	case "doj-09":
		return "epstein_library_transparency_act_hr_4405_dataset9_202602"
	case "doj-10":
		return "epstein_library_transparency_act_hr_4405_dataset10_202605"
	case "doj-11":
		return "epstein_library_transparency_act_hr_4405_dataset11_202602"
	}
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
	selected := f.String("dataset", "all-small", "all-small, all-large, or doj-01 through doj-12")
	root := f.String("output", "./grepstein-archive", "Archived release ZIP directory")
	expectedPlan := f.String("expected-plan-sha256", "", "Require this metadata-plan digest before downloading any content")
	inventoryOnly := f.Bool("inventory-only", false, "Save archive metadata and a bounded acquisition plan without downloading content")
	requestTimeout := f.Duration("timeout", 30*time.Minute, "Timeout for each archive request; large transfers may need several hours")
	budget := f.Int64("max-bytes", 10<<30, "Maximum selected compressed archive bytes, including cached files")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *budget < 1 || *requestTimeout <= 0 {
		return fmt.Errorf("max-bytes and timeout must be positive")
	}
	if *selected != "all-small" && *selected != "all-large" && archiveItem(*selected) == "" {
		return fmt.Errorf("unknown archived DOJ dataset")
	}
	client := &http.Client{Timeout: *requestTimeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
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
	type acquisition struct {
		Destination string         `json:"destination"`
		Receipt     archiveReceipt `json:"receipt"`
		Bytes       int64          `json:"bytes"`
	}
	var planned []acquisition
	for _, dataset := range smallDatasets() {
		item := archiveItem(dataset.ID)
		large := dataset.ID >= "doj-08" && dataset.ID <= "doj-11"
		if item == "" || (*selected == "all-small" && large) || (*selected == "all-large" && !large) || (*selected != "all-small" && *selected != "all-large" && *selected != dataset.ID) {
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
			Files    []archiveFile `json:"files"`
			IsDark   bool          `json:"is_dark"`
			Metadata struct {
				Description string `json:"description"`
			} `json:"metadata"`
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
		receipt.MirrorStatus = "unspecified"
		if strings.Contains(strings.ToLower(metadata.Metadata.Description), "incomplete") {
			receipt.MirrorStatus = "incomplete"
		}
		planned = append(planned, acquisition{destination, receipt, size})
	}
	identities := make([]any, 0, len(planned))
	for _, p := range planned {
		identities = append(identities, []any{p.Receipt.Dataset, p.Receipt.Item, p.Receipt.URL, p.Receipt.File, p.Receipt.OfficialStatus, p.Receipt.MirrorStatus})
	}
	planData, _ := json.Marshal(identities)
	planDigest := fmt.Sprintf("%x", sha256.Sum256(planData))
	if *expectedPlan != "" && *expectedPlan != planDigest {
		return fmt.Errorf("archive metadata changed: expected plan %s, got %s; review a new inventory", *expectedPlan, planDigest)
	}
	if err := atomicCorpusJSON(filepath.Join(*root, "acquisition-plan.json"), map[string]any{"sha256": planDigest, "archives": planned, "content_downloaded": false, "coverage": "archive snapshots; full official/media coverage unverified"}); err != nil {
		return err
	}
	if *inventoryOnly {
		return printCorpusJSON(planned)
	}
	for _, plan := range planned {
		receipt := plan.Receipt
		fmt.Printf("%s: archived ZIP, %d bytes; mirror status %s; current official status unresolved\n", receipt.Dataset, plan.Bytes, receipt.MirrorStatus)
		if err := downloadArchive(client, plan.Destination, &receipt, plan.Bytes); err != nil {
			return fmt.Errorf("%s: %w", receipt.Dataset, err)
		}
		if err := atomicCorpusJSON(plan.Destination+".json", receipt); err != nil {
			return err
		}
		fmt.Printf("stage-originals --source %s=%s\n", receipt.Dataset, plan.Destination)
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
	identity, _ := json.Marshal([]any{receipt.URL, receipt.File})
	binding := sha256.Sum256(identity)
	partial := destination + ".partial-" + hex.EncodeToString(binding[:])
	file, err := os.OpenFile(partial, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	offset := info.Size()
	if offset > size {
		return fmt.Errorf("partial archive exceeds expected size")
	}
	if offset < size {
		req, err := http.NewRequest(http.MethodGet, receipt.URL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept-Encoding", "identity")
		if offset > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode == http.StatusOK {
			if err := file.Truncate(0); err != nil {
				return err
			}
			offset = 0
		} else if offset == 0 || response.StatusCode != http.StatusPartialContent || response.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", offset, size-1, size) {
			return fmt.Errorf("archive download rejected status/range: HTTP %d", response.StatusCode)
		}
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		n, copyErr := io.Copy(file, io.LimitReader(response.Body, size-offset+1))
		if err := file.Sync(); err != nil {
			return err
		}
		if n > size-offset {
			os.Remove(partial)
			return fmt.Errorf("archive exceeds expected size")
		}
		if copyErr != nil {
			return copyErr
		}
		if n != size-offset {
			return io.ErrUnexpectedEOF
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	digest, err := verify(file)
	if err != nil {
		os.Remove(partial)
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	// Check the ZIP structure before making the download discoverable. HTML,
	// partial archives, and archive-site derivatives never become source files.
	z, err := zip.OpenReader(partial)
	if err != nil {
		return err
	}
	if len(z.File) == 0 {
		z.Close()
		return fmt.Errorf("empty archive")
	}
	z.Close()
	if err := os.Rename(partial, destination); err != nil {
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
