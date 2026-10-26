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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var hrefPattern = regexp.MustCompile(`(?i)href\s*=\s*["']([^"']+)["']`)
var pdfName = regexp.MustCompile(`(?i)^EFTA[0-9]+\.pdf$`)

// Discover the release completely before downloading. Missing pages, access
// gates, unexpected redirects, and non-PDF responses are hard failures rather
// than a silently authoritative partial inventory.
func discoverDOJ(client *http.Client, release string) ([]string, error) {
	start, err := url.Parse(release)
	if err != nil {
		return nil, err
	}
	pending := []string{release}
	seen := map[string]bool{}
	pdfs := map[string]bool{}
	names := map[string]string{}
	for len(pending) > 0 {
		current := pending[0]
		pending = pending[1:]
		if seen[current] {
			continue
		}
		seen[current] = true
		if len(seen) > 1000 {
			return nil, fmt.Errorf("release pagination exceeded 1000 pages")
		}
		resp, err := client.Get(current)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != 200 || resp.Request.URL.Host != start.Host || resp.Request.URL.Path != start.Path || len(body) > 8<<20 {
			return nil, fmt.Errorf("release unavailable: %s (HTTP %d)", current, resp.StatusCode)
		}
		found := 0
		for _, match := range hrefPattern.FindAllSubmatch(body, -1) {
			reference, err := url.Parse(html.UnescapeString(string(match[1])))
			if err != nil {
				continue
			}
			u := start.ResolveReference(reference)
			u.Fragment = ""
			if u.Scheme != start.Scheme || u.Host != start.Host || u.User != nil {
				continue
			}
			prefix := "/epstein/files/"
			if match := regexp.MustCompile(`/data-set-([0-9]+)-files$`).FindStringSubmatch(start.Path); len(match) == 2 {
				prefix += "DataSet " + match[1] + "/"
			}
			if pdfName.MatchString(path.Base(u.Path)) && strings.HasPrefix(u.Path, prefix) && u.RawQuery == "" {
				if prior := names[path.Base(u.Path)]; prior != "" && prior != u.String() {
					return nil, fmt.Errorf("ambiguous PDF filename in release")
				}
				names[path.Base(u.Path)] = u.String()
				pdfs[u.String()] = true
				found++
				continue
			}
			if u.Path == start.Path && u.Query().Get("page") != "" {
				page, err := strconv.Atoi(u.Query().Get("page"))
				if err != nil || page < 0 || page > 999 {
					return nil, fmt.Errorf("invalid release page")
				}
				if page == 0 {
					u.RawQuery = ""
				} else {
					u.RawQuery = url.Values{"page": {strconv.Itoa(page)}}.Encode()
				}
				pending = append(pending, u.String())
			}
		}
		if found == 0 {
			return nil, fmt.Errorf("release page contains no PDF links (possible access/age gate): %s", current)
		}
		if len(pdfs) > 10000 {
			return nil, fmt.Errorf("release exceeds v1 inventory budget")
		}
	}
	result := make([]string, 0, len(pdfs))
	for pdf := range pdfs {
		result = append(result, pdf)
	}
	sort.Strings(result)
	return result, nil
}

type downloadReceipt struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

func downloadPDF(client *http.Client, source, destination string) error {
	return downloadPDFBounded(client, source, destination, 2<<30)
}
func downloadPDFBounded(client *http.Client, source, destination string, maxBytes int64) error {
	var previous downloadReceipt
	if readCorpusJSON(destination+".json", &previous) == nil && previous.URL == source {
		if previous.Bytes > maxBytes {
			return fmt.Errorf("download exceeds remaining source byte budget")
		}
		file, err := os.Open(destination)
		if err == nil {
			h := sha256.New()
			n, err := io.Copy(h, file)
			file.Close()
			if err == nil && n == previous.Bytes && hex.EncodeToString(h.Sum(nil)) == previous.SHA256 {
				return nil
			}
		}
	}
	response, err := client.Get(source)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("source HTTP %d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	prefix, err := reader.Peek(5)
	if err != nil || string(prefix) != "%PDF-" {
		return fmt.Errorf("source returned non-PDF content (possible access gate)")
	}
	file, err := os.CreateTemp(filepath.Dir(destination), ".download-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, h), io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return err
	}
	if n > maxBytes {
		return fmt.Errorf("PDF exceeds source byte budget")
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), destination); err != nil {
		return err
	}
	return atomicCorpusJSON(destination+".json", downloadReceipt{source, hex.EncodeToString(h.Sum(nil)), n})
}

func fetchCmd(args []string) error {
	f := flag.NewFlagSet("fetch", flag.ContinueOnError)
	inventoryOnly := f.Bool("inventory-only", false, "Discover all release pages and save inventories without downloading PDFs")
	root := f.String("output", "./grepstein-data", "Download directory")
	maxBytes := f.Int64("max-bytes", 10<<30, "Maximum total source bytes across selected releases")
	selected := f.String("dataset", "all-small", "all-small or doj-01 through doj-07 / doj-12")
	if err := f.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: 15 * time.Minute}
	// Never forward authentication across hosts when upstream redirects.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 5 || req.URL.Scheme != "https" || req.URL.Host != via[0].URL.Host {
			return fmt.Errorf("unexpected upstream redirect")
		}
		return nil
	}
	if *maxBytes < 1 {
		return fmt.Errorf("max-bytes must be positive")
	}
	var downloadedBytes int64
	matched := false
	for _, dataset := range smallDatasets() {
		if !strings.HasPrefix(dataset.ID, "doj-") || dataset.Stage != "v1" {
			continue
		}
		if *selected != "all-small" && dataset.ID != *selected {
			continue
		}
		matched = true
		dir := filepath.Join(*root, dataset.ID)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		unlock, err := corpusLock(dir)
		if err != nil {
			return err
		}
		err = func() error {
			defer unlock()
			files, err := discoverDOJ(client, dataset.Release)
			if err != nil {
				return err
			}
			inventoryBytes, _ := json.Marshal(files)
			inventoryHash := sha256.Sum256(inventoryBytes)
			snapshot, err := filepath.Abs(filepath.Join(dir, hex.EncodeToString(inventoryHash[:])))
			if err != nil {
				return err
			}
			if err = os.MkdirAll(snapshot, 0700); err != nil {
				return err
			}
			dir = snapshot
			if err = atomicCorpusJSON(filepath.Join(dir, "inventory.json"), files); err != nil {
				return err
			}
			if *inventoryOnly {
				fmt.Printf("%s: %d PDFs (inventory only)\n", dataset.ID, len(files))
				return nil
			}
			for i, source := range files {
				u, _ := url.Parse(source)
				fmt.Printf("%s %d/%d %s\n", dataset.ID, i+1, len(files), path.Base(u.Path))
				if downloadedBytes >= *maxBytes {
					return fmt.Errorf("selected releases exceed source byte budget")
				}
				destination := filepath.Join(dir, path.Base(u.Path))
				if err = downloadPDFBounded(client, source, destination, min(2<<30, *maxBytes-downloadedBytes)); err != nil {
					return err
				}
				info, err := os.Stat(destination)
				if err != nil {
					return err
				}
				downloadedBytes += info.Size()
			}
			receipt := downloadSnapshot{Dataset: dataset.ID, Files: len(files), Release: dataset.Release, Directory: dir}
			if err = atomicCorpusJSON(filepath.Join(dir, "complete.json"), receipt); err != nil {
				return err
			}
			return atomicCorpusJSON(filepath.Join(*root, dataset.ID, "current.json"), receipt)
		}()
		if err != nil {
			return fmt.Errorf("%s: %w", dataset.ID, err)
		}
	}
	if !matched {
		return fmt.Errorf("dataset is not in the small v1 release set")
	}
	return nil
}

type downloadSnapshot struct {
	Dataset   string `json:"dataset"`
	Files     int    `json:"files"`
	Release   string `json:"release"`
	Directory string `json:"directory"`
}

func prepareSmallCmd(args []string) error {
	f := flag.NewFlagSet("prepare-small", flag.ContinueOnError)
	root := f.String("downloads", "./grepstein-data", "Completed fetch snapshots")
	state := f.String("state", "./grepstein-corpus", "New prepared state")
	pages := f.Int("pages-per-record", 25, "Maximum pages in each remotely fetched PDF part")
	court := f.String("court", "", "Optional local court-2024 PDF directory or ZIP")
	if err := f.Parse(args); err != nil {
		return err
	}
	prepare := []string{"--state", *state, "--pages-per-record", fmt.Sprint(*pages)}
	for _, dataset := range smallDatasets() {
		if dataset.Stage != "v1" || !strings.HasPrefix(dataset.ID, "doj-") {
			continue
		}
		var receipt downloadSnapshot
		if err := readCorpusJSON(filepath.Join(*root, dataset.ID, "current.json"), &receipt); err != nil {
			return fmt.Errorf("fetch %s first: %w", dataset.ID, err)
		}
		if receipt.Dataset != dataset.ID || receipt.Release != dataset.Release || receipt.Files < 1 {
			return fmt.Errorf("invalid completed download snapshot")
		}
		var complete downloadSnapshot
		if err := readCorpusJSON(filepath.Join(receipt.Directory, "complete.json"), &complete); err != nil || complete != receipt {
			return fmt.Errorf("download snapshot is incomplete")
		}
		if err := validateDownloadSnapshot(receipt); err != nil {
			return err
		}
		prepare = append(prepare, "--source", dataset.ID+"="+receipt.Directory)
	}
	if *court != "" {
		prepare = append(prepare, "--source", "court-2024="+*court)
	}
	return corpusPrepareCmd(prepare)
}

func validateDownloadSnapshot(snapshot downloadSnapshot) error {
	var inventory []string
	if err := readCorpusJSON(filepath.Join(snapshot.Directory, "inventory.json"), &inventory); err != nil {
		return err
	}
	if len(inventory) != snapshot.Files {
		return fmt.Errorf("completed download inventory changed")
	}
	expected := map[string]bool{}
	for _, source := range inventory {
		parsed, err := url.Parse(source)
		if err != nil {
			return err
		}
		name := path.Base(parsed.Path)
		if !pdfName.MatchString(name) || expected[name] {
			return fmt.Errorf("invalid download inventory")
		}
		expected[name] = true
		var receipt downloadReceipt
		if err = readCorpusJSON(filepath.Join(snapshot.Directory, name)+".json", &receipt); err != nil {
			return err
		}
		if receipt.URL != source {
			return fmt.Errorf("download receipt belongs to another source")
		}
		file, err := os.Open(filepath.Join(snapshot.Directory, name))
		if err != nil {
			return err
		}
		digest := sha256.New()
		size, err := io.Copy(digest, file)
		file.Close()
		if err != nil {
			return err
		}
		if size != receipt.Bytes || hex.EncodeToString(digest.Sum(nil)) != receipt.SHA256 {
			return fmt.Errorf("download snapshot bytes changed: %s", name)
		}
	}
	entries, err := os.ReadDir(snapshot.Directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || corpusMIME(entry.Name()) != "" && !expected[entry.Name()] {
			return fmt.Errorf("unexpected source in completed download snapshot")
		}
	}
	return nil
}
