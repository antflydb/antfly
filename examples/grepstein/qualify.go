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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type extractionQualification struct {
	PublicationSHA256 string `json:"publication_sha256"`
	At                string `json:"at"`
	Complete          bool   `json:"complete"`
	Records           int    `json:"records"`
	Pages             int    `json:"pages"`
	OCRAttempted      int    `json:"ocr_attempted"`
	OCRFailed         int    `json:"ocr_failed"`
}

// Index readiness and successful document extraction are separate milestones.
// Validate the sealed source inventory, not just a sample or serving status.
func qualifyCmd(args []string) error {
	f := flag.NewFlagSet("qualify", flag.ContinueOnError)
	state := f.String("state", "./grepstein-corpus", "Published corpus state")
	endpoint := f.String("url", DefaultAntflyURL, "Antfly API")
	table := f.String("table", "grepstein_corpus", "Corpus table")
	output := f.String("output", "", "Save a successful extraction qualification receipt")
	if err := f.Parse(args); err != nil {
		return err
	}
	unlock, err := corpusLock(*state)
	if err != nil {
		return err
	}
	defer unlock()
	q, err := qualifyCorpus(&http.Client{Timeout: time.Minute}, strings.TrimRight(*endpoint, "/")+"/tables/"+url.PathEscape(*table), *state)
	if *output != "" {
		if writeErr := atomicCorpusJSON(*output, q); writeErr != nil {
			return writeErr
		}
	}
	if err != nil {
		return err
	}
	return printCorpusJSON(q)
}

func qualifyCorpus(client *http.Client, tableURL, state string) (extractionQualification, error) {
	q := extractionQualification{At: time.Now().UTC().Format(time.RFC3339)}
	if err := validatePublication(state); err != nil {
		return q, err
	}
	var publication struct {
		SHA256 string `json:"sha256"`
	}
	if err := readCorpusJSON(filepath.Join(state, "publication.json"), &publication); err != nil {
		return q, err
	}
	q.PublicationSHA256 = publication.SHA256
	raw, err := corpusRequest(client, "GET", tableURL+"/indexes/"+corpusTextIndex, nil)
	if err != nil {
		return q, err
	}
	ready, err := indexComplete(raw)
	if err != nil {
		return q, err
	}
	if !ready {
		return q, fmt.Errorf("full-text index is not complete and queryable")
	}
	file, err := os.Open(filepath.Join(state, "published.ndjson"))
	if err != nil {
		return q, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		var record corpusRecord
		if _, err = readCorpusLine(reader, &record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return q, err
		}
		source, _ := record.Document["url"].(string)
		version, _ := record.Document["version"].(string)
		uri, parseErr := url.Parse(source)
		if parseErr != nil || !validOriginalLocator(uri, uri.Host) || uri.Scheme != "gs" || uri.Fragment != "" {
			return q, fmt.Errorf("%s: qualification requires a generation-pinned native GCS source", record.ID)
		}
		hash := strings.TrimSuffix(path.Base(uri.Path), ".pdf")
		if version != hash+":"+uri.Query().Get("generation") {
			return q, fmt.Errorf("%s: source version does not match the immutable locator", record.ID)
		}
		meta, ok := record.Document["metadata"].(map[string]any)
		if !ok {
			return q, fmt.Errorf("%s: missing source metadata", record.ID)
		}
		first, okFirst := corpusMetadataNumber(meta["page_start"])
		last, okLast := corpusMetadataNumber(meta["page_end"])
		if !okFirst || !okLast || first < 1 || last < first || first != float64(int(first)) || last != float64(int(last)) {
			return q, fmt.Errorf("%s: invalid PDF page inventory", record.ID)
		}
		current, err := corpusRequest(client, "GET", tableURL+"/documents/"+url.PathEscape(record.ID)+"?fields=url,version", nil)
		if err != nil {
			return q, fmt.Errorf("%s: %w", record.ID, err)
		}
		var doc map[string]any
		if err = json.Unmarshal(current, &doc); err != nil {
			return q, err
		}
		if nested, ok := doc["_source"].(map[string]any); ok {
			doc = nested
		}
		if doc["url"] != source || doc["version"] != version {
			return q, fmt.Errorf("%s: current source differs from the sealed publication", record.ID)
		}
		raw, err = corpusRequest(client, "GET", tableURL+"/documents/"+url.PathEscape(record.ID)+"/artifacts/"+corpusUnits+"?detail=raw", nil)
		if err != nil {
			return q, fmt.Errorf("%s: %w", record.ID, err)
		}
		var artifact struct {
			Source     string `json:"source_url"`
			Generation uint64 `json:"generation"`
			Units      *int   `json:"unit_count"`
			Route      string `json:"route_type"`
			Merge      string `json:"merge_status"`
			LastError  string `json:"last_error_code"`
			Manifest   string `json:"manifest_json"`
		}
		if err = json.Unmarshal(raw, &artifact); err != nil {
			return q, err
		}
		if artifact.Source != source || artifact.Generation == 0 || artifact.Units == nil || *artifact.Units != int(last-first+1) {
			return q, fmt.Errorf("%s: artifact source/generation/page inventory is incomplete or changed", record.ID)
		}
		// The summary API defaults absent OCR counters to zero. Only the raw
		// manifest can distinguish known zero coverage from missing evidence.
		var coverage struct {
			Attempted *int `json:"ocr_attempted_count"`
			Failed    *int `json:"ocr_failed_count"`
		}
		if err = json.Unmarshal([]byte(artifact.Manifest), &coverage); err != nil {
			return q, fmt.Errorf("%s: missing or invalid extraction manifest: %w", record.ID, err)
		}
		if coverage.Attempted == nil || coverage.Failed == nil || *coverage.Attempted < 0 || *coverage.Failed < 0 || *coverage.Failed > *coverage.Attempted {
			return q, fmt.Errorf("%s: OCR coverage is unknown or invalid", record.ID)
		}
		q.OCRAttempted += *coverage.Attempted
		q.OCRFailed += *coverage.Failed
		if artifact.Route != "pdf" || artifact.Merge != "converged" || artifact.LastError != "" {
			return q, fmt.Errorf("%s: extraction is failed, incomplete or unsupported (route=%q, merge=%q, error=%q)", record.ID, artifact.Route, artifact.Merge, artifact.LastError)
		}
		if *coverage.Failed != 0 {
			return q, fmt.Errorf("%s: %d OCR pages failed; index readiness does not establish extraction coverage", record.ID, *coverage.Failed)
		}
		q.Records++
		q.Pages += *artifact.Units
	}
	if q.Records == 0 {
		return q, fmt.Errorf("empty extraction inventory")
	}
	q.Complete = true
	return q, nil
}
