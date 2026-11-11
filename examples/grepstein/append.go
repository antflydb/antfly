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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type corpusAppendPlan struct {
	At       string `json:"at"`
	Error    string `json:"error,omitempty"`
	absent   map[string]bool
	Binding  string `json:"binding"`
	Records  int    `json:"records"`
	New      int    `json:"new"`
	Reused   int    `json:"reused"`
	Complete bool   `json:"complete"`
}

// Content identity deliberately excludes release-local names, but includes
// access policy and page windows: equal bytes do not imply equal authority.
func corpusSourceIdentity(doc map[string]any) (string, error) {
	source, _ := doc["url"].(string)
	version, _ := doc["version"].(string)
	if source == "" || version == "" {
		return "", fmt.Errorf("append requires a source URL and immutable version")
	}
	u, err := url.Parse(source)
	if err != nil || u.Scheme != "gs" || u.Host == "" || u.User != nil || u.Fragment != "" || u.Query().Get("generation") == "" {
		return "", fmt.Errorf("append requires a generation-pinned GCS locator")
	}
	meta, _ := doc["metadata"].(map[string]any)
	data, err := json.Marshal([]any{source, version, doc["mime_type"], doc["permissions"], meta["page_start"], meta["page_end"]})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}

// Exact-key reuse must not silently swallow changed permissions, metadata or
// producer inputs. Only the uploader timestamp is operational, not source data.
func corpusDocumentDigest(doc map[string]any) (string, error) {
	copy := make(map[string]any, len(doc))
	for k, v := range doc {
		if k != "indexed_at" && k != "_id" && k != "_version" && k != "_score" && k != "_index" {
			copy[k] = v
		}
	}
	data, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}

func corpusAppendExisting(client *http.Client, tableURL, id string, doc map[string]any) (bool, error) {
	return corpusAppendExistingContext(context.Background(), client, tableURL, id, doc)
}

func corpusAppendExistingContext(ctx context.Context, client *http.Client, tableURL, id string, doc map[string]any) (bool, error) {
	raw, err := corpusRequestWithContext(ctx, client, http.MethodGet, tableURL+"/documents/"+url.PathEscape(id), nil)
	if err != nil {
		var status *corpusHTTPError
		if errors.As(err, &status) && status.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	var current map[string]any
	if err := json.Unmarshal(raw, &current); err != nil {
		return false, err
	}
	if nested, ok := current["_source"].(map[string]any); ok {
		current = nested
	}
	wanted, err := corpusDocumentDigest(doc)
	if err != nil {
		return false, err
	}
	found, err := corpusDocumentDigest(current)
	if err != nil {
		return false, err
	}
	if wanted != found {
		return false, fmt.Errorf("%s: existing document differs; append will not update or rerun its producers", id)
	}
	return true, nil
}

func walkCorpusPublication(state string, visit func(corpusRecord) error) error {
	if err := validatePublication(state); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(state, "published.ndjson"))
	if err != nil {
		return err
	}
	defer f.Close()
	return walkCorpusRecords(f, visit)
}

func walkCorpusRecords(input io.Reader, visit func(corpusRecord) error) error {
	reader := bufio.NewReader(input)
	for {
		var r corpusRecord
		_, err := readCorpusLine(reader, &r)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := visit(r); err != nil {
			return err
		}
	}
}

// A complete preflight finds late conflicts before submitting any new rows.
// The remote primary catalog is authoritative, including after a lost response.
// Accepted state catalogs additionally detect renamed copies across releases.
// The identity index is explicitly bounded; source bodies are streamed.
func planCorpusAppend(client *http.Client, tableURL string, input *os.File, accepted []string, endpoint, table, tableID, description string) (plan corpusAppendPlan, err error) {
	plan.At = time.Now().UTC().Format(time.RFC3339)
	identities := make(map[string]string)
	const maxIdentities = 2_000_000
	add := func(r corpusRecord) error {
		source, err := corpusSourceIdentity(r.Document)
		if err != nil {
			return fmt.Errorf("%s: %w", r.ID, err)
		}
		if prior, exists := identities[source]; exists && prior != r.ID {
			return fmt.Errorf("%s duplicates immutable content already identified as %s; retain the association in the raw originals catalog and use the canonical document ID before publishing an append", r.ID, prior)
		}
		if _, exists := identities[source]; !exists && len(identities) >= maxIdentities {
			return fmt.Errorf("append identity budget exceeded; split the release into smaller reviewed batches")
		}
		identities[source] = r.ID
		return nil
	}
	for _, state := range accepted {
		var publication struct {
			SHA256 string `json:"sha256"`
		}
		var progress corpusProgress
		if err := readCorpusJSON(filepath.Join(state, "publication.json"), &publication); err != nil {
			return plan, err
		}
		if err := readCorpusJSON(filepath.Join(state, "load.json"), &progress); err != nil {
			return plan, err
		}
		parts := []any{endpoint, table, tableID, description, publication.SHA256}
		raw, _ := json.Marshal(parts)
		h := sha256.Sum256(raw)
		regular := hex.EncodeToString(h[:])
		raw, _ = json.Marshal(append(parts, "append-only-v1"))
		h = sha256.Sum256(raw)
		if !progress.Complete || (progress.Binding != regular && progress.Binding != hex.EncodeToString(h[:])) {
			return plan, fmt.Errorf("accepted state %s is not a completed load of this exact table/configuration", state)
		}
		if err := walkCorpusPublication(state, add); err != nil {
			return plan, err
		}
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return plan, err
	}
	workCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group, ctx := errgroup.WithContext(workCtx)
	group.SetLimit(4)
	plan.absent = make(map[string]bool)
	var mutex sync.Mutex
	scanErr := walkCorpusRecords(input, func(r corpusRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := add(r); err != nil {
			return err
		}
		group.Go(func() error {
			present, err := corpusAppendExistingContext(ctx, client, tableURL, r.ID, r.Document)
			if err != nil {
				return fmt.Errorf("%s: %w", r.ID, err)
			}
			mutex.Lock()
			defer mutex.Unlock()
			plan.Records++
			if present {
				plan.Reused++
			} else {
				plan.New++
				plan.absent[r.ID] = true
			}
			return nil
		})
		return nil
	})
	if scanErr != nil {
		cancel()
	}
	err = group.Wait()
	if scanErr != nil && (err == nil || !errors.Is(scanErr, context.Canceled)) {
		err = scanErr
	}

	plan.Complete = err == nil
	return plan, err
}

// Never use an ordinary upsert for append. The absence predicates are checked
// at commit, closing the GET/write race with competing writers. On conflicts or
// unknown outcomes the caller stops; a later invocation re-observes primaries
// and reuses matching rows without writing them again.
func appendCorpusBatch(client *http.Client, endpoint, table, tableURL string, inserts map[string]map[string]any, verifiedAbsent ...map[string]bool) (json.RawMessage, int, error) {
	pending := make(map[string]map[string]any)
	readSet := make([]map[string]string, 0, len(inserts))
	reused := 0
	ids := make([]string, 0, len(inserts))
	for id := range inserts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		present := false
		if len(verifiedAbsent) == 0 || !verifiedAbsent[0][id] {
			var err error
			present, err = corpusAppendExisting(client, tableURL, id, inserts[id])
			if err != nil {
				return nil, 0, err
			}
		}
		if present {
			reused++
			continue
		}
		pending[id] = inserts[id]
		readSet = append(readSet, map[string]string{"table": table, "key": id, "version": "0"})
	}
	if len(pending) == 0 {
		return json.RawMessage(`{"status":"committed"}`), reused, nil
	}
	response, err := corpusRequest(client, http.MethodPost, strings.TrimRight(endpoint, "/")+"/transactions/commit", map[string]any{
		"read_set": readSet, "tables": map[string]any{table: map[string]any{"inserts": pending}}, "sync_level": "write",
	})
	if err != nil {
		return nil, 0, err
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return nil, 0, err
	}
	switch result.Status {
	case "committed", "committed_visibility_pending", "committed_recovery_pending":
		return response, reused, nil
	default:
		return nil, 0, fmt.Errorf("append transaction requires observation/repair, not replay: %s", response)
	}
}
