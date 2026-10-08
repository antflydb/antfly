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
	"bytes"
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
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
)

// Each source is a revisioned materialization. Its durable server marker is
// published only after unit upserts and stale-row deletion complete. The local
// checkpoint records pass progress; it never suppresses a later revision check.
const corpusGraphVersion = 2

type corpusGraphManifest struct {
	Units       int    `json:"unit_count"`
	Generation  uint64 `json:"generation"`
	Fingerprint string `json:"source_fingerprint"`
}

type corpusGraphProgress struct {
	corpusProgress
	Unchanged int `json:"unchanged"`
	Deleted   int `json:"deleted"`
}

func corpusGraphCmd(args []string) error {
	flags := flag.NewFlagSet("corpus graph", flag.ContinueOnError)
	state := flags.String("state", "./epstein-corpus", "Prepared corpus state directory")
	endpoint := flags.String("url", DefaultAntflyURL, "Antfly API URL")
	table := flags.String("table", "epstein_corpus", "Corpus table created with corpus load --graph")
	if err := flags.Parse(args); err != nil {
		return err
	}
	unlock, err := corpusLock(*state)
	if err != nil {
		return err
	}
	defer unlock()
	client := &http.Client{Timeout: 10 * time.Minute}
	tableURL := strings.TrimRight(*endpoint, "/") + "/tables/" + url.PathEscape(*table)
	status, err := corpusRequest(client, "GET", tableURL, nil)
	if err != nil {
		return err
	}
	var identity struct {
		TableID string         `json:"table_id"`
		Indexes map[string]any `json:"indexes"`
	}
	if err = json.Unmarshal(status, &identity); err != nil {
		return err
	}
	if identity.TableID == "" || identity.Indexes[DefaultAutographIndex] == nil {
		return fmt.Errorf("corpus graph requires a table created with --graph")
	}
	input, err := os.Open(filepath.Join(*state, "records.ndjson"))
	if err != nil {
		return err
	}
	defer input.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, input); err != nil {
		return err
	}
	binding := sha256.Sum256([]byte(*endpoint + "\x00" + identity.TableID + "\x00" + hex.EncodeToString(digest.Sum(nil))))
	checkpoint := filepath.Join(*state, "graph.json")
	var previous corpusProgress
	if err = readCorpusJSON(checkpoint, &previous); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	expected := hex.EncodeToString(binding[:])
	if previous.Binding != "" && previous.Binding != expected {
		return fmt.Errorf("graph checkpoint belongs to another manifest or table")
	}
	progress := corpusGraphProgress{corpusProgress: corpusProgress{Binding: expected}}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	started := time.Now()
	reader := bufio.NewReader(input)
	if err = atomicCorpusJSON(checkpoint, progress); err != nil {
		return err
	}
	for {
		var record corpusRecord
		n, e := readCorpusLine(reader, &record)
		if errors.Is(e, io.EOF) {
			progress.Complete = true
			break
		}
		if e != nil {
			return e
		}
		submitted, deleted, unchanged, e := reconcileCorpusGraphSource(client, tableURL, record)
		if e != nil {
			return fmt.Errorf("source %s: %w", record.ID, e)
		}
		progress.InputOffset += n
		progress.Files++
		progress.Records += submitted
		progress.Deleted += deleted
		if unchanged {
			progress.Unchanged++
		}
		progress.Seconds = time.Since(started).Seconds()
		if err = atomicCorpusJSON(checkpoint, progress); err != nil {
			return err
		}
		if progress.Files%100 == 0 {
			fmt.Printf("Checked %d sources, reused %d revisions, submitted %d graph units\n", progress.Files, progress.Unchanged, progress.Records)
		}
	}
	progress.Seconds = time.Since(started).Seconds()
	if err = atomicCorpusJSON(checkpoint, progress); err != nil {
		return err
	}
	return printCorpusJSON(progress)
}

func readCorpusGraphManifest(client *http.Client, tableURL string, record corpusRecord) (corpusGraphManifest, error) {
	var manifest corpusGraphManifest
	raw, err := corpusRequest(client, "GET", tableURL+"/documents/"+url.PathEscape(record.ID)+"/artifacts/"+corpusUnits, nil)
	if err != nil {
		return manifest, fmt.Errorf("not ready; retry after enrichment: %w", err)
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		return manifest, err
	}
	if manifest.Units < 1 || manifest.Generation == 0 || manifest.Fingerprint == "" {
		return manifest, fmt.Errorf("missing extracted units or artifact revision")
	}
	return manifest, nil
}

func corpusGraphRevision(record corpusRecord, manifest corpusGraphManifest) string {
	data, _ := json.Marshal(struct {
		Version  int
		Record   corpusRecord
		Manifest corpusGraphManifest
	}{corpusGraphVersion, record, manifest})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func reconcileCorpusGraphSource(client *http.Client, tableURL string, record corpusRecord) (submitted, deleted int, unchanged bool, err error) {
	source, sourceVersion, err := lookupCorpusGraphVersion(client, tableURL, record.ID, "title,mime_type,original_url,metadata")
	if err != nil {
		return 0, 0, false, err
	}
	record.Document = nil
	if err = json.Unmarshal(source, &record.Document); err != nil {
		return 0, 0, false, err
	}
	manifest, err := readCorpusGraphManifest(client, tableURL, record)
	if err != nil {
		return 0, 0, false, err
	}
	revision := corpusGraphRevision(record, manifest)
	prefix := record.ID + ":graph:"
	markerKey := prefix + "state"
	marker, markerVersion, err := lookupCorpusGraphVersion(client, tableURL, markerKey, "")
	if err == nil {
		var stored struct {
			Metadata struct {
				Revision string `json:"corpus_graph_revision"`
			} `json:"metadata"`
		}
		if err = json.Unmarshal(marker, &stored); err != nil {
			return 0, 0, false, err
		}
		if stored.Metadata.Revision == revision {
			return 0, 0, true, nil
		}
	} else {
		var response *corpusHTTPError
		if !errors.As(err, &response) || response.StatusCode != http.StatusNotFound {
			return 0, 0, false, err
		}
		markerVersion = "0"
	}
	writer := newCorpusGraphWriter(client, tableURL, record.ID, sourceVersion, markerKey, markerVersion)
	// Stage one source on disk before publishing. Failed extraction or a changing
	// revision leaves the existing materialization intact. Only unit IDs stay in RAM.
	stage, err := os.CreateTemp("", "epstein-graph-*.ndjson")
	if err != nil {
		return 0, 0, false, err
	}
	defer func() { stage.Close(); os.Remove(stage.Name()) }()
	desired := map[string]struct{}{}
	encode := json.NewEncoder(stage)
	units := 0
	var cursor []any
	for {
		payload := map[string]any{"fields": []string{"unit_id", "unit_type", "text", "provenance"}, "hierarchy": map[string]any{"children": map[string]any{"level": "unit", "parent": map[string]any{"level": "source", "id": record.ID}}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 50}
		if cursor != nil {
			payload["search_after"] = cursor
		}
		raw, e := corpusRequest(client, "POST", tableURL+"/query", payload)
		if e != nil {
			return 0, 0, false, e
		}
		hits, e := corpusGraphHits(raw)
		if e != nil {
			return 0, 0, false, e
		}
		if len(hits) == 0 {
			break
		}
		for _, hit := range hits {
			units++
			text, _ := hit.Source["text"].(string)
			provenance, _ := hit.Source["provenance"].(map[string]any)
			status, _ := provenance["extraction_status"].(string)
			if strings.HasPrefix(status, "failed_") || strings.HasPrefix(status, "pending_") {
				return 0, 0, false, fmt.Errorf("extraction status %s; repair its artifact before graph work", status)
			}
			if strings.TrimSpace(text) == "" {
				mime, _ := record.Document["mime_type"].(string)
				if strings.HasPrefix(mime, "audio/") {
					return 0, 0, false, fmt.Errorf("no transcript yet; inspect its artifact and retry")
				}
				continue
			}
			// Preserve the original key convention so old materializations are updated.
			idSum := sha256.Sum256([]byte(hit.ID))
			key := prefix + hex.EncodeToString(idSum[:16])
			if _, exists := desired[key]; exists {
				return 0, 0, false, fmt.Errorf("duplicate extracted unit %s", hit.ID)
			}
			desired[key] = struct{}{}
			citation, page := corpusGraphCitation(record, provenance)
			doc := map[string]any{"content": text, "title": record.Document["title"], "original_url": citation, "metadata": map[string]any{"source_record_id": record.ID, "unit_id": hit.Source["unit_id"], "page_number": page, "provenance": provenance, "corpus_graph_unit": true, "corpus_graph_revision": revision}}
			if e = encode.Encode(corpusRecord{ID: key, Document: doc}); e != nil {
				return 0, 0, false, e
			}
		}
		next := hits[len(hits)-1].Sort
		if len(next) != 2 {
			return 0, 0, false, fmt.Errorf("unit traversal returned no continuation position")
		}
		a, _ := json.Marshal(cursor)
		b, _ := json.Marshal(next)
		if bytes.Equal(a, b) {
			return 0, 0, false, fmt.Errorf("unit traversal did not advance")
		}
		cursor = next
	}
	if units != manifest.Units {
		return 0, 0, false, fmt.Errorf("artifact changed during graph preparation; retry")
	}
	if err = checkCorpusGraphRevision(client, tableURL, record, revision); err != nil {
		return 0, 0, false, err
	}
	owner, err := newCorpusKey()
	if err != nil {
		return 0, 0, false, err
	}
	publishing := map[string]any{"metadata": map[string]any{"source_record_id": record.ID, "corpus_graph_owner": owner, "corpus_graph_target_revision": revision, "corpus_graph_revision": ""}}
	if err = writer.commit(map[string]any{"inserts": map[string]any{markerKey: publishing}}); err != nil {
		return 0, 0, false, err
	}
	claimed, claimedVersion, err := lookupCorpusGraphVersion(client, tableURL, markerKey, "")
	if err != nil {
		return 0, 0, false, err
	}
	var claim struct {
		Metadata struct {
			Owner string `json:"corpus_graph_owner"`
		} `json:"metadata"`
	}
	if err = json.Unmarshal(claimed, &claim); err != nil {
		return 0, 0, false, err
	}
	if claim.Metadata.Owner != owner {
		return 0, 0, false, fmt.Errorf("another graph writer superseded this pass; retry")
	}
	writer.markerVersion = claimedVersion
	if _, err = stage.Seek(0, io.SeekStart); err != nil {
		return 0, 0, false, err
	}
	staged := bufio.NewReader(stage)
	for {
		inserts := map[string]map[string]any{}
		for len(inserts) < 50 {
			var row corpusRecord
			_, e := readCorpusLine(staged, &row)
			if errors.Is(e, io.EOF) {
				break
			}
			if e != nil {
				return submitted, 0, false, e
			}
			inserts[row.ID] = row.Document
		}
		if len(inserts) == 0 {
			break
		}
		if err = writer.commit(map[string]any{"inserts": inserts}); err != nil {
			return submitted, 0, false, err
		}
		submitted += len(inserts)
	}
	// A primary-key scan finds stale rows even after a crash, a lost local
	// checkpoint, or a previous partially published generation. Deletion includes
	// their generated relation artifacts and edges through the normal batch API.
	from := prefix
	for {
		raw, e := corpusRawRequest(client, "POST", tableURL+"/documents", map[string]any{"from": from, "to": prefix[:len(prefix)-1] + ";", "exclusive_to": true, "limit": 50})
		if e != nil {
			return submitted, deleted, false, e
		}
		scan := bufio.NewReader(bytes.NewReader(raw))
		count := 0
		next := from
		var stale []string
		for {
			var row struct {
				ID string `json:"_id"`
			}
			_, e := readCorpusLine(scan, &row)
			if errors.Is(e, io.EOF) {
				break
			}
			if e != nil {
				return submitted, deleted, false, e
			}
			if row.ID <= next || !strings.HasPrefix(row.ID, prefix) {
				return submitted, deleted, false, fmt.Errorf("graph row scan did not advance within source prefix")
			}
			next = row.ID
			count++
			if _, ok := desired[row.ID]; !ok && row.ID != markerKey {
				stale = append(stale, row.ID)
			}
		}
		if len(stale) > 0 {
			if err = writer.commit(map[string]any{"deletes": stale}); err != nil {
				return submitted, deleted, false, err
			}
			deleted += len(stale)
		}
		if count == 0 {
			break
		}
		from = next
	}
	if err = checkCorpusGraphRevision(client, tableURL, record, revision); err != nil {
		return submitted, deleted, false, err
	}
	markerDoc := map[string]any{"metadata": map[string]any{"source_record_id": record.ID, "corpus_graph_revision": revision, "corpus_graph_materialization_version": corpusGraphVersion, "corpus_graph_unit_count": len(desired)}}
	if err = writer.commit(map[string]any{"inserts": map[string]any{markerKey: markerDoc}}); err != nil {
		return submitted, deleted, false, err
	}
	return submitted, deleted, false, nil
}

func corpusGraphHits(raw []byte) ([]antfly.QueryHit, error) {
	var result antfly.QueryResponses
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	if len(result.Responses) != 1 {
		return nil, fmt.Errorf("missing unit query response")
	}
	if result.Responses[0].Error != "" {
		return nil, fmt.Errorf("unit query: %s", result.Responses[0].Error)
	}
	return result.Responses[0].Hits.Hits, nil
}
func checkCorpusGraphRevision(client *http.Client, tableURL string, record corpusRecord, revision string) error {
	current, err := readCorpusGraphManifest(client, tableURL, record)
	if err != nil {
		return err
	}
	if corpusGraphRevision(record, current) != revision {
		return fmt.Errorf("artifact changed during graph preparation; retry")
	}
	return nil
}

// Version fences protect every mutation, not just the final marker: a newer
// writer can supersede an interrupted pass, while the older pass cannot alter
// its rows after losing ownership. Source replacement is fenced at the same time.
type corpusGraphWriter struct {
	client                                                              *http.Client
	commitURL, table, sourceID, sourceVersion, markerKey, markerVersion string
}

func newCorpusGraphWriter(client *http.Client, tableURL, sourceID, sourceVersion, markerKey, markerVersion string) *corpusGraphWriter {
	split := strings.LastIndex(tableURL, "/tables/")
	table, _ := url.PathUnescape(tableURL[split+len("/tables/"):])
	return &corpusGraphWriter{client: client, commitURL: tableURL[:split] + "/transactions/commit", table: table, sourceID: sourceID, sourceVersion: sourceVersion, markerKey: markerKey, markerVersion: markerVersion}
}
func (w *corpusGraphWriter) commit(operations map[string]any) error {
	body := map[string]any{"sync_level": "write", "read_set": []any{
		map[string]any{"table": w.table, "key": w.sourceID, "version": w.sourceVersion},
		map[string]any{"table": w.table, "key": w.markerKey, "version": w.markerVersion},
	}, "tables": map[string]any{w.table: operations}}
	raw, err := corpusRequest(w.client, "POST", w.commitURL, body)
	if err != nil {
		return err
	}
	var result struct {
		Status string `json:"status"`
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		return err
	}
	switch result.Status {
	case "committed", "committed_visibility_pending", "committed_recovery_pending":
		return nil
	default:
		return fmt.Errorf("graph transaction outcome %q; inspect transaction/index status before retry", result.Status)
	}
}
func lookupCorpusGraphVersion(client *http.Client, tableURL, key, fields string) ([]byte, string, error) {
	endpoint := tableURL + "/documents/" + url.PathEscape(key)
	if fields != "" {
		endpoint += "?fields=" + url.QueryEscape(fields)
	}
	var headers http.Header
	data, err := corpusRawRequestWithHeaders(client, "GET", endpoint, nil, &headers)
	if err != nil {
		return nil, "", err
	}
	version := headers.Get("X-Antfly-Version")
	if version == "" || version == "0" {
		return nil, "", fmt.Errorf("document %s has no version token; graph reconciliation requires OCC lookups", key)
	}
	return data, version, nil
}
func corpusGraphCitation(record corpusRecord, provenance map[string]any) (string, int) {
	original, _ := record.Document["original_url"].(string)
	metadata, _ := record.Document["metadata"].(map[string]any)
	page, _ := provenance["page_number"].(float64)
	citation, number := corpusPageCitation(original, metadata, page)
	mime, _ := record.Document["mime_type"].(string)
	if strings.HasPrefix(mime, "audio/") {
		if spans, ok := provenance["transcript_spans"].([]any); ok && len(spans) > 0 {
			if span, ok := spans[0].(map[string]any); ok {
				if start, ok := span["start_ms"].(float64); ok && start >= 0 {
					citation = strings.Split(original, "#")[0] + fmt.Sprintf("#t=%.3f", start/1000)
				}
			}
		}
	}
	return citation, number
}
