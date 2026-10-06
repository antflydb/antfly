// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The document-unit asset stores a bookkeeping manifest, not an aggregate text
// blob. Traverse its units explicitly before feeding the existing graph producer.
// One source's unit page is held at a time, and replay uses deterministic unit IDs.
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
	var progress corpusProgress
	if err = readCorpusJSON(checkpoint, &progress); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	expected := hex.EncodeToString(binding[:])
	if progress.Binding != "" && progress.Binding != expected {
		return fmt.Errorf("graph checkpoint belongs to another manifest or table")
	}
	progress.Binding = expected
	if progress.Complete {
		return printCorpusJSON(progress)
	}
	if _, err = input.Seek(progress.InputOffset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(input)
	started := time.Now()
	priorSeconds := progress.Seconds
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
		// A missing manifest means enrichment has not finished: stop without advancing
		// this source, rather than permanently skipping pending OCR/transcription.
		manifest, e := corpusRequest(client, "GET", tableURL+"/documents/"+url.PathEscape(record.ID)+"/artifacts/"+corpusUnits, nil)
		if e != nil {
			return fmt.Errorf("source %s is not ready; retry after enrichment: %w", record.ID, e)
		}
		var summary struct {
			Units int `json:"unit_count"`
		}
		if e = json.Unmarshal(manifest, &summary); e != nil {
			return e
		}
		if summary.Units < 1 {
			return fmt.Errorf("source %s has no extracted units yet", record.ID)
		}
		var cursor []any
		unitCount := 0
		submitted := 0
		for {
			payload := map[string]any{"fields": []string{"unit_id", "unit_type", "text", "provenance"}, "hierarchy": map[string]any{"children": map[string]any{"level": "unit", "parent": map[string]any{"level": "source", "id": record.ID}}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 50}
			if cursor != nil {
				payload["search_after"] = cursor
			}
			raw, e := corpusRequest(client, "POST", tableURL+"/query", payload)
			if e != nil {
				return e
			}
			var result antfly.QueryResponses
			if e = json.Unmarshal(raw, &result); e != nil {
				return e
			}
			if len(result.Responses) != 1 {
				return fmt.Errorf("missing unit query response")
			}
			if result.Responses[0].Error != "" {
				return fmt.Errorf("unit query: %s", result.Responses[0].Error)
			}
			hits := result.Responses[0].Hits.Hits
			if len(hits) == 0 {
				break
			}
			inserts := map[string]map[string]any{}
			for _, hit := range hits {
				unitCount++
				text, _ := hit.Source["text"].(string)
				provenance, _ := hit.Source["provenance"].(map[string]any)
				status, _ := provenance["extraction_status"].(string)
				if strings.HasPrefix(status, "failed_") || strings.HasPrefix(status, "pending_") {
					return fmt.Errorf("source %s extraction status %s; repair its artifact before graph work", record.ID, status)
				}
				if strings.TrimSpace(text) == "" {
					mime, _ := record.Document["mime_type"].(string)
					if strings.HasPrefix(mime, "audio/") {
						return fmt.Errorf("audio source %s has no transcript yet; inspect its artifact and retry", record.ID)
					}
					continue
				}
				idSum := sha256.Sum256([]byte(hit.ID))
				key := record.ID + ":graph:" + hex.EncodeToString(idSum[:16])
				inserts[key] = map[string]any{"content": text, "title": record.Document["title"], "original_url": record.Document["original_url"], "metadata": map[string]any{"source_record_id": record.ID, "unit_id": hit.Source["unit_id"], "provenance": provenance, "corpus_graph_unit": true}}
			}
			if len(inserts) > 0 {
				if _, e = corpusRequest(client, "POST", tableURL+"/batch", map[string]any{"inserts": inserts, "sync_level": "write"}); e != nil {
					return e
				}
				submitted += len(inserts)
			}
			next := hits[len(hits)-1].Sort
			if len(next) != 2 {
				return fmt.Errorf("unit traversal returned no continuation position")
			}
			if cursor != nil {
				a, _ := json.Marshal(cursor)
				b, _ := json.Marshal(next)
				if string(a) == string(b) {
					return fmt.Errorf("unit traversal did not advance")
				}
			}
			cursor = next
		}
		if unitCount != summary.Units {
			return fmt.Errorf("source %s changed during graph preparation; retry", record.ID)
		}
		progress.InputOffset += n
		progress.Files++
		progress.Records += submitted
		progress.Seconds = priorSeconds + time.Since(started).Seconds()
		if err = atomicCorpusJSON(checkpoint, progress); err != nil {
			return err
		}
		if progress.Files%100 == 0 {
			fmt.Printf("Submitted %d graph units from %d sources\n", progress.Records, progress.Files)
		}
	}
	progress.Seconds = priorSeconds + time.Since(started).Seconds()
	if err = atomicCorpusJSON(checkpoint, progress); err != nil {
		return err
	}
	return printCorpusJSON(progress)
}
