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
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const corpusUnits = "document_units_v1"
const corpusChunks = "document_chunks_v1"
const corpusTextIndex = "document_text"

type corpusProgress struct {
	Binding      string  `json:"binding,omitempty"`
	InputOffset  int64   `json:"input_offset"`
	OutputOffset int64   `json:"output_offset,omitempty"`
	Files        int     `json:"files"`
	Pages        int     `json:"pages"`
	Audio        int     `json:"audio"`
	Records      int     `json:"records"`
	SourceBytes  int64   `json:"source_bytes"`
	Seconds      float64 `json:"seconds"`
	Complete     bool    `json:"complete"`
}

func corpusCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: grepstein corpus prepare|load|serve|graph|stats [flags]")
	}
	switch args[0] {
	case "prepare":
		return corpusPrepareCmd(args[1:])
	case "load":
		return corpusLoadCmd(args[1:])
	case "serve":
		return corpusServeCmd(args[1:])
	case "graph":
		return corpusGraphCmd(args[1:])
	case "stats":
		return corpusStatsCmd(args[1:])
	}
	return fmt.Errorf("unknown corpus command %q", args[0])
}
func atomicCorpusJSON(name string, value any) error {
	f, err := os.CreateTemp(filepath.Dir(name), ".checkpoint-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err = json.NewEncoder(f).Encode(value); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), name); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	err = dir.Sync()
	// Removable filesystems such as exFAT may not support directory fsync.
	// The checkpoint file itself was synced before its atomic rename.
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		return nil
	}
	return err
}
func readCorpusJSON(name string, value any) error {
	data, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}
func corpusLock(state string) (func(), error) {
	name := filepath.Join(state, ".lock")
	if err := os.Mkdir(name, 0700); err != nil {
		return nil, fmt.Errorf("state is locked (after a crashed process, remove %s): %w", name, err)
	}
	return func() { os.Remove(name) }, nil
}
func corpusPrepareCmd(args []string) error {
	flags := flag.NewFlagSet("corpus prepare", flag.ContinueOnError)
	state := flags.String("state", "./grepstein-corpus", "Manifest/checkpoint directory")
	base := flags.String("base-url", "http://localhost:3001", "Public source server URL reachable by Antfly and your browser")
	pages := flags.Int("pages-per-record", 25, "Maximum PDF pages per source record")
	limitPages := flags.Int("limit-pages", 0, "Pilot PDF page budget (0 means unlimited; applied before extraction)")
	limitFiles := flags.Int("limit-files", 0, "Pilot source file budget (0 means unlimited, includes audio)")
	resume := flags.Bool("resume", false, "Continue an existing preparation with its saved configuration")
	var sources StringSliceFlag
	flags.Var(&sources, "source", "Repeatable dataset=directory or dataset=archive.zip")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *pages < 1 || *limitPages < 0 || *limitFiles < 0 {
		return fmt.Errorf("page size must be positive; limits cannot be negative")
	}
	if err := os.MkdirAll(*state, 0700); err != nil {
		return err
	}
	unlock, err := corpusLock(*state)
	if err != nil {
		return err
	}
	defer unlock()
	cfgPath := filepath.Join(*state, "sources.json")
	var cfg corpusConfig
	if *resume {
		if len(sources) > 0 || flagChanged(flags, "base-url") || flagChanged(flags, "pages-per-record") || flagChanged(flags, "limit-pages") || flagChanged(flags, "limit-files") {
			return fmt.Errorf("resume uses saved source configuration; start a new state to change it")
		}
		if err = readCorpusJSON(cfgPath, &cfg); err != nil {
			return err
		}
	} else {
		if _, err = os.Stat(cfgPath); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("state already exists; use --resume or a new --state")
		}
		u, e := url.Parse(*base)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || u.User != nil || strings.Trim(u.Path, "/") != "" {
			return fmt.Errorf("base-url must be an HTTP(S) origin without credentials, path, or query")
		}
		cfg = corpusConfig{BaseURL: *base, PagesPerRecord: *pages, LimitPages: *limitPages, LimitFiles: *limitFiles}
		cfg.Key, err = newCorpusKey()
		if err != nil {
			return err
		}
		if len(sources) == 0 {
			return fmt.Errorf("at least one --source dataset=path is required")
		}
		for _, arg := range sources {
			label, p, ok := strings.Cut(arg, "=")
			if !ok || !datasetName.MatchString(label) {
				return fmt.Errorf("invalid --source %q; use dataset=path", arg)
			}
			p, e = filepath.Abs(p)
			if e != nil {
				return e
			}
			info, e := os.Stat(p)
			if e != nil {
				return e
			}
			isZip := !info.IsDir() && strings.EqualFold(filepath.Ext(p), ".zip")
			if !info.IsDir() && !isZip {
				return fmt.Errorf("source must be a directory or ZIP: %s", p)
			}
			cfg.Sources = append(cfg.Sources, corpusSource{Dataset: label, Path: p, Zip: isZip})
		}
		if err = atomicCorpusJSON(cfgPath, cfg); err != nil {
			return err
		}
	}
	archives := map[int]*archiveReceipt{}
	for i, source := range cfg.Sources {
		receipt, err := archiveProvenance(source)
		if err != nil {
			return err
		}
		if receipt != nil {
			archives[i] = receipt
		}
	}
	inventory := filepath.Join(*state, "inventory.ndjson")
	if _, err = os.Stat(inventory); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Discovering and deduplicating PDF/audio sources (no OCR or transcription yet)...")
		temp := inventory + ".tmp"
		if err = discoverCorpus(cfg, temp); err != nil {
			os.Remove(temp)
			return err
		}
		if err = os.Rename(temp, inventory); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	progressPath := filepath.Join(*state, "prepare.json")
	var progress corpusProgress
	if err = readCorpusJSON(progressPath, &progress); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if progress.Complete {
		return printCorpusJSON(progress)
	}
	input, err := os.Open(inventory)
	if err != nil {
		return err
	}
	defer input.Close()
	if _, err = input.Seek(progress.InputOffset, io.SeekStart); err != nil {
		return err
	}
	output, err := os.OpenFile(filepath.Join(*state, "records.ndjson"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer output.Close()
	info, err := output.Stat()
	if err != nil {
		return err
	}
	if info.Size() < progress.OutputOffset {
		return fmt.Errorf("manifest is shorter than its checkpoint")
	}
	if err = output.Truncate(progress.OutputOffset); err != nil {
		return err
	}
	if _, err = output.Seek(progress.OutputOffset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(input)
	store := newCorpusStore(cfg)
	store.pageCache = filepath.Join(*state, "page-windows")
	defer store.Close()
	started := time.Now()
	priorSeconds := progress.Seconds
	save := func() error {
		if err := output.Sync(); err != nil {
			return err
		}
		progress.Seconds = priorSeconds + time.Since(started).Seconds()
		return atomicCorpusJSON(progressPath, progress)
	}
	visited := 0
	for {
		if (cfg.LimitFiles > 0 && progress.Files >= cfg.LimitFiles) || (cfg.LimitPages > 0 && progress.Pages >= cfg.LimitPages) {
			progress.Complete = true
			break
		}
		var entry corpusEntry
		n, e := readCorpusLine(reader, &entry)
		if errors.Is(e, io.EOF) {
			progress.Complete = true
			break
		}
		if e != nil {
			return e
		}
		visited++
		totalPages := 0
		if entry.MIME == "application/pdf" {
			totalPages, e = store.pages(entry.Locator)
			if e == nil && totalPages < 1 {
				e = fmt.Errorf("PDF has no pages")
			}
			if e != nil {
				var ioFailure *os.PathError
				if errors.As(e, &ioFailure) {
					return fmt.Errorf("count pages %s: %w", entry.ID, e)
				}
				// An unreadable source (e.g. a corrupt archive entry) must not
				// abort a full-corpus preparation; record it and move on.
				if e = recordSkippedSource(filepath.Join(*state, "skipped.ndjson"), entry.ID, e); e != nil {
					return e
				}
				progress.InputOffset += n
				if visited%100 == 0 {
					if err = save(); err != nil {
						return err
					}
				}
				continue
			}
		} else if entry.Locator.Size > corpusMaxBytes {
			return fmt.Errorf("audio %s exceeds 128 MiB; split it before preparation", entry.ID)
		}
		selectedPages := totalPages
		if cfg.LimitPages > 0 {
			selectedPages = min(selectedPages, cfg.LimitPages-progress.Pages)
		}
		// Prepare every window before publishing any rows for this source.
		// A corrupt later page must not leave a half-published source manifest.
		if selectedPages > 0 {
			for first := 1; first <= selectedPages; first += cfg.PagesPerRecord {
				loc := entry.Locator
				loc.First, loc.Last = first, min(first+cfg.PagesPerRecord-1, selectedPages)
				if loc.First == 1 && loc.Last == totalPages {
					continue
				}
				if e = store.preparePageWindow(loc); e != nil {
					break
				}
			}
			if e != nil {
				var invalid *corpusPDFWindowError
				if !errors.As(e, &invalid) {
					return fmt.Errorf("prepare page windows %s: %w", entry.ID, e)
				}
				if e = recordSkippedSource(filepath.Join(*state, "skipped.ndjson"), entry.ID, e); e != nil {
					return e
				}
				progress.InputOffset += n
				if visited%100 == 0 {
					if err = save(); err != nil {
						return err
					}
				}
				continue
			}
		}
		emit := func(loc sourceLocator) error {
			versionBytes, _ := json.Marshal(loc)
			version := sha256.Sum256(versionBytes)
			original := entry.Locator
			doc := map[string]any{"title": path.Base(loc.Name), "filename": path.Base(loc.Name), "mime_type": entry.MIME, "version": hex.EncodeToString(version[:]), "url": sourceURL(cfg, loc), "original_url": sourceURL(cfg, original), "metadata": map[string]any{"dataset": entry.Dataset, "document_id": entry.ID, "source_locator": loc, "page_start": loc.First, "page_end": loc.Last, "original_pages": totalPages}}
			source := cfg.Sources[loc.Source]
			if receipt := archives[loc.Source]; receipt != nil {
				doc["metadata"].(map[string]any)["archive_source"] = receipt
			}
			if !source.Zip {
				var receipt downloadReceipt
				if readCorpusJSON(filepath.Join(source.Path, filepath.FromSlash(loc.Name))+".json", &receipt) == nil {
					upstream, err := url.Parse(receipt.URL)
					if err == nil && upstream.Scheme == "https" && upstream.Host == "www.justice.gov" && upstream.User == nil && upstream.RawQuery == "" && len(receipt.SHA256) == 64 {
						metadata := doc["metadata"].(map[string]any)
						metadata["upstream_url"] = receipt.URL
						metadata["upstream_sha256"] = receipt.SHA256
					}
				}
			}
			id := entry.ID
			if loc.First > 0 {
				id += fmt.Sprintf(":p%07d", loc.First)
				doc["original_url"] = sourceURL(cfg, original) + fmt.Sprintf("#page=%d", loc.First)
			}
			line, e := json.Marshal(corpusRecord{ID: id, Document: doc})
			if e != nil {
				return e
			}
			line = append(line, '\n')
			written, e := output.Write(line)
			if e != nil {
				return e
			}
			progress.OutputOffset += int64(written)
			progress.Records++
			return nil
		}
		if selectedPages > 0 {
			for first := 1; first <= selectedPages; first += cfg.PagesPerRecord {
				loc := entry.Locator
				loc.First = first
				loc.Last = min(first+cfg.PagesPerRecord-1, selectedPages)
				if e = emit(loc); e != nil {
					return e
				}
			}
			progress.Pages += selectedPages
		} else {
			if e = emit(entry.Locator); e != nil {
				return e
			}
			progress.Audio++
		}
		progress.InputOffset += n
		progress.Files++
		progress.SourceBytes += entry.Locator.Size
		if visited%100 == 0 {
			if err = save(); err != nil {
				return err
			}
		}
		if progress.Files%100 == 0 {
			fmt.Printf("Prepared %d sources, %d pages, %d audio, %d records\n", progress.Files, progress.Pages, progress.Audio, progress.Records)
		}
	}
	if err = save(); err != nil {
		return err
	}
	return printCorpusJSON(progress)
}
func flagChanged(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
func printCorpusJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func corpusIndexes(embeddingModel, extractorModel, inferenceURL, language string, graph, semantic bool, embedderOverride ...map[string]any) (map[string]any, error) {
	ocr := map[string]any{"enabled": false}
	if language != "" {
		data, err := os.ReadFile(language)
		if err != nil {
			return nil, err
		}
		var reader map[string]any
		if err = json.Unmarshal(data, &reader); err != nil {
			return nil, err
		}
		if reader["provider"] != "vertex" && reader["provider"] != "openai" && reader["provider"] != "antfly" && reader["provider"] != "apple" {
			return nil, fmt.Errorf("unsupported native OCR provider")
		}
		for _, key := range []string{"api_key", "bearer_token", "capability_token"} {
			if reader[key] != nil {
				return nil, fmt.Errorf("reader-config must reference server credentials, not contain %s", key)
			}
		}
		ocr = map[string]any{"enabled": true, "executor": "reader", "mode": "auto", "render_dpi": 150, "prompt_policy": "plain", "config": reader}
	}
	producer := map[string]any{"type": "document_extraction", "config": map[string]any{
		"source": map[string]any{"filename_field": "filename", "content_type_field": "mime_type", "version_field": "version"},
		"ocr":    ocr,
	}}
	producerJSON, err := json.Marshal(producer)
	if err != nil {
		return nil, err
	}
	indexes := map[string]any{corpusTextIndex: map[string]any{"type": "full_text", "field": "text", "artifact_name": corpusChunks, "enrichments": []any{
		map[string]any{"name": corpusUnits, "kind": "asset", "field": "url", "content_type": "application/json", "producer_json": string(producerJSON)},
		map[string]any{"name": corpusChunks, "kind": "chunk", "field": "text", "source_artifact_name": corpusUnits, "chunk_size": 512, "chunk_overlap": 50},
	}}}
	// The server owns and creates full_text_index_v0. Including its reserved
	// name in a public create request is rejected, even with default settings.
	if semantic {
		embedder, _ := corpusEmbedderConfig(embeddingModel, "")
		if len(embedderOverride) > 0 {
			embedder = embedderOverride[0]
		}
		if err := validateCorpusEmbedder(embedder); err != nil {
			return nil, err
		}
		dimensions := corpusEmbeddingDimensions(embedder)
		indexes[DefaultEmbeddingIndex] = map[string]any{"type": "embeddings", "field": "embedding", "dimension": dimensions, "distance_metric": "cosine", "embedding_name": "document_dense_v1", "source_artifact_name": corpusChunks, "embedder": embedder, "enrichments": []any{map[string]any{"name": "document_dense_v1", "kind": "embedding", "field": "text", "source_artifact_name": corpusChunks, "expected_dims": dimensions}}}
		// Graph units and revision markers intentionally have no document
		// extraction input. Settle those skips, while keeping provider failures
		// and unfinished extraction visible as incomplete coverage.
		indexes[DefaultEmbeddingIndex].(map[string]any)["coverage_policy"] = "partial"
	}
	if graph {
		index, e := createArtifactGraphIndex(DefaultAutographIndex, DefaultAutographAsset, "extractor", extractorModel, inferenceURL, strings.Split(DefaultEntityLabels, ","), strings.Split(DefaultRelationLabels, ","))
		if e != nil {
			return nil, e
		}
		indexes[DefaultAutographIndex] = index
	}
	return indexes, nil
}

func corpusRequest(client *http.Client, method, endpoint string, body any) (json.RawMessage, error) {
	data, err := corpusRawRequest(client, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid JSON response from %s", endpoint)
	}
	return data, nil
}

type corpusHTTPError struct {
	Method, Path string
	StatusCode   int
	Body         []byte
	RetryAfter   time.Duration
}

func (e *corpusHTTPError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %.4096s", e.Method, e.Path, e.StatusCode, e.Body)
}

func corpusRawRequest(client *http.Client, method, endpoint string, body any) ([]byte, error) {
	return corpusRawRequestWithHeaders(client, method, endpoint, body, nil)
}
func corpusRawRequestWithHeaders(client *http.Client, method, endpoint string, body any, headers *http.Header) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := os.Getenv("ANTFLY_API_KEY"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if headers != nil {
		*headers = resp.Header.Clone()
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var retryAfter time.Duration
		if seconds, e := time.ParseDuration(resp.Header.Get("Retry-After") + "s"); e == nil {
			retryAfter = seconds
		} else if deadline, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil {
			retryAfter = time.Until(deadline)
		}
		return nil, &corpusHTTPError{Method: method, Path: req.URL.Path, StatusCode: resp.StatusCode, Body: data, RetryAfter: retryAfter}
	}
	if len(data) > 16<<20 {
		return nil, fmt.Errorf("response from %s exceeds 16 MiB", req.URL.Path)
	}
	return data, nil
}
func corpusLoadCmd(args []string) error {
	flags := flag.NewFlagSet("corpus load", flag.ContinueOnError)
	state := flags.String("state", "./grepstein-corpus", "Prepared corpus state directory")
	endpoint := flags.String("url", "http://localhost:8080/db/v1", "Antfly API URL")
	table := flags.String("table", "grepstein_corpus", "Corpus table name")
	create := flags.Bool("create-table", false, "Create artifact-backed OCR/transcription/search indexes")
	createOnly := flags.Bool("create-only", false, "Create the table without loading, for a storage baseline (requires --create-table)")
	batchSize := flags.Int("batch-size", 100, "Records per durable upsert (native workers manage OCR/transcription admission)")
	semantic := flags.Bool("semantic", false, "Create semantic index (requires embedding inference)")
	graph := flags.Bool("graph", false, "Create relation graph; run corpus graph after extraction (requires extractor inference)")
	inference := flags.String("inference-url", "", "Antfly inference URL for the optional relation graph")
	model := flags.String("embedding-model", DefaultEmbeddingModel, "Embedding model, with 512 dimensions")
	embedderPath := flags.String("embedder-config", "", "Native embedder JSON including provider, model, project/location and dimension")
	extractorModel := flags.String("artifact-extractor-model", DefaultAutographModel, "Registered inference model for relation extraction")
	language := flags.String("reader-config", "", "Native OCR reader JSON file (Vertex ADC recommended); empty disables OCR")
	numShards := flags.Int("num-shards", 1, "Number of shards when creating the table")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *createOnly && !*create {
		return fmt.Errorf("--create-only requires --create-table")
	}
	if *batchSize < 1 || *batchSize > 1000 {
		return fmt.Errorf("batch-size must be between 1 and 1000")
	}
	unlock, err := corpusLock(*state)
	if err != nil {
		return err
	}
	defer unlock()
	var prepared corpusProgress
	if err = readCorpusJSON(filepath.Join(*state, "prepare.json"), &prepared); err != nil {
		return err
	}
	if !prepared.Complete {
		return fmt.Errorf("preparation is incomplete; run corpus prepare --resume")
	}
	if err = validatePublication(*state); err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	tableURL := strings.TrimRight(*endpoint, "/") + "/tables/" + url.PathEscape(*table)
	var sourceConfig corpusConfig
	if err = readCorpusJSON(filepath.Join(*state, "sources.json"), &sourceConfig); err != nil {
		return err
	}
	embedder, err := corpusEmbedderConfig(*model, *embedderPath)
	if err != nil {
		return err
	}
	indexes, err := corpusIndexes(*model, *extractorModel, *inference, *language, *graph, *semantic, embedder)
	if err != nil {
		return err
	}
	description := corpusDescription(indexes, sourceConfig.PagesPerRecord)
	if *create {
		if _, err = corpusRequest(client, http.MethodPost, tableURL, map[string]any{"num_shards": *numShards, "indexes": indexes, "schema": corpusTableSchema(), "description": description}); err != nil {
			return err
		}
	}
	if *createOnly {
		return nil
	}
	status, err := corpusRequest(client, http.MethodGet, tableURL, nil)
	if err != nil {
		return err
	}
	var identity struct {
		TableID     string `json:"table_id"`
		Description string `json:"description"`
	}
	if err = json.Unmarshal(status, &identity); err != nil {
		return err
	}
	if identity.Description != description {
		return fmt.Errorf("table configuration differs from this corpus (page window or provider/index settings); use matching flags or a new table")
	}
	if identity.TableID == "" {
		return fmt.Errorf("server must expose table_id to safely resume loading")
	}
	input, err := os.Open(filepath.Join(*state, "published.ndjson"))
	if err != nil {
		return err
	}
	defer input.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, input); err != nil {
		return err
	}
	bindingJSON, _ := json.Marshal([]any{*endpoint, *table, identity.TableID, description, hex.EncodeToString(digest.Sum(nil))})
	binding := sha256.Sum256(bindingJSON)
	checkpoint := filepath.Join(*state, "load.json")
	var progress corpusProgress
	if err = readCorpusJSON(checkpoint, &progress); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	expected := hex.EncodeToString(binding[:])
	if progress.Binding != "" && progress.Binding != expected {
		return fmt.Errorf("load checkpoint belongs to another manifest, table, server or configuration; use a separate state")
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
		inserts := map[string]map[string]any{}
		var offset int64
		count := 0
		done := false
		for count < *batchSize {
			var record corpusRecord
			n, e := readCorpusLine(reader, &record)
			if errors.Is(e, io.EOF) {
				done = true
				break
			}
			if e != nil {
				return fmt.Errorf("manifest at byte %d: %w", progress.InputOffset+offset, e)
			}
			if record.ID == "" || record.Document == nil {
				return fmt.Errorf("invalid manifest record")
			}
			if _, exists := inserts[record.ID]; exists {
				return fmt.Errorf("duplicate manifest ID %q", record.ID)
			}
			inserts[record.ID] = record.Document
			offset += n
			count++
		}
		if count > 0 {
			response, e := corpusRequest(client, http.MethodPost, tableURL+"/batch", map[string]any{"inserts": inserts, "sync_level": "write"})
			if e != nil {
				return e
			}
			var result map[string]any
			if e = json.Unmarshal(response, &result); e != nil {
				return e
			}
			if result["status"] == "committed_repair_required" {
				return fmt.Errorf("batch committed but requires repair: %s", response)
			}
			progress.InputOffset += offset
			progress.Records += count
		}
		progress.Complete = done
		progress.Seconds = priorSeconds + time.Since(started).Seconds()
		if err = atomicCorpusJSON(checkpoint, progress); err != nil {
			return err
		}
		if done {
			break
		}
		if progress.Records%100 == 0 {
			fmt.Printf("Durably submitted %d records (enrichment continues on the server)\n", progress.Records)
		}
	}
	return printCorpusJSON(progress)
}
func corpusServeCmd(args []string) error {
	flags := flag.NewFlagSet("corpus serve", flag.ContinueOnError)
	state := flags.String("state", "./grepstein-corpus", "Prepared corpus state directory")
	listen := flags.String("listen", ":3001", "Source server listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var cfg corpusConfig
	if err := readCorpusJSON(filepath.Join(*state, "sources.json"), &cfg); err != nil {
		return err
	}
	store := newCorpusStore(cfg)
	store.pageCache = filepath.Join(*state, "page-windows")
	defer store.Close()
	// Upgrade old manifests before accepting requests. Parsing and splitting
	// giant PDFs belongs to preparation, outside the consumer's fetch deadline.
	if err := warmCorpusPageWindows(store, filepath.Join(*state, "records.ndjson")); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/sources/", store)
	fmt.Printf("Serving signed PDF/audio sources on %s; keep running during enrichment and browsing\n", *listen)
	return (&http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}).ListenAndServe()
}

func warmCorpusPageWindows(store *corpusStore, manifest string) error {
	input, err := os.Open(manifest)
	if err != nil {
		return err
	}
	defer input.Close()
	reader := bufio.NewReader(input)
	for {
		var record corpusRecord
		if _, err := readCorpusLine(reader, &record); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		metadata, ok := record.Document["metadata"].(map[string]any)
		if !ok {
			return fmt.Errorf("missing source metadata for %s", record.ID)
		}
		encoded, err := json.Marshal(metadata["source_locator"])
		if err != nil {
			return err
		}
		var loc sourceLocator
		if err := json.Unmarshal(encoded, &loc); err != nil {
			return err
		}
		total, _ := metadata["original_pages"].(float64)
		if loc.First > 0 && (loc.First != 1 || loc.Last != int(total)) {
			if err := store.preparePageWindow(loc); err != nil {
				var invalid *corpusPDFWindowError
				if errors.As(err, &invalid) {
					fmt.Fprintf(os.Stderr, "Skipping unreadable page window %s: %v\n", record.ID, err)
					continue
				}
				return fmt.Errorf("prepare page window %s: %w", record.ID, err)
			}
		}
	}
}
func corpusStatsCmd(args []string) error {
	flags := flag.NewFlagSet("corpus stats", flag.ContinueOnError)
	state := flags.String("state", "./grepstein-corpus", "Prepared corpus state directory")
	endpoint := flags.String("url", "http://localhost:8080/db/v1", "Antfly API URL")
	table := flags.String("table", "grepstein_corpus", "Corpus table name")
	dataDir := flags.String("data-dir", "", "Local Antfly storage directory to measure allocated bytes")
	output := flags.String("output", "", "Save a metrics snapshot")
	baseline := flags.String("baseline", "", "Prior metrics snapshot for allocated-byte delta")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var prepared, loaded corpusProgress
	if err := readCorpusJSON(filepath.Join(*state, "prepare.json"), &prepared); err != nil {
		return err
	}
	if err := readCorpusJSON(filepath.Join(*state, "load.json"), &loaded); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	status, err := corpusRequest(&http.Client{Timeout: time.Minute}, http.MethodGet, strings.TrimRight(*endpoint, "/")+"/tables/"+url.PathEscape(*table), nil)
	if err != nil {
		return err
	}
	var tableStatus struct {
		Indexes map[string]any `json:"indexes"`
	}
	if err = json.Unmarshal(status, &tableStatus); err != nil {
		return err
	}
	indexMetrics := map[string]json.RawMessage{}
	for name := range tableStatus.Indexes {
		value, e := corpusRequest(&http.Client{Timeout: time.Minute}, "GET", strings.TrimRight(*endpoint, "/")+"/tables/"+url.PathEscape(*table)+"/indexes/"+url.PathEscape(name), nil)
		if e != nil {
			return e
		}
		indexMetrics[name] = value
	}
	metrics := map[string]any{"indexes": indexMetrics, "at": time.Now().UTC().Format(time.RFC3339), "prepare": prepared, "load": loaded, "table": json.RawMessage(status), "note": "Load seconds measure submission, not OCR/index completion. Measure storage after artifact/index queues settle. Source bytes include whole selected files, even partial PDF pilots."}
	if prepared.Seconds > 0 {
		metrics["prepare_sources_per_second"] = float64(prepared.Files) / prepared.Seconds
	}
	if loaded.Seconds > 0 {
		metrics["submitted_records_per_second"] = float64(loaded.Records) / loaded.Seconds
	}
	if *dataDir != "" {
		absolute, e := filepath.Abs(*dataDir)
		if e != nil {
			return e
		}
		data, e := exec.Command("du", "-sk", absolute).Output()
		if e != nil {
			return e
		}
		fields := strings.Fields(string(data))
		if len(fields) == 0 {
			return fmt.Errorf("empty du output")
		}
		kb, e := strconv.ParseInt(fields[0], 10, 64)
		if e != nil {
			return e
		}
		allocated := kb * 1024
		metrics["data_dir"] = absolute
		metrics["allocated_bytes"] = allocated
		if *baseline != "" {
			var old struct {
				DataDir   string `json:"data_dir"`
				Allocated int64  `json:"allocated_bytes"`
			}
			if e = readCorpusJSON(*baseline, &old); e != nil {
				return e
			}
			if old.DataDir != absolute {
				return fmt.Errorf("baseline data directory differs")
			}
			metrics["allocated_delta_bytes"] = allocated - old.Allocated
		}
	} else if *baseline != "" {
		return fmt.Errorf("--baseline requires --data-dir")
	}
	if *output != "" {
		if err = atomicCorpusJSON(*output, metrics); err != nil {
			return err
		}
	}
	return printCorpusJSON(metrics)
}

// Keep locators, signed URLs and provider version metadata out of postings.
// Graph unit content is indexed by the ordinary row index for graph seed search.
func corpusTableSchema() map[string]any {
	properties := map[string]any{}
	for _, name := range []string{"url", "original_url", "filename", "mime_type", "version", "metadata"} {
		properties[name] = map[string]any{"x-antfly-index": false}
	}
	properties["title"] = map[string]any{"type": "string"}
	properties["content"] = map[string]any{"type": "string"}
	// Use the public schema mapping contract for lexical artifact text. Named
	// artifact indexes own their routing; routing the same chunks into the
	// ordinary full-text index as well is unnecessary. The server-created
	// ordinary index remains available for graph-unit content.
	properties["text"] = map[string]any{"type": "string", "x-antfly-field": map[string]any{"type": "text", "analyzer": "simple"}}
	return map[string]any{"default_type": "doc", "document_schemas": map[string]any{"doc": map[string]any{"schema": map[string]any{"type": "object", "additionalProperties": true, "properties": properties}}}}
}

func corpusDescription(indexes map[string]any, pagesPerRecord int) string {
	encoded, _ := json.Marshal([]any{indexes[corpusTextIndex], pagesPerRecord})
	sum := sha256.Sum256(encoded)
	return "grepstein-corpus-v1:" + hex.EncodeToString(sum[:])
}

func recordSkippedSource(path, id string, cause error) error {
	fmt.Printf("Skipping unreadable source %s: %v\n", id, cause)
	line, err := json.Marshal(map[string]string{"id": id, "error": cause.Error()})
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(line, '\n'))
	return err
}
