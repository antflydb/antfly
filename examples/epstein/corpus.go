// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
		return fmt.Errorf("usage: epstein corpus prepare|load|serve|graph|stats [flags]")
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
	state := flags.String("state", "./epstein-corpus", "Manifest/checkpoint directory")
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
		totalPages := 0
		if entry.MIME == "application/pdf" {
			totalPages, e = store.pages(entry.Locator)
			if e != nil {
				return fmt.Errorf("count pages %s: %w", entry.ID, e)
			}
			if totalPages < 1 {
				return fmt.Errorf("PDF %s has no pages", entry.ID)
			}
		} else if entry.Locator.Size > corpusMaxBytes {
			return fmt.Errorf("audio %s exceeds 128 MiB; split it before preparation", entry.ID)
		}
		selectedPages := totalPages
		if cfg.LimitPages > 0 {
			selectedPages = min(selectedPages, cfg.LimitPages-progress.Pages)
		}
		emit := func(loc sourceLocator) error {
			versionBytes, _ := json.Marshal(loc)
			version := sha256.Sum256(versionBytes)
			original := entry.Locator
			doc := map[string]any{"title": path.Base(loc.Name), "filename": path.Base(loc.Name), "mime_type": entry.MIME, "version": hex.EncodeToString(version[:]), "url": sourceURL(cfg, loc), "original_url": sourceURL(cfg, original), "metadata": map[string]any{"dataset": entry.Dataset, "document_id": entry.ID, "source_locator": loc, "page_start": loc.First, "page_end": loc.Last, "original_pages": totalPages}}
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
		if progress.Files%100 == 0 {
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

func corpusIndexes(embeddingModel, inferenceURL, language string, graph, semantic bool) (map[string]any, error) {
	producer := map[string]any{"type": "document_extraction", "config": map[string]any{
		"source":        map[string]any{"filename_field": "filename", "content_type_field": "mime_type", "version_field": "version"},
		"ocr":           map[string]any{"enabled": true, "executor": "reader", "mode": "auto", "render_dpi": 150, "prompt_policy": "plain", "config": map[string]any{"provider": "apple", "recognition_languages": []string{language}, "recognition_level": "accurate", "uses_language_correction": false}},
		"transcription": map[string]any{"enabled": true, "config": map[string]any{"provider": "apple", "language_code": language, "timestamps": true, "download_assets": false}},
	}}
	producerJSON, err := json.Marshal(producer)
	if err != nil {
		return nil, err
	}
	indexes := map[string]any{corpusTextIndex: map[string]any{"type": "full_text", "field": "text", "artifact_name": corpusChunks, "enrichments": []any{
		map[string]any{"name": corpusUnits, "kind": "asset", "field": "url", "content_type": "application/json", "producer_json": string(producerJSON)},
		map[string]any{"name": corpusChunks, "kind": "chunk", "field": "text", "source_artifact_name": corpusUnits, "chunk_size": 512, "chunk_overlap": 50, "full_text_index": true},
	}}}
	apiURL, err := inferenceMLBaseURL(inferenceURL)
	if err != nil {
		return nil, err
	}
	if semantic {
		indexes[DefaultEmbeddingIndex] = map[string]any{"type": "embeddings", "field": "embedding", "dimension": DefaultEmbeddingDims, "distance_metric": "cosine", "embedding_name": "document_dense_v1", "source_artifact_name": corpusChunks, "embedder": map[string]any{"provider": "antfly", "model": embeddingModel, "api_url": apiURL}, "enrichments": []any{map[string]any{"name": "document_dense_v1", "kind": "embedding", "field": "text", "source_artifact_name": corpusChunks, "expected_dims": DefaultEmbeddingDims}}}
	}
	if graph {
		index, e := createArtifactGraphIndex(DefaultAutographIndex, DefaultAutographAsset, "extractor", DefaultAutographModel, inferenceURL, strings.Split(DefaultEntityLabels, ","), strings.Split(DefaultRelationLabels, ","))
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
		return nil, &corpusHTTPError{Method: method, Path: req.URL.Path, StatusCode: resp.StatusCode, Body: data}
	}
	if len(data) > 16<<20 {
		return nil, fmt.Errorf("response from %s exceeds 16 MiB", req.URL.Path)
	}
	return data, nil
}
func corpusLoadCmd(args []string) error {
	flags := flag.NewFlagSet("corpus load", flag.ContinueOnError)
	state := flags.String("state", "./epstein-corpus", "Prepared corpus state directory")
	endpoint := flags.String("url", "http://localhost:8080/db/v1", "Antfly API URL")
	table := flags.String("table", "epstein_corpus", "Corpus table name")
	create := flags.Bool("create-table", false, "Create artifact-backed OCR/transcription/search indexes")
	createOnly := flags.Bool("create-only", false, "Create the table without loading, for a storage baseline (requires --create-table)")
	batchSize := flags.Int("batch-size", 100, "Records per durable upsert (native workers manage OCR/transcription admission)")
	semantic := flags.Bool("semantic", true, "Create semantic index (requires embedding inference)")
	graph := flags.Bool("graph", false, "Create relation graph; run corpus graph after extraction (requires extractor inference)")
	inference := flags.String("inference-url", "http://localhost:8080", "Antfly inference URL")
	model := flags.String("embedding-model", DefaultEmbeddingModel, "Embedding model, with 512 dimensions")
	language := flags.String("language", "en-US", "Apple OCR and transcription locale")
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
	client := &http.Client{Timeout: 10 * time.Minute}
	tableURL := strings.TrimRight(*endpoint, "/") + "/tables/" + url.PathEscape(*table)
	var sourceConfig corpusConfig
	if err = readCorpusJSON(filepath.Join(*state, "sources.json"), &sourceConfig); err != nil {
		return err
	}
	indexes, err := corpusIndexes(*model, *inference, *language, *graph, *semantic)
	if err != nil {
		return err
	}
	description := corpusDescription(indexes, sourceConfig.PagesPerRecord)
	if *create {
		if _, err = corpusRequest(client, http.MethodPost, tableURL, map[string]any{"num_shards": 1, "indexes": indexes, "schema": corpusTableSchema(), "description": description}); err != nil {
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
	input, err := os.Open(filepath.Join(*state, "records.ndjson"))
	if err != nil {
		return err
	}
	defer input.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, input); err != nil {
		return err
	}
	bindingJSON, _ := json.Marshal([]any{*endpoint, *table, identity.TableID, indexes, hex.EncodeToString(digest.Sum(nil))})
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
	state := flags.String("state", "./epstein-corpus", "Prepared corpus state directory")
	listen := flags.String("listen", ":3001", "Source server listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var cfg corpusConfig
	if err := readCorpusJSON(filepath.Join(*state, "sources.json"), &cfg); err != nil {
		return err
	}
	store := newCorpusStore(cfg)
	defer store.Close()
	mux := http.NewServeMux()
	mux.Handle("/sources/", store)
	fmt.Printf("Serving signed PDF/audio sources on %s; keep running during enrichment and browsing\n", *listen)
	return (&http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}).ListenAndServe()
}
func corpusStatsCmd(args []string) error {
	flags := flag.NewFlagSet("corpus stats", flag.ContinueOnError)
	state := flags.String("state", "./epstein-corpus", "Prepared corpus state directory")
	endpoint := flags.String("url", "http://localhost:8080/db/v1", "Antfly API URL")
	table := flags.String("table", "epstein_corpus", "Corpus table name")
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
	return map[string]any{"default_type": "doc", "document_schemas": map[string]any{"doc": map[string]any{"schema": map[string]any{"type": "object", "additionalProperties": true, "properties": properties}}}}
}

func corpusDescription(indexes map[string]any, pagesPerRecord int) string {
	encoded, _ := json.Marshal([]any{indexes, pagesPerRecord})
	sum := sha256.Sum256(encoded)
	return "epstein-corpus-v1:" + hex.EncodeToString(sum[:])
}
