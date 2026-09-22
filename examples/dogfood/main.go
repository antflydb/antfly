// Copyright 2026 Antfly, Inc.
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

// Command dogfood ingests Antfly's own design docs and work log into an
// embedded Antfly Lite database and builds a small knowledge graph over them:
// Qwen3 embeddings over fixed-size chunks for semantic search, and GLiNER2.5
// entity/relation extraction feeding a graph index (the "autograph" pattern).
//
// It dogfoods the docs cleanup itself: design docs (zig/*.md, zig/pkg/**/*.md,
// zig/lib/**/*.md, docs/design/**) and work-log entries (work-log/**/*.md) are
// ingested as distinct document kinds, so `dogfood query` and `dogfood entity`
// can be used to sanity-check that the split reads sensibly end to end.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/antflylite"
)

const (
	// defaultInferenceURL is antfly inference run's default listen address
	// (--host 127.0.0.1 --port 8090; see zig/pkg/inference/src/main.zig).
	defaultInferenceURL = "" // empty: in-process inference via libantfly
	defaultEmbedModel   = "Qwen/Qwen3-Embedding-0.6B-GGUF"
	// GLiNER2.5 base is qualified for production extraction, including
	// windowed long documents (zig/pkg/inference/models/gliner2/GLINER25.md).
	defaultExtractModel  = "fastino/gliner2.5-base-v1"
	defaultTargetTokens  = 400
	defaultOverlapTokens = 40

	// healthProbeTimeout bounds how long dogfood waits for the inference
	// server's health check before treating it as unreachable.
	healthProbeTimeout = 3 * time.Second
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "ingest":
		err = runIngestCmd(os.Args[2:])
	case "query":
		err = runQueryCmd(os.Args[2:])
	case "entity":
		err = runEntityCmd(os.Args[2:])
	case "status":
		err = runStatusCmd(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `dogfood: embedded Antfly Lite knowledge graph over Antfly's own docs

Usage:
  dogfood ingest [-db dogfood.aflite] [-repo ../..] [-reset] [flags...]
  dogfood query "<text>" [-db dogfood.aflite]
  dogfood entity "<name>" [-db dogfood.aflite]
  dogfood status [-db dogfood.aflite]`)
}

func runIngestCmd(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	dbPath := fs.String("db", "dogfood.aflite", "Antfly Lite database path")
	repoRoot := fs.String("repo", "../..", "repository root to ingest design docs and work-log from")
	reset := fs.Bool("reset", false, "remove the existing Lite database before ingesting")
	inferenceURL := fs.String("inference-url", defaultInferenceURL, "optional remote antfly inference server (e.g. http://127.0.0.1:8090); empty runs inference in-process")
	embedModel := fs.String("embed-model", defaultEmbedModel, "Antfly inference embedding model for chunk_vectors")
	extractModel := fs.String("extract-model", defaultExtractModel, "Antfly inference extraction model for the knowledge graph")
	targetTokens := fs.Int("target-tokens", defaultTargetTokens, "fixed chunker target tokens per chunk")
	overlapTokens := fs.Int("overlap-tokens", defaultOverlapTokens, "fixed chunker overlap tokens between chunks")
	metrics := fs.Bool("metrics", true, "publish a pagerank metric on the knowledge graph index")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *reset {
		if err := os.Remove(*dbPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", *dbPath, err)
		}
	}

	db, err := openOrCreateLite(*dbPath, *inferenceURL)
	if err != nil {
		return fmt.Errorf("open or create %s: %w", *dbPath, err)
	}
	defer db.Close()

	if err := requireInferenceProvider(db, *inferenceURL, *extractModel); err != nil {
		return err
	}

	cfg := indexBuildConfig{
		InferenceURL:  *inferenceURL,
		EmbedModel:    *embedModel,
		ExtractModel:  *extractModel,
		TargetTokens:  *targetTokens,
		OverlapTokens: *overlapTokens,
		Metrics:       *metrics,
	}
	if err := ensureSchemaAndIndexes(db, cfg); err != nil {
		return err
	}

	fmt.Printf("Walking corpus under %s...\n", *repoRoot)
	sections, err := collectDocSections(context.Background(), *repoRoot)
	if err != nil {
		return fmt.Errorf("collect doc sections: %w", err)
	}
	fmt.Printf("Found %d document sections\n", len(sections))

	written, err := ingestSections(db, sections)
	if err != nil {
		return fmt.Errorf("ingest sections (%d written before failure): %w", written, err)
	}

	pending, err := db.PendingWorkStats()
	if err != nil {
		return fmt.Errorf("read pending work stats: %w", err)
	}
	fmt.Printf("Pending work before drain: has_async_indexes=%t derived_target_sequence=%d\n",
		pending.HasAsyncIndexes, pending.DerivedTargetSequence)

	fmt.Println("Draining enrichment and index work (this runs the embedder and extractor)...")
	drained, err := db.RunUntilIdleStatus()
	if err != nil {
		return fmt.Errorf("run until idle: %w", err)
	}
	fmt.Printf("Drained. has_async_indexes=%t derived_target_sequence=%d\n",
		drained.HasAsyncIndexes, drained.DerivedTargetSequence)
	if len(drained.TextMerge) > 0 {
		fmt.Printf("text_merge: %s\n", drained.TextMerge)
	}
	fmt.Printf("Done. Wrote %d documents to %s\n", written, *dbPath)
	return nil
}

func runQueryCmd(args []string) error {
	flagArgs, positional := reorderFlags(args, map[string]bool{"db": true, "limit": true, "inference-url": true})
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	dbPath := fs.String("db", "dogfood.aflite", "Antfly Lite database path")
	limit := fs.Int("limit", 8, "number of hybrid search hits to return")
	inferenceURL := fs.String("inference-url", defaultInferenceURL, "remote antfly inference server (must match ingest; empty: in-process)")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if len(positional) < 1 {
		return fmt.Errorf("usage: dogfood query \"<text>\" [-db dogfood.aflite]")
	}
	text := positional[0]

	db, err := openExistingLite(*dbPath, *inferenceURL)
	if err != nil {
		return fmt.Errorf("open %s: %w", *dbPath, err)
	}
	defer db.Close()

	return runQuery(db, text, *limit)
}

func runEntityCmd(args []string) error {
	flagArgs, positional := reorderFlags(args, map[string]bool{"db": true, "inference-url": true})
	fs := flag.NewFlagSet("entity", flag.ExitOnError)
	dbPath := fs.String("db", "dogfood.aflite", "Antfly Lite database path")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if len(positional) < 1 {
		return fmt.Errorf("usage: dogfood entity \"<name>\" [-db dogfood.aflite]")
	}
	name := positional[0]

	db, err := openExistingLite(*dbPath, "")
	if err != nil {
		return fmt.Errorf("open %s: %w", *dbPath, err)
	}
	defer db.Close()

	return runEntity(db, name)
}

// reorderFlags moves recognized "-name value" and "-name=value" flag tokens
// (named in valueFlags, all of which take a value) to the front of args,
// preserving their relative order, and returns the remaining tokens as
// positional arguments. The standard flag package stops recognizing flags at
// the first positional token, which would otherwise make
// `dogfood query "<text>" -db custom.aflite` silently ignore -db.
func reorderFlags(args []string, valueFlags map[string]bool) (flagArgs, positional []string) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		name, _, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if !valueFlags[name] {
			positional = append(positional, arg)
			continue
		}
		flagArgs = append(flagArgs, arg)
		if !hasValue && i+1 < len(args) {
			i++
			flagArgs = append(flagArgs, args[i])
		}
	}
	return flagArgs, positional
}

func runStatusCmd(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dbPath := fs.String("db", "dogfood.aflite", "Antfly Lite database path")
	inferenceURL := fs.String("inference-url", defaultInferenceURL, "remote antfly inference server to probe (empty: in-process runtime)")
	extractModel := fs.String("extract-model", defaultExtractModel, "Antfly inference extraction model to probe")
	if err := fs.Parse(args); err != nil {
		return err
	}

	db, err := openExistingLite(*dbPath, *inferenceURL)
	if err != nil {
		return fmt.Errorf("open %s: %w", *dbPath, err)
	}
	defer db.Close()

	status, err := db.Status()
	if err != nil {
		return fmt.Errorf("read status: %w", err)
	}
	fmt.Printf("storage: format=%s engine=%s\n", status.Storage.Format, status.Storage.Engine)
	fmt.Printf("inference: mode=%s configured=%t remote_provider_configured=%t\n",
		status.Inference.Mode, status.Inference.Configured, status.Inference.RemoteProviderConfigured)

	if *inferenceURL == "" {
		caps, err := db.Capabilities()
		if err != nil {
			return fmt.Errorf("read capabilities: %w", err)
		}
		fmt.Printf("inference provider: in-process (local_inference_runtime=%t)\n", caps.LocalInferenceRuntime)
	} else {
		fmt.Printf("inference provider: %s\n", *inferenceURL)
	}
	if *inferenceURL == "" {
		// nothing to probe over HTTP
	} else if err := probeInferenceHealth(*inferenceURL); err != nil {
		fmt.Printf("inference provider health: unreachable (%v)\n", err)
	} else {
		fmt.Printf("inference provider health: ok\n")
	}
	if *inferenceURL != "" {
		if err := probeExtractor(*inferenceURL, *extractModel); err != nil {
			fmt.Printf("extraction model %q: unavailable (%v)\n", *extractModel, err)
		} else {
			fmt.Printf("extraction model %q: ok\n", *extractModel)
		}
	}

	indexes, err := db.IndexesJSON()
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}
	fmt.Printf("indexes: %s\n", indexes)

	enrichments, err := db.EnrichmentsJSON()
	if err != nil {
		return fmt.Errorf("list enrichments: %w", err)
	}
	fmt.Printf("enrichments: %s\n", enrichments)

	pending, err := db.PendingWorkStats()
	if err != nil {
		return fmt.Errorf("pending work stats: %w", err)
	}
	fmt.Printf("pending work: has_async_indexes=%t derived_target_sequence=%d\n",
		pending.HasAsyncIndexes, pending.DerivedTargetSequence)
	return nil
}

// openOrCreateLite opens an existing native Lite database at path, or creates
// one configured for Lite's "remote inference provider" mode (zig/LITE.md
// "Remote Inference Provider"). libantfly does not embed the inference
// runtime -- the capi link anchor resolves it from an executable consumer,
// and the local-inference-runtime build flag only advertises the capability
// -- so dogfood cannot use LocalRuntimeConfigured from Go. Instead it points
// every embedder/chunker/extractor producer config at a separately running
// `antfly inference run` server and sets RemoteProviderConfigured so
// Status().Inference.Mode reports "remote_provider" rather than the default
// caller-supplied/deferred mode.
func liteOpenOptions(inferenceURL string) antflylite.OpenOptions {
	opts := antflylite.OpenOptions{
		Mode:    antflylite.OpenModeWriter,
		Profile: antflylite.ProfileNative,
	}
	if inferenceURL == "" {
		// libantfly links the standalone inference runtime, so a Lite handle
		// opened with LocalRuntimeConfigured runs chunker, embedder, and
		// extractor producers in-process ("local_embedded").
		opts.LocalRuntimeConfigured = true
	} else {
		// Remote-provider mode: producers carry api_url and Lite calls the
		// external `antfly inference run` server.
		opts.RemoteProviderConfigured = true
	}
	return opts
}

func openOrCreateLite(path, inferenceURL string) (*antflylite.DB, error) {
	opts := liteOpenOptions(inferenceURL)
	if _, err := os.Stat(path); err == nil {
		return antflylite.OpenWithOptions(path, opts)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return antflylite.CreateWithOptions(path, opts)
}

func openExistingLite(path, inferenceURL string) (*antflylite.DB, error) {
	return antflylite.OpenWithOptions(path, liteOpenOptions(inferenceURL))
}

// requireInferenceProvider fails fast with a clear message when the
// configured antfly inference server is not reachable, or when the
// configured extraction model is not servable (e.g. GLiNER2.5 is gated off
// pending a production qualification row -- see extractSmokeInput's doc
// comment). Without this check, ingest would silently accumulate enrichment
// debt with no producer able to satisfy it, or fail deep inside RunUntilIdle
// with a much less clear error.
func requireInferenceProvider(db *antflylite.DB, inferenceURL, extractModel string) error {
	if inferenceURL == "" {
		caps, err := db.Capabilities()
		if err != nil {
			return fmt.Errorf("read capabilities: %w", err)
		}
		if !caps.LocalInferenceRuntime {
			return fmt.Errorf(
				"the linked libantfly does not advertise an embedded inference runtime; " +
					"rebuild it with `zig build capi` from a current tree, or pass -inference-url " +
					"to use an external `antfly inference run` server")
		}
		return nil
	}
	if err := probeInferenceHealth(inferenceURL); err != nil {
		return fmt.Errorf(
			"antfly inference server at %s is not reachable: %w\n"+
				"Start it in another terminal and pull the required models first, e.g.:\n"+
				"  antfly inference run\n"+
				"  antfly inference pull %s\n"+
				"  antfly inference pull %s\n"+
				"See examples/dogfood/README.md for prerequisites.",
			inferenceURL, err, defaultEmbedModel, defaultExtractModel)
	}
	if err := probeExtractor(inferenceURL, extractModel); err != nil {
		return fmt.Errorf(
			"antfly inference server at %s cannot serve extraction model %q: %w\n"+
				"Use a servable extractor, e.g. -extract-model %s.\n"+
				"See examples/dogfood/README.md for prerequisites.",
			inferenceURL, extractModel, err, defaultExtractModel)
	}
	return nil
}

// probeInferenceHealth issues a GET against the inference server's /healthz
// route (zig/pkg/inference/src/server/server.zig registers it unprefixed) and
// returns an error if the server does not respond with a successful status.
func probeInferenceHealth(inferenceURL string) error {
	url := strings.TrimRight(inferenceURL, "/") + "/healthz"
	client := http.Client{Timeout: healthProbeTimeout}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}
	return nil
}

// extractSmokeInput is a one-sentence input used to smoke-test whether a
// model can actually serve /ai/v1/extract before ingest commits to it.
// fastino/gliner2.5-base-v1 pulls successfully but is intentionally gated off
// for serving until a production qualification row lands
// (zig/pkg/inference/src/models/gliner_boundary_qualification.zig); probing
// here surfaces that as a clear preflight failure instead of a confusing
// failure deep inside RunUntilIdle.
const extractSmokeInput = "Raft handles leader election and log replication between nodes."

// probeExtractor issues a minimal POST to the inference server's
// /ai/v1/extract route (specs/openapi/inference/api.yaml) using the dogfood
// entity schema and returns an error with the server's message on failure.
func probeExtractor(inferenceURL, model string) error {
	url := strings.TrimRight(inferenceURL, "/") + "/ai/v1/extract"
	body, err := json.Marshal(map[string]any{
		"model": model,
		"inputs": []map[string]any{
			{"content": extractSmokeInput},
		},
		"schema": map[string]any{
			"entities": entityLabels,
		},
	})
	if err != nil {
		return err
	}
	client := http.Client{Timeout: healthProbeTimeout}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Message != "" {
			return fmt.Errorf("%s: %s", apiErr.Error, apiErr.Message)
		}
		return fmt.Errorf("unexpected status %d from %s: %s", resp.StatusCode, url, strings.TrimSpace(string(respBody)))
	}
	return nil
}

// indexBuildConfig collects the flags ensureSchemaAndIndexes needs to build
// the chunk_vectors and knowledge index configs.
type indexBuildConfig struct {
	InferenceURL  string
	EmbedModel    string
	ExtractModel  string
	TargetTokens  int
	OverlapTokens int
	Metrics       bool
}

type existingIndex struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// ensureSchemaAndIndexes sets the schema and adds any of the three indexes
// (full_text, chunk_vectors, knowledge) that are not already present. Schema
// application is idempotent; index creation is name-keyed and skipped when
// the index already exists so `ingest` can be re-run without -reset.
func ensureSchemaAndIndexes(db *antflylite.DB, cfg indexBuildConfig) error {
	if err := db.SetSchemaJSON(schemaJSON()); err != nil {
		return fmt.Errorf("set schema: %w", err)
	}

	existingRaw, err := db.IndexesJSON()
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}
	var existing []existingIndex
	if err := json.Unmarshal(existingRaw, &existing); err != nil {
		return fmt.Errorf("decode existing indexes: %w\nraw: %s", err, existingRaw)
	}
	have := make(map[string]bool, len(existing))
	for _, idx := range existing {
		have[idx.Name] = true
	}

	if have[fullTextIndexName] {
		fmt.Printf("using pre-provisioned default full-text index %q\n", fullTextIndexName)
	} else {
		// Older libantfly builds do not auto-provision full_text_index_v0 the
		// way the full server does; add the same fallback index explicitly.
		config, err := fullTextIndexJSON()
		if err != nil {
			return err
		}
		if err := db.AddIndexJSON(config); err != nil {
			return fmt.Errorf("add default full-text index: %w", err)
		}
		fmt.Printf("added index %q (this libantfly build did not auto-provision it)\n", fullTextIndexName)
	}

	if !have[chunkVectorsIndex] {
		config, err := chunkVectorsIndexJSON(cfg.EmbedModel, cfg.InferenceURL, cfg.TargetTokens, cfg.OverlapTokens)
		if err != nil {
			return err
		}
		if err := db.AddIndexJSON(config); err != nil {
			return fmt.Errorf("add %s index: %w", chunkVectorsIndex, err)
		}
		fmt.Printf("added index %q (embedder=%s)\n", chunkVectorsIndex, cfg.EmbedModel)
	}

	if !have[knowledgeGraphIndex] {
		config, err := knowledgeGraphIndexJSON(cfg.ExtractModel, cfg.InferenceURL, cfg.Metrics)
		if err != nil {
			return err
		}
		if err := db.AddIndexJSON(config); err != nil {
			if cfg.Metrics {
				// Serverless/managed deployments may reject unrecognized
				// graph metric kinds; retry once without metrics rather than
				// failing the whole ingest over an optional feature.
				log.Printf("add %s index with metrics failed (%v); retrying without metrics", knowledgeGraphIndex, err)
				config, err := knowledgeGraphIndexJSON(cfg.ExtractModel, cfg.InferenceURL, false)
				if err != nil {
					return err
				}
				if err := db.AddIndexJSON(config); err != nil {
					return fmt.Errorf("add %s index: %w", knowledgeGraphIndex, err)
				}
			} else {
				return fmt.Errorf("add %s index: %w", knowledgeGraphIndex, err)
			}
		}
		fmt.Printf("added index %q (extractor=%s)\n", knowledgeGraphIndex, cfg.ExtractModel)
	}
	return nil
}
