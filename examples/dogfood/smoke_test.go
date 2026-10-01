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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestIndexConfigsAreAccepted verifies that the three raw index JSON payloads
// dogfood builds (full_text, chunk_vectors, knowledge) are accepted by
// antfly_db_add_index_json against a real libantfly, without running any
// inference. It does not exercise ingest or query.
func TestIndexConfigsAreAccepted(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "smoke.aflite")
	db, err := openOrCreateLite(dbPath, "")
	if err != nil {
		t.Fatalf("create Lite db: %v", err)
	}
	defer db.Close()

	cfg := indexBuildConfig{
		InferenceURL:  defaultInferenceURL,
		EmbedModel:    defaultEmbedModel,
		ExtractModel:  defaultExtractModel,
		TargetTokens:  defaultTargetTokens,
		OverlapTokens: defaultOverlapTokens,
		Metrics:       true,
	}
	if err := ensureSchemaAndIndexes(db, cfg); err != nil {
		t.Fatalf("ensure schema and indexes: %v", err)
	}

	raw, err := db.IndexesJSON()
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	t.Logf("indexes: %s", raw)

	// Re-running must be a no-op (idempotent index creation, existing configs
	// verified as matching) rather than an AlreadyExists error.
	if err := ensureSchemaAndIndexes(db, cfg); err != nil {
		t.Fatalf("ensure schema and indexes (second call): %v", err)
	}

	// Changed ingestion settings against an existing database must fail
	// loudly with a rebuild instruction, never silently keep the old
	// configuration (the name-only provisioning gap).
	drifted := cfg
	drifted.ExtractModel = "some/other-extractor"
	if err := ensureSchemaAndIndexes(db, drifted); err == nil {
		t.Fatalf("ensure schema and indexes accepted a changed extract model against an existing database")
	}
	driftedChunks := cfg
	driftedChunks.TargetTokens = cfg.TargetTokens * 2
	if err := ensureSchemaAndIndexes(db, driftedChunks); err == nil {
		t.Fatalf("ensure schema and indexes accepted changed chunk geometry against an existing database")
	}

	// REMOVING a behavior-affecting setting is drift too, not harmless
	// surplus in the stored config: a database built with metrics must not
	// verify against -metrics=false.
	noMetrics := cfg
	noMetrics.Metrics = false
	if err := ensureSchemaAndIndexes(db, noMetrics); err == nil {
		t.Fatalf("ensure schema and indexes accepted -metrics=false against a database built with metrics")
	}

	os.Remove(dbPath)
}

func TestRemoteToLocalInferenceIsDrift(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "remote.aflite")
	db, err := openOrCreateLite(dbPath, "")
	if err != nil {
		t.Fatalf("create Lite db: %v", err)
	}
	defer db.Close()

	remoteCfg := indexBuildConfig{
		InferenceURL:  "http://127.0.0.1:8090",
		EmbedModel:    defaultEmbedModel,
		ExtractModel:  defaultExtractModel,
		TargetTokens:  defaultTargetTokens,
		OverlapTokens: defaultOverlapTokens,
		Metrics:       true,
	}
	if err := ensureSchemaAndIndexes(db, remoteCfg); err != nil {
		t.Fatalf("ensure schema and indexes (remote inference): %v", err)
	}
	// Producers built for a remote inference server carry api_url; a re-run
	// in the default in-process mode omits it. The stored remote URL is a
	// live behavioral difference and must be reported as drift instead of
	// passing as an extra stored field.
	localCfg := remoteCfg
	localCfg.InferenceURL = ""
	if err := ensureSchemaAndIndexes(db, localCfg); err == nil {
		t.Fatalf("ensure schema and indexes accepted in-process inference against a database built for a remote inference server")
	}
	os.Remove(dbPath)
}

func TestMissingPipelineEnrichmentsAreDrift(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty.aflite")
	db, err := openOrCreateLite(dbPath, "")
	if err != nil {
		t.Fatalf("create Lite db: %v", err)
	}
	defer db.Close()

	cfg := indexBuildConfig{
		InferenceURL:  defaultInferenceURL,
		EmbedModel:    defaultEmbedModel,
		ExtractModel:  defaultExtractModel,
		TargetTokens:  defaultTargetTokens,
		OverlapTokens: defaultOverlapTokens,
	}
	// A database without the pipeline's enrichments (predating it, or built
	// by another tool) must fail verification, not silently skip the
	// unconfigured stages.
	if err := verifyChunkPipelineEnrichments(db, "", cfg); err == nil {
		t.Fatalf("verifyChunkPipelineEnrichments accepted a database with no pipeline enrichments")
	}
	os.Remove(dbPath)
}

func TestModelComparisonsAreExact(t *testing.T) {
	// A substring check would accept the stored "-v2" variant for the
	// requested base model; the parsed comparison must not.
	if got := producerModel(`{"type":"extractor","config":{"model":"org/model-v2"}}`); got == "org/model" {
		t.Fatalf("producerModel conflated org/model-v2 with org/model")
	}
	if got := producerModel(`{"type":"extractor","config":{"model":"org/model"}}`); got != "org/model" {
		t.Fatalf("producerModel = %q, want org/model", got)
	}
	if jsonHasExactModel(`{"embedder":{"model":"org/model-v2"}}`, "org/model") {
		t.Fatalf("jsonHasExactModel accepted a prefix match")
	}
	if !jsonHasExactModel(`{"nested":{"embedder":{"model":"org/model"}}}`, "org/model") {
		t.Fatalf("jsonHasExactModel missed an exact nested match")
	}
}

func TestSubsetMismatch(t *testing.T) {
	want := map[string]any{"a": map[string]any{"b": float64(1)}, "list": []any{"x"}}
	matching := map[string]any{"a": map[string]any{"b": float64(1), "extra": true}, "list": []any{"x"}, "more": 2}
	if path, mismatch := subsetMismatch(want, matching, "$"); mismatch {
		t.Fatalf("expected subset match, got mismatch at %s", path)
	}
	missing := map[string]any{"a": map[string]any{}, "list": []any{"x"}}
	if path, mismatch := subsetMismatch(want, missing, "$"); !mismatch || path != "$.a.b" {
		t.Fatalf("expected mismatch at $.a.b, got mismatch=%v path=%s", mismatch, path)
	}
	changed := map[string]any{"a": map[string]any{"b": float64(2)}, "list": []any{"x"}}
	if _, mismatch := subsetMismatch(want, changed, "$"); !mismatch {
		t.Fatalf("expected mismatch on changed scalar")
	}
	shorter := map[string]any{"a": map[string]any{"b": float64(1)}, "list": []any{}}
	if _, mismatch := subsetMismatch(want, shorter, "$"); !mismatch {
		t.Fatalf("expected mismatch on array length change")
	}
}

func TestSlugifyMirrorsResolverSlugHelper(t *testing.T) {
	cases := map[string]string{
		"A. Lovelace":      "a_lovelace",
		"metadata server":  "metadata_server",
		"DataServer":       "dataserver",
		"VOPR":             "vopr",
		"  Raft groups  ":  "raft_groups",
		"Epstein's island": "epstein_island",
		"O'Brien & Sons":   "o_brien_sons",
	}
	for input, want := range cases {
		if got := slugify(input); got != want {
			t.Errorf("slugify(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestEntityKeyCandidates(t *testing.T) {
	if got := entityKeyCandidates("component/raft"); len(got) != 1 || got[0] != "component/raft" {
		t.Fatalf("exact key should pass through, got %v", got)
	}
	candidates := entityKeyCandidates("metadata server")
	if len(candidates) != 1 || candidates[0] != "entity/metadata_server" {
		t.Fatalf("expected the label-free entity key, got %v", candidates)
	}
}
