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

	os.Remove(dbPath)
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
