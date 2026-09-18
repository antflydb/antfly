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

	// Re-running must be a no-op (idempotent index creation) rather than an
	// AlreadyExists error.
	if err := ensureSchemaAndIndexes(db, cfg); err != nil {
		t.Fatalf("ensure schema and indexes (second call): %v", err)
	}

	os.Remove(dbPath)
}
