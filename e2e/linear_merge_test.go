// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package e2e

import (
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// TestLinearMerge_IdempotentSync tests that running the same linear merge twice
// should skip unchanged documents on the second run.
//
// This test reproduces the issue where `go run ./examples/docsaf/main.go sync`
// would re-insert all documents every time instead of skipping unchanged ones.
func TestLinearMerge_IdempotentSync(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := testContext(t, 2*time.Minute)

	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	tableName := "test_idempotent_sync"

	// Create table
	t.Log("Creating table...")
	err := swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	// Wait for shards to be ready
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)

	// Prepare test data - simulate documents from docsaf
	// These are documents WITHOUT _timestamp field (as they would come from docsaf)
	testRecords := map[string]any{
		"docs/getting-started.md": map[string]any{
			"title":    "Getting Started",
			"content":  "This is the getting started guide.",
			"filepath": "docs/getting-started.md",
			"type":     "markdown",
		},
		"docs/installation.md": map[string]any{
			"title":    "Installation",
			"content":  "Installation instructions here.",
			"filepath": "docs/installation.md",
			"type":     "markdown",
		},
		"docs/configuration.md": map[string]any{
			"title":    "Configuration",
			"content":  "Configuration guide here.",
			"filepath": "docs/configuration.md",
			"type":     "markdown",
		},
	}

	// FIRST SYNC - all documents should be inserted
	t.Log("=== First Sync ===")
	result1, err := swarm.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
		Records:      testRecords,
		LastMergedId: "",
		DryRun:       false,
		SyncLevel:    antfly.SyncLevelWrite,
	})
	require.NoError(t, err)
	require.NotNil(t, result1)

	t.Logf("First sync - Upserted: %d, Skipped: %d, Deleted: %d",
		result1.Upserted, result1.Skipped, result1.Deleted)

	require.Equal(t, 3, result1.Upserted, "First sync should upsert all 3 documents")
	require.Equal(t, 0, result1.Skipped, "First sync should skip 0 documents")
	require.Equal(t, 0, result1.Deleted, "First sync should delete 0 documents")

	// Wait a moment to ensure writes are flushed
	time.Sleep(500 * time.Millisecond)

	// SECOND SYNC - same data, should skip all documents
	// This is the critical test: documents don't have _timestamp,
	// but the stored documents DO have _timestamp (added by the system).
	// The hash comparison should ignore _timestamp, so these should be skipped.
	t.Log("=== Second Sync (Idempotent) ===")
	result2, err := swarm.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
		Records:      testRecords,
		LastMergedId: "",
		DryRun:       false,
		SyncLevel:    antfly.SyncLevelWrite,
	})
	require.NoError(t, err)
	require.NotNil(t, result2)

	t.Logf("Second sync - Upserted: %d, Skipped: %d, Deleted: %d",
		result2.Upserted, result2.Skipped, result2.Deleted)

	// THIS IS THE KEY ASSERTION - second sync should skip all unchanged documents
	// Before the fix, this would fail because _timestamp differences caused hash mismatches
	require.Equal(t, 0, result2.Upserted, "Second sync should upsert 0 documents (all unchanged)")
	require.Equal(t, 3, result2.Skipped, "Second sync should skip all 3 unchanged documents")
	require.Equal(t, 0, result2.Deleted, "Second sync should delete 0 documents")

	// THIRD SYNC - modify one document, rest should still be skipped
	t.Log("=== Third Sync (With One Change) ===")
	modifiedRecords := map[string]any{
		"docs/getting-started.md": map[string]any{
			"title":    "Getting Started",
			"content":  "This is the UPDATED getting started guide.", // Changed
			"filepath": "docs/getting-started.md",
			"type":     "markdown",
		},
		"docs/installation.md": map[string]any{
			"title":    "Installation",
			"content":  "Installation instructions here.",
			"filepath": "docs/installation.md",
			"type":     "markdown",
		},
		"docs/configuration.md": map[string]any{
			"title":    "Configuration",
			"content":  "Configuration guide here.",
			"filepath": "docs/configuration.md",
			"type":     "markdown",
		},
	}

	result3, err := swarm.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
		Records:      modifiedRecords,
		LastMergedId: "",
		DryRun:       false,
		SyncLevel:    antfly.SyncLevelWrite,
	})
	require.NoError(t, err)
	require.NotNil(t, result3)

	t.Logf("Third sync - Upserted: %d, Skipped: %d, Deleted: %d",
		result3.Upserted, result3.Skipped, result3.Deleted)

	require.Equal(t, 1, result3.Upserted, "Third sync should upsert 1 changed document")
	require.Equal(t, 2, result3.Skipped, "Third sync should skip 2 unchanged documents")
	require.Equal(t, 0, result3.Deleted, "Third sync should delete 0 documents")

	// Give background workers time to shut down gracefully before test completes
	// This prevents "Log in goroutine after test has completed" panics from
	// BleveIndexV2 background goroutines that are still processing the context
	// cancellation signal when the test returns.
	time.Sleep(500 * time.Millisecond)
}
