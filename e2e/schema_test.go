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
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/oapi"
	"github.com/stretchr/testify/require"
)

// TestE2E_SchemaMigration_FullTextIndexRebuild tests the full schema migration
// lifecycle: initial index build, schema update creating a new full-text index
// version, rebuild completion, and reconciler cleanup of the old version via
// DropReadSchema.
//
// It specifically uses 1000 documents to reproduce a bug where flushBatch would
// skip setting rebuilding=false when total docs was an exact multiple of the
// backfill batch size (1000).
func TestE2E_SchemaMigration_FullTextIndexRebuild(t *testing.T) {
	skipUnlessEnv(t, "RUN_SCHEMA_MIGRATION_TESTS")
	ctx := testContext(t, 5*time.Minute)

	// Start a single-node swarm without Termite (only full-text indexing needed).
	t.Log("Starting Antfly swarm (no Termite)...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	defer swarm.Cleanup()

	tableName := "schema_migration_test"

	// Create table — this auto-creates full_text_index_v0.
	t.Log("Creating table...")
	err := swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err, "Failed to create table")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)
	t.Log("Table created with full_text_index_v0")

	// Ingest exactly 1000 documents — the regression case.
	// fullTextBackfillBatchSize is 1000, so 1000 % 1000 == 0 triggers the
	// flushBatch bug where count==0 at the final flush would skip setting
	// bi.rebuilding = false.
	const numDocs = 1000
	t.Logf("Inserting %d documents...", numDocs)

	batchSize := 200
	for i := 0; i < numDocs; i += batchSize {
		end := min(i+batchSize, numDocs)
		inserts := make(map[string]any, end-i)
		for j := i; j < end; j++ {
			key := fmt.Sprintf("doc-%04d", j)
			inserts[key] = map[string]any{
				"title":   fmt.Sprintf("Document %d", j),
				"content": fmt.Sprintf("This is the content of document number %d with some searchable text.", j),
			}
		}
		_, err := swarm.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   inserts,
			SyncLevel: antfly.SyncLevelFullText,
		})
		require.NoError(t, err, "Failed to insert batch starting at %d", i)
	}
	t.Logf("Inserted %d documents", numDocs)

	// Wait for full_text_index_v0 to finish initial indexing.
	t.Log("Waiting for full_text_index_v0 to complete initial build...")
	waitForFullTextIndex(t, ctx, swarm.Client, tableName, "full_text_index_v0", numDocs, 2*time.Minute)
	t.Log("full_text_index_v0 build complete")

	// Update schema via oapi client (AntflyClient doesn't wrap UpdateSchema).
	apiURL := swarm.MetadataAPIURL + "/api/v1"
	oapiClient, err := oapi.NewClient(apiURL, oapi.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}))
	require.NoError(t, err, "Failed to create oapi client")

	t.Log("Updating schema to trigger index migration...")
	newSchema := oapi.TableSchema{
		DocumentSchemas: map[string]oapi.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title": map[string]any{
							"type":                    "string",
							"x-antfly-types":          []any{"text"},
							"x-antfly-include-in-all": true,
						},
						"content": map[string]any{
							"type":                    "string",
							"x-antfly-types":          []any{"text"},
							"x-antfly-include-in-all": true,
						},
					},
				},
			},
		},
	}
	resp, err := oapiClient.UpdateSchema(ctx, tableName, newSchema)
	require.NoError(t, err, "Failed to call UpdateSchema")
	resp.Body.Close()
	require.Less(t, resp.StatusCode, 300, "UpdateSchema returned error status %d", resp.StatusCode)
	t.Log("Schema updated, full_text_index_v1 should now be created")

	tableStatus, err := swarm.Client.GetTable(ctx, tableName)
	require.NoError(t, err, "GetTable after schema update failed")
	require.NotNil(t, tableStatus.Migration, "migration should be present during rebuild")
	require.Equal(t, "rebuilding", string(tableStatus.Migration.State))
	require.Equal(t, uint32(0), tableStatus.Migration.ReadSchema.Version, "read_schema should still serve v0")
	require.Equal(t, uint32(1), tableStatus.Schema.Version, "schema should advance to v1")

	// Verify both index versions now exist.
	indexes, err := swarm.Client.ListIndexes(ctx, tableName)
	require.NoError(t, err)
	require.Contains(t, indexes, "full_text_index_v0", "v0 should still exist during migration")
	require.Contains(t, indexes, "full_text_index_v1", "v1 should be created after schema update")
	t.Log("Both full_text_index_v0 and full_text_index_v1 exist")

	// Wait for full_text_index_v1 rebuild to complete.
	t.Log("Waiting for full_text_index_v1 rebuild to complete...")
	waitForFullTextIndex(t, ctx, swarm.Client, tableName, "full_text_index_v1", numDocs, 2*time.Minute)
	t.Log("full_text_index_v1 rebuild complete")

	// Wait for the reconciler to drop the old full_text_index_v0 via DropReadSchema.
	t.Log("Waiting for reconciler to drop full_text_index_v0...")
	waitForOldIndexDropped(t, ctx, swarm.Client, tableName, "full_text_index_v0", 2*time.Minute)
	t.Log("full_text_index_v0 successfully dropped by reconciler")

	// Verify only v1 remains.
	indexes, err = swarm.Client.ListIndexes(ctx, tableName)
	require.NoError(t, err)
	require.NotContains(t, indexes, "full_text_index_v0", "v0 should be dropped")
	require.Contains(t, indexes, "full_text_index_v1", "v1 should remain")

	tableStatus, err = swarm.Client.GetTable(ctx, tableName)
	require.NoError(t, err, "GetTable after reconciler cleanup failed")
	require.Nil(t, tableStatus.Migration, "migration should be absent when stable")
	require.Equal(t, uint32(1), tableStatus.Schema.Version, "schema should remain on v1")

	// Verify data integrity: a specific document is still accessible.
	doc, err := swarm.Client.LookupKey(ctx, tableName, "doc-0500")
	require.NoError(t, err, "LookupKey for doc-0500 failed")
	require.Equal(t, "Document 500", doc["title"], "Document title should match")

	t.Log("Schema migration e2e test passed")
}

// waitForFullTextIndex polls a full-text index until it finishes rebuilding
// and has indexed the expected number of documents.
func waitForFullTextIndex(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	tableName, indexName string,
	expectedDocs int,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Context cancelled while waiting for %s", indexName)
		case <-ticker.C:
			if time.Now().After(deadline) {
				t.Fatalf("Timeout waiting for %s to finish rebuilding", indexName)
			}

			indexStatus, err := client.GetIndex(ctx, tableName, indexName)
			if err != nil {
				t.Logf("  Error getting %s status: %v", indexName, err)
				continue
			}

			stats, err := indexStatus.Status.AsFullTextIndexStats()
			if err != nil {
				t.Logf("  Error decoding full-text stats for %s: %v", indexName, err)
				continue
			}

			t.Logf("  %s: indexed=%d/%d rebuilding=%v",
				indexName, stats.TotalIndexed, expectedDocs, stats.Rebuilding)

			if !stats.Rebuilding && stats.TotalIndexed >= uint64(expectedDocs) {
				return
			}
		}
	}
}

// waitForOldIndexDropped polls ListIndexes until the specified index is no
// longer present, indicating the reconciler has called DropReadSchema.
func waitForOldIndexDropped(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	tableName, oldIndexName string,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Context cancelled while waiting for %s to be dropped", oldIndexName)
		case <-ticker.C:
			if time.Now().After(deadline) {
				t.Fatalf("Timeout waiting for %s to be dropped", oldIndexName)
			}

			indexes, err := client.ListIndexes(ctx, tableName)
			if err != nil {
				t.Logf("  Error listing indexes: %v", err)
				continue
			}

			names := make([]string, 0, len(indexes))
			for name := range indexes {
				names = append(names, name)
			}
			t.Logf("  Current indexes: %v", names)

			if _, exists := indexes[oldIndexName]; !exists {
				return
			}
		}
	}
}
