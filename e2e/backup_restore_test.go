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
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// TestE2E_BackupRestore_Embeddings tests the full backup and restore flow
// with embedding indexes to ensure embeddings are properly restored and indexed.
func TestE2E_BackupRestore_Embeddings(t *testing.T) {
	skipUnlessML(t)

	SkipIfProviderUnavailable(t)

	// Allow 10 minutes for the test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Step 1: Start Antfly swarm
	t.Log("Starting Antfly swarm...")
	swarm := startAntflySwarm(t, ctx)
	defer swarm.Cleanup()

	tableName := "backup_restore_test"
	backupID := "backup-restore-e2e"

	// Step 2: Create table with embedding index
	t.Log("Creating table with embedding index...")
	createTestTableWithEmbeddings(t, ctx, swarm.Client, tableName)

	// Step 3: Insert test documents
	t.Log("Inserting test documents...")
	testDocs := getTestDocuments()
	insertTestDocuments(t, ctx, swarm.Client, tableName, testDocs, antfly.SyncLevelAknn)

	// Step 4: Wait for embeddings to be generated
	t.Log("Waiting for embeddings to be generated...")
	waitForEmbeddings(t, ctx, swarm.Client, tableName, "embeddings", len(testDocs), 5*time.Minute)

	// Step 5: Get embedding count before backup
	embeddingCountBefore := getEmbeddingCount(t, ctx, swarm.Client, tableName)
	t.Logf("Embedding count before backup: %d", embeddingCountBefore)
	require.Positive(t, embeddingCountBefore, "Expected embeddings to be generated before backup")

	// Step 6: Verify semantic search works before backup
	t.Log("Verifying semantic search works before backup...")
	searchResultsBefore := doSemanticSearch(t, ctx, swarm.Client, tableName, "distributed database")
	require.NotEmpty(t, searchResultsBefore, "Expected search results before backup")
	t.Logf("Search returned %d results before backup", len(searchResultsBefore))

	// Step 7: Create backup
	t.Log("Creating backup...")
	err := BackupTestDatabase(t, ctx, swarm.Client, tableName, backupID)
	require.NoError(t, err, "Failed to create backup")

	// Step 8: Delete the table
	t.Log("Deleting table to simulate fresh restore...")
	err = swarm.Client.DropTable(ctx, tableName)
	require.NoError(t, err, "Failed to delete table")

	// Wait a moment for table to be fully deleted
	time.Sleep(2 * time.Second)

	// Step 9: Restore from backup
	t.Log("Restoring from backup...")
	err = RestoreTestDatabase(t, ctx, swarm.Client, tableName, backupID)
	require.NoError(t, err, "Failed to restore from backup")

	// Step 10: Wait for shards to be ready after restore
	t.Log("Waiting for shards to be ready after restore...")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)

	// Step 11: Wait for embeddings to be indexed after restore
	// After restore, the vector index needs to rebuild from Pebble data
	t.Log("Waiting for embeddings to be indexed after restore...")
	waitForEmbeddings(t, ctx, swarm.Client, tableName, "embeddings", len(testDocs), 5*time.Minute)

	// Step 12: Get embedding count after restore
	embeddingCountAfter := getEmbeddingCount(t, ctx, swarm.Client, tableName)
	t.Logf("Embedding count after restore: %d", embeddingCountAfter)

	// Step 13: Verify embedding count matches
	require.Equal(t, embeddingCountBefore, embeddingCountAfter,
		"Embedding count mismatch after restore: before=%d, after=%d",
		embeddingCountBefore, embeddingCountAfter)

	// Step 14: Verify semantic search still works after restore
	t.Log("Verifying semantic search works after restore...")
	searchResultsAfter := doSemanticSearch(t, ctx, swarm.Client, tableName, "distributed database")
	require.NotEmpty(t, searchResultsAfter, "Expected search results after restore")
	t.Logf("Search returned %d results after restore", len(searchResultsAfter))

	// Verify we get similar results before and after restore
	require.Len(t, searchResultsAfter, len(searchResultsBefore),
		"Search result count changed after restore")

	t.Log("Backup/restore test completed successfully!")
}

// createTestTableWithEmbeddings creates a table with an embedding index
func createTestTableWithEmbeddings(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string) {
	t.Helper()

	// Create embedding index config
	embeddingIndexConfig := antfly.IndexConfig{
		Name: "embeddings",
		Type: "aknn_v0",
	}

	// Configure embedder based on E2E_PROVIDER
	embedder, err := GetDefaultEmbedderConfig(t)
	require.NoError(t, err, "Failed to configure embedder")

	// Configure embedding index
	err = embeddingIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *embedder,
	})
	require.NoError(t, err, "Failed to configure embedding index")

	// Create table
	err = client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"embeddings": embeddingIndexConfig,
		},
	})
	require.NoError(t, err, "Failed to create table")

	// Wait for shards to be ready
	waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

	t.Logf("Created table '%s' with embedding index", tableName)
}

// getTestDocuments returns a set of test documents for backup/restore testing
func getTestDocuments() map[string]map[string]any {
	return map[string]map[string]any{
		"doc1": {
			"title":   "Introduction to Distributed Databases",
			"content": "Distributed databases spread data across multiple nodes for scalability and fault tolerance. They use consensus algorithms like Raft to ensure data consistency.",
		},
		"doc2": {
			"title":   "Vector Search Fundamentals",
			"content": "Vector search enables semantic similarity queries by converting text to embeddings. These embeddings are high-dimensional vectors that capture meaning.",
		},
		"doc3": {
			"title":   "Building Scalable Systems",
			"content": "Scalable systems handle growing workloads by adding resources. Horizontal scaling adds more nodes while vertical scaling increases node capacity.",
		},
		"doc4": {
			"title":   "Consensus Algorithms",
			"content": "Raft is a consensus algorithm that ensures all nodes in a distributed system agree on the same state. It elects a leader to coordinate writes.",
		},
		"doc5": {
			"title":   "Embedding Models",
			"content": "Embedding models transform text into numerical vectors. These vectors preserve semantic relationships, allowing similar texts to have similar vectors.",
		},
	}
}

// insertTestDocuments inserts test documents into the table with the given sync level.
func insertTestDocuments(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string, docs map[string]map[string]any, syncLevel antfly.SyncLevel) {
	t.Helper()

	inserts := make(map[string]any, len(docs))
	for id, doc := range docs {
		inserts[id] = doc
	}

	_, err := client.Batch(ctx, tableName, antfly.BatchRequest{
		Inserts:   inserts,
		SyncLevel: syncLevel,
	})
	require.NoError(t, err, "Failed to insert documents into %s", tableName)

	t.Logf("Inserted %d documents into '%s'", len(docs), tableName)
}

// getEmbeddingCount returns the current embedding count from index stats
func getEmbeddingCount(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string) uint64 {
	t.Helper()

	indexStatus, err := client.GetIndex(ctx, tableName, "embeddings")
	if err != nil {
		t.Logf("Warning: Failed to get index status: %v", err)
		return 0
	}

	stats, err := indexStatus.Status.AsEmbeddingsIndexStats()
	if err != nil {
		t.Logf("Warning: Failed to decode embedding stats: %v", err)
		return 0
	}

	return stats.TotalIndexed
}

// doSemanticSearch performs a semantic search and returns results
func doSemanticSearch(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName, query string) []antfly.Hit {
	t.Helper()

	results, err := client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: query,
		Indexes:        []string{"embeddings"},
		Limit:          5,
	})
	require.NoError(t, err, "Semantic search failed")
	require.NotEmpty(t, results.Responses, "Expected at least one query response")

	return results.Responses[0].Hits.Hits
}

// TestE2E_ClusterBackupRestore tests cluster-level backup and restore of multiple tables
func TestE2E_ClusterBackupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Allow 10 minutes for the test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Step 1: Start Antfly swarm
	t.Log("Starting Antfly swarm...")
	swarm := startAntflySwarm(t, ctx)
	defer swarm.Cleanup()

	table1Name := "cluster_backup_table1"
	table2Name := "cluster_backup_table2"
	backupID := "cluster-backup-e2e"

	// Step 2: Create two simple tables
	t.Log("Creating test tables...")
	createSimpleTable(t, ctx, swarm.Client, table1Name)
	createSimpleTable(t, ctx, swarm.Client, table2Name)

	// Step 3: Insert test documents into both tables
	t.Log("Inserting test documents...")
	table1Docs := map[string]map[string]any{
		"doc1": {"title": "Table 1 Doc 1", "content": "Content for table 1 document 1"},
		"doc2": {"title": "Table 1 Doc 2", "content": "Content for table 1 document 2"},
	}
	table2Docs := map[string]map[string]any{
		"docA": {"title": "Table 2 Doc A", "content": "Content for table 2 document A"},
		"docB": {"title": "Table 2 Doc B", "content": "Content for table 2 document B"},
	}
	insertTestDocuments(t, ctx, swarm.Client, table1Name, table1Docs, antfly.SyncLevelFullText)
	insertTestDocuments(t, ctx, swarm.Client, table2Name, table2Docs, antfly.SyncLevelFullText)

	// Step 4: Verify document counts before backup
	count1Before := getDocumentCount(t, ctx, swarm.Client, table1Name)
	count2Before := getDocumentCount(t, ctx, swarm.Client, table2Name)
	t.Logf("Document counts before backup: table1=%d, table2=%d", count1Before, count2Before)
	require.Equal(t, 2, count1Before, "Expected 2 documents in table1")
	require.Equal(t, 2, count2Before, "Expected 2 documents in table2")

	// Step 5: Create cluster backup of all tables
	t.Log("Creating cluster backup...")
	backupDir := GetBackupDir(t)
	location := "file://" + backupDir

	result, err := swarm.Client.ClusterBackup(ctx, backupID, location, nil) // nil means all tables
	require.NoError(t, err, "Failed to create cluster backup")
	require.Equal(t, "completed", result.Status, "Backup status should be completed")
	t.Logf("Cluster backup completed with status: %s, tables: %d", result.Status, len(result.Tables))

	// Step 6: List backups and verify
	t.Log("Listing backups...")
	backups, err := swarm.Client.ListBackups(ctx, location)
	require.NoError(t, err, "Failed to list backups")
	require.NotEmpty(t, backups, "Expected at least one backup")

	found := false
	for _, b := range backups {
		if b.BackupID == backupID {
			found = true
			t.Logf("Found backup: %s, timestamp: %s, tables: %v", b.BackupID, b.Timestamp, b.Tables)
			require.GreaterOrEqual(t, len(b.Tables), 2, "Expected at least 2 tables in backup")
		}
	}
	require.True(t, found, "Expected to find backup with ID %s", backupID)

	// Step 7: Drop both tables
	t.Log("Dropping tables...")
	err = swarm.Client.DropTable(ctx, table1Name)
	require.NoError(t, err, "Failed to drop table1")
	err = swarm.Client.DropTable(ctx, table2Name)
	require.NoError(t, err, "Failed to drop table2")

	// Wait for tables to be fully deleted
	time.Sleep(2 * time.Second)

	// Step 8: Restore from cluster backup
	t.Log("Restoring from cluster backup...")
	restoreResult, err := swarm.Client.ClusterRestore(ctx, backupID, location, nil, "fail_if_exists")
	require.NoError(t, err, "Failed to restore from cluster backup")
	t.Logf("Cluster restore triggered with status: %s", restoreResult.Status)

	// Step 9: Wait for tables to be ready
	t.Log("Waiting for tables to be ready after restore...")
	waitForShardsReady(t, ctx, swarm.Client, table1Name, 60*time.Second)
	waitForShardsReady(t, ctx, swarm.Client, table2Name, 60*time.Second)

	// Step 10: Verify document counts after restore
	count1After := getDocumentCount(t, ctx, swarm.Client, table1Name)
	count2After := getDocumentCount(t, ctx, swarm.Client, table2Name)
	t.Logf("Document counts after restore: table1=%d, table2=%d", count1After, count2After)
	require.Equal(t, count1Before, count1After, "Document count mismatch in table1")
	require.Equal(t, count2Before, count2After, "Document count mismatch in table2")

	t.Log("Cluster backup/restore test completed successfully!")
}

// createSimpleTable creates a basic table without embedding indexes
func createSimpleTable(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string) {
	t.Helper()

	err := client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err, "Failed to create table %s", tableName)

	// Wait for shards to be ready
	waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

	t.Logf("Created table '%s'", tableName)
}

// getDocumentCount returns the document count for a table
func getDocumentCount(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string) int {
	t.Helper()

	results, err := client.Query(ctx, antfly.QueryRequest{
		Table: tableName,
		Limit: 1000,
	})
	require.NoError(t, err, "Failed to query table %s", tableName)
	require.NotEmpty(t, results.Responses, "Expected at least one query response")

	return len(results.Responses[0].Hits.Hits)
}
