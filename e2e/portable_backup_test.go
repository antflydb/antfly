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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/stretchr/testify/require"
)

// TestE2E_PortableBackupRestore tests the portable (AFB) backup and restore flow.
// It creates a table with documents, backs up with format=portable, drops the table,
// restores from the AFB backup, and verifies data integrity.
func TestE2E_PortableBackupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Start Antfly swarm without Termite (no ML models needed)
	t.Log("Starting Antfly swarm...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	defer swarm.Cleanup()

	tableName := "portable_backup_test"
	backupID := "portable-backup-e2e"

	// Step 1: Create a simple table
	t.Log("Creating table...")
	createSimpleTable(t, ctx, swarm.Client, tableName)

	// Step 2: Insert test documents
	t.Log("Inserting test documents...")
	testDocs := map[string]map[string]any{
		"alpha":   {"title": "Alpha Document", "content": "First document for portable backup test"},
		"beta":    {"title": "Beta Document", "content": "Second document for portable backup test"},
		"gamma":   {"title": "Gamma Document", "content": "Third document for portable backup test"},
		"delta":   {"title": "Delta Document", "content": "Fourth document for portable backup test"},
		"epsilon": {"title": "Epsilon Document", "content": "Fifth document for portable backup test"},
	}
	insertTestDocuments(t, ctx, swarm.Client, tableName, testDocs, antfly.SyncLevelFullText)

	// Step 3: Verify document count before backup
	countBefore := getDocumentCount(t, ctx, swarm.Client, tableName)
	t.Logf("Document count before backup: %d", countBefore)
	require.Equal(t, len(testDocs), countBefore, "Expected all documents to be present")

	// Step 4: Create portable backup
	t.Log("Creating portable backup...")
	backupDir := GetBackupDir(t)
	location := "file://" + backupDir
	err := backupTableWithFormat(t, swarm.MetadataAPIURL, tableName, backupID, location, "portable")
	require.NoError(t, err, "Failed to create portable backup")

	// Step 5: Verify .afb files were created
	t.Log("Verifying AFB files exist...")
	afbFiles, err := filepath.Glob(filepath.Join(backupDir, "*.afb"))
	require.NoError(t, err)
	require.NotEmpty(t, afbFiles, "Expected at least one .afb file in backup directory")
	for _, f := range afbFiles {
		info, err := os.Stat(f)
		require.NoError(t, err)
		t.Logf("AFB file: %s (%d bytes)", filepath.Base(f), info.Size())
		require.Greater(t, info.Size(), int64(common.AFBHeaderSize), "AFB file too small")

		// Verify magic bytes
		data := make([]byte, 8)
		fh, err := os.Open(f) //nolint:gosec
		require.NoError(t, err)
		_, err = io.ReadFull(fh, data)
		_ = fh.Close()
		require.NoError(t, err)
		require.True(t, common.IsAFBFormat(data), "File %s doesn't have AFB magic bytes", filepath.Base(f))
	}

	// Step 6: Drop the table
	t.Log("Dropping table...")
	err = swarm.Client.DropTable(ctx, tableName)
	require.NoError(t, err, "Failed to drop table")
	time.Sleep(2 * time.Second)

	// Step 7: Restore from portable backup
	t.Log("Restoring from portable backup...")
	err = restoreTableWithFormat(t, swarm.MetadataAPIURL, tableName, backupID, location, "portable")
	require.NoError(t, err, "Failed to restore from portable backup")

	// Step 8: Wait for shards to be ready
	t.Log("Waiting for shards to be ready after restore...")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 60*time.Second)

	// Step 9: Verify document count after restore
	countAfter := getDocumentCount(t, ctx, swarm.Client, tableName)
	t.Logf("Document count after restore: %d", countAfter)
	require.Equal(t, countBefore, countAfter, "Document count mismatch after portable restore")

	// Step 10: Verify individual documents
	t.Log("Verifying individual documents...")
	for docID, expected := range testDocs {
		docMap, err := swarm.Client.LookupKey(ctx, tableName, docID)
		require.NoError(t, err, "Failed to get document %s after restore", docID)
		require.NotNil(t, docMap, "Document %s should exist after restore", docID)

		// Verify fields
		require.Equal(t, expected["title"], docMap["title"], "Title mismatch for %s", docID)
		require.Equal(t, expected["content"], docMap["content"], "Content mismatch for %s", docID)
	}

	t.Log("Portable backup/restore test completed successfully!")
}

// backupTableWithFormat calls the backup API with a specific format.
// This bypasses the SDK since the generated client types don't have the format
// field yet (needs make generate).
func backupTableWithFormat(t *testing.T, metadataURL, tableName, backupID, location, format string) error {
	t.Helper()

	reqBody := map[string]string{
		"backup_id": backupID,
		"location":  location,
		"format":    format,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/tables/%s/backup", metadataURL, tableName)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:gosec,noctx
	if err != nil {
		return fmt.Errorf("backup request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backup failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// restoreTableWithFormat calls the restore API with a specific format.
func restoreTableWithFormat(t *testing.T, metadataURL, tableName, backupID, location, format string) error {
	t.Helper()

	reqBody := map[string]string{
		"backup_id": backupID,
		"location":  location,
		"format":    format,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/tables/%s/restore", metadataURL, tableName)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:gosec,noctx
	if err != nil {
		return fmt.Errorf("restore request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("restore failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	// Wait for restore to complete
	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("restore timed out waiting for table to become ready")
			}
			// Check if table exists and has shards
			url := fmt.Sprintf("%s/api/v1/tables/%s", metadataURL, tableName)
			resp, err := http.Get(url) //nolint:gosec,noctx
			if err != nil {
				continue
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
	}
}
