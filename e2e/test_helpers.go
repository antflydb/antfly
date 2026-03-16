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
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/libaf/logging"
	"github.com/antflydb/antfly/src/common"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// skipInShortMode skips the test when -short is passed.
func skipInShortMode(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}
}

// skipUnlessML skips the test unless RUN_ML_TESTS=true. Use for tests that
// require large model downloads (Ollama, ONNX CLIP/CLAP, etc.).
func skipUnlessML(t *testing.T) {
	t.Helper()
	skipInShortMode(t)
	if os.Getenv("RUN_ML_TESTS") != "true" {
		t.Skip("Skipping ML test (set RUN_ML_TESTS=true to run)")
	}
}

// skipUnlessPG skips the test unless RUN_PG_TESTS=true. Use for tests that
// require a running PostgreSQL instance.
func skipUnlessPG(t *testing.T) {
	t.Helper()
	skipInShortMode(t)
	if os.Getenv("RUN_PG_TESTS") != "true" {
		t.Skip("Skipping PostgreSQL test (set RUN_PG_TESTS=true to run)")
	}
}

// testContext returns a context that cancels on SIGINT/SIGTERM or after the
// given timeout. The base context is t.Context(), so it is also cancelled when
// the test ends.
func testContext(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	sigCtx, sigCancel := signal.NotifyContext(t.Context(), syscall.SIGINT, syscall.SIGTERM)
	t.Cleanup(sigCancel)
	ctx, cancel := context.WithTimeout(sigCtx, timeout)
	t.Cleanup(cancel)
	return ctx
}

// setupClusterWithTable creates a 2-store-node cluster, creates the named
// table with the given shard count, and waits for shards to become ready.
// Cleanup is registered via t.Cleanup.
func setupClusterWithTable(t *testing.T, ctx context.Context, tableName string, numShards int) *TestCluster {
	t.Helper()
	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:     2,
		NumShards:         numShards,
		ReplicationFactor: 1,
		DisableShardAlloc: false,
	})
	t.Cleanup(cluster.Cleanup)
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: uint(numShards), //nolint:gosec // G115: bounded value, cannot overflow in practice
	})
	require.NoError(t, err)
	err = cluster.WaitForShardsReady(ctx, tableName, numShards, 60*time.Second)
	require.NoError(t, err)
	return cluster
}

// getE2EDir returns the absolute path to the e2e directory using runtime.Caller
// This is more reliable than os.Getwd() which can change during test execution
func getE2EDir() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to get e2e directory via runtime.Caller")
	}
	return filepath.Dir(filename)
}

// GetFreePort returns an available port on localhost
func GetFreePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	// Give OS time to release the port
	time.Sleep(10 * time.Millisecond)

	return port
}

// CreateTestConfig generates a test configuration with temp directories
func CreateTestConfig(t *testing.T, baseDir string, id types.ID) *common.Config {
	t.Helper()

	config := &common.Config{
		SwarmMode:             true,
		DisableShardAlloc:     true,
		ReplicationFactor:     1,
		DefaultShardsPerTable: 1,
		HealthPort:            GetFreePort(t),
		Storage: common.StorageConfig{
			Local: common.LocalStorageConfig{
				BaseDir: baseDir,
			},
			Data:     common.StorageBackendLocal,
			Metadata: common.StorageBackendLocal,
		},
		Metadata: common.MetadataInfo{
			OrchestrationUrls: map[string]string{},
		},
	}

	return config
}

// SkipIfOllamaUnavailable skips the test if Ollama is not available
func SkipIfOllamaUnavailable(t *testing.T) {
	t.Helper()

	if os.Getenv("SKIP_OLLAMA_TESTS") != "" {
		t.Skip("Skipping test that requires Ollama (SKIP_OLLAMA_TESTS is set)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost:11434/api/tags", nil)
	if err != nil {
		t.Skipf("Skipping test that requires Ollama: %v", err)
	}

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		t.Skipf("Skipping test that requires Ollama: Ollama not available at http://localhost:11434 (%v)", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Skipf("Skipping test that requires Ollama: Unexpected status %d", resp.StatusCode)
	}
}

// GetDefaultOllamaConfig returns standard LLM config for testing
func GetDefaultOllamaConfig(t *testing.T) antfly.GeneratorConfig {
	t.Helper()

	ollamaConfig := antfly.OllamaGeneratorConfig{
		Model:       "gemma3:4b",
		Temperature: 0.1,
	}

	config, err := antfly.NewGeneratorConfig(ollamaConfig)
	if err != nil {
		t.Fatalf("Failed to create generator config: %v", err)
	}

	return *config
}

// GetE2EProvider returns the e2e test provider from E2E_PROVIDER env var
func GetE2EProvider() string {
	if provider := os.Getenv("E2E_PROVIDER"); provider != "" {
		return provider
	}
	return "termite" // default - uses local ONNX models via Termite
}

// GetTermiteURL returns the Termite URL from E2E_TERMITE_URL env var
// This should be set by the test after starting the Termite server
func GetTermiteURL() string {
	return os.Getenv("E2E_TERMITE_URL")
}

// SetTermiteURL sets the Termite URL for the test (via environment variable)
func SetTermiteURL(url string) {
	_ = os.Setenv("E2E_TERMITE_URL", url)
}

// GetDefaultEmbedderConfig returns embedder config based on E2E_PROVIDER
func GetDefaultEmbedderConfig(t *testing.T) (*antfly.EmbedderConfig, error) {
	t.Helper()

	switch GetE2EProvider() {
	case "gemini":
		return antfly.NewEmbedderConfig(antfly.GoogleEmbedderConfig{
			Model: "gemini-embedding-001",
		})
	case "termite":
		termiteURL := GetTermiteURL()
		if termiteURL == "" {
			return nil, fmt.Errorf("E2E_TERMITE_URL not set (required for E2E_PROVIDER=termite)")
		}
		// Use full-precision model - quantized (-i8) models cause 200x memory
		// inflation with CoreML on macOS (6-8GB for a 33MB model)
		return antfly.NewEmbedderConfig(antfly.TermiteEmbedderConfig{
			Model:  "BAAI/bge-small-en-v1.5",
			ApiUrl: termiteURL,
		})
	default: // ollama
		return antfly.NewEmbedderConfig(antfly.OllamaEmbedderConfig{
			Model: "embeddinggemma",
		})
	}
}

// GetDefaultGeneratorConfig returns generator config based on E2E_PROVIDER
func GetDefaultGeneratorConfig(t *testing.T) antfly.GeneratorConfig {
	t.Helper()

	var config *antfly.GeneratorConfig
	var err error

	switch GetE2EProvider() {
	case "gemini":
		config, err = antfly.NewGeneratorConfig(antfly.GoogleGeneratorConfig{
			Model:       "gemini-2.5-flash",
			Temperature: 0.1,
		})
	case "termite":
		termiteURL := GetTermiteURL()
		if termiteURL == "" {
			t.Fatalf("E2E_TERMITE_URL not set (required for E2E_PROVIDER=termite)")
		}
		config, err = antfly.NewGeneratorConfig(antfly.TermiteGeneratorConfig{
			Model:       "onnxruntime/Gemma-3-ONNX",
			ApiUrl:      termiteURL,
			Temperature: 0.1,
			MaxTokens:   768, // Lower than production default (2048) to avoid ONNX timeout in tests
		})
	default: // ollama
		config, err = antfly.NewGeneratorConfig(antfly.OllamaGeneratorConfig{
			Model:       "gemma3:4b",
			Temperature: 0.1,
		})
	}

	if err != nil {
		t.Fatalf("Failed to create generator config: %v", err)
	}
	return *config
}

// GetJudgeConfig returns the generator config for evaluation judging.
// Always uses Gemini Flash for consistent, high-quality evaluations.
func GetJudgeConfig(t *testing.T) antfly.GeneratorConfig {
	t.Helper()

	config, err := antfly.NewGeneratorConfig(antfly.GoogleGeneratorConfig{
		Model:       "gemini-2.5-flash",
		Temperature: 0.0, // Use deterministic output for consistent judging
	})
	if err != nil {
		t.Fatalf("Failed to create judge config: %v", err)
	}
	return *config
}

// SkipIfProviderUnavailable skips tests if the required provider is not available
func SkipIfProviderUnavailable(t *testing.T) {
	t.Helper()

	// GEMINI_API_KEY is always required for the eval judge (Gemini Flash)
	if os.Getenv("GEMINI_API_KEY") == "" {
		t.Skip("Skipping test: GEMINI_API_KEY not set (required for eval judge)")
	}

	switch GetE2EProvider() {
	case "gemini":
		// Already checked above
		return
	case "termite":
		// Termite is started by the test harness, no external check needed
		// The test will set E2E_TERMITE_URL after starting the server
		return
	default: // ollama
		SkipIfOllamaUnavailable(t)
	}
}

// CleanupAntflyData removes raft logs and databases from a directory
func CleanupAntflyData(t *testing.T, baseDir string, nodeID types.ID) {
	t.Helper()

	// Remove metadata raft logs
	metadataLogPath := filepath.Join(baseDir, "metadata", nodeID.String(), "log")
	if err := os.RemoveAll(metadataLogPath); err != nil && !os.IsNotExist(err) {
		t.Logf("Warning: Failed to remove metadata logs: %v", err)
	}

	// Remove store raft logs
	storeDir := filepath.Join(baseDir, "store", nodeID.String())
	if entries, err := os.ReadDir(storeDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				logPath := filepath.Join(storeDir, entry.Name(), "log")
				if entry.Name() == "storeMetadataDB" {
					logPath = filepath.Join(storeDir, entry.Name())
				}
				if err := os.RemoveAll(logPath); err != nil && !os.IsNotExist(err) {
					t.Logf("Warning: Failed to remove store logs at %s: %v", logPath, err)
				}
			}
		}
	}
}

// GetE2ELogLevel returns the log level from E2E_LOG_LEVEL env var.
// Defaults to InfoLevel. Set E2E_LOG_LEVEL=debug for verbose output.
func GetE2ELogLevel() zapcore.Level {
	levelStr := os.Getenv("E2E_LOG_LEVEL")
	if levelStr == "" {
		return zapcore.InfoLevel // Default to info for less noise
	}

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(levelStr)); err != nil {
		return zapcore.InfoLevel
	}
	return level
}

// GetTestLogger returns a logger suitable for testing with logfmt format.
// Log level is controlled by E2E_LOG_LEVEL env var (default: info).
// Set E2E_LOG_LEVEL=debug for full debug output.
func GetTestLogger(t *testing.T) *zap.Logger {
	t.Helper()

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "ts",
		LevelKey:      "lvl",
		NameKey:       "logger",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
	}

	core := zapcore.NewCore(
		logging.NewLogfmtEncoder(encoderConfig),
		zapcore.AddSync(zaptest.NewTestingWriter(t)),
		GetE2ELogLevel(),
	)

	return zap.New(core, zap.AddCaller())
}

// WaitForHTTP waits for an HTTP endpoint to respond
func WaitForHTTP(t *testing.T, ctx context.Context, url string, timeout time.Duration) error {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for %s", url)
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for %s to respond", url)
			}

			reqCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			req, err := http.NewRequestWithContext(reqCtx, "GET", url+"/status", nil)
			cancel()
			if err != nil {
				continue
			}

			resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
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

// GetBackupDir returns the absolute path to the e2e backups directory
func GetBackupDir(t *testing.T) string {
	t.Helper()

	backupDir := filepath.Join(getE2EDir(), "backups")

	// Ensure backup directory exists
	if err := os.MkdirAll(backupDir, 0755); err != nil { //nolint:gosec // G301: standard permissions for data directory
		t.Fatalf("Failed to create backup directory: %v", err)
	}

	return backupDir
}

// ShouldRestoreFromBackup checks if the RESTORE_DB environment variable is set
func ShouldRestoreFromBackup() bool {
	return os.Getenv("RESTORE_DB") != ""
}

// BackupExists checks if a backup with the given ID exists
func BackupExists(t *testing.T, backupID string) bool {
	t.Helper()

	backupDir := GetBackupDir(t)
	metadataFile := filepath.Join(backupDir, fmt.Sprintf("%s-metadata.json", backupID))

	_, err := os.Stat(metadataFile)
	return err == nil
}

// BackupTestDatabase backs up a table to the e2e backups directory
func BackupTestDatabase(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName, backupID string) error {
	t.Helper()

	backupDir := GetBackupDir(t)
	location := fmt.Sprintf("file://%s", backupDir)

	t.Logf("Creating backup '%s' for table '%s' to location: %s", backupID, tableName, location)

	err := client.Backup(ctx, tableName, backupID, location)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	t.Logf("Backup '%s' created successfully", backupID)
	return nil
}

// RestoreTestDatabase restores a table from a backup in the e2e backups directory
func RestoreTestDatabase(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName, backupID string) error {
	t.Helper()

	backupDir := GetBackupDir(t)
	location := fmt.Sprintf("file://%s", backupDir)

	if !BackupExists(t, backupID) {
		return fmt.Errorf("backup '%s' does not exist in %s", backupID, backupDir)
	}

	t.Logf("Restoring table '%s' from backup '%s' at location: %s", tableName, backupID, location)

	err := client.Restore(ctx, tableName, backupID, location)
	if err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

	t.Logf("Restore triggered successfully for backup '%s', waiting for completion...", backupID)

	// Wait for restore to complete by checking table readiness
	// Restore is asynchronous, so we need to poll until the table is ready
	deadline := time.Now().Add(5 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for restore to complete")
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for restore to complete")
			}

			// Check if table is accessible by trying to get table info
			_, err := client.GetTable(ctx, tableName)
			if err == nil {
				t.Logf("Restore completed successfully for backup '%s'", backupID)
				return nil
			}

			t.Logf("Waiting for table to become ready after restore... (%v)", err)
		}
	}
}

// GetE2EResultsDir returns the path to the e2e/test_results directory
func GetE2EResultsDir() string {
	return filepath.Join(getE2EDir(), "test_results")
}

// ---- PostgreSQL / CDC test helpers ----

const (
	pgTestTable = "antfly_e2e_customers"
)

// pgDSN returns the PostgreSQL connection string for the local Homebrew install.
// Override with ANTFLY_E2E_PG_DSN if needed.
func pgDSN() string {
	if dsn := os.Getenv("ANTFLY_E2E_PG_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://localhost:5432/postgres?sslmode=disable"
}

// skipIfPostgresUnavailable skips the test when a local Postgres cannot be reached.
func skipIfPostgresUnavailable(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgDSN())
	if err != nil {
		t.Skipf("Skipping: PostgreSQL not available at %s (%v)", pgDSN(), err)
	}
	_ = conn.Close(ctx)
}

// skipIfWalLevelNotLogical skips the test when PostgreSQL's wal_level is not "logical".
func skipIfWalLevelNotLogical(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgDSN())
	if err != nil {
		t.Skipf("Skipping: cannot connect to PostgreSQL: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var walLevel string
	err = conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		t.Skipf("Skipping: cannot query wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Skipf("Skipping: wal_level=%q, need \"logical\". Run: ALTER SYSTEM SET wal_level = logical; then restart PostgreSQL", walLevel)
	}
}

// setupPGTestData creates a test table in PostgreSQL and populates it with
// sample customer data. Returns a cleanup function.
func setupPGTestData(t *testing.T, ctx context.Context) func() {
	t.Helper()

	conn, err := pgx.Connect(ctx, pgDSN())
	require.NoError(t, err, "connecting to PostgreSQL")

	// Drop table if left over from a previous failed run
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgTestTable))

	// Create and populate.
	// Use TEXT for customer_id to match Antfly's JSON string representation,
	// avoiding type mismatch (float64 vs int) during join value comparison.
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		customer_id TEXT PRIMARY KEY,
		name        TEXT NOT NULL,
		email       TEXT NOT NULL,
		tier        TEXT NOT NULL
	)`, pgTestTable))
	require.NoError(t, err, "creating PG test table")

	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (customer_id, name, email, tier) VALUES
		('cust-1', 'Alice',   'alice@example.com',   'gold'),
		('cust-2', 'Bob',     'bob@example.com',     'silver'),
		('cust-3', 'Charlie', 'charlie@example.com', 'gold'),
		('cust-4', 'Diana',   'diana@example.com',   'bronze'),
		('cust-5', 'Eve',     'eve@example.com',     'silver')
	`, pgTestTable))
	require.NoError(t, err, "inserting PG test data")

	_ = conn.Close(ctx)

	return func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := pgx.Connect(cleanCtx, pgDSN())
		if err != nil {
			t.Logf("Warning: could not connect to PG for cleanup: %v", err)
			return
		}
		defer func() { _ = c.Close(cleanCtx) }()
		_, _ = c.Exec(cleanCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgTestTable))
	}
}

// foreignSource returns an antfly.ForeignSource pointing at the local PG test table.
func foreignSource() antfly.ForeignSource {
	return antfly.ForeignSource{
		Type:          antfly.ForeignSourceTypePostgres,
		Dsn:           pgDSN(),
		PostgresTable: pgTestTable,
		Columns: []antfly.ForeignColumn{
			{Name: "customer_id", Type: "text"},
			{Name: "name", Type: "text"},
			{Name: "email", Type: "text"},
			{Name: "tier", Type: "text"},
		},
	}
}

// cdcCleanupPG drops a PG table, replication slot, and publication created during a CDC test.
func cdcCleanupPG(t *testing.T, tableName, slotName, pubName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgDSN())
	if err != nil {
		t.Logf("Warning: could not connect to PG for CDC cleanup: %v", err)
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	// Drop table
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Drop replication slot (must be inactive first)
	_, _ = conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slotName)

	// Drop publication
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
}

// waitForKeyAvailable polls until a key is available (reusable for swarm tests).
func waitForKeyAvailable(t *testing.T, ctx context.Context, client *antfly.AntflyClient, table, key string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for key %s in table %s", key, table)
			}
			_, err := client.LookupKey(ctx, table, key)
			if err == nil {
				return nil
			}
		}
	}
}

// waitForFieldValue polls until a document's field matches the expected value.
func waitForFieldValue(t *testing.T, ctx context.Context, client *antfly.AntflyClient, table, key, field string, expected any, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	var lastVal any
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for %s.%s=%v in table %s (last value: %v)", key, field, expected, table, lastVal)
			}
			doc, err := client.LookupKey(ctx, table, key)
			if err != nil {
				continue
			}
			lastVal = doc[field]
			if fmt.Sprintf("%v", lastVal) == fmt.Sprintf("%v", expected) {
				return nil
			}
		}
	}
}

// waitForKeyGone polls until LookupKey returns an error (document deleted).
func waitForKeyGone(t *testing.T, ctx context.Context, client *antfly.AntflyClient, table, key string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for key %s to be deleted from table %s", key, table)
			}
			_, err := client.LookupKey(ctx, table, key)
			if err != nil {
				return nil // key is gone
			}
		}
	}
}

// waitForFieldGone polls until a document's field is nil/absent.
func waitForFieldGone(t *testing.T, ctx context.Context, client *antfly.AntflyClient, table, key, field string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for field %s to be removed from %s in table %s", field, key, table)
			}
			doc, err := client.LookupKey(ctx, table, key)
			if err != nil {
				// Document gone entirely — field is certainly gone
				return nil
			}
			if doc[field] == nil {
				return nil
			}
		}
	}
}
