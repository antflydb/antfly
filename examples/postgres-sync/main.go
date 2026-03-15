package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ANCHOR: config_type
// Config holds the sync configuration
type Config struct {
	PostgresURL       string
	AntflyURL         string
	TableName         string
	IDColumn          string
	DataColumn        string
	AntflyTable       string
	FullSyncInterval  time.Duration
	BatchSize         int
	CreateTable       bool
	ReplicationFactor int
}

// ANCHOR_END: config_type

// ANCHOR: postgres_sync_type
// PostgresSync manages syncing Postgres JSONB data to Antfly
type PostgresSync struct {
	config       Config
	pgPool       *pgxpool.Pool
	antflyClient *antfly.AntflyClient
	lastSyncTime time.Time
	stats        SyncStats
}

// ANCHOR_END: postgres_sync_type

// ANCHOR: sync_stats_type
// SyncStats tracks sync statistics
type SyncStats struct {
	mu               sync.RWMutex
	TotalSynced      int64
	TotalSkipped     int64
	TotalDeleted     int64
	TotalErrors      int64
	LastFullSync     time.Time
	LastRealtimeSync time.Time
	RealtimeUpdates  int64
}

// ANCHOR_END: sync_stats_type

// ANCHOR: change_event_type
// ChangeEvent represents a database change notification
type ChangeEvent struct {
	Operation string         `json:"operation"` // INSERT, UPDATE, DELETE
	ID        string         `json:"id"`
	Data      map[string]any `json:"data,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

// ANCHOR_END: change_event_type

func main() {
	config := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create sync manager
	sync, err := NewPostgresSync(ctx, config)
	if err != nil {
		log.Fatalf("Failed to create sync manager: %v", err)
	}
	defer sync.Close()

	fmt.Printf("=== Postgres to Antfly Real-time Sync ===\n")
	fmt.Printf("Postgres: %s\n", maskPassword(config.PostgresURL))
	fmt.Printf("Antfly: %s\n", config.AntflyURL)
	fmt.Printf("Table: %s.%s -> %s\n", config.TableName, config.DataColumn, config.AntflyTable)
	fmt.Printf("Full sync interval: %v\n", config.FullSyncInterval)
	fmt.Printf("Batch size: %d\n\n", config.BatchSize)

	// Initial full sync
	fmt.Println("Performing initial full sync...")
	if err := sync.FullSync(ctx); err != nil {
		log.Fatalf("Initial sync failed: %v", err)
	}
	fmt.Println("✓ Initial sync complete")
	fmt.Println()

	// Start real-time sync
	fmt.Println("Starting real-time sync (LISTEN/NOTIFY)...")
	go func() {
		if err := sync.StartRealtimeSync(ctx); err != nil {
			log.Printf("Real-time sync error: %v", err)
		}
	}()

	// Start periodic full sync
	if config.FullSyncInterval > 0 {
		go func() {
			ticker := time.NewTicker(config.FullSyncInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					fmt.Println("\n--- Periodic full sync starting ---")
					if err := sync.FullSync(ctx); err != nil {
						log.Printf("Periodic sync failed: %v", err)
					}
					fmt.Println("--- Periodic full sync complete ---")
					fmt.Println()
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Stats reporter
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				sync.PrintStats()
			case <-ctx.Done():
				return
			}
		}
	}()

	fmt.Println("✓ Real-time sync active")
	fmt.Println("\nSync is running. Press Ctrl+C to stop.")
	fmt.Println()

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\n\nShutting down gracefully...")
	cancel()

	// Give time for cleanup
	time.Sleep(1 * time.Second)
	sync.PrintStats()
	fmt.Println("\n✓ Sync stopped")
}

// NewPostgresSync creates a new sync manager
func NewPostgresSync(ctx context.Context, config Config) (*PostgresSync, error) {
	// Create Postgres connection pool
	pgConfig, err := pgxpool.ParseConfig(config.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("invalid postgres URL: %w", err)
	}

	pgConfig.MaxConns = 10
	pgConfig.MinConns = 2

	pgPool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Postgres: %w", err)
	}

	// Test connection
	if err := pgPool.Ping(ctx); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("failed to ping Postgres: %w", err)
	}

	// Create Antfly client
	antflyClient, err := antfly.NewAntflyClient(config.AntflyURL, http.DefaultClient)
	if err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("failed to create Antfly client: %w", err)
	}

	// Create table if requested
	if config.CreateTable {
		err := antflyClient.CreateTable(ctx, config.AntflyTable, antfly.CreateTableRequest{
			NumShards: uint(config.ReplicationFactor),
		})
		if err != nil {
			log.Printf("Warning: Failed to create table (may already exist): %v", err)
		} else {
			fmt.Printf("✓ Created Antfly table '%s'\n", config.AntflyTable)
		}
	}

	return &PostgresSync{
		config:       config,
		pgPool:       pgPool,
		antflyClient: antflyClient,
		lastSyncTime: time.Now(),
	}, nil
}

// Close cleans up resources
func (ps *PostgresSync) Close() {
	if ps.pgPool != nil {
		ps.pgPool.Close()
	}
}

// ANCHOR: full_sync
// FullSync performs a complete sync of all data from Postgres to Antfly
func (ps *PostgresSync) FullSync(ctx context.Context) error {
	startTime := time.Now()

	// Query all records from Postgres
	query := fmt.Sprintf(`
		SELECT %s, %s
		FROM %s
		ORDER BY %s
	`, ps.config.IDColumn, ps.config.DataColumn, ps.config.TableName, ps.config.IDColumn)

	rows, err := ps.pgPool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query Postgres: %w", err)
	}
	defer rows.Close()

	// Collect records in batches
	var records []struct {
		ID   string
		Data map[string]any
	}

	for rows.Next() {
		var id string
		var data []byte

		if err := rows.Scan(&id, &data); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		var jsonData map[string]any
		if err := json.Unmarshal(data, &jsonData); err != nil {
			log.Printf("Warning: Failed to parse JSON for ID %s: %v", id, err)
			continue
		}

		records = append(records, struct {
			ID   string
			Data map[string]any
		}{ID: id, Data: jsonData})
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	fmt.Printf("Full sync: Found %d records in Postgres\n", len(records))

	if len(records) == 0 {
		fmt.Println("No records to sync")
		ps.stats.mu.Lock()
		ps.stats.LastFullSync = time.Now()
		ps.stats.mu.Unlock()
		return nil
	}

	// Sync in batches using Linear Merge
	totalUpserted := 0
	totalSkipped := 0
	totalDeleted := 0
	cursor := ""

	for i := 0; i < len(records); i += ps.config.BatchSize {
		end := min(i+ps.config.BatchSize, len(records))
		batch := records[i:end]

		// Convert to Antfly records map
		antflyRecords := make(map[string]any)
		for _, rec := range batch {
			// Add metadata
			doc := make(map[string]any)
			doc["id"] = rec.ID
			doc["data"] = rec.Data
			doc["source"] = "postgres"
			doc["synced_at"] = time.Now().Format(time.RFC3339)

			antflyRecords[rec.ID] = doc
		}

		// Perform linear merge
		result, err := ps.antflyClient.LinearMerge(
			ctx,
			ps.config.AntflyTable,
			antfly.LinearMergeRequest{
				Records:      antflyRecords,
				LastMergedId: cursor,
				DryRun:       false,
			},
		)
		if err != nil {
			ps.stats.mu.Lock()
			ps.stats.TotalErrors++
			ps.stats.mu.Unlock()
			return fmt.Errorf("linear merge failed: %w", err)
		}

		totalUpserted += result.Upserted
		totalSkipped += result.Skipped
		totalDeleted += result.Deleted

		// Update cursor for next batch
		if result.NextCursor != "" {
			cursor = result.NextCursor
		} else {
			// Find max ID in batch
			maxID := ""
			for id := range antflyRecords {
				if id > maxID {
					maxID = id
				}
			}
			cursor = maxID
		}

		fmt.Printf("  Batch %d-%d: %d upserted, %d skipped, %d deleted\n",
			i+1, end, result.Upserted, result.Skipped, result.Deleted)
	}

	duration := time.Since(startTime)

	fmt.Printf("✓ Full sync complete: %d upserted, %d skipped, %d deleted in %v\n",
		totalUpserted, totalSkipped, totalDeleted, duration)

	// Update stats
	ps.stats.mu.Lock()
	ps.stats.TotalSynced += int64(totalUpserted)
	ps.stats.TotalSkipped += int64(totalSkipped)
	ps.stats.TotalDeleted += int64(totalDeleted)
	ps.stats.LastFullSync = time.Now()
	ps.stats.mu.Unlock()

	return nil
}

// ANCHOR_END: full_sync

// ANCHOR: realtime_sync
// StartRealtimeSync starts listening for Postgres notifications
func (ps *PostgresSync) StartRealtimeSync(ctx context.Context) error {
	// Use a dedicated connection for LISTEN
	conn, err := pgx.Connect(ctx, ps.config.PostgresURL)
	if err != nil {
		return fmt.Errorf("failed to create listen connection: %w", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Printf("Warning: Failed to close connection: %v", err)
		}
	}()

	// Start listening for notifications
	channelName := ps.config.TableName + "_changes"
	_, err = conn.Exec(ctx, "LISTEN "+pgx.Identifier{channelName}.Sanitize())
	if err != nil {
		return fmt.Errorf("failed to LISTEN: %w", err)
	}

	fmt.Printf("✓ Listening on channel '%s'\n", channelName)

	// Buffer for batching rapid changes
	changeBatch := make(map[string]ChangeEvent)
	var batchMu sync.Mutex
	batchTicker := time.NewTicker(1 * time.Second)
	defer batchTicker.Stop()

	// Process batched changes
	processBatch := func() {
		batchMu.Lock()
		if len(changeBatch) == 0 {
			batchMu.Unlock()
			return
		}

		// Copy and clear batch
		toProcess := make(map[string]ChangeEvent, len(changeBatch))
		maps.Copy(toProcess, changeBatch)
		changeBatch = make(map[string]ChangeEvent)
		batchMu.Unlock()

		// Process the batch
		if err := ps.processBatchedChanges(ctx, toProcess); err != nil {
			log.Printf("Error processing batch: %v", err)
			ps.stats.mu.Lock()
			ps.stats.TotalErrors++
			ps.stats.mu.Unlock()
		}
	}

	// Goroutine to process batches periodically
	go func() {
		for {
			select {
			case <-batchTicker.C:
				processBatch()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Listen for notifications
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // Context cancelled, clean exit
			}
			return fmt.Errorf("notification error: %w", err)
		}

		// Parse the notification payload
		var event ChangeEvent
		if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
			log.Printf("Warning: Failed to parse notification: %v", err)
			continue
		}

		// Add to batch
		batchMu.Lock()
		changeBatch[event.ID] = event
		batchMu.Unlock()

		fmt.Printf("← Change detected: %s %s\n", event.Operation, event.ID)
	}
}

// ANCHOR_END: realtime_sync

// ANCHOR: process_batched_changes
// processBatchedChanges syncs a batch of changes to Antfly
func (ps *PostgresSync) processBatchedChanges(
	ctx context.Context,
	changes map[string]ChangeEvent,
) error {
	if len(changes) == 0 {
		return nil
	}

	// Separate into upserts and deletes
	var deletes []string
	records := make(map[string]any)

	for id, event := range changes {
		if event.Operation == "DELETE" {
			deletes = append(deletes, id)
		} else {
			// For INSERT/UPDATE, fetch current data from Postgres
			// (the notification might not contain the full data)
			query := fmt.Sprintf(`SELECT %s FROM %s WHERE %s = $1`,
				ps.config.DataColumn, ps.config.TableName, ps.config.IDColumn)

			var data []byte
			err := ps.pgPool.QueryRow(ctx, query, id).Scan(&data)
			if err != nil {
				if err == pgx.ErrNoRows {
					// Record was deleted between notification and now
					deletes = append(deletes, id)
					continue
				}
				log.Printf("Warning: Failed to fetch data for %s: %v", id, err)
				continue
			}

			var jsonData map[string]any
			if err := json.Unmarshal(data, &jsonData); err != nil {
				log.Printf("Warning: Failed to parse JSON for %s: %v", id, err)
				continue
			}

			doc := make(map[string]any)
			doc["id"] = id
			doc["data"] = jsonData
			doc["source"] = "postgres"
			doc["synced_at"] = time.Now().Format(time.RFC3339)

			records[id] = doc
		}
	}

	// Handle upserts via Linear Merge
	if len(records) > 0 {
		// Sort IDs to determine range
		ids := make([]string, 0, len(records))
		for id := range records {
			ids = append(ids, id)
		}
		slices.Sort(ids)

		result, err := ps.antflyClient.LinearMerge(
			ctx,
			ps.config.AntflyTable,
			antfly.LinearMergeRequest{
				Records:      records,
				LastMergedId: "",
				DryRun:       false,
			},
		)
		if err != nil {
			return fmt.Errorf("linear merge failed: %w", err)
		}

		fmt.Printf("→ Real-time sync: %d upserted, %d skipped\n",
			result.Upserted, result.Skipped)

		ps.stats.mu.Lock()
		ps.stats.TotalSynced += int64(result.Upserted)
		ps.stats.TotalSkipped += int64(result.Skipped)
		ps.stats.RealtimeUpdates += int64(len(records))
		ps.stats.LastRealtimeSync = time.Now()
		ps.stats.mu.Unlock()
	}

	// Handle deletes
	if len(deletes) > 0 {
		// Use Batch API to delete multiple documents at once
		batchResult, err := ps.antflyClient.Batch(ctx, ps.config.AntflyTable, antfly.BatchRequest{
			Deletes: deletes,
		})
		if err != nil {
			return fmt.Errorf("batch delete failed: %w", err)
		}

		deletedCount := batchResult.Deleted
		if len(batchResult.Failed) > 0 {
			log.Printf("Warning: %d delete operations failed", len(batchResult.Failed))
			for _, failed := range batchResult.Failed {
				log.Printf("  Failed to delete %s: %s", failed.Id, failed.Error)
			}
		}

		fmt.Printf("→ Real-time sync: %d deleted\n", deletedCount)

		ps.stats.mu.Lock()
		ps.stats.TotalDeleted += int64(deletedCount)
		ps.stats.RealtimeUpdates += int64(deletedCount)
		ps.stats.LastRealtimeSync = time.Now()
		ps.stats.mu.Unlock()
	}

	return nil
}

// ANCHOR_END: process_batched_changes

// PrintStats prints current sync statistics
func (ps *PostgresSync) PrintStats() {
	ps.stats.mu.RLock()
	defer ps.stats.mu.RUnlock()

	fmt.Printf("\n--- Sync Statistics ---\n")
	fmt.Printf("Total synced: %d\n", ps.stats.TotalSynced)
	fmt.Printf("Total skipped: %d\n", ps.stats.TotalSkipped)
	fmt.Printf("Total deleted: %d\n", ps.stats.TotalDeleted)
	fmt.Printf("Real-time updates: %d\n", ps.stats.RealtimeUpdates)
	fmt.Printf("Errors: %d\n", ps.stats.TotalErrors)
	if !ps.stats.LastFullSync.IsZero() {
		fmt.Printf("Last full sync: %v ago\n", time.Since(ps.stats.LastFullSync).Round(time.Second))
	}
	if !ps.stats.LastRealtimeSync.IsZero() {
		fmt.Printf(
			"Last real-time sync: %v ago\n",
			time.Since(ps.stats.LastRealtimeSync).Round(time.Second),
		)
	}
	fmt.Printf("----------------------\n\n")
}

func parseFlags() Config {
	config := Config{}

	flag.StringVar(
		&config.PostgresURL,
		"postgres",
		os.Getenv("POSTGRES_URL"),
		"Postgres connection URL",
	)
	flag.StringVar(&config.AntflyURL, "antfly", "http://localhost:8080/api/v1", "Antfly API URL")
	flag.StringVar(&config.TableName, "pg-table", "documents", "Postgres table name")
	flag.StringVar(&config.IDColumn, "id-column", "id", "ID column name")
	flag.StringVar(&config.DataColumn, "data-column", "data", "JSONB data column name")
	flag.StringVar(&config.AntflyTable, "antfly-table", "postgres_docs", "Antfly table name")
	flag.DurationVar(
		&config.FullSyncInterval,
		"full-sync-interval",
		5*time.Minute,
		"Full sync interval (0 to disable)",
	)
	flag.IntVar(&config.BatchSize, "batch-size", 1000, "Batch size for sync")
	flag.BoolVar(
		&config.CreateTable,
		"create-table",
		false,
		"Create Antfly table if it doesn't exist",
	)
	flag.IntVar(&config.ReplicationFactor, "num-shards", 3, "Number of shards for new table")

	flag.Parse()

	if config.PostgresURL == "" {
		log.Fatal("Error: --postgres or POSTGRES_URL environment variable is required")
	}

	return config
}

func maskPassword(url string) string {
	// Simple password masking for display
	if idx := len(url); idx > 50 {
		return url[:30] + "..." + url[idx-10:]
	}
	return url
}
