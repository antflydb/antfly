package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/docsaf"
)

// StringSliceFlag allows repeated flags to build a slice
type StringSliceFlag []string

func (s *StringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *StringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// ANCHOR: prepare_cmd
func prepareCmd(args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ExitOnError)
	dirPath := fs.String("dir", "", "Path to directory containing documentation files (required)")
	outputFile := fs.String("output", "docs.json", "Output JSON file path")
	baseURL := fs.String("base-url", "", "Base URL for generating document links (optional)")

	var includePatterns StringSliceFlag
	var excludePatterns StringSliceFlag
	fs.Var(&includePatterns, "include", "Include pattern (can be repeated, supports ** wildcards)")
	fs.Var(&excludePatterns, "exclude", "Exclude pattern (can be repeated, supports ** wildcards)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *dirPath == "" {
		return fmt.Errorf("--dir flag is required")
	}

	// Verify path exists and is a directory
	fileInfo, err := os.Stat(*dirPath)
	if err != nil {
		return fmt.Errorf("failed to access path: %w", err)
	}

	if !fileInfo.IsDir() {
		return fmt.Errorf("--dir must be a directory")
	}

	fmt.Printf("=== docsaf prepare - Process Documentation Files ===\n")
	fmt.Printf("Directory: %s\n", *dirPath)
	fmt.Printf("Output: %s\n", *outputFile)
	if len(includePatterns) > 0 {
		fmt.Printf("Include patterns: %v\n", includePatterns)
	}
	if len(excludePatterns) > 0 {
		fmt.Printf("Exclude patterns: %v\n", excludePatterns)
	}
	fmt.Printf("\n")

	// Create filesystem source and processor using library
	source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
		BaseDir:         *dirPath,
		BaseURL:         *baseURL,
		IncludePatterns: includePatterns,
		ExcludePatterns: excludePatterns,
	})
	processor := docsaf.NewProcessor(source, docsaf.DefaultRegistry())

	// Process all files in the directory
	fmt.Printf("Processing documentation files (chunking by markdown headings)...\n")
	sections, err := processor.Process(context.Background())
	if err != nil {
		return fmt.Errorf("failed to process directory: %w", err)
	}

	fmt.Printf("✓ Found %d documents\n\n", len(sections))

	if len(sections) == 0 {
		return fmt.Errorf("no supported files found in directory")
	}

	// Count sections by type
	typeCounts := make(map[string]int)
	for _, section := range sections {
		typeCounts[section.Type]++
	}

	fmt.Printf("Document types found:\n")
	for docType, count := range typeCounts {
		fmt.Printf("  - %s: %d\n", docType, count)
	}
	fmt.Printf("\n")

	// Show sample of documents
	fmt.Printf("Sample documents:\n")
	for i, section := range sections {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(sections)-i)
			break
		}
		fmt.Printf("  [%d] %s (%s) - %s\n",
			i+1, section.Title, section.Type, section.FilePath)
	}
	fmt.Printf("\n")

	// Convert sections to records map (sorted by ID for consistent ordering)
	records := make(map[string]any)
	for _, section := range sections {
		records[section.ID] = section.ToDocument()
	}

	// Write to JSON file
	fmt.Printf("Writing %d records to %s...\n", len(records), *outputFile)
	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	err = os.WriteFile(*outputFile, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("✓ Prepared data written to %s\n", *outputFile)
	return nil
}

// ANCHOR_END: prepare_cmd

// ANCHOR: load_cmd
func loadCmd(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/api/v1", "Antfly API URL")
	tableName := fs.String("table", "docs", "Table name to merge into")
	inputFile := fs.String("input", "docs.json", "Input JSON file path")
	dryRun := fs.Bool("dry-run", false, "Preview changes without applying them")
	createTable := fs.Bool("create-table", false, "Create table if it doesn't exist")
	numShards := fs.Int("num-shards", 1, "Number of shards for new table")
	batchSize := fs.Int("batch-size", 25, "Linear merge batch size")
	embeddingModel := fs.String("embedding-model", "embeddinggemma", "Embedding model to use (e.g., embeddinggemma)")
	chunkerModel := fs.String("chunker-model", "fixed-bert-tokenizer", "Chunker model: fixed-bert-tokenizer, fixed-bpe-tokenizer, or any ONNX model directory name")
	targetTokens := fs.Int("target-tokens", 512, "Target tokens for chunking")
	overlapTokens := fs.Int("overlap-tokens", 50, "Overlap tokens for chunking")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	ctx := context.Background()

	// Create Antfly client
	client, err := antfly.NewAntflyClient(*antflyURL, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create Antfly client: %w", err)
	}

	fmt.Printf("=== docsaf load - Load Data to Antfly ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Input: %s\n", *inputFile)
	fmt.Printf("Dry run: %v\n\n", *dryRun)

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s' with %d shards...\n", *tableName, *numShards)

		// Create embedding index configuration
		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index config: %w", err)
		}

		err = client.CreateTable(ctx, *tableName, antfly.CreateTableRequest{
			NumShards: uint(*numShards),
			Indexes: map[string]antfly.IndexConfig{
				"embeddings": *embeddingIndex,
			},
		})
		if err != nil {
			log.Printf("Warning: Failed to create table (may already exist): %v\n", err)
		} else {
			fmt.Printf("✓ Table created with BM25 and embedding indexes (%s with %s chunking)\n\n",
				*embeddingModel, *chunkerModel)
		}

		// Wait for shards to be ready
		if err := waitForShardsReady(ctx, client, *tableName, 30*time.Second); err != nil {
			return fmt.Errorf("error waiting for shards: %w", err)
		}
	}

	// Read JSON file
	fmt.Printf("Reading records from %s...\n", *inputFile)
	jsonData, err := os.ReadFile(*inputFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var records map[string]any
	err = json.Unmarshal(jsonData, &records)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	fmt.Printf("✓ Loaded %d records\n\n", len(records))

	// Perform batched linear merge
	finalCursor, err := performBatchedLinearMerge(ctx, client, *tableName, records, *batchSize, *dryRun)
	if err != nil {
		return fmt.Errorf("batched linear merge failed: %w", err)
	}

	// Final cleanup: delete any remaining documents beyond the last cursor
	if finalCursor != "" && !*dryRun {
		fmt.Printf("\nPerforming final cleanup to remove orphaned documents...\n")
		cleanupResult, err := client.LinearMerge(ctx, *tableName, antfly.LinearMergeRequest{
			Records:      map[string]any{}, // Empty records
			LastMergedId: finalCursor,      // Start from last cursor
			DryRun:       false,
			SyncLevel:    antfly.SyncLevelAknn,
		})
		if err != nil {
			return fmt.Errorf("final cleanup failed: %w", err)
		}
		fmt.Printf("✓ Final cleanup completed in %s\n", cleanupResult.Took)
		fmt.Printf("  Deleted: %d orphaned documents\n", cleanupResult.Deleted)
	}

	fmt.Printf("\n✓ Load completed successfully\n")
	return nil
}

// ANCHOR_END: load_cmd

// ANCHOR: sync_cmd
func syncCmd(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/api/v1", "Antfly API URL")
	tableName := fs.String("table", "docs", "Table name to merge into")
	dirPath := fs.String("dir", "", "Path to directory containing documentation files (required)")
	baseURL := fs.String("base-url", "", "Base URL for generating document links (optional)")
	dryRun := fs.Bool("dry-run", false, "Preview changes without applying them")
	createTable := fs.Bool("create-table", false, "Create table if it doesn't exist")
	numShards := fs.Int("num-shards", 1, "Number of shards for new table")
	batchSize := fs.Int("batch-size", 25, "Linear merge batch size")
	embeddingModel := fs.String("embedding-model", "embeddinggemma", "Embedding model to use (e.g., embeddinggemma)")
	chunkerModel := fs.String("chunker-model", "fixed-bert-tokenizer", "Chunker model: fixed-bert-tokenizer, fixed-bpe-tokenizer, or any ONNX model directory name")
	targetTokens := fs.Int("target-tokens", 512, "Target tokens for chunking")
	overlapTokens := fs.Int("overlap-tokens", 50, "Overlap tokens for chunking")

	var includePatterns StringSliceFlag
	var excludePatterns StringSliceFlag
	fs.Var(&includePatterns, "include", "Include pattern (can be repeated, supports ** wildcards)")
	fs.Var(&excludePatterns, "exclude", "Exclude pattern (can be repeated, supports ** wildcards)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *dirPath == "" {
		return fmt.Errorf("--dir flag is required")
	}

	ctx := context.Background()

	// Create Antfly client
	client, err := antfly.NewAntflyClient(*antflyURL, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create Antfly client: %w", err)
	}

	fmt.Printf("=== docsaf sync - Full Pipeline ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Directory: %s\n", *dirPath)
	fmt.Printf("Dry run: %v\n", *dryRun)
	if len(includePatterns) > 0 {
		fmt.Printf("Include patterns: %v\n", includePatterns)
	}
	if len(excludePatterns) > 0 {
		fmt.Printf("Exclude patterns: %v\n", excludePatterns)
	}
	fmt.Printf("\n")

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s' with %d shards...\n", *tableName, *numShards)

		// Create embedding index configuration
		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index config: %w", err)
		}

		err = client.CreateTable(ctx, *tableName, antfly.CreateTableRequest{
			NumShards: uint(*numShards),
			Indexes: map[string]antfly.IndexConfig{
				"embeddings": *embeddingIndex,
			},
		})
		if err != nil {
			log.Printf("Warning: Failed to create table (may already exist): %v\n", err)
		} else {
			fmt.Printf("✓ Table created with BM25 and embedding indexes (%s with %s chunking)\n\n",
				*embeddingModel, *chunkerModel)
		}

		// Wait for shards to be ready
		if err := waitForShardsReady(ctx, client, *tableName, 30*time.Second); err != nil {
			return fmt.Errorf("error waiting for shards: %w", err)
		}
	}

	// Verify path exists and is a directory
	fileInfo, err := os.Stat(*dirPath)
	if err != nil {
		return fmt.Errorf("failed to access path: %w", err)
	}

	if !fileInfo.IsDir() {
		return fmt.Errorf("--dir must be a directory")
	}

	// Create filesystem source and processor using library
	source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
		BaseDir:         *dirPath,
		BaseURL:         *baseURL,
		IncludePatterns: includePatterns,
		ExcludePatterns: excludePatterns,
	})
	processor := docsaf.NewProcessor(source, docsaf.DefaultRegistry())

	// Process all files in the directory
	fmt.Printf("Processing documentation files (chunking by markdown headings)...\n")
	sections, err := processor.Process(context.Background())
	if err != nil {
		return fmt.Errorf("failed to process directory: %w", err)
	}

	fmt.Printf("✓ Found %d documents\n\n", len(sections))

	if len(sections) == 0 {
		return fmt.Errorf("no supported files found in directory")
	}

	// Count sections by type
	typeCounts := make(map[string]int)
	for _, section := range sections {
		typeCounts[section.Type]++
	}

	fmt.Printf("Document types found:\n")
	for docType, count := range typeCounts {
		fmt.Printf("  - %s: %d\n", docType, count)
	}
	fmt.Printf("\n")

	// Show sample of documents
	fmt.Printf("Sample documents:\n")
	for i, section := range sections {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(sections)-i)
			break
		}
		fmt.Printf("  [%d] %s (%s) - %s\n",
			i+1, section.Title, section.Type, section.FilePath)
	}
	fmt.Printf("\n")

	// Convert sections to records map
	records := make(map[string]any)
	for _, section := range sections {
		records[section.ID] = section.ToDocument()
	}

	// Perform batched linear merge
	finalCursor, err := performBatchedLinearMerge(ctx, client, *tableName, records, *batchSize, *dryRun)
	if err != nil {
		return fmt.Errorf("batched linear merge failed: %w", err)
	}

	// Final cleanup: delete any remaining documents beyond the last cursor
	if finalCursor != "" && !*dryRun {
		fmt.Printf("\nPerforming final cleanup to remove orphaned documents...\n")
		cleanupResult, err := client.LinearMerge(ctx, *tableName, antfly.LinearMergeRequest{
			Records:      map[string]any{}, // Empty records
			LastMergedId: finalCursor,      // Start from last cursor
			DryRun:       false,
			SyncLevel:    antfly.SyncLevelAknn,
		})
		if err != nil {
			return fmt.Errorf("final cleanup failed: %w", err)
		}
		fmt.Printf("✓ Final cleanup completed in %s\n", cleanupResult.Took)
		fmt.Printf("  Deleted: %d orphaned documents\n", cleanupResult.Deleted)
	}

	fmt.Printf("\n✓ Sync completed successfully\n")
	return nil
}

// ANCHOR_END: sync_cmd

// ANCHOR: create_embedding_index
// createEmbeddingIndex creates an embedding index configuration with chunking
func createEmbeddingIndex(embeddingModel, chunkerModel string, targetTokens, overlapTokens int) (*antfly.IndexConfig, error) {
	embeddingIndexConfig := antfly.IndexConfig{
		Name: "embeddings",
		Type: antfly.IndexTypeEmbeddings,
	}

	// Configure embedder
	embedder, err := antfly.NewEmbedderConfig(antfly.OllamaEmbedderConfig{
		Model: embeddingModel,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure embedder: %w", err)
	}

	// Configure chunker via Termite
	// Model can be "fixed-bert-tokenizer", "fixed-bpe-tokenizer", or any ONNX model directory name
	chunker := antfly.ChunkerConfig{}
	err = chunker.FromTermiteChunkerConfig(antfly.TermiteChunkerConfig{
		Model:         chunkerModel,
		TargetTokens:  targetTokens,
		OverlapTokens: overlapTokens,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure chunker: %w", err)
	}

	// Configure embedding index with chunking
	// Note: Dimension is calculated automatically based on the embedding model
	err = embeddingIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *embedder,
		Chunker:  chunker,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure embedding index: %w", err)
	}

	return &embeddingIndexConfig, nil
}

// ANCHOR_END: create_embedding_index

// ANCHOR: batched_linear_merge
// performBatchedLinearMerge performs LinearMerge in batches with progress logging
// Returns the final cursor position after processing all batches
func performBatchedLinearMerge(
	ctx context.Context,
	client *antfly.AntflyClient,
	tableName string,
	records map[string]any,
	batchSize int,
	dryRun bool,
) (string, error) {
	// Sort IDs for deterministic pagination (REQUIRED for linear merge!)
	ids := make([]string, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	// CRITICAL: Must sort IDs for linear merge cursor logic to work correctly
	sortedIDs := make([]string, len(ids))
	copy(sortedIDs, ids)
	// Use slices.Sort from Go 1.21+
	// If on older Go, use: sort.Strings(sortedIDs)
	slices.Sort(sortedIDs)

	totalBatches := (len(sortedIDs) + batchSize - 1) / batchSize
	fmt.Printf("Loading %d documents in %d batches of %d records\n", len(sortedIDs), totalBatches, batchSize)

	cursor := ""
	totalUpserted := 0
	totalSkipped := 0
	totalDeleted := 0

	for batchNum := range totalBatches {
		// Calculate batch range
		start := batchNum * batchSize
		end := min(start+batchSize, len(sortedIDs))

		// Build batch records map
		batchIDs := sortedIDs[start:end]
		batchRecords := make(map[string]any, len(batchIDs))
		for _, id := range batchIDs {
			batchRecords[id] = records[id]
		}

		fmt.Printf("[Batch %d/%d] Merging records %d-%d\n",
			batchNum+1, totalBatches, start+1, end)

		// Execute LinearMerge with aknn sync level
		result, err := client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
			Records:      batchRecords,
			LastMergedId: cursor,
			DryRun:       dryRun,
			SyncLevel:    antfly.SyncLevelAknn, // Wait for vector index writes
		})
		if err != nil {
			return "", fmt.Errorf("failed to perform linear merge for batch %d: %w", batchNum+1, err)
		}

		// Update cursor for next batch
		if result.NextCursor != "" {
			cursor = result.NextCursor
		} else {
			// Fallback: use last ID in batch
			cursor = batchIDs[len(batchIDs)-1]
		}

		// Accumulate totals
		totalUpserted += result.Upserted
		totalSkipped += result.Skipped
		totalDeleted += result.Deleted

		// Log batch progress
		fmt.Printf("  Completed in %s - Status: %s\n", result.Took, result.Status)
		fmt.Printf("  Upserted: %d, Skipped: %d, Deleted: %d, Keys scanned: %d\n",
			result.Upserted, result.Skipped, result.Deleted, result.KeysScanned)

		if len(result.Failed) > 0 {
			fmt.Printf("  Failed operations: %d\n", len(result.Failed))
			for i, fail := range result.Failed {
				if i >= 5 {
					fmt.Printf("    ... and %d more\n", len(result.Failed)-i)
					break
				}
				fmt.Printf("    [%d] ID=%s, Error=%s\n", i, fail.Id, fail.Error)
			}
		}
	}

	// Log final totals
	fmt.Printf("\n=== Linear Merge Complete ===\n")
	fmt.Printf("Total batches: %d\n", totalBatches)
	fmt.Printf("Total upserted: %d\n", totalUpserted)
	fmt.Printf("Total skipped: %d\n", totalSkipped)
	fmt.Printf("Total deleted: %d\n", totalDeleted)

	return cursor, nil
}

// ANCHOR_END: batched_linear_merge

// waitForShardsReady polls the table status until shards are ready to accept writes
func waitForShardsReady(ctx context.Context, client *antfly.AntflyClient, tableName string, timeout time.Duration) error {
	fmt.Printf("Waiting for shards to be ready...\n")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for shards")
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for shards after %d polls", pollCount)
			}

			// Get table status
			status, err := client.GetTable(ctx, tableName)
			if err != nil {
				fmt.Printf("  [Poll %d] Error getting table status: %v\n", pollCount, err)
				continue
			}

			// Check if we have at least one shard
			if len(status.Shards) > 0 {
				// Wait longer to ensure leader election completes and propagates
				if pollCount >= 6 {
					fmt.Printf("✓ Shards ready after %d polls (~%dms)\n\n", pollCount, pollCount*500)
					return nil
				}
				fmt.Printf("  [Poll %d] Found %d shard(s), waiting for leader status to propagate\n", pollCount, len(status.Shards))
			} else {
				fmt.Printf("  [Poll %d] No shards found yet\n", pollCount)
			}
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "docsaf - Documentation Sync to Antfly\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare [flags]  - Process files and create sorted JSON data\n")
		fmt.Fprintf(os.Stderr, "  docsaf load [flags]     - Load JSON data into Antfly\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync [flags]     - Full pipeline (prepare + load)\n")
		fmt.Fprintf(os.Stderr, "\nCommands:\n")
		fmt.Fprintf(os.Stderr, "  prepare  Process documentation files and save to JSON\n")
		fmt.Fprintf(os.Stderr, "  load     Load prepared JSON data into Antfly table\n")
		fmt.Fprintf(os.Stderr, "  sync     Process files and load directly (original behavior)\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Prepare data\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare --dir /path/to/docs --output docs.json\n\n")
		fmt.Fprintf(os.Stderr, "  # Load prepared data\n")
		fmt.Fprintf(os.Stderr, "  docsaf load --input docs.json --table docs --create-table\n\n")
		fmt.Fprintf(os.Stderr, "  # Full pipeline\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync --dir /path/to/docs --table docs --create-table\n\n")
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "prepare":
		err = prepareCmd(os.Args[2:])
	case "load":
		err = loadCmd(os.Args[2:])
	case "sync":
		err = syncCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Valid commands: prepare, load, sync\n")
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}
