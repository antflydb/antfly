package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	exampleentity "github.com/antflydb/antfly/examples/docsaf/entity"
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

	entityExtraction := exampleentity.RegisterFlags(fs)

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
	entityExtraction.Print(os.Stdout)
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

	fmt.Printf("Found %d documents\n\n", len(sections))

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

	// Extract entities if an extractor model is configured.
	entityResult, err := entityExtraction.Run(context.Background(), sections)
	if err != nil {
		return fmt.Errorf("entity extraction failed: %w", err)
	}

	// Convert sections to records map, enriching sections with extraction results when configured.
	records := exampleentity.BuildRecords(sections, entityResult)

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

	fmt.Printf("Prepared data written to %s\n", *outputFile)
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

	// Read JSON file first (needed to detect entity records for graph index)
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

	fmt.Printf("Loaded %d records\n\n", len(records))

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s' with %d shards...\n", *tableName, *numShards)

		// Create embedding index configuration
		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index config: %w", err)
		}

		indexes := map[string]antfly.IndexConfig{
			"embeddings": *embeddingIndex,
		}

		// Auto-detect entity records and add graph index
		if exampleentity.HasEntityRecords(records) {
			graphIndex, err := createGraphIndex()
			if err != nil {
				return fmt.Errorf("failed to create graph index config: %w", err)
			}
			indexes["knowledge"] = *graphIndex
			fmt.Printf("Detected entity records, adding knowledge graph index\n")
		}

		if err := createTableWithIndexes(ctx, client, *tableName, *numShards, indexes); err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}
	}

	// Perform linear merge with sorted pages
	pages := sortedPages(records, *batchSize)
	mergeResult, err := client.ExecuteLinearMerge(ctx, *tableName, pages, antfly.ExecuteLinearMergeOptions{
		DryRun:    *dryRun,
		SyncLevel: antfly.SyncLevelAknn,
		OnBatch: func(batch int, result *antfly.LinearMergeResult) {
			fmt.Printf("[batch %d] upserted: %d, skipped: %d, deleted: %d, took: %s\n",
				batch, result.Upserted, result.Skipped, result.Deleted, result.Took)
		},
	})
	if err != nil {
		return fmt.Errorf("linear merge failed: %w", err)
	}

	fmt.Printf("\nLoad completed: %d upserted, %d deleted, %d batches\n",
		mergeResult.Upserted, mergeResult.Deleted, mergeResult.Batches)
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

	entityExtraction := exampleentity.RegisterFlags(fs)

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

	// Verify path exists and is a directory before any remote operations
	fileInfo, err := os.Stat(*dirPath)
	if err != nil {
		return fmt.Errorf("failed to access path: %w", err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("--dir must be a directory")
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
	entityExtraction.Print(os.Stdout)
	fmt.Printf("\n")

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s' with %d shards...\n", *tableName, *numShards)

		// Create embedding index configuration
		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index config: %w", err)
		}

		indexes := map[string]antfly.IndexConfig{
			"embeddings": *embeddingIndex,
		}

		// Add a graph index when extraction is enabled.
		if entityExtraction.Enabled() {
			graphIndex, err := createGraphIndex()
			if err != nil {
				return fmt.Errorf("failed to create graph index config: %w", err)
			}
			indexes["knowledge"] = *graphIndex
		}

		if err := createTableWithIndexes(ctx, client, *tableName, *numShards, indexes); err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}
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

	fmt.Printf("Found %d documents\n\n", len(sections))

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

	// Extract entities if an extractor model is configured.
	entityResult, err := entityExtraction.Run(ctx, sections)
	if err != nil {
		return fmt.Errorf("entity extraction failed: %w", err)
	}

	// Convert sections to records map, enriching sections with extraction results when configured.
	records := exampleentity.BuildRecords(sections, entityResult)

	// Perform linear merge with sorted pages
	pages := sortedPages(records, *batchSize)
	mergeResult, err := client.ExecuteLinearMerge(ctx, *tableName, pages, antfly.ExecuteLinearMergeOptions{
		DryRun:    *dryRun,
		SyncLevel: antfly.SyncLevelAknn,
		OnBatch: func(batch int, result *antfly.LinearMergeResult) {
			fmt.Printf("[batch %d] upserted: %d, skipped: %d, deleted: %d, took: %s\n",
				batch, result.Upserted, result.Skipped, result.Deleted, result.Took)
		},
	})
	if err != nil {
		return fmt.Errorf("linear merge failed: %w", err)
	}

	fmt.Printf("\nSync completed: %d upserted, %d deleted, %d batches\n",
		mergeResult.Upserted, mergeResult.Deleted, mergeResult.Batches)
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
		Model: chunkerModel,
		Text: antfly.TextChunkOptions{
			TargetTokens:  targetTokens,
			OverlapTokens: overlapTokens,
		},
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

// createGraphIndex creates a graph index for entity and relation enrichment.
func createGraphIndex() (*antfly.IndexConfig, error) {
	graphIndexConfig := antfly.IndexConfig{
		Name: "knowledge",
		Type: antfly.IndexTypeGraph,
	}

	err := graphIndexConfig.FromGraphIndexConfig(antfly.GraphIndexConfig{
		EdgeTypes: []antfly.EdgeTypeConfig{
			{
				Name:  "mentions_entity",
				Field: "entities",
			},
			{
				Name:  "mentions_relation",
				Field: "relations",
			},
			{
				Name:  "relation_head",
				Field: "head_entity",
			},
			{
				Name:  "relation_tail",
				Field: "tail_entity",
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return &graphIndexConfig, nil
}

// createTableWithIndexes creates the table, logs the result, and waits for shards to be ready.
func createTableWithIndexes(ctx context.Context, client *antfly.AntflyClient, tableName string, numShards int, indexes map[string]antfly.IndexConfig) error {
	err := client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: uint(numShards),
		Indexes:   indexes,
	})
	if err != nil {
		log.Printf("Warning: Failed to create table (may already exist): %v\n", err)
	} else {
		indexNames := make([]string, 0, len(indexes))
		for name := range indexes {
			indexNames = append(indexNames, name)
		}
		fmt.Printf("Table created with indexes: %s\n\n", strings.Join(indexNames, ", "))
	}

	if err := client.WaitForTable(ctx, tableName, 30*time.Second); err != nil {
		return err
	}
	fmt.Printf("Shards ready\n\n")
	return nil
}

// sortedPages is a convenience alias for antfly.SortedPages.
var sortedPages = antfly.SortedPages

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
		fmt.Fprintf(os.Stderr, "  # Prepare with recognizer-based entity extraction\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare --dir /path/to/docs --output docs.json \\\n")
		fmt.Fprintf(os.Stderr, "    --extractor-model fastino/gliner2-base-v1 \\\n")
		fmt.Fprintf(os.Stderr, "    --extractor-kind recognizer \\\n")
		fmt.Fprintf(os.Stderr, "    --entity-label technology --entity-label concept --entity-label api_endpoint\n\n")
		fmt.Fprintf(os.Stderr, "  # Prepare with relation extraction\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare --dir /path/to/docs --output docs.json \\\n")
		fmt.Fprintf(os.Stderr, "    --extractor-model some-relations-model \\\n")
		fmt.Fprintf(os.Stderr, "    --extractor-kind recognizer --extractor-relations \\\n")
		fmt.Fprintf(os.Stderr, "    --relation-label depends_on --relation-label implements\n\n")
		fmt.Fprintf(os.Stderr, "  # Load prepared data\n")
		fmt.Fprintf(os.Stderr, "  docsaf load --input docs.json --table docs --create-table\n\n")
		fmt.Fprintf(os.Stderr, "  # Full pipeline with generator-based extraction + knowledge graph\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync --dir /path/to/docs --table docs --create-table \\\n")
		fmt.Fprintf(os.Stderr, "    --extractor-model functiongemma-270m-it \\\n")
		fmt.Fprintf(os.Stderr, "    --extractor-kind generator \\\n")
		fmt.Fprintf(os.Stderr, "    --entity-label technology --entity-label concept\n\n")
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
