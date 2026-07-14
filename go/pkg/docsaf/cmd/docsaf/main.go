package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/docsaf"
	antfly "github.com/antflydb/antfly/go/pkg/sdk"
)

const (
	defaultDocsafMaxMergeRequestBytes  int64 = 48 << 20
	defaultDocsafMaxInlineContentBytes int64 = 3 << 20
	defaultDriveMaxInlineContentBytes  int64 = 100 << 20
	defaultDocsafEmbeddingProvider           = "antfly"
	defaultDocsafEmbeddingModel              = "antflydb/clipclap:gguf:Q4_K"
)

// StringSliceFlag allows repeated flags to build a slice.
type StringSliceFlag []string

func (s *StringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *StringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type sourceFlags struct {
	fs                *flag.FlagSet
	sourceType        *string
	dirPath           *string
	baseURL           *string
	inlineContent     *bool
	maxInlineBytes    *int64
	idPrefix          *string
	driveFolder       *string
	driveCredentials  *string
	driveAccessToken  *string
	driveTokenFile    *string
	driveConcurrency  *int
	driveSharedDrives *bool
	includePatterns   StringSliceFlag
	excludePatterns   StringSliceFlag
}

func registerSourceFlags(fs *flag.FlagSet) sourceFlags {
	flags := sourceFlags{
		fs:                fs,
		sourceType:        fs.String("source", "filesystem", "Source type: filesystem or google-drive"),
		dirPath:           fs.String("dir", "", "Path to directory containing source documents (required for filesystem source)"),
		baseURL:           fs.String("base-url", "", "Fetchable URL prefix for source documents"),
		inlineContent:     fs.Bool("inline-content", false, "Encode source bytes as data: URLs for local smoke tests and private sources"),
		maxInlineBytes:    fs.Int64("max-inline-bytes", defaultDocsafMaxInlineContentBytes, "Maximum source bytes allowed with --inline-content"),
		idPrefix:          fs.String("id-prefix", "", "Optional prefix for source document IDs"),
		driveFolder:       fs.String("drive-folder", "", "Google Drive folder ID or folder URL (required for google-drive source)"),
		driveCredentials:  fs.String("drive-credentials", "", "Google service account JSON or path for Drive readonly access"),
		driveAccessToken:  fs.String("drive-access-token", "", "Google Drive OAuth access token; falls back to GOOGLE_DRIVE_ACCESS_TOKEN"),
		driveTokenFile:    fs.String("drive-token-file", defaultGoogleDriveTokenFile(), "OAuth token cache from `docsaf auth google-drive`"),
		driveConcurrency:  fs.Int("drive-concurrency", 5, "Parallel Google Drive downloads"),
		driveSharedDrives: fs.Bool("drive-include-shared-drives", true, "Include files from Google shared drives"),
	}
	fs.Var(&flags.includePatterns, "include", "Include pattern (can be repeated, supports ** wildcards)")
	fs.Var(&flags.excludePatterns, "exclude", "Exclude pattern (can be repeated, supports ** wildcards)")
	return flags
}

func (f sourceFlags) validate(ctx context.Context) error {
	switch f.normalizedSourceType() {
	case "filesystem":
		return f.validateFilesystem()
	case "google-drive":
		if strings.TrimSpace(*f.driveFolder) == "" {
			return fmt.Errorf("--drive-folder is required for --source google-drive")
		}
		if f.googleDriveAuthConfigured(ctx) {
			return nil
		}
		return missingGoogleDriveAuthError()
	default:
		return fmt.Errorf("unknown --source %q; expected filesystem or google-drive", *f.sourceType)
	}
}

func (f sourceFlags) normalizeForSource() {
	if f.normalizedSourceType() == "google-drive" && *f.inlineContent && !f.flagSet("max-inline-bytes") {
		*f.maxInlineBytes = defaultDriveMaxInlineContentBytes
	}
}

func (f sourceFlags) flagSet(name string) bool {
	if f.fs == nil {
		return false
	}
	set := false
	f.fs.Visit(func(flag *flag.Flag) {
		if flag.Name == name {
			set = true
		}
	})
	return set
}

func (f sourceFlags) validateFilesystem() error {
	if *f.dirPath == "" {
		return fmt.Errorf("--dir flag is required for --source filesystem")
	}
	if *f.baseURL == "" && !*f.inlineContent {
		return fmt.Errorf("set --base-url for fetchable source URLs or --inline-content for local smoke tests")
	}
	fileInfo, err := os.Stat(*f.dirPath)
	if err != nil {
		return fmt.Errorf("failed to access path: %w", err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("--dir must be a directory")
	}
	return nil
}

func (f sourceFlags) normalizedSourceType() string {
	sourceType := strings.TrimSpace(*f.sourceType)
	if sourceType == "" {
		return "filesystem"
	}
	return sourceType
}

func (f sourceFlags) googleDriveAuthConfigured(ctx context.Context) bool {
	if strings.TrimSpace(*f.driveCredentials) != "" || strings.TrimSpace(*f.driveAccessToken) != "" || strings.TrimSpace(os.Getenv("GOOGLE_DRIVE_ACCESS_TOKEN")) != "" {
		return true
	}
	_, err := resolveGoogleDriveTokenSource(ctx, *f.driveTokenFile)
	return err == nil
}

func (f sourceFlags) source(ctx context.Context) (docsaf.ContentSource, error) {
	switch f.normalizedSourceType() {
	case "filesystem":
		return docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
			BaseDir:         *f.dirPath,
			BaseURL:         *f.baseURL,
			IncludePatterns: f.includePatterns,
			ExcludePatterns: f.excludePatterns,
		}), nil
	case "google-drive":
		config := docsaf.GoogleDriveSourceConfig{
			FolderID:            *f.driveFolder,
			BaseURL:             *f.baseURL,
			IncludePatterns:     f.includePatterns,
			ExcludePatterns:     f.excludePatterns,
			Concurrency:         *f.driveConcurrency,
			IncludeSharedDrives: f.driveSharedDrives,
		}
		if token := strings.TrimSpace(*f.driveAccessToken); token != "" {
			config.AccessToken = token
		} else if credentials := strings.TrimSpace(*f.driveCredentials); credentials != "" {
			config.CredentialsJSON = credentials
		} else if token := strings.TrimSpace(os.Getenv("GOOGLE_DRIVE_ACCESS_TOKEN")); token != "" {
			config.AccessToken = token
		} else {
			tokenSource, err := resolveGoogleDriveTokenSource(ctx, *f.driveTokenFile)
			if err != nil {
				return nil, err
			}
			config.TokenSource = tokenSource
		}
		return docsaf.NewGoogleDriveSource(ctx, config)
	default:
		return nil, fmt.Errorf("unknown --source %q; expected filesystem or google-drive", *f.sourceType)
	}
}

func (f sourceFlags) options() docsaf.SourceDocumentOptions {
	return docsaf.SourceDocumentOptions{
		InlineContent:  *f.inlineContent,
		BaseURL:        *f.baseURL,
		MaxInlineBytes: *f.maxInlineBytes,
		IDPrefix:       *f.idPrefix,
	}
}

func (f sourceFlags) print() {
	sourceType := f.normalizedSourceType()
	fmt.Printf("Source: %s\n", sourceType)
	switch sourceType {
	case "filesystem":
		fmt.Printf("Directory: %s\n", *f.dirPath)
	case "google-drive":
		fmt.Printf("Drive folder: %s\n", *f.driveFolder)
		fmt.Printf("Drive token file: %s\n", *f.driveTokenFile)
		fmt.Printf("Drive service account configured: %v\n", strings.TrimSpace(*f.driveCredentials) != "")
		fmt.Printf("Drive access token configured: %v\n", strings.TrimSpace(*f.driveAccessToken) != "" || strings.TrimSpace(os.Getenv("GOOGLE_DRIVE_ACCESS_TOKEN")) != "")
		fmt.Printf("Drive include shared drives: %v\n", *f.driveSharedDrives)
		fmt.Printf("Drive concurrency: %d\n", *f.driveConcurrency)
		if !*f.inlineContent && *f.baseURL == "" {
			fmt.Printf("Drive URL mode: using Drive web links; private files usually require --inline-content or Antfly-readable URLs\n")
		}
	}
	if *f.baseURL != "" {
		fmt.Printf("Base URL: %s\n", *f.baseURL)
	}
	fmt.Printf("Inline content: %v\n", *f.inlineContent)
	if *f.idPrefix != "" {
		fmt.Printf("ID prefix: %s\n", *f.idPrefix)
	}
	if *f.inlineContent {
		fmt.Printf("Max inline bytes: %d\n", *f.maxInlineBytes)
	}
	if len(f.includePatterns) > 0 {
		fmt.Printf("Include patterns: %v\n", f.includePatterns)
	}
	if len(f.excludePatterns) > 0 {
		fmt.Printf("Exclude patterns: %v\n", f.excludePatterns)
	}
}

// ANCHOR: prepare_cmd
func prepareCmd(args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ExitOnError)
	outputFile := fs.String("output", "docs.json", "Output JSON file path")
	sourceFlags := registerSourceFlags(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}
	sourceFlags.normalizeForSource()
	ctx := context.Background()
	if err := sourceFlags.validate(ctx); err != nil {
		return err
	}

	fmt.Printf("=== docsaf prepare - Build Source Document Rows ===\n")
	sourceFlags.print()
	fmt.Printf("Output: %s\n\n", *outputFile)

	source, err := sourceFlags.source(ctx)
	if err != nil {
		return fmt.Errorf("failed to create source: %w", err)
	}
	docs, err := docsaf.BuildSourceDocuments(ctx, source, sourceFlags.options())
	if err != nil {
		return fmt.Errorf("failed to build source documents: %w", err)
	}
	if len(docs) == 0 {
		return fmt.Errorf("no source documents found")
	}

	records := docsaf.SourceDocumentRecords(docs)
	fmt.Printf("Found %d source documents\n", len(docs))
	printDocumentSample(docs)

	fmt.Printf("Writing %d records to %s...\n", len(records), *outputFile)
	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	if err := os.WriteFile(*outputFile, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Prepared source rows written to %s\n", *outputFile)
	return nil
}

// ANCHOR_END: prepare_cmd

// ANCHOR: load_cmd
func loadCmd(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/db/v1", "Antfly API URL")
	tableName := fs.String("table", "docs", "Table name to merge into")
	inputFile := fs.String("input", "docs.json", "Input JSON file path")
	dryRun := fs.Bool("dry-run", false, "Preview changes without applying them")
	createTable := fs.Bool("create-table", false, "Create table if it doesn't exist")
	numShards := fs.Int("num-shards", 1, "Number of shards for new table")
	batchSize := fs.Int("batch-size", 25, "Linear merge batch size")
	maxRequestBytes := fs.Int64("max-request-bytes", defaultDocsafMaxMergeRequestBytes, "Maximum encoded linear merge request bytes")
	authToken := fs.String("token", "", "Bearer token for Antfly Cloud auth; falls back to ANTFLY_TOKEN or ANTFLY_AUTH_TOKEN")
	chunkSize := fs.Int("chunk-size", 512, "Target characters/tokens for unit-derived chunks")
	chunkOverlap := fs.Int("chunk-overlap", 50, "Overlap for unit-derived chunks")
	embeddingProvider := fs.String("embedding-provider", defaultDocsafEmbeddingProvider, "Embedding provider for managed vector search")
	embeddingModel := fs.String("embedding-model", defaultDocsafEmbeddingModel, "Embedding model for managed vector search")
	embeddingConfigJSON := fs.String("embedding-config-json", "", "Full JSON Antfly SDK EmbedderConfig for managed vector search; overrides --embedding-provider and --embedding-model")
	embeddingDims := fs.Int("embedding-dims", 0, "Expected embedding dimensions; 0 lets Antfly probe the embedder")
	ocrConfigJSON := fs.String("ocr-config-json", "", "Reader provider config JSON; enables selective server-side PDF OCR")
	ocrRenderDPI := fs.Int("ocr-render-dpi", 150, "PDF page render DPI for selective OCR")
	ocrMinContentChars := fs.Int("ocr-min-content-chars", docsaf.DefaultOCRMinContent, "Minimum usable embedded-text characters before OCR fallback")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}
	token := resolveAuthToken(*authToken)

	ctx := context.Background()
	client, err := newDocsafClient(*antflyURL, token)
	if err != nil {
		return fmt.Errorf("failed to create Antfly client: %w", err)
	}

	fmt.Printf("=== docsaf load - Load Source Rows To Antfly ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Auth token configured: %v\n", token != "")
	fmt.Printf("Input: %s\n", *inputFile)
	fmt.Printf("Max request bytes: %d\n", *maxRequestBytes)
	fmt.Printf("Dry run: %v\n\n", *dryRun)

	jsonData, err := os.ReadFile(*inputFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	var records map[string]any
	if err := json.Unmarshal(jsonData, &records); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	fmt.Printf("Loaded %d source records\n\n", len(records))

	if *createTable {
		fmt.Printf("Creating table '%s' with derived document hierarchy indexes...\n", *tableName)
		indexes, err := createHierarchyIndexes(*chunkSize, *chunkOverlap, *embeddingProvider, *embeddingModel, *embeddingConfigJSON, *embeddingDims, *ocrConfigJSON, *ocrRenderDPI, *ocrMinContentChars)
		if err != nil {
			return fmt.Errorf("building hierarchy index config: %w", err)
		}
		if err := createTableWithIndexes(ctx, *antflyURL, token, client, *tableName, *numShards, indexes); err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}
	}

	mergeResult, err := executeLinearMergeRecords(ctx, client, *tableName, records, linearMergeRunOptions{
		batchSize:       *batchSize,
		maxRequestBytes: *maxRequestBytes,
		dryRun:          *dryRun,
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
	antflyURL := fs.String("url", "http://localhost:8080/db/v1", "Antfly API URL")
	tableName := fs.String("table", "docs", "Table name to merge into")
	dryRun := fs.Bool("dry-run", false, "Preview changes without applying them")
	createTable := fs.Bool("create-table", false, "Create table if it doesn't exist")
	numShards := fs.Int("num-shards", 1, "Number of shards for new table")
	batchSize := fs.Int("batch-size", 25, "Linear merge batch size")
	maxRequestBytes := fs.Int64("max-request-bytes", defaultDocsafMaxMergeRequestBytes, "Maximum encoded linear merge request bytes")
	authToken := fs.String("token", "", "Bearer token for Antfly Cloud auth; falls back to ANTFLY_TOKEN or ANTFLY_AUTH_TOKEN")
	chunkSize := fs.Int("chunk-size", 512, "Target characters/tokens for unit-derived chunks")
	chunkOverlap := fs.Int("chunk-overlap", 50, "Overlap for unit-derived chunks")
	embeddingProvider := fs.String("embedding-provider", defaultDocsafEmbeddingProvider, "Embedding provider for managed vector search")
	embeddingModel := fs.String("embedding-model", defaultDocsafEmbeddingModel, "Embedding model for managed vector search")
	embeddingConfigJSON := fs.String("embedding-config-json", "", "Full JSON Antfly SDK EmbedderConfig for managed vector search; overrides --embedding-provider and --embedding-model")
	embeddingDims := fs.Int("embedding-dims", 0, "Expected embedding dimensions; 0 lets Antfly probe the embedder")
	ocrConfigJSON := fs.String("ocr-config-json", "", "Reader provider config JSON; enables selective server-side PDF OCR")
	ocrRenderDPI := fs.Int("ocr-render-dpi", 150, "PDF page render DPI for selective OCR")
	ocrMinContentChars := fs.Int("ocr-min-content-chars", docsaf.DefaultOCRMinContent, "Minimum usable embedded-text characters before OCR fallback")
	sourceFlags := registerSourceFlags(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}
	sourceFlags.normalizeForSource()
	ctx := context.Background()
	if err := sourceFlags.validate(ctx); err != nil {
		return err
	}
	token := resolveAuthToken(*authToken)

	client, err := newDocsafClient(*antflyURL, token)
	if err != nil {
		return fmt.Errorf("failed to create Antfly client: %w", err)
	}

	fmt.Printf("=== docsaf sync - Source Rows + Derived Hierarchy ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Auth token configured: %v\n", token != "")
	sourceFlags.print()
	fmt.Printf("Max request bytes: %d\n", *maxRequestBytes)
	fmt.Printf("Dry run: %v\n\n", *dryRun)

	if *createTable {
		fmt.Printf("Creating table '%s' with derived document hierarchy indexes...\n", *tableName)
		indexes, err := createHierarchyIndexes(*chunkSize, *chunkOverlap, *embeddingProvider, *embeddingModel, *embeddingConfigJSON, *embeddingDims, *ocrConfigJSON, *ocrRenderDPI, *ocrMinContentChars)
		if err != nil {
			return fmt.Errorf("building hierarchy index config: %w", err)
		}
		if err := createTableWithIndexes(ctx, *antflyURL, token, client, *tableName, *numShards, indexes); err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}
	}

	source, err := sourceFlags.source(ctx)
	if err != nil {
		return fmt.Errorf("failed to create source: %w", err)
	}
	docs, err := docsaf.BuildSourceDocuments(ctx, source, sourceFlags.options())
	if err != nil {
		return fmt.Errorf("failed to build source documents: %w", err)
	}
	if len(docs) == 0 {
		return fmt.Errorf("no source documents found")
	}

	fmt.Printf("Found %d source documents\n", len(docs))
	printDocumentSample(docs)

	records := docsaf.SourceDocumentRecords(docs)
	mergeResult, err := executeLinearMergeRecords(ctx, client, *tableName, records, linearMergeRunOptions{
		batchSize:       *batchSize,
		maxRequestBytes: *maxRequestBytes,
		dryRun:          *dryRun,
	})
	if err != nil {
		return fmt.Errorf("linear merge failed: %w", err)
	}

	fmt.Printf("\nSync completed: %d upserted, %d deleted, %d batches\n",
		mergeResult.Upserted, mergeResult.Deleted, mergeResult.Batches)
	return nil
}

// ANCHOR_END: sync_cmd

func printDocumentSample(docs []docsaf.SourceDocument) {
	fmt.Printf("\nSample source documents:\n")
	for i, doc := range docs {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(docs)-i)
			break
		}
		fmt.Printf("  [%d] %s (%s) - %s\n", i+1, doc.ID, doc.MIMEType, doc.URL)
	}
	fmt.Printf("\n")
}

func createHierarchyIndexes(chunkSize, chunkOverlap int, embeddingProvider, embeddingModel, embeddingConfigJSON string, embeddingDims int, ocrConfigJSON string, ocrRenderDPI, ocrMinContentChars int) (map[string]any, error) {
	sourceConfig := map[string]any{
		"filename_field":     "filename",
		"content_type_field": "mime_type",
		"etag_field":         "etag",
		"checksum_field":     "sha256",
		"version_field":      "version",
	}
	documentUnits := docsaf.DefaultDocumentUnitsArtifact
	documentChunks := docsaf.DefaultDocumentChunksArtifact
	documentEmbedding := "document_chunk_dense_v1"

	extractionConfig := map[string]any{
		"route_preset": "mixed_files",
		"source":       sourceConfig,
	}
	if raw := strings.TrimSpace(ocrConfigJSON); raw != "" {
		if ocrRenderDPI < 72 || ocrRenderDPI > 600 {
			return nil, fmt.Errorf("OCR render DPI must be between 72 and 600")
		}
		if ocrMinContentChars < 0 {
			return nil, fmt.Errorf("OCR minimum content characters cannot be negative")
		}
		var readerConfig map[string]any
		if err := json.Unmarshal([]byte(raw), &readerConfig); err != nil {
			return nil, fmt.Errorf("parse OCR reader config JSON: %w", err)
		}
		extractionConfig["ocr"] = map[string]any{
			"enabled":    true,
			"render_dpi": ocrRenderDPI,
			"quality": map[string]any{
				"min_content_chars": ocrMinContentChars,
			},
			"config": readerConfig,
		}
	}

	producerJSON, err := json.Marshal(map[string]any{
		"type":   "document_extraction",
		"config": extractionConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal document extraction producer: %w", err)
	}

	embedder, err := docsafEmbedderConfig(embeddingProvider, embeddingModel, embeddingConfigJSON)
	if err != nil {
		return nil, fmt.Errorf("build embedder config: %w", err)
	}
	vectorIndex, err := antfly.NewArtifactEmbeddingIndexConfig("document_vectors", antfly.ArtifactEmbeddingIndexConfig{
		SourceArtifactName: documentChunks,
		EmbeddingName:      documentEmbedding,
		SourceField:        "text",
		ExpectedDims:       embeddingDims,
		Embedder:           *embedder,
		DistanceMetric:     antfly.DistanceMetricCosine,
	})
	if err != nil {
		return nil, fmt.Errorf("build vector index config: %w", err)
	}
	vectorIndexBody, err := indexConfigMap(*vectorIndex)
	if err != nil {
		return nil, fmt.Errorf("marshal vector index config: %w", err)
	}

	return map[string]any{
		"document_units": map[string]any{
			"type": "graph",
			"source": map[string]any{
				"kind":     "artifact",
				"artifact": documentUnits,
				"path":     "$.edges[*]",
				"format":   "extraction_relation",
			},
			"artifact": map[string]any{
				"name":         documentUnits,
				"kind":         "asset",
				"field":        "url",
				"content_type": "application/json",
				"producer_json": map[string]any{
					"type":   "document_extraction",
					"config": extractionConfig,
				},
			},
			"edge_types": []map[string]any{{"name": "mentions"}},
		},
		"document_text": map[string]any{
			"type":          "full_text",
			"field":         "text",
			"artifact_name": documentChunks,
			"enrichments": []map[string]any{
				{
					"name":          documentUnits,
					"kind":          "asset",
					"field":         "url",
					"content_type":  "application/json",
					"producer_json": string(producerJSON),
				},
				{
					"name":                 documentChunks,
					"kind":                 "chunk",
					"field":                "text",
					"source_artifact_name": documentUnits,
					"chunk_size":           chunkSize,
					"chunk_overlap":        chunkOverlap,
				},
			},
		},
		"document_vectors": vectorIndexBody,
	}, nil
}

func docsafEmbedderConfig(provider, model, configJSON string) (*antfly.EmbedderConfig, error) {
	if raw := strings.TrimSpace(configJSON); raw != "" {
		var cfg antfly.EmbedderConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return nil, fmt.Errorf("parse embedding config JSON: %w", err)
		}
		if strings.TrimSpace(string(cfg.Provider)) == "" {
			return nil, fmt.Errorf("embedding provider is required")
		}
		return &cfg, nil
	}

	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = defaultDocsafEmbeddingProvider
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, fmt.Errorf("embedding model is required")
	}
	raw, err := json.Marshal(map[string]any{
		"provider": provider,
		"model":    model,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal embedding config: %w", err)
	}
	var cfg antfly.EmbedderConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("build embedding config: %w", err)
	}
	return &cfg, nil
}

func indexConfigMap(index antfly.IndexConfig) (map[string]any, error) {
	data, err := json.Marshal(index)
	if err != nil {
		return nil, err
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	return body, nil
}

func resolveAuthToken(flagValue string) string {
	if token := strings.TrimSpace(flagValue); token != "" {
		return token
	}
	if token := strings.TrimSpace(os.Getenv("ANTFLY_TOKEN")); token != "" {
		return token
	}
	return strings.TrimSpace(os.Getenv("ANTFLY_AUTH_TOKEN"))
}

func newDocsafClient(antflyURL string, token string) (*antfly.AntflyClient, error) {
	return antfly.NewAntflyClientWithToken(antflyURL, http.DefaultClient, token)
}

func createTableWithIndexes(ctx context.Context, antflyURL string, token string, client *antfly.AntflyClient, tableName string, numShards int, indexes map[string]any) error {
	body := map[string]any{
		"num_shards": numShards,
		"indexes":    indexes,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal create table request: %w", err)
	}

	endpoint := strings.TrimRight(antflyURL, "/") + "/tables/" + url.PathEscape(tableName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // response body close is best effort

	if resp.StatusCode >= 300 {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		body := strings.TrimSpace(buf.String())
		if resp.StatusCode == http.StatusConflict {
			log.Printf("Table already exists: HTTP %d %s\n\n", resp.StatusCode, body)
		} else {
			if body == "" {
				body = http.StatusText(resp.StatusCode)
			}
			return fmt.Errorf("create table %q failed: HTTP %d %s", tableName, resp.StatusCode, body)
		}
	} else {
		fmt.Printf("Table created with indexes: document_units, document_text, document_vectors\n\n")
	}

	if err := client.WaitForTable(ctx, tableName, 30*time.Second); err != nil {
		return err
	}
	fmt.Printf("Shards ready\n\n")
	return nil
}

type linearMergeRunOptions struct {
	batchSize       int
	maxRequestBytes int64
	dryRun          bool
}

func executeLinearMergeRecords(ctx context.Context, client *antfly.AntflyClient, tableName string, records map[string]any, opts linearMergeRunOptions) (*antfly.ExecuteLinearMergeResult, error) {
	if opts.batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	if opts.maxRequestBytes <= 0 {
		return nil, fmt.Errorf("max request bytes must be positive")
	}

	pages, err := antfly.SortedLinearMergePages(records, antfly.LinearMergePageOptions{
		MaxRecords:      opts.batchSize,
		MaxRequestBytes: opts.maxRequestBytes,
		DryRun:          opts.dryRun,
		SyncLevel:       antfly.SyncLevelFullIndex,
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("Linear merge pages: %d (max %d records/page, max %d bytes/request)\n", len(pages), opts.batchSize, opts.maxRequestBytes)

	pageSeq := func(yield func(map[string]any) bool) {
		for _, page := range pages {
			if !yield(page) {
				return
			}
		}
	}
	return client.ExecuteLinearMerge(ctx, tableName, pageSeq, antfly.ExecuteLinearMergeOptions{
		DryRun:    opts.dryRun,
		SyncLevel: antfly.SyncLevelFullIndex,
		WriteOptions: antfly.WriteOptions{
			MaxRequestBytes: opts.maxRequestBytes,
		},
		OnBatch: func(batch int, result *antfly.LinearMergeResult) {
			fmt.Printf("  Batch %d: upserted=%d skipped=%d deleted=%d next_cursor=%q status=%s\n",
				batch, result.Upserted, result.Skipped, result.Deleted, result.NextCursor, result.Status)
		},
	})
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "docsaf - Source Document Sync to Antfly\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare [flags]  - Traverse files and create source-row JSON\n")
		fmt.Fprintf(os.Stderr, "  docsaf load [flags]     - Load source-row JSON into Antfly\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync [flags]     - Traverse files and load source rows directly\n")
		fmt.Fprintf(os.Stderr, "  docsaf auth google-drive [flags] - Authorize Drive access for prepare/sync\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare --dir ./docs --base-url s3://docs-bucket --output docs.json\n")
		fmt.Fprintf(os.Stderr, "  docsaf load --input docs.json --table docs --create-table\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync --dir ./docs --base-url s3://docs-bucket --table docs --create-table\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync --dir ./docs --inline-content --table docs --create-table\n")
		fmt.Fprintf(os.Stderr, "  docsaf auth google-drive --client-secret ./client_secret.json\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync --source google-drive --drive-folder <folder-url> --inline-content --table docs\n")
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
	case "auth":
		err = authCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Valid commands: prepare, load, sync, auth\n")
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}
