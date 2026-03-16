/*
Copyright 2025 The Antfly Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cli

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/admin"
	"github.com/antflydb/antfly/pkg/client/query"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/cespare/xxhash/v2"
	"github.com/sethvargo/go-retry"
	"golang.org/x/time/rate"
)

// formatBytes formats bytes into a human-readable string
func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// AntflyClient wraps the SDK client with CLI-specific functionality
type AntflyClient struct {
	*antfly.AntflyClient
	internalClient *admin.InternalClient
}

// NewAntflyClient creates a new Antfly client
func NewAntflyClient(baseURL string, httpClient *http.Client) (*AntflyClient, error) {
	client, err := antfly.NewAntflyClient(strings.TrimRight(baseURL, "/")+"/api/v1", httpClient)
	if err != nil {
		return nil, err
	}

	// Initialize internal admin client using the same base URL and HTTP client
	internalClient := admin.NewInternalClient(strings.TrimRight(baseURL, "/"), httpClient)

	return &AntflyClient{
		AntflyClient:   client,
		internalClient: internalClient,
	}, nil
}

// CreateTable creates a new table
func (c *AntflyClient) CreateTable(ctx context.Context, tableName string, numShards int, schemaJSON string, indexes map[string]antfly.IndexConfig) error {
	var schema antfly.TableSchema
	if schemaJSON != "" {
		if err := json.UnmarshalString(schemaJSON, &schema); err != nil {
			return fmt.Errorf("parsing schema JSON: %w", err)
		}
	}
	fmt.Fprintf(os.Stderr, "Table will be created with %d index(es).\n", len(indexes))
	fmt.Fprintf(os.Stderr, "Sending CREATE TABLE request for table '%s'\n", tableName)
	err := c.AntflyClient.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: uint(max(numShards, 0)),
		Indexes:   indexes,
		Schema:    schema,
	})
	if err != nil {
		var apiErr *antfly.APIError
		if errors.As(err, &apiErr) && strings.Contains(apiErr.Message, "already exists") {
			fmt.Fprintf(os.Stderr, "Table '%s' already exists, skipping creation.\n", tableName)
			return nil
		}
		return fmt.Errorf("failed to create table '%s': %w", tableName, err)
	}
	fmt.Fprintf(os.Stderr, "CREATE TABLE request for '%s' acknowledged.\n", tableName)
	return nil
}

// DropTable drops an existing table
func (c *AntflyClient) DropTable(ctx context.Context, tableName string) error {
	fmt.Fprintf(os.Stderr, "Sending DROP TABLE request for table '%s'\n", tableName)
	err := c.AntflyClient.DropTable(ctx, tableName)
	if err != nil {
		var apiErr *antfly.APIError
		if errors.As(err, &apiErr) && strings.Contains(apiErr.Message, "not found") {
			return fmt.Errorf("table '%s' not found", tableName)
		}
		return fmt.Errorf("drop table request failed for '%s': %w", tableName, err)
	}
	fmt.Fprintf(os.Stderr, "DROP TABLE request for '%s' successful.\n", tableName)
	return nil
}

// ListTables lists all tables
func (c *AntflyClient) ListTables(ctx context.Context) ([]antfly.TableStatus, error) {
	fmt.Fprintln(os.Stderr, "Sending LIST TABLES request")

	tables, err := c.AntflyClient.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tables request failed: %w", err)
	}

	// Sort tables by name for consistent ordering
	slices.SortFunc(tables, func(a, b antfly.TableStatus) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return tables, nil
}

// printTableList prints a human-readable list of tables to stderr.
func printTableList(tables []antfly.TableStatus) {
	if len(tables) == 0 {
		fmt.Fprintln(os.Stderr, "No tables found.")
		return
	}

	fmt.Fprintf(os.Stderr, "Found %d table(s):\n", len(tables))
	for _, table := range tables {
		shardCount := len(table.Shards)
		indexCount := len(table.Indexes)

		emptyStr := ""
		diskUsageStr := formatBytes(table.StorageStatus.DiskUsage)
		if table.StorageStatus.Empty {
			emptyStr = ", empty"
		}

		migrationStr := ""
		if table.Migration != nil {
			migrationStr = fmt.Sprintf(", migration: %s", table.Migration.State)
		}

		fmt.Fprintf(os.Stderr, "  - %s (shards: %d, indexes: %d, disk: %s%s%s)\n",
			table.Name, shardCount, indexCount, diskUsageStr, emptyStr, migrationStr)

		if len(table.Schema.DefaultType) > 0 {
			fmt.Fprintf(os.Stderr, "    DefaultType: %v\n", table.Schema.DefaultType)
		}
		if len(table.Schema.DocumentSchemas) > 0 {
			fmt.Fprintln(os.Stderr, "    DocumentSchemas:")
			for docType, fields := range table.Schema.DocumentSchemas {
				fmt.Fprintf(os.Stderr, "      - %s: %v\n", docType, fields)
			}
		}
	}
}

// CreateIndex creates a new index on a table
func (c *AntflyClient) CreateIndex(ctx context.Context, tableName, indexName, indexType, field, template string, dimension int, embedderConfigStr string, generatorConfigStr string, chunkerConfigStr string) error {
	// Validate that either field or template is specified, but not both
	if field == "" && template == "" {
		return fmt.Errorf("either --field or --template must be specified")
	}
	if field != "" && template != "" {
		return fmt.Errorf("--field and --template are mutually exclusive, specify only one")
	}

	var embCfg antfly.EmbedderConfig
	if embedderConfigStr != "" {
		if err := json.UnmarshalString(embedderConfigStr, &embCfg); err != nil {
			return fmt.Errorf("parsing --embedder JSON for index %s: %w", indexName, err)
		}
	}
	var sumCfg antfly.GeneratorConfig
	if generatorConfigStr != "" {
		if err := json.UnmarshalString(generatorConfigStr, &sumCfg); err != nil {
			return fmt.Errorf("parsing --generator JSON for index %s: %w", indexName, err)
		}
	}
	var chunkerCfg antfly.ChunkerConfig
	if chunkerConfigStr != "" {
		if err := json.UnmarshalString(chunkerConfigStr, &chunkerCfg); err != nil {
			return fmt.Errorf("parsing --chunker JSON for index %s: %w", indexName, err)
		}
	}

	indexConfig, err := antfly.NewIndexConfig(indexName, antfly.EmbeddingsIndexConfig{
		Field:      field,
		Template:   template,
		Dimension:  dimension,
		Embedder:   embCfg,
		Summarizer: sumCfg,
		Chunker:    chunkerCfg,
	})
	if err != nil {
		return fmt.Errorf("invalid index configuration for index %s: %w", indexName, err)
	}

	// Override the type if specified
	if indexType != "" {
		indexConfig.Type = antfly.IndexType(indexType)
	}

	if field != "" {
		fmt.Fprintf(os.Stderr, "Sending CREATE INDEX request for '%s' on table '%s', field '%s'\n", indexName, tableName, field)
	} else {
		fmt.Fprintf(os.Stderr, "Sending CREATE INDEX request for '%s' on table '%s', template '%s'\n", indexName, tableName, template)
	}

	if err := c.AntflyClient.CreateIndex(ctx, tableName, indexName, *indexConfig); err != nil {
		var apiErr *antfly.APIError
		if errors.As(err, &apiErr) && strings.Contains(apiErr.Message, "already exists") {
			fmt.Fprintf(os.Stderr, "Index '%s' already exists on table '%s', skipping creation.\n", indexName, tableName)
			return nil
		}
		return fmt.Errorf("failed to create index '%s': %w", indexName, err)
	}
	fmt.Fprintf(os.Stderr, "CREATE INDEX '%s' successful.\n", indexName)
	return nil
}

// DropIndex drops an index from a table
func (c *AntflyClient) DropIndex(ctx context.Context, tableName, indexName string) error {
	fmt.Fprintf(os.Stderr, "Sending DROP INDEX request for '%s' on table '%s'\n", indexName, tableName)
	err := c.AntflyClient.DropIndex(ctx, tableName, indexName)
	if err != nil {
		return fmt.Errorf("drop index request failed for '%s': %w", indexName, err)
	}
	fmt.Fprintf(os.Stderr, "DROP INDEX request for '%s' successful.\n", indexName)
	return nil
}

// printIndexDetails logs the configuration and stats of an index with the given indent prefix.
func printIndexDetails(index antfly.IndexStatus, indent string) {
	// Show config details based on index type
	switch index.Config.Type {
	case antfly.IndexTypeEmbeddings:
		embCfg, err := index.Config.AsEmbeddingsIndexConfig()
		if err != nil {
			break
		}
		if embCfg.Field != "" {
			fmt.Fprintf(os.Stderr, "%sField: %s\n", indent, embCfg.Field)
		}
		if embCfg.Template != "" {
			fmt.Fprintf(os.Stderr, "%sTemplate: %s\n", indent, embCfg.Template)
		}
		fmt.Fprintf(os.Stderr, "%sDimension: %d\n", indent, embCfg.Dimension)
		if embCfg.Embedder.Provider != "" {
			modelCfg := embCfg.Embedder
			fmt.Fprintf(os.Stderr, "%sEmbedder Provider: %s\n", indent, modelCfg.Provider)
			if ollama, err := modelCfg.AsOllamaEmbedderConfig(); err == nil && ollama.Model != "" {
				fmt.Fprintf(os.Stderr, "%sEmbedder Model: %s\n", indent, ollama.Model)
			} else if openai, err := modelCfg.AsOpenAIEmbedderConfig(); err == nil && openai.Model != "" {
				fmt.Fprintf(os.Stderr, "%sEmbedder Model: %s\n", indent, openai.Model)
			}
		}
		if termite, err := embCfg.Chunker.AsTermiteChunkerConfig(); err == nil {
			if termite.Model != "" || termite.Text.TargetTokens != 0 || termite.ApiUrl != "" || termite.MaxChunks != 0 || embCfg.Chunker.FullTextIndex != nil {
				provider := string(embCfg.Chunker.Provider)
				if provider == "" {
					provider = "termite"
				}
				fmt.Fprintf(os.Stderr, "%sChunker Provider: %s\n", indent, provider)
				if termite.Model != "" {
					fmt.Fprintf(os.Stderr, "%sChunker Model: %s\n", indent, termite.Model)
				}
				if termite.Text.TargetTokens != 0 {
					fmt.Fprintf(os.Stderr, "%sChunker Target Tokens: %d\n", indent, termite.Text.TargetTokens)
				}
				if termite.Text.OverlapTokens != 0 {
					fmt.Fprintf(os.Stderr, "%sChunker Overlap Tokens: %d\n", indent, termite.Text.OverlapTokens)
				}
				if termite.MaxChunks != 0 {
					fmt.Fprintf(os.Stderr, "%sChunker Max Chunks: %d\n", indent, termite.MaxChunks)
				}
				if termite.ApiUrl != "" {
					fmt.Fprintf(os.Stderr, "%sChunker API URL: %s\n", indent, termite.ApiUrl)
				}
				if embCfg.Chunker.FullTextIndex != nil {
					fmt.Fprintf(os.Stderr, "%sChunker Full Text Indexing: enabled\n", indent)
				}
				if embCfg.Chunker.StoreChunks {
					fmt.Fprintf(os.Stderr, "%sChunker Store Chunks: true\n", indent)
				}
			}
		}
		if embCfg.Summarizer.Provider != "" {
			genCfg := embCfg.Summarizer
			fmt.Fprintf(os.Stderr, "%sSummarizer Provider: %s\n", indent, genCfg.Provider)
			if ollama, err := genCfg.AsOllamaGeneratorConfig(); err == nil && ollama.Model != "" {
				fmt.Fprintf(os.Stderr, "%sSummarizer Model: %s\n", indent, ollama.Model)
			} else if openai, err := genCfg.AsOpenAIGeneratorConfig(); err == nil && openai.Model != "" {
				fmt.Fprintf(os.Stderr, "%sSummarizer Model: %s\n", indent, openai.Model)
			}
		}
	case antfly.IndexTypeFullText:
		if ftCfg, err := index.Config.AsFullTextIndexConfig(); err == nil {
			fmt.Fprintf(os.Stderr, "%sMemOnly: %v\n", indent, ftCfg.MemOnly)
		}
	}

	// Show stats
	if ftStats, err := index.Status.AsFullTextIndexStats(); err == nil {
		if ftStats.Error != "" {
			fmt.Fprintf(os.Stderr, "%sStatus Error: %s\n", indent, ftStats.Error)
		} else {
			fmt.Fprintf(os.Stderr, "%sTotal Indexed: %d\n", indent, ftStats.TotalIndexed)
			fmt.Fprintf(os.Stderr, "%sDisk Usage: %s\n", indent, formatBytes(ftStats.DiskUsage))
			if ftStats.Rebuilding {
				fmt.Fprintf(os.Stderr, "%sRebuilding: true\n", indent)
			}
		}
	} else if embStats, err := index.Status.AsEmbeddingsIndexStats(); err == nil {
		if embStats.Error != "" {
			fmt.Fprintf(os.Stderr, "%sStatus Error: %s\n", indent, embStats.Error)
		} else {
			fmt.Fprintf(os.Stderr, "%sTotal Indexed: %d\n", indent, embStats.TotalIndexed)
			fmt.Fprintf(os.Stderr, "%sTotal Nodes: %d\n", indent, embStats.TotalNodes)
			fmt.Fprintf(os.Stderr, "%sDisk Usage: %s\n", indent, formatBytes(embStats.DiskUsage))
		}
	}
}

// ListIndexes lists all indexes for a table
func (c *AntflyClient) ListIndexes(ctx context.Context, tableName string) ([]antfly.IndexStatus, error) {
	fmt.Fprintf(os.Stderr, "Sending LIST INDEXES request for table '%s'\n", tableName)

	indexesMap, err := c.AntflyClient.ListIndexes(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("list indexes request failed: %w", err)
	}

	indexes := make([]antfly.IndexStatus, 0, len(indexesMap))
	for _, index := range indexesMap {
		indexes = append(indexes, index)
	}

	slices.SortFunc(indexes, func(a, b antfly.IndexStatus) int {
		return cmp.Compare(a.Config.Name, b.Config.Name)
	})

	return indexes, nil
}

// printIndexList prints a human-readable list of indexes to stderr.
func printIndexList(tableName string, indexes []antfly.IndexStatus) {
	if len(indexes) == 0 {
		fmt.Fprintf(os.Stderr, "No indexes found for table '%s'.\n", tableName)
		return
	}

	fmt.Fprintf(os.Stderr, "Found %d index(es) for table '%s':\n", len(indexes), tableName)
	for _, index := range indexes {
		fmt.Fprintf(os.Stderr, "  - %s (type: %s)\n", index.Config.Name, index.Config.Type)
		printIndexDetails(index, "    ")
	}
}

// GetIndex gets an index for a table
func (c *AntflyClient) GetIndex(ctx context.Context, tableName, indexName string) (*antfly.IndexStatus, error) {
	fmt.Fprintf(os.Stderr, "Sending GET INDEX request for table '%s' index '%s'\n", tableName, indexName)
	index, err := c.AntflyClient.GetIndex(ctx, tableName, indexName)
	if err != nil {
		return nil, fmt.Errorf("get index request failed: %w", err)
	}
	return index, nil
}

// printIndexStatus prints a human-readable summary of an index to stderr.
func printIndexStatus(index *antfly.IndexStatus) {
	fmt.Fprintf(os.Stderr, "%s (type: %s)\n", index.Config.Name, index.Config.Type)
	printIndexDetails(*index, "  ")
}

// Backup backs up a table
func (c *AntflyClient) Backup(ctx context.Context, tableName, backupID, location string) error {
	if tableName == "" {
		return fmt.Errorf("empty table name")
	}
	fmt.Fprintf(os.Stderr, "Sending BACKUP request for table '%s', backupID '%s', location '%s'\n", tableName, backupID, location)

	err := c.AntflyClient.Backup(ctx, tableName, backupID, location)
	if err != nil {
		return fmt.Errorf("backup request failed: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Backup request for table '%s' successfully sent/acknowledged\n", tableName)
	return nil
}

// Restore restores a table from a backup
func (c *AntflyClient) Restore(ctx context.Context, tableName, backupID, location string) error {
	if tableName == "" {
		return fmt.Errorf("empty table name")
	}
	fmt.Fprintf(os.Stderr, "Sending RESTORE request for new table '%s' from backupID '%s', location '%s'\n", tableName, backupID, location)

	err := c.AntflyClient.Restore(ctx, tableName, backupID, location)
	if err != nil {
		return fmt.Errorf("restore request failed: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Restore request for table '%s' successfully sent/acknowledged\n", tableName)
	return nil
}

// ClusterBackup backs up multiple tables or all tables
func (c *AntflyClient) ClusterBackup(ctx context.Context, backupID, location string, tableNames []string) error {
	desc := "all tables"
	if len(tableNames) > 0 {
		desc = strings.Join(tableNames, ", ")
	}
	fmt.Fprintf(os.Stderr, "Sending BACKUP request for %s, backupID '%s', location '%s'\n", desc, backupID, location)

	result, err := c.AntflyClient.ClusterBackup(ctx, backupID, location, tableNames)
	if err != nil {
		return fmt.Errorf("backup request failed: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Backup completed with status: %s\n", result.Status)
	for _, table := range result.Tables {
		if table.Error != "" {
			fmt.Fprintf(os.Stderr, "  - %s: %s (error: %s)\n", table.Name, table.Status, table.Error)
		} else {
			fmt.Fprintf(os.Stderr, "  - %s: %s\n", table.Name, table.Status)
		}
	}
	return nil
}

// ClusterRestore restores multiple tables from a cluster backup
func (c *AntflyClient) ClusterRestore(ctx context.Context, backupID, location string, tableNames []string, restoreMode string) error {
	desc := "all tables"
	if len(tableNames) > 0 {
		desc = strings.Join(tableNames, ", ")
	}
	fmt.Fprintf(os.Stderr, "Sending RESTORE request for %s from backupID '%s', location '%s', mode '%s'\n", desc, backupID, location, restoreMode)

	result, err := c.AntflyClient.ClusterRestore(ctx, backupID, location, tableNames, restoreMode)
	if err != nil {
		return fmt.Errorf("restore request failed: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Restore triggered with status: %s\n", result.Status)
	for _, table := range result.Tables {
		if table.Error != "" {
			fmt.Fprintf(os.Stderr, "  - %s: %s (error: %s)\n", table.Name, table.Status, table.Error)
		} else {
			fmt.Fprintf(os.Stderr, "  - %s: %s\n", table.Name, table.Status)
		}
	}
	return nil
}

// ListBackups lists available cluster backups at a location
func (c *AntflyClient) ListBackups(ctx context.Context, location string) error {
	fmt.Fprintf(os.Stderr, "Sending LIST BACKUPS request for location '%s'\n", location)

	backups, err := c.AntflyClient.ListBackups(ctx, location)
	if err != nil {
		return fmt.Errorf("list backups request failed: %w", err)
	}

	if len(backups) == 0 {
		fmt.Fprintln(os.Stderr, "No backups found at the specified location.")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Found %d backup(s):\n", len(backups))
	for _, backup := range backups {
		fmt.Fprintf(os.Stderr, "  - %s\n", backup.BackupID)
		fmt.Fprintf(os.Stderr, "      Timestamp: %s\n", backup.Timestamp)
		fmt.Fprintf(os.Stderr, "      Tables: %s\n", strings.Join(backup.Tables, ", "))
		if backup.AntflyVersion != "" {
			fmt.Fprintf(os.Stderr, "      Antfly Version: %s\n", backup.AntflyVersion)
		}
	}
	return nil
}

// convertBleveQuery converts a bleve query to an antfly query type via JSON round-trip.
// Returns nil if the input is nil.
func convertBleveQuery(q blevequery.Query, label string) (*query.Query, error) {
	if q == nil {
		return nil, nil
	}
	qBytes, err := json.Marshal(q)
	if err != nil {
		return nil, fmt.Errorf("marshalling %s query: %w", label, err)
	}
	var antflyQuery query.Query
	if err := json.Unmarshal(qBytes, &antflyQuery); err != nil {
		return nil, fmt.Errorf("converting %s query: %w", label, err)
	}
	return &antflyQuery, nil
}

// convertSearchQueries converts the full-text search, filter, and exclusion bleve
// queries into antfly query types, populating the corresponding fields on the request.
func convertSearchQueries(req *antfly.QueryRequest, fullTextSearch, filterQuery, exclusionQuery blevequery.Query) error {
	fts, err := convertBleveQuery(fullTextSearch, "full text search")
	if err != nil {
		return err
	}
	req.FullTextSearch = fts

	fq, err := convertBleveQuery(filterQuery, "filter")
	if err != nil {
		return err
	}
	req.FilterQuery = fq

	eq, err := convertBleveQuery(exclusionQuery, "exclusion")
	if err != nil {
		return err
	}
	req.ExclusionQuery = eq

	return nil
}

// Query queries data from a table
func (c *AntflyClient) Query(ctx context.Context, params SearchParams, verbose bool) (*antfly.QueryResponses, error) {
	opts := antfly.QueryRequest{
		Table:        params.Table,
		Fields:       params.Fields,
		Limit:        params.Limit,
		Offset:       params.Offset,
		OrderBy:      params.OrderBy,
		SearchAfter:  params.SearchAfter,
		SearchBefore: params.SearchBefore,
		FilterPrefix: params.FilterPrefix,
		Aggregations: params.Aggregations,
		Reranker:     params.Reranker,
		Pruner:       params.Pruner,
	}

	if err := convertSearchQueries(&opts, params.FullTextSearch, params.FilterQuery, params.ExclusionQuery); err != nil {
		return nil, err
	}

	if len(params.Indexes) > 0 {
		if params.SemanticSearch == "" {
			return nil, errors.New("missing option `semantic_search` for vector search")
		}
		opts.SemanticSearch = params.SemanticSearch
		opts.Indexes = params.Indexes
		if params.Offset > 0 {
			return nil, errors.New("invalid option `offset` for vector search")
		}
	}
	if verbose {
		queryBodyBytes, err := json.EncodeIndented(opts, "", "  ", json.SortMapKeys)
		if err != nil {
			return nil, fmt.Errorf("marshalling query: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Query options:\n%s\n", string(queryBodyBytes))
	}

	fmt.Fprintf(os.Stderr, "Sending query request for table '%s'\n", params.Table)
	result, err := c.AntflyClient.Query(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	return result, nil
}

// BatchLoad loads data from a file into a table
func (c *AntflyClient) BatchLoad(ctx context.Context, tableName, filePath string, numBatches, batchSize, concurrency int, idField, idTemplate string, rateLimit float64, verbose bool) error {
	if filePath == "" {
		return fmt.Errorf("file path cannot be empty for bulk load")
	}
	if numBatches == 0 || batchSize == 0 {
		fmt.Fprintln(os.Stderr, "No bulk load to perform (batches or size is 0).")
		return nil
	}

	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer func() { _ = file.Close() }()

	fmt.Fprintf(os.Stderr, "Starting bulk load from file '%s' into table '%s'\n", filePath, tableName)
	if rateLimit > 0 {
		fmt.Fprintf(os.Stderr, "Processing up to %d batches of %d items using %d workers (rate limit: %.0f items/sec)\n",
			numBatches, batchSize, concurrency, rateLimit)
	} else {
		fmt.Fprintf(os.Stderr, "Processing up to %d batches of %d items using %d workers\n",
			numBatches, batchSize, concurrency)
	}

	type batchJob struct {
		request antfly.BatchRequest
		id      int
	}

	insertJobs := make(chan batchJob, concurrency) // Buffered channel for workers
	var wg sync.WaitGroup
	var itemsProcessed int64

	var mu sync.Mutex
	var batchErrors []error

	// Create rate limiter if rate limit is specified
	var limiter *rate.Limiter
	if rateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(rateLimit), batchSize)
	}

	// Start worker goroutines
	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for job := range insertJobs {
				r := retry.NewExponential(1 * time.Second)
				r = retry.WithJitter(500*time.Millisecond, r)
				r = retry.WithMaxRetries(5, r)
				rv := 0

				err := retry.Do(ctx, r, func(ctx context.Context) error {
					defer func() {
						rv++
					}()
					_, err := c.Batch(ctx, tableName, job.request)
					if err != nil {
						if strings.Contains(err.Error(), "currently splitting") ||
							strings.Contains(err.Error(), "string unexpected end of") ||
							strings.Contains(err.Error(), "no leader elected for shard") ||
							strings.Contains(err.Error(), "key out of range") {
							if verbose {
								fmt.Fprintf(os.Stderr, "Worker %d batch %d retrying: %v\n", workerID, job.id, err)
							}
							return retry.RetryableError(err)
						}
						if strings.Contains(err.Error(), "exceeded while awaiting headers") ||
							strings.Contains(err.Error(), "raft proposal dropped") {
							if verbose {
								fmt.Fprintf(os.Stderr, "Worker %d batch %d retrying after wait: %v\n", workerID, job.id, err)
							}
							select {
							case <-time.After(20 * time.Second * time.Duration(rv+1)):
								return retry.RetryableError(err)
							case <-ctx.Done():
								return ctx.Err()
							}
						}
						return err

					}
					return nil
				})

				if err != nil {
					fmt.Fprintf(os.Stderr, "Worker %d batch %d insert failed: %v\n", workerID, job.id, err)
					mu.Lock()
					batchErrors = append(batchErrors, fmt.Errorf("batch %d: %w", job.id, err))
					mu.Unlock()
				}
			}
		}(w)
	}

	scanner := bufio.NewScanner(file)
	const maxCapacity = 5 * 1024 * 1024 // 5MB, adjust if lines can be extremely long
	buf := make([]byte, 0, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, maxCapacity)

	var totalItemsSentSoFar int

	startTime := time.Now()

	// Parse ID template if provided
	var idTmpl *template.Template
	if idTemplate != "" {
		var err error
		idTmpl, err = template.New("id").Parse(idTemplate)
		if err != nil {
			return fmt.Errorf("failed to parse ID template: %w", err)
		}
	}

	hasher := xxhash.New()
fileLoop:
	for batchNum := range numBatches {
		inserts := make(map[string]any)
		itemsInCurrentBatch := 0

		for range batchSize {
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					fmt.Fprintf(os.Stderr, "Error scanning file: %v\n", err)
				}
				break // EOF or error
			}
			lineBytes := scanner.Text()
			if len(lineBytes) == 0 {
				continue // Skip empty lines
			}

			var itemData map[string]any
			if err := json.UnmarshalString(lineBytes, &itemData); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to unmarshal JSON line: %v. Line: %q\n", err, lineBytes) //#nosec G705 -- stderr, not a web response
				continue
			}

			// Determine document ID based on configuration
			var docID string
			if idField != "" {
				// Use specified field as ID
				if fieldValue, ok := itemData[idField]; ok {
					docID = fmt.Sprintf("%v", fieldValue)
				} else {
					fmt.Fprintf(os.Stderr, "ID field '%s' not found in JSON data, using hash. Line: %q\n", idField, lineBytes) //#nosec G705 -- stderr, not a web response
				}
			} else if idTmpl != nil {
				// Use template to construct ID
				var buf bytes.Buffer
				if err := idTmpl.Execute(&buf, itemData); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to execute ID template: %v. Using hash. Line: %q\n", err, lineBytes) //#nosec G705 -- stderr, not a web response
				} else {
					docID = buf.String()
				}
			}

			// Fall back to hash if no ID was determined
			if docID == "" {
				if _, err := hasher.Write([]byte(lineBytes)); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to hash line (ID generation): %v. Line: %q\n", err, lineBytes) //#nosec G705 -- stderr, not a web response
					continue
				}
				docID = strconv.FormatUint(hasher.Sum64(), 16)
				hasher.Reset()
			}
			inserts[docID] = itemData
			itemsInCurrentBatch++
			itemsProcessed++
		}

		if len(inserts) > 0 {
			// Apply rate limiting if configured
			if limiter != nil {
				if err := limiter.WaitN(ctx, len(inserts)); err != nil {
					fmt.Fprintf(os.Stderr, "Rate limiter error: %v\n", err)
				}
			}

			job := batchJob{
				request: antfly.BatchRequest{Inserts: inserts},
				id:      batchNum + 1,
			}
			insertJobs <- job
			totalItemsSentSoFar += len(inserts)
			if verbose && (batchNum+1)%100 == 0 {
				fmt.Fprintf(os.Stderr, "Sent batch %d/%d (%d items so far)\n", batchNum+1, numBatches, totalItemsSentSoFar)
			}
		}

		if itemsInCurrentBatch < batchSize {
			break fileLoop
		}
	}

	close(insertJobs)
	wg.Wait()
	duration := time.Since(startTime)

	if duration.Seconds() > 0 {
		throughput := float64(itemsProcessed) / duration.Seconds()
		fmt.Fprintf(os.Stderr, "Completed %d item inserts from file in %.2f seconds (%.2f inserts/sec)\n", itemsProcessed, duration.Seconds(), throughput)
	} else {
		fmt.Fprintf(os.Stderr, "Completed %d item inserts from file.\n", itemsProcessed)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error during file scanning: %w", err)
	}

	if len(batchErrors) > 0 {
		return fmt.Errorf("%d batch(es) failed; first error: %w", len(batchErrors), batchErrors[0])
	}

	return nil
}

// GetTable gets a single table's status and details
func (c *AntflyClient) GetTable(ctx context.Context, tableName string) (*antfly.TableStatus, error) {
	fmt.Fprintf(os.Stderr, "Sending GET TABLE request for '%s'\n", tableName)
	table, err := c.AntflyClient.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table request failed: %w", err)
	}
	return table, nil
}

// printTableStatus prints a human-readable summary of a table to stderr.
func printTableStatus(table *antfly.TableStatus) {
	fmt.Fprintf(os.Stderr, "Table: %s\n", table.Name)
	if table.Description != "" {
		fmt.Fprintf(os.Stderr, "  Description: %s\n", table.Description)
	}

	diskUsageStr := formatBytes(table.StorageStatus.DiskUsage)
	fmt.Fprintf(os.Stderr, "  Shards: %d\n", len(table.Shards))
	fmt.Fprintf(os.Stderr, "  Disk Usage: %s\n", diskUsageStr)
	if table.StorageStatus.Empty {
		fmt.Fprintf(os.Stderr, "  Empty: true\n")
	}

	if table.Migration != nil {
		fmt.Fprintf(os.Stderr, "  Migration: %s (serving reads from schema v%d)\n",
			table.Migration.State, table.Migration.ReadSchema.Version)
	}

	if len(table.Schema.DefaultType) > 0 {
		fmt.Fprintf(os.Stderr, "  DefaultType: %v\n", table.Schema.DefaultType)
	}
	if len(table.Schema.DocumentSchemas) > 0 {
		fmt.Fprintln(os.Stderr, "  DocumentSchemas:")
		for docType, fields := range table.Schema.DocumentSchemas {
			fmt.Fprintf(os.Stderr, "    - %s: %v\n", docType, fields)
		}
	}

	if len(table.Indexes) > 0 {
		type namedIndex struct {
			name string
			cfg  antfly.IndexConfig
		}
		indexes := make([]namedIndex, 0, len(table.Indexes))
		for name, cfg := range table.Indexes {
			indexes = append(indexes, namedIndex{name: name, cfg: cfg})
		}
		slices.SortFunc(indexes, func(a, b namedIndex) int {
			return cmp.Compare(a.name, b.name)
		})

		fmt.Fprintf(os.Stderr, "  Indexes (%d):\n", len(indexes))
		for _, index := range indexes {
			fmt.Fprintf(os.Stderr, "    - %s (type: %s)\n", index.name, index.cfg.Type)
			printIndexDetails(antfly.IndexStatus{Config: index.cfg}, "      ")
		}
	}
}

// LookupKey looks up a document by its key
func (c *AntflyClient) LookupKey(ctx context.Context, tableName, key string) error {
	fmt.Fprintf(os.Stderr, "Sending LOOKUP KEY request for table '%s', key '%s'\n", tableName, key)

	document, err := c.AntflyClient.LookupKey(ctx, tableName, key)
	if err != nil {
		return fmt.Errorf("lookup key request failed: %w", err)
	}

	// Format and display the result
	resultJSON, err := json.EncodeIndented(document, "", "  ", json.SortMapKeys)
	if err != nil {
		return fmt.Errorf("failed to marshal result to JSON: %w", err)
	}
	fmt.Fprintln(os.Stderr, "Lookup successful.")
	fmt.Println(string(resultJSON))
	return nil
}

// RunRetrievalAgent performs a retrieval agent query with the given parameters
func (c *AntflyClient) RunRetrievalAgent(ctx context.Context, params SearchParams, docTemplate string, ragReq antfly.RetrievalAgentRequest, ragOpts antfly.RetrievalAgentOptions) (*antfly.RetrievalAgentResult, error) {
	queryReq := antfly.QueryRequest{
		Table:            params.Table,
		Fields:           params.Fields,
		Limit:            params.Limit,
		FilterPrefix:     params.FilterPrefix,
		Reranker:         params.Reranker,
		Pruner:           params.Pruner,
		DocumentRenderer: docTemplate,
	}

	if err := convertSearchQueries(&queryReq, params.FullTextSearch, params.FilterQuery, params.ExclusionQuery); err != nil {
		return nil, err
	}

	if len(params.Indexes) > 0 {
		if params.SemanticSearch == "" {
			return nil, errors.New("missing option `semantic_search` for vector search")
		}
		queryReq.SemanticSearch = params.SemanticSearch
		queryReq.Indexes = params.Indexes
	}

	// Build a RetrievalQueryRequest from the query by JSON round-tripping
	queryJSON, err := json.Marshal(queryReq)
	if err != nil {
		return nil, fmt.Errorf("marshalling query request: %w", err)
	}
	var retrievalQuery antfly.RetrievalQueryRequest
	if err := json.Unmarshal(queryJSON, &retrievalQuery); err != nil {
		return nil, fmt.Errorf("converting query request: %w", err)
	}
	ragReq.Queries = []antfly.RetrievalQueryRequest{retrievalQuery}

	if !ragReq.Stream {
		fmt.Fprintf(os.Stderr, "Sending retrieval agent request for table '%s'\n", params.Table)
	}

	result, err := c.RetrievalAgent(ctx, ragReq, ragOpts)
	if err != nil {
		return nil, fmt.Errorf("executing retrieval agent query: %w", err)
	}

	return result, nil
}

// RunQueryBuilder generates a structured Bleve query from a natural language intent
func (c *AntflyClient) RunQueryBuilder(ctx context.Context, req antfly.QueryBuilderRequest) (*antfly.QueryBuilderResult, error) {
	fmt.Fprintln(os.Stderr, "Generating query...")
	result, err := c.QueryBuilder(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("executing query builder: %w", err)
	}
	return result, nil
}

// AddMetadataPeer adds a peer to the metadata raft cluster
func (c *AntflyClient) AddMetadataPeer(nodeID uint64, raftURL string) error {
	return c.internalClient.AddMetadataPeer(nodeID, raftURL)
}

// RemoveMetadataPeer removes a peer from the metadata raft cluster
func (c *AntflyClient) RemoveMetadataPeer(nodeID uint64) error {
	return c.internalClient.RemoveMetadataPeer(nodeID)
}

// GetMetadataStatus gets the status of the metadata raft cluster
func (c *AntflyClient) GetMetadataStatus() (*admin.MetadataStatus, error) {
	return c.internalClient.GetMetadataStatus()
}
