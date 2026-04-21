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
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof" // Enable pprof endpoints
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/docsaf"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// startMemoryMonitor logs memory stats every interval. Returns a cancel function.
func startMemoryMonitor(t *testing.T, interval time.Duration) func() {
	t.Helper()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		count := 0
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				count++
				// Get RSS without forcing GC (to see actual usage)
				rssMB := float64(0)
				if out, err := exec.Command("ps", "-o", "rss=", "-p", fmt.Sprintf("%d", os.Getpid())).Output(); err == nil {
					var rssKB int64
					if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &rssKB); err == nil {
						rssMB = float64(rssKB) / 1024
					}
				}
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				t.Logf("MEMORY MONITOR [%d]: RSS=%.0fMB GoHeap=%.0fMB GoSys=%.0fMB",
					count, rssMB, float64(m.HeapAlloc)/1024/1024, float64(m.Sys)/1024/1024)
			}
		}
	}()
	return func() { close(done) }
}

// dumpMemoryProfile captures memory stats and optionally dumps a heap profile.
// It logs both Go heap stats and process RSS (which includes native/CGO memory).
func dumpMemoryProfile(t *testing.T, label string, dumpHeap bool) {
	t.Helper()

	// Force GC to get accurate stats
	runtime.GC()

	// Go heap stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	goHeapMB := float64(m.HeapAlloc) / 1024 / 1024
	goSysMB := float64(m.Sys) / 1024 / 1024

	// Process RSS (includes native memory from ONNX/CoreML)
	rssMB := float64(0)
	if out, err := exec.Command("ps", "-o", "rss=", "-p", fmt.Sprintf("%d", os.Getpid())).Output(); err == nil {
		var rssKB int64
		if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &rssKB); err == nil {
			rssMB = float64(rssKB) / 1024
		}
	}

	t.Logf("MEMORY [%s]: GoHeap=%.1fMB GoSys=%.1fMB ProcessRSS=%.1fMB (native≈%.1fMB)",
		label, goHeapMB, goSysMB, rssMB, rssMB-goSysMB)

	// Optionally dump heap profile
	if dumpHeap {
		profilePath := filepath.Join(os.TempDir(), fmt.Sprintf("heap_%s_%d.prof", label, time.Now().Unix()))
		if f, err := os.Create(profilePath); err == nil {
			if err := pprof.WriteHeapProfile(f); err == nil {
				t.Logf("  Heap profile saved to: %s", profilePath)
			}
			f.Close()
		}
	}
}

// indexAntflyDocsWithHierarchy indexes Antfly documentation with a graph index for tree search.
// It creates both an embedding index and a graph index (doc_hierarchy) with parent-child relationships
// based on file path structure. This enables PageIndex-style tree search navigation.
func indexAntflyDocsWithHierarchy(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string) int {
	t.Helper()

	// Find repository root by walking up the directory tree
	repoRoot := findRepoRoot(t)
	docsDir := filepath.Join(repoRoot, "www", "content", "docs")
	t.Logf("Indexing Antfly docs with hierarchy from: %s", docsDir)

	// Create docsaf processor with filesystem source
	source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
		BaseDir: docsDir,
		IncludePatterns: []string{
			"**/*.md",
			"**/*.mdx",
		},
		ExcludePatterns: []string{
			"**/node_modules/**",
			"**/vendor/**",
			"**/.git/**",
			"**/FIGMA.md",
			"**/component-test.mdx",
		},
	})

	processor := docsaf.NewProcessor(source, docsaf.DefaultRegistry())

	// Process files
	sections, err := processor.Process(ctx)
	require.NoError(t, err, "Failed to traverse documentation")
	require.NotEmpty(t, sections, "No documents found")

	t.Logf("Found %d documents", len(sections))

	// Build file path hierarchy and add parent_id to documents
	records := make(map[string]any)
	ids := make([]string, 0, len(sections))

	// Create a map of directory paths to their synthetic index document IDs
	// This helps establish parent-child relationships based on file structure
	dirToID := make(map[string]string)
	for _, section := range sections {
		// Use file path without extension as base
		dirPath := filepath.Dir(section.FilePath)
		fileName := filepath.Base(section.FilePath)

		// If this is an index file, it represents the directory itself
		if strings.HasPrefix(fileName, "index.") || strings.HasPrefix(fileName, "_index.") {
			dirToID[dirPath] = section.ID
		}
	}

	// Build records with parent_id field
	for _, section := range sections {
		doc := section.ToDocument()

		// Determine parent based on file path
		dirPath := filepath.Dir(section.FilePath)
		parentDir := filepath.Dir(dirPath)
		fileName := filepath.Base(section.FilePath)

		// Assign parent_id
		if strings.HasPrefix(fileName, "index.") || strings.HasPrefix(fileName, "_index.") {
			// Index files' parent is the parent directory's index
			if parentID, ok := dirToID[parentDir]; ok && parentDir != dirPath {
				doc["parent_id"] = parentID
			}
		} else {
			// Non-index files' parent is the current directory's index
			if parentID, ok := dirToID[dirPath]; ok {
				doc["parent_id"] = parentID
			}
		}

		records[section.ID] = doc
		ids = append(ids, section.ID)
	}

	// Sort IDs for deterministic pagination
	sortedIDs := slices.Sorted(slices.Values(ids))

	// Create embedding index config
	embeddingIndexConfig := antfly.IndexConfig{
		Name: "embeddings",
		Type: "aknn_v0",
	}

	embedder, err := GetDefaultEmbedderConfig(t)
	require.NoError(t, err, "Failed to configure embedder")

	chunker := antfly.ChunkerConfig{}
	err = chunker.FromAntflyChunkerConfig(antfly.AntflyChunkerConfig{
		Text: antfly.TextChunkOptions{
			TargetTokens:  512,
			OverlapTokens: 50,
		},
	})
	require.NoError(t, err, "Failed to configure fixed chunker")

	err = embeddingIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *embedder,
		Chunker:  chunker,
	})
	require.NoError(t, err, "Failed to configure embedding index")

	// Create graph index config for document hierarchy
	graphIndexConfig := antfly.IndexConfig{
		Name: "doc_hierarchy",
		Type: "graph_v0",
	}

	err = graphIndexConfig.FromGraphIndexConfig(antfly.GraphIndexConfig{
		EdgeTypes: []antfly.EdgeTypeConfig{
			{
				Name:     "child_of",
				Field:    "parent_id",                       // Auto-create edges from parent_id field
				Topology: antfly.EdgeTypeConfigTopologyTree, // Tree topology (single parent per node)
			},
		},
	})
	require.NoError(t, err, "Failed to configure graph index")

	// Create table with embedding and graph indexes
	err = client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"embeddings":    embeddingIndexConfig,
			"doc_hierarchy": graphIndexConfig,
		},
	})
	require.NoError(t, err, "Failed to create table")

	t.Logf("Created table with embedding and graph (doc_hierarchy) indexes")

	// Wait for shards to be ready
	waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

	// Load data using paginated LinearMerge
	const batchSize = 25
	totalBatches := (len(sortedIDs) + batchSize - 1) / batchSize
	t.Logf("Loading %d documents in %d batches", len(sortedIDs), totalBatches)

	cursor := ""
	totalUpserted := 0

	for batchNum := range totalBatches {
		start := batchNum * batchSize
		end := min(start+batchSize, len(sortedIDs))

		batchIDs := sortedIDs[start:end]
		batchRecords := make(map[string]any, len(batchIDs))
		for _, id := range batchIDs {
			batchRecords[id] = records[id]
		}

		result, err := client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
			Records:      batchRecords,
			LastMergedId: cursor,
			DryRun:       false,
			SyncLevel:    antfly.SyncLevelAknn,
		})
		require.NoError(t, err, "Failed to perform linear merge for batch %d", batchNum+1)

		if result.NextCursor != "" {
			cursor = result.NextCursor
		} else {
			cursor = batchIDs[len(batchIDs)-1]
		}

		totalUpserted += result.Upserted
	}

	t.Logf("Indexed %d documents with hierarchy", totalUpserted)
	return totalUpserted
}

// indexAntflyDocs indexes Antfly documentation and returns the number of documents upserted.
func indexAntflyDocs(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string) int {
	t.Helper()

	// Find repository root by walking up the directory tree
	repoRoot := findRepoRoot(t)
	docsDir := filepath.Join(repoRoot, "www", "content", "docs")
	t.Logf("Indexing Antfly docs from: %s", docsDir)

	// Create docsaf processor with filesystem source
	// Index all markdown documentation files recursively
	// Use ** patterns to allow directory traversal
	source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
		BaseDir: docsDir,
		IncludePatterns: []string{
			"**/*.md",  // All markdown files in all subdirectories
			"**/*.mdx", // All MDX files in all subdirectories
		},
		ExcludePatterns: []string{
			"**/node_modules/**",
			"**/vendor/**",
			"**/.git/**",
			"**/FIGMA.md",           // Internal Claude Code documentation
			"**/component-test.mdx", // Test file, not user documentation
		},
	})

	processor := docsaf.NewProcessor(source, docsaf.DefaultRegistry())

	// Process files
	sections, err := processor.Process(ctx)
	require.NoError(t, err, "Failed to traverse documentation")
	require.NotEmpty(t, sections, "No documents found")

	t.Logf("Found %d documents", len(sections))

	// Convert to records and collect IDs for sorting
	records := make(map[string]any)
	ids := make([]string, 0, len(sections))
	for _, section := range sections {
		records[section.ID] = section.ToDocument()
		ids = append(ids, section.ID)
	}

	// Sort IDs for deterministic pagination
	sortedIDs := slices.Sorted(slices.Values(ids))

	// Create embedding index config for embeddinggemma (768 dimensions)
	embeddingIndexConfig := antfly.IndexConfig{
		Name: "embeddings",
		Type: "aknn_v0",
	}

	// Configure embedder based on E2E_PROVIDER
	embedder, err := GetDefaultEmbedderConfig(t)
	require.NoError(t, err, "Failed to configure embedder")

	// Configure fixed-size chunker (no ML model needed - reduces memory usage)
	chunker := antfly.ChunkerConfig{}
	err = chunker.FromAntflyChunkerConfig(antfly.AntflyChunkerConfig{
		Text: antfly.TextChunkOptions{
			TargetTokens:  512,
			OverlapTokens: 50,
		},
	})
	require.NoError(t, err, "Failed to configure fixed chunker")

	// Configure embedding index with chunking
	err = embeddingIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *embedder,
		Chunker:  chunker,
	})
	require.NoError(t, err, "Failed to configure embedding index")

	// Create table with BM25 (automatic) and embedding indexes
	err = client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"embeddings": embeddingIndexConfig,
		},
	})
	require.NoError(t, err, "Failed to create table")

	t.Logf("Created table with BM25 and embedding indexes (embeddinggemma, 768 dims) with fixed-size chunking (512 tokens)")

	// Wait for shards to be ready
	waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

	// Load data using paginated LinearMerge (25 records per batch)
	const batchSize = 25
	totalBatches := (len(sortedIDs) + batchSize - 1) / batchSize
	t.Logf("Loading %d documents in %d batches of %d records", len(sortedIDs), totalBatches, batchSize)

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

		t.Logf("[Batch %d/%d] Merging records %d-%d (IDs: %s...%s)",
			batchNum+1, totalBatches, start+1, end,
			batchIDs[0], batchIDs[len(batchIDs)-1])

		// Execute LinearMerge with aknn sync level
		result, err := client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
			Records:      batchRecords,
			LastMergedId: cursor,
			DryRun:       false,
			SyncLevel:    antfly.SyncLevelAknn, // Wait for vector index writes
		})
		require.NoError(t, err, "Failed to perform linear merge for batch %d", batchNum+1)

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
		t.Logf("  Completed in %s - Status: %s", result.Took, result.Status)
		t.Logf("  Upserted: %d, Skipped: %d, Deleted: %d, Keys scanned: %d",
			result.Upserted, result.Skipped, result.Deleted, result.KeysScanned)

		if len(result.Failed) > 0 {
			t.Logf("  Failed operations: %d", len(result.Failed))
			for i, fail := range result.Failed {
				t.Logf("    [%d] ID=%s, Error=%s", i, fail.Id, fail.Error)
			}
		}
		if result.Message != "" {
			t.Logf("  Message: %s", result.Message)
		}
	}

	// Log final totals
	t.Logf("Linear merge complete: %d batches processed", totalBatches)
	t.Logf("  Total upserted: %d, skipped: %d, deleted: %d", totalUpserted, totalSkipped, totalDeleted)
	require.Positive(t, totalUpserted, "No documents were upserted")

	return totalUpserted
}

// TestQuery represents a test query with expected validation criteria
type TestQuery struct {
	Query            string   `json:"query"`
	ExpectedKeywords []string `json:"expected_keywords"`
	Description      string   `json:"description"`
}

// loadTestQueries loads test queries from JSON file relative to the e2e directory
func loadTestQueries(t *testing.T, filename string) []TestQuery {
	t.Helper()
	return loadJSON[[]TestQuery](t, filename)
}

// QueryResult represents the result of a single query execution
type QueryResult struct {
	Query            string
	Response         *antfly.RetrievalAgentResult
	Error            error
	ContainsKeywords bool
	KeywordsFound    []string
	KeywordsMissing  []string
	EvalResult       *antfly.EvalResult
}

// executeRetrievalAgentQueries runs queries and validates basic response quality.
// When using Gemini provider, queries run in parallel for faster execution.
func executeRetrievalAgentQueries(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	cfg testQueryConfig,
	queries []TestQuery,
) []QueryResult {
	t.Helper()

	results := make([]QueryResult, len(queries))

	provider := GetE2EProvider()
	if provider == "gemini" || provider == "termite" {
		t.Logf("Running %d queries in parallel (%s mode, concurrency=4)", len(queries), provider)
		executeQueriesParallel(t, ctx, client, cfg, queries, results)
	} else {
		t.Logf("Running %d queries sequentially (Ollama mode)", len(queries))
		executeQueriesSequential(t, ctx, client, cfg, queries, results)
	}

	return results
}

// newGeminiRateLimiter creates a rate limiter configured for 15 requests per minute.
func newGeminiRateLimiter() *rate.Limiter {
	return rate.NewLimiter(rate.Limit(15.0/60.0), 1)
}

func executeQueriesParallel(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	cfg testQueryConfig,
	queries []TestQuery,
	results []QueryResult,
) {
	t.Helper()

	const maxConcurrency = 4
	sem := make(chan struct{}, maxConcurrency)
	limiter := newGeminiRateLimiter()

	var completed int
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)

	for i, q := range queries {
		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := limiter.Wait(ctx); err != nil {
				results[i] = QueryResult{Query: q.Query, Error: err}
				return nil
			}

			result := executeSingleQuery(t, ctx, client, cfg, q)
			results[i] = result

			mu.Lock()
			completed++
			progress := completed
			mu.Unlock()

			if result.Error != nil {
				t.Logf("[%d/%d] ✗ %s: %v", progress, len(queries), q.Query[:min(50, len(q.Query))], result.Error)
			} else {
				t.Logf("[%d/%d] ✓ %s (%d chars, %d/%d keywords)",
					progress, len(queries), q.Query[:min(50, len(q.Query))],
					len(result.Response.Generation), len(result.KeywordsFound), len(q.ExpectedKeywords))
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Logf("Warning: parallel execution had errors: %v", err)
	}
}

func executeQueriesSequential(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	cfg testQueryConfig,
	queries []TestQuery,
	results []QueryResult,
) {
	t.Helper()

	for i, q := range queries {
		t.Logf("[%d/%d] Querying: %s", i+1, len(queries), q.Query)
		results[i] = executeSingleQuery(t, ctx, client, cfg, q)

		if results[i].Error != nil {
			t.Logf("  Error: %v", results[i].Error)
		} else {
			t.Logf("  Answer length: %d chars, Keywords: %d/%d, Hits: %d",
				len(results[i].Response.Generation), len(results[i].KeywordsFound), len(q.ExpectedKeywords),
				len(results[i].Response.Hits))
		}
	}
}

func executeSingleQuery(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	cfg testQueryConfig,
	q TestQuery,
) QueryResult {
	t.Helper()

	evalConfig := newEvalConfig(t, q.ExpectedKeywords)

	req := antfly.RetrievalAgentRequest{
		Query:          q.Query,
		Generator:      cfg.generator,
		AgentKnowledge: cfg.agentKnowledge,
		Stream:         false,
		// Gemma-3-ONNX has a 2048 token context limit, so we need tight constraints
		MaxContextTokens: 1200,
		Steps: antfly.RetrievalAgentSteps{
			Classification: antfly.ClassificationStepConfig{
				Enabled:       true,
				WithReasoning: true,
			},
			Followup: antfly.FollowupStepConfig{
				Enabled: false,
			},
			Eval: evalConfig,
		},
		Queries: []antfly.RetrievalQueryRequest{{
			Table:          cfg.tableName,
			Indexes:        []string{"full_text_index", "embeddings"},
			SemanticSearch: q.Query,
			Limit:          5,
			Reranker:       cfg.reranker,
			Pruner:         cfg.pruner,
		}},
	}

	resp, err := client.RetrievalAgent(ctx, req)
	result := QueryResult{
		Query:    q.Query,
		Response: resp,
		Error:    err,
	}

	if resp != nil {
		result.EvalResult = &resp.EvalResult
	}

	if err == nil && resp != nil {
		kw := checkKeywords(resp.Generation, q.ExpectedKeywords)
		result.ContainsKeywords = kw.ContainsKeywords
		result.KeywordsFound = kw.Found
		result.KeywordsMissing = kw.Missing
	}

	return result
}

// aggregateEvalResults combines individual EvalResults from each query into a summary
func aggregateEvalResults(t *testing.T, results []QueryResult) *antfly.EvalResult {
	t.Helper()

	var totalPassed, totalFailed int
	var totalScore float32
	var validResults int

	// Per-evaluator stats
	type evalStat struct {
		score  float32
		passed int
		failed int
		count  int
	}
	evalStats := make(map[string]*evalStat)

	for _, result := range results {
		if result.EvalResult == nil {
			continue
		}
		validResults++

		totalPassed += result.EvalResult.Summary.Passed
		totalFailed += result.EvalResult.Summary.Failed
		totalScore += result.EvalResult.Summary.AverageScore

		// Aggregate per-evaluator stats from generation scores
		for name, score := range result.EvalResult.Scores.Generation {
			if evalStats[name] == nil {
				evalStats[name] = &evalStat{}
			}
			stats := evalStats[name]
			stats.score += score.Score
			stats.count++
			if score.Pass {
				stats.passed++
			} else {
				stats.failed++
			}
		}

		// Aggregate per-evaluator stats from retrieval scores
		for name, score := range result.EvalResult.Scores.Retrieval {
			if evalStats[name] == nil {
				evalStats[name] = &evalStat{}
			}
			stats := evalStats[name]
			stats.score += score.Score
			stats.count++
			if score.Pass {
				stats.passed++
			} else {
				stats.failed++
			}
		}
	}

	if validResults == 0 {
		return nil
	}

	// Log per-evaluator stats
	t.Logf("Aggregated %d valid evaluation results:", validResults)
	for name, stats := range evalStats {
		avgScore := stats.score / float32(stats.count)
		passRate := float64(stats.passed) / float64(stats.count) * 100
		t.Logf("  - %s: avg=%.3f, pass_rate=%.1f%% (%d/%d)", name, avgScore, passRate, stats.passed, stats.count)
	}

	return &antfly.EvalResult{
		Summary: antfly.EvalSummary{
			AverageScore: totalScore / float32(validResults),
			Passed:       totalPassed,
			Failed:       totalFailed,
			Total:        totalPassed + totalFailed,
		},
	}
}

// assertEvalReport validates that the evaluation report meets quality thresholds
func assertEvalReport(t *testing.T, result *antfly.EvalResult, minPassRate float64) {
	t.Helper()

	if result == nil {
		t.Fatal("No evaluation results")
	}

	total := result.Summary.Passed + result.Summary.Failed
	if total == 0 {
		t.Fatal("No evaluation results")
	}

	passRate := float64(result.Summary.Passed) / float64(total)

	t.Logf("\n=== Inline Evaluation Results ===")
	t.Logf("Overall pass rate: %.2f%% (%d/%d)", passRate*100, result.Summary.Passed, total)
	t.Logf("Average score: %.3f", result.Summary.AverageScore)

	if passRate < minPassRate {
		t.Fatalf("Inline evaluation failed: pass rate %.2f%% < minimum %.2f%%",
			passRate*100, minPassRate*100)
	}
}

// SaveEvaluationReport saves an evaluation report to the e2e/test_results directory in Markdown format
func SaveEvaluationReport(t *testing.T, results []QueryResult, queries []TestQuery, evalResult *antfly.EvalResult, testName string) error {
	t.Helper()

	resultsDir := GetE2EResultsDir()

	// Ensure results directory exists
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return fmt.Errorf("failed to create results directory: %w", err)
	}

	// Generate filename with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	filename := fmt.Sprintf("%s_%s.md", testName, timestamp)
	resultPath := filepath.Join(resultsDir, filename)

	// Build markdown report
	var sb strings.Builder

	// Header
	fmt.Fprintf(&sb, "# Evaluation Report: %s\n\n", testName)
	fmt.Fprintf(&sb, "**Date:** %s\n\n", time.Now().Format(time.RFC3339))

	// Summary
	sb.WriteString("## Summary\n\n")
	if evalResult != nil {
		total := evalResult.Summary.Passed + evalResult.Summary.Failed
		if total > 0 {
			passRate := float64(evalResult.Summary.Passed) / float64(total) * 100
			fmt.Fprintf(&sb, "- **Pass Rate:** %.2f%% (%d/%d)\n", passRate, evalResult.Summary.Passed, total)
		}
		fmt.Fprintf(&sb, "- **Average Score:** %.3f\n", evalResult.Summary.AverageScore)
	}
	sb.WriteString("\n")

	// Per-query results
	sb.WriteString("## Query Results\n\n")
	for i, result := range results {
		query := ""
		description := ""
		if i < len(queries) {
			query = queries[i].Query
			description = queries[i].Description
		} else {
			query = result.Query
		}

		fmt.Fprintf(&sb, "### Query %d: %s\n\n", i+1, query)
		if description != "" {
			fmt.Fprintf(&sb, "**Description:** %s\n\n", description)
		}

		if result.Error != nil {
			fmt.Fprintf(&sb, "**Error:** %v\n\n", result.Error)
			sb.WriteString("---\n\n")
			continue
		}

		if result.Response != nil {
			answer := result.Response.Generation
			if len(answer) > 500 {
				answer = answer[:500] + "..."
			}
			fmt.Fprintf(&sb, "**Answer:** %s\n\n", answer)
		}

		if result.EvalResult != nil {
			sb.WriteString("**Evaluation Scores:**\n\n")
			sb.WriteString("| Evaluator | Score | Pass | Reason |\n")
			sb.WriteString("|-----------|-------|------|--------|\n")

			// Write generation scores
			for name, score := range result.EvalResult.Scores.Generation {
				passStr := "❌"
				if score.Pass {
					passStr = "✅"
				}
				reason := score.Reason
				if len(reason) > 100 {
					reason = reason[:100] + "..."
				}
				fmt.Fprintf(&sb, "| %s | %.3f | %s | %s |\n", name, score.Score, passStr, reason)
			}

			// Write retrieval scores
			for name, score := range result.EvalResult.Scores.Retrieval {
				passStr := "❌"
				if score.Pass {
					passStr = "✅"
				}
				reason := score.Reason
				if len(reason) > 100 {
					reason = reason[:100] + "..."
				}
				fmt.Fprintf(&sb, "| %s | %.3f | %s | %s |\n", name, score.Score, passStr, reason)
			}
			sb.WriteString("\n")
		}

		sb.WriteString("---\n\n")
	}

	// Write to file
	if err := os.WriteFile(resultPath, []byte(sb.String()), 0644); err != nil {
		return fmt.Errorf("failed to write report file: %w", err)
	}

	t.Logf("Saved evaluation report to: %s", resultPath)
	return nil
}

// testQueryConfig holds shared configuration for both answer agent and chat agent queries
type testQueryConfig struct {
	reranker       *antfly.RerankerConfig
	pruner         antfly.Pruner
	agentKnowledge string
	generator      antfly.GeneratorConfig
	tableName      string
}

func newTestQueryConfig(t *testing.T, tableName, termiteURL string) testQueryConfig {
	t.Helper()

	rerankerConfig, err := antfly.NewRerankerConfig(antfly.TermiteRerankerConfig{
		Model: "mixedbread-ai/mxbai-rerank-base-v1",
		Url:   termiteURL,
	})
	require.NoError(t, err, "Failed to create reranker config")
	rerankerConfig.Field = "content"

	return testQueryConfig{
		reranker: rerankerConfig,
		pruner: antfly.Pruner{
			MinScoreRatio:      0.7,
			StdDevThreshold:    0.75,
			MaxScoreGapPercent: 15.0,
		},
		agentKnowledge: `This is the Antfly documentation. Antfly is a distributed, horizontally scalable document database built for the AI era. It combines proven distributed systems technology with vector search capabilities.

Key concepts:
- Tables: Collections of documents with configurable indexes
- Shards: Horizontal partitions for scalability
- Indexes: BM25 (full-text), embedding (vector), and enrichers
- Multi-Raft: Separate consensus groups for metadata and storage
- Termite: ML service for embeddings, chunking, and reranking

Use precise technical terminology when discussing Antfly features.`,
		generator: GetDefaultGeneratorConfig(t),
		tableName: tableName,
	}
}

func newEvalConfig(t *testing.T, expectedKeywords []string) antfly.EvalConfig {
	t.Helper()
	return antfly.EvalConfig{
		Evaluators: []antfly.EvaluatorName{
			antfly.EvaluatorNameRelevance,
			antfly.EvaluatorNameFaithfulness,
			antfly.EvaluatorNameCompleteness,
		},
		Judge: GetJudgeConfig(t),
		GroundTruth: antfly.GroundTruth{
			Expectations: strings.Join(expectedKeywords, ", "),
		},
		Options: antfly.EvalOptions{
			PassThreshold: 0.5,
		},
	}
}

// keywordResult holds the result of checking expected keywords against an answer
type keywordResult struct {
	ContainsKeywords bool
	Found            []string
	Missing          []string
}

// setupTestTableResult holds the result of setting up a test table
type setupTestTableResult struct {
	NumDocs  int
	Restored bool
}

// setupTestTable handles table creation, backup restoration, and indexing.
// Returns the number of documents and whether restoration was used.
func setupTestTable(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	tableName, backupID string,
) setupTestTableResult {
	t.Helper()

	result := setupTestTableResult{}

	// Check if we should restore from backup
	if ShouldRestoreFromBackup() && BackupExists(t, backupID) {
		t.Log("RESTORE_DB is set and backup exists, restoring from backup instead of indexing...")

		err := RestoreTestDatabase(t, ctx, client, tableName, backupID)
		if err != nil {
			t.Logf("Warning: Restore failed (%v), falling back to indexing", err)
		} else {
			t.Log("Waiting for shards to be ready after restore...")
			waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

			status, err := client.GetTable(ctx, tableName)
			if err != nil {
				t.Logf("Warning: Failed to get table status after restore (%v), falling back to indexing", err)
			} else {
				result.NumDocs = 2500 // Approximate count from docsaf indexing
				t.Logf("Successfully restored table '%s' with %d shard(s)", tableName, len(status.Shards))

				t.Log("Waiting for embedding index to be ready after restore...")
				waitForEmbeddings(t, ctx, client, tableName, "embeddings", result.NumDocs, 30*time.Minute)

				t.Log("Restore complete with embeddings ready")
				result.Restored = true
				return result
			}
		}
	}

	// Index from scratch
	t.Log("Indexing Antfly documentation...")
	result.NumDocs = indexAntflyDocs(t, ctx, client, tableName)

	t.Log("Waiting for embedding enrichment...")
	waitForEmbeddings(t, ctx, client, tableName, "embeddings", result.NumDocs, 30*time.Minute)

	t.Log("Creating backup after indexing/embedding completion...")
	err := BackupTestDatabase(t, ctx, client, tableName, backupID)
	if err != nil {
		t.Logf("Warning: Backup failed (%v), but continuing with test", err)
	} else {
		t.Logf("Backup created successfully - future runs can use RESTORE_DB=true to skip indexing")
	}

	return result
}

// setupTestTableWithHierarchy handles table creation with graph hierarchy for tree search.
// This function creates both embedding and graph indexes with parent-child relationships.
func setupTestTableWithHierarchy(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	tableName, backupID string,
) setupTestTableResult {
	t.Helper()

	result := setupTestTableResult{}

	// Check if we should restore from backup (with hierarchy backup suffix)
	hierarchyBackupID := backupID + "-hierarchy"
	if ShouldRestoreFromBackup() && BackupExists(t, hierarchyBackupID) {
		t.Log("RESTORE_DB is set and hierarchy backup exists, restoring...")

		err := RestoreTestDatabase(t, ctx, client, tableName, hierarchyBackupID)
		if err != nil {
			t.Logf("Warning: Restore failed (%v), falling back to indexing", err)
		} else {
			t.Log("Waiting for shards to be ready after restore...")
			waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

			status, err := client.GetTable(ctx, tableName)
			if err != nil {
				t.Logf("Warning: Failed to get table status after restore (%v), falling back to indexing", err)
			} else {
				result.NumDocs = 2500 // Approximate count from docsaf indexing
				t.Logf("Successfully restored table '%s' with %d shard(s)", tableName, len(status.Shards))

				t.Log("Waiting for embedding index to be ready after restore...")
				waitForEmbeddings(t, ctx, client, tableName, "embeddings", result.NumDocs, 30*time.Minute)

				t.Log("Restore complete with hierarchy and embeddings ready")
				result.Restored = true
				return result
			}
		}
	}

	// Index from scratch with hierarchy
	t.Log("Indexing Antfly documentation with hierarchy for tree search...")
	result.NumDocs = indexAntflyDocsWithHierarchy(t, ctx, client, tableName)

	t.Log("Waiting for embedding enrichment...")
	waitForEmbeddings(t, ctx, client, tableName, "embeddings", result.NumDocs, 30*time.Minute)

	t.Log("Creating backup after indexing/embedding completion...")
	err := BackupTestDatabase(t, ctx, client, tableName, hierarchyBackupID)
	if err != nil {
		t.Logf("Warning: Backup failed (%v), but continuing with test", err)
	} else {
		t.Logf("Backup created successfully - future runs can use RESTORE_DB=true to skip indexing")
	}

	return result
}

// evalTestSetup holds common setup for eval tests
type evalTestSetup struct {
	Ctx               context.Context
	Cancel            context.CancelFunc
	Swarm             *SwarmInstance
	StopMemoryMonitor func()
}

// setupEvalTest performs common setup for evaluation E2E tests.
func setupEvalTest(t *testing.T, timeout time.Duration) *evalTestSetup {
	t.Helper()

	// Ensure required models are downloaded
	ensureRegistryModel(t, "BAAI/bge-small-en-v1.5", modelregistry.ModelTypeEmbedder, []string{modelregistry.VariantF32})
	ensureRegistryModel(t, "mirth/chonky-mmbert-small-multilingual-1", modelregistry.ModelTypeChunker, []string{modelregistry.VariantF32})
	ensureRegistryModel(t, "mixedbread-ai/mxbai-rerank-base-v1", modelregistry.ModelTypeReranker, []string{modelregistry.VariantF32})
	ensureHuggingFaceModel(t, "onnxruntime/Gemma-3-ONNX", modelregistry.ModelTypeGenerator)

	// Create a context that cancels on SIGINT/SIGTERM
	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(sigCtx, timeout)

	// Combine cancellation
	combinedCancel := func() {
		cancel()
		sigCancel()
	}

	// Start background memory monitor
	stopMemoryMonitor := startMemoryMonitor(t, 10*time.Second)

	// Start Antfly swarm
	t.Log("Starting Antfly swarm...")
	swarm := startAntflySwarm(t, ctx)

	return &evalTestSetup{
		Ctx:               ctx,
		Cancel:            combinedCancel,
		Swarm:             swarm,
		StopMemoryMonitor: stopMemoryMonitor,
	}
}

// Cleanup releases all resources from the eval test setup.
func (s *evalTestSetup) Cleanup() {
	s.StopMemoryMonitor()
	s.Swarm.Cleanup()
	s.Cancel()
}

func checkKeywords(answer string, expected []string) keywordResult {
	answer = strings.ToLower(answer)
	var result keywordResult
	for _, keyword := range expected {
		if strings.Contains(answer, strings.ToLower(keyword)) {
			result.Found = append(result.Found, keyword)
		} else {
			result.Missing = append(result.Missing, keyword)
		}
	}
	if len(expected) > 0 {
		result.ContainsKeywords = float64(len(result.Found))/float64(len(expected)) >= 0.4
	}
	return result
}

// loadJSON loads a JSON file from the e2e directory into the target type
func loadJSON[T any](t *testing.T, filename string) T {
	t.Helper()
	fullPath := filepath.Join(getE2EDir(), filename)
	data, err := os.ReadFile(fullPath)
	require.NoError(t, err, "Failed to read %s", fullPath)
	var result T
	require.NoError(t, json.Unmarshal(data, &result), "Failed to parse %s", fullPath)
	return result
}

// TestE2E_RetrievalAgent_DocsEval is the main e2e test
func TestE2E_RetrievalAgent_DocsEval(t *testing.T) {
	skipUnlessML(t)
	SkipIfProviderUnavailable(t)

	// Start pprof server for debugging memory issues
	pprofAddr := "localhost:6060"
	pprofServer := &http.Server{Addr: pprofAddr, Handler: nil}
	go func() {
		t.Logf("Starting pprof server at http://%s/debug/pprof/", pprofAddr)
		if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			t.Logf("pprof server error: %v", err)
		}
	}()
	defer pprofServer.Close()

	// Setup test environment
	setup := setupEvalTest(t, 40*time.Minute)
	defer setup.Cleanup()

	dumpMemoryProfile(t, "baseline", true)
	dumpMemoryProfile(t, "after_swarm_start", true)

	tableName := "antfly_docs_e2e"
	backupID := "docsaf-test-backup"

	// Setup table (restore or index)
	tableResult := setupTestTable(t, setup.Ctx, setup.Swarm.Client, tableName, backupID)
	if !tableResult.Restored {
		dumpMemoryProfile(t, "after_indexing", true)
		dumpMemoryProfile(t, "after_embeddings", true)
	}

	// Load and execute queries
	t.Log("Loading test queries...")
	queries := loadTestQueries(t, "test_queries.json")
	require.NotEmpty(t, queries, "No test queries loaded")

	t.Log("Executing retrieval agent queries with hybrid search and inline evaluation...")
	cfg := newTestQueryConfig(t, tableName, setup.Swarm.Config.Termite.ApiUrl)
	results := executeRetrievalAgentQueries(t, setup.Ctx, setup.Swarm.Client, cfg, queries)

	// Aggregate and report
	t.Log("Aggregating evaluation results...")
	evalResult := aggregateEvalResults(t, results)

	t.Log("Saving evaluation report...")
	if err := SaveEvaluationReport(t, results, queries, evalResult, "docsaf_test"); err != nil {
		t.Logf("Warning: Failed to save evaluation report (%v)", err)
	}

	t.Log("Validating evaluation results...")
	assertEvalReport(t, evalResult, 0.60)

	t.Log("E2E test completed successfully!")
}
