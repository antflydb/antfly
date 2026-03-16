# End-to-End Tests

This directory contains end-to-end (e2e) tests for Antfly that test the full system integration.

## Tests

### `TestE2E_RetrievalAgent_DocsEval`

A comprehensive e2e test that:
1. Starts Antfly in swarm mode programmatically (metadata + store + termite servers)
2. Indexes real Antfly documentation using docsaf library (~2,500 document sections)
3. Creates table with hybrid search (BM25 + embeddings) and waits for enrichment
4. Executes 8 retrieval agent queries with semantic search and reasoning
5. Validates answer quality using LLM-as-judge evaluation (faithfulness, relevance, completeness)

**What it tests:**
- Swarm mode startup and initialization (metadata + store + termite servers)
- Document indexing pipeline (docsaf → LinearMerge)
- Hybrid search (BM25 full-text + vector embeddings)
- Embedding enrichment with Termite chunking service
- Retrieval agent query execution with reasoning
- Answer quality evaluation using LLM-as-judge (evalaf framework)

## Prerequisites

### Required
- **Ollama** running locally on `http://localhost:11434`
- Models:
  - `gemma3:4b` - For LLM generation and evaluation
  - `embeddinggemma` - For generating embeddings

### Install Ollama and Models
```bash
# Install Ollama (macOS)
brew install ollama

# Start Ollama service
ollama serve

# Pull the required models (in another terminal)
ollama pull gemma3:4b
ollama pull embeddinggemma
```

## Running the Tests

Most e2e tests run by default. Only tests requiring external services or large
model downloads are gated behind environment variables.

### Run all default e2e tests
```bash
make e2e                            # All default tests (downloads ONNX deps on first run)
make e2e E2E_TEST=TestName          # Run specific test
make e2e E2E_TIMEOUT=45m            # Custom timeout (default: 30m)
```

### Skip e2e tests in short mode
```bash
go test -short ./e2e
# All e2e tests will be skipped
```

### Gated test suites

| Flag | Tests | Requires |
|------|-------|----------|
| `RUN_ML_TESTS=true` | Eval, retrieval generation, backup/restore with embeddings, remote content (CLIP/CLAP) | Ollama or large ONNX model downloads |
| `RUN_PG_TESTS=true` | Foreign table queries, CDC replication | Running PostgreSQL instance |

```bash
# Run ML tests (requires Ollama or model downloads)
RUN_ML_TESTS=true make e2e E2E_TIMEOUT=45m

# Run PostgreSQL tests
cd e2e && RUN_PG_TESTS=true ANTFLY_E2E_PG_DSN=postgres://... go test -v ./... -timeout 10m
```

### Using Gemini instead of Ollama

By default, ML e2e tests use Ollama for embeddings and generation. To use Google Gemini instead:

```bash
export E2E_PROVIDER=gemini
export GEMINI_API_KEY=your-api-key

RUN_ML_TESTS=true go test -v ./e2e -run TestE2E_RetrievalAgent_DocsEval -timeout 45m
```

This uses:
- `gemini-embedding-001` for embeddings
- `gemini-2.5-flash` for text generation

## Backup and Restore

The `TestE2E_RetrievalAgent_DocsEval` test supports backing up and restoring the database to speed up test runs. This is especially useful since indexing and embedding generation can take 30-40 minutes.

### How It Works

1. **First run**: The test indexes all documentation and generates embeddings, then automatically backs up the database to `e2e/backups/` before running evals
2. **Subsequent runs with `RESTORE_DB=true`**: The test restores from backup instead of re-indexing, reducing test time from ~40 minutes to ~2-3 minutes

**Note:** The backup happens immediately after indexing/embedding completion, before running the evaluation queries. This ensures you have a backup even if the evals fail.

### Usage

**First run (creates backup):**
```bash
RUN_ML_TESTS=true go test -v ./e2e -run TestE2E_RetrievalAgent_DocsEval -timeout 45m
# Takes ~40 minutes, creates backup in e2e/backups/
```

**Subsequent runs (restore from backup):**
```bash
RUN_ML_TESTS=true RESTORE_DB=true go test -v ./e2e -run TestE2E_RetrievalAgent_DocsEval -timeout 10m
# Takes ~2-3 minutes, restores from backup
```

### Backup Files

Backups are stored in `e2e/backups/` and consist of:
- `docsaf-test-backup-metadata.json` - Table structure and configuration
- `docsaf-test-backup-{shard_id}.tar.zst` - Compressed shard data (Pebble database)

These files are automatically excluded from version control via `.gitignore`.

### Fallback Behavior

If restore fails for any reason (backup corrupted, missing files, etc.), the test automatically falls back to the standard indexing workflow.

### When to Recreate Backups

You should recreate backups when:
- Documentation content has changed significantly
- Table schema or index configuration has changed
- Embedding model has changed (`embeddinggemma` → different model)
- Chunking strategy has changed

To recreate, simply run the test without `RESTORE_DB=true` - it will overwrite the existing backup.

## Evaluation Reports

The test automatically saves detailed evaluation reports in Markdown format to `e2e/test_results/` after each test run.

### Report Contents

Each report includes:
- **Summary**: Overall pass rate, average scores, total examples
- **Evaluator Statistics**: Per-evaluator metrics (faithfulness, relevance, completeness)
- **Detailed Results**: Per-example evaluation breakdown with scores and reasoning
- **Failed Examples**: Diagnostic information for queries that didn't meet quality thresholds

### Report Files

Reports are saved with timestamps for tracking over time:
```
e2e/test_results/
├── docsaf_test_2025-12-01_14-30-45.md
├── docsaf_test_2025-12-01_15-22-18.md
└── docsaf_test_2025-12-02_09-15-33.md
```

These files are automatically excluded from version control via `.gitignore`.

### Viewing Reports

Simply open the Markdown files in any text editor or Markdown viewer. The reports are human-readable and include:
- Tables with evaluator statistics
- Detailed pass/fail information
- LLM judge reasoning for each evaluation
- Timing information

### Use Cases

- **Regression tracking**: Compare reports across runs to detect quality degradation
- **Documentation**: Share evaluation results with stakeholders
- **Debugging**: Analyze why specific queries failed evaluation
- **A/B testing**: Compare different model configurations or prompts

## Test Structure

### Files

- **`docsaf_test.go`** - Main e2e test implementation
- **`test_helpers.go`** - Utility functions for test setup
- **`test_queries.json`** - Test dataset with queries and expected keywords
- **`README.md`** - This file

### Key Functions

**Swarm Management:**
- `startAntflySwarm()` - Start metadata + store servers with dynamic ports
- `SwarmInstance.Cleanup()` - Graceful shutdown and cleanup

**Document Indexing:**
- `indexAntflyDocs()` - Index documentation using docsaf library
- `setupTableWithIndexes()` - Configure table indexes (currently BM25 only)
- `waitForEmbeddings()` - Wait for embedding enrichment on a named index

**Query & Validation:**
- `executeRetrievalAgentQueries()` - Run queries and collect results
- `assertEvalReport()` - Validate answers meet quality threshold

**Helpers:**
- `GetFreePort()` - Allocate dynamic ports for servers
- `CreateTestConfig()` - Generate test configuration
- `SkipIfOllamaUnavailable()` - Skip test if Ollama not running
- `GetDefaultOllamaConfig()` - Standard LLM config

**Backup & Restore:**
- `GetBackupDir()` - Get absolute path to e2e/backups directory
- `ShouldRestoreFromBackup()` - Check if RESTORE_DB environment variable is set
- `BackupExists()` - Check if a backup with given ID exists
- `BackupTestDatabase()` - Create backup of table to e2e/backups
- `RestoreTestDatabase()` - Restore table from backup and wait for completion

## Test Data

The test indexes these Antfly documentation files:
- `CLAUDE.md` - Project development guide
- `README.md` - Project overview
- `src/metadata/api.yaml` - API specification
- `examples/docsaf/README.md` - Docsaf documentation

Test queries in `test_queries.json` ask questions about:
- Multi-raft architecture
- Linear Merge API
- Embedding providers
- Build process
- Docsaf tool
- Hybrid search
- Swarm mode
- Table creation

## Validation Criteria

The test uses **LLM-as-judge** evaluation with three metrics:

1. **Faithfulness**: Does the answer accurately reflect the source documents?
2. **Relevance**: Does the answer address the user's query?
3. **Completeness**: Does the answer cover all important aspects?

Each query is evaluated by an LLM judge (gemma3:4b) that scores answers on a 0-1 scale.

**Pass criteria:**
- Individual queries pass if all three metrics score ≥ 0.7
- Test passes if **overall pass rate ≥ 60%**

Expected keywords are provided for reference but not used for automated validation.

## Future Improvements

### TODO
- [ ] Add more test scenarios (updates, deletions, multi-table queries)
- [ ] Test with larger document sets
- [ ] Add performance benchmarks
- [ ] Test distributed mode (multiple nodes)
- [ ] Add integration with CI/CD pipeline
- [ ] Test semantic chunking strategy (in addition to fixed chunking)
- [ ] Test Hugot chunking with ONNX models

### Completed
- [x] Add embedding indexes (aknn_v0)
- [x] Implement proper embedding enrichment status polling
- [x] Use LLM-as-judge for answer quality validation (evalaf framework)
- [x] Integrate Termite chunking service

## Troubleshooting

### Test hangs during startup
- Check if ports are already in use
- Ensure data directories are writable
- Check Antfly logs for errors

### Ollama connection errors
```bash
# Check if Ollama is running
curl http://localhost:11434/api/tags

# Restart Ollama
pkill ollama
ollama serve
```

### Test timeouts
- Increase timeout: `-timeout 20m`
- Check system resources (CPU, memory)
- Reduce test data size

### Compilation errors
- Run `go mod tidy` to fix dependencies
- Ensure you're using compatible Go version (1.21+)
- Check if pkg/client SDK is up to date

## Example Output

```
=== RUN   TestE2E_RetrievalAgent_DocsEval
    docsaf_test.go:426: Starting Antfly swarm...
    docsaf_test.go:156: Swarm started successfully
    docsaf_test.go:436: Indexing Antfly documentation...
    docsaf_test.go:210: Found 156 document sections
    docsaf_test.go:227: Linear merge results: upserted=156, skipped=0, deleted=0
    docsaf_test.go:443: Setting up indexes...
    docsaf_test.go:240: Using default BM25 index
    docsaf_test.go:448: Waiting for embedding enrichment...
    docsaf_test.go:250: Skipping embedding wait (not using embeddings)
    docsaf_test.go:453: Loading test queries...
    docsaf_test.go:458: Executing retrieval agent queries...
    docsaf_test.go:297: [1/8] Querying: What is Antfly's multi-raft architecture?
    docsaf_test.go:346: Answer length: 324 chars, Found keywords: 4/5
    docsaf_test.go:297: [2/8] Querying: How do I use the Linear Merge API?
    docsaf_test.go:346: Answer length: 412 chars, Found keywords: 5/6
    ...
    docsaf_test.go:463: Validating answer quality...
    docsaf_test.go:386: Pass rate: 75.00% (6/8 queries passed)
    docsaf_test.go:468: E2E test completed successfully!
--- PASS: TestE2E_RetrievalAgent_DocsEval (47.23s)
PASS
```

## Contributing

When adding new e2e tests:
1. Follow existing patterns (swarm startup, cleanup, validation)
2. Use `t.TempDir()` for temporary directories
3. Use dynamic port allocation (`GetFreePort()`)
4. Add proper cleanup with `defer`
5. Include comprehensive logging with `t.Log()`
6. Document test purpose and expected behavior
7. Add skip conditions for unavailable dependencies
