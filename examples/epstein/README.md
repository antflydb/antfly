# Epstein Documents Search

A complete example demonstrating how to use Antfly to index and search the publicly released Jeffrey Epstein court documents and DOJ files.

## Overview

This tool downloads, processes, and indexes PDF documents from:

1. **January 2024 Court Unsealing** - 943 pages from Giuffre v. Maxwell (Case 1:15-cv-07433-LAP)
2. **DOJ December 2025 Release** - 4,055+ documents across 8 datasets released via the Epstein Files Transparency Act (EFTA)
3. **DOJ January 2026 Release** - 3.5 million+ pages across datasets 10-12 (dataset 9 excluded due to incomplete release)

The documents are processed page-by-page, chunked for semantic search, and made searchable through both BM25 (full-text) and vector similarity search.

## Quick Start

### Prerequisites

- Go 1.21+
- Zig 0.16.0+
- Running Zig Antfly standalone with Antfly inference models

### 1. Build and Start Zig Antfly

```bash
# From the antfly root directory
cd zig
zig build antfly
./zig-out/bin/antfly standalone
```

This starts a single-node Antfly cluster on `http://localhost:8080` with the
Zig inference routes mounted under the same public API.

### 2. Pull Models

The Zig model pull path treats HuggingFace as the default source, so `hf:` is
omitted here.

```bash
cd zig
./zig-out/bin/antfly inference pull antflydb/clipclap:gguf:Q4_K --tasks embed
./zig-out/bin/antfly inference pull antflydb/gliner2-base-v1 --tasks extract --capabilities extraction
./zig-out/bin/antfly inference pull microsoft/Florence-2-base-ft --tasks read
```

### 3. Build the Tool

```bash
cd examples/epstein
go build -o epstein .
```

### 4. Download or Point at Local Documents

Choose a dataset based on your needs:

```bash
# Option A: January 2024 Court Documents (~23MB, 943 pages)
# Good for testing and smaller deployments
./epstein download --dataset court-2024

# Option B: DOJ December 2025 Release (~4.8GB, 8 datasets)
./epstein download --dataset doj-complete

# Option C: DOJ January 2026 Release (~104GB, datasets 10-12)
# These are ZIP archives that are automatically extracted after download
./epstein download --dataset doj-jan2026

# Option D: Everything
./epstein download --dataset all
```

If the files are already on an external disk, point the tool at them instead:

```bash
export EPSTEIN_DOCS_DIR="/path/to/T9/epstein-files"
export EPSTEIN_ZIP="/path/to/T9/DataSet_10.zip"
```

### 5. Prepare, Enrich, and Load

```bash
# Process PDFs into page records. --split-pages enables direct PDF viewing.
./epstein prepare --dir "$EPSTEIN_DOCS_DIR" --split-pages

# Small smoke run from a large local disk: process only the first few PDFs and pages.
./epstein prepare --dir /Volumes/T9/epstein-files --split-pages \
  --limit-files 2 --limit-pages 50 \
  --output epstein-smoke.json

# Optional: OCR low-quality pages through Zig Antfly inference.
./epstein enrich --input epstein-docs.json --dir "$EPSTEIN_DOCS_DIR"

# Optional: add entity metadata to each page record.
./epstein entities --input epstein-docs-enriched.json

# Load into Antfly using ClipClap embeddings.
./epstein load --input epstein-docs-enriched-entities.json --create-table

# Load the small smoke file and enable the Zig artifact-backed relation graph.
./epstein load --input epstein-smoke.json --table epstein_smoke --create-table \
  --enable-artifact-graph \
  --artifact-producer extractor \
  --artifact-extractor-model antflydb/gliner2-base-v1

# Optional: use the slower Gemma tool-call generator path for richer relation extraction.
./epstein load --input epstein-smoke.json --table epstein_smoke_gemma --create-table \
  --enable-artifact-graph \
  --artifact-producer generator \
  --artifact-extractor-model ggml-org/gemma-4-E4B-it-GGUF
```

For ZIP sources that should not be extracted first:

```bash
./epstein prepare --zip "$EPSTEIN_ZIP" --split-pages
./epstein entities --input epstein-docs.json
./epstein load --input epstein-docs-entities.json --create-table
```

### 6. Start Search Interface

```bash
./epstein serve
```

Open http://localhost:3000 in your browser.

## Commands

### `download`

Downloads documents from Internet Archive.

```bash
./epstein download [flags]

Flags:
  --output    Output directory (default: ./epstein-docs)
  --dataset   Dataset to download: court-2024, doj-complete, doj-jan2026, all
```

### `prepare`

Processes PDF files and creates JSON data for loading.

```bash
./epstein prepare [flags]

Flags:
  --dir       Path to PDF directory (default: ./epstein-docs)
  --output    Output JSON file (default: epstein-docs.json)
  --base-url  Base URL for document links
  --split-pages
              Split PDFs into individual page PDFs for direct viewing
  --zip       Path to ZIP archive containing PDFs (repeatable)
  --enable-ocr
              Enable OCR fallback through Antfly inference readers
  --ocr-url   Antfly inference URL (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --ocr-models
              OCR models to try (default: microsoft/Florence-2-base-ft)
  --limit-files
              Process at most this many source PDFs (default: all)
  --limit-pages
              Keep at most this many parsed pages in output (default: all)
```

### `load`

Loads prepared JSON data into Antfly.

```bash
./epstein load [flags]

Flags:
  --url             Antfly API URL (default: http://localhost:8080/db/v1)
  --table           Table name (default: epstein_docs)
  --input           Input JSON file (default: epstein-docs.json)
  --create-table    Create table if it doesn't exist
  --dry-run         Preview changes without applying
  --num-shards      Number of shards (default: 1)
  --batch-size      Batch size for linear merge (default: 25)
  --inference-url     Antfly inference root URL for chunking; table config stores its /ai/v1 route (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --embedding-model Embedding model (default: antflydb/clipclap)
  --chunker-model   Chunker model (default: fixed-bert-tokenizer)
  --target-tokens   Target tokens per chunk (default: 512)
  --overlap-tokens  Overlap between chunks (default: 50)
  --enable-artifact-graph
                    Create an artifact-backed autograph relation graph index
  --artifact-graph-index
                    Graph index name (default: autograph_relations)
  --artifact-name   Generated asset artifact name (default: relations_v1)
  --artifact-producer
                    Artifact producer type: extractor or generator
                    (default: extractor)
  --artifact-extractor-model
                    Antfly model for artifact relation extraction
                    (default: antflydb/gliner2-base-v1)
  --artifact-labels Entity labels for the artifact extractor
  --artifact-relation-labels
                    Relation labels for the artifact extractor
  --autoschema      Provision the AutoSchemaKG pipeline: LLM extraction
                    passes, entities/events resolution, and a concept taxonomy
                    (requires --create-table; see "AutoSchemaKG mode" below)
  --autoschema-model
                    Antfly generative model shared by the AutoSchemaKG
                    extractors and conceptualizer
                    (default: ggml-org/gemma-4-E4B-it-GGUF)
  --autoschema-gliner-model
                    GLiNER2.5 extraction model for the fast closed-schema
                    autoschema lane; "" disables the lane
                    (default: fastino/gliner2.5-base-v1)
```

### AutoSchemaKG mode

`--autoschema` provisions the schema-free knowledge graph pipeline described
in `zig/AUTOSCHEMA.md`, following *AutoSchemaKG: Autonomous Knowledge Graph
Construction through Dynamic Schema Induction from Web-Scale Corpora*
(arXiv:2505.23628). It coexists with `--enable-artifact-graph` and creates:

- Two generator asset enrichments on the documents table, one per
  extraction pass (`kg_ee_v1` entity-entity; `kg_events_v1` events with
  their participating entities and the temporal/causal relations between
  them), each a forced tool call emitting the `extraction_graph` artifact
  shape.
- A `knowledge_graph` graph index merging the artifact streams, with
  label-routed resolvers that promote `event`-labelled mentions into an
  `events` table (compositional `event/{{ hash _entity.event_identity }}`
  keys: sorted participant slugs + predicate lemma, composed from the
  participants' canonical entity keys once their resolution lands) and
  every other mention into an `entities` table
  (label-free `entity/{{ slug _entity.text }}` keys; the extractor's label
  rides the promoted document as `entity_type`, never the key, so mentions
  labeled differently by different passes still converge on one node).
- A third, GLiNER2.5-powered extraction lane (`kg_gliner_v1`, disable with
  `--autoschema-gliner-model ""`): a fast `extractor` asset producer with a
  closed entity/relation schema (person, organization, location, date;
  `employed_by`, `located_in`, `traveled_with`, `met_with`,
  `associated_with`) feeding the same `knowledge_graph` index as an
  `extraction_relation` source. GLiNER relations reference entities
  positionally (`entity_index`); the lane's own catch-all resolver mints the
  SAME label-free `entity/{{ slug _entity.text }}` canonical keys as the
  LLM lanes, so both extractors' edges converge on shared entity nodes
  regardless of how each extractor labeled the mention, and a
  `min_confidence` floor drops low-score junk before it mints nodes.
  The LLM stages keep the paper's open-vocabulary verb-phrase relations,
  which a closed-schema extractor cannot express.
- A recursive conceptualization autograph on the `entities` table: the
  `conceptualize_v1` enrichment abstracts each promoted entity into three or
  more concept phrases (grounded by `neighbor_context` sampling of the
  taxonomy graph), and the `taxonomy` graph index promotes them into a
  `concepts` table with `is_a` edges.

```bash
./epstein load --create-table --autoschema \
  --autoschema-model ggml-org/gemma-4-E4B-it-GGUF \
  --input epstein-docs-small.json
```

The `entities`, `events`, and `concepts` tables are created automatically
before the documents table so cross-table promotion targets exist up front.

### `sync`

Full pipeline: process PDFs and load directly.

```bash
./epstein sync [flags]

# Combines prepare + load flags
```

### `enrich`

Runs a second OCR/vision pass over prepared JSON and writes a new JSON file.

```bash
./epstein enrich [flags]

Flags:
  --input       Input JSON file (default: epstein-docs.json)
  --output      Output JSON file (default: {input-base}-enriched.json)
  --inference-url Antfly inference URL (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --model       Reader model (default: microsoft/Florence-2-base-ft)
  --category    ocr, vision, quality, or all (default: ocr)
  --dir         Base directory for resolving split page PDFs
  --zip         Source ZIP archive for page lookup (repeatable)
```

### `entities`

Adds Antfly inference NER metadata to prepared or enriched JSON records.

```bash
./epstein entities [flags]

Flags:
  --input           Input JSON file (default: epstein-docs.json)
  --output          Output JSON file (default: {input-base}-entities.json)
  --inference-url     Antfly inference URL (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --model           Recognizer model (default: antflydb/gliner2-base-v1)
  --labels          Entity labels to extract
  --relation-labels Relation labels to extract (default: associated with, communicated with, traveled to, visited, worked for, represented by, mentioned in, located in)
  --batch-size      Text windows per Antfly inference recognize request (default: 16)
  --max-chars       Maximum characters per recognizer window (default: 3000)
  --overlap-chars   Characters of overlap between recognizer windows (default: 300)
  --reprocess       Re-process records that already have entities
```

### `serve`

Starts a web server with search interface.

```bash
./epstein serve [flags]

Flags:
  --url     Antfly API URL (default: http://localhost:8080/db/v1)
  --table   Table name to search (default: epstein_docs)
  --listen  Listen address (default: :3000)
```

## Architecture

### Document Processing

1. **PDF Extraction**: Uses `ledongthuc/pdf` to extract text page-by-page
2. **Document Sections**: Each page becomes a `DocumentSection` with:
   - Unique ID (hash of file path + page number)
   - Title (document title + page number)
   - Content (extracted text)
   - Metadata (page number, total pages, PDF metadata)

### Indexing

Documents are indexed with:

1. **BM25 Full-Text Index** (automatic)
   - Keyword search
   - Exact phrase matching

2. **Embedding Index** (aknn_v0)
   - Semantic similarity search
   - Powered by the native Zig Antfly embedder + `antflydb/clipclap`
   - Chunked with configurable overlap

3. **Entity Metadata** (optional)
   - `epstein entities` stores recognized entities in `metadata.entities`
   - Optional relations are stored in `metadata.relations`
   - Long pages are processed as overlapping windows and offsets are rebased to the page
   - Failed entity windows are tracked and retried on the next run
   - The search UI displays entity chips when present

### Search

The web interface performs hybrid search:
- Queries both `full_text_index_v0` and `embeddings` indexes
- Results are ranked by combined relevance score
- Supports natural language queries

## Datasets

### January 2024 Court Unsealing

- **Source**: [Internet Archive](https://archive.org/details/final-epstein-documents)
- **Size**: ~23MB (PDF), 943 pages
- **Content**: Unsealed documents from Giuffre v. Maxwell civil case
- **Released**: January 3, 2024

### DOJ Complete Release

- **Source**: [Internet Archive](https://archive.org/details/combined-all-epstein-files)
- **Size**: ~4.8GB (8 consolidated PDFs)
- **Content**: 4,055+ documents released under EFTA
- **Released**: December 19, 2025
- **Datasets**:
  - DataSet 1: 1.2GB
  - DataSet 2: 629MB
  - DataSet 3: 598MB
  - DataSet 4: 356MB
  - DataSet 5: 61MB
  - DataSet 6: 53MB
  - DataSet 7: 98MB
  - DataSet 8: 1.8GB

## Performance

Expected processing times (approximate):

| Dataset | Size | Prepare | Load | Embeddings |
|---------|------|---------|------|------------|
| court-2024 | ~23MB | 30s | 2min | 20-30min |
| doj-complete | ~4.8GB | 10min | 30min | 4-8hrs |

Embedding generation is the slowest step as each chunk needs to be processed by the ML model.

## API Usage

You can also query the Antfly API directly:

```bash
# Search via curl
curl "http://localhost:8080/db/v1/tables/epstein_docs/query" \
  -H "Content-Type: application/json" \
  -d '{
    "semantic_search": "flight logs to Little St James",
    "indexes": ["full_text_index_v0", "embeddings"],
    "limit": 10
  }'
```

Or use the Go SDK:

```go
client, _ := antfly.NewAntflyClient("http://localhost:8080/db/v1", http.DefaultClient)

resp, _ := client.Query(ctx, "epstein_docs", antfly.QueryRequest{
    SemanticSearch: "flight logs",
    Indexes:        []string{"full_text_index_v0", "embeddings"},
    Limit:          10,
})

for _, hit := range resp.Hits {
    fmt.Printf("Score: %.2f - %s\n", hit.Score, hit.Document["title"])
}
```

## Troubleshooting

### "No PDF files found"

Make sure you've run `download` first and the PDFs are in the expected directory.

### "Failed to create table (may already exist)"

The table already exists. This is fine - the sync will update existing documents.

### Slow embedding generation

Embedding generation runs asynchronously. You can monitor progress:

```bash
curl "http://localhost:8080/db/v1/tables/epstein_docs/indexes/embeddings" | jq '.status.total_indexed'
```

### Out of memory

For large datasets, increase the Go garbage collector threshold:

```bash
GOGC=50 ./epstein sync --create-table
```

Or use multiple shards:

```bash
./epstein sync --create-table --num-shards 4
```

## Legal Notice

These documents are publicly available through official government channels and public archives. This tool is provided for research, journalism, and educational purposes. The creators of this tool do not endorse or condone any illegal activity.

## References

- [DOJ Epstein Library](https://www.justice.gov/epstein)
- [Internet Archive - Epstein Documents](https://archive.org/details/combined-all-epstein-files)
- [PDF Association Analysis](https://pdfa.org/a-case-study-in-pdf-forensics-the-epstein-pdfs/)

## Full corpus: native Apple OCR and audio transcription

Use the `corpus` commands for directories and ZIP archives at full-dataset scale.
They stream an NDJSON source manifest, externally sort and deduplicate it, and
submit ordinary durable upserts. PDFs and audio stay in their original location;
no permanent per-page PDF copies are needed. Video files are excluded.

The Antfly server performs extraction as durable artifact enrichments. Good PDF
text is retained, with Apple Vision OCR as the fallback. Apple speech transcription
retains timestamps; page and transcript units feed BM25 and optional semantic search.
Apple configurations have no `model` or `max_tokens`. Embeddings and graph extraction
still use the example's Antfly inference models.

Build Antfly on the Mac with `zig build antfly -Dapple-providers=true`; Apple speech
requires macOS 26 or newer and installed speech assets for the selected locale.
The source gateway and example CLI can run separately from the Antfly server.
Audio formats: WAV, MP3, M4A, AIFF/AIF, CAF and FLAC. Apple transcription accepts up
to 128 MiB and one hour per recording; split longer/larger recordings first.

### Prepare a pilot on wolfspider

Build the example with `GOWORK=off GOEXPERIMENT=simd go build -o epstein .`.
Assign a stable dataset label to each source. Multiple sources may share a label
when they contain copies of the same release. EFTA filenames identify documents
within a dataset, including their extension; duplicate IDs are compared by SHA-256,
and conflicting contents fail preparation. Other filenames use a hash of the
relative path. Existing `pages/`, hidden files and AppleDouble files are excluded.

```bash
./epstein corpus prepare --state /Volumes/T9/epstein-pilot \
  --source ds9=/Volumes/T9/epstein-docs/doj-dataset-9 \
  --source ds10=/Volumes/T9/epstein-docs/doj-dataset-10.zip \
  --source ds11=/Volumes/T9/epstein-docs/doj-dataset-11.zip \
  --source ds12=/Volumes/T9/epstein-docs/doj-dataset-12.zip \
  --base-url http://wolfspider:3001 \
  --limit-pages 10000 --limit-files 2000 --pages-per-record 25

./epstein corpus serve --state /Volumes/T9/epstein-pilot --listen :3001
```

Use the actual ZIP filenames on the volume. Select completed, stable sources;
prepare later downloads in a new state when the download finishes. Consolidated
PDFs from datasets 1–8 can also be supplied through their containing directories.
A pilot stops before opening later PDFs or invoking OCR/transcription; discovery
still reads directory/ZIP metadata to produce the deduplicated inventory.
`--limit-pages` budgets PDF pages; `--limit-files` bounds both PDF and audio sources.

Source URLs contain a signed locator, not an arbitrary filesystem path. The gateway
checks signatures, root containment and source versions. Keep `sources.json` private:
it contains the signing key. Keep the gateway running while enrichment or browsing
needs the originals. ZIP audio supports browser range requests, and the search UI
provides playback with transcript snippets and timestamp links. PDF links preserve
original page numbers; PDF extraction URLs select at most `--pages-per-record` pages.
Temporary ZIP/PDF ranges use the host's temporary directory, not permanent split
files. Large consolidated PDFs require enough temporary space and memory for the
PDF parser; smaller source PDFs avoid page rewriting when the whole file is selected.

If Antfly fetches sources from localhost or Tailscale, explicitly allow the source
host in its common JSON config (use your actual DNS name):

```json
{"remote_content":{"security":{"block_private_ips":false,"allowed_hosts":["wolfspider"]}}}
```

Start the Apple-enabled server with `antfly standalone --config antfly.json`.
The advertised source URL must be reachable from both Antfly and the browser.

### Load, resume, and build the graph

```bash
# Create an empty table, then measure a storage baseline.
./epstein corpus load --state /Volumes/T9/epstein-pilot \
  --table epstein_pilot --create-table --create-only --graph
./epstein corpus stats --state /Volumes/T9/epstein-pilot \
  --table epstein_pilot --data-dir /path/to/antfly-data --output baseline.json

./epstein corpus load --state /Volumes/T9/epstein-pilot --table epstein_pilot --graph
# Run after extraction finishes. Pending sources stop without advancing the checkpoint.
./epstein corpus graph --state /Volumes/T9/epstein-pilot --table epstein_pilot
./epstein serve --corpus --table epstein_pilot
```

Use `--semantic=false` on both load commands for a BM25-only pilot, and
`serve --corpus --indexes document_text` for its search UI. Omit `--graph` and the
`corpus graph` command to measure text/semantic storage without graph costs. Install
the embedding model and optional extractor model using the existing model-pull
instructions above. Use `--language` to select the Apple OCR/transcription locale.
The relation extractor defaults to the registered model ID `antflydb/gliner2-base-v1`;
its installed bundle selects the quantized weights. Use `--artifact-extractor-model`
on both corpus load commands to select another registered extractor. Inspect
`GET /ai/v1/models` to find registered IDs. Existing tables retain their producer
configuration; changing the CLI default does not migrate them. For an existing
pilot using `antflydb/gliner2-base-v1-q4_k`, create a new table with the corrected
flags and rerun loading and graph materialization. Producer configuration is
write-only, so an index status response cannot recover its original definition.
Permanent extraction HTTP errors (including missing models and authorization
failures) settle as failed artifacts; provider recovery or configuration changes
require reprocessing. Rate limits, temporary server errors, and stale capability
leases remain retryable. Provider logs include the HTTP status and error details.
`ANTFLY_API_KEY`, when set, supplies a bearer token to corpus API requests.

Preparation checkpoints every 100 sources and at completion. Resume with
`corpus prepare --state ... --resume`; its saved configuration is immutable.
Loading checkpoints only acknowledged `sync_level=write` batches. Rerun the same
load command without `--create-table` after interruption. A lost acknowledgement
may replay a batch safely using stable IDs. Resume checks bind the manifest,
server, physical table ID and load configuration, so a recreated table cannot
silently inherit an old checkpoint. The table also records its page-window and
provider/index configuration; loading a different layout into that table is rejected
to prevent overlapping page records. New source states can append to the same table
when they use the same layout and settings. After a crashed process, remove the state
`.lock` directory only once that process has stopped. Each state has one writer.

OCR, transcription, chunking and embeddings run in Antfly's durable enrichment
workers; completing `corpus load` means the source records were submitted, not that
indexing finished. Failed provider work remains visible in server artifact/index
status and can be repaired with the artifact reprocess API. Reprocessing acknowledges
that repair was durably queued; poll the artifact generation/status for completion.
Unrelated retrying providers do not turn that acknowledgment into a failure.
`corpus graph` traverses
units in bounded pages and reconciles deterministic graph-unit rows containing
plain extracted text and provenance. Each source has a durable materialization
revision marker in the table. Every invocation checks the latest artifact generation;
unchanged revisions are reused, repaired revisions update their unit rows, and
obsolete units are deleted along with their generated relations. The local checkpoint
reports pass progress; deleting it does not cause unchanged sources to be rewritten.
Existing graph checkpoints migrate automatically on the next pass.

A source is staged on disk and its revision is rechecked before publication. Failed
or changing extraction leaves its prior materialization intact during preparation.
Interrupted publication is recovered on the next pass; the revision marker is
written only after upserts and stale-row cleanup succeed. A version-checked
publishing marker fences every mutation through Antfly's OCC transaction API;
a superseding writer or source replacement rejects the older pass's writes.
If a repair overlaps publication, rerun the command to converge to the latest
revision. Graph reconciliation requires document lookup version tokens and OCC
transactions from the server. Graph-unit links
use original PDF page numbers or the audio unit's recording timestamp, and appear
in the graph visualization. Completion reports materialization submission; inspect
graph index `readiness.sources` and repair status separately for asynchronous
relation extraction failures. The embedding index uses
`coverage_policy: partial`: graph-unit rows, completion markers, and validated
empty extraction outputs intentionally skip embedding work, while provider failures
still block healthy coverage. Existing tables keep their previous policy; use a
new pilot table with the updated load configuration. Graph visualization searches
the row BM25 index's `content` field to seed the graph-unit keys that own edges;
regular corpus search still groups artifact matches by source and keeps page/time
citations.
These optional rows add stored text and default BM25 postings as well as graph
artifacts/edges. A validated OCR reader response with no detected text preserves
embedded text (or completes an empty page) without counting as a provider failure;
malformed responses, prompt echoes and actual rendering failures remain visible. The original small-dataset `prepare`, `enrich` and `entities`
commands retain their JSON format; use the corpus pipeline for large inputs.

### Measure storage before scaling up

```bash
./epstein corpus stats --state /Volumes/T9/epstein-pilot \
  --table epstein_pilot --data-dir /path/to/antfly-data \
  --baseline baseline.json --output pilot.json
```

Snapshots include prepared files/pages/audio and logical source bytes, preparation
and submission rates, table storage status, individual index status, and optional
allocated bytes from `du -sk`. Run `stats` on the machine that owns `--data-dir`.
Take the final snapshot after artifact/index queues settle and compaction stabilizes.
Allocated delta includes the whole supplied storage directory; use an isolated pilot
server/data directory to attribute it to this table. Source bytes count whole chosen
files, even when a pilot selects only some PDF pages. Raw source storage remains
additional to Antfly storage. Compare representative text, scanned PDF and audio
pilots, with and without semantic/graph indexes, before extrapolating to the corpus.

Graph searches show only edges connected to the matching rows. A search with no edges stays empty. Citation labels and PDF page/audio time links come from the seed documents returned in the same query; graph rendering makes no additional document requests.
