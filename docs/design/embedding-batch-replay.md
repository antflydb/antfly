# Diagnosing managed embedding throughput

Use a ReleaseFast build for throughput measurements and an isolated database.
Compare the same model revision, backend, task type, instruction, and exact
batch contents. A chunker's target token count is not necessarily the model's
token count; a mixed-length batch is padded to its longest encoded row.

## Opt-in capture

`ANTFLY_EMBED_TRACE_DIR` enables text-only captures at the inference boundary.
It must name an existing absolute directory. **Files contain private corpus
text, instructions, and embedding vectors.** Never enable this on sensitive
production traffic without approval. Use a private directory, do not commit
captures, and remove them under your normal data-retention policy after use.
Normal logs do not contain inputs or vectors.

```sh
trace_dir=$(mktemp -d /private/tmp/antfly-embed-trace.XXXXXX)
ANTFLY_EMBED_TRACE_DIR="$trace_dir" ./zig-out/bin/antfly standalone \
  --port 18085 --health-port 14205 \
  --data-dir /private/tmp/antfly-embed-replay-data \
  --models-dir /path/to/isolated/models \
  --inference-host-budget-mb 8192
```

Load a bounded fixture into a fresh managed embedding index on this server.
Wait for that fixture's indexing to finish before replaying, so background
indexing does not compete with the endpoint. Prewarm before ingestion if the
experiment is intended to compare warm paths.

Capture is limited to 64 managed/direct and 64 HTTP requests per process;
the separate quotas leave room for endpoint replay after backfill. Each
request must contain 1–32 text inputs with at most 256 KiB of combined text
and instruction. Files are at most 2 MiB, created exclusively with mode 0600
on POSIX, and never overwrite existing files. Mixed-media and partial-success
HTTP requests are not captured as text batches. Restarting resets the quotas;
use a fresh directory for each experiment. Capture failures do not fail an
otherwise successful embedding operation.

## Replay

From the repository root, with the same server and model still available:

```sh
python3 zig/tools/replay_embedding_trace.py "$trace_dir" \
  --url http://127.0.0.1:18085 \
  --max-batches 8 --batch-size 8 --warmup 1 --repeat 2 \
  --output /private/tmp/antfly-embed-replay-results.json
```

The output file must not already exist. The tool checks batch indices,
dimensions, finite output values, and maximum absolute vector error (default
`1e-4`). It preserves text, task type, and instruction from each capture.
Warmups are excluded from reported HTTP timings. Reports contain timings,
shapes, and request hashes, not corpus text or vectors. Non-loopback targets
require explicit `--allow-remote`; redirects and environment proxy settings
are disabled to avoid inadvertently forwarding private inputs elsewhere.

This is input-identical replay, not a counterbalanced benchmark: the original
managed observations precede the HTTP observations. Thermal conditions,
memory pressure, graph warmup, and competing requests can still differ.
Repeat in a fresh process/database with controlled warmup to test stability.

## Reading timings

Capture timings use a monotonic clock and exclude JSON capture serialization
and file writing from `total`. Instrumentation still adds overhead to outer
managed and HTTP wall times, so disable it for final throughput qualification.

- `admission`: entering the inference admission budget.
- `resolve_manifest`: resolving the model and reading its lightweight manifest.
- `model_acquire`: acquiring/loading the model, summed over recovery attempts.
- `asset_prepare`: preparing required embedding assets/pipeline.
- `tokenize`: prefix application, tokenization, and trailing-token preparation.
- `execution_lock`: waiting for the model's shared execution lock.
- `execute_pool_normalize`: synchronous backend execution, pooling, and
  normalization. This is host-observed time including device waits, **not a
  GPU-only kernel timer**.

Stages are not an exhaustive additive accounting of `total`: tensor setup,
admission around preprocessing, output handling, and other boundary work may
remain unattributed. `attempts > 1` identifies inference runtime recovery;
multiple shapes can also reflect adaptive batch splitting. Shape records
include per-item active token lengths, padded token count, and padded sequence
length. Zero-filled unused slots in `token_lengths` are not extra inputs.

Opt-in `managed embed trace` log lines additionally show pacer/context time,
provider start/end, success, and overall adapter time. Match their monotonic
interval against a capture's `started_ns` to locate bridge/result overhead.
The enrichment worker's existing total embedding timer includes provider
retry/backoff time but not later artifact writes/publication. Inspect retry
counters/logs separately. Timing totals are runtime-scoped; do not attribute
shared-runtime totals to one model when several indexes are active.

If padded-token work explains the gap, investigate bounded length-aware
batching while preserving source/vector association. If lock/acquisition time
dominates, investigate contention or reloads. Do not increase concurrency
before identifying which stage limits throughput.
