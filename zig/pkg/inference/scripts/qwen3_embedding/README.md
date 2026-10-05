# Qwen3-Embedding parity oracle and qualification

Scripts for qualifying the Antfly inference runtime against the fp32
Transformers reference for `Qwen/Qwen3-Embedding-0.6B` (28-layer
`Qwen3ForCausalLM`, last-token EOS pooling, L2 normalize, 1024-dim output with
Matryoshka truncation to 32-1024 dims).

## Files

- `transformers_embedding_oracle.py` — runs the pinned checkpoint (fp32, CPU,
  eager attention, left padding) over a fixed prompt set and emits the golden
  JSON (`antfly.qwen3_embedding.transformers_oracle.v1`): exact token ids
  (including the single tokenizer-appended trailing EOS `151643`), applied
  instruction per case, and embeddings at dims 1024/256/32 (reduced dims are
  truncate-then-renormalize of the 1024 vector).
- `qualify_qwen3_embedding_common.py` — implements the backend-neutral request
  and numerical gates used by the thin Metal and CUDA qualification entry
  points.
- `qualify_qwen3_embedding_metal.py` — replays the oracle cases against a
  running Antfly server's `/ai/v1/embeddings` endpoint and gates cosine
  parity per precision tier, batch-vs-single equivalence, `dimensions`
  truncate+renormalize behavior, unit norms, and top-1 retrieval-rank
  agreement. Stdlib only.
- `requirements-qwen3-embedding-oracle.txt` — frozen oracle dependencies.
- `test_transformers_embedding_oracle.py`, `test_qualify_qwen3_embedding_metal.py`
  — offline unit tests (no network, no model downloads).
- `benchmark_qwen3_embedding_endpoint.py` — throughput/latency benchmark against
  `/v1/embeddings` (separate lane; see its docstring and `BASELINE.md`).
- `build_qwen3_embedding_fixture.py` — builds exact-token document or rendered
  query fixtures from a local tokenizer. Every complete prompt is tokenized and
  checked; no model or dependency download runs implicitly.
- `run_qwen3_embedding_gap_qualification.py` — owns local Antfly and llama.cpp
  servers, records executable/model/fixture hashes, runs repeated short/passage
  comparisons, HTTP correctness and disconnect recovery, and an isolated
  eight-worker capacity soak. Reports fail when a gate fails.
- `fixtures/qwen3_embedding_0_6b_exact_tokens.json` — compact, benchmark-only
  recipe for the cache-neutral 511- and 2551-token comparison prompts. The
  benchmark expands it in memory and verifies the canonical expanded-case
  SHA-256 before making a request. Production serving does not read it.

## Qualification evidence

`reports/` contains historical receipts and compact qualification summaries
referenced by `BASELINE.md`. For new runs, version a summary of the protocol,
artifact hashes, results, failures, and qualification limits. Preserve complete
raw reports, source snapshots, and measurement samples outside Git, in an ignored
output directory or separately hosted artifacts. Update `BASELINE.md` alongside
the summary when rerunning the protocol against the pinned model artifacts.

The [M4 Q8_0 gap summary](reports/qwen3_embedding_metal_gap_m4_q8_summary.md)
records the 2026-10-01 qualification and 2026-10-02 review-fix checks, including
the hashes and local archive location of both complete raw receipts.

## Produce the oracle

```bash
python3 -m venv .venv-qwen3-embedding && source .venv-qwen3-embedding/bin/activate
pip install -r requirements-qwen3-embedding-oracle.txt
python3 transformers_embedding_oracle.py \
    --output /tmp/qwen3_embedding_oracle.json
# or fully offline from a local checkout of the pinned revision:
python3 transformers_embedding_oracle.py \
    --model-dir /path/to/Qwen3-Embedding-0.6B \
    --output /tmp/qwen3_embedding_oracle.json
```

The hub path pins revision `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3` by
default; the dependency set is verified fail-closed against the pins in the
requirements file.

## Qualify a local server

Pull one of the served variants first:

```bash
antfly inference pull hf:Qwen/Qwen3-Embedding-0.6B-GGUF:q8-0-bundle-v1  # GGUF Q8_0
antfly inference pull hf:Qwen/Qwen3-Embedding-0.6B-GGUF:f16-bundle-v1   # GGUF F16
antfly inference pull hf:Qwen/Qwen3-Embedding-0.6B:bf16-safetensors-bundle-v1  # safetensors
```

These pinned references reproduce the qualification fixtures. Normal pulls can
use `Qwen/Qwen3-Embedding-0.6B-GGUF` or
`Qwen/Qwen3-Embedding-0.6B:safetensors`. Serving validates the artifact and
backend contract, without requiring a catalog identity or qualification receipt.
See [model compatibility](../../MODEL_COMPATIBILITY.md).

For Metal, start the supervised server with a required backend and require the
resident Qwen embedding route. Use an absolute model root:

```bash
ANTFLY_INFERENCE_REQUIRED_BACKEND=metal TERMITE_EMBED_RESIDENT_FAIL_CLOSED=1 \
  antfly-inference run --models-dir /absolute/path/to/models \
  --preload-model embedder:metal:MODEL_ID
```

Retain the `selected backend metal` and resident-success logs alongside the
server executable hash, source revision, model/tokenizer hashes, device, macOS
version, and memory configuration. Then run the numerical gate against that
server:

```bash
python3 qualify_qwen3_embedding_metal.py \
    --oracle /tmp/qwen3_embedding_oracle.json \
    --base-url http://127.0.0.1:8080 \
    --model Qwen/Qwen3-Embedding-0.6B-GGUF \
    --tier q8_0 \
    --report /tmp/qwen3_embedding_qualification.json
```

Exit code 0 means every gate passed; 1 prints a failure table; 2 is an
infrastructure/usage error.

The Metal qualifier checks numerical behavior. Its `--tier` is an operator
label, and the script does not attest the server backend, executable, or model
bytes. A passing report alone is insufficient evidence of Metal execution or a
particular precision tier. Strict throughput comparisons separately attest live
process arguments and executable/model hashes.

### Qualified short-row and bounded-batch controls

The three serving features below default on for Apple M4-family Metal devices.
Short-row projection and normalization defaults require the admitted embedding
workspace; batching still requires eligible dense Qwen3 text embeddings and a
dynamic, unpadded tokenizer. Other devices retain explicit opt-in. Current
qualification and its scope are recorded in the
[default qualification receipt](../metal_serving_defaults_m4_summary.md).
Explicit empty, `0`, `false`, `no`, or `off` values disable each feature.

`TERMITE_METAL_ENABLE_Q8_0_SMALL_ROWS=1` enables SG-v2 projections and fused
gate/up for 9–64 rows, selecting M32 for short rows and retaining the established
larger-row selection. It also enables parallel RMS reduction for those rows.
`TERMITE_METAL_ENABLE_Q8_0_SMALL_ROWS_M64=1` selects the M64 short-row candidate
for comparison; `TERMITE_METAL_DISABLE_SMALL_ROWS_NORM_REDUCE=1` isolates the
normalization change. With bounded batching enabled, short frames also reuse
the serial planned encoder through attention and FFN; set
`TERMITE_METAL_DISABLE_QWEN3_SMALL_ENCODER=1` to isolate that change. These
disable flags take precedence; the M64 short-row comparison remains opt-in.

`TERMITE_METAL_ENABLE_QWEN3_HEAD_NORM_SG=1` additionally selects a SIMD
reduction and paired rotary stores for bounded 9–64-row frames with full
128-dimensional rotary heads and 8 or 16 heads per row. Four independent
32-lane SIMD groups normalize four heads, without a threadgroup reduction.
Other shapes, partial or consecutive rotary layouts, and incompatible SIMD
widths use the existing head-normalization kernel. Set this flag to `0` to
isolate the change.

Bounded last-token embeddings also encode the resident L2 normalization tail
before submitting the decoder frame. Set
`TERMITE_METAL_DISABLE_QWEN3_FRAME_NORMALIZE=1` to use the separate normalization
submission for comparison. Reduced dimensions still truncate and renormalize
the normalized full-width vector through the existing API route.

`TERMITE_METAL_ENABLE_QWEN3_EMBED_BATCHING=1` enables bounded, unpadded
tokenization and length buckets for dynamic resident Metal Qwen3 embeddings.
The planner starts with at most 2048 padded tokens, then halves a chunk only
after an identified admission constraint. It preserves API row order, permits
long singletons when the operator budget admits them, and checks cancellation
between chunks. GPU admission runs under the model gate using actual Q/KV
widths, retained frame slots, replacement overlap and fp16 K/V. Retained slots
never replace the separate fresh-temporary allowance for a smaller next shape. Persistent
workspace remains leased across requests; fresh Metal workspace allocations
are bounded by the incremental permit. Model teardown releases physical buffers
before their leases. Warm HTTP requests briefly pin the loaded publication to
copy its immutable manifest, then release that pin before execution or recovery;
cold requests still validate metadata before loading assets.
EOS-only singleton documents use one inactive padding row so they stay on the
resident dense route. Other models and backend paths retain their existing route.

`TERMITE_EMBED_CAPACITY_DIAGNOSTICS=1` logs attempted chunk geometry, retained
workspace, requested/current bytes and applicable limits. Pass-only GPU counters
that would require splitting a bounded Qwen encoder report an incomplete sample
instead of changing frame topology. CPU timing and dispatch census remain usable.
Set all three enable flags to `0` and restart the server to roll back these
serving features. Removing them restores the device-qualified defaults.

For the gap qualification lane, create a fixture and run the owned-server
driver from this directory (install the pinned oracle tokenizer dependency
explicitly if it is unavailable):

```bash
python3 build_qwen3_embedding_fixture.py \
  --tokenizer /absolute/models/model/tokenizer.json \
  --model-file /absolute/models/model/model.gguf --output /tmp/qwen-exact.json
python3 run_qwen3_embedding_gap_qualification.py \
  --antfly /absolute/antfly-inference-bench-server --llama /absolute/llama-server \
  --model-dir /absolute/models/model --fixture /tmp/qwen-exact.json \
  --output-dir /tmp/qwen-gap-report --stage all
```

The output directory must be new. Short gates require a lower 95% throughput
ratio of at least 1.0 and p95 latency within 5% of the reference. The 32×256
passage gate requires a lower ratio of at least 0.90. Every measured vector must
meet cosine 0.995. The soak requires at least 1800 seconds and eight workers,
zero failed requests, a bounded worker-process RSS plateau, no worker restart,
and no host swap growth. Host-wide swap growth invalidates the run without
attributing the cause to Antfly. Competing builds or GPU work invalidate isolation.
The [local M4 Q8_0 results](BASELINE.md#short-query-and-capacity-results--2026-10-01)
include a fixed 600-second conditioning phase before an independent 1800-second
soak on the same worker, with unchanged plateau and swap thresholds. The archived
raw receipt embeds that repeat driver's source and preserves the initial soak's plateau
failure and every conditioning/measurement sample. The default driver uses its
normal geometry warmup; it does not add this extended conditioning implicitly.
The [compact qualification summary](reports/qwen3_embedding_metal_gap_m4_q8_summary.md)
records scoped workspace admission and owned warm-request metadata, focused
admission/ownership tests, HTTP correctness and eviction checks, and the repeated
short matrix on the 2026-10-02 review-fix executable. Capacity, passage regression,
and conditioned soak evidence cover the original 2026-10-01 executable.
Longer singletons may need explicit `--scratch-budget-mb` and
`--combined-budget-mb` operator caps in addition to the process budget;
qualification records those caps and does not
silently weaken the default limits. Run the passage stage with the preserved
baseline binary, the same budgets and all three enable flags set to `0`, then pass its
report directory as `--baseline-dir` for the candidate run. This gates supported
passage cells at a lower 95% throughput ratio of 0.95 against that fixed baseline,
using an unpaired bootstrap across the independent runs. Provenance, model,
fixture, shape and budget mismatches fail the comparison. The newly supported
32×256 cell uses its separate reference gate.
`--stage regression` measures the six previously supported passage/batch shapes
and applies the fixed-baseline 5 percent gate when `--baseline-dir` is supplied.
Its reference responses still require strict attestation and vector parity;
throughput is judged against the fixed baseline. Run that stage on the baseline
first with the same caps, then on the candidate with its report directory.
For a direct comparison under changing host conditions, supply
`--stage regression --baseline-antfly /absolute/fixed-audit-bench-server`
instead of `--baseline-dir`. Both builds stay loaded across the six shapes,
accept identical budgets and model bytes, and alternate measured requests.
The reference endpoint is then the fixed Antfly build, with its injected EOS
usage offset and serving flags explicitly set to `0`. Each paired round requires a
lower 95% candidate/baseline throughput ratio of 0.95 and cosine 0.995.
It uses no llama.cpp server, and cannot be combined with `--baseline-dir`.
Use `--stage capacity` with the default limits to isolate the three-round
32×256 reference gate from the long-singleton operator-budget lane.
The reference defaults to a 2048-token physical microbatch (or the singleton's
full length when longer), while accepting the same API batch. Override with
`--reference-ubatch`; live arguments and the selected cap are recorded, and
baseline comparisons require the same cap. Keep assets and reports in an
ignored workspace directory if they need to survive a host restart.

`--stage correctness --eviction-model SIBLING_MODEL` checks ragged versus
individual vectors, both error policies and partial-result indexes, MRL widths,
query rendering, request-limit rejection, disconnect and overload recovery,
and eviction/reload. A sibling with the explicit four-token capability also
checks that query prefixes count toward the limit under both error policies.
The eviction lane loads through requests instead of pinning a startup preload.

For queries, build a second fixture with `--lengths 32,64 --query-prefix` set to
the exact server-rendered instruction prefix, and pass it with `--query-fixture`.
The driver strips that prefix for Antfly's query API and sends the rendered text
to llama.cpp. It erases every idle reference slot before each reference request,
outside the timer, and requires the erasure acknowledgement. A common
instruction prefix alone is not cache-neutral. This uses llama.cpp's documented
[slot erase endpoint](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md).

The default oracle bounds inputs to 8192 tokens and replays `served_text` for
truncated cases. This does not qualify server-side truncation at the model's
32768-token context boundary. Test that boundary with original overlong input,
one trailing EOS, and a matching reference separately. Also qualify the intended
short-query, passage, long-document, ragged-batch, and concurrent serving
workloads: batch-1 passage throughput does not establish capacity or latency for
the other shapes. The live MRL gate below checks 256 dimensions; 32-dimensional
serving needs its own replay even though the oracle stores a 32-dimensional
reference.

### CUDA qualification

Run the server with CUDA as a required backend so qualification fails closed
instead of silently falling back to CPU. Use an absolute models path so preload
and request-time discovery resolve to the same cache key:

```bash
ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda antfly-inference run \
  --models-dir /absolute/path/to/models \
  --preload-model embedder:cuda:MODEL_ID
```

After the server reports `selected backend cuda` and `listening`, run:

```bash
python3 qualify_qwen3_embedding_cuda.py \
  --oracle /tmp/qwen3_embedding_oracle.json \
  --base-url http://127.0.0.1:8080 \
  --model MODEL_ID
```

`MODEL_ID` must be a promoted Q8_0 or BF16 CUDA reference (or its friendly
alias). The qualifier derives the numerical tier from that reference instead
of accepting a separately supplied label.

For CUDA throughput measurements, run the pretokenized E2E benchmark (model
loading and tokenization remain outside the timed region):

```bash
zig build -Dcuda=true -Doptimize=fast bench-qwen3-embedding-e2e -- \
  --backend cuda --model-dir /path/to/qwen3-embedding-q8 --batch 8 --seq-len 256
zig build -Dcuda=true -Doptimize=fast bench-qwen3-embedding-e2e -- \
  --backend cuda --model-dir /path/to/qwen3-embedding-q8 --lengths 32,64,128,256
```

## Gates per tier

| Tier | Min cosine vs fp32 oracle (1024-dim) |
| ---- | ------------------------------------ |
| bf16 | 0.999 |
| f16  | 0.999 |
| q8_0 | 0.995 |
| q4_k | 0.99  |

Tier-independent gates: batch-vs-single cosine >= 0.9999; `dimensions=256`
response vs host-side truncate+renormalize of the server's own 1024-dim vector
cosine >= 0.99999; L2 norm of every returned vector within 1e-3 of 1.0; top-1
retrieval agreement on the oracle query/document matrix; query-vs-document
embeddings of identical text must differ.

## Tests

```bash
python3 -m unittest discover -s . -p 'test_*.py' -v
```
