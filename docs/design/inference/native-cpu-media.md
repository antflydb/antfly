# Native CPU media execution

Tracks [#1668](https://github.com/antflydb/antfly/issues/1668), after #1659 and #1665.

## Preparation and ownership

Checkpoint maps remain random-access by default. A consumer that scans a full
selected tensor temporarily gives that mapped span sequential advice, then
restores random advice on all exits. The kernel may round to page boundaries;
this does not prefetch or allocate a full checkpoint. Metadata discovery and
embedding-row lookup do not warm all weights. CPU matrix consumers recognize
borrowed F32 operands by both their model owner and actual mapped address;
a converted or cached buffer cannot inherit mmap advice just from its name.

F16/BF16 conversion validates dimensions, overflow and payload length before
allocation. Allocation failures release both data and shape with their original
alignment. Recurrent decoder projection matrices share the existing bounded BF16
linear kernel with attention decoders; convolution, norms and recurrent state
retain their normal representation. The kernel widens at most an 8 MiB matrix
block (or one full row) at a time, checks cancellation between blocks and returns
its scratch reservation on errors. It never makes a full F32 vocabulary mirror
just to apply the output projection. Sparse expert and quantized paths retain
their own storage and execution policy.

Native serial Whisper execution binds the request-local workspace to the loaded
encoder's process ledger. Binding rejects a different ledger or an active borrowed
permit. The execution estimate becomes credit for actual allocations; allocations
grow only the deficit. After encoding, surviving tensors retain credit through
their allocator while unused encoder workspace is released before decoding.
Queued/batched and accelerator executors keep their separate memory ownership.
Document leaves preserve the complete execution control, including the logical
request permit. Direct reader callers acquire that permit before queueing;
physical encoded/raster batches reserve weighted work without acquiring another
logical request. A one-request worker can therefore process a document without
rejecting its own nested OCR call. Expanded work saturation remains a retryable
503 with the admission reason and Retry-After.

Configure request concurrency separately from weighted work capacity. The CPU
profile uses one logical request, 64 work units and a 4-million-pixel request
ceiling. Omitting work capacity makes it follow concurrency and can reject a
normal full-page raster before model execution.

Generative execution uses a reclaiming, thread-safe heap under its existing
model generation resource lease. The caller allocator owns returned text, not
model tensors. Encoded document readers follow the same boundary: execution
retains its own owned buffers, then copies only the compact result into the
caller's bounded output allocator. This prevents a document's 512 MiB
source/metadata/output quota from rejecting legal vision or decoder workspace,
and avoids charging the execution lease again through the document allocator.
Output-copy allocation failures roll back every completed result item.

CPU automatic scratch is a fraction of the safe process pool. Explicit caps,
total host/combined budgets and live memory pressure still apply. Preflight
failures record requested scratch, limiting policy and current reservations using
the same rate-limited diagnostic recorder as execution failures. A stable policy
failure remains permanent; current occupancy remains retryable.

## Reproducible verification

Use Zig 0.17 and a worktree-local cache. Debug regression tests cover malformed
weights, all allocation failures, BF16 values beyond the F16 range, cancellation,
workspace transfer, wrong-ledger rejection and explicit budget failure/recovery.
For realistic timings build the same optimized CPU target as the release:

```sh
cd zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build \
  -Dtarget=aarch64-linux-gnu -Dcpu=baseline -Dmetal=false -Dcuda=false \
  -Dblas=auto -Doptimize=fast -Dstrip=true \
  -Dinference-compile-memory-mb=16384 -j2
```

The verification tool never downloads models, changes shared services or
provisions infrastructure. It starts a fresh local server for each case and
terminates its entire owned process group on success, timeout or failure.
Provide an already installed model root and a JSON list of cases:

```json
[
  {
    "model": "lightonai/LightOnOCR-3-4B",
    "kind": "image",
    "fixture": "/fixtures/ocr.png",
    "checkpoint_dir": "/models/lightonai/LightOnOCR-3-4B",
    "expected": ["ANTFLY TEST 123"],
    "options": {"max_tokens": 128}
  },
  {
    "model": "openai/whisper-large-v3",
    "kind": "audio",
    "fixture": "/fixtures/speech-over-30-seconds.wav",
    "checkpoint_dir": "/models/openai/whisper-large-v3",
    "expected": ["first sentence", "final sentence"],
    "options": {"audio_options": {"language": "en", "timestamps": true}}
  }
]
```

Use `kind: "document"` and a PDF fixture to exercise native document orchestration
and page rendering with the selected reader. For selected pages set `"pages": [2]` in the case; the tool places that
selection in the document source. Keep the
whole-document fixture small or provide a prepared one-page PDF; a token-limited
prefix does not count as a complete read. Use a separately rendered page to
isolate OCR throughput from native document orchestration.

```sh
ANTFLY_OPENBLAS_LIBRARY=/libraries/libopenblas.so.0 \
LD_LIBRARY_PATH=/libraries \
taskset -c 5-8 python3 zig/tools/qualify_cpu_media.py \
  --binary zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --config cpu-media.json --models-dir /models --cases cases.json \
  --threads 4 --blas openblas --trials 3 --timeout 600 \
  --cancel-after-ms 100 --report .benchmark-results/cpu-media/report.json
```

On Linux, the report samples PSS, CPU ticks and major/minor faults for the owned
supervisor and workers. It records runtime/config/fixture/checkpoint/OpenBLAS
hashes, affinity and host load. Preserve server logs for backend selection,
resolved memory policy and admission diagnostics. Set
`TERMITE_SERVER_GENERATE_TIMING=1` for serial reader resolution, loading,
setup and generation timings (including vision, prefill and decode). For a
separate Whisper phase diagnostic, `antfly-inference-dev transcribe MODEL_DIR
speech.wav --backend native --language en` emits `timing_ms` with mel, encoder,
prefill and decode times. That direct CLI diagnostic does not replace the
windowed `/read` check or its inclusive startup measurement. Compare
load latency, first complete request, startup-to-first-complete and warm requests
separately. A fast model-instance POST is not successful execution.

Page cache is deliberately uncontrolled: hashing checkpoints can warm it. Do not
flush a shared host's caches to manufacture a cold-I/O result. Report concurrent
GPU/CPU/memory-bandwidth workloads and do not extrapolate a tiny image into
full-corpus throughput. Full-page correctness and long-audio tail coverage are
separate checks from warm latency.

## Adoption

The native `/read` service owns PDF rendering, OCR and long-audio transcription.
Antfly Sources supplies source references and provenance; Antfly Grep and
Grepstein consume the resulting artifacts. They must not add another OCR stack
or copy source data to compensate for native execution failures.

A CPU pass on GB10 does not qualify x86 or an arbitrary Cloud pod. After release,
import the checksummed runtime through Colony's existing catalog and shared-pool
workflow. Preserve the shared read/write layout, capability-based routing and
explicit workload memory profiles; do not attach local inference pools to the
Grepstein database instance. Recheck a real PDF and a recording longer than one
Whisper window through the Cloud endpoint, then verify resulting transcript/OCR
artifacts through Sources, Grep and the hosted explorer. Release promotion and
infrastructure applies remain separate from these source changes.
