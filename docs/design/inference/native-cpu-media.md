# Native CPU media execution

Tracks [#1668](https://github.com/antflydb/antfly/issues/1668), after #1659 and #1665.

## Preparation and ownership

Checkpoint maps remain random-access by default. A consumer that scans a full
selected tensor temporarily gives that mapped span sequential advice, then
restores random advice on all exits. A compute invocation advises a borrowed
range only on its first matrix traversal, avoiding per-token advice syscalls.
The kernel may round to page boundaries;
this does not prefetch or allocate a full checkpoint. Metadata discovery and
embedding-row lookup do not warm all weights. CPU matrix consumers recognize
borrowed F32 operands by both their model owner and actual mapped address;
a converted or cached buffer cannot inherit mmap advice just from its name.

F16/BF16 conversion validates dimensions, overflow and payload length before
allocation. Allocation failures release both data and shape with their original
alignment. Recurrent decoder projection matrices share the existing bounded BF16
linear kernel with attention decoders; convolution, norms and recurrent state
retain their normal representation. The kernel widens at most an 8 MiB matrix
block (or one full row) at a time and checks cancellation between blocks. It
reuses at most 8 MiB of conversion storage per compute invocation, keeping the
retained capacity charged until teardown. Oversized rows use temporary storage
after releasing idle cached capacity; failed growth refunds its reservation.
It never makes a full F32 vocabulary mirror
just to apply the output projection. Sparse expert and quantized paths retain
their own storage and execution policy.

Native Whisper preprocessing binds its request-local workspace to the loaded
encoder's process ledger. Binding rejects a different ledger or an active borrowed
permit. The physical dispatcher chooses ownership: a serial fallback lends its
execution estimate to actual allocations, while queued/fused work uses the
batch's allocator and lease. Merely installing a dispatcher does not imply a
batch was executed. Queue metadata trims unused growth capacity before admission
of the physical batch. After encoding, surviving outputs retain their respective
allocation/batch credits while unused encoder workspace is released before decoding.
Serial credit is bound to the actual callee for the duration of that call and
restores the prior preparation binding afterward. Distinct encoder/decoder
sessions must not execute through each other's permit. KV outputs, accelerator
execution and foreign ledgers retain separate execution/output leases; only
their small tensor container transfers into the request allocator.
Document leaves preserve the complete execution control, including the logical
request permit. Top-level direct document read/extract operations acquire that
permit before source processing, including operations satisfied by native text.
Direct reader callers acquire it before queueing;
physical encoded/raster batches reserve weighted work without acquiring another
logical request. A one-request worker can therefore process a document without
rejecting its own nested OCR call. Expanded work saturation remains a retryable
503 with the admission reason and Retry-After.

Configure request concurrency separately from weighted work capacity. The CPU
profile uses one logical request, 64 work units and a 4-million-pixel request
ceiling. Omitting work capacity makes it follow concurrency and can reject a
normal full-page raster before model execution.

Generative preparation admits manifest reading, prompt rendering/tokenization
and silence detection before their allocations, independently of the caller's
document quota. Failed preparation preserves the admission error identity and
refunds allocations. Generative execution then uses a reclaiming, thread-safe
heap under its model generation resource lease. The caller allocator owns returned text, not
model tensors. Encoded document readers follow the same boundary: execution
retains its own owned buffers, then copies only the compact result into the
caller's bounded output allocator. This prevents a document's 512 MiB
source/metadata/output quota from rejecting legal vision or decoder workspace,
and avoids charging the execution lease again through the document allocator.
Output-copy allocation failures roll back every completed result item.

Encoded-reader preparation has its own process-admitted allocation owner,
including manifest and tokenizer metadata, independent of document quotas.
Session-backed readers borrow an execution view using the execution heap; their
loaded metadata and teardown remain with the original reader. Multi-stage
execution applies that allocator to nested recognizers as well. Moondream has
no managed session execution lease, so its actual allocations remain admitted
throughout reading, and a separate weight lease covers its mapped checkpoint
until model teardown. Generative readers use the resident model's bound manifest
rather than reopening and rescanning tokenizer metadata before generation.
Warm direct generation also clones the publication-checked cached manifest;
cold requests use admitted manifest discovery. LightOn cohort and continuous
workers each admit prompt planning before the execution lease, returning that
temporary credit before decoder allocation. Moondream's configuration probe
propagates allocation errors instead of misclassifying contention as unsupported
model artifacts.

Image preparation admission does not depend on a GPU execution lane. Ordinary
vision readers share the bounded codec/resampling path, with separate ownership
for the final pixel tensor and codec scratch. Flattened-patch readers admit
source decoding and patch preparation. Multi-stage decoding, regional crops,
detection/layout resizing and CTC preprocessing use the existing allocation-based
preparation scope, retaining only live pixels when execution starts. Each phase
returns unused capacity before acquiring model execution resources.

Codec scratch is allocated lazily: solid-color, nearest and bilinear paths
reserve no slab when they need no scratch. Sizing forecasts are advisory; denied
headroom falls back to actual allocation demand. Capacity is reused across batch
waves, with growth bounded by the aggregate physical cap. Speculative admission
failures are discarded after a successful fallback. Scratch admission refusals
also narrow a concurrent batch after every worker joins. Each retry measures its
own demand; a rejected wider wave cannot poison a serial retry. Backing-allocation
OOM stays terminal. Admission failure precedes the backing allocation and retains its policy/pressure identity.

Prepared model inputs borrow the allocation owner's proven payload range and
stack-owned shape for the synchronous invocation. They do not clone pixels,
patches, masks or stage inputs onto an unadmitted execution heap. Storage-domain
provenance supplies the existing session input-residency credit; execution owns
only the remaining model workspace and output lease. Output admission retires
execution peaks after a forward, independently of the preparation lifetime.

Synchronous child preparation scopes include live ancestors in feasibility
without charging their leases again. Source decoding, regional crops, resized
pixels and codec slab growth therefore cannot hide an intrinsically oversized
request behind several individually valid leases. Slab growth includes both
prior slab capacity and prepared pixels. A stable aggregate policy failure stays
permanent; reservations held by an unrelated request remain retryable contention.
Node-owned metadata and prompt-planning allocation growth follows the same rule:
its retained capacity participates in feasibility, while only the delta is charged.

Multi-stage post-processing borrows the live detector heatmap plane. Masks,
connected-component maps, layout/order scratch, and accumulated region metadata
use an admitted document scope. Model output leases participate in feasibility
without becoming free allocator credit; shared output owners are counted once.
Resized inputs retire before post-processing, scratch slack retires at stage
boundaries, and only compact results are copied to the caller's output allocator.

Boolean broker cancellation callbacks retain their first typed checkpoint
failure atomically until the synchronous caller joins. The retained checkpoint
control propagates through nested LightOn vision, prefill and page brokers as
well as direct encoded-reader dispatch; boolean adapters cannot bypass capture.
The encoded-reader boundary restores that error instead of reporting generic cancellation for an
expired deadline or a one-shot checkpoint failure. Successful completion also
checks this retained failure before copying results to the caller.

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
Provide an already installed model root and a JSON list of cases. Each case must
name its exact `checkpoint_dir` inside that root. The tool builds a private resolver
inventory from hard links to its visible model files, including weights, tokenizer,
configuration and preprocessing assets. It runs only that inventory and verifies
all hashes again after execution. Put the report on the same filesystem as the
checkpoint so no model-byte copy is needed. Private inventory links are removed
after worker teardown; original files are never modified. `revision` is a declared
origin label, not a verified Hub revision: `installation_sha256` and the complete
asset manifest identify the bytes actually supplied to execution.

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
and page rendering with the selected reader. Document qualification requires a
nonzero measured model-call count; native PDF text alone does not qualify OCR.
For selected pages set `"pages": [2]` in the case; the tool places that
selection in the document source. Keep the
whole-document fixture small or provide a prepared one-page PDF; a token-limited
prefix does not count as a complete read. Use a separately rendered page to
isolate OCR throughput from native document orchestration.

The HTTP timeout header narrows the document model's declared deadline; it does
not increase it. For a slow CPU PDF check, use an existing named document model
with an explicit bounded `budget.deadline_ms` and `read.native_text: "always_ocr"`.
Set the case's `model` to that name, `preload_model` to its underlying OCR model,
and `model_definition` to the local definition JSON. The isolated inventory and
asset identity include that definition; its bindings must use only the pinned
local leaf. This exercises forced OCR rather
than accidentally passing on a PDF's existing text layer. Require both header
and footer text, complete document/item status and untruncated output.

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
supervisor and workers. A sampled abort-limit breach is terminal even if it
races the final successful response. Missing PSS measurements are reported as
unknown rather than zero. It records runtime/config/fixture/installation/OpenBLAS
hashes, affinity and host load. Preserve server logs for backend selection,
resolved memory policy and admission diagnostics. Set
`TERMITE_SERVER_GENERATE_TIMING=1` for serial reader resolution, loading,
setup and generation timings (including vision, prefill and decode). Failed
generations emit `generate_failure` with the error identity, active phase,
elapsed phase timings and last observed decoded-token count. Formatting uses
bounded storage and excludes prompt/output contents. For a
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

### Spark measurement receipt

Runtime sources at `7747043d74395f9f8f0ae91ad2818a86878237f7` were verified
by per-file SHA-256 against the built source copy. The CPU-only Arm64 GNU
executable hash was
`0df7344d4f1f7346bf9d6261e6a5612ac6fe0c29989775f443c5344fb5f788ad`.
Four threads were pinned to performance cores 5–8, with a 32 GiB process limit,
4 GiB explicit scratch, one logical request and prompt caching disabled.
OpenBLAS was Ubuntu LP64 pthread 0.3.26 (`armv8sve`), hash
`4f04c974ceb53b9bad6b6dc5d606881450e3a8835837ad07c4636d10c8acfe81`.
The host had a concurrent GPU training workload; additional CPU qualification
also overlapped. Page cache was uncontrolled. These are functional fixture
measurements, not isolated cold-storage benchmarks or corpus throughput.

| Model | Startup through first complete read | Two warm reads | Peak PSS |
| --- | ---: | ---: | ---: |
| LightOnOCR-3-4B | 58.88 s | 38.83–41.02 s | 9.90 GiB |
| LightOnOCR-3-0.8B | 13.67 s | 8.86–9.09 s | 2.05 GiB |
| Whisper-small | 6.38 s | 4.51–4.80 s | 1.08 GiB |
| Whisper-large-v3 | 38.41 s | 26.82–27.26 s | 6.80 GiB |

The OCR fixture was 760×160, expected `ANTFLY TEST 123`; speech was 2.496625 s,
expected `The quick brown fox jumps over the lazy dog.` Every model also
returned the expected 504 deadline identity and completed a subsequent recovery
read. The original 4B baseline had no result within 600 s; this receipt verifies
complete CPU execution but does not establish practical full-page fallback.
A separate 37.58 s, 98-word recording crossed Whisper's window boundary and
retained its final phrase and timestamp (37.29 s), with zero normalized word
errors. That longer recording used the earlier namespace-fix binary, identical
Whisper implementation, and four efficiency cores while compilation overlapped;
its 172.20 s startup is a functional check, not a matched performance result.
The final executable repeated that long recording successfully in 171.43 s with
automatic scratch (6 GiB), preserving the same final phrase and timestamp. A
Whisper-small read with BLAS disabled also completed correctly (22.10 s on four
efficiency cores with concurrent CPU checks).

The dense Grepstein call-detail page (original PDF page 2, rendered 1051×1400)
did **not** complete within a 1200 s client deadline, both as forced document
OCR and as an already rendered image. The respective peak PSS values were
2.21 and 2.34 GiB; the workers used approximately 3.7–3.8 CPU cores. Neither
attempt reached a complete output, so neither passes correctness or throughput
qualification. The document attempt recorded about 408 million minor faults
and 9203 major faults. CPU activity and allocation/page churn require profiling;
these observations do not distinguish slow vision/prefill/decode from repetitive
generation. The previous nested-slot and document-output-quota failures no
longer prevented execution, but this is not a qualified dense-page CPU fallback.
The probe now allows a bounded 30 s response grace after the unchanged execution
deadline so subsequent runs can retain the server's timeout identity rather than
race it with the client socket timeout. No host cache flush or GPU workload
interruption was performed.

The same full raster with LightOn 4B, eight performance cores, automatic scratch
and a 600 s execution budget returned structured retryable HTTP 504
`INFERENCE_TIMEOUT` (peak PSS 9.49 GiB). The response grace preserved that error
identity and the worker exited cleanly. This also failed full-page qualification;
the successful 4B tiny fixture must not be used to advertise dense-page CPU
throughput. Raw receipts and the exact executable were retained locally; no
Grepstein document contents are checked into the repository.

### Follow-up validation receipt

The admission, preparation and scratch-reuse fixes at
`e06fc8ccbdc560e3c623e21045316003300dda1e` repeated all four CPU smoke cases on
Spark. Sources were verified per file against the built source copy; executable
SHA-256 was `ba29b905448328e5f39b55c56b4ef4765a1f331bc2630636a19321ec398fa36f`.
The same OpenBLAS library and four performance cores 5–8 were used, with a
32 GiB process limit, automatic 6 GiB scratch, one logical request and prompt
caching disabled. This was a shared host with uncontrolled page cache.

| Model | Startup through first complete read | Warm read | Sampled peak PSS |
| --- | ---: | ---: | ---: |
| LightOnOCR-3-4B | 60.138 s | 44.636 s | 9.721 GiB |
| LightOnOCR-3-0.8B | 12.130 s | 7.819 s | 2.154 GiB |
| Whisper-small | 8.419 s | 6.454 s | 1.077 GiB |
| Whisper-large-v3 | 46.131 s | 36.418 s | 6.796 GiB |

These use the tiny image and short speech fixtures described above. Every model
passed known-answer checks, deadline cancellation and a subsequent recovery
read; each pinned installation's asset manifest remained unchanged after
execution. These results do not establish a speedup or full-page throughput.
The raw report is retained as `review-e06-smoke/report.json` alongside its
source manifest and executable.

The subsequent session-binding correction at `e342ea10e6` was verified locally:
the real Node dispatcher regression covers distinct encoder/decoder sessions,
fused execution and KV output lifetime under a tight scratch cap. The focused
suite passed 343 tests, with 16 skipped. Nine qualification-tool tests and nine
BLAS-disabled compute tests passed; the production inference host ABI test
also passed on the core-fix source. No GPU execution is claimed by these checks.

A separate exploratory 90 s run of the dense 1051×1400 page used the earlier
core-fix snapshot, rather than the executable above. It returned the correct
`INFERENCE_TIMEOUT` identity and logged 38.192 s of multimodal preparation,
18.320 s of prefill and 33.850 s of decode, with 65 tokens observed before the
deadline. This validates the failure diagnostics but is not complete-page
qualification. Dense-page CPU fallback remains unqualified.

## Encoded-reader ownership and concurrency

Loader metadata, decoded images and retained preprocessing allocations form one
request ancestry for admission feasibility. Their bytes remain charged exactly
once; carrying ancestry into each model forward distinguishes an intrinsically
oversized request from contention with other requests. Different admission
domains never lend capacity to each other.

CTC forwards retain an opaque execution reservation until resident outputs and
backend buffers are destroyed. Post-processing has a separate admitted owner:
resident logits may belong to an imported backend's persistent allocator, so
forward capacity is never loaned to text decoding. Additional host allocations
include the full live host/device execution residency in feasibility. Exported
logits borrow their tensor view instead of making another copy.

Completed multistage OCR transfers its allocation owner and text to the reader.
The reader builds its final region array once, retaining text and label pointers.
Result cleanup releases storage and leases together, independently of model
lifetime. The final wrapper severs synchronous loader ancestry after conversion
and UTF-8 sanitation; subsequent result allocations use a frozen host policy.

Ordinary encoder/decoder readers admit decoder masks, token buffers, and output
text, borrowing prepared tensors instead of cloning them every token. Result
parsing transfers the output owner and retains typed allocation failures.
Prompt planning includes pinned model residency, and Moondream's mapped-weight
lease participates in preparation feasibility until reader teardown. Ordering
fallback applies only to explicit unsupported-operation errors; arbitrary
checkpoint failures propagate through direct pipeline APIs too.

Allocation owners enforce a single reservation domain and cache their retained
host total. Regional result transfers and ancestor queries do not repeatedly
walk all earlier leases; trimming stops after available slack is exhausted.

Preprocessing remembers a geometry-weighted concurrency ceiling after a failed
wave. Equal-size pages retain the proven width instead of repeatedly doubling
and replaying work. Smaller pages can recover parallelism within the learned
capacity. This is a request-local scheduling hint: actual allocation admission
remains authoritative and cancellation is checked between attempts.

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
