# Qwen3-VL inference support

This document is the operational contract for Qwen3-VL generation and
Qwen3-VL-Reranker inference in Antfly. The implementation is deliberately
fail-closed: code and artifact recognition may land before a model/backend
pair is enabled, but an unqualified bundle cannot be made runnable with
`--allow-unknown-models`.

The production promotion is intentionally narrow. Only these managed receipt
identities are runnable, and only through Metal:

| Serving role | Exact managed source | Quantization | Status |
| --- | --- | --- | --- |
| Generation | `Qwen/Qwen3-VL-2B-Instruct-GGUF:q4-k-m-bundle-v1` | Q4_K_M decoder + Q8_0 projector | Promoted |
| Reranking | `Qwen/Qwen3-VL-Reranker-2B-GGUF:q8-0-q8-0-bundle-v1` | Q8_0 decoder + Q8_0 projector + F16 score head | Promoted |

The 4B and 8B generation bundles, BF16 reranker oracle, Q4 reranker, other
quantizations, unmanaged copies, changed receipts, and non-Metal backends
remain blocked. This is an exact-artifact promotion, not family-wide enablement.

## Pinned artifacts

`antfly inference pull` recognizes the following source aliases. Each alias
resolves to immutable Hub revisions, verifies exact sizes and SHA-256 digests,
stages the complete bundle beside the live model directory, and publishes it
with a single managed receipt only after every artifact is valid. A pull alias
is not, by itself, a serving promotion.

| Alias | Decoder or model | Vision projector | Approximate installed size |
| --- | --- | --- | ---: |
| `qwen3-vl-2b` | Official `Qwen3VL-2B-Instruct-Q4_K_M.gguf` | Official Q8_0 mmproj | 1.46 GiB |
| `qwen3-vl-4b` | Official `Qwen3VL-4B-Instruct-Q4_K_M.gguf` | Official Q8_0 mmproj | 2.76 GiB |
| `qwen3-vl-8b` | Official `Qwen3VL-8B-Instruct-Q4_K_M.gguf` | Official Q8_0 mmproj | 5.39 GiB |
| `qwen3-vl-reranker-2b` | Official BF16 `model.safetensors` conversion/parity oracle; not serveable | Embedded in the checkpoint | 3.97 GiB |

The reranker serving artifact is produced from that pinned BF16 source with
`convert_qwen3vl_reranker.py`: Q8_0 decoder, Q8_0 projector, and an F16
two-row semantic classifier head (approximately 2.1 GiB installed). The
converter pins llama.cpp tool hashes, performs two independent conversions,
compares every output byte, inspects all live GGUF tensor contracts, and only
then publishes a version-2 managed receipt. Q4_K_M remains available through
`convert_qwen3vl_reranker_q4.py` as an explicitly ranking-only profile; its
calibrated scores are not production-qualified.

Qwen does not currently publish that exact Q8 serving bundle. Production
deployment must therefore pre-provision the complete converted directory and
its version-2 completion receipt under the model root. Discovery advertises
the receipt identity
`Qwen/Qwen3-VL-Reranker-2B-GGUF:q8-0-q8-0-bundle-v1`, independent of the
directory leaf name. Do not point the BF16 alias or an arbitrary community
GGUF at the production route. Once the exact bundle is published to an
Antfly-controlled immutable repository, a separate change may add a Q8 pull
alias without changing its promotion identity.

The generation aliases use Q4_K_M for the decoder and Q8_0 for the projector.
The projector is a small, accuracy-sensitive component relative to the
decoder, so Q8_0 is the production default. Qwen currently publishes the
reranker checkpoint as BF16 safetensors rather than an official GGUF. Antfly
therefore builds its own deterministic artifact instead of trusting an
unpinned community conversion.

Examples:

```sh
antfly inference pull qwen3-vl-2b
antfly inference pull qwen3-vl-reranker-2b  # BF16 conversion/parity oracle
```

The source catalog, including revisions, exact filenames, sizes, and SHA-256
digests, lives in `src/registry/qwen3vl_catalog.zig`.

The initial real-model lane uses the pinned 2B bundle: decoder SHA-256
`089d75c52f4b7ffc56ba998ffc50aae89fcafc755f9e7208aacca281dca6c2ae`
and Q8_0 projector SHA-256
`f9a68fabba69c3b81e153367b2c7521030b0fa8bb0de400c9599c8e6725f9c82`.
The same-model Transformers oracle uses the official BF16 safetensor SHA-256
`7de1838c87a5349b016c26a1c3f7d2bc400a3d485f95ef39a7059ffd734977a0`.
The reranker lane is pinned to `Qwen/Qwen3-VL-Reranker-2B` revision
`4bd860ac4f15ad1897a214615cccc700f8f71818`; its BF16 model SHA-256 is
`466ec01961061e9d7f804b4fb1625fb6f406106cd1567e026096d4736fa9d5b9`
and the complete managed receipt SHA-256 is
`afa3228dc98b50f2d21f72ccaa80910ae19433d8c994611adb69bd035d2983af`.

## Runtime architecture

The implementation reuses the hardened decoder/session, weight-store,
admission, tokenizer, image-decode, registry, and Metal execution layers. The
Qwen-specific code is limited to architecture contracts that cannot safely be
inferred from Gemma:

1. The request planner expands every image marker, produces temporal/height/
   width M-RoPE positions, records the visual-token mask, and caches the
   incremental-decode position delta.
2. The projector validates the official `qwen3vl_merger` GGUF header and exact
   tensor shapes before allocating image work. It performs merge-aligned
   resizing, temporal patch fusion, learned positional interpolation, vision
   RoPE, all vision transformer blocks, the main merger, and the three
   DeepStack taps.
3. The shared GPT decoder uses Qwen q/k normalization and M-RoPE. DeepStack
   features are injected after the first decoder layers and only into rows
   identified by the visual mask. Single-sequence text generation can use the
   mathematically equivalent scalar position lane; batched reranking supplies
   explicit axis-major positions derived from its validated padding mask.
4. Prefill consumes one immutable prepared-prompt object containing expanded
   IDs, embeddings, M-RoPE positions, visual mask, DeepStack taps, and cached
   position delta. Incremental decode derives all three next-token positions
   from that delta.
5. The reranker has a separate generative scoring mode. It renders the pinned
   Qwen chat template, preserves the five-token assistant suffix under strict
   bounded truncation, requires right padding, selects the final active hidden
   row, and computes only `(W_yes - W_no) dot hidden` on the backend. It never
   falls back to CLS scoring or late interaction.

Video is rejected rather than treated as an image sequence. Typed image
content is parsed by the reranking HTTP contract and routed through the same
projector/decoder planner as the offline qualification path. The public routes
accept only the two exact promoted receipts above; the parser alone does not
enable a model.

## Safety and admission

- Bundle completeness and family matching are checked before session creation.
- Projector metadata, split temporal patch weights, every transformer block,
  mergers, and DeepStack layers have exact shape contracts.
- Image count (eight per request), encoded bytes, decoded pixels, aspect ratio,
  merged visual tokens, prompt bytes, sequence length, batch size, and host
  preprocessing have finite limits and checked arithmetic.
- Model-run admission happens before tokenizer/projector allocations. A failed
  admission returns a resource error without attempting opportunistic work.
- Mismatched image markers, missing images, videos, missing M-RoPE, malformed
  masks, non-finite tensors/scores, invalid token IDs, and partial bundles are
  hard errors.
- Stateful backend execution continues to use the model manager's per-target
  execution gate. Managed downloads retain resumable private staging state but
  never expose a partial model through discovery.

## Local qualification workflow

`scripts/qwen3vl/requirements-qwen3vl-oracle.txt` freezes the independent
oracle runtime. `transformers_oracle.py` runs without model weights and records
the official processor, tokenizer, placeholder expansion, resized geometry,
spatial patches, complete axis-major M-RoPE positions, delta, and DeepStack
contract. `transformers_weights_oracle.py` optionally loads the pinned
checkpoint and emits the complete final-row logit vector. BF16/eager CPU
remains the canonical reference. The oracle also exposes explicit dtype,
attention, load, logit-copy, warmup, and timed-run controls for the MPS
comparator; it rejects non-finite or implausibly large output rather than
accepting it as a reference.

`qualify_qwen3vl_metal.py` verifies every file in the version-2 managed receipt,
records binary and Git provenance, runs the frozen oracle offline, and launches
each real Metal request in its own process group. The watchdog defaults to 60
seconds, 4 GiB RSS, at least 10 percent free system memory, and zero swapout
growth. It requires two identical Metal processes by default and compares
bitwise hashes at the positioned patch embedding, main projector, each
DeepStack tap, full final prefill logits, and generated token boundaries.

Example from the inference package directory:

```sh
env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv run --offline --isolated \
  --with-requirements scripts/qwen3vl/requirements-qwen3vl-oracle.txt \
  python scripts/qwen3vl/qualify_qwen3vl_metal.py \
  --model-dir /path/to/managed/qwen3-vl-2b \
  --weights-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --image /path/to/fixture.jpg \
  --antfly-bin zig-out/bin/antfly-inference \
  --output /private/tmp/qwen3vl-qualification.json
```

`benchmark_qwen3vl_transformers_mps.py` is the standalone MPS performance
lane. Its production-like default is FP16/SDPA with direct device-map loading,
an on-device clone of the final logit row before the CPU copy, one warmup, and
three timed resident forwards. CPU fallback and fast math are disabled. The
MPS allocator is hard-capped at 80 percent of Metal's recommended working set,
with a 70 percent adaptive-commit watermark; the outer watchdog separately
enforces RSS, free-memory, swap-growth, and timeout limits. Supplying the frozen
CPU JSON and logit vector binds the exact model, image, prompt, input IDs, grid,
and reference digest and turns numerical parity into a hard benchmark gate.

```sh
env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv run --offline --isolated \
  --with-requirements scripts/qwen3vl/requirements-qwen3vl-oracle.txt \
  python scripts/qwen3vl/benchmark_qwen3vl_transformers_mps.py \
  --weights-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --processor-dir /path/to/managed/qwen3-vl-2b \
  --image /path/to/fixture.jpg \
  --reference-json /path/to/cpu-oracle.json \
  --reference-logits /path/to/cpu-oracle-logits.f32le \
  --output /private/tmp/qwen3vl-mps-benchmark.json
```

Run `zig build test-qwen3vl-python-contracts` to verify the oracle,
qualification, and MPS benchmark contracts without loading a model.

### Full OCR generation comparison

`benchmark_qwen3vl_transformers_ocr.py` is the separate full-request Torch
lane. Unlike the final-logit MPS benchmark above, it times a resident
`model.generate` call that includes image preprocessing, vision encoding,
prefill, and greedy decode. It records the rendered request digest, merged
visual-token count, every timed generated-token sequence, decoded OCR text,
MPS allocator snapshots, and the resource watchdog result. It is not a
replacement for the native timing breakdown: compare native `generate` and
`decode_inner` with the Torch runner's full-request median and effective
end-to-end tokens/sec, and do not label either framework's internal timing boundary as
serving throughput.

Use the exact pinned BF16 checkpoint from the preceding oracle contract; the
runner verifies its size, SHA-256, and processor sidecars before loading. A
different SafeTensors conversion must not be substituted for a cross-backend
quality or performance claim.

```sh
PROMPT='Transcribe all visible text exactly. Preserve the original reading order, line breaks, punctuation, and accents. Return only the transcription.'

env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv run --offline --isolated \
  --with-requirements scripts/qwen3vl/requirements-qwen3vl-oracle.txt \
  python scripts/qwen3vl/benchmark_qwen3vl_transformers_ocr.py \
  --weights-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --processor-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --image /path/to/ocr-fixture.jpg --prompt "$PROMPT" \
  --device cpu --warmup-runs 1 --timed-runs 3 --max-tokens 64 \
  --output /private/tmp/qwen3vl-transformers-cpu-ocr.json

env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv run --offline --isolated \
  --with-requirements scripts/qwen3vl/requirements-qwen3vl-oracle.txt \
  python scripts/qwen3vl/benchmark_qwen3vl_transformers_ocr.py \
  --weights-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --processor-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --image /path/to/ocr-fixture.jpg --prompt "$PROMPT" \
  --device mps --warmup-runs 1 --timed-runs 3 --max-tokens 64 \
  --output /private/tmp/qwen3vl-transformers-mps-ocr.json
```

The MPS lane disables CPU fallback and fast math, and defaults to the 80/70
percent MPS allocator watermarks. Both lanes fail closed on timeout, RSS,
free-memory, or swap-growth violations. Run them serially on this 16 GB host;
if the BF16 MPS lane cannot satisfy the zero-swap envelope, preserve that
failure report instead of publishing a performance number.

### High-precision and MLX-VLM benchmark lanes

The production Qwen3-VL generation receipt remains the qualified Q4_K_M
decoder plus Q8_0 projector bundle.  The native runtime does not accept
official SafeTensors directly: `convert_qwen3vl_high_precision.py` is an
explicit, reproducible BF16 SafeTensors-to-split-GGUF bridge for benchmarking.
It verifies the pinned official source hash and required processor sidecars,
converts decoder and projector twice with a pinned converter, requires matching
digests, validates their Qwen3-VL tensor contracts, and emits the separate
`bf16-reference-bundle-v1` receipt. That identity is benchmark-only and remains
blocked by the serving compatibility policy.

```sh
env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv run --offline --isolated \
  --with-requirements scripts/qwen3vl/requirements-qwen3vl-conversion.txt \
  python scripts/qwen3vl/convert_qwen3vl_high_precision.py \
  --source-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --output-dir /path/to/qwen3-vl-2b-bf16-reference-gguf \
  --converter /path/to/llama.cpp/convert_hf_to_gguf.py \
  --report /private/tmp/qwen3vl-bf16-conversion.json
```

`benchmark_qwen3vl_native_precision.py` first compares that BF16 native Metal
reference bundle with the pinned Transformers MPS BF16/SDPA run, including
full final-logit quality gates. It then measures the already-qualified Q4/Q8
bundle in a separate profile against the same MPS reference. Every native
sample is a fresh process and excludes model load from `timing_ms.generate`;
the report preserves the corresponding MPS resident timing boundary instead of
calling the result serving throughput.

```sh
env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv run --offline --isolated \
  --with-requirements scripts/qwen3vl/requirements-qwen3vl-oracle.txt \
  python scripts/qwen3vl/benchmark_qwen3vl_native_precision.py \
  --high-precision-model-dir /path/to/qwen3-vl-2b-bf16-reference-gguf \
  --q4-model-dir /path/to/managed/qwen3-vl-2b-q4-q8 \
  --weights-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --processor-dir /path/to/pinned/qwen3-vl-2b-bf16-oracle \
  --image /path/to/fixture.jpg \
  --antfly-bin zig-out/bin/antfly-inference \
  --output /private/tmp/qwen3vl-native-precision.json
```

MLX-VLM is an independent Apple-Silicon benchmark oracle, not an Antfly
backend. Install the pinned MLX environment in its own virtual environment;
Qwen's chat template requires the explicitly pinned `Jinja2` runtime as well
as `mlx-vlm`. Run `benchmark_qwen3vl_mlx_vlm.py` twice there: once with a local
Qwen3-VL BF16 MLX conversion (`--profile bf16`) and once with the matching Q4
MLX conversion (`--profile q4`). The runner hashes every local model artifact,
records package versions and formatted prompt/image/token evidence, validates
an unquantized BF16 config or explicit four-bit MLX quantization metadata, and
deliberately does not claim logit parity from MLX-VLM's public streaming API.
The native Q4 profile is a Q4_K_M decoder with a Q8_0 projector; a four-bit
MLX conversion is a Q4 precision-class comparison, not a byte-identical
quantizer/projector equivalence.

```sh
env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv venv /private/tmp/antfly-qwen3vl-mlx-vlm --python 3.12
env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv pip install --python /private/tmp/antfly-qwen3vl-mlx-vlm/bin/python \
  -r scripts/qwen3vl/requirements-qwen3vl-mlx-vlm.txt

/private/tmp/antfly-qwen3vl-mlx-vlm/bin/python \
  scripts/qwen3vl/benchmark_qwen3vl_mlx_vlm.py \
  --model-dir /path/to/local/qwen3-vl-2b-mlx-bf16 \
  --profile bf16 --image /path/to/fixture.jpg --max-pixels 589824 \
  --output /private/tmp/qwen3vl-mlx-bf16.json

/private/tmp/antfly-qwen3vl-mlx-vlm/bin/python \
  scripts/qwen3vl/benchmark_qwen3vl_mlx_vlm.py \
  --model-dir /path/to/local/qwen3-vl-2b-mlx-q4 \
  --profile q4 --image /path/to/fixture.jpg --max-pixels 589824 \
  --output /private/tmp/qwen3vl-mlx-q4.json

python scripts/qwen3vl/compare_qwen3vl_precision_backends.py \
  --native-report /private/tmp/qwen3vl-native-precision.json \
  --mlx-bf16-report /private/tmp/qwen3vl-mlx-bf16.json \
  --mlx-q4-report /private/tmp/qwen3vl-mlx-q4.json \
  --output /private/tmp/qwen3vl-native-mlx-comparison.json
```

The final comparator fails closed if request metadata, precision profile, or
generated token evidence differ. Its two request-latency ratios are explicitly
BF16-to-BF16 and Q4-to-Q4: native `timing_ms.generate` excludes model load,
while MLX-VLM is a warmed `stream_generate` request that includes image
preparation, prompt processing, and the one bounded decode. They never mix
profiles or imply equivalent serving throughput. Use an isolated MLX
environment because its dependency contract can differ from the frozen
Transformers oracle environment; the generated report is the authoritative
record of the actual MLX and MLX-VLM versions.

### Native multi-image input throughput

`generate` accepts repeated `--image path` flags in request order. Use
`benchmark_qwen3vl_native_image_batch.py` to compare isolated one-image
requests with a single multi-image request under the same Metal budgets and
resource gates. It emits a fail-closed, immutable report only after every run
has deterministic output tokens, expected image count, and no RSS/free-memory/
swap/timeout violation.

```sh
env TERMITE_METAL_ENABLE_Q4_K_HIGH_ROW_MM=1 \
  TERMITE_METAL_ENABLE_Q6_K_HIGH_ROW_MM=1 \
  TERMITE_METAL_ENABLE_VISION_SDPA_HD64_FLASH_Q32=1 \
  TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_SG_ATTENTION=1 \
  TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_SLOTS=1 \
  TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_FRAME=1 \
  TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_FFN=1 \
  python scripts/qwen3vl/benchmark_qwen3vl_native_image_batch.py \
  --model-dir /path/to/managed/qwen3-vl-2b-q4-q8 \
  --antfly-bin zig-out/bin/antfly-inference \
  --image /path/to/fixture.jpg \
  --batch-size 2 --runs 3 \
  --output /private/tmp/qwen3vl-image-batch.json \
  --work-dir /private/tmp/qwen3vl-image-batch.artifacts
```

With no `--batch-image`, the benchmark deliberately repeats `--image`; pass
one `--batch-image` for each input when measuring a real mixed-image request.
The one-token cap reports image-ingest scaling (vision preparation plus
prefill), not full OCR completion time. A throughput gain can come from
amortizing request overhead; it is not evidence that the vision encoder runs
the images in parallel. Measure a representative output-length OCR workload
separately before setting serving batch policy.

`--vision-trace-layer N` is a diagnostic-only option. It attests exactly one
vision block boundary, avoiding the all-layer synchronization that can hide a
buffer-lifetime defect. Ordinary serving never sets the associated environment
variable.

`transformers_reranker_oracle.py` independently validates the generic and
named official reranker templates, uses the pinned left-padding/truncation
contract, runs the BF16 model on CPU, and computes the official
`sigmoid_f32(bf16(final_hidden @ (W_yes - W_no)))` score. The native
qualification trace is caller-owned and disabled in ordinary serving; when
explicitly requested it records the rendered prompt, token IDs, raw logit, and
score without changing the production scoring path.

`qualify_qwen3vl_reranker_metal.py` independently validates both the 12-file
BF16 oracle receipt and the converted GGUF receipt, including live decoder,
projector, and classifier-head tensor catalogs. It launches two isolated real
Metal processes and requires exact prompts, active/expanded token IDs, image
token counts, visual masks, all three M-RoPE axes, ranking, and bitwise output
identity. The calibrated Q8 tier enforces 0.10 raw-logit and 0.03 score
absolute-error ceilings; Q4 can only run the separately named ranking-only
profile. RSS, free-memory, zero-swap, timeout, and forbidden-runtime-output
gates remain mandatory.

Example from the inference package directory:

```sh
env UV_CACHE_DIR=/private/tmp/antfly-uv-cache \
  uv run --offline --isolated \
  --with-requirements scripts/qwen3vl/requirements-qwen3vl-conversion.txt \
  python scripts/qwen3vl/qualify_qwen3vl_reranker_metal.py \
  --model-dir /path/to/managed/qwen3-vl-reranker-2b-q8 \
  --oracle-model-dir /path/to/managed/qwen3-vl-reranker-2b-bf16 \
  --image /path/to/fixture.jpg \
  --document 'A document describing the image.' \
  --antfly-bin zig-out/bin/antfly-inference \
  --output /private/tmp/qwen3vl-reranker-qualification.json
```

## Release qualification gates

Every promotion must remain per exact managed receipt, Metal device family,
and runtime build; success for 2B does not qualify 4B or 8B. Compatibility is
implemented as an allowlist over the immutable receipt and live GGUF geometry,
then restricted to Metal by backend selection. Changing an artifact path,
size, SHA-256, source identity, quantization, model geometry, serving role, or
backend fails closed.

At model load, promotion re-resolves each receipted artifact to its canonical
regular file and streams its SHA-256 against the pinned catalog identity. A
receipt declaration alone can therefore never promote a post-download,
same-size artifact substitution. Generated bundle metadata is covered by the
same live content gate for backward-compatible managed receipts that predate
generated-artifact digest fields.

### Machine-enforced promotion ledger

`scripts/qwen3vl/validate_qwen3vl_promotion.py` remains the family-wide release
gate for expanding beyond the exact 2B allowlist. It requires 42 unique
evidence lanes: ten for each 2B/4B/8B generation bundle and twelve for the 2B
reranker. Every report and its raw artifacts must be regular files beneath the
campaign directory and must match their manifest SHA-256 values. All lanes
must name one clean Git revision, one binary digest, one Metal device family,
and the exact managed receipt for their model. A missing lane, dirty build,
failed check, nonzero swap growth, resource violation, mismatched artifact, or
duplicate lane fails closed.

The generation matrix covers artifact loading, Transformers parity, a real
two-megapixel image, multi-image input, a 256-token decode, four-way
concurrency, cancellation, cache lifecycle, paired performance, and a minimum
one-hour mixed soak. The reranker matrix covers text and typed multimodal
parity, reproducible calibrated-Q8 conversion, frozen-corpus retrieval and
calibration quality, the HTTP API, and an 8K-active-token truncation lane in
addition to the operational lanes. Performance evidence requires at least five
paired trials with identical outputs and no more than a five-percent regression
at the declared timing boundary.

Run the final validator from the inference package directory:

```sh
python3 scripts/qwen3vl/validate_qwen3vl_promotion.py \
  --manifest /path/to/campaign/promotion-manifest.json \
  --output /path/to/campaign/promotion-report.json
```

The manifest schema is `antfly.qwen3vl.promotion_manifest.v1`; each scenario
report uses `antfly.qwen3vl.production_evidence.v1`. The normal CI lane runs
the validator's synthetic fail-closed contract tests, while the multi-gigabyte
campaign remains a gated Apple-Silicon hardware job. A passing aggregate
report authorizes a separate, receipt-specific compatibility change; it never
broadens unknown-model or architecture-family policy itself.

### Artifact and loader

- Pull from an empty cache and from an interrupted `.part` download; verify
  resume, digest failure cleanup, atomic publication, and receipt identity.
- Inspect the complete decoder/projector or safetensors checkpoint, not only a
  prefix. Record architecture metadata, tensor count/types/shapes, tokenizer
  special IDs, projector contract, and model residency estimate.
- Prove corrupt, swapped-size, stale-sidecar, wrong-family, missing-projector,
  and decoder-only bundles fail before readiness.

### Numerical parity

- Compare tokenizer IDs and rendered templates against the pinned Qwen
  processor for empty, Unicode, multilingual, tool-like, long, and multi-image
  prompts.
- Compare resize geometry, normalized pixels, temporal patch fusion, learned
  positions, vision RoPE, representative block outputs, main merger, and every
  DeepStack tap against Transformers in FP32 oracle mode.
- Compare full prefill hidden states and logits, greedy token IDs, incremental
  M-RoPE positions, cached delta, KV reuse, and generated text for text-only,
  one-image, multi-image, portrait, landscape, and admission-boundary cases.
- For reranking, compare exact prompt IDs, final active hidden row, raw
  `yes - no` logit, sigmoid score, and candidate ordering. Include strict
  truncation boundaries and score ties. Multimodal score parity is mandatory
  before the HTTP API advertises image reranking.
- Establish written absolute/relative tolerances by layer and final output.
  Ranking-order and greedy-token mismatches are failures even when aggregate
  floating-point error is small.

### Reliability and operations

- Exercise concurrent generation/reranking, cancellation, client disconnect,
  deadline expiry, repeated load/unload, cache eviction, and memory-pressure
  admission. Verify no stale KV, M-RoPE delta, visual mask, or DeepStack state
  crosses requests.
- Run bounded short probes first. Record resident bytes, peak RSS, GPU working
  set, swap, first-token latency, prompt throughput, decode throughput, and
  reranker pairs/second. Keep streamed and fully resident results separate.
- Soak qualified hardware with mixed text/image sizes and malformed inputs.
  Readiness must become false on a required artifact/session failure, and
  liveness must remain independent of model readiness.
- Add receipt-specific golden vectors and CPU-versus-Metal differential tests
  to CI. Real multi-gigabyte artifacts belong in a gated hardware lane, while
  synthetic contract, shape, planner, and failure-path tests remain in normal
  CI.

### Enablement

The exact 2B promotion is represented by a small artifact-qualified
compatibility decision keyed by the complete receipt. It does not weaken
unknown-model policy or turn the Qwen family on by architecture name. Any
future promotion must extend that allowlist with independent evidence rather
than broadening the family predicate.

### Production deployment

Run generation and reranking as separate low-concurrency deployments during
the initial rollout. Configure an explicit process envelope and startup
preload. The listener is not published until every configured preload succeeds,
so a missing, incompatible, non-Metal, or unadmittable model fails startup
rather than producing a cold-request failure after readiness:

```sh
antfly inference run \
  --models-dir /var/lib/antfly/models \
  --process-memory-budget-mb 12288 \
  --max-concurrent-requests 4 \
  --preload-model 'generator:metal:Qwen/Qwen3-VL-2B-Instruct-GGUF:q4-k-m-bundle-v1'

antfly inference run \
  --models-dir /var/lib/antfly/models \
  --process-memory-budget-mb 12288 \
  --max-concurrent-requests 4 \
  --preload-model 'reranker:metal:Qwen/Qwen3-VL-Reranker-2B-GGUF:q8-0-q8-0-bundle-v1'
```

The 12 GiB process envelope is the bounded qualification configuration for a
16 GiB Apple-Silicon host, not a universal sizing rule. Capacity planning must
measure resident model, projector, KV, scratch, image-decode, concurrency, and
OS headroom on the deployment SKU. Per-bucket CLI overrides remain operator
authority; absent an override, the loader derives artifact-specific residency
floors before applying the process envelope.

Four admission units provide the 64 MiB bounded media-decode envelope needed
by a 2048x1416 image. A request of that size reserves the available media
units, rather than allowing four such image decodes to overlap. The per-model
Metal execution gate remains the backend concurrency boundary; do not reduce
the admission setting to one when the deployment must accept the qualified
2MP image lane.

Roll out with one replica or a small canary pool, inspect compatibility and
backend in `/ai/v1/models`, require `/readyz`, run one text and one representative
image probe, and then increase traffic. Keep the existing deployment rollback
or model-volume switch as the kill switch. Monitor admission denials, request
latency, active requests, model loads, Metal failures, process RSS, GPU working
set, and swap. Never compare cold-start, streamed, and fully resident timings
as if they were the same lane.

## Current status

The artifact catalog/downloader, configuration and projector validation,
request planner, Qwen projector, native and Metal M-RoPE/vision-RoPE kernels,
decoder/DeepStack integration, native generation entrypoint, and text-only
generative reranker path are implemented. Synthetic and differential unit
tests cover their strict contracts.

As of 2026-08-29, the pinned 2B landscape fixture passes exact rendered-prompt,
token expansion, 320x224 resize, `[1,14,20]` grid, 70 visual-token, complete
M-RoPE, delta, fallback-counter, resource, and greedy-token gates. Five serial
real-Metal processes produced one identical main-projector hash, one identical
three-tap DeepStack hash, one identical 151,936-value logit hash, and token
1986. Peak sampled RSS was 2.32 GiB, minimum free memory was 75 percent, and
swapout growth was zero. Against the official CPU BF16 checkpoint, Q4/Q8 kept
the same argmax with top-10 overlap 8/10, cosine similarity 0.9712, Pearson
correlation 0.9701, mean absolute logit error 0.8871, and RMSE 1.1253.

This lane also found and fixed a real stale-weight defect: the Metal dynamic
slot cache was keyed by transient projector tensor addresses, allowing allocator
reuse to execute vision layer 1 with an earlier layer's prepared weights. The
projector now retains a named request-scoped weight cache across every layer
and image, explicitly retires only those dynamic slots before freeing the
weights, and permits cleared slots to be reclaimed by later requests. The
correctness-first lifetime raises the observed 2B peak RSS from roughly 0.87
GiB to 2.32 GiB. Moving this cache to a model-scoped projector session and
eliminating redundant host mirrors is a required performance/residency follow-up,
not grounds to restore unsafe short-lived identities.

The standalone Transformers MPS comparator passes on the 16 GiB M4 Air. The
previous saturated-logit result was a host-transfer defect: copying the offset
final-row view directly from MPS returned corrupt values even though the device
tensor was valid. Cloning that row on-device before copying it to CPU restores
parity without the unsafe CPU-then-MPS duplicate-residency strategy.

As of 2026-08-30, the stage-profiled 2048x1416 photograph lane (532 visual
tokens) measures a 3.190-second FP16/SDPA Transformers MPS resident-forward
median after one warmup. Its median stages are 2.309 seconds in vision, 0.850
seconds in the decoder, 0.0065 seconds in the LM head, and 0.021 seconds
unattributed. The timed logits are bitwise deterministic and select token 1986.
The provenance-bound report is
`/private/tmp/qwen3vl-mps-fp16-sdpa-2mp-stage-profile-v1.json` with SHA-256
`e8002f3199068cca66be7a280c7bae9fa3bcc774efe36a508858d259febb9079`.

On the identical image, prompt, one-token cap, and Apple M4, the qualified
Antfly Q4_K_M decoder/Q8_0 projector profile now measures a 3.204-second
settled median from a freshly rebuilt production binary: 1.613 seconds in
vision/projector preparation and 1.590 seconds in decoder prefill. Antfly is
therefore 0.4 percent behind the matched MPS forward overall while its vision
lane is 30.1 percent faster; decoder prefill remains 1.87x slower and is the
next optimization boundary. Three settled runs selected token 1986 and
produced identical logits and image-patch hashes. Evidence lives under
`/private/tmp/qwen3vl-prepared-ffn-e2e-v2` through `v4`.

The dominant decoder shapes now use two model-neutral, opt-in matrix routes.
`TERMITE_METAL_ENABLE_Q4_K_HIGH_ROW_MM=1` accelerates the six Q4_K attention
and gate/up projections per layer, while
`TERMITE_METAL_ENABLE_Q6_K_HIGH_ROW_MM=1` accelerates the Q6_K FFN-down
projection. Force rollback with the corresponding `TERMITE_METAL_DISABLE_*`
switch. At the exact 558-row Qwen shape, one Q4_K gate/up projection
(`2048 -> 6144`) measures 10.426 ms GPU versus 33.710 ms on the register-tiled
fallback. A Q6_K attention-sized shape (`2048 -> 1024`) measures 2.864 ms versus
21.439 ms, and Q6_K FFN-down (`6144 -> 2048`) measures 17.005 ms versus
125.079 ms. The Q6 differential has maximum absolute error 1.62e-5, below the
existing 0.003 gate. Runtime telemetry reports 168 generated Q4_K dispatches
and 28 `metal.k_quant_dispatch.q6_high_row_mm_matrix` dispatches per request.

One Qwen-specific submission optimization reuses those same prepared kernels.
`TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_FFN=1` executes the existing gated FFN
residual path after the whole-block fast path declines; settled evidence shows
28 direct successes and zero direct, backend, or runtime fallbacks. Enable and
rollback are controlled independently by
`TERMITE_METAL_DISABLE_QWEN3VL_PREPARED_FFN=1`, and compact timing JSON exposes
the `metal.prepared_gated_ffn` counters. The retained path preserves logits
SHA-256 `e752efd635b73d50661bb3d7be89ee0d370f19e0b219f143b19b9276d531499c`
and patch SHA-256
`5a4f565d34dad763af53b1094f693b521d376960f8eff34785c9d948aa8f466e`.

The complete opt-in qualification profile also requires
`TERMITE_METAL_ENABLE_VISION_SDPA_HD64_FLASH_Q32=1` for the Qwen vision
attention shape and `TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_SG_ATTENTION=1` for
direct prefill K/V. Prepared FFN additionally requires both
`TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_SLOTS=1` and
`TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_FRAME=1`; setting only the prepared-FFN
flag is intentionally a no-op. Each route retains its matching `DISABLE`
rollback. Qualification and performance reports must record the complete
environment and assert 28 direct-K/V calls, 28 prepared-FFN successes, 168
generated Q4_K dispatches, 28 generated Q6_K dispatches, and zero fallbacks.

The combined prepared Q/K/V submission was also evaluated but is not enabled
for Qwen3-VL. The corrected seven-sample benchmark hashes all three outputs and
reports the same `762cc188da6970a8` hash for both routes; combined submission
measures 7.134 ms GPU versus 7.105 ms for the existing three submissions. A
shorter sample had suggested a small win, but the hard-hash rerun reduced it to
noise and slightly favored the simpler existing path.

The pinned BF16 reranker text lane also passes the frozen Transformers and
real-Metal gate. The three-document oracle logits are `[1.171875, -1.0546875,
-0.890625]`, scores are `[0.7634837, 0.2583260, 0.29098085]`, and ranking is
`[0, 2, 1]`. Two independent Metal processes produced bitwise-identical logits
`[1.2202648, -1.0643585, -0.9209601]`, scores `[0.77211016, 0.25647742,
0.28476232]`, and the same ranking. Maximum absolute logit and score errors
were 0.0484 and 0.00863 respectively; prompt and active token IDs were exact.
The CPU oracle peaked at 3.18 GiB sampled RSS, Metal processes at 222 MiB
process RSS, minimum free memory was 73 percent, and swapout growth was zero.
All 34 gates passed. The local report is
`/private/tmp/qwen3vl-reranker-text-metal-v8.json` with SHA-256
`fa6b26051c93c9223ade35091513979ece26e329628d9fb0940be9e7cf5e66ce`.

That lane exposed and fixed two semantic defects rather than relaxing the
gate. The tokenizer now recognizes Qwen's exact isolated Split regex instead
of applying GPT-2 boundaries, preserving the official `?\n` and `<Document`
BPE merges. Batched reranking now supplies explicit three-axis positions;
the scalar RoPE API cannot infer batch size independently from head count in a
flattened tensor. A single-document diagnostic already matched Transformers,
and the explicit position contract brought the full batch within tolerance.

The Pillow-compatible antialiased bicubic path now repairs the high-resolution
resize/normalization failure. On the same 2048x1416 photograph, the development
qualification reports mean absolute patch error 0.0000244, RMSE 0.000437,
p99 absolute error 1.19e-7, and maximum absolute error 0.00784 across 1,634,304
values. Two complete opt-in Metal runs pass all 28 gates, select token 1986,
and produce identical projector, DeepStack, and logit hashes. Their median
generation time is 3.242 seconds: 1.632 seconds in vision/projector preparation
and 1.609 seconds in decoder prefill. Peak sampled RSS is 491 MiB and swapout
growth is zero. The report is
`/private/tmp/qwen3vl-2mp-pillow-parity-perf-v5.json` with SHA-256
`398be4795a859bb456d484d85946fe3dca758f2f55bfba72648a901c954dbbf3`.

The reranker now has a reproducible calibrated Q8 candidate. Two independent
BF16-to-GGUF passes produced decoder SHA-256
`77d166d8dba7f157b2c770db642b70ebc32dbdc8cf2d69aebdf44b3dfea24aef`
and projector SHA-256
`62135d45fbed2dfb3d047ef7a84eb04ed97b1721267bdea7e5a6185e08c95ba0`;
the managed receipt SHA-256 is
`0518cecea978bb0a1f71429ad4fb1c23ad553bee9e8c33058f8b72d6bb89046b`.
The live decoder catalog contains 311 tensors and preserves the `[2048,2]`
semantic classifier as F16. The projector contains 316 tensors. The standalone
conversion report is `/private/tmp/qwen3vl-reranker-q8-conversion-report.json`
with SHA-256
`a3cdf935ae893d88c147eb94c329d5804a214f4c21e8eef78e291b90a160713d`.

That exact receipt passes the new small-image and 2048x1416-image calibrated
gates. The 2 MP case matches the official `[1,38,56]` grid, 532 expanded visual
tokens, prompt IDs, visual mask, and axis-major M-RoPE positions. BF16
Transformers scored `0.52220219` from logit `0.08886719`; Q8 Metal scored
`0.53861177` from logit `0.15475526`. Two isolated Metal processes were
bitwise identical, used about 378 MiB sampled RSS, and observed zero swap
growth. Their 15.7-17.0 second guarded process times compare with 116.5 seconds
for the BF16 CPU oracle at the same end-to-end boundary. The report is
`/private/tmp/qwen3vl-reranker-2mp-q8-qualified-v3.json` with SHA-256
`51c07125ede7b0f6cafdd223350f18a4fff11961c553aa6db02bd08b558c3cd3`;
it binds ReleaseSafe+Metal binary SHA-256
`799fdb1d9a6d9851f95f9b7aa5e109844aa17877ff1d33e33985f2283ee3ddae`.
This is not a Transformers MPS comparison.

As of 2026-08-30, the exact 2B generation and calibrated-Q8 reranker receipts
above are enabled for scoped Metal production. The server compatibility path
still blocks every other Qwen receipt and backend, resolves the same managed
identity advertised by discovery, derives Q8 residency floors before applying
the hard serving envelope, admits reranker work at the exact rendered sequence
length, and accounts for lazily loaded projector residency and scratch.

This scoped promotion does not claim that the 42-lane family-wide campaign is
complete. Multi-image, long-decode, expanded concurrency, cancellation,
cache-lifecycle, soak, and 4B/8B qualification remain expansion gates. The Q8
reranker bundle also needs an Antfly-controlled publication target before the
default CLI can pull the production artifact; until then it is production
deployable only as an exact pre-provisioned managed bundle. These boundaries
cannot be bypassed with the unknown-model opt-in.
