# Memorilla checkpoint and native numerical qualification

These tools export the released memory modules, generate independent PyTorch
references, and compare native CPU or CUDA execution. They implement the initial
compatibility work in the [integration plan](../../../../../docs/plans/memorilla-native-memory.md).
Database collection scanning and Gemma memory training are subsequent stages.

The source contract is Memorilla commit
`d8f18891a50bf33c18f04fe5167c22f51e58c9cd`, release revision
`f6b15af56aa687861c8d2d03b1cd8f7979881361`. All five released modules have
194,123,776 BF16 parameters. Conversion preserves every tensor bit, bias, and
normalization parameter, and splits packed attention projections into explicit
`q_proj`, `k_proj`, and `v_proj` tensors. Native serving needs SafeTensors and
JSON only.

The exact frozen pair and every required upstream artifact are pinned in
[base_models.json](base_models.json). The Qwen3-Embedding-4B memory recipe uses
last-token pooling, L2 normalization, no question/document instruction prefix,
and trailing token 151643. Ordinary retrieval recipes are different artifacts.

## Reference environment

Run commands from the repository worktree root. Keep the environment, downloaded
models, and generated evidence inside that worktree:

```sh
UV_CACHE_DIR=.zig-cache/memorilla-uv uv venv --python python3 .venv
UV_CACHE_DIR=.zig-cache/memorilla-uv uv pip install --python .venv/bin/python \
  --index-url https://download.pytorch.org/whl/cpu torch==2.7.0
UV_CACHE_DIR=.zig-cache/memorilla-uv uv pip install --python .venv/bin/python \
  -r zig/pkg/inference/scripts/memorilla/requirements-reference.txt
```

The CPU environment deliberately matches upstream Torch and Transformers versions.
No remote model code executes. Remove the isolated uv cache after installation
and validation; retain the worktree-local environment for reproducible runs.

## Export released modules

Acquisition is explicit and checks immutable Git/LFS identities. Conversion and
verification are offline; existing output bundles are never overwritten.

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/exporter.py acquire \
  --output-root .benchmark-assets/memorilla/checkpoints --checkpoint stage3
.venv/bin/python zig/pkg/inference/scripts/memorilla/exporter.py export \
  --checkpoint-dir .benchmark-assets/memorilla/checkpoints/stage3 \
  --output-dir .benchmark-assets/memorilla/bundles/stage3
.venv/bin/python zig/pkg/inference/scripts/memorilla/exporter.py verify \
  --bundle .benchmark-assets/memorilla/bundles/stage3
```

Use `--checkpoint all` to acquire all five releases. Export each to its own
directory. The manifest records source and output hashes, architecture, tensor
inventory, base-model revisions, embedding/prompt policies, and implementation
provenance. Published-checkpoint mode rejects other weights. Synthetic test
exports use a Python-only `release=False` parameter and are marked accordingly;
native tools require an explicit synthetic opt-in.

Canonical v1 converted weight hashes are pinned independently of each bundle's
manifest. Recomputing a manifest after substituting different weights cannot
impersonate a released checkpoint. Reproducible conversion uses the pinned
SafeTensors version; changing its serialization format requires a new profile.

## Generate memory module references

`reference.py` reconstructs packed `nn.MultiheadAttention`, independently of the
native split-projection execution. It records inputs and each attention,
normalization, FFN, residual, and projection boundary. Its six cases cover one
document, ragged batches, masked-row changes, document permutations, a distinct
duplicate document, and a changed question. Empty collections have an explicit
pre-attention fallback requirement.

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/reference.py \
  --bundle .benchmark-assets/memorilla/bundles/stage3 \
  --output .benchmark-results/memorilla/stage3-f32 --dtype float32 \
  --upstream-source /path/to/pinned/memorilla/source
```

The optional upstream source must contain the hash-pinned original `memory.py`.
Its unchanged `MemoryModule.forward` is compared with the traced oracle. No
vLLM installation or source modification is needed. A separate BF16 reference
can be generated with `--dtype bfloat16` into a different output directory.

These module fixtures use synthetic normalized vectors cast through FP16 storage
and BF16 input precision, then widened for the F32 arithmetic reference.
They establish numerical module behavior, not useful answers or encoder parity.

## Native module verification

Build the focused verifier from the inference package:

```sh
cd zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" \
zig build build-memorilla-verify -Dcuda=true -Doptimize=ReleaseFast -j2
```

Return to the worktree root, then compare each intermediate boundary:

```sh
zig/pkg/inference/zig-out/bin/antfly-memorilla-verify \
  --bundle .benchmark-assets/memorilla/bundles/stage3 \
  --reference .benchmark-results/memorilla/stage3-f32 --backend native \
  --precision-profile trained-f32-v1
zig/pkg/inference/zig-out/bin/antfly-memorilla-verify \
  --bundle .benchmark-assets/memorilla/bundles/stage3 \
  --reference .benchmark-results/memorilla/stage3-f32 --backend cuda \
  --precision-profile trained-f32-v1
```

The initial native memory profile widens source BF16 weights to F32 and uses F32
activations. Compare it to the F32 oracle. A BF16 fixture is a separate rounding
profile and must not be presented as having passed this F32 verifier. CUDA
evidence reports actual device dispatch; diagnostic boundary tracing intentionally
reads intermediate outputs back for comparison. The generic rectangular attention
kernel is a correctness baseline, not an optimized large-collection scan.
The explicit trained profile's component and normwise limits are documented in
[PRECISION.md](PRECISION.md), including the independent F64 analysis. The strict
default remains available; passing a different arithmetic profile must be
reported separately.

## Acquire and verify the exact base models

The pair requires about 24.5 GB of upstream artifacts. Downloads below use bounded
range requests, resumable part receipts, and full-file digest verification.
Interrupted output is never accepted as a model merely because a file exists.

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/assets.py acquire \
  --role encoder --model-dir .benchmark-assets/memorilla/models/qwen3-embedding-4b
.venv/bin/python zig/pkg/inference/scripts/memorilla/assets.py acquire \
  --role decoder --model-dir .benchmark-assets/memorilla/models/qwen3-8b
```

Use `verify` in place of `acquire` for an offline integrity check. The native
managed encoder is also available through
`hf:Qwen/Qwen3-Embedding-4B:memorilla-bf16-safetensors-bundle-v1`.
That variant fixes the feature recipe but retains ordinary mixed CUDA arithmetic.
The explicitly qualified F32 serving variants for both roles are
`hf:Qwen/Qwen3-Embedding-4B:memorilla-f32-safetensors-bundle-v1` and
`hf:Qwen/Qwen3-8B:memorilla-f32-safetensors-bundle-v1`.
The plain Qwen3 CUDA generator admission is limited to the canonical dense 8B
SafeTensors geometry; model-family parsing alone is not a qualification result.

## Encoder and soft-input decoder references

`encoder_reference.py` verifies the complete locked encoder, generates full-width
Transformers features, and can compare native `embed --diagnostics` token IDs,
recipes, batches and vectors. Use its `--help` for the native comparison flags.
CUDA comparison requires measured text-forward kernel, linear, and attention
dispatches, resident GPU weights, and zero recorded host attention, RoPE, and
synchronization fallbacks. Final output readback is allowed.
`--prepare-native-manifest` explicitly installs the catalog's exact memory recipe
beside the immutable upstream files and refuses an incompatible existing recipe.

The offline strict-F32 CUDA encoder verifier checks all twelve locked upstream
artifacts, the exact memory recipe, and the selected shard index before loading.
Its explicit constructor widens the source BF16 weights before GPU upload and
checks every resident weight is F32. It leaves the normal serving profile intact.

```sh
cd zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global ZIG_LOCAL_CACHE_DIR=.zig-cache \
  zig build build-memorilla-encoder-verify -Dcuda=true -Doptimize=ReleaseFast -j2
cd ../../..
.venv/bin/python zig/pkg/inference/scripts/memorilla/encoder_reference.py \
  --model-dir .benchmark-assets/memorilla/models/qwen3-embedding-4b \
  --output .benchmark-results/memorilla/encoder-reference-cuda-strict-f32 \
  --native-binary zig/pkg/inference/zig-out/bin/antfly-memorilla-encoder-verify \
  --backend cuda --threads 8
```

The exact four-case check passed the unchanged `1e-4` component and `0.99999`
cosine limits. It records the binary hash, compiled model-lock hash, arithmetic,
actual F32 residency, and zero low-precision staging or host fallback counters.
The original normal CUDA mixed-precision run failed these F32 gates and remains
a failed receipt. See [PRECISION.md](PRECISION.md) for the independent numerical
evidence and the scope of this diagnostic.

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/encoder_reference.py \
  --model-dir .benchmark-assets/memorilla/models/qwen3-embedding-4b \
  --output .benchmark-results/memorilla/encoder-reference \
  --prepare-native-manifest
.venv/bin/python zig/pkg/inference/scripts/memorilla/decoder_reference.py \
  --model-dir .benchmark-assets/memorilla/models/qwen3-8b \
  --bundle .benchmark-assets/memorilla/bundles/stage3 \
  --output .benchmark-results/memorilla/decoder-reference
```

The decoder reference renders the upstream system memory region with thinking
disabled, injects the module output using the upstream scatter-add arithmetic,
and emits full prefill logits and three teacher-forced cached decode rows. It
preserves the decoder's original vocabulary. The training wrapper's vocabulary
resize is a distinct reference policy. It also records that the initial document
features are synthetic rather than generated by the encoder.
Pass `--encoder-reference .benchmark-results/memorilla/encoder-reference` to
instead use its verified document and question features. The decoder then uses
the same question that produced the encoded query vector. This composes real
encoder features with the memory and decoder reference; isolated module tests
remain independent of encoder arithmetic.

Build `build-memorilla-qwen-verify` with the same worktree-local CUDA build settings.
Then run:

```sh
zig/pkg/inference/zig-out/bin/antfly-memorilla-qwen-verify \
  --model .benchmark-assets/memorilla/models/qwen3-8b \
  --fixture "$PWD/.benchmark-results/memorilla/decoder-reference/reference.safetensors" \
  --backend cuda --kv-dtype f32 \
  --atol 0.001 --rtol 0.001 \
  --output .benchmark-results/memorilla/qwen-cuda-comparison.json
```

F32 KV isolates decoder arithmetic against the CPU reference. To check the F16
cache implementation, generate a separate reference with `decoder_reference.py
--kv-dtype f16`, then select `--kv-dtype f16` in the native verifier. This oracle
rounds post-RoPE keys and values through FP16 before prefill attention and every
cache update, then performs CPU attention in F32. The distinct reference precision
label is recorded in the native report and is rejected with a native F32 cache.
Keep the original F32 reference to measure the F16 quantization cost separately.
The native F16 cache failed the frozen `1e-3` component limits against the F32
oracle; a matching F16 oracle does not erase that failed approximation result.
The paired F16 oracle comparison also failed the unchanged limits; F16 KV
remains unqualified, and its remaining CPU/CUDA discrepancy is unresolved.
See [PRECISION.md](PRECISION.md) for the measured differences.

The reference session widens BF16 source weights to F32 before GPU
upload and rejects mixed FP16/BF16 matmul staging. Normal BF16-weight serving
is a separate execution profile. The verifier's typed soft-input sequence is
internal; HTTP memory-token input and database recall belong to later stages.

The production raw-prompt smoke uses explicit deployment limits large enough for
the loader's conservative reservation. From the worktree root:

```sh
zig/pkg/inference/zig-out/bin/antfly-inference-dev generate \
  .benchmark-assets/memorilla/models/qwen3-8b 'The capital of France is' \
  --backend cuda --raw-prompt --no-chat-template --max-tokens 8 --temperature 0 \
  --print-token-ids --print-timing \
  --host-budget-mb 32768 --backend-budget-mb 49152 --combined-budget-mb 65536 \
  --kv-budget-mb 2048 --scratch-budget-mb 2048
```

The 48 GiB backend value is a configured cap. The measured smoke reported
16,382,087,168 resident weight bytes; its planned backend load peak was
36,858,412,746 bytes. The default 9 GiB backend cap and a 32 GiB cap both rejected
the load before execution. CUDA Qwen generation defaults to F16 KV; this ordinary
BF16 serving smoke is distinct from strict-F32 decoder numerical qualification.

Native text chat was also checked after fixing the pinned Qwen template's
reverse slicing, boolean identity tests, and string methods. Twelve exact
[rendered fixtures](../../../../lib/jinja/src/testdata/qwen3_8b_rendered.json)
cover text, inline reasoning history, and the Memorilla prompt frames. A live
CUDA run matched Hugging Face's rendered bytes and all prompt token IDs:

```sh
zig/pkg/inference/zig-out/bin/antfly-inference-dev generate \
  .benchmark-assets/memorilla/models/qwen3-8b \
  'What is the capital of France? Answer in one word.' \
  --backend cuda --disable-thinking --cache-dtype f32 --no-bos \
  --max-tokens 8 --temperature 0 --print-chat-template-status \
  --print-prompt --print-prompt-token-ids --print-token-ids --print-timing \
  --host-budget-mb 32768 --backend-budget-mb 49152 --combined-budget-mb 65536 \
  --kv-budget-mb 2048 --scratch-budget-mb 2048
```

This is ordinary text-chat framing and generation. It does not compose the
native encoder and memory reader into one serving request.

## Managed service verifier

The composed verifier calls the same managed service used by the inference API.
It loads published model variants through ModelManager, acquires cancellable
asset and execution leases, constructs the released reader under its own GPU
lease, and retires physical request allocations before refunding admission.
It requires finite operator ceilings; it does not enlarge existing defaults.
The managed profiles reject host-attention and reduced-precision overrides.
Each stage checks actual execution counters under its execution lock before
and after the forward; a forbidden fallback cannot publish a result.

```sh
(cd zig/pkg/inference && \
  ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
  ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" \
  zig build build-memorilla-composed-verify -Dcuda=true -Doptimize=ReleaseFast -j2)
zig/pkg/inference/zig-out/bin/antfly-memorilla-composed-verify \
  --mode prepare \
  --encoder /absolute/path/to/managed-f32/encoder \
  --decoder /absolute/path/to/managed-f32/decoder \
  --bundle /absolute/path/to/bundles/stage3 \
  --features /absolute/path/to/encoder-reference-cpu-exact/encoder.safetensors \
  --native-query-output .benchmark-results/memorilla/composed-native-question.json \
  --budget-gib 96
```

Preparation exports the exact native query and exits before decoder or reader
GPU loading. Generate the independent conditional CPU oracle below, after the
preparation process exits. Then run the verifier again with `--mode verify`, the
same model, bundle and feature arguments, plus:

```sh
  --native-query .benchmark-results/memorilla/composed-native-question.json \
  --fixture /absolute/path/to/composed-conditional-reference/decoder/reference.safetensors \
  --budget-gib 112
```

Every verified request re-encodes its question and requires bitwise-identical
stored FP16 and BF16 input features. The numerical gate covers the exact prompt,
reader output, injected embeddings and all vocabulary logits across three
teacher-forced steps. The lifecycle also cancels after prefill, repeats A,
executes a different 64-document collection with 128 greedy tokens, and repeats A
again. The larger request checks capacity and isolation; it has no CPU logit
oracle. A recovery requires unchanged logits and refunded transient admission.
JSONL evidence includes admitted host/device peaks, physical owned CUDA buffer
ceilings and lifetime peaks, and process RSS. Driver and vendor-private CUDA
allocations require additional deployment headroom.

The decoder now retains one validated reader and its complete admission lease
between requests. Profile switches replace that entry. The verifier checks that
decoder pins prevent eviction and that unloading the decoder also refunds the
reader's persistent admission. Warm cancellation and A/B/A checks reuse the
reader while requiring fresh request tensors and KV. Bundle generations are
checked before and after each borrow; full hash validation remains mandatory
on cache misses.

Warm preflight also borrows the admitted model tokenizer, template and verified
manifest. The verifier attempts decoder eviction during framing to prove that
this borrow pins the model before an execution handle exists. It attests that
the qualified strict FP32 projection policy actually ran; actual GEMM shapes,
F32 KV, numerical tolerances and workspace ceilings remain unchanged. See the
[native/Python latency comparison](../../../../../docs/design/inference/history/memorilla/2026-11-09-native-python-latency.md)
for the two-pass timing results and library-matched Python control.

For an isolated three-iteration reader-load diagnostic, build
`build-memorilla-load-bench` with the same CUDA and optimization flags, then run
`zig/pkg/inference/zig-out/bin/antfly-memorilla-load-bench /absolute/path/to/bundle`.
It reports bundle verification, F32 conversion, CUDA initialization, upload and
teardown separately, under a fixed 2 GiB CUDA allocation ceiling. Run it when
the GPU is free; its timings exclude the encoder, decoder and serving path.

This offline harness permits native calls in its own process. Production uses
the existing supervised worker and watchdog; cancellation after prefill is a
cooperative lifecycle test, not a blocked-driver termination test.

The recorded GB10 managed run passed this complete lifecycle with binary
`1c92ad6cd8ac35d372f78cf717207efede94aedccbb433e1eb52aa3f2cca7cf9`.
It requested 112 GiB and the existing model admission policy resolved 88 GiB.
Sampled combined admission peaked at 74.068 GiB during cold construction,
72.069 GiB for warm A and 72.219 GiB for the 64-document/128-token case.
All request KV and scratch returned to the exact two-model baseline. The
summary is `.benchmark-results/memorilla/composed-verify-eager-summary.json`;
its source snapshot, raw traces, conditional oracle and binary are hash-bound.
This is supplied-feature composition evidence. The HTTP route and answer
quality need their own evidence, as described in the production guide.

## Conditional reference for the composed native service

`conditional_reference.py` isolates reader/decoder arithmetic from small encoder
rounding differences. First capture the managed encoder's exact query with the
composed verifier's `--mode prepare --native-query-output` option. Its
`antfly.memorilla.native-query.v1` receipt contains the canonical question,
stored FP16 values, BF16-valued F32 inputs, and the producer publication,
execution identity, model-lock hash, and recipe hash. Preparation stops after
encoding the question; it does not qualify the composed pipeline.

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/conditional_reference.py \
  --model .benchmark-assets/memorilla/models/qwen3-8b \
  --bundle .benchmark-assets/memorilla/bundles/stage3 \
  --encoder-reference .benchmark-results/memorilla/encoder-reference-cpu-exact \
  --native-query .benchmark-results/memorilla/composed-native-question.json \
  --output .benchmark-results/memorilla/composed-conditional-reference \
  --upstream-source /path/to/pinned/memorilla/source --steps 3 --threads 4
```

The helper checks the query's FP16-to-BF16 conversion bit for bit, requires its
question to match the independent CPU encoder receipt, and retains that
receipt's document rows 1 and 2. It runs the released reader and pinned decoder
on CPU using those documents plus the exact native query. Outputs are
`memory/reference.{json,safetensors}`, `decoder/reference.{json,safetensors}`,
and `conditional.json`; receipts bind the consumed query bytes, source artifacts,
helper, and output tensors with SHA256. Existing output directories are refused.
Allow roughly 40 GiB of host memory for the F32 decoder oracle.

Use the decoder tensor file as the composed verifier's `--fixture` and the same
query receipt as `--native-query`. The reader's frozen `trained-f32-v1` gates
and decoder's `atol=rtol=1e-3` gates remain unchanged. These conditional fixtures
do not independently qualify the encoder or authenticate caller features;
retain the separate encoder comparison and actual native execution receipts.
The serving scope and feature recipe are described in the
[production guide](../../../../../docs/design/inference/memorilla-serving.md).

## Tests and evidence

### Raw-document answer-quality diagnostic

`quality_eval.py` starts from raw documents, embeds them through the qualified
native HTTP producer, writes the normalized 2560-dimensional features to JSON,
reads them back, and submits each question with its selected stored collection.
Run it against an already-running supervised server with the named Stage3
profile and qualified encoder. Use a new output filename for each experiment:

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/quality_eval.py \
  --base-url http://127.0.0.1:18091 \
  --profile memorilla-stage3 --encoder encoder \
  --output .benchmark-results/memorilla/quality-e2e/stage3.json
```

The server must permit the suite's 48 generated tokens and the full framed
question input; the earlier HTTP contract fixture's eight-token ceiling is
insufficient. Optional `ANTFLY_API_KEY` supplies bearer authentication without
writing the key to the receipt. This runner does not start a listener or load
models outside the configured service.

The frozen `fixtures/quality_v1.json` has 24 cases: ten pairs of counterfactual
worlds, a reordered-document pair and a missing-fact pair. It tests direct
lookup, multiple-hop reasoning, distractors, exact codes and abstention using
fictional facts. Both worlds must be answered correctly to count a successful
counterfactual pair. Merely changing the response does not establish grounding.
Scores distinguish exact answers, conservative entity matches, forbidden facts
and abstention. Gold answers and evidence labels never enter inference requests.

Receipts retain answers, token IDs, execution identities, per-case scores,
truncation counts, timings and request/response digests. The adjacent feature
store retains raw documents, embeddings and digests. After the last case, the
runner repeats the first stored-feature request and requires an identical
response. Producer or response identity errors and isolation failures make
`execution_passed` false. Incorrect answers lower quality scores independently;
`completed` means the diagnostic finished, and `rollout_qualified` remains false.

Use `--case-id ID` repeatedly for a diagnostic subset. The full-suite score
retains missing-case penalties; selected-case accuracy is labelled separately.
Use `--reuse-features PATH` to compare another configured module against the
same recorded features, with its own `--profile` and `--module-sha256`. Digests
prove artifact integrity, not feature authorization or trustworthy provenance.
The default fixture and scorer are development diagnostics, not a reproduction
of published benchmark scores or a production release gate. Freeze new suites
and expected answers before observing model outputs.

The [2026-11-08 evaluation](../../../../../docs/design/inference/history/memorilla/2026-11-08-answer-quality.md)
completed native execution but scored zero of 22 factual cases correct; two
missing-fact abstentions passed. That outcome blocks factual QA rollout even
though the separate arithmetic and lifecycle checks passed.

`quality_baseline.py` compares four fixed cases with literal document text and
with no documents. Stop the Memorilla server first: every condition starts a
fresh native decoder process with the managed F32 weights, explicit F32 KV and
greedy decoding. The decoder source is shared, but these ordinary generation
controls use a different context representation and do not qualify the serving
composition:

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/quality_baseline.py \
  --native-binary zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --model .benchmark-assets/memorilla/managed-f32/decoder \
  --output .benchmark-results/memorilla/quality-native-baseline-new
```

The baseline checks exact Hugging Face/native prompt token IDs, generated IDs,
available CUDA counters and F32 resident weight bytes. Its timing JSON may omit
raw output IDs; the separately printed native IDs and counts remain required.
Native `stop` and HTTP `eos` are distinct response contracts. Explicit `--resume`
can revalidate completed native artifacts after an interrupted evaluation; it
checks the saved protocol, prompts, source assets and hashes before reuse and
preserves the earlier summary. There is no automatic model or precision fallback.
For reused entries, `elapsed_seconds` measures artifact processing rather than
native inference. Read the original native timing files for their separately
recorded load and generation times; do not average reused and fresh durations.

```sh
MEMORILLA_SOURCE_ROOT=/path/to/pinned/memorilla/source \
.venv/bin/python -m pytest -q zig/pkg/inference/scripts/memorilla
```

Set `MEMORILLA_VERIFY_BIN` to the absolute module-verifier binary to enable its
compiled CPU tests. Set `MEMORILLA_VERIFY_BACKEND=cuda` for the explicit GPU lane.
Without a compiled binary these optional tests skip; a skipped lane does not
qualify native execution. Keep raw receipts and logs in `.benchmark-results/`.

Acceptance requires actual released-checkpoint loading, strict export round-trip,
native module boundary comparison, exact base-model token/vector/logit comparison,
and explicit CUDA dispatch. Passing synthetic tests or adding a CUDA admission
profile alone does not satisfy those gates. See the integration plan for later
database lifecycle, workload-quality, and Gemma training gates.

## Source-verified answer evaluation

`verified_quality_eval.py` evaluates the explicit `source_verified` mode documented
in the [serving guide](../../../../../docs/design/inference/memorilla-serving.md).
It produces embeddings from the actual documents, persists and reloads them,
then submits the stored vectors together with their original source texts. Source
answering runs independently of the memory candidate; a supported source answer
is required before either factual answer path can publish. A memory answer also
requires exact agreement after boundary whitespace trimming.

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/verified_quality_eval.py \
  --base-url http://127.0.0.1:18090 --profile memorilla-stage3 --encoder encoder \
  --native-binary zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --output .benchmark-results/memorilla/verified-development.json
```

Use the installed qualified encoder name. The default development run includes
all 24 frozen v1 cases and the 16 v2 development cases. The v2 fixture has 96 cases
with disjoint paired groups across train, development and test splits. It covers
ordinary names, realistic prose, entity binding, multiple hops, dated corrections,
missing facts, contradictions and instruction injection. Expected answers and
support annotations are never sent to inference.

Held-out evaluation requires `--split test --development-receipt PATH` with a
matching completed development receipt. The tool recomputes the development
scores from saved answers and requires all 22 factual v1 cases, both v1 negatives
and all 16 development cases correct before it issues any held-out request.
The test run includes all 24 v1 cases and the 48 held-out v2 cases. This diagnostic
gate does not establish production corpus accuracy or source truth. Keep failed
attempts; do not change gold answers or scoring rules after observing outputs.

`verified_http_smoke.py` checks the source-mode wire contract using a saved feature
store from that run. It exercises known factual answers, isolation, concurrent
requests, timeout recovery, legacy compatibility and input/error bounds.

`build-source-qa-probe` builds the internal CUDA source-only diagnostic. It accepts
a trusted local JSON job with questions, documents and explicit prompt strings,
then prints raw source answers and verifier verdicts separately. It accepts no
gold fields and provides no public API override. Use it on development data to
distinguish answering errors from verification errors. The separate
`fixtures/mixed_instruction_dev.json` checks that ignoring response demands does
not discard real facts, unresolved conflicts or dated corrections. Keep these
development diagnostics separate from held-out qualification.

For the internal probe, a job may select `"mode": "bounded_thinking"`,
`"raw_max_new_tokens": 1024` and `"reasoning_body_token_cap": 950`.
The defaults remain 128 raw tokens and 64 reasoning tokens. The diagnostic
ceiling is 2048 raw tokens; actual admission still depends on the complete
prompt, KV reservation and operator limits. Only the `</think>` format token
may replace argmax when the reasoning budget is exhausted. Source answers and
verdicts are always generated by the model. Receipts preserve both selected
and argmax IDs, the intervention, and the actual final token suffix. Truncated,
malformed or oversized suffixes cannot qualify as answers or verdicts.

These controls do not change the HTTP API's plain generation or output caps.
Longer reasoning is a development experiment, not an approved serving policy;
retain failed trajectories and require the complete frozen quality gates before
adopting a candidate. Numerical agreement alone does not qualify its answers.

The [resumed qualification](../../../../../docs/design/inference/history/memorilla/2026-11-08-resumed-qualification.md)
records the passing F32 numerical and supervised source-verified QA checks,
including the frozen held-out results and HTTP recovery tests. It also retains
the failed latent-only, reasoning and standalone-critic diagnostics.

## Latent operational QA and GPU timing

The [GPU comparison and adaptation report](../../../../../docs/design/inference/history/memorilla/2026-11-09-latent-operational-qualification.md)
records identical native/Python F32 output IDs on all 24 frozen QA cases,
including 0/22 correct factual answers. Warm median latency is 3.680 s through
the managed native API versus 0.819 s through Python/Transformers on the same
GPU. This is a serving-path comparison, not a vLLM throughput result.

`gpu_quality_reference.py` runs the independent pinned reader and decoder on
CUDA, with TF32 disabled for F32. It archives sources and identities, separates
document ingestion from generation, warms the pipeline and times actual
question encoding, reader execution and autoregressive decoding. Use
`--feature-store` to reuse the native evaluator's saved document features for
the matched comparison. A separate `--dtype bfloat16 --system upstream` control
recomputes features under the released BF16 policy; it does not qualify native
BF16 arithmetic. Keep the CPU reference environment unchanged.

`operational_curriculum.py --output PATH` creates the deterministic disjoint
training/development/final worlds for exact names, codes and operational
relations. `train_operational_reader.py` prints its protocol unless `--run` is
supplied. It trains only a separate reader and preserves released checkpoints.
The final 192 cases stay sealed until all 96 development answers are exact.

For independent candidate checks, pass its explicit `--candidate-sha256` to
the GPU reference. `--operational-curriculum --suite PATH --split dev` tests the
complete development set; `--split test --development-receipt PATH` recomputes
the saved development scores and binds the checkpoint, precision, instruction
and curriculum before final inference. Candidate checkpoints do not enter the
native release allowlist or inherit its conformance status.

The operational candidate currently fails: 66/96 in the F32 development
assessment, including 1/16 exact codes, and 0/22 on the original factual suite.
The final 192 worlds were not executed. Keep latent-only factual QA unqualified.

The [published-data diagnostic](../../../../../docs/design/inference/history/memorilla/2026-11-09-latent-published-qa.md)
records 20/32 normalized exact matches on a bounded released TriviaQA sample
through the Python BF16 latent-only reference. This is separate from exact
operational-fact qualification and does not execute the original vLLM engine.

`vllm_quality_reference.py` exercises the unchanged upstream `MemoryVLLM`
wrapper with actual vLLM encoder and decoder engines. Prepare a separate
environment with upstream's vLLM 0.9.1 and PyTorch 2.7.0; the runner rejects other
versions and verifies the existing local model assets and upstream source.
Pass the usual `--encoder`, `--decoder`, `--checkpoint`, `--upstream-source`
and fresh `--output` paths. It calls `MemoryVLLM.answer` for the frozen default
suite. `--released-benchmark triviaqa --suite PATH` uses the upstream benchmark
instruction/suffix while keeping the question embedding separate from the
decoder suffix.

Use repeated `--compare RECEIPT` arguments for saved runs of the same cases.
Receipts preserve actual vLLM completion IDs, finish reasons, engine settings,
and BF16 document/query/memory/prompt tensors. Raw token equality and equality
after removing a single terminal EOS are reported separately. Diagnostic
capture adds synchronization and ingestion work; its times do not establish a
native/vLLM latency comparison. A successful run or matching wrong answers
does not qualify exact operational factual recall.

`vllm_latency_reference.py` measures the cached-document path separately. Pass
the same model/source/output paths plus `--feature-store PATH`; it uses the
native system instruction, one live query embedding per request, CPU document
vectors, the original reader/decoder implementation and two warm 24-case timing
passes. It excludes ingestion, startup and tensor capture. Its BF16 profile is
separate from the qualified native F32 profile.

The [official engine and timing report](../../../../../docs/design/inference/history/memorilla/2026-11-09-vllm-engine-qualification.md)
records the passing actual vLLM execution, matching QA accuracy, 52/56 matched
BF16-control token sequences and all 56 prompt sequences. Warm medians are
0.819 s native F32 and 0.823 s vLLM BF16 on this GB10 workload, effectively
parity rather than an isolated-kernel or general speed claim.

The trainer saves atomic, fsynced reader/optimizer/RNG/order state every 200
steps. `--resume-from PATH` requires the identical protocol and trainer source;
use the run's archived `source/train_operational_reader.py` and saved arguments.
Choose a fresh output directory to preserve interrupted receipts. The controlled
step-200 termination/restart test restored all optimizer states and continued at
step 201. Checkpoints and candidate files never replace released model assets.
