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
[base_models.json](base_models.json), mirrored from the native model lock so
archived reference runs remain self-contained. The Qwen3-Embedding-4B recipe uses
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
[latency qualification](../../../../../docs/design/inference/memorilla-qualification.md#performance)
for the two-pass timing results and library-matched Python control.

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

The [recorded QA assessment](../../../../../docs/design/inference/memorilla-qualification.md#answer-quality)
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

The compact `fixtures/mixed_instruction_dev.json` regression covers response
demands mixed with real facts, unresolved conflicts and dated corrections.
It includes source-command and shared-surname failures found during native
reliability testing. Run its seven authored development cases against a live
server with the existing evaluator:

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/verified_quality_eval.py \
  --base-url http://127.0.0.1:18091 --profile memorilla-stage3 \
  --diagnostic-suite zig/pkg/inference/scripts/memorilla/fixtures/mixed_instruction_dev.json \
  --output .benchmark-results/memorilla/source-diagnostic.json
```

This mode uses exact answer values, rejects blanket abstention on answerable
cases and reports wrong published answers. Gold labels stay outside requests.
Its receipt cannot authorize held-out QA; the frozen v1/v2 development gate is
unchanged. Use a fresh output path for each run.

`gpu_quality_reference.py` runs the independent pinned reader and decoder on
CUDA, with TF32 disabled for F32. It archives sources and identities, separates
document ingestion from generation, warms the pipeline and times actual
question encoding, reader execution and autoregressive decoding. Use
`--feature-store` to reuse the native evaluator's saved document features for
the matched comparison. A separate `--dtype bfloat16 --system upstream` control
recomputes features under the released BF16 policy; it does not qualify native
BF16 arithmetic. Keep the CPU reference environment unchanged.

Experimental reader adaptation is outside this serving PR. Its archived
[assessment summary](../../../../../docs/design/inference/memorilla-qualification.md#answer-quality)
remains a failed quality result; no candidate is promoted. The released reader
and exact operational-fact tests remain the supported comparison.

The [published-data diagnostic summary](../../../../../docs/design/inference/memorilla-qualification.md#answer-quality)
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

The [official engine and timing summary](../../../../../docs/design/inference/memorilla-qualification.md#performance)
records the passing actual vLLM execution, matching QA accuracy, 52/56 matched
BF16-control token sequences and all 56 prompt sequences. Warm medians are
0.819 s native F32 and 0.823 s vLLM BF16 on this GB10 workload, effectively
parity rather than an isolated-kernel or general speed claim.

## Encoder LoRA research experiment

`encoder_lora.py` compares a trainable reader with a frozen encoder against the
same reader plus rank-8, alpha-16 encoder Q/V LoRA adapters. Both retain the
released 16 learned memory slots and frozen Qwen3-8B weights. Gradients flow
through Qwen into the reader and encoder adapters; source text never enters the
decoder prompt. The small explicit LoRA implementation requires no PEFT package.

This is a Python training experiment, not a qualified native profile. The tested
training environment uses PyTorch 2.9.1+cu130 and Transformers 4.53.3. Base model
files are read-only. Encoder and decoder use BF16 SDPA, trainable weights use
FP32, and differentiable FP16-to-BF16 feature casts retain the storage bottleneck.
Reader dropout is disabled in both arms. Do not interpret this as native BF16
qualification or overwrite an existing retrieval index with adapted embeddings.
Use a separate CUDA environment so the existing CPU oracle remains reproducible:

```sh
train_env=.benchmark-assets/memorilla/training-venv
UV_CACHE_DIR=.zig-cache/memorilla-uv uv venv --python python3 "$train_env"
UV_CACHE_DIR=.zig-cache/memorilla-uv uv pip install --python "$train_env/bin/python" \
  --index-url https://download.pytorch.org/whl/cu130 torch==2.9.1
UV_CACHE_DIR=.zig-cache/memorilla-uv uv pip install --python "$train_env/bin/python" \
  -r zig/pkg/inference/scripts/memorilla/requirements-reference.txt
```

Use that environment's Python for the following commands. Generate fresh
synthetic data before observing model outputs:

```sh
python zig/pkg/inference/scripts/memorilla/encoder_lora.py make-data \
  --output .benchmark-results/memorilla/encoder-lora/data
```

The default corpus has 512 training, 32 development, 32 test and 32 additional
unseen-phrasing examples. Entities, values, documents and counterfactual groups
are disjoint across splits. Changing a code while holding its question fixed
must change the answer; missing-fact controls reuse the positive question forms.
The trainer reads only train/dev, leaving test and unseen-phrasing splits sealed.
It reports exact case-sensitive code/name answers, abstentions and counterfactual
pair accuracy separately. Generated datasets and checkpoints remain outside Git.

Run each arm sequentially with identical arguments apart from arm/output:

```sh
python zig/pkg/inference/scripts/memorilla/encoder_lora.py train \
  --arm reader-only \
  --encoder /path/to/pinned/qwen3-embedding-4b \
  --decoder /path/to/pinned/qwen3-8b \
  --checkpoint /path/to/original/stage3 \
  --upstream-source /path/to/pinned/memorilla \
  --data .benchmark-results/memorilla/encoder-lora/data \
  --output .benchmark-results/memorilla/encoder-lora/reader-only \
  --steps 128 --accum 4 --eval-every 32
```

Repeat with `--arm encoder-lora` and a fresh output directory. The initial
comparison fixes 128 optimizer steps, effective batch four, seed 23, reader
learning rate 5e-5 and adapter learning rate 1e-4. A zero-effect frozen adapter
architecture is retained in the control. Data order is determined by absolute
microstep, so both arms see exactly the same examples in the same order.

Use the same command plus `--resume` after interruption. `resume.pt` is flushed
to disk before it atomically replaces the previous full checkpoint and includes
reader/adapters, optimizer,
step, RNG and protocol state. Resume rejects changed source, data or training
budget. Exported candidate metadata explicitly remains unqualified. Frozen base
parameter version/storage counters and gradient absence are checked during
training; only reader and adapter tensors are exported.

To extend a completed run, keep its output intact and use `--continue-from
/path/to/completed/run --steps 512 --eval-every 128` with a fresh `--output` and
the original arm, data, seed, accumulation and learning rates. The step budget is
the total across both phases. Continuation restores the optimizer and RNG,
records parent checkpoint/protocol hashes, and reproduces every final parent
development answer before updating weights. Ordinary `--resume` still requires
the same protocol, source and budget. Each new run archives its trainer source.

`encoder_lora_eval.py` independently reloads up to three exported candidates.
Pass the same model/source/data paths, a fresh `--output`, and repeated
`--candidate /path/to/completed/run` arguments. It requires all 32 final
development answers to reproduce exactly and evaluates a fixed 32-example
training probe to distinguish failure to fit from failure to generalize. It
provides no test/unseen-phrasing option during this development experiment.

The initial matched 128-step pilot recovered 0/16 development codes and 0/8
names with the reader-only control, versus 1/16 codes and 0/8 names with encoder
LoRA. Both answered all eight missing-fact cases correctly, and neither solved
any complete counterfactual code pair. Both also recovered zero exact facts on
the training probe. All 32 development outputs per arm reproduced after export
and reload. This is a small development signal, not reliable factual recall;
test and unseen-phrasing splits remain unopened.

Extending both arms to 512 total steps with restored optimizer state and the
same examples yielded 0/16 codes, 0/8 names and 8/8 missing-fact answers in both
arms. The single exact code at 128 steps was not sustained. Base asset proofs
matched across all runs; frozen parameter checks passed throughout. These
results do not qualify either research candidate for factual QA.

The independent 512-step reload reproduced all 32 development answer texts per
arm. On the fixed training probe, LoRA recovered 10/16 codes and 5/8 names,
including 4/8 complete counterfactual code pairs; the control recovered 0/16
codes and 2/8 names, with no complete pairs. Both got 8/8 missing-fact answers
right. The adapters can learn training facts, but that improvement has not
transferred to unseen development entities. Test and unseen-phrasing splits
remain unevaluated; further training work must address generalization.


### Expanded corpus and exact-field reconstruction

`encoder_lora_corpus.py` builds 16,384 training examples in 4,096 related groups,
with six document templates, four question forms per fact type and alternating
document order. It preserves the original development cases and adds eight
fresh groups, yielding 64 development questions with separate cohort scores.
Entities, complete answer values and documents remain disjoint between train
and development. Existing test/unseen-phrasing files are never read or copied.

```sh
python zig/pkg/inference/scripts/memorilla/encoder_lora_corpus.py \
  --development-from /path/to/original/pilot/data \
  --output .benchmark-results/memorilla/encoder-lora-next/data
```

For the next matched experiment, both runs use the same completed encoder-LoRA
candidate via `train --initialize-from /path/to/candidate`, the expanded data,
512 steps, accumulation four and evaluation every 128 steps. Initialization
verifies candidate tensor/protocol/base hashes, restores reader and adapters,
and starts a fresh optimizer. Every original development answer must reproduce
before training starts. This is distinct from `--continue-from`, which retains
optimizer state and rejects changed data or objectives.

Use `--reconstruct-every 0` for QA-only and `--reconstruct-every 2` for alternating
QA and record reconstruction. The latter asks the decoder to reconstruct the
exact name and code fields using the same 16 memory slots conditioned on the
original QA query. Reconstruction labels are teacher-forced completion targets. They never enter
encoder inputs or the decoder question/system prompt. The two tasks have separate prompt
instructions and a fixed schedule by absolute microstep. Both runs process the
same ordered rows and optimizer steps; their target token counts and task losses
are recorded separately because reconstruction targets are longer.

The evaluator accepts `--fresh-groups N` to generate two fresh challenge cohorts
only after completed candidate hashes have been checked. Its optional
`--fresh-seed` reproduces a receipt; otherwise a new random seed is recorded.
One cohort uses familiar wording, the other different phrasing; code pairs
differ in exactly one character. Name pairs change only the holder while
retaining the same question; both names must be recovered exactly. All
generated groups are checked against the
training/development inventories and each other. These challenges are separate
from the existing sealed qualification splits and cannot establish universal
reliability. Saved-model development outputs must still reproduce before any
post-freeze score is accepted.


For a character-level reconstruction variant, add `--spell-values` when creating
the expanded corpus. Only reconstruction prompts and targets change: QA facts,
questions, documents, row order and development bytes remain identical. Present
values are spelled with spaces between characters and `/` between name words;
missing values retain `UNKNOWN`. The tested corpus's longest target is 36 tokens
including EOS, within the fixed 64-token reconstruction budget.

When comparing those variants, pass one `--candidate-data PATH` per candidate.
Each candidate retains its original training-manifest hash requirement, and the
evaluator additionally requires all QA inputs and labels to match the shared
`--data` corpus exactly. Reconstruction targets may differ. Training-probe
selection favors groups actually visited during the new phase; receipts separate
QA exposure, reconstruction-only exposure and untouched rows, so a file's
membership in the training corpus is not mistaken for having trained on it.

Fresh challenges require `--prior-training-data PATH` for earlier corpora in the
candidates' initialization lineage. Candidate/protocol hashes authenticate that
lineage, and generated facts exclude entities, values and documents from both
current and earlier training corpora, including codes seen only in source
distractors. Existing qualification files stay sealed.


`--readout-format spelled` adds a separately scored `dev-readout` diagnostic
alongside unchanged direct QA and its mandatory saved-model reproduction check.
The decoder reconstructs a record in the trained spelling format; a deterministic
parser joins characters and selects the requested field using only the public
question text. It supports the two original `access code for ...?` and
`brass key at ...?` question forms. It never consults gold values, source text,
entity annotations or category labels to route or correct an answer. Case,
hyphens and leading zeros are preserved; malformed or non-EOS output fails.
The reconstruction cap is 64 generated tokens versus 24 for direct QA.

If that protocol is selected using development data, fresh challenges use the
same readout protocol and retain both raw records and parsed answers. Direct QA
results remain separate. This is a research-only structured fact lookup protocol,
not a generic question parser or a production serving profile.


### Balanced counterfactual batches

The next corpus variant adds a second holder name for every site while
preserving all 16,384 original QA rows and the exact development bytes:

```sh
python zig/pkg/inference/scripts/memorilla/encoder_lora_corpus.py \
  --base-corpus .benchmark-results/memorilla/encoder-lora-next/data \
  --output .benchmark-results/memorilla/encoder-lora-next/data-paired
```

This produces 20,480 training rows in 4,096 five-row groups. Name worlds keep
the same question and change only the name in the source. Train with
`--sampling counterfactual-group --accum 8 --reconstruct-every 0`. Each update
contains code A/B, name A/B, two copies of the missing-fact control, and an
alternating repeated code or name pair. Overall exposure is 37.5% codes, 37.5%
names and 25% missing facts. Both conflicting values occur before the optimizer
updates, so a question-only prediction cannot fit both worlds. This combined
sampling/data experiment is distinct from the earlier fixed-budget objective
comparison. The encoder LoRA, reader geometry, decoder weights and QA prompt
policy remain the same. Training-exposure receipts understand both samplers.

Add `--phrasing-set v2` to either corpus command to train on ten document
templates instead of six. All six familiar templates name the site first and
introduce the holder with a fixed cue; readers trained on them recover 10-13/16
fresh names under familiar phrasing but 0/16 under the held-out owner-first
sentence, so they learned where a name sits rather than how to find one. The
four added templates put the holder or the code first, use terse and list-style
cues, and spell a missing field with dedicated wording that still says "not
recorded". None reuses the held-out sentence's clauses. Entities, values,
questions, row order, the distractor sentence and development bytes are
unchanged, and every record still parses with `lexical_record_text`. The default
`v1` regenerates existing corpora byte for byte; the manifest records
`phrasing_set`.

`--phrasing-set v3` replaces the fixed templates with a sentence grammar seeded
by the group index: one of six field orders (name-site-code through
code-name-site), eight holder verb phrases ("holds the brass key for", "has
custody of the brass key of", ...), eight code phrases ("the access code is",
"code on file:", "its door code reads", ...), four clause connectors and several
one- or two-sentence layouts, including relative clauses ("{site}, where {owner}
holds the brass key, opens with code ...") and mid-sentence sites. Missing fields
get per-field wording that still says "not recorded". The grammar never emits
either held-out clause or the held-out shape (holder, verb, site, then a
relative clause carrying the code). `data-paired-v3` has 4,786 distinct
site-sentence skeletons over 20,480 rows (2,120 among present-field rows), the
longest document is 43 encoder tokens, and `row["template"]` stores the field
order (0-5). Entities, values, questions, row order and development bytes are
unchanged from `data-paired`.

`--oracle-prefix-probe` is an explicit decoder-copy control, separate from every
QA score: it inserts known answer embeddings into the 16 slots. `--oracle-only`
skips QA evaluation and cannot open fresh qualification. The optional
`--oracle-question copy-value` asks for copying the supplied value instead of
answering the original entity-specific question. All such results carry
`uses_gold_in_memory: true` and `counts_toward_qa: false`; they test prompt/frame
behavior with supplied literal answers, not compression quality or factual QA.

### Token-level document features and lexical memory supervision

Three development-only controls (results under
`.benchmark-results/memorilla/encoder-lora-next/{ceiling-diagnostics,target-readability,encoder-probe}`)
locate the exact-fact failure of the frozen-decoder recipe:

- Placing the literal token embeddings of the source documents into the memory
  frame answers 64/64 development cases, the same as text in the user turn. The
  frame and the frozen decoder impose no readability penalty.
- A bare or site-free value in the slots is answered UNKNOWN under the strict
  prompt: the earlier oracle-copy result was abstention, not unreadability. A
  labeled record bound to the asked site (`" {site} code {code}"` or
  `" {site} key holder {owner}"`) fits 16 slots and is read 59/64.
- A probe trained from the released single pooled vector per document recovers
  0% of exact held-out codes, while per-token encoder states with a 16-query
  attention reader recover 99.8% of codes and 83% of names. The trained encoder
  LoRA had made the pooled vector a near-perfect bag of characters without
  order, which is exactly the scrambled spellings the decoder produced.

The trainer therefore has opt-in flags whose defaults leave every earlier run
byte-identical:

- `--document-features {pooled,tokens,chunks4,chunks8}` feeds the unchanged
  reader every active document token (or K contiguous mean-pooled chunks) as
  its own L2-normalized vector; the question stays last-token pooled. The
  native pipeline already accepts up to 64 masked document vectors, so chunked
  features are extra rows, not an architecture change.
- `--lexical-loss-weight W --lexical-record {owner-code,asked-first,site-asked}`
  adds a training-only pull of the 16 predicted memories toward the frozen
  decoder's input embeddings of a canonical record (cosine plus a squared
  log-norm ratio). The teacher never enters the decoder forward.
- `--memory-scale-init lexical-target` rescales the reader's final projection
  once so predicted memories match the embedding norm (the released reader
  emits memories about 175 times larger), and trains that projection at the
  reader learning rate times the same factor, which equals training the
  unscaled module with a folded output scale.
- `--lexical-warmup-steps N` runs the first N steps on the lexical objective
  only, without decoder passes and with encoder adapters resting, before the
  joint QA-plus-lexical phase.
- `--memory-slots N` (default 16, allowed 16..64) trains an N-slot reader. The
  released 16-slot checkpoint is still the required starting point; its learned
  queries are expanded so the first 16 rows stay bit-identical and row `i >= 16`
  copies row `i mod 16` plus Gaussian noise (std 0.01 times that row's std,
  from a seeded generator separate from the global RNG). Lexical records then
  pad to N tokens, the geometry checks, resume checkpoints, `candidate.json`,
  the exporter and the evaluator all read N from `protocol.memory_slots`. A
  16-slot candidate may initialize a wider run: its reader loads whole before
  expansion, `initialization.parent_memory_slots` records the parent's count,
  and the step-0 reproduction check only records the overlap, as for a changed
  feature mode. Resume and continuation still require equal slot counts.

Initialization from a candidate with a different feature mode is recorded as
`initialization.parent_document_features` and only the overlap of reproduced
development answers is reported, since the same weights see a new input
geometry.

Two further opt-in encoder controls keep the default byte-identical:

- `--encoder-layer N` (1..36 for Qwen3-Embedding-4B; default the final
  normalized state) feeds the reader `hidden_states[N]`, the un-normalized
  output of layer N (`hidden_states[0]` is the embedding output, so N equal to
  the layer count is the normalized final state). It applies to the training
  forward, warmup batches, the warmup feature cache (whose manifest then carries
  `encoder_layer`, so caches from different layers never mix) and the cache
  self-check; the evaluator reads the layer from `protocol.encoder_layer`.
  Resume and continuation require an equal layer. Initialization from a
  candidate trained on another layer is allowed because reader and adapter
  weights are layer-agnostic; it is recorded as
  `initialization.parent_encoder_layer` and, as for a changed feature mode, the
  step-0 check only records the overlap of reproduced development answers.
- `--lora-targets q_proj,v_proj,o_proj,down_proj` (default `q_proj,v_proj`;
  any of `q_proj,k_proj,v_proj,o_proj,gate_proj,up_proj,down_proj`) chooses the
  encoder modules that receive rank-`--rank` adapters; `protocol.target_modules`
  stores the list in canonical order and the evaluator injects exactly those.
  A child run may initialize from a candidate with a subset of its targets: the
  shared adapters restore whole and the added modules start at zero effect
  (B = 0), so the parent's development answers must still reproduce;
  `initialization.parent_target_modules` records the parent's list. Dropping a
  parent target is refused, as is any other adapter geometry change.

`--document-features tokens-dual --dual-layer N` (1..35 for Qwen3-Embedding-4B,
with `--encoder-layer` left unset) gives the reader keys from two layers of one
encoder forward: every active document token from the final normalized state
(exactly the `tokens` rows) followed by every active document token from
`hidden_states[N]`, each row L2-normalized through the same storage channel, so
the key count doubles (about 70 per case) while the question query stays the
final-layer pooled vector. The motivation is the layer trade-off above: final
layer features bind a question to the right document across phrasings but
spell names worse, layer-24 features spell better but bind worse (fresh facts
31/80 against 41/80), and attention over both key sets lets the reader take
binding from one and spelling from the other. The protocol records
`dual_layer` (null unless set); resume and continuation require it equal; a
changed mode or layer in an initialization parent is recorded
(`initialization.parent_document_features`, `parent_dual_layer`) and relaxes the
step-0 reproduction check to the overlap as for any input-geometry change. The
warmup feature cache stores both layers' active-token rows per text and its
manifest carries `document_features` and `dual_layer`, so dual caches never mix
with single-layer ones; the evaluator reads the mode and layer from the
protocol.

Warmup findings (`warmup-sweep/`, `target-readability/noise_result.json`,
`slot-diagnostics/`): the token cross-entropy objective learns the record
characters at a reader learning rate of 2e-4 or below (`--lexical-objective
token-ce`, temperature 0.05, 128 cases per step); 5e-4 oscillates near loss 4
and 1e-3 diverges, while cosine regression toward embedding rows collapses onto
the pad token. The frozen decoder reads gold records perturbed by random noise
down to cosine 0.70 at 64/64, but an exact record whose trailing site is wrong
is answered UNKNOWN on 48/48 facts, and a record whose nearest tokens are all
correct is still refused while letter slots stay ambiguous between several
real tokens. Site and name subword slots are the last to converge.
`--lexical-warmup-cache DIR` encodes every distinct training text once so the
warmup needs no encoder pass; `--lexical-warmup-batch N` processes N cases per
microstep; `--joint-reader-lr` lowers the reader rate for the joint phase;
`--lexical-warmup-schedule cosine` decays the warmup rate; `tokens-question`
additionally exposes the question's token states as reader keys.
`--lexical-name-weight W` (token-ce with the asked-site record) multiplies the
asked name's value slots in the lexical loss, normalized to mean one, because
name subwords are the last slots to converge. A letter-spelled name in memory is
read back letter by letter and never joined by the frozen decoder, so spelling
names is not a usable target.

Development-only results of this recipe (frozen decoder, 16 slots, no source
text in the decoder; `tokens-lexonly-4096` then `tokens-namew-1536`, evaluated
independently by `encoder_lora_eval.py` with eight fresh post-freeze groups):

| candidate | dev 64 | codes 32 | names 16 | pairs 16 | fresh facts 80 | fresh codes 32 | fresh names 32 |
|---|---|---|---|---|---|---|---|
| best pooled run (`paired-qa-256`) | 24 | 4 | 4 | 0 | not reached | | |
| `tokens-lexonly-4096` | 41 | 23 | 2 | 10 | 38 | 21 | 1 |
| `tokens-namew-1536` | 47 | 24 | 7 | 10 | 42 | 22 | 4 |
| `tokens-namew2-3072` (name weight 4, 3072 more steps) | 53 | 31 | 6 | 15 | 41 | 21 | 4 |
| `tokens-enc-128` (encoder adapters trained, 128 live steps) | 57 | 29 | 12 | 14 | 36 | 17 | 3 |
| `tokens-enc-256` (same, 256 steps) | 53 | 29 | 8 | 14 | 39 | 18 | 5 |
| `tokens-l24b-3072` (layer-24 features, 2048+3072 steps) | 48 | 27 | 5 | 12 | 31 | 12 | 3 |
| `tokens-namew2-3072` with `--snap-memory all` | 57 | 30 | 11 | 15 | 50 | 26 | 8 |
| `tokens-l24b-3072` with `--snap-memory all` | 51 | 27 | 8 | 13 | 35 | 14 | 5 |
| `tokens-dual-3072` (final + layer-24 keys, 3072 steps) | 55 | 31 | 8 | 15 | 48 | 26 | 6 |
| `tokens-dual-3072` with `--snap-memory all` | 57 | 30 | 11 | 15 | 52 | 26 | 10 |
| `tokens-copy2-2048` (copy head + pointer loss, from dual) | 55 | 31 | 8 | 15 | 54 | 30 | 8 |
| `tokens-copy2-2048` with `--snap-memory all` | 61 | 31 | 14 | 15 | 56 | 30 | 10 |
| `tokens-copy2-2048` snapped, second seed 7331 | 61 | 31 | 14 | 15 | 57 | 28 | 13 |
| `tokens-copy3-1536` (copy head, low rates) snapped, seed 20261110 | 61 | 32 | 13 | 16 | 56 | 28 | 12 |
| `tokens-copy3-1536` snapped, seed 7331 | 61 | 32 | 13 | 16 | 59 | 30 | 13 |
| `tokens-copy4-v2-3072` (ten-template corpus) snapped, seed 20261110 | 60 | 31 | 13 | 15 | 58 | 30 | 12 |
| `tokens-copy4-v2-3072` snapped, seed 7331 | 60 | 31 | 13 | 15 | 60 | 30 | 14 |
| `tokens-copy4-v2-3072` snapped, third seed 4242 | 60 | 31 | 13 | 15 | 55 | 29 | 10 |
| `tokens-copy5-v3-3072` (grammar corpus, 4,786 skeletons) snapped, seeds 20261110 / 7331 / 4242 | 56 | 31 | 9 | 15 | 49 / 57 / 53 | 26 / 30 / 28 | 7 / 11 / 9 |
| `tokens-copy6-prefix-2048` (document space prefix) snapped, seeds 20261110 / 7331 / 4242 | 61 | 32 | 13 | 16 | 53 / 59 / 55 | 28 / 30 / 29 | 9 / 13 / 10 |
| `tokens-copy7-selfctx-2048` (self-context + pooled context) snapped, seed 20261110 | 59 | 32 | 11 | 16 | 43 | 17 | 10 (held-out 4/16) |
| `tokens-copy7-selfctx-2048` snapped, seed 7331 | 59 | 32 | 11 | 16 | 58 | 26 | 16 (held-out 6/16) |
| `tokens-copy7-selfctx-2048` snapped, seed 4242 | 59 | 32 | 11 | 16 | 54 | 24 | 14 (held-out 8/16) |
| `tokens-copy8-tags-2048` (oracle tags at the reader rate + type-restricted copy) snapped, seed 20261110 | 57 | 32 | 9 | 16 | 53 | 28 | 9 (held-out 0/16) |
| `tokens-copy8-tags-2048` snapped, seed 7331 | 57 | 32 | 9 | 16 | 60 | 30 | 14 (held-out 1/16) |
| `tokens-copy8-tags-2048` snapped, seed 4242 | 57 | 32 | 9 | 16 | 56 | 29 | 11 (held-out 0/16) |
| `tokens-copy9-pooledctx-2048` (pooled context only) snapped, seed 20261110 | 57 | 30 | 11 | 16 | 44 | 20 | 8 (held-out 0/16) |
| `tokens-copy9-pooledctx-2048` snapped, seed 7331 | 57 | 30 | 11 | 16 | 54 | 27 | 11 (held-out 1/16) |
| `tokens-copy11-selfonly-2048` (self-context only, special rows kept) snapped, seed 20261110 | 60 | 32 | 12 | 16 | 56 | 24 | 16 (held-out 6/16) |
| `tokens-copy11-selfonly-2048` snapped, seed 7331 | 60 | 32 | 12 | 16 | 60 | 25 | 19 (held-out 10/16) |
| `tokens-copy11-selfonly-2048` snapped, seed 4242 | 60 | 32 | 12 | 16 | 57 | 25 | 16 (held-out 8/16) |
| `tokens-copy13-loop3-2048` (looped reader, 3 passes, from copy11) snapped, seed 20261110 | 59 | 30 | 13 | 16 | 58 | 26 | 16 (held-out 7/16) |
| `tokens-copy13-loop3-2048` snapped, seed 7331 | 59 | 30 | 13 | 16 | 64 | 26 | 22 (held-out 12/16) |
| `tokens-copy13-loop3-2048` snapped, seed 4242 | 59 | 30 | 13 | 16 | 54 | 22 | 16 (held-out 6/16) |
| `tokens-copy13-loop3-2048` snapped + `--copy-unique-positions`, seed 20261110 | 61 | 30 | 15 | 16 | 66 | 27 | 23 (held-out 9/16) |
| `tokens-copy13-loop3-2048` snapped + unique positions, seed 7331 | 61 | 30 | 15 | 16 | 69 | 26 | 27 (held-out 13/16) |
| `tokens-copy13-loop3-2048` snapped + unique positions, seed 4242 | 61 | 30 | 15 | 16 | 61 | 24 | 21 (held-out 10/16) |
| `tokens-copy13-loop3-2048` snapped + `--copy-contiguous-runs`, seed 20261110 | 64 | 32 | 16 | 16 | 75 | 32 | 27 (held-out 11/16) |
| `tokens-copy13-loop3-2048` snapped + contiguous runs, seed 7331 | 64 | 32 | 16 | 16 | 77 | 30 | 31 (held-out 15/16) |
| `tokens-copy13-loop3-2048` snapped + contiguous runs, seed 4242 | 64 | 32 | 16 | 16 | 76 | 30 | 30 (held-out 15/16) |
| `tokens-copy13-loop3-2048` snapped + `--snap-blank-below 0.80`, seed 20261110 | 58 | 30 | 12 | 16 | 55 | 24 | 15 (held-out 7/16) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs, seed 20261110 | 64 | 32 | 16 | 16 | 75 | 32 | 27 (held-out 11/16) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs, seed 7331 | 64 | 32 | 16 | 16 | 77 | 30 | 31 (held-out 15/16) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs, seed 4242 | 64 | 32 | 16 | 16 | 74 | 29 | 29 (held-out 15/16) |
| `tokens-copy14-loop4-1024` (4 passes, from copy13) snapped, seeds 20261110 / 7331 | 60 | 32 | 12 | 16 | 60 / 69 | 27 / 28 | 17 / 25 |
| `tokens-copy14-loop4-1024` snapped, seed 4242 | 60 | 32 | 12 | 16 | 59 | 25 | 18 (held-out 9/16) |
| `tokens-copy4-v2-3072` snapped + contiguous runs, seed 20261110 | 64 | 32 | 16 | 16 | 68 | 32 | 20 (held-out 4/16) |
| `tokens-copy4-v2-3072` snapped + contiguous runs, seed 7331 | 64 | 32 | 16 | 16 | 67 | 30 | 21 (held-out 5/16) |
| `tokens-copy4-v2-3072` snapped + contiguous runs, seed 4242 | 64 | 32 | 16 | 16 | 65 | 30 | 19 (held-out 3/16) |
| `tokens-copy14-loop4-1024` snapped + contiguous runs, seeds 20261110 / 7331 / 4242 | 64 | 32 | 16 | 16 | 74 / 77 / 75 | 32 / 30 / 30 | 26 / 31 / 29 (held-out 11 / 15 / 14) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs + `--copy-extend-runs`, seeds 20261110 / 7331 | 64 | 32 | 16 | 16 | 75 / 77 | 32 / 30 | 27 / 31 (no change) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs + `--copy-complete-words`, seed 20261110 | 64 | 32 | 16 | 16 | **80** | 32 | 32 (held-out 16/16) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs + word completion, seed 7331 | 64 | 32 | 16 | 16 | 78 | 30 | 32 (held-out 16/16) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs + word completion, seed 4242 | 64 | 32 | 16 | 16 | 77 | 29 | 32 (held-out 16/16) |
| `tokens-copy11-selfonly-2048` snapped + contiguous runs + word completion, **16 fresh groups** (160 cases), seed 20261110 | 64 | 32 | 16 | 16 | 153/160 | 62/64 | 59/64 (held-out 28/32) |
| same, 16 fresh groups, seed 7331 | 64 | 32 | 16 | 16 | 155/160 | 60/64 | 63/64 (held-out 31/32) |
| same, 16 fresh groups, seed 4242 | 64 | 32 | 16 | 16 | 156/160 | 60/64 | 64/64 (held-out 32/32) |
| `tokens-copy17-gate3-1024` (copy11 + gate weight 3, 1024 steps) snapped + contiguous runs + word completion, 16 fresh groups, seed 20261110 | 64 | 32 | 16 | 16 | 153/160 | 62/64 | 59/64 (0 wrong; held-out 28/32) |
| same, seed 7331 | 64 | 32 | 16 | 16 | 153/160 | 60/64 | 61/64 (2 wrong; held-out 29/32) |
| same, seed 4242 | 64 | 32 | 16 | 16 | 158/160 | 62/64 | 64/64 (held-out 32/32) |

Ten training templates (`--phrasing-set v2`) did not transfer to the held-out
sentence: fresh names rose to 12/16 and 14/16 on familiar phrasing but stayed
0/16 on the held-out form, where the memory smears subwords from both documents
and garbles the site tokens even though the site is in the question.

Root cause of the held-out-phrasing failure (`copy-attention-diag/`): the
embedding encoder is causal, so a name that precedes the site and the holder cue
has token states that are bit-identical whatever site follows; the
question-conditioned reader cannot bind such a name to the asked site and drifts
to the distractor's name, while codes, which always follow site and cue, copy
perfectly from the same documents. Controlled variants: any form with site and
cue before the name scores 19-20/21 name subwords, name-first forms 7-9/21.
The document space prefix made sentence-initial names copyable but could not
supply the missing right context.

With the copy head, the remaining fresh-name gap is entirely the held-out
phrasing: on the familiar-template cohort fresh names are 10/16 and 13/16 on the
two seeds, while on the held-out template (owner named before the site) they are
0/16 on both seeds with codes still 14/16. The reader has learned where names
sit in the training phrasings rather than how to find a name; training phrasing
variety is the next lever.

A further name-weighted dual continuation (`tokens-dual2-3072`, weight 4) stays
on the same plateau: snapped fresh facts 52/80 and 51/80 with 9 names on each
seed. Regressing slot vectors onto token embeddings saturates near a third of
unseen names; the copy head below targets that directly.

On a second fresh seed (7331, snapped) the two best candidates are tied: the
dual-layer reader 53/80 (26 codes, 11 names) and the final-layer name-weighted
reader 54/80 (26 codes, 12 names). Across both seeds fresh names are 21/64 and
20/64 respectively; the qualification bar in use is 16/32 fresh names snapped
on both seeds with no increase in wrong values.

Layer-24 features reached a comparable development score after a comparable
budget but generalized worse to fresh facts (31/80, with the held-out-phrasing
cohort at 8/40, i.e. only the missing-fact controls): the final layer's
semantic features matter for binding the question to the right document even
though layer 24 spells better in isolation.
Training the rank-8 query/value adapters during the lexical phase lifted
development names to 12/16 within 128 steps but left fresh names at 3-5/32, so
the development set does not measure name generalization reliably.
Longer name-weighted training lifts development scores but not fresh facts:
fresh names stay at 4/32 while the training probe reaches 13/16 names, so the
remaining name gap is generalization of subword copying, not optimization.
Layer probe (`layer-probe/`): with the same attention probe, token states from
encoder layer 24 of 36 recover 93% of held-out names exactly versus 83% from
the final normalized state, with codes at 100% at every layer; layers 24 to 28
are indistinguishable and the loss of spelling is concentrated in the last four
layers. Intermediate layers carry about one massive-activation token per
document (norm about 7,700 against a median near 100), so per-token
normalization or a global scale is needed before attention. `--encoder-layer 24`
selects that layer for training and evaluation.

Every miss is an abstention; no wrong values were produced, and the sixteen
missing-fact controls stay correct throughout. A joint phase that adds the QA
cross-entropy through the frozen decoder destabilized the reader even at a
1e-5 reader rate (gradient norms in the thousands), so these candidates were
trained on the lexical objective alone. These remain research results on the
synthetic depot corpus, not rollout qualification.

Nearest-token snapping (`snap-test/`, on `tokens-namew2-3072`): replacing every
predicted memory vector by the exact decoder input-embedding row nearest to it
in cosine over the whole 151,936-row table, before the frozen decoder reads it,
lifts development from 53 to 57/64 and fresh facts from 41 to 50/80, and cuts
wrong values from 12 to 9 across both sets. Margin- or cosine-gated partial
snapping was worse than snapping every slot, so `encoder_lora_eval.py
--snap-memory all` applies the all-slots policy to every memory the decoder
reads (development, training probe, structured readout and post-freeze; the
oracle-prefix controls are literal rows already and are not touched). The result
records `snap_memory` on the manifest and each candidate entry, and each row
carries `snapped_text` (the decoded nearest tokens) and `snap_min_top1_cosine`;
the development reload check is skipped because the stored reference was
measured unsnapped. The default `none` leaves the evaluator unchanged. For
serving, this is one matmul of the memories against the embedding table and an
index into it, and the encoder and decoder share one token vocabulary, so the
snapped ids are ordinary decoder token ids.

Gated token copying (`--copy-head`, default off): the encoder and decoder
tokenizers are the same 151,665-id vocabulary, so every value token a record
needs (name subwords, code characters, site tokens) is literally present among
the document token ids; only the label tokens (` code`, ` key`, ` holder`,
` at`, ` UNKNOWN`, padding) are not. The remaining errors are name subwords the
reader regenerates as a wrong neighbour token, so `CopyHead` lets a slot copy
the exact decoder embedding of a document token instead of regenerating it. It
is a small FP32 module applied after the reader: an identity-initialized query
projection, cosine attention over the frozen decoder embeddings of the
concatenated document token ids at temperature 0.05, and a per-slot sigmoid
gate (bias -2, so a fresh head copies about 12% and mostly generates) mixing
the copied row with the generated memory; label tokens are generated through
the gate, value tokens are copied. The gated output is what the lexical loss
and the frozen decoder read, so the token cross-entropy trains the head at the
reader learning rate (it follows the warmup schedule and `--joint-reader-lr`).
The decoder tokenizer tokenizes the documents and the encoder's ids are
asserted equal at startup; a cached warmup (`--lexical-warmup-cache`) stores
each text's ids in `ids.pt` and its manifest carries `copy_head`. The protocol
records `copy_head` and `copy_head_temperature`, resume and continuation
require them equal, each step logs `copy_gate_mean`, resume checkpoints and
exported candidates carry the module (`copy-head.safetensors`,
`candidate.copy_head_sha256`, verified by the candidate proof and the
evaluator). A child with `--copy-head` may initialize from a parent without
one (the head starts fresh and the step-0 reproduction check records the
overlap, as for any input-geometry change); a child without the flag refuses a
parent that has one. The evaluator applies the head identically when a
candidate's protocol declares it, before `--snap-memory`, and records
`copy_head` on the entry and `copy_gate_mean` per row. The serving cost is the
document token ids plus one small attention over their embeddings.

Pointer supervision (`--copy-pointer-weight W`, default 0 = off; needs
`--copy-head` and the `token-ce` lexical objective; `W` within 0..10): the
lexical cross-entropy alone does not train the head. The parent reader already
fits the training records (CE about 0.003), so the gate feels no pressure to
open and the copy attention never becomes selective; when the gate drifted open
anyway, the unselective mixture degraded what the frozen decoder read (dev 57
-> 19). The head is therefore supervised directly from what is known for every
training case: the record's target id per slot and the case's document ids. A
slot is copyable when its target id occurs among the (unpadded) document ids
and is neither the space pad nor a label token (` code`, ` key holder`, ` at`,
` UNKNOWN`, decided on the id so a label that happens to appear in a document
is still generated). The pointer loss is `-log` of the copy mass on the
matching positions (log-sum-exp over the matches minus log-sum-exp over all
valid positions), averaged over copyable slots; the gate loss is BCE of the
gate logit toward 1 on copyable and 0 on other slots, averaged over slots.
`W * (pointer_loss + gate_loss)` joins the training loss next to the lexical
term in both the batched warmup and the live/joint path (`CopyHead
.forward_detailed` exposes the copy and gate logits; `forward` is unchanged).
Each step row logs `pointer_loss`, `gate_loss` and `copyable_fraction` while
it is on. `--copy-head-lr` (default the reader rate) gives the copy-head
optimizer group its own rate, scaled by the warmup schedule like the others
and kept through the joint phase (without it the head follows
`--joint-reader-lr`). Both are recorded in the protocol (`copy_pointer_weight`,
`copy_head_lr`) and resume/continuation require them equal.

Document space prefix (`--document-space-prefix`, default off; protocol
`document_space_prefix`, resume/continuation require it equal): the shared Qwen
tokenizer segments a sentence-initial name differently from the same name
mid-sentence. "Mujapo Manabo is entrusted..." tokenizes as `M`/`uj`/`apo`/
` Man`/`abo` while the lexical record " key holder Mujapo Manabo at ..." has
` Muj`/`apo`/` Man`/`abo`; the copy head copies decoder embeddings of document
token ids, so the record's first subword ` Muj` is absent from an owner-first
document and cannot be copied (0/16 names on the held-out phrasing cohort).
With the flag, every document text (never the question or the lexical record)
is prefixed with one space before tokenization, which restores the record's
segmentation (" Mujapo Manabo is..." -> ` Muj`/`apo`/` Man`/`abo`/` is`). The
prefix applies at the live encoder path, the warmup cache (keys are the
prefixed texts and the manifest records `document_space_prefix`, so caches
never mix), the copy sources, the shared-token-id assertion and the cache
self-check. Initialization records `parent_document_space_prefix` when either
side has it on and a changed prefix relaxes the step-0 reproduction check like
any other input-geometry change. The evaluator reads the field per candidate,
applies the same prefix at its encoder tokenization and copy sources only (the
oracle controls and readout prompts are untouched), and records
`document_space_prefix` on the entry.

Document self-context and pooled context (`--document-self-context`,
`--document-pooled-context`, both default off; protocol `document_self_context`,
`document_pooled_context`, resume/continuation require them equal): the Qwen3
embedding encoder is causal, so a document token's state depends only on the
tokens to its left (`copy-attention-diag`: name-token states are bit-identical
when only the later site changes). A name written before the site and holder
cue of its sentence therefore has states that carry nothing about its site;
the question-conditioned reader cannot bind it (0/16 held-out names, 7-9/21
value slots on every owner-first form, including training template 6) and
drifts to the distractor's name, while codes and sites that follow the cue
copy perfectly (39/39). Both flags are index-time only and may be combined;
the question stays pooled and unchanged, and the decoder still sees no text.
Self-context doubles every (prefixed) document text for the encoder (`D + D`
when the text starts with a space, else `D + " " + D`), asserts that the last
`n` token ids of the doubled text equal the document's own ids
(`self_context_tokens`; a sentence-initial word that re-segments falls back to
the ids of `" " + D`, counted in `result.document_self_context_fallbacks`), and
keeps only those last `n` states as the document's features
(`select_last_rows`, right-padding aware), so every kept token has the whole
document to its left. It applies at the live and batched encoder paths, the
warmup cache (the second copy's states stored under the original text key;
manifest `document_self_context`), the dual-layer pair from the same forward
and the copy sources (the single ids the kept states stand for, one per kept
row, covered by the shared-tokenizer assertion); the 128-token budget applies
to the doubled text (documents are at most 43 tokens). Pooled context adds the
document's own pooled, L2-normalized last-token vector to every token row
before the row's normalization (`add_pooled_context`:
`normalize(normalize(state) + pooled)`), per layer in `tokens-dual`, and is
rejected with `pooled` features; the cached path derives it from the cached
last row and the cache self-check covers it. Initialization records
`parent_document_self_context` / `parent_document_pooled_context` when either
side has one on, and a change relaxes the step-0 reproduction check like the
space prefix. The evaluator reads both fields per candidate, mirrors them at
its encoder, feature and copy-source sites, and records `document_self_context`,
`document_pooled_context` (plus the fallback count) on the entry.

Entity-type tags (`--entity-tags {none,oracle}`, default `none`; protocol
`entity_tags`, resume/continuation require it equal) attack the same causal
blind spot from the other side, GLiNER-style: instead of giving a name more
left context, a bidirectional tagger would mark every entity span at index time
and the reader and copy head would see each token's type next to its causal
state, so a name written before its site and holder cue is still recognizably
a person. The `oracle` mode is the ceiling measurement for that tagger: the
types come from the case's public entity list (site, other site, owner,
distractor), the access-code pattern and the literal "not recorded" marker
(`oracle_entity_tags`: none / site / person / code / missing, one per document
token through the fast tokenizer's offset mapping, never from the QA label), a
learned zero-initialized FP32 table at the encoder width
(`EntityTagEmbedding`, saved as `entity-tags.safetensors` with
`entity_tags_sha256` in `candidate.json`, in resume checkpoints and in the
exporter's averages) adds each token's type vector to its row after the pooled
context and before the final normalization (a fresh table reproduces the
untagged features bit-for-bit), the question row stays untyped, and the table
trains at the reader rate and schedule. Tags apply at the live, batched and
cached paths (`tags.pt` next to the cached states, manifest `entity_tags`) and
in the evaluator (`entity_tags` on the entry). `--copy-restrict-by-type`
(default off; protocol `copy_restrict_by_type`; needs the copy head, tags and a
lexical record) additionally masks the copy head's attention on the record
slots that hold the asked value (`lexical_value_slots`, from the record's
offsets) to tokens of the asked field's type plus the "not recorded" marker
(`copy_type_mask`: person tokens for a name question, code tokens for a code
question), while label, site and pad slots keep every token; the pointer loss
counts only matches inside the allowed set. Initialization records
`parent_entity_tags` / `parent_copy_restrict_by_type` when either side is on;
a child adding tags starts at zero and must reproduce the parent, dropping a
parent's tags is refused, and only a changed restriction relaxes the step-0
check. A real tagger trained on the oracle spans is the next step once the
ceiling is known.

The embedding encoder's tokenizer appends `<|endoftext|>` to every text, so a
document has one more encoder state row than it has token ids: the first
entity-tag run stopped at its cache check ("cached document states and entity
tags differ"). `encoder_special_suffix` measures that count (1 for
Qwen3-Embedding, 0 for the decoder tokenizer), `feature_row_tags` pads a
document's id-aligned tags with the untyped type for those rows, and the copy
head's type restriction keeps the id-aligned tags. The same count had been
missing from the self-context keep: `tokens-copy7-selfctx-2048` kept the last
`n` rows of the doubled document, which is the second copy minus its first
token plus the end-of-text row, one row off the copy-source ids it was meant
to stand for. `self_context_states` now takes the suffix and keeps the whole
second copy plus the special rows; the protocol records
`encoder_special_rows: true` for runs trained this way, the evaluator
reproduces the count-only keep for earlier candidates, and a self-context
parent trained without the flag cannot initialize a child (its features would
move). Dual-layer copy caches already stored the extra row per text, so no
cache needs rebuilding except self-context caches, whose manifest now carries
the flag.

Across the three fresh seeds the self-context candidate answers 10/16/14 of
32 names (held-out phrasing 4/6/8 of 16) against 12/14/10 (held-out 0/0/0)
for `tokens-copy4-v2-3072`, with fewer wrong names (7/8/7 against 14/7/16;
the rest abstain) and every missing-fact control correct, but codes fall to
17/26/24 of 32 from 30/30/29, almost entirely through abstentions on a
blurred site subword. The context features buy name binding at the price of
site precision; the entity-tag and pooled-context-only runs separate the two
effects.

The first oracle-tag run (`tokens-copy8-tags-2048`, tag table at the reader's
rate) ends with its table at row norms 0.02-0.11 of a unit row, so it measures
type-restricted copying rather than typing: fresh names 9/14/11 of 32
(held-out phrasing 0/1/0) and codes 28/30/29 against 12/14/10 (0/0/0) and
30/30/29 for `tokens-copy4-v2-3072`; the restricted copy gate on name
questions falls to 0.56 from about 0.87, so the head copies less rather than
more precisely. `--entity-tag-lr` gives the table its own rate for the
retest.

Looped readout (`--reader-loops {1..4}`, default 1; protocol `reader_loops`,
with `--slot-refiner-lr` / protocol `slot_refiner_lr` for the refiner's own
cosine-scheduled rate, default the reader's) runs the pinned reader forward
`reader_pass` more than once. The reader fills its slots in one parallel
shot, so nothing stops two slots from copying the same name subword or
skipping one, which is the scrambled-name failure ("Galuna Sununa" for
"Sunipo Galuna"). Between passes a `SlotRefiner` (LayerNorm, causal
self-attention over the previous pass's final normalized slots, and a
zero-initialized projection, saved as `slot-refiner.safetensors` and hashed
as `slot_refiner_sha256`) adds its feedback to the learned queries, so slot
`i` of the next pass conditions on what slots `<= i` held. The zero
projection makes every added pass identical to the first, so a child adding
loops to a loop-less parent reproduces it exactly at step 0; a parent's
refiner restores into a child with at least as many loops, and fewer loops
than the parent are refused. The evaluator and selector honor the field.

Self-context alone (`tokens-copy11-selfonly-2048`, keeping the encoder's
special rows) answers 16/19/16 of 32 fresh names across the three seeds
(held-out phrasing 6/10/8 of 16) against 12/14/10 (0/0/0) for
`tokens-copy4-v2-3072`, with every missing-fact control correct and snapped
development 60/64; it is the first candidate to clear the 16-of-32 name bar
on more than one seed. Codes settle at 24/25/25 of 32: the misses are a site
slot misspelled next to a correct code (the decoder abstains on the site
mismatch) or a code with its own prefix digit duplicated into a later digit
slot ("Cn7-007452" for "Cn7-002452"), both readout failures of slots produced
in one parallel pass, which the looped reader (`--reader-loops`) targets.

The looped reader (`tokens-copy13-loop3-2048`: three shared passes with a
zero-initialized causal slot refiner, continued from the self-context
candidate) answers 16/22/16 fresh names (held-out phrasing 7/12/6) and
26/26/22 codes across the three seeds against 16/19/16 (6/10/8) and
24/25/25 for its parent: a clear gain on names on one seed and parity on the
others, codes up on two seeds and down on the third, where the misses are
again misspelled sites beside correct codes. Total 58/64/54 against
56/60/57.

Two inference-time rules on the copy head need no retraining.
`--copy-unique-positions` (`copy_with_unique_positions`) forbids two copying
slots from reading one document token position, re-pointing the less
confident slot to its best unclaimed position: on the looped reader it lifts
fresh names from 16/22/16 to 23/27/21 of 32 (held-out 9/13/10) and codes from
26/26/22 to 27/26/24, fixing 16 names and 3 codes over the three seeds and
breaking none, with development at 61/64. `--copy-contiguous-runs`
(`copy_with_contiguous_runs`) is the stronger form, each run of copying slots
reading one contiguous source span, aimed at digit swaps inside a code that
uniqueness cannot catch. `--snap-blank-below` blanks low-confidence snapped
slots to the pad token so that an uncertain value abstains instead of
answering wrongly.

Contiguous-span decoding is the largest single step of the series: on the
looped reader it reaches 75/77/76 of 80 fresh facts with no wrong code on any
seed (codes 32/30/30, names 27/31/30, held-out phrasing 11/15/15, development
64/64 on every seed, every missing-fact control correct, counterfactual pairs
16/15/15), fixing 15 codes and 17 names over the unique rule and breaking
none. The eleven remaining misses are run-boundary errors: a name truncated
by its last subword where the final slot's gate fell below the copy threshold,
one name whose first subword was taken by the label slot, and code
abstentions beside a truncated site. Blanking low-confidence slots
(`--snap-blank-below 0.80`) is harmful (55/80 on the first seed) and was not
pursued.

Under contiguous-span decoding the looped reader no longer matters: the
self-context candidate without it (`tokens-copy11-selfonly-2048`) scores
75/77/74 against the looped reader's 75/77/76, producing identical snapped
memory text on 77-79 of 80 cases per seed. The constrained decode resolves
the slot-ordering errors the loop was trained to remove, so the simpler
candidate is the deliverable.

The same decode on the single-pass candidate `tokens-copy4-v2-3072` gives
68/67/65 with held-out-phrasing names at 4/5/3 of 16 against copy11's
75/77/74 and 11/15/15: constrained decoding repairs slot order for any
copy-head candidate, but only self-context lets the causal encoder bind a
name written before its site, so the deliverable needs both.

Session summary (2026-11-11/12). Starting point `tokens-copy4-v2-3072`
snapped: fresh 58/60/55 of 80, names 12/14/10 of 32, held-out phrasing
0/0/0. Deliverable `tokens-copy11-selfonly-2048` with `--snap-memory all
--copy-contiguous-runs`: fresh 75/77/74, codes 32/30/29 with no wrong code,
names 27/31/29, held-out phrasing 11/15/15, development 64/64, every
missing-fact control correct, counterfactual pairs 16/15/14. Two ingredients
carry the gain, encoder self-context (binding) and contiguous-span decoding
(readout); pooled context, entity tags (at any tag rate) and the looped
reader (three or four passes) are harmful or redundant on top of them.
Remaining misses are run-boundary errors at the copy gate, the next target.

Word-boundary completion (`--copy-complete-words`, `complete_copied_words`)
closes the run-boundary misses: after snapping, every copied span is extended
through the source document until it starts and ends on a word boundary and
later slots shift into the trailing pads. On `tokens-copy11-selfonly-2048`
with contiguous-span decoding it reaches 80/78/77 of 80 fresh facts, names
32/32 on every seed (held-out phrasing 16/16), codes 32/30/29 with no wrong
answer, development 64/64. The five remaining misses are code abstentions
only: one pair whose memory text is exactly right yet the frozen decoder
declines, and sites truncated by the 16-slot budget, which the 24-slot
continuation targets. Extending runs by a low-gate neighbour
(`--copy-extend-runs`) changes nothing on two seeds and two names on the
third.

Wider fresh challenge (`--fresh-groups 16`, 160 cases per seed, 480 in all)
for the deliverable: 153/155/156 of 160, 464/480 (96.7%), 5 wrong (four names
on one seed where the " at" label slot's gate rose and glued the name and
site runs into one span, one code with a doubled digit) and 11 abstentions
(six sites cut by the 16-slot training record, two of the decoder declining
a fully correct record, three mis-spanned sites); names 186/192, codes
182/192, every control correct. Four further decoding rules were tried on
this set and rejected: a boundary-aligned span preference (no change), a
label-slot split keyed on the reader's raw memory (fragments value runs,
144/160), a split at argmax jumps (98/160: per-slot positions are too noisy
to carry structure), and dropping a cut site span (the dangling label makes
the decoder abstain, 143/160). The remaining glued-label names and cut sites
are training-side issues: a gate target that keeps label slots from copying,
and a record format that fits long sites or omits the trailing " Depot".

Training-side follow-ups. A code is ten decoder tokens, so " code {code} at
{site}" with a four-token site and " Depot" is 17 tokens and the 16-slot
record loses " Depot"; the `asked-site-bare` record drops the " code" label
to fit, but the frozen decoder reads a label-less code as a missing fact
(development codes 7/32 after 512 steps with the lexical loss converged), as
the target-readability probe had already found for bare values, so that
format is withdrawn and the six cut-site abstentions stand as a limit of the
16-slot budget. `--copy-gate-weight` multiplies the gate BCE inside the
pointer term for the rare label slot whose gate rises and glues two runs.

The gate-weighted continuation (`tokens-copy17-gate3-1024`) scores 153/153/158
on the wide challenge against copy11's 153/155/156 (464 each of 480): on one
seed the four glued-label wrong names become abstentions, on another two
new label-slot copies appear, and the third gains two codes. Wrong answers
fall from five to two (codes 1 -> 0, names 4 -> 2) and abstentions rise from
eleven to fourteen, so the weighting shifts the residual toward abstention
without changing the total; both candidates are equivalent on accuracy, and
copy17 is the chosen deliverable because a wrong value costs more than a
missing one in a retrieval setting. Decode it with `--snap-memory all
--copy-contiguous-runs --copy-complete-words`.

### Native export

`encoder_lora_export.py` turns the chosen candidate into the three inputs the
Zig port (`docs/plans/memorilla-copy17-native.md`, section 5.1) consumes,
plus fixtures that record every boundary of the evaluator's own decode path.
Nothing it writes is a release artifact; the copy head ships unchanged
(`copy-head.safetensors`, three F32 tensors).

```sh
B=.benchmark-results/memorilla/encoder-lora-next
A=/path/to/.benchmark-assets/memorilla
S=zig/pkg/inference/scripts/memorilla/encoder_lora_export.py
# Base encoder shards with W + (alpha/rank) * B @ A folded into every
# layers.N.self_attn.{q,v}_proj.weight (FP32 merge, stored F32; every other
# tensor and side file copied byte for byte, dot-files such as the source's
# managed-download marker skipped because they bind the original shard
# bytes). Writes merge.json with per-tensor max |delta|, then the managed
# research sidecars described below. The source must be the released
# catalog encoder (canonical model_manifest.json, base_models.json revision).
python $S merge-encoder --candidate $B/tokens-copy17-gate3-1024 \
  --encoder-f32 $A/managed-f32/encoder --output $B/lora-merged-encoder
# Rewrite only the sidecars of an existing merged directory (shards are
# hashed, never rewritten); --candidate is optional and cross-checked.
python $S merge-encoder --output $B/lora-merged-encoder --sidecars-only
# reader.safetensors -> non-release split-q/k/v bundle whose execution
# contract declares encoder_pooling per-token-dual-layer-l2-normalized-v1
# and which carries copy-head.safetensors, candidate.json and protocol.json
# from the candidate, pinned in manifest.json `files`.
python $S export-reader --candidate $B/tokens-copy17-gate3-1024 \
  --output $B/lora-reader-bundle
# Case lists: the 64 development cases and the evaluator's post-freeze
# challenge (--fresh-groups 8 --fresh-seed 20261110 -> 80 cases).
python $S dev-cases --data $B/data-paired-v2 --output $B/lora-ref-dev.json
python $S fresh-cases --data $B/data-paired-v2 --groups 8 --seed 20261110 \
  --prior-training-data $B/data-spelled --prior-training-data $B/data-paired \
  --prior-training-data .benchmark-results/memorilla/encoder-lora-20261110/data \
  --output $B/lora-ref-fresh.json
# GPU: the evaluator's copy17 path (--snap-memory all --copy-contiguous-runs
# --copy-complete-words) through the same helper functions, one fixture
# directory per case list: reference.json + reference.safetensors with
# <case>.{docs,query,memories,copy_output,gate,logits} in F32.
python $S reference --candidate $B/tokens-copy17-gate3-1024 \
  --cases $B/lora-ref-dev.json --output $B/lora-ref-dev --assets $A
```

`--assets` expands to `models/qwen3-embedding-4b`, `models/qwen3-8b`,
`checkpoints/stage3` and `upstream`; `--encoder`, `--decoder`,
`--checkpoint` and `--upstream-source` override each. The reference command
waits for an idle GPU before loading. `test_encoder_lora_export.py` covers
the merge bytes, the sidecar contents and `--sidecars-only` idempotence, the
non-release bundle contract with its pins and the fixture round trip on
synthetic tensors.

Managed research sidecars (`merge-encoder` writes them after `merge.json`,
in this order; `--sidecars-only` replaces them byte-deterministically):

- `memorilla_lock.json`: `{"schema": "antfly.memorilla.research-encoder-lock.v1",
  "candidate_sha256": <sha256 of candidate.json>, "base": {"model", "revision"}
  (from `base_models.json`), "files": [{"path", "size_bytes", "sha256"}]}`
  sorted by path over every regular non-dot file except the sidecars: the
  two shards, `model.safetensors.index.json`, `merge.json`, `config.json`,
  the tokenizer files and the sentence-transformers side files
  (`1_Pooling/config.json`, `modules.json`, ...). Same `files` shape as the
  v1 lock the Zig qualification code already verifies.
- `model_manifest.json`: the catalog's canonical encoder manifest with
  `"memorilla_profile":"qwen3_encoder_lora_research_f32_v1"` in place of
  `qwen3_encoder_f32_v1`, byte for byte (`RESEARCH_ENCODER_MANIFEST`,
  sha256 `44ada04c47c6e91784346ba7bcf7ae80b0aa55467870fab2077d4885af5625ae`).
  The Zig research profile compares the file against this exact string; the
  offline Qwen3 embedding reference session must learn the tag before it can
  open the directory again.
- `.antfly-download-complete.json`: version-1 managed receipt
  (`registry/managed_receipt.zig`) whose `artifacts` pin every regular
  non-dot file with `path`, `size` and `sha256`, including
  `model_manifest.json`, `memorilla_lock.json` and `merge.json`; its `source`
  block names the base model (`owner`, `name`), the research variant
  `memorilla-encoder-lora-research-f32-v1`, the base revision and a `merge`
  sub-block with the candidate, adapter, `merge.json` and lock digests.
  Version 1 keeps `managedDownloadMatchesSource` from mistaking the
  directory for a Hub download. The receipt is written last, so the
  publication generation (sha256 of the receipt) covers every sidecar.

The reader bundle holds `manifest.json`, `config.json`, `memory.safetensors`,
`copy-head.safetensors`, `candidate.json` and `protocol.json`; all five data
files are pinned in `manifest.json` `files` and `source.candidate_sha256`
equals `files["candidate.json"].sha256`, which the Zig side cross-checks
against the encoder lock's `candidate_sha256`. `exporter.load_bundle` verifies
every pin; release bundles still carry exactly `config.json` and
`memory.safetensors`.

### Native verification of the research candidate

`antfly-memorilla-lora-verify` runs the exported candidate end to end on CUDA
in strict F32 and compares every stage with the fixtures above: document rows
and query (features), reader memories, copy-head logits/gate/output, spans,
snapped and completed ids, prompt ids, generated ids and the answer text.
The merged encoder loads through the offline Qwen3 embedding reference
session (its `model_manifest.json` carries the research profile tag
`qwen3_encoder_lora_research_f32_v1`), the reader bundle through
`Bundle.open(.., allow_synthetic = true)`, and the decoder through the
managed F32 profile.

```bash
cd zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" \
  zig build build-memorilla-lora-verify -Dcuda=true -Doptimize=ReleaseFast -j2
B=../../../.benchmark-results/memorilla/encoder-lora-next
./zig-out/bin/antfly-memorilla-lora-verify \
  --candidate $B/tokens-copy17-gate3-1024 --reader-bundle $B/lora-reader-bundle \
  --encoder $B/lora-merged-encoder --decoder <managed-f32 decoder dir> \
  --reference $B/lora-ref-dev --output $B/lora-verify-dev-summary.json > $B/lora-verify-dev.jsonl
```

Flags: `--max-cases N`, `--skip-decoder`, `--reference-features` (feed the
fixture rows to the reader; isolates reader/copy/decoder), `--reference-memories`
(feed the fixture memories to the copy head), `--atol/--rtol` for the stage
diagnostics. Output is JSON lines (`ready`, one `case` per fixture with stage
statistics, id agreement flags and per-stage milliseconds, `complete`).

Results on 2026-11-12: dev 64/64 identical answers to Python (all correct);
fresh challenge 80 cases, 79 identical answers, Zig 78 correct against
Python's 79 (one gate at the 0.5 threshold shortens a site run without
changing the answer; one cut-site record decodes as UNKNOWN in F32 where the
BF16 decoder returned the code). See `docs/plans/memorilla-copy17-native.md`.

### Managed research profile verification

`antfly-memorilla-lora-serve-verify` drives the actual managed service
(`memorilla_research_service`, the same code the HTTP handler calls for a
profile with `reader_kind: "encoder_lora_research"`) through a real
`ModelManager` over the reference cases: the merged encoder loads as a
managed research publication (receipt + `memorilla_lock.json` +
`memorilla_profile: qwen3_encoder_lora_research_f32_v1`), the reader bundle
through the research reader cache, and every request must refund its
admission.

```bash
cd zig/pkg/inference
zig build build-memorilla-lora-serve-verify -Dcuda=true -Doptimize=ReleaseFast -j2
B=../../../.benchmark-results/memorilla/encoder-lora-next
./zig-out/bin/antfly-memorilla-lora-serve-verify --encoder $B/lora-merged-encoder \
  --decoder <managed-f32 decoder dir> --bundle $B/lora-reader-bundle \
  --reference $B/lora-ref-dev --budget-gib 112 --output $B/lora-serve-dev-summary.json > $B/lora-serve-dev.jsonl
```

`--budget-gib` is the process cap; the shared unified admission limit is 75%
of it and the two F32 models plus one request need about 73 GiB, so 96 GiB
is not enough. Output is JSON lines (`ready`, `case` with answer agreement,
identities, candidate/module digests and stage timestamps, `retained`,
`complete`). The serving contract (config, request, response, limits) is in
`docs/design/inference/memorilla-serving.md`, "Research profile".

### EmbeddingGemma 2 / Gemma 4 pair

`gemma_memory.py` moves the copy17 recipe to a bidirectional 512-wide encoder
(EmbeddingGemma 2, BF16) and a frozen Gemma 4 E4B decoder that reads the
snapped record as plain text; plan and step-1 probes are in
`docs/plans/memorilla-gemma-pair.md`. It reuses the `encoder_lora.py` helpers
(`CopyHead`, `copy_with_contiguous_runs`, `MemoryTokenSnapper`,
`complete_copied_words`, the `asked-site` record, the token-CE lexical recipe,
pointer and gate losses, the counterfactual-group sampler and the cosine
schedule) and the pinned upstream `MemoryModule`, built fresh at
`embedding_dim 512 / output_dim 2560 / 8 heads / 2 cross-attention blocks`.
Schema `antfly.memorilla.gemma-pair-research.v1`; nothing here is a serving
artifact. It needs transformers 5.x (tested 5.22.2 with torch 2.9.1+cu130).

What differs from the Qwen trainer, all recorded in `protocol.json`:

- Features: per-token rows of the text model's final RMS-normalized state
  (512, before the 768 embedding projection, captured by a forward hook on
  `language_model.norm`) plus the rows of `hidden_states[--tap-layer]`
  (default 16 of 24, hook on `layers[tap-1]`), each L2-normalized through the
  FP16->BF16 channel; final rows first, then tap rows, so the reader sees
  `[1, 2M, 512]`. The tokenizer wraps `<bos> ids <eos>`; the BOS row is
  dropped and the EOS row kept per document (`encoder_special_rows`).
  Documents get the copy17 space prefix, no self-context (the encoder is
  bidirectional). The question query is the mean of its final-norm rows over
  the attention mask (the model's own pooling at 512).
- Copy sources are the Gemma 4 input-embedding rows (2560) at the document
  ids; both tokenizers are checked to agree on every training and evaluation
  document (`shared_document_ids_checked`). Copy head temperature 0.05.
- Record: `asked-site`, tokenized by the Gemma 4 tokenizer, padded with the
  single space token (id 236743 in both vocabularies) to `--memory-slots`
  (16, 24 or 32; default 24, since no decoder was trained on a 16-token
  frame). Losses: lexical token-CE (tau 0.05, name weight 4) plus pointer and
  gate losses (gate weight 3). Reader, copy head and encoder q/v LoRA
  (rank 8, alpha 16) train together, live encoding every step (no feature
  cache); AdamW, cosine schedule, grad clip 1.0, BF16 autocast for the
  encoder, FP32 trainable weights. `--memory-scale-init lexical-target`
  folds the teacher row norm into the fresh projection once.
- Decoding: contiguous runs, snap every slot, complete copied words, then the
  decoder reads `tokenizer.decode(ids)` inside one user turn of the Gemma 4
  chat template: `SYSTEM + "\nMemory" + record_text + "Memory\n\n" +
  question`, greedy, 24 new tokens, stop on the decoder's EOS ids
  (`<eos>`, `<turn|>`). Scoring is the Qwen evaluator's exact match.

```sh
P=.venv-gemma/bin/python
S=zig/pkg/inference/scripts/memorilla/gemma_memory.py
B=.benchmark-results/memorilla/encoder-lora-next
G=.benchmark-results/memorilla/gemma-pair
# Decoder readability check: the plan's six records (expect 6/6).
$P $S probe --output $G/probe/probe.json
# Training (defaults: 2048 steps, batch 16 x accum 8, eval every 256,
# tap 16, 24 slots, reader/copy-head lr 2e-4, LoRA lr 1e-4).
$P $S train --data $B/data-paired-v2 --output $G/pair-tap16-slots24-2048
# Development split (64 cases) and the wide fresh challenge, comparable to
# copy17 when the same prior corpora are passed.
$P $S eval --candidate $G/pair-tap16-slots24-2048 --output $G/eval-dev
$P $S eval --candidate $G/pair-tap16-slots24-2048 --fresh-groups 16 \
  --fresh-seed 20261110 --prior-training-data $B/data-spelled \
  --prior-training-data $B/data-paired \
  --prior-training-data .benchmark-results/memorilla/encoder-lora-20261110/data \
  --output $G/eval-wide-seed20261110
```

`train` writes `protocol.json`, `result.json` (per-step losses, gate means,
grad norms, step seconds, peak memory; dev evaluations with per-row answers
and the snapped record of every case), `resume.pt` at every evaluation
(`--resume` continues an identical protocol) and, on completion,
`reader.safetensors`, `encoder-lora.safetensors`, `copy-head.safetensors` and
`candidate.json` with their digests. `eval` verifies those digests and the
base-model identities, then writes `result.json` with `scores` and per-row
answers, record text, spans, gates and snap cosines. The encoder and decoder
directories default to the local BF16 assets (`--encoder`, `--decoder`,
`--upstream-source` override them); the trainer refuses a GPU that already
runs a compute process unless `--allow-shared-gpu` is given. Tests:
`test_gemma_memory.py` (CPU, fake tokenizers and small tensors).

### Summarizing reader: soft slots through the frozen Gemma 4

`gemma_soft_memory.py` is Memorilla's own training recipe on the
EmbeddingGemma 2 / Gemma 4 E4B pair: the reader's memory slots are trained
THROUGH the frozen decoder on Memorilla-Data (Stage 1 `enwiki`
reconstruction, 1,979,809 passages; Stage 2 the ten-config QA/summarization
mixture), with the copy17 copy head kept on a tail of COPY slots for exact
strings. Schema `antfly.memorilla.gemma-soft-memory-research.v1`; it imports
the encoder taps, feature assembly, reader builder and model loader from
`gemma_memory.py` and the copy head, pointer losses, contiguous-run copying and
snapper from `encoder_lora.py`. Nothing here is a serving artifact.

Design decisions (all in `protocol.json`):

- Data: parquet shards of
  `.benchmark-assets/memorilla/data/memorilla-data/data/<config>/<split>-*.parquet`
  read lazily one row group at a time (`ParquetSplit`); the epoch order is a
  deterministic row-group-local shuffle per config, interleaved uniformly at
  random across configs (`mixture_order(seed)`), so the data cursor in
  `resume.pt` resumes on the identical sequence. `--datasets cfg[:cap],...`
  overrides a recipe's configs. Documents get the copy17 space prefix, are cut
  to `--doc-tokens` (256) encoder tokens, at most `--max-docs` (16) per
  example (when there are more: top-k by cosine of the pooled final-norm
  document vector to the pooled question, original order kept), and the
  feature rows are capped at `--max-rows` (4096, both layers counted) by
  dropping trailing documents and truncating the last kept one. Features are
  exactly `gemma_memory.py`'s (final-norm 512 rows + layer-16 tap rows, L2 +
  FP16->BF16 channel, mean-pooled question) and computed live; the encoder is
  frozen unless `--encoder-lora` (rank 8 q/v).
- Slots: `--memory-slots` (24) upstream `MemoryModule` queries; the first
  `K - H` are SOFT (trained only by the decoder loss), the last
  `H = --copy-slots` (8) are COPY slots with the `CopyHead`. When the answer
  occurs verbatim as a token span of a document (space-prefixed then bare
  tokenization, length <= H) the gate target is 1 on the span's slots
  (left-aligned) and the pointer target the span positions; otherwise the
  gate target is 0 and there is no pointer loss (Stage 1 passages never fit,
  so the gate closes during Stage 1 and reopens on Stage 2 QA data). In
  training the gate-mixed soft output feeds the decoder; at inference the
  slots of a contiguous run (gate >= 0.5) snap to their nearest decoder token
  and the other copy slots pass as soft rows.
- Prompt: the Gemma 4 chat template DOES render a `system` role
  (`<|turn>system\n...<turn|>`), so the upstream layout is kept: system =
  `"You are a helpful assistant." + "\nMemory" + <|image|> * K + "Memory"`,
  user = question, assistant = answer. The placeholder is the image soft
  token `<|image|>` (id 258880, `config.image_token_id`): a single token that
  Gemma 4 was trained to read soft rows at, whose per-layer (PLE) identity
  the HF multimodal path takes from the pad token (id 0); the trainer mirrors
  that. Labels cover the answer tokens plus the closing `<turn|>` (the
  template's trailing newline is dropped); answers are cut to
  `--max-answer-tokens` (256) and prompts longer than `--max-tokens` (1024)
  are skipped (counted in `skipped`).
- Injection: transformers 5.22's `Gemma4TextModel.forward` and
  `Gemma4Model.forward` refuse `input_ids` together with `inputs_embeds`, so
  the trainer computes `inputs_embeds = embed_tokens(input_ids)`
  (`Gemma4TextScaledWordEmbedding`, rows already scaled by sqrt(2560); the
  reader output lives in that space and `--memory-scale-init lexical-target`
  rescales the fresh projection once to the mean scaled token norm, 56.5),
  scatters the memory rows into the placeholder positions, and passes
  `per_layer_inputs = get_per_layer_inputs(input_ids with placeholders ->
  pad)` explicitly. The loss is the mean CE over labeled tokens, computed by
  applying `lm_head` and the final logit softcap (30) only at labeled
  positions. The decoder runs under gradient checkpointing in train mode
  (dropout is 0); its parameters stay frozen (`assert_frozen`).
- Optimization: AdamW (wd 0.01), lr 3e-4, linear warmup `--warmup-steps`
  (500, clamped to a tenth of short runs) then cosine to 10%, grad clip 1.0,
  `--batch` x `--accum` (4 x 32 = 128 effective), one epoch per recipe
  (`--max-steps` caps smokes). Loss = CE + 1.0 pointer + 3.0 gate. Checkpoints
  every `--save-every` (1000) steps, validation loss on the first 256 rows of
  each config's validation split every `--eval-every` (2000) and at the end;
  JSONL events (`start`, `memory-scale-init`, `step`, `evaluation`,
  `checkpoint`, `completed`) on stdout with explicit flushes. Artifacts:
  `protocol.json`, `result.json`, `resume.pt`, `reader.safetensors`,
  `copy-head.safetensors` (`encoder-lora.safetensors` with `--encoder-lora`),
  `candidate.json`.
- Recipes: `--recipe stage1` (enwiki train), `--recipe stage2 --init DIR`
  (the ten-config mixture from the Stage 1 candidate or an interrupted run's
  `resume.pt`), `--recipe stage2-replay --init DIR` (the same ten configs plus
  `enwiki:200000` restatement replay, plan section 5.1, about 13% of the
  mixture; implies the three paper options below unless overridden and
  records the mixture in `protocol.json`), `--recipe depot` (Stage-3 analog
  on the depot corpus) is a stub that exits with a message.
- Paper options (arXiv 2610.21375v3, plan section 6; all OFF by default so a
  run started without them keeps a byte-identical `protocol.json`; the
  protocol records them only when one is on or the recipe is a preset):
  - `--chunking sentence128`: each space-prefixed document is cut into
    sentence pieces (terminal punctuation plus closing quotes before
    whitespace, or the last character before a line break; the pieces
    concatenate back to the document) and packed sentence-greedily into
    chunks of at most 128 encoder tokens (the paper's TriviaQA rule). A
    sentence longer than 128 tokens is cut at 128 with its remainder left
    open for the next sentences; chunk order is kept and chunks never span
    documents. Replaces the `--doc-tokens` truncation (with the token stream
    alone, chunks then go through the `--max-docs` selection like documents).
  - `--reader-streams dual`: the reader keys become `[pooled rows; token
    rows]`. Pooled rows: one per chunk for every chunk of the example, the
    mean of its final-norm 512 states over the attention mask (the question
    query's own pooling), L2 + feature channel; in training, above
    `--max-chunks` (512) the lowest pooled cosines to the question are
    dropped (counted in `chunks_dropped`); `eval` and `probe` read the whole
    collection unless `--max-chunks` is given (a cap there turns the pooled
    stream back into a hard top-k). Token rows: the existing final-norm + tap rows of the
    top `--token-chunks` (8) chunks by that cosine, in rank order (ties by
    chunk index), capped by `--max-rows`; copy sources and copy targets
    cover exactly those chunks' tokens in that order. A trainable
    `stream_type` table [2, 512] (zero init, FP32, own optimizer group) is
    added to pooled (row 0) and token rows (row 1) before the reader and
    saved as `stream-type.safetensors` (in `resume.pt` as `stream_type`).
    Every slot reads the union of keys (`slot_routing: union`; the Zig
    reader takes a per-key mask only, so no per-slot masks). One frozen
    encoder pass over all chunks of a micro-batch (batches of
    `--encoder-chunk-batch`, 64) gives both streams; only the running top
    chunks' token states are kept, so memory stays bounded for NarrativeQA
    books (~1,700-2,600 paragraphs). Not combinable with `--encoder-lora`
    (the pooled store must stay on the frozen encoder). `--init` from a
    Stage 1 run directory or candidate loads the reader and copy head and
    zeroes the new `stream_type` (logged in the `init` event and
    `init_new_tensors`).
  - `--reader-dropout P`: dropout of the reader's attention weights,
    residual attention output and feed-forward blocks, train mode only (the
    paper's 0.1; legacy 0).
  - Resume: `protocol.json` keeps the digest of the script that started the
    run; resuming with a changed script is allowed when everything else is
    identical and is recorded in `result.json` `script_resumes`. The script
    that started `$G/stage1` (sha256 `908a1149...`) is kept at
    `$G/stage1-script/gemma_soft_memory.908a1149.py`.
- Evaluation (`eval`): `--configs` takes any Memorilla-Data config with a
  validation split, plus `narrativeqa` (its test split with `--split auto`,
  the paper's protocol) and `enwiki`. Greedy limits are the paper's: 64 new
  tokens for QA (`--max-new-tokens`) and 512 for enwiki reconstruction
  (`--long-form-new-tokens`). Metrics come from the hash-pinned upstream
  `memorilla.metrics` (revision `d8f18891`, `metrics.py` sha256 `075d4942...`,
  executed with the pinned `benchmarks.py` and literal constants of
  `data.py`/`paths.py`, no upstream package import): `clean_generation` on
  the prediction, then per row `exact_match`, `token_f1` (lower-cased
  whitespace tokens, the paper's F1) and `rouge` (ROUGE-1/L F-measure with
  Porter stemming), and corpus `bleu` (BLEU-1/4) per dataset; `squad_f1`
  (SQuAD-normalized F1, this trainer's earlier metric) is kept alongside.
  `.venv-gemma` has `rouge-score==0.1.2` and `nltk==3.10.4` for this
  (installed with uv, cache in `zig/pkg/inference/.zig-cache/memorilla-uv`;
  the same versions as `.venv-memorilla-score`). Without the upstream tree
  or those packages the report's `metrics` field names the local fallback
  (same EM and token F1, ROUGE without stemming, no BLEU). METEOR is not
  computed (it needs the NLTK wordnet download). NarrativeQA adds ROUGE-L
  and token F1 by question first word (who, which, what, why, how, where,
  when, other: the paper's Figure 4a).
  Dual-stream evaluation caches each collection's chunk ids and pooled rows
  (one NarrativeQA book serves ~30 questions): in memory within a run and,
  with `--feature-cache DIR`, as
  `DIR/<key>/<config>/<split>/<collection_id>.safetensors` (pooled rows in
  BF16, lossless after the feature channel; flat chunk ids plus offsets;
  metadata with the documents digest, which must match). `<key>` digests the
  chunking rule, chunk size, pooled-row recipe and the encoder's weights and
  tokenizer sha256 (`key.json` beside the entries). Token rows of the top
  chunks are recomputed per question (one small encoder batch).
  `--noise-documents` replaces every reader key row and copy-source row with
  Gaussian noise of the same per-row norm (the paper's sanity test; the copy
  sources are noised too because they also carry the documents);
  `--report-doc-tokens` reports document tokens per query (decoder side: the
  K memory slots; the untruncated collection; the compression ratio). Each
  dataset also reports chunks, pooled rows and token rows per query.

```sh
P=.venv-gemma/bin/python
S=zig/pkg/inference/scripts/memorilla/gemma_soft_memory.py
G=.benchmark-results/memorilla/gemma-soft
export CUDA_CACHE_PATH=$PWD/zig/pkg/inference/.zig-cache/cuda-cache
export TRITON_CACHE_DIR=$PWD/zig/pkg/inference/.zig-cache/triton-cache
# Plumbing probe (fresh reader) or round trip of a trained candidate on one
# enwiki validation passage: prints the generated text next to the passage.
$P -u $S probe --config enwiki --index 0 --max-new-tokens 256 --output $G/probe.json
$P -u $S probe --candidate $G/stage1 --config enwiki --index 0
# 20-step smoke (step seconds, tokens per step, peak memory in the step events).
$P -u $S train --recipe stage1 --batch 4 --accum 2 --max-steps 20 \
  --eval-every 20 --save-every 20 --skip-initial-eval --output $G/smoke/stage1-b4a2
# Stage 1 (one epoch of enwiki, effective batch 128) and Stage 2 from it.
mkdir -p $G && setsid nohup $P -u $S train --recipe stage1 --batch 8 --accum 16 \
  --output $G/stage1 > $G/stage1.log 2>&1 &
setsid nohup $P -u $S train --recipe stage2 --init $G/stage1 --batch 8 --accum 16 \
  --output $G/stage2 > $G/stage2.log 2>&1 &
# Stage 2 with enwiki replay, sentence chunks, dual streams and reader
# dropout 0.1 (the planned Stage 2; batch 4 x accum 32 = 128).
setsid nohup $P -u $S train --recipe stage2-replay --init $G/stage1 --batch 4 \
  --accum 32 --output $G/stage2-replay > $G/stage2-replay.log 2>&1 &
# Resume an interrupted run (identical arguments plus --resume).
# Exact match / token F1 (SQuAD style), ROUGE-1/L and validation loss, greedy
# 64 tokens (512 for enwiki), hard copy slots snapped, 200 seeded rows per
# config; NarrativeQA by question type; the noise-document sanity test.
$P -u $S eval --candidate $G/stage2-replay \
  --configs squad_v2,drop,triviaqa,narrativeqa,enwiki --limit 200 \
  --report-doc-tokens --feature-cache $G/feature-cache --output $G/eval-stage2-replay
$P -u $S eval --candidate $G/stage2-replay --configs squad_v2,narrativeqa \
  --limit 200 --noise-documents --output $G/eval-stage2-replay-noise
```

Smoke (2026-11-13, GB10, BF16 decoder, `$G/smoke/`): batch 4 x accum 2 runs
2.56 s per step of 8 enwiki examples (about 1,660 decoder tokens and 1,280
encoder tokens per step, peak 21.7 GiB); batch 8 x accum 1 is slower on the
same examples (3.07 s, peak 24.2 GiB: longer padding), so batch 4 is the
setting to use. Per micro-batch of 4 the encoder takes ~5% of the time, the
reader and copy head ~0, the decoder forward ~25% and the backward through
the 42 frozen checkpointed layers ~70%: about 0.32 s per enwiki example, so
one epoch of Stage 1 (1,979,809 examples, 15,468 steps of 128) is ~176 hours
and Stage 2 (1.34 M examples with more documents and longer answers) likely
150-220 hours; `--max-answer-tokens` and `--max-tokens` are the levers. A
run killed after a checkpoint resumed on the identical examples
(`$G/smoke/resume-b4a2`).

Paper-options smoke (2026-11-13, GB10 shared with the running Stage 1 and a
Qwen eval, `$G/smoke-stage2-dual`): `--recipe stage2-replay --init $G/stage1
--batch 4 --accum 2 --max-steps 10 --validation-slice 8 --skip-initial-eval`
loaded `resume.pt@1000` (the `init` event names `stream_type.weight` as the
only new tensor) and trained 10 steps of 8 mixture examples at 2.8-11.3 s
per step (median 3.9 s; shared GPU, so not comparable to the 2.56 s above),
peak 20.4 GiB allocated. Per example: 6.3 chunks (max 19, none dropped) =
6.3 pooled rows, 5.3 token chunks = 902 token rows, 516 encoder tokens; 9
of 80 examples copyable. The full mixture is 1,543,993 examples (12,063
steps of 128; enwiki 12.95%). Eval of that candidate (20 seeded rows each,
`--report-doc-tokens`): NarrativeQA test reads 1,575 chunks per question on
average (max 4,082), 991 token rows, a 68,926-token collection (max
196,806) into 24 slots (2,872x). The first run capped the pooled stream at
512 rows (484 kept, 21,821 chunks dropped) and took 729 s for 20 questions
plus their validation loss, mostly encoder batches padded to the longest
chunk and every question re-encoding its book. With the whole collection
pooled (1,575 rows per question, none dropped), length-grouped encoder
batches and the feature cache, the same 20 questions took 228 s with an
empty cache (19 distinct books, so 19 pooled passes: 221.5 s answering
including 64-token generation, 6.5 s validation loss from cached books) and
63.7 s once cached (56.8 s answering, about 2.8 s per question; the 19
books take 40 MB in `$G/feature-cache`). That run exposed a cold/warm
difference (ROUGE-L 0.090 vs 0.101: miss-path token rows came from the
streaming pass, hit-path ones from a re-encode in a different BF16 batch);
with a cache every question now ranks from its pooled rows and re-encodes
its top chunks, and 8 questions answered cold then warm gave 8 identical
answers and the same validation loss (110.3 s cold for 8 distinct books,
30.4 s warm). SQuAD v2: 2.2 chunks per question,
41.5 s. Scores after 10 steps are meaningless (NarrativeQA ROUGE-L 0.09,
SQuAD F1 0.24); `--noise-documents` ran on `squad_v2,enwiki`
(`$G/smoke-stage2-dual/eval-noise`). Tests:
`test_gemma_soft_memory.py` (CPU: parquet row groups, mixture order, document
caps and selection, placeholder prompts and labels, copy targets, injection
and PLE identity, masked loss, schedule, metrics, resume round trip; paper
options: sentence splitting and greedy packing, chunk caps, pooled rows,
dual-stream encoding through fake taps with copy targets restricted to the
selected chunks, stream-type zero init and init from a Stage 1 `resume.pt`,
reader dropout, the Stage 1 protocol reproduced byte for byte from the CLI
defaults against `$G/stage1/protocol.json`, the stage2-replay mixture,
question-type buckets, norm-matched noise rows, ROUGE fallback; evaluation:
the feature cache's memory/disk round trip, documents digest and key
separation, identical dual features for a cached and an uncached
collection with only the top chunks re-encoded, the no-cap eval default,
and the pinned upstream metrics against `rouge_score` with stemming,
including refusal of an edited `metrics.py`).

### Qwen hybrid memory: soft slots plus the exact record

`qwen_hybrid_eval.py` runs the first experiment of section 8 of
`docs/plans/memorilla-copy17-native.md`: the released Memorilla reader's 16
soft slots (general recall) composed with copy17's 16 snapped record rows
(exact names and codes), read by the stock frozen Qwen3-8B. No training; the
three arms are selected with `--arms soft,record,hybrid` and share one model
load (BF16 encoder with copy17's adapters, swapped out for the soft features,
BF16 decoder, FP32 copy17 reader and copy head, BF16 stage3 reader):

- `soft`: pooled last-token Qwen3-Embedding-4B vectors through the FP16 -> BF16
  channel into the released stage3 reader; 16 soft rows under the released
  helpful prompt (`--soft-system-prompt`).
- `record`: copy17 exactly as `encoder_lora_eval.py --snap-memory all
  --copy-contiguous-runs --copy-complete-words` (the helpers of
  `encoder_lora_export.run_case` in the same order and precision); copy17's
  exact-answer prompt. `--sanity-reference lora-ref-dev/reference.json`
  compares every record answer to the stored reference and fails on a mismatch.
- `hybrid`: 32 markers, rows 1-16 soft and 17-32 record (`--record-rows spans`
  keeps only the copied, word-completed spans and pads with the space token);
  `--system-prompt helpful|exact|merged` (default merged: helpful + exact);
  `--row-order record-first` puts the record rows at positions 1-16;
  `--record-gate` applies the plan's section 7.4 routing: the record rows are
  included only when copy17 produced a copied span whose slots all snapped at
  cosine >= `--record-gate-cosine` (0.85), otherwise the case gets the 16 soft
  rows alone under the soft prompt (the summary counts how often the gate
  fired). Injection is the same scatter-add delta for every row.
  `--answer-canonicalize` re-spells a decoded answer from the record rows it
  was read from (record casing for a word-aligned match; the full record word
  sequence for a prefix-dropped tail such as "3005601" -> "Te3-005601"); rows
  keep `answer_raw` next to the scored `answer`. `--released-checkpoint DIR`
  reads the soft arm's reader from another released stage (stage1/stage2)
  with that directory's `config.json`; copy17's stack stays verified against
  stage3.

Sets (`--sets`): `depot-dev`, `depot-fresh` (exact match, UNKNOWN as the
abstention, plus a substring `contains` rate for verbose arms), `squad`,
`drop` (the first `--qa-limit` validation rows of Memorilla-Data; SQuAD
normalization, EM and token F1 against the stored answer; drop also reports
`em_any`/`f1_any` over its comma-joined annotations) and `enwiki` (first
`--enwiki-limit` validation rows; LCS ROUGE-L of a `--enwiki-max-new-tokens`
restatement against the passage). Rows go to `<arm>-<set>.jsonl` with the
record text and spans, a `summary-<set>.json` per set and `summary.json` for
the run; `--resume` skips cases already in the JSONL files. The copy17 encoder
batch bound is raised to `--encoder-max-length 1024` because squad/drop
passages exceed the evaluator's 128 under self-context doubling.
`test_qwen_hybrid_eval.py` covers the 32-marker prompt, span records, SQuAD
normalization/EM/F1, ROUGE-L and the summaries on CPU.

Results (2026-11-13, no training, shared GB10; rows in
`.benchmark-results/memorilla/qwen-hybrid/`). The record arm reproduced all 64
`lora-ref-dev` answers byte for byte. Depot numbers are exact matches with the
case-insensitive count in parentheses; every arm below answered all 16 missing
facts per depot set with UNKNOWN unless noted; "any" is the max over drop's
comma-joined annotations.

| arm | depot dev (64) | depot fresh (80) | squad EM / F1 (200) | drop EM / F1, any (200) | enwiki ROUGE-L F1, tokens (50) |
|---|---|---|---|---|---|
| soft (stage3) | 0 | 0 | 0.230 / 0.377 | 0.020 / 0.149, 0.220 / 0.246 | 0.068, 18 |
| record (copy17) | 64 | 79 (1 false abstention) | 0.050 / 0.091 (abstains 73%) | 0.000 / 0.005 (abstains 96%) | 0.061, 19 |
| hybrid (merged, soft-first, full rows) | 51 (62) | 69 (75) | 0.285 / 0.449 | 0.010 / 0.143, 0.210 / 0.238 | 0.086, 24 |

Hybrid variants (stage3 soft half, depot dev / fresh exact, squad F1, drop any-F1):
`--system-prompt exact` 51 / 66, 0.441, 0.215 (the same misses as the merged
prompt, so the wording is not the cause); `--row-order record-first` 36 / 52,
0.436, 0.244 (the decoder drops code prefixes when the record precedes the
soft rows); `--record-rows spans` 20 / 25, 0.458, 0.250 (without the label
tokens missing facts stop abstaining, 11 of 16, while QA improves);
`--record-gate` 51 / 68, 0.453, 0.233 (the gate fires on 143/144 depot,
184/200 squad and 173/200 drop cases; closed cases equal the soft arm). Every
hybrid miss on depot carried a complete, correct record text: the soft rows
disturb the surface form the decoder emits (case flips such as `WZ3-003545`,
prefix drops such as `3005601` for `Te3-005601`), not retrieval.

Production setting, `--record-gate --answer-canonicalize` (merged prompt,
soft-first, full rows): depot dev 63/64, fresh 78/80 (remaining misses: a
changed letter in `JESAVO DEGEBE`, and `Hokifu` for `Hidehi Hokifu`, a whole
word the canonicalizer must not extend), 16/16 abstentions and no false
abstention on both sets; squad EM 0.290 / F1 0.453; drop F1 0.135 (any
0.195 / 0.223) before the three-character tail minimum, which removes the two
bare-digit completions that cost that drop F1 (`4` -> `holder4`, `0` ->
`12-10`) and keeps every depot fix.

Soft half from another released stage (gated + canonicalized hybrid; enwiki
hybrid / soft-only ROUGE-L F1 with mean generated tokens):

| soft half | depot dev / fresh | missing abstained dev / fresh | squad EM / F1 | drop F1, any | enwiki hybrid | enwiki soft |
|---|---|---|---|---|---|---|
| stage1 | 45 / 45 | 7 / 4 of 16 | 0.000 / 0.038 | 0.044, 0.035 / 0.064 | 0.230, 140 tokens | 0.237, 141 tokens |
| stage2 | 55 / 67 | 11 / 12 of 16 | 0.300 / 0.448 | 0.151, 0.235 / 0.255 | 0.154, 67 tokens | 0.166, 64 tokens |
| stage3 | 63 / 78 | 16 / 16 of 16 | 0.290 / 0.453 | 0.135, 0.195 / 0.223 | 0.086, 24 tokens (ungated base) | 0.068, 18 tokens |

The stage1 reader restates passages (140 tokens, ROUGE-L 0.23) but turns the
hybrid into a restater everywhere (squad F1 0.038, depot abstention lost);
stage2 sits in between on every column. Recommendation: the stage3 soft half
with copy17's record, soft-first, merged prompt, record gate at 0.85 and
answer canonicalization against the record rows; serve restatement requests
through the stage1 reader as a separate mode rather than mixing it into the
QA composition. The remaining surface-form perturbation is the target of the
planned decoder-in-the-loop tune of the soft slots in the 32-slot frame.

#### Paper protocol on all eight registered benchmarks

`qwen_hybrid_eval.py paper-prep | paper-run | paper-choice-embed | paper-score`
evaluates the soft and hybrid arms under the upstream evaluation protocol on
every registered benchmark (`pmv2`, `pv4`, `factkg`, `triviaqa`, `narrativeqa`,
`pubmedqa`, `lamp4`, `lamp7`, test splits) plus EnWiki reconstruction. System
prompt, suffix, generation budget, prompt token limit, scoring and metrics come
from upstream `memorilla.benchmarks.BENCHMARKS`:

- `paper-prep` (upstream venv) also carries `choices` and pv4's `hard` flag into
  each case and runs upstream `check_scoring_inputs` on the whole split (every
  reference is one of its row's choices; METEOR's NLTK data) after the same
  metric adjustment `evaluate.load_target` makes (`accuracy_hard` exactly when
  the split flags hard rows).
- `paper-run` reads pmv2 and pv4 with their benchmark-specific released
  checkpoints (`checkpoints/pmv2`, `checkpoints/pv4`; paper section 2.6) and
  every other target with stage3 (`--released-checkpoint` overrides); the run
  receipt records the checkpoint, its `memory.pt` sha256 and why it was chosen.
  Memory prompts longer than the benchmark's `max_length` are cut from the
  right like upstream `MemoryCollator` (rows record `prompt_truncated_from`).
- `paper-choice-embed` (CUDA venv, pmv2/pv4/factkg only) embeds every cleaned
  generation and every non-empty choice with Qwen3-Embedding-4B (BF16,
  tokenizer special tokens, last-token pooling, L2 normalization, FP16
  storage: the path that reproduces the stored question vectors) into
  `<output>/choice-embeddings.{safetensors,json}`, keyed by text sha256 and
  extended incrementally. It first re-embeds the first `--verify-cases`
  questions and fails unless each matches its stored (upstream vLLM) question
  vector at cosine >= `--min-verify-cosine` (0.999).
- `paper-score` (upstream venv) applies upstream `choice_predictions` /
  `choice_index` to those vectors for choice targets, upstream
  `compute_metrics` otherwise, and reports per-row primary scores (choice
  correctness, PubMedQA polarity match, TriviaQA EM, ROUGE-L for generation)
  with paired bootstrap 95% intervals (`--bootstrap-resamples`, default
  10,000) for every `--pairs` delta.

`.benchmark-results/memorilla/qwen-hybrid/paper/prep8.sh` wrote the case files
(seed 23: 1,000 rows each for pmv2, pv4, factkg, lamp4, lamp7; all 100 for
pubmedqa) and `bench8.sh` runs the six targets with arms
`soft,hybrid,hybrid-bench`, the choice embeddings and the scores (`TARGETS` and
`ARMS` override the defaults).

#### Copy half for general QA (`qwen_copy_qa.py`, research)

Continues copy17 decoder-free on question answering so the hybrid's record
generalizes beyond depot facts: record `" answer" + exact source span` (gate 1,
pointer onto the span) or 16 pads with gate 0 when the answer is not verbatim
in the top-2 chunks (a learned relevance gate), plus 15% depot replay.

```bash
. .benchmark-results/memorilla/qwen-copy-qa/env.sh
# CPU: examples from the Stage 2 QA configs and the TriviaQA/NarrativeQA
# train splits (top-2 chunks by stored cosine, as the hybrid retrieves).
$PY $COPYQA prep --data-root $DATA --tokenizer $A/models/qwen3-embedding-4b \
  --output $Q/examples --samples triviaqa=45000
# GPU: decoder readability of "<label> <span>" records (6 SQuAD cases).
$PY $COPYQA probe --assets $A --candidate $CAND17 --data-root $DATA --output $Q/probe
# GPU: training (qwen-copy-qa/train.sh NAME STEPS [args] sets the weights).
# --question-prefix question-passage-v1 encodes each document after the
# question text (protocol document_question_prefix, applied by
# qwen_hybrid_eval.record_memory for exported candidates).
$Q/train.sh copyqa2-qprefix-1024 1024 --accum 4 --micro-batch 16 \
  --reader-lr 3e-5 --copy-head-lr 1e-4 --question-prefix question-passage-v1 \
  --max-encoder-tokens 1280
# GPU: remeasure a candidate on all eight benchmarks and the legacy sets.
$Q/remeasure8.sh $Q/<run>/final
```

Status (2026-11-14, plan section 8.10): the gate learns but the copied span
does not follow the question (SQuAD exact span 3-4/200 after 256 steps, with
or without the question prefix); keep the reader learning rate at 3e-5
(1e-4 erases depot recall within 8 steps). A span head over the
question-conditioned encoder states is the proposed replacement.

#### Span copy half (`qwen_span_copy.py`, research)

The extractive replacement for the reader + copy head on general QA (plan
section 8.11): copy17's encoder (+ frozen adapters) reads each of the top-2
chunks after the question text (`question-passage-v1`, self-context doubled);
a small bidirectional span head scores start, end and no-answer over the
per-token final + layer-24 rows; the record is the exact source window around
the best span (32 tokens, 65% of the free budget before the span,
word-aligned), fed to the frozen decoder as its input-embedding rows. The
exported candidate is copy17's directory plus `span-head.safetensors` and
protocol `record_kind: span-window`; `qwen_hybrid_eval.py` dispatches its
records (`--record-gate-mode always` keeps the window in every prompt,
`learned` only when span minus no-answer clears the exported threshold).

```bash
. .benchmark-results/memorilla/qwen-span-copy/env.sh
# Decoder reads gold-span windows (depot dev/fresh, no training).
$PY $SPAN oracle --assets $A --candidate $CAND17 --depot-dev $ENC/lora-ref-dev.json \
  --depot-fresh $ENC/lora-ref-fresh.json --output $D/oracle.json --widths 32,48
# Encode examples once (fp16 per-token features, ~1.5 MB per example).
$PY $SPAN cache --assets $A --candidate $CAND17 --examples $Q/examples \
  --depot-data $DEPOT --output $D/cache-train --split train \
  --quotas triviaqa=8000,squad_v2=6000,coqa=5000,drop=3000,narrativeqa=3000,msmarco=3000,quail=1500,pubmedqa=500,depot=5000
# Train the head from the cache (held-out span/gate metrics each epoch) and
# export the candidate to <output>/candidate.
$PY $SPAN train --cache $D/cache-train --heldout-cache $D/cache-heldout \
  --parent $CAND17 --output $D/span1-35k --epochs 3
# Decoder-side evaluation: legacy sets and all eight benchmarks.
$D/remeasure.sh $D/span1-35k/candidate span1
```

Serving in Zig (plan section 8.12): export the candidate as a hybrid bundle
with the head pinned, then point a `hybrid_research` profile's
`memory_module_path` at it; the service reads the pinned protocol and takes
the span-window path (gate policy from `span_gate_policy`, default `always`).

```bash
$PY encoder_lora_export.py export-reader --candidate $D/span1-35k/candidate \
  --output $D/span1-35k/hybrid-span-bundle --include-encoder-lora --include-span-head
# Managed-path verification against the Python legacy rows (Zig verifier
# built with `zig build build-memorilla-lora-hybrid-verify -Dcuda=true
# -Doptimize=ReleaseFast`); writes span1-zig-verify[-learned].{jsonl,summary.json}.
$D/run_span_verify.sh always
$D/run_span_verify.sh learned
```

Robustness checks (plan section 8.14). `train --seed N` gives another head
seed; `train --exclude-sources triviaqa` drops a source from both caches
(leave-one-out: neither the head nor its exported threshold sees it).
`paper-run --record-extractor embed-window` replaces the head with a cheap
extractor over the same top-2 chunks and window width: the word-aligned
32-token window (stride 8) whose pooled frozen-encoder vector is most similar
to the stored question vector (`span_margin` holds that cosine, so the
offline sweep can gate on it); `lead` takes the first window of the top-1
chunk. `paper-score` also writes `per-row.json`, which `seeds` aggregates
over head seeds (per-seed paired CIs plus the seed-mean delta).

```bash
. .benchmark-results/memorilla/qwen-span-copy/robust/env.sh
$PY $SPAN train --cache $D/cache-train --heldout-cache $D/cache-heldout \
  --parent $CAND17 --output $R/train/loo-triviaqa --exclude-sources triviaqa
run embed triviaqa $H/paper/cases/triviaqa.jsonl   # paper-run, window always
$PY $SPAN seeds --per-row $R/scored/full/narrativeqa/per-row.json \
  --right s31-gate-3,s32-gate-3,s33-gate-3
$R/main.sh   # the whole study: seeds, LOO, extractors, full splits
```
