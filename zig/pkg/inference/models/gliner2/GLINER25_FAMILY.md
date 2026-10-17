# GLiNER2.5 model family

Checkpoint names do not determine the runtime architecture. Read the saved
`architecture` and the nested `encoder_config/config.json` before selecting
an encoder, head, tokenizer, or converter.

| Checkpoint | Saved architecture | Encoder | Geometry |
| --- | --- | --- | --- |
| [`fastino/gliner2.5-multi-v1`](https://huggingface.co/fastino/gliner2.5-multi-v1/tree/2ca71aafb3446d9014e1c55c7ff51c9bc7209c47) | boundary | mDeBERTa-v3-base | hidden 768, FFN 3072, 12 layers, 12 heads, vocabulary 250112 |
| [`fastino/GLiNER2.5-multi-Decide`](https://huggingface.co/fastino/GLiNER2.5-multi-Decide/tree/a35a0cd3b7a0f00f2effc576f454cd48fa98aa5f) | boundary | mDeBERTa-v3-base | same geometry as multi-v1 |
| [`fastino/GLiNER2.5-Decide-1B`](https://huggingface.co/fastino/GLiNER2.5-Decide-1B/tree/688cd7ba8917a0855ad3ce929cba5a9998932e79) | span, markerV0 | Ettin ModernBERT | hidden 1792, FFN 3840, 28 layers, 28 heads, vocabulary 50378 |

| Checkpoint | Qualified FP32 backends | Public tasks |
| --- | --- | --- |
| multi-v1 | native, Metal | measured single-window entity, classification, structure, and relation profiles |
| multi-Decide | native, Metal | the multilingual boundary profiles, plus typed Decide |
| Decide-1B | native, Metal | typed classification/Decide; one item, at most 64 prepared sequences of at most 1024 tokens each, independently measured for FP32 and Q8; FP32 also has diagnostic span execution, with extraction capabilities withheld |

The multilingual checkpoints have the same 334-tensor schema and the same
16 MB Unigram tokenizer, but different learned weights. They share boundary
encoder/head kernels and converter geometry. Both exact FP32 artifacts pass
29 pinned upstream cases on native and resident Metal, covering English
mixed extraction and Spanish, Japanese, and Arabic entities/classification,
descriptions, instruction prompts, structured label selection, and measured
short/longer schema combinations. Classifier-only prepared bounds are
18–183 tokens for bare labels, 55–218 with descriptions, 31–194 with
context, and 66–229 with both. Complete mixed extraction has its own
135–298-token bounds; it does not inherit the classifier-only minimum.
The production policy carries separate measured rows for these request
shapes. It does not inherit base-model long-document, batching, quantized,
or CUDA qualification. The public `mode: "multi"` contract also remains
unqualified: the captured multiple-label example uses structured selection.

Ordinary entity requests have separate measured bounds: 2–663 document bytes,
1–114 original source words, and 2–114 prepared window words. Bare labels
use 17–181 prepared tokens; described labels use 22–202. The references
exercise both Unicode codepoint and UTF-8 byte offsets, including the
Spanish, Japanese, and Arabic examples. Two additional public Decide cases
qualify multi-Decide's choice, score, and NoUL distributions.

The one-byte `multi-v1` input `a` exposes an upstream formatting defect: it
returns `a.` at `0..2`, including a synthetic period beyond the caller's
source. Native presentation retains the entity field with zero values. This
single exception is bound to the complete fixture digest and request ID;
every other case uses strict captured-output parity. Clean punctuation and
the original one-word `Alice` case supply the ordinary short entity evidence.
Captured model-quality misses remain separate from numerical parity.

Public serving checks exercise actual registry-generated manifests, required
native/Metal sessions, direct calls and in-process HTTP handler dispatch,
exact file identities, cached session reuse, full decision distributions,
and idle resource ownership. These fixtures do not bind a socket or establish
live TCP-server behavior.
Device workspace, model-load accounting, and live-memory admission stay
active. Cold loading also reserves the default 512 MiB bounded request
heap; hosts must have room for these overlapping phases. A resource
admission refusal is not a numerical-parity result.

The dedicated multilingual service fixtures configure a 7 GiB process
envelope and 6 GiB generation budgets. The 1B fixtures configure a 10 GiB
process envelope and 10 GiB generation budget. Both retain the default
request heap and mandatory emergency reserve, use actual physical telemetry,
and check the resource ledger after each request. The 1B bound conservatively
charges the source-weight cache and backend-resident weights as separate
ownership domains, together with tokenizer/cache, request-heap, and device
workspace reservations; layer-bounded pins do not imply that the cache maps
are absent between layers. Its current contract upper bound is about 9.46 GiB;
the 10 GiB process ceiling retains the 512 MiB emergency reserve and a small
baseline margin while still binding the fixture to the host's physical-memory
signal. The worker ceilings include resident model bytes and concurrent
construction/request reservations. A higher operator-configured process
envelope applies only when physical host availability can support it;
otherwise automatic headroom remains authoritative. Cached boundary
workspace may grow when request geometry grows, but must remain owned by the
cached session. Both service fixtures require exact model, tokenizer,
tokenizer-cache, weight-cache, and
workspace ownership after each request; an identical HTTP replay must
preserve the complete idle ledger.

The initial family campaign passed both native and physical Metal service
fixtures, including cached in-process HTTP handler replay. Its eight
model/backend/task jobs validated outputs and ownership after 576 calls.
After the final serving-contract repairs, the native and multilingual Metal
campaigns passed again; the 1B Metal service retry was refused by physical
memory admission. Its available request capacity was 450 MiB, below the
unchanged 512 MiB request-heap reservation. These earlier passes do not replace
current-source 1B HTTP qualification on a host with sufficient headroom.

The separate 1B direct pipeline fixture retains ModelManager load and GPU
scratch admission, the target execution mutex, RunBudget, the real Node
watchdog, and the production two-layer frame cadence. It checks exact token
IDs, ordered raw logits, complete probabilities, and retained ownership after
one preflight, three warmups, and twenty samples per request. It excludes
request parsing, admission/lock acquisition, and HTTP handling, and therefore
cannot qualify the HTTP route or replace its request heap.

## Performance

> **Relocated:** the dated 2026-10-06 Metal performance and 2026-10-07 Decide-1B production-allocator follow-ups (AB/BA measurements, exploratory Python comparison, receipts, and pass counts) → [history](../../../../../docs/design/inference/history/gliner2/gliner25-family-metal-followups-2026-10.md).

Both Metal encoder optimizations below are on by default, keep the scalar
FP32 numerical contract and cancellation boundaries, and have a diagnostic
rollback:

- **Packed boundary QKV** reduces the three attention projections to one
  matrix multiplication plus a split/bias epilogue (about 5–6% lower
  extraction latency in same-binary AB/BA runs). It adds 81 MiB of immutable
  resident storage for the 12-layer, 768-wide base/multilingual geometry,
  included in model admission. `TERMITE_METAL_DISABLE_GLINER_BOUNDARY_PACKED_QKV`
  disables execution but retains the storage, so it compares execution cost
  rather than the previous memory envelope.
- **Fused resident FFN** combines bias/exact GELU and
  bias/residual/centered-LayerNorm epilogues while reusing the admitted
  product workspace (a further 4–6% median reduction on Decide requests).
  Missing fused pipelines fall back before acquiring weights or changing
  scope state. `TERMITE_METAL_DISABLE_GLINER_BOUNDARY_FUSED_FFN` disables it.

Decide-1B serving uses a typed Decide span route that avoids the extraction
JSON round trip; tokenizer listing projects marker metadata without
allocating the vocabulary; native and Metal compute implement the packed
exact-GeGLU hook; native M-RoPE shares each token's phases across heads; and
Accelerate attention specializes single contiguous segments up to 198 tokens
with 64-wide heads, retaining the general fallback. The standalone
`inference-bench-server decide-bench` command measures the loaded pipeline,
encoder/head, direct Node call, and in-process HTTP handler separately with
the production allocator; campaign instructions and acceptance boundaries are
in the [development tools README](../../scripts/gliner25/README.md#decide-1b-production-allocator-comparison).

Reference serial latency on an Apple M4 (FP32, exact pinned artifacts, two CPU
threads, twenty warm samples after three warmups). Multilingual rows include
the in-process HTTP handler; Decide-1B rows use the direct pipeline boundary
described above. Python MPS includes schema construction, preprocessing,
model execution, and decoding. These are descriptive serial latencies, not
concurrent throughput or service SLOs:

| Request | Prepared tokens | Metal median ms | Python MPS median ms |
| --- | ---: | ---: | ---: |
| multi-Decide Spanish entities | 61 | 16.76 | 28.18 |
| multi-Decide described choice | 97 | 25.68–25.91 | 31.82 |
| multi-Decide choice/score/NoUL | 167 | 40.32–40.37 | 45.81 |
| Decide-1B described choice, direct pipeline | 87 | 122.04 | 128.88 |
| Decide-1B choice/score/NoUL, direct pipeline | 158 | 177.42 | 200.11 |

The two Decide-1B direct-pipeline p95 values were 123.68 and 178.83 ms. Host
GPU scheduling outliers in the multilingual runs preclude a service-tail claim.

### Open work

- Decide-1B full-model CPU and Metal performance on final source is
  unmeasured. The last exploratory HTTP-handler comparison predates the final
  CPU attention and M-RoPE changes, still regressed against Python MPS on
  shorter requests, and failed the paired-block and 5% p95-ratio acceptance
  checks; no overall win over Python is established.
- Decide-1B HTTP service qualification on current source needs a host with
  enough physical headroom for the 512 MiB request heap.

## Decide-1B architecture and serving contract

The 1B checkpoint has 199 tensors and a 4,755,208,228-byte FP32 weights file.
Its typed classifier uses `[L]` marker states and an H→2H→1 MLP. The encoder
uses bias-free fused QKV, GeGLU, alternating global and local attention (one
global layer every three layers), and a 128-token local window. Its saved
position budget is 7999 tokens, while its tokenizer advertises 8192; the
encoder budget is authoritative. Transformers 5.17 applies split-half RoPE
with theta 160000 for both layer types even though the config also contains
`position_embedding_type: "sans_pos"`.

The complete FP32 1B session passes eight classification cases and two public
Decide wire cases against the pinned upstream runtime on native and physical
Metal. Maximum raw-logit differences are 2.39e-6 native and 3.58e-6 Metal;
complete probabilities and decoded results pass a 5e-4 absolute tolerance.
The 198-token case crosses the local-attention window. One source model
semantic expectation fails in the regular corpus; the runtime preserves that
upstream answer rather than changing the numerical oracle.

The native and Metal encoder paths use bounded attention workspace and plan
against the saved 7999-token ceiling, subject to resource admission. The
current CUDA attention kernel has a 512-token limit, which is enforced in
planning and execution. A config accepting 7999 positions does not establish
measured maximum-length performance or device qualification.

The ModernBERT span path serves typed classification/Decide. The exact pinned
FP32 artifact also implements diagnostic entity/relation execution through its
own marker span representations, GRU count slots, and span scoring heads; see
[GLINER25.md](GLINER25.md#embedded-knowledge-graph-extraction-and-bounded-windows).
Neither precision advertises entity/relation extraction: the retained upstream
probes return empty results even at low thresholds. Numerical head parity
alone does not establish useful graph extraction. See the
[extraction investigation](../../scripts/gliner25/SPAN_1B_INVESTIGATION.md).
Pulling an unqualified decision artifact withholds executable tasks and
capabilities.

The public 1B decision contract is separate from the encoder's saved position
budget. For the exact pinned FP32 artifact it permits native or Metal, one
request item, at most 64 prepared sequences, and at most 1024 prepared tokens
per sequence. The separately pinned Q8 encoder/head bundle has independent
Native and physical Metal pipeline and HTTP measurements at the same bounds;
it does not borrow the FP32 proof. These limits
are `requestLimits` in `src/models/gliner_decide_qualification.zig`, enforced
by `requireRequestIdentity` and applied to the span planner in
`src/server/server.zig`. The planner rejects larger requests without
truncating them. The live session must match the full weight digest, tensor
inventory, geometry, marker IDs, and every sidecar digest. Its tokenizer is
sealed only after the exact consumed bytes have been verified. Registry
metadata or a model alias cannot replace this loaded-artifact check.

Explicit schema-v1 classification and the embedded classifier provider use
the same qualified span processor and serving limits. This preserves task
names and the upstream terminal-period handling, and carries the legacy
classification threshold (including its default of 0.0) into the versioned
schema. Endpoint cutoffs and empty results below a legacy multi-label
cutoff are private compatibility behavior; public v2 calibration keeps its
strict `(0,1)` contract and best-label fallback. Explicit v1 still rejects
v2-only schema fields. The provider adapter preserves descending score order.
Legacy classification executors reject qualified span models if
routing changes between listing and loading. Ordinary untyped DeBERTa
classification keeps its existing explicit-v1 route.

Declared ModernBERT span inference on Metal borrows aligned immutable FP32
weights from the session's mapped store. The store remains alive until the
compute lease is released, and memory admission still accounts for its
resident bytes. This avoids copying the full 1B weights for each compute
lease. Training, reduced precision, and unaligned tensors retain their
existing ownership paths.

The built-in GGUF exporter rejects the ModernBERT span wrapper explicitly;
it cannot reuse the older DeBERTa composite converter. Native/Metal can
retain explicit quantized weights, but converted checkpoints have no
full-model numerical qualification. CUDA rejects quantized ModernBERT span
weights before upload because its current execution profile requires FP32.
The existing DeBERTa Decide Q8_0 exporter retains ordinary `/extract`
classification and strips the reserved `decide` task and `typed_decisions`
capability; that converted artifact does not inherit public `/decisions`
qualification from its FP32 source.

The 1B tokenizer is byte-level BPE with NFC normalization. Composed and
decomposed Unicode text must produce the same IDs; structural tokens retain
their explicit IDs (for example `[L]` is 50374). GLiNER tokenizes each schema
fragment and source word independently, without adding model wrappers to
each fragment. The same processor routing is used for both encoders.

## Verification

Run the offline contract verifier against a staged checkpoint:

```sh
python3 zig/pkg/inference/scripts/gliner25/verify_family_contract.py \
  --profile multi_v1 --model-dir /path/to/multilingual/checkpoint --verify-model-sha256
```

Profiles are `multi_v1`, `multi_decide`, and `decide_1b`. The verifier checks
the pinned architecture, encoder geometry, tensor header/schema, weight
size, and optional complete weight digest. It explicitly reports that a
successful artifact check is not runtime qualification.

Full tokenizer parity uses the immutable tokenizer bytes and 18 reference
cases per model: independent words, schema markers, decomposed accents,
Hangul, Arabic, Japanese, Cyrillic, Hindi, Turkish, emoji, and whitespace.
Stage tokenizer files in `multi`, `multi-decide`, and `decide-1b`
subdirectories, then run from `zig/`:

```sh
ANTFLY_GLINER25_FAMILY_TOKENIZER_DIR=/path/to/family \
  python3 tools/run_bounded_zig_build.py build lib-tokenizer-test -j1
```

Regenerate the reference IDs with the pinned tokenizer files and the pinned
`tokenizers` runtime. The generator refuses changed tokenizer bytes:

```sh
python3 zig/pkg/inference/scripts/gliner25/generate_family_tokenizer_fixtures.py \
  /path/to/family zig/lib/tokenizer/src/gliner_family_fixtures.zig
zig fmt zig/lib/tokenizer/src/gliner_family_fixtures.zig
```

The immutable multilingual Python references use a clean checkout of
[`fastino-ai/GLiNER2` at `55656fb`](https://github.com/fastino-ai/GLiNER2/tree/55656fbfa01d3d4a77485e1a1eeeaf682990ccdf)
and the runtime versions recorded in `family_contract.json`. The retained
goldens include full requests, prepared token IDs, raw logits, decoded outputs,
source/runtime identity, and original generator/request digests. The one-time
capture generators and duplicate request manifests are external qualification
tooling rather than part of this runtime change. The canonical Python benchmark
replays the measured requests against these goldens without those generators.
New serving bounds require fresh captured evidence; do not rewrite existing
goldens to fit results. Their `qualification` fields remain false, and captured
model-quality misses remain separate from implementation parity.

The 1B oracle requires the separate runtime inventory in
`decide_1b_oracle_runtime.json`. An installed Transformers 4.55.4 runtime
retains the nested RoPE metadata but uses theta 10000 for local layers;
the checkpoint requires 160000. The isolated 5.17.0 reference checks both
frequency arrays and every installed runtime file against its wheel RECORD.
GLiNER2's pinned Python source declares Transformers `<5`, so source imports
alone do not establish that this checkpoint can run under its declared
dependency range. The isolated 5.17 reference completes the full checkpoint
load and all ten forward cases; its installed files and RoPE arrays are
verified before capture.

Use `ANTFLY_GLINER25_MULTI_V1_MODEL_DIR` and
`ANTFLY_GLINER25_MULTI_DECIDE_MODEL_DIR` with the inference tests to check
the committed multilingual reference captures. Each test verifies the
complete weights and all four sidecars before encoding. The Metal tests
use the resident encoder, boundary head, and scorer, with bounded downloads.
The 1B full-session harness uses `ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR` and
`ANTFLY_GLINER25_DECIDE_1B_CAPTURE`. Its upstream corpus includes eight
classification requests and two public Decide requests (choice, score, and
NoUL distributions). The 198-token case crosses the local-attention window;
it does not establish execution at the saved 7999-token ceiling.

Explicit ModernBERT configs must match the implemented inference semantics:
exact GELU, bias-free fused encoder projections/norms, split-half default
RoPE, and the supported global/local layer pattern. Present malformed or
unsupported RoPE scaling, activation, or bias settings fail during loading.

### Serial service performance

The service fixtures optionally record three explicit warmups and twenty
measured calls per request and path, alternating direct/handler order. Every
call retains the exact output, cached backend identity, and idle ownership
checks. Those checks and returned-result destruction are outside the timer.
The tests use `std.testing.allocator`; production uses
`processAllocator(smp_allocator)`. Label these measurements as validated
fixture latency. Handler measurements include response generation and omit
network transport. Twenty samples give a descriptive p95 rather than a
concurrent-service percentile.

Build the filtered fixtures from `zig/`, using the repository's declared
14 GiB compiler bound:

```sh
ANTFLY_ZIG_MAX_RSS=15032385536 \
  python3 tools/run_bounded_zig_build.py build inference-test -j1 \
  -Doptimize=fast -Dcuda=false -Dmetal=true \
  '-Dtest-filter=--test-filter=GLiNER2.5 multilingual family pinned' \
  '-Dtest-filter=--test-filter=GLiNER2.5 multilingual Decide pinned' \
  '-Dtest-filter=--test-filter=GLiNER2.5 Decide-1B exact' \
  '-Dtest-filter=--test-filter=GLiNER2.5 Decide-1B Metal direct-only' \
  '-Dtest-filter=--test-filter=GLiNER family performance summary'
```

The artifact tests skip when model directories are absent; the benchmark
supervisor supplies them and requires each actual run to pass without skips.
Use the emitted test binary path from the build output. From the repository
root, run the eight jobs serially:

```sh
python3 zig/pkg/inference/scripts/gliner25/run_family_performance.py \
  --test-binary /path/to/compiled/test \
  --models-root /path/to/family \
  --output .benchmark-results/gliner25-family-new-run \
  --threads 2
```

The model root must contain `multi`, `multi-decide`, and `decide-1b` with the
exact pinned files. The output directory must be new. `--only
decide_1b-metal-decide` selects one job. The supervisor clears inherited
runtime tuning/debug overrides, records configured CPU/vendor thread caps,
and binds source, binary, model, sidecar, and capture identities. It rejects
incomplete case/path samples, incorrect geometry, non-conserved ownership,
and statistics that do not reproduce from the raw nanosecond samples.

The retained upstream Python comparison uses the same captured requests:

```sh
PYTHONHASHSEED=0 python3.12 zig/pkg/inference/scripts/gliner25/benchmark_family_python.py \
  --profile multi_decide --task decide --device mps \
  --model-dir /path/to/family/multi-decide --upstream /path/to/pinned/GLiNER2 \
  --output /path/to/new/python-report.json --threads 2
```

Use `--device cpu` for the CPU comparison. For 1B, select `--profile decide_1b`,
point `--model-dir` at its checkpoint, and pass `--runtime-dir` for the pinned
Transformers 5.17 target directory. The benchmark verifies the complete model,
upstream source, installed runtime files, RoPE arrays, token IDs, and outputs;
it writes run reports outside Git.

To measure the 1B direct pipeline separately, use the explicit diagnostic job:

```sh
python3 zig/pkg/inference/scripts/gliner25/run_family_performance.py \
  --test-binary /path/to/compiled/test \
  --models-root /path/to/family \
  --output .benchmark-results/gliner25-1b-direct-new-run \
  --threads 2 --only decide_1b-metal-direct-pipeline
```

This opt-in job records source-diff and binary digests in each sample report.
It reports two warm loaded-session requests, with no cold-request latency or
HTTP qualification. The default eight service jobs remain unchanged.
Its internal `direct_kernel` identifiers are retained fixture names; the
reported measurement scope and timing boundary describe the full pipeline.

Cold-first-direct timing includes an unloaded model's complete first request
after checkpoint verification, with warm filesystem caches. It is distinct
from pure loading or cold-disk time. macOS `time -l` reports whole-process
memory high-water values including verification and loading. Keep RSS,
physical footprint, and logical admission charges as separate views; their
values cannot be added to estimate physical memory. The JSONs retain all
samples and exact idle ownership, while `campaigns.json` retains commands,
host power/thermal context, process resources, and acceptance summaries.

Token IDs, architecture tests, and synthetic kernel checks establish their
own contracts. Full weights parity, quantized conversion parity, CUDA device
execution, maximum-length memory bounds, and throughput require separate
evidence for each artifact/backend combination. See [GLINER25.md](GLINER25.md)
for the boundary qualification policy and [GLINER25_DECIDE.md](GLINER25_DECIDE.md)
for the existing DeBERTa Decide route.

## Published split bundles and pull defaults (2026-10-17)

Canonical repositories follow Fastino's checkpoint capitalization and omit a
format suffix, matching `antflydb/gliner2-base-v1`'s multi-format repository
layout:

| Repository | Immutable layout revision | Current typed Decide contract |
| --- | --- | --- |
| [`antflydb/GLiNER2.5-Decide-1B`](https://huggingface.co/antflydb/GLiNER2.5-Decide-1B) | `98ce73668cea4b940299396920671ec0a8abc57f` | Native/Metal, one item, 64 sequences, 1024 complete prompt tokens per sequence |
| [`antflydb/GLiNER2.5-Decide`](https://huggingface.co/antflydb/GLiNER2.5-Decide) | `358ae4b1263cedb79faf9e8b7984c3b4e5586874` | Not qualified; the DeBERTa Q8 exporter strips `decide` |

The previous `gliner2.5-decide-1B-gguf` and `gliner2.5-decide-gguf` URLs redirect
to these repositories. Both contain `gliner2-encoder.Q8_0.gguf`,
`gliner2-head.Q8_0.gguf`, `antfly_inference_bundle.json`, and
`antfly_inference_variants.json`. `Q8_0` in the pair's filenames identifies the
bundle variant: auxiliary heads remain FP32. Renaming files changed none of
the weight, tokenizer, or encoder configuration bytes. Historical captures and
receipts retain their original paths; their `gliner_head.gguf` digest identifies
the renamed head. The migration script is
[`migrate_hub_bundles.py`](../../scripts/gliner25/migrate_hub_bundles.py).

Pull selects a split bundle's encoder and head from its descriptor at the
resolved immutable Hub commit. This supports the historical mixed-suffix
layout as well as canonical paired filenames, rejects missing or unsafe
references, and never substitutes an unrelated GGUF for a missing component.
An explicitly requested quantization must match the declared encoder.

Unversioned pulls of reviewed Fastino GLiNER2.5 checkpoints use the immutable
revisions in `src/registry/gliner_catalog.zig`, including base-v1, multi-v1,
multi-Decide, Decide, and Decide-1B. An explicit `:auto@main`, branch, tag or
commit overrides that default and still undergoes loaded-artifact checks.
This keeps an upstream configuration change from silently replacing the
qualified artifact. Repository names and redirects do not grant capabilities.

The SQL decision provider already emits one `state` per request in
`antfly-embedded/src/functions/runtime.zig` (`Job.invoke`). Its `batch_size`
controls concurrent independent requests, rather than combining several states
into a model request. It need not be set to one for the 1B item limit; use one
when serial admission is desirable on a memory-constrained host.

Issue #1083 remains open for useful entity/relation extraction from a trained
artifact. Q8 context expansion is independently measured; the pinned upstream
weights still return empty extraction.
The existing DeBERTa GLiNER trainer and the Laya trainer do not qualify a
Decide-1B adapter. A future trainer must bind the exact encoder/head geometry,
replay labeled decision and extraction holdouts, materialize a versioned bundle,
and qualify that new artifact before publishing its capabilities. Neither the
Hub rename nor existing FP32 evidence changes those requirements.

### Q8 context probes

The retained [Q8 probe receipt](../../testdata/gliner25/decide_1b_q8_context_probe.json)
records 17 cases on Native and physical Metal: all 14 original frozen
classification inputs plus independent upstream 55-, 65-, and 1024-token
single-window captures. Each case ran one preflight and three measured
invocations, with two CPU threads. Maximum raw-logit drift across the complete
sweep was 0.02295 Native and 0.02270 Metal, below the predefined 0.05 probe
tolerance. The 1024-token case alone differed by 0.001661 Native and 0.001581
Metal. These timings describe a shared host, not a latency qualification.

Those original probes are superseded for classification capacity by the
[capacity campaign](../../testdata/gliner25/family/decide_1b/q8_capacity/README.md): 31 complete
in-budget prompts on Native and physical Metal, independent upstream token
IDs/logits/distributions, natural-language 665–748-token contexts, exact
1024-token acceptance, and real HTTP execution of 64 windows. The public Q8
classification ceiling is now one item and 64 prepared sequences of at most
1024 tokens each. Larger requests are rejected without truncation. This does
not qualify entity/relation extraction or prove application-level decision
quality; the campaign retains gold-model misses separately from parity.

Reproduce the capture with `benchmark-cases.json` and `validation.json` from
the immutable bundle revision recorded in the receipt:

```sh
python3 zig/pkg/inference/scripts/gliner25/prepare_decide_quant_capture.py \
  --cases /path/to/bundle/benchmark-cases.json \
  --validation /path/to/bundle/validation.json --reference fp32 \
  --window-reference zig/pkg/inference/src/extractors/testdata/knowledge_graph/decide_1b_windows.json \
  --output /path/to/context-cases.json
```

Build `inference-bench-gliner-decide-quant-build` with Zig 0.17,
`-Doptimize=fast -Dmetal=true -Dcuda=false`. Run the resulting
`antfly-inference-gliner-decide-quant-bench` with `--model-dir` pointing to the
complete canonical Q8 bundle, `--capture /path/to/context-cases.json`,
`--backend native` or `metal`, `--warmups 0 --reps 3 --tolerance 0.05`, and
`ANTFLY_INFERENCE_CPU_THREADS=2 VECLIB_MAXIMUM_THREADS=2`. Capture JSON stdout
and diagnostic stderr separately. The capture preparer verifies the complete
window-reference digest and excludes multi-window aggregate logits from raw
per-sequence comparisons.
