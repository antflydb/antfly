# Memorilla F32 numerical profiles

The native module verifier widens the released BF16 weights to F32 and executes
F32 activations. Document and question features retain the upstream FP16 storage
to BF16 input cast before widening. This is an explicit F32 execution profile;
it does not qualify native BF16 intermediate rounding, the encoder, decoder,
answer quality, or serving throughput.

## Acceptance formulas

For every element of a traced boundary, require
`abs(native - reference) <= atol + rtol * abs(reference)`. All values must be
finite. When a normwise limit is enabled, independently require
`sqrt(sum((native-reference)^2) / sum(reference^2)) <= limit` for the complete
boundary tensor. A zero reference norm uses the absolute component guard;
its reported relative error is null.

| Profile | Intermediate atol | Memory token atol | rtol | Per boundary relative L2 limit |
| --- | --- | --- | --- | --- |
| `strict-f32-v1`, default | `1e-5` | `1e-5` | `1e-5` | None |
| `trained-f32-v1`, explicit opt in | `2e-4` | `5e-5` | `1e-5` | `5e-6` |

`--precision-profile trained-f32-v1` selects the second contract. Named profiles
reject `--atol` and `--rtol` overrides. Supplying numeric tolerance flags without
a named profile produces a separately labeled `custom-f32-diagnostic` receipt.
Changing these constants requires a new versioned profile and new qualification.

Every traced boundary must pass its component and normwise gates. The verifier
reports failed boundaries, component mismatches, maximum absolute difference,
L2 error, reference L2 norm, and relative L2 error. Final memory tokens also have
an aggregate result. CUDA success additionally requires actual kernel, linear,
attention, and normalization execution; recorded device-to-host bytes must equal
the declared boundary readbacks, with no cross-backend synchronization fallback.

## Why trained modules have a separate profile

On 2026-11-07, the six tiny synthetic cases passed the strict profile on native
CPU and NVIDIA GB10 CUDA at all 132 boundaries. Maximum absolute differences
were `1.073e-6` and `8.345e-7` respectively. The strict profile remains unchanged.

The released Stage 1 module exposed wider F32 reduction error. Its strict run
failed 432 CPU component checks and 71 CUDA checks, mostly in unnormalized
residual states. Overall maximum absolute differences were `1.564e-4` on CPU
and `1.488e-4` on CUDA. Those failed receipts remain failures; they are not
reclassified by the new profile.

An independent precision diagnostic ran the hash-pinned upstream
`MemoryModule.double()` with the same BF16-valued inputs and the exact released
weights widened to F64. Hooks recorded the upstream layers and residual states.
The comparison below is upstream PyTorch F32 against upstream F64, before
choosing a native trained-module acceptance profile.

| Stage 1 case and boundary | Maximum absolute error | Relative L2 error | Strict component failures |
| --- | --- | --- | --- |
| Ragged, second residual output | `1.390e-4` | `1.096e-6` | 31 |
| Duplicate document, second residual output | `1.465e-4` | `1.109e-6` | 10 |
| Ragged, final memory tokens | `4.099e-5` | `8.235e-7` | 1 |
| Duplicate document, final memory tokens | `4.777e-5` | `8.436e-7` | 0 |

Thus even the upstream F32 computation exceeds the strict floor against an F64
reference at approximately the native error scale. Unnormalized residual values
reach about 62, whereas the final token RMS is about 3.7. This supports distinct
absolute floors for residual computation and the final decoder input, together
with an independent normwise limit. F64 is a precision diagnostic, not a proof
of exact real arithmetic.

The constants for `trained-f32-v1` were frozen after this Stage 1 analysis and
before examining native Stage 3 numerical results. Stage 3 is the prospective
checkpoint validation; its thresholds must not be retuned to make it pass.
Task-local evidence is retained under `.benchmark-results/memorilla/`, including
the original strict receipts and `stage1-f32-f64-analysis.json`. Input, source,
bundle, and reference hashes accompany the generated evidence.

## Prospective Stage 3 result

The frozen profile passed the released Stage 3 checkpoint on native CPU and
NVIDIA GB10 CUDA, with zero failed boundaries and zero component mismatches
across all 132 boundaries of the six synthetic embedding cases. The verifier
source hash still matched the pre-run profile freeze record.

| Backend | Maximum boundary relative L2 error | Final memory maximum absolute error | Aggregate final memory relative L2 error |
| --- | --- | --- | --- |
| Native CPU | `1.653e-6` | `5.913e-5` | `8.922e-7` |
| GB10 CUDA | `1.563e-6` | `5.531e-5` | `5.806e-7` |

The maximum absolute error anywhere was `2.975e-4` on CPU and `2.518e-4` on CUDA.
An error can exceed the absolute floor while satisfying the full component
criterion, which includes `rtol * abs(reference)`. Every boundary also passed
the independently fixed `5e-6` relative L2 limit.

CUDA recorded 204 kernel launches, 102 linear operations, 18 attention operations,
and 42 affine LayerNorm operations. Resident F32 weights occupied 776,495,104
bytes. Its 55,531,520 device-to-host bytes exactly matched declared tracing
readbacks, with zero cross-backend synchronization fallback. These are execution
receipts for numerical checking, not production memory or latency guarantees.

The frozen profile, strict default, rejection behavior, normwise failure below
the component floor, and zero-reference handling passed 17 compiled-tool tests on
each backend. Native BF16 activation execution remains unqualified.

## Exact 4B encoder precision

The encoder's unchanged acceptance gates require each normalized component to
agree within `1e-4`, cosine similarity at least `0.99999`, and unit-norm error at
most `1e-4`. Native batched and singleton outputs must also agree under those
limits. Exact token IDs, raw document/question prefixes, trailing EOS `151643`,
last-token pooling, and all 2560 dimensions are separate admission checks.

The first exact production CUDA run failed this F32 comparison: maximum
component error was `0.00171024`, and batch/singleton error was `0.00111158`.
Its actual execution used 252 BF16 cuBLASLt calls with F32 normalization,
attention, residuals, and output. The reference used source BF16 weights widened
to F32 and F32 computation. The failed receipt remains unchanged at
`.benchmark-results/memorilla/encoder-reference-cuda-exact/encoder.json`.

Independent CPU diagnostics used the exact locked weights before adding a
separate strict-F32 CUDA diagnostic constructor:

| Comparison on the original four texts | Maximum component error |
| --- | --- |
| Upstream F32 against upstream F64 singleton oracle | `8.421e-7` |
| Independent BF16 linear-input staging with F32 output against F64 | `0.001268` |
| Independent staged-F32 batch against singleton | `0.0005033` |
| Fully BF16 upstream execution against F64 | `0.006513` |

The staging diagnostic rounds each `nn.Linear` input through BF16 while keeping
the source weights widened to F32 and retaining F32 outputs and other operations.
Small reduction differences can change subsequent BF16 rounding decisions.
The F64 diagnostic preserves upstream's explicit F32 RMSNorm and RoPE operations;
it uses unpadded singletons because upstream's F64 padding-mask floor overflows
its explicitly F32 softmax. This is precision characterization, not a new mixed
precision acceptance profile. No original gate was relaxed.

`build-memorilla-encoder-verify` builds an offline verifier with a dedicated
`createQwen3EmbeddingCudaReferenceSession` constructor. It admits the canonical
4B geometry, verifies the twelve compiled-lock artifacts and exact recipe, then
widens weights before GPU upload. The verifier independently checks all resident
weights are F32 and rejects low-precision staging, matrix operations, or recorded
host attention, RoPE, and cross-backend synchronization fallback. Normal model
loading continues to use its original mixed-precision serving policy.

The four-case strict-F32 result passed unchanged limits: maximum error against
the independent F32 oracle was `4.880e-7`; native batch/singleton error was
`7.749e-7`; minimum cosine was `1.0`. Actual execution recorded 362 kernels,
252 linear operations, 36 attention operations, 398 resident F32 weights
(16,087,097,344 bytes), and zero low-precision or recorded host fallback calls.
Final output readback is allowed. The receipt retains its explicit offline scope,
source-lock hash, and binary SHA-256
`6678ce24395a3ee17d379032166d4ca1d14a351d42c6a684eb5842d7cf18579c`.
See the README for the focused build and `encoder_reference.py --native-binary`
commands. This result does not qualify the default mixed-precision profile.

A subsequent held-out run used four new inputs of 7, 13, 145, and 138 tokens,
including Unicode, two long inputs, and a ragged padded batch. Its independent
oracle ran the exact upstream model in F32 on unpadded singleton inputs. The
unchanged gates passed for native batch versus oracle, native singletons versus
oracle, and native batch versus native singletons. Maximum errors were
`4.638e-7`, `4.638e-7`, and `5.122e-7` respectively; minimum cosine was
`0.99999988`. Every batch and singleton invocation independently verified the
locked artifacts and actual F32 GPU execution. The separate receipt is
`.benchmark-results/memorilla/encoder-holdout-cuda-strict-f32/holdout.json`.

Managed execution uses separate versioned manifests,
`qwen3_encoder_f32_v1` and `qwen3_decoder_f32_kv_f32_v1`. These select F32 before
weight upload through normal managed construction and retain model source,
device weights, fixed kernel workspace and request admission. They require the
pinned source inventory, canonical recipe and SM 12.1. The standalone diagnostic
constructors cannot provide this managed-profile proof. A held request lease
also supplies the physical owned-device allocation ceiling; caches and deferred
frees remain inside it until cleanup restores model-only residency. Vendor and
driver-private memory require deployment headroom outside that meter.

The ordinary mixed encoder's failed F32-comparison receipt remains a failure.
The explicit F32 manifests introduce a distinct execution identity; they do not
change an ordinary model's default arithmetic or widen any numerical limit.
Composed service and HTTP lifecycle receipts establish additional scope beyond
the eight strict-F32 encoder cases above.

## Managed composed result on GB10

The actual managed service passed the conditional native-query oracle with
binary `1c92ad6cd8ac35d372f78cf717207efede94aedccbb433e1eb52aa3f2cca7cf9`.
Two document rows came from the independent CPU encoder reference. Each request
encoded its question through the managed F32 CUDA encoder and required exact
stored FP16 and BF16 input bits against the captured query. The independent
CPU reader/decoder oracle consumed those same native query bits. This separates
cast-boundary variation from the reader and decoder arithmetic comparison.

The exact 53-token frame, sixteen memory slots, injected embeddings and all
151,936 logits at prefill and three cached steps passed the unchanged gates on
cold A and both warm repetitions:

| Comparison | Maximum absolute error | Maximum relative L2 | Component mismatches |
| --- | --- | --- | --- |
| Memory tokens | `3.2663345e-5` | `9.54542e-7` | 0 |
| Full-vocabulary logits | `4.7683716e-5` | `3.06074e-6` | 0 |

The logit relative L2 value is diagnostic; its acceptance criterion remains
the frozen component gate.

Cancellation after prefill refunded admission. A after cancellation and A after
a different 64-document, 128-token request had bitwise-identical logits to cold A.
The larger request exercised all 128 tokens with fresh F32 KV and checked resource
bounds; it had no 128-step CPU logit oracle. Actual execution guards checked zero
forbidden attention/RoPE/paged/cross-backend fallback and F16/BF16 counters under
held execution scopes. This trace does not emit positive dispatch totals.

The request used an explicit 112 GiB operator/process ceiling; model policy
resolved 88 GiB. Combined admission sampled every 10 ms peaked at 74.068 GiB
cold, 72.069 GiB warm A and 72.219 GiB for the larger request. Owned decoder
device buffers reached a lifetime peak of 127,549,864 bytes above their model
baseline, within 334,693,888 transient bytes held for that request. The reader
reached 3,635,200 bytes above baseline within its 144,703,488-byte allowance.
The physical allocator enforced those ceilings throughout execution, including
cached and deferred allocations. After each request, only the two managed models
remained: 48,850,038,784 F32 weight bytes and 1,052,672 fixed kernel bytes, with
zero request KV or scratch. Driver/library-private allocations remain outside
the owned-buffer meter and require deployment headroom.

Evidence and all bindings are in
`.benchmark-results/memorilla/composed-verify-eager-summary.json`, SHA256
`f066e05e5e6e99492479ad1f427d477404ad359b71112def90df40c6d0fa4873`.
The rejected pre-decoder replay-policy run remains a failed receipt. Its
constructor correction resolved the qualified eager policy before weight upload;
no arithmetic code or tolerance changed. These checks establish supplied-feature
composition, with HTTP lifecycle and corpus answer quality evaluated separately.

## Run the prospective gate

After generating the independent F32 Stage 3 reference, run from the worktree
root:

```sh
zig/pkg/inference/zig-out/bin/antfly-memorilla-verify \
  --bundle .benchmark-assets/memorilla/bundles/stage3 \
  --reference .benchmark-results/memorilla/stage3-f32 \
  --backend cuda --precision-profile trained-f32-v1
```

Use `--backend native` for the independent native CPU path. Keep the strict run
as a separate diagnostic. A module-only pass does not qualify Qwen prompt
injection, decoder cached generation, or the complete memory service.

## Qwen3-8B decoder precision and KV

The pinned Qwen3-8B decoder diagnostic widens source BF16 weights before CUDA
upload and requires F32 resident weights and activations. It rejects reduced
matmul staging and host attention, RoPE, or synchronization fallback. Every one
of the 151,936 vocabulary logits must satisfy the component formula with fixed
`atol=1e-3, rtol=1e-3`, at prefill and at each of three teacher-forced cached
decode steps. Those limits were frozen before the full-model runs.

On GB10, the F32-KV profile passed a 51-token synthetic-feature prompt, the exact
686-token long tokenizer fixture, and a 53-token prompt using independently
computed CPU encoder features. Worst absolute differences were `4.483e-5` at
prefill and `6.962e-5` during decode. All argmax values agreed. Each run recorded
1,447 kernels, 144 device KV reads and writes, and zero forbidden fallbacks or
low-precision matmul/staging calls. The real-feature run uses CPU encoder output
and reference memory readout; it does not establish a composed all-native service.

F16 KV is a separate storage and arithmetic contract. The generator's optional
`--kv-dtype f16` passes a `DynamicCache` subclass at the first forward. Each
update rounds post-RoPE K and V through FP16 before storing and returning F32
values to the CPU attention operation. This includes prefill attention, matching
the native path that reads its newly written F16 cache. The receipt label is
`BF16-source-weights-widened-F32; F32-activations; FP16-KV-roundtrip-F32`.
The native verifier accepts that label only with `--kv-dtype f16` and records
`reference_precision`. The default F32 reference policy is unchanged.

The original F16-versus-F32 comparisons remain failed results under the same
frozen limits:

| F16 computation versus CPU F32 oracle | Prefill maximum absolute error | Decode maximum absolute error |
| --- | --- | --- |
| Native CUDA F16 KV | `0.005163` | `0.005289` |
| Independent CPU FP16-roundtrip KV | `0.005227` | `0.005420` |

Both CPU references used bitwise-identical prompt embeddings, memory tokens,
input IDs and forced decode IDs. Native F16 versus F32 had 6,606 failing prefill
components and 10,718 across decode; all argmax values still matched. CPU F16
rounding independently exceeds the F32 comparison limits at a similar scale.
A paired F16 oracle checks the cache implementation against its declared
precision; it does not reclassify this quantization cost as F32 equivalence.

The paired F16 run also failed the unchanged component limits: 187 prefill
mismatches and 28, 604, and 13 across decode (832 total). Maximum absolute
difference was `0.002291`, with maximum tolerance ratio `1.65`. All argmax
values agreed, and device execution had zero forbidden fallbacks. F16 KV
therefore remains unqualified under the frozen `1e-3` gate, including against
the explicitly rounded oracle. The F32-KV compatibility baseline remains
qualified; neither F16 failure was removed or assigned a looser limit.

An additional upstream CPU diagnostic compared singleton execution with two
identical batch rows using the same embeddings and forced IDs. Both F32 and
FP16-roundtrip cache outputs were bitwise identical at prefill and all three
decode steps. This specific batch-shape test did not explain the remaining
paired CPU/CUDA F16 discrepancy. Its cause remains unresolved; no CUDA
arithmetic was changed to make the gate pass.

Ordinary BF16-weight generation passed a separate eight-token raw-prompt smoke,
with 288 device KV reads/writes and zero host attention/RoPE/paged fallback.
Its explicit backend cap was 48 GiB, while reported resident weights occupied
16,382,087,168 bytes and the loader's conservative backend peak reservation was
36,858,412,746 bytes. Configured caps, reservation estimates and measured
residency are different quantities. See the README for the complete budget argv.
