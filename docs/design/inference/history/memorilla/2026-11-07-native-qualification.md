# Memorilla native qualification

Qualification date: 2026-11-07. Host: Linux aarch64, NVIDIA GB10, compute
capability 12.1, driver 580.178.04. Source baseline: `c8afb96719` plus the
Memorilla worktree changes. The reference environment uses Python 3.12.3,
PyTorch 2.7.0 CPU, Transformers 4.53.3, and SafeTensors 0.5.3.

F32 component compatibility passes for all five memory modules and the exact
Qwen encoder/decoder pair. The production mixed encoder and F16 KV exceed the
frozen numerical limits and remain unqualified under those limits. Their
original failures and the unresolved paired-F16 discrepancy are retained.

## Checkpoint conversion

All five memory modules at Hugging Face revision
`f6b15af56aa687861c8d2d03b1cd8f7979881361` were acquired and checked against their
original SHA-256 values. Each passed strict tensor/configuration validation and
bitwise conversion round-trip to split-QKV SafeTensors: 194,123,776 BF16
parameters per module. Both the original checkpoint hashes and canonical
converted hashes are pinned independently of mutable bundle metadata.

Stage 3 original `memory.pt` SHA-256:
`782d4a84ac13a2a4e1b72be3284e078e752c2a595b96301741028539aa48305a`.
Its canonical converted weight SHA-256:
`3a7ec610dc29818c2aa29c3867d3be3e69908a191a94d0d6f0f416d564d3470e`.

## Native memory execution

The native reader executes the learned-slot question conditioning, two document
cross-attention/FFN blocks, affine LayerNorm, erf GELU, and final decoder-space
projection. Fixtures use normalized synthetic document/question vectors with
the upstream FP16 storage to BF16 input conversion, widened for F32 arithmetic.
They cover single-document, ragged, masked-padding, permutation, duplication,
and changed-question cases. The independent oracle also matched the unchanged
hash-pinned upstream `MemoryModule.forward` in F32 and BF16.

Tiny synthetic weights passed the unchanged `strict-f32-v1` profile at all 132
boundaries on CPU and CUDA. The released Stage 1 module exposed approximately
`1.5e-4` residual differences at that strict component floor. Those strict
failures remain recorded. An independent upstream F32 versus F64 comparison
showed the same reduction sensitivity before the trained profile was frozen.
See [numerical profiles](../../../../../zig/pkg/inference/scripts/memorilla/PRECISION.md)
for formulas, F64 evidence, and the prospective validation rule.

The `trained-f32-v1` profile was fixed before inspecting native Stage 3 results.
It requires intermediate absolute tolerance `2e-4`, final-token absolute
tolerance `5e-5`, relative tolerance `1e-5`, and relative L2 error at every
boundary at most `5e-6`. Component limits include the relative term; they are
not standalone maximum-absolute-error ceilings.

| Stage 3 result | Native CPU | GB10 CUDA |
| --- | --- | --- |
| Cases and compared boundaries | 6 and 132 | 6 and 132 |
| Failed boundaries and component mismatches | 0 and 0 | 0 and 0 |
| Maximum boundary relative L2 error | `1.653e-6` | `1.563e-6` |
| Aggregate final-token relative L2 error | `8.922e-7` | `5.806e-7` |
| Maximum final-token absolute difference | `5.913e-5` | `5.531e-5` |

The CUDA run reports 204 kernels, 102 linears, 18 attention operations, and
42 LayerNorm operations across the six cases. It retains 776,495,104 bytes of
F32 weights. Its 55,531,520 device-to-host bytes exactly match the explicitly
declared boundary readbacks, with zero cross-backend synchronization fallback.
These are numerical diagnostics, not serving throughput measurements.

Raw receipts are in the task worktree under
`.benchmark-results/memorilla/stage3-{native,cuda}-trained-f32-v1.jsonl`.
The frozen profile record and original strict failures are retained alongside
`stage1-f32-f64-analysis.json`. The tested module verifier executable SHA-256 is
`4e1ac6c978acbf5f4f0c94528a3dcf72e804387e58ad5163e30b12d91ec3f0bb`.

The same frozen profile also passed all five released checkpoints on both
backends: 1,320 boundary comparisons, zero failed boundaries or component
mismatches. Every reference matched the pinned upstream forward. Stage 1 and
Stage 3 receipts were reused after checking their artifact hash chains;
Stage 2, PV4, and PMv2 were generated and tested afresh. Across all ten runs,
the largest boundary relative L2 error was `1.894e-6`, below the `5e-6` limit.
Per-checkpoint receipts and CUDA dispatch/readback evidence are indexed in
`.benchmark-results/memorilla/all-checkpoints-trained-f32-v1-summary.json`.

## Qwen base model qualification

The exact source pair is pinned: Qwen3-8B at
`b968826d9c46dd6066d109eabc6255188de91218`, Qwen3-Embedding-4B at
`5cf2132abc99cad020ac570b19d031efec650f2b`. The native memory encoder recipe
uses full-width 2560-dimensional vectors, last-token pooling, L2 normalization,
empty question/document prefixes, and tokenizer trailing token 151643.

The new dense Qwen3 generator CUDA profile admits the canonical 8B SafeTensors
geometry and forbids host attention/RoPE fallback. Device capability discovery
on this GB10 reports the required primitives. A separate strict F32 reference
session widens BF16 weights before upload; normal mixed-precision serving is
a separate execution policy.

An independent random two-layer PyTorch Qwen model passed native CPU soft-input
prefill and three teacher-forced paged decode steps. Prefill maximum absolute
logit difference was `7.45e-8`; worst decode difference was `8.94e-8`, with equal
argmax at every step. Its seven integration tests also cover fixture/model
identity binding and invalid inputs. Synthetic models require explicit opt-in.

All roughly 24.5 GB of pinned base-model artifacts were acquired and verified.
The first production encoder CUDA run passed tokenization, recipe, residency,
and zero-fallback checks, but failed its initial F32 comparison gate: maximum
normalized-vector difference `0.00171024` versus the `0.0001` limit. Its mixed
BF16 matmuls and F32 residual/normalization arithmetic require a separate
precision analysis. The original failed receipt is retained under
`.benchmark-results/memorilla/encoder-reference-cuda-exact/encoder.json`.

A separate diagnostic constructor widens weights to F32 before CUDA upload
and leaves ordinary serving precision unchanged. The full 4B encoder passed
the original `0.0001` component and `0.99999` cosine gates through that path:
maximum reference difference `4.88013e-7`, batch-versus-singleton difference
`7.74860e-7`, and minimum cosine `1.0` in both comparisons. All 398 resident
weights were F32, totaling 16,087,097,344 bytes. Measured forward work included
362 kernels, 252 linears, and 36 attention operations, with zero low-precision
staging or matmul calls and zero recorded host attention/RoPE/synchronization
fallback. The strict receipt is
`.benchmark-results/memorilla/encoder-reference-cuda-strict-f32/encoder.json`.

Independent encoder analysis found F32-versus-F64 difference at most
`8.42e-7`, retaining upstream's explicit F32 RMSNorm/RoPE operations in the
F64 diagnostic. Rounding each linear input through BF16, while retaining F32 output
and other operations, produced a `0.001268` difference from F64 and
`0.000503` batch-versus-singleton variation. That supports keeping the mixed
and F32 arithmetic profiles separate; the original mixed-profile failure was
not reclassified as a pass.

Four independent holdout texts had token lengths 7, 13, 145, and 138, including
Unicode, long questions/documents, and ragged padding. Native F32 CUDA batch
and singleton vectors differed from the independent unpadded HF singleton
oracle by at most `4.638e-7`; native batch versus singleton difference was
`5.122e-7`. Minimum cosine was `0.99999988`. All original limits and execution
checks remained unchanged. The receipt is
`.benchmark-results/memorilla/encoder-holdout-cuda-strict-f32/holdout.json`.

The initial full Qwen3-8B F32 CUDA check passed at the predeclared `0.001`
absolute and relative logit limits. It compared all 151,936 logits for a
51-token soft-input prefill and three teacher-forced cached decode steps,
using the Stage 3 module with synthetic input features. Prefill maximum
absolute error was `4.4823e-5`; worst decode error was `6.9618e-5`. All four
argmax values agreed. Device evidence reported 1,447 kernels and 144 KV reads
and writes, with zero recorded host attention/RoPE/synchronization fallback
and zero low-precision activation staging or matmul calls. All 12 pinned decoder
artifacts were verified.

The same unchanged limits also passed a 686-token prompt and a 53-token prompt
built from real CPU encoder-reference features. Each compared prefill and three
cached decode steps, all 151,936 logits per step, and recorded the same positive
device KV work and zero forbidden fallbacks. All argmax values agreed.

| F32 KV decoder fixture | Prefill max absolute error | Worst decode max absolute error |
| --- | --- | --- |
| Synthetic features, 51 prompt tokens | `4.482e-5` | `6.962e-5` |
| Synthetic features, 686 prompt tokens | `4.101e-5` | `6.390e-5` |
| CPU encoder-reference features, 53 prompt tokens | `4.482e-5` | `5.531e-5` |

Against the F32 oracle, F16 KV failed those same component limits on 17,324
logits across the four rows, with maximum error `0.005289`; argmax still agreed.
An independent CPU cache that rounds post-RoPE K/V through FP16 also differed
from the CPU F32 oracle by up to `0.005420`. The approximation cost is retained
separately from the paired F16 cache implementation comparison.

The paired F16 CPU oracle also failed the unchanged `0.001` absolute/relative
gate: 832 component mismatches across the four rows, maximum error `0.002291`,
and maximum tolerance ratio about `1.65`. Its prompt embeddings, memory rows,
input IDs, and forced decode IDs were bitwise equal to the F32 fixture. All
argmax values agreed and device evidence remained clean. F16 KV is therefore
unqualified at this threshold; neither paired-reference agreement nor F32
equivalence is claimed. The paired failure remains in
`.benchmark-results/memorilla/qwen3-8b-cuda-cpuencoderfeatures-f16kv-paired-oracle.json`.
An upstream CPU singleton-versus-duplicate-batch diagnostic was bitwise identical
under both F32 and F16-rounded cache policies. That experiment does not explain
the remaining paired CPU/CUDA F16 discrepancy; its cause remains unresolved.

The decoder verifier consumes precomputed prompt embeddings from the independent
reference. This isolates its soft-input and KV behavior; it does not execute
the native memory reader inside the decoder run. Native encoder, native memory
readout, and decoder comparisons are separate component gates. A composed
native encoder-to-memory-to-decoder run remains a subsequent integration gate.

## Native text chat and deployment limits

The initial ordinary production smoke ran with BF16 weights and the default F16
KV cache, generating eight raw-prompt tokens successfully. Its declared limits
were host 32 GiB, backend 48 GiB, combined 64 GiB, and 2 GiB each for KV and
scratch. These caps are not measured residency: resident weights were
16,382,087,168 bytes, while the conservative backend load reservation was
36,858,412,746 bytes. Both the default 9 GiB and an explicit 32 GiB backend cap
rejected admission before execution. Exact arguments and measurements are in
`.benchmark-results/memorilla/actual8b-production-generation-smoke-admitted48g.json`.

That smoke also exposed a real native chat-template parser failure. The pinned
Qwen template requires reverse list slices, boolean identity tests, and string
affix/strip methods. These are now implemented and tested against 12 exact
Transformers-rendered text/inline-reasoning fixtures, including all three pinned
Memorilla prompt cases. The full template test suite passes; tool declaration
serialization is outside those fixtures' scope.

The rebuilt CUDA CLI passed a live templated generation with thinking disabled
and explicit F32 KV. Native rendered bytes and all 24 prompt token IDs exactly
matched the pinned Hugging Face tokenizer/template; it generated `Paris` for the
capital-of-France question. It reported 108 device KV reads/writes and zero
recorded attention/RoPE/paged fallback. The receipt is
`.benchmark-results/memorilla/actual8b-native-chat.json`.

The final CLI built all 28 steps with no source changes during compilation.
Its executable SHA-256 is
`494c775c2c6ef9648bfbdd5b5bc488d4b0a2ef99853f0219cc0f2f9236ab83ab`;
the source snapshot SHA-256 is
`50375d115891776a7632d911d5be8d9ec7b2124c2e9353af5c6d97521af73aca`.

## Reproduction and scope

Commands and artifact contracts are in the
[Memorilla tool guide](../../../../../zig/pkg/inference/scripts/memorilla/README.md).
The [implementation plan](../../../../plans/memorilla-native-memory.md)
defines the later database and Gemma training stages.

The local aggregate report `.benchmark-results/memorilla/full-validation.json`
indexes 14 evidence artifacts with their SHA-256 values, records the passing
F32 baseline, and retains each failed precision profile separately.

The complete Python suite passed 181 tests with no skips when both compiled
verifiers and the hash-pinned upstream source were supplied, with the native
memory integration tests using CUDA. Focused Zig checks also passed in CUDA
(20 tests) and CPU-only (9 tests) builds, plus two soft-input tests. The final
standalone CUDA CLI built successfully. Full-model numerical qualification is
limited to the explicit passing profiles and input sizes recorded above.

The passed module gate establishes the explicit F32 arithmetic profile. BF16
activation rounding, workload answer quality, concurrent encoder/decoder
residency, database source-policy integration, and production serving limits
require their own qualification.
