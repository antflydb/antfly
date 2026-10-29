# Model kernel families: share implementations, retain tuned specializations

Implementation status: [AZ outcomes](unified-execution-az-outcomes.md) records
shared CUDA row/statistics and chronological-attention owners, BF16 packed
products, Metal audio/channel rows and head statistics, plus device and model
qualification. Unique rotary/audio/geometry policies remain specialized; existing
CPU/vector and vision-attention sharing earns no new source-reduction credit.

The following contracts were planned from `4e90c8f8f6`. AY's earlier Glowworm
projection qualification is separate from the AZ device-family migration.

The recommendation is to move model names out of kernel implementation policy.
A model profile selects operation semantics, a proven shape range and a tuned
schedule. Backend owners instantiate that combination statically. Existing
native export names may remain small compatibility wrappers. CUDA/Metal/CPU
share operation contracts, layout admission and profile metadata; their inner
loops retain native scheduling and vectorization.

Do not replace the existing kernels with a single runtime-parametric path.
Compile-time specialization must remove unused policies and retain the current
fast shape paths. A specialization is admitted by dtype, layout, shape and
semantics, with a compatible fallback for other inputs. Model identity can stay
in the model profile where it selects an arithmetic contract; it need not be
repeated in the implementation body.

## Family contracts and cohort order

| Cohort | Concrete source anchors | Share | Preserve as specialization policy |
| --- | --- | --- | --- |
| 1. Fused row normalization and residuals | CUDA `embedding_gemma2.cuh`, `embedding_gemma2_optimized.cuh`, `prototypes/lighton_vision.cu`; corresponding Metal row kernels | Row traversal, reduction acquisition/publication, native parameter/admission and fused epilogue ownership | RMS versus LayerNorm, direct versus offset weights, shared-tree versus warp/block versus training vector-lane reduction, exact BF16/F32 conversion boundaries, input stride and thread count |
| 2. Packed projection consumers | CUDA `termite_embedding_gemma2_split_columns_bf16`, `_packed_heads_bf16`, `_packed_gelu_bf16`, LightOn packed QKV/vision epilogues; Metal equivalents | Packed plane addressing, column split, bias/activation/gating/residual phases | Plane widths, row/head layout, rotary position axes, activation, bias/residual ordering, rounding before versus after multiply and retained fusion boundaries |
| 3. Attention IO/layout variants | `vision_bf16_attention_m32n16.cuh`, encoder/vision adapters, existing local-attention owner | Reuse the existing tile/online-softmax owners for additional compatible consumers and statically select IO traits | Head size, query/key tiles, architecture, BF16/F32 IO boundaries, mask/scale/bias semantics, shared-memory layout and thread count |
| 4. Audio and geometry programs | Gemma audio row/channel/local attention and image pooling/patch operations | Geometry admission and matching row/convolution/reduction phases where a second compatible implementation actually exists | Relative-position score terms, validity masks, convolution indexing, exact image resize/quantization semantics and native backend loops |

Cohorts 1 and 2 are the first substantial prototype. Adopt the prototype only
when net maintained source is smaller after charging the new owners, metadata,
wrappers and admission code. Set a **20% net authored-device-source target for
the explicitly selected cohort**, then measure it. This is not a prediction of
20% fewer instructions or a 20% reduction across the entire kernel tree.

The CUDA EmbeddingGemma base/optimized files contain 74,778 and 13,949 bytes;
the LightOn vision prototype contains 6,506 bytes. Those are whole-file exposed
source inventories, including different operations and training code. They are
not duplication or removable-byte estimates. The 8,615-byte vision-attention
body already serves both native BF16 and F32-boundary wrappers, so that existing
sharing earns no new reduction credit. Likewise, `antfly_warp_block_statistics`
already owns several EmbeddingGemma reduction phases.

## Numerical contracts that cannot be inferred from the model name

- EmbeddingGemma normalization uses direct RMS weights. Its inference reduction
  is warp-then-block; its training implementation deliberately uses a different
  four-lane accumulation order. Preserve both schedules.
- LightOn vision LayerNorm uses a 256-thread shared-memory sum/square tree.
  Replacing it with the EmbeddingGemma warp reduction can change output bits.
- Packed EmbeddingGemma GELU rounds the activation to BF16 before multiplying
  by the other packed plane. A generic activation/multiply that rounds only its
  final output changes the model's arithmetic graph.
- LightOn bias and residual epilogues use explicit F32 add boundaries. Its
  two-axis rotary layout differs from a conventional one-axis decoder RoPE.
- Encoder attention, causal decoder attention, audio-relative attention and
  DeBERTa disentangled attention do not have interchangeable score functions.
  Share a compatible tile or softmax phase without deleting those semantics.

The descriptor should identify an admitted program and its relevant traits,
rather than instantiate every possible boolean combination. Keep native ABI,
parameter layouts and architecture specialization under the existing owners.
Use the current kernel compiler/catalog and backend template machinery where
it fits; avoid introducing a second general-purpose compiler or runtime
interpreter merely to reduce source spelling.

## Acceptance before replacing a family

1. Freeze the original native kernels and their input generation; baseline and
   candidate must use the same compiler, artifact policy and device clocks.
2. Compare independent references and original outputs across real model
   geometries, tail sizes, strides, masked/empty rows, precision boundaries,
   frame ownership and mutable CUDA graph replay. Keep bitwise contracts where
   they exist; declare tolerances before samples elsewhere.
3. Compare CUDA registers, spills, local/shared-memory usage, block geometry and
   retained tensor-core paths. On Metal, preserve threadgroup/simdgroup layout
   and scratch; on CPU, preserve ISA dispatch, vector widths and parallel grain.
4. On Glowworm, measure host submission and CUDA-event stream duration with
   original/candidate AA/AB/BA orders, controls and predefined tail limits.
   Include captured-batch replay to distinguish GPU work from direct enqueue
   starvation; report both metrics without substituting away a failed assay. Add
   full-model tokens/sec or examples/sec, time-to-first-output and retained
   memory for models that consume the migrated family. Metal must be measured
   on Apple hardware; Glowworm cannot qualify it.
5. Charge every new production owner and trait declaration in the net source
   ledger. Report authored device code, host orchestration, generated text and
   compiled image size separately. Keep a tuned specialization when a proposed
   shared implementation fails its performance or numeric gate.

AY's projection timing fixtures cover CUDA launch-family consolidation. Its
selected EmbeddingGemma/LightOn device tests establish useful pre-migration
correctness coverage. They do not constitute a full-model performance comparison
or qualification of this future migration. AV's Metal GPU p95 miss remains open.
