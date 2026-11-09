# Memorilla native inference and Gemma memory training

Status: active. Research date: 2026-11-07. Antfly source baseline:
`c8afb967195898139656c5ca463f493184ffc23f`. Memorilla source baseline:
`d8f18891a50bf33c18f04fe5167c22f51e58c9cd`. Paper: [version 3, November 3,
2026](https://arxiv.org/html/2610.21375v3).

PR integration is recorded against main at `c776a772e4`; see the
[refreshed validation and scope](../design/inference/history/memorilla/2026-11-09-pr-integration.md).
The supplied-feature native compatibility milestone is implemented. Database
collection integration, exact latent-only operational QA, native BF16 and Gemma
training remain separate work; the opt-in serving route stays disabled by default.

Checkpoint conversion and native memory-module execution are implemented.
All five released modules passed strict export validation and 1,320 boundary
comparisons on CPU and GB10 CUDA under the explicit trained F32 profile. The
exact 4B encoder and 8B decoder passed F32 CUDA component comparisons, including
long/ragged encoder inputs, a 686-token decoder prompt, and cached decoding.
The production mixed encoder's original strict F32 comparison failed and remains
recorded separately. The managed Stage3 F32 encoder-to-reader-to-decoder path now
passes the conditional CPU oracle, cancellation recovery, bitwise A/B/A isolation,
and a 64-document/128-token resource case under the unchanged numerical gates.
The supplied-feature inference API is implemented and disabled by default. The
current CLI passed 27 supervised HTTP cases, including concurrent isolation,
deadline recovery, and model-file drift rejection and recovery. The first
raw-document diagnostic completed execution but scored 0/22 factual answers
correct and 2/2 missing-fact abstentions correct. Workload answer quality remains
a blocking rollout gate; see the
[answer-quality evaluation](../design/inference/history/memorilla/2026-11-08-answer-quality.md),
[component qualification](../design/inference/history/memorilla/2026-11-07-native-qualification.md),
[production serving evidence](../design/inference/history/memorilla/2026-11-07-production-serving.md),
[serving guide](../design/inference/memorilla-serving.md), and
[tool guide](../../zig/pkg/inference/scripts/memorilla/README.md).

## Recommended direction

Build a native memory readout that converts a policy-authorized collection of
stored document embeddings into query-dependent soft tokens, then feeds those
tokens directly into an Antfly decoder. First reproduce the released
Qwen3-Embedding-4B plus Qwen3-8B model. Then train separate memory modules for
EmbeddingGemma 2 and dense Gemma4 decoders. Keep document evidence and Antfly's
existing temporal memory policy authoritative.

Antfly has a useful architectural advantage: it can own document identity,
embedding production, derived projection caches, query admission, and decoder
execution together. That makes incremental memory maintenance and bounded GPU
reads plausible product improvements. Their performance advantage still needs
measurement against the same models behind ordinary retrieval.

The first deliverable should be an offline native answer over a bounded
collection, with numerical parity against Memorilla's reference. Database
integration, large collections, and new training follow that gate.

Decisions to adopt:

- Preserve the released architecture and exact model pair for the compatibility
  milestone. Its checkpoint is a separate neural network, not a LoRA adapter.
- Preserve the released BF16 base and memory source weights. The initial
  qualified numerical baseline widens them to F32 and uses F32 KV storage.
  Keep mixed/BF16 activation arithmetic, F16 KV, and quantized deployment
  separate from that baseline.
- Expose memory tokens through an internal typed generation input. They are
  floating point input embeddings, not text tokens or precomputed decoder KV.
- Start with exact scans of explicitly bounded collections. Report any
  incomplete coverage; make approximate selection a separate mode.
- Train a new module for each encoder, embedding recipe, decoder, and prompt
  contract. Reuse the frozen encoder and decoder weights.
- Use EmbeddingGemma 2 at its full 768-dimensional output and Gemma4 E2B as the
  first compact Gemma pair. Define its per-layer embedding behavior before
  training.
- Use upstream Python training for the initial learning baseline, export to
  native serving, then add the native training recipe. Native CUDA inference
  does not depend on completing native training.

## What Memorilla contributes

Memorilla stores one pooled embedding per document or source unit. For a question,
a learned set of slots reads the question embedding and the entire collection.
The module projects those slots into the frozen decoder's input space. This
separates persistent document storage from question-specific compression.

The [paper](https://arxiv.org/html/2610.21375v3) reports five benchmark wins out of
eight with 16 memory tokens. FactKG reaches 84.33 versus 80.43 with full context;
PubMedQA reaches 59 versus 67. The benchmarks include downstream supervised
training, with dedicated personalization checkpoints. These are useful results,
but do not establish generalization to arbitrary enterprise corpora. TextWorld
uses reinforcement-trained memory alongside full history: it does not demonstrate
replacing that history. The latency experiment uses one H100 80 GB, batch one,
32 generated tokens, and up to 512 documents; it compares with full context under
Hugging Face generation without paged KV. It does not time RAG. Its reported
27-fold gain is an experimental result, not an Antfly serving target.

The [website](https://memory.stanford.edu/) makes the intended distinction from
RAG clear: query-conditioned access to the whole embedding collection can connect
weakly related facts, while exact retrieved text remains useful for names and
details. Antfly should evaluate latent memory together with a small grounded
retrieval path, especially for citations and exact values.

### The released network

The following contract comes from the pinned
[memory module implementation](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/memorilla/memory.py).
Let `B` be batch size, `N` document count, `D` encoder output width, `H` decoder
input width, and `K` memory slot count.

| Component | Released Qwen contract |
| --- | --- |
| Encoder | Frozen Qwen3-Embedding-4B, output width `D = 2560` |
| Decoder | Frozen Qwen3-8B, input width `H = 4096` |
| Persistent input | Documents `[B, N, D]`, question `[B, D]` |
| Initial slots | Learned `[1, 16, D]`, broadcast over the batch |
| Question conditioning | Slots attend to one question vector, then residual and LayerNorm |
| Document preparation | One shared LayerNorm over each document vector |
| Readout | Two pre-norm cross-attention and residual FFN blocks |
| Attention | Eight heads, head width 320, biased Q/K/V and output linears |
| FFN | Biased `D → 4D → D`, ordinary erf GELU |
| Final output | LayerNorm then biased `D → H`, yielding `[B, 16, H]` |
| Document self-attention | Disabled in the published checkpoints |
| Retrieval initialization | Disabled in the published checkpoints |
| Normalization | Ordinary affine LayerNorm, PyTorch default epsilon `1e-5` |
| Dropout | Configured for training, disabled for inference |

In schematic form, with all attention projections and biases retained:

```text
S = broadcast(learned_slots)
S = LN_query(S + MHA_query(S, question, question))
X = LN_docs(document_embeddings)
repeat for each of two blocks:
    S = S + MHA_block(LN_cross(S), X, X, padding_mask)
    S = S + FFN_block(LN_ffn(S))
M = output_linear(LN_final(S))
```

The memory module uses noncausal attention. It has no RoPE, QK RMSNorm,
document position encoding, or document timestamps. Document order alone cannot
express recency; source time must be represented in the encoded content or in a
new trained architecture. All eligible documents participate in the softmax.
Replacing them with ANN top-k changes the computation.

Question conditioning attends to a single vector. At evaluation its attention
weight is always one, so the question's value and output projections determine
the conditioning addition. Fuse that computation only after validating the
literal implementation. Retain the original tensors in the artifact for strict
checkpoint compatibility.

For the default two-block architecture, counting biases and learned slots gives
`28D² + DH + (K + 36)D + H` parameters: 194,123,776 for the released pair, or
about 370.3 MiB of BF16 tensors. It is small relative to the two base models,
but substantial enough to need its own residency and admission accounting.

### Embedding and prompt contracts

The pinned [embedding pipeline](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/memorilla/embeddings.py)
uses vLLM's pooled embedding output with no instruction prefix for either
documents or questions. The official encoder sidecars and pinned vLLM 0.9.1
adapter use last-token pooling followed by L2 normalization. Cached vectors are
FP16; the module consumes BF16.
Preserve the actual tokenizer, pooling, normalization, truncation, and cast
sequence through reference fixtures. Antfly's ordinary query instruction must
not be added automatically. Equal dimensions or high cosine similarity do not
prove compatibility with the trained readout.

The [decoder wrapper](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/memorilla/llm.py)
inserts a memory region in the system message. It replaces the embedding rows
for 16 `<|memory|>` placeholders with `M`; surrounding `Memory` delimiters and
the selected system prompt remain text. The user question remains in the chat
prompt. Match the Qwen chat template and `enable_thinking=False`, special token
handling, attention mask, and answer extraction. The upstream training wrapper
adds a placeholder token and resizes its embedding table. Native serving can
construct the final sequence directly, but must match the framed token sequence
and account for added-vocabulary logit differences against the training wrapper.

The [vLLM serving wrapper](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/memorilla/vllm.py)
also supplies prompt embeddings. Use it as an additional end-to-end reference;
keep numerical module fixtures independent of serving-stack sampling.

Upstream injection computes `original + (memory - original)` using BF16
`scatter_add`. Direct replacement can differ because subtraction and addition
round separately. Capture the actual injected rows in fixtures and declare whether
the profile reproduces that arithmetic or the intended replacement semantics.
See the [injection tests](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/tests/test_vllm.py).

### Checkpoint selection

The [Hugging Face release](https://huggingface.co/memorilla/Memorilla-Qwen3-8B)
contains `stage1`, `stage2`, `stage3`, `pv4`, and `pmv2`. Each directory contains
`config.json` and `memory.pt`. The general Stage 3 release is `epoch-00`; the
personalization checkpoints branch from Stage 1. Treat those as distinct tasks
and artifact identities. A folder named Stage 3 does not imply it is the final
epoch of the five-epoch training recipe.

Start native parity with Stage 3 and fixed inputs, then load and smoke every
released configuration. Keep Stage 2 as a general-task comparison and Stage 1
as a reconstruction/training diagnostic.

Pin the release at HF revision `f6b15af56aa687861c8d2d03b1cd8f7979881361`.
The Stage 3 `memory.pt` SHA-256 is
`782d4a84ac13a2a4e1b72be3284e078e752c2a595b96301741028539aa48305a`;
its reported artifact size is 388,258,057 bytes. This identifies the artifact to
download and verify; the weights have not been executed as part of this analysis.
Exact pretrained encoder and decoder revisions still need pinning in Stage A.

### Results and reproduction limits

The [project results](https://memory.stanford.edu/#results) provide the following
comparison. Values are percentages or scores scaled to 100. These task-specific
metrics do not make their average a calibrated measure of memory quality.

| Task and metric | Full context | Strongest baseline | Memorilla |
| --- | --- | --- | --- |
| PersonaMem-v2 accuracy | 27.92 | 30.90 | 44.60 |
| PersonalizationV4 accuracy | 59.12 | 67.88 | 66.65 |
| PubMedQA accuracy | 67.00 | 69.00 | 59.00 |
| FactKG accuracy | 80.43 | 80.43 | 84.33 |
| NarrativeQA ROUGE-L | 17.99 | 20.51 | 19.31 |
| TriviaQA exact match | 52.60 | 56.60 | 57.00 |
| LaMP-4 ROUGE-L | 14.21 | 16.10 | 18.15 |
| LaMP-7 ROUGE-L | 43.71 | 46.84 | 48.37 |

The [evaluation implementation guide](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/docs/evaluation.md)
uses embedding-to-answer-choice matching for personalization and FactKG rather
than literal exact match. Keep its Qwen3-Embedding-4B scoring encoder and recipe
fixed when changing the memory encoder to EmbeddingGemma; a changed scorer is a
separate metric. TriviaQA evaluates 500 held-out questions drawn from validation
(packaged as the release's test split), and PubMedQA 100; use
uncertainty intervals when interpreting small score differences. Baseline and
memory prompt protocols differ, and oversized full-context prompts can be
truncated. Reproduce the upstream protocol for compatibility, then run a
controlled same-prompt/same-budget comparison for product decisions.

Resolve three release differences explicitly:

- The paper's mean-pooling description differs from the released last-token
  embedding path. Treat actual reference vectors, official sidecars, and
  [vLLM's pinned adapter](https://github.com/vllm-project/vllm/blob/v0.9.1/vllm/model_executor/models/adapters.py)
  and [pooler](https://github.com/vllm-project/vllm/blob/v0.9.1/vllm/model_executor/layers/pooler.py)
  as the checkpoint compatibility contract.
- Released personalization modules descend from Stage 1; the current
  [personalization recipe](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/configs/personalization_pv4.yaml)
  starts from Stage 3. Running today's recipe does not establish release ancestry.
- The release publishes Stage 3 epoch 00, while the current training guide allows
  validation-based choice among saved epochs. Record per-benchmark checkpoint
  selection and never choose on test scores.

The paper's architectural ablations use separately trained 4B decoders.
Query-aware soft readout beats parameter-matched pooling/top-k alternatives;
larger slot budgets do not consistently help. These trends cannot establish the
released 8B checkpoint's performance at different slot counts or encoder sizes.

## Antfly capability audit

These findings describe the inspected source baseline, not a completed
Memorilla implementation.

| Area | Existing code | Work needed |
| --- | --- | --- |
| Qwen3 model family | GPT parsing and execution understand Qwen3 | Qualify the exact 8B generator and 4B encoder weights |
| CUDA Qwen3 sessions | `session_factory` admits Qwen3 as a last-token embedder | Add an explicit dense Qwen3 generation CUDA profile and verify prefill, paged KV, and decode |
| Managed Qwen embeddings | Immutable catalog covers the 0.6B model | Add a pinned 4B artifact and full 2560-dimensional recipe |
| Soft input embeddings | GPT forward and prepared multimodal prefill accept embedding tensors | Add a model-neutral internal memory input and service routing |
| Gemma4 inference | Embedding input path computes per-layer vectors using token IDs and initial hidden states | Define a memory-token PLE and scaling policy and preserve it across training and serving |
| EmbeddingGemma 2 | Managed, pinned default embedder; 768/512/256/128 output widths | Pin a memory-specific task recipe, re-encode training collections, train new readout |
| Fine-tuning | Graph autodiff, Gemma input nodes, non-LoRA trainable parameters, CUDA optimizers | Compose the memory graph with a frozen decoder and qualify input-gradient propagation |
| Native memory | Evidence, visibility, temporal recall, lifecycle, source fences | Add an eligible collection stream and latent answer execution under the same policy |
| Derived artifacts | Source identities, guarded publication, replay and coverage | Maintain optional versioned memory K/V projections without making them authoritative |

Concrete extension points:

- [CUDA session selection](../../zig/pkg/inference/src/architectures/session_factory.zig):
  the `.gpt/.qwen3` branch currently returns `.qwen3_embedding` only for a
  last-token decoder embedder and otherwise returns null. Do not bypass this
  capability gate to get an apparent success through another backend.
- [Qwen embedding catalog](../../zig/pkg/inference/src/registry/qwen3_embedding_catalog.zig)
  and [registry](../../zig/pkg/inference/src/registry/registry.zig): resolve and
  hash the exact model pair, tokenizer, and processor recipe.
- [GPT execution](../../zig/pkg/inference/src/architectures/gpt.zig):
  `forwardFromEmbeddings`,
  `forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides`, and
  `computePleVectors` supply useful building blocks.
- [Generation pipeline](../../zig/pkg/inference/src/pipelines/generation.zig):
  `executePreparedMultimodalPrefill` and
  `forwardPreparedEmbeddingsLastLogits` show embedding-prefill ownership and
  subsequent decode. Introduce a neutral preparation type rather than treating
  memory as fake image input.
- [Backend primitives](../../zig/pkg/inference/src/ops/ops.zig) and
  [CUDA cross-attention](../../zig/pkg/inference/src/ops/cuda/cuda_compute.zig):
  ordinary `layerNorm`, `geluExact`, linears, and rectangular `crossAttention`
  are present. The generic CUDA path is a correctness starting point; its
  single-query fast path does not cover the 16-slot readout.
- [Gemma graph](../../zig/pkg/inference/src/architectures/gemma_graph.zig),
  [multimodal autodiff](../../zig/pkg/inference/src/finetune/gemma4_multimodal_real_autodiff.zig),
  and [trainer](../../zig/pkg/inference/src/finetune/real_autodiff_trainer.zig):
  connect the memory output node to `GemmaInputs.input_embeddings`, with only
  memory weights registered as trainable.
- [Memory service](../../zig/pkg/antfly-embedded/src/memory/service.zig),
  [model bridge](../../zig/pkg/antfly-embedded/src/memory/models.zig), and
  [contracts](../../zig/pkg/antfly-embedded/src/memory/types.zig): reuse
  authorization and inference fences, while adding a separate bounded source
  enumeration path. Existing ranked recall has a 4096-candidate work ceiling
  and a rendered token budget; it is not an all-collection tensor scan.
  The model bridge's query role calls `ManagedEmbedder.embedQueries`; select an
  explicit memory encoding recipe so ordinary query instructions cannot leak
  into released Qwen checkpoint execution.

The existing [training design](../../zig/pkg/inference/finetuning/FINETUNING.md)
already requires explicit CUDA execution and optimizer evidence. Preserve that
standard for the new recipe. Current dense Gemma graph support is a better
starting point than Gemma4 MoE training, which has separate unsupported paths.

## Proposed serving architecture

```text
Immutable source versions
    -> model-aware chunks or episodes
    -> pinned encoder recipe
    -> reusable document embedding artifacts
    -> optional module-specific document K/V artifacts

Question + authenticated scope + temporal policy
    -> authorized collection snapshot and coverage check
    -> question embedding
    -> two exact memory readout layers over eligible vectors
    -> 16 soft tokens + framed text prompt
    -> decoder prefill + ordinary paged KV decode
    -> answer + execution provenance + grounded evidence when requested
```

The source unit is explicit: a conversation episode, evidence document, chunk,
or current assertion representation. Benchmark replication uses the upstream
unit and chunking policy. The first product mode uses one unit type per
collection. Combining raw episodes with curated current assertions can duplicate
facts and introduce superseded guidance; evaluate that as a separate recipe.

### Artifact identity and publication

Preserve source references and coordinates using the existing
[enrichment architecture](../../zig/ENRICHMENTS.md) and
[model-aware chunk contract](model-aware-media-chunking.md). Reuse an existing
embedding only when its full recipe identity matches. A normal search index may
use different query instructions, dimensions, or vector quantization.

The proposed memory bundle manifest records:

- Memory checkpoint SHA-256, strict architecture config, tensor layouts and
  dtype, exporter version, and upstream repository revision.
- Encoder and decoder immutable revisions, artifact hashes, tokenizer hashes,
  pooling/normalization, task rendering, chunking/truncation, and cast policy.
- Memory slots, attention heads, LayerNorm epsilon, GELU mode, prompt template,
  Qwen thinking setting, and Gemma soft-token scale/PLE policy where applicable.
- Supported backend/dtype profiles and the conformance fixture digest.

Raw embedding identity depends on the source version and encoder recipe. Cached
document K/V additionally depends on the memory checkpoint, layer, document
normalization, and projection recipe. A decoder-only prompt change should not
force document re-encoding; a changed encoder recipe must. A changed memory
module invalidates its K/V projections but can retain compatible raw vectors.

Prepared soft prompts and decoder KV have a different identity: include the
question, authorized collection/control revision, encoder, memory module,
decoder, text framing, and PLE policy. Do not key them only by document cache
identity. Recheck current authority even when an execution artifact is cached.

Upserts, deletion, TTL expiry, forgetting, and policy changes invalidate eligibility
immediately. Background cache cleanup is separate. Apply derived publication
through existing leader-owned replay and source/configuration fences. An old
inference result cannot republish vectors for a retired source.

### Collection and authority contract

An exact collection is the set of eligible units for a declared source scope and
revision. Before readout, filter tenant, space, principal visibility, source
retention, requested valid/known time, and lifecycle. Use the
[native memory policy](../../zig/MEMORY.md), including the existing inference
boundary checks. Soft tokens and document projections retain the authority of
all their inputs.

Current-assertion memory should encode the eligible current representation;
raw historical episodes remain a separate mode. The released module cannot
reliably enforce temporal supersession by document order. Keep historical and
current semantics outside the neural readout.

Return the admitted collection revision, source unit type, eligible/encoded/read
counts, coverage watermark, exact or approximate mode, and fallback reason.
If ingestion has not encoded every eligible unit, either wait within a bounded
deadline or use an explicitly reported fallback. Never call a capped scan exact
over the entire source collection. Handle an empty collection through an explicit
closed-book or grounded retrieval fallback rather than all-masked attention.

Generation must revalidate the admitted input closure after inference lease
transitions and before further provider calls or answer publication. Concurrent
forgetting or revocation cancels the old execution. Reuse the service-owned
boundary; do not add a parallel authorization mechanism.

### Worker and resource ownership

GPU builds execute in a supervised inference worker, as described in
[standalone ownership](../../zig/STANDALONE.md). Co-location does not imply one
address space or free database-to-GPU transfers. Pass bounded vector batches or
versioned internal handles across the worker boundary, and measure the transfer
cost. Keep GPU tensor handles internal to their owner and worker generation.

Admit encoder, memory readout, decoder, KV, staging, and projection-cache memory
together through existing resource owners. A pressure response can shrink a
physical tile without changing the collection semantics. Switching to top-k
changes semantics and requires an explicit mode/fallback report. Cancellation
must wait or fence device work before freeing referenced buffers.

### Answer and evidence surfaces

Add an internal `MemoryReadout` input to generation first. A proposed product
surface is a distinct latent answer option over a native memory space or an
authorized ordinary table collection. Preserve ordinary `recall` as a structured
record API; 16 latent tokens cannot be rendered as its existing cited items.
Design the public request after native parity and policy tests.

Support three explicit experiment modes: exact latent readout, ordinary grounded
retrieval, and latent readout plus a small textual evidence set. Attention weights
can diagnose readout behavior but are not verified citations. The hybrid mode
must verify source support before presenting a claim as cited. Opaque source IDs
inside a latent vector cannot provide a reliable evidence trail on their own.

Public API work follows the repository's schema-generation contracts. Native
memory types and `scripts/generate_memory_openapi.py` own `/memory/v1`; ordinary
inference schemas own `/ai/v1`. Generate bindings and add HTTP/MCP/embedded
conformance for whichever surface is introduced.

## CUDA implementation and collection scaling

### Correctness path

Implement the literal memory module on the common graph/backend interface,
with a native CPU reference and CUDA execution. Start with existing matrix
multiplication, affine LayerNorm, exact GELU, residual, and softmax primitives.
Use explicit Q/K/V splits from PyTorch's packed `[3D, D]` projection layout and
preserve output and FFN biases. Validate transpose and reshape rules with
intermediate fixtures.

The memory head needs unpositioned noncausal MHA with head width 320 for Qwen,
and initially 96 for an eight-head, 768-wide Gemma module. Model self-attention
kernels specialized for causal heads of width 128 or Gemma head shapes do not
establish support. A bounded GEMM plus stable softmax path can establish parity
before a fused implementation. Pin padding polarity: upstream `True` means
masked. Prevent padded or unauthorized rows from entering softmax at all.

For the initial end-to-end run, use precomputed upstream vectors to isolate the
new module and decoder path. Then replace document and question encoding with
native CUDA and compare the whole answer path. Require backend receipts so a
host implementation or unsupported-session fallback cannot masquerade as CUDA.

### Cost and projection caching

For `L = 2` readout layers, literal document key/value projection costs
`O(L N D²)` per query. The attention read costs `O(L K N D)`, with slot
projection and FFNs adding `O(L K D²)` and output projection `O(K D H)`.
The fixed decoder token count does not make database reads constant in `N`.

Because the released document vectors do not undergo self-attention, compute
`LN_docs(x)` and each layer's document K/V once for a frozen module. This moves
the `N D²` work to ingestion/cache construction. Both readout layers still scan
the eligible K/V, and the second uses slots updated by the first. Confirm
precision and rounding parity before advertising this as the exact profile.

Calculated storage below assumes two-byte vectors, two layers, separate K and V,
and excludes metadata, raw text, indexes, replicas, allocation slack, and temporary
build overlap:

| Per million document units | Qwen width 2560 | Gemma memory width 768 |
| --- | --- | --- |
| Raw document vectors | 4.77 GiB | 1.43 GiB |
| Two-layer K/V cache, additional | 19.07 GiB | 5.72 GiB |
| Raw plus K/V | 23.84 GiB | 7.15 GiB |

Choose raw-only versus projection-cached execution from measured collection
reuse and memory pressure. Keep raw vectors durable; treat K/V as rebuildable.
Cache construction and eviction must not duplicate uncontrolled GPU residency.

### Exact scans before approximate memory

Tile document reads and use stable online softmax so scratch need not grow as
`K × N`. For each slot/head and tile, retain maximum `m`, exponential sum `l`,
and weighted-value numerator `u`. Two partial reductions combine as:

```text
m = max(m1, m2)
l = exp(m1-m)*l1 + exp(m2-m)*l2
u = exp(m1-m)*u1 + exp(m2-m)*u2
attention_output = u/l
```

This also provides an exact-in-real-arithmetic distributed design: broadcast
the layer's slot queries, reduce partial statistics globally, finish the layer,
then repeat with the updated slots. Do not average per-shard memory outputs;
their separate softmax denominators and nonlinear FFNs make that incorrect.
Deduplicate shard migration overlap by source identity and bind the distributed
read to an admitted collection manifest. Floating point reduction order needs
its own tolerance and determinism policy.

Defer distributed readout until the local path establishes the benefit. Broad
collections can saturate GPU memory bandwidth and network links even with only
16 output tokens. Start product limits from measured latency and admission,
not a claim that millions of documents fit cheaply.

Later alternatives include ANN candidate selection, entity/time partitions,
learned hierarchical summaries, smaller memory width, and quantized K/V. Each
has a separate quality/cost experiment. Semantic authorization filtering is
always mandatory and does not count as approximate retrieval; relevance pruning
does. Hierarchical memories or a changed document encoder require training and
must not be labeled released-checkpoint parity.

## Gemma and EmbeddingGemma training strategy

### Separate encoder and decoder changes

Use the following experiment matrix to distinguish representation loss from a
decoder-injection failure. Each learned entry is a separate module and manifest.

| Experiment | Encoder | Frozen decoder | Purpose |
| --- | --- | --- | --- |
| Q0 | Qwen3-Embedding-4B | Qwen3-8B | Released compatibility and evaluation anchor |
| G1 | EmbeddingGemma 2, full 768 output | Qwen3-8B | Isolate replacement of the encoder and the store |
| G2 | Qwen3-Embedding-4B | Gemma4 E2B | Isolate decoder injection and PLE policy |
| G3 | EmbeddingGemma 2, full 768 output | Gemma4 E2B | Compact all-Gemma candidate |
| G4 | EmbeddingGemma 2 | Gemma4 E4B | Scale the decoder after G3 passes |

G1 and G2 can be small diagnostic pilots; they need not both receive a full
three-stage training campaign. Prioritize G3 for the product if those pilots
validate the interfaces. Do not transplant Qwen module weights into Gemma and
interpret the output as a trained analog. Distillation or partial initialization
is a later measured alternative to training the new module from scratch.

The official [EmbeddingGemma 2 config](https://huggingface.co/google/embeddinggemma-2/blob/main/config.json)
distinguishes a 512-wide text backbone from a 768-dimensional output. Use the
served output, not a hidden-size heuristic. Start with 768 dimensions and eight
memory heads, giving head width 96. With the same architecture and E2B's
1536-wide decoder, the formula above gives 17,736,192 parameters, about 33.8 MiB
in BF16. This is a calculated architecture size, not a quality result.

EmbeddingGemma's supported Matryoshka widths are useful later cost experiments,
but each width changes the memory input and requires a trained/qualified bundle.
Its normal retrieval task rendering can be used for the new module if training
and serving match exactly. Choose and pin the document task and question task;
do not inherit Memorilla's no-prefix recipe accidentally. Recompute both document
and question vectors. Existing Qwen caches are not reusable training features.

### Gemma4 soft tokens and per-layer embeddings

The [E2B configuration](https://huggingface.co/google/gemma-4-E2B-it/blob/main/config.json)
uses hidden width 1536, 35 layers, and per-layer input width 256. In
[Transformers' pinned Gemma4 implementation](https://github.com/huggingface/transformers/blob/c1c34249fa27deefbd4a377dfbf883a39baf5c6d/src/transformers/models/gemma4/modeling_gemma4.py),
supplying only arbitrary `inputs_embeds` can enter a reverse vocabulary lookup
when per-layer inputs are absent. Learned soft vectors do not have vocabulary
identities. Training needs an explicit per-layer input path.

Proposed initial policy, modeled on the existing media-placeholder behavior:

1. Put the memory module output directly in the decoder's initial activation
   units. Apply the normal embedding scale to text lookups; do not apply it
   again to already-scaled memory outputs.
2. Carry real token IDs for text and a fixed PAD identity for memory slots into
   the PLE token lookup. These are identity carriers: memory positions remain
   visible with attention mask one.
3. In the Python reference, pass the token-identity PLE lookup explicitly through
   `per_layer_inputs`. Let the decoder compute its context projection from the
   actual combined initial hidden sequence and combine those branches once.
   Passing already-combined vectors into this argument would combine them twice.
4. Implement equivalent final per-layer values with Antfly's `computePleVectors`
   and Gemma graph, matching normalization and scale at the API boundary.
5. Keep native Gemma BOS/chat framing, answer delimiters, positions, and stopping
   policy. Establish the memory insertion location as a new training contract.

PAD-identity PLE, zero token PLE, and learned memory PLE are different designs.
Train the initial PAD-identity policy consistently; compare alternatives only
after it learns. A mask derived mechanically from PAD IDs would hide all memory
slots and must be tested explicitly. Validate memory gradients through both the
main activation and the context-derived PLE branch.

Gemma4 12B has a [configuration without PLE](https://huggingface.co/google/gemma-4-12B-it/blob/main/config.json),
but uses `Gemma4UnifiedForConditionalGeneration`. It is a possible later experiment,
not an assumed drop-in route through Antfly's existing dense Gemma4 trainer.
E2B/E4B keep the first campaign aligned with inspected runtime support.

### Training objective and curriculum

Freeze encoder and decoder weights; train memory slots, attention, normalization,
FFNs, and output projector. Use teacher-forced answer cross-entropy, masking
instruction, document, placeholder, and question positions out of the answer
loss. Frozen decoder parameters still participate in backward propagation to
the memory inputs. Running the decoder inside `no_grad`, detaching memory
embeddings, or using cached answer hidden states would prevent the required
learning.

Mirror the released curriculum before architectural experimentation. These
starting hyperparameters come from the pinned
[Stage 1](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/configs/stage1_enwiki.yaml),
[Stage 2](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/configs/stage2_mixture.yaml),
and [Stage 3](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/configs/stage3_multitask.yaml)
recipes; retune the new Gemma module on validation data.

| Stage | Learning task | Starting recipe |
| --- | --- | --- |
| 1 | Reconstruct/paraphrase EnWiki content from encoded units | One epoch, LR `3e-4`, length 1024, warmup 3%, batch 4 per GPU, accumulation 8 |
| 2 | Reading comprehension, QA, and summarization mixture | One epoch, LR `3e-4`, length 1024, same batch/accumulation and warmup |
| 3 | Multi-task benchmark or Antfly domain training | Up to five epochs, LR `1e-4`, length 2048, warmup 10%, batch 2 per GPU, accumulation 8 |

The upstream recipes specify four GPUs, yielding effective batch sizes 128,
128, and 64 respectively. The Stage 2 mixture is SQuAD v2, DROP, CoQA, PubMedQA,
QuAIL, PWC, CNN/DailyMail, SAMSum, DialogSum, and MS MARCO. The upstream Stage 3
recipe includes PV4, PMv2, FactKG, TriviaQA, NarrativeQA, PubMedQA, LaMP-4, and
LaMP-7. Stage names identify a curriculum, not evidence of task-independent
performance. Keep downstream-trained and unseen-domain results separate.

Split Antfly datasets by source collection, user, organization, and time as
appropriate. A document collection shared between train and test can leak
personalization or corpus-specific knowledge. For private deployments, use
explicitly admitted training data and honor source retention; global checkpoints
must not silently absorb tenant memory. Deleting a vector removes it from
readout but cannot erase a fact already learned into trained weights.

Add an Antfly domain stage with multi-document connections, corrections and
supersession, ambiguous identities, exact identifiers/numbers, insufficient
evidence and abstention, irrelevant collections, prompt injection in source
content, and very unequal document counts. Encode source/time metadata using a
declared rendering recipe, and train on that recipe. Include empty-memory and
irrelevant-memory controls so the decoder can operate when memory is unhelpful.

For large-N training, mix collection sizes and distractors and evaluate beyond
the training count. Efficient document caching does not remove attention dilution
or information loss from a fixed slot budget. Compare 8/16/32 slots and 1/2
readout blocks only after the 16-slot baseline works; slot count is an artifact
property, not an unrestricted serving knob.
Include shuffled-question and noise-document controls to establish that the
trained module uses question-specific evidence rather than task priors alone.

### Training infrastructure and budgets

Use the [upstream trainer](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/train.py)
to establish a short overfit run and then a learning baseline. Parameterize
actual pooled output width, decoder embedding scale, chat rendering, explicit
Gemma PLE, and frozen-parameter behavior. Do not assume changing two model IDs
in the Qwen recipe is sufficient.

The pinned [upstream dependencies](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/pyproject.toml)
use Torch 2.7.0, vLLM 0.9.1, and Transformers 4.53.3. Preserve that environment
for Qwen reproduction; choose and pin a modern supported environment for
Gemma4/EmbeddingGemma 2 and port the small memory network. Resolve nested
`text_config` and actual pooled output width explicitly: the Qwen trainer's
top-level `hidden_size` assumptions do not cover these models.

The [upstream training guide](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/docs/training.md)
estimates 6/4/9 hours for the three stages on four H100 80 GB GPUs: roughly 76
GPU-hours, excluding new embedding preparation. Use that as a Qwen reference
budget, not an estimate for Gemma. The
[data guide](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/docs/data.md)
describes hundreds of GiB of data and cached features. Audit dataset-specific
licenses and available disk before a full download; fetch the parity and pilot
subsets first. Precompute new encoder features once per immutable collection
and recipe, retaining collection-to-document boundaries and question row order.
Start with AdamW, weight decay `0.01`, gradient clipping `1.0`, BF16 mixed
precision, and cosine decay to 10% of peak learning rate, matching the reference
training guide; validate the new module's learning rate rather than importing
decoder LoRA defaults.

For the first native training recipe, compose the memory graph directly with
the Gemma decoder graph. Register memory weights as regular trainable parameters,
keep all base parameters frozen, and optimize the answer loss. Existing fixed
multimodal embeddings demonstrate a graph input, not the completed upstream
memory gradient path. Add literal noncausal attention and affine LayerNorm/GELU
backward support at required shapes where missing.

Disable inference projection caches during baseline training: document K/V depend
on trainable parameters and must remain in the active autograd graph. Even fresh
detached K/V would remove gradients to their projections and document LayerNorm.
A later custom checkpoint/recomputation path must reconstruct those operations
during backward. Frozen encoder document features can be cached safely.

After SFT works, compare teacher-text distillation from full-context Gemma or
Qwen on the same tasks. Logit KL requires compatible vocabularies and tokenization;
use sequence/text supervision across families unless an explicit alignment is
implemented. Raw soft-token coordinates from different embedding spaces are not
aligned targets. Decoder LoRA is a later experiment that changes the frozen-decoder
premise. Reward optimization of the memory module alone can retain frozen base
models, but still needs separate rollout, reward, and evaluation qualification.

## Implementation stages and acceptance gates

The stages below are dependency boundaries, not calendar estimates.

### Stage A Pin references and export fixtures

Deliver a strict offline exporter from `memory.pt` to safetensors plus a native
bundle manifest. Use `torch.load(..., weights_only=True)` in the conversion tool,
validate names/shapes/config, preserve biases, and split packed projections
explicitly. Native serving must not need Python or pickle loading. Record
upstream/HF immutable revisions and hashes in the produced artifact.

Generate small fixtures for input vectors, each intermediate module boundary,
soft token outputs, framed token IDs, first logits, and deterministic continuations.
Include single-document, ragged batch, masked padding, duplicate source content,
permutation, long question, and empty-collection fallback cases. In evaluation
mode, changing document order should preserve outputs within floating point
tolerance. Duplicating content as a distinct document can change attention
weighting; arbitrary content deduplication is not a parity optimization.

Gate: every published module config loads strictly, safetensors round-trip
matches the PyTorch tensors exactly, and fixtures reproduce at pinned revisions.
Use precomputed upstream vectors initially. A small parity subset avoids a full
training-data download.

### Stage B Qualify the exact Qwen CUDA pair

Add the pinned 4B encoder bundle and dense Qwen3 generator CUDA session profile.
Verify tokenizer output, no-prefix question/document rendering, full-width pooled
vectors, long/ragged encoder batches, Qwen3 QK normalization and RoPE behavior,
full decoder logits under an explicit arithmetic/KV profile, paged KV
prefill/decode, and worker model admission.

Gate: the exact base models execute through production CUDA paths, with explicit
backend evidence and no silent host fallback. Set model-shape numerical limits
from independent reference comparisons. Confirm both models, module, working
buffers, and decoder KV can coexist within an admitted deployment budget.

### Stage C Native memory readout and soft input generation

Implement literal CPU and CUDA memory execution and the neutral soft input
preparation type. Integrate the released Qwen prompt and normal generation
continuation. Keep serving code separate from artifact conversion.

Gate: compare per-layer outputs and teacher-forced logits with the pinned
reference before relying on sampled text. F32 CPU fixtures should begin around
`1e-5` absolute/relative tolerance at module boundaries; establish BF16 limits
from observed reference variation, logits, and task scores rather than accepting
only high cosine similarity. Verify sequence limits count soft tokens, padding
never affects real rows, cancellation frees leases, and separate requests cannot
reuse each other's soft prompts or KV. Greedy text equality is a useful smoke
where logit margins are stable, not the sole numerical criterion.

### Stage C serving entry point: supplied stored features

The selected first production entry point is an inference API over supplied
stored features. The implementation adds a disabled-by-default named profile
and `POST /ai/v1/memorilla/generate`, through both standalone inference and the
authenticated database gateway. It composes the managed qualified encoder,
released reader and managed decoder, with greedy buffered output and fresh F32
KV. Requests cannot override model assets or enable sampling, streaming,
speculative decoding or prefix reuse. See the [serving guide](../design/inference/memorilla-serving.md).

The implementation keeps the encoder execution identity with full-width
normalized stored features, checks dimensions/finite values/norm/bounds, and
applies the pinned FP16-storage/BF16-readout conversion. The question is encoded
natively under the same recipe. Named profiles pin the source manifests,
managed publications and all required source artifacts. Source-generation
changes retire a loaded model instead of reusing stale qualification.

Admission includes simultaneous source mappings, F32 weights, cold conversion,
module construction, host buffers, actual CUDA workspace, cached request
allocations and fresh decoder KV. Reader and decoder receive independent
request budgets bounded by their globally held leases. Request-owned physical
allocations must retire before credit is refunded; the fixed model-owned CUDA
workspace stays charged. A node-wide lane queues compositions before model and
request leases, and existing model lanes protect their mutable native state.

Acceptance requires current-source managed end-to-end numerical evidence,
repeated A/B/A isolation, cancellation during prefill followed by recovery,
resource baselines after success/error/cancellation, and real HTTP/gateway
validation. The conditional CPU oracle must consume the actual native query
feature after the storage/readout conversion: tiny encoder F32 differences can
cross quantization boundaries. Preserve a separate full-CPU-input comparison;
do not conceal those feature differences by relaxing reader or decoder gates.

This release makes no database source-authority, freshness or complete-coverage
claim for caller-supplied vectors. Stage D remains a separate milestone with its
own producer provisioning, storage authorization and lifecycle acceptance gates.
Before rollout, evaluate workload answer quality and capacity on representative
collections. Numerical conformance alone does not establish useful answers.

### Stage D Database collection execution

Add the authorized unit iterator, recipe-pinned embedding artifacts, coverage
watermarks, model bridge, source-bound generation, and answer provenance. Start
with one local memory space and one ordinary table collection under explicit
limits. Add K/V projection caching only after raw-vector parity.

Gate: demonstrate append, edit, delete, TTL, forget, reconfiguration, restart,
and worker eviction/reload. Add native deterministic simulation cases for
leader transfer, stale derived publication, changed source revisions, and
concurrent forgetting during encode/readout/decode. Test principal isolation
and historical policy before the GPU sees vectors. Fail or report fallback when
coverage or budget is insufficient. Existing structured recall stays compatible.

### Stage E Train the Gemma baseline

Run G1/G2 diagnostic pilots, implement the Gemma PLE policy, and train G3 using
new EmbeddingGemma features. Begin with a small collection-disjoint overfit and
validation run, then reconstruction, mixture, and domain stages. Export with the
same manifest/conformance workflow as the Qwen module.

Gate: gradients reach memory slots/projector and leave all frozen base weights
unchanged; the module can overfit a small supervised set; validation improves
over the frozen closed-book Gemma baseline; exported native CUDA readout and
logits match the Python checkpoint. Compare the full model against RAG using
the same encoder, decoder, source split, generation budget, and metrics.

### Stage F Native training and optimized scans

Add a memory-SFT recipe to the existing fine-tuning runner with graph composition,
checkpoint/resume, recipe hashes, and validation. In parallel after Stage D,
qualify tiled online softmax and optional cached K/V. Distributed reduction,
quantization, approximate selection, multimodal training, and RL follow measured
need rather than blocking the first release.

Gate: one-step gradients and optimizer deltas match an independent reference;
multi-step learning agrees on loss and held-out task quality. CUDA reports must
show device optimizer steps and finite parameter changes, with zero undeclared
host gradients or fallback. Optimized scans match literal attention within the
declared precision profile and pass workload-level quality gates.

## Evaluation and release criteria

Maintain two suites. Compatibility tests compare the released Qwen artifact to
its exact reference. Product experiments compare the best deployable Antfly
choices under equal model and serving budgets. A successful Gemma model does
not establish Qwen checkpoint parity, and a parity pass does not establish
product usefulness.

| Dimension | Required comparisons and measurements |
| --- | --- |
| Quality | Closed book, full context where feasible, lexical/vector/graph RAG, exact latent, latent plus cited evidence |
| Generalization | Upstream task-trained results, unseen collections, unseen domains, future time slices; report them separately |
| Fidelity | Names, IDs, dates, numbers, corrections, abstention, citation support, unauthorized/forgotten fact disclosure |
| Collection scale | `N = 1, 16, 128, 512, 4096`; then 32768 and higher only under measured budgets; distractor and document-length sweeps |
| Serving | Cold load, warm cache, cache miss, append/update work, p50/p95/p99 TTFT, output tokens/sec, concurrency, overload and cancellation |
| Resource cost | GPU/host memory, source bytes, K/V bytes read, worker transfer, device kernels, fallback, cache build/write amplification |
| Lifecycle | Restarts, model eviction, schema/recipe upgrades, deletion, authority revocation, source reconfiguration, failover |

Time query embedding, source selection/read, transfers, readout, decoder prefill,
and decode separately and end to end. Account for encoding and K/V construction
as ingestion cost. Compare with Antfly's own paged-KV full context and RAG on
the same GPU; do not import the paper's latency multiplier. Use hardware with
room for the exact BF16 pair first, then qualify specific constrained deployment
profiles instead of assuming H100 results transfer to L4 or GB10.

Proposed product quality gate: predeclare primary workloads and an acceptable
noninferiority margin versus the same-decoder RAG baseline; require improvement
in the targeted multi-document/personalization cohort and measured serving cost.
Use bootstrap confidence intervals and retain subgroup failures. Temporal and
authorization fixtures must have zero policy violations. Approximate modes need
their own recall/answer-quality gate and must expose coverage.

Roll out behind an explicit model/space feature flag. Publish a bundle matrix
with model pair, recipe, precision, backend, maximum admitted collection, and
qualification hardware. Keep grounded retrieval available for unsupported
models, insufficient coverage, exact evidence needs, and latency budgets. Once
implemented, move stable runtime contracts into inference/model documentation
and memory policy into `zig/MEMORY.md`, following the [plan lifecycle](README.md).

## Open decisions to resolve with experiments

1. Which initial workload benefits most: user episodes, document collections,
   or current curated assertions? Use one clear source unit for the first launch.
2. Does G3 with 16 slots and full-width EmbeddingGemma preserve enough identifiers
   and multi-hop facts? If not, compare small grounded evidence and chunk size
   before increasing model complexity.
3. Does PAD-identity PLE learn reliably? Compare a context-only or learned PLE
   design only with matched training and serving semantics.
4. At what reuse/collection size does cached K/V repay its storage and ingestion
   cost? Measure raw projection versus cached scans before setting defaults.
5. What exact collection size meets the actual latency and memory budget? Choose
   limits from receipts, then evaluate approximate modes beyond that boundary.
6. Can a global module generalize across Antfly domains, or are separately routed
   domain modules necessary? Keep tenant data out of global training by default.

## Source and license notes

Primary references are linked beside their claims. The Memorilla code is
[Apache 2.0](https://github.com/snap-stanford/memorilla/blob/d8f18891a50bf33c18f04fe5167c22f51e58c9cd/LICENSE),
and the released memory weights list Apache 2.0. Base-model and dataset licenses
remain independent; Gemma-derived bundles and training datasets need their own
recorded terms. Public personalization data and generated samples do not imply
permission to train on a deployment's private memory.

Relevant living Antfly references are [native memory](../../zig/MEMORY.md),
[enrichments](../../zig/ENRICHMENTS.md),
[inference training](../../zig/pkg/inference/finetuning/FINETUNING.md),
[EmbeddingGemma 2](../guides/models/embeddinggemma2.mdx),
[Gemma4](../../zig/pkg/inference/models/gemma4/GEMMA4.md), and
[Qwen embedding qualification](../../zig/pkg/inference/scripts/qwen3_embedding/BASELINE.md).
