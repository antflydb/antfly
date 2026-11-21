# Memorilla copy17 native serving plan

Status: milestone 1 reached 2026-11-12 (offline parity tool reproduces the Python candidate); milestone 2 (managed serving) not started. Port of the encoder-LoRA research candidate
`tokens-copy17-gate3-1024` (schema `antfly.memorilla.encoder-lora-research.v1`)
to the Zig CUDA inference runtime. The released Memorilla path stays untouched;
copy17 arrives as a separate, separately qualified variant, following item 4 of
"Remaining integration work" in `memorilla-native-memory.md`.

## 1. What the candidate computes at inference

Reference: `zig/pkg/inference/scripts/memorilla/encoder_lora_eval.py` with
`--snap-memory all --copy-contiguous-runs --copy-complete-words`; helpers in
`encoder_lora.py`. Python runs the per-case loop under `inference_mode` and
`autocast(cuda, bf16)`; the copy head and the snapper run in FP32 with autocast
off. Weights in all three artifacts are stored FP32.

### 1.1 Artifacts (candidate directory)

| file | tensors | notes |
|---|---|---|
| `reader.safetensors` | 37 F32 | upstream `MemoryModule` state, packed `in_proj_weight [7680,2560]` / `in_proj_bias [7680]` per attention |
| `encoder-lora.safetensors` | 144 F32 | `layers.{0..35}.self_attn.{q_proj,v_proj}.{A,B}`; A `[8,2560]`, B `[4096,8]` (q) / `[1024,8]` (v); scale alpha/rank = 16/8 = 2.0 |
| `copy-head.safetensors` | 3 F32 | `query.weight [4096,4096]` (no bias), `gate.weight [1,4096]`, `gate.bias [1]` |
| `protocol.json`, `candidate.json`, `base-proofs.json` | | `memory_slots` 16, `document_features` "tokens-dual", `dual_layer` 24, `document_space_prefix` true, `document_self_context` true, `encoder_special_rows` true, `copy_head_temperature` 0.05, `reader_loops` 1, `entity_tags` "none" |

Reader geometry is the release geometry: d 2560, out 4096, 16 memories, 8 heads
(head_dim 320), 2 cross-attention blocks, LayerNorm eps 1e-5, erf GELU.

### 1.2 Encoder input (Qwen3-Embedding-4B tokenizer, BF16 encoder + LoRA)

1. Space prefix: each document that does not start with " " gets one leading
   space. The question gets no prefix and no instruction.
2. Self-context: for document D, `single = encode(D)` (no specials);
   `doubled = D + D` (joined directly, the second copy's leading space is the
   separator); require `encode(doubled)[-len(single):] == single`, else retry
   with `single = encode(" " + D)`, else error. `copy_ids = cat(single_i)`
   (length T = sum n_i) are the copy sources.
3. Batch `[D1+D1, D2+D2, ..., question]`, right padded. The tokenizer appends
   `<|endoftext|>` (151643) to every row (no BOS); pad id is also 151643 with
   mask 0. Padded length must be <= 128.
4. One causal forward with hidden states. `final` = output after the final
   RMSNorm; `layer` = `hidden_states[24]` (raw output of layer index 23, not
   normalized). Position ids `arange(L)`.
5. LoRA on every `q_proj` and `v_proj`: `y = base(x) + ((x A^T) B^T) * 2.0`,
   added before q_norm/k_norm and RoPE. Under autocast these run in BF16.
6. Row selection: for each document keep the last `n_i + 1` rows (second copy's
   tokens, then its EOS state); keep the question row whole. Apply to both
   `final` and `layer`.

### 1.3 tokens-dual features

- Question query `[1,2560]` FP32: last active row of `final` for the question
  (its EOS state), L2-normalized (eps 1e-12), then `feature_channel`:
  FP32 -> FP16 (RNE, subnormals kept) -> BF16 (RNE) -> FP32.
- Document rows per layer X in (final, layer24): concatenate every document's
  `n_i + 1` selected rows in order -> `[M, 2560]`, row-wise L2 normalize in
  FP32, `feature_channel`.
- Reader input `docs = cat([docs_final, docs_layer24], dim=1)` -> `[1, 2M, 2560]`
  FP32: all final-layer rows first, then all layer-24 rows, same order. Key
  padding mask all False. Question tokens are never document rows.

### 1.4 Reader forward (upstream MemoryModule, eval mode, BF16 autocast)

```
question = q[None]                              # [1,1,2560]
slots = memory_queries                          # [1,16,2560] FP32
conditioned = MHA_query_cond(Q=slots, K=V=question)   # single key => weight 1.0
                                                # == out_proj(W_v q + b_v) for every slot
slots = LN_query_cond_norm(slots + conditioned) # post-norm
docs = LN_doc_norm(docs)
for l in 0,1:
    slots = slots + MHA_l(Q=LN_cross_norm_l(slots), K=V=docs)
    slots = slots + Lin2_l(GELU_erf(Lin1_l(LN_ffn_norm_l(slots))))   # 2560->10240->2560
out = memory_projection(LN_layer_norm(slots))   # [1,16,4096]
```

MHA details (torch slow path): packed weights split q = rows 0:2560,
k = 2560:5120, v = 5120:7680 (same for bias); 8 heads of 320; scores
`(q * 320^-0.5) @ k^T`; softmax in FP32, probabilities rounded to BF16 before
`p @ v`; GEMMs in BF16 with BF16 outputs; LayerNorm in FP32; residual sums in
FP32; GELU on the BF16 tensor. Output memories `[16,4096]` are BF16.

### 1.5 Copy head (FP32, autocast off)

Sources `E = decoder.embed_tokens.weight[copy_ids].float()` `[T,4096]`: the
encoder-tokenizer ids of the space-prefixed documents (single copy), EOS rows
excluded. Encoder and decoder ids are assumed equal (asserted on one dev case).

```
m = memories.float()                            # [16,4096]
uq = normalize(m @ Wq^T); ut = normalize(E)
logits = (uq @ ut^T) / 0.05                     # [16,T]
probs = softmax(logits); copied = probs @ E
g = sigmoid(m @ gate.w^T + gate.b)              # [16]
soft = g*copied + (1-g)*m
```

### 1.6 Decoding rules, in order

a. `copy_with_contiguous_runs(min_gate=0.5)`: `log_probs = log_softmax(logits)`;
   `copying[s] = g[s] >= 0.5`; runs = maximal consecutive copying slots
   `(begin,end)`. For each run of length L <= T, `scores[s] = sum_k
   log_probs[begin+k, s+k]` for s in 0..T-L (runs longer than T keep the soft
   output and yield no span). Process runs by descending max score (stable);
   within a run take positions in descending score order, choose the first
   finite start whose L positions are all unclaimed, else the first finite start
   (overlap allowed), else skip. Claim positions; set
   `output[begin+k] = g*E[chosen+k] + (1-g)*m`. Return output, spans sorted.
b. Snap all: `ids[s] = argmax_v cos(output[s], table[v])` over the full decoder
   input-embedding table `[151936,4096]` (untrained tail rows included), FP32
   matmul over normalized rows. No blank handling.
c. `complete_copied_words(pad_id = 220 (" "), overflow truncate)`, only when
   spans exist. `continues(id)`: decoded text does not start with " ", body =
   text.strip() non-empty, every char alnum (Unicode) or '-'. Walk spans in
   order: emit snapped ids before `begin`, then extend `[start, start+L)` back
   while `continues(src[first])` and forward while `continues(src[last])`, emit
   those SOURCE ids (overriding snapped ids), cursor = end; emit the rest; drop
   trailing pads while > 16, then truncate to 16, then pad with 220.
   Final memories = `embed.weight[ids]` BF16.

### 1.7 Decoder prompt and generation (Qwen3-8B)

`<|memory|>` is an added special token, id 151669. Messages: system =
`SYSTEM + "\nMemory" + "<|memory|>"*16 + "Memory"`, user = question.strip();
chat template with `add_generation_prompt`, `enable_thinking=False`:

```
<|im_start|>system\n{SYSTEM}\nMemory<|memory|>x16Memory<|im_end|>\n<|im_start|>user\n{q}<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n
```

SYSTEM = "Answer using the supplied memory. Return only the complete exact name
or code, preserving case, hyphens and leading zeros. If the requested fact is
missing, return UNKNOWN."

Injection is by embedding rows: `emb[pos] = bf16(e_mem + bf16(v - e_mem))`
where `e_mem` is the table row of 151669 (nonzero). This differs from the exact
table row in about 22% of elements (max abs 0.002); the existing Zig `compose`
already mirrors the same delta trick. Prefill BF16 in Python (F32 in Zig),
greedy argmax, stop on 151645 or 151643, max 24 new tokens. Answer =
decode(skip specials).strip(); exact match; "UNKNOWN" literal for missing facts.

Serving inputs are only the documents, the question and the frozen artifacts.
Priors and corpus data are evaluation-only.

## 2. What the Zig runtime has today

- Reader forward `src/pipelines/memorilla.zig forward()` is generic over row
  count (`documents [B*N, d]`, mask per row) and already matches the math above
  on split q/k/v weights, F32 strict, with trace hooks for the verifier.
  Question conditioning, two blocks, FFN, final projection all present.
- Everything above it is pooled-only: request `document_embeddings` 1..64 x
  2560 unit vectors, `memorilla_service.validateInputs`, `storedValue`
  FP16->BF16 channel, `encoder.encodeQuestion` (last-token pooled).
- Bundle loader `src/models/memorilla.zig`: schema `antfly.memorilla.bundle.v1`,
  exactly `17 + 16L` tensors, execution strings pinned (`encoder_pooling`
  "last-token-l2-normalized"), release pins to five checkpoints.
  `memorilla_runtime.Owner.open` refuses non-release bundles and requires
  `parameter_bytes == 776_495_104`, compute capability 12.1, strict F32.
- Decoder injection `src/pipelines/memorilla_generation.zig`: recipe with
  marker id 151669 (= vocab size), 16 rows, F32 `[16,4096]` MemoryRows,
  `compose` applies `original + (imported - original)`, paged F32 KV, greedy.
  Default system prompt differs from copy17's SYSTEM (recipe field).
- No per-token encoder state export, no intermediate-layer tap, no CUDA LoRA
  linear (native CPU `linearLoRA` only), no attention-probability op (the copy
  head needs probabilities, `crossAttention` returns weighted values only), no
  snapping, no span rules, no tokenizer-level word-boundary logic.
- Verifier `src/bench/memorilla_verify.zig` compares `6 + 8L` boundaries per
  case against `scripts/memorilla/reference.py` fixtures (pooled inputs).
- Config: `inference.memorilla.profiles[] {name, encoder, decoder,
  memory_module_path, max_documents, max_new_tokens}`
  (`zig/lib/platform/src/inference_settings.zig`). OpenAPI in
  `specs/openapi/inference/{config,api}.yaml`; `make generate` regenerates.
- Tests: add new files to the `test {}` block in `src/inference.zig` or a
  dedicated root like `src/test_memorilla_generation.zig` with a `build.zig`
  step; run `zig build test-memorilla-source -- --test-filter "<name>"` with
  `ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache"`.

### 2.1 Encoder specifics (from the CUDA path survey)

- Encoder profile load: `session_factory.zig:1945` with geometry pin (36 layers,
  hidden 2560, 32/8 heads, head_dim 128) and byte-exact lock on the two F32
  shards (`memorilla_v1_base_models.json`, `verifyArtifacts`); 398 resident F32
  weights required. Any modified shard fails closed, and adapter files next to
  the model are rejected (`optional_paths`).
- Run path `archRunImpl .gpt` -> `gpt_arch.hiddenForward` returns one host F32
  tensor `last_hidden_state [B,S,2560]` (after final RMSNorm); causal, no mask,
  right padding is harmless for active rows. Pooling happens afterwards on the
  host in `pipelines/embedding.zig pool3D` (`.last`) + `l2Normalize`.
- No intermediate-layer tap on this path. Minimal change: in
  `gpt.zig hiddenForwardFromEmbeddingsResidentWithOverrides` layer loop after
  `hidden = new_hidden;` (around :4331), copy `hidden` to host when
  `layer + 1 == tap_layer` (HF `hidden_states[k]` = output of 0-based layer
  k-1, before the final norm). Plumb through `hiddenForward` and
  `archRunImpl` as a second output tensor; `embedding.zig` reads only
  `outputs[0]`.
- LoRA: CUDA has no `linearLoRA`; the only load-time merge is Jina v5 specific
  (`gpu_hosted_store.zig JinaLoraAdapter.mergeIntoLoadedWeight`). Route: merge
  offline, `W_f32 + 2.0 * B @ A`, store F32, same tensor count and bytes.
- Diagnostic session without the v1 lock:
  `createQwen3EmbeddingCudaReferenceSession` (`session_factory.zig:1700`,
  strict F32, requires `model_manifest.json` without `memorilla_profile`);
  template `src/bench/memorilla_encoder_verify.zig`. Decoder equivalent:
  `createQwen3CudaReferenceSession` as used by `memorilla_qwen_verify.zig`.
- Tokenizer: `zig/lib/tokenizer` `HfTokenizer` loads `tokenizer.json`;
  `decodeToken(id, at_start=false)` returns " Depot"-style pieces (special ids
  decode to ""); encoder and decoder share vocab/merges; EOS 151643 is appended
  by the pipeline (`ensureTrailingEos`), not the tokenizer.

## 3. Port design

Two milestones. Milestone 1 proves the numerics end to end with an offline
verify tool against Python fixtures; milestone 2 wires the variant into the
managed service. Precision: Zig runs strict F32 everywhere (the qualified
decoder profile); Python ran BF16 autocast, so parity is measured at
boundaries with tolerances and, decisively, on snapped ids, completed ids and
answer text.

### 3.1 Offline export (Python, `scripts/memorilla/encoder_lora_export.py`)

1. `merge-encoder`: read the F32 managed encoder shards
   (`.benchmark-assets/memorilla/managed-f32/encoder`), add `2.0 * B @ A` to
   every `model.layers.N.self_attn.{q,v}_proj.weight` in FP32, write a complete
   HF directory (F32 shards, index, config, tokenizer files) plus a
   `merge.json` receipt (candidate sha, adapter sha, per-tensor max delta).
2. `export-reader`: `reader.safetensors` -> split q/k/v bundle via
   `exporter.split_projections`, non-release manifest, `execution.encoder_pooling`
   = `"per-token-dual-layer-l2-normalized-v1"` (new value; Zig accepts it only
   for non-release bundles), `config.json` = release geometry.
3. `copy-head` is used as-is (`copy-head.safetensors`, 3 F32 tensors).
4. `reference`: run the Python pipeline (the eval's own functions) on a case
   list and write `reference.json` + `reference.safetensors`:
   - json: `schema` `antfly.memorilla.encoder-lora-pipeline-reference.v1`,
     candidate/adapter/reader/copy-head sha256, `dual_layer`, `cases[]` with
     `name, documents[], question, expected_answer, copy_ids[], spans[[begin,end,start]],
     snapped_ids[16], completed_ids[16], prompt_ids[], output_ids[], answer`.
   - safetensors (F32 unless noted): `<case>.docs [2M,2560]`, `<case>.query [2560]`,
     `<case>.memories [16,4096]` (BF16 upcast), `<case>.copy_output [16,4096]`,
     `<case>.gate [16]`, `<case>.logits [16,T]`.

### 3.2 Zig pieces

- `gpt.zig` layer tap (see 2.1) exposed through the session run as a second
  output, `hidden_states_<k>`.
- `src/pipelines/memorilla_token_features.zig`: space prefix, self-context
  doubling with the fallback rule, EOS append, right padding, row selection
  (last `n_i+1` rows per document, whole question row), per-row L2 normalize,
  feature channel (reuse `memorilla_encoder.storedValue`), `docs [2M,2560]`
  + `query [2560]` + `copy_ids`. Pure host code over the two returned tensors.
- `src/pipelines/memorilla_copy.zig` (host F32): copy head forward, contiguous
  run selection, snap (device dot products via `linear` against the embedding
  table plus host table norms computed once), `complete_copied_words` with the
  tokenizer, all mirroring section 1.5 and 1.6 with unit tests ported from
  `test_encoder_lora.py`.
- Reader: existing `pipelines/memorilla.forward` on the exported bundle with
  `allow_synthetic`; `models/memorilla.zig validateExecution` accepts the new
  `encoder_pooling` value for non-release bundles only.
- Decoder: `memorilla_generation.prepareText` with `Recipe.system_prompt` set
  to copy17's SYSTEM, memory rows = embedding rows of `completed_ids` (F32, via
  `embeddingLookup`), existing `compose` delta trick, greedy, 24 new tokens.
- `src/bench/memorilla_lora_verify.zig` + `build-memorilla-lora-verify`: loads
  merged encoder (reference session), reader bundle, copy head, decoder
  (reference session), runs every fixture case, compares each boundary with
  tolerances and reports id/answer agreement as JSONL.

### 3.3 Milestone 2 (managed serving)

New `Profile` variant with its own base-model lock, catalog bundle, manifest
and `executionIdentity`; `inference.memorilla.profiles[]` gains the candidate
directory; latent requests carry `document_texts` (per-token path needs text,
not pooled vectors); OpenAPI + `make generate`; admission for per-token rows;
docs in `docs/design/inference/memorilla-serving.md`. Not started until
milestone 1 reproduces the Python answers.

## 4. Milestone 1 results (2026-11-12)

Tool: `zig build build-memorilla-lora-verify -Dcuda=true -Doptimize=ReleaseFast`,
binary `zig-out/bin/antfly-memorilla-lora-verify --candidate <copy17 dir>
--reader-bundle <lora-reader-bundle> --encoder <lora-merged-encoder> --decoder
<managed-f32/decoder> --reference <lora-ref-dev|lora-ref-fresh> [--output
summary.json] [--max-cases N] [--skip-decoder] [--reference-features]
[--reference-memories]`. Fixtures from `encoder_lora_export.py reference`.

| set | cases | copy ids = Python | spans = | snapped = | completed = | prompt = | answer = Python | Zig correct | Python correct |
|---|---|---|---|---|---|---|---|---|---|
| dev | 64 | 64 | 64 | 64 | 64 | 64 | 64 | 64 | 64 |
| fresh (8 groups, seed 20261110) | 80 | 80 | 79 | 79 | 79 | 80 | 79 | 78 | 79 |

Stage numerics (Zig strict F32 vs Python BF16 autocast): document rows
relative L2 <= 2.6%, min row cosine 0.999; reader memories relative L2 2-6%
typically, worst 17% (cosine 0.86) on one held-out-phrasing owner case that
still decoded identically; copy gate within 1e-4 except where a gate sits at
the 0.5 threshold.

The two fresh divergences:
- `ood-0002-code-a`: one gate crosses 0.5 in the other direction, so Zig's
  site run ends one slot earlier (" Depot" slot becomes a pad). Both answers
  are the correct code.
- `ood-0002-code-b`: all sixteen memory ids and the prompt are identical, yet
  the F32 decoder answers UNKNOWN where the BF16 decoder returned the code.
  The record is a cut-site record (" code Ly5-009275 at Mozeba" + pads), the
  class already known to sit on the decoder's acceptance boundary.

Mean per-case timings on GB10, strict F32, offline tool (no batching, host
readbacks included): encoder 620 ms (3 rows x ~140 tokens through the generic
CUDA Qwen3 path), feature assembly 0.7 ms, reader 19 ms, copy head 9 ms
(host), snap 26 ms (GPU dot products + 10 MB readback), word completion
<0.1 ms, decoder 1.3-1.8 s (prompt ~100 tokens, up to 24 new tokens, F32 KV).
The Python BF16 harness decoded the same answers in ~0.65 s; the qualified F32
profile is the cost of strict parity, and a BF16 serving profile would need
its own qualification.

Unit tests: `zig build test-memorilla-source -Dtest-filter='memorilla copy'
-Dtest-filter='memorilla token features' -Dtest-filter='gpt hidden-state tap'
-Dtest-filter='memorilla execution contract'` (models/memorilla.zig,
memorilla_copy.zig and memorilla_token_features.zig are imported from the
`src/inference.zig` test block).

## 5. Milestone 2 design: managed serving of the research candidate

Principles: the released path keeps every pin unchanged; the research path is
a second, explicitly configured profile kind that fails closed on its own
pins; nothing is promoted to "qualified". Interfaces below are fixed so the
pieces can be built in parallel.

### 5.1 Artifacts (produced by `encoder_lora_export.py`)

Merged encoder directory (`merge-encoder`, sidecars refreshable with
`--sidecars-only`):
- `model_manifest.json` = the catalog encoder manifest with
  `"memorilla_profile":"qwen3_encoder_lora_research_f32_v1"` (byte-exact
  canonical form is `memorilla_catalog.research_encoder_manifest`).
- `memorilla_lock.json`: `{"schema":"antfly.memorilla.research-encoder-lock.v1",
  "candidate_sha256":"<sha256 of candidate.json>", "base":{"model":"Qwen/Qwen3-Embedding-4B",
  "revision":"5cf2132a..."}, "files":[{"path","size_bytes","sha256"}...]}` listing
  every shard, `model.safetensors.index.json`, `config.json`, `tokenizer.json`,
  `tokenizer_config.json`, `vocab.json`, `merges.txt`, `merge.json`,
  `1_Pooling/config.json`, `modules.json`, `config_sentence_transformers.json`,
  `generation_config.json` (not `model_manifest.json`, not itself).
- `.antfly-download-complete.json` managed receipt (version 1) listing every
  regular file in the directory with size and sha256, including
  `model_manifest.json`, `memorilla_lock.json` and `merge.json`, so the
  directory is a managed publication (`publication_generation` = sha256 of
  the receipt).

Research reader bundle directory (`export-reader`): `manifest.json`
(`release:false`, `execution.encoder_pooling` per-token, `files` pins
`config.json`, `memory.safetensors`, `copy-head.safetensors`, `candidate.json`,
`protocol.json`; `source.candidate_sha256` = sha256 of `candidate.json`),
plus those five files. The Zig side reads `system_prompt`,
`copy_head_temperature` and `memory_slots` from `protocol.json`.

Cross-check at load: encoder lock `candidate_sha256` == bundle
`source.candidate_sha256` == sha256(`candidate.json` in the bundle).

### 5.2 Qualification profile (Zig)

- `memorilla_qualification.Profile` gains `qwen3_encoder_lora_research_f32_v1`
  (role encoder). `requireRoleRecipe`, `residencyPlan`, `role` handle it; the
  geometry pin, 398-weight / 16,087,097,344-byte residency checks and the
  `archRunAdmitted` gating apply to both encoder tags (role based).
- Lock source: v1 keeps `@embedFile`; the research tag reads
  `<dir>/memorilla_lock.json` (<= 1 MiB), validates the schema, and
  `verifyArtifacts` hashes its `files`, requires `model_manifest.json` ==
  `research_encoder_manifest`, and `artifactGeneration` fences the lock file,
  `merge.json` and the receipt. `sourceBytes`/host residency come from the
  lock. `optional_paths` stays forbidden.
- `executionIdentity(.qwen3_encoder_lora_research_f32_v1)` is fixed per tag
  (tag, canonical research manifest, arithmetic, sm_121, question recipe,
  marker `"research-lock-in-directory"`); the per-candidate pin is surfaced
  as `candidate_sha256` in the response, not folded into the identity, because
  clients never produce vectors for this path.
- `memorilla_encoder.Producer.profile` is the actual tag;
  `requireProducer(producer, publication, expected)`.

### 5.3 Research reader runtime (Zig, `src/models/memorilla_research_runtime.zig`)

Mirror of `memorilla_runtime.zig` for `ReaderCache`: `parameter_bytes` =
reader 776_495_104 + copy head 67_125_252 = 843_620_356 host-resident copy
head (device-resident reader weights only: backend_weight_bytes 776_495_104);
`weightPlan`, `requireLimits`, `requireAdmission`, `Owner{bundle, compute,
copy_head: memorilla_copy.CopyHead, candidate_sha256: [64]u8, protocol:
{system_prompt: []u8, copy_head_temperature: f32, memory_slots: usize},
execution_mutex, open(a, io, dir, admission, limits, control),
computeBackendWithBudget, releaseRequestResources, deinit}`. `open` requires
`release == false`, the per-token pooling, the five pinned files, the
candidate cross-check, SM 12.1, strict F32, resident bytes == reader bytes.
`memorilla_reader_cache.ReaderCache` becomes `pub` with an identity file list
taken from the Owner (`Owner.identity_files`); `pub const ResearchCache =
ReaderCache(research.Owner)`. `LoadedModel` (decoder) carries both caches.

### 5.4 Per-token encoder entry (Zig, `memorilla_encoder.zig`)

`encodeTokenFeatures(a, handle, documents: []const []const u8, question,
limits, control) !TokenFeatures` with `TokenFeatures{ features:
memorilla_token_features.Features, question_tokens: usize, producer:
Producer, lease, deinit }`. Same guards as `encodeImpl` (profile tag
research, publication, identity before/after, embedding asset lease, lane),
CPU lease sized for `(docs+1) * 128 * 2560 * 4 * 2` plus features, session
run through the admitted path with explicit amounts and the
`hidden_states_tap` input (layer 24), then `memorilla_token_features.prepare`
/ `features`. Documents <= 64, each <= 8192 bytes, batch rows <= 128 tokens.

### 5.5 Service (Zig, `src/server/memorilla_research_service.zig`)

`generate(a, io, manager, profile, paths, question, documents: []const []const u8,
identity, max_new_tokens, limits, request_limits, control, options)
!OwnedResult` where `OwnedResult` extends the latent one with
`candidate_sha256`. Flow = `memorilla_service.generateImpl` with: identity ==
research encoder identity; `prepareText` recipe `system_prompt` from the
bundle protocol; `encodeTokenFeatures`; `ResearchCache.acquire`; reader
forward with `num_documents = rows`, `max_document_rows = 16384`,
`backend_scratch = 128 MiB + rows * 2560 * 4 * 6`; copy head on the host
(sources = decoder embedding rows fetched on the decoder backend under its
execution scope, released before generation); snap via `linearNoBias`
against the resident embedding table on the decoder backend (16 x vocab
readback, table norms cached per loaded decoder); `completeCopiedWords` with
the decoder tokenizer (pad id = tokenizer encode of " " must equal 220);
memory rows = embedding rows of the completed ids uploaded to the reader
backend; `decode.generate` unchanged. Extra decoder-budget scratch reserved
for the copy/snap transients (32 MiB).

### 5.6 Settings, request, response (Zig + OpenAPI + docs)

- `MemorillaProfile.reader_kind: enum { released, encoder_lora_research } =
  .released` (JSON `"released"` / `"encoder_lora_research"`); for
  `encoder_lora_research` the `encoder` is the merged directory and
  `memory_module_path` the research bundle.
- Request: `document_embeddings` optional (required for `released`,
  rejected for research); research latent requires `document_texts`
  (1..64, same text limits as source_verified); `source_verified` with a
  research profile returns `UnsupportedMemorillaProfile` (409) for now.
  `encoder_execution_identity` must equal the research encoder identity.
- Response: `context_source` gains `supplied_documents`; new optional
  `candidate_sha256`; `memory_tokens` stays 16; `input_documents` = texts.
- Handler: item count = texts for research; `memorillaFailureResponse` maps
  `SyntheticMemorillaBundleNotAllowed`, `UnsupportedMemorillaProfile`,
  `MemorillaCandidateMismatch`, `MemorillaEncoderBoundExceeded` (413).
- `make generate` after `config.yaml` / `api.yaml`; docs in
  `docs/design/inference/memorilla-serving.md` ("research profile" section).

### 5.7 Verification

Unit tests per module; HTTP tests with `/missing` paths for the new
validation branches; offline `antfly-memorilla-lora-serve-verify` (template:
`memorilla_composed_verify.zig`) building a `ModelManager`, the research
profile and running the fixture cases through `memorilla_research_service`,
comparing answers with `lora-ref-{dev,fresh}` (target: same 143/144).

## 6. Milestone 2 results (2026-11-12)

The research profile serves end to end through the managed path
(`memorilla_research_service`, invoked by the HTTP handler for
`reader_kind: "encoder_lora_research"`), verified offline with
`antfly-memorilla-lora-serve-verify` over a real `ModelManager`:

| set | cases | answer = Python | Zig correct | Python correct | admission refunded |
|---|---|---|---|---|---|
| dev | 64 | 64 | 64 | 64 | 64 |
| fresh | 80 | 79 | 78 | 79 | 80 |

The single divergence is the same cut-site record as in milestone 1
(F32 decoder answers UNKNOWN; identical memory ids). Warm request latency
(median, F32 strict, GB10): 2.0 s total = 0.62 s per-token encoding + 1.35 s
reader, copy head, snapping and decoding. Cold loads: encoder 52 s, decoder
95 s, reader bundle 4 s. Operator cap: the shared unified admission limit is
75% of the process cap; two F32 models plus one request need about 73 GiB.

Shipped in this milestone: research profile tag with in-directory lock and
fixed execution identity; research bundle pins (copy head, candidate,
protocol) with candidate cross-check against the encoder lock; research
reader runtime and cache; per-token managed encoder entry; text-input
service; `reader_kind` setting; request/response contract (`document_texts`
in latent mode, `context_source: supplied_documents`, `candidate_sha256`);
HTTP handler branch and error mapping; OpenAPI + Zig/Python/Rust generated
code; serving docs section; Python sidecar export (`merge-encoder`
`--sidecars-only`, pinned bundle extras).

Open items: Go and TypeScript SDK regeneration (`make generate` needs `go`
and Node 24 on the build host); `source_verified` mode for research
profiles (texts are already present, the verifier can consume them); a BF16
serving profile to recover decoder latency would need its own qualification.

## 7. Wide challenge through the managed path (2026-11-12)

Fixtures: `encoder_lora_export.py fresh-cases --groups 16` for seeds
20261110, 7331, 4242 (160 cases each) and `reference` for each; the Python
reference reproduces the research evaluator's answers on all 480 cases.
Managed service via `antfly-memorilla-lora-serve-verify`; comparison with
`.benchmark-results/memorilla/encoder-lora-next/compare_wide.py`.

| source | correct | wrong | abstain |
|---|---|---|---|
| research evaluator (Python BF16) | 464 / 480 (96.7%) | 2 | 14 |
| Python fixture reference | 464 / 480 | 2 | 14 |
| managed Zig service (F32) | 462 / 480 (96.2%) | 2 | 16 |

477 of 480 answers are identical. The three differences, all on records whose
copy gates or decoder decision sit at a numerical threshold:
- seed 20261110 `ood-0002-code-b`: cut-site code record; F32 decoder answers
  UNKNOWN (Python: correct code).
- seed 20261110 `ood-0008-owner`: gates 0.587 / 0.479 next to the 0.5 run
  threshold; the F32 path resolves the runs differently and the decoder echoes
  the memory frame ("Memory key holder with the brass key for ... Memory"),
  a wrong answer where Python was correct.
- seed 7331 `ood-0005-owner-b`: F32 abstains where Python returned a wrong name.

Per-request latency over the 480 managed requests: median 1.97 s, p90 2.52 s
(including the three cold loads). Per seed: 151 / 153 / 158 versus research
153 / 153 / 158.

Stage-level confirmation (`antfly-memorilla-lora-verify` over seed 20261110,
160 cases): copy sources 160/160 identical, spans/snapped/completed ids
157/160, prompt ids 160/160, answers 158/160. In `ood-0008-owner` the gate of
the " Z" slot (0.479 in Python) crosses 0.5 in F32 (max gate difference 0.09),
so the name run [2,7) and the site run [8,11) merge into one nine-slot run
whose best diagonal starts inside the sentence ("with the brass key for ..."),
the name leaves the record, and the decoder echoes the frame. Mitigations, in
order of cost: an answer-format guard turning implausible decoder output into
an abstention; a decode rule that refuses to merge runs across a slot whose
gate lies within a margin of the threshold (needs its own evaluation); or
training with a gate margin loss.

### 7.1 Answer-format guard (2026-11-12)

`memorilla_research_service.guardAnswer` runs on the decoder text after
`decode.generate` in the managed research path. Trimmed text equal to `UNKNOWN`
is accepted. Otherwise the text is rejected when it is empty, longer than 64
bytes, more than six whitespace-separated words, contains a control character
or newline, contains the frame literal `Memory` as a standalone word or the
`<|memory|>` marker, or contains a byte outside letters, digits, space, `-`,
`.` and `'`. A rejection replaces the text with `UNKNOWN` and sets
`finish_reason` to `abstained`; the token ids are kept. The HTTP response
reports the outcome in `answer_guard` (`accepted` / `rejected`).

Rationale: the task's answers are short exact facts (a one-to-three-word name,
a `Xx0-00dddd` code, or `UNKNOWN`) and the frozen decoder has no format
constraint in direct-answer mode. The only garbled wrong answer observed in the
480-case wide challenge (`ood-0008-owner`, section 7) was a frame echo,
"Memory key holder with the brass key for Zehisi Depot  \nMemory", which the
guard converts into an abstention. The rules are general (no corpus-specific
regex) so a correct name or code is never rejected; the remaining wrong answers
in the table above are well-formed names or codes and are not affected. This
is serving policy for the research profile only, unit-tested in the service;
a GPU re-run of the wide challenge should record 462 correct / 1 wrong / 17
abstain for the managed path if nothing else changes.

### 7.2 Bridge-margin merge rule: evaluated and rejected (2026-11-12)

Rule (`memorilla_copy.contiguousRunsWithMargin`): a copying slot whose gate is
within `margin` above the 0.5 threshold and that lies between confident slots
of the same run stops copying, splitting the run. Evaluated with
`antfly-memorilla-lora-verify --copy-bridge-margins 0,0.05,0.1,0.15` on wide
seed 20261110 (160 cases, F32):

| margin | correct | wrong | answers = Python |
|---|---|---|---|
| 0 (current) | 151 | 1 | 158 |
| 0.05 | 151 | 2 | 157 |
| 0.1 | 151 | 3 | 156 |
| 0.15 | 151 | 4 | 155 |

No case became correct at any margin. The target case `ood-0008-owner` turns
from the frame echo into "Gemebo Bunokaoka Zehisi Depot" (still wrong), and
three other cases turn from abstentions into wrong answers of the shape
"<name><suffix> <site> Depot": splitting a run at an uncertain slot leaves the
site run adjacent to the name, and the frozen decoder reads the concatenation
as the answer. The sweep was stopped after the first seed; the rule stays
available behind the flag (margin 0 in the service) and is not adopted. The
answer-format guard (7.1) remains the mitigation for garbled output; the
remaining fence-sitting records are a training-side matter (gate margin loss
or more held-out-phrasing data).

### 7.3 Wide challenge, managed path with the answer guard (2026-11-12)

Rerun of section 7 with the guard of 7.1 active (`lora-serve-wide-<seed>-guard.jsonl`):

| source | correct | wrong | abstain |
|---|---|---|---|
| research evaluator (Python BF16) | 464 / 480 (96.7%) | 2 | 14 |
| managed Zig service, F32, guarded | 462 / 480 (96.2%) | 1 | 17 |

The guard fired exactly once in 480 requests, on the frame-echo answer of
`ood-0008-owner`, and rejected nothing else. 477 of 480 answers remain
identical to Python. Latency unchanged (median 1.97 s, p90 2.53 s).

## 8. Qwen hybrid memory: general soft recall plus exact records (started 2026-11-13)

Goal: one memory that restates passages like Memorilla and recalls names and
codes exactly like copy17. Observation: the released reader consumes pooled
document vectors (general, soft), copy17 consumes per-token states (exact,
snapped); pooled features cannot pinpoint codes (section 2 of the research
notes), so the hybrid composes two readers rather than widening one.

Design (no training for the first experiment):
- Rows 1-16: the released Memorilla reader (stage3) over pooled
  Qwen3-Embedding-4B vectors, injected as soft rows exactly as the released
  profile does.
- Rows 17-32: copy17's snapped record (16 real-token rows: contiguous runs,
  word completion), or only its copying spans with the remainder dropped.
- Decoder: stock Qwen3-8B, prompt = system text + "Memory" + 32 placeholder
  slots + "Memory", question in the user turn; system text to be chosen by
  measurement (released "helpful assistant" prompt vs copy17's exact-answer
  prompt vs a merged one).

Evaluation (Python first, `scripts/memorilla/qwen_hybrid_eval.py`): three
arms (soft only, record only, hybrid) on (a) the depot dev and fresh sets
(exact match), (b) squad_v2 / drop validation subsets (EM, F1), (c) an enwiki
restatement sample (ROUGE-L against the passage). If the hybrid keeps copy17's
exact accuracy and the released reader's free-form accuracy, the follow-ups
are: a short decoder-in-the-loop fine-tune of the soft slots on Stage 2 data
with the 32-slot frame if the extra rows perturb them, then the Zig service
change (mixed soft/token rows through the existing injection path).

### 7.4 Fallback trigger signals (2026-11-13, CPU analysis over the 480 wide cases)

Per-slot gates and snap cosines from the Python fixtures joined with the
guarded managed-service outcomes (462 correct, 1 wrong, 17 abstained):

| trigger | flagged | wrong caught | abstentions caught | correct flagged |
|---|---|---|---|---|
| min copy-slot snap cosine < 0.85 | 10 | 0/1 | 5/17 | 5/462 |
| min copy-slot snap cosine < 0.80 | 5 | 0/1 | 4/17 | 1/462 |
| min snap cosine (all slots) < 0.80 | 45 | 1/1 | 6/17 | 38/462 |
| gate within 0.1 of 0.5 | 12 | 0/1 | 5/17 | 7/462 |

Reading: the one wrong answer (`ood-0007-owner`, a fence-sitting owner record)
is only caught by the all-slot cosine at the price of flagging 8% of correct
answers; the fence-case abstentions are caught cheaply by the copy-slot
cosine (5 of 17 at one false positive per caught case); the remaining
abstentions are cut-site records whose signals are clean (gates saturated,
cosines 1.0) and whose only signal is the UNKNOWN itself. Practical routing:
UNKNOWN -> full-context fallback (catches all 17 abstentions after the fact),
copy-slot cosine < 0.85 -> fallback before decoding (catches the fence cases
early), optional all-slot cosine < 0.80 for a precision-first deployment that
accepts 8% extra slow-path traffic to remove the last wrong answer. Both
cosines are available in the service (snap scores) and should be surfaced in
the response alongside `answer_guard`.

### 8.1 First hybrid results (2026-11-13, no training; `qwen_hybrid_eval.py`)

| set | soft (released stage3) | record (copy17) | hybrid (16 soft + 16 record rows, merged prompt) |
|---|---|---|---|
| depot dev, exact (64) | 0 | 64 | 51 (62 case-insensitive) |
| depot fresh, exact (80) | 0 | 79 | 69 (75 case-insensitive) |
| squad_v2 val 200, EM / F1 | 0.230 / 0.377 | 0.050 / 0.091 (73% abstain) | 0.285 / 0.449 |

The composition is additive on free-form QA: the hybrid beats both parts on
squad because the record rows often hold the exact span under depot-style
labels (" key holder in Medieval Latin, 9th century") that the record arm's
prompt turns into UNKNOWN, while the soft rows and prompt keep the decoder
answering. On exact facts the record text is complete and correct in every
hybrid miss; the loss is surface form (case flips of code prefixes, a dropped
prefix or first name), attributed to the merged system prompt diluting
"preserving case" and to the soft rows beside the record. Abstention is intact
(16/16 UNKNOWN on both depot sets, zero false abstentions; the hybrid even
removed copy17's one false abstention). Pooled soft memory alone cannot carry
codes (0/144 on depot). Pending: drop and enwiki arms; then prompt (`exact`),
row order (record first) and `--record-rows spans` variants; then the short
soft-slot fine-tune in the 32-row frame.

### 8.2 Hybrid variants (2026-11-13, no training)

| variant | depot dev exact (ci) | depot fresh exact (ci) | squad EM / F1 | drop F1_any |
|---|---|---|---|---|
| soft only | 0 (0) | 0 (0) | 0.230 / 0.377 | 0.246 |
| record only (copy17) | 64 (64) | 79 (79) | 0.050 / 0.091 (73% abstain) | 0.009 |
| hybrid, merged prompt, soft first (base) | 51 (62) | 69 (75) | 0.285 / 0.449 | 0.238 |
| hybrid, exact prompt | 51 (62) | 66 (74) | 0.275 / 0.441 | 0.215 |
| hybrid, record gated (clean span, cosine >= 0.85) | 51 (62) | 68 (74) | 0.290 / 0.453 | 0.233 |
| hybrid, record rows first | 36 (40) | 52 (54) | 0.285 / 0.436 | 0.244 |
| hybrid, span-only record rows | 20 (31) | 25 (29) | 0.295 / 0.458 | 0.250 |

enwiki ROUGE-L F1 (50 passages): soft 0.068, record 0.061, hybrid 0.086; every
arm emits one sentence (recall about 5%).

Conclusions: the prompt wording is not the cause of the exact-recall loss
(the exact prompt reproduces the misses one for one); the soft rows' presence
is, and putting them after the record is worse; the record's label tokens are
needed for the decoder to read it (span-only collapses depot). Gating the
record on a clean copied span is neutral on depot and best on squad, but the
0.85 threshold fires on 92% of squad and 87% of drop cases. Passage
restatement is absent for the stage3 reader under stock Qwen3-8B in this frame;
the stage1 checkpoint is the test of whether it exists at all.

### 8.3 Answer canonicalization to the copied span (CPU re-scoring of the same outputs)

Every hybrid depot miss had the exact text in the record. Re-scoring the base
and gated hybrid outputs with a post-decode rule, "if the answer matches a
record substring case-insensitively, or is the tail of a record word
sequence, return the record's exact text", recovers the case flips with no
regressions: dev 51 -> 62/64, fresh 69 -> 75/80 (gated 68 -> 74), 0 correct
answers broken. The copy slots are the verbatim source and the decoder is
the selector, so letting the record spell the answer is the principled fix;
to be implemented as a decode rule in the evaluator and the service, with the
answer guard, and checked on squad/drop for side effects.

### 8.4 Production composition and the restatement check (2026-11-13 05:40Z)

Gated hybrid with answer canonicalization (soft rows first, full record rows,
merged prompt, record gate at cosine 0.85, canonicalize to the record span):

| set | copy17 alone | soft alone | gated + canonicalized hybrid |
|---|---|---|---|
| depot dev exact (64) | 64 | 0 | 63 (12 answers canonicalized) |
| depot fresh exact (80) | 79 | 0 | 78 (10 canonicalized) |
| squad EM / F1 (200) | 0.050 / 0.091 | 0.230 / 0.377 | 0.290 / 0.453 |
| drop F1_any (200) | 0.009 | 0.246 | 0.223 |

Exact recall is back to within one case per set of copy17 while free-form QA
gains +0.08 F1 over the released reader; drop (arithmetic) loses 0.02. This
is the recommended no-training composition.

Restatement: the released STAGE1 checkpoint restates passages (enwiki 50,
soft arm): ROUGE-L F1 0.237 / P 0.248 / R 0.234, 141 generated tokens on
average, paraphrased gist of the passage; stage3 gives 0.068 and one sentence.
The capability exists in the released readers and is lost through the QA
stages, which validates the Gemma Stage 1 objective and suggests testing the
hybrid with stage1 (or stage2) soft slots instead of stage3.

### 8.5 Soft half by released checkpoint (2026-11-13 08:20Z, gated + canonicalized hybrid)

| soft half | depot dev | depot fresh | squad EM / F1 | drop F1_any | enwiki hybrid ROUGE-L (tokens) | enwiki soft-only ROUGE-L (tokens) |
|---|---|---|---|---|---|---|
| stage1 | 45/64 (7/16 missing abstained) | 45/80 | 0.000 / 0.038 | 0.064 | 0.230 (140) | 0.237 (141) |
| stage2 | 55/64 (11/16) | 67/80 | 0.300 / 0.448 | 0.255 | 0.154 (67) | 0.166 |
| stage3 | 63/64 (16/16) | 78/80 | 0.290 / 0.453 | 0.223 | 0.086 (24, base hybrid) | 0.068 (18) |

The released readers are task-tuned: stage1 restates but makes the decoder
ignore the question (QA collapses, abstention fails), stage3 answers but does
not restate, stage2 sits between on every axis. Production hybrid keeps the
stage3 soft half (exact + QA); passage restatement is a separate mode served
by the stage1 reader when a request asks for it, and a single soft half that
does both needs training (the decoder-in-the-loop tune of the soft slots, or
the Gemma Stage 1 -> Stage 2 path with restatement kept in the mixture).

### 8.6 Source citations on the research serving path (2026-11-13)

The research response now returns, for every span the copy head copied into
the 16-slot record, the document it came from and its exact UTF-8 byte range
in the caller's text: `citations: [{document_index, start, end, text,
supports_answer, match}]` and `citations_unavailable` (serving doc, "Research
profile"). Mechanism:

- Source map (`memorilla_token_features.sourceMap`): each document's
  single-copy ids are decoded piece by piece (`tokenBytes`) and checked byte
  for byte against the encoded text (the caller's text after the added space,
  plus one more space on the `encode(" " + D)` fallback). A piece's cumulative
  range, shifted back by the added spaces and clamped at 0, is its range in
  the caller's text. If the pieces do not reproduce the text (NFC composing a
  decomposed accent, a special token), there is no map: citations are
  unavailable and the request still succeeds.
- Spans (`memorilla_copy.completeCopiedWords`): `Completed.spans` reports, per
  copy-head span, the source range emitted after word extension and its
  record slots, clipped by the overflow truncation.
- Citations (`memorilla_copy.buildCitations`): each emitted range is split
  per document, because a contiguous run over the concatenated sources can
  cross a document boundary. Regions of one document that touch or overlap
  are merged, widened to whole UTF-8 characters and trimmed of whitespace.
  `supports_answer` and `match` (`exact`, `case_insensitive`, `none`) compare
  the published answer after the guard; UNKNOWN never counts as support.

CPU replay over the Python reference receipts (real Qwen3 tokenizer, the
reference copy-head spans, snapped ids and answers; word completion
reproduces every reference record exactly; every citation equals its
document bytes):

| set | cases | mapped | citations | correct non-UNKNOWN answers with a supporting citation | supported but wrong |
|---|---|---|---|---|---|
| dev | 64 | 64 | 112 | 48/48 | 0 |
| fresh | 80 | 80 | 143 | 63/63 | 0 |
| wide 20261110 | 160 | 160 | 283 | 121/121 | 0 |
| wide 4242 | 160 | 160 | 288 | 126/126 | 0 |
| wide 7331 | 160 | 160 | 287 | 121/121 | 0 |

Example (dev-0000-code-a, answer `Kg9-009369`): `{document_index 0, start
95, end 105, text "Kg9-009369", supports_answer true, match exact}` from
record slots 1..10, and `{document_index 0, start 23, end 35, text "Pipofo
Depot", supports_answer false}` from slots 11..15.

Managed service (`antfly-memorilla-lora-serve-verify`, first 8 dev cases,
shared GPU): 8/8 answers correct and equal to the reference, 14 citations,
0 mismatches against `document_texts[i][start..end]`, 0 unavailable, 6/6
correct non-UNKNOWN answers with a supporting citation (the two UNKNOWN cases
cite only the site name), admission refunded on every case.

### 8.7 Hybrid serving in Zig: one encoder, run-time encoder LoRA (2026-11-13)

The production composition of 8.4 (stage3 soft half, record gate at 0.85,
merged prompt, soft rows first, canonicalization with the three-character
tail minimum) is served by a third profile kind, `reader_kind:
hybrid_research` (serving doc, "Hybrid research profile"). Design:

- One resident encoder: the stock managed `qwen3_encoder_f32_v1`. The pooled
  pass runs it unchanged; the per-token record pass applies copy17's encoder
  LoRA to the q/v projections at run time (`gpt.EncoderLora`, `y = W x +
  (alpha/rank) B (A x)` on the raw projection output before q/k norm and
  RoPE). The adapters travel as two run inputs of the tapped hidden-state run
  (`encoder_lora` F32 `[36, 2 * 8 * 2560 + (4096 + 1024) * 8]`,
  `encoder_lora_scale` F32 `[1]`), are uploaded for the run and freed with it,
  so the resident weights and their residency pins are untouched. Residency
  drops from about 67 GB (two F32 encoders) to about 51 GB.
- Artifacts: `memory_module_path` = the hybrid bundle (`export-reader
  --include-encoder-lora`): the research bundle plus `encoder-lora.safetensors`
  pinned in `manifest.json` and by `candidate.json` `encoder_lora_sha256`;
  `protocol.json` `rank`/`alpha`/`target_modules`. `released_memory_module_path`
  = the stage3 bundle. The merged research encoder is not used.
  `encoder_execution_identity` is the stock encoder's.
- Service (`src/server/memorilla_hybrid_service.zig`): adapter and prompt pins
  before any model load; a tokenizer preflight on the stock encoder (question
  tokens, pooled texts, the record batch rows); one admission covering the
  larger decoder plan (32 rows, merged prompt) or the smaller (16 rows,
  helpful prompt), copy/snap transients, the released reader over up to 64
  pooled rows and the research reader over the exact per-token rows; pooled
  pass; record pass with adapters (padded rows up to the evaluator's 1024
  tokens, at most 16384 positions); released reader (`Cache`) borrowed and
  retired first, then the research reader (`ResearchCache`), copy head, runs,
  snapping (with the top-1 cosines), word completion; the gate on the
  four-decimal cosines as the evaluator records them; on a closed gate the
  32-row reservation is swapped in place for the 16-row plan; generation;
  canonicalization (gate open), frame-echo guard, citations (gate open only).
- Deviations from `qwen_hybrid_eval.py`: F32 throughout (the evaluator is
  BF16, adapters in BF16); texts above the 256-token pooled limit are chunked
  sentence-greedily at 128 tokens (the evaluator pools any length; no
  verification text reaches the limit); a 16384-position cap on the record
  batch; ASCII-only case folding in canonicalization; the frame-echo guard
  (empty, control characters, `<|memory|>`, standalone `Memory`) is serving
  policy the evaluator lacks.
- Tests: native-backend equality of run-time LoRA and the merged q/v weights
  (two-layer Qwen3, two padded rows, relative error <= 1e-5, plain forward
  bit-identical), adapter file packing and pin checks, canonicalization on the
  evaluator's 19 cases, the gate on the evaluator's cases, guard, chunker,
  admission maximum, settings, request shape and HTTP validation branches.
- Verification: `antfly-memorilla-lora-hybrid-verify` reads the Python
  `gated-canon` rows (`hybrid-{depot-dev,depot-fresh,squad}.jsonl`) with case
  files for the documents (depot `lora-ref-{dev,fresh}.json`; squad exported
  to `.benchmark-results/memorilla/qwen-hybrid-serve/squad-cases.json` from
  the same 200 validation rows) and reports per set agreement with the Python
  answer and gate, exact (depot) or EM/F1 (squad), record-text agreement,
  citation checks and latency.

### 8.8 Paper-protocol results (2026-11-13 20:00Z, scored with the pinned upstream metrics.py)

Calibration (released stage3 reader on the paper's precomputed chunk embeddings,
paper prompts and limits): TriviaQA test 500 closed-book EM 36.60 (paper 36.00),
RAG-5 54.80 (55.20), soft memory 54.60 / F1 59.91 (57.00 / 60.60); NarrativeQA
1,000-question sample soft ROUGE-L 18.90 / F1 16.90 (19.31 / 16.56), RAG-5
13.88 / 11.83 (14.59 / 11.64). Calibrated within sampling noise except the
2.4-point TriviaQA EM gap (batch-1 and upstream-vLLM controls queued).

| arm | TriviaQA EM / F1 | NarrativeQA ROUGE-L / F1 |
|---|---|---|
| closed-book | 36.60 / 37.81 | - |
| RAG-1 | 52.00 / 52.51 | 11.68 / 10.31 |
| RAG-5 | 54.80 / 54.29 | 13.88 / 11.83 |
| soft (released stage3) | 54.60 / 59.91 | 18.90 / 16.90 |
| hybrid (soft + copy17 record) | 56.20 / 60.30 | 17.80 / 15.58 |
| soft, noise documents | 45.80 / 50.72 | - |
| hybrid, noise soft stream | 48.60 / 53.94 | - |

NarrativeQA ROUGE-L by question type (soft / hybrid / RAG-5): who 10.30 / 10.13 /
11.54 (n=253), which 20.19 / 11.69 / 14.75 (n=21), why 21.09 / 22.47 / 12.99,
how 19.78 / 20.05 / 11.79, what 22.98 / 21.09 / 16.74.

Reading: the hybrid helps short verbatim answers (TriviaQA +1.6 EM) but does
NOT close the paper's who/which gap on NarrativeQA and costs 1.1 ROUGE-L
overall: copy17's record is trained on depot records only, so on book chunks
it is off-topic noise and the cosine gate rarely closes. The copy component
must be trained on general QA spans (Stanford's Stage 2 mixture) for the
hybrid to generalize: the Gemma reader's copy slots are trained that way, and
the Qwen equivalent is the deferred single-reader training of copy slots on
Stage 2 data. Noise test: soft -8.8 EM, hybrid -7.6 EM (the readers rely on the
documents; paper FactKG -5.93).

Copy ceiling per benchmark (gold answer, SQuAD-normalized, contained verbatim in
the retrieved chunks of the same samples): TriviaQA 61.0% in the top-2 chunks,
74.8% top-5, 93.2% top-50. NarrativeQA overall 6-7% top-2; by question type in
the top-2 / top-50: who 15.4% / 45.1%, which 0% / 28.6%, what 7.0% / 23.5%,
why 0% / 0%. NarrativeQA references are annotator-written descriptions ("He is a
high school student in Phoenix."), so a copy mechanism has little to copy there
even when trained perfectly; TriviaQA, extractive QA and exact-fact tasks are the
benchmarks that can prove or refute the copy idea.

### 8.9 Resumed 2026-11-14: Zig hybrid verification and the eight-benchmark evaluation

The host rebooted on 2026-11-13 and every running job had stopped at about
20:17Z (paper chain2, copy-QA prep, Gemma Stage 1). Gemma training moves to a
GPU cluster; the Qwen work resumed here.

**Zig `hybrid_research` verification** (`qwen-hybrid-serve/run_verify.sh`,
one stock F32 encoder with run-time LoRA, F32 decoder, both readers) against
the Python gated + canonicalized rows: depot dev Zig 64/64 (Python 63/64, 63
identical answers, gates and record texts 64/64), depot fresh 78/80 = Python
(80/80 identical answers), SQuAD 200 EM .290 = Python, F1 .441 vs .453 (169/200
identical answers, gate agreement 190/200, record text 123/200: F32 vs BF16
encoder drift on long paragraphs). Citations 0 mismatches, admission refunded
on every request. Latency (p50 2.5 s depot, 7 s SQuAD) was measured while a
training job shared the GPU and overstates the idle figure.

**Eight registered Memorilla benchmarks** (`qwen_hybrid_eval.py paper-*`,
upstream prompts, budgets and metrics; PMv2/PV4 read their benchmark-specific
released checkpoints, the rest stage3; seeded 1,000-row samples, PubMedQA all
100; choice scoring re-implemented with transformers Qwen3-Embedding-4B and
checked against upstream `choice_accuracy`). Soft = released reader alone;
hybrid = soft + gated copy17 record (cosine gate 0.85) under the merged prompt;
hybrid-bench = the same rows under the benchmark prompt alone. Deltas are
paired bootstrap 95% intervals against soft.

| benchmark | metric | paper | soft | hybrid | hybrid-bench |
|---|---|---|---|---|---|
| PMv2 | acc | 44.60 | 44.3 | -3.5 [-6.4, -0.6] | -2.0 [-4.8, 0.7] |
| PV4 | acc | 66.65 | 66.7 | -1.5 [-4.1, 1.1] | -0.8 [-3.3, 1.8] |
| PubMedQA | acc | 59.00 | 59.0 | -7.0 [-15, 0] | -1.0 [-5, 3] |
| FactKG | acc | 84.33 | 84.5 | +0.7 [-0.8, 2.3] | +0.1 [-1.2, 1.5] |
| TriviaQA (500) | EM | 57.00 | 54.6 | +1.6 [-1.4, 4.8] | - |
| NarrativeQA (1,000) | ROUGE-L | 19.31 | 18.9 | -1.1 [-2.2, 0.0] | - |
| LaMP-4 | ROUGE-L | 18.15 | 15.97 | -0.2 [-0.8, 0.4] | 0.0 [-0.5, 0.6] |
| LaMP-7 | ROUGE-L | 48.37 | 46.58 | -0.5 [-1.2, 0.2] | -0.9 [-1.6, -0.1] |
| depot dev / fresh (ours) | exact | - | 0/64, 0/80 | 63/64, 78/80 | - |
| SQuAD 200 (ours) | EM | - | 23.0 | 29.0 | - |

The soft baseline reproduces the paper within sampling noise (LaMP is lower,
possibly a split difference: the paper's text calls them dev splits). copy17's
cosine gate opens on 88-92% of questions on every benchmark, so its record is
off-topic text there: the hybrid is neutral to slightly harmful on
personalization and abstractive tasks, mostly through the merged exact-answer
prompt (PubMedQA: 14% UNKNOWN), and the benchmark prompt alone removes most of
that harm. The gains stay where exact recall matters: depot facts (soft alone
recalls none), SQuAD and TriviaQA. On PMv2 the decoder sometimes repeats
secret-like strings from user histories (1 soft, 3 hybrid answers by a crude
regex; no open record contained one): collections of personal history need
secret scrubbing at index time whichever memory reads them.

### 8.10 Copy half for general QA: what failed and why (2026-11-14)

`qwen_copy_qa.py` continues copy17 decoder-free on 258k prepared examples
(Stage 2 QA configs + TriviaQA/NarrativeQA train; record `" answer" + exact
span` or 16 pads with gate 0; 15% depot replay). Two runs, reader lr 3e-5
(1e-4 dropped depot dev 64 -> 24 in 8 steps), 64 examples per step:

- `copyqa1-1024` (documents encoded without the question), stopped at 261:
  the relevance gate learns (held-out accuracy 0.50 -> 0.78, false opens 1.00
  -> 0.30, depot dev 64/64 kept) but the span does not follow the question:
  every Normandy question gets `" answer West Francia"`; SQuAD exact span
  3/200; pointer CE flat at ~2.4 from step 64.
- `copyqa2-qprefix-1024` (`--question-prefix question-passage-v1`: the
  question text precedes every self-context document in the encoder input;
  protocol key `document_question_prefix`, honoured by `record_memory`),
  stopped at 258: same plateau, SQuAD exact span 4/200.

Diagnostics:

- Linear start/end probe on copy17's per-token states (900 train / 300 test
  SQuAD + TriviaQA positives): exact span SQuAD 10.4% -> 32.5% and TriviaQA
  32.1% -> 50.4% with the question prefix (overlap 20 -> 63%, 48 -> 80%). The
  binding information is in the question-conditioned encoder states; the
  bottleneck is the reader's 16 latent slots feeding a copy head whose keys
  are context-free token embeddings.
- Oracle records in the hybrid (gold value given, canonicalized): bare
  `" answer <value>"` reads dev 48/64, fresh 57/80 (unusual names are replaced
  by real-sounding ones; missing facts are no longer abstained: 5/16, 3/16)
  vs copy17's site-bound record 63/64, 79/80. A record needs its binding
  context. An exact source window ending at the gold value reads dev 48/48 and
  fresh 59/64 non-missing facts at 32 tokens (47/48, 55/64 at 16); the misses
  are owner-first sentences whose site follows the name (a centred window
  would include it).

Proposed next design (awaiting decision): a span copy half -- a small
bidirectional span head (start/end/no-answer) over the question-prefixed
per-token states, trained decoder-free on the prepared examples plus depot
spans; the record is an exact ~32-token evidence window centred on the span
(no snapping, citation = window byte range); the gate is span minus no-answer
score; the benchmark prompt replaces the merged one.

### 8.11 Span copy half (2026-11-14, `qwen_span_copy.py`)

Built on the section 8.10 diagnostics. Features: copy17's encoder with its
frozen adapters over each top-2 chunk after the question text
(`question-passage-v1`), self-context doubled; per-token final + layer-24
rows, each L2-normalized (5,120 wide), aligned to the copy-source ids. Head:
projection to 1,024, document-segment and position embeddings, a learned
no-answer row, two pre-norm transformer layers, start/end logits (31M
parameters); marginal likelihood over every occurrence of the gold span;
no-answer is the target for negatives and depot `missing` facts. Record: the
exact source window around the best span (spans stay in one document, at most
30 tokens), 32 tokens with 65% of the free budget before the span (a depot
code's site precedes it; a held-out-phrasing owner's site follows it, so the
window is not end-aligned), trimmed to word boundaries and padded with the
space token. No snapping; the window is a byte range of the caller's text.

Oracle (gold-span windows, merged prompt, canonicalized): depot dev 48/48 and
fresh 64/64 present facts at 32 and at 48 tokens (copy17's own record 47/48,
63/64 on the same cases).

Pilot head (5,120 training examples, 2 epochs), held out: window contains the
answer SQuAD 78%, TriviaQA 87%, depot 100%; exact span 38% / 52% / 98%; gate
AUC 0.91, accuracy 0.83 (depot missing facts all closed). Decoder end to end:
SQuAD 200 EM .57 / F1 .69 (copy17 hybrid .29 / .45, soft .23 / .38); depot dev
63/64 with the window always in the prompt (48/48 present, 15/16 missing
answered UNKNOWN because the window reads "Its access code is not recorded"),
48/64 when a closed gate drops the window (the decoder then invents values).
Gate policy for the depot task: always include the window.

### 8.12 Span copy half in Zig (2026-11-14)

Design (no new setting: the bundle selects the record kind):

- Bundle: `encoder_lora_export.py export-reader --include-encoder-lora
  --include-span-head` on the span candidate pins `span-head.safetensors`
  next to the hybrid files; `protocol.json` carries `record_kind:
  span-window`, `document_question_prefix`, the head geometry, window width,
  left fraction, span length bound, gate threshold and optionally
  `span_gate_policy` (default `always`). `reader_kind: hybrid_research` is
  unchanged; `memorilla_hybrid_service.generate` reads the pinned protocol
  (sha256 and size checked against `manifest.json`) and takes the span path
  when `record_kind` is `span-window`, otherwise the copy17 path, untouched.
  `Options.span_gate_policy` overrides the policy (verifier `--span-gate`).
- Features (`memorilla_token_features.prepareSpanBounded`,
  `memorilla_encoder.encodeSpanTokenFeatures`): the copy17 batch (space
  prefix, self-context doubling, source map) with each doubled document
  encoded after `"Question: {q}\nPassage:"` (Python whitespace collapsed); the
  kept tail ids are checked unchanged; one tapped run of the stock encoder with
  the run-time LoRA; per document the second copy's own-token rows (EOS rows
  dropped), final and layer-24 each L2-normalized, concatenated (5,120) and
  rounded through FP16 (`memorilla_span.spanFeatures`).
- Head (`src/pipelines/memorilla_span.zig`): host F32, weights from the pinned
  file, GEMMs on the native SGEMM (`backends/native.zig`, Io-parallel):
  projection + LayerNorm, segment and position embeddings (clamped to the
  tables), the no-answer row plus position 0, two pre-norm encoder layers
  (packed in-projection, 8 heads, exact-erf GELU), final LayerNorm, start/end
  logits. Loaded per request (130 MB, sha256-checked); load + forward is about
  0.8 s.
- Decoding: `bestSpan` (in-document, at most 30 tokens, F32 sums, first
  maximum in row-major order, earlier documents win ties), margin = span -
  no-answer - threshold, `evidenceWindow` (Python round-half-even budget,
  word trimming with `tokenContinuesWord` on decoder pieces), 32 window ids
  padded with the space token.
- Service: one admission covering either decoder plan (48 rows merged prompt
  or 16 rows helpful prompt) plus embedding-lookup transients, the released
  reader plus the composed rows it uploads, and the host head (weights,
  features, forward scratch). The released reader stays borrowed through
  generation and carries the composed rows (the decoder compute holds only
  resident weights when generation starts); the window rows come from the
  decoder's embedding table. Gate open: merged prompt, 16 soft + 32 window
  rows, canonicalization against the window text, citation = the window's
  byte range (`copy.buildCitations` over one span), guard as before. Closed
  (`learned` only): 16 soft rows under the helpful prompt, as the Python
  legacy path. Stage timings are logged as `memorilla_span_timing`.

Verification (`antfly-memorilla-lora-hybrid-verify` with `--span-gate`,
managed path, one stock F32 encoder + run-time LoRA, F32 decoder, stage3
released reader; against `qwen-span-copy/span1/legacy-{always,learned}` rows;
`.benchmark-results/memorilla/qwen-span-copy/span1-zig-verify[-learned].{jsonl,summary.json}`):

| set | policy | Zig | Python | answers equal | windows equal | spans equal | gates equal |
|---|---|---|---|---|---|---|---|
| depot dev (64) | always | 63/64 | 64/64 | 63 | 64 | 64 | 64 |
| depot fresh (80) | always | 80/80 | 80/80 | 80 | 80 | 78 | 80 |
| SQuAD (200) | always | EM .600 / F1 .717 | .595 / .714 | 188 | 194 | 193 | 200 |
| depot dev (64) | learned | 47/64 | 48/64 | 57 | 48/48 open | 64 | 64 |
| depot fresh (80) | learned | 64/80 | 64/80 | 78 | 64/64 open | 78 | 80 |
| SQuAD (200) | learned | EM .600 / F1 .717 | .595 / .714 | 189 | 192/198 open | 193 | 200 |

No errors, citation mismatches 0 (every citation reproduces its document
bytes), admission refunded on every request, all supported correct depot
answers cited (47/47 and 64/64). Differences: the one depot miss
(`newdev-0001-owner`) has the identical window; the F32 decoder answers
`JESAVO DEGEBOR` where the BF16 Python decoder gave `Jesavo Degebo` (the same
case the copy17 hybrid tripped on). The two depot span mismatches are missing
facts with margins near -21 to -25 whose best spans tie differently (F32 Zig
head vs BF16 autocast Python head); their windows and answers agree. SQuAD:
7 span and 6 window differences on borderline scores; 12 answer differences,
6 of them with identical windows (generation numerics). Under `learned`, a
closed gate leaves the soft rows alone and the decoder invents values for
missing facts in both implementations (0/32), so `always` stays the depot
policy.

Latency, back to back with the copy17 bundle on the same binary (40 cases per
set, a 0.9 GB co-tenant on the GPU): depot dev p50/p90 6.4/7.6 s (copy17
5.4/6.4 s), SQuAD 8.5/15.9 s (copy17 13.1/21.4 s). Span stage medians: pooled
encode 0.78 s, span record pass 2.66 s, head load + forward 0.77 s,
generation 1.72 s. Under the concurrent Python 8-benchmark job the full runs
measured depot p50 6.9 s and SQuAD 12.2 s. Unit tests: span module (window,
rounding, best span, prefix, protocol, tiny head forward vs PyTorch reference
at 2e-4), span batch, pinned-protocol detection; the Memorilla filter passes
146/148 (2 real-tokenizer tests skipped without the model directory env).

### 8.13 Span hybrid on the eight Memorilla benchmarks (2026-11-14, `span1-35k`)

Head trained on the 35k-example cache (3 epochs): held out, the window
contains the answer for SQuAD 83%, TriviaQA 92%, DROP 67%, depot 100%; gate
AUC 0.93. Legacy sets, window always in the prompt (merged prompt): depot dev
64/64 and fresh 80/80 (every missing fact answered UNKNOWN, 0 false
abstentions), SQuAD 200 EM .595 / F1 .714, DROP 200 EM-any .43 / F1-any .46
(copy17 hybrid 63/64, 78/80, .29/.45, .195/.223). A learned gate on depot
reads every present fact (48/48, 64/64) but drops the window for missing
facts, and the decoder then invents values (0/32): depot-style exact lookup
keeps the window always.

Eight benchmarks (same samples and scorer as 8.9; `remeasure.sh`). Three arms
per benchmark from one record cache: window always + merged prompt, window
always + benchmark prompt, learned gate + benchmark prompt. The merged
exact-answer prompt is harmful off-task (PubMedQA -17, 30% UNKNOWN; PMv2
-8.5) and the always-on window hurts stylistic and persona tasks (LaMP-7
-1.2, PMv2 -6.2) while helping extractive ones; the exported gate (margin 0)
is too shy for NarrativeQA (opens on 26/1000).

Gate threshold: a closed gate reproduces the soft prompt exactly, so any
threshold can be evaluated offline (`qwen_span_copy.py sweep`: window rows
where the record margin >= t, soft rows elsewhere). Chosen on 300 disjoint
validation rows per benchmark (`val/`, NarrativeQA, FactKG, LaMP-4/7,
PV4, PMv2; TriviaQA and PubMedQA have no spare test rows) by the rule "largest
mean gain with no per-benchmark loss": t = -3 (validation mean +0.98, worst
+0.10; always-on +1.25 but worst -0.89). Reported on the original samples:

| benchmark | metric | paper Memorilla | soft | copy17 hybrid | span hybrid (t = -3) | delta vs soft [95% CI] | window |
|---|---|---|---|---|---|---|---|
| TriviaQA | EM | 57.00 | 54.60 | 56.20 | **66.20** | +11.60 [+7.80, +15.40] | 390/500 |
| NarrativeQA | ROUGE-L | 19.31 | 18.90 | 17.80 | **20.47** | +1.60 [+0.76, +2.49] | 129/1000 |
| FactKG | acc | 84.33 | 84.50 | 85.20 | 86.00 | +1.50 [0.00, +3.00] | 705/1000 |
| LaMP-4 | ROUGE-L | 18.15 | 15.97 | 15.79 | 16.77 | +0.79 [-0.03, +1.64] | 743/1000 |
| PubMedQA | acc | 59.00 | 59.00 | 52.00 | 59.00 | 0.00 | 1/100 |
| PMv2 | acc | 44.60 | 44.30 | 40.80 | 44.40 | +0.10 [-0.40, +0.60] | 22/1000 |
| LaMP-7 | ROUGE-L | 48.37 | 46.58 | 46.07 | 46.52 | -0.06 [-0.30, +0.16] | 89/1000 |
| PV4 | acc | 66.65 | 66.70 | 65.20 | 66.00 | -0.70 [-1.90, +0.50] | 191/1000 |

No benchmark loses significantly; TriviaQA and NarrativeQA gain significantly
(TriviaQA EM 66.2 is above every method in the paper's Table 11, best 57.0;
the NarrativeQA "who" bucket rises from 10.3 to 16.6 with the window always on,
above the paper's RAG-5 11.5). Caveats: 1,000-row seeded samples (TriviaQA
500, PubMedQA 100), not full splits; our soft baseline is 2.4 EM under the
paper on TriviaQA and lower on LaMP (split labelling), so read deltas, not
absolute ranks; batched greedy generation is not bit-stable across batch
compositions (157/972 PV4 outputs differ between two soft-prompt runs; the
offline sweep reuses the soft rows, so closed gates add no such noise).

Serving policy that follows: exact-lookup requests (answer or UNKNOWN):
window always, merged prompt; general requests: benchmark/user prompt, window
when span minus no-answer clears the exported threshold minus 3 (raw margin
>= -6.83 for `span1-35k`). The Zig service (8.12) implements `always` and
`learned` at the exported threshold; the -3 offset and a per-request policy
are the follow-up.

### 8.14 Robustness: extractor ablation, leave-one-out heads, seeds, full splits (2026-11-14, `robust/`)

Three checks on the 8.13 result before calling it a contribution:

- Extractor ablation. Does the head matter, or would any raw-text window
  of the same top-2 chunks do? `embed-window`: the word-aligned 32-token
  window (stride 8) whose pooled frozen-encoder vector (adapters off, the
  stored chunk vectors' pooling) is most cosine-similar to the stored
  question vector; precomputable at ingest, no training. `lead`: the first
  32 tokens of the top-1 chunk (floor). Gated three ways: always; by s31's
  span gate at -3 (same open rows, different window); by its own cosine,
  threshold picked on the 8.13 validation rows by the same rule as the span
  gate (largest mean gain, no per-benchmark loss).
- Leave-one-out heads. TriviaQA (8,000) and NarrativeQA (3,000) train rows
  were in the head's 35k cache, and they carry the two significant gains.
  `loo-triviaqa` / `loo-narrativeqa` drop that source from the training and
  held-out caches (seed 31, so neither the head nor its exported threshold
  sees it) and are scored on the dropped benchmark.
- Head seeds and full splits. Seeds 32 and 33 beside s31 (same cache,
  `train --seed`), gate rule fixed in advance (exported threshold - 3, no
  re-tuning). Full test splits minus the 300 validation rows per benchmark
  (TriviaQA 500 and PubMedQA 100 were already full): NarrativeQA 10,257,
  FactKG 8,741, PV4 3,937, PMv2 4,700, LaMP-4 1,625, LaMP-7 1,200.
  `seeds` reports each seed's paired CI and the delta of the seed-mean
  per-row score.

Generation batch 64 (16 in 8.13; rows generated earlier are reused).
Scripts: `qwen-span-copy/robust/{main,train,prep,score}.sh`, `assemble.py`
(policy arms from the soft and window-always rows; an errored record counts
as a closed gate).

Results (full splits for soft and seeds 31-33; ablation on the 1000-row
samples). Thresholds chosen on the validation rows by the 8.13 rule: s31 -3,
s32 -2, s33 -4 (the fixed offset -3 does not transfer: s32 opened on 170/1000
PMv2 sample rows vs 22 for s31, PMv2 -1.5 [-2.9, -0.1]).

| benchmark | rows | paper | soft | seed mean (val gates) [95% CI] | s31 / s32 / s33 | always (seed mean) |
|---|---|---|---|---|---|---|
| TriviaQA EM | 500 | 57.00 | 54.60 | **+11.53** [+7.80, +15.33] | +11.6 / +11.8 / +11.2 | +10.80 |
| NarrativeQA R-L | 10,257 | 19.31 | 19.15 | **+1.80** [+1.54, +2.07] | +1.66 / +1.66 / +2.09 | +3.23 |
| FactKG acc | 8,741 | 84.33 | 83.73 | **+1.84** [+1.33, +2.34] | +1.86 / +1.77 / +1.88 | +2.22 |
| LaMP-4 R-L | 1,625 | 18.15 | 16.31 | +0.39 [-0.25, +1.06] | +0.34 / +0.19 / +0.65 | +0.40 |
| LaMP-7 R-L | 1,200 | 48.37 | 46.51 | +0.00 [-0.16, +0.16] | -0.10 / -0.09 / +0.20 | -1.18 (sig) |
| PubMedQA acc | 100 | 59.00 | 59.00 | 0.00 (gate closed) | 0 / 0 / 0 | +0.67 |
| PV4 acc | 3,937 | 66.65 | 66.93 | -0.25 [-0.97, +0.48] | -0.05 / -0.23 / -0.46 | -0.37 |
| PMv2 acc | 4,700 | 44.60 | 44.47 | -0.24 [-0.55, +0.06] | 0.00 / -0.49 (sig) / -0.23 | -6.43 (sig) |

LaMP-4's sample result (+0.82 [0.03, 1.63]) shrinks to +0.39 n.s. on the
full split; PV4's sample lean (-0.83) to -0.25 n.s.
PMv2: the gate removes almost all of the always-on loss (-6.43 -> -0.24
seed mean, n.s.), but seed 32 alone loses 0.49 [-0.96, -0.02], and the
fixed offset -3 leaves a significant seed-mean loss (-0.38 [-0.70, -0.07]).

Leave-one-out (seed 31, exported threshold - 3): no-TriviaQA head on
TriviaQA +12.6 [+9.2, +16.0] (main head +11.6); no-NarrativeQA head on full
NarrativeQA +2.91 [+2.49, +3.33] (main head +1.66), window opened 6,201 vs
1,288 rows: training on NarrativeQA's abstractive answers makes the gate shy
on NarrativeQA (always-on windows score the same, +3.2). NarrativeQA "who"
R-L: soft 12.8, main head 15.1, no-NarrativeQA head 17.8.

Extractor ablation (samples, window always on, change vs soft): TriviaQA
lead +2.6 / embed-window +7.4 / span head +10.4; NarrativeQA +1.69 / +2.09 /
+3.09; FactKG +2.9 / +3.1 / +1.8; persona tasks negative for all three. Span
head minus embed-window on identical gate decisions (s31 gate -3): TriviaQA
+3.8 [+1.0, +6.8], NarrativeQA +0.71 [+0.14, +1.33], FactKG -0.9 [-1.7,
-0.1], others n.s. The embed-window's own cosine gate (0.5, chosen on the
validation rows) opens on 721/1000 PMv2 rows: PMv2 -4.4 [-7.2, -1.7], PV4
-2.5 [-5.0, +0.1]; behind s31's span gate it is safe (PMv2 +0.1) and better
on FactKG (+2.4 vs +1.5). 300 validation rows are too noisy for the
choice-scored persona tasks (embed always-on PMv2 -1.7 +/- 5 on validation,
-5.3 on the test sample).

Conclusions: the gains replicate across head seeds and full splits and do
not come from in-domain training; the hybrid layout carries much of the
gain, the span head adds a significant step on extractive QA, and its
no-answer gate (not a similarity threshold) is what protects persona tasks.
Serving: choose each head's gate offset on validation rows; a larger
persona/no-answer calibration set and a gate trained without abstractive
sources are the main headroom. Full-split embed-window runs were skipped
(user, 2026-11-15). Write-up: Claude Docs "Memorilla span hybrid:
robustness study".

### 8.15 Gate study: what a better gate can recover (2026-11-15, `robust/gate/`)

Question: can a better-calibrated gate remove the persona cost of 8.14 and
keep more of the always-on NarrativeQA gain (+3.23 vs +1.80 gated)? A gate
is a row pick between the soft prompt and the head's window-always run, so
every gate here is scored offline from the 8.14 full-split runs (31,060
rows x seeds 31-33), no new generation. Thresholds always come from
calibration rows by the 8.13 rule (largest mean gain, no per-benchmark loss);
results are seed means with paired bootstrap CIs on the full test rows.

Calibration settings: `xfit` 5-fold cross-fit over the full rows (folds
grouped by memory collection, stratified by benchmark; a large calibration
set); `val` the same 300-row validation sets the shipped gate used (six
benchmarks; TriviaQA and PubMedQA have none); `lobo` fit on seven
benchmarks, score the held-out one (an unseen task type).

Diagnosis: within a benchmark the span margin barely ranks rows by window
benefit. AUC (helps vs hurts): TriviaQA 0.74, NarrativeQA 0.57, FactKG
0.57, LaMP / PV4 / PMv2 0.50-0.51. On the choice-scored persona tasks the
per-row deltas are mostly answer flips (PMv2: 10% of rows gain, 16% lose).
The margin gate protects them only because their margins are lower on
average, so one global offset trades NarrativeQA against PMv2. With 25k
calibration rows instead of 300 the no-loss rule closes almost everything
(NarrativeQA +0.42, TriviaQA +9.40). Per-row learned gates (cost-sensitive
logistic regression on 13 record and retrieval scalars, or on those plus the
stored question vector; k-nearest-neighbour benefit on question vectors)
do not beat the shipped gate: no feature predicts the flips, and any
spread in a per-row score on PMv2 forces the threshold above NarrativeQA.

The headroom is a per-task decision. Routed gate: each calibration
benchmark gets its own margin offset, chosen as the largest one-sided 95%
lower bound of its gain (standard error clustered by collection; the profile
stays closed unless that bound is above zero). A question goes to the
benchmark whose centroid (mean stored question vector) is nearest (routing
accuracy 91-100% under cross-fit). No task id at serving; the question
vector is the one the soft reader already uses.

| gate | TQA | NQA | PubMed | FactKG | LaMP-7 | LaMP-4 | PV4 | PMv2 |
|---|---|---|---|---|---|---|---|---|
| always | **+10.80** | **+3.23** | +0.67 | **+2.22** | **-1.18** | +0.40 | -0.37 | **-6.43** |
| shipped margin gate (8.14) | **+11.53** | **+1.80** | 0 | **+1.84** | 0.00 | +0.39 | -0.25 | -0.24 |
| routed, `val` | **+10.53** | **+2.97** | 0.00 | **+2.22** | 0.00 | 0.00 | -0.02 | -0.16 |
| routed, `xfit` | **+11.60** | **+2.88** | +0.67 | **+2.17** | +0.03 | -0.14 | -0.03 | -0.01 |
| per-task offset (task id), `xfit` | **+11.60** | **+3.19** | +0.67 | **+2.17** | +0.03 | -0.14 | -0.01 | 0.00 |
| routed, `lobo` | **+11.00** | **+0.96** | -3.33 | **+1.55** | -0.10 | +0.17 | -0.40 | -0.07 |
| margin gate, `xfit` | **+9.40** | **+0.42** | 0 | **+0.87** | +0.01 | +0.07 | -0.02 | +0.04 |
| scalar regression, `xfit` | **+10.87** | **+1.64** | 0 | **+1.95** | -0.04 | +0.09 | -0.02 | +0.04 |

Bold: 95% CI excludes zero. Routed minus shipped, paired: `val`
NarrativeQA **+1.17** [+0.91, +1.45], FactKG **+0.38** [+0.10, +0.66],
TriviaQA **-1.00** [-2.00, -0.13] (no TriviaQA calibration rows, so it
routes to NarrativeQA's open-all profile and loses the margin's TriviaQA
ranking), LaMP-4 -0.39, PV4 +0.23, PMv2 +0.08 (n.s.). `xfit` NarrativeQA
**+1.08**, FactKG **+0.33**, LaMP-4 **-0.53** [-1.02, -0.05], TriviaQA
+0.07, PV4 +0.23, PMv2 +0.23 (n.s.). Persona costs go to ~0 in both
(PV4 -0.02 / -0.03, PMv2 -0.16 [-0.35, +0.02] / -0.01). The routed gate
reaches the task-id ceiling except on NarrativeQA questions that route to
TriviaQA (+2.88 vs +3.19).

Unseen task types are the limit: under `lobo` a held-out benchmark
inherits its nearest profile's policy (PubMedQA -> NarrativeQA/TriviaQA,
opens 55%; NarrativeQA -> TriviaQA, offset 0, +0.96). A novelty fallback
(questions below the 1st percentile of calibration rows' similarity to
their own centroid take the global margin gate) barely fired on held-out
benchmarks (0-2% of their rows; PMv2 17%, changing its result by 0.01):
question-vector similarity does not detect a new task type.

Caveats: the gate family was chosen after looking at cross-fit results on
these test rows, and the lower-bound rule was added after seeing PV4's
fold-to-fold offset swing (-7 to +2 with 30 users); the `val` rows fit,
but the design choice is not independent of the test rows. Confirm on
fresh rows before shipping.

Conclusions: calibration data alone does not fix the margin gate; the
window's value is a property of the task type, not of the row. A routed
gate gets +1.1-1.4 NarrativeQA and +0.3-0.4 FactKG over the shipped gate
with the persona costs removed, when every served task type has a
calibration profile. For an unseen task type it is worse than the shipped
gate, so serving needs a profile per task type (calibrated, or declared by
the application per collection) and the shipped margin gate as the
default. Scripts: `robust/gate/{features,study,knn,route,compare}.py`;
table `robust/gate/compare.md`.

### 8.16 Routed gate on fresh rows: pre-registration (2026-11-15, `robust/fresh/`)

Written before any fresh row is generated. 8.15 chose the routed gate after
looking at cross-fit results on the test rows; this checks it on rows that
neither the gate design, the gate fit nor the span heads have seen.

Fresh rows: the Memorilla-Data validation splits (the span heads trained on
train splits only, `qwen_copy_qa.QA_SPLIT`; every gate so far was fit on test
rows). TriviaQA 2,000 of 17,444 and FactKG 4,000 of 13,266 (seed 23);
NarrativeQA all 3,461, PV4 all 1,755, PMv2 all 2,059. LaMP-4, LaMP-7 and
PubMedQA have no validation split and are not part of the check.
`qwen_hybrid_eval.py paper-prep --split validation` (cases carry their split;
`paper-run` reads it). Arms as 8.14: soft, and window-always for heads s31,
s32, s33 (benchmark prompt, batch 64).

Frozen gate (`robust/gate/freeze.py` -> `robust/fresh/gate-frozen.{pt,json}`,
sha256 `463658e561828f5e717d05f1a3c13ab126107dcd172007694a851ec5ba02052c`):
routed LCB profiles fit on every row the gate study saw (300-row validation
sets plus full test rows, so TriviaQA and PubMedQA have profiles); a question
routes to the nearest benchmark centroid of the stored question vectors and
opens the window when its span margin clears that profile's offset.

| head | TQA | NQA | PubMed | FactKG | LaMP-7 | LaMP-4 | PV4 | PMv2 |
|---|---|---|---|---|---|---|---|---|
| s31 | 0 | open | closed | -6 | closed | closed | closed | closed |
| s32 | 0 | open | -9 | -7 | closed | closed | 2 | closed |
| s33 | -2 | open | closed | -7 | -3 | -4 | closed | -1 |

Primary comparison: frozen routed gate minus the shipped margin gate (s31 -3,
s32 -2, s33 -4), per fresh benchmark, on the per-row seed mean, paired
bootstrap 95% CI (10,000 resamples, seed 23). The routed gate becomes the
default for task types that have a profile only if all three hold:

1. NarrativeQA: routed minus shipped, CI lower bound above 0.
2. No fresh benchmark has routed minus shipped with CI upper bound below 0.
3. PV4 and PMv2: routed minus soft has CI upper bound at or above 0.

Otherwise the shipped margin gate stays. Reported but not decisive: routing
accuracy and open rates on fresh rows, the mean-rule variant, the task-id
ceiling, window always, per-seed results, and where LaMP-4/7 and PubMedQA
train-split questions route (question vectors only, no generation).

Caveats fixed in advance: the released stage-3 reader may have used the
validation splits for checkpoint selection (it affects the soft and window
arms alike); persona validation users may overlap test users.

### 8.16 Routed gate on fresh rows: result (2026-11-16, `robust/fresh/result.{json,md}`)

Run as pre-registered: frozen gate sha256 checked by `fresh_eval.py`, no
record errors, all 15 benchmark × head runs complete. One scoring bug, fixed
before evaluation: `paper-choice-embed` read the registered split's question
vectors, so its alignment check refused FactKG validation rows. It now reads
the split back from the case names (`records_split`). FactKG was re-scored
(probe cosine 0.9997); no generation was affected.

Routed minus shipped (per-row seed mean, points, 95% CI):

| | TQA | NQA | FactKG | PV4 | PMv2 |
|---|---|---|---|---|---|
| routed − shipped | −0.15 [−0.40, +0.10] | **+0.87** [+0.32, +1.41] | **+0.40** [+0.03, +0.77] | +0.08 [−0.97, +1.12] | **+0.73** [+0.23, +1.21] |
| routed − soft | **+8.82** | **+2.22** | **+2.05** | −0.00 [−0.13, +0.13] | +0.03 [−0.11, +0.19] |
| shipped − soft | **+8.97** | **+1.35** | **+1.65** | −0.08 [−1.14, +1.01] | **−0.70** [−1.23, −0.16] |
| always − soft | **+8.88** | **+2.36** | **+2.11** | −0.06 [−2.15, +2.07] | **−7.43** [−9.50, −5.34] |
| open: shipped / routed | 0.79 / 0.72 | 0.13 / 0.92 | 0.70 / 0.91 | 0.35 / 0.02 | 0.07 / 0.01 |

Decision rule: (1) NQA lower bound +0.32 > 0, holds; (2) no benchmark has a
significant loss to shipped (worst: TQA upper bound +0.10), holds; (3) PV4 and
PMv2 upper bounds vs soft +0.13 and +0.19, holds. **The routed gate becomes the
default for task types that have a profile.**

Not decisive, reported as planned:
- Routing on fresh rows: TQA 96.3% own profile, NQA 90.6% (8.6% to TQA),
  FactKG 99.9%, PV4 99.6%, PMv2 92.4% (6.4% to PV4). Misroutes land on
  profiles with similar policies, so routed is within 0.1 point of the
  task-id ceiling everywhere. LaMP-4/7 and PubMedQA train questions route
  to their own profile 99.8–99.9%.
- The gain comes from per-task offsets: NQA opens wide (0.13 → 0.92), FactKG
  more (0.70 → 0.91), persona closes (PV4 0.35 → 0.02). On fresh PMv2 rows
  the shipped gate is a significant loss to soft (−0.70); routed removes it.
- Mean-rule variant is close to LCB (NQA +0.91, PMv2 +0.79 vs shipped) but
  opens PV4 34%; the LCB rule stays primary.
- Per-seed routed vs soft: NQA 2.20/2.18/2.28, FactKG 2.08/2.08/2.00,
  PMv2 0.05/0.05/0.00 (s31/s32/s33).

Not covered: LaMP-4/7 and PubMedQA on fresh rows (routing only), and
questions unlike all eight benchmarks. Production needs a fallback for those
(e.g. the shipped margin gate when the nearest-centroid similarity is low),
fit and checked before the Zig port.

### 8.17 Task types without a profile: fallback study and pre-registration (2026-11-16, `robust/ood/`)

The routed gate (8.16) is validated only for the eight benchmarks it has
profiles for. Production questions can be of any type; this asks what the
routed gate does on a task type it has never seen, and whether a novelty
fallback (the shipped margin gate for unfamiliar questions) is needed.

Offline first (`robust/gate/novelty.py` -> `gate/novelty.{json,md}`): each
benchmark in turn loses its profile (lobo); familiarity = cosine to the
nearest centroid, to the 1 / 10 nearest calibration questions, or to 16 / 64
k-means prototypes per benchmark; a question falls back when below its
routed benchmark's q-quantile (own rows, grouped 5-fold cross-fit).

- Without a profile, the routed gate is never significantly below soft, but
  it inherits a neighbour's stricter offset and gives up some of the shipped
  gate's gain: vs shipped NQA **-0.84**, FactKG **-0.30**, PubMedQA -3.33
  (100 rows, n.s.), TQA -0.67 (n.s.).
- No familiarity score detects a held-out task type reliably. PubMedQA (the
  one large loss) is flagged at 2-20% by every method; per-benchmark
  thresholds catch more than 8.15's global floor did (LaMP-4 67-85%, PMv2
  32-91%), but NQA (19-46%) and FactKG (4-19%) stay mostly unflagged. Nearest
  neighbours misfire in profile: 41-76% of fresh FactKG rows fall below the
  test-split threshold (the test split has near-duplicate claims).
- Best fallbacks recover part of the loss (centroid at q 0.10: NQA -0.69,
  FactKG -0.22, PubMedQA -2.33 vs shipped; prototypes at 0.05: -0.68 / -0.21
  / -3.33); NQA stays significant under every method.

Unseen task types (all rows generated after this entry): eight Memorilla-Data
configs with no profile, validation splits, 600 rows each (seed 23):
SQuAD v2, DROP, CoQA, QuAIL, MS MARCO, PwC (QA or instruction over a
document; the span heads trained on the first five's train splits), SAMSum
and CNN/DailyMail (summaries; one fixed instruction per task, so every row
has the same question vector). Prompting and scoring follow evaluate.py's
custom-config defaults (helpful prompt, no suffix, 128 new tokens, per-row
ROUGE-L; `qwen_hybrid_eval.CUSTOM_TARGETS`). Arms as 8.16: soft, and
window-always for s31, s32, s33.

Frozen before generation: the 8.16 routed gate (sha256 `463658e5...`) and the
fallback (`gate/freeze_fallback.py` -> `ood/fallback-frozen.{pt,json}`, sha256
`03cc7016650579c8ae41b3468e1ec68014e5bc13a9e3c607c29715ee6c968f53`):
primary = cosine to the nearest centroid below that benchmark's 5% quantile
(TQA 0.437, NQA 0.459, PubMedQA 0.644, FactKG 0.522, LaMP-7 0.637, LaMP-4
0.591, PV4 0.520, PMv2 0.356); secondary = max cosine to 16 prototypes per
benchmark, same quantile. Already known when this was written: on the 8.16
fresh rows the centroid fallback fires on 3-8% and the 8.16 rule still holds
(NQA +0.91 [+0.38, +1.43] vs shipped).

Decision rule (`gate/ood_eval.py`; per unseen task type, routed minus shipped
on the per-row seed mean, paired bootstrap 95% CI):

1. No unseen task type has routed minus shipped with CI upper bound below 0:
   the routed gate is the default for all traffic, no fallback.
2. Otherwise, if no unseen task type has routed-with-fallback minus shipped
   with CI upper bound below 0 (and the fallback keeps the 8.16 rule on the
   fresh rows): routed with the centroid fallback is the default.
3. Otherwise the routed gate is used only when a request declares a profile;
   the shipped margin gate stays the default.

Reported, not decisive: gates vs soft, open and fallback rates, routing,
the prototype fallback, window always, per-seed results. Eight task types
with two comparisons each lean toward rule 3 by chance (no multiplicity
correction): a conservative bias, kept on purpose.

### 8.17 Task types without a profile: result (2026-11-16, `robust/ood/result.{json,md}`)

Run as pre-registered: both frozen sha256s checked by `ood_eval.py`, all
8 task types × 4 arms complete (600 rows each), no record errors.

Gate minus shipped on the unseen task types (per-row seed mean, points, 95% CI;
bold = CI excludes 0):

| | SQuAD v2 | DROP | CoQA | QuAIL | MS MARCO | PwC | SAMSum | CNN/DM |
|---|---|---|---|---|---|---|---|---|
| routed | **−5.50** | **−1.99** | −0.70 | **−6.21** | +0.14 | **−2.01** | **−1.47** | +0.29 |
| routed + centroid fb | **−0.35** [−0.71, −0.07] | **−0.22** [−0.47, −0.03] | −0.19 | **−6.35** [−8.68, −4.14] | +0.03 | −0.22 | **−1.47** [−1.91, −1.06] | +0.29 |
| routed + proto16 fb | **−0.22** | −0.15 | −0.24 | **−6.40** | −0.08 | +0.03 | **−1.47** | +0.29 |
| always | +0.00 | +0.05 | +0.16 | **−9.66** | **+4.86** | **+1.17** | **+6.31** | **−0.35** |
| shipped − soft | **+34.62** | **+11.44** | **+10.31** | **−3.60** | **+3.22** | **+8.29** | **+1.96** | **−0.56** |
| fallback rate (centroid) | 0.48 | 0.40 | 0.39 | 0.14 | 0.63 | 0.49 | 0.00 | 0.00 |
| open: shipped / routed-fb | 1.00 / 0.99 | 0.97 / 0.96 | 0.92 / 0.96 | 0.33 / 0.79 | 0.47 / 0.43 | 0.78 / 0.70 | 0.27 / 0.07 | 0.65 / 0.30 |

Decision rule: (1) routed has a significant loss to shipped on five task
types, fails; (2) routed with the centroid fallback still has four (SQuAD v2,
DROP, QuAIL, SAMSum), fails, although the fallback keeps the 8.16 rule on
fresh rows (NQA +0.91 [+0.38, +1.43], FactKG +0.41, PMv2 +0.68 vs shipped).
**Rule 3: the routed gate is used only when a request declares a profile; the
shipped margin gate (s31 offset −3) stays the default.**

Not decisive, reported as planned:
- The fallback does its job where the questions look unfamiliar: it fires on
  39-63% of SQuAD v2, DROP, CoQA, MS MARCO and PwC rows and turns −5.50 / −1.99
  / −2.01 into −0.35 / −0.22 / −0.22. The remaining SQuAD v2 and DROP losses
  are small but significant.
- The two large losses are confident misroutes that no familiarity score can
  catch. QuAIL (multiple-choice reading comprehension) routes 75% to NQA,
  above NQA's floor (median cosine 0.569 vs floor 0.459; only 14% fall back),
  and NQA's profile opens every row; but on QuAIL the window hurts (always
  −13.26 vs soft, shipped already −3.60). SAMSum has one instruction for every
  row, which routes to LaMP-7 at cosine 0.677 (floor 0.637); LaMP-7's profile
  closes, giving up the window's gain (always +8.27 vs soft, shipped +1.96,
  routed +0.49). CNN/DM routes the same way to LaMP-4 and comes out ahead,
  because the window hurts there.
- The question vector says what the question looks like, not what the window
  does for it: QuAIL looks like NQA and SAMSum looks like LaMP-7, but the
  window's effect is the opposite of the profile's in both. Per-task offsets
  need the task identity, which only the caller has.
- The shipped gate is not safe everywhere either: it loses to soft on QuAIL
  (−3.60), CNN/DM (−0.56) and fresh PMv2 (−0.70). Always-on beats shipped on
  MS MARCO (+4.86), PwC (+1.17) and SAMSum (+6.31). A caller that knows its
  task type gains from choosing the policy; the default cannot.
- Per seed (s31/s32/s33, vs soft): QuAIL shipped −2.23/−3.59/−4.97, routed
  −9.04/−10.40/−9.99; SAMSum shipped 1.66/1.64/2.57, routed 0.00/0.00/1.46,
  always 8.40/8.27/8.14.

Post-hoc correction (2026-11-16, after the result; the verdict is unchanged):
the QuAIL loss is a scoring artifact, not a reading loss. With the window in
the prompt the decoder answers with the option letter ("C", "A. ...") on
387/600 rows (soft 139/600), and per-row ROUGE-L against the option text
scores a bare letter as 0. Mapping a leading letter to its option and scoring
exact match: soft 336/600 (56.0%), window-always 392/600 (s31; s32 392, s33
394), seed-mean change **+9.44** [+5.89, +13.00] points (paired bootstrap,
10k, seed 23). So on QuAIL the window helps and the NQA route (opens 79-80%)
was the right policy; the "confident misroute" above holds for SAMSum only.
Rule 3 still follows from SQuAD v2, DROP and SAMSum. Choice-format tasks
need a letter-aware scorer, or a prompt that asks for the option text.
Persona losses are not this artifact: PMv2 answers are free text in both
arms, and the window there is a fragment of an unrelated past conversation.

Serving consequence: the bundle default is the learned margin gate at the
8.14 offset; the routes file ships alongside, and a request may name one of
the eight profiles to get its offset. The Zig `decideGate` already takes a
declared profile under any policy (8.18).

### 8.18 Gate policies in Zig: routes, declared profiles, offset (2026-11-16)

The 8.12 service had two gate policies, `always` and `learned` at the
exported threshold. It now serves everything 8.13-8.17 chose:

- Protocol (`protocol.json`, parsed by `memorilla_span.parseProtocol`):
  `span_gate_policy` is `always`, `learned` or `routed`; `span_gate_offset`
  (finite, default 0) is the learned gate's offset; `span_gate_routes`
  (`profiles`: 1-64 unique names of at most 64 bytes, `fallback`: bool) names
  the route table; `routed` requires it.
- Route table `span-gate-routes.safetensors`, pinned in `manifest.json`:
  `centroids` [P, 2560] (unit rows), `offsets` [P] (±inf allowed: always
  open / always closed), `floors` [P] (centroid fallback thresholds, -inf =
  never). `Routes.fromBytes` rejects anything else (`InvalidMemorillaGateRoutes`).
- `decideGate(policy, protocol, routes, question, margin, declared)`: a
  declared profile takes its offset under any policy (the routes load only
  when needed); `always` opens; `learned` opens at margin >= offset;
  `routed` takes the nearest centroid (cosine on the pooled question vector,
  first maximum wins) and falls back to the learned offset when the cosine is
  below that profile's floor and the protocol enables the fallback. NaN
  margins and unknown profiles are errors, never a silent default.
- Diagnostics per case: `gate_offset`, `gate_profile`, `gate_similarity`,
  `gate_fallback` beside the margin. The declared profile is an internal
  `Options.span_gate_profile` (verifier flag `--span-gate-profile`); it is not
  in the HTTP API yet.
- Python side: `qwen_span_copy.py gate-routes` writes the candidate (route
  table from the frozen 8.16 gate for one head seed, optional 8.17 fallback,
  policy, offset, digests); `span_gate_decision` mirrors `decideGate` and
  shares its test vectors; `encoder_lora_export.py export-reader
  --include-span-gate-routes` ships the table.

Bundles (s31 head, `qwen-span-copy/`): `span1-35k-routed/` (policy routed,
centroid fallback, offset -3) for the parity check; `span1-35k-declared/`
(policy learned, offset -3, routes without fallback) is the 8.17 default.

Verification on the 8.12 sets (depot dev 64, depot fresh 80, SQuAD 200;
`span1-35k-routed/{verify.sh,compare.py,compare.json}`), against the Python
reference on the reference encoder's question vectors
(`python-route-reference.json`):

- Routed pass: margins identical to the 8.12 always-on Zig run (max diff
  0.0); profile, fallback and open decision agree on 344/344 cases; cosine
  differences max 0.0048, mean 0.0014 (managed f32 encoder vs the reference
  encoder).
- Declared pass (`narrativeqa`, offset -inf): all 344 open, every answer
  equal to the always-on run's.
- Outcomes: depot dev correct 63 always / 47 learned at offset 0 / 63
  routed; depot fresh 80 / 64 / 79 (the one routed miss is a missing-fact
  case that fell back and closed); SQuAD EM 0.600, F1 0.717 under every
  policy (open 200 / 198 / 197).

Tests: `memorilla_span.zig` (protocol parsing, route loading and routing,
malformed tables), `test_qwen_span_copy.py` (the same vectors,
`gate_routes_table`, `gate-routes` candidate), `test_encoder_lora_export.py`.
Zig tests run from the installed test binary: `zig build test-bin
-Doptimize=ReleaseSafe`, then `zig-out/bin/antfly-inference-tests
--test-filter emorilla` (the debug test binary does not link on aarch64,
"fixup value out of range", and `zig build test-memorilla-source --
--test-filter` does not filter). The full run also exposed a test bug in
`memorilla_token_features.zig` ("span batch prefixes every doubled document
with the question" unwrapped a null `sources`), fixed.

Default bundle (8.17 rule 3; `span1-35k-declared/{verify.sh,compare.py,compare.json}`):
policy learned at offset -3, routes present, no declared profile. All 344
cases report policy learned, offset -3 and no profile; a case opens exactly
when its margin is >= -3 (311 open vs 310 at the 8.12 offset 0: one case has
a margin in [-3, 0)); margins equal the always-on run; every answer equals
the always-on answer when open and the 8.12 learned answer when closed. Depot
dev 47/64, fresh 64/80; SQuAD EM 0.600, F1 0.717. The depot numbers are why
exact-lookup requests need the window always on (8.11): the margin gate
closes 16 dev and 16 fresh cases, and without the window the decoder answers
from the soft rows alone. What remains for serving is a per-request policy
(`always` for exact lookups, a declared profile for known task types); it
needs an HTTP API field and was not added pending a decision.

### 8.19 Zig span-hybrid latency (2026-11-17, `qwen-span-copy/{latency,zig-latency}/`)

Goal: per-request latency of the Zig span hybrid near the Python BF16
evaluator at batch 1, answers as close to Python as before. Baseline (8.12
`latency-idle-span`, medians depot / SQuAD): pooled 568 / 1343 ms,
span_features 1797 / 4770, span_head 775 / 754, soft_rows 19 / 9, generate
3159 / 1299; p50 total 6.4 s / 8.5 s.

Diagnosis (nsys API trace; CUPTI kernel records are dropped on this GB10 /
driver 580, so per-op timing came from the runtime's own event profiler,
`ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS=1`, now logged per stage as
`memorilla_op_profile`):

- span_head: the 130 MB head was re-read, sha256-checked and parsed on every
  request (~700 ms).
- Encoder passes and decoder prefill: the Memorilla profiles' dense F32
  attention ran `termite_gqa_attention_f32`, one thread per output element
  that recomputes Q.K for every head dimension (O(n^2 d^2)); the cooperative
  `prefill_fast` kernel is not taken under the `automatic` profile and is
  sync-latency bound anyway (SQuAD span pass: 1.28 s of attention). F32 GEMMs
  run zero-workspace cuBLASLt. The pooled pass padded every text to 256
  tokens (`trim_padding_to_batch_max` off).
- Decode: ~140-155 ms per token, the F32 weight-bandwidth floor (32.8 GB per
  token); only BF16 weights halve it.
- Per request: the 12 MB encoder LoRA was re-read and hashed (~65 ms).

Changes:

1. Default path (all profiles): per-decoder span-head cache and per-encoder
   adapter cache (`server/memorilla_span_head_cache.zig`, fields in
   `model_manager.zig`): full sha256 verification against `manifest.json` on
   a miss, stat-identity fence (inode, size, mtime, ctime of the directory and
   the pinned files) on a hit, weights admitted for the entry's lifetime, no
   admission under the cache mutex (unit test for the head cache). The
   decoder is pinned before the head stage. Results are bit-identical.
2. Research mode (opt-in env, not a qualification; both switches change the
   execution identity so no pinned F32 identity matches):
   - `ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES=1`: rank-2 layer matrices of
     both models (`model.layers.*` / encoder `layers.*`) stay resident in their
     BF16 source precision and run as BF16 cuBLASLt GEMMs with F32
     accumulation and output; embeddings, norms, lm_head, KV and every other
     activation stay F32 (`memorilla_qualification.researchBf16Matrices`,
     upload in `session_factory`, the checks in `requireMemorillaProfile`,
     `requireCudaExecutionStats` and `memorilla_generation.Decoder.validate`).
   - `ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL=1`: dense multi-row attention runs
     a new tiled F32 kernel (`ops/cuda/memorilla_tiled_prefill.cu`: 32x32
     query/key tiles in shared memory, online softmax, head_dim 128; prebuilt
     sm_121 cubin via `scripts/build_memorilla_tiled_prefill.sh`, because
     nvcc 13.2 PTX 9.2 does not JIT on driver 580; loaded lazily in
     `ops/cuda/kernels.zig`); max abs error <= 1.7e-6 against an F64
     reference, ~1.5 ms per layer for 3 x 190 tokens vs ~35 ms for the
     cooperative kernel. The pooled pass pads to the batch's longest text; the
     plain-prompt cap rises from 1024 to 4096 tokens (RAG baselines).

Parity, research mode over all 344 cases (`latency/full-research-*`, all
passed; citation mismatches 0; admission refunded on every request):

| set | policy | research mode | F32 Zig (8.12/8.18) | Python | answers = Python (research / F32 Zig) | answers = F32 Zig | gates = F32 Zig |
|---|---|---|---|---|---|---|---|
| depot dev (64) | always | 63/64 | 63/64 | 64/64 | 63 / 63 | 64 | 64 |
| depot fresh (80) | always | 80/80 | 80/80 | 80/80 | 80 / 80 | 80 | 80 |
| SQuAD (200) | always | EM .595 / F1 .716 | .600 / .717 | .595 / .714 | 186 / 188 | 198 | 200 |
| depot dev (64) | learned | 47/64 | 47/64 | 48/64 | 57 / 57 | 64 | 64 |
| depot fresh (80) | learned | 64/80 | 64/80 | 64/80 | 78 / 78 | 80 | 80 |
| SQuAD (200) | learned | .595 / .716 | .600 / .717 | .595 / .714 | 187 / 189 | 198 | 200 |
| default bundle (344) | learned, offset -3 | 311 open, rule holds | 311 open | | | | 344 |

The research mode moves two SQuAD answers away from Python (`squad-0154`
"randomized" for "randomized algorithms", `squad-0171` lower-case
"computational"); three SQuAD windows differ on borderline spans (max margin
difference 0.13 SQuAD, 0.42 depot). The default F32 path is unchanged
(head/adapter caches only; margins bit-identical on the subsets checked).

Latency (`zig-latency/final_timing.sh`, 2026-11-17 06:02-06:38Z, GPU otherwise
idle: 40 depot-dev + 40 SQuAD cases, window always, batch 1, first case per set
dropped; medians in ms, `zig-latency/summarize.py`):

| run | set | pooled | span features | span head | soft rows | generate | p50 total | p90 total |
|---|---|---|---|---|---|---|---|---|
| Python BF16 | depot dev | 138 | 68 | 2 | 2 | 653 | 863 | 1,226 |
| Python BF16 | SQuAD | 149 | 110 | 2 | 2 | 470 | 681 | 1,003 |
| Zig before (8.12 idle run) | depot dev | 568 | 1,797 | 775 | 19 | 3,159 | 6,405 | 7,629 |
| Zig before (8.12 idle run) | SQuAD | 1,343 | 4,770 | 754 | 9 | 1,299 | 8,473 | 15,914 |
| Zig F32 + caches (default path) | depot dev | 266 | 891 | 26 | 8 | 1,771 | 2,776 | 3,272 |
| Zig F32 + caches (default path) | SQuAD | 1,244 | 4,611 | 50 | 8 | 1,126 | 7,317 | 11,120 |
| Zig BF16 matrices only | depot dev | 143 | 625 | 27 | 8 | 1,112 | 1,847 | 2,107 |
| Zig BF16 matrices only | SQuAD | 958 | 4,049 | 52 | 8 | 733 | 5,931 | 9,128 |
| Zig fast prefill only | depot dev | 178 | 358 | 27 | 8 | 1,438 | 1,894 | 2,303 |
| Zig fast prefill only | SQuAD | 431 | 840 | 52 | 8 | 793 | 2,206 | 2,994 |
| **Zig research (both)** | depot dev | 55 | 95 | 25 | 8 | 778 | **960** | 1,134 |
| **Zig research (both)** | SQuAD | 84 | 194 | 50 | 8 | 402 | **782** | 1,068 |

Parity of these timing runs (`final-*.compare.json`): every variant's
answers, windows, spans and gates equal the F32 Zig reference on 80/80;
answers equal Python on 39/40 depot and 40/40 SQuAD in all variants; max
margin difference 0.25 (research), 0.17 (BF16 only), 0.0006 (fast prefill
only), 0 (default path).

Reading: the research mode is 6.7x (depot) and 10.8x (SQuAD) faster than
before and within 11-15% of Python at p50. The fast-prefill kernel is the
bigger lever on long inputs (SQuAD span features 4.6 s -> 0.84 s), BF16
matrices on decode (depot generate 1.77 -> 1.11 s); together they compound.
What is left vs Python: the span head (25-50 ms, host CPU; Python 2 ms on the
GPU) and per-token decode (F32 lm_head and activations). The default F32 path
gains only from the caches (6.4 -> 2.8 s depot; SQuAD barely moves).

RAG timing on the same runtime (research mode, 40 TriviaQA cases over the
256-token chunks of 8.20, plain prompts through `source_decoder_service`,
retrieval excluded): RAG-1 p50 502 ms (293 prompt tokens), RAG-3 851 (710),
RAG-5 1,227 (1,095); span hybrid with the top-2 256-token chunks as documents,
window always: p50 1,360 ms (pooled 163, span features 724, span head 138,
soft rows 8, generate 364). With 256-token chunks the span pass (the encoder
over two ~280-token passages, each doubled for self-context) costs more than
the RAG-5 prefill it replaces; precomputed chunk vectors (-163 ms) and a GPU
span head (-~130 ms) would bring the hybrid to ~1.07 s, between RAG-3 and
RAG-5.

### 8.20 RAG over 256-token chunks vs the span hybrid (2026-11-17, `qwen-span-copy/rag256/`)

Question: does the hybrid still beat text RAG when RAG gets a stronger
chunking (256-token chunks instead of Memorilla-Data's short source chunks),
and what does each cost per request?

Setup (`rag256/{rechunk.py,main.sh,assemble.py}`):

- Chunks: each collection's Memorilla-Data chunks, in source order,
  newline-joined and packed greedily into at most 256 Qwen3 tokens; a longer
  source chunk is split at word starts. TriviaQA: 500 collections, 28,624
  chunks, mean 208 tokens. NarrativeQA: 342 collections, 128,480 chunks, mean
  206.5 tokens (one sampled chunk is 257 tokens after a join). Chunks and
  questions are embedded with the frozen Qwen3-Embedding-4B exactly as the
  released soft path pools them; the top 10 by cosine replace each case's
  `top_documents`.
- Same paper samples (TriviaQA 500, NarrativeQA 1,000), same frozen Qwen3-8B.
  RAG-k uses the paper baseline protocol (8.13). The soft reader still reads
  the original chunk vectors, so `soft` is unchanged.
- Hybrid: s31 (`span1-35k`), window cut from the top-2 256-token chunks
  (`--encoder-max-length 2048`), window always; the gate is applied offline as
  the default bundle does it (window iff margin >= -3, else the soft row).
  `span-orig-*` are the 8.14 s31 rows (window from the top-2 original chunks).

Retrieval. TriviaQA, gold answer string in the top-k chunks: original chunks
46.6 / 60.6 / 74.2 / 80.6% (k = 1 / 2 / 5 / 10), 256-token chunks
74.0 / 84.0 / 90.0 / 93.8%. NarrativeQA answers are free-form, so the
measure is the share of the answer's content words in the top-k chunks:
original 11.5 / 16.2 / 25.5 / 34.0%, 256-token 22.9 / 31.4 / 44.3 / 53.8%
(the top-2 chunks hold ~32 words before, ~259 after).

Quality (TriviaQA EM, NarrativeQA ROUGE-L; mean prompt tokens in brackets):

| arm | TriviaQA | NarrativeQA |
|---|---|---|
| soft | 54.6 (133) | 18.90 (67) |
| RAG-1, 256-token | 54.6 (295) | 14.37 (278) |
| RAG-3, 256-token | 56.4 (703) | 17.06 (710) |
| RAG-5, 256-token | 57.6 (1,093) | 17.76 (1,142) |
| RAG-5, original chunks | 54.8 (316) | 13.87 (418) |
| hybrid, original chunks, gate -3 | 66.2 (158) | 20.49 (71) |
| hybrid, original chunks, window always | 65.0 (165) | 21.99 (99) |
| hybrid, 256-token chunks, gate -3 | 66.0 (164) | 21.68 (75) |
| hybrid, 256-token chunks, window always | 66.0 (165) | **23.25** (99) |

Paired deltas (bootstrap 95% CI):

| pair | TriviaQA | NarrativeQA |
|---|---|---|
| RAG-5 original -> RAG-5 256-token | +2.80 [+0.20, +5.60] | +3.88 [+2.65, +5.10] |
| RAG-5 256-token -> hybrid 256-token gate -3 | **+8.4 [+4.2, +12.6]** | **+3.92 [+2.08, +5.75]** |
| RAG-5 256-token -> hybrid 256-token always | +8.4 [+4.2, +12.6] | +5.50 [+3.59, +7.40] |
| RAG-3 256-token -> hybrid 256-token gate -3 | +9.6 [+5.4, +14.0] | +4.62 [+2.75, +6.45] |
| hybrid original -> 256-token, gate -3 | -0.2 [-3.6, +3.2] | +1.19 [+0.00, +2.37] |
| hybrid original -> 256-token, always | +1.00 [-2.60, +4.60] | +1.26 [-0.37, +2.88] |
| hybrid 256-token, gate -3 -> always | 0.00 (488/500 open) | +1.57 [+0.33, +2.95] (233/1000 open) |

Findings:

1. 256-token chunks make RAG better (TriviaQA +2.8, NarrativeQA +3.9 for
   RAG-5) but not better than the hybrid: the hybrid over the same 256-token
   chunks beats RAG-5 by +8.4 EM on TriviaQA and +3.9 to +5.5 ROUGE-L on
   NarrativeQA, with 85-93% fewer prompt tokens. On NarrativeQA RAG-5 still
   trails the soft reader alone (-1.15 [-2.98, +0.66]).
2. On TriviaQA the hybrid does not gain from the higher recall. Split by where
   the gold string is (window always): answer only in the 256-token top-2 (127
   rows): hybrid 50.4 -> 59.1, gold inside the window on 83/127, but soft
   alone already scores 56.7 on these, so the decoder mostly knew them; answer
   in both top-2 sets (299 rows): gold inside the window 268 -> 243, EM
   78.3 -> 77.6, i.e. the head localizes worse in 256-token passages (it was
   trained on the short source chunks). The two effects cancel (+1.0, CI
   spans 0). A head trained on longer passages is the obvious next step.
3. On NarrativeQA the 256-token chunks help the window (+1.2 to +1.3), and
   window-always beats the no-profile gate (-3 opens 23% of rows) by +1.6, as
   8.15 found for the original chunks (the routed gate's NarrativeQA profile
   opens every row). Best NarrativeQA arm: 23.25, +4.35 [+2.68, +6.06] over
   soft.

Latency: 8.21 (Python BF16 and Zig, batch 1, with native Memorilla as RAG-0).

### 8.21 GPU span head and RAG-0..5 latency (2026-11-17, `rag256/latency/`, `zig-latency/gpuhead-*`)

GPU span head. The span head now runs on the device by default
(`models/memorilla_span_head_runtime.zig`, cached per decoder as
`memorilla_reader_cache.SpanHeadCache`): `span-head.safetensors` is verified
against `manifest.json` on every miss, uploaded once as F32 resident weights
of its own CUDA compute (the fused `in_proj` split into q/k/v), and run with
cuBLASLt F32 GEMMs, the backend LayerNorm / exact GELU / add, and full
self-attention on the tiled F32 kernel of 8.19 in non-causal form
(`KernelModule.launchMemorillaFullAttentionF32`: every query placed after the
last key). The host adds the segment/position/no-answer rows as one uploaded
addend; only the `[T + 1, 2]` logits come back. Request tensors are freed and
`releaseMemorillaRequestResources` checks for leaks after every forward; a
failed forward retires the entry. `ANTFLY_MEMORILLA_SPAN_HEAD_HOST=1` keeps the
host head for A/B runs.

| run (40 + 40, batch 1) | span head ms (depot / SQuAD) | p50 total ms (depot / SQuAD) |
|---|---|---|
| Zig F32, host head | 26 / 50 | 2,776 / 7,317 |
| Zig F32, GPU head | 3 / 4 | 2,755 / 7,244 |
| Zig research, host head | 25 / 50 | 960 / 782 |
| Zig research, GPU head | 4 / 5 | 945 / 735 |
| Python BF16 | 2 / 2 | 863 / 681 |

Parity (`gpuhead-*.vs-host.json`): answers, windows, spans and gates equal the
host-head runs on 80/80 in both modes; the largest margin difference is
4.1e-5 (median 4.8e-6). Full parity, research mode, all 344 cases
(`gpuhead-full-{always,learned}.vs-host.json`): answers, windows, spans and
gates equal the host-head research runs (8.19) on 344/344 under both
policies; margin difference max 4.1e-5, median 3.8e-6. Against the F32
reference the GPU-head run differs exactly where the host-head research run
did (SQuAD: 2 answers, 3 windows; gates 344/344). Unit tests
(`--test-filter emorilla`, ReleaseSafe): 154 selected, 151 passed, 3 skipped,
including the new resident-geometry test for the device head.

RAG-0..5 latency (batch 1, first rows dropped as warm-up, retrieval itself
excluded; RAG-0 is native Memorilla: the released reader over the stored
chunk vectors, no text). Python BF16 on the first 53 cases per target
(`rag256/latency.sh`, 50 timed); Zig research mode with the GPU head on 40
TriviaQA cases (`zig-latency/rag/run_rag.sh`, `--soft-set` for RAG-0: the
first 64 stored vectors per collection, the Zig native cap; 26 of 40
collections are larger; both runtimes start at the first case, 37 timed cases
in common):

| TriviaQA (256-token chunks) | prompt tokens | Python p50 ms | Zig p50 ms | EM (500 rows, 8.20) |
|---|---|---|---|---|
| RAG-0 (native Memorilla) | 128 | **386** | **385** | 54.6 |
| RAG-1 | 296 | 486 | 499 | 54.6 |
| RAG-3 | 710 | 645 | 850 | 56.4 |
| RAG-5 | 1,109 | 861 | 1,233 | 57.6 |
| span hybrid | 160 | 625 | 1,258 | **66.0** |

| NarrativeQA (Python) | prompt tokens | generated tokens | p50 ms | ROUGE-L (8.20) |
|---|---|---|---|---|
| RAG-0 | 66 | 5 | **469** | 18.90 |
| RAG-1 | 293 | 18 | 1,689 | 14.37 |
| RAG-3 | 727 | 18 | 1,872 | 17.06 |
| RAG-5 | 1,165 | 18 | 2,040 | 17.76 |
| span hybrid | 98 | 5 | 712 | 21.68 (gate -3) / 23.25 (always) |

Reading:

1. Native Memorilla is the fastest arm on both runtimes and the same speed in
   Zig and Python (385 / 386 ms: question encoding ~41 ms, reader ~8 ms, the
   rest generation).
2. Python: the hybrid costs RAG-3 time on TriviaQA (625 vs 645 ms; span
   record pass 234 ms, generation 387 ms) and is 27% faster than RAG-5 with
   +8.4 EM. On NarrativeQA the baseline RAG prompt makes the decoder write
   ~18 tokens instead of ~5, so RAG-k there is slower mostly by output length.
3. Zig: decode-bound arms match Python (RAG-0, RAG-1, the hybrid's 364 ms
   generation); long prefills do not: the span-features pass over two
   256-token chunks (each doubled) takes 731 ms vs Python's 234 ms for the
   whole record pass, and RAG-5 1,233 vs 861 ms. Zig keeps F32 activations
   (BF16 only in the layer matrices); BF16 activations for long prefills are
   the next lever. The Zig hybrid also re-embeds its two documents per
   request (pooled 163 ms); with stored vectors only the question (~41 ms)
   remains, ~1.14 s.
4. The span head is no longer a cost anywhere (3-8 ms).

### 8.22 Zig faster than Python (2026-11-17, `zig-latency/fast-*`, `fast2-*`)

Goal: every 8.21 arm at least as fast in Zig as in Python BF16, same answers.
All runs: batch 1, research mode (`ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES=1`,
`..._FAST_PREFILL=1`), each run started on a quiet GPU (no other compute
process at start or end), first case dropped, 39 timed TriviaQA cases per arm
and 39 + 39 depot-dev / SQuAD.

Diagnosis (8.21 hybrid, TriviaQA). Kernel tracing is unavailable on this host
(nsys drops every kernel record of the verifier as incomplete; ncu has no
counter permission), so the CUDA API trace and the per-op event profile
(`ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS=1`, now printing every category)
were used. Of the 724 ms span-feature pass, ~240 ms were the two 15 MB F32
hidden-state downloads (layer-24 tap and final layer) into fresh pageable
memory (87-155 ms each); attention (F32 tiled kernel) 105 ms; F32 activation
traffic (staging, elementwise, norms, RoPE, the LoRA scale/add) ~150 ms; BF16
GEMMs ~150 ms (64 TFLOPS, the same rate PyTorch reaches for these shapes). The
pooled pass (163 ms) re-embedded the question and the two span documents,
which Python reads as stored vectors (its timing excludes retrieval).

Changes:

1. Large downloads stage through the pinned 4 MB buffer in chunks
   (`copyToPinnedBulkDownloadAndSync`; before, only 64 KB-4 MB did).
2. Stored vectors: `Options.stored_question` (native service and hybrid) and
   `Options.stored_documents` (the hybrid's soft half reads the collection's
   stored chunk vectors, as Python's hybrid does; the span pass still reads
   the top-2 texts). Verifier: `--stored set:cases:vectors:questions` and an
   optional questions file on `--soft-set`.
3. BF16 tensor-core flash attention for research causal prefill
   (`memorilla_gqa_prefill_flash_bf16`, mma.sync m16n8k16, F32 in/out, BF16
   Q/K/V/P, F32 softmax; same cubin; `ANTFLY_MEMORILLA_RESEARCH_F32_ATTENTION=1`
   keeps the tiled F32 kernel). Against an FP64 reference its error is BF16
   rounding (mean 0.3% of the mean output); 0.40 vs 5.4 ms per layer for the
   span pass (SDPA: 0.25 ms). The span head keeps the F32 kernel.
4. BF16 activations where PyTorch has them: hidden-width RMS norms also emit
   their BF16 rounding (the exact values staging produced), the gated SiLU FFN
   keeps BF16 between its projections (gate/up BF16 out, fused BF16
   SiLU-multiply, down F32 out), and the LM head is a BF16 matrix. The
   residual stream, norms, KV cache and embeddings stay F32.
5. Fused LoRA epilogue (`loraUpAdd`: `base + scale * down @ up^T` in one pass;
   was an F32 GEMM, a scale and an add per projection).
6. Lossless speculative decoding (`memorilla_generation.speculativeDraft`):
   at every greedy step the answer so far is looked up (word start, ASCII
   case-insensitive) in the hybrid's predicted span, then its evidence
   window, or a plain prompt's user turn (prompt lookup); the following text's
   tokens (span drafts plus the end-of-turn token, at most 16) are fed in one
   multi-row step, the prefix greedy decoding itself selects is kept and the
   rejected tail's KV truncated. A wrong draft costs about one decode step,
   which it replaces. `ANTFLY_MEMORILLA_SPAN_DRAFT_OFF=1` disables it.

| TriviaQA p50 ms (39 cases) | Python BF16 | Zig 8.21 | Zig, no drafts | Zig |
|---|---|---|---|---|
| RAG-0 (native Memorilla) | 386 | 385 | 330 | **329** |
| RAG-1 | 486 | 499 | 463 | **225** |
| RAG-3 | 645 | 850 | 707 | **360** |
| RAG-5 | 861 | 1,233 | 928 | **492** |
| span hybrid | 625 | 1,258 | 633 | **481** |

| depot-dev / SQuAD hybrid p50 ms | Python BF16 | Zig 8.21 | Zig |
|---|---|---|---|
| depot-dev | 863 | 945 | **324** |
| SQuAD | 681 | 735 | **399** |

"No drafts" is the like-for-like decoder (Python decodes plainly): there Zig
matches Python on the hybrid (633 vs 625), is faster on RAG-0/1 and slower on
RAG-3/5 (longer prefills). With drafts every arm is faster than Python; the
drafts change no answer (hybrid drafts-off control 39/39, every RAG arm
identical with and without prompt lookup). Hybrid stages now (p50): span
features 273, span head 8, soft rows 13, window rows 2, generation 177 ms
(was 364; a verified span draft finishes the answer in one step).

Parity (b20, all 344 cases, always gate): answers equal the 8.21 research
run on 342/344 and the F32 reference on 342/344, gates 344/344, largest margin
change 0.71; SQuAD EM/F1 59.5/71.6 and Python agreement 186/200, depot 63/64
and 80/80, all unchanged (of the two SQuAD changes one now matches Python,
one no longer does). TriviaQA hybrid EM 66.7 as in 8.21 (35/39 answers equal).

Left: the span-feature pass is still slower than Python's whole record pass
(273 vs 234 ms: F32 residual stream, separate residual adds, the o-projection
staging), the soft reader costs 13 ms (Python 4), and NarrativeQA has no Zig
rows yet. Prompt lookup would speed Python's baselines too
(`prompt_lookup_num_tokens`); the tables compare against Python as measured.

Unit tests (`zig-out-test`, ReleaseSafe): `--test-filter emorilla` 155
selected, 152 passed, 3 skipped (new: speculative drafts, research BF16
matrices incl. the LM head); the filters touching the shared code (LoRA,
gated, pinned, BF16, encoder, rms) pass with no failures.

### 8.23 NER-boosted top-2 retrieval (2026-11-18, `qwen-span-copy/ner-retrieval/`)

Question: if Antfly tags every chunk with GLiNER at ingest, does an entity
signal pick better top-2 chunks for the span hybrid than dense retrieval, and
than BM25, which Antfly already has? The span head reads only the top-2
chunks, so the measure is answer coverage of the top 2.

Setup (rules in `RULES.md`, written before the test half was scored):

- Cases: the 8.20 samples, TriviaQA 500 and NarrativeQA 1,000; dev = even
  `row`, test = odd. Every arm reranks each question's dense top-50.
- Pools: `256` = the 8.20 256-token chunks (questions re-embedded as
  `rechunk.py` did; top-10 identical on 1,500/1,500); `orig` = the
  Memorilla-Data chunks the 8.14 hybrid reads (stored paper vectors).
- Coverage: 8.20's code (`coverage.py`), reproduced exactly: TriviaQA
  74.0/84.0 and 46.6/60.6, NarrativeQA 22.9/31.4 and 11.5/16.2 (k = 1/2;
  256 / orig).
- GLiNER: fastino `gliner2.5-base-v1`, gliner2 2.1.0 `BoundaryExtractor`,
  threshold 0.5, labels person, organization, location, date, event, work of
  art, product, number; 475/500 TriviaQA and 961/1,000 NarrativeQA questions
  get at least one entity.
- Arms: (a) dense; (b) dense + lambda x chunk-entity matches (GLiNER on
  question and chunk, token-subset match; count b1 / fraction b2); (c) dense +
  lambda x question entities found in the chunk text (query-side NER only;
  c1 / c2); (d) BM25 over the whole collection fused with dense by RRF (d1)
  or linearly (d2); (e) d2 plus the better of b/c. Settings picked on dev.

Test half, coverage@2 (Δ vs dense, paired bootstrap 95% CI, 10,000
resamples, seed 23); `results.md` has every arm with coverage@1:

| arm | TriviaQA 256 | TriviaQA orig | NarrativeQA 256 | NarrativeQA orig |
|---|---|---|---|---|
| a dense | 84.0 | 57.6 | 31.6 | 17.1 |
| b1 chunk NER, count | 83.2 (−0.8 [−2.4, +0.8]) | 63.2 (**+5.6** [+2.0, +9.2]) | 31.4 (−0.2) | 17.3 (+0.1) |
| c1 query NER, string | 84.0 (+0.0) | 65.2 (**+7.6** [+3.6, +12.0]) | 31.9 (+0.3) | 18.3 (+1.2 [−0.2, +2.5]) |
| d1 BM25, RRF | 82.4 (−1.6 [−3.6, +0.0]) | 71.6 (**+14.0** [+9.6, +18.8]) | 31.8 (+0.2) | 18.2 (+1.1) |
| d2 BM25, linear | 83.2 (−0.8) | 73.6 (**+16.0** [+10.8, +21.2]) | 31.5 (−0.1) | 18.0 (+0.8) |
| e BM25 + query NER | 84.0 (+0.0) | 75.2 (**+17.6** [+12.8, +22.8]) | 32.6 (+1.0 [−0.8, +2.9]) | 18.1 (+0.9) |

Throughput (GLiNER base, GB10, batch 16): 80.8 chunks/s for 256-token
chunks (903 characters) and 200 chunks/s for the short originals (230
characters) on a quiet GPU; NarrativeQA's 65.6 and 73.5 ran beside another
job. At 80 chunks/s a 28,624-chunk collection set (TriviaQA 256) tags in about
six minutes per GPU.

Reading:

1. Short chunks leave lexical signal on the table: on TriviaQA's original
   chunks every lexical arm lifts top-2 coverage, and BM25 alone takes most of
   it (57.6 to 73.6). Adding query-side entities on top of BM25 is +1.6
   [−1.2, +4.4] (post hoc, `posthoc.txt`), not significant.
2. Chunk-side NER (what ingest tagging buys) never beats searching the
   question's entities in the chunk text (c1 − b1: +2.0 [−1.2, +5.6] on
   TriviaQA orig, post hoc), so tagging chunks at ingest adds nothing over
   tagging the query.
3. On 256-token chunks dense retrieval already covers 84% and no arm helps;
   on NarrativeQA (free-form answers, book-length collections) no arm is
   significant in either pool.

Verdict: NER is not worth wiring into Antfly retrieval for the hybrid. What
the experiment does recommend is BM25 + dense fusion when the top-2 come from
short chunks (+16 points of TriviaQA coverage on the chunks the 8.14 hybrid
reads); whether that coverage turns into EM is untested, since 8.20 found that
higher coverage from 256-token chunks did not raise the hybrid's TriviaQA EM.

### 8.24 Span head trained on 256-token passages (2026-11-18, `budget-study/head256/`)

Question (study A, pre-registered in `budget-study/DESIGN.md`): 256-token
chunks raised TriviaQA top-2 coverage from 60.6% to 84.0% (8.20) but hybrid EM
stayed flat; does a head trained on the passages it reads turn that coverage
into EM?

Setup: `qwen_copy_qa.py prep --chunking pack256` (opt-in; the rag256 packing,
identical on 60/60 TriviaQA and 6/6 NarrativeQA collections), each example's
documents the packs holding its top original chunks; span1-35k's sources,
quotas, seed and held-out split (the quota draw over packed rows shares
17,363 of 35,000 training names with span1-35k). Heads at seed 31: A-pack
(packed only) and A-mix (span1-35k's cache plus the packed one, twice the
steps). Packed train cache 5.05 h and 101 GB (2.4x / 2.1x span1-35k); training
38 / 48 min.

Held-out (8.11 metrics; screening on packed window-contains, A-pack 0.743 vs
A-mix 0.735, 6 of 785 positives apart, while A-mix led on exact span, F1 and
gate AUC):

| held-out | head | window contains | exact | gate AUC | TriviaQA window |
|---|---|---|---|---|---|
| original chunks | s31 | 0.749 | 0.428 | 0.925 | 0.919 |
| original chunks | A-pack | 0.766 | 0.471 | 0.905 | 0.903 |
| 256-token packs | s31 | 0.698 | 0.432 | 0.892 | 0.691 |
| 256-token packs | A-pack | 0.743 | 0.473 | 0.908 | 0.783 |

Test (8.20 rag256 cases, window always): TriviaQA EM 64.6 vs s31 66.0,
−1.4 [−3.4, +0.6]; NarrativeQA ROUGE-L 23.45 vs 23.25, +0.20 [−0.79, +1.17].
Rule A (TriviaQA lower bound > 0) fails; seeds 32/33 not trained. Gate −3 and
the comparison with s31 on original chunks are non-significant as well.

Reading (with the independent audit): the head localizes better on held-out
packs, but EM changes on only 29 of 500 TriviaQA rows (11 better, 18 worse).
The audit's split of s31-256's 170 TriviaQA misses is 57 answer not in the
top-2, 56 in the top-2 but outside the window, 57 in the window but answered
wrong (s31 on original chunks: 111 / 18 / 46): longer passages tripled the
window-placement misses, and A-pack's extra coverage mostly landed on rows s31
already answered. A compares fresh A-pack rows with 8.20's stored s31 rows;
identical reruns move TriviaQA EM by about ±1 (10 of 500 predictions change),
so part of −1.4 can be run noise. One seed only.

### 8.25 Read budget and window policy at equal encoder cost (2026-11-18, `budget-study/windows/`)

Question (study B, pre-registered in `budget-study/DESIGN.md`): does the
hybrid under-use what it reads? Read budget R2 (top-2 chunks, today) or R512
(ranked chunks until 512 Qwen3 document tokens); window policy P1 (one 32-token
window, today), P2 (one 64-token window) or P3 (best span per document, 32
tokens each, up to 64 rows). s31 head, original chunks. Options are opt-in in
`qwen_span_copy.py` / `qwen_hybrid_eval.py` (`--record-read-tokens`,
`--record-window-policy`); the default path reproduces the s31 records on all
300 dev rows.

Dev (300 validation rows per benchmark, disjoint from the 8.13 gate-fit rows)
selected R512P2 (mean +1.21) and R512P1 (+0.93); R512P3 led TriviaQA and
NarrativeQA (+2.0, +2.96) but lost FactKG −2.67, and R2P2/R2P3 lost about 1.5
ROUGE-L on NarrativeQA. Test (paper cases, window always, vs R2P1 regenerated):

| arm | TriviaQA EM (500) | NarrativeQA ROUGE-L (1,000) | FactKG acc (1,000) |
|---|---|---|---|
| soft | 54.20 | 18.94 | 84.40 |
| R2P1 | 66.00 | 21.90 | 86.60 |
| R512P2 | **+4.60 [+1.20, +8.00]** | **+2.01 [+0.42, +3.60]** | **−2.10 [−3.90, −0.40]** |
| R512P1 | +2.00 [−1.20, +5.20] | **+2.38 [+0.89, +3.90]** | **−1.70 [−3.30, −0.10]** |

Read cost (chunks / tokens read): R2 2 / 95, 139, 23; R512 15.2 / 459,
15.6 / 469, 36.6 / 422 (TriviaQA, NarrativeQA, FactKG). Prompt rows 48 (P1) or
80 (P2).

Rule B (no benchmark CI upper < 0, one CI lower > 0) keeps R2P1: both
finalists lose FactKG. The audit recomputed every delta exactly and adds:
R512P1's exclusion hangs on one FactKG row (upper −0.10) and R512P1 passes rule
B in the gate −3 view, so it is a marginal exclusion; R512's encoder input is
3.4–4.8x R2's on these short chunks (it equals two 256-token chunks, the
setting of the 8.19–8.22 latency numbers); and all of R512's gains, and all of
its FactKG loss (−1.8 points; rows with the window in documents 0–1 net +0.1),
come from windows placed in document index >= 2, where the s31 head's segment
rows are untrained (it trained on two documents and clamps after four). Wider
reading helps prose questions with this head, but the effect is confounded
with an out-of-distribution head; the FactKG "near-duplicate distractor"
account is one explanation, untrained segment rows another. Dev sets of 300
rows were too small to screen FactKG (R512P2 dev +1.33 [−2.00, +4.67], test
−2.10).

Next (study C, `budget-study/DESIGN-C.md`, pre-registered): a head trained on
R512 read sets with 64 segment rows and a 64-token window (C1), and the same
head on BM25 + dense fused read order (C2), against R2P1 on the same test
cases.

### 8.26 Span head trained on wide read sets; fused read order (2026-11-18, `budget-study/wide/`)

Question (study C, pre-registered in `budget-study/DESIGN-C.md`): 8.25's R512P2
gains, and its FactKG loss, came from windows in document index >= 2, where
s31's segment rows are untrained. Does a head trained on R512 read sets keep
the gains and remove the loss (C1)? Does BM25 + dense fused order (8.23 d2,
w = 0.5) add EM with the same head (C2)?

Setup: `qwen_copy_qa.py prep --read-tokens 512` (opt-in; each example's
documents are its dense R512 read set, the eval's rule, matching the paper
cases' ranking on 15/15 cases per benchmark), span1-35k's sources, quotas,
seed and held-out split; `qwen_span_copy.py train --max-documents 64 --window
64` (opt-in; 64 segment rows, otherwise s31's settings), seed 31. Train cache
7.6 h and 107 GB, training 15 min. Test: paper cases, R512, P2, window always,
paired against B's stored base (R2P1) and ctrl (s31, R512P2) rows.

| arm (window always) | TriviaQA EM (500) | NarrativeQA ROUGE-L (1,000) | FactKG acc (1,000) |
|---|---|---|---|
| base: s31, R2, P1 | 66.0 | 21.90 | 86.6 |
| ctrl: s31, R512, P2 | 70.6 | 23.91 | 84.5 |
| C1: new head, R512, P2 | 70.6 | 24.25 | 84.8 |
| C2: new head, fused R512, P2 | 70.6 | 24.61 | 84.8 |
| **C1 − base (rule C1)** | **+4.60 [+1.40, +7.80]** | **+2.34 [+0.88, +3.85]** | **−1.80 [−3.30, −0.30]** |
| C2 − C1 (rule C2) | +0.00 [−2.20, +2.20] | +0.36 [−1.01, +1.69] | +0.00 [−0.70, +0.70] |
| C1 − ctrl (head effect) | +0.00 [−2.20, +2.20] | +0.34 [−0.65, +1.35] | +0.30 [−1.00, +1.60] |

Both rules fail: C1 on FactKG (39 rows worse, 21 better), C2 with no CI lower
bound above 0 (also on the odd-row halves 8.23 did not tune on: TriviaQA
+2.0 [−0.8, +5.2], NarrativeQA −0.39). Seeds 32/33 not trained. Gate −3:
C1 − base +4.20 / +0.82 / −1.40 [−2.90, 0.00]. Held-out (R512 cache, window
64): window contains 0.742 → 0.775, exact 0.412 → 0.447, gate AUC 0.903 →
0.914; none of it reached test. The identical 70.6 TriviaQA EM of ctrl, C1 and
C2 is a coincidence of totals (ctrl and C1 differ on 174 windows and 63
predictions, 15 better each way).

Reading (with the independent audit, `wide/AUDIT-C.md`, which rescored every
row and reproduced every decisive CI exactly): the head changes where the
window lands, not the outcome. On FactKG every document is one ~12-token
triple, so reading more only changes which of ~37 near-duplicates the reader
sees. The trained head moved the window back into the top 2 on 237 of ctrl's
750 far-window rows and ties base there, but loses 3.44 points on the 524 rows
where it still picks a later triple, including document indices 2–9 whose
segment rows were well trained. Untrained segment rows are therefore not the
main cause of B's FactKG loss; the reader taking a later near-duplicate is the
likely one (untested). The same move to later documents carries most of the
TriviaQA and all of the NarrativeQA gain. Fusion reads half as many, longer
chunks on TriviaQA at the same token budget and changes nothing measurable.

Two corrections from the audit:
- The rerun-noise rows used here and in 8.24/8.25 (the "8.14" s31 rows, 10 of
  500 TriviaQA predictions different) were generated at batch 16, the
  study rows at batch 64. Record texts are identical, so that estimate is
  batch-size sensitivity, not run-to-run noise. Same-batch generation looks
  deterministic (73 of 73 identical inputs gave identical outputs in C1/C2).
  RESULTS.md's post-hoc sensitivity row (C1 against the 8.14 rows, FactKG
  −1.50 [−3.00, 0.00]) compares across batch sizes and does not show the
  verdict sits within rerun noise.
- Latent hazard, not triggered here: the record-cache key omits the span
  head, so running a new head into a directory holding another head's records
  reuses them silently. Key it on the head's hash before the next head study.

The span-head training caches (span1-35k, study A, study C) and the
encoder-LoRA warm-up caches and checkpoints were deleted after this study
(about 743 GB); the held-out caches, heads, case files and scored rows remain.

What A, B and C show together: a head that localizes better on held-out data
has not raised test EM (A, C); reading more text helps prose QA and costs
claim verification, and the head is not the lever (B, C). The remaining
candidate is the read budget per route: R512P2 on prose routes, R2P1 on
claim verification, using 8.18's routes. It was suggested by these test
rows, so it needs pre-registration and rows outside the paper cases.

### 8.27 Read budget per route; fused order at two chunks (2026-11-19, `budget-study/routed/`)

Question (study D, pre-registered in `budget-study/DESIGN-D.md`): D1, R512P2
on prose routes (route `triviaqa` or `narrativeqa` of the frozen 8.16 gate)
and R2P1 everywhere else; D2, the top two chunks in BM25 + dense fused order
(8.23 d2, w = 0.5) instead of dense order. D1 came from B's and C's test rows,
so both run on rows no read-budget, window or fusion choice has seen: 8.16's
fresh validation rows minus B's dev rows (TriviaQA 1,700, NarrativeQA 3,161,
FactKG 3,700, PV4 1,755, PMv2 2,059). s31 head, batch 64, window always;
arms base (R2P1), fused (R2P1 on fused cases) and wide (R512P2), the
policies assembled per row. Pipeline 18:39-00:58Z, every stage on attempt 1.
Smoke: base and wide reproduce B's dev rows on 384/384 (predictions, token
ids, records), so same-batch generation is deterministic.

Routing (prose route / rows): TriviaQA 1,687 / 1,700, NarrativeQA 3,133 /
3,161, FactKG 1 / 3,700, PV4 4 / 1,755, PMv2 3 / 2,059.

| policy − base (window always) | TriviaQA EM | NarrativeQA ROUGE-L | FactKG | PV4 | PMv2 |
|---|---|---|---|---|---|
| base (absolute) | 63.12 | 21.54 | 81.59 | 70.31 | 35.41 |
| **D1 (rule D1 passes)** | **+3.35 [+1.59, +5.12]** | **+1.92 [+1.07, +2.78]** | −0.03 [−0.08, 0.00] | −0.06 [−0.17, 0.00] | 0.00 |
| D2 (rule D2 passes) | **+1.88 [+0.41, +3.35]** | +0.41 [−0.36, +1.17] | −0.32 [−0.92, +0.27] | −1.08 [−2.85, +0.63] | +1.41 [−0.15, +2.91] |
| wide on every row (reported) | **+3.35** | **+1.94** | **−1.95 [−2.81, −1.05]** | | |
| D1 + fused elsewhere (reported) | **+3.41** | **+1.95** | −0.32 | −1.14 | +1.46 |
| D1, shipped gate | **+3.12** | **+1.67** | 0.00 | −0.06 | 0.00 |
| D2, shipped gate | **+1.94** | +0.29 | **−0.65 [−1.22, −0.08]** | 0.00 | −0.05 |

The independent audit (`routed/AUDIT-D.md`) recomputed rows, routes (0 of
12,375 differ), the fused order (750/750 sampled), every score and all 66
bootstrap pairs exactly; no discrepancy changes a verdict or number. Its
reading, adopted here:
- D1 is robust: both prose gains survive 99.5% intervals, and dropping the
  67 TriviaQA rows whose questions duplicate paper-test or B-dev questions
  leaves +3.06 [+1.29, +4.84]. Its FactKG/persona cells are zero by
  construction (1, 4, 3 rows differ from base), so "no FactKG loss" is a
  routing result, not a measured absence of harm; wide on every FactKG row
  repeats B's loss on fresh rows (−1.95), which the routing avoids.
- D2 passes only as registered: one benchmark (TriviaQA, sign test p ≈ 0.016,
  99% interval −0.06 to +3.71) and neutral to negative elsewhere. The gain
  comes from coverage (top-2 answer coverage 57.3% → 69.4%; the 295 rows that
  gain coverage go 58.3 → 77.3 EM). Under the shipped gate it loses FactKG:
  fused order pushes 181 FactKG margins below the −6 offset, which was fit on
  dense-order margins. The soft reader reads every stored chunk vector, not the
  ranked list, so this is not an artefact of mixed fallback rows.
- With D1 in place, fusion adds about one TriviaQA row (D1 + fused +3.41 vs
  +3.35) and brings back the FactKG gate loss.

Decision: adopt D1 (per-route read budget). Do not adopt fused order as a
default; it is at most a prose-route option, and only with a FactKG gate
offset refit on fused margins.

Cost: D1 reads 14.5 chunks (about 460 Qwen3 tokens) and 80 memory rows on
prose routes, against 2 chunks and 48 rows. In the Python batch-64 runs,
record preparation rose from 0.10 to 1.03 s per row (TriviaQA) and 0.13 to
0.77 (NarrativeQA), and generation by about 25%. The Zig service has no
R512/P2 path yet. Its nearest measurement is 8.22's hybrid over two 256-token
chunks, about the same encoder input: p50 481 ms on TriviaQA (span features
273 ms). Other routes are unchanged.

Next: port D1 to Zig (read until 512 tokens on prose routes, 64-row window,
budget from the 8.18 route table; the span features of many short documents
packed into as few encoder passes as possible) and measure batch-1 latency
against R2P1 on TriviaQA and NarrativeQA, with answer parity against the
Python D1 rows.

### 8.28 D1 in Zig: per-route read budget, batch-1 latency (2026-11-19, `budget-study/routed/zig/`)

Code (uncommitted):
- `pipelines/memorilla_span.zig`: optional protocol field `span_read_routes`
  (route -> read tokens, window rows; strict parsing as in 8.18), plus
  `readBudget` and `readPlan`.
- `server/memorilla_hybrid_service.zig`: with read routes, `documents` is the
  ranked list. The question routes on its question vector (nearest centroid,
  no floor fallback, as `route_of`; a declared profile overrides) before the
  span pass. The read set is then the top 2 or the budget rule with the
  encoder tokenizer. Admission and the prompt are sized for the chosen window.
  Diagnostics carry the read route, documents/tokens read and window rows.
  Citations cover the read set only.
- Without read routes the path is unchanged.
- Verifier: `--bench-prompt` (paper prompts on the span path, internal
  `Options.span_bench_prompt`) and `--pause-file`.
- Python mirror: `span_read_set`, `span_read_routes_table`,
  `gate-routes --read-route` (shared test vectors).
- Bundle `span1-35k-d1/`: `span1-35k-declared` plus triviaqa and narrativeqa
  at 512 tokens / 64 rows. Only `protocol.json` and `candidate.json` differ.
- Opt-in `ANTFLY_MEMORILLA_RESEARCH_SPAN_SUBBATCH=1`: the span pass runs in
  length-sorted sub-batches, each document still its own sequence.

Tests:
- `--test-filter emorilla`: 158 selected, 155 passed, 3 skipped (3 new).
- 8.22's shared filters and `hybrid`: no failures.
- The `span` filter's one failure, `gliner_span_v2_executor (Overflow)`, also
  fails in the 8.22 binary and is in untouched code.
- Python: 38 passed.

Latency. 8.22 research mode, batch 1, window always, paper prompts, stored
question and chunk vectors. Rows: the first 100 prose-route rows of study D's
TriviaQA and NarrativeQA cases and 50 FactKG rows on R2. Another user's GPU
jobs ran throughout, so every case waited, untimed, for a quiet GPU, and
cases with a busy poll within 2 s were dropped. Deltas are paired over rows
clean in both arms, in ms.

| benchmark | base p50 / p90 | D1 p50 / p90 | D1 − base p50 | D1 + sub-batches p50 / p90 | D1+sb − base p50 |
|---|---|---|---|---|---|
| TriviaQA | 368 / 533 | 916 / 1,529 | **+548 (+149%)** | 556 / 692 | **+188 (+51%)** |
| NarrativeQA | 585 / 991 | 974 / 1,459 | **+390 (+67%)** | 720 / 1,180 | **+144 (+25%)** |
| FactKG (R2 route) | 273 / 278 | 270 / 276 | −3 | 268 / 273 | −6 |

- About 95% of the delta is the span-feature pass (median per case +528 ms
  TriviaQA, +370 NarrativeQA).
- D1 reads 13.2 / 11.6 chunks (~460 tokens). The single padded batch holds
  3,559 / 3,008 positions vs 476 / 582 for base, about 2.8x the useful ones.
  The cost is the encoder's dense per-token work (BF16 GEMMs, norms,
  elementwise, F32 state downloads), not attention.
- Sub-batches do the same useful work with little padding. TriviaQA span
  features go 619 → 304 ms.
- Generation is not slower: TriviaQA 270 → 202 ms, because the 64-row window
  contains the drafted answer more often (drafts accepted 67 vs 45 of 100).
- Span head +4 ms. Non-prose routes cost nothing measurable.

Parity with study D's Python rows (batch 64; base is the yardstick):
- Read sets equal Python's `read_documents` and `read_tokens` on every prose
  row. Routes equal `routes.json` on every row. Prompt token counts are equal.
- Answers equal, D1 vs base: TriviaQA 94 vs 94, NarrativeQA 47 vs 37, FactKG
  50 vs 50.
- Windows equal: 97 vs 97, 93/98 vs 98/100, 49 vs 49.
- Subset scores, Zig vs Python: TriviaQA EM base 55/55, D1 62/63. NarrativeQA
  ROUGE-L base 23.87/20.41, D1 25.27/24.02. NarrativeQA agreement is low in
  both arms: free-form wording, Zig's soft half capped at 64 stored chunk
  vectors (Python reads all), and batch 1 vs 64.
- Sub-batched D1 vs one-batch D1: answers, windows, spans and read sets equal
  on TriviaQA and NarrativeQA. Margins are bit-equal on 99/100 and 93/98
  (max 0.017). On FactKG one window and one span in 50 differ (margins bit-equal
  12/50, max 0.16): batch shape changes GEMM rounding.
- Regression: with the default bundle, 8.22's depot-dev/SQuAD timing set is
  bit-identical to `fast2-timing.jsonl` (80/80 answers, windows, spans,
  gates, citations; margin max diff 0.0). p50 326 / 403 ms (was 324 / 399).

Crash (open). One padded D1 batch panics the worker on 2 of 100 NarrativeQA
rows (25 x 536 and 15 x 443 positions). After the encoder run,
`releaseMemorillaRequestResources` finds 137 MB / 129 MB still live on the
device: "Memorilla encoder cleanup failed; worker restart required". It
reproduces exactly. It is not a size limit: larger TriviaQA batches pass, and
both rows are under the 16,384-position bound. Sub-batches run both rows.

Reading: with today's one-batch span pass, D1 is a large hit on prose routes
(+149% / +67% p50). With sub-batches it costs +188 ms (+51%) on TriviaQA and
+144 ms (+25%) on NarrativeQA for +3.35 EM / +1.92 ROUGE-L (8.27), and nothing
on other routes. The sub-batched pass already runs close to the per-token
cost of the useful positions. What is left is the price of encoding about 5x
more tokens; cheaper per-token encoder work (8.22 "Left": F32 residual
stream, activation traffic) is the remaining lever.

Before D1 ships:
- root-cause and fix the cleanup crash;
- make sub-batching the span pass's default and accept non-bit-identical
  margins, re-verifying the 344-case set as in 8.19/8.22;
- add an HTTP field for the ranked candidate list;
- lift or match the 64-vector soft-half cap.

### 8.29 D1 on the paper's eight benchmarks (2026-11-19, `budget-study/routed/paper-view/`)

This is a reading, not a test. It is assembled offline from existing rows,
with no new generation.

Routing the full test splits with the frozen 8.16 gate (prose routes / rows):
- TriviaQA 496 / 500, NarrativeQA 10,153 / 10,257.
- PubMedQA 0 / 100, FactKG 0 / 8,741, LaMP-4 0 / 1,625.
- LaMP-7 1 / 1,200, PV4 9 / 3,937, PMv2 6 / 4,700.

On the paper sample cases, D1 therefore changes only TriviaQA and
NarrativeQA, plus one row each of LaMP-7, PV4 and PMv2. Those three rows were
not generated, so each of those benchmarks can move by at most 0.1 point.

TriviaQA, NarrativeQA and FactKG come from study B's R2P1 and R512P2 test
rows (batch 64, window always). The shipped-gate view uses the 8.16 routed
s31 offsets, with B's soft rows when the gate is closed. The other five use
8.14's s31 and soft rows under the same gate.

| benchmark | metric | paper Memorilla | soft | today (R2P1, routed gate) | D1 (routed gate) | D1 − today [95% CI] |
|---|---|---|---|---|---|---|
| TriviaQA | EM | 57.00 | 54.20 | 67.20 | **70.80** | **+3.60 [+0.40, +7.00]** |
| NarrativeQA | ROUGE-L | 19.31 | 18.94 | 21.61 | **23.44** | **+1.84 [+0.30, +3.39]** |
| FactKG | acc | 84.33 | 84.40 | 86.60 | 86.60 | 0 (no row changes) |
| LaMP-4 | ROUGE-L | 18.15 | 15.97 | 15.97 | 15.97 | 0 |
| PubMedQA | acc | 59.00 | 59.00 | 59.00 | 59.00 | 0 |
| PMv2 | acc | 44.60 | 44.30 | 44.20 | 44.20 ± 0.1 | 1 row |
| LaMP-7 | ROUGE-L | 48.37 | 46.58 | 46.58 | 46.58 ± 0.1 | 1 row |
| PV4 | acc | 66.65 | 66.70 | 66.70 | 66.70 ± 0.1 | 1 row |

With the window always on: TriviaQA 66.0 → 70.6 (+4.6 [+1.2, +8.0]) and
NarrativeQA 21.90 → 23.90 (+1.99 [+0.42, +3.57]).

Caveat: these TriviaQA and NarrativeQA cases are the ones whose study B rows
suggested D1, so they are not independent evidence. The confirmation is
study D's fresh rows (8.27: +3.35 / +1.92), which agree in size. The full
NarrativeQA test split (about 9,150 prose rows outside the sample) has no
wide rows yet.

### 8.30 D1 in Zig: crash fix and sub-batch default (2026-11-19, `budget-study/routed/zig/`, RESULTS "Follow-up")

Crash root cause (8.28, "Memorilla encoder cleanup failed"). It is three
faults in a chain:
1. The span pass ran out of its admitted device budget
   (`ResourceLimitExceeded`). `tokenFeatureRunRequestBounded` admits 8 F32
   row widths per position at the 1,024 bound, and long padded rows need
   20-22 live row widths.
2. On that error, `gpt.zig` `decoderBlockImpl` leaked the attention residual
   `attn_res`, which had no errdefer.
3. The request cleanup check then panicked on the leaked tensor, hiding the
   original error.

Fixes:
- `decoderBlockImpl` frees `attn_res`, through an ownership flag, and
  `result` on any FFN-sublayer error. This is shared code, but only its error
  paths change.
- `memorilla_encoder.runRequestAmounts`: runs with a run-time adapter (the
  hybrid passes) admit max(old amount, 32 row widths x batch x padded length)
  (`token_feature_peak_row_widths`). Only admitted bytes change; copy17 is
  untouched.
- New test: "dense decoder block releases its attention residual when the FFN
  fails". It fails with the fix reverted and passes with it.

Sub-batch default:
- Read sets of 3 or more documents run the span pass in length-sorted
  sub-batches when the grouping splits into more than one group.
  `ANTFLY_MEMORILLA_SPAN_SUBBATCH=0` turns it off. The research switch of
  8.28 is gone.
- R2 reads keep one padded batch and stay bit-identical. Sub-batching them
  would save only 3-19 ms per request and would change outputs (TriviaQA
  83/100 margins bit-equal; one NarrativeQA answer).

Re-verification (verifier b4):
- 344-case set (depot dev, depot fresh, SQuAD), research mode, policies
  always, learned and default bundle: bit-identical to the 8.22 binary on all
  1,032 runs. Depot 63/64 and 80/80 (always), 47/64 and 64/80 (learned and
  default). SQuAD EM / F1 59.50 / 71.57. No case in this set is sub-batched.
- D1 on the 250 study D rows:
  - With sub-batching off, all rows complete, including both former crash
    rows. Every row matches the earlier one-batch run bit for bit.
  - The new default is bit-identical to the measured D1 + sub-batches arm on
    TriviaQA and NarrativeQA, and to one-batch on FactKG (R2).
  - Against Python: read sets, routes and prompt tokens are equal on every
    row. Answers equal 94 / 47 / 50.
  - The 8.28 latency of D1 + sub-batches is therefore the default's:
    +188 ms p50 on TriviaQA, +144 ms on NarrativeQA, 0 on other routes.

Tests:
- `--test-filter emorilla`: 159 selected, 156 passed, 3 skipped.
- Shared filters (LoRA, gated, pinned, BF16, encoder, rms, hybrid, decoder,
  qwen3, gpt): no failures.
- Full suite: 6 failures, the same 6 as the 8.22 binary. The `span` filter's
  gliner Overflow test is among them.
- Python: 38 passed.

Open:
- The out-of-memory sweep found four small leaks (96-192 bytes) in
  native-backend ops; they are unrelated and not pursued.
- No HTTP field for the ranked candidate list yet.
- Zig's soft half reads at most 64 stored chunk vectors (Python reads all).
- Nothing is committed.

### 8.31 D1 on the full NarrativeQA test split (2026-11-19, `budget-study/nqa-full/`)

Question (study E, pre-registered in `budget-study/DESIGN-E.md`): does D1
raise NarrativeQA ROUGE-L over R2P1 on test rows that no read-budget, window,
fusion or head choice has used? Rows: 8.14's full test split minus the 8.13
threshold rows and the 1,000 paper-sample rows, giving 9,257 rows.

Setup:
- Routes come from the frozen 8.16 gate. 9,168 of the 9,257 rows go to a
  prose route: narrativeqa 8,406, triviaqa 762.
- Arms: base (R2P1) and wide (R512P2), both on every row, with the s31 head,
  batch 64 and the window always on. D1 is assembled per row.
- Pipeline 16:13-19:10Z. Both stages ran on attempt 1.

| view | base | D1 | D1 − base [95% CI] |
|---|---|---|---|
| **window always (Rule E: passes)** | 22.41 | 23.80 | **+1.39 [+0.87, +1.92]** |
| shipped gate (8.16 routed offsets, soft row when closed) | 22.04 | 23.32 | **+1.28 [+0.77, +1.79]** |

Context:
- The soft reader alone scores 19.17. D1 − soft is +4.63 window always and
  +4.15 under the shipped gate.
- Wide on every row adds +1.43 over base.
- Base reproduces 8.14's s31 window-always predictions on 9,257 of 9,257
  rows, so the base absolute score is 8.14's on these rows.

By question type (window always):
- "who" questions (2,211 rows) gain the most: +2.67 [+1.31, +4.01].
- "what" gains +1.34 [+0.56, +2.12] and "other" +2.87 [+0.73, +5.02].
- "how" leans negative (−1.02 [−2.34, +0.31]). "why", "where" and "when"
  are flat.

The base arm hits the 2048 encoder bound on 34 rows and wide on 22. Those
rows get the soft row. Dropping all 34 leaves +1.38 [+0.86, +1.90].

Reading:
- D1's NarrativeQA gain holds on the largest independent test set:
  - paper sample: +1.99 (it suggested D1);
  - study D: +1.92;
  - here: +1.39.
- The estimate is smaller here, but all intervals overlap.
- The cost is unchanged from 8.27/8.28: about 15.5 chunks and 462 tokens
  read, 80 memory rows, record preparation 6.3× in the Python batch-64 path,
  and +144 ms p50 at batch 1 in Zig.

Decision: unchanged, adopt D1.

Still open before shipping (8.30):
- the HTTP field for the ranked candidate list;
- Zig's 64-vector cap on the soft half.

Nothing is committed.

### 8.32 Confidence cascade replaces benchmark routing for the read (2026-11-19, `budget-study/cascade/`)

Why: D1 picks the read budget with the 8.16 nearest-centroid route, which is a
classifier over the eight paper benchmarks. Every question is forced into one
of them, and 8.17 showed this fails on unseen task types. The route mostly
stands in for one question: did the narrow read find a confident span? The
span margin answers that directly.

Study F (pre-registered in `budget-study/DESIGN-F.md`) tested a cascade:
1. Narrow read R2P1 on every question.
2. If its margin is below t, or the record errored, read wide (R512P2) and
   use the wide record.
3. Gate: the plain margin gate (s31 −3), with no benchmark labels anywhere.

F1, selection (CPU, existing rows with both reads):
- Rows: study D + study B dev, i.e. TriviaQA 2,000, NarrativeQA 3,461,
  FactKG 4,000.
- 28 policies were tried. C* = `replace@-4` (J +1.03; the best feasible J
  was +1.06, and the tie-break picks the lowest widened share).

| benchmark | C* − narrow [95% CI] | widened |
|---|---|---|
| TriviaQA | +1.45 [+0.60, +2.30] | 19% |
| NarrativeQA | +1.63 [+1.18, +2.08] | 82% |
| FactKG | +0.00 [−0.30, +0.30] | 23% |

Thresholds from −1 up lose FactKG significantly. Reading wide on every row
gives TriviaQA +3.20 but FactKG −1.47.

F2, held-out check (study E, NarrativeQA, 9,257 rows): **+1.45 [+1.19, +1.72],
passes.**
- With the window always on, C* is level with D1: +0.19 [−0.05, +0.43].
- It widens 82% of these rows, so on NarrativeQA it costs about what D1 does.

F3, generalization (GPU, 20:18–22:44Z, all generation on attempt 1):
- Rows: the 8.17 unseen types (600 each) and PV4/PMv2 (study D rows). Wide
  ran only on the rows C* widens.
- The new narrow rows reproduce 8.17's s31 predictions on 600/600 rows per
  type.
- **No type loses significantly: passes.**

| type | C* − narrow, margin gate [95% CI] | widened |
|---|---|---|
| SQuAD v2 | 0 | 0% |
| DROP | 0.00 | 2% |
| CoQA | 0.00 | 3% |
| PwC | **+0.71 [+0.24, +1.26]** | 17% |
| MS MARCO | +0.38 [−0.00, +0.87] | 42% |
| SAMSum | +0.03 | 65% |
| QuAIL | −0.12 [−0.82, +0.59] | 66% |
| CNN/DM | −0.15 [−0.35, +0.05] | 28% |
| PV4 | +0.23 [−0.06, +0.57] | 65% |
| PMv2 | 0.00 | 96% |

With the window always on, QuAIL loses 2.85 [−5.19, −0.53]. The margin gate
is what keeps the cascade safe, which repeats the 8.17 finding.

Decision (per the design): C* becomes the default read policy (`auto`) and
replaces the centroid read routing. A per-request read mode (narrow, wide or
auto) lets callers force the wide read. Zig: `budget-study/cascade/zig/`
(in progress).

Open:
- **Wasted wide passes.** On PMv2, PV4, SAMSum and QuAIL, 65–96% of rows
  widen, but the wide record clears the gate on only 0–5% of them. PMv2
  tokens per row go 269 → 658 for nothing. Rows with narrow margin below −8
  almost never gain: 9 of 1,583 open the gate. A margin floor on widening
  needs its own study.
- **The gate.** Under the margin gate, NarrativeQA scores 22.29 with C*.
  D1 under the benchmark-routed gate scored 23.32, because that gate always
  opens the window on NarrativeQA-like questions. Closing that gap without a
  benchmark classifier is the next label-free question.

Nothing is committed.

### 8.33 Cascade floor and wide-record gate: both rejected (2026-11-19, `budget-study/cascade/g/`, `h/`)

Two label-free refinements of C*, both CPU studies on existing rows:
- **G** (`DESIGN-G.md`): a floor on widening plus a separate gate offset for
  wide records, chosen jointly on S.
- **H** (`DESIGN-H.md`, registered after G): the floor alone.

G chose f −12, g_w −8.
- **Study E: passes.** +1.03 [+0.68, +1.39] over C*, taking 22.29 to 23.32.
  That equals D1 under the benchmark-routed gate, with no labels.
- **F3 rows: fails.**
  - Losses: QuAIL **−8.73** [−11.48, −6.04] and PMv2 **−4.27** [−5.83,
    −2.72].
  - Gains: SAMSum +6.14, MS MARCO +3.13, PwC +0.46.
- Not adopted. On QuAIL and PMv2, an opened wide window misleads the decoder
  at the same margins where it helps NarrativeQA. S (TriviaQA, NarrativeQA,
  FactKG) could not see that.

H chose f −12.
- **F3 rows: passes.** No difference from C*.
- **Study E: fails.** −0.10 [−0.16, −0.04]. NarrativeQA rows with very low
  narrow margins still gain from widening.
- It saves little: PMv2 widened 96% → 92%, E tokens 512 → 488. Not adopted.

Decision: C* stays (`replace@-4`, every record gated at −3). The bundle keeps
`floor` and `wide_gate_offset` fields at their C* defaults.

Lessons for a general system:
- Margin-only knobs are exhausted. Whether a wide window helps depends on
  the kind of task, not on the margin.
- The cascade's cost has to come down on the engineering side: reuse the
  narrow pass's work, and a cheaper span pass.
- Future label-free knobs should be selected on a diverse task mix with
  leave-types-out checks. Richer gate signals are worth trying: m_w − m_n,
  agreement between the narrow and wide spans, answer form. Three benchmarks
  are too narrow a selection set.

### 8.34 Cascade in Zig: read mode, D1 routing removed, batch-1 latency (2026-11-20, `budget-study/cascade/zig/`)

Built (uncommitted; full write-up in `cascade/zig/RESULTS.md`):
- Bundle data `span_read_cascade` (`threshold`, `read_tokens`,
  `window_tokens`; optional `floor` and `wide_gate_offset`, unused, defaults
  = C*). New bundle `qwen-span-copy/span1-35k-cascade/` = span1-35k-declared
  plus `{-4, 512, 64}`; no existing bundle changed.
- Per-request read mode `narrow | wide | auto` (`Options.span_read_mode`),
  `auto` the default on a cascade bundle. No HTTP field yet: it would go in
  `MemorillaGenerateRequest` (`specs/openapi/inference/config.yaml`,
  `additionalProperties: false`) and `server/memorilla_request.zig`.
- D1 centroid read routing deleted from Zig (the D1 bundle is refused).
  Centroids remain only in the gate (`span_gate_policy: "routed"` or an
  internal profile option); both default bundles use `learned`.
- Wide pass reuses the narrow pass's features for the shared leading
  documents (default on, `ANTFLY_MEMORILLA_SPAN_CASCADE_REUSE=0` to switch
  off). Admission is sized for the wide read and narrowed after the decision.

Verification:
- Bundles without a cascade unchanged: 250 study D rows and 1,032/1,032 on
  the 344-case set. `narrow` = today's R2 rows (250/250); `wide` = the 8.30
  D1 wide rows (200/200); `auto` with reuse off = `narrow` on kept rows and
  `wide` on widened rows (250/250). With reuse, every decision equals `wide`
  mode; margins differ in the last bits on 19 of 106 widened rows (max 0.040).
- `auto` vs the Python cascade (250 rows): 9 rows flip sides of −4, all with
  Python margins within 0.6 of it. Answers equal on same-record rows 93/97
  (TriviaQA), 43/95 (NarrativeQA, free form), 49/49 (FactKG).
- Unit tests: emorilla 161 passed / 3 skipped; full suite with the same 6
  known failures; Python 42 passed.

Latency, batch 1, research mode, modes interleaved per case in one process,
two repetitions (lat2a / lat2b; the per-mode-process run lat1 agrees):

| set (rows, widened) | auto − narrow p50 | auto − narrow p90 | D1 − narrow p50 |
|---|---|---|---|
| TriviaQA (100, 24%) | +9 / +12 ms (+3%) | +146 ms (+28%) | +208 / +202 ms |
| NarrativeQA (100, 75%) | +183 / +178 ms (+32%) | +274 / +282 ms (+28%) | +151 / +155 ms |
| FactKG (100, 12%) | +2 / +3 ms (+1%) | +194 / +190 ms (+70%) | – |
| PV4 (100, 68%) | +99 / +113 ms (+3%) | +258 / +268 ms (+7%) | – |
| PMv2 (99, 96%) | +112 / +128 ms (+3%) | +164 / +249 ms (+3–5%) | – |
| 8 unseen types pooled (396, 28%) | +34 / +28 ms (+6%) | +112 / +107 ms (+6%) | – |

- Kept-narrow rows cost −3 to +4 ms; widened rows +152 to +437 ms at p50
  (second encoder pass, a 4–8 ms head, and an 80-row prompt).
- Reuse saves per widened row (median): 21 ms TriviaQA, 52 NarrativeQA,
  31 FactKG, 267 PV4, 130 PMv2, 131 the unseen types pooled.
- PV4/PMv2 ran at the service's 128 new tokens (Python uses 256).

Decision: the cascade (`auto`) is the default read for the span hybrid in
Zig. Open: the HTTP `read_mode` field; staged admission; a cheaper narrow
pass (it would change today's narrow numerics).

### 8.35 HTTP read mode, span diagnostics and a local playground (2026-11-20)

Built (uncommitted):
- **Request** (`MemorillaGenerateRequest`, hybrid_research only; other kinds
  get 400):
  - `read_mode`: `auto | narrow | wide | none`.
    - `auto`, `narrow` and `wide` map to `Options.span_read_mode` and need a
      bundle with `span_read_cascade` (400 `INVALID_REQUEST` otherwise).
    - `none` (`Options.span_soft_only`) needs a span bundle. It skips the span
      pass; the decoder reads the 16 soft rows alone under the released
      helpful prompt, i.e. stock Memorilla over the same pooled vectors.
  - `include_diagnostics` (bool, default false).
  - Absent fields behave exactly as before. On a live server, absent,
    explicit `auto` and `include_diagnostics: false` return byte-identical
    bodies, and the diagnostics body minus `diagnostics` equals them.
- **Response**: a `diagnostics` object, last and only when asked
  (`MemorillaDiagnostics`). Fields that do not apply are absent, never null.
  - Read: `read_mode` (`all` for a span bundle without a cascade),
    `read_pass`, `widened`, `cascade_threshold`, `narrow_margin`,
    `wide_margin`, `narrow_unavailable`/`wide_unavailable`, `read_documents`,
    `read_tokens`, `read_budget_tokens`, `window_rows`.
  - Gate: `score`, `null_score`, `margin`, `gate_policy`, `gate_offset`
    (absent for `always`), `gate_profile`, `gate_open`.
  - `span` and `window.regions`: document index and UTF-8 byte range, mapped
    with `copy.buildCitations` like citations. They are reported whether or
    not the gate opened. `window.text` is the window as the decoder reads it.
  - `raw_answer`: before canonicalization and the guard.
  - `timings_ms`: the existing `memorilla_span_timing` stages, now kept in
    `SpanTimings`. They are `pooled`, `span_features`, `span_head`,
    `wide_features`/`wide_head` when a wide pass followed, `window`,
    `soft_rows`, `generate` and `total` (the service call).
- Spec: `memory_tokens` gained 48 and 80, which span bundles already
  returned.
- `none` reports the bundle's pinned span-head, candidate and adapter digests
  (`bundlePins`), because nothing of the record half runs.
- Generation:
  - `make generate` stops at `make tidy`: no `go` on this host.
  - Its earlier steps ran: `openapi.yaml`, the Rust spec, the Zig generated
    types (`antfly-client`, `antfly-embedded`, `host/engine_errors.zig`, which
    also picked up error names from earlier uncommitted work), and the Python
    SDK.
  - Go and TypeScript SDK regeneration remain open (they need `go` and
    Node 24).
- **Playground** (`zig/pkg/inference/scripts/memorilla/playground/`):
  - `run.sh start|stop|status` starts `antfly-inference-dev` (research mode as
    in `cascade/zig/run_arm.sh`) with one profile, span1-35k-cascade over
    stage3, plus the page server. Both bind to 127.0.0.1; state, config and
    logs are in `.benchmark-results/memorilla/playground/`.
  - Splitting: each chunk becomes sentence-greedy passages of at most 64
    encoder tokens (no specials; the rule of the Zig `chunkDocument`).
  - Ranking: dense cosine between stock Qwen3-Embedding-4B vectors from the
    server's `/ai/v1/embed`. This is the upstream retrieval behind the study
    sets' `top_documents` (`qwen_hybrid_eval.retrieval_order`).
  - Request: `document_texts` is every passage in ranked order, with the
    chosen mode and diagnostics. With "compare" on, a second call uses
    `read_mode: none`.

Verification:
- Unit tests:
  - New: request fields (wire strictness, kinds); HTTP body unchanged
    byte-for-byte without diagnostics; the diagnostics block (widened, kept,
    no cascade, `none`); failure mapping; `SpanTimings`; span/window regions;
    `bundlePins`.
  - Filters: emorilla 169 / 166 / 3 skipped; hybrid 23 / 23; cascade 6 / 6;
    span 140 / 120 / 19 skipped / 1 failed (gliner `Overflow`, known).
  - Full suite 7,381 / 6,369 / 1,006 / 6 failed: the same 6 known failures as
    8.34, +5 tests.
  - Playground CPU tests: 7 passed.
- End to end through the playground API: 17 questions × `auto`, `narrow`,
  `wide`, each with the `none` comparison (102 generations).
  - Every closed-gate hybrid answer (12) equals `none` in text and token ids.
  - `auto` equals `narrow` on kept rows and `wide` on widened rows: answer,
    span and gate 17/17. Margins are bit-equal on the 5 widened rows.
  - Exact recall, 8 synthetic site-record questions:

    | read | correct | partial |
    |---|---|---|
    | `auto` | 6 | +1 ("Corvane Utilities" for "... plc") |
    | `narrow` | 5 | +1 |
    | `wide` | 4 | – |
    | stock (`none`) | 0 | – |

    - Stock answers "1234", "KAYE", "Balfour Beatty".
    - Widening fixed the Bellmoor gate code: narrow margin −6.04 → wide
      +5.34.
  - Latency, warm, batch 1, research mode, server total p50: `auto` 684 ms,
    `narrow` 678, `wide` 852, stock 778.
    - Pooling the 36 passages takes ~383 ms of every request.
    - Short exact answers decode faster in the hybrid (span drafts) than in
      stock.
- Verifier spot-check: 14 study rows through HTTP `read_mode: auto` with the
  verifier's ranked documents (6 TriviaQA, 4 NarrativeQA, 4 FactKG; 7 of them
  widened), compared with `cascade/zig/runs/parity2/auto-*`.
  - Bit-equal on 14/14: read pass, narrow and wide margins, score,
    null_score, read documents and tokens, window rows, window text, and
    window byte ranges against the verifier's citations.
  - Answers equal 9/14 (10 ignoring case). The inputs cannot be identical:
    the verifier ran `--span-gate always`, the benchmark prompts and stored
    Python chunk vectors (64 per collection) for the soft half. HTTP uses the
    bundle's learned gate (−3), the service prompts and pools the 50 ranked
    texts itself.
- Answer guard: no general answer was rejected (0 of 102). The longest was a
  41-token sentence with no newline.

Findings:
- 64-token passages lose their subject. A site record's key-holder sentence
  no longer names the site, so "key holder for Bellmoor Cutting" ranks
  another site's key-holder passage #2. The narrow read answers
  "Anouk Fennimore" (from another site's record) at margin +5.1, and every
  mode is wrong.
  - A per-passage context prefix (chunk title or first sentence) is the
    obvious fix. It is not built.
- `wide` alone is worse than `auto` on exact recall (4 vs 6 of 8). With up to
  9 passages read, the head picks a span in another site's record
  (Lindqvist and Rowe for Tamsin Reach; Bellmoor's gate code for the Ordway
  alarm code, answered "123456"; Anouk Fennimore for "who signs off
  deliveries", answered UNKNOWN), all at open gates. This repeats 8.33's
  lesson that wide windows mislead.
- Every request pools all passages again; a stored-vector input for the
  hybrid HTTP route would save ~380 ms per question in the playground.
