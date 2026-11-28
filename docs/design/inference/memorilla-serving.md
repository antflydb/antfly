# Memorilla supplied-feature inference

The first serving surface is `POST /ai/v1/memorilla/generate`. It accepts a
question and stored document features, then runs the native question encoder,
released Memorilla reader and Qwen3 decoder. Explicit `source_verified` mode
also checks answers against supplied document text and uses a text-grounded
fallback or abstention when needed. The route is disabled by default.
The database gateway applies its existing public-inference policy and
`inference:*` permission before forwarding to the supervised inference worker.
A standalone listener should remain on loopback or behind an authenticated proxy.

This surface accepts caller-supplied vectors. Its encoder identity establishes
numerical compatibility. It does not establish their source, authorization,
freshness, or complete coverage of a database collection. Collection selection
and database policy enforcement are a separate integration milestone.

## Qualified execution contract

- CUDA compute capability 12.1, with the qualified F32 kernels and F32 KV.
- Frozen Qwen3-Embedding-4B encoder, 2560 dimensions, last-token pooling,
  L2 normalization, no instruction prefix and trailing token 151643.
- Frozen Qwen3-8B decoder and its pinned chat template, with thinking disabled.
- One of the five released memory-module bundles: stage1, stage2, stage3,
  pv4 or pmv2. Sixteen memory rows replace virtual memory slots inside the
  native decoder; the tokenizer vocabulary and base weights are unchanged.
- One through 64 document vectors and an encoder question of at most 256 tokens.
  The raw question is bounded to 8192 UTF-8 bytes. Python-compatible Unicode
  whitespace is stripped before encoding and prompting. Empty questions and
  literal `<|memory|>` markers are rejected.
- Buffered greedy decoding, EOS stopping and at most 128 new tokens. Every
  invocation owns fresh KV. Unsupported fields, including sampling, streaming,
  speculative decoding and caller-selected asset paths, are rejected.

Mixed/BF16 activation arithmetic, F16 KV, quantized weights and other devices
need separate numerical qualification. The recorded F16-KV gate failed; it
must not inherit the F32 profile's status. Numerical agreement does not establish
answer quality on a production corpus; evaluate that workload before rollout.

The [qualification summary](memorilla-qualification.md) records the numerical,
HTTP/lifecycle, answer-quality and performance checks. Exact unseen names and
codes remain an unmet latent-only quality gate. The source-verified assessment
supports its bounded workload; it does not satisfy that latent-only gate.
Warm native latency is near parity with the Python controls, with no general
speedup or bitwise native/vLLM generation claim.

## Install and configure

Install the immutable managed base-model variants using the normal model pull
command with these references:

```text
hf:Qwen/Qwen3-Embedding-4B:memorilla-f32-safetensors-bundle-v1
hf:Qwen/Qwen3-8B:memorilla-f32-safetensors-bundle-v1
```

The registry verifies all source artifacts and publishes managed receipts.
Ordinary Qwen variants and hand-edited manifests do not qualify. The qualified
decoder variant is reserved for this composition API; ordinary generation and
checkpoint routes reject it. Other Qwen variants retain their existing routes. Export a
released reader bundle with the [checkpoint tools](../../../zig/pkg/inference/scripts/memorilla/README.md).
The bundle's converted weights and source hashes are checked independently of
its manifest. Keep these administrator-controlled assets immutable while serving.

Configure a named composition in the inference process:

```json
{
  "resources": {
    "memory": {"max_bytes": 120259084288},
    "inference": {
      "memory": {
        "max_host_bytes": 34359738368,
        "max_device_bytes": 68719476736,
        "max_combined_bytes": 103079215104,
        "max_kv_bytes": 4294967296,
        "max_scratch_bytes": 8589934592
      },
      "admission": {"max_concurrent_requests": 2, "max_work_units": 4},
      "request_limits": {
        "max_items_per_request": 64,
        "max_input_tokens_per_item": 1024,
        "max_output_tokens_per_item": 128
      }
    }
  },
  "inference": {
    "models_dir": "/srv/antfly/models",
    "memorilla": {
      "enabled": true,
      "profiles": [{
        "name": "memorilla-stage3",
        "encoder": "hf:Qwen/Qwen3-Embedding-4B:memorilla-f32-safetensors-bundle-v1",
        "decoder": "hf:Qwen/Qwen3-8B:memorilla-f32-safetensors-bundle-v1",
        "memory_module_path": "/srv/antfly/memorilla/stage3",
        "max_documents": 64,
        "max_new_tokens": 128
      }]
    }
  }
}
```

These are example explicit ceilings for a large-memory host, not a promise that
every workload fits. The F32 encoder and decoder weights alone occupy
48,850,038,784 bytes. Source mappings, the reader, cold conversion, tokenizer
assets, CUDA workspace, activations and KV are charged in addition. Request
admission cannot widen operator ceilings. Cold-load admission includes its
conversion peak; warm model residency remains charged. The fixed CUDA kernel
workspace remains owned by each loaded model after request scratch is reclaimed.

Operator request ceilings also narrow the document count, generated-token
count, native question-token count and fully framed decoder input. The input
token ceiling counts the complete rendered input, including each verifier or
source-fallback prompt. The internal limit is 1024 tokens per decoder call;
source-verified requests never silently truncate documents to fit.

Composed requests queue before acquiring model handles or request resource
leases. The ordinary request-count gate bounds that queue. Normal model
execution lanes still serialize their own mutable backend state. Cancellation
and deadlines flow through queueing, model load, encoding, reading and decoding;
physical request allocations retire before their admission credit is returned.
Each loaded decoder retains at most one reader and its complete resource lease.
Cold acquisition performs the full bundle hash and released-checkpoint checks.
Warm requests check the canonical bundle paths and file generations before and
after use; detected changes reject the request with retryable
`MODEL_ARTIFACTS_CHANGING` and retire the reader after request work drains.
Switching reader profiles replaces the retained reader. Decoder eviction frees
the reader before refunding its resource lease, and an active decoder handle
prevents eviction during use. Request tensors and KV are reclaimed separately;
document projections are recomputed for each question.

Warm preflight can borrow the loaded model's tokenizer, template and manifest
through a model handle after verifying the current artifact generation. Cold
preflight retains independent metadata validation and admission, and rejects
over-limit inputs before loading GPU models. These borrows pin model lifetime;
they do not retain request KV or change operator ceilings.

## Produce and submit compatible features

Use the qualified encoder variant through the embedding API, with its full
2560-dimensional normalized output and the pinned no-prefix recipe. Preserve
its returned `model_identity` with the stored features. The expected execution
identity for this profile is:

```text
e3d6de85a3204759c8d575bfe109aba3ec1a49318ae01a88c71515415e0ce91a
```

Do not reuse features from an ordinary retrieval-instruction or mixed-precision
encoder profile. The service checks every vector's width, finite components and
unit norm (tolerance 0.01 for quantized storage drift), then applies the pinned
FP16-storage to BF16-readout conversion. It does not renormalize supplied rows.

Write the following JSON to `request.json`, replacing `document_embeddings` with
one or more actual 2560-component feature arrays for the exact supplied texts;
the placeholder is illustrative:

```json
{
  "model": "memorilla-stage3",
  "question": "Who owns the launch?",
  "document_embeddings": ["REPLACE_WITH_A_2560_COMPONENT_ARRAY"],
  "context_mode": "source_verified",
  "document_texts": ["The launch owner is Alice Chen."],
  "encoder_execution_identity": "e3d6de85a3204759c8d575bfe109aba3ec1a49318ae01a88c71515415e0ce91a",
  "max_new_tokens": 64
}
```

```sh
curl --fail-with-body http://127.0.0.1:4200/ai/v1/memorilla/generate \
  -H 'Content-Type: application/json' \
  -H 'X-Antfly-Inference-Timeout-Ms: 300000' \
  --data-binary @request.json
```

The response identifies the encoder, decoder and reader weights. In
`source_verified` mode, it also includes ordered SHA256 hashes of the original
UTF-8 source texts and `context_source: supplied_embeddings_and_documents`.
Text and vector association is caller-supplied; hashes identify text bytes
without certifying their truth, authorization or correspondence to vectors.

## Source verification and fallback

Set `context_mode: source_verified` and provide one document text per feature
row in the same order. Texts are bounded to 8192 UTF-8 bytes each and 32768 bytes
total, and the complete prompts must fit the operator and internal token limits.
The server independently answers from document text without showing that call
the memory candidate. It then checks the source answer using the same qualified
decoder with fresh F32 KV. Before that model check, the answer must have an exact,
token-bounded occurrence in the original sources. Case, leading zeros, spelling
and punctuation are preserved; partial identifiers do not provide evidence.
This conservative path answers literal factual values. Derived or paraphrased
answers without a matching source value produce `UNKNOWN`. Semantic relevance,
complete names, corrections and conflicts still require the model check; a
literal occurrence alone does not prove that the value answers the question.
An exact completed `SUPPORTED` verdict is required.
If the initial source answer receives a completed `UNSUPPORTED` verdict, the
server permits one repair using only the original question and documents. Neither
the memory proposal nor the rejected source answer is supplied to the repair.
A changed repair must pass literal evidence and a fresh verifier call before publication.
Malformed/truncated verdicts, missing literal evidence, repeated rejected values
and unsuccessful repairs abstain. Cancellation, model errors and artifact drift
still propagate as errors. Every phase keeps the caller/operator token limits;
`usage.verification_calls` is bounded to two and all repair work is included in
total prompt/completion usage. A repair adds latency only on the rejected path.
The memory candidate can be published only when it agrees with that supported
source answer after trimming boundary Unicode whitespace; case, punctuation,
digits and internal spacing must match exactly. Otherwise the supported source
answer is published. An incomplete, truncated or unsupported source answer
produces the policy abstention `UNKNOWN`.

The response reports the actual path:

| `answer_path` | Published answer | `memory_tokens` |
| --- | --- | ---: |
| `memory_verified` | Memory candidate agrees with the independently generated, supported source answer | 16 |
| `source_fallback_verified` | Source-text fallback passed the source check | 0 |
| `abstained` | Policy-produced `UNKNOWN` | 0 |

`verification_status: model_supported` describes a model entailment judgment,
not a formal proof or a guarantee that the supplied source facts are true.
Source hashes are provenance for all inputs, not selected supporting citations.
The policy abstention has `finish_reason: abstained`, empty `token_ids`, and zero
final completion tokens. Underlying model work is still counted in
`usage.total_prompt_tokens`, `usage.total_completion_tokens`,
`usage.verification_calls`. `usage.source_fallback_used` reports the final
source-answer or abstention selection; it is false for `memory_verified`, even
though independent source-answer work was performed and counted in total usage.

The source policy preserves factual claims alongside response commands in the
same document. A demand about what the assistant should answer cannot revoke a
dated factual correction. The decoder still makes a model judgment: independent
critic controls exposed false approvals, so an isolated support label cannot
authorize the memory candidate.

At most one memory candidate, one independent source answer, one source repair
and two verification calls run. Answer candidates each obey `max_new_tokens`;
each verifier call is capped at eight tokens and narrowed by profile and operator
ceilings. All calls share
the request deadline. Cancellation, identity failures and resource errors
propagate; they do not become an apparently successful fallback.

The request captures both base models' managed publication receipts and complete
artifact generations before memory execution, then checks them again after all
phases retire. Changes to required files, manifests or optional sidecar inventory
prevent publication, even when the receipt bytes remain unchanged. Cold model
construction still requires full cryptographic artifact validation.

Omitting `context_mode` preserves the earlier `latent` behavior over embeddings
alone. It publishes sixteen memory tokens, `context_source: supplied_embeddings`
and the ordinary `eos` or `length` result without source checks. This mode remains
an experimental latent-memory path; the initial factual diagnostic scored 0/22.
Supplying source texts without explicitly selecting `source_verified` is rejected.

Disabled or unknown profiles return 404, incompatible feature identities return
409, malformed or unsupported options return 400, and input bounds return 413.
Transient shared-capacity rejection returns 503 with `Retry-After`; permanent
operator-budget rejection is not retryable. Deadline and cancellation behavior
uses the existing inference error contract. Deployment must retain supervised
worker execution for native CUDA calls.

## Research profile (encoder-LoRA candidate)

A profile with `reader_kind: encoder_lora_research` serves the experimental
encoder-LoRA copy candidate described in
[the copy17 port plan](../../plans/memorilla-copy17-native.md). It is a second,
explicitly configured profile kind. The released path above keeps every pin
unchanged; the research path fails closed on its own artifact pins and nothing
about it is qualified. Do not enable it on a shared deployment.

The composition differs from a released profile in what the two paths point at:
`encoder` is the merged research encoder directory exported by
`encoder_lora_export.py merge-encoder` (its `model_manifest.json` carries the
`qwen3_encoder_lora_research_f32_v1` profile and `memorilla_lock.json` pins
every shard and sidecar), and `memory_module_path` is the research reader
bundle from `export-reader` (`manifest.json` with `release: false`, the reader
weights, the copy head, `candidate.json` and `protocol.json`). The decoder is
the same managed qualified Qwen3-8B variant. At load, the encoder lock's
`candidate_sha256`, the bundle's `source.candidate_sha256` and the hash of the
bundle's `candidate.json` must agree.

```json
{
  "inference": {
    "memorilla": {
      "enabled": true,
      "profiles": [{
        "name": "memorilla-copy17",
        "reader_kind": "encoder_lora_research",
        "encoder": "/srv/antfly/memorilla/lora-merged-encoder",
        "decoder": "hf:Qwen/Qwen3-8B:memorilla-f32-safetensors-bundle-v1",
        "memory_module_path": "/srv/antfly/memorilla/lora-reader-bundle",
        "max_documents": 64,
        "max_new_tokens": 64
      }]
    }
  }
}
```

Requests to a research profile supply document texts, not embeddings. The
server encodes each text with the merged encoder (per-token hidden states,
not a pooled vector), builds the token features, runs the research reader and
copy head, and decodes with the qualified decoder. `document_embeddings` is
rejected with 400, and `context_mode: source_verified` is rejected with 409
because the verified path has not been built for this reader. One to 64 texts
are accepted under the same per-text (8192 bytes) and total (32768 bytes)
limits as source-verified mode; a count above 64 returns 413.
`encoder_execution_identity` must equal the fixed research encoder identity
(`executionIdentity(qwen3_encoder_lora_research_f32_v1)`), which differs from
the released encoder identity; a released identity returns 409.

```json
{
  "model": "memorilla-copy17",
  "question": "Who owns the badge?",
  "document_texts": [
    "Badge owner: Alice Chen.",
    "Launch window opens Tuesday."
  ],
  "encoder_execution_identity": "REPLACE_WITH_THE_RESEARCH_ENCODER_IDENTITY",
  "max_new_tokens": 24
}
```

The response has the latent shape with two differences: `context_source` is
`supplied_documents`, and `candidate_sha256` names the candidate pinned by both
the encoder lock and the reader bundle. `memory_tokens` is 16,
`input_documents` is the text count, and `usage.encoder_query_tokens` reports
the question's encoder tokens. `encoder_execution_identity`,
`decoder_execution_identity` and `memory_module_sha256` are reported as for a
released profile; `candidate_sha256` identifies the experiment, not a
qualification status.

The research path also applies an answer-format guard to the decoder text,
reported in the optional `answer_guard` field (`accepted` or `rejected`). The
research task answers are short exact facts: a name of one to a few words, a
short code, or the literal `UNKNOWN`. The frozen decoder has no format
constraint of its own and, when the copied record misses the fact, can echo its
memory frame as prose. The guard trims the text and accepts the literal
`UNKNOWN`; otherwise it rejects text that is empty, longer than 64 bytes, more
than six whitespace-separated words, contains any control character (including
a newline), contains the frame literal `Memory` as a standalone word or the
`<|memory|>` marker, or contains bytes outside letters, digits, space, `-`,
`.` and `'`. A rejected answer is published as `text: "UNKNOWN"` with
`finish_reason: abstained`; `token_ids` and `usage.completion_tokens` keep what
the decoder produced so the rejected output remains inspectable. The rules are
general format rules, not corpus patterns, and they are serving policy for the
research profile only; released profiles are unchanged.

A research response also says where every span copied into the memory record
came from. `citations` lists one entry per contiguous cited region of a
document: `document_index` into the request's `document_texts`, a byte range
`[start, end)` into that text, and `text`, which is exactly those bytes. For
the request above, an answer of `Alice Chen` copied from the first text would
come back as:

```json
"citations": [
  {"document_index": 0, "start": 13, "end": 23, "text": "Alice Chen",
   "supports_answer": true, "match": "exact"}
],
"citations_unavailable": false
```

`start` and `end` are UTF-8 byte offsets, not character or UTF-16 offsets. A
JavaScript client must slice the encoded bytes (for example from
`TextEncoder`), not the string; in Python, slice `text.encode()` and decode the
result. Offsets always fall on character boundaries.

How the server finds the ranges: when it builds the copy sources it records,
for every source token, the document it came from and the bytes its token
piece covers. The Qwen3 tokenizer is byte-level BPE, so a document's pieces
concatenate to exactly the text that was encoded: the caller's text after the
one leading space the protocol adds (none if the text already starts with a
space), plus one more space when the self-context fallback applied. The server
checks that byte for byte and subtracts the added spaces; a piece that covers only an added space gets an empty range at the
start of the document. Citations are built from the spans after word
completion, so they cover the whole words the record received, not just the
raw copied run. A copied run that crosses from one document's tokens into the
next is split per document. Regions of one document that touch or overlap
are merged, widened to whole UTF-8 characters and trimmed of surrounding
whitespace. Entries are sorted by `document_index`, then `start`.

`supports_answer` is true when the published answer, after the answer guard,
occurs in `text`. `match` says how: `exact`, `case_insensitive` (ASCII case
only), or `none`. It is always false when the answer is `UNKNOWN`, including
a guard abstention. A citation shows what the record copied, not that the
answer is right: the decoder can still answer from another slot, or abstain
even when the fact was copied.

The tokenizer applies NFC normalization. If a text is not already in NFC (a
decomposed accent, for example), or anything else makes the pieces differ
from the supplied bytes, the server cannot map tokens to offsets. It then
returns `citations: []` and `citations_unavailable: true` and leaves the answer
unchanged; the request does not fail. `citations` is also empty when nothing
was copied. Released profiles never return either field.

Caveats:

- Experimental and unqualified. The candidate reached 64/64 on its dev
  fixtures and 78/80 on fresh fixtures in the offline Zig tool, with two
  divergences on records already known to sit at the decoder's acceptance
  boundary. That is a research result, not a production QA gate.
- Strict F32 only. Mean offline per-case timings on GB10 were roughly 620 ms
  for the encoder (three rows of about 140 tokens through the generic CUDA
  Qwen3 path), 19 ms reader, 9 ms copy head (host), 26 ms snap, and 1.3 to 1.8 s
  for the decoder (about 100 prompt tokens, up to 24 new tokens, F32 KV). The
  Python BF16 harness decoded the same answers in about 0.65 s; a BF16 serving
  profile would need its own qualification.
- Texts are caller-supplied inputs. Encoding them server-side removes the
  text/vector pairing problem but establishes nothing about their truth,
  authorization or coverage.
- `source_verified` is not available; a request that asks for it returns 409
  `INCOMPATIBLE_MEMORILLA_PROFILE` rather than silently falling back to the
  latent answer.

## Hybrid research profile (soft memory plus exact record)

A profile with `reader_kind: hybrid_research` answers general questions over
caller texts by composing two readers, as section 8 of
[the copy17 port plan](../../plans/memorilla-copy17-native.md) describes: the
released reader's 16 soft rows (general recall, pooled vectors) and the copy17
candidate's 16-row exact record (names and codes copied token for token). It
is the production composition the Python evaluator
`qwen_hybrid_eval.py --record-gate --answer-canonicalize` measured (merged
prompt, soft rows first, full record rows, record gate at cosine 0.85, answer
canonicalization). Like the research profile it is experimental, unqualified
and fails closed on its own pins.

The hybrid keeps one resident encoder. `encoder` is the stock managed
`qwen3_encoder_f32_v1` Qwen3-Embedding-4B, the same one a released profile
uses; it serves both passes. The pooled (soft) pass runs it unchanged. The
per-token (record) pass applies the candidate's encoder LoRA to the q and v
projections of every layer at run time, `y = W x + (alpha / rank) * B (A x)`,
added to the projection output before Qwen3's q/k head norms and RoPE. The
adapters are request inputs uploaded with the run and freed after it; the
resident weights and their 398-weight, byte-exact residency checks are never
touched. `memory_module_path` is the hybrid bundle that
`encoder_lora_export.py export-reader --include-encoder-lora` writes: the
research bundle plus a pinned `encoder-lora.safetensors` (rank-8 F32 `A`/`B`
pairs for `q_proj` and `v_proj` of all 36 layers). The new field
`released_memory_module_path` names the released reader bundle that produces
the soft rows (stage3; it is required for this kind and rejected for every
other kind). The merged research encoder is not used.

```json
{
  "inference": {
    "memorilla": {
      "enabled": true,
      "profiles": [{
        "name": "memorilla-hybrid",
        "reader_kind": "hybrid_research",
        "encoder": "hf:Qwen/Qwen3-Embedding-4B:memorilla-f32-safetensors-bundle-v1",
        "decoder": "hf:Qwen/Qwen3-8B:memorilla-f32-safetensors-bundle-v1",
        "memory_module_path": "/srv/antfly/memorilla/lora-hybrid-bundle",
        "released_memory_module_path": "/srv/antfly/memorilla/bundles/stage3",
        "max_documents": 64,
        "max_new_tokens": 64
      }]
    }
  }
}
```

Pins checked before any model loads: the bundle's `manifest.json` pins the
size and SHA-256 of `encoder-lora.safetensors`, `candidate.json` and
`protocol.json`; `source.candidate_sha256` must be the hash of
`candidate.json`; `candidate.json`'s `encoder_lora_sha256` must be the hash of
the adapter file; `protocol.json` gives `rank`, `alpha` (scale `alpha / rank`,
2.0 for copy17) and `target_modules` (`q_proj`, `v_proj`). The adapter file
must hold exactly the 144 expected F32 tensors. When the research reader is
opened from the same bundle, its manifest must be the one the adapters were
verified against and its candidate the same. Any mismatch returns 409
`INCOMPATIBLE_MEMORILLA_PROFILE` (`MemorillaCandidateMismatch`,
`InvalidMemorillaEncoderLora`).

Requests have the research shape: `document_texts` (1 to 64, the same
per-text and total limits), no `document_embeddings` (400) and no
`source_verified` (409). `encoder_execution_identity` must be the stock
encoder's identity (`executionIdentity(qwen3_encoder_f32_v1)`), because both
passes run on it; the research encoder identity returns 409. The server
computes the pooled document vectors and the question vector itself.

```json
{
  "model": "memorilla-hybrid",
  "question": "In what country is Normandy located?",
  "document_texts": [
    "The Normans were the people who in the 10th and 11th centuries gave their name to Normandy, a region in France."
  ],
  "encoder_execution_identity": "REPLACE_WITH_THE_QUALIFIED_ENCODER_IDENTITY",
  "max_new_tokens": 48
}
```

What one request does:

1. Pooled pass: every text (and the question) through the stock encoder,
   last-token pooled, L2-normalized and sent through the FP16 then BF16
   storage channel, as a released client would store it. A text longer than
   the encoder's 256-token pooled limit, which the released path rejects, is
   split into sentence-greedy chunks of at most 128 tokens, one vector each
   (at most 64 vectors in total). The released reader turns the vectors into
   16 soft rows.
2. Record pass: the same encoder with the adapters, per-token `tokens-dual`
   features of the self-context batch (padded rows of at most 1024 tokens, the
   evaluator's bound for general passages; at most 16384 batch positions),
   then the research reader, copy head, contiguous runs, snapping and word
   completion: 16 record token ids, their copied spans and the source map.
3. Record gate: open when at least one copied span snapped every one of its
   slots at cosine 0.85 or more. Open: the system prompt is the released
   helpful prompt followed by the candidate's exact-answer prompt
   (`"You are a helpful assistant. " + protocol.system_prompt`) and the decoder
   reads 32 memory rows, the 16 soft rows then the record's 16 embedding rows.
   Closed (no copied span, or every span has a fence-sitting slot): the
   helpful prompt and the 16 soft rows alone.
4. Greedy decoding, then, when the gate was open, answer canonicalization: an
   answer that occurs in the decoded record text case-insensitively on word
   boundaries takes the record's spelling, and an answer that is the tail of
   one to three record words (at least three characters, hyphens and spaces
   ignored, starting inside a word only when it contains a digit) becomes that
   word sequence. This repairs the case flips and dropped code prefixes the
   soft rows induce (`WZ3-003545` to `Wz3-003545`, `3005601` to
   `Te3-005601`). The published text is the stripped answer.

The response has the research shape plus four fields: `record_gate` (`open`
or `closed`), `memory_tokens` (32 or 16, the rows the decoder read),
`canonicalized` (the text was re-spelled from the record) and
`soft_module_sha256` (the released reader's weights; `memory_module_sha256`
remains the research reader's). `candidate_sha256` names the candidate. An
abbreviated response (token ids, identities and usage omitted):

```json
{
  "model": "memorilla-hybrid",
  "text": "France",
  "finish_reason": "eos",
  "input_documents": 1,
  "memory_tokens": 32,
  "context_source": "supplied_documents",
  "answer_guard": "accepted",
  "record_gate": "open",
  "canonicalized": false,
  "citations": [
    {"document_index": 0, "start": 35, "end": 52, "text": "the 10th and 11th",
     "supports_answer": false, "match": "none"}
  ],
  "citations_unavailable": false,
  "candidate_sha256": "...",
  "memory_module_sha256": "...",
  "soft_module_sha256": "..."
}
```

Answer guard: hybrid answers are general (a sentence, a list, a date), so
only the frame-echo rules of the research guard apply. The trimmed text is
rejected when it is empty, contains a control character (including a
newline), contains the `<|memory|>` marker or contains the frame literal
`Memory` as a standalone word; the literal `UNKNOWN` is accepted. There is no
length, word-count or character-set limit. A rejection publishes `UNKNOWN`
with `finish_reason: abstained` and keeps the decoder's `token_ids`. The
Python evaluator has no guard; this is serving policy.

Citations and what is citable: only the record half can be traced to source
bytes. Citations are built exactly as on the research profile, from the
record's copied spans after word completion, and `supports_answer` is
evaluated on the published (canonicalized) answer. When the gate is closed the
decoder never read the record, so `citations` is empty (and
`citations_unavailable` false). Soft rows are a learned summary of pooled
vectors and are never cited: an answer that came from the soft half (most
free-form answers, for example `France` above, where the record copied a
different span) has no supporting citation even when it is correct. A
supporting citation shows that the answer text occurs in a copied region, not
that it is right.

Residency and admission: one encoder (16.1 GB F32), the decoder (32.8 GB F32)
and both readers (0.78 GB each on the device, plus the 67 MB host copy head)
stay resident, about 51 GB of weights. Each request admits, on top, the
larger of the two decoder plans (32 rows under the merged prompt or 16 under
the helpful prompt) plus 32 MiB of copy and snap transients, the released
reader's scratch for up to 64 pooled rows, and the research reader's scratch
for the exact number of per-token rows (computed by a tokenizer preflight
before admission). The encoder passes admit their own run workspace,
including the 11.8 MB of adapters and their per-position temporaries. The two
reader caches are borrowed one after the other, never together.

Differences from the Python evaluator, by design:

- F32 everywhere; the evaluator runs the encoder, readers and decoder in BF16
  and applies the adapters in BF16. Answers are compared case by case with
  `antfly-memorilla-lora-hybrid-verify`.
- Texts above the 256-token pooled limit are chunked; the evaluator pools any
  length. The verification sets never reach the limit.
- The record batch is capped at 16384 positions; the evaluator has no cap
  (depot uses at most 219, squad 1525).
- Canonicalization and SQuAD-style matching fold ASCII case only, and the
  record text is decoded token by token with invalid UTF-8 pieces replaced by
  U+FFFD, as the evaluator's per-token `decode` does for whole pieces.
- The frame-echo guard (above) does not exist in the evaluator.

## Evidence and remaining rollout gates

The [HTTP smoke tool](../../../zig/pkg/inference/scripts/memorilla/http_smoke.py)
exercises a running loopback server, including native embedding production,
strict request errors, A/B/A and concurrent request isolation, and deadline
recovery. It writes a local JSON receipt and stored-feature request for reuse:

```sh
.venv/bin/python zig/pkg/inference/scripts/memorilla/http_smoke.py \
  --base-url http://127.0.0.1:18090 --profile memorilla-stage3 \
  --encoder encoder --decoder decoder \
  --output .benchmark-results/memorilla/production-http.json
```

Use the installed relative encoder and decoder names for the running server.
The server must have room to load the full managed pair. Add
`--input-token-limit 64` when testing a server configured with that operator
input-token ceiling; the normal question and framing fit, while a longer
question must be rejected before GPU model loading.

The [qualification summary](memorilla-qualification.md) records the passing
component, managed execution, supervised HTTP and source-verified QA checks.
The supported composition uses F32 arithmetic/F32 KV on GB10 (SM121); F16 KV
remains unqualified. The 64-document/128-token resource check establishes
lifecycle behavior for that fixture, not corpus accuracy or sustained throughput.

Before enabling a deployment, evaluate representative answer quality and measure
capacity for its workload. The released reader's exact latent-only operational
QA remains unqualified. Serving stays disabled by default; database collection
integration and Gemma training remain [separate work](../../plans/memorilla-native-memory.md).


## Metal hybrid span-window serving

On macOS, build inference with `-Dmetal=true` and set
`ANTFLY_INFERENCE_REQUIRED_BACKEND=metal`. Use a `hybrid_research` profile
whose pinned protocol has `record_kind: span-window`, with the managed
Qwen3 encoder and decoder and the released memory-module bundle. Both the
span-window and `read_mode: none` paths use the Metal profile. Supply the
`model_identity` returned by that encoder on the same Metal server as
`encoder_execution_identity`.

The default Metal profile widens the checkpoint weights to F32. On Apple
silicon, set `ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES=1` to keep the pinned
Qwen3 matrices in BF16 and share their storage with the GPU. Token lookup
stages only the rows it needs. Both modes use F32 decoder KV and F32
normalization, residuals and reader arithmetic. The BF16 mode has its own
encoder and decoder execution identities. Leave
`ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL` unset on Metal.

Set finite host, device, combined, KV and scratch limits and provision
memory for both working sets. The two pinned BF16 checkpoints total
about 23 GiB before the reader, request scratch and KV. The standalone
released and copy17 research profiles still require CUDA.

The playground launcher accepts `BACKEND=metal RESEARCH=1` for BF16 and
`BACKEND=metal RESEARCH=0` for F32, with the same bundle and tokenizer inputs
as the CUDA launcher. Metal serving remains experimental.
