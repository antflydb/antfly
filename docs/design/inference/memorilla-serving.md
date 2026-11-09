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

The [latent operational QA comparison](history/memorilla/2026-11-09-latent-operational-qualification.md)
reproduces the released reader's factual failures in Python CUDA and native
CUDA with identical output IDs. Exact unseen names and codes remain an explicit
quality gate; source-verified hybrid qualification does not satisfy that gate.
The [published-data diagnostic](history/memorilla/2026-11-09-latent-published-qa.md)
records 20/32 Python BF16 TriviaQA exact matches, distinguishing those operational
failures from a claim that all latent-only QA fails. That diagnostic did not
execute the original vLLM engine.
The subsequent [official vLLM engine check](history/memorilla/2026-11-09-vllm-engine-qualification.md)
executes the pinned vLLM 0.9.1 path: QA scores match the independent BF16 control,
and fresh warm latency is 0.819 s native F32 versus 0.823 s vLLM BF16 on GB10.
The small median difference does not establish a general speed advantage.
The [PR integration record](history/memorilla/2026-11-09-pr-integration.md)
checks the implementation after merging current main, with fresh native
numerical, supervised HTTP, generated-client and lifecycle validation.
The [reader residency performance report](history/memorilla/2026-11-09-native-performance.md)
records the subsequent native latency improvement and cache lifecycle checks.
The [native/Python latency comparison](history/memorilla/2026-11-09-native-python-latency.md)
records tokenizer and metadata reuse, strict FP32 projection optimization, and
the resulting narrow observed median win over both Python controls.

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
decoder with fresh F32 KV. An exact completed `SUPPORTED` verdict is required.
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

At most one memory candidate, one independent source answer and one verification
call run. Answer candidates each obey `max_new_tokens`; the verifier is capped
at eight tokens and narrowed by profile and operator ceilings. All calls share
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

The [component qualification](history/memorilla/2026-11-07-native-qualification.md)
and [production serving record](history/memorilla/2026-11-07-production-serving.md)
document the passing numerical, managed execution and supervised HTTP checks.
The serving record binds the tested binary to its source hashes and includes
27 HTTP cases, concurrent isolation, deadline recovery, and model-file drift
rejection and recovery. Qualification covers the pinned Stage3 composition with
F32 arithmetic and F32 KV on GB10 (SM121). F16 KV remains unqualified.

Before enabling a deployment, evaluate representative answer quality and measure
capacity and load for its actual workload. The 64-document/128-token composed
resource test establishes lifecycle behavior for that fixture, not corpus
quality or sustained throughput. The initial
[raw-document quality diagnostic](history/memorilla/2026-11-08-answer-quality.md)
completed native execution but answered none of 22 factual cases correctly in
latent-only mode. The
[resumed F32 qualification](history/memorilla/2026-11-08-resumed-qualification.md)
passed 45 supervised source-verified development cases, 48 unseen held-out cases,
24 repeated sanity cases, and 30 HTTP contract/recovery requests. Cold asset
drift failed closed and recovered after restoration. These bounded diagnostics
support the opt-in source-verified path; latent-only factual QA remains
unqualified. The route remains disabled by default. See the
[implementation plan](../../plans/memorilla-native-memory.md) for the later
database and Gemma training work.
