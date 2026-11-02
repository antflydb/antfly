# GLiNER2 PII and GLiGuard

These original FP32 safetensors checkpoints use the extraction schema-v2 API:

| Model | Pinned revision | Schema task |
|---|---|---|
| `fastino/gliner2-privacy-filter-PII-multi` | `1cb4166094dc58fa8d836429f060d6c95f62b495` | `entities` |
| `fastino/gliguard-LLMGuardrails-300M` | `7776816b54581f37b90dd2de12dbe13123229a9b` | `classifications` |

Managed pull preserves upstream files. Loading checks the original weights,
configuration, tokenizer, tensor inventory, encoder geometry, and structural
marker IDs. A model name or handwritten manifest cannot grant this contract.
The specialty profiles are independent of the modern span declarations and the
Decide API. Quantized or converted artifacts are outside this contract.

Each item must fit within **512 prepared tokens, including schema tokens**.
Oversized items and long-document requests are rejected. Schema tasks are not
automatically split across forwards. Public request cardinality follows the
server's configured limits. GLiGuard groups up to eight items per physical GPU
forward; PII processes items serially and preserves their input order.

## Local CUDA server

From the worktree root, pull the original checkpoints and start the normal CLI
with the qualified CUDA settings and admission budgets:

```sh
SPECIALTY_CLI=zig/pkg/inference/zig-out/bin/antfly-inference-dev
SPECIALTY_MODELS="$PWD/.specialty-artifacts/production-managed"
"$SPECIALTY_CLI" pull fastino/gliner2-privacy-filter-PII-multi --models-dir "$SPECIALTY_MODELS"
"$SPECIALTY_CLI" pull fastino/gliguard-LLMGuardrails-300M --models-dir "$SPECIALTY_MODELS"
ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda \
ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE=f32 \
ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO=0 \
"$SPECIALTY_CLI" run --models-dir "$SPECIALTY_MODELS" \
  --preload-model extractor:cuda:fastino/gliner2-privacy-filter-PII-multi \
  --preload-model extractor:cuda:fastino/gliguard-LLMGuardrails-300M \
  --max-loaded-models 2 --max-concurrent-requests 8 \
  --host-budget-mb 8192 --backend-budget-mb 12288 \
  --combined-budget-mb 20480 --scratch-budget-mb 4096
```

The catalog pins both pulls to the revisions above. Qualified specialty CUDA
sessions enforce FP32 weights and attention, including when global attention
diagnostics request reduced precision. The budgets reproduce the qualification
envelope; they are admission ceilings. The request examples below use the
canonical managed model names.

## PII extraction

```sh
curl localhost:8090/ai/v1/extract -H 'content-type: application/json' -d '{
  "model": "fastino/gliner2-privacy-filter-PII-multi",
  "schema_version": 2,
  "inputs": [{"content": "Email john.smith@acme.com or call +1 415 555 0199."}],
  "schema": {"entities": ["email", "phone_number", "person"]},
  "options": {"threshold": 0.5, "include_confidence": true, "include_spans": true}
}'
```

Select any subset of the documented labels. The full 42-label schema is:

```json
{"entities": [
  "person", "full_name", "first_name", "middle_name", "last_name", "date_of_birth",
  "email", "phone_number", "address", "street_address", "city", "state_or_region",
  "postal_code", "country", "government_id", "national_id_number", "passport_number",
  "drivers_license_number", "license_number", "tax_id", "tax_number", "bank_account",
  "account_number", "routing_number", "iban", "payment_card", "card_number",
  "card_expiry", "card_cvv", "username", "ip_address", "account_id",
  "sensitive_account_id", "password", "secret", "api_key", "access_token",
  "recovery_code", "sensitive_date", "document_date", "expiration_date", "transaction_date"
]}
```

The model card documents English, French, Spanish, German, Italian, Portuguese,
and Dutch. Schema size contributes to the token limit even for short text.
When overlap options are omitted, PII uses the upstream confidence-first greedy
selection independently for each label. Cross-label overlaps remain possible.
Explicit `overlap` or `flat_ner` uses Antfly's existing overlap policies.
Threshold comparisons are inclusive. A zero count prediction suppresses the
entity group; every positive count uses slot zero without capping mentions.

Offsets always refer to the original text. The default `offset_unit` is
`utf8_bytes`; request `"offset_unit": "unicode_codepoints"` when comparing with
Python string indices. Repeated mentions retain their own coordinates. The
response contains extracted entities; applications decide how to redact them.

## GLiGuard classification

The following schema composes all six tasks. A caller may select individual
tasks, and task and label order remains significant to the model's prompt.

```json
{
  "model": "fastino/gliguard-LLMGuardrails-300M",
  "schema_version": 2,
  "inputs": [{"content": "Prompt: Write a greeting.\nResponse: Hello!"}],
  "schema": {"classifications": [
    {"name": "prompt_safety", "mode": "single", "activation": "softmax", "labels": ["safe", "unsafe"]},
    {"name": "prompt_toxicity", "mode": "multi", "activation": "sigmoid", "threshold": 0.4,
     "labels": ["violence_and_weapons", "non_violent_crime", "sexual_content", "hate_and_discrimination", "self_harm_and_suicide", "pii_exposure", "misinformation", "copyright_violation", "child_safety", "political_manipulation", "unethical_conduct", "regulated_advice", "privacy_violation", "other", "benign"]},
    {"name": "jailbreak_detection", "mode": "multi", "activation": "sigmoid", "threshold": 0.4,
     "labels": ["prompt_injection", "jailbreak_attempt", "policy_evasion", "instruction_override", "system_prompt_exfiltration", "data_exfiltration", "roleplay_bypass", "hypothetical_bypass", "obfuscated_attack", "multi_step_attack", "social_engineering", "benign"]},
    {"name": "response_safety", "mode": "single", "activation": "softmax", "labels": ["safe", "unsafe"]},
    {"name": "response_toxicity", "mode": "multi", "activation": "sigmoid", "threshold": 0.4,
     "labels": ["violence_and_weapons", "non_violent_crime", "sexual_content", "hate_and_discrimination", "self_harm_and_suicide", "pii_exposure", "misinformation", "copyright_violation", "child_safety", "political_manipulation", "unethical_conduct", "regulated_advice", "privacy_violation", "other", "benign"]},
    {"name": "response_refusal", "mode": "single", "activation": "softmax", "labels": ["refusal", "compliance"]}
  ]},
  "options": {"include_confidence": true}
}
```

Single-label tasks use softmax and argmax. Multi-label tasks use sigmoid and
the explicit threshold of `0.4`, with upstream argmax fallback if no label
passes. Extraction schema v2 enables that fallback. The caller supplies raw
prompts for prompt-side tasks and `Response: ...` or
`Prompt: ...\nResponse: ...` for response-side tasks. Antfly returns task
classifications; the caller owns input formatting and final moderation policy.

## GB10 CUDA validation

The original FP32 models were validated against 23 pinned upstream cases,
including the seven documented PII languages, all 42 PII labels, all six
GLiGuard tasks, composed tasks, Unicode offsets, overlap and empty results.
Native and CUDA outputs matched labels, spans and order; maximum public
confidence error was `6.32e-6` against the unchanged `5e-4` gate.

On NVIDIA GB10, all 46 matched workloads (each case with one and eight request
items) were faster than pinned PyTorch CUDA by **1.32–2.19×** in median latency.
Each case used 10 warmups and 50 measured iterations. Representative medians:

| Workload | Request items | Antfly CUDA HTTP | PyTorch CUDA |
|---|---:|---:|---:|
| PII English | 1 | 7.89 ms | 11.56 ms |
| PII full 42-label schema | 1 | 14.92 ms | 19.83 ms |
| GLiGuard prompt safety | 1 | 6.37 ms | 10.21 ms |
| GLiGuard all six tasks | 1 | 16.72 ms | 22.00 ms |
| GLiGuard prompt safety | 8 | 9.77 ms | 20.43 ms |
| GLiGuard all six tasks | 8 | 80.11 ms | 148.90 ms |

Both implementations used FP32 with TF32 disabled. The Python baseline used
pinned eager PyTorch `2.9.1+cu128` and `PYTORCH_JIT=0` because its NVRTC cannot
target SM121. Antfly includes the full loopback HTTP exchange; Python measures
the in-process public API. Loading, warmup and allocation profiling are outside
the latency measurements. Results apply to this host and these workloads.

Serving checks covered 128 items at 512 prepared tokens each, 513-token
rejection, cancellation, scratch denial/retry, concurrency and unload/reload.
The 300-second soak at concurrency eight completed 11,277 requests with zero
retries and 176 KiB of late-window host PSS growth against a 64 MiB bound.
Owned CUDA-buffer growth stayed within scratch reservations, and warm service
requests required no new device allocations. Driver/library-private GPU
allocations are outside that buffer meter.

All 12 sanitizer lanes passed. Memcheck and initcheck were unfiltered;
racecheck and synccheck covered project kernels matching `^termite_.*`,
excluding unmatched vendor kernels. Metal hardware qualification was not run.

### Post-merge check (2026-11-01–02)

After merging `main` at `70fb539fee`, the DGX Spark GB10/SM121 passed the
ReleaseFast CUDA regression run: 191 tests passed and 94 optional model-fixture
or Metal tests skipped. Both specialty profiles ran their complete pinned
native and CUDA head oracles and public CUDA serving tests, including the
128-item/512-token geometry, cancellation, admission recovery and reload.

Production HTTP passed all 23 cases at request cardinalities one and eight,
with 10 warmups and 50 measured iterations per workload. Maximum public
confidence error remained `6.32e-6` against the unchanged `5e-4` bound. All four
conflicting global attention diagnostics retained the specialty FP32 policy.
The artifact-drift gate also passed. These checks do not rerun the earlier
paired PyTorch performance comparison.

The 300-second, concurrency-eight soak completed 11,593 requests and 57,309
items with zero capacity retries. Late-window host process-group PSS growth
was 126 KiB against the 64 MiB bound, with no change in numerical error.

Initcheck exposed uninitialized inputs in the shared cuBLAS BF16 startup
warmup, which the merged capability policy enables on GB10. Warmup now uploads
zero inputs and weights and drains queued work before releasing their storage,
including on error paths. Specialty model execution continues to enforce FP32.
Sanitizer receipts now record the warmup setting and other CUDA controls.
With the fix and warmup enabled, all 12 sanitizer lanes passed: unfiltered
memcheck/initcheck and project-kernel racecheck/synccheck (`^termite_.*`).

The default fatbin was regenerated from the merged CUDA source using CUDA
13.2. Local SASS inspection verified non-stub specialty kernel bodies for A100
(SM80), L4 (SM89), GB10's compatible SM120 image, and the standalone L4 cubin.
Dispatch-policy tests passed with fatbin, portable PTX and SM89 build settings.
L4 and A100 hardware execution was not performed. The qualification harness
now accepts all three GPUs and records the selected CUDA device rather than
inferring it from the physical GPU inventory.

[Reproduction commands](../../scripts/gliner25/specialty/README.md) keep model
weights, full intermediate captures and machine-specific receipts in the
ignored `.specialty-artifacts/` directory. Only small regression fixtures and
immutable input pins are checked in.

Pinned upstream references: [PII model card](https://huggingface.co/fastino/gliner2-privacy-filter-PII-multi/blob/1cb4166094dc58fa8d836429f060d6c95f62b495/README.md),
[GLiGuard task reference](https://huggingface.co/fastino/gliguard-LLMGuardrails-300M/blob/7776816b54581f37b90dd2de12dbe13123229a9b/README.md),
and [span decoder](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/inference/runtime.py).
