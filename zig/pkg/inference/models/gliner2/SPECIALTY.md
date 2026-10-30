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

## GB10 initial production qualification

This initial baseline describes CLI SHA-256
`079ca98ab412687d1e1bf1aef42f316870e281ac624e869ddec8778ebe08aeca`.
Its production receipt and measurements remain historical evidence for that
executable. The performance follow-up below uses a different candidate.

On 2026-10-30, both original checkpoints passed native and NVIDIA GB10 CUDA
head parity and public HTTP validation against all 23 frozen cases. Entity
text, coordinates, classification labels, and ordering match exactly; maximum
public confidence error was `2.09e-6` against the `5e-4` gate.

The [production qualification receipt](../../testdata/gliner25/specialty/gb10-production-qualification.json)
binds the release CLI and test executable, source fingerprint, original model
and oracle pins, CUDA artifacts, driver and cuBLAS Lt library hashes, backend
settings, numerical results, serving costs, and sanitizer scope. The
[reproduction commands](../../scripts/gliner25/specialty/README.md) rebuild and
rerun each gate. Metal hardware qualification was not run.

Official canonical pulls and re-pulls passed for both models. The full artifact
matrix passed 72 load/extraction probes across 36 cases, including FP16,
changed FP32 weights, configuration drift, invalid declarations, provenance
drift, and recovery. The five-minute managed-model soak passed 5,545 requests
and 27,616 output items at concurrency eight with zero retries. It also checked
all public offset units, versionless requests, ragged schemas, catalog
capabilities, chunked and maximum requests, rejection/retry, unload/reload, and
shutdown. Late-window host PSS growth was 29.9 MiB against a 64 MiB bound;
request admission ledgers returned to their initial idle values.

The native/CUDA service tests passed 128 items at 512 prepared tokens each,
513-token rejection, cancellation while queued, scratch denial/retry,
eight concurrent requests, in-use unload refusal, and fresh qualified reload.
Owned CUDA-buffer growth was 223 MiB against a 510 MiB scratch reservation for
PII and 338 MiB against 1,057 MiB for GLiGuard. Driver and library-private
allocations are outside that buffer meter. Warm timed service requests needed
no new device allocations. PII dispatched its GRU without residual addition;
GLiGuard dispatched no GRU or span head.

All 12 final sanitizer lanes passed. Memcheck and initcheck covered all kernels
for both complete model fixtures and both GRU residual modes. Racecheck and
synccheck covered Antfly kernels matching `^termite_.*`; unmatched vendor
kernels are excluded from those two tools. The receipts retain this distinction
and do not claim a fully unfiltered four-tool campaign.

Initial baseline medians in milliseconds on the same GB10 host:

| Workload | Prepared tokens | PyTorch CPU | PyTorch CUDA | Antfly CUDA HTTP |
|---|---:|---:|---:|---:|
| PII English | 46 | 209.21 | 12.33 | 32.52 |
| PII full 42-label schema | 207 | 280.34 | 20.50 | 56.32 |
| GLiGuard prompt safety | 26 | 169.65 | 10.93 | 19.07 |
| GLiGuard all six tasks | 268 | 306.08 | 22.69 | 56.41 |

Initial baseline eight-item GLiGuard HTTP throughput was 244 items/s for safety and 27 items/s
for all six tasks, versus 372 and 53 items/s through the pinned Python CUDA
API. HTTP measurements include transport, parsing and presentation; the Python
measurements cover the in-process API. Model load, warmup and separate
allocation profiling are excluded. The pinned PyTorch CUDA runtime used
`PYTORCH_JIT=0` because its NVRTC cannot target SM121; TF32 was disabled. CUPTI
transfer counters were unavailable on GB10; allocator counters and Antfly's
transfer counters remain recorded separately. The [pinned Python benchmark
baseline](../../testdata/gliner25/specialty/gb10_qualification.json) retains
the package versions and per-case allocation evidence.

The broader regression run recorded 224 passes, 62 skips, zero leaks, and one
pre-existing default-native-Q8 Decide failure. Its exact `0.0669221878` logit
error exceeds that test's `0.05` gate and matches the preserved historical run.
[Native Q8 was already documented as unqualified](../../scripts/gliner25/CUDA.md#production-serving-checks-on-gb10),
and its implementation and numerical tolerance are unchanged. The production
receipt preserves the failed raw result and pinned baseline evidence. This
exception applies to the existing native Q8 model; the new FP32 specialty
checks all passed. Quantization remains outside this release.

## GB10 performance follow-up

The [performance comparison receipt](../../testdata/gliner25/specialty/gb10-performance-comparison.json)
passes **all 46 matched cases**: all 23 frozen cases with one request item and
again with eight request items. Every case used 10 warmups and 50 measured
iterations. Antfly's median was lower in every comparison, with speedups from
**1.316× to 2.189×**. Eight-item PII requests still execute serially; GLiGuard
uses physical batches of eight. These results apply to this GB10 host and
the pinned eager PyTorch baseline, without a claim about other runtimes or
unmeasured workloads.

Representative latency in milliseconds, shown as median / p95:

| Workload | Request items | Prepared tokens per item | Antfly CUDA HTTP | PyTorch CUDA | Median speedup |
|---|---:|---:|---:|---:|---:|
| PII English | 1 | 46 | 7.887 / 8.052 | 11.556 / 12.270 | 1.465× |
| PII full 42-label schema | 1 | 207 | 14.916 / 15.938 | 19.827 / 20.130 | 1.329× |
| GLiGuard prompt safety | 1 | 26 | 6.374 / 6.511 | 10.207 / 10.792 | 1.601× |
| GLiGuard all six tasks | 1 | 268 | 16.720 / 17.477 | 22.004 / 22.590 | 1.316× |
| GLiGuard prompt safety | 8 | 26 | 9.767 / 10.449 | 20.427 / 21.136 | 2.091× |
| GLiGuard all six tasks | 8 | 268 | 80.105 / 81.532 | 148.905 / 149.801 | 1.859× |

The candidate CLI SHA-256 is
`c96c022a00b6f7d874bca31838344e537cd5520253568435f8db85da393f643d`;
its captured source fingerprint is
`c6989938cea70b8f7deb35e3a2921fb94cada9b7f8db75a7b9f93316f98ef0df`.
The associated test executable SHA-256 is
`1a88bae2d61f5649ed392ce7bd7cbe06ab4863a6f0755fc34a4275978a3dbc71`.
The comparison retains every raw timing sample, input receipt hashes, candidate
binary hash, model/source/runtime pins and hardware identity. HTTP receipts
validate all frozen outputs and record both loaded instances using CUDA.
The source fingerprint identifies observed inputs; it does not attest a
reproducible compilation.

The follow-up enables the existing fused FP32 DeBERTa attention path for
qualified specialty models on GB10, uses strict FP32 cuBLAS Lt projections with
one bounded 32 MiB workspace reserved per execution, and keeps FFN output
projections on that path. It caches unchanged attention masks, avoids
full tokenizer scans during extraction metadata lookup, and skips redundant
task-length preparation when splitting is disabled. The fused attention kernel
also gains a block barrier between consuming the shared maximum and reusing
its storage for the softmax denominator, fixing a race exposed during this
qualification. The original FP32 artifacts and numerical tolerances are
unchanged; FP16 and TF32 are not used.

The first optimized candidate exposed physical retention in SMP allocator
slabs during concurrent request teardown. Five short-lived extraction heaps
now use the process allocator: the bounded execution heap, version-probe heap,
request-upgrade arena and two metadata-resolution arenas. Admission wrappers
and limits are unchanged. The corrected candidate passed the same 300-second
soak at concurrency eight: **11,277 requests and 56,419 output items**, with
**180,224 bytes (176 KiB)** of late-window host PSS growth against the unchanged
64 MiB limit. Admission returned to its initial idle state; unload, reload and
recovery passed for both models. Driver/library-private GPU allocations remain
outside the host PSS meter.

The baseline uses pinned PyTorch `2.9.1+cu128`, CUDA, four CPU threads, and
`PYTORCH_JIT=0`. Its CUDA 12.8 NVRTC cannot target SM121, so the pinned upstream
model runs eagerly. FP32 matmul precision is `highest`, both TF32 switches are
disabled, and capture hooks are absent. Antfly timings include the complete
loopback HTTP exchange and response serialization; PyTorch timings cover the
in-process public API with preprocessing, model execution, decoding and
formatting. Both exclude model loading and warmup. This is a conservative
comparison of public serving latency, with different transport boundaries.

The [completed follow-up qualification receipt](../../testdata/gliner25/specialty/gb10-performance-qualification.json)
binds this optimized executable and its test binary to the unchanged FP32
artifacts, source fingerprint, CUDA artifacts and runtime libraries. Selected
specialty and Decide regressions recorded **50 Zig passes and nine skips**;
all **75 Python tests** passed. Skips include Metal and the unconfigured native
Q8 diagnostic; this follow-up does not qualify native Q8. Public outputs match
the frozen fixtures exactly, with maximum confidence error `6.32e-6` against
the unchanged `5e-4` gate.

All 72 artifact load/extraction checks passed. Native and CUDA service tests
covered 128 items at 512 prepared tokens each, over-limit rejection, cancellation,
concurrency, scratch denial/retry and qualified unload/reload. Owned CUDA-buffer
growth was **255.115 MiB / 369.548 MiB** for PII / GLiGuard, within reservations
of **542.087 MiB / 1,089.125 MiB**. Warm per-case service measurements used no
new device allocations; vendor and driver-private allocations are outside
that buffer meter.

All **12 sanitizer lanes** passed. Memcheck and initcheck covered unfiltered
kernels for both complete head fixtures and both GRU residual modes. Racecheck
and synccheck covered project kernels matching `^termite_.*`, excluding
unmatched vendor kernels. This scope is retained explicitly in the receipt;
it is not a fully unfiltered four-tool campaign. Metal hardware qualification
remains unexecuted.

Pinned upstream references: [PII model card](https://huggingface.co/fastino/gliner2-privacy-filter-PII-multi/blob/1cb4166094dc58fa8d836429f060d6c95f62b495/README.md),
[GLiGuard task reference](https://huggingface.co/fastino/gliguard-LLMGuardrails-300M/blob/7776816b54581f37b90dd2de12dbe13123229a9b/README.md),
and [span decoder](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/inference/runtime.py).
