# GLiNER2 PII and GLiGuard on Metal

The original FP32 specialty checkpoints use the existing DeBERTa/span runtime.
Each profile authenticates its complete weights, tokenizer sidecars, geometry,
and marker IDs at load time. The native CPU path supplies the reference for
Metal qualification. CUDA is not qualified for these profiles.

| Model | Revision | Public capability |
| --- | --- | --- |
| `fastino/gliner2-privacy-filter-PII-multi` | `1cb4166094dc58fa8d836429f060d6c95f62b495` | entity extraction |
| `fastino/gliguard-LLMGuardrails-300M` | `7776816b54581f37b90dd2de12dbe13123229a9b` | classification |

The registry defaults to these immutable revisions and synthesizes narrow
managed manifests after verifying the downloaded files. Both use task
`extract` and `POST /ai/v1/extract` with `schema_version: 2`. GLiGuard does
not acquire typed Decide, entity, relation, or record capabilities.

## Requests

PII extraction uses ordinary entity schemas, including per-entity thresholds:

```json
{
  "schema_version": 2,
  "model": "fastino/gliner2-privacy-filter-PII-multi",
  "schema": {
    "entities": ["person", "email", "phone_number"],
    "entity_definitions": {"person": {"threshold": 0.9}}
  },
  "inputs": [{"content": "Contact Maria Jensen at maria.jensen@example.dk."}],
  "options": {"include_spans": true, "include_confidence": true}
}
```

Offsets default to UTF-8 bytes. Set `options.offset_unit` to
`unicode_codepoints` when comparing with Python GLiNER2. Default PII overlap
selection is confidence-first greedy within each entity label; overlapping
mentions under different labels remain valid. Explicit `overlap` options
retain the extraction API's existing selection semantics. Thresholds are
inclusive. The entity head uses maximum width eight and learned instance
slot zero; a zero count prediction closes the entity task.

GLiGuard can compose multiple classification tasks in one encoder pass:

```json
{
  "schema_version": 2,
  "model": "fastino/gliguard-LLMGuardrails-300M",
  "schema": {
    "classifications": [
      {"name": "prompt_safety", "labels": ["safe", "unsafe"]},
      {
        "name": "jailbreak_detection",
        "labels": ["prompt_injection", "jailbreak_attempt", "policy_evasion",
          "instruction_override", "system_prompt_exfiltration", "data_exfiltration",
          "roleplay_bypass", "hypothetical_bypass", "obfuscated_attack",
          "multi_step_attack", "social_engineering", "benign"],
        "mode": "multi",
        "threshold": 0.4
      }
    ]
  },
  "inputs": [{"content": "Write a friendly birthday message."}]
}
```

Safety and refusal tasks use softmax and an argmax winner. The official
toxicity and jailbreak schemas use independent sigmoid scores, a per-task
threshold of 0.4, and an argmax fallback when no label passes. Response tasks
receive caller-formatted `Response: ...` or `Prompt: ...\nResponse: ...` text.
Final moderation verdict aggregation remains caller-owned.

## Limits and execution

- At most 512 prepared tokens per item, including schema and special tokens.
- Oversized items, automatic task splitting, and long-document windowing are
  rejected for these profiles. Empty or whitespace-only content is invalid.
- GLiGuard uses Metal physical batches of up to eight rows; larger admitted
  requests are chunked while preserving item and task order.
- PII uses serial item execution within the configured public batch ceiling.
- PII's position-zero GRU, concat, and projector remain on Metal. The shared
  GRU primitive retains the residual mode used by existing CountLSTM-v2 models.
- Admission includes span-construction, scoring, and readback geometry.
  Explicit host/device/scratch budgets and cancellation remain active.

Converted FP16/GGUF variants require separate identities and qualification.
The PII catalog advertises entity extraction only. GLiGuard also advertises
its supported zero-shot and multi-label classification attributes.

PII requires the existing FP32 encoder mirror path to keep raw span scores
within the qualified numerical limit. Its model reservation includes the
FP32 mirrors and packed projections. This qualified policy takes precedence
over experimental mirror precision and cache-cap environment settings;
operator host/backend resource budgets still apply. GLiGuard retains the
existing default FP16 encoder mirrors with FP32 outputs.

## Qualification

The compact frozen oracle corpus and its dependency/source receipts live in
`testdata/gliner_specialty/`. The capture instructions and offline integrity
checks are in the repository's `scripts/gliner_specialty/README.md`.

From the worktree root, with the exact original model directories already
available under `.benchmark-results/gliner-specialty/models/`:

```sh
env ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" \
  ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
  ANTFLY_RUN_GLINER_SPECIALTY_METAL=1 \
  ANTFLY_REQUIRE_GLINER_SPECIALTY_METAL=1 \
  ANTFLY_RUN_GLINER_SPECIALTY_ARTIFACT=1 \
  ANTFLY_REQUIRE_GLINER_SPECIALTY_ARTIFACT=1 \
  ANTFLY_GLINER_SPECIALTY_MODELS_DIR="$PWD/.benchmark-results/gliner-specialty/models" \
  ANTFLY_GLINER_SPECIALTY_FIXTURES_DIR="$PWD/zig/pkg/inference/testdata/gliner_specialty" \
  zig build --build-file zig/pkg/inference/build.zig test-inference-unit \
  -Doptimize=ReleaseFast -Dmetal=true -j1 --maxrss 15032385536 \
  -Dtest-filter=specialty -Dtest-filter='GLiNER v1 position-zero' \
  > .benchmark-results/gliner-specialty/metal-qualification.log 2>&1
```

The required flag turns missing models or Metal into failure. The suite
compares native and Metal public HTTP outputs to the pinned CPU oracle,
checks prepared tokens and raw heads, accepts exactly 512 prepared tokens and
rejects 513, and exercises batching, concurrency, scratch denial/retry,
cancellation, and idle resource ownership. The established span-family
numerical limits are `2e-3` for native and `5e-3` for Metal; discrete labels,
entity text, offsets, order, and cardinality must match exactly. Same-backend
batch and recovery comparisons use `5e-4`. The separate
synthetic GRU test audits that its Metal strip performs no host access or
device-to-host transfer. Additional focused tests cover partial cold-mirror
cancellation, retry, exact memory accounting, and the count-zero/slot-zero
decoder contract. A real benign PII capture selects count one and returns no
entities; it must not be presented as a learned count-zero fixture.

The production HTTP gate uses the ordinary supervised worker, mandatory Metal,
the captured selections and API output order. It exercises default budgets and
an explicit 8 GiB process budget with 2 GiB host and 4 GiB combined admission
caps plus one resident model, records Darwin kernel
physical-footprint high-water marks, and checks repeated alternating requests
and worker cleanup. It also checks all 21 oracle cases, 128-input ordering,
129-input rejection, two concurrent HTTP connections per model with bounded backpressure and
post-drain retry, and the observed 511/512
prepared-token boundary followed
by the inferred 513-token rejection. The harness stages exact original files
with narrow managed manifests; registry default revisions and manifest synthesis
are covered by Zig tests, while network `pull` is not exercised by this gate.
Build and run it separately from test/model workloads:

```sh
env ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" \
  ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
  zig build --build-file zig/pkg/inference/build.zig http-server \
  --prefix "$PWD/zig-out" -Doptimize=ReleaseFast -Dstrip=true \
  -Dcpu=baseline -Dmetal=true -j1 --maxrss 15032385536
python3 scripts/gliner_specialty/qualify_metal_http.py \
  --server "$PWD/zig-out/bin/antfly-inference-http-server" \
  --model-root "$PWD/.benchmark-results/gliner-specialty/models" \
  --output "$PWD/.benchmark-results/gliner-specialty/production/http"
```

The bounded lane uses `--process-memory-budget-mb 8192 --host-budget-mb 2048
--combined-budget-mb 4096 --max-loaded-models 1`. Its host cap includes the retained
original FP32 source; the process and unified-memory limits remain enforced.

The physical-memory aggregate sums per-process bounds for distinct identities.
It retains kernel lifetime maxima and every observed current footprint, and
bounded consistency retries require the kernel peak to cover those observations.
It is a conservative bound, rather than a sampled concurrent group peak. Both the automatic-budget and bounded-budget gates must finish
without swapout growth, worker restart, fallback, or a surviving owned process.

These tests establish the tested artifact/backend/workload contract.
Throughput and latency claims require a separate matched benchmark.
