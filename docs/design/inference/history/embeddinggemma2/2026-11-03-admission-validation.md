# Admission smoke checks, 2026-11-03

These checks exercise CPU admission and mixed-model progress for the changes in
`8b4768a38e`. They verify serving and failure contracts; they do not establish
new numerical-reference, retrieval-quality, or throughput qualification.

## Builds and assets

Zig 0.17.0 HTTP-only production servers were built with
`-Doptimize=ReleaseSmall -Dstrip=true -Dcuda=false`. Linux used `-Dmetal=false`;
the macOS build used `-Dmetal=true`. The ARM64 Linux build ran in the local
`antfly-zig-dev:b03f5e5fa7cb23d4` image. Compilation used a 20 GiB container;
serving was separately constrained to the smaller runtime caps below.

The Gemma safetensors header contained 1,376 BF16 tensor entries. Assets were
isolated copies of the local `google/embeddinggemma-2` and
`openai/whisper-small` model directories. SHA256 values:

| Asset | SHA256 |
| --- | --- |
| Gemma `model.safetensors` | `197a32965d4b1105faf060417baa899e193fb73cd401f42ec9295234d5553d79` |
| Gemma `config.json` | `b8f1e9931b57fbc054acdb445c41765d55b0074c58d145fa82839941ad1b5bb3` |
| Whisper-small `model.safetensors` | `1d7734884874f1a1513ed9aa760a4f8e97aaa02fd6d93a3a85d27b2ae9ca596b` |

## ARM64 Linux CPU

Each container had four CPU units and a hard `--memory` limit. The native
backend was required with `ANTFLY_INFERENCE_REQUIRED_BACKEND=native`.
Inference budget overrides were omitted. The resolved CPU policy retained its
2 GiB host limit and 357,913,941-byte scratch limit.

| Hard runtime cap | Requests | Result |
| --- | --- | --- |
| 8 GiB | Four flat text requests and one ordered text group | 5/5 HTTP 200 |
| 14 GiB | Four flat text requests and one ordered text group | 5/5 HTTP 200 |

Every result contained a finite 768-dimensional vector. Both forms used
`POST /ai/v1/embed`, with these request bodies:

```json
{"model":"google/embeddinggemma-2","input":["hello"]}
```

```json
{"model":"google/embeddinggemma-2","input":[{"content":[{"type":"text","text":"hello"}]}]}
```

The flat path previously reserved a 512 MiB temporary ceiling up front. The
ordered-group path also reserved conservative preprocessing and encoder peaks
before the real token count was known. The revised paths retain their hard
ceilings, admit physical temporary allocations before allocation, and reserve
encoder workspace from token rows. Repeated requests after grouped execution
also succeeded.

## Metal with Whisper resident

The Metal backend was required. The server ran with
`--process-memory-budget-mb 24576 --backend-budget-mb 7372`, representing a
24 GiB process envelope and approximately 7.2 GiB of backend memory. No host,
KV, combined, or scratch override was supplied. This was an explicit process
policy on the available Mac, rather than a separate 24 GiB physical machine.

All ten requests returned HTTP 200: one flat text embedding, one ordered text
group, four image embeddings, and four Whisper transcriptions. Three image and
transcription pairs ran concurrently while both models remained loaded. Image
input was a valid 32-by-32 RGB PNG; transcription input was one second of
16 kHz mono PCM silence in a WAV container. These inputs exercise admission
and model residency, rather than transcription accuracy.

## Retained regression checks

- `test-memory-admission`: 78 passed; 17 CUDA-only cases skipped in the CPU build.
  Includes actual temporary-allocation growth, default CPU scratch admission,
  overflow, refusal cleanup, session observer lock ordering and sampler gating.
- `admission-api-test`: six passed with strict error-log expectations and no
  leaks, including hierarchy diagnostics rejected before canonical dispatch.
- Both production HTTP listener regressions passed with strict error-log checks,
  no leaks and exit status zero: hierarchy JSON pointers and dense/sparse
  table-creation resource refusals.
- The full CPU database build passed with
  `zig build antfly -j1 -Dmetal=false -Dcuda=false -Doptimize=ReleaseFast`.
  The resulting executable also returned its version successfully.
- Four storage admission/restart checks passed. The disk case closed and
  reopened the LSM backend through seventeen writes/replays; long scopes also
  exercised the status-key heap fallback.
- Five selected inference model-manager and HTTP failure-response checks passed,
  along with the CPU inference executable type-check.

The sampler-gate regression processes 271,168 refusals with one sampler
invocation during that interval, then permits another sample after one second
of logical time. Its isolated execution took 0.03 seconds including startup;
this measures accounting overhead with a test sampler, not model throughput.
