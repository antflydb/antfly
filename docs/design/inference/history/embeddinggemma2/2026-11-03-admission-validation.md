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

- `test-memory-admission`: 78 passed; 17 accelerator-dependent cases skipped in the CPU build.
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

## Follow-up review and main integration

The follow-up merges `origin/main` at `95592ec39a`. Native encoder allocations
now consume prepaid execution workspace rather than acquiring a second lease
for the same bytes. At the end of each phase, the allocator adopts the execution
lease before returning unused capacity. Retained outputs remain admitted without
an extra copy or a fresh cleanup-time reservation. Cumulative request-owned
scratch is checked before reserving a delta, preserving the distinction between
an intrinsic limit and contention with another request.

Dense and sparse remote providers now share classification of both
`MODEL_RESOURCE_BUSY` and the database's `reason: "inference_admission"`
envelope. The HTTP regression matrix includes both forms, ordinary 503s,
non-retryable admission-looking bodies, and permanent 400/507 resource failures.

`zig/e2e/inference/test_cpu_admission.py` is an opt-in real-model regression for
native execution with default inference budgets. It checks the first HTTP
response without capacity retries: flat/grouped text through 384 words,
retained outputs across an eight-item multi-cohort batch, precise concurrent-capacity refusals
and serial recovery after contention, and permanent
refusal of an oversized request. Against a running native server with the local
BF16 bundle and an 8 or 14 GiB process/runtime envelope, run:

```sh
ANTFLY_INFERENCE_CPU_ADMISSION=1 \
ANTFLY_INFERENCE_URL=http://127.0.0.1:18095 \
uv run --project zig/e2e/inference pytest -q zig/e2e/inference/test_cpu_admission.py
```

Follow-up CPU results (all twelve tests passed in each envelope):

| Runtime | Host / scratch policy | Result |
| --- | --- | --- |
| ARM64 Linux, hard 8 GiB cap, four CPU units | 2 GiB / 357,913,941 bytes | 12/12 passed |
| ARM64 Linux, hard 14 GiB cap, four CPU units | 4 GiB / 715,827,882 bytes | 12/12 passed |
| macOS native, explicit 8 GiB process envelope | 2 GiB / 357,913,941 bytes | 12/12 passed |

The over-limit input contains 4096 repeated words. A 512-word request correctly
fits the larger policy observed under the 14 GiB cap and is not a portable
permanent-refusal case. Concurrent flat/grouped lanes may correctly return
retryable `MODEL_RESOURCE_BUSY`; the test verifies first-response identity,
progress by an admitted caller, and a successful single serial recovery attempt
after competing callers have drained.

The final CPU admission suite passed 81 tests with 17 accelerator-dependent
skips. Shared inference/provider contracts (including twelve dense/sparse HTTP
error combinations), strict focused API contracts, generated registry/catalog
checks, and the regenerated Go SDK tests also passed.

The final Metal-enabled admission suite passed 97 tests with 17 skips. The
rebuilt Metal server also passed all ten mixed-model smoke requests under the
24 GiB process envelope and 7,372 MiB backend cap described above: flat/grouped
text, image embeddings, Whisper-small, and three concurrent image/ASR pairs.
Both model load selections were Metal; no model reload was observed between
pairs. These remain admission and output-shape smoke checks, not new quality or
throughput qualification.

The complete CPU production database build passed for source commit
`5e8ecab4ad` on the initial main merge (`95592ec39a`), using
`zig build antfly admission-routing-test -j2 -Dmetal=false -Dcuda=false
-Doptimize=ReleaseFast`. Both real HTTP regressions passed with no leaks and
also exited zero with strict unexpected-error-log enforcement. The installed
executable returned its version successfully.

Main was subsequently merged cleanly through `6817ccb184` (merge
`ea7ec6e0d6`). The generated engine-error registry was refreshed for the new
lake reachability errors. On that merged source, the CPU admission suite again
passed 81 tests with 17 accelerator-dependent skips, and the standalone
inference executable type check passed. Focused strict API and shared provider
contracts, generated registry/catalog checks, and formatting checks passed.

The retention regression was strengthened to keep three distinct outputs live
through later allocator scopes. The real-model eight-item batch now uses
distinct inputs and compares each row against an independent scalar request.
This strengthened case passed on the qualified server binaries under Linux
8 GiB, Linux 14 GiB, and macOS native 8 GiB policies. These binaries and the
complete production database build above predate the final main refresh; a
complete database rebuild of that final merge was not repeated locally.
Repository-wide integration CI remains subject to the repository approval gate.

Final review coverage extends the retention case to both flat inputs and
multipart text groups, with deliberately shuffled lengths
`[256, 16, 192, 48, 128, 8, 224, 80]`. Every response index must match its input
position, and every vector must match an independent request within `1e-5`.
The complete updated suite passed 13/13 cases in each of the Linux 8 GiB,
Linux 14 GiB, and macOS native 8 GiB envelopes (39 passing cases). Only test
and qualification documentation changed after the preceding review.

The earlier review's claimed grouped-path coverage gap was incorrect: public
grouped HTTP execution calls `grouped.embedCached` directly from
`Node.createGroupedEmbeddingTracked`; the text-only flattening helper belongs
to a different pipeline dispatch. Existing grouped cases already exercised
the native grouped allocator. The additional multipart batch case makes that
coverage explicit and verifies retained output order across multiple groups.
