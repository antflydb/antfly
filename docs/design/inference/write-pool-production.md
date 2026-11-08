# Bounded CUDA write-pool serving qualification

> Fresh PR #1574 qualification passed on 2026-11-08 on glowworm/GB10.
> The current results below identify production code `c845eded3c`; the separately
> labelled historical results retain their original identities. This qualifies
> the declared fixture and serving envelope, not arbitrary workloads.

This work targets the four-model profile in
[`configs/inference-write-pool-40g.json`](../../../configs/inference-write-pool-40g.json):
BF16 EmbeddingGemma 2, FP32 GLiNER2.5 boundary extraction, Whisper small, and
LightOnOCR3 0.8B. Hardware evidence is collected on the DGX Spark/GB10. This
qualifies a declared serving envelope and the listed fixtures; it does not
establish A100 performance or broad ASR/OCR accuracy.

## Execution and ownership

| Model | Automatic execution across independent HTTP requests |
|---|---|
| EmbeddingGemma 2 | Existing masked document cohorts: four rows, 8,192 padded tokens total; individual inputs through 8,192 tokens. |
| GLiNER boundary | Eight compatible single-input requests, 512 prepared tokens per row, 4,096 bucket-padded tokens total; 1 ms default collection. |
| Whisper small | Four compatible encoder rows, 5 ms default collection; each native decoder retains exclusive cache ownership. |
| LightOnOCR3 | Existing continuous page scheduler, two decoder rows and a four-page active window in the shipped write profile. |

GLiNER compatibility includes the loaded model generation, schema, options,
response/resource limits and hard-cancellation boundary. Nearby masked sequence
lengths share buckets; larger contexts, windowed documents and regex validation
retain exclusive execution. Backend capabilities declare the boundary cohort
limits and the Whisper stage policy. An explicit internal executor collection
override takes precedence, including zero. Decoder steps do not inherit the
Whisper encoder's collection delay.

GLiNER callers keep their managed model handles and parsed inputs until the
broker rejoins. The physical invocation owns an independently admitted bounded
host heap, one device reservation, and the existing fair model lane/mutex.
Response bytes have a separate shared admission owner that survives execution
and releases only after every result has been consumed or discarded. Physical
batch attribution comes from successful encoder/head execution, including any
pre-dispatch subdivisions. Admission refusal can subdivide before dispatch;
device failures are never replayed. Cancellation stops a physical cohort only
after every member is gone. The public request item ceiling applies before
schema-v2 dispatch and is independent of the cross-request cohort width.

Qualification exposed Whisper's old 4,096-entry flattened position-ID buffer:
three full-context rows need 4,500 entries and four need 6,000. The encoder now
allocates checked batch-sized position IDs, repeats positions independently for
each row, and validates the per-row source context before device work. Native
decoding remains per request; this change does not claim continuous multirow
Whisper decode.

## Qualification protocol and gates

[`qualify_write_pool_production.py`](../../../zig/pkg/inference/scripts/qualify_write_pool_production.py)
records the executable, configuration, selected runtime environment, GPU,
harness and every loaded model artifact. It verifies model artifact identities
again after the campaign. Missing memory measurements make qualification
incomplete. Every phase requires managed-memory measurements and service-PID
NVML attribution, with at least two successful samples and no sampling errors.

The profile retains the shared 38 GiB managed CUDA guard, 32 GiB host admission,
70 GiB combined admission, 20 GiB KV and 12 GiB scratch sublimits, with a
72 GiB cgroup process ceiling. The 2 GiB difference between the managed guard
and nominal 40 GiB device target is reserved for excluded driver/vendor memory.
Reservations, managed allocations and NVML measurements overlap and must not
be added together.

The declared gates precede the candidate measurements:

- At least three controlled rounds of eight serialized versus eight concurrent
  independent requests per model. All rows must retain output/usage parity;
  encoder vectors require cosine at least 0.9999 and maximum coordinate drift
  at most 0.005, and extraction confidence drift is bounded by 0.0005.
- Repeated observed physical batching, rather than queue turnover or executor
  attribution alone. Collection is opportunistic; individual staggered bursts
  may remain singleton. OCR reports actual decoder step width in responses.
- Concurrent throughput at least 90% of serialized throughput. A 10% throughput
  improvement is a separate performance-promotion gate, reported explicitly.
  Burst completion must fit 1.25 times serialized work plus 50 ms.
- At least 100 mixed queue turnovers and five minutes of sustained traffic,
  including exclusive windowed extraction alongside brokered short requests.
  Mixed p95 engineering limits are 1.5 s for embedding, 2 s for extraction,
  5 s for short transcription and 15 s for a single OCR page. These apply to
  this fixture/concurrency envelope, rather than an independently agreed SLO.
- Boundary context sweeps near 64, 128, 256 and 512 tokens, and exclusive
  fallback above the cohort limit, with per-request singleton reference parity.
- Frozen extraction spans/confidences for entities, attributes, relationships,
  legacy/natural/latent/anchorless records, enum fields and joint extraction,
  with eight independent requests per schema. The joint fixture explicitly
  selects its qualified native `auto` solver; classification-only fixtures
  belong to the decisions API. Independent required OCR values must survive
  all four synthetic layouts under traffic.
- All 73 clips from the hash-pinned LibriSpeech dummy dev-clean shard, one
  speaker, with aggregate WER at most 12%; dense varied 86/156-second composites
  with WER at most 15%.
- Configured item/context limit rejection without poisoning healthy peers;
  deadline/disconnect cleanup, subsequent successful requests, unchanged model
  instance IDs, no active requests/KV, and retained scratch at its warmed bound.

The independent Transformers CPU reference reproduces the checkpoint's collapse
of identical repeated speech. That known model-accuracy failure is separate
from passing the varied-speech/serving envelope; these fixtures do not qualify
all long-audio content or arbitrary production documents.

The production engine/host pressure harness separately exercises the physical
backstop with a 64 MiB guard, real allocations, failed-free quarantine,
confirmed cleanup and reload, with mandatory NVML evidence in each stable
phase. It reserves the same explicit 2 GiB excluded-allocation allowance. This
is a scaled guard test; ordinary write traffic far below 38 GiB cannot establish
behavior at 38 GiB exhaustion or qualify a 40 GiB A100.

The public fixture acquisition command removes the dependency on a private model
directory. It pins each public Hugging Face revision, verifies Git blob identities
for small artifacts and SHA-256 for LFS weights, and checks the speech corpus
against the campaign's fixed SHA-256. Existing immutable cache entries are only
read and verified; acquisition receipts live with the new campaign.

```sh
python3 zig/pkg/inference/scripts/qualify_write_pool_fixtures.py \
  --models-dir .benchmark-results/write-pool-production/models \
  --config .benchmark-results/write-pool-production/config.json
```

The returned config uses self-contained local directories for the canonical model
names. Artifact files share immutable cache inodes when possible and copy across
filesystem boundaries. LightOnOCR3 receives the canonical reader manifest and
trained page template from the runtime import contract; its raw hub template and
the verified cache remain unchanged. This preserves artifact-root confinement and
canonical runtime inventory for every model. Downloads
are shared under `~/.cache/antfly/conformance-fixtures`. The campaign additionally
checks decoded items, returned items and prompt-token counters against each
schema-sweep cohort's actual responses. These counters must be present and exact;
physical batching alone does not establish correct caller observations.

Run the final campaign against the owned write service, with local model paths
substituted in the canonical config:

```sh
python3 zig/pkg/inference/scripts/qualify_write_pool_production.py \
  --binary .benchmark-results/write-pool-production/server-merged \
  --config .benchmark-results/write-pool-40g/local-config.json \
  --output .benchmark-results/write-pool-production/merged-final \
  --cgroup /sys/fs/cgroup/user.slice/user-1002.slice/user@1002.service/app.slice/antfly-write-pool-40g.service \
  --speech-corpus ~/.cache/antfly/conformance-fixtures/gemma4-transcription/4e69a06fa5edc90921e5e7e39a7084881f8b3ed9c805c574f4f39c6fde27c603.parquet
```

The harness requires Python 3.11 or newer; the corpus-decoding environment
also requires numpy, av and pyarrow. A shorter
`--diagnostic` run is recorded as diagnostic evidence and cannot replace the
full sustained campaign. Preserve failed and incomplete receipts alongside
successful reruns.

## Fresh PR #1574 GB10 results

The complete campaign and legacy write regression passed on `glowworm`
(`spark-beb5`, NVIDIA GB10). The optimized CUDA HTTP server was built from
`c845eded3c8f6a86552d57bc321ea27c61859203`; its SHA-256 is
`850cbe958ea8f29d65a5edf017d95d21a9f177d6159b8956cf11a4d8fc0deb22`.
The subsequent merge of main `3e8975a1da` changes no inference-package Zig source.
The merged branch also passes the 64 focused tests and production type-check.

The [machine-readable receipt](write-pool-pr1574-qualification.json) records
configuration, loaded model hashes, environment, physical batch counters,
context/schema sweeps, memory sampling and independent pressure recovery.
Raw requests, samples, acquisition receipts and service logs are retained in
`.benchmark-results/pr1574` on the qualification worktree and in the local
`fresh-evidence.tar.gz` bundle; the receipt records its SHA-256.

| Model | Concurrent / serialized throughput, five rounds | Mixed p95 |
|---|---:|---:|
| EmbeddingGemma 2 | 1.61× | 21.7 ms |
| GLiNER2.5 | 2.95× | 50.8 ms |
| Whisper small | 1.39× | 445.8 ms |
| LightOnOCR3 | 1.91× | 2,458.9 ms |

Every controlled cohort retained output/usage parity and repeated physical
batching. The mixed soak completed 172 turnovers and 1,376 requests in 300.9 s,
including exclusive windowed extraction. All context and frozen schema sweeps
passed, including exact per-caller decoded/returned/token observations.

All 73 pinned speech clips scored 7.57% WER. The 86.345 s and 156.08 s varied
composites scored 12.00% and 12.47%. The intentionally repetitive checkpoint
accuracy case still scores 87.5% WER; it is recorded as a known limitation and
is excluded from passed accuracy claims. The four synthetic OCR layouts passed.

Memory evidence is complete for every phase, with service-PID NVML attribution
and no sampling errors. Peak sampled managed CUDA allocation was 6,307,409,404 B;
peak service NVML was 6,833,618,944 B. The cgroup recorded a 3,987,664,896 B peak.
These overlapping measurements must not be added. Configured limits remain
38 GiB managed CUDA and 72 GiB process memory; unified-memory auto-sizing may
resolve admission ceilings below the nominal configuration. Deadlines,
disconnects, healthy-peer recovery and instance identity checks passed. Recovery
returned to the warmed scratch reservations with no active requests or KV.

The independent production engine/host pressure test also passed guard rejection,
failed-free quarantine, unload/reload and confirmed-free cleanup. Its sampler
collected 50 raw samples, including stable samples for all four required phases,
under a deliberately bounded 64 MiB managed allocation guard. This test exercises
failure recovery without filling the GPU.

## Historical GB10 results

The merged-source campaign passed 2,138 recorded task responses: 2,131 successful
responses and seven expected item/context/deadline rejections, plus four
intentional disconnect probes. Its sustained phase completed 185 turnovers /
1,480 mixed requests in 300.12 seconds. All four model instance IDs survived;
after recovery, active requests and KV reservations were zero. Host scratch
returned exactly to 67,637,248 bytes and backend scratch to 1,611,251,712 bytes,
their warmed retained-cache bounds.

These are median end-to-end times for the same eight independent requests,
including client transport, preprocessing and queueing, rather than peak
standalone kernel or decode rates. Five controlled rounds per model passed
both the non-regression and separate 10% throughput-promotion gates:

| Model | Eight serialized | Eight concurrent | Throughput ratio | Mixed p95 |
|---|---:|---:|---:|---:|
| EmbeddingGemma 2 | 0.120 s | 0.062 s | 1.92× | 20.6 ms |
| GLiNER boundary | 0.131 s | 0.057 s | 2.30× | 49.0 ms |
| Whisper small | 1.360 s | 0.994 s | 1.37× | 445.0 ms |
| LightOnOCR3 | 7.986 s | 4.199 s | 1.90× | 2,281.8 ms |

Every model showed repeated physical batching. All 40 concurrent OCR pages
participated in observed two-row decoder steps. Nine extraction schemas passed
their frozen Python oracles and singleton parity across independent callers,
including explicit-native joint decoding. The context sweep passed through
the 512-token cohort envelope and checked exclusive fallback near 1,024 tokens.

Speech WER was 7.57% over the 73 real clips / 1,150 reference words. The 86.345 s
and 156.08 s varied composites scored 12.00% and 12.47%. The separately recorded
24-repeat stress case still scored 87.5% WER and is explicitly a known accuracy
failure, not a passed accuracy case.

All 1,704 memory samples were valid, including 1,682 across 18 workload phases,
with no sampling errors. Peak sampled managed live allocation was 5.76 GiB,
cumulative managed highwater 5.83 GiB, and service-specific NVML 6.27 GiB.
These measurements are far below 38 GiB and do not establish near-exhaustion performance. The production-host
64 MiB guard/recovery test separately passed with 50 valid NVML samples,
48 MiB retained across failed free/quarantine, confirmed free, and a successful
64 MiB reload/readback. Observed excluded allocation peaked at 178,765,824 bytes,
within the explicit 2 GiB allowance.

The existing 102-request write regression also passed, including eight-page
OCR, sparse 68-second speech and deadline recovery. The same executable passed
the 287-request read regression with fresh native-batch controls, retrieval,
concurrent Gemma generation, cancellation and 115 valid memory samples. Focused
CPU/CUDA suites passed all 161 tests per configuration, including full-context
Whisper positions, encoder-only collection, response ownership and admission. The 19 artifact/telemetry tests,
five new qualification tests, license headers, Apache boundary and diff checks
passed. GPU campaigns are local evidence; no A100 performance claim is made.

Provenance for the local receipts under
`.benchmark-results/write-pool-production/`:

- Qualified serving source: `7067d84b67187698af5ab20a367bbc9201bf4ae8`, including
  the merged CUDA teardown ownership and unlimited-admission fixes.
- Serving binary SHA-256:
  `a8c738517099f877cdbf75fb70e8c07e3c8d3f6a00d059daced9d05a79a53479`.
- Full campaign harness SHA-256:
  `9ce43c91db2caf2d796a0d75a5bbd901875f19e1487dcebe9a727c0891ca08f4`.
- Canonical sorted JSON model-artifact inventory SHA-256 (44 files, verified
  before and after):
  `4012b3912c7f049c8729982f87c9bdb1c331d0165b20b9bde3dc9a7edc232d76`.
- Production engine/host test binary SHA-256:
  `056e403d6982b7fc5eea2375f450bddc3273272daba6257e592207beec14f4c7`.

`merged-final/` contains the full campaign and an exact archived harness;
`merged-physical-recovery/`, `merged-legacy-write/` and
`merged-read-regression/` contain the separate recovery and regression receipts.
`source-receipt-merged.json` records source hashes and both executable identities.
The earlier pre-merge campaigns and binaries remain preserved separately.
At campaign completion, the owned write service was restored on port 18841
with the measured binary and 38 GiB guard, and the owned read service was
stopped. These are historical campaign conditions, not current service state.
