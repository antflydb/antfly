# Write-pool experiment with a 40 GiB device target

For the current automatic batching contract, explicit serving gates and expanded
qualification, see [write-pool production serving](write-pool-production.md).
The campaigns below retain their original narrower experimental scope.

This experiment runs BF16 EmbeddingGemma2, Whisper small, the FP32
GLiNER2.5 boundary extractor and LightOnOCR3 0.8B together on the DGX Spark.
It follows the [read-pool experiment](read-pool-40g-gb10.md). It qualifies the
listed requests against a constrained serving process; it does not measure
A100 performance or qualify a broad OCR/ASR accuracy corpus.

## Configuration and isolation

[The write-pool config](../../../configs/inference-write-pool-40g.json) pins
four CUDA model instances, disables idle unloading and JIT, caps requests at
eight items and four million decoded image pixels, and disables prompt caching.
The process-wide managed CUDA ceiling is 38 GiB. All four contexts share it;
per-model budgets alone would not enforce co-residency. Host admission is
32 GiB, combined admission 70 GiB, KV admission 20 GiB and scratch admission
12 GiB. KV and scratch are sublimits, not independent preallocations.

The server runs as the owned `antfly-write-pool-40g.service` user unit on
`127.0.0.1:18841`, with cgroup `MemoryMax=72G` and `MemorySwapMax=0`. This is a
serving-process/cgroup experiment, not a running Kubernetes pod. Read and write
campaigns run sequentially. The unrelated PDF and Circus workers are not
stopped. Another GPU worker was active during these experiments, and some
campaigns overlapped a CPU build. Whole-device utilization and these latency
samples therefore do not establish isolated throughput or an A100 SLO.

The private model directory contains immutable hardlinks or copies of qualified
artifacts. Whisper small is the official `openai/whisper-small` checkpoint at
revision `973afd24965f72e36ca33b3055d56a652f456b4d`; its safetensors SHA-256 is
`1d7734884874f1a1513ed9aa760a4f8e97aaa02fd6d93a3a85d27b2ae9ca596b`.
Static artifact inspection found an SM80 Whisper machine-code image and
SM75/SM80 targets in the inspected portable LightOn kernels. LightOn keeps
its SM121a-only routes behind the exact-device policy; artifact coverage is
not an A100 execution test. Model, source, configuration and executable hashes
are recorded with the raw
campaign data under `.benchmark-results/write-pool-40g/`.

## Workload and checks

The [stdlib Python harness](../../../zig/pkg/inference/scripts/qualify_write_pool_40g.py)
requires all four startup-managed instances to report CUDA. It runs warmup,
individual baselines, twelve rounds of concurrent requests and a burst of
24 requests from eight client workers:

- Embeddings: the qualified query at 256 dimensions, including batches of eight
  identical inputs. Every vector must be finite and normalized. BF16 batch
  accumulation differs slightly from single-item accumulation: all batch
  vectors must have cosine similarity at least 0.9999 and maximum coordinate
  drift below 0.005; single-item traffic retains the 0.002 drift bound.
- GLiNER: the independent Unicode pipeline fixture, with exact entity labels,
  text and codepoint spans. Unpadded scores must stay within 0.0005 of the
  fixture. Batches contain four longer inputs with independently shifted spans.
- Whisper: the English WAV and independently checked transcript, with WER at
  most 0.10, complete responses and monotonic segment starts. Digital silence
  must return no transcript. A 68-second fixture places the sentence at seconds
  2, 32 and 62 to exercise multiple recognition windows.
- OCR: four synthetic invoice, receipt, letter and shipping pages. Each returned
  page must be complete and contain its independent required strings. Bursts
  include four-page requests, and a separate request exercises all eight item
  slots with 3,456,000 decoded pixels.

A nine-page request must fail the item limit. An eight-page OCR request with a
200 ms caller deadline must return retryable HTTP 504. Recovery repeats all
four tasks and requires unchanged model IDs, no active requests and no active
KV reservations. Retained scratch reservations must return to the warmed
baseline. Memory sampling checks managed allocations against 38 GiB and the
service's own NVML footprint against the nominal 40 GiB target.

Run against the owned server, substituting the local model and ML directories
in the canonical config:

```sh
python3 zig/pkg/inference/scripts/qualify_write_pool_40g.py \
  --output .benchmark-results/write-pool-40g/final \
  --cgroup /sys/fs/cgroup/user.slice/user-1002.slice/user@1002.service/app.slice/antfly-write-pool-40g.service
```

Set `ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda`. The write pool uses its ordinary
model policies, without the Gemma-specific Flash-prefill or BF16-mirror flags.
Generic generation decode-graph replay is explicitly disabled. LightOnOCR keeps
its separate model-specific automatic policy, including supported graph paths;
the placement log records that choice.

## Results and accounting

The initial full campaign passed all 100 recorded task requests. Peak service
NVML memory was 5.69 GiB; peak managed allocations were 5.35 GiB. The same four
instances survived. Initial observed p95 values were:

| Task | Individual baseline | Mixed single-request rounds | Burst with batches |
|---|---:|---:|---:|
| Embedding | 14.4 ms | 18.4 ms | 307 ms |
| GLiNER extraction | 42.3 ms | 42.7 ms | 417 ms |
| Whisper | 213 ms | 423 ms | 2.09 s |
| OCR | 733 ms | 4.28 s | 25.95 s |

Burst OCR requests contain four pages and include queueing behind other OCR
work; their latency is not a per-page latency. Baselines contain five samples
per task, so p95 is effectively the slowest sample. These contended timings
identify workloads needing further tuning, not production performance claims.

The extended smoke checks passed eight-page OCR, the sparse 68-second audio
fixture with zero WER, deadline handling and recovery. The warmed and recovered
backend scratch reservations were both 1,611,251,712 bytes (1.50 GiB), with
67,637,248 host scratch bytes, zero active KV and zero active requests.
LightOnOCR retains an admitted temporary/deferred-free buffer cache; these
scratch-classified reservations deliberately outlive an individual request.
They are neither immutable weights nor a transient-memory leak. The failed
intermediate report that incorrectly required scratch to return to zero is
preserved as `extended-campaign`; the corrected rerun is separate.

Admission weight reservations, retained scratch reservations, live managed
allocations and NVML are overlapping views. Do not add them. Managed physical
bytes include resident weights, cached scratch and active buffers. Admission
can reserve more than is currently allocated, particularly the OCR cache
capacity. NVML adds driver/vendor memory not visible to `DeviceBuffer`.

## Complete corrected rerun

`validated/report.json` passed all 102 task requests, including the extended
68-second windowed transcription, eight-page OCR and final reservation checks.
Peak service NVML was 5.59 GiB. All speech-fixture WER values were zero;
minimum embedding cosine was 0.9999546 and maximum coordinate drift was
0.0021175. The same four instance IDs survived. The 200 ms OCR deadline returned
retryable 504 after 495 ms while the claimed device work drained. Warmed and
recovered host/backend scratch reservations matched exactly, with no active KV
or requests. This rerun used the executable saved as
`next/read-pool-server-before-manual-tail`; the later manual-tail scheduling
change applies only to the opt-in Gemma read path.

| Task | Individual baseline p95 | Mixed rounds p95 | Burst with batches p95 |
|---|---:|---:|---:|
| Embedding | 11.7 ms | 18.2 ms | 725 ms |
| GLiNER extraction | 44.8 ms | 52.1 ms | 785 ms |
| Whisper | 197 ms | 421 ms | 2.03 s |
| OCR | 741 ms | 4.44 s | 25.73 s |

The isolated eight-page invoice request completed in 3.03 seconds, and the
sparse 68-second audio request completed in 581 ms. These are synthetic smoke
fixtures on a shared machine, not representative service throughput.

## Accuracy limit found by the extra stress probe

A separate 59.9-second clip concatenating the same sentence 24 times failed:
Whisper returned only three copies, WER 0.875, despite reporting a complete
response. This is retained in `extended/long-audio.json`; the sparse-window
success must not be presented as a pass for dense repetitive speech. The extra
probe can be reproduced with `--repeated-audio`, which deliberately gates its
WER and currently makes that campaign fail. Longer varied real recordings and
an independent recognizer comparison remain necessary before claiming broad
long-audio accuracy.

## Final executable and running service

The complete `final/` campaign repeated all 102 task requests on the same final
manual-tail executable used for the read qualification and passed. Peak service
NVML was 5.46 GiB. The four CUDA model instance IDs remained unchanged within
the campaign; deadline, sparse-window audio, eight-page OCR and warmed-cache
recovery checks passed again. `environment-final.json` and
`live-environment-final.json` pin the final executable/model identities and CUDA
feature settings. The write service remains running on port 18841; the owned
read service is stopped. The separate repetitive-audio failure remains open.


## Repeat after cooperative ownership changes

The final serving binary passes the 102-request write campaign again, including
all phase-specific managed-memory and service NVML evidence. See the
[cooperative execution follow-up](cooperative-execution.md#write-pool-repeat) for
exact memory, retained-scratch and recovery results. This repeat does not change
the earlier dense repeated-audio limitation or qualify A100 performance. The
write service was stopped after qualification and the cooperative read service
was restored on port 18840.
