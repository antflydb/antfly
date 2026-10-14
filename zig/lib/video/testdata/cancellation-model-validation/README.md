# Observed video cancellation and retry qualification

HTTP grouped embeddings now expose active, completed, cancelled, timed-out and
failed request metrics. An outer trace publishes its outcome only after the
inner operation releases frame-cache fences, model handles and arena storage.
The qualification also waits for weighted request admission to return to zero.
These metrics describe HTTP grouped requests; they do not cover every direct
embedding API or identify the GPU kernel active when a reset arrives.

The client now observes an active grouped request before an abortive TCP reset.
It verifies a new cancelled outcome after cleanup, zero active/admitted requests
and correct retry vectors. A separate write-half-close request must complete
normally with the correct vector and no new cancellation. This distinguishes
hard disconnects from legal HTTP half-close semantics. Earlier blind
shutdown/immediate-retry failures remain historical evidence; no inference
memory or concurrency limit was relaxed to make these checks pass.

Capacity retry honors the server's declared millisecond delay, with one 30-second
request/retry deadline and at most 16 attempts. Every denial is retained. Invalid,
non-capacity, non-retryable and transport failures stop immediately; deadline and
attempt exhaustion remain failures. A focused video probe uses the existing
explicit test-only run-admission hook to force one 503, waits one second, verifies
pretrained vector parity and checks released admission while real limits remain.

Fresh native CPU, Metal and actual Linux ARM64 reports pass all ten video vectors,
both queries, exact rankings and full top-1/5/10 overlap against the pinned
PyTorch CPU reference. Each passes observed reset cleanup, retry, legal
half-close and ten serial sustained requests. Each also passes the real forced
video capacity-denial probe. The cleanup metric unit test passes on macOS and
actual Linux; 31 Python contract tests cover bounded retry, retained failures and
cleanup attribution. Linux compares against the macOS PyTorch reference, rather
than a fresh Linux PyTorch execution.

These checks establish request-level cleanup for this generated corpus and
owned-server workload. Timings and memory samples are shared-host diagnostics;
they do not prove production throughput, arbitrary GPU-stage cancellation or
all concurrent load patterns. Broader display/scalable/real-stream qualification,
full-pipeline performance and the remaining non-CUDA scope stay active.

Reproduce the complete suite with `tools/embeddinggemma2/video_compare.py`,
`tools/embeddinggemma2/testdata/av1-layers/corpus.json`, the pinned checkpoint,
`--reference zig/lib/video/testdata/av1-layer-model-validation/pytorch-cpu.json`,
`--warmups 0 --iterations 1 --threads 4 --concurrency 1 --sustained-rounds 1`
and `--process-memory-budget-mb 4096`, using native/Metal backends and the current
server. Use `tools/embeddinggemma2/video_capacity_retry.py` with the same binary,
checkpoint, corpus and reference for the explicit forced-run-denial probe.
The probe requires an owned server and never injects failures into a deployment.
Linux runs the static `aarch64-linux-musl` binaries in the pinned ARM64 Python
image, network disabled, source/checkpoint read-only. No checkpoint or executable
is distributed. The initial incorrect metrics-route and wrong-target unit test
failures are retained separately; final runs use `/ml/v1/metrics` and verified
Linux ELF binaries. `qualification.json` binds current source and all evidence.
