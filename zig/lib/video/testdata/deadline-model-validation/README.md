# Inference caller deadlines: real-video timeout and retry

The standalone inference POST route wrapper accepts one optional
`X-Antfly-Inference-Timeout-Ms` header: decimal milliseconds from 1 through
3,600,000. It establishes a relative application budget at route dispatch,
narrows an existing deadline, rejects malformed/duplicate fields before
admission, and feeds the existing preprocessing/model execution control.
The maintained Python client declares its remaining per-attempt budget, so
capacity retries do not reset the original request allowance.

## Qualification

Fresh production server binaries pass the known 52-second Sintel campaign on
native CPU, Metal and actual Linux ARM64. Five malformed/duplicate requests
return 400 without changing group lifecycle counters. A one-second video budget
returns 504 in approximately **1.05 / 1.01 / 1.20 seconds**, respectively. The
one timed-out outcome is published after active/admitted counts return to zero.
A subsequent full-clip request preserves the pinned reference vector and model
identity and completes with released admission. Explicit functional retry budgets
are 600 seconds; observed retry times are about 62 / 12 / 99 seconds, with
maximum coordinate errors below 1.5e-6 and unchanged numerical gates.

**40 Python contract tests and two deadline-parser tests on each of macOS and
actual Linux ARM64 pass.** Parser checks cover aliases/duplicates, unsigned
bounds, budget narrowing and clock saturation. Linux uses the recorded local
ARM64 image, no external network, a 6 GiB model cgroup and read-only source,
checkpoint and server mounts.

## Failures and boundaries

The first native run retained a failed 180-second client retry while compiler
work overlapped. A diagnostic run with an explicit 600-second budget completed;
stack sampling found active vision SDPA and sequential portable GEMM work,
not a waiting cache. The final native/Linux checks ran concurrently; these
shared-host retry timings do not establish a performance regression, normalized
speedup or production throughput. Portable vision attention is a concrete
profiling/optimization target.

The first cold Metal probe saw worker exit86 at its deadline and a supervisor
replacement generation, then stopped before verifying a retry. This remains a
failed historical report. The existing process-fatal hard watchdog is unchanged:
it protects buffers retained by unsafe driver calls. A hard stop can close the
connection instead of returning JSON. The probe now accepts that alternate
outcome only with the owned supervisor's exit86, a verified fresh generation and
a correct subsequent retry. An unexplained disconnect never passes. The final
three successful runs used cooperative 504 responses; **full hard-stop retry
and specific GPU-stage interruption remain unqualified**. Group counters are
worker-generation scoped.

Application checks stop work at cooperative boundaries; transport read/write
limits remain separate. This does not qualify all ingestion stalls or long
native driver calls. The previous Linux concurrent load failure is not rerun or
relabelled as passing. Broader codec/display/fuzz, CPU SIMD/performance, overload
recovery, transfer telemetry and the complete non-CUDA objective remain active.

## Reproduction and provenance

Run the production benchmark server build from `zig/pkg/inference` with Zig
0.17.0: `zig build bench-server -Doptimize=ReleaseFast --prefix /tmp/deadline`.
For Linux add `-Dtarget=aarch64-linux-musl`. With the pinned checkpoint and
reference environment, from the worktree:

```sh
python tools/embeddinggemma2/video_deadline_qualification.py --backend native --binary /tmp/deadline/bin/antfly-inference-bench-server --model-dir .benchmark-assets/native-video/model --corpus tools/embeddinggemma2/testdata/retrieval-long/corpus.json --reference zig/lib/video/testdata/retrieval-validation/long-pytorch-cpu.json --retry-timeout 600 --output /tmp/deadline-native.json
python -m unittest tools.embeddinggemma2.test_compare tools.embeddinggemma2.test_video_compare
zig test -O ReleaseSafe zig/pkg/inference/src/server/inference_deadline.zig
```

Repeat the model probe with `metal`, or execute the static Linux server and tool
in the recorded image with mounted paths. No checkpoint or executable is
published. [qualification.json](qualification.json) binds sources, binaries,
checkpoint/corpus, reference, raw results, build/test logs and stack samples.
Final probes bind the current probe source; historical reports keep their original
source identities and failure labels. Linux compares with the pinned macOS
PyTorch reference. This is reference preservation and deadline ownership, not
application relevance or broad retrieval-quality qualification.
