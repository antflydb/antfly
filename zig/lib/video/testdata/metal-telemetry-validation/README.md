# Full-request Metal bridge telemetry

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

The inference metrics route exposes ten worker-process counters for explicit
buffer upload/download/copy bytes and calls, runtime command completion/failure,
completed GPU nanoseconds and successful commands without usable timestamps.
Counters are saturating atomics and never reset within a worker. Disabled Metal
builds expose zero counters without calling or linking the Metal bridge.
Snapshots are independent atomic reads, not a transactional request ledger.

Explicit buffer transfers include Shared-storage memcpy and Private staging.
Copy-pair operations count two logical copies. Transfers count only successful
API returns; an accepted upload/copy encoded in an active frame can still fail
when submitted later. Command failures are counted separately. Both maintained
runtime command factories attach one completion handler, adding timestamps only
after a successful GPU completion. Neither scraping nor instrumentation inserts
an extra GPU wait. Scrapes do not acquire a model execution lock or retain a
runtime. Existing watchdog/ownership behavior is unchanged.

With `--collect-metal-telemetry`, the maintained video comparison tool requires
all counters and preserves unsigned integer precision through uint64 maximum.
Missing, duplicated, malformed and out-of-range counters fail qualification.
Counter regression fails the interval (for example a worker restart). Saturated
counters are explicitly listed. Reports retain before/after/delta for each
completed warmup/measured request, reset/retry plus half-close interval, and
concurrent workload. Metric scraping is outside measured request HTTP time.
Existing numerical, lifecycle, admission and completion-accounting gates remain
mandatory; telemetry never turns a failed workload into a passing one.

## Verified checks

All 45 Python contract checks pass, including the added decoded-reference input guard. Two portable telemetry tests pass on macOS
and actual Linux ARM64. All 53 device-host video policy/lifetime checks pass,
including the named Shared/Private transfer oracle: exact logical upload,
download and paired-copy byte/call deltas, rejected calls leaving transfer counts
unchanged, preserved pixels and nonzero completed GPU timing. GPU unavailability
is not treated as passing this device gate.

The real 52-second Sintel clip also passes fresh pretrained Metal vector/score
parity, reset/retry, legal half-close and four-request concurrency-two load with
reconciled completion counts and released admission. Its measured request
records one explicit 3,072-byte buffer download, 3,799,928 logical upload bytes
over 2,027 calls, 264,118,272 copy bytes over 96 calls and 6,946 completed runtime
command buffers. Completed GPU time sums to 8.62 seconds, with zero failed
commands or missing timestamps; HTTP time is 17.22 seconds. These shared-host
observations are scoped diagnostics, not a normalized speedup or comprehensive
transfer count. All workload intervals retain raw counters.

Actual Linux ARM64 passes all 12 expanded-depth AV1/VP9 clip vectors, two query
vectors, rankings, reset/retry, half-close and concurrent load with the flag
enabled. Every disabled-Metal before/after counter is zero. The 6 GiB envelope,
180-second requests and numerical/admission gates are unchanged.

The measured harness source is preserved as `video_compare.measured.py`; raw
Metal/Linux tool hashes bind that snapshot. A later change adds only an early
PyTorch reference-input identity guard, rejecting recorded RGB/presentation
mismatches before expensive model execution. Native numerical/load gates are
unchanged. Current contract logs bind the maintained source. The final Linux
server was rebuilt after the device test addition and requalified; its initial
passing binary/report is separately retained.

A fresh Linux PyTorch run using distribution FFmpeg 7.1.5 fails the video gate
(max coordinate error 0.00479857, cosine 0.99926339). Text vectors preserve the
oracle within 1.1e-7; frame counts and selected IDs match, while decoded RGB
hashes differ from the pinned FFmpeg 9.0.2 oracle. This failed result is retained
as `pytorch-linux-ffmpeg7-failed`; no tolerance or oracle was changed.

With the complete hash-pinned FFmpeg 9.0.2 build, fresh actual Linux ARM64
PyTorch CPU passes the recorded RGB hash and all selected IDs, then all five
pretrained vectors/scores under unchanged numerical gates. The fresh reference
also directly preserves current Metal and the separately bound published native
Linux real-clip vectors; `linux-reference-preservation.json` records those
comparisons. The native Linux comparison uses the prior attention-phase captured
binary/report, not a fresh current-server real-clip execution. Current-server
Linux execution is independently qualified on the expanded-format corpus above.
The single-document ranking is trivial; no application relevance claim is made.

The image inventory records actual PyTorch 2.10.0+cpu, torchvision 0.25.0+cpu
and FFmpeg 9.0.2. Model weights are local/read-only and execution has no network.
This closes the fresh-Linux-reference gate for this known clip, not all formats
or Linux x86_64. Source preparation receipts contain no weights or credentials.
The earlier distribution-FFmpeg7 failure remains retained.

## Scope

These are logical explicit-buffer API counts, **not physical bus traffic or all
host/device movement**. Direct kernel convenience `newBufferWithBytes`, mapped
weight aliases, Shared host aliases, tensor mirror paths that bypass these APIs,
VideoToolbox and the separate video preparation bridge are excluded. GPU time
covers the two maintained runtime factories; legacy direct queue command buffers
are excluded. Command times may overlap and their sum is not wall-clock latency.
GPU private-memory peaks and comprehensive transfer attribution remain open.
Cold setup and unrelated requests in the same worker can contribute to a scrape
interval. Owned isolated servers give stronger attribution than an external URL.
No counters claim per-request ownership. Partial failures before an after-scrape
have no complete interval; ordinary qualification failure reports remain failures.

## Reproduction

With Zig 0.17.0, build the production benchmark server from `zig/pkg/inference`:

```sh
zig build bench-server -Doptimize=ReleaseFast --prefix /tmp/telemetry
zig build test-video-model -Doptimize=ReleaseSafe
```

From the worktree with the pinned checkpoint/reference environment:

```sh
python zig/pkg/inference/scripts/embeddinggemma2/video_compare.py --backend metal --binary /tmp/telemetry/bin/antfly-inference-bench-server --model-dir .benchmark-assets/native-video/model --corpus zig/pkg/inference/scripts/embeddinggemma2/testdata/retrieval-long/corpus.json --reference zig/lib/video/testdata/retrieval-validation/long-pytorch-cpu.json --warmups 0 --iterations 1 --concurrency 2 --sustained-rounds 4 --request-timeout 600 --sustained-request-timeout 180 --collect-metal-telemetry --output /tmp/telemetry-metal.json
python -m unittest tools.embeddinggemma2.test_compare tools.embeddinggemma2.test_video_compare
zig test zig/pkg/inference/src/backends/metal_telemetry.zig -O ReleaseSafe
```

For a reference Linux image, download the official
`https://ffmpeg.org/releases/ffmpeg-9.0.2.tar.xz` archive, then prepare a fresh
context from the verified reference environment and build it:

```sh
python zig/pkg/inference/scripts/embeddinggemma2/prepare_linux_reference.py --ffmpeg-source /tmp/ffmpeg-9.0.2.tar.xz --output /tmp/linux-reference
docker build --platform linux/arm64 -t antfly-video-reference-linux:ffmpeg9 /tmp/linux-reference
```

Preparation verifies the pinned model source files, the complete archive SHA-256
and its release marker before creating the context. The image uses an immutable
Python base, PyTorch 2.10.0 CPU/torchvision 0.25.0, pinned Python packages and
FFmpeg 9.0.2 local-video tools. It excludes network protocols and supports the
documented MOV/MP4, WebM, TS and JPEG reference inputs. This is qualification
tooling; Antfly build/runtime codec paths retain their zero-library-dependency
contract. Runtime validation mounts source and checkpoint read-only and disables
network access. Current source/pixel/numerical gates must pass independently.

For Linux add `-Dtarget=aarch64-linux-musl` to the server build, execute the static
binary in the recorded local image, and use the native backend. No checkpoint or
executable is published. Qualification receipts bind current sources, binaries,
raw observations and commands. The broader non-CUDA objective stays active.
