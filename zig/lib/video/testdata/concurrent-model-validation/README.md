# Completed concurrent pretrained video qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

The load harness now requires every request to complete with the same model
identity and correct vector. It retries only declared capacity denials, keeps
failed outcomes and their attempts, separates executor queue time from bounded
request time, samples lifecycle/admission counters and gates final completion
accounting. Normal requests and lifecycle retries have an explicit deadline;
cancellation observation/cleanup keeps its independent bounded checks.

Previously, arbitrary 429/503 responses counted as acceptable load outcomes.
They now fail unless the declared bounded retry eventually returns a correct
vector. Capacity exhaustion, timeouts, identity/vector drift, leaked admission,
unexpected cancellation, metrics errors and completion mismatches remain failed
outcomes. Thirty-seven contract tests pass.

## Measured results

A maintained 52-second, 1,253-picture Sintel H.264 derivative and four queries
use the pinned checkpoint, processor and unchanged parity gates. All five
backends/runtimes preserve the completed baseline vectors and scores.

| Runtime | Completed clip request (s) | Four-request batch (s) | Batch outcome |
|---|---:|---:|---|
| Native CPU, macOS | 66.12 | 142.43 | Four correct results, drained admission |
| Metal | 14.64 | 62.41 | Four correct results, drained admission |
| PyTorch CPU | 14.55 | — | Baseline parity passes |
| PyTorch MPS | 10.89 | — | Baseline parity passes |
| Native CPU, Linux ARM64 | 137.67 | 333.10 | Failed: two timeouts, one retry exhaustion, one result |

The owned macOS servers observe two active/admitted requests, no denials and no
late/unacknowledged completions. Both pass observed reset cleanup/retry and legal
half-close. Linux also passes baseline/lifecycle parity, but its concurrency-two
batch misses two 180-second client deadlines and exhausts 16 attempts on another
request. It retains **28 declared denials** across failed and successful attempts.
The server completes two timed-out requests later; final active/admitted counters
are zero. This is failed deadline/completion qualification, without an admission
leak at the final snapshot. Peak observed Linux admission is four, including work
that outlives client deadlines. Portable CPU cost, deadline handling and overload
recovery remain work.

The initial native run also retains a failed historical receipt: its 30-second
retry deadline was shorter than measured 67-second inference. Explicit normal
and load deadlines of 180 seconds address that macOS harness limit. No model
memory, admission or vector gate was raised. Linux uses the existing local ARM64
image with a 6 GiB cgroup, no external network and read-only source/model/server
mounts.

## Evidence and reproduction

[qualification.json](https://github.com/antflydb/antfly/blob/bf9d632d025a6b12837997f6aeb86e1a818ec831/zig/lib/video/testdata/concurrent-model-validation/qualification.json) binds sources, checkpoint/corpus,
binaries, results and logs. Measurements use retained
[measured_video_compare.py](https://github.com/antflydb/antfly/blob/bf9d632d025a6b12837997f6aeb86e1a818ec831/zig/lib/video/testdata/concurrent-model-validation/measured_video_compare.py), before the final reporting
correction. Raw reports remain unchanged. [accounting.json](https://github.com/antflydb/antfly/blob/bf9d632d025a6b12837997f6aeb86e1a818ec831/zig/lib/video/testdata/concurrent-model-validation/accounting.json)
reanalyses their request rows with current `sustained_evidence`: it includes failed
retry denials and separates quiescence from unacknowledged server completions.
The correction does not change runtime measurements or pass/fail workload gates.

From the worktree, using the pinned reference environment and an existing
qualified server binary/checkpoint:

```sh
python zig/pkg/inference/scripts/embeddinggemma2/video_compare.py --backend native --binary /path/to/antfly-inference-bench-server --model-dir .benchmark-assets/native-video/model --corpus zig/pkg/inference/scripts/embeddinggemma2/testdata/retrieval-long/corpus.json --reference zig/lib/video/testdata/retrieval-validation/long-pytorch-cpu.json --warmups 0 --iterations 1 --concurrency 2 --sustained-rounds 4 --request-timeout 180 --sustained-request-timeout 180 --output /tmp/native-long.json
python -m unittest discover -s zig/pkg/inference/scripts/embeddinggemma2 -p 'test*compare.py'
```

Repeat with `metal`, or `cpu`/`mps` without the server binary. Reference runs use
four Torch threads. Linux uses a static aarch64-linux-musl server inside the
image recorded in the manifest and the same command with mounted paths.
No checkpoint or executable is distributed.

These are sequentially executed, shared-host diagnostics: one resident-model
clip measurement per backend, four bounded load requests, different HTTP and
threading scopes, sampled RSS and no native Metal private-memory peak. PyTorch
reports self RSS and final MPS driver allocation, with different scopes. No
normalized speedup, memory advantage or production-tail/throughput claim follows.
A single document makes rankings/top-k trivial; this checks reference preservation,
not relevance. The whole non-CUDA objective remains active, including portable
CPU tuning, deadline/overload recovery, broader codecs/display/fuzz, transfer
telemetry and sustained pipelines.
