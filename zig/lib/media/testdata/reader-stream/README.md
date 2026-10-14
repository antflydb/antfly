# Reader-selected model streaming qualification

`hls_live.Reader.streamNext` and the shared DASH Reader deliver selected plaintext
fMP4 fragments through a prefix-verified replay ledger. Resource context binds
manifest addressing, sequence/configuration identity and the stable credential
origin. Accepted native clocks and global ordinals survive retries. Cursor commit
waits for successful resource EOF. Reload cannot remove or mutate an unfinished
selection, and next() cannot bypass its accepted progress. Initialization/epochs
and lifetime byte/fragment limits remain shared between both APIs. DASH fragment
metadata exposes resource completeness; compatible new Period bounds preserve
coded-picture continuity without resetting the decoder.

`embedding_gemma2.LiveStream` connects that Reader directly to the coordinator,
native/Metal preparation, vision/projector and backbone execution. It copies
caption/task inputs and holds admitted context memory. Once a fragment transfers
into the coordinator it is acknowledged independently of a later model/output
failure. A ready output retains its exact vector, checkpoint and preprocessing
permit until both consumer acknowledgement and window commit succeed. Retry
redelivers an unacknowledged vector before any new HTTP request; acknowledged
side effects are not repeated after commit cancellation. New epochs drain old
outputs before incoming ownership transfers. Caller controls and borrowed
session/tokenizer/coordinator/cache owners outlive retained state.

## Completed gates

- macOS ARM64: 186 media, 13 production HTTP, 267 video checks (two platform
  skips), and 42 model checks (one optional checkpoint skip).
- Actual Linux ARM64: 186 media, 13 production HTTP, and all 25 owned live-window
  checks, including the two new streamed epoch/Period-continuity checks.
- Pretrained macOS CPU and Metal: each executes the authenticated real chunked
  HTTP/model test and passes six source windows. Streamed and buffered vectors
  have zero logged coordinate difference; token counts and window clocks match.

The HTTP server waits for the first real embedding before closing an incomplete
response. The consumer injects failure, while two encoded fragments have already
moved into the coordinator. Retry delivers the identical retained vector before
request four. The retry server withholds its tail until the next real embedding
callback. Three callbacks execute before that tail; all six normalized vectors
are copied/checked against buffered inference. A separate fault test retains an
acknowledged output across cancellation during a real coordinator window commit
and confirms that the consumer is not called again.

The fixture is a maintained synthetic Apache-2.0 copy from lib/media and lib/video,
with the pinned SHA in [receipt.json](receipt.json). FFprobe independently records
60 H.264 samples, six one-second fragments and B-picture presentation interval
0.2–6.2 seconds. Initial tests assumed three windows and failed; only fixture
expectations changed after inspecting these clocks. Pipeline policy and numeric
tolerances were not weakened. The inference binary predates test-only corrections
to external coordinator test declarations; imported modules do not collect those
tests. Shared runtime code is unchanged, and current coordinator tests execute
separately on macOS and Linux.

The receipt binds code, fixture, executable and completed raw log hashes. This
phase adds no independent PyTorch ranking, new Linux pretrained execution, TLS or
HTTP/2 incremental gate. AES/TS/LL-HLS streaming and partial-DASH availability/
append-only selected-resource completion remain active implementation work.

Reproduce from zig/pkg/inference with actual device/loopback access:

```sh
ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" zig build test-inference-unit \
  '-Dtest-filter=embeddinggemma2 video' -Dmetal=true --summary all
```

Run the produced test binary with `--test-filter 'embeddinggemma2 video streamed
pretrained'`, `ANTFLY_EMBEDDINGGEMMA2_MODEL` set to the verified checkpoint and
`ANTFLY_EMBEDDINGGEMMA2_METAL=true` or `false`. The test owns four authenticated
loopback requests and does not require a remote transport or codec library.

From zig, run `zig build test-video`; from zig/lib/media, run
`zig build test-media-http -Doptimize=Debug`. Cross-compile the portable media root
and install `install-live-window-tests` from zig/lib/video for
`-Dtarget=aarch64-linux-musl`. Execute both ELFs from a writable temporary working
directory on Linux. The receipt records the pinned image and native platform.
