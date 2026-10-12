# Published packet windows and native model reader input

`antfly_video.live_windows.join` copies finite published HLS/DASH units into an
owned packet reader without remuxing MP4. It owns the source identity string
as well as sample bytes and metadata, and preserves
media/source clocks and supplied codec preroll. Samples must arrive in decode
order, and mixed configuration epochs/DASH periods are rejected. Original units
can be released while the new reader remains valid. Admission covers staging,
retained snapshots and packet indexes; allocation-failure tests release leases
and tokens. The `../fragmented.mp4` fixture is byte-identical to the existing
media fixture, and its hash is bound in the receipt.

EmbeddingGemma 2's typed native API accepts `SampledVideo.reader` and exposes
`embedLiveWindow` for a finite unit set plus timestamp policy and optional
caption. Preflight, synchronous encoding and asynchronous preparation borrow
immutable source/packet/metadata storage until completion. Independent source
counters and controls leave caller read state unchanged. Reader-based projection
caching is disabled because it lacks a complete immutable clip cache identity.
Window construction uses a shared model host-preprocessing permit; execution
keeps existing model admission, cancellation, queues and locking.

Tests compare container and borrowed-reader preparation exactly on native CPU
and on completed Metal GPU work, using the same MJPEG samples and timestamp
policy. They validate cancellation, source read-state preservation, exclusive
bytes/reader selection and invalid-policy rejection before entering a model
session. The Metal check includes an explicit completion fence and readback;
it establishes preparation preservation, not pretrained model/retrieval parity.
Window tests cover multi-unit byte/timing equality, destruction of the original
segment before reading the retained window, caller identity mutation, duplicate
clocks, mixed epochs,
packet limits and exhaustive allocation failures.

The final macOS suite passes 37 model tests, 202 decoder tests (two platform
skips) and four focused window tests. Linux runtime results and all source,
fixture, binary and report identities are recorded in `qualification.json`.
Actual Linux ARM64 passes four window tests and 33 model tests with four Metal
skips. Its pinned offline container mounts read-only binaries and uses a bounded
64 MiB tmpfs work directory. Build reports remove trailing blank lines for Git
whitespace checks.

Reproduce with Zig 0.17.0:

```sh
# zig/lib/video
zig build test-video test-live-windows -Doptimize=Debug --summary all
zig build install-live-window-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --summary all
# zig/pkg/inference
zig build test-video-model install-video-model-tests -Doptimize=Debug --summary all
zig build install-video-model-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --summary all
```

This is finite-window construction and typed model entry. A live scheduler must
establish presentation completeness and include required preroll/reorder context;
PART/EXTINF durations do not prove a presentation watermark. Automatic reload-to-
window orchestration, bounded cross-window retention/eviction and pretrained
streamed-window CPU/Metal/Linux parity remain required by the full active goal.
No runtime codec library or new transport dependency is added.
