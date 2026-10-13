# Bounded live coordinator and exact source-clock sampling

The request-local coordinator pumps existing authenticated HLS/DASH readers into
bounded owned packet windows. Fetched units remain pending after capacity or
validation failure; configuration epochs drain before presentation clocks reset.
Prepare/commit performs all fallible probing before advancing the cursor. Preroll
eviction retains complete logical IDR assemblies and earlier-coded future
presentation samples. Retained outputs remain independently owned.

Automatic frontiers currently cover MJPEG and static progressive AVC. The
scheduling contract requires PTS >= DTS and monotone decode clocks; AVC also
validates complete native pictures. The next scheduled decode point bounds
subsequently admitted presentation. Unsupported timing and late data fail before
ownership transfer. Field-coded and in-band configuration streams use explicit
watermarks or EOF; automatic proof for those layouts remains open.

Sampling uses an explicit source-clock mode with rational tick comparisons.
Existing clip-relative sampling stays unchanged. VFR tests use a three-tick
clock and compare selected picture identities before/after preroll eviction,
including the actual model selector. Source intervals are returned separately
from manifest timing. Empty intervals carry explicit gaps, with no invented
embedding and no lost future pictures.

`embedding_gemma2.embedNextLive` consumes one ready window through the ordinary
native/Metal model path and commits after inference succeeds. A null outer
result means reload/EOF inspection is needed. A returned event with null result
marks a gap. Conservative shared host admission covers staging/validation/decode;
input bounds above 64 MiB or 8192 packets fail before model admission. No polling
sleep is hidden and blocking reload is opt-in. Failure retains the same window.

Tests cover retry without duplicate fetch, epoch clock reset, explicit gap
progress, transactional failure during commit, source-time identity after
preroll eviction, timing rejection before reads, retained packet leases and
exhaustive allocation failure cleanup. Production loopback HTTP verifies bearer
authentication, identity encoding, five exact playlist/init/media requests and
successive reload responses. It also retains the existing split-PES TS decode
qualification. The existing fragmented fixture has a 200 ms initial presentation
clock; expected intervals use that actual clock.

Reproduce using Zig 0.17.0:

```sh
# zig/lib/video
zig build test-video test-live-windows test-transport-http -Doptimize=Debug --summary all
zig build install-live-window-tests install-transport-http-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --summary all
# zig/pkg/inference
zig build test-video-model install-video-model-tests -Doptimize=Debug --summary all
zig build install-video-model-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --summary all
```

Final macOS gates pass 210 decoder tests (two skips), 12 focused coordinator/
sampling tests, two production HTTP tests and 39 model tests. Actual Linux ARM64
passes 12 focused tests, two production HTTP tests and 35 model tests with four
Metal skips. Fixture/binary/source identities are in `qualification.json`. Reports remove trailing blank lines for
Git whitespace checks. Runtime containers are pinned, offline and read-only,
with bounded writable temporary space for file tests.

This qualifies scheduling/ownership/source selection and the typed model adapter
boundary, not pretrained streamed-window inference or retrieval parity. Current
frontier probing decodes retained GOPs repeatedly; reducing and measuring that
work remains required. Automatic field/dynamic watermarks, cross-window
projection reuse, pretrained CPU/Metal/Linux stream parity and the wider
non-CUDA codec/container/performance goal remain active. No runtime codec
library or new transport dependency is introduced.
