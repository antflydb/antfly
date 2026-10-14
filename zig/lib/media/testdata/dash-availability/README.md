# DASH availability-offset qualification

Selected BaseURL offsets accumulate along the MPD/Period/AdaptationSet/
Representation path. The effective inherited SegmentTemplate offset is added
once; nearer addressing declarations override their parent value. Alternative
BaseURLs outside the selected path do not contribute. Positive offsets permit
earlier requests, negative offsets delay eligibility. Offsets change the upper
availability boundary and leave presentation clocks, sequence numbers, expiry
and decoder configuration identity unchanged.

Arithmetic uses exact scaled nanoseconds with checked signed int64 offset sums,
not floating-point approximations. Decimal and scientific notation support
nanosecond precision; finer nonzero values are explicitly unsupported. INF
accepts finite advertised timeline/Period extents and rejects an unbounded
expansion rather than inventing a future horizon. Existing URI, metadata and
segment limits apply. Partial-segment availabilityTimeComplete=false remains
unsupported; this change qualifies complete-segment request scheduling.

Tests cover sub-boundary/exact-boundary eligibility, positive/negative offsets,
scientific notation, selected BaseURL alternatives, inherited overrides, open
timelines, finite/unbounded INF, unchanged retention expiry and configuration,
invalid numeric syntax, capacity and every allocation-failure rollback. A live
reader test observes no early media request, an offset-only reload, retry after
transport reset and retained prior packet leases. The [receipt](receipt.json)
records all 161 portable tests passing on macOS ARM64 and in the pinned Linux
ARM64 container, plus eleven production HTTP tests passing on macOS. The added
HTTP test requests only the manifest before the adjusted boundary and authenticates
all four requests once the boundary is reached. No model-vector or native-hardware
throughput claim is added.

Protocol: [DASH-IF restricted timing model](https://dashif.org/Guidelines-TimingModel/)
defines the upper boundary as wall clock plus total applicable availability
offset; the retention boundary remains wall clock minus timeShiftBufferDepth.

From the worktree with the repository Zig compiler:

```sh
zig test zig/lib/media/media_test_root.zig -O ReleaseSafe
zig test zig/lib/media/media_test_root.zig -O ReleaseSafe \
  -target aarch64-linux-musl --test-no-exec -femit-bin=/tmp/dash-media-tests
cd zig/lib/media
zig build test-media-http -Doptimize=ReleaseSafe --summary all
```

Execute the static Linux test binary on Linux. Loopback HTTP tests require local
socket access. The complete non-CUDA objective remains active.
