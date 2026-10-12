# Native partial fMP4 HLS qualification

This receipt covers published partial-unit ingestion, verified completed-parent
fallback, explicit bounded blocking reload and authentication query preservation.
It is library ingestion evidence; model-level live orchestration remains open.
Antfly owns its Zig implementation and adds no runtime dependency.

The pinned semantic reference is the work-in-progress
[HLS second-edition draft 22](https://datatracker.ietf.org/doc/html/draft-pantos-hls-rfc8216bis-22),
especially sections 3.2, 4.4.4.9 and 6.3.8. Tests do not establish full HLS
conformance. Independent AES-128 resources are qualified; CBC byte-range chaining,
TS partials, delta playlists and rendition coordination are not.

The fixture is the existing `../../fragmented.mp4`: initialization bytes 0–796,
first partial bytes 797–11205 and second partial bytes 11206–20810. Each part
contains ten samples. The completed parent combines both fragments. Tests cover
part reload, all-consumed acknowledgement, unlisted-tail recovery, a changed
encoded prefix, HTTP retry, retained packet leases and exhaustive allocation
cleanup. Production loopback HTTP asserts bearer authentication, identity content
encoding, exact blocking query bytes, relative resource resolution and absence
of speculative preload requests. Parser tests cover ranges, flags and bounds.

Current-source media and HTTP gates pass on macOS Debug and actual Linux ARM64
ReleaseSafe: 128 media tests and eight production HTTP tests on each host.
The macOS video-model suite passes 34 tests; the full decoder regression suite
passes 199 tests with two platform skips (201 total). The Linux image is pinned in
`qualification.json`; execution disables external networking and mounts only
read-only binaries. File-source tests use a bounded 64 MiB tmpfs working directory.
The first Linux attempt omitted that writable temporary directory and failed in
`std.testing.tmpDir` before reaching the streaming tests; the corrected terminal
run is recorded, rather than treating the setup failure as codec evidence.

The fresh bounded parser fuzz campaign executes 55,971 cases (422 unique),
covering 997 of 10,521 reported edges, with two positively accepted serialized
Smith seeds. It reports no panic. This is not sustained production fuzzing.
The decorative `======= FUZZING REPORT =======` marker is normalized to
`FUZZING REPORT` in the copied receipt to avoid Git conflict-marker checks;
all execution counts and substantive report content are retained. Trailing blank
lines are removed from copied build reports for Git whitespace checks.

Reproduce from `zig/lib/media` using Zig 0.17.0:

```sh
zig build test-media test-media-http install-media-tests install-media-http-tests -Doptimize=Debug --summary all
zig build install-media-tests install-media-http-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --summary all
zig build test-media-hls-partial-fuzz -Doptimize=Debug --fuzz=50000 --summary all
```

Use an empty dedicated fuzz cache to reproduce a fresh campaign. The JSON receipt
binds source, fixture, binary and report identities; platform-dependent skips and
broader codec/model/live qualification remain separate scopes.
