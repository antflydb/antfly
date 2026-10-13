# Native HLS segment-only delta snapshots

The native parser reconstructs skipped complete segments from the committed
snapshot, retaining map/key declarations, explicit identity METHOD=NONE,
discontinuities and byte-range continuation. Independent owned URIs survive
release/overwrite of old manifests. Exhaustive parser/reader allocation failure checks and
shared admission/cancellation checks cover cleanup. Incomplete skipped parents,
conflicting history, duplicate/malformed tags and descriptor mutations cannot
silently manufacture history.

An explicit reloadDelta requests YES only with advertised capability and a
monotone-clock age no greater than half the skip boundary. Stale, absent or
backwards clocks use full requests. Authentication query bytes are preserved and
delivery aliases rejected. Missing history issues exactly one ordinary request;
a failed retry retains the prior committed playlist and cursor.

The real HTTP test verifies bearer authentication and identity encoding on seven
exact requests: initial manifest/init/media, delta/media, unavailable-history
delta and full retry. Two successive media units preserve decode clocks, one
held packet lease survives reload, and no media/init is duplicated. The shared
pool returns to zero after owner release. Fixture-specific media reads are
bounded at 64 KiB with a 256 KiB packet index and a 64 MiB shared pool. Delta
planning conservatively reserves the configured planning bound before shrinking
to retained owned bytes.

Final ReleaseSafe gates pass 139 portable media tests and nine production HTTP
tests on macOS and actual Linux ARM64. Linux uses the pinned offline/read-only
image in qualification.json, read-only binaries and a 64 MiB temporary work
filesystem. The bounded fuzz report records 20,456 runs, 430 unique runs and
764 instrumented coverage counters on the final source. The two serialized
positive seeds validate the Smith encoding and reach actual inherited-key
merging. This is bounded evidence, not protocol or codec conformance.

Reproduce with Zig 0.17.0 from zig/lib/media:

```sh
zig build test-media test-media-http -Doptimize=ReleaseSafe --summary all
zig build test-media-hls-delta-fuzz -Doptimize=ReleaseSafe --fuzz=20000 --summary all
zig build install-media-tests install-media-http-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/hls-delta-linux
```

Protocol basis is the [HLS second-edition draft 22](https://datatracker.ietf.org/doc/html/draft-pantos-hls-rfc8216bis-22), sections 4.4.5.2, 6.2.5.1 and 6.3.7.
Date-range v2, combined blocking+delta directives, rendition coordination,
TS partials and CBC byte-range chaining remain open. This change does not
qualify pretrained inference through delta HTTP transport. Prior pretrained and
field-frontier receipts retain their original source snapshots. The full
non-CUDA goal, including performance/codec/scheduling scope, remains active.
