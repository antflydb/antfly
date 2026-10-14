# HLS date-range v2 qualification

Native Zig keeps owned date-range attributes and explicit program timestamps,
merges same-ID augmentations, rejects mutation, and retains resolved END-ON-NEXT
endpoints after removal of the following range. Decimal duration arithmetic
preserves arbitrary fractional precision without floating-point cancellation.
V2 removal lists accept tab-separated IDs and the required empty list.

The reader gates v2 on advertised capability and snapshot freshness, composes
blocking/delta directives without changing authentication query bytes, and
retries incomplete history with a full request. Full responses retain and
validate existing attributes for IDs they include. Failed updates leave the
committed cursor, snapshot and admission intact. Shared planning admission and
allocation-failure checks cover metadata construction and destruction.

## Evidence

ReleaseSafe Zig 0.17.0: **153 media tests and 10 production HTTP tests pass on
macOS ARM64 and actual Linux ARM64**. The local production HTTP client checks
Authorization, exact query parameters, v2 augmentation, combined requests and
full fallback. Fake transport checks additionally exercise failed fallback and
retry. Positively decoded serialized corpus seeds precede **100,879** bounded
coverage-guided date-range fuzz runs, with immutable-history and released-memory
invariants. Source, Linux ELF binaries and retained logs are SHA-256 bound in
[manifest.json](manifest.json).

## Reproduction

From the worktree, with Zig 0.17.0:

```sh
zig test -O ReleaseSafe -Mroot=zig/lib/media/media_test_root.zig
cd zig/lib/media
zig build test-media-http -Doptimize=ReleaseSafe --summary all
zig build test-media-hls-daterange-fuzz -Doptimize=ReleaseSafe --fuzz=100000
zig build install-media-http-tests -Doptimize=ReleaseSafe -Dtarget=aarch64-linux-musl --prefix /tmp/hls-http
```

Run the installed ELF on Linux. Portable tests cross-compile with `zig test
-O ReleaseSafe -target aarch64-linux-musl --test-no-exec -femit-bin=/tmp/hls-media
-Mroot=zig/lib/media/media_test_root.zig` and execute there. The recorded Docker
runs use the existing local ARM64 image, `--network none`, a 2 GiB limit and
read-only ELF bind mounts. Loopback is sufficient for the HTTP fixtures.

This qualifies generated metadata and local HTTP fixtures, not arbitrary live
origins or application semantics. Dates accept extended Gregorian date/time
and optional UTC offsets; leap-second notation is rejected. SCTE35 payloads are
preserved hexadecimal bytes without splice-command interpretation. Metadata
changes do not alter model preparation, and pretrained parity was not rerun.
Linux x86_64, sustained fuzzing and the remaining non-CUDA scope remain open.

Protocol reference: [HLS second-edition draft 22](https://datatracker.ietf.org/doc/html/draft-pantos-hls-rfc8216bis-22).
