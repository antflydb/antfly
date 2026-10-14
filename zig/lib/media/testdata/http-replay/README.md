# Prefix-verified native HTTP fMP4 replay

`http_stream.Adapter.streamFmp4Resuming` reconstructs framing on explicit retry
and suppresses previously acknowledged fragments after verifying their complete
canonical prefix. The caller-owned constant-space ledger binds URL, range,
initialization and epoch. Fragment ordinals/counts, native clocks, duration,
size, sync and coded bytes form the commitment. Mismatch or shorter clean EOF
poisons the ledger before new delivery. A failed/incomplete transport prefix
remains retryable. Hashing and framing retain cancellation and shared admission.

Callback success acknowledges automatically. Explicit `delivery.acknowledge()`
records an accepted side effect before a later callback error. `take()` transfers
ownership independently; a taken but unacknowledged fragment may recur and the
caller must reconcile that owner. Snapshot identities use the configuration
binding hash; authentication query bytes are not retained in that identity.
Complete reads are idempotent. `ledger.reopen()` explicitly polls append-only
body versions while preserving the prefix and attempts/work charges. HTTP EOF
completes a body version; manifest segment completion is a higher-level decision.

Both **macOS ARM64 and actual Linux ARM64** pass **179 portable media tests** and
**13 production HTTP tests**, with no reduced selection. [receipt.json](receipt.json)
binds sources, encoded fixture, binaries and completed raw logs. Native replay
preserves every packet byte and native timestamp against the unsplit full MP4
index. Tests cover consumer acceptance/ownership, config/range/epoch changes,
changed payload and native clock, truncated prefix, cancellation, admission,
recursive use, limits, every allocation failure and unchanged/appended polling.

The real authenticated chunked server closes without a terminating chunk after
one accepted fragment. Native httpx reports UnexpectedEof. An explicit second
request verifies/skips that fragment, then publishes five new fragments. The
server withholds its retry tail until the first new callback executes, proving
before-EOF delivery and suppressing duplicate consumer effects. Old/new snapshots
remain independently readable; the shared pool drains to zero. The append-only
fixture preserves the original six fragments and appends a copy of the initial
fragment with advanced tfdt clocks: unchanged polling yields zero new callbacks;
growth yields exactly one.

Linux executes static musl binaries in the pinned ARM64 image recorded in the
receipt, with network disabled, loopback available, a read-only root and writable
`/tmp`. An initial run failed because its default working directory prevented
existing temporary-file fixtures; the identical binary passes from `/tmp`.
This is not a new TLS, HTTP/2 streaming, pretrained model or DASH-partial gate.
Conservative HTTP admission and the bounded buffered HTTP/2 fallback are retained.
Durable exactly-once effects, Reader-selected resource cursors, partial-DASH
manifest policies and direct model-coordinator streaming remain active work.

Reproduce with Zig 0.17.0 from `zig`:

```sh
ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" zig test lib/media/media_test_root.zig \
  -O Debug -femit-bin=zig-out/bin/http-replay-media-macos
ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" zig test lib/media/media_test_root.zig \
  -O Debug -target aarch64-linux-musl --test-no-exec \
  -femit-bin=zig-out/bin/http-replay-media-linux
```

From `zig/lib/media`:

```sh
ZIG_LOCAL_CACHE_DIR="$PWD/../../.zig-cache" zig build test-media-http \
  -Doptimize=Debug --summary all
ZIG_LOCAL_CACHE_DIR="$PWD/../../.zig-cache" zig build install-media-http-tests \
  -Dtarget=aarch64-linux-musl -Doptimize=Debug --prefix ../../zig-out --summary all
```

Execute both Linux ELF files on ARM64 Linux from a writable test working directory.
The final tests require no codec or external transport runtime library.
