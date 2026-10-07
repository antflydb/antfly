# Platform I/O

Import the platform module as `platform`. Its `Io` namespace owns executor
selection while keeping borrowed contexts compatible with Zig's I/O APIs:

```zig
const std = @import("std");
const platform = @import("antfly_platform");

var executor = platform.Io.Threaded.init(allocator, .{});
defer executor.deinit();
const io: platform.Io.Context = executor.io();
try std.Io.sleep(io, .fromMilliseconds(1), .awake);
```

`platform.Io.Context` is an alias to `std.Io`, not a wrapper. Functions accepting
`std.Io` can use it directly, and a borrowed context retains the vtable and
ownership of the executor that created it. Keep that executor at a stable address
and alive until every operation using its context has completed.

`platform.Io.Threaded` selects the repository-owned Windows executor on Windows
and Zig's executor elsewhere. `platform.Io.Evented` selects the qualified local
io_uring backend on Linux and dispatch backend on macOS when fibers are supported;
it is `void` when fibers are unavailable. See [EVENTED.md](EVENTED.md) for its
qualification and current usage policy.

Add future backends and I/O helpers to `src/io.zig`. Changing a backend means
constructing a different owning executor; changing the namespace or context type
does not change an existing context's vtable. Shared APIs may continue to accept
`std.Io` and use its types such as `std.Io.File` and `std.Io.net.IpAddress`.

`lib/runtime` re-exports this namespace as `Io` and retains its existing `Threaded`
compatibility alias. New executor owners should use `platform.Io.Threaded`.

Composed builds use `bindPlatform` to share one platform module across standalone
library dependencies. Imports with the same target and physical platform source
are rebound to the composition's module, including when dependency roots use
different path representations. Different adapters and cross-target dependencies
retain their own modules.

Standalone `bindBuild` bindings preserve each artifact's libc policy. A module
with no explicit libc requirement gets a libc-free platform dependency, keeping
pure Zig consumers usable on freestanding targets. The JSON
`test-standalone-consumer` step compiles real consumers for libc-free Linux and
freestanding WASM; the composed `lib-json-test` step includes this check.
Raft and structlog compile their actual Linux unit tests without libc through
`test-nolibc`, also included in their normal standalone test steps. HTTPX's
`test-standalone-consumer` checks a Windows consumer that shares its platform
module with HTTPX; the composed `lib-httpx-test` includes that check.
Objectstore forwards the selected target and optimization to every dependency
and shares one platform module across its graph. Its `test-standalone-build`
step compiles the actual standalone tests for Windows and Linux, also included
in `lib-objectstore-test`. Platform tests exercise the public `Clock.real()`
API and compile that consumer without libc on Linux.

On Windows, system-directory lookup uses `GetSystemDirectoryW` rather than
private PEB fields. Wine sockets use Winsock overlapped stream and datagram I/O;
cancellation drains each pending request before releasing its event or buffers.
The I/O namespace tests cover hostname resolution, UDP payloads and source
addresses, empty datagrams, oversize errors, and socket reuse after cancellation.

Wine hostname resolution uses `GetAddrInfoW` in native workers because Wine's
resolver cancellation API is a stub. Each request owns its event and provider
storage independently of the executor, so cancellation and executor teardown
can finish while the provider completes. At most 32 requests, including canceled
requests still completing, can hold those resources. Native Windows retains
`DnsQueryEx`. The namespace tests exercise cancellation and admission limits;
`tools/windows/build_tests.py --suite dns` builds the external-hostname
qualification tests, which require internet access when executed.
