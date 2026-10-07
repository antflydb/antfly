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
