# Experimental Windows build

Branch `experimental/windows-build` cross-compiles `antfly.exe` for
`x86_64-windows-gnu` from macOS or Linux. It targets one workflow: `antfly lite
serve` over a full-text table, with no local inference. It is not a supported
platform.

## Build

```sh
python3 tools/windows/make_zig_lib_overlay.py \
  --zig-lib ~/.local/zig-0.17.0/lib --out /path/to/zig-lib-windows-overlay
ZIG_LIB_DIR=/path/to/zig-lib-windows-overlay \
  python3 tools/run_bounded_zig_build.py --zig zig -- build antfly \
  -Dtarget=x86_64-windows-gnu -Doptimize=ReleaseFast -Donnx=false -Dblas=off
```

`zig build --zig-lib-dir` is rejected after the step name, so the overlay is
selected with `ZIG_LIB_DIR`.

## What the overlay does

Zig 0.17's `std.c` leaves these POSIX declarations as `void` or as unresolved
externs on Windows. Antfly calls them directly from dozens of modules, so the
overlay (`antfly_windows_compat.zig`) supplies Win32-backed versions instead
of patching every call site:

| std.c declaration | Windows backing |
| --- | --- |
| `time_t`, `clockid_t`, `clock_gettime` | `QueryPerformanceCounter`, `GetSystemTimePreciseAsFileTime` |
| `nanosleep` | `Sleep` (millisecond resolution) |
| `pthread_mutex_*`, `pthread_cond_*` | SRW locks and condition variables |
| `MADV`, `madvise` | accepted and ignored |
| `dirent`, `readdir` | mingw-w64 `misc/dirent.c` (already in mingwex) |
| `pread` | `ReadFile` with an `OVERLAPPED` offset |
| `munmap` | `UnmapViewOfFile` |
| `std.DynLib` | `LoadLibraryW`, `GetProcAddress` |
| `Io.Threaded` file lock range | sentinel byte at offset 2^62 instead of offset 0 |

The lock change matters most. Zig locks byte 0 of a file, and Windows byte-range
locks are mandatory, so any other handle (even in the same process) that reads
a Lite header or writer-lock marker gets `LockViolation`. SQLite avoids this
by locking a byte range past real data, and the overlay does the same.

A supported port should move these behind `antfly_platform`, since modules
like `lib/generating` cannot import it today, or upstream them to Zig.

## Source changes and their limits

- `runtime_process.zig`, `platform.process.argsIterator`: Windows `std.process.Args`
  is a WTF-16 command line, not argv. Argument subsets are serialized with
  `CommandLineToArgvW` quoting. The `argsIterator` buffers are process-lifetime.
- Durability (`lib/runtime/src/fs_paths.zig`, `lib/objectstore`): file sync uses
  `std.Io.File.sync` (`NtFlushBuffersFile`, so handles are opened read-write).
  Directory sync is a no-op, relying on NTFS metadata journaling. Crash safety
  of rename publication is not tested.
- LSM atomic writes on Windows use `BufferedAtomicWriteSink` (in-memory, then
  write, sync and rename) instead of the POSIX fd sink. Large compactions hold
  the whole output in memory.
- `storage_io.zig` fd cache, `vector_block_store` mmap, and `peer_disconnect_observer`
  stay disabled on Windows (pre-existing gates).
- Inference `c_file.zig`: open, size, pread and read-only mmap use Win32 handles.
  `mmapTempCopy` and file identity are unsupported.
- Disabled on Windows: `antfly inference finetune`, the Postgres (libpq) foreign
  source executor, and `dladdr`-based worker discovery.
- httpx: Zig 0.17 Windows sockets are AFD device handles, not Winsock
  `SOCKET`s, so `ws2_32.setsockopt` and `WSAPoll` reject them. Native socket
  timeouts are disabled (the `std.Io` select-based timeouts take over), and the
  HTTP/1 disconnect-cancellation observer is skipped. A client that disconnects
  no longer cancels its in-flight request on Windows.
- Lite virtual index paths: `std.fs.path.join` uses `\` on Windows, which Lite's
  in-file path validation rejects (`InvalidNativeIndexPath`). On Windows the
  vector-block paths now join with `/`. Other `std.fs.path.join` uses on virtual paths
  probably need the same fix.
- Shutdown: console control events (Ctrl+C, close, logoff) replace SIGINT and SIGTERM.
- `File.Permissions.fromMode` and signed Windows inode numbers are handled at
  each call site.

## Verified

On a GCE `windows-2022` VM: `lite init`, `lite serve`, table creation, batch
insert, full-text query, `lite check`, and reopening after a hard kill. The
antfly-agent-workshop corpus checks pass (4/4) through both `promote.mjs check`
and `check-atlas.ps1`.

The tested binary was built just before a small behavior-preserving cleanup in
this commit (shared command-line quoting, unused-helper removal, `/` joins
limited to Windows). After the cleanup it was only compile-checked for Windows
(Debug), not re-run on Windows.

## Known gaps for supported Windows

- No Windows CI. `zig build test` has not run on Windows.
- Untested: vector or hybrid search, enrichments, backups, the distributed
  (Raft) runtime, TLS client calls (CryptoAPI trust store), and long-running
  or concurrent load.
- Lite files written by v0.2.x (format version 2) do not open on main. Convert
  them through the `/db/v1` API.
- The ONNX, CUDA and BLAS inference backends are untested. Metal is not
  applicable.
- Release plumbing (`scripts/release/platforms.json`, `.zip` archives, signing)
  is untouched.
