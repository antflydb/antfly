# Experimental Windows build

Branch `experimental/windows-build` cross-compiles `antfly.exe` for
`x86_64-windows-gnu` from macOS or Linux. It targets a full-text table in Lite
storage, with no local inference. After merging main, use `antfly standalone
--storage-engine lite --storage-path <db.aflite>` for HTTP serving; main removed
`antfly lite serve`. It is not a supported
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

The generator rejects overlapping input/output paths and stages its patches
before replacing an existing overlay, so a mismatched Zig release leaves the
previous overlay intact.

## CrossOver smoke testing on macOS

CrossOver's Intel loader requires Rosetta on Apple Silicon. Create a dedicated
bottle so testing and forced shutdown do not affect other Windows applications:

```sh
export CX_BOTTLE_PATH=/private/tmp/antfly-windows-bottles
/Applications/CrossOver.app/Contents/SharedSupport/CrossOver/bin/cxbottle \
  --bottle antfly-test --create --template win10_64
python3 tools/windows/crossover_smoke.py \
  --binary zig-out/bin/antfly.exe \
  --bottle antfly-test --bottle-path "$CX_BOTTLE_PATH" \
  --out /private/tmp/antfly-windows-smoke
```

Use a fresh output directory each time. The runner retains logs and data. It
creates a table, inserts documents with `full_index` synchronization, checks
full-text results, runs concurrent requests, forcibly terminates the Windows
process, reopens the database, repeats the queries, and runs `lite check`.
Paths contain spaces to exercise argument forwarding into runtime units.
Wine does not establish native NTFS power-loss durability, console shutdown,
or Windows Server compatibility; those still need a real Windows runner.

The Windows argument parser can also be tested without a full Antfly build:

```sh
ZIG_LIB_DIR=/path/to/zig-lib-windows-overlay zig test \
  lib/platform/src/process.zig -target x86_64-windows-gnu -lc \
  --test-no-exec -femit-bin=/private/tmp/antfly-windows-args.exe
/Applications/CrossOver.app/Contents/SharedSupport/CrossOver/bin/wine \
  --bottle antfly-test --no-update /private/tmp/antfly-windows-args.exe
python3 -m unittest discover -s tools/windows -p 'test_*.py'
```

Use the same `zig test` command with `tools/windows/compat_test.zig` and a
different output executable to check positional reads, EOF, clocks, and
condition-variable timeout/lock behavior through the overlay.

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
  Directory sync is a no-op. NTFS journaling is not a proof that an acknowledged
  rename survives power loss; crash safety of namespace publication is unverified.
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
