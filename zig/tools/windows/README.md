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

Use `-Doptimize=Debug` for frequent edit/test loops to avoid optimizing every
runtime library. Use `ReleaseFast` for final release-behavior qualification.
The full target includes the partitioned storage and inference libraries even
when the smoke workload does not use local inference.

When other builds share a cache, set `ZIG_LOCAL_CACHE_DIR` and
`ZIG_GLOBAL_CACHE_DIR` to dedicated directories and use `-j1` to limit compiler
concurrency. A suspended compiler can hold cache locks needed by other builds.

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
condition-variable timeout/lock behavior, secure entropy, and loopback TCP
through the overlay. The file-lock test checks exclusion, header reads through
a separate handle, explicit unlock, reacquisition, shared locks, and
exclusive-to-shared downgrade. The TCP test checks connection, byte transfer,
half-close, and EOF.

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
| `Io.Threaded` lock/unlock under Wine | synchronous lock ABI and contention status adapters |
| `Io.Threaded` secure entropy under Wine | `BCryptGenRandom` system-preferred RNG |
| `Io.Threaded` TCP under Wine | Winsock initialization, creation, options, connect/accept, vectored send/receive, shutdown, and close |

The lock change matters most. Zig locks byte 0 of a file, and Windows byte-range
locks are mandatory, so any other handle (even in the same process) that reads
a Lite header or writer-lock marker gets `LockViolation`. SQLite avoids this
by locking a byte range past real data, and the overlay does the same.

Wine rejects a non-null `NtLockFile` status block and reports contention as
`FILE_LOCK_CONFLICT`. It also declares the `NtUnlockFile` key as a pointer
instead of native Windows' `ULONG`. The overlay detects Wine through ntdll's
`wine_get_version`, uses Wine's synchronous locking arguments, and normalizes
contention to Zig's `WouldBlock`. It retains real byte-range locks and preserves
native Windows calls. See [Wine's implementation](https://github.com/wine-mirror/wine/blob/master/dlls/ntdll/unix/file.c).

Zig's direct `\\Device\\CNG` entropy source is unavailable under CrossOver.
For Wine only, the overlay uses [`BCryptGenRandom`](https://learn.microsoft.com/en-us/windows/win32/api/bcrypt/nf-bcrypt-bcryptgenrandom)
with the system-preferred cryptographic RNG. API errors remain
`EntropyUnavailable`; there is no predictable entropy fallback. The native
Windows CNG path and the I/O cancellation check are preserved.

Wine does not implement all of the native AFD socket IOCTLs used by Zig.
The overlay therefore uses Winsock for TCP operations under Wine, including
balanced `WSASocketW`/`accept` and `closesocket` bookkeeping. Wine detection is
cached so native Windows I/O does not repeatedly probe DLL exports. Wine's
unsupported outbound `REUSE_UNICASTPORT` hint is omitted; normal ephemeral
port allocation remains. Native Windows keeps the AFD path. These Wine calls
are synchronous, so cancellation and timeout behavior needs separate
qualification. UDP and Unix-domain sockets are not covered by this adapter.

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

## Verified after merging main

PR #987 (`ce319462af8b`) was merged with `origin/main` (`e6d4ce9bbc71`).
On Apple Silicon macOS with Rosetta, CrossOver 26.2.0, and Zig 0.17.0:

- The final Windows Debug application build passed all 46 build steps.
- The Windows argument round-trip test passed, including spaces, quotes,
  trailing backslashes, Unicode, and an unpaired surrogate.
- All five compatibility tests passed under both Debug and ReleaseFast:
  loopback TCP with half-close/EOF, secure entropy, file-lock contention and
  downgrade, positional reads, and clock/condition-variable behavior.
- The Debug application passed the CrossOver HTTP smoke: table creation,
  `full_index` batch synchronization, full-text results, 32 concurrent queries,
  hard kill/reopen, another 32 concurrent queries, another hard kill, and
  `lite check` (`valid: true`, no tail bytes). Database and runtime paths
  contained spaces.
- Native suites passed: platform 13 tests, httpx/objectstore 675 tests
  (10 skipped), and Lite ReleaseSafe 341 tests (5 benchmark tests skipped).
  Platform Python lifecycle suites (4 and 9 tests), overlay safety tests
  (2 tests), Ruff checks, and Zig formatting checks also passed.

The full application also compiled in ReleaseFast before the Wine adapters
were added. The final adapters were exercised in ReleaseFast by the focused
compatibility tests; the final full HTTP/recovery workload ran in Debug.
This does not qualify native NTFS power-loss behavior or a supported Windows
release.

## Prior native Windows qualification

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
