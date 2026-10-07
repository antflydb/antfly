# Experimental Windows build

Branch `experimental/windows-build` cross-compiles `antfly.exe` for
`x86_64-windows-gnu` from macOS or Linux. It targets a full-text table in Lite
storage, with no local inference. After merging main, use `antfly standalone
--storage-engine lite --storage-path <db.aflite>` for HTTP serving; main removed
`antfly lite serve`. It is not a supported
platform.

## Build

```sh
python3 tools/run_bounded_zig_build.py --zig zig -- build antfly \
  -Dtarget=x86_64-windows-gnu -Doptimize=Debug -Donnx=false -Dblas=off -j1
```

Use the installed, unmodified Zig 0.17.0 library. No `ZIG_LIB_DIR` overlay or
compiler patch is needed. Debug is suitable for edit/test loops; use
ReleaseFast for optimized qualification. Set dedicated `ZIG_LOCAL_CACHE_DIR`
and `ZIG_GLOBAL_CACHE_DIR` directories when other builds share a cache.

## Repository-owned Windows platform and I/O

`lib/platform` now supplies the Windows implementations. Its `c` API adapts
native clocks, synchronization, directory entries, positional reads and
mapping hints; `DynLib` supplies Windows dynamic loading. Existing Unix
implementations remain selected on their targets.

`platform.Threaded` selects `threaded_windows.zig` on Windows and stock
`std.Io.Threaded` elsewhere. The Windows backend is a checked-in adaptation
of Zig 0.17's Threaded backend, with its MIT notice and upstream source hash.
It owns its worker TLS, cancellation, path conversion and I/O vtable. Locks,
hardlinks and Wine socket/entropy adapters execute inside this owner.
`lib/runtime` also exports this executor. Libraries accept borrowed `std.Io`
values and dispatch through their caller's vtable.

All executor constructors and affected C/dynamic-library callers use platform
APIs. Product graphs and standalone libraries bind the platform dependency;
independent runtime archives retain their own executor ownership.
Windows process entry points replace Zig's default `std.process.Init.io` with
the platform executor. Legacy diagnostic I/O uses `platform.debug_io`; runtime
operations continue to use their caller-supplied executor. Test fixtures
use `platform.testing` for Windows I/O, with per-test teardown in the repository
runner. The overlay generator and its compatibility source files are removed.

When upgrading Zig, adapt the checked-in backend to the new public `std.Io`
interface, retain upstream license/provenance, and rerun the stock-Zig suites
and native NTFS qualification. Nothing rewrites the installed standard library.

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

## Focused qualification with stock Zig

From `zig/`, build all seven suites in Debug and ReleaseFast:

```sh
python3 tools/windows/build_tests.py --zig zig --out /private/tmp/antfly-windows-tests
```

The builder explicitly selects the installed Zig library, clears `ZIG_LIB_DIR`,
and creates hashed executables, `manifest.json`, and `tests.zip`. It covers
compatibility primitives, hardlinks, backup/staging cancellation, object-store
filesystem publication, Lite index/vacuum, storage I/O, and a separately compiled
archive borrower that dispatches 64 times after owning workers park. Use
`--suite backup --mode Debug` for a focused loop, or `--target native` for macOS
comparison. It uses the repository test runner and checks expected error logs.

Run each Windows executable in the dedicated CrossOver bottle or use
`gce_hardlink_qualify.ps1` with the archive on a disposable NTFS VM. The native
runner requires matching binary hashes and reports every process exit code.
CrossOver's four open-destination replacement failures require native comparison;
atomic publication is not weakened to accommodate Wine.

The file-lock implementation uses a sentinel byte at offset 2^62 to avoid
Windows mandatory locks blocking Lite header reads. Wine's lock ABI is adapted
inside the backend. Secure entropy uses `BCryptGenRandom` under Wine; native
Windows retains CNG. TCP uses Winsock under Wine and native AFD elsewhere.
Hardlinks use `NtSetInformationFile(FileLinkInformation)`, preserve source
identity, and refuse replacing an existing destination.

Wine stream reads and writes use an event and `OVERLAPPED` request owned by the
operation. Each Wine worker also owns a cancellation event; the wait observes
both events. Cancellation calls `CancelIoEx` for that request and drains its
completion before releasing buffers, the event or the worker's stack. It does
not close a shared socket. Native Windows retains the AFD implementation.
Compatibility tests cover pending-read cancellation, subsequent socket reuse,
task-level deadlines, and backpressured write cancellation. Windows Threaded
Batch socket concurrency remains unavailable; deadlines use task selection.

The `platform.c.pread` contract preserves the caller's shared file position and
sets CRT `errno` on every failure, including invalid offsets and write-only
handles. Synchronous handles are reopened by object identity with overlapped
I/O for the duration of the read; no pathname lookup or seek/restore is used.
Completion is drained before releasing request storage. Existing overlapped
handles read directly. Owners performing repeated reads should use
`platform.filesystem.openPositionalReadOnly` and retain the returned file;
model-file readers do this to avoid reopening on each read. Handle ownership
stays with the caller, which closes it through its executor. There is no global
cache keyed by reusable Windows handle values. Tests check unchanged offsets,
concurrent reads, stale `errno`, EOF, and offsets beyond 4 GiB.

## Source changes and their limits

- `runtime_process.zig`, `platform.process.argsIterator`: Windows `std.process.Args`
  is a WTF-16 command line, not argv. Argument subsets are serialized with
  `CommandLineToArgvW` quoting. The `argsIterator` buffers are process-lifetime.
- Durability (`lib/runtime/src/fs_paths.zig`, `lib/objectstore`): file sync uses
  `std.Io.File.sync` (`NtFlushBuffersFile`, so handles are opened read-write).
  LSM and immutable object publication flush file contents before rename and
  reopen/flush the published file afterward. Flush failures propagate to the
  caller, including failures after publication. Microsoft's
  [file caching documentation](https://learn.microsoft.com/en-us/windows/win32/fileio/file-caching)
  describes flushing file metadata this way. Directory sync remains a no-op;
  this does not prove durability of new ancestor directories or deletion, nor
  qualify the storage device's power-loss behavior.
- LSM atomic writes on Windows use `NativeStreamingAtomicWriteSink`: a sibling
  staging file, a fixed 64 KiB write buffer, and a 64 KiB checksum scratch buffer.
  Header patches and CRC ranges work across buffered and persisted bytes. The
  writer covers both native storage and the runtime bridge's `IoStorage`.
  Owned native sinks retain the I/O runtime through finish/abort; borrowed
  executors must outlive their sinks. The writer closes the staging handle
  before rename, and removes staging on abort or pre-publication failure with
  cancellation blocked during cleanup. Memory
  used by this writer no longer scales with compaction output size. Windows
  still lacks the POSIX cold-cache eviction hints and descriptor cache.
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
- HTTP batch offload completion uses a cancellation-protected future wait
  instead of polling every millisecond. Caller I/O cancellation must not
  interrupt mutation work before durability is known; the borrowed request
  token still controls safe visibility waits.
  Offload selects the imported durable executor at the API kernel boundary.
  Reconstructing a `Threaded` vtable from a foreign runtime pointer can split
  the owning archive's thread-local and Windows parked-worker wakeup state.
- Lite virtual index paths: `std.fs.path.join` uses `\` on Windows, which Lite's
  in-file path validation rejects (`InvalidNativeIndexPath`). On Windows the
  vector-block paths now join with `/` while preserving the standard join's
  empty-component and separator-boundary rules.
- Shutdown: console control events (Ctrl+C, close, logoff) replace SIGINT and SIGTERM.
- `File.Permissions.fromMode` and signed Windows inode numbers are handled at
  each call site.

## Verified after merging main

PR #987 (`ce319462af8b`) was merged with `origin/main` (`e6d4ce9bbc71`).
On Apple Silicon macOS with Rosetta, CrossOver 26.2.0, and Zig 0.17.0:

- The final Windows Debug and ReleaseFast builds passed all 46 build steps.
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

The follow-up executor fix also passes a 1,000-write indexed workload in both
Debug and ReleaseFast under CrossOver, including every body hash and full-text
entry after hard-kill recovery and an offline integrity check with zero tail
bytes. Native NTFS tests use hard VM resets; physical storage power-loss
behavior and a supported Windows release remain unqualified.

See [the follow-up qualification report](QUALIFICATION.md) for bounded-writer
tests, native Windows Server/NTFS reset evidence, and sustained-write limits.

## Native Windows reset qualification

Use a disposable Windows machine and an NTFS data directory. Do not reset a
shared machine. An abrupt hypervisor reset tests loss of the guest's buffered
state; it does not simulate loss of power to the storage controller or prove
physical-device durability. A process kill alone leaves the OS cache intact.

On Windows, check the filesystem and run the server with explicit fsync:

```powershell
New-Item -ItemType Directory -Path C:\antfly-test -ErrorAction Stop
Get-Volume -FilePath C:\antfly-test | Select-Object FileSystem, HealthStatus
C:\antfly-test\antfly.exe standalone --host 127.0.0.1 --port 8080 --health false `
  --storage-engine lite --storage-path C:\antfly-test\data.aflite `
  --data-dir C:\antfly-test\runtime --fsync true
```

Keep the acknowledgment ledger on a separate controller machine. Reach the
server over a private connection or tunnel; do not expose the test HTTP API
publicly. Start an ongoing write stream from that controller:

```sh
python3 tools/windows/durability_probe.py write --url http://127.0.0.1:8080 \
  --ledger /path/on/controller/acknowledged.jsonl --count 0
```

After some acknowledgments, abruptly reset **only the disposable test VM**
while the write stream is active. The controller will exit on a lost request;
its ledger includes only successful `full_index` responses. Restart the same
server/database, restore the tunnel, then run:

```sh
python3 tools/windows/durability_probe.py verify --url http://127.0.0.1:8080 \
  --ledger /path/on/controller/acknowledged.jsonl
```

The verifier requires every acknowledged document's body hash and full-text
entry to survive. A last interrupted request may survive without acknowledgment
and is permitted. Stop the server and run `antfly lite check` afterward. Record
the binary hash, Windows build, volume type, reset mechanism, ledger, server
logs, and check result. Repeat with fresh ledgers/databases around table creation
and compaction. Lite qualification does not qualify the separate LSM engine.

The focused suite builder above replaces the old overlay-specific manual
commands. `gce_hardlink_qualify.ps1` fetches `tests.zip` from the private bucket
named by `antfly-artifact-bucket` metadata, requires NTFS, publishes status
through guest attributes and uploads `results.json`. Enable guest attributes
and grant its service account access only to that temporary bucket; delete the
VM and supporting resources after collecting evidence.

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
- Native backup pinning uses the repository-owned executor. Focused backup
  tests pass; LSM backup recovery across abrupt resets remains unqualified.
- Untested: vector or hybrid search, enrichments, the distributed
  (Raft) runtime, TLS client calls (CryptoAPI trust store), and long-running
  or concurrent load.
- Lite files written by v0.2.x (format version 2) do not open on main. Convert
  them through the `/db/v1` API.
- The ONNX, CUDA and BLAS inference backends are untested. Metal is not
  applicable.
- Release plumbing (`scripts/release/platforms.json`, `.zip` archives, signing)
  is untouched.
