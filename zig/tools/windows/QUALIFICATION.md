# PR #987 follow-up qualification

The review branch merges PR head `ce319462af8b` with main `e6d4ce9bbc71`.
During qualification, `origin/main` advanced to `b79388a7d6`, including the
embedded-source layout refactor. These results remain pinned to the merged
`e6d4ce9bbc71` snapshot; the newer main layout is not included in this branch.
Follow-up source commit `99003f02a2` replaces whole-output Windows LSM buffering
with bounded staging and flushes published Windows files after rename.
Commit `4cc39651e3` extends staging to the production borrowed-executor path.

## Memory and publication checks

The staging writer uses a 64 KiB buffer and a 64 KiB CRC scratch buffer. Its
integration test publishes an 8 MiB output through an 80 KiB fixed allocator,
checks replacement visibility and abort cleanup, and retains owned I/O runtimes
after storage shutdown. The runtime bridge's borrowed `IoStorage` also uses
bounded staging on Windows; its executor must outlive the sink.
Boundary-crossing header patches and CRC ranges pass.
A truncated staging read prevents subsequent append and publication. A mocked
Windows publication flush fails observably and still closes its handle.

Native macOS storage tests: 36 passed, 1 Windows-only test skipped. Windows
ReleaseFast storage tests: 16 passed, 21 POSIX tests skipped, including the
borrowed-executor bounded-write and authority checks.
Staging: 2 passed on both platforms.
Object publication: 1 Windows test passed, 1 POSIX test skipped; the macOS
objectstore suite passed 77 tests with 2 skips before the Windows-only test
was added. CrossOver executed the Windows binaries successfully.
A separate diagnostic with 1,000 thread-pool dispatches and eight background
sleepers passes under CrossOver; it does not reproduce the HTTP stall.
Two diagnostics pass 1,000 dispatches/completions through the actual
executor bridge between separately compiled Windows archives, with deadline
cancellation and either the polling or protected completion wait. Both pass
on native Windows as well as CrossOver.

Commit `deefe3fb98` changes HTTP batch completion to block on the future instead of polling
every millisecond. Cancellation protection prevents `Future.await` from
forwarding caller I/O cancellation into mutation work before its durability
is known; the request token still controls the batch's safe visibility waits.
Its cancellation regression passes on macOS and CrossOver and fails when
the protection is removed. This change alone does not establish the cause
of the sustained-write stalls.

## Native Windows reset checks

The disposable GCE runner uses `windows-server-2022-dc-v20260909`, Windows
Server build 20348, an NTFS boot volume, an e2-standard-4 machine and an 80 GB
pd-balanced boot disk. It has no external IP; HTTP is reached through IAP.
Compatibility, staging, storage and publication-flush executable exit codes
are all zero on native Windows.

The Debug application SHA-256 is
`9bd35fcb285d97c1375c3aaa6475fcc51131cb6b4dca6d424ee8340507b164df`.
It runs Lite with `--fsync true`. A separate macOS controller fsyncs a JSONL
ledger only after successful `full_index` responses, then verifies each body
hash and full-text entry after `gcloud compute instances reset`.
This is a [hard reset](https://docs.cloud.google.com/sdk/gcloud/reference/compute/instances/reset),
without a graceful guest shutdown.

| Run | Requested payload bytes | Acknowledged writes | Recovery |
| --- | ---: | ---: | --- |
| 1 | 4096 | 13 | All bodies and search entries survived |
| 2 | 4096 | 12 | All bodies and search entries survived |
| 3 | 1024 | 17 | All bodies and search entries survived |

The first two reset tests followed a stalled request. The third reset
interrupted an active stream. An interrupted request may commit without being
acknowledged; the verifier deliberately permits that. The first ledger was
also verified again after the second reset.

An offline native `lite check` after the recovery runs exits zero:
`valid: true`, file size and valid prefix both 1,429,504 bytes, zero tail bytes,
506 records and no reported issue.

Local evidence is retained under `/private/tmp/antfly-pr987-ntfs-results`:
the three acknowledgment ledgers, binary manifest, recovery-results JSON,
Windows minidumps and symbolized stacks. These contain only generated test
data. The Debug application also passes the standard CrossOver HTTP smoke:
64 concurrent queries across hard kill/reopen, followed by `lite check` with
`valid: true` and zero tail bytes.

## Updated native Debug run

With bounded borrowed writes and protected batch completion, the Debug SHA-256
is `5615babfd7fd3e345220275beb6c24037375c2068b1ebe0b92e5cafe4d9363c8`.
All eight focused native executables exit zero, including the borrowed writer,
cancellation regression and both executor-bridge diagnostics.

A 4096-byte stream reached 31 acknowledgments without a timeout before an
active-stream hard reset. All 31 bodies and full-text entries verify after
recovery. Offline `lite check` exits zero with `valid: true`, file size and
valid prefix both 1,949,696 bytes, zero tail bytes, 781 records and no issue.
The standard CrossOver smoke also passes again with 64 queries, hard-kill
recovery and zero tail bytes.

## Executor ownership fix

Commit `557c43a153` fixes the batch offload's executor selection. The handler
used a raw `BackendRuntime` pointer to rebuild a `Threaded` vtable in the API
archive, bypassing the already imported durable executor. On Windows, the
archives have separate parked-worker wakeup and thread-local state. Dispatch
and completion must call the owning executor through the imported view.
The existing selector also preserves intentionally unavailable imported views.

A reconstructed-vtable diagnostic hangs under CrossOver. A new regression
compiles its owner and borrower separately, parks the owning workers before
64 dispatches, and awaits every completion through the borrow. It passes on
macOS, CrossOver and native Windows. The earlier diagnostics kept background
workers active and did not expose the parked-worker case.

The fixed Debug application SHA-256 is
`bb9ab2b2b64e3ef0a1a26b3a87c05975893ae73e292c2bbedee4380408cc4b72`.
Its CrossOver extended smoke completes all 64 indexed writes, verifies all
64 body hashes and full-text entries after hard-kill recovery, runs 64
concurrent queries, and passes `lite check`: valid prefix/file size 1,699,840,
zero tail bytes and no issue. The full Debug build passes 46/46 steps.

A larger CrossOver run completes 1,000 indexed writes and verifies all 1,000
bodies and full-text entries after hard-kill recovery. Its final offline check
is valid with file size/prefix 14,229,504 bytes, zero tail bytes and no issue.

The native fixed Debug run completes 149 acknowledgments without a timeout
before an active-stream hard reset. All 149 bodies and full-text entries
verify afterward. All nine native executables exit zero. Offline integrity
checking exits zero with valid prefix/file size 4,571,136, zero tail bytes
and no issue. See `status-owner-debug.json`, `acknowledged-owner-debug.jsonl`
and `check-owner-debug.json` in the retained evidence directory.

## Final optimized build

The executor-fixed ReleaseFast application SHA-256 is
`38684ad67827ae13442d55cda905964374c1c3b6eee91d0a34e62ea1c8035e6e`.
The complete build passes 46/46 steps. Its CrossOver workload completes 1,000
indexed writes and verifies all 1,000 bodies and full-text entries after
hard-kill recovery. All 64 concurrent queries pass. Offline integrity checking
is valid with prefix/file size 14,360,576, zero tail bytes and no issue.

The native ReleaseFast run reaches 141 acknowledgments without a timeout
before an active-stream hard reset. All 141 body hashes and full-text entries
verify afterward. All nine focused executables exit zero on the new boot.
Offline `lite check` exits zero with `valid: true`, file size and valid prefix
both 6,557,696 bytes, zero tail bytes, 3,072 records and no issue. The retained
evidence includes `acknowledged-owner-release.jsonl`,
`status-owner-release-recovered.json`, `verify-owner-release.log` and
`check-owner-release.json`.

Before this fix, the consistent ReleaseFast binary
`83d377bc2f8ff1d6069cef34a2e6d1ded71762cde89132c3ae02326d2cf2e071`
stalled after 8 acknowledgments natively and 4 under CrossOver. All 8 native
acknowledgments survive its hard reset; offline integrity checking exits zero
with valid prefix/file size 2,150,400, zero tail bytes and no issue. Its native
snapshot has the HTTP handler awaiting batch completion, idle executor
workers, and metadata/maintenance threads waiting on the Lite store mutex.

## Follow-up code review

Review found the same executor reconstruction in HTTP SQL execution and the
pgwire statement, stream and disconnect paths. They now use the imported
durable executor and protect the completion join from caller cancellation.
New dispatch regressions distinguish the raw and imported executors and reject
an explicitly unavailable imported view without falling back to raw I/O.
All 18 focused SQL/pgwire/imported-view tests pass on macOS. The production
Windows ReleaseFast API archive builds successfully (14/14 steps). These SQL
changes have not been rerun through the native Windows application.

Two existing Windows object-store tests fail before path normalization:
pagination returns no nested entries, and prefix download returns
`InvalidObjectKey`. Directory walker paths are now converted to '/' object
keys before prefix matching and download lookup. Both tests pass afterward.
The Windows filesystem suite has eight passes and two remaining failures
under CrossOver; the macOS filesystem suite has 14 passes and one Windows-only
skip. No filesystem test is skipped to conceal these failures.

Vector-block abort cleanup constructed a native Windows '\\' path while
publication used a Lite-compatible '/' path, leaving rejected blocks behind.
Cleanup now uses the publication path constructor. The existing
`owned staged base removes blocks after pre-CURRENT rejection` regression
fails under CrossOver with the original code and passes with the fix. It also
passes on macOS. `windows_vector_test.zig` provides a focused test root.

The two remaining CrossOver filesystem failures are `filesystem get pins
metadata and body across atomic path replacement` and `filesystem GET keeps
one generation when publication replaces or deletes the object`. Replacing a
file while its reader remains open returns `AccessDenied`. Zig already requests
`FILE_RENAME_POSIX_SEMANTICS`, whose [Windows contract](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information)
allows existing readers to retain the replaced file. [Wine's implementation](https://github.com/wine-mirror/wine/blob/master/server/fd.c)
rejects an open destination. These are observable Wine limitations; native
Windows object-store snapshot/replacement behavior remains to be qualified.

## Additional path and cancellation review

The Windows vector/catalog path helper used a string join, which introduced a
leading slash for an empty root and doubled a trailing root separator. A new
checkpoint-path regression fails before the fix (`block-1-2.afvb` becomes
`/block-1-2.afvb`). Both helpers now use a shared standard path join and convert
Windows separators to '/'. Empty roots, trailing separators and Windows drive
paths preserve the standard join's semantics. The shared helper regression
passes on macOS and Windows Debug under CrossOver. The checkpoint-path and
rejected-block cleanup tests pass on macOS and Windows Debug and ReleaseFast
under CrossOver (three tests in each focused run, including the test root).

Pending I/O cancellation could also prevent an atomic writer's abort from
deleting its staging file. A controlled cancellation regression observes one
orphaned file before the fix. The streaming writer now blocks cancellation
only while deleting staging on abort or pre-publication failure, restoring
protection before releasing any owned runtime. The full storage suite passes
on macOS (37 passed, one Windows-only skip) and Windows Debug and ReleaseFast
under CrossOver (17 passed, 21 POSIX skips). This includes the new cancellation
regression and the existing bounded-memory, publication and executor-authority
checks.

These additional fixes have not been rerun through the native Windows
application or hard-reset workload. The native results and binary hashes above
remain evidence for their explicitly identified earlier builds. The two Wine
open-reader replacement failures and remaining qualification limits are
unchanged.

## Lite index staging review

The preceding cancellation fix covered the LSM streaming writer, while Lite
index storage has a separate physical staging writer. Its abort still deleted
the named staging file through cancellable I/O. A new controlled regression
forces the writer to spill, leaves cancellation pending until abort, and finds
an orphaned `.aflite-write-*` file with the original cleanup. Lite cleanup now
blocks cancellation around deletion and restores the previous protection.
The regression passes after the fix on macOS and Windows Debug and ReleaseFast
under CrossOver. No partial logical index record is published.

The complete focused Lite index suite has 24 passes on macOS. Windows Debug
and ReleaseFast each have 23 passes and one failure, with no skips. The failing
test is `lite native staged atomic writes bound heap and survive concurrent
commits and vacuum`: `replaceWithPreparedGeneration` returns `AccessDenied`
while renaming the prepared generation over the open database. This exposes
the same Wine open-destination replacement limitation documented above in
the object-store tests. The failure occurs at vacuum publication, before the
test can exercise publication of the staged index writer after vacuum.
Native Windows Lite vacuum/replacement remains unqualified. The test is
retained without an unlink-before-rename workaround or a skip.

`windows_lite_index_test.zig` supplies a small test root for this suite. These
results do not extend the earlier native Windows reset qualification to the
latest source changes.

## Vacuum image cleanup review

`VacuumImage.deinit` also deleted its unpublished prepared database through
cancellable I/O. A controlled Windows regression leaves cancellation pending
at cleanup and observes that the prepared path still exists afterward. Cleanup
now blocks cancellation during deletion and restores protection before closing
the prepared file and releasing any owned runtime. The original live database
and its document remain usable after the discarded image is removed.

The three cancellation cleanup regressions now wait for cancellation at an
event and re-arm it immediately before cleanup, replacing the previous timed
release. This guarantees pending cancellation without relying on scheduling
within a 50 ms window. All three pass on macOS and Windows Debug and ReleaseFast
under CrossOver. The focused root now explicitly includes native Lite tests.

A six-test run, including failed index imports and corrupt vacuum input, passes
all six tests on macOS. Windows Debug and ReleaseFast each have five passes and
one failure, with no skips: `lite native streaming vacuum rejects corrupt
values before publication` fails at its final successful-vacuum attempt after
the corruption has been repaired. The failure is again `AccessDenied` at
open-database replacement. Together with the earlier Lite index vacuum test
and two object-store tests, four executed tests expose Wine's replacement
limitation. Native Windows vacuum and object-store replacement behavior still
need verification. No replacement workaround or test skip was introduced.

The new vacuum cleanup fix has not been included in a native Windows
application or hard-reset qualification run.

## Backup inventory review

Windows directory walkers return native backslash separators. Backup seal
inventories rejected a nested file as `InvalidBackupSeal`; a new nested-file
regression reproduces that failure before the fix. Generation inventory
collection and complete-inventory restore matching also used raw walker paths
against portable '/' manifest paths. All three now normalize trusted walker
output before inventory validation or matching. External manifest path
validation remains strict, including rejection of backslashes and extra files.

Two new regressions pass on macOS and Windows Debug and ReleaseFast under
CrossOver: nested backup seal creation/reopening, and physical generation
inventory finalization/validation/materialization with nested run paths. The
latter also verifies rejection of an undeclared run and a backslash manifest
path. These tests use generated placeholder payloads; they qualify inventory
handling, not recovery of a real LSM checkpoint.

The complete focused backup suite passes 11 tests on macOS, with zero skips or
leaks and its one expected error log. Windows Debug and ReleaseFast each pass
six tests and fail five, with zero skips or leaks. All five failures originate
in `std.Io.Threaded.dirHardLink`, whose Windows implementation in this Zig
version immediately returns `OperationUnsupported`. The failing tests cover
explicit generation pins, shared vector acceleration capture, generation
capture/materialization, materialization receipts and in-place mutation
detection. The last test also reports its absent expected error log because
pinning fails before it reaches the mutation check.

This is a Windows runtime capability gap, distinct from Wine's open-file
replacement limitation. Native backup capture that requires hardlinks remains
blocked with this I/O runtime even after the path fixes. Supporting it requires
an owning Windows I/O implementation of hardlinks that preserves source/pin
identity and caller authority. A copy fallback or a direct Win32 bypass was
not introduced. The focused root is `windows_backup_test.zig`; no backup test
was skipped. The path fixes have not been rerun on native Windows.

## Windows hardlink implementation

The preceding backup blocker is resolved by the overlay's new owning
`Io.Threaded` implementation. Both directory-relative and file-handle hardlinks
use `NtSetInformationFile(FileLinkInformation)` inside the runtime, with its
existing Unicode path conversion, directory handles, syscall cancellation and
error mapping. `ReplaceIfExists` is false. Pins preserve file identity rather
than copying bytes, and a borrowed I/O vtable retains authority over the call.
The generator still requires exact Zig source anchors and stages changes
before replacing an overlay.

Once pin creation succeeded, the generation capture test exposed a second
Windows gap: the stable WAL-prefix reader returned
`UnsupportedNativeStorageRuntime`. Windows now retains one `Io.File` and its
owning `NativeFdPermit`, reads only requested ranges and closes the handle
before releasing the executor. A regression replaces the path and shuts down
the storage owner, then verifies that the reader still observes the original
file generation. Windows has no POSIX cache hints or coalescing in this reader.

CrossOver Debug and ReleaseFast each pass all three direct hardlink tests,
all 11 backup tests (zero skips/leaks, one expected error log), and 18 storage
I/O tests (21 POSIX-only skips). The direct tests cover Unicode names, separate
relative source/destination handles, inode/size/mtime identity, destination
conflicts, file-handle links, source removal, caller authority and pending
cancellation with no destination publication. macOS storage I/O passes 37
tests with two Windows-only skips. Overlay safety tests pass 2/2; Zig formatting
and whitespace checks pass.

A private GCE Windows Server 2022 VM (image
`windows-server-2022-dc-v20260909`, build `10.0.20348.0`, NTFS,
`e2-standard-4`, 80 GB boot disk) ran all six focused executables. Both Debug
and ReleaseFast passed: hardlinks 3/3; backups 11/11, zero skips/leaks and one
expected error log; storage I/O 18 passes, 21 POSIX-only skips and zero failures.
Every process exited 0 and its SHA-256 matched the prepared manifest.

| Native executable | SHA-256 |
| --- | --- |
| `hardlink-debug.exe` | `065cff3f840c7ed955e403ad48bcf6dcb9881623406e1e58a10c6f86b3cec0f8` |
| `hardlink-release.exe` | `4ac3c39c999dc886f95009403e4c797aecef665033f43a09a00b97f7c4cbe0ac` |
| `backup-debug.exe` | `d26fc654db2c2ff26d1034959c401ab8af7fe9be2f84618708a2ef343a4ce5d0` |
| `backup-release.exe` | `ccf69bef06e720555eedd43f890bc79ea8268109d8ea5f10a17bba382d6f62eb` |
| `storage-debug.exe` | `c6f2179a885ede5a4fb728fabde8efe6eae4e07605aba4b7bcbabc0146695e95` |
| `storage-release.exe` | `3db6a92ca8332e5041539e9893cbdb607b1ab47d8cc47c194b34006e7cc3ebf1` |

Evidence is retained at
`/private/tmp/antfly-pr987-hardlink-native-results/final-results.json` and
`manifest.json`; source hashes are in `qualified-source.json`. These tests include the backup inventory path corrections and
stable WAL reader. They do not qualify native object-store/vacuum replacement
or backup recovery after cache loss. The four earlier Wine replacement
failures remain outside this focused run.

The initial PowerShell runner lost process exit codes and serialized enriched
`Get-Content` strings into an unnecessarily large result. The corrected runner
owns a .NET process, drains its two output pipes asynchronously and captures
plain strings and reliable exit codes. The same six hashed binaries were rerun
successfully before collecting the final evidence.

The hardlink qualification VM `antfly-pr987-hardlinks`, its auto-delete boot
disk, private artifact bucket `antfly-pr987-hardlinks-20261006`, service account,
subnet and network were deleted. Independent resource listings verified their
absence. No firewall rules or tunnels were created for this focused run.

The full Windows ReleaseFast application rebuild passed 46/46 steps with
`ZIG_LIB_DIR=/private/tmp/antfly-pr987-wine10-zig-lib`, ONNX disabled and BLAS off.
All runtime archives and the executable used that same overlay. The executable
at `/private/tmp/antfly-pr987-hardlink-release/bin/antfly.exe` has SHA-256
`510c114a7c6fc333d6ea64e05d5b89e1d6f521b96419914461609484d09cce82`.

Its CrossOver smoke test passed 64 concurrent queries across initial startup
and reopening after forced termination. All 64 acknowledged 4 KiB documents
and their full-text entries were verified after reopening. `lite check`
reported `valid=true`, zero tail bytes and no issue. Logs and the database are
retained at `/private/tmp/antfly-pr987-hardlink-app-smoke`; the build log is
`/private/tmp/antfly-pr987-hardlink-build.log`. This application workload uses
Lite under Wine; it does not extend the earlier native hard-reset results to
this new binary.

## Backup pin cancellation cleanup

Review found that `PinnedGeneratedArtifacts.deinit` ignored a canceled tree
delete and discarded its pin paths. A deterministic macOS regression left the
hardlink in place before the fix, retaining the immutable generation on disk.
The same review regression passed under CrossOver before the fix.

Pin-set destruction and the three failed-pin-creation unwind paths now share
`cleanupPinTree`. It blocks cancellation only during tree deletion and restores
the prior protection before file leases can release their owning I/O runtime.
Normal materialization remains cancelable. The regression re-arms a real task
cancellation immediately before destruction, verifies removal of the pin tree
and preservation of the source bytes, and confirms cancellation remains pending
after cleanup.

The focused backup suite passes all 12 tests on macOS and Windows Debug and
ReleaseFast under CrossOver, with zero skips, failures or leaks and one expected
error log. Formatting and whitespace checks pass. This cleanup follow-up has
not been included in a native NTFS run or a full application rebuild; the earlier
native and application results above qualify their specified source snapshots.

## Snapshot staging cancellation cleanup

The DB snapshot exporter still discarded staging-path ownership after a
cancelable tree deletion. Normal snapshot construction, sealed export,
temporary portable decoding and staging-directory creation unwind now use a
shared `snapshot_staging.cleanup` helper. It protects only tree deletion and
restores the prior cancellation protection before subsequent cleanup runs.
Staging creation was extracted into the same module for focused fault injection;
its unique-name, collision-retry and parent-sync behavior is preserved.

Two regressions cover recursive deletion of copied payloads under pending
cancellation and creation unwind after an injected parent-sync failure with
cancellation pending. Both also verify cancellation is restored; the first
checks that a neighboring published file is untouched. Both fail on macOS
when the protection is removed, leaving the tree behind. With protection,
all 14 focused backup/staging tests pass on macOS and Windows Debug and
ReleaseFast under CrossOver (zero skips, failures or leaks; one expected error
log). Zig formatting and whitespace checks pass. No new native NTFS run was
performed for this staging follow-up.

The full Windows Debug application build passed 46/46 steps using the same
Wine-compatible Zig overlay, with ONNX disabled and BLAS off. Its executable
at `/private/tmp/antfly-pr987-staging-app/bin/antfly.exe` has SHA-256
`56cb490cc5ec51a2f2053a78676097763b5c4d3dc0eb5d1471cb43fb16ae1272`.
The CrossOver smoke test verified all 32 acknowledged documents and their
full-text entries after forced termination and reopening, passed 64 concurrent
queries across both runs, and reported `valid=true`, zero tail bytes and no
issue from `lite check`. Evidence remains in
`/private/tmp/antfly-pr987-staging-app-smoke` and
`/private/tmp/antfly-pr987-staging-app-build.log`. These checks include the pin
cleanup and snapshot staging follow-ups. The accumulated review changes target
PR #987's `dovinmu/antfly:experimental/windows-build` branch.

## Cleanup

The disposable VM, auto-delete boot disk, artifact bucket, service account,
IAP firewall rule, subnet and network were deleted after the final integrity
check. Resource listings confirm their absence. Both local IAP tunnels were
stopped. Generated evidence remains in the local temporary directory above.
At completion of native qualification, all review changes were committed
locally and no branch had been pushed.

## Remaining qualification limits

A hypervisor reset tests loss of guest cache; it does not remove power from
the physical disk/controller. Windows directory sync remains a no-op, so new
ancestor directories and deletion remain unqualified. The HTTP reset workload
uses Lite; it does not establish LSM compaction crash durability. Focused LSM
writer tests establish bounded memory and publication behavior. These bounded
workloads do not qualify an endurance run or a supported Windows port.


## Main integration and native replacement qualification (2026-10-07)

Merge `c3845962d6` incorporates `origin/main` at `f2e6054632` into the review
branch. Directory rename detection moved all six new focused test/staging
files to `pkg/antfly-embedded/src/`; current README commands use these paths.
A freshly generated Zig overlay matches the previously tested overlay.
Its safety tests pass 2/2 and CrossOver compatibility tests pass 5/5.

After the merge, macOS passes backup/staging 14/14, object-store filesystem
10/10 and selected Lite index/vacuum tests 25/25. CrossOver Debug and
ReleaseFast each pass backup/staging 14/14, filesystem 8/10 and Lite 23/25.
The four failures remain `AccessDenied` when replacing an open destination.

A new disposable Windows Server 2022 VM (build `10.0.20348.0`, NTFS,
`e2-standard-4`, 80 GB boot disk) executed all six hashed binaries below.
Both Debug and ReleaseFast pass backup/staging 14/14, filesystem 10/10 and
Lite 25/25, with no failures or skips. Backup/staging reports no leaks and
one expected error log. All six processes exited 0 and matched the manifest.
This resolves native qualification of the four open-destination replacement
failures and of the snapshot staging cancellation regressions. No atomic
replacement workaround or test skip was needed; the failures are specific to
CrossOver. It does not extend the earlier hard-reset evidence to this build.

| Native executable | SHA-256 |
| --- | --- |
| `backup-Debug.exe` | `e232b9ae693a655d251c40fe4ff3919b4ffbcbf40c8cdbd7514f9fe2865f3576` |
| `backup-ReleaseFast.exe` | `2f76199e539792b56c1bc1cd224896f32e4063f29ea531cc27756edfd88f18b4` |
| `filesystem-Debug.exe` | `3969cec0c28f56d85b9dd53e57524dd164a46b3314d78e4ea682f1bb19395d39` |
| `filesystem-ReleaseFast.exe` | `68f5f4ffc00186ab9fcfbf3d07a71e076e3c8328f2ecdf54ba0d9bd2d7f46aeb` |
| `lite-Debug.exe` | `9628bdefe82d2cbbf2f7abb99fb85547e5c125abb61b3526035eb097f51e99ac` |
| `lite-ReleaseFast.exe` | `be87bb7bb719c5931c7c6bdc06fe0d04dae79604efa16aee4c8b37714a3d96c2` |

Native evidence and source hashes are retained in
`/private/tmp/antfly-pr987-merged-native-results/` (`final-results.json`,
`manifest.json`, `qualified-source.json`, `resources.json`). The VM, boot disk,
artifact bucket, service account, subnet and network were deleted afterward.
No ingress firewall rules or tunnels were created.

The merged full Windows Debug application passes all 46 build steps, with
ONNX disabled and BLAS off. Executable SHA-256:
`9560875ae71396351c75ccb77b5811d33f1edcf4cddad7a7cfd17272036b3f4e`.
CrossOver verifies all 32 acknowledged documents and full-text entries after
forced termination/reopening, passes 64 concurrent queries across both starts,
and reports `valid=true`, zero tail bytes and no integrity issue. Evidence:
`/private/tmp/antfly-pr987-merged-app-build.log` and
`/private/tmp/antfly-pr987-merged-app-smoke/`.

The CI policy test incorrectly rejected main's existing automatic Apache
license source check. It now explicitly permits that read-only workflow and
checks its unprivileged PR trigger, permissions and credential handling;
expensive suites remain gated. All 92 CI script tests pass. Hosted policy CI
also tests its executing main revision, so the isolated assertion fix must
land on main before that job can pass. Hosted tests still require a non-draft
PR and a human `/ci run <full-head-sha>` approval under the repository policy.

## Repository-owned Windows backend with stock Zig (2026-10-07)

The platform executor, C compatibility APIs, dynamic loading, test I/O and
callers now live in the repository. The Zig-library overlay and generator
have been removed. Windows process entry points explicitly select the platform
executor instead of accepting Zig's default `std.process.Init.io`. Diagnostic
fallbacks use `platform.debug_io`; borrowed runtime I/O keeps its caller's
vtable and executor authority.

All qualification builds use the installed, unmodified Zig 0.17.0 library.
Stock `std/Io/Threaded.zig` SHA-256:
`1a770001e309f24c8c58a9fdb3c095994c454cbfa9dba9d4f1dede2f134eeac7`.
Repository-owned Windows executor SHA-256:
`2c063f1b3d3d4796aece947002fdc10193617d9faec4890fa7008aa44f5f9b67`.
The checked-in adaptation retains Zig's MIT notice and upstream provenance;
it must be maintained alongside future Zig API updates.

Windows Server 2022 (10.0.20348.0), NTFS, passed all 14 qualification
executables in Debug and ReleaseFast: 152 passed, 42 POSIX-only skips,
zero failures and zero leaks. Every downloaded executable hash matched the
local build manifest. These focused executables qualify the Windows backend;
the subsequent process-startup integration fix is covered by the application
smoke below. CrossOver reproduced four destination-replacement failures per
mode (72 passed, 21 skipped, four failed, no leaks); all four passed on native
Windows. Tests retain these failures and do not skip them under Wine.

| Executable | SHA-256 |
| --- | --- |
| `compat-Debug.exe` | `c7d5f14bb0beb1d257471cf7a1abfeb633df64ef58f5ac710d5ed4f645dbd271` |
| `hardlink-Debug.exe` | `b7c792022464c0fc516bb1a9405c31b0642e5f5ee54288d4973bdaa52022e4bc` |
| `backup-Debug.exe` | `2710e6307910310879a4e404a6cbf904581b5aae6bcdc0fa368d90382add50d6` |
| `filesystem-Debug.exe` | `758b0d8f973ad31265bafe839231994c2ba9280df3d4bd632540cd3f052a8b46` |
| `lite-Debug.exe` | `a4bd940a67376fd862b61c49453617588ba5d17207c0c8797e8bde3ff35f48bb` |
| `storage-Debug.exe` | `8e8e4dfdcf3b9ab496c37cd23fc1e0d86ca389f4ec9bf50c53e023244b9cd579` |
| `bridge-Debug.exe` | `4f44f74ffd724ac8e8f4ff47c0fe7674ac13e91676aaf9639b2adace33f1f4b9` |
| `compat-ReleaseFast.exe` | `52314f75a3b0e3d0415cc8da59384f7323fa2c0c3e538506d0b04a90dca2e2ba` |
| `hardlink-ReleaseFast.exe` | `a47c9fb1d012c050412e9db2495029f9648bc1530c7285a0f08eb523b97ce2b6` |
| `backup-ReleaseFast.exe` | `d291c608f46e74c8076e06fbfe4a48ed175d9d90b1a80c00ab9973d6345101fa` |
| `filesystem-ReleaseFast.exe` | `4dc84bc0fa0a63e6719ed01b3e7767290e314f293305dfea21fa86e30a7eaf23` |
| `lite-ReleaseFast.exe` | `147eb1f2560d0f180897489c4c390500daff20bd1a72eb59cb97e3eb9615c0e9` |
| `storage-ReleaseFast.exe` | `d85e3a3e4361452c76ed0e408f2d22e15dfe4fa377c4515f8c64073feb4a3bcc` |
| `bridge-ReleaseFast.exe` | `b1b4917db1299976e61c978e0fd34e56aa1698c61a74c7b8579133448c827081` |

Native results, manifest, CrossOver output and verified resource cleanup are
retained in `/private/tmp/antfly-pr987-stock-windows-tests/`. The VM, boot disk,
private artifact bucket, service account, subnet and network were deleted and
absence verified. No inbound firewall rules or tunnels were created.

The final full Windows Debug application passes 46/46 build steps with ONNX
disabled and BLAS off. Its SHA-256 is `fc119c5de3b74c6c689a93981c059b752e82f0ab948a9da1446036b9b112f181`.
CrossOver confirms 32 acknowledged documents and full-text entries survive
forced process termination, passes 64 concurrent queries across both starts,
and reports `valid=true`, zero tail bytes and no integrity issue. Evidence:
`/private/tmp/antfly-pr987-stock-app-final-graph-build.log` and
`/private/tmp/antfly-pr987-stock-app-smoke-qualified/`.

Native macOS focused qualification passes 87 tests with two platform-specific
skips. Standalone Raft passes 427 tests, VOPR passes 167, structlog passes nine,
and platform passes all 14 build steps (13 Zig tests and 13 Python process
checks). Both native and browser module-boundary audits pass. Apache source
boundary checks verify 1,896 files, license-header checks pass, and CI policy
scripts pass 92 tests. The existing human CI approval gate still applies;
the main-branch policy assertion fix is isolated in PR #996.

These results do not extend the earlier native abrupt-reset/physical-durability
qualification to this new application binary. Windows remains experimental;
CrossOver cannot establish loss of the Windows OS cache.

## Current main integration and owned-source publication (2026-10-07)

Merged `origin/main` at `e19ac3c0830f3b3cd304a4ad116a47f93ab603cb`,
preserving its bounded Lite transaction and remote-lake changes. Resolved four
source conflicts and migrated its new executor callers to platform ownership.
Normalized 36 incoming license headers, including two Apache engine sources
that incorrectly carried ELv2 notices. The Apache boundary now verifies 1,905
sources.

The merged tests exposed an optional authenticated-page buffer treating a
caller allocator limit as fatal. It now authenticates with bounded worker
scratch when either allocator or cache admission rejects that optional page.
The retained source/navigation peak in the 2 MiB segment test is 750 bytes.
The existing cache-performance assertion incorrectly assumed physical reads
remained uncached; the test now distinguishes source calls from disk reads,
verifies the same identifiers, and retains its sixteenfold source-call reduction
and 256 KiB cap (8,192 source calls versus three scoped calls in macOS Debug).

The new native lease capability is available on Windows independently of
memory mapping. Atomic publication also advertises its owned-source result;
this fixes a compaction crash that dereferenced an absent mapping callback.
A regression verifies publication preserves immutable bytes through replacement
and storage-owner shutdown without a mapping capability. New lake builder
permission calls select the target API, and temporary paths use the platform
host-environment directory instead of hard-coded `/tmp`.

The final native Windows Server 2022/NTFS run passes all 14 Debug and ReleaseFast
executables: **168 passed, 50 capability-specific skips, zero failures and zero
leaks**. All guest executable hashes match this final manifest:

| Executable | SHA-256 |
| --- | --- |
| `compat-Debug.exe` | `54eb133c57294c59ddaa7689ef545107f09769562204b14ff7a1eab4d7d4219b` |
| `hardlink-Debug.exe` | `217e89adaac74a9495d8e75279bf237242bab6dcaf53c858c95f382d12b203a4` |
| `backup-Debug.exe` | `31aeb856ddd8c539f1e2b438110d1448b31fbe6aa022a481f31e4d710df0c2fb` |
| `filesystem-Debug.exe` | `cb4d74b308c0439d574ff426094a0137b8c72b7a8bcd675deba4c0c5ca87c20f` |
| `lite-Debug.exe` | `4b7cbc2ee53593336a829fc0211ae521d334bf0928556b48084f138e5f52861a` |
| `storage-Debug.exe` | `319c19255e79489b91af62e017e49c2e6163341315d6ad3e89ffc3e97c295c92` |
| `bridge-Debug.exe` | `3a9b3114f0f0eb533e6f4eb6cd05f4224e5709592a065d2171e82c310bdc80cc` |
| `compat-ReleaseFast.exe` | `9163cc8ce13149495d5030dd4de174a28ca42605e3c1d19e336878fd342a3111` |
| `hardlink-ReleaseFast.exe` | `8163279b0d7eea537b2e7fa83ec02ec8f98f5ca16691bf16d942c3e875f84600` |
| `backup-ReleaseFast.exe` | `523dcc931983a079c22c1fc3f86f85ff2a5d93b92acf658ac8f522ddc681cc6c` |
| `filesystem-ReleaseFast.exe` | `50787b46701a27d73399db5cd0a9d3020774ae2d8d2aff406380b7ace2309dfb` |
| `lite-ReleaseFast.exe` | `b28fdb94f694c585ac065d66739f4e88213b5a17ad18d401f69fa3f01c8d5d3a` |
| `storage-ReleaseFast.exe` | `c197fa137bc2a0f83d97ab66f721c7e196f11744f1d3e48e9743cc978e96b71f` |
| `bridge-ReleaseFast.exe` | `dc359340c26765d16ae3ac73f71a9d1e95d0518b741c6bc3c86152663e96cfdd` |

Evidence is retained in `/private/tmp/antfly-pr987-stock-merged-windows-tests/`
(`native-results.json`, `manifest.json`, `qualified-source.json`, `resources.json`).
CrossOver's final Lite tests in both modes report 29 passed, four mapped-artifact
skips and four replacement failures, with zero leaks. The new publication test
passes in both modes; all four failing replacement tests pass on native NTFS.
These runs preserve the Wine failures in the test results.

The corresponding macOS focused checks pass 99 tests with two Windows-specific
skips; the final Lite executable passes 37/37. The application and native/browser
module-boundary audit build pass **59/59 steps** with stock Zig, ONNX disabled
and BLAS off. Final Windows executable SHA-256: `8ced3f3c1e881f6df53571e7914a4bc01efc94c2c395986251b2670167c8aae2`.
CrossOver then verifies all 32 acknowledged documents and full-text entries
after forced termination, passes 64 concurrent queries across both starts,
and reports a valid database, no integrity issue and zero tail bytes.
Evidence: `/private/tmp/antfly-pr987-stock-app-merged-publication-build.log` and
`/private/tmp/antfly-pr987-stock-app-merged-qualified-smoke/`.

Both disposable qualification environments have been deleted. No ingress
firewall rules or tunnels were created. The stock Zig library remains unchanged;
the historical physical-durability qualification and experimental Windows
status retain the limits stated above.


## Positional reads and stream cancellation (2026-10-07)

This follow-up applies to PR head `1032869517` using the same unmodified
Zig 0.17.0 library. The platform implementation now owns overlapped stream
requests and Wine worker cancellation events, translates positional-read
failures into CRT `errno`, and preserves shared file offsets. Model-file
readers retain an overlapped handle instead of reopening on each read.

Windows Server 2022 build `10.0.20348.0` on native NTFS passes all 14 Debug and
ReleaseFast executables: **176 passed, 50 capability skips, zero failures and
zero leaks**. Every executable hash matches `manifest.json`. The nine
compatibility tests pass in each mode, including idle-read cancellation and
socket reuse, task-level deadlines, backpressured write cancellation, stale
`errno`, unchanged file offsets, concurrent reads and retained overlapped
handles. Windows Threaded Batch socket concurrency remains unavailable;
the deadline test uses task selection.

CrossOver passes ten executables in both modes: **90 passed, 42 capability
skips, zero failures and zero leaks**. The existing filesystem/Lite replacement
cases are covered by the native NTFS run. macOS platform unit and process
checks pass. The full Windows application build and native/browser module
boundary audits pass **59/59 steps**.

The rebuilt application SHA-256 is
`ac60ad8ab6e5a985c76c930df30d94be6a35fec2b461f6a0a066e0965be3b097`.
Its CrossOver HTTP smoke passes 64 queries across forced termination/reopen,
verifies all 32 acknowledged documents and full-text entries, and reports
`valid: true` with zero tail bytes. This run does not add native application
VM-reset durability qualification.

Evidence is retained in `/private/tmp/pr987-fix-review-tests` (hashed manifest,
native and Wine results, source hashes and verified cleanup),
`/private/tmp/pr987-io-fix-app-build.log`, and
`/private/tmp/pr987-io-fix-app-smoke`. Both disposable VM environments and
all temporary boot disks, buckets, service accounts, subnets and networks
have been deleted; their absence was verified.


## Model-file ownership and idle accept cancellation (2026-10-07)

This follow-up fixes two failures reproduced at PR head `14792aed6a`.
The model reader discarded the positional file's asynchronous mode, then
reconstructed a synchronous `File` and reached `unreachable` on a pending
read. Windows descriptors now retain the complete `std.Io.File` through
reads, mapping, parallel prefetch, stat and close. POSIX descriptors retain
their existing representation.

Wine accepts now use an owned `AcceptEx` request with zero receive length,
a per-operation event and the executor's cancellation wakeup. Cancellation
uses `CancelIoEx` for that request and drains completion before releasing
its socket, address storage and event. A successful accept adopts the
listener context before returning the peer address. The native AFD accept
path and the installed Zig standard library are unchanged.

The default qualification archive adds a model-file consumer suite covering
whole-file and region reads, admission/bounds failures, mmap and parallel
prefetch. The compatibility suite adds 16 idle accept cancellations with
listener reuse and server-first traffic, plus cancellation of a parked accept
group before listener closure.

All four focused Debug/ReleaseFast executables pass on both CrossOver and
Windows Server 2022 build `10.0.20348.0` on NTFS: **22 passed, zero skips,
zero failures and zero leaks** per environment. Native executable hashes
match the manifest. macOS passes all seven model-file tests, both focused
optimization modes, and all 14 platform build steps including 13 Python
process checks. The full Windows application and native/browser module
boundary audits pass **59/59 steps**.

The application SHA-256 is
`aeaf3dbcb9ba77aa15c9f2aba48964f2e60447d85beb1c5f361284004a26df11`.
Its CrossOver smoke passes 64 queries across forced termination/reopen,
verifies all 32 acknowledged documents and full-text entries, and reports
valid storage with zero tail bytes. This is not an additional native
application VM-reset durability run.

Evidence is retained in `/private/tmp/pr987-ownership-fix` (manifest,
source hashes, native/Wine results and cleanup records),
`/private/tmp/pr987-ownership-native`,
`/private/tmp/pr987-ownership-c-file-native.log`,
`/private/tmp/pr987-ownership-app-build.log`, and
`/private/tmp/pr987-ownership-app-smoke`.


## Main merge, Winsock connections and Linux clocks (2026-10-07)

PR head `3573538aa0` is merged with `origin/main` at
`cd69bdadfb556f21b8b9c54075b097a20d3449b3` in merge commit `6f0b23614c`.
The lake-test conflict was formatting only on the PR side; the resolved
Python AST matches main exactly, preserving its new text/highlight and
filtered-limit coverage. Locked TypeScript dependencies were installed to
run the repository's normal commit formatter.

Wine connection-oriented sockets now use a provider-resolved `ConnectEx`
request on their already-bound socket, an owned event, and the executor's
cancellation wakeup. Cancellation targets and drains that request before
releasing storage; success applies `SO_UPDATE_CONNECT_CONTEXT`. Datagram
peer association retains the local Winsock connect operation. Native
Windows retains its existing AFD connection path.

Winsock failures use explicit typed mappings for resets, refusal, timeout,
unreachable networks/hosts and cancellation. Unknown socket errors are
logged numerically on connect, accept, stream and generic socket paths;
they are never formatted as unrelated Win32 enum values. Both original
live probes now pass: a reset returns `ConnectionResetByPeer`, and a stalled
connection cancels and joins instead of hanging.

POSIX clocks and nanosleep again use `std.posix.system`, preserving Linux
syscalls when libc is absent. Platform checks now run clock tests and always
compile their x86_64 Linux variant without libc, including on macOS hosts.
The two clock tests also pass in a disposable local ARM64 Linux container
with libc absent, networking disabled and a read-only filesystem; that
container was removed. macOS platform checks pass **17/17 build steps**,
including the clock tests and 13 Python process checks.

All six focused Debug/ReleaseFast executables pass on CrossOver and Windows
Server 2022 build `10.0.20348.0` on NTFS: **28 passed, zero skips, zero
failures and zero leaks** per environment. The archive now includes dedicated
Winsock mapping/tracing tests. The compatibility suite covers task-level
outbound-connect deadlines and repeated cancellation near submission and
while pending. Environments rejecting the documentation address immediately
skip that pending-connect case; it ran successfully in both qualification
environments. All native executable hashes match the manifest.

The merged Windows application and native/browser module boundary audits
pass **59/59 build steps**. Application SHA-256:
`d12177d8a6fb68c1e9f592502035be07550e45e4b626a81f6320f9195debe831`.
Its CrossOver smoke passes 64 queries across forced termination/reopen,
verifies all 32 acknowledged documents and full-text entries, and reports
valid storage with zero tail bytes. This does not add native application
VM-reset durability qualification. The installed Zig library is unchanged.

Evidence is retained in `/private/tmp/pr987-connect-fix-tests` (manifest,
source hashes, native/Wine results and cleanup records),
`/private/tmp/pr987-connect-platform-tests.log`,
`/private/tmp/pr987-connect-linux-clocks.log`,
`/private/tmp/pr987-connect-fix-app-build.log`, and
`/private/tmp/pr987-connect-fix-app-smoke`.

## Platform I/O namespace (2026-10-07)

`platform.Io` now owns backend exports: `Threaded` selects the repository-owned
Windows executor and the stock executor elsewhere; `Evented` retains the existing
qualified Linux/macOS selection. `Context` aliases `std.Io`, preserving borrowed
vtable dispatch and compatibility with standard APIs. Repository imports use
`platform`, including files that previously imported the same module twice.
`lib/runtime` re-exports `Io` and keeps its existing `Threaded` compatibility alias.
The benchmark uses its configured module import instead of importing the same
platform root into a second module.

The namespace regression checks assert context/backend type compatibility and
execute a clock-reading task through an owning executor. They run in the platform
build and the new `io-namespace` Windows suite. Validation evidence:

- macOS platform tests and I/O benchmark compilation: 22/22 build steps passed;
  the platform tests include the two new namespace checks and existing Python
  process-lifecycle checks.
- CrossOver Debug and ReleaseFast namespace, compatibility/cancellation, and
  independent archive executor bridge suites: 28 passed, no skips, failures,
  or leaks. Evidence: `/private/tmp/pr987-io-namespace-tests`.
- ARM64 Linux namespace/task checks compiled without libc and executed in the
  existing Alpine container: 2/2 passed.
- macOS HTTP and objectstore suites: 675 passed, 11 integration/optional skips.
  The initial sandbox run denied loopback listeners with EPERM; rerunning with
  socket access passed.
- Full Debug Windows application and native/browser module boundary audits:
  59/59 build steps passed. The Windows executable SHA-256 is
  `0ab7eb468763c3ac7712b7e77ba2315306cfc344824939c2a3dfa2128a7ad97b`.
- CrossOver application smoke: 64 concurrent queries passed across two runs;
  all 32 acknowledged documents and full-text entries survived forced termination
  and reopen. Evidence: `/private/tmp/pr987-io-namespace-app-smoke`.
- A normalized comparison of 575 mechanically migrated Zig files found no
  unrelated edits. Explicit API, benchmark, and local-name changes were reviewed
  separately; formatting and whitespace checks passed.

This namespace refactor was exercised under CrossOver; it does not add a new
native Windows VM run to the earlier NTFS qualification evidence.

## Main #1003 merge and composed platform identity (2026-10-07)

Merged `origin/main` at `0c3b38667a6d7cd8589104f12dc343dd32acbef7` with
no textual conflicts. The external lake API tests exposed a second platform
module supplied by standalone VOPR. Composed graphs now canonicalize platform
imports when their target and physical source file match, preserving different
adapters and cross-target dependencies. Physical path comparison covers relative,
absolute, and symlinked roots; authored file metadata participates in configuration
caching. The namespace test graph models a standalone dependency with an absolute
platform root and checks that it shares the composition's executor type.

Validation after the merge and binding fix:

- Embedded lake reader/SQL tests: 216 passed, no skips, failures, or leaks.
- Focused external lake API and local-owner tests: 51 passed, no skips,
  failures, or leaks, including the new scoring and projection regressions.
- Dedicated lake integration and local-owner suites: 154 passed, no
  skips, failures, or leaks, including packed text ranges and native GC.
- macOS platform suite: 19/19 build steps passed, including the duplicate-module
  regression and Python process-lifecycle checks.
- CrossOver namespace interoperability tests: 4 passed across Debug and
  ReleaseFast, no skips, failures, or leaks.
- Windows Debug application and native/browser module boundary checks:
  59/59 build steps passed.
- CrossOver application smoke: 64 queries passed; all 32 acknowledged documents
  and full-text entries survived forced termination and reopen, with a valid
  recovered file and no invalid tail.

Windows executable SHA-256:
`9beb1ef5ae44f2932e2a8d53331a6127b99eab95cf9f247006a96e48c706e1d4`.
Logs and hashes use `/private/tmp/pr987-main1003-*`. No new native Windows VM
was created for this merge.

## Standalone consumers, Wine DNS and PJRT regression (2026-10-07)

Standalone platform bindings preserve each artifact's libc policy. JSON's
actual external consumers compile without libc for Linux and freestanding
WASM. Raft and structlog use platform clocks and compile their Linux unit
suites without libc. The public `Clock.real()` consumer also compiles without
libc, retaining the Linux syscall path instead of calling a C clock symbol.

HTTPX exports one module with explicit target/optimization and its JSON import.
A Windows consumer verifies that its borrowed observer and HTTP runtime share
one platform executor type. Objectstore forwards target/optimization to every
standalone dependency and binds one platform module across the resulting graph;
its normal test steps include Windows Debug and Linux ReleaseFast compilation.
The standalone and composed native objectstore suites each pass 77 tests with
three capability/integration skips. Platform's 22 build steps pass, including
public clock checks and 13 Python process checks.

Windows system-directory lookup uses `GetSystemDirectoryW`, avoiding private
PEB fields that Wine omits. Wine datagrams use owned overlapped Winsock requests
with cancellation and completion draining. Its external DNS lookup uses native
`GetAddrInfoW` workers: canceled requests own provider storage independently of
caller tasks/executors, with admission capped at 32 outstanding requests,
including canceled work still completing. Native Windows retains `DnsQueryEx`.
The DNS DLL/procedure names are terminated, and canonical-name conversion checks
length and character range before copying into the caller's fixed buffer.

CrossOver Debug and ReleaseFast each pass five namespace tests (including DNS
cancellation, executor teardown, admission bounds, and datagram reuse), two
external DNS tests, and the public real-clock test. These checks do not extend
native Windows/NTFS qualification to this source snapshot; earlier native
results remain pinned to the binaries documented above.

The fresh review also reproduced the PJRT unit-broadcast failure on main
`0c3b38667a`. The implementation correctly reshapes an all-unit tensor to a
scalar before broadcasting it; the older regression assumed a direct broadcast
at a fixed instruction index. It now follows the multiplication operands and
verifies the scalar reshape, empty broadcast dimensions, output shape and
source operand. The standalone PJRT suites pass 25/25 tests. A PJRT plugin is
not installed, so this is builder/unit qualification, not plugin execution.
