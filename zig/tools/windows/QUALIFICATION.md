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
disabled and BLAS off. Its SHA-256 is `c8de500b4ae9f48347646a3caabaf624daf86ee22b0c3530ac43815f999e67bc`.
CrossOver confirms 32 acknowledged documents and full-text entries survive
forced process termination, passes 64 concurrent queries across both starts,
and reports `valid=true`, zero tail bytes and no integrity issue. Evidence:
`/private/tmp/antfly-pr987-stock-app-final-startup-build.log` and
`/private/tmp/antfly-pr987-stock-app-smoke-final/`.

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
