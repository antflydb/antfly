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

## Cleanup

The disposable VM, auto-delete boot disk, artifact bucket, service account,
IAP firewall rule, subnet and network were deleted after the final integrity
check. Resource listings confirm their absence. Both local IAP tunnels were
stopped. Generated evidence remains in the local temporary directory above.
All review changes are committed locally; no branch was pushed.

## Remaining qualification limits

A hypervisor reset tests loss of guest cache; it does not remove power from
the physical disk/controller. Windows directory sync remains a no-op, so new
ancestor directories and deletion remain unqualified. The HTTP reset workload
uses Lite; it does not establish LSM compaction crash durability. Focused LSM
writer tests establish bounded memory and publication behavior. These bounded
workloads do not qualify an endurance run or a supported Windows port.
