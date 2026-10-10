# Private spill creation and owned wait results

Private spill directory acquisition and file creation/unlinking now release
runnable capacity around filesystem operations. Random-name generation and SQL
preparation stay in the CPU phase. Successful reacquisition is followed by the
backend checkpoint before publishing newly acquired handles. The serial-only
scheduled spill contract remains unchanged.

`execution_wait.runOwned` discards a successfully acquired result if resumption
fails. The spill wrapper also discards it if the post-resume checkpoint fails.
Cleanup closes file/directory handles through their original I/O owner, removes
private artifacts and blocks cancellation during cleanup. Failed unlinking attempts removal again under cancellation protection. No rows
are written before unlink succeeds. Cleanup
does not depend on a new scheduler grant. Ordinary teardown close/delete still
uses mandatory cleanup rather than a cancelable scheduler wait; broader cleanup
execution ownership remains required.

The full native Zig 0.17.0 ReleaseSafe SQL target passes all ten build steps,
269 embedded SQL tests and 118 coordinator SQL tests, without skips or leaks:

```sh
zig build sql-test -Doptimize=ReleaseSafe -j1 --summary all
```

Seven creation schedules use real private files/directories: success, failed
directory/file reacquisition, failed directory/file authority checkpoint, failed
begin, and filesystem error. Ordinary query and protected control entries execute
while the parent is resource-blocked. Close callbacks preserve native I/O userdata
and count actual handle retirement; directory iteration verifies no linked file
or leaked private directory remains. The fixture pauses ownership at resumption,
not inside the syscall. It is distinct from the existing ten positional-read/write
schedules, whose controlled I/O vtable stalls actual I/O; those also pass in the
same full SQL run. Original metadata charges retire to zero.

The standalone owned-result primitive passes all three named tests with pinned
Nix/glibc Zig 0.17.0 on GNU AArch64 in a two-CPU, 1 GiB container without extra
swap. Its new five-case fixture checks success, failed begin, failed I/O, failed
resumption and combined I/O/resumption failure, including exactly-once discard.
The full Linux SQL target also passes all nine build steps and all 387 named
tests without skips or leaks in a two-CPU, 3 GiB container without extra swap.
Both complete logs contain all seven creation markers and the ten stalled
positional read/write markers. The explicit production boundary audit covers
737 sources with no server coordination imports. Whole native API qualification of
these creation hooks also passes all 50 build steps and 75 selected tests
without skips or leaks. Full Linux API qualification also passes all 47 build steps and 75 selected
tests. Both exact GNU AArch64 artifacts were then rerun without invoking a
compiler in a two-CPU, 2 GiB container without extra swap; all 36 embedded and
39 coordinator tests pass with explicit zero exit statuses and no skips or leaks.
`linux-runtime-artifacts.json` records the exact paths, arguments, sizes and
SHA-256 hashes. This API snapshot is detached commit `adbc2df496` and precedes
the request-local read-index wait integration. Adjacent logs and source hashes preserve the
current evidence; no automatic defaults are qualified by this slice.
