# Private spill wait ownership

The full SQL corpus passes all 267 local and 118 server contracts on macOS
ReleaseSafe and pinned Nix/glibc GNU Linux ReleaseSafe, without leaks. The
736-source authored production boundary passes without server coordination imports.

One named spill fixture runs ten schedules: stalled positional reads and private
spill writes, each with success, cancellation, original-deadline expiry, scheduler
shutdown and I/O failure. Real files and the production spill buffering paths are
used; a controlled I/O vtable pauses the syscall boundary. Both ordinary and
protected control work run while I/O is blocked. Resumption queues behind held
capacity, metadata persists during the wait, deadlines are unchanged and spill
charges remain until file close. All ten cases appear in both platform logs.
The focused Linux container uses two CPUs, 2 GiB memory and no additional swap;
the full SQL container uses two CPUs and 3 GiB with no additional swap.

The initial focused target did not select the new fixture and is not qualifying
evidence. A strict direct selection caught this omission. Explicitly including
SQL spill tests in the SQL test root corrected it; the strict direct run selects
one named fixture, and full corpus runs include all ten case markers.

The backend capability and API adapter now hand off execution around private
spill reads/writes, reacquiring before CPU decoding or propagating an I/O failure.
Post-resumption backend checkpoints retain the pgwire credential check. CPU spill
children remain disabled with this single-worker wait capability. Native API
qualification for these hooks passes all 33 local and 38 API contracts without
skips, failures or leaks; all 50 build steps succeed. Full Linux API qualification
for these hooks now passes all 33 local and 38 API contracts, without skips,
failures, or leaks, and all 47 build steps. The source was the isolated
`d5baee40e5` snapshot with one build-only change: the GNU compiler RSS estimate
was reduced from 26 to 23 GiB after 26 GiB was rejected before compilation
because it exceeded available VM memory. This estimate remains above the
previous measured 22.45 GiB compiler peak; runtime limits are unchanged. Both
exact compiled artifacts subsequently passed again in two-CPU containers with
2 GiB memory and no extra swap. The rejected scheduling log is preserved. Storage/RPC and directory
waits, fresh policy/catalog fences, independent parallel child ownership, broader
workload scheduling and automatic default qualification remain incomplete.
