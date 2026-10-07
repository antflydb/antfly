# Request-local read-index waits: contract evidence

Native ReleaseSafe `raft-read-gate-test sql-test` passes all 54 build steps:
269 embedded SQL tests, 118 coordinator SQL tests and 13 read-gate tests.
Pinned Nix/glibc GNU AArch64 runs the same 400 named tests and reports all
51 build steps succeeded. Its compiler also reports 2.31 GB peak usage against
a declared 2 GiB compile estimate; the complete log retains that diagnostic.
This run is not evidence of passing the compile memory estimate. Runtime
memory limits remain unchanged.

The six read-index schedules use a real controlled event wait through the
production enrichment read gate: success, cancellation, deadline, shutdown,
I/O failure and revoked authority. They preserve request identity and deadline,
allow ordinary and protected control work while blocked, and retire scheduler
operations, slots and metadata. They do not exercise a real DataServer quorum
or apply delay. Two additional barrier cases verify stale and already-safe reads
never yield, missing contextual support fails closed, and unavailable backends
retain their typed availability failure.

The checked native callback qualifier separately compiles its callback owner
on both platforms. Three wait fixtures and all 25 error ABI fixtures pass,
including the newly transported read-safety/backend availability details.
Adjacent JSON records source hashes, commands, owner hashes and zero exits.

Production CLI compilation, actual DataServer wait integration, retained-owner
poison callback execution and whole API qualification of this slice remain
separate gates. No automatic scheduler defaults are established by this receipt.

The integration snapshot adds the missing generated control-catalog read-fence
export, rejects an unsupported serverless runnable policy at bootstrap, and
adds the actual DataServer owner-lock wait fixture. GNU AArch64 SQL/read-gate
qualification was rerun against that snapshot and passes all 400 named tests
and 51 build steps. The Linux SQL compiler reservation is now 3 GiB based on the
observed 2.31 GB peak; the 384 MiB runtime estimate is unchanged. The corrected
run has no compile memory overrun. The regenerated embedded boundary covers
737 production sources without server coordination imports. Both checked
callback receipts were rerun against the corrected control catalog.

`initial-contract-source-hashes.json` pins the earlier native/Linux contract
logs. `source-hashes.json` pins the current integration snapshot. Native SQL/read-gate contracts also pass against the updated snapshot: all
400 named tests and 54 build steps, without skips or leaks. Actual DataServer
owner-lock fixture execution remains pending. Its owner-lock fixture holds resumption after a real
polling sleep; it does not simulate a complete quorum or apply delay.
