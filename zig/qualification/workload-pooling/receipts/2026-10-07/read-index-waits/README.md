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

The live GNU ARM64 storage archive compiler subsequently reported a
21,815,246,848-byte peak, above its previous 20 GiB production reservation.
The native GNU ARM64 ReleaseSafe storage compile reservation is now 26 GiB,
providing more than 25% measured headroom. Other profiles and all executor
runtime limits remain unchanged. Four memory-profile tests pass on macOS and
pinned Nix/glibc GNU AArch64. Adjacent claim logs and the separate source hash
record this build-budget correction; they do not establish runtime qualification.
The active fixture jobs still use the source-frozen pre-correction build policy
and retain its overrun diagnostic. A corrected build graph must be verified
separately after those jobs finish.

Actual DataServer read-safety qualification passes all six selected contracts
and all 86 build steps on pinned Nix/glibc GNU AArch64. The four new owner-lock
schedules cover cancellation, the original deadline, shutdown and rejected
resumption authority. The real production polling sleep releases capacity;
the fixture holds resumption while the Raft owner mutex remains occupied.
Ordinary and protected work execute in the released slots. All schedules cancel
the real ReadIndex tracker registration and retire operations/slots/metadata.
Other contracts cover matching ReadState application, restart identity, frozen
proof lock exclusion and persisted-but-uninstalled snapshot rejection.

The exact artifact then passes again with two CPUs, 2 GiB memory and no extra
swap, without invoking a compiler. Its verifier requires six passing tests and
all four failure markers, checks the recorded artifact hash, and reports zero
exit status. No tests skip or leak. The source-frozen integration manifest pins
this evidence; the compiler reservation correction is recorded separately.
This proves the owner-lock failure schedules, not a complete stalled quorum or
apply schedule with live peer transport. Native execution of the new owner-lock
fixture and the full corrected CLI/API graph remain separate gates.

Routed-read/retained-owner Linux qualification passes 14 local implementation,
22 physical implementation and 83 consumer tests (119 total), with all 84 build
steps reported succeeded. Both retained-owner lifetime contracts run, including
nonnull poison wait callbacks on immediate and delayed snapshot opens: the
physical opener sees no borrowed request callback. Complete logs retain compiler
reservation overruns for storage (21,815,246,848 bytes against 20 GiB) and the
routed implementation tests (17,479,122,944 bytes against 8 GiB). This run does
not establish passing those compiler reservations.

All three exact artifacts then pass again with two CPUs, 2 GiB memory and no
extra swap or compiler. The verifier checks artifact hashes, all 119 tests and
both lifetime fixture names, and each artifact has an explicit zero exit status.
No tests skip or leak.

The corrected build snapshot reserves 26 GiB for the measured storage profile
and 21 GiB for native GNU ARM64 ReleaseSafe routed-read implementation tests;
both include more than 25% measured headroom. Its graph parses but Zig refuses
to admit the 26 GiB storage step against this Docker VM's 25,159,036,928-byte
available-memory budget. The adjacent insufficient-VM log is negative evidence,
not a passing corrected-graph receipt. The two corrected build-source hashes
pin that attempted graph. Whole corrected CLI/API graph qualification remains
required on an environment that can admit the declared compile reservation.
