# Preauthorized retained completion phase

Execution can reserve completion options while its original write/control root
is still running and admission remains open. It requires an independent trusted
completion Io, a finite absolute deadline, no foreground cancellation callback
and a waiting capability. It may not enlarge the root's retained byte charge.
The reservation uses the existing bounded entry/operation/byte ownership; it
creates no fresh foreground or control admission. The independent Io and all
retained resources must remain valid until all consumers join or an authoritative
durable transfer completes. The scheduler cannot itself prove Io ownership or
a storage decision; those are mandatory integration obligations.

Activation also requires a live original foreground fence, a valid completion
fence, the prior reservation and open admission. It must precede any potentially
durable acceptance. Active completion replaces foreground scheduling options
with the reserved options and retains its original capacity partition. It can
park, fairly reenter, yield or resize after admission closes. Fresh admission
remains closed, and unactivated queued/parked requests stop as before. Completion
cannot promote itself into a different CPU partition. Deadline/reentry failure
keeps stopped operation/byte ownership until owning finish; later calls preserve
the stopped cause. CPU is released at real waits only after all old team children
have quiesced. The caller still owes joins or durable transfer on every exit,
including an ambiguous proposal outcome.

Native macOS and pinned Nix/glibc GNU AArch64 pass all 38 scheduler/admission
contracts. Five new regressions cover original cancellation/deadline expiry
followed by completion through close, fair queued completion behind a running
foreground root, rejected absent/unbounded/canceled/late activation, expired
completion retaining stopped consumers, and foreground CPU release dispatching
completion before foreground joins finish. The last case exercises both yield
and park; it exposed a missing dispatch in the closing foreground branches.
Thirty real-owner/SQL-team/worker-output/public-cleanup regressions also execute
and pass per platform after the entry layout changed. No owner result is a
physical WAL/Raft acceptance or independent completion-task lifetime proof.

No production consumer activates completion yet. SQL write team wait/checkpoint
hooks remain unavailable. Required next work includes independently owned
completion Io/resources reserved before SQL seal/participant acceptance, checked
boundary exposure, actual storage/quorum wait integration, authoritative recovery
transfer and cancellation/shutdown/unknown-outcome physical regressions. A durable
SQL sealed plan is a recovery obligation, not proof of a committed storage decision.
The supplemental proposal is relative to 4eae9c6da3 and includes inherited
production changes and both process fixtures. Pooling remains disabled; default,
whole-node, numerical-team and mixed-load qualification remain outstanding.
