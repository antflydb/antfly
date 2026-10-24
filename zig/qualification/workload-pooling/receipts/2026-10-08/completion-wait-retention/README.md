# Completion waits retain published consumers

Scheduler.wait previously treated an expired active completion as unpublished
initial admission and called finish. The before-fix regression observed finished
instead of stopped while a live 64-byte consumer still existed: 39 tests passed
and one failed. Its exact source and native log are preserved. Initial wait now
checks retained completion state under the scheduler lock and uses waitRetained
for active completion. Ordinary unpublished admission keeps its original failure
behavior. A completion deadline failure keeps its stopped root, operation and
byte charge until consumers are joined and the owning caller finishes.

All 40 scheduler/admission regressions pass on native macOS and pinned Nix/glibc
GNU AArch64. The new lifetime regression verifies retained ownership even when a
legacy caller uses initial wait. The other new regression uses two actual bounded
Io.Threaded tasks: it reserves the independent task before completion activation,
parks the root, allows health progress, cancels and joins the foreground task,
closes admission, then lets the independent task resume/checkpoint/finish. The
two tasks share an executor instance but have distinct actual cancellation scopes.
A second schedule cancels the independent task while it waits before any storage
work; ownership remains charged until that task joins and finishes its root.
An atomic handoff flag assigns cleanup to the parent before transfer and to the
child afterwards, including failure of the child's initial wait. Executor and
fixture lifetimes outlast both joins. These tests perform no physical WAL/Raft
write and do not prove a durable recovery transfer or node-wide thread accounting.

Thirty real-owner, SQL-team, worker-output and cleanup contracts also execute and
pass per platform after the wait path changed. Production write parking remains
withheld. Required integration still includes completion task/resources reserved
before actual acceptance, checked native-boundary capability exposure, physical
storage/quorum waits, ambiguous-outcome recovery and authoritative durable
transfer. No production consumer activates completion yet; pooling stays disabled.
The supplemental inherited production proposal is relative to 3c62b08a6f and
includes both process fixture files. Whole-node/default and numerical-team
qualification remain outstanding.
