# Dynamic CPU team reservation on retained execution

Scheduler.resizeTeam and Execution.resizeTeam let a running operation reserve a
bounded CPU team before publishing parallel work. Team demand must include all
concurrently runnable callers and children. All children of the old team must
join before resizing, checkpointing, parking or finishing their parent. This
local trusted capability does not yet exist on the checked cross-module ABI.

Growth returns the old CPU grant and queues the full requested demand with the
existing weighted scheduling and bounded wait policy, avoiding a parent holding
CPU while waiting for its own additional slots. Registry/count/byte reservations
remain on the same stable execution. Denied reentry stops CPU work and preserves
those reservations until owning finish. Shrinking returns excess slots
synchronously and dispatches eligible queued work. Invalid demand and already
closed admission preserve the current running grant; this does not confer any
new accepted-write completion authority. Original deadline/cancellation remains
in force and original stopped causes cannot be cleared to resurrect a team.

All 33 scheduler/bounded-admission contracts pass on native macOS and pinned
Nix/glibc GNU AArch64. Regressions cover growth under contention, health/control
progress, retained operation/byte accounting, success generation invalidation,
shrinking, invalid demand/control capacity, denied reentry and shutdown. A
deterministic clock regression exposed reuse of an expired earlier queue timeout
when shrinking a running team. The failed run and exact before-fix source are
preserved; synchronous shrink now clears the old queued-wait marker.

This is scheduler foundation. Transaction participant workers, numerical teams,
the checked runtime ABI and retained child allocation ownership still need this
capability wired through their actual production paths. Existing participant
fanout remains unaccounted. Accepted-write completion transfer and physical IO
parking remain separate required work. No production/default qualification is
claimed; pooling remains disabled. The previous full production pair runs from
a different frozen worktree and excludes these new team methods.
