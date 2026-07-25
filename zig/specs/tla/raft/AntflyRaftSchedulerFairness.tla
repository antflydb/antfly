---------------------- MODULE AntflyRaftSchedulerFairness ----------------------
(*
Code anchors:
  - lib/raft/src/runtime/scheduler.zig nextTickGroup/nextReadyGroup,
    nextRoundRobinGroup, tickBatch, and readyBatch.
What this proves:
  - Every active group is selected within one complete bounded scan for both
    ticking and Ready processing, even when another group remains hot.
  - Under weak fairness, every group continues to receive both kinds of work.
Deliberate omissions:
  - Quiesce/register churn, wall-clock duration, payload bytes, and transport
    backpressure. Those do not change the cursor-ordering contract modeled here.
State bounds:
  - GroupCount is three in the fast check; gaps saturate at GroupCount + 1.
Make targets:
  - AntflyRaftSchedulerFairness
  - AntflyRaftSchedulerFairnessBadTickHot
  - AntflyRaftSchedulerFairnessBadReadyHot
Correspondence tier:
  - Mature: focused scheduler tests exercise hot-group tick and Ready starvation.
*)

EXTENDS Naturals

CONSTANTS GroupCount, HotGroup, BuggyTickHot, BuggyReadyHot

ASSUME /\ GroupCount >= 2
       /\ HotGroup \in 1..GroupCount
       /\ BuggyTickHot \in BOOLEAN
       /\ BuggyReadyHot \in BOOLEAN

VARIABLES tickCursor, readyCursor, tickGap, readyGap

vars == <<tickCursor, readyCursor, tickGap, readyGap>>

NextCursor(cursor) == IF cursor = GroupCount THEN 1 ELSE cursor + 1

AdvanceGap(gaps, served) ==
    [g \in 1..GroupCount |->
        IF g = served
        THEN 0
        ELSE IF gaps[g] < GroupCount + 1 THEN gaps[g] + 1 ELSE gaps[g]]

Init ==
    /\ tickCursor = 1
    /\ readyCursor = 1
    /\ tickGap = [g \in 1..GroupCount |-> 0]
    /\ readyGap = [g \in 1..GroupCount |-> 0]

Tick ==
    LET served == IF BuggyTickHot THEN HotGroup ELSE tickCursor
    IN  /\ tickCursor' =
               IF BuggyTickHot THEN tickCursor ELSE NextCursor(tickCursor)
        /\ tickGap' = AdvanceGap(tickGap, served)
        /\ UNCHANGED <<readyCursor, readyGap>>

Ready ==
    LET served == IF BuggyReadyHot THEN HotGroup ELSE readyCursor
    IN  /\ readyCursor' =
               IF BuggyReadyHot THEN readyCursor ELSE NextCursor(readyCursor)
        /\ readyGap' = AdvanceGap(readyGap, served)
        /\ UNCHANGED <<tickCursor, tickGap>>

Next == Tick \/ Ready

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(Tick) /\ WF_vars(Ready)

BoundedTickGap == \A g \in 1..GroupCount : tickGap[g] <= GroupCount - 1
BoundedReadyGap == \A g \in 1..GroupCount : readyGap[g] <= GroupCount - 1

TickEventuallyEach == \A g \in 1..GroupCount : []<>(tickGap[g] = 0)
ReadyEventuallyEach == \A g \in 1..GroupCount : []<>(readyGap[g] = 0)

=============================================================================
