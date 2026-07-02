\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.

-------------------------- MODULE AntflyHAFailoverSafety --------------------------
(*
  Focused failover safety model for Antfly hot-standby HA.

  This sibling model exists because the broader AntflyHAReplication model
  previously made the two hardest failure modes structurally impossible:
  acknowledged-write loss and split-brain writes after promotion.

  Implementation/design correspondence:
  - HA.md requires fencing for promotion and warns that promotion is not
    automatically safe.
  - Sync acknowledgement records prove that some standby durably received or
    applied a write, but a failover is safe only if the promoted standby has
    every acknowledged write that the policy promises to preserve.
  - The old primary must lose write authority before the promoted standby can
    accept writes on the new timeline.

  Commit-mode durability (HA.md): the durability contract is parameterized by
  commit mode. A write acknowledged under a sync mode (remote_write /
  remote_apply) must survive promotion; a write acknowledged under async mode
  is acknowledged without standby receipt evidence and MAY be lost on
  failover. The positive model therefore contains reachable states where an
  async-acked write is absent from the promoted standby and no invariant
  fails; only sync-acked loss is a violation (see
  PromotedNodeHasAllSyncAckedWrites and the BuggyPromoteMissingAck mutant).

  Deliberate omissions:
  - record payload bytes, HTTP admin tokens, base-backup manifests, and exact
    WAL storage are abstracted to bounded write identifiers.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS BuggyPromoteMissingAck, BuggyOldPrimaryWrite, BuggyAckWithoutReceipt

Standbys == {"standbyA", "standbyB"}
Nodes == {"oldPrimary", "standbyA", "standbyB"}
PromotedNodes == Standbys \cup {"none"}
Writes == 1..2

VARIABLES
    primaryLsn,
    oldPrimaryWritable,
    promotedNode,
    promotedWritable,
    oldTimeline,
    newTimeline,
    fenceHeld,
    fenceOwner,
    oldHas,
    standbyHas,
    syncAcked,
    asyncAcked,
    ackSource,
    splitBrainWrites

vars == <<primaryLsn, oldPrimaryWritable, promotedNode, promotedWritable,
          oldTimeline, newTimeline, fenceHeld, fenceOwner, oldHas, standbyHas,
          syncAcked, asyncAcked, ackSource, splitBrainWrites>>

NodeHas(n) ==
    IF n = "oldPrimary" THEN oldHas ELSE standbyHas[n]

Init ==
    /\ primaryLsn = 0
    /\ oldPrimaryWritable = TRUE
    /\ promotedNode = "none"
    /\ promotedWritable = FALSE
    /\ oldTimeline = 1
    /\ newTimeline = 0
    /\ fenceHeld = FALSE
    /\ fenceOwner = "none"
    /\ oldHas = {}
    /\ standbyHas = [s \in Standbys |-> {}]
    /\ syncAcked = {}
    /\ asyncAcked = {}
    /\ ackSource = [w \in Writes |-> "none"]
    /\ splitBrainWrites = {}

AppendOldPrimary ==
    /\ oldPrimaryWritable
    /\ primaryLsn < 2
    /\ primaryLsn' = primaryLsn + 1
    /\ oldHas' = oldHas \cup {primaryLsn + 1}
    /\ UNCHANGED <<oldPrimaryWritable, promotedNode, promotedWritable,
                  oldTimeline, newTimeline, fenceHeld, fenceOwner, standbyHas,
                  syncAcked, asyncAcked, ackSource, splitBrainWrites>>

ReplicateToStandby(s, w) ==
    /\ s \in Standbys
    /\ promotedNode = "none"
    /\ w \in oldHas
    /\ w \notin standbyHas[s]
    /\ standbyHas' = [standbyHas EXCEPT ![s] = @ \cup {w}]
    /\ UNCHANGED <<primaryLsn, oldPrimaryWritable, promotedNode,
                  promotedWritable, oldTimeline, newTimeline, fenceHeld,
                  fenceOwner, oldHas, syncAcked, asyncAcked, ackSource,
                  splitBrainWrites>>

\* Sync-mode acknowledgement (remote_write / remote_apply): requires durable
\* standby receipt evidence unless the ack-bookkeeping mutant is enabled.
AckSyncFromStandby(s, w) ==
    /\ s \in Standbys
    /\ promotedNode = "none"
    /\ ~BuggyAckWithoutReceipt => w \in standbyHas[s]
    /\ w \in oldHas
    /\ w \notin syncAcked
    /\ w \notin asyncAcked
    /\ syncAcked' = syncAcked \cup {w}
    /\ ackSource' = [ackSource EXCEPT ![w] = s]
    /\ UNCHANGED <<primaryLsn, oldPrimaryWritable, promotedNode,
                  promotedWritable, oldTimeline, newTimeline, fenceHeld,
                  fenceOwner, oldHas, standbyHas, asyncAcked,
                  splitBrainWrites>>

\* Async-mode acknowledgement: the primary acknowledges the client without
\* waiting for standby receipt. Such writes carry no preservation promise
\* across failover; their loss is permitted by design (HA.md async RPO).
AckAsyncPrimary(w) ==
    /\ promotedNode = "none"
    /\ w \in oldHas
    /\ w \notin syncAcked
    /\ w \notin asyncAcked
    /\ asyncAcked' = asyncAcked \cup {w}
    /\ UNCHANGED <<primaryLsn, oldPrimaryWritable, promotedNode,
                  promotedWritable, oldTimeline, newTimeline, fenceHeld,
                  fenceOwner, oldHas, standbyHas, syncAcked, ackSource,
                  splitBrainWrites>>

FenceAndPromote(s) ==
    /\ s \in Standbys
    /\ promotedNode = "none"
    /\ ~BuggyPromoteMissingAck => syncAcked \subseteq standbyHas[s]
    /\ oldPrimaryWritable' = FALSE
    /\ promotedNode' = s
    /\ promotedWritable' = TRUE
    /\ fenceHeld' = TRUE
    /\ fenceOwner' = s
    /\ newTimeline' = oldTimeline + 1
    /\ UNCHANGED <<primaryLsn, oldTimeline, oldHas, standbyHas, syncAcked,
                  asyncAcked, ackSource, splitBrainWrites>>

PromotedAppend ==
    /\ promotedWritable
    /\ promotedNode \in Standbys
    /\ primaryLsn < 2
    /\ primaryLsn' = primaryLsn + 1
    /\ standbyHas' = [standbyHas EXCEPT ![promotedNode] = @ \cup {primaryLsn + 1}]
    /\ UNCHANGED <<oldPrimaryWritable, promotedNode, promotedWritable,
                  oldTimeline, newTimeline, fenceHeld, fenceOwner, oldHas,
                  syncAcked, asyncAcked, ackSource, splitBrainWrites>>

BuggyOldPrimaryAppendAfterPromotion ==
    /\ BuggyOldPrimaryWrite
    /\ promotedWritable
    /\ primaryLsn < 2
    /\ primaryLsn' = primaryLsn + 1
    /\ oldPrimaryWritable' = TRUE
    /\ oldHas' = oldHas \cup {primaryLsn + 1}
    /\ splitBrainWrites' = splitBrainWrites \cup {primaryLsn + 1}
    /\ UNCHANGED <<promotedNode, promotedWritable, oldTimeline, newTimeline,
                  fenceHeld, fenceOwner, standbyHas, syncAcked, asyncAcked,
                  ackSource>>

Next ==
    \/ AppendOldPrimary
    \/ PromotedAppend
    \/ BuggyOldPrimaryAppendAfterPromotion
    \/ \E w \in Writes: AckAsyncPrimary(w)
    \/ \E s \in Standbys:
        \/ FenceAndPromote(s)
        \/ \E w \in Writes:
            \/ ReplicateToStandby(s, w)
            \/ AckSyncFromStandby(s, w)

\* Liveness: failover eventually completes (no permanent stall). Replication
\* fairness is required because two sync acks can land on different standbys,
\* leaving every individual standby missing some acked write; fair replication
\* eventually gives some standby the full acked set (all state sets are
\* monotone and bounded), after which fair promotion must fire. Appends and
\* acks are deliberately unfair: liveness must not depend on more writes.
Fairness ==
    /\ \A s \in Standbys: WF_vars(FenceAndPromote(s))
    /\ \A s \in Standbys: \A w \in Writes: WF_vars(ReplicateToStandby(s, w))

Spec == Init /\ [][Next]_vars

\* Liveness-checked spec used by the positive config; mutant configs check
\* invariants only and use the unfair Spec.
FairSpec == Spec /\ Fairness

EventuallyPromoted ==
    <>(promotedNode \in Standbys)

TypeOK ==
    /\ BuggyPromoteMissingAck \in BOOLEAN
    /\ BuggyOldPrimaryWrite \in BOOLEAN
    /\ BuggyAckWithoutReceipt \in BOOLEAN
    /\ primaryLsn \in 0..2
    /\ oldPrimaryWritable \in BOOLEAN
    /\ promotedNode \in PromotedNodes
    /\ promotedWritable \in BOOLEAN
    /\ oldTimeline \in 1..2
    /\ newTimeline \in 0..2
    /\ fenceHeld \in BOOLEAN
    /\ fenceOwner \in PromotedNodes
    /\ oldHas \subseteq Writes
    /\ standbyHas \in [Standbys -> SUBSET Writes]
    /\ syncAcked \subseteq Writes
    /\ asyncAcked \subseteq Writes
    /\ syncAcked \cap asyncAcked = {}
    /\ ackSource \in [Writes -> (Standbys \cup {"none"})]
    /\ splitBrainWrites \subseteq Writes

\* Sync acknowledgements must carry standby receipt evidence. Async
\* acknowledgements deliberately carry none.
AckEvidenceExists ==
    \A w \in syncAcked:
        /\ ackSource[w] \in Standbys
        /\ w \in standbyHas[ackSource[w]]

\* The commit-mode-parameterized durability contract: only sync-acked writes
\* must survive promotion. Async-acked writes may be absent from the promoted
\* standby without violating safety.
PromotedNodeHasAllSyncAckedWrites ==
    promotedNode \in Standbys => syncAcked \subseteq standbyHas[promotedNode]

NoSplitBrainWritablePrimaries ==
    promotedWritable => ~oldPrimaryWritable

\* NOTE: true-by-construction diagnostic, not an independently testable
\* contract. FenceAndPromote establishes all three conjuncts in the same step
\* that sets promotedWritable, and no action retracts them, so no mutant of
\* this model can violate this invariant without also rewriting the promotion
\* action itself. It is kept as a regression tripwire for future edits to
\* FenceAndPromote (e.g. splitting fencing from promotion), not as evidence.
PromotionRequiresFence ==
    promotedWritable =>
        /\ fenceHeld
        /\ fenceOwner = promotedNode
        /\ newTimeline = oldTimeline + 1

NoSplitBrainWrites ==
    splitBrainWrites = {}

Safety ==
    /\ TypeOK
    /\ AckEvidenceExists
    /\ PromotedNodeHasAllSyncAckedWrites
    /\ NoSplitBrainWritablePrimaries
    /\ PromotionRequiresFence
    /\ NoSplitBrainWrites

=============================================================================
