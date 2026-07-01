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

--------------------------- MODULE AntflyDerivedReplay ---------------------------
(*
  Bounded model of the shipped primary-store derived replay contract.

  Implementation correspondence:
  - DocStore.writeReplayEntries writes the replay-all row and replay-all latest
    sequence, then writes per-hint lane rows and per-hint latest sequence keys
    only when a target hint is decoded for that record.
  - replay_source.zig primaryStoreForEachMatchingRecord scans only the requested
    hint lane. error.ReplayIndexUnavailable returns zero matches; there is no
    replay-all fallback in the shipped primary-store path.
  - DocStore.latestReplaySequenceForHint reads the per-hint latest key. That
    key, not the replay-all latest key, defines a hinted derived worker target.

  The previous model treated an empty/unavailable hint lane as requiring a
  replay-all fallback. That verified a different design. This model instead
  checks the safety boundary the code relies on: a hinted target may advance
  only when every matching replay-all row up to the per-hint latest sequence has
  a visible hint-lane row the worker can consume.

  Deliberate omissions:
  - payload bytes and hint-mask decoding are abstracted into atomic append modes.
  - retry/backoff and batch sizing are left to enrichment/runtime models.
  - all-lane-only rows with no per-hint latest are allowed; they are not part of
    the hinted worker target until a corresponding per-hint latest key exists.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyEmptyHintAdvance, Indexes, MaxSeq

KnownIndexes == {"dense", "fulltext"}
Seqs == 1..MaxSeq

VARIABLES
    journalSeq,
    replayAll,
    hintLane,
    hintLaneAvailable,
    latestHintMeta,
    truncateFloor,
    applied,
    appliedRecords,
    target,
    queryTarget,
    catchupActive,
    bulkSessionActive

vars == <<journalSeq, replayAll, hintLane, hintLaneAvailable, latestHintMeta,
          truncateFloor, applied, appliedRecords, target, queryTarget,
          catchupActive, bulkSessionActive>>

MatchingHint(i, lo, hi) ==
    {s \in Seqs:
        /\ lo < s
        /\ s <= hi
        /\ truncateFloor < s
        /\ s \in hintLane[i]
        /\ i \in replayAll[s]}

TargetedAllLane(i, lo, hi) ==
    {s \in Seqs:
        /\ lo < s
        /\ s <= hi
        /\ truncateFloor < s
        /\ i \in replayAll[s]}

Earliest(S, s) ==
    /\ s \in S
    /\ \A x \in S : s <= x

SafeToTruncate(nextFloor) ==
    \A i \in Indexes:
        \A s \in Seqs:
            /\ s <= nextFloor
            /\ s <= latestHintMeta[i]
            /\ i \in replayAll[s]
            => s \in appliedRecords[i]

Init ==
    /\ journalSeq = 0
    /\ replayAll = [s \in Seqs |-> {}]
    /\ hintLane = [i \in Indexes |-> {}]
    /\ hintLaneAvailable = [i \in Indexes |-> TRUE]
    /\ latestHintMeta = [i \in Indexes |-> 0]
    /\ truncateFloor = 0
    /\ applied = [i \in Indexes |-> 0]
    /\ appliedRecords = [i \in Indexes |-> {}]
    /\ target = [i \in Indexes |-> 0]
    /\ queryTarget = [i \in Indexes |-> 0]
    /\ catchupActive = [i \in Indexes |-> FALSE]
    /\ bulkSessionActive = [i \in Indexes |-> FALSE]

\* Normal hinted append: replay-all row, hint-lane row, and per-hint latest key
\* become durable together from the model's point of view. This is the contract
\* that keeps a hint-lane-only scan safe.
AppendHintedRecord(hints) ==
    /\ hints \in SUBSET Indexes
    /\ hints # {}
    /\ journalSeq < MaxSeq
    /\ journalSeq' = journalSeq + 1
    /\ replayAll' = [replayAll EXCEPT ![journalSeq + 1] = hints]
    /\ hintLane' = [i \in Indexes |->
          IF i \in hints THEN hintLane[i] \cup {journalSeq + 1} ELSE hintLane[i]]
    /\ latestHintMeta' = [i \in Indexes |->
          IF i \in hints THEN journalSeq + 1 ELSE latestHintMeta[i]]
    /\ UNCHANGED <<hintLaneAvailable, truncateFloor, applied, appliedRecords,
                  target, queryTarget, catchupActive, bulkSessionActive>>

\* All-lane-only records are representable because writeReplayEntries writes
\* the all lane before returning for unhinted payloads. In the good model they
\* carry no modeled target hint and therefore do not move per-hint latest
\* metadata. The expected-failure mutant below covers the dangerous case where
\* a matching hinted row exists without the corresponding hint-lane entry.
AppendAllLaneOnly ==
    /\ journalSeq < MaxSeq
    /\ journalSeq' = journalSeq + 1
    /\ replayAll' = [replayAll EXCEPT ![journalSeq + 1] = {}]
    /\ UNCHANGED <<hintLane, hintLaneAvailable, latestHintMeta, truncateFloor,
                  applied, appliedRecords, target, queryTarget, catchupActive,
                  bulkSessionActive>>

\* Expected-failure mutant: per-hint latest is advanced without the hinted row.
\* A worker using latestReplaySequenceForHint will target work that the
\* hint-lane-only scanner cannot see.
BuggyAppendLatestWithoutHint(i) ==
    /\ BuggyEmptyHintAdvance
    /\ i \in Indexes
    /\ journalSeq < MaxSeq
    /\ journalSeq' = journalSeq + 1
    /\ replayAll' = [replayAll EXCEPT ![journalSeq + 1] = {i}]
    /\ latestHintMeta' = [latestHintMeta EXCEPT ![i] = journalSeq + 1]
    /\ UNCHANGED <<hintLane, hintLaneAvailable, truncateFloor, applied,
                  appliedRecords, target, queryTarget, catchupActive,
                  bulkSessionActive>>

ToggleHintLaneAvailability(i) ==
    /\ i \in Indexes
    /\ hintLaneAvailable' = [hintLaneAvailable EXCEPT ![i] = ~@]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, latestHintMeta,
                  truncateFloor, applied, appliedRecords, target, queryTarget,
                  catchupActive, bulkSessionActive>>

StartBulkSession(i) ==
    /\ i \in Indexes
    /\ ~bulkSessionActive[i]
    /\ ~catchupActive[i]
    /\ bulkSessionActive' = [bulkSessionActive EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, applied, appliedRecords,
                  target, queryTarget, catchupActive>>

FinishBulkSession(i) ==
    /\ i \in Indexes
    /\ bulkSessionActive[i]
    /\ bulkSessionActive' = [bulkSessionActive EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, applied, appliedRecords,
                  target, queryTarget, catchupActive>>

ObserveReplayTarget(i) ==
    /\ i \in Indexes
    /\ ~catchupActive[i]
    /\ ~bulkSessionActive[i]
    /\ target[i] < latestHintMeta[i]
    /\ target' = [target EXCEPT ![i] = latestHintMeta[i]]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, applied, appliedRecords,
                  queryTarget, catchupActive, bulkSessionActive>>

StartCatchup(i) ==
    /\ i \in Indexes
    /\ ~catchupActive[i]
    /\ ~bulkSessionActive[i]
    /\ applied[i] < target[i]
    /\ catchupActive' = [catchupActive EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, applied, appliedRecords,
                  target, queryTarget, bulkSessionActive>>

ApplyHintMatch(i, s) ==
    /\ i \in Indexes
    /\ catchupActive[i]
    /\ hintLaneAvailable[i]
    /\ Earliest(MatchingHint(i, applied[i], target[i]), s)
    /\ applied' = [applied EXCEPT ![i] = s]
    /\ appliedRecords' = [appliedRecords EXCEPT ![i] = @ \cup {s}]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, target, queryTarget,
                  catchupActive, bulkSessionActive>>

\* The shipped primary-store scanner returns no matches when the requested hint
\* lane is unavailable or empty. This is safe only if per-hint latest metadata
\* cannot point at hidden all-lane-only matching work.
AdvanceWhenNoVisibleHintMatch(i) ==
    /\ i \in Indexes
    /\ catchupActive[i]
    /\ hintLaneAvailable[i]
    /\ MatchingHint(i, applied[i], target[i]) = {}
    /\ applied' = [applied EXCEPT ![i] = target[i]]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, appliedRecords, target,
                  queryTarget, catchupActive, bulkSessionActive>>

FinishCatchup(i) ==
    /\ i \in Indexes
    /\ catchupActive[i]
    /\ applied[i] = target[i]
    /\ catchupActive' = [catchupActive EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, applied, appliedRecords,
                  target, queryTarget, bulkSessionActive>>

AdvanceQueryTarget(i) ==
    /\ i \in Indexes
    /\ ~catchupActive[i]
    /\ ~bulkSessionActive[i]
    /\ queryTarget[i] < applied[i]
    /\ queryTarget' = [queryTarget EXCEPT ![i] = applied[i]]
    /\ UNCHANGED <<journalSeq, replayAll, hintLane, hintLaneAvailable,
                  latestHintMeta, truncateFloor, applied, appliedRecords,
                  target, catchupActive, bulkSessionActive>>

TruncateReplayFloor ==
    /\ truncateFloor < journalSeq
    /\ SafeToTruncate(truncateFloor + 1)
    /\ truncateFloor' = truncateFloor + 1
    /\ hintLane' = [i \in Indexes |-> hintLane[i] \ {truncateFloor + 1}]
    /\ UNCHANGED <<journalSeq, replayAll, hintLaneAvailable, latestHintMeta,
                  applied, appliedRecords, target, queryTarget, catchupActive,
                  bulkSessionActive>>

Next ==
    \/ TruncateReplayFloor
    \/ AppendAllLaneOnly
    \/ \E hints \in SUBSET Indexes: AppendHintedRecord(hints)
    \/ \E i \in Indexes:
        \/ BuggyAppendLatestWithoutHint(i)
        \/ ToggleHintLaneAvailability(i)
        \/ StartBulkSession(i)
        \/ FinishBulkSession(i)
        \/ ObserveReplayTarget(i)
        \/ StartCatchup(i)
        \/ AdvanceWhenNoVisibleHintMatch(i)
        \/ FinishCatchup(i)
        \/ AdvanceQueryTarget(i)
        \/ \E s \in Seqs: ApplyHintMatch(i, s)

Spec == Init /\ [][Next]_vars

(*
  Liveness: hinted catch-up never stalls permanently. Strong fairness is
  needed (not weak) because hint-lane availability toggles and bulk sessions
  can churn, so worker actions are enabled only intermittently. The property
  is conditional on the hint lane being available infinitely often: a
  permanently unavailable lane is a modeled environment failure and is
  allowed to stall catch-up. Appends quiesce structurally (journalSeq is
  monotone and bounded by MaxSeq), so per-hint latest metadata is eventually
  constant and fair workers must drain to it.
*)
Fairness ==
    \A i \in Indexes:
        /\ SF_vars(ObserveReplayTarget(i))
        /\ SF_vars(StartCatchup(i))
        /\ SF_vars(AdvanceWhenNoVisibleHintMatch(i))
        /\ SF_vars(FinishCatchup(i))
        /\ SF_vars(FinishBulkSession(i))
        /\ \A s \in Seqs: SF_vars(ApplyHintMatch(i, s))

\* Liveness-checked spec used by the positive config; mutant and heavy configs
\* check invariants only and use the unfair Spec.
FairSpec == Spec /\ Fairness

CatchupEventuallyCompletes ==
    \A i \in Indexes:
        []<>(hintLaneAvailable[i]) => <>[](applied[i] = latestHintMeta[i])

TypeOK ==
    /\ BuggyEmptyHintAdvance \in BOOLEAN
    /\ Indexes \in SUBSET KnownIndexes
    /\ Indexes # {}
    /\ MaxSeq \in 1..3
    /\ journalSeq \in 0..MaxSeq
    /\ replayAll \in [Seqs -> SUBSET Indexes]
    /\ hintLane \in [Indexes -> SUBSET Seqs]
    /\ hintLaneAvailable \in [Indexes -> BOOLEAN]
    /\ latestHintMeta \in [Indexes -> 0..MaxSeq]
    /\ truncateFloor \in 0..MaxSeq
    /\ applied \in [Indexes -> 0..MaxSeq]
    /\ appliedRecords \in [Indexes -> SUBSET Seqs]
    /\ target \in [Indexes -> 0..MaxSeq]
    /\ queryTarget \in [Indexes -> 0..MaxSeq]
    /\ catchupActive \in [Indexes -> BOOLEAN]
    /\ bulkSessionActive \in [Indexes -> BOOLEAN]

ReplayPublicationOrdered ==
    /\ \A i \in Indexes:
        /\ hintLane[i] \subseteq Seqs
        /\ \A s \in hintLane[i]:
            /\ s <= journalSeq
            /\ i \in replayAll[s]
    /\ \A s \in Seqs:
        replayAll[s] # {} => s <= journalSeq

WatermarksOrdered ==
    \A i \in Indexes:
        /\ applied[i] <= target[i]
        /\ target[i] <= latestHintMeta[i]
        /\ latestHintMeta[i] <= journalSeq
        /\ queryTarget[i] <= applied[i]
        /\ catchupActive[i] => ~bulkSessionActive[i]

LatestHintMetadataHasHintRows ==
    \A i \in Indexes:
        \A s \in Seqs:
            /\ s <= latestHintMeta[i]
            /\ truncateFloor < s
            /\ i \in replayAll[s]
            => s \in hintLane[i]

NoAppliedSkipsTargetedReplay ==
    \A i \in Indexes:
        \A s \in TargetedAllLane(i, 0, applied[i]):
            s <= latestHintMeta[i] => s \in appliedRecords[i]

QueryTargetNeverObservesBeyondSafeApplied ==
    \A i \in Indexes:
        /\ queryTarget[i] <= applied[i]
        /\ queryTarget[i] <= target[i]

TruncationKeepsNeededReplay ==
    SafeToTruncate(truncateFloor)

UnavailableHintLaneCannotBeTreatedAsApplied ==
    \A i \in Indexes:
        /\ catchupActive[i]
        /\ ~hintLaneAvailable[i]
        /\ TargetedAllLane(i, applied[i], target[i]) # {}
        => applied[i] < target[i]

Safety ==
    /\ TypeOK
    /\ ReplayPublicationOrdered
    /\ WatermarksOrdered
    /\ LatestHintMetadataHasHintRows
    /\ NoAppliedSkipsTargetedReplay
    /\ QueryTargetNeverObservesBeyondSafeApplied
    /\ TruncationKeepsNeededReplay
    /\ UnavailableHintLaneCannotBeTreatedAsApplied

=============================================================================
