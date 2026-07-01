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

-------------------------- MODULE AntflyEnrichmentLease --------------------------
(*
  Bounded model of storage/db/enrichment/enrichment_runtime.zig.

  Implementation correspondence:
  - targetSeq/appliedSeq/retrying/workerFailed abstract EnrichmentRuntime's
    target_sequence, applied_sequence, retrying, and worker_failed fields.
  - pendingRequired and visibleReplay abstract replay_source.collectPendingDocumentGroups:
    a source replay row may require generated enrichment, and that work can only
    be collected after replay is visible.
  - leaseOwner/leaseValid/leaseEpoch abstract ownership.ensureLease/release.
  - collected/generated/publishedArtifacts abstract collection, generated replay
    window building, and flushGeneratedReplayWindow.
  - isolatedFailedIndexes/isolatedSeqs abstract recordIsolatedRequestError,
    where one bad request/index is quarantined without failing the whole worker.

  Deliberate omissions:
  - Embedding payloads, chunk text, sparse values, and asset bytes are reduced to
    "this source sequence requires generated work".
  - Window byte/item sizing and exact backoff timing are modeled as nondeterministic
    retry/flush choices.
  - The model uses ghost pendingRequired knowledge to express the intended safety
    contract: target advancement must not permanently skip hidden generated work.

  Bug classes:
  - stale owner publishes generated artifacts after lease loss;
  - empty pending replay scan advances applied through hidden generated work;
  - retryable failures advance applied as if work completed;
  - isolated request failure poisons unrelated worker progress.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyStalePublish, BuggyEmptyPendingAdvance, MaxSeq

Owners == {"none", "nodeA", "nodeB"}
RealOwners == {"nodeA", "nodeB"}
Indexes == {"dense", "asset"}
Seqs == 1..MaxSeq
MaxEpoch == 3

VARIABLES
    leaseOwner,
    leaseValid,
    leaseEpoch,
    sourceSeq,
    targetSeq,
    appliedSeq,
    visibleReplay,
    pendingRequired,
    collected,
    collectedEpoch,
    generated,
    generatedEpoch,
    publishedArtifacts,
    publishValid,
    retrying,
    retrySeq,
    workerFailed,
    isolatedFailedIndexes,
    isolatedSeqs,
    lostLeaseCount

vars == <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq, appliedSeq,
          visibleReplay, pendingRequired, collected, collectedEpoch, generated,
          generatedEpoch, publishedArtifacts, publishValid, retrying, retrySeq,
          workerFailed, isolatedFailedIndexes, isolatedSeqs, lostLeaseCount>>

Range(lo, hi) == {s \in Seqs : lo < s /\ s <= hi}

PendingBetween(lo, hi) ==
    {s \in Range(lo, hi) : s \in pendingRequired}

VisiblePendingBetween(lo, hi) ==
    {s \in PendingBetween(lo, hi) : s \in visibleReplay}

CanApplySeq(s) ==
    /\ s \in Seqs
    /\ \/ s \notin pendingRequired
       \/ /\ s \in visibleReplay
          /\ (s \in publishedArtifacts \/ s \in isolatedSeqs)

Init ==
    /\ leaseOwner = "none"
    /\ leaseValid = FALSE
    /\ leaseEpoch = 0
    /\ sourceSeq = 0
    /\ targetSeq = 0
    /\ appliedSeq = 0
    /\ visibleReplay = {}
    /\ pendingRequired = {}
    /\ collected = {}
    /\ collectedEpoch = [s \in Seqs |-> 0]
    /\ generated = {}
    /\ generatedEpoch = [s \in Seqs |-> 0]
    /\ publishedArtifacts = {}
    /\ publishValid = [s \in Seqs |-> FALSE]
    /\ retrying = FALSE
    /\ retrySeq = 0
    /\ workerFailed = FALSE
    /\ isolatedFailedIndexes = {}
    /\ isolatedSeqs = {}
    /\ lostLeaseCount = 0

\* DB.batch appends/commits a replay source record and notifySequence advances
\* the runtime target. The generated-enrichment hint is represented by needs.
AppendSource(needs) ==
    /\ needs \in BOOLEAN
    /\ sourceSeq < MaxSeq
    /\ sourceSeq' = sourceSeq + 1
    /\ targetSeq' = sourceSeq + 1
    /\ pendingRequired' =
        IF needs THEN pendingRequired \cup {sourceSeq + 1} ELSE pendingRequired
    /\ retrying' = FALSE
    /\ retrySeq' = 0
    /\ workerFailed' = FALSE
    /\ isolatedFailedIndexes' = {}
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, appliedSeq,
                  visibleReplay, collected, collectedEpoch, generated,
                  generatedEpoch, publishedArtifacts, publishValid,
                  isolatedSeqs, lostLeaseCount>>

PublishReplay(s) ==
    /\ s \in Seqs
    /\ s <= sourceSeq
    /\ s \notin visibleReplay
    /\ \A r \in Seqs : r < s /\ r <= sourceSeq => r \in visibleReplay
    /\ visibleReplay' = visibleReplay \cup {s}
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, pendingRequired, collected, collectedEpoch,
                  generated, generatedEpoch, publishedArtifacts, publishValid,
                  retrying, retrySeq, workerFailed, isolatedFailedIndexes,
                  isolatedSeqs, lostLeaseCount>>

AcquireLease(o) ==
    /\ o \in RealOwners
    /\ leaseOwner = "none"
    /\ ~leaseValid
    /\ leaseEpoch < MaxEpoch
    /\ leaseOwner' = o
    /\ leaseValid' = TRUE
    /\ leaseEpoch' = leaseEpoch + 1
    /\ UNCHANGED <<sourceSeq, targetSeq, appliedSeq, visibleReplay,
                  pendingRequired, collected, collectedEpoch, generated,
                  generatedEpoch, publishedArtifacts, publishValid, retrying,
                  retrySeq, workerFailed, isolatedFailedIndexes, isolatedSeqs,
                  lostLeaseCount>>

LoseLease ==
    /\ leaseValid
    /\ leaseOwner \in RealOwners
    /\ lostLeaseCount < MaxEpoch
    /\ leaseValid' = FALSE
    /\ leaseOwner' = "none"
    /\ lostLeaseCount' = lostLeaseCount + 1
    /\ UNCHANGED <<leaseEpoch, sourceSeq, targetSeq, appliedSeq, visibleReplay,
                  pendingRequired, collected, collectedEpoch, generated,
                  generatedEpoch, publishedArtifacts, publishValid, retrying,
                  retrySeq, workerFailed, isolatedFailedIndexes, isolatedSeqs>>

CollectPending(s) ==
    /\ leaseValid
    /\ ~retrying
    /\ ~workerFailed
    /\ s \in VisiblePendingBetween(appliedSeq, targetSeq)
    /\ s \notin collected
    /\ collected' = collected \cup {s}
    /\ collectedEpoch' = [collectedEpoch EXCEPT ![s] = leaseEpoch]
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, generated,
                  generatedEpoch, publishedArtifacts, publishValid, retrying,
                  retrySeq, workerFailed, isolatedFailedIndexes, isolatedSeqs,
                  lostLeaseCount>>

GenerateArtifact(s) ==
    /\ leaseValid
    /\ ~retrying
    /\ ~workerFailed
    /\ s \in collected
    /\ collectedEpoch[s] = leaseEpoch
    /\ s \notin generated
    /\ generated' = generated \cup {s}
    /\ generatedEpoch' = [generatedEpoch EXCEPT ![s] = leaseEpoch]
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, collected,
                  collectedEpoch, publishedArtifacts, publishValid, retrying,
                  retrySeq, workerFailed, isolatedFailedIndexes, isolatedSeqs,
                  lostLeaseCount>>

PublishGenerated(s) ==
    /\ leaseValid
    /\ ~retrying
    /\ ~workerFailed
    /\ s \in generated
    /\ s \in visibleReplay
    /\ generatedEpoch[s] = leaseEpoch
    /\ s \notin publishedArtifacts
    /\ publishedArtifacts' = publishedArtifacts \cup {s}
    /\ publishValid' = [publishValid EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, collected,
                  collectedEpoch, generated, generatedEpoch, retrying,
                  retrySeq, workerFailed, isolatedFailedIndexes, isolatedSeqs,
                  lostLeaseCount>>

BuggyPublishAfterLeaseLoss(s) ==
    /\ BuggyStalePublish
    /\ ~leaseValid
    /\ s \in generated
    /\ s \notin publishedArtifacts
    /\ publishedArtifacts' = publishedArtifacts \cup {s}
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, collected,
                  collectedEpoch, generated, generatedEpoch, publishValid,
                  retrying, retrySeq, workerFailed, isolatedFailedIndexes,
                  isolatedSeqs, lostLeaseCount>>

RetryTransient(s) ==
    /\ leaseValid
    /\ ~workerFailed
    /\ ~retrying
    /\ s \in VisiblePendingBetween(appliedSeq, targetSeq)
    /\ s \notin publishedArtifacts
    /\ s \notin isolatedSeqs
    /\ retrying' = TRUE
    /\ retrySeq' = s
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, collected,
                  collectedEpoch, generated, generatedEpoch, publishedArtifacts,
                  publishValid, workerFailed, isolatedFailedIndexes,
                  isolatedSeqs, lostLeaseCount>>

RetryLater ==
    /\ retrying
    /\ retrying' = FALSE
    /\ retrySeq' = 0
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, collected,
                  collectedEpoch, generated, generatedEpoch, publishedArtifacts,
                  publishValid, workerFailed, isolatedFailedIndexes,
                  isolatedSeqs, lostLeaseCount>>

IsolateRequestFailure(i, s) ==
    /\ i \in Indexes
    /\ leaseValid
    /\ ~retrying
    /\ s \in VisiblePendingBetween(appliedSeq, targetSeq)
    /\ s \notin publishedArtifacts
    /\ isolatedFailedIndexes' = isolatedFailedIndexes \cup {i}
    /\ isolatedSeqs' = isolatedSeqs \cup {s}
    /\ workerFailed' = FALSE
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, collected,
                  collectedEpoch, generated, generatedEpoch, publishedArtifacts,
                  publishValid, retrying, retrySeq, lostLeaseCount>>

FatalWorkerFailure ==
    /\ leaseValid
    /\ ~workerFailed
    /\ workerFailed' = TRUE
    /\ retrying' = FALSE
    /\ retrySeq' = 0
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  appliedSeq, visibleReplay, pendingRequired, collected,
                  collectedEpoch, generated, generatedEpoch, publishedArtifacts,
                  publishValid, isolatedFailedIndexes, isolatedSeqs,
                  lostLeaseCount>>

AdvanceAppliedOne ==
    /\ leaseValid
    /\ ~retrying
    /\ ~workerFailed
    /\ appliedSeq < targetSeq
    /\ CanApplySeq(appliedSeq + 1)
    /\ appliedSeq' = appliedSeq + 1
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  visibleReplay, pendingRequired, collected, collectedEpoch,
                  generated, generatedEpoch, publishedArtifacts, publishValid,
                  retrying, retrySeq, workerFailed, isolatedFailedIndexes,
                  isolatedSeqs, lostLeaseCount>>

AdvanceNoPendingToTarget ==
    /\ leaseValid
    /\ ~retrying
    /\ ~workerFailed
    /\ appliedSeq < targetSeq
    /\ PendingBetween(appliedSeq, targetSeq) = {}
    /\ appliedSeq' = targetSeq
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  visibleReplay, pendingRequired, collected, collectedEpoch,
                  generated, generatedEpoch, publishedArtifacts, publishValid,
                  retrying, retrySeq, workerFailed, isolatedFailedIndexes,
                  isolatedSeqs, lostLeaseCount>>

BuggyAdvanceEmptyVisiblePendingToTarget ==
    /\ BuggyEmptyPendingAdvance
    /\ leaseValid
    /\ ~retrying
    /\ ~workerFailed
    /\ appliedSeq < targetSeq
    /\ VisiblePendingBetween(appliedSeq, targetSeq) = {}
    /\ PendingBetween(appliedSeq, targetSeq) # {}
    /\ appliedSeq' = targetSeq
    /\ UNCHANGED <<leaseOwner, leaseValid, leaseEpoch, sourceSeq, targetSeq,
                  visibleReplay, pendingRequired, collected, collectedEpoch,
                  generated, generatedEpoch, publishedArtifacts, publishValid,
                  retrying, retrySeq, workerFailed, isolatedFailedIndexes,
                  isolatedSeqs, lostLeaseCount>>

Next ==
    \/ \E needs \in BOOLEAN: AppendSource(needs)
    \/ \E s \in Seqs:
        \/ PublishReplay(s)
        \/ CollectPending(s)
        \/ GenerateArtifact(s)
        \/ PublishGenerated(s)
        \/ BuggyPublishAfterLeaseLoss(s)
        \/ RetryTransient(s)
        \/ BuggyAdvanceEmptyVisiblePendingToTarget
    \/ RetryLater
    \/ LoseLease
    \/ FatalWorkerFailure
    \/ AdvanceAppliedOne
    \/ AdvanceNoPendingToTarget
    \/ \E o \in RealOwners: AcquireLease(o)
    \/ \E i \in Indexes, s \in Seqs: IsolateRequestFailure(i, s)

Spec == Init /\ [][Next]_vars

(*
  Liveness: generated enrichment work eventually drains (applied reaches
  target) in stable-lease, non-failed runs. The antecedent excludes behaviors
  with any lease loss or a permanent worker failure: collected/generated
  window state is epoch-pinned in this model and is not re-collectable after
  lease churn (the stale-publish mutant needs the old window to survive), so
  post-churn drain is a known modeling limitation, not a code claim. Strong
  fairness is used because retry cycling makes worker actions only
  intermittently enabled.
*)
Fairness ==
    /\ \A s \in Seqs:
        /\ SF_vars(PublishReplay(s))
        /\ SF_vars(CollectPending(s))
        /\ SF_vars(GenerateArtifact(s))
        /\ SF_vars(PublishGenerated(s))
    /\ SF_vars(AdvanceAppliedOne)
    /\ SF_vars(AdvanceNoPendingToTarget)
    /\ SF_vars(RetryLater)

\* Liveness-checked spec used by the positive config; mutant configs check
\* invariants only and use the unfair Spec.
FairSpec == Spec /\ Fairness

EnrichmentEventuallyDrains ==
    ([](lostLeaseCount = 0) /\ <>[](leaseValid /\ ~workerFailed))
        => <>[](appliedSeq = targetSeq)

TypeOK ==
    /\ BuggyStalePublish \in BOOLEAN
    /\ BuggyEmptyPendingAdvance \in BOOLEAN
    /\ MaxSeq \in 1..3
    /\ leaseOwner \in Owners
    /\ leaseValid \in BOOLEAN
    /\ leaseEpoch \in 0..MaxEpoch
    /\ sourceSeq \in 0..MaxSeq
    /\ targetSeq \in 0..MaxSeq
    /\ appliedSeq \in 0..MaxSeq
    /\ visibleReplay \subseteq Seqs
    /\ pendingRequired \subseteq Seqs
    /\ collected \subseteq Seqs
    /\ collectedEpoch \in [Seqs -> 0..MaxEpoch]
    /\ generated \subseteq Seqs
    /\ generatedEpoch \in [Seqs -> 0..MaxEpoch]
    /\ publishedArtifacts \subseteq Seqs
    /\ publishValid \in [Seqs -> BOOLEAN]
    /\ retrying \in BOOLEAN
    /\ retrySeq \in 0..MaxSeq
    /\ workerFailed \in BOOLEAN
    /\ isolatedFailedIndexes \subseteq Indexes
    /\ isolatedSeqs \subseteq Seqs
    /\ lostLeaseCount \in 0..MaxEpoch

TargetsOrdered ==
    /\ appliedSeq <= targetSeq
    /\ targetSeq <= sourceSeq
    /\ \A s \in visibleReplay: s <= sourceSeq
    /\ pendingRequired \subseteq 1..sourceSeq

OwnerMatchesLease ==
    leaseValid <=> leaseOwner \in RealOwners

CollectedAndGeneratedUnderLease ==
    /\ \A s \in collected: collectedEpoch[s] > 0
    /\ \A s \in generated:
        /\ s \in collected
        /\ generatedEpoch[s] = collectedEpoch[s]

PublishedArtifactsSafe ==
    \A s \in publishedArtifacts:
        /\ s \in generated
        /\ s \in visibleReplay
        /\ publishValid[s]

AppliedDoesNotSkipGeneratedWork ==
    \A s \in Seqs:
        /\ s <= appliedSeq
        /\ s \in pendingRequired
        => /\ s \in visibleReplay
           /\ (s \in publishedArtifacts \/ s \in isolatedSeqs)

RetryableFailureDoesNotAdvanceApplied ==
    retrying => /\ retrySeq \in Seqs /\ appliedSeq < retrySeq

IsolatedFailureRecorded ==
    isolatedFailedIndexes # {} => isolatedSeqs # {}

WorkerFailureStatusStable ==
    workerFailed => ~retrying

Safety ==
    /\ TypeOK
    /\ TargetsOrdered
    /\ OwnerMatchesLease
    /\ CollectedAndGeneratedUnderLease
    /\ PublishedArtifactsSafe
    /\ AppliedDoesNotSkipGeneratedWork
    /\ RetryableFailureDoesNotAdvanceApplied
    /\ IsolatedFailureRecorded
    /\ WorkerFailureStatusStable

=============================================================================
