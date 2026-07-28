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

--------------------- MODULE AntflyReplayEnrichmentBridge ---------------------
(*
  Bridge model for the durable replay journal and generated-enrichment worker.

  A fast derived consumer and generated enrichment share the replay journal.
  Provider failures may leave enrichment work collected only in volatile
  memory. Replay truncation therefore has to honor both durable consumer
  watermarks. After restart, an empty scan must not advance the enrichment
  watermark while durable source coverage debt remains.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyOmitEnrichmentFloor, BuggyAdvanceEmptyScan,
          BuggyOmitRestartArm, BuggyRetryWithoutBoundary, MaxSeq, MaxEpoch

Seqs == 1..MaxSeq

VARIABLES
    sourceSeq,
    journal,
    fastApplied,
    enrichmentApplied,
    coverageDebt,
    completed,
    volatileCollected,
    providerUp,
    workerArmed,
    retryAttemptsSinceBoundary,
    processEpoch

vars == <<sourceSeq, journal, fastApplied, enrichmentApplied, coverageDebt,
          completed, volatileCollected, providerUp, workerArmed,
          retryAttemptsSinceBoundary, processEpoch>>

Init ==
    /\ sourceSeq = 0
    /\ journal = {}
    /\ fastApplied = 0
    /\ enrichmentApplied = 0
    /\ coverageDebt = {}
    /\ completed = {}
    /\ volatileCollected = {}
    /\ providerUp = TRUE
    /\ workerArmed = TRUE
    /\ retryAttemptsSinceBoundary = 0
    /\ processEpoch = 0

AppendGeneratedSource ==
    /\ sourceSeq < MaxSeq
    /\ sourceSeq' = sourceSeq + 1
    /\ journal' = journal \cup {sourceSeq + 1}
    /\ coverageDebt' = coverageDebt \cup {sourceSeq + 1}
    /\ UNCHANGED <<fastApplied, enrichmentApplied, completed,
                  volatileCollected, providerUp, workerArmed,
                  retryAttemptsSinceBoundary, processEpoch>>

FastConsumerAdvance ==
    /\ fastApplied < sourceSeq
    /\ fastApplied' = fastApplied + 1
    /\ UNCHANGED <<sourceSeq, journal, enrichmentApplied, coverageDebt,
                  completed, volatileCollected, providerUp, workerArmed,
                  retryAttemptsSinceBoundary, processEpoch>>

CollectPending(s) ==
    /\ s \in coverageDebt
    /\ s \in journal
    /\ s \notin volatileCollected
    /\ volatileCollected' = volatileCollected \cup {s}
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  coverageDebt, completed, providerUp, workerArmed,
                  retryAttemptsSinceBoundary, processEpoch>>

ProviderFails ==
    /\ providerUp
    /\ providerUp' = FALSE
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  coverageDebt, completed, volatileCollected, workerArmed,
                  retryAttemptsSinceBoundary, processEpoch>>

ProviderRecovers ==
    /\ ~providerUp
    /\ providerUp' = TRUE
    /\ retryAttemptsSinceBoundary' = 0
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  coverageDebt, completed, volatileCollected, workerArmed, processEpoch>>

TransientProviderRetry ==
    /\ ~providerUp
    /\ workerArmed
    /\ coverageDebt /= {}
    /\ retryAttemptsSinceBoundary < 2
    /\ BuggyRetryWithoutBoundary \/ retryAttemptsSinceBoundary = 0
    /\ retryAttemptsSinceBoundary' = retryAttemptsSinceBoundary + 1
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  coverageDebt, completed, volatileCollected, providerUp,
                  workerArmed, processEpoch>>

RetrySchedulerBoundary ==
    /\ retryAttemptsSinceBoundary = 1
    /\ retryAttemptsSinceBoundary' = 0
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  coverageDebt, completed, volatileCollected, providerUp,
                  workerArmed, processEpoch>>

CompleteEnrichment(s) ==
    /\ providerUp
    /\ workerArmed
    /\ s \in coverageDebt
    /\ s \in journal
    /\ completed' = completed \cup {s}
    /\ coverageDebt' = coverageDebt \ {s}
    /\ volatileCollected' = volatileCollected \ {s}
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  providerUp, workerArmed, retryAttemptsSinceBoundary, processEpoch>>

AdvanceEnrichment ==
    /\ enrichmentApplied < sourceSeq
    /\ LET next == enrichmentApplied + 1 IN
       \/ next \notin coverageDebt
       \/ /\ BuggyAdvanceEmptyScan
          /\ next \in coverageDebt
          /\ next \notin journal
          /\ next \notin volatileCollected
    /\ enrichmentApplied' = enrichmentApplied + 1
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, coverageDebt,
                  completed, volatileCollected, providerUp, workerArmed,
                  retryAttemptsSinceBoundary, processEpoch>>

TruncateReplay(s) ==
    /\ s \in journal
    /\ s <= fastApplied
    /\ BuggyOmitEnrichmentFloor \/ s <= enrichmentApplied
    /\ journal' = journal \ {s}
    /\ UNCHANGED <<sourceSeq, fastApplied, enrichmentApplied, coverageDebt,
                  completed, volatileCollected, providerUp, workerArmed,
                  retryAttemptsSinceBoundary, processEpoch>>

Restart ==
    /\ processEpoch < MaxEpoch
    /\ processEpoch' = processEpoch + 1
    /\ volatileCollected' = {}
    /\ workerArmed' = FALSE
    /\ retryAttemptsSinceBoundary' = 0
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  coverageDebt, completed, providerUp>>

ArmStartupEnrichment ==
    /\ processEpoch > 0
    /\ ~workerArmed
    /\ ~BuggyOmitRestartArm
    /\ workerArmed' = TRUE
    /\ UNCHANGED <<sourceSeq, journal, fastApplied, enrichmentApplied,
                  coverageDebt, completed, volatileCollected, providerUp,
                  retryAttemptsSinceBoundary, processEpoch>>

Next ==
    \/ AppendGeneratedSource
    \/ FastConsumerAdvance
    \/ ProviderFails
    \/ ProviderRecovers
    \/ TransientProviderRetry
    \/ RetrySchedulerBoundary
    \/ AdvanceEnrichment
    \/ Restart
    \/ ArmStartupEnrichment
    \/ \E s \in Seqs:
        \/ CollectPending(s)
        \/ CompleteEnrichment(s)
        \/ TruncateReplay(s)

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(ProviderRecovers)
    /\ WF_vars(RetrySchedulerBoundary)
    /\ WF_vars(Restart)
    /\ WF_vars(ArmStartupEnrichment)
    /\ WF_vars(AdvanceEnrichment)
    /\ \A s \in Seqs: SF_vars(CompleteEnrichment(s))

TypeOK ==
    /\ BuggyOmitEnrichmentFloor \in BOOLEAN
    /\ BuggyAdvanceEmptyScan \in BOOLEAN
    /\ BuggyOmitRestartArm \in BOOLEAN
    /\ BuggyRetryWithoutBoundary \in BOOLEAN
    /\ MaxSeq \in 1..3
    /\ MaxEpoch \in 0..2
    /\ sourceSeq \in 0..MaxSeq
    /\ journal \in SUBSET Seqs
    /\ fastApplied \in 0..MaxSeq
    /\ enrichmentApplied \in 0..MaxSeq
    /\ coverageDebt \in SUBSET Seqs
    /\ completed \in SUBSET Seqs
    /\ volatileCollected \in SUBSET Seqs
    /\ providerUp \in BOOLEAN
    /\ workerArmed \in BOOLEAN
    /\ retryAttemptsSinceBoundary \in 0..2
    /\ processEpoch \in 0..MaxEpoch

WatermarksOrdered ==
    /\ fastApplied <= sourceSeq
    /\ enrichmentApplied <= sourceSeq

RetainedUntilAllConsumersApplied ==
    \A s \in coverageDebt:
        s > enrichmentApplied => s \in journal

NoAdvancePastCoverageDebt ==
    \A s \in coverageDebt:
        s > enrichmentApplied

CompletedWorkWasDurable ==
    completed \subseteq 1..sourceSeq

RetryRequiresSchedulerBoundary ==
    retryAttemptsSinceBoundary <= 1

Safety ==
    /\ TypeOK
    /\ WatermarksOrdered
    /\ RetainedUntilAllConsumersApplied
    /\ NoAdvancePastCoverageDebt
    /\ CompletedWorkWasDurable
    /\ RetryRequiresSchedulerBoundary

CoverageEventuallyDrains == <>[](coverageDebt = {})

=============================================================================
