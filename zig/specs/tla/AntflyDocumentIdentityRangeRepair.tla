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

-------------------- MODULE AntflyDocumentIdentityRangeRepair --------------------
(*
  Bounded cross-boundary model for document identity namespace guards around
  split, merge, and restore repair.

  This complements AntflyDocumentIdentity.tla, which models a single store's
  ordinal/generation rows. This model instead checks the metadata/data boundary
  contracts used by:

    - metadata/http_server.zig and metadata/table_workflow.zig split/merge
      doc identity validators.
    - metadata/reconciler.zig doc identity lifecycle gates.
    - data/storage/db_split_handoff.zig split destination and merge receiver
      namespace handling.
    - storage/db/db.zig deferred restore/import/runtime-repair ordering.

  Concrete contracts modeled:
    - split intents require a healthy source identity lifecycle.
    - split destination status must report the expected destination namespace.
    - merge intents reject incompatible namespaces unless reassignment is
      explicitly opted in, while old mixed-version no-row reports are tolerated.
    - merge reassignment requires opt-in and healthy donor/receiver status.
    - strict deferred restore rejects namespace mismatch.
    - restore intent clears only after primary import recovery and runtime
      repair are both complete.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggySplitAllowsUnhealthySource,
    BuggySplitAcceptsStaleDestNamespace,
    BuggyMergeAllowsMismatchWithoutOptIn,
    BuggyMergeAllowsActiveReassignment,
    BuggyRestoreAcceptsNamespaceMismatch,
    BuggyRestoreClearsBeforeRepair

Namespaces == 1..2
ExpectedDestNamespace == 2

StatusKinds ==
    {"healthyA", "healthyB", "oldNoRows", "active", "conflict", "rebuild", "exhausted"}

VARIABLES
    sourceStatus,
    donorStatus,
    receiverStatus,
    destStoredNamespace,
    mergeOptIn,
    restoreSourceNamespace,
    restoreTargetNamespace,
    splitAccepted,
    splitStatusAccepted,
    mergeAccepted,
    receiverReassigned,
    strictRestoreAccepted,
    importRecovered,
    runtimeRepairNeeded,
    runtimeRepairComplete,
    restoreIntentCleared

vars ==
    <<sourceStatus, donorStatus, receiverStatus, destStoredNamespace, mergeOptIn,
      restoreSourceNamespace, restoreTargetNamespace, splitAccepted,
      splitStatusAccepted, mergeAccepted, receiverReassigned,
      strictRestoreAccepted, importRecovered, runtimeRepairNeeded,
      runtimeRepairComplete, restoreIntentCleared>>

Init ==
    /\ sourceStatus \in StatusKinds
    /\ donorStatus \in StatusKinds
    /\ receiverStatus \in StatusKinds
    /\ destStoredNamespace \in Namespaces
    /\ mergeOptIn \in BOOLEAN
    /\ restoreSourceNamespace \in Namespaces
    /\ restoreTargetNamespace \in Namespaces
    /\ splitAccepted = FALSE
    /\ splitStatusAccepted = FALSE
    /\ mergeAccepted = FALSE
    /\ receiverReassigned = FALSE
    /\ strictRestoreAccepted = FALSE
    /\ importRecovered = FALSE
    /\ runtimeRepairNeeded = FALSE
    /\ runtimeRepairComplete = FALSE
    /\ restoreIntentCleared = FALSE

StatusNamespace(s) ==
    CASE s = "healthyB" -> 2
      [] OTHER -> 1

HasOrdinalRows(s) ==
    s # "oldNoRows"

HealthyStatus(s) ==
    s \in {"healthyA", "healthyB", "oldNoRows"}

SplitCompatible(s) ==
    HealthyStatus(s)

MergeCompatible(donor, receiver, allowReassignment) ==
    /\ HealthyStatus(donor)
    /\ HealthyStatus(receiver)
    /\ \/ ~HasOrdinalRows(donor)
       \/ ~HasOrdinalRows(receiver)
       \/ allowReassignment
       \/ StatusNamespace(donor) = StatusNamespace(receiver)

ReassignmentAllowed(donor, receiver, allowReassignment) ==
    /\ allowReassignment
    /\ HealthyStatus(donor)
    /\ HealthyStatus(receiver)

ValidateSplit ==
    /\ splitAccepted' =
        IF BuggySplitAllowsUnhealthySource
        THEN TRUE
        ELSE SplitCompatible(sourceStatus)
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitStatusAccepted, mergeAccepted,
                  receiverReassigned, strictRestoreAccepted, importRecovered,
                  runtimeRepairNeeded, runtimeRepairComplete,
                  restoreIntentCleared>>

ObserveSplitDestinationStatus ==
    /\ splitStatusAccepted' =
        IF BuggySplitAcceptsStaleDestNamespace
        THEN TRUE
        ELSE destStoredNamespace = ExpectedDestNamespace
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitAccepted, mergeAccepted,
                  receiverReassigned, strictRestoreAccepted, importRecovered,
                  runtimeRepairNeeded, runtimeRepairComplete,
                  restoreIntentCleared>>

ValidateMerge ==
    /\ mergeAccepted' =
        IF BuggyMergeAllowsMismatchWithoutOptIn
        THEN HealthyStatus(donorStatus) /\ HealthyStatus(receiverStatus)
        ELSE MergeCompatible(donorStatus, receiverStatus, mergeOptIn)
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitAccepted, splitStatusAccepted,
                  receiverReassigned, strictRestoreAccepted, importRecovered,
                  runtimeRepairNeeded, runtimeRepairComplete,
                  restoreIntentCleared>>

ReassignReceiverNamespace ==
    /\ receiverReassigned' =
        IF BuggyMergeAllowsActiveReassignment
        THEN TRUE
        ELSE ReassignmentAllowed(donorStatus, receiverStatus, mergeOptIn)
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitAccepted, splitStatusAccepted,
                  mergeAccepted, strictRestoreAccepted, importRecovered,
                  runtimeRepairNeeded, runtimeRepairComplete,
                  restoreIntentCleared>>

StrictDeferredRestore ==
    /\ strictRestoreAccepted' =
        IF BuggyRestoreAcceptsNamespaceMismatch
        THEN TRUE
        ELSE restoreSourceNamespace = restoreTargetNamespace
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitAccepted, splitStatusAccepted,
                  mergeAccepted, receiverReassigned, importRecovered,
                  runtimeRepairNeeded, runtimeRepairComplete,
                  restoreIntentCleared>>

RecoverIncompleteImport ==
    /\ importRecovered' = TRUE
    /\ runtimeRepairNeeded' = TRUE
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitAccepted, splitStatusAccepted,
                  mergeAccepted, receiverReassigned, strictRestoreAccepted,
                  runtimeRepairComplete, restoreIntentCleared>>

RunRuntimeRepair ==
    /\ importRecovered
    /\ runtimeRepairNeeded
    /\ runtimeRepairComplete' = TRUE
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitAccepted, splitStatusAccepted,
                  mergeAccepted, receiverReassigned, strictRestoreAccepted,
                  importRecovered, runtimeRepairNeeded, restoreIntentCleared>>

ClearRestoreIntent ==
    /\ restoreIntentCleared' =
        IF BuggyRestoreClearsBeforeRepair
        THEN TRUE
        ELSE importRecovered /\ runtimeRepairNeeded /\ runtimeRepairComplete
    /\ UNCHANGED <<sourceStatus, donorStatus, receiverStatus,
                  destStoredNamespace, mergeOptIn, restoreSourceNamespace,
                  restoreTargetNamespace, splitAccepted, splitStatusAccepted,
                  mergeAccepted, receiverReassigned, strictRestoreAccepted,
                  importRecovered, runtimeRepairNeeded, runtimeRepairComplete>>

Next ==
    \/ ValidateSplit
    \/ ObserveSplitDestinationStatus
    \/ ValidateMerge
    \/ ReassignReceiverNamespace
    \/ StrictDeferredRestore
    \/ RecoverIncompleteImport
    \/ RunRuntimeRepair
    \/ ClearRestoreIntent

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ sourceStatus \in StatusKinds
    /\ donorStatus \in StatusKinds
    /\ receiverStatus \in StatusKinds
    /\ destStoredNamespace \in Namespaces
    /\ mergeOptIn \in BOOLEAN
    /\ restoreSourceNamespace \in Namespaces
    /\ restoreTargetNamespace \in Namespaces
    /\ splitAccepted \in BOOLEAN
    /\ splitStatusAccepted \in BOOLEAN
    /\ mergeAccepted \in BOOLEAN
    /\ receiverReassigned \in BOOLEAN
    /\ strictRestoreAccepted \in BOOLEAN
    /\ importRecovered \in BOOLEAN
    /\ runtimeRepairNeeded \in BOOLEAN
    /\ runtimeRepairComplete \in BOOLEAN
    /\ restoreIntentCleared \in BOOLEAN

SplitRequiresHealthySource ==
    splitAccepted => SplitCompatible(sourceStatus)

SplitDestinationStatusMatchesExpectedNamespace ==
    splitStatusAccepted => destStoredNamespace = ExpectedDestNamespace

MergeRequiresCompatibleIdentityStatus ==
    mergeAccepted => MergeCompatible(donorStatus, receiverStatus, mergeOptIn)

MergeReassignmentRequiresOptInAndHealthyStatus ==
    receiverReassigned => ReassignmentAllowed(donorStatus, receiverStatus, mergeOptIn)

StrictRestoreRejectsNamespaceMismatch ==
    strictRestoreAccepted => restoreSourceNamespace = restoreTargetNamespace

RuntimeRepairRequiresRecoveredPrimaryImport ==
    runtimeRepairComplete => importRecovered /\ runtimeRepairNeeded

RestoreIntentClearsOnlyAfterRepairComplete ==
    restoreIntentCleared =>
        /\ importRecovered
        /\ runtimeRepairNeeded
        /\ runtimeRepairComplete

Safety ==
    /\ TypeOK
    /\ SplitRequiresHealthySource
    /\ SplitDestinationStatusMatchesExpectedNamespace
    /\ MergeRequiresCompatibleIdentityStatus
    /\ MergeReassignmentRequiresOptInAndHealthyStatus
    /\ StrictRestoreRejectsNamespaceMismatch
    /\ RuntimeRepairRequiresRecoveredPrimaryImport
    /\ RestoreIntentClearsOnlyAfterRepairComplete

=============================================================================
