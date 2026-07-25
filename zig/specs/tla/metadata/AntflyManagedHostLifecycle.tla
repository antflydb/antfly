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

---------------------- MODULE AntflyManagedHostLifecycle ----------------------
(*
  Managed raft host reconciliation model.

  This models the boundary between desired placement metadata, hosted raft
  replicas, live peer routes, durable apply stores, file-backed replica catalog
  state, and backup-restore bootstrap. It is intentionally bounded to the
  metadata and data groups, but includes the crash/restart and restore-cancel
  edges that are easy to regress in the implementation.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    BuggyActivateBeforeRestoreComplete,
    BuggyRemoveKeepsRoute,
    BuggyRestartRevivesRemoved,
    BuggyRestoreNotCancelled,
    BuggyAcceptMismatchedArtifact,
    BuggyCatalogLosesArtifactBinding

Groups == {"metadata", "data"}
BootstrapStatuses == {"none", "preparing", "succeeded", "failed"}
Artifacts == {"none", "bound", "other"}

VARIABLES
    desired,
    hosted,
    active,
    routes,
    durableApplyStore,
    replicaCatalog,
    restoreIntent,
    bootstrapStatus,
    restartCount,
    expectedArtifact,
    preparedArtifact,
    artifactVerified,
    catalogArtifact,
    activatedWithWrongArtifact

vars == <<desired, hosted, active, routes, durableApplyStore, replicaCatalog,
          restoreIntent, bootstrapStatus, restartCount, expectedArtifact,
          preparedArtifact, artifactVerified, catalogArtifact,
          activatedWithWrongArtifact>>

Init ==
    /\ desired = {}
    /\ hosted = {}
    /\ active = {}
    /\ routes = {}
    /\ durableApplyStore = {}
    /\ replicaCatalog = {}
    /\ restoreIntent = {}
    /\ bootstrapStatus = [g \in Groups |-> "none"]
    /\ restartCount = 0
    /\ expectedArtifact = [g \in Groups |-> "none"]
    /\ preparedArtifact = [g \in Groups |-> "none"]
    /\ artifactVerified = {}
    /\ catalogArtifact = [g \in Groups |-> "none"]
    /\ activatedWithWrongArtifact = FALSE

MetadataAdds(g) ==
    /\ g \in Groups
    /\ g \notin desired
    /\ desired' = desired \cup {g}
    /\ UNCHANGED <<hosted, active, routes, durableApplyStore, replicaCatalog,
                  restoreIntent, bootstrapStatus, restartCount,
                  expectedArtifact, preparedArtifact, artifactVerified,
                  catalogArtifact, activatedWithWrongArtifact>>

MetadataRemoves(g) ==
    /\ g \in desired
    /\ desired' = desired \ {g}
    /\ active' = active \ {g}
    /\ routes' = IF BuggyRemoveKeepsRoute THEN routes ELSE routes \ {g}
    /\ restoreIntent' =
        IF BuggyRestoreNotCancelled THEN restoreIntent ELSE restoreIntent \ {g}
    /\ bootstrapStatus' =
        IF BuggyRestoreNotCancelled THEN
            bootstrapStatus
        ELSE
            [bootstrapStatus EXCEPT ![g] = "none"]
    /\ expectedArtifact' =
        IF BuggyRestoreNotCancelled THEN expectedArtifact
        ELSE [expectedArtifact EXCEPT ![g] = "none"]
    /\ preparedArtifact' =
        IF BuggyRestoreNotCancelled THEN preparedArtifact
        ELSE [preparedArtifact EXCEPT ![g] = "none"]
    /\ artifactVerified' =
        IF BuggyRestoreNotCancelled THEN artifactVerified
        ELSE artifactVerified \ {g}
    /\ replicaCatalog' =
        IF BuggyRestartRevivesRemoved THEN replicaCatalog ELSE replicaCatalog \ {g}
    /\ catalogArtifact' =
        IF BuggyRestartRevivesRemoved THEN catalogArtifact
        ELSE [catalogArtifact EXCEPT ![g] = "none"]
    /\ UNCHANGED <<hosted, durableApplyStore, restartCount,
                  activatedWithWrongArtifact>>

EnsureFreshReplica(g) ==
    /\ g \in desired
    /\ g \notin hosted
    /\ g \notin restoreIntent
    /\ hosted' = hosted \cup {g}
    /\ active' = active \cup {g}
    /\ routes' = routes \cup {g}
    /\ durableApplyStore' = durableApplyStore \cup {g}
    /\ replicaCatalog' = replicaCatalog \cup {g}
    /\ UNCHANGED <<desired, restoreIntent, bootstrapStatus, restartCount,
                  expectedArtifact, preparedArtifact, artifactVerified,
                  catalogArtifact, activatedWithWrongArtifact>>

StartBackupRestore(g) ==
    /\ g \in desired
    /\ g \notin hosted
    /\ g \notin restoreIntent
    /\ restoreIntent' = restoreIntent \cup {g}
    /\ bootstrapStatus' = [bootstrapStatus EXCEPT ![g] = "preparing"]
    /\ expectedArtifact' = [expectedArtifact EXCEPT ![g] = "bound"]
    /\ preparedArtifact' = [preparedArtifact EXCEPT ![g] = "none"]
    /\ artifactVerified' = artifactVerified \ {g}
    /\ IF BuggyActivateBeforeRestoreComplete THEN
          /\ hosted' = hosted \cup {g}
          /\ active' = active \cup {g}
          /\ routes' = routes \cup {g}
          /\ durableApplyStore' = durableApplyStore
          /\ replicaCatalog' = replicaCatalog
       ELSE
          /\ UNCHANGED <<hosted, active, routes, durableApplyStore, replicaCatalog>>
    /\ UNCHANGED <<desired, restartCount, catalogArtifact,
                  activatedWithWrongArtifact>>

PrepareRestoreArtifact(g, artifact) ==
    /\ g \in restoreIntent
    /\ bootstrapStatus[g] = "preparing"
    /\ artifact \in Artifacts \ {"none"}
    /\ preparedArtifact' = [preparedArtifact EXCEPT ![g] = artifact]
    /\ artifactVerified' =
        IF artifact = expectedArtifact[g] \/ BuggyAcceptMismatchedArtifact
        THEN artifactVerified \cup {g}
        ELSE artifactVerified \ {g}
    /\ activatedWithWrongArtifact' =
        (activatedWithWrongArtifact \/
            (artifact # expectedArtifact[g] /\ BuggyAcceptMismatchedArtifact))
    /\ UNCHANGED <<desired, hosted, active, routes, durableApplyStore,
                  replicaCatalog, restoreIntent, bootstrapStatus, restartCount,
                  expectedArtifact, catalogArtifact>>

CompleteBackupRestore(g) ==
    /\ g \in restoreIntent
    /\ bootstrapStatus[g] = "preparing"
    /\ g \in desired
    /\ g \in artifactVerified
    /\ hosted' = hosted \cup {g}
    /\ active' = active \cup {g}
    /\ routes' = routes \cup {g}
    /\ durableApplyStore' = durableApplyStore \cup {g}
    /\ replicaCatalog' = replicaCatalog \cup {g}
    /\ catalogArtifact' =
        [catalogArtifact EXCEPT ![g] =
            IF BuggyCatalogLosesArtifactBinding
            THEN "none"
            ELSE preparedArtifact[g]]
    /\ bootstrapStatus' = [bootstrapStatus EXCEPT ![g] = "succeeded"]
    /\ UNCHANGED <<desired, restoreIntent, restartCount, expectedArtifact,
                  preparedArtifact, artifactVerified,
                  activatedWithWrongArtifact>>

FailBackupRestore(g) ==
    /\ g \in restoreIntent
    /\ bootstrapStatus[g] = "preparing"
    /\ bootstrapStatus' = [bootstrapStatus EXCEPT ![g] = "failed"]
    /\ UNCHANGED <<desired, hosted, active, routes, durableApplyStore,
                  replicaCatalog, restoreIntent, restartCount,
                  expectedArtifact, preparedArtifact, artifactVerified,
                  catalogArtifact, activatedWithWrongArtifact>>

RemoveUndesiredReplica(g) ==
    /\ g \in hosted
    /\ g \notin desired
    /\ hosted' = hosted \ {g}
    /\ active' = active \ {g}
    /\ routes' = routes \ {g}
    /\ replicaCatalog' =
        IF BuggyRestartRevivesRemoved THEN replicaCatalog ELSE replicaCatalog \ {g}
    /\ catalogArtifact' =
        IF BuggyRestartRevivesRemoved THEN catalogArtifact
        ELSE [catalogArtifact EXCEPT ![g] = "none"]
    /\ UNCHANGED <<desired, durableApplyStore, restoreIntent, bootstrapStatus,
                  restartCount, expectedArtifact, preparedArtifact,
                  artifactVerified, activatedWithWrongArtifact>>

RestartHost ==
    /\ restartCount = 0
    /\ restartCount' = 1
    /\ hosted' =
        {g \in replicaCatalog \cap desired:
            catalogArtifact[g] \in {"none", expectedArtifact[g]}}
    /\ active' = hosted'
    /\ routes' = hosted'
    /\ restoreIntent' = restoreIntent \cap desired
    /\ bootstrapStatus' =
        [g \in Groups |->
            IF g \in desired THEN bootstrapStatus[g] ELSE "none"]
    /\ UNCHANGED <<desired, durableApplyStore, replicaCatalog,
                  expectedArtifact, preparedArtifact, artifactVerified,
                  catalogArtifact, activatedWithWrongArtifact>>

Next ==
    \/ RestartHost
    \/ \E g \in Groups:
        \/ MetadataAdds(g)
        \/ MetadataRemoves(g)
        \/ EnsureFreshReplica(g)
        \/ StartBackupRestore(g)
        \/ PrepareRestoreArtifact(g, expectedArtifact[g])
        \/ PrepareRestoreArtifact(g, "other")
        \/ CompleteBackupRestore(g)
        \/ FailBackupRestore(g)
        \/ RemoveUndesiredReplica(g)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ desired \subseteq Groups
    /\ hosted \subseteq Groups
    /\ active \subseteq Groups
    /\ routes \subseteq Groups
    /\ durableApplyStore \subseteq Groups
    /\ replicaCatalog \subseteq Groups
    /\ restoreIntent \subseteq Groups
    /\ bootstrapStatus \in [Groups -> BootstrapStatuses]
    /\ restartCount \in 0..1
    /\ expectedArtifact \in [Groups -> Artifacts]
    /\ preparedArtifact \in [Groups -> Artifacts]
    /\ artifactVerified \subseteq Groups
    /\ catalogArtifact \in [Groups -> Artifacts]
    /\ activatedWithWrongArtifact \in BOOLEAN

ActiveReplicasAreHosted ==
    active \subseteq hosted

RoutesOnlyForActiveReplicas ==
    routes \subseteq active

NoUndesiredActiveReplica ==
    active \subseteq desired

NoUndesiredRoute ==
    routes \subseteq desired

DurableStoreForHostedReplica ==
    hosted \subseteq durableApplyStore

CatalogOnlyForDesiredReplica ==
    replicaCatalog \subseteq desired

RestoreBootstrapRequiresDesiredGroup ==
    /\ restoreIntent \subseteq desired
    /\ \A g \in Groups: bootstrapStatus[g] # "none" => g \in desired

RestoreDoesNotActivateBeforeSuccess ==
    \A g \in Groups:
        bootstrapStatus[g] = "preparing" =>
            /\ g \notin hosted
            /\ g \notin active
            /\ g \notin routes

RestartRestoresOnlyCatalogedDesiredReplicas ==
    restartCount = 1 =>
        /\ active \subseteq replicaCatalog
        /\ routes \subseteq replicaCatalog

VerifiedRestoreUsesCommittedArtifact ==
    /\ ~activatedWithWrongArtifact
    /\ \A g \in artifactVerified:
        preparedArtifact[g] = expectedArtifact[g]

RestoredCatalogRetainsArtifactBinding ==
    \A g \in Groups:
        bootstrapStatus[g] = "succeeded" /\ g \in replicaCatalog =>
            /\ expectedArtifact[g] # "none"
            /\ catalogArtifact[g] = expectedArtifact[g]

RestoredActivationRequiresVerifiedArtifact ==
    \A g \in Groups:
        bootstrapStatus[g] = "succeeded" /\ g \in active =>
            /\ g \in artifactVerified
            /\ preparedArtifact[g] = expectedArtifact[g]

Safety ==
    /\ TypeOK
    /\ ActiveReplicasAreHosted
    /\ RoutesOnlyForActiveReplicas
    /\ NoUndesiredActiveReplica
    /\ NoUndesiredRoute
    /\ DurableStoreForHostedReplica
    /\ CatalogOnlyForDesiredReplica
    /\ RestoreBootstrapRequiresDesiredGroup
    /\ RestoreDoesNotActivateBeforeSuccess
    /\ RestartRestoresOnlyCatalogedDesiredReplicas
    /\ VerifiedRestoreUsesCommittedArtifact
    /\ RestoredCatalogRetainsArtifactBinding
    /\ RestoredActivationRequiresVerifiedArtifact

=============================================================================
