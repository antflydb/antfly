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

-------------------------- MODULE AntflyLitePublication --------------------------
(*
  Bounded Lite/serverless publication model.

  Concrete Zig contracts modeled:
    - serverless artifacts are written before a manifest references them.
    - manifest HEAD moves only after a complete manifest exists.
    - retry after manifest-write/no-head-advance can publish the same version.
    - query sessions pin one manifest generation while HEAD advances.
    - failed publication attempts cannot become visible.
    - cleanup/vacuum cannot delete reader-pinned generation data.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyManifestBeforeArtifacts,
    BuggyFailedPublicationAdvancesHead,
    BuggyCleanupPinnedReader,
    BuggyMixedVisibleGeneration

MaxGen == 2
Generations == 1..MaxGen
Versions == 0..MaxGen
Kinds == {"document", "mutation", "text", "vector", "sparse", "graph"}
BuildStates == {"idle", "building", "manifest_written", "failed"}

VARIABLES
    buildState,
    buildVersion,
    artifactVersion,
    manifestStoredVersion,
    manifestRefs,
    headVersion,
    visibleRefs,
    failedVersion,
    readerPinnedVersion,
    readerRefs,
    deletedGeneration

vars == <<buildState, buildVersion, artifactVersion, manifestStoredVersion,
          manifestRefs, headVersion, visibleRefs, failedVersion,
          readerPinnedVersion, readerRefs, deletedGeneration>>

NoRefs == [k \in Kinds |-> 0]

AllArtifactsReady(v) ==
    \A k \in Kinds : artifactVersion[k] = v

RefsAllVersion(refs, v) ==
    \A k \in Kinds : refs[k] = v

RefsPublished(refs) ==
    \A k \in Kinds : refs[k] = 0 \/ artifactVersion[k] >= refs[k]

NextVersionAvailable ==
    headVersion < MaxGen

Init ==
    /\ buildState = "idle"
    /\ buildVersion = 0
    /\ artifactVersion = NoRefs
    /\ manifestStoredVersion = 0
    /\ manifestRefs = NoRefs
    /\ headVersion = 0
    /\ visibleRefs = NoRefs
    /\ failedVersion = 0
    /\ readerPinnedVersion = 0
    /\ readerRefs = NoRefs
    /\ deletedGeneration = [v \in Generations |-> FALSE]

StartPublication ==
    /\ buildState = "idle"
    /\ NextVersionAvailable
    /\ buildState' = "building"
    /\ buildVersion' = headVersion + 1
    /\ failedVersion' = 0
    /\ UNCHANGED <<artifactVersion, manifestStoredVersion, manifestRefs,
                  headVersion, visibleRefs, readerPinnedVersion, readerRefs,
                  deletedGeneration>>

PublishArtifact(k) ==
    /\ buildState = "building"
    /\ k \in Kinds
    /\ artifactVersion[k] < buildVersion
    /\ artifactVersion' = [artifactVersion EXCEPT ![k] = buildVersion]
    /\ UNCHANGED <<buildState, buildVersion, manifestStoredVersion,
                  manifestRefs, headVersion, visibleRefs, failedVersion,
                  readerPinnedVersion, readerRefs, deletedGeneration>>

WriteManifest ==
    /\ buildState = "building"
    /\ AllArtifactsReady(buildVersion)
    /\ manifestStoredVersion' = buildVersion
    /\ manifestRefs' = [k \in Kinds |-> buildVersion]
    /\ buildState' = "manifest_written"
    /\ UNCHANGED <<buildVersion, artifactVersion, headVersion, visibleRefs,
                  failedVersion, readerPinnedVersion, readerRefs,
                  deletedGeneration>>

BuggyWriteManifestBeforeArtifacts ==
    /\ BuggyManifestBeforeArtifacts
    /\ buildState = "building"
    /\ ~AllArtifactsReady(buildVersion)
    /\ manifestStoredVersion' = buildVersion
    /\ manifestRefs' = [k \in Kinds |-> buildVersion]
    /\ buildState' = "manifest_written"
    /\ UNCHANGED <<buildVersion, artifactVersion, headVersion, visibleRefs,
                  failedVersion, readerPinnedVersion, readerRefs,
                  deletedGeneration>>

AdvanceHead ==
    /\ buildState = "manifest_written"
    /\ manifestStoredVersion = buildVersion
    /\ RefsPublished(manifestRefs)
    /\ headVersion' = buildVersion
    /\ visibleRefs' = manifestRefs
    /\ buildState' = "idle"
    /\ buildVersion' = 0
    /\ failedVersion' = 0
    /\ UNCHANGED <<artifactVersion, manifestStoredVersion, manifestRefs,
                  readerPinnedVersion, readerRefs, deletedGeneration>>

CrashAfterManifestBeforeHead ==
    /\ buildState = "manifest_written"
    /\ buildState' = "idle"
    /\ buildVersion' = 0
    /\ UNCHANGED <<artifactVersion, manifestStoredVersion, manifestRefs,
                  headVersion, visibleRefs, failedVersion, readerPinnedVersion,
                  readerRefs, deletedGeneration>>

RetryManifestHeadAdvance ==
    /\ buildState = "idle"
    /\ manifestStoredVersion = headVersion + 1
    /\ manifestStoredVersion <= MaxGen
    /\ RefsPublished(manifestRefs)
    /\ headVersion' = manifestStoredVersion
    /\ visibleRefs' = manifestRefs
    /\ failedVersion' = 0
    /\ UNCHANGED <<buildState, buildVersion, artifactVersion,
                  manifestStoredVersion, manifestRefs, readerPinnedVersion,
                  readerRefs, deletedGeneration>>

FailPublication ==
    /\ buildState = "building"
    /\ failedVersion' = buildVersion
    /\ buildState' = "failed"
    /\ UNCHANGED <<buildVersion, artifactVersion, manifestStoredVersion,
                  manifestRefs, headVersion, visibleRefs, readerPinnedVersion,
                  readerRefs, deletedGeneration>>

DiscardFailedPublication ==
    /\ buildState = "failed"
    /\ buildState' = "idle"
    /\ buildVersion' = 0
    /\ UNCHANGED <<artifactVersion, manifestStoredVersion, manifestRefs,
                  headVersion, visibleRefs, failedVersion, readerPinnedVersion,
                  readerRefs, deletedGeneration>>

BuggyAdvanceFailedPublication ==
    /\ BuggyFailedPublicationAdvancesHead
    /\ buildState = "failed"
    /\ headVersion' = failedVersion
    /\ visibleRefs' = [k \in Kinds |-> failedVersion]
    /\ buildState' = "idle"
    /\ buildVersion' = 0
    /\ UNCHANGED <<artifactVersion, manifestStoredVersion, manifestRefs,
                  failedVersion, readerPinnedVersion, readerRefs,
                  deletedGeneration>>

OpenReader ==
    /\ readerPinnedVersion = 0
    /\ headVersion > 0
    /\ readerPinnedVersion' = headVersion
    /\ readerRefs' = visibleRefs
    /\ UNCHANGED <<buildState, buildVersion, artifactVersion,
                  manifestStoredVersion, manifestRefs, headVersion, visibleRefs,
                  failedVersion, deletedGeneration>>

CloseReader ==
    /\ readerPinnedVersion > 0
    /\ readerPinnedVersion' = 0
    /\ readerRefs' = NoRefs
    /\ UNCHANGED <<buildState, buildVersion, artifactVersion,
                  manifestStoredVersion, manifestRefs, headVersion, visibleRefs,
                  failedVersion, deletedGeneration>>

CleanupObsolete(v) ==
    /\ v \in Generations
    /\ v < headVersion
    /\ readerPinnedVersion # v
    /\ deletedGeneration' = [deletedGeneration EXCEPT ![v] = TRUE]
    /\ UNCHANGED <<buildState, buildVersion, artifactVersion,
                  manifestStoredVersion, manifestRefs, headVersion, visibleRefs,
                  failedVersion, readerPinnedVersion, readerRefs>>

BuggyCleanupPinned ==
    /\ BuggyCleanupPinnedReader
    /\ readerPinnedVersion > 0
    /\ deletedGeneration' = [deletedGeneration EXCEPT ![readerPinnedVersion] = TRUE]
    /\ UNCHANGED <<buildState, buildVersion, artifactVersion,
                  manifestStoredVersion, manifestRefs, headVersion, visibleRefs,
                  failedVersion, readerPinnedVersion, readerRefs>>

BuggyPublishMixedVisibleGeneration ==
    /\ BuggyMixedVisibleGeneration
    /\ buildState = "manifest_written"
    /\ headVersion' = buildVersion
    /\ visibleRefs' = [k \in Kinds |-> IF k = "document" THEN buildVersion ELSE headVersion]
    /\ buildState' = "idle"
    /\ buildVersion' = 0
    /\ UNCHANGED <<artifactVersion, manifestStoredVersion, manifestRefs,
                  failedVersion, readerPinnedVersion, readerRefs,
                  deletedGeneration>>

Next ==
    \/ StartPublication
    \/ \E k \in Kinds : PublishArtifact(k)
    \/ WriteManifest
    \/ BuggyWriteManifestBeforeArtifacts
    \/ AdvanceHead
    \/ CrashAfterManifestBeforeHead
    \/ RetryManifestHeadAdvance
    \/ FailPublication
    \/ DiscardFailedPublication
    \/ BuggyAdvanceFailedPublication
    \/ OpenReader
    \/ CloseReader
    \/ \E v \in Generations : CleanupObsolete(v)
    \/ BuggyCleanupPinned
    \/ BuggyPublishMixedVisibleGeneration

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ buildState \in BuildStates
    /\ buildVersion \in Versions
    /\ artifactVersion \in [Kinds -> Versions]
    /\ manifestStoredVersion \in Versions
    /\ manifestRefs \in [Kinds -> Versions]
    /\ headVersion \in Versions
    /\ visibleRefs \in [Kinds -> Versions]
    /\ failedVersion \in Versions
    /\ readerPinnedVersion \in Versions
    /\ readerRefs \in [Kinds -> Versions]
    /\ deletedGeneration \in [Generations -> BOOLEAN]

ManifestReferencesPublishedArtifacts ==
    manifestStoredVersion > 0 =>
        /\ RefsAllVersion(manifestRefs, manifestStoredVersion)
        /\ RefsPublished(manifestRefs)

VisibleManifestReferencesPublishedArtifacts ==
    headVersion > 0 =>
        /\ RefsAllVersion(visibleRefs, headVersion)
        /\ RefsPublished(visibleRefs)
        /\ ~deletedGeneration[headVersion]

ReaderGenerationIsPinnedAndConsistent ==
    readerPinnedVersion > 0 =>
        /\ RefsAllVersion(readerRefs, readerPinnedVersion)
        /\ RefsPublished(readerRefs)
        /\ ~deletedGeneration[readerPinnedVersion]

FailedPublicationCannotAdvanceVisibleGeneration ==
    failedVersion > 0 => headVersion # failedVersion

CleanupCannotDeleteReaderPinnedGeneration ==
    readerPinnedVersion > 0 => ~deletedGeneration[readerPinnedVersion]

HeadNeverExceedsStoredManifest ==
    headVersion <= manifestStoredVersion

Safety ==
    /\ TypeOK
    /\ ManifestReferencesPublishedArtifacts
    /\ VisibleManifestReferencesPublishedArtifacts
    /\ ReaderGenerationIsPinnedAndConsistent
    /\ FailedPublicationCannotAdvanceVisibleGeneration
    /\ CleanupCannotDeleteReaderPinnedGeneration
    /\ HeadNeverExceedsStoredManifest

=============================================================================
