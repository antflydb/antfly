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

------------------------- MODULE AntflyOpenApiCodegen -------------------------
(*
  Bounded OpenAPI checked-generation publication model.

  Concrete Zig/repo contracts modeled:
    - `make openapi-check` first checks that root `../openapi.yaml` matches the
      modular public OpenAPI specs via `join_public_openapi.py --compare`.
    - `zig build regen-openapi` regenerates many checked-in packages from
      concrete specs, generated modes, and import mappings in `zig/build.zig`.
    - Public server/extractor code is generated from the joined public spec,
      while the public client is generated from the prefixed public spec.
    - Checked generated packages may be transiently stale after a source spec
      edit, but `generated-check` must not pass while root, package, dependency,
      mode, or import mapping state is stale or partial.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyCheckStalePackage,
    BuggyCheckStaleRoot,
    BuggyPublicClientImportsInternal,
    BuggyCommitFailedPartial

Versions == 0..2
SourceVersions == 1..2
Sources == {"schema", "indexes", "metadata", "internal"}
Packages == {"schema", "indexes", "public", "client", "internal"}
PublicPackages == {"schema", "indexes", "public", "client"}
InternalOnlyPackages == {"internal"}
Modes == {"none", "types", "types_client", "types_server", "types_extractors"}

VARIABLES
    sourceVersion,
    joinedPublicVersion,
    prefixedPublicVersion,
    rootOpenApiVersion,
    generatedVersion,
    generatedMode,
    generatedImports,
    generatedComplete,
    committedVersion,
    committedMode,
    committedImports,
    committedComplete,
    checkPassed

vars == <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
          rootOpenApiVersion, generatedVersion, generatedMode,
          generatedImports, generatedComplete, committedVersion,
          committedMode, committedImports, committedComplete, checkPassed>>

NoVersionByPackage == [p \in Packages |-> 0]
InitialSourceVersions == [s \in Sources |-> 1]

ExpectedMode(p) ==
    CASE p = "client" -> "types_client"
      [] p = "internal" -> "types_server"
      [] p = "public" -> "types_extractors"
      [] OTHER -> "types"

ExpectedImports(p) ==
    CASE p = "indexes" -> {"schema"}
      [] p = "public" -> {"schema", "indexes"}
      [] p = "client" -> {"schema", "indexes"}
      [] OTHER -> {}

InitialMode == [p \in Packages |-> ExpectedMode(p)]
InitialImports == [p \in Packages |-> ExpectedImports(p)]

\* The public join/prefix steps are versioned as one aggregate public spec.
RequiredJoinedPublicVersion ==
    IF sourceVersion["schema"] = 2 \/ sourceVersion["indexes"] = 2 \/ sourceVersion["metadata"] = 2
    THEN 2
    ELSE 1

SpecViewReady(p) ==
    CASE p = "public" -> joinedPublicVersion = RequiredJoinedPublicVersion
      [] p = "client" -> /\ prefixedPublicVersion = joinedPublicVersion
                         /\ joinedPublicVersion = RequiredJoinedPublicVersion
      [] OTHER -> TRUE

CurrentSpecVersion(p) ==
    CASE p = "schema" -> sourceVersion["schema"]
      [] p = "indexes" -> sourceVersion["indexes"]
      [] p = "public" -> joinedPublicVersion
      [] p = "client" -> prefixedPublicVersion
      [] p = "internal" -> sourceVersion["internal"]

PackageShapeValid(version, mode, imports, complete, p) ==
    version[p] = 0 \/
        /\ complete[p]
        /\ mode[p] = ExpectedMode(p)
        /\ imports[p] = ExpectedImports(p)
        /\ p \in PublicPackages => imports[p] \cap InternalOnlyPackages = {}

PackageCurrent(p) ==
    /\ committedVersion[p] = CurrentSpecVersion(p)
    /\ committedComplete[p]
    /\ committedMode[p] = ExpectedMode(p)
    /\ committedImports[p] = ExpectedImports(p)

DependenciesCurrent(p) ==
    \A dep \in ExpectedImports(p) : PackageCurrent(dep)

AllPackagesCurrent ==
    \A p \in Packages : /\ PackageCurrent(p)
                         /\ DependenciesCurrent(p)

RootOpenApiCurrent ==
    /\ joinedPublicVersion = RequiredJoinedPublicVersion
    /\ prefixedPublicVersion = joinedPublicVersion
    /\ rootOpenApiVersion = prefixedPublicVersion

Init ==
    /\ sourceVersion = InitialSourceVersions
    /\ joinedPublicVersion = 1
    /\ prefixedPublicVersion = 1
    /\ rootOpenApiVersion = 1
    /\ generatedVersion = NoVersionByPackage
    /\ generatedMode = [p \in Packages |-> "none"]
    /\ generatedImports = [p \in Packages |-> {}]
    /\ generatedComplete = [p \in Packages |-> FALSE]
    /\ committedVersion = [p \in Packages |-> 1]
    /\ committedMode = InitialMode
    /\ committedImports = InitialImports
    /\ committedComplete = [p \in Packages |-> TRUE]
    /\ checkPassed = TRUE

EditSource(s) ==
    /\ s \in Sources
    /\ sourceVersion[s] = 1
    /\ sourceVersion' = [sourceVersion EXCEPT ![s] = 2]
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<joinedPublicVersion, prefixedPublicVersion, rootOpenApiVersion,
                  generatedVersion, generatedMode, generatedImports,
                  generatedComplete, committedVersion, committedMode,
                  committedImports, committedComplete>>

JoinPublicSpec ==
    /\ joinedPublicVersion # RequiredJoinedPublicVersion
    /\ joinedPublicVersion' = RequiredJoinedPublicVersion
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, prefixedPublicVersion, rootOpenApiVersion,
                  generatedVersion, generatedMode, generatedImports,
                  generatedComplete, committedVersion, committedMode,
                  committedImports, committedComplete>>

PrefixPublicSpec ==
    /\ joinedPublicVersion = RequiredJoinedPublicVersion
    /\ prefixedPublicVersion # joinedPublicVersion
    /\ prefixedPublicVersion' = joinedPublicVersion
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, rootOpenApiVersion,
                  generatedVersion, generatedMode, generatedImports,
                  generatedComplete, committedVersion, committedMode,
                  committedImports, committedComplete>>

UpdateRootOpenApi ==
    /\ prefixedPublicVersion = joinedPublicVersion
    /\ joinedPublicVersion = RequiredJoinedPublicVersion
    /\ rootOpenApiVersion # prefixedPublicVersion
    /\ rootOpenApiVersion' = prefixedPublicVersion
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  generatedVersion, generatedMode, generatedImports,
                  generatedComplete, committedVersion, committedMode,
                  committedImports, committedComplete>>

GeneratePackage(p) ==
    /\ p \in Packages
    /\ SpecViewReady(p)
    /\ generatedVersion' = [generatedVersion EXCEPT ![p] = CurrentSpecVersion(p)]
    /\ generatedMode' = [generatedMode EXCEPT ![p] = ExpectedMode(p)]
    /\ generatedImports' = [generatedImports EXCEPT ![p] = ExpectedImports(p)]
    /\ generatedComplete' = [generatedComplete EXCEPT ![p] = TRUE]
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, committedVersion, committedMode,
                  committedImports, committedComplete>>

GenerateFailedPartial(p) ==
    /\ p \in Packages
    /\ SpecViewReady(p)
    /\ generatedVersion' = [generatedVersion EXCEPT ![p] = CurrentSpecVersion(p)]
    /\ generatedMode' = [generatedMode EXCEPT ![p] = ExpectedMode(p)]
    /\ generatedImports' = [generatedImports EXCEPT ![p] = ExpectedImports(p)]
    /\ generatedComplete' = [generatedComplete EXCEPT ![p] = FALSE]
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, committedVersion, committedMode,
                  committedImports, committedComplete>>

CommitPackage(p) ==
    /\ p \in Packages
    /\ generatedComplete[p]
    /\ generatedVersion[p] = CurrentSpecVersion(p)
    /\ generatedMode[p] = ExpectedMode(p)
    /\ generatedImports[p] = ExpectedImports(p)
    /\ DependenciesCurrent(p)
    /\ committedVersion' = [committedVersion EXCEPT ![p] = generatedVersion[p]]
    /\ committedMode' = [committedMode EXCEPT ![p] = generatedMode[p]]
    /\ committedImports' = [committedImports EXCEPT ![p] = generatedImports[p]]
    /\ committedComplete' = [committedComplete EXCEPT ![p] = TRUE]
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, generatedVersion, generatedMode,
                  generatedImports, generatedComplete>>

RunGeneratedCheck ==
    /\ RootOpenApiCurrent
    /\ AllPackagesCurrent
    /\ checkPassed' = TRUE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, generatedVersion, generatedMode,
                  generatedImports, generatedComplete, committedVersion,
                  committedMode, committedImports, committedComplete>>

BuggyPassCheckWithStalePackage ==
    /\ BuggyCheckStalePackage
    /\ ~AllPackagesCurrent
    /\ checkPassed' = TRUE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, generatedVersion, generatedMode,
                  generatedImports, generatedComplete, committedVersion,
                  committedMode, committedImports, committedComplete>>

BuggyPassCheckWithStaleRoot ==
    /\ BuggyCheckStaleRoot
    /\ ~RootOpenApiCurrent
    /\ checkPassed' = TRUE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, generatedVersion, generatedMode,
                  generatedImports, generatedComplete, committedVersion,
                  committedMode, committedImports, committedComplete>>

BuggyCommitClientInternalImport ==
    /\ BuggyPublicClientImportsInternal
    /\ generatedVersion["client"] = CurrentSpecVersion("client")
    /\ generatedComplete["client"]
    /\ committedVersion' = [committedVersion EXCEPT !["client"] = generatedVersion["client"]]
    /\ committedMode' = [committedMode EXCEPT !["client"] = "types_client"]
    /\ committedImports' = [committedImports EXCEPT !["client"] = {"schema", "internal"}]
    /\ committedComplete' = [committedComplete EXCEPT !["client"] = TRUE]
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, generatedVersion, generatedMode,
                  generatedImports, generatedComplete>>

BuggyCommitFailedGeneratedPackage ==
    /\ BuggyCommitFailedPartial
    /\ \E p \in Packages :
        /\ generatedVersion[p] > 0
        /\ ~generatedComplete[p]
        /\ committedVersion' = [committedVersion EXCEPT ![p] = generatedVersion[p]]
        /\ committedMode' = [committedMode EXCEPT ![p] = generatedMode[p]]
        /\ committedImports' = [committedImports EXCEPT ![p] = generatedImports[p]]
        /\ committedComplete' = [committedComplete EXCEPT ![p] = FALSE]
    /\ checkPassed' = FALSE
    /\ UNCHANGED <<sourceVersion, joinedPublicVersion, prefixedPublicVersion,
                  rootOpenApiVersion, generatedVersion, generatedMode,
                  generatedImports, generatedComplete>>

Next ==
    \/ \E s \in Sources : EditSource(s)
    \/ JoinPublicSpec
    \/ PrefixPublicSpec
    \/ UpdateRootOpenApi
    \/ \E p \in Packages : GeneratePackage(p)
    \/ \E p \in Packages : GenerateFailedPartial(p)
    \/ \E p \in Packages : CommitPackage(p)
    \/ RunGeneratedCheck
    \/ BuggyPassCheckWithStalePackage
    \/ BuggyPassCheckWithStaleRoot
    \/ BuggyCommitClientInternalImport
    \/ BuggyCommitFailedGeneratedPackage

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ sourceVersion \in [Sources -> SourceVersions]
    /\ joinedPublicVersion \in SourceVersions
    /\ prefixedPublicVersion \in SourceVersions
    /\ rootOpenApiVersion \in SourceVersions
    /\ generatedVersion \in [Packages -> Versions]
    /\ generatedMode \in [Packages -> Modes]
    /\ generatedImports \in [Packages -> SUBSET Packages]
    /\ generatedComplete \in [Packages -> BOOLEAN]
    /\ committedVersion \in [Packages -> Versions]
    /\ committedMode \in [Packages -> Modes]
    /\ committedImports \in [Packages -> SUBSET Packages]
    /\ committedComplete \in [Packages -> BOOLEAN]
    /\ checkPassed \in BOOLEAN

GeneratedPackageShapeValid ==
    \A p \in Packages :
        generatedVersion[p] = 0 \/
            /\ generatedMode[p] = ExpectedMode(p)
            /\ generatedImports[p] = ExpectedImports(p)
            /\ p \in PublicPackages => generatedImports[p] \cap InternalOnlyPackages = {}

CommittedPackageShapeValid ==
    \A p \in Packages :
        PackageShapeValid(committedVersion, committedMode, committedImports, committedComplete, p)

NoCommittedPartialPackages ==
    \A p \in Packages : committedVersion[p] = 0 \/ committedComplete[p]

GeneratedCheckOnlyPassesForCurrentState ==
    checkPassed => /\ RootOpenApiCurrent
                   /\ AllPackagesCurrent

Safety ==
    /\ TypeOK
    /\ GeneratedPackageShapeValid
    /\ CommittedPackageShapeValid
    /\ NoCommittedPartialPackages
    /\ GeneratedCheckOnlyPassesForCurrentState

=============================================================================
