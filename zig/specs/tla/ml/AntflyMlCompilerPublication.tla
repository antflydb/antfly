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

----------------------- MODULE AntflyMlCompilerPublication --------------------
(*
  Bounded model for the partition-export -> compiler-artifact -> runtime
  executor publication boundary.

  This complements AntflyMlGraphPasses.tla. That model checks graph pass and
  export graph-reference safety. This one checks the lower-level publication
  contract for PJRT/native compiler artifacts:

  - export materializes graph parameters and semantic KV cache inputs as runtime
    inputs;
  - compile artifacts are based on a complete export of the current graph
    version;
  - selected runtime outputs do not leak semantic KV side outputs;
  - failed/partial compiler artifacts are not published;
  - fallback partitions remain fail-closed before executor attachment.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    BuggyStaleCompile,
    BuggyMissingExternalInput,
    BuggyWrongOutputSelection,
    BuggyFallbackPublish,
    BuggyPartialArtifactPublish

Versions == 0..2
Inputs == {"weight", "bias", "pastK", "pastV"}
Params == {"weight", "bias"}
SemanticKvInputs == {"pastK", "pastV"}
RequiredInputs == Params \cup SemanticKvInputs

Outputs == {"main", "presentK", "presentV"}
RequiredExportOutputs == Outputs
SelectedRuntimeOutputs == {"main"}

VARIABLES
    graphVersion,
    exportVersion,
    exportComplete,
    exportedInputs,
    exportedOutputs,
    compileVersion,
    compileComplete,
    compiledInputs,
    compiledOutputs,
    compileFailed,
    partialArtifactVisible,
    fallbackPartition,
    requireNoFallback,
    runtimePublished,
    runtimeVersion,
    runtimeVisibleOutputs

vars ==
    <<graphVersion, exportVersion, exportComplete, exportedInputs,
      exportedOutputs, compileVersion, compileComplete, compiledInputs,
      compiledOutputs, compileFailed, partialArtifactVisible,
      fallbackPartition, requireNoFallback, runtimePublished, runtimeVersion,
      runtimeVisibleOutputs>>

Init ==
    /\ graphVersion = 1
    /\ exportVersion = 0
    /\ exportComplete = FALSE
    /\ exportedInputs = {}
    /\ exportedOutputs = {}
    /\ compileVersion = 0
    /\ compileComplete = FALSE
    /\ compiledInputs = {}
    /\ compiledOutputs = {}
    /\ compileFailed = FALSE
    /\ partialArtifactVisible = FALSE
    /\ fallbackPartition = TRUE
    /\ requireNoFallback = TRUE
    /\ runtimePublished = FALSE
    /\ runtimeVersion = 0
    /\ runtimeVisibleOutputs = {}

UpdateGraph ==
    /\ graphVersion = 1
    /\ ~runtimePublished
    /\ graphVersion' = 2
    /\ UNCHANGED <<exportVersion, exportComplete, exportedInputs,
                  exportedOutputs, compileVersion, compileComplete,
                  compiledInputs, compiledOutputs, compileFailed,
                  partialArtifactVisible, fallbackPartition, requireNoFallback,
                  runtimePublished, runtimeVersion, runtimeVisibleOutputs>>

ExportPartition ==
    /\ ~runtimePublished
    /\ exportVersion # graphVersion
    /\ exportVersion' = graphVersion
    /\ exportComplete' = TRUE
    /\ exportedInputs' =
        IF BuggyMissingExternalInput THEN
            RequiredInputs \ {"pastV"}
        ELSE
            RequiredInputs
    /\ exportedOutputs' = RequiredExportOutputs
    /\ compileVersion' = 0
    /\ compileComplete' = FALSE
    /\ compiledInputs' = {}
    /\ compiledOutputs' = {}
    /\ compileFailed' = FALSE
    /\ partialArtifactVisible' = FALSE
    /\ UNCHANGED <<graphVersion, fallbackPartition, requireNoFallback,
                  runtimePublished, runtimeVersion, runtimeVisibleOutputs>>

CompileArtifact ==
    /\ exportComplete
    /\ ~compileComplete
    /\ IF BuggyStaleCompile THEN TRUE ELSE exportVersion = graphVersion
    /\ compileVersion' = exportVersion
    /\ compileComplete' = TRUE
    /\ compiledInputs' = exportedInputs
    /\ compiledOutputs' = exportedOutputs
    /\ UNCHANGED <<graphVersion, exportVersion, exportComplete, exportedInputs,
                  exportedOutputs, compileFailed, partialArtifactVisible,
                  fallbackPartition, requireNoFallback, runtimePublished,
                  runtimeVersion, runtimeVisibleOutputs>>

FailCompile ==
    /\ ~compileComplete
    /\ ~compileFailed
    /\ compileFailed' = TRUE
    /\ partialArtifactVisible' = BuggyPartialArtifactPublish
    /\ UNCHANGED <<graphVersion, exportVersion, exportComplete, exportedInputs,
                  exportedOutputs, compileVersion, compileComplete,
                  compiledInputs, compiledOutputs, fallbackPartition,
                  requireNoFallback, runtimePublished, runtimeVersion,
                  runtimeVisibleOutputs>>

ClearFallbackPartition ==
    /\ fallbackPartition
    /\ fallbackPartition' = FALSE
    /\ UNCHANGED <<graphVersion, exportVersion, exportComplete, exportedInputs,
                  exportedOutputs, compileVersion, compileComplete,
                  compiledInputs, compiledOutputs, compileFailed,
                  partialArtifactVisible, requireNoFallback, runtimePublished,
                  runtimeVersion, runtimeVisibleOutputs>>

PublishRuntime ==
    /\ compileComplete
    /\ ~runtimePublished
    /\ ~partialArtifactVisible
    /\ IF BuggyStaleCompile THEN TRUE ELSE compileVersion = graphVersion
    /\ IF BuggyFallbackPublish THEN TRUE ELSE ~(requireNoFallback /\ fallbackPartition)
    /\ runtimePublished' = TRUE
    /\ runtimeVersion' = compileVersion
    /\ runtimeVisibleOutputs' =
        IF BuggyWrongOutputSelection THEN
            {"main", "presentK"}
        ELSE
            SelectedRuntimeOutputs
    /\ UNCHANGED <<graphVersion, exportVersion, exportComplete, exportedInputs,
                  exportedOutputs, compileVersion, compileComplete,
                  compiledInputs, compiledOutputs, compileFailed,
                  partialArtifactVisible, fallbackPartition, requireNoFallback>>

Next ==
    \/ UpdateGraph
    \/ ExportPartition
    \/ CompileArtifact
    \/ FailCompile
    \/ ClearFallbackPartition
    \/ PublishRuntime

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ graphVersion \in Versions
    /\ exportVersion \in Versions
    /\ exportComplete \in BOOLEAN
    /\ exportedInputs \subseteq Inputs
    /\ exportedOutputs \subseteq Outputs
    /\ compileVersion \in Versions
    /\ compileComplete \in BOOLEAN
    /\ compiledInputs \subseteq Inputs
    /\ compiledOutputs \subseteq Outputs
    /\ compileFailed \in BOOLEAN
    /\ partialArtifactVisible \in BOOLEAN
    /\ fallbackPartition \in BOOLEAN
    /\ requireNoFallback \in BOOLEAN
    /\ runtimePublished \in BOOLEAN
    /\ runtimeVersion \in Versions
    /\ runtimeVisibleOutputs \subseteq Outputs

ExportMaterializesRequiredRuntimeInputs ==
    exportComplete => RequiredInputs \subseteq exportedInputs

ExportIncludesCompilerOutputs ==
    exportComplete => RequiredExportOutputs \subseteq exportedOutputs

CompileUsesFreshCompleteExport ==
    compileComplete =>
        /\ exportComplete
        /\ compileVersion = exportVersion
        /\ RequiredInputs \subseteq compiledInputs
        /\ RequiredExportOutputs \subseteq compiledOutputs

FailedCompileArtifactNotVisible ==
    compileFailed => ~partialArtifactVisible

RuntimePublishesOnlyFreshCompleteArtifact ==
    runtimePublished =>
        /\ compileComplete
        /\ runtimeVersion = graphVersion
        /\ runtimeVersion = compileVersion

RuntimeOutputSelectionIsExact ==
    runtimePublished => runtimeVisibleOutputs = SelectedRuntimeOutputs

RuntimeGateFailsClosed ==
    runtimePublished => ~(requireNoFallback /\ fallbackPartition)

Safety ==
    /\ TypeOK
    /\ ExportMaterializesRequiredRuntimeInputs
    /\ ExportIncludesCompilerOutputs
    /\ CompileUsesFreshCompleteExport
    /\ FailedCompileArtifactNotVisible
    /\ RuntimePublishesOnlyFreshCompleteArtifact
    /\ RuntimeOutputSelectionIsExact
    /\ RuntimeGateFailsClosed

=============================================================================
