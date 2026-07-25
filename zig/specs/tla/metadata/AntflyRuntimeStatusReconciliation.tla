-------------------- MODULE AntflyRuntimeStatusReconciliation -------------------
(*
Code anchors:
  - pkg/antfly/src/api/http_server.zig runtimeStatusPreferred and authoritative
    per-group runtime observation selection.
  - pkg/antfly/src/data/runtime.zig storage-root generation disk cache.
  - pkg/antfly/src/metadata/reconciler.zig schema-progress aggregation and
    maybeFinalizeSchemaMigration.
  - pkg/antfly/src/storage/ha/read_gate.zig standby read availability.
What this proves:
  - Older, wrong-root, removed-owner, placeholder, or incomplete observations
    cannot displace current authoritative runtime facts.
  - Capacity/join planning consumes complete fresh known disk facts only.
  - Schema migration finalizes only after current progress covers every hosted
    range on every serving replica, while the old read schema remains available
    until that boundary.
Deliberate omissions:
  - JSON encoding, byte counts beyond known/unknown, continuous timestamps,
    schema payload semantics, index build internals, and network transport.
State bounds:
  - Two nodes, two ranges, two topology/root generations, one migration, and
    bounded status publications.
Make targets:
  - AntflyRuntimeStatusReconciliation, heavy-liveness, and its Bad* checks.
Correspondence tier:
  - Mature: runtime-status, join planner, migration, and HA read tests anchor it.
*)

EXTENDS Naturals, FiniteSets

CONSTANTS BuggyOlderStatusWins, BuggyRemovedOwnerWins,
          BuggyCrossRootDiskReuse, BuggyUnknownZeroIsKnown,
          BuggyPartialJoinStats, BuggyPartialSchemaFinalize,
          BuggyStaleSchemaProgress, BuggyDropOldReadSchema

ASSUME /\ BuggyOlderStatusWins \in BOOLEAN
       /\ BuggyRemovedOwnerWins \in BOOLEAN
       /\ BuggyCrossRootDiskReuse \in BOOLEAN
       /\ BuggyUnknownZeroIsKnown \in BOOLEAN
       /\ BuggyPartialJoinStats \in BOOLEAN
       /\ BuggyPartialSchemaFinalize \in BOOLEAN
       /\ BuggyStaleSchemaProgress \in BOOLEAN
       /\ BuggyDropOldReadSchema \in BOOLEAN

Nodes == {"n1", "n2"}
Ranges == {"r1", "r2"}
Schemas == {1, 2}
Hosted == [n \in Nodes |-> Ranges]
NoNat == [n \in Nodes |-> 0]
NoBool == [n \in Nodes |-> FALSE]
NoRanges == [n \in Nodes |-> {}]
OldSchemaOnly == [n \in Nodes |-> {1}]

VARIABLES topologyGen, activeNodes, rootGen,
          localDiskKnown, localDiskBytes, localDiskEvidenceRoot,
          schemaBuilt, availableSchemas,
          selectedTopology, selectedRoot, selectedStatusGen, selectedFresh,
          selectedSource, selectedDiskKnown, selectedDiskBytes,
          selectedDiskEvidenceRoot, selectedSchema, selectedSchemaCurrent,
          staleDisplaced, removedOwnerSelected, joinPlanned, joinUsedIncomplete,
          readSchema, targetSchema, migrationFinalized, finalizedWithoutCoverage

vars ==
    <<topologyGen, activeNodes, rootGen,
      localDiskKnown, localDiskBytes, localDiskEvidenceRoot,
      schemaBuilt, availableSchemas,
      selectedTopology, selectedRoot, selectedStatusGen, selectedFresh,
      selectedSource, selectedDiskKnown, selectedDiskBytes,
      selectedDiskEvidenceRoot, selectedSchema, selectedSchemaCurrent,
      staleDisplaced, removedOwnerSelected, joinPlanned, joinUsedIncomplete,
      readSchema, targetSchema, migrationFinalized, finalizedWithoutCoverage>>

Init ==
    /\ topologyGen = 1
    /\ activeNodes = Nodes
    /\ rootGen = [n \in Nodes |-> 1]
    /\ localDiskKnown = NoBool
    /\ localDiskBytes = NoNat
    /\ localDiskEvidenceRoot = NoNat
    /\ schemaBuilt = NoRanges
    /\ availableSchemas = OldSchemaOnly
    /\ selectedTopology = NoNat
    /\ selectedRoot = NoNat
    /\ selectedStatusGen = NoNat
    /\ selectedFresh = NoBool
    /\ selectedSource = [n \in Nodes |-> "placeholder"]
    /\ selectedDiskKnown = NoBool
    /\ selectedDiskBytes = NoNat
    /\ selectedDiskEvidenceRoot = NoNat
    /\ selectedSchema = NoRanges
    /\ selectedSchemaCurrent = NoBool
    /\ staleDisplaced = FALSE
    /\ removedOwnerSelected = FALSE
    /\ joinPlanned = FALSE
    /\ joinUsedIncomplete = FALSE
    /\ readSchema = 1
    /\ targetSchema = 2
    /\ migrationFinalized = FALSE
    /\ finalizedWithoutCoverage = FALSE

BuildTargetSchema(n, r) ==
    /\ n \in activeNodes
    /\ r \in Hosted[n] \ schemaBuilt[n]
    /\ schemaBuilt' = [schemaBuilt EXCEPT ![n] = @ \cup {r}]
    /\ availableSchemas' =
        IF schemaBuilt'[n] = Hosted[n]
        THEN [availableSchemas EXCEPT ![n] = @ \cup {targetSchema}]
        ELSE availableSchemas
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, selectedTopology,
                  selectedRoot, selectedStatusGen, selectedFresh,
                  selectedSource, selectedDiskKnown, selectedDiskBytes,
                  selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

RefreshDiskFacts(n) ==
    /\ n \in activeNodes
    /\ ~localDiskKnown[n] \/ localDiskEvidenceRoot[n] # rootGen[n]
    /\ localDiskKnown' = [localDiskKnown EXCEPT ![n] = TRUE]
    /\ localDiskBytes' = [localDiskBytes EXCEPT ![n] = 1]
    /\ localDiskEvidenceRoot' =
        [localDiskEvidenceRoot EXCEPT ![n] = rootGen[n]]
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, schemaBuilt,
                  availableSchemas, selectedTopology, selectedRoot,
                  selectedStatusGen, selectedFresh, selectedSource,
                  selectedDiskKnown, selectedDiskBytes,
                  selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

FreshPayloadDiffers(n) ==
    \/ selectedTopology[n] # topologyGen
    \/ selectedRoot[n] # rootGen[n]
    \/ ~selectedFresh[n]
    \/ selectedSource[n] # "runtime"
    \/ selectedDiskKnown[n] # localDiskKnown[n]
    \/ selectedDiskBytes[n] # localDiskBytes[n]
    \/ selectedDiskEvidenceRoot[n] # localDiskEvidenceRoot[n]
    \/ selectedSchema[n] # schemaBuilt[n]
    \/ ~selectedSchemaCurrent[n]

PublishFreshStatus(n) ==
    /\ n \in activeNodes
    /\ FreshPayloadDiffers(n)
    /\ selectedTopology' = [selectedTopology EXCEPT ![n] = topologyGen]
    /\ selectedRoot' = [selectedRoot EXCEPT ![n] = rootGen[n]]
    /\ selectedStatusGen' = [selectedStatusGen EXCEPT ![n] = 1]
    /\ selectedFresh' = [selectedFresh EXCEPT ![n] = TRUE]
    /\ selectedSource' = [selectedSource EXCEPT ![n] = "runtime"]
    /\ selectedDiskKnown' =
        [selectedDiskKnown EXCEPT ![n] = localDiskKnown[n]]
    /\ selectedDiskBytes' =
        [selectedDiskBytes EXCEPT ![n] = localDiskBytes[n]]
    /\ selectedDiskEvidenceRoot' =
        [selectedDiskEvidenceRoot EXCEPT ![n] = localDiskEvidenceRoot[n]]
    /\ selectedSchema' = [selectedSchema EXCEPT ![n] = schemaBuilt[n]]
    /\ selectedSchemaCurrent' =
        [selectedSchemaCurrent EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  availableSchemas, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

PublishOlderStatus(n) ==
    /\ n \in activeNodes
    /\ selectedStatusGen[n] > 0
    /\ IF BuggyOlderStatusWins
       THEN /\ selectedTopology' = [selectedTopology EXCEPT ![n] = 0]
            /\ selectedRoot' = [selectedRoot EXCEPT ![n] = 0]
            /\ selectedStatusGen' = [selectedStatusGen EXCEPT ![n] = 0]
            /\ selectedFresh' = [selectedFresh EXCEPT ![n] = FALSE]
            /\ selectedSource' = [selectedSource EXCEPT ![n] = "placeholder"]
            /\ selectedDiskKnown' = [selectedDiskKnown EXCEPT ![n] = FALSE]
            /\ selectedDiskBytes' = [selectedDiskBytes EXCEPT ![n] = 0]
            /\ selectedDiskEvidenceRoot' =
                [selectedDiskEvidenceRoot EXCEPT ![n] = 0]
            /\ selectedSchema' = [selectedSchema EXCEPT ![n] = {}]
            /\ selectedSchemaCurrent' =
                [selectedSchemaCurrent EXCEPT ![n] = FALSE]
            /\ staleDisplaced' = TRUE
       ELSE /\ UNCHANGED <<selectedTopology, selectedRoot, selectedStatusGen,
                           selectedFresh, selectedSource, selectedDiskKnown,
                           selectedDiskBytes, selectedDiskEvidenceRoot,
                           selectedSchema, selectedSchemaCurrent,
                           staleDisplaced>>
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  availableSchemas, removedOwnerSelected, joinPlanned,
                  joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

RotateStorageRoot(n) ==
    /\ n \in activeNodes
    /\ rootGen[n] = 1
    /\ rootGen' = [rootGen EXCEPT ![n] = 2]
    /\ IF BuggyCrossRootDiskReuse
       THEN /\ UNCHANGED <<localDiskKnown, localDiskBytes,
                           localDiskEvidenceRoot>>
       ELSE /\ localDiskKnown' = [localDiskKnown EXCEPT ![n] = FALSE]
            /\ localDiskBytes' = [localDiskBytes EXCEPT ![n] = 0]
            /\ localDiskEvidenceRoot' =
                [localDiskEvidenceRoot EXCEPT ![n] = 0]
    /\ UNCHANGED <<topologyGen, activeNodes, schemaBuilt, availableSchemas,
                  selectedTopology, selectedRoot, selectedStatusGen,
                  selectedFresh, selectedSource, selectedDiskKnown,
                  selectedDiskBytes, selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

PublishUnknownZeroAsKnown(n) ==
    /\ BuggyUnknownZeroIsKnown
    /\ n \in activeNodes
    /\ selectedTopology' = [selectedTopology EXCEPT ![n] = topologyGen]
    /\ selectedRoot' = [selectedRoot EXCEPT ![n] = rootGen[n]]
    /\ selectedStatusGen' = [selectedStatusGen EXCEPT ![n] = 1]
    /\ selectedFresh' = [selectedFresh EXCEPT ![n] = TRUE]
    /\ selectedSource' = [selectedSource EXCEPT ![n] = "runtime"]
    /\ selectedDiskKnown' = [selectedDiskKnown EXCEPT ![n] = TRUE]
    /\ selectedDiskBytes' = [selectedDiskBytes EXCEPT ![n] = 0]
    /\ selectedDiskEvidenceRoot' =
        [selectedDiskEvidenceRoot EXCEPT ![n] = 0]
    /\ selectedSchema' = [selectedSchema EXCEPT ![n] = schemaBuilt[n]]
    /\ selectedSchemaCurrent' =
        [selectedSchemaCurrent EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  availableSchemas, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

RemoveOwner(n) ==
    /\ n \in activeNodes
    /\ Cardinality(activeNodes) > 1
    /\ activeNodes' = activeNodes \ {n}
    /\ topologyGen' = 2
    /\ UNCHANGED <<rootGen, localDiskKnown, localDiskBytes,
                  localDiskEvidenceRoot, schemaBuilt, availableSchemas,
                  selectedTopology, selectedRoot, selectedStatusGen,
                  selectedFresh, selectedSource, selectedDiskKnown,
                  selectedDiskBytes, selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

PublishRemovedOwner(n) ==
    /\ n \in Nodes \ activeNodes
    /\ IF BuggyRemovedOwnerWins
       THEN /\ removedOwnerSelected' = TRUE
            /\ selectedFresh' = [selectedFresh EXCEPT ![n] = TRUE]
       ELSE /\ UNCHANGED <<removedOwnerSelected, selectedFresh>>
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  availableSchemas, selectedTopology, selectedRoot,
                  selectedStatusGen, selectedSource, selectedDiskKnown,
                  selectedDiskBytes, selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, joinPlanned,
                  joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

PublishStaleSchemaProgress(n) ==
    /\ BuggyStaleSchemaProgress
    /\ n \in activeNodes
    /\ selectedSchema' = [selectedSchema EXCEPT ![n] = Hosted[n]]
    /\ selectedSchemaCurrent' =
        [selectedSchemaCurrent EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  availableSchemas, selectedTopology, selectedRoot,
                  selectedStatusGen, selectedFresh, selectedSource,
                  selectedDiskKnown, selectedDiskBytes,
                  selectedDiskEvidenceRoot, staleDisplaced,
                  removedOwnerSelected, joinPlanned, joinUsedIncomplete,
                  readSchema, targetSchema, migrationFinalized,
                  finalizedWithoutCoverage>>

CompleteFreshFacts(n) ==
    /\ selectedTopology[n] = topologyGen
    /\ selectedRoot[n] = rootGen[n]
    /\ selectedFresh[n]
    /\ selectedSource[n] = "runtime"
    /\ selectedDiskKnown[n]
    /\ selectedDiskEvidenceRoot[n] = selectedRoot[n]

PlanJoin ==
    /\ ~joinPlanned
    /\ IF BuggyPartialJoinStats
       THEN \E n \in activeNodes: selectedFresh[n]
       ELSE \A n \in activeNodes: CompleteFreshFacts(n)
    /\ joinPlanned' = TRUE
    /\ joinUsedIncomplete' =
        ~(\A n \in activeNodes: CompleteFreshFacts(n))
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  availableSchemas, selectedTopology, selectedRoot,
                  selectedStatusGen, selectedFresh, selectedSource,
                  selectedDiskKnown, selectedDiskBytes,
                  selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, removedOwnerSelected,
                  readSchema, targetSchema, migrationFinalized,
                  finalizedWithoutCoverage>>

NodeMigrationComplete(n) ==
    /\ selectedSchema[n] = Hosted[n]
    /\ selectedSchemaCurrent[n]
    /\ selectedTopology[n] = topologyGen
    /\ selectedFresh[n]

FinalizeSchemaMigration ==
    /\ ~migrationFinalized
    /\ IF BuggyPartialSchemaFinalize
       THEN \E n \in activeNodes: selectedSchema[n] # {}
       ELSE IF BuggyStaleSchemaProgress
            THEN \A n \in activeNodes: selectedSchema[n] = Hosted[n]
            ELSE \A n \in activeNodes: NodeMigrationComplete(n)
    /\ finalizedWithoutCoverage' =
        ~(\A n \in activeNodes: NodeMigrationComplete(n))
    /\ migrationFinalized' = TRUE
    /\ readSchema' = targetSchema
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  availableSchemas, selectedTopology, selectedRoot,
                  selectedStatusGen, selectedFresh, selectedSource,
                  selectedDiskKnown, selectedDiskBytes,
                  selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, targetSchema>>

DropOldReadSchema(n) ==
    /\ n \in activeNodes
    /\ 1 \in availableSchemas[n]
    /\ migrationFinalized \/ BuggyDropOldReadSchema
    /\ availableSchemas' =
        [availableSchemas EXCEPT ![n] = @ \ {1}]
    /\ UNCHANGED <<topologyGen, activeNodes, rootGen, localDiskKnown,
                  localDiskBytes, localDiskEvidenceRoot, schemaBuilt,
                  selectedTopology, selectedRoot, selectedStatusGen,
                  selectedFresh, selectedSource, selectedDiskKnown,
                  selectedDiskBytes, selectedDiskEvidenceRoot, selectedSchema,
                  selectedSchemaCurrent, staleDisplaced, removedOwnerSelected,
                  joinPlanned, joinUsedIncomplete, readSchema, targetSchema,
                  migrationFinalized, finalizedWithoutCoverage>>

Next ==
    \/ PlanJoin
    \/ FinalizeSchemaMigration
    \/ \E n \in Nodes:
        \/ RefreshDiskFacts(n)
        \/ PublishFreshStatus(n)
        \/ PublishOlderStatus(n)
        \/ RotateStorageRoot(n)
        \/ PublishUnknownZeroAsKnown(n)
        \/ RemoveOwner(n)
        \/ PublishRemovedOwner(n)
        \/ PublishStaleSchemaProgress(n)
        \/ DropOldReadSchema(n)
        \/ \E r \in Ranges: BuildTargetSchema(n, r)

Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ \A n \in Nodes:
        /\ WF_vars(RefreshDiskFacts(n))
        /\ WF_vars(PublishFreshStatus(n))
        /\ \A r \in Ranges: WF_vars(BuildTargetSchema(n, r))
    /\ WF_vars(FinalizeSchemaMigration)

NewerAuthoritativeStatusIsStable == ~staleDisplaced
RemovedOwnersAreIgnored == ~removedOwnerSelected

KnownDiskFactsMatchStorageRoot ==
    \A n \in activeNodes:
        selectedDiskKnown[n] =>
            selectedDiskEvidenceRoot[n] = selectedRoot[n]

JoinUsesCompleteFreshFacts == ~joinUsedIncomplete

SchemaFinalizesWithCompleteCurrentCoverage ==
    migrationFinalized => ~finalizedWithoutCoverage

ReadSchemaAvailableOnEveryServingReplica ==
    \A n \in activeNodes: readSchema \in availableSchemas[n]

MigrationEventuallyFinalizes == <>migrationFinalized

=============================================================================
