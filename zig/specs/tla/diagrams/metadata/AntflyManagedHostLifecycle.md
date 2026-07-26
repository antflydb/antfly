<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyManagedHostLifecycle — structural diagrams

Generated from [`AntflyManagedHostLifecycle.tla`](../../metadata/AntflyManagedHostLifecycle.tla). 14 state variables, 9 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `bootstrapStatus`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    preparing --> succeeded : CompleteBackupRestore
    preparing --> failed : FailBackupRestore
    classDef c_none fill:#2a78d630,stroke:#2a78d6
    class none c_none
    classDef c_preparing fill:#eb683430,stroke:#eb6834
    class preparing c_preparing
    classDef c_succeeded fill:#1baf7a30,stroke:#1baf7a
    class succeeded c_succeeded
    classDef c_failed fill:#eda10030,stroke:#eda100
    class failed c_failed
```

Writes whose source state is not statically determined:

- `MetadataRemoves` sets `bootstrapStatus` to `"none"`
- `RestartHost` sets `bootstrapStatus` to `"none"`
- `StartBackupRestore` sets `bootstrapStatus` to `"preparing"`

### `expectedArtifact`

Domain: `none`, `bound`, `other`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `MetadataRemoves` sets `expectedArtifact` to `"none"`
- `StartBackupRestore` sets `expectedArtifact` to `"bound"`

### `preparedArtifact`

Domain: `none`, `bound`, `other`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `MetadataRemoves` sets `preparedArtifact` to `"none"`
- `StartBackupRestore` sets `preparedArtifact` to `"none"`

### `catalogArtifact`

Domain: `none`, `bound`, `other`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `CompleteBackupRestore` sets `catalogArtifact` to `"none"`
- `MetadataRemoves` sets `catalogArtifact` to `"none"`
- `RemoveUndesiredReplica` sets `catalogArtifact` to `"none"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `RestartHost` | `desired`, `replicaCatalog`, `restoreIntent`, `bootstrapStatus`, `restartCount`, `expectedArtifact`, `catalogArtifact` | `hosted`, `active`, `routes`, `restoreIntent`, `bootstrapStatus`, `restartCount` |
| `MetadataAdds` | `desired` | `desired` |
| `MetadataRemoves` | `desired`, `active`, `routes`, `replicaCatalog`, `restoreIntent`, `bootstrapStatus`, `expectedArtifact`, `preparedArtifact`, `artifactVerified`, `catalogArtifact` | `desired`, `active`, `routes`, `replicaCatalog`, `restoreIntent`, `bootstrapStatus`, `expectedArtifact`, `preparedArtifact`, `artifactVerified`, `catalogArtifact` |
| `EnsureFreshReplica` | `desired`, `hosted`, `active`, `routes`, `durableApplyStore`, `replicaCatalog`, `restoreIntent` | `hosted`, `active`, `routes`, `durableApplyStore`, `replicaCatalog` |
| `StartBackupRestore` | `desired`, `hosted`, `active`, `routes`, `durableApplyStore`, `replicaCatalog`, `restoreIntent`, `bootstrapStatus`, `expectedArtifact`, `preparedArtifact`, `artifactVerified` | `hosted`, `active`, `routes`, `durableApplyStore`, `replicaCatalog`, `restoreIntent`, `bootstrapStatus`, `expectedArtifact`, `preparedArtifact`, `artifactVerified` |
| `PrepareRestoreArtifact` | `restoreIntent`, `bootstrapStatus`, `expectedArtifact`, `preparedArtifact`, `artifactVerified`, `activatedWithWrongArtifact` | `preparedArtifact`, `artifactVerified`, `activatedWithWrongArtifact` |
| `CompleteBackupRestore` | `desired`, `hosted`, `active`, `routes`, `durableApplyStore`, `replicaCatalog`, `restoreIntent`, `bootstrapStatus`, `preparedArtifact`, `artifactVerified`, `catalogArtifact` | `hosted`, `active`, `routes`, `durableApplyStore`, `replicaCatalog`, `bootstrapStatus`, `catalogArtifact` |
| `FailBackupRestore` | `restoreIntent`, `bootstrapStatus` | `bootstrapStatus` |
| `RemoveUndesiredReplica` | `desired`, `hosted`, `active`, `routes`, `replicaCatalog`, `catalogArtifact` | `hosted`, `active`, `routes`, `replicaCatalog`, `catalogArtifact` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[RestartHost]
        a1[MetadataAdds]
        a2[MetadataRemoves]
        a3[EnsureFreshReplica]
        a4[StartBackupRestore]
        a5[PrepareRestoreArtifact]
        a6[CompleteBackupRestore]
        a7[FailBackupRestore]
        a8[RemoveUndesiredReplica]
    end
    subgraph state["State variables"]
        v0([desired])
        v1([hosted])
        v2([active])
        v3([routes])
        v4([replicaCatalog])
        v5([restoreIntent])
        v6([bootstrapStatus])
        v7([restartCount])
        v8([expectedArtifact])
        v9([catalogArtifact])
        v10([preparedArtifact])
        v11([artifactVerified])
        v12([durableApplyStore])
        v13([activatedWithWrongArtifact])
    end
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v5
    a0 --> v6
    a0 --> v7
    v0 -.-> a0
    v4 -.-> a0
    v8 -.-> a0
    v9 -.-> a0
    a1 --> v0
    a2 --> v0
    a2 --> v2
    a2 --> v3
    a2 --> v4
    a2 --> v5
    a2 --> v6
    a2 --> v8
    a2 --> v10
    a2 --> v11
    a2 --> v9
    a3 --> v1
    a3 --> v2
    a3 --> v3
    a3 --> v12
    a3 --> v4
    v0 -.-> a3
    v5 -.-> a3
    a4 --> v1
    a4 --> v2
    a4 --> v3
    a4 --> v12
    a4 --> v4
    a4 --> v5
    a4 --> v6
    a4 --> v8
    a4 --> v10
    a4 --> v11
    v0 -.-> a4
    a5 --> v10
    a5 --> v11
    a5 --> v13
    v5 -.-> a5
    v6 -.-> a5
    v8 -.-> a5
    a6 --> v1
    a6 --> v2
    a6 --> v3
    a6 --> v12
    a6 --> v4
    a6 --> v6
    a6 --> v9
    v0 -.-> a6
    v5 -.-> a6
    v10 -.-> a6
    v11 -.-> a6
    a7 --> v6
    v5 -.-> a7
    a8 --> v1
    a8 --> v2
    a8 --> v3
    a8 --> v4
    a8 --> v9
    v0 -.-> a8
```
