<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyLitePublication — structural diagrams

Generated from [`AntflyLitePublication.tla`](../../../storage/lite/AntflyLitePublication.tla). 11 state variables, 11 actions in `Next`. 4 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `buildState`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> idle
    idle --> building : StartPublication
    building --> manifest_written : WriteManifest
    manifest_written --> idle : AdvanceHead, CrashAfterManifestBeforeHead
    building --> failed : FailPublication
    failed --> idle : DiscardFailedPublication
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `StartPublication` | `buildState`, `headVersion` | `buildState`, `buildVersion`, `failedVersion` |
| `PublishArtifact` | `buildState`, `buildVersion`, `artifactVersion` | `artifactVersion` |
| `WriteManifest` | `buildState`, `buildVersion`, `artifactVersion` | `buildState`, `manifestStoredVersion`, `manifestRefs` |
| `AdvanceHead` | `buildState`, `buildVersion`, `artifactVersion`, `manifestStoredVersion`, `manifestRefs` | `buildState`, `buildVersion`, `headVersion`, `visibleRefs`, `failedVersion` |
| `CrashAfterManifestBeforeHead` | `buildState` | `buildState`, `buildVersion` |
| `RetryManifestHeadAdvance` | `buildState`, `artifactVersion`, `manifestStoredVersion`, `manifestRefs`, `headVersion` | `headVersion`, `visibleRefs`, `failedVersion` |
| `FailPublication` | `buildState`, `buildVersion` | `buildState`, `failedVersion` |
| `DiscardFailedPublication` | `buildState` | `buildState`, `buildVersion` |
| `OpenReader` | `headVersion`, `visibleRefs`, `readerPinnedVersion` | `readerPinnedVersion`, `readerRefs` |
| `CloseReader` | `readerPinnedVersion` | `readerPinnedVersion`, `readerRefs` |
| `CleanupObsolete` | `headVersion`, `readerPinnedVersion`, `deletedGeneration` | `deletedGeneration` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[StartPublication]
        a1[PublishArtifact]
        a2[WriteManifest]
        a3[AdvanceHead]
        a4[CrashAfterManifestBeforeHead]
        a5[RetryManifestHeadAdvance]
        a6[FailPublication]
        a7[DiscardFailedPublication]
        a8[OpenReader]
        a9[CloseReader]
        a10[CleanupObsolete]
    end
    subgraph state["State variables"]
        v0([buildState])
        v1([buildVersion])
        v2([headVersion])
        v3([failedVersion])
        v4([artifactVersion])
        v5([manifestStoredVersion])
        v6([manifestRefs])
        v7([visibleRefs])
        v8([readerPinnedVersion])
        v9([readerRefs])
        v10([deletedGeneration])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v3
    v2 -.-> a0
    a1 --> v4
    v0 -.-> a1
    v1 -.-> a1
    a2 --> v0
    a2 --> v5
    a2 --> v6
    v1 -.-> a2
    a3 --> v0
    a3 --> v1
    a3 --> v2
    a3 --> v7
    a3 --> v3
    v5 -.-> a3
    v6 -.-> a3
    a4 --> v0
    a4 --> v1
    a5 --> v2
    a5 --> v7
    a5 --> v3
    v0 -.-> a5
    v5 -.-> a5
    v6 -.-> a5
    a6 --> v0
    a6 --> v3
    v1 -.-> a6
    a7 --> v0
    a7 --> v1
    a8 --> v8
    a8 --> v9
    v2 -.-> a8
    v7 -.-> a8
    a9 --> v8
    a9 --> v9
    a10 --> v10
    v2 -.-> a10
    v8 -.-> a10
```
