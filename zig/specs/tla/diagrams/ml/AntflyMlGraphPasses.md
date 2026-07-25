<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyMlGraphPasses — structural diagrams

Generated from [`AntflyMlGraphPasses.tla`](../../ml/AntflyMlGraphPasses.tla). 18 state variables, 9 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `RunConstFold` | `constFoldDone` | `constFoldDone` |
| `RunCSE` | `live`, `deps`, `constFoldDone`, `cseDone` | `live`, `deps`, `cseDone` |
| `RunFuse` | `live`, `vjpAlt`, `constFoldDone`, `cseDone`, `fuseDone` | `live`, `deps`, `vjpAlt`, `fuseDone` |
| `RunDCEKeepLoweringClosure` | `live`, `fuseDone`, `dceDone` | `live`, `deps`, `dceDone` |
| `RunDCEPruneVjpOnlyClosure` | `live`, `vjpAlt`, `fuseDone`, `dceDone` | `live`, `deps`, `vjpAlt`, `dceDone` |
| `FailPass` | `passFailed`, `exported` | `passFailed`, `partialVisible` |
| `ExportPartition` | `live`, `deps`, `vjpAlt`, `dceDone`, `partialVisible`, `exported` | `exported`, `exportLive`, `exportDeps`, `exportVjpAlt`, `runtimeInputs` |
| `ClearFallbackPartition` | `fallbackPartition` | `fallbackPartition` |
| `AttachRuntime` | `exported`, `fallbackPartition`, `requireNoFallback`, `runtimePublished` | `runtimePublished` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[RunConstFold]
        a1[RunCSE]
        a2[RunFuse]
        a3[RunDCEKeepLoweringClosure]
        a4[RunDCEPruneVjpOnlyClosure]
        a5[FailPass]
        a6[ExportPartition]
        a7[ClearFallbackPartition]
        a8[AttachRuntime]
    end
    subgraph state["State variables"]
        v0([constFoldDone])
        v1([live])
        v2([deps])
        v3([cseDone])
        v4([vjpAlt])
        v5([fuseDone])
        v6([dceDone])
        v7([passFailed])
        v8([partialVisible])
        v9([exported])
        v10([exportLive])
        v11([exportDeps])
        v12([exportVjpAlt])
        v13([runtimeInputs])
        v14([fallbackPartition])
        v15([requireNoFallback])
        v16([runtimePublished])
    end
    a0 --> v0
    a1 --> v1
    a1 --> v2
    a1 --> v3
    v0 -.-> a1
    a2 --> v1
    a2 --> v2
    a2 --> v4
    a2 --> v5
    v0 -.-> a2
    v3 -.-> a2
    a3 --> v1
    a3 --> v2
    a3 --> v6
    v5 -.-> a3
    a4 --> v1
    a4 --> v2
    a4 --> v4
    a4 --> v6
    v5 -.-> a4
    a5 --> v7
    a5 --> v8
    v9 -.-> a5
    a6 --> v9
    a6 --> v10
    a6 --> v11
    a6 --> v12
    a6 --> v13
    v1 -.-> a6
    v2 -.-> a6
    v4 -.-> a6
    v6 -.-> a6
    v8 -.-> a6
    a7 --> v14
    a8 --> v16
    v9 -.-> a8
    v14 -.-> a8
    v15 -.-> a8
```
