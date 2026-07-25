<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyMlCompilerPublication — structural diagrams

Generated from [`AntflyMlCompilerPublication.tla`](../../ml/AntflyMlCompilerPublication.tla). 16 state variables, 6 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `UpdateGraph` | `graphVersion`, `runtimePublished` | `graphVersion` |
| `ExportPartition` | `graphVersion`, `exportVersion`, `runtimePublished` | `exportVersion`, `exportComplete`, `exportedInputs`, `exportedOutputs`, `compileVersion`, `compileComplete`, `compiledInputs`, `compiledOutputs`, `compileFailed`, `partialArtifactVisible` |
| `CompileArtifact` | `graphVersion`, `exportVersion`, `exportComplete`, `exportedInputs`, `exportedOutputs`, `compileComplete` | `compileVersion`, `compileComplete`, `compiledInputs`, `compiledOutputs` |
| `FailCompile` | `compileComplete`, `compileFailed` | `compileFailed`, `partialArtifactVisible` |
| `ClearFallbackPartition` | `fallbackPartition` | `fallbackPartition` |
| `PublishRuntime` | `graphVersion`, `compileVersion`, `compileComplete`, `partialArtifactVisible`, `fallbackPartition`, `requireNoFallback`, `runtimePublished` | `runtimePublished`, `runtimeVersion`, `runtimeVisibleOutputs` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[UpdateGraph]
        a1[ExportPartition]
        a2[CompileArtifact]
        a3[FailCompile]
        a4[ClearFallbackPartition]
        a5[PublishRuntime]
    end
    subgraph state["State variables"]
        v0([graphVersion])
        v1([runtimePublished])
        v2([exportVersion])
        v3([exportComplete])
        v4([exportedInputs])
        v5([exportedOutputs])
        v6([compileVersion])
        v7([compileComplete])
        v8([compiledInputs])
        v9([compiledOutputs])
        v10([compileFailed])
        v11([partialArtifactVisible])
        v12([fallbackPartition])
        v13([requireNoFallback])
        v14([runtimeVersion])
        v15([runtimeVisibleOutputs])
    end
    a0 --> v0
    v1 -.-> a0
    a1 --> v2
    a1 --> v3
    a1 --> v4
    a1 --> v5
    a1 --> v6
    a1 --> v7
    a1 --> v8
    a1 --> v9
    a1 --> v10
    a1 --> v11
    v0 -.-> a1
    v1 -.-> a1
    a2 --> v6
    a2 --> v7
    a2 --> v8
    a2 --> v9
    v0 -.-> a2
    v2 -.-> a2
    v3 -.-> a2
    v4 -.-> a2
    v5 -.-> a2
    a3 --> v10
    a3 --> v11
    v7 -.-> a3
    a4 --> v12
    a5 --> v1
    a5 --> v14
    a5 --> v15
    v0 -.-> a5
    v6 -.-> a5
    v7 -.-> a5
    v11 -.-> a5
    v12 -.-> a5
    v13 -.-> a5
```
