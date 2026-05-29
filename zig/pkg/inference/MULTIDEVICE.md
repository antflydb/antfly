# Multi-Device Inference

Antfly inference supports splitting model execution across multiple compute devices (CPU+GPU, multiple GPUs, or networked machines). This builds on the graph partitioning system described in [GRAPH.md](GRAPH.md).

## Architecture

```
graph → planParallel(config) → DevicePartitionPlan → executeMultiDevice(plan, mesh)
```

1. **Device Mesh** defines the available devices
2. **Parallel Strategy** decides how to split the graph
3. **Multi-Device Executor** runs partitions sequentially across devices
4. **Collective Ops** synchronize tensors between devices (all-reduce, all-gather)
5. **Sharding Specs** describe how weights are split across devices

## Device Mesh

`src/graph/device_mesh.zig`

An ordered set of `(DeviceId, ComputeBackend)` pairs representing physical or logical devices:

```
DeviceMesh { devices: [
    (0, mlx_backend,  .mlx,  "gpu:0"),
    (1, blas_backend, .native, "cpu:0"),
] }
```

Provides `device(id)`, `deviceCount()`, and `devicesOfKind(kind)` queries. The mesh is passed to the executor at runtime — the graph IR itself stays device-agnostic.

## Parallel Strategies

`src/graph/parallel_strategy.zig`

Three strategies, selected via `ParallelConfig`:

| Strategy | How it splits | Use case |
|----------|--------------|----------|
| `single` | All nodes on device 0 | Default / single-GPU |
| `pipeline` | Splits at attention layer boundaries, assigns stages round-robin to devices | Large models that don't fit on one device |
| `tensor` | Replicates graph on all devices with sharded weights + collective sync points | Latency-sensitive inference |

`planParallel(allocator, graph, config)` returns a `DevicePartitionPlan` that extends the base `PartitionPlan` with per-partition device assignments.

## Multi-Device Executor

`src/graph/multi_executor.zig`

Executes a `DevicePartitionPlan` by walking partitions in order:

1. For each partition, resolve its assigned device from the mesh
2. Transfer external inputs (cross-partition tensors) via CPU-mediated `toFloat32`/`fromFloat32`. On Apple Silicon unified memory this is effectively a memcpy
3. Execute the partition's nodes through the target device's `ComputeBackend`
4. Thread `ExecState` across partitions (e.g., `attention_layer` counter continues from the previous partition)

Returns `MultiExecutionResult` with output tensors and which device produced each.

**Current limitation**: Partitions execute sequentially. True pipeline overlap (stage N+1 starts while N finishes) would require `std.Thread` and is not yet implemented.

## Weight Sharding

`src/graph/sharding.zig`

Describes how weight tensors are split for tensor parallelism without modifying the graph:

```zig
ShardDim = enum { column, row, replicate }
ShardRule { name_pattern: []const u8, dim: ShardDim }
ShardingSpec { rules: []const ShardRule, num_shards: u16 }
```

- **Column split**: Divides the output dimension (used for Q/K/V/gate/up projections)
- **Row split**: Divides the input dimension (used for O/down projections)
- **Replicate**: Full copy on each device (used for norms, embeddings)

`gptTensorParallelSpec(num_shards)` returns a pre-built spec for standard GPT-style transformer weight layouts. Rules match on weight name substrings (e.g., `"q_proj"`, `"v_proj"`).

## Collective Operations

`src/graph/collective_ops.zig`

Synchronization primitives for tensor parallelism:

- **All-reduce sum**: Each device contributes a tensor, receives the element-wise sum. Used after row-sharded linear layers to combine partial results.
- **All-gather**: Concatenates shards along a dimension to reconstruct the full tensor.

Both are CPU-mediated: download from each device to f32, perform the collective, upload results back. On unified memory architectures this avoids actual data movement.

## Generation Pipeline Integration

`src/pipelines/generation.zig`

Multi-device inference is opt-in. `NativeGenerationPipeline` has optional `device_mesh` and `parallel_config` fields. In `graphForward`:

- If `device_mesh` is set and `parallel_config` requests a real non-`single` strategy across multiple devices: build a `DevicePartitionPlan` via `planParallel`, then execute through `multi_executor.executeMultiDevice`
- Otherwise: stay on the normal single-device path through `interpreter.execute`

Single-device `--backend mlx` runs do not opportunistically partition the graph.

The output is transferred to f32 from whichever device produced it, then the last-position logits are sliced and returned.

## Key Files

| File | Purpose |
|------|---------|
| `src/graph/device_mesh.zig` | Device topology abstraction |
| `src/graph/parallel_strategy.zig` | Strategy planning (single/pipeline/tensor) |
| `src/graph/multi_executor.zig` | Cross-device partition execution |
| `src/graph/sharding.zig` | Weight sharding specs and slicing |
| `src/graph/collective_ops.zig` | All-reduce, all-gather primitives |
| `src/graph/partition.zig` | Base partitioning (extended with `device_id`) |
| `src/pipelines/generation.zig` | Opt-in multi-device in generation pipeline |

## Future Work

- **Native MLX multi-device**: Replace CPU-mediated transfers with MLX's native multi-device primitives when available.
- **Pipeline overlap**: Run consecutive pipeline stages concurrently via `std.Thread` for true pipeline parallelism.
- **Graph-level collective nodes**: Currently collectives are executor-boundary operations. Promoting them to graph nodes would enable the compiler passes to optimize around them.
- **Distributed training**: Compose collective ops with gradient all-reduce for data-parallel training (see [TRAINING.md](TRAINING.md)).
