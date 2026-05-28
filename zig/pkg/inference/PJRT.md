# TPU/PJRT Backend

## 2026 Status

PJRT is now a package-backed compiled backend with a working whole-model artifact
path, but it is still not a finished backend-owned decoder runtime.

Current state:

- `src/graph/pjrt_compiler.zig` lowers supported graph partitions to HLO.
- `lib/pjrt/proto/xla_proto_stub.zig` now provides real wire encoding for the HLO proto subset used by the current builder; PJRT CPU-plugin tests compile and execute those partition HLOs when the plugin is present.
- `src/graph/pjrt_executor.zig` compiles HLO through a PJRT client and exposes it through `PartitionExecutor`.
- `src/graph/pjrt_executor.zig` exposes the shared `ModelExecutor` / `ModelRuntime` whole-model surface. `compiled-target=whole-model` can now attach either offline PJRT phase packages or an inline-compiled traced graph when the shape has one PJRT-owned compute partition. The graph cache owns the immutable `ModelExecutor` plus its per-entry `ModelRuntime` so repeated calls reuse the executable and runtime wrapper. Host-assisted graph input materialization is still present for parts of the ABI, and `ModelRuntime` reports that state ownership explicitly.
- `compile-artifact --backend xla` emits serialized HLO plus PJRT input/output binding metadata by default. `--xla-artifact-kind executable` compiles that HLO through the configured PJRT plugin at export time, serializes the plugin-native executable, and writes `pjrt_executable` or `pjrt_partition_executable` manifests for load-only attach. Bounded partition executable export is allowed by default; large whole-model executable export is guarded by `TERMITE_PJRT_MAX_EXECUTABLE_EXPORT_HLO_BYTES` because dense embedded-constant HLO can exceed local plugin compile/serialize capacity. `--xla-parameter-mode inputs` is an explicit host-assisted validation mode that emits model parameters as PJRT inputs instead of embedding their f32 values in HLO, and manifests record `pjrt_parameter_mode` so embedded and parameter-input artifacts can coexist without ambiguous lookup.
- the default artifact root now mirrors the model namespace: `~/.termite/artifacts/<owner>/<model>/xla/...`
- whole-model PJRT artifact directories now refresh a package manifest (`*.termite-package.json`) that indexes the prefill entry plus compatible decode buckets, and `compile-artifact` prints that package path as part of the result
- `run-artifact <xla partition manifest>` can now execute bounded `pjrt_partition_hlo` and `pjrt_partition_executable` artifacts through the configured PJRT plugin, materialize external graph inputs from native, and compare partition outputs back to captured native values. The Gemma best-partition executable proof writes a 1.4 MiB plugin-native executable and matches native outputs within float noise.
- `src/graph/pjrt_artifact_executor.zig` owns PJRT artifact loading and wraps it as a `ModelExecutor`; `run-artifact <xla manifest>` uses that path to run the exact traced prompt shape through `ModelExecutor` / `ModelRuntime`, and `run-artifact <package>` can resolve the matching prefill entry from the package directly. `pjrt_hlo` artifacts still compile HLO through the configured plugin at load time, while `pjrt_executable` artifacts deserialize a plugin-native executable without HLO compile-on-load. `generate --backend xla --compiled-target whole-model` prefers matching plugin-native executables, then parameter-input HLO, then embedded HLO before falling back to inline graph compilation. `--validate` checks the artifact bytes exist without execution.
- whole-model PJRT attach is now package-first for both prefill and decode bucket discovery; raw sidecar scanning remains as fallback compatibility
- GPT-2 whole-model PJRT package execution is proven locally through package-backed prefill plus decode buckets; package attach can step through multiple decode buckets instead of stopping at the first decode transition
- GPT-2 whole-model prefill can now export as `pjrt_hlo`; tracing correctly falls back from absent split Q/K/V weights to fused `c_attn`, and the compiler lowers graph constants. The default dense artifact embeds constants in HLO, so the local proof is export/validate only for HLO. Parameter-input HLO export is available for smaller compile modules, but it still materializes weights from the host at execution time and is not the final backend-owned weight residency model. Executable export is wired, but large dense whole-model executable creation is refused by default once the HLO exceeds `TERMITE_PJRT_MAX_EXECUTABLE_EXPORT_HLO_BYTES`; raise that budget only when the configured plugin has enough compile/serialize capacity.
- PJRT artifact loading now reports its load mode explicitly. The local wrapper binds `PJRT_LoadedExecutable_GetExecutable`, `PJRT_Executable_Serialize`, and `PJRT_Executable_DeserializeAndLoad`; the CPU plugin round-trips a small executable in tests. Plugin-native load-only artifacts are therefore supported as a separate `pjrt_executable` kind, not by changing existing `pjrt_hlo` manifests.
- PJRT manifests now have semantic binding vocabulary for `input_ids` and past/present KV input/output bindings. `input_ids` maps to the current embedding-id execution path; semantic `present.*` output bindings can populate the retained-buffer cache, and semantic `past_*` inputs can bind retained PJRT buffers back into matching executable input slots. Native XLA artifact export now emits those binding names for whole-model phase artifacts. GPT-2 semantic decode HLO export now validates a real bucketed decode ABI with `input_ids`, 24 `past_key_values.*` inputs, and 24 `present.*` outputs.
- `PjrtModelRuntime` now owns a retained-buffer cache and clears it on runtime reset/deinit. Artifacts with semantic `present.*` output bindings can retain raw PJRT buffers instead of forcing those outputs through host tensors; a prefill artifact plus decode artifact share that cache, and packages with semantic prefill `present.*` outputs plus decode `past_*` inputs advertise backend-owned decode state.
- `generate --backend xla --mode compiled` can attach PJRT partition executors when a PJRT plugin is configured.
- `compiled-target=whole-model` is now treated strictly for PJRT: it only attaches when the traced plan is one PJRT-owned compute partition. Multiple PJRT islands are still partitioned execution, not whole-model ownership.
- The compiled-backend registry now treats PJRT whole-model runtime as an offline-artifact strategy when matching phase packages are present; inline graph attachment remains the fallback when no compatible package is available.
- Shared whole-model diagnostics now report when PJRT cannot own the traced graph shape, including unsupported compute-node counts, the first unsupported op, and attention/RoPE blocker counts. That distinguishes a real whole-model miss from ordinary partitioned PJRT execution.
- PJRT HLO lowering now covers static 2D `fused_rope` by emitting constant cos/sin masks and a per-head rotation permutation matrix for exact traced sequence shapes.
- PJRT HLO lowering now covers static batch-1 full-recompute `fused_gqa_causal_attention`, including grouped-query KV head repetition, causal masking, and softmax. Offline semantic HLO export can also lower a static single-token `skip_kv_write` attention node by adding past-KV parameters, concatenating past plus current K/V, and returning present K/V outputs for the runtime cache.

What is still missing:

- no proof yet that PJRT whole-model decode is broadly backend-owned; the current GPT-2 whole-model package proof still relies on host-assisted pieces even though prefill/decode package execution works
- incomplete decoder op coverage for full LLM ownership, especially dynamic or bucketed cache lengths, multi-batch RoPE layouts, and broader stateful/cache-writing attention cases
- no large whole-model `pjrt_executable` proof yet; the binding, small executable round-trip, and bounded Gemma partition executable proof work, but dense whole-model HLO currently embeds constants and is budget-gated because it may exceed what the local CPU plugin can compile/serialize reliably
- no executable-artifact import/compat metadata beyond the manifest kind yet; serialized executables remain plugin/version specific and should be regenerated for the target PJRT plugin
- semantic PJRT `past_*` inputs and `present.*` outputs now line up through the GPT-2 package proof, but larger and more realistic decoder packages still need execution coverage

The next practical PJRT step is making the semantic prefill/decode package
more backend-owned and more general. The parameter-input bridge can still shrink
HLO enough for host-assisted proofs, while the long-term fix is moving dense
constants to an offline/load-only weight boundary or exporting plugin-native
executables on a target plugin that can compile and serialize the shape. Until
that broader executed artifact proof exists, PJRT should still be evaluated as
an advancing whole-model package path, not as complete whole-model compiled
serving.

The long-run PJRT shape is:

1. Offline HLO and plugin-native executable artifacts are separate serialization boundaries.
2. `ModelExecutor` owns the immutable compiled PJRT executable, whether it came from HLO compile-on-load or a load-only executable artifact.
3. `ModelRuntime` owns per-session mutable state, including backend-owned KV/cache buffers.
4. Partitioned PJRT stays useful for experimentation and multi-device placement, but `whole-model` continues to mean one coherent runtime owner, not several PJRT islands.

## Context

TPUs (Google's Tensor Processing Units) offer high throughput for ML inference, especially for large models on Cloud TPU VMs. TPUs operate at the **model level** (compile entire computation graph, then run as unit), so the same `PartitionExecutor` protocol applies. The runtime is **PJRT** (Portable JIT Runtime), a C API that JAX uses under the hood. The IR is **HLO protobuf** (XLA's intermediate representation), which PJRT compiles to TPU-native code.

Key PJRT differences:
- **HLO protobuf** instead of MIL protobuf (same protobuf wire format)
- **PJRT C API** loaded via `dlopen` instead of Obj-C bridge (simpler — just C function pointers, no XLA build dependency)
- **Non-unified memory**: TPU HBM is separate from host DRAM; explicit DMA transfers needed
- **Linux-only** (Cloud TPU VMs) vs macOS-only
- **Multi-chip**: TPUs come in pods with ICI interconnects; PJRT natively supports multi-device
- **Full model claim**: TPU handles nearly all ops

### Repo Boundaries

- **`antfly-zig/lib/protobuf/`** — Shared protobuf module (already exists). Referenced from termite-zig as `../../lib/protobuf`, same pattern as `openapi`.
- **`lib/pjrt/`** — New PJRT/HLO library, local to termite-zig (like `lib/ml/` and `lib/jinja/`). Referenced as `lib/pjrt` in `build.zig.zon`.
- **`termite-zig/src/`** — All backend integration code (`src/ops/`, `src/graph/`, `src/backends/`, `src/pipelines/`, `src/architectures/`).
- **`termite-zig/build.zig`** + **`build.zig.zon`** — Build flags and dependency wiring.

---

## Increment 1: `lib/protobuf/` Shared Module — DONE

Already exists at `lib/protobuf/` with encode+decode (`src/wire.zig`), module scaffold (`build.zig`), and comprehensive tests (varint, string, nested message, packed floats/int64s, fixed32/64, skipField round-trips).

---

## Increment 2: `lib/pjrt/` Module Scaffold + HLO Builder (~550 lines)

**Create `lib/pjrt/build.zig`** + **`build.zig.zon`** — Module definition, depends on `lib/protobuf/` (via `../../lib/protobuf` relative path from `antfly-zig`).

**Create `lib/pjrt/src/root.zig`** — Public exports: `hlo`, `pjrt`.

**Create `lib/pjrt/src/hlo.zig`** (~500 lines) — HLO program builder:

```zig
pub const ElementType = enum(i32) {
    pred = 1, s8 = 2, s16 = 3, s32 = 4, s64 = 5,
    u8 = 6, u16 = 7, u32 = 8, u64 = 9,
    f16 = 10, f32 = 11, f64 = 12, bf16 = 16,
};

pub const Builder = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    instructions: ArrayList(Instruction),
    next_id: u64,

    // Parameters
    pub fn parameter(self, param_number: u32, shape: HloShape, name: []const u8) u64

    // Constants
    pub fn constantF32(self, shape: HloShape, data: []const f32) u64

    // Elementwise unary: neg, exp, log, sqrt, rsqrt, tanh, sigmoid (logistic)
    // Elementwise binary: add, multiply, subtract, divide, maximum
    // Contraction: dot (with DotDimensionNumbers)
    // Reduction: reduce (with sub-computation)
    // Shape: reshape, transpose, broadcast, slice, concatenate
    // Gather/scatter
    // Custom call (for fused kernels)

    // Composite ops (decomposed into primitives):
    pub fn gelu(self, x: u64) u64
    pub fn silu(self, x: u64) u64
    pub fn rmsNorm(self, x: u64, gamma: u64, eps: f32) u64
    pub fn layerNorm(self, x: u64, gamma: u64, beta: u64, eps: f32) u64
    pub fn linear(self, x: u64, w: u64, bias: u64) u64

    pub fn build(self) Module  // returns HloModuleProto-serializable structure
};

pub const Module = struct {
    pub fn serialize(self, allocator: std.mem.Allocator) ![]u8  // HloModuleProto bytes
};
```

HLO proto field numbers (from `xla/service/hlo.proto`):
- `HloModuleProto`: name=1, entry_computation_name=2, computations=3, program_shape=7
- `HloComputationProto`: name=1, instructions=2, root_id=4, id=5
- `HloInstructionProto`: name=1, opcode=2, shape=3, operand_ids=6, id=10
- `ShapeProto`: element_type=2, dimensions=3, layout=4

**Modify `termite-zig/build.zig.zon`** — Add dependencies:
```zig
.protobuf = .{ .path = "../../lib/protobuf" },  // antfly-zig shared lib
.pjrt = .{ .path = "lib/pjrt" },                // termite-zig local lib
```

**Test:** Build simple HLO programs (add, matmul), serialize, verify protobuf bytes.

---

## Increment 3: PJRT C API Bindings (~400 lines)

Zig bindings for the PJRT C API. Plugin loaded at runtime via `dlopen("libtpu.so")`.

**Create `lib/pjrt/src/pjrt_c_types.zig`** — Opaque handle types + argument structs matching `pjrt_c_api.h`:
- `PJRT_Client`, `PJRT_Device`, `PJRT_Buffer`, `PJRT_LoadedExecutable`, `PJRT_Error`
- `PJRT_Api` struct with ~80 function pointers

**Create `lib/pjrt/src/pjrt.zig`** — High-level Zig wrappers:

```zig
pub const Client = struct {
    pub fn init(plugin_path: []const u8) !Client        // dlopen + GetPjrtApi()
    pub fn deinit(self: *Client) void
    pub fn devices(self) ![]Device
    pub fn addressableDevices(self) ![]Device
    pub fn compile(self, hlo_bytes: []const u8) !LoadedExecutable
    pub fn bufferFromHost(self, data: []const u8, shape: HloShape, device: Device) !Buffer
};

pub const Buffer = struct {
    pub fn toHost(self, allocator) ![]u8               // TPU HBM → host DRAM
    pub fn toFloat32(self, allocator) ![]f32
    pub fn destroy(self) void
    pub fn onDeviceSizeInBytes(self) !usize
};

pub const LoadedExecutable = struct {
    pub fn execute(self, inputs: []const *Buffer, device: Device) ![]Buffer
    pub fn serialize(self) ![]u8                        // for disk caching
    pub fn destroy(self) void
};
```

No Obj-C bridge needed — PJRT is a pure C API. No link-time dependency — `libtpu.so` loaded via `dlopen` at runtime.

**Test:** Load PJRT CPU plugin (software fallback, ~5MB, available from XLA releases), create client, compile+execute simple HLO. Works in standard Linux CI without TPU hardware.

---

## Increment 4: Build Infrastructure + Backend Registration (~250 lines)

**Modify `build.zig`**:
```zig
const enable_tpu = if (enable_wasm) false else
    (b.option(bool, "tpu", "Enable TPU/PJRT backend (Linux only)") orelse
     (target.result.os.tag == .linux));
```
Import `pjrt` module when enabled. Link libc (for `dlopen`).

**Modify `src/ops/ops.zig:17`** — Add `tpu` to `BackendKind`:
```zig
pub const BackendKind = enum { native, mlx, wasm, tpu, graph };
```

**Modify `src/backends/backends.zig`** — Add `tpu` to `BackendType` with priority 35 and `available()` returning `build_options.enable_tpu`.

**Test:** Build with `-Dtpu=true`/`false`. Verify compilation on both paths.

---

## Increment 5: PartitionExecutor Protocol (~130 lines)

General-purpose, not backend-specific.

**Modify `src/graph/partition.zig`** — Add `PartitionExecutor` protocol:
```zig
pub const PartitionExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        execute: *const fn (ctx: *anyopaque, ext_vals: []const ExtVal, alloc: std.mem.Allocator) anyerror![]Output,
        deinit_fn: *const fn (ctx: *anyopaque) void,
    };
    pub const ExtVal = struct { node_id: NodeId, value: CT };
    pub const Output = struct { node_id: NodeId, value: CT };
};
```

Add `executor: ?PartitionExecutor = null` to `Partition`.

**Modify `src/graph/multi_executor.zig:170`** — Branch on `part.executor`: if non-null, collect external inputs and call `executor.execute()` instead of per-node dispatch.

**Test:** Mock `PartitionExecutor` returning canned values, verify multi_executor dispatches correctly.

---

## Increment 6: Graph IR → HLO Compiler (~450 lines)

**Create `src/graph/tpu_compiler.zig`**:

```zig
pub const TpuCompiler = struct {
    pub const CompiledPartition = struct {
        hlo_bytes: []const u8,
        input_map: []InputMapping,    // external_input node_id → HLO parameter number
        output_map: []OutputMapping,  // partition output node_id → HLO output index
        weight_blobs: []WeightBlob,   // weight data for upload to TPU HBM
    };

    pub fn compilePartition(
        self: *TpuCompiler,
        graph: *const Graph,
        part: *const Partition,
        cb: *const ComputeBackend,
    ) !CompiledPartition
};
```

Op mapping (TPU claims the full model):

| Graph IR Op | HLO Builder Call(s) |
|---|---|
| `fused_linear` | `dot(x, w_transposed)` + `add(result, bias)` |
| `fused_linear_no_bias` | `dot(x, w_transposed)` |
| `fused_rms_norm` | decomposed: `square→reduce_mean→add(eps)→rsqrt→mul→mul(gamma)` |
| `fused_layer_norm` | decomposed: `reduce_mean→sub→square→reduce_mean→add(eps)→rsqrt→mul(gamma)→add(beta)` |
| `fused_gelu` | `builder.gelu(x)` (decomposed composite) |
| `fused_relu` | `maximum(x, zero)` |
| `fused_silu` | `builder.silu(x)` (x * sigmoid(x)) |
| `fused_sigmoid` | `logistic(x)` |
| `fused_causal_self_attention` | decomposed: QK^T dot, mask, softmax, V dot |
| `fused_gqa_causal_attention` | decomposed: broadcast KV heads, QK^T, mask, softmax, V dot |
| `fused_rope` | decomposed: sin/cos rotation pairs |
| `fused_embedding_lookup` | `gather(table, indices)` |
| `fused_elem_add` / `fused_elem_multiply` | `add` / `multiply` |
| `fused_from_float32` / `fused_to_float32` | `convert` or identity |

Weight data extracted via `cb.toFloat32()`, passed as additional HLO parameters (not embedded as constants — avoids bloating the HLO).

**Test:** Compile a linear+rms_norm+gelu partition to HLO. Verify HLO protobuf is valid. Optionally execute via PJRT CPU plugin.

---

## Increment 7: TPU Executor + Compilation Cache (~400 lines)

**Create `src/graph/tpu_executor.zig`** — `PartitionExecutor` implementation:
- Holds compiled `LoadedExecutable`, input/output mappings, pre-uploaded weight buffers in HBM
- `execute()`: map ext_vals → PJRT inputs (host→TPU DMA), call `executable.execute()`, map outputs
- Output buffers stay on TPU HBM as `CT` handles; `toFloat32` DMAs to host when needed for cross-partition transfer

**Create `src/graph/tpu_cache.zig`** — Compilation cache at `~/.cache/termite/tpu/`:
- Content-addressed key: hash of (HLO bytes + weight shapes + device topology)
- First run: PJRT compile (~5-30s for large models). Subsequent: `PJRT_Executable_Deserialize` (~50ms)
- Uses `PJRT_Executable_Serialize` for disk persistence

**Modify `src/graph/cache.zig`** — Add `compiled_executors: ?[]?PartitionExecutor` to `CacheEntry`.

**Test:** On PJRT CPU plugin: compile, execute, verify cache hit on second call. Verify serialize/deserialize round-trip.

---

## Increment 8: TPU ComputeBackend + End-to-End Wiring (~450 lines)

**Create `src/ops/tpu_compute.zig`** — Minimal VTable stub:
- `backendKind() → .tpu`
- `fromFloat32()` — host DRAM → TPU HBM via `client.bufferFromHost`
- `toFloat32()` — TPU HBM → host DRAM via `buffer.toHost`
- `freeTensor()` — `PJRT_Buffer_Destroy`
- All fused ops: `error.UsePartitionExecutor` (computation goes through PartitionExecutor)

**Create `src/graph/tpu_capability.zig`** — Op support filter:
```zig
pub fn supportsTPU(op: OpCode) bool {
    return switch (op) {
        // Linear, norm, activation, elementwise
        .fused_linear, .fused_linear_no_bias, .fused_linear_no_bias_pair,
        .fused_rms_norm, .fused_layer_norm,
        .fused_gelu, .fused_relu, .fused_silu, .fused_quick_gelu, .fused_sigmoid, .fused_tanh_act,
        .fused_elem_add, .fused_elem_multiply,
        // Attention (decomposed to HLO by compiler)
        .fused_causal_self_attention, .fused_gqa_causal_attention,
        .fused_sdpa, .fused_cross_attention,
        // RoPE, embedding, conv, data movement
        .fused_rope, .fused_rope_per_item,
        .fused_embedding_lookup, .fused_concat,
        .fused_conv1d, .fused_conv2d,
        .fused_from_float32, .fused_to_float32,
        => true,
        // Paged KV cache stays on host (BLAS fallback)
        else => false,
    };
}
```

**Modify `src/pipelines/generation.zig`** — When `enable_tpu`:
```zig
const caps = [_]Capability{
    .{ .backend = .tpu, .priority = 35, .supports = &tpu_capability.supportsTPU },
    .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
};
```

**Modify `src/architectures/session_factory.zig`** — Add TPU session path: load PJRT plugin, create client, create `TpuCompute`.

**Modify `src/graph/root.zig`** — Export TPU modules.

**Test:** End-to-end: load a small model, run inference via TPU backend, verify outputs match BLAS-only within tolerance (1e-4 f32, 1e-2 bf16).

---

## Increment 9: Multi-Chip TPU Support (~250 lines)

**Create `src/graph/tpu_mesh.zig`** — TPU topology → DeviceMesh:
```zig
pub fn createTpuMesh(
    allocator: std.mem.Allocator,
    client: *pjrt.Client,
    blas_backend: *const ComputeBackend,
) !DeviceMesh
```
Creates one `DeviceEntry` per addressable TPU chip + BLAS fallback. For v4-8 (4 chips): `[tpu:0, tpu:1, tpu:2, tpu:3, native:cpu]`.

**Modify `src/graph/tpu_executor.zig`** — Multi-device execution:
- One `DeviceEntry` per chip in `DeviceMesh`
- Use `parallel_strategy.zig` for weight sharding across chips
- PJRT handles inter-chip ICI communication natively

**Modify `src/graph/collective_ops.zig`** — Add PJRT-native all-reduce:
```zig
pub fn pjrtAllReduceSum(client: *pjrt.Client, buffers: []pjrt.Buffer, devices: []pjrt.Device) !void
```
Uses PJRT's cross-replica-sum via ICI hardware (~300 GB/s per chip), bypassing CPU-mediated transfer.

**Test:** On multi-chip TPU (v4-8 or v5e-4), verify tensor-parallel inference matches single-chip results. Confirm latency speedup.

---

## Dependency Chain

```
Inc 1 (lib/protobuf)  →  Inc 2 (lib/pjrt: HLO builder)  →  Inc 6 (Graph→HLO compiler)
                                                                      ↓
Inc 3 (lib/pjrt: PJRT C API)  →  Inc 4 (build infra)  →  Inc 7 (TPU executor + cache)
                                                                      ↓
Inc 5 (PartitionExecutor)  ────────────────────────────→  Inc 8 (end-to-end wiring)
                                                                      ↓
                                                                Inc 9 (multi-chip)
```

Increments 1-2 and 3 can proceed in parallel. Increments 4-5 can proceed in parallel with 1-3.

## Critical Files

| File | Repo | Change |
|------|------|--------|
| `lib/protobuf/` | antfly-zig | DONE — shared protobuf encoder |
| `lib/pjrt/{build.zig,build.zig.zon,src/root.zig}` | termite-zig | NEW — module scaffold |
| `lib/pjrt/src/hlo.zig` | termite-zig | NEW — HLO program builder |
| `lib/pjrt/src/pjrt.zig` | termite-zig | NEW — PJRT client/buffer/executable wrappers |
| `lib/pjrt/src/pjrt_c_types.zig` | termite-zig | NEW — PJRT C API type definitions |
| `build.zig` | termite-zig | `-Dtpu` flag, pjrt module import |
| `build.zig.zon` | termite-zig | Add `protobuf` + `pjrt` path dependencies |
| `src/ops/ops.zig` | termite-zig | Add `tpu` to `BackendKind` |
| `src/backends/backends.zig` | termite-zig | Add `tpu` to `BackendType` |
| `src/graph/partition.zig` | termite-zig | Add `PartitionExecutor` protocol |
| `src/graph/multi_executor.zig` | termite-zig | Branch on `part.executor` |
| `src/graph/tpu_compiler.zig` | termite-zig | NEW — Graph IR → HLO translation |
| `src/graph/tpu_executor.zig` | termite-zig | NEW — PartitionExecutor for TPU |
| `src/graph/tpu_cache.zig` | termite-zig | NEW — compiled executable caching |
| `src/graph/tpu_capability.zig` | termite-zig | NEW — op support filter |
| `src/graph/tpu_mesh.zig` | termite-zig | NEW — TPU topology → DeviceMesh |
| `src/ops/tpu_compute.zig` | termite-zig | NEW — minimal VTable (DMA + lifecycle) |
| `src/graph/cache.zig` | termite-zig | Add `compiled_executors` to `CacheEntry` |
| `src/graph/root.zig` | termite-zig | Export TPU modules |
| `src/architectures/session_factory.zig` | termite-zig | TPU session creation |
| `src/pipelines/generation.zig` | termite-zig | TPU partition capabilities + executor wiring |
| `src/graph/collective_ops.zig` | termite-zig | PJRT-native all-reduce via ICI |

## Existing Code to Reuse

- `partition.supportsLinearNormActivation()` / `supportsAttention()` — reference for `supportsTPU()`
- `multi_executor.executeMultiDevice()` + `transferTensor()` — works as-is for non-unified memory
- `device_mesh.DeviceMesh` — used directly for multi-chip topologies
- `parallel_strategy.planParallel()` + `sharding.gptTensorParallelSpec()` — tensor parallelism for multi-chip
- `collective_ops.allReduceSum()` — CPU fallback (replaced by ICI-native for TPU)
- `cache.CacheKey` / `GraphCache` — pattern for TPU compilation cache
- `ShapeConstraint.fixed` / `.bounded` — TPU uses fixed shapes (compiled per shape)
- `lib/ml/build.zig` — module scaffold pattern
- ONNX `@cImport` pattern in `src/backends/onnx.zig` — reference for C API integration

## Testing Strategy

- **Inc 1-2** (protobuf + HLO builder): Pure Zig, standard CI (macOS + Linux)
- **Inc 3** (PJRT bindings): Test with **PJRT CPU plugin** (`pjrt_c_api_cpu_plugin.so`, ~5MB, from XLA releases) — full PJRT API on software CPU backend, works in standard Linux CI
- **Inc 4-6**: No hardware dependency, standard CI
- **Inc 7-8**: Full pipeline via PJRT CPU plugin catches integration bugs. TPU-specific behavior tested on GCE TPU VM (preemptible `ct5lp-hightpu-1t`, ~$1.20/hr)
- **Inc 9**: Multi-chip requires v4-8+ hardware

All TPU code behind `if (build_options.enable_tpu)`. Non-Linux CI skips entirely.

## Scope

~2,880 lines across 9 increments:
- `lib/protobuf/`: ~200 lines (shared protobuf encoder)
- `lib/pjrt/`: ~950 lines (HLO builder, PJRT bindings, C types)
- `src/` Zig: ~1,600 lines (compiler, executor, cache, capability, compute backend, mesh, wiring)
- No Obj-C bridge (PJRT is pure C via dlopen)
- `PartitionExecutor`: ~130 lines

## Verification

1. `zig build -Dtpu=true` compiles on Linux
2. `zig build -Dtpu=false` compiles everywhere (no TPU code included)
3. PJRT CPU plugin: end-to-end inference matches BLAS-only within tolerance
4. TPU hardware: real model inference produces correct outputs
5. Multi-chip: tensor-parallel inference matches single-chip results
