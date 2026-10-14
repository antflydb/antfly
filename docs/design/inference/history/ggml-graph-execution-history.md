# GGML Graph Execution Implementation History

> Summary of a verbatim log. Full text: [docs/design/inference/history/ggml-graph-execution-history.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/ggml-graph-execution-history.md). Originally relocated verbatim from `zig/pkg/inference/GGML.md` (lines 635–954 at commit 271838a195) on 2026-09-16 during the documentation cleanup.

Bullet-by-bullet record of the slices that landed graph partitioning and backend partition executors (Metal, WebGPU) plus shared quant-matmul routing. The current-state design is in [`GGML.md`](../../../../zig/pkg/inference/GGML.md).

## Decisions

- **Metal partitions run through a dedicated `PartitionExecutor`** (`src/graph/metal_partition_executor.zig`) instead of the generic per-node fallback in `MultiExecutor`; it validates the buffer plan, reserves persistent `MTLBuffer` graph-plan slots, submits one decoder-runtime frame, and evaluates only partition boundary outputs (no per-node sync).
- **Residency policy:** Metal boundary outputs stay device-resident until the final caller readback; cross-device transfers into Metal upload explicitly into private buffers rather than depending on an eager-upload env flag.
- **Scalar operands stay one-element device tensors** with scalar-aware dispatch flags for `sub`, `div`, `less_than`, `where_select`, instead of full-device scalar expansion.
- **Packed quant weights never fall into dense fallback by accident:** Metal capability consults the shared quant matmul planner; unsupported formats/storage reject with `missing_quant_kernel`. Packed quant metadata survives metadata views (reshape/slice) for the same reason. Dense activation × packed weight is the accepted matmul shape.
- **Persisted `OperatorPlan` per node** in `PartitionPlan`; executors pass it to optional planned linear hooks (`linearPlanned`/`linearNoBiasPlanned`) without changing model code. Q8_0 row buckets: decode `mul_mv`, small prompt `mul_mv_ext`, larger prompt `mul_mm`; Q4_0, Q4_1, Q5_K later promoted beyond decode using each format's direct packed kernel.
- **Q8_0 `mul_mm` uses a conservative 16-output by 8-row reduction tile** — validator coverage with varied activation columns catches the old simdgroup-matrix under-accumulation pattern. Fused gate/up activation MM uses the same tile and is on by default.
- **Graph-mode generation routes traced replay through `MultiExecutor`** and skips the direct whole-model executor when graph mode is requested, so graph-mode smokes no longer silently exit before the graph executor runs.
- **WebGPU executor** (`src/graph/webgpu_partition_executor.zig`) is a real executor with its own conservative capability decision (claims host inputs only when large enough to amortize transfer). GPU buffers are the source of truth with host downloads deferred to `toFloat32`/export. Rule: future WebGPU op claims land with matching WGSL/import and executor classification coverage in the same slice.

## Defects found

- Zig's layer-norm runtime slot capacity was misaligned with the Objective-C runtime constant, so dynamic slot allocation probed invalid slots.
- Shared quant support table had a duplicate Q4_1 switch case.
- Graph-plan reservation double-counted transfer-in buffers as scratch and aborted on partitions with many live values; now keeps the largest slots within the Metal runtime slot cap.
- Cross-device transfers required backend tensor-shape metadata; they now use the graph's static output shape, keeping scalar/native constants transferable.
- Prepared Q4/Q5_K and Q6_K parallel worker contexts did not carry `prepared_panel8_packed_bytes`.

## Observability and bisection switches

- `TERMITE_GRAPH_EXECUTOR_STATS=1` prints command dispatches, interpreter fallbacks, transfers, resident vs materialized outputs, graph-plan slot/byte reservations, and `planned_commands`; direct paths (`termite.generate`, `termite.embed`) print an explicit bypass line.
- Real Metal model smokes (Gemma generation, CLIPCLAP text embedding) passed under the Metal debug wrapper with no diagnostic reports; they used direct executors, not `MultiExecutor`. The wrapper's `--api-validate` mode keeps `MTL_DEBUG_LAYER=1` without shader validation where shader validation prevents device creation.
- `TERMITE_METAL_DISABLE_Q8_MM=1` and `TERMITE_METAL_DISABLE_Q8_PAIR_ACTIVATION_MM=1` force the verified small-batch/MMV and split gate/up paths for bisection.

## Key measurements

Fused prepared-panel8 Q4_K/Q5_K pair/triple projections on CLIPCLAP-shaped rows 64, vs separate-call baseline: Q4_K pair about 1.50x, Q4_K triple about 1.63x, Q5_K pair about 1.45x, Q5_K triple about 1.55x (`--only-native-quant-buckets`).
