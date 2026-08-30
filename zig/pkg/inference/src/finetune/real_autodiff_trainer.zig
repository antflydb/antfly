// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Architecture-agnostic "Phase R3" training harness.
//
// Given a user-supplied forward-graph builder callback, this harness:
//   1. Lazily constructs a forward graph via the callback.
//   2. Injects LoRA adapters for matching linear layers (lora_mod.injectLoRA).
//   3. Runs ml.graph.autodiff.gradient to build a backward graph covering
//      every LoRA parameter.
//   4. On each step, runs the combined forward+backward through a real
//      ComputeBackend via the existing graph/training.zig `trainStep`
//      helper, which in turn calls the eager graph interpreter.
//   5. Reads the loss and per-parameter gradients out of the execution
//      result and drives AdamW updates over the LoRA parameter slices
//      owned by this trainer.
//
// The harness is architecture agnostic: BERT/Qwen2/LayoutLMv3 all plug in
// via the BuildForwardFn / BuildLossFn callback pair. Input placeholders
// are created by the harness (on first step) and fed into the callback so
// the user code only constructs the forward DAG.
//
// Optional Hypura `TrainingMemoryCoordinator` integration registers each
// LoRA gradient block with the residency tracker, pins all blocks for the
// duration of the step, and unpins after the optimizer step. A rough
// activation budget is reserved at the start of every step.

const std = @import("std");
const builtin = @import("builtin");
const ml = @import("ml");
const platform = @import("antfly_platform");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const Shape = ml.graph.Shape;
const DType = ml.graph.shape.DType;
const lora_mod = ml.graph.lora;
const autodiff = ml.graph.autodiff;
const optimizers = ml.graph.optimizers;

const ops_mod = @import("../ops/ops.zig");
const CT = ops_mod.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const training = @import("../graph/training.zig");

const coord_mod = @import("training_memory_coordinator.zig");
const TrainingMemoryCoordinator = coord_mod.TrainingMemoryCoordinator;
const grad_residency = @import("grad_residency.zig");
const compat = @import("../io/compat.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const graph_weight_bridge = @import("graph_weight_bridge.zig");
const safetensors_checkpoint = @import("safetensors_checkpoint.zig");
const safetensors = @import("../models/safetensors.zig");
const system_memory = @import("../runtime/tier/memory.zig");

// ── Public callbacks ─────────────────────────────────────────────────────────

/// Architecture-agnostic forward graph builder callback.
///
/// Given a fresh Builder and the per-step input shapes, construct the forward
/// pass through the architecture. Return the NodeId of the final output
/// (typically a [batch, seq, hidden] hidden state or [batch, vocab] logits
/// tensor, depending on the task). Input placeholders are created by the
/// caller and passed in — the callback just consumes them and wires the
/// graph.
pub const BuildForwardFn = *const fn (
    ctx: *anyopaque,
    bld: *Builder,
    input_ids: NodeId,
    attention_mask: NodeId,
    batch: u32,
    seq_len: u32,
) anyerror!NodeId;

/// Task-specific loss builder. Called after `buildForward` with the forward's
/// output and the target tensor. Returns a scalar loss NodeId. Typical:
///   - classification: `b.crossEntropyLoss(logits, targets)`
///   - regression / score-matching: `b.mseLoss(output, targets)`
pub const BuildLossFn = *const fn (
    ctx: *anyopaque,
    bld: *Builder,
    forward_output: NodeId,
    targets: NodeId,
) anyerror!NodeId;

// ── Distributed gradient reduction hook ─────────────────────────────────────

/// A single LoRA parameter's gradient block, handed to the distributed
/// reduction hook. `data` is the in-place mutable accumulator that the hook
/// is expected to all-reduce across ranks.
pub const GradBlock = struct {
    name: []const u8,
    data: []f32,
};

/// Device-resident gradient block handed to `ReduceDeviceGradsFn`. `data`
/// is the mutable backend tensor for the accumulated gradient. Hooks should
/// mutate it in place or replace its storage through backend-specific APIs.
pub const DeviceGradBlock = struct {
    name: []const u8,
    data: CT,
    elem_count: usize,
    dims: []const i32,
};

/// Optional hook called once per accumulation flush, after `grad_accum` is
/// finalized but before the optimizer step. The harness passes a slice of
/// `(name, data)` pairs — one per LoRA parameter block — and the hook is
/// expected to mutate each `data` slice in place with the all-reduced
/// gradient. When null (default), no reduction is performed (single-rank
/// training). The harness does not know about MLX / NCCL / any specific
/// transport — the caller is responsible for plumbing the real primitive.
pub const ReduceGradsFn = *const fn (
    ctx: *anyopaque,
    grads: []const GradBlock,
) anyerror!void;

/// Device-resident equivalent of `ReduceGradsFn`. This is preferred by the
/// compiled Metal path because it avoids materializing full accumulated
/// gradients on the host for distributed/custom reductions.
pub const ReduceDeviceGradsFn = *const fn (
    ctx: *anyopaque,
    cb: *const ComputeBackend,
    grads: []const DeviceGradBlock,
) anyerror!void;

// ── Config ───────────────────────────────────────────────────────────────────

pub const TrainerConfig = struct {
    /// LoRA injection config (target patterns + rank + alpha).
    lora: lora_mod.LoRAConfig,
    /// AdamW optimizer config for all LoRA parameters.
    optimizer: optimizers.AdamWConfig = .{},
    /// LR schedule.
    lr_schedule: optimizers.LearningRateSchedule = .{ .constant = 1e-4 },
    /// Gradient clipping global norm. 0 = disabled.
    max_grad_norm: f32 = 1.0,
    /// Number of micro-batches to accumulate before stepping the optimizer.
    grad_accum_steps: u32 = 1,
    /// Optional hint used when estimating the activation budget that the
    /// Hypura coordinator should reserve at the start of each step. If 0 the
    /// trainer falls back to a reasonable heuristic.
    hidden_size_hint: u32 = 0,
    num_layers_hint: u32 = 0,

    /// Optional Gaussian std-dev override for LoRA A matrices.
    /// B is always zero-initialized so the LoRA path is a no-op at step 0.
    /// When `null`, A matrices use the official GLiNER2/PyTorch default:
    /// `kaiming_uniform_(A, a=sqrt(5))`, which reduces to
    /// uniform[-1/sqrt(fan_in), 1/sqrt(fan_in)].
    lora_a_init_std: ?f32 = null,
    /// RNG seed used to initialize A matrices.
    seed: u64 = 42,
    /// Regular graph parameters that should be owned and updated by the
    /// trainer in addition to LoRA adapter parameters. Names must match
    /// parameter nodes in the post-LoRA graph. Initial values are copied
    /// from the backend weight store during graph construction and then
    /// rebound from trainer-owned buffers on each step.
    regular_trainable_params: []const []const u8 = &.{},

    /// Optional distributed gradient-reduction hook. Called exactly once per
    /// accumulation flush, immediately before the optimizer step, with a
    /// GradBlock per LoRA parameter. A typical DDP caller would wire this to
    /// a backend-specific all-reduce hook to fold LoRA gradients across ranks
    /// before AdamW updates.
    reduce_grads: ?ReduceGradsFn = null,
    /// Opaque context pointer forwarded to `reduce_grads` on each call.
    reduce_grads_ctx: ?*anyopaque = null,
    /// Optional device-resident gradient-reduction hook. Used by compiled
    /// Metal training when provided; unlike `reduce_grads`, this does not
    /// require full gradient accumulator downloads.
    reduce_device_grads: ?ReduceDeviceGradsFn = null,
    /// Opaque context pointer forwarded to `reduce_device_grads`.
    reduce_device_grads_ctx: ?*anyopaque = null,
    /// Execution engine for the gradient graph. The interpreter preserves
    /// historical behavior. `compiled_device` caches the autodiff graph and
    /// keeps trainable optimizer state on any backend exposing device-training
    /// operations. `compiled_metal` remains a compatibility spelling.
    execution_engine: TrainingExecutionEngine = .interpreter,
    /// When true, fail instead of silently falling back to interpreter if the
    /// requested compiled engine cannot be prepared.
    compiled_required: bool = false,
    /// Activation (gradient) checkpointing. When set, the backward graph
    /// recomputes non-checkpoint forward activations from the nearest
    /// checkpoint boundary instead of keeping them live, bounding peak
    /// activation memory (trades ~1.3-2x forward compute for memory). null =
    /// disabled. Applies to both the interpreter and compiled (Metal) paths.
    checkpoint_config: ?ml.graph.checkpoint.CheckpointConfig = null,
    /// Maximum number of shape-specialized forward/backward graphs retained,
    /// including the active graph. A value of 1 preserves the historical
    /// single-graph memory bound while still rebuilding safely when an input
    /// shape changes. Optimizer/trainable state is shared across all entries.
    graph_cache_capacity: u8 = 1,
    /// Minimum currently-available system memory required before retaining an
    /// inactive graph while another shape is built. Zero disables the guard.
    /// Callers should pass their conservative active-step peak estimate.
    graph_cache_build_reserve_bytes: u64 = 0,
};

pub const TrainingExecutionEngine = enum {
    interpreter,
    compiled_device,
    compiled_metal,
};

// ── Step I/O ─────────────────────────────────────────────────────────────────

/// Optional hook that binds architecture-specific input placeholders
/// (position_ids, attn_bias, RoPE cos/sin, bbox components, etc.) into the
/// runtime-input map before graph execution. Without this, only the 3
/// standard placeholders (__input_ids, __attention_mask, __targets) + LoRA
/// params are bound — any extra placeholders the architecture callback
/// created would go unbound and execution would fail.
///
/// The hook receives:
///   - `ctx`:      same opaque pointer as TrainerInput.ctx
///   - `cb`:       the compute backend (for `fromFloat32Shape`)
///   - `allocator`: for scratch allocations
///   - `graph`:    the post-injection graph (to discover placeholder names)
///   - `rt_map`:   the runtime-input map to INSERT additional bindings into
///   - `batch`, `seq_len`: current step's dimensions
///
/// Architecture-specific trainers implement this using helpers from
/// `graph_input_binder.zig` (e.g. `BertPlaceholderPrep.buildPositionIds`).
pub const BindArchInputsFn = *const fn (
    ctx: *anyopaque,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    graph: *const Graph,
    rt_map: *std.AutoHashMapUnmanaged(NodeId, CT),
    batch: u32,
    seq_len: u32,
    attention_mask: []const f32,
) anyerror!void;

pub const RemapGraphNodesFn = *const fn (
    ctx: *anyopaque,
    id_map: []const NodeId,
) anyerror!void;

/// Optional shape-cache hooks for callbacks that retain graph-local NodeIds
/// in their opaque context. The captured pointer is owned by the trainer and
/// released with `deinit_graph_context` when its graph entry is evicted.
pub const CaptureGraphContextFn = *const fn (
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
) anyerror!?*anyopaque;
pub const RestoreGraphContextFn = *const fn (
    ctx: *anyopaque,
    state: ?*anyopaque,
) anyerror!void;
pub const DeinitGraphContextFn = *const fn (
    state: ?*anyopaque,
    allocator: std.mem.Allocator,
) void;

pub const TrainerInput = struct {
    /// User-opaque context passed to buildForward / buildLoss.
    ctx: *anyopaque,
    build_forward: BuildForwardFn,
    build_loss: BuildLossFn,
    /// Actual [batch, seq] input IDs for this step.
    input_ids: []const i64,
    /// Actual [batch, seq] attention mask for this step.
    attention_mask: []const f32,
    /// Actual target tensor for the loss (shape/contents task-specific).
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    /// Optional architecture-specific input binder. When non-null, called
    /// during `step()` after binding the 3 standard inputs + LoRA params and
    /// before graph execution. This is how BERT position_ids, Qwen2 RoPE
    /// tables, LayoutLMv3 bbox components, etc. get wired into the graph.
    bind_arch_inputs: ?BindArchInputsFn = null,
    /// Optional callback invoked if the trainer reorders the graph after
    /// construction, so architecture-specific cached NodeIds can be remapped.
    remap_graph_nodes: ?RemapGraphNodesFn = null,
    capture_graph_context: ?CaptureGraphContextFn = null,
    restore_graph_context: ?RestoreGraphContextFn = null,
    deinit_graph_context: ?DeinitGraphContextFn = null,
    /// Caller-owned build-time discriminator for graph-affecting context that
    /// is not represented by batch/sequence/target shapes. Keep zero when the
    /// callbacks depend only on those shapes. GLiNER2 uses this for its
    /// contextual-slot, structure-instance, and task-count buckets.
    graph_variant: [4]u64 = @splat(0),
};

pub const StepResult = struct {
    loss: f32,
    grad_norm: f32,
    step: u64,
    /// True if this step actually dispatched an optimizer update (i.e. the
    /// gradient-accumulation window closed). False when the step only
    /// accumulated gradients for a later flush.
    optimizer_stepped: bool,
    learning_rate: f32 = 0.0,
    profile: StepProfile = .{},
};

pub const FlushResult = struct {
    grad_norm: f32,
    learning_rate: f32,
    optimizer_step: u64,
    micro_batches: u32,
};

pub const StepProfile = struct {
    graph_build_ns: u64 = 0,
    runtime_input_ns: u64 = 0,
    train_step_ns: u64 = 0,
    compile_ns: u64 = 0,
    autodiff_ns: u64 = 0,
    execute_ns: u64 = 0,
    extract_ns: u64 = 0,
    optimizer_update_ns: u64 = 0,
    device_optimizer_ns: u64 = 0,
    total_ns: u64 = 0,
    peak_resident_bytes: usize = 0,
    optimizer_backend: OptimizerBackend = .host,
    device_resident_transfer_count: u64 = 0,
    device_trainable_bytes: usize = 0,
    /// Static string set when the Metal training graph executor was requested
    /// but the step fell back to the regular compiled path.
    graph_executor_fallback_reason: ?[]const u8 = null,
    graph_executor_partitions: u64 = 0,
    graph_executor_command_dispatches: u64 = 0,
    graph_executor_planned_dispatches: u64 = 0,
    graph_executor_interpreter_fallbacks: u64 = 0,
    graph_executor_host_outputs: u64 = 0,
    graph_executor_true_host_outputs: u64 = 0,
    graph_executor_host_output_command: u64 = 0,
    graph_executor_host_output_interpreter: u64 = 0,
    graph_executor_host_output_pre_materialized_constant: u64 = 0,
    graph_executor_host_output_runtime_region: u64 = 0,
    graph_executor_host_output_parameter: u64 = 0,
    graph_executor_host_output_unattributed: u64 = 0,
    graph_executor_device_outputs: u64 = 0,
    graph_executor_device_output_parameter: u64 = 0,
    graph_executor_metal_frame_chunk_boundaries: u64 = 0,
    graph_executor_metal_frame_chunk_promoted_values: u64 = 0,
    graph_executor_metal_frame_chunk_swept_values: u64 = 0,
    graph_executor_metal_chunk_local_output_peak_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_live_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_allocations: u64 = 0,
    graph_executor_metal_chunk_local_output_reuse_hits: u64 = 0,
    graph_executor_metal_chunk_local_output_consumed_hints: u64 = 0,
    graph_executor_metal_chunk_local_output_unconsumed_hints: u64 = 0,
    graph_executor_metal_chunk_local_output_spill_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_alias_conflicts: u64 = 0,
    graph_executor_metal_chunk_local_output_resets: u64 = 0,
    graph_executor_metal_chunk_local_output_reset_freed_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_discard_freed_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_reset_live_carry_values: u64 = 0,
    graph_executor_metal_gather_input_promotions: u64 = 0,
    graph_executor_metal_gather_input_promotion_bytes: u64 = 0,
    graph_executor_metal_gather_input_promotion_ns: u64 = 0,
    graph_executor_metal_gather_output_promotions: u64 = 0,
    graph_executor_metal_gather_output_promotion_bytes: u64 = 0,
    graph_executor_metal_gather_output_promotion_ns: u64 = 0,
    graph_executor_metal_reduce_input_promotions: u64 = 0,
    graph_executor_metal_reduce_input_promotion_bytes: u64 = 0,
    graph_executor_metal_reduce_input_promotion_ns: u64 = 0,
    graph_executor_metal_resident_input_cache_hits: u64 = 0,
    graph_executor_metal_resident_input_cache_misses: u64 = 0,
    graph_executor_metal_resident_input_cache_unique_promotions: u64 = 0,
    graph_executor_metal_resident_input_cache_retained_live_bytes: u64 = 0,
    graph_executor_metal_resident_input_cache_retained_peak_bytes: u64 = 0,
    graph_executor_metal_resident_input_cache_reused_bytes: u64 = 0,
    graph_executor_metal_resident_input_cache_released_bytes: u64 = 0,
    graph_executor_metal_eager_arena_peak_bytes: u64 = 0,
    graph_executor_metal_eager_arena_live_bytes: u64 = 0,
    graph_executor_metal_eager_arena_reuse_hits: u64 = 0,
    graph_executor_metal_eager_arena_allocations: u64 = 0,
    graph_executor_metal_eager_arena_spill_bytes: u64 = 0,
    graph_executor_metal_eager_arena_hazard_declines: u64 = 0,
    graph_executor_metal_eager_arena_alias_conflicts: u64 = 0,
    graph_executor_metal_eager_arena_alias_reclaims: u64 = 0,
    graph_executor_metal_eager_arena_alias_reclaim_bytes: u64 = 0,
    graph_executor_regions: u64 = 0,
    graph_executor_region_ops: u64 = 0,
    graph_executor_runtime_region_dispatches: u64 = 0,
    graph_executor_runtime_region_fallbacks: u64 = 0,
    graph_executor_runtime_region_active_regions: u64 = 0,
    graph_executor_runtime_region_covered_nodes: u64 = 0,
    graph_executor_runtime_region_elided_nodes: u64 = 0,
    graph_executor_runtime_region_plan_compiles: u64 = 0,
    graph_executor_runtime_region_plan_reuses: u64 = 0,
    graph_executor_runtime_region_pre_skipped_nodes: u64 = 0,
    graph_executor_runtime_region_pre_skipped_transposes: u64 = 0,
    graph_executor_runtime_region_pre_skip_declined_external_consumers: u64 = 0,
    metal_deberta_ffn_forward_regions: u64 = 0,
    metal_deberta_encoder_lora_layer_regions: u64 = 0,
    metal_deberta_encoder_lora_residual_layernorm_regions: u64 = 0,
    metal_deberta_encoder_lora_layer_scaffold_regions: u64 = 0,
    metal_deberta_encoder_lora_layer_fallbacks: u64 = 0,
    metal_lora_backward_regions: u64 = 0,
    metal_low_rank_lora_backward_regions: u64 = 0,
    metal_rank_adapter_backward_regions: u64 = 0,
    metal_ffn_gelu_backward_regions: u64 = 0,
    metal_head_mlp_forward_regions: u64 = 0,
    metal_head_mlp_backward_regions: u64 = 0,
    metal_command_dot_general_dispatches: u64 = 0,
    metal_command_head_dot_dispatches: u64 = 0,
    metal_command_transpose_dispatches: u64 = 0,
    metal_command_gather_dispatches: u64 = 0,
    metal_command_reduce_dispatches: u64 = 0,
    metal_command_elementwise_dispatches: u64 = 0,
    metal_command_activation_dispatches: u64 = 0,
    metal_command_activation_backward_dispatches: u64 = 0,
    metal_command_other_dispatches: u64 = 0,
    graph_executor_runtime_frame_candidates: u64 = 0,
    graph_executor_runtime_frame_eligible: u64 = 0,
    graph_executor_runtime_frame_metadata_ready: u64 = 0,
    graph_executor_runtime_frame_ineligible_no_regions: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_qkv: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_attention: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_ffn: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_ple: u64 = 0,
    graph_executor_runtime_frame_ineligible_single_row: u64 = 0,
    graph_executor_runtime_frame_ineligible_non_layer_order: u64 = 0,
    graph_executor_runtime_frame_ineligible_shape_mismatch: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_model_metadata: u64 = 0,
    graph_executor_plan_build_ns: u64 = 0,
    graph_executor_buffer_plan_build_ns: u64 = 0,
    graph_executor_plan_cache_hits: u64 = 0,
    graph_executor_plan_cache_misses: u64 = 0,
    metal_frame_wait_ns: u64 = 0,
    metal_frame_gpu_ns: u64 = 0,
    metal_tensor_device_owned_live_bytes: u64 = 0,
    metal_tensor_device_owned_peak_live_bytes: u64 = 0,
    metal_tensor_device_owned_peak_lt_256kb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_256kb_1mb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_1mb_4mb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_4mb_16mb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_ge_16mb_bytes: u64 = 0,
    metal_runtime_total_bytes: u64 = 0,
    metal_runtime_scratch_bytes: u64 = 0,
    metal_runtime_scratch_pool_bytes: u64 = 0,
    metal_runtime_graph_plan_bytes: u64 = 0,
    metal_runtime_dense_linear_bytes: u64 = 0,
    metal_runtime_attention_span_bytes: u64 = 0,
    metal_runtime_hidden_state_bytes: u64 = 0,
    metal_runtime_frame_retained_bytes: u64 = 0,
    metal_runtime_reuse_pool_bytes: u64 = 0,
    metal_runtime_reuse_pool_slots: u64 = 0,
    metal_runtime_reuse_pool_peak_slots: u64 = 0,
    metal_runtime_reuse_alloc_delta: u64 = 0,
    metal_runtime_reuse_hit_delta: u64 = 0,
    metal_last_frame_compute_encoders: u64 = 0,
    metal_last_frame_blit_encoders: u64 = 0,
    metal_last_frame_planned_scopes: u64 = 0,
    metal_last_frame_planned_barriers: u64 = 0,
    metal_last_frame_planned_command_ops: u64 = 0,
    metal_deberta_encoder_plan_attempts: u64 = 0,
    metal_deberta_encoder_plan_successes: u64 = 0,
    metal_deberta_encoder_plan_reuses: u64 = 0,
    metal_deberta_encoder_plan_failures: u64 = 0,
    metal_deberta_encoder_layer_attempts: u64 = 0,
    metal_deberta_encoder_layer_successes: u64 = 0,
    metal_deberta_encoder_layer_fallbacks: u64 = 0,
    metal_deberta_relative_qk_pair_calls: u64 = 0,
    metal_deberta_relative_qk_pair_fallbacks: u64 = 0,
    metal_deberta_ffn_fused_calls: u64 = 0,
    metal_deberta_ffn_fused_mps_matmuls: u64 = 0,
    metal_deberta_ffn_fused_fallbacks: u64 = 0,
    metal_deberta_attention_flash_calls: u64 = 0,
    metal_deberta_attention_gemm_calls: u64 = 0,
    metal_deberta_attention_gemm_fallbacks: u64 = 0,
    metal_deberta_attention_legacy_calls: u64 = 0,
    cuda_device_allocations: u64 = 0,
    cuda_device_frees: u64 = 0,
    cuda_h2d_bytes: u64 = 0,
    cuda_d2h_bytes: u64 = 0,
    cuda_largest_d2h_transfer_bytes: u64 = 0,
    cuda_to_float32_calls: u64 = 0,
    cuda_download_alloc_calls: u64 = 0,
    cuda_stream_synchronizations: u64 = 0,
    cuda_upload_synchronizations: u64 = 0,
    cuda_temp_cache_hits: u64 = 0,
    cuda_temp_cache_misses: u64 = 0,
    cuda_kernel_launches: u64 = 0,
    cuda_packed_attention_forward_calls: u64 = 0,
    cuda_packed_attention_backward_calls: u64 = 0,
    cuda_exact_gelu_forward_calls: u64 = 0,
    cuda_exact_gelu_backward_calls: u64 = 0,
    cuda_training_input_uploads: u64 = 0,
    cuda_training_input_upload_bytes: u64 = 0,
};

pub const OptimizerBackend = enum { host, metal, cuda };

const ExecutionMode = enum {
    train,
    eval,
};

// ── Trainer ──────────────────────────────────────────────────────────────────

pub const max_conditional_optimizer_families = 8;

pub const ConditionalOptimizerFamily = struct {
    prefix: []const u8,
    window_present: bool = false,
};

pub const RealAutodiffTrainer = struct {
    allocator: std.mem.Allocator,
    compute_backend: *const ComputeBackend,
    /// Post-injection graph owned by the trainer. Built lazily on first step.
    graph_state: ?GraphState = null,
    config: TrainerConfig,
    optimizer_state: optimizers.OptimizerState,
    step_count: u64 = 0,
    // Number of completed optimizer updates (accumulation-window flushes). Unlike
    // step_count (micro-batches), this drives the LR schedule and the AdamW
    // bias-correction exponent so both are correct under gradient accumulation. At
    // grad_accum==1 it tracks step_count exactly (parity config is unchanged).
    optimizer_step_count: u64 = 0,
    /// Number of micro-batches already accumulated into `grad_accum` in the
    /// current accumulation window.
    accum_count: u32 = 0,
    device_optimizer_transfers: u64 = 0,
    device_trainable_bytes: usize = 0,
    runtime_input_cache: RuntimeInputCache = .{},
    /// Optional Hypura coordinator.
    coord: ?*TrainingMemoryCoordinator = null,
    compiled_session: ?training.CompiledTrainSession = null,
    graph_signature: ?GraphSignature = null,
    graph_context_state: ?*anyopaque = null,
    inactive_graphs: std.ArrayListUnmanaged(CachedGraphState) = .empty,
    graph_cache_clock: u64 = 0,
    graph_cache_builds: u64 = 0,
    graph_cache_hits: u64 = 0,
    graph_cache_active_reuses: u64 = 0,
    graph_cache_evictions: u64 = 0,
    graph_cache_peak_entries: usize = 0,
    /// Per-LoRA-parameter state. Indices match `graph_state.lora_adapter.adapters`:
    /// `lora_params[i*2]` = A matrix, `lora_params[i*2+1]` = B matrix.
    /// Optimizer families whose parameters only receive gradients when the
    /// current accumulation window contained a matching task (PyTorch leaves
    /// `grad=None` for modules absent from the loss graph, so their Adam
    /// state does not advance and decoupled weight decay is not applied).
    /// Registered prefixes must outlive the trainer (string literals).
    conditional_optimizer_families: [max_conditional_optimizer_families]ConditionalOptimizerFamily = undefined,
    conditional_optimizer_family_count: usize = 0,
    lora_params: std.ArrayListUnmanaged(ParamSlot) = .empty,
    /// Per-regular-parameter state for non-LoRA trainables such as task heads.
    regular_params: std.ArrayListUnmanaged(ParamSlot) = .empty,

    pub const ParamSlot = struct {
        /// Trainer-owned parameter name, stable across graph-cache eviction.
        name: []const u8,
        /// Owned mutable weight buffer updated by the optimizer.
        weights: []f32,
        /// Owned accumulator buffer (same length as `weights`). Holds the
        /// running mean gradient over the current accumulation window.
        grad_accum: []f32,
        /// NodeId of this parameter in `graph_state.graph`.
        node_id: NodeId,
        /// Shape used when binding the parameter tensor at runtime.
        dims: []i32,
        /// Residency-tracker block id, when the coordinator is attached.
        block_id: ?grad_residency.GradBlockId = null,
        /// True once the block has been registered with the coordinator.
        block_registered: bool = false,
        /// Optimizer steps this parameter actually took. Mirrors the host
        /// `optimizers.ParamState.step_count`, which conditional-family gating
        /// leaves behind the global optimizer step, and drives the device
        /// AdamW bias correction.
        adam_step_count: u32 = 0,
        device: ?DeviceOptimizerSlot = null,
    };

    pub const DeviceOptimizerSlot = struct {
        weight: CT,
        grad_accum: CT,
        m: CT,
        v: CT,
    };

    pub const RuntimeInputCache = struct {
        input_ids: CachedRuntimeTensor = .{},
        attention_mask: CachedRuntimeTensor = .{},
        targets: CachedRuntimeTensor = .{},
        gliner2_attn_bias: CachedRuntimeTensor = .{},
    };

    pub const CachedRuntimeTensor = struct {
        tensor: ?CT = null,
        dims: []i32 = &.{},
        elem_count: usize = 0,
    };

    pub const GraphSignature = struct {
        ctx: *anyopaque,
        build_forward: BuildForwardFn,
        build_loss: BuildLossFn,
        capture_graph_context: ?CaptureGraphContextFn,
        restore_graph_context: ?RestoreGraphContextFn,
        deinit_graph_context: ?DeinitGraphContextFn,
        batch: u32,
        seq_len: u32,
        targets_shape: Shape,
        variant: [4]u64,

        fn init(input: TrainerInput) GraphSignature {
            return .{
                .ctx = input.ctx,
                .build_forward = input.build_forward,
                .build_loss = input.build_loss,
                .capture_graph_context = input.capture_graph_context,
                .restore_graph_context = input.restore_graph_context,
                .deinit_graph_context = input.deinit_graph_context,
                .batch = input.batch,
                .seq_len = input.seq_len,
                .targets_shape = input.targets_shape,
                .variant = input.graph_variant,
            };
        }

        fn eql(a: GraphSignature, b: GraphSignature) bool {
            return a.ctx == b.ctx and
                a.build_forward == b.build_forward and
                a.build_loss == b.build_loss and
                a.capture_graph_context == b.capture_graph_context and
                a.restore_graph_context == b.restore_graph_context and
                a.deinit_graph_context == b.deinit_graph_context and
                a.batch == b.batch and
                a.seq_len == b.seq_len and
                Shape.eq(a.targets_shape, b.targets_shape) and
                std.mem.eql(u64, &a.variant, &b.variant);
        }
    };

    const CachedGraphState = struct {
        signature: GraphSignature,
        graph_state: GraphState,
        compiled_session: ?training.CompiledTrainSession,
        graph_context_state: ?*anyopaque,
        last_used: u64,

        fn deinit(self: *CachedGraphState, trainer: *RealAutodiffTrainer) void {
            if (self.compiled_session) |*session| session.deinit();
            if (self.signature.deinit_graph_context) |deinit_context| {
                deinit_context(self.graph_context_state, trainer.allocator);
            }
            self.graph_state.graph.deinit();
            self.graph_state.lora_adapter.deinit();
            self.* = undefined;
        }
    };

    pub const GraphState = struct {
        /// Immutable template graph with LoRA injected. The loss node is
        /// already part of this graph; the gradient graph is rebuilt on each
        /// call to `training.trainStep` because autodiff.gradient appends
        /// gradient nodes and we must run it against the original (loss)
        /// graph so LoRA parameter NodeIds stay stable across steps.
        graph: Graph,
        input_ids_node: NodeId,
        attention_mask_node: NodeId,
        targets_node: NodeId,
        loss_node: NodeId,
        forward_output_node: NodeId,
        lora_adapter: lora_mod.LoRAAdapter,
    };

    pub const GraphCacheStats = struct {
        capacity: usize,
        build_reserve_bytes: u64,
        builds: u64,
        hits: u64,
        active_reuses: u64,
        evictions: u64,
        resident_signatures: usize,
        peak_resident_signatures: usize,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        compute_backend: *const ComputeBackend,
        config: TrainerConfig,
    ) !RealAutodiffTrainer {
        return .{
            .allocator = allocator,
            .compute_backend = compute_backend,
            .config = config,
            .optimizer_state = optimizers.OptimizerState.init(allocator),
        };
    }

    pub fn deinit(self: *RealAutodiffTrainer) void {
        self.deinitActiveGraph();
        for (self.inactive_graphs.items) |*entry| entry.deinit(self);
        self.inactive_graphs.deinit(self.allocator);
        for (self.lora_params.items) |*slot| {
            self.deinitDeviceOptimizerSlot(slot);
            self.allocator.free(slot.name);
            self.allocator.free(slot.weights);
            self.allocator.free(slot.grad_accum);
            self.allocator.free(slot.dims);
        }
        self.lora_params.deinit(self.allocator);
        for (self.regular_params.items) |*slot| {
            self.deinitDeviceOptimizerSlot(slot);
            self.allocator.free(slot.name);
            self.allocator.free(slot.weights);
            self.allocator.free(slot.grad_accum);
            self.allocator.free(slot.dims);
        }
        self.regular_params.deinit(self.allocator);
        self.optimizer_state.deinit();
        self.* = undefined;
    }

    /// Optional: attach a Hypura coordinator for memory-aware training.
    pub fn attachCoordinator(self: *RealAutodiffTrainer, coord: *TrainingMemoryCoordinator) void {
        self.coord = coord;
    }

    /// Materialize the post-LoRA graph and parameter slots without running
    /// an optimizer step. This lets higher-level trainers seed LoRA weights
    /// from an external adapter checkpoint before training begins.
    pub fn ensureGraphBuilt(self: *RealAutodiffTrainer, input: TrainerInput) !void {
        const context_hook_count = @as(u2, @intFromBool(input.capture_graph_context != null)) +
            @as(u2, @intFromBool(input.restore_graph_context != null)) +
            @as(u2, @intFromBool(input.deinit_graph_context != null));
        if (context_hook_count != 0 and context_hook_count != 3) return error.IncompleteGraphContextHooks;
        self.graph_cache_clock +%= 1;
        const signature = GraphSignature.init(input);
        if (self.graph_signature) |active| {
            if (GraphSignature.eql(active, signature)) {
                self.graph_cache_active_reuses += 1;
                return;
            }
        }

        if (self.findInactiveGraph(signature)) |index| {
            var cached = self.inactive_graphs.swapRemove(index);
            var cached_owned = true;
            errdefer if (cached_owned) cached.deinit(self);
            try self.stashOrDropActiveGraph();
            self.graph_state = cached.graph_state;
            self.compiled_session = cached.compiled_session;
            self.runtime_input_cache = .{};
            self.graph_signature = cached.signature;
            self.graph_context_state = cached.graph_context_state;
            cached_owned = false;
            try self.bindParamSlotsToActiveGraph();
            if (input.restore_graph_context) |restore| try restore(input.ctx, self.graph_context_state);
            self.graph_cache_hits += 1;
            return;
        }

        try self.stashOrDropActiveGraph();
        compiledDiag(
            "graph build begin batch={} seq_len={} targets={} rss={}",
            .{ input.batch, input.seq_len, input.targets.len, currentResidentBytes() },
        );
        try self.buildGraphState(input);
        self.graph_signature = signature;
        if (input.capture_graph_context) |capture| {
            self.graph_context_state = capture(input.ctx, self.allocator) catch |err| {
                self.deinitActiveGraph();
                return err;
            };
        }
        self.graph_cache_builds += 1;
        self.updateGraphCachePeak();
        const gs = &self.graph_state.?;
        compiledDiag(
            "graph build done nodes={} params={} lora_slots={} regular_slots={} rss={}",
            .{
                gs.graph.nodeCount(),
                gs.graph.parameters.items.len,
                self.lora_params.items.len,
                self.regular_params.items.len,
                currentResidentBytes(),
            },
        );
    }

    /// Finish one-time device optimizer allocation and weight upload before a
    /// caller starts recording training-step latency or transfer telemetry.
    /// Checkpoint restore already uses the same idempotent initializer, so it
    /// is safe to call after either fresh or resumed setup.
    pub fn prepareDeviceTraining(self: *RealAutodiffTrainer) !void {
        if (!self.deviceOptimizerRequested()) return;
        if (self.graph_state == null) return error.GraphNotBuilt;
        try self.ensureDeviceOptimizerSlots();
        // Close the initialization stream interval before measured steps.
        // CUDA also uses this safe boundary to recycle its pinned upload ring.
        try self.compute_backend.trainingSynchronize();
    }

    fn findInactiveGraph(self: *const RealAutodiffTrainer, signature: GraphSignature) ?usize {
        for (self.inactive_graphs.items, 0..) |entry, index| {
            if (GraphSignature.eql(entry.signature, signature)) return index;
        }
        return null;
    }

    fn graphCacheCapacity(self: *const RealAutodiffTrainer) usize {
        return @max(@as(usize, self.config.graph_cache_capacity), 1);
    }

    fn stashOrDropActiveGraph(self: *RealAutodiffTrainer) !void {
        if (self.graph_state == null) return;
        const signature = self.graph_signature orelse return error.MissingGraphSignature;
        const inactive_capacity = self.graphCacheCapacity() - 1;
        if (inactive_capacity == 0 or !self.canRetainGraphWhileBuilding()) {
            self.deinitActiveGraph();
            return;
        }
        while (self.inactive_graphs.items.len >= inactive_capacity) {
            var lru_index: usize = 0;
            for (self.inactive_graphs.items[1..], 1..) |entry, index| {
                if (entry.last_used < self.inactive_graphs.items[lru_index].last_used) lru_index = index;
            }
            var evicted = self.inactive_graphs.swapRemove(lru_index);
            evicted.deinit(self);
            self.graph_cache_evictions += 1;
        }
        self.deinitRuntimeInputCache();
        try self.inactive_graphs.append(self.allocator, .{
            .signature = signature,
            .graph_state = self.graph_state.?,
            .compiled_session = self.compiled_session,
            .graph_context_state = self.graph_context_state,
            .last_used = self.graph_cache_clock,
        });
        self.graph_state = null;
        self.compiled_session = null;
        self.runtime_input_cache = .{};
        self.graph_context_state = null;
        self.graph_signature = null;
        self.updateGraphCachePeak();
    }

    fn canRetainGraphWhileBuilding(self: *const RealAutodiffTrainer) bool {
        const reserve = self.config.graph_cache_build_reserve_bytes;
        if (reserve == 0) return true;
        const info = system_memory.currentSystemMemoryInfo() orelse return false;
        const available = info.available_bytes orelse return false;
        return available >= reserve;
    }

    pub fn graphCacheStats(self: *const RealAutodiffTrainer) GraphCacheStats {
        return .{
            .capacity = self.graphCacheCapacity(),
            .build_reserve_bytes = self.config.graph_cache_build_reserve_bytes,
            .builds = self.graph_cache_builds,
            .hits = self.graph_cache_hits,
            .active_reuses = self.graph_cache_active_reuses,
            .evictions = self.graph_cache_evictions,
            .resident_signatures = self.inactive_graphs.items.len + @intFromBool(self.graph_state != null),
            .peak_resident_signatures = self.graph_cache_peak_entries,
        };
    }

    fn updateGraphCachePeak(self: *RealAutodiffTrainer) void {
        self.graph_cache_peak_entries = @max(
            self.graph_cache_peak_entries,
            self.inactive_graphs.items.len + @intFromBool(self.graph_state != null),
        );
    }

    fn deinitActiveGraph(self: *RealAutodiffTrainer) void {
        if (self.compiled_session) |*session| session.deinit();
        self.compiled_session = null;
        self.deinitRuntimeInputCache();
        if (self.graph_signature) |signature| {
            if (signature.deinit_graph_context) |deinit_context| {
                deinit_context(self.graph_context_state, self.allocator);
            }
        }
        self.graph_context_state = null;
        if (self.graph_state) |*gs| {
            gs.graph.deinit();
            gs.lora_adapter.deinit();
        }
        self.graph_state = null;
        self.graph_signature = null;
    }

    pub fn ensureCompiledSessionBuilt(self: *RealAutodiffTrainer, trainable: []const []const u8) !bool {
        if (self.config.execution_engine == .interpreter) return false;
        switch (self.config.execution_engine) {
            .interpreter => unreachable,
            .compiled_device => if (!self.compute_backend.supportsDeviceTraining()) {
                if (self.config.compiled_required) return error.DeviceTrainingUnavailable;
                return false;
            },
            .compiled_metal => if (self.compute_backend.kind() != .metal or !self.compute_backend.supportsDeviceTraining()) {
                if (self.config.compiled_required) return error.CompiledMetalRequiresMetalBackend;
                return false;
            },
        }
        if (self.compiled_session == null) {
            const gs = &(self.graph_state orelse return error.GraphNotBuilt);
            compiledDiag(
                "compiled session build begin trainable={} graph_nodes={} rss={}",
                .{ trainable.len, gs.graph.nodeCount(), currentResidentBytes() },
            );
            self.compiled_session = try training.CompiledTrainSession.init(
                self.allocator,
                &gs.graph,
                gs.loss_node,
                .{ .trainable_params = trainable, .checkpoint_config = self.config.checkpoint_config },
            );
            compiledDiag(
                "compiled session build done compiled_nodes={} outputs={} compile_ms={d:.3} peak_rss={}",
                .{
                    self.compiled_session.?.graph.nodeCount(),
                    self.compiled_session.?.graph.outputs.items.len,
                    nsToMs(self.compiled_session.?.build_profile.total_ns),
                    self.compiled_session.?.build_profile.peak_resident_bytes,
                },
            );
        }
        return true;
    }

    /// Run one training step.
    pub fn step(self: *RealAutodiffTrainer, input: TrainerInput) !StepResult {
        return self.runStep(input, .train);
    }

    pub fn evaluate(self: *RealAutodiffTrainer, input: TrainerInput) !StepResult {
        return self.runStep(input, .eval);
    }

    pub fn optimizerSteps(self: *const RealAutodiffTrainer) u64 {
        return self.optimizer_step_count;
    }

    pub fn microBatchSteps(self: *const RealAutodiffTrainer) u64 {
        return self.step_count;
    }

    /// Save trainable weights plus Adam moments at an accumulation boundary.
    /// Higher-level trainers pair this with their deterministic epoch index so
    /// data ordering can be reconstructed exactly on resume.
    pub fn saveTrainingState(
        self: *RealAutodiffTrainer,
        path: []const u8,
        run_fingerprint: ?*const [32]u8,
        metrics_prefix_sha256: ?*const [32]u8,
    ) !void {
        if (self.accum_count != 0) return error.CheckpointDuringGradientAccumulation;
        if (self.optimizer_step_count > self.step_count) return error.InvalidTrainingStateCounters;
        if (self.optimizer_step_count > std.math.maxInt(u32)) return error.CheckpointStepOverflow;

        var tensors = std.ArrayListUnmanaged(safetensors_checkpoint.NamedTensor).empty;
        defer tensors.deinit(self.allocator);
        var names = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (names.items) |name| self.allocator.free(name);
            names.deinit(self.allocator);
        }
        var shapes = std.ArrayListUnmanaged([]usize).empty;
        defer {
            for (shapes.items) |shape| self.allocator.free(shape);
            shapes.deinit(self.allocator);
        }
        var owned_data = std.ArrayListUnmanaged([]f32).empty;
        defer {
            for (owned_data.items) |data| self.allocator.free(data);
            owned_data.deinit(self.allocator);
        }

        const counters = encodeTrainingStateCounters(self.step_count, self.optimizer_step_count);
        try tensors.append(self.allocator, .{
            .name = "__trainer_counters",
            .data = &counters,
            .shape = &.{counters.len},
        });
        var fingerprint_values: [32]f32 = undefined;
        if (run_fingerprint) |fingerprint| {
            for (fingerprint, 0..) |byte, idx| fingerprint_values[idx] = @floatFromInt(byte);
            try tensors.append(self.allocator, .{
                .name = "__run_fingerprint",
                .data = &fingerprint_values,
                .shape = &.{fingerprint_values.len},
            });
        }
        var metrics_fingerprint_values: [32]f32 = undefined;
        if (metrics_prefix_sha256) |fingerprint| {
            for (fingerprint, 0..) |byte, idx| metrics_fingerprint_values[idx] = @floatFromInt(byte);
            try tensors.append(self.allocator, .{
                .name = "__metrics_prefix_sha256",
                .data = &metrics_fingerprint_values,
                .shape = &.{metrics_fingerprint_values.len},
            });
        }
        try self.appendTrainingStateSlots(&tensors, &names, &shapes, &owned_data, self.lora_params.items);
        try self.appendTrainingStateSlots(&tensors, &names, &shapes, &owned_data, self.regular_params.items);

        const temporary_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp", .{path});
        defer self.allocator.free(temporary_path);
        std.Io.Dir.cwd().deleteFile(compat.testingIo(), temporary_path) catch {};
        errdefer std.Io.Dir.cwd().deleteFile(compat.testingIo(), temporary_path) catch {};
        try safetensors_checkpoint.save(self.allocator, temporary_path, tensors.items);
        try std.Io.Dir.rename(std.Io.Dir.cwd(), temporary_path, std.Io.Dir.cwd(), path, compat.testingIo());
        try syncParentDirectory(path);
    }

    /// Verify that the metrics prefix used to reconstruct aggregate state is
    /// exactly the prefix durably paired with this checkpoint.
    pub fn validateTrainingStateMetricsPrefix(
        self: *RealAutodiffTrainer,
        path: []const u8,
        expected: *const [32]u8,
    ) !void {
        const absolute_path = try std.Io.Dir.cwd().realPathFileAlloc(compat.testingIo(), path, self.allocator);
        defer self.allocator.free(absolute_path);
        var reader = try safetensors.MMapReader.openFileAbsolute(self.allocator, absolute_path);
        defer reader.deinit();
        try validateTrainingStateDigest(&reader, "__metrics_prefix_sha256", expected, error.TrainingStateMetricsMismatch);
    }

    /// Restore a state written by `saveTrainingState`. The graph must already
    /// be built so checkpoint names and shapes are checked against live slots.
    pub fn loadTrainingState(self: *RealAutodiffTrainer, path: []const u8, expected_run_fingerprint: ?*const [32]u8) !void {
        if (self.lora_params.items.len == 0 and self.regular_params.items.len == 0) return error.GraphNotBuilt;
        if (self.step_count != 0 or self.optimizer_step_count != 0 or self.accum_count != 0 or self.optimizer_state.param_states.count() != 0) {
            return error.TrainerAlreadyStarted;
        }
        const absolute_path = try std.Io.Dir.cwd().realPathFileAlloc(compat.testingIo(), path, self.allocator);
        defer self.allocator.free(absolute_path);
        var reader = try safetensors.MMapReader.openFileAbsolute(self.allocator, absolute_path);
        defer reader.deinit();

        var counter_tensor = try reader.readTensor("__trainer_counters");
        defer counter_tensor.deinit();
        if (counter_tensor.dtype != .f32) return error.InvalidTrainingStateDType;
        const counters = counter_tensor.asFloat32();
        const restored = try decodeTrainingStateCounters(counters);
        if (restored.optimizer_steps > restored.micro_batch_steps) return error.InvalidTrainingStateCounters;
        if (restored.optimizer_steps > std.math.maxInt(u32)) return error.CheckpointStepOverflow;
        if (expected_run_fingerprint) |expected| try validateTrainingStateFingerprint(&reader, expected);

        try self.loadTrainingStateSlots(&reader, self.lora_params.items, restored.optimizer_steps);
        try self.loadTrainingStateSlots(&reader, self.regular_params.items, restored.optimizer_steps);
        self.step_count = restored.micro_batch_steps;
        self.optimizer_step_count = restored.optimizer_steps;
        self.optimizer_state.step_count = @intCast(restored.optimizer_steps);
        if (self.deviceOptimizerRequested()) try self.uploadRestoredOptimizerState();
    }

    /// Validate a recoverable checkpoint without mutating trainer state.
    pub fn inspectTrainingState(
        self: *RealAutodiffTrainer,
        path: []const u8,
        expected_run_fingerprint: ?*const [32]u8,
    ) !RestoredTrainingStateCounters {
        const absolute_path = try std.Io.Dir.cwd().realPathFileAlloc(compat.testingIo(), path, self.allocator);
        defer self.allocator.free(absolute_path);
        var reader = try safetensors.MMapReader.openFileAbsolute(self.allocator, absolute_path);
        defer reader.deinit();
        var counter_tensor = try reader.readTensor("__trainer_counters");
        defer counter_tensor.deinit();
        if (counter_tensor.dtype != .f32) return error.InvalidTrainingStateDType;
        const counters = try decodeTrainingStateCounters(counter_tensor.asFloat32());
        if (counters.optimizer_steps > counters.micro_batch_steps) return error.InvalidTrainingStateCounters;
        if (expected_run_fingerprint) |expected| try validateTrainingStateFingerprint(&reader, expected);
        return counters;
    }

    /// Select checkpoint weights for final export without rewinding optimizer
    /// counters. This is intentionally terminal: device optimizer slots are
    /// dropped after their host weights are replaced.
    pub fn loadTrainingStateWeightsForExport(
        self: *RealAutodiffTrainer,
        path: []const u8,
        expected_run_fingerprint: ?*const [32]u8,
    ) !void {
        _ = try self.inspectTrainingState(path, expected_run_fingerprint);
        const absolute_path = try std.Io.Dir.cwd().realPathFileAlloc(compat.testingIo(), path, self.allocator);
        defer self.allocator.free(absolute_path);
        var reader = try safetensors.MMapReader.openFileAbsolute(self.allocator, absolute_path);
        defer reader.deinit();
        for (self.lora_params.items) |*slot| {
            try readTrainingStateTensor(self.allocator, &reader, "weight", slot.name, slot.weights);
            self.deinitDeviceOptimizerSlot(slot);
        }
        for (self.regular_params.items) |*slot| {
            try readTrainingStateTensor(self.allocator, &reader, "weight", slot.name, slot.weights);
            self.deinitDeviceOptimizerSlot(slot);
        }
    }

    /// Apply a final, incomplete accumulation window. Match the pinned
    /// Register a parameter-name prefix whose optimizer step is gated on
    /// per-window task presence (see `conditional_optimizer_families`).
    /// Families start absent each accumulation window; callers mark presence
    /// per micro-batch with `markOptimizerFamilyPresent`.
    pub fn registerConditionalOptimizerFamily(self: *RealAutodiffTrainer, prefix: []const u8) !void {
        if (self.conditional_optimizer_family_count >= max_conditional_optimizer_families)
            return error.TooManyConditionalOptimizerFamilies;
        self.conditional_optimizer_families[self.conditional_optimizer_family_count] = .{ .prefix = prefix };
        self.conditional_optimizer_family_count += 1;
    }

    pub fn markOptimizerFamilyPresent(self: *RealAutodiffTrainer, prefix: []const u8) void {
        for (self.conditional_optimizer_families[0..self.conditional_optimizer_family_count]) |*family| {
            if (std.mem.eql(u8, family.prefix, prefix)) family.window_present = true;
        }
    }

    fn optimizerFamilyAbsent(self: *const RealAutodiffTrainer, name: []const u8) bool {
        for (self.conditional_optimizer_families[0..self.conditional_optimizer_family_count]) |family| {
            if (!family.window_present and std.mem.startsWith(u8, name, family.prefix)) return true;
        }
        return false;
    }

    fn resetOptimizerFamilyWindow(self: *RealAutodiffTrainer) void {
        for (self.conditional_optimizer_families[0..self.conditional_optimizer_family_count]) |*family| {
            family.window_present = false;
        }
    }

    /// Step one enrolled trainable on the host optimizer. Parameters whose
    /// module produced gradients this window step even when those gradients are
    /// exactly zero (e.g. LoRA-A at step 0, where grad_A ∝ B=0): PyTorch
    /// advances the per-parameter step and applies decoupled weight decay
    /// whenever `p.grad` EXISTS. Modules absent from the loss graph for the
    /// whole window — registered conditional families with no matching batch
    /// task — keep `grad=None` upstream, so they get no step and no decay.
    fn stepHostAdamWSlot(
        self: *RealAutodiffTrainer,
        config: optimizers.Optimizer,
        slot: *ParamSlot,
        learning_rate: f32,
    ) !void {
        if (!self.optimizerFamilyAbsent(slot.name)) {
            try optimizers.step(config, &self.optimizer_state, learning_rate, slot.name, slot.weights, slot.grad_accum);
            slot.adam_step_count += 1;
        }
        @memset(slot.grad_accum, 0);
    }

    /// upstream trainer: every micro-batch was divided by the configured
    /// accumulation count, and a partial flush keeps that divisor.
    pub fn flushAccumulatedGradients(self: *RealAutodiffTrainer) !?FlushResult {
        if (self.accum_count == 0) return null;

        const micro_batches = self.accum_count;
        const use_device_optimizer = self.deviceOptimizerRequested();
        try self.reduceAccumulatedGradients(use_device_optimizer);

        self.optimizer_state.step_count = @intCast(self.optimizer_step_count + 1);
        const learning_rate = self.config.lr_schedule.lr(@intCast(self.optimizer_step_count));
        var grad_norm: f32 = undefined;
        if (use_device_optimizer) {
            const accumulated_norm = try self.deviceGlobalGradNorm();
            grad_norm = accumulated_norm;
            const clip_scale = if (self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm)
                self.config.max_grad_norm / (grad_norm + 1e-6)
            else
                1.0;
            try self.stepDeviceAdamW(learning_rate, clip_scale);
            for (self.lora_params.items) |*slot| @memset(slot.grad_accum, 0);
            for (self.regular_params.items) |*slot| @memset(slot.grad_accum, 0);
        } else {
            grad_norm = self.globalGradNorm();
            if (self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm) {
                const clip_scale = self.config.max_grad_norm / (grad_norm + 1e-6);
                for (self.lora_params.items) |*slot| {
                    for (slot.grad_accum) |*value| value.* *= clip_scale;
                }
                for (self.regular_params.items) |*slot| {
                    for (slot.grad_accum) |*value| value.* *= clip_scale;
                }
            }

            const optimizer = optimizers.Optimizer{ .adamw = self.config.optimizer };
            for (self.lora_params.items) |*slot| try self.stepHostAdamWSlot(optimizer, slot, learning_rate);
            for (self.regular_params.items) |*slot| try self.stepHostAdamWSlot(optimizer, slot, learning_rate);
        }

        self.optimizer_step_count += 1;
        self.accum_count = 0;
        self.resetOptimizerFamilyWindow();
        return .{
            .grad_norm = grad_norm,
            .learning_rate = learning_rate,
            .optimizer_step = self.optimizer_step_count,
            .micro_batches = micro_batches,
        };
    }

    fn runStep(self: *RealAutodiffTrainer, input: TrainerInput, mode: ExecutionMode) !StepResult {
        const total_start_ns = monotonicNowNs();
        // Sample before graph selection and runtime-input staging. The
        // executor also records a narrower internal profile, but the public
        // step contract must include all H2D/D2H and synchronization work
        // performed on behalf of this training step.
        const training_runtime_before = self.compute_backend.trainingRuntimeStats();
        // Initialization, checkpoint save, and checkpoint restore may
        // legitimately transfer resident optimizer state between steps. The
        // release metric is a hot-step invariant, so report only transfers
        // initiated while executing this step instead of the lifetime total.
        const device_optimizer_transfers_start = self.device_optimizer_transfers;
        var profile = StepProfile{};

        // 1. Lazy graph construction.
        const graph_build_start_ns = monotonicNowNs();
        try self.ensureGraphBuilt(input);
        profile.graph_build_ns = elapsedNs(graph_build_start_ns, monotonicNowNs());
        const gs = &self.graph_state.?;
        const use_device_optimizer = mode == .train and self.deviceOptimizerRequested();
        const use_cached_runtime_inputs = use_device_optimizer and
            (self.compute_backend.kind() == .metal or self.compute_backend.kind() == .cuda);
        if (use_device_optimizer) {
            compiledDiag(
                "device optimizer slots begin lora_slots={} regular_slots={} rss={}",
                .{ self.lora_params.items.len, self.regular_params.items.len, currentResidentBytes() },
            );
            try self.ensureDeviceOptimizerSlots();
            compiledDiag(
                "device optimizer slots done trainable_bytes={} transfers={} rss={}",
                .{ self.device_trainable_bytes, self.device_optimizer_transfers, currentResidentBytes() },
            );
            profile.optimizer_backend = self.deviceOptimizerBackend();
            profile.device_resident_transfer_count = self.device_optimizer_transfers - device_optimizer_transfers_start;
            profile.device_trainable_bytes = self.device_trainable_bytes;
        }

        // 2. Optional activation-budget reservation via the coordinator.
        var lora_blocks_pinned = false;
        defer if (lora_blocks_pinned) self.unpinAllLoraBlocks() catch {};
        if (self.coord != null) {
            try self.reserveActivationBudget(input);
            // Set this before pinning so a partial pin failure is also cleaned up.
            lora_blocks_pinned = true;
            try self.pinAllLoraBlocks();
        }

        // 3. Build the runtime input map: input_ids + mask + targets + all
        //    LoRA parameter tensors. The underlying interpreter will free the
        //    CT handles after execution — we re-upload them on every step.
        const runtime_input_start_ns = monotonicNowNs();
        var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
        var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
        defer {
            var it = rt.iterator();
            while (it.next()) |e| {
                if (!borrowed_rt.contains(e.key_ptr.*)) self.compute_backend.free(e.value_ptr.*);
            }
            borrowed_rt.deinit(self.allocator);
            rt.deinit(self.allocator);
        }

        // input_ids as f32 (graph represents them as f32 placeholders to
        // match the Builder parameter API; the callback is free to gather
        // from an embedding table inside the forward pass).
        const input_ids_f32 = try self.allocator.alloc(f32, input.input_ids.len);
        defer self.allocator.free(input_ids_f32);
        for (input.input_ids, 0..) |id, i| input_ids_f32[i] = @floatFromInt(id);

        const ids_dims = [_]i32{ @intCast(input.batch), @intCast(input.seq_len) };
        if (use_cached_runtime_inputs) {
            try self.putCachedRuntimeInput(&rt, &borrowed_rt, &self.runtime_input_cache.input_ids, gs.input_ids_node, input_ids_f32, &ids_dims);
            try self.putCachedRuntimeInput(&rt, &borrowed_rt, &self.runtime_input_cache.attention_mask, gs.attention_mask_node, input.attention_mask, &ids_dims);
        } else {
            try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.input_ids_node, input_ids_f32, &ids_dims);
            try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.attention_mask_node, input.attention_mask, &ids_dims);
        }

        const target_dims = try shapeToDims(self.allocator, input.targets_shape);
        defer self.allocator.free(target_dims);
        if (use_cached_runtime_inputs) {
            try self.putCachedRuntimeInput(&rt, &borrowed_rt, &self.runtime_input_cache.targets, gs.targets_node, input.targets, target_dims);
        } else {
            try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.targets_node, input.targets, target_dims);
        }

        // LoRA dropout masks. These are non-trainable per-step inputs used
        // only by adapters injected with config.lora.dropout > 0. Eval binds
        // all-ones masks so reload/materialization paths are deterministic.
        if (self.config.lora.dropout > 0.0) {
            var mask_seed = self.config.seed ^ (self.step_count + 0x9e3779b97f4a7c15);
            for (gs.lora_adapter.adapters.items, 0..) |info, idx| {
                if (info.dropout_mask_id == null_node) continue;
                const mask_node = gs.graph.node(info.dropout_mask_id);
                const mask_dims = try shapeToDims(self.allocator, mask_node.output_shape);
                defer self.allocator.free(mask_dims);
                var mask_len: usize = 1;
                for (mask_dims) |dim| {
                    if (dim <= 0) return error.InvalidTensorShape;
                    mask_len *= @intCast(dim);
                }
                const mask = try self.allocator.alloc(f32, mask_len);
                defer self.allocator.free(mask);
                if (mode == .train) {
                    mask_seed +%= @as(u64, @intCast(idx)) *% 0xbf58476d1ce4e5b9;
                    var prng = std.Random.DefaultPrng.init(mask_seed);
                    var rng = prng.random();
                    const keep_prob = 1.0 - self.config.lora.dropout;
                    const keep_scale = 1.0 / keep_prob;
                    for (mask) |*value| {
                        value.* = if (rng.float(f32) < keep_prob) keep_scale else 0.0;
                    }
                } else {
                    @memset(mask, 1.0);
                }
                try putRuntimeInput(self.allocator, self.compute_backend, &rt, info.dropout_mask_id, mask, mask_dims);
            }
        }

        // LoRA parameter slices.
        for (self.lora_params.items) |*slot| {
            if (slot.device) |device| {
                try rt.put(self.allocator, slot.node_id, device.weight);
                try borrowed_rt.put(self.allocator, slot.node_id, {});
            } else {
                try putRuntimeInput(self.allocator, self.compute_backend, &rt, slot.node_id, slot.weights, slot.dims);
            }
        }
        for (self.regular_params.items) |*slot| {
            if (slot.device) |device| {
                try rt.put(self.allocator, slot.node_id, device.weight);
                try borrowed_rt.put(self.allocator, slot.node_id, {});
            } else {
                try putRuntimeInput(self.allocator, self.compute_backend, &rt, slot.node_id, slot.weights, slot.dims);
            }
        }

        if (use_cached_runtime_inputs) {
            try self.putKnownCachedArchInputs(&rt, &borrowed_rt, &gs.graph, input.batch, input.seq_len, input.attention_mask);
        }

        // 3b. Architecture-specific input binding. This is how BERT
        //     position_ids, Qwen2 RoPE tables, LayoutLMv3 bbox components,
        //     etc. get into the runtime map. Without this, only the 3
        //     standard placeholders + LoRA params are bound.
        if (input.bind_arch_inputs) |bind_fn| {
            if (!use_cached_runtime_inputs or self.hasUnboundRuntimePlaceholders(&gs.graph, &rt)) {
                try bind_fn(input.ctx, self.compute_backend, self.allocator, &gs.graph, &rt, input.batch, input.seq_len, input.attention_mask);
            }
        }
        profile.runtime_input_ns = elapsedNs(runtime_input_start_ns, monotonicNowNs());
        if (use_device_optimizer) {
            compiledDiag(
                "runtime inputs ready entries={} borrowed={} runtime_ms={d:.3} transfers={} rss={}",
                .{ rt.count(), borrowed_rt.count(), nsToMs(profile.runtime_input_ns), self.device_optimizer_transfers, currentResidentBytes() },
            );
        }

        // 4. Run the graph training step (autodiff + execute + extract).
        if (mode == .train) {
            // AdamW bias-correction exponent for this update = optimizer steps so far + 1.
            self.optimizer_state.step_count = @intCast(self.optimizer_step_count + 1);
        }

        // Collect the list of trainer-owned parameter names we want
        // gradients for: LoRA adapters plus explicitly enrolled regular
        // trainables such as task heads.
        var trainable = try self.allocator.alloc([]const u8, self.lora_params.items.len + self.regular_params.items.len);
        defer self.allocator.free(trainable);
        var trainable_idx: usize = 0;
        for (self.lora_params.items) |slot| {
            trainable[trainable_idx] = slot.name;
            trainable_idx += 1;
        }
        for (self.regular_params.items) |slot| {
            trainable[trainable_idx] = slot.name;
            trainable_idx += 1;
        }

        const train_step_start_ns = monotonicNowNs();
        const had_compiled_session = self.compiled_session != null;
        const use_compiled = try self.ensureCompiledSessionBuilt(trainable);
        if (use_compiled) {
            compiledDiag(
                "compiled execute dispatch device_optimizer={} trainable={} runtime_inputs={} rss={}",
                .{ use_device_optimizer, trainable.len, rt.count(), currentResidentBytes() },
            );
        }
        var step_result = if (use_compiled and use_device_optimizer)
            try self.compiled_session.?.executeDeviceGradients(self.compute_backend, rt)
        else if (use_compiled)
            try self.compiled_session.?.execute(self.compute_backend, rt)
        else
            try training.trainStep(
                self.allocator,
                &gs.graph,
                gs.loss_node,
                self.compute_backend,
                rt,
                .{
                    .trainable_params = trainable,
                    .checkpoint_config = self.config.checkpoint_config,
                },
            );
        profile.train_step_ns = elapsedNs(train_step_start_ns, monotonicNowNs());
        if (use_compiled) {
            compiledDiag(
                "compiled execute complete train_step_ms={d:.3} loss={d:.6} rss={}",
                .{ nsToMs(profile.train_step_ns), step_result.loss, currentResidentBytes() },
            );
        }
        profile.autodiff_ns = if (use_compiled) 0 else step_result.profile.autodiff_ns;
        profile.compile_ns = if (use_compiled and !had_compiled_session) self.compiled_session.?.build_profile.total_ns else 0;
        profile.execute_ns = step_result.profile.execute_ns;
        profile.extract_ns = step_result.profile.extract_ns;
        profile.peak_resident_bytes = step_result.profile.peak_resident_bytes;
        profile.graph_executor_fallback_reason = step_result.profile.graph_executor_fallback_reason;
        profile.graph_executor_partitions = step_result.profile.graph_executor_partitions;
        profile.graph_executor_command_dispatches = step_result.profile.graph_executor_command_dispatches;
        profile.graph_executor_planned_dispatches = step_result.profile.graph_executor_planned_dispatches;
        profile.graph_executor_interpreter_fallbacks = step_result.profile.graph_executor_interpreter_fallbacks;
        profile.graph_executor_host_outputs = step_result.profile.graph_executor_host_outputs;
        profile.graph_executor_true_host_outputs = step_result.profile.graph_executor_true_host_outputs;
        profile.graph_executor_host_output_command = step_result.profile.graph_executor_host_output_command;
        profile.graph_executor_host_output_interpreter = step_result.profile.graph_executor_host_output_interpreter;
        profile.graph_executor_host_output_pre_materialized_constant = step_result.profile.graph_executor_host_output_pre_materialized_constant;
        profile.graph_executor_host_output_runtime_region = step_result.profile.graph_executor_host_output_runtime_region;
        profile.graph_executor_host_output_parameter = step_result.profile.graph_executor_host_output_parameter;
        profile.graph_executor_host_output_unattributed = step_result.profile.graph_executor_host_output_unattributed;
        profile.graph_executor_device_outputs = step_result.profile.graph_executor_device_outputs;
        profile.graph_executor_device_output_parameter = step_result.profile.graph_executor_device_output_parameter;
        profile.graph_executor_metal_frame_chunk_boundaries = step_result.profile.graph_executor_metal_frame_chunk_boundaries;
        profile.graph_executor_metal_frame_chunk_promoted_values = step_result.profile.graph_executor_metal_frame_chunk_promoted_values;
        profile.graph_executor_metal_frame_chunk_swept_values = step_result.profile.graph_executor_metal_frame_chunk_swept_values;
        profile.graph_executor_metal_chunk_local_output_peak_bytes = step_result.profile.graph_executor_metal_chunk_local_output_peak_bytes;
        profile.graph_executor_metal_chunk_local_output_live_bytes = step_result.profile.graph_executor_metal_chunk_local_output_live_bytes;
        profile.graph_executor_metal_chunk_local_output_allocations = step_result.profile.graph_executor_metal_chunk_local_output_allocations;
        profile.graph_executor_metal_chunk_local_output_reuse_hits = step_result.profile.graph_executor_metal_chunk_local_output_reuse_hits;
        profile.graph_executor_metal_chunk_local_output_consumed_hints = step_result.profile.graph_executor_metal_chunk_local_output_consumed_hints;
        profile.graph_executor_metal_chunk_local_output_unconsumed_hints = step_result.profile.graph_executor_metal_chunk_local_output_unconsumed_hints;
        profile.graph_executor_metal_chunk_local_output_spill_bytes = step_result.profile.graph_executor_metal_chunk_local_output_spill_bytes;
        profile.graph_executor_metal_chunk_local_output_alias_conflicts = step_result.profile.graph_executor_metal_chunk_local_output_alias_conflicts;
        profile.graph_executor_metal_chunk_local_output_resets = step_result.profile.graph_executor_metal_chunk_local_output_resets;
        profile.graph_executor_metal_chunk_local_output_reset_freed_bytes = step_result.profile.graph_executor_metal_chunk_local_output_reset_freed_bytes;
        profile.graph_executor_metal_chunk_local_output_discard_freed_bytes = step_result.profile.graph_executor_metal_chunk_local_output_discard_freed_bytes;
        profile.graph_executor_metal_chunk_local_output_reset_live_carry_values = step_result.profile.graph_executor_metal_chunk_local_output_reset_live_carry_values;
        profile.graph_executor_metal_gather_input_promotions = step_result.profile.graph_executor_metal_gather_input_promotions;
        profile.graph_executor_metal_gather_input_promotion_bytes = step_result.profile.graph_executor_metal_gather_input_promotion_bytes;
        profile.graph_executor_metal_gather_input_promotion_ns = step_result.profile.graph_executor_metal_gather_input_promotion_ns;
        profile.graph_executor_metal_gather_output_promotions = step_result.profile.graph_executor_metal_gather_output_promotions;
        profile.graph_executor_metal_gather_output_promotion_bytes = step_result.profile.graph_executor_metal_gather_output_promotion_bytes;
        profile.graph_executor_metal_gather_output_promotion_ns = step_result.profile.graph_executor_metal_gather_output_promotion_ns;
        profile.graph_executor_metal_reduce_input_promotions = step_result.profile.graph_executor_metal_reduce_input_promotions;
        profile.graph_executor_metal_reduce_input_promotion_bytes = step_result.profile.graph_executor_metal_reduce_input_promotion_bytes;
        profile.graph_executor_metal_reduce_input_promotion_ns = step_result.profile.graph_executor_metal_reduce_input_promotion_ns;
        profile.graph_executor_metal_resident_input_cache_hits = step_result.profile.graph_executor_metal_resident_input_cache_hits;
        profile.graph_executor_metal_resident_input_cache_misses = step_result.profile.graph_executor_metal_resident_input_cache_misses;
        profile.graph_executor_metal_resident_input_cache_unique_promotions = step_result.profile.graph_executor_metal_resident_input_cache_unique_promotions;
        profile.graph_executor_metal_resident_input_cache_retained_live_bytes = step_result.profile.graph_executor_metal_resident_input_cache_retained_live_bytes;
        profile.graph_executor_metal_resident_input_cache_retained_peak_bytes = step_result.profile.graph_executor_metal_resident_input_cache_retained_peak_bytes;
        profile.graph_executor_metal_resident_input_cache_reused_bytes = step_result.profile.graph_executor_metal_resident_input_cache_reused_bytes;
        profile.graph_executor_metal_resident_input_cache_released_bytes = step_result.profile.graph_executor_metal_resident_input_cache_released_bytes;
        profile.graph_executor_metal_eager_arena_peak_bytes = step_result.profile.graph_executor_metal_eager_arena_peak_bytes;
        profile.graph_executor_metal_eager_arena_live_bytes = step_result.profile.graph_executor_metal_eager_arena_live_bytes;
        profile.graph_executor_metal_eager_arena_reuse_hits = step_result.profile.graph_executor_metal_eager_arena_reuse_hits;
        profile.graph_executor_metal_eager_arena_allocations = step_result.profile.graph_executor_metal_eager_arena_allocations;
        profile.graph_executor_metal_eager_arena_spill_bytes = step_result.profile.graph_executor_metal_eager_arena_spill_bytes;
        profile.graph_executor_metal_eager_arena_hazard_declines = step_result.profile.graph_executor_metal_eager_arena_hazard_declines;
        profile.graph_executor_metal_eager_arena_alias_conflicts = step_result.profile.graph_executor_metal_eager_arena_alias_conflicts;
        profile.graph_executor_metal_eager_arena_alias_reclaims = step_result.profile.graph_executor_metal_eager_arena_alias_reclaims;
        profile.graph_executor_metal_eager_arena_alias_reclaim_bytes = step_result.profile.graph_executor_metal_eager_arena_alias_reclaim_bytes;
        profile.graph_executor_regions = step_result.profile.graph_executor_regions;
        profile.graph_executor_region_ops = step_result.profile.graph_executor_region_ops;
        profile.graph_executor_runtime_region_dispatches = step_result.profile.graph_executor_runtime_region_dispatches;
        profile.graph_executor_runtime_region_fallbacks = step_result.profile.graph_executor_runtime_region_fallbacks;
        profile.graph_executor_runtime_region_active_regions = step_result.profile.graph_executor_runtime_region_active_regions;
        profile.graph_executor_runtime_region_covered_nodes = step_result.profile.graph_executor_runtime_region_covered_nodes;
        profile.graph_executor_runtime_region_elided_nodes = step_result.profile.graph_executor_runtime_region_elided_nodes;
        profile.graph_executor_runtime_region_plan_compiles = step_result.profile.graph_executor_runtime_region_plan_compiles;
        profile.graph_executor_runtime_region_plan_reuses = step_result.profile.graph_executor_runtime_region_plan_reuses;
        profile.graph_executor_runtime_region_pre_skipped_nodes = step_result.profile.graph_executor_runtime_region_pre_skipped_nodes;
        profile.graph_executor_runtime_region_pre_skipped_transposes = step_result.profile.graph_executor_runtime_region_pre_skipped_transposes;
        profile.graph_executor_runtime_region_pre_skip_declined_external_consumers = step_result.profile.graph_executor_runtime_region_pre_skip_declined_external_consumers;
        profile.metal_deberta_ffn_forward_regions = step_result.profile.metal_deberta_ffn_forward_regions;
        profile.metal_deberta_encoder_lora_layer_regions = step_result.profile.metal_deberta_encoder_lora_layer_regions;
        profile.metal_deberta_encoder_lora_residual_layernorm_regions = step_result.profile.metal_deberta_encoder_lora_residual_layernorm_regions;
        profile.metal_deberta_encoder_lora_layer_scaffold_regions = step_result.profile.metal_deberta_encoder_lora_layer_scaffold_regions;
        profile.metal_deberta_encoder_lora_layer_fallbacks = step_result.profile.metal_deberta_encoder_lora_layer_fallbacks;
        profile.metal_lora_backward_regions = step_result.profile.metal_lora_backward_regions;
        profile.metal_low_rank_lora_backward_regions = step_result.profile.metal_low_rank_lora_backward_regions;
        profile.metal_rank_adapter_backward_regions = step_result.profile.metal_rank_adapter_backward_regions;
        profile.metal_ffn_gelu_backward_regions = step_result.profile.metal_ffn_gelu_backward_regions;
        profile.metal_head_mlp_forward_regions = step_result.profile.metal_head_mlp_forward_regions;
        profile.metal_head_mlp_backward_regions = step_result.profile.metal_head_mlp_backward_regions;
        profile.metal_command_dot_general_dispatches = step_result.profile.metal_command_dot_general_dispatches;
        profile.metal_command_head_dot_dispatches = step_result.profile.metal_command_head_dot_dispatches;
        profile.metal_command_transpose_dispatches = step_result.profile.metal_command_transpose_dispatches;
        profile.metal_command_gather_dispatches = step_result.profile.metal_command_gather_dispatches;
        profile.metal_command_reduce_dispatches = step_result.profile.metal_command_reduce_dispatches;
        profile.metal_command_elementwise_dispatches = step_result.profile.metal_command_elementwise_dispatches;
        profile.metal_command_activation_dispatches = step_result.profile.metal_command_activation_dispatches;
        profile.metal_command_activation_backward_dispatches = step_result.profile.metal_command_activation_backward_dispatches;
        profile.metal_command_other_dispatches = step_result.profile.metal_command_other_dispatches;
        profile.graph_executor_runtime_frame_candidates = step_result.profile.graph_executor_runtime_frame_candidates;
        profile.graph_executor_runtime_frame_eligible = step_result.profile.graph_executor_runtime_frame_eligible;
        profile.graph_executor_runtime_frame_metadata_ready = step_result.profile.graph_executor_runtime_frame_metadata_ready;
        profile.graph_executor_runtime_frame_ineligible_no_regions = step_result.profile.graph_executor_runtime_frame_ineligible_no_regions;
        profile.graph_executor_runtime_frame_ineligible_missing_qkv = step_result.profile.graph_executor_runtime_frame_ineligible_missing_qkv;
        profile.graph_executor_runtime_frame_ineligible_missing_attention = step_result.profile.graph_executor_runtime_frame_ineligible_missing_attention;
        profile.graph_executor_runtime_frame_ineligible_missing_ffn = step_result.profile.graph_executor_runtime_frame_ineligible_missing_ffn;
        profile.graph_executor_runtime_frame_ineligible_missing_ple = step_result.profile.graph_executor_runtime_frame_ineligible_missing_ple;
        profile.graph_executor_runtime_frame_ineligible_single_row = step_result.profile.graph_executor_runtime_frame_ineligible_single_row;
        profile.graph_executor_runtime_frame_ineligible_non_layer_order = step_result.profile.graph_executor_runtime_frame_ineligible_non_layer_order;
        profile.graph_executor_runtime_frame_ineligible_shape_mismatch = step_result.profile.graph_executor_runtime_frame_ineligible_shape_mismatch;
        profile.graph_executor_runtime_frame_ineligible_missing_model_metadata = step_result.profile.graph_executor_runtime_frame_ineligible_missing_model_metadata;
        profile.graph_executor_plan_build_ns = step_result.profile.graph_executor_plan_build_ns;
        profile.graph_executor_buffer_plan_build_ns = step_result.profile.graph_executor_buffer_plan_build_ns;
        profile.graph_executor_plan_cache_hits = step_result.profile.graph_executor_plan_cache_hits;
        profile.graph_executor_plan_cache_misses = step_result.profile.graph_executor_plan_cache_misses;
        profile.metal_frame_wait_ns = step_result.profile.metal_frame_wait_ns;
        profile.metal_frame_gpu_ns = step_result.profile.metal_frame_gpu_ns;
        profile.metal_tensor_device_owned_live_bytes = step_result.profile.metal_tensor_device_owned_live_bytes;
        profile.metal_tensor_device_owned_peak_live_bytes = step_result.profile.metal_tensor_device_owned_peak_live_bytes;
        profile.metal_tensor_device_owned_peak_lt_256kb_bytes = step_result.profile.metal_tensor_device_owned_peak_lt_256kb_bytes;
        profile.metal_tensor_device_owned_peak_256kb_1mb_bytes = step_result.profile.metal_tensor_device_owned_peak_256kb_1mb_bytes;
        profile.metal_tensor_device_owned_peak_1mb_4mb_bytes = step_result.profile.metal_tensor_device_owned_peak_1mb_4mb_bytes;
        profile.metal_tensor_device_owned_peak_4mb_16mb_bytes = step_result.profile.metal_tensor_device_owned_peak_4mb_16mb_bytes;
        profile.metal_tensor_device_owned_peak_ge_16mb_bytes = step_result.profile.metal_tensor_device_owned_peak_ge_16mb_bytes;
        profile.metal_runtime_total_bytes = step_result.profile.metal_runtime_total_bytes;
        profile.metal_runtime_scratch_bytes = step_result.profile.metal_runtime_scratch_bytes;
        profile.metal_runtime_scratch_pool_bytes = step_result.profile.metal_runtime_scratch_pool_bytes;
        profile.metal_runtime_graph_plan_bytes = step_result.profile.metal_runtime_graph_plan_bytes;
        profile.metal_runtime_dense_linear_bytes = step_result.profile.metal_runtime_dense_linear_bytes;
        profile.metal_runtime_attention_span_bytes = step_result.profile.metal_runtime_attention_span_bytes;
        profile.metal_runtime_hidden_state_bytes = step_result.profile.metal_runtime_hidden_state_bytes;
        profile.metal_runtime_frame_retained_bytes = step_result.profile.metal_runtime_frame_retained_bytes;
        profile.metal_runtime_reuse_pool_bytes = step_result.profile.metal_runtime_reuse_pool_bytes;
        profile.metal_runtime_reuse_pool_slots = step_result.profile.metal_runtime_reuse_pool_slots;
        profile.metal_runtime_reuse_pool_peak_slots = step_result.profile.metal_runtime_reuse_pool_peak_slots;
        profile.metal_runtime_reuse_alloc_delta = step_result.profile.metal_runtime_reuse_alloc_delta;
        profile.metal_runtime_reuse_hit_delta = step_result.profile.metal_runtime_reuse_hit_delta;
        profile.metal_last_frame_compute_encoders = step_result.profile.metal_last_frame_compute_encoders;
        profile.metal_last_frame_blit_encoders = step_result.profile.metal_last_frame_blit_encoders;
        profile.metal_last_frame_planned_scopes = step_result.profile.metal_last_frame_planned_scopes;
        profile.metal_last_frame_planned_barriers = step_result.profile.metal_last_frame_planned_barriers;
        profile.metal_last_frame_planned_command_ops = step_result.profile.metal_last_frame_planned_command_ops;
        profile.metal_deberta_encoder_plan_attempts = step_result.profile.metal_deberta_encoder_plan_attempts;
        profile.metal_deberta_encoder_plan_successes = step_result.profile.metal_deberta_encoder_plan_successes;
        profile.metal_deberta_encoder_plan_reuses = step_result.profile.metal_deberta_encoder_plan_reuses;
        profile.metal_deberta_encoder_plan_failures = step_result.profile.metal_deberta_encoder_plan_failures;
        profile.metal_deberta_encoder_layer_attempts = step_result.profile.metal_deberta_encoder_layer_attempts;
        profile.metal_deberta_encoder_layer_successes = step_result.profile.metal_deberta_encoder_layer_successes;
        profile.metal_deberta_encoder_layer_fallbacks = step_result.profile.metal_deberta_encoder_layer_fallbacks;
        profile.metal_deberta_relative_qk_pair_calls = step_result.profile.metal_deberta_relative_qk_pair_calls;
        profile.metal_deberta_relative_qk_pair_fallbacks = step_result.profile.metal_deberta_relative_qk_pair_fallbacks;
        profile.metal_deberta_ffn_fused_calls = step_result.profile.metal_deberta_ffn_fused_calls;
        profile.metal_deberta_ffn_fused_mps_matmuls = step_result.profile.metal_deberta_ffn_fused_mps_matmuls;
        profile.metal_deberta_ffn_fused_fallbacks = step_result.profile.metal_deberta_ffn_fused_fallbacks;
        profile.metal_deberta_attention_flash_calls = step_result.profile.metal_deberta_attention_flash_calls;
        profile.metal_deberta_attention_gemm_calls = step_result.profile.metal_deberta_attention_gemm_calls;
        profile.metal_deberta_attention_gemm_fallbacks = step_result.profile.metal_deberta_attention_gemm_fallbacks;
        profile.metal_deberta_attention_legacy_calls = step_result.profile.metal_deberta_attention_legacy_calls;
        profile.cuda_device_allocations = step_result.profile.cuda_device_allocations;
        profile.cuda_device_frees = step_result.profile.cuda_device_frees;
        profile.cuda_h2d_bytes = step_result.profile.cuda_h2d_bytes;
        profile.cuda_d2h_bytes = step_result.profile.cuda_d2h_bytes;
        profile.cuda_largest_d2h_transfer_bytes = step_result.profile.cuda_largest_d2h_transfer_bytes;
        profile.cuda_to_float32_calls = step_result.profile.cuda_to_float32_calls;
        profile.cuda_download_alloc_calls = step_result.profile.cuda_download_alloc_calls;
        profile.cuda_stream_synchronizations = step_result.profile.cuda_stream_synchronizations;
        profile.cuda_upload_synchronizations = step_result.profile.cuda_upload_synchronizations;
        profile.cuda_temp_cache_hits = step_result.profile.cuda_temp_cache_hits;
        profile.cuda_temp_cache_misses = step_result.profile.cuda_temp_cache_misses;
        profile.cuda_kernel_launches = step_result.profile.cuda_kernel_launches;
        profile.cuda_packed_attention_forward_calls = step_result.profile.cuda_packed_attention_forward_calls;
        profile.cuda_packed_attention_backward_calls = step_result.profile.cuda_packed_attention_backward_calls;
        profile.cuda_exact_gelu_forward_calls = step_result.profile.cuda_exact_gelu_forward_calls;
        profile.cuda_exact_gelu_backward_calls = step_result.profile.cuda_exact_gelu_backward_calls;
        profile.cuda_training_input_uploads = step_result.profile.cuda_training_input_uploads;
        profile.cuda_training_input_upload_bytes = step_result.profile.cuda_training_input_upload_bytes;
        var step_result_live = true;
        defer if (step_result_live) step_result.deinit();
        const loss_value = step_result.loss;
        if (!std.math.isFinite(loss_value)) return error.NonFiniteTrainingLoss;

        var grad_norm: f32 = 0.0;
        var stepped = false;
        var learning_rate = self.config.lr_schedule.lr(@intCast(self.optimizer_step_count));
        const optimizer_update_start_ns = monotonicNowNs();
        switch (mode) {
            .train => {
                // 5. Accumulate gradients (mean over the accumulation window).
                const accum_steps: u32 = @max(self.config.grad_accum_steps, 1);
                const scale: f32 = 1.0 / @as(f32, @floatFromInt(accum_steps));
                const direct_device_step = use_device_optimizer and
                    (self.compute_backend.kind() == .metal or self.compute_backend.kind() == .cuda) and
                    accum_steps == 1 and
                    self.config.reduce_device_grads == null and
                    self.config.reduce_grads == null;
                if (!direct_device_step) {
                    for (self.lora_params.items) |*slot| {
                        if (use_device_optimizer) {
                            const g_ct = step_result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                            try self.accumulateDeviceGradientCt(slot, g_ct, scale, self.accum_count == 0);
                            continue;
                        }
                        const g = step_result.gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                        if (g.len != slot.grad_accum.len) return error.GradientShapeMismatch;
                        if (self.accum_count == 0) {
                            for (slot.grad_accum, g) |*a, v| a.* = v * scale;
                        } else {
                            for (slot.grad_accum, g) |*a, v| a.* += v * scale;
                        }
                    }
                    for (self.regular_params.items) |*slot| {
                        if (use_device_optimizer) {
                            const g_ct = step_result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                            try self.accumulateDeviceGradientCt(slot, g_ct, scale, self.accum_count == 0);
                            continue;
                        }
                        const g = step_result.gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                        if (g.len != slot.grad_accum.len) return error.GradientShapeMismatch;
                        if (self.accum_count == 0) {
                            for (slot.grad_accum, g) |*a, v| a.* = v * scale;
                        } else {
                            for (slot.grad_accum, g) |*a, v| a.* += v * scale;
                        }
                    }
                    self.accum_count += 1;
                } else {
                    self.accum_count = accum_steps;
                }

                // 6. If the accumulation window is full, clip + step the optimizer.
                if (self.accum_count >= accum_steps) {
                    if (!direct_device_step) try self.reduceAccumulatedGradients(use_device_optimizer);

                    learning_rate = self.config.lr_schedule.lr(@intCast(self.optimizer_step_count));
                    grad_norm = if (direct_device_step)
                        try self.deviceGlobalGradNormFromResult(&step_result)
                    else if (use_device_optimizer)
                        try self.deviceGlobalGradNorm()
                    else
                        self.globalGradNorm();
                    if (!std.math.isFinite(grad_norm)) return error.NonFiniteGradientNorm;
                    if (!use_device_optimizer and self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm) {
                        const clip = self.config.max_grad_norm / (grad_norm + 1e-6);
                        for (self.lora_params.items) |*slot| {
                            for (slot.grad_accum) |*v| v.* *= clip;
                        }
                        for (self.regular_params.items) |*slot| {
                            for (slot.grad_accum) |*v| v.* *= clip;
                        }
                    }

                    if (direct_device_step) {
                        const device_opt_start_ns = monotonicNowNs();
                        const clip_scale = if (self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm)
                            self.config.max_grad_norm / (grad_norm + 1e-6)
                        else
                            1.0;
                        try self.stepDeviceAdamWFromResult(&step_result, learning_rate, clip_scale);
                        profile.device_optimizer_ns = elapsedNs(device_opt_start_ns, monotonicNowNs());
                        profile.optimizer_backend = self.deviceOptimizerBackend();
                    } else if (use_device_optimizer) {
                        const device_opt_start_ns = monotonicNowNs();
                        const clip_scale = if (self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm)
                            self.config.max_grad_norm / (grad_norm + 1e-6)
                        else
                            1.0;
                        try self.stepDeviceAdamW(learning_rate, clip_scale);
                        profile.device_optimizer_ns = elapsedNs(device_opt_start_ns, monotonicNowNs());
                        profile.optimizer_backend = self.deviceOptimizerBackend();
                    } else {
                        const opt_config = optimizers.Optimizer{ .adamw = self.config.optimizer };
                        for (self.lora_params.items) |*slot| try self.stepHostAdamWSlot(opt_config, slot, learning_rate);
                        for (self.regular_params.items) |*slot| try self.stepHostAdamWSlot(opt_config, slot, learning_rate);
                    }
                    self.optimizer_step_count += 1;
                    self.accum_count = 0;
                    self.resetOptimizerFamilyWindow();
                    stepped = true;
                }
            },
            .eval => {
                grad_norm = try self.gradientNormFromResult(&step_result);
            },
        }
        profile.optimizer_update_ns = elapsedNs(optimizer_update_start_ns, monotonicNowNs());

        // 7. Release pinned LoRA blocks.
        if (lora_blocks_pinned) {
            try self.unpinAllLoraBlocks();
            lora_blocks_pinned = false;
        }

        if (mode == .train) self.step_count += 1;
        profile.device_resident_transfer_count = self.device_optimizer_transfers - device_optimizer_transfers_start;
        profile.device_trainable_bytes = self.device_trainable_bytes;
        // Device gradient outputs are step-local. Release them before the
        // final runtime snapshot so allocation/free telemetry covers the same
        // complete step boundary as transfer and synchronization telemetry.
        step_result.deinit();
        step_result_live = false;
        // Overwrite the executor-local CUDA counters with the complete step
        // delta, including cached input overwrites and the resident optimizer.
        training.recordTrainingRuntimeProfile(
            &profile,
            training_runtime_before,
            self.compute_backend.trainingRuntimeStats(),
        );
        profile.total_ns = elapsedNs(total_start_ns, monotonicNowNs());
        return .{
            .loss = loss_value,
            .grad_norm = grad_norm,
            .step = self.step_count,
            .optimizer_stepped = stepped,
            .learning_rate = learning_rate,
            .profile = profile,
        };
    }

    /// Save LoRA adapter weights to the given directory.
    ///
    /// Writes one `.bin` file per adapter parameter containing a tiny header
    /// plus raw f32 data. This is intentionally a dumb format — callers
    /// needing HF/safetensors compatibility should build on top of this.
    ///
    /// File layout (all little-endian):
    ///     "LORA"         (4 bytes magic)
    ///     ndims          (u32)
    ///     dims[ndims]    (i32 each)
    ///     n_elements     (u64)
    ///     raw f32 data   (n_elements * 4 bytes)
    pub fn saveAdapters(self: *const RealAutodiffTrainer, out_dir: []const u8) !void {
        try std.Io.Dir.cwd().createDirPath(compat.testingIo(), out_dir);
        // A reused output directory must describe exactly this run. Remove
        // only legacy autodiff adapter parameter files; preserve manifests,
        // configs, and unrelated user artifacts.
        var dir = try std.Io.Dir.cwd().openDir(compat.testingIo(), out_dir, .{ .iterate = true });
        defer dir.close(compat.testingIo());
        var iter = dir.iterate();
        while (try iter.next(compat.testingIo())) |entry| {
            if (entry.kind != .file) continue;
            if (std.mem.endsWith(u8, entry.name, ".lora_A.bin") or
                std.mem.endsWith(u8, entry.name, ".lora_B.bin") or
                std.mem.endsWith(u8, entry.name, ".lora_A.bin.tmp") or
                std.mem.endsWith(u8, entry.name, ".lora_B.bin.tmp"))
            {
                try dir.deleteFile(compat.testingIo(), entry.name);
            }
        }
        for (self.lora_params.items) |slot| {
            const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.bin", .{ out_dir, slot.name });
            defer self.allocator.free(file_name);
            const tmp_file_name = try std.fmt.allocPrint(self.allocator, "{s}.tmp", .{file_name});
            defer self.allocator.free(tmp_file_name);

            // Build the payload in memory then write in one call — keeps us
            // clear of the 0.16 streaming writer API differences.
            var buf = std.ArrayListUnmanaged(u8).empty;
            defer buf.deinit(self.allocator);

            try buf.appendSlice(self.allocator, "LORA");

            var u32_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &u32_buf, @intCast(slot.dims.len), .little);
            try buf.appendSlice(self.allocator, &u32_buf);

            for (slot.dims) |d| {
                var i32_buf: [4]u8 = undefined;
                std.mem.writeInt(i32, &i32_buf, d, .little);
                try buf.appendSlice(self.allocator, &i32_buf);
            }

            var u64_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &u64_buf, slot.weights.len, .little);
            try buf.appendSlice(self.allocator, &u64_buf);

            // Raw f32 data. Correct on little-endian hosts; big-endian hosts
            // (none in our target set) would need a per-element swap.
            try buf.appendSlice(self.allocator, std.mem.sliceAsBytes(slot.weights));

            std.Io.Dir.cwd().deleteFile(compat.testingIo(), tmp_file_name) catch {};
            errdefer std.Io.Dir.cwd().deleteFile(compat.testingIo(), tmp_file_name) catch {};
            try std.Io.Dir.cwd().writeFile(compat.testingIo(), .{
                .sub_path = tmp_file_name,
                .data = buf.items,
            });
            try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_file_name, std.Io.Dir.cwd(), file_name, compat.testingIo());
        }
    }

    pub fn syncDeviceTrainablesToHost(self: *RealAutodiffTrainer) !void {
        for (self.lora_params.items) |*slot| try self.syncDeviceSlotToHost(slot);
        for (self.regular_params.items) |*slot| try self.syncDeviceSlotToHost(slot);
    }

    fn appendTrainingStateSlots(
        self: *RealAutodiffTrainer,
        tensors: *std.ArrayListUnmanaged(safetensors_checkpoint.NamedTensor),
        names: *std.ArrayListUnmanaged([]u8),
        shapes: *std.ArrayListUnmanaged([]usize),
        owned_data: *std.ArrayListUnmanaged([]f32),
        slots: []const ParamSlot,
    ) !void {
        for (slots) |slot| {
            var weights: []const f32 = slot.weights;
            var moments: struct { m: []const f32, v: []const f32 } = undefined;
            if (slot.device) |device| {
                weights = try self.downloadTrainingStateTensor(owned_data, device.weight);
                moments = .{
                    .m = try self.downloadTrainingStateTensor(owned_data, device.m),
                    .v = try self.downloadTrainingStateTensor(owned_data, device.v),
                };
            } else {
                if (self.optimizer_state.param_states.get(slot.name)) |state| {
                    moments = .{ .m = state.m, .v = state.v };
                } else {
                    // Adam state is created lazily on the first host step.
                    // Conditional task-head families mirror torch grad=None:
                    // a family that never appeared has no optimizer state and
                    // a per-parameter step of zero. Persist explicit zero
                    // moments so the checkpoint remains self-contained and
                    // can create that state normally when it is restored.
                    if (slot.adam_step_count != 0) return error.MissingOptimizerState;
                    moments = .{
                        .m = try self.allocTrainingStateZeros(owned_data, slot.weights.len),
                        .v = try self.allocTrainingStateZeros(owned_data, slot.weights.len),
                    };
                }
            }
            if (weights.len != slot.weights.len or moments.m.len != slot.weights.len or moments.v.len != slot.weights.len) {
                return error.CheckpointSizeMismatch;
            }
            const adam_step = try self.allocTrainingStateScalar(owned_data, slot.adam_step_count);
            try appendTrainingStateTensor(self.allocator, tensors, names, shapes, "weight", slot.name, weights);
            try appendTrainingStateTensor(self.allocator, tensors, names, shapes, "adam_m", slot.name, moments.m);
            try appendTrainingStateTensor(self.allocator, tensors, names, shapes, "adam_v", slot.name, moments.v);
            try appendTrainingStateTensor(self.allocator, tensors, names, shapes, "adam_step", slot.name, adam_step);
        }
    }

    fn allocTrainingStateScalar(
        self: *RealAutodiffTrainer,
        owned_data: *std.ArrayListUnmanaged([]f32),
        value: u32,
    ) ![]f32 {
        const values = try self.allocator.alloc(f32, 1);
        errdefer self.allocator.free(values);
        values[0] = @floatFromInt(value);
        try owned_data.append(self.allocator, values);
        return values;
    }

    fn allocTrainingStateZeros(
        self: *RealAutodiffTrainer,
        owned_data: *std.ArrayListUnmanaged([]f32),
        len: usize,
    ) ![]f32 {
        const values = try self.allocator.alloc(f32, len);
        errdefer self.allocator.free(values);
        @memset(values, 0.0);
        try owned_data.append(self.allocator, values);
        return values;
    }

    fn downloadTrainingStateTensor(
        self: *RealAutodiffTrainer,
        owned_data: *std.ArrayListUnmanaged([]f32),
        tensor: CT,
    ) ![]f32 {
        const values = try self.compute_backend.toFloat32(tensor, self.allocator);
        errdefer self.allocator.free(values);
        try owned_data.append(self.allocator, values);
        self.device_optimizer_transfers += 1;
        return values;
    }

    fn loadTrainingStateSlots(
        self: *RealAutodiffTrainer,
        reader: *safetensors.MMapReader,
        slots: []ParamSlot,
        optimizer_steps: u64,
    ) !void {
        for (slots) |*slot| {
            try readTrainingStateTensor(self.allocator, reader, "weight", slot.name, slot.weights);
            const state = try self.optimizer_state.getOrCreate(slot.name, slot.weights.len, true);
            try readTrainingStateTensor(self.allocator, reader, "adam_m", slot.name, state.m);
            try readTrainingStateTensor(self.allocator, reader, "adam_v", slot.name, state.v);
            // Checkpoints written before per-slot Adam step counts existed only
            // carry the global optimizer step; that is the correct value for
            // every parameter that never sat out a step.
            const adam_step = try readTrainingStateStep(self.allocator, reader, slot.name) orelse @as(u32, @intCast(optimizer_steps));
            state.step_count = adam_step;
            slot.adam_step_count = adam_step;
        }
    }

    fn uploadRestoredOptimizerState(self: *RealAutodiffTrainer) !void {
        try self.ensureDeviceOptimizerSlots();
        for (self.lora_params.items) |*slot| try self.uploadRestoredOptimizerSlot(slot);
        for (self.regular_params.items) |*slot| try self.uploadRestoredOptimizerSlot(slot);
    }

    fn uploadRestoredOptimizerSlot(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        const state = self.optimizer_state.param_states.get(slot.name) orelse return error.MissingOptimizerState;
        const device = if (slot.device) |*value| value else return error.DeviceOptimizerNotInitialized;
        // `ensureDeviceOptimizerSlots` is intentionally idempotent and may
        // find an already-resident weight from before checkpoint loading.
        // Restore the checkpointed host value into that stable device buffer;
        // otherwise resumed CUDA training silently continues from the stale
        // pre-load weight while using the restored Adam moments.
        try self.compute_backend.trainingOverwriteF32(device.weight, slot.weights, slot.dims);
        const restored_m = try self.compute_backend.fromFloat32Shape(state.m, slot.dims);
        errdefer self.compute_backend.free(restored_m);
        const restored_v = try self.compute_backend.fromFloat32Shape(state.v, slot.dims);
        errdefer self.compute_backend.free(restored_v);
        self.compute_backend.free(device.m);
        self.compute_backend.free(device.v);
        device.m = restored_m;
        device.v = restored_v;
        self.device_optimizer_transfers += 3;
    }

    // ── Internals ────────────────────────────────────────────────────────

    fn deviceOptimizerRequested(self: *const RealAutodiffTrainer) bool {
        return switch (self.config.execution_engine) {
            .interpreter => false,
            .compiled_device => self.compute_backend.supportsDeviceTraining(),
            .compiled_metal => self.compute_backend.kind() == .metal and self.compute_backend.supportsDeviceTraining(),
        };
    }

    fn deviceOptimizerBackend(self: *const RealAutodiffTrainer) OptimizerBackend {
        return switch (self.compute_backend.kind()) {
            .metal => .metal,
            .cuda => .cuda,
            else => .host,
        };
    }

    fn putCachedRuntimeInput(
        self: *RealAutodiffTrainer,
        rt: *std.AutoHashMapUnmanaged(NodeId, CT),
        borrowed_rt: *std.AutoHashMapUnmanaged(NodeId, void),
        cache: *CachedRuntimeTensor,
        node_id: NodeId,
        data: []const f32,
        dims: []const i32,
    ) !void {
        const ct = try self.ensureCachedRuntimeTensor(cache, data, dims);
        try rt.put(self.allocator, node_id, ct);
        errdefer _ = rt.remove(node_id);
        try borrowed_rt.put(self.allocator, node_id, {});
    }

    fn ensureCachedRuntimeTensor(
        self: *RealAutodiffTrainer,
        cache: *CachedRuntimeTensor,
        data: []const f32,
        dims: []const i32,
    ) !CT {
        if (cache.tensor) |ct| {
            if (cache.elem_count == data.len and std.mem.eql(i32, cache.dims, dims)) {
                try self.compute_backend.trainingOverwriteF32(ct, data, dims);
                return ct;
            }
            self.compute_backend.free(ct);
            cache.tensor = null;
            if (cache.dims.len > 0) self.allocator.free(cache.dims);
            cache.dims = &.{};
            cache.elem_count = 0;
        }
        const ct = if (self.compute_backend.kind() == .cuda) blk: {
            // Allocate cached CUDA inputs without an unclassified H2D copy so
            // both initial population and subsequent overwrites contribute to
            // the dedicated runtime-input upload counters.
            const device_ct = try self.compute_backend.trainingZeroF32(data.len, dims);
            errdefer self.compute_backend.free(device_ct);
            try self.compute_backend.trainingOverwriteF32(device_ct, data, dims);
            break :blk device_ct;
        } else try self.compute_backend.fromFloat32Shape(data, dims);
        errdefer self.compute_backend.free(ct);
        cache.dims = try self.allocator.dupe(i32, dims);
        cache.elem_count = data.len;
        cache.tensor = ct;
        return ct;
    }

    fn deinitRuntimeInputCache(self: *RealAutodiffTrainer) void {
        self.deinitRuntimeInputCacheValue(&self.runtime_input_cache);
    }

    fn deinitRuntimeInputCacheValue(self: *RealAutodiffTrainer, cache: *RuntimeInputCache) void {
        self.deinitCachedRuntimeTensor(&cache.input_ids);
        self.deinitCachedRuntimeTensor(&cache.attention_mask);
        self.deinitCachedRuntimeTensor(&cache.targets);
        self.deinitCachedRuntimeTensor(&cache.gliner2_attn_bias);
    }

    fn deinitCachedRuntimeTensor(self: *RealAutodiffTrainer, cache: *CachedRuntimeTensor) void {
        if (cache.tensor) |ct| self.compute_backend.free(ct);
        if (cache.dims.len > 0) self.allocator.free(cache.dims);
        cache.* = .{};
    }

    fn putKnownCachedArchInputs(
        self: *RealAutodiffTrainer,
        rt: *std.AutoHashMapUnmanaged(NodeId, CT),
        borrowed_rt: *std.AutoHashMapUnmanaged(NodeId, void),
        graph: *const Graph,
        batch: u32,
        seq_len: u32,
        attention_mask: []const f32,
    ) !void {
        if (graph_weight_bridge.findParameterByName(graph, "__gliner2_attn_bias")) |node_id| {
            if (rt.contains(node_id)) return;
            const node = graph.node(node_id);
            if (node.output_shape.rank() != 3) return error.InvalidTensorShape;
            const dim0 = node.output_shape.dim(0);
            const dim1 = node.output_shape.dim(1);
            const dim2 = node.output_shape.dim(2);
            if (dim1 != seq_len or dim2 != seq_len) return error.InvalidTensorShape;
            const batch_i64: i64 = @intCast(batch);
            if (batch_i64 == 0 or @mod(dim0, batch_i64) != 0) return error.InvalidTensorShape;
            const num_heads: u32 = @intCast(@divExact(dim0, batch_i64));
            const bias = try graph_input_binder.BertPlaceholderPrep.buildAttnBias(
                self.allocator,
                attention_mask,
                batch,
                seq_len,
                num_heads,
            );
            defer self.allocator.free(bias);
            var dims = [_]i32{ @intCast(dim0), @intCast(dim1), @intCast(dim2) };
            try self.putCachedRuntimeInput(
                rt,
                borrowed_rt,
                &self.runtime_input_cache.gliner2_attn_bias,
                node_id,
                bias,
                &dims,
            );
        }
    }

    fn hasUnboundRuntimePlaceholders(
        self: *RealAutodiffTrainer,
        graph: *const Graph,
        rt: *const std.AutoHashMapUnmanaged(NodeId, CT),
    ) bool {
        _ = self;
        for (graph.parameters.items) |param_id| {
            const node = graph.node(param_id);
            if (node.op != .parameter) continue;
            const name = graph.parameterName(node);
            if (name.len < 2 or name[0] != '_' or name[1] != '_') continue;
            if (!rt.contains(param_id)) return true;
        }
        return false;
    }

    fn ensureDeviceOptimizerSlots(self: *RealAutodiffTrainer) !void {
        if (!self.compute_backend.supportsDeviceTraining()) return error.DeviceOptimizerBackendUnavailable;
        for (self.lora_params.items) |*slot| try self.ensureDeviceOptimizerSlot(slot);
        for (self.regular_params.items) |*slot| try self.ensureDeviceOptimizerSlot(slot);
    }

    fn ensureDeviceOptimizerSlot(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        if (slot.device != null) return;
        const weight = try self.compute_backend.fromFloat32Shape(slot.weights, slot.dims);
        errdefer self.compute_backend.free(weight);
        const grad_accum = try self.compute_backend.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(grad_accum);
        const m = try self.compute_backend.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(m);
        const v = try self.compute_backend.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(v);
        slot.device = .{
            .weight = weight,
            .grad_accum = grad_accum,
            .m = m,
            .v = v,
        };
        self.device_trainable_bytes += slot.weights.len * @sizeOf(f32) * 4;
    }

    fn deinitDeviceOptimizerSlot(self: *RealAutodiffTrainer, slot: *ParamSlot) void {
        if (slot.device) |device| {
            self.compute_backend.free(device.weight);
            self.compute_backend.free(device.grad_accum);
            self.compute_backend.free(device.m);
            self.compute_backend.free(device.v);
            slot.device = null;
        }
    }

    fn syncDeviceSlotToHost(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        const device = slot.device orelse return;
        const weights = try self.compute_backend.toFloat32(device.weight, self.allocator);
        defer self.allocator.free(weights);
        if (weights.len != slot.weights.len) return error.TrainableParameterShapeMismatch;
        @memcpy(slot.weights, weights);
        self.device_optimizer_transfers += 1;
    }

    fn accumulateDeviceGradientCt(
        self: *RealAutodiffTrainer,
        slot: *ParamSlot,
        grad_ct: CT,
        scale: f32,
        first: bool,
    ) !void {
        const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
        try self.compute_backend.trainingAccumulateF32(device.grad_accum, grad_ct, slot.weights.len, scale, first);
    }

    fn syncDeviceGradAccumToHost(self: *RealAutodiffTrainer) !void {
        for (self.lora_params.items) |*slot| try self.syncDeviceGradAccumSlotToHost(slot);
        for (self.regular_params.items) |*slot| try self.syncDeviceGradAccumSlotToHost(slot);
    }

    fn syncDeviceGradAccumSlotToHost(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        const device = slot.device orelse return;
        const grad = try self.compute_backend.toFloat32(device.grad_accum, self.allocator);
        defer self.allocator.free(grad);
        if (grad.len != slot.grad_accum.len) return error.GradientShapeMismatch;
        @memcpy(slot.grad_accum, grad);
        self.device_optimizer_transfers += 1;
    }

    fn replaceDeviceGradAccumFromHost(self: *RealAutodiffTrainer) !void {
        for (self.lora_params.items) |*slot| try self.replaceDeviceGradAccumSlotFromHost(slot);
        for (self.regular_params.items) |*slot| try self.replaceDeviceGradAccumSlotFromHost(slot);
    }

    fn replaceDeviceGradAccumSlotFromHost(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        const device = slot.device orelse return;
        const host_ct = try self.compute_backend.fromFloat32Shape(slot.grad_accum, slot.dims);
        defer self.compute_backend.free(host_ct);
        try self.compute_backend.trainingAccumulateF32(device.grad_accum, host_ct, slot.weights.len, 1.0, true);
        self.device_optimizer_transfers += 1;
    }

    fn reduceAccumulatedGradients(self: *RealAutodiffTrainer, use_device_optimizer: bool) !void {
        if (use_device_optimizer and self.config.reduce_device_grads != null) {
            try self.reduceDeviceGradAccum();
            return;
        }
        const reduce_fn = self.config.reduce_grads orelse return;
        if (use_device_optimizer) try self.syncDeviceGradAccumToHost();
        var blocks = try self.allocator.alloc(GradBlock, self.lora_params.items.len + self.regular_params.items.len);
        defer self.allocator.free(blocks);
        var block_idx: usize = 0;
        for (self.lora_params.items) |*slot| {
            blocks[block_idx] = .{ .name = slot.name, .data = slot.grad_accum };
            block_idx += 1;
        }
        for (self.regular_params.items) |*slot| {
            blocks[block_idx] = .{ .name = slot.name, .data = slot.grad_accum };
            block_idx += 1;
        }
        const ctx = self.config.reduce_grads_ctx orelse @as(*anyopaque, @ptrFromInt(@alignOf(usize)));
        try reduce_fn(ctx, blocks);
        if (use_device_optimizer) try self.replaceDeviceGradAccumFromHost();
    }

    fn reduceDeviceGradAccum(self: *RealAutodiffTrainer) !void {
        const reduce_fn = self.config.reduce_device_grads orelse return;
        var blocks = try self.allocator.alloc(DeviceGradBlock, self.lora_params.items.len + self.regular_params.items.len);
        defer self.allocator.free(blocks);
        var block_idx: usize = 0;
        for (self.lora_params.items) |*slot| {
            const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
            blocks[block_idx] = .{
                .name = slot.name,
                .data = device.grad_accum,
                .elem_count = slot.weights.len,
                .dims = slot.dims,
            };
            block_idx += 1;
        }
        for (self.regular_params.items) |*slot| {
            const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
            blocks[block_idx] = .{
                .name = slot.name,
                .data = device.grad_accum,
                .elem_count = slot.weights.len,
                .dims = slot.dims,
            };
            block_idx += 1;
        }
        const ctx = self.config.reduce_device_grads_ctx orelse @as(*anyopaque, @ptrFromInt(@alignOf(usize)));
        try reduce_fn(ctx, self.compute_backend, blocks);
    }

    fn deviceGlobalGradNorm(self: *RealAutodiffTrainer) !f32 {
        var inputs = try std.ArrayList(ops_mod.TrainingSumSquaresInput).initCapacity(self.allocator, self.lora_params.items.len + self.regular_params.items.len);
        defer inputs.deinit(self.allocator);
        for (self.lora_params.items) |*slot| {
            const device = slot.device orelse continue;
            try inputs.append(self.allocator, .{ .tensor = device.grad_accum, .elem_count = slot.weights.len });
        }
        for (self.regular_params.items) |*slot| {
            const device = slot.device orelse continue;
            try inputs.append(self.allocator, .{ .tensor = device.grad_accum, .elem_count = slot.weights.len });
        }
        return @sqrt(try self.compute_backend.trainingSumSquaresManyF32(inputs.items));
    }

    fn deviceGlobalGradNormFromResult(self: *RealAutodiffTrainer, result: *const training.TrainStepResult) !f32 {
        var inputs = try std.ArrayList(ops_mod.TrainingSumSquaresInput).initCapacity(self.allocator, self.lora_params.items.len + self.regular_params.items.len);
        defer inputs.deinit(self.allocator);
        for (self.lora_params.items) |*slot| {
            const grad = result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient;
            try inputs.append(self.allocator, .{ .tensor = grad, .elem_count = slot.weights.len });
        }
        for (self.regular_params.items) |*slot| {
            const grad = result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient;
            try inputs.append(self.allocator, .{ .tensor = grad, .elem_count = slot.weights.len });
        }
        const sumsq = try self.compute_backend.trainingSumSquaresManyF32(inputs.items);
        const kernel_norm: f32 = @sqrt(sumsq);
        self.debugCompareDeviceGradNorm(result, kernel_norm);
        return kernel_norm;
    }

    /// TERMITE_DEBUG_DEVICE_GRAD_NORM=1: once per process, recompute the
    /// global gradient norm on the HOST (each device gradient read back via
    /// `toFloat32`, the same view-aware path the lifetime diagnostic uses)
    /// and print it next to the Metal sum-squares kernel result. Decisively
    /// splits the "gradients nonzero but grad_norm=0" contradiction:
    /// kernel=0 + host>0 => the sum-squares kernel (or its readback /
    /// offsets) is broken; both 0 => zeros genuinely reached the trainer
    /// and the bug is upstream in extraction.
    fn debugCompareDeviceGradNorm(
        self: *RealAutodiffTrainer,
        result: *const training.TrainStepResult,
        kernel_norm: f32,
    ) void {
        if (device_grad_norm_debug_done) return;
        if (!platform.env.getenvBoolDefault("TERMITE_DEBUG_DEVICE_GRAD_NORM", false)) return;
        device_grad_norm_debug_done = true;
        var total: f64 = 0;
        var tensors: usize = 0;
        var nonzero_tensors: usize = 0;
        var printed: usize = 0;
        var it = result.device_gradients.iterator();
        while (it.next()) |entry| {
            tensors += 1;
            const data = self.compute_backend.toFloat32(entry.value_ptr.*, self.allocator) catch |err| {
                std.debug.print(
                    "[grad-norm-debug] {s}: host read failed: {s}\n",
                    .{ entry.key_ptr.*, @errorName(err) },
                );
                continue;
            };
            defer self.allocator.free(data);
            var tensor_sumsq: f64 = 0;
            for (data) |value| tensor_sumsq += @as(f64, value) * @as(f64, value);
            total += tensor_sumsq;
            if (tensor_sumsq > 0) {
                nonzero_tensors += 1;
                if (printed < 4) {
                    printed += 1;
                    std.debug.print(
                        "[grad-norm-debug] nonzero sample name={s} len={} host_sumsq={d:.9}\n",
                        .{ entry.key_ptr.*, data.len, tensor_sumsq },
                    );
                }
            }
        }
        std.debug.print(
            "[grad-norm-debug] kernel_norm={d:.9} host_norm={d:.9} tensors={} host_nonzero_tensors={} (kernel 0 + host >0 => sum-squares kernel/offset broken; both 0 => zeros reached the trainer)\n",
            .{ kernel_norm, @sqrt(total), tensors, nonzero_tensors },
        );
    }

    /// Where one device AdamW step reads each parameter's gradient from.
    pub const DeviceAdamWGradSource = union(enum) {
        /// The slot's own device accumulator, filled over the window.
        accumulated,
        /// Per-parameter gradients produced directly by a compiled step.
        step_result: *const training.TrainStepResult,
    };

    /// Build the device AdamW batch for one optimizer step and advance the
    /// Adam step count of every slot that takes part. Parameters whose
    /// conditional family never appeared in the accumulation window are left
    /// out entirely — upstream those modules keep `grad=None`, so
    /// `optimizer.step()` applies no update, no decoupled decay and no moment
    /// decay to them. Touches no device state, so the gating and the per-slot
    /// bias correction stay testable without a Metal device; the caller clears
    /// the excluded slots' accumulators.
    fn buildDeviceAdamWBatch(
        self: *RealAutodiffTrainer,
        grad_source: DeviceAdamWGradSource,
    ) ![]ops_mod.TrainingAdamWBatchInput {
        var batch = try std.ArrayList(ops_mod.TrainingAdamWBatchInput).initCapacity(
            self.allocator,
            self.lora_params.items.len + self.regular_params.items.len,
        );
        errdefer batch.deinit(self.allocator);
        try self.appendDeviceAdamWBatchSlots(&batch, self.lora_params.items, grad_source);
        try self.appendDeviceAdamWBatchSlots(&batch, self.regular_params.items, grad_source);
        return batch.toOwnedSlice(self.allocator);
    }

    fn appendDeviceAdamWBatchSlots(
        self: *RealAutodiffTrainer,
        batch: *std.ArrayList(ops_mod.TrainingAdamWBatchInput),
        slots: []ParamSlot,
        grad_source: DeviceAdamWGradSource,
    ) !void {
        const opt = self.config.optimizer;
        for (slots) |*slot| {
            if (self.optimizerFamilyAbsent(slot.name)) continue;
            const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
            const grad = switch (grad_source) {
                .accumulated => device.grad_accum,
                .step_result => |result| result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient,
            };
            const t: f32 = @floatFromInt(slot.adam_step_count + 1);
            batch.appendAssumeCapacity(.{
                .weight = device.weight,
                .grad = grad,
                .m = device.m,
                .v = device.v,
                .elem_count = slot.weights.len,
                .bias_correction1 = 1.0 - std.math.pow(f32, opt.beta1, t),
                .bias_correction2 = 1.0 - std.math.pow(f32, opt.beta2, t),
            });
        }
    }

    /// Advance the per-slot Adam step counters for exactly the slots the batch
    /// builder admitted. Kept separate from `buildDeviceAdamWBatch` so the
    /// counters — which are persisted into the checkpoint — can only move after
    /// the device work they describe has actually landed. Gating is a pure
    /// function of the accumulation window, which does not change in between.
    fn commitDeviceAdamWStepCounts(self: *RealAutodiffTrainer) void {
        for (self.lora_params.items) |*slot| {
            if (!self.optimizerFamilyAbsent(slot.name)) slot.adam_step_count += 1;
        }
        for (self.regular_params.items) |*slot| {
            if (!self.optimizerFamilyAbsent(slot.name)) slot.adam_step_count += 1;
        }
    }

    fn stepDeviceAdamWBatch(self: *RealAutodiffTrainer, grad_source: DeviceAdamWGradSource, lr: f32, grad_scale: f32) !void {
        const batch = try self.buildDeviceAdamWBatch(grad_source);
        defer self.allocator.free(batch);
        try self.zeroGatedDeviceGradAccum();
        if (batch.len == 0) return;
        const opt = self.config.optimizer;
        var frame_active = try self.compute_backend.decoderRuntimeBeginFrame();
        errdefer if (frame_active) self.compute_backend.decoderRuntimeCancelFrame() catch {};
        try self.compute_backend.trainingAdamWManyF32(batch, .{
            .lr = lr,
            .beta1 = opt.beta1,
            .beta2 = opt.beta2,
            .eps = opt.eps,
            .weight_decay = opt.weight_decay,
            .grad_scale = grad_scale,
        });
        try self.compute_backend.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
        try self.compute_backend.trainingSynchronize();
        self.commitDeviceAdamWStepCounts();
    }

    fn stepDeviceAdamW(self: *RealAutodiffTrainer, lr: f32, grad_scale: f32) !void {
        return self.stepDeviceAdamWFrom(.accumulated, lr, grad_scale);
    }

    fn stepDeviceAdamWFromResult(
        self: *RealAutodiffTrainer,
        result: *const training.TrainStepResult,
        lr: f32,
        grad_scale: f32,
    ) !void {
        return self.stepDeviceAdamWFrom(.{ .step_result = result }, lr, grad_scale);
    }

    fn stepDeviceAdamWFrom(self: *RealAutodiffTrainer, grad_source: DeviceAdamWGradSource, lr: f32, grad_scale: f32) !void {
        return self.stepDeviceAdamWBatch(grad_source, lr, grad_scale);
    }

    /// Single-slot form of the batched step, for device backends without a
    /// batched AdamW entry point. Applies the same conditional-family gate and
    /// the same per-slot bias correction.
    fn stepDeviceAdamWSlotGated(
        self: *RealAutodiffTrainer,
        slot: *ParamSlot,
        grad_source: DeviceAdamWGradSource,
        lr: f32,
        grad_scale: f32,
    ) !void {
        if (self.optimizerFamilyAbsent(slot.name)) return self.zeroDeviceGradAccum(slot);
        const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
        const grad = switch (grad_source) {
            .accumulated => device.grad_accum,
            .step_result => |result| result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient,
        };
        const opt = self.config.optimizer;
        slot.adam_step_count += 1;
        const t: f32 = @floatFromInt(slot.adam_step_count);
        try self.stepDeviceAdamWSlotWithGrad(
            slot,
            grad,
            lr,
            grad_scale,
            1.0 - std.math.pow(f32, opt.beta1, t),
            1.0 - std.math.pow(f32, opt.beta2, t),
        );
    }

    fn zeroGatedDeviceGradAccum(self: *RealAutodiffTrainer) !void {
        for (self.lora_params.items) |*slot| {
            if (self.optimizerFamilyAbsent(slot.name)) try self.zeroDeviceGradAccum(slot);
        }
        for (self.regular_params.items) |*slot| {
            if (self.optimizerFamilyAbsent(slot.name)) try self.zeroDeviceGradAccum(slot);
        }
    }

    /// Clear both halves of a slot's gradient accumulator. The device copy is
    /// rewritten in place by accumulating it into itself at scale 0, which is
    /// what `first` means to the accumulate kernel.
    fn zeroDeviceGradAccum(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        @memset(slot.grad_accum, 0);
        const device = slot.device orelse return;
        try self.compute_backend.trainingAccumulateF32(device.grad_accum, device.grad_accum, slot.weights.len, 0.0, true);
    }

    fn stepDeviceAdamWSlotWithGrad(
        self: *RealAutodiffTrainer,
        slot: *ParamSlot,
        grad: CT,
        lr: f32,
        grad_scale: f32,
        bias_correction1: f32,
        bias_correction2: f32,
    ) !void {
        const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
        const opt = self.config.optimizer;
        const batch = [_]ops_mod.TrainingAdamWBatchInput{.{
            .weight = device.weight,
            .grad = grad,
            .m = device.m,
            .v = device.v,
            .elem_count = slot.weights.len,
            .bias_correction1 = bias_correction1,
            .bias_correction2 = bias_correction2,
        }};
        try self.compute_backend.trainingAdamWManyF32(&batch, .{
            .lr = lr,
            .beta1 = opt.beta1,
            .beta2 = opt.beta2,
            .eps = opt.eps,
            .weight_decay = opt.weight_decay,
            .grad_scale = grad_scale,
        });
        @memset(slot.grad_accum, 0);
    }

    fn buildGraphState(self: *RealAutodiffTrainer, input: TrainerInput) !void {
        // 1. Build the bare forward graph.
        var base_graph = Graph.init(self.allocator);
        var base_graph_owned = true;
        errdefer if (base_graph_owned) base_graph.deinit();

        var bld = Builder.init(&base_graph);

        // Placeholders. Input IDs are bound at runtime as f32 (the user
        // forward callback typically gathers them into an embedding table,
        // which is happy to consume a flat float view).
        const ids_shape = Shape.init(.f32, &.{ @intCast(input.batch), @intCast(input.seq_len) });
        var input_ids_node = try bld.parameter("__input_ids", ids_shape);

        const mask_shape = Shape.init(.f32, &.{ @intCast(input.batch), @intCast(input.seq_len) });
        var attention_mask_node = try bld.parameter("__attention_mask", mask_shape);

        // Forward pass via callback.
        var forward_output = try input.build_forward(
            input.ctx,
            &bld,
            input_ids_node,
            attention_mask_node,
            input.batch,
            input.seq_len,
        );

        // Targets placeholder.
        var targets_node = try bld.parameter("__targets", input.targets_shape);

        // Loss.
        var loss_node = try input.build_loss(input.ctx, &bld, forward_output, targets_node);
        try base_graph.markOutput(loss_node);

        // 2. Inject LoRA. This clones the graph.
        var lora_result = try lora_mod.injectLoRA(self.allocator, &base_graph, self.config.lora);
        var lora_result_owned = true;
        // The original graph is no longer needed — LoRAResult owns a clone.
        base_graph.deinit();
        base_graph_owned = false;
        errdefer if (lora_result_owned) {
            lora_result.graph.deinit();
            lora_result.adapter.deinit();
        };

        // Placeholder nodes retain their IDs because injectLoRA only
        // *appends* new parameter / op nodes; it never reorders or renumbers
        // the pre-existing nodes. See the redirectConsumers scan in lora.zig
        // — original nodes in [0..node_count) are updated in place, not
        // moved.

        const sorted = try topologicallySortGraph(self.allocator, &lora_result.graph);
        lora_result.graph.deinit();
        lora_result.graph = sorted.graph;

        input_ids_node = sorted.id_map[input_ids_node];
        attention_mask_node = sorted.id_map[attention_mask_node];
        targets_node = sorted.id_map[targets_node];
        loss_node = sorted.id_map[loss_node];
        forward_output = sorted.id_map[forward_output];

        for (lora_result.adapter.adapters.items) |*info| {
            info.lora_a_id = sorted.id_map[info.lora_a_id];
            info.lora_b_id = sorted.id_map[info.lora_b_id];
            if (info.dropout_mask_id != null_node) info.dropout_mask_id = sorted.id_map[info.dropout_mask_id];
        }
        if (input.remap_graph_nodes) |remap| try remap(input.ctx, sorted.id_map);

        self.allocator.free(sorted.id_map);

        const initialize_slots = self.lora_params.items.len == 0 and self.regular_params.items.len == 0;
        if (initialize_slots) {
            // 3. Allocate trainer-global parameter/optimizer inputs once.
            var rng = std.Random.DefaultPrng.init(self.config.seed);
            var rnd = rng.random();

            for (lora_result.adapter.adapters.items) |info| {
                const a_node = lora_result.graph.node(info.lora_a_id);
                const b_node = lora_result.graph.node(info.lora_b_id);
                try self.appendParamSlot(info.lora_a_name, info.lora_a_id, a_node.output_shape, true, &rnd);
                try self.appendParamSlot(info.lora_b_name, info.lora_b_id, b_node.output_shape, false, &rnd);
            }
            for (self.config.regular_trainable_params) |name| {
                const node_id = findParameterByName(&lora_result.graph, name) orelse return error.TrainableParameterNotFound;
                const node = lora_result.graph.node(node_id);
                try self.appendRegularParamSlotFromBackend(name, node_id, node.output_shape);
            }
        } else if (self.lora_params.items.len != lora_result.adapter.adapters.items.len * 2 or
            self.regular_params.items.len != self.config.regular_trainable_params.len)
        {
            return error.GraphTrainableSetMismatch;
        }

        self.graph_state = .{
            .graph = lora_result.graph,
            .input_ids_node = input_ids_node,
            .attention_mask_node = attention_mask_node,
            .targets_node = targets_node,
            .loss_node = loss_node,
            .forward_output_node = forward_output,
            .lora_adapter = lora_result.adapter,
        };
        lora_result_owned = false;
        errdefer self.deinitActiveGraph();
        try self.bindParamSlotsToActiveGraph();

        // 4. Register residency blocks only once; graph-cache entries share them.
        if (initialize_slots and self.coord != null) {
            const coord = self.coord.?;
            for (self.lora_params.items, 0..) |*slot, i| {
                const id = grad_residency.GradBlockId{
                    .layer_idx = @intCast(i / 2),
                    .module_idx = @intCast(i % 2),
                };
                const bytes: u64 = @intCast(slot.weights.len * @sizeOf(f32));
                coord.registerGradBlock(id, bytes) catch |err| switch (err) {
                    // Budget denied is not fatal here — we simply won't
                    // track this block. The training step itself still works.
                    error.BudgetDenied => continue,
                    else => return err,
                };
                slot.block_id = id;
                slot.block_registered = true;
            }
        }
    }

    fn bindParamSlotsToActiveGraph(self: *RealAutodiffTrainer) !void {
        const graph = &(self.graph_state orelse return error.GraphNotBuilt).graph;
        for (self.lora_params.items) |*slot| try bindParamSlotToGraph(graph, slot);
        for (self.regular_params.items) |*slot| try bindParamSlotToGraph(graph, slot);
    }

    fn bindParamSlotToGraph(graph: *const Graph, slot: *ParamSlot) !void {
        const node_id = findParameterByName(graph, slot.name) orelse return error.TrainableParameterNotFound;
        const shape = graph.node(node_id).output_shape;
        if (shape.rank() != slot.dims.len) return error.TrainableParameterShapeMismatch;
        for (slot.dims, 0..) |dim, axis| {
            if (shape.dim(@intCast(axis)) != dim) return error.TrainableParameterShapeMismatch;
        }
        slot.node_id = node_id;
    }

    fn appendParamSlot(
        self: *RealAutodiffTrainer,
        name: []const u8,
        node_id: NodeId,
        shape: Shape,
        init_random: bool,
        rnd: *std.Random,
    ) !void {
        const n_elems: usize = @intCast(shape.numElements() orelse return error.DynamicShapeNotAllowed);

        const weights = try self.allocator.alloc(f32, n_elems);
        errdefer self.allocator.free(weights);
        if (init_random) {
            if (self.config.lora_a_init_std) |std_dev| {
                for (weights) |*w| w.* = rnd.floatNorm(f32) * std_dev;
            } else {
                if (shape.rank() < 2) return error.InvalidLoRAAdapterShape;
                const fan_in = @as(f32, @floatFromInt(shape.dim(1)));
                if (fan_in <= 0.0) return error.InvalidLoRAAdapterShape;
                const bound = 1.0 / @sqrt(fan_in);
                for (weights) |*w| w.* = (rnd.float(f32) * 2.0 - 1.0) * bound;
            }
        } else {
            @memset(weights, 0.0);
        }

        const grad_accum = try self.allocator.alloc(f32, n_elems);
        errdefer self.allocator.free(grad_accum);
        @memset(grad_accum, 0.0);

        const rank = shape.rank();
        const dims = try self.allocator.alloc(i32, rank);
        errdefer self.allocator.free(dims);
        for (0..rank) |i| dims[i] = @intCast(shape.dim(@intCast(i)));

        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);

        try self.lora_params.append(self.allocator, .{
            .name = owned_name,
            .weights = weights,
            .grad_accum = grad_accum,
            .node_id = node_id,
            .dims = dims,
        });
    }

    fn appendRegularParamSlotFromBackend(
        self: *RealAutodiffTrainer,
        name: []const u8,
        node_id: NodeId,
        shape: Shape,
    ) !void {
        if (isLoraParamName(name)) return error.InvalidRegularTrainableParameter;
        for (self.lora_params.items) |slot| {
            if (std.mem.eql(u8, slot.name, name)) return error.DuplicateTrainableParameter;
        }
        for (self.regular_params.items) |slot| {
            if (std.mem.eql(u8, slot.name, name)) return error.DuplicateTrainableParameter;
        }

        const n_elems: usize = @intCast(shape.numElements() orelse return error.DynamicShapeNotAllowed);
        const ct = try self.compute_backend.getWeight(name);
        defer self.compute_backend.free(ct);
        const weights = try self.compute_backend.toFloat32(ct, self.allocator);
        errdefer self.allocator.free(weights);
        if (weights.len != n_elems) return error.TrainableParameterShapeMismatch;

        const grad_accum = try self.allocator.alloc(f32, n_elems);
        errdefer self.allocator.free(grad_accum);
        @memset(grad_accum, 0.0);

        const rank = shape.rank();
        const dims = try self.allocator.alloc(i32, rank);
        errdefer self.allocator.free(dims);
        for (0..rank) |i| dims[i] = @intCast(shape.dim(@intCast(i)));

        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);

        try self.regular_params.append(self.allocator, .{
            .name = owned_name,
            .weights = weights,
            .grad_accum = grad_accum,
            .node_id = node_id,
            .dims = dims,
        });
    }

    fn reserveActivationBudget(self: *RealAutodiffTrainer, input: TrainerInput) !void {
        const coord = self.coord orelse return;
        _ = coord;
        // Rough estimate: batch * seq * hidden * 4 bytes * num_layers * 2
        // (forward + backward). This is advisory only — the coordinator's
        // budget machinery is the ground truth. We invoke touchGradBlock on
        // each LoRA block so the residency tracker sees recent activity.
        const hidden: u64 = if (self.config.hidden_size_hint > 0) self.config.hidden_size_hint else 768;
        const layers: u64 = if (self.config.num_layers_hint > 0) self.config.num_layers_hint else 12;
        const est: u64 = @as(u64, input.batch) * @as(u64, input.seq_len) * hidden * 4 * layers * 2;
        _ = est; // Reserved for future fine-grained reservation.
    }

    fn pinAllLoraBlocks(self: *RealAutodiffTrainer) !void {
        const coord = self.coord orelse return;
        for (self.lora_params.items) |slot| {
            if (!slot.block_registered) continue;
            const id = slot.block_id orelse continue;
            try coord.pinGradBlock(id);
        }
    }

    fn unpinAllLoraBlocks(self: *RealAutodiffTrainer) !void {
        const coord = self.coord orelse return;
        for (self.lora_params.items) |slot| {
            if (!slot.block_registered) continue;
            const id = slot.block_id orelse continue;
            try coord.unpinGradBlock(id);
        }
    }

    fn globalGradNorm(self: *const RealAutodiffTrainer) f32 {
        var total: f64 = 0.0;
        for (self.lora_params.items) |slot| {
            for (slot.grad_accum) |g| total += @as(f64, g) * @as(f64, g);
        }
        for (self.regular_params.items) |slot| {
            for (slot.grad_accum) |g| total += @as(f64, g) * @as(f64, g);
        }
        return @floatCast(@sqrt(total));
    }

    fn gradientNormFromResult(self: *const RealAutodiffTrainer, step_result: *const training.TrainStepResult) !f32 {
        var total: f64 = 0.0;
        for (self.lora_params.items) |slot| {
            const g = step_result.gradients.get(slot.name) orelse continue;
            for (g) |value| total += @as(f64, value) * @as(f64, value);
        }
        for (self.regular_params.items) |slot| {
            const g = step_result.gradients.get(slot.name) orelse continue;
            for (g) |value| total += @as(f64, value) * @as(f64, value);
        }
        return @floatCast(@sqrt(total));
    }
};

// ── Helpers ──────────────────────────────────────────────────────────────────

pub const RestoredTrainingStateCounters = struct {
    micro_batch_steps: u64,
    optimizer_steps: u64,
};

fn encodeTrainingStateCounters(micro_batch_steps: u64, optimizer_steps: u64) [8]f32 {
    var values: [8]f32 = undefined;
    for (0..4) |idx| {
        const shift: u6 = @intCast(idx * 16);
        values[idx] = @floatFromInt((micro_batch_steps >> shift) & 0xffff);
        values[idx + 4] = @floatFromInt((optimizer_steps >> shift) & 0xffff);
    }
    return values;
}

fn decodeTrainingStateCounters(values: []const f32) !RestoredTrainingStateCounters {
    if (values.len != 8) return error.InvalidTrainingStateCounters;
    var result = RestoredTrainingStateCounters{ .micro_batch_steps = 0, .optimizer_steps = 0 };
    for (values, 0..) |value, idx| {
        if (!std.math.isFinite(value) or value < 0 or value > 65535 or value != @floor(value)) {
            return error.InvalidTrainingStateCounters;
        }
        const part: u64 = @intFromFloat(value);
        const shift: u6 = @intCast((idx % 4) * 16);
        if (idx < 4) {
            result.micro_batch_steps |= part << shift;
        } else {
            result.optimizer_steps |= part << shift;
        }
    }
    return result;
}

fn validateTrainingStateFingerprint(reader: *safetensors.MMapReader, expected: *const [32]u8) !void {
    return validateTrainingStateDigest(reader, "__run_fingerprint", expected, error.TrainingStateFingerprintMismatch);
}

fn validateTrainingStateDigest(
    reader: *safetensors.MMapReader,
    name: []const u8,
    expected: *const [32]u8,
    mismatch: anyerror,
) !void {
    var tensor = reader.readTensor(name) catch return mismatch;
    defer tensor.deinit();
    if (tensor.dtype != .f32) return error.InvalidTrainingStateDType;
    const values = tensor.asFloat32();
    if (values.len != expected.len) return mismatch;
    for (values, expected) |value, byte| {
        if (!std.math.isFinite(value) or value < 0 or value > 255 or value != @floor(value) or @as(u8, @intFromFloat(value)) != byte) {
            return mismatch;
        }
    }
}

fn syncParentDirectory(path: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return;
    const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
    var dir = if (std.fs.path.isAbsolute(parent))
        try std.Io.Dir.openDirAbsolute(compat.testingIo(), parent, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(compat.testingIo(), parent, .{ .iterate = true });
    defer dir.close(compat.testingIo());
    while (true) switch (std.posix.errno(std.posix.system.fsync(dir.handle))) {
        .SUCCESS => return,
        .INTR => continue,
        .INVAL => return,
        .BADF => return error.InvalidFileDescriptor,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return std.posix.unexpectedErrno(err),
    };
}

fn appendTrainingStateTensor(
    allocator: std.mem.Allocator,
    tensors: *std.ArrayListUnmanaged(safetensors_checkpoint.NamedTensor),
    names: *std.ArrayListUnmanaged([]u8),
    shapes: *std.ArrayListUnmanaged([]usize),
    prefix: []const u8,
    parameter_name: []const u8,
    data: []const f32,
) !void {
    const names_len = names.items.len;
    const shapes_len = shapes.items.len;
    errdefer {
        while (names.items.len > names_len) allocator.free(names.pop().?);
        while (shapes.items.len > shapes_len) allocator.free(shapes.pop().?);
    }
    const name = try std.fmt.allocPrint(allocator, "{s}::{s}", .{ prefix, parameter_name });
    try names.append(allocator, name);
    const shape = try allocator.alloc(usize, 1);
    shape[0] = data.len;
    try shapes.append(allocator, shape);
    try tensors.append(allocator, .{ .name = name, .data = data, .shape = shape });
}

fn readTrainingStateTensor(
    allocator: std.mem.Allocator,
    reader: *safetensors.MMapReader,
    prefix: []const u8,
    parameter_name: []const u8,
    destination: []f32,
) !void {
    const name = try std.fmt.allocPrint(allocator, "{s}::{s}", .{ prefix, parameter_name });
    defer allocator.free(name);
    var tensor = try reader.readTensor(name);
    defer tensor.deinit();
    if (tensor.dtype != .f32) return error.InvalidTrainingStateDType;
    const source = tensor.asFloat32();
    if (source.len != destination.len) return error.CheckpointSizeMismatch;
    for (source) |value| if (!std.math.isFinite(value)) return error.NonFiniteCheckpointState;
    @memcpy(destination, source);
}

/// Read a parameter's persisted Adam step count, or null when the checkpoint
/// predates the per-slot counter.
fn readTrainingStateStep(
    allocator: std.mem.Allocator,
    reader: *safetensors.MMapReader,
    parameter_name: []const u8,
) !?u32 {
    const name = try std.fmt.allocPrint(allocator, "adam_step::{s}", .{parameter_name});
    defer allocator.free(name);
    var tensor = reader.readTensor(name) catch |err| switch (err) {
        error.TensorNotFound => return null,
        else => return err,
    };
    defer tensor.deinit();
    if (tensor.dtype != .f32) return error.InvalidTrainingStateDType;
    const source = tensor.asFloat32();
    if (source.len != 1) return error.CheckpointSizeMismatch;
    // f32 only represents integers exactly below 2^24, and no real run gets
    // near that many optimizer steps, so a larger value is corruption.
    const max_exact_step: f32 = std.math.maxInt(u24);
    const value = source[0];
    if (!std.math.isFinite(value) or value < 0 or value != @floor(value) or value > max_exact_step) {
        return error.InvalidTrainingStateCounters;
    }
    return @intFromFloat(value);
}

fn putRuntimeInput(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    rt: *std.AutoHashMapUnmanaged(NodeId, CT),
    id: NodeId,
    data: []const f32,
    dims: []const i32,
) !void {
    const ct = try cb.fromFloat32Shape(data, dims);
    errdefer cb.free(ct);
    try rt.put(allocator, id, ct);
}

fn shapeToDims(allocator: std.mem.Allocator, shape: Shape) ![]i32 {
    const rank = shape.rank();
    const dims = try allocator.alloc(i32, rank);
    for (0..rank) |i| dims[i] = @intCast(shape.dim(@intCast(i)));
    return dims;
}

/// One-shot latch for `debugCompareDeviceGradNorm` (env-gated diagnostic).
var device_grad_norm_debug_done: bool = false;

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn elapsedNs(start_ns: u64, end_ns: u64) u64 {
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn currentResidentBytes() usize {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;
    const maxrss: usize = @intCast(usage.maxrss);
    return switch (@import("builtin").os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}

fn compiledDiag(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBoolDefault("TERMITE_COMPILED_TRAIN_TRACE", false)) return;
    std.debug.print("[real-autodiff] " ++ fmt ++ "\n", args);
}

fn findParameterByName(graph: *const Graph, name: []const u8) ?NodeId {
    for (graph.parameters.items) |param_id| {
        const node = graph.node(param_id);
        if (node.op != .parameter) continue;
        if (std.mem.eql(u8, graph.parameterName(node), name)) return param_id;
    }
    return null;
}

fn isLoraParamName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, ".lora_A") != null or
        std.mem.indexOf(u8, name, ".lora_B") != null;
}

const TopologicalSortResult = struct {
    graph: Graph,
    id_map: []NodeId,
};

fn topologicallySortGraph(allocator: std.mem.Allocator, graph: *const Graph) !TopologicalSortResult {
    const count = graph.nodeCount();
    const in_degree = try allocator.alloc(u32, count);
    defer allocator.free(in_degree);
    @memset(in_degree, 0);

    for (0..count) |i| {
        const node = graph.node(@intCast(i));
        for (node.getInputs()) |input_id| {
            if (input_id != null_node) in_degree[i] += 1;
        }
    }

    var queue = std.ArrayListUnmanaged(NodeId).empty;
    defer queue.deinit(allocator);
    for (0..count) |i| {
        if (in_degree[i] == 0) try queue.append(allocator, @intCast(i));
    }

    const id_map = try allocator.alloc(NodeId, count);
    errdefer allocator.free(id_map);
    @memset(id_map, null_node);

    var sorted_nodes = try std.ArrayListUnmanaged(ml.graph.Node).initCapacity(allocator, count);
    errdefer sorted_nodes.deinit(allocator);

    var head: usize = 0;
    while (head < queue.items.len) {
        const old_id = queue.items[head];
        head += 1;

        id_map[old_id] = @intCast(sorted_nodes.items.len);
        sorted_nodes.appendAssumeCapacity(graph.node(old_id).*);

        for (0..count) |j| {
            const node = graph.node(@intCast(j));
            for (node.getInputs()) |input_id| {
                if (input_id == old_id) {
                    in_degree[j] -= 1;
                    if (in_degree[j] == 0) try queue.append(allocator, @intCast(j));
                }
            }
        }
    }

    if (sorted_nodes.items.len != count) return error.CycleDetected;

    for (sorted_nodes.items) |*node| {
        for (&node.inputs, 0..) |*input_id, j| {
            if (j >= node.num_inputs) break;
            if (input_id.* != null_node) input_id.* = id_map[input_id.*];
        }
        if (node.vjp_alternate != null_node) node.vjp_alternate = id_map[node.vjp_alternate];
    }

    var sorted_graph = Graph.init(allocator);
    errdefer sorted_graph.deinit();
    try sorted_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try sorted_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);
    try sorted_graph.nodes.appendSlice(allocator, sorted_nodes.items);
    for (graph.outputs.items) |output_id| try sorted_graph.outputs.append(allocator, id_map[output_id]);
    for (graph.parameters.items) |parameter_id| {
        if (id_map[parameter_id] != null_node) try sorted_graph.parameters.append(allocator, id_map[parameter_id]);
    }
    sorted_nodes.deinit(allocator);

    return .{
        .graph = sorted_graph,
        .id_map = id_map,
    };
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Minimal forward graph used in the unit test: a single frozen linear
/// layer + a ReLU. LoRA will get injected into the `test.linear.weight`
/// parameter because the pattern matches.
const TestCtx = struct {
    hidden: u32,
    active_seq_len: u32 = 0,

    fn buildForward(
        ctx: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        const self: *TestCtx = @ptrCast(@alignCast(ctx));
        _ = attention_mask;
        self.active_seq_len = seq_len;

        const rows: u32 = batch * seq_len;
        const hidden = self.hidden;

        // The `input_ids` placeholder is [batch, seq_len] f32. Reshape to
        // [rows, 1] and embed it into a [rows, hidden] tensor via a
        // (frozen) linear layer.
        const flat_shape = Shape.init(.f32, &.{ @intCast(rows), 1 });
        const flat = try bld.reshape(input_ids, flat_shape);

        // Frozen linear weight: [hidden, 1]
        const weight = try bld.parameter(
            "test.linear.weight",
            Shape.init(.f32, &.{ @intCast(hidden), 1 }),
        );
        const linear = try bld.linearNoBias(flat, weight, rows, 1, hidden);
        return try bld.relu(linear);
    }

    fn buildLoss(
        ctx: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        _ = ctx;
        return try bld.mseLoss(forward_output, targets);
    }

    fn captureGraphContext(ctx: *anyopaque, allocator: std.mem.Allocator) !?*anyopaque {
        const self: *TestCtx = @ptrCast(@alignCast(ctx));
        const state = try allocator.create(u32);
        state.* = self.active_seq_len;
        return @ptrCast(state);
    }

    fn restoreGraphContext(ctx: *anyopaque, state: ?*anyopaque) !void {
        const self: *TestCtx = @ptrCast(@alignCast(ctx));
        const value: *u32 = @ptrCast(@alignCast(state orelse return error.MissingGraphContextState));
        self.active_seq_len = value.*;
    }

    fn deinitGraphContext(state: ?*anyopaque, allocator: std.mem.Allocator) void {
        const value: *u32 = @ptrCast(@alignCast(state orelse return));
        allocator.destroy(value);
    }
};

test "RealAutodiffTrainer: graph construction injects LoRA adapters" {
    const allocator = testing.allocator;

    // Build the post-injection graph directly so we can inspect the
    // adapter list without needing a live ComputeBackend. This exercises
    // the same buildGraphState pre-conditions the trainer would hit.
    var g = Graph.init(allocator);
    defer g.deinit();

    var bld = Builder.init(&g);
    var ctx = TestCtx{ .hidden = 8 };

    const batch: u32 = 1;
    const seq_len: u32 = 4;

    const ids_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const input_ids_node = try bld.parameter("__input_ids", ids_shape);
    const mask_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const mask_node = try bld.parameter("__attention_mask", mask_shape);

    const fwd = try TestCtx.buildForward(@ptrCast(&ctx), &bld, input_ids_node, mask_node, batch, seq_len);

    const target_shape = Shape.init(.f32, &.{ @intCast(batch * seq_len), @intCast(ctx.hidden) });
    const targets_node = try bld.parameter("__targets", target_shape);
    const loss_node = try TestCtx.buildLoss(@ptrCast(&ctx), &bld, fwd, targets_node);
    try g.markOutput(loss_node);

    var lora_result = try lora_mod.injectLoRA(allocator, &g, .{
        .rank = 2,
        .alpha = 2.0,
        .target_patterns = &.{"linear.weight"},
    });
    defer lora_result.deinit();

    // Expect exactly one adapter pair for `test.linear.weight`.
    try testing.expectEqual(@as(usize, 1), lora_result.adapter.adapters.items.len);
    const info = lora_result.adapter.adapters.items[0];
    try testing.expectEqualStrings("test.linear.weight", info.base_name);

    // Autodiff should succeed on the injected graph w.r.t. the two LoRA
    // parameters. (We do not execute; the interpreter needs a backend.)
    var grad_result = try autodiff.gradient(
        allocator,
        &lora_result.graph,
        loss_node,
        &.{ info.lora_a_id, info.lora_b_id },
    );
    defer grad_result.deinit();

    try testing.expectEqual(@as(usize, 2), grad_result.param_grads.len);
    try testing.expect(grad_result.param_grads[0] != null_node);
    try testing.expect(grad_result.param_grads[1] != null_node);
}

test "RealAutodiffTrainer: bounded shape cache shares trainables and isolates compiled/runtime state" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    var ctx = TestCtx{ .hidden = 8 };
    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{ .rank = 2, .alpha = 2.0, .target_patterns = &.{"linear.weight"} },
        .graph_cache_capacity = 2,
    });
    defer trainer.deinit();

    var ids = @as([8]i64, @splat(0));
    var mask = @as([8]f32, @splat(1.0));
    var targets = @as([(8 * 8)]f32, @splat(0.0));
    const input_a = TrainerInput{
        .ctx = @ptrCast(&ctx),
        .build_forward = &TestCtx.buildForward,
        .build_loss = &TestCtx.buildLoss,
        .input_ids = ids[0..2],
        .attention_mask = mask[0..2],
        .targets = targets[0 .. 2 * ctx.hidden],
        .targets_shape = Shape.init(.f32, &.{ 2, ctx.hidden }),
        .batch = 1,
        .seq_len = 2,
        .capture_graph_context = &TestCtx.captureGraphContext,
        .restore_graph_context = &TestCtx.restoreGraphContext,
        .deinit_graph_context = &TestCtx.deinitGraphContext,
        .graph_variant = .{ 1, 0, 0, 0 },
    };
    try trainer.ensureGraphBuilt(input_a);
    try testing.expectEqual(@as(usize, 2), trainer.lora_params.items.len);
    trainer.lora_params.items[0].weights[0] = 3.25;
    trainer.step_count = 7;
    trainer.optimizer_step_count = 3;

    const trainable = [_][]const u8{
        trainer.lora_params.items[0].name,
        trainer.lora_params.items[1].name,
    };
    const gs_a = &trainer.graph_state.?;
    trainer.compiled_session = try training.CompiledTrainSession.init(
        allocator,
        &gs_a.graph,
        gs_a.loss_node,
        .{ .trainable_params = &trainable },
    );
    trainer.runtime_input_cache.input_ids.dims = try allocator.dupe(i32, &.{ 1, 2 });
    trainer.runtime_input_cache.input_ids.elem_count = 2;

    const input_b = TrainerInput{
        .ctx = @ptrCast(&ctx),
        .build_forward = &TestCtx.buildForward,
        .build_loss = &TestCtx.buildLoss,
        .input_ids = ids[0..4],
        .attention_mask = mask[0..4],
        .targets = targets[0 .. 4 * ctx.hidden],
        .targets_shape = Shape.init(.f32, &.{ 4, ctx.hidden }),
        .batch = 1,
        .seq_len = 4,
        .capture_graph_context = &TestCtx.captureGraphContext,
        .restore_graph_context = &TestCtx.restoreGraphContext,
        .deinit_graph_context = &TestCtx.deinitGraphContext,
        .graph_variant = .{ 2, 0, 0, 0 },
    };
    try trainer.ensureGraphBuilt(input_b);
    try testing.expectEqual(@as(usize, 1), trainer.inactive_graphs.items.len);
    try testing.expect(trainer.compiled_session == null);
    try testing.expectEqual(@as(usize, 0), trainer.runtime_input_cache.input_ids.dims.len);
    try testing.expectEqual(@as(f32, 3.25), trainer.lora_params.items[0].weights[0]);
    try testing.expectEqual(@as(u64, 7), trainer.step_count);
    try testing.expectEqual(@as(u64, 3), trainer.optimizer_step_count);
    try testing.expect(findParameterByName(&trainer.graph_state.?.graph, trainer.lora_params.items[0].name) == trainer.lora_params.items[0].node_id);

    trainer.runtime_input_cache.input_ids.dims = try allocator.dupe(i32, &.{ 1, 4 });
    trainer.runtime_input_cache.input_ids.elem_count = 4;
    try trainer.ensureGraphBuilt(input_a);
    try testing.expect(trainer.compiled_session != null);
    try testing.expectEqual(@as(usize, 0), trainer.runtime_input_cache.input_ids.dims.len);
    try testing.expectEqual(@as(u32, 2), ctx.active_seq_len);
    try testing.expectEqual(@as(f32, 3.25), trainer.lora_params.items[0].weights[0]);
    try testing.expectEqualStrings("test.linear.weight.lora_A", trainer.lora_params.items[0].name);

    try trainer.ensureGraphBuilt(input_b);
    const input_c = TrainerInput{
        .ctx = @ptrCast(&ctx),
        .build_forward = &TestCtx.buildForward,
        .build_loss = &TestCtx.buildLoss,
        .input_ids = ids[0..6],
        .attention_mask = mask[0..6],
        .targets = targets[0 .. 6 * ctx.hidden],
        .targets_shape = Shape.init(.f32, &.{ 6, ctx.hidden }),
        .batch = 1,
        .seq_len = 6,
        .capture_graph_context = &TestCtx.captureGraphContext,
        .restore_graph_context = &TestCtx.restoreGraphContext,
        .deinit_graph_context = &TestCtx.deinitGraphContext,
        .graph_variant = .{ 3, 0, 0, 0 },
    };
    try trainer.ensureGraphBuilt(input_c);
    try testing.expectEqual(@as(usize, 1), trainer.inactive_graphs.items.len);
    try testing.expectEqual(@as(u32, 4), trainer.inactive_graphs.items[0].signature.seq_len);
    try testing.expectEqual(@as(u32, 6), trainer.graph_signature.?.seq_len);
    try testing.expectEqualStrings("test.linear.weight.lora_A", trainer.lora_params.items[0].name);
}

// Mock reduce hook for the DDP hook test. Writes a sentinel value into
// each grad block and bumps a counter via the opaque context so the test
// can assert the harness actually invoked it.
const MockReduceCtx = struct {
    call_count: u32 = 0,
    last_block_count: usize = 0,
    sentinel: f32 = 7.0,
};

fn mockReduceGrads(ctx_opaque: *anyopaque, grads: []const GradBlock) anyerror!void {
    const ctx: *MockReduceCtx = @ptrCast(@alignCast(ctx_opaque));
    ctx.call_count += 1;
    ctx.last_block_count = grads.len;
    for (grads) |block| {
        for (block.data) |*v| v.* = ctx.sentinel;
    }
}

fn mockReduceDeviceGrads(ctx_opaque: *anyopaque, cb: *const ComputeBackend, grads: []const DeviceGradBlock) anyerror!void {
    _ = cb;
    const ctx: *MockReduceCtx = @ptrCast(@alignCast(ctx_opaque));
    ctx.call_count += 1;
    ctx.last_block_count = grads.len;
}

test "RealAutodiffTrainer: reduce_grads hook type wiring" {
    // Compile-time verification that the hook type + config fields line up,
    // plus a runtime check that a caller-provided hook can be invoked
    // directly with a synthetic GradBlock list. The full flush path is not
    // exercised here because it requires a live ComputeBackend — the hook
    // itself only cares about f32 slices, and its invocation inside `step`
    // is a straight-line call site verified by `zig test` compilation.
    const allocator = testing.allocator;

    comptime {
        const F: ?ReduceGradsFn = mockReduceGrads;
        _ = F;
        // Ensure the TrainerConfig field exists and has the right type.
        const C = TrainerConfig{
            .lora = .{ .rank = 1, .alpha = 1.0, .target_patterns = &.{"x"} },
        };
        _ = @TypeOf(C.reduce_grads);
        _ = @TypeOf(C.reduce_grads_ctx);
        _ = @TypeOf(C.reduce_device_grads);
        _ = @TypeOf(C.reduce_device_grads_ctx);
    }

    var ctx = MockReduceCtx{};
    var buf_a = [_]f32{ 1.0, 2.0, 3.0 };
    var buf_b = [_]f32{ 4.0, 5.0 };
    const blocks = [_]GradBlock{
        .{ .name = "a", .data = &buf_a },
        .{ .name = "b", .data = &buf_b },
    };

    const reduce: ReduceGradsFn = mockReduceGrads;
    try reduce(@ptrCast(&ctx), &blocks);

    try testing.expectEqual(@as(u32, 1), ctx.call_count);
    try testing.expectEqual(@as(usize, 2), ctx.last_block_count);
    for (buf_a) |v| try testing.expectEqual(ctx.sentinel, v);
    for (buf_b) |v| try testing.expectEqual(ctx.sentinel, v);

    // Also verify a TrainerConfig can actually carry the hook + ctx.
    const cfg = TrainerConfig{
        .lora = .{ .rank = 2, .alpha = 2.0, .target_patterns = &.{"q_proj"} },
        .reduce_grads = mockReduceGrads,
        .reduce_grads_ctx = @ptrCast(&ctx),
    };
    try testing.expect(cfg.reduce_grads != null);
    try testing.expect(cfg.reduce_grads_ctx != null);

    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, cfg);
    defer trainer.deinit();
    try testing.expect(trainer.config.reduce_grads != null);
}

test "RealAutodiffTrainer: reduce_device_grads hook type wiring" {
    const allocator = testing.allocator;

    comptime {
        const F: ?ReduceDeviceGradsFn = mockReduceDeviceGrads;
        _ = F;
    }

    var ctx = MockReduceCtx{};
    const fake_ct_a: CT = @ptrFromInt(0x1000);
    const fake_ct_b: CT = @ptrFromInt(0x2000);
    const dims_a = [_]i32{3};
    const dims_b = [_]i32{2};
    const blocks = [_]DeviceGradBlock{
        .{ .name = "a", .data = fake_ct_a, .elem_count = 3, .dims = &dims_a },
        .{ .name = "b", .data = fake_ct_b, .elem_count = 2, .dims = &dims_b },
    };
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    const reduce: ReduceDeviceGradsFn = mockReduceDeviceGrads;
    try reduce(@ptrCast(&ctx), dummy_cb, &blocks);
    try testing.expectEqual(@as(u32, 1), ctx.call_count);
    try testing.expectEqual(@as(usize, 2), ctx.last_block_count);

    const cfg = TrainerConfig{
        .lora = .{ .rank = 2, .alpha = 2.0, .target_patterns = &.{"q_proj"} },
        .reduce_device_grads = mockReduceDeviceGrads,
        .reduce_device_grads_ctx = @ptrCast(&ctx),
    };
    try testing.expect(cfg.reduce_device_grads != null);
    try testing.expect(cfg.reduce_device_grads_ctx != null);

    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, cfg);
    defer trainer.deinit();
    try testing.expect(trainer.config.reduce_device_grads != null);
}

test "RealAutodiffTrainer: partial accumulation flush keeps upstream configured divisor" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));

    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{ .rank = 1, .alpha = 1.0, .target_patterns = &.{"x"} },
        .optimizer = .{ .beta1 = 0.0, .beta2 = 0.0, .eps = 1.0, .weight_decay = 0.0 },
        .lr_schedule = .{ .constant = 1.0 },
        .max_grad_norm = 0.0,
        .grad_accum_steps = 4,
    });
    defer trainer.deinit();

    const weights = try allocator.alloc(f32, 1);
    weights[0] = 0.0;
    const grad_accum = try allocator.alloc(f32, 1);
    // Two micro-batch gradients summing to 8 were accumulated with the
    // configured 1/4 scale. Upstream flushes the stored value as-is.
    grad_accum[0] = 2.0;
    const dims = try allocator.dupe(i32, &.{1});
    const name = try allocator.dupe(u8, "x.lora_A");
    trainer.lora_params.append(allocator, .{
        .name = name,
        .weights = weights,
        .grad_accum = grad_accum,
        .node_id = null_node,
        .dims = dims,
    }) catch |err| {
        allocator.free(name);
        allocator.free(weights);
        allocator.free(grad_accum);
        allocator.free(dims);
        return err;
    };
    trainer.accum_count = 2;

    const result = (try trainer.flushAccumulatedGradients()).?;
    try testing.expectEqual(@as(u32, 2), result.micro_batches);
    try testing.expectEqual(@as(u64, 1), result.optimizer_step);
    try testing.expectApproxEqAbs(@as(f32, 2.0), result.grad_norm, 1e-6);
    // AdamW with beta1=beta2=0 and eps=1 updates by g/(|g|+1).
    try testing.expectApproxEqAbs(@as(f32, -2.0 / 3.0), weights[0], 1e-6);
    try testing.expectEqual(@as(f32, 0.0), grad_accum[0]);
    try testing.expect((try trainer.flushAccumulatedGradients()) == null);
}

test "RealAutodiffTrainer: training state round-trips weights moments and counters" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{ .rank = 1, .alpha = 1.0, .target_patterns = &.{"x"} },
    });
    defer trainer.deinit();

    const weights = try allocator.dupe(f32, &.{ 1.0, -2.0 });
    const grad_accum = try allocator.alloc(f32, 2);
    @memset(grad_accum, 0.0);
    const dims = try allocator.dupe(i32, &.{2});
    const name = try allocator.dupe(u8, "x.lora_A");
    trainer.lora_params.append(allocator, .{
        .name = name,
        .weights = weights,
        .grad_accum = grad_accum,
        .node_id = null_node,
        .dims = dims,
    }) catch |err| {
        allocator.free(name);
        allocator.free(weights);
        allocator.free(grad_accum);
        allocator.free(dims);
        return err;
    };
    const state = try trainer.optimizer_state.getOrCreate("x.lora_A", 2, true);
    @memcpy(state.m, &[_]f32{ 0.25, -0.5 });
    @memcpy(state.v, &[_]f32{ 0.75, 1.25 });
    state.step_count = 3;
    trainer.lora_params.items[0].adam_step_count = 3;
    trainer.optimizer_state.step_count = 3;
    trainer.step_count = 5;
    trainer.optimizer_step_count = 3;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/training_state.safetensors", .{tmp.sub_path});
    defer allocator.free(path);
    const fingerprint = @as([32]u8, @splat(7));
    const metrics_fingerprint = @as([32]u8, @splat(9));
    try trainer.saveTrainingState(path, &fingerprint, &metrics_fingerprint);
    const inspected = try trainer.inspectTrainingState(path, &fingerprint);
    try testing.expectEqual(@as(u64, 5), inspected.micro_batch_steps);
    try testing.expectEqual(@as(u64, 3), inspected.optimizer_steps);
    try trainer.validateTrainingStateMetricsPrefix(path, &metrics_fingerprint);
    const wrong_metrics_fingerprint = @as([32]u8, @splat(10));
    try testing.expectError(
        error.TrainingStateMetricsMismatch,
        trainer.validateTrainingStateMetricsPrefix(path, &wrong_metrics_fingerprint),
    );

    @memset(weights, 0.0);
    try trainer.loadTrainingStateWeightsForExport(path, &fingerprint);
    try testing.expectEqualSlices(f32, &.{ 1.0, -2.0 }, weights);
    @memset(weights, 0.0);
    trainer.optimizer_state.deinit();
    trainer.optimizer_state = optimizers.OptimizerState.init(allocator);
    trainer.step_count = 0;
    trainer.optimizer_step_count = 0;
    const wrong_fingerprint = @as([32]u8, @splat(8));
    try testing.expectError(error.TrainingStateFingerprintMismatch, trainer.loadTrainingState(path, &wrong_fingerprint));
    try trainer.loadTrainingState(path, &fingerprint);

    try testing.expectEqualSlices(f32, &.{ 1.0, -2.0 }, weights);
    const restored = trainer.optimizer_state.param_states.get("x.lora_A").?;
    try testing.expectEqualSlices(f32, &.{ 0.25, -0.5 }, restored.m);
    try testing.expectEqualSlices(f32, &.{ 0.75, 1.25 }, restored.v);
    try testing.expectEqual(@as(u64, 5), trainer.microBatchSteps());
    try testing.expectEqual(@as(u64, 3), trainer.optimizerSteps());
    try testing.expectEqual(@as(u32, 3), restored.step_count);
    try testing.expectEqual(@as(u32, 3), trainer.lora_params.items[0].adam_step_count);
}

test "RealAutodiffTrainer: checkpoint persists zero Adam state for a never-stepped conditional family" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{ .rank = 1, .alpha = 1.0, .target_patterns = &.{"classifier"} },
    });
    defer trainer.deinit();

    try trainer.registerConditionalOptimizerFamily("classifier.");
    const weights = try allocator.dupe(f32, &.{ 1.0, -2.0 });
    const grad_accum = try allocator.alloc(f32, weights.len);
    @memset(grad_accum, 0.0);
    const dims = try allocator.dupe(i32, &.{2});
    const name = try allocator.dupe(u8, "classifier.0.weight.lora_A");
    trainer.lora_params.append(allocator, .{
        .name = name,
        .weights = weights,
        .grad_accum = grad_accum,
        .node_id = null_node,
        .dims = dims,
    }) catch |err| {
        allocator.free(name);
        allocator.free(weights);
        allocator.free(grad_accum);
        allocator.free(dims);
        return err;
    };
    // The family never appeared, so the host optimizer correctly has no
    // lazily-created ParamState for this slot.
    try testing.expect(trainer.optimizer_state.param_states.get(name) == null);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/never_stepped.safetensors", .{tmp.sub_path});
    defer allocator.free(path);
    try trainer.saveTrainingState(path, null, null);

    try trainer.loadTrainingState(path, null);
    const restored = trainer.optimizer_state.param_states.get(name).?;
    try testing.expectEqualSlices(f32, &.{ 0.0, 0.0 }, restored.m);
    try testing.expectEqualSlices(f32, &.{ 0.0, 0.0 }, restored.v);
    try testing.expectEqual(@as(u32, 0), restored.step_count);
    try testing.expectEqual(@as(u32, 0), trainer.lora_params.items[0].adam_step_count);
}

test "RealAutodiffTrainer: training state without per-slot adam steps falls back to the optimizer step count" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{ .rank = 1, .alpha = 1.0, .target_patterns = &.{"x"} },
    });
    defer trainer.deinit();
    try appendTestDeviceParamSlot(&trainer, &trainer.lora_params, "x.lora_A", 1);
    // Host-only restore path: the slot has no device counterpart.
    trainer.lora_params.items[0].device = null;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/legacy_state.safetensors", .{tmp.sub_path});
    defer allocator.free(path);

    // A checkpoint written before the per-slot Adam step count existed.
    const counters = encodeTrainingStateCounters(5, 3);
    const zeros = [_]f32{0.0};
    try safetensors_checkpoint.save(allocator, path, &.{
        .{ .name = "__trainer_counters", .data = &counters, .shape = &.{counters.len} },
        .{ .name = "weight::x.lora_A", .data = &[_]f32{1.5}, .shape = &.{1} },
        .{ .name = "adam_m::x.lora_A", .data = &zeros, .shape = &.{1} },
        .{ .name = "adam_v::x.lora_A", .data = &zeros, .shape = &.{1} },
    });

    try trainer.loadTrainingState(path, null);
    try testing.expectEqual(@as(f32, 1.5), trainer.lora_params.items[0].weights[0]);
    try testing.expectEqual(@as(u32, 3), trainer.optimizer_state.param_states.get("x.lora_A").?.step_count);
    try testing.expectEqual(@as(u32, 3), trainer.lora_params.items[0].adam_step_count);
}

test "RealAutodiffTrainer: init and deinit are clean with no graph built" {
    const allocator = testing.allocator;

    // A dummy backend pointer — we never call into it because we never
    // call `.step`. init/deinit must tolerate the graph_state being null.
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));

    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{
            .rank = 2,
            .alpha = 2.0,
            .target_patterns = &.{"q_proj"},
        },
    });
    defer trainer.deinit();

    try testing.expectEqual(@as(u64, 0), trainer.step_count);
    try testing.expect(trainer.graph_state == null);
    try testing.expectEqual(@as(usize, 0), trainer.lora_params.items.len);
}

test "RealAutodiffTrainer: conditional optimizer families gate absent-task steps" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));

    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{
            .rank = 2,
            .alpha = 2.0,
            .target_patterns = &.{"q_proj"},
        },
    });
    defer trainer.deinit();

    try trainer.registerConditionalOptimizerFamily("classifier.");
    try trainer.registerConditionalOptimizerFamily("count_pred.");

    // Registered families start absent (torch grad=None) and gate matching
    // parameter names; unregistered names always step.
    try testing.expect(trainer.optimizerFamilyAbsent("classifier.0.lora_A"));
    try testing.expect(trainer.optimizerFamilyAbsent("count_pred.2.lora_B"));
    try testing.expect(!trainer.optimizerFamilyAbsent("encoder.layer.0.attention.self.query_proj.weight.lora_A"));

    // Marking presence lifts the gate for that family only.
    trainer.markOptimizerFamilyPresent("classifier.");
    try testing.expect(!trainer.optimizerFamilyAbsent("classifier.0.lora_A"));
    try testing.expect(trainer.optimizerFamilyAbsent("count_pred.2.lora_B"));

    // The per-optimizer-step window reset restores the absent default.
    trainer.resetOptimizerFamilyWindow();
    try testing.expect(trainer.optimizerFamilyAbsent("classifier.0.lora_A"));

    // Registration capacity is bounded and fails closed.
    while (trainer.conditional_optimizer_family_count < max_conditional_optimizer_families) {
        try trainer.registerConditionalOptimizerFamily("overflow.");
    }
    try testing.expectError(
        error.TooManyConditionalOptimizerFamilies,
        trainer.registerConditionalOptimizerFamily("one_too_many."),
    );
}

/// Enroll a one-element trainable whose device handles are unique opaque
/// markers, so batch membership can be checked without a Metal device.
fn appendTestDeviceParamSlot(
    trainer: *RealAutodiffTrainer,
    slots: *std.ArrayListUnmanaged(RealAutodiffTrainer.ParamSlot),
    name: []const u8,
    marker_id: usize,
) !void {
    const allocator = trainer.allocator;
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    const weights = try allocator.alloc(f32, 1);
    errdefer allocator.free(weights);
    weights[0] = 0.0;
    const grad_accum = try allocator.alloc(f32, 1);
    errdefer allocator.free(grad_accum);
    grad_accum[0] = 0.0;
    const dims = try allocator.dupe(i32, &.{1});
    errdefer allocator.free(dims);
    const marker: CT = @ptrFromInt(marker_id * @alignOf(usize));
    try slots.append(allocator, .{
        .name = owned_name,
        .weights = weights,
        .grad_accum = grad_accum,
        .node_id = null_node,
        .dims = dims,
        .device = .{ .weight = marker, .grad_accum = marker, .m = marker, .v = marker },
    });
}

fn findTestParamSlot(trainer: *RealAutodiffTrainer, name: []const u8) ?*RealAutodiffTrainer.ParamSlot {
    for (trainer.lora_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, name)) return slot;
    }
    for (trainer.regular_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, name)) return slot;
    }
    return null;
}

test "RealAutodiffTrainer: device AdamW batch skips absent optimizer families and uses per-slot bias correction" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));

    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{ .rank = 1, .alpha = 1.0, .target_patterns = &.{"q_proj"} },
        .optimizer = .{ .beta1 = 0.9, .beta2 = 0.999, .eps = 1e-8, .weight_decay = 0.01 },
    });
    defer {
        // The device handles are identity markers, never real tensors; drop
        // them before deinit so the dummy backend is not asked to free them.
        for (trainer.lora_params.items) |*slot| slot.device = null;
        for (trainer.regular_params.items) |*slot| slot.device = null;
        trainer.deinit();
    }

    try trainer.registerConditionalOptimizerFamily("classifier.");
    try trainer.registerConditionalOptimizerFamily("count_pred.");

    const names = [_][]const u8{
        "encoder.layer.0.attention.self.query_proj.weight.lora_A",
        "classifier.0.weight",
        "count_pred.0.weight",
        "count_pred.2.bias",
    };
    try appendTestDeviceParamSlot(&trainer, &trainer.lora_params, names[0], 1);
    for (names[1..], 2..) |name, marker_id| {
        try appendTestDeviceParamSlot(&trainer, &trainer.regular_params, name, marker_id);
    }

    // The same window schedule driven through the host optimizer, whose
    // per-parameter step count is the reference the device path must match.
    var host_state = optimizers.OptimizerState.init(allocator);
    defer host_state.deinit();
    const host_config = optimizers.Optimizer{ .adamw = trainer.config.optimizer };
    var host_weight = [_]f32{0.0};
    const host_grad = [_]f32{0.1};

    const opt = trainer.config.optimizer;
    var committed_windows: u32 = 0;
    // classification tasks appear in every window; count tasks only in the second.
    for ([_]bool{ false, true, false }) |count_pred_present| {
        trainer.markOptimizerFamilyPresent("classifier.");
        if (count_pred_present) trainer.markOptimizerFamilyPresent("count_pred.");

        const batch = try trainer.buildDeviceAdamWBatch(.accumulated);
        defer allocator.free(batch);
        try testing.expectEqual(@as(usize, if (count_pred_present) 4 else 2), batch.len);
        // Building must not advance the persisted counters — an aborted step
        // would otherwise leave them permanently ahead of the moments.
        try testing.expectEqual(committed_windows, findTestParamSlot(&trainer, names[0]).?.adam_step_count);
        if (!count_pred_present) {
            for (names[2..]) |absent_name| {
                const absent = findTestParamSlot(&trainer, absent_name).?;
                for (batch) |item| try testing.expect(item.weight != absent.device.?.weight);
            }
        }
        for (batch, 0..) |item, idx| {
            // Batch order is lora slots then regular slots, gated entries removed.
            const slot = findTestParamSlot(&trainer, names[idx]).?;
            try testing.expectEqual(slot.device.?.weight, item.weight);
            // Bias correction describes the step about to be taken, so it runs
            // one ahead of the counter until the step is committed.
            const t: f32 = @floatFromInt(slot.adam_step_count + 1);
            try testing.expectEqual(1.0 - std.math.pow(f32, opt.beta1, t), item.bias_correction1);
            try testing.expectEqual(1.0 - std.math.pow(f32, opt.beta2, t), item.bias_correction2);
        }
        trainer.commitDeviceAdamWStepCounts();
        committed_windows += 1;

        for (names) |name| {
            if (trainer.optimizerFamilyAbsent(name)) continue;
            try optimizers.step(host_config, &host_state, 1e-3, name, &host_weight, &host_grad);
        }
        trainer.resetOptimizerFamilyWindow();
    }

    try testing.expectEqual(@as(u32, 3), findTestParamSlot(&trainer, names[0]).?.adam_step_count);
    try testing.expectEqual(@as(u32, 3), findTestParamSlot(&trainer, names[1]).?.adam_step_count);
    try testing.expectEqual(@as(u32, 1), findTestParamSlot(&trainer, names[2]).?.adam_step_count);
    try testing.expectEqual(@as(u32, 1), findTestParamSlot(&trainer, names[3]).?.adam_step_count);

    for (names) |name| {
        try testing.expectEqual(
            host_state.param_states.get(name).?.step_count,
            findTestParamSlot(&trainer, name).?.adam_step_count,
        );
    }
}
