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

//! Cross-backend GLiNER2 **gradient** parity gate (native vs Metal/CUDA).
//!
//! Why this file exists
//! ────────────────────
//! Forward parity between the native and Metal compute backends was covered
//! (`metal_compute.zig` has per-op forward audits, and the GLiNER2 eval path
//! compares logits). The *backward* pass was not covered at the graph level by
//! anything. That gap let a ~17.5% native-vs-Metal divergence in the GLiNER2
//! structure-loss backward ship completely unnoticed: on a `json_structures`
//! record with identical init and seed, one optimizer step produced
//!
//!     native: loss 34.010937  grad_norm  89.7090
//!     Metal:  loss 34.010914  grad_norm 105.4414
//!
//! i.e. the forward agreed to 6.8e-7 relative while the gradients were 17.5%
//! apart. A forward-only gate cannot see that, and a loss-only gate cannot see
//! it either — the loss is the *last* forward value, and it agrees.
//!
//! What this gate does
//! ───────────────────
//! For each objective it runs the SAME autodiff graph, from the SAME synthetic
//! weights, with the SAME trainer seed and the SAME batch, once on
//! `NativeCompute` and once on each enabled accelerator, and compares the **per-parameter
//! gradient tensors** that the optimizer is about to consume.
//!
//! Per-parameter (not just the global grad-norm) is the point: `grad_norm` is a
//! single scalar over every trainable, so equal-and-opposite errors in two
//! parameters cancel and the scalar looks fine. This compares each parameter's
//! gradient block independently, on two complementary statistics:
//!
//!   * relative L2   — `‖g_metal − g_native‖₂ / max(‖g_native‖₂, ‖g_metal‖₂)`
//!     catches a systematically wrong tensor.
//!   * relative max  — `max|g_metal − g_native| / max(max|g_native|, max|g_metal|)`
//!     catches a small number of badly wrong elements that L2 would average away.
//!
//! Gradients are intercepted through the trainer's existing
//! `TrainerConfig.reduce_grads` hook, which fires once per accumulation flush
//! with the finalized host-side accumulator for every trainable, *before*
//! clipping and before the optimizer step. No trainer changes are needed, and
//! the values compared are exactly the ones AdamW would apply.
//!
//! Self-contained by construction
//! ──────────────────────────────
//! No checkpoint, no tokenizer, no network. The graph is built once to
//! enumerate its parameter nodes, then every parameter is synthesized at its
//! declared shape from a name-seeded PRNG — so both backends are guaranteed
//! bit-identical inputs regardless of iteration order. The bug being guarded
//! against is in kernel math, so random weights of the right shape expose it.
//!
//! Running
//! ───────
//!     zig build test-gliner2-backend-grad-parity
//!     TERMITE_REQUIRE_CUDA_TESTS=1 zig build test-gliner2-backend-grad-parity \
//!       -Dcuda=true -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast
//!
//! Skips (rather than fails) when Metal is not compiled in or no Metal device
//! is present, matching the convention used by the `metal_compute.zig` audits.
//! Set `TERMITE_REQUIRE_METAL_TESTS=1` in Metal CI to turn either condition into
//! a hard failure so a misconfigured runner cannot silently bypass this gate.
//! `TERMITE_REQUIRE_CUDA_TESTS=1` provides the equivalent fail-closed CUDA gate.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ml = @import("ml");
const inference = @import("inference_internal");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const deberta_graph = inference.architectures.deberta_graph;
const native_compute = inference.native_compute.native;
const NativeCompute = native_compute.NativeCompute;
const NativeWeightStore = native_compute.WeightStore;
const gpu_hosted_store = inference.native_compute.gpu_hosted_store;
const metal_compute = if (build_options.enable_metal) inference.native_compute.metal else struct {};
const cuda_compute = if (build_options.enable_cuda) inference.native_compute.cuda else struct {};
const MetalWeightStore = if (build_options.enable_metal) gpu_hosted_store.WeightStore else void;
const metal_runtime = inference.metal_runtime;

const ops_mod = inference.ops;
const ComputeBackend = ops_mod.ComputeBackend;
const Tensor = inference.backends.Tensor;

const real_autodiff = inference.finetune.real_autodiff_trainer;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;
const compat = inference.io.compat;

// ── Model geometry ───────────────────────────────────────────────────────────
//
// Shapes mirror the real `fastino/gliner2-base-v1` encoder (deberta-v3-base:
// hidden 768, 12 heads, intermediate 3072, max_position_embeddings 512,
// position_buckets 256) because accelerator kernel selection is shape-driven —
// threadgroup width, SIMD-group count and the fused-DeBERTa fast paths all key
// off hidden size / head dim / sequence length. Only two deliberate reductions
// are made, and neither changes which kernel runs:
//
//   * 2 encoder layers instead of 12. Layer count only repeats the same kernels.
//   * vocab 4096 instead of 128011. The embedding table is a gather; its row
//     count does not select a different kernel, and 128011x768 f32 would cost
//     ~393 MB of synthetic weights for no additional coverage.

const HIDDEN: u32 = 768;
const NUM_LAYERS: u32 = 2;
const NUM_HEADS: u32 = 12;
const INTERMEDIATE: u32 = 3072;
const VOCAB: u32 = 4096;
const MAX_POS: u32 = 512;
const POS_BUCKETS: u32 = 256;

const graph_config = deberta_graph.Config{
    .vocab_size = VOCAB,
    .hidden_size = HIDDEN,
    .num_hidden_layers = NUM_LAYERS,
    .num_attention_heads = NUM_HEADS,
    .intermediate_size = INTERMEDIATE,
    .max_position_embeddings = MAX_POS,
    .position_buckets = POS_BUCKETS,
    .layer_norm_eps = 1e-7,
    .use_v3_names = true,
};

// Two samples are the smallest fixture that exercises cross-batch flattening,
// masks, span ownership, and count-state indexing. The production comparison
// wrapper separately gates the real batch-32/seq-128 geometry.
const BATCH: u32 = 2;
const SEQ_LEN: u32 = 128;
/// Number of real tokens; the tail is padding so the attention-mask/bias path
/// is exercised too.
const VALID_TOKENS: u32 = 96;
/// Entity types (schema fields) for the span / structure objectives.
const NUM_ENTITY_TYPES: u32 = 4;
/// Token-classification classes: "O" plus one per entity type.
const NUM_CLASSES: u32 = NUM_ENTITY_TYPES + 1;
/// Candidate spans per sample.
const MAX_SPANS: u32 = 8;

/// Seed for the synthetic parameter store. Mixed with each parameter name so
/// values are independent of enumeration order.
const WEIGHT_SEED: u64 = 0x6C1_1DE2;
/// Seed for the post-build LoRA A/B override (see `seedLoraSlots`).
const LORA_SEED: u64 = 0xA5A5_1337;
/// Trainer's own RNG seed. Identical on both backends.
const TRAINER_SEED: u64 = 42;

const lora_targets = [_][]const u8{ "query_proj", "key_proj", "value_proj" };

// ── Tolerance ────────────────────────────────────────────────────────────────
//
// Both statistics are scale-normalized, so a single dimensionless bound applies
// to every parameter regardless of its magnitude.
//
// Floor (what honest f32 disagreement costs). f32 eps is 1.19e-7. The two
// backends use different GEMM implementations with different accumulation
// orders, so the relative error of one contraction of length K grows like
// ~sqrt(K)*eps for random-sign operands. The widest contraction in this graph
// is the 3072-wide FFN projection: sqrt(3072)*1.19e-7 ≈ 6.6e-6. The backward
// pass chains on the order of ten such contractions per layer, and errors add
// in quadrature, so even a deliberately pessimistic bound lands under ~1e-4.
// Measured reality is far below that: the reported forward agrees to 6.8e-7
// relative and the `token` backward — the known-good control, which this file
// asserts on directly — agrees to 7.9e-7.
//
// Ceiling (what a real kernel bug costs). The divergence this gate exists to
// catch is 0.175 relative on grad_norm, and per-parameter it is larger still
// because the scalar partially cancels.
//
// 1e-3 sits at least an order of magnitude above the pessimistic accumulation
// bound (≈1e-4) and ~175x below the known defect, so it can neither flake on
// rounding nor miss a genuine backward divergence. It is intentionally NOT
// tightened toward the ~1e-6 noise floor: this is a correctness gate, not a
// bit-exactness gate, and the backends are not required to agree bit-for-bit.
const TOL_REL_L2: f64 = 1e-3;
const TOL_REL_MAX: f64 = 1e-3;

/// Denominators are guarded with this so an all-zero gradient pair reports 0
/// rather than NaN.
const ZERO_GUARD: f64 = 1e-30;

// ── Synthetic parameter specs ────────────────────────────────────────────────

const ParamSpec = struct {
    /// Owned.
    name: []u8,
    /// Owned.
    dims: []i64,

    fn elemCount(self: ParamSpec) usize {
        var n: usize = 1;
        for (self.dims) |d| n *= @intCast(d);
        return n;
    }
};

fn freeSpecs(allocator: std.mem.Allocator, specs: []ParamSpec) void {
    for (specs) |spec| {
        allocator.free(spec.name);
        allocator.free(spec.dims);
    }
    allocator.free(specs);
}

/// Build the objective's forward+loss graph once (no LoRA, no backend) purely
/// to enumerate the parameter nodes it will demand from the weight store.
///
/// Doing this instead of hard-coding a name list means the gate keeps working
/// when the GLiNER2 heads gain or lose a projection: a new parameter is
/// synthesized automatically instead of failing with `WeightNotFound`.
fn discoverParams(
    allocator: std.mem.Allocator,
    config: gliner2_autodiff.GlinerAutodiffConfig,
    targets_shape: Shape,
) ![]ParamSpec {
    var ctx = gliner2_autodiff.GlinerAutodiffCtx.init(config);

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const shape_2d = Shape.init(.f32, &.{ @intCast(BATCH), @intCast(SEQ_LEN) });
    const ids_node = try bld.parameter("__input_ids", shape_2d);
    const mask_node = try bld.parameter("__attention_mask", shape_2d);
    const forward = try gliner2_autodiff.GlinerAutodiffCtx.buildForward(
        @ptrCast(&ctx),
        &bld,
        ids_node,
        mask_node,
        BATCH,
        SEQ_LEN,
    );
    const targets_node = try bld.parameter("__targets", targets_shape);
    const loss = try gliner2_autodiff.GlinerAutodiffCtx.buildLoss(
        @ptrCast(&ctx),
        &bld,
        forward,
        targets_node,
    );
    try g.markOutput(loss);

    var specs: std.ArrayListUnmanaged(ParamSpec) = .empty;
    errdefer {
        for (specs.items) |spec| {
            allocator.free(spec.name);
            allocator.free(spec.dims);
        }
        specs.deinit(allocator);
    }
    var seen: std.StringHashMapUnmanaged(usize) = .empty;
    defer seen.deinit(allocator);

    for (g.parameters.items) |param_id| {
        const node = g.node(param_id);
        switch (node.op) {
            .parameter => {},
            else => continue,
        }
        const name = g.parameterName(node);
        // "__"-prefixed nodes are runtime placeholders (input ids, attention
        // mask, targets, gliner2 attention bias) bound by the trainer, not
        // weights fetched from the store.
        if (std.mem.startsWith(u8, name, "__")) continue;

        const rank = node.output_shape.rank();
        if (seen.get(name)) |existing_idx| {
            // The same weight may be referenced by more than one node (the
            // classification and count heads reuse `classifier.*`). That is
            // fine as long as every reference agrees on the shape; if it ever
            // stops agreeing, a single store entry cannot serve both and the
            // gate must say so rather than silently synthesize the wrong one.
            const existing = specs.items[existing_idx];
            if (existing.dims.len != rank) return error.ConflictingParameterShape;
            for (existing.dims, 0..) |d, axis| {
                if (d != node.output_shape.dim(@intCast(axis))) return error.ConflictingParameterShape;
            }
            continue;
        }

        const dims = try allocator.alloc(i64, rank);
        errdefer allocator.free(dims);
        for (0..rank) |axis| dims[axis] = node.output_shape.dim(@intCast(axis));
        const owned_name = try allocator.dupe(u8, name);
        errdefer allocator.free(owned_name);

        try specs.append(allocator, .{ .name = owned_name, .dims = dims });
        try seen.put(allocator, owned_name, specs.items.len - 1);
    }

    return specs.toOwnedSlice(allocator);
}

/// Deterministically synthesize one parameter's values.
///
/// Seeded from the parameter *name* (not its index), so both backends get
/// bit-identical bytes without depending on enumeration order.
fn fillWeightValues(name: []const u8, dims: []const i64, out: []f32) void {
    var prng = std.Random.DefaultPrng.init(std.hash.Wyhash.hash(WEIGHT_SEED, name));
    var rnd = prng.random();

    // LayerNorm gains sit at ~1.0; initializing them from a zero-mean normal
    // would collapse every normalized activation and mask real divergence.
    const is_norm_gain = std.mem.endsWith(u8, name, "LayerNorm.weight") or
        std.mem.endsWith(u8, name, "norm1.weight") or
        std.mem.endsWith(u8, name, "norm2.weight");
    if (is_norm_gain) {
        for (out) |*v| v.* = 1.0 + rnd.floatNorm(f32) * 0.02;
        return;
    }
    if (std.mem.endsWith(u8, name, ".bias")) {
        for (out) |*v| v.* = rnd.floatNorm(f32) * 0.01;
        return;
    }
    // Embedding tables use the transformer default; everything else gets a
    // fan-in scaled normal so activations stay O(1) through the stack.
    const is_embedding = std.mem.indexOf(u8, name, "embeddings.") != null or
        std.mem.indexOf(u8, name, "pos_embedding") != null;
    const fan_in: f32 = if (dims.len >= 2)
        @floatFromInt(dims[dims.len - 1])
    else
        @floatFromInt(@max(dims[0], 1));
    const sd: f32 = if (is_embedding) 0.02 else 1.0 / @sqrt(fan_in);
    for (out) |*v| v.* = rnd.floatNorm(f32) * sd;
}

// ── Weight stores ────────────────────────────────────────────────────────────
//
// The two backends take *different* store types: NativeCompute wants
// `native_compute.WeightStore` (resident_weights), MetalCompute wants
// `gpu_hosted_store.WeightStore` (lazy_weights with a host_loaded tensor).
// Both are filled from the same `ParamSpec` list through `fillWeightValues`,
// so their contents are byte-identical.

const NativeStore = struct {
    allocator: std.mem.Allocator,
    store: NativeWeightStore,
    names: std.ArrayListUnmanaged([]u8) = .empty,

    fn init(allocator: std.mem.Allocator, specs: []const ParamSpec) !NativeStore {
        var self = NativeStore{
            .allocator = allocator,
            .store = .{
                .allocator = allocator,
                .resident_weights = .{},
                .lazy_weights = .{},
            },
        };
        errdefer self.deinit();

        const scratch = try allocator.alloc(f32, maxElemCount(specs));
        defer allocator.free(scratch);

        for (specs) |spec| {
            const n = spec.elemCount();
            fillWeightValues(spec.name, spec.dims, scratch[0..n]);
            const owned_name = try allocator.dupe(u8, spec.name);
            try self.names.append(allocator, owned_name);
            const tensor = try Tensor.initFloat32(allocator, owned_name, spec.dims, scratch[0..n]);
            try self.store.resident_weights.put(allocator, owned_name, .{ .tensor = tensor });
        }
        return self;
    }

    fn deinit(self: *NativeStore) void {
        var it = self.store.resident_weights.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit();
        self.store.resident_weights.deinit(self.allocator);
        for (self.names.items) |name| self.allocator.free(name);
        self.names.deinit(self.allocator);
    }
};

const MetalStore = struct {
    allocator: std.mem.Allocator,
    store: MetalWeightStore,

    fn init(allocator: std.mem.Allocator, specs: []const ParamSpec) !MetalStore {
        if (comptime !build_options.enable_metal) return error.SkipZigTest;
        var self = MetalStore{
            .allocator = allocator,
            .store = .{
                .allocator = allocator,
                .prefix = "",
                .lazy_weights = .{},
            },
        };
        errdefer self.deinit();

        const scratch = try allocator.alloc(f32, maxElemCount(specs));
        defer allocator.free(scratch);

        for (specs) |spec| {
            const n = spec.elemCount();
            fillWeightValues(spec.name, spec.dims, scratch[0..n]);
            const owned_name = try allocator.dupe(u8, spec.name);
            const tensor = try Tensor.initFloat32(allocator, owned_name, spec.dims, scratch[0..n]);
            try self.store.lazy_weights.put(allocator, owned_name, .{
                .tensor_ref = undefined,
                .host_loaded = .{ .tensor = tensor },
                .active_tier = .host,
                .loaded_bytes = tensor.data.len,
            });
        }
        metal_compute.initPrefetchQueue(&self.store, allocator);
        return self;
    }

    fn deinit(self: *MetalStore) void {
        if (comptime !build_options.enable_metal) return;
        metal_compute.deinitPrefetchQueue(&self.store);
        metal_compute.deinitSharedNativeProvider(&self.store);
        var it = self.store.lazy_weights.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            if (entry.value_ptr.host_loaded) |*loaded| loaded.deinit();
            if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
        }
        self.store.lazy_weights.deinit(self.allocator);
    }
};

fn maxElemCount(specs: []const ParamSpec) usize {
    var largest: usize = 1;
    for (specs) |spec| largest = @max(largest, spec.elemCount());
    return largest;
}

// ── Gradient capture ─────────────────────────────────────────────────────────

/// Snapshots every trainable's finalized gradient accumulator.
///
/// Installed as `TrainerConfig.reduce_grads`, which the trainer calls exactly
/// once per accumulation flush, after the accumulator is complete and *before*
/// global-norm clipping and the optimizer step. Grad-norm clipping is disabled
/// (`max_grad_norm = 0`) anyway so the captured values are the raw gradients.
const GradCapture = struct {
    allocator: std.mem.Allocator,
    names: std.ArrayListUnmanaged([]u8) = .empty,
    grads: std.ArrayListUnmanaged([]f32) = .empty,
    calls: usize = 0,

    fn hook(ctx_opaque: *anyopaque, blocks: []const real_autodiff.GradBlock) anyerror!void {
        const self: *GradCapture = @ptrCast(@alignCast(ctx_opaque));
        self.calls += 1;
        for (blocks) |block| {
            const name = try self.allocator.dupe(u8, block.name);
            errdefer self.allocator.free(name);
            const data = try self.allocator.dupe(f32, block.data);
            errdefer self.allocator.free(data);
            try self.names.append(self.allocator, name);
            try self.grads.append(self.allocator, data);
        }
    }

    fn find(self: *const GradCapture, name: []const u8) ?[]const f32 {
        for (self.names.items, self.grads.items) |candidate, data| {
            if (std.mem.eql(u8, candidate, name)) return data;
        }
        return null;
    }

    fn deinit(self: *GradCapture) void {
        for (self.names.items) |name| self.allocator.free(name);
        for (self.grads.items) |data| self.allocator.free(data);
        self.names.deinit(self.allocator);
        self.grads.deinit(self.allocator);
    }
};

const RunResult = struct {
    loss: f32,
    grad_norm: f32,
    capture: GradCapture,
    device_peak_bytes: u64,

    fn deinit(self: *RunResult) void {
        self.capture.deinit();
    }
};

const StepInputs = struct {
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
};

/// Override the trainer's LoRA slots with deterministic non-zero values.
///
/// LoRA B is zero-initialized by design, which makes `dL/dA ∝ B` exactly zero
/// on the very first step — half of the trainable set would then be a trivially
/// matching all-zero tensor and would contribute nothing to the gate. Writing
/// both matrices from a name-seeded PRNG (identical on both backends, since the
/// slot names are identical) puts every LoRA parameter on a real gradient path.
fn seedLoraSlots(trainer: *real_autodiff.RealAutodiffTrainer) void {
    for (trainer.lora_params.items) |*slot| {
        var prng = std.Random.DefaultPrng.init(std.hash.Wyhash.hash(LORA_SEED, slot.name));
        var rnd = prng.random();
        for (slot.weights) |*w| w.* = rnd.floatNorm(f32) * 0.05;
    }
}

/// One forward+backward on `cb`, returning the loss, the global grad-norm and
/// every per-parameter gradient block.
fn runOneStep(
    allocator: std.mem.Allocator,
    cb: *ComputeBackend,
    config: gliner2_autodiff.GlinerAutodiffConfig,
    inputs: StepInputs,
) !RunResult {
    var capture = GradCapture{ .allocator = allocator };
    errdefer capture.deinit();

    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(config);

    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, cb, .{
        .lora = .{
            .rank = 4,
            .alpha = 8.0,
            .target_patterns = &lora_targets,
        },
        // The optimizer never gets to matter — a single step is taken and the
        // gradients are read before the update — but a zero LR keeps the run
        // free of any weight drift if that ever changes.
        .lr_schedule = .{ .constant = 0.0 },
        // Clipping off: compare the raw gradients, not gradients that a global
        // norm has already rescaled (which would partially hide a divergence by
        // renormalizing both sides toward the same magnitude).
        .max_grad_norm = 0.0,
        .grad_accum_steps = 1,
        .lora_a_init_std = 0.02,
        .hidden_size_hint = HIDDEN,
        .num_layers_hint = NUM_LAYERS,
        .seed = TRAINER_SEED,
        .regular_trainable_params = &.{},
        .reduce_grads = &GradCapture.hook,
        .reduce_grads_ctx = @ptrCast(&capture),
        // Interpreter on BOTH backends. This is deliberate: it holds the graph,
        // the autodiff transformation and the execution order fixed, so the
        // only thing that differs between the two runs is the kernel math.
        .execution_engine = .interpreter,
    });
    defer trainer.deinit();

    const input = gliner2_autodiff.makeTrainerInput(
        &gliner_ctx,
        inputs.input_ids,
        inputs.attention_mask,
        inputs.targets,
        inputs.targets_shape,
        BATCH,
        SEQ_LEN,
    );

    // Build first so the LoRA slots exist, override them, then step. `step()`
    // re-runs `ensureGraphBuilt` with the same signature and hits the
    // active-graph reuse path, so the override survives.
    try trainer.ensureGraphBuilt(input);
    seedLoraSlots(&trainer);

    const result = try trainer.step(input);

    if (capture.calls != 1) return error.GradientCaptureNotInvoked;
    if (capture.names.items.len == 0) return error.NoTrainableGradientsCaptured;

    return .{
        .loss = result.loss,
        .grad_norm = result.grad_norm,
        .capture = capture,
        .device_peak_bytes = cb.debugTimingSnapshot().provider.metal_tensor_device_owned_peak_live_bytes,
    };
}

// ── Comparison ───────────────────────────────────────────────────────────────

const ParamDelta = struct {
    name: []const u8,
    rel_l2: f64,
    rel_max: f64,
    native_l2: f64,
    metal_l2: f64,
    elems: usize,
};

fn compareBlocks(name: []const u8, a: []const f32, b: []const f32) ParamDelta {
    var diff_sq: f64 = 0.0;
    var a_sq: f64 = 0.0;
    var b_sq: f64 = 0.0;
    var max_diff: f64 = 0.0;
    var max_a: f64 = 0.0;
    var max_b: f64 = 0.0;
    for (a, b) |x, y| {
        const xa: f64 = x;
        const yb: f64 = y;
        const d = xa - yb;
        diff_sq += d * d;
        a_sq += xa * xa;
        b_sq += yb * yb;
        max_diff = @max(max_diff, @abs(d));
        max_a = @max(max_a, @abs(xa));
        max_b = @max(max_b, @abs(yb));
    }
    const scale_l2 = @max(@sqrt(a_sq), @max(@sqrt(b_sq), ZERO_GUARD));
    const scale_max = @max(max_a, @max(max_b, ZERO_GUARD));
    return .{
        .name = name,
        .rel_l2 = @sqrt(diff_sq) / scale_l2,
        .rel_max = max_diff / scale_max,
        .native_l2 = @sqrt(a_sq),
        .metal_l2 = @sqrt(b_sq),
        .elems = a.len,
    };
}

fn relDelta(a: f64, b: f64) f64 {
    return @abs(a - b) / @max(@abs(a), @max(@abs(b), ZERO_GUARD));
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn cudaParityTestSelected(name: []const u8) bool {
    const filter = platform.env.getenv("TERMITE_CUDA_PARITY_TEST_FILTER") orelse return true;
    return filter.len == 0 or std.mem.indexOf(u8, name, filter) != null;
}

// ── Batch construction ───────────────────────────────────────────────────────

fn fillInputs(input_ids: []i64, attention_mask: []f32) void {
    for (input_ids, 0..) |*id, i| id.* = @intCast((i * 7919 + 13) % VOCAB);
    for (attention_mask, 0..) |*m, i| m.* = if (i % SEQ_LEN < VALID_TOKENS) 1.0 else 0.0;
}

/// Schema-prompt token position for entity type `e`, matching the layout the
/// real encoder produces (entity-type prompt tokens at the front of the
/// sequence, spans well after them).
fn schemaTokenPos(e: usize) usize {
    return 1 + e;
}

fn spanStartTokenPos(span_idx: usize) usize {
    return 8 + 3 * span_idx;
}

/// Write the packed span-start section of one target row.
///
/// Layout (see `gliner2_real_autodiff.spanStartTargetWidth`):
///   labels[E] | mask[E] | schema_token_idx[E] | row_repeat_idx[E] |
///   count_state_idx[E] | start_token_idx | end_token_idx
///
/// Index conventions mirror `fillSpanStartTargetsFromEncodedBatchWithOptions`
/// exactly (flat `sample * max_length + token` addressing, per-sample instance
/// ordinals for the count state) so the synthetic targets drive the same
/// gathers the real data path does.
fn fillSpanSection(row: []f32, sample_idx: usize, span_idx: usize) void {
    const E: usize = NUM_ENTITY_TYPES;
    const L: usize = SEQ_LEN;
    const flat_span_idx = sample_idx * MAX_SPANS + span_idx;
    const schema_off = 2 * E;
    const row_off = schema_off + E;
    const count_off = row_off + E;
    const start_off = count_off + E;

    const start_tok = spanStartTokenPos(span_idx);
    const end_tok = start_tok + 1;

    for (0..E) |e| {
        // Deterministic, non-degenerate label pattern: some rows positive for
        // some fields, so the BCE has both signs of signal.
        row[e] = if ((span_idx + e) % 3 == 0) 1.0 else 0.0;
        row[E + e] = 1.0; // fully supervised
        row[schema_off + e] = @floatFromInt(sample_idx * L + schemaTokenPos(e));
        row[row_off + e] = @floatFromInt(flat_span_idx);
        // Every span here is valid, so the instance ordinal is the span index.
        row[count_off + e] = @floatFromInt(span_idx * BATCH * E + (sample_idx * E + e));
    }
    row[start_off] = @floatFromInt(sample_idx * L + start_tok);
    row[start_off + 1] = @floatFromInt(sample_idx * L + end_tok);
}

const Batch = struct {
    allocator: std.mem.Allocator,
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
    targets_shape: Shape,

    fn deinit(self: *Batch) void {
        self.allocator.free(self.input_ids);
        self.allocator.free(self.attention_mask);
        self.allocator.free(self.targets);
    }

    fn inputs(self: *const Batch) StepInputs {
        return .{
            .input_ids = self.input_ids,
            .attention_mask = self.attention_mask,
            .targets = self.targets,
            .targets_shape = self.targets_shape,
        };
    }
};

fn buildBatch(allocator: std.mem.Allocator, objective: gliner2_autodiff.GlinerObjective) !Batch {
    const total_tokens = BATCH * SEQ_LEN;
    const input_ids = try allocator.alloc(i64, total_tokens);
    errdefer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(f32, total_tokens);
    errdefer allocator.free(attention_mask);
    fillInputs(input_ids, attention_mask);

    const E: usize = NUM_ENTITY_TYPES;
    const rows: usize = BATCH * MAX_SPANS;

    switch (objective) {
        .token => {
            const shape = gliner2_autodiff.tokenTargetsShape(BATCH, SEQ_LEN, NUM_CLASSES);
            const targets = try allocator.alloc(f32, total_tokens * NUM_CLASSES);
            errdefer allocator.free(targets);
            @memset(targets, 0.0);
            for (0..total_tokens) |t| {
                // Padding rows stay all-zero, which the token loss treats as
                // ignored — same as the real data path.
                if (t % SEQ_LEN >= VALID_TOKENS) continue;
                targets[t * NUM_CLASSES + (t % NUM_CLASSES)] = 1.0;
            }
            return .{
                .allocator = allocator,
                .input_ids = input_ids,
                .attention_mask = attention_mask,
                .targets = targets,
                .targets_shape = shape,
            };
        },
        .span_start => {
            const width = gliner2_autodiff.spanStartTargetWidth(E);
            const shape = gliner2_autodiff.spanStartTargetsShape(BATCH, MAX_SPANS, NUM_ENTITY_TYPES);
            const targets = try allocator.alloc(f32, rows * width);
            errdefer allocator.free(targets);
            @memset(targets, 0.0);
            for (0..rows) |row_idx| {
                fillSpanSection(
                    targets[row_idx * width ..][0..width],
                    row_idx / MAX_SPANS,
                    row_idx % MAX_SPANS,
                );
            }
            return .{
                .allocator = allocator,
                .input_ids = input_ids,
                .attention_mask = attention_mask,
                .targets = targets,
                .targets_shape = shape,
            };
        },
        .gliner2_total_loss => {
            const span_width = gliner2_autodiff.spanStartTargetWidth(E);
            const width = gliner2_autodiff.gliner2TotalLossTargetWidth(E);
            const classification_labels_off = gliner2_autodiff.gliner2TotalLossClassificationLabelsOffset(E);
            const classification_mask_off = gliner2_autodiff.gliner2TotalLossClassificationMaskOffset(E);
            const count_labels_off = gliner2_autodiff.gliner2TotalLossCountLabelsOffset(E);
            const count_mask_off = gliner2_autodiff.gliner2TotalLossCountMaskOffset(E);
            const parent_idx_off = gliner2_autodiff.gliner2TotalLossParentIndexOffset(E);
            const active_off = gliner2_autodiff.gliner2TotalLossActiveFieldsOffset(E, 1);
            const shape = gliner2_autodiff.gliner2TotalLossTargetsShape(BATCH, MAX_SPANS, NUM_ENTITY_TYPES);
            const targets = try allocator.alloc(f32, rows * width);
            errdefer allocator.free(targets);
            @memset(targets, 0.0);
            for (0..rows) |row_idx| {
                const row = targets[row_idx * width ..][0..width];
                fillSpanSection(row[0..span_width], row_idx / MAX_SPANS, row_idx % MAX_SPANS);
                // Keep every full-task loss branch live. Classification uses
                // mixed positive/negative field labels, while count uses a
                // valid one-hot target and parent token for every schema row.
                for (0..E) |field| {
                    row[classification_labels_off + field] = if ((row_idx + field) % 2 == 0) 1.0 else 0.0;
                    row[classification_mask_off + field] = 1.0;
                }
                row[count_labels_off + (row_idx % 5)] = 1.0;
                row[count_mask_off] = 1.0;
                row[parent_idx_off] = @floatFromInt((row_idx / MAX_SPANS) * SEQ_LEN);
                // The count-embed active mask is independent of the structure
                // BCE mask and scores every field in the synthetic schema.
                @memset(row[active_off..][0..E], 1.0);
            }
            return .{
                .allocator = allocator,
                .input_ids = input_ids,
                .attention_mask = attention_mask,
                .targets = targets,
                .targets_shape = shape,
            };
        },
    }
}

// ── The gate ─────────────────────────────────────────────────────────────────

fn runBackendGradParity(
    allocator: std.mem.Allocator,
    label: []const u8,
    objective: gliner2_autodiff.GlinerObjective,
) !void {
    if (comptime !build_options.enable_metal) {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_METAL_TESTS", false)) {
            std.debug.print("FAIL [{s}]: Metal is required but this build has Metal disabled\n", .{label});
            return error.RequiredMetalBuildDisabled;
        }
        std.debug.print("SKIP [{s}]: build has Metal disabled\n", .{label});
        return error.SkipZigTest;
    }
    if (!metal_runtime.metalDeviceAvailable()) {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_METAL_TESTS", false)) {
            std.debug.print("FAIL [{s}]: Metal is required but no Metal device is available\n", .{label});
            return error.RequiredMetalDeviceUnavailable;
        }
        std.debug.print("SKIP [{s}]: no Metal device available\n", .{label});
        return error.SkipZigTest;
    }

    const config = gliner2_autodiff.GlinerAutodiffConfig{
        .graph_config = graph_config,
        .num_classes = NUM_CLASSES,
        .objective = objective,
        .span_start_loss = .bce,
        .span_start_loss_reduction = .sum,
        .span_start_positive_weight = 1.0,
        .span_start_negative_weight = 1.0,
    };

    var batch = try buildBatch(allocator, objective);
    defer batch.deinit();

    const specs = try discoverParams(allocator, config, batch.targets_shape);
    defer freeSpecs(allocator, specs);

    // ── native ──
    var native_run = blk: {
        var store = try NativeStore.init(allocator, specs);
        defer store.deinit();
        var compute = NativeCompute.init(allocator, &store.store, null);
        var cb = compute.computeBackend();
        break :blk try runOneStep(allocator, &cb, config, batch.inputs());
    };
    defer native_run.deinit();

    // ── metal ──
    var metal_run = blk: {
        var store = try MetalStore.init(allocator, specs);
        defer store.deinit();
        var compute = try metal_compute.MetalCompute.init(allocator, &store.store, null);
        defer compute.deinit();
        var cb = compute.computeBackend();
        if (cb.kind() != .metal) return error.MetalBackendNotSelected;
        break :blk try runOneStep(allocator, &cb, config, batch.inputs());
    };
    defer metal_run.deinit();

    // Liveness: if the "Metal" run never allocated a device tensor it silently
    // ran on the host fallback and this whole comparison would be native vs
    // native — a gate that always passes. Refuse to report that as a pass.
    if (metal_run.device_peak_bytes == 0) {
        std.debug.print(
            "FAIL [{s}]: Metal run allocated no device tensors; the comparison would be vacuous\n",
            .{label},
        );
        return error.MetalRunDidNotUseDevice;
    }

    try std.testing.expect(std.math.isFinite(native_run.loss));
    try std.testing.expect(std.math.isFinite(metal_run.loss));
    try std.testing.expect(std.math.isFinite(native_run.grad_norm));
    try std.testing.expect(std.math.isFinite(metal_run.grad_norm));

    const loss_rel = relDelta(native_run.loss, metal_run.loss);
    const norm_rel = relDelta(native_run.grad_norm, metal_run.grad_norm);

    std.debug.print(
        \\
        \\── GLiNER2 backward parity [{s}] ──────────────────────────────
        \\  trainables      : {d}
        \\  loss            : native {d:.6}  metal {d:.6}   rel {e:.3}
        \\  grad_norm       : native {d:.6}  metal {d:.6}   rel {e:.3}
        \\
    , .{
        label,
        native_run.capture.names.items.len,
        native_run.loss,
        metal_run.loss,
        loss_rel,
        native_run.grad_norm,
        metal_run.grad_norm,
        norm_rel,
    });

    if (native_run.capture.names.items.len != metal_run.capture.names.items.len) {
        std.debug.print(
            "FAIL [{s}]: trainable count differs (native {d}, metal {d})\n",
            .{ label, native_run.capture.names.items.len, metal_run.capture.names.items.len },
        );
        return error.TrainableSetMismatch;
    }

    var worst_l2 = ParamDelta{
        .name = "<none>",
        .rel_l2 = -1.0,
        .rel_max = 0.0,
        .native_l2 = 0.0,
        .metal_l2 = 0.0,
        .elems = 0,
    };
    var worst_max = worst_l2;
    var failures: usize = 0;

    for (native_run.capture.names.items, native_run.capture.grads.items) |name, native_grad| {
        const metal_grad = metal_run.capture.find(name) orelse {
            std.debug.print("FAIL [{s}]: metal run has no gradient for '{s}'\n", .{ label, name });
            return error.TrainableSetMismatch;
        };
        if (native_grad.len != metal_grad.len) {
            std.debug.print(
                "FAIL [{s}]: gradient length differs for '{s}' (native {d}, metal {d})\n",
                .{ label, name, native_grad.len, metal_grad.len },
            );
            return error.GradientShapeMismatch;
        }

        const delta = compareBlocks(name, native_grad, metal_grad);
        if (delta.rel_l2 > worst_l2.rel_l2) worst_l2 = delta;
        if (delta.rel_max > worst_max.rel_max) worst_max = delta;
        if (delta.rel_l2 > TOL_REL_L2 or delta.rel_max > TOL_REL_MAX) {
            failures += 1;
            std.debug.print(
                "  DIVERGED  {s:<58} n={d:<8} rel_l2={e:.4} rel_max={e:.4} |g|_native={e:.4} |g|_metal={e:.4}\n",
                .{ delta.name, delta.elems, delta.rel_l2, delta.rel_max, delta.native_l2, delta.metal_l2 },
            );
        }
    }

    std.debug.print(
        \\  worst rel_l2    : {e:.4}  ({s})
        \\  worst rel_max   : {e:.4}  ({s})
        \\  tolerance       : rel_l2 <= {e:.1}, rel_max <= {e:.1}
        \\  diverged params : {d} / {d}
        \\
    , .{
        worst_l2.rel_l2,
        worst_l2.name,
        worst_max.rel_max,
        worst_max.name,
        TOL_REL_L2,
        TOL_REL_MAX,
        failures,
        native_run.capture.names.items.len,
    });

    if (failures != 0) {
        std.debug.print(
            \\FAIL [{s}]: {d} parameter gradient(s) diverge between native and Metal.
            \\  worst offender : {s}
            \\    relative L2  : {e:.6}  (tolerance {e:.1})
            \\    relative max : {e:.6}  (tolerance {e:.1})
            \\    ‖g‖ native   : {e:.6}
            \\    ‖g‖ metal    : {e:.6}
            \\  grad_norm      : native {d:.6} vs metal {d:.6} ({d:.2}% apart)
            \\  The forward agrees to {e:.3} relative, so this is a BACKWARD-only
            \\  divergence: a forward/loss parity check cannot see it.
            \\
        , .{
            label,
            failures,
            worst_l2.name,
            worst_l2.rel_l2,
            TOL_REL_L2,
            worst_l2.rel_max,
            TOL_REL_MAX,
            worst_l2.native_l2,
            worst_l2.metal_l2,
            native_run.grad_norm,
            metal_run.grad_norm,
            norm_rel * 100.0,
            loss_rel,
        });
        return error.BackendGradientParityViolated;
    }

    std.debug.print(
        "PASS [{s}]: {d} parameter gradients agree; worst rel_l2 {e:.4} on {s}\n",
        .{ label, native_run.capture.names.items.len, worst_l2.rel_l2, worst_l2.name },
    );
}

// ── Tests ────────────────────────────────────────────────────────────────────

// Control. This objective is reported to agree to 7.9e-7 between backends, so
// it establishes the measured noise floor and proves the harness itself is not
// manufacturing a difference.
test "GLiNER2 backend grad parity: token objective backward matches native and Metal" {
    if (platform.env.getenv("TERMITE_CUDA_PARITY_TEST_FILTER") != null) return error.SkipZigTest;
    try runBackendGradParity(std.testing.allocator, "token", .token);
}

test "GLiNER2 backend grad parity: span-start objective backward matches native and Metal" {
    if (platform.env.getenv("TERMITE_CUDA_PARITY_TEST_FILTER") != null) return error.SkipZigTest;
    try runBackendGradParity(std.testing.allocator, "span-start", .span_start);
}

// The gate proper. The structure term of `gliner2-total-loss` previously
// exposed a large native-vs-Metal backward divergence; keep the strict
// tolerance so this remains a regression gate for the fixed kernel path.
test "GLiNER2 backend grad parity: gliner2-total-loss objective backward matches native and Metal" {
    if (platform.env.getenv("TERMITE_CUDA_PARITY_TEST_FILTER") != null) return error.SkipZigTest;
    try runBackendGradParity(std.testing.allocator, "gliner2-total-loss", .gliner2_total_loss);
}

fn runCudaGradParity(
    allocator: std.mem.Allocator,
    label: []const u8,
    objective: gliner2_autodiff.GlinerObjective,
) !void {
    if (comptime !build_options.enable_cuda) {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return error.RequiredCudaBuildDisabled;
        return error.SkipZigTest;
    }
    const config = gliner2_autodiff.GlinerAutodiffConfig{
        .graph_config = graph_config,
        .num_classes = NUM_CLASSES,
        .objective = objective,
        .span_start_loss = .bce,
        .span_start_loss_reduction = .sum,
        .span_start_positive_weight = 1.0,
        .span_start_negative_weight = 1.0,
    };
    var batch = try buildBatch(allocator, objective);
    defer batch.deinit();
    const specs = try discoverParams(allocator, config, batch.targets_shape);
    defer freeSpecs(allocator, specs);

    var native_step_ns: u64 = 0;
    var native_run = blk: {
        var store = try NativeStore.init(allocator, specs);
        defer store.deinit();
        var compute = NativeCompute.init(allocator, &store.store, null);
        var cb = compute.computeBackend();
        const started_ns = monotonicNowNs();
        const run = try runOneStep(allocator, &cb, config, batch.inputs());
        native_step_ns = monotonicNowNs() -| started_ns;
        break :blk run;
    };
    defer native_run.deinit();

    var cuda_step_ns: u64 = 0;
    var cuda_run = blk: {
        var store = try NativeStore.init(allocator, specs);
        defer store.deinit();
        var compute = cuda_compute.CudaCompute.init(allocator) catch |err| {
            if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return err;
            return error.SkipZigTest;
        };
        defer compute.deinit();
        try compute.requireProfile(.gliner2_training);
        var it = store.store.resident_weights.iterator();
        while (it.next()) |entry| {
            const owned_name = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(owned_name);
            try compute.insertWeightFromLoaded(owned_name, entry.value_ptr);
        }
        var cb = compute.computeBackend();
        if (cb.kind() != .cuda) return error.CudaBackendNotSelected;
        const started_ns = monotonicNowNs();
        const run = try runOneStep(allocator, &cb, config, batch.inputs());
        cuda_step_ns = monotonicNowNs() -| started_ns;
        break :blk run;
    };
    defer cuda_run.deinit();

    try std.testing.expect(std.math.isFinite(native_run.loss));
    try std.testing.expect(std.math.isFinite(cuda_run.loss));
    try std.testing.expect(std.math.isFinite(native_run.grad_norm));
    try std.testing.expect(std.math.isFinite(cuda_run.grad_norm));
    if (native_run.capture.names.items.len != cuda_run.capture.names.items.len) return error.TrainableSetMismatch;

    var worst_l2 = ParamDelta{
        .name = "<none>",
        .rel_l2 = -1.0,
        .rel_max = 0.0,
        .native_l2 = 0.0,
        .metal_l2 = 0.0,
        .elems = 0,
    };
    var worst_max = worst_l2;
    var failures: usize = 0;
    for (native_run.capture.names.items, native_run.capture.grads.items) |name, native_grad| {
        const cuda_grad = cuda_run.capture.find(name) orelse return error.TrainableSetMismatch;
        if (native_grad.len != cuda_grad.len) return error.GradientShapeMismatch;
        const delta = compareBlocks(name, native_grad, cuda_grad);
        if (delta.rel_l2 > worst_l2.rel_l2) worst_l2 = delta;
        if (delta.rel_max > worst_max.rel_max) worst_max = delta;
        if (delta.rel_l2 > TOL_REL_L2 or delta.rel_max > TOL_REL_MAX) {
            failures += 1;
            std.debug.print(
                "  CUDA DIVERGED  {s:<58} n={d:<8} rel_l2={e:.4} rel_max={e:.4}\n",
                .{ delta.name, delta.elems, delta.rel_l2, delta.rel_max },
            );
        }
    }
    const loss_rel = relDelta(native_run.loss, cuda_run.loss);
    const norm_rel = relDelta(native_run.grad_norm, cuda_run.grad_norm);
    std.debug.print(
        \\── GLiNER2 native/CUDA gradient parity [{s}] ────────────────
        \\  trainables      : {d}
        \\  loss            : native {d:.6}  cuda {d:.6}   rel {e:.3}
        \\  grad_norm       : native {d:.6}  cuda {d:.6}   rel {e:.3}
        \\  step runtime ms : native {d:.3}  cuda {d:.3}   speedup {d:.2}x
        \\  worst rel_l2    : {e:.4}  ({s})
        \\  worst rel_max   : {e:.4}  ({s})
        \\
    , .{
        label,
        native_run.capture.names.items.len,
        native_run.loss,
        cuda_run.loss,
        loss_rel,
        native_run.grad_norm,
        cuda_run.grad_norm,
        norm_rel,
        @as(f64, @floatFromInt(native_step_ns)) / @as(f64, std.time.ns_per_ms),
        @as(f64, @floatFromInt(cuda_step_ns)) / @as(f64, std.time.ns_per_ms),
        if (cuda_step_ns == 0) 0.0 else @as(f64, @floatFromInt(native_step_ns)) / @as(f64, @floatFromInt(cuda_step_ns)),
        worst_l2.rel_l2,
        worst_l2.name,
        worst_max.rel_max,
        worst_max.name,
    });
    if (loss_rel > TOL_REL_L2 or failures != 0) return error.BackendGradientParityViolated;
}

test "GLiNER2 CUDA gradient parity: token objective" {
    if (!cudaParityTestSelected("gradient-token")) return error.SkipZigTest;
    try runCudaGradParity(std.testing.allocator, "token", .token);
}

test "GLiNER2 CUDA gradient parity: span-start objective" {
    if (!cudaParityTestSelected("gradient-span")) return error.SkipZigTest;
    try runCudaGradParity(std.testing.allocator, "span-start", .span_start);
}

test "GLiNER2 CUDA gradient parity: full total-loss objective" {
    if (!cudaParityTestSelected("gradient-full")) return error.SkipZigTest;
    try runCudaGradParity(std.testing.allocator, "gliner2-total-loss", .gliner2_total_loss);
}

test "GLiNER2 CUDA packed DeBERTa attention forward and VJP match native" {
    if (!cudaParityTestSelected("attention")) return error.SkipZigTest;
    if (comptime !build_options.enable_cuda) {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return error.RequiredCudaBuildDisabled;
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const batch: usize = 2;
    const seq_len: usize = 128;
    const num_heads: usize = 12;
    const head_dim: usize = 64;
    const hidden = num_heads * head_dim;
    const token_count = batch * seq_len * hidden;
    const rel_count = (2 * seq_len - 1) * hidden;
    const pair_count = batch * num_heads * seq_len * seq_len;

    const q = try allocator.alloc(f32, token_count);
    defer allocator.free(q);
    const k = try allocator.alloc(f32, token_count);
    defer allocator.free(k);
    const v = try allocator.alloc(f32, token_count);
    defer allocator.free(v);
    const q_r = try allocator.alloc(f32, rel_count);
    defer allocator.free(q_r);
    const k_r = try allocator.alloc(f32, rel_count);
    defer allocator.free(k_r);
    const d_out = try allocator.alloc(f32, token_count);
    defer allocator.free(d_out);
    for (q, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 19)) - 9)) * 0.03125;
    for (k, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 17)) - 8)) * 0.02734375;
    for (v, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 13)) - 6)) * 0.0390625;
    for (q_r, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 23)) - 11)) * 0.01953125;
    for (k_r, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 29)) - 14)) * 0.015625;
    for (d_out, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 11)) - 5)) * 0.046875;
    const mask = try allocator.alloc(i64, batch * seq_len);
    defer allocator.free(mask);
    for (0..batch) |b| {
        const valid = if (b == 0) seq_len - 32 else seq_len - 47;
        for (0..seq_len) |i| mask[b * seq_len + i] = @intFromBool(i < valid);
    }

    const qkv = try allocator.alloc(f32, token_count * 3);
    defer allocator.free(qkv);
    @memcpy(qkv[0..token_count], q);
    @memcpy(qkv[token_count .. 2 * token_count], k);
    @memcpy(qkv[2 * token_count .. 3 * token_count], v);
    const qr_kr = try allocator.alloc(f32, rel_count * 2);
    defer allocator.free(qr_kr);
    @memcpy(qr_kr[0..rel_count], q_r);
    @memcpy(qr_kr[rel_count .. 2 * rel_count], k_r);
    const attention_bias = try allocator.alloc(f32, pair_count);
    defer allocator.free(attention_bias);
    for (0..batch) |b| {
        for (0..num_heads) |h| {
            for (0..seq_len) |qi| {
                for (0..seq_len) |ki| {
                    const idx = ((b * num_heads + h) * seq_len + qi) * seq_len + ki;
                    attention_bias[idx] = if (mask[b * seq_len + ki] != 0) 0.0 else -1.0e9;
                }
            }
        }
    }

    var native_store = try NativeStore.init(allocator, &.{});
    defer native_store.deinit();
    var native_compute_instance = NativeCompute.init(allocator, &native_store.store, null);
    var native_cb = native_compute_instance.computeBackend();
    const token_shape = [_]i32{ @intCast(batch * seq_len), @intCast(hidden) };
    const rel_shape = [_]i32{ @intCast(2 * seq_len - 1), @intCast(hidden) };
    const native_q = try native_cb.fromFloat32Shape(q, &token_shape);
    defer native_cb.free(native_q);
    const native_k = try native_cb.fromFloat32Shape(k, &token_shape);
    defer native_cb.free(native_k);
    const native_v = try native_cb.fromFloat32Shape(v, &token_shape);
    defer native_cb.free(native_v);
    const native_qr = try native_cb.fromFloat32Shape(q_r, &rel_shape);
    defer native_cb.free(native_qr);
    const native_kr = try native_cb.fromFloat32Shape(k_r, &rel_shape);
    defer native_cb.free(native_kr);
    const native_dout = try native_cb.fromFloat32Shape(d_out, &token_shape);
    defer native_cb.free(native_dout);
    const native_forward = try native_cb.disentangledRelativeAttention(native_q, native_k, native_v, native_qr, native_kr, mask, batch, seq_len, num_heads, head_dim);
    defer native_cb.free(native_forward);
    const native_backward = try native_cb.disentangledRelativeAttentionBackward(native_q, native_k, native_v, native_qr, native_kr, mask, native_dout, batch, seq_len, num_heads, head_dim);
    defer native_cb.free(native_backward);
    const expected_forward = try native_cb.toFloat32(native_forward, allocator);
    defer allocator.free(expected_forward);
    const expected_backward = try native_cb.toFloat32(native_backward, allocator);
    defer allocator.free(expected_backward);

    var cuda_compute_instance = cuda_compute.CudaCompute.init(allocator) catch |err| {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return err;
        return error.SkipZigTest;
    };
    defer cuda_compute_instance.deinit();
    try cuda_compute_instance.requireProfile(.gliner2_training);
    var cuda_cb = cuda_compute_instance.computeBackend();
    const qkv_shape = [_]i32{ @intCast(3 * batch * seq_len), @intCast(hidden) };
    const qrkr_shape = [_]i32{ @intCast(2 * (2 * seq_len - 1)), @intCast(hidden) };
    const bias_shape = [_]i32{ @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(seq_len) };
    const cuda_qkv = try cuda_cb.fromFloat32Shape(qkv, &qkv_shape);
    defer cuda_cb.free(cuda_qkv);
    const cuda_qrkr = try cuda_cb.fromFloat32Shape(qr_kr, &qrkr_shape);
    defer cuda_cb.free(cuda_qrkr);
    const cuda_bias = try cuda_cb.fromFloat32Shape(attention_bias, &bias_shape);
    defer cuda_cb.free(cuda_bias);
    const cuda_dout = try cuda_cb.fromFloat32Shape(d_out, &token_shape);
    defer cuda_cb.free(cuda_dout);
    const cuda_forward = (try cuda_cb.disentangledRelativeAttentionPacked(cuda_qkv, cuda_qrkr, cuda_bias, batch, seq_len, num_heads, head_dim)) orelse return error.CudaPackedAttentionUnavailable;
    defer cuda_cb.free(cuda_forward);
    const cuda_forward_repeat = (try cuda_cb.disentangledRelativeAttentionPacked(cuda_qkv, cuda_qrkr, cuda_bias, batch, seq_len, num_heads, head_dim)) orelse return error.CudaPackedAttentionUnavailable;
    defer cuda_cb.free(cuda_forward_repeat);
    const cuda_backward = (try cuda_cb.disentangledRelativeAttentionBackwardPacked(cuda_qkv, cuda_qrkr, cuda_bias, cuda_dout, batch, seq_len, num_heads, head_dim)) orelse return error.CudaPackedAttentionBackwardUnavailable;
    defer cuda_cb.free(cuda_backward);
    const actual_forward = try cuda_cb.toFloat32(cuda_forward, allocator);
    defer allocator.free(actual_forward);
    const repeat_forward = try cuda_cb.toFloat32(cuda_forward_repeat, allocator);
    defer allocator.free(repeat_forward);
    const actual_backward = try cuda_cb.toFloat32(cuda_backward, allocator);
    defer allocator.free(actual_backward);

    const forward_delta = compareBlocks("attention.forward", expected_forward, actual_forward);
    const repeat_delta = compareBlocks("attention.forward.repeat", actual_forward, repeat_forward);
    const backward_delta = compareBlocks("attention.backward", expected_backward, actual_backward);
    std.debug.print(
        "CUDA packed attention parity: forward rel_l2={e:.4} rel_max={e:.4}; repeat rel_l2={e:.4} rel_max={e:.4}; VJP rel_l2={e:.4} rel_max={e:.4}\n",
        .{ forward_delta.rel_l2, forward_delta.rel_max, repeat_delta.rel_l2, repeat_delta.rel_max, backward_delta.rel_l2, backward_delta.rel_max },
    );
    try std.testing.expectEqualSlices(f32, actual_forward, repeat_forward);
    try std.testing.expect(forward_delta.rel_l2 <= TOL_REL_L2);
    try std.testing.expect(forward_delta.rel_max <= TOL_REL_MAX);
    try std.testing.expect(backward_delta.rel_l2 <= TOL_REL_L2);
    try std.testing.expect(backward_delta.rel_max <= TOL_REL_MAX);
}

test "GLiNER2 CUDA compiled-device trainer updates resident LoRA weights" {
    if (!cudaParityTestSelected("compiled-update")) return error.SkipZigTest;
    if (comptime !build_options.enable_cuda) {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return error.RequiredCudaBuildDisabled;
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const config = gliner2_autodiff.GlinerAutodiffConfig{
        .graph_config = graph_config,
        .num_classes = NUM_CLASSES,
        .objective = .gliner2_total_loss,
        .span_start_loss = .bce,
        .span_start_loss_reduction = .sum,
    };
    var batch = try buildBatch(allocator, .gliner2_total_loss);
    defer batch.deinit();
    const specs = try discoverParams(allocator, config, batch.targets_shape);
    defer freeSpecs(allocator, specs);
    var store = try NativeStore.init(allocator, specs);
    defer store.deinit();
    var compute = cuda_compute.CudaCompute.init(allocator) catch |err| {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return err;
        return error.SkipZigTest;
    };
    defer compute.deinit();
    try compute.requireProfile(.gliner2_training);
    var weight_it = store.store.resident_weights.iterator();
    while (weight_it.next()) |entry| {
        const owned_name = try allocator.dupe(u8, entry.key_ptr.*);
        errdefer allocator.free(owned_name);
        try compute.insertWeightFromLoaded(owned_name, entry.value_ptr);
    }
    var cb = compute.computeBackend();
    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(config);
    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, &cb, .{
        .lora = .{
            .rank = 4,
            .alpha = 8.0,
            .target_patterns = &lora_targets,
        },
        .lr_schedule = .{ .constant = 1.0e-3 },
        .max_grad_norm = 1.0,
        .grad_accum_steps = 1,
        .lora_a_init_std = 0.02,
        .hidden_size_hint = HIDDEN,
        .num_layers_hint = NUM_LAYERS,
        .seed = TRAINER_SEED,
        .regular_trainable_params = &.{},
        .execution_engine = .compiled_device,
        .compiled_required = true,
    });
    defer trainer.deinit();

    const input = gliner2_autodiff.makeTrainerInput(
        &gliner_ctx,
        batch.input_ids,
        batch.attention_mask,
        batch.targets,
        batch.targets_shape,
        BATCH,
        SEQ_LEN,
    );
    try trainer.ensureGraphBuilt(input);
    seedLoraSlots(&trainer);
    try trainer.prepareDeviceTraining();
    if (trainer.lora_params.items.len == 0) return error.NoTrainableGradientsCaptured;
    const before = try allocator.dupe(f32, trainer.lora_params.items[0].weights);
    defer allocator.free(before);
    var before_step_logits = try gliner2_autodiff.gliner2EvalLogitsForBatch(
        allocator,
        &trainer,
        &gliner_ctx,
        batch.input_ids,
        batch.attention_mask,
        batch.targets,
        batch.targets_shape,
        BATCH,
        SEQ_LEN,
    );
    defer before_step_logits.deinit(allocator);

    const result = try trainer.step(input);
    try std.testing.expect(result.optimizer_stepped);
    try std.testing.expectEqual(real_autodiff.OptimizerBackend.cuda, result.profile.optimizer_backend);
    try std.testing.expect(result.profile.device_trainable_bytes > 0);
    // The public profile is a complete step boundary, not merely the graph
    // executor interval: all three changing batch inputs plus the derived
    // attention bias must be classified and included in total H2D traffic.
    try std.testing.expect(result.profile.cuda_training_input_uploads >= 4);
    try std.testing.expect(result.profile.cuda_training_input_upload_bytes > 0);
    try std.testing.expect(result.profile.cuda_h2d_bytes >= result.profile.cuda_training_input_upload_bytes);
    if (result.profile.device_resident_transfer_count != 0) return error.UnexpectedDeviceResidentTransfer;
    if (result.profile.cuda_upload_synchronizations > 1) return error.UnexpectedCudaUploadSynchronization;
    try std.testing.expect(std.math.isFinite(result.loss));
    try std.testing.expect(std.math.isFinite(result.grad_norm));

    // A single cold-start synchronization is permitted while the first
    // compiled optimizer graph becomes resident. Every subsequent step must
    // remain fully asynchronous and keep parameter/optimizer state resident.
    const hot_result = try trainer.step(input);
    try std.testing.expect(hot_result.optimizer_stepped);
    if (hot_result.profile.device_resident_transfer_count != 0) return error.UnexpectedDeviceResidentTransfer;
    if (hot_result.profile.cuda_upload_synchronizations != 0) return error.UnexpectedCudaUploadSynchronization;
    try std.testing.expect(hot_result.profile.cuda_training_input_uploads >= 4);
    try std.testing.expect(hot_result.profile.cuda_training_input_upload_bytes > 0);
    try std.testing.expect(hot_result.profile.cuda_h2d_bytes >= hot_result.profile.cuda_training_input_upload_bytes);
    var eval_logits = try gliner2_autodiff.gliner2EvalLogitsForBatch(
        allocator,
        &trainer,
        &gliner_ctx,
        batch.input_ids,
        batch.attention_mask,
        batch.targets,
        batch.targets_shape,
        BATCH,
        SEQ_LEN,
    );
    defer eval_logits.deinit(allocator);
    try std.testing.expect(eval_logits.span.len > 0);
    try std.testing.expect(eval_logits.classification.len > 0);
    try std.testing.expect(eval_logits.count.len > 0);
    for (eval_logits.span) |value| try std.testing.expect(std.math.isFinite(value));
    for (eval_logits.classification) |value| try std.testing.expect(std.math.isFinite(value));
    for (eval_logits.count) |value| try std.testing.expect(std.math.isFinite(value));

    var logits_changed = false;
    for (before_step_logits.span, eval_logits.span) |old, new| {
        if (@abs(old - new) > 1.0e-7) {
            logits_changed = true;
            break;
        }
    }
    if (!logits_changed) {
        for (before_step_logits.classification, eval_logits.classification) |old, new| {
            if (@abs(old - new) > 1.0e-7) {
                logits_changed = true;
                break;
            }
        }
    }
    if (!logits_changed) {
        for (before_step_logits.count, eval_logits.count) |old, new| {
            if (@abs(old - new) > 1.0e-7) {
                logits_changed = true;
                break;
            }
        }
    }
    try std.testing.expect(logits_changed);

    try trainer.syncDeviceTrainablesToHost();
    const lora_devices = try allocator.alloc(?real_autodiff.RealAutodiffTrainer.DeviceOptimizerSlot, trainer.lora_params.items.len);
    defer allocator.free(lora_devices);
    const regular_devices = try allocator.alloc(?real_autodiff.RealAutodiffTrainer.DeviceOptimizerSlot, trainer.regular_params.items.len);
    defer allocator.free(regular_devices);
    for (trainer.lora_params.items, lora_devices) |*slot, *saved| {
        saved.* = slot.device;
        slot.device = null;
    }
    for (trainer.regular_params.items, regular_devices) |*slot, *saved| {
        saved.* = slot.device;
        slot.device = null;
    }
    defer {
        for (trainer.lora_params.items, lora_devices) |*slot, saved| slot.device = saved;
        for (trainer.regular_params.items, regular_devices) |*slot, saved| slot.device = saved;
    }
    var host_synced_logits = try gliner2_autodiff.gliner2EvalLogitsForBatch(
        allocator,
        &trainer,
        &gliner_ctx,
        batch.input_ids,
        batch.attention_mask,
        batch.targets,
        batch.targets_shape,
        BATCH,
        SEQ_LEN,
    );
    defer host_synced_logits.deinit(allocator);
    for (eval_logits.span, host_synced_logits.span) |resident, host_synced| {
        try std.testing.expectApproxEqAbs(resident, host_synced, 1.0e-6);
    }
    for (eval_logits.classification, host_synced_logits.classification) |resident, host_synced| {
        try std.testing.expectApproxEqAbs(resident, host_synced, 1.0e-6);
    }
    for (eval_logits.count, host_synced_logits.count) |resident, host_synced| {
        try std.testing.expectApproxEqAbs(resident, host_synced, 1.0e-6);
    }

    var changed = false;
    for (before, trainer.lora_params.items[0].weights) |old, new| {
        if (old != new) {
            changed = true;
            break;
        }
    }
    try std.testing.expect(changed);
}

test "GLiNER2 CUDA checkpoint resume preserves resident AdamW trajectory" {
    if (!cudaParityTestSelected("checkpoint-resume")) return error.SkipZigTest;
    if (comptime !build_options.enable_cuda) {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return error.RequiredCudaBuildDisabled;
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const config = gliner2_autodiff.GlinerAutodiffConfig{
        .graph_config = graph_config,
        .num_classes = NUM_CLASSES,
        .objective = .token,
    };
    var batch = try buildBatch(allocator, .token);
    defer batch.deinit();
    const specs = try discoverParams(allocator, config, batch.targets_shape);
    defer freeSpecs(allocator, specs);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const resume_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/cuda_resume.safetensors", .{tmp.sub_path});
    defer allocator.free(resume_path);
    const expected_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/cuda_expected.safetensors", .{tmp.sub_path});
    defer allocator.free(expected_path);
    const actual_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/cuda_actual.safetensors", .{tmp.sub_path});
    defer allocator.free(actual_path);
    const fingerprint = @as([32]u8, @splat(0xC7));

    var uninterrupted_loss: f32 = undefined;
    var uninterrupted_norm: f32 = undefined;
    {
        var store = try NativeStore.init(allocator, specs);
        defer store.deinit();
        var compute = cuda_compute.CudaCompute.init(allocator) catch |err| {
            if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return err;
            return error.SkipZigTest;
        };
        defer compute.deinit();
        try compute.requireProfile(.gliner2_training);
        var weight_it = store.store.resident_weights.iterator();
        while (weight_it.next()) |entry| {
            const owned_name = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(owned_name);
            try compute.insertWeightFromLoaded(owned_name, entry.value_ptr);
        }
        var cb = compute.computeBackend();
        var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(config);
        var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, &cb, .{
            .lora = .{ .rank = 4, .alpha = 8.0, .target_patterns = &lora_targets },
            .lr_schedule = .{ .constant = 1.0e-3 },
            .max_grad_norm = 1.0,
            .grad_accum_steps = 1,
            .lora_a_init_std = 0.02,
            .hidden_size_hint = HIDDEN,
            .num_layers_hint = NUM_LAYERS,
            .seed = TRAINER_SEED,
            .regular_trainable_params = &.{},
            .execution_engine = .compiled_device,
            .compiled_required = true,
        });
        defer trainer.deinit();
        const input = gliner2_autodiff.makeTrainerInput(
            &gliner_ctx,
            batch.input_ids,
            batch.attention_mask,
            batch.targets,
            batch.targets_shape,
            BATCH,
            SEQ_LEN,
        );
        try trainer.ensureGraphBuilt(input);
        seedLoraSlots(&trainer);
        const first = try trainer.step(input);
        try std.testing.expect(first.optimizer_stepped);
        try trainer.saveTrainingState(resume_path, &fingerprint, null);
        const second = try trainer.step(input);
        uninterrupted_loss = second.loss;
        uninterrupted_norm = second.grad_norm;
        try std.testing.expectEqual(@as(u64, 2), trainer.microBatchSteps());
        try std.testing.expectEqual(@as(u64, 2), trainer.optimizerSteps());
        try trainer.saveTrainingState(expected_path, &fingerprint, null);
    }

    {
        var store = try NativeStore.init(allocator, specs);
        defer store.deinit();
        var compute = try cuda_compute.CudaCompute.init(allocator);
        defer compute.deinit();
        try compute.requireProfile(.gliner2_training);
        var weight_it = store.store.resident_weights.iterator();
        while (weight_it.next()) |entry| {
            const owned_name = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(owned_name);
            try compute.insertWeightFromLoaded(owned_name, entry.value_ptr);
        }
        var cb = compute.computeBackend();
        var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(config);
        var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, &cb, .{
            .lora = .{ .rank = 4, .alpha = 8.0, .target_patterns = &lora_targets },
            .lr_schedule = .{ .constant = 1.0e-3 },
            .max_grad_norm = 1.0,
            .grad_accum_steps = 1,
            .lora_a_init_std = 0.02,
            .hidden_size_hint = HIDDEN,
            .num_layers_hint = NUM_LAYERS,
            .seed = TRAINER_SEED,
            .regular_trainable_params = &.{},
            .execution_engine = .compiled_device,
            .compiled_required = true,
        });
        defer trainer.deinit();
        const input = gliner2_autodiff.makeTrainerInput(
            &gliner_ctx,
            batch.input_ids,
            batch.attention_mask,
            batch.targets,
            batch.targets_shape,
            BATCH,
            SEQ_LEN,
        );
        try trainer.ensureGraphBuilt(input);
        seedLoraSlots(&trainer);
        try trainer.loadTrainingState(resume_path, &fingerprint);
        try std.testing.expectEqual(@as(u64, 1), trainer.microBatchSteps());
        try std.testing.expectEqual(@as(u64, 1), trainer.optimizerSteps());
        const resumed = try trainer.step(input);
        try std.testing.expect(resumed.optimizer_stepped);
        try std.testing.expectEqual(real_autodiff.OptimizerBackend.cuda, resumed.profile.optimizer_backend);
        try std.testing.expectEqual(@as(u64, 0), resumed.profile.device_resident_transfer_count);
        try std.testing.expectApproxEqAbs(uninterrupted_loss, resumed.loss, 1.0e-6);
        try std.testing.expectApproxEqAbs(uninterrupted_norm, resumed.grad_norm, 1.0e-6);
        try trainer.saveTrainingState(actual_path, &fingerprint, null);
    }

    const expected = try compat.cwd().readFileAlloc(compat.io(), expected_path, allocator, .unlimited);
    defer allocator.free(expected);
    const actual = try compat.cwd().readFileAlloc(compat.io(), actual_path, allocator, .unlimited);
    defer allocator.free(actual);
    // Byte equality covers every LoRA weight, both Adam moments, per-slot Adam
    // steps, and global micro-batch/optimizer counters in the safetensors file.
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "GLiNER2 CUDA device training primitives: accumulation clipping norm and AdamW" {
    if (!cudaParityTestSelected("training-primitives")) return error.SkipZigTest;
    if (comptime !build_options.enable_cuda) {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return error.RequiredCudaBuildDisabled;
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    var compute = cuda_compute.CudaCompute.init(allocator) catch |err| {
        if (platform.env.getenvBoolDefault("TERMITE_REQUIRE_CUDA_TESTS", false)) return err;
        return error.SkipZigTest;
    };
    defer compute.deinit();
    try compute.requireProfile(.gliner2_training);
    const memory_info = try compute.deviceMemoryInfo();
    try std.testing.expect(memory_info.total_bytes > 0);
    try std.testing.expect(memory_info.free_bytes <= memory_info.total_bytes);
    var cb = compute.computeBackend();
    try std.testing.expect(cb.supportsDeviceTraining());

    const shape = [_]i32{4};
    const initial_weight = [_]f32{ 1.0, -2.0, 3.0, -4.0 };
    const grad_a_data = [_]f32{ 0.5, -1.0, 1.5, -2.0 };
    const grad_b_data = [_]f32{ 2.0, 4.0, -6.0, 8.0 };
    const weight = try cb.fromFloat32Shape(&initial_weight, &shape);
    defer cb.free(weight);
    const grad_a = try cb.fromFloat32Shape(&grad_a_data, &shape);
    defer cb.free(grad_a);
    const grad_b = try cb.fromFloat32Shape(&grad_b_data, &shape);
    defer cb.free(grad_b);
    const accum = try cb.trainingZeroF32(4, &shape);
    defer cb.free(accum);
    const m = try cb.trainingZeroF32(4, &shape);
    defer cb.free(m);
    const v = try cb.trainingZeroF32(4, &shape);
    defer cb.free(v);

    try cb.trainingAccumulateF32(accum, grad_a, 4, 0.25, true);
    try cb.trainingAccumulateF32(accum, grad_b, 4, 0.5, false);
    const expected_accum = [_]f32{ 1.125, 1.75, -2.625, 3.5 };
    const sum_squares = try cb.trainingSumSquaresManyF32(&.{.{ .tensor = accum, .elem_count = 4 }});
    try std.testing.expectApproxEqAbs(@as(f32, 23.46875), sum_squares, 2.0e-5);
    const actual_accum = try cb.toFloat32(accum, allocator);
    defer allocator.free(actual_accum);
    for (expected_accum, actual_accum) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1.0e-6);
    }

    const beta1: f32 = 0.9;
    const beta2: f32 = 0.999;
    const learning_rate: f32 = 0.01;
    const weight_decay: f32 = 0.1;
    const grad_scale: f32 = 0.5;
    try cb.trainingAdamWManyF32(&.{.{
        .weight = weight,
        .grad = accum,
        .m = m,
        .v = v,
        .elem_count = 4,
        .bias_correction1 = 1.0 - beta1,
        .bias_correction2 = 1.0 - beta2,
    }}, .{
        .lr = learning_rate,
        .beta1 = beta1,
        .beta2 = beta2,
        .eps = 1.0e-8,
        .weight_decay = weight_decay,
        .grad_scale = grad_scale,
    });
    try cb.trainingSynchronize();

    const actual_weight = try cb.toFloat32(weight, allocator);
    defer allocator.free(actual_weight);
    const actual_m = try cb.toFloat32(m, allocator);
    defer allocator.free(actual_m);
    const actual_v = try cb.toFloat32(v, allocator);
    defer allocator.free(actual_v);
    for (initial_weight, expected_accum, 0..) |initial, accumulated, idx| {
        const scaled_grad = accumulated * grad_scale;
        const expected_m = (1.0 - beta1) * scaled_grad;
        const expected_v = (1.0 - beta2) * scaled_grad * scaled_grad;
        const m_hat = expected_m / (1.0 - beta1);
        const v_hat = expected_v / (1.0 - beta2);
        const expected_weight = initial - learning_rate * (m_hat / (@sqrt(v_hat) + 1.0e-8) + weight_decay * initial);
        try std.testing.expectApproxEqAbs(expected_m, actual_m[idx], 1.0e-6);
        try std.testing.expectApproxEqAbs(expected_v, actual_v[idx], 1.0e-6);
        try std.testing.expectApproxEqAbs(expected_weight, actual_weight[idx], 2.0e-6);
    }
}
