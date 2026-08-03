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

//! Cross-backend GLiNER2 **gradient** parity gate (native vs Metal).
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
//! `NativeCompute` and once on `MetalCompute`, and compares the **per-parameter
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
//!
//! Skips (rather than fails) when Metal is not compiled in or no Metal device
//! is present, matching the convention used by the `metal_compute.zig` audits.
//! Set `TERMITE_REQUIRE_METAL_TESTS=1` in Metal CI to turn either condition into
//! a hard failure so a misconfigured runner cannot silently bypass this gate.

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
const MetalWeightStore = if (build_options.enable_metal) gpu_hosted_store.WeightStore else void;
const metal_runtime = inference.metal_runtime;

const ops_mod = inference.ops;
const ComputeBackend = ops_mod.ComputeBackend;
const Tensor = inference.backends.Tensor;

const real_autodiff = inference.finetune.real_autodiff_trainer;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;

// ── Model geometry ───────────────────────────────────────────────────────────
//
// Shapes mirror the real `fastino/gliner2-base-v1` encoder (deberta-v3-base:
// hidden 768, 12 heads, intermediate 3072, max_position_embeddings 512,
// position_buckets 256) because Metal kernel selection is shape-driven —
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

const BATCH: u32 = 1;
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

// ── Batch construction ───────────────────────────────────────────────────────

fn fillInputs(input_ids: []i64, attention_mask: []f32) void {
    for (input_ids, 0..) |*id, i| id.* = @intCast((i * 7919 + 13) % VOCAB);
    for (attention_mask, 0..) |*m, i| m.* = if (i < VALID_TOKENS) 1.0 else 0.0;
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
                if (t >= VALID_TOKENS) continue;
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
            const active_off = gliner2_autodiff.gliner2TotalLossActiveFieldsOffset(E, 1);
            const shape = gliner2_autodiff.gliner2TotalLossTargetsShape(BATCH, MAX_SPANS, NUM_ENTITY_TYPES);
            const targets = try allocator.alloc(f32, rows * width);
            errdefer allocator.free(targets);
            @memset(targets, 0.0);
            for (0..rows) |row_idx| {
                const row = targets[row_idx * width ..][0..width];
                fillSpanSection(row[0..span_width], row_idx / MAX_SPANS, row_idx % MAX_SPANS);
                // Classification/count sections are left masked out (all-zero)
                // so this isolates the structure term — which is where the
                // native-vs-Metal backward divergence lives. The count-embed
                // active-field mask lives in its own block and must be set
                // independently of the span mask.
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
    try runBackendGradParity(std.testing.allocator, "token", .token);
}

test "GLiNER2 backend grad parity: span-start objective backward matches native and Metal" {
    try runBackendGradParity(std.testing.allocator, "span-start", .span_start);
}

// The gate proper. The structure term of `gliner2-total-loss` is where the
// ~17.5% native-vs-Metal backward divergence lives. This is EXPECTED TO FAIL
// until that kernel bug is fixed — that failure is the whole point of the file.
// Do not widen the tolerance to make it green.
test "GLiNER2 backend grad parity: gliner2-total-loss objective backward matches native and Metal" {
    try runBackendGradParity(std.testing.allocator, "gliner2-total-loss", .gliner2_total_loss);
}
