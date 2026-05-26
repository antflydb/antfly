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

const std = @import("std");
const build_options = @import("build_options");
const ml = @import("ml");
const c_file = @import("../util/c_file.zig");
const qwen2_graph = @import("../architectures/qwen2_graph.zig");
const gpt_model = @import("../models/gpt.zig");
const manifest_mod = @import("../models/manifest.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const gemma4 = @import("gemma4.zig");
const colqwen2 = @import("colqwen2.zig");
const compat = @import("../io/compat.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const ShardedSafetensorsSource = weight_source_mod.ShardedSafetensorsSource;
const native_compute = @import("../ops/native_compute.zig");
const ops_mod = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const ComputeBackend = ops_mod.ComputeBackend;
const NodeId = ml.graph.NodeId;
const CT = ops_mod.CT;

const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;

pub const default_lora_target_modules = [_][]const u8{
    "q_proj",
    "k_proj",
    "v_proj",
    "o_proj",
    "gate_proj",
    "up_proj",
    "down_proj",
};

pub const qwen35_lora_target_modules = [_][]const u8{
    "q_proj",
    "k_proj",
    "v_proj",
    "o_proj",
    "gate_proj",
    "up_proj",
    "down_proj",
    "linear_attn.in_proj_qkv",
    "linear_attn.in_proj_z",
    "linear_attn.in_proj_b",
    "linear_attn.in_proj_a",
    "linear_attn.out_proj",
};

pub const GraphConfig = struct {
    arch: qwen2_graph.Config,
    weight_tying: bool,
    weight_prefix: []const u8 = "",
};

fn copyTensorFloat32(dst: []f32, tensor: *const Tensor) !void {
    if (tensor.dtype != .f32) return error.AdapterShapeMismatch;
    if (tensor.data.len != dst.len * @sizeOf(f32)) return error.AdapterShapeMismatch;

    if (tensor.asFloat32IfAligned()) |values| {
        if (values.len != dst.len) return error.AdapterShapeMismatch;
        @memcpy(dst, values);
        return;
    }

    for (dst, 0..) |*value, idx| {
        const offset = idx * @sizeOf(f32);
        const bits = std.mem.readInt(u32, tensor.data[offset..][0..@sizeOf(f32)], .little);
        value.* = @bitCast(bits);
    }
}

pub const BackendKind = enum { native };

pub const LoadedBackend = struct {
    allocator: std.mem.Allocator,
    kind: BackendKind,
    compute_backend: ComputeBackend,
    native_ws: ?native_compute.WeightStore = null,
    native_engine: ?*native_compute.NativeCompute = null,
    safetensors_source: ?*SafetensorsSource = null,
    sharded_safetensors_source: ?*ShardedSafetensorsSource = null,

    pub fn backendPtr(self: *LoadedBackend) *const ComputeBackend {
        self.compute_backend = switch (self.kind) {
            .native => blk: {
                const engine = self.native_engine.?;
                engine.data = &self.native_ws.?;
                break :blk engine.computeBackend();
            },
        };
        return &self.compute_backend;
    }

    pub fn deinit(self: *LoadedBackend) void {
        if (self.native_ws) |*ws| {
            native_compute.deinitPrefetchQueue(ws);
            var it = ws.resident_weights.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                entry.value_ptr.deinit();
            }
            ws.resident_weights.deinit(self.allocator);
            ws.lazy_weights.deinit(self.allocator);
        }
        if (self.safetensors_source) |src| src.weightSource().deinit();
        if (self.sharded_safetensors_source) |src| src.weightSource().deinit();
        switch (self.kind) {
            .native => if (self.native_engine) |engine| {
                engine.data = &self.native_ws.?;
                var cb = engine.computeBackend();
                cb.deinit();
            },
        }
        self.* = undefined;
    }
};

pub const Qwen2AutodiffCtx = struct {
    graph_config: GraphConfig,
    built: ?qwen2_graph.QwenGraph = null,
    lm_logits: ?NodeId = null,

    pub fn init(graph_config: GraphConfig) Qwen2AutodiffCtx {
        return .{ .graph_config = graph_config };
    }

    pub fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        _ = attention_mask;
        const self: *Qwen2AutodiffCtx = @ptrCast(@alignCast(ctx_opaque));

        const rope = try graph_input_binder.QwenPlaceholderPrep.buildRopeCosSin(
            bld.graph.allocator,
            seq_len,
            self.graph_config.arch.head_dim,
            self.graph_config.arch.rope_theta,
        );
        defer bld.graph.allocator.free(rope.cos);
        defer bld.graph.allocator.free(rope.sin);

        const rope_shape = Shape.init(.f32, &.{
            @as(i64, @intCast(seq_len)),
            @as(i64, @intCast(self.graph_config.arch.head_dim)),
        });
        const cos_node = try bld.tensorConst(rope.cos, rope_shape);
        const sin_node = try bld.tensorConst(rope.sin, rope_shape);

        self.built = try qwen2_graph.buildForwardGraph(
            bld,
            self.graph_config.arch,
            batch,
            seq_len,
            .{
                .input_ids = input_ids,
                .rope_cos = cos_node,
                .rope_sin = sin_node,
            },
        );
        return self.built.?.output_node;
    }

    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        const self: *Qwen2AutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const out_shape = bld.graph.node(forward_output).output_shape;
        const total_rows: u32 = @intCast(out_shape.dim(0) * out_shape.dim(1));
        const hidden_size: u32 = @intCast(out_shape.dim(2));

        const hidden_flat = try bld.reshape(forward_output, Shape.init(.f32, &.{
            @as(i64, @intCast(total_rows)),
            @as(i64, @intCast(hidden_size)),
        }));
        const lm_head_w = if (self.graph_config.weight_tying) blk: {
            var name_buf: [256]u8 = undefined;
            const name = try prefixedModelName(&name_buf, self.graph_config, "model.embed_tokens.weight");
            break :blk try bld.parameter(name, Shape.init(.f32, &.{
                @as(i64, @intCast(self.graph_config.arch.vocab_size)),
                @as(i64, @intCast(hidden_size)),
            }));
        } else blk: {
            var name_buf: [256]u8 = undefined;
            const name = try prefixedModelName(&name_buf, self.graph_config, "lm_head.weight");
            break :blk try bld.parameter(name, Shape.init(.f32, &.{
                @as(i64, @intCast(self.graph_config.arch.vocab_size)),
                @as(i64, @intCast(hidden_size)),
            }));
        };
        const logits = try bld.linearNoBias(hidden_flat, lm_head_w, total_rows, hidden_size, self.graph_config.arch.vocab_size);
        self.lm_logits = logits;
        return bld.crossEntropyLoss(logits, targets);
    }

    pub fn bindArchInputs(
        ctx_opaque: *anyopaque,
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        graph: *const ml.graph.Graph,
        rt_map: *std.AutoHashMapUnmanaged(NodeId, CT),
        batch: u32,
        seq_len: u32,
        attention_mask: []const f32,
    ) anyerror!void {
        _ = ctx_opaque;
        _ = cb;
        _ = allocator;
        _ = graph;
        _ = rt_map;
        _ = batch;
        _ = seq_len;
        _ = attention_mask;
    }

    pub fn remapGraphNodes(ctx_opaque: *anyopaque, id_map: []const NodeId) anyerror!void {
        const self: *Qwen2AutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        if (self.built) |*built| {
            built.input_ids_node = id_map[built.input_ids_node];
            built.rope_cos_node = id_map[built.rope_cos_node];
            built.rope_sin_node = id_map[built.rope_sin_node];
            built.output_node = id_map[built.output_node];
        }
        if (self.lm_logits) |node_id| self.lm_logits = id_map[node_id];
    }
};

fn prefixedModelName(buf: *[256]u8, config: GraphConfig, name: []const u8) ![]const u8 {
    if (config.weight_prefix.len == 0) return name;
    if (std.mem.startsWith(u8, name, "model.")) {
        return std.fmt.bufPrint(buf, "{s}.{s}", .{ config.weight_prefix, name["model.".len..] }) catch error.NameTooLong;
    }
    return std.fmt.bufPrint(buf, "{s}.{s}", .{ config.weight_prefix, name }) catch error.NameTooLong;
}

pub const OwnedTrainerInput = struct {
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
    trainer_input: real_autodiff.TrainerInput,

    pub fn deinit(self: *OwnedTrainerInput, allocator: std.mem.Allocator) void {
        allocator.free(self.input_ids);
        allocator.free(self.attention_mask);
        allocator.free(self.targets);
        self.* = undefined;
    }
};

pub fn loadGraphConfig(allocator: std.mem.Allocator, model_dir: []const u8) !GraphConfig {
    const config_path = try std.fs.path.join(allocator, &.{ model_dir, colqwen2.hf_config_file_name });
    defer allocator.free(config_path);
    const config_bytes = try c_file.readFile(allocator, config_path);
    defer allocator.free(config_bytes);
    const config = try gpt_model.parseConfig(allocator, config_bytes);
    if (config.family != .qwen2 and config.family != .qwen3_5) return error.UnsupportedModelFamily;

    return .{
        .arch = .{
            .family = switch (config.family) {
                .qwen2 => .qwen2,
                .qwen3_5 => .qwen3_5,
                else => unreachable,
            },
            .vocab_size = config.vocab_size,
            .hidden_size = config.hidden_size,
            .num_hidden_layers = config.num_hidden_layers,
            .num_attention_heads = config.num_attention_heads,
            .num_kv_heads = config.effectiveKVHeads(),
            .head_dim = config.headDim(),
            .intermediate_size = config.intermediate_size,
            .max_position_embeddings = config.max_position_embeddings,
            .rope_theta = config.rope_theta,
            .rms_norm_eps = config.norm_eps,
            .rope_partial_factor = config.rope_partial_factor,
            .norm_weight_offset = config.norm_weight_offset,
            .qwen35_has_linear_attention = config.qwen35_has_linear_attention,
            .qwen35_full_attention_interval = config.qwen35_full_attention_interval,
            .qwen35_linear_conv_kernel_dim = config.qwen35_linear_conv_kernel_dim,
            .qwen35_linear_key_head_dim = config.qwen35_linear_key_head_dim,
            .qwen35_linear_value_head_dim = config.qwen35_linear_value_head_dim,
            .qwen35_linear_num_key_heads = config.qwen35_linear_num_key_heads,
            .qwen35_linear_num_value_heads = config.qwen35_linear_num_value_heads,
            .qwen35_attn_output_gate = config.qwen35_attn_output_gate,
        },
        .weight_tying = config.weight_tying,
        .weight_prefix = config.weight_prefix,
    };
}

pub fn loadBackendForModelDir(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend_kind: BackendKind,
) !LoadedBackend {
    _ = backend_kind;
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    var native_ws = native_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };

    var single_source: ?*SafetensorsSource = null;
    var sharded_source: ?*ShardedSafetensorsSource = null;
    var source: weight_source_mod.WeightSource = undefined;
    if (manifest.safetensors_path) |st_path| {
        const src = try SafetensorsSource.initAbsolute(allocator, st_path);
        single_source = src;
        source = src.weightSource();
    } else if (manifest.safetensors_index_path) |idx_path| {
        const src = try ShardedSafetensorsSource.initAbsolute(allocator, idx_path);
        sharded_source = src;
        source = src.weightSource();
    } else {
        return error.MissingMergedCheckpoint;
    }
    errdefer {
        if (single_source) |src| src.weightSource().deinit();
        if (sharded_source) |src| src.weightSource().deinit();
    }

    const names = try source.listNames(allocator);
    defer allocator.free(names);
    for (names) |name| {
        const lw = try source.getTensor(name);
        errdefer {
            var doomed = lw;
            doomed.deinit();
        }
        try native_ws.resident_weights.put(allocator, try allocator.dupe(u8, name), lw);
    }

    const native_engine = try allocator.create(native_compute.NativeCompute);
    native_engine.* = native_compute.NativeCompute.init(allocator, &native_ws, null);
    return .{
        .allocator = allocator,
        .kind = .native,
        .compute_backend = native_engine.computeBackend(),
        .native_ws = native_ws,
        .native_engine = native_engine,
        .safetensors_source = single_source,
        .sharded_safetensors_source = sharded_source,
    };
}

pub fn makeTrainerInputForExample(
    allocator: std.mem.Allocator,
    ctx: *Qwen2AutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !OwnedTrainerInput {
    return makeTrainerInputForExampleWeighted(allocator, ctx, example, seq_len, null, null);
}

pub fn makeTrainerInputForExampleScaled(
    allocator: std.mem.Allocator,
    ctx: *Qwen2AutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    token_scale_override: ?f32,
) !OwnedTrainerInput {
    return makeTrainerInputForExampleWeighted(allocator, ctx, example, seq_len, token_scale_override, null);
}

pub fn makeTrainerInputForLogprobCoeff(
    allocator: std.mem.Allocator,
    ctx: *Qwen2AutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    logprob_coeff: f32,
) !OwnedTrainerInput {
    const rows_f: f32 = @floatFromInt(seq_len);
    return makeTrainerInputForExampleScaled(allocator, ctx, example, seq_len, -logprob_coeff * rows_f);
}

pub fn makeTrainerInputForTokenLogprobGrads(
    allocator: std.mem.Allocator,
    ctx: *Qwen2AutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    logprob_grads: []const f32,
) !OwnedTrainerInput {
    const rows_f: f32 = @floatFromInt(seq_len);
    const token_scales = try allocator.alloc(f32, logprob_grads.len);
    defer allocator.free(token_scales);
    for (logprob_grads, 0..) |grad, idx| token_scales[idx] = -grad * rows_f;
    return makeTrainerInputForExampleWeighted(allocator, ctx, example, seq_len, null, token_scales);
}

fn makeTrainerInputForExampleWeighted(
    allocator: std.mem.Allocator,
    ctx: *Qwen2AutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    token_scale_override: ?f32,
    token_scales: ?[]const f32,
) !OwnedTrainerInput {
    const rows: usize = @intCast(seq_len);
    const vocab_size: usize = @intCast(ctx.graph_config.arch.vocab_size);

    const input_ids = try allocator.alloc(i64, rows);
    errdefer allocator.free(input_ids);
    @memset(input_ids, 0);
    const attention_mask = try allocator.alloc(f32, rows);
    errdefer allocator.free(attention_mask);
    @memset(attention_mask, 0.0);

    const usable = @min(example.input_ids.len, rows);
    for (0..usable) |i| {
        input_ids[i] = example.input_ids[i];
        attention_mask[i] = 1.0;
    }

    const targets = try allocator.alloc(f32, rows * vocab_size);
    errdefer allocator.free(targets);
    @memset(targets, 0.0);

    var valid_tokens: usize = 0;
    for (0..@min(example.labels.len, rows)) |i| {
        if (example.labels[i] != -100) valid_tokens += 1;
    }
    if (token_scales) |scales| {
        if (scales.len != valid_tokens) return error.GradientShapeMismatch;
    }
    const default_row_scale: f32 = token_scale_override orelse if (valid_tokens == 0)
        0.0
    else
        @as(f32, @floatFromInt(rows)) / @as(f32, @floatFromInt(valid_tokens));

    var supervised_idx: usize = 0;
    for (0..@min(example.labels.len, rows)) |i| {
        const label = example.labels[i];
        if (label < 0) continue;
        const idx: usize = @intCast(label);
        if (idx >= vocab_size) return error.LabelOutOfRange;
        const row_scale = if (token_scales) |scales| scales[supervised_idx] else default_row_scale;
        targets[i * vocab_size + idx] = row_scale;
        supervised_idx += 1;
    }

    return .{
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .trainer_input = .{
            .ctx = @ptrCast(ctx),
            .build_forward = &Qwen2AutodiffCtx.buildForward,
            .build_loss = &Qwen2AutodiffCtx.buildLoss,
            .input_ids = input_ids,
            .attention_mask = attention_mask,
            .targets = targets,
            .targets_shape = Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(vocab_size)) }),
            .batch = 1,
            .seq_len = seq_len,
            .bind_arch_inputs = &Qwen2AutodiffCtx.bindArchInputs,
            .remap_graph_nodes = &Qwen2AutodiffCtx.remapGraphNodes,
        },
    };
}

pub fn findFirstSupervisedExample(examples: []const gemma4.PreparedExampleInput) ?*const gemma4.PreparedExampleInput {
    for (examples) |*example| {
        if (example.num_supervised_tokens > 0) return example;
    }
    return null;
}

pub fn initializeTrainerFromAdapterDir(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *Qwen2AutodiffCtx,
    adapter_model_dir: []const u8,
    bootstrap_example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !void {
    var bootstrap = try makeTrainerInputForExample(allocator, ctx, bootstrap_example, seq_len);
    defer bootstrap.deinit(allocator);
    try trainer.ensureGraphBuilt(bootstrap.trainer_input);

    var inspection = try colqwen2.inspectCheckpoint(allocator, adapter_model_dir);
    defer colqwen2.freeInspectionSummary(allocator, &inspection);
    const checkpoint_path = inspection.adapter_checkpoint_path orelse return error.MissingAdapterCheckpoint;

    var source = try SafetensorsSource.initAbsolute(allocator, checkpoint_path);
    defer source.weightSource().deinit();
    const ws = source.weightSource();

    for (trainer.lora_params.items) |*slot| {
        const tensor_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{slot.name});
        defer allocator.free(tensor_name);

        var loaded = try ws.getTensor(tensor_name);
        defer loaded.deinit();

        if (loaded.tensor.shape.len != slot.dims.len) return error.AdapterShapeMismatch;
        for (slot.dims, 0..) |want_dim, idx| {
            if (loaded.tensor.shape[idx] != @as(i64, want_dim)) return error.AdapterShapeMismatch;
        }

        try copyTensorFloat32(slot.weights, &loaded.tensor);
        @memset(slot.grad_accum, 0.0);
    }
}

pub fn sequenceLogprobForExample(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *Qwen2AutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !f32 {
    var owned = try makeTrainerInputForExample(allocator, ctx, example, seq_len);
    defer owned.deinit(allocator);
    const step = try trainer.evaluate(owned.trainer_input);
    return -step.loss * @as(f32, @floatFromInt(example.num_supervised_tokens));
}

pub fn tokenLogprobsForPromptCompletion(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *Qwen2AutodiffCtx,
    prompt: []const i32,
    completion: []const i32,
    seq_len: u32,
    out_logps: []f32,
) !void {
    if (completion.len != out_logps.len) return error.LogpLenMismatch;
    if (prompt.len == 0) return error.EmptyPrompt;
    if (completion.len == 0) return error.EmptyCompletion;
    const joined = try concatPromptCompletion(allocator, prompt, completion);
    defer allocator.free(joined);
    const logits = try executeLogitsForInputIds(allocator, trainer, ctx, joined, seq_len);
    defer allocator.free(logits);
    const vocab_size: usize = @intCast(ctx.graph_config.arch.vocab_size);
    for (completion, 0..) |token_id, comp_idx| {
        const row_idx = prompt.len + comp_idx - 1;
        const row = logits[row_idx * vocab_size ..][0..vocab_size];
        out_logps[comp_idx] = logProbAtToken(row, @intCast(token_id));
    }
}

pub fn sampleCompletionRanked(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *Qwen2AutodiffCtx,
    prompt: []const i32,
    seq_len: u32,
    max_completion_tokens: usize,
    rank: usize,
    eos_token_id: ?i32,
    out_tokens: *std.ArrayList(i32),
    out_logps: *std.ArrayList(f32),
) !void {
    if (prompt.len == 0) return error.EmptyPrompt;

    var seq = std.ArrayList(i32).empty;
    defer seq.deinit(allocator);
    try seq.appendSlice(allocator, prompt);

    var step: usize = 0;
    while (step < max_completion_tokens and seq.items.len < seq_len) : (step += 1) {
        const logits = try executeLogitsForInputIds(allocator, trainer, ctx, seq.items, seq_len);
        defer allocator.free(logits);
        const vocab_size: usize = @intCast(ctx.graph_config.arch.vocab_size);
        const row = logits[(seq.items.len - 1) * vocab_size ..][0..vocab_size];
        const token_id = try selectRankedToken(allocator, row, rank);
        const token_logp = logProbAtToken(row, token_id);
        try out_tokens.append(allocator, @intCast(token_id));
        try out_logps.append(allocator, token_logp);
        try seq.append(allocator, @intCast(token_id));
        if (eos_token_id) |eos_id| if (token_id == @as(usize, @intCast(eos_id))) break;
    }
    if (out_tokens.items.len == 0) return error.EmptyCompletion;
}

pub fn saveTrainerAsQwenAdapterDir(
    allocator: std.mem.Allocator,
    trainer: *const real_autodiff.RealAutodiffTrainer,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    out_dir: []const u8,
) !void {
    var adapter_inspect = try colqwen2.inspectCheckpoint(allocator, adapter_model_dir);
    defer colqwen2.freeInspectionSummary(allocator, &adapter_inspect);

    try compat.cwd().createDirPath(compat.io(), out_dir);
    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, colqwen2.adapter_checkpoint_file_name });
    defer allocator.free(adapter_checkpoint_path);
    const adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, colqwen2.adapter_config_file_name });
    defer allocator.free(adapter_config_path);

    var tensors = std.ArrayList(WriteTensorF32).empty;
    defer tensors.deinit(allocator);
    var owned_names = std.ArrayList([]const u8).empty;
    defer {
        for (owned_names.items) |name| allocator.free(name);
        owned_names.deinit(allocator);
    }
    var owned_shapes = std.ArrayList([]const usize).empty;
    defer {
        for (owned_shapes.items) |shape| allocator.free(shape);
        owned_shapes.deinit(allocator);
    }

    for (trainer.lora_params.items) |slot| {
        const mapped_name = try mapTrainerSlotNameToQwenAdapterTensor(allocator, slot.name);
        errdefer allocator.free(mapped_name);
        const dims = try dimsToUsize(allocator, slot.dims);
        errdefer allocator.free(dims);
        try owned_names.append(allocator, mapped_name);
        try owned_shapes.append(allocator, dims);
        try tensors.append(allocator, .{
            .name = mapped_name,
            .shape = dims,
            .data = slot.weights,
        });
    }

    try writeHeaderAndTensorsF32(allocator, adapter_checkpoint_path, tensors.items);
    const base_name = adapter_inspect.base_model_name_or_path orelse base_model_dir;
    const rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse default_lora_target_modules[0..];
    try writeAdapterConfigJson(allocator, adapter_config_path, base_name, rank, alpha, target_modules);

    try copySupportingArtifactIfPresent(allocator, adapter_inspect.tokenizer_config_path, out_dir, colqwen2.tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_inspect.tokenizer_path, out_dir, colqwen2.tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_inspect.special_tokens_map_path, out_dir, colqwen2.special_tokens_map_file_name);
}

fn executeLogitsForInputIds(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *Qwen2AutodiffCtx,
    raw_input_ids: []const i32,
    seq_len: u32,
) ![]f32 {
    const rows: usize = @intCast(seq_len);
    if (raw_input_ids.len == 0) return error.EmptyPrompt;
    if (raw_input_ids.len > rows) return error.SequenceTooLong;

    const input_ids = try allocator.alloc(i64, rows);
    defer allocator.free(input_ids);
    @memset(input_ids, 0);
    const attention_mask = try allocator.alloc(f32, rows);
    defer allocator.free(attention_mask);
    @memset(attention_mask, 0.0);
    for (raw_input_ids, 0..) |token_id, idx| {
        input_ids[idx] = token_id;
        attention_mask[idx] = 1.0;
    }

    const vocab_size: usize = @intCast(ctx.graph_config.arch.vocab_size);
    const targets = try allocator.alloc(f32, rows * vocab_size);
    defer allocator.free(targets);
    @memset(targets, 0.0);

    const trainer_input = real_autodiff.TrainerInput{
        .ctx = @ptrCast(ctx),
        .build_forward = &Qwen2AutodiffCtx.buildForward,
        .build_loss = &Qwen2AutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(vocab_size)) }),
        .batch = 1,
        .seq_len = seq_len,
        .bind_arch_inputs = &Qwen2AutodiffCtx.bindArchInputs,
        .remap_graph_nodes = &Qwen2AutodiffCtx.remapGraphNodes,
    };

    try trainer.ensureGraphBuilt(trainer_input);
    var gs = &trainer.graph_state.?;
    const logits_node = ctx.lm_logits orelse return error.MissingTrainerLogitsNode;

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| trainer.compute_backend.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }

    const input_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.input_ids_node,
        .name = "__input_ids",
        .shape = gs.graph.node(gs.input_ids_node).output_shape,
    };
    const mask_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.attention_mask_node,
        .name = "__attention_mask",
        .shape = gs.graph.node(gs.attention_mask_node).output_shape,
    };
    const targets_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.targets_node,
        .name = "__targets",
        .shape = gs.graph.node(gs.targets_node).output_shape,
    };

    try rt.put(allocator, gs.input_ids_node, try graph_input_binder.bindI64(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, targets));

    for (trainer.lora_params.items) |slot| {
        const dims = try allocator.alloc(i32, slot.dims.len);
        defer allocator.free(dims);
        @memcpy(dims, slot.dims);
        const ct = try trainer.compute_backend.fromFloat32Shape(slot.weights, dims);
        try rt.put(allocator, slot.node_id, ct);
    }

    const saved_outputs = try allocator.dupe(NodeId, gs.graph.outputs.items);
    defer {
        gs.graph.outputs.clearRetainingCapacity();
        for (saved_outputs) |node_id| gs.graph.outputs.append(allocator, node_id) catch {};
        allocator.free(saved_outputs);
    }
    gs.graph.outputs.clearRetainingCapacity();
    try gs.graph.markOutput(logits_node);

    var rt_inputs = std.ArrayList(interpreter.RuntimeInput).empty;
    defer rt_inputs.deinit(allocator);
    {
        var it = rt.iterator();
        while (it.next()) |entry| {
            try rt_inputs.append(allocator, .{
                .node_id = entry.key_ptr.*,
                .value = entry.value_ptr.*,
            });
        }
    }

    var exec_result = try interpreter.execute(allocator, &gs.graph, trainer.compute_backend, .{
        .runtime_inputs = rt_inputs.items,
    });
    defer exec_result.deinit(trainer.compute_backend);
    return trainer.compute_backend.toFloat32(exec_result.outputs[0], allocator);
}

fn concatPromptCompletion(allocator: std.mem.Allocator, prompt: []const i32, completion: []const i32) ![]i32 {
    const out = try allocator.alloc(i32, prompt.len + completion.len);
    @memcpy(out[0..prompt.len], prompt);
    @memcpy(out[prompt.len..], completion);
    return out;
}

fn logProbAtToken(row: []const f32, token_id: usize) f32 {
    const target = row[token_id];
    var max_logit = row[0];
    for (row[1..]) |value| max_logit = @max(max_logit, value);
    var sum_exp: f32 = 0.0;
    for (row) |value| sum_exp += @exp(value - max_logit);
    return target - max_logit - @log(sum_exp);
}

const RankedToken = struct {
    token_id: usize,
    logit: f32,
};

fn selectRankedToken(allocator: std.mem.Allocator, row: []const f32, rank: usize) !usize {
    var ranked = try allocator.alloc(RankedToken, row.len);
    defer allocator.free(ranked);
    for (row, 0..) |logit, idx| ranked[idx] = .{ .token_id = idx, .logit = logit };
    std.mem.sort(RankedToken, ranked, {}, struct {
        fn lessThan(_: void, lhs: RankedToken, rhs: RankedToken) bool {
            return lhs.logit > rhs.logit;
        }
    }.lessThan);
    return ranked[@min(rank, ranked.len - 1)].token_id;
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

fn mapTrainerSlotNameToQwenAdapterTensor(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        return std.fmt.allocPrint(allocator, "{s}.weight", .{name});
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
        return std.fmt.allocPrint(allocator, "{s}.weight", .{name});
    }
    return try allocator.dupe(u8, name);
}

fn dimsToUsize(allocator: std.mem.Allocator, dims: []const i32) ![]usize {
    const out = try allocator.alloc(usize, dims.len);
    for (dims, 0..) |dim, i| out[i] = @intCast(dim);
    return out;
}

fn writeHeaderAndTensorsF32(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorF32) !void {
    _ = allocator;
    var header_buf: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
    defer header_buf.deinit();
    const writer = &header_buf.writer;
    try writer.writeByte('{');
    var offset: u64 = 0;
    for (tensors, 0..) |tensor, idx| {
        if (idx != 0) try writer.writeByte(',');
        const byte_len = tensor.data.len * @sizeOf(f32);
        try writer.print("\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{tensor.name});
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try writer.writeByte(',');
            try writer.print("{}", .{dim});
        }
        try writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        offset += byte_len;
    }
    try writer.writeByte('}');

    var file = try compat.cwd().createFile(compat.io(), path, .{ .truncate = true });
    defer file.close(compat.io());
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(compat.io(), &len_buf);
    try file.writeStreamingAll(compat.io(), header_buf.written());
    for (tensors) |tensor| {
        for (tensor.data) |item| {
            const bits: u32 = @bitCast(item);
            var bits_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &bits_buf, bits, .little);
            try file.writeStreamingAll(compat.io(), &bits_buf);
        }
    }
}

fn writeAdapterConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    target_modules: []const []const u8,
) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{
        .base_model_name_or_path = base_model_name_or_path,
        .peft_type = "LORA",
        .task_type = "CAUSAL_LM",
        .r = rank,
        .lora_alpha = alpha,
        .target_modules = target_modules,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

fn copySupportingArtifactIfPresent(
    allocator: std.mem.Allocator,
    maybe_src_path: ?[]const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    const src_path = maybe_src_path orelse return;
    const contents = try c_file.readFile(allocator, src_path);
    defer allocator.free(contents);
    const dst_path = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst_path, .data = contents });
}
