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

// Synthetic end-to-end CLIP/CLAP native-backend bench.
//
// This mirrors the GLiNER2 native bench: random resident weights, real eager
// architecture forwards, and optional GGUF-style QuantizedStorage for 2D
// matrices.  It verifies model-level CLIP/CLAP text/vision paths reach the
// quantized NativeCompute kernels rather than only exercising microbenches.

const std = @import("std");
const build_options = @import("build_options");

const termite_internal = @import("termite_internal");
const clip_arch = termite_internal.architectures.clip;
const clap_arch = termite_internal.architectures.clap;
const clip_config_mod = termite_internal.models.clip;
const clap_config_mod = termite_internal.models.clap;
const native_compute = termite_internal.native_compute.native;
const NativeCompute = native_compute.NativeCompute;
const cuda_compute = if (build_options.enable_cuda) termite_internal.native_compute.cuda else struct {};
const WeightStore = native_compute.WeightStore;
const QuantizedStorage = termite_internal.models.weight_source.QuantizedStorage;
const Tensor = termite_internal.backends.Tensor;
const quant_codec = termite_internal.gguf.quant_codec;
const tensor_types = termite_internal.gguf.tensor_types;

const QuantMode = enum {
    none,
    q1_0,
    q4_0,
    q4_1,
    q5_0,
    q5_1,
    q8_0,
    q8_1,
    q2_k,
    q3_k,
    q4_k,
    q5_k,
    q6_k,
    q8_k,
};

const BenchTarget = enum {
    clip_text,
    clip_vision,
    clap_text,
    clap_audio,
    all,
};

const OutputFormat = enum {
    text,
    csv,
};

const BackendMode = enum {
    native,
    cuda,
};

const BenchConfig = struct {
    target: BenchTarget = .all,
    batch: usize = 1,
    seq_len: usize = 77,
    warmup_iters: usize = 1,
    measure_iters: usize = 3,
    quant: QuantMode = .q5_k,
    matrix: bool = false,
    compare_quant: bool = false,
    compare_dispatch: bool = false,
    format: OutputFormat = .text,
    backend: BackendMode = .native,
    io: ?std.Io = null,

    clip_text_hidden: u32 = 512,
    clip_text_layers: u32 = 12,
    clip_text_heads: u32 = 8,
    clip_text_intermediate: u32 = 2048,
    clip_vision_hidden: u32 = 768,
    clip_vision_layers: u32 = 12,
    clip_vision_heads: u32 = 12,
    clip_vision_intermediate: u32 = 3072,
    clip_image_size: u32 = 224,
    clip_patch_size: u32 = 32,
    clip_vocab_size: u32 = 4096,
    projection_dim: u32 = 512,

    clap_hidden: u32 = 768,
    clap_layers: u32 = 12,
    clap_heads: u32 = 12,
    clap_intermediate: u32 = 3072,
    clap_vocab_size: u32 = 4096,
    clap_max_positions: u32 = 514,
    clap_audio_patch_hidden: u32 = 32,
    clap_audio_depth: u32 = 1,
    clap_audio_spec_size: u32 = 32,
    clap_audio_mel_bins: u32 = 16,
    clap_audio_window_size: u32 = 1,
};

const text_matrix_batches = [_]usize{ 1, 4, 16, 32 };
const media_matrix_batches = [_]usize{ 1, 4, 16 };

fn parseArgs(init: std.process.Init) !BenchConfig {
    var cfg = BenchConfig{};
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--target")) {
            cfg.target = parseTarget(args_iter.next() orelse return error.MissingValue) orelse return error.InvalidTarget;
        } else if (std.mem.eql(u8, arg, "--batch")) {
            cfg.batch = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--seq-len")) {
            cfg.seq_len = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            cfg.warmup_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            cfg.measure_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--quant")) {
            const value = args_iter.next() orelse return error.MissingValue;
            if (std.ascii.eqlIgnoreCase(value, "f32")) {
                cfg.quant = .none;
            } else {
                cfg.quant = parseQuantMode(value) orelse return error.InvalidQuantMode;
            }
        } else if (std.mem.eql(u8, arg, "--backend")) {
            cfg.backend = parseBackendMode(args_iter.next() orelse return error.MissingValue) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--matrix")) {
            cfg.matrix = true;
        } else if (std.mem.eql(u8, arg, "--compare-quant")) {
            cfg.compare_quant = true;
        } else if (std.mem.eql(u8, arg, "--compare-dispatch")) {
            cfg.compare_dispatch = true;
        } else if (std.mem.eql(u8, arg, "--format")) {
            cfg.format = parseOutputFormat(args_iter.next() orelse return error.MissingValue) orelse return error.InvalidOutputFormat;
        } else if (std.mem.eql(u8, arg, "--clip-text-layers")) {
            cfg.clip_text_layers = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--clip-vision-layers")) {
            cfg.clip_vision_layers = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--clap-layers")) {
            cfg.clap_layers = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--clap-audio-depth")) {
            cfg.clap_audio_depth = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--clap-audio-patch-hidden")) {
            cfg.clap_audio_patch_hidden = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--clap-audio-spec-size")) {
            cfg.clap_audio_spec_size = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--clap-audio-mel-bins")) {
            cfg.clap_audio_mel_bins = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--clap-audio-window-size")) {
            cfg.clap_audio_window_size = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--hidden")) {
            const hidden = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
            cfg.clip_text_hidden = hidden;
            cfg.clip_vision_hidden = hidden;
            cfg.clap_hidden = hidden;
        } else if (std.mem.eql(u8, arg, "--intermediate")) {
            const intermediate = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
            cfg.clip_text_intermediate = intermediate;
            cfg.clip_vision_intermediate = intermediate;
            cfg.clap_intermediate = intermediate;
        }
    }
    return cfg;
}

fn parseTarget(value: []const u8) ?BenchTarget {
    inline for (@typeInfo(BenchTarget).@"enum".fields) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn parseOutputFormat(value: []const u8) ?OutputFormat {
    inline for (@typeInfo(OutputFormat).@"enum".fields) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn parseBackendMode(value: []const u8) ?BackendMode {
    inline for (@typeInfo(BackendMode).@"enum".fields) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn parseQuantMode(value: []const u8) ?QuantMode {
    inline for (@typeInfo(QuantMode).@"enum".fields) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn quantToKnown(mode: QuantMode) ?tensor_types.KnownTensorType {
    return switch (mode) {
        .none => null,
        .q1_0 => .Q1_0,
        .q4_0 => .Q4_0,
        .q4_1 => .Q4_1,
        .q5_0 => .Q5_0,
        .q5_1 => .Q5_1,
        .q8_0 => .Q8_0,
        .q8_1 => .Q8_1,
        .q2_k => .Q2_K,
        .q3_k => .Q3_K,
        .q4_k => .Q4_K,
        .q5_k => .Q5_K,
        .q6_k => .Q6_K,
        .q8_k => .Q8_K,
    };
}

fn pickQuantTypeForRow(in_dim: usize, requested: QuantMode) ?tensor_types.KnownTensorType {
    const known = quantToKnown(requested) orelse return null;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = known }) orelse return null;
    return if (in_dim % values_per_block == 0) known else null;
}

fn quantizeFromF32(
    allocator: std.mem.Allocator,
    kind: tensor_types.KnownTensorType,
    data: []const f32,
) ![]u8 {
    return switch (kind) {
        .Q1_0 => try quant_codec.quantizeQ1_0FromF32(allocator, data),
        .Q4_0 => try quant_codec.quantizeQ4_0FromF32(allocator, data),
        .Q4_1 => try quant_codec.quantizeQ4_1FromF32(allocator, data),
        .Q5_0 => try quant_codec.quantizeQ5_0FromF32(allocator, data),
        .Q5_1 => try quant_codec.quantizeQ5_1FromF32(allocator, data),
        .Q8_0 => try quant_codec.quantizeQ8_0FromF32(allocator, data),
        .Q8_1 => try quant_codec.quantizeQ8_1FromF32(allocator, data),
        .Q2_K => try quant_codec.quantizeQ2_KFromF32(allocator, data),
        .Q3_K => try quant_codec.quantizeQ3_KFromF32(allocator, data),
        .Q4_K => try quant_codec.quantizeQ4_KFromF32(allocator, data),
        .Q5_K => try quant_codec.quantizeQ5_KFromF32(allocator, data),
        .Q6_K => try quant_codec.quantizeQ6_KFromF32(allocator, data),
        .Q8_K => try quant_codec.quantizeQ8_KFromF32(allocator, data),
        else => error.UnsupportedQuantMode,
    };
}

fn putRandomWeight(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    name: []const u8,
    shape: []const i64,
    rng: std.Random,
) !void {
    var n_elems: usize = 1;
    for (shape) |d| n_elems *= @intCast(d);
    const data = try allocator.alloc(f32, n_elems);
    defer allocator.free(data);
    const scale: f32 = if (shape.len == 1) 0.02 else 1.0 / @sqrt(@as(f32, @floatFromInt(@max(n_elems, 1))));
    for (data) |*v| v.* = (rng.float(f32) * 2.0 - 1.0) * scale;

    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    const tensor = try Tensor.initFloat32(allocator, owned_name, shape, data);
    try store.resident_weights.put(allocator, owned_name, .{ .tensor = tensor });
}

fn putOnesWeight(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    name: []const u8,
    len: usize,
) !void {
    const data = try allocator.alloc(f32, len);
    defer allocator.free(data);
    @memset(data, 1.0);
    const shape = [_]i64{@intCast(len)};
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    const tensor = try Tensor.initFloat32(allocator, owned_name, &shape, data);
    try store.resident_weights.put(allocator, owned_name, .{ .tensor = tensor });
}

fn putZerosWeight(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    name: []const u8,
    len: usize,
) !void {
    const data = try allocator.alloc(f32, len);
    defer allocator.free(data);
    @memset(data, 0.0);
    const shape = [_]i64{@intCast(len)};
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    const tensor = try Tensor.initFloat32(allocator, owned_name, &shape, data);
    try store.resident_weights.put(allocator, owned_name, .{ .tensor = tensor });
}

fn putRandom2DWeight(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    name: []const u8,
    out_dim: usize,
    in_dim: usize,
    rng: std.Random,
    quant: QuantMode,
) !void {
    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const picked_or_null = pickQuantTypeForRow(in_dim, quant);
    if (picked_or_null == null) {
        try putRandomWeight(allocator, store, name, &shape, rng);
        return;
    }
    const picked = picked_or_null.?;
    const data = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(data);
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(in_dim, 1))));
    for (data) |*v| v.* = (rng.float(f32) * 2.0 - 1.0) * scale;

    const raw_bytes = try quantizeFromF32(allocator, picked, data);
    errdefer allocator.free(raw_bytes);
    const owned_shape = try allocator.dupe(i64, &shape);
    errdefer allocator.free(owned_shape);
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = picked },
        .raw_bytes = raw_bytes,
        .shape = owned_shape,
        .raw_owned = true,
        .allocator = allocator,
    };

    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    const empty_tensor = try Tensor.initFloat32(allocator, owned_name, &shape, &.{});
    try store.resident_weights.put(allocator, owned_name, .{
        .tensor = empty_tensor,
        .quantized = true,
        .quantized_storage = storage,
    });
}

fn populateClipWeights(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    cfg: BenchConfig,
    rng: std.Random,
) !clip_config_mod.Config {
    const clip_cfg = clip_config_mod.Config{
        .text_hidden_size = cfg.clip_text_hidden,
        .text_num_layers = cfg.clip_text_layers,
        .text_num_heads = cfg.clip_text_heads,
        .text_intermediate_size = cfg.clip_text_intermediate,
        .text_max_position_embeddings = @intCast(@max(cfg.seq_len, 77)),
        .vocab_size = cfg.clip_vocab_size,
        .vision_hidden_size = cfg.clip_vision_hidden,
        .vision_num_layers = cfg.clip_vision_layers,
        .vision_num_heads = cfg.clip_vision_heads,
        .vision_intermediate_size = cfg.clip_vision_intermediate,
        .image_size = cfg.clip_image_size,
        .patch_size = cfg.clip_patch_size,
        .projection_dim = cfg.projection_dim,
    };

    const text_h: usize = clip_cfg.text_hidden_size;
    const text_i: usize = clip_cfg.text_intermediate_size;
    const vision_h: usize = clip_cfg.vision_hidden_size;
    const vision_i: usize = clip_cfg.vision_intermediate_size;
    try putRandomWeight(allocator, store, "text_model.embeddings.token_embedding.weight", &.{ @intCast(clip_cfg.vocab_size), @intCast(text_h) }, rng);
    try putRandomWeight(allocator, store, "text_model.embeddings.position_embedding.weight", &.{ @intCast(clip_cfg.text_max_position_embeddings), @intCast(text_h) }, rng);
    try putOnesWeight(allocator, store, "text_model.final_layer_norm.weight", text_h);
    try putZerosWeight(allocator, store, "text_model.final_layer_norm.bias", text_h);
    try putRandom2DWeight(allocator, store, "text_projection.weight", clip_cfg.projection_dim, text_h, rng, cfg.quant);

    var buf: [192]u8 = undefined;
    for (0..clip_cfg.text_num_layers) |layer| {
        const prefix = try std.fmt.bufPrint(&buf, "text_model.encoder.layers.{d}", .{layer});
        try populateClipLayer(allocator, store, prefix, text_h, text_i, rng, cfg.quant);
    }

    const patch_dim: usize = 3 * clip_cfg.patch_size * clip_cfg.patch_size;
    const num_patches: usize = @intCast(clip_cfg.numPatches());
    try putRandomWeight(allocator, store, "vision_model.embeddings.class_embedding", &.{@intCast(vision_h)}, rng);
    try putRandomWeight(allocator, store, "vision_model.embeddings.patch_embedding.weight", &.{ @intCast(vision_h), 3, @intCast(clip_cfg.patch_size), @intCast(clip_cfg.patch_size) }, rng);
    try putRandomWeight(allocator, store, "vision_model.embeddings.position_embedding.weight", &.{ @intCast(num_patches + 1), @intCast(vision_h) }, rng);
    try putOnesWeight(allocator, store, "vision_model.pre_layrnorm.weight", vision_h);
    try putZerosWeight(allocator, store, "vision_model.pre_layrnorm.bias", vision_h);
    try putOnesWeight(allocator, store, "vision_model.post_layernorm.weight", vision_h);
    try putZerosWeight(allocator, store, "vision_model.post_layernorm.bias", vision_h);
    try putRandom2DWeight(allocator, store, "visual_projection.weight", clip_cfg.projection_dim, vision_h, rng, cfg.quant);
    _ = patch_dim;

    for (0..clip_cfg.vision_num_layers) |layer| {
        const prefix = try std.fmt.bufPrint(&buf, "vision_model.encoder.layers.{d}", .{layer});
        try populateClipLayer(allocator, store, prefix, vision_h, vision_i, rng, cfg.quant);
    }

    return clip_cfg;
}

fn populateClipLayer(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    prefix: []const u8,
    hidden: usize,
    intermediate: usize,
    rng: std.Random,
    quant: QuantMode,
) !void {
    var name: [256]u8 = undefined;
    try putOnesWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layer_norm1.weight", .{prefix}), hidden);
    try putZerosWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layer_norm1.bias", .{prefix}), hidden);
    try putOnesWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layer_norm2.weight", .{prefix}), hidden);
    try putZerosWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layer_norm2.bias", .{prefix}), hidden);
    inline for (.{ "q_proj", "k_proj", "v_proj", "out_proj" }) |proj| {
        try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.self_attn.{s}.weight", .{ prefix, proj }), hidden, hidden, rng, quant);
        try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.self_attn.{s}.bias", .{ prefix, proj }), &.{@intCast(hidden)}, rng);
    }
    try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.mlp.fc1.weight", .{prefix}), intermediate, hidden, rng, quant);
    try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.mlp.fc1.bias", .{prefix}), &.{@intCast(intermediate)}, rng);
    try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.mlp.fc2.weight", .{prefix}), hidden, intermediate, rng, quant);
    try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.mlp.fc2.bias", .{prefix}), &.{@intCast(hidden)}, rng);
}

fn populateClapTextWeights(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    cfg: BenchConfig,
    rng: std.Random,
) !clap_config_mod.Config {
    var clap_cfg = clap_config_mod.Config{};
    clap_cfg.text_config.hidden_size = cfg.clap_hidden;
    clap_cfg.text_config.num_hidden_layers = cfg.clap_layers;
    clap_cfg.text_config.num_attention_heads = cfg.clap_heads;
    clap_cfg.text_config.intermediate_size = cfg.clap_intermediate;
    clap_cfg.text_config.vocab_size = cfg.clap_vocab_size;
    clap_cfg.text_config.max_position_embeddings = cfg.clap_max_positions;
    clap_cfg.text_config.type_vocab_size = 1;
    clap_cfg.projection_dim = cfg.projection_dim;

    const H: usize = clap_cfg.text_config.hidden_size;
    const I: usize = clap_cfg.text_config.intermediate_size;
    try putRandomWeight(allocator, store, "text_model.embeddings.word_embeddings.weight", &.{ @intCast(clap_cfg.text_config.vocab_size), @intCast(H) }, rng);
    try putRandomWeight(allocator, store, "text_model.embeddings.position_embeddings.weight", &.{ @intCast(clap_cfg.text_config.max_position_embeddings), @intCast(H) }, rng);
    try putRandomWeight(allocator, store, "text_model.embeddings.token_type_embeddings.weight", &.{ 1, @intCast(H) }, rng);
    try putOnesWeight(allocator, store, "text_model.embeddings.LayerNorm.weight", H);
    try putZerosWeight(allocator, store, "text_model.embeddings.LayerNorm.bias", H);
    try putRandom2DWeight(allocator, store, "text_model.pooler.dense.weight", H, H, rng, cfg.quant);
    try putZerosWeight(allocator, store, "text_model.pooler.dense.bias", H);
    try putRandom2DWeight(allocator, store, "text_projection.linear1.weight", cfg.projection_dim, H, rng, cfg.quant);
    try putRandomWeight(allocator, store, "text_projection.linear1.bias", &.{@intCast(cfg.projection_dim)}, rng);
    try putRandom2DWeight(allocator, store, "text_projection.linear2.weight", cfg.projection_dim, cfg.projection_dim, rng, cfg.quant);
    try putRandomWeight(allocator, store, "text_projection.linear2.bias", &.{@intCast(cfg.projection_dim)}, rng);

    var name: [256]u8 = undefined;
    for (0..clap_cfg.text_config.num_hidden_layers) |layer| {
        const prefix = try std.fmt.bufPrint(&name, "text_model.encoder.layer.{d}", .{layer});
        try populateClapTextLayer(allocator, store, prefix, H, I, rng, cfg.quant);
    }
    return clap_cfg;
}

fn populateClapAudioWeights(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    cfg: BenchConfig,
    rng: std.Random,
) !clap_config_mod.Config {
    var clap_cfg = clap_config_mod.Config{};
    clap_cfg.projection_dim = cfg.projection_dim;
    var ac = clap_cfg.audio_config;
    ac.patch_embeds_hidden_size = cfg.clap_audio_patch_hidden;
    ac.hidden_size = cfg.clap_audio_patch_hidden << 3;
    ac.num_mel_bins = cfg.clap_audio_mel_bins;
    ac.spec_size = cfg.clap_audio_spec_size;
    ac.window_size = cfg.clap_audio_window_size;
    ac.patch_size = 4;
    ac.patch_stride = .{ 4, 4 };
    ac.depths = .{
        cfg.clap_audio_depth,
        cfg.clap_audio_depth,
        cfg.clap_audio_depth,
        cfg.clap_audio_depth,
    };
    ac.num_attention_heads = .{ 4, 4, 8, 8 };
    ac.enable_fusion = false;
    ac.enable_patch_fusion = false;
    ac.enable_patch_layer_norm = true;
    clap_cfg.audio_config = ac;

    const mel_bins: usize = ac.num_mel_bins;
    try putOnesWeight(allocator, store, "audio_model.audio_encoder.batch_norm.weight", mel_bins);
    try putZerosWeight(allocator, store, "audio_model.audio_encoder.batch_norm.bias", mel_bins);
    try putZerosWeight(allocator, store, "audio_model.audio_encoder.batch_norm.running_mean", mel_bins);
    try putOnesWeight(allocator, store, "audio_model.audio_encoder.batch_norm.running_var", mel_bins);

    const patch_hidden: usize = ac.patch_embeds_hidden_size;
    try putRandomWeight(allocator, store, "audio_model.audio_encoder.patch_embed.proj.weight", &.{ @intCast(patch_hidden), 1, @intCast(ac.patch_size), @intCast(ac.patch_size) }, rng);
    try putRandomWeight(allocator, store, "audio_model.audio_encoder.patch_embed.proj.bias", &.{@intCast(patch_hidden)}, rng);
    try putOnesWeight(allocator, store, "audio_model.audio_encoder.patch_embed.norm.weight", patch_hidden);
    try putZerosWeight(allocator, store, "audio_model.audio_encoder.patch_embed.norm.bias", patch_hidden);

    var prefix_buf: [256]u8 = undefined;
    var name: [320]u8 = undefined;
    for (0..clap_config_mod.Config.audio_stage_count) |stage| {
        const dim: usize = ac.stageDim(stage);
        const inner_dim = dim * 4;
        for (0..ac.depths[stage]) |block| {
            const prefix = try std.fmt.bufPrint(&prefix_buf, "audio_model.audio_encoder.layers.{d}.blocks.{d}", .{ stage, block });
            try putOnesWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layernorm_before.weight", .{prefix}), dim);
            try putZerosWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layernorm_before.bias", .{prefix}), dim);
            inline for (.{ "query", "key", "value" }) |proj| {
                try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.self.{s}.weight", .{ prefix, proj }), dim, dim, rng, cfg.quant);
                try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.self.{s}.bias", .{ prefix, proj }), &.{@intCast(dim)}, rng);
            }
            const side = 2 * ac.window_size - 1;
            try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.self.relative_position_bias_table", .{prefix}), &.{ @intCast(side * side), @intCast(ac.num_attention_heads[stage]) }, rng);
            try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.output.dense.weight", .{prefix}), dim, dim, rng, cfg.quant);
            try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.output.dense.bias", .{prefix}), &.{@intCast(dim)}, rng);
            try putOnesWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layernorm_after.weight", .{prefix}), dim);
            try putZerosWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.layernorm_after.bias", .{prefix}), dim);
            try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.intermediate.dense.weight", .{prefix}), inner_dim, dim, rng, cfg.quant);
            try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.intermediate.dense.bias", .{prefix}), &.{@intCast(inner_dim)}, rng);
            try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.output.dense.weight", .{prefix}), dim, inner_dim, rng, cfg.quant);
            try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.output.dense.bias", .{prefix}), &.{@intCast(dim)}, rng);
        }

        if (stage + 1 < clap_config_mod.Config.audio_stage_count) {
            try putOnesWeight(allocator, store, try std.fmt.bufPrint(&name, "audio_model.audio_encoder.layers.{d}.downsample.norm.weight", .{stage}), dim * 4);
            try putZerosWeight(allocator, store, try std.fmt.bufPrint(&name, "audio_model.audio_encoder.layers.{d}.downsample.norm.bias", .{stage}), dim * 4);
            try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "audio_model.audio_encoder.layers.{d}.downsample.reduction.weight", .{stage}), dim * 2, dim * 4, rng, cfg.quant);
        }
    }

    const hidden: usize = ac.hidden_size;
    try putOnesWeight(allocator, store, "audio_model.audio_encoder.norm.weight", hidden);
    try putZerosWeight(allocator, store, "audio_model.audio_encoder.norm.bias", hidden);
    try putRandom2DWeight(allocator, store, "audio_projection.linear1.weight", cfg.projection_dim, hidden, rng, cfg.quant);
    try putRandomWeight(allocator, store, "audio_projection.linear1.bias", &.{@intCast(cfg.projection_dim)}, rng);
    try putRandom2DWeight(allocator, store, "audio_projection.linear2.weight", cfg.projection_dim, cfg.projection_dim, rng, cfg.quant);
    try putRandomWeight(allocator, store, "audio_projection.linear2.bias", &.{@intCast(cfg.projection_dim)}, rng);
    return clap_cfg;
}

fn populateClapTextLayer(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    prefix: []const u8,
    hidden: usize,
    intermediate: usize,
    rng: std.Random,
    quant: QuantMode,
) !void {
    var name: [256]u8 = undefined;
    inline for (.{ "query", "key", "value" }) |proj| {
        try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.self.{s}.weight", .{ prefix, proj }), hidden, hidden, rng, quant);
        try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.self.{s}.bias", .{ prefix, proj }), &.{@intCast(hidden)}, rng);
    }
    try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.output.dense.weight", .{prefix}), hidden, hidden, rng, quant);
    try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.output.dense.bias", .{prefix}), &.{@intCast(hidden)}, rng);
    try putOnesWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.output.LayerNorm.weight", .{prefix}), hidden);
    try putZerosWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.attention.output.LayerNorm.bias", .{prefix}), hidden);
    try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.intermediate.dense.weight", .{prefix}), intermediate, hidden, rng, quant);
    try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.intermediate.dense.bias", .{prefix}), &.{@intCast(intermediate)}, rng);
    try putRandom2DWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.output.dense.weight", .{prefix}), hidden, intermediate, rng, quant);
    try putRandomWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.output.dense.bias", .{prefix}), &.{@intCast(hidden)}, rng);
    try putOnesWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.output.LayerNorm.weight", .{prefix}), hidden);
    try putZerosWeight(allocator, store, try std.fmt.bufPrint(&name, "{s}.output.LayerNorm.bias", .{prefix}), hidden);
}

fn buildTokenInputs(
    allocator: std.mem.Allocator,
    batch: usize,
    seq_len: usize,
    vocab_size: u32,
    pad_token_id: i64,
) !struct { ids: []i64, mask: []i64, token_types: []i64 } {
    const total = batch * seq_len;
    const ids = try allocator.alloc(i64, total);
    errdefer allocator.free(ids);
    const mask = try allocator.alloc(i64, total);
    errdefer allocator.free(mask);
    const token_types = try allocator.alloc(i64, total);
    errdefer allocator.free(token_types);

    for (0..batch) |b| {
        for (0..seq_len) |s| {
            const idx = b * seq_len + s;
            ids[idx] = @intCast(2 + ((s * 17 + b * 13) % @as(usize, @intCast(vocab_size - 2))));
            mask[idx] = 1;
            token_types[idx] = 0;
        }
        if (seq_len > 0) ids[b * seq_len + seq_len - 1] = pad_token_id + 1;
    }
    return .{ .ids = ids, .mask = mask, .token_types = token_types };
}

fn buildPixels(
    allocator: std.mem.Allocator,
    batch: usize,
    image_size: usize,
) ![]f32 {
    const pixels = try allocator.alloc(f32, batch * 3 * image_size * image_size);
    var prng = std.Random.DefaultPrng.init(0xC11C_A11D);
    const rng = prng.random();
    for (pixels) |*v| v.* = (rng.float(f32) - 0.5) * 2.0;
    return pixels;
}

fn buildMelFeatures(
    allocator: std.mem.Allocator,
    batch: usize,
    channels: usize,
    time_frames: usize,
    mel_bins: usize,
) ![]f32 {
    const features = try allocator.alloc(f32, batch * channels * time_frames * mel_bins);
    var prng = std.Random.DefaultPrng.init(0xC1A9_A9D1);
    const rng = prng.random();
    for (features) |*v| v.* = (rng.float(f32) - 0.5) * 2.0;
    return features;
}

fn deinitWeightStore(allocator: std.mem.Allocator, store: *WeightStore) void {
    native_compute.deinitPrefetchQueue(store);
    var it = store.resident_weights.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit();
    }
    store.resident_weights.deinit(allocator);
}

fn populateCudaWeights(
    allocator: std.mem.Allocator,
    cuda: anytype,
    store: *WeightStore,
) !void {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    var it = store.resident_weights.iterator();
    while (it.next()) |entry| {
        const owned_key = try allocator.dupe(u8, entry.key_ptr.*);
        errdefer allocator.free(owned_key);
        try cuda.insertWeightFromLoaded(owned_key, entry.value_ptr);
    }
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn nsToSeconds(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e9;
}

const BenchTiming = struct {
    warmup_ns: u64,
    total_ns: u64,
    avg_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    min_ns: u64,
    max_ns: u64,
    iters: usize,

    fn throughput(self: BenchTiming, batch: usize) f64 {
        if (self.total_ns == 0) return 0;
        return @as(f64, @floatFromInt(batch * self.iters)) / nsToSeconds(self.total_ns);
    }
};

const BenchResult = struct {
    target: BenchTarget,
    variant: []const u8,
    quant: QuantMode,
    batch: usize,
    timing: BenchTiming,
    dispatch: native_compute.NativeQuantDispatchStats = .{},
};

fn timingFromSamples(
    allocator: std.mem.Allocator,
    warmup_ns: u64,
    samples: []const u64,
) !BenchTiming {
    if (samples.len == 0) return error.InvalidMeasureIters;
    const sorted = try allocator.dupe(u64, samples);
    defer allocator.free(sorted);
    std.mem.sort(u64, sorted, {}, std.sort.asc(u64));

    var total: u64 = 0;
    for (samples) |sample| total += sample;
    const p50_idx = percentileIndex(sorted.len, 50);
    const p95_idx = percentileIndex(sorted.len, 95);
    return .{
        .warmup_ns = warmup_ns,
        .total_ns = total,
        .avg_ns = total / samples.len,
        .p50_ns = sorted[p50_idx],
        .p95_ns = sorted[p95_idx],
        .min_ns = sorted[0],
        .max_ns = sorted[sorted.len - 1],
        .iters = samples.len,
    };
}

fn percentileIndex(len: usize, percentile: usize) usize {
    if (len <= 1) return 0;
    const rank = (len * percentile + 99) / 100;
    return @min(len - 1, if (rank == 0) 0 else rank - 1);
}

fn checksum(values: []const f32) f64 {
    var sum: f64 = 0;
    const limit = @min(values.len, 32);
    for (values[0..limit]) |v| sum += v;
    return sum;
}

fn runClipText(
    allocator: std.mem.Allocator,
    cb: anytype,
    clip_cfg: clip_config_mod.Config,
    inputs: anytype,
    cfg: BenchConfig,
) !u64 {
    const start = nowNs();
    const out = try clip_arch.textEncoderForward(cb, allocator, clip_cfg, inputs.ids, cfg.batch, cfg.seq_len);
    defer allocator.free(out);
    const elapsed = nowNs() - start;
    if (!std.math.isFinite(checksum(out))) return error.NonFiniteOutput;
    return elapsed;
}

fn runClipVision(
    allocator: std.mem.Allocator,
    cb: anytype,
    clip_cfg: clip_config_mod.Config,
    pixels: []const f32,
    cfg: BenchConfig,
) !u64 {
    const start = nowNs();
    const out = try clip_arch.visionEncoderForward(cb, allocator, clip_cfg, pixels, cfg.batch);
    defer allocator.free(out);
    const elapsed = nowNs() - start;
    if (!std.math.isFinite(checksum(out))) return error.NonFiniteOutput;
    return elapsed;
}

fn runClapText(
    allocator: std.mem.Allocator,
    cb: anytype,
    clap_cfg: clap_config_mod.Config,
    inputs: anytype,
    cfg: BenchConfig,
) !u64 {
    const start = nowNs();
    const out = try clap_arch.textEncoderForward(cb, allocator, clap_cfg, inputs.ids, inputs.mask, inputs.token_types, cfg.batch, cfg.seq_len);
    defer allocator.free(out);
    const elapsed = nowNs() - start;
    if (!std.math.isFinite(checksum(out))) return error.NonFiniteOutput;
    return elapsed;
}

fn runClapAudio(
    allocator: std.mem.Allocator,
    cb: anytype,
    clap_cfg: clap_config_mod.Config,
    features: []const f32,
    is_longer: []const u8,
    cfg: BenchConfig,
) !u64 {
    const channels: usize = 1;
    const mel_bins: usize = clap_cfg.audio_config.num_mel_bins;
    const time_frames: usize = clap_cfg.audio_config.spec_size * (clap_cfg.audio_config.spec_size / clap_cfg.audio_config.num_mel_bins);
    const start = nowNs();
    const out = try clap_arch.audioEncoderForward(cb, allocator, clap_cfg, features, cfg.batch, channels, time_frames, mel_bins, is_longer);
    defer allocator.free(out);
    const elapsed = nowNs() - start;
    if (!std.math.isFinite(checksum(out))) return error.NonFiniteOutput;
    return elapsed;
}

fn printBenchResult(
    result: BenchResult,
    format: OutputFormat,
) void {
    const timing = result.timing;
    switch (format) {
        .text => {
            std.debug.print(
                "bench target={s} variant={s} quant={s} batch={} warmup_ms={d:.3} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} min_ms={d:.3} max_ms={d:.3} throughput_embeddings_s={d:.2} iters={} dequant_sgemm={} dequant_sgemm_pair={} dequant_sgemm_triple={} q4q5_q8k={} q4q5_q8k_pair={} q4q5_q8k_triple={}",
                .{
                    @tagName(result.target),
                    result.variant,
                    @tagName(result.quant),
                    result.batch,
                    nsToMs(timing.warmup_ns),
                    nsToMs(timing.avg_ns),
                    nsToMs(timing.p50_ns),
                    nsToMs(timing.p95_ns),
                    nsToMs(timing.min_ns),
                    nsToMs(timing.max_ns),
                    timing.throughput(result.batch),
                    timing.iters,
                    result.dispatch.dequant_sgemm,
                    result.dispatch.dequant_sgemm_pair,
                    result.dispatch.dequant_sgemm_triple,
                    result.dispatch.q4_q5_k_q8k_activation,
                    result.dispatch.q4_q5_k_q8k_activation_pair,
                    result.dispatch.q4_q5_k_q8k_activation_triple,
                },
            );
            std.debug.print(
                " packed_qkv_mr4={} packed_qkv_mr2={} q4q5_panel={} q8_0={} q8_0_pair={} q8_0_triple={} q8_k_q8k={} q8k_alloc_ms={d:.3} q8k_quant_ms={d:.3} q4q5_compute_ms={d:.3} q4q5_pair_compute_ms={d:.3} q4q5_triple_compute_ms={d:.3} dequant_fetch_ms={d:.3} dequant_sgemm_compute_ms={d:.3}\n",
                .{
                    result.dispatch.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4,
                    result.dispatch.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2,
                    result.dispatch.q4_q5_k_prepared_panel,
                    result.dispatch.q8_0_direct,
                    result.dispatch.q8_0_pair,
                    result.dispatch.q8_0_triple,
                    result.dispatch.q8_k_q8k_activation,
                    nsToMs(result.dispatch.q8k_activation_alloc_ns),
                    nsToMs(result.dispatch.q8k_activation_quant_ns),
                    nsToMs(result.dispatch.q4q5_q8k_compute_ns),
                    nsToMs(result.dispatch.q4q5_q8k_pair_compute_ns),
                    nsToMs(result.dispatch.q4q5_q8k_triple_compute_ns),
                    nsToMs(result.dispatch.dequant_fetch_ns),
                    nsToMs(result.dispatch.dequant_sgemm_compute_ns),
                },
            );
        },
        .csv => {
            std.debug.print(
                "{s},{s},{s},{},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.2},{},{},{},{},{},{},{}",
                .{
                    @tagName(result.target),
                    result.variant,
                    @tagName(result.quant),
                    result.batch,
                    nsToMs(timing.warmup_ns),
                    nsToMs(timing.avg_ns),
                    nsToMs(timing.p50_ns),
                    nsToMs(timing.p95_ns),
                    nsToMs(timing.min_ns),
                    nsToMs(timing.max_ns),
                    timing.throughput(result.batch),
                    timing.iters,
                    result.dispatch.dequant_sgemm,
                    result.dispatch.dequant_sgemm_pair,
                    result.dispatch.dequant_sgemm_triple,
                    result.dispatch.q4_q5_k_q8k_activation,
                    result.dispatch.q4_q5_k_q8k_activation_pair,
                    result.dispatch.q4_q5_k_q8k_activation_triple,
                },
            );
            std.debug.print(
                ",{},{},{},{},{},{},{},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3}\n",
                .{
                    result.dispatch.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4,
                    result.dispatch.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2,
                    result.dispatch.q4_q5_k_prepared_panel,
                    result.dispatch.q8_0_direct,
                    result.dispatch.q8_0_pair,
                    result.dispatch.q8_0_triple,
                    result.dispatch.q8_k_q8k_activation,
                    nsToMs(result.dispatch.q8k_activation_alloc_ns),
                    nsToMs(result.dispatch.q8k_activation_quant_ns),
                    nsToMs(result.dispatch.q4q5_q8k_compute_ns),
                    nsToMs(result.dispatch.q4q5_q8k_pair_compute_ns),
                    nsToMs(result.dispatch.q4q5_q8k_triple_compute_ns),
                    nsToMs(result.dispatch.dequant_fetch_ns),
                    nsToMs(result.dispatch.dequant_sgemm_compute_ns),
                },
            );
        },
    }
}

fn printCsvHeader(format: OutputFormat) void {
    if (format == .csv) {
        std.debug.print("target,variant,quant,batch,warmup_ms,avg_ms,p50_ms,p95_ms,min_ms,max_ms,throughput_embeddings_s,iters,dequant_sgemm,dequant_sgemm_pair,dequant_sgemm_triple,q4q5_q8k,q4q5_q8k_pair,q4q5_q8k_triple,packed_qkv_mr4,packed_qkv_mr2,q4q5_panel,q8_0,q8_0_pair,q8_0_triple,q8_k_q8k,q8k_alloc_ms,q8k_quant_ms,q4q5_compute_ms,q4q5_pair_compute_ms,q4q5_triple_compute_ms,dequant_fetch_ms,dequant_sgemm_compute_ms\n", .{});
    }
}

fn benchClipText(
    allocator: std.mem.Allocator,
    cb: anytype,
    clip_cfg: clip_config_mod.Config,
    inputs: anytype,
    cfg: BenchConfig,
    variant: []const u8,
) !BenchResult {
    var warmup_ns: u64 = 0;
    for (0..cfg.warmup_iters) |_| warmup_ns = try runClipText(allocator, cb, clip_cfg, inputs, cfg);
    native_compute.resetNativeQuantDispatchStats();
    const samples = try allocator.alloc(u64, cfg.measure_iters);
    defer allocator.free(samples);
    for (samples) |*sample| sample.* = try runClipText(allocator, cb, clip_cfg, inputs, cfg);
    return .{ .target = .clip_text, .variant = variant, .quant = cfg.quant, .batch = cfg.batch, .timing = try timingFromSamples(allocator, warmup_ns, samples), .dispatch = native_compute.nativeQuantDispatchStats() };
}

fn benchClipVision(
    allocator: std.mem.Allocator,
    cb: anytype,
    clip_cfg: clip_config_mod.Config,
    pixels: []const f32,
    cfg: BenchConfig,
    variant: []const u8,
) !BenchResult {
    var warmup_ns: u64 = 0;
    for (0..cfg.warmup_iters) |_| warmup_ns = try runClipVision(allocator, cb, clip_cfg, pixels, cfg);
    native_compute.resetNativeQuantDispatchStats();
    const samples = try allocator.alloc(u64, cfg.measure_iters);
    defer allocator.free(samples);
    for (samples) |*sample| sample.* = try runClipVision(allocator, cb, clip_cfg, pixels, cfg);
    return .{ .target = .clip_vision, .variant = variant, .quant = cfg.quant, .batch = cfg.batch, .timing = try timingFromSamples(allocator, warmup_ns, samples), .dispatch = native_compute.nativeQuantDispatchStats() };
}

fn benchClapText(
    allocator: std.mem.Allocator,
    cb: anytype,
    clap_cfg: clap_config_mod.Config,
    inputs: anytype,
    cfg: BenchConfig,
    variant: []const u8,
) !BenchResult {
    var warmup_ns: u64 = 0;
    for (0..cfg.warmup_iters) |_| warmup_ns = try runClapText(allocator, cb, clap_cfg, inputs, cfg);
    native_compute.resetNativeQuantDispatchStats();
    const samples = try allocator.alloc(u64, cfg.measure_iters);
    defer allocator.free(samples);
    for (samples) |*sample| sample.* = try runClapText(allocator, cb, clap_cfg, inputs, cfg);
    return .{ .target = .clap_text, .variant = variant, .quant = cfg.quant, .batch = cfg.batch, .timing = try timingFromSamples(allocator, warmup_ns, samples), .dispatch = native_compute.nativeQuantDispatchStats() };
}

fn benchClapAudio(
    allocator: std.mem.Allocator,
    cb: anytype,
    clap_cfg: clap_config_mod.Config,
    features: []const f32,
    is_longer: []const u8,
    cfg: BenchConfig,
    variant: []const u8,
) !BenchResult {
    var warmup_ns: u64 = 0;
    for (0..cfg.warmup_iters) |_| warmup_ns = try runClapAudio(allocator, cb, clap_cfg, features, is_longer, cfg);
    native_compute.resetNativeQuantDispatchStats();
    const samples = try allocator.alloc(u64, cfg.measure_iters);
    defer allocator.free(samples);
    for (samples) |*sample| sample.* = try runClapAudio(allocator, cb, clap_cfg, features, is_longer, cfg);
    return .{ .target = .clap_audio, .variant = variant, .quant = cfg.quant, .batch = cfg.batch, .timing = try timingFromSamples(allocator, warmup_ns, samples), .dispatch = native_compute.nativeQuantDispatchStats() };
}

fn printQuantCacheStats(store: *WeightStore) void {
    const cache_stats = native_compute.quantDequantCacheStats(store);
    std.debug.print(
        "quant_dequant_cache entries={} bytes={} hits={} misses={} inserts={} scratch={} disabled={} too_large={} capacity_denied={} tier_denied={}\n",
        .{
            cache_stats.entries,
            cache_stats.bytes,
            cache_stats.hits,
            cache_stats.misses,
            cache_stats.inserts,
            cache_stats.scratch_fallbacks,
            cache_stats.disabled,
            cache_stats.too_large,
            cache_stats.capacity_denied,
            cache_stats.tier_denied,
        },
    );
}

fn runConcreteScenario(
    allocator: std.mem.Allocator,
    base_cfg: BenchConfig,
    target: BenchTarget,
    batch: usize,
    quant: QuantMode,
    variant: []const u8,
) !BenchResult {
    if (target == .all) return error.InvalidTarget;
    var cfg = base_cfg;
    cfg.target = target;
    cfg.batch = batch;
    cfg.quant = quant;
    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer deinitWeightStore(allocator, &weight_store);

    var prng = std.Random.DefaultPrng.init(0xC11C_C1A9);
    const rng = prng.random();

    if (cfg.format == .text) {
        std.debug.print(
            "config: target={s} backend={s} quant={s} batch={} seq_len={} clip_text_layers={} clip_vision_layers={} clap_layers={} clap_audio_depth={} clap_audio_spec={} clap_audio_patch_hidden={}\n",
            .{ @tagName(cfg.target), @tagName(cfg.backend), @tagName(cfg.quant), cfg.batch, cfg.seq_len, cfg.clip_text_layers, cfg.clip_vision_layers, cfg.clap_layers, cfg.clap_audio_depth, cfg.clap_audio_spec_size, cfg.clap_audio_patch_hidden },
        );
    }

    switch (target) {
        .clip_text => {
            const clip_cfg = try populateClipWeights(allocator, &weight_store, cfg, rng);
            const inputs = try buildTokenInputs(allocator, cfg.batch, cfg.seq_len, clip_cfg.vocab_size, 0);
            defer {
                allocator.free(inputs.ids);
                allocator.free(inputs.mask);
                allocator.free(inputs.token_types);
            }
            switch (cfg.backend) {
                .native => {
                    var compute = if (cfg.io) |io| NativeCompute.initWithIo(allocator, &weight_store, null, io) else NativeCompute.init(allocator, &weight_store, null);
                    const cb = compute.computeBackend();
                    const result = try benchClipText(allocator, &cb, clip_cfg, inputs, cfg, variant);
                    if (cfg.format == .text) printQuantCacheStats(&weight_store);
                    return result;
                },
                .cuda => {
                    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
                    var compute = try cuda_compute.CudaCompute.init(allocator);
                    defer compute.deinit();
                    try populateCudaWeights(allocator, &compute, &weight_store);
                    const cb = compute.computeBackend();
                    return try benchClipText(allocator, &cb, clip_cfg, inputs, cfg, variant);
                },
            }
        },
        .clip_vision => {
            const clip_cfg = try populateClipWeights(allocator, &weight_store, cfg, rng);
            const pixels = try buildPixels(allocator, cfg.batch, clip_cfg.image_size);
            defer allocator.free(pixels);
            switch (cfg.backend) {
                .native => {
                    var compute = if (cfg.io) |io| NativeCompute.initWithIo(allocator, &weight_store, null, io) else NativeCompute.init(allocator, &weight_store, null);
                    const cb = compute.computeBackend();
                    const result = try benchClipVision(allocator, &cb, clip_cfg, pixels, cfg, variant);
                    if (cfg.format == .text) printQuantCacheStats(&weight_store);
                    return result;
                },
                .cuda => {
                    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
                    var compute = try cuda_compute.CudaCompute.init(allocator);
                    defer compute.deinit();
                    try populateCudaWeights(allocator, &compute, &weight_store);
                    const cb = compute.computeBackend();
                    return try benchClipVision(allocator, &cb, clip_cfg, pixels, cfg, variant);
                },
            }
        },
        .clap_text => {
            const clap_cfg = try populateClapTextWeights(allocator, &weight_store, cfg, rng);
            const inputs = try buildTokenInputs(allocator, cfg.batch, cfg.seq_len, clap_cfg.text_config.vocab_size, clap_cfg.text_pad_token_id);
            defer {
                allocator.free(inputs.ids);
                allocator.free(inputs.mask);
                allocator.free(inputs.token_types);
            }
            switch (cfg.backend) {
                .native => {
                    var compute = if (cfg.io) |io| NativeCompute.initWithIo(allocator, &weight_store, null, io) else NativeCompute.init(allocator, &weight_store, null);
                    const cb = compute.computeBackend();
                    const result = try benchClapText(allocator, &cb, clap_cfg, inputs, cfg, variant);
                    if (cfg.format == .text) printQuantCacheStats(&weight_store);
                    return result;
                },
                .cuda => {
                    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
                    var compute = try cuda_compute.CudaCompute.init(allocator);
                    defer compute.deinit();
                    try populateCudaWeights(allocator, &compute, &weight_store);
                    const cb = compute.computeBackend();
                    return try benchClapText(allocator, &cb, clap_cfg, inputs, cfg, variant);
                },
            }
        },
        .clap_audio => {
            const clap_audio_cfg = try populateClapAudioWeights(allocator, &weight_store, cfg, rng);
            const audio_channels: usize = 1;
            const audio_mel_bins: usize = clap_audio_cfg.audio_config.num_mel_bins;
            const audio_time_frames: usize = clap_audio_cfg.audio_config.spec_size * (clap_audio_cfg.audio_config.spec_size / clap_audio_cfg.audio_config.num_mel_bins);
            const audio_features = try buildMelFeatures(allocator, cfg.batch, audio_channels, audio_time_frames, audio_mel_bins);
            defer allocator.free(audio_features);
            const is_longer = try allocator.alloc(u8, cfg.batch);
            defer allocator.free(is_longer);
            @memset(is_longer, 0);
            switch (cfg.backend) {
                .native => {
                    var compute = if (cfg.io) |io| NativeCompute.initWithIo(allocator, &weight_store, null, io) else NativeCompute.init(allocator, &weight_store, null);
                    const cb = compute.computeBackend();
                    const result = try benchClapAudio(allocator, &cb, clap_audio_cfg, audio_features, is_longer, cfg, variant);
                    if (cfg.format == .text) printQuantCacheStats(&weight_store);
                    return result;
                },
                .cuda => {
                    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
                    var compute = try cuda_compute.CudaCompute.init(allocator);
                    defer compute.deinit();
                    try populateCudaWeights(allocator, &compute, &weight_store);
                    const cb = compute.computeBackend();
                    return try benchClapAudio(allocator, &cb, clap_audio_cfg, audio_features, is_longer, cfg, variant);
                },
            }
        },
        .all => unreachable,
    }
}

fn printComparison(baseline: BenchResult, candidate: BenchResult, format: OutputFormat) void {
    if (format == .csv) return;
    const base_avg = @as(f64, @floatFromInt(baseline.timing.avg_ns));
    const cand_avg = @as(f64, @floatFromInt(candidate.timing.avg_ns));
    const speedup = if (cand_avg == 0) 0 else base_avg / cand_avg;
    const delta_pct = if (base_avg == 0) 0 else ((cand_avg - base_avg) / base_avg) * 100.0;
    std.debug.print(
        "compare target={s} batch={} baseline={s}/{s} candidate={s}/{s} avg_speedup={d:.3}x avg_delta_pct={d:.2}\n",
        .{
            @tagName(candidate.target),
            candidate.batch,
            baseline.variant,
            @tagName(baseline.quant),
            candidate.variant,
            @tagName(candidate.quant),
            speedup,
            delta_pct,
        },
    );
}

fn runAndPrint(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    target: BenchTarget,
    batch: usize,
) !void {
    if (cfg.compare_dispatch) {
        if (cfg.quant == .none) return error.InvalidQuantMode;
        native_compute.setClipClapDequantSgemmOverrideForBench(null);
        defer native_compute.setClipClapDequantSgemmOverrideForBench(null);

        native_compute.setClipClapDequantSgemmOverrideForBench(false);
        const direct = try runConcreteScenario(allocator, cfg, target, batch, cfg.quant, "direct_quant");
        printBenchResult(direct, cfg.format);

        native_compute.setClipClapDequantSgemmOverrideForBench(true);
        const dequant = try runConcreteScenario(allocator, cfg, target, batch, cfg.quant, "dequant_sgemm");
        printBenchResult(dequant, cfg.format);
        printComparison(direct, dequant, cfg.format);
    } else if (cfg.compare_quant and cfg.quant != .none) {
        const baseline = try runConcreteScenario(allocator, cfg, target, batch, .none, "baseline_dense");
        printBenchResult(baseline, cfg.format);
        const candidate = try runConcreteScenario(allocator, cfg, target, batch, cfg.quant, "candidate_quant");
        printBenchResult(candidate, cfg.format);
        printComparison(baseline, candidate, cfg.format);
    } else {
        const result = try runConcreteScenario(allocator, cfg, target, batch, cfg.quant, "candidate");
        printBenchResult(result, cfg.format);
    }
}

fn targetBatches(target: BenchTarget) []const usize {
    return switch (target) {
        .clip_text, .clap_text => &text_matrix_batches,
        .clip_vision, .clap_audio => &media_matrix_batches,
        .all => &.{},
    };
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    var cfg = try parseArgs(init);
    cfg.io = init.io;

    if (cfg.measure_iters == 0) return error.InvalidMeasureIters;
    printCsvHeader(cfg.format);

    const targets = [_]BenchTarget{ .clip_text, .clip_vision, .clap_text, .clap_audio };
    if (cfg.matrix) {
        for (targets) |target| {
            for (targetBatches(target)) |batch| {
                try runAndPrint(allocator, cfg, target, batch);
            }
        }
        return;
    }

    if (cfg.target == .all) {
        for (targets) |target| try runAndPrint(allocator, cfg, target, cfg.batch);
    } else {
        try runAndPrint(allocator, cfg, cfg.target, cfg.batch);
    }
}
