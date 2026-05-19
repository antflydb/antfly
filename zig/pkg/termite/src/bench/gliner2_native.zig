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

// Synthetic end-to-end GLiNER2 native-backend bench.
//
// Runs the real eager forward pass (deberta_arch.forward followed by
// gliner_head.forward) against a NativeCompute backend backed by random
// weights at DeBERTa-v3-base shapes.  Outputs are nonsense (the weights
// are random) but the compute path is identical to a production
// inference, so wall-clock measurements reflect what optimisations to
// the native kernels actually buy at the model level.
//
// Quantisation: `--quant <mode>` round-trips each 2D weight matrix through
// the matching quant_codec.quantize*FromF32 helper and stuffs a manually
// constructed QuantizedStorage into the WeightStore -- the same shape the
// production GGUF loader produces.  This exercises production quantized
// inference without needing a real quantized checkpoint on disk.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");

const termite_internal = @import("termite_internal");
const deberta_arch = termite_internal.architectures.deberta;
const deberta_config_mod = termite_internal.models.deberta;
const gliner_head = termite_internal.architectures.gliner_head;
const native_compute = termite_internal.native_compute.native;
const NativeCompute = termite_internal.native_compute.native.NativeCompute;
const cuda_compute = if (build_options.enable_cuda) termite_internal.native_compute.cuda else struct {};
const WeightStore = termite_internal.native_compute.native.WeightStore;
const LoadedWeight = termite_internal.models.weight_source.LoadedWeight;
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

const BackendMode = enum {
    native,
    cuda,
};

const BenchConfig = struct {
    hidden_size: u32 = 768,
    num_layers: u32 = 12,
    num_heads: u32 = 12,
    intermediate_size: u32 = 3072,
    vocab_size: u32 = 4096,
    position_buckets: u32 = 256,
    max_position_embeddings: u32 = 512,
    num_labels: u32 = 8,
    seq_len: usize = 256,
    batch: usize = 1,
    warmup_iters: usize = 1,
    measure_iters: usize = 3,
    /// Quantization for 2D weight matrices (encoder linears and head MLPs).
    /// Weights fall back to dense f32 when the inner dim is not compatible
    /// with the selected GGUF block size.  Embeddings, biases, and layernorm
    /// scales always stay f32.
    quant: QuantMode = .none,
    backend: BackendMode = .native,
};

fn parseArgs(init: std.process.Init) !BenchConfig {
    var cfg = BenchConfig{};
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next(); // skip exe name
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seq-len")) {
            cfg.seq_len = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--num-layers")) {
            cfg.num_layers = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--num-heads")) {
            cfg.num_heads = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--hidden")) {
            cfg.hidden_size = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--intermediate")) {
            cfg.intermediate_size = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--num-labels")) {
            cfg.num_labels = try std.fmt.parseInt(u32, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            cfg.warmup_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            cfg.measure_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--batch")) {
            cfg.batch = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--quant")) {
            const value = args_iter.next() orelse return error.MissingValue;
            if (std.ascii.eqlIgnoreCase(value, "none") or std.ascii.eqlIgnoreCase(value, "f32")) {
                cfg.quant = .none;
            } else if (parseQuantMode(value)) |mode| {
                cfg.quant = mode;
            } else {
                return error.InvalidQuantMode;
            }
        } else if (std.mem.eql(u8, arg, "--backend")) {
            const value = args_iter.next() orelse return error.MissingValue;
            if (std.ascii.eqlIgnoreCase(value, "native")) {
                cfg.backend = .native;
            } else if (std.ascii.eqlIgnoreCase(value, "cuda")) {
                cfg.backend = .cuda;
            } else {
                return error.InvalidBackend;
            }
        }
    }
    return cfg;
}

fn parseQuantMode(value: []const u8) ?QuantMode {
    inline for (@typeInfo(QuantMode).@"enum".fields) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) return @enumFromInt(field.value);
    }
    return null;
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
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(n_elems, 1))));
    for (data) |*v| v.* = (rng.float(f32) * 2.0 - 1.0) * scale;

    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);

    const tensor = try Tensor.initFloat32(allocator, owned_name, shape, data);
    allocator.free(data);

    try store.resident_weights.put(allocator, owned_name, .{ .tensor = tensor });
}

/// Pick the quantization that fits the inner dim.  Otherwise the weight stays
/// dense f32, matching how unsupported GGUF block shapes are handled.
fn pickQuantTypeForRow(in_dim: usize, requested: QuantMode) ?tensor_types.KnownTensorType {
    if (requested == .none) return null;
    const known: tensor_types.KnownTensorType = switch (requested) {
        .none => unreachable,
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

/// Register a 2D weight matrix [out_dim, in_dim].  Quantises into the
/// requested format when the inner dim is compatible; otherwise stores
/// it as dense f32.  Mirrors what the production GGUF loader produces:
/// an empty f32 Tensor (preserving shape metadata) plus a populated
/// QuantizedStorage.  The native_compute getWeight path notices the
/// quantized_storage and wires it into the buf so linearOp routes
/// through linearNoBiasQuantized.
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

    const n_elems = out_dim * in_dim;
    const data = try allocator.alloc(f32, n_elems);
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
    // Q4_K prepared-block cache is populated lazily by ensurePreparedQ4K
    // on the first getWeight call; we account for that in the warmup
    // iteration below.

    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);

    // An empty-data f32 tensor preserves the shape metadata that the
    // backend uses for shape checks; the actual numbers live in `storage`.
    const empty_tensor = try Tensor.initFloat32(allocator, owned_name, &shape, &.{});

    try store.resident_weights.put(allocator, owned_name, .{
        .tensor = empty_tensor,
        .quantized = true,
        .quantized_storage = storage,
    });
}

fn populateEncoderWeights(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    cfg: BenchConfig,
    rng: std.Random,
) !void {
    const H: i64 = @intCast(cfg.hidden_size);
    const I: i64 = @intCast(cfg.intermediate_size);
    const V: i64 = @intCast(cfg.vocab_size);
    const P: i64 = @intCast(cfg.max_position_embeddings);

    try putRandomWeight(allocator, store, "embeddings.word_embeddings.weight", &.{ V, H }, rng);
    try putRandomWeight(allocator, store, "embeddings.LayerNorm.weight", &.{H}, rng);
    try putRandomWeight(allocator, store, "embeddings.LayerNorm.bias", &.{H}, rng);
    try putRandomWeight(allocator, store, "encoder.rel_embeddings.weight", &.{ P, H }, rng);
    try putRandomWeight(allocator, store, "encoder.LayerNorm.weight", &.{H}, rng);
    try putRandomWeight(allocator, store, "encoder.LayerNorm.bias", &.{H}, rng);

    var name_buf: [256]u8 = undefined;
    for (0..cfg.num_layers) |layer| {
        const pfx = try std.fmt.bufPrint(name_buf[0..128], "encoder.layer.{d}.", .{layer});
        const pfx_owned = try allocator.dupe(u8, pfx);
        defer allocator.free(pfx_owned);

        const Suffix2D = struct { name: []const u8, shape: [2]i64 };
        const Suffix1D = struct { name: []const u8, shape: [1]i64 };
        const sufs2d = [_]Suffix2D{
            .{ .name = "attention.self.query_proj.weight", .shape = .{ H, H } },
            .{ .name = "attention.self.key_proj.weight", .shape = .{ H, H } },
            .{ .name = "attention.self.value_proj.weight", .shape = .{ H, H } },
            .{ .name = "attention.output.dense.weight", .shape = .{ H, H } },
            .{ .name = "intermediate.dense.weight", .shape = .{ I, H } },
            .{ .name = "output.dense.weight", .shape = .{ H, I } },
        };
        const sufs1d = [_]Suffix1D{
            .{ .name = "attention.self.query_proj.bias", .shape = .{H} },
            .{ .name = "attention.self.key_proj.bias", .shape = .{H} },
            .{ .name = "attention.self.value_proj.bias", .shape = .{H} },
            .{ .name = "attention.output.dense.bias", .shape = .{H} },
            .{ .name = "attention.output.LayerNorm.weight", .shape = .{H} },
            .{ .name = "attention.output.LayerNorm.bias", .shape = .{H} },
            .{ .name = "intermediate.dense.bias", .shape = .{I} },
            .{ .name = "output.dense.bias", .shape = .{H} },
            .{ .name = "output.LayerNorm.weight", .shape = .{H} },
            .{ .name = "output.LayerNorm.bias", .shape = .{H} },
        };
        for (sufs2d) |s| {
            var full: [256]u8 = undefined;
            const full_name = try std.fmt.bufPrint(&full, "{s}{s}", .{ pfx_owned, s.name });
            try putRandom2DWeight(allocator, store, full_name, @intCast(s.shape[0]), @intCast(s.shape[1]), rng, cfg.quant);
        }
        for (sufs1d) |s| {
            var full: [256]u8 = undefined;
            const full_name = try std.fmt.bufPrint(&full, "{s}{s}", .{ pfx_owned, s.name });
            try putRandomWeight(allocator, store, full_name, &s.shape, rng);
        }
    }
}

fn populateGlinerHeadWeights(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    cfg: BenchConfig,
    rng: std.Random,
) !void {
    const H: i64 = @intCast(cfg.hidden_size);
    const D: i64 = 128; // downscaled-transformer hidden
    const D_FFN: i64 = 256;

    // span_rep: project_start, project_end (H -> 4H -> H), out_project (2H -> 4H -> H)
    const span_mlps = [_]struct { name: []const u8, in_dim: i64, hidden_dim: i64, out_dim: i64 }{
        .{ .name = "span_rep.span_rep_layer.project_start", .in_dim = H, .hidden_dim = 4 * H, .out_dim = H },
        .{ .name = "span_rep.span_rep_layer.project_end", .in_dim = H, .hidden_dim = 4 * H, .out_dim = H },
        .{ .name = "span_rep.span_rep_layer.out_project", .in_dim = 2 * H, .hidden_dim = 4 * H, .out_dim = H },
    };
    var name_buf: [256]u8 = undefined;
    for (span_mlps) |m| {
        // Layer 0: in_dim -> hidden_dim
        const w0_name = try std.fmt.bufPrint(name_buf[0..128], "{s}.0.weight", .{m.name});
        try putRandom2DWeight(allocator, store, w0_name, @intCast(m.hidden_dim), @intCast(m.in_dim), rng, cfg.quant);
        const b0_name = try std.fmt.bufPrint(name_buf[0..128], "{s}.0.bias", .{m.name});
        try putRandomWeight(allocator, store, b0_name, &.{m.hidden_dim}, rng);
        // Layer 3: hidden_dim -> out_dim
        const w3_name = try std.fmt.bufPrint(name_buf[0..128], "{s}.3.weight", .{m.name});
        try putRandom2DWeight(allocator, store, w3_name, @intCast(m.out_dim), @intCast(m.hidden_dim), rng, cfg.quant);
        const b3_name = try std.fmt.bufPrint(name_buf[0..128], "{s}.3.bias", .{m.name});
        try putRandomWeight(allocator, store, b3_name, &.{m.out_dim}, rng);
    }

    // count_embed pos_embedding stays dense -- it's a row-gather, not a matmul.
    try putRandomWeight(allocator, store, "count_embed.pos_embedding.weight", &.{ 4, H }, rng);
    try putRandom2DWeight(allocator, store, "count_embed.gru.weight_ih_l0", @intCast(3 * H), @intCast(H), rng, cfg.quant);
    try putRandom2DWeight(allocator, store, "count_embed.gru.weight_hh_l0", @intCast(3 * H), @intCast(H), rng, cfg.quant);
    try putRandomWeight(allocator, store, "count_embed.gru.bias_ih_l0", &.{3 * H}, rng);
    try putRandomWeight(allocator, store, "count_embed.gru.bias_hh_l0", &.{3 * H}, rng);

    // count_embed.transformer
    try putRandom2DWeight(allocator, store, "count_embed.transformer.in_projector.weight", @intCast(D), @intCast(H), rng, cfg.quant);
    try putRandomWeight(allocator, store, "count_embed.transformer.in_projector.bias", &.{D}, rng);

    // 2 mini-transformer layers (D=128, D_FFN=256, 4 heads, head_dim=32).
    // D=128 isn't a Q4_K super-block multiple -- pickQuantTypeForRow falls
    // back to Q8_0 (or dense f32 if not Q8-eligible either).
    inline for (.{ 0, 1 }) |layer| {
        const layer_str = if (layer == 0) "0" else "1";
        const layer_name = "count_embed.transformer.transformer.layers." ++ layer_str;
        try putRandom2DWeight(allocator, store, layer_name ++ ".self_attn.in_proj_weight", @intCast(3 * D), @intCast(D), rng, cfg.quant);
        try putRandomWeight(allocator, store, layer_name ++ ".self_attn.in_proj_bias", &.{3 * D}, rng);
        try putRandom2DWeight(allocator, store, layer_name ++ ".self_attn.out_proj.weight", @intCast(D), @intCast(D), rng, cfg.quant);
        try putRandomWeight(allocator, store, layer_name ++ ".self_attn.out_proj.bias", &.{D}, rng);
        try putRandomWeight(allocator, store, layer_name ++ ".norm1.weight", &.{D}, rng);
        try putRandomWeight(allocator, store, layer_name ++ ".norm1.bias", &.{D}, rng);
        try putRandomWeight(allocator, store, layer_name ++ ".norm2.weight", &.{D}, rng);
        try putRandomWeight(allocator, store, layer_name ++ ".norm2.bias", &.{D}, rng);
        try putRandom2DWeight(allocator, store, layer_name ++ ".linear1.weight", @intCast(D_FFN), @intCast(D), rng, cfg.quant);
        try putRandomWeight(allocator, store, layer_name ++ ".linear1.bias", &.{D_FFN}, rng);
        try putRandom2DWeight(allocator, store, layer_name ++ ".linear2.weight", @intCast(D), @intCast(D_FFN), rng, cfg.quant);
        try putRandomWeight(allocator, store, layer_name ++ ".linear2.bias", &.{D}, rng);
    }

    // out_projector: 3-layer MLP (concat=D+H -> H, ReLU, H -> H, ReLU, H -> H)
    try putRandom2DWeight(allocator, store, "count_embed.transformer.out_projector.0.weight", @intCast(H), @intCast(D + H), rng, cfg.quant);
    try putRandomWeight(allocator, store, "count_embed.transformer.out_projector.0.bias", &.{H}, rng);
    try putRandom2DWeight(allocator, store, "count_embed.transformer.out_projector.2.weight", @intCast(H), @intCast(H), rng, cfg.quant);
    try putRandomWeight(allocator, store, "count_embed.transformer.out_projector.2.bias", &.{H}, rng);
    try putRandom2DWeight(allocator, store, "count_embed.transformer.out_projector.4.weight", @intCast(H), @intCast(H), rng, cfg.quant);
    try putRandomWeight(allocator, store, "count_embed.transformer.out_projector.4.bias", &.{H}, rng);
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

fn buildInputs(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !struct {
    input_ids: []i64,
    attention_mask: []i64,
    words_mask: []i64,
    span_idx: []i64,
} {
    const total = cfg.batch * cfg.seq_len;
    const input_ids = try allocator.alloc(i64, total);
    const attention_mask = try allocator.alloc(i64, total);
    const words_mask = try allocator.alloc(i64, total);

    // Layout each batch as: [P]/[E] schema preamble (num_labels * 2 + a few),
    // then word tokens.  We don't need realistic tokenization -- only the
    // entity_token_id placement matters so the head finds num_labels labels.
    const entity_id: i64 = @intCast(cfg.vocab_size - 1);
    for (0..cfg.batch) |b| {
        const row_off = b * cfg.seq_len;
        // First: schema region (no words)
        // Place num_labels [E] markers spaced 2 apart starting at index 2.
        for (0..cfg.seq_len) |t| {
            input_ids[row_off + t] = @intCast((t * 7 + 13) % @as(usize, @intCast(cfg.vocab_size - 1))); // arbitrary tokens
            attention_mask[row_off + t] = 1;
            words_mask[row_off + t] = 0;
        }
        var label_count: u32 = 0;
        for (0..cfg.num_labels) |li| {
            const pos = 2 + li * 2;
            if (pos >= cfg.seq_len) break;
            input_ids[row_off + pos] = entity_id;
            label_count += 1;
        }

        // Then: word region after schema.  Mark every token from word_start..end as a word.
        const word_start: usize = @as(usize, label_count) * 2 + 4;
        var word_id: i64 = 1;
        var t = word_start;
        while (t < cfg.seq_len) : (t += 1) {
            // 2 sub-tokens per word so the avg path runs.
            words_mask[row_off + t] = word_id;
            if (t + 1 < cfg.seq_len) {
                words_mask[row_off + t + 1] = word_id;
                t += 1;
            }
            word_id += 1;
        }
    }

    // span_idx: enumerate (start, end) pairs over words, max width 12.
    // We compute against batch 0's word count for shape.
    const num_words: usize = blk: {
        var max_id: i64 = 0;
        for (words_mask) |v| {
            if (v > max_id) max_id = v;
        }
        break :blk @intCast(max_id);
    };
    const max_width: usize = @min(12, num_words);
    const num_spans = num_words * max_width;

    const span_idx = try allocator.alloc(i64, cfg.batch * num_spans * 2);
    for (0..cfg.batch) |b| {
        for (0..num_words) |s| {
            for (0..max_width) |w| {
                const idx = (b * num_spans + s * max_width + w) * 2;
                span_idx[idx] = @intCast(s);
                span_idx[idx + 1] = @intCast(@min(s + w, num_words - 1));
            }
        }
    }

    return .{
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .words_mask = words_mask,
        .span_idx = span_idx,
    };
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

const ForwardTiming = struct {
    total_ns: u64 = 0,
    encoder_ns: u64 = 0,
    head_ns: u64 = 0,
    logits_to_f32_ns: u64 = 0,
    encoder_profile: deberta_arch.EncoderProfile = .{},
    head_profile: gliner_head.ForwardProfile = .{},

    fn add(self: *ForwardTiming, other: ForwardTiming) void {
        self.total_ns += other.total_ns;
        self.encoder_ns += other.encoder_ns;
        self.head_ns += other.head_ns;
        self.logits_to_f32_ns += other.logits_to_f32_ns;
        self.encoder_profile.add(other.encoder_profile);
        self.head_profile.add(other.head_profile);
    }
};

const ForwardRun = struct {
    timing: ForwardTiming,
    logits: []f32,
};

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn runForwardTimed(
    allocator: std.mem.Allocator,
    cb: anytype,
    cfg: BenchConfig,
    inputs: anytype,
    deberta_cfg: deberta_config_mod.Config,
) !ForwardRun {
    const total_start = nowNs();

    var timing = ForwardTiming{};
    var start = nowNs();
    var encoder_profile = deberta_arch.EncoderProfile{};
    const hidden = deberta_arch.forwardCtProfiled(
        cb,
        allocator,
        deberta_cfg,
        inputs.input_ids,
        inputs.attention_mask,
        cfg.batch,
        cfg.seq_len,
        &encoder_profile,
    ) catch |err| {
        std.debug.print("bench gliner2: encoder failed: {s}\n", .{@errorName(err)});
        return err;
    };
    try cb.evalTensor(hidden);
    defer cb.free(hidden);
    timing.encoder_ns = nowNs() - start;
    timing.encoder_profile = encoder_profile;

    var head_profile = gliner_head.ForwardProfile{};
    start = nowNs();
    const head_result = gliner_head.forwardCtProfiled(
        cb,
        allocator,
        hidden,
        inputs.input_ids,
        inputs.words_mask,
        inputs.span_idx,
        cfg.batch,
        cfg.seq_len,
        deberta_cfg.hidden_size,
        deberta_cfg.entity_token_id,
        &head_profile,
    ) catch |err| {
        std.debug.print("bench gliner2: head failed: {s}\n", .{@errorName(err)});
        return err;
    };
    try cb.evalTensor(head_result.logits);
    defer cb.free(head_result.logits);
    timing.head_ns = nowNs() - start;
    timing.head_profile = head_profile;

    start = nowNs();
    const logits = if (head_result.num_labels == 0)
        try allocator.alloc(f32, 0)
    else
        cb.toFloat32(head_result.logits, allocator) catch |err| {
            std.debug.print("bench gliner2: logits download failed: {s}\n", .{@errorName(err)});
            return err;
        };
    timing.logits_to_f32_ns = nowNs() - start;
    timing.total_ns = nowNs() - total_start;

    return .{ .timing = timing, .logits = logits };
}

fn runBenchmark(
    allocator: std.mem.Allocator,
    cb: anytype,
    cfg: BenchConfig,
    inputs: anytype,
    maybe_weight_store: ?*WeightStore,
) !void {
    const deberta_cfg = deberta_config_mod.Config{
        .hidden_size = cfg.hidden_size,
        .num_hidden_layers = cfg.num_layers,
        .num_attention_heads = cfg.num_heads,
        .intermediate_size = cfg.intermediate_size,
        .vocab_size = cfg.vocab_size,
        .max_position_embeddings = cfg.max_position_embeddings,
        .position_buckets = cfg.position_buckets,
        .layer_norm_eps = 1e-7,
        .entity_token_id = @intCast(cfg.vocab_size - 1),
        .num_labels = cfg.num_labels,
    };

    std.debug.print(
        "config: backend={s} batch={} seq_len={} layers={} heads={} hidden={} intermediate={} num_labels={} quant={s}\n",
        .{ @tagName(cfg.backend), cfg.batch, cfg.seq_len, cfg.num_layers, cfg.num_heads, cfg.hidden_size, cfg.intermediate_size, cfg.num_labels, @tagName(cfg.quant) },
    );

    // Warmup + sanity-check the first iteration's output is finite.  Random
    // weights produce random logits, but quantization-induced overflow would
    // surface as Inf/NaN -- a useful trip-wire that the quantized path is
    // wired correctly without needing a real model.
    {
        const warmup = try runForwardTimed(allocator, cb, cfg, inputs, deberta_cfg);
        defer allocator.free(warmup.logits);

        var nan_count: usize = 0;
        var inf_count: usize = 0;
        var max_abs: f32 = 0.0;
        for (warmup.logits) |v| {
            if (std.math.isNan(v)) nan_count += 1;
            if (std.math.isInf(v)) inf_count += 1;
            if (@abs(v) > max_abs) max_abs = @abs(v);
        }
        std.debug.print(
            "warmup: {d:.3} ms, logits_count={}, nan={}, inf={}, max_abs={d:.3}\n",
            .{ nsToMs(warmup.timing.total_ns), warmup.logits.len, nan_count, inf_count, max_abs },
        );
    }
    // Extra warmup iterations beyond the sanity check.
    if (cfg.warmup_iters > 1) {
        for (0..cfg.warmup_iters - 1) |_| {
            const warmup = try runForwardTimed(allocator, cb, cfg, inputs, deberta_cfg);
            allocator.free(warmup.logits);
        }
    }

    // Measure
    if (maybe_weight_store != null) {
        native_compute.resetNativeQuantDispatchStats();
    }
    var total_timing = ForwardTiming{};
    var min_ns: u64 = std.math.maxInt(u64);
    for (0..cfg.measure_iters) |_| {
        const run = try runForwardTimed(allocator, cb, cfg, inputs, deberta_cfg);
        defer allocator.free(run.logits);
        total_timing.add(run.timing);
        if (run.timing.total_ns < min_ns) min_ns = run.timing.total_ns;
    }
    const avg_ns = total_timing.total_ns / cfg.measure_iters;
    const avg_encoder_ns = total_timing.encoder_ns / cfg.measure_iters;
    const avg_head_ns = total_timing.head_ns / cfg.measure_iters;
    const avg_logits_to_f32_ns = total_timing.logits_to_f32_ns / cfg.measure_iters;
    const avg_encoder_profile = deberta_arch.EncoderProfile{
        .embeddings_ns = total_timing.encoder_profile.embeddings_ns / cfg.measure_iters,
        .relative_position_ns = total_timing.encoder_profile.relative_position_ns / cfg.measure_iters,
        .layer_total_ns = total_timing.encoder_profile.layer_total_ns / cfg.measure_iters,
        .qkv_ns = total_timing.encoder_profile.qkv_ns / cfg.measure_iters,
        .relative_qk_ns = total_timing.encoder_profile.relative_qk_ns / cfg.measure_iters,
        .attention_ns = total_timing.encoder_profile.attention_ns / cfg.measure_iters,
        .attention_output_ns = total_timing.encoder_profile.attention_output_ns / cfg.measure_iters,
        .ffn_intermediate_ns = total_timing.encoder_profile.ffn_intermediate_ns / cfg.measure_iters,
        .ffn_output_ns = total_timing.encoder_profile.ffn_output_ns / cfg.measure_iters,
        .layernorm_residual_ns = total_timing.encoder_profile.layernorm_residual_ns / cfg.measure_iters,
    };
    const avg_profile = gliner_head.ForwardProfile{
        .materialize_hidden_ns = total_timing.head_profile.materialize_hidden_ns / cfg.measure_iters,
        .extract_words_ns = total_timing.head_profile.extract_words_ns / cfg.measure_iters,
        .extract_labels_ns = total_timing.head_profile.extract_labels_ns / cfg.measure_iters,
        .span_info_ns = total_timing.head_profile.span_info_ns / cfg.measure_iters,
        .span_marker_ns = total_timing.head_profile.span_marker_ns / cfg.measure_iters,
        .span_word_to_ct_ns = total_timing.head_profile.span_word_to_ct_ns / cfg.measure_iters,
        .span_start_end_mlp_ns = total_timing.head_profile.span_start_end_mlp_ns / cfg.measure_iters,
        .span_start_end_first_linear_ns = total_timing.head_profile.span_start_end_first_linear_ns / cfg.measure_iters,
        .span_start_end_relu_ns = total_timing.head_profile.span_start_end_relu_ns / cfg.measure_iters,
        .span_start_end_second_linear_ns = total_timing.head_profile.span_start_end_second_linear_ns / cfg.measure_iters,
        .span_gather_concat_relu_ns = total_timing.head_profile.span_gather_concat_relu_ns / cfg.measure_iters,
        .span_out_project_ns = total_timing.head_profile.span_out_project_ns / cfg.measure_iters,
        .span_out_project_first_linear_ns = total_timing.head_profile.span_out_project_first_linear_ns / cfg.measure_iters,
        .span_out_project_relu_ns = total_timing.head_profile.span_out_project_relu_ns / cfg.measure_iters,
        .span_out_project_second_linear_ns = total_timing.head_profile.span_out_project_second_linear_ns / cfg.measure_iters,
        .label_projection_ns = total_timing.head_profile.label_projection_ns / cfg.measure_iters,
        .logits_ns = total_timing.head_profile.logits_ns / cfg.measure_iters,
    };

    std.debug.print(
        "gliner2_e2e_avg_ms={d:.3} gliner2_e2e_min_ms={d:.3} iters={}\n",
        .{ nsToMs(avg_ns), nsToMs(min_ns), cfg.measure_iters },
    );
    std.debug.print(
        "gliner2_sections_avg_ms encoder={d:.3} head_ct={d:.3} logits_to_f32={d:.3}\n",
        .{ nsToMs(avg_encoder_ns), nsToMs(avg_head_ns), nsToMs(avg_logits_to_f32_ns) },
    );
    std.debug.print(
        "deberta_encoder_avg_ms embeddings={d:.3} relative_pos={d:.3} layers_total={d:.3} qkv={d:.3} relative_qk={d:.3} attention={d:.3} attn_output={d:.3} ffn_intermediate={d:.3} ffn_output={d:.3} residual_norm={d:.3}\n",
        .{
            nsToMs(avg_encoder_profile.embeddings_ns),
            nsToMs(avg_encoder_profile.relative_position_ns),
            nsToMs(avg_encoder_profile.layer_total_ns),
            nsToMs(avg_encoder_profile.qkv_ns),
            nsToMs(avg_encoder_profile.relative_qk_ns),
            nsToMs(avg_encoder_profile.attention_ns),
            nsToMs(avg_encoder_profile.attention_output_ns),
            nsToMs(avg_encoder_profile.ffn_intermediate_ns),
            nsToMs(avg_encoder_profile.ffn_output_ns),
            nsToMs(avg_encoder_profile.layernorm_residual_ns),
        },
    );
    std.debug.print(
        "gliner2_head_avg_ms hidden_to_f32={d:.3} extract_words={d:.3} extract_labels={d:.3} span_info={d:.3} span_marker={d:.3} label_projection={d:.3} logits={d:.3}\n",
        .{
            nsToMs(avg_profile.materialize_hidden_ns),
            nsToMs(avg_profile.extract_words_ns),
            nsToMs(avg_profile.extract_labels_ns),
            nsToMs(avg_profile.span_info_ns),
            nsToMs(avg_profile.span_marker_ns),
            nsToMs(avg_profile.label_projection_ns),
            nsToMs(avg_profile.logits_ns),
        },
    );
    std.debug.print(
        "gliner2_span_avg_ms word_to_ct={d:.3} start_end_mlp={d:.3} gather_concat_relu={d:.3} out_project={d:.3}\n",
        .{
            nsToMs(avg_profile.span_word_to_ct_ns),
            nsToMs(avg_profile.span_start_end_mlp_ns),
            nsToMs(avg_profile.span_gather_concat_relu_ns),
            nsToMs(avg_profile.span_out_project_ns),
        },
    );
    std.debug.print(
        "gliner2_span_mlp_avg_ms start_end_first_linear={d:.3} start_end_relu={d:.3} start_end_second_linear={d:.3} out_project_first_linear={d:.3} out_project_relu={d:.3} out_project_second_linear={d:.3}\n",
        .{
            nsToMs(avg_profile.span_start_end_first_linear_ns),
            nsToMs(avg_profile.span_start_end_relu_ns),
            nsToMs(avg_profile.span_start_end_second_linear_ns),
            nsToMs(avg_profile.span_out_project_first_linear_ns),
            nsToMs(avg_profile.span_out_project_relu_ns),
            nsToMs(avg_profile.span_out_project_second_linear_ns),
        },
    );
    if (maybe_weight_store) |weight_store| {
        const cache_stats = native_compute.quantDequantCacheStats(weight_store);
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
        native_compute.nativeQuantDispatchStats().print();
    }
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const cfg = try parseArgs(init);

    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer deinitWeightStore(allocator, &weight_store);

    var prng = std.Random.DefaultPrng.init(0xC1A4B2A5);
    const rng = prng.random();

    try populateEncoderWeights(allocator, &weight_store, cfg, rng);
    try populateGlinerHeadWeights(allocator, &weight_store, cfg, rng);

    const inputs = try buildInputs(allocator, cfg);
    defer {
        allocator.free(inputs.input_ids);
        allocator.free(inputs.attention_mask);
        allocator.free(inputs.words_mask);
        allocator.free(inputs.span_idx);
    }

    switch (cfg.backend) {
        .native => {
            var compute = NativeCompute.init(allocator, &weight_store, null);
            const cb = compute.computeBackend();
            try runBenchmark(allocator, &cb, cfg, inputs, &weight_store);
        },
        .cuda => {
            if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
            var compute = try cuda_compute.CudaCompute.init(allocator);
            defer compute.deinit();
            try populateCudaWeights(allocator, &compute, &weight_store);
            const cb = compute.computeBackend();
            try runBenchmark(allocator, &cb, cfg, inputs, null);
        },
    }
}
