// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! EmbeddingGemma 2 is a bidirectional encoder, not a causal Gemma decoder.
//! This implementation deliberately keeps activations in F32 and streams the
//! projection-only per-layer input instead of allocating [tokens,24,512].
const std = @import("std");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

const model_config = @import("embedding_gemma2_config.zig");
pub const checkpoint_revision = model_config.checkpoint_revision;
pub const recipe_version = model_config.recipe_version;
pub const max_tokens = model_config.max_tokens;
pub const output_dim: usize = 768;
pub const Config = model_config.Config;
pub const isModel = model_config.isModel;
pub const parseConfig = model_config.parseConfig;

pub fn validateWeights(allocator: std.mem.Allocator, store: anytype, cfg: Config) !void {
    if (store.kind() != .safetensors) return error.UnsupportedEmbeddingGemma2Precision;
    const global = .{
        .{ "language_model.embed_tokens.weight", .{ cfg.vocab_size, cfg.hidden_size } },
        .{ "language_model.embedding_projection.weight", .{ cfg.embedding_dim, cfg.hidden_size } },
        .{ "language_model.ple.per_layer_model_projection.weight", .{ cfg.num_hidden_layers * cfg.per_layer_input_dim, cfg.hidden_size } },
    };
    inline for (global) |entry| try requireWeight(allocator, store, entry[0], &.{ @intCast(entry[1][0]), @intCast(entry[1][1]) });
    try requireWeight(allocator, store, "language_model.norm.weight", &.{@intCast(cfg.hidden_size)});
    try requireWeight(allocator, store, "language_model.ple.per_layer_projection_norm.weight", &.{@intCast(cfg.per_layer_input_dim)});
    for (0..cfg.num_hidden_layers) |layer| {
        var buf: [192]u8 = undefined;
        const d = cfg.headDim(layer);
        const matrices = .{
            .{ "self_attn.q_proj.weight", cfg.num_attention_heads * d, cfg.hidden_size },
            .{ "self_attn.k_proj.weight", cfg.kvHeads(layer) * d, cfg.hidden_size },
            .{ "self_attn.v_proj.weight", cfg.kvHeads(layer) * d, cfg.hidden_size },
            .{ "self_attn.o_proj.weight", cfg.hidden_size, cfg.num_attention_heads * d },
            .{ "mlp.gate_proj.weight", cfg.intermediate_size, cfg.hidden_size },
            .{ "mlp.up_proj.weight", cfg.intermediate_size, cfg.hidden_size },
            .{ "mlp.down_proj.weight", cfg.hidden_size, cfg.intermediate_size },
            .{ "ple_block.per_layer_input_gate.weight", cfg.per_layer_input_dim, cfg.hidden_size },
            .{ "ple_block.per_layer_projection.weight", cfg.hidden_size, cfg.per_layer_input_dim },
        };
        inline for (matrices) |entry| {
            const name = try std.fmt.bufPrint(&buf, "language_model.layers.{d}.{s}", .{ layer, entry[0] });
            try requireWeight(allocator, store, name, &.{ @intCast(entry[1]), @intCast(entry[2]) });
        }
        for ([_][]const u8{ "input_layernorm.weight", "post_attention_layernorm.weight", "pre_feedforward_layernorm.weight", "post_feedforward_layernorm.weight", "ple_block.post_per_layer_input_norm.weight", "self_attn.q_norm.weight", "self_attn.k_norm.weight", "layer_scalar" }) |suffix| {
            const name = try std.fmt.bufPrint(&buf, "language_model.layers.{d}.{s}", .{ layer, suffix });
            const dim = if (std.mem.eql(u8, suffix, "layer_scalar")) 1 else if (std.mem.indexOf(u8, suffix, "self_attn.") != null) d else cfg.hidden_size;
            try requireWeight(allocator, store, name, &.{@intCast(dim)});
        }
    }
    if (cfg.vision) {
        try requireWeight(allocator, store, "vision_tower.patch_embedder.input_proj.weight", &.{ 768, 768 });
        try requireWeight(allocator, store, "vision_tower.patch_embedder.position_embedding_table", &.{ 2, 10240, 768 });
        try requireWeight(allocator, store, "embed_vision.embedding_projection.weight", &.{ 512, 768 });
        for (0..16) |layer| {
            var buf: [192]u8 = undefined;
            const matrices = .{ .{ "self_attn.q_proj", 768, 768 }, .{ "self_attn.k_proj", 768, 768 }, .{ "self_attn.v_proj", 768, 768 }, .{ "self_attn.o_proj", 768, 768 }, .{ "mlp.gate_proj", 3072, 768 }, .{ "mlp.up_proj", 3072, 768 }, .{ "mlp.down_proj", 768, 3072 } };
            inline for (matrices) |entry| try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "vision_tower.encoder.layers.{d}.{s}.linear.weight", .{ layer, entry[0] }), &.{ entry[1], entry[2] });
            for ([_][]const u8{ "input_layernorm", "post_attention_layernorm", "pre_feedforward_layernorm", "post_feedforward_layernorm", "self_attn.q_norm", "self_attn.k_norm" }) |name| try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "vision_tower.encoder.layers.{d}.{s}.weight", .{ layer, name }), &.{if (std.mem.startsWith(u8, name, "self_attn")) 64 else 768});
        }
    }
    if (cfg.audio) {
        const globals = [_]struct { name: []const u8, shape: []const i64 }{
            .{ .name = "audio_tower.subsample_conv_projection.layer0.conv.weight", .shape = &.{ 128, 1, 3, 3 } },
            .{ .name = "audio_tower.subsample_conv_projection.layer1.conv.weight", .shape = &.{ 32, 128, 3, 3 } },
            .{ .name = "audio_tower.subsample_conv_projection.layer0.norm.weight", .shape = &.{128} },
            .{ .name = "audio_tower.subsample_conv_projection.layer1.norm.weight", .shape = &.{32} },
            .{ .name = "audio_tower.subsample_conv_projection.input_proj_linear.weight", .shape = &.{ 1024, 1024 } },
            .{ .name = "audio_tower.output_proj.weight", .shape = &.{ 1536, 1024 } },
            .{ .name = "audio_tower.output_proj.bias", .shape = &.{1536} },
            .{ .name = "embed_audio.embedding_projection.weight", .shape = &.{ 512, 1536 } },
        };
        for (globals) |entry| try requireWeight(allocator, store, entry.name, entry.shape);
        for (0..12) |layer| {
            var buf: [192]u8 = undefined;
            const matrices = .{ .{ "feed_forward1.ffw_layer_1", 4096, 1024 }, .{ "feed_forward1.ffw_layer_2", 1024, 4096 }, .{ "feed_forward2.ffw_layer_1", 4096, 1024 }, .{ "feed_forward2.ffw_layer_2", 1024, 4096 }, .{ "lconv1d.linear_start", 2048, 1024 }, .{ "lconv1d.linear_end", 1024, 1024 }, .{ "self_attn.q_proj", 1024, 1024 }, .{ "self_attn.k_proj", 1024, 1024 }, .{ "self_attn.v_proj", 1024, 1024 }, .{ "self_attn.post", 1024, 1024 } };
            inline for (matrices) |entry| {
                try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "audio_tower.layers.{d}.{s}.linear.weight", .{ layer, entry[0] }), &.{ entry[1], entry[2] });
                inline for (.{ "input_min", "input_max", "output_min", "output_max" }) |bound| try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "audio_tower.layers.{d}.{s}.{s}", .{ layer, entry[0], bound }), &.{});
            }
            for ([_][]const u8{ "feed_forward1.pre_layer_norm", "feed_forward1.post_layer_norm", "feed_forward2.pre_layer_norm", "feed_forward2.post_layer_norm", "lconv1d.pre_layer_norm", "lconv1d.conv_norm", "norm_pre_attn", "norm_post_attn", "norm_out" }) |name| try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "audio_tower.layers.{d}.{s}.weight", .{ layer, name }), &.{1024});
            try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "audio_tower.layers.{d}.lconv1d.depthwise_conv1d.weight", .{layer}), &.{ 1024, 1, 5 });
            try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "audio_tower.layers.{d}.self_attn.per_dim_scale", .{layer}), &.{128});
            try requireWeight(allocator, store, try std.fmt.bufPrint(&buf, "audio_tower.layers.{d}.self_attn.relative_k_proj.weight", .{layer}), &.{ 1024, 1024 });
        }
    }
}

fn requireWeight(allocator: std.mem.Allocator, store: anytype, name: []const u8, shape: []const i64) !void {
    var ref = (try store.describeTensorRange(allocator, name)) orelse return error.InvalidEmbeddingGemma2Weight;
    defer ref.deinit(allocator);
    if (ref.dtype != .bf16 and ref.dtype != .f32) return error.UnsupportedEmbeddingGemma2Precision;
    if (!std.mem.eql(i64, shape, ref.shape)) return error.InvalidEmbeddingGemma2Weight;
}

pub fn validDimension(dim: usize) bool {
    return dim > 0 and dim <= output_dim;
}

pub fn taskPrefix(task: []const u8) ![]const u8 {
    const entries = [_]struct { task: []const u8, prefix: []const u8 }{
        .{ .task = "RETRIEVAL_QUERY", .prefix = "task: search result | query: " },
        .{ .task = "RETRIEVAL_DOCUMENT", .prefix = "title: none | text: " },
        .{ .task = "QUESTION_ANSWERING", .prefix = "task: question answering | query: " },
        .{ .task = "FACT_VERIFICATION", .prefix = "task: fact checking | query: " },
        .{ .task = "FACT_CHECKING", .prefix = "task: fact checking | query: " },
        .{ .task = "CODE_RETRIEVAL_QUERY", .prefix = "task: code retrieval | query: " },
        .{ .task = "CODE_RETRIEVAL", .prefix = "task: code retrieval | query: " },
        .{ .task = "CLASSIFICATION", .prefix = "task: classification | query: " },
        .{ .task = "CLUSTERING", .prefix = "task: clustering | query: " },
        .{ .task = "SEMANTIC_SIMILARITY", .prefix = "task: sentence similarity | query: " },
    };
    for (entries) |entry| if (std.mem.eql(u8, task, entry.task)) return entry.prefix;
    return error.UnsupportedEmbeddingTask;
}

fn scaled(cb: *const ComputeBackend, input: CT, scale: f32) !CT {
    return (try cb.multiplyScalar(input, scale)) orelse error.EmbeddingGemma2OperationUnavailable;
}

// The nullable slot owns the source until an operation succeeds. Native can
// then transfer unique storage or release the replaced source at its last use.
// Metal keeps its existing frame-bounded producer lifetimes and slot identities.
fn finishLastUse(cb: *const ComputeBackend, source: *?CT, result: CT) CT {
    if (cb.kind() == .native) {
        if (result != source.*.?) cb.free(source.*.?);
        source.* = null;
    }
    return result;
}

fn geluLastUse(cb: *const ComputeBackend, source: *?CT) !CT {
    const consumed = if (cb.kind() == .native) try cb.unaryConsume(.gelu, source.*.?) else null;
    return finishLastUse(cb, source, consumed orelse try cb.geluNew(source.*.?));
}

fn rmsNormLastUse(cb: *const ComputeBackend, source: *?CT, gamma: CT, dim: usize, eps: f32) !CT {
    const consumed = if (cb.kind() == .native) try cb.rmsNormConsumeInput(source.*.?, gamma, dim, eps) else null;
    return finishLastUse(cb, source, consumed orelse try cb.rmsNorm(source.*.?, gamma, dim, eps));
}

fn reshapeLastUse(cb: *const ComputeBackend, source: *?CT, rows: usize, columns: usize) !CT {
    const result = (try cb.reshape2d(source.*.?, rows, columns)) orelse return error.EmbeddingGemma2OperationUnavailable;
    return finishLastUse(cb, source, result);
}

test "embeddinggemma2 CPU last use preserves aliases allocation failure retry and independent activations" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
    const Mode = enum { unique, aliased, unsupported };
    const values = [_]f32{ -2, -0.5, 0, 0.25, 0.5, 1, 1.5, 2 };
    const gamma_values = [_]f32{ 1, 0.5, -0.25, 2 };
    for ([_]Mode{ .unique, .aliased, .unsupported }) |mode| {
        for ([_]bool{ false, true }) |rms| {
            var budget = Budget{ .backing = a, .limit = 1024 * 1024 };
            var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
            defer store.deinitOwned();
            var compute = native.NativeCompute.init(budget.allocator(), &store, null);
            defer compute.deinit();
            var cb = compute.computeBackend();
            var unsupported = cb.vtable.*;
            if (mode == .unsupported) {
                unsupported.unaryConsume = null;
                unsupported.rmsNormConsumeInput = null;
                cb.vtable = &unsupported;
            }
            const gamma = try cb.fromFloat32Shape(&gamma_values, &.{4});
            defer cb.free(gamma);
            var parent: ?CT = try cb.fromFloat32Shape(&values, &.{ 2, 4 });
            defer if (parent) |value| cb.free(value);
            var source: ?CT = if (mode == .aliased)
                (try cb.reshape2d(parent.?, 2, 4)).?
            else
                try reshapeLastUse(&cb, &parent, 2, 4);
            defer if (source) |value| cb.free(value);
            const original = source.?;
            const live = budget.live;
            budget.limit = live;
            if (mode != .unique) {
                if (rms)
                    try std.testing.expectError(error.OutOfMemory, rmsNormLastUse(&cb, &source, gamma, 4, 1e-6))
                else
                    try std.testing.expectError(error.OutOfMemory, geluLastUse(&cb, &source));
                try std.testing.expectEqual(original, source.?);
                try std.testing.expectEqual(live, budget.live);
                const unchanged = try cb.toFloat32(source.?, a);
                defer a.free(unchanged);
                try std.testing.expectEqualSlices(f32, &values, unchanged);
                budget.limit = 1024 * 1024;
            }
            const result = if (rms) try rmsNormLastUse(&cb, &source, gamma, 4, 1e-6) else try geluLastUse(&cb, &source);
            defer cb.free(result);
            try std.testing.expectEqual(@as(?CT, null), source);
            if (mode == .unique) {
                try std.testing.expectEqual(original, result);
                try std.testing.expectEqual(live, budget.live);
            }
            const actual = try cb.toFloat32(result, a);
            defer a.free(actual);
            for (values, actual, 0..) |input, got, i| {
                const expected: f64 = if (rms) normalized: {
                    var sum: f64 = 0;
                    for (values[i / 4 * 4 ..][0..4]) |value| sum += @as(f64, value) * value;
                    break :normalized @as(f64, input) * gamma_values[i % 4] / @sqrt(sum / 4 + 1e-6);
                } else activated: {
                    const x: f64 = input;
                    break :activated 0.5 * x * (1 + std.math.tanh(@sqrt(2.0 / std.math.pi) * (x + 0.044715 * x * x * x)));
                };
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), got, 1e-6);
            }
            if (parent) |alias| {
                const unchanged = try cb.toFloat32(alias, a);
                defer a.free(unchanged);
                try std.testing.expectEqualSlices(f32, &values, unchanged);
            }
        }
    }
}

test "embeddinggemma2 CPU last use preserves source ownership after operation failure" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var source: ?CT = try cb.fromFloat32Shape(&.{ -1, 0, 1, 2 }, &.{ 1, 4 });
    defer if (source) |value| cb.free(value);
    const original = source.?;
    const Fault = struct {
        fn unary(_: *anyopaque, _: ops.UnaryConsumeOp, _: CT) anyerror!?CT {
            return error.Cancelled;
        }
        fn norm(_: *anyopaque, _: CT, _: CT, _: usize, _: f32) anyerror!?CT {
            return error.Cancelled;
        }
    };
    var failing_vtable = cb.vtable.*;
    failing_vtable.unaryConsume = Fault.unary;
    failing_vtable.rmsNormConsumeInput = Fault.norm;
    var failing_cb = cb;
    failing_cb.vtable = &failing_vtable;
    try std.testing.expectError(error.Cancelled, geluLastUse(&failing_cb, &source));
    try std.testing.expectEqual(original, source.?);
    try std.testing.expectError(error.Cancelled, rmsNormLastUse(&failing_cb, &source, source.?, 4, 1e-6));
    try std.testing.expectEqual(original, source.?);
    try std.testing.expectError(error.InvalidTensorShape, reshapeLastUse(&cb, &source, 2, 3));
    try std.testing.expectEqual(original, source.?);
    const unchanged = try cb.toFloat32(source.?, a);
    defer a.free(unchanged);
    try std.testing.expectEqualSlices(f32, &.{ -1, 0, 1, 2 }, unchanged);
    const result = try geluLastUse(&cb, &source);
    defer cb.free(result);
    try std.testing.expectEqual(@as(?CT, null), source);
}

fn weight(cb: *const ComputeBackend, layer: usize, comptime suffix: []const u8) !CT {
    if (comptime ops.EmbeddingModelWeight.fromSuffix(suffix)) |kind| if (cb.vtable.embeddingModelWeight) |get| {
        if (get(cb.ptr, layer, kind)) |ct| return ct;
    };
    var buf: [192]u8 = undefined;
    return cb.getWeight(try std.fmt.bufPrint(&buf, "language_model.layers.{d}.{s}", .{ layer, suffix }));
}

fn globalWeight(cb: *const ComputeBackend, comptime kind: ops.EmbeddingModelWeight) !CT {
    if (cb.vtable.embeddingModelWeight) |get| if (get(cb.ptr, 0, kind)) |ct| return ct;
    return cb.getWeight(kind.suffix());
}

fn norm(cb: *const ComputeBackend, layer: usize, comptime suffix: []const u8, input: CT, dim: usize, eps: f32) !CT {
    const w = try weight(cb, layer, suffix);
    defer cb.free(w);
    return cb.rmsNorm(input, w, dim, eps);
}

fn normLastUse(cb: *const ComputeBackend, layer: usize, comptime suffix: []const u8, input: *?CT, dim: usize, eps: f32) !CT {
    const w = try weight(cb, layer, suffix);
    defer cb.free(w);
    return rmsNormLastUse(cb, input, w, dim, eps);
}

fn normResidual(cb: *const ComputeBackend, layer: usize, comptime suffix: []const u8, input: *?CT, residual: CT, dim: usize, eps: f32, output_scale: f32) !CT {
    const w = try weight(cb, layer, suffix);
    defer cb.free(w);
    if (try cb.rmsNormResidual(input.*.?, w, residual, dim, eps, 1, output_scale)) |result| return finishLastUse(cb, input, result);
    const normalized = try rmsNormLastUse(cb, input, w, dim, eps);
    defer cb.free(normalized);
    const sum = try cb.add(residual, normalized);
    if (output_scale == 1) return sum;
    defer cb.free(sum);
    return scaled(cb, sum, output_scale);
}

fn linear(cb: *const ComputeBackend, layer: usize, comptime suffix: []const u8, input: CT, rows: usize, in_dim: usize, out_dim: usize) !CT {
    if (comptime ops.EmbeddingModelProjection.fromSuffix(suffix)) |kind| if (cb.vtable.embeddingModelLinear) |run| {
        if (try run(cb.ptr, layer, kind, input, rows, in_dim, out_dim)) |ct| return ct;
    };
    const w = try weight(cb, layer, suffix);
    defer cb.free(w);
    return cb.linearNoBias(input, w, rows, in_dim, out_dim);
}

const Visibility = struct {
    ranges: []u32,
    positions: []i32,
    key_positions: []i32,
    gather: []i64,
    device_gather: ?CT = null,
    global_metadata: ?CT = null,
    window_metadata: ?CT = null,
    windowed: ?[]u32 = null,
    allocator: std.mem.Allocator,
    borrowed: bool = false,
    fn deinit(self: Visibility) void {
        if (self.borrowed) return;
        self.allocator.free(self.ranges);
        self.allocator.free(self.positions);
        self.allocator.free(self.key_positions);
        self.allocator.free(self.gather);
        if (self.windowed) |ranges| self.allocator.free(ranges);
    }
};

fn visibility(allocator: std.mem.Allocator, mask: []const i64, batch: usize, seq: usize) !Visibility {
    const count = batch * seq;
    const ranges = try allocator.alloc(u32, count * 6);
    errdefer allocator.free(ranges);
    @memset(ranges, 0);
    const positions = try allocator.alloc(i32, count);
    errdefer allocator.free(positions);
    var valid: usize = 0;
    for (mask) |m| {
        if (m != 0 and m != 1) return error.InvalidAttentionMask;
        valid += @intFromBool(m == 1);
    }
    const key_positions = try allocator.alloc(i32, valid);
    errdefer allocator.free(key_positions);
    const gather = try allocator.alloc(i64, valid);
    errdefer allocator.free(gather);
    var at: usize = 0;
    for (0..batch) |b| {
        const start = at;
        for (0..seq) |s| {
            const row = b * seq + s;
            positions[row] = @intCast(s);
            if (mask[row] == 1) {
                gather[at] = @intCast(row);
                key_positions[at] = @intCast(s);
                at += 1;
            }
        }
        if (at == start) return error.EmptyEmbeddingInput;
        for (0..seq) |s| {
            ranges[(b * seq + s) * 6] = @intCast(start);
            ranges[(b * seq + s) * 6 + 1] = @intCast(at);
        }
    }
    return .{ .ranges = ranges, .positions = positions, .key_positions = key_positions, .gather = gather, .allocator = allocator };
}

fn visibilityMetadata(cb: *const ComputeBackend, a: std.mem.Allocator, vis: Visibility, ranges: []const u32) !?CT {
    const query_at = std.mem.alignForward(usize, ranges.len, 64);
    const key_at = std.mem.alignForward(usize, query_at + vis.positions.len, 64);
    const words = try a.alloc(i32, key_at + vis.key_positions.len);
    defer a.free(words);
    @memset(words, 0);
    for (ranges, words[0..ranges.len]) |value, *dest| dest.* = @intCast(value);
    @memcpy(words[query_at..][0..vis.positions.len], vis.positions);
    @memcpy(words[key_at..], vis.key_positions);
    return cb.fromInt32Shape(words, &.{@intCast(words.len)});
}

// Keys are position-sorted within each item's range. Restrict the work itself,
// rather than computing a full-sequence score tile and masking it afterwards.
// Keep the original positions so padding holes do not change window semantics.
fn windowedRanges(allocator: std.mem.Allocator, vis: Visibility, window: u32) ![]u32 {
    const ranges = try allocator.dupe(u32, vis.ranges);
    for (vis.positions, 0..) |position, row| {
        const first: i64 = @as(i64, position) - window;
        const last: i64 = @as(i64, position) + window;
        for (0..3) |interval| {
            const at = row * 6 + interval * 2;
            const start: usize = ranges[at];
            const end: usize = ranges[at + 1];
            var lo = start;
            var hi = end;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (vis.key_positions[mid] < first) lo = mid + 1 else hi = mid;
            }
            const lower = lo;
            hi = end;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (vis.key_positions[mid] <= last) lo = mid + 1 else hi = mid;
            }
            ranges[at] = @intCast(lower);
            ranges[at + 1] = @intCast(lo);
        }
    }
    return ranges;
}

test "embeddinggemma2 sliding ranges preserve positions holes and batch isolation" {
    const a = std.testing.allocator;
    const vis = try visibility(a, &.{ 1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1 }, 2, 9);
    defer vis.deinit();
    const ranges = try windowedRanges(a, vis, 2);
    defer a.free(ranges);
    for (0..18) |q| for (vis.gather, 0..) |original, key| {
        const expected = @as(usize, @intCast(original)) / 9 == q / 9 and @abs(@as(i64, vis.positions[q]) - vis.key_positions[key]) <= 2;
        const included = key >= ranges[q * 6] and key < ranges[q * 6 + 1];
        try std.testing.expectEqual(expected, included);
    };
    // Compare the optimized work ranges to independently masked full ranges.
    var queries: [18 * 4]f32 = undefined;
    var keys: [12 * 4]f32 = undefined;
    var values: [12 * 4]f32 = undefined;
    try std.testing.expectEqual(@as(usize, 12), vis.gather.len);
    for (&queries, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)));
    for (&keys, &values, 0..) |*key, *value, i| {
        key.* = @cos(@as(f32, @floatFromInt(i)));
        value.* = @as(f32, @floatFromInt(i % 7)) / 7;
    }
    const linalg = @import("inference_linalg");
    const expected = try linalg.segmentAttentionHost(a, &queries, &keys, &values, vis.ranges, vis.positions, vis.key_positions, 2, 18, 12, 1, 4);
    defer a.free(expected);
    const actual = try linalg.segmentAttentionHost(a, &queries, &keys, &values, ranges, vis.positions, vis.key_positions, std.math.maxInt(u32), 18, 12, 1, 4);
    defer a.free(actual);
    for (actual, expected) |x, y| try std.testing.expectApproxEqAbs(y, x, 1e-6);
}

fn attention(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: Config, input: CT, vis: Visibility, batch: usize, seq: usize, layer: usize) !CT {
    const rows = batch * seq;
    const d = cfg.headDim(layer);
    const kv = cfg.kvHeads(layer);
    const heads = cfg.num_attention_heads;
    var row_identity = vis.gather.len == rows;
    if (row_identity) for (vis.gather, 0..) |row, i| {
        if (row != i) {
            row_identity = false;
            break;
        }
    };
    const fused = cb.vtable.embeddingHeadPrepare != null and (row_identity or vis.device_gather != null);
    const lengths = try allocator.alloc(usize, if (fused) 0 else batch);
    defer allocator.free(lengths);
    @memset(lengths, seq);
    const offsets = try allocator.alloc(usize, if (fused) 0 else batch);
    defer allocator.free(offsets);
    @memset(offsets, 0);
    const grouped = (cb.kind() == .native or cb.kind() == .metal) and cb.vtable.segmentAttentionGrouped != null and !@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_GROUPED_ATTENTION");
    const identity = grouped and row_identity;
    const gathered_heads = if (grouped) kv else heads;
    const ids = try allocator.alloc(i64, if (fused) 0 else vis.gather.len * gathered_heads);
    defer allocator.free(ids);
    if (!fused) for (vis.gather, 0..) |row, i| for (0..gathered_heads) |head| {
        ids[i * gathered_heads + head] = row * @as(i64, @intCast(kv)) + @as(i64, @intCast(if (grouped) head else head / (heads / kv)));
    };
    // Release each projection's producer temporaries before building the next.
    // Full-context CPU execution otherwise retains several redundant Q/K/V
    // copies until attention returns, exceeding the admitted workspace.
    var names: [3][128]u8 = undefined;
    var packed_rows = if (cb.vtable.embeddingModelLinear) |run| try run(cb.ptr, layer, .packed_qkv, input, rows, cfg.hidden_size, (heads + 2 * kv) * d) else null;
    if (packed_rows == null) packed_rows = try cb.linearPackedNamed(input, &.{
        try std.fmt.bufPrint(&names[0], "language_model.layers.{d}.self_attn.q_proj.weight", .{layer}),
        try std.fmt.bufPrint(&names[1], "language_model.layers.{d}.self_attn.k_proj.weight", .{layer}),
        try std.fmt.bufPrint(&names[2], "language_model.layers.{d}.self_attn.v_proj.weight", .{layer}),
    }, &.{ heads * d, kv * d, kv * d }, rows, cfg.hidden_size);
    defer if (packed_rows) |tensor| cb.free(tensor);
    const q = blk: {
        if (fused) {
            const source = packed_rows orelse try linear(cb, layer, "self_attn.q_proj.weight", input, rows, cfg.hidden_size, heads * d);
            defer if (packed_rows == null) cb.free(source);
            const norm_weight = try weight(cb, layer, "self_attn.q_norm.weight");
            defer cb.free(norm_weight);
            break :blk try cb.vtable.embeddingHeadPrepare.?(cb.ptr, source, norm_weight, null, .{
                .source_rows = @intCast(rows),
                .rows = @intCast(rows),
                .source_heads = @intCast(heads),
                .heads = @intCast(heads),
                .dim = @intCast(d),
                .sequence = @intCast(seq),
                .stride = @intCast(if (packed_rows != null) (heads + 2 * kv) * d else heads * d),
                .start = 0,
                .rotate = 1,
                .gather = 0,
                .weighted = 1,
                .eps = cfg.rms_norm_eps,
                .theta = cfg.theta(layer),
                .scale = if (grouped) 1 else @sqrt(@as(f32, @floatFromInt(d))),
            });
        }

        var raw: ?CT = if (packed_rows) |tensor| try cb.sliceLastDim(tensor, 0, heads * d) else try linear(cb, layer, "self_attn.q_proj.weight", input, rows, cfg.hidden_size, heads * d);
        defer if (raw) |value| cb.free(value);
        var shaped: ?CT = try reshapeLastUse(cb, &raw, rows * heads, d);
        defer if (shaped) |value| cb.free(value);
        const normalized = try normLastUse(cb, layer, "self_attn.q_norm.weight", &shaped, d, cfg.rms_norm_eps);
        defer cb.free(normalized);
        const rotated = try cb.ropePerItem(normalized, batch, seq, d, d, cfg.theta(layer), 1, lengths, offsets, false);
        if (grouped) break :blk rotated;
        defer cb.free(rotated);
        // Segment attention scales by rsqrt(d); this encoder scales by 1.
        break :blk try scaled(cb, rotated, @sqrt(@as(f32, @floatFromInt(d))));
    };
    defer cb.free(q);
    const kg = blk: {
        if (fused) {
            const source = packed_rows orelse try linear(cb, layer, "self_attn.k_proj.weight", input, rows, cfg.hidden_size, kv * d);
            defer if (packed_rows == null) cb.free(source);
            const norm_weight = try weight(cb, layer, "self_attn.k_norm.weight");
            defer cb.free(norm_weight);
            break :blk try cb.vtable.embeddingHeadPrepare.?(cb.ptr, source, norm_weight, if (row_identity) null else vis.device_gather, .{
                .source_rows = @intCast(rows),
                .rows = @intCast(vis.gather.len),
                .source_heads = @intCast(kv),
                .heads = @intCast(gathered_heads),
                .dim = @intCast(d),
                .sequence = @intCast(seq),
                .stride = @intCast(if (packed_rows != null) (heads + 2 * kv) * d else kv * d),
                .start = @intCast(if (packed_rows != null) heads * d else 0),
                .rotate = 1,
                .gather = @intFromBool(!row_identity),
                .weighted = 1,
                .eps = cfg.rms_norm_eps,
                .theta = cfg.theta(layer),
                .scale = 1,
            });
        }

        var raw: ?CT = if (packed_rows) |tensor| try cb.sliceLastDim(tensor, heads * d, (heads + kv) * d) else try linear(cb, layer, "self_attn.k_proj.weight", input, rows, cfg.hidden_size, kv * d);
        defer if (raw) |value| cb.free(value);
        var shaped: ?CT = try reshapeLastUse(cb, &raw, rows * kv, d);
        defer if (shaped) |value| cb.free(value);
        const normalized = try normLastUse(cb, layer, "self_attn.k_norm.weight", &shaped, d, cfg.rms_norm_eps);
        defer cb.free(normalized);
        const rotated = try cb.ropePerItem(normalized, batch, seq, d, d, cfg.theta(layer), 1, lengths, offsets, false);
        if (identity) break :blk rotated;
        defer cb.free(rotated);
        break :blk try cb.embeddingLookup(rotated, ids, ids.len, d);
    };
    defer cb.free(kg);
    const vg = blk: {
        if (fused) {
            const source = packed_rows orelse try linear(cb, layer, "self_attn.v_proj.weight", input, rows, cfg.hidden_size, kv * d);
            defer if (packed_rows == null) cb.free(source);
            break :blk try cb.vtable.embeddingHeadPrepare.?(cb.ptr, source, null, if (row_identity) null else vis.device_gather, .{
                .source_rows = @intCast(rows),
                .rows = @intCast(vis.gather.len),
                .source_heads = @intCast(kv),
                .heads = @intCast(gathered_heads),
                .dim = @intCast(d),
                .sequence = @intCast(seq),
                .stride = @intCast(if (packed_rows != null) (heads + 2 * kv) * d else kv * d),
                .start = @intCast(if (packed_rows != null) (heads + kv) * d else 0),
                .rotate = 0,
                .gather = @intFromBool(!row_identity),
                .weighted = 0,
                .eps = cfg.rms_norm_eps,
                .theta = cfg.theta(layer),
                .scale = 1,
            });
        }

        var raw: ?CT = if (packed_rows) |tensor| try cb.sliceLastDim(tensor, (heads + kv) * d, (heads + 2 * kv) * d) else try linear(cb, layer, "self_attn.v_proj.weight", input, rows, cfg.hidden_size, kv * d);
        defer if (raw) |value| cb.free(value);
        var shaped: ?CT = try reshapeLastUse(cb, &raw, rows * kv, d);
        defer if (shaped) |value| cb.free(value);
        const ones = try allocator.alloc(f32, d);
        defer allocator.free(ones);
        @memset(ones, 1);
        const ones_ct = try cb.fromFloat32Shape(ones, &.{@intCast(d)});
        defer cb.free(ones_ct);
        const normalized = try rmsNormLastUse(cb, &shaped, ones_ct, d, cfg.rms_norm_eps);
        defer cb.free(normalized);
        const gathered = (try cb.reshape2d(normalized, rows * kv, d)) orelse return error.EmbeddingGemma2OperationUnavailable;
        if (identity) break :blk gathered;
        defer cb.free(gathered);
        break :blk try cb.embeddingLookup(gathered, ids, ids.len, d);
    };
    defer cb.free(vg);
    // Every head output now owns its rows. Retire the packed producer before
    // attention allocates score/work buffers; encoded consumers remain ordered
    // by the backend's frame ownership and reuse barriers.
    if (packed_rows) |tensor| {
        cb.free(tensor);
        packed_rows = null;
    }
    const restricted = if (cfg.window(layer) != std.math.maxInt(u32)) vis.windowed orelse try windowedRanges(allocator, vis, cfg.window(layer)) else null;
    defer if (vis.windowed == null) if (restricted) |ranges| allocator.free(ranges);
    // windowedRanges already applies the exact position window, including
    // padding holes and batch isolation. Avoid repeating it for every score.
    const request = ops.SegmentAttention{ .ranges = restricted orelse vis.ranges, .query_positions = vis.positions, .key_positions = vis.key_positions, .window = std.math.maxInt(u32), .queries = rows, .keys = vis.gather.len, .num_heads = heads, .head_dim = d };
    const result = blk: {
        if (grouped) {
            const grouped_request = ops.SegmentAttentionGrouped{ .visibility = request, .metadata = if (restricted != null) vis.window_metadata else vis.global_metadata, .num_kv_heads = kv, .score_scale = 1.0, .control = cb.execution_control };
            try cb.checkExecutionControl();
            if (try cb.vtable.segmentAttentionGrouped.?(cb.ptr, q, kg, vg, &grouped_request)) |result| break :blk result;
            // Unsupported device capability declines to the existing device
            // attention, expanding the already compacted grouped rows once.
            const expanded_ids = try allocator.alloc(i64, vis.gather.len * heads);
            defer allocator.free(expanded_ids);
            for (0..vis.gather.len) |row| for (0..heads) |head| {
                expanded_ids[row * heads + head] = @intCast(row * kv + head / (heads / kv));
            };
            const ek = try cb.embeddingLookup(kg, expanded_ids, expanded_ids.len, d);
            defer cb.free(ek);
            const ev = try cb.embeddingLookup(vg, expanded_ids, expanded_ids.len, d);
            defer cb.free(ev);
            const sq = try scaled(cb, q, @sqrt(@as(f32, @floatFromInt(d))));
            defer cb.free(sq);
            break :blk (if (cb.vtable.segmentAttention) |op| try op(cb.ptr, sq, ek, ev, &request) else null) orelse return error.EmbeddingGemma2OperationUnavailable;
        }
        // No implicit host attention fallback for Metal.
        break :blk if (cb.kind() == .metal)
            (if (cb.vtable.segmentAttention) |op| try op(cb.ptr, q, kg, vg, &request) else null) orelse return error.EmbeddingGemma2OperationUnavailable
        else
            try cb.segmentAttention(allocator, q, kg, vg, &request);
    };
    defer cb.free(result);
    return linear(cb, layer, "self_attn.o_proj.weight", result, rows, heads * d, cfg.hidden_size);
}

const LayerFrameBatch = struct {
    slices: [24]CT = undefined,
    count: usize = 0,

    fn deinit(self: *LayerFrameBatch, cb: *const ComputeBackend) void {
        for (self.slices[0..self.count]) |slice| {
            @import("../ops/metal_compute.zig").MetalCompute.releaseDynamicSlotsForTensor(cb, slice);
            cb.free(slice);
        }
    }
};

fn encoderLayer(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: Config, input: CT, embeddings: CT, pw: CT, vis: Visibility, batch: usize, seq: usize, layer: usize, frame_batch: ?*LayerFrameBatch, packed_ple: ?CT, packed_first: usize) !CT {
    if (cb.kind() == .metal and cb.decoderRuntimeHasActiveFrame() and frame_batch == null) return error.EmbeddingGemma2ExternalFrame;
    const rows = batch * seq;
    const h = cfg.hidden_size;
    if (frame_batch) |owner| if (owner.count >= owner.slices.len) return error.EmbeddingGemma2OperationUnavailable;
    const slice: ?CT = if (packed_ple == null) try cb.sliceRows2D(allocator, pw, layer * cfg.per_layer_input_dim, cfg.per_layer_input_dim, h) else null;
    if (slice) |value| if (frame_batch) |owner| {
        owner.slices[owner.count] = value;
        owner.count += 1;
    };
    defer if (frame_batch == null) if (slice) |value| {
        // The identity-keyed transient slot expires after the layer's frame
        // completes (or is cancelled), before this address can be reused.
        if (cb.kind() == .metal) @import("../ops/metal_compute.zig").MetalCompute.releaseDynamicSlotsForTensor(cb, value);
        cb.free(value);
    };
    // Batch the layer's device operations into one owned frame. A frame per
    // layer bounds retained GPU intermediates and preserves cancellation
    // boundaries, without paying a command-buffer wait for every primitive.
    var frame = false;
    if (cb.kind() == .metal and frame_batch == null) {
        frame = try cb.decoderRuntimeBeginFrame();
        if (!frame) return error.EmbeddingGemma2OperationUnavailable;
    }
    defer if (frame) cb.decoderRuntimeCancelFrame() catch {};
    const profile = cb.kind() == .metal and @import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_PROFILE_ENCODER");
    if (profile and frame) try cb.decoderRuntimeSetActiveFrameRegime(.prefill);
    var attention_region = if (profile) try cb.decoderRuntimePushComputeRegion(.attention_project) else ops.DecoderRuntimeComputeRegionScope{};
    defer attention_region.deinit();
    const n = try norm(cb, layer, "input_layernorm.weight", input, h, cfg.rms_norm_eps);
    defer cb.free(n);
    var attn: ?CT = try attention(cb, allocator, cfg, n, vis, batch, seq, layer);
    defer if (attn) |value| cb.free(value);
    const residual = try normResidual(cb, layer, "post_attention_layernorm.weight", &attn, input, h, cfg.rms_norm_eps, 1);
    defer cb.free(residual);
    attention_region.deinit();
    var ffn_region = if (profile) try cb.decoderRuntimePushComputeRegion(.ffn) else ops.DecoderRuntimeComputeRegionScope{};
    defer ffn_region.deinit();
    const fnorm = try norm(cb, layer, "pre_feedforward_layernorm.weight", residual, h, cfg.rms_norm_eps);
    defer cb.free(fnorm);
    var down: ?CT = blk: {
        const product = product: {
            var names: [2][128]u8 = undefined;
            const prepared = if (cb.vtable.embeddingModelLinear) |run| try run(cb.ptr, layer, .packed_ffn, fnorm, rows, h, 2 * cfg.intermediate_size) else null;
            if (prepared orelse try cb.linearPackedNamed(fnorm, &.{
                try std.fmt.bufPrint(&names[0], "language_model.layers.{d}.mlp.gate_proj.weight", .{layer}),
                try std.fmt.bufPrint(&names[1], "language_model.layers.{d}.mlp.up_proj.weight", .{layer}),
            }, &.{ cfg.intermediate_size, cfg.intermediate_size }, rows, h)) |packed_rows| {
                defer cb.free(packed_rows);
                if (cb.vtable.packedGeluMultiply) |op| if (try op(cb.ptr, packed_rows, rows, cfg.intermediate_size)) |product| break :product product;
                const gate = try cb.sliceLastDim(packed_rows, 0, cfg.intermediate_size);
                defer cb.free(gate);
                const up = try cb.sliceLastDim(packed_rows, cfg.intermediate_size, cfg.intermediate_size * 2);
                defer cb.free(up);
                break :product (try cb.activationMultiply(gate, up, .gelu)) orelse return error.EmbeddingGemma2OperationUnavailable;
            }
            var gate: ?CT = try linear(cb, layer, "mlp.gate_proj.weight", fnorm, rows, h, cfg.intermediate_size);
            defer if (gate) |value| cb.free(value);
            const up = try linear(cb, layer, "mlp.up_proj.weight", fnorm, rows, h, cfg.intermediate_size);
            defer cb.free(up);
            if (try cb.activationMultiply(gate.?, up, .gelu)) |fused| break :product fused;
            const activated = try geluLastUse(cb, &gate);
            defer cb.free(activated);
            break :product try cb.multiply(activated, up);
        };
        defer cb.free(product);
        break :blk try linear(cb, layer, "mlp.down_proj.weight", product, rows, cfg.intermediate_size, h);
    };
    defer if (down) |value| cb.free(value);
    const ffn = try normResidual(cb, layer, "post_feedforward_layernorm.weight", &down, residual, h, cfg.rms_norm_eps, 1);
    defer cb.free(ffn);
    ffn_region.deinit();
    var ple_region = if (profile) try cb.decoderRuntimePushComputeRegion(.ple) else ops.DecoderRuntimeComputeRegionScope{};
    defer ple_region.deinit();
    const ple = if (packed_ple) |packed_input|
        try cb.sliceLastDim(packed_input, (layer - packed_first) * cfg.per_layer_input_dim, (layer - packed_first + 1) * cfg.per_layer_input_dim)
    else blk: {
        const projected = try cb.linearNoBias(embeddings, slice.?, rows, h, cfg.per_layer_input_dim);
        defer cb.free(projected);
        const ps = try scaled(cb, projected, 1 / @sqrt(@as(f32, @floatFromInt(h))));
        defer cb.free(ps);
        const pnw = try globalWeight(cb, .ple_norm);
        defer cb.free(pnw);
        break :blk try cb.rmsNorm(ps, pnw, cfg.per_layer_input_dim, cfg.rms_norm_eps);
    };
    defer cb.free(ple);
    var pg: ?CT = try linear(cb, layer, "ple_block.per_layer_input_gate.weight", ffn, rows, h, cfg.per_layer_input_dim);
    defer if (pg) |value| cb.free(value);
    const pp = (try cb.activationMultiply(pg.?, ple, .gelu)) orelse blk: {
        const pga = try geluLastUse(cb, &pg);
        defer cb.free(pga);
        break :blk try cb.multiply(pga, ple);
    };
    defer cb.free(pp);
    var po: ?CT = try linear(cb, layer, "ple_block.per_layer_projection.weight", pp, rows, cfg.per_layer_input_dim, h);
    defer if (po) |value| cb.free(value);
    const scale: f32 = cached: {
        if (cb.vtable.embeddingModelScalar) |get| if (get(cb.ptr, layer)) |value| break :cached value;
        const scalar = try weight(cb, layer, "layer_scalar");
        defer cb.free(scalar);
        const value = try cb.toFloat32(scalar, allocator);
        defer allocator.free(value);
        if (value.len != 1 or !std.math.isFinite(value[0])) return error.InvalidEmbeddingGemma2Weight;
        break :cached value[0];
    };
    const result = try normResidual(cb, layer, "ple_block.post_per_layer_input_norm.weight", &po, ffn, h, cfg.rms_norm_eps, scale);
    errdefer cb.free(result);
    ple_region.deinit();
    if (comptime @import("build_options").enable_metal) {
        if (profile) {
            const compute: *const @import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(cb.ptr));
            const memory = @import("../backends/metal_runtime.zig").runtimeMemorySnapshot(compute.provider_impl.raw_decode_runtime);
            const allocated = @import("../backends/metal_runtime.zig").currentAllocatedSize(compute.provider_impl.raw_decode_runtime);
            std.debug.print("embeddinggemma2_frame_memory: layer={d} global={} frame_retained={d} scratch_pool={d} in_use={d} pending={d} reuse_pool={d} graph_plan={d} total={d} allocated_current={d}\n", .{ layer, cfg.headDim(layer) == 512, memory.frame_retained_bytes, memory.scratch_pool_bytes, memory.scratch_pool_in_use_slots, memory.scratch_pool_pending_slots, memory.reuse_pool_bytes, memory.graph_plan_bytes, memory.total_bytes, allocated });
        }
    }
    if (frame) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame = false;
        if (comptime @import("build_options").enable_metal) {
            if (profile) {
                const compute: *const @import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(cb.ptr));
                std.debug.print("embeddinggemma2_layer_gpu: layer={d} global={} nanos={d}\n", .{ layer, cfg.headDim(layer) == 512, @import("../backends/metal_runtime.zig").lastFrameGpuNanos(compute.provider_impl.raw_decode_runtime) });
            }
        }
        try @import("../ops/metal_compute.zig").MetalCompute.releaseEmbeddingGemma2ActivationViews(cb);
    }
    try cb.checkExecutionControl();
    return result;
}

/// Match the upstream packed projection and norm for bounded row counts. The
/// last dimension is [layer, per-layer width]; each layer later slices its
/// columns without downloading the device result. Long contexts pack only their current
/// owned layer group to avoid a 24-fold activation workspace.
fn packedPerLayerInputs(cb: *const ComputeBackend, cfg: Config, embeddings: CT, pw: CT, rows: usize, shared_frame: bool) !CT {
    var frame = false;
    if (cb.kind() == .metal and !shared_frame) {
        frame = try cb.decoderRuntimeBeginFrame();
        if (!frame) return error.EmbeddingGemma2OperationUnavailable;
    }
    defer if (frame) cb.decoderRuntimeCancelFrame() catch {};
    const width = cfg.num_hidden_layers * cfg.per_layer_input_dim;
    const projected = try cb.linearNoBias(embeddings, pw, rows, cfg.hidden_size, width);
    defer cb.free(projected);
    const ps = try scaled(cb, projected, 1 / @sqrt(@as(f32, @floatFromInt(cfg.hidden_size))));
    defer cb.free(ps);
    const shaped = (try cb.reshape2d(ps, rows * cfg.num_hidden_layers, cfg.per_layer_input_dim)) orelse return error.EmbeddingGemma2OperationUnavailable;
    defer cb.free(shaped);
    const pnw = try globalWeight(cb, .ple_norm);
    defer cb.free(pnw);
    const normalized = try cb.rmsNorm(shaped, pnw, cfg.per_layer_input_dim, cfg.rms_norm_eps);
    defer cb.free(normalized);
    const result = (try cb.reshape2d(normalized, rows, width)) orelse return error.EmbeddingGemma2OperationUnavailable;
    errdefer cb.free(result);
    if (frame) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame = false;
        try @import("../ops/metal_compute.zig").MetalCompute.releaseEmbeddingGemma2ActivationViews(cb);
    }
    return result;
}

pub fn forwardCT(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: Config, ids: []const i64, mask: []const i64, batch: usize, seq: usize) !CT {
    if (cb.kind() != .native and cb.kind() != .metal and cb.kind() != .wasm) return error.UnsupportedEmbeddingGemma2Backend;
    if (cb.kind() == .metal and (cb.decoderRuntimeHasActiveFrame() or cb.decoderRuntimeHasSubmittedFrame())) return error.EmbeddingGemma2ExternalFrame;
    if (batch == 0 or seq == 0 or seq > max_tokens) return error.InvalidEmbeddingInputLength;
    const rows = std.math.mul(usize, batch, seq) catch return error.InvalidEmbeddingInputLength;
    if (ids.len != rows or mask.len != rows) return error.InvalidInputShape;
    for (ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidTokenId;
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, ids, rows, cfg.hidden_size);
    defer cb.free(raw);
    const embeddings = try scaled(cb, raw, @sqrt(@as(f32, @floatFromInt(cfg.hidden_size))));
    defer cb.free(embeddings);
    return forwardEmbeddingsCT(cb, allocator, cfg, embeddings, mask, batch, seq);
}

/// Already scaled hard-token embeddings with unscaled projected media tokens.
fn metalFrameRows() usize {
    return if (@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_MEDIUM_METAL_FRAME")) 128 else 512;
}
/// Independent rows use segmented frames and packed PLE while their admitted
/// workspace permits it; total rows no longer force a singleton-only regime.
pub fn metalBatchCapacity(sequence: usize, maximum: usize) usize {
    if (sequence == 0) return 0;
    return @min(maximum, 8192 / sequence);
}
fn metalLayerGroup(cfg: Config, rows: usize) usize {
    if (rows <= metalFrameRows()) return cfg.num_hidden_layers;
    // The existing run envelope supplies 192 KiB/row. Reserve one third for
    // packed inputs, attention and outputs. Two groups may be live: the
    // submitted predecessor and its encoding successor each receive 64 KiB/row.
    const per_row = (12 * cfg.hidden_size + 3 * cfg.intermediate_size + 4 * cfg.per_layer_input_dim) * @sizeOf(f32);
    return @max(@as(usize, 1), @min(@as(usize, 8), (64 * 1024) / @max(per_row, 1)));
}
fn packPerLayerInputs(cfg: Config, rows: usize) bool {
    const elements = std.math.mul(usize, rows, cfg.num_hidden_layers * cfg.per_layer_input_dim) catch return false;
    return elements <= 64 * 1024 * 1024 / @sizeOf(f32);
}

test "embeddinggemma2 video cohorts cross former frame boundaries without singleton fallback" {
    try std.testing.expectEqual(@as(usize, 4), metalBatchCapacity(386, 4));
    try std.testing.expectEqual(@as(usize, 4), metalBatchCapacity(513, 4));
    try std.testing.expectEqual(@as(usize, 2), metalBatchCapacity(4096, 4));
    try std.testing.expectEqual(@as(usize, 0), metalBatchCapacity(0, 4));
    const cfg = Config{};
    try std.testing.expect(metalLayerGroup(cfg, 772) > 0);
    try std.testing.expect(metalLayerGroup(cfg, 772) <= 8);
    try std.testing.expect(packPerLayerInputs(cfg, 772));
    try std.testing.expect(!packPerLayerInputs(cfg, 8192));
}

pub fn forwardEmbeddingsCT(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: Config, embeddings: CT, mask: []const i64, batch: usize, seq: usize) !CT {
    return forwardEmbeddingsImpl(cb, allocator, cfg, embeddings, mask, batch, seq, null);
}

/// Embedding-only output: normalize tokens, pool, project once per item, then
/// truncate and normalize. Token-level callers retain forwardEmbeddingsCT.
pub fn forwardPooledEmbeddingsCT(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: Config, embeddings: CT, mask: []const i64, batch: usize, seq: usize, dimensions: usize) !CT {
    if (!validDimension(dimensions)) return error.InvalidEmbeddingDimensions;
    return forwardEmbeddingsImpl(cb, allocator, cfg, embeddings, mask, batch, seq, dimensions);
}

fn forwardEmbeddingsImpl(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: Config, embeddings: CT, mask: []const i64, batch: usize, seq: usize, dimensions: ?usize) !CT {
    if (cb.kind() != .native and cb.kind() != .metal and cb.kind() != .wasm) return error.UnsupportedEmbeddingGemma2Backend;
    if (cb.kind() == .metal and (cb.decoderRuntimeHasActiveFrame() or cb.decoderRuntimeHasSubmittedFrame())) return error.EmbeddingGemma2ExternalFrame;
    if (batch == 0 or seq == 0 or seq > max_tokens) return error.InvalidEmbeddingInputLength;
    const rows = std.math.mul(usize, batch, seq) catch return error.InvalidEmbeddingInputLength;
    if (mask.len != rows) return error.InvalidEmbeddingInputLength;
    if (cb.vtable.embeddingModelPrepare) |prepare| try prepare(cb.ptr, cfg.num_hidden_layers);
    const cached = if (cb.vtable.embeddingModelVisibility) |get| try get(cb.ptr, mask, batch, seq, cfg.sliding_window) else null;
    var vis = if (cached) |view| Visibility{ .ranges = view.ranges, .positions = view.positions, .key_positions = view.positions, .gather = view.gather, .windowed = view.windowed, .global_metadata = view.global_metadata, .window_metadata = view.window_metadata, .allocator = allocator, .borrowed = true } else try visibility(allocator, mask, batch, seq);
    defer vis.deinit();
    if (!vis.borrowed) {
        vis.windowed = try windowedRanges(allocator, vis, cfg.sliding_window);
        if (cb.vtable.embeddingHeadPrepare != null) {
            const ids = try allocator.alloc(i32, vis.gather.len);
            defer allocator.free(ids);
            for (vis.gather, ids) |id, *dest| dest.* = @intCast(id);
            vis.device_gather = (try cb.fromInt32Shape(ids, &.{@intCast(ids.len)})) orelse return error.EmbeddingGemma2OperationUnavailable;
        }
        if (cb.kind() == .metal) {
            vis.global_metadata = try visibilityMetadata(cb, allocator, vis, vis.ranges);
            vis.window_metadata = visibilityMetadata(cb, allocator, vis, vis.windowed.?) catch |err| {
                if (vis.global_metadata) |tensor| cb.free(tensor);
                return err;
            };
        }
    }
    defer if (vis.device_gather) |tensor| cb.free(tensor);
    defer if (vis.global_metadata) |tensor| cb.free(tensor);
    defer if (vis.window_metadata) |tensor| cb.free(tensor);
    // Native BF16 weights are widened on acquisition. Keep this shared
    // projection alive through all layers instead of widening its entire
    // 24-layer matrix again for each layer's row slice.
    const pw = try cb.getWeight("language_model.ple.per_layer_model_projection.weight");
    defer cb.free(pw);
    // Small inputs can keep the complete encoder in one owned frame. Bound
    // this by total rows (including padding and batch), so retained activations
    // do not scale into the long-context regime. Transient PLE slots stay alive
    // until the owner completes or cancels every queued consumer.
    var frame_batch: LayerFrameBatch = .{};
    defer frame_batch.deinit(cb);
    var shared_frame = false;
    const group_layers = metalLayerGroup(cfg, rows);
    // Rolling two-slot schedule: retire only the completed predecessor.
    // Encoded MPS views belong to their frame, keeping two groups within
    // the admitted 128 KiB/row layer envelope without draining the successor.
    const pipeline = cb.kind() == .metal and rows > metalFrameRows() and
        !@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_PIPELINED_ENCODER") and
        cb.decoderRuntimeSetWhisperPipelinedFrames(true);
    var pending_frame = false;
    var pending_batch: LayerFrameBatch = .{};
    var pending_ple: ?CT = null;
    defer {
        if (shared_frame) cb.decoderRuntimeCancelFrame() catch {};
        if (pending_frame) cb.decoderRuntimeWaitSubmittedFrame() catch {};
        if (pipeline) _ = cb.decoderRuntimeSetWhisperPipelinedFrames(false);
        if (pending_ple) |tensor| cb.free(tensor);
        pending_batch.deinit(cb);
        @import("../ops/metal_compute.zig").MetalCompute.releaseEmbeddingGemma2ActivationViews(cb) catch {};
    }
    if (cb.kind() == .metal and cfg.num_hidden_layers <= frame_batch.slices.len) {
        shared_frame = try cb.decoderRuntimeBeginFrame();
        if (!shared_frame) return error.EmbeddingGemma2OperationUnavailable;
        if (@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_PROFILE_ENCODER")) try cb.decoderRuntimeSetActiveFrameRegime(.prefill);
    }
    // Registered after slice cleanup so cancellation happens before release.
    defer if (shared_frame) cb.decoderRuntimeCancelFrame() catch {};
    const cpu_ple_group = if (cb.kind() == .native and rows >= 32)
        @min(cfg.num_hidden_layers, @max(@as(usize, 1), (16 * 1024 * 1024) / (rows * cfg.per_layer_input_dim * @sizeOf(f32))))
    else
        1;
    const whole_packed = cb.kind() == .metal and packPerLayerInputs(cfg, rows);
    var packed_first: usize = 0;
    var packed_ple: ?CT = if (whole_packed and cfg.num_hidden_layers <= frame_batch.slices.len)
        try packedPerLayerInputs(cb, cfg, embeddings, pw, rows, shared_frame)
    else
        null;
    defer if (packed_ple) |packed_input| cb.free(packed_input);
    var hidden = embeddings;
    var owns_hidden = false;
    defer if (owns_hidden) cb.free(hidden);
    for (0..cfg.num_hidden_layers) |layer| {
        try cb.checkExecutionControl();
        if (cb.kind() == .native and cpu_ple_group > 1 and packed_ple == null) {
            var group_cfg = cfg;
            group_cfg.num_hidden_layers = @min(cpu_ple_group, cfg.num_hidden_layers - layer);
            const slice = try cb.sliceRows2D(allocator, pw, layer * cfg.per_layer_input_dim, group_cfg.num_hidden_layers * cfg.per_layer_input_dim, cfg.hidden_size);
            defer cb.free(slice);
            packed_ple = try packedPerLayerInputs(cb, group_cfg, embeddings, slice, rows, false);
            packed_first = layer;
        }
        if (shared_frame and !whole_packed and packed_ple == null) {
            var group_cfg = cfg;
            group_cfg.num_hidden_layers = @min(group_layers, cfg.num_hidden_layers - layer);
            // Keep the projected group and its source slice alive through the
            // owned frame. No all-layer activation allocation at long context.
            const slice = try cb.sliceRows2D(allocator, pw, layer * cfg.per_layer_input_dim, group_cfg.num_hidden_layers * cfg.per_layer_input_dim, cfg.hidden_size);
            frame_batch.slices[frame_batch.count] = slice;
            frame_batch.count += 1;
            packed_ple = try packedPerLayerInputs(cb, group_cfg, embeddings, slice, rows, true);
            packed_first = layer;
        }
        const next = try encoderLayer(cb, allocator, cfg, hidden, embeddings, pw, vis, batch, seq, layer, if (shared_frame) &frame_batch else null, packed_ple, packed_first);
        if (owns_hidden) cb.free(hidden);
        hidden = next;
        owns_hidden = true;
        if (cb.kind() == .native and packed_ple != null and (layer + 1 == cfg.num_hidden_layers or (layer + 1) % cpu_ple_group == 0)) {
            cb.free(packed_ple.?);
            packed_ple = null;
        }
        if (shared_frame and (layer + 1) % group_layers == 0 and layer + 1 < cfg.num_hidden_layers) {
            if (pipeline) {
                // Detach before waiting: the successor owns its encoded views
                // even while the predecessor's wrappers are being retired.
                try @import("../ops/metal_compute.zig").MetalCompute.detachEmbeddingGemma2ActivationViews(cb);
                if (pending_frame) {
                    try cb.decoderRuntimeWaitSubmittedFrame();
                    pending_frame = false;
                    if (pending_ple) |tensor| cb.free(tensor);
                    pending_ple = null;
                    pending_batch.deinit(cb);
                    pending_batch.count = 0;
                }
                try cb.decoderRuntimeSubmitFrame();
                shared_frame = false;
                pending_frame = true;
                pending_batch = frame_batch;
                frame_batch.count = 0;
                if (!whole_packed) {
                    pending_ple = packed_ple;
                    packed_ple = null;
                }
            } else {
                try cb.decoderRuntimeSubmitAndWaitFrame();
                shared_frame = false;
                if (!whole_packed) {
                    if (packed_ple) |tensor| cb.free(tensor);
                    packed_ple = null;
                }
                try @import("../ops/metal_compute.zig").MetalCompute.releaseEmbeddingGemma2ActivationViews(cb);
                frame_batch.deinit(cb);
                frame_batch.count = 0;
            }
            try cb.checkExecutionControl();
            shared_frame = try cb.decoderRuntimeBeginFrame();
            if (!shared_frame) return error.EmbeddingGemma2OperationUnavailable;
        }
    }
    const nw = try globalWeight(cb, .final_norm);
    defer cb.free(nw);
    const normalized = try cb.rmsNorm(hidden, nw, cfg.hidden_size, cfg.rms_norm_eps);
    defer cb.free(normalized);
    const projection = try cb.getWeight("language_model.embedding_projection.weight");
    defer cb.free(projection);
    const result = if (dimensions) |dim| try poolProjectNormalize(cb, allocator, cfg, normalized, projection, mask, batch, seq, dim) else try cb.linearNoBias(normalized, projection, rows, cfg.hidden_size, cfg.embedding_dim);
    errdefer cb.free(result);
    if (pending_frame) {
        try cb.decoderRuntimeWaitSubmittedFrame();
        pending_frame = false;
        if (pending_ple) |tensor| cb.free(tensor);
        pending_ple = null;
        pending_batch.deinit(cb);
        pending_batch.count = 0;
    }
    if (shared_frame) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        shared_frame = false;
    }
    try @import("../ops/metal_compute.zig").MetalCompute.releaseEmbeddingGemma2ActivationViews(cb);
    try cb.checkExecutionControl();
    return result;
}

/// Mean commutes only with the final linear projection, never with token RMSNorm.
fn poolProjectNormalize(cb: *const ComputeBackend, a: std.mem.Allocator, cfg: Config, normalized: CT, projection: CT, mask: []const i64, batch: usize, seq: usize, dimensions: usize) !CT {
    if (cb.kind() == .metal) {
        const mean = cb.vtable.maskedMeanRows orelse return error.EmbeddingGemma2OperationUnavailable;
        const normalize = cb.vtable.maskedMeanNormalizedRows orelse return error.EmbeddingGemma2OperationUnavailable;
        const pooled = (try cb.allocUninitF32Shape(&.{ @intCast(batch), @intCast(cfg.hidden_size) })) orelse return error.EmbeddingGemma2OperationUnavailable;
        defer cb.free(pooled);
        for (0..batch) |item| {
            try cb.checkExecutionControl();
            const view = try cb.sliceRows2D(a, normalized, item * seq, seq, cfg.hidden_size);
            defer cb.free(view);
            const row = (try mean(cb.ptr, view, mask[item * seq ..][0..seq], cfg.hidden_size, cfg.hidden_size)) orelse return error.EmbeddingGemma2OperationUnavailable;
            defer cb.free(row);
            if (!try cb.copyRows2D(a, pooled, item, row, 0, 1, cfg.hidden_size)) return error.EmbeddingGemma2OperationUnavailable;
        }
        const projected = try cb.linearNoBias(pooled, projection, batch, cfg.hidden_size, cfg.embedding_dim);
        defer cb.free(projected);
        const output = (try cb.allocUninitF32Shape(&.{ @intCast(batch), @intCast(dimensions) })) orelse return error.EmbeddingGemma2OperationUnavailable;
        errdefer cb.free(output);
        for (0..batch) |item| {
            const view = try cb.sliceRows2D(a, projected, item, 1, cfg.embedding_dim);
            defer cb.free(view);
            const row = (try normalize(cb.ptr, view, &.{1}, cfg.embedding_dim, dimensions)) orelse return error.EmbeddingGemma2OperationUnavailable;
            defer cb.free(row);
            if (!try cb.copyRows2D(a, output, item, row, 0, 1, dimensions)) return error.EmbeddingGemma2OperationUnavailable;
        }
        return output;
    }
    const values = try cb.toFloat32(normalized, a);
    defer a.free(values);
    const pooled = try a.alloc(f32, batch * cfg.hidden_size);
    defer a.free(pooled);
    @memset(pooled, 0);
    for (0..batch) |item| {
        var count: usize = 0;
        const row = pooled[item * cfg.hidden_size ..][0..cfg.hidden_size];
        for (mask[item * seq ..][0..seq], 0..) |active, token| {
            if (active == 0) continue;
            count += 1;
            for (row, values[(item * seq + token) * cfg.hidden_size ..][0..cfg.hidden_size]) |*sum, value| sum.* += value;
        }
        if (count == 0) return error.EmptyEmbeddingInput;
        for (row) |*value| value.* /= @as(f32, @floatFromInt(count));
    }
    const input = try cb.fromFloat32Shape(pooled, &.{ @intCast(batch), @intCast(cfg.hidden_size) });
    defer cb.free(input);
    const projected = try cb.linearNoBias(input, projection, batch, cfg.hidden_size, cfg.embedding_dim);
    defer cb.free(projected);
    const data = try cb.toFloat32(projected, a);
    defer a.free(data);
    const compact = try a.alloc(f32, batch * dimensions);
    defer a.free(compact);
    for (0..batch) |item| {
        const vector = try @import("antfly_decisions").scoring.centroid(a, &.{data[item * cfg.embedding_dim ..][0..dimensions]}, dimensions);
        defer a.free(vector);
        @memcpy(compact[item * dimensions ..][0..dimensions], vector);
    }
    return cb.fromFloat32Shape(compact, &.{ @intCast(batch), @intCast(dimensions) });
}

fn expectBackendParity(a: std.mem.Allocator, cpu: *const ComputeBackend, gpu: *const ComputeBackend, x: CT, y: CT, stage: []const u8) !void {
    const xx = try cpu.toFloat32(x, a);
    defer a.free(xx);
    const yy = try gpu.toFloat32(y, a);
    defer a.free(yy);
    try std.testing.expectEqual(xx.len, yy.len);
    var max_abs: f64 = 0;
    var dot: f64 = 0;
    var aa: f64 = 0;
    var bb: f64 = 0;
    for (xx, yy) |u, v| {
        max_abs = @max(max_abs, @abs(u - v));
        dot += @as(f64, u) * v;
        aa += @as(f64, u) * u;
        bb += @as(f64, v) * v;
    }
    const cos = dot / @sqrt(aa * bb);
    std.debug.print("embeddinggemma2 stage={s} cosine={d:.9} max_abs={d:.9}\n", .{ stage, cos, max_abs });
    try std.testing.expect(cos >= 0.9999);
}

test "embeddinggemma2 native Metal layer parity" {
    const env = @import("antfly_platform").env;
    if (!env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_METAL")) return error.SkipZigTest;
    const model = env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("session_factory.zig");
    const cpu_session = try factory.createNativeSession(a, model);
    defer cpu_session.close();
    const gpu_session = try factory.createMetalSession(a, model);
    defer gpu_session.close();
    var cpu = try factory.getComputeBackend(cpu_session, a);
    defer cpu.deinit();
    var gpu = try factory.getComputeBackend(gpu_session, a);
    defer gpu.deinit();
    const cfg = Config{};
    const ids: []const i64 = &.{ 2, 7314, 236786, 1356, 236743, 1006, 2931, 236786, 564, 30591 };
    const mask: []const i64 = &.{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    const vis = try visibility(a, mask, 1, ids.len);
    defer vis.deinit();
    const cw = try cpu.getWeight("language_model.embed_tokens.weight");
    defer cpu.free(cw);
    const gw = try gpu.getWeight("language_model.embed_tokens.weight");
    defer gpu.free(gw);
    const ce = try cpu.embeddingLookup(cw, ids, ids.len, 512);
    defer cpu.free(ce);
    const ge = try gpu.embeddingLookup(gw, ids, ids.len, 512);
    defer gpu.free(ge);
    try expectBackendParity(a, &cpu, &gpu, ce, ge, "embedding");
    const cs = try scaled(&cpu, ce, @sqrt(@as(f32, 512)));
    defer cpu.free(cs);
    const gs = try scaled(&gpu, ge, @sqrt(@as(f32, 512)));
    defer gpu.free(gs);
    const cn = try norm(&cpu, 0, "input_layernorm.weight", cs, 512, 1e-6);
    defer cpu.free(cn);
    const gn = try norm(&gpu, 0, "input_layernorm.weight", gs, 512, 1e-6);
    defer gpu.free(gn);
    try expectBackendParity(a, &cpu, &gpu, cn, gn, "input_norm");
    const cq = try linear(&cpu, 0, "self_attn.q_proj.weight", cn, ids.len, 512, 1024);
    defer cpu.free(cq);
    const gq = try linear(&gpu, 0, "self_attn.q_proj.weight", gn, ids.len, 512, 1024);
    defer gpu.free(gq);
    try expectBackendParity(a, &cpu, &gpu, cq, gq, "q_linear");
    const ca = try attention(&cpu, a, cfg, cn, vis, 1, ids.len, 0);
    defer cpu.free(ca);
    const ga = try attention(&gpu, a, cfg, gn, vis, 1, ids.len, 0);
    defer gpu.free(ga);
    try expectBackendParity(a, &cpu, &gpu, ca, ga, "attention");
    const cpw = try cpu.getWeight("language_model.ple.per_layer_model_projection.weight");
    defer cpu.free(cpw);
    const gpw = try gpu.getWeight("language_model.ple.per_layer_model_projection.weight");
    defer gpu.free(gpw);
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    // A cancelled layer must unwind its frame and transient PLE slot before
    // the same backend executes again. A foreign frame remains caller-owned.
    gpu.execution_control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, encoderLayer(&gpu, a, cfg, gs, gs, gpw, vis, 1, ids.len, 0, null, null, 0));
    gpu.execution_control = null;
    try std.testing.expect(!gpu.decoderRuntimeHasActiveFrame());
    try std.testing.expect(try gpu.decoderRuntimeBeginFrame());
    try std.testing.expectError(error.EmbeddingGemma2ExternalFrame, encoderLayer(&gpu, a, cfg, gs, gs, gpw, vis, 1, ids.len, 0, null, null, 0));
    try std.testing.expect(gpu.decoderRuntimeHasActiveFrame());
    try gpu.decoderRuntimeCancelFrame();
    var ch = cs;
    var gh = gs;
    var owns = false;
    defer if (owns) {
        cpu.free(ch);
        gpu.free(gh);
    };
    for (0..24) |layer| {
        const next_cpu = try encoderLayer(&cpu, a, cfg, ch, cs, cpw, vis, 1, ids.len, layer, null, null, 0);
        const next_gpu = try encoderLayer(&gpu, a, cfg, gh, gs, gpw, vis, 1, ids.len, layer, null, null, 0);
        if (owns) {
            cpu.free(ch);
            gpu.free(gh);
        }
        ch = next_cpu;
        gh = next_gpu;
        owns = true;
        var buf: [32]u8 = undefined;
        try expectBackendParity(a, &cpu, &gpu, ch, gh, try std.fmt.bufPrint(&buf, "layer_{d}", .{layer}));
    }
}

test "embeddinggemma2 visibility preserves padding holes and batch boundaries" {
    const vis = try visibility(std.testing.allocator, &.{ 1, 0, 1, 0, 1, 0 }, 2, 3);
    defer vis.deinit();
    try std.testing.expectEqualSlices(i64, &.{ 0, 2, 4 }, vis.gather);
    try std.testing.expectEqualSlices(i32, &.{ 0, 2, 1 }, vis.key_positions);
    try std.testing.expectEqualSlices(u32, &.{ 0, 2, 0, 0, 0, 0 }, vis.ranges[0..6]);
    try std.testing.expectEqualSlices(u32, &.{ 2, 3, 0, 0, 0, 0 }, vis.ranges[18..24]);
    try std.testing.expectError(error.EmptyEmbeddingInput, visibility(std.testing.allocator, &.{ 0, 0 }, 1, 2));
}

test "embeddinggemma2 official task prefixes and matryoshka dimensions" {
    try std.testing.expectEqualStrings("title: none | text: ", try taskPrefix("RETRIEVAL_DOCUMENT"));
    try std.testing.expectEqualStrings("task: clustering | query: ", try taskPrefix("CLUSTERING"));
    try std.testing.expectError(error.UnsupportedEmbeddingTask, taskPrefix("unknown"));
    try std.testing.expect(validDimension(1));
    try std.testing.expect(validDimension(128));
    try std.testing.expect(validDimension(384));
    try std.testing.expect(validDimension(768));
    try std.testing.expect(!validDimension(0));
    try std.testing.expect(!validDimension(769));
}

test "embeddinggemma2 real official checkpoint matches F32 oracle" {
    const env = @import("antfly_platform").env;
    const model = env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    const oracle_path = env.getenv("ANTFLY_EMBEDDINGGEMMA2_ORACLE") orelse return error.MissingEmbeddingGemma2Oracle;
    const allocator = std.testing.allocator;
    const bytes = try @import("../util/c_file.zig").readFile(allocator, oracle_path);
    defer allocator.free(bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try std.testing.expectEqualStrings(checkpoint_revision, root.get("revision").?.string);
    const factory = @import("session_factory.zig");
    const metal = env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_METAL");
    const session = if (metal) try factory.createMetalSession(allocator, model) else try factory.createNativeSession(allocator, model);
    defer session.close();
    if (!metal) factory.attachIo(session, std.testing.io);
    const Tensor = @import("../backends/tensor.zig").Tensor;
    for (root.get("token_ids").?.array.items, root.get("embeddings").?.array.items, 0..) |raw_ids, reference, index| {
        const ids = try allocator.alloc(i64, raw_ids.array.items.len);
        defer allocator.free(ids);
        for (ids, raw_ids.array.items) |*dest, source| dest.* = source.integer;
        const mask = try allocator.alloc(i64, ids.len);
        defer allocator.free(mask);
        @memset(mask, 1);
        var input = try Tensor.initInt64(allocator, "input_ids", &.{ 1, @intCast(ids.len) }, ids);
        defer input.deinit();
        var attention_mask = try Tensor.initInt64(allocator, "attention_mask", &.{ 1, @intCast(ids.len) }, mask);
        defer attention_mask.deinit();
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = @import("../pipelines/embedding_gemma2.zig").workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
        defer std.debug.assert(bounded.live == 0);
        const output_allocator = if (env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_BOUNDED")) bounded.allocator() else allocator;
        const outputs = session.run(&.{ input, attention_mask }, output_allocator) catch |err| {
            if (env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_BOUNDED")) std.debug.print("embeddinggemma2 bounded tokens={d} peak_bytes={d} failure={s}\n", .{ ids.len, bounded.peak, @errorName(err) });
            return err;
        };
        if (env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_BOUNDED")) std.debug.print("embeddinggemma2 bounded tokens={d} peak_bytes={d}\n", .{ ids.len, bounded.peak });
        defer {
            for (outputs) |*output| output.deinit();
            output_allocator.free(outputs);
            if (env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_BOUNDED")) {
                std.testing.expectEqual(@as(usize, 0), bounded.live) catch |err| std.debug.panic("embeddinggemma2 workspace remains live after output release: {s}, {d} bytes", .{ @errorName(err), bounded.live });
            }
        }
        const data = outputs[0].asFloat32();
        var pooled: [768]f32 = @splat(0);
        for (0..ids.len) |row| for (&pooled, data[row * 768 ..][0..768]) |*dest, x| {
            dest.* += x / @as(f32, @floatFromInt(ids.len));
        };
        var sq: f64 = 0;
        for (pooled) |x| sq += @as(f64, x) * x;
        for (&pooled) |*x| x.* = @floatCast(@as(f64, x.*) / @sqrt(sq));
        var dot: f64 = 0;
        var expected_sq: f64 = 0;
        var actual_sq: f64 = 0;
        var max_abs: f64 = 0;
        for (pooled, reference.array.items) |actual, expected| {
            const e: f64 = expected.float;
            dot += @as(f64, actual) * e;
            expected_sq += e * e;
            actual_sq += @as(f64, actual) * actual;
            max_abs = @max(max_abs, @abs(actual - e));
        }
        const cos = dot / @sqrt(actual_sq * expected_sq);
        std.debug.print("embeddinggemma2 oracle case={d} metal={} cosine={d:.9} max_abs={d:.9}\n", .{ index, metal, cos, max_abs });
        try std.testing.expect(cos >= @as(f64, if (metal) 0.9999 else 0.99999));
        try std.testing.expect(max_abs <= @as(f64, if (metal) 1e-3 else 1e-4));
    }
}

test "embeddinggemma2 real padded batch is invariant and cancellation permits retry" {
    const env = @import("antfly_platform").env;
    const model = env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    const oracle_path = env.getenv("ANTFLY_EMBEDDINGGEMMA2_ORACLE") orelse return error.MissingEmbeddingGemma2Oracle;
    const a = std.testing.allocator;
    const bytes = try @import("../util/c_file.zig").readFile(a, oracle_path);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const references = parsed.value.object.get("embeddings").?.array.items;
    const raw_ids = parsed.value.object.get("token_ids").?.array.items;
    const seq = @max(raw_ids[0].array.items.len, raw_ids[1].array.items.len) + 3;
    const ids = try a.alloc(i64, 2 * seq);
    defer a.free(ids);
    const mask = try a.alloc(i64, 2 * seq);
    defer a.free(mask);
    @memset(ids, 0);
    @memset(mask, 0);
    for (0..2) |b| for (raw_ids[b].array.items, 0..) |id, row| {
        ids[b * seq + row] = id.integer;
        mask[b * seq + row] = 1;
    };
    const factory = @import("session_factory.zig");
    const metal = env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_METAL");
    const session = if (metal) try factory.createMetalSession(a, model) else try factory.createNativeSession(a, model);
    defer session.close();
    if (!metal) factory.attachIo(session, std.testing.io);
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var input = try Tensor.initInt64(a, "input_ids", &.{ 2, @intCast(seq) }, ids);
    defer input.deinit();
    var attention_mask = try Tensor.initInt64(a, "attention_mask", &.{ 2, @intCast(seq) }, mask);
    defer attention_mask.deinit();
    const Cancel = struct {
        checks: usize = 0,
        cancel_at: usize,
        pending_runtime: ?*@import("../backends/metal_runtime.zig").RawMetalDecodeRuntime = null,
        saw_pending: bool = false,
        first_pending_frame: ?u64 = null,
        pending_frames: u64 = 0,
        cancel_pending_at: u64 = 1,
        armed: bool = false,
        arms: usize = 0,
        disarms: usize = 0,

        fn check(ctx: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.checks += 1;
            if (comptime @import("build_options").enable_metal) {
                if (self.pending_runtime) |runtime| if (@import("../backends/metal_runtime.zig").hasSubmittedFrame(runtime)) {
                    self.saw_pending = true;
                    const count = @import("../backends/metal_runtime.zig").frameCommandBufferCount(runtime);
                    if (self.first_pending_frame == null) self.first_pending_frame = count;
                    self.pending_frames = count - self.first_pending_frame.? + 1;
                    if (self.pending_frames >= self.cancel_pending_at) return error.Cancelled;
                };
            }
            if (self.checks >= self.cancel_at) return error.Cancelled;
        }

        // This synchronous checkpoint probe exercises cooperative cleanup.
        // Model the required process guard without running a watchdog that
        // would race the deterministic check counter. Live serving separately
        // exercises the actual supervised worker boundary.
        fn arm(raw: *anyopaque, _: @import("../execution_control.zig").MonitorControl) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.armed);
            self.armed = true;
            self.arms += 1;
            return self.arms;
        }

        fn disarm(raw: *anyopaque, token: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.armed and token == self.arms);
            self.armed = false;
            self.disarms += 1;
        }
    };
    // Cancel at entry and after several encoder layers have queued consumers.
    // Every retry reacquires the same model provider and must find its frame,
    // transient projection slots, source pins and request admission unwound.
    for ([_]usize{ 1, 9, 31 }) |cancel_at| {
        var cancellation = Cancel{ .cancel_at = cancel_at };
        try std.testing.expectError(error.Cancelled, session.runWithControl(&.{ input, attention_mask }, a, .{
            .ptr = &cancellation,
            .check_fn = Cancel.check,
            .hard_cancellation = if (metal) .{ .ptr = &cancellation, .arm_fn = Cancel.arm, .disarm_fn = Cancel.disarm } else null,
        }));
        try std.testing.expectEqual(cancel_at, cancellation.checks);
        try std.testing.expect(!cancellation.armed);
        try std.testing.expectEqual(cancellation.arms, cancellation.disarms);
        if (metal and cancel_at > 1) try std.testing.expectEqual(@as(usize, 1), cancellation.arms);
        std.debug.print("embeddinggemma2 cancellation metal={} checks={d}\n", .{ metal, cancellation.checks });
    }
    const outputs = try session.run(&.{ input, attention_mask }, a);
    defer {
        for (outputs) |*output| output.deinit();
        a.free(outputs);
    }
    const hidden = outputs[0].asFloat32();
    for (0..2) |b| {
        const count = raw_ids[b].array.items.len;
        var vector: [768]f32 = @splat(0);
        for (0..count) |row| for (&vector, hidden[(b * seq + row) * 768 ..][0..768]) |*dest, value| {
            dest.* += value / @as(f32, @floatFromInt(count));
        };
        var squared_norm: f64 = 0;
        for (vector) |value| squared_norm += @as(f64, value) * value;
        var max_abs: f64 = 0;
        for (vector, references[b].array.items) |value, expected| max_abs = @max(max_abs, @abs(@as(f64, value) / @sqrt(squared_norm) - expected.float));
        std.debug.print("embeddinggemma2 padded batch={d} metal={} max_abs={d:.9}\n", .{ b, metal, max_abs });
        try std.testing.expect(max_abs <= 1e-4);
    }
    // Cross the packed-projection threshold by total padded rows, including
    // both batch members. Active tokens must produce the same oracle vectors
    // with either execution schedule.
    for ([_]usize{ 256, 257 }) |padded_seq| {
        const padded_ids = try a.alloc(i64, 2 * padded_seq);
        defer a.free(padded_ids);
        const padded_mask = try a.alloc(i64, padded_ids.len);
        defer a.free(padded_mask);
        @memset(padded_ids, 0);
        @memset(padded_mask, 0);
        for (0..2) |b| for (raw_ids[b].array.items, 0..) |id, row| {
            padded_ids[b * padded_seq + row] = id.integer;
            padded_mask[b * padded_seq + row] = 1;
        };
        var padded_input = try Tensor.initInt64(a, "input_ids", &.{ 2, @intCast(padded_seq) }, padded_ids);
        defer padded_input.deinit();
        var padded_attention = try Tensor.initInt64(a, "attention_mask", &.{ 2, @intCast(padded_seq) }, padded_mask);
        defer padded_attention.deinit();
        if (comptime @import("build_options").enable_metal) {
            if (metal and padded_seq == 257 and !env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_PIPELINED_ENCODER")) {
                var backend = try factory.getComputeBackend(session, a);
                const compute: *const @import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(backend.ptr));
                const runtime = compute.provider_impl.raw_decode_runtime;
                backend.deinit();
                for ([_]u64{ 1, 3, 7 }) |pending_frames| {
                    var cancellation = Cancel{ .cancel_at = std.math.maxInt(usize), .pending_runtime = runtime, .cancel_pending_at = pending_frames };
                    try std.testing.expectError(error.Cancelled, session.runWithControl(&.{ padded_input, padded_attention }, a, .{
                        .ptr = &cancellation,
                        .check_fn = Cancel.check,
                        .hard_cancellation = .{ .ptr = &cancellation, .arm_fn = Cancel.arm, .disarm_fn = Cancel.disarm },
                    }));
                    try std.testing.expect(cancellation.saw_pending);
                    try std.testing.expectEqual(pending_frames, cancellation.pending_frames);
                    try std.testing.expect(!cancellation.armed);
                    if (comptime @import("build_options").enable_metal) try std.testing.expect(!@import("../backends/metal_runtime.zig").hasSubmittedFrame(runtime));
                    std.debug.print("embeddinggemma2 submitted-frame cancellation rows=514 pending_frames={d} drained=true\n", .{pending_frames});
                }
            }
        }
        for ([_]usize{ 9, 31 }) |cancel_at| {
            var cancellation = Cancel{ .cancel_at = cancel_at };
            try std.testing.expectError(error.Cancelled, session.runWithControl(&.{ padded_input, padded_attention }, a, .{
                .ptr = &cancellation,
                .check_fn = Cancel.check,
                .hard_cancellation = if (metal) .{ .ptr = &cancellation, .arm_fn = Cancel.arm, .disarm_fn = Cancel.disarm } else null,
            }));
            try std.testing.expectEqual(cancel_at, cancellation.checks);
            try std.testing.expect(!cancellation.armed);
            try std.testing.expectEqual(cancellation.arms, cancellation.disarms);
            std.debug.print("embeddinggemma2 padded cancellation rows={d} metal={} checks={d}\n", .{ 2 * padded_seq, metal, cancellation.checks });
        }
        const padded_outputs = try session.run(&.{ padded_input, padded_attention }, a);
        defer {
            for (padded_outputs) |*output| output.deinit();
            a.free(padded_outputs);
        }
        const padded_hidden = padded_outputs[0].asFloat32();
        for (0..2) |b| {
            const count = raw_ids[b].array.items.len;
            var vector: [768]f32 = @splat(0);
            for (0..count) |row| for (&vector, padded_hidden[(b * padded_seq + row) * 768 ..][0..768]) |*dest, value| {
                dest.* += value / @as(f32, @floatFromInt(count));
            };
            var sq: f64 = 0;
            for (vector) |value| sq += @as(f64, value) * value;
            var max_abs: f64 = 0;
            for (vector, references[b].array.items) |value, expected| max_abs = @max(max_abs, @abs(@as(f64, value) / @sqrt(sq) - expected.float));
            std.debug.print("embeddinggemma2 packed boundary rows={d} batch={d} metal={} max_abs={d:.9}\n", .{ 2 * padded_seq, b, metal, max_abs });
            try std.testing.expect(max_abs <= 1e-4);
        }
    }
}

test "embeddinggemma2 rejects incompatible checkpoint semantics" {
    const a = std.testing.allocator;
    const fixture = @embedFile("embedding_gemma2_config.json");
    const cfg = try parseConfig(a, fixture);
    try std.testing.expect(cfg.resident != null);
    try std.testing.expect(cfg.resident.?.isOfficialTextTopology());
    const parsed = try std.json.parseFromSlice(std.json.Value, a, fixture, .{});
    defer parsed.deinit();
    const tc = parsed.value.object.getPtr("text_config").?;
    try tc.object.put(a, "attention_bias", .{ .bool = true });
    const bytes = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(bytes);
    try std.testing.expectError(error.UnsupportedEmbeddingGemma2Geometry, parseConfig(a, bytes));
}

fn checkPooledProjection(cb: ComputeBackend, _: ComputeBackend) !void {
    if (comptime @import("build_options").enable_metal) {
        if (cb.kind() == .metal) {
            const compute: *@import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(cb.ptr));
            compute.require_f32_linears = true;
        }
    }
    const a = std.testing.allocator;
    const cfg = Config{ .hidden_size = 4 };
    const nan = std.math.nan(f32);
    const values = [_]f32{ 1, 2, 3, 4, nan, nan, nan, nan, -2, 1, 2, 3, 4, -1, 3, 2, nan, nan, nan, nan, nan, nan, nan, nan };
    const mask = [_]i64{ 1, 0, 1, 1, 0, 0 };
    var weights: [768 * 4]f32 = undefined;
    for (&weights, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.127);
    const input = try cb.fromFloat32Shape(&values, &.{ 6, 4 });
    defer cb.free(input);
    const projection = try cb.fromFloat32Shape(&weights, &.{ 768, 4 });
    defer cb.free(projection);
    if (cb.kind() == .metal) {
        defer cb.decoderRuntimeWaitSubmittedFrame() catch {};
        defer if (cb.decoderRuntimeHasActiveFrame()) cb.decoderRuntimeCancelFrame() catch {};
        try std.testing.expect(try cb.decoderRuntimeBeginFrame());
        try std.testing.expectError(error.EmbeddingGemma2ExternalFrame, forwardCT(&cb, a, cfg, &.{0}, &.{1}, 1, 1));
        try cb.decoderRuntimeSubmitFrame();
        try std.testing.expect(cb.decoderRuntimeHasSubmittedFrame());
        try std.testing.expectError(error.EmbeddingGemma2ExternalFrame, forwardCT(&cb, a, cfg, &.{0}, &.{1}, 1, 1));
        try std.testing.expectError(error.EmbeddingGemma2ExternalFrame, forwardEmbeddingsCT(&cb, a, cfg, input, &mask, 2, 3));
        try std.testing.expect(cb.decoderRuntimeHasSubmittedFrame());
        try cb.decoderRuntimeWaitSubmittedFrame();
    }
    for ([_]usize{ 128, 256, 512, 768 }) |dim| {
        var frame = false;
        if (cb.kind() == .metal) frame = try cb.decoderRuntimeBeginFrame();
        defer if (frame) cb.decoderRuntimeCancelFrame() catch {};
        const before = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
        const result = try poolProjectNormalize(&cb, a, cfg, input, projection, &mask, 2, 3, dim);
        defer cb.free(result);
        if (frame) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            frame = false;
        }
        try before.expectUnchanged(cb);
        const actual = try cb.toFloat32(result, a);
        defer a.free(actual);
        for (0..2) |item| {
            var reference: [768]f32 = @splat(0);
            var count: usize = 0;
            for (mask[item * 3 ..][0..3], 0..) |active, row| {
                if (active == 0) continue;
                count += 1;
                for (reference[0..dim], 0..) |*sum, col| for (values[(item * 3 + row) * 4 ..][0..4], weights[col * 4 ..][0..4]) |x, w| {
                    sum.* += x * w;
                };
            }
            for (reference[0..dim]) |*sum| sum.* /= @as(f32, @floatFromInt(count));
            const expected = try @import("antfly_decisions").scoring.centroid(a, &.{reference[0..dim]}, dim);
            defer a.free(expected);
            for (expected, actual[item * dim ..][0..dim]) |x, y| try std.testing.expectApproxEqAbs(x, y, 2e-6);
        }
    }
}
test "embeddinggemma2 native pooled projection preserves masked token reference all dimensions" {
    try @import("../ops/media_residency_test.zig").onNative(@import("../ops/media_residency_test.zig").noWeights, checkPooledProjection);
}
test "embeddinggemma2 Metal pooled projection stays resident across dimensions and independent items" {
    try @import("../ops/media_residency_test.zig").onMetal(@import("../ops/media_residency_test.zig").noWeights, checkPooledProjection);
}

fn checkPreparedHeadFusion(cb: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const op = cb.vtable.embeddingHeadPrepare orelse return error.SkipZigTest;
    var data: [6 * 64]f32 = undefined;
    var weights: [8]f32 = undefined;
    for (&data, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.17);
    for (&weights, 0..) |*v, i| v.* = 0.7 + @as(f32, @floatFromInt(i)) * 0.1;
    @memset(data[64..128], std.math.nan(f32));
    @memset(data[256..320], std.math.nan(f32));
    const input = try cb.fromFloat32Shape(&data, &.{ 6, 64 });
    defer cb.free(input);
    const weight_tensor = try cb.fromFloat32Shape(&weights, &.{8});
    defer cb.free(weight_tensor);
    const ids = [_]i32{ 0, 2, 3, 5 };
    const gather = (try cb.fromInt32Shape(&ids, &.{4})).?;
    defer cb.free(gather);
    for ([_]u32{ 2, 4 }) |heads| for ([_]bool{ false, true }) |rotate| {
        const p = ops.EmbeddingHeadParams{ .source_rows = 6, .rows = 4, .source_heads = 2, .heads = heads, .dim = 8, .sequence = 3, .stride = 64, .start = 7, .rotate = @intFromBool(rotate), .gather = 1, .weighted = @intFromBool(rotate), .eps = 1e-6, .theta = 10000, .scale = 0.75 };
        var frame = if (cb.kind() == .metal) try cb.decoderRuntimeBeginFrame() else false;
        defer if (frame) cb.decoderRuntimeCancelFrame() catch {};
        const transfers = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
        const result = try op(cb.ptr, input, if (rotate) weight_tensor else null, gather, p);
        defer cb.free(result);
        if (frame) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            frame = false;
        }
        try transfers.expectUnchanged(cb);
        const actual = try cb.toFloat32(result, a);
        defer a.free(actual);
        for (ids, 0..) |src_row, row| for (0..heads) |h| {
            const source = data[@as(usize, @intCast(src_row)) * 64 + 7 + h / (heads / 2) * 8 ..][0..8];
            var squares: f64 = 0;
            for (source) |value| squares += @as(f64, value) * value;
            const inv: f32 = @floatCast(1 / @sqrt(squares / 8 + 1e-6));
            for (0..8) |d| {
                var value = source[d] * inv;
                if (rotate) {
                    const left = d % 4;
                    const angle = @as(f32, @floatFromInt(@mod(src_row, 3))) / std.math.pow(f32, p.theta, @as(f32, @floatFromInt(2 * left)) / 8);
                    const x = source[left] * inv * weights[left];
                    const y = source[left + 4] * inv * weights[left + 4];
                    value = if (d < 4) x * @cos(angle) - y * @sin(angle) else x * @sin(angle) + y * @cos(angle);
                }
                try std.testing.expectApproxEqAbs(value * p.scale, actual[(row * heads + h) * 8 + d], 2e-6);
            }
        };
    };
}
test "embeddinggemma2 native fused heads preserve strided slices masks rotary and compact expansion" {
    try @import("../ops/media_residency_test.zig").onNative(@import("../ops/media_residency_test.zig").noWeights, checkPreparedHeadFusion);
}
test "embeddinggemma2 Metal fused heads preserve strided slices masks rotary without downloads" {
    try @import("../ops/media_residency_test.zig").onMetal(@import("../ops/media_residency_test.zig").noWeights, checkPreparedHeadFusion);
}

fn checkGroupedRangeTiles(cb: ComputeBackend, reference: ComputeBackend) !void {
    if (cb.kind() != .metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const count = 140;
    for ([_]usize{ 256, 512 }) |dim| {
        const heads = 4;
        const kv = 1;
        const q_data = try a.alloc(f32, count * heads * dim);
        defer a.free(q_data);
        const k_data = try a.alloc(f32, count * kv * dim);
        defer a.free(k_data);
        for (q_data, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.17) * 0.1;
        for (k_data, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i)) * 0.13) * 0.1;
        var ranges: [count * 6]u32 = @splat(0);
        var positions: [count]i32 = undefined;
        for (0..count) |i| {
            ranges[i * 6] = @intCast(i / 35 * 35);
            ranges[i * 6 + 1] = @intCast((i / 35 + 1) * 35);
            positions[i] = @intCast(i % 35);
        }
        @memset(ranges[0..6], 0);
        ranges[6..12].* = .{ 0, 3, 8, 19, 22, 35 };
        const metadata = (try visibilityMetadata(&cb, a, .{ .ranges = &ranges, .positions = &positions, .key_positions = &positions, .gather = &.{}, .allocator = a }, &ranges)).?;
        defer cb.free(metadata);
        const request = ops.SegmentAttentionGrouped{ .visibility = .{ .ranges = &ranges, .query_positions = &positions, .key_positions = &positions, .queries = count, .keys = count, .num_heads = heads, .head_dim = dim, .window = 17 }, .metadata = metadata, .num_kv_heads = kv, .score_scale = 1 };
        const q = try cb.fromFloat32Shape(q_data, &.{ count * heads, @intCast(dim) });
        defer cb.free(q);
        const k = try cb.fromFloat32Shape(k_data, &.{ count * kv, @intCast(dim) });
        defer cb.free(k);
        const rq = try reference.fromFloat32Shape(q_data, &.{ count * heads, @intCast(dim) });
        defer reference.free(rq);
        const rk = try reference.fromFloat32Shape(k_data, &.{ count * kv, @intCast(dim) });
        defer reference.free(rk);
        const expected = (try reference.vtable.segmentAttentionGrouped.?(reference.ptr, rq, rk, rk, &request)).?;
        defer reference.free(expected);
        var frame = try cb.decoderRuntimeBeginFrame();
        defer if (frame) cb.decoderRuntimeCancelFrame() catch {};
        const transfers = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
        const actual = (try cb.vtable.segmentAttentionGrouped.?(cb.ptr, q, k, k, &request)).?;
        defer cb.free(actual);
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame = false;
        try transfers.expectUnchanged(cb);
        const x = try reference.toFloat32(expected, a);
        defer a.free(x);
        const y = try cb.toFloat32(actual, a);
        defer a.free(y);
        for (x, y) |xx, yy| try std.testing.expectApproxEqAbs(xx, yy, 1e-6);
    }
}
test "embeddinggemma2 Metal disjoint grouped GEMM tiles preserve holes empty queries and window tails" {
    try @import("../ops/media_residency_test.zig").onMetal(@import("../ops/media_residency_test.zig").noWeights, checkGroupedRangeTiles);
}
