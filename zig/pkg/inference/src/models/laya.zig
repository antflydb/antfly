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

//! Checkpoint-owned Laya decision configuration; no borrowed JSON storage.
const std = @import("std");
pub const QuestionType = enum(u8) { choice, score, noul };

/// Released checkpoints encode one sequence per question and admit 20 options.
pub const max_options = 20;
/// Candidate-branch packing gives every option its own branch (see LAYA.md).
pub const max_packed_options = 255;

/// Tree-packed execution (zig/pkg/inference/models/laya/LAYA.md). The state is a
/// shared trunk that attends only to itself; each question, and in candidate
/// mode each option, is a branch that attends to its ancestors and itself.
/// Positions restart after the parent at every branch. Released checkpoints
/// use `.none`; a packed mode requires weights fine-tuned for that layout.
pub const PackingMode = enum(u8) { none, question, candidate };
pub const Packing = struct {
    mode: PackingMode = .none,
    /// Physical tokens in one packed row. `Config.max_len` still bounds the
    /// logical length (trunk plus the longest root-to-leaf branch path).
    max_packed_len: usize = 0,

    pub fn enabled(self: Packing) bool {
        return self.mode != .none;
    }
};

pub const Config = struct {
    mask_token: [128]u8 = "[MASK]".* ++ ([_]u8{0} ** 122),
    mask_token_len: usize = 6,
    head_layers: usize = 2,
    max_len: usize = 512,
    head_max_len: usize = 192,
    n_act: usize = 2,
    temperature: [3]f32 = .{ 1, 1, 1 },
    buckets: [3][4]?f32 = .{ .{ null, null, null, null }, .{ null, null, null, null }, .{ null, null, null, null } },
    packing: Packing = .{},

    pub fn scale(self: Config, kind: QuestionType, count: usize) f32 {
        const bucket: usize = if (count <= 2) 0 else if (count <= 5) 1 else if (count <= 10) 2 else 3;
        return @max(0.001, self.buckets[@intFromEnum(kind)][bucket] orelse self.temperature[@intFromEnum(kind)]);
    }

    pub fn maxOptions(self: Config) usize {
        return if (self.packing.mode == .candidate) max_packed_options else max_options;
    }

    pub fn parse(value: std.json.Value) !Config {
        if (value != .object) return error.InvalidLayaConfig;
        const obj = value.object;
        var out = Config{};
        if (obj.get("mask_token")) |v| {
            if (v != .string or v.string.len == 0 or v.string.len > out.mask_token.len) return error.InvalidLayaConfig;
            @memcpy(out.mask_token[0..v.string.len], v.string);
            out.mask_token_len = v.string.len;
        }
        inline for (.{ "head_layers", "max_len", "head_max_len" }) |name| {
            if (obj.get(name)) |v| {
                if (v != .integer or v.integer < 0) return error.InvalidLayaConfig;
                @field(out, name) = std.math.cast(usize, v.integer) orelse return error.InvalidLayaConfig;
            }
        }
        if (out.head_layers > 16 or out.max_len < 16 or out.max_len > 8192 or out.head_max_len < 16 or out.head_max_len >= out.max_len) return error.InvalidLayaConfig;
        if (obj.get("act_costs")) |v| {
            if (v != .object or v.object.count() > 32) return error.InvalidLayaConfig;
            out.n_act = v.object.count() + 1;
        }
        if (obj.get("temperature")) |v| {
            if (v != .array or v.array.items.len != 3) return error.InvalidLayaConfig;
            for (v.array.items, &out.temperature) |item, *dest| dest.* = try positive(item);
        }
        if (obj.get("temperature_by_options")) |v| {
            if (v != .object) return error.InvalidLayaConfig;
            inline for (.{ "choice", "score", "noul" }, 0..) |kind, k| {
                inline for (.{ "2", "3-5", "6-10", "11+" }, 0..) |bucket, b| {
                    if (v.object.get(kind ++ ":" ++ bucket)) |item| out.buckets[k][b] = try positive(item);
                }
            }
        }
        if (obj.get("packing")) |v| out.packing = try parsePacking(v, out.max_len);
        return out;
    }
};

fn parsePacking(value: std.json.Value, max_len: usize) !Packing {
    if (value != .object) return error.InvalidLayaConfig;
    var out = Packing{};
    for (value.object.keys()) |key| {
        if (!std.mem.eql(u8, key, "mode") and !std.mem.eql(u8, key, "max_packed_len")) return error.InvalidLayaConfig;
    }
    const mode = value.object.get("mode") orelse return error.InvalidLayaConfig;
    if (mode != .string) return error.InvalidLayaConfig;
    out.mode = std.meta.stringToEnum(PackingMode, mode.string) orelse return error.InvalidLayaConfig;
    if (out.mode == .none) return out;
    out.max_packed_len = @min(4 * max_len, 8192);
    if (value.object.get("max_packed_len")) |v| {
        if (v != .integer or v.integer < 0) return error.InvalidLayaConfig;
        out.max_packed_len = std.math.cast(usize, v.integer) orelse return error.InvalidLayaConfig;
    }
    // Encoder and head attention materialize one [L, L] mask per packed row.
    if (out.max_packed_len < max_len or out.max_packed_len > 8192) return error.InvalidLayaConfig;
    return out;
}

test "laya packing config defaults, bounds, and rejects unknown fields" {
    const a = std.testing.allocator;
    const cases = [_]struct { json: []const u8, mode: ?PackingMode, len: usize = 0 }{
        .{ .json = "{}", .mode = .none },
        .{ .json = "{\"packing\":{\"mode\":\"question\"}}", .mode = .question, .len = 2048 },
        .{ .json = "{\"packing\":{\"mode\":\"candidate\",\"max_packed_len\":1024}}", .mode = .candidate, .len = 1024 },
        .{ .json = "{\"packing\":{\"mode\":\"none\"}}", .mode = .none },
        .{ .json = "{\"packing\":{\"mode\":\"question\",\"max_packed_len\":256}}", .mode = null },
        .{ .json = "{\"packing\":{\"mode\":\"question\",\"max_packed_len\":65536}}", .mode = null },
        .{ .json = "{\"packing\":{\"mode\":\"tree\"}}", .mode = null },
        .{ .json = "{\"packing\":{\"mode\":\"question\",\"shared\":true}}", .mode = null },
    };
    for (cases) |case| {
        const parsed = try std.json.parseFromSlice(std.json.Value, a, case.json, .{});
        defer parsed.deinit();
        if (case.mode) |mode| {
            const cfg = try Config.parse(parsed.value);
            try std.testing.expectEqual(mode, cfg.packing.mode);
            try std.testing.expectEqual(case.len, cfg.packing.max_packed_len);
            try std.testing.expectEqual(@as(usize, if (mode == .candidate) 255 else 20), cfg.maxOptions());
        } else try std.testing.expectError(error.InvalidLayaConfig, Config.parse(parsed.value));
    }
}
fn positive(value: std.json.Value) !f32 {
    const n: f32 = switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| @floatCast(v),
        else => return error.InvalidLayaConfig,
    };
    if (!std.math.isFinite(n) or n <= 0) return error.InvalidLayaConfig;
    return n;
}

test "laya calibration bucket takes precedence and invalid temperatures fail" {
    const a = std.testing.allocator;
    const p = try std.json.parseFromSlice(std.json.Value, a, "{\"temperature\":[2,3,4],\"temperature_by_options\":{\"choice:3-5\":1.5}}", .{});
    defer p.deinit();
    const cfg = try Config.parse(p.value);
    try std.testing.expectEqual(@as(f32, 1.5), cfg.scale(.choice, 4));
    try std.testing.expectEqual(@as(f32, 2), cfg.scale(.choice, 2));
    try std.testing.expectEqual(@as(f32, 4), cfg.scale(.noul, 2));
    const bad = try std.json.parseFromSlice(std.json.Value, a, "{\"temperature\":[0,1,1]}", .{});
    defer bad.deinit();
    try std.testing.expectError(error.InvalidLayaConfig, Config.parse(bad.value));
}

/// Validate the complete source checkpoint before a backend can execute it.
/// Initial support intentionally accepts one dense safetensors artifact.
pub fn validateWeights(store: @import("tensor_store.zig").TensorStore, cfg: Config, encoder: anytype) !void {
    const reader = store.singleSafetensorsReader() orelse return error.UnsupportedLayaArtifact;
    return validateReader(reader, cfg, encoder);
}

pub fn validateReader(reader: *const @import("safetensors.zig").MMapReader, cfg: Config, encoder: anytype) !void {
    const Check = struct {
        fn tensor(r: @TypeOf(reader), name: []const u8, shape: []const i64) !void {
            const meta = r.header.tensors.get(name) orelse return error.InvalidLayaWeights;
            if (!std.mem.eql(i64, meta.shape, shape)) return error.InvalidLayaWeights;
            switch (meta.dtype) {
                .f32, .f16, .bf16 => {},
                else => return error.InvalidLayaWeights,
            }
        }
        fn pair(r: @TypeOf(reader), prefix: []const u8, input: i64, output: i64) !void {
            var buf: [256]u8 = undefined;
            try tensor(r, try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}), &.{ output, input });
            try tensor(r, try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}), &.{output});
        }
        fn norm(r: @TypeOf(reader), prefix: []const u8, dim: i64, bias: bool) !void {
            var buf: [256]u8 = undefined;
            try tensor(r, try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}), &.{dim});
            if (bias) try tensor(r, try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}), &.{dim});
        }
    };
    const d: i64 = encoder.hidden_size;
    const f: i64 = encoder.intermediate_size;
    try Check.tensor(reader, "encoder.embeddings.tok_embeddings.weight", &.{ encoder.vocab_size, d });
    try Check.norm(reader, "encoder.embeddings.norm", d, false);
    try Check.norm(reader, "encoder.final_norm", d, false);
    var name: [128]u8 = undefined;
    for (0..encoder.num_hidden_layers) |layer| {
        if (layer > 0) try Check.norm(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn_norm", .{layer}), d, false);
        try Check.norm(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp_norm", .{layer}), d, false);
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wqkv.weight", .{layer}), &.{ d * 3, d });
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wo.weight", .{layer}), &.{ d, d });
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wi.weight", .{layer}), &.{ f * 2, d });
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wo.weight", .{layer}), &.{ d, f });
    }
    try Check.tensor(reader, "type_emb.weight", &.{ 3, d });
    try Check.norm(reader, "scorer.0", d, true);
    try Check.pair(reader, "scorer.1", d, d);
    try Check.pair(reader, "scorer.3", d, 1);
    try Check.pair(reader, "act_head.0", d + 4, 256);
    try Check.pair(reader, "act_head.2", 256, @intCast(cfg.n_act));
    for (0..cfg.head_layers) |layer| {
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "head.layers.{d}.self_attn.in_proj_weight", .{layer}), &.{ d * 3, d });
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "head.layers.{d}.self_attn.in_proj_bias", .{layer}), &.{d * 3});
        try Check.pair(reader, try std.fmt.bufPrint(&name, "head.layers.{d}.self_attn.out_proj", .{layer}), d, d);
        try Check.pair(reader, try std.fmt.bufPrint(&name, "head.layers.{d}.linear1", .{layer}), d, d * 4);
        try Check.pair(reader, try std.fmt.bufPrint(&name, "head.layers.{d}.linear2", .{layer}), d * 4, d);
        try Check.norm(reader, try std.fmt.bufPrint(&name, "head.layers.{d}.norm1", .{layer}), d, true);
        try Check.norm(reader, try std.fmt.bufPrint(&name, "head.layers.{d}.norm2", .{layer}), d, true);
    }
}
