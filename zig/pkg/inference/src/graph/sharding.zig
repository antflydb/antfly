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

// Weight sharding specification for tensor parallelism.
//
// Describes how model weights should be partitioned across devices.
// Column-parallel splits the output dimension (Q/K/V/gate/up projections);
// row-parallel splits the input dimension (output/down projections).
// Replicated weights (norms, embeddings) are copied to all devices.
//
// Pattern matching uses substring search on weight names, which is
// sufficient for standard transformer naming conventions
// (e.g. "q_proj.weight", "down_proj.weight").

const std = @import("std");

pub const ShardDim = enum {
    /// Split along output dimension (rows). For weight [out_dim, in_dim],
    /// shard i gets rows [i*out_dim/N .. (i+1)*out_dim/N].
    column,
    /// Split along input dimension (columns). For weight [out_dim, in_dim],
    /// shard i gets columns [i*in_dim/N .. (i+1)*in_dim/N] from each row.
    row,
    /// No sharding — replicate the full weight on every device.
    replicate,
};

pub const ShardRule = struct {
    /// Substring pattern to match against weight names.
    name_pattern: []const u8,
    /// How to shard weights matching this pattern.
    dim: ShardDim,
};

pub const ShardingSpec = struct {
    rules: []const ShardRule,
    num_shards: u16,

    /// Determine the shard dimension for a given weight name.
    /// Returns .replicate if no rule matches.
    pub fn shardDimForWeight(self: *const ShardingSpec, name: []const u8) ShardDim {
        for (self.rules) |rule| {
            if (containsSubstring(name, rule.name_pattern)) return rule.dim;
        }
        return .replicate;
    }
};

fn containsSubstring(haystack: []const u8, needle: []const u8) bool {
    if (needle.len > haystack.len) return false;
    if (needle.len == 0) return true;
    const end = haystack.len - needle.len + 1;
    for (0..end) |i| {
        if (std.mem.eql(u8, haystack[i..][0..needle.len], needle)) return true;
    }
    return false;
}

/// Pre-built sharding spec for standard GPT tensor parallelism.
/// Q/K/V/gate/up projections: column-split (split attention heads / FFN intermediate).
/// Output/down projections: row-split (each device has partial result, needs all_reduce).
/// Norms, embeddings, and everything else: replicate.
pub const gpt_tensor_parallel_rules = [_]ShardRule{
    .{ .name_pattern = "q_proj.weight", .dim = .column },
    .{ .name_pattern = "k_proj.weight", .dim = .column },
    .{ .name_pattern = "v_proj.weight", .dim = .column },
    .{ .name_pattern = "o_proj.weight", .dim = .row },
    .{ .name_pattern = "gate_proj.weight", .dim = .column },
    .{ .name_pattern = "up_proj.weight", .dim = .column },
    .{ .name_pattern = "down_proj.weight", .dim = .row },
};

pub fn gptTensorParallelSpec(num_shards: u16) ShardingSpec {
    return .{
        .rules = &gpt_tensor_parallel_rules,
        .num_shards = num_shards,
    };
}

/// Slice a flat f32 weight buffer along the output dimension (rows).
/// weight is [out_dim, in_dim], shard_index selects a contiguous
/// block of rows.
pub fn sliceColumns(
    allocator: std.mem.Allocator,
    data: []const f32,
    out_dim: usize,
    in_dim: usize,
    shard_index: u16,
    num_shards: u16,
) ![]f32 {
    const shard_out = out_dim / num_shards;
    const start_row = shard_index * shard_out;
    const shard_size = shard_out * in_dim;
    const result = try allocator.alloc(f32, shard_size);
    const offset = start_row * in_dim;
    @memcpy(result, data[offset..][0..shard_size]);
    return result;
}

/// Slice a flat f32 weight buffer along the input dimension (columns).
/// weight is [out_dim, in_dim], shard_index selects a contiguous
/// block of columns from each row.
pub fn sliceRows(
    allocator: std.mem.Allocator,
    data: []const f32,
    out_dim: usize,
    in_dim: usize,
    shard_index: u16,
    num_shards: u16,
) ![]f32 {
    const shard_in = in_dim / num_shards;
    const start_col = @as(usize, shard_index) * shard_in;
    const result = try allocator.alloc(f32, out_dim * shard_in);
    for (0..out_dim) |row| {
        const src_offset = row * in_dim + start_col;
        const dst_offset = row * shard_in;
        @memcpy(result[dst_offset..][0..shard_in], data[src_offset..][0..shard_in]);
    }
    return result;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "ShardingSpec matches patterns" {
    const spec = gptTensorParallelSpec(2);

    try std.testing.expectEqual(ShardDim.column, spec.shardDimForWeight("model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expectEqual(ShardDim.column, spec.shardDimForWeight("model.layers.5.self_attn.k_proj.weight"));
    try std.testing.expectEqual(ShardDim.column, spec.shardDimForWeight("model.layers.0.self_attn.v_proj.weight"));
    try std.testing.expectEqual(ShardDim.row, spec.shardDimForWeight("model.layers.0.self_attn.o_proj.weight"));
    try std.testing.expectEqual(ShardDim.column, spec.shardDimForWeight("model.layers.0.mlp.gate_proj.weight"));
    try std.testing.expectEqual(ShardDim.column, spec.shardDimForWeight("model.layers.0.mlp.up_proj.weight"));
    try std.testing.expectEqual(ShardDim.row, spec.shardDimForWeight("model.layers.0.mlp.down_proj.weight"));
    try std.testing.expectEqual(ShardDim.replicate, spec.shardDimForWeight("model.layers.0.input_layernorm.weight"));
    try std.testing.expectEqual(ShardDim.replicate, spec.shardDimForWeight("model.embed_tokens.weight"));
}

test "sliceColumns splits rows evenly" {
    const allocator = std.testing.allocator;

    // 4x3 matrix, 2 shards → each gets 2x3
    const data = [_]f32{
        1, 2, 3, // row 0
        4, 5, 6, // row 1
        7, 8, 9, // row 2
        10, 11, 12, // row 3
    };

    const shard0 = try sliceColumns(allocator, &data, 4, 3, 0, 2);
    defer allocator.free(shard0);
    const shard1 = try sliceColumns(allocator, &data, 4, 3, 1, 2);
    defer allocator.free(shard1);

    try std.testing.expectEqual(@as(usize, 6), shard0.len);
    try std.testing.expectEqual(@as(usize, 6), shard1.len);

    // Shard 0: rows 0-1
    try std.testing.expectEqual(@as(f32, 1), shard0[0]);
    try std.testing.expectEqual(@as(f32, 6), shard0[5]);
    // Shard 1: rows 2-3
    try std.testing.expectEqual(@as(f32, 7), shard1[0]);
    try std.testing.expectEqual(@as(f32, 12), shard1[5]);
}

test "sliceRows splits columns evenly" {
    const allocator = std.testing.allocator;

    // 2x4 matrix, 2 shards → each gets 2x2
    const data = [_]f32{
        1, 2, 3, 4, // row 0
        5, 6, 7, 8, // row 1
    };

    const shard0 = try sliceRows(allocator, &data, 2, 4, 0, 2);
    defer allocator.free(shard0);
    const shard1 = try sliceRows(allocator, &data, 2, 4, 1, 2);
    defer allocator.free(shard1);

    try std.testing.expectEqual(@as(usize, 4), shard0.len);
    try std.testing.expectEqual(@as(usize, 4), shard1.len);

    // Shard 0: columns 0-1
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 5, 6 }, shard0);
    // Shard 1: columns 2-3
    try std.testing.expectEqualSlices(f32, &.{ 3, 4, 7, 8 }, shard1);
}

test "row-sharded partial results sum to full matmul" {
    // Verify that splitting a weight row-wise and summing partial outputs
    // reconstructs the full matrix-vector product.
    const allocator = std.testing.allocator;

    // Weight: 2x4 (out_dim=2, in_dim=4)
    const weight = [_]f32{
        1, 2, 3, 4,
        5, 6, 7, 8,
    };
    const input = [_]f32{ 1, 1, 1, 1 };

    // Full matmul: [1+2+3+4, 5+6+7+8] = [10, 26]
    const expected = [_]f32{ 10, 26 };

    // Split weight into 2 row-shards
    const w0 = try sliceRows(allocator, &weight, 2, 4, 0, 2);
    defer allocator.free(w0);
    const w1 = try sliceRows(allocator, &weight, 2, 4, 1, 2);
    defer allocator.free(w1);

    // Partial matmuls: each shard uses its columns of the input
    var partial0 = [_]f32{ 0, 0 };
    var partial1 = [_]f32{ 0, 0 };
    for (0..2) |row| {
        for (0..2) |col| {
            partial0[row] += w0[row * 2 + col] * input[col];
            partial1[row] += w1[row * 2 + col] * input[2 + col];
        }
    }

    // Sum partials → should equal full result
    var result = [_]f32{ 0, 0 };
    for (0..2) |i| result[i] = partial0[i] + partial1[i];

    try std.testing.expectEqualSlices(f32, &expected, &result);
}
