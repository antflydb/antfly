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

//! Shared upload/admission policy for the E4B sparse PLE lookup table.
const std = @import("std");
const platform = @import("antfly_platform");
const types = @import("../../gguf/tensor_types.zig");

pub fn requested() bool {
    // Discrete-GPU latency has not yet been qualified. Preserve placement by
    // default, including the existing Metal unified-memory implementation.
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GEMMA4_PLE_HOST", false);
}

pub fn eligibleConfig(config: @import("../../models/gpt.zig").Config) bool {
    return config.usesGemma4Channels() and !config.usesMoe() and !config.gemma4_mtp_assistant and
        config.hidden_size == 2560 and config.num_hidden_layers == 42 and config.num_key_value_heads == 2;
}

pub fn eligible(name: []const u8, rows: u64, cols: u64, tensor_type: types.TensorType) bool {
    if (rows == 0 or cols != 10752) return false;
    if (!std.mem.eql(u8, name, "per_layer_token_embd.weight") and
        !std.mem.eql(u8, name, "model.per_layer_input.per_layer_token_embd.weight")) return false;
    return switch (tensor_type) {
        .known => |kind| kind == .Q6_K,
        else => false,
    };
}

test "Gemma4 CUDA host PLE placement excludes projections and unrelated tables" {
    try std.testing.expect(eligible("per_layer_token_embd.weight", 262144, 10752, .{ .known = .Q6_K }));
    try std.testing.expect(eligible("model.per_layer_input.per_layer_token_embd.weight", 2, 10752, .{ .known = .Q6_K }));
    try std.testing.expect(!eligible("model.per_layer_input.per_layer_model_proj.weight", 2560, 10752, .{ .known = .Q6_K }));
    try std.testing.expect(!eligible("per_layer_token_embd.weight", 262144, 5376, .{ .known = .Q6_K }));
    try std.testing.expect(!eligible("per_layer_token_embd.weight", 0, 10752, .{ .known = .Q6_K }));
    try std.testing.expect(!eligible("per_layer_token_embd.weight", 262144, 10752, .{ .known = .Q4_0 }));
}

/// An independent load lease covers the pinned copy while the native GGUF
/// mapping is alive and through physical retirement. Parse headers only.
pub fn estimateGgufBytes(allocator: std.mem.Allocator, path: ?[]const u8) !usize {
    return estimateGgufBytesForPolicy(allocator, path, requested());
}

pub fn estimateGgufBytesForPolicy(allocator: std.mem.Allocator, path: ?[]const u8, enabled: bool) !usize {
    if (!enabled) return 0;
    const file_path = path orelse return 0;
    var mapped = try @import("../../util/c_file.zig").MmapRegion.init(allocator, file_path);
    mapped.preserveFileCacheOnDeinit();
    defer mapped.deinit();
    const format = @import("../../gguf/format.zig");
    var parsed = try format.parseStructure(allocator, mapped.data);
    defer parsed.deinit(allocator);
    try format.validateTensorDataRanges(&parsed, mapped.data.len);
    return estimateParsedBytes(&parsed);
}

/// The caller validates data ranges before using a parsed header for admission.
pub fn estimateParsedBytes(parsed: *const @import("../../gguf/format.zig").File) !usize {
    var bytes: usize = 0;
    for (parsed.tensors) |tensor| {
        // GGUF orders dimensions from contiguous columns to rows.
        if (tensor.dimensions.len != 2 or !eligible(tensor.name, tensor.dimensions[1], tensor.dimensions[0], tensor.tensor_type)) continue;
        const count = types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.InvalidShape;
        bytes = try std.math.add(usize, bytes, std.math.cast(usize, count) orelse return error.ResourceLimitExceeded);
    }
    return bytes;
}

test "Gemma4 CUDA host PLE admission sizes encoded rows and rejects truncation" {
    const a = std.testing.allocator;
    try std.testing.expectEqual(@as(usize, 0), try estimateGgufBytesForPolicy(a, "/missing-model.gguf", false));
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const writer = @import("../../gguf/writer.zig");
    const tensors = [_]writer.TensorSpec{
        .{ .name = "per_layer_token_embd.weight", .dimensions = &.{ 10752, 2 }, .tensor_type = .{ .known = .Q6_K } },
        .{ .name = "token_embd.weight", .dimensions = &.{ 10752, 2 }, .tensor_type = .{ .known = .Q6_K } },
    };
    var layout = try writer.buildLayout(a, &.{}, &tensors);
    defer layout.deinit(a);
    const encoded: usize = @intCast(types.byteLen(tensors[0].tensor_type, tensors[0].dimensions).?);
    const header_len = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    const bytes = try a.alloc(u8, header_len + @as(usize, @intCast(layout.offsets[1])) + encoded);
    defer a.free(bytes);
    @memset(bytes, 0);
    @memcpy(bytes[0..layout.header_bytes.len], layout.header_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = bytes });
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, "model.gguf", a);
    defer a.free(path);
    try std.testing.expectEqual(encoded, try estimateGgufBytesForPolicy(a, path, true));
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = bytes[0 .. bytes.len - 1] });
    try std.testing.expectError(error.TruncatedTensorData, estimateGgufBytesForPolicy(a, path, true));
}

test "Gemma4 CUDA host PLE model policy excludes experts assistant and other geometries" {
    const Config = @import("../../models/gpt.zig").Config;
    const dense = Config{ .family = .gemma, .gemma4_channel_protocol = true, .hidden_size = 2560, .num_hidden_layers = 42, .num_key_value_heads = 2 };
    try std.testing.expect(eligibleConfig(dense));
    var excluded = dense;
    excluded.gemma4_mtp_assistant = true;
    try std.testing.expect(!eligibleConfig(excluded));
    excluded = dense;
    excluded.num_local_experts = 8;
    excluded.num_experts_per_tok = 2;
    try std.testing.expect(!eligibleConfig(excluded));
    excluded = dense;
    excluded.num_hidden_layers = 41;
    try std.testing.expect(!eligibleConfig(excluded));
    excluded = dense;
    excluded.gemma4_channel_protocol = false;
    try std.testing.expect(!eligibleConfig(excluded));
}
