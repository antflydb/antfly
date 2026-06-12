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

pub const artifact_family_version = "recursive_lora/v1alpha1";

pub const Config = struct {
    enabled: bool = false,
    source_num_layers: usize = 0,
    shared_block_size: usize = 0,
    loop_count: usize = 0,
    init_strategy: []const u8 = "average_residual_svd",
};

pub const LayerMapping = struct {
    logical_layer: usize,
    physical_layer: usize,
    loop_index: usize,
};

pub const AdapterTensorKind = enum { a, b };

pub fn inferLoopCount(source_num_layers: usize, shared_block_size: usize) !usize {
    if (source_num_layers == 0) return error.InvalidRecursiveLoRAConfig;
    if (shared_block_size == 0) return error.InvalidRecursiveLoRAConfig;
    if (shared_block_size > source_num_layers) return error.InvalidRecursiveLoRAConfig;
    if (source_num_layers % shared_block_size != 0) return error.InvalidRecursiveLoRAConfig;
    return source_num_layers / shared_block_size;
}

pub fn validate(config: Config) !void {
    if (!config.enabled) return;
    const inferred = try inferLoopCount(config.source_num_layers, config.shared_block_size);
    if (config.loop_count != inferred) return error.InvalidRecursiveLoRAConfig;
}

pub fn physicalLayerFor(logical_layer: usize, shared_block_size: usize) !usize {
    if (shared_block_size == 0) return error.InvalidRecursiveLoRAConfig;
    return logical_layer % shared_block_size;
}

pub fn loopIndexFor(logical_layer: usize, shared_block_size: usize) !usize {
    if (shared_block_size == 0) return error.InvalidRecursiveLoRAConfig;
    return logical_layer / shared_block_size;
}

pub fn buildLayerMapping(
    allocator: std.mem.Allocator,
    source_num_layers: usize,
    shared_block_size: usize,
) ![]LayerMapping {
    _ = try inferLoopCount(source_num_layers, shared_block_size);
    const mapping = try allocator.alloc(LayerMapping, source_num_layers);
    errdefer allocator.free(mapping);
    for (mapping, 0..) |*entry, logical| {
        entry.* = .{
            .logical_layer = logical,
            .physical_layer = try physicalLayerFor(logical, shared_block_size),
            .loop_index = try loopIndexFor(logical, shared_block_size),
        };
    }
    return mapping;
}

pub fn formatLoopAdapterTensorName(
    allocator: std.mem.Allocator,
    base_tensor_name: []const u8,
    loop_index: usize,
    comptime kind: AdapterTensorKind,
) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "{s}.loop_{d}.lora_{c}.weight",
        .{ base_tensor_name, loop_index, if (kind == .a) @as(u8, 'A') else @as(u8, 'B') },
    );
}

pub const ParsedLoopAdapterTensorName = struct {
    base_tensor_name: []const u8,
    loop_index: usize,
    kind: AdapterTensorKind,
};

pub fn parseLoopAdapterTensorName(name: []const u8) ?ParsedLoopAdapterTensorName {
    const suffix_a = ".lora_A.weight";
    const suffix_b = ".lora_B.weight";
    const kind: AdapterTensorKind, const without_suffix = blk: {
        if (std.mem.endsWith(u8, name, suffix_a)) break :blk .{ .a, name[0 .. name.len - suffix_a.len] };
        if (std.mem.endsWith(u8, name, suffix_b)) break :blk .{ .b, name[0 .. name.len - suffix_b.len] };
        return null;
    };

    const marker = ".loop_";
    const marker_pos = std.mem.lastIndexOf(u8, without_suffix, marker) orelse return null;
    const digits = without_suffix[marker_pos + marker.len ..];
    if (digits.len == 0) return null;
    const loop_index = std.fmt.parseUnsigned(usize, digits, 10) catch return null;
    return .{
        .base_tensor_name = without_suffix[0..marker_pos],
        .loop_index = loop_index,
        .kind = kind,
    };
}

test "recursive lora layer mapping cycles physical layers" {
    const allocator = std.testing.allocator;
    const mapping = try buildLayerMapping(allocator, 6, 2);
    defer allocator.free(mapping);

    try std.testing.expectEqual(@as(usize, 6), mapping.len);
    try std.testing.expectEqual(@as(usize, 0), mapping[0].physical_layer);
    try std.testing.expectEqual(@as(usize, 1), mapping[1].physical_layer);
    try std.testing.expectEqual(@as(usize, 0), mapping[2].physical_layer);
    try std.testing.expectEqual(@as(usize, 1), mapping[5].physical_layer);
    try std.testing.expectEqual(@as(usize, 2), mapping[5].loop_index);
}

test "recursive lora loop adapter tensor names round trip" {
    const allocator = std.testing.allocator;
    const name = try formatLoopAdapterTensorName(allocator, "model.layers.0.self_attn.q_proj.weight", 3, .a);
    defer allocator.free(name);

    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.weight.loop_3.lora_A.weight", name);
    const parsed = parseLoopAdapterTensorName(name).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.weight", parsed.base_tensor_name);
    try std.testing.expectEqual(@as(usize, 3), parsed.loop_index);
    try std.testing.expectEqual(.a, parsed.kind);
}
