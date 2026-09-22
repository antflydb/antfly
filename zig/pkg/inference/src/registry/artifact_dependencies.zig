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

//! Artifact references are authoritative: never guess shard or external-data names.
const std = @import("std");
const wire = @import("protobuf").wire;
const receipt = @import("managed_receipt.zig");

pub const Paths = struct {
    allocator: std.mem.Allocator,
    items: std.StringHashMapUnmanaged(void) = .empty,

    pub fn deinit(self: *Paths) void {
        var keys = self.items.keyIterator();
        while (keys.next()) |key| self.allocator.free(key.*);
        self.items.deinit(self.allocator);
    }

    fn add(self: *Paths, parent: []const u8, relative: []const u8) !void {
        if (!receipt.artifactPathIsSafe(relative)) return error.InvalidModelArtifactPath;
        const directory = std.fs.path.dirname(parent);
        const path = if (directory) |dir|
            try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ dir, relative })
        else
            try self.allocator.dupe(u8, relative);
        errdefer self.allocator.free(path);
        if (self.items.count() >= receipt.max_artifact_count) return error.TooManyArtifactDependencies;
        const entry = try self.items.getOrPut(self.allocator, path);
        if (entry.found_existing) self.allocator.free(path);
    }
};

pub fn safetensors(allocator: std.mem.Allocator, filename: []const u8, bytes: []const u8) !Paths {
    var paths: Paths = .{ .allocator = allocator };
    errdefer paths.deinit();
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSafetensorsIndex;
    const weights = parsed.value.object.get("weight_map") orelse return error.InvalidSafetensorsIndex;
    if (weights != .object or weights.object.count() == 0) return error.InvalidSafetensorsIndex;
    for (weights.object.values()) |value| {
        if (value != .string or !std.mem.endsWith(u8, value.string, ".safetensors")) return error.InvalidSafetensorsIndex;
        try paths.add(filename, value.string);
    }
    return paths;
}

// Traverse only schema-defined protobuf messages, skipping raw tensor payloads.
// Includes Constant attributes, subgraphs, sparse tensors and local functions.
// Field numbers follow onnx/onnx.proto, not heuristics over arbitrary bytes.
const Message = enum { model, graph, node, attribute, tensor, sparse, training, function };

fn childMessage(kind: Message, field: u32) ?Message {
    return switch (kind) {
        .model => switch (field) {
            7 => .graph,
            20 => .training,
            25 => .function,
            else => null,
        },
        .graph => switch (field) {
            1 => .node,
            5 => .tensor,
            15 => .sparse,
            else => null,
        },
        .node => if (field == 5) .attribute else null,
        .attribute => switch (field) {
            5, 10 => .tensor,
            6, 11 => .graph,
            22, 23 => .sparse,
            else => null,
        },
        .sparse => switch (field) {
            1, 2 => .tensor,
            else => null,
        },
        .training => switch (field) {
            1, 2 => .graph,
            else => null,
        },
        .function => switch (field) {
            7 => .node,
            11 => .attribute,
            else => null,
        },
        .tensor => null,
    };
}

// Check subtraction before slicing; corrupt lengths must not overflow usize.
fn readBytes(bytes: []const u8, pos: *usize) ![]const u8 {
    const size = std.math.cast(usize, try wire.readVarint(bytes, pos)) orelse return error.InvalidOnnxModel;
    if (size > bytes.len - pos.*) return error.InvalidOnnxModel;
    const result = bytes[pos.*..][0..size];
    pos.* += size;
    return result;
}

fn externalLocation(bytes: []const u8) !?[]const u8 {
    var pos: usize = 0;
    var key: []const u8 = "";
    var value: []const u8 = "";
    while (pos < bytes.len) {
        const tag = try wire.readTag(bytes, &pos);
        if (tag.wire_type == .length_delimited) {
            const text = try readBytes(bytes, &pos);
            if (tag.field == 1) key = text;
            if (tag.field == 2) value = text;
        } else try wire.skipField(bytes, &pos, tag.wire_type);
    }
    return if (std.mem.eql(u8, key, "location")) value else null;
}

fn visit(paths: *Paths, filename: []const u8, bytes: []const u8, kind: Message, depth: usize) anyerror!void {
    if (depth > 64) return error.OnnxGraphNestingTooDeep;
    var pos: usize = 0;
    while (pos < bytes.len) {
        const tag = try wire.readTag(bytes, &pos);
        if (tag.field == 0) return error.InvalidOnnxModel;
        if (tag.wire_type != .length_delimited) {
            try wire.skipField(bytes, &pos, tag.wire_type);
            continue;
        }
        const payload = try readBytes(bytes, &pos);
        if (kind == .tensor and tag.field == 13) {
            if (try externalLocation(payload)) |location| try paths.add(filename, location);
        } else if (childMessage(kind, tag.field)) |child| {
            try visit(paths, filename, payload, child, depth + 1);
        }
    }
}

pub fn onnx(allocator: std.mem.Allocator, filename: []const u8, bytes: []const u8) !Paths {
    var paths: Paths = .{ .allocator = allocator };
    errdefer paths.deinit();
    try visit(&paths, filename, bytes, .model, 0);
    return paths;
}

test "safetensors index selects arbitrary shard names and deduplicates references" {
    var paths = try safetensors(std.testing.allocator, "weights/model.safetensors.index.json",
        \\{"weight_map":{"a":"custom.safetensors","b":"custom.safetensors","c":"parts/other.safetensors"}}
    );
    defer paths.deinit();
    try std.testing.expectEqual(@as(u32, 2), paths.items.count());
    try std.testing.expect(paths.items.contains("weights/custom.safetensors"));
    try std.testing.expect(paths.items.contains("weights/parts/other.safetensors"));
    try std.testing.expectError(error.InvalidModelArtifactPath, safetensors(std.testing.allocator, "model.safetensors.index.json",
        \\{"weight_map":{"a":"../escape.safetensors"}}
    ));
}

test "ONNX dependencies include external Constant attributes as well as initializers" {
    const allocator = std.testing.allocator;
    var entry: wire.Buf = .empty;
    defer entry.deinit(allocator);
    try wire.writeString(allocator, &entry, 1, "location");
    try wire.writeString(allocator, &entry, 2, "Constant_7_attr__value");
    var tensor: wire.Buf = .empty;
    defer tensor.deinit(allocator);
    try wire.writeString(allocator, &tensor, 13, entry.items);
    var attribute: wire.Buf = .empty;
    defer attribute.deinit(allocator);
    try wire.writeString(allocator, &attribute, 5, tensor.items);
    var node: wire.Buf = .empty;
    defer node.deinit(allocator);
    try wire.writeString(allocator, &node, 5, attribute.items);
    var graph: wire.Buf = .empty;
    defer graph.deinit(allocator);
    try wire.writeString(allocator, &graph, 1, node.items);
    try wire.writeString(allocator, &graph, 5, tensor.items);
    var model: wire.Buf = .empty;
    defer model.deinit(allocator);
    try wire.writeString(allocator, &model, 7, graph.items);
    var paths = try onnx(allocator, "onnx/model.onnx", model.items);
    defer paths.deinit();
    try std.testing.expectEqual(@as(u32, 1), paths.items.count());
    try std.testing.expect(paths.items.contains("onnx/Constant_7_attr__value"));
}

test "artifact references reject traversal and malformed protobuf lengths" {
    const allocator = std.testing.allocator;
    for ([_][]const u8{ "../weights.bin", "/weights.bin", "C:/weights.bin", "https://example.com/weights.bin", "sub/../weights.bin" }) |path| {
        var paths: Paths = .{ .allocator = allocator };
        defer paths.deinit();
        try std.testing.expectError(error.InvalidModelArtifactPath, paths.add("onnx/model.onnx", path));
    }
    try std.testing.expectError(error.InvalidOnnxModel, onnx(allocator, "model.onnx", &.{ 0x3a, 0xff, 0xff, 0xff, 0xff, 0x7f }));
}
