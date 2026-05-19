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

// Attribute extraction helpers for ONNX NodeProto attributes.
//
// Provides typed accessors with defaults, matching the pattern used
// in onnx-gomlx's GetIntAttrOr / GetFloatAttrOr / etc.

const std = @import("std");
const proto = @import("proto.zig");

const AttributeProto = proto.AttributeProto;
const TensorProto = proto.TensorProto;

/// Find an attribute by name in the attribute list.
pub fn findAttr(attributes: []const AttributeProto, name: []const u8) ?*const AttributeProto {
    for (attributes) |*attr| {
        if (std.mem.eql(u8, attr.name, name)) return attr;
    }
    return null;
}

/// Get an integer attribute, or return default.
pub fn getInt(attributes: []const AttributeProto, name: []const u8, default: i64) i64 {
    if (findAttr(attributes, name)) |attr| return attr.i;
    return default;
}

/// Get a float attribute, or return default.
pub fn getFloat(attributes: []const AttributeProto, name: []const u8, default: f32) f32 {
    if (findAttr(attributes, name)) |attr| return attr.f;
    return default;
}

/// Get a string attribute, or return default.
pub fn getString(attributes: []const AttributeProto, name: []const u8, default: []const u8) []const u8 {
    if (findAttr(attributes, name)) |attr| return attr.s;
    return default;
}

/// Get an integer array attribute, or return empty slice.
pub fn getInts(attributes: []const AttributeProto, name: []const u8) []const i64 {
    if (findAttr(attributes, name)) |attr| return attr.ints;
    return &.{};
}

/// Get a float array attribute, or return empty slice.
pub fn getFloats(attributes: []const AttributeProto, name: []const u8) []const f32 {
    if (findAttr(attributes, name)) |attr| return attr.floats;
    return &.{};
}

/// Get a tensor attribute, or return null.
pub fn getTensor(attributes: []const AttributeProto, name: []const u8) ?*const TensorProto {
    if (findAttr(attributes, name)) |attr| {
        if (attr.t) |*t| return t;
    }
    return null;
}

/// Get a graph attribute (sub-graph for If/Loop/Scan), or return null.
pub fn getGraph(attributes: []const AttributeProto, name: []const u8) ?*const proto.GraphProto {
    if (findAttr(attributes, name)) |attr| {
        if (attr.g) |*g| return g;
    }
    return null;
}

// ── Tests ────────────────────────────────────────────────────────────

test "getInt finds attribute" {
    const attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 2 },
        .{ .name = "keepdims", .i = 1 },
    };
    try std.testing.expectEqual(@as(i64, 2), getInt(&attrs, "axis", 0));
    try std.testing.expectEqual(@as(i64, 1), getInt(&attrs, "keepdims", 0));
}

test "getInt returns default for missing" {
    const attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 2 },
    };
    try std.testing.expectEqual(@as(i64, -1), getInt(&attrs, "missing", -1));
}

test "getFloat finds attribute" {
    const attrs = [_]AttributeProto{
        .{ .name = "epsilon", .f = 1e-5 },
    };
    try std.testing.expectEqual(@as(f32, 1e-5), getFloat(&attrs, "epsilon", 0));
}

test "getString finds attribute" {
    const attrs = [_]AttributeProto{
        .{ .name = "mode", .s = "reflect" },
    };
    try std.testing.expectEqualStrings("reflect", getString(&attrs, "mode", ""));
}

test "getInts returns values" {
    var ints = [_]i64{ 0, 2, 3 };
    const attrs = [_]AttributeProto{
        .{ .name = "perm", .ints = &ints },
    };
    const result = getInts(&attrs, "perm");
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(i64, 2), result[1]);
}

test "getInts returns empty for missing" {
    const attrs = [_]AttributeProto{};
    try std.testing.expectEqual(@as(usize, 0), getInts(&attrs, "perm").len);
}
