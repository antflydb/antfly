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

//! Frozen physical column and JSON-path references. Resolve these at creation,
//! never by trying an exact dotted column name before a nested path at runtime.
const std = @import("std");
pub const Element = union(enum) { key: []const u8, index: u32 };
pub const Cell = struct { value: std.json.Value = .null, nonnull: bool = false };
pub const Reference = struct {
    column: []const u8,
    path: []const Element = &.{},
    /// SQL JSON results preserve a present JSON literal null; scalar results
    /// map that literal to SQL NULL. Missing paths always yield SQL NULL.
    result: enum { scalar, json } = .scalar,

    pub fn validate(self: Reference) !void {
        if (self.column.len == 0 or self.column.len > 256 or !std.unicode.utf8ValidateSlice(self.column) or
            std.mem.indexOfScalar(u8, self.column, 0) != null or self.path.len > 32) return error.InvalidRetainedSeries;
        var bytes = self.column.len;
        for (self.path) |part| switch (part) {
            .key => |name| {
                if (name.len > 256 or !std.unicode.utf8ValidateSlice(name)) return error.InvalidRetainedSeries;
                bytes += name.len;
            },
            .index => bytes += 4,
        };
        if (bytes > 1024) return error.InvalidRetainedSeries;
    }

    pub fn get(self: Reference, root: std.json.Value, sql_presence: std.StringHashMapUnmanaged(bool)) Cell {
        if (root != .object) return .{};
        var current = root.object.get(self.column) orelse return .{};
        // Packed rows know SQL-null provenance independently of JSON values.
        // This also works when COUNT(root) only projected presence, not bytes.
        if (sql_presence.get(self.column)) |present| if (!present) return .{};
        for (self.path) |part| current = switch (part) {
            .key => |name| if (current == .object) current.object.get(name) orelse return .{} else return .{},
            .index => |index| if (current == .array and index < current.array.items.len) current.array.items[index] else return .{},
        };
        return .{ .value = current, .nonnull = self.result == .json or current != .null };
    }
};

test "retained frozen fields keep dotted columns distinct from paths and preserve SQL JSON nulls" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"a.b\":99,\"a\":{\"b\":1,\"items\":[null,2]},\"doc\":null}", .{});
    defer parsed.deinit();
    const nested: Reference = .{ .column = "a", .path = &.{.{ .key = "b" }} };
    const literal: Reference = .{ .column = "a.b" };
    try nested.validate();
    try literal.validate();
    try std.testing.expectEqual(@as(i64, 1), nested.get(parsed.value, .empty).value.integer);
    try std.testing.expectEqual(@as(i64, 99), literal.get(parsed.value, .empty).value.integer);
    const json_null: Reference = .{ .column = "a", .path = &.{ .{ .key = "items" }, .{ .index = 0 } }, .result = .json };
    try std.testing.expect(json_null.get(parsed.value, .empty).nonnull);
    var scalar_null = json_null;
    scalar_null.result = .scalar;
    try std.testing.expect(!scalar_null.get(parsed.value, .empty).nonnull);
    var missing = json_null;
    missing.path = &.{ .{ .key = "items" }, .{ .index = 99 } };
    try std.testing.expect(!missing.get(parsed.value, .empty).nonnull);
    missing.path = &.{.{ .key = "missing" }};
    try std.testing.expect(!missing.get(parsed.value, .empty).nonnull);
    const doc: Reference = .{ .column = "doc", .result = .json };
    try std.testing.expect(doc.get(parsed.value, .empty).nonnull);
    var presence: std.StringHashMapUnmanaged(bool) = .empty;
    defer presence.deinit(a);
    try presence.put(a, "doc", false);
    try std.testing.expect(!doc.get(parsed.value, presence).nonnull);
    try presence.put(a, "doc", true);
    try std.testing.expect(doc.get(parsed.value, presence).nonnull);
    // The durable representation retains path element identity, including
    // numeric-looking object keys versus array indices.
    const bytes = try std.json.Stringify.valueAlloc(a, json_null, .{});
    defer a.free(bytes);
    var decoded = try std.json.parseFromSlice(Reference, a, bytes, .{});
    defer decoded.deinit();
    try decoded.value.validate();
    try std.testing.expectEqualDeep(json_null, decoded.value);
}

/// Portable extraction program compiled against one immutable source schema.
/// Persist it inside the definition identity, not only in a host's cache.
pub const Projection = struct {
    format: u8 = 1,
    source_schema: [32]u8,
    timestamp: Reference,
    dimensions: []const Reference = &.{},
    measures: []const ?Reference,

    pub fn validate(self: Projection) !void {
        if (self.format != 1 or std.mem.allEqual(u8, &self.source_schema, 0) or self.dimensions.len > 32 or
            self.measures.len == 0 or self.measures.len > 256) return error.InvalidRetainedSeries;
        try self.timestamp.validate();
        for (self.dimensions) |dimension| try dimension.validate();
        for (self.measures) |measure| if (measure) |reference| try reference.validate();
    }
    pub fn jsonStringify(self: Projection, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("format");
        try writer.write(self.format);
        try writer.objectField("source_schema");
        try writer.beginArray();
        for (self.source_schema) |byte| try writer.write(byte);
        try writer.endArray();
        try writer.objectField("timestamp");
        try writer.write(self.timestamp);
        try writer.objectField("dimensions");
        try writer.write(self.dimensions);
        try writer.objectField("measures");
        try writer.write(self.measures);
        try writer.endObject();
    }
};

test "retained projection persists exact schema identity and bounded typed references" {
    const a = std.testing.allocator;
    var projection: Projection = .{ .source_schema = @splat(255), .timestamp = .{ .column = "at" }, .dimensions = &.{.{ .column = "payload", .path = &.{.{ .key = "region" }} }}, .measures = &.{ null, .{ .column = "amount" } } };
    try projection.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, projection, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(Projection, a, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    try std.testing.expectEqualDeep(projection, parsed.value);
    projection.source_schema = @splat(0);
    try std.testing.expectError(error.InvalidRetainedSeries, projection.validate());
}
