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

//! Versioned graph transport values. User maps never share the discriminator
//! namespace; INTEGER uses decimal text so JavaScript preserves every bit.
const std = @import("std");
const v = @import("value.zig");
pub const version = 1;
pub const Value = struct {
    value: v.Value,
    include_native: bool = false,
    /// Decode owner values directly into canonical values. The canonical
    /// type-first envelope avoids a JSON DOM for nodes and their properties;
    /// member-order-independent inputs retain the version-one contract.
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!Value {
        return .{ .value = parseTyped(a, source, options, 0) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.UnexpectedToken,
        }, .include_native = true };
    }
    pub fn jsonStringify(self: Value, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("type");
        try writer.write(@tagName(self.value));
        try writer.objectField("value");
        switch (self.value) {
            .null => try writer.write(@as(?bool, null)),
            .boolean => |x| try writer.write(x),
            .integer => |x| {
                var buffer: [32]u8 = undefined;
                try writer.write(std.fmt.bufPrint(&buffer, "{d}", .{x}) catch unreachable);
            },
            .number => |x| try writer.write(x),
            .string => |x| try writer.write(x),
            .list => |items| {
                try writer.beginArray();
                for (items) |item| try writer.write(Value{ .value = item, .include_native = self.include_native });
                try writer.endArray();
            },
            .map => |entries| {
                try writer.beginObject();
                for (entries) |entry| {
                    try writer.objectField(entry.name);
                    try writer.write(Value{ .value = entry.value, .include_native = self.include_native });
                }
                try writer.endObject();
            },
            .node => |node| try writer.write(.{ .identity = node.identity, .labels = node.labels, .properties = Value{ .value = .{ .map = node.properties } } }),
            .relationship => |r| {
                try writer.beginObject();
                inline for (.{ "identity", "index", "type", "source", "target", "ownership", "producer" }) |field| {
                    try writer.objectField(field);
                    try writer.write(@field(r, field));
                }
                if (self.include_native) {
                    try writer.objectField("native");
                    try writer.write(r.native);
                }
                try writer.objectField("properties");
                try writer.write(Value{ .value = .{ .map = r.properties } });
                try writer.endObject();
            },
            .path => |p| {
                try writer.beginObject();
                try writer.objectField("nodes");
                try writer.beginArray();
                for (p.nodes) |n| try writer.write(Value{ .value = .{ .node = n }, .include_native = self.include_native });
                try writer.endArray();
                try writer.objectField("relationships");
                try writer.beginArray();
                for (p.relationships) |r| try writer.write(Value{ .value = .{ .relationship = r }, .include_native = self.include_native });
                try writer.endArray();
                try writer.endObject();
            },
        }
        try writer.endObject();
    }
};

fn expectToken(source: anytype, token: std.meta.Tag(std.json.Token)) !void {
    if (std.meta.activeTag(try source.next()) != token) return error.InvalidCypherWireValue;
}

fn parseText(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) ![]const u8 {
    // std.json also accepts a byte array for []u8. Wire text is a JSON string.
    if (try source.peekNextTokenType() != .string) return error.InvalidCypherWireValue;
    return std.json.innerParse([]const u8, a, source, options);
}

fn parseTyped(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions, depth: usize) anyerror!v.Value {
    if (depth > 64) return error.CypherValueDepthExceeded;
    var structural = options;
    structural.allocate = .alloc_if_needed;
    try expectToken(source, .object_begin);
    const first = try parseText(a, source, structural);
    if (std.mem.eql(u8, first, "value")) {
        const payload = try std.json.innerParse(std.json.Value, a, source, options);
        const second = try parseText(a, source, structural);
        if (!std.mem.eql(u8, second, "type")) return error.InvalidCypherWireValue;
        const kind = try parseText(a, source, structural);
        try expectToken(source, .object_end);
        return decodePayload(a, kind, payload, depth);
    }
    if (!std.mem.eql(u8, first, "type")) return error.InvalidCypherWireValue;
    const kind = try parseText(a, source, structural);
    const second = try parseText(a, source, structural);
    if (!std.mem.eql(u8, second, "value")) return error.InvalidCypherWireValue;
    const tag = std.meta.stringToEnum(std.meta.Tag(v.Value), kind) orelse return error.InvalidCypherWireValue;
    const result: v.Value = switch (tag) {
        .null => result: {
            try expectToken(source, .null);
            break :result .null;
        },
        .boolean => .{ .boolean = try std.json.innerParse(bool, a, source, options) },
        .integer => .{ .integer = try std.fmt.parseInt(i64, try parseText(a, source, options), 10) },
        .number => result: {
            if (try source.peekNextTokenType() != .number) return error.InvalidCypherWireValue;
            const number = try std.json.innerParse(f64, a, source, options);
            if (!std.math.isFinite(number)) return error.InvalidCypherWireValue;
            break :result .{ .number = number };
        },
        .string => .{ .string = try parseText(a, source, options) },
        .list => result: {
            try expectToken(source, .array_begin);
            var items: std.ArrayList(v.Value) = .empty;
            while (try source.peekNextTokenType() != .array_end) try items.append(a, try parseTyped(a, source, options, depth + 1));
            try expectToken(source, .array_end);
            break :result .{ .list = try items.toOwnedSlice(a) };
        },
        .map => result: {
            try expectToken(source, .object_begin);
            var entries: std.ArrayList(v.Entry) = .empty;
            var names: std.StringHashMapUnmanaged(void) = .empty;
            defer names.deinit(a);
            while (try source.peekNextTokenType() != .object_end) {
                const name = try parseText(a, source, options);
                if ((try names.getOrPut(a, name)).found_existing) return error.InvalidCypherWireValue;
                try entries.append(a, .{ .name = name, .value = try parseTyped(a, source, options, depth + 1) });
            }
            try expectToken(source, .object_end);
            break :result .{ .map = try entries.toOwnedSlice(a) };
        },
        .node => result: {
            const input = try parseEntity(struct { identity: v.Identity, labels: []const []const u8, properties: v.Value }, a, source, options, depth);
            if (input.properties != .map or input.identity.table.len == 0 or input.identity.key.len == 0) return error.InvalidCypherWireValue;
            const node = try a.create(v.Node);
            node.* = .{ .identity = input.identity, .labels = input.labels, .properties = input.properties.map };
            break :result .{ .node = node };
        },
        .relationship => result: {
            const Native = struct { weight: f64, created_at: []const u8, updated_at: []const u8 };
            const input = try parseEntity(struct { identity: v.Identity, index: []const u8, type: []const u8, source: v.Identity, target: v.Identity, ownership: @FieldType(v.Relationship, "ownership"), producer: ?v.Identity = null, properties: v.Value, native: ?Native = null }, a, source, options, depth);
            if (input.properties != .map or input.identity.table.len == 0 or input.identity.key.len == 0 or input.index.len == 0 or input.type.len == 0) return error.InvalidCypherWireValue;
            const relationship = try a.create(v.Relationship);
            relationship.* = .{ .identity = input.identity, .index = input.index, .type = input.type, .source = input.source, .target = input.target, .ownership = input.ownership, .producer = input.producer, .properties = input.properties.map, .native = if (input.native) |native| .{ .weight = native.weight, .created_at = try std.fmt.parseUnsigned(u64, native.created_at, 10), .updated_at = try std.fmt.parseUnsigned(u64, native.updated_at, 10) } else .{} };
            break :result .{ .relationship = relationship };
        },
        // Paths are uncommon on the owner data plane. Retain the shared strict
        // adjacency validator instead of duplicating its contract here.
        .path => try decodePayload(a, kind, try std.json.innerParse(std.json.Value, a, source, options), depth),
    };
    try expectToken(source, .object_end);
    return result;
}

fn parseEntity(comptime T: type, a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions, depth: usize) !T {
    var structural = options;
    structural.allocate = .alloc_if_needed;
    try expectToken(source, .object_begin);
    var result: T = undefined;
    const fields = @typeInfo(T).@"struct";
    var seen: [fields.field_names.len]bool = @splat(false);
    while (try source.peekNextTokenType() != .object_end) {
        const name = try parseText(a, source, structural);
        var matched = false;
        inline for (fields.field_names, fields.field_types, 0..) |name_zig, Field, index| {
            if (std.mem.eql(u8, name, name_zig)) {
                if (seen[index]) return error.InvalidCypherWireValue;
                seen[index] = true;
                matched = true;
                @field(result, name_zig) = if (comptime std.mem.eql(u8, name_zig, "properties")) try parseTyped(a, source, options, depth + 1) else try std.json.innerParse(Field, a, source, options);
            }
        }
        if (!matched) return error.InvalidCypherWireValue;
    }
    try expectToken(source, .object_end);
    inline for (fields.field_names, fields.field_types, fields.field_attrs, 0..) |name_zig, Field, attrs, index| {
        if (!seen[index]) {
            if (attrs.defaultValue(Field)) |default| @field(result, name_zig) = default else return error.InvalidCypherWireValue;
        }
    }
    return result;
}

fn decodePayload(a: std.mem.Allocator, kind: []const u8, payload: std.json.Value, depth: usize) !v.Value {
    var envelope: std.json.ObjectMap = .empty;
    defer envelope.deinit(a);
    try envelope.put(a, "type", .{ .string = kind });
    try envelope.put(a, "value", payload);
    return decode(a, .{ .object = envelope }, depth, true);
}

/// A native metadata key alone is never a wire discriminator. Recognize only
/// the complete private envelope emitted by mutations.build; all other objects
/// remain native user metadata, including arbitrary antfly_cypher properties.
pub fn relationshipProperties(a: std.mem.Allocator, metadata: std.json.Value) !?[]const v.Entry {
    if (metadata != .object or metadata.object.count() != 3) return null;
    const source = metadata.object.get("source_table") orelse return null;
    const target = metadata.object.get("target_table") orelse return null;
    if (source != .string or target != .string) return null;
    const envelope = metadata.object.get("antfly_cypher") orelse return null;
    if (envelope != .object or envelope.object.count() != 2) return null;
    const format = envelope.object.get("version") orelse return null;
    const supported = switch (format) {
        .integer => format.integer == 1,
        .number_string => std.mem.eql(u8, format.number_string, "1"),
        else => false,
    };
    if (!supported) return null;
    const input = envelope.object.get("properties") orelse return null;
    const decoded = parameter(a, input) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    return if (decoded == .map) decoded.map else null;
}

/// Parameter decoding deliberately accepts only data values. Entity handles
/// cannot be forged by a public caller; MATCH resolves authorized identities.
pub fn parameter(a: std.mem.Allocator, input: std.json.Value) !v.Value {
    return decode(a, input, 0, false);
}
pub fn transport(a: std.mem.Allocator, input: std.json.Value) !v.Value {
    return decode(a, input, 0, true);
}
fn decode(a: std.mem.Allocator, input: std.json.Value, depth: usize, entities: bool) anyerror!v.Value {
    if (depth > 64) return error.CypherValueDepthExceeded;
    if (input != .object or input.object.count() != 2) return error.InvalidCypherWireValue;
    const kind = input.object.get("type") orelse return error.InvalidCypherWireValue;
    const value = input.object.get("value") orelse return error.InvalidCypherWireValue;
    if (kind != .string) return error.InvalidCypherWireValue;
    if (std.mem.eql(u8, kind.string, "null") and value == .null) return .null;
    if (std.mem.eql(u8, kind.string, "boolean") and value == .bool) return .{ .boolean = value.bool };
    if (std.mem.eql(u8, kind.string, "string") and value == .string) return .{ .string = try a.dupe(u8, value.string) };
    if (std.mem.eql(u8, kind.string, "integer") and value == .string) return .{ .integer = std.fmt.parseInt(i64, value.string, 10) catch return error.CypherNumericOverflow };
    if (std.mem.eql(u8, kind.string, "number")) {
        const decoded = try v.Value.fromJson(a, value);
        if (decoded != .integer and decoded != .number) return error.InvalidCypherWireValue;
        return .{ .number = try decoded.numeric() };
    }
    if (std.mem.eql(u8, kind.string, "list") and value == .array) {
        const items = try a.alloc(v.Value, value.array.items.len);
        for (value.array.items, items) |item, *out| out.* = try decode(a, item, depth + 1, entities);
        return .{ .list = items };
    }
    if (std.mem.eql(u8, kind.string, "map") and value == .object) {
        const entries = try a.alloc(v.Entry, value.object.count());
        var iterator = value.object.iterator();
        var i: usize = 0;
        while (iterator.next()) |entry| : (i += 1) entries[i] = .{ .name = try a.dupe(u8, entry.key_ptr.*), .value = try decode(a, entry.value_ptr.*, depth + 1, entities) };
        return .{ .map = entries };
    }
    if (entities and std.mem.eql(u8, kind.string, "node")) {
        const Input = struct { identity: v.Identity, labels: []const []const u8, properties: std.json.Value };
        const node = try std.json.parseFromValueLeaky(Input, a, value, .{ .allocate = .alloc_always });
        const properties = try decode(a, node.properties, depth + 1, true);
        if (properties != .map or node.identity.table.len == 0 or node.identity.key.len == 0) return error.InvalidCypherWireValue;
        const result = try a.create(v.Node);
        result.* = .{ .identity = node.identity, .labels = node.labels, .properties = properties.map };
        return .{ .node = result };
    }
    if (entities and std.mem.eql(u8, kind.string, "relationship")) {
        const Native = struct { weight: f64, created_at: []const u8, updated_at: []const u8 };
        const Input = struct { identity: v.Identity, index: []const u8, type: []const u8, source: v.Identity, target: v.Identity, ownership: @FieldType(v.Relationship, "ownership"), producer: ?v.Identity = null, properties: std.json.Value, native: ?Native = null };
        const relation = try std.json.parseFromValueLeaky(Input, a, value, .{ .allocate = .alloc_always });
        const properties = try decode(a, relation.properties, depth + 1, true);
        if (properties != .map or relation.identity.table.len == 0 or relation.identity.key.len == 0 or relation.index.len == 0 or relation.type.len == 0) return error.InvalidCypherWireValue;
        const result = try a.create(v.Relationship);
        result.* = .{ .identity = relation.identity, .index = relation.index, .type = relation.type, .source = relation.source, .target = relation.target, .ownership = relation.ownership, .producer = relation.producer, .properties = properties.map, .native = if (relation.native) |native| .{ .weight = native.weight, .created_at = std.fmt.parseUnsigned(u64, native.created_at, 10) catch return error.InvalidCypherWireValue, .updated_at = std.fmt.parseUnsigned(u64, native.updated_at, 10) catch return error.InvalidCypherWireValue } else .{} };
        return .{ .relationship = result };
    }
    if (entities and std.mem.eql(u8, kind.string, "path")) {
        const Input = struct { nodes: []const std.json.Value, relationships: []const std.json.Value };
        const path = try std.json.parseFromValueLeaky(Input, a, value, .{});
        if (path.nodes.len != path.relationships.len + 1) return error.InvalidCypherWireValue;
        const nodes = try a.alloc(*const v.Node, path.nodes.len);
        const relationships = try a.alloc(*const v.Relationship, path.relationships.len);
        for (path.nodes, nodes) |item, *out| {
            const node = try decode(a, item, depth + 1, true);
            if (node != .node) return error.InvalidCypherWireValue;
            out.* = node.node;
        }
        for (path.relationships, relationships, 0..) |item, *out, i| {
            const relationship = try decode(a, item, depth + 1, true);
            if (relationship != .relationship) return error.InvalidCypherWireValue;
            const r = relationship.relationship;
            if (!(r.source.eql(nodes[i].identity) and r.target.eql(nodes[i + 1].identity)) and !(r.target.eql(nodes[i].identity) and r.source.eql(nodes[i + 1].identity))) return error.InvalidCypherWireValue;
            out.* = r;
        }
        return .{ .path = .{ .nodes = nodes, .relationships = relationships } };
    }
    return error.InvalidCypherWireValue;
}

test "graph wire preserves signed integers and map discriminator keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original: v.Value = .{ .map = &.{ .{ .name = "type", .value = .{ .string = "node" } }, .{ .name = "$type", .value = .{ .integer = std.math.minInt(i64) } }, .{ .name = "value", .value = .{ .list = &.{.{ .integer = std.math.maxInt(i64) }} } } } };
    const json = try std.json.Stringify.valueAlloc(a, Value{ .value = original }, .{});
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{});
    try std.testing.expect(v.Value.eql(original, try parameter(a, parsed)));
    const forged = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"type\":\"node\",\"value\":{}}", .{});
    try std.testing.expectError(error.InvalidCypherWireValue, parameter(a, forged));
}

test "relationship transport and cloning retain explicit producer identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]@FieldType(v.Relationship, "ownership"){ .document, .field, .artifact }) |ownership| {
        const relationship: v.Relationship = .{ .identity = .{ .table = "docs", .key = "opaque-fact" }, .index = "links", .type = "LINK", .source = .{ .table = "docs", .key = "a" }, .target = .{ .table = "docs", .key = "b" }, .ownership = ownership, .producer = .{ .table = "docs", .key = "claim" } };
        const cloned = try (v.Value{ .relationship = &relationship }).clone(a);
        const json = try std.json.Stringify.valueAlloc(a, Value{ .value = cloned }, .{});
        const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{});
        const decoded = try transport(a, parsed);
        try std.testing.expect(decoded.relationship.producer.?.eql(relationship.producer.?));
        try std.testing.expectEqual(ownership, decoded.relationship.ownership);
        try std.testing.expect((try decoded.relationship.producingDocument(a)).?.eql(relationship.producer.?));
    }
}

test "native metadata needs a complete Cypher envelope and keeps ordinary marker keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "{\"antfly_cypher\":\"customer\"}",
        "{\"antfly_cypher\":{\"version\":1,\"properties\":{\"type\":\"map\",\"value\":{}}},\"tag\":7}",
        "{\"source_table\":\"docs\",\"target_table\":\"docs\",\"antfly_cypher\":{\"version\":2,\"properties\":{\"type\":\"map\",\"value\":{}}}}",
        "{\"source_table\":\"docs\",\"target_table\":\"docs\",\"antfly_cypher\":{\"version\":1,\"properties\":{\"type\":\"map\",\"value\":{\"bad\":{\"type\":\"node\",\"value\":{}}}}}}",
    }) |text| {
        const metadata = try std.json.parseFromSliceLeaky(std.json.Value, a, text, .{ .parse_numbers = false });
        try std.testing.expect(try relationshipProperties(a, metadata) == null);
    }
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .source_table = "docs", .target_table = "docs", .antfly_cypher = .{ .version = @as(u8, 1), .properties = Value{ .value = .{ .map = &.{.{ .name = "antfly_cypher", .value = .{ .integer = std.math.maxInt(i64) } }} } } } }, .{});
    for ([_]bool{ false, true }) |parse_numbers| {
        const metadata = try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{ .parse_numbers = parse_numbers });
        const properties = (try relationshipProperties(a, metadata)).?;
        try std.testing.expectEqualStrings("antfly_cypher", properties[0].name);
        try std.testing.expectEqual(std.math.maxInt(i64), properties[0].value.integer);
    }
}

test "private owner transport retains native attributes without exposing them in public values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const r: v.Relationship = .{ .identity = .{ .table = "docs", .key = "edge" }, .index = "links", .type = "R", .source = .{ .table = "docs", .key = "a" }, .target = .{ .table = "docs", .key = "b" }, .native = .{ .weight = 3.5, .created_at = std.math.maxInt(u64), .updated_at = 9007199254740993 } };
    for ([_]bool{ false, true }) |include_native| {
        const encoded = try std.json.Stringify.valueAlloc(a, Value{ .value = .{ .relationship = &r }, .include_native = include_native }, .{});
        const input = try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{});
        try std.testing.expectEqual(include_native, input.object.get("value").?.object.contains("native"));
        const decoded = (try transport(a, input)).relationship;
        if (include_native) {
            try std.testing.expectEqualDeep(r.native, decoded.native);
            const cloned = (try (v.Value{ .relationship = decoded }).clone(a)).relationship;
            try std.testing.expectEqualDeep(r.native, cloned.native);
        }
    }
}

test "typed owner decoder matches version one values and rejects malformed envelopes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n: v.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"D"}, .properties = &.{.{ .name = "type", .value = .{ .list = &.{ .null, .{ .integer = std.math.minInt(i64) }, .{ .string = "escaped\nvalue" } } } }} };
    const r: v.Relationship = .{ .identity = .{ .table = "docs", .key = "edge" }, .index = "links", .type = "LINK", .source = n.identity, .target = n.identity, .ownership = .document, .producer = n.identity, .native = .{ .weight = 3.5, .created_at = std.math.maxInt(u64), .updated_at = 9007199254740993 } };
    for ([_]v.Value{ .null, .{ .boolean = true }, .{ .number = 1.5 }, .{ .node = &n }, .{ .relationship = &r }, .{ .path = .{ .nodes = &.{ &n, &n }, .relationships = &.{&r} } } }) |value| {
        const encoded = try std.json.Stringify.valueAlloc(a, Value{ .value = value, .include_native = true }, .{});
        const direct = try std.json.parseFromSliceLeaky(Value, a, encoded, .{ .allocate = .alloc_always });
        const dom = try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{});
        const canonical = try transport(a, dom);
        try std.testing.expect(v.Value.eql(canonical, direct.value));
        if (value == .relationship) try std.testing.expectEqualDeep(value.relationship.native, direct.value.relationship.native);
    }
    const reversed = try std.json.parseFromSliceLeaky(Value, a, "{\"value\":\"9223372036854775807\",\"type\":\"integer\"}", .{});
    try std.testing.expectEqual(std.math.maxInt(i64), reversed.value.integer);
    for ([_][]const u8{
        "{\"type\":\"number\",\"value\":\"1.0\"}",
        "{\"type\":\"string\",\"value\":[97]}",
        "{\"type\":\"integer\",\"value\":[49]}",
        "{\"type\":[110,117,108,108],\"value\":null}",
        "{\"type\":\"null\",\"value\":null,\"extra\":1}",
        "{\"type\":\"map\",\"value\":{\"a\":{\"type\":\"null\",\"value\":null},\"a\":{\"type\":\"null\",\"value\":null}}}",
        "{\"type\":\"node\",\"value\":{\"identity\":{\"table\":\"docs\",\"key\":\"\"},\"labels\":[],\"properties\":{\"type\":\"map\",\"value\":{}}}}",
        "{\"type\":\"integer\",\"value\":\"9223372036854775808\"}",
    }) |encoded| try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSliceLeaky(Value, a, encoded, .{}));
    var nested = try a.dupe(u8, "{\"type\":\"null\",\"value\":null}");
    for (0..65) |_| nested = try std.fmt.allocPrint(a, "{{\"type\":\"list\",\"value\":[{s}]}}", .{nested});
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSliceLeaky(Value, a, nested, .{}));
}
