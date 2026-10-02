// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Relationship predicates run before neighbor admission and path ranking.
//! Metadata is a projection of the authoritative fact, not a late document join.
const std = @import("std");
const schema = @import("../storage/schema.zig");
const json_number = @import("../common/json_number.zig");
const Allocator = std.mem.Allocator;

pub const Operator = enum { eq, ne, lt, lte, gt, gte, is_null, is_not_null };
pub const ValueType = enum { scalar, datetime };
pub const Predicate = struct {
    field: []const u8,
    op: Operator,
    value_json: []const u8 = "",
    value_type: ValueType = .scalar,
};

pub const Filter = struct {
    properties: []const Predicate = &.{},
    valid_at_ns: ?i128 = null,
    known_at_ns: ?i128 = null,

    pub fn active(self: Filter) bool {
        return self.properties.len > 0 or self.valid_at_ns != null or self.known_at_ns != null;
    }

    pub fn deinit(self: Filter, alloc: Allocator) void {
        for (self.properties) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json.len > 0) alloc.free(predicate.value_json);
        }
        if (self.properties.len > 0) alloc.free(self.properties);
    }

    pub fn clone(self: Filter, alloc: Allocator) !Filter {
        const properties = try alloc.alloc(Predicate, self.properties.len);
        var count: usize = 0;
        errdefer {
            for (properties[0..count]) |p| {
                alloc.free(p.field);
                if (p.value_json.len > 0) alloc.free(p.value_json);
            }
            alloc.free(properties);
        }
        for (self.properties, 0..) |p, i| {
            const field = try alloc.dupe(u8, p.field);
            errdefer alloc.free(field);
            properties[i] = .{ .field = field, .op = p.op, .value_json = if (p.value_json.len > 0) try alloc.dupe(u8, p.value_json) else "", .value_type = p.value_type };
            count += 1;
        }
        return .{ .properties = properties, .valid_at_ns = self.valid_at_ns, .known_at_ns = self.known_at_ns };
    }

    pub fn validate(self: Filter, alloc: Allocator) !void {
        if (self.properties.len > 64) return error.InvalidRelationshipFilter;
        var bytes: usize = 0;
        for (self.properties) |p| {
            bytes +|= p.field.len +| p.value_json.len;
            if (bytes > 65536 or !validField(p.field)) return error.InvalidRelationshipFilter;
            if (p.op == .is_null or p.op == .is_not_null) {
                if (p.value_json.len > 0 or p.value_type != .scalar) return error.InvalidRelationshipFilter;
                continue;
            }
            if (p.value_json.len == 0) return error.InvalidRelationshipFilter;
            var value = std.json.parseFromSlice(std.json.Value, alloc, p.value_json, .{ .parse_numbers = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRelationshipFilter;
            defer value.deinit();
            if (!scalar(value.value)) return error.InvalidRelationshipFilter;
            if (p.value_type == .datetime and (value.value != .string or schema.parseRfc3339ToSignedNs(value.value.string) == null)) return error.InvalidRelationshipFilter;
            if ((p.op == .lt or p.op == .lte or p.op == .gt or p.op == .gte) and value.value == .bool) return error.InvalidRelationshipFilter;
        }
    }

    pub fn matches(self: Filter, alloc: Allocator, edge: anytype) !bool {
        if (!self.active()) return true;
        var metadata = std.json.parseFromSlice(std.json.Value, alloc, if (edge.metadata.len > 0) edge.metadata else "{}", .{ .parse_numbers = false }) catch |err| return if (err == error.OutOfMemory) err else false;
        defer metadata.deinit();
        if (self.valid_at_ns) |at| if (!intervalContains(metadata.value, "valid_at", "invalid_at", at, false)) return false;
        if (self.known_at_ns) |at| if (!intervalContains(metadata.value, "created_at", "expired_at", at, true)) return false;
        for (self.properties) |p| {
            var timestamp_buffer: [32]u8 = undefined;
            const actual = try edgeValue(alloc, edge, metadata.value, p.field, &timestamp_buffer);
            const nullish = actual == null or actual.? == .null;
            if (p.op == .is_null) {
                if (!nullish) return false;
                continue;
            }
            if (p.op == .is_not_null) {
                if (nullish) return false;
                continue;
            }
            // Cypher comparisons with absent/null properties do not pass WHERE,
            // including !=. Null tests must be requested explicitly.
            if (nullish) return false;
            var expected = try std.json.parseFromSlice(std.json.Value, alloc, p.value_json, .{ .parse_numbers = false });
            defer expected.deinit();
            const order = compare(actual.?, expected.value, p.value_type) orelse return false;
            const passes = switch (p.op) {
                .eq => order == .eq,
                .ne => order != .eq,
                .lt => order == .lt,
                .lte => order != .gt,
                .gt => order == .gt,
                .gte => order != .lt,
                else => unreachable,
            };
            if (!passes) return false;
        }
        return true;
    }
};

fn scalar(value: std.json.Value) bool {
    return switch (value) {
        .string, .integer, .bool => true,
        .float => |n| std.math.isFinite(n),
        .number_string => |n| json_number.Number.parse(n) != null,
        else => false,
    };
}

fn validField(field: []const u8) bool {
    for ([_][]const u8{ "/edge_id", "/owner_document", "/source", "/target", "/type", "/weight", "/created_at", "/updated_at" }) |name| if (std.mem.eql(u8, field, name)) return true;
    if (!std.mem.startsWith(u8, field, "/metadata/")) return false;
    var i: usize = 0;
    while (i < field.len) : (i += 1) if (field[i] == '~') {
        i += 1;
        if (i >= field.len or (field[i] != '0' and field[i] != '1')) return false;
    };
    return true;
}

fn edgeValue(alloc: Allocator, edge: anytype, metadata: std.json.Value, field: []const u8, timestamp_buffer: *[32]u8) !?std.json.Value {
    if (std.mem.eql(u8, field, "/edge_id")) return if (edge.edge_id.len > 0) .{ .string = edge.edge_id } else null;
    if (std.mem.eql(u8, field, "/owner_document")) return .{ .string = if (edge.owner_document.len > 0) edge.owner_document else edge.source };
    if (std.mem.eql(u8, field, "/source")) return .{ .string = edge.source };
    if (std.mem.eql(u8, field, "/target")) return .{ .string = edge.target };
    if (std.mem.eql(u8, field, "/type")) return .{ .string = edge.edge_type };
    if (std.mem.eql(u8, field, "/weight")) return .{ .float = edge.weight };
    if (std.mem.eql(u8, field, "/created_at")) return .{ .number_string = try std.fmt.bufPrint(timestamp_buffer, "{d}", .{edge.created_at}) };
    if (std.mem.eql(u8, field, "/updated_at")) return .{ .number_string = try std.fmt.bufPrint(timestamp_buffer, "{d}", .{edge.updated_at}) };
    if (!std.mem.startsWith(u8, field, "/metadata/")) return null;
    var current = metadata;
    var components = std.mem.splitScalar(u8, field[10..], '/');
    while (components.next()) |component| {
        var key = std.ArrayListUnmanaged(u8).empty;
        defer key.deinit(alloc);
        var i: usize = 0;
        while (i < component.len) : (i += 1) {
            var ch = component[i];
            if (ch == '~') {
                i += 1;
                if (i >= component.len) return error.InvalidRelationshipFilter;
                ch = switch (component[i]) {
                    '0' => '~',
                    '1' => '/',
                    else => return error.InvalidRelationshipFilter,
                };
            }
            try key.append(alloc, ch);
        }
        current = switch (current) {
            .object => |object| object.get(key.items) orelse return null,
            .array => |array| blk: {
                const index = std.fmt.parseUnsigned(usize, key.items, 10) catch return null;
                if (index >= array.items.len) return null;
                break :blk array.items[index];
            },
            else => return null,
        };
    }
    return current;
}

fn compare(actual: std.json.Value, expected: std.json.Value, value_type: ValueType) ?std.math.Order {
    if (value_type == .datetime) {
        if (actual != .string or expected != .string) return null;
        const a = schema.parseRfc3339ToSignedNs(actual.string) orelse return null;
        const b = schema.parseRfc3339ToSignedNs(expected.string) orelse return null;
        return std.math.order(a, b);
    }
    var actual_buffer: [64]u8 = undefined;
    var expected_buffer: [64]u8 = undefined;
    if (json_number.fromValue(actual, &actual_buffer)) |a| {
        const b = json_number.fromValue(expected, &expected_buffer) orelse return null;
        return a.order(b);
    }
    if (actual == .string and expected == .string) return std.mem.order(u8, actual.string, expected.string);
    if (actual == .bool and expected == .bool) return std.math.order(@intFromBool(actual.bool), @intFromBool(expected.bool));
    return null;
}

fn intervalContains(metadata: std.json.Value, lower: []const u8, upper: []const u8, at: i128, require_lower: bool) bool {
    if (metadata != .object) return false;
    const begin = metadata.object.get(lower) orelse .null;
    const end = metadata.object.get(upper) orelse .null;
    if (begin == .null and require_lower) return false;
    if (begin != .null) {
        if (begin != .string) return false;
        const instant = schema.parseRfc3339ToSignedNs(begin.string) orelse return false;
        if (at < instant) return false;
    }
    if (end != .null) {
        if (end != .string) return false;
        const instant = schema.parseRfc3339ToSignedNs(end.string) orelse return false;
        if (at >= instant) return false;
    }
    return true;
}

pub fn parsePublicAlloc(alloc: Allocator, value: anytype) !Filter {
    const raw = try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRelationshipFilter;
    const root = parsed.value.object;
    var result = Filter{};
    if (root.get("valid_at")) |at| {
        if (at != .string) return error.InvalidRelationshipFilter;
        result.valid_at_ns = schema.parseRfc3339ToSignedNs(at.string) orelse return error.InvalidRelationshipFilter;
    }
    if (root.get("known_at")) |at| {
        if (at != .string) return error.InvalidRelationshipFilter;
        result.known_at_ns = schema.parseRfc3339ToSignedNs(at.string) orelse return error.InvalidRelationshipFilter;
    }
    if (root.get("properties")) |properties| {
        if (properties != .array or properties.array.items.len > 64) return error.InvalidRelationshipFilter;
        var list = std.ArrayListUnmanaged(Predicate).empty;
        errdefer {
            for (list.items) |p| {
                alloc.free(p.field);
                if (p.value_json.len > 0) alloc.free(p.value_json);
            }
            list.deinit(alloc);
        }
        for (properties.array.items) |p| {
            if (p != .object) return error.InvalidRelationshipFilter;
            const field = p.object.get("field") orelse return error.InvalidRelationshipFilter;
            const op = p.object.get("op") orelse return error.InvalidRelationshipFilter;
            if (field != .string or op != .string) return error.InvalidRelationshipFilter;
            const operator = std.meta.stringToEnum(Operator, op.string) orelse return error.InvalidRelationshipFilter;
            const value_type: ValueType = if (p.object.get("value_type")) |t| blk: {
                if (t != .string) return error.InvalidRelationshipFilter;
                break :blk std.meta.stringToEnum(ValueType, t.string) orelse return error.InvalidRelationshipFilter;
            } else .scalar;
            const owned_field = try alloc.dupe(u8, field.string);
            errdefer alloc.free(owned_field);
            const literal = if (p.object.get("value")) |literal| try std.json.Stringify.valueAlloc(alloc, literal, .{}) else "";
            errdefer if (literal.len > 0) alloc.free(literal);
            try list.append(alloc, .{ .field = owned_field, .op = operator, .value_json = literal, .value_type = value_type });
        }
        result.properties = try list.toOwnedSlice(alloc);
    }
    errdefer result.deinit(alloc);
    try result.validate(alloc);
    if (!result.active()) return error.InvalidRelationshipFilter;
    return result;
}

test "relationship predicates enforce bitemporal intervals and Cypher null semantics" {
    const alloc = std.testing.allocator;
    const Edge = struct { source: []const u8 = "Alice", target: []const u8 = "Acme", edge_type: []const u8 = "RELATES_TO", edge_id: []const u8 = "fact", owner_document: []const u8 = "fact", weight: f64 = 0.5, created_at: u64 = 0, updated_at: u64 = 0, metadata: []const u8 };
    const edge = Edge{ .metadata =
        \\{"valid_at":"2020-01-01T01:00:00+01:00","invalid_at":"2021-01-01T00:00:00Z","created_at":"2020-02-01T00:00:00Z","expired_at":null,"tenant":"g","a/b":{"~x":[2]},"nullable":null}
    };
    const valid = Filter{ .valid_at_ns = schema.parseRfc3339ToSignedNs("2020-01-01T00:00:00Z") };
    try std.testing.expect(try valid.matches(alloc, edge));
    const invalid = Filter{ .valid_at_ns = schema.parseRfc3339ToSignedNs("2021-01-01T00:00:00Z") };
    try std.testing.expect(!try invalid.matches(alloc, edge));
    const unknown = Filter{ .known_at_ns = schema.parseRfc3339ToSignedNs("2020-01-15T00:00:00Z") };
    try std.testing.expect(!try unknown.matches(alloc, edge));
    const known = Filter{ .known_at_ns = schema.parseRfc3339ToSignedNs("2020-02-01T00:00:00Z") };
    try std.testing.expect(try known.matches(alloc, edge));
    for ([_][]const u8{ "/metadata/missing", "/metadata/nullable" }) |field| {
        try std.testing.expect(!try (Filter{ .properties = &.{.{ .field = field, .op = .ne, .value_json = "1" }} }).matches(alloc, edge));
        try std.testing.expect(try (Filter{ .properties = &.{.{ .field = field, .op = .is_null }} }).matches(alloc, edge));
    }
    try std.testing.expect(try (Filter{ .properties = &.{.{ .field = "/metadata/a~1b/~0x/0", .op = .gte, .value_json = "2" }} }).matches(alloc, edge));
    try std.testing.expect(try (Filter{ .properties = &.{.{ .field = "/metadata/valid_at", .op = .eq, .value_json = "\"2020-01-01T00:00:00Z\"", .value_type = .datetime }} }).matches(alloc, edge));
    try std.testing.expect(!try known.matches(alloc, Edge{ .metadata = "{}" }));
    try std.testing.expect(!try valid.matches(alloc, Edge{ .metadata = "{\"valid_at\":42}" }));
    try std.testing.expect(schema.parseRfc3339ToSignedNs("1969-12-31T23:59:59.999999999Z").? == -1);
    try std.testing.expect(schema.parseRfc3339ToNs("1969-12-31T23:59:59Z") == null);
}

test "relationship predicates reject invalid query values and clone ownership" {
    const alloc = std.testing.allocator;
    var filter = try parsePublicAlloc(alloc, .{ .properties = .{.{ .field = "/metadata/tenant", .op = "eq", .value = "g" }}, .valid_at = "2020-01-01T00:00:00Z" });
    defer filter.deinit(alloc);
    var copy = try filter.clone(alloc);
    defer copy.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/tenant", copy.properties[0].field);
    try std.testing.expectEqual(filter.valid_at_ns, copy.valid_at_ns);
    try std.testing.expectError(error.InvalidRelationshipFilter, parsePublicAlloc(alloc, .{ .properties = .{.{ .field = "/metadata/x", .op = "eq", .value = @as(?u8, null) }} }));
    try std.testing.expectError(error.InvalidRelationshipFilter, parsePublicAlloc(alloc, .{ .properties = .{.{ .field = "/metadata/x", .op = "is_null", .value = 1 }} }));
    try std.testing.expectError(error.InvalidRelationshipFilter, parsePublicAlloc(alloc, .{ .properties = .{.{ .field = "/metadata/x~2", .op = "eq", .value = 1 }} }));
}

test "relationship predicate roots reject every nonobject JSON type" {
    for ([_][]const u8{ "null", "true", "42", "\"x\"", "[]" }) |json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidRelationshipFilter, parsePublicAlloc(std.testing.allocator, parsed.value));
    }
}

test "relationship predicates preserve exact decimal literals" {
    const alloc = std.testing.allocator;
    const Edge = struct { source: []const u8 = "a", target: []const u8 = "b", edge_type: []const u8 = "R", edge_id: []const u8 = "fact", owner_document: []const u8 = "fact", weight: f64 = 1, created_at: u64 = 0, updated_at: u64 = 0, metadata: []const u8 };
    for ([_][]const u8{ "eq", "ne", "gt", "gte", "lt", "lte" }) |op| {
        const raw = try std.fmt.allocPrint(alloc, "{{\"properties\":[{{\"field\":\"/metadata/value\",\"op\":\"{s}\",\"value\":1.0000000000000001}}]}}", .{op});
        defer alloc.free(raw);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .parse_numbers = false });
        defer parsed.deinit();
        const filter = try parsePublicAlloc(alloc, parsed.value);
        defer filter.deinit(alloc);
        try std.testing.expectEqualStrings("1.0000000000000001", filter.properties[0].value_json);
        const equal_passes = std.mem.eql(u8, op, "eq") or std.mem.eql(u8, op, "gte") or std.mem.eql(u8, op, "lte");
        try std.testing.expectEqual(equal_passes, try filter.matches(alloc, Edge{ .metadata = "{\"value\":1.0000000000000001}" }));
        const lower_passes = std.mem.eql(u8, op, "ne") or std.mem.eql(u8, op, "lt") or std.mem.eql(u8, op, "lte");
        try std.testing.expectEqual(lower_passes, try filter.matches(alloc, Edge{ .metadata = "{\"value\":1}" }));
    }
}

test "relationship predicates normalize stored floating weights" {
    const alloc = std.testing.allocator;
    const Edge = struct { source: []const u8 = "a", target: []const u8 = "b", edge_type: []const u8 = "R", edge_id: []const u8 = "", owner_document: []const u8 = "", weight: f64 = 0.1, created_at: u64 = 0, updated_at: u64 = 0, metadata: []const u8 = "{}" };
    for ([_]Operator{ .eq, .ne, .lt, .lte, .gt, .gte }) |op| {
        const filter = Filter{ .properties = &.{.{ .field = "/weight", .op = op, .value_json = "0.1" }} };
        try filter.validate(alloc);
        try std.testing.expectEqual(op == .eq or op == .lte or op == .gte, try filter.matches(alloc, Edge{}));
    }
    const precise = Filter{ .properties = &.{.{ .field = "/weight", .op = .eq, .value_json = "1.5000000000000001" }} };
    try std.testing.expect(!try precise.matches(alloc, Edge{ .weight = 1.5 }));
}

test "relationship predicates distinguish arbitrary precision metadata" {
    const alloc = std.testing.allocator;
    const Edge = struct { source: []const u8 = "a", target: []const u8 = "b", edge_type: []const u8 = "R", edge_id: []const u8 = "", owner_document: []const u8 = "", weight: f64 = 1, created_at: u64 = 0, updated_at: u64 = 0, metadata: []const u8 = "{\"value\":1}" };
    for ([_]Operator{ .eq, .ne, .lt, .lte, .gt, .gte }) |op| {
        const filter = Filter{ .properties = &.{.{ .field = "/metadata/value", .op = op, .value_json = "1.00000000000000000000000000000000001" }} };
        try filter.validate(alloc);
        try std.testing.expectEqual(op == .ne or op == .lt or op == .lte, try filter.matches(alloc, Edge{}));
    }
    const enormous = Filter{ .properties = &.{.{ .field = "/metadata/value", .op = .eq, .value_json = "10e999999999999999999999999999999999999" }} };
    try enormous.validate(alloc);
    try std.testing.expect(try enormous.matches(alloc, Edge{ .metadata = "{\"value\":1e1000000000000000000000000000000000000}" }));
}
