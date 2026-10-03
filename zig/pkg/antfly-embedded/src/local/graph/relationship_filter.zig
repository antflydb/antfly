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
const work_budget = @import("work_budget.zig");

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
    /// Execution-only cache. Wire hooks expose only the durable filter syntax.
    prepared: ?*Prepared = null,

    const Wire = struct {
        properties: []const Predicate = &.{},
        valid_at_ns: ?i128 = null,
        known_at_ns: ?i128 = null,
    };

    pub fn jsonStringify(self: Filter, writer: anytype) !void {
        try writer.write(Wire{ .properties = self.properties, .valid_at_ns = self.valid_at_ns, .known_at_ns = self.known_at_ns });
    }

    pub fn jsonParse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) !Filter {
        const wire = try std.json.innerParse(Wire, alloc, source, options);
        return .{ .properties = wire.properties, .valid_at_ns = wire.valid_at_ns, .known_at_ns = wire.known_at_ns };
    }

    pub fn jsonParseFromValue(alloc: Allocator, source: std.json.Value, options: std.json.ParseOptions) !Filter {
        const wire = try std.json.innerParseFromValue(Wire, alloc, source, options);
        return .{ .properties = wire.properties, .valid_at_ns = wire.valid_at_ns, .known_at_ns = wire.known_at_ns };
    }

    /// Borrow an existing immutable cache or own a newly prepared one. Syntax
    /// stays borrowed; callers release only a cache they created themselves.
    pub fn prepare(self: Filter, alloc: Allocator) !Filter {
        if (!self.active() or self.prepared != null) return self;
        try self.validate(alloc);
        const prepared = try alloc.create(Prepared);
        errdefer alloc.destroy(prepared);
        prepared.* = .{ .arena = std.heap.ArenaAllocator.init(alloc) };
        errdefer prepared.arena.deinit();
        const a = prepared.arena.allocator();
        var nodes = std.ArrayListUnmanaged(ProjectionNode).empty;
        try nodes.append(a, .{});
        prepared.properties = try a.alloc(PreparedPredicate, self.properties.len);
        for (self.properties, prepared.properties) |p, *out| {
            const path = if (std.mem.startsWith(u8, p.field, "/metadata/")) try decodePointer(a, p.field[10..]) else &.{};
            out.* = .{ .syntax = p, .path = path, .expected = if (p.value_json.len > 0) try std.json.parseFromSliceLeaky(std.json.Value, a, p.value_json, .{ .parse_numbers = false, .allocate = .alloc_always }) else .null };
            out.expected_number = if (out.expected == .number_string) json_number.Number.parse(out.expected.number_string) else null;
            out.expected_time = if (p.value_type == .datetime) schema.parseRfc3339ToSignedNs(out.expected.string) else null;
            if (path.len > 0) out.metadata_slot = try internPath(a, &nodes, &prepared.slot_count, path, p.op != .is_null and p.op != .is_not_null);
        }
        if (self.valid_at_ns != null) prepared.valid_interval = .{
            .lower = try internPath(a, &nodes, &prepared.slot_count, &.{"valid_at"}, true),
            .upper = try internPath(a, &nodes, &prepared.slot_count, &.{"invalid_at"}, true),
        };
        if (self.known_at_ns != null) prepared.known_interval = .{
            .lower = try internPath(a, &nodes, &prepared.slot_count, &.{"created_at"}, true),
            .upper = try internPath(a, &nodes, &prepared.slot_count, &.{"expired_at"}, true),
        };
        prepared.nodes = try nodes.toOwnedSlice(a);
        var result = self;
        result.prepared = prepared;
        return result;
    }

    pub fn retainedBytes(self: Filter) usize {
        return if (self.prepared) |prepared| @sizeOf(Prepared) + prepared.arena.queryCapacity() else 0;
    }

    pub fn releasePrepared(self: Filter, alloc: Allocator) void {
        if (self.prepared) |prepared| {
            prepared.arena.deinit();
            alloc.destroy(prepared);
        }
    }

    pub fn active(self: Filter) bool {
        return self.properties.len > 0 or self.valid_at_ns != null or self.known_at_ns != null;
    }

    pub fn deinit(self: Filter, alloc: Allocator) void {
        self.releasePrepared(alloc);
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
        return try (Filter{ .properties = properties, .valid_at_ns = self.valid_at_ns, .known_at_ns = self.known_at_ns }).prepare(alloc);
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
        return self.matchesWithBudget(alloc, edge, null);
    }

    pub fn matchesWithBudget(self: Filter, alloc: Allocator, edge: anytype, budget: ?*work_budget.WorkBudget) !bool {
        var retained = work_budget.RetainedAllocator{ .backing = alloc, .budget = budget };
        return self.matchesImpl(retained.allocator(), edge) catch |err| {
            if (retained.denied) return error.GraphWorkBudgetExceeded;
            return err;
        };
    }

    fn matchesImpl(self: Filter, alloc: Allocator, edge: anytype) !bool {
        if (!self.active()) return true;
        const filter = try self.prepare(alloc);
        defer if (self.prepared == null) filter.releasePrepared(alloc);
        const prepared = filter.prepared.?;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        var values: [68]?std.json.Value = @splat(null);
        if (prepared.slot_count > 0) {
            const metadata = if (edge.metadata.len > 0) edge.metadata else "{}";
            projectMetadata(scratch.allocator(), metadata, prepared, &values) catch |err|
                return if (err == error.OutOfMemory) err else false;
        }
        if (prepared.valid_interval) |interval| if (!intervalContainsValues(values[interval.lower], values[interval.upper], self.valid_at_ns.?, false)) return false;
        if (prepared.known_interval) |interval| if (!intervalContainsValues(values[interval.lower], values[interval.upper], self.known_at_ns.?, true)) return false;
        for (prepared.properties) |p| {
            var timestamp_buffer: [32]u8 = undefined;
            const actual = if (p.metadata_slot) |slot| values[slot] else try edgeValue(edge, p.syntax.field, &timestamp_buffer);
            const nullish = actual == null or actual.? == .null;
            if (p.syntax.op == .is_null) {
                if (!nullish) return false;
                continue;
            }
            if (p.syntax.op == .is_not_null) {
                if (nullish) return false;
                continue;
            }
            if (nullish) return false;
            const order = comparePrepared(actual.?, p) orelse return false;
            const passes = switch (p.syntax.op) {
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
    if (std.mem.count(u8, field[10..], "/") >= 256) return false;
    var i: usize = 0;
    while (i < field.len) : (i += 1) if (field[i] == '~') {
        i += 1;
        if (i >= field.len or (field[i] != '0' and field[i] != '1')) return false;
    };
    return true;
}

fn edgeValue(edge: anytype, field: []const u8, timestamp_buffer: *[32]u8) !?std.json.Value {
    if (std.mem.eql(u8, field, "/edge_id")) return if (edge.edge_id.len > 0) .{ .string = edge.edge_id } else null;
    if (std.mem.eql(u8, field, "/owner_document")) return .{ .string = if (edge.owner_document.len > 0) edge.owner_document else edge.source };
    if (std.mem.eql(u8, field, "/source")) return .{ .string = edge.source };
    if (std.mem.eql(u8, field, "/target")) return .{ .string = edge.target };
    if (std.mem.eql(u8, field, "/type")) return .{ .string = edge.edge_type };
    if (std.mem.eql(u8, field, "/weight")) return .{ .float = edge.weight };
    if (std.mem.eql(u8, field, "/created_at")) return .{ .number_string = try std.fmt.bufPrint(timestamp_buffer, "{d}", .{edge.created_at}) };
    if (std.mem.eql(u8, field, "/updated_at")) return .{ .number_string = try std.fmt.bufPrint(timestamp_buffer, "{d}", .{edge.updated_at}) };
    return null;
}

fn comparePrepared(actual: std.json.Value, predicate: PreparedPredicate) ?std.math.Order {
    if (predicate.expected_time) |time| {
        if (actual != .string) return null;
        return std.math.order(schema.parseRfc3339ToSignedNs(actual.string) orelse return null, time);
    }
    if (predicate.expected_number) |expected| {
        var buffer: [64]u8 = undefined;
        const number = json_number.fromValue(actual, &buffer) orelse return null;
        return number.order(expected);
    }
    const expected = predicate.expected;
    if (actual == .string and expected == .string) return std.mem.order(u8, actual.string, expected.string);
    if (actual == .bool and expected == .bool) return std.math.order(@intFromBool(actual.bool), @intFromBool(expected.bool));
    return null;
}

const PreparedPredicate = struct {
    syntax: Predicate,
    path: []const []const u8,
    metadata_slot: ?usize = null,
    expected: std.json.Value,
    expected_number: ?json_number.Number = null,
    expected_time: ?i128 = null,
};
const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    properties: []PreparedPredicate = &.{},
    nodes: []ProjectionNode = &.{},
    slot_count: usize = 0,
    valid_interval: ?IntervalSlots = null,
    known_interval: ?IntervalSlots = null,
};

fn decodePointer(alloc: Allocator, text: []const u8) ![]const []const u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    var components = std.mem.splitScalar(u8, text, '/');
    while (components.next()) |component| {
        if (parts.items.len == 256) return error.InvalidRelationshipFilter;
        var key = std.ArrayListUnmanaged(u8).empty;
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
        try parts.append(alloc, try key.toOwnedSlice(alloc));
    }
    return try parts.toOwnedSlice(alloc);
}

const IntervalSlots = struct { lower: usize, upper: usize };
const ProjectionNode = struct {
    children: std.StringHashMapUnmanaged(usize) = .empty,
    slot: ?usize = null,
    needs_value: bool = false,
};

fn internPath(alloc: Allocator, nodes: *std.ArrayListUnmanaged(ProjectionNode), slots: *usize, path: []const []const u8, needs_value: bool) !usize {
    var current: usize = 0;
    for (path) |key| {
        const entry = try nodes.items[current].children.getOrPut(alloc, key);
        if (!entry.found_existing) {
            entry.value_ptr.* = nodes.items.len;
            try nodes.append(alloc, .{});
        }
        current = entry.value_ptr.*;
    }
    if (nodes.items[current].slot == null) {
        nodes.items[current].slot = slots.*;
        slots.* += 1;
    }
    nodes.items[current].needs_value = nodes.items[current].needs_value or needs_value;
    return nodes.items[current].slot.?;
}

/// One scan projects every selected scalar. Unrelated containers are skipped,
/// never materialized. Every selected pointer node can appear only once;
/// duplicate keys along a selected path fail closed as ambiguous.
fn projectMetadata(alloc: Allocator, raw: []const u8, prepared: *const Prepared, values: *[68]?std.json.Value) !void {
    var scanner = std.json.Scanner.initCompleteInput(alloc, raw);
    defer scanner.deinit();
    if ((prepared.valid_interval != null or prepared.known_interval != null) and try scanner.peekNextTokenType() != .object_begin) return error.InvalidRelationshipFilter;
    const seen = try alloc.alloc(bool, prepared.nodes.len);
    @memset(seen, false);
    try projectNode(alloc, &scanner, prepared.nodes, 0, values, seen, raw.len);
    if (try scanner.next() != .end_of_document) return error.InvalidRelationshipFilter;
}

fn projectNode(alloc: Allocator, scanner: *std.json.Scanner, nodes: []const ProjectionNode, index: usize, values: *[68]?std.json.Value, seen: []bool, max_value_len: usize) anyerror!void {
    if (seen[index]) return error.DuplicateField;
    seen[index] = true;
    const node = nodes[index];
    const kind = try scanner.peekNextTokenType();
    if (kind != .object_begin and kind != .array_begin) {
        if (node.slot != null and !node.needs_value) {
            values[node.slot.?] = if (kind == .null) .null else .{ .bool = true };
            try scanner.skipValue();
            return;
        }
        if (node.slot) |slot| values[slot] = switch (try scanner.nextAllocMax(alloc, .alloc_if_needed, max_value_len)) {
            .string, .allocated_string => |v| .{ .string = v },
            .number, .allocated_number => |v| .{ .number_string = v },
            .true => .{ .bool = true },
            .false => .{ .bool = false },
            .null => .null,
            else => return error.InvalidRelationshipFilter,
        } else try scanner.skipValue();
        return;
    }
    if (node.slot) |slot| values[slot] = if (kind == .object_begin) .{ .object = .empty } else .{ .array = std.json.Array.init(alloc) };
    if (node.children.count() == 0) return scanner.skipValue();
    _ = try scanner.next();
    if (kind == .object_begin) {
        while (try scanner.peekNextTokenType() != .object_end) {
            const token = try scanner.nextAllocMax(alloc, .alloc_if_needed, max_value_len);
            const key = switch (token) {
                .string, .allocated_string => |v| v,
                else => return error.InvalidRelationshipFilter,
            };
            defer if (token == .allocated_string) alloc.free(token.allocated_string);
            if (node.children.get(key)) |child| try projectNode(alloc, scanner, nodes, child, values, seen, max_value_len) else try scanner.skipValue();
        }
    } else {
        var ordinal: usize = 0;
        while (try scanner.peekNextTokenType() != .array_end) : (ordinal += 1) {
            var buffer: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&buffer, "{d}", .{ordinal});
            if (node.children.get(key)) |child| try projectNode(alloc, scanner, nodes, child, values, seen, max_value_len) else try scanner.skipValue();
        }
    }
    _ = try scanner.next();
}

fn intervalContainsValues(begin_value: ?std.json.Value, end_value: ?std.json.Value, at: i128, require_lower: bool) bool {
    const begin = begin_value orelse .null;
    const end = end_value orelse .null;
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
    return try result.prepare(alloc);
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

const RegressionEdge = struct {
    source: []const u8 = "a",
    target: []const u8 = "b",
    edge_type: []const u8 = "R",
    edge_id: []const u8 = "",
    owner_document: []const u8 = "",
    weight: f64 = 0.1,
    created_at: u64 = 0,
    updated_at: u64 = 0,
    metadata: []const u8 = "{}",
};

test "relationship predicates prepared intrinsic filters allocate no per-edge state" {
    const alloc = std.testing.allocator;
    const raw = Filter{ .properties = &.{.{ .field = "/weight", .op = .eq, .value_json = "0.1" }} };
    const filter = try raw.prepare(alloc);
    defer filter.releasePrepared(alloc);
    var counter = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    for (0..100) |_| try std.testing.expect(try filter.matches(counter.allocator(), RegressionEdge{ .metadata = "legacy malformed metadata" }));
    try std.testing.expectEqual(@as(usize, 0), counter.allocations);
    const wire = try std.json.Stringify.valueAlloc(alloc, filter, .{});
    defer alloc.free(wire);
    try std.testing.expect(std.mem.indexOf(u8, wire, "prepared") == null);
    var parsed = try std.json.parseFromSlice(Filter, alloc, wire, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.prepared == null);
    try std.testing.expect(try parsed.value.matches(alloc, RegressionEdge{}));
}

test "relationship predicates scan unused arrays without materializing them" {
    const alloc = std.testing.allocator;
    const raw = Filter{ .properties = &.{.{ .field = "/metadata/value", .op = .eq, .value_json = "1.00000000000000000000000000000000001" }} };
    const filter = try raw.prepare(alloc);
    defer filter.releasePrepared(alloc);
    var metadata = std.ArrayListUnmanaged(u8).empty;
    defer metadata.deinit(alloc);
    try metadata.appendSlice(alloc, "{\"value\":1.00000000000000000000000000000000001,\"unused\":[0");
    for (0..10000) |_| try metadata.appendSlice(alloc, ",0");
    try metadata.appendSlice(alloc, "]}");
    var counter = std.testing.FailingAllocator.init(alloc, .{});
    try std.testing.expect(try filter.matches(counter.allocator(), RegressionEdge{ .metadata = metadata.items }));
    try std.testing.expect(counter.allocated_bytes < 4096);
    try std.testing.expectEqual(counter.allocated_bytes, counter.freed_bytes);
    try std.testing.expect(!try filter.matches(alloc, RegressionEdge{ .metadata = "{\"value\":1,\"value\":2}" }));
    const valid = Filter{ .valid_at_ns = 0 };
    try std.testing.expect(!try valid.matches(alloc, RegressionEdge{ .metadata = "null" }));
}

test "relationship predicates selected decoding respects graph memory admission" {
    const alloc = std.testing.allocator;
    const raw = Filter{ .properties = &.{.{ .field = "/metadata/text", .op = .eq, .value_json = "\"" ++ ("a" ** 1024) ++ "\"" }} };
    const filter = try raw.prepare(alloc);
    defer filter.releasePrepared(alloc);
    const metadata = "{\"text\":\"" ++ ("\\u0061" ** 1024) ++ "\"}";
    var budget = work_budget.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = 64 });
    try std.testing.expectError(error.GraphWorkBudgetExceeded, filter.matchesWithBudget(alloc, RegressionEdge{ .metadata = metadata }, &budget));
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
    try std.testing.expect(try filter.matches(alloc, RegressionEdge{ .metadata = metadata }));
}

fn preparedFilterAllocationScenario(alloc: Allocator) !void {
    const raw = Filter{ .properties = &.{.{ .field = "/metadata/a~1b/~0key/0", .op = .eq, .value_json = "\"escaped\\u0020value\"" }} };
    const prepared = try raw.prepare(alloc);
    defer prepared.releasePrepared(alloc);
    const clone = try prepared.clone(alloc);
    defer clone.deinit(alloc);
    try std.testing.expect(try clone.matches(alloc, RegressionEdge{ .metadata = "{\"a/b\":{\"~key\":[\"escaped value\"]}}" }));
}

test "relationship predicates preparation cloning and projection clean up every failed allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, preparedFilterAllocationScenario, .{});
}

test "relationship predicates project overlapping paths and shared array leaves once" {
    const alloc = std.testing.allocator;
    const raw = Filter{ .properties = &.{
        .{ .field = "/metadata/a", .op = .is_not_null },
        .{ .field = "/metadata/a/items/0", .op = .eq, .value_json = "7" },
        .{ .field = "/metadata/a/items/0", .op = .lt, .value_json = "8" },
        .{ .field = "/metadata/a/items/1", .op = .is_null },
        .{ .field = "/metadata/a/", .op = .eq, .value_json = "true" },
    } };
    const filter = try raw.prepare(alloc);
    defer filter.releasePrepared(alloc);
    try std.testing.expectEqual(@as(usize, 4), filter.prepared.?.slot_count);
    try std.testing.expect(try filter.matches(alloc, RegressionEdge{ .metadata = "{\"a\":{\"items\":[7,null],\"\":true}}" }));
    try std.testing.expect(!try filter.matches(alloc, RegressionEdge{ .metadata = "{\"a\":{\"items\":[7,null],\"items\":[7,null],\"\":true}}" }));
    try std.testing.expect(!try filter.matches(alloc, RegressionEdge{ .metadata = "{\"a\":{\"items\":[7,null],\"\":true}} trailing" }));
}

test "relationship predicates large escaped scalars use explicit budgets and presence projection" {
    const alloc = std.testing.allocator;
    const text = try alloc.alloc(u8, 4 * 1024 * 1024 + 1);
    defer alloc.free(text);
    @memset(text, 'x');
    const escaped = try std.fmt.allocPrint(alloc, "{{\"payload\":\"\\u0078{s}\"}}", .{text});
    defer alloc.free(escaped);
    const plain = try std.fmt.allocPrint(alloc, "{{\"payload\":\"x{s}\"}}", .{text});
    defer alloc.free(plain);
    const presence = try (Filter{ .properties = &.{.{ .field = "/metadata/payload", .op = .is_not_null }} }).prepare(alloc);
    defer presence.releasePrepared(alloc);
    for ([_][]const u8{ plain, escaped }) |raw| {
        var budget = work_budget.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = 16384 });
        try std.testing.expect(try presence.matchesWithBudget(alloc, RegressionEdge{ .metadata = raw }, &budget));
        try std.testing.expect(budget.exhaustion() == null);
        try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
    }
    const comparison = try (Filter{ .properties = &.{
        .{ .field = "/metadata/payload", .op = .is_not_null },
        .{ .field = "/metadata/payload", .op = .gt, .value_json = "\"w\"" },
    } }).prepare(alloc);
    defer comparison.releasePrepared(alloc);
    for ([_][]const u8{ plain, escaped }) |raw| {
        var budget = work_budget.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = 32 * 1024 * 1024 });
        try std.testing.expect(try comparison.matchesWithBudget(alloc, RegressionEdge{ .metadata = raw }, &budget));
        try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
    }
    var small = work_budget.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = 16384 });
    try std.testing.expectError(error.GraphWorkBudgetExceeded, comparison.matchesWithBudget(alloc, RegressionEdge{ .metadata = escaped }, &small));
    try std.testing.expect(small.exhaustion() != null);
    try std.testing.expectEqual(@as(usize, 0), small.retained_state_bytes);
    try std.testing.expect(!try presence.matches(alloc, RegressionEdge{ .metadata = "{\"payload\":null}" }));
    try std.testing.expect(!try presence.matches(alloc, RegressionEdge{ .metadata = "{\"payload\":\"a\",\"payload\":\"b\"}" }));
}

test "relationship predicates escaped oversized keys are controlled by graph budgets" {
    const alloc = std.testing.allocator;
    const text = try alloc.alloc(u8, 4 * 1024 * 1024 + 1);
    defer alloc.free(text);
    @memset(text, 'x');
    const raw = try std.fmt.allocPrint(alloc, "{{\"\\u0078{s}\":null,\"payload\":true}}", .{text});
    defer alloc.free(raw);
    const filter = try (Filter{ .properties = &.{.{ .field = "/metadata/payload", .op = .is_not_null }} }).prepare(alloc);
    defer filter.releasePrepared(alloc);
    var ample = work_budget.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = 32 * 1024 * 1024 });
    try std.testing.expect(try filter.matchesWithBudget(alloc, RegressionEdge{ .metadata = raw }, &ample));
    try std.testing.expectEqual(@as(usize, 0), ample.retained_state_bytes);
    var small = work_budget.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = 16384 });
    try std.testing.expectError(error.GraphWorkBudgetExceeded, filter.matchesWithBudget(alloc, RegressionEdge{ .metadata = raw }, &small));
    try std.testing.expect(small.exhaustion() != null);
    try std.testing.expectEqual(@as(usize, 0), small.retained_state_bytes);
}
