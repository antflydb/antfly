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

//! Private, validated projections. Nullness sentinels are licensed only by
//! the compiler's demand proof and never replace inputs to authorization.
const std = @import("std");
const cypher = @import("cypher");
const View = @import("json_view.zig").View;
const Selector = @import("json_projection_plan.zig").Selector;
const A = std.mem.Allocator;
const Projection = cypher.read_fields.Projection;
pub fn decode(a: A, encoded: []const u8) ![]const Projection {
    return if (encoded.len == 0) &.{} else try std.json.parseFromSliceLeaky([]const Projection, a, encoded, .{ .allocate = .alloc_always });
}
pub fn find(projection: []const Projection, name: []const u8) ?Projection {
    for (projection) |item| if (!item.root and std.mem.eql(u8, item.name, name)) return item;
    return null;
}
/// Reconstruct only proved paths, keeping null/missing and non-map ancestors
/// distinguishable without copying their unobserved payloads.
fn projectValue(view: *View, node: *View.Node, item: Projection, depth: usize) anyerror!std.json.Value {
    if (depth > 64) return error.CypherValueDepthExceeded;
    switch (item.mode) {
        .value => {
            try cypher.value.validateJsonSlice(view.alloc, node.raw, depth);
            return view.materialize(node);
        },
        .predicate => {
            try cypher.value.validateJsonSlice(view.alloc, node.raw, depth);
            if (std.mem.eql(u8, std.mem.trim(u8, node.raw, " \r\n\t"), "null")) return .null;
            const fingerprint = if (item.fingerprints.len == item.alternatives.len and item.alternatives.len > 1)
                try cypher.read_fields.predicate_fingerprint.raw(view.alloc, node.raw)
            else
                null;
            for (item.alternatives, 0..) |expected, i| {
                if (fingerprint) |actual| if (!std.mem.eql(u8, &actual, &item.fingerprints[i])) continue;
                var scanner = std.json.Scanner.initCompleteInput(view.alloc, node.raw);
                defer scanner.deinit();
                if ((try predicateEqual(view.alloc, &scanner, expected)) orelse false) return expected;
            }
            return predicateNonmatch(view.alloc);
        },
        .presence => return presenceAtDepth(view.alloc, node.raw, depth),
        .children => {
            if (item.validate) try cypher.value.validateJsonSlice(view.alloc, node.raw, depth);
            if (node.kind() != '{') return presenceAtDepth(view.alloc, node.raw, depth);
            var object = std.json.ObjectMap.empty;
            if (item.keys) for (try memberNames(view, node)) |name| {
                const child = (try view.field(node, name)).?;
                try object.put(view.alloc, name, try presenceAtDepth(view.alloc, child.raw, depth + 1));
            };
            for (item.children) |child| if (try view.field(node, child.name)) |value| {
                try object.put(view.alloc, child.name, try projectValue(view, value, child, depth + 1));
            };
            return .{ .object = object };
        },
    }
}
pub fn rawValue(a: A, raw: []const u8, item: Projection) !std.json.Value {
    var view = View.init(a, raw);
    const selector: Selector = .{ .prepared = true };
    try view.prepareProjectionWithOptions(&selector, .all, .any_container, .{ .defer_fanout = true });
    return projectValue(&view, &view.root, item, 1);
}
fn projectTyped(view: *View, node: *View.Node, item: Projection) anyerror!cypher.value.Value {
    if (item.mode == .value) return cypher.wire.parameter(view.alloc, try view.materialize(node));
    const tag = try view.materialize((try view.field(node, "type")).?);
    if (std.mem.eql(u8, tag.string, "null")) return .null;
    if (item.mode == .presence or !std.mem.eql(u8, tag.string, "map")) return .{ .boolean = true };
    const payload = (try view.field(node, "value")).?;
    var entries: std.ArrayList(cypher.value.Entry) = .empty;
    if (item.keys) for (try memberNames(view, payload)) |name| {
        const selected = find(item.children, name);
        try entries.append(view.alloc, .{ .name = name, .value = try projectTyped(view, (try view.field(payload, name)).?, selected orelse .{ .name = name, .mode = .presence }) });
    };
    for (item.children) |child| if (try view.field(payload, child.name)) |value| {
        if (item.keys) continue;
        try entries.append(view.alloc, .{ .name = child.name, .value = try projectTyped(view, value, child) });
    };
    return .{ .map = entries.items };
}
/// The true part of Cypher equality is an equivalence relation for JSON values.
/// A matching catalog constant therefore preserves every label equality test;
/// A [NULL] nonmatch preserves nonnullness but cannot equal any JSON value.
/// Only equality and nullness observers may use this private carrier.
pub fn predicateJson(a: A, input: std.json.Value, item: Projection) !std.json.Value {
    if (input == .null) return .null;
    const actual: cypher.value.Value = if (input == .string) .{ .string = input.string } else try cypher.value.Value.fromJson(a, input);
    for (item.alternatives) |expected| {
        if (cypher.value.Value.equal(actual, try cypher.value.Value.fromJson(a, expected)) orelse false) return expected;
    }
    return predicateNonmatch(a);
}
fn predicateNonmatch(a: A) !std.json.Value {
    var values: std.array_list.Managed(std.json.Value) = .init(a);
    try values.append(.null);
    return .{ .array = values };
}
fn stringEqual(scanner: *std.json.Scanner, expected: []const u8) !bool {
    var offset: usize = 0;
    var matches = true;
    while (true) {
        const token = try scanner.next();
        var buffer: [4]u8 = undefined;
        const part: []const u8 = switch (token) {
            .string, .partial_string => |part| part,
            .partial_string_escaped_1 => |bytes| copy: {
                @memcpy(buffer[0..1], &bytes);
                break :copy buffer[0..1];
            },
            .partial_string_escaped_2 => |bytes| copy: {
                @memcpy(buffer[0..2], &bytes);
                break :copy buffer[0..2];
            },
            .partial_string_escaped_3 => |bytes| copy: {
                @memcpy(buffer[0..3], &bytes);
                break :copy buffer[0..3];
            },
            .partial_string_escaped_4 => |bytes| copy: {
                @memcpy(&buffer, &bytes);
                break :copy &buffer;
            },
            else => return error.SyntaxError,
        };
        if (offset > expected.len or part.len > expected.len -| offset or !std.mem.eql(u8, part, expected[offset..@min(expected.len, offset +| part.len)])) matches = false;
        offset +|= part.len;
        if (token == .string) return matches and offset == expected.len;
    }
}
/// Streams large actual strings/containers, owning only catalog-sized keys and
/// comparison state. The caller validates the entire value before comparing so
/// an early nonmatch cannot hide numeric or depth failures in its tail.
fn predicateEqual(a: A, scanner: *std.json.Scanner, expected: std.json.Value) anyerror!?bool {
    const kind = try scanner.peekNextTokenType();
    if (kind == .null) {
        _ = try scanner.next();
        return null;
    }
    if (expected == .null) {
        try scanner.skipValue();
        return null;
    }
    if (expected == .string) return if (kind == .string) try stringEqual(scanner, expected.string) else false;
    if (expected == .array) {
        if (kind != .array_begin) return false;
        _ = try scanner.next();
        var unknown = false;
        for (expected.array.items) |item| {
            if (try scanner.peekNextTokenType() == .array_end) return false;
            if (try predicateEqual(a, scanner, item)) |same| {
                if (!same) return false;
            } else unknown = true;
        }
        if (try scanner.next() != .array_end) return false;
        return if (unknown) null else true;
    }
    if (expected == .object) {
        if (kind != .object_begin) return false;
        _ = try scanner.next();
        const seen = try a.alloc(bool, expected.object.count());
        @memset(seen, false);
        var maximum: usize = 0;
        for (expected.object.keys()) |key| maximum = @max(maximum, key.len);
        var count: usize = 0;
        var unknown = false;
        while (try scanner.peekNextTokenType() != .object_end) {
            if (count == seen.len) return false;
            const token = scanner.nextAllocMax(a, .alloc_if_needed, maximum) catch |err| switch (err) {
                error.ValueTooLong => return false,
                else => return err,
            };
            const key = if (token == .string) token.string else if (token == .allocated_string) token.allocated_string else return error.SyntaxError;
            const index = expected.object.getIndex(key) orelse return false;
            if (seen[index]) return error.DuplicateField;
            seen[index] = true;
            count += 1;
            if (try predicateEqual(a, scanner, expected.object.values()[index])) |same| {
                if (!same) return false;
            } else unknown = true;
        }
        _ = try scanner.next();
        return if (count != seen.len) false else if (unknown) null else true;
    }
    const token = try scanner.next();
    const actual: cypher.value.Value = switch (token) {
        .true => .{ .boolean = true },
        .false => .{ .boolean = false },
        .number => |number| try cypher.value.Value.fromJson(a, .{ .number_string = number }),
        else => return false,
    };
    return cypher.value.Value.equal(actual, try cypher.value.Value.fromJson(a, expected));
}
pub fn contains(names: []const []const u8, name: []const u8) bool {
    for (names) |item| if (std.mem.eql(u8, item, name)) return true;
    return false;
}
fn memberNames(view: *View, node: *View.Node) ![]const []const u8 {
    _ = try view.children(node);
    const Named = struct { name: []const u8, index: usize };
    var ordered: std.ArrayList(Named) = .empty;
    var iterator = node.object_index.iterator();
    while (iterator.next()) |entry| if (try view.field(node, entry.key_ptr.*) != null) try ordered.append(view.alloc, .{ .name = entry.key_ptr.*, .index = entry.value_ptr.* });
    std.mem.sort(Named, ordered.items, {}, struct {
        fn less(_: void, left: Named, right: Named) bool {
            return left.index < right.index;
        }
    }.less);
    const names = try view.alloc.alloc([]const u8, ordered.items.len);
    for (ordered.items, names) |entry, *name| name.* = entry.name;
    return names;
}
pub fn presenceValue(a: A, raw: []const u8) !std.json.Value {
    return presenceAtDepth(a, raw, 1);
}
fn presenceAtDepth(a: A, raw: []const u8, depth: usize) !std.json.Value {
    try cypher.value.validateJsonSlice(a, raw, depth);
    return if (std.mem.eql(u8, std.mem.trim(u8, raw, " \r\n\t"), "null")) .null else .{ .bool = true };
}
pub fn record(a: A, raw: []const u8, fields: []const []const u8, presence: []const []const u8, projection: []const Projection) !std.json.Value {
    var view = View.init(a, raw);
    view.parse_numbers = true;
    const selector: Selector = .{ .prepared = true };
    try view.prepareProjectionWithOptions(&selector, .all, .object_only, .{ .defer_fanout = true });
    var result = std.json.ObjectMap.empty;
    if (cypher.read_fields.rootKeys(projection)) for (try memberNames(&view, &view.root)) |name| {
        const node = (try view.field(&view.root, name)).?;
        const value = if (find(projection, name)) |item| try projectValue(&view, node, item, 1) else if (contains(fields, name) and !contains(presence, name)) try projectValue(&view, node, .{ .name = name, .mode = .value }, 1) else try presenceValue(a, node.raw);
        try result.put(a, name, value);
    };
    for (fields) |name| if (try view.field(&view.root, name)) |node| {
        if (cypher.read_fields.rootKeys(projection)) continue;
        const value = if (find(projection, name)) |item| try projectValue(&view, node, item, 1) else if (contains(presence, name)) try presenceValue(a, node.raw) else try projectValue(&view, node, .{ .name = name, .mode = .value }, 1);
        try result.put(a, name, value);
    };
    return .{ .object = result };
}
/// Validate a private typed value before recognizing the mutation envelope.
/// Values omitted by projection still participate in envelope discrimination.
fn typed(view: *View, node: *View.Node, depth: usize) anyerror!bool {
    if (depth > 64 or node.kind() != '{' or (try view.children(node)).len != 2) return false;
    const tag = (try view.field(node, "type")) orelse return false;
    const payload = (try view.field(node, "value")) orelse return false;
    if (tag.raw.len > 64) return false;
    const kind = try view.materialize(tag);
    if (kind != .string) return false;
    if (std.mem.eql(u8, kind.string, "string")) return payload.kind() == '"';
    if (std.mem.eql(u8, kind.string, "null")) return std.mem.eql(u8, std.mem.trim(u8, payload.raw, " \r\n\t"), "null");
    if (std.mem.eql(u8, kind.string, "map") or std.mem.eql(u8, kind.string, "list")) {
        if (payload.kind() != (if (std.mem.eql(u8, kind.string, "map")) @as(u8, '{') else '[')) return false;
        for (try view.children(payload)) |*entry| if (!try typed(view, &entry.node, depth + 1)) return false;
        return true;
    }
    const value = try view.materialize(node);
    _ = cypher.wire.parameter(view.alloc, value) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    };
    return true;
}
pub fn prepareMetadata(_: *anyopaque, a: A, raw: []const u8) !cypher.read_fields.PreparedMetadata {
    const prepared = try a.create(Prepared);
    prepared.* = .{ .view = View.init(a, raw), .headers = std.json.ObjectMap.empty };
    const view = &prepared.view;
    const selector: Selector = .{ .prepared = true };
    try view.prepareProjectionWithOptions(&selector, .all, .object_only, .{ .defer_fanout = true, .retain_root_fields = true });
    for ([_][]const u8{ "source_table", "target_table" }) |name| if (try view.field(&view.root, name)) |node| try prepared.headers.put(a, name, try view.materialize(node));
    return .{ .headers = .{ .object = prepared.headers }, .context = prepared, .project = Prepared.project };
}
pub fn metadata(ptr: *anyopaque, a: A, raw: []const u8, weight: f64, demand: cypher.read_fields.Entity) !cypher.read_fields.Metadata {
    const prepared = try prepareMetadata(ptr, a, raw);
    return .{ .headers = prepared.headers, .properties = try prepared.project(prepared.context, a, weight, demand) };
}
const Prepared = struct {
    view: View,
    headers: std.json.ObjectMap,
    fn project(ptr: *anyopaque, a: A, weight: f64, demand: cypher.read_fields.Entity) ![]const cypher.value.Entry {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const view = &self.view;
        const headers = self.headers;
        const requested = demand.fields orelse return error.InvalidArgument;
        var private_properties: ?*View.Node = null;
        if ((try view.children(&view.root)).len == 3 and headers.count() == 2 and headers.get("source_table").? == .string and headers.get("target_table").? == .string) {
            if (try view.field(&view.root, "antfly_cypher")) |envelope| if (envelope.kind() == '{' and (try view.children(envelope)).len == 2) {
                const version = try view.field(envelope, "version");
                const properties = try view.field(envelope, "properties");
                if (version != null and std.mem.eql(u8, std.mem.trim(u8, version.?.raw, " \r\n\t"), "1") and properties != null and try typed(view, properties.?, 0)) {
                    const tag = try view.materialize((try view.field(properties.?, "type")).?);
                    if (std.mem.eql(u8, tag.string, "map")) private_properties = (try view.field(properties.?, "value")).?;
                }
            };
        }
        const keys_only = cypher.read_fields.rootKeys(demand.projection);
        var selected_fields: std.ArrayList([]const u8) = .empty;
        try selected_fields.appendSlice(a, requested);
        if (keys_only) {
            const root = private_properties orelse &view.root;
            for (try memberNames(view, root)) |name| if (!contains(selected_fields.items, name)) try selected_fields.append(a, name);
            if (private_properties == null and !contains(selected_fields.items, "weight")) try selected_fields.append(a, "weight");
        }
        const fields = selected_fields.items;
        var properties: std.ArrayList(cypher.value.Entry) = .empty;
        for (fields) |name| {
            if (private_properties == null and (std.mem.eql(u8, name, "source_table") or std.mem.eql(u8, name, "target_table") or std.mem.eql(u8, name, "weight"))) continue;
            const root = private_properties orelse &view.root;
            const node = (try view.field(root, name)) orelse continue;
            const presence = contains(demand.presence, name) or (keys_only and !contains(requested, name));
            const value: cypher.value.Value = if (private_properties != null) value: {
                if (find(demand.projection, name)) |item| break :value try projectTyped(view, node, item);
                if (presence) {
                    const tag = try view.materialize((try view.field(node, "type")).?);
                    break :value if (std.mem.eql(u8, tag.string, "null")) .null else .{ .boolean = true };
                }
                break :value try cypher.wire.parameter(a, try view.materialize(node));
            } else try cypher.value.Value.fromJson(a, if (find(demand.projection, name)) |item| try projectValue(view, node, item, 0) else if (presence) try presenceAtDepth(a, node.raw, 0) else try view.materialize(node));
            try properties.append(a, .{ .name = try a.dupe(u8, name), .value = value });
        }
        if (private_properties == null and contains(fields, "weight")) try properties.append(a, .{ .name = "weight", .value = if (contains(demand.presence, "weight")) .{ .boolean = true } else .{ .number = weight } });
        return try properties.toOwnedSlice(a);
    }
};

/// Captured once per owner page. The graph index invokes this specialization
/// on borrowed winner metadata, before constructing an owned output page.
pub const EdgeProjection = struct {
    const Spec = struct {
        fields: []const []const u8,
        presence: []const []const u8 = &.{},
        projection: []const Projection = &.{},
        source: []const u8,
        target: []const u8,
        source_storage: ?[]const u8 = null,
        target_storage: ?[]const u8 = null,
        skip_self_loops: bool = false,
    };
    spec: ?Spec = null,
    pub fn init(a: A, encoded: []const u8) !@This() {
        return .{ .spec = if (encoded.len == 0) null else try std.json.parseFromSliceLeaky(Spec, a, encoded, .{}) };
    }
    fn matches(headers: std.json.ObjectMap, name: []const u8, logical: []const u8, physical: ?[]const u8) bool {
        const value = headers.get(name) orelse return true;
        if (value == .null) return true;
        return value == .string and (std.mem.eql(u8, value.string, logical) or (if (physical) |actual| std.mem.eql(u8, value.string, actual) else false));
    }
    pub fn project(self: @This(), a: A, edge: anytype) ![]const u8 {
        const spec = self.spec orelse return edge.metadata;
        var dummy: u8 = 0;
        const prepared = try prepareMetadata(&dummy, a, if (edge.metadata.len == 0) "{}" else edge.metadata);
        const headers = prepared.headers.object;
        if (!matches(headers, "source_table", spec.source, spec.source_storage) or !matches(headers, "target_table", spec.target, spec.target_storage) or
            (spec.skip_self_loops and std.mem.eql(u8, spec.source, spec.target) and std.mem.eql(u8, edge.source, edge.target)))
            return std.json.Stringify.valueAlloc(a, prepared.headers, .{});
        const properties = try prepared.project(prepared.context, a, edge.weight, .{ .fields = spec.fields, .presence = spec.presence, .projection = spec.projection });
        // Legacy entry decoding has no enclosing property-map level. If a
        // valid boundary-depth value cannot fit a private envelope, retain the
        // original representation and let the adapter project it directly.
        cypher.value.validatePropertyValue(.{ .map = properties }) catch return edge.metadata;
        // A complete private envelope keeps native and mutation-owned metadata
        // unambiguous after pruning, including the legacy synthetic weight.
        const encoded = try std.json.Stringify.valueAlloc(a, .{
            .source_table = spec.source,
            .target_table = spec.target,
            .antfly_cypher = .{ .version = 1, .properties = cypher.wire.Value{ .value = .{ .map = properties } } },
        }, .{});
        return if (encoded.len < edge.metadata.len) encoded else edge.metadata;
    }
};

test "Cypher demand projection skips wide record and relationship values without changing nullness" {
    const a = std.testing.allocator;
    const wide = try a.alloc(u8, 2 << 20);
    defer a.free(wide);
    @memset(wide, 'x');
    const raw = try std.json.Stringify.valueAlloc(a, .{ .body = wide, .rank = 7, .nil = @as(?bool, null) }, .{});
    defer a.free(raw);
    var quota: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 128 << 10 };
    var arena = std.heap.ArenaAllocator.init(quota.allocator());
    defer arena.deinit();
    const projected = try record(arena.allocator(), raw, &.{ "body", "nil", "rank" }, &.{ "body", "nil" }, &.{});
    try std.testing.expect(projected.object.get("body").?.bool);
    try std.testing.expect(projected.object.get("nil").? == .null);
    var dummy: u8 = 0;
    const legacy = try metadata(&dummy, arena.allocator(), raw, 1, .{ .fields = &.{"rank"} });
    try std.testing.expectEqual(@as(usize, 1), legacy.properties.len);
    try std.testing.expectEqual(@as(i64, 7), legacy.properties[0].value.integer);
    const typed_raw = try std.json.Stringify.valueAlloc(a, .{ .source_table = "docs", .target_table = "docs", .antfly_cypher = .{ .version = 1, .properties = .{ .type = "map", .value = .{ .body = .{ .type = "string", .value = wide }, .rank = .{ .type = "integer", .value = "9" } } } } }, .{});
    defer a.free(typed_raw);
    const typed_result = try metadata(&dummy, arena.allocator(), typed_raw, 1, .{ .fields = &.{ "rank", "body" }, .presence = &.{"body"} });
    try std.testing.expectEqual(@as(usize, 2), typed_result.properties.len);
    try std.testing.expectEqual(@as(i64, 9), typed_result.properties[0].value.integer);
    try std.testing.expect(typed_result.properties[1].value.boolean);
}
test "Cypher demand projection preserves envelope discrimination validation and failure cleanup" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var dummy: u8 = 0;
    for ([_][]const u8{
        "{\"source_table\":\"docs\",\"target_table\":\"docs\",\"antfly_cypher\":{\"version\":1,\"properties\":{\"type\":\"string\",\"value\":\"ordinary\"}}}",
        "{\"source_table\":\"docs\",\"target_table\":\"docs\",\"antfly_cypher\":{\"version\":1,\"properties\":{\"type\":\"map\",\"value\":{\"rank\":{\"type\":\"integer\",\"value\":\"invalid\"}}}}}",
    }) |raw| {
        const result = try metadata(&dummy, arena.allocator(), raw, 2, .{ .fields = &.{ "antfly_cypher", "weight" } });
        try std.testing.expectEqual(@as(usize, 2), result.properties.len);
        try std.testing.expect(result.properties[0].value == .map);
        try std.testing.expectEqual(@as(f64, 2), result.properties[1].value.number);
    }
    // Preparation validates syntax and headers without converting properties.
    // The adapter can reject a mismatched table/endpoint before this failure.
    const rejected = try prepareMetadata(&dummy, arena.allocator(), "{\"source_table\":\"other\",\"rank\":9223372036854775808}");
    try std.testing.expectEqualStrings("other", rejected.headers.object.get("source_table").?.string);
    try std.testing.expectError(error.CypherNumericOverflow, rejected.project(rejected.context, arena.allocator(), 1, .{ .fields = &.{"rank"} }));
    try std.testing.expectError(error.DuplicateField, record(arena.allocator(), "{\"unused\":1,\"unused\":2}", &.{}, &.{}, &.{}));
    try std.testing.expectError(error.CypherNumericOverflow, record(arena.allocator(), "{\"n\":9223372036854775808}", &.{"n"}, &.{"n"}, &.{}));
    var no_resize: @import("../../lite/test_allocator.zig").NoResizeAllocator = .{ .backing = a };
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), struct {
        fn run(alloc: A) !void {
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            var context: u8 = 0;
            const result = try metadata(&context, scratch.allocator(), "{\"rank\":1,\"body\":\"unused\"}", 1, .{ .fields = &.{"rank"} });
            try std.testing.expectEqual(@as(i64, 1), result.properties[0].value.integer);
        }
    }.run, .{});
}

test "Cypher demand projection prunes nested native and typed maps within bounded scratch" {
    const a = std.testing.allocator;
    const wide = try a.alloc(u8, 2 << 20);
    defer a.free(wide);
    @memset(wide, 'x');
    const raw = try std.json.Stringify.valueAlloc(a, .{ .payload = .{ .rank = 7, .body = wide, .nil = @as(?bool, null) } }, .{});
    defer a.free(raw);
    const fields = [_]Projection{.{ .name = "payload", .mode = .children, .children = &.{
        .{ .name = "rank", .mode = .value },
        .{ .name = "body", .mode = .presence },
        .{ .name = "nil", .mode = .presence },
        .{ .name = "missing", .mode = .presence },
    } }};
    var quota: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 128 << 10 };
    var arena = std.heap.ArenaAllocator.init(quota.allocator());
    defer arena.deinit();
    const projected = try record(arena.allocator(), raw, &.{"payload"}, &.{}, &fields);
    const payload = projected.object.get("payload").?.object;
    try std.testing.expectEqual(@as(i64, 7), payload.get("rank").?.integer);
    try std.testing.expect(payload.get("body").?.bool);
    try std.testing.expect(payload.get("nil").? == .null);
    try std.testing.expect(!payload.contains("missing"));
    var dummy: u8 = 0;
    const legacy = try metadata(&dummy, arena.allocator(), raw, 1, .{ .fields = &.{"payload"}, .projection = &fields });
    try std.testing.expectEqual(@as(i64, 7), legacy.properties[0].value.property("rank").integer);
    const typed_raw = try std.json.Stringify.valueAlloc(a, .{ .source_table = "docs", .target_table = "docs", .antfly_cypher = .{ .version = 1, .properties = .{ .type = "map", .value = .{ .payload = .{ .type = "map", .value = .{ .rank = .{ .type = "integer", .value = "9" }, .body = .{ .type = "string", .value = wide } } } } } } }, .{});
    defer a.free(typed_raw);
    const private = try metadata(&dummy, arena.allocator(), typed_raw, 1, .{ .fields = &.{"payload"}, .projection = &fields });
    try std.testing.expectEqual(@as(i64, 9), private.properties[0].value.property("rank").integer);
    try std.testing.expect(private.properties[0].value.property("body").boolean);
    try std.testing.expect(quota.peak < 128 << 10);
}

test "Cypher demand projection retains nested nullness literal keys and validation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const encoded = try a.dupe(u8, "[{\"name\":\"payload\",\"mode\":\"children\",\"children\":[{\"name\":\"rank\",\"mode\":\"value\"}]}]");
    const retained = try decode(a, encoded);
    @memset(encoded, 'x');
    try std.testing.expectEqualStrings("payload", retained[0].name);
    try std.testing.expectEqualStrings("rank", retained[0].children[0].name);
    const projection: Projection = .{ .name = "payload", .mode = .children, .children = &.{.{ .name = "rank.with.dot", .mode = .value }} };
    for ([_][]const u8{ "null", "7", "true", "[]", "\"str\"" }, 0..) |raw, i| {
        const value = try rawValue(a, raw, projection);
        if (i == 0) try std.testing.expect(value == .null) else try std.testing.expect(value.bool);
    }
    const object = try rawValue(a, "{\"rank.with.dot\":7,\"ignored\":\"wide\"}", projection);
    try std.testing.expectEqual(@as(usize, 1), object.object.count());
    try std.testing.expectEqual(@as(i64, 7), object.object.get("rank.with.dot").?.integer);
    try std.testing.expectError(error.CypherNumericOverflow, rawValue(a, "{\"rank.with.dot\":9223372036854775808}", projection));
    try std.testing.expectError(error.DuplicateField, rawValue(a, "{\"ignored\":1,\"ignored\":2}", projection));
    // Presence of a partially consumed ancestor keeps its validation contract.
    var observed_ancestor = projection;
    observed_ancestor.validate = true;
    _ = try rawValue(a, "{\"rank.with.dot\":7,\"ignored\":9223372036854775808}", projection);
    try std.testing.expectError(error.CypherNumericOverflow, rawValue(a, "{\"rank.with.dot\":7,\"ignored\":9223372036854775808}", observed_ancestor));
}

test "Cypher demand projection keeps empty map shapes and presence validation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var shape: Projection = .{ .name = "payload", .mode = .children };
    const value = try rawValue(a, "{\"ignored\":9223372036854775808}", shape);
    try std.testing.expect(value == .object and value.object.count() == 0);
    try std.testing.expect((try rawValue(a, "null", shape)) == .null);
    try std.testing.expect((try rawValue(a, "42", shape)).bool);
    shape.validate = true;
    try std.testing.expectError(error.CypherNumericOverflow, rawValue(a, "{\"ignored\":9223372036854775808}", shape));
}

test "Cypher demand projection predicate representatives preserve equality without owning actual strings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_]struct { actual: []const u8, alternatives: []const u8, matches: bool }{
        .{ .actual = "\"acme\"", .alternatives = "[\"other\",\"acme\"]", .matches = true },
        .{ .actual = "\"a\\u0063me\"", .alternatives = "[\"acme\"]", .matches = true },
        .{ .actual = "\"\\ud83d\\ude00\"", .alternatives = "[\"😀\"]", .matches = true },
        .{ .actual = "[1,2]", .alternatives = "[[1.0,2]]", .matches = true },
        .{ .actual = "{\"b\":[2],\"a\":1}", .alternatives = "[{\"a\":1.0,\"b\":[2]}]", .matches = true },
        .{ .actual = "[null,1]", .alternatives = "[[null,1]]", .matches = false },
        .{ .actual = "null", .alternatives = "[null,1]", .matches = false },
        .{ .actual = "{\"x\":1}", .alternatives = "[{\"a\":1}]", .matches = false },
        .{ .actual = "[1,2,3]", .alternatives = "[[1,2]]", .matches = false },
        .{ .actual = "true", .alternatives = "[true]", .matches = true },
        .{ .actual = "1e0", .alternatives = "[1]", .matches = true },
    };
    for (cases) |case| {
        const alternatives = try std.json.parseFromSliceLeaky([]const std.json.Value, a, case.alternatives, .{});
        const projected = try rawValue(a, case.actual, .{ .name = "field", .mode = .predicate, .alternatives = alternatives });
        if (case.matches) try std.testing.expect(projected != .null);
        try std.testing.expect((projected == .null) == std.mem.eql(u8, case.actual, "null"));
        const actual = try cypher.value.Value.fromJson(a, try std.json.parseFromSliceLeaky(std.json.Value, a, case.actual, .{ .parse_numbers = false }));
        const representative = try cypher.value.Value.fromJson(a, projected);
        for (alternatives) |expected| {
            const value = try cypher.value.Value.fromJson(a, expected);
            try std.testing.expectEqual(cypher.value.Value.equal(actual, value) orelse false, cypher.value.Value.equal(representative, value) orelse false);
        }
    }
    const alternatives = try std.json.parseFromSliceLeaky([]const std.json.Value, a, "[{\"a\":0}]", .{});
    const projection: Projection = .{ .name = "field", .mode = .predicate, .alternatives = alternatives };
    try std.testing.expectError(error.CypherNumericOverflow, rawValue(a, "{\"a\":1,\"bad\":9223372036854775808}", projection));
    try std.testing.expectError(error.DuplicateField, rawValue(a, "{\"bad\":{\"k\":1,\"k\":2}}", projection));
    const wide = try a.alloc(u8, (2 << 20) + 2);
    @memset(wide, 'x');
    wide[0] = '"';
    wide[wide.len - 1] = '"';
    var quota: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 64 << 10 };
    var scratch = std.heap.ArenaAllocator.init(quota.allocator());
    defer scratch.deinit();
    const rejected = try rawValue(scratch.allocator(), wide, projection);
    try std.testing.expect(rejected == .array and rejected.array.items.len == 1 and rejected.array.items[0] == .null);
    const scalar = try predicateJson(scratch.allocator(), .{ .string = wide[1 .. wide.len - 1] }, projection);
    try std.testing.expect(scalar == .array);
    try std.testing.expect(quota.peak < 64 << 10);
}

test "Cypher demand projection key shapes preserve names nulls and independent values" {
    const a = std.testing.allocator;
    const wide = try a.alloc(u8, 2 << 20);
    defer a.free(wide);
    @memset(wide, 'x');
    const source = try std.json.Stringify.valueAlloc(a, .{ .empty = wide, .@"a.b" = wide, .rank = 7, .nil = @as(?u8, null), .payload = .{ .body = wide, .rank = 7, .nil = @as(?u8, null) } }, .{});
    defer a.free(source);
    const raw = try std.mem.replaceOwned(u8, a, source, "\"empty\":", "\"\":");
    defer a.free(raw);
    var quota: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 64 << 10 };
    var arena = std.heap.ArenaAllocator.init(quota.allocator());
    defer arena.deinit();
    const projections = [_]Projection{
        .{ .name = "", .mode = .children, .root = true, .keys = true },
        .{ .name = "payload", .mode = .children, .keys = true, .children = &.{.{ .name = "rank", .mode = .value }} },
    };
    const projected = try record(arena.allocator(), raw, &.{ "rank", "payload" }, &.{}, &projections);
    try std.testing.expect(projected.object.get("").?.bool);
    try std.testing.expect(projected.object.get("a.b").?.bool);
    try std.testing.expectEqual(@as(i64, 7), projected.object.get("rank").?.integer);
    try std.testing.expect(projected.object.get("nil").? == .null);
    const nested = projected.object.get("payload").?.object;
    try std.testing.expect(nested.get("body").?.bool);
    try std.testing.expect(nested.get("nil").? == .null);
    try std.testing.expectEqual(@as(i64, 7), nested.get("rank").?.integer);
    const names = projected.object.keys();
    try std.testing.expectEqualStrings("", names[0]);
    try std.testing.expectEqualStrings("a.b", names[1]);
    try std.testing.expectEqualStrings("rank", names[2]);
}

test "Cypher demand projection fingerprints share JSON scanning without changing exact equality" {
    const a = std.testing.allocator;
    const Fingerprint = cypher.read_fields.predicate_fingerprint;
    for ([_][]const u8{ "1", "1.0", "-0.0", "0", "9007199254740993", "9007199254740992.0", "{\"x\":1,\"y\":[true,\"a\\u0042\",null]}", "{\"y\":[true,\"aB\",null],\"x\":1.0}" }) |raw| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{ .parse_numbers = false });
        defer parsed.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const value = try cypher.value.Value.fromJson(arena.allocator(), parsed.value);
        try std.testing.expectEqual(Fingerprint.value(value), try Fingerprint.raw(arena.allocator(), raw));
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const allocator = arena.allocator();
    const alternatives = try allocator.alloc(std.json.Value, 64);
    const fingerprints = try allocator.alloc(Fingerprint.Digest, alternatives.len);
    for (alternatives, fingerprints, 0..) |*alternative, *fingerprint, i| {
        alternative.* = .{ .string = try std.fmt.allocPrint(allocator, "notmatched_{d}", .{i}) };
        fingerprint.* = Fingerprint.value(.{ .string = alternative.string });
    }
    const item: Projection = .{ .name = "body", .mode = .predicate, .alternatives = alternatives, .fingerprints = fingerprints };
    const actual = try std.json.Stringify.valueAlloc(allocator, alternatives[37], .{});
    try std.testing.expectEqualStrings(alternatives[37].string, (try rawValue(allocator, actual, item)).string);
    // Fingerprints only index candidates. Even deliberately identical digests
    // cannot turn a false comparison into membership.
    fingerprints[0] = fingerprints[37];
    try std.testing.expectEqualStrings(alternatives[37].string, (try rawValue(allocator, actual, item)).string);
}
