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

//! Lazy scalar source pinned to the query's native read transaction. Metadata
//! is an optimization only: missing/old certificates fall back to source reads.
const std = @import("std");
const projection = @import("../../api/field_projection.zig");
const sql = @import("../../sql/scalar.zig");
const logical = @import("../logical_source.zig");
const conditional = @import("../conditional_source.zig");
const query = @import("document_query.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const OperandPlans = struct {
    const View = @import("query/json_view.zig").View;
    const Selector = @import("query/json_projection_plan.zig");
    const Operand = struct { parts: []const []const u8, selector: *const Selector.Selector };
    paths: std.StringHashMapUnmanaged(Operand) = .empty,
    selector: *const Selector.Selector,
    pub fn init(a: A, plan: *const projection.Plan) !OperandPlans {
        var paths: std.ArrayList(Selector.Path) = .empty;
        var self: OperandPlans = .{ .selector = undefined };
        var fields = plan.required_fields.keyIterator();
        while (fields.next()) |path| {
            var parts: std.ArrayList([]const u8) = .empty;
            var tokens = std.mem.splitScalar(u8, path.*, '.');
            while (tokens.next()) |part| try parts.append(a, part);
            try paths.append(a, .{ .parts = parts.items });
            try self.paths.put(a, path.*, .{ .parts = parts.items, .selector = try Selector.compile(a, &.{.{ .parts = parts.items }}) });
        }
        self.selector = try Selector.compile(a, paths.items);
        return self;
    }
};
pub const Row = struct {
    a: A,
    txn: ?*@import("../docstore.zig").DocStore.Txn,
    physical: []const u8,
    id: []const u8,
    authority: conditional.Authority,
    req: @import("types.zig").SearchRequest,
    public_plan: ?*const query.PublicPlan = null,
    operands: ?*const OperandPlans = null,
    raw_sources: std.StringHashMapUnmanaged([]const u8) = .empty,
    raw_operands: std.StringHashMapUnmanaged([]const u8) = .empty,
    full_view: ?OperandPlans.View = null,
    scope: ?@import("../backend_erased.zig").ReadScope = null,
    prefetched: ?[]const u8 = null,
    prefetched_view: ?OperandPlans.View = null,
    prefetched_root: ?V = null,
    values: std.StringHashMapUnmanaged(V) = .empty,
    identities: std.StringHashMapUnmanaged(sql.Datum) = .empty,
    metadata_hits: usize = 0,
    certificate_hits: usize = 0,
    value_reads: usize = 0,
    full_bytes: ?[]const u8 = null,
    full_root: ?V = null,
    pub fn source(self: *Row) projection.Source {
        return .{ .context = self, .read = read, .read_raw = readRaw, .prefetch = prefetch };
    }
    pub fn deinit(self: *Row) void {
        if (self.scope) |*scope| scope.close();
        self.scope = null;
    }
    fn prefetchedPath(self: *Row, path: []const u8) bool {
        const bytes = self.raw_sources.get(path) orelse return false;
        const cached = self.prefetched orelse return false;
        return bytes.ptr == cached.ptr and bytes.len == cached.len;
    }
    fn prefetch(raw: *anyopaque, paths: []const []const u8) !void {
        const self: *Row = @ptrCast(@alignCast(raw));
        if (self.full_bytes != null) return;
        const bytes = try self.load(paths);
        if (self.full_bytes != null) return;
        self.prefetched = bytes;
        for (paths) |path| try self.raw_sources.put(self.a, path, bytes);
    }
    fn load(self: *Row, roots: []const []const u8) ![]const u8 {
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        if (self.full_bytes) |bytes| return bytes;
        if (self.scope == null) self.scope = try (self.txn orelse return error.DocumentNotFound).openReadScope(self.a);
        var authority = self.authority.source;
        const selective = roots.len != 0 and roots.len <= 64 and for (roots) |path| {
            if (!projection.validPath(path)) break false;
        } else true;
        if (!selective) authority.directory = null;
        const rows = try logical.loadPrepared(self.a, self.a, &self.scope.?, &.{self.physical}, &.{self.id}, if (selective) roots else &.{}, &.{authority});
        self.value_reads += 1;
        const bytes = rows[0].bytes orelse return error.DocumentNotFound;
        if (!rows[0].projected) self.full_bytes = bytes;
        return bytes;
    }
    fn readRaw(raw: *anyopaque, _: A, path: []const u8) !?[]const u8 {
        const self: *Row = @ptrCast(@alignCast(raw));
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        if (self.raw_operands.get(path)) |value| return value;
        // A prior operator may already have materialized this source. Reuse
        // that value rather than building a second index for small/mixed rows.
        if (self.full_root != null or self.values.contains(path) or (self.prefetched_root != null and self.prefetchedPath(path))) return null;
        const operands = self.operands orelse return null;
        const operand = operands.paths.get(path) orelse return null;
        if (std.mem.eql(u8, path, "_id")) return null;
        const bytes = self.raw_sources.get(path) orelse loaded: {
            const bytes = try self.load(&.{path});
            try self.raw_sources.put(self.a, path, bytes);
            break :loaded bytes;
        };
        var partial = OperandPlans.View.init(self.a, bytes);
        const view = if (self.full_bytes != null) full: {
            if (self.full_view == null) {
                var prepared = OperandPlans.View.init(self.a, self.full_bytes.?);
                try prepared.prepareProjectionWithOptions(operands.selector, .all, .object_only, .{ .defer_fanout = true });
                self.full_view = prepared;
            }
            break :full &self.full_view.?;
        } else if (self.prefetchedPath(path)) prefetched: {
            if (self.prefetched_view == null) {
                var prepared = OperandPlans.View.init(self.a, self.prefetched.?);
                try prepared.prepareProjectionWithOptions(operands.selector, .all, .object_only, .{ .defer_fanout = true });
                self.prefetched_view = prepared;
            }
            break :prefetched &self.prefetched_view.?;
        } else projected: {
            try partial.prepareProjectionWithOptions(operand.selector, .all, .object_only, .{ .defer_fanout = true });
            break :projected &partial;
        };
        var node = &view.root;
        for (operand.parts) |part| {
            node = switch (node.kind()) {
                '{' => (try view.field(node, part)) orelse return null,
                '[' => if (OperandPlans.Selector.index(part)) |index| (try view.element(node, index)) orelse return null else return null,
                else => return null,
            };
        }
        try self.raw_operands.put(self.a, path, node.raw);
        return node.raw;
    }
    fn read(raw: *anyopaque, a: A, path: []const u8, kind: sql.ColumnRead) anyerror!sql.Datum {
        const self: *Row = @ptrCast(@alignCast(raw));
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        if (std.mem.eql(u8, path, "_id")) {
            var source_value: projection.JsonSource = .{ .id = self.id, .root = .null };
            return source_value.source().read(&source_value, a, path, kind);
        }
        if (kind == .identity) if (self.identities.get(path)) |identity| return identity;
        if (kind != .value) if (self.authority.source.directory) |directory| {
            if (directory.resolveExact(path)) |field| {
                // Opaque subtrees do not prove that a descendant exists.
                {
                    if (kind == .missing) {
                        self.metadata_hits += 1;
                        return sql.Datum.json(.{ .bool = false });
                    }
                    if (field.kind != .object) if (self.authority.certificate) |certificate| if (certificate.find(field.index)) |digest| {
                        const value = sql.Datum.json(.{ .string = try std.fmt.allocPrint(a, "sv1-{s}", .{std.fmt.bytesToHex(digest, .lower)}) });
                        try self.identities.put(self.a, path, value);
                        self.metadata_hits += 1;
                        self.certificate_hits += 1;
                        return value;
                    };
                }
            } else if (directory.version == 2 and directory.resolve(path) == null) {
                self.metadata_hits += 1;
                return if (kind == .missing) sql.Datum.json(.{ .bool = true }) else .{};
            }
        };
        const root = self.full_root orelse self.values.get(path) orelse (if (self.prefetchedPath(path)) self.prefetched_root else null) orelse loaded: {
            const bytes = self.raw_sources.get(path) orelse try self.load(&.{path});
            // The authenticated range loader preserves opaque array positions.
            // Parse that immutable row view directly: re-masking compacts array
            // indexes, and copying/encoding it adds work proportional to payload.
            const value = try @import("antfly-json").parseFromSliceLeaky(V, self.a, bytes, .{ .allocate = .alloc_if_needed });
            if (self.full_bytes != null) self.full_root = value else if (self.prefetchedPath(path)) self.prefetched_root = value else try self.values.put(self.a, path, value);
            break :loaded value;
        };
        var source_value: projection.JsonSource = .{ .id = self.id, .root = root };
        return source_value.source().read(&source_value, a, path, kind);
    }
    pub fn base(self: *Row, output: A) ![]u8 {
        if (!self.req.include_all_fields and self.req.fields.len == 0) return output.dupe(u8, "{}");
        const bytes = try self.load(if (self.req.include_all_fields) &.{} else self.req.fields);
        if (self.public_plan) |plan| return (try plan.apply(self.a, output, bytes)).json;
        var plan = try query.PublicPlan.init(self.a, .{ .fields = self.req.fields, .include_all_fields = self.req.include_all_fields });
        defer plan.deinit();
        return (try plan.apply(self.a, output, bytes)).json;
    }
};

test "api query contract native lazy source uses nested leaf identities without opening payloads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const directory = @import("../document_projection.zig");
    const fields = [_]directory.Field{
        .{ .name = "body", .kind = .object, .offset = 0, .length = 0, .digest = @splat(0), .index = 0, .first_child = 1, .child_count = 1 },
        .{ .name = "image", .kind = .leaf, .offset = 0, .length = 0, .digest = @splat(0), .index = 1, .parent = 0 },
    };
    const identity = try @import("../../common/value_identity.zig").digestValue(.{ .string = "image" });
    var record: [34]u8 = undefined;
    std.mem.writeInt(u16, record[0..2], 1, .little);
    record[2..].* = identity;
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{
        .source = .{ .directory = .{ .fields = &fields, .root_count = 1, .version = 2 } },
        .certificate = .{ .records = &record },
    } };
    const token = try row.source().read(&row, a, "body.image", .identity);
    try std.testing.expectEqualStrings(try std.fmt.allocPrint(a, "sv1-{s}", .{std.fmt.bytesToHex(identity, .lower)}), token.value.string);
    const present = try row.source().read(&row, a, "body.image", .missing);
    try std.testing.expect(!present.value.bool);
    const absent = try row.source().read(&row, a, "body.absent", .missing);
    try std.testing.expect(absent.value.bool);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
}

test "api query contract native full source fallback retains hidden null and sibling operands" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .full_bytes = "{\"body\":{\"image\":\"large\",\"null\":null,\"integer\":9007199254740993}}" };
    _ = try row.source().read(&row, a, "body.image", .identity);
    const token = try row.source().read(&row, a, "body.null", .identity);
    try std.testing.expectEqualStrings(&(try @import("../../common/value_identity.zig").identity(.null)), token.value.string);
    const number = try row.source().read(&row, a, "body.integer", .value);
    try std.testing.expectEqual(@as(i64, 9007199254740993), number.value.integer);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
}

test "api query contract native scalar base preserves indexed array selection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "{\"arr\":[{\"n\":1},{\"n\":2}]}",
        "{\"arr\":[{\"n\":1},{\"n\":2}],\"metadata\":{\"_type\":\"image\"},\"padding\":\"small\"}",
    }) |bytes| {
        var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{ .fields = &.{"arr.1.n"}, .include_all_fields = false }, .authority = .{}, .full_bytes = bytes };
        try std.testing.expectEqualStrings("{\"arr\":[{\"n\":2}]}", try row.base(a));
    }
}

test "api query contract opaque identity proves exact path without certifying descendants" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const directory = @import("../document_projection.zig");
    const fields = [_]directory.Field{.{ .name = "items", .kind = .subtree, .offset = 0, .length = 0, .digest = @splat(0), .index = 0 }};
    const digest = try @import("../../common/value_identity.zig").digestValue(.null);
    var record: [34]u8 = undefined;
    std.mem.writeInt(u16, record[0..2], 0, .little);
    record[2..].* = digest;
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{
        .source = .{ .directory = .{ .fields = &fields, .root_count = 1, .version = 2 } },
        .certificate = .{ .records = &record },
    } };
    _ = try row.source().read(&row, a, "items", .identity);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
    try std.testing.expectError(error.DocumentNotFound, row.source().read(&row, a, "items.child", .missing));
}

test "api query contract raw full fallback shares one row view and memoized operands" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"first","expression":{"field":"body.first"}},
        \\  {"name":"second","expression":{"field":"body.second"}} ]
    , .{});
    var plan = try projection.Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const operands = try OperandPlans.init(a, &plan);
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .operands = &operands, .full_bytes = "{\"body\":{\"first\":[1,null],\"second\":\"two\"},\"unused\":{\"large\":[0,1,2]}}" };
    const source = row.source();
    const first = (try source.read_raw.?(source.context, a, "body.first")).?;
    try std.testing.expectEqualStrings("[1,null]", first);
    try std.testing.expectEqualStrings("\"two\"", (try source.read_raw.?(source.context, a, "body.second")).?);
    const again = (try source.read_raw.?(source.context, a, "body.first")).?;
    try std.testing.expect(first.ptr == again.ptr);
    try std.testing.expectEqual(@as(usize, 2), row.raw_operands.count());
    try std.testing.expectEqual(@as(usize, 0), row.full_view.?.materialized_values);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
    var typed: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .operands = &operands, .full_root = try std.json.parseFromSliceLeaky(V, a, row.full_bytes.?, .{}) };
    const typed_source = typed.source();
    try std.testing.expect(try typed_source.read_raw.?(typed_source.context, a, "body.first") == null);
    try std.testing.expectEqual(@as(usize, 2), (try typed_source.read(typed_source.context, a, "body.first", .value)).value.array.items.len);
    try std.testing.expect(typed.full_view == null);
}
