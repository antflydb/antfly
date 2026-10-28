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
    read_set: []const []const u8 = &.{},
    full_read: bool = false,
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
        var read_set: std.ArrayList([]const u8) = .empty;
        for (plan.source_fields) |path| {
            if (!projection.validPath(path)) self.full_read = true;
            if (!hasPath(read_set.items, path)) try read_set.append(a, path);
        }
        for (plan.unconditional_fields) |path| if (!hasPath(read_set.items, path)) try read_set.append(a, path);
        self.read_set = read_set.items;
        return self;
    }
};
fn hasPath(paths: []const []const u8, path: []const u8) bool {
    for (paths) |value| if (std.mem.eql(u8, value, path)) return true;
    return false;
}
/// Selected rows only. Ownership is bounded by a byte window, not page count;
/// unknown/large afterimages retain ordinary lazy per-row reads.
pub const Batch = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    rows: []logical.Row = &.{},
    start: usize = 0,
    end: usize = 0,
    pub fn init(a: A) Batch {
        return .{ .a = a, .arena = .init(a) };
    }
    pub fn deinit(self: *Batch) void {
        self.arena.deinit();
    }
    pub fn reset(self: *Batch) void {
        _ = self.arena.reset(.free_all);
        self.rows = &.{};
        self.start = 0;
        self.end = 0;
    }
    pub fn at(self: *Batch, txn: *@import("../docstore.zig").DocStore.Txn, index: usize, physical: []const []const u8, ids: []const []const u8, authorities: []const conditional.Authority, req: @import("types.zig").SearchRequest, operands: *const OperandPlans) !?logical.Row {
        if (!req.include_all_fields and operands.read_set.len == 0) return null;
        if (index < self.end) return self.rows[index - self.start];
        self.reset();
        const full = req.include_all_fields or operands.full_read or operands.read_set.len > 64;
        const a = self.arena.allocator();
        const selections = try a.alloc(?logical.Selection, ids.len - index);
        @memset(selections, null);
        var end = index;
        var bytes: usize = 0;
        while (end < ids.len) : (end += 1) {
            const authority = authorities[end].source;
            const proof = authority.proof orelse break;
            var bound = proof.bytes;
            if (!full) if (authority.directory) |directory| {
                const selection = logical.Selection.init(directory, operands.read_set);
                if (selection.rangeEligible(proof)) {
                    bound = selection.workingBytes();
                    selections[end - index] = selection;
                }
            };
            const count = std.math.cast(usize, bound) orelse break;
            if (count > 1024 * 1024 -| bytes) break;
            bytes += count;
        }
        if (end - index < 2) return null;
        try @import("query/search_exec.zig").checkSearchRequestDeadline(req);
        var scope = try txn.openReadScope(a);
        defer scope.close();
        const selected = try a.alloc(logical.Authority, end - index);
        for (authorities[index..end], selected) |authority, *out| out.* = authority.source;
        if (full) for (selected) |*authority| {
            authority.directory = null;
        };
        self.rows = try logical.loadPreparedSelected(a, a, &scope, physical[index..end], ids[index..end], if (full) &.{} else operands.read_set, selected, selections[0 .. end - index]);
        self.start = index;
        self.end = end;
        return self.rows[0];
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
    base_prefetched: bool = false,
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
        return .{ .context = self, .read = read, .read_raw = readRaw, .read_normalized = readNormalized, .prefetch = prefetch };
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
    pub fn attach(self: *Row, prepared: logical.Row) void {
        if (!prepared.projected) self.full_bytes = prepared.bytes else {
            self.prefetched = prepared.bytes;
            self.base_prefetched = true;
        }
    }
    fn prefetch(raw: *anyopaque, paths: []const []const u8) !void {
        const self: *Row = @ptrCast(@alignCast(raw));
        if (self.full_bytes != null) return;
        const read_set = if (self.operands) |operands| operands.read_set else paths;
        if (!self.req.include_all_fields and read_set.len == 0) return;
        const full = self.req.include_all_fields or (if (self.operands) |operands| operands.full_read else false);
        const bytes = self.prefetched orelse try self.load(if (full) &.{} else read_set);
        if (self.full_bytes != null) return;
        self.prefetched = bytes;
        self.base_prefetched = self.operands != null;
        for (read_set) |path| try self.raw_sources.put(self.a, path, bytes);
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
    fn readNormalized(raw: *anyopaque, path: []const u8) !?sql.NormalizedOutput {
        const self: *Row = @ptrCast(@alignCast(raw));
        // readRaw establishes strict immutable node extents with the row's
        // authenticated view. Strings contain no numeric descendants and the
        // output contract preserves their escape spelling, so no second scan
        // is needed. Numeric/composite values retain the streaming normalizer.
        const bytes = (try readRaw(self, self.a, path)) orelse return null;
        if (bytes.len == 0) return null;
        if (bytes[0] == '"' or std.mem.eql(u8, bytes, "true") or std.mem.eql(u8, bytes, "false") or std.mem.eql(u8, bytes, "null"))
            return .{ .bytes = bytes, .original = bytes, .nodes = 1, .height = 1 };
        return null;
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
        if (kind == .missing) if (try readRaw(self, a, path)) |_| return sql.Datum.json(.{ .bool = false });
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
        const bytes = if (self.base_prefetched) self.prefetched.? else try self.load(if (self.req.include_all_fields) &.{} else self.req.fields);
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

test "api query contract native scalar receipts require strict nodes and retain numeric normalization" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"s","expression":{"field":"s"}}, {"name":"n","expression":{"field":"n"}}, {"name":"o","expression":{"field":"o"}} ]
    , .{});
    var plan = try projection.Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const operands = try OperandPlans.init(a, &plan);
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .authority = .{}, .req = .{}, .operands = &operands, .full_bytes = "{\"s\":\"café λ \\uD83D\\uDE80\\n\",\"n\":1e-400,\"o\":{\"x\":1e-400}}" };
    const source = row.source();
    const value = (try source.read_normalized.?(source.context, "s")).?;
    try std.testing.expectEqual(@as(usize, 1), value.nodes);
    try std.testing.expectEqual(@as(usize, 1), value.height);
    try std.testing.expect(value.bytes.ptr == (try source.read_raw.?(source.context, a, "s")).?.ptr);
    try std.testing.expect(try source.read_normalized.?(source.context, "n") == null);
    try std.testing.expect(try source.read_normalized.?(source.context, "o") == null);
    for ([_][]const u8{ "{\"s\":\"unterminated}", "{\"s\":\"a\",\"s\":\"b\"}" }) |bad| {
        var invalid: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .authority = .{}, .req = .{}, .operands = &operands, .full_bytes = bad };
        const invalid_source = invalid.source();
        const proof = invalid_source.read_normalized.?(invalid_source.context, "s") catch null;
        try std.testing.expect(proof == null);
    }
}

test "api query contract selected byte budgets admit narrow reads of large documents" {
    const directory = @import("../document_projection.zig");
    const fields = [_]directory.Field{
        .{ .name = "body", .kind = .subtree, .offset = 1, .length = 4 * 1024 * 1024, .digest = @splat(0), .index = 0 },
        .{ .name = "meta", .kind = .object, .offset = 5 * 1024 * 1024, .length = 64, .digest = @splat(0), .index = 1, .first_child = 2, .child_count = 1 },
        .{ .name = "\nrevision", .kind = .leaf, .offset = 5 * 1024 * 1024 + 16, .length = 4, .digest = @splat(0), .index = 2, .parent = 1 },
    };
    const authority: logical.Authority = .{ .proof = .{ .revision = 1, .bytes = 6 * 1024 * 1024, .kind = @import("../internal_keys.zig").primary_kind, .digest = @splat(0) }, .directory = .{ .fields = &fields, .root_count = 2, .version = 2 } };
    const selected = logical.Selection.init(authority.directory.?, &.{"meta.\nrevision"});
    try std.testing.expect(selected.rangeEligible(authority.proof.?));
    try std.testing.expect(selected.workingBytes() * 64 < 1024 * 1024);
    try std.testing.expect(selected.output_bound >= "{\"meta\":{\"\\nrevision\":\"r1\"}}".len);
    const overlapping = logical.Selection.init(authority.directory.?, &.{ "meta", "meta.\nrevision" });
    try std.testing.expectEqual(@as(u64, 64), overlapping.range_bytes);
    const opaque_selection = logical.Selection.init(authority.directory.?, &.{"body.deep.path"});
    try std.testing.expect(!opaque_selection.rangeEligible(authority.proof.?));
}
