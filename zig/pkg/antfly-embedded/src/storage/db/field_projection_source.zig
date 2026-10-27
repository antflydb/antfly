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
pub const Row = struct {
    a: A,
    txn: ?*@import("../docstore.zig").DocStore.Txn,
    physical: []const u8,
    id: []const u8,
    authority: conditional.Authority,
    req: @import("types.zig").SearchRequest,
    values: std.StringHashMapUnmanaged(V) = .empty,
    identities: std.StringHashMapUnmanaged(sql.Datum) = .empty,
    metadata_hits: usize = 0,
    value_reads: usize = 0,
    full_bytes: ?[]const u8 = null,
    full_root: ?V = null,
    pub fn source(self: *Row) projection.Source {
        return .{ .context = self, .read = read };
    }
    fn load(self: *Row, roots: []const []const u8) ![]const u8 {
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        if (self.full_bytes) |bytes| return bytes;
        var scope = try (self.txn orelse return error.DocumentNotFound).openReadScope(self.a);
        defer scope.close();
        var authority = self.authority.source;
        const selective = roots.len != 0 and roots.len <= 64 and for (roots) |path| {
            if (!projection.validPath(path)) break false;
        } else true;
        if (!selective) authority.directory = null;
        const rows = try logical.loadPrepared(self.a, self.a, &scope, &.{self.physical}, &.{self.id}, if (selective) roots else &.{}, &.{authority});
        self.value_reads += 1;
        const bytes = rows[0].bytes orelse return error.DocumentNotFound;
        if (!rows[0].projected) self.full_bytes = bytes;
        return bytes;
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
            if (directory.resolve(path)) |field| {
                // Opaque subtrees do not prove that a descendant exists.
                if (field.kind != .subtree) {
                    if (kind == .missing) {
                        self.metadata_hits += 1;
                        return sql.Datum.json(.{ .bool = false });
                    }
                    if (field.kind == .leaf) if (self.authority.certificate) |certificate| if (certificate.find(field.index)) |digest| {
                        const value = sql.Datum.json(.{ .string = try std.fmt.allocPrint(a, "sv1-{s}", .{std.fmt.bytesToHex(digest, .lower)}) });
                        try self.identities.put(self.a, path, value);
                        self.metadata_hits += 1;
                        return value;
                    };
                }
            } else if (directory.version == 2) {
                self.metadata_hits += 1;
                return if (kind == .missing) sql.Datum.json(.{ .bool = true }) else .{};
            }
        };
        const root = self.full_root orelse self.values.get(path) orelse loaded: {
            const bytes = try self.load(&.{path});
            // Normalize stored wrappers/private fields through the same public
            // projection rules before resolving any scalar operand.
            const public = try query.lookupJsonWithOutput(self.a, self.a, bytes, .{ .fields = if (self.full_bytes != null) &.{} else &.{path}, .include_all_fields = self.full_bytes != null });
            const value = try std.json.parseFromSliceLeaky(V, self.a, public.json, .{ .allocate = .alloc_always });
            if (self.full_bytes != null) self.full_root = value else try self.values.put(self.a, path, value);
            break :loaded value;
        };
        var source_value: projection.JsonSource = .{ .id = self.id, .root = root };
        return source_value.source().read(&source_value, a, path, kind);
    }
    pub fn base(self: *Row, output: A) ![]u8 {
        if (!self.req.include_all_fields and self.req.fields.len == 0) return output.dupe(u8, "{}");
        const bytes = try self.load(if (self.req.include_all_fields) &.{} else self.req.fields);
        return (try query.lookupJsonWithOutput(self.a, output, bytes, .{ .fields = self.req.fields, .include_all_fields = self.req.include_all_fields })).json;
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
