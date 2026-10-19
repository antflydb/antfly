// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! View-resource row filters use the same compiler and canonical SQL row image
//! as Antfly mutation/overlay authorization. Only referenced output columns are
//! materialized; no stringify/parse round trip occurs on the row path.
const std = @import("std");
const sources = @import("antfly_local_sources");
const catalog = sources.sql_catalog;
const scalar = sources.sql_scalar;
const http = @import("http_server.zig");
const Pattern = sources.search_pattern_filter.PreparedPatternFilter;
const Compiled = @FieldType(Pattern, "compiled");
const A = std.mem.Allocator;

const Required = struct {
    names: std.StringHashMapUnmanaged(void) = .empty,
    all: bool = false,
    fn collect(self: *Required, a: A, filter: Compiled, depth: usize) anyerror!void {
        if (depth >= 128) return error.SqlProgramLimitExceeded;
        switch (filter) {
            .match_all, .match_none => {},
            .doc_id => try self.names.put(a, "_id", {}),
            .conjuncts, .disjuncts => |children| for (children) |child| try self.collect(a, child, depth + 1),
            .bool_query => |query| {
                for (query.must) |child| try self.collect(a, child, depth + 1);
                for (query.should) |child| try self.collect(a, child, depth + 1);
                for (query.must_not) |child| try self.collect(a, child, depth + 1);
            },
            .field_matcher => |matcher| {
                const top = switch (matcher.path) {
                    .single => |name| name,
                    .dotted, .json_pointer => |path| if (path.len == 0) "" else path[0],
                };
                if (top.len == 0) self.all = true else try self.names.put(a, top, {});
            },
        }
    }
};

const Filter = struct {
    pattern: Pattern,
    columns: []const catalog.Column,
    ordinals: []const usize,
    context: sources.api_operation.RequestContext,

    fn matches(raw: *const anyopaque, scratch: A, cells: []const scalar.Datum) !bool {
        const self: *const Filter = @ptrCast(@alignCast(raw));
        if (cells.len != self.columns.len) return error.InvalidSqlBackendResponse;
        var object: std.json.ObjectMap = .empty;
        try object.ensureTotalCapacity(scratch, @intCast(self.ordinals.len));
        var key: []const u8 = "";
        for (self.ordinals, 0..) |ordinal, position| {
            if (position % 64 == 0) try self.context.ensureActive();
            const datum = cells[ordinal];
            if (datum.sql_null) continue; // SQL NULL is absent, JSONB null exists.
            const column = self.columns[ordinal];
            const value: std.json.Value = if (datum.temporal() != null) temporal: {
                var bytes: [@import("antfly_local_sources").sql_temporal_text.output_capacity]u8 = undefined;
                break :temporal .{ .string = try scratch.dupe(u8, try scalar.formatTimestampResult(datum, column.type, &bytes)) };
            } else if (datum.numeric != null or datum.array != null)
                try sources.sql_runtime.encodeStorageDatum(scratch, datum, column, 8 * 1024 * 1024)
            else
                datum.value;
            try object.put(scratch, column.name, value);
            if (std.mem.eql(u8, column.name, "_id") and value == .string) key = value.string;
        }
        return self.pattern.matchesJson(scratch, key, .{ .object = object });
    }
};

pub fn prepare(a: A, context: sources.api_operation.RequestContext, caller: ?http.AuthenticatedIdentity, incoming: ?http.AuthenticatedIdentity, logical_view: []const u8, columns: []const catalog.Column) !?catalog.ViewRowFilter {
    const left = try http.resolveEffectiveRowFilterJson(a, caller, logical_view);
    const right = try http.resolveEffectiveRowFilterJson(a, incoming, logical_view);
    if (left == null and right == null) return null;
    const json = if (left != null and right != null and !std.mem.eql(u8, left.?, right.?)) combined: {
        const first = try std.json.parseFromSliceLeaky(std.json.Value, a, left.?, .{});
        const second = try std.json.parseFromSliceLeaky(std.json.Value, a, right.?, .{});
        break :combined try std.json.Stringify.valueAlloc(a, .{ .conjuncts = [_]std.json.Value{ first, second } }, .{});
    } else left orelse right.?;
    const filter = try a.create(Filter);
    filter.* = .{ .pattern = try Pattern.init(a, json), .columns = columns, .ordinals = &.{}, .context = context };
    var required: Required = .{};
    try required.collect(a, filter.pattern.compiled, 0);
    var ordinals: std.ArrayList(usize) = .empty;
    for (columns, 0..) |column, ordinal| if (required.all or required.names.contains(column.name))
        try ordinals.append(a, ordinal);
    filter.ordinals = try ordinals.toOwnedSlice(a);
    return .{ .ptr = filter, .matches = Filter.matches };
}

test "SQL view filters preserve wide integers and SQL versus JSON null without reading cold outputs" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const region = arena.allocator();
    const usermgr = @import("../usermgr/mod.zig");
    var filters = [_]usermgr.RowFilterEntry{.{
        .table = @constCast("*"),
        .filter = @constCast("{\"conjuncts\":[{\"term\":{\"n\":9007199254740993}},{\"exists\":{\"field\":\"json_value\"}},{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"sql_value\"}}]}}]}"),
    }};
    const identity: http.AuthenticatedIdentity = .{ .username = @constCast("reader"), .row_filter = &filters };
    const columns = [_]catalog.Column{
        .{ .name = "n", .path = "n", .type = .integer },
        .{ .name = "json_value", .path = "json_value", .type = .json },
        .{ .name = "sql_value", .path = "sql_value", .type = .string },
        .{ .name = "cold_temporal", .path = "cold_temporal", .type = .datetime },
    };
    const filter = (try prepare(region, .{}, identity, identity, "stored", &columns)).?;
    const owner: *const Filter = @ptrCast(@alignCast(filter.ptr));
    try std.testing.expectEqual(@as(usize, 3), owner.ordinals.len);
    var cells = [_]scalar.Datum{ scalar.Datum.json(.{ .integer = 9007199254740993 }), scalar.Datum.json(.null), .{}, .{} };
    try std.testing.expect(try filter.matches(filter.ptr, region, &cells));
    cells[0].value.integer -= 1;
    try std.testing.expect(!try filter.matches(filter.ptr, region, &cells));
    cells[0].value.integer += 1;
    cells[1] = .{};
    try std.testing.expect(!try filter.matches(filter.ptr, region, &cells));
}
