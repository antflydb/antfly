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

const std = @import("std");

const db_mod = @import("../storage/db/mod.zig");
const document_plan = @import("document_plan.zig");
const document_runtime = @import("document_runtime.zig");

pub fn aggregateFromDbAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    req: document_runtime.AlgebraicAggregateRequest,
) !document_runtime.AlgebraicAggregateResponse {
    const entry = db.core.index_manager.algebraicIndex(req.index_name) orelse return error.DocumentSqlIndexUnavailable;

    if (req.group_by != null) {
        const entries = try entry.index.scanMaterializedExpressionEntriesForMaterialization(db.core.store, req.materialization_name);
        defer {
            for (entries) |*fold| fold.deinit(entry.index.alloc);
            entry.index.alloc.free(entries);
        }
        const output_count = if (req.limit) |limit| @min(entries.len, limit) else entries.len;
        const rows = try alloc.alloc(document_runtime.AlgebraicAggregateRow, output_count);
        errdefer alloc.free(rows);
        var initialized: usize = 0;
        errdefer {
            for (rows[0..initialized]) |*row| row.deinit(alloc);
        }
        for (entries[0..output_count], 0..) |fold, i| {
            rows[i] = .{
                .group_json = try singleGroupJsonAlloc(alloc, &entry.index, fold.group_key),
                .value_json = try aggregateValueJsonAlloc(alloc, req.aggregate_op, fold.value),
                .raw_value = try alloc.dupe(u8, fold.value),
            };
            initialized += 1;
        }
        return .{
            .rows = rows,
            .total_groups = @intCast(entries.len),
        };
    }

    const empty_group = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{});
    defer alloc.free(empty_group);
    const raw = try entry.index.rawValueAlloc(db.core.store, req.materialization_name, empty_group);
    defer if (raw) |value| entry.index.alloc.free(value);
    const rows = try alloc.alloc(document_runtime.AlgebraicAggregateRow, 1);
    errdefer alloc.free(rows);
    rows[0] = .{
        .value_json = if (raw) |value|
            try aggregateValueJsonAlloc(alloc, req.aggregate_op, value)
        else
            try aggregateMissingValueJsonAlloc(alloc, req.aggregate_op),
        .raw_value = if (raw) |value| try alloc.dupe(u8, value) else null,
    };
    return .{
        .rows = rows,
        .total_groups = 1,
    };
}

fn singleGroupJsonAlloc(
    alloc: std.mem.Allocator,
    index: *const db_mod.algebraic.index.Index,
    group_key: []const u8,
) ![]u8 {
    const component = try db_mod.algebraic.token.componentAt(group_key, 0);
    if (component.next != group_key.len) return error.InvalidRowsRequest;
    return try index.scalarTokenJsonAlloc(alloc, component.payload);
}

fn aggregateMissingValueJsonAlloc(
    alloc: std.mem.Allocator,
    op: document_plan.DocumentAggregateOp,
) ![]u8 {
    return switch (op) {
        .count => try alloc.dupe(u8, "0"),
        .sum, .avg, .min, .max => try alloc.dupe(u8, "null"),
    };
}

fn aggregateValueJsonAlloc(
    alloc: std.mem.Allocator,
    op: document_plan.DocumentAggregateOp,
    raw: []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    switch (op) {
        .count => try writer.print("{d}", .{try db_mod.algebraic.algebra.parseI64(raw)}),
        .sum, .min, .max => try writer.print("{d}", .{try db_mod.algebraic.algebra.parseF64(raw)}),
        .avg => {
            const avg = try db_mod.algebraic.algebra.parseAvg(raw);
            if (avg.count == 0) {
                try writer.writeAll("null");
            } else {
                try writer.print("{d}", .{avg.sum / @as(f64, @floatFromInt(avg.count))});
            }
        },
    }
    return try out.toOwnedSlice();
}

const MergedGroup = struct {
    group_json: ?[]u8 = null,
    raw_value: ?[]u8 = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.group_json) |value| alloc.free(value);
        if (self.raw_value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn mergeResponsesAlloc(
    alloc: std.mem.Allocator,
    req: document_runtime.AlgebraicAggregateRequest,
    responses: []const document_runtime.AlgebraicAggregateResponse,
) !document_runtime.AlgebraicAggregateResponse {
    var groups = std.ArrayListUnmanaged(MergedGroup).empty;
    defer {
        for (groups.items) |*group| group.deinit(alloc);
        groups.deinit(alloc);
    }

    for (responses) |response| {
        for (response.rows) |row| {
            if (row.raw_value == null and !missingRawValueAllowed(req, row.value_json)) {
                return error.UnsupportedQueryRequest;
            }
            try mergeRowAlloc(alloc, &groups, req.aggregate_op, row.group_json, row.raw_value);
        }
    }

    const output_count = if (req.limit) |limit| @min(groups.items.len, limit) else groups.items.len;
    const rows = try alloc.alloc(document_runtime.AlgebraicAggregateRow, output_count);
    errdefer alloc.free(rows);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |*row| row.deinit(alloc);
    }

    for (groups.items[0..output_count], 0..) |group, i| {
        rows[i] = .{
            .group_json = if (group.group_json) |value| try alloc.dupe(u8, value) else null,
            .value_json = if (group.raw_value) |value|
                try aggregateValueJsonAlloc(alloc, req.aggregate_op, value)
            else
                try aggregateMissingValueJsonAlloc(alloc, req.aggregate_op),
            .raw_value = if (group.raw_value) |value| try alloc.dupe(u8, value) else null,
        };
        initialized += 1;
    }

    return .{
        .rows = rows,
        .total_groups = @intCast(groups.items.len),
    };
}

fn missingRawValueAllowed(req: document_runtime.AlgebraicAggregateRequest, value_json: []const u8) bool {
    if (req.group_by != null) return false;
    return switch (req.aggregate_op) {
        .count => std.mem.eql(u8, value_json, "0"),
        .sum, .avg, .min, .max => std.mem.eql(u8, value_json, "null"),
    };
}

fn mergeRowAlloc(
    alloc: std.mem.Allocator,
    groups: *std.ArrayListUnmanaged(MergedGroup),
    op: document_plan.DocumentAggregateOp,
    group_json: ?[]const u8,
    raw_value: ?[]const u8,
) !void {
    for (groups.items) |*group| {
        if (optionalStringsEqual(group.group_json, group_json)) {
            if (raw_value) |right| {
                if (group.raw_value) |left| {
                    const merged = try mergeRawValueAlloc(alloc, op, left, right);
                    alloc.free(left);
                    group.raw_value = merged;
                } else {
                    group.raw_value = try alloc.dupe(u8, right);
                }
            } else if (op == .count and group.raw_value == null) {
                group.raw_value = try db_mod.algebraic.algebra.encodeI64Alloc(alloc, 0);
            }
            return;
        }
    }

    try groups.append(alloc, .{
        .group_json = if (group_json) |value| try alloc.dupe(u8, value) else null,
        .raw_value = if (raw_value) |value| try alloc.dupe(u8, value) else if (op == .count)
            try db_mod.algebraic.algebra.encodeI64Alloc(alloc, 0)
        else
            null,
    });
}

fn optionalStringsEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return std.mem.eql(u8, left.?, right.?);
}

fn mergeRawValueAlloc(
    alloc: std.mem.Allocator,
    op: document_plan.DocumentAggregateOp,
    left: []const u8,
    right: []const u8,
) ![]u8 {
    return switch (op) {
        .count => try db_mod.algebraic.algebra.encodeI64Alloc(
            alloc,
            try db_mod.algebraic.algebra.parseI64(left) + try db_mod.algebraic.algebra.parseI64(right),
        ),
        .sum => try db_mod.algebraic.algebra.encodeF64Alloc(
            alloc,
            try db_mod.algebraic.algebra.parseF64(left) + try db_mod.algebraic.algebra.parseF64(right),
        ),
        .min => try db_mod.algebraic.algebra.encodeF64Alloc(
            alloc,
            @min(try db_mod.algebraic.algebra.parseF64(left), try db_mod.algebraic.algebra.parseF64(right)),
        ),
        .max => try db_mod.algebraic.algebra.encodeF64Alloc(
            alloc,
            @max(try db_mod.algebraic.algebra.parseF64(left), try db_mod.algebraic.algebra.parseF64(right)),
        ),
        .avg => blk: {
            const left_avg = try db_mod.algebraic.algebra.parseAvg(left);
            const right_avg = try db_mod.algebraic.algebra.parseAvg(right);
            break :blk try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{
                .sum = left_avg.sum + right_avg.sum,
                .count = left_avg.count + right_avg.count,
            });
        },
    };
}

test "document algebraic aggregate fan-in merges raw grouped averages before applying limit" {
    const alloc = std.testing.allocator;
    const left_raw = try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{ .sum = 10, .count = 1 });
    defer alloc.free(left_raw);
    const right_raw = try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{ .sum = 12, .count = 1 });
    defer alloc.free(right_raw);
    var left_rows = [_]document_runtime.AlgebraicAggregateRow{.{
        .group_json = try alloc.dupe(u8, "\"active\""),
        .value_json = try alloc.dupe(u8, "10"),
        .raw_value = try alloc.dupe(u8, left_raw),
    }};
    defer left_rows[0].deinit(alloc);
    var right_rows = [_]document_runtime.AlgebraicAggregateRow{
        .{
            .group_json = try alloc.dupe(u8, "\"active\""),
            .value_json = try alloc.dupe(u8, "12"),
            .raw_value = try alloc.dupe(u8, right_raw),
        },
        .{
            .group_json = try alloc.dupe(u8, "\"archived\""),
            .value_json = try alloc.dupe(u8, "30"),
            .raw_value = try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{ .sum = 30, .count = 1 }),
        },
    };
    defer {
        right_rows[0].deinit(alloc);
        right_rows[1].deinit(alloc);
    }
    const responses = [_]document_runtime.AlgebraicAggregateResponse{
        .{ .rows = left_rows[0..], .total_groups = 1 },
        .{ .rows = right_rows[0..], .total_groups = 2 },
    };
    var merged = try mergeResponsesAlloc(alloc, .{
        .index_name = "amount_alg",
        .materialization_name = "avg_by_status",
        .aggregate_op = .avg,
        .group_by = .{ .field = "status", .source_field = "status", .field_type = .keyword, .output = "status" },
        .limit = 1,
    }, responses[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), merged.total_groups);
    try std.testing.expectEqual(@as(usize, 1), merged.rows.len);
    try std.testing.expectEqualStrings("\"active\"", merged.rows[0].group_json.?);
    try std.testing.expectEqualStrings("11", merged.rows[0].value_json);
}

test "document algebraic aggregate fan-in preserves empty scalar aggregate semantics" {
    const alloc = std.testing.allocator;
    var empty_sum_rows = [_]document_runtime.AlgebraicAggregateRow{
        .{ .value_json = try alloc.dupe(u8, "null"), .raw_value = null },
        .{ .value_json = try alloc.dupe(u8, "null"), .raw_value = null },
    };
    defer {
        empty_sum_rows[0].deinit(alloc);
        empty_sum_rows[1].deinit(alloc);
    }
    const empty_sum_responses = [_]document_runtime.AlgebraicAggregateResponse{
        .{ .rows = empty_sum_rows[0..1], .total_groups = 1 },
        .{ .rows = empty_sum_rows[1..2], .total_groups = 1 },
    };
    var merged_empty_sum = try mergeResponsesAlloc(alloc, .{
        .index_name = "amount_alg",
        .materialization_name = "sum_all",
        .aggregate_op = .sum,
        .group_by = null,
        .limit = null,
    }, empty_sum_responses[0..]);
    defer merged_empty_sum.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged_empty_sum.rows.len);
    try std.testing.expectEqualStrings("null", merged_empty_sum.rows[0].value_json);
    try std.testing.expect(merged_empty_sum.rows[0].raw_value == null);

    var empty_count_rows = [_]document_runtime.AlgebraicAggregateRow{
        .{ .value_json = try alloc.dupe(u8, "0"), .raw_value = null },
        .{ .value_json = try alloc.dupe(u8, "0"), .raw_value = null },
    };
    defer {
        empty_count_rows[0].deinit(alloc);
        empty_count_rows[1].deinit(alloc);
    }
    const empty_count_responses = [_]document_runtime.AlgebraicAggregateResponse{
        .{ .rows = empty_count_rows[0..1], .total_groups = 1 },
        .{ .rows = empty_count_rows[1..2], .total_groups = 1 },
    };
    var merged_empty_count = try mergeResponsesAlloc(alloc, .{
        .index_name = "amount_alg",
        .materialization_name = "count_all",
        .aggregate_op = .count,
        .group_by = null,
        .limit = null,
    }, empty_count_responses[0..]);
    defer merged_empty_count.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged_empty_count.rows.len);
    try std.testing.expectEqualStrings("0", merged_empty_count.rows[0].value_json);
}
