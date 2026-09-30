// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Bounded index-order fan-in. Every owner is checked before exposing output;
//! primary-range concatenation is never valid for a secondary ordered index.
//! Keeps only the best K rows, plus one incoming line. No retained cursors.
const std = @import("std");
const cursor = @import("../storage/db/relational_row_cursor.zig");
const Allocator = std.mem.Allocator;
const max_bytes = 16 * 1024 * 1024;

pub fn isIndexQuery(alloc: Allocator, json: []const u8) !bool {
    if (json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(struct { index: ?[]const u8 = null }, alloc, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return parsed.value.index != null;
}

pub fn isIndexScan(alloc: Allocator, opts: @import("../storage/db/types.zig").ScanOptions) !bool {
    if (opts.relational_query) |query| return query.index != null;
    return isIndexQuery(alloc, opts.relational_query_json);
}

const Row = struct {
    line: []u8,
    token: []u8,

    fn deinit(row: Row, alloc: Allocator) void {
        alloc.free(row.line);
        alloc.free(row.token);
    }

    fn compare(_: void, a: Row, b: Row) std.math.Order {
        // Max heap: the worst retained row is the first replacement candidate.
        return std.mem.order(u8, b.token, a.token);
    }
};

pub const Merger = struct {
    alloc: Allocator,
    limit: u32,
    rows: std.PriorityQueue(Row, void, Row.compare),
    bytes: usize = 0,
    input: std.ArrayList(u8) = .empty,
    previous: std.ArrayList(u8) = .empty,
    header: ?[cursor.identity_len * 2]u8 = null,
    group_rows: usize = 0,

    pub fn init(alloc: Allocator, limit: u32) !Merger {
        if (limit == 0 or limit > 4096) return error.InvalidRelationalRowsRequest;
        return .{ .alloc = alloc, .limit = limit, .rows = .initContext({}) };
    }

    pub fn deinit(self: *Merger) void {
        while (self.rows.pop()) |row| row.deinit(self.alloc);
        self.rows.deinit(self.alloc);
        self.input.deinit(self.alloc);
        self.previous.deinit(self.alloc);
    }

    pub fn beginGroup(self: *Merger) void {
        std.debug.assert(self.input.items.len == 0);
        self.previous.clearRetainingCapacity();
        self.group_rows = 0;
    }

    pub fn write(self: *Merger, bytes: []const u8) !void {
        var remaining = bytes;
        while (std.mem.indexOfScalar(u8, remaining, '\n')) |end| {
            if (end > max_bytes -| self.input.items.len) return error.RelationalRowsOutputBudgetExceeded;
            try self.input.appendSlice(self.alloc, remaining[0..end]);
            try self.addLine(self.input.items);
            self.input.clearRetainingCapacity();
            remaining = remaining[end + 1 ..];
        }
        if (remaining.len > max_bytes -| self.input.items.len) return error.RelationalRowsOutputBudgetExceeded;
        try self.input.appendSlice(self.alloc, remaining);
    }

    pub fn endGroup(self: *Merger) !void {
        if (self.input.items.len != 0) return error.InvalidRemoteResponse;
    }

    fn addLine(self: *Merger, line: []const u8) !void {
        if (line.len == 0 or self.group_rows >= self.limit) return error.InvalidRemoteResponse;
        var parsed = std.json.parseFromSlice(struct { cursor: []const u8, schema_version: u32 }, self.alloc, line, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidRemoteResponse,
        };
        defer parsed.deinit();
        const token = parsed.value.cursor;
        cursor.validate(token) catch return error.InvalidRemoteResponse;
        var schema: [4]u8 = undefined;
        _ = std.fmt.hexToBytes(&schema, token[0..8]) catch return error.InvalidRemoteResponse;
        if (std.mem.readInt(u32, &schema, .big) != parsed.value.schema_version) return error.InvalidRemoteResponse;
        const header = token[0 .. cursor.identity_len * 2];
        if (self.header) |expected| {
            if (!std.mem.eql(u8, &expected, header)) return error.PreparedGenerationChanged;
        } else self.header = header[0 .. cursor.identity_len * 2].*;
        if (self.previous.items.len != 0 and std.mem.order(u8, self.previous.items, token) != .lt) return error.InvalidRemoteResponse;
        self.previous.clearRetainingCapacity();
        try self.previous.appendSlice(self.alloc, token);
        self.group_rows += 1;
        if (self.rows.items.len == self.limit) {
            const worst = self.rows.peek().?;
            if (std.mem.order(u8, token, worst.token) != .lt) return;
        }
        const outgoing = if (self.rows.items.len == self.limit) self.rows.peek().?.line.len + self.rows.peek().?.token.len else 0;
        if (line.len + token.len > max_bytes -| (self.bytes - outgoing)) return error.RelationalRowsOutputBudgetExceeded;
        const owned_line = try self.alloc.dupe(u8, line);
        errdefer self.alloc.free(owned_line);
        const owned_token = try self.alloc.dupe(u8, token);
        errdefer self.alloc.free(owned_token);
        try self.rows.ensureUnusedCapacity(self.alloc, 1);
        if (self.rows.items.len == self.limit) {
            const removed = self.rows.pop().?;
            self.bytes -= removed.line.len + removed.token.len;
            removed.deinit(self.alloc);
        }
        try self.rows.push(self.alloc, .{ .line = owned_line, .token = owned_token });
        self.bytes += line.len + token.len;
    }

    pub fn finishAlloc(self: *Merger) ![]u8 {
        try self.endGroup();
        const ordered = try self.alloc.alloc(Row, self.rows.items.len);
        defer self.alloc.free(ordered);
        var remaining = ordered.len;
        while (self.rows.pop()) |row| {
            remaining -= 1;
            ordered[remaining] = row;
        }
        defer for (ordered) |row| row.deinit(self.alloc);
        var result = std.ArrayList(u8).empty;
        errdefer result.deinit(self.alloc);
        for (ordered, 0..) |row, i| {
            if (i != 0 and std.mem.eql(u8, row.token, ordered[i - 1].token)) return error.TopologyChanged;
            try result.appendSlice(self.alloc, row.line);
            try result.append(self.alloc, '\n');
        }
        self.bytes = 0;
        return try result.toOwnedSlice(self.alloc);
    }
};

test "relational row query fan-in globally orders owner prefixes and keeps bounded top K" {
    const alloc = std.testing.allocator;
    var merge = try Merger.init(alloc, 2);
    defer merge.deinit();
    const identity = cursor.identity(1, "by_id", .{0} ** 32);
    for ([_][]const u8{ "bz", "ac", "dy" }) |keys| {
        merge.beginGroup();
        for (keys) |key| {
            const token = try cursor.encode(alloc, identity, &.{key});
            defer alloc.free(token);
            const line = try std.fmt.allocPrint(alloc, "{{\"_id\":\"{c}\",\"schema_version\":1,\"cursor\":\"{s}\"}}\n", .{ key, token });
            defer alloc.free(line);
            try merge.write(line[0..7]);
            try merge.write(line[7..]);
        }
        try merge.endGroup();
        try std.testing.expectEqual(@as(usize, 2), merge.rows.items.len);
    }
    const result = try merge.finishAlloc();
    defer alloc.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"_id\":\"a\""));
    try std.testing.expect(std.mem.indexOf(u8, result, "{\"_id\":\"b\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "{\"_id\":\"c\"") == null);
}

test "relational row query fan-in rejects changed logical index duplicate ownership and malformed streams" {
    const alloc = std.testing.allocator;
    const first = try cursor.encode(alloc, cursor.identity(1, "by_id", .{0} ** 32), "a");
    defer alloc.free(first);
    const changed = try cursor.encode(alloc, cursor.identity(1, "by_other_id", .{0} ** 32), "b");
    defer alloc.free(changed);
    const first_line = try std.fmt.allocPrint(alloc, "{{\"schema_version\":1,\"cursor\":\"{s}\"}}\n", .{first});
    defer alloc.free(first_line);
    const changed_line = try std.fmt.allocPrint(alloc, "{{\"schema_version\":1,\"cursor\":\"{s}\"}}\n", .{changed});
    defer alloc.free(changed_line);
    {
        var merge = try Merger.init(alloc, 3);
        defer merge.deinit();
        merge.beginGroup();
        try merge.write(first_line);
        try merge.endGroup();
        merge.beginGroup();
        try std.testing.expectError(error.PreparedGenerationChanged, merge.write(changed_line));
    }
    {
        var merge = try Merger.init(alloc, 3);
        defer merge.deinit();
        merge.beginGroup();
        try merge.write(first_line);
        try merge.endGroup();
        merge.beginGroup();
        try merge.write(first_line);
        try std.testing.expectError(error.TopologyChanged, merge.finishAlloc());
    }
    {
        var merge = try Merger.init(alloc, 3);
        defer merge.deinit();
        merge.beginGroup();
        try merge.write(first_line[0 .. first_line.len - 1]);
        try std.testing.expectError(error.InvalidRemoteResponse, merge.endGroup());
    }
}
