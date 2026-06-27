// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const relational_rows_api = @import("../relational_rows.zig");
const runtime_schema = @import("../../storage/schema.zig");
const sql_adapter = @import("../../sql/mod.zig");
const table_reads = @import("../table_reads.zig");

pub const ReadResult = struct {
    result: table_reads.LoweredSqlReadPlanResult,
    columns: []const runtime_schema.RelationalColumn = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.result.deinit(alloc);
        relational_rows_api.freeRowsOutputColumns(alloc, self.columns);
        self.* = undefined;
    }
};

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    sessions: std.AutoHashMapUnmanaged(u64, Session) = .empty,

    const Session = struct {
        portals: std.StringHashMapUnmanaged(Portal) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            var it = self.portals.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                entry.value_ptr.deinit(alloc);
            }
            self.portals.deinit(alloc);
            self.* = undefined;
        }

        fn clear(self: *@This(), alloc: std.mem.Allocator) void {
            var it = self.portals.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                entry.value_ptr.deinit(alloc);
            }
            self.portals.clearRetainingCapacity();
        }
    };

    const Portal = struct {
        rows: [][]const u8,
        columns: []const runtime_schema.RelationalColumn,
        position: usize = 0,
        scroll: sql_adapter.CursorScrollMode = .default,
        hold: bool = false,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            for (self.rows) |row| alloc.free(@constCast(row));
            if (self.rows.len > 0) alloc.free(self.rows);
            relational_rows_api.freeRowsOutputColumns(alloc, self.columns);
            self.* = undefined;
        }
    };

    pub fn init(alloc: std.mem.Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(self.alloc);
        self.sessions.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn declareReadPortal(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.DeclareCursorPortalPlan,
        read_result: *table_reads.LoweredSqlReadPlanResult,
        columns_ptr: *[]const runtime_schema.RelationalColumn,
    ) !void {
        if (session_id == 0) return error.InvalidSqlSession;
        const result = try self.sessions.getOrPut(self.alloc, session_id);
        if (!result.found_existing) result.value_ptr.* = .{};
        const session = result.value_ptr;
        if (session.portals.contains(plan.portal_name)) return error.CursorPortalAlreadyExists;

        const key = try self.alloc.dupe(u8, plan.portal_name);
        errdefer self.alloc.free(key);
        const taken = table_reads.takeLoweredSqlReadRows(read_result);
        const columns = columns_ptr.*;
        columns_ptr.* = &.{};
        errdefer {
            for (taken.rows) |row| self.alloc.free(@constCast(row));
            if (taken.rows.len > 0) self.alloc.free(taken.rows);
            relational_rows_api.freeRowsOutputColumns(self.alloc, columns);
        }
        try session.portals.put(self.alloc, key, .{
            .rows = taken.rows,
            .columns = columns,
            .scroll = plan.scroll,
            .hold = plan.hold,
        });
    }

    pub fn close(self: *@This(), session_id: u64, plan: sql_adapter.CloseCursorPortalPlan) !void {
        if (session_id == 0) return error.InvalidSqlSession;
        const session = self.sessions.getPtr(session_id) orelse {
            if (plan.all) return;
            return error.CursorPortalNotFound;
        };
        if (plan.all) {
            session.clear(self.alloc);
            return;
        }
        const name = plan.portal_name orelse return error.UnsupportedSqlShape;
        if (session.portals.fetchRemove(name)) |removed| {
            self.alloc.free(removed.key);
            var value = removed.value;
            value.deinit(self.alloc);
            return;
        }
        return error.CursorPortalNotFound;
    }

    pub fn closeTransactionPortals(self: *@This(), session_id: u64) !void {
        if (session_id == 0) return;
        const session = self.sessions.getPtr(session_id) orelse return;
        var close_names: std.ArrayListUnmanaged([]const u8) = .empty;
        defer close_names.deinit(self.alloc);

        var it = session.portals.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.hold) try close_names.append(self.alloc, entry.key_ptr.*);
        }
        for (close_names.items) |name| {
            if (session.portals.fetchRemove(name)) |removed| {
                self.alloc.free(removed.key);
                var value = removed.value;
                value.deinit(self.alloc);
            }
        }
    }

    pub fn fetch(self: *@This(), session_id: u64, plan: sql_adapter.FetchCursorPortalPlan) !ReadResult {
        const portal_value = try self.portal(session_id, plan.portal_name);
        return try self.fetchFromPortal(portal_value, plan, true);
    }

    pub fn move(self: *@This(), session_id: u64, plan: sql_adapter.FetchCursorPortalPlan) !usize {
        const portal_value = try self.portal(session_id, plan.portal_name);
        var read = try self.fetchFromPortal(portal_value, plan, false);
        defer read.deinit(self.alloc);
        return switch (read.result) {
            .query => |query| query.rows.len,
            else => 0,
        };
    }

    pub fn portalCountForTest(self: *@This(), session_id: u64) usize {
        const session = self.sessions.getPtr(session_id) orelse return 0;
        return session.portals.count();
    }

    fn portal(self: *@This(), session_id: u64, portal_name: []const u8) !*Portal {
        if (session_id == 0) return error.InvalidSqlSession;
        const session = self.sessions.getPtr(session_id) orelse return error.CursorPortalNotFound;
        return session.portals.getPtr(portal_name) orelse error.CursorPortalNotFound;
    }

    fn fetchFromPortal(
        self: *@This(),
        portal_value: *Portal,
        plan: sql_adapter.FetchCursorPortalPlan,
        include_rows: bool,
    ) !ReadResult {
        const bounds = cursorFetchBounds(portal_value.rows.len, portal_value.position, plan);
        portal_value.position = bounds.next_position;
        const row_count = bounds.end - bounds.start;
        const rows = try self.alloc.alloc([]const u8, if (include_rows) row_count else 0);
        var initialized: usize = 0;
        errdefer {
            for (rows[0..initialized]) |row| self.alloc.free(@constCast(row));
            self.alloc.free(rows);
        }
        if (include_rows) {
            if (bounds.reverse) {
                var source_index = bounds.end;
                while (source_index > bounds.start) {
                    source_index -= 1;
                    rows[initialized] = try self.alloc.dupe(u8, portal_value.rows[source_index]);
                    initialized += 1;
                }
            } else {
                for (portal_value.rows[bounds.start..bounds.end]) |row| {
                    rows[initialized] = try self.alloc.dupe(u8, row);
                    initialized += 1;
                }
            }
        }
        const columns = try cloneColumnsAlloc(self.alloc, portal_value.columns);
        errdefer relational_rows_api.freeRowsOutputColumns(self.alloc, columns);
        return .{
            .result = .{ .query = .{ .rows = rows, .total = @intCast(row_count) } },
            .columns = columns,
        };
    }
};

const CursorFetchBounds = struct {
    start: usize,
    end: usize,
    next_position: usize,
    reverse: bool = false,
};

fn cursorFetchBounds(row_len: usize, position: usize, plan: sql_adapter.FetchCursorPortalPlan) CursorFetchBounds {
    const current = @min(position, row_len);
    switch (plan.direction) {
        .next => {
            const end = @min(current + 1, row_len);
            return .{ .start = current, .end = end, .next_position = end };
        },
        .prior => {
            if (current == 0) return .{ .start = 0, .end = 0, .next_position = 0, .reverse = true };
            const start = current - 1;
            return .{ .start = start, .end = current, .next_position = start, .reverse = true };
        },
        .first => {
            const end = @min(@as(usize, 1), row_len);
            return .{ .start = 0, .end = end, .next_position = end };
        },
        .last => {
            if (row_len == 0) return .{ .start = 0, .end = 0, .next_position = 0 };
            return .{ .start = row_len - 1, .end = row_len, .next_position = row_len };
        },
        .forward => {
            const count = cursorFetchCount(plan.count, row_len - current);
            const end = @min(current + count, row_len);
            return .{ .start = current, .end = end, .next_position = end };
        },
        .backward => {
            const count = cursorFetchCount(plan.count, current);
            const start = current - count;
            return .{ .start = start, .end = current, .next_position = start, .reverse = true };
        },
        .all => return .{ .start = current, .end = row_len, .next_position = row_len },
        .absolute => {
            const target = cursorAbsoluteIndex(plan.count orelse 1, row_len);
            if (target >= row_len) return .{ .start = row_len, .end = row_len, .next_position = row_len };
            return .{ .start = target, .end = target + 1, .next_position = target + 1 };
        },
        .relative => {
            const offset = plan.count orelse 1;
            const target = cursorRelativeIndex(current, offset, row_len);
            if (target >= row_len) return .{ .start = row_len, .end = row_len, .next_position = row_len };
            return .{ .start = target, .end = target + 1, .next_position = target + 1 };
        },
    }
}

fn cursorFetchCount(raw: ?i64, max_count: usize) usize {
    const value = raw orelse 1;
    if (value <= 0) return 0;
    return @min(@as(usize, @intCast(value)), max_count);
}

fn cursorAbsoluteIndex(raw: i64, row_len: usize) usize {
    if (raw == 0) return row_len;
    if (raw > 0) return @as(usize, @intCast(raw - 1));
    const back = @as(usize, @intCast(-raw));
    if (back > row_len) return row_len;
    return row_len - back;
}

fn cursorRelativeIndex(position: usize, offset: i64, row_len: usize) usize {
    if (offset >= 0) {
        const forward = @as(usize, @intCast(offset));
        return @min(position + forward, row_len);
    }
    const back = @as(usize, @intCast(-offset));
    if (back > position) return row_len;
    return position - back;
}

fn cloneColumnsAlloc(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) ![]const runtime_schema.RelationalColumn {
    var owned: std.ArrayListUnmanaged(runtime_schema.RelationalColumn) = .empty;
    errdefer {
        for (owned.items) |column| {
            alloc.free(column.name);
            alloc.free(column.path);
            if (column.collation) |collation| alloc.free(collation);
        }
        owned.deinit(alloc);
    }
    for (columns) |column| {
        const name = try alloc.dupe(u8, column.name);
        errdefer alloc.free(name);
        const path = try alloc.dupe(u8, column.path);
        errdefer alloc.free(path);
        const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
        errdefer if (collation) |value| alloc.free(value);
        var cloned = column;
        cloned.name = name;
        cloned.path = path;
        cloned.collation = collation;
        try owned.append(alloc, cloned);
    }
    return try owned.toOwnedSlice(alloc);
}
