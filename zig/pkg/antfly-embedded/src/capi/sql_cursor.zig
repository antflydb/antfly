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

const h = @import("handles.zig");
const api = @import("db.zig");
const sql = @import("sql.zig");
const dependencies = h.antfly.capi_dependencies;
const std = h.std;
const Request = struct { statement: []const u8, parameters: []const std.json.Value = &.{}, session_id: ?u64 = null };
pub const Cursor = struct {
    session_id: ?u64 = null,
    alloc: std.mem.Allocator,
    budget: dependencies.sql_memory_budget,
    arena: std.heap.ArenaAllocator,
    compiled: sql.compiler.Compiled,
    adapter: sql.Adapter(h.antfly),
    stream: *dependencies.sql_read_stream.Stream,
    lease: @import("../storage/db/row_policy_gate.zig").Gate.Lease,
    fn close(self: *Cursor) void {
        self.stream.close();
        self.compiled.deinit();
        self.lease.release();
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};
pub fn closeAll(handle: *h.Handle) void {
    var iterator = handle.sql_cursors.valueIterator();
    while (iterator.next()) |cursor| cursor.*.close();
    handle.sql_cursors.deinit(handle.alloc);
}
pub fn closeSession(handle: *h.Handle, id: u64) void {
    while (true) {
        var iterator = handle.sql_cursors.iterator();
        const key = while (iterator.next()) |entry| {
            if (entry.value_ptr.*.session_id == id) break entry.key_ptr.*;
        } else break;
        handle.sql_cursors.fetchRemove(key).?.value.close();
    }
}
pub fn diagnostic(err: anyerror, out: *h.capi.Buffer) h.capi.ErrorCode {
    const description = dependencies.sql_errors.describe(err);
    out.* = api.stringifyJson(.{ .@"error" = description }) catch return .internal;
    if (std.mem.eql(u8, description.code, "0A000")) return .unsupported;
    if (std.mem.eql(u8, description.code, "40001")) return .version_conflict;
    if (std.mem.eql(u8, description.code, "40003")) return .outcome_unknown;
    if (std.mem.eql(u8, description.code, "XX000") or std.mem.eql(u8, description.code, "53200")) return .internal;
    return .invalid_argument;
}
fn open(handle: *h.Handle, table_name: []const u8, request_json: []const u8) !u64 {
    if (handle.storage_owner_context != null or handle.storage_owner_path != null or handle.readable_lease_hook != null) return error.UnsupportedSqlExecution;
    if (request_json.len > sql.runtime.resource_limits.request_bytes or table_name.len > 1024 or handle.sql_cursors.count() >= 64) return error.SqlProgramLimitExceeded;
    const session = try @import("sql_session.zig").requestSession(handle, request_json);
    return openWithSession(handle, table_name, request_json, session) catch |err| {
        // Unsupported cursor shapes may fall back to materialized execution.
        if (err != error.UnsupportedSqlExecution) if (session) |value| {
            if (value.active) value.failed = true;
        };
        return err;
    };
}
fn openWithSession(handle: *h.Handle, table_name: []const u8, request_json: []const u8, session: ?*@import("sql_session.zig").Session) !u64 {
    try @import("tables.zig").load(handle);
    try @import("sql_commit.zig").recover(handle);
    try @import("sql_ddl.zig").recover(handle);
    const cursor = try handle.alloc.create(Cursor);
    errdefer handle.alloc.destroy(cursor);
    cursor.alloc = handle.alloc;
    cursor.budget = .{ .backing = handle.alloc, .limit = sql.runtime.resource_limits.default_memory_bytes };
    cursor.arena = std.heap.ArenaAllocator.init(cursor.budget.allocator());
    errdefer cursor.arena.deinit();
    const request = prepare(cursor, handle, table_name, request_json, session) catch |err| {
        if (err == error.OutOfMemory and cursor.budget.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    errdefer cursor.compiled.deinit();
    cursor.lease = try handle.db.local_execution.row_policy_gate.enterRaw();
    errdefer cursor.lease.release();
    cursor.stream = (try dependencies.sql_read_stream.Stream.open(handle.alloc, cursor.adapter.backend(), &cursor.compiled, request.parameters, .{})) orelse return error.UnsupportedSqlExecution;
    errdefer cursor.stream.close();
    const id = handle.next_sql_cursor_id;
    handle.next_sql_cursor_id = std.math.add(u64, id, 1) catch return error.SqlProgramLimitExceeded;
    try handle.sql_cursors.putNoClobber(handle.alloc, id, cursor);
    return id;
}
// Only this phase allocates from the cursor's preparation budget. Keep its
// quota rejection separate from backing-allocator failures in stream creation.
fn prepare(cursor: *Cursor, handle: *h.Handle, table_name: []const u8, request_json: []const u8, session: ?*@import("sql_session.zig").Session) !Request {
    const a = cursor.arena.allocator();
    const request = try std.json.parseFromSliceLeaky(Request, a, request_json, .{});
    cursor.session_id = request.session_id;
    cursor.compiled = try sql.compiler.compile(cursor.budget.allocator(), request.statement, .{});
    errdefer cursor.compiled.deinit();
    switch (cursor.compiled.statement) {
        .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint => return error.UnsupportedSqlExecution,
        else => {},
    }
    if (session) |value| if (value.failed) return error.SqlTransactionAborted;
    cursor.adapter = .{ .transaction = session, .handle = handle, .db = &handle.db, .table_name = try a.dupe(u8, table_name), .read_only = !h.liteOpenModeCanWrite(handle.open_mode) or (if (session) |value| value.read_only else false) };
    return request;
}
pub export fn antfly_db_sql_open_cursor_json(ptr: ?*anyopaque, request: h.capi.Slice, out_id: *u64, out: *h.capi.Buffer) h.capi.ErrorCode {
    out.* = .{};
    out_id.* = 0;
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    if (guard.handle.parent_id != null) return .invalid_argument;
    out_id.* = open(guard.handle, "default", request.bytes()) catch |err| return diagnostic(err, out);
    return .ok;
}
pub export fn antfly_db_sql_fetch_cursor_json(ptr: ?*anyopaque, id: u64, rows: u32, out: *h.capi.Buffer) h.capi.ErrorCode {
    out.* = .{};
    const guard = api.enterHandlePinned(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    if (rows == 0 or rows > 4096) return diagnostic(error.InvalidSqlLimit, out);
    const cursor = guard.handle.sql_cursors.get(id) orelse return .invalid_argument;
    var page = cursor.stream.next(rows) catch |err| {
        if (cursor.session_id) |session_id| if (guard.handle.sql_sessions.get(session_id)) |session| {
            if (session.active) session.failed = true;
        };
        return diagnostic(err, out);
    };
    defer page.deinit();
    // Streaming execution retains typed integers. The public JSON ABI uses
    // decimal strings, just like materialized SQL, for exact JS int64 values.
    for (page.output.rows) |row| {
        for (page.output.columns, @constCast(row)) |column, *cell| {
            if (column.type == .integer and cell.* == .integer) {
                const encoded = std.fmt.allocPrint(page.arena.allocator(), "{d}", .{cell.integer}) catch return diagnostic(error.OutOfMemory, out);
                cell.* = .{ .string = encoded };
            }
        }
    }
    out.* = api.stringifyJson(.{ .result = page.output, .exhausted = page.exhausted }) catch return .internal;
    return .ok;
}
pub export fn antfly_db_sql_close_cursor(ptr: ?*anyopaque, id: u64) h.capi.ErrorCode {
    const guard = api.enterHandlePinned(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    const entry = guard.handle.sql_cursors.fetchRemove(id) orelse return .invalid_argument;
    entry.value.close();
    return .ok;
}
