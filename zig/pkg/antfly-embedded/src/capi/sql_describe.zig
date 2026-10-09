// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const h = @import("handles.zig");
const api = @import("db.zig");
const sql = @import("sql.zig");
const d = h.antfly.capi_dependencies;
const std = h.std;
fn describe(handle: *h.Handle, request_json: []const u8) !h.capi.Buffer {
    if (request_json.len > 2 * 1024 * 1024) return error.SqlProgramLimitExceeded;
    const Request = struct { statement: []const u8, parameter_types: []const ?d.sql_ast.ColumnType = &.{} };
    const request = try std.json.parseFromSlice(Request, handle.alloc, request_json, .{});
    defer request.deinit();
    var compiled = try sql.compiler.compile(handle.alloc, request.value.statement, .{});
    defer compiled.deinit();
    try @import("tables.zig").load(handle);
    try @import("sql_commit.zig").recover(handle);
    var adapter = sql.Adapter(h.antfly){ .handle = handle, .db = &handle.db, .table_name = "default", .read_only = !h.liteOpenModeCanWrite(handle.open_mode) };
    var description = try d.sql_describe.describe(handle.alloc, adapter.backend(), &compiled, request.value.parameter_types);
    defer description.deinit();
    return api.stringifyJson(.{ .columns = description.binding.columns, .parameter_types = description.binding.parameter_types });
}
pub export fn antfly_db_sql_describe_json(ptr: ?*anyopaque, request: h.capi.Slice, out: *h.capi.Buffer) h.capi.ErrorCode {
    out.* = .{};
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    if (guard.handle.parent_id != null) return .invalid_argument;
    out.* = describe(guard.handle, request.bytes()) catch |err| return @import("sql_cursor.zig").diagnostic(err, out);
    return .ok;
}
