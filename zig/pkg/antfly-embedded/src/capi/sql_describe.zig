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
const d = h.antfly.capi_dependencies;
const std = h.std;
fn describe(handle: *h.Handle, request_json: []const u8) !h.capi.Buffer {
    if (request_json.len > sql.runtime.resource_limits.request_bytes) return error.SqlRequestTooLarge;
    const Budget = d.sql_memory_budget;
    var budget = Budget{ .backing = handle.alloc, .limit = sql.runtime.resource_limits.preparation_bytes };
    return describePrepared(handle, request_json, budget.allocator()) catch |err| {
        if (err == error.OutOfMemory and budget.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
}
fn describePrepared(handle: *h.Handle, request_json: []const u8, alloc: std.mem.Allocator) !h.capi.Buffer {
    const Request = struct { statement: []const u8, parameter_types: []const ?d.sql_ast.ColumnType = &.{} };
    const request = try std.json.parseFromSlice(Request, alloc, request_json, .{ .allocate = .alloc_always });
    defer request.deinit();
    var compiled = try sql.compiler.compile(alloc, request.value.statement, .{});
    defer compiled.deinit();
    try @import("tables.zig").load(handle);
    try @import("sql_commit.zig").recover(handle);
    try @import("sql_ddl.zig").recover(handle);
    var adapter = sql.Adapter(h.antfly){ .handle = handle, .db = &handle.db, .table_name = "default", .read_only = !h.liteOpenModeCanWrite(handle.open_mode) };
    var description = try d.sql_describe.describe(alloc, adapter.backend(), &compiled, request.value.parameter_types);
    defer description.deinit();
    return api.stringifyJson(.{ .columns = description.binding.columns, .parameter_types = description.binding.parameter_types });
}
pub export fn antfly_db_sql_describe_json(ptr: ?*anyopaque, request: h.capi.Slice, out: *h.capi.Buffer) h.capi.ErrorCode {
    out.* = .{};
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    if (guard.entry_error) |code| {
        guard.leave();
        return code;
    }
    defer guard.leave();
    if (guard.handle.parent_id != null) return .invalid_argument;
    out.* = describe(guard.handle, request.bytes()) catch |err| return @import("sql_cursor.zig").diagnostic(err, out);
    return .ok;
}
