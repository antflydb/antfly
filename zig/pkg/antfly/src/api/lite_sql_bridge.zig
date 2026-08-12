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

//! Opaque caller side of the API-owned Lite SQL runtime.

const std = @import("std");
const abi = @import("kernel_abi.zig");
const lite_sql_runtime = @import("lite_sql_runtime.zig");
const lite_sql_source = @import("lite_sql_source.zig");

const ErrorInt = abi.ErrorInt;
const Source = lite_sql_source.Source;

extern fn antfly_api_kernel_lite_sql_session_create(context: *const abi.LiteSqlSessionCreateContext) callconv(.c) c_int;
extern fn antfly_api_kernel_lite_sql_session_destroy(handle: *anyopaque) callconv(.c) void;
extern fn antfly_api_kernel_lite_sql_classify(context: *const abi.LiteSqlClassifyContext) callconv(.c) c_int;
extern fn antfly_api_kernel_lite_sql_execute(context: *const abi.LiteSqlExecuteContext) callconv(.c) c_int;

fn callError(status: c_int, error_code: ErrorInt) !void {
    if (status == 0) return;
    if (error_code != 0) return @errorFromInt(error_code);
    return error.ApiKernelOperationFailed;
}

pub const Session = struct {
    handle: *anyopaque,

    pub fn init(alloc: std.mem.Allocator, options: lite_sql_runtime.CatalogOptions) !Session {
        var alloc_copy = alloc;
        var options_copy = options;
        var handle: ?*anyopaque = null;
        var error_code: ErrorInt = 0;
        try callError(antfly_api_kernel_lite_sql_session_create(&.{
            .owner_alloc = &alloc_copy,
            .options = &options_copy,
            .out_handle = &handle,
            .error_code = &error_code,
        }), error_code);
        return .{ .handle = handle orelse return error.ApiKernelOperationFailed };
    }

    pub fn deinit(self: *Session, _: std.mem.Allocator) void {
        antfly_api_kernel_lite_sql_session_destroy(self.handle);
        self.* = undefined;
    }

    pub fn executeJsonAlloc(self: *Session, alloc: std.mem.Allocator, source: Source, sql: []const u8) ![]u8 {
        var alloc_copy = alloc;
        var source_copy = source;
        var sql_copy = sql;
        var out: []u8 = undefined;
        var error_code: ErrorInt = 0;
        try callError(antfly_api_kernel_lite_sql_execute(&.{
            .handle = self.handle,
            .alloc = &alloc_copy,
            .source = &source_copy,
            .sql = @ptrCast(&sql_copy),
            .out_json = @ptrCast(&out),
            .error_code = &error_code,
        }), error_code);
        return out;
    }
};

pub fn statementIsReadOnly(alloc: std.mem.Allocator, sql: []const u8) !bool {
    var alloc_copy = alloc;
    var sql_copy = sql;
    var out = false;
    var error_code: ErrorInt = 0;
    try callError(antfly_api_kernel_lite_sql_classify(&.{
        .alloc = &alloc_copy,
        .sql = @ptrCast(&sql_copy),
        .out_read_only = &out,
        .error_code = &error_code,
    }), error_code);
    return out;
}
