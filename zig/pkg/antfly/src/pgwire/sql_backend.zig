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
const table_ddl = @import("../metadata/catalog/table_ddl.zig");
const sql_adapter = @import("../sql/mod.zig");
const runtime_schema = @import("../storage/schema.zig");

pub const AuthenticatedIdentity = struct {
    ptr: *anyopaque,
    deinit_fn: *const fn (*anyopaque, std.mem.Allocator) void,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.deinit_fn(self.ptr, alloc);
        self.* = undefined;
    }
};

pub const PublicSqlTransactionStatus = enum {
    idle,
    in_transaction,
    failed_transaction,
};

pub const PublicSqlErrorResponse = struct {
    status: u16,
    body: []u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.body.len > 0) alloc.free(self.body);
        self.* = undefined;
    }
};

pub const PublicSqlResult = struct {
    pub const Read = struct {
        rows: []const []const u8 = &.{},
        columns: []const runtime_schema.RelationalColumn = &.{},

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            freeRows(alloc, self.rows);
            freeColumns(alloc, self.columns);
            self.* = undefined;
        }
    };

    pub const Ddl = struct {
        applied: table_ddl.AppliedRelationalSqlDdlRecord,
        command_tag: []const u8 = "DDL",

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.applied.deinit(alloc);
            self.* = undefined;
        }
    };

    pub const RowsBatchResult = struct {
        inserted: u32 = 0,
        deleted: u32 = 0,
        transformed: u32 = 0,
        returning_rows: []const []const u8 = &.{},

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            freeRows(alloc, self.returning_rows);
            self.* = undefined;
        }
    };

    pub const RowsBatch = struct {
        result: RowsBatchResult = .{},
        columns: []const runtime_schema.RelationalColumn = &.{},

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.result.deinit(alloc);
            freeColumns(alloc, self.columns);
            self.* = undefined;
        }
    };

    pub const MutationSourceResult = struct {
        staged: u32 = 0,
        returning_rows: []const []const u8 = &.{},

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            freeRows(alloc, self.returning_rows);
            self.* = undefined;
        }
    };

    pub const MutationSource = struct {
        result: MutationSourceResult = .{},
        columns: []const runtime_schema.RelationalColumn = &.{},

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.result.deinit(alloc);
            freeColumns(alloc, self.columns);
            self.* = undefined;
        }
    };

    pub const BulkIo = struct {
        operation: sql_adapter.BulkSqlIoOperation,
        stream: sql_adapter.BulkSqlIoStream,
        row_count: ?usize = null,
        payload: ?[]u8 = null,

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.payload) |payload| alloc.free(payload);
            self.* = undefined;
        }
    };

    session_id: u64,
    statement_kind: []const u8,
    transaction_status: PublicSqlTransactionStatus = .idle,
    result: union(enum) {
        ddl: Ddl,
        read: Read,
        rows_batch: RowsBatch,
        mutation_source: MutationSource,
        bulk_io: BulkIo,
    },

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.result) {
            .ddl => |*ddl| ddl.deinit(alloc),
            .read => |*read| read.deinit(alloc),
            .rows_batch => |*rows_batch| rows_batch.deinit(alloc),
            .mutation_source => |*mutation_source| mutation_source.deinit(alloc),
            .bulk_io => |*bulk_io| bulk_io.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const PublicSqlResultOrResponse = union(enum) {
    result: PublicSqlResult,
    response: PublicSqlErrorResponse,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .result => |*result| result.deinit(alloc),
            .response => |*response| response.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const PublicSqlDescribeResult = struct {
    session_id: u64,
    statement_kind: []const u8,
    transaction_status: PublicSqlTransactionStatus = .idle,
    has_row_description: bool = false,
    columns: []const runtime_schema.RelationalColumn = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeColumns(alloc, self.columns);
        self.* = undefined;
    }
};

pub const PublicSqlDescribeResultOrResponse = union(enum) {
    result: PublicSqlDescribeResult,
    response: PublicSqlErrorResponse,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .result => |*result| result.deinit(alloc),
            .response => |*response| response.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const Request = struct {
    parsed_sql: *const sql_adapter.ParsedSql,
    session_id: ?u64,
    database: ?[]const u8,
    namespace: ?[]const u8,
    read_only: bool,
    params: []const sql_adapter.SqlValue,
};

pub const Backend = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        allocator: *const fn (*anyopaque) std.mem.Allocator,
        authentication_required: *const fn (*anyopaque) bool,
        can_authenticate_password: *const fn (*anyopaque) bool,
        authenticate_user_password: *const fn (*anyopaque, []const u8, []const u8) anyerror!AuthenticatedIdentity,
        describe_parsed_sql: *const fn (*anyopaque, Request, ?AuthenticatedIdentity) anyerror!PublicSqlDescribeResultOrResponse,
        execute_parsed_sql: *const fn (*anyopaque, Request, ?AuthenticatedIdentity) anyerror!PublicSqlResultOrResponse,
        free_param: *const fn (*anyopaque, std.mem.Allocator, sql_adapter.SqlValue) void,
    };

    pub fn allocator(self: Backend) std.mem.Allocator {
        return self.vtable.allocator(self.ptr);
    }

    pub fn authenticationRequired(self: Backend) bool {
        return self.vtable.authentication_required(self.ptr);
    }

    pub fn canAuthenticatePassword(self: Backend) bool {
        return self.vtable.can_authenticate_password(self.ptr);
    }

    pub fn authenticateUserPassword(self: Backend, username: []const u8, password: []const u8) !AuthenticatedIdentity {
        return try self.vtable.authenticate_user_password(self.ptr, username, password);
    }

    pub fn describeParsedSql(
        self: Backend,
        request: Request,
        authenticated_identity: ?AuthenticatedIdentity,
    ) !PublicSqlDescribeResultOrResponse {
        return try self.vtable.describe_parsed_sql(self.ptr, request, authenticated_identity);
    }

    pub fn executeParsedSql(
        self: Backend,
        request: Request,
        authenticated_identity: ?AuthenticatedIdentity,
    ) !PublicSqlResultOrResponse {
        return try self.vtable.execute_parsed_sql(self.ptr, request, authenticated_identity);
    }

    pub fn freeParam(self: Backend, alloc: std.mem.Allocator, value: sql_adapter.SqlValue) void {
        self.vtable.free_param(self.ptr, alloc, value);
    }
};

pub fn freeRows(alloc: std.mem.Allocator, rows: []const []const u8) void {
    for (rows) |row| alloc.free(@constCast(row));
    if (rows.len > 0) alloc.free(@constCast(rows));
}

pub fn cloneRowsAlloc(alloc: std.mem.Allocator, rows: []const []const u8) ![]const []const u8 {
    if (rows.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, rows.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |row| alloc.free(@constCast(row));
        alloc.free(out);
    }
    for (rows, 0..) |row, i| {
        out[i] = try alloc.dupe(u8, row);
        initialized += 1;
    }
    return out;
}

pub fn freeParam(alloc: std.mem.Allocator, value: sql_adapter.SqlValue) void {
    sql_adapter.deinitSqlValue(alloc, value);
}

pub fn freeColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) void {
    sql_adapter.freeDdlRelationalColumns(alloc, columns);
}

pub fn cloneColumnsAlloc(
    alloc: std.mem.Allocator,
    columns: []const runtime_schema.RelationalColumn,
) ![]const runtime_schema.RelationalColumn {
    return try sql_adapter.cloneDdlRelationalColumns(alloc, columns);
}
