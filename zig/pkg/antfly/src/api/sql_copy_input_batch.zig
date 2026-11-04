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

//! Shared typed COPY batching, independent of socket framing or producer I/O.
//! The caller owns transaction admission, completion and rollback. A batch never
//! retries an attempted mutation and retains one reusable row/batch arena.
const std = @import("std");
const backend = @import("../pgwire/backend.zig");
const values = @import("../pgwire/values.zig");
const Datum = @import("antfly_local_sources").sql_scalar.Datum;
pub const Driver = struct {
    ptr: *anyopaque,
    check: *const fn (*anyopaque) anyerror!void,
    execute: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const std.json.Value, []const backend.Type, []const backend.Parameter, []const Datum, ?[]const u8, ?u64) anyerror!backend.Result,
    sqlstate: *const fn (anyerror) []const u8,
    notice: ?*const fn (*anyopaque, []const u8) anyerror!void = null,
    diagnostic: ?*const fn (*anyopaque, []const u8, []const u8) void = null,
};

pub const Batch = struct {
    alloc: std.mem.Allocator,
    driver: Driver,
    description: backend.Description,
    arena: std.heap.ArenaAllocator,
    row_arena: std.heap.ArenaAllocator,
    row_cells: []@import("antfly_local_sources").sql_scalar.Datum = &.{},
    modifiers: []?@import("antfly_local_sources").sql_scalar.Program = &.{},
    defaults: std.ArrayList(bool) = .empty,
    rejected: u64 = 0,
    admission_started: bool = false,
    received: u64 = 0,
    max_rows: usize,
    count: usize = 0,
    total: u64 = 0,
    bytes: usize = 0,
    parameters: std.ArrayList(std.json.Value) = .empty,
    cells: std.ArrayList(@import("antfly_local_sources").sql_scalar.Datum) = .empty,
    types: std.ArrayList(backend.Type) = .empty,
    descriptors: std.ArrayList(backend.Parameter) = .empty,
    pub fn deinit(self: *Batch) void {
        for (self.modifiers) |*program| if (program.*) |*value| value.deinit();
        self.arena.deinit();
        self.row_arena.deinit();
        self.defaults.deinit(self.alloc);
        self.parameters.deinit(self.alloc);
        self.cells.deinit(self.alloc);
        self.types.deinit(self.alloc);
        self.descriptors.deinit(self.alloc);
    }
    pub fn initColumns(self: *Batch, a: std.mem.Allocator) !void {
        const scalar = @import("antfly_local_sources").sql_scalar;
        const ast = @import("antfly_local_sources").sql_ast;
        const columns = self.description.copy_in.?.columns;
        self.row_cells = try a.alloc(scalar.Datum, columns.len);
        self.modifiers = try a.alloc(?scalar.Program, columns.len);
        @memset(self.modifiers, null);
        for (columns, self.modifiers) |column, *program| if (column.numeric_modifier) |modifier| {
            const descriptor = try values.parameterFromColumn(column);
            const operand: ast.Scalar = .{ .column = "copy_value" };
            const cast: ast.Scalar = .{ .cast = .{ .operand = &operand, .type = descriptor.kind.?, .element_type = descriptor.element_type, .numeric_modifier = modifier } };
            program.* = try scalar.bindTyped(a, &cast, &.{.{ .name = "copy_value", .type = descriptor.kind.?, .element_type = descriptor.element_type }}, &.{}, .{});
        };
    }
    fn decodeRow(self: *Batch, fields: []const @import("../pgwire/copy_in.zig").Field, defaults: []const bool) !void {
        const input = self.description.copy_in.?;
        const a = if (input.options.on_error == .ignore) self.row_arena.allocator() else self.arena.allocator();
        for (fields, input.columns, self.row_cells, self.modifiers, 0..) |field, column, *datum, *program, i| {
            if (defaults.len != 0 and defaults[i]) {
                if (!column.defaulted) return error.InvalidCopyData;
                datum.* = .{};
                continue;
            }
            if (column.type == .string) if (field) |bytes| {
                if (!std.unicode.utf8ValidateSlice(bytes) or std.mem.indexOfScalar(u8, bytes, 0) != null) return error.SqlInvalidTextEncoding;
            };
            datum.* = if (field) |bytes| (values.decodeColumnNative(a, column, if (input.options.format == .binary) 1 else 0, bytes) catch |err| return if (err == error.InvalidParameter) error.SqlInvalidTextRepresentation else err) else .{};
            if (program.*) |*value| datum.* = try value.evaluate(a, &.{datum.*}, &.{}, .{});
        }
    }
    fn notice(self: *Batch, message_text: []const u8) !void {
        if (self.driver.notice) |emit| try emit(self.driver.ptr, message_text);
    }
    pub fn report(self: *Batch) !void {
        if (self.rejected == 0 or self.description.copy_in.?.options.log_verbosity == .silent) return;
        var buffer: [160]u8 = undefined;
        try self.notice(try std.fmt.bufPrint(&buffer, "{d} rows were skipped due to data type incompatibility", .{self.rejected}));
    }
    pub fn row(self: *Batch, fields: []const @import("../pgwire/copy_in.zig").Field, defaults: []const bool) !void {
        const input = self.description.copy_in.?;
        if (fields.len != input.columns.len or (defaults.len != 0 and defaults.len != fields.len)) return error.InvalidCopyData;
        try self.driver.check(self.driver.ptr);
        self.received = std.math.add(u64, self.received, 1) catch return error.ProgramLimitExceeded;
        var row_bytes: usize = 0;
        for (fields) |field| if (field) |value| {
            row_bytes +|= value.len;
        };
        if (self.count != 0 and self.bytes +| row_bytes > 256 * 1024) try self.flush();
        _ = self.row_arena.reset(.retain_capacity);
        self.decodeRow(fields, defaults) catch |err| {
            const code = self.driver.sqlstate(err);
            // Only the typed input boundary supports soft rejection. Never
            // catch framing, constraints, authority, commit or resource errors.
            if (input.options.on_error != .ignore or !std.mem.startsWith(u8, code, "22") or err == error.InvalidCopyData or err == error.SqlInvalidTextEncoding) return err;
            self.rejected = std.math.add(u64, self.rejected, 1) catch return error.ProgramLimitExceeded;
            if (input.options.reject_limit) |limit| if (self.rejected > limit) {
                var buffer: [160]u8 = undefined;
                const message_text = try std.fmt.bufPrint(&buffer, "skipped more than REJECT_LIMIT ({d}) rows due to data type incompatibility", .{limit});
                if (self.driver.diagnostic) |set| set(self.driver.ptr, "22P02", message_text);
                return error.SqlInvalidTextRepresentation;
            };
            if (input.options.log_verbosity == .verbose) {
                var buffer: [160]u8 = undefined;
                try self.notice(try std.fmt.bufPrint(&buffer, "COPY input row {d} skipped due to data type incompatibility", .{self.received}));
            }
            return;
        };
        self.bytes +|= row_bytes;
        const a = self.arena.allocator();
        for (self.row_cells, input.columns, 0..) |value, column, i| {
            const default = defaults.len != 0 and defaults[i];
            try self.defaults.append(self.alloc, default);
            if (default) continue;
            const datum = if (input.options.on_error == .ignore) try @import("antfly_local_sources").sql_operators.cloneDatum(a, value) else value;
            try self.cells.append(self.alloc, datum);
            try self.parameters.append(self.alloc, datum.value);
            try self.types.append(self.alloc, column.type);
            try self.descriptors.append(self.alloc, try values.parameterFromColumn(column));
        }
        self.count += 1;
        if (self.count == self.max_rows) try self.flush();
    }
    fn quote(writer: *std.Io.Writer, name: []const u8) !void {
        try writer.writeByte('"');
        for (name) |ch| {
            try writer.writeByte(ch);
            if (ch == '"') try writer.writeByte(ch);
        }
        try writer.writeByte('"');
    }
    pub fn flush(self: *Batch) !void {
        if (self.count == 0) return;
        const input = self.description.copy_in.?;
        const a = self.arena.allocator();
        var sql: std.Io.Writer.Allocating = .init(a);
        try sql.writer.writeAll("INSERT INTO ");
        if (input.table.database) |database| {
            try quote(&sql.writer, database);
            try sql.writer.writeByte('.');
        }
        if (input.table.namespace) |namespace| {
            try quote(&sql.writer, namespace);
            try sql.writer.writeByte('.');
        }
        try quote(&sql.writer, input.table.table);
        try sql.writer.writeByte('(');
        for (input.columns, 0..) |column, i| {
            if (i != 0) try sql.writer.writeByte(',');
            try quote(&sql.writer, column.name);
        }
        try sql.writer.writeAll(") OVERRIDING SYSTEM VALUE VALUES ");
        var parameter: usize = 0;
        for (0..self.count) |row_index| {
            if (row_index != 0) try sql.writer.writeByte(',');
            try sql.writer.writeByte('(');
            for (0..input.columns.len) |i| {
                if (i != 0) try sql.writer.writeByte(',');
                if (self.defaults.items[row_index * input.columns.len + i]) try sql.writer.writeAll("DEFAULT") else {
                    parameter += 1;
                    try sql.writer.print("${d}", .{parameter});
                }
            }
            try sql.writer.writeByte(')');
        }
        self.admission_started = true;
        var result = try self.driver.execute(self.driver.ptr, a, sql.written(), self.parameters.items, self.types.items, self.descriptors.items, self.cells.items, self.description.binding_guard, self.description.setting_epoch);
        defer result.deinit();
        if (result.transaction_status != .in_transaction or result.rows_affected > self.count or (input.source_statement == null and result.rows_affected != self.count)) return error.InvalidResult;
        self.total = std.math.add(u64, self.total, result.rows_affected) catch return error.ProgramLimitExceeded;
        self.count = 0;
        self.bytes = 0;
        self.defaults.clearRetainingCapacity();
        self.parameters.clearRetainingCapacity();
        self.cells.clearRetainingCapacity();
        self.types.clearRetainingCapacity();
        self.descriptors.clearRetainingCapacity();
        result.deinit();
        _ = self.arena.reset(.retain_capacity);
        self.admission_started = false;
    }
};
