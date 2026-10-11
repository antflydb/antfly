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

const std = @import("std");
const c = @import("ffi.zig").c;
const plan = @import("plan.zig");
pub const Value = std.json.Value;

pub const Source = struct {
    db: ?*c.sqlite3,

    pub fn open(a: std.mem.Allocator, path: []const u8) !Source {
        const z = try a.dupeSentinel(u8, path, 0);
        defer a.free(z);
        var db: ?*c.sqlite3 = null;
        if (c.sqlite3_open_v2(z, &db, c.SQLITE_OPEN_READONLY, null) != c.SQLITE_OK) {
            if (db != null) _ = c.sqlite3_close(db);
            return error.SqliteOpenFailed;
        }
        errdefer _ = c.sqlite3_close(db);
        _ = c.sqlite3_busy_timeout(db, 5000);
        if (c.sqlite3_exec(db, "BEGIN", null, null, null) != c.SQLITE_OK) return error.SqliteBeginFailed;
        // Force the read snapshot now, rather than on the first mapped query.
        var source: Source = .{ .db = db };
        const statement = try source.prepare(a, "SELECT count(*) FROM sqlite_schema");
        defer _ = c.sqlite3_finalize(statement);
        if (c.sqlite3_step(statement) != c.SQLITE_ROW) return error.SqliteSnapshotFailed;
        return source;
    }

    pub fn close(self: Source) void {
        _ = c.sqlite3_close(self.db);
    }

    pub fn prepare(self: Source, a: std.mem.Allocator, query: []const u8) !*c.sqlite3_stmt {
        if (query.len > 1024 * 1024 or std.mem.indexOfScalar(u8, query, 0) != null) return error.InvalidSourceQuery;
        const z = try a.dupeSentinel(u8, query, 0);
        defer a.free(z);
        var statement: ?*c.sqlite3_stmt = null;
        var tail: [*c]const u8 = null;
        if (c.sqlite3_prepare_v2(self.db, z, @intCast(z.len + 1), &statement, &tail) != c.SQLITE_OK) {
            std.debug.print("SQLite query failed: {s}\n", .{c.sqlite3_errmsg(self.db)});
            return error.SqlitePrepareFailed;
        }
        errdefer _ = c.sqlite3_finalize(statement);
        const stmt = statement orelse return error.EmptyQuery;
        if (std.mem.trim(u8, std.mem.span(tail), " \t\r\n").len != 0 or c.sqlite3_stmt_readonly(stmt) == 0 or c.sqlite3_bind_parameter_count(stmt) != 0 or c.sqlite3_column_count(stmt) == 0) return error.InvalidSourceQuery;
        return stmt;
    }
};

pub fn next(statement: *c.sqlite3_stmt) !bool {
    return switch (c.sqlite3_step(statement)) {
        c.SQLITE_ROW => true,
        c.SQLITE_DONE => false,
        else => error.SqliteReadFailed,
    };
}

pub fn columnIndex(statement: *c.sqlite3_stmt, name: []const u8) !c_int {
    var found: ?c_int = null;
    var i: c_int = 0;
    while (i < c.sqlite3_column_count(statement)) : (i += 1) {
        if (std.mem.eql(u8, std.mem.span(c.sqlite3_column_name(statement, i)), name)) {
            if (found != null) return error.AmbiguousSourceColumn;
            found = i;
        }
    }
    return found orelse error.MissingSourceColumn;
}

pub fn cell(a: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int, col: plan.Column, max_bytes: usize) !Value {
    const kind = c.sqlite3_column_type(stmt, index);
    if (kind == c.SQLITE_NULL) {
        if (!col.nullable) return error.NullNotAllowed;
        return .null;
    }
    if (col.transform == .base64) {
        if (kind != c.SQLITE_BLOB) return error.ExpectedBlob;
        const bytes = try byteCell(stmt, index, max_bytes);
        const output = try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
        return .{ .string = std.base64.standard.Encoder.encode(output, bytes) };
    }
    return switch (col.type) {
        .integer => if (kind == c.SQLITE_INTEGER) .{ .integer = c.sqlite3_column_int64(stmt, index) } else error.ExpectedInteger,
        .real => blk: {
            if (kind != c.SQLITE_FLOAT and kind != c.SQLITE_INTEGER) return error.ExpectedReal;
            if (kind == c.SQLITE_INTEGER and (c.sqlite3_column_int64(stmt, index) > 9007199254740992 or c.sqlite3_column_int64(stmt, index) < -9007199254740992)) return error.InexactRealConversion;
            const value = c.sqlite3_column_double(stmt, index);
            if (!std.math.isFinite(value)) return error.NonFiniteReal;
            break :blk .{ .float = value };
        },
        .boolean => blk: {
            if (kind != c.SQLITE_INTEGER) return error.ExpectedBoolean;
            const value = c.sqlite3_column_int64(stmt, index);
            if (value != 0 and value != 1) return error.ExpectedBoolean;
            break :blk .{ .bool = value == 1 };
        },
        .text, .json => blk: {
            if (kind != c.SQLITE_TEXT) return error.ExpectedText;
            const bytes = try byteCell(stmt, index, max_bytes);
            if (!std.unicode.utf8ValidateSlice(bytes)) return error.InvalidUtf8;
            if (col.type == .json) break :blk try std.json.parseFromSliceLeaky(Value, a, bytes, .{ .allocate = .alloc_always });
            break :blk .{ .string = try a.dupe(u8, bytes) };
        },
    };
}

fn byteCell(stmt: *c.sqlite3_stmt, index: c_int, max_bytes: usize) ![]const u8 {
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, index));
    if (len > max_bytes) return error.CellTooLarge;
    if (len == 0) return "";
    const ptr: [*]const u8 = @ptrCast(c.sqlite3_column_blob(stmt, index) orelse return error.SqliteReadFailed);
    return ptr[0..len];
}

test "SQLite storage classes require explicit conversion" {
    const a = std.testing.allocator;
    var db: ?*c.sqlite3 = null;
    try std.testing.expectEqual(c.SQLITE_OK, c.sqlite3_open(":memory:", &db));
    defer _ = c.sqlite3_close(db);
    const source: Source = .{ .db = db };
    const stmt = try source.prepare(a, "SELECT 9223372036854775807 AS n, 2 AS b, x'00ff' AS bytes");
    defer _ = c.sqlite3_finalize(stmt);
    try std.testing.expect(try next(stmt));
    const col: plan.Column = .{ .name = "n", .from = "n", .type = .integer };
    try std.testing.expectEqual(std.math.maxInt(i64), (try cell(a, stmt, 0, col, 1024)).integer);
    var boolean = col;
    boolean.type = .boolean;
    try std.testing.expectError(error.ExpectedBoolean, cell(a, stmt, 1, boolean, 1024));
    var blob = col;
    blob.type = .text;
    blob.transform = .base64;
    const encoded = try cell(a, stmt, 2, blob, 1024);
    defer a.free(encoded.string);
    try std.testing.expectEqualStrings("AP8=", encoded.string);
    try std.testing.expectError(error.InvalidSourceQuery, source.prepare(a, "SELECT 1; SELECT 2"));
}
