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
pub const Value = std.json.Value;
// Conservative public SQL compiler bound. Each mapped cell binds once.
pub const max_parameters = 1024;
// Leave headroom for AST nodes from JSON casts and upsert assignments on ABI
// 3 libraries with smaller compiler budgets. This also bounds preparation work.
pub const max_batch_cells = 256;

pub const Sink = struct {
    db: ?*c.antfly_db,
    pub fn open(a: std.mem.Allocator, path: []const u8, seed: ?[]const u8) !Sink {
        const z = try a.dupeSentinel(u8, path, 0);
        defer a.free(z);
        var db: ?*c.antfly_db = null;
        if (seed) |source| {
            const src = try a.dupeSentinel(u8, source, 0);
            defer a.free(src);
            var out: c.antfly_buffer = .{ .ptr = null, .len = 0 };
            defer c.antfly_buffer_free(&out);
            try check(c.antfly_lite_copy_stable_snapshot_file_json(src, z, false, &out), out);
            try check(c.antfly_lite_open(z, &db), .{ .ptr = null, .len = 0 });
        } else try check(c.antfly_lite_create(z, &db), .{ .ptr = null, .len = 0 });
        return .{ .db = db };
    }
    pub fn close(self: Sink) void {
        c.antfly_db_close(self.db);
    }

    pub fn sql(self: Sink, a: std.mem.Allocator, statement: []const u8, parameters: []const Value) !Value {
        return self.sqlLimit(a, statement, parameters, 2);
    }

    pub fn sqlLimit(self: Sink, a: std.mem.Allocator, statement: []const u8, parameters: []const Value, limit: usize) !Value {
        const request = try std.json.Stringify.valueAlloc(a, .{ .statement = statement, .parameters = parameters, .limit = limit }, .{});
        var out: c.antfly_buffer = .{ .ptr = null, .len = 0 };
        defer c.antfly_buffer_free(&out);
        try check(c.antfly_db_sql_json(self.db, .{ .ptr = request.ptr, .len = request.len }, &out), out);
        const result = try std.json.parseFromSliceLeaky(Value, a, out.ptr[0..out.len], .{ .allocate = .alloc_always });
        if (result.object.get("mutation_outcome")) |outcome| {
            if (outcome != .null and !std.mem.eql(u8, outcome.string, "committed")) return error.IncompleteMutation;
        }
        return result;
    }
};

pub fn check(code: c.antfly_error_code, out: c.antfly_buffer) !void {
    if (code == c.ANTFLY_OK) return;
    // Keep row values and diagnostic text private, but include SQLSTATE and
    // a schema identifier so real migration failures are actionable.
    if (out.ptr != null and out.len != 0) {
        if (std.json.parseFromSlice(Value, std.heap.page_allocator, out.ptr[0..out.len], .{})) |parsed| {
            defer parsed.deinit();
            if (parsed.value == .object) if (parsed.value.object.get("error")) |diagnostic| {
                if (diagnostic == .object) {
                    if (diagnostic.object.get("code")) |state| if (state == .string and state.string.len == 5) std.debug.print("SQLSTATE {s}\n", .{state.string});
                    if (diagnostic.object.get("column_name")) |column| if (column == .string) {
                        if (@import("plan.zig").identifier(column.string)) |_| std.debug.print("column {s}\n", .{column.string}) else |_| {}
                    };
                }
            };
        } else |_| {}
    }
    std.debug.print("Antfly operation failed: {s}\n", .{c.antfly_error_code_name(code)});
    if (code == c.ANTFLY_OUTCOME_UNKNOWN) return error.AntflyOutcomeUnknown;
    return error.AntflyOperationFailed;
}

pub fn checkFile(a: std.mem.Allocator, path: []const u8) !void {
    const z = try a.dupeSentinel(u8, path, 0);
    defer a.free(z);
    var out: c.antfly_buffer = .{ .ptr = null, .len = 0 };
    defer c.antfly_buffer_free(&out);
    try check(c.antfly_lite_check_file_json(z, &out), out);
    const result = try std.json.parseFromSliceLeaky(Value, a, out.ptr[0..out.len], .{});
    if (!result.object.get("valid").?.bool) return error.InvalidAfliteFile;
}

pub fn equal(expected: Value, actual: Value) bool {
    if (expected == .float and actual == .integer) return actual.integer >= -9007199254740992 and actual.integer <= 9007199254740992 and expected.float == @as(f64, @floatFromInt(actual.integer));
    if (std.meta.activeTag(expected) != std.meta.activeTag(actual)) return false;
    return switch (expected) {
        .null => true,
        .bool => expected.bool == actual.bool,
        .integer => expected.integer == actual.integer,
        .float => expected.float == actual.float,
        .number_string => std.mem.eql(u8, expected.number_string, actual.number_string),
        .string => std.mem.eql(u8, expected.string, actual.string),
        .array => blk: {
            if (expected.array.items.len != actual.array.items.len) break :blk false;
            for (expected.array.items, actual.array.items) |e, v| if (!equal(e, v)) break :blk false;
            break :blk true;
        },
        .object => blk: {
            if (expected.object.count() != actual.object.count()) break :blk false;
            var it = expected.object.iterator();
            while (it.next()) |entry| if (!equal(entry.value_ptr.*, actual.object.get(entry.key_ptr.*) orelse break :blk false)) break :blk false;
            break :blk true;
        },
    };
}

pub fn integer(value: Value) !i64 {
    return switch (value) {
        .integer => value.integer,
        // The SQL ABI encodes integer cells as decimal strings to preserve
        // all 64 bits for clients with IEEE-754 JSON number implementations.
        .string => try std.fmt.parseInt(i64, value.string, 10),
        else => error.InvalidIntegerResponse,
    };
}

pub fn decode(a: std.mem.Allocator, kind: @import("plan.zig").Kind, value: Value, sql_null: bool) !Value {
    if (sql_null) return .null;
    return switch (kind) {
        .integer => .{ .integer = try integer(value) },
        // JSON columns are serialized JSON text in the SQL ABI, independently
        // of SQL NULL provenance; parse them once, including JSON strings/null.
        .json => if (value == .string) try std.json.parseFromSliceLeaky(Value, a, value.string, .{ .allocate = .alloc_always }) else error.InvalidJsonResponse,
        else => value,
    };
}

test "verification compares JSON objects independent of key order" {
    const a = std.testing.allocator;
    const lhs = try std.json.parseFromSlice(Value, a, "{\"x\":null,\"y\":[1,true]}", .{});
    defer lhs.deinit();
    const rhs = try std.json.parseFromSlice(Value, a, "{\"y\":[1,true],\"x\":null}", .{});
    defer rhs.deinit();
    try std.testing.expect(equal(lhs.value, rhs.value));
    try std.testing.expect(!equal(.{ .integer = 9007199254740993 }, .{ .float = 9007199254740992 }));
}
