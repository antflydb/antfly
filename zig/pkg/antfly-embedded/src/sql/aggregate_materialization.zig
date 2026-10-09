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

//! Semantic identity for native aggregate state. Output aliases, HAVING,
//! ordering and paging stay with SQL. Predicates, casts, expressions and
//! filtered aggregates require a separate equivalence proof before reuse.
const std = @import("std");
const catalog = @import("catalog.zig");
const binding = @import("aggregate_binding.zig");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const A = std.mem.Allocator;

pub const Key = struct {
    path: []const u8,
    type: @import("ast.zig").ColumnType,
    nullable: bool,
};
pub const Input = struct {
    spec: operators.AggregateSpec,
    /// null is COUNT(*), which includes SQL NULL rows. COUNT(column) carries
    /// its column identity even when its reducer has the same output type.
    column: ?Key,
};
pub const Recipe = struct {
    keys: []const Key,
    inputs: []const Input,

    pub fn fingerprint(self: Recipe) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("native-sql-aggregate-recipe-v1");
        word(&hash, self.keys.len);
        for (self.keys) |key| keyHash(&hash, key);
        word(&hash, self.inputs.len);
        for (self.inputs) |input| {
            part(&hash, @tagName(input.spec.kind));
            if (input.spec.input_type) |kind| part(&hash, @tagName(kind)) else part(&hash, "*");
            hash.update(&.{ @intFromBool(input.spec.distinct), @intFromBool(input.column != null) });
            if (input.column) |column| keyHash(&hash, column);
        }
        return hash.finalResult();
    }
    pub fn eql(self: Recipe, other: Recipe) bool {
        if (self.keys.len != other.keys.len or self.inputs.len != other.inputs.len) return false;
        for (self.keys, other.keys) |left, right| if (!keyEql(left, right)) return false;
        for (self.inputs, other.inputs) |left, right| {
            if (!std.meta.eql(left.spec, right.spec) or (left.column == null) != (right.column == null)) return false;
            if (left.column) |column| if (!keyEql(column, right.column.?)) return false;
        }
        return true;
    }
};
fn keyEql(left: Key, right: Key) bool {
    return left.type == right.type and left.nullable == right.nullable and std.mem.eql(u8, left.path, right.path);
}
fn word(hash: *std.crypto.hash.sha2.Sha256, value: usize) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, @intCast(value), .little);
    hash.update(&bytes);
}
fn part(hash: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    word(hash, value.len);
    hash.update(value);
}
fn keyHash(hash: *std.crypto.hash.sha2.Sha256, key: Key) void {
    part(hash, key.path);
    part(hash, @tagName(key.type));
    hash.update(&.{@intFromBool(key.nullable)});
}
fn direct(table: catalog.Table, columns: []const scalar.Column, program: scalar.Program) !?Key {
    if (program.instructions.len != 1 or program.root != 0 or program.instructions[0].operation != .column) return null;
    const ordinal = program.instructions[0].operation.column;
    if (ordinal >= columns.len) return error.InvalidSqlBackendResponse;
    const column = table.column(columns[ordinal].name) catch return null;
    if (std.mem.eql(u8, column.name, "_id") or program.output_type.kind != column.type) return null;
    return .{ .path = column.path, .type = column.type, .nullable = column.nullable };
}

/// The result borrows column names from the request-owned table. Arrays belong
/// to the supplied allocator, normally the statement arena. Unsupported
/// expressions return null; allocation and corrupt binding failures propagate.
pub fn fromBound(a: A, table: catalog.Table, bound: binding.Bound) !?Recipe {
    if (bound.input.predicate != null) return null;
    if (bound.specs.len != bound.inputs.len or bound.inputs.len != bound.filters.len or bound.group_count > bound.input.projections.len) return error.InvalidSqlBackendResponse;
    for (bound.filters) |filter| if (filter != null) return null;
    const keys = try a.alloc(Key, bound.group_count);
    errdefer a.free(keys);
    for (keys, bound.input.projections[0..keys.len]) |*key, program| {
        key.* = (try direct(table, bound.input.columns, program orelse return error.InvalidSqlBackendResponse)) orelse {
            a.free(keys);
            return null;
        };
    }
    const inputs = try a.alloc(Input, bound.inputs.len);
    errdefer a.free(inputs);
    for (inputs, bound.inputs, bound.specs) |*input, slot, spec| {
        if (slot == null and spec.kind != .count) return error.InvalidSqlBackendResponse;
        if (spec.kind == .pattern_set) {
            a.free(keys);
            a.free(inputs);
            return null;
        }
        input.* = .{ .spec = spec, .column = if (slot) |index| column: {
            if (index >= bound.input.projections.len) return error.InvalidSqlBackendResponse;
            break :column (try direct(table, bound.input.columns, bound.input.projections[index] orelse return error.InvalidSqlBackendResponse)) orelse {
                a.free(keys);
                a.free(inputs);
                return null;
            };
        } else null };
    }
    return .{ .keys = keys, .inputs = inputs };
}

/// COUNT(*) has a dedicated compiler path. It still uses the same semantic
/// identity and exact partial provider, avoiding a second index protocol.
pub fn countFromProvider(context: anytype, table: catalog.Table) !?u64 {
    const open = context.backend.vtable.aggregate_partials orelse return null;
    const spec: operators.AggregateSpec = .{ .kind = .count };
    const recipe: Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = null }} };
    const cursor = (try open(context.backend.ptr, context.alloc, table, recipe)) orelse return null;
    defer cursor.close(cursor.ptr);
    var count: u64 = 0;
    var pages: usize = 0;
    var records: usize = 0;
    while (true) {
        try context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const partials = (try cursor.next(cursor.ptr, a, context.limits.executionRows())) orelse break;
        if (partials.len == 0 or partials.len > context.limits.executionRows()) return error.InvalidSqlBackendResponse;
        pages += 1;
        if (pages > context.limits.scan_pages or partials.len > context.limits.scan_rows -| records) return error.SqlProgramLimitExceeded;
        records += partials.len;
        for (partials) |partial| {
            if (partial.keys.len != 0 or partial.aggregates.len != 1) return error.InvalidSqlBackendResponse;
            if (partial.aggregate_slots) |slots| if (slots.len != 1 or slots[0] != 0) return error.InvalidSqlBackendResponse;
            var state = try @import("aggregate_partial.zig").decode(a, partial.aggregates[0], spec);
            defer state.deinit();
            count = std.math.add(u64, count, state.count) catch return error.SqlNumericOutOfRange;
            if (count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
        }
    }
    return count;
}

test "SQL native materialization recipe proves bound columns and null semantics" {
    const a = std.testing.allocator;
    const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{
        .{ .name = "k", .path = "literal.k", .type = .string },
        .{ .name = "n", .path = "n", .type = .integer },
    } };
    const cases = [_][]const u8{
        "SELECT k, SUM(n) AS total FROM docs GROUP BY k",
        "SELECT k, SUM(n) AS other FROM docs GROUP BY k HAVING SUM(n) > 0 ORDER BY SUM(n) DESC LIMIT 1",
        "SELECT k, SUM(n) FROM docs WHERE n > 0 GROUP BY k",
        "SELECT k, SUM(n + 1) FROM docs GROUP BY k",
        "SELECT LOWER(k), SUM(n) FROM docs GROUP BY LOWER(k)",
        "SELECT k, SUM(n) FILTER (WHERE n > 0) FROM docs GROUP BY k",
        "SELECT k, SUM(CAST(n AS DOUBLE PRECISION)) FROM docs GROUP BY k",
        "SELECT COUNT(*) FROM docs HAVING COUNT(*) >= 0",
        "SELECT COUNT(n) FROM docs",
        "SELECT COUNT(DISTINCT n) FROM docs",
    };
    var reference: ?[32]u8 = null;
    var count_star: ?[32]u8 = null;
    var count_column: ?[32]u8 = null;
    for (cases, 0..) |sql, index| {
        var compiled = try @import("compiler.zig").compile(a, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const bound = binding.bind(scratch, table, compiled.statement.select, &.{}) catch |err| {
            std.debug.print("materialization recipe SQL failed to bind: {s} ({s})\n", .{ sql, @errorName(err) });
            return err;
        };
        // Programs own binding arenas separately from the caller's arena.
        defer {
            for (bound.input.projections) |optional| if (optional) |program| {
                var owned = program;
                owned.deinit();
            };
            if (bound.input.predicate) |program| {
                var owned = program;
                owned.deinit();
            }
            for (bound.outputs) |program| {
                var owned = program;
                owned.deinit();
            }
            for (bound.orders) |program| {
                var owned = program;
                owned.deinit();
            }
            if (bound.having) |program| {
                var owned = program;
                owned.deinit();
            }
        }
        const recipe = try fromBound(scratch, table, bound);
        if (index >= 2 and index <= 6) {
            try std.testing.expect(recipe == null);
            continue;
        }
        const fingerprint = recipe.?.fingerprint();
        if (index == 0) {
            reference = fingerprint;
            try std.testing.expectEqualStrings("literal.k", recipe.?.keys[0].path);
        } else if (index == 1) try std.testing.expectEqual(reference.?, fingerprint) else if (index == 7) count_star = fingerprint else if (index == 8) {
            count_column = fingerprint;
            try std.testing.expect(!std.mem.eql(u8, &count_star.?, &fingerprint));
        } else try std.testing.expect(!std.mem.eql(u8, &count_column.?, &fingerprint));
    }
}
