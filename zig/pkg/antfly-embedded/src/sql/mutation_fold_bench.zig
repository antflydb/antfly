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

//! Standalone CPU/allocation comparison of session postimage folding only.
//! Run: zig run -O ReleaseFast pkg/antfly-embedded/src/sql/mutation_fold_bench.zig
const std = @import("std");
const fold = @import("mutation_fold.zig");
const Entry = struct {
    table: struct { physical_name: []const u8, id: u64, schema_version: u32 = 1, storage_mode: enum { relational } = .relational },
    mutation: struct { key: []const u8, row: ?u64, json_null_fields: []const []const u8 = &.{}, predicate_only: bool = false },
};

fn legacy(a: std.mem.Allocator, input: []const Entry) ![]Entry {
    var result: std.ArrayList(Entry) = .empty;
    errdefer result.deinit(a);
    try result.ensureTotalCapacity(a, input.len);
    for (input) |entry| {
        var found = false;
        for (result.items) |*prior| {
            if (!std.mem.eql(u8, prior.table.physical_name, entry.table.physical_name) or !std.mem.eql(u8, prior.mutation.key, entry.mutation.key)) continue;
            if (prior.table.schema_version != entry.table.schema_version) return error.PreparedGenerationChanged;
            if (!entry.mutation.predicate_only) {
                prior.mutation.row = entry.mutation.row;
                prior.mutation.json_null_fields = entry.mutation.json_null_fields;
                prior.mutation.predicate_only = false;
            }
            found = true;
            break;
        }
        if (!found) result.appendAssumeCapacity(entry);
    }
    return result.toOwnedSlice(a);
}

pub fn main(init: std.process.Init) !void {
    const count = fold.max_entries;
    const iterations = 20;
    var names: [count][32]u8 = undefined;
    var keys: [count][32]u8 = undefined;
    var input: [count]Entry = undefined;
    for ([_]struct { label: []const u8, tables: usize, unique: usize }{
        .{ .label = "distinct-one-table", .tables = 1, .unique = count },
        .{ .label = "distinct-many-tables", .tables = 64, .unique = count },
        .{ .label = "half-repeated", .tables = 8, .unique = count / 2 },
        .{ .label = "hot-rows", .tables = 4, .unique = 32 },
    }) |shape| {
        for (&input, &names, &keys, 0..) |*entry, *name, *key, i| {
            const ordinal = i % shape.unique;
            entry.* = .{ .table = .{ .physical_name = try std.fmt.bufPrint(name, "table_{d:0>5}", .{ordinal % shape.tables}), .id = ordinal % shape.tables + 1 }, .mutation = .{ .key = try std.fmt.bufPrint(key, "row_{d:0>5}", .{ordinal / shape.tables}), .row = i } };
        }
        const expected = try legacy(init.gpa, &input);
        defer init.gpa.free(expected);
        const observed = try fold.merge(Entry, init.gpa, &input);
        defer init.gpa.free(observed);
        if (expected.len != observed.len) return error.BenchmarkSemanticMismatch;
        for (expected, observed) |lhs, rhs| if (lhs.mutation.row != rhs.mutation.row or !std.mem.eql(u8, lhs.table.physical_name, rhs.table.physical_name) or !std.mem.eql(u8, lhs.mutation.key, rhs.mutation.key)) return error.BenchmarkSemanticMismatch;
        for ([_]bool{ false, true }) |indexed| {
            var counting = std.testing.FailingAllocator.init(init.gpa, .{});
            const a = counting.allocator();
            const started = std.Io.Clock.awake.now(init.io).nanoseconds;
            for (0..iterations) |_| {
                const output = if (indexed) try fold.merge(Entry, a, &input) else try legacy(a, &input);
                std.mem.doNotOptimizeAway(output);
                a.free(output);
            }
            const elapsed = std.Io.Clock.awake.now(init.io).nanoseconds - started;
            std.debug.print("mutation_fold shape={s} mode={s} input={d} output={d} iterations={d} ns/op={d:.1} allocations/op={d:.1} bytes/op={d:.1}\n", .{ shape.label, if (indexed) "indexed" else "legacy", count, observed.len, iterations, @as(f64, @floatFromInt(elapsed)) / iterations, @as(f64, @floatFromInt(counting.allocations)) / iterations, @as(f64, @floatFromInt(counting.allocated_bytes)) / iterations });
        }
    }
}
