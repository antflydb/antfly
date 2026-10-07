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

//! Repeatable native microbenchmarks. Fixtures live outside measured budgets;
//! both paths produce and validate the same outputs. No timing assertions.
const native_platform = @import("antfly_platform");
const std = @import("std");

const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const operators = @import("operators.zig");
const disk = @import("disk_rows.zig");
const spill = @import("spill.zig");
// Count admitted backing allocations without adding bookkeeping to production
// execution. Reallocations are reported separately from fresh allocations.
const CountingAllocator = struct {
    backing: std.mem.Allocator = std.testing.allocator,
    allocations: usize = 0,
    reallocations: usize = 0,
    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ptr));
        const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
        self.allocations += 1;
        return result;
    }
    fn resize(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ptr));
        if (!self.backing.rawResize(bytes, alignment, len, ra)) return false;
        self.reallocations += 1;
        return true;
    }
    fn remap(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ptr));
        const result = self.backing.rawRemap(bytes, alignment, len, ra) orelse return null;
        self.reallocations += 1;
        return result;
    }
    fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ptr));
        self.backing.rawFree(bytes, alignment, ra);
    }
};
fn now() i96 {
    return std.Io.Clock.now(.awake, native_platform.testing.io).nanoseconds;
}
fn expression(vector: bool, program: *const scalar.Program, rows: []const []const Datum, count: usize) !struct { ns: i96, peak: usize, checksum: f64 } {
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    var checksum: f64 = 0;
    const start = now();
    for (0..count / rows.len) |_| {
        _ = arena.reset(.free_all);
        const a = arena.allocator();
        const values = if (vector) (try @import("vector_eval.zig").evaluate(a, program, rows, &.{})).? else blk: {
            const values = try a.alloc(Datum, rows.len);
            for (rows, values) |row, *value| value.* = try program.evaluate(a, row, &.{}, .{});
            break :blk values;
        };
        for (values) |value| checksum += value.value.float;
    }
    return .{ .ns = now() - start, .peak = budget.peak, .checksum = checksum };
}
fn aggregation(batched: bool, rows: []const []const Datum, count: usize) !struct { ns: i96, probes: u64, peak: usize, sum: i64 } {
    const specs = [_]operators.AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .bool_or } };
    var groups = try operators.Grouped.create(std.testing.allocator, &specs, .{});
    defer groups.deinit();
    const start = now();
    for (0..count / rows.len) |_| {
        if (batched) try groups.addGlobalBatch(rows) else for (rows) |row| try groups.add(&.{}, row);
    }
    const elapsed = now() - start;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const result = try groups.resultAt(arena.allocator(), 0);
    try std.testing.expectEqual(@as(i64, @intCast(count)), result.aggregates[0].value.integer);
    try std.testing.expect(result.aggregates[2].value.bool);
    return .{ .ns = elapsed, .probes = groups.hash_probes, .peak = groups.budget.peak, .sum = result.aggregates[1].value.integer };
}
fn windows(overlay: bool, count: usize) !struct { ns: i96, bytes: u64 } {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = native_platform.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var rows = try disk.Rows.init(a, &manager, 3);
    defer rows.deinit();
    const text: [8192]u8 = @splat('x');
    for (0..count) |index| try rows.append(.{ .values = &.{ Datum.json(.{ .string = &text }), .{}, .{} }, .keys = &.{}, .ordinal = index });
    const before = manager.written_bytes;
    const start = now();
    if (overlay) try rows.enableColumnUpdates(1);
    for (1..3) |column| for (0..count) |index| try rows.setCell(index, column, Datum.json(.{ .integer = @intCast(index + column) }));
    const elapsed = now() - start;
    const written = manager.written_bytes - before;
    for (0..count) |index| {
        const row = try rows.row(index);
        try std.testing.expectEqual(@as(i64, @intCast(index + 1)), row.values[1].value.integer);
        try std.testing.expectEqual(@as(i64, @intCast(index + 2)), row.values[2].value.integer);
        try std.testing.expectEqualStrings(&text, row.values[0].value.string);
    }
    return .{ .ns = elapsed, .bytes = written };
}
fn sorting(buffer_bytes: usize, count: usize) !struct { ns: i96, first_ns: i96, peak: usize, written: u64, reads: u64, writes: u64, allocations: usize, reallocations: usize } {
    var counted: CountingAllocator = .{};
    var budget: @import("memory_budget.zig") = .{ .backing = counted.allocator(), .limit = 8 * 1024 * 1024 };
    const a = budget.allocator();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = native_platform.testing.io, .context = &dummy, .checkpoint = Hook.check, .buffer_bytes = buffer_bytes };
    defer manager.deinit();
    var sort = spill.Sort.init(a, &manager, &.{.{}}, 32768);
    defer sort.deinit();
    const text: [256]u8 = @splat('x');
    const started = now();
    for (0..count) |i| try sort.add(.{ .values = &.{Datum.json(.{ .string = &text })}, .keys = &.{Datum.json(.{ .integer = @intCast(count - i) })}, .ordinal = i });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var first_ns: i96 = 0;
    for (0..count) |i| {
        _ = arena.reset(.free_all);
        const row = (try sort.next(arena.allocator())).?;
        if (i == 0) first_ns = now() - started;
        try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.keys[0].value.integer);
        try std.testing.expectEqualStrings(&text, row.values[0].value.string);
    }
    try std.testing.expect((try sort.next(arena.allocator())) == null);
    return .{ .ns = now() - started, .first_ns = first_ns, .peak = budget.peak, .written = manager.written_bytes, .reads = manager.read_calls, .writes = manager.write_calls, .allocations = counted.allocations, .reallocations = counted.reallocations };
}

fn joining(partitioned: bool, count: usize) !struct { ns: i96, first_ns: i96, peak: usize, written: u64, reads: u64, writes: u64, allocations: usize, matches: usize, checksum: i64, filtered: usize } {
    var counted: CountingAllocator = .{};
    var budget: @import("memory_budget.zig") = .{ .backing = counted.allocator(), .limit = 8 * 1024 * 1024 };
    const a = budget.allocator();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = native_platform.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const bytes = 256 * 1024;
    const grace = if (partitioned) try @import("partition_join.zig").Join.create(a, &manager, bytes, count * 2, count * 256, false, false) else null;
    defer if (grace) |join| join.close();
    const hash = if (!partitioned) try operators.HashJoin.create(a, .{ .bytes = bytes / 4, .rows = count * 2, .spill = &manager }) else null;
    defer if (hash) |join| join.deinit();
    var match_arena = std.heap.ArenaAllocator.init(a);
    defer match_arena.deinit();
    const started = now();
    for (0..count) |i| {
        const key = Datum.json(.{ .integer = @intCast(i % (count / 2)) });
        if (grace) |join| try join.add(true, &.{key}, &.{key}, i) else try hash.?.add(&.{key}, &.{key});
    }
    var matches: usize = 0;
    var checksum: i64 = 0;
    var first_ns: i96 = 0;
    for (0..count) |i| {
        const key = Datum.json(.{ .integer = @intCast(if (i < count / 2) i else count + i) });
        if (grace) |join| {
            try join.add(false, &.{key}, &.{key}, i);
        } else {
            var probe = try hash.?.probe(&.{key});
            while (try probe.next()) |match| {
                if (matches == 0) first_ns = now() - started;
                matches += 1;
                _ = match_arena.reset(.retain_capacity);
                checksum += (try match.materializeKeys(match_arena.allocator()))[0].value.integer;
            }
        }
    }
    if (grace) |join| while (try join.next()) |pair| {
        try std.testing.expect(pair.match != null);
        if (matches == 0) first_ns = now() - started;
        matches += 1;
        checksum += pair.right.?[0].value.integer;
        try join.accept(pair.match.?);
    };
    try std.testing.expectEqual(count, matches);
    try std.testing.expectEqual(@as(i64, @intCast((count / 2) * (count / 2 - 1))), checksum);
    return .{ .ns = now() - started, .first_ns = first_ns, .peak = budget.peak, .written = manager.written_bytes, .reads = manager.read_calls, .writes = manager.write_calls, .allocations = counted.allocations, .matches = matches, .checksum = checksum, .filtered = if (grace) |join| join.filtered_rows else 0 };
}

fn typedState(count: usize) !void {
    var counted: CountingAllocator = .{};
    var budget: @import("memory_budget.zig") = .{ .backing = counted.allocator(), .limit = 64 * 1024 * 1024 };
    const a = budget.allocator();
    var join_ns: i96 = 0;
    var join_peak: usize = 0;
    var join_allocations: usize = 0;
    {
        const join = try operators.HashJoin.create(a, .{ .bytes = 32 * 1024 * 1024, .rows = count });
        defer join.deinit();
        const started = now();
        for (0..count) |i| {
            const key = Datum.json(.{ .integer = @intCast(i) });
            try join.add(&.{ key, Datum.json(.{ .float = @as(f64, @floatFromInt(i)) / 8 }), Datum.json(.{ .bool = i % 2 == 0 }), Datum.json(.{ .string = "owned" }) }, &.{key});
        }
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        for (0..count) |i| {
            _ = arena.reset(.retain_capacity);
            var probe = try join.probe(&.{Datum.json(.{ .integer = @intCast(i) })});
            const match = (try probe.next()).?;
            const values = try match.materializeValues(arena.allocator());
            try std.testing.expectEqual(@as(i64, @intCast(i)), values[0].value.integer);
            try std.testing.expectEqualStrings("owned", values[3].value.string);
            try std.testing.expect((try probe.next()) == null);
        }
        join_ns = now() - started;
        join_peak = budget.peak;
        join_allocations = counted.allocations;
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    budget.peak = 0;
    counted.allocations = 0;
    {
        const specs = [_]operators.AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .min, .input_type = .string }, .{ .kind = .bool_or, .input_type = .boolean } };
        const grouped = try operators.Grouped.create(a, &specs, .{ .groups = count, .bytes = 32 * 1024 * 1024 });
        defer grouped.deinit();
        const started = now();
        for (0..count * 2) |i| {
            const key = Datum.json(.{ .integer = @intCast(i % count) });
            try grouped.add(&.{key}, &.{ key, key, Datum.json(.{ .string = "selected" }), Datum.json(.{ .bool = i % 2 == 0 }) });
        }
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        for (0..count) |i| {
            _ = arena.reset(.retain_capacity);
            const result = (try grouped.nextResult(arena.allocator())).?;
            try std.testing.expectEqual(@as(i64, 2), result.aggregates[0].value.integer);
            try std.testing.expectEqual(@as(i64, @intCast(i * 2)), result.aggregates[1].value.integer);
            try std.testing.expectEqualStrings("selected", result.aggregates[2].value.string);
        }
        std.debug.print("native_refinement {{\"case\":\"typed_join_group_state\",\"rows\":{d},\"join_ns\":{d},\"join_peak_bytes\":{d},\"join_allocations\":{d},\"group_ns\":{d},\"group_peak_bytes\":{d},\"group_allocations\":{d}}}\n", .{ count, join_ns, join_peak, join_allocations, now() - started, budget.peak, counted.allocations });
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

fn groupedColumns(batched: bool, count: usize) !struct { ns: i96, checksum: i64, peak: usize } {
    const specs = [_]operators.AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .bool_or } };
    const groups = try operators.Grouped.create(std.testing.allocator, &specs, .{});
    defer groups.deinit();
    var keys: [1024]Datum = undefined;
    var counts: [1024]Datum = undefined;
    var sums: [1024]Datum = undefined;
    var booleans: [1024]Datum = undefined;
    for (&keys, &counts, &sums, &booleans, 0..) |*key, *count_, *sum, *boolean, index| {
        key.* = Datum.json(.{ .integer = @intCast(index % 64) });
        count_.* = Datum.json(.{ .integer = 1 });
        sum.* = if (index % 17 == 0) .{} else Datum.json(.{ .integer = @intCast(index % 7) });
        boolean.* = Datum.json(.{ .bool = index % 3 == 0 });
    }
    const started = now();
    for (0..count / keys.len) |_| {
        if (batched) try groups.addColumns(&.{&keys}, &.{ &counts, &sums, &booleans }, keys.len) else for (keys, counts, sums, booleans) |key, count_, sum, boolean| try groups.add(&.{key}, &.{ count_, sum, boolean });
    }
    const elapsed = now() - started;
    var checksum: i64 = 0;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    for (0..groups.groupCount()) |index| {
        _ = arena.reset(.free_all);
        const result = try groups.resultAt(arena.allocator(), index);
        checksum += result.aggregates[0].value.integer + result.aggregates[1].value.integer + @as(i64, @intFromBool(result.aggregates[2].value.bool));
    }
    return .{ .ns = elapsed, .checksum = checksum, .peak = groups.budget.peak };
}

test "native refinements benchmark" {
    for ([_]usize{ 8192, 32768 }) |count| try typedState(count);
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "(n + 1.25) * 2.0 - 3.0", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .number }}, &.{}, .{});
    defer program.deinit();
    var numeric: [1024][1]Datum = undefined;
    var aggregate: [1024][3]Datum = undefined;
    var numeric_rows: [1024][]const Datum = undefined;
    var aggregate_rows: [1024][]const Datum = undefined;
    for (&numeric, &aggregate, &numeric_rows, &aggregate_rows, 0..) |*n, *agg, *nr, *ar, index| {
        n.* = .{Datum.json(.{ .float = @as(f64, @floatFromInt(index)) / 8.0 })};
        agg.* = .{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = @intCast(index % 7) }), Datum.json(.{ .bool = index % 2 == 0 }) };
        nr.* = n;
        ar.* = agg;
    }
    _ = try expression(false, &program, &numeric_rows, 1024);
    _ = try expression(true, &program, &numeric_rows, 1024);
    for ([_]usize{ 262144, 1048576 }) |count| for (0..3) |sample| {
        const first_expression = try expression(sample % 2 != 0, &program, &numeric_rows, count);
        const second_expression = try expression(sample % 2 == 0, &program, &numeric_rows, count);
        const baseline = if (sample % 2 == 0) first_expression else second_expression;
        const refined = if (sample % 2 == 0) second_expression else first_expression;
        try std.testing.expectEqual(baseline.checksum, refined.checksum);
        std.debug.print("native_refinement {{\"case\":\"float_expression\",\"rows\":{d},\"sample\":{d},\"scalar_ns\":{d},\"vector_ns\":{d},\"scalar_peak_bytes\":{d},\"vector_peak_bytes\":{d}}}\n", .{ count, sample, baseline.ns, refined.ns, baseline.peak, refined.peak });
        const first_group = try aggregation(sample % 2 != 0, &aggregate_rows, count);
        const second_group = try aggregation(sample % 2 == 0, &aggregate_rows, count);
        const scalar_group = if (sample % 2 == 0) first_group else second_group;
        const batch_group = if (sample % 2 == 0) second_group else first_group;
        try std.testing.expectEqual(scalar_group.sum, batch_group.sum);
        std.debug.print("native_refinement {{\"case\":\"global_aggregate\",\"rows\":{d},\"sample\":{d},\"scalar_ns\":{d},\"batch_ns\":{d},\"scalar_probes\":{d},\"batch_probes\":{d},\"scalar_peak_bytes\":{d},\"batch_peak_bytes\":{d}}}\n", .{ count, sample, scalar_group.ns, batch_group.ns, scalar_group.probes, batch_group.probes, scalar_group.peak, batch_group.peak });
    };
    for ([_]usize{ 262144, 1048576 }) |count| for (0..3) |sample| {
        const first = try groupedColumns(sample % 2 != 0, count);
        const second = try groupedColumns(sample % 2 == 0, count);
        const baseline = if (sample % 2 == 0) first else second;
        const refined = if (sample % 2 == 0) second else first;
        try std.testing.expectEqual(baseline.checksum, refined.checksum);
        std.debug.print("native_refinement {{\"case\":\"grouped_columns\",\"rows\":{d},\"sample\":{d},\"scalar_ns\":{d},\"batch_ns\":{d},\"scalar_peak_bytes\":{d},\"batch_peak_bytes\":{d}}}\n", .{ count, sample, baseline.ns, refined.ns, baseline.peak, refined.peak });
    };
    for ([_]usize{ 512, 4096 }) |count| for (0..3) |sample| {
        const first_window = try windows(sample % 2 != 0, count);
        const second_window = try windows(sample % 2 == 0, count);
        const baseline = if (sample % 2 == 0) first_window else second_window;
        const refined = if (sample % 2 == 0) second_window else first_window;
        std.debug.print("native_refinement {{\"case\":\"wide_window_updates\",\"rows\":{d},\"sample\":{d},\"row_ns\":{d},\"cell_ns\":{d},\"row_written_bytes\":{d},\"cell_written_bytes\":{d}}}\n", .{ count, sample, baseline.ns, refined.ns, baseline.bytes, refined.bytes });
    };
    for ([_]usize{ 1024, 4096 }) |count| {
        const chained = try joining(false, count);
        const partitioned = try joining(true, count);
        try std.testing.expectEqual(chained.matches, partitioned.matches);
        try std.testing.expectEqual(chained.checksum, partitioned.checksum);
        std.debug.print("native_refinement {{\"case\":\"partitioned_join\",\"rows_per_side\":{d},\"chained_ns\":{d},\"partitioned_ns\":{d},\"chained_first_row_ns\":{d},\"partitioned_first_row_ns\":{d},\"chained_peak_bytes\":{d},\"partitioned_peak_bytes\":{d},\"chained_written_bytes\":{d},\"partitioned_written_bytes\":{d},\"chained_reads\":{d},\"partitioned_reads\":{d},\"chained_writes\":{d},\"partitioned_writes\":{d},\"chained_allocations\":{d},\"partitioned_allocations\":{d},\"filtered_probes\":{d}}}\n", .{ count, chained.ns, partitioned.ns, chained.first_ns, partitioned.first_ns, chained.peak, partitioned.peak, chained.written, partitioned.written, chained.reads, partitioned.reads, chained.writes, partitioned.writes, chained.allocations, partitioned.allocations, partitioned.filtered });
    }
    for ([_]usize{ 2048, 8192 }) |count| {
        const unbuffered = try sorting(1, count);
        const buffered = try sorting(4096, count);
        std.debug.print("native_refinement {{\"case\":\"external_sort\",\"rows\":{d},\"unbuffered_ns\":{d},\"buffered_ns\":{d},\"unbuffered_first_row_ns\":{d},\"buffered_first_row_ns\":{d},\"unbuffered_peak_bytes\":{d},\"buffered_peak_bytes\":{d},\"spilled_bytes\":{d},\"unbuffered_reads\":{d},\"buffered_reads\":{d},\"unbuffered_writes\":{d},\"buffered_writes\":{d},\"unbuffered_allocations\":{d},\"buffered_allocations\":{d},\"unbuffered_reallocations\":{d},\"buffered_reallocations\":{d}}}\n", .{ count, unbuffered.ns, buffered.ns, unbuffered.first_ns, buffered.first_ns, unbuffered.peak, buffered.peak, buffered.written, unbuffered.reads, buffered.reads, unbuffered.writes, buffered.writes, unbuffered.allocations, buffered.allocations, unbuffered.reallocations, buffered.reallocations });
    }
}

fn nativeJoinBatch(batched: bool, count: usize) !struct { ns: i96, allocations: usize, peak: usize } {
    var counted: CountingAllocator = .{};
    var budget: @import("memory_budget.zig") = .{ .backing = counted.allocator(), .limit = 16 * 1024 * 1024 };
    const a = budget.allocator();
    var fixture = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer fixture.deinit();
    const f = fixture.allocator();
    const integers = try f.alloc(i64, count);
    const strings = try f.alloc([]const u8, count);
    const refs = try f.alloc(@import("../storage/rowsource/types.zig").RowRef, count);
    const selection = try f.alloc(usize, count);
    const keys = try f.alloc([]const Datum, count);
    const cells = try f.alloc(Datum, count);
    const repeated_payload = comptime block: {
        const part = "wide repeated projected payload";
        var payload: [part.len * 8]u8 = undefined;
        for (0..8) |repetition| @memcpy(payload[repetition * part.len ..][0..part.len], part);
        break :block payload;
    };
    for (integers, strings, refs, selection, keys, cells, 0..) |*integer, *string, *ref, *index, *key, *cell, i| {
        integer.* = @intCast(i);
        string.* = &repeated_payload;
        ref.* = .{ .relational_key = "id" };
        index.* = i;
        cell.* = Datum.json(.{ .integer = @intCast(i) });
        key.* = cells[i..][0..1];
    }
    const batch: @import("execution_batch.zig").Batch = .{ .columns = .{ .page = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = refs, .columns = &.{ .{ .name = "key", .values = .{ .i64 = integers } }, .{ .name = "payload", .values = .{ .bytes = strings } } } }, .selection = selection }, .definitions = &.{ .{ .name = "key", .type = .integer }, .{ .name = "payload", .type = .string } } } };
    var elapsed: i96 = 0;
    {
        const join = try operators.HashJoin.create(a, .{ .bytes = 8 * 1024 * 1024, .rows = count });
        defer join.deinit();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const started = now();
        for (0..count / 1024) |group| {
            _ = scratch.reset(.free_all);
            var part = batch;
            part.columns.page.selection = selection[group * 1024 ..][0..1024];
            if (batched) {
                try join.addBatch(scratch.allocator(), part, keys[group * 1024 ..][0..1024]);
            } else {
                const rows = try scratch.allocator().alloc([]const Datum, 1024);
                for (rows, 0..) |*row, i| {
                    const values = try part.row(scratch.allocator(), i);
                    const owned = try scratch.allocator().alloc(Datum, values.len);
                    for (values, owned) |value, *copy| copy.* = try operators.cloneDatum(scratch.allocator(), value);
                    row.* = owned;
                }
                for (rows, keys[group * 1024 ..][0..1024]) |row, key| try join.add(row, key);
            }
        }
        elapsed = now() - started;
        _ = scratch.reset(.free_all);
        var probe = try join.probe(keys[count - 1]);
        const match = (try probe.next()).?;
        const values = try match.materializeValues(scratch.allocator());
        try std.testing.expectEqualStrings(strings[count - 1], values[1].value.string);
        try std.testing.expect((try probe.next()) == null);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    return .{ .ns = elapsed, .allocations = counted.allocations, .peak = budget.peak };
}

test "native refinements benchmark borrowed operator batches" {
    for ([_]usize{ 8192, 32768 }) |count| for (0..3) |sample| {
        const first = try nativeJoinBatch(sample % 2 != 0, count);
        const second = try nativeJoinBatch(sample % 2 == 0, count);
        const baseline = if (sample % 2 == 0) first else second;
        const refined = if (sample % 2 == 0) second else first;
        std.debug.print("native_refinement {{\"case\":\"borrowed_join_batch\",\"rows\":{d},\"sample\":{d},\"rows_ns\":{d},\"batch_ns\":{d},\"rows_allocations\":{d},\"batch_allocations\":{d},\"rows_peak_bytes\":{d},\"batch_peak_bytes\":{d}}}\n", .{ count, sample, baseline.ns, refined.ns, baseline.allocations, refined.allocations, baseline.peak, refined.peak });
    };
}

fn sharedExpressions(fused: bool, programs: []const *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column) !struct { ns: i96, peak: usize, checksum: i64 } {
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 2 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    var checksum: i64 = 0;
    const start = now();
    for (0..256) |_| {
        _ = arena.reset(.free_all);
        const a = arena.allocator();
        if (fused) {
            const outputs = try @import("vector_eval.zig").evaluateColumnsMany(a, programs, page, columns, &.{});
            for (outputs) |vector| for (vector.?) |value| {
                if (!value.sql_null) checksum += value.value.integer;
            };
        } else {
            for (programs) |program| {
                const vector = (try @import("vector_eval.zig").evaluateColumns(a, program, page, columns, &.{})).?;
                for (vector) |value| if (!value.sql_null) {
                    checksum += value.value.integer;
                };
            }
        }
    }
    return .{ .ns = now() - start, .peak = budget.peak, .checksum = checksum };
}
fn resultDelivery(blocks: bool, count: usize) !struct { ns: i96, peak: usize, written: u64, reads: u64, writes: u64, checksum: i64 } {
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 1024 * 1024 };
    const a = budget.allocator();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = native_platform.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var rows: ?disk.Rows = if (!blocks) try disk.Rows.init(a, &manager, 4) else null;
    defer if (rows) |*owner| owner.deinit();
    const cursor = if (blocks) try @import("result_cursor.zig").Cursor.create(a, &manager, 4) else null;
    defer if (cursor) |owner| owner.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const start = now();
    for (0..count) |i| {
        const values = &.{ Datum.json(.{ .integer = @intCast(i) }), Datum.json(.{ .string = "shared result payload" }), Datum.json(.null), Datum{} };
        if (cursor) |owner| try @import("result_cursor.zig").Cursor.append(owner, values) else try rows.?.append(.{ .values = values, .keys = &.{}, .ordinal = i });
    }
    var checksum: i64 = 0;
    for (0..count) |i| {
        _ = arena.reset(.free_all);
        const alloc = arena.allocator();
        const values = if (cursor) |owner| (try owner.next(alloc)).? else blk: {
            const borrowed = (try rows.?.row(i)).values;
            const owned = try alloc.alloc(Datum, borrowed.len);
            for (borrowed, owned) |value, *out| out.* = try operators.cloneDatum(alloc, value);
            break :blk owned;
        };
        checksum += values[0].value.integer;
        try std.testing.expectEqualStrings("shared result payload", values[1].value.string);
        try std.testing.expect(!values[2].sql_null and values[2].value == .null and values[3].sql_null);
    }
    return .{ .ns = now() - start, .peak = budget.peak, .written = manager.written_bytes, .reads = manager.read_calls, .writes = manager.write_calls, .checksum = checksum };
}
test "native pipeline refinements benchmark" {
    const a = std.testing.allocator;
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    for ([_][]const u8{ "(n + 2) * 3", "((((n + 2) * 3 - 5) * 2 + 11) * 3 - 7) % 97" }, [_][]const u8{ "short", "shared_chain" }) |prefix, shape| {
        var compiled: [3]@import("compiler.zig").CompiledScalar = undefined;
        var programs: [3]scalar.Program = undefined;
        for ([_][]const u8{ "+ 7", "- 7", "* 2" }, &compiled, &programs) |suffix, *expression_, *program| {
            const sql = try std.fmt.allocPrint(a, "({s}) {s}", .{ prefix, suffix });
            defer a.free(sql);
            expression_.* = try @import("compiler.zig").compileScalar(a, sql, .{});
            program.* = try scalar.bind(a, expression_.expression, &definitions, &.{}, .{});
        }
        defer {
            for (&compiled, &programs) |*expression_, *program| {
                program.deinit();
                expression_.deinit();
            }
        }
        const pointers = [_]*const scalar.Program{ &programs[0], &programs[1], &programs[2] };
        const types = @import("../storage/rowsource/types.zig");
        const refs: [512]types.RowRef = @splat(.{ .relational_key = "r" });
        var values: [512]i64 = undefined;
        var selection: [512]usize = undefined;
        for (&values, &selection, 0..) |*value, *index, row| {
            value.* = @intCast(row);
            index.* = row;
        }
        const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &.{.{ .name = "n", .values = .{ .i64 = &values } }} }, .selection = &selection };
        for (0..3) |sample| {
            const first = try sharedExpressions(sample % 2 != 0, &pointers, page, &definitions);
            const second = try sharedExpressions(sample % 2 == 0, &pointers, page, &definitions);
            const separate = if (sample % 2 == 0) first else second;
            const fused = if (sample % 2 == 0) second else first;
            try std.testing.expectEqual(separate.checksum, fused.checksum);
            std.debug.print("native_pipeline {{\"case\":\"shared_expression_dag\",\"shape\":\"{s}\",\"rows\":131072,\"sample\":{d},\"separate_ns\":{d},\"fused_ns\":{d},\"separate_peak_bytes\":{d},\"fused_peak_bytes\":{d}}}\n", .{ shape, sample, separate.ns, fused.ns, separate.peak, fused.peak });
            if (!std.mem.eql(u8, shape, "short")) continue;
            const before = try resultDelivery(false, 4096);
            const after = try resultDelivery(true, 4096);
            try std.testing.expectEqual(before.checksum, after.checksum);
            std.debug.print("native_pipeline {{\"case\":\"blocking_result_delivery\",\"rows\":4096,\"sample\":{d},\"indexed_ns\":{d},\"block_ns\":{d},\"indexed_peak_bytes\":{d},\"block_peak_bytes\":{d},\"indexed_written_bytes\":{d},\"block_written_bytes\":{d},\"indexed_reads\":{d},\"block_reads\":{d},\"indexed_writes\":{d},\"block_writes\":{d}}}\n", .{ sample, before.ns, after.ns, before.peak, after.peak, before.written, after.written, before.reads, after.reads, before.writes, after.writes });
        }
    }
}

fn windowLayouts(shared: bool, count: usize, bytes: usize) !struct { ns: i96, written: u64, checksum: i64 } {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = native_platform.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    const payload = try a.alloc(u8, bytes);
    defer a.free(payload);
    @memset(payload, 'x');
    var rows = try disk.Rows.init(a, &manager, 2);
    defer rows.deinit();
    for (0..count) |index| try rows.append(.{ .values = &.{ Datum.json(.{ .string = payload }), .{} }, .keys = &.{}, .ordinal = index });
    const started = now();
    if (shared) try rows.enableColumnUpdates(1);
    for (0..3) |pass| {
        if (shared) {
            var order = try disk.Integers.init(&manager);
            defer order.deinit();
            for (0..count) |index| try order.append((index + pass) % count);
            var view: disk.View = .{ .source = &rows, .indices = &order, .len = count };
            for (0..count) |index| {
                const ordinal = (try view.row(index)).ordinal;
                try view.setCell(index, 1, Datum.json(.{ .integer = @intCast(ordinal + pass) }));
            }
        } else {
            var partition = try disk.Rows.init(a, &manager, 2);
            defer partition.deinit();
            var next = try disk.Rows.init(a, &manager, 2);
            errdefer next.deinit();
            for (0..count) |index| try partition.append(try rows.row((index + pass) % count));
            try partition.enableColumnUpdates(1);
            for (0..count) |index| {
                const ordinal = (try partition.row(index)).ordinal;
                try partition.setCell(index, 1, Datum.json(.{ .integer = @intCast(ordinal + pass) }));
                try next.append(try partition.row(index));
            }
            rows.deinit();
            rows = next;
        }
    }
    const elapsed = now() - started;
    var checksum: i64 = 0;
    for (0..count) |index| checksum += (try rows.cell(index, 1)).value.integer;
    return .{ .ns = elapsed, .written = manager.written_bytes, .checksum = checksum };
}
test "native refinements benchmark shared window permutations" {
    for ([_]usize{ 1024, 16384 }) |width| for (0..3) |sample| {
        const first = try windowLayouts(sample % 2 != 0, 256, width);
        const second = try windowLayouts(sample % 2 == 0, 256, width);
        const baseline = if (sample % 2 == 0) first else second;
        const refined = if (sample % 2 == 0) second else first;
        try std.testing.expectEqual(baseline.checksum, refined.checksum);
        std.debug.print("native_refinement {{\"case\":\"shared_window_permutations\",\"rows\":256,\"payload_bytes\":{d},\"sample\":{d},\"copy_ns\":{d},\"shared_ns\":{d},\"copy_written_bytes\":{d},\"shared_written_bytes\":{d}}}\n", .{ width, sample, baseline.ns, refined.ns, baseline.written, refined.written });
    };
}

fn windowColumnReads(columnar: bool, count: usize) !struct { ns: i96, read_bytes: u64, checksum: i64 } {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = native_platform.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var rows = try disk.Rows.init(a, &manager, 2);
    defer rows.deinit();
    if (columnar) try rows.enableColumns();
    const payload: [8192]u8 = @splat('x');
    for (0..count) |index| try rows.append(.{ .values = &.{ Datum.json(.{ .integer = @intCast(index) }), Datum.json(.{ .string = &payload }) }, .keys = &.{}, .ordinal = index });
    if (rows.columnar) |*store| try store.flush();
    const before = manager.read_bytes;
    const start = now();
    var checksum: i64 = 0;
    // Two key passes model sorting and partition/frame evaluation.
    for (0..2) |_| for (0..count) |index| {
        checksum += (try rows.cell(index, 0)).value.integer;
    };
    return .{ .ns = now() - start, .read_bytes = manager.read_bytes - before, .checksum = checksum };
}
test "native refinements benchmark column-addressable window keys" {
    for (0..3) |sample| {
        const first = try windowColumnReads(sample % 2 != 0, 512);
        const second = try windowColumnReads(sample % 2 == 0, 512);
        const baseline = if (sample % 2 == 0) first else second;
        const refined = if (sample % 2 == 0) second else first;
        try std.testing.expectEqual(baseline.checksum, refined.checksum);
        std.debug.print("native_refinement {{\"case\":\"column_window_keys\",\"rows\":512,\"payload_bytes\":8192,\"sample\":{d},\"row_ns\":{d},\"column_ns\":{d},\"row_read_bytes\":{d},\"column_read_bytes\":{d}}}\n", .{ sample, baseline.ns, refined.ns, baseline.read_bytes, refined.read_bytes });
    }
}
