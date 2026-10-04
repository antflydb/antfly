// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Repeatable native microbenchmarks. Fixtures live outside measured budgets;
//! both paths produce and validate the same outputs. No timing assertions.
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
    return std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
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
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var rows = try disk.Rows.init(a, &manager, 3);
    defer rows.deinit();
    const text = [_]u8{'x'} ** 8192;
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
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .buffer_bytes = buffer_bytes };
    defer manager.deinit();
    var sort = spill.Sort.init(a, &manager, &.{.{}}, 32768);
    defer sort.deinit();
    const text = [_]u8{'x'} ** 256;
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
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const bytes = 256 * 1024;
    const grace = if (partitioned) try @import("partition_join.zig").Join.create(a, &manager, bytes, count * 2, count * 256, false, false) else null;
    defer if (grace) |join| join.close();
    const hash = if (!partitioned) try operators.HashJoin.create(a, .{ .bytes = bytes / 4, .rows = count * 2, .spill = &manager }) else null;
    defer if (hash) |join| join.deinit();
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
                checksum += match.row.values[0].value.integer;
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

test "native refinements benchmark" {
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
