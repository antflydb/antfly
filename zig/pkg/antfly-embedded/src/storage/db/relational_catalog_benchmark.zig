// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit microbenchmark; not part of the normal test aggregate.
const std = @import("std");
const catalog = @import("relational_integrity_catalog.zig");

test "benchmark relational catalog preparation scaling" {
    const alloc = std.testing.allocator;
    const iterations = 1000;
    for ([_]usize{ 64, 256, 1000 }) |count| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const temporary = arena.allocator();
        const definitions = try temporary.alloc(catalog.Definition, count);
        for (definitions, 0..) |*definition, i| definition.* = .{
            .kind = .unique,
            .name = try std.fmt.allocPrint(temporary, "index_{d:0>4}", .{i}),
            .fingerprint = @splat(@intCast(i % 255 + 1)),
        };
        var initial = try catalog.prepare(alloc, null, @splat(1), 1, @splat(2), definitions);
        defer initial.deinit();
        for ([_]bool{ false, true }) |retirement| {
            const selected = if (retirement) definitions[0 .. count / 2] else definitions;
            var checksum: usize = 0;
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            for (0..iterations) |_| {
                var update = try catalog.prepare(alloc, initial.value, @splat(1), 2, @splat(3), selected);
                checksum +%= update.value.len;
                update.deinit();
            }
            const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
            std.mem.doNotOptimizeAway(checksum);
            std.debug.print("catalog definitions={d} retirement={} iterations={d} ns_per_prepare={d}\n", .{ count, retirement, iterations, @divFloor(elapsed, iterations) });
        }
    }
}
