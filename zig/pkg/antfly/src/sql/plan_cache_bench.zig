// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).
//! Standalone compile/cache microbenchmark; not query/storage throughput.
const std = @import("std");
const compiler = @import("compiler.zig");
const plans = @import("plan_cache.zig");

pub fn main(init: std.process.Init) !void {
    var counting = std.testing.FailingAllocator.init(init.gpa, .{});
    const allocator = counting.allocator();
    const io = init.io;
    const statement = "SELECT _id,name,age FROM public.users WHERE age >= $1 AND enabled=true LIMIT 100";
    const iterations = 20_000;
    var cache = plans.Cache.init(allocator, .{ .max_entries = 1 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    var warm = try cache.acquire(io, .{ .statement = statement, .principal = "benchmark" }, &diagnostic);
    warm.release(io);
    for ([_][]const u8{ "uncached", "hit", "miss" }) |mode| {
        const initial_allocations = counting.allocations;
        const initial_bytes = counting.allocated_bytes;
        const start = std.Io.Clock.now(.awake, io).nanoseconds;
        for (0..iterations) |i| {
            if (std.mem.eql(u8, mode, "uncached")) {
                var compiled = try compiler.compile(allocator, statement, .{});
                std.mem.doNotOptimizeAway(compiled.statement);
                compiled.deinit();
            } else {
                var key: plans.Key = .{ .statement = statement, .principal = "benchmark" };
                if (std.mem.eql(u8, mode, "miss")) key.principal = if (i % 2 == 0) "scope-a" else "scope-b";
                var lease = try cache.acquire(io, key, &diagnostic);
                std.mem.doNotOptimizeAway(lease.compiled().statement);
                lease.release(io);
            }
        }
        const elapsed = std.Io.Clock.now(.awake, io).nanoseconds - start;
        std.debug.print("plan_cache mode={s} operations={d} ns/op={d:.1} allocations/op={d:.2} allocated_bytes/op={d:.1}\n", .{
            mode,                                                                           iterations,
            @as(f64, @floatFromInt(elapsed)) / iterations,                                  @as(f64, @floatFromInt(counting.allocations - initial_allocations)) / iterations,
            @as(f64, @floatFromInt(counting.allocated_bytes - initial_bytes)) / iterations,
        });
    }
    const state = cache.stats(io);
    std.debug.print("plan_cache retained_bytes={d} entries={d} hits={d} misses={d} evictions={d}\n", .{
        state.bytes, state.entries, state.hits, state.misses, state.evictions,
    });
}
