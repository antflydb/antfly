const std = @import("std");
const local = @import("antfly_local_sources");
const plans = @import("plan_cache");
pub const local_test_sources = struct {};
test "SQL cache allocator reproduction" {
    for ([_]bool{true}) |serial| {
        var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
        var backing: local.sql_memory_budget = .{ .backing = if (serial) no_resize.allocator() else std.testing.allocator, .limit = 8192 };
        var cache = plans.Cache.init(backing.allocator(), .{ .max_entries = 8, .max_bytes = 8192, .max_compile_bytes = 4096 });
        defer cache.deinit(std.testing.io);
        var diagnostic: local.sql_compiler.Diagnostic = .{};
        for (0..30) |i| {
            var buffer: [128]u8 = undefined;
            const statement = try std.fmt.bufPrint(&buffer, "SELECT _id,name,age FROM t WHERE age>{d}", .{i});
            var lease = cache.acquire(std.testing.io, .{ .statement = statement }, &diagnostic) catch |err| {
                std.debug.print("no_resize={} i={d} live={d} peak={d} diagnostic={s} error={}\n", .{ serial, i, backing.live, backing.peak, diagnostic.message, err });
                return err;
            };
            lease.release(std.testing.io);
            try std.testing.expectEqual(backing.live, cache.stats(std.testing.io).bytes);
        }
        std.debug.print("no_resize={} live={d} peak={d} evictions={d}\n", .{ serial, backing.live, backing.peak, cache.stats(std.testing.io).evictions });
        try std.testing.expect(cache.stats(std.testing.io).evictions > 0);
        try std.testing.expect(backing.peak <= cache.config.max_bytes);
        try std.testing.expect(!backing.exhausted);
    }
}
