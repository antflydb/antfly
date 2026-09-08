//! Standalone Zig 0.16 API probes for the std.Thread migration audit.
//! Run: zig test zig/scratch/std_io_migration_probe.zig -lc
const std = @import("std");
const Io = std.Io;

fn mark(value: *bool) void {
    value.* = true;
}

test "async can execute inline when the pool has no capacity" {
    var runtime = Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer runtime.deinit();
    const io = runtime.io();
    var called = false;
    var future = io.async(mark, .{&called});
    try std.testing.expect(called);
    future.await(io);
}

test "concurrent refuses unavailable capacity without starting the task" {
    var runtime = Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer runtime.deinit();
    var called = false;
    try std.testing.expectError(error.ConcurrencyUnavailable, runtime.io().concurrent(mark, .{&called}));
    try std.testing.expect(!called);
}

const Worker = struct {
    started: Io.Event = .unset,
    stop: Io.Event = .unset,
    completed: bool = false,

    fn run(self: *Worker, io: Io) void {
        self.started.set(io);
        self.stop.waitUncancelable(io);
        self.completed = true;
    }
};

test "owned future preserves explicit stop and join across restarts" {
    var runtime = Io.Threaded.init(std.testing.allocator, .{
        .concurrent_limit = .limited(1),
    });
    defer runtime.deinit();
    const io = runtime.io();
    for (0..3) |_| {
        var worker = Worker{};
        var future = try io.concurrent(Worker.run, .{ &worker, io });
        defer {
            worker.stop.set(io);
            future.await(io);
        }
        worker.started.waitUncancelable(io);
        worker.stop.set(io);
        future.await(io);
        try std.testing.expect(worker.completed);
    }
}

test "partial group startup needs an explicit wake before draining" {
    var runtime = Io.Threaded.init(std.testing.allocator, .{
        .concurrent_limit = .limited(1),
    });
    defer runtime.deinit();
    const io = runtime.io();
    var group: Io.Group = .init;
    var first = Worker{};
    var second = Worker{};
    defer {
        first.stop.set(io);
        second.stop.set(io);
        group.cancel(io);
    }
    try group.concurrent(io, Worker.run, .{ &first, io });
    first.started.waitUncancelable(io);
    try std.testing.expectError(error.ConcurrencyUnavailable, group.concurrent(io, Worker.run, .{ &second, io }));
    first.stop.set(io);
    try group.await(io);
    try std.testing.expect(first.completed);
    try std.testing.expect(!second.completed);
}
