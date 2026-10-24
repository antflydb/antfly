// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const vopr = @import("vopr");
const socket = @import("httpx").socket;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;

test "socket child cancellation preserves parent cancellation ownership" {
    inline for (.{ false, true }) |write| inline for (.{ false, true }) |cancel_parent| inline for (.{ false, true }) |cancel_timer| {
        const Sim = vopr.vopr_io.VoprIo;
        const Hook = struct {
            var sim: *Sim = undefined;
            var parent: @FieldType(@FieldType(Sim, "tasks"), "current") = null;
            fn operate(raw: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
                switch (operation) {
                    .net_read, .net_write => {
                        // A real cancellation point acknowledges only the
                        // operation child. The parent retains its own state.
                        if (cancel_timer) {
                            if (operation == .net_write) {
                                // Keep the write pending until the selected
                                // timer cancels it; a ready write may rightly win.
                                try sim.io().vtable.sleep(raw, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(60000) } });
                                return .{ .net_write = 1 };
                            }
                            return sim.io().vtable.operate(raw, operation);
                        }
                        sim.tasks.current.?.cancel_requested = true;
                        if (cancel_parent) parent.?.cancel_requested = true;
                        sim.tasks.checkCancel() catch |err| return err;
                        unreachable;
                    },
                    else => return sim.io().vtable.operate(raw, operation),
                }
            }
            fn sleep(raw: ?*anyopaque, duration: std.Io.Timeout) std.Io.Cancelable!void {
                if (!cancel_timer) return sim.io().vtable.sleep(raw, duration);
                sim.tasks.current.?.cancel_requested = true;
                if (cancel_parent) parent.?.cancel_requested = true;
                sim.tasks.checkCancel() catch |err| return err;
                unreachable;
            }
        };
        var sim = try Sim.init(.{});
        defer sim.deinit();
        Hook.sim = &sim;
        const base = sim.io();
        var vtable = base.vtable.*;
        vtable.operate = Hook.operate;
        vtable.sleep = Hook.sleep;
        const io = std.Io{ .userdata = base.userdata, .vtable = &vtable };
        const pair = try sim.network.createPair(.{ .family = .ip4, .mode = .stream }, 1);
        defer {
            pair[0].close(base);
            pair[1].close(base);
        }
        const Context = struct {
            done: bool = false,
            failure: ?anyerror = null,
            parent_canceled: bool = false,
            fn run(io_arg: std.Io, handle: std.Io.net.Socket.Handle, self: *@This()) void {
                Hook.parent = Hook.sim.tasks.current;
                var stream = socket.Socket.fromHandle(handle, io_arg);
                stream.recv_timeout_ms = 60000;
                stream.send_timeout_ms = 60000;
                var bytes: [1]u8 = undefined;
                const result = if (write) stream.send("x") else stream.recv(&bytes);
                _ = result catch |err| blk: {
                    self.failure = err;
                    break :blk 0;
                };
                io_arg.checkCancel() catch {
                    self.parent_canceled = true;
                };
                self.done = true;
            }
        };
        var context = Context{};
        var future = io.async(Context.run, .{ io, pair[0].handle, &context });
        var ready: vopr.transition.List = .{};
        defer ready.deinit(std.testing.allocator);
        var sink: vopr.event.Sink = .{};
        defer sink.deinit(std.testing.allocator);
        for (0..100) |_| {
            if (context.done) break;
            ready.items.clearRetainingCapacity();
            try sim.scheduler().enumerateReady(&ready, std.testing.allocator);
            try ready.canonicalize();
            try std.testing.expect(ready.items.items.len != 0);
            const selected = for (ready.items.items) |candidate| {
                if (!std.mem.eql(u8, candidate.name, "vopr-io.time_advance")) break candidate;
            } else ready.items.items[0];
            try sim.scheduler().executeReady(selected.id, &sink, std.testing.allocator);
        }
        try std.testing.expect(context.done);
        future.await(io);
        try sim.ensureNoCapabilityViolation();
        try std.testing.expectEqual(cancel_parent, context.parent_canceled);
        try std.testing.expectEqual(@as(?anyerror, if (write and cancel_parent) error.Canceled else if (write) error.SendFailed else error.RecvFailed), context.failure);
        try std.testing.expectEqual(@as(usize, 0), sim.resourceSnapshot().total_tasks);
    };
}
