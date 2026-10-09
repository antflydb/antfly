// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Owner-produced SQL lanes cross the kernel boundary as versioned capabilities.
//! Imported receivers remain at stable addresses until the producer joins the
//! lane; neither the kernel nor a request interprets its owner's runtime/vtable.
const std = @import("std");
const local = @import("antfly_local_sources");
const abi = @import("kernel_abi.zig");
const Runtime = local.storage_background_runtime.BackendRuntime;
const Receiver = @import("antfly_runtime_abi").io_abi.Receiver;

pub fn factory(runtime: *Runtime) ?abi.SqlWorkerFactory {
    // Borrowed leases alias general IO and cannot prove lane isolation.
    if (runtime.borrowed_io != null or runtime.lane_limits.worker_capacity < 10) return null;
    return .{ .context = runtime, .capacity = runtime.lane_limits.worker_capacity, .acquire = acquire };
}
const Owned = struct {
    runtime: *Runtime,
    workers: Runtime.WorkerLease,
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const allocator = self.runtime.alloc;
        self.workers.release();
        allocator.destroy(self);
    }
};
fn acquire(raw: *anyopaque, capacity: usize, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
    const runtime: *Runtime = @ptrCast(@alignCast(raw));
    const owned = runtime.alloc.create(Owned) catch return abi.statusFromError(error.SqlWriteCapacityUnavailable);
    owned.* = .{ .runtime = runtime, .workers = runtime.acquireWorkers(.{ .capacity = capacity }) catch {
        runtime.alloc.destroy(owned);
        return abi.statusFromError(error.SqlWriteCapacityUnavailable);
    } };
    out.* = .{ .context = owned, .capacity = owned.workers.capacity, .io = .init(&owned.workers.io()), .release = Owned.release };
    return .ok;
}

pub const Lease = struct {
    capacity: usize,
    native: ?Runtime.WorkerLease = null,
    imported: ?abi.SqlWorkerBorrow = null,
    receiver: ?*Receiver = null,
    allocator: ?std.mem.Allocator = null,

    pub fn acquireNative(runtime: *Runtime, capacity: usize) !Lease {
        const workers = try runtime.acquireWorkers(.{ .capacity = capacity });
        return .{ .capacity = workers.capacity, .native = workers };
    }
    pub fn acquireImported(allocator: std.mem.Allocator, producer: abi.SqlWorkerFactory, capacity: usize) !Lease {
        if (!abi.validContext(abi.SqlWorkerFactory, producer.version, producer.struct_size)) return error.UnsupportedVersion;
        if (capacity == 0 or capacity > producer.capacity) return error.SqlWriteCapacityUnavailable;
        var borrow: abi.SqlWorkerBorrow = undefined;
        const status = producer.acquire(producer.context, capacity, &borrow);
        if (!status.isOk()) return abi.errorFromStatus(status);
        errdefer borrow.release(borrow.context);
        if (!abi.validContext(abi.SqlWorkerBorrow, borrow.version, borrow.struct_size) or borrow.capacity != capacity)
            return error.InvalidArgument;
        const received = try borrow.io.receive();
        const receiver = try allocator.create(Receiver);
        receiver.* = received;
        return .{ .capacity = capacity, .imported = borrow, .receiver = receiver, .allocator = allocator };
    }
    pub fn io(self: *const Lease) std.Io {
        if (self.native) |workers| return workers.io();
        return self.receiver.?.io();
    }
    pub fn release(self: *Lease) void {
        if (self.native) |*workers| {
            workers.release();
            self.native = null;
        }
        if (self.imported) |borrow| {
            // The receiver stays valid while the owner's executor joins.
            borrow.release(borrow.context);
            self.allocator.?.destroy(self.receiver.?);
            self.imported = null;
            self.receiver = null;
        }
    }
};

test "SQL imported worker lanes preserve control and reaper progress under saturation" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16 } });
    defer runtime.deinit();
    var producer = factory(runtime.ptr()).?;
    // Force the receiver path even in this one-codegen test. The producer
    // trampoline uses its own executor and error-domain dispatch.
    const Forward = struct {
        const Dispatch = @FieldType(abi.native_abi.IoBorrow, "dispatch");
        var owner_dispatch: Dispatch = undefined;
        const P = @typeInfo(@typeInfo(Dispatch).pointer.child).@"fn".param_types;
        fn dispatch(a: P[0].?, b: P[1].?, c: P[2].?, d: P[3].?, e: P[4].?, f: P[5].?) callconv(.c) void {
            owner_dispatch(a, b, c, d, e, f);
        }
        fn create(raw: *anyopaque, capacity: usize, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, capacity, out);
            if (result.isOk()) {
                owner_dispatch = out.io.dispatch;
                out.io.dispatch = dispatch;
            }
            return result;
        }
    };
    producer.acquire = Forward.create;
    var foreground = try Lease.acquireImported(std.testing.allocator, producer, 1);
    defer foreground.release();
    var cleanup = try Lease.acquireImported(std.testing.allocator, producer, 1);
    defer cleanup.release();
    var control = try Lease.acquireImported(std.testing.allocator, producer, 1);
    defer control.release();
    var reaper = try Lease.acquireImported(std.testing.allocator, producer, 1);
    defer reaper.release();
    const foreground_io = foreground.io();
    const cleanup_io = cleanup.io();
    const control_io = control.io();
    const reaper_io = reaper.io();
    try std.testing.expect(@as(*const anyopaque, foreground_io.vtable) != foreground.imported.?.io.vtable);
    try std.testing.expect(foreground_io.userdata != cleanup_io.userdata);
    try std.testing.expect(control_io.userdata != reaper_io.userdata);
    try std.testing.expectEqual(@as(usize, 4), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Lease.acquireImported(std.testing.allocator, producer, 13));
    try std.testing.expectEqual(@as(usize, 4), runtime.ptr().laneStats().reserved_workers);
    const Block = struct {
        fn run(ready: *std.Io.Event, stop: *std.Io.Event, io: std.Io) void {
            ready.set(io);
            stop.wait(io) catch {};
        }
        fn probe(io: std.Io) !void {
            try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().statFile(io, "/antfly-sql-imported-worker-missing", .{}));
        }
    };
    var foreground_ready: std.Io.Event = .unset;
    var cleanup_ready: std.Io.Event = .unset;
    var foreground_stop: std.Io.Event = .unset;
    var cleanup_stop: std.Io.Event = .unset;
    var foreground_task = try foreground_io.concurrent(Block.run, .{ &foreground_ready, &foreground_stop, foreground_io });
    var foreground_open = true;
    defer if (foreground_open) {
        foreground_stop.set(std.testing.io);
        foreground_task.await(foreground_io);
    };
    var cleanup_task = try cleanup_io.concurrent(Block.run, .{ &cleanup_ready, &cleanup_stop, cleanup_io });
    var cleanup_open = true;
    defer if (cleanup_open) {
        cleanup_stop.set(std.testing.io);
        cleanup_task.await(cleanup_io);
    };
    try foreground_ready.wait(std.testing.io);
    try cleanup_ready.wait(std.testing.io);
    try std.testing.expectError(error.ConcurrencyUnavailable, foreground_io.concurrent(Block.probe, .{foreground_io}));
    try std.testing.expectError(error.ConcurrencyUnavailable, cleanup_io.concurrent(Block.probe, .{cleanup_io}));
    var control_task = try control_io.concurrent(Block.probe, .{control_io});
    try control_task.await(control_io);
    var reaper_task = try reaper_io.concurrent(Block.probe, .{reaper_io});
    try reaper_task.await(reaper_io);
    foreground_stop.set(std.testing.io);
    foreground_task.await(foreground_io);
    foreground_open = false;
    cleanup_stop.set(std.testing.io);
    cleanup_task.await(cleanup_io);
    cleanup_open = false;
    foreground.release();
    cleanup.release();
    control.release();
    reaper.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
}

test "SQL imported worker acquisition retires owner capacity after receiver failure" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16 } });
    defer runtime.deinit();
    var producer = factory(runtime.ptr()).?;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Lease.acquireImported(failing.allocator(), producer, 2));
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    const Bad = struct {
        fn create(raw: *anyopaque, capacity: usize, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, capacity, out);
            if (result.isOk()) out.io.contract.version +%= 1;
            return result;
        }
    };
    producer.acquire = Bad.create;
    try std.testing.expectError(error.InvalidArgument, Lease.acquireImported(std.testing.allocator, producer, 2));
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
    producer.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, Lease.acquireImported(std.testing.allocator, producer, 2));
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
}

test "SQL imported worker factory rejects borrowed aliases and small envelopes" {
    var borrowed = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual, .borrowed_io = .{ .general = std.testing.io } });
    defer borrowed.deinit();
    try std.testing.expect(factory(borrowed.ptr()) == null);
    var small = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 9 } });
    defer small.deinit();
    try std.testing.expect(factory(small.ptr()) == null);
}
