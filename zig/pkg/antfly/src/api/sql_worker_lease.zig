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
//! Imported receivers stay stable through each consumer's physical task joins.
//! Shared role leases retain the node executor until its final physical join;
//! neither the kernel nor a request interprets its owner's runtime/vtable.
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
    workers: union(enum) {
        exclusive: Runtime.WorkerLease,
        shared: Runtime.SharedSqlLease,
        fn release(self: *@This()) void {
            switch (self.*) {
                inline else => |*workers| workers.release(),
            }
        }
        fn io(self: *const @This()) std.Io {
            return switch (self.*) {
                inline else => |workers| workers.io(),
            };
        }
        fn capacity(self: *const @This()) usize {
            return switch (self.*) {
                inline else => |workers| workers.capacity,
            };
        }
    },
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const allocator = self.runtime.alloc;
        var workers = self.workers;
        // Retire wrapper metadata before releasing the final runtime gate.
        // Its allocator may belong to the concurrently shutting-down runtime.
        allocator.destroy(self);
        workers.release();
    }
};
fn acquire(raw: *anyopaque, role_value: c_int, capacity: usize, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
    const role = std.enums.fromInt(abi.SqlWorkerRole, role_value) orelse return abi.statusFromError(error.InvalidArgument);
    const runtime: *Runtime = @ptrCast(@alignCast(raw));
    const owned = runtime.alloc.create(Owned) catch return abi.statusFromError(error.SqlWriteCapacityUnavailable);
    owned.* = .{ .runtime = runtime, .workers = switch (role) {
        .exclusive => .{ .exclusive = runtime.acquireWorkers(.{ .capacity = capacity }) catch {
            runtime.alloc.destroy(owned);
            return abi.statusFromError(error.SqlWriteCapacityUnavailable);
        } },
        else => .{ .shared = runtime.acquireSharedSqlWorkers(switch (role) {
            .execution => .execution,
            .control => .control,
            .cleanup => .cleanup,
            .exclusive => unreachable,
        }, capacity) catch {
            runtime.alloc.destroy(owned);
            return abi.statusFromError(error.SqlWriteCapacityUnavailable);
        } },
    } };
    const io = owned.workers.io();
    out.* = .{ .context = owned, .capacity = owned.workers.capacity(), .io = .init(&io), .release = Owned.release };
    return .ok;
}

pub const Lease = struct {
    capacity: usize,
    native: ?Runtime.WorkerLease = null,
    shared: ?Runtime.SharedSqlLease = null,
    imported: ?abi.SqlWorkerBorrow = null,
    receiver: ?*Receiver = null,
    allocator: ?std.mem.Allocator = null,

    pub fn acquireNative(runtime: *Runtime, capacity: usize) !Lease {
        const workers = try runtime.acquireWorkers(.{ .capacity = capacity });
        return .{ .capacity = workers.capacity, .native = workers };
    }
    pub fn acquireNativeShared(runtime: *Runtime, role: abi.SqlWorkerRole, capacity: usize) !Lease {
        if (role == .exclusive) return acquireNative(runtime, capacity);
        const workers = try runtime.acquireSharedSqlWorkers(switch (role) {
            .execution => .execution,
            .control => .control,
            .cleanup => .cleanup,
            .exclusive => unreachable,
        }, capacity);
        return .{ .capacity = workers.capacity, .shared = workers };
    }
    pub fn acquireImported(allocator: std.mem.Allocator, producer: abi.SqlWorkerFactory, capacity: usize) !Lease {
        return acquireImportedShared(allocator, producer, .exclusive, capacity);
    }
    pub fn acquireImportedShared(allocator: std.mem.Allocator, producer: abi.SqlWorkerFactory, role: abi.SqlWorkerRole, capacity: usize) !Lease {
        if (!abi.validContext(abi.SqlWorkerFactory, producer.version, producer.struct_size)) return error.UnsupportedVersion;
        if (capacity == 0 or capacity > producer.capacity) return error.SqlWriteCapacityUnavailable;
        var borrow: abi.SqlWorkerBorrow = undefined;
        const status = producer.acquire(producer.context, @backingInt(role), capacity, &borrow);
        if (!status.isOk()) return abi.errorFromStatus(status);
        errdefer borrow.release(borrow.context);
        if (!abi.validContext(abi.SqlWorkerBorrow, borrow.version, borrow.struct_size) or borrow.capacity < capacity or borrow.capacity > producer.capacity or (role == .exclusive and borrow.capacity != capacity))
            return error.InvalidArgument;
        const received = try borrow.io.receive();
        const receiver = try allocator.create(Receiver);
        receiver.* = received;
        return .{ .capacity = borrow.capacity, .imported = borrow, .receiver = receiver, .allocator = allocator };
    }
    pub fn io(self: *const Lease) std.Io {
        if (self.native) |workers| return workers.io();
        if (self.shared) |workers| return workers.io();
        return self.receiver.?.io();
    }
    pub fn release(self: *Lease) void {
        if (self.shared) |*workers| {
            workers.release();
            self.shared = null;
        }
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
        fn create(raw: *anyopaque, role: c_int, capacity: usize, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, out);
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
        fn create(raw: *anyopaque, role: c_int, capacity: usize, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, out);
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

test "SQL shared worker roles reserve one node executor for multiple consumers" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var producer = factory(runtime.ptr()).?;
    const Forward = struct {
        const Dispatch = @FieldType(abi.native_abi.IoBorrow, "dispatch");
        var owner_dispatch: Dispatch = undefined;
        const P = @typeInfo(@typeInfo(Dispatch).pointer.child).@"fn".param_types;
        fn dispatch(a: P[0].?, b: P[1].?, c: P[2].?, d: P[3].?, e: P[4].?, f: P[5].?) callconv(.c) void {
            owner_dispatch(a, b, c, d, e, f);
        }
        fn create(raw: *anyopaque, role: c_int, capacity: usize, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, out);
            if (result.isOk()) {
                owner_dispatch = out.io.dispatch;
                out.io.dispatch = dispatch;
            }
            return result;
        }
    };
    producer.acquire = Forward.create;
    var first = try Lease.acquireImportedShared(std.testing.allocator, producer, .execution, 1);
    defer first.release();
    var peer = try Lease.acquireImportedShared(std.testing.allocator, producer, .execution, 1);
    defer peer.release();
    try std.testing.expectEqual(@as(usize, 11), first.capacity);
    try std.testing.expectEqual(first.imported.?.io.userdata, peer.imported.?.io.userdata);
    try std.testing.expect(@as(*const anyopaque, first.io().vtable) != first.imported.?.io.vtable);
    try std.testing.expectEqual(@as(usize, 11), runtime.ptr().laneStats().reserved_workers);
    var control = try Lease.acquireImportedShared(std.testing.allocator, producer, .control, 5);
    defer control.release();
    var cleanup = try Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 1);
    defer cleanup.release();
    try std.testing.expect(first.io().userdata != control.io().userdata);
    try std.testing.expect(control.io().userdata != cleanup.io().userdata);
    try std.testing.expectEqual(@as(usize, 24), runtime.ptr().laneStats().reserved_workers);
    const Block = struct {
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
        }
        fn probe() void {}
    };
    var block: Block = .{ .io = peer.io() };
    const peer_io = peer.io();
    var future = try peer_io.concurrent(Block.run, .{&block});
    var future_joined = false;
    defer if (!future_joined) future.await(peer_io);
    defer if (!future_joined) block.release.set(peer_io);
    try block.entered.waitTimeout(peer.io(), .{ .duration = .{ .raw = std.Io.Duration.fromSeconds(5), .clock = .awake } });
    // This consumer has no outstanding submissions. Releasing it must neither
    // cancel its peer nor retire the peer's shared executor reservation.
    first.release();
    try std.testing.expectEqual(@as(usize, 24), runtime.ptr().laneStats().reserved_workers);
    var health = try control.io().concurrent(Block.probe, .{});
    health.await(control.io());
    block.release.set(peer.io());
    future.await(peer_io);
    future_joined = true;
    peer.release();
    try std.testing.expectEqual(@as(usize, 13), runtime.ptr().laneStats().reserved_workers);
    control.release();
    cleanup.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
}

test "SQL shared worker consumer failures preserve peer capacity and reject invalid roles" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var producer = factory(runtime.ptr()).?;
    var peer = try Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 1);
    defer peer.release();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Lease.acquireImportedShared(failing.allocator(), producer, .cleanup, 1));
    try std.testing.expectEqual(@as(usize, 8), runtime.ptr().laneStats().reserved_workers);
    var out: abi.SqlWorkerBorrow = undefined;
    try std.testing.expect(!producer.acquire(producer.context, 999, 1, &out).isOk());
    try std.testing.expectEqual(@as(usize, 8), runtime.ptr().laneStats().reserved_workers);
    producer.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 1));
    try std.testing.expectEqual(@as(usize, 8), runtime.ptr().laneStats().reserved_workers);
    peer.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
}

test "SQL shared worker leases keep runtime alive through shutdown and physical callback join" {
    const builtin = @import("builtin");
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    const owner = runtime.ptr();
    const producer = factory(owner).?;
    const Probe = struct {
        io: std.Io = undefined,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        retired: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
            self.retired.store(true, .release);
        }
    };
    var probe: Probe = .{};
    var first: ?Lease = null;
    var peer: ?Lease = null;
    var callback: ?std.Io.Future(void) = null;
    var shutdown: ?std.Thread = null;
    var runtime_destroyed = false;
    var deinitialized = std.atomic.Value(bool).init(false);
    defer {
        if (callback != null) {
            probe.release.set(probe.io);
            callback.?.await(probe.io);
        }
        if (first) |*lease| lease.release();
        if (peer) |*lease| lease.release();
        if (shutdown) |thread| thread.join() else if (!runtime_destroyed) runtime.deinit();
    }
    first = try Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 1);
    peer = try Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 1);
    probe.io = peer.?.io();
    callback = try probe.io.concurrent(Probe.run, .{&probe});
    try probe.entered.waitTimeout(probe.io, .{ .duration = .{ .raw = std.Io.Duration.fromSeconds(5), .clock = .awake } });
    first.?.release();
    shutdown = try std.Thread.spawn(.{}, struct {
        fn run(handle: *local.storage_background_runtime.BackendRuntimeHandle, done: *std.atomic.Value(bool)) void {
            handle.deinit();
            done.store(true, .release);
        }
    }.run, .{ &runtime, &deinitialized });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    var closed = false;
    while (@import("antfly_platform").time.monotonicNs() < deadline) {
        if (owner.acquireSharedSqlWorkers(.cleanup, 1)) |lease| {
            var temporary = lease;
            temporary.release();
        } else |err| {
            if (err == error.BackendRuntimeShuttingDown) {
                closed = true;
                break;
            }
            return err;
        }
        std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
    try std.testing.expect(closed);
    try std.testing.expect(!deinitialized.load(.acquire));
    try std.testing.expect(!probe.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 8), owner.laneStats().reserved_workers);
    probe.release.set(probe.io);
    callback.?.await(probe.io);
    callback = null;
    try std.testing.expect(probe.retired.load(.acquire));
    try std.testing.expect(!deinitialized.load(.acquire));
    peer.?.release();
    shutdown.?.join();
    runtime_destroyed = true;
    shutdown = null;
    // Runtime was freed by the joined shutdown thread; do not use its I/O or
    // allocator after this point, including in the failure-cleanup defer.
    try std.testing.expect(deinitialized.load(.acquire));
}
