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

const std = @import("std");
const local = @import("antfly_local_sources");
const services = local.storage_execution_services;
const runtime = @import("antfly_runtime_abi");
const background = local.storage_background_runtime;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;

extern fn antfly_execution_fixture_create(*const runtime.memory_abi.Allocator, *services.Offer, *?*anyopaque) callconv(.c) runtime.error_abi.Status;
extern fn antfly_execution_fixture_pressure(*anyopaque, bool) callconv(.c) u64;
extern fn antfly_execution_fixture_destroy(*anyopaque) callconv(.c) void;
extern fn antfly_execution_fixture_invocations() callconv(.c) usize;
const Fixture = struct {
    allocator: std.mem.Allocator = std.testing.allocator,
    offer: services.Offer = undefined,
    context: ?*anyopaque = null,
    fn init(self: *@This()) !void {
        const port = runtime.memory_abi.Allocator.fromStd(&self.allocator);
        const status = antfly_execution_fixture_create(&port, &self.offer, &self.context);
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    }
    fn deinit(self: *@This()) void {
        antfly_execution_fixture_destroy(self.context.?);
    }
};
fn yieldUntil(value: *std.atomic.Value(bool)) !void {
    const end = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!value.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= end) return error.Timeout;
        std.Thread.yield() catch {};
    }
}

test "execution services archive boundary rejects incompatible offers before native dispatch" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    const before = antfly_execution_fixture_invocations();
    var bad = f.offer;
    bad.version = 0;
    bad.methods = @as(@typeInfo(@FieldType(services.Offer, "methods")).optional.child, @ptrFromInt(8));
    try std.testing.expectError(error.UnsupportedVersion, services.Client.open(&bad));
    bad.version = 1;
    try std.testing.expectError(error.UnsupportedVersion, services.Client.open(&bad));
    bad.version = 2;
    try std.testing.expectError(error.UnsupportedVersion, services.Client.open(&bad));
    bad = f.offer;
    bad.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, services.Client.open(&bad));
    bad = f.offer;
    bad.contract.compiler_id ^= 1;
    bad.methods = @as(@typeInfo(@FieldType(services.Offer, "methods")).optional.child, @ptrFromInt(8));
    try std.testing.expectError(error.UnsupportedVersion, services.Client.open(&bad));
    bad = f.offer;
    bad.contract.layout_id ^= 1;
    try std.testing.expectError(error.UnsupportedVersion, services.Client.open(&bad));
    bad = f.offer;
    bad.methods = null;
    try std.testing.expectError(error.InvalidArgument, services.Client.open(&bad));
    bad = f.offer;
    bad.context = null;
    try std.testing.expectError(error.InvalidArgument, services.Client.open(&bad));
    try std.testing.expectEqual(before, antfly_execution_fixture_invocations());
}

test "execution services archive boundary shares worker and SQL capacity across independent scopes" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var a = try services.Client.open(&f.offer);
    defer a.close() catch @panic("scope close");
    var b = try services.Client.open(&f.offer);
    defer b.close() catch @panic("scope close");
    var one = try a.acquireWorkers(.{});
    defer one.release() catch @panic("worker release");
    var two = try b.acquireWorkers(.{});
    defer two.release() catch @panic("worker release");
    try std.testing.expectError(error.WorkerCapacityExceeded, a.acquireWorkers(.{}));
    try std.testing.expectEqual(@as(usize, 2), (try a.stats()).reserved_service_workers);
    try std.testing.expectEqual(@as(usize, 2), (try b.stats()).reserved_service_workers);
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var io = try one.receiveIo();
    var future = try io.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), future.await(io.io()));
    try one.release();
    var replacement = try b.acquireWorkers(.{});
    defer replacement.release() catch @panic("worker release");
    var sql_a = try a.acquireSql(.control, 1);
    defer sql_a.release() catch @panic("SQL release");
    var sql_b = try b.acquireSql(.control, 1);
    defer sql_b.release() catch @panic("SQL release");
    try std.testing.expectEqual(sql_a.capacity(), sql_b.capacity());
    try std.testing.expectEqual(sql_a.capacity(), (try a.stats()).reserved_sql_workers);
    try sql_a.release();
    try std.testing.expectEqual(sql_b.capacity(), (try b.stats()).reserved_sql_workers);
    var sql_io = try sql_b.receiveIo();
    var sql_future = try sql_io.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), sql_future.await(sql_io.io()));
}

test "execution services archive boundary preserves protected control and Raft under exhausted ordinary memory" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var client = try services.Client.open(&f.offer);
    defer client.close() catch @panic("scope close");
    try std.testing.expectEqual(@as(u64, 2048 << 20), antfly_execution_fixture_pressure(f.context.?, true));
    defer _ = antfly_execution_fixture_pressure(f.context.?, false);
    try std.testing.expectError(error.OutOfMemory, client.acquireIo(.api));
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    for ([_]services.Purpose{ .control, .raft_inbound, .raft_outbound }) |purpose| {
        var lease = try client.acquireIo(purpose);
        defer lease.release() catch @panic("purpose release");
        var receiver = try lease.receiveIo();
        var future = try receiver.io().concurrent(Work.run, .{});
        try std.testing.expectEqual(@as(u32, 42), future.await(receiver.io()));
    }
    const stats = (try client.memoryStats()).?;
    try std.testing.expect(stats.control_heap.used > 0 and stats.raft_inbound.heap.used > 0 and stats.raft_outbound.heap.used > 0);
}

const JobProbe = struct {
    entered: std.atomic.Value(bool) = .init(false),
    go: std.atomic.Value(bool) = .init(true),
    destroyed: std.atomic.Value(bool) = .init(false),
    fn run(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.entered.store(true, .release);
        while (!self.go.load(.acquire)) std.Thread.yield() catch {};
    }
    fn deinit(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.destroyed.store(true, .release);
    }
    fn job(self: *@This(), owner: u64) background.Job {
        return .{ .owner_id = owner, .class = .commit_durable, .ptr = self, .run = run, .deinit = deinit };
    }
};

test "execution services archive boundary scopes durable owners and preserves named refusal and payload ownership" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var a = try services.Client.open(&f.offer);
    defer a.close() catch @panic("scope close");
    var b = try services.Client.open(&f.offer);
    defer b.close() catch @panic("scope close");
    const owner_a = try a.allocOwnerId();
    const owner_b = try b.allocOwnerId();
    try std.testing.expect(owner_a != owner_b);
    const lane_a = try a.durableJobs();
    const lane_b = try b.durableJobs();
    try std.testing.expect(!lane_a.executesInline());
    var probe: JobProbe = .{};
    try std.testing.expectError(error.BackgroundOwnerClosed, lane_a.submit(probe.job(owner_b)));
    try std.testing.expect(!probe.entered.load(.acquire) and !probe.destroyed.load(.acquire));
    try lane_b.pauseOwner(owner_b);
    try std.testing.expectError(error.BackgroundOwnerPaused, lane_b.submit(probe.job(owner_b)));
    try std.testing.expect(!probe.destroyed.load(.acquire));
    try lane_b.resumeOwner(owner_b);
    try a.close();
    try lane_b.submit(probe.job(owner_b));
    lane_b.drainOwner(owner_b);
    try std.testing.expect(probe.entered.load(.acquire) and probe.destroyed.load(.acquire));
    lane_b.closeOwner(owner_b);
    try lane_b.reopenOwner(owner_b);
    var second: JobProbe = .{};
    try lane_b.submit(second.job(owner_b));
    lane_b.closeOwner(owner_b);
    try std.testing.expect(second.destroyed.load(.acquire));
}

test "execution services archive boundary scope close drains accepted jobs while peers continue" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var a = try services.Client.open(&f.offer);
    defer a.close() catch @panic("scope close");
    var b = try services.Client.open(&f.offer);
    defer b.close() catch @panic("scope close");
    const owner_a = try a.allocOwnerId();
    const owner_b = try b.allocOwnerId();
    const lane_a = try a.durableJobs();
    const lane_b = try b.durableJobs();
    var probe: JobProbe = .{ .go = .init(false) };
    defer probe.go.store(true, .release);
    try lane_a.submit(probe.job(owner_a));
    try yieldUntil(&probe.entered);
    const Close = struct {
        client: *services.Client,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.client.close() catch @panic("close failed");
            self.done.store(true, .release);
        }
    };
    var close: Close = .{ .client = &a };
    const thread = try std.Thread.spawn(.{}, Close.run, .{&close});
    defer thread.join();
    defer probe.go.store(true, .release);
    const end = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (lane_a.isAccepting()) {
        if (@import("antfly_platform").time.monotonicNs() >= end) return error.Timeout;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(!close.done.load(.acquire) and !probe.destroyed.load(.acquire));
    var peer: JobProbe = .{};
    try lane_b.submit(peer.job(owner_b));
    lane_b.drainOwner(owner_b);
    try std.testing.expect(peer.destroyed.load(.acquire));
    probe.go.store(true, .release);
    try yieldUntil(&close.done);
    try std.testing.expect(probe.destroyed.load(.acquire));
}

test "execution services archive boundary retains native runtime through the final scope and worker join" {
    var f: Fixture = .{};
    try f.init();
    var node_live = true;
    defer if (node_live) f.deinit();
    var client = try services.Client.open(&f.offer);
    defer client.close() catch @panic("scope close");
    var worker = try client.acquireWorkers(.{});
    defer worker.release() catch @panic("worker release");
    const Close = struct {
        fixture: *Fixture,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.fixture.deinit();
            self.done.store(true, .release);
        }
    };
    var close: Close = .{ .fixture = &f };
    const thread = try std.Thread.spawn(.{}, Close.run, .{&close});
    node_live = false;
    defer {
        worker.release() catch @panic("worker cleanup");
        client.close() catch @panic("client cleanup");
        thread.join();
    }
    const end = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        if (services.Client.open(&f.offer)) |value| {
            var temporary = value;
            try temporary.close();
        } else |err| {
            try std.testing.expectEqual(error.BackendRuntimeShuttingDown, err);
            break;
        }
        if (@import("antfly_platform").time.monotonicNs() >= end) return error.Timeout;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(!close.done.load(.acquire));
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var receiver = try worker.receiveIo();
    var future = try receiver.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), future.await(receiver.io()));
    try worker.release();
    try std.testing.expect(!close.done.load(.acquire));
    try client.close();
    try yieldUntil(&close.done);
}

test "execution services archive boundary movable leases keep their translated IO at a stable address" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var client = try services.Client.open(&f.offer);
    defer client.close() catch @panic("scope close");
    var first = try client.acquireWorkers(.{});
    defer first.release() catch @panic("worker release");
    const io = first.io();
    var moved = first;
    first.resource = null;
    defer moved.release() catch @panic("worker release");
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var future = try io.concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), future.await(io));
}

test "execution services archive boundary shares PDF workers and preserves caller scratch callbacks" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var a = try services.Client.open(&f.offer);
    defer a.close() catch @panic("scope close");
    var b = try services.Client.open(&f.offer);
    defer b.close() catch @panic("scope close");
    var first = try a.acquirePdf();
    defer first.release() catch @panic("PDF release");
    const mappings = (try a.memoryStats()).?.stacks.used;
    var second = try b.acquirePdf();
    defer second.release() catch @panic("PDF release");
    try std.testing.expectEqual(mappings, (try b.memoryStats()).?.stacks.used);
    try std.testing.expectEqual(@as(usize, 2), (try b.stats()).pdf_render_active_leases);
    const Probe = struct {
        count: std.atomic.Value(usize) = .init(0),
        fn run(raw: *anyopaque, scratch: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = scratch.alloc(u8, 1024) catch @panic("scratch refused");
            @memset(bytes, 42);
            _ = self.count.fetchAdd(1, .acq_rel);
        }
    };
    var probe: Probe = .{};
    const contexts = [_]*anyopaque{ &probe, &probe, &probe, &probe };
    const stats = try first.pageExecutor().runBatch(&contexts, Probe.run, 1 << 20);
    try std.testing.expectEqual(@as(usize, 4), probe.count.load(.acquire));
    try std.testing.expect(stats.peak_parallelism > 0 and stats.peak_parallelism <= first.capacity());
    try first.release();
    _ = try second.pageExecutor().runBatch(&contexts, Probe.run, 1 << 20);
    try std.testing.expectEqual(@as(usize, 8), probe.count.load(.acquire));
}

test "execution services archive boundary scopes recurring registration and preserves self wake" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var a = try services.Client.open(&f.offer);
    defer a.close() catch @panic("scope close");
    var b = try services.Client.open(&f.offer);
    defer b.close() catch @panic("scope close");
    const Probe = struct {
        client: *const services.Client,
        count: std.atomic.Value(usize) = .init(0),
        done: std.atomic.Value(bool) = .init(false),
        fn step(raw: *anyopaque) ?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const previous = self.count.fetchAdd(1, .acq_rel);
            if (previous == 0) self.client.wakeMaintenance(self) catch @panic("self wake");
            if (previous >= 1) self.done.store(true, .release);
            return null;
        }
        fn runnable(_: *anyopaque, _: runtime.runnable_abi.Borrowed) !?u64 {
            return null;
        }
    };
    var pa: Probe = .{ .client = &a };
    var pb: Probe = .{ .client = &b };
    var one = try a.registerMaintenance(.{ .class = .maintenance, .context = &pa, .step = Probe.step });
    defer one.release() catch @panic("registration release");
    var two = try b.registerMaintenance(.{ .class = .derived, .context = &pb, .step = Probe.step });
    defer two.release() catch @panic("registration release");
    try yieldUntil(&pa.done);
    try yieldUntil(&pb.done);
    try std.testing.expectEqual(@as(usize, 2), (try a.maintenanceStats()).?.owned_registrations);
    try one.awaitRegistration();
    try std.testing.expectEqual(@as(usize, 1), (try b.maintenanceStats()).?.owned_registrations);
    try a.close();
    try b.wakeMaintenance(&pb);
    try std.testing.expectError(error.ExecutionWaitUnsupported, b.registerMaintenance(.{
        .context = &pb,
        .runnable_step = Probe.runnable,
        .pass = .{ .budget_ns = std.time.ns_per_s },
    }));
    try std.testing.expectEqual(@as(usize, 1), (try b.maintenanceStats()).?.owned_registrations);
}

test "execution services archive boundary remote scheduler wrappers close only their own registrations" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var a = try services.Client.open(&f.offer);
    defer a.close() catch @panic("scope close");
    var b = try services.Client.open(&f.offer);
    defer b.close() catch @panic("scope close");
    var io_a = try a.ioView(.general);
    var io_b = try b.ioView(.general);
    const Scheduler = background.MaintenanceScheduler;
    const first = try Scheduler.createRemote(std.testing.allocator, io_a.io(), try a.maintenancePort());
    defer first.destroy();
    const mappings = (try a.memoryStats()).?.stacks.used;
    const second = try Scheduler.createRemote(std.testing.allocator, io_b.io(), try b.maintenancePort());
    defer second.destroy();
    try std.testing.expectEqual(mappings, (try b.memoryStats()).?.stacks.used);
    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(true),
        count: std.atomic.Value(usize) = .init(0),
        fn step(self: *@This()) ?u64 {
            _ = self.count.fetchAdd(1, .acq_rel);
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
            return null;
        }
    };
    var blocked: Probe = .{ .go = .init(false) };
    defer blocked.go.store(true, .release);
    var peer: Probe = .{};
    var one: ?Scheduler.Handle = try first.registerClass(.producer, &blocked, Probe.step);
    defer if (one) |*handle| handle.cancel(io_a.io());
    defer blocked.go.store(true, .release);
    try yieldUntil(&blocked.entered);
    first.closeAdmission();
    try std.testing.expectError(error.BackendRuntimeShuttingDown, first.registerClass(.maintenance, &peer, Probe.step));
    var two = try second.registerClass(.derived, &peer, Probe.step);
    defer two.cancel(io_b.io());
    try yieldUntil(&peer.entered);
    try std.testing.expectEqual(@as(usize, 2), second.snapshot().owned_registrations);
    blocked.go.store(true, .release);
    one.?.await(io_a.io());
    one = null;
    try std.testing.expectEqual(@as(usize, 1), first.snapshot().owned_registrations);
    try std.testing.expectEqual(@as(usize, 1), blocked.count.load(.acquire));
    peer.entered.store(false, .release);
    second.wake(&peer);
    try yieldUntil(&peer.entered);
    try std.testing.expect(peer.count.load(.acquire) >= 2);
}

test "execution services archive boundary runtime proxies share node capacity without duplicate execution floors" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var resources = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 8 << 20 } });
    defer resources.deinit(std.testing.allocator);
    var first = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .execution_services = &f.offer, .memory_parent = resources.memoryParentPort() });
    var first_live = true;
    defer if (first_live) first.deinit();
    var second = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .execution_services = &f.offer, .memory_parent = resources.memoryParentPort() });
    defer second.deinit();
    try std.testing.expect(!first.ptr().usesBorrowedIo());
    try std.testing.expect(!first.ptr().durable_jobs.executesInline());
    try std.testing.expect(first.ptr().io_impl == null and second.ptr().io_impl == null);
    var one = try first.ptr().acquireWorkers(.{});
    defer one.release();
    var two = try second.ptr().acquireWorkers(.{});
    defer two.release();
    try std.testing.expectError(error.WorkerCapacityExceeded, second.ptr().acquireWorkers(.{}));
    try std.testing.expectEqual(@as(usize, 2), first.ptr().laneStats().reserved_service_workers);
    try std.testing.expectEqual(@as(usize, 2), second.ptr().laneStats().reserved_service_workers);
    var sql_one = try first.ptr().acquireSharedSqlWorkers(.control, 1);
    defer sql_one.release();
    var sql_two = try second.ptr().acquireSharedSqlWorkers(.control, 1);
    defer sql_two.release();
    try std.testing.expectEqual(sql_one.capacity, second.ptr().laneStats().reserved_sql_workers);
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    const io = two.io();
    var future = try io.concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), future.await(io));
    one.release();
    sql_one.release();
    first.deinit();
    first_live = false;
    try std.testing.expectEqual(@as(usize, 1), second.ptr().laneStats().reserved_service_workers);
    var replacement = try second.ptr().acquireWorkers(.{});
    defer replacement.release();
    var sql_future = try sql_two.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), sql_future.await(sql_two.io()));
}

test "execution services archive boundary runtime proxy retirement drains asynchronous durable payloads" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var first = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .execution_services = &f.offer });
    var first_live = true;
    defer if (first_live) first.deinit();
    var second = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .execution_services = &f.offer });
    defer second.deinit();
    const owner_one = try first.ptr().allocOwnerId();
    const owner_two = try second.ptr().allocOwnerId();
    try std.testing.expect(owner_one != owner_two);
    var probe: JobProbe = .{ .go = .init(false) };
    defer probe.go.store(true, .release);
    try first.ptr().durable_jobs.submit(probe.job(owner_one));
    try yieldUntil(&probe.entered);
    const Close = struct {
        handle: *background.BackendRuntimeHandle,
        entered: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.entered.store(true, .release);
            self.handle.deinit();
            self.done.store(true, .release);
        }
    };
    var close: Close = .{ .handle = &first };
    const thread = try std.Thread.spawn(.{}, Close.run, .{&close});
    defer thread.join();
    defer probe.go.store(true, .release);
    first_live = false;
    try yieldUntil(&close.entered);
    try std.testing.expect(!close.done.load(.acquire));
    var peer: JobProbe = .{};
    try second.ptr().durable_jobs.submit(peer.job(owner_two));
    second.ptr().durable_jobs.drainOwner(owner_two);
    try std.testing.expect(peer.destroyed.load(.acquire));
    try std.testing.expect(!close.done.load(.acquire));
    probe.go.store(true, .release);
    try yieldUntil(&close.done);
    try std.testing.expect(probe.destroyed.load(.acquire));
}

test "execution services archive boundary shares complete Raft attempts and preserves control progress" {
    var fixture: Fixture = .{};
    try fixture.init();
    defer fixture.deinit();
    var first = try services.Client.open(&fixture.offer);
    defer first.close() catch @panic("scope close");
    var second = try services.Client.open(&fixture.offer);
    defer second.close() catch @panic("scope close");
    const stats = try first.stats();
    try std.testing.expectEqual(@as(usize, 5), stats.raft_outbound_request_capacity);
    var leases: [5]services.Lease = undefined;
    var count: usize = 0;
    defer for (leases[0..count]) |*lease| lease.release() catch @panic("Raft release");
    for (&leases, 0..) |*lease, index| {
        lease.* = try (if (index % 2 == 0) &first else &second).acquireIo(.raft_outbound);
        count += 1;
        try std.testing.expectEqual(@as(usize, 6), lease.capacity());
    }
    try std.testing.expectError(error.RaftOutboundCapacityUnavailable, first.acquireIo(.raft_outbound));
    try std.testing.expectEqual(@as(usize, 5), (try second.stats()).raft_outbound_active_leases);
    var control = try second.acquireIo(.control);
    defer control.release() catch @panic("control release");
    const Work = struct {
        fn run(flag: *std.atomic.Value(bool)) void {
            flag.store(true, .release);
        }
    };
    var completed: std.atomic.Value(bool) = .init(false);
    const io = control.io();
    var task = try io.concurrent(Work.run, .{&completed});
    task.await(io);
    try std.testing.expect(completed.load(.acquire));
    leases[4].release() catch @panic("Raft release");
    count -= 1;
    var next = try second.acquireIo(.raft_outbound);
    defer next.release() catch @panic("Raft release");
    try std.testing.expectEqual(@as(usize, 5), (try first.stats()).raft_outbound_active_leases);
}

test "execution services archive boundary recovery dispatch survives ordinary parent exhaustion" {
    var f: Fixture = .{};
    try f.init();
    defer f.deinit();
    var client = try services.Client.open(&f.offer);
    defer client.close() catch @panic("scope close");
    const owner = try client.allocOwnerId();
    const lane = try client.durableJobs();
    var probe: JobProbe = .{};
    var job = probe.job(owner);
    job.class = .recovery;
    _ = antfly_execution_fixture_pressure(f.context.?, true);
    defer _ = antfly_execution_fixture_pressure(f.context.?, false);
    try lane.submit(job);
    lane.closeOwner(owner);
    try std.testing.expect(probe.entered.load(.acquire) and probe.destroyed.load(.acquire));
}

extern fn antfly_execution_fixture_poll_publisher(*const runtime.native_abi.TypeContract, *const anyopaque, *bool, *?[*:0]const u8) callconv(.c) runtime.error_abi.Status;
const PublisherPollDispatch = struct {
    const Dispatch = @FieldType(local.storage_db_replication_contract.Publisher, "callback_dispatch");
    var original: ?Dispatch = null;
    fn forward(contract: *const runtime.native_abi.CallContract, method: *const anyopaque, arguments: *const anyopaque, output: *anyopaque) callconv(.c) runtime.error_abi.Status {
        return original.?(contract, method, arguments, output);
    }
};
test "execution services archive boundary publisher polling preserves pending and rejects old layouts before dispatch" {
    const contract = local.storage_db_replication_contract;
    const Mirror = contract.AsyncEffectMirror;
    const Kind = local.storage_db_durable_outbox.Kind;
    const Namespace = local.storage_db_doc_identity_namespace.Namespace;
    const Peer = struct {
        ready: bool = false,
        reject: bool = false,
        polls: usize = 0,
        blocking_calls: usize = 0,
        fn next(_: *anyopaque) u64 {
            return 7;
        }
        fn identity(_: *anyopaque) contract.Publisher.Identity {
            return .{ .table_id = 1, .shard_id = 2, .timeline_id = 3, .epoch = 4 };
        }
        fn publish(_: Mirror, _: Kind, _: []const u8, _: Namespace) !u64 {
            return error.UnexpectedPublish;
        }
        fn recover(_: Mirror, _: Kind, _: local.storage_db_durable_outbox.DurableReplicationOutbox, _: Namespace) !u64 {
            return error.UnexpectedRecover;
        }
        fn preflight(_: Mirror, _: bool) !void {
            return error.UnexpectedPreflight;
        }
        fn complete(m: Mirror, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(m.publisher.ptr));
            self.blocking_calls += 1;
            return error.CommitCompletionPending;
        }
        fn poll(m: Mirror, lsn: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(m.publisher.ptr));
            try std.testing.expectEqual(@as(u64, 7), lsn);
            self.polls += 1;
            if (self.reject) return error.SyncPolicyUnsatisfied;
            return self.ready;
        }
    };
    var peer: Peer = .{};
    var publisher: contract.Publisher = .{ .ptr = &peer, .vtable = &.{ .next_lsn = Peer.next, .identity = Peer.identity, .publish = Peer.publish, .recover = Peer.recover, .preflight = Peer.preflight, .complete = Peer.complete }, .poll_completion = Peer.poll };
    PublisherPollDispatch.original = publisher.callback_dispatch;
    defer PublisherPollDispatch.original = null;
    publisher.callback_dispatch = PublisherPollDispatch.forward;
    const expected = runtime.native_abi.TypeContract.of(contract.Publisher);
    var old = expected;
    old.version = expected.version - 1;
    var ready = false;
    var failure: ?[*:0]const u8 = null;
    const incompatible = antfly_execution_fixture_poll_publisher(&old, @ptrFromInt(8), &ready, &failure);
    try std.testing.expectEqual(error.UnsupportedVersion, runtime.error_abi.errorFromStatus(incompatible));
    try std.testing.expectEqual(@as(usize, 0), peer.polls);
    var status = antfly_execution_fixture_poll_publisher(&expected, &publisher, &ready, &failure);
    try std.testing.expect(status.isOk() and !ready and failure == null);
    peer.ready = true;
    status = antfly_execution_fixture_poll_publisher(&expected, &publisher, &ready, &failure);
    try std.testing.expect(status.isOk() and ready and failure == null);
    peer.reject = true;
    status = antfly_execution_fixture_poll_publisher(&expected, &publisher, &ready, &failure);
    try std.testing.expect(status.isOk());
    try std.testing.expectEqual(error.SyncPolicyUnsatisfied, @import("storage_engine_errors").fromName(std.mem.span(failure.?)));
    publisher.poll_completion = null;
    status = antfly_execution_fixture_poll_publisher(&expected, &publisher, &ready, &failure);
    try std.testing.expect(status.isOk());
    try std.testing.expectEqual(error.ExecutionWaitUnsupported, @import("storage_engine_errors").fromName(std.mem.span(failure.?)));
    try std.testing.expectEqual(@as(usize, 3), peer.polls);
    try std.testing.expectEqual(@as(usize, 0), peer.blocking_calls);
}

extern fn antfly_execution_fixture_recover_publisher(*const runtime.native_abi.TypeContract, *const anyopaque, *const anyopaque, *u64, *?[*:0]const u8) callconv(.c) runtime.error_abi.Status;
test "execution services archive boundary controlled publisher recovery preserves authority and refuses legacy fallback" {
    const contract = local.storage_db_replication_contract;
    const Mirror = contract.AsyncEffectMirror;
    const Kind = local.storage_db_durable_outbox.Kind;
    const Pending = local.storage_db_durable_outbox.DurableReplicationOutbox;
    const Namespace = local.storage_db_doc_identity_namespace.Namespace;
    const read = local.common_read_admission;
    const Peer = struct {
        calls: usize = 0,
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn next(_: *anyopaque) u64 {
            return 7;
        }
        fn identity(_: *anyopaque) contract.Publisher.Identity {
            return .{ .table_id = 1, .shard_id = 2, .timeline_id = 3, .epoch = 4 };
        }
        fn publish(_: Mirror, _: Kind, _: []const u8, _: Namespace) !u64 {
            return error.UnexpectedPublish;
        }
        fn recover(_: Mirror, _: Kind, _: Pending, _: Namespace) !u64 {
            return error.UnexpectedLegacyRecovery;
        }
        fn preflight(_: Mirror, _: bool) !void {
            return error.UnexpectedPreflight;
        }
        fn complete(_: Mirror, _: u64) !void {
            return error.UnexpectedComplete;
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.cause;
        }
        fn controlled(mirror: Mirror, kind: Kind, pending: Pending, namespace: Namespace, control: read.Port) !u64 {
            const self: *@This() = @ptrCast(@alignCast(mirror.publisher.ptr));
            self.calls += 1;
            try std.testing.expectEqual(Kind.batch, kind);
            try std.testing.expectEqual(@as(u64, 3), pending.from_lsn);
            try std.testing.expectEqualStrings("payload", pending.payload);
            try std.testing.expectEqualDeep(@as([16]u8, @splat(0x5a)), pending.recovery_identity.?);
            try std.testing.expectEqual(@as(u64, 99), pending.recovery_cursor.?.state.from_lsn);
            try std.testing.expectEqual(@as(u64, 2), namespace.shard_id);
            try control.checkpoint();
            pending.recovery_cursor.?.state.next_lsn = 123;
            return 7;
        }
    };
    var peer: Peer = .{};
    var vtable: contract.Publisher.VTable = .{ .next_lsn = Peer.next, .identity = Peer.identity, .publish = Peer.publish, .recover = Peer.recover, .recover_with_admission = Peer.controlled, .preflight = Peer.preflight, .complete = Peer.complete };
    var publisher: contract.Publisher = .{ .ptr = &peer, .vtable = &vtable };
    PublisherPollDispatch.original = publisher.callback_dispatch;
    defer PublisherPollDispatch.original = null;
    publisher.callback_dispatch = PublisherPollDispatch.forward;
    const payload: read.Payload = .{ .ptr = &peer, .vtable = &.{ .checkpoint = Peer.check } };
    const port = read.Port.init(&payload);
    const expected = runtime.native_abi.TypeContract.of(contract.Publisher);
    var old = expected;
    old.version = expected.version - 1;
    var output: u64 = 0;
    var failure: ?[*:0]const u8 = null;
    const incompatible = antfly_execution_fixture_recover_publisher(&old, @ptrFromInt(8), @ptrFromInt(8), &output, &failure);
    try std.testing.expectEqual(error.UnsupportedVersion, runtime.error_abi.errorFromStatus(incompatible));
    try std.testing.expectEqual(@as(usize, 0), peer.calls);
    var status = antfly_execution_fixture_recover_publisher(&expected, &publisher, &port, &output, &failure);
    try std.testing.expect(status.isOk() and failure == null);
    try std.testing.expectEqual(@as(u64, 7), output);
    try std.testing.expectEqual(@as(usize, 1), peer.calls);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        peer = .{ .cause = cause, .reject_at = 2 };
        status = antfly_execution_fixture_recover_publisher(&expected, &publisher, &port, &output, &failure);
        try std.testing.expect(status.isOk());
        try std.testing.expectEqualStrings(@errorName(cause), std.mem.span(failure.?));
        try std.testing.expectEqual(@as(usize, 1), peer.calls);
        try std.testing.expectEqual(@as(u64, 0), output);
    }
    peer = .{};
    vtable.recover_with_admission = null;
    status = antfly_execution_fixture_recover_publisher(&expected, &publisher, &port, &output, &failure);
    try std.testing.expect(status.isOk());
    try std.testing.expectEqualStrings("ExecutionWaitUnsupported", std.mem.span(failure.?));
    try std.testing.expectEqual(@as(usize, 0), peer.calls);
}
