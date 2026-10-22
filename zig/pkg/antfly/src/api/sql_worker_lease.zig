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

test "SQL shared worker physical retirement holds capacity through pthread TLS destructors" {
    if (comptime !std.Thread.use_pthreads or @import("builtin").single_threaded) return error.SkipZigTest;
    const Probe = struct {
        key: std.c.pthread_key_t,
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(false),
        exited: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(bool) = .init(false),
        release_done: std.Io.Event = .unset,
        fn destroy(raw: ?*anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
            self.exited.store(true, .release);
        }
        fn work(self: *@This()) void {
            std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
        }
        fn release(lease: *Lease, self: *@This()) void {
            lease.release();
            self.released.store(true, .release);
            self.release_done.set(std.testing.io);
        }
    };
    var key: std.c.pthread_key_t = undefined;
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destroy));
    defer _ = std.c.pthread_key_delete(key);
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
    defer root.deinit(std.testing.allocator);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort(), .lane_limits = .{ .control = 1, .raft_inbound = 1, .raft_outbound = 1 } });
    defer runtime.deinit();
    var lease = try Lease.acquireImportedShared(std.testing.allocator, factory(runtime.ptr()).?, .cleanup, 1);
    defer lease.release();
    const capacity = lease.capacity;
    const baseline = runtime.ptr().runtimeMemoryStats().?.stacks.used;
    var probe: Probe = .{ .key = key };
    var releasing: ?std.Thread = null;
    defer {
        probe.go.store(true, .release);
        if (releasing) |thread| thread.join();
        // Also make the old detached-worker negative control safe: it may
        // have returned its lease while this destructor was still executing.
        if (probe.entered.load(.acquire)) while (!probe.exited.load(.acquire)) {
            std.Thread.yield() catch {};
        };
    }
    const io = lease.io();
    var future = try io.concurrent(Probe.work, .{&probe});
    future.await(io);
    releasing = try std.Thread.spawn(.{}, Probe.release, .{ &lease, &probe });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.entered.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        std.Thread.yield() catch {};
    }
    // TLS destructor entry proves the executor's worker body has returned.
    // A wait-group-only join can no longer justify freeing its physical slot.
    try std.testing.expectError(error.Timeout, probe.release_done.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }));
    try std.testing.expect(!probe.released.load(.acquire));
    try std.testing.expectEqual(capacity, runtime.ptr().laneStats().reserved_workers);
    try std.testing.expect(runtime.ptr().runtimeMemoryStats().?.stacks.used > baseline);
    const charged = root.snapshot().memory.used_bytes;
    try std.testing.expect(charged >= runtime.ptr().runtimeMemoryStats().?.stacks.used);
    probe.go.store(true, .release);
    releasing.?.join();
    releasing = null;
    try std.testing.expect(probe.exited.load(.acquire) and probe.released.load(.acquire));
    try std.testing.expectEqual(baseline, runtime.ptr().runtimeMemoryStats().?.stacks.used);
    try std.testing.expect(root.snapshot().memory.used_bytes < charged);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
}

test "SQL shared worker physical retirement joins async and group worker TLS destructors" {
    if (comptime !std.Thread.use_pthreads or @import("builtin").single_threaded) return error.SkipZigTest;
    const Probe = struct {
        key: std.c.pthread_key_t,
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(false),
        done: std.Io.Event = .unset,
        fn destroy(raw: ?*anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
        }
        fn work(self: *@This()) void {
            std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
        }
        fn noop() void {}
        fn recordThread(out: *std.atomic.Value(u64)) void {
            out.store(@intCast(std.Thread.getCurrentId()), .release);
        }
        fn close(owner: *local.storage_background_runtime.BackendRuntimeHandle, self: *@This()) void {
            owner.deinit();
            self.done.set(std.testing.io);
        }
    };
    inline for (.{ .async, .group_async, .group_concurrent }) |mode| {
        var key: std.c.pthread_key_t = undefined;
        try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destroy));
        defer _ = std.c.pthread_key_delete(key);
        var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
        var closed = false;
        defer if (!closed) runtime.deinit();
        var lane = try runtime.ptr().acquireApiLane();
        defer lane.release();
        // Exercise the async spawn branch even on a single-CPU test runner,
        // staying below this production lane's configured physical ceiling.
        runtime.ptr().api_io_impl.load(.acquire).?.setAsyncLimit(.limited(1));
        const io = lane.io();
        var probe: Probe = .{ .key = key };
        var closing: ?std.Thread = null;
        defer {
            probe.go.store(true, .release);
            lane.release();
            if (closing) |thread| {
                thread.join();
                closed = true;
            }
        }
        switch (mode) {
            .async => {
                var future = io.async(Probe.work, .{&probe});
                future.await(io);
            },
            .group_async => {
                var group: std.Io.Group = .init;
                group.async(io, Probe.work, .{&probe});
                try group.await(io);
            },
            .group_concurrent => {
                var group: std.Io.Group = .init;
                try group.concurrent(io, Probe.work, .{&probe});
                try group.await(io);
            },
            else => unreachable,
        }
        lane.release();
        closing = try std.Thread.spawn(.{}, Probe.close, .{ &runtime, &probe });
        const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (!probe.entered.load(.acquire)) {
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
            std.Thread.yield() catch {};
        }
        if (io.concurrent(Probe.noop, .{})) |value| {
            var unexpected = value;
            unexpected.await(io);
            return error.TestExpectedError;
        } else |err| try std.testing.expectEqual(error.ConcurrencyUnavailable, err);
        var group: std.Io.Group = .init;
        if (group.concurrent(io, Probe.noop, .{})) |_| {
            try group.await(io);
            return error.TestExpectedError;
        } else |err| try std.testing.expectEqual(error.ConcurrencyUnavailable, err);
        var caller: std.atomic.Value(u64) = .init(0);
        var inline_future = io.async(Probe.recordThread, .{&caller});
        inline_future.await(io);
        try std.testing.expectEqual(@as(u64, @intCast(std.Thread.getCurrentId())), caller.load(.acquire));
        caller.store(0, .release);
        group.async(io, Probe.recordThread, .{&caller});
        try group.await(io);
        try std.testing.expectEqual(@as(u64, @intCast(std.Thread.getCurrentId())), caller.load(.acquire));
        try std.testing.expectError(error.Timeout, probe.done.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }));
        probe.go.store(true, .release);
        closing.?.join();
        closing = null;
        closed = true;
        try std.testing.expect(probe.done.isSet());
    }
}

test "SQL shared worker stack bundle rejects an undersized node before runtime construction" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Manager = local.storage_resource_manager.ResourceManager;
    var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 8 << 20 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.ResourceBudgetExceeded, Runtime.init(backing.allocator(), .{ .backend = .io_threaded, .shared_sql_memory_parent = manager.memoryParentPort() }));
    try std.testing.expect(!backing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectError(error.OutOfMemory, std.Io.Threaded.workerStackMappingSize(std.math.maxInt(usize)));
    const Bank = local.storage_background_runtime.SqlWorkerMemory.Bank;
    try std.testing.expectError(error.InvalidWorkerMemoryLimit, Bank.createWithStacks(std.testing.allocator, manager.memoryParentPort(), .{}, .{ .execution = std.math.maxInt(usize) }));
}

test "SQL shared worker stack mapping remains charged through TLS retirement and is unmapped before credit returns" {
    if (comptime !std.Thread.use_pthreads or @import("builtin").single_threaded) return error.SkipZigTest;
    const Memory = local.storage_background_runtime.SqlWorkerMemory;
    const Manager = local.storage_resource_manager.ResourceManager;
    const mapping_size = try std.Io.Threaded.workerStackMappingSize((std.Io.Threaded.InitOptions{}).stack_size);
    var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    const memory = try Memory.Bank.createWithStacks(std.testing.allocator, manager.memoryParentPort(), .{}, .{ .execution = mapping_size });
    defer memory.destroy();
    try std.testing.expectError(error.OutOfMemory, memory.stackAllocator(.execution).alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 0), memory.stackStats(.execution).used);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    const Probe = struct {
        bank: *Memory.Bank,
        manager: *Manager,
        pin: u64,
        key: std.c.pthread_key_t,
        mapping: ?[]u8 = null,
        marker: std.atomic.Value(usize) = .init(0),
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(false),
        done: std.Io.Event = .unset,
        frees: std.atomic.Value(usize) = .init(0),
        unmapped_before_credit: std.atomic.Value(bool) = .init(false),
        extern "c" fn mincore(address: *anyopaque, len: usize, vector: [*]u8) c_int;
        const Query = enum { mapped, unmapped, guard, read_write };
        fn query(address: usize, len: usize, mode: Query) bool {
            if (comptime @import("builtin").os.tag.isDarwin()) {
                var start: std.c.mach_vm_address_t = address;
                var size: std.c.mach_vm_size_t = 0;
                // Read only protection (the first int), avoiding dependence on
                // the SDK's packed layout for the rest of BASIC_INFO_64.
                var info: [16]c_int = undefined;
                var count: std.c.mach_msg_type_number_t = info.len;
                var object: std.c.mach_port_t = 0;
                const rc = std.c.mach_vm_region(std.c.mach_task_self(), &start, &size, std.c.VM.REGION.BASIC_INFO_64, @ptrCast(&info), &count, &object);
                defer if (object != 0) {
                    _ = std.c.mach_port_deallocate(std.c.mach_task_self(), object);
                };
                // KERN_INVALID_ADDRESS means there is no following region.
                if (mode == .unmapped) return rc == 1 or (rc == 0 and start >= address + len);
                if (rc != 0 or start > address or size < address - start + len or count == 0) return false;
                return switch (mode) {
                    .mapped => true,
                    .guard => info[0] == 0,
                    .read_write => (info[0] & 3) == 3,
                    .unmapped => unreachable,
                };
            }
            var vector: [1]u8 = undefined;
            if (mode == .unmapped) {
                var offset: usize = 0;
                while (offset < len) : (offset += std.heap.pageSize()) {
                    if (std.posix.errno(mincore(@ptrFromInt(address + offset), std.heap.pageSize(), &vector)) != .NOMEM) return false;
                }
                return true;
            }
            return mode == .mapped and mincore(@ptrFromInt(address), std.heap.pageSize(), &vector) == 0;
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = std.heap.page_allocator.rawAlloc(len, alignment, ret) orelse return null;
            self.mapping = bytes[0..len];
            return bytes;
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const held = self.bank.stackStats(.execution).used == bytes.len and self.manager.snapshot().memory.used_bytes == self.pin;
            const mapped_before = query(@intFromPtr(bytes.ptr), std.heap.pageSize(), .mapped);
            std.heap.page_allocator.rawFree(bytes, alignment, ret);
            const absent_after = query(@intFromPtr(bytes.ptr), bytes.len, .unmapped);
            self.unmapped_before_credit.store(held and mapped_before and absent_after and self.bank.stackStats(.execution).used == bytes.len and self.manager.snapshot().memory.used_bytes == self.pin, .release);
            _ = self.frees.fetchAdd(1, .acq_rel);
        }
        fn destroy(raw: ?*anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
        }
        fn work(self: *@This()) void {
            var marker: u8 = 42;
            self.marker.store(@intFromPtr(&marker), .release);
            std.mem.doNotOptimizeAway(&marker);
            std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
        }
        fn release(lease: *Runtime.WorkerLease, self: *@This()) void {
            lease.release();
            self.done.set(std.testing.io);
        }
    };
    var key: std.c.pthread_key_t = undefined;
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destroy));
    defer _ = std.c.pthread_key_delete(key);
    var probe: Probe = .{ .bank = memory, .manager = &manager, .pin = manager.snapshot().memory.used_bytes, .key = key };
    memory.stacks[@backingInt(Memory.Role.execution)].backing = probe.allocator();
    var lease = try runtime.ptr().acquireWorkers(.{ .allocator = memory.allocator(.execution), .stack_allocator = memory.stackAllocator(.execution) });
    defer lease.release();
    var releasing: ?std.Thread = null;
    defer {
        probe.go.store(true, .release);
        if (releasing) |thread| thread.join();
    }
    const io = lease.io();
    var future = try io.concurrent(Probe.work, .{&probe});
    future.await(io);
    const mapping = probe.mapping.?;
    const marker = probe.marker.load(.acquire);
    try std.testing.expectEqual(mapping_size, mapping.len);
    if (comptime @import("builtin").os.tag.isDarwin()) {
        try std.testing.expect(Probe.query(@intFromPtr(mapping.ptr), std.heap.pageSize(), .guard));
        try std.testing.expect(Probe.query(@intFromPtr(mapping.ptr) + mapping.len - std.heap.pageSize(), std.heap.pageSize(), .guard));
        try std.testing.expect(Probe.query(marker, 1, .read_write));
    }
    try std.testing.expect(marker >= @intFromPtr(mapping.ptr) + std.heap.pageSize() and marker < @intFromPtr(mapping.ptr) + mapping.len - std.heap.pageSize());
    releasing = try std.Thread.spawn(.{}, Probe.release, .{ &lease, &probe });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.entered.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        std.Thread.yield() catch {};
    }
    try std.testing.expectError(error.Timeout, probe.done.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }));
    try std.testing.expectEqual(mapping_size, memory.stackStats(.execution).used);
    try std.testing.expectEqual(@as(usize, 0), probe.frees.load(.acquire));
    try std.testing.expectEqual(probe.pin, manager.snapshot().memory.used_bytes);
    probe.go.store(true, .release);
    releasing.?.join();
    releasing = null;
    try std.testing.expectEqual(@as(usize, 1), probe.frees.load(.acquire));
    try std.testing.expect(probe.unmapped_before_credit.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.stackStats(.execution).used);
    try std.testing.expectEqual(@as(usize, 0), memory.stats(.execution).used);
}

test "SQL shared worker stack spawn rollback returns mapped pages when thread argument funding fails" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Memory = local.storage_background_runtime.SqlWorkerMemory;
    const Manager = local.storage_resource_manager.ResourceManager;
    const mapping_size = try std.Io.Threaded.workerStackMappingSize((std.Io.Threaded.InitOptions{}).stack_size);
    var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    const memory = try Memory.Bank.createWithStacks(std.testing.allocator, manager.memoryParentPort(), .{}, .{ .execution = mapping_size });
    defer memory.destroy();
    try std.testing.expectError(error.OutOfMemory, memory.stackAllocator(.execution).alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 0), memory.stackStats(.execution).used);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    try std.testing.expectError(error.InvalidWorkerStackSize, runtime.ptr().acquireWorkers(.{ .stack_size = std.math.maxInt(usize) }));
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    var failing = std.testing.FailingAllocator.init(memory.allocator(.execution), .{});
    var lease = try runtime.ptr().acquireWorkers(.{ .allocator = failing.allocator(), .stack_allocator = memory.stackAllocator(.execution) });
    defer lease.release();
    const before = memory.stats(.execution).used;
    // Admit the future, then refuse the pthread entry wrapper after mapping.
    failing.fail_index = failing.alloc_index + 1;
    const Work = struct {
        fn run(done: *bool) void {
            done.* = true;
        }
    };
    var done = false;
    const io = lease.io();
    try std.testing.expectError(error.ConcurrencyUnavailable, io.concurrent(Work.run, .{&done}));
    try std.testing.expect(!done and failing.has_induced_failure);
    try std.testing.expectEqual(before, memory.stats(.execution).used);
    try std.testing.expectEqual(@as(usize, 0), memory.stackStats(.execution).used);
    try std.testing.expectEqual(mapping_size, memory.stackStats(.execution).peak);
    failing.fail_index = std.math.maxInt(usize);
    var future = try io.concurrent(Work.run, .{&done});
    future.await(io);
    try std.testing.expect(done);
    try std.testing.expectEqual(mapping_size, memory.stackStats(.execution).used);
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), memory.stackStats(.execution).used);
    try std.testing.expectEqual(@as(usize, 0), memory.stats(.execution).used);
}

test "SQL shared worker node memory isolates control and cleanup from exhausted parent and execution heaps" {
    const Manager = local.storage_resource_manager.ResourceManager;
    const Work = struct {
        fn run(done: *std.atomic.Value(bool)) void {
            done.store(true, .release);
        }
    };
    for ([_]bool{ false, true }) |imported| {
        var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 << 20 }, .identity_allocator = std.testing.allocator });
        defer manager.deinit(std.testing.allocator);
        var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(backing.allocator(), .{
            .backend = .io_threaded,
            .shared_sql_memory_parent = manager.memoryParentPort(),
        });
        defer runtime.deinit();
        const node = runtime.ptr();
        const pin = manager.snapshot().memory.used_bytes;
        try std.testing.expect(pin > 4 << 20);
        const pressure = (1024 << 20) - pin;
        try std.testing.expect(manager.reserveExternal(pressure));
        defer manager.releaseExternal(pressure);
        backing.fail_index = backing.alloc_index;
        defer backing.fail_index = std.math.maxInt(usize);
        try std.testing.expectError(error.OutOfMemory, node.alloc.alloc(u8, 1));
        {
            var first_memory: @import("../common/allocation_budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 512 << 10 };
            var peer_memory: @import("../common/allocation_budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 512 << 10 };
            var first = if (imported) try Lease.acquireImportedShared(first_memory.allocator(), factory(node).?, .control, 1) else try Lease.acquireNativeShared(node, .control, 1);
            defer first.release();
            var peer = if (imported) try Lease.acquireImportedShared(peer_memory.allocator(), factory(node).?, .control, 1) else try Lease.acquireNativeShared(node, .control, 1);
            defer peer.release();
            var execution = try Lease.acquireNativeShared(node, .execution, 1);
            defer execution.release();
            var cleanup = if (imported) try Lease.acquireImportedShared(peer_memory.allocator(), factory(node).?, .cleanup, 1) else try Lease.acquireNativeShared(node, .cleanup, 1);
            defer cleanup.release();
            var bounded = try Lease.acquireNativeShared(node, .bounded_read, 1);
            defer bounded.release();
            first.release();
            try std.testing.expectEqual(@as(usize, 0), first_memory.stats().used_bytes);
            first_memory.limit = 0;
            const memory = node.shared_sql_memory.?;
            const execution_used = memory.stats(.execution);
            const full = try memory.allocator(.execution).alloc(u8, execution_used.limit - execution_used.used);
            var heap_live = true;
            defer if (heap_live) memory.allocator(.execution).free(full);
            var done: std.atomic.Value(bool) = .init(false);
            try std.testing.expectError(error.ConcurrencyUnavailable, execution.io().concurrent(Work.run, .{&done}));
            memory.allocator(.execution).free(full);
            heap_live = false;
            const stack_limit = memory.stackStats(.execution).limit;
            const full_stacks = try memory.stackAllocator(.execution).alloc(u8, stack_limit);
            defer memory.stackAllocator(.execution).free(full_stacks);
            try std.testing.expectError(error.ConcurrencyUnavailable, execution.io().concurrent(Work.run, .{&done}));
            try std.testing.expect(!done.load(.acquire));
            for ([_]std.Io{ peer.io(), cleanup.io(), bounded.io() }) |io| {
                done.store(false, .release);
                var future = try io.concurrent(Work.run, .{&done});
                future.await(io);
                try std.testing.expect(done.load(.acquire));
            }
            try std.testing.expectEqual(@as(u64, 0), first_memory.stats().rejected_total);
            try std.testing.expectEqual(@as(u64, 1024 << 20), manager.snapshot().memory.used_bytes);
        }
        try std.testing.expectEqual(@as(usize, 0), node.laneStats().reserved_workers);
        inline for (std.meta.tags(Runtime.SharedSqlRole)) |role| {
            try std.testing.expectEqual(@as(usize, 0), node.sharedSqlMemoryStats(role).?.used);
            try std.testing.expectEqual(@as(usize, 0), node.sharedSqlStackStats(role).?.used);
        }
        node.closeSharedSqlMemory();
        try std.testing.expectEqual(pressure, manager.snapshot().memory.used_bytes);
        try std.testing.expect(node.sharedSqlMemoryStats(.control) == null);
        const rejected = node.sharedSqlStats(.control).rejections_total;
        try std.testing.expectError(error.BackendRuntimeShuttingDown, node.acquireSharedSqlWorkers(.control, 1));
        try std.testing.expectEqual(rejected + 1, node.sharedSqlStats(.control).rejections_total);
        // Closing this borrowed memory child does not disable the runtime I/O
        // needed by its storage parent during the parent's later destruction.
        backing.fail_index = std.math.maxInt(usize);
        var other = try node.acquireWorkers(.{ .capacity = 1 });
        other.release();
    }
}

test "SQL shared worker node memory close retains parent through live callbacks and peer release" {
    const Manager = local.storage_resource_manager.ResourceManager;
    var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 << 20 }, .identity_allocator = std.testing.allocator });
    var parent_closed = false;
    defer if (!parent_closed) manager.deinit(std.testing.allocator);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    const node = runtime.ptr();
    try node.installSharedSqlMemory(manager.memoryParentPort(), .{});
    try std.testing.expectError(error.SharedSqlMemoryAlreadyInstalled, node.installSharedSqlMemory(manager.memoryParentPort(), .{}));
    var lease = try Lease.acquireImportedShared(std.testing.allocator, factory(node).?, .cleanup, 1);
    defer lease.release();
    try std.testing.expectError(error.SharedSqlMemoryAlreadyPublished, node.installSharedSqlMemory(manager.memoryParentPort(), .{}));
    const Probe = struct {
        io: std.Io,
        entered: std.Io.Event = .unset,
        go: std.Io.Event = .unset,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.entered.set(self.io);
            self.go.waitUncancelable(self.io);
        }
        fn close(owner: *Runtime, self: *@This()) void {
            owner.closeSharedSqlMemory();
            self.done.store(true, .release);
        }
    };
    var probe: Probe = .{ .io = lease.io() };
    var future = try probe.io.concurrent(Probe.run, .{&probe});
    var joined = false;
    var closer: ?std.Thread = null;
    defer {
        if (!joined) {
            probe.go.set(probe.io);
            future.await(probe.io);
        }
        lease.release();
        if (closer) |thread| thread.join();
    }
    try probe.entered.waitTimeout(probe.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    const pin = manager.snapshot().memory.used_bytes;
    closer = try std.Thread.spawn(.{}, Probe.close, .{ node, &probe });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        if (node.acquireSharedSqlWorkers(.control, 1)) |value| {
            var temporary = value;
            temporary.release();
        } else |err| {
            if (err == error.BackendRuntimeShuttingDown) break;
            return err;
        }
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!probe.done.load(.acquire));
    try std.testing.expectEqual(pin, manager.snapshot().memory.used_bytes);
    probe.go.set(probe.io);
    future.await(probe.io);
    joined = true;
    try std.testing.expect(!probe.done.load(.acquire));
    try std.testing.expectEqual(pin, manager.snapshot().memory.used_bytes);
    lease.release();
    closer.?.join();
    closer = null;
    try std.testing.expect(probe.done.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    manager.deinit(std.testing.allocator);
    parent_closed = true;
    // The borrowed parent has now actually closed; unrelated runtime lanes
    // and the runtime's later destruction must not access its former bank.
    var other = try node.acquireWorkers(.{ .capacity = 1 });
    other.release();
    node.closeSharedSqlMemory();
}

test "SQL shared worker node memory charges metadata before allocation and frees physical bytes before parent credit" {
    const Manager = local.storage_resource_manager.ResourceManager;
    const Bank = local.storage_background_runtime.SqlWorkerMemory.Bank;
    var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 8 << 20 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Bank.create(failure.allocator(), manager.memoryParentPort(), .{}));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectError(error.InvalidWorkerMemoryLimit, Bank.create(std.testing.allocator, manager.memoryParentPort(), .{ .execution = std.math.maxInt(usize) }));
    var invalid = manager.memoryParentPort();
    invalid.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, Bank.create(std.testing.allocator, invalid, .{}));
    const Backing = struct {
        manager: *Manager,
        expected: u64 = 0,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ret);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.manager.snapshot().memory.used_bytes == self.expected);
            std.testing.allocator.rawFree(bytes, alignment, ret);
        }
    };
    var backing: Backing = .{ .manager = &manager };
    const bank = try Bank.create(backing.allocator(), manager.memoryParentPort(), .{ .execution = 128, .control = 128, .cleanup = 128, .bounded_read = 128 });
    var live = true;
    defer if (live) bank.destroy();
    backing.expected = manager.snapshot().memory.used_bytes;
    const heap = bank.allocator(.execution);
    const old = try heap.alloc(u8, 80);
    @memset(old, 42);
    try std.testing.expectError(error.OutOfMemory, heap.realloc(old, 96));
    try std.testing.expectEqual(@as(usize, 80), bank.stats(.execution).used);
    try std.testing.expectEqual(@as(u8, 42), old[0]);
    const control = try bank.allocator(.control).alloc(u8, 128);
    const cleanup = try bank.allocator(.cleanup).alloc(u8, 128);
    heap.free(old);
    bank.allocator(.control).free(control);
    bank.allocator(.cleanup).free(cleanup);
    try std.testing.expectEqual(backing.expected, manager.snapshot().memory.used_bytes);
    bank.destroy();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expect(manager.reserveExternal(8 << 20));
    defer manager.releaseExternal(8 << 20);
    try std.testing.expectError(error.ResourceBudgetExceeded, Bank.create(std.testing.allocator, manager.memoryParentPort(), .{}));
}

test "SQL shared worker node memory runtime construction failures return all parent charges" {
    const Manager = local.storage_resource_manager.ResourceManager;
    var reached = false;
    for (0..64) |index| {
        var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 << 20 }, .identity_allocator = std.testing.allocator });
        defer manager.deinit(std.testing.allocator);
        var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index });
        if (local.storage_background_runtime.BackendRuntimeHandle.init(backing.allocator(), .{ .backend = .io_threaded, .shared_sql_memory_parent = manager.memoryParentPort() })) |value| {
            var runtime = value;
            runtime.deinit();
            reached = true;
        } else |_| try std.testing.expect(backing.has_induced_failure);
        try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
        if (reached) break;
    }
    try std.testing.expect(reached);
}

pub fn factory(runtime: *Runtime) ?abi.SqlWorkerFactory {
    // Borrowed leases alias general IO and cannot prove lane isolation.
    if (runtime.usesBorrowedIo() or runtime.lane_limits.sql_worker_capacity < 10) return null;
    return .{ .context = runtime, .capacity = runtime.lane_limits.worker_capacity, .shared_capacity = runtime.lane_limits.sql_worker_capacity, .acquire = acquire };
}
const Owned = struct {
    funding: abi.memory_abi.Allocator,
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
        const funding = self.funding;
        var workers = self.workers;
        // The exclusive executor's allocator points into this wrapper. Join
        // it first; the caller keeps the independent carrier until we return.
        workers.release();
        funding.asStd().destroy(self);
    }
};
fn acquire(raw: *anyopaque, role_value: c_int, capacity: usize, funding: ?*const abi.memory_abi.Allocator, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
    const role = std.enums.fromInt(abi.SqlWorkerRole, role_value) orelse return abi.statusFromError(error.InvalidArgument);
    const runtime: *Runtime = @ptrCast(@alignCast(raw));
    const port = funding orelse return abi.statusFromError(error.InvalidArgument);
    if (!port.valid()) return abi.statusFromError(error.UnsupportedVersion);
    const allocator = port.asStd();
    const owned = allocator.create(Owned) catch return abi.statusFromError(error.OutOfMemory);
    owned.* = .{ .funding = port.*, .workers = undefined };
    const executor_allocator = owned.funding.asStd();
    owned.workers = switch (role) {
        .exclusive => .{ .exclusive = runtime.acquireWorkers(.{ .capacity = capacity, .allocator = executor_allocator }) catch |err| {
            allocator.destroy(owned);
            return abi.statusFromError(if (err == error.OutOfMemory) error.OutOfMemory else error.SqlWriteCapacityUnavailable);
        } },
        else => .{ .shared = runtime.acquireSharedSqlWorkers(switch (role) {
            .execution => .execution,
            .control => .control,
            .cleanup => .cleanup,
            .bounded_read => .bounded_read,
            .exclusive => unreachable,
        }, capacity) catch |err| {
            allocator.destroy(owned);
            return abi.statusFromError(if (err == error.OutOfMemory) error.OutOfMemory else error.SqlWriteCapacityUnavailable);
        } },
    };
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
    producer_allocator: ?*std.mem.Allocator = null,
    allocator: ?std.mem.Allocator = null,

    pub fn acquireNative(runtime: *Runtime, capacity: usize) !Lease {
        const workers = try runtime.acquireWorkers(.{ .capacity = capacity });
        return .{ .capacity = workers.capacity, .native = workers };
    }
    pub fn acquireNativeFunded(runtime: *Runtime, allocator: std.mem.Allocator, capacity: usize) !Lease {
        const workers = try runtime.acquireWorkers(.{ .capacity = capacity, .allocator = allocator });
        return .{ .capacity = workers.capacity, .native = workers };
    }
    pub fn acquireNativeShared(runtime: *Runtime, role: abi.SqlWorkerRole, capacity: usize) !Lease {
        if (role == .exclusive) return acquireNative(runtime, capacity);
        const workers = try runtime.acquireSharedSqlWorkers(switch (role) {
            .execution => .execution,
            .control => .control,
            .cleanup => .cleanup,
            .bounded_read => .bounded_read,
            .exclusive => unreachable,
        }, capacity);
        return .{ .capacity = workers.capacity, .shared = workers };
    }
    pub fn acquireImported(allocator: std.mem.Allocator, producer: abi.SqlWorkerFactory, capacity: usize) !Lease {
        return acquireImportedShared(allocator, producer, .exclusive, capacity);
    }
    pub fn acquireImportedShared(allocator: std.mem.Allocator, producer: abi.SqlWorkerFactory, role: abi.SqlWorkerRole, capacity: usize) !Lease {
        if (!abi.validContext(abi.SqlWorkerFactory, producer.version, producer.struct_size)) return error.UnsupportedVersion;
        const ceiling = if (role == .exclusive) producer.capacity else producer.shared_capacity;
        if (capacity == 0 or capacity > ceiling) return error.SqlWriteCapacityUnavailable;
        // A returned Lease is movable. Never lend a pointer to its stack copy
        // of std.mem.Allocator to an executor that survives this call.
        const carrier = try allocator.create(std.mem.Allocator);
        errdefer allocator.destroy(carrier);
        carrier.* = allocator;
        const funding = abi.memory_abi.Allocator.fromStd(carrier);
        var borrow: abi.SqlWorkerBorrow = undefined;
        const status = producer.acquire(producer.context, @backingInt(role), capacity, &funding, &borrow);
        if (!status.isOk()) return abi.errorFromStatus(status);
        errdefer borrow.release(borrow.context);
        if (!abi.validContext(abi.SqlWorkerBorrow, borrow.version, borrow.struct_size) or borrow.capacity < capacity or borrow.capacity > ceiling or (role == .exclusive and borrow.capacity != capacity) or (role == .bounded_read and borrow.capacity != 1))
            return error.InvalidArgument;
        const received = try borrow.io.receive();
        const receiver = try allocator.create(Receiver);
        receiver.* = received;
        return .{ .capacity = borrow.capacity, .imported = borrow, .receiver = receiver, .allocator = allocator, .producer_allocator = carrier };
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
            self.allocator.?.destroy(self.producer_allocator.?);
            self.producer_allocator = null;
            self.imported = null;
            self.receiver = null;
        }
    }
};

test "SQL imported worker lanes preserve control and reaper progress under saturation" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16, .sql_worker_capacity = 16 } });
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
        fn create(raw: *anyopaque, role: c_int, capacity: usize, funding: ?*const abi.memory_abi.Allocator, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, funding, out);
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
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16, .sql_worker_capacity = 16 } });
    defer runtime.deinit();
    var producer = factory(runtime.ptr()).?;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Lease.acquireImported(failing.allocator(), producer, 2));
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    const Bad = struct {
        fn create(raw: *anyopaque, role: c_int, capacity: usize, funding: ?*const abi.memory_abi.Allocator, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, funding, out);
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
    var small = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .sql_worker_capacity = 9 } });
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
        fn create(raw: *anyopaque, role: c_int, capacity: usize, funding: ?*const abi.memory_abi.Allocator, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, funding, out);
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
    try std.testing.expectEqual(@as(usize, 12), first.capacity);
    try std.testing.expectEqual(first.imported.?.io.userdata, peer.imported.?.io.userdata);
    try std.testing.expect(@as(*const anyopaque, first.io().vtable) != first.imported.?.io.vtable);
    try std.testing.expectEqual(@as(usize, 12), runtime.ptr().laneStats().reserved_workers);
    var control = try Lease.acquireImportedShared(std.testing.allocator, producer, .control, 5);
    defer control.release();
    var cleanup = try Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 1);
    defer cleanup.release();
    try std.testing.expect(first.io().userdata != control.io().userdata);
    try std.testing.expect(control.io().userdata != cleanup.io().userdata);
    try std.testing.expectEqual(@as(usize, 25), runtime.ptr().laneStats().reserved_workers);
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
    try std.testing.expectEqual(@as(usize, 25), runtime.ptr().laneStats().reserved_workers);
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
    inline for (.{ abi.SqlWorkerRole.cleanup, abi.SqlWorkerRole.bounded_read }) |role| {
        var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var producer = factory(runtime.ptr()).?;
        var peer = try Lease.acquireImportedShared(std.testing.allocator, producer, role, 1);
        defer peer.release();
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, Lease.acquireImportedShared(failing.allocator(), producer, role, 1));
        try std.testing.expectEqual(@as(usize, if (role == .bounded_read) 1 else 8), runtime.ptr().laneStats().reserved_workers);
        var out: abi.SqlWorkerBorrow = undefined;
        try std.testing.expect(!producer.acquire(producer.context, 999, 1, null, &out).isOk());
        try std.testing.expectEqual(@as(usize, if (role == .bounded_read) 1 else 8), runtime.ptr().laneStats().reserved_workers);
        producer.version -= 1;
        try std.testing.expectError(error.UnsupportedVersion, Lease.acquireImportedShared(std.testing.allocator, producer, role, 1));
        try std.testing.expectEqual(@as(usize, if (role == .bounded_read) 1 else 8), runtime.ptr().laneStats().reserved_workers);
        peer.release();
        try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    }
}

test "SQL shared worker leases keep runtime alive through shutdown and physical callback join" {
    const builtin = @import("builtin");
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    inline for (.{ abi.SqlWorkerRole.cleanup, abi.SqlWorkerRole.bounded_read }) |role| {
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
        first = try Lease.acquireImportedShared(std.testing.allocator, producer, role, 1);
        peer = try Lease.acquireImportedShared(std.testing.allocator, producer, role, 1);
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
            if (owner.acquireSharedSqlWorkers(if (role == .bounded_read) .bounded_read else .cleanup, 1)) |lease| {
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
        try std.testing.expectEqual(@as(usize, if (role == .bounded_read) 1 else 8), owner.laneStats().reserved_workers);
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
}

test "SQL shared worker saturation stays node bounded across thirty two consumers" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    const owner = runtime.ptr();
    const producer = factory(owner).?;
    var occupied = try Lease.acquireNative(owner, 32);
    defer occupied.release();
    var independent = try Lease.acquireImportedShared(std.testing.allocator, producer, .execution, 1);
    defer independent.release();
    independent.release();
    try std.testing.expectEqual(@as(u64, 0), owner.sharedSqlStats(.execution).rejections_total);
    occupied.release();
    try std.testing.expectError(error.InvalidWorkerCapacity, owner.acquireSharedSqlWorkers(.execution, 0));
    try std.testing.expectError(error.InvalidWorkerCapacity, owner.acquireSharedSqlWorkers(.execution, 13));
    var consumers: [32]Lease = undefined;
    var acquired: usize = 0;
    defer for (consumers[0..acquired]) |*lease| lease.release();
    for (&consumers) |*lease| {
        lease.* = try Lease.acquireImportedShared(std.testing.allocator, producer, .execution, 1);
        acquired += 1;
    }
    const execution = owner.sharedSqlStats(.execution);
    try std.testing.expectEqual(@as(usize, 12), execution.capacity);
    try std.testing.expectEqual(@as(usize, 12), execution.retained_capacity);
    try std.testing.expectEqual(@as(usize, 32), execution.borrowers);
    try std.testing.expectEqual(@as(usize, 32), execution.peak_borrowers);
    try std.testing.expectEqual(@as(u64, 33), execution.acquisitions_total);
    try std.testing.expectEqual(@as(u64, 2), execution.rejections_total);
    var cleanup = try Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 1);
    defer cleanup.release();
    var control = try Lease.acquireImportedShared(std.testing.allocator, producer, .control, 1);
    defer control.release();
    try std.testing.expectEqual(@as(usize, 25), owner.laneStats().reserved_workers);
    const Block = struct {
        entered: std.Io.Event = .unset,
        stop: std.Io.Event = .unset,
        io: std.Io,
        fn run(self: *@This()) void {
            self.entered.set(self.io);
            self.stop.waitUncancelable(self.io);
        }
        fn probe() u32 {
            return 42;
        }
    };
    // Use distinct imported receivers for execution submissions. The producer
    // still rejects the thirteenth concurrent task across all consumers together.
    var blocks: [20]Block = undefined;
    var futures: [20]std.Io.Future(void) = undefined;
    var launched: usize = 0;
    defer {
        for (blocks[0..launched]) |*block| block.stop.set(std.testing.io);
        for (futures[0..launched], blocks[0..launched]) |*future, block| future.await(block.io);
    }
    for (&blocks, &futures, 0..) |*block, *future, i| {
        block.* = .{ .io = if (i < 12) consumers[i].io() else cleanup.io() };
        future.* = try block.io.concurrent(Block.run, .{block});
        launched += 1;
        try block.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = std.Io.Duration.fromSeconds(5), .clock = .awake } });
    }
    try std.testing.expectError(error.ConcurrencyUnavailable, consumers[31].io().concurrent(Block.probe, .{}));
    try std.testing.expectError(error.ConcurrencyUnavailable, cleanup.io().concurrent(Block.probe, .{}));
    var health = try control.io().concurrent(Block.probe, .{});
    try std.testing.expectEqual(@as(u32, 42), health.await(control.io()));
    // Idle consumers may retire while other consumers are physically active.
    for (consumers[12..]) |*lease| lease.release();
    try std.testing.expectEqual(@as(usize, 12), owner.sharedSqlStats(.execution).borrowers);
    try std.testing.expectEqual(@as(usize, 25), owner.laneStats().reserved_workers);
    var metrics: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer metrics.deinit();
    try @import("sql_worker_metrics.zig").append(&metrics.writer, owner);
    const text = metrics.written();
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_executor_sql_borrowers{role=\"execution\"} 12\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_executor_sql_retained_capacity{role=\"execution\"} 12\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_executor_sql_rejections_total{role=\"execution\"} 2\n") != null);
    for (blocks[0..launched]) |*block| block.stop.set(std.testing.io);
    for (futures[0..launched], blocks[0..launched]) |*future, block| future.await(block.io);
    launched = 0;
    for (&consumers) |*lease| lease.release();
    cleanup.release();
    control.release();
    const retired = owner.sharedSqlStats(.execution);
    try std.testing.expectEqual(@as(usize, 0), retired.borrowers);
    try std.testing.expectEqual(@as(usize, 0), retired.retained_capacity);
    try std.testing.expect(!retired.retiring);
    try std.testing.expectEqual(@as(usize, 32), retired.peak_borrowers);
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().worker_active_leases);
    // Reopening starts a new physical executor while counters remain node-wide.
    var reopened = try Lease.acquireNativeShared(owner, .execution, 1);
    defer reopened.release();
    try std.testing.expectEqual(@as(u64, 34), owner.sharedSqlStats(.execution).acquisitions_total);
    try std.testing.expectEqual(@as(usize, 1), owner.sharedSqlStats(.execution).borrowers);
    try std.testing.expectEqual(@as(usize, 12), owner.laneStats().reserved_workers);
}

test "SQL shared worker bounded read floor survives saturated node pools across thirty two consumers" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    const owner = runtime.ptr();
    var producer = factory(owner).?;
    const Forward = struct {
        const Dispatch = @FieldType(abi.native_abi.IoBorrow, "dispatch");
        var original: Dispatch = undefined;
        const P = @typeInfo(@typeInfo(Dispatch).pointer.child).@"fn".param_types;
        fn dispatch(a: P[0].?, b: P[1].?, c: P[2].?, d: P[3].?, e: P[4].?, f: P[5].?) callconv(.c) void {
            original(a, b, c, d, e, f);
        }
        fn create(raw: *anyopaque, role: c_int, capacity: usize, funding: ?*const abi.memory_abi.Allocator, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, funding, out);
            if (result.isOk()) {
                original = out.io.dispatch;
                out.io.dispatch = dispatch;
            }
            return result;
        }
    };
    producer.acquire = Forward.create;
    var execution = try Lease.acquireImportedShared(std.testing.allocator, producer, .execution, 12);
    defer execution.release();
    var cleanup = try Lease.acquireImportedShared(std.testing.allocator, producer, .cleanup, 8);
    defer cleanup.release();
    var control = try Lease.acquireImportedShared(std.testing.allocator, producer, .control, 5);
    defer control.release();
    var bounded: [32]Lease = undefined;
    var acquired: usize = 0;
    defer for (bounded[0..acquired]) |*lease| lease.release();
    for (&bounded) |*lease| {
        lease.* = try Lease.acquireImportedShared(std.testing.allocator, producer, .bounded_read, 1);
        acquired += 1;
        try std.testing.expectEqual(@as(usize, 1), lease.capacity);
        try std.testing.expectEqual(bounded[0].imported.?.io.userdata, lease.imported.?.io.userdata);
    }
    const bounded_io = bounded[0].io();
    try std.testing.expect(bounded_io.userdata != execution.io().userdata);
    try std.testing.expect(bounded_io.userdata != cleanup.io().userdata);
    try std.testing.expect(bounded_io.userdata != control.io().userdata);
    try std.testing.expect(@as(*const anyopaque, bounded_io.vtable) != bounded[0].imported.?.io.vtable);
    try std.testing.expectEqual(@as(usize, 26), owner.laneStats().reserved_workers);
    var rest = try Lease.acquireNative(owner, owner.lane_limits.worker_capacity);
    defer rest.release();
    try std.testing.expectEqual(@as(usize, 58), owner.laneStats().reserved_workers);
    try std.testing.expectError(error.WorkerCapacityExceeded, owner.acquireWorkers(.{ .capacity = 1 }));
    const Block = struct {
        fn run(io: std.Io, gate: *std.Io.Event) void {
            gate.waitUncancelable(io);
        }
        fn probe(io: std.Io) !void {
            try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().statFile(io, "/antfly-bounded-sql-worker-missing", .{}));
        }
    };
    var general_gate: std.Io.Event = .unset;
    var cleanup_gate: std.Io.Event = .unset;
    var general_jobs: std.Io.Group = .init;
    var cleanup_jobs: std.Io.Group = .init;
    const general_io = execution.io();
    const cleanup_io = cleanup.io();
    defer {
        general_gate.set(std.testing.io);
        general_jobs.await(general_io) catch {};
    }
    defer {
        cleanup_gate.set(std.testing.io);
        cleanup_jobs.await(cleanup_io) catch {};
    }
    for (0..execution.capacity) |_| try general_jobs.concurrent(general_io, Block.run, .{ general_io, &general_gate });
    for (0..cleanup.capacity) |_| try cleanup_jobs.concurrent(cleanup_io, Block.run, .{ cleanup_io, &cleanup_gate });
    try std.testing.expectError(error.ConcurrencyUnavailable, general_io.concurrent(Block.probe, .{general_io}));
    try std.testing.expectError(error.ConcurrencyUnavailable, cleanup_io.concurrent(Block.probe, .{cleanup_io}));
    var task = try bounded_io.concurrent(Block.probe, .{bounded_io});
    try task.await(bounded_io);
    var health = try control.io().concurrent(Block.probe, .{control.io()});
    try health.await(control.io());
    var bounded_gate: std.Io.Event = .unset;
    var held = try bounded_io.concurrent(Block.run, .{ bounded_io, &bounded_gate });
    defer {
        bounded_gate.set(std.testing.io);
        held.await(bounded_io);
    }
    try std.testing.expectError(error.ConcurrencyUnavailable, bounded[31].io().concurrent(Block.probe, .{bounded[31].io()}));
    bounded[31].release();
    try std.testing.expectEqual(@as(usize, 31), owner.sharedSqlStats(.bounded_read).borrowers);
    try std.testing.expectEqual(@as(usize, 58), owner.laneStats().reserved_workers);
    var metrics: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer metrics.deinit();
    try @import("sql_worker_metrics.zig").append(&metrics.writer, owner);
    try std.testing.expect(std.mem.indexOf(u8, metrics.written(), "antfly_executor_sql_retained_capacity{role=\"bounded_read\"} 1\n") != null);
}

test "SQL imported worker bounded read capacity mismatch retires only the failed consumer" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var producer = factory(runtime.ptr()).?;
    var peer = try Lease.acquireImportedShared(std.testing.allocator, producer, .bounded_read, 1);
    defer peer.release();
    const Bad = struct {
        fn create(raw: *anyopaque, role: c_int, capacity: usize, funding: ?*const abi.memory_abi.Allocator, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
            const result = acquire(raw, role, capacity, funding, out);
            if (result.isOk()) out.capacity = 2;
            return result;
        }
    };
    producer.acquire = Bad.create;
    try std.testing.expectError(error.InvalidArgument, Lease.acquireImportedShared(std.testing.allocator, producer, .bounded_read, 1));
    try std.testing.expectEqual(@as(usize, 1), runtime.ptr().sharedSqlStats(.bounded_read).borrowers);
    try std.testing.expectEqual(@as(usize, 1), runtime.ptr().laneStats().reserved_workers);
    peer.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
}

test "SQL shared worker bounded read policy retains all roles inside small and large node ceilings" {
    inline for (.{ @as(usize, 10), @as(usize, 16), @as(usize, 32), @as(usize, 64) }) |ceiling| {
        // Redistribute the fixed profile for the larger SQL envelope rather
        // than exceeding the process-wide aggregate worker ceiling.
        var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{
            .sql_worker_capacity = ceiling,
            .durable_background = if (ceiling > 32) 48 - (ceiling - 32) else 48,
        } });
        defer runtime.deinit();
        const owner = runtime.ptr();
        var execution = try Lease.acquireNativeShared(owner, .execution, 1);
        defer execution.release();
        var control = try Lease.acquireNativeShared(owner, .control, 5);
        defer control.release();
        var cleanup = try Lease.acquireNativeShared(owner, .cleanup, 1);
        defer cleanup.release();
        var bounded = try Lease.acquireNativeShared(owner, .bounded_read, 1);
        defer bounded.release();
        try std.testing.expectEqual(@as(usize, 1), bounded.capacity);
        try std.testing.expectEqual(execution.capacity + control.capacity + cleanup.capacity + 1, owner.laneStats().reserved_workers);
        try std.testing.expect(owner.laneStats().reserved_workers <= ceiling);
        bounded.release();
        cleanup.release();
        control.release();
        execution.release();
        try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_workers);
        try std.testing.expectEqual(@as(usize, 0), owner.laneStats().worker_active_leases);
    }
}

test "SQL imported worker producer heap survives exhausted runtime allocator until physical join" {
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(backing.allocator(), .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16, .sql_worker_capacity = 16 } });
    defer runtime.deinit();
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    defer {
        backing.fail_index = std.math.maxInt(usize);
        backing.resize_fail_index = std.math.maxInt(usize);
    }
    var memory: @import("../common/allocation_budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 512 << 10 };
    var lease = try Lease.acquireImported(memory.allocator(), factory(runtime.ptr()).?, 1);
    defer lease.release();
    try std.testing.expect(memory.stats().used_bytes != 0);
    const Work = struct {
        fn run(done: *std.atomic.Value(bool)) void {
            done.store(true, .release);
        }
    };
    var done: std.atomic.Value(bool) = .init(false);
    const io = lease.io();
    var future = try io.concurrent(Work.run, .{&done});
    future.await(io);
    try std.testing.expect(done.load(.acquire));
    try std.testing.expect(!backing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 1), runtime.ptr().laneStats().reserved_workers);
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
}

test "SQL imported worker native funded executor never spends the runtime heap" {
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(backing.allocator(), .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16, .sql_worker_capacity = 16 } });
    defer runtime.deinit();
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    defer {
        backing.fail_index = std.math.maxInt(usize);
        backing.resize_fail_index = std.math.maxInt(usize);
    }
    var memory: @import("../common/allocation_budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 512 << 10 };
    var lease = try Lease.acquireNativeFunded(runtime.ptr(), memory.allocator(), 1);
    defer lease.release();
    const Work = struct {
        fn run(done: *std.atomic.Value(bool)) void {
            done.store(true, .release);
        }
    };
    var done: std.atomic.Value(bool) = .init(false);
    const io = lease.io();
    var future = try io.concurrent(Work.run, .{&done});
    future.await(io);
    try std.testing.expect(done.load(.acquire));
    try std.testing.expect(memory.stats().used_bytes != 0);
    lease.release();
    try std.testing.expect(!backing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
}

test "SQL shared worker borrower funding retires independently of peer executor lifetime" {
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(backing.allocator(), .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16, .sql_worker_capacity = 16 } });
    defer runtime.deinit();
    var anchor = try Lease.acquireNativeShared(runtime.ptr(), .control, 1);
    defer anchor.release();
    var first_memory: @import("../common/allocation_budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 512 << 10 };
    var peer_memory: @import("../common/allocation_budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 512 << 10 };
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    defer {
        backing.fail_index = std.math.maxInt(usize);
        backing.resize_fail_index = std.math.maxInt(usize);
    }
    var first = try Lease.acquireImportedShared(first_memory.allocator(), factory(runtime.ptr()).?, .control, 1);
    defer first.release();
    var peer = try Lease.acquireImportedShared(peer_memory.allocator(), factory(runtime.ptr()).?, .control, 1);
    defer peer.release();
    try std.testing.expect(!backing.has_induced_failure);
    try std.testing.expect(first_memory.stats().used_bytes != 0 and peer_memory.stats().used_bytes != 0);
    first.release();
    try std.testing.expectEqual(@as(usize, 0), first_memory.stats().used_bytes);
    first_memory.limit = 0;
    // Shared executor/task allocation belongs to the node, never its first
    // borrower. Its node allocator is restored before submitting peer work.
    backing.fail_index = std.math.maxInt(usize);
    backing.resize_fail_index = std.math.maxInt(usize);
    const Work = struct {
        fn run(done: *std.atomic.Value(bool)) void {
            done.store(true, .release);
        }
    };
    var done: std.atomic.Value(bool) = .init(false);
    const io = peer.io();
    var future = try io.concurrent(Work.run, .{&done});
    future.await(io);
    try std.testing.expect(done.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), first_memory.stats().rejected_total);
    peer.release();
    try std.testing.expectEqual(@as(usize, 0), peer_memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 5), runtime.ptr().laneStats().reserved_workers);
    anchor.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
}

test "SQL imported worker funded construction failures retire carriers wrappers and executor credit" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16, .sql_worker_capacity = 16 } });
    defer runtime.deinit();
    var reached_success = false;
    for (0..32) |index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index });
        var memory: @import("../common/allocation_budget.zig").Shared = .{ .child = failing.allocator(), .limit = 512 << 10 };
        if (Lease.acquireImported(memory.allocator(), factory(runtime.ptr()).?, 1)) |value| {
            var lease = value;
            lease.release();
            reached_success = true;
        } else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
        }
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
        try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
        if (reached_success) break;
    }
    try std.testing.expect(reached_success);
    var allocator = std.testing.allocator;
    var invalid = abi.memory_abi.Allocator.fromStd(&allocator);
    invalid.version = 0;
    const producer = factory(runtime.ptr()).?;
    var out: abi.SqlWorkerBorrow = undefined;
    try std.testing.expectEqual(error.UnsupportedVersion, abi.errorFromStatus(producer.acquire(producer.context, @backingInt(abi.SqlWorkerRole.exclusive), 1, &invalid, &out)));
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(producer.acquire(producer.context, @backingInt(abi.SqlWorkerRole.exclusive), 1, null, &out)));
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
}

test "SQL shared worker reservations coexist with the full dedicated service envelope" {
    inline for (.{ false, true }) |imported| {
        var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
        defer runtime.deinit();
        const owner = runtime.ptr();
        // Raft senders/snapshots, listeners, maintenance, reporting, apply,
        // progress, and the API reaper need independent service reservations.
        const composition = [_]usize{ 4, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1 };
        var services: [composition.len + 1]Lease = undefined;
        var acquired: usize = 0;
        defer for (services[0..acquired]) |*lease| lease.release();
        var used: usize = 0;
        for (composition) |capacity| {
            services[acquired] = try Lease.acquireNative(owner, capacity);
            acquired += 1;
            used += capacity;
        }
        var roles: [4]Lease = undefined;
        var retained: usize = 0;
        defer for (roles[0..retained]) |*lease| lease.release();
        inline for (.{ abi.SqlWorkerRole.execution, .control, .cleanup, .bounded_read }) |role| {
            roles[retained] = if (imported)
                try Lease.acquireImportedShared(std.testing.allocator, factory(owner).?, role, 1)
            else
                try Lease.acquireNativeShared(owner, role, 1);
            retained += 1;
        }
        services[acquired] = try Lease.acquireNative(owner, owner.lane_limits.worker_capacity - used);
        acquired += 1;
        try std.testing.expectError(error.WorkerCapacityExceeded, owner.acquireWorkers(.{}));
        const Probe = struct {
            fn run() u32 {
                return 42;
            }
        };
        for (services[0..acquired]) |*lease| {
            var task = try lease.io().concurrent(Probe.run, .{});
            try std.testing.expectEqual(@as(u32, 42), task.await(lease.io()));
        }
        for (&roles) |*lease| {
            var task = try lease.io().concurrent(Probe.run, .{});
            try std.testing.expectEqual(@as(u32, 42), task.await(lease.io()));
        }
        var sql_capacity: usize = 0;
        for (roles) |lease| sql_capacity += lease.capacity;
        try std.testing.expectEqual(@as(usize, owner.lane_limits.worker_capacity) + sql_capacity, owner.laneStats().reserved_workers);
    }
}

test "SQL shared worker imported limits and failed admission preserve independent class credit" {
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 1, .sql_worker_capacity = 16 } });
    defer runtime.deinit();
    const owner = runtime.ptr();
    const producer = factory(owner).?;
    try std.testing.expectEqual(@as(usize, 1), producer.capacity);
    try std.testing.expectEqual(@as(usize, 16), producer.shared_capacity);
    var service = try Lease.acquireImported(std.testing.allocator, producer, 1);
    defer service.release();
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Lease.acquireImported(std.testing.allocator, producer, 2));
    // Control's five workers exceed the service envelope, but use the SQL one.
    var control = try Lease.acquireImportedShared(std.testing.allocator, producer, .control, 5);
    defer control.release();
    try std.testing.expectEqual(@as(usize, 1), owner.laneStats().reserved_service_workers);
    try std.testing.expectEqual(@as(usize, 5), owner.laneStats().reserved_sql_workers);
    try std.testing.expectEqual(@as(usize, 6), owner.laneStats().reserved_workers);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Lease.acquireImportedShared(failing.allocator(), producer, .cleanup, 1));
    try std.testing.expectEqual(@as(usize, 5), owner.laneStats().reserved_sql_workers);
    var bad = producer;
    bad.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, Lease.acquireImportedShared(std.testing.allocator, bad, .control, 1));
    bad = producer;
    bad.shared_capacity = 0;
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Lease.acquireImportedShared(std.testing.allocator, bad, .control, 1));
    control.release();
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_sql_workers);
    try std.testing.expectEqual(@as(usize, 1), owner.laneStats().reserved_service_workers);
    service.release();
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_service_workers);
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().worker_active_leases);
}

test "SQL shared worker construction rollback preserves service reservation and permits retry" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(failing.allocator(), .{ .backend = .io_threaded });
    defer runtime.deinit();
    const owner = runtime.ptr();
    var service = try Lease.acquireNative(owner, owner.lane_limits.worker_capacity);
    defer service.release();
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    defer {
        failing.fail_index = std.math.maxInt(usize);
        failing.resize_fail_index = std.math.maxInt(usize);
    }
    try std.testing.expectError(error.OutOfMemory, Lease.acquireNativeShared(owner, .execution, 1));
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_sql_workers);
    try std.testing.expectEqual(@as(usize, 32), owner.laneStats().reserved_service_workers);
    try std.testing.expectEqual(@as(usize, 32), owner.laneStats().reserved_workers);
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    var execution = try Lease.acquireNativeShared(owner, .execution, 1);
    defer execution.release();
    try std.testing.expectEqual(@as(usize, 12), owner.laneStats().reserved_sql_workers);
    execution.release();
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_sql_workers);
    try std.testing.expectEqual(@as(usize, 32), owner.laneStats().reserved_workers);
    service.release();
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().reserved_service_workers);
    try std.testing.expectEqual(@as(usize, 0), owner.laneStats().worker_active_leases);
}

test "SQL shared worker runtime memory metadata admits before physical allocation and retires after free" {
    const Memory = local.storage_background_runtime.RuntimeMemory;
    const Manager = local.storage_resource_manager.ResourceManager;
    var root = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 << 20 } });
    defer root.deinit(std.testing.allocator);
    const Tracking = struct {
        root: *Manager,
        minimum: usize = @sizeOf(Memory.Owner),
        allocations: usize = 0,
        releases: usize = 0,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.root.snapshot().memory.used_bytes >= self.minimum);
            self.allocations += 1;
            return std.testing.allocator.rawAlloc(len, alignment, ret);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.root.snapshot().memory.used_bytes >= self.minimum);
            std.testing.allocator.rawFree(bytes, alignment, ret);
            self.releases += 1;
        }
    };
    var tracking: Tracking = .{ .root = &root };
    const memory = try Memory.Owner.create(tracking.allocator(), root.memoryParentPort());
    var live = true;
    defer if (live) memory.destroy();
    tracking.minimum += 73;
    const bytes = try memory.allocator().alloc(u8, 73);
    try std.testing.expectEqual(@as(u64, tracking.minimum), root.snapshot().memory.used_bytes);
    memory.allocator().free(bytes);
    tracking.minimum -= 73;
    try std.testing.expectEqual(@as(u64, tracking.minimum), root.snapshot().memory.used_bytes);
    const remaining = (1 << 20) - root.snapshot().memory.used_bytes;
    try std.testing.expect(root.reserveExternal(remaining));
    const before = tracking.allocations;
    try std.testing.expectError(error.OutOfMemory, memory.allocator().alloc(u8, 1));
    try std.testing.expectEqual(before, tracking.allocations);
    root.releaseExternal(remaining);
    memory.destroy();
    live = false;
    try std.testing.expectEqual(tracking.allocations, tracking.releases);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker runtime memory realloc preserves old bytes at the node peak limit" {
    const Memory = local.storage_background_runtime.RuntimeMemory;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = @sizeOf(Memory.Owner) + 100 } });
    defer root.deinit(std.testing.allocator);
    const memory = try Memory.Owner.create(std.testing.allocator, root.memoryParentPort());
    defer memory.destroy();
    const allocator = memory.allocator();
    const bytes = try allocator.alloc(u8, 60);
    defer allocator.free(bytes);
    @memset(bytes, 42);
    try std.testing.expectError(error.OutOfMemory, allocator.realloc(bytes, 100));
    try std.testing.expectEqual(@as(u64, @sizeOf(Memory.Owner) + 60), root.snapshot().memory.used_bytes);
    for (bytes) |value| try std.testing.expectEqual(@as(u8, 42), value);
}

test "SQL shared worker runtime memory construction failures retire header and child charges" {
    const Check = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var root = local.storage_resource_manager.ResourceManager.init(.{});
            defer root.deinit(std.testing.allocator);
            defer std.debug.assert(root.snapshot().memory.used_bytes == 0);
            var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(allocator, .{ .backend = .manual, .memory_parent = root.memoryParentPort() });
            defer runtime.deinit();
            try std.testing.expect(root.snapshot().memory.used_bytes >= @sizeOf(Runtime));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL shared worker runtime memory charges real lane stacks and preserves prepaid control under node exhaustion" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 2048 << 20 } });
    defer root.deinit(std.testing.allocator);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .io_threaded,
        .memory_parent = root.memoryParentPort(),
        .shared_sql_memory_parent = root.memoryParentPort(),
    });
    var live = true;
    defer if (live) runtime.deinit();
    const owner = runtime.ptr();
    const memory = owner.runtime_memory.?;
    const baseline = memory.stackStats().used;
    try std.testing.expect(baseline > 0); // Actual durable reaper's guarded map.
    var api = try owner.acquireApiLane();
    defer api.release();
    var service = try owner.acquireWorkers(.{ .allocator = std.testing.allocator });
    defer service.release();
    var control = try owner.acquireSharedSqlWorkers(.control, 1);
    defer control.release();
    const Work = struct {
        fn probe() u32 {
            return 42;
        }
    };
    var first = try api.io().concurrent(Work.probe, .{});
    try std.testing.expectEqual(@as(u32, 42), first.await(api.io()));
    const after_api = memory.stackStats().used;
    try std.testing.expect(after_api > baseline);
    const sibling = (2048 << 20) - root.snapshot().memory.used_bytes;
    try std.testing.expect(root.reserveExternal(sibling));
    var held = true;
    defer if (held) root.releaseExternal(sibling);
    const before_rejections = memory.stackStats().rejected;
    try std.testing.expectError(error.ConcurrencyUnavailable, service.io().concurrent(Work.probe, .{}));
    try std.testing.expect(memory.stackStats().rejected > before_rejections);
    try std.testing.expectEqual(after_api, memory.stackStats().used);
    var progress = try control.io().concurrent(Work.probe, .{});
    try std.testing.expectEqual(@as(u32, 42), progress.await(control.io()));
    root.releaseExternal(sibling);
    held = false;
    var retry = try service.io().concurrent(Work.probe, .{});
    try std.testing.expectEqual(@as(u32, 42), retry.await(service.io()));
    try std.testing.expect(memory.stackStats().used > after_api);
    service.release();
    try std.testing.expectEqual(after_api, memory.stackStats().used);
    control.release();
    api.release();
    runtime.deinit();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker runtime memory denies before backing allocation and refunds backing failure" {
    const Memory = local.storage_background_runtime.RuntimeMemory;
    var denied = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = @sizeOf(Memory.Owner) - 1 } });
    defer denied.deinit(std.testing.allocator);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.ResourceBudgetExceeded, Memory.Owner.create(failing.allocator(), denied.memoryParentPort()));
    try std.testing.expect(!failing.has_induced_failure);
    var root = local.storage_resource_manager.ResourceManager.init(.{});
    defer root.deinit(std.testing.allocator);
    try std.testing.expectError(error.OutOfMemory, Memory.Owner.create(failing.allocator(), root.memoryParentPort()));
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    failing.fail_index = std.math.maxInt(usize);
    const memory = try Memory.Owner.create(failing.allocator(), root.memoryParentPort());
    var live = true;
    defer if (live) memory.destroy();
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, memory.allocator().alloc(u8, 10));
    try std.testing.expectEqual(@as(u64, @sizeOf(Memory.Owner)), root.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), memory.heapStats().used);
    memory.destroy();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker runtime memory accepts only exact page stack mappings" {
    const Memory = local.storage_background_runtime.RuntimeMemory;
    const page = std.heap.pageSize();
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = @sizeOf(Memory.Owner) + page } });
    defer root.deinit(std.testing.allocator);
    const memory = try Memory.Owner.create(std.testing.allocator, root.memoryParentPort());
    defer memory.destroy();
    const allocator = memory.stackAllocator();
    try std.testing.expect(allocator.rawAlloc(1, .of(u8), @returnAddress()) == null);
    try std.testing.expect(allocator.rawAlloc(page, .fromByteUnits(page * 2), @returnAddress()) == null);
    const bytes = allocator.rawAlloc(page, .fromByteUnits(page), @returnAddress()) orelse return error.OutOfMemory;
    defer allocator.rawFree(bytes[0..page], .fromByteUnits(page), @returnAddress());
    try std.testing.expectEqual(@as(usize, page), memory.stackStats().used);
    try std.testing.expectEqual(@as(u64, @sizeOf(Memory.Owner) + page), root.snapshot().memory.used_bytes);
    try std.testing.expect(allocator.rawAlloc(page, .fromByteUnits(page), @returnAddress()) == null);
}

test "SQL shared worker PDF scratch charges rounded pages and preserves old memory on failed growth" {
    const Memory = local.storage_background_runtime.RuntimeMemory;
    const page = std.heap.pageSize();
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = @sizeOf(Memory.Owner) + page } });
    defer root.deinit(std.testing.allocator);
    const memory = try Memory.Owner.create(std.testing.allocator, root.memoryParentPort());
    defer memory.destroy();
    const allocator = memory.pageAllocator();
    const bytes = try allocator.alloc(u8, 1);
    defer allocator.free(bytes);
    bytes[0] = 42;
    try std.testing.expectEqual(page, memory.pageStats().used);
    try std.testing.expectEqual(@as(u64, @sizeOf(Memory.Owner) + page), root.snapshot().memory.used_bytes);
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 1));
    try std.testing.expectError(error.OutOfMemory, allocator.realloc(bytes, page + 1));
    try std.testing.expectEqual(@as(u8, 42), bytes[0]);
    try std.testing.expectEqual(page, memory.pageStats().used);
    try std.testing.expect(allocator.rawAlloc(std.math.maxInt(usize), .of(u8), @returnAddress()) == null);
}

test "SQL shared worker PDF construction joins and unmaps earlier workers when a later stack is denied" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Memory = local.storage_background_runtime.RuntimeMemory;
    const Executor = @typeInfo(@typeInfo(@FieldType(Runtime.PdfRenderLaneLease, "borrowed_executor")).optional.child).pointer.child;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 24 << 20 } });
    defer root.deinit(std.testing.allocator);
    const memory = try Memory.Owner.create(std.testing.allocator, root.memoryParentPort());
    defer memory.destroy();
    try std.testing.expectError(error.OutOfMemory, Executor.create(memory.allocator(), .{
        .worker_count = 2,
        .queue_capacity = 2,
        .max_scratch_bytes = 1 << 20,
        .retained_scratch_bytes_per_worker = 0,
        .worker_stack_allocator = memory.stackAllocator(),
        .scratch_allocator = memory.pageAllocator(),
    }));
    try std.testing.expectEqual(@as(usize, 0), memory.stackStats().used);
    try std.testing.expectEqual(@as(usize, 0), memory.heapStats().used);
    try std.testing.expectEqual(@as(u64, @sizeOf(Memory.Owner)), root.snapshot().memory.used_bytes);
    const recovered = try Executor.create(memory.allocator(), .{
        .worker_count = 1,
        .queue_capacity = 1,
        .max_scratch_bytes = 1 << 20,
        .retained_scratch_bytes_per_worker = 0,
        .worker_stack_allocator = memory.stackAllocator(),
        .scratch_allocator = memory.pageAllocator(),
    });
    recovered.destroy();
    try std.testing.expectEqual(@as(u64, @sizeOf(Memory.Owner)), root.snapshot().memory.used_bytes);
}

test "SQL shared worker PDF real lane retains scratch under node pressure and recovers after refusal" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
    defer root.deinit(std.testing.allocator);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort(), .lane_limits = .{ .pdf_render = 1, .control = 1, .raft_inbound = 1, .raft_outbound = 1 } });
    var live = true;
    defer if (live) runtime.deinit();
    const before = runtime.ptr().runtimeMemoryStats().?.stacks.used;
    var lane = try runtime.ptr().acquirePdfRenderLane();
    defer lane.release();
    try std.testing.expect(runtime.ptr().runtimeMemoryStats().?.stacks.used > before);
    const Job = struct {
        count: usize = 2000,
        failed: bool = false,
        fn run(raw: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = allocator.alloc(u8, self.count) catch {
                self.failed = true;
                return;
            };
            @memset(bytes, 42);
        }
    };
    var job: Job = .{};
    _ = try lane.executor().runBatch(&.{&job}, Job.run, 32 << 20);
    try std.testing.expect(!job.failed);
    const retained = runtime.ptr().runtimeMemoryStats().?.scratch.used;
    try std.testing.expect(retained > 0 and retained % std.heap.pageSize() == 0);
    const sibling = (128 << 20) - root.snapshot().memory.used_bytes;
    try std.testing.expect(root.reserveExternal(sibling));
    var held = true;
    defer if (held) root.releaseExternal(sibling);
    job.count = 8 << 20;
    _ = try lane.executor().runBatch(&.{&job}, Job.run, 32 << 20);
    try std.testing.expect(job.failed);
    try std.testing.expect(runtime.ptr().runtimeMemoryStats().?.scratch.rejected > 0);
    try std.testing.expectEqual(retained, runtime.ptr().runtimeMemoryStats().?.scratch.used);
    root.releaseExternal(sibling);
    held = false;
    job.failed = false;
    _ = try lane.executor().runBatch(&.{&job}, Job.run, 32 << 20);
    try std.testing.expect(!job.failed);
    lane.release();
    runtime.deinit();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker PDF stack and scratch remain charged through pthread TLS retirement" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Probe = struct {
        key: std.c.pthread_key_t,
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(false),
        done: std.Io.Event = .unset,
        fn destructor(raw: ?*anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
        }
        fn work(raw: *anyopaque, scratch: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = scratch.alloc(u8, 2000) catch unreachable;
            @memset(bytes, 42);
            std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
        }
        fn close(handle: *local.storage_background_runtime.BackendRuntimeHandle, self: *@This()) void {
            handle.deinit();
            self.done.set(std.testing.io);
        }
    };
    var key: std.c.pthread_key_t = undefined;
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destructor));
    defer _ = std.c.pthread_key_delete(key);
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
    defer root.deinit(std.testing.allocator);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort(), .lane_limits = .{ .pdf_render = 1, .control = 1, .raft_inbound = 1, .raft_outbound = 1 } });
    var live = true;
    defer if (live) runtime.deinit();
    const memory = runtime.ptr().runtime_memory.?;
    var lane = try runtime.ptr().acquirePdfRenderLane();
    defer lane.release();
    var probe: Probe = .{ .key = key };
    _ = try lane.executor().runBatch(&.{&probe}, Probe.work, 1 << 20);
    lane.release();
    var closing: ?std.Thread = null;
    defer {
        probe.go.store(true, .release);
        if (closing) |thread| {
            thread.join();
            live = false;
        }
    }
    closing = try std.Thread.spawn(.{}, Probe.close, .{ &runtime, &probe });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.entered.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(!probe.done.isSet());
    try std.testing.expect(memory.stackStats().used > 0);
    try std.testing.expect(memory.pageStats().used > 0);
    try std.testing.expect(root.snapshot().memory.used_bytes >= memory.stackStats().used + memory.pageStats().used);
    probe.go.store(true, .release);
    closing.?.join();
    closing = null;
    live = false;
    try std.testing.expect(probe.done.isSet());
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker private manual IO funds both authorities through physical teardown" {
    inline for (.{ false, true }) |general| {
        var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
        defer root.deinit(std.testing.allocator);
        var runtime = if (general)
            try local.storage_background_runtime.BackendRuntimeHandle.initManualWithOwnedIoAndParent(std.testing.allocator, root.memoryParentPort())
        else
            try local.storage_background_runtime.BackendRuntimeHandle.initManualWithOwnedFilesystemIoAndParent(std.testing.allocator, root.memoryParentPort());
        var live = true;
        defer if (live) runtime.deinit();
        const io = if (general) runtime.ptr().io().? else runtime.ptr().filesystemIo().?;
        const Work = struct {
            fn run() u32 {
                return 42;
            }
        };
        var task = try io.concurrent(Work.run, .{});
        try std.testing.expectEqual(@as(u32, 42), task.await(io));
        try std.testing.expect(runtime.ptr().runtimeMemoryStats().?.stacks.used > 0);
        runtime.deinit();
        live = false;
        try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    }
}

test "SQL shared worker native retained state pins runtime parent and refuses new owners during close" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Native = local.storage_lsm_backend_mod.NativeStorage;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
    defer root.deinit(std.testing.allocator);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort(), .lane_limits = .{ .control = 1, .raft_inbound = 1, .raft_outbound = 1 } });
    var live = true;
    defer if (live) runtime.deinit();
    const pool = runtime.ptr().nativeStoragePool();
    var native = try Native.initWithPool(std.testing.allocator, .threaded, pool);
    const memory = native.state.memory.?;
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    const io = native.state.threaded.io();
    var task = try io.concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), task.await(io));
    try std.testing.expect(memory.stackStats().used > 0);
    var lease = try native.acquireLease();
    defer lease.deinit();
    native.deinit();
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    pool.closeAdmission();
    try std.testing.expectError(error.StorageClosed, Native.initWithPool(std.testing.allocator, .threaded, pool));
    const Closing = struct {
        done: std.Io.Event = .unset,
        fn run(owner: *local.storage_background_runtime.BackendRuntimeHandle, self: *@This()) void {
            owner.deinit();
            self.done.set(std.testing.io);
        }
    };
    var closing: Closing = .{};
    var thread: ?std.Thread = null;
    defer {
        lease.deinit();
        if (thread) |running| {
            running.join();
            live = false;
        }
    }
    thread = try std.Thread.spawn(.{}, Closing.run, .{ &runtime, &closing });
    try std.testing.expectError(error.Timeout, closing.done.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }));
    try std.testing.expect(memory.stackStats().used > 0);
    try std.testing.expect(root.snapshot().memory.used_bytes > 0);
    lease.deinit();
    thread.?.join();
    thread = null;
    live = false;
    try std.testing.expect(closing.done.isSet());
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker native constructor failure refunds node bytes and pool ownership" {
    const Native = local.storage_lsm_backend_mod.NativeStorage;
    const Check = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var root = local.storage_resource_manager.ResourceManager.init(.{});
            defer root.deinit(std.testing.allocator);
            var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual, .memory_parent = root.memoryParentPort() });
            defer runtime.deinit();
            const baseline = root.snapshot().memory.used_bytes;
            const pool = runtime.ptr().nativeStoragePool();
            defer std.debug.assert(pool.retainedStates() == 0);
            defer std.debug.assert(root.snapshot().memory.used_bytes == baseline);
            var native = try Native.initWithPool(allocator, .threaded, pool);
            defer native.deinit();
            try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL shared worker private manual IO construction failures refund all node charges" {
    const Check = struct {
        fn run(allocator: std.mem.Allocator, general: bool) !void {
            var root = local.storage_resource_manager.ResourceManager.init(.{});
            defer root.deinit(std.testing.allocator);
            defer std.debug.assert(root.snapshot().memory.used_bytes == 0);
            var runtime = if (general)
                try local.storage_background_runtime.BackendRuntimeHandle.initManualWithOwnedIoAndParent(allocator, root.memoryParentPort())
            else
                try local.storage_background_runtime.BackendRuntimeHandle.initManualWithOwnedFilesystemIoAndParent(allocator, root.memoryParentPort());
            runtime.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{true});
}

test "SQL shared worker native state retains memory and pool ownership through actual TLS destruction" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Native = local.storage_lsm_backend_mod.NativeStorage;
    const Probe = struct {
        key: std.c.pthread_key_t,
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(false),
        done: std.Io.Event = .unset,
        fn destructor(raw: ?*anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
        }
        fn work(self: *@This()) void {
            std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
        }
        fn close(lease: *Native.Lease, self: *@This()) void {
            lease.deinit();
            self.done.set(std.testing.io);
        }
    };
    var key: std.c.pthread_key_t = undefined;
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destructor));
    defer _ = std.c.pthread_key_delete(key);
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
    defer root.deinit(std.testing.allocator);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual, .memory_parent = root.memoryParentPort() });
    defer runtime.deinit();
    const pool = runtime.ptr().nativeStoragePool();
    var native = try Native.initWithPool(std.testing.allocator, .threaded, pool);
    var lease = try native.acquireLease();
    defer lease.deinit();
    const memory = native.state.memory.?;
    const io = native.state.threaded.io();
    var probe: Probe = .{ .key = key };
    var task = try io.concurrent(Probe.work, .{&probe});
    task.await(io);
    native.deinit();
    var closing: ?std.Thread = null;
    defer {
        probe.go.store(true, .release);
        if (closing) |thread| thread.join();
    }
    closing = try std.Thread.spawn(.{}, Probe.close, .{ &lease, &probe });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.entered.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(!probe.done.isSet());
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    try std.testing.expect(memory.stackStats().used > 0);
    try std.testing.expect(root.snapshot().memory.used_bytes >= memory.stackStats().used);
    probe.go.store(true, .release);
    closing.?.join();
    closing = null;
    try std.testing.expect(probe.done.isSet());
    try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
}

test "SQL shared worker Lite private file IO stays stable when the file value moves" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Native = local.storage_lite_native.NativeFile;
    const Resources = local.storage_resource_manager;
    var root = Resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
    defer root.deinit(std.testing.allocator);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/stable.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var first = try Native.createWithOptions(std.testing.allocator, path, .{ .resource_manager = &root, .no_sync = true });
    const io = first.runtime();
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var task = try io.concurrent(Work.run, .{});
    var moved = first;
    first = undefined;
    var live = true;
    defer if (live) moved.close();
    try std.testing.expectEqual(io.userdata, moved.runtime().userdata);
    try std.testing.expectEqual(@as(u32, 42), task.await(io));
    try std.testing.expect(moved.owned_io.?.memory.?.stackStats().used > 0);
    try std.testing.expect(!moved.usesBorrowedIo());
    moved.close();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker Lite private IO and shared manager construction failures return all allocations" {
    const Native = local.storage_lite_native.NativeFile;
    const IoOwnerType = @typeInfo(@typeInfo(@FieldType(Native, "owned_io")).optional.child).pointer.child;
    const Check = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const manager = try local.storage_resource_manager.SharedResourceManager.create(allocator, .{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 }, .identity_allocator = allocator });
            defer manager.releaseOwned(allocator);
            const owner = try IoOwnerType.create(allocator, manager);
            owner.destroy();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL shared worker Lite artifact IO retains a shared manager after backend and owner close" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = backing.allocator();
    const manager = try local.storage_resource_manager.SharedResourceManager.create(allocator, .{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 }, .identity_allocator = allocator });
    var held = true;
    defer if (held) manager.releaseOwned(allocator);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/provider.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var backend = try local.storage_lite_backend.Handle.createWithOptions(allocator, path, .{ .resource_manager = manager, .exclusive = true, .no_sync = true });
    var open = true;
    defer if (open) backend.deinit();
    const bytes = try std.testing.allocator.alloc(u8, 128 << 10);
    defer std.testing.allocator.free(bytes);
    @memset(bytes, 73);
    const key = "__antfly_lite/segments/provider";
    try backend.native_index_storage.?.storage().writeFileAbsolute(key, bytes);
    var source = try backend.native_docstore.?.openArtifactSource(key);
    var source_open = true;
    defer if (source_open) source.close();
    const registry = backend.native_docstore.?.artifact_registry.?;
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    const io = registry.io_owner.io();
    var task = try io.concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), task.await(io));
    try std.testing.expect(registry.io_owner.memory.?.stackStats().used > 0);
    backend.deinit();
    open = false;
    manager.releaseOwned(allocator);
    held = false;
    var out: [16]u8 = undefined;
    try source.readInto(100 << 10, &out);
    try std.testing.expectEqualSlices(u8, bytes[100 << 10 ..][0..16], &out);
    try std.testing.expect(manager.snapshot().memory.used_bytes > 0);
    source.close();
    source_open = false;
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
}

test "SQL shared worker Lite borrowed IO retains managed resources without owning the caller IO" {
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = backing.allocator();
    const manager = try local.storage_resource_manager.SharedResourceManager.create(allocator, .{ .identity_allocator = allocator });
    var held = true;
    defer if (held) manager.releaseOwned(allocator);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/borrowed.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var file = try local.storage_lite_native.NativeFile.createWithIo(allocator, std.testing.io, path, .{ .resource_manager = manager, .no_sync = true });
    var live = true;
    defer if (live) file.close();
    try std.testing.expect(file.usesBorrowedIo() and file.owned_io == null);
    manager.releaseOwned(allocator);
    held = false;
    try std.testing.expect((try file.file.stat(file.runtime())).size > 0);
    file.close();
    live = false;
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    // The caller authority still works after the file releases its resources.
    _ = try std.Io.Dir.cwd().statFile(std.testing.io, path, .{});
}

test "SQL shared worker Lite file descriptor and metadata remain live through TLS retirement" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Native = local.storage_lite_native.NativeFile;
    const Probe = struct {
        key: std.c.pthread_key_t,
        file: *Native,
        expected_path: []const u8,
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(false),
        valid: std.atomic.Value(bool) = .init(false),
        done: std.Io.Event = .unset,
        fn destructor(raw: ?*anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
            const file_valid = if (self.file.file.stat(std.Io.Threaded.global_single_threaded.io())) |_| true else |_| false;
            self.valid.store(file_valid and std.mem.eql(u8, self.expected_path, self.file.path), .release);
        }
        fn work(self: *@This()) void {
            std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
        }
        fn close(self: *@This()) void {
            self.file.close();
            self.done.set(std.testing.io);
        }
    };
    var key: std.c.pthread_key_t = undefined;
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destructor));
    defer _ = std.c.pthread_key_delete(key);
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = backing.allocator();
    const manager = try local.storage_resource_manager.SharedResourceManager.create(allocator, .{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 }, .identity_allocator = allocator });
    var held = true;
    defer if (held) manager.releaseOwned(allocator);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/tls.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var file = try Native.createWithOptions(allocator, path, .{ .resource_manager = manager, .no_sync = true });
    var live = true;
    defer if (live) file.close();
    var probe: Probe = .{ .key = key, .file = &file, .expected_path = path };
    const io = file.runtime();
    var work = try io.concurrent(Probe.work, .{&probe});
    work.await(io);
    manager.releaseOwned(allocator);
    held = false;
    var closing: ?std.Thread = null;
    defer {
        probe.go.store(true, .release);
        if (closing) |thread| {
            thread.join();
            live = false;
        }
    }
    closing = try std.Thread.spawn(.{}, Probe.close, .{&probe});
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.entered.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(!probe.done.isSet());
    try std.testing.expect(manager.snapshot().memory.used_bytes > 0);
    probe.go.store(true, .release);
    closing.?.join();
    closing = null;
    live = false;
    try std.testing.expect(probe.valid.load(.acquire));
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
}

test "SQL shared worker Lite path lock keeps its executor and budget stable across moves" {
    const Native = local.storage_lite_native;
    const Resources = local.storage_resource_manager;
    var root = Resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
    defer root.deinit(std.testing.allocator);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/lock.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "lock.aflite", .data = "" });
    var first = try Native.lockWriterPathWithResources(std.testing.allocator, path, &root);
    const io = first.io_impl.io();
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var task = try io.concurrent(Work.run, .{});
    var moved = first;
    first = undefined;
    var live = true;
    defer if (live) moved.close();
    try std.testing.expectEqual(io.userdata, moved.io_impl.io().userdata);
    try std.testing.expectEqual(@as(u32, 42), task.await(io));
    try std.testing.expect(moved.owned_io.?.memory.?.stackStats().used > 0);
    if (Native.lockWriterPathWithIo(std.testing.allocator, std.testing.io, path)) |lease| {
        var unexpected = lease;
        unexpected.close();
        return error.TestUnexpectedResult;
    } else |err| try std.testing.expect(err == error.WouldBlock or err == error.FileBusy);
    moved.close();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    var borrowed = try Native.lockWriterPathWithIo(std.testing.allocator, std.testing.io, path);
    borrowed.close();
    _ = try std.Io.Dir.cwd().statFile(std.testing.io, path, .{});
}

test "SQL shared worker native control floor admits a cold full lane under exhausted node memory" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const a = std.testing.allocator;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1536 << 20 } });
    defer root.deinit(a);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort() });
    var runtime_live = true;
    defer if (runtime_live) runtime.deinit();
    const held = (1536 << 20) - root.snapshot().memory.used_bytes;
    try std.testing.expect(root.reserveExternal(held));
    defer root.releaseExternal(held);
    // First control publication, task metadata and every configured worker map
    // must all work after ordinary allocation authority has been exhausted.
    try std.testing.expectError(error.BackendRuntimeUnavailable, runtime.ptr().acquireApiLane());
    var control = try runtime.ptr().acquireControlLane();
    var control_live = true;
    defer if (control_live) control.release();
    try std.testing.expectEqual(@as(u32, 8), control.concurrentCapacity());
    const Probe = struct {
        entered: std.atomic.Value(usize) = .init(0),
        go: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            _ = self.entered.fetchAdd(1, .acq_rel);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
        }
    };
    var probe: Probe = .{};
    var tasks: [8]std.Io.Future(void) = undefined;
    var started: usize = 0;
    defer {
        probe.go.store(true, .release);
        for (tasks[0..started]) |*task| task.await(control.io());
    }
    for (&tasks) |*task| {
        task.* = try control.io().concurrent(Probe.run, .{&probe});
        started += 1;
    }
    while (probe.entered.load(.acquire) != tasks.len) std.Thread.yield() catch {};
    const stats = runtime.ptr().runtimeMemoryStats().?;
    const mapping = try std.Io.Threaded.workerStackMappingSize((std.Io.Threaded.InitOptions{}).stack_size);
    // Dedicated recovery workers use the same prepaid control stack account.
    const recovery_workers = @import("antfly_runtime_fs").threaded_io_limits.backend_runtime_recovery;
    try std.testing.expectEqual(mapping * (tasks.len + recovery_workers), stats.control_stacks.used);
    try std.testing.expect(stats.control_heap.used > 0);
    // Preserve the existing aggregate metric meanings across the partition.
    try std.testing.expect(stats.heap.used >= stats.control_heap.used);
    try std.testing.expect(stats.stacks.used >= stats.control_stacks.used);
    try std.testing.expectEqual(@as(u64, 1536 << 20), root.snapshot().memory.used_bytes);
    probe.go.store(true, .release);
    for (tasks[0..started]) |*task| task.await(control.io());
    started = 0;
    control.release();
    control_live = false;
    runtime.deinit();
    runtime_live = false;
    try std.testing.expectEqual(held, root.snapshot().memory.used_bytes);
}

test "SQL shared worker native control heap cannot borrow ordinary memory and recovers after release" {
    const a = std.testing.allocator;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1536 << 20 } });
    defer root.deinit(a);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort() });
    defer runtime.deinit();
    var control = try runtime.ptr().acquireControlLane();
    defer control.release();
    const memory = runtime.ptr().runtime_memory.?;
    const free = (1 << 20) - memory.controlHeapStats().used;
    const bytes = try memory.controlAllocator().alloc(u8, free);
    var live = true;
    defer if (live) memory.controlAllocator().free(bytes);
    const before = root.snapshot().memory.used_bytes;
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    try std.testing.expectError(error.ConcurrencyUnavailable, control.io().concurrent(Work.run, .{}));
    try std.testing.expectEqual(before, root.snapshot().memory.used_bytes);
    try std.testing.expect(memory.controlHeapStats().rejected > 0);
    memory.controlAllocator().free(bytes);
    live = false;
    var task = try control.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), task.await(control.io()));
}

test "SQL shared worker native control reservation survives physical TLS retirement" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Handle = local.storage_background_runtime.BackendRuntimeHandle;
    const Probe = struct {
        key: std.c.pthread_key_t,
        entered: std.atomic.Value(bool) = .init(false),
        go: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        fn destructor(raw: ?*anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered.store(true, .release);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
        }
        fn work(self: *@This()) void {
            std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
        }
        fn close(handle: *Handle, self: *@This()) void {
            handle.deinit();
            self.done.store(true, .release);
        }
    };
    var key: std.c.pthread_key_t = undefined;
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destructor));
    defer _ = std.c.pthread_key_delete(key);
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1536 << 20 } });
    defer root.deinit(std.testing.allocator);
    var runtime = try Handle.init(std.testing.allocator, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort() });
    var live = true;
    defer if (live) runtime.deinit();
    const reserved = runtime.ptr().runtimeMemoryStats().?.control_reserved_bytes;
    var lane = try runtime.ptr().acquireControlLane();
    var lane_live = true;
    defer if (lane_live) lane.release();
    var probe: Probe = .{ .key = key };
    var task = try lane.io().concurrent(Probe.work, .{&probe});
    task.await(lane.io());
    lane.release();
    lane_live = false;
    var closing = try std.Thread.spawn(.{}, Probe.close, .{ &runtime, &probe });
    live = false;
    defer {
        probe.go.store(true, .release);
        closing.join();
    }
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.entered.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(!probe.done.load(.acquire));
    try std.testing.expect(root.snapshot().memory.used_bytes >= reserved);
    probe.go.store(true, .release);
    while (!probe.done.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "SQL shared worker native Raft floors admit both cold full lanes under exhausted node memory" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const a = std.testing.allocator;
    const limit = 1536 << 20;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = limit } });
    defer root.deinit(a);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort() });
    var live = true;
    defer if (live) runtime.deinit();
    const held = limit - root.snapshot().memory.used_bytes;
    try std.testing.expect(root.reserveExternal(held));
    defer root.releaseExternal(held);
    try std.testing.expectError(error.BackendRuntimeUnavailable, runtime.ptr().acquireApiLane());
    const memory = runtime.ptr().runtime_memory.?;
    // Refusing one private direction cannot spend the other's startup credit.
    const occupied = try memory.raftAllocator(.inbound).alloc(u8, 1 << 20);
    try std.testing.expect(runtime.ptr().raftInboundIoImpl() == null);
    const outbound = runtime.ptr().raftOutboundIoImpl().?;
    memory.raftAllocator(.inbound).free(occupied);
    const inbound = runtime.ptr().raftInboundIoImpl().?;
    try std.testing.expectEqual(std.Io.Limit.limited(32), inbound.concurrent_limit);
    try std.testing.expectEqual(std.Io.Limit.limited(32), outbound.concurrent_limit);
    const Probe = struct {
        entered: std.atomic.Value(usize) = .init(0),
        go: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            _ = self.entered.fetchAdd(1, .acq_rel);
            while (!self.go.load(.acquire)) std.Thread.yield() catch {};
        }
    };
    var probe: Probe = .{};
    var tasks: [2][32]std.Io.Future(void) = undefined;
    var started: [2]usize = .{ 0, 0 };
    const ios = [_]std.Io{ inbound.io(), outbound.io() };
    defer {
        probe.go.store(true, .release);
        for (&tasks, started, ios) |*lane_tasks, count, io| for (lane_tasks[0..count]) |*task| task.await(io);
    }
    for (&tasks, &started, ios) |*lane_tasks, *count, io| for (lane_tasks) |*task| {
        task.* = try io.concurrent(Probe.run, .{&probe});
        count.* += 1;
    };
    while (probe.entered.load(.acquire) != 64) std.Thread.yield() catch {};
    const stats = runtime.ptr().runtimeMemoryStats().?;
    const mapping = try std.Io.Threaded.workerStackMappingSize((std.Io.Threaded.InitOptions{}).stack_size);
    try std.testing.expectEqual(32 * mapping, stats.raft_inbound.stacks.used);
    try std.testing.expectEqual(32 * mapping, stats.raft_outbound.stacks.used);
    try std.testing.expect(stats.raft_inbound.heap.used > 0 and stats.raft_outbound.heap.used > 0);
    try std.testing.expect(stats.stacks.used >= 64 * mapping);
    try std.testing.expectEqual(@as(u64, limit), root.snapshot().memory.used_bytes);
    probe.go.store(true, .release);
    for (&tasks, &started, ios) |*lane_tasks, *count, io| {
        for (lane_tasks[0..count.*]) |*task| task.await(io);
        count.* = 0;
    }
    runtime.deinit();
    live = false;
    try std.testing.expectEqual(held, root.snapshot().memory.used_bytes);
}

test "SQL shared worker native Raft heaps refuse independently without ordinary memory fallback" {
    const a = std.testing.allocator;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1536 << 20 } });
    defer root.deinit(a);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort() });
    defer runtime.deinit();
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    const memory = runtime.ptr().runtime_memory.?;
    const ios = [_]std.Io{ runtime.ptr().raftInboundIo().?, runtime.ptr().raftOutboundIo().? };
    inline for (.{ .inbound, .outbound }, 0..) |direction, index| {
        const allocator = memory.raftAllocator(direction);
        const bytes = try allocator.alloc(u8, (1 << 20) - memory.raftStats(direction).heap.used);
        var held = true;
        defer if (held) allocator.free(bytes);
        const before = root.snapshot().memory.used_bytes;
        try std.testing.expectError(error.ConcurrencyUnavailable, ios[index].concurrent(Work.run, .{}));
        try std.testing.expectEqual(before, root.snapshot().memory.used_bytes);
        try std.testing.expect(memory.raftStats(direction).heap.rejected > 0);
        var control = try runtime.ptr().acquireControlLane();
        defer control.release();
        var control_task = try control.io().concurrent(Work.run, .{});
        try std.testing.expectEqual(@as(u32, 42), control_task.await(control.io()));
        allocator.free(bytes);
        held = false;
        var retry = try ios[index].concurrent(Work.run, .{});
        try std.testing.expectEqual(@as(u32, 42), retry.await(ios[index]));
    }
}

test "SQL shared worker native Raft reservations survive physical TLS retirement" {
    inline for (.{ .inbound, .outbound }) |direction| {
        if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
        const Handle = local.storage_background_runtime.BackendRuntimeHandle;
        const Probe = struct {
            key: std.c.pthread_key_t,
            entered: std.atomic.Value(bool) = .init(false),
            go: std.atomic.Value(bool) = .init(false),
            done: std.atomic.Value(bool) = .init(false),
            fn destructor(raw: ?*anyopaque) callconv(.c) void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.entered.store(true, .release);
                while (!self.go.load(.acquire)) std.Thread.yield() catch {};
            }
            fn work(self: *@This()) void {
                std.debug.assert(std.c.pthread_setspecific(self.key, self) == 0);
            }
            fn close(handle: *Handle, self: *@This()) void {
                handle.deinit();
                self.done.store(true, .release);
            }
        };
        var key: std.c.pthread_key_t = undefined;
        try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_key_create(&key, Probe.destructor));
        defer _ = std.c.pthread_key_delete(key);
        var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1536 << 20 } });
        defer root.deinit(std.testing.allocator);
        var runtime = try Handle.init(std.testing.allocator, .{ .backend = .io_threaded, .memory_parent = root.memoryParentPort() });
        var live = true;
        defer if (live) runtime.deinit();
        const stats = runtime.ptr().runtimeMemoryStats().?;
        const reserved = stats.control_reserved_bytes + stats.raft_inbound.reserved_bytes + stats.raft_outbound.reserved_bytes;
        const io = if (direction == .inbound) runtime.ptr().raftInboundIo().? else runtime.ptr().raftOutboundIo().?;
        var probe: Probe = .{ .key = key };
        var task = try io.concurrent(Probe.work, .{&probe});
        task.await(io);
        var closing = try std.Thread.spawn(.{}, Probe.close, .{ &runtime, &probe });
        live = false;
        defer {
            probe.go.store(true, .release);
            closing.join();
        }
        const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (!probe.entered.load(.acquire)) {
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
            std.Thread.yield() catch {};
        }
        try std.testing.expect(!probe.done.load(.acquire));
        try std.testing.expect(root.snapshot().memory.used_bytes >= reserved);
        probe.go.store(true, .release);
        while (!probe.done.load(.acquire)) std.Thread.yield() catch {};
        try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    }
}

test "SQL shared worker native Raft startup floor refuses before physical owner allocation" {
    const a = std.testing.allocator;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 << 20 } });
    defer root.deinit(a);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.ResourceBudgetExceeded, local.storage_background_runtime.BackendRuntime.init(failing.allocator(), .{ .memory_parent = root.memoryParentPort() }));
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    try std.testing.expectError(error.InvalidRaftMemoryLimit, local.storage_background_runtime.BackendRuntime.init(failing.allocator(), .{ .memory_parent = root.memoryParentPort(), .raft_memory_bytes = 0 }));
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}
