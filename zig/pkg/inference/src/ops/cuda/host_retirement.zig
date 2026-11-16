// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Durable ownership of mapped, pinned host allocations. Metadata, an exact
//! primary-context retain, a driver-library reference and admission are owned
//! before allocation. Failed retirement survives construction/session teardown.
const std = @import("std");
const driver = @import("driver.zig");
const memory = @import("../../runtime/tier/memory.zig");

pub var process: Queue = .{ .allocator = std.heap.smp_allocator };

pub const Owner = struct {
    queue: *Queue,
    next: ?*Owner = null,
    driver: driver.CudaDriver,
    owns_library: bool = true,
    primary: driver.CUcontext,
    device: driver.CUdevice,
    ptr: ?*anyopaque = null,
    bytes: usize,
    readers_done: bool = false,
    lease: ?memory.AdmissionLease = null,

    pub fn create(primary: driver.CUcontext, device: driver.CUdevice, bytes: usize, lease: ?*?memory.AdmissionLease) !*Owner {
        process.retry();
        if (lease) |slot| if (slot.*) |held| {
            if (held.amounts.host_weight_bytes != bytes) return error.InvalidAdmissionLeaseReduction;
        };
        const owner = init: {
            var owned_driver = try driver.CudaDriver.open();
            errdefer owned_driver.deinit();
            if (owned_driver.fns.cuCtxGetCurrent == null or owned_driver.fns.cuCtxSynchronize == null or owned_driver.fns.cuMemFreeHost == null) return error.CudaSymbolMissing;
            const owner = try process.allocator.create(Owner);
            errdefer process.allocator.destroy(owner);
            var retained: driver.CUcontext = null;
            try owned_driver.check(owned_driver.fns.cuDevicePrimaryCtxRetain(&retained, device));
            owner.* = .{ .queue = &process, .driver = owned_driver, .primary = retained, .device = device, .bytes = bytes, .readers_done = true };
            break :init owner;
        };
        // The caller still holds its exact primary lifetime. Never publish an
        // alias in a different lifetime, even if an external reset intervened.
        if (owner.primary != primary) {
            owner.retire(true);
            return error.InvalidCudaState;
        }
        if (lease) |slot| {
            owner.lease = slot.*;
            slot.* = null;
        }
        return owner;
    }

    pub fn freeHost(self: *Owner) driver.Error!void {
        if (self.ptr == null) return;
        var previous: driver.CUcontext = null;
        if (self.driver.fns.cuCtxGetCurrent.?(&previous) != driver.CUDA_SUCCESS) return error.CudaDriverError;
        defer _ = self.driver.fns.cuCtxSetCurrent(previous);
        if (self.driver.fns.cuCtxSetCurrent(self.primary) != driver.CUDA_SUCCESS) return error.CudaDriverError;
        if (self.driver.fns.cuMemFreeHost.?(self.ptr) != driver.CUDA_SUCCESS) return error.CudaDriverError;
        self.ptr = null;
    }

    /// No fallible allocation on retirement, including failed import rollback.
    /// Once published, only a confirmed drain can authorize a host free.
    pub fn retire(self: *Owner, readers_done: bool) void {
        self.readers_done = readers_done;
        const queue = self.queue;
        // Driver waits and external-budget callbacks never run under the
        // queue's metadata lock. This owner is exclusively ours until enqueue.
        if (self.release()) {
            if (self.owns_library) self.driver.deinit();
            queue.allocator.destroy(self);
            return;
        }
        std.log.warn("CUDA mapped host retirement deferred: host_bytes={d} readers_done={} primary_retained=true", .{
            if (self.ptr != null) self.bytes else 0, self.readers_done,
        });
        queue.enqueue(self);
    }

    fn release(self: *Owner) bool {
        var previous: driver.CUcontext = null;
        if (self.driver.fns.cuCtxGetCurrent.?(&previous) != driver.CUDA_SUCCESS) return false;
        defer _ = self.driver.fns.cuCtxSetCurrent(previous);
        if (self.ptr != null) {
            if (!self.readers_done) {
                if (self.driver.fns.cuCtxSetCurrent(self.primary) != driver.CUDA_SUCCESS) return false;
                if (self.driver.fns.cuCtxSynchronize.?() != driver.CUDA_SUCCESS) return false;
                self.readers_done = true;
            }
            self.freeHost() catch return false;
        }
        // Host allocations are not reclaimed by primary-context teardown.
        // Keep admission through the actual cuMemFreeHost, never just release.
        if (self.lease) |*lease| lease.release();
        self.lease = null;
        if (self.driver.fns.cuDevicePrimaryCtxRelease(self.device) != driver.CUDA_SUCCESS) return false;
        return true;
    }
};

pub const Queue = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    head: ?*Owner = null,
    pending: std.atomic.Value(bool) = .init(false),

    pub fn hasPending(self: *const Queue) bool {
        return self.pending.load(.acquire);
    }

    fn lock(self: *Queue) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn retry(self: *Queue) void {
        _ = self.reclaim();
    }

    /// Physical host bytes or their still-held admission credit actually
    /// released by this pass. A failed primary release alone is not progress
    /// against host pressure. Callers can re-probe the authoritative budget.
    fn enqueue(self: *Queue, owner: *Owner) void {
        self.lock();
        defer self.mutex.unlock();
        owner.next = self.head;
        self.head = owner;
        self.pending.store(true, .release);
    }

    pub fn reclaim(self: *Queue) usize {
        if (!self.hasPending()) return 0;
        self.lock();
        var remaining = self.head;
        self.head = null;
        self.pending.store(false, .release);
        self.mutex.unlock();
        var released_bytes: usize = 0;
        while (remaining) |owner| {
            const next = owner.next;
            owner.next = null;
            const held_host = owner.ptr != null or owner.lease != null;
            const retired = owner.release();
            if (held_host and owner.ptr == null and owner.lease == null)
                released_bytes +|= owner.bytes;
            if (retired) {
                if (owner.owns_library) owner.driver.deinit();
                self.allocator.destroy(owner);
            } else self.enqueue(owner);
            remaining = next;
        }
        return released_bytes;
    }
};

const Fake = struct {
    var current: driver.CUcontext = null;
    var fail_wait: bool = false;
    var fail_free: bool = false;
    var fail_release: bool = false;
    var waits: usize = 0;
    var frees: usize = 0;
    var releases: usize = 0;
    var checked_queue: ?*Queue = null;
    var driver_lock_free: bool = true;

    fn getCurrent(ptr: *driver.CUcontext) callconv(.c) driver.CUresult {
        ptr.* = current;
        return driver.CUDA_SUCCESS;
    }
    fn setCurrent(ptr: driver.CUcontext) callconv(.c) driver.CUresult {
        current = ptr;
        return driver.CUDA_SUCCESS;
    }
    fn wait() callconv(.c) driver.CUresult {
        waits += 1;
        return if (fail_wait) 1 else driver.CUDA_SUCCESS;
    }
    fn streamWait(_: driver.CUstream) callconv(.c) driver.CUresult {
        return wait();
    }
    fn free(_: ?*anyopaque) callconv(.c) driver.CUresult {
        frees += 1;
        if (checked_queue) |queue| {
            const unlocked = queue.mutex.tryLock();
            driver_lock_free = driver_lock_free and unlocked;
            if (unlocked) queue.mutex.unlock();
        }
        return if (fail_free) 1 else driver.CUDA_SUCCESS;
    }
    fn release(_: driver.CUdevice) callconv(.c) driver.CUresult {
        releases += 1;
        return if (fail_release) 1 else driver.CUDA_SUCCESS;
    }
    fn owner(queue: *Queue, lease: memory.AdmissionLease) !*Owner {
        current = @ptrFromInt(0xbeef);
        fail_wait = false;
        fail_free = false;
        fail_release = false;
        waits = 0;
        frees = 0;
        releases = 0;
        checked_queue = null;
        driver_lock_free = true;
        const result = try queue.allocator.create(Owner);
        result.* = .{
            .queue = queue,
            .driver = undefined,
            .owns_library = false,
            .primary = @ptrFromInt(1),
            .device = 0,
            .ptr = @ptrFromInt(0x1234),
            .bytes = 64,
            .lease = lease,
        };
        result.driver.fns.cuCtxGetCurrent = getCurrent;
        result.driver.fns.cuCtxSetCurrent = setCurrent;
        result.driver.fns.cuCtxSynchronize = wait;
        result.driver.fns.cuStreamSynchronize = streamWait;
        result.driver.fns.cuMemFreeHost = free;
        result.driver.fns.cuDevicePrimaryCtxRelease = release;
        return result;
    }
};

test "Gemma4 CUDA host PLE retirement survives wrapper teardown and failed drain/free" {
    var controller = memory.AdmissionController{};
    var model = try controller.tryAcquire(.gpu, .{}, .{ .backend_weight_bytes = 128 }, false);
    var queue = Queue{ .allocator = std.testing.allocator };
    const owner = try Fake.owner(&queue, try controller.tryAcquire(.gpu, .{}, .{ .host_weight_bytes = 64 }, false));
    var mapped = @import("buffer.zig").MappedHostBuffer{
        .host = .{ .ptr = owner.ptr, .len = 64 },
        .device_ptr = 0x5678,
        .owner = owner,
    };
    var ctx: @import("context.zig").CudaContext = undefined;
    ctx.ctx = owner.primary;
    ctx.stream = null;
    ctx.driver.fns.cuCtxSetCurrent = Fake.setCurrent;
    ctx.driver.fns.cuStreamSynchronize = Fake.streamWait;
    Fake.fail_wait = true;
    mapped.retire(&ctx); // Production deinit entry point; wrapper can disappear.
    model.release();
    try std.testing.expect(mapped.host.ptr == null and mapped.owner == null);
    try std.testing.expect(queue.head != null);
    try std.testing.expectEqual(@as(usize, 0), Fake.frees);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_weight_bytes);
    Fake.current = @ptrFromInt(0xbeef);
    Fake.fail_wait = false;
    Fake.fail_free = true;
    queue.retry();
    try std.testing.expect(queue.head.?.readers_done);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), Fake.releases);
    const waits = Fake.waits;
    Fake.fail_free = false;
    queue.retry();
    try std.testing.expect(queue.head == null);
    try std.testing.expectEqual(waits, Fake.waits); // A confirmed drain is durable.
    try std.testing.expectEqual(@as(usize, 1), Fake.releases);
    try std.testing.expectEqual(@as(usize, 0xbeef), @intFromPtr(Fake.current));
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "Gemma4 CUDA host PLE failed construction rollback retains admission until physical free" {
    var controller = memory.AdmissionController{};
    var queue = Queue{ .allocator = std.testing.allocator };
    const owner = try Fake.owner(&queue, try controller.tryAcquire(.gpu, .{}, .{ .host_weight_bytes = 64 }, false));
    Fake.fail_free = true;
    owner.retire(true); // Mapping/import failed before publishing a GPU reader.
    try std.testing.expectEqual(@as(usize, 0), Fake.waits);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_weight_bytes);
    Fake.fail_free = false;
    Fake.fail_release = true;
    queue.retry();
    try std.testing.expect(queue.head != null); // Exact primary/library still held.
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    const frees = Fake.frees;
    Fake.fail_release = false;
    queue.retry();
    queue.retry();
    try std.testing.expect(queue.head == null);
    try std.testing.expectEqual(frees, Fake.frees); // No double free on retain retry.
    try std.testing.expectEqual(@as(usize, 0xbeef), @intFromPtr(Fake.current));
}

test "Gemma4 CUDA host PLE retirement does not hold queue metadata across driver callbacks" {
    var controller = memory.AdmissionController{};
    var queue = Queue{ .allocator = std.testing.allocator };
    const owner = try Fake.owner(&queue, try controller.tryAcquire(.gpu, .{}, .{ .host_weight_bytes = 64 }, false));
    Fake.checked_queue = &queue;
    Fake.fail_free = true;
    owner.retire(true);
    try std.testing.expect(queue.hasPending());
    Fake.fail_free = false;
    Fake.fail_release = true;
    try std.testing.expectEqual(@as(usize, 64), queue.reclaim());
    try std.testing.expect(queue.hasPending()); // Only the primary retain remains.
    try std.testing.expectEqual(@as(usize, 0), queue.reclaim());
    Fake.fail_release = false;
    try std.testing.expectEqual(@as(usize, 0), queue.reclaim());
    try std.testing.expect(!queue.hasPending());
    try std.testing.expect(Fake.driver_lock_free);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}
