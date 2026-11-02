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

const std = @import("std");
const memory = @import("runtime/kv/device_memory.zig");
const buffers = @import("ops/cuda/buffer.zig");
const context = @import("ops/cuda/context.zig");
const driver = @import("ops/cuda/driver.zig");
extern fn antfly_test_expect_error_logs(usize) callconv(.c) void;

const Probe = struct {
    used: usize = 0,
    limit: usize = 100,
    references: usize = 0,
    records: [16]Record = @splat(.{}),
    const Record = struct {
        owner: *Probe = undefined,
        bytes: usize = 0,
        active: bool = false,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active);
            self.owner.used -= self.bytes;
            self.owner.references -= 1;
            self.active = false;
        }
    };
    fn retain(raw: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.references += 1;
        return true;
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.references -= 1;
    }
    fn acquire(raw: *anyopaque, _: std.mem.Allocator, bytes: usize) !memory.Credit {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (bytes > self.limit - self.used) return error.ResourceTemporarilyUnavailable;
        for (&self.records) |*record| if (!record.active) {
            record.* = .{ .owner = self, .bytes = bytes, .active = true };
            self.used += bytes;
            self.references += 1;
            return .{ .context = record, .release_fn = Record.release };
        };
        return error.ResourceTemporarilyUnavailable;
    }
    fn port(self: *@This()) !memory.Budget {
        const result: memory.Budget = .{ .context = self, .allocator = std.testing.allocator, .retain_context = retain, .release_context = release, .acquire = acquire };
        return result.clone();
    }
};

const Fake = struct {
    var allocations: usize = 0;
    var frees: usize = 0;
    var fail_alloc = false;
    var fail_free = false;
    var fail_current = false;
    fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
        return if (fail_current) 1 else 0;
    }
    fn alloc(out: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
        allocations += 1;
        if (fail_alloc) return 1;
        out.* = @intCast(allocations);
        return 0;
    }
    fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
        frees += 1;
        return if (fail_free) 1 else 0;
    }
    fn errorText(_: driver.CUresult, out: *?[*:0]const u8) callconv(.c) driver.CUresult {
        out.* = "injected";
        return 0;
    }
    fn init() context.CudaContext {
        allocations = 0;
        frees = 0;
        fail_alloc = false;
        fail_free = false;
        fail_current = false;
        var table: driver.CudaDriver.Table = undefined;
        table.cuCtxSetCurrent = current;
        table.cuMemAlloc = alloc;
        table.cuMemFree = free;
        table.cuGetErrorName = errorText;
        table.cuGetErrorString = errorText;
        return .{ .driver = .{ .lib = undefined, .fns = table }, .device = 0, .ctx = @ptrFromInt(1), .stream = null, .info = .{} };
    }
};

test "CUDA physical cache credit admits growth peak and survives capability and queue transfer" {
    var probe: Probe = .{ .used = 40 };
    var ctx = Fake.init();
    const budget = try probe.port();
    var old = try buffers.DeviceBuffer.allocAdmitted(&ctx, 32, budget);
    try std.testing.expectEqual(@as(usize, 72), probe.used);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, buffers.DeviceBuffer.allocAdmitted(&ctx, 40, budget));
    try std.testing.expectEqual(@as(usize, 1), Fake.allocations);
    // Transfer exactly as the reuse/deferred-free queues do: ownership moves
    // with the full allocation descriptor; logical length cannot refund bytes.
    var queue: [1]buffers.DeviceBuffer = .{old};
    old = .{};
    queue[0].len = 1;
    budget.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.references);
    try std.testing.expectEqual(@as(usize, 72), probe.used);
    queue[0].free(&ctx);
    try std.testing.expectEqual(@as(usize, 40), probe.used);
    try std.testing.expectEqual(@as(usize, 0), probe.references);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
}

test "CUDA physical cache admission rolls back driver context and local-capacity failures" {
    antfly_test_expect_error_logs(2);
    var probe: Probe = .{};
    var ctx = Fake.init();
    const budget = try probe.port();
    defer budget.deinit();
    Fake.fail_alloc = true;
    try std.testing.expectError(error.CudaDriverError, buffers.DeviceBuffer.allocAdmitted(&ctx, 16, budget));
    Fake.fail_alloc = false;
    Fake.fail_current = true;
    try std.testing.expectError(error.CudaDriverError, buffers.DeviceBuffer.allocAdmitted(&ctx, 16, budget));
    Fake.fail_current = false;
    try ctx.device_allocations.setLimit(8);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, buffers.DeviceBuffer.allocAdmitted(&ctx, 16, budget));
    try std.testing.expectEqual(@as(usize, 1), Fake.allocations);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), probe.used);
    try std.testing.expectEqual(@as(usize, 1), probe.references);
    const empty = try buffers.DeviceBuffer.allocAdmitted(&ctx, 0, budget);
    try std.testing.expect(empty.device_credit == null);
}

test "CUDA failed physical retirement preserves shared credit and descriptor for retry" {
    antfly_test_expect_error_logs(2);
    var probe: Probe = .{};
    var ctx = Fake.init();
    const budget = try probe.port();
    defer budget.deinit();
    var buffer = try buffers.DeviceBuffer.allocAdmitted(&ctx, 16, budget);
    Fake.fail_free = true;
    buffer.free(&ctx);
    try std.testing.expect(buffer.ptr != 0 and buffer.device_credit != null);
    Fake.fail_free = false;
    Fake.fail_current = true;
    buffer.free(&ctx);
    try std.testing.expectEqual(@as(usize, 16), probe.used);
    try std.testing.expectEqual(@as(usize, 16), ctx.device_allocations.snapshot().live);
    try std.testing.expectEqual(@as(usize, 1), Fake.frees);
    Fake.fail_current = false;
    buffer.free(&ctx);
    try std.testing.expectEqual(@as(usize, 0), probe.used);
    try std.testing.expect(buffer.ptr == 0 and buffer.device_credit == null);
    buffer.free(&ctx);
    try std.testing.expectEqual(@as(usize, 2), Fake.frees);
}

test "CUDA physical borrowed views retain both allocation and shared admission owners" {
    var probe: Probe = .{};
    var ctx = Fake.init();
    const budget = try probe.port();
    defer budget.deinit();
    var owner = try buffers.DeviceBuffer.allocAdmitted(&ctx, 32, budget);
    defer owner.free(&ctx);
    // Graph suballocations borrow the allocation. Even a copied credit token
    // cannot refund its owner while the backing allocation is still live.
    var view = owner;
    view.borrowed = true;
    view.len = 8;
    view.free(&ctx);
    view.free(&ctx);
    try std.testing.expectEqual(@as(usize, 0), Fake.frees);
    try std.testing.expectEqual(@as(usize, 32), probe.used);
    try std.testing.expectEqual(@as(usize, 32), ctx.device_allocations.snapshot().live);
    owner.len = 1;
    owner.free(&ctx);
    try std.testing.expectEqual(@as(usize, 1), Fake.frees);
    try std.testing.expectEqual(@as(usize, 0), probe.used);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
}
