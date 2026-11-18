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
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");
const transfer_audit = @import("../../execution/transfer_audit.zig");
const allocation_budget = @import("allocation_budget.zig");
const host_retirement = @import("host_retirement.zig");
const memory = @import("../../runtime/tier/memory.zig");
const allocation_ledger = @import("allocation_ledger.zig");

pub const HostBuffer = struct {
    ptr: ?*anyopaque = null,
    len: usize = 0,

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!HostBuffer {
        if (len == 0) return .{};
        const alloc_host = ctx.driver.fns.cuMemAllocHost orelse return error.CudaSymbolMissing;
        try ctx.makeCurrent();
        var ptr: ?*anyopaque = null;
        const result = alloc_host(&ptr, len);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.warn("CUDA pinned-host allocation unavailable: bytes={d} error={s} ({s})", .{
                len,
                ctx.driver.errorName(result),
                ctx.driver.errorString(result),
            });
            return error.CudaHostAllocationFailed;
        }
        return .{ .ptr = ptr, .len = len };
    }

    /// Admitted owners must keep both their pointer and reservation when the
    /// driver cannot confirm a physical free. The ordinary compatibility API
    /// below remains unchanged for existing unadmitted callers.
    pub fn freeChecked(self: *HostBuffer, ctx: *context_mod.CudaContext) driver_mod.Error!void {
        if (self.ptr) |ptr| {
            try ctx.makeCurrent();
            const free_host = ctx.driver.fns.cuMemFreeHost orelse return error.CudaSymbolMissing;
            if (free_host(ptr) != driver_mod.CUDA_SUCCESS) return error.CudaDriverError;
            self.* = .{};
        }
    }

    pub fn free(self: *HostBuffer, ctx: *context_mod.CudaContext) void {
        if (self.ptr) |ptr| {
            ctx.makeCurrent() catch {};
            if (ctx.driver.fns.cuMemFreeHost) |free_host| {
                _ = free_host(ptr);
            }
            self.ptr = null;
            self.len = 0;
        }
    }

    pub fn bytes(self: HostBuffer) []u8 {
        if (self.ptr == null or self.len == 0) return &.{};
        const ptr: [*]u8 = @ptrCast(self.ptr.?);
        return ptr[0..self.len];
    }

    pub fn constBytes(self: HostBuffer) []const u8 {
        return self.bytes();
    }
};

/// Immutable checkpoint storage accessible to device-side gathers without a
/// full VRAM copy. The owner outlives every resident descriptor and graph.
/// Host bytes have an independent load lease, never the device scratch pool.
pub const MappedHostBuffer = struct {
    host: HostBuffer = .{},
    device_ptr: driver_mod.CUdeviceptr = 0,

    owner: ?*host_retirement.Owner = null,

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) !MappedHostBuffer {
        return allocAdmitted(ctx, len, null);
    }

    pub fn allocAdmitted(ctx: *context_mod.CudaContext, len: usize, lease: ?*?memory.AdmissionLease) !MappedHostBuffer {
        if (len == 0) return error.InvalidCudaState;
        if (ctx.driver.fns.cuMemHostAlloc == null or ctx.driver.fns.cuMemHostGetDevicePointer == null) return error.CudaSymbolMissing;
        try ctx.makeCurrent();
        const owner = try host_retirement.Owner.create(ctx.ctx, ctx.device, len, lease);
        return allocUsingOwner(ctx, len, owner);
    }

    fn allocUsingOwner(ctx: *context_mod.CudaContext, len: usize, owner: *host_retirement.Owner) !MappedHostBuffer {
        errdefer owner.retire(true); // No device reader has seen this allocation.
        const alloc_host = ctx.driver.fns.cuMemHostAlloc orelse return error.CudaSymbolMissing;
        const device_pointer = ctx.driver.fns.cuMemHostGetDevicePointer orelse return error.CudaSymbolMissing;
        // CU_MEMHOSTALLOC_DEVICEMAP; preserve ordinary cached CPU stores.
        if (alloc_host(&owner.ptr, len, 0x02) != driver_mod.CUDA_SUCCESS) return error.CudaHostAllocationFailed;
        if (owner.ptr == null) return error.InvalidCudaState;
        var device_ptr: driver_mod.CUdeviceptr = 0;
        if (device_pointer(&device_ptr, owner.ptr, 0) != driver_mod.CUDA_SUCCESS) return error.CudaDriverError;
        if (device_ptr == 0) return error.InvalidCudaState;
        if (owner.lease) |*held| try held.retain(held.amounts);
        return .{ .host = .{ .ptr = owner.ptr, .len = len }, .device_ptr = device_ptr, .owner = owner };
    }

    pub fn view(self: MappedHostBuffer) DeviceBuffer {
        return .{ .ptr = self.device_ptr, .len = self.host.len, .borrowed = true };
    }

    pub fn clear(self: *MappedHostBuffer, ctx: *context_mod.CudaContext) driver_mod.Error!void {
        if (self.host.ptr == null) return;
        // Readers may be asynchronous, including captured decode kernels.
        // A failed wait/free must leave ownership intact for a later retry.
        try ctx.synchronize();
        if (self.owner) |owner| {
            try owner.freeHost();
            owner.retire(true);
        } else try self.host.freeChecked(ctx);
        self.* = .{};
    }

    /// Transfer failed waits/frees to the process owner without dropping its
    /// primary retain, driver library, or independent host admission lease.
    pub fn retire(self: *MappedHostBuffer, ctx: *context_mod.CudaContext) void {
        const owner = self.owner orelse return;
        const drained = ctx.ctx != null and ctx.driver.fns.cuCtxSetCurrent(ctx.ctx) == driver_mod.CUDA_SUCCESS and
            ctx.driver.fns.cuStreamSynchronize(ctx.stream) == driver_mod.CUDA_SUCCESS;
        owner.retire(drained);
        self.* = .{};
    }
};

pub const DeviceBuffer = struct {
    pub const Purpose = @import("allocation_budget.zig").OwnedPurpose;
    /// CUDA cuMemAlloc base guarantee, also used by canonical cuBLASLt profiles.
    /// Borrowed private KV slots preserve this base selection contract.
    pub const allocation_alignment: usize = 256;
    ptr: driver_mod.CUdeviceptr = 0,
    len: usize = 0,
    /// Original allocation size, independent of a logical view's length.
    /// Non-owning descriptors constructed from pointers have no charge.
    allocation_bytes: usize = 0,
    /// Immutable birth purpose travels with the movable ownership descriptor.
    /// Borrowed views never allocate/free; no post-publication re-tagging.
    allocation_purpose: @import("allocation_budget.zig").OwnedPurpose = .unclassified,
    allocation_token: ?allocation_ledger.Token = null,
    /// Suballocations of a graph arena cannot be freed through an op's direct
    /// error cleanup. The arena owner alone releases their physical storage.
    borrowed: bool = false,

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!DeviceBuffer {
        return allocWithPurpose(ctx, len, .unclassified);
    }
    pub fn allocWithPurpose(ctx: *context_mod.CudaContext, len: usize, purpose: @import("allocation_budget.zig").OwnedPurpose) driver_mod.Error!DeviceBuffer {
        if (len == 0) return .{};
        try ctx.makeCurrent();
        try ctx.device_allocations.reserve(len);
        errdefer ctx.device_allocations.release(len);
        const token = allocation_ledger.process.reserve(@intFromPtr(ctx.ctx), &ctx.device_allocations.owner_id, len) catch
            return error.CudaMemoryLimitExceeded;
        errdefer allocation_ledger.process.release(token);
        var ptr: driver_mod.CUdeviceptr = 0;
        const result = ctx.driver.fns.cuMemAlloc(&ptr, len);
        if (result != driver_mod.CUDA_SUCCESS) {
            // Physical exhaustion (including other resident GPU runtimes)
            // follows the same bounded retry path as our allocation quota.
            if (result == driver_mod.CUDA_ERROR_OUT_OF_MEMORY) {
                // Keep physical exhaustion distinguishable from our quota even
                // though both use the same bounded retry/error contract.
                std.log.warn("CUDA cuMemAlloc failed: bytes={d} error=CUDA_ERROR_OUT_OF_MEMORY", .{len});
                return error.CudaMemoryLimitExceeded;
            }
            std.log.err("CUDA cuMemAlloc failed: bytes={d} error={s} ({s})", .{ len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
        allocation_ledger.process.publish(token, ptr);
        ctx.device_allocations.allocatedWithPurpose(len, purpose);
        return .{ .ptr = ptr, .len = len, .allocation_bytes = len, .allocation_purpose = purpose, .allocation_token = token };
    }

    /// Retained owners must preserve both the address and its physical charge
    /// on driver failure. Legacy free keeps its existing best-effort contract.
    pub fn freeChecked(self: *DeviceBuffer, ctx: *context_mod.CudaContext) !usize {
        if (self.borrowed) {
            self.* = .{};
            return 0;
        }
        if (self.ptr == 0) return 0;
        if (self.allocation_token) |token| if (!allocation_ledger.process.isLive(token)) {
            self.* = .{};
            return 0;
        };
        try ctx.makeCurrent();
        const bytes = self.allocation_bytes;
        const result = ctx.driver.fns.cuMemFree(self.ptr);
        if (result != driver_mod.CUDA_SUCCESS) return error.CudaDriverError;
        ctx.device_allocations.freedWithPurpose(bytes, self.allocation_purpose);
        if (self.allocation_token) |token| allocation_ledger.process.release(token);
        self.* = .{};
        return bytes;
    }

    pub fn handoffLedgerOwner(self: *const DeviceBuffer, source: *const context_mod.CudaContext, destination: *const context_mod.CudaContext) !void {
        if (self.borrowed or self.allocation_token == null) return;
        if (source.ctx != destination.ctx) return error.InvalidCudaAllocationOwner;
        try allocation_ledger.process.handoff(&.{self.allocation_token.?}, source.device_allocations.owner_id, destination.device_allocations.owner_id, @intFromPtr(destination.ctx));
    }

    pub fn free(self: *DeviceBuffer, ctx: *context_mod.CudaContext) void {
        if (self.borrowed) {
            self.* = .{};
            return;
        }
        if (self.ptr != 0) {
            if (self.allocation_token) |token| if (!allocation_ledger.process.isLive(token)) {
                // Confirmed primary teardown already reclaimed these bytes.
                // Never free a successor allocation at a reused device address.
                self.* = .{};
                return;
            };
            const current = blk: {
                ctx.makeCurrent() catch break :blk false;
                break :blk true;
            };
            const result = if (current) ctx.driver.fns.cuMemFree(self.ptr) else @as(driver_mod.CUresult, 201);
            if (result == driver_mod.CUDA_SUCCESS) {
                ctx.device_allocations.freedWithPurpose(self.allocation_bytes, self.allocation_purpose);
                if (self.allocation_token) |token| allocation_ledger.process.release(token);
            } else {
                // The process ledger retains the pointer, context identity,
                // and charge after this descriptor retires. A later confirmed
                // free or primary-context teardown is required to release it.
                if (self.allocation_token) |token| allocation_ledger.process.failedFree(token);
                std.log.warn("CUDA cuMemFree failed: bytes={d} error={s}", .{ self.allocation_bytes, ctx.driver.errorName(result) });
            }
            self.ptr = 0;
            self.len = 0;
            self.allocation_bytes = 0;
            self.allocation_token = null;
        }
    }

    pub fn copyFromHost(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []const u8) driver_mod.Error!void {
        if (bytes.len > self.len) return error.InvalidCudaState;
        if (bytes.len == 0) return;
        try ctx.makeCurrent();
        const result = ctx.driver.fns.cuMemcpyHtoDAsync(self.ptr, bytes.ptr, bytes.len, ctx.stream);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemcpyHtoDAsync failed: bytes={d} error={s} ({s})", .{ bytes.len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
        transfer_audit.upload(bytes.len);
    }

    pub fn copyToHost(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []u8) (driver_mod.Error || error{DeviceReadbackForbidden})!void {
        return self.copyToHostWithKind(ctx, bytes, .activation);
    }

    pub fn copyIndexMetadataToHost(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []u8) (driver_mod.Error || error{DeviceReadbackForbidden})!void {
        return self.copyToHostWithKind(ctx, bytes, .index_metadata);
    }

    fn copyToHostWithKind(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []u8, kind: enum { activation, index_metadata }) (driver_mod.Error || error{DeviceReadbackForbidden})!void {
        if (bytes.len > self.len) return error.InvalidCudaState;
        if (bytes.len == 0) return;
        switch (kind) {
            .activation => try transfer_audit.checkDeviceHostAccess(),
            .index_metadata => try transfer_audit.checkIndexMetadataAccess(),
        }
        try ctx.makeCurrent();
        const result = ctx.driver.fns.cuMemcpyDtoHAsync(bytes.ptr, self.ptr, bytes.len, ctx.stream);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemcpyDtoHAsync failed: bytes={d} error={s} ({s})", .{ bytes.len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
        transfer_audit.download(bytes.len);
    }

    pub fn copyFromDevice(self: DeviceBuffer, ctx: *context_mod.CudaContext, src: DeviceBuffer, len: usize) driver_mod.Error!void {
        if (len > self.len or len > src.len) return error.InvalidCudaState;
        if (len == 0) return;
        try ctx.makeCurrent();
        const result = ctx.driver.fns.cuMemcpyDtoDAsync(self.ptr, src.ptr, len, ctx.stream);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemcpyDtoDAsync failed: bytes={d} error={s} ({s})", .{ len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
        transfer_audit.deviceCopy(len);
    }
};

/// Validate byte regions before driver access. Read-only regions may alias;
/// every nonempty output must be disjoint from inputs and earlier outputs.
/// Alignment is static ABI metadata; lengths describe actual used bytes.
pub inline fn validateRegions(comptime count: usize, regions: [count]DeviceBuffer, lengths: [count]usize, comptime alignment: [count]u64, comptime first_write: usize) !void {
    comptime std.debug.assert(first_write <= count);
    var ends: [count]u64 = undefined;
    inline for (regions, lengths, alignment, 0..) |region, length, align_bytes, i| {
        if (length != 0 and (region.ptr == 0 or align_bytes == 0 or region.ptr % align_bytes != 0 or region.len < length)) return error.InvalidShape;
        ends[i] = try std.math.add(u64, region.ptr, length);
    }
    inline for (first_write..count) |written| {
        if (lengths[written] != 0) {
            inline for (0..written) |other| {
                if (lengths[other] != 0 and regions[written].ptr < ends[other] and regions[other].ptr < ends[written]) return error.InvalidShape;
            }
        }
    }
}

test "CUDA region admission permits readonly aliases and rejects output overlap overflow and truncation" {
    var regions = [_]DeviceBuffer{ .{ .ptr = 4096, .len = 64 }, .{ .ptr = 4096, .len = 64 }, .{}, .{ .ptr = 8192, .len = 64 } };
    const lengths = [4]usize{ 64, 64, 0, 64 };
    try validateRegions(4, regions, lengths, @splat(4), 3);
    regions[3].ptr = 4100;
    try std.testing.expectError(error.InvalidShape, validateRegions(4, regions, lengths, @splat(4), 3));
    regions[3].ptr = 8192;
    regions[0].len = 63;
    try std.testing.expectError(error.InvalidShape, validateRegions(4, regions, lengths, @splat(4), 3));
    regions[0].len = 64;
    regions[0].ptr = std.math.maxInt(u64) - 3;
    try std.testing.expectError(error.Overflow, validateRegions(4, regions, lengths, @splat(4), 3));
}

test "CUDA physical OOM releases its reservation and permits a later allocation" {
    const Mock = struct {
        var oom: bool = true;

        fn setCurrent(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }

        fn alloc(ptr: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            if (oom) return driver_mod.CUDA_ERROR_OUT_OF_MEMORY;
            ptr.* = 1;
            return driver_mod.CUDA_SUCCESS;
        }

        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
    };
    var ctx: context_mod.CudaContext = undefined;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
    ctx.ctx = @ptrFromInt(1);
    ctx.device_allocations = .{ .limit = 32 };
    ctx.driver.fns.cuCtxSetCurrent = Mock.setCurrent;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    Mock.oom = true;
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&ctx, 16));
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    Mock.oom = false;
    var buffer = try DeviceBuffer.alloc(&ctx, 16);
    try std.testing.expectEqual(@as(usize, 16), ctx.device_allocations.snapshot().live);
    buffer.free(&ctx);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
}

test "embeddinggemma2 CUDA graph suballocation error cleanup never frees its arena" {
    var borrowed = DeviceBuffer{ .ptr = 1234, .len = 256, .borrowed = true };
    borrowed.free(undefined);
    try std.testing.expectEqual(@as(usize, 0), borrowed.len);
    try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0), borrowed.ptr);
}

test "CUDA strict transfer audit rejects downloads before any driver access" {
    var scope = transfer_audit.Audit{ .forbid_device_host_access = true };
    try transfer_audit.begin(&scope);
    defer transfer_audit.end();
    var bytes: [4]u8 = undefined;
    const storage = DeviceBuffer{ .ptr = 4096, .len = 4 };
    try std.testing.expectError(error.DeviceReadbackForbidden, storage.copyToHost(undefined, &bytes));
    try std.testing.expectEqual(@as(u64, 0), scope.download_calls);
    try std.testing.expectError(error.InvalidCudaState, (DeviceBuffer{ .ptr = 4096, .len = 2 }).copyToHost(undefined, &bytes));
}

test "CUDA immutable allocation purpose fits original descriptor and survives checked free failure" {
    const Original = struct { ptr: driver_mod.CUdeviceptr = 0, len: usize = 0, allocation_bytes: usize = 0, allocation_token: ?allocation_ledger.Token = null, borrowed: bool = false };
    try std.testing.expectEqual(@sizeOf(Original), @sizeOf(DeviceBuffer));
    try std.testing.expectEqual(@alignOf(Original), @alignOf(DeviceBuffer));
    const Mock = struct {
        var fail_free: bool = true;
        var fail_alloc: bool = true;
        fn setCurrent(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
        fn alloc(ptr: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            if (fail_alloc) return driver_mod.CUDA_ERROR_OUT_OF_MEMORY;
            ptr.* = 16;
            return driver_mod.CUDA_SUCCESS;
        }
        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            return if (fail_free) driver_mod.CUDA_ERROR_OUT_OF_MEMORY else driver_mod.CUDA_SUCCESS;
        }
    };
    var ctx: context_mod.CudaContext = undefined;
    ctx.observation = .init(null);
    ctx.observation_enable_failed = .init(false);
    ctx.ctx = @ptrFromInt(1);
    ctx.device_allocations = .{ .limit = 32 };
    ctx.driver.fns.cuCtxSetCurrent = Mock.setCurrent;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    Mock.fail_alloc = true;
    Mock.fail_free = true;
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.allocWithPurpose(&ctx, 16, .kv_state));
    try std.testing.expectEqual(@as(u64, 0), ctx.device_allocations.snapshot().purpose_peak[@backingInt(DeviceBuffer.Purpose.kv_state)]);
    try std.testing.expectEqual(@as(u64, 0), ctx.device_allocations.snapshot().allocations);
    Mock.fail_alloc = false;
    var buffer = try DeviceBuffer.allocWithPurpose(&ctx, 16, .kv_state);
    const before = ctx.device_allocations.snapshot();
    try std.testing.expectError(error.CudaDriverError, buffer.freeChecked(&ctx));
    try std.testing.expectEqualDeep(before, ctx.device_allocations.snapshot());
    try std.testing.expectEqual(DeviceBuffer.Purpose.kv_state, buffer.allocation_purpose);
    Mock.fail_free = false;
    try std.testing.expectEqual(@as(usize, 16), try buffer.freeChecked(&ctx));
    try std.testing.expectEqual(@as(u64, 0), ctx.device_allocations.snapshot().purpose_live[@backingInt(DeviceBuffer.Purpose.kv_state)]);
}

test "CUDA process allocation budget bounds multiple model contexts and restores failed reservations" {
    const Mock = struct {
        var oom = false;
        fn setCurrent(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
        fn alloc(ptr: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            if (oom) return driver_mod.CUDA_ERROR_OUT_OF_MEMORY;
            ptr.* = 1;
            return driver_mod.CUDA_SUCCESS;
        }
        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
    };
    const shared = &allocation_budget.process_device_allocations;
    const before = shared.snapshot();
    try shared.setLimit(before.live + 32);
    defer shared.setLimit(before.limit) catch unreachable;
    var contexts: [2]context_mod.CudaContext = undefined;
    for (&contexts) |*ctx| {
        ctx.ctx = @ptrFromInt(1);
        ctx.device_allocations = .{ .limit = 32 };
        ctx.driver.fns.cuCtxSetCurrent = Mock.setCurrent;
        ctx.driver.fns.cuMemAlloc = Mock.alloc;
        ctx.driver.fns.cuMemFree = Mock.free;
    }
    Mock.oom = false;
    var first = try DeviceBuffer.alloc(&contexts[0], 24);
    defer first.free(&contexts[0]);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&contexts[1], 16));
    try std.testing.expectEqual(@as(usize, 0), contexts[1].device_allocations.snapshot().live);
    try std.testing.expectEqual(before.live + 24, shared.snapshot().live);
    first.free(&contexts[0]);
    Mock.oom = true;
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&contexts[1], 16));
    try std.testing.expectEqual(before.live, shared.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), contexts[1].device_allocations.snapshot().live);
    Mock.oom = false;
    var second = try DeviceBuffer.alloc(&contexts[1], 32);
    second.free(&contexts[1]);
    try std.testing.expectEqual(before.live, shared.snapshot().live);
}

test "CUDA quarantine pins primary lifetime across external final release and handle address reuse" {
    const Mock = struct {
        var references: usize = 0;
        var resets: usize = 0;
        var frees: usize = 0;
        var fail_free = true;
        fn current(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn alloc(ptr: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            ptr.* = 999;
            return 0;
        }
        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            frees += 1;
            return if (fail_free) 201 else 0;
        }
        fn sync(_: driver_mod.CUstream) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn release(_: driver_mod.CUdevice) callconv(.c) driver_mod.CUresult {
            std.debug.assert(references > 0);
            references -= 1;
            if (references == 0) resets += 1;
            return 0;
        }
        fn state(_: driver_mod.CUdevice, flags: *c_uint, active: *c_int) callconv(.c) driver_mod.CUresult {
            flags.* = 0;
            active.* = @intFromBool(references > 0);
            return 0;
        }
        fn errorName(_: driver_mod.CUresult, value: *?[*:0]const u8) callconv(.c) driver_mod.CUresult {
            value.* = "TEST_FREE_FAILURE";
            return 0;
        }
        fn context() context_mod.CudaContext {
            references += 1;
            var ctx: context_mod.CudaContext = .{ .ctx = @ptrFromInt(0xfeed0001), .stream = @ptrFromInt(8), .device = 0, .info = .{}, .driver = undefined };
            ctx.driver.fns.cuCtxSetCurrent = current;
            ctx.driver.fns.cuMemAlloc = alloc;
            ctx.driver.fns.cuMemFree = free;
            ctx.driver.fns.cuStreamSynchronize = sync;
            ctx.driver.fns.cuStreamDestroy = sync;
            ctx.driver.fns.cuDevicePrimaryCtxRelease = release;
            ctx.driver.fns.cuDevicePrimaryCtxGetState = state;
            ctx.driver.fns.cuGetErrorName = errorName;
            return ctx;
        }
    };
    const shared = &allocation_budget.process_device_allocations;
    const before = shared.snapshot();
    try shared.setLimit(before.live + 32);
    defer shared.setLimit(before.limit) catch unreachable;
    Mock.references = 1; // External CUDA retainer, independent of managed owners.
    Mock.resets = 0;
    Mock.frees = 0;
    Mock.fail_free = true;
    var first = Mock.context();
    var stale = try DeviceBuffer.alloc(&first, 24);
    // Keep a copied descriptor to test later ledger slot + address reuse.
    var copied = stale;
    stale.free(&first);
    first.releaseResources();
    first.releaseResources(); // Idempotent: never relinquish the transferred retain.
    try std.testing.expectEqual(@as(usize, 2), Mock.references);
    try std.testing.expectEqual(before.live + 24, shared.snapshot().live);
    // This WOULD be the final release under the former implementation.
    _ = Mock.release(0);
    try std.testing.expectEqual(@as(usize, 1), Mock.references);
    try std.testing.expectEqual(@as(usize, 0), Mock.resets);
    var reloaded = Mock.context();
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&reloaded, 16));
    // This is the same post-retain retry used by initDefault. The pin proves
    // continuity even though no managed wrapper existed between the owners.
    Mock.fail_free = false;
    allocation_ledger.process.retryRetired(@intFromPtr(reloaded.ctx), 0, Mock.free, Mock.release);
    try std.testing.expectEqual(before.live, shared.snapshot().live);
    try std.testing.expectEqual(@as(usize, 1), Mock.references);
    reloaded.releaseResources();
    try std.testing.expectEqual(@as(usize, 1), Mock.resets);
    // A genuinely new primary lifetime reuses both handle and device address.
    var successor = Mock.context();
    var fresh = try DeviceBuffer.alloc(&successor, 32);
    const frees_before = Mock.frees;
    copied.free(&successor);
    allocation_ledger.process.retryRetired(@intFromPtr(successor.ctx), 0, Mock.free, Mock.release);
    try std.testing.expectEqual(frees_before, Mock.frees);
    try std.testing.expectEqual(before.live + 32, shared.snapshot().live);
    fresh.free(&successor);
    successor.releaseResources();
    try std.testing.expectEqual(@as(usize, 0), Mock.references);
    try std.testing.expectEqual(before.live, shared.snapshot().live);
}

test "CUDA allocation ledger handoff keeps destination buffer live through source retirement and checked retry" {
    const Mock = struct {
        var references: usize = 0;
        var frees: usize = 0;
        var resets: usize = 0;
        var fail_free: bool = false;
        fn current(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn alloc(out: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            out.* = 0x777000;
            return 0;
        }
        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            if (fail_free) return 700;
            frees += 1;
            return 0;
        }
        fn sync(_: driver_mod.CUstream) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn release(_: driver_mod.CUdevice) callconv(.c) driver_mod.CUresult {
            references -= 1;
            if (references == 0) resets += 1;
            return 0;
        }
        fn state(_: driver_mod.CUdevice, flags: *c_uint, active: *c_int) callconv(.c) driver_mod.CUresult {
            flags.* = 0;
            active.* = @intFromBool(references != 0);
            return 0;
        }
        fn context() context_mod.CudaContext {
            references += 1;
            var result = context_mod.CudaContext{ .driver = undefined, .ctx = @ptrFromInt(0x777), .stream = null, .device = 0, .info = .{} };
            result.driver.fns.cuCtxSetCurrent = current;
            result.driver.fns.cuMemAlloc = alloc;
            result.driver.fns.cuMemFree = free;
            result.driver.fns.cuStreamSynchronize = sync;
            result.driver.fns.cuDevicePrimaryCtxRelease = release;
            result.driver.fns.cuDevicePrimaryCtxGetState = state;
            return result;
        }
    };
    Mock.references = 0;
    Mock.frees = 0;
    Mock.resets = 0;
    Mock.fail_free = false;
    const process_before = allocation_budget.process_device_allocations.snapshot();
    var source = Mock.context();
    defer source.releaseResources();
    var destination = Mock.context();
    defer destination.releaseResources();
    try allocation_ledger.process.prepareOwner(@intFromPtr(destination.ctx), &destination.device_allocations.owner_id);
    var buffer = try DeviceBuffer.allocWithPurpose(&source, 24, .kv_state);
    var stale = buffer;
    try buffer.handoffLedgerOwner(&source, &destination);
    try source.device_allocations.handoffToWithPurpose(&destination.device_allocations, 24, .kv_state);
    source.releaseResources();
    try std.testing.expectEqual(@as(usize, 0), Mock.frees);
    try std.testing.expectEqual(@as(usize, 1), Mock.references);
    try std.testing.expect(allocation_ledger.process.isLive(buffer.allocation_token.?));
    try std.testing.expectEqual(process_before.live + 24, allocation_budget.process_device_allocations.snapshot().live);
    Mock.fail_free = true;
    const old = buffer;
    try std.testing.expectError(error.CudaDriverError, buffer.freeChecked(&destination));
    try std.testing.expectEqualDeep(old, buffer);
    try std.testing.expectEqual(@as(usize, 24), destination.device_allocations.snapshot().physical_live);
    try std.testing.expectEqual(process_before.live + 24, allocation_budget.process_device_allocations.snapshot().live);
    Mock.fail_free = false;
    try std.testing.expectEqual(@as(usize, 24), try buffer.freeChecked(&destination));
    try std.testing.expectEqual(@as(usize, 1), Mock.frees);
    destination.releaseResources();
    try std.testing.expectEqual(@as(usize, 1), Mock.resets);
    try std.testing.expectEqual(process_before.live, allocation_budget.process_device_allocations.snapshot().live);
    var successor = Mock.context();
    defer successor.releaseResources();
    var fresh = try DeviceBuffer.alloc(&successor, 32);
    defer fresh.free(&successor);
    try std.testing.expectEqual(@as(usize, 0), try stale.freeChecked(&successor));
    try std.testing.expectEqual(@as(usize, 1), Mock.frees);
    try std.testing.expectEqual(@as(usize, 32), successor.device_allocations.snapshot().live);
}

test "Gemma4 CUDA host PLE mapped allocation waits for readers and retains failed frees" {
    const Fake = struct {
        var bytes: [64]u8 = undefined;
        var waits: usize = 0;
        var frees: usize = 0;
        var fail_free: bool = false;
        var fail_device: bool = false;
        fn getCurrent(ptr: *driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            ptr.* = @ptrFromInt(1);
            return driver_mod.CUDA_SUCCESS;
        }
        fn contextWait() callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
        fn releasePrimary(_: driver_mod.CUdevice) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
        fn current(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
        fn alloc(ptr: *?*anyopaque, len: usize, flags: c_uint) callconv(.c) driver_mod.CUresult {
            if (len != bytes.len or flags != 2) return 1;
            ptr.* = &bytes;
            return driver_mod.CUDA_SUCCESS;
        }
        fn device(ptr: *driver_mod.CUdeviceptr, _: ?*anyopaque, flags: c_uint) callconv(.c) driver_mod.CUresult {
            if (flags != 0 or fail_device) return 1;
            ptr.* = 0x123456;
            return driver_mod.CUDA_SUCCESS;
        }
        fn wait(_: driver_mod.CUstream) callconv(.c) driver_mod.CUresult {
            waits += 1;
            return driver_mod.CUDA_SUCCESS;
        }
        fn free(_: ?*anyopaque) callconv(.c) driver_mod.CUresult {
            frees += 1;
            return if (fail_free) 1 else driver_mod.CUDA_SUCCESS;
        }
    };
    var ctx = context_mod.CudaContext{ .driver = undefined, .ctx = @ptrFromInt(1), .stream = null, .device = 0, .info = .{} };
    ctx.driver.fns.cuCtxSetCurrent = Fake.current;
    ctx.driver.fns.cuMemHostAlloc = Fake.alloc;
    ctx.driver.fns.cuMemHostGetDevicePointer = Fake.device;
    ctx.driver.fns.cuMemFreeHost = Fake.free;
    ctx.driver.fns.cuStreamSynchronize = Fake.wait;
    Fake.waits = 0;
    Fake.frees = 0;
    Fake.fail_free = false;
    var mapped = MappedHostBuffer{ .host = .{ .len = 64 } };
    try std.testing.expectEqual(driver_mod.CUDA_SUCCESS, Fake.alloc(&mapped.host.ptr, 64, 2));
    try std.testing.expectEqual(driver_mod.CUDA_SUCCESS, Fake.device(&mapped.device_ptr, mapped.host.ptr, 0));
    var view = mapped.view();
    try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0x123456), view.ptr);
    try std.testing.expect(view.borrowed and view.allocation_token == null);
    view.free(&ctx);
    try std.testing.expectEqual(@as(usize, 0), Fake.frees);
    Fake.fail_free = true;
    try std.testing.expectError(error.CudaDriverError, mapped.clear(&ctx));
    try std.testing.expect(mapped.host.ptr != null and mapped.device_ptr != 0);
    try std.testing.expectEqual(@as(usize, 1), Fake.waits);
    Fake.fail_free = false;
    try mapped.clear(&ctx);
    try std.testing.expect(mapped.host.ptr == null and mapped.device_ptr == 0);
    try std.testing.expectEqual(@as(usize, 2), Fake.waits);
    try std.testing.expectEqual(@as(usize, 2), Fake.frees);
    ctx.driver.fns.cuMemHostGetDevicePointer = null;
    try std.testing.expectError(error.CudaSymbolMissing, MappedHostBuffer.alloc(&ctx, 64));
    // A real allocation followed by a failed device-alias query must not
    // drop ownership/admission when rollback's cuMemFreeHost also fails.
    var controller = memory.AdmissionController{};
    var queue = host_retirement.Queue{ .allocator = std.testing.allocator };
    const owner = try std.testing.allocator.create(host_retirement.Owner);
    owner.* = .{
        .queue = &queue,
        .driver = undefined,
        .owns_library = false,
        .primary = ctx.ctx,
        .device = 0,
        .bytes = 64,
        .lease = try controller.tryAcquire(.gpu, .{}, .{ .host_weight_bytes = 64 }, false),
    };
    owner.driver.fns.cuCtxGetCurrent = Fake.getCurrent;
    owner.driver.fns.cuCtxSetCurrent = Fake.current;
    owner.driver.fns.cuCtxSynchronize = Fake.contextWait;
    owner.driver.fns.cuMemFreeHost = Fake.free;
    owner.driver.fns.cuDevicePrimaryCtxRelease = Fake.releasePrimary;
    ctx.driver.fns.cuMemHostGetDevicePointer = Fake.device;
    Fake.fail_device = true;
    Fake.fail_free = true;
    try std.testing.expectError(error.CudaDriverError, MappedHostBuffer.allocUsingOwner(&ctx, 64, owner));
    try std.testing.expect(queue.head != null and queue.head.?.ptr != null);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_weight_bytes);
    Fake.fail_free = false;
    queue.retry();
    try std.testing.expect(queue.head == null);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}
