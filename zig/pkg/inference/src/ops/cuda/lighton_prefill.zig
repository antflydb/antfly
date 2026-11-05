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

//! Optional SM12.1 BF16 recurrent prefill. Scratch is owned and admitted by
//! CudaCompute; the library never allocates device memory during execution.
const std = @import("std");
const driver = @import("driver.zig");
const context = @import("context.zig");
const buffer = @import("buffer.zig");
const memory = @import("../../runtime/tier/memory.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
const Quota = @import("cache_quota.zig").Quota;

pub const Module = struct {
    metadata_mutex: std.atomic.Mutex = .unlocked,
    library: std.DynLib,
    attention: ?@import("lighton_prefill_attention.zig").Module = null,
    workspace: *const fn (*const anyopaque) callconv(.c) usize,
    execute: *const fn (*const anyopaque, driver.CUdeviceptr, usize, c_int, driver.CUstream) callconv(.c) c_int,
    geometry_workspace: ?*const fn (*const @import("prefill_geometry.zig").Geometry) callconv(.c) usize = null,
    scratch: buffer.DeviceBuffer = .{},
    quota: Quota = .none,
    revision: u64 = 0,
    failed: bool = false,
    managed: bool = false,
    closing: bool = false,
    managed_decline: bool = false,
    planned_geometry_hash: ?u64 = null,
    planned_geometry: [2]@import("prefill_geometry.zig").Geometry = undefined,
    planned_packet_count: usize = 0,
    planned_packet_bytes: [2]usize = @splat(0),
    planned_attention: bool = false,
    planned_workspace_bytes: usize = 0,
    calls: usize = 0,
    workspace_peak: usize = 0,

    pub const QuotaSnapshot = struct { raw_quota: bool, controller: ?*memory.AdmissionController, closing: bool };
    /// Values only. Never borrow the mutable prepared Charge or SDK address.
    pub fn quotaSnapshot(self: *const Module) QuotaSnapshot {
        const mutable = @constCast(self);
        while (!mutable.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        defer mutable.metadata_mutex.unlock();
        return .{ .raw_quota = self.quota == .raw, .controller = if (self.quota.peek()) |lease| lease.controller else null, .closing = self.closing };
    }
    fn markClosing(self: *Module) void {
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.metadata_mutex.unlock();
        self.closing = true;
    }
    pub fn init(path: []const u8) !Module {
        var library = try std.DynLib.open(path);
        errdefer library.close();
        const abi = library.lookup(*const fn () callconv(.c) u64, "antfly_lighton_prefill_abi") orelse return error.LightOnPrefillAbiMismatch;
        if (abi() != (@as(u64, 1) << 32 | 2408)) return error.LightOnPrefillAbiMismatch;
        const geometry_abi = library.lookup(*const fn () callconv(.c) u64, "antfly_recurrent_prefill_workspace_geometry_abi");
        const geometry_workspace = library.lookup(*const fn (*const @import("prefill_geometry.zig").Geometry) callconv(.c) usize, "antfly_recurrent_prefill_workspace_geometry");
        if ((geometry_abi == null) != (geometry_workspace == null)) return error.RecurrentPrefillGeometryAbiMismatch;
        if (geometry_abi) |version| if (version() != @import("prefill_geometry.zig").Geometry.abi) return error.RecurrentPrefillGeometryAbiMismatch;
        return .{
            .library = library,
            .geometry_workspace = geometry_workspace,
            .attention = try @import("lighton_prefill_attention.zig").Module.init(&library),
            .workspace = library.lookup(*const fn (*const anyopaque) callconv(.c) usize, "antfly_lighton_prefill_workspace") orelse return error.LightOnPrefillAbiMismatch,
            .execute = library.lookup(*const fn (*const anyopaque, driver.CUdeviceptr, usize, c_int, driver.CUstream) callconv(.c) c_int, "antfly_lighton_prefill_execute") orelse return error.LightOnPrefillAbiMismatch,
        };
    }

    pub fn clearWorkspace(self: *Module, ctx: *context.CudaContext) !void {
        return self.clearWorkspaceUsing(ctx, null);
    }
    pub fn clearWorkspaceWithCleanup(self: *Module, ctx: *context.CudaContext, current: Cleanup) !void {
        return self.clearWorkspaceUsing(ctx, current);
    }
    fn clearWorkspaceUsing(self: *Module, ctx: *context.CudaContext, current: ?Cleanup) !void {
        if (current) |value| try self.quota.validateCleanup(value) else try self.quota.validateLegacyRaw();
        if (self.attention) |*attention| {
            if (current) |value| try attention.clearWithCleanup(ctx, value) else try attention.clear(ctx);
        }
        try self.clearRecurrentWorkspaceUsing(ctx, current);
    }
    pub fn clearRecurrentWorkspaceWithCleanup(self: *Module, ctx: *context.CudaContext, current: Cleanup) !void {
        return self.clearRecurrentWorkspaceUsing(ctx, current);
    }
    pub fn validateCleanup(self: *const Module, current: Cleanup) !void {
        try self.quota.validateCleanup(current);
        if (self.attention) |*attention| try attention.quota.validateCleanup(current);
    }
    fn clearRecurrentWorkspace(self: *Module, ctx: *context.CudaContext) !void {
        return self.clearRecurrentWorkspaceUsing(ctx, null);
    }
    fn clearRecurrentWorkspaceUsing(self: *Module, ctx: *context.CudaContext, current: ?Cleanup) !void {
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        var locked = true;
        defer if (locked) self.metadata_mutex.unlock();
        if (current) |value| try self.quota.validateCleanup(value) else try self.quota.validateLegacyRaw();
        if (self.scratch.ptr != 0) {
            if (try ctx.streamCaptureActive()) return error.CudaGraphCaptureUnsafeTempAlloc;
            try ctx.synchronize();
            _ = try self.scratch.freeChecked(ctx);
        }
        // Consume/reset the actual owner BEFORE callbacks can reenter it.
        var quota = self.quota;
        self.quota = .none;
        self.failed = false;
        self.revision +%= 1;
        self.metadata_mutex.unlock();
        locked = false;
        errdefer {
            while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
            self.quota = quota; // registration failure precedes callbacks
            self.metadata_mutex.unlock();
        }
        if (current) |value| try quota.releaseAfterSdkDeath(value) else try quota.releaseLegacyRawAfterSdkDeath();
    }

    /// Exact cold-funded first birth. A failed allocation remains in this
    /// persistent Module until CHECKED cleanup; no dead local lease/header.
    pub fn reservePrepared(self: *Module, ctx: *context.CudaContext, lease: *@import("prepared_cache.zig").Lease, request: @import("prefill_workspace.zig").Request, current: Cleanup) !buffer.DeviceBuffer {
        try @import("prefill_workspace.zig").validateToken(lease, request, current);
        if (try ctx.streamCaptureActive()) return error.CudaGraphCaptureUnsafeTempAlloc;
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        var locked = true;
        defer if (locked) self.metadata_mutex.unlock();
        if (request.kind != .recurrent or request.host_bytes != 0 or request.device_bytes == 0 or self.scratch.ptr != 0 or self.quota.hasLease() or self.failed or self.closing) return error.InvalidPreparedCacheState;
        self.quota = lease.publishQuota();
        self.failed = true;
        self.revision +%= 1;
        self.scratch = buffer.DeviceBuffer.alloc(ctx, request.device_bytes) catch |err| {
            self.metadata_mutex.unlock();
            locked = false;
            self.clearRecurrentWorkspaceUsing(ctx, current) catch {};
            return err;
        };
        self.failed = false;
        return self.scratch;
    }

    /// Legacy explicit-outside entry. Suppliers and quota discharge remain
    /// outside the metadata lock; exact publication is revalidated afterward.
    pub fn reserve(self: *Module, ctx: *context.CudaContext, bytes: usize) !buffer.DeviceBuffer {
        if (self.closing) return error.RecurrentPrefillClosing;
        if (bytes == 0) return .{};
        if (self.managed_decline) return error.RecurrentPrefillColdPreparationRequired;
        if (self.failed) return error.LightOnPrefillExecutionFailed;
        if (self.scratch.len >= bytes) return self.scratch;
        if (self.managed or self.quota == .prepared) return error.RecurrentPrefillColdPreparationRequired;
        try self.clearRecurrentWorkspace(ctx);
        var lease: ?memory.AdmissionLease = null;
        defer if (lease) |*active| active.release();
        if (ctx.video_cache_budget.admission) |policy| {
            lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{ .backend_scratch_bytes = bytes }, policy.check_live_memory);
        }
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        var locked = true;
        defer if (locked) self.metadata_mutex.unlock();
        if (self.closing or self.failed or self.scratch.ptr != 0 or self.quota.hasLease()) return error.InvalidPreparedCacheState;
        self.quota = Quota.fromRawAdmission(lease);
        lease = null;
        self.failed = true;
        self.scratch = buffer.DeviceBuffer.alloc(ctx, bytes) catch |err| {
            self.metadata_mutex.unlock();
            locked = false;
            self.clearRecurrentWorkspace(ctx) catch {};
            return err;
        };
        self.failed = false;
        self.revision +%= 1;
        return self.scratch;
    }

    pub fn deinitChecked(self: *Module, ctx: *context.CudaContext) !void {
        self.markClosing();
        if (self.attention) |*attention| attention.markClosing();
        try self.clearWorkspace(ctx);
        self.library.close();
        self.* = undefined;
    }
    pub fn deinitWithCleanup(self: *Module, ctx: *context.CudaContext, current: Cleanup) !void {
        self.markClosing();
        if (self.attention) |*attention| attention.markClosing();
        try self.clearWorkspaceWithCleanup(ctx, current);
        self.library.close();
        self.* = undefined;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.deinitChecked(ctx) catch {};
    }

    /// Real selected-library capability, queried outside numerical ownership.
    /// Older DSOs retain their raw ABI; a managed cold owner can observe this
    /// refusal before numerical commitment rather than faking packet pointers.
    pub fn coldWorkspaceBytes(self: *const Module, geometry: *const @import("prefill_geometry.zig").Geometry) !usize {
        try geometry.validate();
        const query = self.geometry_workspace orelse return error.RecurrentPrefillColdGeometryUnavailable;
        const bytes = query(geometry);
        if (bytes == std.math.maxInt(usize)) return error.InvalidTensorShape;
        return bytes;
    }

    pub fn workspaceBytes(self: *const Module, packet: *const anyopaque) !usize {
        if (self.closing) return error.RecurrentPrefillClosing;
        var expected: ?usize = null;
        if (self.managed) {
            if (self.managed_decline) return error.RecurrentPrefillColdPreparationRequired;
            const typed: *const @import("gated_delta.zig").RaggedParams = @ptrCast(@alignCast(packet));
            const actual = try @import("prefill_geometry.zig").Geometry.fromPacket(typed);
            for (self.planned_geometry[0..self.planned_packet_count], self.planned_packet_bytes[0..self.planned_packet_count]) |geometry, planned_bytes| if (std.meta.eql(geometry, actual)) {
                expected = planned_bytes;
                break;
            };
            if (expected == null) return error.RecurrentPrefillColdPreparationRequired;
        }
        const bytes = self.workspace(packet); // original REAL packet ABI, unchanged
        if (bytes == std.math.maxInt(usize)) return error.InvalidTensorShape;
        if (expected) |value| if (bytes != value) return error.RecurrentPrefillWorkspaceGeometryMismatch;
        return bytes;
    }

    pub fn run(self: *Module, packet: *const anyopaque, scratch: driver.CUdeviceptr, bytes: usize, device: c_int, stream: driver.CUstream) !void {
        // No fallback is safe after the first queued state update.
        if (self.closing) return error.RecurrentPrefillClosing;
        if (self.execute(packet, scratch, bytes, device, stream) != 0) return error.LightOnPrefillExecutionFailed;
        self.calls += 1;
        self.workspace_peak = @max(self.workspace_peak, bytes);
    }
};

test "recurrent prefill values-only cold query and actual managed packet agree before any DSO state update" {
    const Geometry = @import("prefill_geometry.zig").Geometry;
    const Fake = struct {
        var queries: usize = 0;
        var executions: usize = 0;
        fn query(value: *const Geometry) callconv(.c) usize {
            queries += 1;
            return if (value.items == 2 and value.lengths[0] == 3 and value.lengths[1] == 1) 8192 else std.math.maxInt(usize);
        }
        fn workspace(_: *const anyopaque) callconv(.c) usize {
            queries += 1;
            return 8192;
        }
        fn execute(_: *const anyopaque, _: driver.CUdeviceptr, _: usize, _: c_int, _: driver.CUstream) callconv(.c) c_int {
            executions += 1;
            return 0;
        }
    };
    var module: Module = .{ .library = undefined, .workspace = Fake.workspace, .execute = Fake.execute, .geometry_workspace = Fake.query };
    Fake.queries = 0;
    Fake.executions = 0;
    const geometry = try Geometry.fromLengths(16, 16, &.{ 3, 1 });
    try std.testing.expectEqual(@as(usize, 8192), try module.coldWorkspaceBytes(&geometry));
    var packet = @import("gated_delta.zig").RaggedParams{ .base = .{ .key_heads = 16, .value_heads = 16, .items = 2 } };
    packet.lengths[0] = 3;
    packet.lengths[1] = 1;
    packet.offsets[1] = 3;
    module.managed = true;
    module.planned_packet_count = 1;
    module.planned_geometry[0] = geometry;
    module.planned_packet_bytes[0] = 8192;
    try std.testing.expectEqual(@as(usize, 8192), try module.workspaceBytes(&packet));
    packet.lengths[0] = 4;
    packet.offsets[1] = 4;
    const before = Fake.queries;
    try std.testing.expectError(error.RecurrentPrefillColdPreparationRequired, module.workspaceBytes(&packet));
    try std.testing.expectEqual(before, Fake.queries);
    module.geometry_workspace = null;
    try std.testing.expectError(error.RecurrentPrefillColdGeometryUnavailable, module.coldWorkspaceBytes(&geometry));
    try std.testing.expectEqual(@as(usize, 0), Fake.executions);
}
