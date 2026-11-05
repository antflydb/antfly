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

//! Optional paged prefill attention, resolved from the explicitly selected
//! recurrent-prefill library. Independent bounded memory and copy ownership.
const std = @import("std");
const driver = @import("driver.zig");
const context = @import("context.zig");
const buffer = @import("buffer.zig");
const memory = @import("../../runtime/tier/memory.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
const Quota = @import("cache_quota.zig").Quota;

pub const PlanRequest = extern struct {
    float_workspace: u64,
    int_workspace: u64,
    pinned_workspace: u64,
    float_bytes: u64,
    int_bytes: u64,
    pinned_bytes: u64,
    cpu_query_indptr: u64,
    cpu_kv_indptr: u64,
    cpu_kv_lengths: u64,
    batch: i32,
    heads: i32,
    kv_heads: i32,
    rows: i32,
    page_size: i32,
    device: i32,
    stream: u64,
};
pub const RunRequest = extern struct {
    plan: u64,
    q: u64,
    k: u64,
    v: u64,
    out: u64,
    query_indptr: u64,
    kv_indptr: u64,
    kv_indices: u64,
    last_page_lengths: u64,
    pages: u64,
    stream: u64,
};
comptime {
    if (@sizeOf(PlanRequest) != 104 or @sizeOf(RunRequest) != 88)
        @compileError("Native prefill attention ABI changed");
}
const float_bytes = 64 * 1024 * 1024;
const int_bytes = 128 * 1024;
const q_bytes = 4096 * 16 * 256 * 2;
const meta_bytes = 4096;
pub const device_bytes = float_bytes + int_bytes + q_bytes + meta_bytes;
pub const metadata_slots = 64;
const slot_bytes = int_bytes + meta_bytes;
pub const host_bytes = metadata_slots * slot_bytes;
pub const host_plan_bytes = 64 * 1024;

pub const Shape = struct {
    batch: usize,
    rows: usize,
    tokens: usize,
    total_tokens: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    mask_len: usize,
    bias_mode: usize,
    heads: usize,
    kv_heads: usize,
    head_dim: usize,
    page_size: usize,
    key_f16: bool,
    value_f16: bool,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    value_row_bytes: usize,
    pub fn supported(self: Shape) bool {
        return self.batch == 1 and self.rows >= 2 and self.rows <= 4096 and
            self.tokens >= self.rows and self.tokens <= 4096 and self.total_tokens == self.tokens and
            self.kv_position_offset == 0 and self.sliding_window == 0 and
            self.mask_len == 0 and self.bias_mode == 0 and self.head_dim == 256 and
            ((self.heads == 8 and self.kv_heads == 2) or (self.heads == 16 and self.kv_heads == 4)) and
            self.page_size == 16 and self.key_f16 and self.value_f16 and
            self.key_row_bytes == self.kv_heads * self.head_dim * 2 and
            self.base_key_row_bytes == self.kv_heads * self.head_dim * 2 and
            self.value_row_bytes == self.kv_heads * self.head_dim * 2;
    }
};

pub const Module = struct {
    // Independent cold metadata readers never acquire the numerical Model.
    // This mutex protects publication/death snapshots, not execution authority.
    metadata_mutex: std.atomic.Mutex = .unlocked,
    make_plan: *const fn (*const PlanRequest, *?*anyopaque) callconv(.c) c_int,
    execute: *const fn (*const RunRequest) callconv(.c) c_int,
    drop: *const fn (?*anyopaque) callconv(.c) void,
    device: buffer.DeviceBuffer = .{},
    host: buffer.HostBuffer = .{},
    quota: Quota = .none,
    revision: u64 = 0,
    managed: bool = false,
    closing: bool = false,
    copy_events: [metadata_slots]driver.CUevent = @splat(null),
    copy_pending: [metadata_slots]bool = @splat(false),
    next_slot: usize = 0,
    staged_slot: ?usize = null,
    slot_reuses: usize = 0,
    plan: ?*anyopaque = null,
    plan_key: [3]usize = @splat(0),
    failed: bool = false,
    calls: usize = 0,
    plans: usize = 0,

    pub const StorageSnapshot = struct {
        raw_quota: bool,
        has_lease: bool,
        controller: ?*memory.AdmissionController,
        ready: bool,
        failed: bool,
        closing: bool,
    };
    pub fn storageSnapshot(self: *const Module) StorageSnapshot {
        const mutable = @constCast(self);
        while (!mutable.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        defer mutable.metadata_mutex.unlock();
        return .{
            .raw_quota = self.quota == .raw,
            .has_lease = self.quota.hasLease(),
            .controller = if (self.quota.peek()) |lease| lease.controller else null,
            .ready = self.device.ptr != 0 and self.host.ptr != null,
            .failed = self.failed,
            .closing = self.closing,
        };
    }
    pub fn markClosing(self: *Module) void {
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.metadata_mutex.unlock();
        self.closing = true;
    }

    /// The enclosing recurrent-prefill owner retains the library until clear.
    /// Existing libraries without this optional capability retain their path.
    pub fn init(library: *std.DynLib) !?Module {
        const abi = library.lookup(*const fn () callconv(.c) u64, "antfly_attention_abi") orelse return null;
        if (abi() != (@as(u64, 1) << 32 | 104 << 16 | 88)) return error.LightOnAttentionAbiMismatch;
        return .{
            .make_plan = library.lookup(*const fn (*const PlanRequest, *?*anyopaque) callconv(.c) c_int, "antfly_attention_plan") orelse return error.LightOnAttentionAbiMismatch,
            .execute = library.lookup(*const fn (*const RunRequest) callconv(.c) c_int, "antfly_attention_run") orelse return error.LightOnAttentionAbiMismatch,
            .drop = library.lookup(*const fn (?*anyopaque) callconv(.c) void, "antfly_attention_drop") orelse return error.LightOnAttentionAbiMismatch,
        };
    }

    pub fn reserve(self: *Module, ctx: *context.CudaContext) !void {
        // External quota callbacks precede publication locking. Recheck after
        // reservation; a reentrant cold observer must never see a partial owner.
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        const ready = self.device.ptr != 0;
        const invalid = self.failed or self.closing;
        const managed = self.managed or self.quota == .prepared;
        self.metadata_mutex.unlock();
        if (invalid) return error.LightOnAttentionExecutionFailed;
        if (ready) return;
        if (managed) return error.RecurrentPrefillColdPreparationRequired;
        var lease: ?memory.AdmissionLease = null;
        defer if (lease) |*value| value.release();
        if (ctx.video_cache_budget.admission) |policy|
            lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{
                .backend_scratch_bytes = device_bytes,
                .host_scratch_bytes = host_bytes + host_plan_bytes,
            }, policy.check_live_memory);
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        var locked = true;
        defer if (locked) self.metadata_mutex.unlock();
        if (self.failed or self.closing) return error.LightOnAttentionExecutionFailed;
        if (self.device.ptr != 0) return;
        if (self.managed or self.quota.hasLease()) return error.RecurrentPrefillColdPreparationRequired;
        self.quota = Quota.fromRawAdmission(lease);
        lease = null;
        self.failed = true;
        self.revision +%= 1;
        self.allocateStorage(ctx) catch |err| {
            self.metadata_mutex.unlock();
            locked = false;
            self.clear(ctx) catch {};
            return err;
        };
        self.failed = false;
    }
    pub fn reservePrepared(self: *Module, ctx: *context.CudaContext, lease: *@import("prepared_cache.zig").Lease, request: @import("prefill_workspace.zig").Request, current: Cleanup) !void {
        try @import("prefill_workspace.zig").validateToken(lease, request, current);
        if (try ctx.streamCaptureActive()) return error.CudaGraphCaptureUnsafeTempAlloc;
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        var locked = true;
        defer if (locked) self.metadata_mutex.unlock();
        if (request.kind != .attention or request.device_bytes != device_bytes or request.host_bytes != host_bytes + host_plan_bytes or self.device.ptr != 0 or self.host.ptr != null or self.quota.hasLease() or self.failed or self.closing) return error.InvalidPreparedCacheState;
        self.quota = lease.publishQuota();
        self.failed = true;
        self.revision +%= 1;
        self.allocateStorage(ctx) catch |err| {
            self.metadata_mutex.unlock();
            locked = false;
            self.clearWithCleanup(ctx, current) catch {};
            return err;
        };
        self.failed = false;
    }
    fn allocateStorage(self: *Module, ctx: *context.CudaContext) !void {
        // Assign each actual owner before the next fallible SDK call. A
        // failed checked unwind never drops a still-live local handle/quota.
        self.device = try buffer.DeviceBuffer.alloc(ctx, device_bytes);
        self.host = try buffer.HostBuffer.alloc(ctx, host_bytes);
        for (&self.copy_events) |*event| event.* = try ctx.createStreamEvent();
    }
    pub fn clear(self: *Module, ctx: *context.CudaContext) !void {
        return self.clearUsing(ctx, null);
    }
    pub fn clearWithCleanup(self: *Module, ctx: *context.CudaContext, current: Cleanup) !void {
        return self.clearUsing(ctx, current);
    }
    fn clearUsing(self: *Module, ctx: *context.CudaContext, current: ?Cleanup) !void {
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        var locked = true;
        defer if (locked) self.metadata_mutex.unlock();
        if (current) |value| try self.quota.validateCleanup(value) else try self.quota.validateLegacyRaw();
        if (self.device.ptr != 0 or self.host.ptr != null or self.plan != null) if (try ctx.streamCaptureActive()) return error.CudaGraphCaptureUnsafeTempAlloc;
        self.failed = true;
        if (self.device.ptr != 0 or self.host.ptr != null) try ctx.synchronize();
        // The library's Plan is host-only Array<int64_t>/scalar metadata;
        // every CUDA backing belongs to these explicit buffers, after drain.
        self.drop(self.plan);
        self.plan = null;
        self.plan_key = @splat(0);
        for (&self.copy_events) |*event| try ctx.destroyEventChecked(event);
        try self.host.freeChecked(ctx);
        _ = try self.device.freeChecked(ctx);
        self.copy_pending = @splat(false);
        self.next_slot = 0;
        self.staged_slot = null;
        self.failed = false;
        self.revision +%= 1;
        var quota = self.quota;
        self.quota = .none;
        self.metadata_mutex.unlock();
        locked = false;
        errdefer {
            while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
            self.quota = quota; // registration failure precedes callbacks
            self.metadata_mutex.unlock();
        }
        if (current) |value| try quota.releaseAfterSdkDeath(value) else try quota.releaseLegacyRawAfterSdkDeath();
    }

    /// One owner submits on one stream. A slot cannot be borrowed twice or
    /// overwritten until its last DMA reader retires. A frame may enqueue all
    /// selected prefix/layer packets before wrapping this bounded ring.
    pub fn acquireSlot(self: *Module, ctx: *context.CudaContext) !usize {
        if (self.failed or self.device.ptr == 0 or self.staged_slot != null) return error.InvalidExecutionPlan;
        const index = self.next_slot;
        if (self.copy_pending[index]) {
            try ctx.makeCurrent();
            try ctx.driver.check(ctx.synchronizeEventResult(self.copy_events[index]));
            self.copy_pending[index] = false;
            self.slot_reuses += 1;
        }
        self.next_slot = (index + 1) % metadata_slots;
        self.staged_slot = index;
        return index;
    }

    pub fn hostSlot(self: *Module, index: usize) []u8 {
        std.debug.assert(self.staged_slot != null and self.staged_slot.? == index and !self.copy_pending[index]);
        return self.host.bytes()[index * slot_bytes ..][0..slot_bytes];
    }

    pub fn finishSlotCopy(self: *Module, ctx: *context.CudaContext, index: usize) !void {
        if (index >= metadata_slots or self.staged_slot == null or self.staged_slot.? != index or self.copy_pending[index]) return error.InvalidExecutionPlan;
        errdefer self.failed = true;
        self.copy_pending[index] = true;
        try ctx.recordEvent(self.copy_events[index]);
        self.staged_slot = null;
    }

    pub fn query(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.device.ptr + float_bytes + int_bytes, .len = q_bytes, .borrowed = true };
    }

    /// Prefix-only first integration: batch=1, Q/KV head_dim=256, NHD F16 K/V,
    /// F16 Q with F32 output. The caller validates its actual backing capacities.
    /// Every metadata copy uses pinned storage; the event retires that copy
    /// before CPU reuse, without waiting for unrelated decoder GPU work.
    pub fn run(self: *Module, ctx: *context.CudaContext, output: buffer.DeviceBuffer, k: buffer.DeviceBuffer, v: buffer.DeviceBuffer, rows: usize, tokens: usize, heads: usize, kv_heads: usize, blocks: []const u32, physical_pages: usize) !void {
        while (!self.metadata_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.metadata_mutex.unlock();
        if (self.failed or self.closing) return error.LightOnAttentionExecutionFailed;
        if (self.device.ptr == 0 or rows < 2 or rows > 4096 or tokens < rows or tokens > 4096 or
            !((heads == 8 and kv_heads == 2) or (heads == 16 and kv_heads == 4)) or
            blocks.len != (tokens + 15) / 16 or physical_pages == 0 or
            output.len < rows * heads * 256 * 4 or
            k.len < physical_pages * 16 * kv_heads * 256 * 2 or v.len < physical_pages * 16 * kv_heads * 256 * 2)
            return error.InvalidShape;
        for (blocks) |block| if (block >= physical_pages) return error.InvalidShape;
        const slot = try self.acquireSlot(ctx);
        // A failed upload/plan may already have queued copies. Preserve all
        // storage until clear drains it, and refuse reuse after that failure.
        errdefer self.failed = true;
        // The scheduler and explicit metadata have disjoint storage inside
        // the selected slot. Other slots may still have queued H2D readers.
        const host_slot = self.hostSlot(slot);
        const host_meta = host_slot[int_bytes..];
        const meta: [*]u32 = @ptrCast(@alignCast(host_meta.ptr));
        meta[0] = 0;
        meta[1] = @intCast(rows);
        meta[2] = 0;
        meta[3] = @intCast(blocks.len);
        meta[4] = @intCast((tokens - 1) % 16 + 1);
        @memcpy(meta[5..][0..blocks.len], blocks);
        const key = [3]usize{ rows, tokens, heads };
        if (self.plan == null or !std.mem.eql(usize, &self.plan_key, &key)) {
            self.drop(self.plan);
            self.plan = null;
            const length: i32 = @intCast(tokens);
            const request = PlanRequest{
                .float_workspace = self.device.ptr,
                .int_workspace = self.device.ptr + float_bytes,
                .pinned_workspace = @intFromPtr(host_slot.ptr),
                .float_bytes = float_bytes,
                .int_bytes = int_bytes,
                .pinned_bytes = int_bytes,
                .cpu_query_indptr = @intFromPtr(&meta[0]),
                .cpu_kv_indptr = @intFromPtr(&meta[2]),
                .cpu_kv_lengths = @intFromPtr(&length),
                .batch = 1,
                .heads = @intCast(heads),
                .kv_heads = @intCast(kv_heads),
                .rows = @intCast(rows),
                .page_size = 16,
                .device = ctx.info.selected_device,
                .stream = @intFromPtr(ctx.stream),
            };
            if (self.make_plan(&request, &self.plan) != 0) return error.LightOnAttentionPlanFailed;
            self.plan_key = key;
            self.plans += 1;
        }
        const meta_device = buffer.DeviceBuffer{ .ptr = self.device.ptr + float_bytes + int_bytes + q_bytes, .len = meta_bytes, .borrowed = true };
        try meta_device.copyFromHost(ctx, host_meta[0 .. (5 + blocks.len) * 4]);
        try self.finishSlotCopy(ctx, slot);
        const request = RunRequest{
            .plan = @intFromPtr(self.plan),
            .q = self.query().ptr,
            .k = k.ptr,
            .v = v.ptr,
            .out = output.ptr,
            .query_indptr = meta_device.ptr,
            .kv_indptr = meta_device.ptr + 8,
            .last_page_lengths = meta_device.ptr + 16,
            .kv_indices = meta_device.ptr + 20,
            .pages = physical_pages,
            .stream = @intFromPtr(ctx.stream),
        };
        if (self.execute(&request) != 0) return error.LightOnAttentionExecutionFailed;
        self.calls += 1;
    }
};

const PrefillSdkTable136 = struct {
    var next_event: usize = 0;
    var next_device: usize = 0x1000;
    var next_host: usize = 0x2000;
    var reject_event: usize = 0;
    var fail_event_create: usize = 0;
    var reject_host: bool = false;
    var reject_device: bool = false;
    var host_frees: usize = 0;
    var device_frees: usize = 0;
    var event_frees: usize = 0;
    fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
        return driver.CUDA_SUCCESS;
    }
    fn synchronize(_: driver.CUstream) callconv(.c) driver.CUresult {
        return driver.CUDA_SUCCESS;
    }
    fn alloc(ptr: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
        next_device += 0x1000;
        ptr.* = next_device;
        return driver.CUDA_SUCCESS;
    }
    fn host(ptr: *?*anyopaque, _: usize) callconv(.c) driver.CUresult {
        next_host += 0x1000;
        ptr.* = @ptrFromInt(next_host);
        return driver.CUDA_SUCCESS;
    }
    fn event(ptr: *driver.CUevent, _: c_uint) callconv(.c) driver.CUresult {
        next_event += 1;
        if (next_event == fail_event_create) return 1;
        ptr.* = @ptrFromInt(next_event + 0x100);
        return driver.CUDA_SUCCESS;
    }
    fn freeHost(_: ?*anyopaque) callconv(.c) driver.CUresult {
        if (reject_host) return 1;
        host_frees += 1;
        return driver.CUDA_SUCCESS;
    }
    fn freeDevice(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
        if (reject_device) return 1;
        device_frees += 1;
        return driver.CUDA_SUCCESS;
    }
    fn destroy(value: driver.CUevent) callconv(.c) driver.CUresult {
        if (reject_event != 0 and @intFromPtr(value) == reject_event + 0x100) {
            reject_event = 0;
            return 1;
        }
        event_frees += 1;
        return driver.CUDA_SUCCESS;
    }
    fn errorText(_: driver.CUresult, out: *?[*:0]const u8) callconv(.c) driver.CUresult {
        out.* = "injected_prefill_sdk_error";
        return driver.CUDA_SUCCESS;
    }
    fn makeContext() context.CudaContext {
        next_event = 0;
        next_device = 0x1000;
        next_host = 0x2000;
        reject_event = 0;
        fail_event_create = 0;
        reject_host = false;
        reject_device = false;
        host_frees = 0;
        device_frees = 0;
        event_frees = 0;
        var ctx: context.CudaContext = undefined;
        ctx.observation.store(null, .release);
        ctx.observation_enable_failed.store(false, .release);
        ctx.ctx = @ptrFromInt(1);
        ctx.stream = null;
        ctx.stats = .{};
        ctx.device_allocations = .{};
        ctx.debug_graph_capture_active = false;
        ctx.driver.fns.cuCtxSetCurrent = current;
        ctx.driver.fns.cuStreamIsCapturing = null;
        ctx.driver.fns.cuStreamSynchronize = synchronize;
        ctx.driver.fns.cuMemAlloc = alloc;
        ctx.driver.fns.cuMemAllocHost = host;
        ctx.driver.fns.cuEventCreate = event;
        ctx.driver.fns.cuMemFree = freeDevice;
        ctx.driver.fns.cuMemFreeHost = freeHost;
        ctx.driver.fns.cuEventDestroy = destroy;
        ctx.driver.fns.cuGetErrorName = errorText;
        ctx.driver.fns.cuGetErrorString = errorText;
        return ctx;
    }
    fn recurrentWorkspace(_: *const anyopaque) callconv(.c) usize {
        return 64;
    }
    fn recurrentExecute(_: *const anyopaque, _: driver.CUdeviceptr, _: usize, _: c_int, _: driver.CUstream) callconv(.c) c_int {
        return 0;
    }
    fn plan(_: *const PlanRequest, _: *?*anyopaque) callconv(.c) c_int {
        return 0;
    }
    fn execute(_: *const RunRequest) callconv(.c) c_int {
        return 0;
    }
    fn drop(_: ?*anyopaque) callconv(.c) void {}
};

fn preparedPrefillToken136(domain: *@import("../../runtime/tier/admission_domain.zig").Domain, request: @import("prefill_workspace.zig").Request) !*@import("prepared_cache.zig").Funding {
    const F = @import("prepared_cache.zig");
    const funding = try F.Funding.create(.{ .requests = &.{.{ .external_prefill = request }} });
    errdefer funding.finishOutsideExecution();
    try funding.noteMiss(0, request.amounts(), 0);
    funding.admission = .{ .controller = &domain.controller, .limits = .{}, .check_live_memory = false };
    funding.state = .snapped;
    const pin = domain.pin();
    defer pin.release();
    try funding.fundOutsideExecution(&pin);
    return funding;
}

test "optional prefill attention checked SDK death retries each owner and holds quota to actual final B after publication A" {
    @import("../../test_error_logs.zig").expectErrorLogs(1); // injected event destroy check
    const Sink = @import("../../execution/retirement_sink.zig");
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    var gate: std.atomic.Mutex = .unlocked;
    const Probe = struct {
        gate: *std.atomic.Mutex,
        metadata: ?*std.atomic.Mutex = null,
        violation: bool = false,
        fn check(self: *@This()) void {
            if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
            if (self.metadata) |mutex| {
                if (mutex.tryLock()) mutex.unlock() else self.violation = true;
            }
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
    };
    var probe: Probe = .{ .gate = &gate };
    const domain = try Domain.create();
    defer domain.release();
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    defer domain.controller.configureResourceBudget(null) catch @panic("attention136 quota leak");
    const request = @import("prefill_workspace.zig").Request{ .kind = .attention, .library_identity = 1, .geometry_hash = 2, .device_bytes = device_bytes, .host_bytes = host_bytes + host_plan_bytes };
    const funding = try preparedPrefillToken136(domain, request);
    var funding_live = true;
    defer if (funding_live) {
        if (funding.state == .bound) funding.unbind();
        funding.finishOutsideExecution();
    };
    var reference = try Sink.Ref.create();
    defer reference.release();
    var publication = try reference.enter();
    defer publication.finish();
    var a = try publication.retainEpoch();
    defer a.release();
    try funding.bind(&a);
    var token = try funding.take(.{ .external_prefill = request }, request.amounts());
    var ctx = PrefillSdkTable136.makeContext();
    var module: Module = .{ .make_plan = PrefillSdkTable136.plan, .execute = PrefillSdkTable136.execute, .drop = PrefillSdkTable136.drop };
    probe.metadata = &module.metadata_mutex;
    defer module.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch @panic("attention136 SDK ownership leak");
    try module.reservePrepared(&ctx, &token, request, .{ .boundary = &a });
    funding.unbind();
    funding.finishOutsideExecution();
    funding_live = false;
    publication.finish();
    try std.testing.expectEqual(@as(usize, device_bytes), ctx.device_allocations.snapshot().live);
    var consumer = try reference.enter();
    defer consumer.finish();
    var b = try consumer.retainEpoch();
    defer b.release();
    try std.testing.expect(a.record != b.record or a.nonce != b.nonce);
    try std.testing.expect(gate.tryLock());
    var held = true;
    defer if (held) gate.unlock();
    PrefillSdkTable136.reject_event = 5;
    try std.testing.expectError(error.CudaDriverError, module.clearWithCleanup(&ctx, .{ .boundary = &b }));
    try std.testing.expect(module.failed and module.copy_events[0] == null and module.copy_events[4] != null and module.quota.hasLease());
    PrefillSdkTable136.reject_host = true;
    try std.testing.expectError(error.CudaDriverError, module.clearWithCleanup(&ctx, .{ .boundary = &b }));
    try std.testing.expect(module.failed and module.host.ptr != null and module.device.ptr != 0 and module.quota.hasLease());
    PrefillSdkTable136.reject_host = false;
    PrefillSdkTable136.reject_device = true;
    try std.testing.expectError(error.CudaDriverError, module.clearWithCleanup(&ctx, .{ .boundary = &b }));
    try std.testing.expect(module.failed and module.host.ptr == null and module.device.ptr != 0 and module.quota.hasLease());
    PrefillSdkTable136.reject_device = false;
    try module.clearWithCleanup(&ctx, .{ .boundary = &b });
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    try std.testing.expect(module.quota == .none and module.device.ptr == 0 and module.host.ptr == null);
    try std.testing.expectEqualDeep(request.amounts(), domain.controller.snapshot());
    try std.testing.expectEqual(@as(usize, metadata_slots), PrefillSdkTable136.event_frees);
    try std.testing.expectEqual(@as(usize, 1), PrefillSdkTable136.host_frees);
    try std.testing.expectEqual(@as(usize, 1), PrefillSdkTable136.device_frees);
    gate.unlock();
    held = false;
    consumer.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    try std.testing.expect(!probe.violation);
}

test "optional recurrent prefill checked SDK free retains exact quota and closing blocks new births" {
    const Sink = @import("../../execution/retirement_sink.zig");
    const domain = try @import("../../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var ctx = PrefillSdkTable136.makeContext();
    const R = @import("lighton_prefill.zig").Module;
    var module: R = .{ .library = undefined, .workspace = PrefillSdkTable136.recurrentWorkspace, .execute = PrefillSdkTable136.recurrentExecute };
    defer module.clearWorkspaceWithCleanup(&ctx, .{ .outside_execution = true }) catch @panic("recurrent136 SDK owner leak");
    const request = @import("prefill_workspace.zig").Request{ .kind = .recurrent, .library_identity = 1, .geometry_hash = 2, .device_bytes = 64 };
    const funding = try preparedPrefillToken136(domain, request);
    defer {
        if (funding.state == .bound) funding.unbind();
        funding.finishOutsideExecution();
    }
    var ref = try Sink.Ref.create();
    defer ref.release();
    var publication = try ref.enter();
    defer publication.finish();
    var a = try publication.retainEpoch();
    defer a.release();
    try funding.bind(&a);
    var token = try funding.take(.{ .external_prefill = request }, request.amounts());
    _ = try module.reservePrepared(&ctx, &token, request, .{ .boundary = &a });
    funding.unbind();
    publication.finish();
    var consumer = try ref.enter();
    defer consumer.finish();
    var b = try consumer.retainEpoch();
    defer b.release();
    PrefillSdkTable136.reject_device = true;
    try std.testing.expectError(error.CudaDriverError, module.clearWorkspaceWithCleanup(&ctx, .{ .boundary = &b }));
    try std.testing.expect(module.scratch.ptr != 0 and module.quota.hasLease());
    try std.testing.expectEqual(@as(usize, 64), ctx.device_allocations.snapshot().live);
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_scratch_bytes);
    PrefillSdkTable136.reject_device = false;
    try module.clearWorkspaceWithCleanup(&ctx, .{ .boundary = &b });
    try std.testing.expect(module.scratch.ptr == 0 and module.quota == .none);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_scratch_bytes);
    consumer.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    module.closing = true;
    try std.testing.expectError(error.RecurrentPrefillClosing, module.reserve(&ctx, 64));
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
}

test "optional prefill partial event-creation failure retains failed SDK storage until checked retry under current B" {
    @import("../../test_error_logs.zig").expectErrorLogs(1); // injected create, checked unwind failure is silent
    const Sink = @import("../../execution/retirement_sink.zig");
    const domain = try @import("../../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var ctx = PrefillSdkTable136.makeContext();
    var module: Module = .{ .make_plan = PrefillSdkTable136.plan, .execute = PrefillSdkTable136.execute, .drop = PrefillSdkTable136.drop };
    defer module.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch @panic("partial attention136 SDK owner leak");
    const request = @import("prefill_workspace.zig").Request{ .kind = .attention, .library_identity = 1, .geometry_hash = 2, .device_bytes = device_bytes, .host_bytes = host_bytes + host_plan_bytes };
    const funding = try preparedPrefillToken136(domain, request);
    defer {
        if (funding.state == .bound) funding.unbind();
        funding.finishOutsideExecution();
    }
    var ref = try Sink.Ref.create();
    defer ref.release();
    var publication = try ref.enter();
    defer publication.finish();
    var a = try publication.retainEpoch();
    defer a.release();
    try funding.bind(&a);
    var token = try funding.take(.{ .external_prefill = request }, request.amounts());
    PrefillSdkTable136.fail_event_create = 3;
    PrefillSdkTable136.reject_device = true;
    try std.testing.expectError(error.CudaDriverError, module.reservePrepared(&ctx, &token, request, .{ .boundary = &a }));
    try std.testing.expect(token.owner == null and module.failed and module.device.ptr != 0 and module.host.ptr == null and module.quota.hasLease());
    try std.testing.expectEqual(@as(usize, 2), PrefillSdkTable136.event_frees);
    try std.testing.expectEqualDeep(request.amounts(), domain.controller.snapshot());
    funding.unbind();
    publication.finish();
    var consumer = try ref.enter();
    defer consumer.finish();
    var b = try consumer.retainEpoch();
    defer b.release();
    PrefillSdkTable136.reject_device = false;
    try module.clearWithCleanup(&ctx, .{ .boundary = &b });
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    try std.testing.expectEqualDeep(request.amounts(), domain.controller.snapshot());
    consumer.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "optional prefill attention shared cold and actual packet eligibility preserves boundaries and rejects overflow geometry" {
    const shape = Shape{ .batch = 1, .rows = 2, .tokens = 3, .total_tokens = 3, .kv_position_offset = 0, .sliding_window = 0, .mask_len = 0, .bias_mode = 0, .heads = 8, .kv_heads = 2, .head_dim = 256, .page_size = 16, .key_f16 = true, .value_f16 = true, .key_row_bytes = 1024, .base_key_row_bytes = 1024, .value_row_bytes = 1024 };
    try std.testing.expect(shape.supported());
    inline for (.{ "batch", "kv_position_offset", "sliding_window", "mask_len", "bias_mode", "head_dim", "page_size", "key_row_bytes", "base_key_row_bytes", "value_row_bytes" }) |field| {
        var bad = shape;
        @field(bad, field) += 1;
        try std.testing.expect(!bad.supported());
    }
    var bad = shape;
    bad.rows = 1;
    try std.testing.expect(!bad.supported());
    bad = shape;
    bad.rows = 4097;
    try std.testing.expect(!bad.supported());
    bad = shape;
    bad.tokens = 4097;
    bad.total_tokens = 4097;
    try std.testing.expect(!bad.supported());
    bad = shape;
    bad.value_f16 = false;
    try std.testing.expect(!bad.supported());
    bad = shape;
    bad.kv_heads = std.math.maxInt(usize);
    bad.head_dim = std.math.maxInt(usize);
    try std.testing.expect(!bad.supported()); // rejects before byte-stride arithmetic
}

test "optional attention cold shape refuses fewer live keys than query rows" {
    var shape: Shape = .{ .batch = 1, .rows = 16, .tokens = 16, .total_tokens = 16, .kv_position_offset = 0, .sliding_window = 0, .mask_len = 0, .bias_mode = 0, .heads = 8, .kv_heads = 2, .head_dim = 256, .page_size = 16, .key_f16 = true, .value_f16 = true, .key_row_bytes = 1024, .base_key_row_bytes = 1024, .value_row_bytes = 1024 };
    try std.testing.expect(shape.supported());
    shape.tokens = 15;
    shape.total_tokens = 15;
    try std.testing.expect(!shape.supported());
    shape.tokens = 0;
    shape.total_tokens = 0;
    try std.testing.expect(!shape.supported());
}
