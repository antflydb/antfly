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
const device_bytes = float_bytes + int_bytes + q_bytes + meta_bytes;
pub const metadata_slots = 64;
const slot_bytes = int_bytes + meta_bytes;
const host_bytes = metadata_slots * slot_bytes;
const host_plan_bytes = 64 * 1024;

pub const Module = struct {
    make_plan: *const fn (*const PlanRequest, *?*anyopaque) callconv(.c) c_int,
    execute: *const fn (*const RunRequest) callconv(.c) c_int,
    drop: *const fn (?*anyopaque) callconv(.c) void,
    device: buffer.DeviceBuffer = .{},
    host: buffer.HostBuffer = .{},
    admission: ?memory.AdmissionLease = null,
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
        if (self.device.ptr != 0) return;
        var lease: ?memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy|
            lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{
                .backend_scratch_bytes = device_bytes,
                .host_scratch_bytes = host_bytes + host_plan_bytes,
            }, policy.check_live_memory);
        errdefer if (lease) |*value| value.release();
        var device = try buffer.DeviceBuffer.alloc(ctx, device_bytes);
        errdefer device.free(ctx);
        var host = try buffer.HostBuffer.alloc(ctx, host_bytes);
        errdefer host.free(ctx);
        var events: [metadata_slots]driver.CUevent = @splat(null);
        errdefer for (events) |event| ctx.destroyEvent(event);
        for (&events) |*event| event.* = try ctx.createStreamEvent();
        self.device = device;
        self.host = host;
        self.copy_events = events;
        self.admission = lease;
    }

    pub fn clear(self: *Module, ctx: *context.CudaContext) !void {
        if (self.device.ptr != 0) try ctx.synchronize();
        self.drop(self.plan);
        self.plan = null;
        self.plan_key = @splat(0);
        for (self.copy_events) |event| ctx.destroyEvent(event);
        self.copy_events = @splat(null);
        self.copy_pending = @splat(false);
        self.next_slot = 0;
        self.staged_slot = null;
        self.failed = false;
        self.host.free(ctx);
        self.device.free(ctx);
        if (self.admission) |*lease| lease.release();
        self.admission = null;
    }

    /// One owner submits on one stream. A slot cannot be borrowed twice or
    /// overwritten until its last DMA reader retires. A frame may enqueue all
    /// selected prefix/layer packets before wrapping this bounded ring.
    pub fn acquireSlot(self: *Module, ctx: *context.CudaContext) !usize {
        if (self.failed or self.device.ptr == 0 or self.staged_slot != null) return error.InvalidExecutionPlan;
        const index = self.next_slot;
        if (self.copy_pending[index]) {
            try ctx.makeCurrent();
            try ctx.driver.check(ctx.driver.fns.cuEventSynchronize(self.copy_events[index]));
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
        if (self.failed) return error.LightOnAttentionExecutionFailed;
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
