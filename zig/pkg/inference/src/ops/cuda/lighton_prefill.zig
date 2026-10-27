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

pub const Module = struct {
    library: std.DynLib,
    attention: ?@import("lighton_prefill_attention.zig").Module = null,
    workspace: *const fn (*const anyopaque) callconv(.c) usize,
    execute: *const fn (*const anyopaque, driver.CUdeviceptr, usize, c_int, driver.CUstream) callconv(.c) c_int,
    scratch: buffer.DeviceBuffer = .{},
    admission: ?memory.AdmissionLease = null,
    calls: usize = 0,
    workspace_peak: usize = 0,

    pub fn init(path: []const u8) !Module {
        var library = try std.DynLib.open(path);
        errdefer library.close();
        const abi = library.lookup(*const fn () callconv(.c) u64, "antfly_lighton_prefill_abi") orelse return error.LightOnPrefillAbiMismatch;
        if (abi() != (@as(u64, 1) << 32 | 2408)) return error.LightOnPrefillAbiMismatch;
        return .{
            .library = library,
            .attention = try @import("lighton_prefill_attention.zig").Module.init(&library),
            .workspace = library.lookup(*const fn (*const anyopaque) callconv(.c) usize, "antfly_lighton_prefill_workspace") orelse return error.LightOnPrefillAbiMismatch,
            .execute = library.lookup(*const fn (*const anyopaque, driver.CUdeviceptr, usize, c_int, driver.CUstream) callconv(.c) c_int, "antfly_lighton_prefill_execute") orelse return error.LightOnPrefillAbiMismatch,
        };
    }

    pub fn clearWorkspace(self: *Module, ctx: *context.CudaContext) !void {
        if (self.attention) |*attention| try attention.clear(ctx);
        try self.clearRecurrentWorkspace(ctx);
    }

    fn clearRecurrentWorkspace(self: *Module, ctx: *context.CudaContext) !void {
        if (self.scratch.ptr != 0) {
            try ctx.synchronize();
            self.scratch.free(ctx);
        }
        if (self.admission) |*lease| lease.release();
        self.admission = null;
    }

    /// This working set is additional to the existing epoch envelopes and
    /// keeps an independent reservation until its last GPU consumer retires.
    pub fn reserve(self: *Module, ctx: *context.CudaContext, bytes: usize) !buffer.DeviceBuffer {
        if (bytes == 0) return .{};
        if (self.scratch.len >= bytes) return self.scratch;
        try self.clearRecurrentWorkspace(ctx);
        var lease: ?memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy| {
            lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{ .backend_scratch_bytes = bytes }, policy.check_live_memory);
        }
        errdefer if (lease) |*active| active.release();
        self.scratch = try buffer.DeviceBuffer.alloc(ctx, bytes);
        self.admission = lease;
        return self.scratch;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.clearWorkspace(ctx) catch {};
        self.library.close();
        self.* = undefined;
    }

    pub fn workspaceBytes(self: *const Module, packet: *const anyopaque) !usize {
        const bytes = self.workspace(packet);
        if (bytes == std.math.maxInt(usize)) return error.InvalidTensorShape;
        return bytes;
    }

    pub fn run(self: *Module, packet: *const anyopaque, scratch: driver.CUdeviceptr, bytes: usize, device: c_int, stream: driver.CUstream) !void {
        // No fallback is safe after the first queued state update.
        if (self.execute(packet, scratch, bytes, device, stream) != 0) return error.LightOnPrefillExecutionFailed;
        self.calls += 1;
        self.workspace_peak = @max(self.workspace_peak, bytes);
    }
};
