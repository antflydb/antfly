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

const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");

pub const Error = driver_mod.Error || error{ CudaGraphUnavailable, CudaGraphCaptureFailed };

pub const CapturedGraph = struct {
    graph: driver_mod.CUgraph = null,
    exec: driver_mod.CUgraphExec = null,

    pub fn instantiate(ctx: *context_mod.CudaContext, graph: driver_mod.CUgraph) Error!CapturedGraph {
        const exec = try ctx.instantiateGraph(graph);
        return .{ .graph = graph, .exec = exec };
    }

    pub fn launch(self: *const CapturedGraph, ctx: *context_mod.CudaContext) Error!void {
        if (self.exec == null) return error.InvalidCudaState;
        try ctx.launchGraph(self.exec);
    }

    /// Each successful SDK destruction consumes only its own handle. A failed
    /// handle remains owned for an exact retry; address owners must remain live.
    pub fn deinitChecked(self: *CapturedGraph, ctx: *context_mod.CudaContext) !void {
        try ctx.destroyGraphExecChecked(&self.exec);
        try ctx.destroyGraphChecked(&self.graph);
    }
    pub fn deinit(self: *CapturedGraph, ctx: *context_mod.CudaContext) void {
        self.deinitChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
};

pub fn available(ctx: *const context_mod.CudaContext) bool {
    return ctx.driver.fns.cuMemAllocHost != null and
        ctx.driver.fns.cuMemFreeHost != null;
}

pub fn beginCapture(ctx: *context_mod.CudaContext) Error!void {
    try ctx.beginStreamCapture(driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED);
}

pub fn endCapture(ctx: *context_mod.CudaContext) Error!CapturedGraph {
    var owner = CapturedGraph{
        .graph = ctx.endStreamCapture() catch |err| {
            // Caller error defers may retire captured addresses. An SDK error alone
            // does not prove recording ended; refuse all unwind while still active
            // or while the actual stream status cannot be established.
            requireCaptureEnded(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            return err;
        },
    };
    errdefer owner.deinit(ctx);
    owner.exec = try ctx.instantiateGraph(owner.graph);
    return owner;
}

pub fn requireCaptureEnded(ctx: *context_mod.CudaContext) !void {
    // CudaContext.streamCaptureActive intentionally has a legacy debug-flag
    // fallback. That is not physical end-capture proof after a failed SDK call.
    const query = ctx.driver.fns.cuStreamIsCapturing orelse return error.CudaSymbolMissing;
    try ctx.makeCurrent();
    var status: driver_mod.CUstreamCaptureStatus = driver_mod.CU_STREAM_CAPTURE_STATUS_NONE;
    try ctx.driver.check(query(ctx.stream, &status));
    if (status != driver_mod.CU_STREAM_CAPTURE_STATUS_NONE) return error.CudaGraphCaptureStillActive;
}

test "CUDA allocation failed end capture requires actual inactive stream before caller owner unwind" {
    const std = @import("std");
    const Mock = struct {
        var active: bool = true;
        var queries: usize = 0;
        var ends: usize = 0;
        fn current(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn status(_: driver_mod.CUstream, out: *driver_mod.CUstreamCaptureStatus) callconv(.c) driver_mod.CUresult {
            queries += 1;
            out.* = if (active) driver_mod.CU_STREAM_CAPTURE_STATUS_ACTIVE else driver_mod.CU_STREAM_CAPTURE_STATUS_NONE;
            return 0;
        }
        fn end(_: driver_mod.CUstream, out: *driver_mod.CUgraph) callconv(.c) driver_mod.CUresult {
            ends += 1;
            out.* = null;
            return 700;
        }
        fn text(_: driver_mod.CUresult, out: *?[*:0]const u8) callconv(.c) driver_mod.CUresult {
            out.* = "injected end capture failure";
            return 0;
        }
    };
    var ctx: context_mod.CudaContext = undefined;
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamIsCapturing = Mock.status;
    ctx.driver.fns.cuStreamEndCapture = Mock.end;
    ctx.driver.fns.cuGetErrorName = Mock.text;
    ctx.driver.fns.cuGetErrorString = Mock.text;
    Mock.active = true;
    Mock.queries = 0;
    Mock.ends = 0;
    // Test the fallible SDK proof below the failclosed boundary without exiting
    // the test worker. Active or unavailable status cannot grant safe unwind.
    try std.testing.expectError(error.CudaGraphCaptureStillActive, requireCaptureEnded(&ctx));
    ctx.driver.fns.cuStreamIsCapturing = null;
    try std.testing.expectError(error.CudaSymbolMissing, requireCaptureEnded(&ctx));
    ctx.driver.fns.cuStreamIsCapturing = Mock.status;
    Mock.active = false;
    @import("../../test_error_logs.zig").expectErrorLogs(1);
    try std.testing.expectError(error.CudaDriverError, endCapture(&ctx));
    try std.testing.expectEqual(@as(usize, 1), Mock.ends);
    try std.testing.expectEqual(@as(usize, 2), Mock.queries);
}
