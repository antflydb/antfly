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
const CudaContext = @import("context.zig").CudaContext;
const driver = @import("driver.zig");
const DeviceBuffer = @import("buffer.zig").DeviceBuffer;
const Cublas = @import("libraries.zig").Cublas;

/// A separate stream and BLAS handle warm the primary context's provider.
/// Request execution can use the portable kernels during library startup.
pub const Warmup = struct {
    const State = enum(u8) { idle, starting, ready, unavailable };

    state: std.atomic.Value(State) = .init(.idle),
    thread: ?std.Thread = null,

    pub fn start(self: *Warmup, context: *const CudaContext) void {
        std.debug.assert(self.thread == null);
        self.state.store(.starting, .release);
        self.thread = std.Thread.spawn(.{}, run, .{ self, context.ctx }) catch {
            self.state.store(.unavailable, .release);
            return;
        };
    }

    pub fn needsPortable(self: *const Warmup) bool {
        return switch (self.state.load(.acquire)) {
            .starting, .unavailable => true,
            .idle, .ready => false,
        };
    }

    pub fn deinit(self: *Warmup) void {
        if (self.thread) |thread| thread.join();
        self.thread = null;
    }

    fn run(self: *Warmup, primary: driver.CUcontext) void {
        prepare(primary) catch |err| {
            std.log.debug("CUDA graph BLAS background warmup unavailable: {}", .{err});
            self.state.store(.unavailable, .release);
            return;
        };
        self.state.store(.ready, .release);
    }

    fn prepare(primary: driver.CUcontext) !void {
        var ctx = try CudaContext.initDefault();
        defer ctx.deinit();
        if (ctx.ctx != primary) return error.ForeignCudaContext;
        var blas = try Cublas.initWithLibrary(ctx.stream, null);
        defer blas.deinit();

        // The portable split reduction owns the first four rows. Prime
        // the vendor small-GEMM schedule that takes over above that range.
        const rows = 8;
        const width = 512;
        const columns = 512;
        const lhs_bytes = rows * width * @sizeOf(f32);
        const rhs_bytes = width * columns * @sizeOf(f32);
        const output_bytes = rows * columns * @sizeOf(f32);

        var scratch = try DeviceBuffer.alloc(&ctx, lhs_bytes + rhs_bytes + output_bytes);
        defer scratch.free(&ctx);
        const zero_inputs = try std.heap.page_allocator.alloc(u8, lhs_bytes + rhs_bytes);
        defer std.heap.page_allocator.free(zero_inputs);
        @memset(zero_inputs, 0);
        // Even if the API rejects its submission, keep the scratch owned
        // until the dedicated stream has completed any accepted work.
        errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        try scratch.copyFromHost(&ctx, zero_inputs);
        try blas.matrix(ctx.stream, @ptrFromInt(scratch.ptr + lhs_bytes + rhs_bytes), @ptrFromInt(scratch.ptr), @ptrFromInt(scratch.ptr + lhs_bytes), rows, width, columns, false, false, false);
        try ctx.synchronize();
    }
};
