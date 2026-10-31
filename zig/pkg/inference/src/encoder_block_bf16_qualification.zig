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

//! Qualification-only C ABI; executes the actual serving Module.block.
const std = @import("std");
const context = @import("ops/cuda/context.zig");
const buffer = @import("ops/cuda/buffer.zig");
const cublas = @import("ops/cuda/cublaslt.zig");
const cudnn = @import("ops/cuda/cudnn_sdpa.zig");
const vision = @import("ops/cuda/encoder_block_bf16.zig");
const Packet = extern struct {
    input: u64,
    output: u64,
    weights: [12]u64,
    positions: u64,
    rows: u32,
    hidden: u32,
    intermediate: u32,
    batch: u32,
    epsilon: f32,
    theta: f32,
};
const State = struct {
    ctx: context.CudaContext,
    blas: cublas.CublasLt,
    attention: cudnn.Module,
    module: vision.Module = .{},
};
var state: ?State = null;
pub export fn antfly_vision_block_abi() callconv(.c) u64 {
    return (@as(u64, 1) << 32) | @sizeOf(Packet);
}
fn initialize() !void {
    if (state != null) return;
    var ctx = try context.CudaContext.initDefault();
    errdefer ctx.deinit();
    var blas = try cublas.CublasLt.openWithAllocator(std.heap.c_allocator, &ctx);
    errdefer blas.deinit();
    try blas.setPlanCacheCapacity(1024);
    const attention = try cudnn.Module.init(ctx.stream);
    state = .{ .ctx = ctx, .blas = blas, .attention = attention };
}
pub export fn antfly_vision_block_init() callconv(.c) c_int {
    initialize() catch |err| {
        std.debug.print("vision qualification init error={s}\n", .{@errorName(err)});
        return -1;
    };
    return 0;
}
fn execute(p: *const Packet) !void {
    const s = if (state) |*value| value else return error.Uninitialized;
    const rows: usize = p.rows;
    const hidden: usize = p.hidden;
    const intermediate: usize = p.intermediate;
    _ = try vision.Layout.init(rows, hidden, intermediate, 0);
    if (p.batch == 0 or rows % p.batch != 0) return error.InvalidShape;
    const key = cudnn.Key{ .batch = p.batch, .sequence = rows / p.batch, .q_heads = hidden / 64, .kv_heads = hidden / 64, .dim = 64, .scale = 0.125, .ocr_vision = true, .dtype = .bf16 };
    const workspace = (try s.attention.workspaceBytesFor(key)) orelse return error.CudnnFailure;
    var vendor = try buffer.DeviceBuffer.alloc(&s.ctx, workspace);
    defer vendor.free(&s.ctx);
    const layout = try vision.Layout.init(rows, hidden, intermediate, 0);
    var arena = try buffer.DeviceBuffer.alloc(&s.ctx, layout.bytes);
    defer arena.free(&s.ctx);
    defer s.ctx.synchronize() catch {};
    const lengths = [12]usize{ hidden * 4, hidden * 4, 3 * hidden * hidden * 2, 3 * hidden * 2, hidden * hidden * 2, hidden * 2, hidden * 4, hidden * 4, hidden * intermediate * 2, intermediate * 2, intermediate * hidden * 2, hidden * 2 };
    var weights: [12]buffer.DeviceBuffer = undefined;
    for (&weights, p.weights, lengths) |*w, ptr, len| w.* = .{ .ptr = ptr, .len = len, .borrowed = true };
    try s.module.block(&s.ctx, &s.blas, &s.attention, key, arena, layout, .{}, .{ .input = .{ .ptr = p.input, .len = rows * hidden * 2, .borrowed = true }, .output = .{ .ptr = p.output, .len = rows * hidden * 2, .borrowed = true }, .weights = weights, .positions = .{ .ptr = p.positions, .len = rows * 8, .borrowed = true }, .rows = rows, .hidden = hidden, .intermediate = intermediate, .batch = p.batch, .epsilon = p.epsilon, .theta = p.theta, .attention_workspace = vendor });
    try s.ctx.synchronize();
}
pub export fn antfly_vision_block_run(packet: ?*const Packet) callconv(.c) c_int {
    execute(packet orelse return -2) catch |err| {
        std.debug.print("vision qualification execute error={s}\n", .{@errorName(err)});
        return -1;
    };
    return 0;
}
pub export fn antfly_vision_block_deinit() callconv(.c) void {
    if (state) |*s| {
        s.ctx.synchronize() catch return;
        s.module.deinit(&s.ctx);
        s.attention.deinit();
        s.blas.deinit();
        s.ctx.deinit();
        state = null;
    }
}
