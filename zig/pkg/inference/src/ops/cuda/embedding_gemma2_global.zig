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

//! Bounded global MQA with disjoint packed matrices. No batched output views
//! overlap another head's physical storage, even when the source is BSHD.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const Blas = @import("cublaslt.zig").CublasLt;
const Encoder = @import("encoder_multimodal.zig");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const memory = @import("../../runtime/tier/memory.zig");

/// Smaller long-input tiles keep score/probability traffic within the L4 cache.
/// Shorter inputs benefit from fewer library launches with 256-row tiles.
pub fn queryTile(sequence: usize) usize {
    return if (sequence >= 4096) 128 else 256;
}

/// Retained workspace has its own shared admission reservation; a request's
/// active workspace reservation must not disappear while these bytes stay live.
pub const Scratch = struct {
    buffer: Buffer = .{},
    lease: ?memory.AdmissionLease = null,
    generation: u64 = 0,

    pub fn clear(self: *Scratch, ctx: *Context) !void {
        if (self.buffer.ptr != 0) try ctx.synchronize();
        _ = try self.buffer.freeChecked(ctx);
        if (self.lease) |*lease| lease.release();
        // Cached graphs can outlive this allocation, including failed growth.
        // Preserve its generation so an allocator reusing the address cannot
        // make a later, smaller allocation appear to be the old workspace.
        self.* = .{ .generation = self.generation };
    }

    pub fn acquire(self: *Scratch, ctx: *Context, bytes: usize) !?Buffer {
        if (bytes == 0) return null;
        if (self.buffer.len >= bytes) return .{ .ptr = self.buffer.ptr, .len = bytes, .borrowed = true };
        try self.clear(ctx);
        var lease: ?memory.AdmissionLease = if (ctx.video_cache_budget.admission) |admission|
            admission.controller.tryAcquire(.gpu, admission.limits, .{ .backend_scratch_bytes = bytes }, admission.check_live_memory) catch return null
        else
            null;
        const buffer = Buffer.alloc(ctx, bytes) catch |err| {
            if (lease) |*active| active.release();
            if (err == error.CudaMemoryLimitExceeded) return null;
            return err;
        };
        self.* = .{ .buffer = buffer, .lease = lease, .generation = self.generation +% 1 };
        return .{ .ptr = buffer.ptr, .len = bytes, .borrowed = true };
    }

    pub fn releaseAfterContextDestroyed(self: *Scratch) void {
        if (self.lease) |*lease| lease.release();
        self.* = .{ .generation = self.generation };
    }
};

pub const Layout = struct {
    capacity: usize,
    scores_bytes: usize,
    probabilities_bytes: usize,
    packed_bytes: usize,
    total_bytes: usize,
    pub fn init(sequence: usize, tile: usize) !Layout {
        if (sequence == 0 or sequence > 8192 or tile == 0 or tile > 512 or tile % 32 != 0) return error.InvalidShape;
        const cap = @min(tile, std.mem.alignForward(usize, sequence, 32));
        const elements = 4 * cap * sequence;
        const packed_size = 4 * cap * 512 * 2;
        return .{ .capacity = cap, .scores_bytes = elements * 4, .probabilities_bytes = elements * 2, .packed_bytes = packed_size, .total_bytes = elements * 6 + 2 * packed_size };
    }
};
fn view(buffer: Buffer, offset: usize, len: usize) !Buffer {
    if (offset > buffer.len or len > buffer.len - offset) return error.InvalidShape;
    return .{ .ptr = buffer.ptr + offset, .len = len, .borrowed = true };
}

fn disjoint(a: Buffer, b: Buffer) bool {
    if (a.len == 0 or b.len == 0) return true;
    const ae = std.math.add(u64, a.ptr, a.len) catch return false;
    const be = std.math.add(u64, b.ptr, b.len) catch return false;
    return ae <= b.ptr or be <= a.ptr;
}

/// Caller owns scratch/workspace until stream completion. Input/output must be
/// disjoint; Q packing and output unpacking use only a bounded query tile.
pub fn execute(ctx: *Context, module: *Encoder.Module, blas: *Blas, output: Buffer, q: Buffer, k: Buffer, v: Buffer, mask: Buffer, sequence: usize, scratch: Buffer, workspace: Buffer, tile: usize, control: ?Control) !void {
    const layout = try Layout.init(sequence, tile);
    const query_bytes = sequence * 4 * 512 * 2;
    const kv_bytes = sequence * 512 * 2;
    if (output.len < query_bytes or q.len < query_bytes or k.len < kv_bytes or v.len < kv_bytes or scratch.len < layout.total_bytes or (mask.ptr != 0 and mask.len < sequence * 8)) return error.InvalidShape;
    if (output.ptr == 0 or q.ptr == 0 or k.ptr == 0 or v.ptr == 0 or scratch.ptr == 0) return error.InvalidShape;
    for ([_]Buffer{ q, k, v, mask, workspace }) |input| {
        if (!disjoint(output, input) or !disjoint(scratch, input)) return error.InvalidShape;
    }
    if (!disjoint(output, scratch)) return error.InvalidShape;
    const scores = try view(scratch, 0, layout.scores_bytes);
    const probabilities = try view(scratch, layout.scores_bytes, layout.probabilities_bytes);
    const packed_q = try view(scratch, layout.scores_bytes + layout.probabilities_bytes, layout.packed_bytes);
    const packed_output = try view(scratch, layout.scores_bytes + layout.probabilities_bytes + layout.packed_bytes, layout.packed_bytes);
    var begin: usize = 0;
    while (begin < sequence) : (begin += layout.capacity) {
        if (control) |value| try value.check();
        const rows = @min(layout.capacity, sequence - begin);
        const source = try view(q, begin * 4 * 512 * 2, rows * 4 * 512 * 2);
        const destination = try view(output, begin * 4 * 512 * 2, rows * 4 * 512 * 2);
        try module.launchTrainingPackBhsdBf16(ctx, packed_q, source, 1, @intCast(rows), 4, 4, 512, false);
        // [H, rows, D] @ [D, S]; K is broadcast, destination heads disjoint.
        try blas.matmulEmbeddingGemma2Attention(ctx, scores, packed_q, k, workspace, rows, sequence, 512, 512, 512, sequence, 4, rows * 512, 0, layout.capacity * sequence, false, true, true, 1);
        module.noteCublasLtMatmul();
        try module.launchGlobalAttentionSoftmax(ctx, probabilities, scores, mask, @intCast(sequence), @intCast(begin), @intCast(rows), 0, 4, @intCast(layout.capacity));
        // [H, rows, S] @ [S, D] -> [H, rows, D], never interleaved batches.
        try blas.matmulEmbeddingGemma2Attention(ctx, packed_output, probabilities, v, workspace, rows, 512, sequence, sequence, 512, 512, 4, layout.capacity * sequence, 0, rows * 512, false, false, false, 1);
        module.noteCublasLtMatmul();
        try module.launchTrainingUnpackBhsdBf16(ctx, destination, packed_output, 1, @intCast(rows), 4, 4, 512, false);
    }
}

test "global MQA tile scratch is bounded and aligned for odd lengths" {
    const largest = try Layout.init(8192, 256);
    try std.testing.expectEqual(@as(usize, 50 * 1024 * 1024), largest.total_bytes);
    for ([_]usize{ 1, 17, 31, 32, 65, 257, 513, 8191, 8192 }) |n| {
        const l = try Layout.init(n, 256);
        try std.testing.expect(l.total_bytes <= largest.total_bytes);
        try std.testing.expectEqual(@as(usize, 0), l.scores_bytes % 256);
        try std.testing.expectEqual(@as(usize, 0), l.probabilities_bytes % 256);
        try std.testing.expectEqual(@as(usize, 0), l.packed_bytes % 256);
    }
    try std.testing.expectError(error.InvalidShape, Layout.init(8193, 256));
    try std.testing.expectError(error.InvalidShape, Layout.init(0, 256));
    try std.testing.expectError(error.InvalidShape, Layout.init(512, 513));
}

test "global MQA qualified tiles cap scratch below 26 MiB" {
    for (1..8193) |sequence| {
        const layout = try Layout.init(sequence, queryTile(sequence));
        try std.testing.expect(layout.total_bytes <= 26 * 1024 * 1024);
    }
    try std.testing.expectEqual(@as(usize, 256), queryTile(2048));
    try std.testing.expectEqual(@as(usize, 128), queryTile(4096));
    try std.testing.expectEqual(@as(usize, 25 * 1024 * 1024), (try Layout.init(8192, queryTile(8192))).total_bytes);
}

test "global MQA rejects overlap and pointer overflow before GPU execution" {
    try std.testing.expect(disjoint(.{ .ptr = 0x1000, .len = 16 }, .{ .ptr = 0x1010, .len = 16 }));
    try std.testing.expect(!disjoint(.{ .ptr = 0x1000, .len = 32 }, .{ .ptr = 0x1010, .len = 16 }));
    try std.testing.expect(!disjoint(.{ .ptr = std.math.maxInt(u64) - 1, .len = 32 }, .{ .ptr = 0x1010, .len = 16 }));
}

test "global MQA retained scratch shares admission and releases failed growth" {
    const Mock = struct {
        var next: usize = 256;
        fn current(_: @import("driver.zig").CUcontext) callconv(.c) c_int {
            return 0;
        }
        fn sync(_: @import("driver.zig").CUstream) callconv(.c) c_int {
            return 0;
        }
        fn alloc(ptr: *u64, _: usize) callconv(.c) c_int {
            ptr.* = next;
            next += 256;
            return 0;
        }
        fn free(_: u64) callconv(.c) c_int {
            return 0;
        }
    };
    var controller = memory.AdmissionController{};
    var ctx = Context{ .driver = undefined, .ctx = @ptrFromInt(1), .stream = @ptrFromInt(2), .device = 0, .info = .{} };
    ctx.device_allocations = .{ .limit = 4096 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1024 }, .check_live_memory = false } };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    var first = Scratch{};
    defer first.clear(&ctx) catch unreachable;
    var second = Scratch{};
    defer second.clear(&ctx) catch unreachable;
    const initial = (try first.acquire(&ctx, 512)).?;
    try std.testing.expectEqual(initial.ptr, (try first.acquire(&ctx, 256)).?.ptr);
    try std.testing.expectEqual(@as(usize, 512), controller.snapshot().backend_scratch_bytes);
    try std.testing.expect((try second.acquire(&ctx, 768)) == null);
    try std.testing.expectEqual(@as(usize, 512), ctx.device_allocations.snapshot().live);
    try std.testing.expect((try first.acquire(&ctx, 2048)) == null);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    ctx.device_allocations.limit = 128;
    try std.testing.expect((try first.acquire(&ctx, 512)) == null);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
}

test "global MQA failed growth and reused address invalidate captured workspace" {
    const Mock = struct {
        fn current(_: @import("driver.zig").CUcontext) callconv(.c) c_int {
            return 0;
        }
        fn sync(_: @import("driver.zig").CUstream) callconv(.c) c_int {
            return 0;
        }
        fn alloc(ptr: *u64, _: usize) callconv(.c) c_int {
            ptr.* = 0x1000;
            return 0;
        }
        fn free(_: u64) callconv(.c) c_int {
            return 0;
        }
    };
    var controller = memory.AdmissionController{};
    var ctx = Context{ .driver = undefined, .ctx = @ptrFromInt(1), .stream = @ptrFromInt(2), .device = 0, .info = .{} };
    ctx.device_allocations = .{ .limit = 4096 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1024 }, .check_live_memory = false } };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    var scratch = Scratch{};
    defer scratch.clear(&ctx) catch unreachable;
    const old = (try scratch.acquire(&ctx, 512)).?;
    const old_generation = scratch.generation;
    var graph = @import("../../backends/embedding_gemma2_cuda.zig").Arena{};
    graph.setAttentionWorkspaces(0, old.ptr, old_generation, 0);
    // Ordinary reuse must preserve the key for a live allocation.
    _ = (try scratch.acquire(&ctx, 256)).?;
    try std.testing.expect(graph.attentionWorkspacesMatch(0, scratch.buffer.ptr, scratch.generation, 0));
    // Admission denial retires the old allocation. The driver then gives a
    // smaller allocation exactly the same address, reproducing the ABA case.
    try std.testing.expect((try scratch.acquire(&ctx, 2048)) == null);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    const smaller = (try scratch.acquire(&ctx, 256)).?;
    try std.testing.expectEqual(old.ptr, smaller.ptr);
    try std.testing.expect(smaller.len < old.len);
    try std.testing.expect(scratch.generation != old_generation);
    try std.testing.expect(!graph.attentionWorkspacesMatch(0, smaller.ptr, scratch.generation, 0));
    // Explicit cache clearing must not reset the epoch either.
    const previous_generation = scratch.generation;
    try scratch.clear(&ctx);
    try scratch.clear(&ctx);
    _ = (try scratch.acquire(&ctx, 256)).?;
    try std.testing.expect(scratch.generation != previous_generation);
    try std.testing.expectEqual(@as(usize, 256), controller.snapshot().backend_scratch_bytes);
}

test "global MQA failed physical free retains allocation and admission until retry" {
    const Mock = struct {
        var fail_free = true;
        fn current(_: @import("driver.zig").CUcontext) callconv(.c) c_int {
            return 0;
        }
        fn sync(_: @import("driver.zig").CUstream) callconv(.c) c_int {
            return 0;
        }
        fn alloc(pointer: *u64, _: usize) callconv(.c) c_int {
            pointer.* = 0x8000;
            return 0;
        }
        fn free(_: u64) callconv(.c) c_int {
            return if (fail_free) 1 else 0;
        }
    };
    var controller = memory.AdmissionController{};
    var ctx = Context{ .driver = undefined, .ctx = @ptrFromInt(1), .stream = @ptrFromInt(2), .device = 0, .info = .{} };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    ctx.device_allocations = .{ .limit = 1024 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1024 }, .check_live_memory = false } };
    var scratch = Scratch{};
    Mock.fail_free = true;
    defer {
        Mock.fail_free = false;
        scratch.clear(&ctx) catch unreachable;
    }
    const allocation = (try scratch.acquire(&ctx, 512)).?;
    const generation = scratch.generation;
    try std.testing.expectError(error.CudaDriverError, scratch.clear(&ctx));
    try std.testing.expectEqual(allocation.ptr, scratch.buffer.ptr);
    try std.testing.expectEqual(generation, scratch.generation);
    try std.testing.expectEqual(@as(usize, 512), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 512), ctx.device_allocations.snapshot().live);
    Mock.fail_free = false;
    try scratch.clear(&ctx);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
}
