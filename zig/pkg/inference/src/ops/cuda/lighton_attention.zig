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
const context = @import("context.zig");
const driver = @import("driver.zig");

pub const max_blocks = 4096;
// Native decoder KV pages contain 16 tokens. The packet bounds aggregate
// table ownership; individual request/context ceilings remain independent.
pub const max_native_context_tokens = max_blocks * 16;

// The checked-in packet artifact is PTX 9.0 (CUDA 13.0). Older drivers
// retain the ordinary CUDA attention path rather than attempting this ABI.
pub fn packetSupported(compute_major: i32, driver_version: i32) bool {
    return compute_major >= 8 and driver_version >= 13000;
}

pub fn useChunk128(requested: ?bool, heads: usize) bool {
    return requested orelse (heads == 16);
}

test "LightOnOCR3 batched attention policy checks artifact support and preserves chunk override" {
    try std.testing.expect(!packetSupported(7, 13000));
    try std.testing.expect(!packetSupported(8, 12900));
    try std.testing.expect(packetSupported(8, 13000));
    try std.testing.expect(packetSupported(12, 13020));
    try std.testing.expect(!useChunk128(null, 8));
    try std.testing.expect(useChunk128(null, 16));
    try std.testing.expect(!useChunk128(false, 16));
    try std.testing.expect(useChunk128(true, 8));
}

pub const Page = extern struct {
    tokens: u32 = 0,
    query_position: u32 = 0,
    block_count: u32 = 0,
    block_offset: u32 = 0,
};
pub const Params = extern struct {
    q: driver.CUdeviceptr = 0,
    k: driver.CUdeviceptr = 0,
    v: driver.CUdeviceptr = 0,
    out: driver.CUdeviceptr = 0,
    partials: driver.CUdeviceptr = 0,
    statistics: driver.CUdeviceptr = 0,
    new_k: driver.CUdeviceptr = 0,
    new_v: driver.CUdeviceptr = 0,
    items: u32 = 0,
    heads: u32 = 0,
    kv_heads: u32 = 0,
    chunks: u32 = 0,
    page_size: u32 = 0,
    row_values: u32 = 0,
    capacity: u32 = 0,
    pages: [64]Page = @splat(.{}),
    blocks: [max_blocks]u32 = @splat(0),
    graph_progress: driver.CUdeviceptr = 0,
    graph_step: u32 = 0,
    reserved: u32 = 0,

    /// Snapshot the authoritative physical table before another sequence can
    /// reuse storage's host scratch. The packet has one shared capacity rather
    /// than an artificial 64-block limit for every page.
    pub fn appendPage(self: *Params, index: usize, tokens: usize, query: usize, ids: []const u32) bool {
        if (self.items > self.pages.len or self.page_size == 0 or index >= self.items or index >= self.pages.len or ids.len == 0 or
            tokens == 0 or tokens > std.math.maxInt(u32) or query >= tokens) return false;
        const capacity = std.math.mul(usize, ids.len, self.page_size) catch return false;
        if (tokens > capacity or (index != 0 and self.pages[index - 1].block_count == 0)) return false;
        const offset: usize = if (index == 0) 0 else @as(usize, self.pages[index - 1].block_offset) + self.pages[index - 1].block_count;
        if (offset > max_blocks or ids.len > max_blocks - offset) return false;
        self.pages[index] = .{ .tokens = @intCast(tokens), .query_position = @intCast(query), .block_count = @intCast(ids.len), .block_offset = @intCast(offset) };
        @memcpy(self.blocks[offset..][0..ids.len], ids);
        return true;
    }

    pub fn blockIds(self: *const Params, page: Page) []const u32 {
        return self.blocks[page.block_offset..][0..page.block_count];
    }
};
comptime {
    if (@sizeOf(Page) != 16 or @sizeOf(Params) != 17520 or @offsetOf(Params, "pages") != 92)
        @compileError("LightOn CUDA attention packet ABI changed");
}

/// Group four query heads only when the active KV chunks can populate at
/// least four waves of the device's SMs. Row count alone misses long small
/// cohorts and over-groups short wider cohorts. Reserved future chunks are
/// excluded: their empty launches cannot hide the grouped kernel's latency.
/// Unknown device capacity retains the ordinary split-attention kernel.
pub fn useGrouped(params: *const Params, chunk128: bool, multiprocessors: usize) bool {
    if (multiprocessors == 0 or multiprocessors > 1024 or params.items == 0 or params.items > params.pages.len or params.chunks == 0) return false;
    if (!((params.heads == 8 and params.kv_heads == 2 and !chunk128) or
        (params.heads == 16 and params.kv_heads == 4 and chunk128))) return false;
    const chunk: u32 = if (chunk128) 128 else 32;
    var active_blocks: u64 = 0;
    for (params.pages[0..params.items]) |page| {
        if (page.tokens == 0) return false;
        active_blocks += @as(u64, params.kv_heads) * @min(params.chunks, 1 + (page.tokens - 1) / chunk);
    }
    return active_blocks >= 4 * @as(u64, @intCast(multiprocessors));
}

test "LightOnOCR3 grouped attention follows active chunks and device capacity" {
    var params = Params{ .heads = 16, .kv_heads = 4, .items = 4, .chunks = 31 };
    for (&params.pages) |*page| page.tokens = 1914;
    try std.testing.expect(useGrouped(&params, true, 48));
    try std.testing.expect(!useGrouped(&params, true, 108));
    try std.testing.expect(!useGrouped(&params, true, 0));
    params.items = 2;
    try std.testing.expect(!useGrouped(&params, true, 48));
    params.items = 16;
    for (&params.pages) |*page| page.tokens = 256;
    // Future reservations do not turn empty work into useful occupancy.
    try std.testing.expect(!useGrouped(&params, true, 48));
    params.items = 32;
    try std.testing.expect(useGrouped(&params, true, 48));
    params.heads = 8;
    params.kv_heads = 2;
    params.items = 8;
    params.chunks = 124;
    try std.testing.expect(!useGrouped(&params, false, 48));
    params.items = 16;
    try std.testing.expect(useGrouped(&params, false, 48));
    params.items = 2;
    for (&params.pages) |*page| page.tokens = 1914;
    try std.testing.expect(useGrouped(&params, false, 48));
    try std.testing.expect(!useGrouped(&params, true, 48));
    params.kv_heads = 1;
    try std.testing.expect(!useGrouped(&params, false, 48));
    params.kv_heads = 2;
    params.pages[1].tokens = 0;
    try std.testing.expect(!useGrouped(&params, false, 48));
}

pub const Module = struct {
    module: driver.CUmodule = null,
    functions: [7]driver.CUfunction = @splat(null),
    calls: usize = 0,
    suffix_write_calls: usize = 0,
    chunk128_calls: usize = 0,
    items: usize = 0,
    grouped_calls: usize = 0,
    multiprocessors: usize = 0,

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.* = .{};
        }
    }
    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.module != null) return;
        var module: driver.CUmodule = null;
        const image = @embedFile("lighton_attention.ptx") ++ "\x00";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var functions: [7]driver.CUfunction = @splat(null);
        inline for (.{ "lighton_attention_chunks", "lighton_attention_combine", "lighton_attention_write_suffix", "lighton_attention_chunks128", "lighton_graph_advance", "lighton_attention_grouped_chunks32", "lighton_attention_grouped_chunks128" }, 0..) |name, i|
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&functions[i], module, name));
        var multiprocessors: c_int = 0;
        if (ctx.driver.fns.cuDeviceGetAttribute) |attribute| {
            if (attribute(&multiprocessors, 16, ctx.device) != driver.CUDA_SUCCESS) multiprocessors = 0;
        }
        self.multiprocessors = if (multiprocessors > 0) @intCast(multiprocessors) else 0;
        self.module = module;
        self.functions = functions;
    }
    pub fn advanceGraph(self: *Module, ctx: *context.CudaContext, positions: driver.CUdeviceptr, progress: driver.CUdeviceptr, count: u32) !void {
        if (count == 0 or count > 384) return error.InvalidShape;
        var p = positions;
        var c = progress;
        var n = count;
        var args = [_]?*anyopaque{ &p, &c, &n };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[4], 1, 1, 1, 512, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
    pub fn writeSuffix(self: *Module, ctx: *context.CudaContext, params: *Params) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[2], (params.row_values + 127) / 128, params.items, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.suffix_write_calls += 1;
    }
    pub fn run(self: *Module, ctx: *context.CudaContext, params: *Params, chunk128: bool) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        const grouped = useGrouped(params, chunk128, self.multiprocessors);
        const function = self.functions[if (grouped) (if (chunk128) @as(usize, 6) else 5) else if (chunk128) 3 else 0];
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(function, if (grouped) params.kv_heads else params.heads, params.items * params.chunks, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[1], params.items * params.heads, 1, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
        self.grouped_calls += @intFromBool(grouped);
        self.chunk128_calls += @intFromBool(chunk128);
        self.items += params.items;
    }
};

test "LightOnOCR3 CUDA attention packets snapshot independent full page tables" {
    var params = Params{ .items = 4, .page_size = 16 };
    var source: [1024]u32 = undefined;
    for (&source, 0..) |*value, i| value.* = @intCast(i * 3 + 1);
    try std.testing.expect(params.appendPage(0, 2697, 2696, source[0..169]));
    @memset(&source, 42);
    try std.testing.expect(params.appendPage(1, 8192, 8191, source[0..512]));
    try std.testing.expectEqual(@as(u32, 169), params.pages[1].block_offset);
    try std.testing.expectEqual(@as(u32, 1), params.blockIds(params.pages[0])[0]);
    try std.testing.expectEqual(@as(u32, 505), params.blockIds(params.pages[0])[168]);
    try std.testing.expectEqual(@as(u32, 42), params.blockIds(params.pages[1])[511]);
    try std.testing.expect(params.appendPage(2, 16384, 16383, &source));
    try std.testing.expect(params.appendPage(3, 16384, 16383, &source));
}

test "LightOnOCR3 CUDA attention packet capacity is aggregate and fails before mutation" {
    var params = Params{ .items = 4, .page_size = 16 };
    const ids: [max_blocks]u32 = @splat(7);
    try std.testing.expect(params.appendPage(0, 65536, 65535, &ids));
    try std.testing.expect(!params.appendPage(1, 1, 0, ids[0..1]));
    try std.testing.expectEqual(@as(u32, 0), params.pages[1].block_count);
    try std.testing.expect(!params.appendPage(4, 1, 0, ids[0..1]));
    try std.testing.expect(!params.appendPage(0, 0, 0, ids[0..1]));
    try std.testing.expect(!params.appendPage(0, 1, 1, ids[0..1]));
}
