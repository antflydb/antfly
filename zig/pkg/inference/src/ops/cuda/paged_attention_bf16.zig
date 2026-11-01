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

// Native export names remain the compatibility ABI of the qualified PTX.
const catalog = @import("../../tile/cuda_standalone.zig");
const Program = @import("embedded_module.zig").EmbeddedModule(@import("embedded_module.zig").profile(catalog.paged_attention_bf16).image, catalog.paged_attention_bf16_entries);

pub const Module = struct {
    program: Program = .{},
    calls: usize = 0,
    suffix_write_calls: usize = 0,
    chunk128_calls: usize = 0,
    items: usize = 0,

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.program.deinitChecked(ctx) catch return;
        self.* = .{};
    }
    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
    }
    pub fn advanceGraph(self: *Module, ctx: *context.CudaContext, positions: driver.CUdeviceptr, progress: driver.CUdeviceptr, count: u32) !void {
        if (count == 0 or count > 384) return error.InvalidShape;
        var p = positions;
        var c = progress;
        var n = count;
        var args = [_]?*anyopaque{ &p, &c, &n };
        try self.program.launch(ctx, .advance_graph, 1, 1, 512, &args);
    }
    pub fn writeSuffix(self: *Module, ctx: *context.CudaContext, params: *Params) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        try self.program.launch(ctx, .write_suffix, (params.row_values + 127) / 128, params.items, 128, &args);
        self.suffix_write_calls += 1;
    }
    pub fn run(self: *Module, ctx: *context.CudaContext, params: *Params, chunk128: bool) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        // Retain main's qualified grouped-CTA eligibility and geometry.
        const grouped = (params.heads == 8 and !chunk128 and params.items >= 8) or
            (params.heads == 16 and chunk128 and params.items >= 16);
        const entry: Program.Entry = if (grouped)
            (if (chunk128) .grouped_chunks128_d128 else .grouped_chunks32_d128)
        else if (chunk128) .chunks128_d128 else .chunks_d128;
        try self.program.launch(ctx, entry, if (grouped) params.kv_heads else params.heads, params.items * params.chunks, 128, &args);
        try self.program.launch(ctx, .combine_d128, params.items * params.heads, 1, 128, &args);
        self.calls += 1;
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
