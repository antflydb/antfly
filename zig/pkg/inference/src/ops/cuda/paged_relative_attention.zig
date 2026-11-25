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

//! BF16 staged paged GQA with query-relative additive logits, zero-valued
//! sink logits, and an output sigmoid gate. The prep caller owns clipping
//! and positional scaling; physical pages remain in the resident KV banks.
const std = @import("std");
const buffer = @import("buffer.zig");
const Context = @import("context.zig").CudaContext;
const cache = @import("hybrid_paged_kv.zig");
const embedded = @import("embedded_module.zig");
const catalog = @import("../../tile/cuda_standalone.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const Program = embedded.EmbeddedModule(struct {
    pub const image = embedded.profile(catalog.paged_relative_attention).image;
}, catalog.paged_relative_attention_entries);

const head_dim = 128;
const partial_width = head_dim + 2;
const keys_per_split = 128;

pub const Query = @import("../paged_relative_attention.zig").Query;

pub const Request = struct {
    geometry: Query,
    kv: cache.StagedView,
    device_page_ids: DeviceBuffer,
    replay_position: DeviceBuffer = .{},
    query: DeviceBuffer,
    relative: DeviceBuffer,
    sinks: DeviceBuffer,
    gate: DeviceBuffer,
    scratch: DeviceBuffer,
    output: DeviceBuffer,
};

const Plan = struct {
    rows_by_heads: u32,
    splits: u32,
    key_tile: u32,
    scratch_bytes: usize,
};

fn plan(query: Query, visible_start: usize, visible_end: usize) !Plan {
    const coverage = try query.coverage(visible_start, visible_end);
    const row_heads = coverage.row_heads;
    const live = coverage.live_keys;
    const splits = @min(if (query.rows >= 8) @as(u32, 16) else 64, (live - 1) / keys_per_split + 1);
    const records = try std.math.mul(usize, row_heads, splits);
    return .{
        .rows_by_heads = row_heads,
        .splits = splits,
        .key_tile = (live - 1) / splits + 1,
        .scratch_bytes = try std.math.mul(usize, records, partial_width * @sizeOf(f32)),
    };
}

pub fn scratchBytes(query: Query, visible_start: usize, visible_end: usize) !usize {
    return (try plan(query, visible_start, visible_end)).scratch_bytes;
}

const Params = extern struct {
    query: u64,
    keys: u64,
    values: u64,
    relative: u64,
    sinks: u64,
    gate: u64,
    page_ids: u64,
    partials: u64,
    output: u64,
    replay_position: u64,
    query_start: u32,
    visible_start: u32,
    page_size: u32,
    first_page: u32,
    page_count: u32,
    physical_pages: u32,
    q_heads: u32,
    kv_heads: u32,
    window: u32,
    relative_extent: u32,
    splits: u32,
    key_tile: u32,
    score_scale: f32,
};

comptime {
    if (@sizeOf(Params) != 136) @compileError("paged relative attention ABI mismatch");
}

pub const Module = struct {
    program: Program = .{},
    use_serial_chunks: bool = false,

    pub fn deinit(self: *Module, ctx: *Context) void {
        self.program.deinit(ctx);
    }

    pub fn appendReplay(self: *Module, ctx: *Context, binding: cache.StagedView, positions: DeviceBuffer, ids: DeviceBuffer, source: cache.Planes, kv_heads: u32) !void {
        const bytes = @as(usize, kv_heads) * head_dim * 2;
        if (binding.range.visible_start != 0 or binding.range.first_page != 0 or bytes == 0 or binding.range.page_tokens == 0) return error.InvalidAttentionGeometry;
        const plane_bytes = try std.math.mul(usize, bytes, try binding.range.physicalRows());
        try buffer.validateRegions(6, .{ source.key, source.value, ids, positions, binding.planes.key, binding.planes.value }, .{
            bytes, bytes, binding.range.page_count * 4, 4, plane_bytes, plane_bytes,
        }, @splat(4), 4);
        try self.program.init(ctx);
        var pointers = [_]u64{ binding.planes.key.ptr, binding.planes.value.ptr, source.key.ptr, source.value.ptr, ids.ptr, positions.ptr };
        var tokens: u32 = binding.range.page_tokens;
        var pairs: u32 = @intCast(bytes / 4);
        var pages: u32 = @intCast(binding.range.physical_pages);
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &pointers[4], &pointers[5], &tokens, &pairs, &pages };
        try self.program.launch(ctx, .append, (pairs - 1) / 128 + 1, 1, 128, &args);
    }

    /// Queue attention on the same stream as the staged K/V and page upload.
    pub fn run(self: *Module, ctx: *Context, r: Request) !void {
        const q = r.geometry;
        const range = r.kv.range;
        const g = try plan(q, range.visible_start, range.visible_end);
        const page_tokens = range.page_tokens;
        const physical_rows = try range.physicalRows();
        const kv_bytes = try std.math.mul(usize, physical_rows, q.kv_heads * head_dim * 2);
        const q_bytes = @as(usize, g.rows_by_heads) * head_dim * 2;
        const relative_bytes = try std.math.mul(usize, g.rows_by_heads, @as(usize, q.relative_extent) * 2);
        const replay_bytes: usize = if (r.replay_position.ptr == 0) 0 else 4;
        if (replay_bytes != 0 and (q.rows != 1 or range.first_page != 0 or range.visible_start != 0)) return error.InvalidAttentionGeometry;
        try buffer.validateRegions(10, .{
            r.query, r.kv.planes.key, r.kv.planes.value, r.relative, r.sinks, r.gate, r.device_page_ids, r.replay_position, r.scratch, r.output,
        }, .{
            q_bytes,              kv_bytes,     kv_bytes,        relative_bytes, @as(usize, g.rows_by_heads) * 2, q_bytes,
            range.page_count * 4, replay_bytes, g.scratch_bytes, q_bytes,
        }, .{ 2, 2, 2, 2, 2, 2, 4, 4, 4, 2 }, 8);
        if (ctx.info.compute_major < 8) return error.UnsupportedCudaDevice;
        try self.program.init(ctx);
        var params: Params = .{
            .query = r.query.ptr,
            .keys = r.kv.planes.key.ptr,
            .values = r.kv.planes.value.ptr,
            .relative = r.relative.ptr,
            .sinks = r.sinks.ptr,
            .gate = r.gate.ptr,
            .page_ids = r.device_page_ids.ptr,
            .partials = r.scratch.ptr,
            .output = r.output.ptr,
            .replay_position = r.replay_position.ptr,
            .query_start = q.start,
            .visible_start = @intCast(range.visible_start),
            .page_size = page_tokens,
            .first_page = @intCast(range.first_page),
            .page_count = @intCast(range.page_count),
            .physical_pages = @intCast(range.physical_pages),
            .q_heads = q.q_heads,
            .kv_heads = q.kv_heads,
            .window = q.window,
            .relative_extent = q.relative_extent,
            .splits = g.splits,
            .key_tile = g.key_tile,
            .score_scale = q.score_scale,
        };
        var args = [_]?*anyopaque{&params};
        const parallel = !self.use_serial_chunks and ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1 and
            q.rows == 1 and g.key_tile <= keys_per_split;
        try self.program.launch(ctx, if (parallel) .parallel_chunks else .chunks, g.splits, g.rows_by_heads, if (parallel) 256 else 32, &args);
        try self.program.launch(ctx, .combine, g.rows_by_heads, 1, 32, &args);
    }
};
