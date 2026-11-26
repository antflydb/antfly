// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
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
const shared_query_rows = 4;
const shared_query_min_position = 8192;
const page_rows_min_queries = 16;
const page_rows_min_position = 1024;

pub const Query = struct {
    rows: u32,
    start: u32,
    q_heads: u32,
    kv_heads: u32,
    window: u32 = 0,
    relative_extent: u32,
    score_scale: f32,
};

pub const relative_width = 16;
pub const wide_query_rows = 128;

pub const RelativeProjection = struct {
    query: DeviceBuffer,
    basis: DeviceBuffer,
};

pub const Request = struct {
    use_native_prefill: bool = false,
    geometry: Query,
    kv: cache.StagedView,
    device_page_ids: DeviceBuffer,
    replay_position: DeviceBuffer = .{},
    query: DeviceBuffer,
    relative: DeviceBuffer,
    relative_projection: ?RelativeProjection = null,
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
    if (query.rows == 0 or query.q_heads == 0 or query.q_heads > 64 or query.kv_heads == 0 or
        query.q_heads % query.kv_heads != 0 or !std.math.isFinite(query.score_scale) or query.score_scale <= 0)
        return error.InvalidAttentionGeometry;
    const end = try std.math.add(u32, query.start, query.rows);
    const row_heads = try std.math.mul(u32, query.rows, query.q_heads);
    if (visible_start > query.start or end > visible_end or visible_end > (1 << 24) or row_heads > 65535)
        return error.InvalidAttentionGeometry;
    const keys: u32 = end - @as(u32, @intCast(visible_start));
    const live = if (query.window == 0) keys else @min(keys, query.window);
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
    use_page_rows: bool = true,
    pipeline_shared_memory: bool = false,
    relative_shared_memory: bool = false,

    pub fn deinit(self: *Module, ctx: *Context) void {
        self.program.deinit(ctx);
        self.pipeline_shared_memory = false;
        self.relative_shared_memory = false;
    }

    pub fn appendReplay(self: *Module, ctx: *Context, binding: cache.StagedView, positions: DeviceBuffer, ids: DeviceBuffer, source: cache.Planes, kv_heads: u32) !void {
        const bytes = @as(usize, kv_heads) * head_dim * 2;
        if (binding.visible_start != 0 or binding.first_page != 0 or bytes == 0 or binding.page_tokens == 0) return error.InvalidAttentionGeometry;
        const plane_bytes = try std.math.mul(usize, bytes, binding.physical_pages * binding.page_tokens);
        try buffer.validateRegions(6, .{ source.key, source.value, ids, positions, binding.planes.key, binding.planes.value }, .{
            bytes, bytes, binding.page_count * 4, 4, plane_bytes, plane_bytes,
        }, @splat(4), 4);
        try self.program.init(ctx);
        var pointers = [_]u64{ binding.planes.key.ptr, binding.planes.value.ptr, source.key.ptr, source.value.ptr, ids.ptr, positions.ptr };
        var tokens: u32 = binding.page_tokens;
        var pairs: u32 = @intCast(bytes / 4);
        var pages: u32 = @intCast(binding.physical_pages);
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &pointers[4], &pointers[5], &tokens, &pairs, &pages };
        try self.program.launch(ctx, .append, (pairs - 1) / 128 + 1, 1, 128, &args);
    }

    pub fn appendStaged(self: *Module, ctx: *Context, destination: cache.StagedView, ids: DeviceBuffer, source: cache.Planes, start: usize, row_bytes: usize) !void {
        if (destination.page_tokens == 0 or destination.visible_start > start or start >= destination.visible_end or
            destination.visible_end > std.math.maxInt(u32) or destination.physical_pages == 0 or destination.physical_pages > std.math.maxInt(u32) or
            row_bytes == 0 or row_bytes % 16 != 0 or row_bytes / 16 > std.math.maxInt(u32))
            return error.InvalidAttentionGeometry;
        const rows = destination.visible_end - start;
        if (destination.first_page != destination.visible_start / destination.page_tokens or
            destination.page_count != (destination.visible_end - 1) / destination.page_tokens + 1 - destination.first_page)
            return error.InvalidAttentionGeometry;
        const plane_rows = try std.math.mul(usize, destination.physical_pages, destination.page_tokens);
        const plane_bytes = try std.math.mul(usize, plane_rows, row_bytes);
        const input_bytes = try std.math.mul(usize, rows, row_bytes);
        try buffer.validateRegions(5, .{ source.key, source.value, ids, destination.planes.key, destination.planes.value }, .{
            input_bytes, input_bytes, destination.page_count * 4, plane_bytes, plane_bytes,
        }, .{ 16, 16, 4, 16, 16 }, 3);
        try self.program.init(ctx);
        var ptrs = [_]u64{ destination.planes.key.ptr, destination.planes.value.ptr, source.key.ptr, source.value.ptr, ids.ptr };
        var token: u32 = @intCast(start);
        var page_tokens = destination.page_tokens;
        var first_page: u32 = @intCast(destination.first_page);
        var page_count: u32 = @intCast(destination.page_count);
        var physical_pages: u32 = @intCast(destination.physical_pages);
        var vectors: u32 = @intCast(row_bytes / 16);
        var args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &ptrs[2], &ptrs[3], &ptrs[4], &token, &page_tokens, &first_page, &page_count, &physical_pages, &vectors };
        try self.program.launch(ctx, .append_rows, @intCast(rows), 1, 128, &args);
    }

    pub fn supportsRelativeProjection(self: *const Module, ctx: *const Context, rows: u32, query_heads: u32, kv_heads: u32) bool {
        return !self.use_serial_chunks and ctx.info.compute_major == 12 and
            ctx.info.compute_minor == 1 and rows >= wide_query_rows and kv_heads != 0 and query_heads / kv_heads >= 2;
    }

    fn launchWithShared(ctx: *Context, function: @import("driver.zig").CUfunction, grid_x: u32, grid_y: u32, shared_bytes: u32, configured: *bool, args: []?*anyopaque) !void {
        if (!configured.*) {
            const set_attribute = ctx.driver.fns.cuFuncSetAttribute orelse return error.CudaKernelUnavailable;
            const max_dynamic_shared_size_bytes = 8;
            try ctx.driver.check(set_attribute(function, max_dynamic_shared_size_bytes, @intCast(shared_bytes)));
            configured.* = true;
        }
        try ctx.launchKernel(function, grid_x, grid_y, 1, wide_query_rows * 2, 1, 1, shared_bytes, ctx.stream, args.ptr, null);
        ctx.noteKernelLaunch();
    }

    /// Queue attention on the same stream as the staged K/V and page upload.
    pub fn run(self: *Module, ctx: *Context, r: Request) !void {
        const q = r.geometry;
        const g = try plan(q, r.kv.visible_start, r.kv.visible_end);
        const page_tokens = r.kv.page_tokens;
        if (page_tokens == 0 or r.kv.physical_pages == 0 or r.kv.physical_pages > std.math.maxInt(u32) or
            r.kv.first_page != r.kv.visible_start / page_tokens or
            r.kv.page_count != (r.kv.visible_end - 1) / page_tokens + 1 - r.kv.first_page)
            return error.InvalidAttentionGeometry;
        const physical_rows = try std.math.mul(usize, r.kv.physical_pages, page_tokens);
        const kv_bytes = try std.math.mul(usize, physical_rows, q.kv_heads * head_dim * 2);
        const q_bytes = @as(usize, g.rows_by_heads) * head_dim * 2;
        const projection = r.relative_projection orelse RelativeProjection{ .query = .{}, .basis = .{} };
        const fuse_relative = r.relative_projection != null;
        const relative_bytes = if (fuse_relative) 0 else try std.math.mul(usize, g.rows_by_heads, @as(usize, q.relative_extent) * 2);
        const relative_query_bytes = if (fuse_relative) @as(usize, g.rows_by_heads) * relative_width * 2 else 0;
        const relative_basis_bytes = if (fuse_relative) @as(usize, q.relative_extent) * relative_width * 2 else 0;
        const replay_bytes: usize = if (r.replay_position.ptr == 0) 0 else 4;
        if (replay_bytes != 0 and (q.rows != 1 or r.kv.first_page != 0 or r.kv.visible_start != 0)) return error.InvalidAttentionGeometry;
        try buffer.validateRegions(12, .{
            r.query,           r.kv.planes.key,   r.kv.planes.value, r.relative, projection.query, projection.basis, r.sinks, r.gate,
            r.device_page_ids, r.replay_position, r.scratch,         r.output,
        }, .{
            q_bytes,             kv_bytes,     kv_bytes,        relative_bytes, relative_query_bytes, relative_basis_bytes, @as(usize, g.rows_by_heads) * 2, q_bytes,
            r.kv.page_count * 4, replay_bytes, g.scratch_bytes, q_bytes,
        }, .{ 2, 2, 2, 2, 4, 4, 2, 2, 4, 4, 4, 2 }, 10);
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
            .visible_start = @intCast(r.kv.visible_start),
            .page_size = page_tokens,
            .first_page = @intCast(r.kv.first_page),
            .page_count = @intCast(r.kv.page_count),
            .physical_pages = @intCast(r.kv.physical_pages),
            .q_heads = q.q_heads,
            .kv_heads = q.kv_heads,
            .window = q.window,
            .relative_extent = q.relative_extent,
            .splits = g.splits,
            .key_tile = g.key_tile,
            .score_scale = q.score_scale,
        };
        var args = [_]?*anyopaque{&params};
        const tuned = !self.use_serial_chunks and ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1;
        const parallel = tuned and q.rows == 1 and g.key_tile <= keys_per_split;
        const prefill = tuned and ctx.info.compute_minor == 1 and replay_bytes == 0;
        const page_rows = prefill and self.use_page_rows and page_tokens == 16 and q.rows >= page_rows_min_queries and q.start >= page_rows_min_position and
            q.rows % shared_query_rows == 0;
        const share_rows = prefill and q.window == 0 and q.start >= shared_query_min_position and q.rows % shared_query_rows == 0;
        const mma = r.use_native_prefill and prefill and page_tokens == 16 and q.rows >= wide_query_rows and (r.query.ptr | r.kv.planes.key.ptr | r.kv.planes.value.ptr) % 16 == 0;
        if (fuse_relative and (!mma or q.window == 0 or q.relative_extent == 0 or
            !self.supportsRelativeProjection(ctx, q.rows, q.q_heads, q.kv_heads))) return error.InvalidAttentionGeometry;
        if (mma) {
            const tile_rows = wide_query_rows / (q.q_heads / q.kv_heads);
            const max_splits: u32 = if (q.rows < 512) 2 else 1;
            params.splits = if (q.window != 0) 1 else @min(g.splits, max_splits);
            var query_rows = q.rows;
            var mma_args = [_]?*anyopaque{ &params, &query_rows };
            const grid_x = (q.rows - 1) / tile_rows + 1;
            const grid_y = q.kv_heads * params.splits;
            if (fuse_relative) {
                var addresses = [_]u64{ projection.query.ptr, projection.basis.ptr };
                var fused_args = mma_args ++ [_]?*anyopaque{ &addresses[0], &addresses[1] };
                const shared_bytes = (2 * 64 * head_dim + wide_query_rows * 72) * @sizeOf(u16);
                try launchWithShared(ctx, self.program.function(.page16_m128n64_fused_relative), grid_x, grid_y, shared_bytes, &self.relative_shared_memory, &fused_args);
            } else if (q.window == 0) {
                const shared_bytes = 4 * 64 * head_dim * @sizeOf(u16);
                try launchWithShared(ctx, self.program.function(.page16_m128n64_pipeline), grid_x, grid_y, shared_bytes, &self.pipeline_shared_memory, &mma_args);
            } else {
                try self.program.launch(ctx, .page16_m128n64_chunks, grid_x, grid_y, wide_query_rows * 2, &mma_args);
            }
            if (params.splits == 1) return;
        } else if (page_rows) {
            const entry: Program.Entry = if (q.window == 0) .page16_global_rows4_chunks else .page16_sliding_rows4_chunks;
            try self.program.launch(ctx, entry, g.splits, g.rows_by_heads / shared_query_rows, 32, &args);
        } else if (share_rows) {
            try self.program.launch(ctx, .global_rows4_chunks, g.splits, g.rows_by_heads / shared_query_rows, 32, &args);
        } else {
            try self.program.launch(ctx, if (parallel) .parallel_chunks else .chunks, g.splits, g.rows_by_heads, if (parallel) 256 else 32, &args);
        }
        try self.program.launch(ctx, .combine, g.rows_by_heads, 1, 32, &args);
    }
};
