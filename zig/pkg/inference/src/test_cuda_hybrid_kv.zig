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
const kv = @import("ops/cuda/hybrid_paged_kv.zig");
const buffer = @import("ops/cuda/buffer.zig");
const CudaContext = @import("ops/cuda/context.zig").CudaContext;

test {
    _ = @import("runtime/kv/hybrid_paged.zig");
    _ = @import("runtime/kv/storage_runtime.zig");
    _ = @import("runtime/kv/writable_blocks.zig");
}

const config: kv.Config = .{
    .layout = .{ .page_tokens = 16, .window_tokens = 1024, .max_tokens = 32768, .max_append_tokens = 256 },
    .full_slots = 8,
    .sliding_slots = 40,
    .kv_heads = 8,
    .head_dim = 128,
};
const visits = blk: {
    var result: [48]kv.Visit = undefined;
    var full: u16 = 0;
    var sliding: u16 = 0;
    for (&result, 0..) |*visit, index| {
        if (index % 6 == 5) {
            visit.* = .{ .group = .full, .slot = full };
            full += 1;
        } else {
            visit.* = .{ .group = .sliding, .slot = sliding };
            sliding += 1;
        }
    }
    break :blk result;
};
const row_values = config.kv_heads * config.head_dim;
const row_bytes = row_values * @sizeOf(u16);
const workspace_bytes = visits.len * 2 * config.layout.max_append_tokens * row_bytes;

const Scratch = struct {
    host: buffer.HostBuffer,
    device: buffer.DeviceBuffer,

    fn init(ctx: *CudaContext) !Scratch {
        var host = try buffer.HostBuffer.alloc(ctx, workspace_bytes);
        errdefer host.free(ctx);
        return .{ .host = host, .device = try buffer.DeviceBuffer.allocWithPurpose(ctx, workspace_bytes, .command_workspace) };
    }

    fn deinit(self: *Scratch, ctx: *CudaContext) void {
        self.device.free(ctx);
        self.host.free(ctx);
    }

    fn planes(self: Scratch, offset: usize, bytes: usize) kv.Planes {
        return .{
            .key = .{ .ptr = self.device.ptr + offset, .len = bytes, .borrowed = true },
            .value = .{ .ptr = self.device.ptr + offset + bytes, .len = bytes, .borrowed = true },
        };
    }
};

fn fill(dst: []u8, start: usize, rows: usize, visit: usize, salt: usize) void {
    for (0..2) |operand| {
        const words = std.mem.bytesAsSlice(u16, dst[operand * rows * row_bytes ..][0 .. rows * row_bytes]);
        for (0..rows) |row| {
            const position = start + row;
            const base: u16 = @truncate(position * 193 + visit * 4999 + operand * 2131 + salt);
            const high: u16 = @truncate(position * 1237 + visit * 743 + operand * 3569 + salt * 97);
            for (words[row * row_values ..][0..row_values], 0..) |*value, column|
                value.* = (base +% @as(u16, @intCast(column)) *% 11) ^ (if (column % 2 == 1) high else 0);
        }
    }
}

fn stage(cache: *kv.Cache, ctx: *CudaContext, scratch: *Scratch, rows: usize, salt: usize) !void {
    const start = cache.committedTokens();
    try cache.prepareAppend(rows);
    const plane_bytes = rows * row_bytes;
    for (0..visits.len) |i|
        fill(scratch.host.bytes()[i * 2 * plane_bytes ..][0 .. 2 * plane_bytes], start, rows, i, salt);
    try scratch.device.copyFromHost(ctx, scratch.host.constBytes()[0 .. visits.len * 2 * plane_bytes]);
    for (visits, 0..) |visit, i|
        try cache.queueWrite(visit, scratch.planes(i * 2 * plane_bytes, plane_bytes));
}

fn verify(cache: *kv.Cache, ctx: *CudaContext, got: *Scratch, expected: *Scratch, start: usize, rows: usize, group: ?kv.Group) !void {
    const plane_bytes = rows * row_bytes;
    var used: usize = 0;
    for (visits, 0..) |visit, i| {
        if (group != null and visit.group != group.?) continue;
        try cache.queueRead(visit, start, rows, got.planes(used, plane_bytes));
        fill(expected.host.bytes()[used..][0 .. 2 * plane_bytes], start, rows, i, 0);
        used += 2 * plane_bytes;
    }
    try got.device.copyToHost(ctx, got.host.bytes()[0..used]);
    try ctx.synchronize();
    if (!std.mem.eql(u8, expected.host.constBytes()[0..used], got.host.constBytes()[0..used])) {
        std.debug.print("hybrid KV payload mismatch at position {d}, rows={d}, group={?}\n", .{ start, rows, group });
        return error.HybridKvPayloadMismatch;
    }
}

test "CUDA hybrid paged KV verifies BF16 payload and abort across 48 visits at 32K" {
    var ctx = try CudaContext.initDefault();
    defer ctx.deinit();
    const allocator = std.testing.allocator;
    const full_bytes = try config.deviceBytes(.full);
    const sliding_bytes = try config.deviceBytes(.sliding);
    try std.testing.expectEqual(@as(usize, 1073741824), full_bytes);
    try std.testing.expectEqual(@as(usize, 212336640), sliding_bytes);

    // The first bank physically allocates. Denying the second bank must unwind
    // that allocation, leave no ledger charge, and permit construction retry.
    var cache: kv.Cache = undefined;
    try ctx.device_allocations.setLimit(full_bytes);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, cache.init(allocator, &ctx, config));
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.physical_live);
    try ctx.device_allocations.setLimit(full_bytes + sliding_bytes + 2 * workspace_bytes);
    try cache.init(allocator, &ctx, config);
    var open = true;
    defer if (open) cache.deinit() catch @panic("unable to drain hybrid KV cache");
    var source = try Scratch.init(&ctx);
    defer source.deinit(&ctx);
    var readback = try Scratch.init(&ctx);
    defer readback.deinit(&ctx);
    errdefer ctx.synchronize() catch {};

    var verified_recovery = false;
    var first_sliding_page: ?@import("runtime/kv/block.zig").KvBlockId = null;
    var verified_reuse = false;
    try cache.prepareAppend(1);
    try std.testing.expectError(error.KvVisitNotWritten, cache.queueStagedRead(visits[0], 0, 1, readback.planes(0, row_bytes)));
    try std.testing.expectError(error.IncompleteKvWriteSet, cache.commit());
    try cache.abort();
    while (cache.committedTokens() < config.layout.max_tokens) {
        const before = cache.committedTokens();
        const rows = @min(if (before == 0) 1 else config.layout.max_append_tokens, config.layout.max_tokens - before);
        if (before == 1025) {
            // Deny the new sliding pages after the full group prepared, then
            // abort another append after both groups queued actual D2D copies.
            const sliding_pool = cache.sliding.storage.hostPool().?;
            sliding_pool.setTargetMaxBlocks(sliding_pool.liveBlocks());
            try std.testing.expectError(error.KvCapacityTooSmall, cache.prepareAppend(rows));
            try std.testing.expectEqual(before, cache.committedTokens());
            try std.testing.expectEqual(@as(usize, 16), cache.full.storage.hostPool().?.freeListBlocks());
            sliding_pool.setTargetMaxBlocks(try config.layout.pageCapacity(.sliding));

            try stage(&cache, &ctx, &source, rows, 137);
            try std.testing.expectEqual(before, cache.committedTokens());
            try std.testing.expectError(error.InvalidKvRange, cache.queueRead(visits[0], before, 1, readback.planes(0, row_bytes)));
            const staged_rows = 3;
            try cache.queueStagedRead(visits[0], before - 2, staged_rows, readback.planes(0, staged_rows * row_bytes));
            try readback.device.copyToHost(&ctx, readback.host.bytes()[0 .. staged_rows * 2 * row_bytes]);
            const syncs = ctx.stats.stream_syncs;
            try cache.abort();
            try std.testing.expectEqual(syncs + 1, ctx.stats.stream_syncs);
            try std.testing.expectEqual(before, cache.committedTokens());
            try std.testing.expectEqual(@as(usize, 16), sliding_pool.freeListBlocks());
            // The staged view saw the modified suffix and the two original
            // rows spanning a page boundary before the abort released it.
            for (0..staged_rows) |row| {
                var expected: [2 * row_bytes]u8 = undefined;
                fill(&expected, before - 2 + row, 1, 0, if (row == 2) 137 else 0);
                for (0..2) |operand| {
                    const offset = (operand * staged_rows + row) * row_bytes;
                    try std.testing.expectEqualSlices(u8, expected[operand * row_bytes ..][0..row_bytes], readback.host.constBytes()[offset..][0..row_bytes]);
                }
            }
            for (0..4) |i|
                try verify(&cache, &ctx, &readback, &source, 1 + i * 256, 256, null);
            try verify(&cache, &ctx, &readback, &source, 0, 1, .full);
            verified_recovery = true;
        }

        try stage(&cache, &ctx, &source, rows, 0);
        try cache.commit();
        if (before == 0) first_sliding_page = cache.sequence.pageId(.sliding, 0).?;
        if (before == 1281) {
            const page_tokens = config.layout.page_tokens;
            try std.testing.expectEqual(null, cache.sequence.pageId(.sliding, 0));
            for (before / page_tokens + 1..(before + rows - 1) / page_tokens + 1) |page| {
                if (cache.sequence.pageId(.sliding, page) == first_sliding_page) verified_reuse = true;
            }
        }
        try verify(&cache, &ctx, &readback, &source, before, rows, null);
    }
    try std.testing.expect(verified_recovery);
    try std.testing.expect(verified_reuse);
    try std.testing.expectEqual(@as(usize, 32768), cache.committedTokens());
    const oldest = cache.committedTokens() - config.layout.window_tokens;
    try std.testing.expectError(error.InvalidKvRange, cache.queueRead(visits[0], oldest - 1, 1, readback.planes(0, row_bytes)));
    for (0..4) |i|
        try verify(&cache, &ctx, &readback, &source, oldest + i * 256, 256, null);
    for ([_]usize{ 0, 8191, 16384 }) |position|
        try verify(&cache, &ctx, &readback, &source, position, 256, .full);

    const usage = cache.usage();
    try std.testing.expectEqual(@as(usize, 2048), usage.full_pages);
    try std.testing.expectEqual(@as(usize, 81), usage.sliding_pages);
    try std.testing.expectEqual(full_bytes + sliding_bytes, usage.device_bytes);
    try std.testing.expectEqual(usage.device_bytes + 2 * workspace_bytes, ctx.device_allocations.physical_peak);
    std.debug.print("hybrid BF16 KV device={s} sm={d}{d} tokens={d} visits={d} full_pages={d} sliding_pages={d} kv_device_bytes={d} test_peak_device_bytes={d}\n", .{
        ctx.info.nameSlice(), ctx.info.compute_major, ctx.info.compute_minor, cache.committedTokens(), visits.len, usage.full_pages, usage.sliding_pages, usage.device_bytes, ctx.device_allocations.physical_peak,
    });
    try cache.deinit();
    open = false;
    try std.testing.expectEqual(2 * workspace_bytes, ctx.device_allocations.physical_live);
}
