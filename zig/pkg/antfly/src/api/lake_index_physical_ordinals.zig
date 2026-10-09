// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Compressed, delete-aware physical rows to file-local native text ordinals.
//! Blocks cover 2^20 rows, including u64 row coordinates. Contiguous blocks
//! need no bitmap artifact; holes use rank over an authenticated Roaring set.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Bitmap = local.encoding_roaring.RoaringBitmap;
pub const shift = 20;
pub const Block = struct {
    group: u32,
    high: u64,
    base: u32,
    count: u32,
    lower: u32,
    bitmap: ?artifacts.ChunkRef = null,
};
pub fn validate(blocks: []const Block) !u32 {
    var count: u32 = 0;
    for (blocks, 0..) |block, i| {
        if (block.base != count or block.count == 0 or block.count > 1 << shift or block.lower >= 1 << shift or block.high > std.math.maxInt(u64) >> shift) return error.InvalidNativeLakeTextCorpus;
        if (block.bitmap == null and block.count > (1 << shift) - block.lower) return error.InvalidNativeLakeTextCorpus;
        if (i > 0) {
            const previous = blocks[i - 1];
            if (block.group < previous.group or (block.group == previous.group and block.high <= previous.high)) return error.InvalidNativeLakeTextCorpus;
        }
        if (block.bitmap) |ref| {
            try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
            if (ref.byte_len == 0 or ref.byte_len > 256 * 1024) return error.InvalidNativeLakeTextCorpus;
        }
        count = try std.math.add(u32, count, block.count);
    }
    return count;
}
pub fn find(blocks: []const Block, group: u32, row: u64) ?usize {
    const high = row >> shift;
    var lower: usize = 0;
    var upper = blocks.len;
    while (lower < upper) {
        const mid = lower + (upper - lower) / 2;
        const block = blocks[mid];
        if (block.group < group or (block.group == group and block.high < high)) lower = mid + 1 else upper = mid;
    }
    if (lower == blocks.len or blocks[lower].group != group or blocks[lower].high != high) return null;
    return lower;
}
pub const Builder = struct {
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    cancellation: @import("antfly_cancellation").CancellationToken,
    blocks: std.ArrayList(Block) = .empty,
    current: ?Block = null,
    bitmap: Bitmap,
    count: u32 = 0,
    last: ?u32 = null,
    pub fn init(a: A, out: A, store: *stores.ArtifactStore, cancellation: @import("antfly_cancellation").CancellationToken) Builder {
        return .{ .a = a, .out = out, .store = store, .cancellation = cancellation, .bitmap = Bitmap.init(a) };
    }
    pub fn deinit(self: *Builder) void {
        self.bitmap.deinit();
    }
    pub fn add(self: *Builder, group: u32, row: u64) !void {
        const high = row >> shift;
        const low: u32 = @intCast(row & ((1 << shift) - 1));
        if (self.current) |block| {
            if (group != block.group or high != block.high) try self.flush();
        }
        if (self.current == null) self.current = .{ .group = group, .high = high, .lower = low, .base = self.count, .count = 0 };
        if (self.last) |last| if (low <= last) return error.InvalidNativeLakeTextCorpus;
        try self.bitmap.add(low);
        self.current.?.count = try std.math.add(u32, self.current.?.count, 1);
        self.count = try std.math.add(u32, self.count, 1);
        self.last = low;
    }
    pub fn flush(self: *Builder) !void {
        var block = self.current orelse return;
        if (self.last.? - block.lower + 1 != block.count) {
            const bytes = try self.bitmap.toBytes(self.a);
            defer self.a.free(bytes);
            if (bytes.len > 256 * 1024) return error.InvalidNativeLakeTextCorpus;
            var upload = self.store.*;
            upload.allocator = self.out;
            const ref = try upload.putWithCancellation(bytes, self.cancellation);
            block.bitmap = .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
        }
        try self.blocks.append(self.out, block);
        self.bitmap.deinit();
        self.bitmap = Bitmap.init(self.a);
        self.current = null;
        self.last = null;
    }
};

test "external lake physical ordinal blocks preserve deleted holes and full u64 coordinates" {
    const blocks = [_]Block{
        .{ .group = 0, .high = 0, .lower = 3, .base = 0, .count = 4 },
        .{ .group = 2, .high = 1 << 32, .lower = 0, .base = 4, .count = 1 },
    };
    try std.testing.expectEqual(@as(u32, 5), try validate(&blocks));
    try std.testing.expectEqual(@as(?usize, 1), find(&blocks, 2, @as(u64, 1) << 52));
    try std.testing.expectEqual(@as(?usize, null), find(&blocks, 1, 3));
}
