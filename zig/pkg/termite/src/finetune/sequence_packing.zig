// Copyright 2026 Antfly, Inc.
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

pub const PackConfig = struct {
    max_seq_len: usize,
    pad_token_id: i32 = 0,
    pad_label: i32 = -100,
    truncate_long: bool = true,
};

pub const Sample = struct {
    input_ids: []const i32,
    labels: []const i32,
};

pub const PackedBuffer = struct {
    allocator: std.mem.Allocator,
    input_ids: []i32,
    labels: []i32,
    position_ids: []i32,
    doc_ids: []i32,
    cu_seqlens: []i32,
    num_docs: u32,
    pad_count: u32,

    pub fn deinit(self: *PackedBuffer) void {
        self.allocator.free(self.input_ids);
        self.allocator.free(self.labels);
        self.allocator.free(self.position_ids);
        self.allocator.free(self.doc_ids);
        self.allocator.free(self.cu_seqlens);
        self.* = undefined;
    }
};

pub const PackResult = struct {
    allocator: std.mem.Allocator,
    buffers: []PackedBuffer,
    dropped_long: usize,

    pub fn deinit(self: *PackResult) void {
        for (self.buffers) |*b| b.deinit();
        self.allocator.free(self.buffers);
        self.* = undefined;
    }
};

const Bin = struct {
    sample_indices: std.ArrayListUnmanaged(usize) = .empty,
    fill: usize = 0,
    truncated_len: usize = 0, // non-zero only for solo-truncated bins
    is_truncated: bool = false,
};

fn finalizeBin(
    allocator: std.mem.Allocator,
    bin: *Bin,
    samples: []const Sample,
    config: PackConfig,
) !PackedBuffer {
    const seq = config.max_seq_len;
    var input_ids = try allocator.alloc(i32, seq);
    errdefer allocator.free(input_ids);
    var labels = try allocator.alloc(i32, seq);
    errdefer allocator.free(labels);
    var position_ids = try allocator.alloc(i32, seq);
    errdefer allocator.free(position_ids);
    var doc_ids = try allocator.alloc(i32, seq);
    errdefer allocator.free(doc_ids);

    const num_docs: u32 = @intCast(bin.sample_indices.items.len);
    var cu_seqlens = try allocator.alloc(i32, num_docs + 1);
    errdefer allocator.free(cu_seqlens);

    var cursor: usize = 0;
    cu_seqlens[0] = 0;
    for (bin.sample_indices.items, 0..) |sample_idx, di| {
        const s = samples[sample_idx];
        var take_len = s.input_ids.len;
        if (bin.is_truncated and take_len > seq) take_len = seq;
        var k: usize = 0;
        while (k < take_len) : (k += 1) {
            input_ids[cursor] = s.input_ids[k];
            labels[cursor] = s.labels[k];
            position_ids[cursor] = @intCast(k);
            doc_ids[cursor] = @intCast(di);
            cursor += 1;
        }
        cu_seqlens[di + 1] = @intCast(cursor);
    }

    const filled = cursor;
    const last_doc_id: i32 = if (num_docs == 0) 0 else @intCast(num_docs - 1);
    var pad_start_pos: i32 = 0;
    if (num_docs > 0) {
        // continue position ids from last doc's end for pad tail
        const last_doc_len: usize = @intCast(cu_seqlens[num_docs] - cu_seqlens[num_docs - 1]);
        pad_start_pos = @intCast(last_doc_len);
    }

    var p: usize = filled;
    while (p < seq) : (p += 1) {
        input_ids[p] = config.pad_token_id;
        labels[p] = config.pad_label;
        position_ids[p] = pad_start_pos;
        pad_start_pos += 1;
        doc_ids[p] = last_doc_id;
    }

    const pad_count: u32 = @intCast(seq - filled);

    return PackedBuffer{
        .allocator = allocator,
        .input_ids = input_ids,
        .labels = labels,
        .position_ids = position_ids,
        .doc_ids = doc_ids,
        .cu_seqlens = cu_seqlens,
        .num_docs = num_docs,
        .pad_count = pad_count,
    };
}

pub fn pack(
    allocator: std.mem.Allocator,
    samples: []const Sample,
    config: PackConfig,
) !PackResult {
    std.debug.assert(config.max_seq_len > 0);

    const order = try allocator.alloc(usize, samples.len);
    defer allocator.free(order);
    for (order, 0..) |*o, i| o.* = i;

    const SortCtx = struct {
        samples: []const Sample,
        pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
            return ctx.samples[a].input_ids.len > ctx.samples[b].input_ids.len;
        }
    };
    std.mem.sort(usize, order, SortCtx{ .samples = samples }, SortCtx.lessThan);

    var bins: std.ArrayListUnmanaged(Bin) = .empty;
    defer {
        for (bins.items) |*b| b.sample_indices.deinit(allocator);
        bins.deinit(allocator);
    }

    var dropped_long: usize = 0;

    for (order) |idx| {
        const s = samples[idx];
        if (s.input_ids.len != s.labels.len) return error.SampleLengthMismatch;
        const len = s.input_ids.len;

        if (len > config.max_seq_len) {
            if (config.truncate_long) {
                var new_bin = Bin{};
                try new_bin.sample_indices.append(allocator, idx);
                new_bin.fill = config.max_seq_len;
                new_bin.is_truncated = true;
                new_bin.truncated_len = config.max_seq_len;
                try bins.append(allocator, new_bin);
            } else {
                dropped_long += 1;
            }
            continue;
        }

        if (len == 0) continue;

        var placed = false;
        for (bins.items) |*bin| {
            if (bin.is_truncated) continue;
            if (bin.fill + len <= config.max_seq_len) {
                try bin.sample_indices.append(allocator, idx);
                bin.fill += len;
                placed = true;
                break;
            }
        }
        if (!placed) {
            var new_bin = Bin{};
            try new_bin.sample_indices.append(allocator, idx);
            new_bin.fill = len;
            try bins.append(allocator, new_bin);
        }
    }

    var buffers = try allocator.alloc(PackedBuffer, bins.items.len);
    errdefer allocator.free(buffers);

    var built: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < built) : (j += 1) buffers[j].deinit();
    }
    for (bins.items) |*bin| {
        buffers[built] = try finalizeBin(allocator, bin, samples, config);
        built += 1;
    }

    return PackResult{
        .allocator = allocator,
        .buffers = buffers,
        .dropped_long = dropped_long,
    };
}

pub fn buildBlockDiagonalCausalMask(
    allocator: std.mem.Allocator,
    doc_ids: []const i32,
    seq_len: usize,
) ![]f32 {
    std.debug.assert(doc_ids.len == seq_len);
    const mask = try allocator.alloc(f32, seq_len * seq_len);
    @memset(mask, 0);
    var i: usize = 0;
    while (i < seq_len) : (i += 1) {
        var j: usize = 0;
        while (j <= i) : (j += 1) {
            if (doc_ids[i] == doc_ids[j]) {
                mask[i * seq_len + j] = 1.0;
            }
        }
    }
    return mask;
}

// ---------------- tests ----------------

test "one short sample fits with pad tail" {
    const alloc = std.testing.allocator;
    const ids = [_]i32{ 10, 11, 12 };
    const lbls = [_]i32{ 10, 11, 12 };
    const samples = [_]Sample{.{ .input_ids = &ids, .labels = &lbls }};
    var result = try pack(alloc, &samples, .{ .max_seq_len = 8, .pad_token_id = 0, .pad_label = -100 });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.buffers.len);
    const b = result.buffers[0];
    try std.testing.expectEqual(@as(u32, 1), b.num_docs);
    try std.testing.expectEqual(@as(u32, 5), b.pad_count);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 11, 12, 0, 0, 0, 0, 0 }, b.input_ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 11, 12, -100, -100, -100, -100, -100 }, b.labels);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 1, 2, 3, 4, 5, 6, 7 }, b.position_ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 0, 0, 0, 0, 0, 0, 0 }, b.doc_ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 3 }, b.cu_seqlens);
}

test "multiple short samples pack into one buffer" {
    const alloc = std.testing.allocator;
    const a_ids = [_]i32{ 1, 2 };
    const a_lbls = [_]i32{ 1, 2 };
    const b_ids = [_]i32{ 3, 4, 5 };
    const b_lbls = [_]i32{ 3, 4, 5 };
    const c_ids = [_]i32{6};
    const c_lbls = [_]i32{6};
    const samples = [_]Sample{
        .{ .input_ids = &a_ids, .labels = &a_lbls },
        .{ .input_ids = &b_ids, .labels = &b_lbls },
        .{ .input_ids = &c_ids, .labels = &c_lbls },
    };
    var result = try pack(alloc, &samples, .{ .max_seq_len = 8 });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.buffers.len);
    const buf = result.buffers[0];
    try std.testing.expectEqual(@as(u32, 3), buf.num_docs);
    // sorted desc: b(3), a(2), c(1); all fit -> filled = 6, pad = 2
    try std.testing.expectEqual(@as(u32, 2), buf.pad_count);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 3, 4, 5, 1, 2, 6, 0, 0 }, buf.input_ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 1, 2, 0, 1, 0, 1, 2 }, buf.position_ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 0, 0, 1, 1, 2, 2, 2 }, buf.doc_ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 3, 5, 6 }, buf.cu_seqlens);

    // monotone cu_seqlens
    var k: usize = 1;
    while (k < buf.cu_seqlens.len) : (k += 1) {
        try std.testing.expect(buf.cu_seqlens[k] >= buf.cu_seqlens[k - 1]);
    }
}

test "sample exactly equal to max_seq_len gets its own bin with no pad" {
    const alloc = std.testing.allocator;
    const ids = [_]i32{ 1, 2, 3, 4 };
    const lbls = [_]i32{ 1, 2, 3, 4 };
    const samples = [_]Sample{.{ .input_ids = &ids, .labels = &lbls }};
    var result = try pack(alloc, &samples, .{ .max_seq_len = 4 });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.buffers.len);
    const b = result.buffers[0];
    try std.testing.expectEqual(@as(u32, 1), b.num_docs);
    try std.testing.expectEqual(@as(u32, 0), b.pad_count);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 4 }, b.input_ids);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 4 }, b.cu_seqlens);
}

test "long sample truncated when truncate_long=true" {
    const alloc = std.testing.allocator;
    const long_ids = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const long_lbls = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const short_ids = [_]i32{9};
    const short_lbls = [_]i32{9};
    const samples = [_]Sample{
        .{ .input_ids = &long_ids, .labels = &long_lbls },
        .{ .input_ids = &short_ids, .labels = &short_lbls },
    };
    var result = try pack(alloc, &samples, .{ .max_seq_len = 4, .truncate_long = true });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.buffers.len);
    try std.testing.expectEqual(@as(usize, 0), result.dropped_long);

    // truncated bin is appended after sorted placement; find the 4-token one
    var found_trunc = false;
    for (result.buffers) |b| {
        if (b.pad_count == 0 and b.num_docs == 1 and b.input_ids[0] == 1) {
            try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 4 }, b.input_ids);
            try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 4 }, b.cu_seqlens);
            found_trunc = true;
        }
    }
    try std.testing.expect(found_trunc);
}

test "long sample dropped when truncate_long=false" {
    const alloc = std.testing.allocator;
    const long_ids = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const long_lbls = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const samples = [_]Sample{.{ .input_ids = &long_ids, .labels = &long_lbls }};
    var result = try pack(alloc, &samples, .{ .max_seq_len = 4, .truncate_long = false });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.buffers.len);
    try std.testing.expectEqual(@as(usize, 1), result.dropped_long);
}

test "buildBlockDiagonalCausalMask matches expected pattern" {
    const alloc = std.testing.allocator;
    const doc_ids = [_]i32{ 0, 0, 1, 1 };
    const mask = try buildBlockDiagonalCausalMask(alloc, &doc_ids, 4);
    defer alloc.free(mask);

    const expected = [_]f32{
        1, 0, 0, 0,
        1, 1, 0, 0,
        0, 0, 1, 0,
        0, 0, 1, 1,
    };
    try std.testing.expectEqualSlices(f32, &expected, mask);
}
