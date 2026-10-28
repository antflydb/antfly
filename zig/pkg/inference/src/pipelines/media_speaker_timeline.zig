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

//! Copy-on-write speaker timelines. Only the last block is mutable; published
//! readers pin blocks independently of cache entries and request allocators.
const std = @import("std");
const speakers = @import("inference_chunker").diarization;
const memory = @import("../runtime/tier/memory.zig");
pub const block_size = 256;
pub const Admission = struct { context: *anyopaque, acquire: *const fn (*anyopaque, usize) anyerror!memory.AdmissionLease };
const Hash = std.crypto.hash.sha2.Sha256;
pub fn hashSpan(hash: *Hash, span: speakers.Span) void {
    var raw: [20]u8 = undefined;
    std.mem.writeInt(i64, raw[0..8], span.start_ns, .little);
    std.mem.writeInt(i64, raw[8..16], span.end_ns, .little);
    std.mem.writeInt(u32, raw[16..20], span.speaker_index orelse std.math.maxInt(u32), .little);
    hash.update(&raw);
}
pub const Block = struct {
    refs: std.atomic.Value(usize) = .init(1),
    allocator: std.mem.Allocator,
    lease: ?memory.AdmissionLease = null,
    prefix: Hash = .init(.{}),
    max_label_plus_one: u32 = 0,
    spans: [block_size]speakers.Span = undefined,
    fn create(a: std.mem.Allocator, admission: ?Admission) !*Block {
        var lease: ?memory.AdmissionLease = if (admission) |admit| try admit.acquire(admit.context, @sizeOf(Block)) else null;
        errdefer if (lease) |*owned| owned.release();
        const block = try a.create(Block);
        block.* = .{ .allocator = a, .lease = lease };
        return block;
    }
    fn release(self: *Block) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) {
            const a = self.allocator;
            if (self.lease) |*owned| owned.release();
            a.destroy(self);
        }
    }
};
pub const View = struct {
    blocks: []const *Block,
    count: usize,
    pub fn at(self: View, i: usize) speakers.Span {
        std.debug.assert(i < self.count);
        return self.blocks[i / block_size].spans[i % block_size];
    }
    pub fn prefixDigest(self: View, count: usize) [32]u8 {
        std.debug.assert(count <= self.count);
        if (count == 0) {
            var hash = Hash.init(.{});
            return hash.finalResult();
        }
        const index = (count - 1) / block_size;
        var hash = self.blocks[index].prefix;
        for (self.blocks[index].spans[0 .. count - index * block_size]) |span| hashSpan(&hash, span);
        return hash.finalResult();
    }
    pub fn copy(self: View, a: std.mem.Allocator) !View {
        const blocks = try a.dupe(*Block, self.blocks);
        for (blocks) |block| _ = block.refs.fetchAdd(1, .monotonic);
        return .{ .blocks = blocks, .count = self.count };
    }
    pub fn releaseBlocks(self: View) void {
        for (self.blocks) |block| block.release();
    }
    pub fn deinit(self: View, a: std.mem.Allocator) void {
        self.releaseBlocks();
        a.free(self.blocks);
    }
    pub fn validate(self: View, registry_count: usize) !void {
        if (self.count > speakers.max_spans or self.blocks.len != (self.count + block_size - 1) / block_size) return error.InvalidSpeakerCheckpoint;
        if (self.count != 0 and self.blocks[self.blocks.len - 1].max_label_plus_one > registry_count) return error.InvalidSpeakerCheckpoint;
    }
};
pub const Timeline = struct {
    blocks: std.ArrayList(*Block) = .empty,
    count: usize = 0,
    admission: ?Admission = null,
    backing: std.mem.Allocator = std.heap.smp_allocator,
    pub fn view(self: Timeline) View {
        return .{ .blocks = self.blocks.items, .count = self.count };
    }
    pub fn len(self: Timeline) usize {
        return self.count;
    }
    pub fn at(self: Timeline, i: usize) speakers.Span {
        return self.view().at(i);
    }
    pub fn deinit(self: *Timeline, a: std.mem.Allocator) void {
        self.view().releaseBlocks();
        self.blocks.deinit(a);
        self.blocks = .empty;
        self.count = 0;
    }
    pub fn restore(self: *Timeline, a: std.mem.Allocator, view_: View) !void {
        const saved = try view_.copy(a);
        self.deinit(a);
        self.blocks = .fromOwnedSlice(@constCast(saved.blocks));
        self.count = saved.count;
    }
    fn mutableLast(self: *Timeline) !*Block {
        const index = self.blocks.items.len - 1;
        const old = self.blocks.items[index];
        if (old.refs.load(.acquire) == 1) return old;
        const block = try Block.create(self.backing, self.admission);
        block.prefix = old.prefix;
        block.max_label_plus_one = old.max_label_plus_one;
        const n = self.count - index * block_size;
        @memcpy(block.spans[0..n], old.spans[0..n]);
        self.blocks.items[index] = block;
        old.release();
        return block;
    }
    pub fn append(self: *Timeline, a: std.mem.Allocator, span: speakers.Span) !void {
        return self.appendImpl(a, span, true);
    }
    pub fn appendRecorded(self: *Timeline, a: std.mem.Allocator, span: speakers.Span) !void {
        return self.appendImpl(a, span, false);
    }
    fn appendImpl(self: *Timeline, a: std.mem.Allocator, span: speakers.Span, merge: bool) !void {
        if (span.start_ns < 0 or span.end_ns <= span.start_ns or (span.speaker_index != null and span.speaker_index.? >= 64)) return error.InvalidSpeakerTimeline;
        if (self.count > 0) {
            const last = self.at(self.count - 1);
            if (last.end_ns != span.start_ns) return error.InvalidSpeakerTimeline;
            if (merge and last.speaker_index == span.speaker_index) {
                const block = try self.mutableLast();
                block.spans[(self.count - 1) % block_size].end_ns = span.end_ns;
                return;
            }
        } else if (span.start_ns != 0) return error.InvalidSpeakerTimeline;
        if (self.count == speakers.max_spans) return error.SpeakerTimelineTooLarge;
        if (self.count % block_size == 0) {
            try self.blocks.ensureUnusedCapacity(a, 1);
            const block = try Block.create(self.backing, self.admission);
            if (self.count > 0) {
                const previous = self.blocks.items[self.blocks.items.len - 1];
                block.prefix = previous.prefix;
                for (previous.spans) |item| hashSpan(&block.prefix, item);
                block.max_label_plus_one = previous.max_label_plus_one;
            }
            self.blocks.appendAssumeCapacity(block);
        }
        const block = try self.mutableLast();
        block.spans[self.count % block_size] = span;
        if (span.speaker_index) |index| block.max_label_plus_one = @max(block.max_label_plus_one, index + 1);
        self.count += 1;
    }
    pub fn first(self: Timeline, start: i64) usize {
        var lo: usize = 0;
        var hi = self.count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.at(mid).end_ns <= start) lo = mid + 1 else hi = mid;
        }
        return lo;
    }
    pub fn label(self: Timeline, start: i64, end: i64) ?u32 {
        const i = self.first(start);
        if (start >= end or i == self.count) return null;
        const span = self.at(i);
        return if (span.start_ns <= start and span.end_ns >= end) span.speaker_index else null;
    }
    pub fn turnEnd(self: Timeline, start: i64, end: i64) i64 {
        const i = self.first(start);
        return if (i < self.count) @min(end, self.at(i).end_ns) else end;
    }
    pub fn slice(self: Timeline, a: std.mem.Allocator, start: i64, end: i64) ![]const speakers.Span {
        var out: std.ArrayList(speakers.Span) = .empty;
        errdefer out.deinit(a);
        var i = self.first(start);
        while (i < self.count) : (i += 1) {
            const span = self.at(i);
            if (span.start_ns >= end) break;
            try out.append(a, .{ .start_ns = @max(start, span.start_ns), .end_ns = @min(end, span.end_ns), .speaker_index = span.speaker_index });
        }
        return out.toOwnedSlice(a);
    }
};

test "model aware media shared speaker blocks isolate open turns and retain incremental prefix hashes" {
    const a = std.testing.allocator;
    var original = Timeline{ .backing = a };
    defer original.deinit(a);
    for (0..512) |i| try original.append(a, .{ .start_ns = @intCast(i * 10), .end_ns = @intCast((i + 1) * 10), .speaker_index = @intCast(i % 2) });
    const saved = try original.view().copy(a);
    defer saved.deinit(a);
    var resumed = Timeline{ .backing = a };
    defer resumed.deinit(a);
    try resumed.restore(a, saved);
    try std.testing.expect(resumed.blocks.items[0] == saved.blocks[0]);
    try std.testing.expect(resumed.blocks.items[1] == saved.blocks[1]);
    try resumed.append(a, .{ .start_ns = 5120, .end_ns = 5130, .speaker_index = 1 });
    try std.testing.expect(resumed.blocks.items[0] == saved.blocks[0]);
    try std.testing.expect(resumed.blocks.items[1] != saved.blocks[1]);
    try std.testing.expectEqual(@as(i64, 5120), saved.at(511).end_ns);
    try std.testing.expectEqual(@as(i64, 5130), resumed.at(511).end_ns);
    try resumed.append(a, .{ .start_ns = 5130, .end_ns = 5140, .speaker_index = 0 });
    for ([_]usize{ 0, 1, 255, 256, 257, 511, 512, 513 }) |count| {
        var hash = Hash.init(.{});
        for (0..count) |i| hashSpan(&hash, resumed.at(i));
        try std.testing.expectEqualSlices(u8, &hash.finalResult(), &resumed.view().prefixDigest(count));
    }
    try saved.validate(2);
    try std.testing.expectError(error.InvalidSpeakerCheckpoint, saved.validate(1));
    const slice = try resumed.slice(a, 5105, 5135);
    defer a.free(slice);
    try std.testing.expectEqual(@as(usize, 3), slice.len);
    try std.testing.expectEqual(@as(i64, 5105), slice[0].start_ns);
    try std.testing.expectEqual(@as(i64, 5135), slice[2].end_ns);
}

test "model aware media speaker block allocation faults release shared roots and writable tails" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var timeline_ = Timeline{ .backing = a };
            defer timeline_.deinit(a);
            for (0..257) |i| try timeline_.append(a, .{ .start_ns = @intCast(i), .end_ns = @intCast(i + 1), .speaker_index = @intCast(i % 2) });
            const view_ = try timeline_.view().copy(a);
            defer view_.deinit(a);
            var next = Timeline{ .backing = a };
            defer next.deinit(a);
            try next.restore(a, view_);
            try next.append(a, .{ .start_ns = 257, .end_ns = 258, .speaker_index = 0 });
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "model aware media shared speaker blocks keep admission until the last reader releases" {
    const a = std.testing.allocator;
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        fn acquire(raw: *anyopaque, size: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 1024 * 1024 }, .{ .host_scratch_bytes = size }, false);
        }
    };
    var owner: Owner = .{};
    var timeline_ = Timeline{ .backing = a, .admission = .{ .context = &owner, .acquire = Owner.acquire } };
    defer timeline_.deinit(a);
    try timeline_.append(a, .{ .start_ns = 0, .end_ns = 10, .speaker_index = 0 });
    const saved = try timeline_.view().copy(a);
    defer saved.deinit(a);
    try timeline_.append(a, .{ .start_ns = 10, .end_ns = 20, .speaker_index = 0 });
    try std.testing.expectEqual(@as(usize, @sizeOf(Block) * 2), owner.controller.snapshot().hostTotalBytes());
    timeline_.deinit(a);
    try std.testing.expectEqual(@as(usize, @sizeOf(Block)), owner.controller.snapshot().hostTotalBytes());
}
