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

//! Shared FLAC directories grow only through requested frames. Encoded input
//! remains borrowed from the current immutable source, never copied into cache.
const std = @import("std");
const audio = @import("inference_audio");
const media = @import("antfly_media");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Admission = @import("media_opus_seek.zig").Admission;
const memory = @import("../runtime/tier/memory.zig");
const admitted = @import("../backends/admitted_allocator.zig");
const Frame = audio.flac.WindowIndex.Frame;
pub const Index = struct {
    owner: *Owner,
    pub fn init(bytes: []const u8, max_frames: usize, admission: ?Admission) !Index {
        return initParsed(try audio.flac.parseStream(bytes), bytes.len, max_frames, admission);
    }
    /// Read only metadata headers and STREAMINFO. Padding, comments, pictures,
    /// and the encoded tail remain in the current request's source.
    pub fn initSource(source: *media.source.Source, max_frames: usize, admission: ?Admission, control: Control) !Index {
        const length = std.math.cast(usize, source.length()) orelse return error.ResourceLimitExceeded;
        var signature: [4]u8 = undefined;
        try readExact(source, 0, &signature, control);
        if (!std.mem.eql(u8, &signature, "fLaC")) return error.UnsupportedStreamingAudioProfile;
        var offset: usize = 4;
        var info: ?audio.flac.StreamInfo = null;
        while (true) {
            var header: [4]u8 = undefined;
            try readExact(source, offset, &header, control);
            offset = std.math.add(usize, offset, 4) catch return error.UnsupportedAudioFormat;
            const size = (@as(usize, header[1]) << 16) | (@as(usize, header[2]) << 8) | header[3];
            if (offset > length or size > length - offset) return error.IncompleteAudioStream;
            if (header[0] & 0x7f == 0) {
                if (info != null or size != 34) return error.UnsupportedAudioFormat;
                var stream_info: [34]u8 = undefined;
                try readExact(source, offset, &stream_info, control);
                info = try audio.flac.parseStreamInfoBlock(&stream_info);
            }
            offset += size;
            if (header[0] & 0x80 != 0) break;
        }
        return initParsed(.{ .stream_info = info orelse return error.UnsupportedAudioFormat, .data_offset = offset }, length, max_frames, admission);
    }
    fn readExact(source: *media.source.Source, offset: usize, out: []u8, control: Control) !void {
        if (offset > source.length() or out.len > source.length() - offset) return error.IncompleteAudioStream;
        var at: usize = 0;
        while (at < out.len) {
            try control.check();
            const size = @min(out.len - at, @min(source.limits.max_read_bytes, source.limits.max_retained_bytes));
            if (size == 0) return error.ResourceLimitExceeded;
            var part = try source.read(offset + at, size);
            defer part.deinit();
            if (part.bytes.len == 0) return error.IncompleteAudioStream;
            @memcpy(out[at..][0..part.bytes.len], part.bytes);
            at += part.bytes.len;
        }
    }
    fn initParsed(parsed: audio.flac.ParsedStream, length: usize, max_frames: usize, admission: ?Admission) !Index {
        if (parsed.stream_info.total_samples == 0) return error.UnsupportedStreamingAudioProfile;
        const sample_count = std.math.cast(usize, parsed.stream_info.total_samples) orelse return error.ResourceLimitExceeded;
        const min_block = @max(1, parsed.stream_info.min_block_size);
        const rows = @min(max_frames, (sample_count / min_block + @intFromBool(sample_count % min_block != 0)) +| 1);
        if (rows == 0) return error.UnsupportedAudioFormat;
        var lease: ?memory.AdmissionLease = if (admission) |quota| try quota.acquire(quota.context, @sizeOf(Owner)) else null;
        errdefer if (lease) |*owned| owned.release();
        const backing = @import("antfly_platform").allocator.concurrentFallback();
        const owner = try backing.create(Owner);
        owner.* = .{ .lease = lease, .admission = admission, .allocation = .{ .backing = backing, .admission_context = if (admission) |quota| quota.context else null, .admit = if (admission) |quota| quota.acquire else null, .trim_on_free = true }, .info = parsed.stream_info, .offset = parsed.data_offset, .length = length, .max_frames = rows };
        return .{ .owner = owner };
    }
    pub fn copy(self: Index, _: std.mem.Allocator) !Index {
        _ = self.owner.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn deinit(self: *Index) void {
        if (self.owner.refs.fetchSub(1, .acq_rel) == 1) self.owner.destroy();
    }
    pub fn samples(self: Index) usize {
        return @intCast(self.owner.info.total_samples);
    }
    pub fn rate(self: Index) u32 {
        return self.owner.info.sample_rate;
    }
    // Cache accounting covers the largest directory and allocator growth
    // overhead; actual node admission follows its incremental allocations.
    pub fn estimatedBytes(self: Index) usize {
        return @sizeOf(Owner) +| self.owner.max_frames *| @sizeOf(Frame) *| 3 +| 128 * 1024;
    }
    pub fn read(self: Index, a: std.mem.Allocator, first: usize, last: usize, out: []f32, source: ?*media.source.Source, bytes: []const u8, control: Control) !void {
        try self.owner.read(a, first, last, out, source, bytes, control);
    }
};
pub const Owner = struct {
    refs: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    lease: ?memory.AdmissionLease,
    admission: ?Admission,
    allocation: admitted.AdmittedAllocator,
    info: audio.flac.StreamInfo,
    frames: std.ArrayList(Frame) = .empty,
    offset: usize,
    length: usize,
    covered: usize = 0,
    max_frames: usize,
    decoded_frames: std.atomic.Value(u64) = .init(0),
    fn allocator(self: *Owner) std.mem.Allocator {
        return if (self.admission != null) self.allocation.allocator() else @import("antfly_platform").allocator.concurrentFallback();
    }
    fn destroy(self: *Owner) void {
        self.frames.deinit(self.allocator());
        self.allocation.deinit();
        if (self.lease) |*lease| lease.release();
        @import("antfly_platform").allocator.concurrentFallback().destroy(self);
    }
    fn frame(self: *Owner, a: std.mem.Allocator, offset: usize, known_size: ?usize, source: ?*media.source.Source, bytes: []const u8, control: Control) !audio.flac.FrameDecoded {
        if (offset >= self.length) return error.IncompleteAudioStream;
        if (source == null) return audio.flac.decodeFrameAlloc(a, self.info, bytes[offset..]);
        const limit = @min(self.length - offset, known_size orelse if (self.info.max_frame_size > 0) @as(usize, self.info.max_frame_size) else 16 * 1024 * 1024);
        var buffer: std.ArrayList(u8) = .empty;
        defer buffer.deinit(a);
        var size = @min(limit, known_size orelse 4096);
        while (true) {
            try buffer.ensureTotalCapacity(a, size);
            while (buffer.items.len < size) {
                try control.check();
                var part = try source.?.read(offset + buffer.items.len, @min(size - buffer.items.len, @min(source.?.limits.max_read_bytes, source.?.limits.max_retained_bytes)));
                defer part.deinit();
                if (part.bytes.len == 0) return error.IncompleteAudioStream;
                buffer.appendSliceAssumeCapacity(part.bytes);
            }
            return audio.flac.decodeFrameAlloc(a, self.info, buffer.items) catch |err| {
                if (err != error.UnsupportedAudioFormat or size == limit or known_size != null) return err;
                size = @min(limit, size *| 2);
                continue;
            };
        }
    }
    fn read(self: *Owner, a: std.mem.Allocator, first: usize, last: usize, out: []f32, source: ?*media.source.Source, bytes: []const u8, control: Control) !void {
        if (last < first or last > self.info.total_samples or out.len != last - first) return error.InvalidAudioWindow;
        if ((if (source) |value| value.length() else bytes.len) != self.length) return error.InvalidAudioWindow;
        try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
        defer self.mutex.unlock();
        var lo: usize = 0;
        var hi = self.frames.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const row = self.frames.items[mid];
            if (row.first + row.samples <= first) lo = mid + 1 else hi = mid;
        }
        var index = lo;
        var copied: usize = 0;
        while (copied < out.len) {
            try control.check();
            const existing = index < self.frames.items.len;
            if (!existing and self.frames.items.len == self.max_frames) return error.ResourceLimitExceeded;
            const offset = if (existing) self.frames.items[index].offset else self.offset;
            var decoded = try self.frame(a, offset, if (existing) self.frames.items[index].bytes else null, source, bytes, control);
            defer decoded.deinit();
            _ = self.decoded_frames.fetchAdd(1, .monotonic);
            if (decoded.sample_rate != self.info.sample_rate or decoded.channels != self.info.channels or decoded.frame_samples == 0 or decoded.consumed == 0 or decoded.consumed > self.length - offset) return error.UnsupportedAudioFormat;
            const row = if (existing) self.frames.items[index] else Frame{ .offset = offset, .bytes = decoded.consumed, .first = self.covered, .samples = decoded.frame_samples };
            const end_sample = std.math.add(usize, row.first, row.samples) catch return error.UnsupportedAudioFormat;
            if (row.samples != decoded.frame_samples or row.bytes != decoded.consumed or end_sample > self.info.total_samples) return error.UnsupportedAudioFormat;
            if (!existing) {
                if (end_sample == self.info.total_samples and row.offset + row.bytes != self.length) return error.UnsupportedAudioFormat;
                if (row.samples < self.info.min_block_size and end_sample != self.info.total_samples) return error.UnsupportedAudioFormat;
                try self.frames.append(self.allocator(), row);
                self.covered += row.samples;
                self.offset += row.bytes;
            }
            const start = first -| row.first;
            const stop = @min(row.samples, last -| row.first);
            if (stop > start) for (start..stop) |sample| {
                var sum: f32 = 0;
                for (decoded.samples[sample * decoded.channels ..][0..decoded.channels]) |value| sum += if (std.math.isFinite(value)) std.math.clamp(value, -1, 1) else 0;
                out[copied] = sum / @as(f32, @floatFromInt(decoded.channels));
                copied += 1;
            };
            index += 1;
        }
    }
};

test "model aware media progressive FLAC directories avoid tail decoding and preserve PCM" {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(bytes);
    var decoded = try audio.decodeBounded(a, bytes, .{}, 16 * 1024 * 1024);
    defer decoded.deinit();
    var eager = try audio.flac.WindowIndex.init(a, bytes, 100000);
    defer eager.deinit();
    var index = try Index.init(bytes, 100000, null);
    defer index.deinit();
    try std.testing.expectEqual(@as(u64, 0), index.owner.decoded_frames.load(.monotonic));
    const count = @min(@as(usize, 1000), index.samples());
    const samples = try a.alloc(f32, count);
    defer a.free(samples);
    try index.read(a, 0, count, samples, null, bytes, .{});
    try std.testing.expectEqualSlices(f32, decoded.samples[0..count], samples);
    try std.testing.expect(index.owner.frames.items.len < eager.frames.len);
    const first_frames = index.owner.decoded_frames.load(.monotonic);
    var clone = try index.copy(a);
    defer clone.deinit();
    var start: usize = 0;
    while (start < index.samples()) : (start += count) {
        const last = @min(index.samples(), start + count);
        try clone.read(a, start, last, samples[0 .. last - start], null, bytes, .{});
        try std.testing.expectEqualSlices(f32, decoded.samples[start..last], samples[0 .. last - start]);
    }
    try std.testing.expectEqual(eager.frames.len, index.owner.frames.items.len);
    std.debug.print("progressive FLAC total_frames={d} cold_decoded=0 first_window_decoded={d}\n", .{ eager.frames.len, first_frames });
}

test "model aware media progressive FLAC releases admission and rejects unknown duration" {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(bytes);
    const Quota = struct {
        controller: memory.AdmissionController = .{},
        fn acquire(raw: *anyopaque, size: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 16 * 1024 * 1024 }, .{ .host_scratch_bytes = size }, false);
        }
    };
    var quota: Quota = .{};
    var index = try Index.init(bytes, 100000, .{ .context = &quota, .acquire = Quota.acquire });
    var clone = try index.copy(a);
    index.deinit();
    const samples = try a.alloc(f32, 1000);
    defer a.free(samples);
    try clone.read(a, 0, 1000, samples, null, bytes, .{});
    try std.testing.expect(quota.controller.snapshot().hostTotalBytes() > 0);
    clone.deinit();
    try std.testing.expectEqual(@as(usize, 0), quota.controller.snapshot().hostTotalBytes());
    const changed = try a.dupe(u8, bytes);
    defer a.free(changed);
    // STREAMINFO's 36-bit total-sample field occupies the low nibble of
    // byte 21 and bytes 22..25 in a standard FLAC header.
    changed[21] &= 0xf0;
    @memset(changed[22..26], 0);
    try std.testing.expectError(error.UnsupportedStreamingAudioProfile, Index.init(changed, 100000, null));
}

test "model aware media range FLAC bounds metadata reads and detects truncation cancellation and unknown duration" {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(bytes);
    const Provider = struct {
        bytes: []const u8,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: media.source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset >= self.bytes.len) return 0;
            const n = @min(out.len, self.bytes.len - @as(usize, @intCast(offset)));
            @memcpy(out[0..n], self.bytes[@intCast(offset)..][0..n]);
            return n;
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var provider = Provider{ .bytes = bytes };
    var source = media.source.Source{ .allocator = a, .identity = "flac", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = bytes.len } }, .limits = .{ .max_read_bytes = 17, .max_retained_bytes = 17, .max_reads = 100000 } };
    var index = try Index.initSource(&source, 100000, null, .{});
    defer index.deinit();
    try std.testing.expect(source.total_bytes < bytes.len / 2);
    const samples = try a.alloc(f32, 1000);
    defer a.free(samples);
    try index.read(a, 0, samples.len, samples, &source, &.{}, .{});
    var baseline = try audio.decodeBounded(a, bytes, .{}, 16 * 1024 * 1024);
    defer baseline.deinit();
    try std.testing.expectEqualSlices(f32, baseline.samples[0..samples.len], samples);
    try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
    try std.testing.expectError(error.Cancelled, Index.initSource(&source, 100000, null, .{ .check_fn = Provider.cancel }));
    provider.bytes = bytes[0..7];
    try std.testing.expectError(error.UnexpectedEndOfSource, Index.initSource(&source, 100000, null, .{}));
    provider.bytes = bytes[0 .. index.owner.frames.items[0].offset + 1];
    try std.testing.expectError(error.UnexpectedEndOfSource, index.read(a, 0, samples.len, samples, &source, &.{}, .{}));
    const changed = try a.dupe(u8, bytes);
    defer a.free(changed);
    changed[21] &= 0xf0;
    @memset(changed[22..26], 0);
    provider.bytes = changed;
    try std.testing.expectError(error.UnsupportedStreamingAudioProfile, Index.initSource(&source, 100000, null, .{}));
    // A metadata extent beyond the declared source must fail before fetching it.
    changed[5] = 0xff;
    try std.testing.expectError(error.IncompleteAudioStream, Index.initSource(&source, 100000, null, .{}));
}
