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

//! Seekable length-prefixed AVC over immutable 188-byte MPEG transport streams.
const std = @import("std");
const media = @import("antfly_media");
const avc = @import("transport_avc.zig");

/// A normalized packet contains synthetic four-byte NAL lengths and borrowed
/// elementary-stream bytes. Full consecutive TS payloads share one stride run.
const Nal = struct { logical: u64, elementary: u64, size: u32 };
const Run = struct {
    elementary: u64,
    wire: u64,
    len: u64,
    fn offset(self: Run, relative: u64) u64 {
        return self.wire + relative / 184 * 188 + relative % 184;
    }
};

fn containing(comptime T: type, items: []const T, comptime field: []const u8, pos: u64) usize {
    var lo: usize = 0;
    var hi = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (@field(items[mid], field) <= pos) lo = mid + 1 else hi = mid;
    }
    return lo - 1;
}

pub const Mapping = struct {
    nals: []const Nal,
    runs: []const Run,
    len: u64,
    /// Copies into the versioned container cache's arena.
    pub fn copy(self: Mapping, arena: std.mem.Allocator) !Mapping {
        return .{ .nals = try arena.dupe(Nal, self.nals), .runs = try arena.dupe(Run, self.runs), .len = self.len };
    }
};

/// Bound for one request. Every underlying read retains the provider's source
/// budget, cancellation, and immutable-version checks.
pub const View = struct {
    map: Mapping,
    wire: *media.source.Source,
    // Reader copies can issue simultaneous normalized reads. The physical
    // provider and its request-wide byte/read counters have one owner.
    mutex: std.Io.Mutex = .init,
    pub fn source(self: *View, identity: []const u8, limits: media.source.Limits) media.source.Source {
        return .{ .allocator = self.wire.allocator, .identity = identity, .control = self.wire.control, .admission_pool = self.wire.admission_pool, .limits = limits, .storage = .{ .range = .{ .context = self, .length = self.map.len, .read_at = read } } };
    }
    fn read(raw: *anyopaque, start: u64, out: []u8, control: media.source.Control) !usize {
        const self: *View = @ptrCast(@alignCast(raw));
        if (out.len == 0) return 0;
        const io = std.Io.Threaded.global_single_threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (start >= self.map.len or out.len > self.map.len - start) return error.UnexpectedEndOfSource;
        var pos = start;
        var id = containing(Nal, self.map.nals, "logical", pos);
        const end = start + out.len;
        const final_id = containing(Nal, self.map.nals, "logical", end - 1);
        const final = self.map.nals[final_id];
        // A read ending inside a synthetic prefix requires no wire bytes
        // from that NAL, including any preceding Annex B padding.
        const final_es = if (end > final.logical + 4) final.elementary + (end - final.logical - 4) else if (final_id > 0) self.map.nals[final_id - 1].elementary + self.map.nals[final_id - 1].size else 0;
        const final_wire: u64 = if (final_es == 0) 0 else blk: {
            const last = self.map.runs[containing(Run, self.map.runs, "elementary", final_es - 1)];
            break :blk last.offset(final_es - 1 - last.elementary) + 1;
        };
        // A single wire read can serve many contiguous TS packets and NALs.
        var lease: ?media.source.Lease = null;
        defer if (lease) |*held| held.deinit();
        var wire_start: u64 = 0;
        while (pos < end) : (id += 1) {
            try control.check();
            const nal = self.map.nals[id];
            const payload = nal.logical + 4;
            if (pos < payload) {
                var length: [4]u8 = undefined;
                std.mem.writeInt(u32, &length, nal.size, .big);
                const n: usize = @intCast(@min(end, payload) - pos);
                @memcpy(out[@intCast(pos - start)..][0..n], length[@intCast(pos - nal.logical)..][0..n]);
                pos += n;
            }
            const stop = @min(end, payload + nal.size);
            if (pos >= stop) continue;
            var es = nal.elementary + (pos - payload);
            var run_id = containing(Run, self.map.runs, "elementary", es);
            while (pos < stop) {
                const run = self.map.runs[run_id];
                const relative = es - run.elementary;
                const wire = run.offset(relative);
                if (lease == null or wire < wire_start or wire - wire_start >= lease.?.bytes.len) {
                    if (lease) |*held| held.deinit();
                    lease = null;
                    try control.check();
                    const remaining = self.wire.limits.max_total_bytes -| self.wire.total_bytes;
                    const capacity = @min(256 * 1024, self.wire.limits.max_read_bytes, self.wire.limits.max_retained_bytes -| self.wire.retained_bytes, remaining);
                    // Coalesce nearby extents while there is room to pay for
                    // transport overhead in addition to all remaining payload.
                    const whole_span_fits = final_wire - wire <= remaining;
                    const synthetic = 4 * (final_id - id) -| (final.logical + 4 -| end);
                    const slack = remaining -| (end - pos - synthetic);
                    var last_id = run_id;
                    // Under a tight byte budget do not prefetch Annex B start
                    // codes or padding between this NAL and the next one.
                    const fetch_end = if (whole_span_fits) final_es else es + (stop - pos);
                    var last_es = @min(fetch_end, run.elementary + run.len);
                    var wire_end = run.offset(last_es - 1 - run.elementary) + 1;
                    while (last_es < fetch_end and wire_end - wire < capacity) {
                        const next = self.map.runs[last_id + 1];
                        if (next.wire - wire_end > 188 and !(whole_span_fits and final_wire - wire <= capacity and self.wire.limits.max_reads -| self.wire.reads == 1)) break;
                        const next_es = @min(fetch_end, next.elementary + next.len);
                        const next_end = next.offset(next_es - 1 - next.elementary) + 1;
                        if (!whole_span_fits and next_end - wire - (next_es - es) > slack) break;
                        last_id += 1;
                        last_es = next_es;
                        wire_end = next_end;
                    }
                    if (!whole_span_fits and wire_end - wire - (last_es - es) > slack)
                        wire_end = wire + @min(run.len - relative, 184 - relative % 184, stop - pos);
                    const count: usize = @intCast(@min(wire_end - wire, capacity));
                    if (count == 0) return error.ResourceLimitExceeded;
                    lease = try self.wire.read(wire, count);
                    wire_start = wire;
                }
                const offset: usize = @intCast(wire - wire_start);
                const n: usize = @intCast(@min(stop - pos, run.len - relative, 184 - relative % 184, lease.?.bytes.len - offset));
                @memcpy(out[@intCast(pos - start)..][0..n], lease.?.bytes[offset..][0..n]);
                pos += n;
                es += n;
                if (relative + n == run.len) run_id += 1;
            }
        }
        return out.len;
    }
};

pub const Index = struct {
    memory: media.admission.TrackedAllocator,
    packets: std.ArrayList(media.mp4.Packet) = .empty,
    nals: std.ArrayList(Nal) = .empty,
    runs: std.ArrayList(Run) = .empty,
    track: ?media.mp4.Track = null,
    length: u64 = 0,
    packet_end: u64 = 0,
    headers: std.ArrayList([]u8) = .empty,
    current_header: std.ArrayList(u8) = .empty,

    capture_headers: bool = true,

    pub fn build(self: *Index, a: std.mem.Allocator, wire: *media.source.Source, options: avc.Options) !void {
        return self.buildForReader(a, wire, options, true);
    }

    pub fn buildForReader(self: *Index, a: std.mem.Allocator, wire: *media.source.Source, options: avc.Options, capture_headers: bool) !void {
        self.* = .{ .capture_headers = capture_headers, .memory = .{ .backing = a, .limit = options.index.max_index_bytes, .reservation = if (wire.admission_pool) |pool| try pool.acquire(.{}) else .{} } };
        errdefer self.deinit();
        var units = try avc.Stream.initIndex(a, wire.identity, options, .{ .context = self, .track = recordTrack, .nal = recordNal, .packet = recordPacket });
        defer if (!units.ended) units.deinit();
        var consumer = units.consumer();
        consumer.extent = recordRun;
        consumer.extent_context = self;
        avc.scanSource(a, wire, options, &units, consumer) catch |err|
            return if (err == error.OutOfMemory) self.memory.denied orelse err else err;
        if (self.track == null or self.packet_end != self.length or self.runs.items.len == 0) return error.InvalidVideoIndex;
    }
    fn recordRun(raw: *anyopaque, elementary: usize, wire: usize, len: usize) !void {
        const self: *Index = @ptrCast(@alignCast(raw));
        if (self.runs.items.len > 0) {
            const last = &self.runs.items[self.runs.items.len - 1];
            if (elementary != last.elementary + last.len) return error.InvalidVideoIndex;
            if (last.len % 184 == 0 and wire == last.offset(last.len)) {
                last.len += len;
                return;
            }
        } else if (elementary != 0) return error.InvalidVideoIndex;
        try self.runs.append(self.memory.allocator(), .{ .wire = wire, .elementary = elementary, .len = len });
    }
    fn recordNal(raw: *anyopaque, elementary: usize, nal: []const u8) !void {
        const self: *Index = @ptrCast(@alignCast(raw));
        const len = nal.len;
        const a = self.memory.allocator();
        if (self.capture_headers) {
            const retained = try @import("h264_prefix.zig").retainedSize(nal[0], len);
            var prefix: [4]u8 = undefined;
            std.mem.writeInt(u32, &prefix, @intCast(retained), .big);
            try self.current_header.appendSlice(a, &prefix);
            try self.current_header.appendSlice(a, nal[0..retained]);
        }
        try self.nals.append(self.memory.allocator(), .{ .logical = self.length, .elementary = elementary, .size = @intCast(len) });
        self.length = try std.math.add(u64, self.length, 4 + len);
    }
    fn recordTrack(raw: *anyopaque, track: media.mp4.Track) !void {
        const self: *Index = @ptrCast(@alignCast(raw));
        var owned = track;
        owned.avcc = try self.memory.allocator().dupe(u8, track.avcc);
        self.track = owned;
    }
    fn recordPacket(raw: *anyopaque, original: media.mp4.Packet) !void {
        const self: *Index = @ptrCast(@alignCast(raw));
        var packet = original;
        packet.offset = self.packet_end;
        self.packet_end += packet.size;
        if (self.packet_end != self.length) return error.InvalidVideoIndex;
        const a = self.memory.allocator();
        try self.packets.append(a, packet);
        if (!self.capture_headers) return;
        try self.headers.ensureUnusedCapacity(a, 1);
        self.headers.appendAssumeCapacity(try self.current_header.toOwnedSlice(a));
    }
    pub fn releaseHeaders(self: *Index) void {
        const a = self.memory.allocator();
        for (self.headers.items) |bytes| a.free(bytes);
        self.headers.deinit(a);
        self.headers = .empty;
        self.current_header.deinit(a);
        self.current_header = .empty;
    }
    pub fn mapping(self: *const Index) Mapping {
        return .{ .runs = self.runs.items, .nals = self.nals.items, .len = self.length };
    }
    pub fn deinit(self: *Index) void {
        const a = self.memory.allocator();
        self.releaseHeaders();
        if (self.track) |track| a.free(track.avcc);
        self.runs.deinit(a);
        self.nals.deinit(a);
        self.packets.deinit(a);
        self.memory.reservation.deinit();
    }
};

test "seekable transport matches normalized packet payloads and every partial NAL prefix" {
    const a = std.testing.allocator;
    inline for (.{ "closed", "bframes-split" }) |name| {
        const bytes = @embedFile("../testdata/transport/" ++ name ++ ".ts");
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 8 * 1024 * 1024 } };
        defer std.debug.assert(pool.snapshot().host_bytes == 0);
        var wire = media.source.Source{ .allocator = a, .identity = name, .admission_pool = &pool, .storage = .{ .borrowed = bytes } };
        var index: Index = undefined;
        try index.build(a, &wire, .{ .stream = .{ .admission_pool = &pool } });
        defer index.deinit();
        var expected = try avc.Reader.init(a, name, bytes, .{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualSlices(u8, expected.reader.track.avcc, index.track.?.avcc);
        try std.testing.expectEqual(expected.reader.packets.len, index.packets.items.len);
        var view = View{ .map = index.mapping(), .wire = &wire };
        var virtual = view.source(name, .{});
        // Registry and picture planning consume the headers captured during
        // demux even if there is no provider budget left for a second scan.
        {
            virtual.limits.max_reads = 0;
            defer virtual.limits.max_reads = (media.source.Limits{}).max_reads;
            var reader = expected.reader;
            reader.input = &virtual;
            reader.packets = index.packets.items;
            const pictures = @import("h264_pictures.zig");
            const options = pictures.Options.forTrack(reader.packets.len);
            const registry = try @import("h264_dynamic.zig").Session.initWithHeaders(a, &reader, options.decode, index.headers.items);
            defer registry.deinit();
            var logical = try pictures.planning(a, &reader, options, registry);
            defer logical.deinit();
            try std.testing.expect(logical.pictures.len > 0);
            try std.testing.expectEqual(@as(usize, 0), virtual.reads);
            index.releaseHeaders();
        }
        // A physical provider can lend much smaller reads than a coded packet.
        wire.limits.max_read_bytes = 377;
        for (index.packets.items, 0..) |packet, id| {
            var oracle = try expected.reader.readPacket(id);
            defer oracle.deinit();
            var actual = try virtual.read(packet.offset, packet.size);
            defer actual.deinit();
            try std.testing.expectEqualSlices(u8, oracle.bytes, actual.bytes);
            try std.testing.expectEqual(expected.reader.packets[id].pts, packet.pts);
            try std.testing.expectEqual(expected.reader.packets[id].duration, packet.duration);
            for (1..4) |skip| {
                var partial = try virtual.read(packet.offset + skip, packet.size - skip);
                defer partial.deinit();
                try std.testing.expectEqualSlices(u8, oracle.bytes[skip..], partial.bytes);
            }
        }
    }
}

test "seekable transport skips multiplexed padding under exact payload budget" {
    const a = std.testing.allocator;
    var bytes: [4096]u8 = @splat(0);
    bytes[0] = 9;
    bytes[80] = 10;
    bytes[4000] = 11;
    var wire = media.source.Source{ .allocator = a, .storage = .{ .borrowed = &bytes }, .identity = "padded", .limits = .{ .max_total_bytes = 3 } };
    var view = View{ .wire = &wire, .map = .{ .len = 7, .nals = &.{.{ .logical = 0, .elementary = 0, .size = 3 }}, .runs = &.{ .{ .elementary = 0, .wire = 0, .len = 1 }, .{ .elementary = 1, .wire = 80, .len = 1 }, .{ .elementary = 2, .wire = 4000, .len = 1 } } } };
    var source = view.source("mapped", .{});
    var payload = try source.read(0, 7);
    defer payload.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 3, 9, 10, 11 }, payload.bytes);
    try std.testing.expectEqual(@as(u64, 3), wire.total_bytes);

    // Two NALs in one physical run have a synthetic length between them.
    var packed_wire = media.source.Source{ .allocator = a, .storage = .{ .borrowed = &bytes }, .identity = "annex", .limits = .{ .max_total_bytes = 2 } };
    var packed_view = View{ .wire = &packed_wire, .map = .{ .len = 10, .nals = &.{ .{ .logical = 0, .elementary = 0, .size = 1 }, .{ .logical = 5, .elementary = 80, .size = 1 } }, .runs = &.{.{ .elementary = 0, .wire = 0, .len = 81 }} } };
    var mapped = packed_view.source("annex-mapped", .{});
    var both = try mapped.read(0, 10);
    defer both.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 1, 9, 0, 0, 0, 1, 10 }, both.bytes);
    packed_wire.limits.max_total_bytes = 100;
    const before = packed_wire.total_bytes;
    var prefix = try mapped.read(4, 4);
    defer prefix.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 9, 0, 0, 0 }, prefix.bytes);
    try std.testing.expectEqual(@as(u64, 1), packed_wire.total_bytes - before);
}

test "seekable transport coalesces prefixes and TS headers under one exact read" {
    var bytes: [192]u8 = @splat(0);
    bytes[183] = 9;
    bytes[191] = 10;
    var wire = media.source.Source{ .allocator = std.testing.allocator, .storage = .{ .borrowed = &bytes }, .identity = "stride", .limits = .{ .max_total_bytes = 9, .max_reads = 1 } };
    var view = View{ .wire = &wire, .map = .{ .len = 10, .nals = &.{ .{ .logical = 0, .elementary = 183, .size = 1 }, .{ .logical = 5, .elementary = 187, .size = 1 } }, .runs = &.{.{ .elementary = 0, .wire = 0, .len = 188 }} } };
    var source = view.source("mapped", .{});
    var payload = try source.read(0, 10);
    defer payload.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 1, 9, 0, 0, 0, 1, 10 }, payload.bytes);
    try std.testing.expectEqual(@as(usize, 1), wire.reads);
}
