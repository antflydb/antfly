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
    pub fn source(self: *View, identity: []const u8, limits: media.source.Limits) media.source.Source {
        return .{ .allocator = self.wire.allocator, .identity = identity, .control = self.wire.control, .admission_pool = self.wire.admission_pool, .limits = limits, .storage = .{ .range = .{ .context = self, .length = self.map.len, .read_at = read } } };
    }
    fn read(raw: *anyopaque, start: u64, out: []u8, control: media.source.Control) !usize {
        const self: *View = @ptrCast(@alignCast(raw));
        if (out.len == 0) return 0;
        if (start >= self.map.len or out.len > self.map.len - start) return error.UnexpectedEndOfSource;
        var pos = start;
        var id = containing(Nal, self.map.nals, "logical", pos);
        const end = start + out.len;
        const final = self.map.nals[containing(Nal, self.map.nals, "logical", end - 1)];
        const final_es = final.elementary + ((end - final.logical) -| 4);
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
                    const last = self.map.runs[containing(Run, self.map.runs, "elementary", final_es - 1)];
                    const wire_end = last.offset(final_es - 1 - last.elementary) + 1;
                    const count: usize = @intCast(@min(wire_end - wire, 256 * 1024, self.wire.limits.max_read_bytes, self.wire.limits.max_retained_bytes -| self.wire.retained_bytes));
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

    pub fn build(self: *Index, a: std.mem.Allocator, wire: *media.source.Source, options: avc.Options) !void {
        self.* = .{ .memory = .{ .backing = a, .limit = options.index.max_index_bytes, .reservation = if (wire.admission_pool) |pool| try pool.acquire(.{}) else .{} } };
        errdefer self.deinit();
        var units = try avc.Stream.init(a, wire.identity, .{ .video = options, .retain_initial_description = true, .locations = .{ .context = self, .record = recordNal } }, self, accept);
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
    fn recordNal(raw: *anyopaque, elementary: usize, len: usize) !void {
        const self: *Index = @ptrCast(@alignCast(raw));
        try self.nals.append(self.memory.allocator(), .{ .logical = self.length, .elementary = elementary, .size = @intCast(len) });
        self.length = try std.math.add(u64, self.length, 4 + len);
    }
    fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
        const self: *Index = @ptrCast(@alignCast(raw));
        const reader = delivery.segment.?.reader;
        if (self.track == null) {
            var track = reader.track;
            track.avcc = try self.memory.allocator().dupe(u8, track.avcc);
            self.track = track;
        }
        for (reader.packets) |original| {
            var packet = original;
            packet.offset = self.packet_end;
            self.packet_end += packet.size;
            try self.packets.append(self.memory.allocator(), packet);
        }
        if (self.packet_end != self.length) return error.InvalidVideoIndex;
    }
    pub fn mapping(self: *const Index) Mapping {
        return .{ .runs = self.runs.items, .nals = self.nals.items, .len = self.length };
    }
    pub fn deinit(self: *Index) void {
        const a = self.memory.allocator();
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
