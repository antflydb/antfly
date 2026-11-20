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

const WireSpan = struct {
    start: u64,
    end: u64,
    fn size(self: WireSpan) u64 {
        return self.end - self.start;
    }
};

/// When both the call count and byte budget constrain a gather, choose the
/// physical ranges before spending either. A group of adjacent extents costs
/// its wire width in bytes and ceil(width / capacity) calls. Every useful cut
/// is either a skipped gap or a capacity split inside one such group.
fn constrainedRanges(a: std.mem.Allocator, map: Mapping, first: usize, last: usize, start: u64, end: u64, capacity: usize, budget: media.source.Limits, control: media.source.Control) ![]WireSpan {
    if (capacity == 0) return error.ResourceLimitExceeded;
    var spans: std.ArrayList(WireSpan) = .empty;
    defer spans.deinit(a);
    var payload: u64 = 0;
    for (map.nals[first .. last + 1]) |nal| {
        try control.check();
        const base = nal.logical + 4;
        const lo = @max(start, base);
        const hi = @min(end, base + nal.size);
        if (lo >= hi) continue;
        var es = nal.elementary + (lo - base);
        var needed = hi - lo;
        var k = containing(Run, map.runs, "elementary", es);
        while (needed != 0) {
            if (spans.items.len % 256 == 0) try control.check();
            const run = map.runs[k];
            const rel = es - run.elementary;
            const n = @min(needed, run.len - rel, 184 - rel % 184);
            const wire = run.offset(rel);
            if (spans.items.len != 0 and spans.items[spans.items.len - 1].end == wire)
                spans.items[spans.items.len - 1].end += n
            else
                try spans.append(a, .{ .start = wire, .end = wire + n });
            es += n;
            needed -= n;
            payload += n;
            if (rel + n == run.len) k += 1;
        }
    }
    if (payload > budget.max_total_bytes or budget.max_reads == 0) return error.ResourceLimitExceeded;
    const slack = budget.max_total_bytes - payload;
    var ranges: std.ArrayList(WireSpan) = .empty;
    defer ranges.deinit(a);
    var calls: u64 = 0;
    var overhead: u64 = 0;
    // The linear plan normally suffices. Bridge a gap when it saves a call
    // within the available overhead. Preserve remaining slack otherwise.
    for (spans.items) |span| {
        const separate = std.math.divCeil(u64, span.size(), capacity) catch unreachable;
        if (ranges.getLastOrNull()) |previous| {
            const before = std.math.divCeil(u64, previous.size(), capacity) catch unreachable;
            const joined = std.math.divCeil(u64, span.end - previous.start, capacity) catch unreachable;
            const gap = span.start - previous.end;
            if (joined < before + separate and gap <= slack - overhead) {
                ranges.items[ranges.items.len - 1].end = span.end;
                overhead += gap;
                calls += joined - before;
                continue;
            }
        }
        try ranges.append(a, span);
        calls += separate;
    }
    if (calls <= budget.max_reads) return ranges.toOwnedSlice(a);
    ranges.clearRetainingCapacity();

    // Unconstrained by bytes, starting each read at the first uncovered
    // payload byte gives the minimum call count. Rule out impossible maps
    // before exploring byte/call tradeoffs (especially a one-call budget).
    var covered: u64 = 0;
    var minimum_calls: u64 = 0;
    for (spans.items) |span| {
        if (span.end <= covered) continue;
        const next = @max(span.start, covered);
        const n = std.math.divCeil(u64, span.end - next, capacity) catch unreachable;
        minimum_calls += n;
        if (minimum_calls > budget.max_reads) return error.ResourceLimitExceeded;
        covered = next +| (n *| capacity);
    }

    // Even without a width limit, at most calls-1 gaps can be skipped.
    // Cutting the largest gaps gives a lower bound on bytes and frequently
    // produces the optimal schedule directly.
    const gaps = try a.alloc(usize, spans.items.len - 1);
    defer a.free(gaps);
    for (gaps, 1..) |*gap, i| gap.* = i;
    std.mem.sort(usize, gaps, spans.items, struct {
        fn less(items: []WireSpan, l: usize, r: usize) bool {
            return items[l].start - items[l - 1].end > items[r].start - items[r - 1].end;
        }
    }.less);
    const cuts = gaps[0..@min(gaps.len, budget.max_reads - 1)];
    std.mem.sort(usize, cuts, {}, std.sort.asc(usize));
    var base: usize = 0;
    var minimum_bytes: u64 = 0;
    calls = 0;
    for (0..cuts.len + 1) |i| {
        const stop = if (i == cuts.len) spans.items.len else cuts[i];
        const group = WireSpan{ .start = spans.items[base].start, .end = spans.items[stop - 1].end };
        try ranges.append(a, group);
        calls += std.math.divCeil(u64, group.size(), capacity) catch unreachable;
        minimum_bytes += group.size();
        base = stop;
    }
    if (minimum_bytes > budget.max_total_bytes) return error.ResourceLimitExceeded;
    if (calls <= budget.max_reads) return ranges.toOwnedSlice(a);
    ranges.clearRetainingCapacity();

    // For competing gaps retain only nondominated (calls, overhead) choices.
    // Trace through completed prefixes so a later cheap join can displace an
    // earlier expensive one. Gaps at least one read wide always split a group.
    const Choice = struct { calls: u64, extra: u64, first: usize, parent: usize };
    const frontiers = try a.alloc(std.ArrayList(Choice), spans.items.len + 1);
    defer a.free(frontiers);
    @memset(frontiers, .empty);
    defer for (frontiers) |*frontier| frontier.deinit(a);
    try frontiers[0].append(a, .{ .calls = 0, .extra = 0, .first = 0, .parent = 0 });
    var remaining = payload;
    for (spans.items, 1..) |span, stop| {
        try control.check();
        remaining -= span.size();
        const suffix_calls = std.math.divCeil(u64, remaining, capacity) catch unreachable;
        const frontier = &frontiers[stop];
        var i = stop;
        var bytes: u64 = 0;
        while (i > 0) {
            i -= 1;
            const part = spans.items[i];
            bytes += part.size();
            const width = span.end - part.start;
            const extra = width - bytes;
            const reads = std.math.divCeil(u64, width, capacity) catch unreachable;
            if (extra > slack or reads + suffix_calls > budget.max_reads) break;
            if (i % 256 == 0) try control.check();
            for (frontiers[i].items, 0..) |prefix, parent| {
                const cost = prefix.extra + extra;
                const count = prefix.calls + reads;
                if (cost > slack or count + suffix_calls > budget.max_reads) continue;
                for (frontier.items) |other| {
                    if (other.calls <= count and other.extra <= cost) break;
                } else {
                    var k = frontier.items.len;
                    while (k != 0) {
                        k -= 1;
                        const other = frontier.items[k];
                        if (count <= other.calls and cost <= other.extra) _ = frontier.swapRemove(k);
                    }
                    try frontier.append(a, .{ .calls = count, .extra = cost, .first = i, .parent = parent });
                }
            }
            if (i > 0 and part.start - spans.items[i - 1].end >= capacity) break;
        }
    }
    var stop = spans.items.len;
    if (frontiers[stop].items.len == 0) return error.ResourceLimitExceeded;
    var chosen: usize = 0;
    for (frontiers[stop].items, 0..) |choice, i| {
        if (choice.calls < frontiers[stop].items[chosen].calls) chosen = i;
    }
    while (stop != 0) {
        const choice = frontiers[stop].items[chosen];
        try ranges.append(a, .{ .start = spans.items[choice.first].start, .end = spans.items[stop - 1].end });
        stop = choice.first;
        chosen = choice.parent;
    }
    std.mem.reverse(WireSpan, ranges.items);
    return ranges.toOwnedSlice(a);
}

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
        const available = self.wire.remaining();
        const hard_capacity = @min(available.max_read_bytes, available.max_retained_bytes);
        const physical_runs = if (final_es > self.map.nals[id].elementary)
            containing(Run, self.map.runs, "elementary", final_es - 1) - containing(Run, self.map.runs, "elementary", self.map.nals[id].elementary) + 1
        else
            0;
        // Each intersection of a NAL and a mapping run adds at most two
        // partial strides. With sufficient calls, skip expensive multiplex gaps.
        const extent_bound = 2 * (physical_runs + final_id - id + 1) + out.len / 184;
        const calls_tight = available.max_reads < extent_bound + out.len / @max(1, hard_capacity);
        var scratch = media.admission.TrackedAllocator{ .backing = self.wire.allocator, .limit = std.math.maxInt(usize), .reservation = .{} };
        defer scratch.reservation.deinit();
        var constrained: ?[]WireSpan = null;
        defer if (constrained) |ranges| scratch.allocator().free(ranges);
        var range_index: usize = 0;
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
                    const limits = self.wire.remaining();
                    const remaining = limits.max_total_bytes;
                    const capacity = @min(if (calls_tight) hard_capacity else @min(256 * 1024, hard_capacity), limits.max_retained_bytes, remaining);
                    // Coalesce nearby extents while there is room to pay for
                    // transport overhead in addition to all remaining payload.
                    const whole_span_fits = final_wire - wire <= remaining;
                    const count: usize = physical: {
                        if (calls_tight and !whole_span_fits and constrained == null) {
                            if (self.wire.admission_pool) |pool| scratch.reservation = try pool.acquire(.{});
                            constrained = constrainedRanges(scratch.allocator(), self.map, id, final_id, pos, end, hard_capacity, limits, control) catch |err|
                                return if (err == error.OutOfMemory) scratch.denied orelse err else err;
                        }
                        if (constrained) |ranges| {
                            while (wire >= ranges[range_index].end) range_index += 1;
                            break :physical @intCast(@min(ranges[range_index].end - wire, capacity));
                        }
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
                            if (next.wire - wire >= capacity or (next.wire - wire_end > 188 and !calls_tight)) break;
                            const next_es = @min(fetch_end, next.elementary + next.len);
                            const next_end = next.offset(next_es - 1 - next.elementary) + 1;
                            if (!whole_span_fits and next_end - wire - (next_es - es) > slack) break;
                            last_id += 1;
                            last_es = next_es;
                            wire_end = next_end;
                        }
                        if (!whole_span_fits and wire_end - wire - (last_es - es) > slack)
                            wire_end = wire + @min(run.len - relative, 184 - relative % 184, stop - pos);
                        break :physical @intCast(@min(wire_end - wire, capacity));
                    };
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

test "seekable transport budgets read calls across multiplex gaps" {
    var bytes: [1321]u8 = @splat(0);
    bytes[4] = 9;
    bytes[380] = 10;
    bytes[1320] = 11;
    for ([_]u64{ 2048, 378 }) |budget| {
        var wire = media.source.Source{ .allocator = std.testing.allocator, .storage = .{ .borrowed = &bytes }, .identity = "gaps", .limits = .{ .max_total_bytes = budget, .max_reads = 2, .max_read_bytes = 512 } };
        var view = View{ .wire = &wire, .map = .{ .len = 7, .nals = &.{.{ .logical = 0, .elementary = 0, .size = 3 }}, .runs = &.{ .{ .elementary = 0, .wire = 4, .len = 1 }, .{ .elementary = 1, .wire = 380, .len = 1 }, .{ .elementary = 2, .wire = 1320, .len = 1 } } } };
        var source = view.source("mapped", .{});
        var payload = try source.read(0, 7);
        defer payload.deinit();
        try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 3, 9, 10, 11 }, payload.bytes);
        try std.testing.expectEqual(@as(usize, 2), wire.reads);
        try std.testing.expectEqual(@as(u64, 378), wire.total_bytes);
    }
}

test "seekable transport constrained range plans match all partitions of small maps" {
    const a = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(38272);
    const random = prng.random();
    for (0..500) |_| {
        var runs: [7]Run = undefined;
        var wire: u64 = 0;
        var es: u64 = 0;
        for (&runs) |*run| {
            const n = random.intRangeAtMost(u8, 1, 8);
            wire += random.intRangeAtMost(u8, 1, 20);
            run.* = .{ .elementary = es, .wire = wire, .len = n };
            wire += n;
            es += n;
        }
        const capacity = random.intRangeAtMost(usize, 3, 40);
        const limit = random.intRangeAtMost(usize, 1, 10);
        const budget = random.intRangeAtMost(u64, es, wire);
        var optimum: u64 = std.math.maxInt(u64);
        for (0..1 << (runs.len - 1)) |cuts| {
            var start = runs[0].wire;
            var bytes: u64 = 0;
            var calls: u64 = 0;
            for (runs, 0..) |run, i| {
                if (i + 1 != runs.len and (cuts >> @intCast(i)) & 1 == 0) continue;
                const size = run.wire + run.len - start;
                bytes += size;
                calls += try std.math.divCeil(u64, size, capacity);
                if (i + 1 < runs.len) start = runs[i + 1].wire;
            }
            if (calls <= limit) optimum = @min(optimum, bytes);
        }
        const map = Mapping{ .runs = &runs, .nals = &.{.{ .logical = 0, .elementary = 0, .size = @intCast(es) }}, .len = es + 4 };
        const result = constrainedRanges(a, map, 0, 0, 4, es + 4, capacity, .{ .max_reads = limit, .max_total_bytes = budget }, .{});
        if (optimum > budget) {
            try std.testing.expectError(error.ResourceLimitExceeded, result);
        } else {
            const ranges = try result;
            defer a.free(ranges);
            var bytes: u64 = 0;
            var calls: u64 = 0;
            for (ranges) |range| {
                bytes += range.size();
                calls += try std.math.divCeil(u64, range.size(), capacity);
            }
            try std.testing.expect(bytes <= budget and calls <= limit);
            for (runs) |run| {
                for (ranges) |range| {
                    if (range.start <= run.wire and range.end >= run.wire + run.len) break;
                } else return error.MissingWireExtent;
            }
        }
    }
}
