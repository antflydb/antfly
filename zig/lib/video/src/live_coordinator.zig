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

//! Request-local published-unit retention, reload pumping and transactional windows.
const std = @import("std");
const media = @import("antfly_media");
const windows = @import("live_windows.zig");
const sampling = @import("sampling.zig");
const h264 = @import("h264.zig");
const pictures = @import("h264_pictures.zig");
const avc = @import("avc.zig");
pub const Options = struct {
    window: windows.Options = .{},
    duration_ns: i64 = 2 * std.time.ns_per_s,
    step_ns: u64 = std.time.ns_per_s,
    max_frames: usize = 32,
    max_retained_bytes: usize = 64 * 1024 * 1024,
    max_windows: usize = 4096,
};
pub const Prepared = struct {
    window: windows.Window,
    policy: sampling.TimestampPolicy,
    ordinal: usize,
    source_start_ns: i64,
    source_end_ns: i64,
    has_pictures: bool,
    dash_timing: ?media.hls.DashTiming = null,
    hls_timing: ?media.live_mp4.HlsTiming = null,
    pub fn deinit(self: *Prepared) void {
        self.window.deinit();
        self.* = undefined;
    }
};
/// Single consumer. Unit ownership transfers only after successful append.
/// A prepared output owns independent storage; commit only after delivery succeeds.
pub const Coordinator = struct {
    allocator: std.mem.Allocator,
    options: Options,
    units: [256]media.live_mp4.Segment = undefined,
    count: usize = 0,
    retained_bytes: usize = 0,
    reservation: media.admission.Token = .{},
    start_ns: ?i64 = null,
    sealed_ns: ?i64 = null,
    tail_ns: i64 = 0,
    ordinal: usize = 0,
    finished: bool = false,
    epoch_committed: bool = false,
    // Pending fetched units survive admission/configuration failure and retry.
    pending: ?media.live_mp4.Segment = null,
    pub fn init(a: std.mem.Allocator, options: Options) !Coordinator {
        if (options.duration_ns <= 0 or options.max_windows == 0 or options.window.max_units == 0 or options.window.max_units > 256) return error.ResourceLimitExceeded;
        try (sampling.TimestampPolicy{ .step_ns = options.step_ns, .max_frames = options.max_frames }).validate();
        const reservation = if (options.window.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = @sizeOf(Coordinator) }) else media.admission.Token{};
        return .{ .allocator = a, .options = options, .reservation = reservation };
    }
    pub fn deinit(self: *Coordinator) void {
        for (self.units[0..self.count]) |*unit| unit.deinit();
        if (self.pending) |*unit| unit.deinit();
        self.reservation.deinit();
        self.* = undefined;
    }
    fn ns(ticks: i64, scale: u32) !i64 {
        return media.timeline.rescale(ticks, scale, std.time.ns_per_s);
    }
    fn charge(unit: media.live_mp4.Segment) !usize {
        return std.math.add(usize, unit.backing.bytes.len, try std.math.mul(usize, unit.reader.packets.len, @sizeOf(media.mp4.Packet)));
    }
    fn sameEpoch(self: *const Coordinator, unit: media.live_mp4.Segment) bool {
        if (self.count == 0) return true;
        const first = self.units[0];
        if (first.epoch != unit.epoch) return false;
        if (first.dash_timing) |left| return unit.dash_timing != null and std.meta.eql(left.period, unit.dash_timing.?.period);
        return unit.dash_timing == null;
    }
    pub fn append(self: *Coordinator, unit: *?media.live_mp4.Segment) !void {
        try self.options.window.stream.control.check();
        if (self.finished) return error.LiveEpochFinished;
        const value = unit.* orelse return error.EmptyVideo;
        if (!self.sameEpoch(value)) return error.LiveEpochChanged;
        if (self.count >= self.options.window.max_units) return error.ResourceLimitExceeded;
        const bytes = try charge(value);
        if (bytes > self.options.max_retained_bytes -| self.retained_bytes) return error.ResourceLimitExceeded;
        var start = self.start_ns;
        var tail = self.tail_ns;
        for (value.reader.packets) |packet| {
            if (packet.pts < packet.dts) return error.UnsupportedLiveTiming;
            const pts = try ns(packet.pts, value.reader.track.timescale);
            if (self.sealed_ns) |sealed| if (pts < sealed) return error.LateLivePicture;
            start = if (start) |old| @min(old, @max(0, pts)) else @max(0, pts);
            tail = @max(tail, std.math.cast(i64, @divFloor(@as(i128, try std.math.add(i64, packet.pts, packet.duration)) * std.time.ns_per_s + value.reader.track.timescale - 1, value.reader.track.timescale)) orelse return error.TimestampOverflow);
        }
        // Join validates complete clock/configuration compatibility transactionally.
        self.units[self.count] = value;
        var view = try windows.join(self.allocator, "live-probe", self.units[0 .. self.count + 1], self.options.window);
        defer view.deinit();
        var sealed = self.sealed_ns;
        if (view.reader.track.codec == .mjpeg) {
            const last = view.reader.packets[view.reader.packets.len - 1];
            sealed = @max(sealed orelse 0, try ns(try std.math.add(i64, last.dts, last.duration), view.reader.track.timescale));
        } else {
            const cfg = try h264.configParse(self.allocator, view.reader.track.avcc);
            defer cfg.groups.deinit(self.allocator);
            if (cfg.frame_only and !view.reader.track.inband_parameter_sets) {
                // Full decode validates IDR syntax and complete logical pictures.
                var logical = try pictures.index(self.allocator, &view.reader, .{ .max_packets = self.options.window.max_packets, .decode = .{ .max_dependency_packets = self.options.window.max_packets, .max_parameter_packets = self.options.window.max_packets } });
                defer logical.deinit();
                if (logical.pictures.len != 0) {
                    // Validated scheduling contract: PTS >= DTS and monotone
                    // decode clocks bound every subsequently admitted picture.
                    const last = view.reader.packets[view.reader.packets.len - 1];
                    sealed = @max(sealed orelse 0, try ns(try std.math.add(i64, last.dts, last.duration), view.reader.track.timescale));
                }
            }
        }
        self.count += 1;
        self.retained_bytes += bytes;
        // Start remains fixed after the first window has been delivered.
        if (!self.epoch_committed) self.start_ns = start;
        self.tail_ns = tail;
        self.sealed_ns = sealed;
        unit.* = null;
    }
    /// Explicit presentation watermark for layouts without an automatic proof.
    /// Late subsequent pictures are rejected before advancing retention state.
    pub fn seal(self: *Coordinator, end_ns: i64) !void {
        if (self.count == 0 or end_ns > self.tail_ns or end_ns < (self.sealed_ns orelse self.start_ns orelse 0)) return error.InvalidLiveWatermark;
        self.sealed_ns = end_ns;
    }
    pub fn finish(self: *Coordinator) !void {
        try self.options.window.stream.control.check();
        self.finished = true;
        if (self.count != 0) self.sealed_ns = self.tail_ns;
    }
    pub fn prepare(self: *Coordinator) !?Prepared {
        try self.options.window.stream.control.check();
        const start = self.start_ns orelse return null;
        const sealed = self.sealed_ns orelse return null;
        if (start >= sealed) return null;
        const requested = try std.math.add(i64, start, self.options.duration_ns);
        if (sealed < requested and !self.finished) return null;
        if (self.ordinal >= self.options.max_windows) return error.ResourceLimitExceeded;
        const end = @min(requested, sealed);
        var window = try windows.join(self.allocator, "live-model-window", self.units[0..self.count], self.options.window);
        errdefer window.deinit();
        var eligible = false;
        var logical_sampling = false;
        if (window.reader.track.codec == .avc) {
            const cfg = try h264.configParse(self.allocator, window.reader.track.avcc);
            defer cfg.groups.deinit(self.allocator);
            logical_sampling = window.reader.track.inband_parameter_sets or !cfg.frame_only or cfg.profile == 88 or cfg.separate_planes;
        }
        if (logical_sampling) {
            var logical = try pictures.index(self.allocator, &window.reader, .{ .max_packets = self.options.window.max_packets, .decode = .{ .max_dependency_packets = self.options.window.max_packets, .max_parameter_packets = self.options.window.max_packets } });
            defer logical.deinit();
            if (logical.pictures.len == 0) return error.EmptyVideo;
            for (logical.pictures) |picture| {
                const time = @as(i128, picture.pts) * std.time.ns_per_s;
                eligible = eligible or (time >= @as(i128, start) * window.reader.track.timescale and time < @as(i128, end) * window.reader.track.timescale);
            }
        } else {
            for (window.reader.packets) |packet| {
                const time = @as(i128, packet.pts) * std.time.ns_per_s;
                eligible = eligible or (time >= @as(i128, start) * window.reader.track.timescale and time < @as(i128, end) * window.reader.track.timescale);
            }
        }
        return .{ .window = window, .policy = .{ .start_ns = start, .end_ns = end, .step_ns = self.options.step_ns, .max_frames = self.options.max_frames }, .source_start_ns = start, .source_end_ns = end, .has_pictures = eligible, .ordinal = self.ordinal, .dash_timing = self.units[0].dash_timing, .hls_timing = self.units[0].hls_timing };
    }
    pub fn commit(self: *Coordinator, prepared: *const Prepared) !void {
        try self.options.window.stream.control.check();
        if (prepared.ordinal != self.ordinal or self.start_ns == null or prepared.source_start_ns != self.start_ns.? or self.count == 0 or prepared.window.epoch != self.units[0].epoch) return error.InvalidLiveWindowCommit;
        const end = prepared.source_end_ns;
        if (end <= self.start_ns.? or end > (self.sealed_ns orelse 0)) return error.InvalidLiveWindowCommit;
        if (end > try std.math.add(i64, self.start_ns.?, self.options.duration_ns)) return error.InvalidLiveWindowCommit;
        // All fallible probing precedes the cursor update. Logical first-packet
        // selectors preserve complete field/partition preroll across unit edges.
        var keep: usize = 0;
        if (prepared.window.reader.track.codec == .mjpeg) {
            var at: usize = 0;
            for (self.units[0..self.count], 0..) |unit, index| {
                for (unit.reader.packets) |packet| {
                    if (at >= prepared.window.reader.packets.len) break;
                    if (try ns(packet.pts, unit.reader.track.timescale) <= end) keep = index;
                    at += 1;
                }
            }
        } else {
            var reader = prepared.window.reader;
            var logical = try pictures.index(self.allocator, &reader, .{ .max_packets = self.options.window.max_packets, .decode = .{ .max_dependency_packets = self.options.window.max_packets, .max_parameter_packets = self.options.window.max_packets } });
            defer logical.deinit();
            for (logical.pictures) |picture| {
                if (try ns(picture.pts, reader.track.timescale) > end) continue;
                var payload = try reader.readPacket(picture.first_packet);
                defer payload.deinit();
                if (!try avc.isPortableIdr(payload.bytes, reader.track.nal_length_bytes)) continue;
                var count: usize = 0;
                for (self.units[0..self.count], 0..) |unit, index| {
                    count += unit.reader.packets.len;
                    if (picture.first_packet < count) {
                        keep = index;
                        break;
                    }
                }
            }
        }
        // Retain earlier-coded samples whose presentation lies in a future
        // window, even if a later IDR has already arrived in decode order.
        for (self.units[0..keep], 0..) |unit, index| {
            var future = false;
            for (unit.reader.packets) |packet| {
                if (try ns(packet.pts, unit.reader.track.timescale) >= end) {
                    future = true;
                    break;
                }
            }
            if (future) {
                keep = index;
                break;
            }
        }
        var released: usize = 0;
        for (self.units[0..keep]) |unit| released = try std.math.add(usize, released, try charge(unit));
        self.start_ns = end;
        self.epoch_committed = true;
        self.ordinal += 1;
        self.retained_bytes -= released;
        for (self.units[0..keep]) |*unit| unit.deinit();
        std.mem.copyForwards(media.live_mp4.Segment, self.units[0 .. self.count - keep], self.units[keep..self.count]);
        self.count -= keep;
    }
    /// Explicit reload bridge; bounded HTTP waiting is opt-in, and polling
    /// delay remains visible through reader.reloadAfterSeconds().
    pub fn reload(self: *Coordinator, reader: anytype, blocking: bool) !?Prepared {
        try self.options.window.stream.control.check();
        if (blocking and reader.canBlockReload()) try reader.reloadBlocking() else try reader.reload();
        return self.pull(reader);
    }
    /// Consume currently published units. A null result means reload is needed
    /// unless reader.ended(); no polling sleep or speculative preload is hidden.
    pub fn pull(self: *Coordinator, reader: anytype) !?Prepared {
        if (try self.prepare()) |prepared| return prepared;
        while (true) {
            if (self.pending == null) self.pending = try reader.next();
            if (self.pending == null) {
                if (reader.ended()) try self.finish();
                return self.prepare();
            }
            if (!self.sameEpoch(self.pending.?)) {
                try self.finish();
                if (try self.prepare()) |prepared| return prepared;
                for (self.units[0..self.count]) |*unit| unit.deinit();
                self.count = 0;
                self.retained_bytes = 0;
                self.start_ns = null;
                self.sealed_ns = null;
                self.tail_ns = 0;
                self.finished = false;
                self.epoch_committed = false;
            }
            try self.append(&self.pending);
            if (try self.prepare()) |prepared| return prepared;
        }
    }
};

const fixture = @embedFile("../testdata/live/fragmented.mp4");
fn part(ingest: *media.live_mp4.Ingest, begin: usize, end: usize) !?media.live_mp4.Segment {
    try ingest.push(fixture[begin..end]);
    return try ingest.finishSegment();
}
test "live coordinator derives validated progressive decode clock windows and commits eviction transactionally" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var ingest = try media.live_mp4.Ingest.init(a, "coordinator", fixture[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .window = .{ .stream = .{ .admission_pool = &pool } } });
    defer flow.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    try flow.append(&first);
    try std.testing.expect(first == null and (try flow.prepare()) == null);
    var second = try part(&ingest, 11206, 20811);
    defer if (second) |*unit| unit.deinit();
    try flow.append(&second);
    var prepared = (try flow.prepare()).?;
    defer prepared.deinit();
    const anchor = try Coordinator.ns(prepared.window.reader.packets[0].pts, prepared.window.reader.track.timescale);
    try std.testing.expectEqual(anchor, prepared.source_start_ns);
    try std.testing.expectEqual(anchor + std.time.ns_per_s, prepared.source_end_ns);
    try assertFirstSample(a, &prepared);
    const old_count = flow.count;
    const old_budget = pool.snapshot();
    const source = prepared.window.reader.input;
    const reads = source.limits.max_reads;
    source.limits.max_reads = source.reads;
    try std.testing.expectError(error.ResourceLimitExceeded, flow.commit(&prepared));
    try std.testing.expectEqual(@as(usize, 0), flow.ordinal);
    try std.testing.expectEqual(old_count, flow.count);
    try std.testing.expectEqual(old_budget, pool.snapshot());
    source.limits.max_reads = reads;
    try flow.commit(&prepared);
    try std.testing.expectEqual(@as(usize, 1), flow.count);
    try std.testing.expectEqual(@as(usize, 1), flow.ordinal);
    try std.testing.expectError(error.InvalidLiveWindowCommit, flow.commit(&prepared));
    var retained = try prepared.window.reader.readPacket(0);
    defer retained.deinit();
    try std.testing.expect(retained.bytes.len != 0);
    try flow.finish();
    var tail = (try flow.prepare()).?;
    defer tail.deinit();
    try std.testing.expectEqual(prepared.source_end_ns, tail.source_start_ns);
    try assertFirstSample(a, &tail);
    try flow.commit(&tail);
    try std.testing.expect((try flow.prepare()) == null);
}
test "live coordinator capacity and watermark failures retain incoming unit ownership" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "capacity", fixture[0..797], .{});
    defer ingest.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .max_retained_bytes = 1 });
    defer flow.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, flow.append(&first));
    try std.testing.expect(first != null and flow.count == 0);
    try std.testing.expectError(error.InvalidLiveWatermark, flow.seal(1));
}

const FakeReader = struct {
    units: [2]?media.live_mp4.Segment,
    available: usize = 1,
    at: usize = 0,
    done: bool = false,
    blocking: bool = false,
    calls: usize = 0,
    pub fn next(self: *@This()) !?media.live_mp4.Segment {
        self.calls += 1;
        if (self.at >= self.available) return null;
        const value = self.units[self.at];
        self.units[self.at] = null;
        self.at += 1;
        return value;
    }
    pub fn ended(self: *const @This()) bool {
        return self.done and self.at >= self.available;
    }
    pub fn canBlockReload(_: *const @This()) bool {
        return true;
    }
    pub fn reloadBlocking(self: *@This()) !void {
        self.blocking = true;
        self.available = 2;
    }
    pub fn reload(self: *@This()) !void {
        self.available = 2;
    }
    fn deinit(self: *@This()) void {
        for (&self.units) |*value| if (value.*) |*unit| unit.deinit();
    }
};
test "live coordinator pumps reloads and preserves fetched units across capacity retry" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "pump", fixture[0..797], .{});
    defer ingest.deinit();
    var reader = FakeReader{ .units = .{ try part(&ingest, 797, 11206), try part(&ingest, 11206, 20811) } };
    defer reader.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .max_retained_bytes = 1 });
    defer flow.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, flow.pull(&reader));
    try std.testing.expect(flow.pending != null and reader.calls == 1);
    flow.options.max_retained_bytes = 64 * 1024 * 1024;
    try std.testing.expect((try flow.pull(&reader)) == null);
    var output = (try flow.reload(&reader, true)).?;
    defer output.deinit();
    try std.testing.expect(reader.blocking and flow.pending == null);
    try flow.commit(&output);
    reader.done = true;
    var tail = (try flow.pull(&reader)).?;
    defer tail.deinit();
    try flow.commit(&tail);
    try std.testing.expect((try flow.pull(&reader)) == null);
}
test "live coordinator drains old epoch before accepting reset presentation clocks" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "epochs", fixture[0..797], .{});
    defer ingest.deinit();
    const first = try part(&ingest, 797, 11206);
    try ingest.reconfigure("new-epoch", fixture[0..797]);
    var reader = FakeReader{ .units = .{ first, try part(&ingest, 797, 11206) }, .available = 2, .done = true };
    defer reader.deinit();
    var flow = try Coordinator.init(a, .{});
    defer flow.deinit();
    var one = (try flow.pull(&reader)).?;
    defer one.deinit();
    try std.testing.expectEqual(@as(usize, 0), one.window.epoch);
    try flow.commit(&one);
    var two = (try flow.pull(&reader)).?;
    defer two.deinit();
    try std.testing.expectEqual(@as(usize, 1), two.window.epoch);
    try std.testing.expectEqual(one.source_start_ns, two.source_start_ns);
    try std.testing.expectEqual(@as(usize, 1), two.ordinal);
    try flow.commit(&two);
}

fn allocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var ingest = try media.live_mp4.Ingest.init(a, "allocation", fixture[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .window = .{ .stream = .{ .admission_pool = &pool } } });
    defer flow.deinit();
    try flow.append(&first);
    try flow.finish();
    var output = (try flow.prepare()).?;
    defer output.deinit();
    try flow.commit(&output);
}
test "live coordinator allocation failures release decoder snapshots units and shared admission" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationRun, .{});
}

fn assertFirstSample(a: std.mem.Allocator, output: *const Prepared) !void {
    const metadata = try a.alloc(sampling.TimedPicture, output.window.reader.packets.len);
    defer a.free(metadata);
    for (output.window.reader.packets, metadata) |packet, *item| item.* = .{ .pts = packet.pts, .duration = packet.duration };
    const selected = try sampling.cappedSourceTimestamps(a, metadata, output.window.reader.track.timescale, output.policy, .{});
    defer a.free(selected);
    try std.testing.expectEqual(output.source_start_ns, try Coordinator.ns(metadata[selected[0]].pts, output.window.reader.track.timescale));
}
test "live coordinator emits explicit empty intervals without dropping future presentation samples" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "sparse", fixture[0..797], .{});
    defer ingest.deinit();
    var one = try part(&ingest, 797, 11206);
    defer if (one) |*unit| unit.deinit();
    var two = try part(&ingest, 11206, 20811);
    defer if (two) |*unit| unit.deinit();
    const shift: i64 = @as(i64, two.?.reader.track.timescale) * 4;
    for (two.?.reader.packets) |*packet| {
        packet.dts += shift;
        packet.pts += shift;
        packet.media_dts += shift;
        packet.media_pts += shift;
    }
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s });
    defer flow.deinit();
    try flow.append(&one);
    try flow.append(&two);
    var initial = (try flow.prepare()).?;
    defer initial.deinit();
    try std.testing.expect(initial.has_pictures);
    try flow.commit(&initial);
    var gap = (try flow.prepare()).?;
    defer gap.deinit();
    try std.testing.expect(!gap.has_pictures);
    try flow.commit(&gap);
    try std.testing.expectEqual(@as(usize, 2), flow.count);
    try std.testing.expectEqual(@as(usize, 2), flow.ordinal);
}
test "live coordinator timing violations fail before transfer or payload reads" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "timing", fixture[0..797], .{});
    defer ingest.deinit();
    var unit = try part(&ingest, 797, 11206);
    defer if (unit) |*value| value.deinit();
    unit.?.reader.packets[0].dts = unit.?.reader.packets[0].pts + 1;
    const reads = unit.?.reader.input.reads;
    var flow = try Coordinator.init(a, .{});
    defer flow.deinit();
    try std.testing.expectError(error.UnsupportedLiveTiming, flow.append(&unit));
    try std.testing.expect(unit != null and flow.count == 0);
    try std.testing.expectEqual(reads, unit.?.reader.input.reads);
}
