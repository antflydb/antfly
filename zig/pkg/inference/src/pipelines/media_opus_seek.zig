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

//! Exact Opus state advances on demand. Recent checkpoints bound forward
//! page replay without decoding the unrequested tail of a recording.
const std = @import("std");
const audio = @import("inference_audio");
const opus = audio.opus;
const memory = @import("../runtime/tier/memory.zig");
const admitted = @import("../backends/admitted_allocator.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Admission = struct { context: *anyopaque, acquire: *const fn (*anyopaque, usize) anyerror!memory.AdmissionLease };
pub const max_checkpoints = 64;
threadlocal var active_lane: u1 = 0;
pub const Lane = struct {
    previous: u1,
    pub fn enter(lane: u1) Lane {
        const previous = active_lane;
        active_lane = lane;
        return .{ .previous = previous };
    }
    pub fn deinit(self: Lane) void {
        active_lane = self.previous;
    }
};
const interval = 48000;
const Checkpoint = struct { packet: usize, first: usize, state: opus.OpusFrameState, used: u64 = 0 };
pub const Index = struct {
    owner: *Owner,
    pub fn init(bytes: []const u8, max_packets: usize, admission: ?Admission) !Index {
        const owner = try create(admission);
        errdefer owner.destroy();
        owner.demuxed = try opus.demuxOggAlloc(owner.arena.allocator(), bytes);
        try finish(owner, max_packets);
        return .{ .owner = owner };
    }
    pub fn create(admission: ?Admission) !*Owner {
        const backing = @import("antfly_platform").allocator.concurrentFallback();
        var lease: ?memory.AdmissionLease = if (admission) |quota| try quota.acquire(quota.context, @sizeOf(Owner)) else null;
        errdefer if (lease) |*owned| owned.release();
        const owner = try backing.create(Owner);
        errdefer backing.destroy(owner);
        owner.* = .{ .allocation = .{ .backing = backing, .admission_context = if (admission) |quota| quota.context else null, .admit = if (admission) |quota| quota.acquire else null }, .lease = lease, .admission = admission, .arena = undefined };
        owner.arena = .init(if (admission != null) owner.allocation.allocator() else backing);
        errdefer {
            owner.arena.deinit();
            owner.allocation.deinit();
        }
        return owner;
    }
    fn finish(owner: *Owner, max_packets: usize) !void {
        if (owner.demuxed.packets.len == 0 or owner.demuxed.packets.len > max_packets) return error.UnsupportedAudioFormat;
        _ = try opus.initialFrameState(owner.demuxed.header.channels);
        owner.capacity = @intCast(@min(max_checkpoints, @max(2, (owner.demuxed.playable_frames + interval - 1) / interval + 2)));
        if (owner.admission) |quota| owner.checkpoint_lease = try quota.acquire(quota.context, (owner.capacity + 4) * @sizeOf(Checkpoint));
        owner.state_bytes = (owner.capacity + 4) * @sizeOf(Checkpoint);
        refreshBytes(owner);
    }
    pub fn refreshBytes(owner: *Owner) void {
        owner.retained_bytes = @sizeOf(Owner) + owner.state_bytes + @max(owner.arena.queryCapacity(), owner.allocation.reserved_bytes + owner.allocation.metadata_bytes);
    }
    pub const Builder = struct {
        owner: *Owner,
        pub fn build(self: Builder, _: std.mem.Allocator, demuxed: opus.Demuxed, max_packets: usize) !Index {
            self.owner.demuxed = demuxed;
            try finish(self.owner, max_packets);
            _ = self.owner.refs.fetchAdd(1, .monotonic);
            // Packet ownership is already in this owner's admitted arena.
            // A second payload copy or a later admission step is unnecessary.
            return .{ .owner = self.owner };
        }
    };
    pub fn copy(self: Index, _: std.mem.Allocator) !Index {
        _ = self.owner.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn deinit(self: *Index) void {
        if (self.owner.refs.fetchSub(1, .acq_rel) == 1) self.owner.destroy();
    }
    pub fn estimatedBytes(self: Index) usize {
        return self.owner.retained_bytes;
    }
    pub fn samples(self: Index) usize {
        return @intCast(self.owner.demuxed.playable_frames);
    }
    fn check(_: ?*anyopaque) !void {
        try audio.work_control.check();
    }
    pub fn visitWindow(self: Index, a: std.mem.Allocator, first: usize, last: usize, sink: opus.WindowSink) !void {
        try self.owner.visit(a, first, last, sink, .{ .check_fn = check });
    }
    pub fn visitWindowControlled(self: Index, a: std.mem.Allocator, first: usize, last: usize, sink: opus.WindowSink, control: Control) !void {
        try self.owner.visit(a, first, last, sink, control);
    }
};
pub const Owner = struct {
    refs: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    allocation: admitted.AdmittedAllocator,
    arena: std.heap.ArenaAllocator,
    lease: ?memory.AdmissionLease,
    checkpoint_lease: ?memory.AdmissionLease = null,
    admission: ?Admission,
    capacity: usize = max_checkpoints,
    demuxed: opus.Demuxed = undefined,
    checkpoints: [max_checkpoints]?*Checkpoint = @splat(null),
    frontiers: [4]?*Checkpoint = @splat(null),
    clock: u64 = 0,
    retained_bytes: usize = 0,
    state_bytes: usize = 0,
    decoded_packets: std.atomic.Value(u64) = .init(0),
    pub fn destroy(self: *Owner) void {
        for (self.checkpoints) |point| if (point) |owned| @import("antfly_platform").allocator.concurrentFallback().destroy(owned);
        for (self.frontiers) |point| if (point) |owned| @import("antfly_platform").allocator.concurrentFallback().destroy(owned);
        self.arena.deinit();
        self.allocation.deinit();
        if (self.checkpoint_lease) |*lease| lease.release();
        if (self.lease) |*lease| lease.release();
        @import("antfly_platform").allocator.concurrentFallback().destroy(self);
    }
    fn remember(self: *Owner, value: Checkpoint) !void {
        var slot: usize = 0;
        for (self.checkpoints[0..self.capacity], 0..) |maybe, i| {
            const point = maybe orelse {
                slot = i;
                break;
            };
            if (point.first == value.first) {
                slot = i;
                break;
            }
            if (point.used < self.checkpoints[slot].?.used) slot = i;
        }
        const point = self.checkpoints[slot] orelse blk: {
            const owned = try @import("antfly_platform").allocator.concurrentFallback().create(Checkpoint);
            self.checkpoints[slot] = owned;
            break :blk owned;
        };
        self.clock += 1;
        point.* = value;
        point.used = self.clock;
    }
    fn frontier(self: *Owner, slot: usize, value: Checkpoint) !void {
        const point = self.frontiers[slot] orelse blk: {
            const owned = try @import("antfly_platform").allocator.concurrentFallback().create(Checkpoint);
            self.frontiers[slot] = owned;
            break :blk owned;
        };
        point.* = value;
    }
    fn visit(self: *Owner, a: std.mem.Allocator, first: usize, last: usize, sink: opus.WindowSink, control: Control) !void {
        if (last < first or last > self.demuxed.playable_frames) return error.InvalidAudioWindow;
        if (first == last) return;
        try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
        defer self.mutex.unlock();
        const raw_first = first + self.demuxed.header.pre_skip;
        const raw_last = last + self.demuxed.header.pre_skip;
        var selected: ?*Checkpoint = null;
        for (self.checkpoints) |maybe| if (maybe) |point| {
            if (point.first <= raw_first and (selected == null or point.first > selected.?.first)) selected = point;
        };
        const lane: usize = active_lane;
        for (self.frontiers[lane * 2 ..][0..2]) |maybe| if (maybe) |point| {
            if (point.first <= raw_first and (selected == null or point.first > selected.?.first)) selected = point;
        };
        var state = if (selected) |point| point.state else try opus.initialFrameState(self.demuxed.header.channels);
        var position: usize = if (selected) |point| point.first else 0;
        var packet_index: usize = if (selected) |point| point.packet else 0;
        var saved_at = position;
        if (selected) |point| {
            self.clock += 1;
            point.used = self.clock;
        }
        var filled: usize = 0;
        while (position < raw_last and packet_index < self.demuxed.packets.len) : (packet_index += 1) {
            try control.check();
            if (position - saved_at >= interval) {
                try self.remember(.{ .packet = packet_index, .first = position, .state = state });
                saved_at = position;
            }
            const expected = self.demuxed.packet_sample_counts[packet_index];
            if (position <= raw_first and position + expected > raw_first) try self.frontier(lane * 2, .{ .packet = packet_index, .first = position, .state = state });
            if (position < raw_last and position + expected >= raw_last) try self.frontier(lane * 2 + 1, .{ .packet = packet_index, .first = position, .state = state });
            const decoded = try opus.decodeWindowPacket(a, &state, self.demuxed.packets[packet_index], self.demuxed.packet_tocs[packet_index], self.demuxed.header.channels);
            defer a.free(decoded);
            _ = self.decoded_packets.fetchAdd(1, .monotonic);
            const frames = decoded.len / self.demuxed.header.channels;
            if (frames != self.demuxed.packet_sample_counts[packet_index]) return error.UnsupportedAudioFormat;
            const begin = @min(frames, raw_first -| position);
            const end = @min(frames, raw_last - position);
            if (end > begin) {
                const values = decoded[begin * self.demuxed.header.channels .. end * self.demuxed.header.channels];
                opus.applyOutputGainInPlace(values, self.demuxed.header.output_gain_q8);
                try sink.push(values, 48000, self.demuxed.header.channels);
                filled += end - begin;
            }
            position += frames;
        }
        if (filled != last - first) return error.IncompleteAudioStream;
        try self.remember(.{ .packet = packet_index, .first = position, .state = state });
    }
};

test "model aware media progressive Opus indexes decode requested prefixes and retain separate reader frontiers" {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.opus", a, .limited(1024 * 1024));
    defer a.free(bytes);
    var original = try opus.demuxOggAlloc(a, bytes);
    defer original.deinit();
    // A long recording's packet directory is cheap to construct; no packet
    // may be decoded until a requested window actually needs it.
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var repeated = original;
    repeated.allocator = alloc;
    repeated.packets = try alloc.alloc([]u8, original.packets.len * 100);
    repeated.packet_tocs = try alloc.alloc(opus.Toc, repeated.packets.len);
    repeated.packet_sample_counts = try alloc.alloc(u16, repeated.packets.len);
    var total: u64 = 0;
    for (repeated.packets, repeated.packet_tocs, repeated.packet_sample_counts, 0..) |*packet, *toc, *count, i| {
        const source = i % original.packets.len;
        packet.* = try alloc.dupe(u8, original.packets[source]);
        toc.* = original.packet_tocs[source];
        count.* = original.packet_sample_counts[source];
        total += count.*;
    }
    repeated.total_decoded_frames = total;
    repeated.playable_frames = total - repeated.header.pre_skip;
    const owner = try Index.create(null);
    const meta = owner.arena.allocator();
    var transferred = repeated;
    transferred.allocator = meta;
    transferred.packets = try meta.alloc([]u8, repeated.packets.len);
    for (transferred.packets, repeated.packets) |*out, packet| out.* = try meta.dupe(u8, packet);
    transferred.packet_tocs = try meta.dupe(opus.Toc, repeated.packet_tocs);
    transferred.packet_sample_counts = try meta.dupe(u16, repeated.packet_sample_counts);
    var index = try (Index.Builder{ .owner = owner }).build(meta, transferred, 1000000);
    var initial = Index{ .owner = owner };
    initial.deinit();
    defer index.deinit();
    try std.testing.expectEqual(@as(u64, 0), index.owner.decoded_packets.load(.monotonic));
    var reference = try audio.decodeInterleaved(a, bytes, .{});
    defer reference.deinit();
    const Sink = struct {
        a: std.mem.Allocator,
        samples: std.ArrayList(f32) = .empty,
        fn write(raw: *anyopaque, samples: []const f32, _: u32, _: u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.samples.appendSlice(self.a, samples);
        }
        fn discard(_: *anyopaque, _: []const f32, _: u32, _: u8) !void {}
    };
    var capture = Sink{ .a = a };
    defer capture.samples.deinit(a);
    try index.visitWindow(a, 0, 4800, .{ .context = &capture, .write = Sink.write });
    try std.testing.expectEqualSlices(f32, reference.samples[0 .. 4800 * @as(usize, original.header.channels)], capture.samples.items);
    const first_runs = index.owner.decoded_packets.load(.monotonic);
    try std.testing.expect(first_runs < original.packets.len);
    // The lookahead reader jumps forward independently of the output reader.
    {
        const lane = Lane.enter(1);
        defer lane.deinit();
        try index.visitWindow(a, 480000, 484800, .{ .context = &capture, .write = Sink.discard });
    }
    // Drop recent directory checkpoints to exercise each protected frontier.
    for (&index.owner.checkpoints) |*point| if (point.*) |owned| {
        @import("antfly_platform").allocator.concurrentFallback().destroy(owned);
        point.* = null;
    };
    const before_output = index.owner.decoded_packets.load(.monotonic);
    capture.samples.clearRetainingCapacity();
    try index.visitWindow(a, 4800, 9600, .{ .context = &capture, .write = Sink.write });
    try std.testing.expectEqualSlices(f32, reference.samples[4800 * @as(usize, original.header.channels) .. 9600 * @as(usize, original.header.channels)], capture.samples.items);
    try std.testing.expect(index.owner.decoded_packets.load(.monotonic) - before_output <= first_runs + 2);
    const before_analysis = index.owner.decoded_packets.load(.monotonic);
    {
        const lane = Lane.enter(1);
        defer lane.deinit();
        try index.visitWindow(a, 484800, 489600, .{ .context = &capture, .write = Sink.discard });
    }
    try std.testing.expect(index.owner.decoded_packets.load(.monotonic) - before_analysis <= first_runs + 2);
    std.debug.print("progressive Opus packets={d} cold_decoded=0 first_window_packets={d} next_output_packets={d} next_analysis_packets={d}\n", .{ repeated.packets.len, first_runs, before_analysis - before_output, index.owner.decoded_packets.load(.monotonic) - before_analysis });
}

test "model aware media progressive Opus owners release admission after the last cached reference" {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.opus", a, .limited(1024 * 1024));
    defer a.free(bytes);
    const Quota = struct {
        controller: memory.AdmissionController = .{},
        calls: usize = 0,
        fail_at: ?usize = null,
        fn acquire(raw: *anyopaque, size: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.fail_at) |limit| if (self.calls >= limit) return error.OutOfMemory;
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64 * 1024 * 1024 }, .{ .host_scratch_bytes = size }, false);
        }
    };
    var quota: Quota = .{};
    var index = try Index.init(bytes, 100000, .{ .context = &quota, .acquire = Quota.acquire });
    var clone = try index.copy(a);
    index.deinit();
    try std.testing.expect(quota.controller.snapshot().hostTotalBytes() > 0);
    try std.testing.expect(clone.owner.capacity < max_checkpoints);
    clone.deinit();
    try std.testing.expectEqual(@as(usize, 0), quota.controller.snapshot().hostTotalBytes());
    const calls = quota.calls;
    for (1..calls + 1) |failure| {
        var failed = Quota{ .fail_at = failure };
        try std.testing.expectError(error.OutOfMemory, Index.init(bytes, 100000, .{ .context = &failed, .acquire = Quota.acquire }));
        try std.testing.expectEqual(@as(usize, 0), failed.controller.snapshot().hostTotalBytes());
    }
}
