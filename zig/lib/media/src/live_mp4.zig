// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Incremental, caller-framed live fMP4 segments. Emitted readers own immutable
//! init+segment snapshots, so later input cannot invalidate packet leases.
const std = @import("std");
const mp4 = @import("mp4.zig");
const iso = @import("isobmff.zig");
const stream = @import("stream.zig");
const admission = @import("admission.zig");
pub const Options = struct {
    stream: stream.Options = .{},
    index: mp4.Limits = .{},
    max_segments: usize = 100_000,
    max_identity_bytes: usize = 4096,
    max_ingested_bytes: u64 = 1024 * 1024 * 1024,
};
pub const Segment = struct {
    backing: *stream.Snapshot,
    reader: mp4.Reader,
    ordinal: usize,
    identity: []u8,
    identity_reservation: admission.Token,
    pub fn deinit(self: *Segment) void {
        const allocator = self.backing.input.allocator;
        self.reader.deinit();
        self.backing.deinit();
        allocator.free(self.identity);
        self.identity_reservation.deinit();
        self.* = undefined;
    }
};
const Identity = struct {
    id: u32,
    scale: u32,
    width: u16,
    height: u16,
    codec: mp4.Codec,
    config: [32]u8,
    fn from(track: mp4.Track) Identity {
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(track.avcc, &hash, .{});
        return .{ .id = track.id, .scale = track.timescale, .width = track.width, .height = track.height, .codec = track.codec, .config = hash };
    }
};
pub const Ingest = struct {
    allocator: std.mem.Allocator,
    initialization: *stream.Snapshot,
    pending: std.ArrayList(u8) = .empty,
    pending_reservation: admission.Token = .{},
    options: Options,
    identity: ?Identity = null,
    next_decode_time: ?i64 = null,
    segments: usize = 0,
    ingested_bytes: u64 = 0,
    pub fn init(allocator: std.mem.Allocator, identity: []const u8, initialization: []const u8, options: Options) !Ingest {
        if (identity.len > options.max_identity_bytes) return error.ResourceLimitExceeded;
        var cursor: usize = 0;
        var movie = false;
        var boxes: usize = 0;
        while (cursor < initialization.len) {
            try options.stream.control.check();
            if (boxes >= options.index.max_boxes) return error.ResourceLimitExceeded;
            boxes += 1;
            const box = try iso.readBox(initialization, cursor);
            cursor = box.end;
            if (box.typ == iso.fourcc("moov")) {
                if (movie) return error.MalformedMedia;
                movie = true;
            }
            if (box.typ == iso.fourcc("moof") or box.typ == iso.fourcc("mdat")) return error.UnsupportedContainer;
        }
        if (!movie) return error.MissingMovieMetadata;
        if (initialization.len > options.max_ingested_bytes) return error.ResourceLimitExceeded;
        const snapshot = try stream.Snapshot.copy(allocator, identity, &.{initialization}, options.stream);
        errdefer snapshot.deinit();
        const reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{}) else admission.Token{};
        return .{ .allocator = allocator, .initialization = snapshot, .pending_reservation = reservation, .options = options, .ingested_bytes = initialization.len };
    }
    /// Partial network reads may be supplied in any chunk size. Segment framing
    /// is explicit: only finishSegment commits timeline state or publishes data.
    pub fn push(self: *Ingest, bytes: []const u8) !void {
        try self.options.stream.control.check();
        if (self.segments >= self.options.max_segments or bytes.len > self.options.max_ingested_bytes -| self.ingested_bytes) return error.ResourceLimitExceeded;
        const size = try std.math.add(usize, self.pending.items.len, bytes.len);
        if (size > self.options.stream.max_bytes -| self.initialization.bytes.len) return error.ResourceLimitExceeded;
        if (size > self.pending.capacity) {
            const previous = self.pending_reservation.resources;
            try self.pending_reservation.resize(.{ .host_bytes = try std.math.add(usize, size, self.pending.capacity) });
            const grown = self.allocator.alloc(u8, size) catch |err| {
                try self.pending_reservation.resize(previous);
                return err;
            };
            @memcpy(grown[0..self.pending.items.len], self.pending.items);
            const used = self.pending.items.len;
            self.pending.deinit(self.allocator);
            self.pending = .empty;
            self.pending.items = grown[0..used];
            self.pending.capacity = grown.len;
            try self.pending_reservation.resize(.{ .host_bytes = size });
        }
        self.pending.appendSliceAssumeCapacity(bytes);
        self.ingested_bytes += bytes.len;
    }
    /// A segment consists of complete moof/mdat boxes, using moof-relative
    /// addressing. Failed validation retains input for inspection/discard/retry.
    /// The returned value can move; its backing Source stays at a stable address.
    pub fn finishSegment(self: *Ingest) !Segment {
        try self.options.stream.control.check();
        if (self.segments >= self.options.max_segments) return error.ResourceLimitExceeded;
        var cursor: usize = 0;
        var fragment = false;
        var payload = false;
        var boxes: usize = 0;
        while (cursor < self.pending.items.len) {
            if (boxes >= self.options.index.max_boxes) return error.ResourceLimitExceeded;
            boxes += 1;
            const box = try iso.readBox(self.pending.items, cursor);
            cursor = box.end;
            if (box.typ == iso.fourcc("moof")) fragment = true;
            if (box.typ == iso.fourcc("mdat")) payload = true;
            if (box.typ == iso.fourcc("moov")) return error.UnsupportedDynamicVideoConfig;
        }
        if (!fragment or !payload) return error.IncompleteMediaSegment;
        const identity_bytes = std.fmt.count("{s}/segment/{d}", .{ self.initialization.input.identity, self.segments });
        if (identity_bytes > self.options.max_identity_bytes) return error.ResourceLimitExceeded;
        var identity_reservation = if (self.options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = identity_bytes }) else admission.Token{};
        errdefer identity_reservation.deinit();
        const owned_identity = try self.allocator.alloc(u8, identity_bytes);
        errdefer self.allocator.free(owned_identity);
        _ = try std.fmt.bufPrint(owned_identity, "{s}/segment/{d}", .{ self.initialization.input.identity, self.segments });
        const snapshot = try stream.Snapshot.copy(self.allocator, owned_identity, &.{ self.initialization.bytes, self.pending.items }, self.options.stream);
        errdefer snapshot.deinit();
        var reader = try mp4.Reader.init(self.allocator, &snapshot.input, self.options.index);
        errdefer reader.deinit();
        const identity = Identity.from(reader.track);
        if (self.identity) |previous| if (!std.meta.eql(previous, identity)) return error.UnsupportedDynamicVideoConfig;
        if (self.next_decode_time) |next| if (reader.packets[0].media_dts < next) return error.NonMonotonicMediaSegment;
        const last = reader.packets[reader.packets.len - 1];
        const next = try std.math.add(i64, last.media_dts, last.duration);
        self.identity = identity;
        self.next_decode_time = next;
        const ordinal = self.segments;
        self.segments += 1;
        self.pending.clearRetainingCapacity();
        return .{ .backing = snapshot, .reader = reader, .ordinal = ordinal, .identity = owned_identity, .identity_reservation = identity_reservation };
    }
    pub fn discardSegment(self: *Ingest) void {
        self.pending.clearRetainingCapacity();
    }
    pub fn deinit(self: *Ingest) void {
        self.pending.deinit(self.allocator);
        self.pending_reservation.deinit();
        self.initialization.deinit();
        self.* = undefined;
    }
};
test "live MP4 partial segments preserve earlier packet leases and reject replay" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/fragmented.mp4");
    var cursor: usize = 0;
    var split: usize = 0;
    while (cursor < bytes.len) {
        const box = try iso.readBox(bytes, cursor);
        if (box.typ == iso.fourcc("moof")) {
            split = cursor;
            break;
        }
        cursor = box.end;
    }
    try std.testing.expect(split != 0);
    const Harness = struct {
        fn run(backing: std.mem.Allocator, init_bytes: []const u8, segment_bytes: []const u8) !void {
            // Force allocation fallbacks: in-place metadata growth otherwise
            // varies with heap placement and changes the failure-count schedule.
            var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
            const allocator = no_resize.allocator();
            var ingest = try Ingest.init(allocator, "live", init_bytes, .{});
            defer ingest.deinit();
            try ingest.push(segment_bytes[0..7]);
            try std.testing.expectError(error.MalformedMedia, ingest.finishSegment());
            try ingest.push(segment_bytes[7..]);
            var segment = try ingest.finishSegment();
            defer segment.deinit();
            var packet = try segment.reader.readPacket(0);
            defer packet.deinit();
            try ingest.push(segment_bytes);
            if (ingest.finishSegment()) |value| {
                var replay = value;
                replay.deinit();
                return error.UnexpectedReplay;
            } else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.NonMonotonicMediaSegment, err);
            }
            try std.testing.expectEqualSlices(u8, segment.backing.bytes[@intCast(segment.reader.packets[0].offset)..][0..packet.bytes.len], packet.bytes);
        }
    };
    try Harness.run(a, bytes[0..split], bytes[split..]);
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{ bytes[0..split], bytes[split..] });
}

test "live MP4 successive segments retain stable leases across bounded arrival chunks" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/fragmented.mp4");
    var offsets: [32]usize = undefined;
    var count: usize = 0;
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        const box = try iso.readBox(bytes, cursor);
        if (box.typ == iso.fourcc("moof")) {
            offsets[count] = cursor;
            count += 1;
        }
        cursor = box.end;
    }
    try std.testing.expect(count > 1);
    var pool = admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    {
        var ingest = try Ingest.init(a, "live-sequence", bytes[0..offsets[0]], .{ .index = .{ .max_index_bytes = 256 * 1024 }, .stream = .{ .max_bytes = bytes.len, .admission_pool = &pool } });
        defer ingest.deinit();
        var first: ?Segment = null;
        defer if (first) |*segment| segment.deinit();
        var lease: ?@import("source.zig").Lease = null;
        defer if (lease) |*packet| packet.deinit();
        for (0..count) |index| {
            const end = if (index + 1 < count) offsets[index + 1] else bytes.len;
            cursor = offsets[index];
            while (cursor < end) {
                const next = @min(cursor + 13, end);
                try ingest.push(bytes[cursor..next]);
                cursor = next;
            }
            var segment = try ingest.finishSegment();
            try std.testing.expectEqual(index, segment.ordinal);
            if (index == 0) {
                first = segment;
                lease = try first.?.reader.readPacket(0);
            } else segment.deinit();
            if (lease) |packet| try std.testing.expectEqualSlices(u8, first.?.backing.bytes[@intCast(first.?.reader.packets[0].offset)..][0..packet.bytes.len], packet.bytes);
        }
        try std.testing.expectEqual(@as(usize, 0), ingest.pending.items.len);
    }
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
}
