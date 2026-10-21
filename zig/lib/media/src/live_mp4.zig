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

//! Incremental fMP4 with automatic moof/mdat segment framing. Emitted readers own immutable
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
    max_reconfigurations: usize = 1024,
    max_identity_bytes: usize = 4096,
    max_ingested_bytes: u64 = 1024 * 1024 * 1024,
};
/// Constant-space commitment to ordered sample metadata and encoded payloads.
/// HLS parent fallback verifies this exact prefix before discarding duplicates.
pub const SamplesDigest = struct {
    count: usize = 0,
    state: std.crypto.hash.sha2.Sha256 = .init(.{}),
    /// Cancellation/read failure leaves the prior commitment unchanged.
    pub fn extend(self: *SamplesDigest, reader: *mp4.Reader, begin: usize, end: usize) !void {
        var candidate = self.*;
        for (reader.packets[begin..end], begin..) |packet, index| {
            try reader.input.control.check();
            var header: [25]u8 = undefined;
            std.mem.writeInt(i64, header[0..8], packet.media_dts, .big);
            std.mem.writeInt(i64, header[8..16], packet.media_pts, .big);
            std.mem.writeInt(u32, header[16..20], packet.duration, .big);
            std.mem.writeInt(u32, header[20..24], packet.size, .big);
            header[24] = @intFromBool(packet.sync);
            var payload = try reader.readPacket(index);
            defer payload.deinit();
            candidate.state.update(&header);
            var offset: usize = 0;
            while (offset < payload.bytes.len) {
                try reader.input.control.check();
                const stop = @min(payload.bytes.len, offset + 8192);
                candidate.state.update(payload.bytes[offset..stop]);
                offset = stop;
            }
            candidate.count = std.math.add(usize, candidate.count, 1) catch return error.ResourceLimitExceeded;
        }
        self.* = candidate;
    }
    pub fn bytes(self: SamplesDigest) [32]u8 {
        var state = self.state;
        var hash: [32]u8 = undefined;
        state.final(&hash);
        return hash;
    }
};
pub const HlsTiming = struct { sequence: u64, part_index: ?u32 = null, independent_hint: bool = false, discontinuity_sequence: u64 };
pub const Segment = struct {
    backing: *stream.Snapshot,
    reader: mp4.Reader,
    /// Normalized TS snapshots contain codec config plus length-prefixed packets,
    /// not a serialized MP4 file. Consumers use reader rather than backing bytes.
    origin: enum { fmp4, normalized_transport } = .fmp4,
    ordinal: usize,
    /// Decoder/timestamp state must be reset when this changes.
    epoch: usize,
    identity: []u8,
    identity_reservation: admission.Token,
    /// Packet slices borrow the original identity through their snapshot ref.
    owns_identity: bool = true,
    /// DASH presentation mapping; packet timestamps remain in the native track
    /// clock. Period transitions expose a new decoder epoch independently.
    dash_timing: ?@import("hls.zig").DashTiming = null,
    samples_digest: ?SamplesDigest = null,
    hls_timing: ?HlsTiming = null,
    /// Own only packet metadata; immutable encoded bytes/configuration and
    /// identity remain in the shared snapshot. No remux or payload copy.
    pub fn packetSlice(self: *const Segment, a: std.mem.Allocator, begin: usize, end: usize) !Segment {
        if (begin >= end or end > self.reader.packets.len) return error.InvalidPacketIndex;
        // Published readers refer directly to this immutable snapshot. Reject
        // foreign readers rather than borrowing separately allocated metadata.
        for ([_][]const u8{ self.reader.track.avcc, self.reader.track.color_info }) |bytes| {
            if (bytes.len == 0) continue;
            const base = @intFromPtr(self.backing.bytes.ptr);
            const address = @intFromPtr(bytes.ptr);
            if (address < base or address - base > self.backing.bytes.len or bytes.len > self.backing.bytes.len - (address - base)) return error.InvalidVideoReaderSelection;
        }
        var reservation = if (self.reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(usize, end - begin, @sizeOf(mp4.Packet)) }) else admission.Token{};
        errdefer reservation.deinit();
        const packets = try a.dupe(mp4.Packet, self.reader.packets[begin..end]);
        var result = self.*;
        result.reader.allocator = a;
        result.reader.packets = packets;
        result.reader.metadata = .{ .bytes = &.{}, .source = null };
        result.reader.index_reservation = reservation;
        result.owns_identity = false;
        result.identity_reservation = .{};
        self.backing.retain();
        return result;
    }
    pub fn deinit(self: *Segment) void {
        // Retained window/decoder owners can outlive the segment. Keep the
        // source identity and its admission alive with that same backing.
        if (self.owns_identity) {
            std.debug.assert(self.backing.owned_identity == null);
            self.backing.owned_identity = self.identity;
            self.backing.identity_reservation = self.identity_reservation;
        }
        self.reader.deinit();
        self.backing.deinit();
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
    epoch: usize = 0,
    ingested_bytes: u64 = 0,
    ended: bool = false,
    scan_cursor: usize = 0,
    scan_boxes: usize = 0,
    scan_fragment: bool = false,
    scan_payload: bool = false,
    pub fn init(allocator: std.mem.Allocator, identity: []const u8, initialization: []const u8, options: Options) !Ingest {
        if (identity.len > options.max_identity_bytes) return error.ResourceLimitExceeded;
        var cursor: usize = 0;
        var movie = false;
        var track_identity: ?Identity = null;
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
                track_identity = Identity.from(try mp4.fragmentedInitialization(box.payload, options.index, options.stream.control));
            }
            if (box.typ == iso.fourcc("moof") or box.typ == iso.fourcc("mdat")) return error.UnsupportedContainer;
        }
        if (!movie) return error.MissingMovieMetadata;
        if (initialization.len > options.max_ingested_bytes) return error.ResourceLimitExceeded;
        const snapshot = try stream.Snapshot.copy(allocator, identity, &.{initialization}, options.stream);
        errdefer snapshot.deinit();
        const reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{}) else admission.Token{};
        return .{ .allocator = allocator, .initialization = snapshot, .pending_reservation = reservation, .options = options, .identity = track_identity, .ingested_bytes = initialization.len };
    }
    /// Publish a new initialization at an explicit transport discontinuity.
    /// Drain or discard pending media first. Old segments keep their snapshots.
    /// Timeline and codec identity reset together; lifetime byte/segment budgets
    /// remain cumulative, including bytes in every replacement initialization.
    pub fn reconfigure(self: *Ingest, identity: []const u8, initialization: []const u8) !void {
        try self.options.stream.control.check();
        if (self.pending.items.len != 0) return error.PendingMediaSegment;
        if (self.epoch >= self.options.max_reconfigurations or initialization.len > self.options.max_ingested_bytes -| self.ingested_bytes) return error.ResourceLimitExceeded;
        var replacement = try Ingest.init(self.allocator, identity, initialization, self.options);
        // Keep replacement validation/allocation transactional. Its unused
        // pending reservation is released while the existing one is retained.
        defer replacement.pending_reservation.deinit();
        const previous = self.initialization;
        self.initialization = replacement.initialization;
        self.ingested_bytes += initialization.len;
        self.epoch += 1;
        self.identity = replacement.identity;
        self.next_decode_time = null;
        self.ended = false;
        self.resetScan();
        previous.deinit();
    }
    /// Partial network reads may be supplied in any chunk size. Drain nextSegment
    /// after arrivals; no transport segment envelope is needed.
    pub fn push(self: *Ingest, bytes: []const u8) !void {
        try self.options.stream.control.check();
        if (self.ended) return error.MediaStreamEnded;
        if (self.segments >= self.options.max_segments or bytes.len > self.options.max_ingested_bytes -| self.ingested_bytes) return error.ResourceLimitExceeded;
        const size = try std.math.add(usize, self.pending.items.len, bytes.len);
        if (size > self.options.stream.max_bytes -| self.initialization.bytes.len) return error.ResourceLimitExceeded;
        if (size > self.pending.capacity) {
            const previous = self.pending_reservation.resources;
            // Arbitrarily small HTTP pieces must not force quadratic copying.
            // Charge actual capacity and the simultaneous old/new allocation.
            // A tight shared budget can still choose the exact required size.
            const ceiling = self.options.stream.max_bytes -| self.initialization.bytes.len;
            var capacity = @max(size, @min(ceiling, self.pending.capacity *| 2));
            self.pending_reservation.resize(.{ .host_bytes = try std.math.add(usize, capacity, self.pending.capacity) }) catch |err| {
                if (capacity == size or err != error.SharedAdmissionExceeded) return err;
                capacity = size;
                try self.pending_reservation.resize(.{ .host_bytes = try std.math.add(usize, capacity, self.pending.capacity) });
            };
            const grown = self.allocator.alloc(u8, capacity) catch |err| {
                try self.pending_reservation.resize(previous);
                return err;
            };
            @memcpy(grown[0..self.pending.items.len], self.pending.items);
            const used = self.pending.items.len;
            self.pending.deinit(self.allocator);
            self.pending = .empty;
            self.pending.items = grown[0..used];
            self.pending.capacity = grown.len;
            try self.pending_reservation.resize(.{ .host_bytes = capacity });
        }
        self.pending.appendSliceAssumeCapacity(bytes);
        self.ingested_bytes += bytes.len;
    }
    /// A segment consists of complete moof/mdat boxes, using moof-relative
    /// addressing. Failed validation retains input for inspection/discard/retry.
    /// The returned value can move; its backing Source stays at a stable address.
    /// Drain complete automatically framed segments. A following moof/styp ends
    /// the preceding moof + one-or-more mdat sequence; final EOF closes the last.
    /// Size-zero boxes need final EOF. Failed commits retain all pending bytes.
    pub fn nextSegment(self: *Ingest, end_of_stream: bool) !?Segment {
        try self.options.stream.control.check();
        self.ended = self.ended or end_of_stream;
        while (self.scan_cursor < self.pending.items.len) {
            try self.options.stream.control.check();
            const available = self.pending.items[self.scan_cursor..];
            if (available.len < 8) return if (self.ended) error.IncompleteMediaSegment else null;
            const size32 = std.mem.readInt(u32, available[0..4], .big);
            const typ = std.mem.readInt(u32, available[4..8], .big);
            // A boundary header suffices: its payload may still be arriving.
            if (self.scan_fragment and (typ == iso.fourcc("moof") or typ == iso.fourcc("styp"))) {
                if (!self.scan_payload) return error.IncompleteMediaSegment;
                return try self.commitPrefix(self.scan_cursor);
            }
            var size: u64 = size32;
            var header: usize = 8;
            if (size32 == 1) {
                if (available.len < 16) return if (self.ended) error.IncompleteMediaSegment else null;
                size = std.mem.readInt(u64, available[8..16], .big);
                header = 16;
            } else if (size32 == 0) {
                if (!self.ended) return null;
                size = available.len;
            }
            if (size < header) return error.MalformedMedia;
            if (size > self.options.stream.max_bytes -| self.initialization.bytes.len) return error.ResourceLimitExceeded;
            if (size > available.len) return if (self.ended) error.IncompleteMediaSegment else null;
            if (self.scan_boxes >= self.options.index.max_boxes) return error.ResourceLimitExceeded;
            if (typ == iso.fourcc("moov") or typ == iso.fourcc("ftyp")) return error.UnsupportedDynamicVideoConfig;
            if (typ == iso.fourcc("mdat") and !self.scan_fragment) return error.MalformedMedia;
            if (typ == iso.fourcc("moof")) self.scan_fragment = true;
            if (typ == iso.fourcc("mdat")) self.scan_payload = true;
            self.scan_cursor += @intCast(size);
            self.scan_boxes += 1;
        }
        if (!self.ended) return null;
        if (self.scan_fragment) {
            if (!self.scan_payload) return error.IncompleteMediaSegment;
            return try self.commitPrefix(self.scan_cursor);
        }
        // Complete non-media trailer boxes do not publish an empty segment.
        self.discardSegment();
        return null;
    }
    pub fn finishSegment(self: *Ingest) !Segment {
        return self.commitPrefix(self.pending.items.len);
    }
    pub fn finishHlsSegment(self: *Ingest, progress: SamplesDigest, parent_replay: bool) !Segment {
        return self.commitPrefixProgress(self.pending.items.len, progress, parent_replay);
    }
    fn commitPrefix(self: *Ingest, prefix: usize) !Segment {
        return self.commitPrefixProgress(prefix, null, false);
    }
    fn commitPrefixProgress(self: *Ingest, prefix: usize, progress: ?SamplesDigest, parent_replay: bool) !Segment {
        try self.options.stream.control.check();
        if (self.segments >= self.options.max_segments) return error.ResourceLimitExceeded;
        var cursor: usize = 0;
        var fragment = false;
        var payload = false;
        var boxes: usize = 0;
        while (cursor < prefix) {
            if (boxes >= self.options.index.max_boxes) return error.ResourceLimitExceeded;
            boxes += 1;
            const box = try iso.readBox(self.pending.items[0..prefix], cursor);
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
        const snapshot = try stream.Snapshot.copy(self.allocator, owned_identity, &.{ self.initialization.bytes, self.pending.items[0..prefix] }, self.options.stream);
        errdefer snapshot.deinit();
        var reader = try mp4.Reader.init(self.allocator, &snapshot.input, self.options.index);
        errdefer reader.deinit();
        const identity = Identity.from(reader.track);
        if (self.identity) |previous| if (!std.meta.eql(previous, identity)) return error.UnsupportedDynamicVideoConfig;
        var skip: usize = 0;
        var digest = progress;
        if (parent_replay) {
            const previous = progress orelse return error.InvalidHlsReplay;
            if (previous.count > reader.packets.len) return error.SourceVersionChanged;
            var prefix_digest = SamplesDigest{};
            try prefix_digest.extend(&reader, 0, previous.count);
            if (!std.mem.eql(u8, &prefix_digest.bytes(), &previous.bytes())) return error.SourceVersionChanged;
            skip = previous.count;
        }
        if (self.next_decode_time) |next| if (skip < reader.packets.len and reader.packets[skip].media_dts < next) return error.NonMonotonicMediaSegment;
        if (digest) |*value| try value.extend(&reader, skip, reader.packets.len);
        const last = reader.packets[reader.packets.len - 1];
        const next = try std.math.add(i64, last.media_dts, last.duration);
        if (skip != 0) try reader.discardPacketPrefix(skip);
        self.identity = identity;
        self.next_decode_time = next;
        const ordinal = self.segments;
        self.segments += 1;
        const remaining = self.pending.items.len - prefix;
        std.mem.copyForwards(u8, self.pending.items[0..remaining], self.pending.items[prefix..]);
        self.pending.items.len = remaining;
        self.resetScan();
        return .{ .backing = snapshot, .reader = reader, .ordinal = ordinal, .epoch = self.epoch, .identity = owned_identity, .identity_reservation = identity_reservation, .samples_digest = digest };
    }
    fn resetScan(self: *Ingest) void {
        self.scan_cursor = 0;
        self.scan_boxes = 0;
        self.scan_fragment = false;
        self.scan_payload = false;
    }
    pub fn discardSegment(self: *Ingest) void {
        self.pending.clearRetainingCapacity();
        self.resetScan();
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

test "live MP4 automatic framing handles every byte arrival and final EOF" {
    const bytes = @embedFile("../testdata/fragmented.mp4");
    var split: usize = 0;
    var expected: usize = 0;
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        const box = try iso.readBox(bytes, cursor);
        if (box.typ == iso.fourcc("moof")) {
            if (expected == 0) split = cursor;
            expected += 1;
        }
        cursor = box.end;
    }
    const Harness = struct {
        fn run(backing: std.mem.Allocator, init_bytes: []const u8, media_bytes: []const u8, expected_count: usize, chunk: usize) !void {
            var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
            const allocator = no_resize.allocator();
            var pool = admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
            {
                var ingest = try Ingest.init(allocator, "automatic-live", init_bytes, .{ .index = .{ .max_index_bytes = 256 * 1024 }, .stream = .{ .max_bytes = 65536, .admission_pool = &pool } });
                defer ingest.deinit();
                var count: usize = 0;
                var pos: usize = 0;
                while (pos < media_bytes.len) {
                    const end = @min(pos + chunk, media_bytes.len);
                    try ingest.push(media_bytes[pos..end]);
                    pos = end;
                    while (try ingest.nextSegment(false)) |value| {
                        var segment = value;
                        defer segment.deinit();
                        try std.testing.expectEqual(count, segment.ordinal);
                        count += 1;
                    }
                }
                while (try ingest.nextSegment(true)) |value| {
                    var segment = value;
                    defer segment.deinit();
                    try std.testing.expectEqual(count, segment.ordinal);
                    count += 1;
                }
                try std.testing.expectEqual(expected_count, count);
                try std.testing.expectError(error.MediaStreamEnded, ingest.push("x"));
            }
            try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
        }
    };
    try Harness.run(std.testing.allocator, bytes[0..split], bytes[split..], expected, 1);
    try Harness.run(std.testing.allocator, bytes[0..split], bytes[split..], expected, bytes.len);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{ bytes[0..split], bytes[split..], expected, bytes.len });
}

test "live MP4 automatic framing validates partial extended headers and EOF sized payloads" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/fragmented.mp4");
    var split: usize = 0;
    var end: usize = bytes.len;
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        const box = try iso.readBox(bytes, cursor);
        if (box.typ == iso.fourcc("moof")) {
            if (split == 0) split = cursor else {
                end = cursor;
                break;
            }
        }
        cursor = box.end;
    }
    {
        var ingest = try Ingest.init(a, "partial-header", bytes[0..split], .{});
        defer ingest.deinit();
        const extended = [_]u8{ 0, 0, 0, 1, 'f', 'r', 'e', 'e', 0, 0, 0, 0, 0, 0, 0, 16 };
        try ingest.push(extended[0..9]);
        try std.testing.expect((try ingest.nextSegment(false)) == null);
        try std.testing.expectError(error.IncompleteMediaSegment, ingest.nextSegment(true));
        try std.testing.expectEqual(@as(usize, 9), ingest.pending.items.len);
    }
    {
        var ingest = try Ingest.init(a, "malformed-size", bytes[0..split], .{});
        defer ingest.deinit();
        try ingest.push(&.{ 0, 0, 0, 7, 'f', 'r', 'e', 'e' });
        try std.testing.expectError(error.MalformedMedia, ingest.nextSegment(false));
        try std.testing.expectEqual(@as(usize, 8), ingest.pending.items.len);
    }
    {
        const segment = try a.dupe(u8, bytes[split..end]);
        defer a.free(segment);
        cursor = 0;
        var payload: ?usize = null;
        while (cursor < segment.len) {
            const box = try iso.readBox(segment, cursor);
            if (box.typ == iso.fourcc("mdat") and box.end == segment.len) payload = cursor;
            cursor = box.end;
        }
        const offset = payload orelse return error.MissingMediaPayload;
        @memset(segment[offset..][0..4], 0);
        var ingest = try Ingest.init(a, "eof-payload", bytes[0..split], .{});
        defer ingest.deinit();
        try ingest.push(segment);
        try std.testing.expect((try ingest.nextSegment(false)) == null);
        var framed = (try ingest.nextSegment(true)) orelse return error.MissingMediaPayload;
        defer framed.deinit();
        try std.testing.expect(framed.reader.packets.len > 0);
        try std.testing.expect((try ingest.nextSegment(true)) == null);
    }
}

test "live MP4 reconfiguration is transactional and preserves outstanding old epoch leases" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/fragmented.mp4");
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        const box = try iso.readBox(bytes, cursor);
        if (box.typ == iso.fourcc("moof")) break;
        cursor = box.end;
    }
    const Harness = struct {
        fn run(allocator: std.mem.Allocator, initialization: []const u8, media_bytes: []const u8) !void {
            var ingest = try Ingest.init(allocator, "live", initialization, .{ .max_reconfigurations = 1 });
            defer ingest.deinit();
            try ingest.push(media_bytes);
            try std.testing.expectError(error.PendingMediaSegment, ingest.reconfigure("changed", initialization));
            var first = try ingest.finishSegment();
            defer first.deinit();
            var lease = try first.reader.readPacket(0);
            defer lease.deinit();
            const expected = try allocator.dupe(u8, lease.bytes);
            defer allocator.free(expected);
            const previous = ingest.initialization;
            const ingested = ingest.ingested_bytes;
            try std.testing.expectError(error.MissingMovieMetadata, ingest.reconfigure("invalid", &.{}));
            try std.testing.expectEqual(previous, ingest.initialization);
            try std.testing.expectEqual(ingested, ingest.ingested_bytes);
            try std.testing.expectEqual(@as(usize, 0), ingest.epoch);
            try ingest.reconfigure("changed", initialization);
            try std.testing.expectEqual(@as(usize, 1), ingest.epoch);
            try std.testing.expectEqual(ingested + initialization.len, ingest.ingested_bytes);
            try std.testing.expectError(error.ResourceLimitExceeded, ingest.reconfigure("again", initialization));
            // Replaying the old DTS is valid only after the explicit discontinuity.
            try ingest.push(media_bytes);
            var second = try ingest.finishSegment();
            defer second.deinit();
            try std.testing.expectEqual(@as(usize, 0), first.epoch);
            try std.testing.expectEqual(@as(usize, 1), second.epoch);
            try std.testing.expectEqual(@as(usize, 1), second.ordinal);
            try std.testing.expectEqualSlices(u8, expected, lease.bytes);
        }
    };
    try Harness.run(a, bytes[0..cursor], bytes[cursor..]);
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{ bytes[0..cursor], bytes[cursor..] });
}

test "live MP4 reconfiguration accepts new SPS and geometry at an explicit epoch boundary" {
    const a = std.testing.allocator;
    const original = @embedFile("../testdata/fragmented.mp4");
    const changed = @embedFile("../testdata/live-reconfigure.mp4");
    const Split = struct {
        fn initialization(bytes: []const u8) !usize {
            var cursor: usize = 0;
            while (cursor < bytes.len) {
                const box = try iso.readBox(bytes, cursor);
                if (box.typ == iso.fourcc("moof")) return cursor;
                cursor = box.end;
            }
            return error.MissingMovieMetadata;
        }
    };
    const original_split = try Split.initialization(original);
    const changed_split = try Split.initialization(changed);
    var ingest = try Ingest.init(a, "same-live-source", original[0..original_split], .{});
    defer ingest.deinit();
    try ingest.push(original[original_split..]);
    var first = try ingest.finishSegment();
    defer first.deinit();
    var lease = try first.reader.readPacket(0);
    defer lease.deinit();
    const previous_hash = Identity.from(first.reader.track).config;
    const malformed = try a.dupe(u8, changed[0..changed_split]);
    defer a.free(malformed);
    const avcc_type = std.mem.indexOf(u8, malformed, "avcC") orelse return error.TestUnexpectedResult;
    malformed[avcc_type + 4] = 0; // Invalid AVC configuration version.
    try std.testing.expectError(error.MalformedMedia, ingest.reconfigure("same-live-source", malformed));
    try std.testing.expectEqual(@as(usize, 0), ingest.epoch);
    try ingest.reconfigure("same-live-source", changed[0..changed_split]);
    try ingest.push(changed[changed_split..]);
    var second = try ingest.finishSegment();
    defer second.deinit();
    try std.testing.expectEqual(@as(u16, 96), second.reader.track.width);
    try std.testing.expectEqual(@as(u16, 48), second.reader.track.height);
    try std.testing.expect(!std.mem.eql(u8, &previous_hash, &Identity.from(second.reader.track).config));
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
    try std.testing.expectEqualSlices(u8, first.backing.bytes[@intCast(first.reader.packets[0].offset)..][0..lease.bytes.len], lease.bytes);
}

test "live MP4 packet slice outlives original identity and metadata leases" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    const bytes = @embedFile("../testdata/fragmented.mp4");
    var ingest = try Ingest.init(a, "slice-owner", bytes[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    try ingest.push(bytes[797..11206]);
    var original = try ingest.finishSegment();
    var slice = try original.packetSlice(a, 0, 1);
    defer slice.deinit();
    original.deinit();
    try std.testing.expectEqualStrings("slice-owner/segment/0", slice.backing.input.identity);
    var lease = try slice.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
    try std.testing.expect(slice.reader.track.avcc.len > 0);
}
