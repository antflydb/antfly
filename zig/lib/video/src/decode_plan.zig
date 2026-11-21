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

//! Portable, conservative static-avc1 dependency planning. Sync hints alone
//! never authorize a nonzero start: the sample's VCL NALs must be IDR slices.
const std = @import("std");
const media = @import("antfly_media");
const avc = @import("avc.zig");
pub const Mode = enum { from_start, verified_idr };
pub const Options = struct {
    mode: Mode = .verified_idr,
    /// Request-owned, immutable-source cache. Never share with mutable readers.
    seek_cache: ?*SeekCache = null,
    /// Software AVC profiles; the decoder still validates full picture syntax.
    portable_config: bool = false,
    /// Metadata-only consumers do not need to pin complete IDR payloads.
    headers_only: bool = false,
    /// Split adjacent IDR runs before exceeding a decoder's dependency bound.
    max_run_packets: usize = std.math.maxInt(usize),
    max_selections: usize = 64,
    max_decode_packets: usize = 500_000,
    max_probe_candidates: usize = 256,
    max_probe_bytes: usize = 16 * 1024 * 1024,
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_search_steps: usize = 1_000_000,
    /// Bridge a gap when skipping it would save no more than this many packets.
    /// Zero minimizes submissions; tune from measured source/backend costs.
    merge_gap_packets: usize = 0,
};
pub const Run = struct { first: usize, last: usize };
const Anchor = struct { index: usize, payload: media.source.Lease };
pub const Stamp = struct {
    identity: [32]u8,
    config: [32]u8,
    codec: media.mp4.Codec,
    track_id: u32,
    timescale: u32,
    packets: usize,
    pub fn init(reader: *const media.mp4.Reader) Stamp {
        var out = Stamp{ .codec = reader.track.codec, .identity = undefined, .config = undefined, .track_id = reader.track.id, .timescale = reader.track.timescale, .packets = reader.packets.len };
        std.crypto.hash.sha2.Sha256.hash(reader.input.identity, &out.identity, .{});
        std.crypto.hash.sha2.Sha256.hash(reader.track.avcc, &out.config, .{});
        return out;
    }
};
/// Move-only owned plan. Source/provider storage must outlive retained probe
/// leases until deinit. Selection order is copied; runs are in decode order.
pub const Plan = struct {
    allocator: std.mem.Allocator,
    selection: []usize,
    runs: []Run,
    anchors: []Anchor,
    stamp: Stamp,
    submitted_packets: usize,
    probe_candidates: usize,
    probe_bytes: usize,
    skipped_packets: usize,
    pub fn deinit(self: *Plan) void {
        for (self.anchors) |*anchor| anchor.payload.deinit();
        self.allocator.free(self.anchors);
        self.allocator.free(self.selection);
        self.allocator.free(self.runs);
        self.* = undefined;
    }
    pub fn anchorBytes(self: *const Plan, index: usize) ?[]const u8 {
        for (self.anchors) |anchor| if (anchor.index == index) return anchor.payload.bytes;
        return null;
    }
};
/// Cached nearest verified IDR for immutable packet metadata and payloads.
/// Payload leases remain plan-owned; this cache retains indexes only.
pub const SeekCache = struct {
    /// Sorted immutable sync-sample directory. Only the result of inspecting
    /// content is published. Failed or canceled reads remain retryable.
    pub const Candidate = struct {
        packet: usize,
        verified: std.atomic.Value(Status) = .init(.unknown),

        const Status = enum(u32) { unknown, probing, idr, rejected };

        fn isIdr(self: *Candidate, builder: *Builder) !bool {
            const io = std.Io.Threaded.global_single_threaded.io();
            while (true) switch (self.verified.load(.acquire)) {
                .idr => return true,
                .rejected => return false,
                .probing => {
                    try builder.reader.input.control.check();
                    try std.Io.futexWaitTimeout(io, Status, &self.verified.raw, .probing, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } });
                },
                .unknown => {
                    if (self.verified.cmpxchgStrong(.unknown, .probing, .acq_rel, .acquire) != null) continue;
                    var status: Status = .unknown;
                    defer {
                        self.verified.store(status, .release);
                        std.Io.futexWake(io, Status, &self.verified.raw, std.math.maxInt(u32));
                    }
                    if (builder.probe_candidates >= builder.options.max_probe_candidates or builder.reader.packets[self.packet].size > builder.options.max_packet_bytes) return error.ResourceLimitExceeded;
                    builder.probe_candidates += 1;
                    const idr = try builder.probe(self.packet);
                    status = if (idr) .idr else .rejected;
                    return idr;
                },
            };
        }
    };
    candidates: ?[]Candidate = null,
    allocator: std.mem.Allocator,
    nearest: []usize,
    stamp: Stamp,
    portable: bool,
    // A paged request borrows sorted verified IDRs and memoizes just its
    // recently searched intervals. Dense storage remains useful for cold scans.
    verified: []const usize = &.{},
    complete: bool = false,
    registry: ?*const @import("h264_dynamic.zig").Session = null,
    recent: [32]struct { first: usize, last: usize, anchor: usize } = undefined,
    recent_count: usize = 0,
    next_slot: usize = 0,

    pub fn forPage(a: std.mem.Allocator, reader: *const media.mp4.Reader, verified: []const usize) SeekCache {
        return .{ .allocator = a, .nearest = &.{}, .stamp = Stamp.init(reader), .portable = true, .verified = verified };
    }
    fn lookup(self: *const SeekCache, index: usize) ?usize {
        if (self.nearest.len != 0) {
            const value = self.nearest[index];
            return if (value == std.math.maxInt(usize)) null else value;
        }
        const epoch_start = if (self.registry) |registry| registry.independentEpochStart(index) else null;
        if (self.complete) {
            const after = std.sort.upperBound(usize, self.verified, index, usizeOrder);
            return @max(epoch_start orelse 0, if (after == 0) 0 else self.verified[after - 1]);
        }
        if (epoch_start == index) return index;
        for (self.recent[0..self.recent_count]) |range| if (index >= range.first and index <= range.last) return range.anchor;
        return if (std.sort.binarySearch(usize, self.verified, index, usizeOrder) != null) index else null;
    }
    fn usizeOrder(a: usize, b: usize) std.math.Order {
        return std.math.order(a, b);
    }
    fn remember(self: *SeekCache, first: usize, last: usize, anchor: usize) void {
        if (self.nearest.len != 0) {
            @memset(self.nearest[first .. last + 1], anchor);
            return;
        }
        self.recent[self.next_slot] = .{ .first = first, .last = last, .anchor = anchor };
        self.next_slot = (self.next_slot + 1) % self.recent.len;
        self.recent_count = @min(self.recent_count + 1, self.recent.len);
    }

    pub fn init(a: std.mem.Allocator, reader: *const media.mp4.Reader, portable: bool) !SeekCache {
        if (reader.input.identity.len == 0) return error.InvalidVideoReaderSelection;
        const nearest = try a.alloc(usize, reader.packets.len);
        @memset(nearest, std.math.maxInt(usize));
        return .{ .allocator = a, .nearest = nearest, .stamp = Stamp.init(reader), .portable = portable };
    }
    pub fn deinit(self: *SeekCache) void {
        self.allocator.free(self.nearest);
    }
};
const Builder = struct {
    allocator: std.mem.Allocator,
    reader: *media.mp4.Reader,
    options: Options,
    anchors: std.ArrayList(Anchor) = .empty,
    rejected: std.ArrayList(usize) = .empty,
    probe_candidates: usize = 0,
    probe_bytes: usize = 0,
    search_steps: usize = 0,
    fn probe(self: *Builder, index: usize) !bool {
        const packet = self.reader.packets[index];
        const width = self.reader.track.nal_length_bytes;
        if (!self.options.headers_only) {
            if (packet.size > self.options.max_probe_bytes -| self.probe_bytes) return error.ResourceLimitExceeded;
            var payload = try self.reader.readPacket(index);
            defer payload.deinit();
            self.probe_bytes += packet.size;
            const idr = if (self.options.portable_config) try avc.isPortableIdr(payload.bytes, width) else try avc.isIdr(payload.bytes, width);
            if (idr) {
                try self.anchors.append(self.allocator, .{ .index = index, .payload = payload });
                payload = .{ .source = null, .bytes = &.{} };
            }
            return idr;
        }
        if (packet.size == 0 or (width != 1 and width != 2 and width != 4)) return error.MalformedVideoPacket;
        var cursor: usize = 0;
        var nals: usize = 0;
        var idr_start = false;
        while (cursor < packet.size) {
            const header_size = @as(usize, width) + 1;
            if (packet.size - cursor < header_size) return error.MalformedVideoPacket;
            const n = @min(header_size + 1, packet.size - cursor);
            if (n > self.options.max_probe_bytes -| self.probe_bytes or nals == 4096) return error.ResourceLimitExceeded;
            var prefix = try self.reader.input.read(try std.math.add(u64, packet.offset, cursor), n);
            defer prefix.deinit();
            self.probe_bytes += n;
            nals += 1;
            var size: usize = 0;
            for (prefix.bytes[0..width]) |b| size = (size << 8) | b;
            if (size == 0 or size > packet.size - cursor - width) return error.MalformedVideoPacket;
            const kind = try avc.checkedNalType(prefix.bytes[width], self.options.portable_config);
            if (kind >= 1 and kind <= 4) return false;
            if (kind == 5) {
                if (size == 1) return error.MalformedVideoPacket;
                // first_mb_in_slice == 0 is the one-bit Exp-Golomb code 1.
                idr_start = idr_start or prefix.bytes[header_size] & 0x80 != 0;
            }
            cursor += width + size;
        }
        return idr_start;
    }
    fn start(self: *Builder, selected: usize) !usize {
        if (self.options.mode == .from_start) return 0;
        var index = selected;
        if (self.options.seek_cache) |cache| {
            if (cache.lookup(selected)) |anchor| return anchor;
        }
        if (self.options.seek_cache) |cache| if (cache.candidates) |candidates| {
            // Compare only immutable packet numbers, never copy a concurrently
            // updated candidate during the binary search.
            var lo: usize = 0;
            var hi = candidates.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (candidates[mid].packet <= selected) lo = mid + 1 else hi = mid;
            }
            while (lo > 0) {
                lo -= 1;
                if (self.search_steps >= self.options.max_search_steps) return error.ResourceLimitExceeded;
                self.search_steps += 1;
                try self.reader.input.control.check();
                const candidate = &candidates[lo];
                if (!try candidate.isIdr(self)) continue;
                cache.remember(candidate.packet, selected, candidate.packet);
                return candidate.packet;
            }
            cache.remember(0, selected, 0);
            return 0;
        };
        while (true) {
            if (self.search_steps >= self.options.max_search_steps) return error.ResourceLimitExceeded;
            self.search_steps += 1;
            if (self.search_steps % 256 == 1) try self.reader.input.control.check();
            if (self.options.seek_cache) |cache| if (cache.lookup(index)) |anchor| {
                cache.remember(index, selected, anchor);
                return anchor;
            };
            if (self.reader.packets[index].sync) {
                for (self.anchors.items) |anchor| if (anchor.index == index) return index;
                var rejected = false;
                for (self.rejected.items) |other| if (other == index) {
                    rejected = true;
                    break;
                };
                if (!rejected) {
                    const size = self.reader.packets[index].size;
                    if (size > self.options.max_packet_bytes or self.probe_candidates >= self.options.max_probe_candidates) return error.ResourceLimitExceeded;
                    self.probe_candidates += 1;
                    if (try self.probe(index)) {
                        if (self.options.seek_cache) |cache| cache.remember(index, selected, index);
                        return index;
                    }
                    try self.rejected.append(self.allocator, index);
                }
            }
            if (index == 0) {
                if (self.options.seek_cache) |cache| cache.remember(0, selected, 0);
                return 0;
            } // Baseline fallback is not a claimed verified seek.
            index -= 1;
        }
    }
};
pub fn create(allocator: std.mem.Allocator, reader: *media.mp4.Reader, selection: []const usize, options: Options) !Plan {
    try reader.input.control.check();
    if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
    if (options.seek_cache) |cache| {
        if (cache.portable != options.portable_config or !std.meta.eql(cache.stamp, Stamp.init(reader))) return error.InvalidVideoReaderSelection;
    }
    if (options.portable_config) try avc.validatePortableConfig(reader.track.avcc) else try avc.validateConfig(reader.track.avcc);
    if (selection.len == 0 or selection.len > options.max_selections) return error.ResourceLimitExceeded;
    for (selection, 0..) |index, i| {
        if (index >= reader.packets.len) return error.InvalidPacketIndex;
        for (selection[0..i]) |other| if (other == index) return error.DuplicateFrameSelection;
    }
    const copied = try allocator.dupe(usize, selection);
    errdefer allocator.free(copied);
    const sorted = try allocator.dupe(usize, selection);
    defer allocator.free(sorted);
    std.mem.sort(usize, sorted, {}, std.sort.asc(usize));
    var builder = Builder{ .allocator = allocator, .reader = reader, .options = options };
    defer builder.rejected.deinit(allocator);
    errdefer {
        for (builder.anchors.items) |*anchor| anchor.payload.deinit();
        builder.anchors.deinit(allocator);
    }
    var runs: std.ArrayList(Run) = .empty;
    errdefer runs.deinit(allocator);
    for (sorted) |index| {
        try reader.input.control.check();
        const first = try builder.start(index);
        if (index - first + 1 > options.max_run_packets) return error.ResourceLimitExceeded;
        if (runs.items.len != 0) {
            const last = &runs.items[runs.items.len - 1];
            if (first == last.first) {
                last.last = index;
                continue;
            }
        }
        try runs.append(allocator, .{ .first = first, .last = index });
    }
    // First collect each restart's entire dependency interval. Only then can
    // we bridge neighboring GOPs without splitting a later GOP across runs.
    var kept: usize = 0;
    for (runs.items) |run| {
        if (kept != 0) {
            const last = &runs.items[kept - 1];
            if (run.first - last.last - 1 <= options.merge_gap_packets and run.last - last.first + 1 <= options.max_run_packets) {
                last.last = run.last;
                continue;
            }
        }
        runs.items[kept] = run;
        kept += 1;
    }
    runs.shrinkRetainingCapacity(kept);
    var submissions: usize = 0;
    for (runs.items) |run| submissions = std.math.add(usize, submissions, run.last - run.first + 1) catch return error.ResourceLimitExceeded;
    if (submissions > options.max_decode_packets) return error.ResourceLimitExceeded;
    const owned_runs = try runs.toOwnedSlice(allocator);
    errdefer allocator.free(owned_runs);
    return .{ .allocator = allocator, .selection = copied, .runs = owned_runs, .anchors = try builder.anchors.toOwnedSlice(allocator), .stamp = Stamp.init(reader), .submitted_packets = submissions, .probe_candidates = builder.probe_candidates, .probe_bytes = builder.probe_bytes, .skipped_packets = sorted[sorted.len - 1] + 1 - submissions };
}
