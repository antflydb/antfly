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
    allocator: std.mem.Allocator,
    nearest: []usize,
    stamp: Stamp,
    portable: bool,
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
    fn start(self: *Builder, selected: usize) !usize {
        if (self.options.mode == .from_start) return 0;
        var index = selected;
        if (self.options.seek_cache) |cache| {
            if (cache.nearest[selected] != std.math.maxInt(usize)) return cache.nearest[selected];
        }
        while (true) {
            if (self.search_steps >= self.options.max_search_steps) return error.ResourceLimitExceeded;
            self.search_steps += 1;
            if (self.search_steps % 256 == 1) try self.reader.input.control.check();
            if (self.options.seek_cache) |cache| if (cache.nearest[index] != std.math.maxInt(usize)) {
                const anchor = cache.nearest[index];
                @memset(cache.nearest[index .. selected + 1], anchor);
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
                    if (size > self.options.max_packet_bytes or self.probe_candidates >= self.options.max_probe_candidates or size > self.options.max_probe_bytes -| self.probe_bytes) return error.ResourceLimitExceeded;
                    self.probe_candidates += 1;
                    var lease = try self.reader.readPacket(index);
                    errdefer lease.deinit();
                    self.probe_bytes += size;
                    if (try (if (self.options.portable_config) avc.isPortableIdr(lease.bytes, self.reader.track.nal_length_bytes) else avc.isIdr(lease.bytes, self.reader.track.nal_length_bytes))) {
                        try self.anchors.append(self.allocator, .{ .index = index, .payload = lease });
                        if (self.options.seek_cache) |cache| @memset(cache.nearest[index .. selected + 1], index);
                        return index;
                    }
                    lease.deinit();
                    try self.rejected.append(self.allocator, index);
                }
            }
            if (index == 0) {
                if (self.options.seek_cache) |cache| @memset(cache.nearest[0 .. selected + 1], 0);
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
            if ((first <= last.last or first - last.last - 1 <= options.merge_gap_packets) and index - last.first + 1 <= options.max_run_packets) {
                last.last = index;
                continue;
            }
        }
        try runs.append(allocator, .{ .first = first, .last = index });
    }
    var submissions: usize = 0;
    for (runs.items) |run| submissions = std.math.add(usize, submissions, run.last - run.first + 1) catch return error.ResourceLimitExceeded;
    if (submissions > options.max_decode_packets) return error.ResourceLimitExceeded;
    const owned_runs = try runs.toOwnedSlice(allocator);
    errdefer allocator.free(owned_runs);
    return .{ .allocator = allocator, .selection = copied, .runs = owned_runs, .anchors = try builder.anchors.toOwnedSlice(allocator), .stamp = Stamp.init(reader), .submitted_packets = submissions, .probe_candidates = builder.probe_candidates, .probe_bytes = builder.probe_bytes, .skipped_packets = sorted[sorted.len - 1] + 1 - submissions };
}
