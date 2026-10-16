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

//! Planning-only AVC picture assembly. Slice payloads remain untrusted until
//! decode: this index proves transport membership, never entropy completeness.
//! No inverse transforms, reference pictures, deblocking or pixel allocations.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const dynamic = @import("h264_dynamic.zig");
const options_mod = @import("h264_pictures.zig");
const Header = struct {
    first: u32,
    frame: u32,
    plane: u2,
    field: bool,
    parity: u1,
    reference: bool,
    idr: bool,
    idr_id: u32,
    poc: [2]i32,
    redundant: u32,
};
// Decode only the consumed RBSP prefix. Entropy bytes are neither copied nor
// interpreted during planning; the actual decoder validates their full syntax.
const Bits = struct {
    bytes: []const u8,
    cursor: usize = 1,
    zeros: u2 = 0,
    byte: u8 = 0,
    remaining: u4 = 0,
    fn read(self: *Bits, count: usize) !u32 {
        var out: u32 = 0;
        for (0..count) |_| {
            if (self.remaining == 0) {
                if (self.cursor >= self.bytes.len) return error.MalformedVideoPacket;
                var byte = self.bytes[self.cursor];
                self.cursor += 1;
                if (self.zeros == 2 and byte == 3) {
                    if (self.cursor >= self.bytes.len or self.bytes[self.cursor] > 3) return error.MalformedVideoPacket;
                    byte = self.bytes[self.cursor];
                    self.cursor += 1;
                    self.zeros = 0;
                } else if (self.zeros == 2 and byte <= 2) return error.MalformedVideoPacket;
                self.zeros = if (byte == 0) @min(@as(u2, 2), self.zeros + 1) else 0;
                self.byte = byte;
                self.remaining = 8;
            }
            self.remaining -= 1;
            out = (out << 1) | ((self.byte >> @as(u3, @intCast(self.remaining))) & 1);
        }
        return out;
    }
    fn ue(self: *Bits) !u32 {
        var zeros: u5 = 0;
        while (try self.read(1) == 0) {
            if (zeros == 30) return error.MalformedVideoPacket;
            zeros += 1;
        }
        return (@as(u32, 1) << zeros) - 1 + try self.read(zeros);
    }
    fn se(self: *Bits) !i32 {
        const n = try self.ue();
        return if (n & 1 != 0) @intCast((n + 1) / 2) else -@as(i32, @intCast(n / 2));
    }
};
fn header(nal: []const u8, cfg: h264.Config) !Header {
    var bits = Bits{ .bytes = nal };
    const first = try bits.ue();
    if (try bits.ue() > 9 or try bits.ue() != cfg.pps) return error.MalformedVideoPacket;
    const plane: u2 = if (cfg.separate_planes) @intCast(try bits.read(2)) else 0;
    if (plane > 2) return error.MalformedVideoPacket;
    const frame = try bits.read(cfg.frame_bits);
    const field = !cfg.frame_only and try bits.read(1) != 0;
    const parity: u1 = if (field) @intCast(try bits.read(1)) else 0;
    const idr = nal[0] & 31 == 5;
    const idr_id = if (idr) try bits.ue() else 0;
    var poc: [2]i32 = .{ 0, 0 };
    if (cfg.poc_bits) |n| {
        poc[0] = @intCast(try bits.read(n));
        if (cfg.bottom_poc and !field) poc[1] = try bits.se();
    } else if (cfg.poc_type == 1 and !cfg.poc_zero) {
        poc[0] = try bits.se();
        if (cfg.bottom_poc and !field) poc[1] = try bits.se();
    }
    const redundant = if (cfg.redundant) try bits.ue() else 0;
    if (redundant > 127 or first >= cfg.coded_width / 16 * (cfg.coded_height / 16) / (if (field) @as(usize, 2) else 1)) return error.MalformedVideoPacket;
    return .{ .first = first, .frame = frame, .plane = plane, .field = field, .parity = parity, .reference = nal[0] & 0x60 != 0, .idr = idr, .idr_id = idr_id, .poc = poc, .redundant = redundant };
}
const Row = struct {
    picture: h264.Picture,
    end: i64,
    lanes: [6]?Header = @splat(null),
    starts: u6 = 0,
    mask: u6 = 0,
    required: u6,
    slices: usize = 0,
};
pub fn planning(a: std.mem.Allocator, reader: *media.mp4.Reader, options: options_mod.Options, registry: ?*dynamic.Session) !options_mod.Index {
    try reader.input.control.check();
    if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
    if (reader.packets.len == 0) return error.EmptyVideo;
    if (reader.packets.len > options.max_packets or options.max_pictures == 0) return error.ResourceLimitExceeded;
    const cfg0 = try h264.configParse(a, reader.track.avcc);
    defer cfg0.groups.deinit(a);
    const capacity = @min(reader.packets.len, options.max_pictures);
    // Exact row storage plus a conservative hash-table/transient allocation
    // allowance; no pixel/decode reservation is needed for this phase.
    const bound = try std.math.mul(usize, capacity, @sizeOf(Row) + @sizeOf(h264.Picture) + 256);
    var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = bound }) else media.admission.Token{};
    errdefer reservation.deinit();
    const rows = try a.alloc(Row, capacity);
    defer a.free(rows);
    var lookup: std.AutoHashMap(u64, usize) = .init(a);
    defer lookup.deinit();
    try lookup.ensureTotalCapacity(std.math.cast(u32, capacity) orelse return error.ResourceLimitExceeded);
    var count: usize = 0;
    var packet_index: usize = 0;
    var generation: u32 = 0;
    var epoch_index: usize = 0;
    var payload: u64 = 0;
    while (packet_index < reader.packets.len) : (packet_index += 1) {
        try reader.input.control.check();
        if (registry) |owner| {
            while (epoch_index + 1 < owner.epochs.items.len and owner.epochs.items[epoch_index + 1].start <= packet_index) {
                epoch_index += 1;
                generation = try std.math.add(u32, generation, 1);
            }
        }
        if (reader.packets[packet_index].size > options.decode.max_packet_bytes) return error.ResourceLimitExceeded;
        var lease = try reader.readPacket(packet_index);
        defer lease.deinit();
        var span = try @import("h264_partition_span.zig").Span.init(a, reader, packet_index, packet_index, lease.bytes, options.decode);
        defer span.deinit();
        payload += lease.bytes.len + span.extra_read_bytes;
        try @import("avc.zig").validatePortablePacket(span.bytes, reader.track.nal_length_bytes);
        var at: usize = 0;
        var packet_row: ?usize = null;
        while (at < span.bytes.len) {
            try reader.input.control.check();
            var size: usize = 0;
            for (span.bytes[at..][0..reader.track.nal_length_bytes]) |b| size = (size << 8) | b;
            at += reader.track.nal_length_bytes;
            const nal = span.bytes[at..][0..size];
            at += size;
            const kind = nal[0] & 31;
            if (kind != 1 and kind != 2 and kind != 5) continue;
            const cfg = if (registry) |owner| blk: {
                const id = owner.packet_configs[packet_index];
                if (id >= owner.entries.items.len) return error.MalformedVideoConfig;
                break :blk owner.entries.items[id].config;
            } else cfg0;
            const h = try header(nal, cfg);
            if (h.redundant != 0) continue;
            const lane: usize = @as(usize, h.plane) * 2 + h.parity;
            const bit: u6 = @as(u6, 1) << @intCast(lane);
            var key = (@as(u64, generation) << 32) | (@as(u64, h.frame) << 1) | @intFromBool(h.reference);
            var existing = if (h.field) lookup.get(key) else packet_row;
            if (existing) |id| {
                const row = &rows[id];
                if (row.lanes[lane]) |old| {
                    if (!std.meta.eql(old.poc, h.poc) or old.idr != h.idr or old.idr_id != h.idr_id or (h.first == 0 and row.starts & bit != 0 and row.picture.last_packet.? < packet_index)) {
                        if (row.mask != row.required) return error.IncompleteVideoPicture;
                        existing = null;
                    }
                }
            }
            if (h.idr and existing == null) {
                generation = try std.math.add(u32, generation, 1);
                key = (@as(u64, generation) << 32) | (@as(u64, h.frame) << 1) | @intFromBool(h.reference);
            }
            const id = existing orelse blk: {
                if (count == capacity) return error.ResourceLimitExceeded;
                const id = count;
                count += 1;
                rows[id] = .{ .picture = .{ .first_packet = packet_index, .last_packet = span.last, .pts = reader.packets[packet_index].pts, .duration = 0, .width = @intCast(cfg.width), .height = @intCast(cfg.height), .bit_depth = cfg.bit_depth, .chroma_format = cfg.chroma_format }, .end = reader.packets[packet_index].pts, .required = if (cfg.separate_planes) (if (h.field) 63 else 21) else (if (h.field) 3 else 1) };
                try lookup.put(key, id);
                break :blk id;
            };
            if (packet_row) |previous| if (previous != id) return error.MixedVideoPictures;
            packet_row = id;
            const row = &rows[id];
            if (row.picture.width != cfg.width or row.picture.height != cfg.height or row.picture.bit_depth != cfg.bit_depth or row.required != (if (cfg.separate_planes) (if (h.field) @as(u6, 63) else 21) else (if (h.field) @as(u6, 3) else 1))) return error.MixedVideoPictures;
            if (row.slices >= try std.math.mul(usize, options.decode.max_slices, if (cfg.separate_planes) 3 else 1)) return error.ResourceLimitExceeded;
            row.slices += 1;
            row.mask |= bit;
            if (h.first == 0) row.starts |= bit;
            row.lanes[lane] = h;
            row.picture.last_packet = span.last;
            const members = if (span.partitioned) span.members.items else &.{packet_index};
            for (members) |member| {
                const part = reader.packets[member];
                row.picture.pts = @min(row.picture.pts, part.pts);
                row.end = @max(row.end, try std.math.add(i64, part.pts, part.duration));
            }
        }
        packet_index = span.last;
    }
    const pictures = try a.alloc(h264.Picture, count);
    errdefer a.free(pictures);
    for (rows[0..count], pictures) |row, *picture| {
        if (row.mask != row.required) return error.IncompleteVideoPicture;
        picture.* = row.picture;
        picture.duration = std.math.cast(u32, std.math.sub(i64, row.end, row.picture.pts) catch return error.TimestampOverflow) orelse return error.TimestampOverflow;
    }
    const Less = struct {
        fn less(_: void, l: h264.Picture, r: h264.Picture) bool {
            return l.last_packet.? < r.last_packet.? or (l.last_packet.? == r.last_packet.? and l.first_packet < r.first_packet);
        }
    };
    std.mem.sort(h264.Picture, pictures, {}, Less.less);
    // Keep the peak reservation through deferred row/hash destruction. The
    // returned token is reduced by the caller after those owners are gone.
    return .{ .allocator = a, .pictures = pictures, .timescale = reader.track.timescale, .packet_count = reader.packets.len, .payload_bytes = payload, .reservation = reservation };
}
