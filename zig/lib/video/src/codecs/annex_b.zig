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

//! AV1 Annex B framing. Normalize each temporal unit to low-overhead OBUs before
//! entering the decoder. All nested lengths and redundant sizes are checked.
const std = @import("std");
const media = @import("antfly_media");
pub const Units = struct {
    allocator: std.mem.Allocator,
    packets: [][]const u8,
    pub fn deinit(self: *Units) void {
        for (self.packets) |packet| self.allocator.free(packet);
        self.allocator.free(self.packets);
        self.* = undefined;
    }
};
const Cursor = struct {
    bytes: []const u8,
    at: usize = 0,
    fn length(self: *Cursor) !usize {
        var value: u64 = 0;
        for (0..8) |i| {
            if (self.at == self.bytes.len) return error.InvalidVideoData;
            const byte = self.bytes[self.at];
            self.at += 1;
            value |= @as(u64, byte & 127) << @intCast(i * 7);
            if (byte & 128 == 0) {
                if (value > std.math.maxInt(u32)) return error.InvalidVideoData;
                return @intCast(value);
            }
        }
        return error.InvalidVideoData;
    }
    fn unit(self: *Cursor) ![]const u8 {
        const size = try self.length();
        if (size == 0 or size > self.bytes.len - self.at) return error.InvalidVideoData;
        const bytes = self.bytes[self.at..][0..size];
        self.at += size;
        return bytes;
    }
};
fn appendLength(out: *std.array_list.Managed(u8), size: usize) !void {
    var remaining = size;
    while (true) {
        const byte: u8 = @intCast(remaining & 127);
        remaining >>= 7;
        try out.append(byte | @as(u8, if (remaining == 0) 0 else 128));
        if (remaining == 0) return;
    }
}
pub fn normalize(allocator: std.mem.Allocator, bytes: []const u8, max_bytes: usize, max_units: usize, control: media.source.Control) !Units {
    if (bytes.len == 0) return error.InvalidVideoData;
    var packets: std.array_list.Managed([]const u8) = .init(allocator);
    errdefer {
        for (packets.items) |packet| allocator.free(packet);
        packets.deinit();
    }
    var total: usize = 0;
    var frame_units: usize = 0;
    var stream = Cursor{ .bytes = bytes };
    while (stream.at < bytes.len) {
        var temporal = Cursor{ .bytes = try stream.unit() };
        var output: std.array_list.Managed(u8) = .init(allocator);
        defer output.deinit();
        var first_frame = true;
        while (temporal.at < temporal.bytes.len) {
            try control.check();
            if (frame_units >= max_units) return error.ResourceLimitExceeded;
            frame_units += 1;
            var coded = Cursor{ .bytes = try temporal.unit() };
            var first_obu = true;
            var frame_header: ?u8 = null;
            var frame_extension: u8 = 0;
            var frame_extension_flag: u8 = 0;
            while (coded.at < coded.bytes.len) {
                try control.check();
                const obu = try coded.unit();
                const flags = obu[0];
                if (flags & 0x81 != 0) return error.InvalidVideoData;
                const kind = (flags >> 3) & 15;
                if (first_frame and first_obu) {
                    if (kind != 2) return error.InvalidVideoData;
                } else if (kind == 2) return error.InvalidVideoData;
                var payload_at: usize = 1;
                if (flags & 4 != 0) {
                    if (obu.len < 2 or obu[1] & 7 != 0) return error.InvalidVideoData;
                    payload_at = 2;
                }
                if (kind == 3 or kind == 6) {
                    if (frame_header != null) return error.InvalidVideoData;
                    frame_header = kind;
                    frame_extension_flag = flags & 4;
                    frame_extension = if (flags & 4 != 0) obu[1] else 0;
                } else if (kind == 4) {
                    if (frame_header != 3 or frame_extension_flag != flags & 4 or frame_extension != (if (flags & 4 != 0) obu[1] else 0)) return error.InvalidVideoData;
                }
                if (flags & 2 != 0) {
                    var sized = Cursor{ .bytes = obu, .at = payload_at };
                    const size = try sized.length();
                    payload_at = sized.at;
                    if (size != obu.len - payload_at) return error.InvalidVideoData;
                }
                const payload = obu[payload_at..];
                if (kind == 2 and payload.len != 0) return error.InvalidVideoData;
                if (payload.len > max_bytes -| total or output.items.len > max_bytes -| total -| payload.len) return error.ResourceLimitExceeded;
                try output.append(flags | 2);
                if (flags & 4 != 0) try output.append(obu[1]);
                try appendLength(&output, payload.len);
                try output.appendSlice(payload);
                if (output.items.len > max_bytes -| total) return error.ResourceLimitExceeded;
                first_obu = false;
            }
            first_frame = false;
        }
        const packet = try output.toOwnedSlice();
        errdefer allocator.free(packet);
        try packets.append(packet);
        total += packet.len;
    }
    return .{ .allocator = allocator, .packets = try packets.toOwnedSlice() };
}

test "AV1 Annex B validates nesting, redundant sizes and delimiter placement" {
    const a = std.testing.allocator;
    // TU(FU(delimiter, sequence header)), retaining the extension header.
    var units = try normalize(a, &.{ 9, 8, 1, 0x10, 5, 0x0c, 0x20, 1, 2, 3 }, 64, 4, .{});
    defer units.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0x12, 0, 0x0e, 0x20, 3, 1, 2, 3 }, units.packets[0]);
    var redundant = try normalize(a, &.{ 4, 3, 2, 0x12, 0 }, 64, 4, .{});
    defer redundant.deinit();
}
test "AV1 Annex B malformed preflight and resource bounds" {
    const a = std.testing.allocator;
    const bad = [_][]const u8{
        &.{}, &.{128}, &.{ 5, 3, 1, 0x10 }, &.{ 3, 2, 2, 0x10 },
        &.{ 3, 2, 1, 0x08 }, // no delimiter
        &.{ 4, 3, 2, 0x12, 1 }, // inconsistent redundant size
        &.{ 4, 3, 2, 0x14, 1 }, // extension reserved bits
        &.{ 5, 4, 1, 0x10, 1, 0x10 }, // repeated delimiter
        &.{ 3, 2, 1, 0x90 }, // forbidden bit
        &.{ 128, 128, 128, 128, 128, 128, 128, 128, 0 },
    };
    for (bad) |bytes| try std.testing.expectError(error.InvalidVideoData, normalize(a, bytes, 64, 8, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, normalize(a, &.{ 3, 2, 1, 0x10 }, 1, 8, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, normalize(a, &.{ 3, 2, 1, 0x10 }, 64, 0, .{}));
}
