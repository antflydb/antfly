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

//! RBSP bit reader for HEVC/H.265 (ITU-T H.265 clause 7.2/9.2), written from
//! the specification. Emulation-prevention three-byte removal and the
//! Exp-Golomb/signed Exp-Golomb readers are the same mechanism H.264 uses
//! (clause 7.3.1.1/9.1 in both standards), but kept as an independent
//! implementation here to avoid coupling the two codec cores.
const std = @import("std");
const media = @import("antfly_media");

/// Owned de-escaped RBSP body (NAL unit header already stripped by the
/// caller). `end` excludes the rbsp_stop_one_bit and trailing padding.
pub const Bits = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    end: usize,
    position: usize = 0,

    pub fn init(allocator: std.mem.Allocator, rbsp: []const u8) !Bits {
        return initControlled(allocator, rbsp, .{});
    }

    /// Strips emulation_prevention_three_byte sequences (0x00 0x00 0x03 -> 0x00
    /// 0x00) per clause 7.3.1.1 and locates the rbsp_trailing_bits per 7.3.2.11.
    pub fn initControlled(allocator: std.mem.Allocator, rbsp: []const u8, control: media.source.Control) !Bits {
        try control.check();
        if (rbsp.len == 0) return error.MalformedVideoPacket;
        var output: std.ArrayList(u8) = .empty;
        errdefer output.deinit(allocator);
        try output.ensureTotalCapacityPrecise(allocator, rbsp.len);
        var zeros: usize = 0;
        var cursor: usize = 0;
        while (cursor < rbsp.len) : (cursor += 1) {
            if (cursor % 4096 == 0) try control.check();
            const byte = rbsp[cursor];
            if (zeros == 2 and byte == 3) {
                if (cursor + 1 == rbsp.len or rbsp[cursor + 1] > 3) return error.MalformedVideoPacket;
                zeros = 0;
                continue;
            }
            if (zeros == 2 and byte <= 2) return error.MalformedVideoPacket;
            output.appendAssumeCapacity(byte);
            zeros = if (byte == 0) zeros + 1 else 0;
        }
        var length = output.items.len;
        while (length > 0 and output.items[length - 1] == 0) length -= 1;
        if (length == 0) return error.MalformedVideoPacket;
        const trailing: usize = @ctz(output.items[length - 1]);
        if (trailing > 7) return error.MalformedVideoPacket;
        const end = length * 8 - trailing - 1;
        return .{ .allocator = allocator, .bytes = try output.toOwnedSlice(allocator), .end = end };
    }

    pub fn deinit(self: *Bits) void {
        self.allocator.free(self.bytes);
        self.* = undefined;
    }

    pub fn read(self: *Bits, count: usize) !u32 {
        if (count > 32 or count > self.end -| self.position) return error.MalformedVideoPacket;
        var result: u32 = 0;
        for (0..count) |_| {
            result = (result << 1) | ((self.bytes[self.position / 8] >> @as(u3, @intCast(7 - self.position % 8))) & 1);
            self.position += 1;
        }
        return result;
    }

    pub fn flag(self: *Bits) !bool {
        return try self.read(1) != 0;
    }

    /// ue(v) per clause 9.2.
    pub fn ue(self: *Bits) !u32 {
        var leading: usize = 0;
        while (try self.read(1) == 0) {
            leading += 1;
            if (leading > 31) return error.MalformedVideoPacket;
        }
        if (leading == 0) return 0;
        return (@as(u32, 1) << @as(u5, @intCast(leading))) - 1 + try self.read(leading);
    }

    /// se(v) per clause 9.2.2.
    pub fn se(self: *Bits) !i32 {
        const code = try self.ue();
        if (code > 0x7fff_ffff) return error.MalformedVideoPacket;
        return if (code & 1 != 0) @as(i32, @intCast((code + 1) / 2)) else -@as(i32, @intCast(code / 2));
    }

    /// ue(v) rejected outright (clause-level, not just representational) if
    /// it exceeds `max`, so a caller's subsequent narrowing `@intCast` can
    /// never panic on a malformed/adversarial value. `max` should still
    /// allow every value a conformant stream can legally signal.
    pub fn ueBounded(self: *Bits, max: u32) !u32 {
        const value = try self.ue();
        if (value > max) return error.MalformedVideoPacket;
        return value;
    }

    /// se(v) bounded to `[min, max]`; see `ueBounded`.
    pub fn seBounded(self: *Bits, min: i32, max: i32) !i32 {
        const value = try self.se();
        if (value < min or value > max) return error.MalformedVideoPacket;
        return value;
    }

    pub fn moreRbspData(self: *const Bits) bool {
        return self.position < self.end;
    }

    pub fn bitsLeft(self: *const Bits) usize {
        return self.end -| self.position;
    }

    pub fn byteAlign(self: *Bits) !void {
        _ = try self.read((8 - self.position % 8) % 8);
    }

    pub fn finish(self: *const Bits) !void {
        if (self.position != self.end) return error.MalformedVideoPacket;
    }
};

test "HEVC Bits strips emulation prevention and reads ue/se" {
    const a = std.testing.allocator;
    // RBSP: 0x00 0x00 0x03 0x01 (escaped 00 00 01) then ue(v)=1 ("010"),
    // se(v)=-1 (ue(2)="011"), rbsp_stop_one_bit, one padding zero bit: 0x4E.
    var bits = try Bits.init(a, &.{ 0x00, 0x00, 0x03, 0x01, 0x4E });
    defer bits.deinit();
    try std.testing.expectEqual(@as(u32, 0), try bits.read(8));
    try std.testing.expectEqual(@as(u32, 0), try bits.read(8));
    try std.testing.expectEqual(@as(u32, 1), try bits.read(8));
    try std.testing.expectEqual(@as(u32, 1), try bits.ue());
    try std.testing.expectEqual(@as(i32, -1), try bits.se());
    try bits.finish();
}

test "HEVC Bits rejects malformed emulation prevention and empty input" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.MalformedVideoPacket, Bits.init(a, &.{}));
    try std.testing.expectError(error.MalformedVideoPacket, Bits.init(a, &.{ 0x00, 0x00, 0x00 }));
    try std.testing.expectError(error.MalformedVideoPacket, Bits.init(a, &.{ 0x00, 0x00, 0x01, 0x80 }));
    var ok = try Bits.init(a, &.{ 0x00, 0x00, 0x04, 0x80 });
    ok.deinit();
    var bits = try Bits.init(a, &.{0x80});
    defer bits.deinit();
    try std.testing.expectError(error.MalformedVideoPacket, bits.read(1));
}
