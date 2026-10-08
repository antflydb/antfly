// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Narrow AVC input qualification before handing untrusted packets to the OS.
const std = @import("std");
/// The initial VideoToolbox lane qualifies 8-bit 4:2:0 Baseline/Main/High.
/// The platform validates full SPS syntax and geometry before session creation.
pub fn validateConfig(config: []const u8) !void {
    if (config.len < 7 or config[0] != 1) return error.MalformedVideoConfig;
    if (config[1] != 66 and config[1] != 77 and config[1] != 100) return error.UnsupportedVideoProfile;
    const n = config[5] & 31;
    if (n == 0) return error.MalformedVideoConfig;
    var cursor: usize = 6;
    for (0..n) |_| {
        const sps = try readSet(config, &cursor);
        if (sps.len < 4 or sps[0] & 31 != 7 or sps[1] != config[1]) return error.MalformedVideoConfig;
        if (config[1] == 100) {
            var bits = Bits{ .bytes = sps[4..] };
            _ = try bits.ue(); // seq_parameter_set_id
            if (try bits.ue() != 1) return error.UnsupportedVideoProfile;
            if (try bits.ue() != 0 or try bits.ue() != 0) return error.UnsupportedVideoProfile;
        }
    }
    if (cursor >= config.len) return error.MalformedVideoConfig;
    const pps_count = config[cursor];
    cursor += 1;
    if (pps_count == 0) return error.MalformedVideoConfig;
    for (0..pps_count) |_| {
        const pps = try readSet(config, &cursor);
        if (pps[0] & 31 != 8) return error.MalformedVideoConfig;
    }
    if (cursor < config.len) {
        if (config[1] != 100 or config.len - cursor < 4) return error.MalformedVideoConfig;
        if (config[cursor] & 3 != 1 or config[cursor + 1] & 7 != 0 or config[cursor + 2] & 7 != 0) return error.UnsupportedVideoProfile;
        const extensions = config[cursor + 3];
        cursor += 4;
        for (0..extensions) |_| _ = try readSet(config, &cursor);
    }
    if (cursor != config.len) return error.MalformedVideoConfig;
}
fn readSet(config: []const u8, cursor: *usize) ![]const u8 {
    if (config.len - cursor.* < 2) return error.MalformedVideoConfig;
    const size = std.mem.readInt(u16, config[cursor.*..][0..2], .big);
    cursor.* += 2;
    if (size == 0 or size > config.len - cursor.*) return error.MalformedVideoConfig;
    const set = config[cursor.*..][0..size];
    cursor.* += size;
    return set;
}
const Bits = struct {
    bytes: []const u8,
    cursor: usize = 0,
    byte: u8 = 0,
    remaining: u4 = 0,
    zeros: u2 = 0,
    fn bit(self: *Bits) !u1 {
        if (self.remaining == 0) {
            if (self.cursor >= self.bytes.len) return error.MalformedVideoConfig;
            self.byte = self.bytes[self.cursor];
            self.cursor += 1;
            if (self.zeros == 2 and self.byte == 3) {
                if (self.cursor >= self.bytes.len or self.bytes[self.cursor] > 3) return error.MalformedVideoConfig;
                self.byte = self.bytes[self.cursor];
                self.cursor += 1;
                self.zeros = 0;
            }
            self.zeros = if (self.byte == 0) @min(self.zeros + 1, 2) else 0;
            self.remaining = 8;
        }
        self.remaining -= 1;
        return @intCast((self.byte >> @as(u3, @intCast(self.remaining))) & 1);
    }
    fn ue(self: *Bits) !u32 {
        var leading: u6 = 0;
        while (try self.bit() == 0) {
            leading += 1;
            if (leading > 31) return error.MalformedVideoConfig;
        }
        var result: u64 = 1;
        for (0..leading) |_| result = (result << 1) | try self.bit();
        return @intCast(result - 1);
    }
};
/// Static avc1 parameter sets only. Reject in-band changes before OS decoding,
/// rather than allowing a larger/different frame to bypass metadata admission.
pub fn validatePacket(bytes: []const u8, length_bytes: u3) !void {
    if (length_bytes != 1 and length_bytes != 2 and length_bytes != 4) return error.MalformedVideoPacket;
    var cursor: usize = 0;
    var count: usize = 0;
    while (cursor < bytes.len) {
        if (bytes.len - cursor < length_bytes) return error.MalformedVideoPacket;
        var size: u32 = 0;
        for (bytes[cursor..][0..length_bytes]) |byte| size = (size << 8) | byte;
        cursor += length_bytes;
        if (size == 0 or size > bytes.len - cursor or bytes[cursor] & 128 != 0) return error.MalformedVideoPacket;
        const kind = bytes[cursor] & 31;
        if (kind == 7 or kind == 8 or kind == 13 or kind == 15) return error.UnsupportedDynamicVideoConfig;
        if (kind == 0 or kind >= 16) return error.UnsupportedVideoNal;
        count += 1;
        if (count > 4096) return error.ResourceLimitExceeded;
        cursor += size;
    }
    if (count == 0) return error.MalformedVideoPacket;
}
test "AVC rejects malformed lengths and in-band parameter changes before decode" {
    try validatePacket(&.{ 0, 0, 0, 2, 0x65, 0 }, 4);
    try std.testing.expectError(error.MalformedVideoPacket, validatePacket(&.{ 0, 0, 0, 3, 0x65, 0 }, 4));
    try std.testing.expectError(error.UnsupportedDynamicVideoConfig, validatePacket(&.{ 0, 0, 0, 2, 0x67, 0 }, 4));
    try std.testing.expectError(error.MalformedVideoPacket, validatePacket(&.{}, 4));
    try std.testing.expectError(error.MalformedVideoConfig, validateConfig(&.{ 1, 100, 0 }));
    try std.testing.expectError(error.UnsupportedVideoProfile, validateConfig(&.{ 1, 110, 0, 0, 0, 0, 0 }));
}

/// An IDR picture resets prior reference dependencies. Ordinary I pictures,
/// recovery-point SEI and container sync flags alone do not establish that.
/// Full slice syntax is still validated by the actual decoder.
pub fn isIdr(bytes: []const u8, length_bytes: u3) !bool {
    try validatePacket(bytes, length_bytes);
    var cursor: usize = 0;
    var has_idr = false;
    while (cursor < bytes.len) {
        var size: usize = 0;
        for (bytes[cursor..][0..length_bytes]) |byte| size = (size << 8) | byte;
        cursor += length_bytes;
        const kind = bytes[cursor] & 31;
        if (kind >= 1 and kind <= 4) return false;
        if (kind == 5) has_idr = true;
        cursor += size;
    }
    return has_idr;
}
