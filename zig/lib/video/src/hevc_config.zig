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

//! What an hvcC's SPS declares about color and pixel aspect. Containers can omit
//! both, and a decoder surface doesn't report them, so routes read them from here.
const std = @import("std");
const bits_mod = @import("hevc_bits.zig");
const nal = @import("hevc_nal.zig");
const params = @import("hevc_params.zig");

pub const Signal = struct {
    primaries: ?u8 = null,
    transfer: ?u8 = null,
    matrix: ?u8 = null,
    full_range: bool = false,
    sar: Aspect = .{},
};
pub const Aspect = struct { horizontal: u32 = 1, vertical: u32 = 1 };

fn specified(value: ?u8) ?u8 {
    const code = value orelse return null;
    return if (code == 2) null else code;
}

/// The first SPS in the record. Null when parameter sets are only in-band; a record
/// that declares an SPS this parser refuses is an error, not an SDR default.
pub fn parse(a: std.mem.Allocator, config: []const u8) !?Signal {
    const record = try nal.parseConfigurationRecord(config);
    var arrays = record.arrays();
    while (try arrays.next()) |array| {
        if (array.nal_unit_type != 33) continue;
        var nals = array.nals;
        const unit = (try nals.next()) orelse continue;
        const parsed = try nal.parseHeader(unit);
        var bits = try bits_mod.Bits.init(a, parsed.rbsp);
        defer bits.deinit();
        const sps = try params.parseSps(&bits);
        const vui = sps.vui;
        return .{
            .primaries = specified(vui.colour_primaries),
            .transfer = specified(vui.transfer_characteristics),
            .matrix = specified(vui.matrix_coefficients),
            .full_range = vui.video_full_range_flag,
            .sar = if (vui.sar_width != 0 and vui.sar_height != 0) .{ .horizontal = vui.sar_width, .vertical = vui.sar_height } else .{},
        };
    }
    return null;
}

/// The container's color declaration when present, which must agree with the stream.
pub fn fullRange(signal: ?Signal, color_info: []const u8) !bool {
    const nclx = color_info.len >= 11 and std.mem.eql(u8, color_info[0..4], "nclx");
    const declared = nclx and color_info[10] & 0x80 != 0;
    if (signal) |value| {
        if (nclx and declared != value.full_range) return error.UnsupportedVideoColor;
        return value.full_range;
    }
    return declared;
}

test "SPS signalling survives without any container color" {
    const a = std.testing.allocator;
    const fixture = @embedFile("../testdata/hevc-hlg-config.bin");
    const signal = (try parse(a, fixture)).?;
    try std.testing.expectEqual(@as(?u8, 9), signal.primaries);
    try std.testing.expectEqual(@as(?u8, 18), signal.transfer);
    try std.testing.expectEqual(@as(?u8, 9), signal.matrix);
    try std.testing.expect(!signal.full_range);
    try std.testing.expect(!try fullRange(signal, ""));
    try std.testing.expectError(error.UnsupportedVideoColor, fullRange(.{ .full_range = false }, "nclx" ++ [_]u8{ 0, 9, 0, 18, 0, 9, 0x80 }));
    try std.testing.expect(try fullRange(null, "nclx" ++ [_]u8{ 0, 9, 0, 18, 0, 9, 0x80 }));
}

test "A record without parameter sets carries no signal" {
    var config = @as([23]u8, @splat(0));
    config[0] = 1;
    config[21] = 3;
    try std.testing.expect((try parse(std.testing.allocator, &config)) == null);
}
