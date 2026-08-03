// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub fn isValid(text: []const u8) bool {
    return algebraicParseIpCidr(text) != null or algebraicParseIPv4(text) != null;
}

const AlgebraicIpCidr = struct {
    network: [4]u8,
    prefix_len: u8,
};

fn algebraicParseIpCidr(text: []const u8) ?AlgebraicIpCidr {
    const slash_pos = std.mem.indexOfScalar(u8, text, '/') orelse return null;
    const ip = algebraicParseIPv4(text[0..slash_pos]) orelse return null;
    const prefix_len = std.fmt.parseInt(u8, text[slash_pos + 1 ..], 10) catch return null;
    if (prefix_len > 32) return null;
    const mask = algebraicIpMask(prefix_len);
    return .{
        .network = .{ ip[0] & mask[0], ip[1] & mask[1], ip[2] & mask[2], ip[3] & mask[3] },
        .prefix_len = prefix_len,
    };
}

fn algebraicParseIPv4(text: []const u8) ?[4]u8 {
    var parts = std.mem.splitScalar(u8, text, '.');
    var out: [4]u8 = undefined;
    var i: usize = 0;
    while (parts.next()) |part| {
        if (i >= 4 or part.len == 0) return null;
        out[i] = std.fmt.parseInt(u8, part, 10) catch return null;
        i += 1;
    }
    if (i != 4) return null;
    return out;
}

fn algebraicIpMask(prefix_len: u8) [4]u8 {
    var mask = [_]u8{ 0, 0, 0, 0 };
    var remaining = prefix_len;
    for (&mask) |*byte| {
        if (remaining >= 8) {
            byte.* = 0xff;
            remaining -= 8;
        } else if (remaining > 0) {
            byte.* = @as(u8, 0xff) << @intCast(8 - remaining);
            remaining = 0;
        }
    }
    return mask;
}

test "IPv4 and CIDR validation rejects malformed ranges" {
    try std.testing.expect(isValid("127.0.0.1"));
    try std.testing.expect(isValid("10.2.0.0/16"));
    try std.testing.expect(!isValid("10.2.0.0/33"));
    try std.testing.expect(!isValid("10.2.0/24"));
    try std.testing.expect(!isValid("10.2.0.999"));
}
