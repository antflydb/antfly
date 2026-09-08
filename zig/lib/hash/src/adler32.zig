// Copyright 2026 Antfly, Inc.
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

//! Shared vectorized Adler32, originally in lib/image's PNG encoder.
const std = @import("std");

pub const Adler32 = struct {
    state: u32 = 1,

    pub fn init() Adler32 {
        return .{};
    }
    pub fn update(self: *Adler32, bytes: []const u8) void {
        self.state = updateState(self.state, bytes);
    }
    pub fn final(self: Adler32) u32 {
        return self.state;
    }
    pub fn hash(bytes: []const u8) u32 {
        var adler = init();
        adler.update(bytes);
        return adler.final();
    }
};

fn updateState(state: u32, bytes: []const u8) u32 {
    const base = 65521;
    const nmax = 5552;
    const weights: @Vector(16, u32) = .{ 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };

    var s1 = state & 0xffff;
    var s2 = state >> 16;
    var index: usize = 0;

    while (index < bytes.len) {
        const end = @min(index + nmax, bytes.len);
        while (index + 16 <= end) : (index += 16) {
            const byte_vec: @Vector(16, u8) = bytes[index..][0..16].*;
            const lanes: @Vector(16, u32) = @intCast(byte_vec);
            const sum = @reduce(.Add, lanes);
            const weighted_sum = @reduce(.Add, lanes * weights);
            s2 += 16 * s1 + weighted_sum;
            s1 += sum;
        }
        while (index < end) : (index += 1) {
            s1 += bytes[index];
            s2 += s1;
        }
        s1 %= base;
        s2 %= base;
    }

    return s1 | (s2 << 16);
}

test "Adler32 vector lanes tails and reduction bounds match standard" {
    try std.testing.expectEqual(@as(u32, 0x091e01de), Adler32.hash("123456789"));
    try std.testing.expectEqual(@as(u32, 1), Adler32.hash(""));
    try std.testing.expectEqual(@as(u32, 0x091e01de), comptime Adler32.hash("123456789"));
    var bytes: [65536 + 32]u8 = undefined;
    var random = std.Random.DefaultPrng.init(0xad1e32);
    random.random().bytes(&bytes);
    for (0..32) |offset| {
        for (0..65) |len| try check(bytes[offset..][0..len]);
        for ([_]usize{ 255, 256, 1023, 1024, 5551, 5552, 5553, 11103, 11104, 11105, 65536 }) |len| {
            try check(bytes[offset..][0..len]);
        }
    }
    // All-ones input maximizes both sums and exposes reduction overflow.
    @memset(&bytes, 0xff);
    try check(&bytes);
    var adler = Adler32.init();
    var oracle: std.hash.Adler32 = .{};
    for (0..64) |_| {
        adler.update(&bytes);
        oracle.update(&bytes);
    }
    try std.testing.expectEqual(oracle.adler, adler.final());
}

fn check(data: []const u8) !void {
    const expected = std.hash.Adler32.hash(data);
    try std.testing.expectEqual(expected, Adler32.hash(data));
    var adler = Adler32.init();
    const split = data.len / 3;
    adler.update(data[0..split]);
    adler.update("");
    adler.update(data[split..]);
    try std.testing.expectEqual(expected, adler.final());
}
