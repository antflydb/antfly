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

const std = @import("std");
const portable = @import("portable_crc.zig");

/// CRC-64/NVME, not CRC-64/ECMA or CRC-64/XZ. Its reflected polynomial is
/// 0x9a6c9329ac4bc9b5. Raw state and final XOR are both all ones.
pub const Crc64Nvme = struct {
    crc: u64 = 0xffffffffffffffff,

    pub fn init() Crc64Nvme {
        return .{};
    }

    pub fn update(self: *Crc64Nvme, bytes: []const u8) void {
        self.crc = portable.update(u64, 0x9a6c9329ac4bc9b5, self.crc, bytes);
    }

    pub fn final(self: Crc64Nvme) u64 {
        return self.crc ^ 0xffffffffffffffff;
    }

    pub fn hash(bytes: []const u8) u64 {
        var crc = init();
        crc.update(bytes);
        return crc.final();
    }
};

pub const Oracle = std.hash.crc.Crc(u64, .{
    .polynomial = 0xad93d23594c93659,
    .initial = 0xffffffffffffffff,
    .reflect_input = true,
    .reflect_output = true,
    .xor_output = 0xffffffffffffffff,
});

test "CRC64 NVME matches standard across alignment and incremental boundaries" {
    try std.testing.expectEqual(@as(u64, 0xae8b14860a799888), Crc64Nvme.hash("123456789"));
    try std.testing.expectEqual(@as(u64, 0), Crc64Nvme.hash(""));
    try std.testing.expectEqual(@as(u64, 0xae8b14860a799888), comptime Crc64Nvme.hash("123456789"));
    var bytes: [65536 + 32]u8 = undefined;
    var random = std.Random.DefaultPrng.init(0x64_593c32);
    random.random().bytes(&bytes);
    for (0..32) |offset| {
        for (0..513) |len| try check(bytes[offset..][0..len]);
        for ([_]usize{ 1023, 1024, 4095, 4096, 65535, 65536 }) |len| try check(bytes[offset..][0..len]);
    }
}

fn check(data: []const u8) !void {
    const expected = Oracle.hash(data);
    try std.testing.expectEqual(expected, Crc64Nvme.hash(data));
    var crc = Crc64Nvme.init();
    const split = data.len / 3;
    crc.update(data[0..split]);
    crc.update("");
    crc.update(data[split..]);
    try std.testing.expectEqual(expected, crc.final());
}
