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

const std = @import("std");

pub fn bf16ValueFitsF16(bits: u16) bool {
    const magnitude = bits & 0x7fff;
    if (magnitude == 0) return true;
    const exponent = magnitude >> 7;
    if (exponent > 142 or exponent < 103) return false;
    if (exponent >= 110) return true;
    const shift: u4 = @intCast(110 - exponent);
    return magnitude & ((@as(u16, 1) << shift) - 1) == 0;
}

pub fn bf16TensorFitsF16(bytes: []const u8) bool {
    if (bytes.len % 2 != 0) return false;
    var offset: usize = 0;
    while (offset < bytes.len) : (offset += 2) {
        if (!bf16ValueFitsF16(std.mem.readInt(u16, bytes[offset..][0..2], .little))) return false;
    }
    return true;
}

test "projector BF16 eligibility matches every finite binary16 roundtrip" {
    for (0..65536) |raw| {
        const bits: u16 = @intCast(raw);
        const original: f32 = @bitCast(@as(u32, bits) << 16);
        const narrowed: f16 = @floatCast(original);
        const restored: f32 = @floatCast(narrowed);
        const exact = std.math.isFinite(original) and std.math.isFinite(restored) and original == restored;
        try std.testing.expectEqual(exact, bf16ValueFitsF16(bits));
    }
    try std.testing.expect(!bf16TensorFitsF16(&.{0}));
    try std.testing.expect(bf16TensorFitsF16(&.{ 0x80, 0x3f, 0, 0x80 }));
}
