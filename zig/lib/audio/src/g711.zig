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

pub fn decodeMuLaw(encoded: u8) i16 {
    const sample = ~encoded;
    const exponent = (sample >> 4) & 0x07;
    const mantissa = sample & 0x0f;
    var magnitude = (@as(i32, mantissa) << 3) + 0x84;
    magnitude <<= @intCast(exponent);
    const decoded = if ((sample & 0x80) != 0) 0x84 - magnitude else magnitude - 0x84;
    return @intCast(decoded);
}

pub fn decodeALaw(encoded: u8) i16 {
    const sample = encoded ^ 0x55;
    const exponent = (sample >> 4) & 0x07;
    const mantissa = sample & 0x0f;
    const magnitude = if (exponent == 0)
        (@as(i32, mantissa) << 4) + 8
    else
        ((@as(i32, mantissa) << 4) + 0x108) << @intCast(exponent - 1);
    return @intCast(if ((sample & 0x80) != 0) magnitude else -magnitude);
}

test "g711 decoders expose symmetric extremes" {
    try std.testing.expectEqual(@as(i16, -32124), decodeMuLaw(0x00));
    try std.testing.expectEqual(@as(i16, 32124), decodeMuLaw(0x80));
    try std.testing.expectEqual(@as(i16, -32256), decodeALaw(0x2a));
    try std.testing.expectEqual(@as(i16, 32256), decodeALaw(0xaa));
}
