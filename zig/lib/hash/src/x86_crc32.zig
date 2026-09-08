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

// Copyright 2017 The Chromium Authors
// SPDX-License-Identifier: Apache-2.0 AND BSD-3-Clause
// See ../LICENSE.chromium for the upstream license.
//
// The folding/reduction algorithm and constants are adapted from Chromium's
// third_party/zlib/crc32_simd.c (crc32_sse42_simd_), based on Intel's
// "Fast CRC Computation for Generic Polynomials Using PCLMULQDQ Instruction".
// This port needs only baseline SSE2 plus PCLMUL, not SSE4.1/4.2 or AVX.
const std = @import("std");
const V = @Vector(2, u64);
const fold512: V = .{ 0x0154442bd4, 0x01c6e41596 };
const fold128: V = .{ 0x01751997d0, 0x00ccaa009e };
const fold96: V = .{ 0x0163cd6124, 0 };
const polynomial: V = .{ 0x01db710641, 0x01f7011641 };
const low32: V = .{ 0xffffffff, 0xffffffff };

/// Raw IEEE state update for at least 64 bytes, in multiples of 16.
/// The caller handles short buffers/tails and checks CPU support.
pub noinline fn update(initial: u32, bytes: []const u8) u32 {
    std.debug.assert(bytes.len >= 64 and bytes.len % 16 == 0);
    var lanes = [4]V{ load(bytes[0..16]), load(bytes[16..32]), load(bytes[32..48]), load(bytes[48..64]) };
    lanes[0] ^= V{ initial, 0 };
    var remaining = bytes[64..];
    while (remaining.len >= 64) {
        inline for (0..4) |i| lanes[i] = fold(lanes[i], fold512) ^ load(remaining[i * 16 ..][0..16]);
        remaining = remaining[64..];
    }
    var state = lanes[0];
    inline for (1..4) |i| state = fold(state, fold128) ^ lanes[i];
    while (remaining.len >= 16) {
        state = fold(state, fold128) ^ load(remaining[0..16]);
        remaining = remaining[16..];
    }
    // Reduce 128 -> 96 -> 64 bits, then Barrett-reduce to the raw CRC32.
    state = shiftRight(state, 64) ^ clmul(0x10, state, fold128);
    state = shiftRight(state, 32) ^ clmul(0x00, state & low32, fold96);
    var quotient = clmul(0x10, state & low32, polynomial) & low32;
    quotient = clmul(0x00, quotient, polynomial);
    return @truncate(@as(u128, @bitCast(state ^ quotient)) >> 32);
}

inline fn load(bytes: *const [16]u8) V {
    return @bitCast(bytes.*);
}

inline fn shiftRight(value: V, comptime bits: u7) V {
    return @bitCast(@as(u128, @bitCast(value)) >> bits);
}

inline fn fold(value: V, factors: V) V {
    return clmul(0x00, value, factors) ^ clmul(0x11, value, factors);
}

inline fn clmul(comptime selector: u8, lhs: V, rhs: V) V {
    return asm (std.fmt.comptimePrint("pclmulqdq ${d}, %[rhs], %[out]", .{selector})
        : [out] "=x" (-> V),
        : [_] "0" (lhs),
          [rhs] "x" (rhs),
    );
}
