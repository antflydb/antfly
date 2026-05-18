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

pub const GoPcg = struct {
    hi: u64,
    lo: u64,

    pub fn init(seed1: u64, seed2: u64) GoPcg {
        return .{
            .hi = seed1,
            .lo = seed2,
        };
    }

    pub fn uint64(self: *GoPcg) u64 {
        const mul_hi: u64 = 2549297995355413924;
        const mul_lo: u64 = 4865540595714422341;
        const inc_hi: u64 = 6364136223846793005;
        const inc_lo: u64 = 1442695040888963407;

        const state: u128 = (@as(u128, self.hi) << 64) | self.lo;
        const mul: u128 = (@as(u128, mul_hi) << 64) | mul_lo;
        const inc: u128 = (@as(u128, inc_hi) << 64) | inc_lo;
        const next_state = state *% mul +% inc;

        var hi: u64 = @truncate(next_state >> 64);
        const lo: u64 = @truncate(next_state);
        self.hi = hi;
        self.lo = lo;

        const cheap_mul: u64 = 0xda942042e4dd58b5;
        hi ^= hi >> 32;
        hi *%= cheap_mul;
        hi ^= hi >> 48;
        hi *%= (lo | 1);
        return hi;
    }

    pub fn uint32(self: *GoPcg) u32 {
        return @truncate(self.uint64() >> 32);
    }

    pub fn float32(self: *GoPcg) f32 {
        const v: u32 = (self.uint32() << 8) >> 8;
        return @as(f32, @floatFromInt(v)) / (1 << 24);
    }

    pub fn uint64n(self: *GoPcg, n: u64) u64 {
        if (n == 0) unreachable;
        if ((n & (n - 1)) == 0) return self.uint64() & (n - 1);

        var product = @as(u128, self.uint64()) * @as(u128, n);
        var hi: u64 = @truncate(product >> 64);
        var lo: u64 = @truncate(product);
        if (lo < n) {
            const thresh = (0 -% n) % n;
            while (lo < thresh) {
                product = @as(u128, self.uint64()) * @as(u128, n);
                hi = @truncate(product >> 64);
                lo = @truncate(product);
            }
        }
        return hi;
    }

    pub fn intN(self: *GoPcg, n: usize) usize {
        return @intCast(self.uint64n(@intCast(n)));
    }
};
