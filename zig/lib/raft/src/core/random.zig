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

pub const RandomSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        next_below: *const fn (ptr: *anyopaque, upper_exclusive: u32) u32,
    };

    pub fn nextBelow(self: RandomSource, upper_exclusive: u32) u32 {
        std.debug.assert(upper_exclusive > 0);
        return self.vtable.next_below(self.ptr, upper_exclusive);
    }
};

pub const SplitMix64 = struct {
    state: u64,

    pub fn init(seed: u64) SplitMix64 {
        return .{ .state = seed };
    }

    pub fn randomSource(self: *SplitMix64) RandomSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .next_below = nextBelowImpl,
            },
        };
    }

    pub fn nextBelow(self: *SplitMix64, upper_exclusive: u32) u32 {
        std.debug.assert(upper_exclusive > 0);

        self.state +%= 0x9e3779b97f4a7c15;
        var z = self.state;
        z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
        z ^= z >> 31;
        return @intCast(z % upper_exclusive);
    }

    fn nextBelowImpl(ptr: *anyopaque, upper_exclusive: u32) u32 {
        const self: *SplitMix64 = @ptrCast(@alignCast(ptr));
        return self.nextBelow(upper_exclusive);
    }
};
