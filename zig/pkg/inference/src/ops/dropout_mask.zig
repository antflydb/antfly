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

//! Counter-based inverted-dropout masks: element i keeps with probability
//! 1 - p, from splitmix64(seed ^ splitmix64(i)), so a backend can fill a mask
//! on the device without a host random stream or an upload. The Metal kernel
//! (`gliner_boundary_device` kind `dropout_mask`) uses the same arithmetic.
const std = @import("std");

pub fn mix(value: u64) u64 {
    var x = value +% 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    return x ^ (x >> 31);
}

/// Elements whose high 32 hash bits fall below this are dropped.
pub fn threshold(probability: f32) u32 {
    return @intFromFloat(@min(@as(f64, probability) * 4294967296.0, 4294967295.0));
}

pub fn fill(mask: []f32, probability: f32, seed: u64) !void {
    if (!(probability >= 0 and probability < 1)) return error.InvalidDropoutProbability;
    const limit = threshold(probability);
    const scale = 1 / (1 - probability);
    for (mask, 0..) |*value, i| {
        const bits: u32 = @intCast(mix(seed ^ mix(i)) >> 32);
        value.* = if (bits < limit) 0 else scale;
    }
}

test "dropout masks are deterministic and keep about 1 - p" {
    var a: [4096]f32 = undefined;
    var b: [4096]f32 = undefined;
    try fill(&a, 0.1, 42);
    try fill(&b, 0.1, 42);
    try std.testing.expectEqualSlices(f32, &a, &b);
    var dropped: usize = 0;
    for (a) |v| {
        if (v == 0) dropped += 1 else try std.testing.expectApproxEqAbs(@as(f32, 1.0 / 0.9), v, 1e-6);
    }
    try std.testing.expect(dropped > 300 and dropped < 520);
    try fill(&b, 0.1, 43);
    try std.testing.expect(!std.mem.eql(f32, &a, &b));
    try fill(&a, 0, 7);
    for (a) |v| try std.testing.expectEqual(@as(f32, 1), v);
}
