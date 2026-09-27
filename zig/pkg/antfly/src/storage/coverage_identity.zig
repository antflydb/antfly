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

/// Coverage incarnations cross the JSON metadata boundary as positive i64
/// values. Keep every internally generated identity in that same domain so a
/// catalog round trip cannot silently lose the incarnation.
pub const max_generation: u64 = std.math.maxInt(i64);

pub fn isValid(generation: u64) bool {
    return generation > 0 and generation <= max_generation;
}

pub fn fromBits(bits: u64) ?u64 {
    const generation = bits & max_generation;
    return if (generation == 0) null else generation;
}

pub fn fromHashBits(bits: u64) u64 {
    return fromBits(bits) orelse 1;
}

pub fn generate(io: std.Io) !u64 {
    while (true) {
        var bits: u64 = 0;
        try @import("antfly_platform").entropy.fill(io, std.mem.asBytes(&bits));
        if (fromBits(bits)) |generation| return generation;
    }
}

test "coverage identities stay in the positive JSON integer domain" {
    try std.testing.expect(fromBits(0) == null);
    try std.testing.expect(fromBits(@as(u64, 1) << 63) == null);
    try std.testing.expectEqual(max_generation, fromBits(std.math.maxInt(u64)).?);
    try std.testing.expectEqual(@as(u64, 1), fromHashBits(@as(u64, 1) << 63));
    try std.testing.expect(isValid(1));
    try std.testing.expect(isValid(max_generation));
    try std.testing.expect(!isValid(0));
    try std.testing.expect(!isValid(max_generation + 1));
}
