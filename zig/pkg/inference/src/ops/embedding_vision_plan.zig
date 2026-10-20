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
pub const max_rows: usize = 8192;
pub const arena_limit: usize = 256 * 1024 * 1024;
pub fn arenaBytes(rows: usize, hidden: usize, intermediate: usize) !usize {
    if (rows == 0 or rows > max_rows or hidden != 768 or intermediate != 3072) return error.InvalidVisionPlan;
    const floats = try std.math.add(usize, try std.math.mul(usize, rows, 11 * hidden), try std.math.mul(usize, @min(rows, 512), 3 * intermediate));
    const bytes = std.mem.alignForward(usize, try std.math.mul(usize, floats, 4), 4096);
    if (bytes > arena_limit) return error.InvalidVisionPlan;
    return bytes;
}
pub fn capacity(patches: usize, requested: usize) usize {
    var count = @min(requested, 8);
    while (count > 0) : (count -= 1) {
        const rows = std.math.mul(usize, patches, count) catch continue;
        _ = arenaBytes(rows, 768, 3072) catch continue;
        return count;
    }
    return 0;
}
/// Arena/immutable cache residency has its own lease. This independent bound
/// covers overlapping layer outputs, positions, patch/projector staging and
/// Flash attention. A capability decline requires separately admitted fallback.
pub fn transientBytes(rows: usize) !usize {
    _ = try arenaBytes(rows, 768, 3072);
    return try std.math.add(usize, 64 * 1024 * 1024, try std.math.mul(usize, rows, 16 * 768 * 4));
}
test "embeddinggemma2 vision plan keeps full-size cohorts within prepared arena and row ceilings" {
    try std.testing.expectEqual(@as(usize, 3), capacity(2304, 4));
    try std.testing.expectEqual(@as(usize, 2), capacity(2520, 8)); // 2*2520 fits; 3 does not.
    for ([_]usize{ 9, 900, 1260, 2304, 2520 }) |patches| {
        const count = capacity(patches, 8);
        try std.testing.expect(count > 0);
        try std.testing.expect(try arenaBytes(count * patches, 768, 3072) <= arena_limit);
        if (count < 8) try std.testing.expectError(error.InvalidVisionPlan, arenaBytes((count + 1) * patches, 768, 3072));
    }
}
