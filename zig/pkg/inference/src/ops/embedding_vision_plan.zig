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
pub const minimum_tile: usize = 512;
pub const preferred_tile: usize = 2048;
/// Avoid widening a fast regular GEMM for a tiny remainder (e.g. 513 rows).
/// Smaller logical views preserve this policy when rebinding a larger arena.
pub fn tileRows(rows: usize, capacity_rows: usize) usize {
    const limit = @min(rows, capacity_rows);
    return if (limit < minimum_tile) limit else std.math.floorPowerOfTwo(usize, limit);
}
/// Keep the original capacity contract: a wider FFN tile is optional residency,
/// never a reason to reject a cohort that fits with the minimum tile.
pub fn arenaBytes(rows: usize, hidden: usize, intermediate: usize) !usize {
    return arenaBytesForTile(rows, hidden, intermediate, @min(rows, minimum_tile));
}
pub fn arenaBytesForTile(rows: usize, hidden: usize, intermediate: usize, tile: usize) !usize {
    if (rows == 0 or rows > max_rows or hidden != 768 or intermediate != 3072) return error.InvalidVisionPlan;
    if (tile == 0 or tile > rows or tile > preferred_tile) return error.InvalidVisionPlan;
    const floats = try std.math.add(usize, try std.math.mul(usize, rows, 11 * hidden), try std.math.mul(usize, tile, 3 * intermediate));
    const bytes = std.mem.alignForward(usize, try std.math.mul(usize, floats, 4), 4096);
    if (bytes > arena_limit) return error.InvalidVisionPlan;
    return bytes;
}
/// Wider GEMMs amortize the two FFN projections. Bound their scratch by the
/// existing arena ceiling; actual shared-cache admission may shrink it again.
pub fn tileCapacity(rows: usize, hidden: usize, intermediate: usize) !usize {
    _ = try arenaBytes(rows, hidden, intermediate);
    var tile = tileRows(rows, preferred_tile);
    while (true) {
        if (arenaBytesForTile(rows, hidden, intermediate, tile)) |_| return tile else |_| {}
        tile = @max(@min(rows, minimum_tile), tile / 2);
    }
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

test "embeddinggemma2 video FFN tiles widen without reducing cohort capacity" {
    for (1..max_rows + 1) |rows| {
        const minimum = arenaBytes(rows, 768, 3072) catch {
            try std.testing.expectError(error.InvalidVisionPlan, tileCapacity(rows, 768, 3072));
            continue;
        };
        const tile = try tileCapacity(rows, 768, 3072);
        try std.testing.expect(tile >= @min(rows, minimum_tile));
        try std.testing.expect(tile <= @min(rows, preferred_tile));
        const wide = try arenaBytesForTile(rows, 768, 3072, tile);
        try std.testing.expect(wide >= minimum and wide <= arena_limit);
    }
    try std.testing.expectEqual(@as(usize, 2048), try tileCapacity(4096, 768, 3072));
    try std.testing.expectEqual(@as(usize, 512), try tileCapacity(513, 768, 3072));
    try std.testing.expectEqual(@as(usize, 512), tileRows(513, preferred_tile));
    try std.testing.expect((try tileCapacity(7300, 768, 3072)) < preferred_tile);
}
