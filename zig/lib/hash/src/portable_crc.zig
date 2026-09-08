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

/// Reflected CRC update with raw (uncomplemented) state. Eight tables allow
/// independent lookups for each byte of a word, with no alignment requirement.
pub fn update(comptime T: type, comptime polynomial: T, initial: T, bytes: []const u8) T {
    const tables = comptime slicingTables(T, polynomial);
    var crc = initial;
    var remaining = bytes;
    while (remaining.len >= 8) {
        const word = std.mem.readInt(u64, remaining[0..8], .little) ^ @as(u64, crc);
        crc = 0;
        inline for (0..8) |i| crc ^= tables[7 - i][@as(u8, @truncate(word >> (8 * i)))];
        remaining = remaining[8..];
    }
    for (remaining) |byte| crc = tables[0][@as(u8, @truncate(crc ^ byte))] ^ (crc >> 8);
    return crc;
}

fn slicingTables(comptime T: type, comptime polynomial: T) [8][256]T {
    @setEvalBranchQuota(30000);
    var tables: [8][256]T = undefined;
    for (0..256) |i| {
        var crc: T = @intCast(i);
        for (0..8) |_| crc = if (crc & 1 != 0) (crc >> 1) ^ polynomial else crc >> 1;
        tables[0][i] = crc;
    }
    for (1..8) |table_index| {
        for (0..256) |i| {
            const previous = tables[table_index - 1][i];
            tables[table_index][i] = (previous >> 8) ^ tables[0][@as(u8, @truncate(previous))];
        }
    }
    return tables;
}
