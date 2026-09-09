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

/// SSE4.2's CRC32 instruction computes Castagnoli, not IEEE. Only called after
/// CPUID or compilation-target validation; all memory reads stay in the slice.
pub noinline fn update(initial: u32, bytes: []const u8) u32 {
    var crc: u64 = initial;
    var remaining = bytes;
    while (remaining.len >= 8) {
        crc = asm ("crc32q %[value], %[crc]"
            : [crc] "=r" (-> u64),
            : [_] "0" (crc),
              [value] "r" (std.mem.readInt(u64, remaining[0..8], .little)),
        );
        remaining = remaining[8..];
    }
    var tail: u32 = @truncate(crc);
    for (remaining) |byte| {
        tail = asm ("crc32b %[value], %[crc]"
            : [crc] "=r" (-> u32),
            : [_] "0" (tail),
              [value] "r" (byte),
        );
    }
    return tail;
}
