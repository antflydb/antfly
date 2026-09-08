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

/// Call only after compile-time or runtime detection of ARM CRC support.
/// The directive enables assembly of the guarded instructions in a baseline
/// build; it does not let the compiler emit CRC elsewhere in that build.
pub noinline fn update(comptime castagnoli: bool, initial: u32, bytes: []const u8) u32 {
    var crc = initial;
    var remaining = bytes;
    while (remaining.len >= 8) {
        crc = instruction(castagnoli, u64, crc, std.mem.readInt(u64, remaining[0..8], .little));
        remaining = remaining[8..];
    }
    if (remaining.len >= 4) {
        crc = instruction(castagnoli, u32, crc, std.mem.readInt(u32, remaining[0..4], .little));
        remaining = remaining[4..];
    }
    if (remaining.len >= 2) {
        crc = instruction(castagnoli, u16, crc, std.mem.readInt(u16, remaining[0..2], .little));
        remaining = remaining[2..];
    }
    if (remaining.len != 0) crc = instruction(castagnoli, u8, crc, remaining[0]);
    return crc;
}

inline fn instruction(comptime castagnoli: bool, comptime T: type, crc: u32, value: T) u32 {
    const mnemonic = "crc32" ++ (if (castagnoli) "c" else "") ++ switch (T) {
        u64 => "x",
        u32 => "w",
        u16 => "h",
        u8 => "b",
        else => unreachable,
    };
    const operand = if (T == u64) "%[value]" else "%[value:w]";
    return asm (".arch_extension crc\n" ++ mnemonic ++ " %[out:w], %[crc:w], " ++ operand
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}
