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

//! Exact atomic-file sibling recognition for writer-fenced startup inventory.
const std = @import("std");

pub fn target(name: []const u8) ?[]const u8 {
    if (std.mem.indexOfAny(u8, name, "/\\") != null) return null;
    const marker: usize, const nonce: []const u8 = if (std.mem.endsWith(u8, name, ".tmp")) blk: {
        const at = std.mem.lastIndexOf(u8, name, ".owned-") orelse return null;
        break :blk .{ at, name[at + 7 .. name.len - 4] };
    } else blk: {
        const at = std.mem.lastIndexOf(u8, name, ".tmp-") orelse return null;
        break :blk .{ at, name[at + 5 ..] };
    };
    if (marker == 0 or nonce.len == 0) return null;
    for (nonce) |byte| if (!std.ascii.isDigit(byte)) return null;
    _ = std.fmt.parseUnsigned(u64, nonce, 10) catch return null;
    return name[0..marker];
}

test "controlled physical sync atomic inventory recognizes only exact bounded numeric writer siblings" {
    for ([_][]const u8{ "1.tbl.tmp-42", "1.tbl.owned-42.tmp", "1.tbl.owned-18446744073709551615.tmp" }) |name|
        try std.testing.expectEqualStrings("1.tbl", target(name).?);
    for ([_][]const u8{ "", "1.tbl", "1.tbl.owned-.tmp", "1.tbl.owned-x.tmp", "1.tbl.owned-1.tmp-extra", "1.tbl.owned-18446744073709551616.tmp", "1.tbl.tmp-", "1.tbl.tmp-+42", "1.tbl.tmp-18446744073709551616", ".owned-1.tmp", ".tmp-1", "../1.tbl.owned-1.tmp", "dir/1.tbl.tmp-42", "dir\\1.tbl.owned-1.tmp" }) |name|
        try std.testing.expect(target(name) == null);
}
