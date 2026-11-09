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

//! Reserved logical metadata keys shared by admission and byte accounting.
const std = @import("std");

pub fn isDictionaryRegistry(key: []const u8) bool {
    var position: usize = 0;
    for (0..3) |component| {
        const start = position;
        var length: usize = 0;
        while (position < key.len and key[position] >= '0' and key[position] <= '9') : (position += 1) {
            length = std.math.add(usize, std.math.mul(usize, length, 10) catch return false, key[position] - '0') catch return false;
        }
        if (position == start or position == key.len or key[position] != ':' or
            (position - start > 1 and key[start] == '0')) return false;
        position += 1;
        if (length > key.len - position or (length == 0 and component != 1)) return false;
        const payload = key[position..][0..length];
        if (component == 0 and !std.mem.eql(u8, payload, "dictionary_registry:v1")) return false;
        position += length;
        if (component == 2) return position == key.len;
        if (position == key.len or key[position] != '|') return false;
        position += 1;
    }
    unreachable;
}

/// Native metadata participants never receive authored row authority. Older
/// dictionary registries use canonical ASCII tuples rather than a NUL family.
pub fn isScopedMetadata(key: []const u8) bool {
    return (key.len >= 2 and key[0] == 0 and key[1] == 0) or isDictionaryRegistry(key);
}

test "control registry keys require the complete canonical metadata family" {
    const key = "22:dictionary_registry:v1|4:text|8:identity";
    try std.testing.expect(isDictionaryRegistry(key));
    try std.testing.expect(isScopedMetadata(key));
    try std.testing.expect(isDictionaryRegistry("22:dictionary_registry:v1|0:|8:identity"));
    for (0..key.len) |end| try std.testing.expect(!isDictionaryRegistry(key[0..end]));
    for ([_][]const u8{
        "document:22:dictionary_registry:v1|4:text|8:identity",
        "22:dictionary_registry:v1|4:text|8:identity|1:x",
        "022:dictionary_registry:v1|4:text|8:identity",
        "999999999999999999999999999999999999:dictionary_registry:v1|4:text|8:identity",
        "22:dictionary_registry:v1|4:text|0:",
        "\x00\x01row:22:dictionary_registry:v1|4:text|8:identity",
    }) |invalid| try std.testing.expect(!isDictionaryRegistry(invalid));
    try std.testing.expect(isScopedMetadata("\x00\x00__metadata__:text"));
    try std.testing.expect(!isScopedMetadata("authored-document"));
}
