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

//! An index's durable mutation identity is independent of its table's other
//! indexes. Identity records live outside the virtual filesystem namespace.
const std = @import("std");
const prefix = "\x00antfly/lite/index-scope/";
pub const key_capacity = prefix.len + 64;

pub fn root(path: []const u8) ?[]const u8 {
    const marker = "/indexes/";
    const start = (std.mem.indexOf(u8, path, marker) orelse return null) + marker.len;
    if (start == path.len) return null;
    const end = if (std.mem.indexOfScalar(u8, path[start..], '/')) |relative| start + relative else path.len;
    if (end == start) return null;
    return path[0..end];
}

pub fn key(buffer: *[key_capacity]u8, scope: []const u8) []const u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(scope, &hash, .{});
    @memcpy(buffer[0..prefix.len], prefix);
    const hex = "0123456789abcdef";
    for (hash, 0..) |byte, i| {
        buffer[prefix.len + 2 * i] = hex[byte >> 4];
        buffer[prefix.len + 2 * i + 1] = hex[byte & 15];
    }
    return buffer;
}

test "native index scopes isolate indexes and namespaces without recursive marker domains" {
    const a = "__antfly_lite/tables/first/indexes/text/main/wal/one";
    const b = "__antfly_lite/tables/first/indexes/graph/main/wal/one";
    const c = "__antfly_lite/tables/second/indexes/text/main/wal/one";
    try std.testing.expectEqualStrings("__antfly_lite/tables/first/indexes/text", root(a).?);
    var ka: [key_capacity]u8 = undefined;
    var kb: [key_capacity]u8 = undefined;
    var kc: [key_capacity]u8 = undefined;
    try std.testing.expect(!std.mem.eql(u8, key(&ka, root(a).?), key(&kb, root(b).?)));
    try std.testing.expect(!std.mem.eql(u8, key(&ka, root(a).?), key(&kc, root(c).?)));
    try std.testing.expect(root(key(&ka, root(a).?)) == null);
    try std.testing.expect(root("__antfly_lite/indexes/") == null);
}
