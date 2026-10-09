// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const A = std.mem.Allocator;

/// Public names may contain punctuation. Length framing keeps independent
/// index/materialization pairs distinct without inventing naming restrictions.
pub fn materialization(a: A, index: []const u8, name: []const u8) ![]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-lake-materialization-v2");
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, index.len, .little);
    hash.update(&length);
    hash.update(index);
    std.mem.writeInt(u64, &length, name.len, .little);
    hash.update(&length);
    hash.update(name);
    return std.fmt.allocPrint(a, "sql-aggregate:{s}", .{std.fmt.bytesToHex(hash.finalResult(), .lower)});
}
pub fn matches(a: A, actual: []const u8, index: []const u8, name: []const u8, version: u16) !bool {
    const framed = try materialization(a, index, name);
    defer a.free(framed);
    if (std.mem.eql(u8, framed, actual)) return true;
    if (version != 1) return false;
    const legacy = try std.fmt.allocPrint(a, "{s}.{s}", .{ index, name });
    defer a.free(legacy);
    return std.mem.eql(u8, legacy, actual);
}

test "external lake materialization identities frame punctuation and recognize legacy roots" {
    const a = std.testing.allocator;
    const first = try materialization(a, "a.b", "c");
    defer a.free(first);
    const second = try materialization(a, "a", "b.c");
    defer a.free(second);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expect(try matches(a, first, "a.b", "c", 2));
    try std.testing.expect(try matches(a, "a.b.c", "a.b", "c", 1));
    try std.testing.expect(!try matches(a, "a.b.c", "a.b", "c", 2));
    var long_index: [128]u8 = undefined;
    var long_name: [128]u8 = undefined;
    @memset(&long_index, 'x');
    @memset(&long_name, 'y');
    const long = try materialization(a, &long_index, &long_name);
    defer a.free(long);
    try std.testing.expect(long.len <= 128);
}
