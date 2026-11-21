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

/// Native numerical source has one canonical owner. JIT/identity consumers use
/// the complete source; checked-in AOT files may include that same owner.
pub const NativeOwner = struct {
    source: []const u8,
    include_path: []const u8,
};

/// Store each matching, disjoint native component exactly once. Translation
/// units can combine shared phases and a complete schedule. JIT payloads are
/// unchanged; expansion reverses storage against the same owner table.
pub fn compact(allocator: std.mem.Allocator, source: []const u8, owners: []const NativeOwner) !?[]u8 {
    return rewrite(allocator, source, owners, false);
}

fn rewrite(allocator: std.mem.Allocator, source: []const u8, owners: []const NativeOwner, comptime expanding: bool) !?[]u8 {
    var result: ?[]u8 = null;
    errdefer if (result) |data| allocator.free(data);
    for (owners) |owner| {
        const include = try std.mem.concat(allocator, u8, &.{ "#include \"", owner.include_path, "\"\n" });
        defer allocator.free(include);
        const current = result orelse source;
        const from = if (expanding) include else owner.source;
        const to = if (expanding) owner.source else include;
        const count = std.mem.count(u8, current, from);
        if (count == 0) continue;
        if (count != 1) return error.RepeatedNativeSourceOwner;
        const updated = try std.mem.replaceOwned(u8, allocator, current, from, to);
        if (result) |data| allocator.free(data);
        result = updated;
    }
    return result;
}

test "native source storage roundtrip retains the complete semantic payload" {
    const owner = NativeOwner{ .source = "template<class T> void numerical_body(T x) {}\n", .include_path = "../../../graph/templates/native.h" };
    const full = "// ABI prefix\n" ++ owner.source ++ "// geometry instantiation\n";
    const stored = (try compact(std.testing.allocator, full, &.{owner})).?;
    defer std.testing.allocator.free(stored);
    const restored = try std.mem.replaceOwned(u8, std.testing.allocator, stored, "#include \"../../../graph/templates/native.h\"\n", owner.source);
    defer std.testing.allocator.free(restored);
    try std.testing.expectEqualStrings(full, restored);
    try std.testing.expectError(error.RepeatedNativeSourceOwner, compact(std.testing.allocator, owner.source ++ owner.source, &.{owner}));
    try std.testing.expectEqual(null, try compact(std.testing.allocator, "// other artifact\n", &.{owner}));
}

/// Reconstruct the complete native source for offline validation. Production
/// JIT does not read stored files or depend on include resolution.
pub fn expand(allocator: std.mem.Allocator, source: []const u8, owners: []const NativeOwner) !?[]u8 {
    return rewrite(allocator, source, owners, true);
}

test "native source storage roundtrip preserves multiple components in either order" {
    const phases = NativeOwner{ .source = "#define shared_phase(x) ((x) + 1)\n", .include_path = "phases.h" };
    const schedule = NativeOwner{ .source = "void schedule() { shared_phase(3); }\n", .include_path = "schedule.h" };
    const full = "// ABI prefix\n" ++ phases.source ++ "// schedule boundary\n" ++ schedule.source ++ "// exports\n";
    for ([_][2]NativeOwner{ .{ phases, schedule }, .{ schedule, phases } }) |owners| {
        const stored = (try compact(std.testing.allocator, full, &owners)).?;
        defer std.testing.allocator.free(stored);
        try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, stored, "#include"));
        const restored = (try expand(std.testing.allocator, stored, &owners)).?;
        defer std.testing.allocator.free(restored);
        try std.testing.expectEqualStrings(full, restored);
        try std.testing.expectError(error.RepeatedNativeSourceOwner, compact(std.testing.allocator, full ++ schedule.source, &owners));
        const repeated = try std.mem.concat(std.testing.allocator, u8, &.{ stored, "#include \"schedule.h\"\n" });
        defer std.testing.allocator.free(repeated);
        try std.testing.expectError(error.RepeatedNativeSourceOwner, expand(std.testing.allocator, repeated, &owners));
    }
}
