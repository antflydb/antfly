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

/// Standalone artifacts contain at most one native family. Reject repeated
/// owners rather than silently storing an incomplete include closure.
pub fn compact(allocator: std.mem.Allocator, source: []const u8, owners: []const NativeOwner) !?[]u8 {
    for (owners) |owner| {
        if (std.mem.indexOf(u8, source, owner.source)) |start| {
            if (std.mem.count(u8, source, owner.source) != 1) return error.RepeatedNativeSourceOwner;
            return try std.mem.concat(allocator, u8, &.{ source[0..start], "#include \"", owner.include_path, "\"\n", source[start + owner.source.len ..] });
        }
    }
    return null;
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
    for (owners) |owner| {
        const include = try std.mem.concat(allocator, u8, &.{ "#include \"", owner.include_path, "\"\n" });
        defer allocator.free(include);
        if (std.mem.indexOf(u8, source, include) != null) {
            if (std.mem.count(u8, source, include) != 1) return error.RepeatedNativeSourceOwner;
            return try std.mem.replaceOwned(u8, allocator, source, include, owner.source);
        }
    }
    return null;
}
