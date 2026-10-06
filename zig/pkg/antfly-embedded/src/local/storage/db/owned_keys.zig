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

//! Owned key insertion with allocation-safe adoption.
const std = @import("std");
const Allocator = std.mem.Allocator;
/// Clone and adopt an ordered key, preserving duplicates. Caller owns the
/// existing list on failure; a newly cloned key is installed without allocation.
pub fn appendOwnedKey(alloc: Allocator, list: anytype, key: []const u8) !void {
    try list.ensureUnusedCapacity(alloc, 1);
    const owned = try alloc.dupe(u8, key);
    list.appendAssumeCapacity(owned);
}

pub fn appendUniqueOwnedKey(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8), key: []const u8) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, key)) return;
    }
    try appendOwnedKey(alloc, list, key);
}

test "owned unique keys release every allocation and preserve duplicate ownership" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            var keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (keys.items) |key| alloc.free(key);
                keys.deinit(alloc);
            }
            try appendUniqueOwnedKey(alloc, &keys, "target");
            try appendUniqueOwnedKey(alloc, &keys, "target");
            try std.testing.expectEqual(@as(usize, 1), keys.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "owned ordered keys release failed allocations and retain duplicates" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            var keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (keys.items) |key| alloc.free(key);
                keys.deinit(alloc);
            }
            try appendOwnedKey(alloc, &keys, "target");
            try appendOwnedKey(alloc, &keys, "target");
            try std.testing.expectEqual(@as(usize, 2), keys.items.len);
            try std.testing.expectEqualStrings("target", keys.items[0]);
            try std.testing.expectEqualStrings("target", keys.items[1]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
