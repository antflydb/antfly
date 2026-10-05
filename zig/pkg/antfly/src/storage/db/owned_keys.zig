// Copyright 2026 Antfly, Inc.
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

//! Owned key insertion with allocation-safe adoption.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub fn appendUniqueOwnedKey(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8), key: []const u8) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, key)) return;
    }
    try list.ensureUnusedCapacity(alloc, 1);
    const owned = try alloc.dupe(u8, key);
    list.appendAssumeCapacity(owned);
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
