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

const std = @import("std");
const Allocator = std.mem.Allocator;

fn groupCreatedAtMetadataKeyAlloc(alloc: Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_created_at:{d}", .{group_id});
}
pub fn getGroupCreatedAtMillis(self: anytype, alloc: Allocator, group_id: u64) !?u64 {
    const key = try groupCreatedAtMetadataKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    // Group metadata is trusted server state, not a user-row projection.
    // Reading it borrows the stable core and does not mutate the DB wrapper
    // or enter user row-policy/TTL admission during a status observation.
    const raw = try self.core.getStoreValue(alloc, key) orelse return null;
    defer alloc.free(raw);
    return try std.fmt.parseInt(u64, raw, 10);
}

pub fn ensureGroupCreatedAtMillis(self: anytype, alloc: Allocator, group_id: u64, now_ms: u64) !u64 {
    if (try getGroupCreatedAtMillis(self, alloc, group_id)) |created_at_millis| return created_at_millis;

    const key = try groupCreatedAtMetadataKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    const encoded = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
    defer alloc.free(encoded);

    try self.batch(.{
        .writes = &.{
            .{
                .key = key,
                .value = encoded,
            },
        },
    });
    return now_ms;
}

test "server group creation timestamp preserves its key and the first recorded value" {
    const db = @import("antfly_local_sources").storage_db_db;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("group-metadata");
    defer directory.cleanup();
    var owner = try db.DB.open(std.testing.allocator, directory.path(), .{ .start_index_workers = false });
    defer owner.close();
    try std.testing.expectEqual(@as(u64, 1234), try ensureGroupCreatedAtMillis(&owner, std.testing.allocator, 7, 1234));
    try std.testing.expectEqual(@as(u64, 1234), try ensureGroupCreatedAtMillis(&owner, std.testing.allocator, 7, 5678));
    const borrowed: *const db.DB = &owner;
    try std.testing.expectEqual(@as(?u64, 1234), try getGroupCreatedAtMillis(borrowed, std.testing.allocator, 7));
    const raw = (try owner.get(std.testing.allocator, "\x00\x00__metadata__:data_group_created_at:7")).?;
    defer std.testing.allocator.free(raw);
    try std.testing.expectEqualStrings("1234", raw);
}
