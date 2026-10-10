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

//! Owned, expected-linear folding of transaction postimages. Keys borrow the
//! input only during this call; output retains first-observation predicates
//! and first-seen order, with the latest non-predicate postimage per table/key.
const std = @import("std");
pub const max_entries = 4096;
const Key = struct { table: []const u8, row: []const u8 };
const Context = struct {
    pub fn hash(_: @This(), key: Key) u64 {
        var state = std.hash.Wyhash.init(0);
        std.hash.autoHash(&state, key.table.len);
        state.update(key.table);
        state.update(key.row);
        return state.final();
    }
    pub fn eql(_: @This(), lhs: Key, rhs: Key) bool {
        return std.mem.eql(u8, lhs.table, rhs.table) and std.mem.eql(u8, lhs.row, rhs.row);
    }
};

pub fn merge(comptime Entry: type, alloc: std.mem.Allocator, input: []const Entry) ![]Entry {
    if (input.len > max_entries) return error.SqlProgramLimitExceeded;
    const Table = @TypeOf(@as(Entry, undefined).table);
    var tables: std.StringHashMapUnmanaged(Table) = .empty;
    defer tables.deinit(alloc);
    var positions: std.HashMapUnmanaged(Key, usize, Context, std.hash_map.default_max_load_percentage) = .empty;
    defer positions.deinit(alloc);
    var result: std.ArrayList(Entry) = .empty;
    errdefer result.deinit(alloc);
    try result.ensureTotalCapacity(alloc, input.len);
    for (input) |entry| {
        const table = try tables.getOrPut(alloc, entry.table.physical_name);
        if (table.found_existing) {
            const prior = table.value_ptr.*;
            // Fence the entire physical table, not only repeated primary keys.
            if (prior.id != entry.table.id or prior.schema_version != entry.table.schema_version or
                prior.storage_mode != entry.table.storage_mode) return error.PreparedGenerationChanged;
        } else table.value_ptr.* = entry.table;
        const position = try positions.getOrPut(alloc, .{ .table = entry.table.physical_name, .row = entry.mutation.key });
        if (!position.found_existing) {
            position.value_ptr.* = result.items.len;
            result.appendAssumeCapacity(entry);
        } else if (!entry.mutation.predicate_only) {
            const prior = &result.items[position.value_ptr.*];
            prior.mutation.row = entry.mutation.row;
            prior.mutation.json_null_fields = entry.mutation.json_null_fields;
            prior.mutation.predicate_only = false;
        }
    }
    return result.toOwnedSlice(alloc);
}

const TestEntry = struct {
    table: struct { physical_name: []const u8, id: u64 = 1, schema_version: u32 = 1, storage_mode: enum { relational, document } = .relational },
    mutation: struct { key: []const u8, row: ?u64, json_null_fields: []const []const u8 = &.{}, predicate_only: bool = false, expected_version: u64 = 0, expected_content_digest: ?[32]u8 = null, unique_absence: bool = false },
};

test "mutation fold retains first predicates latest postimages tuple identity and input order" {
    const a = std.testing.allocator;
    const original: TestEntry = .{ .table = .{ .physical_name = "ab" }, .mutation = .{ .key = "c", .row = 1, .expected_version = 7, .expected_content_digest = @splat(3), .unique_absence = true } };
    var updated = original;
    updated.mutation = .{ .key = "c", .row = 2, .expected_version = 99, .json_null_fields = &.{"j"} };
    var predicate = updated;
    predicate.mutation.predicate_only = true;
    predicate.mutation.row = 99;
    const other: TestEntry = .{ .table = .{ .physical_name = "a", .id = 2 }, .mutation = .{ .key = "bc", .row = 3 } };
    const same_key: TestEntry = .{ .table = other.table, .mutation = .{ .key = "c", .row = 4 } };
    const output = try merge(TestEntry, a, &.{ original, other, updated, same_key, predicate });
    defer a.free(output);
    try std.testing.expectEqual(@as(usize, 3), output.len);
    try std.testing.expectEqual(@as(?u64, 2), output[0].mutation.row);
    try std.testing.expectEqual(@as(u64, 7), output[0].mutation.expected_version);
    try std.testing.expectEqual(original.mutation.expected_content_digest, output[0].mutation.expected_content_digest);
    try std.testing.expect(output[0].mutation.unique_absence);
    try std.testing.expectEqualStrings("j", output[0].mutation.json_null_fields[0]);
    try std.testing.expectEqualStrings("bc", output[1].mutation.key);
    try std.testing.expectEqual(@as(?u64, 4), output[2].mutation.row);
    try std.testing.expectEqual(@as(?u64, 1), original.mutation.row);
}

test "mutation fold fences table incarnation schema and storage across distinct primary keys" {
    const a = std.testing.allocator;
    const original: TestEntry = .{ .table = .{ .physical_name = "rows" }, .mutation = .{ .key = "first", .row = 1 } };
    for (0..3) |fault| {
        var changed = original;
        changed.mutation.key = "other";
        switch (fault) {
            0 => changed.table.id += 1,
            1 => changed.table.schema_version += 1,
            2 => changed.table.storage_mode = .document,
            else => unreachable,
        }
        try std.testing.expectError(error.PreparedGenerationChanged, merge(TestEntry, a, &.{ original, changed }));
    }
}

test "mutation fold handles predicate insert delete resurrection and releases every allocation fault" {
    const a = std.testing.allocator;
    const predicate: TestEntry = .{ .table = .{ .physical_name = "rows" }, .mutation = .{ .key = "id", .row = null, .predicate_only = true, .expected_version = 11 } };
    var insert = predicate;
    insert.mutation.predicate_only = false;
    insert.mutation.row = 1;
    var deleted = insert;
    deleted.mutation.row = null;
    var resurrected = insert;
    resurrected.mutation.row = 2;
    for ([_][]const TestEntry{ &.{ predicate, insert, deleted, predicate }, &.{ predicate, insert, deleted, resurrected } }, 0..) |input, i| {
        const result = try merge(TestEntry, a, input);
        defer a.free(result);
        try std.testing.expectEqual(@as(usize, 1), result.len);
        try std.testing.expectEqual(if (i == 0) @as(?u64, null) else @as(?u64, 2), result[0].mutation.row);
        try std.testing.expectEqual(@as(u64, 11), result[0].mutation.expected_version);
        try std.testing.expect(!result[0].mutation.predicate_only);
    }
    const Fault = struct {
        fn run(alloc: std.mem.Allocator, input: []const TestEntry) !void {
            const result = try merge(TestEntry, alloc, input);
            defer alloc.free(result);
            try std.testing.expectEqual(@as(usize, 1), result.len);
        }
    };
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fault.run, .{@as([]const TestEntry, &.{ predicate, insert, deleted, resurrected })});
    var excess: [max_entries + 1]TestEntry = undefined;
    try std.testing.expectError(error.SqlProgramLimitExceeded, merge(TestEntry, a, &excess));
}
