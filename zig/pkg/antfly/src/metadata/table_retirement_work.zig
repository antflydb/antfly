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

//! Sparse owning-retirement discovery. Queue entries are hints, not permission
//! to execute an owner effect. Each quantum rechecks the current native head.
const std = @import("std");
const root = @import("table_retirement_ledger.zig");
const locks = @import("table_lifecycle_lock.zig");
pub const max_locks = 16;
pub const Page = struct { next: u64 = 0, lookup: ?root.Lookup = null };
pub fn read(txn: anytype, group: u64, after: u64) !Page {
    var prefix_buffer: [192]u8 = undefined;
    var key_buffer: [192]u8 = undefined;
    const prefix = try locks.prefix(&prefix_buffer, group);
    const start = if (after == 0) prefix else try locks.key(&key_buffer, group, after);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(start);
    if (after != 0) if (row) |value| if (std.mem.eql(u8, value.key, start)) {
        row = try cursor.next();
    };
    var visited: usize = 0;
    var last = after;
    while (row) |value| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, value.key, prefix)) break;
        if (visited == max_locks) return .{ .next = last };
        visited += 1;
        const table = (try locks.tableFromKey(value.key)) orelse return error.InvalidCatalogRecord;
        const lock = try locks.Lock.decode(value.value);
        last = table;
        if (lock.kind != .table_retirement) continue;
        const member = (try root.member(txn, group, table)) orelse return error.InvalidCatalogRecord;
        if (member.root.table != table) continue;
        const head = (try root.head(txn, member.root)) orelse return error.InvalidCatalogRecord;
        if (!head.active() or !std.mem.eql(u8, &lock.id, &head.lookup.id)) return error.InvalidCatalogRecord;
        // Common schema recovery owns all work after survivor admission.
        if (head.phase == .validating_survivors) continue;
        return .{ .next = table, .lookup = head.lookup };
    }
    return .{};
}

pub const PublicationProjection = struct {
    lookup: root.Lookup,
    revision: u64,
    tables: []const @import("antfly_local_sources").common_topology_records.TableRecord = &.{},
    removed_table: u64,
    removed_ranges: []const u64,
    logical_revision: ?u64,
    pub fn requireFor(self: @This(), head: root.Head) !void {
        try head.validate();
        if (!std.meta.eql(self.lookup, head.lookup) or head.phase != .published or head.direct_catalog_revision == 0 or
            self.revision == 0 or self.tables.len != 0 or self.removed_table != head.lookup.table or
            self.logical_revision == null or self.logical_revision.? < head.direct_catalog_revision or
            self.removed_ranges.len != head.physical.?.summary.expected[0]) return error.InvalidCatalogRecord;
        for (self.removed_ranges, 0..) |group, i| if (group == 0 or (i != 0 and self.removed_ranges[i - 1] >= group)) return error.InvalidCatalogRecord;
    }
};
