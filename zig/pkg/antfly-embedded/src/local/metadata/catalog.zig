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

pub const std = @import("std");

pub const topology_records = @import("../common/topology_records.zig");
pub const TableRecord = topology_records.TableRecord;

// TableDefinition is the preferred product/control-plane name. TableRecord
// remains as the current storage/runtime name during the migration.
pub fn sortKeyspaceRanges(comptime Range: type, ranges: []Range) void {
    std.mem.sort(Range, ranges, {}, struct {
        pub fn lessThan(_: void, lhs: Range, rhs: Range) bool {
            return std.mem.order(u8, lhs.start_key, rhs.start_key) == .lt;
        }
    }.lessThan);
}

/// Validate a sorted, gap-free, non-overlapping partition of the complete
/// byte-string keyspace. The empty start and open final end are routing
/// sentinels, not optional decoration: omitting either would publish a table
/// for which some document keys have no owner.
pub fn validateCompleteKeyspaceRanges(ranges: anytype) !void {
    if (ranges.len == 0 or ranges[0].start_key.len != 0 or
        ranges[ranges.len - 1].end_key != null)
        return error.InvalidRangeTopology;

    for (ranges, 0..) |range, index| {
        if (range.end_key) |end_key| {
            if (end_key.len == 0 or std.mem.order(u8, range.start_key, end_key) != .lt)
                return error.InvalidRangeTopology;
        } else if (index != ranges.len - 1) {
            return error.InvalidRangeTopology;
        }
        if (index > 0) {
            const previous_end = ranges[index - 1].end_key orelse
                return error.InvalidRangeTopology;
            if (!std.mem.eql(u8, previous_end, range.start_key))
                return error.InvalidRangeTopology;
        }
    }
}

pub fn cloneTable(alloc: std.mem.Allocator, record: TableRecord) !TableRecord {
    const relational_retirement_json = try alloc.dupe(u8, record.relational_retirement_json);
    errdefer alloc.free(relational_retirement_json);
    var storage_migration = record.storage_migration;
    if (storage_migration) |*migration| migration.request.job_id = try alloc.dupe(u8, migration.request.job_id);
    errdefer if (storage_migration) |migration| alloc.free(migration.request.job_id);
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const description = try alloc.dupe(u8, record.description);
    errdefer alloc.free(description);
    const schema_json = try alloc.dupe(u8, record.schema_json);
    errdefer alloc.free(schema_json);
    const read_schema_json = try alloc.dupe(u8, record.read_schema_json);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try alloc.dupe(u8, record.indexes_json);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try alloc.dupe(u8, record.replication_sources_json);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try alloc.dupe(u8, record.placement_role);
    errdefer alloc.free(placement_role);
    const restore_backup_id = try alloc.dupe(u8, record.restore_backup_id);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try alloc.dupe(u8, record.restore_location);
    errdefer alloc.free(restore_location);
    return .{
        .storage = record.storage,
        .relational_retirement_json = relational_retirement_json,
        .storage_migration = storage_migration,
        .table_id = record.table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .desired_replica_count = record.desired_replica_count,
        .min_ranges = record.min_ranges,
    };
}

pub fn freeTable(alloc: std.mem.Allocator, record: TableRecord) void {
    @import("record_memory.zig").freeTable(alloc, record);
}
