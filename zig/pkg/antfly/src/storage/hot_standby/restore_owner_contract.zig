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

//! Immutable restore-owner wire values and pure catalog projection.
const std = @import("std");
const native = @import("../db/restore_staging_contract.zig");
const records = @import("../../common/topology_records.zig");

/// Resolve private restore authority from a configured node-local root. Never
/// infer a deployment layout by walking ancestors: custom replica roots may
/// be siblings, and their parents are not owned by this node.
pub fn metadataRootAlloc(alloc: std.mem.Allocator, replica_root: []const u8, explicit_metadata_root: ?[]const u8) ![]u8 {
    if (explicit_metadata_root) |root| {
        if (root.len == 0) return error.InvalidHASeedSnapshotRoot;
        return alloc.dupe(u8, root);
    }
    if (replica_root.len == 0) return error.InvalidHASeedSnapshotRoot;
    return std.fs.path.join(alloc, &.{ replica_root, ".restore-owner-metadata" });
}
pub const Owner = native.OwnerBootstrap;
pub const terminal_artifact_name = "restore-terminals.bin";
pub const max_owners = 65_536;

pub const OwnerRef = struct {
    scope: native.Scope,
    byte_range: @import("../byte_range.zig").ByteRange,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../db/relational_integrity_json.zig").write(self, jw);
    }
};

pub const OwnerTable = struct { table_id: u64, name: []const u8, schema_json: []const u8, read_schema_json: []const u8 = "", indexes_json: []const u8 };

pub const Compact = struct { tables: []const OwnerTable, owners: []const OwnerRef };

/// Keep schema payload proportional to tables, not table×owner count.
pub fn compact(alloc: std.mem.Allocator, owners: []const Owner) !Compact {
    var tables: std.ArrayList(OwnerTable) = .empty;
    errdefer tables.deinit(alloc);
    const refs = try alloc.alloc(OwnerRef, owners.len);
    errdefer alloc.free(refs);
    var seen: std.AutoHashMapUnmanaged(u64, OwnerTable) = .empty;
    defer seen.deinit(alloc);
    for (owners, refs) |owner, *ref| {
        const table: OwnerTable = .{ .table_id = owner.scope.target_namespace.table_id, .name = owner.table_name, .schema_json = owner.schema_json, .read_schema_json = owner.read_schema_json, .indexes_json = owner.indexes_json };
        if (seen.get(table.table_id)) |existing| {
            if (!std.mem.eql(u8, existing.name, table.name) or !std.mem.eql(u8, existing.schema_json, table.schema_json) or !std.mem.eql(u8, existing.read_schema_json, table.read_schema_json) or !std.mem.eql(u8, existing.indexes_json, table.indexes_json)) return error.RestoreStagingScopeChanged;
        } else {
            try seen.put(alloc, table.table_id, table);
            try tables.append(alloc, table);
        }
        ref.* = .{ .scope = owner.scope, .byte_range = owner.byte_range };
    }
    std.mem.sort(OwnerTable, tables.items, {}, struct {
        fn less(_: void, left: OwnerTable, right: OwnerTable) bool {
            return left.table_id < right.table_id;
        }
    }.less);
    return .{ .tables = tables.items, .owners = refs };
}

pub fn expand(alloc: std.mem.Allocator, tables: []const OwnerTable, refs: []const OwnerRef) ![]const Owner {
    if (refs.len > max_owners or tables.len > refs.len) return error.InvalidRestoreStagingRecord;
    var lookup: std.AutoHashMapUnmanaged(u64, struct { value: OwnerTable, used: bool = false }) = .empty;
    defer lookup.deinit(alloc);
    for (tables, 0..) |table, index| {
        if (table.table_id == 0 or (index != 0 and tables[index - 1].table_id >= table.table_id)) return error.InvalidRestoreStagingRecord;
        try lookup.put(alloc, table.table_id, .{ .value = table });
    }
    const owners = try alloc.alloc(Owner, refs.len);
    errdefer alloc.free(owners);
    for (refs, owners) |ref, *owner| {
        const table = lookup.getPtr(ref.scope.target_namespace.table_id) orelse return error.RestoreStagingScopeChanged;
        table.used = true;
        owner.* = .{ .scope = ref.scope, .table_name = table.value.name, .schema_json = table.value.schema_json, .read_schema_json = table.value.read_schema_json, .indexes_json = table.value.indexes_json, .byte_range = ref.byte_range };
        try owner.validate();
    }
    var iterator = lookup.valueIterator();
    while (iterator.next()) |entry| if (!entry.used) return error.InvalidRestoreStagingRecord;
    return owners;
}

pub const Projection = struct { owners: []const Owner, tables: []const records.TableRecord, ranges: []const records.RangeRecord };

/// Catalog overlaps are deduplicated by immutable owner identity, never by
/// table name. An unpublished replacement may intentionally reuse that name.
pub fn project(alloc: std.mem.Allocator, owners: []const Owner, public_tables: []const records.TableRecord, existing_tables: []const records.TableRecord, existing_ranges: []const records.RangeRecord) !Projection {
    if (owners.len > max_owners) return error.InvalidRestoreStagingRecord;
    var tables: std.ArrayList(records.TableRecord) = .empty;
    errdefer tables.deinit(alloc);
    var ranges: std.ArrayList(records.RangeRecord) = .empty;
    errdefer ranges.deinit(alloc);
    var result: std.ArrayList(Owner) = .empty;
    errdefer result.deinit(alloc);
    var table_ids: std.AutoHashMapUnmanaged(u64, records.TableRecord) = .empty;
    defer table_ids.deinit(alloc);
    var group_ids: std.AutoHashMapUnmanaged(u64, records.RangeRecord) = .empty;
    defer group_ids.deinit(alloc);
    for (existing_tables) |table| try table_ids.put(alloc, table.table_id, table);
    for (existing_ranges) |range| try group_ids.put(alloc, range.group_id, range);
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(alloc);
    var public_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer public_ids.deinit(alloc);
    for (public_tables) |table| try public_ids.put(alloc, table.table_id, {});
    for (owners) |owner| {
        try owner.validate();
        const namespace = owner.scope.target_namespace;
        if (namespace.shard_id != namespace.range_id) return error.RestoreStagingScopeChanged;
        const unique = try seen.getOrPut(alloc, namespace.shard_id);
        if (unique.found_existing) return error.InvalidRestoreStagingRecord;
        if (public_ids.contains(namespace.table_id)) continue;
        if (group_ids.get(namespace.shard_id)) |range| {
            const effective_shard = if (range.doc_identity_shard_id != 0) range.doc_identity_shard_id else range.group_id;
            const effective_range = if (range.doc_identity_range_id != 0) range.doc_identity_range_id else if (range.range_id != 0) range.range_id else range.group_id;
            if (range.table_id != namespace.table_id or effective_shard != namespace.shard_id or effective_range != namespace.range_id) return error.RestoreStagingScopeChanged;
            continue;
        }
        if (table_ids.get(namespace.table_id)) |table| {
            if (!std.mem.eql(u8, table.name, owner.table_name)) return error.RestoreStagingScopeChanged;
        } else {
            const table: records.TableRecord = .{ .table_id = namespace.table_id, .name = owner.table_name, .schema_json = owner.schema_json, .read_schema_json = owner.read_schema_json, .indexes_json = owner.indexes_json };
            try table_ids.put(alloc, namespace.table_id, table);
            try tables.append(alloc, table);
        }
        try result.append(alloc, owner);
        try ranges.append(alloc, .{ .group_id = namespace.shard_id, .table_id = namespace.table_id, .range_id = namespace.range_id, .doc_identity_shard_id = namespace.shard_id, .doc_identity_range_id = namespace.range_id, .start_key = owner.byte_range.start, .end_key = owner.byte_range.end });
    }
    return .{ .owners = result.items, .tables = tables.items, .ranges = ranges.items };
}
