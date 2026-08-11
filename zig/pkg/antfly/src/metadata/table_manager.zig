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
const group_ids = @import("../common/group_ids.zig");
const transition_state = @import("transition_state.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_mod = @import("../schema/mod.zig");

pub const default_database_name = "default";
pub const default_namespace_name = "public";

pub fn deriveDatabaseId(database_name: []const u8) u64 {
    const id = std.hash.Wyhash.hash(0x44425441, database_name);
    return if (id == 0) 1 else id;
}

pub fn deriveNamespaceId(database_id: u64, namespace_name: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(0x4e535041);
    hasher.update(std.mem.asBytes(&database_id));
    hasher.update(&[_]u8{0});
    hasher.update(namespace_name);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

pub fn deriveTablespaceId(tablespace_name: []const u8) u64 {
    const id = std.hash.Wyhash.hash(0x54535041, tablespace_name);
    return if (id == 0) 1 else id;
}

pub fn deriveSequenceId(database_name: []const u8, namespace_name: []const u8, sequence_name: []const u8) u64 {
    const database_id = deriveDatabaseId(database_name);
    const namespace_id = deriveNamespaceId(database_id, namespace_name);
    var hasher = std.hash.Wyhash.init(0x53455141);
    hasher.update(std.mem.asBytes(&namespace_id));
    hasher.update(&[_]u8{0});
    hasher.update(sequence_name);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

pub fn sequenceInitialLastValueFromOptionsJson(alloc: std.mem.Allocator, options_json: []const u8) !i64 {
    const start = (try sequenceOptionInteger(alloc, options_json, "restart_with")) orelse
        try sequenceOptionIntegerOrDefault(alloc, options_json, "start_with", 1);
    const increment = try sequenceOptionIntegerOrDefault(alloc, options_json, "increment_by", 1);
    return std.math.sub(i64, start, increment) catch return error.InvalidSequenceCatalog;
}

pub fn sequenceIncrementFromOptionsJson(alloc: std.mem.Allocator, options_json: []const u8) !i64 {
    return try sequenceOptionIntegerOrDefault(alloc, options_json, "increment_by", 1);
}

pub fn sequenceNextValueFromRecord(alloc: std.mem.Allocator, record: SequenceRecord) !i64 {
    const increment = try sequenceIncrementFromOptionsJson(alloc, record.options_json);
    const next_value = std.math.add(i64, record.last_value, increment) catch return error.SequenceExhausted;
    if (increment > 0) {
        if (try sequenceOptionInteger(alloc, record.options_json, "max_value")) |max_value| {
            if (next_value > max_value) return error.SequenceExhausted;
        }
    } else if (increment < 0) {
        if (try sequenceOptionInteger(alloc, record.options_json, "min_value")) |min_value| {
            if (next_value < min_value) return error.SequenceExhausted;
        }
    } else return error.InvalidSequenceCatalog;
    return next_value;
}

fn sequenceOptionIntegerOrDefault(alloc: std.mem.Allocator, options_json: []const u8, key: []const u8, default_value: i64) !i64 {
    return (try sequenceOptionInteger(alloc, options_json, key)) orelse default_value;
}

fn sequenceOptionInteger(alloc: std.mem.Allocator, options_json: []const u8, key: []const u8) !?i64 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, options_json, .{}) catch return error.InvalidSequenceCatalog;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSequenceCatalog;
    const value = parsed.value.object.get(key) orelse return null;
    return switch (value) {
        .integer => |number| number,
        .null => null,
        else => error.InvalidSequenceCatalog,
    };
}

fn sequenceIdentityAllocatorResetTargetIndex(targets: []const SequenceIdentityAllocatorReset, sequence_id: u64) ?usize {
    for (targets, 0..) |target, index| {
        if (target.sequence_id == sequence_id) return index;
    }
    return null;
}

fn sequenceIdForTableDefaultAlloc(
    alloc: std.mem.Allocator,
    table: TableRecord,
    value_json: []const u8,
) !u64 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidSequenceCatalog;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSequenceCatalog;
    const sequence_value = parsed.value.object.get("sequence") orelse return error.InvalidSequenceCatalog;
    if (sequence_value != .string or sequence_value.string.len == 0) return error.InvalidSequenceCatalog;
    const database_name = if (parsed.value.object.get("database")) |database_value| blk: {
        if (database_value != .string) return error.InvalidSequenceCatalog;
        break :blk if (database_value.string.len == 0) table.database_name else database_value.string;
    } else table.database_name;
    const namespace_name = if (parsed.value.object.get("schema")) |schema_value| blk: {
        if (schema_value != .string) return error.InvalidSequenceCatalog;
        break :blk if (schema_value.string.len == 0) table.namespace_name else schema_value.string;
    } else table.namespace_name;
    return deriveSequenceId(database_name, namespace_name, sequence_value.string);
}

fn sequenceOwnedByTableColumn(
    alloc: std.mem.Allocator,
    sequence: SequenceRecord,
    table: TableRecord,
    runtime: runtime_schema.TableSchema,
) !?[]const u8 {
    if (!std.mem.eql(u8, sequence.database_name, table.database_name) or
        !std.mem.eql(u8, sequence.namespace_name, table.namespace_name))
    {
        return null;
    }
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, sequence.options_json, .{}) catch return error.InvalidSequenceCatalog;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSequenceCatalog;
    const owned_by = parsed.value.object.get("owned_by") orelse return null;
    if (owned_by == .null) return null;
    if (owned_by != .object) return error.InvalidSequenceCatalog;
    const table_name = owned_by.object.get("table_name") orelse return error.InvalidSequenceCatalog;
    const column_name = owned_by.object.get("column_name") orelse return error.InvalidSequenceCatalog;
    if (table_name != .string or column_name != .string or
        table_name.string.len == 0 or column_name.string.len == 0)
    {
        return error.InvalidSequenceCatalog;
    }
    if (!std.mem.eql(u8, table_name.string, table.name)) return null;
    for (runtime.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, column_name.string)) return column.name;
    }
    return null;
}

pub const DatabaseRecord = struct {
    database_id: u64,
    name: []const u8,
    settings_json: []const u8 = "{}",
    tablespace_name: []const u8 = "",
};

pub const NamespaceRecord = struct {
    namespace_id: u64,
    database_id: u64,
    name: []const u8,
    tablespace_name: []const u8 = "",
};

pub const TablespaceRecord = struct {
    tablespace_id: u64,
    name: []const u8,
    location_json: []const u8 = "null",
    placement_policy_json: []const u8 = "{}",
};

pub const SequenceRecord = struct {
    sequence_id: u64,
    name: []const u8,
    database_name: []const u8 = default_database_name,
    namespace_name: []const u8 = default_namespace_name,
    options_json: []const u8 = "{}",
    last_value: i64 = 0,
    last_allocation_id: u128 = 0,
};

pub const SequenceCompareAndSwapRequest = struct {
    sequence_id: u64,
    expected_last_value: i64,
    next_last_value: i64,
    allocation_id: u128,
};

pub const SequenceIdentityAllocatorReset = struct {
    sequence_id: u64,
    reset_last_value: i64,
};

pub const PlacementClass = enum {
    data,
    hot,
    cold,
    serving,
    bulk,
    archive,
};

pub const TableRecord = struct {
    table_id: u64,
    name: []const u8,
    database_name: []const u8 = default_database_name,
    namespace_name: []const u8 = default_namespace_name,
    description: []const u8 = "",
    schema_json: []const u8 = "",
    read_schema_json: []const u8 = "",
    foreign_key_validation_json: []const u8 = "{}",
    indexes_json: []const u8 = "{}",
    replication_sources_json: []const u8 = "[]",
    placement_role: []const u8 = "data",
    tablespace_name: []const u8 = "",
    restore_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    desired_replica_count: u16 = 3,
    min_ranges: u32 = 1,
    data_generation: u64 = 0,

    pub fn migrationState(self: *const TableRecord) TableMigrationState {
        return .{
            .schema_json = self.schema_json,
            .read_schema_json = self.read_schema_json,
        };
    }

    pub fn indexCatalog(self: *const TableRecord) TableIndexCatalog {
        return .{
            .indexes_json = self.indexes_json,
        };
    }
};

// TableDefinition is the preferred product/control-plane name. TableRecord
// remains as the current storage/runtime name during the migration.
pub const TableDefinition = TableRecord;

pub fn tableDefinitionsEqual(lhs: TableDefinition, rhs: TableDefinition) bool {
    return lhs.table_id == rhs.table_id and
        std.mem.eql(u8, lhs.name, rhs.name) and
        std.mem.eql(u8, lhs.database_name, rhs.database_name) and
        std.mem.eql(u8, lhs.namespace_name, rhs.namespace_name) and
        std.mem.eql(u8, lhs.description, rhs.description) and
        std.mem.eql(u8, lhs.schema_json, rhs.schema_json) and
        std.mem.eql(u8, lhs.read_schema_json, rhs.read_schema_json) and
        std.mem.eql(u8, lhs.foreign_key_validation_json, rhs.foreign_key_validation_json) and
        std.mem.eql(u8, lhs.indexes_json, rhs.indexes_json) and
        std.mem.eql(u8, lhs.replication_sources_json, rhs.replication_sources_json) and
        std.mem.eql(u8, lhs.placement_role, rhs.placement_role) and
        std.mem.eql(u8, lhs.tablespace_name, rhs.tablespace_name) and
        std.mem.eql(u8, lhs.restore_backup_id, rhs.restore_backup_id) and
        std.mem.eql(u8, lhs.restore_location, rhs.restore_location) and
        lhs.desired_replica_count == rhs.desired_replica_count and
        lhs.min_ranges == rhs.min_ranges and
        lhs.data_generation == rhs.data_generation;
}

test "table definition equality includes branch catalog identity and generation" {
    const base: TableDefinition = .{
        .table_id = 7,
        .name = "events",
        .database_name = "tenant",
        .namespace_name = "analytics",
        .foreign_key_validation_json = "{\"validated\":true}",
        .tablespace_name = "hot",
        .data_generation = 9,
    };
    try std.testing.expect(tableDefinitionsEqual(base, base));

    var changed = base;
    changed.database_name = "other";
    try std.testing.expect(!tableDefinitionsEqual(base, changed));
    changed = base;
    changed.namespace_name = "other";
    try std.testing.expect(!tableDefinitionsEqual(base, changed));
    changed = base;
    changed.foreign_key_validation_json = "{}";
    try std.testing.expect(!tableDefinitionsEqual(base, changed));
    changed = base;
    changed.tablespace_name = "cold";
    try std.testing.expect(!tableDefinitionsEqual(base, changed));
    changed = base;
    changed.data_generation += 1;
    try std.testing.expect(!tableDefinitionsEqual(base, changed));
}

pub const TableMigrationState = struct {
    schema_json: []const u8,
    read_schema_json: []const u8,

    pub fn migrating(self: TableMigrationState) bool {
        return self.read_schema_json.len > 0;
    }
};

pub const TableIndexCatalog = struct {
    indexes_json: []const u8,
};

pub const RangeRecord = struct {
    group_id: u64,
    range_id: u64 = 0,
    table_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8 = null,
    doc_identity_shard_id: u64 = 0,
    doc_identity_range_id: u64 = 0,
    /// Monotonic source-local split attempt allocator. Durable metadata
    /// advances this only in the CAS command that admits the corresponding
    /// transition, so an epoch cannot be consumed without recovery state.
    split_attempt_epoch: u64 = 0,
    restore_backup_id: []const u8 = "",
    restore_artifact_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    restore_snapshot_path: []const u8 = "",
    /// Cluster-local authority used to resolve `restore_location`. This is an
    /// identifier only; credentials remain in each node's secret/config store.
    restore_connection: []const u8 = "",
    /// Content identity captured from the immutable backup manifest at
    /// admission. New restores require a SHA-256 binding.
    restore_artifact_size_bytes: u64 = 0,
    restore_artifact_sha256: []const u8 = "",
    /// Durable, bounded idempotency provenance for the most recently
    /// completed restore. Active replica progress can be garbage-collected
    /// without making an exact job retry ambiguous.
    completed_restore_fingerprint: RestoreCompletionFingerprint =
        empty_restore_completion_fingerprint,
};

pub const foreign_key_ref_range_active = "active";
pub const foreign_key_ref_range_splitting = "splitting";
pub const foreign_key_ref_range_merging = "merging";
pub const foreign_key_ref_range_rebuilding = "rebuilding";

pub const ForeignKeyReferenceRangeRecord = struct {
    child_table_id: u64,
    constraint_name: []const u8,
    parent_table_id: u64,
    start_parent_key: []const u8,
    end_parent_key: ?[]const u8 = null,
    group_id: u64,
    range_id: u64 = 0,
    topology_epoch: u64 = 0,
    state: []const u8 = foreign_key_ref_range_active,
};

pub const unique_constraint_range_active = "active";
pub const unique_constraint_range_splitting = "splitting";
pub const unique_constraint_range_merging = "merging";
pub const unique_constraint_range_rebuilding = "rebuilding";

pub const UniqueConstraintRangeRecord = struct {
    table_id: u64,
    constraint_name: []const u8,
    start_encoded_value: []const u8,
    end_encoded_value: ?[]const u8 = null,
    group_id: u64,
    range_id: u64 = 0,
    topology_epoch: u64 = 0,
    state: []const u8 = unique_constraint_range_active,
};

pub const secondary_index_rebuild_declared = "declared";
pub const secondary_index_rebuild_building = "building";
pub const secondary_index_rebuild_ready = "ready";
pub const secondary_index_rebuild_invalid = "invalid";

pub const SecondaryIndexRebuildRangeRecord = struct {
    table_id: u64,
    index_name: []const u8,
    index_generation: u64,
    start_row_key: []const u8,
    end_row_key: ?[]const u8 = null,
    group_id: u64,
    range_id: u64 = 0,
    topology_epoch: u64 = 0,
    state: []const u8 = secondary_index_rebuild_declared,
    lease_owner: []const u8 = "",
    lease_expires_at_ms: u64 = 0,
    attempts: u32 = 0,
    completed_row_count: u64 = 0,
    progress_row_key: []const u8 = "",
    last_error: []const u8 = "",
};

pub const schema_rewrite_declared = "declared";
pub const schema_rewrite_running = "running";
pub const schema_rewrite_paused = "paused";
pub const schema_rewrite_ready = "ready";
pub const schema_rewrite_invalid = "invalid";
pub const schema_rewrite_canceled = "canceled";

pub const SchemaRewriteExpression = struct {
    target_column: []const u8,
    expression: runtime_schema.RelationalRowsExpression,
};

pub const SchemaRewriteRename = struct {
    old_path: []const u8,
    new_path: []const u8,
};

pub const SchemaRewriteJobRecord = struct {
    job_id: u64,
    table_id: u64,
    group_id: u64,
    range_id: u64 = 0,
    schema_generation: u64,
    action: []const u8,
    reason: []const u8,
    start_row_key: []const u8,
    end_row_key: ?[]const u8 = null,
    state: []const u8 = schema_rewrite_declared,
    target_column: []const u8 = "",
    expression: ?runtime_schema.RelationalRowsExpression = null,
    full_row_rewrite: bool = false,
    rewrite_renames: []const SchemaRewriteRename = &.{},
    rewrite_drops: []const []const u8 = &.{},
    lease_owner: []const u8 = "",
    lease_expires_at_ms: u64 = 0,
    attempts: u32 = 0,
    completed_row_count: u64 = 0,
    progress_row_key: []const u8 = "",
    last_error: []const u8 = "",
};

pub const SchemaRewriteJobBeginRequest = struct {
    job_id: u64,
    lease_owner: []const u8,
    now_ms: u64 = 0,
    lease_expires_at_ms: u64,
};

pub const SchemaRewriteJobFinishRequest = struct {
    job_id: u64,
    lease_owner: []const u8,
    completed_row_count: u64,
    progress_row_key: []const u8,
};

pub const SchemaRewriteJobInvalidateRequest = struct {
    job_id: u64,
    lease_owner: []const u8,
    last_error: []const u8,
};

pub const SchemaRewriteJobControlRequest = struct {
    job_id: u64,
    reason: []const u8 = "",
};

pub const TableCatalogUpdateWithSchemaRewriteJobsRequest = struct {
    table: TableRecord,
    schema_rewrite_jobs: []const SchemaRewriteJobRecord = &.{},
};

pub const TableCatalogBatchUpdateWithSchemaRewriteJobsRequest = struct {
    tables: []const TableRecord,
    schema_rewrite_jobs: []const SchemaRewriteJobRecord = &.{},
};

pub const TableCatalogDropWithSchemaRewriteJobsRequest = struct {
    table_id: u64,
    sequence_ids: []const u64 = &.{},
    range_group_ids: []const u64 = &.{},
    table_updates: []const TableRecord = &.{},
    schema_rewrite_jobs: []const SchemaRewriteJobRecord = &.{},
};

pub const table_emptying_declared = "declared";
pub const table_emptying_running = "running";
pub const table_emptying_paused = "paused";
pub const table_emptying_ready = "ready";
pub const table_emptying_invalid = "invalid";
pub const table_emptying_canceled = "canceled";

pub const TableEmptyingJobRecord = struct {
    job_id: u64,
    table_id: u64,
    group_id: u64,
    range_id: u64 = 0,
    schema_generation: u64,
    data_generation: u64 = 0,
    barrier_id: u64 = 0,
    start_row_key: []const u8 = "",
    end_row_key: ?[]const u8 = null,
    affected_table_ids: []const u64 = &.{},
    restart_identity: bool = false,
    cascade: bool = false,
    state: []const u8 = table_emptying_declared,
    lease_owner: []const u8 = "",
    lease_expires_at_ms: u64 = 0,
    attempts: u32 = 0,
    completed_row_count: u64 = 0,
    progress_row_key: []const u8 = "",
    last_error: []const u8 = "",
};

pub const TableEmptyingJobBeginRequest = struct {
    job_id: u64,
    lease_owner: []const u8,
    now_ms: u64 = 0,
    lease_expires_at_ms: u64,
};

pub const TableEmptyingJobFinishRequest = struct {
    job_id: u64,
    lease_owner: []const u8,
    completed_row_count: u64,
    progress_row_key: []const u8,
};

pub const TableEmptyingJobInvalidateRequest = struct {
    job_id: u64,
    lease_owner: []const u8,
    last_error: []const u8,
};

pub const TableEmptyingJobControlRequest = struct {
    job_id: u64,
    reason: []const u8 = "",
};

pub const TableEmptyingBarrierPromotion = struct {
    table_id: u64,
    target_generation: u64,
};

pub const TableEmptyingBarrierPromotionRequest = struct {
    job_ids: []const u64 = &.{},
    promotions: []const TableEmptyingBarrierPromotion = &.{},
};

pub const TableEmptyingIdentityAllocatorResetRequest = struct {
    barrier_id: u64,
    affected_table_ids: []const u64,
    job_ids: []const u64,
    cascade: bool = false,
};

pub const ForeignKeyReferenceRangeSelector = struct {
    child_table_id: u64,
    constraint_name: []const u8,
    parent_table_id: u64,
    start_parent_key: []const u8,
    range_id: u64 = 0,
};

pub const ForeignKeyReferenceRangeSplitRequest = struct {
    selector: ForeignKeyReferenceRangeSelector,
    split_parent_key: []const u8,
    left_group_id: u64,
    right_group_id: u64,
};

pub const ForeignKeyReferenceRangeMergeRequest = struct {
    left_selector: ForeignKeyReferenceRangeSelector,
    right_start_parent_key: []const u8,
    merged_group_id: u64,
};

pub const UniqueConstraintRangeSelector = struct {
    table_id: u64,
    constraint_name: []const u8,
    start_encoded_value: []const u8,
    range_id: u64 = 0,
};

pub const SecondaryIndexRebuildRangeSelector = struct {
    table_id: u64,
    index_name: []const u8,
    index_generation: u64,
    start_row_key: []const u8,
};

pub const SecondaryIndexRebuildRangeBeginRequest = struct {
    selector: SecondaryIndexRebuildRangeSelector,
    lease_owner: []const u8,
    now_ms: u64 = 0,
    lease_expires_at_ms: u64,
};

pub const SecondaryIndexRebuildRangeFinishRequest = struct {
    selector: SecondaryIndexRebuildRangeSelector,
    completed_row_count: u64,
    progress_row_key: []const u8,
};

pub const SecondaryIndexRebuildRangeProgressRequest = struct {
    selector: SecondaryIndexRebuildRangeSelector,
    completed_row_count: u64,
    progress_row_key: []const u8,
};

pub const SecondaryIndexRebuildRangeInvalidateRequest = struct {
    selector: SecondaryIndexRebuildRangeSelector,
    last_error: []const u8,
};

pub const SecondaryIndexReadyExpectation = struct {
    generation: u64,
    access_method: runtime_schema.RelationalIndexAccessMethod,
    schema_fingerprint: []const u8,
};

pub const SecondaryIndexReadyPromotionRequest = struct {
    table_id: u64,
    index_name: []const u8,
    expected: SecondaryIndexReadyExpectation,
    expected_schema_json: []const u8,
    promoted_table: TableRecord,
};

pub const TableSchemaCompareAndSwapRequest = struct {
    table_id: u64,
    expected_schema_json: []const u8,
    promoted_table: TableRecord,
};

pub const UniqueConstraintRangeSplitRequest = struct {
    selector: UniqueConstraintRangeSelector,
    split_encoded_value: []const u8,
    left_group_id: u64,
    right_group_id: u64,
};

pub const UniqueConstraintRangeMergeRequest = struct {
    left_selector: UniqueConstraintRangeSelector,
    right_start_encoded_value: []const u8,
    merged_group_id: u64,
};

pub fn foreignKeyReferenceRangeStateValid(state: []const u8) bool {
    return std.mem.eql(u8, state, foreign_key_ref_range_active) or
        std.mem.eql(u8, state, foreign_key_ref_range_splitting) or
        std.mem.eql(u8, state, foreign_key_ref_range_merging) or
        std.mem.eql(u8, state, foreign_key_ref_range_rebuilding);
}

pub fn foreignKeyReferenceRangeRoutable(record: ForeignKeyReferenceRangeRecord) bool {
    return std.mem.eql(u8, record.state, foreign_key_ref_range_active);
}

pub fn uniqueConstraintRangeStateValid(state: []const u8) bool {
    return std.mem.eql(u8, state, unique_constraint_range_active) or
        std.mem.eql(u8, state, unique_constraint_range_splitting) or
        std.mem.eql(u8, state, unique_constraint_range_merging) or
        std.mem.eql(u8, state, unique_constraint_range_rebuilding);
}

pub fn uniqueConstraintRangeRoutable(record: UniqueConstraintRangeRecord) bool {
    return std.mem.eql(u8, record.state, unique_constraint_range_active);
}

pub fn secondaryIndexRebuildRangeStateValid(state: []const u8) bool {
    return std.mem.eql(u8, state, secondary_index_rebuild_declared) or
        std.mem.eql(u8, state, secondary_index_rebuild_building) or
        std.mem.eql(u8, state, secondary_index_rebuild_ready) or
        std.mem.eql(u8, state, secondary_index_rebuild_invalid);
}

pub fn secondaryIndexRebuildRangeComplete(record: SecondaryIndexRebuildRangeRecord) bool {
    return std.mem.eql(u8, record.state, secondary_index_rebuild_ready);
}

pub fn schemaRewriteJobStateValid(state: []const u8) bool {
    return std.mem.eql(u8, state, schema_rewrite_declared) or
        std.mem.eql(u8, state, schema_rewrite_running) or
        std.mem.eql(u8, state, schema_rewrite_paused) or
        std.mem.eql(u8, state, schema_rewrite_ready) or
        std.mem.eql(u8, state, schema_rewrite_invalid) or
        std.mem.eql(u8, state, schema_rewrite_canceled);
}

pub fn schemaRewriteJobComplete(record: SchemaRewriteJobRecord) bool {
    return std.mem.eql(u8, record.state, schema_rewrite_ready);
}

pub fn tableEmptyingJobStateValid(state: []const u8) bool {
    return std.mem.eql(u8, state, table_emptying_declared) or
        std.mem.eql(u8, state, table_emptying_running) or
        std.mem.eql(u8, state, table_emptying_paused) or
        std.mem.eql(u8, state, table_emptying_ready) or
        std.mem.eql(u8, state, table_emptying_invalid) or
        std.mem.eql(u8, state, table_emptying_canceled);
}

pub fn tableEmptyingJobComplete(record: TableEmptyingJobRecord) bool {
    return std.mem.eql(u8, record.state, table_emptying_ready);
}

pub fn tableEmptyingAffectedTableIdsValid(primary_table_id: u64, affected_table_ids: []const u64) bool {
    if (primary_table_id == 0 or affected_table_ids.len == 0) return false;
    var includes_primary = false;
    for (affected_table_ids, 0..) |table_id, i| {
        if (table_id == 0) return false;
        if (table_id == primary_table_id) includes_primary = true;
        for (affected_table_ids[0..i]) |prior| {
            if (prior == table_id) return false;
        }
    }
    return includes_primary;
}

pub fn tableEmptyingAffectedTableIdsCanonicalSetValid(affected_table_ids: []const u64) bool {
    if (affected_table_ids.len == 0) return false;
    for (affected_table_ids, 0..) |table_id, i| {
        if (table_id == 0) return false;
        if (i != 0 and affected_table_ids[i - 1] >= table_id) return false;
    }
    return true;
}

pub const RestoreCompletionFingerprint =
    [std.crypto.hash.sha2.Sha256.digest_length]u8;
pub const empty_restore_completion_fingerprint: RestoreCompletionFingerprint =
    [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length;

fn hashRestoreCompletionPart(
    hasher: *std.crypto.hash.sha2.Sha256,
    value: []const u8,
) void {
    var len_bytes: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &len_bytes, @intCast(value.len), .little);
    hasher.update(&len_bytes);
    hasher.update(value);
}

pub fn restoreCompletionFingerprint(
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    location: []const u8,
) RestoreCompletionFingerprint {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-restore-completion-v1");
    hashRestoreCompletionPart(&hasher, backup_id);
    hashRestoreCompletionPart(&hasher, artifact_backup_id);
    hashRestoreCompletionPart(&hasher, location);
    var fingerprint: RestoreCompletionFingerprint = undefined;
    hasher.final(&fingerprint);
    return fingerprint;
}

pub fn rangeRestoreCompletionMatches(
    record: RangeRecord,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    location: []const u8,
) bool {
    const expected = restoreCompletionFingerprint(
        backup_id,
        artifact_backup_id,
        location,
    );
    return std.mem.eql(
        u8,
        &record.completed_restore_fingerprint,
        &expected,
    );
}

/// Exact identity of a range-scoped restore intent. Completion commands carry
/// this value so a delayed proposal cannot clear a superseding restore.
pub const RestoreIntentIdentity = struct {
    group_id: u64,
    table_id: u64,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
    connection: []const u8,
    artifact_size_bytes: u64,
    artifact_sha256: []const u8,
};

pub fn restoreIntentIdentity(record: RangeRecord) RestoreIntentIdentity {
    return .{
        .group_id = record.group_id,
        .table_id = record.table_id,
        .backup_id = record.restore_backup_id,
        .artifact_backup_id = record.restore_artifact_backup_id,
        .location = record.restore_location,
        .snapshot_path = record.restore_snapshot_path,
        .connection = record.restore_connection,
        .artifact_size_bytes = record.restore_artifact_size_bytes,
        .artifact_sha256 = record.restore_artifact_sha256,
    };
}

pub fn restoreIntentMatchesRange(expected: RestoreIntentIdentity, record: RangeRecord) bool {
    return expected.group_id == record.group_id and
        expected.table_id == record.table_id and
        std.mem.eql(u8, expected.backup_id, record.restore_backup_id) and
        std.mem.eql(u8, expected.artifact_backup_id, record.restore_artifact_backup_id) and
        std.mem.eql(u8, expected.location, record.restore_location) and
        std.mem.eql(u8, expected.snapshot_path, record.restore_snapshot_path) and
        std.mem.eql(u8, expected.connection, record.restore_connection) and
        expected.artifact_size_bytes == record.restore_artifact_size_bytes and
        std.mem.eql(u8, expected.artifact_sha256, record.restore_artifact_sha256);
}

pub fn cloneRestoreIntentIdentity(
    alloc: std.mem.Allocator,
    identity: RestoreIntentIdentity,
) !RestoreIntentIdentity {
    const backup_id = try alloc.dupe(u8, identity.backup_id);
    errdefer alloc.free(backup_id);
    const artifact_backup_id = try alloc.dupe(u8, identity.artifact_backup_id);
    errdefer alloc.free(artifact_backup_id);
    const location = try alloc.dupe(u8, identity.location);
    errdefer alloc.free(location);
    const snapshot_path = try alloc.dupe(u8, identity.snapshot_path);
    errdefer alloc.free(snapshot_path);
    const connection = try alloc.dupe(u8, identity.connection);
    errdefer alloc.free(connection);
    const artifact_sha256 = try alloc.dupe(u8, identity.artifact_sha256);
    errdefer alloc.free(artifact_sha256);
    return .{
        .group_id = identity.group_id,
        .table_id = identity.table_id,
        .backup_id = backup_id,
        .artifact_backup_id = artifact_backup_id,
        .location = location,
        .snapshot_path = snapshot_path,
        .connection = connection,
        .artifact_size_bytes = identity.artifact_size_bytes,
        .artifact_sha256 = artifact_sha256,
    };
}

pub fn freeRestoreIntentIdentity(
    alloc: std.mem.Allocator,
    identity: RestoreIntentIdentity,
) void {
    alloc.free(identity.backup_id);
    alloc.free(identity.artifact_backup_id);
    alloc.free(identity.location);
    alloc.free(identity.snapshot_path);
    alloc.free(identity.connection);
    alloc.free(identity.artifact_sha256);
}

pub fn clearOwnedRangeRestoreIntent(alloc: std.mem.Allocator, record: *RangeRecord) !void {
    const completed_restore_fingerprint = restoreCompletionFingerprint(
        record.restore_backup_id,
        record.restore_artifact_backup_id,
        record.restore_location,
    );
    const backup_id = try alloc.dupe(u8, "");
    errdefer alloc.free(backup_id);
    const artifact_backup_id = try alloc.dupe(u8, "");
    errdefer alloc.free(artifact_backup_id);
    const location = try alloc.dupe(u8, "");
    errdefer alloc.free(location);
    const snapshot_path = try alloc.dupe(u8, "");
    errdefer alloc.free(snapshot_path);
    const connection = try alloc.dupe(u8, "");
    errdefer alloc.free(connection);
    const artifact_sha256 = try alloc.dupe(u8, "");
    errdefer alloc.free(artifact_sha256);

    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_artifact_backup_id);
    alloc.free(record.restore_location);
    alloc.free(record.restore_snapshot_path);
    alloc.free(record.restore_connection);
    alloc.free(record.restore_artifact_sha256);
    record.restore_backup_id = backup_id;
    record.restore_artifact_backup_id = artifact_backup_id;
    record.restore_location = location;
    record.restore_snapshot_path = snapshot_path;
    record.restore_connection = connection;
    record.restore_artifact_size_bytes = 0;
    record.restore_artifact_sha256 = artifact_sha256;
    record.completed_restore_fingerprint = completed_restore_fingerprint;
}

pub fn rangeRecordsEqual(lhs: RangeRecord, rhs: RangeRecord) bool {
    return lhs.group_id == rhs.group_id and
        lhs.range_id == rhs.range_id and
        lhs.table_id == rhs.table_id and
        std.mem.eql(u8, lhs.start_key, rhs.start_key) and
        ((lhs.end_key == null and rhs.end_key == null) or
            (lhs.end_key != null and rhs.end_key != null and std.mem.eql(u8, lhs.end_key.?, rhs.end_key.?))) and
        lhs.doc_identity_shard_id == rhs.doc_identity_shard_id and
        lhs.doc_identity_range_id == rhs.doc_identity_range_id and
        lhs.split_attempt_epoch == rhs.split_attempt_epoch and
        std.mem.eql(u8, lhs.restore_backup_id, rhs.restore_backup_id) and
        std.mem.eql(u8, lhs.restore_artifact_backup_id, rhs.restore_artifact_backup_id) and
        std.mem.eql(u8, lhs.restore_location, rhs.restore_location) and
        std.mem.eql(u8, lhs.restore_snapshot_path, rhs.restore_snapshot_path) and
        std.mem.eql(u8, lhs.restore_connection, rhs.restore_connection) and
        lhs.restore_artifact_size_bytes == rhs.restore_artifact_size_bytes and
        std.mem.eql(u8, lhs.restore_artifact_sha256, rhs.restore_artifact_sha256) and
        std.mem.eql(
            u8,
            &lhs.completed_restore_fingerprint,
            &rhs.completed_restore_fingerprint,
        );
}

/// Returns true when an existing topology is either the exact requested
/// restore intent or a prefix left by an interrupted multi-record publish.
pub fn restoreIntentTopologyCompatible(
    alloc: std.mem.Allocator,
    existing_table: TableRecord,
    existing_ranges: []const RangeRecord,
    expected_table: TableRecord,
    expected_ranges: []const RangeRecord,
) !bool {
    if (!tableDefinitionsEqual(existing_table, expected_table)) return false;

    var expected_by_group: std.AutoHashMapUnmanaged(u64, RangeRecord) = .empty;
    defer expected_by_group.deinit(alloc);
    try expected_by_group.ensureTotalCapacity(alloc, @intCast(expected_ranges.len));
    for (expected_ranges) |expected_range| {
        if (expected_by_group.contains(expected_range.group_id)) return false;
        expected_by_group.putAssumeCapacity(expected_range.group_id, expected_range);
    }
    for (existing_ranges) |existing_range| {
        if (existing_range.table_id != existing_table.table_id) continue;
        const expected_range = expected_by_group.get(existing_range.group_id) orelse return false;
        if (!rangeRecordsEqual(existing_range, expected_range)) return false;
    }
    return true;
}

pub fn tableEmptyingAffectedTableIdsCanonicalValid(primary_table_id: u64, affected_table_ids: []const u64) bool {
    return tableEmptyingAffectedTableIdsCanonicalSetValid(affected_table_ids) and
        tableEmptyingAffectedTableIdsValid(primary_table_id, affected_table_ids);
}

fn hashTableEmptyingJobU64(hasher: *std.hash.Wyhash, value: u64) void {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, value, .little);
    hasher.update(&raw);
}

fn hashTableEmptyingJobBool(hasher: *std.hash.Wyhash, value: bool) void {
    hasher.update(if (value) "\x01" else "\x00");
}

pub fn stableTableEmptyingJobId(record: TableEmptyingJobRecord) u64 {
    var hasher = std.hash.Wyhash.init(0x5445_4d50_5459_2026);
    hashTableEmptyingJobU64(&hasher, record.table_id);
    hashTableEmptyingJobU64(&hasher, record.group_id);
    hashTableEmptyingJobU64(&hasher, record.range_id);
    hashTableEmptyingJobU64(&hasher, record.schema_generation);
    hashTableEmptyingJobU64(&hasher, record.data_generation);
    hashTableEmptyingJobU64(&hasher, record.barrier_id);
    hasher.update(record.start_row_key);
    hasher.update(&[_]u8{0});
    if (record.end_row_key) |end_row_key| hasher.update(end_row_key);
    hasher.update(&[_]u8{0});
    hashTableEmptyingJobU64(&hasher, @intCast(record.affected_table_ids.len));
    for (record.affected_table_ids) |table_id| hashTableEmptyingJobU64(&hasher, table_id);
    hashTableEmptyingJobBool(&hasher, record.restart_identity);
    hashTableEmptyingJobBool(&hasher, record.cascade);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

pub fn schemaRewriteGenerationForSchemaJson(schema_json: []const u8) u64 {
    const value = std.hash.Wyhash.hash(0x53434a47, schema_json);
    return if (value == 0) 1 else value;
}

pub const node_lifecycle_active = "active";
pub const node_lifecycle_draining = "draining";

pub fn nodeLifecycleActive(lifecycle: []const u8) bool {
    return std.mem.eql(u8, lifecycle, node_lifecycle_active);
}

pub const NodeRecord = struct {
    node_id: u64,
    role: []const u8 = "data",
    lifecycle: []const u8 = node_lifecycle_active,
};

pub const StoreRecord = struct {
    store_id: u64,
    node_id: u64,
    api_url: []const u8 = "",
    raft_url: []const u8 = "",
    role: []const u8 = "data",
    health_class: []const u8 = "healthy",
    failure_domain: []const u8 = "",
    live: bool = true,
    drain_requested: bool = false,
    capacity_bytes: u64 = 0,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    active_backfills: u32 = 0,
    backfill_progress_millis: u16 = 1000,
    group_statuses: []GroupStatusReport = &.{},
    runtime_statuses: []RuntimeGroupStatusReport = &.{},
};

pub const GroupStatusReport = struct {
    group_id: u64,
    /// Placement incarnation observed by the reporting store. A relocation
    /// target may only be promoted from status produced for the same
    /// incarnation as its placement intent.
    relocation_generation: u64 = 0,
    /// Highest data-Raft log index applied to this store's local state
    /// machine for the group.
    raft_applied_index: u64 = 0,
    /// Raft term observed with the membership and leadership fields below.
    /// Unlike report timestamps, terms are comparable across replicas.
    raft_term: u64 = 0,
    /// Applied Raft index at which this replica observed `voter_set_fingerprint`.
    /// Conflicting membership from a lower index is stale evidence, while a
    /// leader may resolve a conflict only after observing at least this index.
    raft_membership_index: u64 = 0,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    disk_bytes_known: bool = false,
    empty: bool = true,
    created_at_millis: u64 = 0,
    updated_at_millis: u64 = 0,
    local_leader: bool = false,
    local_voter: bool = false,
    voter_count: u16 = 0,
    voter_set_known: bool = false,
    voter_set_fingerprint: VoterSetFingerprint = [_]u8{0} ** voter_set_fingerprint_len,
    joint_consensus: bool = false,
    transition_pending: bool = false,
    replay_required: bool = false,
    replay_caught_up: bool = false,
    cutover_ready: bool = false,
    reads_ready_after_cutover: bool = false,
};

pub const voter_set_fingerprint_len = std.crypto.hash.sha2.Sha256.digest_length;
pub const VoterSetFingerprint = [voter_set_fingerprint_len]u8;

pub const ResolvedVoterSetEvidence = struct {
    voter_count_known: bool,
    voter_count: u16,
    from_leader: bool,
    voter_set_known: bool = false,
    voter_set_fingerprint: VoterSetFingerprint = [_]u8{0} ** voter_set_fingerprint_len,
    membership_index: u64 = 0,
};

pub const GroupLeaderCandidate = struct {
    store_id: u64,
    report: GroupStatusReport,
};

/// Selects a leader report using Raft evidence that is comparable across
/// replicas. Reporter timestamps are deliberately excluded: they are useful
/// for freshness, but cannot establish authority across hosts. Callers must
/// first fence each report against its own placement's relocation generation;
/// those generations are member-local and cannot order reports across stores.
pub const GroupLeaderEvidence = struct {
    max_raft_term: u64 = 0,
    max_membership_index: u64 = 0,
    candidate: ?GroupLeaderCandidate = null,
    ambiguous: bool = false,

    pub fn observe(
        self: *@This(),
        store_id: u64,
        report: GroupStatusReport,
    ) void {
        self.max_membership_index = @max(
            self.max_membership_index,
            report.raft_membership_index,
        );
        if (report.raft_term > self.max_raft_term) {
            self.max_raft_term = report.raft_term;
            self.candidate = null;
            self.ambiguous = false;
        } else if (report.raft_term < self.max_raft_term) {
            return;
        }
        if (!report.local_leader) return;

        if (self.candidate) |current| {
            if (current.report.raft_term < report.raft_term) {
                self.candidate = .{ .store_id = store_id, .report = report };
                self.ambiguous = false;
                return;
            }
            if (current.store_id != store_id) {
                // Two stores claiming leadership in the same Raft term is
                // contradictory evidence. Fail closed until a higher term
                // resolves it.
                self.ambiguous = true;
                return;
            }
            if (report.raft_membership_index > current.report.raft_membership_index or
                (report.raft_membership_index == current.report.raft_membership_index and
                    report.raft_applied_index > current.report.raft_applied_index) or
                (report.raft_membership_index == current.report.raft_membership_index and
                    report.raft_applied_index == current.report.raft_applied_index and
                    report.updated_at_millis > current.report.updated_at_millis))
            {
                self.candidate = .{ .store_id = store_id, .report = report };
            }
            return;
        }
        self.candidate = .{ .store_id = store_id, .report = report };
    }

    pub fn resolve(self: @This()) ?GroupLeaderCandidate {
        if (self.ambiguous) return null;
        const candidate = self.candidate orelse return null;
        if (candidate.report.raft_term != self.max_raft_term or
            candidate.report.raft_membership_index < self.max_membership_index)
        {
            return null;
        }
        return candidate;
    }
};

/// Merges membership observations without allowing a reporter that explicitly
/// lacks Raft voter-set knowledge to poison authoritative evidence. A count
/// from such a reporter remains useful as a conservative fallback; once any
/// fingerprint-qualified evidence exists, only qualified reports can conflict
/// with it. As with leader evidence, callers own per-member relocation fences.
pub const VoterSetEvidence = struct {
    fallback_voter_count: ?u16 = null,
    fallback_membership_index: u64 = 0,
    ambiguous_fallback_voter_count: bool = false,
    known_voter_count: ?u16 = null,
    known_voter_set_fingerprint: VoterSetFingerprint = [_]u8{0} ** voter_set_fingerprint_len,
    known_membership_index: u64 = 0,
    has_known_voter_set: bool = false,
    ambiguous_known_voter_set: bool = false,

    pub fn observe(self: *@This(), report: GroupStatusReport) void {
        if (report.voter_count == 0) return;
        if (self.fallback_voter_count == null or
            report.raft_membership_index > self.fallback_membership_index)
        {
            self.fallback_voter_count = report.voter_count;
            self.fallback_membership_index = report.raft_membership_index;
            self.ambiguous_fallback_voter_count = false;
        } else if (report.raft_membership_index == self.fallback_membership_index) {
            if (self.fallback_voter_count.? != report.voter_count)
                self.ambiguous_fallback_voter_count = true;
        }
        if (!report.voter_set_known) return;

        if (!self.has_known_voter_set or
            report.raft_membership_index > self.known_membership_index)
        {
            self.known_voter_count = report.voter_count;
            self.known_voter_set_fingerprint = report.voter_set_fingerprint;
            self.known_membership_index = report.raft_membership_index;
            self.has_known_voter_set = true;
            self.ambiguous_known_voter_set = false;
            return;
        }
        if (report.raft_membership_index < self.known_membership_index) return;
        if (self.known_voter_count.? != report.voter_count or
            !std.mem.eql(
                u8,
                &self.known_voter_set_fingerprint,
                &report.voter_set_fingerprint,
            ))
        {
            self.ambiguous_known_voter_set = true;
        }
    }

    pub fn resolve(
        self: @This(),
        authoritative_leader: ?GroupStatusReport,
    ) ResolvedVoterSetEvidence {
        if (authoritative_leader) |leader| {
            if (leader.voter_set_known and
                leader.voter_count > 0 and
                (!self.has_known_voter_set or
                    leader.raft_membership_index >= self.known_membership_index))
            {
                return .{
                    .voter_count_known = true,
                    .voter_count = leader.voter_count,
                    .from_leader = true,
                    .voter_set_known = true,
                    .voter_set_fingerprint = leader.voter_set_fingerprint,
                    .membership_index = leader.raft_membership_index,
                };
            }
        }
        if (self.has_known_voter_set and
            self.known_membership_index >= self.fallback_membership_index)
        {
            return .{
                .voter_count_known = !self.ambiguous_known_voter_set,
                .voter_count = self.known_voter_count.?,
                .from_leader = false,
                .voter_set_known = !self.ambiguous_known_voter_set,
                .voter_set_fingerprint = self.known_voter_set_fingerprint,
                .membership_index = self.known_membership_index,
            };
        }
        return .{
            .voter_count_known = self.fallback_voter_count != null and
                !self.ambiguous_fallback_voter_count,
            .voter_count = self.fallback_voter_count orelse 0,
            .from_leader = false,
        };
    }
};

test "table manager leader evidence does not compare member-local relocation generations" {
    var evidence: GroupLeaderEvidence = .{};
    evidence.observe(102, .{
        .group_id = 77,
        .relocation_generation = 4,
        .local_leader = true,
        .local_voter = true,
        .raft_term = 9,
        .raft_membership_index = 80,
    });
    evidence.observe(105, .{
        .group_id = 77,
        .relocation_generation = 5,
        .local_voter = true,
        .raft_term = 9,
        .raft_membership_index = 80,
    });

    const leader = evidence.resolve() orelse return error.MissingLeader;
    try std.testing.expectEqual(@as(u64, 102), leader.store_id);
}

test "table manager voter set evidence is order independent when newer reports lack fingerprints" {
    const older_known: GroupStatusReport = .{
        .group_id = 1,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = [_]u8{0x11} ** voter_set_fingerprint_len,
        .raft_membership_index = 10,
    };
    const newer_unqualified: GroupStatusReport = .{
        .group_id = 1,
        .voter_count = 5,
        .raft_membership_index = 20,
    };

    var forward: VoterSetEvidence = .{};
    forward.observe(older_known);
    forward.observe(newer_unqualified);
    const forward_result = forward.resolve(null);

    var reverse: VoterSetEvidence = .{};
    reverse.observe(newer_unqualified);
    reverse.observe(older_known);
    const reverse_result = reverse.resolve(null);

    try std.testing.expectEqual(forward_result, reverse_result);
    try std.testing.expect(forward_result.voter_count_known);
    try std.testing.expectEqual(@as(u16, 5), forward_result.voter_count);
    try std.testing.expect(!forward_result.from_leader);
}

pub fn normalizedVoterCount(node_ids: []const u64, required_node_id: ?u64) usize {
    var count: usize = 0;
    for (node_ids, 0..) |node_id, index| {
        var first = true;
        for (node_ids[0..index]) |previous| {
            if (previous == node_id) {
                first = false;
                break;
            }
        }
        if (first) count += 1;
    }
    if (required_node_id) |required| {
        for (node_ids) |node_id| {
            if (node_id == required) return count;
        }
        count += 1;
    }
    return count;
}

/// Produces a canonical membership fingerprint without allocating. Raft voter
/// sets are unique and small, so an O(n^2) ordered scan is preferable to
/// allocating and sorting on every status report.
pub fn voterSetFingerprint(node_ids: []const u64, required_node_id: ?u64) VoterSetFingerprint {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-raft-voter-set-v1\x00");

    const count = normalizedVoterCount(node_ids, required_node_id);
    var count_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &count_bytes, @intCast(count), .big);
    hasher.update(&count_bytes);

    var previous: ?u64 = null;
    for (0..count) |_| {
        var next: ?u64 = null;
        for (node_ids) |node_id| {
            if (previous != null and node_id <= previous.?) continue;
            if (next == null or node_id < next.?) next = node_id;
        }
        if (required_node_id) |node_id| {
            if ((previous == null or node_id > previous.?) and (next == null or node_id < next.?)) {
                next = node_id;
            }
        }
        const node_id = next orelse unreachable;
        var node_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &node_bytes, node_id, .big);
        hasher.update(&node_bytes);
        previous = node_id;
    }

    var digest: VoterSetFingerprint = undefined;
    hasher.final(&digest);
    return digest;
}

pub const StoreStatusReport = struct {
    store_id: u64,
    live: bool = true,
    health_class: []const u8 = "healthy",
    capacity_bytes: u64 = 0,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    active_backfills: u32 = 0,
    backfill_progress_millis: u16 = 1000,
    group_statuses: []GroupStatusReport = &.{},
    runtime_statuses: []RuntimeGroupStatusReport = &.{},
};

pub const RuntimeEnrichmentStatusReport = struct {
    enabled: bool = false,
    lease_owned: bool = true,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
    target_sequence: u64 = 0,
    applied_sequence: u64 = 0,
    projection_checkpoint_status: []const u8 = "clean",
    projection_checkpoint_applied_sequence: u64 = 0,
    projection_checkpoint_generation: u64 = 0,
    projection_checkpoint_config_hash: u64 = 0,
    checkpoint_replay_tail_sequence_count: u64 = 0,
    processed_requests: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    consecutive_retry_count: u32 = 0,
    next_retry_at_ms: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
    worker_started: bool = false,
    stalled: bool = false,
    skip_by_hash_count: u64 = 0,
    skipped_source_count: u64 = 0,
    codec_decode_failures: u64 = 0,
    embed_batches_started: u64 = 0,
    embed_batches_completed: u64 = 0,
    embed_items_started: u64 = 0,
    embed_items_completed: u64 = 0,
    active_embed_batch_items: u64 = 0,
    active_embed_batch_bytes: u64 = 0,
    active_embed_batch_max_bytes: u64 = 0,
    active_embed_batch_started_ms: u64 = 0,
    last_embed_batch_items: u64 = 0,
    last_embed_batch_bytes: u64 = 0,
    last_embed_batch_max_bytes: u64 = 0,
    last_embed_batch_completed_ms: u64 = 0,
    last_embed_batch_ns: u64 = 0,
    total_embed_ns: u64 = 0,
    dense_artifact_bytes_written: u64 = 0,
    sparse_artifact_bytes_written: u64 = 0,
    chunk_artifact_bytes_written: u64 = 0,
    artifact_bytes_written: u64 = 0,
};

pub const RuntimeGroupStatusReport = struct {
    table_id: u64 = 0,
    table_name: []const u8 = "",
    group_id: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,
    /// Unix/realtime observation time. This report crosses process boundaries;
    /// monotonic clocks from different hosts are not comparable.
    updated_at_ns: u64 = 0,
    source: []const u8 = "unknown",
    freshness: []const u8 = "unknown",
    topology_generation: u64 = 0,
    lsm_root_generation: u64 = 0,
    status_generation: u64 = 0,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    disk_bytes_known: bool = false,
    created_at_millis: u64 = 0,
    index_count: u32 = 0,
    enrichment: RuntimeEnrichmentStatusReport = .{},
    async_indexing_active: bool = false,
    async_startup_active: bool = false,
    async_dense_catch_up_active: bool = false,
    async_bulk_coalescing_active: bool = false,
    doc_identity: RuntimeDocIdentityStatusReport = .{},
    doc_set_planning: RuntimeDocSetPlanningStatusReport = .{},
    indexes: []RuntimeIndexStatusReport = &.{},
};

pub const RuntimeDocIdentityStatusReport = struct {
    namespace_table_id: u64 = 0,
    namespace_shard_id: u64 = 0,
    namespace_range_id: u64 = 0,
    next_ordinal: u32 = 1,
    allocated_ordinals: u64 = 0,
    ordinal_capacity_remaining: u64 = 0,
    ordinal_capacity_exhausted: bool = false,
    rebuild_required: bool = false,
    state_rows: u64 = 0,
    live_ordinals: u64 = 0,
    tombstone_ordinals: u64 = 0,
    min_created_generation: u64 = 0,
    max_created_generation: u64 = 0,
    min_deleted_generation: u64 = 0,
    max_deleted_generation: u64 = 0,
    scanned_primary_docs: u64 = 0,
    primary_docs_missing_ordinals: u64 = 0,
    primary_docs_missing_identity_state: u64 = 0,
    primary_docs_with_tombstone_ordinals: u64 = 0,
    complete: bool = false,
};

pub const RuntimeDocSetPlanningStatusReport = struct {
    resolved_set_count: u64 = 0,
    all_set_count: u64 = 0,
    none_set_count: u64 = 0,
    doc_key_list_count: u64 = 0,
    ordinal_list_count: u64 = 0,
    ordinal_bitmap_count: u64 = 0,
    doc_key_list_docs: u64 = 0,
    ordinal_list_docs: u64 = 0,
    ordinal_bitmap_docs: u64 = 0,
    missing_ordinal_coverage_count: u64 = 0,
    bitmap_promotion_count: u64 = 0,
    unsupported_filter_shape_count: u64 = 0,
    stale_identity_generation_rejection_count: u64 = 0,
};

pub const RuntimeIndexStatusReport = struct {
    name: []const u8 = "",
    kind: []const u8 = "",
    /// Stable error name for an index artifact that cannot be trusted or
    /// loaded. This is index-scoped so one failed derived projection does not
    /// make the other indexes in the same group appear unhealthy.
    load_error: ?[]const u8 = null,
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    coverage_produced_count: u64 = 0,
    coverage_skipped_count: u64 = 0,
    coverage_terminal_failed_count: u64 = 0,
    coverage_generation: u64 = 0,
    coverage_config_hash: u64 = 0,
    coverage_identity_ready: bool = false,
    coverage_summary_ready: bool = true,
    backfill_active: bool = false,
    backfill_progress_millis: u16 = 0,
    replay_applied_sequence: u64 = 0,
    replay_target_sequence: u64 = 0,
    replay_catch_up_required: bool = false,
};

pub const SchemaProgressRecord = struct {
    table_id: u64,
    node_id: u64,
    schema_version: u32 = 0,
};

pub const RestoreProgressRecord = struct {
    table_id: u64,
    node_id: u64,
    group_id: u64,
    backup_id: []const u8,
    /// Immutable artifact namespace selected by the admitted backup manifest.
    /// This distinguishes repeated cluster-backup attempts that share a
    /// logical table backup ID and source location.
    artifact_backup_id: []const u8 = "",
    location: []const u8 = "",
    snapshot_path: []const u8 = "",
    artifact_sha256: []const u8 = "",
    primary_restored: bool = false,
    runtime_repair_complete: bool = false,
    phase: []const u8 = "",
    last_error: []const u8 = "",
    updated_at_ms: u64 = 0,
};

pub const ReplicationSourceStatusRecord = struct {
    table_id: u64,
    source_ordinal: u32,
    source_kind: []const u8,
    external_table: []const u8 = "",
    cutover_mode: []const u8 = "",
    slot_name: []const u8 = "",
    publication_name: []const u8 = "",
    phase: []const u8 = "configured",
    checkpoint: []const u8 = "",
    snapshot_offset: u64 = 0,
    prepared_checkpoint: []const u8 = "",
    stream_checkpoint: []const u8 = "",
    last_error: []const u8 = "",
    failure_class: []const u8 = "",
    lag_records: u64 = 0,
    lag_millis: u64 = 0,
    consecutive_failures: u64 = 0,
    last_source_commit_at_ms: u64 = 0,
    last_success_at_ms: u64 = 0,
    last_change_applied_at_ms: u64 = 0,
    /// Non-zero only after an exact-cutover ownership intent is replicated.
    /// Retries must present this identity and the matching configuration
    /// fingerprint before reclaiming the provider slot.
    cutover_intent_id: u64 = 0,
    /// Fresh for every provider mutation attempt. Unlike cutover_intent_id,
    /// this token is never reused after an authority handoff; the durable
    /// acknowledgement must match it before provider state may be changed.
    cutover_authority_id: u64 = 0,
    cutover_config_fingerprint: [std.crypto.hash.sha2.Sha256.digest_length]u8 =
        [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length,
    /// Authenticated PostgreSQL cluster, database, and database-incarnation
    /// identity. This deliberately excludes connection credentials.
    cutover_provider_identity: [std.crypto.hash.sha2.Sha256.digest_length]u8 =
        [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length,
    /// Provider resources from the authority superseded by the current claim.
    /// They remain durable until inactive cleanup succeeds; a newer claim is
    /// not admitted while this retirement is pending.
    retired_cutover_authority_id: u64 = 0,
    retired_slot_name: []const u8 = "",
    retired_publication_name: []const u8 = "",
    updated_at_ms: u64 = 0,
};

pub const ShuffleJoinLeaseRecord = struct {
    job_id: u64,
    owner_group_id: u64,
    expires_at_ms: u64,
};

pub const SplitIntent = struct {
    transition_id: u64,
    attempt_epoch: u64 = 0,
    table_id: u64,
    source_group_id: u64,
    destination_group_id: u64,
    split_key: []const u8,
    projected_source_range_end: ?[]const u8 = null,
    rollback_reason: ?[]const u8 = null,
    automatic: bool = false,
    projected: bool = false,
    projected_contract: ?transition_state.TransitionTableContract = null,
};

pub const MergeIntent = struct {
    transition_id: u64,
    table_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    rollback_reason: ?[]const u8 = null,
    automatic: bool = false,
    allow_doc_identity_reassignment: bool = false,
    projected: bool = false,
    projected_contract: ?transition_state.TransitionTableContract = null,
};

pub const TableManager = struct {
    alloc: std.mem.Allocator,
    databases: std.AutoHashMapUnmanaged(u64, DatabaseRecord) = .empty,
    namespaces: std.AutoHashMapUnmanaged(u64, NamespaceRecord) = .empty,
    tablespaces: std.AutoHashMapUnmanaged(u64, TablespaceRecord) = .empty,
    sequences: std.AutoHashMapUnmanaged(u64, SequenceRecord) = .empty,
    tables: std.AutoHashMapUnmanaged(u64, TableRecord) = .empty,
    ranges: std.AutoHashMapUnmanaged(u64, RangeRecord) = .empty,
    foreign_key_ref_ranges: std.ArrayListUnmanaged(ForeignKeyReferenceRangeRecord) = .empty,
    unique_constraint_ranges: std.ArrayListUnmanaged(UniqueConstraintRangeRecord) = .empty,
    secondary_index_rebuild_ranges: std.ArrayListUnmanaged(SecondaryIndexRebuildRangeRecord) = .empty,
    schema_rewrite_jobs: std.ArrayListUnmanaged(SchemaRewriteJobRecord) = .empty,
    table_emptying_jobs: std.ArrayListUnmanaged(TableEmptyingJobRecord) = .empty,
    split_intents: std.AutoHashMapUnmanaged(u64, SplitIntent) = .empty,
    merge_intents: std.AutoHashMapUnmanaged(u64, MergeIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) TableManager {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *TableManager) void {
        var database_it = self.databases.valueIterator();
        while (database_it.next()) |database| freeDatabase(self.alloc, database.*);
        self.databases.deinit(self.alloc);

        var namespace_it = self.namespaces.valueIterator();
        while (namespace_it.next()) |namespace| freeNamespace(self.alloc, namespace.*);
        self.namespaces.deinit(self.alloc);

        var tablespace_it = self.tablespaces.valueIterator();
        while (tablespace_it.next()) |tablespace| freeTablespace(self.alloc, tablespace.*);
        self.tablespaces.deinit(self.alloc);

        var sequence_it = self.sequences.valueIterator();
        while (sequence_it.next()) |sequence| freeSequence(self.alloc, sequence.*);
        self.sequences.deinit(self.alloc);

        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| freeTable(self.alloc, table.*);
        self.tables.deinit(self.alloc);

        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| freeRange(self.alloc, range.*);
        self.ranges.deinit(self.alloc);

        for (self.foreign_key_ref_ranges.items) |record| freeForeignKeyReferenceRange(self.alloc, record);
        self.foreign_key_ref_ranges.deinit(self.alloc);

        for (self.unique_constraint_ranges.items) |record| freeUniqueConstraintRange(self.alloc, record);
        self.unique_constraint_ranges.deinit(self.alloc);

        for (self.secondary_index_rebuild_ranges.items) |record| freeSecondaryIndexRebuildRange(self.alloc, record);
        self.secondary_index_rebuild_ranges.deinit(self.alloc);

        for (self.schema_rewrite_jobs.items) |record| freeSchemaRewriteJob(self.alloc, record);
        self.schema_rewrite_jobs.deinit(self.alloc);

        for (self.table_emptying_jobs.items) |record| freeTableEmptyingJob(self.alloc, record);
        self.table_emptying_jobs.deinit(self.alloc);

        var split_it = self.split_intents.valueIterator();
        while (split_it.next()) |intent| freeSplitIntent(self.alloc, intent.*);
        self.split_intents.deinit(self.alloc);

        var merge_it = self.merge_intents.valueIterator();
        while (merge_it.next()) |intent| freeMergeIntent(self.alloc, intent.*);
        self.merge_intents.deinit(self.alloc);

        self.* = undefined;
    }

    pub fn ensureDefaultCatalog(self: *TableManager) !void {
        const database_id = deriveDatabaseId(default_database_name);
        try self.upsertDatabase(.{
            .database_id = database_id,
            .name = default_database_name,
        });
        try self.upsertNamespace(.{
            .namespace_id = deriveNamespaceId(database_id, default_namespace_name),
            .database_id = database_id,
            .name = default_namespace_name,
        });
    }

    pub fn upsertDatabase(self: *TableManager, record: DatabaseRecord) !void {
        const owned = try cloneDatabase(self.alloc, record);
        errdefer freeDatabase(self.alloc, owned);
        if (self.databases.getPtr(record.database_id)) |existing| {
            freeDatabase(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.databases.put(self.alloc, record.database_id, owned);
    }

    pub fn upsertNamespace(self: *TableManager, record: NamespaceRecord) !void {
        if (!self.databases.contains(record.database_id)) return error.UnknownDatabase;
        const owned = try cloneNamespace(self.alloc, record);
        errdefer freeNamespace(self.alloc, owned);
        if (self.namespaces.getPtr(record.namespace_id)) |existing| {
            freeNamespace(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.namespaces.put(self.alloc, record.namespace_id, owned);
    }

    pub fn upsertTablespace(self: *TableManager, record: TablespaceRecord) !void {
        const owned = try cloneTablespace(self.alloc, record);
        errdefer freeTablespace(self.alloc, owned);
        if (self.tablespaces.getPtr(record.tablespace_id)) |existing| {
            freeTablespace(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.tablespaces.put(self.alloc, record.tablespace_id, owned);
    }

    pub fn upsertSequence(self: *TableManager, record: SequenceRecord) !void {
        try self.ensureCatalogForObject(record.database_name, record.namespace_name);
        if (record.sequence_id != deriveSequenceId(record.database_name, record.namespace_name, record.name)) return error.InvalidSequenceCatalog;
        const owned = try cloneSequence(self.alloc, record);
        errdefer freeSequence(self.alloc, owned);
        if (self.sequences.getPtr(record.sequence_id)) |existing| {
            freeSequence(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.sequences.put(self.alloc, record.sequence_id, owned);
    }

    pub fn removeSequence(self: *TableManager, sequence_id: u64) !SequenceRecord {
        const kv = self.sequences.fetchRemove(sequence_id) orelse return error.SequenceNotFound;
        return kv.value;
    }

    pub fn allocateSequenceValue(
        self: *TableManager,
        alloc: std.mem.Allocator,
        database_name: []const u8,
        namespace_name: []const u8,
        sequence_name: []const u8,
    ) !i64 {
        const sequence_id = deriveSequenceId(database_name, namespace_name, sequence_name);
        const record = self.sequences.getPtr(sequence_id) orelse return error.SequenceNotFound;
        const next_value = try sequenceNextValueFromRecord(alloc, record.*);
        record.last_value = next_value;
        return next_value;
    }

    pub fn tableEmptyingIdentityAllocatorResetTargetsAlloc(
        self: *TableManager,
        alloc: std.mem.Allocator,
        request: TableEmptyingIdentityAllocatorResetRequest,
    ) ![]SequenceIdentityAllocatorReset {
        try self.validateTableEmptyingIdentityAllocatorReset(request);

        var targets = std.ArrayListUnmanaged(SequenceIdentityAllocatorReset).empty;
        errdefer targets.deinit(alloc);

        for (request.affected_table_ids) |table_id| {
            const table = self.tables.get(table_id) orelse return error.UnknownTable;
            try self.appendSequenceIdentityAllocatorResetTargetsForTableAlloc(alloc, &targets, table);
        }

        return try targets.toOwnedSlice(alloc);
    }

    pub fn resetIdentityAllocatorsForTableEmptyingBarrier(
        self: *TableManager,
        alloc: std.mem.Allocator,
        request: TableEmptyingIdentityAllocatorResetRequest,
    ) !usize {
        const targets = try self.tableEmptyingIdentityAllocatorResetTargetsAlloc(alloc, request);
        defer alloc.free(targets);
        var reset_count: usize = 0;
        for (targets) |target| {
            const record = self.sequences.getPtr(target.sequence_id) orelse return error.SequenceNotFound;
            if (record.last_value != target.reset_last_value or record.last_allocation_id != 0) {
                record.last_value = target.reset_last_value;
                record.last_allocation_id = 0;
                reset_count += 1;
            }
        }
        return reset_count;
    }

    pub fn upsertTable(self: *TableManager, record: TableRecord) !void {
        try self.ensureCatalogForTable(record);
        const owned = try cloneTable(self.alloc, record);
        errdefer freeTable(self.alloc, owned);
        if (self.tables.getPtr(record.table_id)) |existing| {
            freeTable(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.tables.put(self.alloc, record.table_id, owned);
    }

    fn appendSequenceIdentityAllocatorResetTargetsForTableAlloc(
        self: *TableManager,
        alloc: std.mem.Allocator,
        targets: *std.ArrayListUnmanaged(SequenceIdentityAllocatorReset),
        table: TableRecord,
    ) !void {
        var parsed_schema = schema_mod.parseValidatedTableSchema(alloc, table.schema_json) catch |err| switch (err) {
            error.InvalidSchemaUpdateRequest => return,
            else => return err,
        };
        defer parsed_schema.deinit(alloc);
        const runtime = schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema) catch |err| switch (err) {
            error.InvalidSchemaUpdateRequest => return,
            else => return err,
        };
        defer runtime_schema.freeSchema(alloc, runtime);

        for (runtime.relational_columns) |column| {
            const default_value = column.default_value orelse continue;
            if (default_value.kind != .sequence_next) continue;
            const sequence_id = try sequenceIdForTableDefaultAlloc(alloc, table, default_value.value_json);
            if (sequenceIdentityAllocatorResetTargetIndex(targets.items, sequence_id) != null) continue;
            const sequence = self.sequences.get(sequence_id) orelse return error.SequenceNotFound;
            try targets.append(alloc, .{
                .sequence_id = sequence_id,
                .reset_last_value = try sequenceInitialLastValueFromOptionsJson(alloc, sequence.options_json),
            });
        }

        var sequence_it = self.sequences.valueIterator();
        while (sequence_it.next()) |sequence| {
            if ((try sequenceOwnedByTableColumn(alloc, sequence.*, table, runtime)) == null) continue;
            if (sequenceIdentityAllocatorResetTargetIndex(targets.items, sequence.sequence_id) != null) continue;
            try targets.append(alloc, .{
                .sequence_id = sequence.sequence_id,
                .reset_last_value = try sequenceInitialLastValueFromOptionsJson(alloc, sequence.options_json),
            });
        }
    }

    pub fn applyTableCatalogUpdateWithSchemaRewriteJobs(self: *TableManager, request: TableCatalogUpdateWithSchemaRewriteJobsRequest) !void {
        try self.applyTableCatalogBatchUpdateWithSchemaRewriteJobs(.{
            .tables = &[_]TableRecord{request.table},
            .schema_rewrite_jobs = request.schema_rewrite_jobs,
        });
    }

    pub fn applyTableCatalogBatchUpdateWithSchemaRewriteJobs(self: *TableManager, request: TableCatalogBatchUpdateWithSchemaRewriteJobsRequest) !void {
        try self.validateTableCatalogBatchUpdateWithSchemaRewriteJobs(request);

        for (request.tables) |table| try self.upsertTable(table);
        for (request.schema_rewrite_jobs) |job| try self.upsertSchemaRewriteJob(job);
    }

    pub fn applyTableCatalogDropWithSchemaRewriteJobs(self: *TableManager, request: TableCatalogDropWithSchemaRewriteJobsRequest) !void {
        try self.validateTableCatalogDropWithSchemaRewriteJobs(request);
        if (request.table_updates.len > 0) {
            try self.applyTableCatalogBatchUpdateWithSchemaRewriteJobs(.{
                .tables = request.table_updates,
                .schema_rewrite_jobs = request.schema_rewrite_jobs,
            });
        } else if (request.schema_rewrite_jobs.len > 0) {
            return error.InvalidSchemaRewriteJob;
        }
        for (request.range_group_ids) |group_id| _ = self.removeRange(group_id);
        for (request.sequence_ids) |sequence_id| {
            const removed = try self.removeSequence(sequence_id);
            freeSequence(self.alloc, removed);
        }
        if (!self.removeTable(request.table_id)) return error.UnknownTable;
    }

    fn validateTableCatalogBatchUpdateWithSchemaRewriteJobs(self: *TableManager, request: TableCatalogBatchUpdateWithSchemaRewriteJobsRequest) !void {
        if (request.tables.len == 0) return error.UnknownTable;
        var validator = TableManager.init(self.alloc);
        defer validator.deinit();

        for (request.tables, 0..) |table, i| {
            if (table.table_id == 0) return error.UnknownTable;
            if (schemaRewriteGenerationForSchemaJson(table.schema_json) == 0) return error.InvalidSchemaRewriteGeneration;
            for (request.tables[0..i]) |previous| {
                if (previous.table_id == table.table_id) return error.UnknownTable;
            }
            try validator.upsertTable(table);
        }
        for (request.schema_rewrite_jobs, 0..) |job, i| {
            try validateSchemaRewriteJobForBatchUpdate(request.tables, job);
            try validator.upsertSchemaRewriteJob(job);
            for (request.schema_rewrite_jobs[0..i]) |previous| {
                if (previous.job_id == job.job_id) return error.InvalidSchemaRewriteJob;
            }
        }
    }

    fn validateTableCatalogDropWithSchemaRewriteJobs(self: *TableManager, request: TableCatalogDropWithSchemaRewriteJobsRequest) !void {
        if (request.table_id == 0) return error.UnknownTable;
        if (!self.tables.contains(request.table_id)) return error.UnknownTable;
        if (request.table_updates.len > 0 or request.schema_rewrite_jobs.len > 0) {
            try self.validateTableCatalogBatchUpdateWithSchemaRewriteJobs(.{
                .tables = request.table_updates,
                .schema_rewrite_jobs = request.schema_rewrite_jobs,
            });
        }
        for (request.table_updates) |table| {
            if (table.table_id == request.table_id) return error.UnknownTable;
        }
        for (request.sequence_ids, 0..) |sequence_id, i| {
            if (!self.sequences.contains(sequence_id)) return error.SequenceNotFound;
            for (request.sequence_ids[0..i]) |previous| {
                if (previous == sequence_id) return error.SequenceNotFound;
            }
        }
        for (request.range_group_ids, 0..) |group_id, i| {
            const range = self.ranges.get(group_id) orelse return error.UnknownRange;
            if (range.table_id != request.table_id) return error.UnknownRange;
            for (request.range_group_ids[0..i]) |previous| {
                if (previous == group_id) return error.UnknownRange;
            }
        }
        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| {
            if (range.table_id != request.table_id) continue;
            if (std.mem.indexOfScalar(u64, request.range_group_ids, range.group_id) == null) return error.UnknownRange;
        }
    }

    fn validateSchemaRewriteJobForTableUpdate(table_id: u64, schema_generation: u64, record: SchemaRewriteJobRecord) !void {
        if (record.table_id != table_id) return error.InvalidSchemaRewriteJob;
        if (record.schema_generation != schema_generation) return error.InvalidSchemaRewriteGeneration;
        try group_ids.requireDataGroupId(record.group_id);
        if (record.job_id == 0) return error.InvalidSchemaRewriteJob;
    }

    fn validateSchemaRewriteJobForBatchUpdate(tables: []const TableRecord, record: SchemaRewriteJobRecord) !void {
        for (tables) |table| {
            if (record.table_id != table.table_id) continue;
            return try validateSchemaRewriteJobForTableUpdate(
                table.table_id,
                schemaRewriteGenerationForSchemaJson(table.schema_json),
                record,
            );
        }
        return error.InvalidSchemaRewriteJob;
    }

    fn ensureCatalogForTable(self: *TableManager, record: TableRecord) !void {
        try self.ensureCatalogForObject(record.database_name, record.namespace_name);
    }

    fn ensureCatalogForObject(self: *TableManager, database_name: []const u8, namespace_name: []const u8) !void {
        const database_id = deriveDatabaseId(database_name);
        if (!self.databases.contains(database_id)) {
            try self.upsertDatabase(.{
                .database_id = database_id,
                .name = database_name,
            });
        }
        const namespace_id = deriveNamespaceId(database_id, namespace_name);
        if (!self.namespaces.contains(namespace_id)) {
            try self.upsertNamespace(.{
                .namespace_id = namespace_id,
                .database_id = database_id,
                .name = namespace_name,
            });
        }
    }

    pub fn upsertRange(self: *TableManager, record: RangeRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        const table = self.tables.get(record.table_id) orelse return error.UnknownTable;
        _ = table;

        var normalized = record;
        if (normalized.range_id == 0) normalized.range_id = normalized.group_id;
        if (self.foreignKeyReferenceRangeGroupExists(record.group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (self.uniqueConstraintRangeGroupExists(record.group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (self.secondaryIndexRebuildRangeGroupExists(record.group_id)) return error.SecondaryIndexRebuildRangeGroupCollision;
        const owned = try cloneRange(self.alloc, normalized);
        errdefer freeRange(self.alloc, owned);
        if (self.ranges.getPtr(record.group_id)) |existing| {
            freeRange(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.ranges.put(self.alloc, record.group_id, owned);
    }

    pub fn upsertForeignKeyReferenceRange(self: *TableManager, record: ForeignKeyReferenceRangeRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        if (!self.tables.contains(record.child_table_id)) return error.UnknownChildTable;
        if (!self.tables.contains(record.parent_table_id)) return error.UnknownParentTable;
        if (record.constraint_name.len == 0) return error.InvalidForeignKeyConstraint;
        if (!foreignKeyReferenceRangeStateValid(record.state)) return error.InvalidForeignKeyReferenceRangeState;
        if (self.ranges.contains(record.group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (self.uniqueConstraintRangeGroupExists(record.group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (self.secondaryIndexRebuildRangeGroupExists(record.group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (record.end_parent_key) |end_parent_key| {
            if (std.mem.order(u8, record.start_parent_key, end_parent_key) != .lt) return error.InvalidForeignKeyReferenceRange;
        }
        for (self.foreign_key_ref_ranges.items) |existing| {
            if (foreignKeyReferenceRangeIdentityMatches(existing, record)) continue;
            if (!foreignKeyReferenceRangeConstraintMatches(existing, record)) continue;
            if (foreignKeyReferenceRangesOverlap(existing, record)) return error.ForeignKeyReferenceRangeOverlap;
        }

        var normalized = record;
        if (normalized.range_id == 0) normalized.range_id = normalized.group_id;
        const owned = try cloneForeignKeyReferenceRange(self.alloc, normalized);
        errdefer freeForeignKeyReferenceRange(self.alloc, owned);
        for (self.foreign_key_ref_ranges.items) |*existing| {
            if (!foreignKeyReferenceRangeIdentityMatches(existing.*, record)) continue;
            freeForeignKeyReferenceRange(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.foreign_key_ref_ranges.append(self.alloc, owned);
    }

    pub fn upsertUniqueConstraintRange(self: *TableManager, record: UniqueConstraintRangeRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        if (!self.tables.contains(record.table_id)) return error.UnknownTable;
        if (record.constraint_name.len == 0) return error.InvalidUniqueConstraint;
        if (!uniqueConstraintRangeStateValid(record.state)) return error.InvalidUniqueConstraintRangeState;
        if (self.ranges.contains(record.group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (self.foreignKeyReferenceRangeGroupExists(record.group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (self.secondaryIndexRebuildRangeGroupExists(record.group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (record.end_encoded_value) |end_encoded_value| {
            if (std.mem.order(u8, record.start_encoded_value, end_encoded_value) != .lt) return error.InvalidUniqueConstraintRange;
        }
        for (self.unique_constraint_ranges.items) |existing| {
            if (uniqueConstraintRangeIdentityMatches(existing, record)) continue;
            if (existing.group_id == record.group_id) return error.UniqueConstraintRangeGroupCollision;
            if (!uniqueConstraintRangeConstraintMatches(existing, record)) continue;
            if (uniqueConstraintRangesOverlap(existing, record)) return error.UniqueConstraintRangeOverlap;
        }

        var normalized = record;
        if (normalized.range_id == 0) normalized.range_id = normalized.group_id;
        const owned = try cloneUniqueConstraintRange(self.alloc, normalized);
        errdefer freeUniqueConstraintRange(self.alloc, owned);
        for (self.unique_constraint_ranges.items) |*existing| {
            if (!uniqueConstraintRangeIdentityMatches(existing.*, record)) continue;
            freeUniqueConstraintRange(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.unique_constraint_ranges.append(self.alloc, owned);
    }

    pub fn upsertSecondaryIndexRebuildRange(self: *TableManager, record: SecondaryIndexRebuildRangeRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        if (!self.tables.contains(record.table_id)) return error.UnknownTable;
        if (record.index_name.len == 0) return error.InvalidSecondaryIndex;
        if (record.index_generation == 0) return error.InvalidSecondaryIndexGeneration;
        if (!secondaryIndexRebuildRangeStateValid(record.state)) return error.InvalidSecondaryIndexRebuildRangeState;
        if (self.ranges.contains(record.group_id)) return error.SecondaryIndexRebuildRangeGroupCollision;
        if (self.foreignKeyReferenceRangeGroupExists(record.group_id)) return error.SecondaryIndexRebuildRangeGroupCollision;
        if (self.uniqueConstraintRangeGroupExists(record.group_id)) return error.SecondaryIndexRebuildRangeGroupCollision;
        if (record.end_row_key) |end_row_key| {
            if (std.mem.order(u8, record.start_row_key, end_row_key) != .lt) return error.InvalidSecondaryIndexRebuildRange;
        }
        for (self.secondary_index_rebuild_ranges.items) |existing| {
            if (secondaryIndexRebuildRangeIdentityMatches(existing, record)) continue;
            if (existing.group_id == record.group_id) return error.SecondaryIndexRebuildRangeGroupCollision;
            if (!secondaryIndexRebuildRangeIndexMatches(existing, record)) continue;
            if (secondaryIndexRebuildRangesOverlap(existing, record)) return error.SecondaryIndexRebuildRangeOverlap;
        }

        const owned = try cloneSecondaryIndexRebuildRange(self.alloc, record);
        errdefer freeSecondaryIndexRebuildRange(self.alloc, owned);
        for (self.secondary_index_rebuild_ranges.items) |*existing| {
            if (!secondaryIndexRebuildRangeIdentityMatches(existing.*, record)) continue;
            freeSecondaryIndexRebuildRange(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.secondary_index_rebuild_ranges.append(self.alloc, owned);
    }

    pub fn upsertSchemaRewriteJob(self: *TableManager, record: SchemaRewriteJobRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        if (!self.tables.contains(record.table_id)) return error.UnknownTable;
        if (record.job_id == 0) return error.InvalidSchemaRewriteJob;
        if (record.schema_generation == 0) return error.InvalidSchemaRewriteGeneration;
        if (record.action.len == 0 or record.reason.len == 0) return error.InvalidSchemaRewriteJob;
        if (!schemaRewriteJobStateValid(record.state)) return error.InvalidSchemaRewriteJobState;
        const has_expression_rewrite = record.expression != null or record.target_column.len != 0;
        const has_row_rewrite = record.rewrite_renames.len != 0 or record.rewrite_drops.len != 0;
        if (has_expression_rewrite) {
            if (record.expression == null or record.target_column.len == 0) return error.InvalidSchemaRewriteExpression;
            if (has_row_rewrite or record.full_row_rewrite) return error.InvalidSchemaRewriteExpression;
        } else if (record.full_row_rewrite and has_row_rewrite) {
            return error.InvalidSchemaRewriteExpression;
        } else if (std.mem.eql(u8, record.action, "rewrite") and std.mem.eql(u8, record.reason, "row_images") and !has_row_rewrite and !record.full_row_rewrite) {
            return error.InvalidSchemaRewriteExpression;
        }
        for (record.rewrite_renames) |rename| {
            if (rename.old_path.len == 0 or rename.new_path.len == 0) return error.InvalidSchemaRewriteExpression;
            if (std.mem.eql(u8, rename.old_path, rename.new_path)) return error.InvalidSchemaRewriteExpression;
        }
        for (record.rewrite_drops) |drop| {
            if (drop.len == 0) return error.InvalidSchemaRewriteExpression;
        }
        if (record.end_row_key) |end_row_key| {
            if (std.mem.order(u8, record.start_row_key, end_row_key) != .lt) return error.InvalidSchemaRewriteJobRange;
        }

        const owned = try cloneSchemaRewriteJob(self.alloc, record);
        errdefer freeSchemaRewriteJob(self.alloc, owned);
        for (self.schema_rewrite_jobs.items) |*existing| {
            if (existing.job_id != record.job_id) continue;
            freeSchemaRewriteJob(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.schema_rewrite_jobs.append(self.alloc, owned);
    }

    pub fn upsertTableEmptyingJob(self: *TableManager, record: TableEmptyingJobRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        if (!self.tables.contains(record.table_id)) return error.UnknownTable;
        if (record.job_id == 0) return error.InvalidTableEmptyingJob;
        if (record.schema_generation == 0) return error.InvalidTableEmptyingGeneration;
        if (!tableEmptyingJobStateValid(record.state)) return error.InvalidTableEmptyingJobState;
        if (record.end_row_key) |end_row_key| {
            if (std.mem.order(u8, record.start_row_key, end_row_key) != .lt) return error.InvalidTableEmptyingJob;
        }
        if (!tableEmptyingAffectedTableIdsCanonicalValid(record.table_id, record.affected_table_ids)) return error.InvalidTableEmptyingJob;
        for (record.affected_table_ids) |table_id| {
            if (!self.tables.contains(table_id)) return error.UnknownTable;
        }

        const owned = try cloneTableEmptyingJob(self.alloc, record);
        errdefer freeTableEmptyingJob(self.alloc, owned);
        for (self.table_emptying_jobs.items) |*existing| {
            if (existing.job_id != record.job_id) continue;
            freeTableEmptyingJob(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.table_emptying_jobs.append(self.alloc, owned);
    }

    pub fn clearTopology(self: *TableManager) void {
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| freeTable(self.alloc, table.*);
        self.tables.clearRetainingCapacity();

        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| freeRange(self.alloc, range.*);
        self.ranges.clearRetainingCapacity();

        for (self.foreign_key_ref_ranges.items) |record| freeForeignKeyReferenceRange(self.alloc, record);
        self.foreign_key_ref_ranges.clearRetainingCapacity();

        for (self.unique_constraint_ranges.items) |record| freeUniqueConstraintRange(self.alloc, record);
        self.unique_constraint_ranges.clearRetainingCapacity();

        for (self.secondary_index_rebuild_ranges.items) |record| freeSecondaryIndexRebuildRange(self.alloc, record);
        self.secondary_index_rebuild_ranges.clearRetainingCapacity();

        for (self.schema_rewrite_jobs.items) |record| freeSchemaRewriteJob(self.alloc, record);
        self.schema_rewrite_jobs.clearRetainingCapacity();

        for (self.table_emptying_jobs.items) |record| freeTableEmptyingJob(self.alloc, record);
        self.table_emptying_jobs.clearRetainingCapacity();
    }

    pub fn replaceTopology(self: *TableManager, tables: []const TableRecord, ranges: []const RangeRecord) !void {
        try self.replaceTopologyWithForeignKeyReferenceRanges(tables, ranges, &.{});
    }

    pub fn replaceTopologyWithForeignKeyReferenceRanges(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
    ) !void {
        try self.replaceTopologyWithConstraintOwnerRanges(tables, ranges, foreign_key_ref_ranges, &.{});
    }

    pub fn replaceTopologyWithConstraintOwnerRanges(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
        unique_constraint_ranges: []const UniqueConstraintRangeRecord,
    ) !void {
        try self.replaceTopologyWithDerivedRanges(tables, ranges, foreign_key_ref_ranges, unique_constraint_ranges, &.{});
    }

    pub fn replaceTopologyWithDerivedRanges(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
        unique_constraint_ranges: []const UniqueConstraintRangeRecord,
        secondary_index_rebuild_ranges: []const SecondaryIndexRebuildRangeRecord,
    ) !void {
        try self.replaceTopologyWithDerivedWork(tables, ranges, foreign_key_ref_ranges, unique_constraint_ranges, secondary_index_rebuild_ranges, &.{}, &.{});
    }

    pub fn replaceTopologyWithDerivedWork(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
        unique_constraint_ranges: []const UniqueConstraintRangeRecord,
        secondary_index_rebuild_ranges: []const SecondaryIndexRebuildRangeRecord,
        schema_rewrite_jobs: []const SchemaRewriteJobRecord,
        table_emptying_jobs: []const TableEmptyingJobRecord,
    ) !void {
        self.clearTopology();
        for (tables) |record| try self.upsertTable(record);
        for (ranges) |record| try self.upsertRange(record);
        for (foreign_key_ref_ranges) |record| try self.upsertForeignKeyReferenceRange(record);
        for (unique_constraint_ranges) |record| try self.upsertUniqueConstraintRange(record);
        for (secondary_index_rebuild_ranges) |record| try self.upsertSecondaryIndexRebuildRange(record);
        for (schema_rewrite_jobs) |record| try self.upsertSchemaRewriteJob(record);
        for (table_emptying_jobs) |record| try self.upsertTableEmptyingJob(record);
    }

    pub const ProjectedTopologyLoadResult = struct {
        skipped_orphan_ranges: usize = 0,
    };

    pub fn replaceProjectedTopology(self: *TableManager, tables: []const TableRecord, ranges: []const RangeRecord) !ProjectedTopologyLoadResult {
        return try self.replaceProjectedTopologyWithForeignKeyReferenceRanges(tables, ranges, &.{});
    }

    pub fn replaceProjectedTopologyWithForeignKeyReferenceRanges(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
    ) !ProjectedTopologyLoadResult {
        return try self.replaceProjectedTopologyWithConstraintOwnerRanges(tables, ranges, foreign_key_ref_ranges, &.{});
    }

    pub fn replaceProjectedTopologyWithConstraintOwnerRanges(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
        unique_constraint_ranges: []const UniqueConstraintRangeRecord,
    ) !ProjectedTopologyLoadResult {
        return try self.replaceProjectedTopologyWithDerivedRanges(tables, ranges, foreign_key_ref_ranges, unique_constraint_ranges, &.{});
    }

    pub fn replaceProjectedTopologyWithDerivedRanges(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
        unique_constraint_ranges: []const UniqueConstraintRangeRecord,
        secondary_index_rebuild_ranges: []const SecondaryIndexRebuildRangeRecord,
    ) !ProjectedTopologyLoadResult {
        return try self.replaceProjectedTopologyWithDerivedWork(tables, ranges, foreign_key_ref_ranges, unique_constraint_ranges, secondary_index_rebuild_ranges, &.{}, &.{});
    }

    pub fn replaceProjectedTopologyWithDerivedWork(
        self: *TableManager,
        tables: []const TableRecord,
        ranges: []const RangeRecord,
        foreign_key_ref_ranges: []const ForeignKeyReferenceRangeRecord,
        unique_constraint_ranges: []const UniqueConstraintRangeRecord,
        secondary_index_rebuild_ranges: []const SecondaryIndexRebuildRangeRecord,
        schema_rewrite_jobs: []const SchemaRewriteJobRecord,
        table_emptying_jobs: []const TableEmptyingJobRecord,
    ) !ProjectedTopologyLoadResult {
        self.clearTopology();
        for (tables) |record| try self.upsertTable(record);

        var result: ProjectedTopologyLoadResult = .{};
        for (ranges) |record| {
            if (!self.tables.contains(record.table_id)) {
                result.skipped_orphan_ranges += 1;
                continue;
            }
            try self.upsertRange(record);
        }
        for (foreign_key_ref_ranges) |record| {
            if (!self.tables.contains(record.child_table_id) or !self.tables.contains(record.parent_table_id)) {
                result.skipped_orphan_ranges += 1;
                continue;
            }
            try self.upsertForeignKeyReferenceRange(record);
        }
        for (unique_constraint_ranges) |record| {
            if (!self.tables.contains(record.table_id)) {
                result.skipped_orphan_ranges += 1;
                continue;
            }
            try self.upsertUniqueConstraintRange(record);
        }
        for (secondary_index_rebuild_ranges) |record| {
            if (!self.tables.contains(record.table_id)) {
                result.skipped_orphan_ranges += 1;
                continue;
            }
            try self.upsertSecondaryIndexRebuildRange(record);
        }
        for (schema_rewrite_jobs) |record| {
            if (!self.tables.contains(record.table_id)) {
                result.skipped_orphan_ranges += 1;
                continue;
            }
            try self.upsertSchemaRewriteJob(record);
        }
        for (table_emptying_jobs) |record| {
            if (!self.tables.contains(record.table_id)) {
                result.skipped_orphan_ranges += 1;
                continue;
            }
            try self.upsertTableEmptyingJob(record);
        }
        return result;
    }

    pub fn removeTable(self: *TableManager, table_id: u64) bool {
        const removed = self.tables.fetchRemove(table_id);
        if (removed) |entry| {
            freeTable(self.alloc, entry.value);
            _ = self.removeForeignKeyReferenceRangesForTable(table_id);
            _ = self.removeUniqueConstraintRangesForTable(table_id);
            _ = self.removeSecondaryIndexRebuildRangesForTable(table_id);
            _ = self.removeSchemaRewriteJobsForTable(table_id);
            _ = self.removeTableEmptyingJobsForTable(table_id);
            return true;
        }
        return false;
    }

    pub fn removeNamespace(self: *TableManager, namespace_id: u64) !bool {
        const namespace = self.namespaces.get(namespace_id) orelse return false;
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| {
            if (deriveDatabaseId(table.database_name) != namespace.database_id) continue;
            if (std.mem.eql(u8, table.namespace_name, namespace.name)) return error.NamespaceNotEmpty;
        }
        var sequence_it = self.sequences.valueIterator();
        while (sequence_it.next()) |sequence| {
            if (deriveDatabaseId(sequence.database_name) != namespace.database_id) continue;
            if (std.mem.eql(u8, sequence.namespace_name, namespace.name)) return error.NamespaceNotEmpty;
        }
        const removed = self.namespaces.fetchRemove(namespace_id) orelse return false;
        freeNamespace(self.alloc, removed.value);
        return true;
    }

    pub fn removeDatabase(self: *TableManager, database_id: u64) !bool {
        var namespace_it = self.namespaces.valueIterator();
        while (namespace_it.next()) |namespace| {
            if (namespace.database_id == database_id) return error.DatabaseNotEmpty;
        }
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| {
            if (deriveDatabaseId(table.database_name) == database_id) return error.DatabaseNotEmpty;
        }
        var sequence_it = self.sequences.valueIterator();
        while (sequence_it.next()) |sequence| {
            if (deriveDatabaseId(sequence.database_name) == database_id) return error.DatabaseNotEmpty;
        }
        const removed = self.databases.fetchRemove(database_id) orelse return false;
        freeDatabase(self.alloc, removed.value);
        return true;
    }

    pub fn removeTablespace(self: *TableManager, tablespace_id: u64) !bool {
        const removed = self.tablespaces.fetchRemove(tablespace_id) orelse return false;
        freeTablespace(self.alloc, removed.value);
        return true;
    }

    pub fn removeTableTopology(self: *TableManager, table_id: u64) usize {
        var removed_ranges: usize = 0;
        var to_remove = std.ArrayListUnmanaged(u64).empty;
        defer to_remove.deinit(self.alloc);

        var it = self.ranges.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.table_id != table_id) continue;
            to_remove.append(self.alloc, entry.key_ptr.*) catch continue;
        }
        for (to_remove.items) |group_id| {
            if (self.removeRange(group_id)) removed_ranges += 1;
        }
        _ = self.removeTable(table_id);
        return removed_ranges;
    }

    pub fn removeRange(self: *TableManager, group_id: u64) bool {
        const removed = self.ranges.fetchRemove(group_id);
        if (removed) |entry| {
            freeRange(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    pub fn removeForeignKeyReferenceRangesForTable(self: *TableManager, table_id: u64) usize {
        var removed: usize = 0;
        var i: usize = 0;
        while (i < self.foreign_key_ref_ranges.items.len) {
            const record = self.foreign_key_ref_ranges.items[i];
            if (record.child_table_id != table_id and record.parent_table_id != table_id) {
                i += 1;
                continue;
            }
            freeForeignKeyReferenceRange(self.alloc, record);
            _ = self.foreign_key_ref_ranges.orderedRemove(i);
            removed += 1;
        }
        return removed;
    }

    pub fn removeForeignKeyReferenceRange(
        self: *TableManager,
        child_table_id: u64,
        constraint_name: []const u8,
        parent_table_id: u64,
        start_parent_key: []const u8,
    ) bool {
        for (self.foreign_key_ref_ranges.items, 0..) |record, i| {
            if (record.child_table_id != child_table_id) continue;
            if (record.parent_table_id != parent_table_id) continue;
            if (!std.mem.eql(u8, record.constraint_name, constraint_name)) continue;
            if (!std.mem.eql(u8, record.start_parent_key, start_parent_key)) continue;
            freeForeignKeyReferenceRange(self.alloc, record);
            _ = self.foreign_key_ref_ranges.orderedRemove(i);
            return true;
        }
        return false;
    }

    pub fn removeUniqueConstraintRangesForTable(self: *TableManager, table_id: u64) usize {
        var removed: usize = 0;
        var i: usize = 0;
        while (i < self.unique_constraint_ranges.items.len) {
            const record = self.unique_constraint_ranges.items[i];
            if (record.table_id != table_id) {
                i += 1;
                continue;
            }
            freeUniqueConstraintRange(self.alloc, record);
            _ = self.unique_constraint_ranges.orderedRemove(i);
            removed += 1;
        }
        return removed;
    }

    pub fn removeUniqueConstraintRange(
        self: *TableManager,
        table_id: u64,
        constraint_name: []const u8,
        start_encoded_value: []const u8,
    ) bool {
        for (self.unique_constraint_ranges.items, 0..) |record, i| {
            if (record.table_id != table_id) continue;
            if (!std.mem.eql(u8, record.constraint_name, constraint_name)) continue;
            if (!std.mem.eql(u8, record.start_encoded_value, start_encoded_value)) continue;
            freeUniqueConstraintRange(self.alloc, record);
            _ = self.unique_constraint_ranges.orderedRemove(i);
            return true;
        }
        return false;
    }

    pub fn removeSecondaryIndexRebuildRangesForTable(self: *TableManager, table_id: u64) usize {
        var removed: usize = 0;
        var i: usize = 0;
        while (i < self.secondary_index_rebuild_ranges.items.len) {
            const record = self.secondary_index_rebuild_ranges.items[i];
            if (record.table_id != table_id) {
                i += 1;
                continue;
            }
            freeSecondaryIndexRebuildRange(self.alloc, record);
            _ = self.secondary_index_rebuild_ranges.orderedRemove(i);
            removed += 1;
        }
        return removed;
    }

    pub fn removeSecondaryIndexRebuildRange(
        self: *TableManager,
        table_id: u64,
        index_name: []const u8,
        index_generation: u64,
        start_row_key: []const u8,
    ) bool {
        for (self.secondary_index_rebuild_ranges.items, 0..) |record, i| {
            if (record.table_id != table_id) continue;
            if (record.index_generation != index_generation) continue;
            if (!std.mem.eql(u8, record.index_name, index_name)) continue;
            if (!std.mem.eql(u8, record.start_row_key, start_row_key)) continue;
            freeSecondaryIndexRebuildRange(self.alloc, record);
            _ = self.secondary_index_rebuild_ranges.orderedRemove(i);
            return true;
        }
        return false;
    }

    pub fn removeSchemaRewriteJobsForTable(self: *TableManager, table_id: u64) usize {
        var removed: usize = 0;
        var i: usize = 0;
        while (i < self.schema_rewrite_jobs.items.len) {
            const record = self.schema_rewrite_jobs.items[i];
            if (record.table_id != table_id) {
                i += 1;
                continue;
            }
            freeSchemaRewriteJob(self.alloc, record);
            _ = self.schema_rewrite_jobs.orderedRemove(i);
            removed += 1;
        }
        return removed;
    }

    pub fn removeSchemaRewriteJob(self: *TableManager, job_id: u64) bool {
        for (self.schema_rewrite_jobs.items, 0..) |record, i| {
            if (record.job_id != job_id) continue;
            freeSchemaRewriteJob(self.alloc, record);
            _ = self.schema_rewrite_jobs.orderedRemove(i);
            return true;
        }
        return false;
    }

    pub fn removeTableEmptyingJobsForTable(self: *TableManager, table_id: u64) usize {
        var removed: usize = 0;
        var i: usize = 0;
        while (i < self.table_emptying_jobs.items.len) {
            const record = self.table_emptying_jobs.items[i];
            if (record.table_id != table_id and !u64SliceContains(record.affected_table_ids, table_id)) {
                i += 1;
                continue;
            }
            freeTableEmptyingJob(self.alloc, record);
            _ = self.table_emptying_jobs.orderedRemove(i);
            removed += 1;
        }
        return removed;
    }

    pub fn removeTableEmptyingJob(self: *TableManager, job_id: u64) bool {
        for (self.table_emptying_jobs.items, 0..) |record, i| {
            if (record.job_id != job_id) continue;
            freeTableEmptyingJob(self.alloc, record);
            _ = self.table_emptying_jobs.orderedRemove(i);
            return true;
        }
        return false;
    }

    pub fn beginSchemaRewriteJob(self: *TableManager, request: SchemaRewriteJobBeginRequest) !void {
        if (request.lease_owner.len == 0 or request.lease_expires_at_ms <= request.now_ms) return error.InvalidSchemaRewriteJobLease;
        const record = self.findSchemaRewriteJob(request.job_id) orelse return error.UnknownSchemaRewriteJob;
        const claimable = std.mem.eql(u8, record.state, schema_rewrite_declared) or
            (std.mem.eql(u8, record.state, schema_rewrite_running) and record.lease_expires_at_ms != 0 and record.lease_expires_at_ms <= request.now_ms);
        if (!claimable) {
            if (std.mem.eql(u8, record.state, schema_rewrite_running)) return error.SchemaRewriteJobClaimBusy;
            return error.SchemaRewriteJobNotDeclared;
        }
        var updated = record.*;
        updated.state = schema_rewrite_running;
        updated.lease_owner = request.lease_owner;
        updated.lease_expires_at_ms = request.lease_expires_at_ms;
        updated.attempts +%= 1;
        try self.upsertSchemaRewriteJob(updated);
    }

    pub fn finishSchemaRewriteJob(self: *TableManager, request: SchemaRewriteJobFinishRequest) !void {
        const record = self.findSchemaRewriteJob(request.job_id) orelse return error.UnknownSchemaRewriteJob;
        if (!std.mem.eql(u8, record.state, schema_rewrite_running)) return error.SchemaRewriteJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.SchemaRewriteJobLeaseMismatch;
        var updated = record.*;
        updated.state = schema_rewrite_ready;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.completed_row_count = request.completed_row_count;
        updated.progress_row_key = request.progress_row_key;
        updated.last_error = "";
        try self.upsertSchemaRewriteJob(updated);
    }

    pub fn invalidateSchemaRewriteJob(self: *TableManager, request: SchemaRewriteJobInvalidateRequest) !void {
        const record = self.findSchemaRewriteJob(request.job_id) orelse return error.UnknownSchemaRewriteJob;
        if (!std.mem.eql(u8, record.state, schema_rewrite_running)) return error.SchemaRewriteJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.SchemaRewriteJobLeaseMismatch;
        var updated = record.*;
        updated.state = schema_rewrite_invalid;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.last_error;
        try self.upsertSchemaRewriteJob(updated);
    }

    pub fn pauseSchemaRewriteJob(self: *TableManager, request: SchemaRewriteJobControlRequest) !void {
        const record = self.findSchemaRewriteJob(request.job_id) orelse return error.UnknownSchemaRewriteJob;
        if (!std.mem.eql(u8, record.state, schema_rewrite_declared) and
            !std.mem.eql(u8, record.state, schema_rewrite_running))
        {
            return error.SchemaRewriteJobNotDeclared;
        }
        var updated = record.*;
        updated.state = schema_rewrite_paused;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertSchemaRewriteJob(updated);
    }

    pub fn resumeSchemaRewriteJob(self: *TableManager, request: SchemaRewriteJobControlRequest) !void {
        const record = self.findSchemaRewriteJob(request.job_id) orelse return error.UnknownSchemaRewriteJob;
        if (!std.mem.eql(u8, record.state, schema_rewrite_paused)) return error.SchemaRewriteJobNotPaused;
        var updated = record.*;
        updated.state = schema_rewrite_declared;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertSchemaRewriteJob(updated);
    }

    pub fn retrySchemaRewriteJob(self: *TableManager, request: SchemaRewriteJobControlRequest) !void {
        const record = self.findSchemaRewriteJob(request.job_id) orelse return error.UnknownSchemaRewriteJob;
        if (!std.mem.eql(u8, record.state, schema_rewrite_invalid)) return error.SchemaRewriteJobNotInvalid;
        var updated = record.*;
        updated.state = schema_rewrite_declared;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertSchemaRewriteJob(updated);
    }

    pub fn cancelSchemaRewriteJob(self: *TableManager, request: SchemaRewriteJobControlRequest) !void {
        const record = self.findSchemaRewriteJob(request.job_id) orelse return error.UnknownSchemaRewriteJob;
        if (std.mem.eql(u8, record.state, schema_rewrite_ready)) return error.SchemaRewriteJobComplete;
        if (std.mem.eql(u8, record.state, schema_rewrite_canceled)) return error.SchemaRewriteJobCanceled;
        var updated = record.*;
        updated.state = schema_rewrite_canceled;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertSchemaRewriteJob(updated);
    }

    pub fn beginTableEmptyingJob(self: *TableManager, request: TableEmptyingJobBeginRequest) !void {
        if (request.lease_owner.len == 0 or request.lease_expires_at_ms <= request.now_ms) return error.InvalidTableEmptyingJobLease;
        const record = self.findTableEmptyingJob(request.job_id) orelse return error.UnknownTableEmptyingJob;
        const claimable = std.mem.eql(u8, record.state, table_emptying_declared) or
            (std.mem.eql(u8, record.state, table_emptying_running) and record.lease_expires_at_ms != 0 and record.lease_expires_at_ms <= request.now_ms);
        if (!claimable) {
            if (std.mem.eql(u8, record.state, table_emptying_running)) return error.TableEmptyingJobClaimBusy;
            return error.TableEmptyingJobNotDeclared;
        }
        var updated = record.*;
        updated.state = table_emptying_running;
        updated.lease_owner = request.lease_owner;
        updated.lease_expires_at_ms = request.lease_expires_at_ms;
        updated.attempts +%= 1;
        try self.upsertTableEmptyingJob(updated);
    }

    pub fn finishTableEmptyingJob(self: *TableManager, request: TableEmptyingJobFinishRequest) !void {
        const record = self.findTableEmptyingJob(request.job_id) orelse return error.UnknownTableEmptyingJob;
        if (!std.mem.eql(u8, record.state, table_emptying_running)) return error.TableEmptyingJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.TableEmptyingJobLeaseMismatch;
        var updated = record.*;
        updated.state = table_emptying_ready;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.completed_row_count = request.completed_row_count;
        updated.progress_row_key = request.progress_row_key;
        updated.last_error = "";
        try self.upsertTableEmptyingJob(updated);
    }

    pub fn invalidateTableEmptyingJob(self: *TableManager, request: TableEmptyingJobInvalidateRequest) !void {
        const record = self.findTableEmptyingJob(request.job_id) orelse return error.UnknownTableEmptyingJob;
        if (!std.mem.eql(u8, record.state, table_emptying_running)) return error.TableEmptyingJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.TableEmptyingJobLeaseMismatch;
        var updated = record.*;
        updated.state = table_emptying_invalid;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.last_error;
        try self.upsertTableEmptyingJob(updated);
    }

    pub fn pauseTableEmptyingJob(self: *TableManager, request: TableEmptyingJobControlRequest) !void {
        const record = self.findTableEmptyingJob(request.job_id) orelse return error.UnknownTableEmptyingJob;
        if (!std.mem.eql(u8, record.state, table_emptying_declared) and
            !std.mem.eql(u8, record.state, table_emptying_running))
        {
            return error.TableEmptyingJobNotDeclared;
        }
        var updated = record.*;
        updated.state = table_emptying_paused;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertTableEmptyingJob(updated);
    }

    pub fn resumeTableEmptyingJob(self: *TableManager, request: TableEmptyingJobControlRequest) !void {
        const record = self.findTableEmptyingJob(request.job_id) orelse return error.UnknownTableEmptyingJob;
        if (!std.mem.eql(u8, record.state, table_emptying_paused)) return error.TableEmptyingJobNotPaused;
        var updated = record.*;
        updated.state = table_emptying_declared;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertTableEmptyingJob(updated);
    }

    pub fn retryTableEmptyingJob(self: *TableManager, request: TableEmptyingJobControlRequest) !void {
        const record = self.findTableEmptyingJob(request.job_id) orelse return error.UnknownTableEmptyingJob;
        if (!std.mem.eql(u8, record.state, table_emptying_invalid)) return error.TableEmptyingJobNotInvalid;
        var updated = record.*;
        updated.state = table_emptying_declared;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertTableEmptyingJob(updated);
    }

    pub fn cancelTableEmptyingJob(self: *TableManager, request: TableEmptyingJobControlRequest) !void {
        const record = self.findTableEmptyingJob(request.job_id) orelse return error.UnknownTableEmptyingJob;
        if (std.mem.eql(u8, record.state, table_emptying_ready)) return error.TableEmptyingJobComplete;
        if (std.mem.eql(u8, record.state, table_emptying_canceled)) return error.TableEmptyingJobCanceled;
        var updated = record.*;
        updated.state = table_emptying_canceled;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.reason;
        try self.upsertTableEmptyingJob(updated);
    }

    pub fn promoteTableEmptyingBarrier(self: *TableManager, request: TableEmptyingBarrierPromotionRequest) !void {
        if (request.job_ids.len == 0 or request.promotions.len == 0) return error.InvalidTableEmptyingBarrierPromotion;

        const first_record = self.findTableEmptyingJob(request.job_ids[0]) orelse return error.UnknownTableEmptyingJob;
        if (first_record.barrier_id == 0 or first_record.affected_table_ids.len == 0) return error.InvalidTableEmptyingBarrierPromotion;
        if (!tableEmptyingAffectedTableIdsCanonicalValid(first_record.table_id, first_record.affected_table_ids)) return error.InvalidTableEmptyingBarrierPromotion;
        if (request.promotions.len != first_record.affected_table_ids.len) return error.InvalidTableEmptyingBarrierPromotion;

        for (request.job_ids, 0..) |job_id, i| {
            if (job_id == 0) return error.InvalidTableEmptyingBarrierPromotion;
            for (request.job_ids[0..i]) |previous| {
                if (previous == job_id) return error.InvalidTableEmptyingBarrierPromotion;
            }
            const record = self.findTableEmptyingJob(job_id) orelse return error.UnknownTableEmptyingJob;
            if (!std.mem.eql(u8, record.state, table_emptying_ready)) return error.TableEmptyingJobNotReady;
            if (record.barrier_id != first_record.barrier_id or
                record.restart_identity != first_record.restart_identity or
                record.cascade != first_record.cascade or
                !u64SlicesEqual(record.affected_table_ids, first_record.affected_table_ids) or
                !u64SliceContains(first_record.affected_table_ids, record.table_id))
            {
                return error.InvalidTableEmptyingBarrierPromotion;
            }
        }

        for (request.promotions, 0..) |promotion, i| {
            if (promotion.table_id == 0 or promotion.target_generation == 0) return error.InvalidTableEmptyingBarrierPromotion;
            const table = self.tables.getPtr(promotion.table_id) orelse return error.UnknownTable;
            if (!u64SliceContains(first_record.affected_table_ids, promotion.table_id)) return error.InvalidTableEmptyingBarrierPromotion;
            if (promotion.target_generation != table.data_generation +| 1) return error.InvalidTableEmptyingBarrierPromotion;
            for (request.promotions[0..i]) |previous| {
                if (previous.table_id == promotion.table_id) return error.InvalidTableEmptyingBarrierPromotion;
            }
        }
        for (first_record.affected_table_ids) |table_id| {
            if (!promotionListContainsTable(request.promotions, table_id)) return error.InvalidTableEmptyingBarrierPromotion;
            try self.validateCompleteTableEmptyingBarrierTable(first_record.*, request.job_ids, table_id);
        }

        if (first_record.restart_identity) {
            _ = try self.resetIdentityAllocatorsForTableEmptyingBarrier(self.alloc, .{
                .barrier_id = first_record.barrier_id,
                .affected_table_ids = first_record.affected_table_ids,
                .job_ids = request.job_ids,
                .cascade = first_record.cascade,
            });
        }

        for (request.promotions) |promotion| {
            const table = self.tables.getPtr(promotion.table_id).?;
            if (table.data_generation < promotion.target_generation) {
                table.data_generation = promotion.target_generation;
            }
        }

        for (request.job_ids) |job_id| {
            if (!self.removeTableEmptyingJob(job_id)) return error.UnknownTableEmptyingJob;
        }
    }

    pub fn validateTableEmptyingIdentityAllocatorReset(self: *TableManager, request: TableEmptyingIdentityAllocatorResetRequest) !void {
        if (request.barrier_id == 0 or request.job_ids.len == 0 or
            !tableEmptyingAffectedTableIdsCanonicalSetValid(request.affected_table_ids))
        {
            return error.InvalidTableEmptyingIdentityAllocatorReset;
        }

        const first_record = self.findTableEmptyingJob(request.job_ids[0]) orelse return error.UnknownTableEmptyingJob;
        if (first_record.barrier_id != request.barrier_id or
            !first_record.restart_identity or
            first_record.cascade != request.cascade or
            !tableEmptyingAffectedTableIdsCanonicalValid(first_record.table_id, first_record.affected_table_ids) or
            !u64SlicesEqual(first_record.affected_table_ids, request.affected_table_ids))
        {
            return error.InvalidTableEmptyingIdentityAllocatorReset;
        }

        for (request.affected_table_ids) |table_id| {
            if (self.tables.get(table_id) == null) return error.UnknownTable;
        }

        for (request.job_ids, 0..) |job_id, i| {
            if (job_id == 0) return error.InvalidTableEmptyingIdentityAllocatorReset;
            for (request.job_ids[0..i]) |previous| {
                if (previous == job_id) return error.InvalidTableEmptyingIdentityAllocatorReset;
            }
            const record = self.findTableEmptyingJob(job_id) orelse return error.UnknownTableEmptyingJob;
            if (!std.mem.eql(u8, record.state, table_emptying_ready)) return error.TableEmptyingJobNotReady;
            if (record.barrier_id != request.barrier_id or
                !record.restart_identity or
                record.cascade != request.cascade or
                !u64SlicesEqual(record.affected_table_ids, request.affected_table_ids) or
                !u64SliceContains(request.affected_table_ids, record.table_id))
            {
                return error.InvalidTableEmptyingIdentityAllocatorReset;
            }
        }

        for (request.affected_table_ids) |table_id| {
            self.validateCompleteTableEmptyingBarrierTable(first_record.*, request.job_ids, table_id) catch |err| switch (err) {
                error.InvalidTableEmptyingBarrierPromotion => return error.InvalidTableEmptyingIdentityAllocatorReset,
                else => return err,
            };
        }
    }

    fn validateCompleteTableEmptyingBarrierTable(
        self: *TableManager,
        barrier: TableEmptyingJobRecord,
        job_ids: []const u64,
        table_id: u64,
    ) !void {
        const table = self.tables.get(table_id) orelse return error.UnknownTable;
        var range_count: usize = 0;
        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| {
            if (range.table_id != table_id) continue;
            range_count += 1;
            if (!self.tableEmptyingPromotionHasReadyRangeJob(barrier, job_ids, table, range.*)) {
                return error.InvalidTableEmptyingBarrierPromotion;
            }
        }
        if (range_count == 0) return error.InvalidTableEmptyingBarrierPromotion;
    }

    fn tableEmptyingPromotionHasReadyRangeJob(
        self: *TableManager,
        barrier: TableEmptyingJobRecord,
        job_ids: []const u64,
        table: TableRecord,
        range: RangeRecord,
    ) bool {
        const schema_generation = schemaRewriteGenerationForSchemaJson(table.schema_json);
        for (job_ids) |job_id| {
            const record = self.findTableEmptyingJob(job_id) orelse continue;
            if (!std.mem.eql(u8, record.state, table_emptying_ready)) continue;
            if (record.barrier_id != barrier.barrier_id) continue;
            if (record.table_id != table.table_id) continue;
            if (record.group_id != range.group_id) continue;
            if (record.range_id != 0 and record.range_id != range.range_id) continue;
            if (record.schema_generation != schema_generation) continue;
            if (record.data_generation != table.data_generation) continue;
            if (!std.mem.eql(u8, record.start_row_key, range.start_key)) continue;
            if (!optionalStringSlicesEqual(record.end_row_key, range.end_key)) continue;
            if (!u64SlicesEqual(record.affected_table_ids, barrier.affected_table_ids)) continue;
            if (record.restart_identity != barrier.restart_identity) continue;
            if (record.cascade != barrier.cascade) continue;
            return true;
        }
        return false;
    }

    pub fn beginForeignKeyReferenceRangeSplit(self: *TableManager, request: ForeignKeyReferenceRangeSplitRequest) !void {
        try group_ids.requireDataGroupId(request.left_group_id);
        try group_ids.requireDataGroupId(request.right_group_id);
        if (request.left_group_id == request.right_group_id) return error.InvalidForeignKeyReferenceRangeSplit;
        const source = self.findForeignKeyReferenceRange(request.selector) orelse return error.UnknownForeignKeyReferenceRange;
        if (!std.mem.eql(u8, source.state, foreign_key_ref_range_active)) return error.ForeignKeyReferenceRangeNotActive;
        if (!keyStrictlyInsideRange(request.split_parent_key, source.start_parent_key, source.end_parent_key)) return error.InvalidForeignKeyReferenceRangeSplit;
        if (request.left_group_id != source.group_id and self.foreignKeyReferenceRangeGroupExists(request.left_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (self.foreignKeyReferenceRangeGroupExists(request.right_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        var updated = source.*;
        updated.state = foreign_key_ref_range_splitting;
        updated.topology_epoch +%= 1;
        try self.upsertForeignKeyReferenceRange(updated);
    }

    pub fn finishForeignKeyReferenceRangeSplit(self: *TableManager, request: ForeignKeyReferenceRangeSplitRequest) !void {
        try group_ids.requireDataGroupId(request.left_group_id);
        try group_ids.requireDataGroupId(request.right_group_id);
        if (request.left_group_id == request.right_group_id) return error.InvalidForeignKeyReferenceRangeSplit;
        const source = self.findForeignKeyReferenceRange(request.selector) orelse return error.UnknownForeignKeyReferenceRange;
        if (!std.mem.eql(u8, source.state, foreign_key_ref_range_splitting)) return error.ForeignKeyReferenceRangeNotSplitting;
        if (!keyStrictlyInsideRange(request.split_parent_key, source.start_parent_key, source.end_parent_key)) return error.InvalidForeignKeyReferenceRangeSplit;
        if (request.left_group_id != source.group_id and self.foreignKeyReferenceRangeGroupExists(request.left_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (self.foreignKeyReferenceRangeGroupExists(request.right_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        const original = try cloneForeignKeyReferenceRange(self.alloc, source.*);
        defer freeForeignKeyReferenceRange(self.alloc, original);
        if (!self.removeForeignKeyReferenceRange(original.child_table_id, original.constraint_name, original.parent_table_id, original.start_parent_key)) return error.UnknownForeignKeyReferenceRange;
        errdefer self.restoreForeignKeyReferenceRangeBestEffort(original);

        try self.upsertForeignKeyReferenceRange(.{
            .child_table_id = original.child_table_id,
            .constraint_name = original.constraint_name,
            .parent_table_id = original.parent_table_id,
            .start_parent_key = original.start_parent_key,
            .end_parent_key = request.split_parent_key,
            .group_id = request.left_group_id,
            .range_id = original.range_id,
            .topology_epoch = original.topology_epoch +% 1,
            .state = foreign_key_ref_range_active,
        });
        errdefer _ = self.removeForeignKeyReferenceRange(original.child_table_id, original.constraint_name, original.parent_table_id, original.start_parent_key);
        try self.upsertForeignKeyReferenceRange(.{
            .child_table_id = original.child_table_id,
            .constraint_name = original.constraint_name,
            .parent_table_id = original.parent_table_id,
            .start_parent_key = request.split_parent_key,
            .end_parent_key = original.end_parent_key,
            .group_id = request.right_group_id,
            .range_id = request.right_group_id,
            .topology_epoch = original.topology_epoch +% 1,
            .state = foreign_key_ref_range_active,
        });
    }

    pub fn beginForeignKeyReferenceRangeMerge(self: *TableManager, request: ForeignKeyReferenceRangeMergeRequest) !void {
        try group_ids.requireDataGroupId(request.merged_group_id);
        const left = self.findForeignKeyReferenceRange(request.left_selector) orelse return error.UnknownForeignKeyReferenceRange;
        const right = self.findForeignKeyReferenceRange(.{
            .child_table_id = request.left_selector.child_table_id,
            .constraint_name = request.left_selector.constraint_name,
            .parent_table_id = request.left_selector.parent_table_id,
            .start_parent_key = request.right_start_parent_key,
        }) orelse return error.UnknownForeignKeyReferenceRange;
        if (!foreignKeyReferenceRangesAdjacent(left.*, right.*)) return error.ForeignKeyReferenceRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, foreign_key_ref_range_active) or !std.mem.eql(u8, right.state, foreign_key_ref_range_active)) return error.ForeignKeyReferenceRangeNotActive;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and self.foreignKeyReferenceRangeGroupExists(request.merged_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        var updated_left = left.*;
        updated_left.state = foreign_key_ref_range_merging;
        updated_left.topology_epoch +%= 1;
        try self.upsertForeignKeyReferenceRange(updated_left);
        var updated_right = right.*;
        updated_right.state = foreign_key_ref_range_merging;
        updated_right.topology_epoch +%= 1;
        try self.upsertForeignKeyReferenceRange(updated_right);
    }

    pub fn finishForeignKeyReferenceRangeMerge(self: *TableManager, request: ForeignKeyReferenceRangeMergeRequest) !void {
        try group_ids.requireDataGroupId(request.merged_group_id);
        const left = self.findForeignKeyReferenceRange(request.left_selector) orelse return error.UnknownForeignKeyReferenceRange;
        const right = self.findForeignKeyReferenceRange(.{
            .child_table_id = request.left_selector.child_table_id,
            .constraint_name = request.left_selector.constraint_name,
            .parent_table_id = request.left_selector.parent_table_id,
            .start_parent_key = request.right_start_parent_key,
        }) orelse return error.UnknownForeignKeyReferenceRange;
        if (!foreignKeyReferenceRangesAdjacent(left.*, right.*)) return error.ForeignKeyReferenceRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, foreign_key_ref_range_merging) or !std.mem.eql(u8, right.state, foreign_key_ref_range_merging)) return error.ForeignKeyReferenceRangeNotMerging;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and self.foreignKeyReferenceRangeGroupExists(request.merged_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        const owned_left = try cloneForeignKeyReferenceRange(self.alloc, left.*);
        defer freeForeignKeyReferenceRange(self.alloc, owned_left);
        const owned_right = try cloneForeignKeyReferenceRange(self.alloc, right.*);
        defer freeForeignKeyReferenceRange(self.alloc, owned_right);
        if (!self.removeForeignKeyReferenceRange(owned_left.child_table_id, owned_left.constraint_name, owned_left.parent_table_id, owned_left.start_parent_key)) return error.UnknownForeignKeyReferenceRange;
        errdefer self.restoreForeignKeyReferenceRangeBestEffort(owned_left);
        if (!self.removeForeignKeyReferenceRange(owned_right.child_table_id, owned_right.constraint_name, owned_right.parent_table_id, owned_right.start_parent_key)) return error.UnknownForeignKeyReferenceRange;
        errdefer self.restoreForeignKeyReferenceRangeBestEffort(owned_right);

        try self.upsertForeignKeyReferenceRange(.{
            .child_table_id = owned_left.child_table_id,
            .constraint_name = owned_left.constraint_name,
            .parent_table_id = owned_left.parent_table_id,
            .start_parent_key = owned_left.start_parent_key,
            .end_parent_key = owned_right.end_parent_key,
            .group_id = request.merged_group_id,
            .range_id = mergedForeignKeyReferenceRangeId(request.merged_group_id, owned_left, owned_right),
            .topology_epoch = @max(owned_left.topology_epoch, owned_right.topology_epoch) +% 1,
            .state = foreign_key_ref_range_active,
        });
    }

    pub fn beginForeignKeyReferenceRangeRebuild(self: *TableManager, selector: ForeignKeyReferenceRangeSelector) !void {
        const record = self.findForeignKeyReferenceRange(selector) orelse return error.UnknownForeignKeyReferenceRange;
        if (!std.mem.eql(u8, record.state, foreign_key_ref_range_active)) return error.ForeignKeyReferenceRangeNotActive;
        var updated = record.*;
        updated.state = foreign_key_ref_range_rebuilding;
        updated.topology_epoch +%= 1;
        try self.upsertForeignKeyReferenceRange(updated);
    }

    pub fn finishForeignKeyReferenceRangeRebuild(self: *TableManager, selector: ForeignKeyReferenceRangeSelector) !void {
        const record = self.findForeignKeyReferenceRange(selector) orelse return error.UnknownForeignKeyReferenceRange;
        if (!std.mem.eql(u8, record.state, foreign_key_ref_range_rebuilding)) return error.ForeignKeyReferenceRangeNotRebuilding;
        var updated = record.*;
        updated.state = foreign_key_ref_range_active;
        updated.topology_epoch +%= 1;
        try self.upsertForeignKeyReferenceRange(updated);
    }

    fn foreignKeyReferenceRangeGroupExists(self: *const TableManager, group_id: u64) bool {
        for (self.foreign_key_ref_ranges.items) |record| {
            if (record.group_id == group_id) return true;
        }
        return false;
    }

    fn findForeignKeyReferenceRange(self: *TableManager, selector: ForeignKeyReferenceRangeSelector) ?*ForeignKeyReferenceRangeRecord {
        for (self.foreign_key_ref_ranges.items) |*record| {
            if (record.child_table_id != selector.child_table_id) continue;
            if (record.parent_table_id != selector.parent_table_id) continue;
            if (!std.mem.eql(u8, record.constraint_name, selector.constraint_name)) continue;
            if (!std.mem.eql(u8, record.start_parent_key, selector.start_parent_key)) continue;
            if (selector.range_id != 0 and record.range_id != selector.range_id) continue;
            return record;
        }
        return null;
    }

    fn restoreForeignKeyReferenceRangeBestEffort(self: *TableManager, record: ForeignKeyReferenceRangeRecord) void {
        self.upsertForeignKeyReferenceRange(record) catch {};
    }

    pub fn beginUniqueConstraintRangeSplit(self: *TableManager, request: UniqueConstraintRangeSplitRequest) !void {
        try group_ids.requireDataGroupId(request.left_group_id);
        try group_ids.requireDataGroupId(request.right_group_id);
        if (request.left_group_id == request.right_group_id) return error.InvalidUniqueConstraintRangeSplit;
        const source = self.findUniqueConstraintRange(request.selector) orelse return error.UnknownUniqueConstraintRange;
        if (!std.mem.eql(u8, source.state, unique_constraint_range_active)) return error.UniqueConstraintRangeNotActive;
        if (!keyStrictlyInsideRange(request.split_encoded_value, source.start_encoded_value, source.end_encoded_value)) return error.InvalidUniqueConstraintRangeSplit;
        if (request.left_group_id != source.group_id and self.uniqueConstraintRangeGroupExists(request.left_group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (self.uniqueConstraintRangeGroupExists(request.right_group_id)) return error.UniqueConstraintRangeGroupCollision;

        var updated = source.*;
        updated.state = unique_constraint_range_splitting;
        updated.topology_epoch +%= 1;
        try self.upsertUniqueConstraintRange(updated);
    }

    pub fn finishUniqueConstraintRangeSplit(self: *TableManager, request: UniqueConstraintRangeSplitRequest) !void {
        try group_ids.requireDataGroupId(request.left_group_id);
        try group_ids.requireDataGroupId(request.right_group_id);
        if (request.left_group_id == request.right_group_id) return error.InvalidUniqueConstraintRangeSplit;
        const source = self.findUniqueConstraintRange(request.selector) orelse return error.UnknownUniqueConstraintRange;
        if (!std.mem.eql(u8, source.state, unique_constraint_range_splitting)) return error.UniqueConstraintRangeNotSplitting;
        if (!keyStrictlyInsideRange(request.split_encoded_value, source.start_encoded_value, source.end_encoded_value)) return error.InvalidUniqueConstraintRangeSplit;
        if (request.left_group_id != source.group_id and self.uniqueConstraintRangeGroupExists(request.left_group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (self.uniqueConstraintRangeGroupExists(request.right_group_id)) return error.UniqueConstraintRangeGroupCollision;

        const original = try cloneUniqueConstraintRange(self.alloc, source.*);
        defer freeUniqueConstraintRange(self.alloc, original);
        if (!self.removeUniqueConstraintRange(original.table_id, original.constraint_name, original.start_encoded_value)) return error.UnknownUniqueConstraintRange;
        errdefer self.restoreUniqueConstraintRangeBestEffort(original);

        try self.upsertUniqueConstraintRange(.{
            .table_id = original.table_id,
            .constraint_name = original.constraint_name,
            .start_encoded_value = original.start_encoded_value,
            .end_encoded_value = request.split_encoded_value,
            .group_id = request.left_group_id,
            .range_id = original.range_id,
            .topology_epoch = original.topology_epoch +% 1,
            .state = unique_constraint_range_active,
        });
        errdefer _ = self.removeUniqueConstraintRange(original.table_id, original.constraint_name, original.start_encoded_value);
        try self.upsertUniqueConstraintRange(.{
            .table_id = original.table_id,
            .constraint_name = original.constraint_name,
            .start_encoded_value = request.split_encoded_value,
            .end_encoded_value = original.end_encoded_value,
            .group_id = request.right_group_id,
            .range_id = request.right_group_id,
            .topology_epoch = original.topology_epoch +% 1,
            .state = unique_constraint_range_active,
        });
    }

    pub fn beginUniqueConstraintRangeMerge(self: *TableManager, request: UniqueConstraintRangeMergeRequest) !void {
        try group_ids.requireDataGroupId(request.merged_group_id);
        const left = self.findUniqueConstraintRange(request.left_selector) orelse return error.UnknownUniqueConstraintRange;
        const right = self.findUniqueConstraintRange(.{
            .table_id = request.left_selector.table_id,
            .constraint_name = request.left_selector.constraint_name,
            .start_encoded_value = request.right_start_encoded_value,
        }) orelse return error.UnknownUniqueConstraintRange;
        if (!uniqueConstraintRangesAdjacent(left.*, right.*)) return error.UniqueConstraintRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, unique_constraint_range_active) or !std.mem.eql(u8, right.state, unique_constraint_range_active)) return error.UniqueConstraintRangeNotActive;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and self.uniqueConstraintRangeGroupExists(request.merged_group_id)) return error.UniqueConstraintRangeGroupCollision;

        var updated_left = left.*;
        updated_left.state = unique_constraint_range_merging;
        updated_left.topology_epoch +%= 1;
        try self.upsertUniqueConstraintRange(updated_left);
        var updated_right = right.*;
        updated_right.state = unique_constraint_range_merging;
        updated_right.topology_epoch +%= 1;
        try self.upsertUniqueConstraintRange(updated_right);
    }

    pub fn finishUniqueConstraintRangeMerge(self: *TableManager, request: UniqueConstraintRangeMergeRequest) !void {
        try group_ids.requireDataGroupId(request.merged_group_id);
        const left = self.findUniqueConstraintRange(request.left_selector) orelse return error.UnknownUniqueConstraintRange;
        const right = self.findUniqueConstraintRange(.{
            .table_id = request.left_selector.table_id,
            .constraint_name = request.left_selector.constraint_name,
            .start_encoded_value = request.right_start_encoded_value,
        }) orelse return error.UnknownUniqueConstraintRange;
        if (!uniqueConstraintRangesAdjacent(left.*, right.*)) return error.UniqueConstraintRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, unique_constraint_range_merging) or !std.mem.eql(u8, right.state, unique_constraint_range_merging)) return error.UniqueConstraintRangeNotMerging;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and self.uniqueConstraintRangeGroupExists(request.merged_group_id)) return error.UniqueConstraintRangeGroupCollision;

        const owned_left = try cloneUniqueConstraintRange(self.alloc, left.*);
        defer freeUniqueConstraintRange(self.alloc, owned_left);
        const owned_right = try cloneUniqueConstraintRange(self.alloc, right.*);
        defer freeUniqueConstraintRange(self.alloc, owned_right);
        if (!self.removeUniqueConstraintRange(owned_left.table_id, owned_left.constraint_name, owned_left.start_encoded_value)) return error.UnknownUniqueConstraintRange;
        errdefer self.restoreUniqueConstraintRangeBestEffort(owned_left);
        if (!self.removeUniqueConstraintRange(owned_right.table_id, owned_right.constraint_name, owned_right.start_encoded_value)) return error.UnknownUniqueConstraintRange;
        errdefer self.restoreUniqueConstraintRangeBestEffort(owned_right);

        try self.upsertUniqueConstraintRange(.{
            .table_id = owned_left.table_id,
            .constraint_name = owned_left.constraint_name,
            .start_encoded_value = owned_left.start_encoded_value,
            .end_encoded_value = owned_right.end_encoded_value,
            .group_id = request.merged_group_id,
            .range_id = mergedUniqueConstraintRangeId(request.merged_group_id, owned_left, owned_right),
            .topology_epoch = @max(owned_left.topology_epoch, owned_right.topology_epoch) +% 1,
            .state = unique_constraint_range_active,
        });
    }

    pub fn beginUniqueConstraintRangeRebuild(self: *TableManager, selector: UniqueConstraintRangeSelector) !void {
        const record = self.findUniqueConstraintRange(selector) orelse return error.UnknownUniqueConstraintRange;
        if (!std.mem.eql(u8, record.state, unique_constraint_range_active)) return error.UniqueConstraintRangeNotActive;
        var updated = record.*;
        updated.state = unique_constraint_range_rebuilding;
        updated.topology_epoch +%= 1;
        try self.upsertUniqueConstraintRange(updated);
    }

    pub fn finishUniqueConstraintRangeRebuild(self: *TableManager, selector: UniqueConstraintRangeSelector) !void {
        const record = self.findUniqueConstraintRange(selector) orelse return error.UnknownUniqueConstraintRange;
        if (!std.mem.eql(u8, record.state, unique_constraint_range_rebuilding)) return error.UniqueConstraintRangeNotRebuilding;
        var updated = record.*;
        updated.state = unique_constraint_range_active;
        updated.topology_epoch +%= 1;
        try self.upsertUniqueConstraintRange(updated);
    }

    fn uniqueConstraintRangeGroupExists(self: *const TableManager, group_id: u64) bool {
        for (self.unique_constraint_ranges.items) |record| {
            if (record.group_id == group_id) return true;
        }
        return false;
    }

    fn findUniqueConstraintRange(self: *TableManager, selector: UniqueConstraintRangeSelector) ?*UniqueConstraintRangeRecord {
        for (self.unique_constraint_ranges.items) |*record| {
            if (record.table_id != selector.table_id) continue;
            if (!std.mem.eql(u8, record.constraint_name, selector.constraint_name)) continue;
            if (!std.mem.eql(u8, record.start_encoded_value, selector.start_encoded_value)) continue;
            if (selector.range_id != 0 and record.range_id != selector.range_id) continue;
            return record;
        }
        return null;
    }

    fn restoreUniqueConstraintRangeBestEffort(self: *TableManager, record: UniqueConstraintRangeRecord) void {
        self.upsertUniqueConstraintRange(record) catch {};
    }

    pub fn beginSecondaryIndexRebuildRange(self: *TableManager, request: SecondaryIndexRebuildRangeBeginRequest) !void {
        const selector = request.selector;
        const record = self.findSecondaryIndexRebuildRange(selector) orelse return error.UnknownSecondaryIndexRebuildRange;
        const claimable = std.mem.eql(u8, record.state, secondary_index_rebuild_declared) or
            (std.mem.eql(u8, record.state, secondary_index_rebuild_building) and (record.lease_expires_at_ms == 0 or record.lease_expires_at_ms <= request.now_ms));
        if (!claimable) {
            if (std.mem.eql(u8, record.state, secondary_index_rebuild_building)) return error.SecondaryIndexRebuildRangeClaimBusy;
            return error.SecondaryIndexRebuildRangeNotDeclared;
        }
        var updated = record.*;
        updated.state = secondary_index_rebuild_building;
        updated.lease_owner = request.lease_owner;
        updated.lease_expires_at_ms = request.lease_expires_at_ms;
        updated.attempts +%= 1;
        updated.topology_epoch +%= 1;
        try self.upsertSecondaryIndexRebuildRange(updated);
    }

    pub fn finishSecondaryIndexRebuildRange(self: *TableManager, request: SecondaryIndexRebuildRangeFinishRequest) !void {
        const selector = request.selector;
        const record = self.findSecondaryIndexRebuildRange(selector) orelse return error.UnknownSecondaryIndexRebuildRange;
        if (!std.mem.eql(u8, record.state, secondary_index_rebuild_building)) return error.SecondaryIndexRebuildRangeNotBuilding;
        var updated = record.*;
        updated.state = secondary_index_rebuild_ready;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.completed_row_count = request.completed_row_count;
        updated.progress_row_key = request.progress_row_key;
        updated.last_error = "";
        updated.topology_epoch +%= 1;
        try self.upsertSecondaryIndexRebuildRange(updated);
    }

    pub fn saveSecondaryIndexRebuildRangeProgress(self: *TableManager, request: SecondaryIndexRebuildRangeProgressRequest) !void {
        const selector = request.selector;
        const record = self.findSecondaryIndexRebuildRange(selector) orelse return error.UnknownSecondaryIndexRebuildRange;
        if (!std.mem.eql(u8, record.state, secondary_index_rebuild_building)) return error.SecondaryIndexRebuildRangeNotBuilding;
        var updated = record.*;
        updated.state = secondary_index_rebuild_building;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.completed_row_count = request.completed_row_count;
        updated.progress_row_key = request.progress_row_key;
        updated.last_error = "";
        updated.topology_epoch +%= 1;
        try self.upsertSecondaryIndexRebuildRange(updated);
    }

    pub fn invalidateSecondaryIndexRebuildRange(self: *TableManager, request: SecondaryIndexRebuildRangeInvalidateRequest) !void {
        const selector = request.selector;
        const record = self.findSecondaryIndexRebuildRange(selector) orelse return error.UnknownSecondaryIndexRebuildRange;
        var updated = record.*;
        updated.state = secondary_index_rebuild_invalid;
        updated.lease_owner = "";
        updated.lease_expires_at_ms = 0;
        updated.last_error = request.last_error;
        updated.topology_epoch +%= 1;
        try self.upsertSecondaryIndexRebuildRange(updated);
    }

    fn secondaryIndexRebuildRangeGroupExists(self: *const TableManager, group_id: u64) bool {
        for (self.secondary_index_rebuild_ranges.items) |record| {
            if (record.group_id == group_id) return true;
        }
        return false;
    }

    fn findSecondaryIndexRebuildRange(self: *TableManager, selector: SecondaryIndexRebuildRangeSelector) ?*SecondaryIndexRebuildRangeRecord {
        for (self.secondary_index_rebuild_ranges.items) |*record| {
            if (record.table_id != selector.table_id) continue;
            if (record.index_generation != selector.index_generation) continue;
            if (!std.mem.eql(u8, record.index_name, selector.index_name)) continue;
            if (!std.mem.eql(u8, record.start_row_key, selector.start_row_key)) continue;
            return record;
        }
        return null;
    }

    fn findSchemaRewriteJob(self: *TableManager, job_id: u64) ?*SchemaRewriteJobRecord {
        for (self.schema_rewrite_jobs.items) |*record| {
            if (record.job_id == job_id) return record;
        }
        return null;
    }

    fn findTableEmptyingJob(self: *TableManager, job_id: u64) ?*TableEmptyingJobRecord {
        for (self.table_emptying_jobs.items) |*record| {
            if (record.job_id == job_id) return record;
        }
        return null;
    }

    pub fn listTables(self: *TableManager, alloc: std.mem.Allocator) ![]TableRecord {
        var out = std.ArrayListUnmanaged(TableRecord).empty;
        errdefer {
            for (out.items) |record| freeTable(alloc, record);
            out.deinit(alloc);
        }
        var it = self.tables.valueIterator();
        while (it.next()) |record| {
            const owned = try cloneTable(alloc, record.*);
            errdefer freeTable(alloc, owned);
            try out.append(alloc, owned);
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeTables(_: *TableManager, alloc: std.mem.Allocator, records: []TableRecord) void {
        for (records) |record| freeTable(alloc, record);
        alloc.free(records);
    }

    pub fn listDatabases(self: *TableManager, alloc: std.mem.Allocator) ![]DatabaseRecord {
        var out = std.ArrayListUnmanaged(DatabaseRecord).empty;
        errdefer {
            for (out.items) |record| freeDatabase(alloc, record);
            out.deinit(alloc);
        }
        var it = self.databases.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneDatabase(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeDatabases(_: *TableManager, alloc: std.mem.Allocator, records: []DatabaseRecord) void {
        for (records) |record| freeDatabase(alloc, record);
        alloc.free(records);
    }

    pub fn listNamespaces(self: *TableManager, alloc: std.mem.Allocator) ![]NamespaceRecord {
        var out = std.ArrayListUnmanaged(NamespaceRecord).empty;
        errdefer {
            for (out.items) |record| freeNamespace(alloc, record);
            out.deinit(alloc);
        }
        var it = self.namespaces.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneNamespace(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeNamespaces(_: *TableManager, alloc: std.mem.Allocator, records: []NamespaceRecord) void {
        for (records) |record| freeNamespace(alloc, record);
        alloc.free(records);
    }

    pub fn listTablespaces(self: *TableManager, alloc: std.mem.Allocator) ![]TablespaceRecord {
        var out = std.ArrayListUnmanaged(TablespaceRecord).empty;
        errdefer {
            for (out.items) |record| freeTablespace(alloc, record);
            out.deinit(alloc);
        }
        var it = self.tablespaces.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneTablespace(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeTablespaces(_: *TableManager, alloc: std.mem.Allocator, records: []TablespaceRecord) void {
        for (records) |record| freeTablespace(alloc, record);
        alloc.free(records);
    }

    pub fn listSequences(self: *TableManager, alloc: std.mem.Allocator) ![]SequenceRecord {
        var out = std.ArrayListUnmanaged(SequenceRecord).empty;
        errdefer {
            for (out.items) |record| freeSequence(alloc, record);
            out.deinit(alloc);
        }
        var it = self.sequences.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneSequence(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeSequences(_: *TableManager, alloc: std.mem.Allocator, records: []SequenceRecord) void {
        for (records) |record| freeSequence(alloc, record);
        alloc.free(records);
    }

    pub fn listRanges(self: *TableManager, alloc: std.mem.Allocator) ![]RangeRecord {
        var out = std.ArrayListUnmanaged(RangeRecord).empty;
        errdefer {
            for (out.items) |record| freeRange(alloc, record);
            out.deinit(alloc);
        }
        var it = self.ranges.valueIterator();
        while (it.next()) |record| {
            const owned = try cloneRange(alloc, record.*);
            errdefer freeRange(alloc, owned);
            try out.append(alloc, owned);
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeRanges(_: *TableManager, alloc: std.mem.Allocator, records: []RangeRecord) void {
        for (records) |record| freeRange(alloc, record);
        alloc.free(records);
    }

    pub fn listForeignKeyReferenceRanges(self: *TableManager, alloc: std.mem.Allocator) ![]ForeignKeyReferenceRangeRecord {
        var out = std.ArrayListUnmanaged(ForeignKeyReferenceRangeRecord).empty;
        errdefer {
            for (out.items) |record| freeForeignKeyReferenceRange(alloc, record);
            out.deinit(alloc);
        }
        for (self.foreign_key_ref_ranges.items) |record| try out.append(alloc, try cloneForeignKeyReferenceRange(alloc, record));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeForeignKeyReferenceRanges(_: *TableManager, alloc: std.mem.Allocator, records: []ForeignKeyReferenceRangeRecord) void {
        for (records) |record| freeForeignKeyReferenceRange(alloc, record);
        alloc.free(records);
    }

    pub fn listUniqueConstraintRanges(self: *TableManager, alloc: std.mem.Allocator) ![]UniqueConstraintRangeRecord {
        var out = std.ArrayListUnmanaged(UniqueConstraintRangeRecord).empty;
        errdefer {
            for (out.items) |record| freeUniqueConstraintRange(alloc, record);
            out.deinit(alloc);
        }
        for (self.unique_constraint_ranges.items) |record| try out.append(alloc, try cloneUniqueConstraintRange(alloc, record));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeUniqueConstraintRanges(_: *TableManager, alloc: std.mem.Allocator, records: []UniqueConstraintRangeRecord) void {
        for (records) |record| freeUniqueConstraintRange(alloc, record);
        alloc.free(records);
    }

    pub fn listSecondaryIndexRebuildRanges(self: *TableManager, alloc: std.mem.Allocator) ![]SecondaryIndexRebuildRangeRecord {
        var out = std.ArrayListUnmanaged(SecondaryIndexRebuildRangeRecord).empty;
        errdefer {
            for (out.items) |record| freeSecondaryIndexRebuildRange(alloc, record);
            out.deinit(alloc);
        }
        for (self.secondary_index_rebuild_ranges.items) |record| try out.append(alloc, try cloneSecondaryIndexRebuildRange(alloc, record));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeSecondaryIndexRebuildRanges(_: *TableManager, alloc: std.mem.Allocator, records: []SecondaryIndexRebuildRangeRecord) void {
        for (records) |record| freeSecondaryIndexRebuildRange(alloc, record);
        alloc.free(records);
    }

    pub fn listSchemaRewriteJobs(self: *TableManager, alloc: std.mem.Allocator) ![]SchemaRewriteJobRecord {
        var out = std.ArrayListUnmanaged(SchemaRewriteJobRecord).empty;
        errdefer {
            for (out.items) |record| freeSchemaRewriteJob(alloc, record);
            out.deinit(alloc);
        }
        for (self.schema_rewrite_jobs.items) |record| try out.append(alloc, try cloneSchemaRewriteJob(alloc, record));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeSchemaRewriteJobs(_: *TableManager, alloc: std.mem.Allocator, records: []SchemaRewriteJobRecord) void {
        for (records) |record| freeSchemaRewriteJob(alloc, record);
        alloc.free(records);
    }

    pub fn listTableEmptyingJobs(self: *TableManager, alloc: std.mem.Allocator) ![]TableEmptyingJobRecord {
        var out = std.ArrayListUnmanaged(TableEmptyingJobRecord).empty;
        errdefer {
            for (out.items) |record| freeTableEmptyingJob(alloc, record);
            out.deinit(alloc);
        }
        for (self.table_emptying_jobs.items) |record| try out.append(alloc, try cloneTableEmptyingJob(alloc, record));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeTableEmptyingJobs(_: *TableManager, alloc: std.mem.Allocator, records: []TableEmptyingJobRecord) void {
        for (records) |record| freeTableEmptyingJob(alloc, record);
        alloc.free(records);
    }

    pub fn requestSplit(self: *TableManager, intent: SplitIntent) !void {
        try group_ids.requireDataGroupId(intent.source_group_id);
        try group_ids.requireDataGroupId(intent.destination_group_id);
        const source = self.ranges.getPtr(intent.source_group_id) orelse return error.UnknownSourceRange;
        if (source.table_id != intent.table_id) return error.TableRangeMismatch;
        if (!keyStrictlyInsideRange(intent.split_key, source.start_key, source.end_key)) return error.InvalidSplitKey;
        if (self.ranges.contains(intent.destination_group_id)) return error.DestinationRangeAlreadyExists;

        if (self.split_intents.get(intent.transition_id)) |existing| {
            if (existing.table_id != intent.table_id or
                existing.source_group_id != intent.source_group_id or
                existing.destination_group_id != intent.destination_group_id or
                (intent.attempt_epoch != 0 and intent.attempt_epoch != existing.attempt_epoch) or
                !std.mem.eql(u8, existing.split_key, intent.split_key))
            {
                return error.ConflictingSplitTransition;
            }
            return;
        }

        var allocated = intent;
        if (allocated.attempt_epoch == 0) {
            if (source.split_attempt_epoch == std.math.maxInt(u64)) return error.SplitAttemptEpochExhausted;
            allocated.attempt_epoch = source.split_attempt_epoch + 1;
        } else {
            if (allocated.attempt_epoch <= source.split_attempt_epoch) return error.StaleSplitAttempt;
        }

        const owned = try cloneSplitIntent(self.alloc, allocated);
        errdefer freeSplitIntent(self.alloc, owned);
        try self.split_intents.put(self.alloc, intent.transition_id, owned);
        // Publish the epoch only after every fallible allocation succeeds. This
        // keeps an OOM from consuming a durable fencing epoch without an intent.
        source.split_attempt_epoch = allocated.attempt_epoch;
    }

    pub fn requestMerge(self: *TableManager, intent: MergeIntent) !void {
        try group_ids.requireDataGroupId(intent.donor_group_id);
        try group_ids.requireDataGroupId(intent.receiver_group_id);
        const donor = self.ranges.get(intent.donor_group_id) orelse return error.UnknownDonorRange;
        const receiver = self.ranges.get(intent.receiver_group_id) orelse return error.UnknownReceiverRange;
        if (donor.table_id != intent.table_id or receiver.table_id != intent.table_id) return error.TableRangeMismatch;
        if (!rangesAdjacent(donor, receiver)) return error.RangesNotAdjacent;

        const owned = try cloneMergeIntent(self.alloc, intent);
        errdefer freeMergeIntent(self.alloc, owned);
        if (self.merge_intents.getPtr(intent.transition_id)) |existing| {
            freeMergeIntent(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.merge_intents.put(self.alloc, intent.transition_id, owned);
    }

    pub fn mergeRequiresDocIdentityReassignment(
        self: *const TableManager,
        donor_group_id: u64,
        receiver_group_id: u64,
    ) !bool {
        const donor = self.ranges.get(donor_group_id) orelse return error.UnknownDonorRange;
        const receiver = self.ranges.get(receiver_group_id) orelse return error.UnknownReceiverRange;
        return rangeDocIdentityShardId(donor) != rangeDocIdentityShardId(receiver) or
            rangeDocIdentityRangeId(donor) != rangeDocIdentityRangeId(receiver);
    }

    /// Rehydrate active split intents from replicated metadata after a
    /// reconciliation-authority handoff. Projected records are authoritative:
    /// they replace a local copy with the same transition ID without allocating
    /// a new fencing epoch. Local intents still awaiting admission are retained.
    pub fn syncProjectedSplitTransitions(self: *TableManager, records: []const transition_state.SplitTransitionRecord) !void {
        var hydrated = std.ArrayListUnmanaged(SplitIntent).empty;
        defer hydrated.deinit(self.alloc);
        errdefer for (hydrated.items) |intent| freeSplitIntent(self.alloc, intent);
        var active_ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer active_ids.deinit(self.alloc);

        for (records) |record| {
            if (transitionTerminal(record.phase)) continue;
            try record.table_contract.validateForSplit();
            const active = try active_ids.getOrPut(self.alloc, record.transition_id);
            if (active.found_existing) return error.DuplicateProjectedSplitTransition;

            try group_ids.requireDataGroupId(record.source_group_id);
            try group_ids.requireDataGroupId(record.destination_group_id);
            const split_key = record.split_key orelse return error.MissingSplitKey;
            if (record.transition_id == 0 or
                record.attempt_epoch == 0 or
                split_key.len == 0)
            {
                return error.InvalidProjectedSplitTransition;
            }

            const owned = try cloneSplitIntent(self.alloc, .{
                .transition_id = record.transition_id,
                .attempt_epoch = record.attempt_epoch,
                .table_id = record.table_contract.table_id,
                .source_group_id = record.source_group_id,
                .destination_group_id = record.destination_group_id,
                .split_key = split_key,
                .projected_source_range_end = record.source_range_end,
                .rollback_reason = record.rollback_reason,
                .projected = true,
                .projected_contract = record.table_contract,
            });
            errdefer freeSplitIntent(self.alloc, owned);
            try hydrated.append(self.alloc, owned);
        }

        var stale_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_ids.deinit(self.alloc);
        try stale_ids.ensureTotalCapacity(self.alloc, self.split_intents.count());
        var existing_it = self.split_intents.iterator();
        while (existing_it.next()) |entry| {
            if (entry.value_ptr.projected and !active_ids.contains(entry.key_ptr.*)) {
                stale_ids.appendAssumeCapacity(entry.key_ptr.*);
            }
        }
        try self.split_intents.ensureUnusedCapacity(self.alloc, @intCast(hydrated.items.len));

        for (stale_ids.items) |transition_id| _ = self.removeSplitIntent(transition_id);
        for (hydrated.items) |intent| {
            if (self.split_intents.getPtr(intent.transition_id)) |existing| {
                freeSplitIntent(self.alloc, existing.*);
                existing.* = intent;
            } else {
                self.split_intents.putAssumeCapacity(intent.transition_id, intent);
            }
        }
        hydrated.items.len = 0;
    }

    /// Merge counterpart to syncProjectedSplitTransitions.
    pub fn syncProjectedMergeTransitions(self: *TableManager, records: []const transition_state.MergeTransitionRecord) !void {
        var hydrated = std.ArrayListUnmanaged(MergeIntent).empty;
        defer hydrated.deinit(self.alloc);
        errdefer for (hydrated.items) |intent| freeMergeIntent(self.alloc, intent);
        var active_ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer active_ids.deinit(self.alloc);

        for (records) |record| {
            if (transitionTerminal(record.phase)) continue;
            try record.table_contract.validateForMerge(
                record.allow_doc_identity_reassignment,
            );
            const active = try active_ids.getOrPut(self.alloc, record.transition_id);
            if (active.found_existing) return error.DuplicateProjectedMergeTransition;

            try group_ids.requireDataGroupId(record.donor_group_id);
            try group_ids.requireDataGroupId(record.receiver_group_id);
            if (record.transition_id == 0)
                return error.InvalidProjectedMergeTransition;

            const owned = try cloneMergeIntent(self.alloc, .{
                .transition_id = record.transition_id,
                .table_id = record.table_contract.table_id,
                .donor_group_id = record.donor_group_id,
                .receiver_group_id = record.receiver_group_id,
                .rollback_reason = record.rollback_reason,
                .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
                .projected = true,
                .projected_contract = record.table_contract,
            });
            errdefer freeMergeIntent(self.alloc, owned);
            try hydrated.append(self.alloc, owned);
        }

        var stale_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_ids.deinit(self.alloc);
        try stale_ids.ensureTotalCapacity(self.alloc, self.merge_intents.count());
        var existing_it = self.merge_intents.iterator();
        while (existing_it.next()) |entry| {
            if (entry.value_ptr.projected and !active_ids.contains(entry.key_ptr.*)) {
                stale_ids.appendAssumeCapacity(entry.key_ptr.*);
            }
        }
        try self.merge_intents.ensureUnusedCapacity(self.alloc, @intCast(hydrated.items.len));

        for (stale_ids.items) |transition_id| _ = self.removeMergeIntent(transition_id);
        for (hydrated.items) |intent| {
            if (self.merge_intents.getPtr(intent.transition_id)) |existing| {
                freeMergeIntent(self.alloc, existing.*);
                existing.* = intent;
            } else {
                self.merge_intents.putAssumeCapacity(intent.transition_id, intent);
            }
        }
        hydrated.items.len = 0;
    }

    /// Reconstruct topology changes whose terminal transition marker committed
    /// before all range records. The marker is a write-ahead intent: replay is
    /// idempotent across authority handoff and every partially published prefix.
    pub fn applyProjectedTerminalTransitions(
        self: *TableManager,
        split_records: []const transition_state.SplitTransitionRecord,
        merge_records: []const transition_state.MergeTransitionRecord,
    ) !void {
        for (split_records) |record| switch (record.phase) {
            .finalized => {
                try self.materializeFinalizedSplit(record);
                _ = self.removeSplitIntent(record.transition_id);
            },
            .rolled_back => {
                try record.table_contract.validateForSplit();
                _ = self.removeSplitIntent(record.transition_id);
            },
            else => {},
        };
        for (merge_records) |record| switch (record.phase) {
            .finalized => {
                try self.materializeFinalizedMerge(record);
                _ = self.removeMergeIntent(record.transition_id);
            },
            .rolled_back => {
                try record.table_contract.validateForMerge(
                    record.allow_doc_identity_reassignment,
                );
                _ = self.removeMergeIntent(record.transition_id);
            },
            else => {},
        };
    }

    pub fn removeSplitIntent(self: *TableManager, transition_id: u64) bool {
        if (self.split_intents.fetchRemove(transition_id)) |entry| {
            freeSplitIntent(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    pub fn removeMergeIntent(self: *TableManager, transition_id: u64) bool {
        if (self.merge_intents.fetchRemove(transition_id)) |entry| {
            freeMergeIntent(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    pub fn applyFinalizedSplit(self: *TableManager, record: transition_state.SplitTransitionRecord) !void {
        if (!self.split_intents.contains(record.transition_id)) return;
        try self.materializeFinalizedSplit(record);
        _ = self.removeSplitIntent(record.transition_id);
    }

    fn materializeFinalizedSplit(self: *TableManager, record: transition_state.SplitTransitionRecord) !void {
        try record.table_contract.validateForSplit();
        try self.validateTransitionTable(record.table_contract);
        const split_key = record.split_key orelse return error.MissingSplitKey;
        const source = self.ranges.get(record.source_group_id) orelse return error.UnknownSourceRange;
        if (source.table_id != record.table_contract.table_id)
            return error.TransitionTopologyConflict;
        if (source.split_attempt_epoch != record.attempt_epoch)
            return error.StaleSplitAttempt;
        if (!rangeMatchesTransitionIdentity(source, record.table_contract.source_identity))
            return error.DocIdentityNamespaceMismatch;
        if (std.mem.order(u8, split_key, source.start_key) != .gt)
            return error.InvalidSplitKey;
        if (record.source_range_end) |source_range_end| {
            if (std.mem.order(u8, split_key, source_range_end) != .lt)
                return error.InvalidSplitKey;
        }
        const source_already_narrowed = source.end_key != null and
            std.mem.eql(u8, source.end_key.?, split_key);
        if (!source_already_narrowed and
            !optionalBytesEqual(source.end_key, record.source_range_end))
        {
            return error.TransitionTopologyConflict;
        }
        const identity_shard_id = rangeDocIdentityShardId(source);
        const identity_range_id = rangeDocIdentityRangeId(source);
        const table_id = source.table_id;
        const completion_fingerprint = source.completed_restore_fingerprint;

        if (!source_already_narrowed) {
            try self.upsertRange(.{
                .group_id = source.group_id,
                .range_id = source.range_id,
                .table_id = table_id,
                .start_key = source.start_key,
                .end_key = split_key,
                .doc_identity_shard_id = source.doc_identity_shard_id,
                .doc_identity_range_id = source.doc_identity_range_id,
                .split_attempt_epoch = source.split_attempt_epoch,
                .completed_restore_fingerprint = completion_fingerprint,
            });
        }

        if (self.ranges.get(record.destination_group_id)) |destination| {
            if (destination.range_id != record.destination_group_id or
                destination.table_id != table_id or
                !std.mem.eql(u8, destination.start_key, split_key) or
                !optionalBytesEqual(destination.end_key, record.source_range_end) or
                destination.split_attempt_epoch != 0 or
                !rangeMatchesTransitionIdentity(
                    destination,
                    record.table_contract.target_identity,
                ) or
                !std.mem.eql(
                    u8,
                    &destination.completed_restore_fingerprint,
                    &completion_fingerprint,
                ))
            {
                return error.TransitionTopologyConflict;
            }
        } else {
            try self.upsertRange(.{
                .group_id = record.destination_group_id,
                .range_id = record.destination_group_id,
                .table_id = table_id,
                .start_key = split_key,
                .end_key = record.source_range_end,
                .doc_identity_shard_id = identity_shard_id,
                .doc_identity_range_id = identity_range_id,
                .split_attempt_epoch = 0,
                .completed_restore_fingerprint = completion_fingerprint,
            });
        }
    }

    pub fn applyRolledBackSplit(self: *TableManager, transition_id: u64) void {
        _ = self.removeSplitIntent(transition_id);
    }

    pub fn applyFinalizedMerge(self: *TableManager, record: transition_state.MergeTransitionRecord) !void {
        if (!self.merge_intents.contains(record.transition_id)) return;
        try self.materializeFinalizedMerge(record);
        _ = self.removeMergeIntent(record.transition_id);
    }

    fn materializeFinalizedMerge(self: *TableManager, record: transition_state.MergeTransitionRecord) !void {
        try record.table_contract.validateForMerge(
            record.allow_doc_identity_reassignment,
        );
        try self.validateTransitionTable(record.table_contract);
        const receiver = self.ranges.get(record.receiver_group_id) orelse return error.UnknownReceiverRange;
        if (receiver.table_id != record.table_contract.table_id)
            return error.TransitionTopologyConflict;
        if (!rangeMatchesTransitionIdentity(receiver, record.table_contract.target_identity))
            return error.DocIdentityNamespaceMismatch;

        const donor = self.ranges.get(record.donor_group_id) orelse return;
        if (donor.table_id != record.table_contract.table_id)
            return error.TransitionTopologyConflict;
        if (!rangeMatchesTransitionIdentity(donor, record.table_contract.source_identity))
            return error.DocIdentityNamespaceMismatch;
        const merged_start = if (std.mem.order(u8, donor.start_key, receiver.start_key) == .lt) donor.start_key else receiver.start_key;
        const merged_end = switch (optionalBytesOrder(donor.end_key, receiver.end_key)) {
            .lt => receiver.end_key,
            .eq => receiver.end_key,
            .gt => donor.end_key,
        };
        const completed_restore_fingerprint = if (std.mem.eql(
            u8,
            &donor.completed_restore_fingerprint,
            &receiver.completed_restore_fingerprint,
        ))
            receiver.completed_restore_fingerprint
        else
            empty_restore_completion_fingerprint;
        const receiver_already_merged =
            std.mem.eql(u8, receiver.start_key, merged_start) and
            optionalBytesEqual(receiver.end_key, merged_end);
        if (!receiver_already_merged and !rangesAdjacent(donor, receiver))
            return error.TransitionTopologyConflict;

        if (receiver_already_merged) {
            if (!std.mem.eql(
                u8,
                &receiver.completed_restore_fingerprint,
                &completed_restore_fingerprint,
            )) {
                return error.TransitionTopologyConflict;
            }
        } else {
            try self.upsertRange(.{
                .group_id = receiver.group_id,
                .range_id = receiver.range_id,
                .table_id = receiver.table_id,
                .start_key = merged_start,
                .end_key = merged_end,
                .doc_identity_shard_id = receiver.doc_identity_shard_id,
                .doc_identity_range_id = receiver.doc_identity_range_id,
                .completed_restore_fingerprint = completed_restore_fingerprint,
            });
        }
        _ = self.removeRange(donor.group_id);
    }

    fn validateTransitionTable(
        self: *const TableManager,
        contract: transition_state.TransitionTableContract,
    ) !void {
        const table = self.tables.get(contract.table_id) orelse
            return error.TransitionTableContractViolated;
        if (!std.mem.eql(u8, table.name, contract.table_name) or
            !std.mem.eql(u8, table.schema_json, contract.schema_json) or
            !std.mem.eql(u8, table.indexes_json, contract.indexes_json))
        {
            return error.TransitionTableContractViolated;
        }
    }

    pub fn applyRolledBackMerge(self: *TableManager, transition_id: u64) void {
        _ = self.removeMergeIntent(transition_id);
    }

    pub fn listDesiredSplitTransitions(self: *TableManager, alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
        var out = std.ArrayListUnmanaged(transition_state.SplitTransitionRecord).empty;
        errdefer {
            for (out.items) |record| freeSplitTransitionRecord(alloc, record);
            out.deinit(alloc);
        }

        var it = self.split_intents.valueIterator();
        while (it.next()) |intent| {
            const owned = try self.buildSplitTransition(alloc, intent.*);
            errdefer freeSplitTransitionRecord(alloc, owned);
            try out.append(alloc, owned);
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn listDesiredMergeTransitions(self: *TableManager, alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
        var out = std.ArrayListUnmanaged(transition_state.MergeTransitionRecord).empty;
        errdefer {
            for (out.items) |record| freeMergeTransitionRecord(alloc, record);
            out.deinit(alloc);
        }

        var it = self.merge_intents.valueIterator();
        while (it.next()) |intent| {
            const table_contract = if (intent.projected) blk: {
                break :blk intent.projected_contract orelse
                    return error.ProjectedTransitionContractMissing;
            } else blk: {
                const donor = self.ranges.get(intent.donor_group_id) orelse
                    return error.UnknownDonorRange;
                const receiver = self.ranges.get(intent.receiver_group_id) orelse
                    return error.UnknownReceiverRange;
                if (donor.table_id != receiver.table_id)
                    return error.MergeTableMismatch;
                const table = self.tables.get(receiver.table_id) orelse
                    return error.UnknownTable;
                break :blk transitionTableContract(table, donor, receiver);
            };
            const owned = try cloneMergeTransitionRecord(alloc, .{
                .transition_id = intent.transition_id,
                .donor_group_id = intent.donor_group_id,
                .receiver_group_id = intent.receiver_group_id,
                .phase = .prepare,
                .rollback_reason = intent.rollback_reason,
                .allow_doc_identity_reassignment = intent.allow_doc_identity_reassignment,
                .table_contract = table_contract,
            });
            errdefer freeMergeTransitionRecord(alloc, owned);
            try out.append(alloc, owned);
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeSplitTransitions(_: *TableManager, alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
        for (records) |record| freeSplitTransitionRecord(alloc, record);
        alloc.free(records);
    }

    pub fn freeMergeTransitions(_: *TableManager, alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
        for (records) |record| freeMergeTransitionRecord(alloc, record);
        alloc.free(records);
    }

    fn buildSplitTransition(
        self: *TableManager,
        alloc: std.mem.Allocator,
        intent: SplitIntent,
    ) !transition_state.SplitTransitionRecord {
        if (intent.projected) {
            const table_contract = intent.projected_contract orelse
                return error.ProjectedTransitionContractMissing;
            return try cloneSplitTransitionRecord(alloc, .{
                .transition_id = intent.transition_id,
                .attempt_epoch = intent.attempt_epoch,
                .source_group_id = intent.source_group_id,
                .destination_group_id = intent.destination_group_id,
                .phase = .prepare,
                .split_key = intent.split_key,
                .source_range_end = intent.projected_source_range_end,
                .rollback_reason = intent.rollback_reason,
                .table_contract = table_contract,
            });
        }
        const source = self.ranges.get(intent.source_group_id) orelse return error.UnknownSourceRange;
        const table = self.tables.get(source.table_id) orelse return error.UnknownTable;
        return try cloneSplitTransitionRecord(alloc, .{
            .transition_id = intent.transition_id,
            .attempt_epoch = intent.attempt_epoch,
            .source_group_id = intent.source_group_id,
            .destination_group_id = intent.destination_group_id,
            .phase = .prepare,
            .split_key = intent.split_key,
            .source_range_end = source.end_key,
            .rollback_reason = intent.rollback_reason,
            .table_contract = transitionTableContract(table, source, source),
        });
    }
};

fn transitionTableContract(
    table: TableRecord,
    source: RangeRecord,
    target: RangeRecord,
) transition_state.TransitionTableContract {
    return .{
        .table_id = table.table_id,
        .table_name = table.name,
        .schema_json = table.schema_json,
        .indexes_json = table.indexes_json,
        .source_identity = .{
            .shard_id = rangeDocIdentityShardId(source),
            .range_id = rangeDocIdentityRangeId(source),
        },
        .target_identity = .{
            .shard_id = rangeDocIdentityShardId(target),
            .range_id = rangeDocIdentityRangeId(target),
        },
    };
}

pub fn parsePlacementClass(role: []const u8) ?PlacementClass {
    inline for (comptime std.meta.fields(PlacementClass)) |field| {
        if (std.mem.eql(u8, role, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

pub fn placementRoleCompatible(table_role: []const u8, store_role: []const u8) bool {
    if (table_role.len == 0) return true;
    const table_class = parsePlacementClass(table_role) orelse return std.mem.eql(u8, table_role, store_role);
    const store_class = parsePlacementClass(store_role) orelse return std.mem.eql(u8, table_role, store_role);
    return table_class == store_class;
}

fn keyStrictlyInsideRange(key: []const u8, start_key: []const u8, end_key: ?[]const u8) bool {
    if (std.mem.order(u8, key, start_key) != .gt) return false;
    if (end_key) |end| {
        if (std.mem.order(u8, key, end) != .lt) return false;
    }
    return true;
}

fn u64SliceContains(values: []const u64, needle: u64) bool {
    for (values) |value| {
        if (value == needle) return true;
    }
    return false;
}

fn u64SlicesEqual(a: []const u64, b: []const u64) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left != right) return false;
    }
    return true;
}

fn optionalStringSlicesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn promotionListContainsTable(promotions: []const TableEmptyingBarrierPromotion, table_id: u64) bool {
    for (promotions) |promotion| {
        if (promotion.table_id == table_id) return true;
    }
    return false;
}

fn rangesAdjacent(a: RangeRecord, b: RangeRecord) bool {
    if (a.end_key) |a_end| {
        if (std.mem.eql(u8, a_end, b.start_key)) return true;
    }
    if (b.end_key) |b_end| {
        if (std.mem.eql(u8, b_end, a.start_key)) return true;
    }
    return false;
}

fn optionalBytesOrder(a: ?[]const u8, b: ?[]const u8) std.math.Order {
    const a_bytes = a orelse return if (b == null) .eq else .gt;
    const b_bytes = b orelse return .lt;
    return std.mem.order(u8, a_bytes, b_bytes);
}

fn optionalBytesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

fn transitionTerminal(phase: transition_state.TransitionPhase) bool {
    return phase == .finalized or phase == .rolled_back;
}

fn cloneOwnedOptional(alloc: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    return if (value) |bytes| try alloc.dupe(u8, bytes) else null;
}

fn freeOwnedOptional(alloc: std.mem.Allocator, value: ?[]const u8) void {
    if (value) |bytes| alloc.free(bytes);
}

pub fn cloneDatabase(alloc: std.mem.Allocator, record: DatabaseRecord) !DatabaseRecord {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const settings_json = try alloc.dupe(u8, record.settings_json);
    errdefer alloc.free(settings_json);
    const tablespace_name = try alloc.dupe(u8, record.tablespace_name);
    errdefer alloc.free(tablespace_name);
    return .{
        .database_id = record.database_id,
        .name = name,
        .settings_json = settings_json,
        .tablespace_name = tablespace_name,
    };
}

pub fn freeDatabase(alloc: std.mem.Allocator, record: DatabaseRecord) void {
    alloc.free(record.name);
    alloc.free(record.settings_json);
    alloc.free(record.tablespace_name);
}

pub fn cloneNamespace(alloc: std.mem.Allocator, record: NamespaceRecord) !NamespaceRecord {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const tablespace_name = try alloc.dupe(u8, record.tablespace_name);
    errdefer alloc.free(tablespace_name);
    return .{
        .namespace_id = record.namespace_id,
        .database_id = record.database_id,
        .name = name,
        .tablespace_name = tablespace_name,
    };
}

pub fn freeNamespace(alloc: std.mem.Allocator, record: NamespaceRecord) void {
    alloc.free(record.name);
    alloc.free(record.tablespace_name);
}

pub fn cloneTablespace(alloc: std.mem.Allocator, record: TablespaceRecord) !TablespaceRecord {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const location_json = try alloc.dupe(u8, record.location_json);
    errdefer alloc.free(location_json);
    const placement_policy_json = try alloc.dupe(u8, record.placement_policy_json);
    errdefer alloc.free(placement_policy_json);
    return .{
        .tablespace_id = record.tablespace_id,
        .name = name,
        .location_json = location_json,
        .placement_policy_json = placement_policy_json,
    };
}

pub fn freeTablespace(alloc: std.mem.Allocator, record: TablespaceRecord) void {
    alloc.free(record.name);
    alloc.free(record.location_json);
    alloc.free(record.placement_policy_json);
}

pub fn cloneSequence(alloc: std.mem.Allocator, record: SequenceRecord) !SequenceRecord {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const database_name = try alloc.dupe(u8, record.database_name);
    errdefer alloc.free(database_name);
    const namespace_name = try alloc.dupe(u8, record.namespace_name);
    errdefer alloc.free(namespace_name);
    const options_json = try alloc.dupe(u8, record.options_json);
    errdefer alloc.free(options_json);
    return .{
        .sequence_id = record.sequence_id,
        .name = name,
        .database_name = database_name,
        .namespace_name = namespace_name,
        .options_json = options_json,
        .last_value = record.last_value,
        .last_allocation_id = record.last_allocation_id,
    };
}

pub fn freeSequence(alloc: std.mem.Allocator, record: SequenceRecord) void {
    alloc.free(record.name);
    alloc.free(record.database_name);
    alloc.free(record.namespace_name);
    alloc.free(record.options_json);
}

pub fn cloneTable(alloc: std.mem.Allocator, record: TableRecord) !TableRecord {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const database_name = try alloc.dupe(u8, record.database_name);
    errdefer alloc.free(database_name);
    const namespace_name = try alloc.dupe(u8, record.namespace_name);
    errdefer alloc.free(namespace_name);
    const description = try alloc.dupe(u8, record.description);
    errdefer alloc.free(description);
    const schema_json = try alloc.dupe(u8, record.schema_json);
    errdefer alloc.free(schema_json);
    const read_schema_json = try alloc.dupe(u8, record.read_schema_json);
    errdefer alloc.free(read_schema_json);
    const foreign_key_validation_json = try alloc.dupe(u8, record.foreign_key_validation_json);
    errdefer alloc.free(foreign_key_validation_json);
    const indexes_json = try alloc.dupe(u8, record.indexes_json);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try alloc.dupe(u8, record.replication_sources_json);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try alloc.dupe(u8, record.placement_role);
    errdefer alloc.free(placement_role);
    const tablespace_name = try alloc.dupe(u8, record.tablespace_name);
    errdefer alloc.free(tablespace_name);
    const restore_backup_id = try alloc.dupe(u8, record.restore_backup_id);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try alloc.dupe(u8, record.restore_location);
    errdefer alloc.free(restore_location);
    return .{
        .table_id = record.table_id,
        .name = name,
        .database_name = database_name,
        .namespace_name = namespace_name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .foreign_key_validation_json = foreign_key_validation_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .tablespace_name = tablespace_name,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .desired_replica_count = record.desired_replica_count,
        .min_ranges = record.min_ranges,
        .data_generation = record.data_generation,
    };
}

pub fn freeTable(alloc: std.mem.Allocator, record: TableRecord) void {
    alloc.free(record.name);
    alloc.free(record.database_name);
    alloc.free(record.namespace_name);
    alloc.free(record.description);
    alloc.free(record.schema_json);
    alloc.free(record.read_schema_json);
    alloc.free(record.foreign_key_validation_json);
    alloc.free(record.indexes_json);
    alloc.free(record.replication_sources_json);
    alloc.free(record.placement_role);
    alloc.free(record.tablespace_name);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_location);
}

pub fn cloneRange(alloc: std.mem.Allocator, record: RangeRecord) !RangeRecord {
    const start_key = try alloc.dupe(u8, record.start_key);
    errdefer alloc.free(start_key);
    const end_key = try cloneOwnedOptional(alloc, record.end_key);
    errdefer freeOwnedOptional(alloc, end_key);
    const restore_backup_id = try alloc.dupe(u8, record.restore_backup_id);
    errdefer alloc.free(restore_backup_id);
    const restore_artifact_backup_id = try alloc.dupe(u8, record.restore_artifact_backup_id);
    errdefer alloc.free(restore_artifact_backup_id);
    const restore_location = try alloc.dupe(u8, record.restore_location);
    errdefer alloc.free(restore_location);
    const restore_snapshot_path = try alloc.dupe(u8, record.restore_snapshot_path);
    errdefer alloc.free(restore_snapshot_path);
    const restore_connection = try alloc.dupe(u8, record.restore_connection);
    errdefer alloc.free(restore_connection);
    const restore_artifact_sha256 = try alloc.dupe(u8, record.restore_artifact_sha256);
    errdefer alloc.free(restore_artifact_sha256);
    return .{
        .group_id = record.group_id,
        .range_id = if (record.range_id == 0) record.group_id else record.range_id,
        .table_id = record.table_id,
        .start_key = start_key,
        .end_key = end_key,
        .doc_identity_shard_id = record.doc_identity_shard_id,
        .doc_identity_range_id = record.doc_identity_range_id,
        .split_attempt_epoch = record.split_attempt_epoch,
        .restore_backup_id = restore_backup_id,
        .restore_artifact_backup_id = restore_artifact_backup_id,
        .restore_location = restore_location,
        .restore_snapshot_path = restore_snapshot_path,
        .restore_connection = restore_connection,
        .restore_artifact_size_bytes = record.restore_artifact_size_bytes,
        .restore_artifact_sha256 = restore_artifact_sha256,
        .completed_restore_fingerprint = record.completed_restore_fingerprint,
    };
}

pub fn rangeDocIdentityShardId(record: RangeRecord) u64 {
    return if (record.doc_identity_shard_id == 0) record.group_id else record.doc_identity_shard_id;
}

pub fn rangeDocIdentityRangeId(record: RangeRecord) u64 {
    if (record.doc_identity_range_id != 0) return record.doc_identity_range_id;
    return if (record.range_id == 0) record.group_id else record.range_id;
}

fn rangeMatchesTransitionIdentity(
    record: RangeRecord,
    identity: transition_state.TransitionIdentity,
) bool {
    return rangeDocIdentityShardId(record) == identity.shard_id and
        rangeDocIdentityRangeId(record) == identity.range_id;
}

pub fn freeRange(alloc: std.mem.Allocator, record: RangeRecord) void {
    alloc.free(record.start_key);
    freeOwnedOptional(alloc, record.end_key);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_artifact_backup_id);
    alloc.free(record.restore_location);
    alloc.free(record.restore_snapshot_path);
    alloc.free(record.restore_connection);
    alloc.free(record.restore_artifact_sha256);
}

fn foreignKeyReferenceRangeIdentityMatches(a: ForeignKeyReferenceRangeRecord, b: ForeignKeyReferenceRangeRecord) bool {
    return a.child_table_id == b.child_table_id and
        a.parent_table_id == b.parent_table_id and
        std.mem.eql(u8, a.constraint_name, b.constraint_name) and
        (a.range_id == 0 or b.range_id == 0 or a.range_id == b.range_id) and
        std.mem.eql(u8, a.start_parent_key, b.start_parent_key);
}

fn foreignKeyReferenceRangeConstraintMatches(a: ForeignKeyReferenceRangeRecord, b: ForeignKeyReferenceRangeRecord) bool {
    return a.child_table_id == b.child_table_id and
        a.parent_table_id == b.parent_table_id and
        std.mem.eql(u8, a.constraint_name, b.constraint_name);
}

fn foreignKeyReferenceRangesOverlap(a: ForeignKeyReferenceRangeRecord, b: ForeignKeyReferenceRangeRecord) bool {
    if (a.end_parent_key) |a_end| {
        if (std.mem.order(u8, a_end, b.start_parent_key) != .gt) return false;
    }
    if (b.end_parent_key) |b_end| {
        if (std.mem.order(u8, b_end, a.start_parent_key) != .gt) return false;
    }
    return true;
}

fn foreignKeyReferenceRangesAdjacent(left: ForeignKeyReferenceRangeRecord, right: ForeignKeyReferenceRangeRecord) bool {
    if (left.child_table_id != right.child_table_id) return false;
    if (left.parent_table_id != right.parent_table_id) return false;
    if (!std.mem.eql(u8, left.constraint_name, right.constraint_name)) return false;
    const left_end = left.end_parent_key orelse return false;
    return std.mem.eql(u8, left_end, right.start_parent_key);
}

fn uniqueConstraintRangeIdentityMatches(a: UniqueConstraintRangeRecord, b: UniqueConstraintRangeRecord) bool {
    return a.table_id == b.table_id and
        std.mem.eql(u8, a.constraint_name, b.constraint_name) and
        (a.range_id == 0 or b.range_id == 0 or a.range_id == b.range_id) and
        std.mem.eql(u8, a.start_encoded_value, b.start_encoded_value);
}

fn uniqueConstraintRangeConstraintMatches(a: UniqueConstraintRangeRecord, b: UniqueConstraintRangeRecord) bool {
    return a.table_id == b.table_id and
        std.mem.eql(u8, a.constraint_name, b.constraint_name);
}

fn uniqueConstraintRangesOverlap(a: UniqueConstraintRangeRecord, b: UniqueConstraintRangeRecord) bool {
    if (a.end_encoded_value) |a_end| {
        if (std.mem.order(u8, a_end, b.start_encoded_value) != .gt) return false;
    }
    if (b.end_encoded_value) |b_end| {
        if (std.mem.order(u8, b_end, a.start_encoded_value) != .gt) return false;
    }
    return true;
}

fn uniqueConstraintRangesAdjacent(left: UniqueConstraintRangeRecord, right: UniqueConstraintRangeRecord) bool {
    if (left.table_id != right.table_id) return false;
    if (!std.mem.eql(u8, left.constraint_name, right.constraint_name)) return false;
    const left_end = left.end_encoded_value orelse return false;
    return std.mem.eql(u8, left_end, right.start_encoded_value);
}

fn mergedForeignKeyReferenceRangeId(merged_group_id: u64, left: ForeignKeyReferenceRangeRecord, right: ForeignKeyReferenceRangeRecord) u64 {
    if (merged_group_id == left.group_id) return left.range_id;
    if (merged_group_id == right.group_id) return right.range_id;
    return merged_group_id;
}

fn mergedUniqueConstraintRangeId(merged_group_id: u64, left: UniqueConstraintRangeRecord, right: UniqueConstraintRangeRecord) u64 {
    if (merged_group_id == left.group_id) return left.range_id;
    if (merged_group_id == right.group_id) return right.range_id;
    return merged_group_id;
}

fn secondaryIndexRebuildRangeIdentityMatches(a: SecondaryIndexRebuildRangeRecord, b: SecondaryIndexRebuildRangeRecord) bool {
    return a.table_id == b.table_id and
        a.index_generation == b.index_generation and
        std.mem.eql(u8, a.index_name, b.index_name) and
        (a.range_id == 0 or b.range_id == 0 or a.range_id == b.range_id) and
        std.mem.eql(u8, a.start_row_key, b.start_row_key);
}

fn secondaryIndexRebuildRangeIndexMatches(a: SecondaryIndexRebuildRangeRecord, b: SecondaryIndexRebuildRangeRecord) bool {
    return a.table_id == b.table_id and
        a.index_generation == b.index_generation and
        std.mem.eql(u8, a.index_name, b.index_name);
}

fn secondaryIndexRebuildRangesOverlap(a: SecondaryIndexRebuildRangeRecord, b: SecondaryIndexRebuildRangeRecord) bool {
    if (a.end_row_key) |a_end| {
        if (std.mem.order(u8, a_end, b.start_row_key) != .gt) return false;
    }
    if (b.end_row_key) |b_end| {
        if (std.mem.order(u8, b_end, a.start_row_key) != .gt) return false;
    }
    return true;
}

pub fn cloneForeignKeyReferenceRange(alloc: std.mem.Allocator, record: ForeignKeyReferenceRangeRecord) !ForeignKeyReferenceRangeRecord {
    const constraint_name = try alloc.dupe(u8, record.constraint_name);
    errdefer alloc.free(constraint_name);
    const start_parent_key = try alloc.dupe(u8, record.start_parent_key);
    errdefer alloc.free(start_parent_key);
    const end_parent_key = try cloneOwnedOptional(alloc, record.end_parent_key);
    errdefer freeOwnedOptional(alloc, end_parent_key);
    const state = try alloc.dupe(u8, record.state);
    errdefer alloc.free(state);
    return .{
        .child_table_id = record.child_table_id,
        .constraint_name = constraint_name,
        .parent_table_id = record.parent_table_id,
        .start_parent_key = start_parent_key,
        .end_parent_key = end_parent_key,
        .group_id = record.group_id,
        .range_id = record.range_id,
        .topology_epoch = record.topology_epoch,
        .state = state,
    };
}

pub fn freeForeignKeyReferenceRange(alloc: std.mem.Allocator, record: ForeignKeyReferenceRangeRecord) void {
    alloc.free(record.constraint_name);
    alloc.free(record.start_parent_key);
    freeOwnedOptional(alloc, record.end_parent_key);
    alloc.free(record.state);
}

pub fn cloneUniqueConstraintRange(alloc: std.mem.Allocator, record: UniqueConstraintRangeRecord) !UniqueConstraintRangeRecord {
    const constraint_name = try alloc.dupe(u8, record.constraint_name);
    errdefer alloc.free(constraint_name);
    const start_encoded_value = try alloc.dupe(u8, record.start_encoded_value);
    errdefer alloc.free(start_encoded_value);
    const end_encoded_value = try cloneOwnedOptional(alloc, record.end_encoded_value);
    errdefer freeOwnedOptional(alloc, end_encoded_value);
    const state = try alloc.dupe(u8, record.state);
    errdefer alloc.free(state);
    return .{
        .table_id = record.table_id,
        .constraint_name = constraint_name,
        .start_encoded_value = start_encoded_value,
        .end_encoded_value = end_encoded_value,
        .group_id = record.group_id,
        .range_id = record.range_id,
        .topology_epoch = record.topology_epoch,
        .state = state,
    };
}

pub fn freeUniqueConstraintRange(alloc: std.mem.Allocator, record: UniqueConstraintRangeRecord) void {
    alloc.free(record.constraint_name);
    alloc.free(record.start_encoded_value);
    freeOwnedOptional(alloc, record.end_encoded_value);
    alloc.free(record.state);
}

pub fn cloneSecondaryIndexRebuildRange(alloc: std.mem.Allocator, record: SecondaryIndexRebuildRangeRecord) !SecondaryIndexRebuildRangeRecord {
    const index_name = try alloc.dupe(u8, record.index_name);
    errdefer alloc.free(index_name);
    const start_row_key = try alloc.dupe(u8, record.start_row_key);
    errdefer alloc.free(start_row_key);
    const end_row_key = try cloneOwnedOptional(alloc, record.end_row_key);
    errdefer freeOwnedOptional(alloc, end_row_key);
    const state = try alloc.dupe(u8, record.state);
    errdefer alloc.free(state);
    const lease_owner = try alloc.dupe(u8, record.lease_owner);
    errdefer alloc.free(lease_owner);
    const progress_row_key = try alloc.dupe(u8, record.progress_row_key);
    errdefer alloc.free(progress_row_key);
    const last_error = try alloc.dupe(u8, record.last_error);
    errdefer alloc.free(last_error);
    return .{
        .table_id = record.table_id,
        .index_name = index_name,
        .index_generation = record.index_generation,
        .start_row_key = start_row_key,
        .end_row_key = end_row_key,
        .group_id = record.group_id,
        .range_id = record.range_id,
        .topology_epoch = record.topology_epoch,
        .state = state,
        .lease_owner = lease_owner,
        .lease_expires_at_ms = record.lease_expires_at_ms,
        .attempts = record.attempts,
        .completed_row_count = record.completed_row_count,
        .progress_row_key = progress_row_key,
        .last_error = last_error,
    };
}

pub fn freeSecondaryIndexRebuildRange(alloc: std.mem.Allocator, record: SecondaryIndexRebuildRangeRecord) void {
    alloc.free(record.index_name);
    alloc.free(record.start_row_key);
    freeOwnedOptional(alloc, record.end_row_key);
    alloc.free(record.state);
    alloc.free(record.lease_owner);
    alloc.free(record.progress_row_key);
    alloc.free(record.last_error);
}

pub fn cloneSchemaRewriteJob(alloc: std.mem.Allocator, record: SchemaRewriteJobRecord) !SchemaRewriteJobRecord {
    const action = try alloc.dupe(u8, record.action);
    errdefer alloc.free(action);
    const reason = try alloc.dupe(u8, record.reason);
    errdefer alloc.free(reason);
    const start_row_key = try alloc.dupe(u8, record.start_row_key);
    errdefer alloc.free(start_row_key);
    const end_row_key = if (record.end_row_key) |value| try alloc.dupe(u8, value) else null;
    errdefer if (end_row_key) |value| alloc.free(value);
    const state = try alloc.dupe(u8, record.state);
    errdefer alloc.free(state);
    const target_column = try alloc.dupe(u8, record.target_column);
    errdefer alloc.free(target_column);
    const expression = if (record.expression) |expr| try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, expr) else null;
    errdefer if (expression) |expr| runtime_schema.freeRelationalRowsExpression(alloc, expr);
    const rewrite_renames = try alloc.alloc(SchemaRewriteRename, record.rewrite_renames.len);
    var rewrite_renames_initialized: usize = 0;
    errdefer {
        for (rewrite_renames[0..rewrite_renames_initialized]) |rename| {
            alloc.free(rename.old_path);
            alloc.free(rename.new_path);
        }
        alloc.free(rewrite_renames);
    }
    for (record.rewrite_renames, 0..) |rename, i| {
        const old_path = try alloc.dupe(u8, rename.old_path);
        errdefer alloc.free(old_path);
        const new_path = try alloc.dupe(u8, rename.new_path);
        rewrite_renames[i] = .{ .old_path = old_path, .new_path = new_path };
        rewrite_renames_initialized += 1;
    }
    const rewrite_drops = try alloc.alloc([]const u8, record.rewrite_drops.len);
    var rewrite_drops_initialized: usize = 0;
    errdefer {
        for (rewrite_drops[0..rewrite_drops_initialized]) |drop| alloc.free(drop);
        alloc.free(rewrite_drops);
    }
    for (record.rewrite_drops, 0..) |drop, i| {
        rewrite_drops[i] = try alloc.dupe(u8, drop);
        rewrite_drops_initialized += 1;
    }
    const lease_owner = try alloc.dupe(u8, record.lease_owner);
    errdefer alloc.free(lease_owner);
    const progress_row_key = try alloc.dupe(u8, record.progress_row_key);
    errdefer alloc.free(progress_row_key);
    const last_error = try alloc.dupe(u8, record.last_error);
    errdefer alloc.free(last_error);
    return .{
        .job_id = record.job_id,
        .table_id = record.table_id,
        .group_id = record.group_id,
        .range_id = record.range_id,
        .schema_generation = record.schema_generation,
        .action = action,
        .reason = reason,
        .start_row_key = start_row_key,
        .end_row_key = end_row_key,
        .state = state,
        .target_column = target_column,
        .expression = expression,
        .full_row_rewrite = record.full_row_rewrite,
        .rewrite_renames = rewrite_renames,
        .rewrite_drops = rewrite_drops,
        .lease_owner = lease_owner,
        .lease_expires_at_ms = record.lease_expires_at_ms,
        .attempts = record.attempts,
        .completed_row_count = record.completed_row_count,
        .progress_row_key = progress_row_key,
        .last_error = last_error,
    };
}

pub fn freeSchemaRewriteJob(alloc: std.mem.Allocator, record: SchemaRewriteJobRecord) void {
    alloc.free(record.action);
    alloc.free(record.reason);
    alloc.free(record.start_row_key);
    freeOwnedOptional(alloc, record.end_row_key);
    alloc.free(record.state);
    alloc.free(record.target_column);
    if (record.expression) |expr| runtime_schema.freeRelationalRowsExpression(alloc, expr);
    for (record.rewrite_renames) |rename| {
        alloc.free(rename.old_path);
        alloc.free(rename.new_path);
    }
    alloc.free(record.rewrite_renames);
    for (record.rewrite_drops) |drop| alloc.free(drop);
    alloc.free(record.rewrite_drops);
    alloc.free(record.lease_owner);
    alloc.free(record.progress_row_key);
    alloc.free(record.last_error);
}

pub fn cloneTableEmptyingJob(alloc: std.mem.Allocator, record: TableEmptyingJobRecord) !TableEmptyingJobRecord {
    const affected_table_ids = try alloc.dupe(u64, record.affected_table_ids);
    errdefer alloc.free(affected_table_ids);
    const start_row_key = try alloc.dupe(u8, record.start_row_key);
    errdefer alloc.free(start_row_key);
    const end_row_key = if (record.end_row_key) |end_row_key| try alloc.dupe(u8, end_row_key) else null;
    errdefer if (end_row_key) |owned| alloc.free(owned);
    const state = try alloc.dupe(u8, record.state);
    errdefer alloc.free(state);
    const lease_owner = try alloc.dupe(u8, record.lease_owner);
    errdefer alloc.free(lease_owner);
    const progress_row_key = try alloc.dupe(u8, record.progress_row_key);
    errdefer alloc.free(progress_row_key);
    const last_error = try alloc.dupe(u8, record.last_error);
    errdefer alloc.free(last_error);
    return .{
        .job_id = record.job_id,
        .table_id = record.table_id,
        .group_id = record.group_id,
        .range_id = record.range_id,
        .schema_generation = record.schema_generation,
        .data_generation = record.data_generation,
        .barrier_id = record.barrier_id,
        .start_row_key = start_row_key,
        .end_row_key = end_row_key,
        .affected_table_ids = affected_table_ids,
        .restart_identity = record.restart_identity,
        .cascade = record.cascade,
        .state = state,
        .lease_owner = lease_owner,
        .lease_expires_at_ms = record.lease_expires_at_ms,
        .attempts = record.attempts,
        .completed_row_count = record.completed_row_count,
        .progress_row_key = progress_row_key,
        .last_error = last_error,
    };
}

pub fn freeTableEmptyingJob(alloc: std.mem.Allocator, record: TableEmptyingJobRecord) void {
    alloc.free(record.start_row_key);
    if (record.end_row_key) |end_row_key| alloc.free(end_row_key);
    alloc.free(record.affected_table_ids);
    alloc.free(record.state);
    alloc.free(record.lease_owner);
    alloc.free(record.progress_row_key);
    alloc.free(record.last_error);
}

pub fn cloneRestoreProgress(alloc: std.mem.Allocator, record: RestoreProgressRecord) !RestoreProgressRecord {
    const backup_id = try alloc.dupe(u8, record.backup_id);
    errdefer alloc.free(backup_id);
    const artifact_backup_id = try alloc.dupe(u8, record.artifact_backup_id);
    errdefer alloc.free(artifact_backup_id);
    const location = try alloc.dupe(u8, record.location);
    errdefer alloc.free(location);
    const snapshot_path = try alloc.dupe(u8, record.snapshot_path);
    errdefer alloc.free(snapshot_path);
    const artifact_sha256 = try alloc.dupe(u8, record.artifact_sha256);
    errdefer alloc.free(artifact_sha256);
    const phase = try alloc.dupe(u8, record.phase);
    errdefer alloc.free(phase);
    const last_error = try alloc.dupe(u8, record.last_error);
    errdefer alloc.free(last_error);
    return .{
        .table_id = record.table_id,
        .node_id = record.node_id,
        .group_id = record.group_id,
        .backup_id = backup_id,
        .artifact_backup_id = artifact_backup_id,
        .location = location,
        .snapshot_path = snapshot_path,
        .artifact_sha256 = artifact_sha256,
        .primary_restored = record.primary_restored,
        .runtime_repair_complete = record.runtime_repair_complete,
        .phase = phase,
        .last_error = last_error,
        .updated_at_ms = record.updated_at_ms,
    };
}

pub fn freeRestoreProgress(alloc: std.mem.Allocator, record: RestoreProgressRecord) void {
    alloc.free(record.backup_id);
    alloc.free(record.artifact_backup_id);
    alloc.free(record.location);
    alloc.free(record.snapshot_path);
    alloc.free(record.artifact_sha256);
    alloc.free(record.phase);
    alloc.free(record.last_error);
}

pub fn cloneReplicationSourceStatus(alloc: std.mem.Allocator, record: ReplicationSourceStatusRecord) !ReplicationSourceStatusRecord {
    const source_kind = try alloc.dupe(u8, record.source_kind);
    errdefer alloc.free(source_kind);
    const external_table = try alloc.dupe(u8, record.external_table);
    errdefer alloc.free(external_table);
    const cutover_mode = try alloc.dupe(u8, record.cutover_mode);
    errdefer alloc.free(cutover_mode);
    const slot_name = try alloc.dupe(u8, record.slot_name);
    errdefer alloc.free(slot_name);
    const publication_name = try alloc.dupe(u8, record.publication_name);
    errdefer alloc.free(publication_name);
    const phase = try alloc.dupe(u8, record.phase);
    errdefer alloc.free(phase);
    const checkpoint = try alloc.dupe(u8, record.checkpoint);
    errdefer alloc.free(checkpoint);
    const prepared_checkpoint = try alloc.dupe(u8, record.prepared_checkpoint);
    errdefer alloc.free(prepared_checkpoint);
    const stream_checkpoint = try alloc.dupe(u8, record.stream_checkpoint);
    errdefer alloc.free(stream_checkpoint);
    const last_error = try alloc.dupe(u8, record.last_error);
    errdefer alloc.free(last_error);
    const failure_class = try alloc.dupe(u8, record.failure_class);
    errdefer alloc.free(failure_class);
    const retired_slot_name = try alloc.dupe(u8, record.retired_slot_name);
    errdefer alloc.free(retired_slot_name);
    const retired_publication_name = try alloc.dupe(u8, record.retired_publication_name);
    errdefer alloc.free(retired_publication_name);
    return .{
        .table_id = record.table_id,
        .source_ordinal = record.source_ordinal,
        .source_kind = source_kind,
        .external_table = external_table,
        .cutover_mode = cutover_mode,
        .slot_name = slot_name,
        .publication_name = publication_name,
        .phase = phase,
        .checkpoint = checkpoint,
        .snapshot_offset = record.snapshot_offset,
        .prepared_checkpoint = prepared_checkpoint,
        .stream_checkpoint = stream_checkpoint,
        .last_error = last_error,
        .failure_class = failure_class,
        .lag_records = record.lag_records,
        .lag_millis = record.lag_millis,
        .consecutive_failures = record.consecutive_failures,
        .last_source_commit_at_ms = record.last_source_commit_at_ms,
        .last_success_at_ms = record.last_success_at_ms,
        .last_change_applied_at_ms = record.last_change_applied_at_ms,
        .cutover_intent_id = record.cutover_intent_id,
        .cutover_authority_id = record.cutover_authority_id,
        .cutover_config_fingerprint = record.cutover_config_fingerprint,
        .cutover_provider_identity = record.cutover_provider_identity,
        .retired_cutover_authority_id = record.retired_cutover_authority_id,
        .retired_slot_name = retired_slot_name,
        .retired_publication_name = retired_publication_name,
        .updated_at_ms = record.updated_at_ms,
    };
}

pub fn freeReplicationSourceStatus(alloc: std.mem.Allocator, record: ReplicationSourceStatusRecord) void {
    alloc.free(record.source_kind);
    alloc.free(record.external_table);
    alloc.free(record.cutover_mode);
    alloc.free(record.slot_name);
    alloc.free(record.publication_name);
    alloc.free(record.phase);
    alloc.free(record.checkpoint);
    alloc.free(record.prepared_checkpoint);
    alloc.free(record.stream_checkpoint);
    alloc.free(record.last_error);
    alloc.free(record.failure_class);
    alloc.free(record.retired_slot_name);
    alloc.free(record.retired_publication_name);
}

pub fn cloneShuffleJoinLease(_: std.mem.Allocator, record: ShuffleJoinLeaseRecord) !ShuffleJoinLeaseRecord {
    return record;
}

pub fn freeShuffleJoinLease(_: std.mem.Allocator, _: ShuffleJoinLeaseRecord) void {}

pub fn cloneNode(alloc: std.mem.Allocator, record: NodeRecord) !NodeRecord {
    const role = try alloc.dupe(u8, record.role);
    errdefer alloc.free(role);
    return .{
        .node_id = record.node_id,
        .role = role,
        .lifecycle = try alloc.dupe(u8, record.lifecycle),
    };
}

pub fn freeNode(alloc: std.mem.Allocator, record: NodeRecord) void {
    alloc.free(record.role);
    alloc.free(record.lifecycle);
}

pub fn cloneStore(alloc: std.mem.Allocator, record: StoreRecord) !StoreRecord {
    const api_url = try alloc.dupe(u8, record.api_url);
    errdefer alloc.free(api_url);
    const raft_url = try alloc.dupe(u8, record.raft_url);
    errdefer alloc.free(raft_url);
    const role = try alloc.dupe(u8, record.role);
    errdefer alloc.free(role);
    const health_class = try alloc.dupe(u8, record.health_class);
    errdefer alloc.free(health_class);
    const failure_domain = try alloc.dupe(u8, record.failure_domain);
    errdefer alloc.free(failure_domain);
    const group_statuses = try cloneGroupStatuses(alloc, record.group_statuses);
    errdefer freeGroupStatuses(alloc, group_statuses);
    const runtime_statuses = try cloneRuntimeGroupStatusReports(alloc, record.runtime_statuses);
    errdefer freeRuntimeGroupStatusReports(alloc, runtime_statuses);
    return .{
        .store_id = record.store_id,
        .node_id = record.node_id,
        .api_url = api_url,
        .raft_url = raft_url,
        .role = role,
        .health_class = health_class,
        .failure_domain = failure_domain,
        .live = record.live,
        .drain_requested = record.drain_requested,
        .capacity_bytes = record.capacity_bytes,
        .available_bytes = record.available_bytes,
        .lease_pressure = record.lease_pressure,
        .read_load = record.read_load,
        .write_load = record.write_load,
        .active_backfills = record.active_backfills,
        .backfill_progress_millis = record.backfill_progress_millis,
        .group_statuses = group_statuses,
        .runtime_statuses = runtime_statuses,
    };
}

pub fn freeStore(alloc: std.mem.Allocator, record: StoreRecord) void {
    alloc.free(record.api_url);
    alloc.free(record.raft_url);
    alloc.free(record.role);
    alloc.free(record.health_class);
    alloc.free(record.failure_domain);
    freeGroupStatuses(alloc, record.group_statuses);
    freeRuntimeGroupStatusReports(alloc, record.runtime_statuses);
}

pub fn cloneGroupStatus(alloc: std.mem.Allocator, record: GroupStatusReport) !GroupStatusReport {
    _ = alloc;
    return .{
        .group_id = record.group_id,
        .relocation_generation = record.relocation_generation,
        .raft_applied_index = record.raft_applied_index,
        .raft_term = record.raft_term,
        .raft_membership_index = record.raft_membership_index,
        .doc_count = record.doc_count,
        .disk_bytes = record.disk_bytes,
        .disk_bytes_known = record.disk_bytes_known,
        .empty = record.empty,
        .created_at_millis = record.created_at_millis,
        .updated_at_millis = record.updated_at_millis,
        .local_leader = record.local_leader,
        .local_voter = record.local_voter,
        .voter_count = record.voter_count,
        .voter_set_known = record.voter_set_known,
        .voter_set_fingerprint = record.voter_set_fingerprint,
        .joint_consensus = record.joint_consensus,
        .transition_pending = record.transition_pending,
        .replay_required = record.replay_required,
        .replay_caught_up = record.replay_caught_up,
        .cutover_ready = record.cutover_ready,
        .reads_ready_after_cutover = record.reads_ready_after_cutover,
    };
}

pub fn freeGroupStatus(alloc: std.mem.Allocator, record: GroupStatusReport) void {
    _ = alloc;
    _ = record;
}

pub fn cloneGroupStatuses(alloc: std.mem.Allocator, records: []const GroupStatusReport) ![]GroupStatusReport {
    const out = try alloc.alloc(GroupStatusReport, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| freeGroupStatus(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try cloneGroupStatus(alloc, record);
        initialized += 1;
    }
    return out;
}

pub fn freeGroupStatuses(alloc: std.mem.Allocator, records: []const GroupStatusReport) void {
    for (records) |record| freeGroupStatus(alloc, record);
    if (records.len > 0) alloc.free(records);
}

test "raft voter set fingerprint is canonical and includes required local voter" {
    const canonical = voterSetFingerprint(&.{ 101, 102, 104 }, null);
    const reordered = voterSetFingerprint(&.{ 104, 101, 102 }, null);
    const local_added = voterSetFingerprint(&.{ 101, 102 }, 104);
    const duplicate = voterSetFingerprint(&.{ 101, 102, 104, 102 }, null);
    const different = voterSetFingerprint(&.{ 101, 103, 104 }, null);
    try std.testing.expectEqualSlices(u8, &canonical, &reordered);
    try std.testing.expectEqualSlices(u8, &canonical, &local_added);
    try std.testing.expectEqualSlices(u8, &canonical, &duplicate);
    try std.testing.expect(!std.mem.eql(u8, &canonical, &different));
    try std.testing.expectEqual(@as(usize, 3), normalizedVoterCount(&.{ 101, 102 }, 104));
}

pub fn cloneRuntimeGroupStatusReport(alloc: std.mem.Allocator, record: RuntimeGroupStatusReport) !RuntimeGroupStatusReport {
    const table_name = try alloc.dupe(u8, record.table_name);
    errdefer alloc.free(table_name);
    const source = try alloc.dupe(u8, record.source);
    errdefer alloc.free(source);
    const freshness = try alloc.dupe(u8, record.freshness);
    errdefer alloc.free(freshness);
    const projection_checkpoint_status = try alloc.dupe(u8, record.enrichment.projection_checkpoint_status);
    errdefer alloc.free(projection_checkpoint_status);
    const indexes = try cloneRuntimeIndexStatusReports(alloc, record.indexes);
    errdefer freeRuntimeIndexStatusReports(alloc, indexes);
    var result: RuntimeGroupStatusReport = .{
        .table_id = record.table_id,
        .table_name = table_name,
        .group_id = record.group_id,
        .store_id = record.store_id,
        .node_id = record.node_id,
        .updated_at_ns = record.updated_at_ns,
        .source = source,
        .freshness = freshness,
        .topology_generation = record.topology_generation,
        .lsm_root_generation = record.lsm_root_generation,
        .status_generation = record.status_generation,
        .doc_count = record.doc_count,
        .disk_bytes = record.disk_bytes,
        .disk_bytes_known = record.disk_bytes_known,
        .created_at_millis = record.created_at_millis,
        .index_count = record.index_count,
        .enrichment = record.enrichment,
        .async_indexing_active = record.async_indexing_active,
        .async_startup_active = record.async_startup_active,
        .async_dense_catch_up_active = record.async_dense_catch_up_active,
        .async_bulk_coalescing_active = record.async_bulk_coalescing_active,
        .doc_identity = record.doc_identity,
        .doc_set_planning = record.doc_set_planning,
        .indexes = indexes,
    };
    result.enrichment.projection_checkpoint_status = projection_checkpoint_status;
    return result;
}

pub fn freeRuntimeGroupStatusReport(alloc: std.mem.Allocator, record: RuntimeGroupStatusReport) void {
    alloc.free(record.table_name);
    alloc.free(record.source);
    alloc.free(record.freshness);
    alloc.free(record.enrichment.projection_checkpoint_status);
    freeRuntimeIndexStatusReports(alloc, record.indexes);
}

pub fn cloneRuntimeGroupStatusReports(alloc: std.mem.Allocator, records: []const RuntimeGroupStatusReport) ![]RuntimeGroupStatusReport {
    const out = try alloc.alloc(RuntimeGroupStatusReport, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| freeRuntimeGroupStatusReport(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try cloneRuntimeGroupStatusReport(alloc, record);
        initialized += 1;
    }
    return out;
}

pub fn freeRuntimeGroupStatusReports(alloc: std.mem.Allocator, records: []const RuntimeGroupStatusReport) void {
    for (records) |record| freeRuntimeGroupStatusReport(alloc, record);
    if (records.len > 0) alloc.free(records);
}

pub fn cloneRuntimeIndexStatusReport(alloc: std.mem.Allocator, record: RuntimeIndexStatusReport) !RuntimeIndexStatusReport {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const kind = try alloc.dupe(u8, record.kind);
    errdefer alloc.free(kind);
    const load_error = if (record.load_error) |value| try alloc.dupe(u8, value) else null;
    errdefer if (load_error) |value| alloc.free(value);
    return .{
        .name = name,
        .kind = kind,
        .load_error = load_error,
        .doc_count = record.doc_count,
        .term_count = record.term_count,
        .edge_count = record.edge_count,
        .node_count = record.node_count,
        .root_node = record.root_node,
        .coverage_produced_count = record.coverage_produced_count,
        .coverage_skipped_count = record.coverage_skipped_count,
        .coverage_terminal_failed_count = record.coverage_terminal_failed_count,
        .coverage_generation = record.coverage_generation,
        .coverage_config_hash = record.coverage_config_hash,
        .coverage_identity_ready = record.coverage_identity_ready,
        .coverage_summary_ready = record.coverage_summary_ready,
        .backfill_active = record.backfill_active,
        .backfill_progress_millis = record.backfill_progress_millis,
        .replay_applied_sequence = record.replay_applied_sequence,
        .replay_target_sequence = record.replay_target_sequence,
        .replay_catch_up_required = record.replay_catch_up_required,
    };
}

pub fn freeRuntimeIndexStatusReport(alloc: std.mem.Allocator, record: RuntimeIndexStatusReport) void {
    alloc.free(record.name);
    alloc.free(record.kind);
    if (record.load_error) |value| alloc.free(value);
}

pub fn cloneRuntimeIndexStatusReports(alloc: std.mem.Allocator, records: []const RuntimeIndexStatusReport) ![]RuntimeIndexStatusReport {
    const out = try alloc.alloc(RuntimeIndexStatusReport, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| freeRuntimeIndexStatusReport(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try cloneRuntimeIndexStatusReport(alloc, record);
        initialized += 1;
    }
    return out;
}

pub fn freeRuntimeIndexStatusReports(alloc: std.mem.Allocator, records: []const RuntimeIndexStatusReport) void {
    for (records) |record| freeRuntimeIndexStatusReport(alloc, record);
    if (records.len > 0) alloc.free(records);
}

fn cloneSplitIntent(alloc: std.mem.Allocator, intent: SplitIntent) !SplitIntent {
    const split_key = try alloc.dupe(u8, intent.split_key);
    errdefer alloc.free(split_key);
    const source_range_end = try cloneOwnedOptional(
        alloc,
        intent.projected_source_range_end,
    );
    errdefer freeOwnedOptional(alloc, source_range_end);
    const rollback_reason = try cloneOwnedOptional(alloc, intent.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    const projected_contract = if (intent.projected_contract) |contract|
        try contract.clone(alloc)
    else
        null;
    return .{
        .transition_id = intent.transition_id,
        .attempt_epoch = intent.attempt_epoch,
        .table_id = intent.table_id,
        .source_group_id = intent.source_group_id,
        .destination_group_id = intent.destination_group_id,
        .split_key = split_key,
        .projected_source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
        .automatic = intent.automatic,
        .projected = intent.projected,
        .projected_contract = projected_contract,
    };
}

fn freeSplitIntent(alloc: std.mem.Allocator, intent: SplitIntent) void {
    alloc.free(intent.split_key);
    freeOwnedOptional(alloc, intent.projected_source_range_end);
    freeOwnedOptional(alloc, intent.rollback_reason);
    if (intent.projected_contract) |contract_value| {
        var contract = contract_value;
        contract.deinitOwned(alloc);
    }
}

fn cloneMergeIntent(alloc: std.mem.Allocator, intent: MergeIntent) !MergeIntent {
    const rollback_reason = try cloneOwnedOptional(alloc, intent.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    const projected_contract = if (intent.projected_contract) |contract|
        try contract.clone(alloc)
    else
        null;
    return .{
        .transition_id = intent.transition_id,
        .table_id = intent.table_id,
        .donor_group_id = intent.donor_group_id,
        .receiver_group_id = intent.receiver_group_id,
        .rollback_reason = rollback_reason,
        .automatic = intent.automatic,
        .allow_doc_identity_reassignment = intent.allow_doc_identity_reassignment,
        .projected = intent.projected,
        .projected_contract = projected_contract,
    };
}

fn freeMergeIntent(alloc: std.mem.Allocator, intent: MergeIntent) void {
    freeOwnedOptional(alloc, intent.rollback_reason);
    if (intent.projected_contract) |contract_value| {
        var contract = contract_value;
        contract.deinitOwned(alloc);
    }
}

pub fn cloneSplitTransitionRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) !transition_state.SplitTransitionRecord {
    const split_key = try cloneOwnedOptional(alloc, record.split_key);
    errdefer freeOwnedOptional(alloc, split_key);
    const source_range_end = try cloneOwnedOptional(alloc, record.source_range_end);
    errdefer freeOwnedOptional(alloc, source_range_end);
    const rollback_reason = try cloneOwnedOptional(alloc, record.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    const table_contract = try record.table_contract.clone(alloc);
    errdefer {
        var owned_contract = table_contract;
        owned_contract.deinitOwned(alloc);
    }
    return .{
        .transition_id = record.transition_id,
        .attempt_epoch = record.attempt_epoch,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
        .table_contract = table_contract,
    };
}

pub fn freeSplitTransitionRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) void {
    freeOwnedOptional(alloc, record.split_key);
    freeOwnedOptional(alloc, record.source_range_end);
    freeOwnedOptional(alloc, record.rollback_reason);
    var table_contract = record.table_contract;
    table_contract.deinitOwned(alloc);
}

pub fn cloneMergeTransitionRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) !transition_state.MergeTransitionRecord {
    const rollback_reason = try cloneOwnedOptional(alloc, record.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    const table_contract = try record.table_contract.clone(alloc);
    errdefer {
        var owned_contract = table_contract;
        owned_contract.deinitOwned(alloc);
    }
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
        .table_contract = table_contract,
    };
}

pub fn freeMergeTransitionRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) void {
    freeOwnedOptional(alloc, record.rollback_reason);
    var table_contract = record.table_contract;
    table_contract.deinitOwned(alloc);
}

test "table manager validates split and merge intents" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    try manager.requestSplit(.{
        .transition_id = 5001,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    });

    const splits = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 1), splits[0].attempt_epoch);
    try std.testing.expectEqualStrings("doc:h", splits[0].split_key.?);
    try std.testing.expectEqualStrings("doc:m", splits[0].source_range_end.?);
    try splits[0].table_contract.validateForSplit();
    try std.testing.expectEqual(@as(u64, 10), splits[0].table_contract.table_id);
    try std.testing.expectEqualStrings("docs", splits[0].table_contract.table_name);
    try std.testing.expectEqualStrings("{}", splits[0].table_contract.indexes_json);
    try std.testing.expectEqual(@as(u64, 101), splits[0].table_contract.source_identity.shard_id);
    try std.testing.expectEqual(@as(u64, 101), splits[0].table_contract.source_identity.range_id);
    try std.testing.expect(splits[0].table_contract.source_identity.eql(
        splits[0].table_contract.target_identity,
    ));
    try std.testing.expectError(error.ConflictingSplitTransition, manager.requestSplit(.{
        .transition_id = 5001,
        .attempt_epoch = 2,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    }));

    manager.applyRolledBackSplit(5001);
    try manager.requestSplit(.{
        .transition_id = 5001,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    });
    const retried_splits = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, retried_splits);
    try std.testing.expectEqual(@as(usize, 1), retried_splits.len);
    try std.testing.expectEqual(@as(u64, 2), retried_splits[0].attempt_epoch);

    try manager.requestMerge(.{
        .transition_id = 6001,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
    });

    const merges = try manager.listDesiredMergeTransitions(std.testing.allocator);
    defer manager.freeMergeTransitions(std.testing.allocator, merges);
    try std.testing.expectEqual(@as(usize, 1), merges.len);
    try std.testing.expectEqual(@as(u64, 102), merges[0].donor_group_id);
    try std.testing.expectEqual(@as(u64, 101), merges[0].receiver_group_id);
    try std.testing.expect(merges[0].allow_doc_identity_reassignment);
    try merges[0].table_contract.validateForMerge(
        merges[0].allow_doc_identity_reassignment,
    );
    try std.testing.expectEqual(@as(u64, 10), merges[0].table_contract.table_id);
    try std.testing.expectEqualStrings("docs", merges[0].table_contract.table_name);
    try std.testing.expectEqual(@as(u64, 102), merges[0].table_contract.source_identity.shard_id);
    try std.testing.expectEqual(@as(u64, 102), merges[0].table_contract.source_identity.range_id);
    try std.testing.expectEqual(@as(u64, 101), merges[0].table_contract.target_identity.shard_id);
    try std.testing.expectEqual(@as(u64, 101), merges[0].table_contract.target_identity.range_id);
}

test "table manager rehydrates projected transitions without consuming split epochs" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .split_attempt_epoch = 2,
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const split_contract: transition_state.TransitionTableContract = .{
        .table_id = 10,
        .table_name = "docs",
        .indexes_json = "{}",
        .source_identity = .{ .shard_id = 101, .range_id = 101 },
        .target_identity = .{ .shard_id = 101, .range_id = 101 },
    };
    const merge_contract: transition_state.TransitionTableContract = .{
        .table_id = 10,
        .table_name = "docs",
        .indexes_json = "{}",
        .source_identity = .{ .shard_id = 102, .range_id = 102 },
        .target_identity = .{ .shard_id = 101, .range_id = 101 },
    };
    const projected_splits = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 5001,
        .attempt_epoch = 2,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
        .table_contract = split_contract,
    }};
    const projected_merges = [_]transition_state.MergeTransitionRecord{.{
        .transition_id = 6001,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
        .table_contract = merge_contract,
    }};
    try manager.syncProjectedSplitTransitions(&projected_splits);
    try manager.syncProjectedMergeTransitions(&projected_merges);

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
        .schema_json = "{\"version\":2}",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:n",
        .split_attempt_epoch = 2,
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:n",
        .end_key = "doc:z",
    });
    try std.testing.expectEqual(
        @as(usize, 2),
        manager.removeTableTopology(10),
    );

    const splits = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 2), splits[0].attempt_epoch);
    try std.testing.expectEqualStrings("doc:h", splits[0].split_key.?);
    try std.testing.expectEqualStrings("doc:m", splits[0].source_range_end.?);
    try std.testing.expect(splits[0].table_contract.eql(split_contract));

    const merges = try manager.listDesiredMergeTransitions(std.testing.allocator);
    defer manager.freeMergeTransitions(std.testing.allocator, merges);
    try std.testing.expectEqual(@as(usize, 1), merges.len);
    try std.testing.expect(merges[0].allow_doc_identity_reassignment);
    try std.testing.expect(merges[0].table_contract.eql(merge_contract));

    const rolled_back_splits = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 5001,
        .attempt_epoch = 2,
        .source_group_id = 101,
        .destination_group_id = 103,
        .phase = .rolled_back,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
    }};
    const finalized_merges = [_]transition_state.MergeTransitionRecord{.{
        .transition_id = 6001,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .phase = .finalized,
    }};
    try manager.syncProjectedSplitTransitions(&rolled_back_splits);
    try manager.syncProjectedMergeTransitions(&finalized_merges);
    try std.testing.expectEqual(@as(usize, 0), manager.split_intents.count());
    try std.testing.expectEqual(@as(usize, 0), manager.merge_intents.count());

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
        .schema_json = "{\"version\":2}",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:n",
        .split_attempt_epoch = 2,
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:n",
        .end_key = "doc:z",
    });
    try manager.requestSplit(.{
        .transition_id = 5002,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    });
    try manager.syncProjectedSplitTransitions(&.{});
    try std.testing.expectEqual(@as(usize, 1), manager.split_intents.count());
    const local_splits = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, local_splits);
    try std.testing.expectEqual(@as(u64, 3), local_splits[0].attempt_epoch);
}

test "table manager applies finalized split to desired topology" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();
    const completion_fingerprint = restoreCompletionFingerprint(
        "nightly",
        "nightly-artifacts",
        "s3://backups",
    );

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
        .completed_restore_fingerprint = completion_fingerprint,
    });
    try manager.requestSplit(.{
        .transition_id = 5003,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 102,
        .split_key = "doc:m",
    });

    try manager.applyFinalizedSplit(.{
        .transition_id = 5003,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 102,
        .phase = .finalized,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
        .table_contract = .{
            .table_id = 10,
            .table_name = "docs",
            .indexes_json = "{}",
            .source_identity = .{ .shard_id = 101, .range_id = 101 },
            .target_identity = .{ .shard_id = 101, .range_id = 101 },
        },
    });
    try manager.applyFinalizedSplit(.{
        .transition_id = 5003,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 102,
        .phase = .finalized,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
        .table_contract = .{
            .table_id = 10,
            .table_name = "docs",
            .indexes_json = "{}",
            .source_identity = .{ .shard_id = 101, .range_id = 101 },
            .target_identity = .{ .shard_id = 101, .range_id = 101 },
        },
    });

    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
    for (ranges) |range| {
        try std.testing.expectEqualSlices(
            u8,
            &completion_fingerprint,
            &range.completed_restore_fingerprint,
        );
        if (range.group_id == 101) try std.testing.expectEqual(@as(u64, 101), range.range_id);
        if (range.group_id == 102) {
            try std.testing.expectEqual(@as(u64, 102), range.range_id);
            try std.testing.expectEqual(@as(u64, 101), range.doc_identity_shard_id);
            try std.testing.expectEqual(@as(u64, 101), range.doc_identity_range_id);
        }
    }
    try std.testing.expect(manager.split_intents.count() == 0);
}

test "table manager applies finalized merge preserving receiver range id" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();
    const completion_fingerprint = restoreCompletionFingerprint(
        "nightly",
        "nightly-artifacts",
        "s3://backups",
    );

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .range_id = 1001,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .completed_restore_fingerprint = completion_fingerprint,
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .range_id = 1002,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
        .completed_restore_fingerprint = completion_fingerprint,
    });
    try manager.requestMerge(.{
        .transition_id = 6003,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
    });

    try manager.applyFinalizedMerge(.{
        .transition_id = 6003,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .phase = .finalized,
        .allow_doc_identity_reassignment = true,
        .table_contract = .{
            .table_id = 10,
            .table_name = "docs",
            .indexes_json = "{}",
            .source_identity = .{ .shard_id = 102, .range_id = 1002 },
            .target_identity = .{ .shard_id = 101, .range_id = 1001 },
        },
    });
    try manager.applyFinalizedMerge(.{
        .transition_id = 6003,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .phase = .finalized,
        .allow_doc_identity_reassignment = true,
        .table_contract = .{
            .table_id = 10,
            .table_name = "docs",
            .indexes_json = "{}",
            .source_identity = .{ .shard_id = 102, .range_id = 1002 },
            .target_identity = .{ .shard_id = 101, .range_id = 1001 },
        },
    });

    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 101), ranges[0].group_id);
    try std.testing.expectEqual(@as(u64, 1001), ranges[0].range_id);
    try std.testing.expectEqual(@as(u64, 0), ranges[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 0), ranges[0].doc_identity_range_id);
    try std.testing.expectEqualStrings("doc:a", ranges[0].start_key);
    try std.testing.expectEqualStrings("doc:z", ranges[0].end_key.?);
    try std.testing.expectEqualSlices(
        u8,
        &completion_fingerprint,
        &ranges[0].completed_restore_fingerprint,
    );
    try std.testing.expect(manager.merge_intents.count() == 0);
}

test "table manager replays terminal split and merge topology idempotently" {
    var split_manager = TableManager.init(std.testing.allocator);
    defer split_manager.deinit();
    try split_manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    // Model a crash after publishing the narrowed source but before publishing
    // the destination range.
    try split_manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .split_attempt_epoch = 1,
    });
    const terminal_split: transition_state.SplitTransitionRecord = .{
        .transition_id = 7001,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 102,
        .phase = .finalized,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
        .table_contract = .{
            .table_id = 10,
            .table_name = "docs",
            .indexes_json = "{}",
            .source_identity = .{ .shard_id = 101, .range_id = 101 },
            .target_identity = .{ .shard_id = 101, .range_id = 101 },
        },
    };
    try split_manager.applyProjectedTerminalTransitions(&.{terminal_split}, &.{});
    try split_manager.applyProjectedTerminalTransitions(&.{terminal_split}, &.{});
    const split_ranges = try split_manager.listRanges(std.testing.allocator);
    defer split_manager.freeRanges(std.testing.allocator, split_ranges);
    try std.testing.expectEqual(@as(usize, 2), split_ranges.len);

    var merge_manager = TableManager.init(std.testing.allocator);
    defer merge_manager.deinit();
    try merge_manager.upsertTable(.{ .table_id = 20, .name = "events" });
    try merge_manager.upsertRange(.{
        .group_id = 201,
        .table_id = 20,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try merge_manager.upsertRange(.{
        .group_id = 202,
        .table_id = 20,
        .start_key = "doc:m",
        .end_key = "doc:z",
        .doc_identity_shard_id = 201,
        .doc_identity_range_id = 201,
    });
    const terminal_merge: transition_state.MergeTransitionRecord = .{
        .transition_id = 7002,
        .donor_group_id = 202,
        .receiver_group_id = 201,
        .phase = .finalized,
        .table_contract = .{
            .table_id = 20,
            .table_name = "events",
            .indexes_json = "{}",
            .source_identity = .{ .shard_id = 201, .range_id = 201 },
            .target_identity = .{ .shard_id = 201, .range_id = 201 },
        },
    };
    try merge_manager.applyProjectedTerminalTransitions(&.{}, &.{terminal_merge});
    try merge_manager.applyProjectedTerminalTransitions(&.{}, &.{terminal_merge});
    const merge_ranges = try merge_manager.listRanges(std.testing.allocator);
    defer merge_manager.freeRanges(std.testing.allocator, merge_ranges);
    try std.testing.expectEqual(@as(usize, 1), merge_ranges.len);
    try std.testing.expectEqualStrings("doc:a", merge_ranges[0].start_key);
    try std.testing.expectEqualStrings("doc:z", merge_ranges[0].end_key.?);
}

test "table manager rejects invalid split key" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });

    try std.testing.expectError(error.InvalidSplitKey, manager.requestSplit(.{
        .transition_id = 5002,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 102,
        .split_key = "doc:m",
    }));
}

test "table manager can replace topology from projected state" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 1, .name = "old" });
    try manager.upsertRange(.{
        .group_id = 11,
        .table_id = 1,
        .start_key = "a",
        .end_key = "m",
    });

    const tables = [_]TableRecord{
        .{ .table_id = 2, .name = "new" },
    };
    const ranges = [_]RangeRecord{
        .{ .group_id = 21, .table_id = 2, .start_key = "doc:a", .end_key = "doc:z" },
    };
    try manager.replaceTopology(&tables, &ranges);

    const listed_tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, listed_tables);
    const listed_ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, listed_ranges);

    try std.testing.expectEqual(@as(usize, 1), listed_tables.len);
    try std.testing.expectEqual(@as(u64, 2), listed_tables[0].table_id);
    try std.testing.expectEqualStrings("new", listed_tables[0].name);
    try std.testing.expectEqual(@as(usize, 1), listed_ranges.len);
    try std.testing.expectEqual(@as(u64, 21), listed_ranges[0].group_id);
}

test "table manager owns foreign key reference owner ranges" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    try manager.upsertTable(.{ .table_id = 8, .name = "customers" });
    try manager.upsertTable(.{ .table_id = 9, .name = "accounts" });

    try manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 8,
        .start_parent_key = "",
        .end_parent_key = "customer:m",
        .group_id = 9001,
        .topology_epoch = 11,
    });
    try manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 8,
        .start_parent_key = "customer:m",
        .end_parent_key = null,
        .group_id = 9002,
        .topology_epoch = 12,
    });
    try manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_account_id_fkey",
        .parent_table_id = 9,
        .start_parent_key = "",
        .end_parent_key = null,
        .group_id = 9003,
        .state = foreign_key_ref_range_rebuilding,
    });
    try std.testing.expectError(error.InvalidForeignKeyReferenceRangeState, manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_account_id_fkey",
        .parent_table_id = 9,
        .start_parent_key = "account:z",
        .end_parent_key = null,
        .group_id = 9004,
        .state = "unknown",
    }));
    try std.testing.expectError(error.ForeignKeyReferenceRangeOverlap, manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 8,
        .start_parent_key = "customer:h",
        .end_parent_key = "customer:t",
        .group_id = 9005,
    }));
    try manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 8,
        .start_parent_key = "customer:m",
        .end_parent_key = null,
        .group_id = 9012,
        .topology_epoch = 22,
    });
    try std.testing.expectError(error.ForeignKeyReferenceRangeGroupCollision, manager.upsertRange(.{
        .group_id = 9012,
        .table_id = 7,
        .start_key = "order:a",
        .end_key = "order:z",
    }));
    try manager.upsertRange(.{
        .group_id = 9100,
        .table_id = 7,
        .start_key = "order:a",
        .end_key = "order:z",
    });
    try std.testing.expectError(error.ForeignKeyReferenceRangeGroupCollision, manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 8,
        .start_parent_key = "customer:z",
        .end_parent_key = null,
        .group_id = 9100,
    }));

    const listed = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
    defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 3), listed.len);

    var saw_replaced = false;
    for (listed) |record| {
        if (!std.mem.eql(u8, record.constraint_name, "orders_customer_id_fkey")) continue;
        if (!std.mem.eql(u8, record.start_parent_key, "customer:m")) continue;
        try std.testing.expectEqual(@as(u64, 9012), record.group_id);
        try std.testing.expectEqual(@as(u64, 22), record.topology_epoch);
        saw_replaced = true;
    }
    try std.testing.expect(saw_replaced);

    try std.testing.expectEqual(@as(usize, 2), manager.removeForeignKeyReferenceRangesForTable(8));
    const remaining = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
    defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, remaining);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualStrings("orders_account_id_fkey", remaining[0].constraint_name);
    try std.testing.expectEqualStrings(foreign_key_ref_range_rebuilding, remaining[0].state);
}

test "table manager applies foreign key reference range lifecycle operations" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    try manager.upsertTable(.{ .table_id = 8, .name = "customers" });
    const selector: ForeignKeyReferenceRangeSelector = .{
        .child_table_id = 7,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 8,
        .start_parent_key = "",
    };
    try manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 7,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 8,
        .start_parent_key = "",
        .end_parent_key = null,
        .group_id = 9001,
    });

    try manager.beginForeignKeyReferenceRangeRebuild(selector);
    {
        const ranges = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
        defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings(foreign_key_ref_range_rebuilding, ranges[0].state);
        try std.testing.expect(!foreignKeyReferenceRangeRoutable(ranges[0]));
    }

    try manager.finishForeignKeyReferenceRangeRebuild(selector);
    {
        const ranges = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
        defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, ranges);
        try std.testing.expectEqualStrings(foreign_key_ref_range_active, ranges[0].state);
        try std.testing.expect(foreignKeyReferenceRangeRoutable(ranges[0]));
    }

    const split_request: ForeignKeyReferenceRangeSplitRequest = .{
        .selector = selector,
        .split_parent_key = "customer:m",
        .left_group_id = 9001,
        .right_group_id = 9002,
    };
    try manager.beginForeignKeyReferenceRangeSplit(split_request);
    {
        const ranges = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
        defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings(foreign_key_ref_range_splitting, ranges[0].state);
        try std.testing.expect(!foreignKeyReferenceRangeRoutable(ranges[0]));
    }

    try manager.finishForeignKeyReferenceRangeSplit(split_request);
    {
        const ranges = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
        defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 2), ranges.len);
        var saw_left = false;
        var saw_right = false;
        for (ranges) |record| {
            try std.testing.expectEqualStrings(foreign_key_ref_range_active, record.state);
            if (std.mem.eql(u8, record.start_parent_key, "")) {
                try std.testing.expectEqualStrings("customer:m", record.end_parent_key.?);
                try std.testing.expectEqual(@as(u64, 9001), record.group_id);
                try std.testing.expectEqual(@as(u64, 9001), record.range_id);
                saw_left = true;
            } else if (std.mem.eql(u8, record.start_parent_key, "customer:m")) {
                try std.testing.expect(record.end_parent_key == null);
                try std.testing.expectEqual(@as(u64, 9002), record.group_id);
                try std.testing.expectEqual(@as(u64, 9002), record.range_id);
                saw_right = true;
            }
        }
        try std.testing.expect(saw_left and saw_right);
    }

    const merge_request: ForeignKeyReferenceRangeMergeRequest = .{
        .left_selector = selector,
        .right_start_parent_key = "customer:m",
        .merged_group_id = 9001,
    };
    try manager.beginForeignKeyReferenceRangeMerge(merge_request);
    {
        const ranges = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
        defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, ranges);
        for (ranges) |record| {
            try std.testing.expectEqualStrings(foreign_key_ref_range_merging, record.state);
            try std.testing.expect(!foreignKeyReferenceRangeRoutable(record));
        }
    }

    try manager.finishForeignKeyReferenceRangeMerge(merge_request);
    {
        const ranges = try manager.listForeignKeyReferenceRanges(std.testing.allocator);
        defer manager.freeForeignKeyReferenceRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings("", ranges[0].start_parent_key);
        try std.testing.expect(ranges[0].end_parent_key == null);
        try std.testing.expectEqual(@as(u64, 9001), ranges[0].group_id);
        try std.testing.expectEqual(@as(u64, 9001), ranges[0].range_id);
        try std.testing.expectEqualStrings(foreign_key_ref_range_active, ranges[0].state);
    }
}

test "table manager owns unique constraint owner ranges" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "users" });

    try manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_email_key",
        .start_encoded_value = "",
        .end_encoded_value = "email:m",
        .group_id = 7101,
        .topology_epoch = 21,
    });
    try manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_email_key",
        .start_encoded_value = "email:m",
        .end_encoded_value = null,
        .group_id = 7102,
        .topology_epoch = 22,
    });
    try manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_username_key",
        .start_encoded_value = "",
        .end_encoded_value = null,
        .group_id = 7201,
        .state = unique_constraint_range_rebuilding,
    });
    try std.testing.expectError(error.InvalidUniqueConstraintRangeState, manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_username_key",
        .start_encoded_value = "username:z",
        .end_encoded_value = null,
        .group_id = 7202,
        .state = "unknown",
    }));
    try std.testing.expectError(error.UniqueConstraintRangeOverlap, manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_email_key",
        .start_encoded_value = "email:h",
        .end_encoded_value = "email:t",
        .group_id = 7105,
    }));
    try manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_email_key",
        .start_encoded_value = "email:m",
        .end_encoded_value = null,
        .group_id = 7112,
        .topology_epoch = 32,
    });
    try std.testing.expectError(error.UniqueConstraintRangeGroupCollision, manager.upsertRange(.{
        .group_id = 7112,
        .table_id = 7,
        .start_key = "user:a",
        .end_key = "user:z",
    }));
    try manager.upsertRange(.{
        .group_id = 7300,
        .table_id = 7,
        .start_key = "user:a",
        .end_key = "user:z",
    });
    try std.testing.expectError(error.UniqueConstraintRangeGroupCollision, manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_email_key",
        .start_encoded_value = "email:z",
        .end_encoded_value = null,
        .group_id = 7300,
    }));

    const listed = try manager.listUniqueConstraintRanges(std.testing.allocator);
    defer manager.freeUniqueConstraintRanges(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 3), listed.len);

    var saw_replaced = false;
    for (listed) |record| {
        if (!std.mem.eql(u8, record.constraint_name, "users_email_key")) continue;
        if (!std.mem.eql(u8, record.start_encoded_value, "email:m")) continue;
        try std.testing.expectEqual(@as(u64, 7112), record.group_id);
        try std.testing.expectEqual(@as(u64, 32), record.topology_epoch);
        saw_replaced = true;
    }
    try std.testing.expect(saw_replaced);

    try std.testing.expectEqual(@as(usize, 3), manager.removeUniqueConstraintRangesForTable(7));
    const remaining = try manager.listUniqueConstraintRanges(std.testing.allocator);
    defer manager.freeUniqueConstraintRanges(std.testing.allocator, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "table manager applies unique constraint range lifecycle operations" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "users" });
    const selector: UniqueConstraintRangeSelector = .{
        .table_id = 7,
        .constraint_name = "users_email_key",
        .start_encoded_value = "",
    };
    try manager.upsertUniqueConstraintRange(.{
        .table_id = 7,
        .constraint_name = "users_email_key",
        .start_encoded_value = "",
        .end_encoded_value = null,
        .group_id = 7101,
    });

    try manager.beginUniqueConstraintRangeRebuild(selector);
    {
        const ranges = try manager.listUniqueConstraintRanges(std.testing.allocator);
        defer manager.freeUniqueConstraintRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings(unique_constraint_range_rebuilding, ranges[0].state);
        try std.testing.expect(!uniqueConstraintRangeRoutable(ranges[0]));
    }

    try manager.finishUniqueConstraintRangeRebuild(selector);
    {
        const ranges = try manager.listUniqueConstraintRanges(std.testing.allocator);
        defer manager.freeUniqueConstraintRanges(std.testing.allocator, ranges);
        try std.testing.expectEqualStrings(unique_constraint_range_active, ranges[0].state);
        try std.testing.expect(uniqueConstraintRangeRoutable(ranges[0]));
    }

    const split_request: UniqueConstraintRangeSplitRequest = .{
        .selector = selector,
        .split_encoded_value = "email:m",
        .left_group_id = 7101,
        .right_group_id = 7102,
    };
    try manager.beginUniqueConstraintRangeSplit(split_request);
    {
        const ranges = try manager.listUniqueConstraintRanges(std.testing.allocator);
        defer manager.freeUniqueConstraintRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings(unique_constraint_range_splitting, ranges[0].state);
        try std.testing.expect(!uniqueConstraintRangeRoutable(ranges[0]));
    }

    try manager.finishUniqueConstraintRangeSplit(split_request);
    {
        const ranges = try manager.listUniqueConstraintRanges(std.testing.allocator);
        defer manager.freeUniqueConstraintRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 2), ranges.len);
        var saw_left = false;
        var saw_right = false;
        for (ranges) |record| {
            try std.testing.expectEqualStrings(unique_constraint_range_active, record.state);
            if (std.mem.eql(u8, record.start_encoded_value, "")) {
                try std.testing.expectEqualStrings("email:m", record.end_encoded_value.?);
                try std.testing.expectEqual(@as(u64, 7101), record.group_id);
                try std.testing.expectEqual(@as(u64, 7101), record.range_id);
                saw_left = true;
            } else if (std.mem.eql(u8, record.start_encoded_value, "email:m")) {
                try std.testing.expect(record.end_encoded_value == null);
                try std.testing.expectEqual(@as(u64, 7102), record.group_id);
                try std.testing.expectEqual(@as(u64, 7102), record.range_id);
                saw_right = true;
            }
        }
        try std.testing.expect(saw_left and saw_right);
    }

    const merge_request: UniqueConstraintRangeMergeRequest = .{
        .left_selector = selector,
        .right_start_encoded_value = "email:m",
        .merged_group_id = 7101,
    };
    try manager.beginUniqueConstraintRangeMerge(merge_request);
    {
        const ranges = try manager.listUniqueConstraintRanges(std.testing.allocator);
        defer manager.freeUniqueConstraintRanges(std.testing.allocator, ranges);
        for (ranges) |record| {
            try std.testing.expectEqualStrings(unique_constraint_range_merging, record.state);
            try std.testing.expect(!uniqueConstraintRangeRoutable(record));
        }
    }

    try manager.finishUniqueConstraintRangeMerge(merge_request);
    {
        const ranges = try manager.listUniqueConstraintRanges(std.testing.allocator);
        defer manager.freeUniqueConstraintRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings("", ranges[0].start_encoded_value);
        try std.testing.expect(ranges[0].end_encoded_value == null);
        try std.testing.expectEqual(@as(u64, 7101), ranges[0].group_id);
        try std.testing.expectEqual(@as(u64, 7101), ranges[0].range_id);
        try std.testing.expectEqualStrings(unique_constraint_range_active, ranges[0].state);
    }
}

test "table manager owns secondary index rebuild work ranges" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });

    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_status_idx",
        .index_generation = 42,
        .start_row_key = "",
        .end_row_key = "order:m",
        .group_id = 8101,
        .topology_epoch = 11,
    });
    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_status_idx",
        .index_generation = 42,
        .start_row_key = "order:m",
        .end_row_key = null,
        .group_id = 8102,
        .topology_epoch = 12,
    });
    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_email_idx",
        .index_generation = 99,
        .start_row_key = "",
        .end_row_key = null,
        .group_id = 8103,
        .state = secondary_index_rebuild_building,
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 1234,
    });
    try std.testing.expectError(error.InvalidSecondaryIndexRebuildRangeState, manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_email_idx",
        .index_generation = 99,
        .start_row_key = "order:z",
        .end_row_key = null,
        .group_id = 8104,
        .state = "unknown",
    }));
    try std.testing.expectError(error.InvalidSecondaryIndexGeneration, manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_bad_idx",
        .index_generation = 0,
        .start_row_key = "",
        .end_row_key = null,
        .group_id = 8104,
    }));
    try std.testing.expectError(error.SecondaryIndexRebuildRangeOverlap, manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_status_idx",
        .index_generation = 42,
        .start_row_key = "order:h",
        .end_row_key = "order:t",
        .group_id = 8105,
    }));
    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_status_idx",
        .index_generation = 42,
        .start_row_key = "order:m",
        .end_row_key = null,
        .group_id = 8112,
        .topology_epoch = 22,
    });
    try std.testing.expectError(error.SecondaryIndexRebuildRangeGroupCollision, manager.upsertRange(.{
        .group_id = 8112,
        .table_id = 7,
        .start_key = "order:a",
        .end_key = "order:z",
    }));

    const listed = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
    defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 3), listed.len);

    var saw_replaced = false;
    for (listed) |record| {
        if (!std.mem.eql(u8, record.index_name, "orders_status_idx")) continue;
        if (!std.mem.eql(u8, record.start_row_key, "order:m")) continue;
        try std.testing.expectEqual(@as(u64, 8112), record.group_id);
        try std.testing.expectEqual(@as(u64, 22), record.topology_epoch);
        saw_replaced = true;
    }
    try std.testing.expect(saw_replaced);

    try std.testing.expectEqual(@as(usize, 3), manager.removeSecondaryIndexRebuildRangesForTable(7));
    const remaining = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
    defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "table manager owns schema rewrite jobs" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });

    const status_expr: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{
            .{ .kind = .field, .field = "status" },
        },
    };

    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = "m",
        .target_column = "status_norm",
        .expression = status_expr,
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 1234,
    });
    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9102,
        .table_id = 7,
        .group_id = 9002,
        .schema_generation = 42,
        .action = "validate",
        .reason = "constraints",
        .start_row_key = "m",
        .end_row_key = null,
        .state = schema_rewrite_ready,
        .completed_row_count = 12,
        .progress_row_key = "order:z",
    });
    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9104,
        .table_id = 7,
        .group_id = 9003,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "z",
        .rewrite_renames = &.{.{ .old_path = "status", .new_path = "state" }},
        .rewrite_drops = &.{"legacy_status"},
    });
    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9105,
        .table_id = 7,
        .group_id = 9004,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "zz",
        .full_row_rewrite = true,
    });
    try std.testing.expectError(error.UnknownTable, manager.upsertSchemaRewriteJob(.{
        .job_id = 9199,
        .table_id = 99,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
    }));
    try std.testing.expectError(error.InvalidSchemaRewriteJob, manager.upsertSchemaRewriteJob(.{
        .job_id = 0,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
    }));
    try std.testing.expectError(error.InvalidSchemaRewriteGeneration, manager.upsertSchemaRewriteJob(.{
        .job_id = 9103,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 0,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
    }));
    try std.testing.expectError(error.InvalidSchemaRewriteJobState, manager.upsertSchemaRewriteJob(.{
        .job_id = 9103,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .state = "unknown",
    }));
    try std.testing.expectError(error.InvalidSchemaRewriteExpression, manager.upsertSchemaRewriteJob(.{
        .job_id = 9103,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .expression = status_expr,
    }));
    try std.testing.expectError(error.InvalidSchemaRewriteExpression, manager.upsertSchemaRewriteJob(.{
        .job_id = 9103,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
    }));
    try std.testing.expectError(error.InvalidSchemaRewriteExpression, manager.upsertSchemaRewriteJob(.{
        .job_id = 9103,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .full_row_rewrite = true,
        .rewrite_drops = &.{"legacy_status"},
    }));

    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 43,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = "m",
        .state = schema_rewrite_invalid,
        .target_column = "status_norm",
        .expression = status_expr,
        .last_error = "schema generation moved",
    });

    const listed = try manager.listSchemaRewriteJobs(std.testing.allocator);
    defer manager.freeSchemaRewriteJobs(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 4), listed.len);

    var saw_replaced = false;
    var saw_ready = false;
    var saw_row_plan = false;
    var saw_full_row_rewrite = false;
    for (listed) |record| {
        if (record.job_id == 9101) {
            try std.testing.expectEqual(@as(u64, 43), record.schema_generation);
            try std.testing.expectEqualStrings(schema_rewrite_invalid, record.state);
            try std.testing.expectEqualStrings("schema generation moved", record.last_error);
            try std.testing.expectEqualStrings("status_norm", record.target_column);
            try std.testing.expect(record.expression != null);
            try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.lower, record.expression.?.kind);
            try std.testing.expectEqual(@as(usize, 1), record.expression.?.operands.len);
            try std.testing.expectEqualStrings("status", record.expression.?.operands[0].field);
            saw_replaced = true;
        } else if (record.job_id == 9102) {
            try std.testing.expect(schemaRewriteJobComplete(record));
            try std.testing.expectEqual(@as(u64, 12), record.completed_row_count);
            try std.testing.expectEqualStrings("order:z", record.progress_row_key);
            saw_ready = true;
        } else if (record.job_id == 9104) {
            try std.testing.expectEqual(@as(usize, 1), record.rewrite_renames.len);
            try std.testing.expectEqualStrings("status", record.rewrite_renames[0].old_path);
            try std.testing.expectEqualStrings("state", record.rewrite_renames[0].new_path);
            try std.testing.expectEqual(@as(usize, 1), record.rewrite_drops.len);
            try std.testing.expectEqualStrings("legacy_status", record.rewrite_drops[0]);
            saw_row_plan = true;
        } else if (record.job_id == 9105) {
            try std.testing.expect(record.full_row_rewrite);
            try std.testing.expectEqual(@as(usize, 0), record.rewrite_renames.len);
            try std.testing.expectEqual(@as(usize, 0), record.rewrite_drops.len);
            saw_full_row_rewrite = true;
        }
    }
    try std.testing.expect(saw_replaced and saw_ready and saw_row_plan and saw_full_row_rewrite);

    try std.testing.expect(manager.removeSchemaRewriteJob(9102));
    try std.testing.expect(!manager.removeSchemaRewriteJob(9102));
    try std.testing.expectEqual(@as(usize, 3), manager.removeSchemaRewriteJobsForTable(7));
    const remaining = try manager.listSchemaRewriteJobs(std.testing.allocator);
    defer manager.freeSchemaRewriteJobs(std.testing.allocator, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "table manager atomically applies table catalog updates with schema rewrite jobs" {
    const alloc = std.testing.allocator;
    const original_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const updated_schema_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    var manager = TableManager.init(alloc);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders", .schema_json = original_schema_json });
    const generation = schemaRewriteGenerationForSchemaJson(updated_schema_json);
    try manager.applyTableCatalogUpdateWithSchemaRewriteJobs(.{
        .table = .{ .table_id = 7, .name = "orders", .schema_json = updated_schema_json },
        .schema_rewrite_jobs = &.{
            .{
                .job_id = 9101,
                .table_id = 7,
                .group_id = 9001,
                .schema_generation = generation,
                .action = "validate",
                .reason = "constraints",
                .start_row_key = "",
            },
        },
    });

    const tables = try manager.listTables(alloc);
    defer manager.freeTables(alloc, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings(updated_schema_json, tables[0].schema_json);

    const jobs = try manager.listSchemaRewriteJobs(alloc);
    defer manager.freeSchemaRewriteJobs(alloc, jobs);
    try std.testing.expectEqual(@as(usize, 1), jobs.len);
    try std.testing.expectEqual(@as(u64, 9101), jobs[0].job_id);

    const rejected_schema_json = "{\"version\":3}";
    try std.testing.expectError(error.InvalidSchemaRewriteJob, manager.applyTableCatalogUpdateWithSchemaRewriteJobs(.{
        .table = .{ .table_id = 7, .name = "orders", .schema_json = rejected_schema_json },
        .schema_rewrite_jobs = &.{
            .{
                .job_id = 0,
                .table_id = 7,
                .group_id = 9001,
                .schema_generation = schemaRewriteGenerationForSchemaJson(rejected_schema_json),
                .action = "validate",
                .reason = "constraints",
                .start_row_key = "",
            },
        },
    }));

    const tables_after_error = try manager.listTables(alloc);
    defer manager.freeTables(alloc, tables_after_error);
    try std.testing.expectEqualStrings(updated_schema_json, tables_after_error[0].schema_json);
}

test "table manager atomically applies multi-table catalog batch updates with schema rewrite jobs" {
    const alloc = std.testing.allocator;
    const orders_schema_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const invoices_schema_json =
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}}}
    ;
    var manager = TableManager.init(alloc);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders", .schema_json = "{\"version\":1}" });
    try manager.upsertTable(.{ .table_id = 8, .name = "invoices", .schema_json = "{\"version\":1}" });
    try manager.applyTableCatalogBatchUpdateWithSchemaRewriteJobs(.{
        .tables = &.{
            .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json },
            .{ .table_id = 8, .name = "invoices", .schema_json = invoices_schema_json },
        },
        .schema_rewrite_jobs = &.{
            .{
                .job_id = 9101,
                .table_id = 7,
                .group_id = 9001,
                .schema_generation = schemaRewriteGenerationForSchemaJson(orders_schema_json),
                .action = "validate",
                .reason = "constraints",
                .start_row_key = "",
            },
            .{
                .job_id = 9102,
                .table_id = 8,
                .group_id = 9002,
                .schema_generation = schemaRewriteGenerationForSchemaJson(invoices_schema_json),
                .action = "rewrite",
                .reason = "row_images",
                .start_row_key = "",
                .full_row_rewrite = true,
            },
        },
    });

    const tables = try manager.listTables(alloc);
    defer manager.freeTables(alloc, tables);
    try std.testing.expectEqual(@as(usize, 2), tables.len);

    const jobs = try manager.listSchemaRewriteJobs(alloc);
    defer manager.freeSchemaRewriteJobs(alloc, jobs);
    try std.testing.expectEqual(@as(usize, 2), jobs.len);

    try std.testing.expectError(error.InvalidSchemaRewriteJob, manager.applyTableCatalogBatchUpdateWithSchemaRewriteJobs(.{
        .tables = &.{
            .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json },
            .{ .table_id = 8, .name = "invoices", .schema_json = invoices_schema_json },
        },
        .schema_rewrite_jobs = &.{
            .{
                .job_id = 9103,
                .table_id = 9,
                .group_id = 9003,
                .schema_generation = schemaRewriteGenerationForSchemaJson(orders_schema_json),
                .action = "validate",
                .reason = "constraints",
                .start_row_key = "",
            },
        },
    }));
}

test "table manager applies table catalog drop with child updates atomically" {
    const alloc = std.testing.allocator;
    const child_schema_json =
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"parent_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    var manager = TableManager.init(alloc);
    defer manager.deinit();

    const sequence_id = deriveSequenceId(default_database_name, default_namespace_name, "parents_id_seq");
    try manager.upsertTable(.{ .table_id = 7, .name = "parents", .schema_json = "{\"version\":1}" });
    try manager.upsertTable(.{ .table_id = 8, .name = "children", .schema_json = "{\"version\":1}" });
    try manager.upsertSequence(.{ .sequence_id = sequence_id, .name = "parents_id_seq" });
    try manager.upsertRange(.{ .table_id = 7, .group_id = 9001, .start_key = "", .end_key = null });
    try manager.upsertRange(.{ .table_id = 8, .group_id = 9002, .start_key = "", .end_key = null });

    try std.testing.expectError(error.UnknownRange, manager.applyTableCatalogDropWithSchemaRewriteJobs(.{
        .table_id = 7,
        .range_group_ids = &.{9002},
        .table_updates = &.{.{ .table_id = 8, .name = "children", .schema_json = child_schema_json }},
        .schema_rewrite_jobs = &.{},
    }));
    try std.testing.expect(manager.tables.contains(7));
    try std.testing.expect(manager.ranges.contains(9001));

    try manager.applyTableCatalogDropWithSchemaRewriteJobs(.{
        .table_id = 7,
        .sequence_ids = &.{sequence_id},
        .range_group_ids = &.{9001},
        .table_updates = &.{.{ .table_id = 8, .name = "children", .schema_json = child_schema_json }},
        .schema_rewrite_jobs = &.{
            .{
                .job_id = 9101,
                .table_id = 8,
                .group_id = 9002,
                .schema_generation = schemaRewriteGenerationForSchemaJson(child_schema_json),
                .action = "rewrite",
                .reason = "drop_fk_parent",
                .start_row_key = "",
                .full_row_rewrite = true,
            },
        },
    });

    try std.testing.expect(!manager.tables.contains(7));
    try std.testing.expect(!manager.sequences.contains(sequence_id));
    try std.testing.expect(!manager.ranges.contains(9001));
    const child = manager.tables.get(8) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(child_schema_json, child.schema_json);
    const jobs = try manager.listSchemaRewriteJobs(alloc);
    defer manager.freeSchemaRewriteJobs(alloc, jobs);
    try std.testing.expectEqual(@as(usize, 1), jobs.len);
    try std.testing.expectEqual(@as(u64, 9101), jobs[0].job_id);
}

test "table manager applies schema rewrite job lifecycle operations" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = null,
        .full_row_rewrite = true,
    });

    try std.testing.expectError(error.InvalidSchemaRewriteJobLease, manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "",
        .now_ms = 1000,
        .lease_expires_at_ms = 1234,
    }));
    try std.testing.expectError(error.InvalidSchemaRewriteJobLease, manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 1000,
    }));
    try manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 1234,
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqual(@as(usize, 1), jobs.len);
        try std.testing.expectEqualStrings(schema_rewrite_running, jobs[0].state);
        try std.testing.expectEqualStrings("worker-a", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 1234), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 1), jobs[0].attempts);
    }

    try std.testing.expectError(error.SchemaRewriteJobClaimBusy, manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-b",
        .now_ms = 1200,
        .lease_expires_at_ms = 2000,
    }));
    try manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-b",
        .now_ms = 1234,
        .lease_expires_at_ms = 2000,
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(schema_rewrite_running, jobs[0].state);
        try std.testing.expectEqualStrings("worker-b", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 2000), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 2), jobs[0].attempts);
    }

    try std.testing.expectError(error.SchemaRewriteJobLeaseMismatch, manager.finishSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-a",
        .completed_row_count = 17,
        .progress_row_key = "order:z",
    }));
    try manager.finishSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-b",
        .completed_row_count = 17,
        .progress_row_key = "order:z",
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(schema_rewrite_ready, jobs[0].state);
        try std.testing.expect(schemaRewriteJobComplete(jobs[0]));
        try std.testing.expectEqualStrings("", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u64, 17), jobs[0].completed_row_count);
        try std.testing.expectEqualStrings("order:z", jobs[0].progress_row_key);
    }

    try std.testing.expectError(error.SchemaRewriteJobNotDeclared, manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-c",
        .now_ms = 3000,
        .lease_expires_at_ms = 4000,
    }));
    try std.testing.expectError(error.SchemaRewriteJobNotRunning, manager.invalidateSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-b",
        .last_error = "schema generation moved",
    }));
    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9102,
        .table_id = 7,
        .group_id = 9002,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "m",
        .end_row_key = null,
        .full_row_rewrite = true,
    });
    try manager.beginSchemaRewriteJob(.{
        .job_id = 9102,
        .lease_owner = "worker-c",
        .now_ms = 3000,
        .lease_expires_at_ms = 4000,
    });
    try std.testing.expectError(error.SchemaRewriteJobLeaseMismatch, manager.invalidateSchemaRewriteJob(.{
        .job_id = 9102,
        .lease_owner = "worker-b",
        .last_error = "schema generation moved",
    }));
    try manager.invalidateSchemaRewriteJob(.{
        .job_id = 9102,
        .lease_owner = "worker-c",
        .last_error = "schema generation moved",
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        var saw_invalid = false;
        for (jobs) |job| {
            if (job.job_id != 9102) continue;
            try std.testing.expectEqualStrings(schema_rewrite_invalid, job.state);
            try std.testing.expectEqualStrings("", job.lease_owner);
            try std.testing.expectEqual(@as(u64, 0), job.lease_expires_at_ms);
            try std.testing.expectEqualStrings("schema generation moved", job.last_error);
            saw_invalid = true;
        }
        try std.testing.expect(saw_invalid);
    }

    try std.testing.expectError(error.UnknownSchemaRewriteJob, manager.finishSchemaRewriteJob(.{
        .job_id = 9999,
        .lease_owner = "worker-c",
        .completed_row_count = 1,
        .progress_row_key = "order:a",
    }));
}

test "table manager applies schema rewrite operator controls" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    try manager.upsertSchemaRewriteJob(.{
        .job_id = 9101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .full_row_rewrite = true,
    });

    try manager.pauseSchemaRewriteJob(.{
        .job_id = 9101,
        .reason = "operator pause",
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(schema_rewrite_paused, jobs[0].state);
        try std.testing.expectEqualStrings("", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqualStrings("operator pause", jobs[0].last_error);
    }
    try std.testing.expectError(error.SchemaRewriteJobNotDeclared, manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 2000,
    }));

    try manager.resumeSchemaRewriteJob(.{
        .job_id = 9101,
        .reason = "operator resume",
    });
    try manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 2000,
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(schema_rewrite_running, jobs[0].state);
        try std.testing.expectEqualStrings("worker-a", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u32, 1), jobs[0].attempts);
    }

    try manager.pauseSchemaRewriteJob(.{
        .job_id = 9101,
        .reason = "operator takeover pause",
    });
    try manager.resumeSchemaRewriteJob(.{ .job_id = 9101 });
    try manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-b",
        .now_ms = 2100,
        .lease_expires_at_ms = 3000,
    });
    try manager.invalidateSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-b",
        .last_error = "worker failure",
    });
    try manager.retrySchemaRewriteJob(.{
        .job_id = 9101,
        .reason = "operator retry",
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(schema_rewrite_declared, jobs[0].state);
        try std.testing.expectEqualStrings("", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqualStrings("operator retry", jobs[0].last_error);
        try std.testing.expectEqual(@as(u32, 2), jobs[0].attempts);
    }

    try manager.cancelSchemaRewriteJob(.{
        .job_id = 9101,
        .reason = "operator cancel",
    });
    {
        const jobs = try manager.listSchemaRewriteJobs(std.testing.allocator);
        defer manager.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(schema_rewrite_canceled, jobs[0].state);
        try std.testing.expectEqualStrings("operator cancel", jobs[0].last_error);
    }
    try std.testing.expectError(error.SchemaRewriteJobNotDeclared, manager.beginSchemaRewriteJob(.{
        .job_id = 9101,
        .lease_owner = "worker-c",
        .now_ms = 4000,
        .lease_expires_at_ms = 5000,
    }));

    try std.testing.expectError(error.SchemaRewriteJobCanceled, manager.cancelSchemaRewriteJob(.{ .job_id = 9101 }));
    try std.testing.expectError(error.SchemaRewriteJobNotPaused, manager.resumeSchemaRewriteJob(.{ .job_id = 9101 }));
    try std.testing.expectError(error.SchemaRewriteJobNotInvalid, manager.retrySchemaRewriteJob(.{ .job_id = 9101 }));
}

test "table emptying affected table ids require primary nonzero unique membership" {
    try std.testing.expect(tableEmptyingAffectedTableIdsValid(7, &.{7}));
    try std.testing.expect(tableEmptyingAffectedTableIdsValid(7, &.{ 8, 7, 9 }));
    try std.testing.expect(!tableEmptyingAffectedTableIdsValid(0, &.{7}));
    try std.testing.expect(!tableEmptyingAffectedTableIdsValid(7, &.{}));
    try std.testing.expect(!tableEmptyingAffectedTableIdsValid(7, &.{0}));
    try std.testing.expect(!tableEmptyingAffectedTableIdsValid(7, &.{8}));
    try std.testing.expect(!tableEmptyingAffectedTableIdsValid(7, &.{ 7, 8, 7 }));
    try std.testing.expect(tableEmptyingAffectedTableIdsCanonicalSetValid(&.{ 7, 8, 9 }));
    try std.testing.expect(!tableEmptyingAffectedTableIdsCanonicalSetValid(&.{ 8, 7, 9 }));
    try std.testing.expect(tableEmptyingAffectedTableIdsCanonicalValid(7, &.{ 7, 8 }));
    try std.testing.expect(!tableEmptyingAffectedTableIdsCanonicalValid(7, &.{ 8, 7, 9 }));
}

test "table manager owns table emptying jobs" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    try manager.upsertTable(.{ .table_id = 8, .name = "order_items" });

    try manager.upsertTableEmptyingJob(.{
        .job_id = 9201,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{7},
        .restart_identity = true,
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 1234,
    });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 9202,
        .table_id = 7,
        .group_id = 9002,
        .schema_generation = 42,
        .affected_table_ids = &.{ 7, 8 },
        .cascade = true,
        .state = table_emptying_ready,
        .completed_row_count = 12,
        .progress_row_key = "order:z",
    });

    try std.testing.expectError(error.UnknownTable, manager.upsertTableEmptyingJob(.{
        .job_id = 9299,
        .table_id = 99,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{99},
    }));
    try std.testing.expectError(error.UnknownTable, manager.upsertTableEmptyingJob(.{
        .job_id = 9299,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{ 7, 99 },
    }));
    try std.testing.expectError(error.InvalidTableEmptyingJob, manager.upsertTableEmptyingJob(.{
        .job_id = 0,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{7},
    }));
    try std.testing.expectError(error.InvalidTableEmptyingGeneration, manager.upsertTableEmptyingJob(.{
        .job_id = 9203,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 0,
        .affected_table_ids = &.{7},
    }));
    try std.testing.expectError(error.InvalidTableEmptyingJobState, manager.upsertTableEmptyingJob(.{
        .job_id = 9203,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{7},
        .state = "unknown",
    }));
    try std.testing.expectError(error.InvalidTableEmptyingJob, manager.upsertTableEmptyingJob(.{
        .job_id = 9203,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{},
    }));
    try std.testing.expectError(error.InvalidTableEmptyingJob, manager.upsertTableEmptyingJob(.{
        .job_id = 9203,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{8},
    }));
    try std.testing.expectError(error.InvalidTableEmptyingJob, manager.upsertTableEmptyingJob(.{
        .job_id = 9203,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{ 7, 7 },
    }));
    try std.testing.expectError(error.InvalidTableEmptyingJob, manager.upsertTableEmptyingJob(.{
        .job_id = 9203,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{ 8, 7 },
    }));

    try manager.upsertTableEmptyingJob(.{
        .job_id = 9201,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 43,
        .affected_table_ids = &.{7},
        .state = table_emptying_invalid,
        .last_error = "schema generation moved",
    });

    const listed = try manager.listTableEmptyingJobs(std.testing.allocator);
    defer manager.freeTableEmptyingJobs(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 2), listed.len);

    var saw_replaced = false;
    var saw_ready = false;
    for (listed) |record| {
        if (record.job_id == 9201) {
            try std.testing.expectEqual(@as(u64, 43), record.schema_generation);
            try std.testing.expectEqualStrings(table_emptying_invalid, record.state);
            try std.testing.expectEqualStrings("schema generation moved", record.last_error);
            try std.testing.expectEqual(@as(usize, 1), record.affected_table_ids.len);
            try std.testing.expectEqual(@as(u64, 7), record.affected_table_ids[0]);
            saw_replaced = true;
        } else if (record.job_id == 9202) {
            try std.testing.expect(tableEmptyingJobComplete(record));
            try std.testing.expect(record.cascade);
            try std.testing.expectEqual(@as(usize, 2), record.affected_table_ids.len);
            try std.testing.expectEqual(@as(u64, 12), record.completed_row_count);
            try std.testing.expectEqualStrings("order:z", record.progress_row_key);
            saw_ready = true;
        }
    }
    try std.testing.expect(saw_replaced and saw_ready);

    try std.testing.expect(manager.removeTableEmptyingJob(9201));
    try std.testing.expect(!manager.removeTableEmptyingJob(9201));
    try std.testing.expectEqual(@as(usize, 1), manager.removeTableEmptyingJobsForTable(8));
    const remaining = try manager.listTableEmptyingJobs(std.testing.allocator);
    defer manager.freeTableEmptyingJobs(std.testing.allocator, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "table emptying stable job id captures durable intent" {
    const base = TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{ 7, 8 },
        .restart_identity = true,
        .cascade = true,
    };
    const same = TableEmptyingJobRecord{
        .job_id = 1234,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{ 7, 8 },
        .restart_identity = true,
        .cascade = true,
        .state = table_emptying_invalid,
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 123,
        .attempts = 2,
        .completed_row_count = 9,
        .progress_row_key = "row:z",
        .last_error = "boom",
    };
    try std.testing.expect(stableTableEmptyingJobId(base) != 0);
    try std.testing.expectEqual(stableTableEmptyingJobId(base), stableTableEmptyingJobId(same));

    var changed_generation = base;
    changed_generation.schema_generation = 43;
    try std.testing.expect(stableTableEmptyingJobId(base) != stableTableEmptyingJobId(changed_generation));

    var changed_group = base;
    changed_group.group_id = 9002;
    try std.testing.expect(stableTableEmptyingJobId(base) != stableTableEmptyingJobId(changed_group));

    var changed_range = base;
    changed_range.range_id = 9101;
    try std.testing.expect(stableTableEmptyingJobId(base) != stableTableEmptyingJobId(changed_range));

    var changed_affected = base;
    changed_affected.affected_table_ids = &.{7};
    try std.testing.expect(stableTableEmptyingJobId(base) != stableTableEmptyingJobId(changed_affected));

    var changed_restart_identity = base;
    changed_restart_identity.restart_identity = false;
    try std.testing.expect(stableTableEmptyingJobId(base) != stableTableEmptyingJobId(changed_restart_identity));

    var changed_cascade = base;
    changed_cascade.cascade = false;
    try std.testing.expect(stableTableEmptyingJobId(base) != stableTableEmptyingJobId(changed_cascade));
}

test "table manager applies table emptying job lifecycle operations" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 9201,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{7},
        .restart_identity = true,
    });

    try std.testing.expectError(error.InvalidTableEmptyingJobLease, manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "",
        .now_ms = 1000,
        .lease_expires_at_ms = 1234,
    }));
    try std.testing.expectError(error.InvalidTableEmptyingJobLease, manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 1000,
    }));
    try manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 1234,
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqual(@as(usize, 1), jobs.len);
        try std.testing.expectEqualStrings(table_emptying_running, jobs[0].state);
        try std.testing.expectEqualStrings("worker-a", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 1234), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 1), jobs[0].attempts);
    }

    try std.testing.expectError(error.TableEmptyingJobClaimBusy, manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-b",
        .now_ms = 1200,
        .lease_expires_at_ms = 2000,
    }));
    try manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-b",
        .now_ms = 1234,
        .lease_expires_at_ms = 2000,
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(table_emptying_running, jobs[0].state);
        try std.testing.expectEqualStrings("worker-b", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 2000), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 2), jobs[0].attempts);
    }

    try std.testing.expectError(error.TableEmptyingJobLeaseMismatch, manager.finishTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-a",
        .completed_row_count = 17,
        .progress_row_key = "order:z",
    }));
    try manager.finishTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-b",
        .completed_row_count = 17,
        .progress_row_key = "order:z",
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(table_emptying_ready, jobs[0].state);
        try std.testing.expect(tableEmptyingJobComplete(jobs[0]));
        try std.testing.expectEqualStrings("", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u64, 17), jobs[0].completed_row_count);
        try std.testing.expectEqualStrings("order:z", jobs[0].progress_row_key);
    }

    try std.testing.expectError(error.TableEmptyingJobNotDeclared, manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-c",
        .now_ms = 3000,
        .lease_expires_at_ms = 4000,
    }));
    try std.testing.expectError(error.TableEmptyingJobNotRunning, manager.invalidateTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-b",
        .last_error = "schema generation moved",
    }));
    try manager.upsertTableEmptyingJob(.{
        .job_id = 9202,
        .table_id = 7,
        .group_id = 9002,
        .schema_generation = 42,
        .affected_table_ids = &.{7},
    });
    try manager.beginTableEmptyingJob(.{
        .job_id = 9202,
        .lease_owner = "worker-c",
        .now_ms = 3000,
        .lease_expires_at_ms = 4000,
    });
    try std.testing.expectError(error.TableEmptyingJobLeaseMismatch, manager.invalidateTableEmptyingJob(.{
        .job_id = 9202,
        .lease_owner = "worker-b",
        .last_error = "schema generation moved",
    }));
    try manager.invalidateTableEmptyingJob(.{
        .job_id = 9202,
        .lease_owner = "worker-c",
        .last_error = "schema generation moved",
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        var saw_invalid = false;
        for (jobs) |job| {
            if (job.job_id != 9202) continue;
            try std.testing.expectEqualStrings(table_emptying_invalid, job.state);
            try std.testing.expectEqualStrings("", job.lease_owner);
            try std.testing.expectEqual(@as(u64, 0), job.lease_expires_at_ms);
            try std.testing.expectEqualStrings("schema generation moved", job.last_error);
            saw_invalid = true;
        }
        try std.testing.expect(saw_invalid);
    }

    try std.testing.expectError(error.UnknownTableEmptyingJob, manager.finishTableEmptyingJob(.{
        .job_id = 9999,
        .lease_owner = "worker-c",
        .completed_row_count = 1,
        .progress_row_key = "order:a",
    }));
}

test "table manager applies table emptying operator controls" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 9201,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = 42,
        .affected_table_ids = &.{7},
        .restart_identity = true,
    });

    try manager.pauseTableEmptyingJob(.{
        .job_id = 9201,
        .reason = "operator pause",
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(table_emptying_paused, jobs[0].state);
        try std.testing.expectEqualStrings("", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqualStrings("operator pause", jobs[0].last_error);
    }
    try std.testing.expectError(error.TableEmptyingJobNotDeclared, manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 2000,
    }));

    try manager.resumeTableEmptyingJob(.{
        .job_id = 9201,
        .reason = "operator resume",
    });
    try manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 2000,
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(table_emptying_running, jobs[0].state);
        try std.testing.expectEqualStrings("worker-a", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u32, 1), jobs[0].attempts);
    }

    try manager.pauseTableEmptyingJob(.{
        .job_id = 9201,
        .reason = "operator takeover pause",
    });
    try manager.resumeTableEmptyingJob(.{ .job_id = 9201 });
    try manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-b",
        .now_ms = 2100,
        .lease_expires_at_ms = 3000,
    });
    try manager.invalidateTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-b",
        .last_error = "worker failure",
    });
    try manager.retryTableEmptyingJob(.{
        .job_id = 9201,
        .reason = "operator retry",
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(table_emptying_declared, jobs[0].state);
        try std.testing.expectEqualStrings("", jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), jobs[0].lease_expires_at_ms);
        try std.testing.expectEqualStrings("operator retry", jobs[0].last_error);
        try std.testing.expectEqual(@as(u32, 2), jobs[0].attempts);
    }

    try manager.cancelTableEmptyingJob(.{
        .job_id = 9201,
        .reason = "operator cancel",
    });
    {
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqualStrings(table_emptying_canceled, jobs[0].state);
        try std.testing.expectEqualStrings("operator cancel", jobs[0].last_error);
    }
    try std.testing.expectError(error.TableEmptyingJobNotDeclared, manager.beginTableEmptyingJob(.{
        .job_id = 9201,
        .lease_owner = "worker-c",
        .now_ms = 4000,
        .lease_expires_at_ms = 5000,
    }));

    try std.testing.expectError(error.TableEmptyingJobCanceled, manager.cancelTableEmptyingJob(.{ .job_id = 9201 }));
    try std.testing.expectError(error.TableEmptyingJobNotPaused, manager.resumeTableEmptyingJob(.{ .job_id = 9201 }));
    try std.testing.expectError(error.TableEmptyingJobNotInvalid, manager.retryTableEmptyingJob(.{ .job_id = 9201 }));
}

test "table manager applies secondary index rebuild lifecycle operations" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "orders" });
    const selector: SecondaryIndexRebuildRangeSelector = .{
        .table_id = 7,
        .index_name = "orders_status_idx",
        .index_generation = 42,
        .start_row_key = "",
    };
    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 7,
        .index_name = "orders_status_idx",
        .index_generation = 42,
        .start_row_key = "",
        .end_row_key = null,
        .group_id = 8101,
    });

    try manager.beginSecondaryIndexRebuildRange(.{ .selector = selector, .lease_owner = "worker-a", .now_ms = 1000, .lease_expires_at_ms = 1234 });
    {
        const ranges = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
        defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings(secondary_index_rebuild_building, ranges[0].state);
        try std.testing.expectEqualStrings("worker-a", ranges[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 1234), ranges[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 1), ranges[0].attempts);
    }
    try std.testing.expectError(error.SecondaryIndexRebuildRangeClaimBusy, manager.beginSecondaryIndexRebuildRange(.{ .selector = selector, .lease_owner = "worker-b", .now_ms = 1200, .lease_expires_at_ms = 2000 }));
    try manager.beginSecondaryIndexRebuildRange(.{ .selector = selector, .lease_owner = "worker-b", .now_ms = 1234, .lease_expires_at_ms = 2000 });
    {
        const ranges = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
        defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, ranges);
        try std.testing.expectEqualStrings(secondary_index_rebuild_building, ranges[0].state);
        try std.testing.expectEqualStrings("worker-b", ranges[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 2000), ranges[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 2), ranges[0].attempts);
    }

    try manager.saveSecondaryIndexRebuildRangeProgress(.{ .selector = selector, .completed_row_count = 5, .progress_row_key = "order:m" });
    {
        const ranges = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
        defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, ranges);
        try std.testing.expectEqualStrings(secondary_index_rebuild_building, ranges[0].state);
        try std.testing.expectEqualStrings("", ranges[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), ranges[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u64, 5), ranges[0].completed_row_count);
        try std.testing.expectEqualStrings("order:m", ranges[0].progress_row_key);
    }
    try manager.beginSecondaryIndexRebuildRange(.{ .selector = selector, .lease_owner = "worker-c", .now_ms = 2001, .lease_expires_at_ms = 3000 });
    {
        const ranges = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
        defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, ranges);
        try std.testing.expectEqualStrings(secondary_index_rebuild_building, ranges[0].state);
        try std.testing.expectEqualStrings("worker-c", ranges[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 3000), ranges[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 3), ranges[0].attempts);
        try std.testing.expectEqual(@as(u64, 5), ranges[0].completed_row_count);
        try std.testing.expectEqualStrings("order:m", ranges[0].progress_row_key);
    }

    try manager.finishSecondaryIndexRebuildRange(.{ .selector = selector, .completed_row_count = 17, .progress_row_key = "order:z" });
    {
        const ranges = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
        defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, ranges);
        try std.testing.expectEqualStrings(secondary_index_rebuild_ready, ranges[0].state);
        try std.testing.expect(secondaryIndexRebuildRangeComplete(ranges[0]));
        try std.testing.expectEqualStrings("", ranges[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 0), ranges[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u64, 17), ranges[0].completed_row_count);
        try std.testing.expectEqualStrings("order:z", ranges[0].progress_row_key);
    }

    try std.testing.expectError(error.SecondaryIndexRebuildRangeNotDeclared, manager.beginSecondaryIndexRebuildRange(.{ .selector = selector, .lease_owner = "worker-b", .now_ms = 5678, .lease_expires_at_ms = 6000 }));
    try manager.invalidateSecondaryIndexRebuildRange(.{ .selector = selector, .last_error = "schema generation moved" });
    {
        const ranges = try manager.listSecondaryIndexRebuildRanges(std.testing.allocator);
        defer manager.freeSecondaryIndexRebuildRanges(std.testing.allocator, ranges);
        try std.testing.expectEqualStrings(secondary_index_rebuild_invalid, ranges[0].state);
        try std.testing.expectEqualStrings("schema generation moved", ranges[0].last_error);
    }
}

test "table manager promotes table-emptying barriers atomically" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    const schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}";
    const schema_generation = schemaRewriteGenerationForSchemaJson(schema_json);
    try manager.upsertTable(.{ .table_id = 7, .name = "orders", .schema_json = schema_json, .data_generation = 4 });
    try manager.upsertTable(.{ .table_id = 8, .name = "order_items", .schema_json = schema_json, .data_generation = 9 });
    try manager.upsertRange(.{ .group_id = 9001, .range_id = 9101, .table_id = 7, .start_key = "", .end_key = null });
    try manager.upsertRange(.{ .group_id = 9002, .range_id = 9102, .table_id = 8, .start_key = "", .end_key = null });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 7101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = schema_generation,
        .data_generation = 4,
        .barrier_id = 77,
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
    });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 8101,
        .table_id = 8,
        .group_id = 9002,
        .schema_generation = schema_generation,
        .data_generation = 9,
        .barrier_id = 77,
        .state = table_emptying_declared,
        .affected_table_ids = &.{ 7, 8 },
    });

    try std.testing.expectError(error.TableEmptyingJobNotReady, manager.promoteTableEmptyingBarrier(.{
        .job_ids = &.{ 7101, 8101 },
        .promotions = &.{
            .{ .table_id = 7, .target_generation = 5 },
            .{ .table_id = 8, .target_generation = 10 },
        },
    }));

    {
        const tables = try manager.listTables(std.testing.allocator);
        defer manager.freeTables(std.testing.allocator, tables);
        for (tables) |table| {
            if (table.table_id == 7) try std.testing.expectEqual(@as(u64, 4), table.data_generation);
            if (table.table_id == 8) try std.testing.expectEqual(@as(u64, 9), table.data_generation);
        }
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqual(@as(usize, 2), jobs.len);
    }

    try manager.upsertTableEmptyingJob(.{
        .job_id = 8101,
        .table_id = 8,
        .group_id = 9002,
        .schema_generation = schema_generation,
        .data_generation = 9,
        .barrier_id = 77,
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
    });
    try manager.promoteTableEmptyingBarrier(.{
        .job_ids = &.{ 7101, 8101 },
        .promotions = &.{
            .{ .table_id = 7, .target_generation = 5 },
            .{ .table_id = 8, .target_generation = 10 },
        },
    });
    {
        const tables = try manager.listTables(std.testing.allocator);
        defer manager.freeTables(std.testing.allocator, tables);
        for (tables) |table| {
            if (table.table_id == 7) try std.testing.expectEqual(@as(u64, 5), table.data_generation);
            if (table.table_id == 8) try std.testing.expectEqual(@as(u64, 10), table.data_generation);
        }
        const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
        defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqual(@as(usize, 0), jobs.len);
    }
}

test "table manager promotes restart identity barrier by resetting sequence allocation markers" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric","x-antfly-default":{"op":"sequence_next","sequence":"usage_id_seq"}},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema_generation = schemaRewriteGenerationForSchemaJson(schema_json);
    try manager.upsertTable(.{
        .table_id = 7,
        .name = "usage_records",
        .database_name = "tenant",
        .namespace_name = "billing",
        .schema_json = schema_json,
        .data_generation = 4,
    });
    try manager.upsertRange(.{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    });
    const sequence_id = deriveSequenceId("tenant", "billing", "usage_id_seq");
    try manager.upsertSequence(.{
        .sequence_id = sequence_id,
        .name = "usage_id_seq",
        .database_name = "tenant",
        .namespace_name = "billing",
        .options_json = "{\"start_with\":10,\"increment_by\":5}",
        .last_value = 5,
        .last_allocation_id = 1234,
    });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 7101,
        .table_id = 7,
        .group_id = 9001,
        .range_id = 9101,
        .schema_generation = schema_generation,
        .data_generation = 4,
        .barrier_id = 77,
        .state = table_emptying_ready,
        .affected_table_ids = &.{7},
        .restart_identity = true,
    });

    try manager.promoteTableEmptyingBarrier(.{
        .job_ids = &.{7101},
        .promotions = &.{.{ .table_id = 7, .target_generation = 5 }},
    });

    try std.testing.expectEqual(@as(u64, 5), manager.tables.get(7).?.data_generation);
    try std.testing.expectEqual(@as(i64, 5), manager.sequences.get(sequence_id).?.last_value);
    try std.testing.expectEqual(@as(u128, 0), manager.sequences.get(sequence_id).?.last_allocation_id);
    const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
    defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 0), jobs.len);
}

test "table manager validates table-emptying identity allocator reset barriers" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    const schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}";
    const schema_generation = schemaRewriteGenerationForSchemaJson(schema_json);
    try manager.upsertTable(.{ .table_id = 7, .name = "orders", .schema_json = schema_json, .data_generation = 4 });
    try manager.upsertTable(.{ .table_id = 8, .name = "order_items", .schema_json = schema_json, .data_generation = 9 });
    try manager.upsertRange(.{ .group_id = 9001, .range_id = 9101, .table_id = 7, .start_key = "", .end_key = "m" });
    try manager.upsertRange(.{ .group_id = 9002, .range_id = 9102, .table_id = 7, .start_key = "m", .end_key = null });
    try manager.upsertRange(.{ .group_id = 9003, .range_id = 9103, .table_id = 8, .start_key = "", .end_key = null });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 7101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = schema_generation,
        .data_generation = 4,
        .barrier_id = 77,
        .start_row_key = "",
        .end_row_key = "m",
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
        .restart_identity = true,
        .cascade = true,
    });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 7102,
        .table_id = 7,
        .group_id = 9002,
        .schema_generation = schema_generation,
        .data_generation = 4,
        .barrier_id = 77,
        .start_row_key = "m",
        .end_row_key = null,
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
        .restart_identity = true,
        .cascade = true,
    });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 8101,
        .table_id = 8,
        .group_id = 9003,
        .schema_generation = schema_generation,
        .data_generation = 9,
        .barrier_id = 77,
        .start_row_key = "",
        .end_row_key = null,
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
        .restart_identity = true,
        .cascade = true,
    });

    try manager.validateTableEmptyingIdentityAllocatorReset(.{
        .barrier_id = 77,
        .affected_table_ids = &.{ 7, 8 },
        .job_ids = &.{ 7101, 7102, 8101 },
        .cascade = true,
    });
    try std.testing.expectError(error.InvalidTableEmptyingIdentityAllocatorReset, manager.validateTableEmptyingIdentityAllocatorReset(.{
        .barrier_id = 77,
        .affected_table_ids = &.{ 7, 8 },
        .job_ids = &.{ 7101, 7102 },
        .cascade = true,
    }));
    try std.testing.expectError(error.InvalidTableEmptyingIdentityAllocatorReset, manager.validateTableEmptyingIdentityAllocatorReset(.{
        .barrier_id = 77,
        .affected_table_ids = &.{ 8, 7 },
        .job_ids = &.{ 7101, 7102, 8101 },
        .cascade = true,
    }));

    try manager.upsertTableEmptyingJob(.{
        .job_id = 8101,
        .table_id = 8,
        .group_id = 9003,
        .schema_generation = schema_generation,
        .data_generation = 9,
        .barrier_id = 77,
        .start_row_key = "",
        .end_row_key = null,
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
        .restart_identity = false,
        .cascade = true,
    });
    try std.testing.expectError(error.InvalidTableEmptyingIdentityAllocatorReset, manager.validateTableEmptyingIdentityAllocatorReset(.{
        .barrier_id = 77,
        .affected_table_ids = &.{ 7, 8 },
        .job_ids = &.{ 7101, 7102, 8101 },
        .cascade = true,
    }));
}

test "table manager rejects mixed table-emptying barrier promotion" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    const schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}";
    const schema_generation = schemaRewriteGenerationForSchemaJson(schema_json);
    try manager.upsertTable(.{ .table_id = 7, .name = "orders", .schema_json = schema_json, .data_generation = 4 });
    try manager.upsertTable(.{ .table_id = 8, .name = "order_items", .schema_json = schema_json, .data_generation = 9 });
    try manager.upsertRange(.{ .group_id = 9001, .range_id = 9101, .table_id = 7, .start_key = "", .end_key = null });
    try manager.upsertRange(.{ .group_id = 9002, .range_id = 9102, .table_id = 8, .start_key = "", .end_key = null });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 7101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = schema_generation,
        .data_generation = 4,
        .barrier_id = 77,
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
    });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 8101,
        .table_id = 8,
        .group_id = 9002,
        .schema_generation = schema_generation,
        .data_generation = 9,
        .barrier_id = 78,
        .state = table_emptying_ready,
        .affected_table_ids = &.{ 7, 8 },
    });

    try std.testing.expectError(error.InvalidTableEmptyingBarrierPromotion, manager.promoteTableEmptyingBarrier(.{
        .job_ids = &.{ 7101, 8101 },
        .promotions = &.{
            .{ .table_id = 7, .target_generation = 5 },
            .{ .table_id = 8, .target_generation = 10 },
        },
    }));

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    for (tables) |table| {
        if (table.table_id == 7) try std.testing.expectEqual(@as(u64, 4), table.data_generation);
        if (table.table_id == 8) try std.testing.expectEqual(@as(u64, 9), table.data_generation);
    }
    const jobs = try manager.listTableEmptyingJobs(std.testing.allocator);
    defer manager.freeTableEmptyingJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 2), jobs.len);
}

test "table manager allocates sequence values from durable cursor" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertDatabase(.{
        .database_id = deriveDatabaseId("tenant"),
        .name = "tenant",
    });
    try manager.upsertNamespace(.{
        .namespace_id = deriveNamespaceId(deriveDatabaseId("tenant"), "billing"),
        .database_id = deriveDatabaseId("tenant"),
        .name = "billing",
    });
    const options_json = "{\"start_with\":10,\"increment_by\":5,\"max_value\":20}";
    try manager.upsertSequence(.{
        .sequence_id = deriveSequenceId("tenant", "billing", "usage_id_seq"),
        .name = "usage_id_seq",
        .database_name = "tenant",
        .namespace_name = "billing",
        .options_json = options_json,
        .last_value = try sequenceInitialLastValueFromOptionsJson(std.testing.allocator, options_json),
    });

    try std.testing.expectEqual(@as(i64, 10), try manager.allocateSequenceValue(std.testing.allocator, "tenant", "billing", "usage_id_seq"));
    try std.testing.expectEqual(@as(i64, 15), try manager.allocateSequenceValue(std.testing.allocator, "tenant", "billing", "usage_id_seq"));
    try std.testing.expectEqual(@as(i64, 20), try manager.allocateSequenceValue(std.testing.allocator, "tenant", "billing", "usage_id_seq"));
    try std.testing.expectError(error.SequenceExhausted, manager.allocateSequenceValue(std.testing.allocator, "tenant", "billing", "usage_id_seq"));
}

test "table manager resets table-owned sequence defaults for restart identity barriers" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric","x-antfly-default":{"op":"sequence_next","sequence":"usage_id_seq"}},"owned_id":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema_generation = schemaRewriteGenerationForSchemaJson(schema_json);
    try manager.upsertTable(.{
        .table_id = 7,
        .name = "usage_records",
        .database_name = "tenant",
        .namespace_name = "billing",
        .schema_json = schema_json,
        .data_generation = 4,
    });
    try manager.upsertRange(.{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    });
    const options_json = "{\"start_with\":10,\"increment_by\":5}";
    const sequence_id = deriveSequenceId("tenant", "billing", "usage_id_seq");
    const owned_options_json = "{\"start_with\":50,\"increment_by\":10,\"owned_by\":{\"table_name\":\"usage_records\",\"column_name\":\"owned_id\"}}";
    const owned_sequence_id = deriveSequenceId("tenant", "billing", "usage_owned_id_seq");
    try manager.upsertSequence(.{
        .sequence_id = sequence_id,
        .name = "usage_id_seq",
        .database_name = "tenant",
        .namespace_name = "billing",
        .options_json = options_json,
        .last_value = 100,
    });
    try manager.upsertSequence(.{
        .sequence_id = owned_sequence_id,
        .name = "usage_owned_id_seq",
        .database_name = "tenant",
        .namespace_name = "billing",
        .options_json = owned_options_json,
        .last_value = 200,
    });
    try manager.upsertTableEmptyingJob(.{
        .job_id = 7101,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = schema_generation,
        .data_generation = 4,
        .barrier_id = 77,
        .state = table_emptying_ready,
        .affected_table_ids = &.{7},
        .restart_identity = true,
    });

    const request: TableEmptyingIdentityAllocatorResetRequest = .{
        .barrier_id = 77,
        .affected_table_ids = &.{7},
        .job_ids = &.{7101},
    };
    const targets = try manager.tableEmptyingIdentityAllocatorResetTargetsAlloc(std.testing.allocator, request);
    defer std.testing.allocator.free(targets);
    try std.testing.expectEqual(@as(usize, 2), targets.len);
    try expectIdentityAllocatorResetTarget(targets, sequence_id, 5);
    try expectIdentityAllocatorResetTarget(targets, owned_sequence_id, 40);

    try std.testing.expectEqual(@as(usize, 2), try manager.resetIdentityAllocatorsForTableEmptyingBarrier(std.testing.allocator, request));
    try std.testing.expectEqual(@as(i64, 5), manager.sequences.get(sequence_id).?.last_value);
    try std.testing.expectEqual(@as(i64, 40), manager.sequences.get(owned_sequence_id).?.last_value);
    try std.testing.expectEqual(@as(u128, 0), manager.sequences.get(sequence_id).?.last_allocation_id);
    try std.testing.expectEqual(@as(u128, 0), manager.sequences.get(owned_sequence_id).?.last_allocation_id);
    try std.testing.expectEqual(@as(usize, 0), try manager.resetIdentityAllocatorsForTableEmptyingBarrier(std.testing.allocator, request));

    manager.sequences.getPtr(sequence_id).?.last_allocation_id = 9001;
    manager.sequences.getPtr(owned_sequence_id).?.last_allocation_id = 9002;
    try std.testing.expectEqual(@as(usize, 2), try manager.resetIdentityAllocatorsForTableEmptyingBarrier(std.testing.allocator, request));
    try std.testing.expectEqual(@as(u128, 0), manager.sequences.get(sequence_id).?.last_allocation_id);
    try std.testing.expectEqual(@as(u128, 0), manager.sequences.get(owned_sequence_id).?.last_allocation_id);
}

fn expectIdentityAllocatorResetTarget(targets: []const SequenceIdentityAllocatorReset, sequence_id: u64, reset_last_value: i64) !void {
    for (targets) |target| {
        if (target.sequence_id != sequence_id) continue;
        try std.testing.expectEqual(reset_last_value, target.reset_last_value);
        return;
    }
    return error.SequenceNotFound;
}

test "table manager parses placement classes and checks compatibility" {
    try std.testing.expectEqual(PlacementClass.serving, parsePlacementClass("serving").?);
    try std.testing.expectEqual(PlacementClass.bulk, parsePlacementClass("bulk").?);
    try std.testing.expect(parsePlacementClass("custom") == null);

    try std.testing.expect(placementRoleCompatible("serving", "serving"));
    try std.testing.expect(!placementRoleCompatible("serving", "bulk"));
    try std.testing.expect(placementRoleCompatible("custom", "custom"));
    try std.testing.expect(!placementRoleCompatible("custom", "archive"));
}
