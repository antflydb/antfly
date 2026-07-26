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
    description: []const u8 = "",
    schema_json: []const u8 = "",
    read_schema_json: []const u8 = "",
    indexes_json: []const u8 = "{}",
    replication_sources_json: []const u8 = "[]",
    placement_role: []const u8 = "data",
    restore_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    desired_replica_count: u16 = 3,
    min_ranges: u32 = 1,

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
        std.mem.eql(u8, lhs.description, rhs.description) and
        std.mem.eql(u8, lhs.schema_json, rhs.schema_json) and
        std.mem.eql(u8, lhs.read_schema_json, rhs.read_schema_json) and
        std.mem.eql(u8, lhs.indexes_json, rhs.indexes_json) and
        std.mem.eql(u8, lhs.replication_sources_json, rhs.replication_sources_json) and
        std.mem.eql(u8, lhs.placement_role, rhs.placement_role) and
        std.mem.eql(u8, lhs.restore_backup_id, rhs.restore_backup_id) and
        std.mem.eql(u8, lhs.restore_location, rhs.restore_location) and
        lhs.desired_replica_count == rhs.desired_replica_count and
        lhs.min_ranges == rhs.min_ranges;
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
    /// Monotonic source-local split attempt allocator. Gaps are valid: the
    /// range update may commit before its transition record during recovery.
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
};

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
        std.mem.eql(u8, lhs.restore_artifact_sha256, rhs.restore_artifact_sha256);
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
};

/// Merges membership observations without allowing a reporter that explicitly
/// lacks Raft voter-set knowledge to poison authoritative evidence. A count
/// from such a reporter remains useful as a conservative fallback; once any
/// fingerprint-qualified evidence exists, only qualified reports can conflict
/// with it.
pub const VoterSetEvidence = struct {
    fallback_voter_count: ?u16 = null,
    ambiguous_fallback_voter_count: bool = false,
    known_voter_count: ?u16 = null,
    known_voter_set_fingerprint: VoterSetFingerprint = [_]u8{0} ** voter_set_fingerprint_len,
    has_known_voter_set: bool = false,
    ambiguous_known_voter_set: bool = false,

    pub fn observe(self: *@This(), report: GroupStatusReport) void {
        if (report.voter_count == 0) return;
        if (self.fallback_voter_count) |existing| {
            if (existing != report.voter_count) self.ambiguous_fallback_voter_count = true;
        } else {
            self.fallback_voter_count = report.voter_count;
        }
        if (!report.voter_set_known) return;

        if (!self.has_known_voter_set) {
            self.known_voter_count = report.voter_count;
            self.known_voter_set_fingerprint = report.voter_set_fingerprint;
            self.has_known_voter_set = true;
            return;
        }
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
            if (leader.voter_set_known and leader.voter_count > 0) {
                return .{
                    .voter_count_known = true,
                    .voter_count = leader.voter_count,
                    .from_leader = true,
                };
            }
        }
        if (self.has_known_voter_set) {
            return .{
                .voter_count_known = !self.ambiguous_known_voter_set,
                .voter_count = self.known_voter_count.?,
                .from_leader = false,
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
    rollback_reason: ?[]const u8 = null,
    automatic: bool = false,
    projected: bool = false,
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
};

pub const TableManager = struct {
    alloc: std.mem.Allocator,
    tables: std.AutoHashMapUnmanaged(u64, TableRecord) = .empty,
    ranges: std.AutoHashMapUnmanaged(u64, RangeRecord) = .empty,
    split_intents: std.AutoHashMapUnmanaged(u64, SplitIntent) = .empty,
    merge_intents: std.AutoHashMapUnmanaged(u64, MergeIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) TableManager {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *TableManager) void {
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| freeTable(self.alloc, table.*);
        self.tables.deinit(self.alloc);

        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| freeRange(self.alloc, range.*);
        self.ranges.deinit(self.alloc);

        var split_it = self.split_intents.valueIterator();
        while (split_it.next()) |intent| freeSplitIntent(self.alloc, intent.*);
        self.split_intents.deinit(self.alloc);

        var merge_it = self.merge_intents.valueIterator();
        while (merge_it.next()) |intent| freeMergeIntent(self.alloc, intent.*);
        self.merge_intents.deinit(self.alloc);

        self.* = undefined;
    }

    pub fn upsertTable(self: *TableManager, record: TableRecord) !void {
        const owned = try cloneTable(self.alloc, record);
        errdefer freeTable(self.alloc, owned);
        if (self.tables.getPtr(record.table_id)) |existing| {
            freeTable(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.tables.put(self.alloc, record.table_id, owned);
    }

    pub fn upsertRange(self: *TableManager, record: RangeRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        const table = self.tables.get(record.table_id) orelse return error.UnknownTable;
        _ = table;

        var normalized = record;
        if (normalized.range_id == 0) normalized.range_id = normalized.group_id;
        const owned = try cloneRange(self.alloc, normalized);
        errdefer freeRange(self.alloc, owned);
        if (self.ranges.getPtr(record.group_id)) |existing| {
            freeRange(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.ranges.put(self.alloc, record.group_id, owned);
    }

    pub fn clearTopology(self: *TableManager) void {
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| freeTable(self.alloc, table.*);
        self.tables.clearRetainingCapacity();

        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| freeRange(self.alloc, range.*);
        self.ranges.clearRetainingCapacity();
    }

    pub fn replaceTopology(self: *TableManager, tables: []const TableRecord, ranges: []const RangeRecord) !void {
        self.clearTopology();
        for (tables) |record| try self.upsertTable(record);
        for (ranges) |record| try self.upsertRange(record);
    }

    pub const ProjectedTopologyLoadResult = struct {
        skipped_orphan_ranges: usize = 0,
    };

    pub fn replaceProjectedTopology(self: *TableManager, tables: []const TableRecord, ranges: []const RangeRecord) !ProjectedTopologyLoadResult {
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
        return result;
    }

    pub fn removeTable(self: *TableManager, table_id: u64) bool {
        const removed = self.tables.fetchRemove(table_id);
        if (removed) |entry| {
            freeTable(self.alloc, entry.value);
            return true;
        }
        return false;
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

    pub fn listTables(self: *TableManager, alloc: std.mem.Allocator) ![]TableRecord {
        var out = std.ArrayListUnmanaged(TableRecord).empty;
        errdefer {
            for (out.items) |record| freeTable(alloc, record);
            out.deinit(alloc);
        }
        var it = self.tables.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneTable(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeTables(_: *TableManager, alloc: std.mem.Allocator, records: []TableRecord) void {
        for (records) |record| freeTable(alloc, record);
        alloc.free(records);
    }

    pub fn listRanges(self: *TableManager, alloc: std.mem.Allocator) ![]RangeRecord {
        var out = std.ArrayListUnmanaged(RangeRecord).empty;
        errdefer {
            for (out.items) |record| freeRange(alloc, record);
            out.deinit(alloc);
        }
        var it = self.ranges.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneRange(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeRanges(_: *TableManager, alloc: std.mem.Allocator, records: []RangeRecord) void {
        for (records) |record| freeRange(alloc, record);
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

    /// Rehydrate active split intents from replicated metadata after a
    /// reconciliation-authority handoff. Projected records are authoritative:
    /// they replace a local copy with the same transition ID without allocating
    /// a new fencing epoch. Local intents still awaiting admission are retained.
    pub fn syncProjectedSplitTransitions(self: *TableManager, records: []const transition_state.SplitTransitionRecord) !void {
        var hydrated = std.ArrayListUnmanaged(SplitIntent).empty;
        defer hydrated.deinit(self.alloc);
        errdefer for (hydrated.items) |intent| freeSplitIntent(self.alloc, intent);

        for (records, 0..) |record, i| {
            if (transitionTerminal(record.phase)) continue;
            for (records[0..i]) |prior| {
                if (!transitionTerminal(prior.phase) and prior.transition_id == record.transition_id) {
                    return error.DuplicateProjectedSplitTransition;
                }
            }

            try group_ids.requireDataGroupId(record.source_group_id);
            try group_ids.requireDataGroupId(record.destination_group_id);
            const source = self.ranges.get(record.source_group_id) orelse return error.UnknownSourceRange;
            const split_key = record.split_key orelse return error.MissingSplitKey;
            if (record.attempt_epoch == 0 or source.split_attempt_epoch != record.attempt_epoch) {
                return error.ProjectedSplitEpochMismatch;
            }
            if (!keyStrictlyInsideRange(split_key, source.start_key, source.end_key)) return error.InvalidSplitKey;
            if (!optionalBytesEqual(record.source_range_end, source.end_key)) return error.ProjectedSplitRangeMismatch;
            if (self.ranges.contains(record.destination_group_id)) return error.DestinationRangeAlreadyExists;

            const owned = try cloneSplitIntent(self.alloc, .{
                .transition_id = record.transition_id,
                .attempt_epoch = record.attempt_epoch,
                .table_id = source.table_id,
                .source_group_id = record.source_group_id,
                .destination_group_id = record.destination_group_id,
                .split_key = split_key,
                .rollback_reason = record.rollback_reason,
                .projected = true,
            });
            errdefer freeSplitIntent(self.alloc, owned);
            try hydrated.append(self.alloc, owned);
        }

        var stale_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_ids.deinit(self.alloc);
        try stale_ids.ensureTotalCapacity(self.alloc, self.split_intents.count());
        var existing_it = self.split_intents.iterator();
        while (existing_it.next()) |entry| {
            if (entry.value_ptr.projected and !containsActiveSplitRecord(records, entry.key_ptr.*)) {
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

        for (records, 0..) |record, i| {
            if (transitionTerminal(record.phase)) continue;
            for (records[0..i]) |prior| {
                if (!transitionTerminal(prior.phase) and prior.transition_id == record.transition_id) {
                    return error.DuplicateProjectedMergeTransition;
                }
            }

            try group_ids.requireDataGroupId(record.donor_group_id);
            try group_ids.requireDataGroupId(record.receiver_group_id);
            const donor = self.ranges.get(record.donor_group_id) orelse return error.UnknownDonorRange;
            const receiver = self.ranges.get(record.receiver_group_id) orelse return error.UnknownReceiverRange;
            if (donor.table_id != receiver.table_id) return error.TableRangeMismatch;
            if (!rangesAdjacent(donor, receiver)) return error.RangesNotAdjacent;

            const owned = try cloneMergeIntent(self.alloc, .{
                .transition_id = record.transition_id,
                .table_id = donor.table_id,
                .donor_group_id = record.donor_group_id,
                .receiver_group_id = record.receiver_group_id,
                .rollback_reason = record.rollback_reason,
                .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
                .projected = true,
            });
            errdefer freeMergeIntent(self.alloc, owned);
            try hydrated.append(self.alloc, owned);
        }

        var stale_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_ids.deinit(self.alloc);
        try stale_ids.ensureTotalCapacity(self.alloc, self.merge_intents.count());
        var existing_it = self.merge_intents.iterator();
        while (existing_it.next()) |entry| {
            if (entry.value_ptr.projected and !containsActiveMergeRecord(records, entry.key_ptr.*)) {
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
        const split_key = record.split_key orelse return error.MissingSplitKey;
        const source = self.ranges.get(record.source_group_id) orelse return error.UnknownSourceRange;
        const identity_shard_id = rangeDocIdentityShardId(source);
        const identity_range_id = rangeDocIdentityRangeId(source);

        try self.upsertRange(.{
            .group_id = source.group_id,
            .range_id = source.range_id,
            .table_id = source.table_id,
            .start_key = source.start_key,
            .end_key = split_key,
            .doc_identity_shard_id = source.doc_identity_shard_id,
            .doc_identity_range_id = source.doc_identity_range_id,
            .split_attempt_epoch = source.split_attempt_epoch,
        });
        try self.upsertRange(.{
            .group_id = record.destination_group_id,
            .range_id = record.destination_group_id,
            .table_id = source.table_id,
            .start_key = split_key,
            .end_key = record.source_range_end,
            .doc_identity_shard_id = identity_shard_id,
            .doc_identity_range_id = identity_range_id,
            .split_attempt_epoch = 0,
        });
        _ = self.removeSplitIntent(record.transition_id);
    }

    pub fn applyRolledBackSplit(self: *TableManager, transition_id: u64) void {
        _ = self.removeSplitIntent(transition_id);
    }

    pub fn applyFinalizedMerge(self: *TableManager, record: transition_state.MergeTransitionRecord) !void {
        if (!self.merge_intents.contains(record.transition_id)) return;
        const donor = self.ranges.get(record.donor_group_id) orelse return error.UnknownDonorRange;
        const receiver = self.ranges.get(record.receiver_group_id) orelse return error.UnknownReceiverRange;
        const merged_start = if (std.mem.order(u8, donor.start_key, receiver.start_key) == .lt) donor.start_key else receiver.start_key;
        const merged_end = switch (optionalBytesOrder(donor.end_key, receiver.end_key)) {
            .lt => receiver.end_key,
            .eq => receiver.end_key,
            .gt => donor.end_key,
        };

        try self.upsertRange(.{
            .group_id = receiver.group_id,
            .range_id = receiver.range_id,
            .table_id = receiver.table_id,
            .start_key = merged_start,
            .end_key = merged_end,
            .doc_identity_shard_id = receiver.doc_identity_shard_id,
            .doc_identity_range_id = receiver.doc_identity_range_id,
        });
        _ = self.removeRange(donor.group_id);
        _ = self.removeMergeIntent(record.transition_id);
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
            try out.append(alloc, try self.buildSplitTransition(intent.*));
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
            try out.append(alloc, try cloneMergeTransitionRecord(alloc, .{
                .transition_id = intent.transition_id,
                .donor_group_id = intent.donor_group_id,
                .receiver_group_id = intent.receiver_group_id,
                .phase = .prepare,
                .rollback_reason = intent.rollback_reason,
                .allow_doc_identity_reassignment = intent.allow_doc_identity_reassignment,
            }));
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

    fn buildSplitTransition(self: *TableManager, intent: SplitIntent) !transition_state.SplitTransitionRecord {
        const source = self.ranges.get(intent.source_group_id) orelse return error.UnknownSourceRange;
        return try cloneSplitTransitionRecord(self.alloc, .{
            .transition_id = intent.transition_id,
            .attempt_epoch = intent.attempt_epoch,
            .source_group_id = intent.source_group_id,
            .destination_group_id = intent.destination_group_id,
            .phase = .prepare,
            .split_key = intent.split_key,
            .source_range_end = source.end_key,
            .rollback_reason = intent.rollback_reason,
        });
    }
};

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

fn containsActiveSplitRecord(records: []const transition_state.SplitTransitionRecord, transition_id: u64) bool {
    for (records) |record| {
        if (!transitionTerminal(record.phase) and record.transition_id == transition_id) return true;
    }
    return false;
}

fn containsActiveMergeRecord(records: []const transition_state.MergeTransitionRecord, transition_id: u64) bool {
    for (records) |record| {
        if (!transitionTerminal(record.phase) and record.transition_id == transition_id) return true;
    }
    return false;
}

fn cloneOwnedOptional(alloc: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    return if (value) |bytes| try alloc.dupe(u8, bytes) else null;
}

fn freeOwnedOptional(alloc: std.mem.Allocator, value: ?[]const u8) void {
    if (value) |bytes| alloc.free(bytes);
}

pub fn cloneTable(alloc: std.mem.Allocator, record: TableRecord) !TableRecord {
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
    alloc.free(record.name);
    alloc.free(record.description);
    alloc.free(record.schema_json);
    alloc.free(record.read_schema_json);
    alloc.free(record.indexes_json);
    alloc.free(record.replication_sources_json);
    alloc.free(record.placement_role);
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
    };
}

pub fn rangeDocIdentityShardId(record: RangeRecord) u64 {
    return if (record.doc_identity_shard_id == 0) record.group_id else record.doc_identity_shard_id;
}

pub fn rangeDocIdentityRangeId(record: RangeRecord) u64 {
    if (record.doc_identity_range_id != 0) return record.doc_identity_range_id;
    return if (record.range_id == 0) record.group_id else record.range_id;
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

pub fn cloneRestoreProgress(alloc: std.mem.Allocator, record: RestoreProgressRecord) !RestoreProgressRecord {
    const backup_id = try alloc.dupe(u8, record.backup_id);
    errdefer alloc.free(backup_id);
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
    return .{
        .name = name,
        .kind = kind,
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
    return .{
        .transition_id = intent.transition_id,
        .attempt_epoch = intent.attempt_epoch,
        .table_id = intent.table_id,
        .source_group_id = intent.source_group_id,
        .destination_group_id = intent.destination_group_id,
        .split_key = try alloc.dupe(u8, intent.split_key),
        .rollback_reason = try cloneOwnedOptional(alloc, intent.rollback_reason),
        .automatic = intent.automatic,
        .projected = intent.projected,
    };
}

fn freeSplitIntent(alloc: std.mem.Allocator, intent: SplitIntent) void {
    alloc.free(intent.split_key);
    freeOwnedOptional(alloc, intent.rollback_reason);
}

fn cloneMergeIntent(alloc: std.mem.Allocator, intent: MergeIntent) !MergeIntent {
    return .{
        .transition_id = intent.transition_id,
        .table_id = intent.table_id,
        .donor_group_id = intent.donor_group_id,
        .receiver_group_id = intent.receiver_group_id,
        .rollback_reason = try cloneOwnedOptional(alloc, intent.rollback_reason),
        .automatic = intent.automatic,
        .allow_doc_identity_reassignment = intent.allow_doc_identity_reassignment,
        .projected = intent.projected,
    };
}

fn freeMergeIntent(alloc: std.mem.Allocator, intent: MergeIntent) void {
    freeOwnedOptional(alloc, intent.rollback_reason);
}

fn cloneSplitTransitionRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) !transition_state.SplitTransitionRecord {
    const split_key = try cloneOwnedOptional(alloc, record.split_key);
    errdefer freeOwnedOptional(alloc, split_key);
    const source_range_end = try cloneOwnedOptional(alloc, record.source_range_end);
    errdefer freeOwnedOptional(alloc, source_range_end);
    const rollback_reason = try cloneOwnedOptional(alloc, record.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    return .{
        .transition_id = record.transition_id,
        .attempt_epoch = record.attempt_epoch,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
    };
}

pub fn freeSplitTransitionRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) void {
    freeOwnedOptional(alloc, record.split_key);
    freeOwnedOptional(alloc, record.source_range_end);
    freeOwnedOptional(alloc, record.rollback_reason);
}

fn cloneMergeTransitionRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) !transition_state.MergeTransitionRecord {
    const rollback_reason = try cloneOwnedOptional(alloc, record.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
    };
}

pub fn freeMergeTransitionRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) void {
    freeOwnedOptional(alloc, record.rollback_reason);
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

    const projected_splits = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 5001,
        .attempt_epoch = 2,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
    }};
    const projected_merges = [_]transition_state.MergeTransitionRecord{.{
        .transition_id = 6001,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
    }};
    try manager.syncProjectedSplitTransitions(&projected_splits);
    try manager.syncProjectedMergeTransitions(&projected_merges);

    const splits = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 2), splits[0].attempt_epoch);
    try std.testing.expectEqualStrings("doc:h", splits[0].split_key.?);

    const merges = try manager.listDesiredMergeTransitions(std.testing.allocator);
    defer manager.freeMergeTransitions(std.testing.allocator, merges);
    try std.testing.expectEqual(@as(usize, 1), merges.len);
    try std.testing.expect(merges[0].allow_doc_identity_reassignment);

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

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
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
    });
    try manager.applyFinalizedSplit(.{
        .transition_id = 5003,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 102,
        .phase = .finalized,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
    });

    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
    for (ranges) |range| {
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
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .range_id = 1002,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try manager.requestMerge(.{
        .transition_id = 6003,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
    });

    try manager.applyFinalizedMerge(.{
        .transition_id = 6003,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .phase = .finalized,
    });
    try manager.applyFinalizedMerge(.{
        .transition_id = 6003,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .phase = .finalized,
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
    try std.testing.expect(manager.merge_intents.count() == 0);
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

test "table manager parses placement classes and checks compatibility" {
    try std.testing.expectEqual(PlacementClass.serving, parsePlacementClass("serving").?);
    try std.testing.expectEqual(PlacementClass.bulk, parsePlacementClass("bulk").?);
    try std.testing.expect(parsePlacementClass("custom") == null);

    try std.testing.expect(placementRoleCompatible("serving", "serving"));
    try std.testing.expect(!placementRoleCompatible("serving", "bulk"));
    try std.testing.expect(placementRoleCompatible("custom", "custom"));
    try std.testing.expect(!placementRoleCompatible("custom", "archive"));
}
