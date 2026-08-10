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

//! Versioned internal ABI for the storage kernel's live DB owner. Keep this
//! module free of storage and distributed-runtime imports.

pub const abi_version: u32 = 20;

pub const BorrowedBytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: u64 = 0,

    pub fn fromSlice(value: []const u8) BorrowedBytes {
        return .{
            .ptr = if (value.len == 0) null else value.ptr,
            .len = @intCast(value.len),
        };
    }

    pub fn slice(self: BorrowedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

pub const OwnedBytes = extern struct {
    ptr: ?[*]u8 = null,
    len: u64 = 0,

    pub fn slice(self: OwnedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

pub const VersionedOwnedBytes = extern struct {
    buffer: OwnedBytes = .{},
    version: u64 = 0,
};

pub const TxnId = extern struct {
    bytes: [16]u8 = @splat(0),
};

pub const TxnStatus = enum(u32) {
    pending = 0,
    committed = 1,
    aborted = 2,
};

pub const Status = enum(u32) {
    ok = 0,
    invalid_abi = 1,
    invalid_argument = 2,
    not_found = 3,
    busy = 4,
    version_conflict = 5,
    intent_conflict = 6,
    transaction_not_found = 7,
    read_only = 8,
    out_of_memory = 9,
    corrupted = 10,
    identity_namespace_mismatch = 11,
    invalid_query = 12,
    unsupported_query = 13,
    index_not_found = 14,
    identity_read_generation_changed = 15,
    timeout = 16,
    cancelled = 17,
    restore_identity_mismatch = 18,
    invalid_backup = 19,
    backup_integrity = 20,
    unsupported_backup_migration = 21,
    restore_identity_namespace_mismatch = 22,
    internal = 255,
};

/// Process-scoped physical-storage owner. The handle owns shared caches and
/// admission state; individual table/group owners borrow it for their full
/// lifetime. A null context in `OpenRequest` retains the standalone ABI path
/// used by focused C API callers and compatibility tests.
pub const ContextRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
};

pub const ContextCacheKindStats = extern struct {
    hits: u64 = 0,
    misses: u64 = 0,
    inserts: u64 = 0,
    evictions: u64 = 0,
    invalidations: u64 = 0,
    waits: u64 = 0,
    used_bytes: u64 = 0,
};

/// Process-scoped physical cache metrics. These are copied across the ABI in
/// one snapshot so control-plane observability never imports the cache owner.
pub const ContextMetricsResult = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    lsm_cache_used_bytes: u64 = 0,
    lsm_cache_entry_count: u64 = 0,
    lsm_run_state: ContextCacheKindStats = .{},
    lsm_run_table_raw: ContextCacheKindStats = .{},
    lsm_run_table_index: ContextCacheKindStats = .{},
    lsm_run_table_block: ContextCacheKindStats = .{},
    lsm_run_table_physical_block: ContextCacheKindStats = .{},
};

/// Process-owned data-Raft apply/projection store. Requests are deliberately
/// batch-, snapshot-, and placement-sized; no record or backend primitive
/// crosses this ABI.
pub const DataApplyOpenRequest = extern struct {
    version: u32 = abi_version,
    no_sync: u8 = 0,
    read_only: u8 = 0,
    _reserved0: u16 = 0,
    context: ?*anyopaque = null,
    root_dir: BorrowedBytes = .{},
};

pub const DataApplyBatchRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
    commit_index: u64 = 0,
    entries: BorrowedBytes = .{},
};

pub const DataApplySnapshotRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
    commit_index: u64 = 0,
    snapshot: BorrowedBytes = .{},
};

pub const DataApplyGroupRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
};

pub const DataApplyGroupsRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_ids: ?[*]const u64 = null,
    group_count: u64 = 0,

    pub fn slice(self: DataApplyGroupsRequest) ?[]const u64 {
        if (self.group_count == 0) return &.{};
        const ptr = self.group_ids orelse return null;
        return ptr[0..@intCast(self.group_count)];
    }
};

pub const DataApplyLatestResult = extern struct {
    version: u32 = abi_version,
    present: u8 = 0,
    _reserved0: [3]u8 = .{ 0, 0, 0 },
    commit_index: u64 = 0,
    entry_count: u64 = 0,
    normal_entry_count: u64 = 0,
    admin_entry_count: u64 = 0,
    last_entry_term: u64 = 0,
    last_entry_index: u64 = 0,
};

pub const TransactionRecoveryResolveFn = *const fn (
    ?*anyopaque,
    *const TxnId,
    BorrowedBytes,
    TxnStatus,
    u64,
) callconv(.c) Status;
pub const TransactionRecoveryOwnsFn = *const fn (?*anyopaque, BorrowedBytes) callconv(.c) u8;
pub const TransactionRecoveryAcknowledgeFn = *const fn (
    ?*anyopaque,
    *const TxnId,
    BorrowedBytes,
    BorrowedBytes,
) callconv(.c) Status;
pub const TransactionRecoveryCleanupFn = *const fn (
    ?*anyopaque,
    *const TxnId,
    BorrowedBytes,
    u64,
    u64,
) callconv(.c) Status;

/// Distributed control callbacks retained by a live owner for transaction
/// recovery. Callback byte slices are borrowed only for each synchronous call.
pub const TransactionRecoveryConfig = extern struct {
    enabled: u8 = 0,
    lease_owned: u8 = 0,
    replicated_metadata: u8 = 0,
    _reserved0: u8 = 0,
    _reserved1: u32 = 0,
    interval_ms: u64 = 30_000,
    cutoff_ns: u64 = 5 * 60 * 1_000_000_000,
    callback_ctx: ?*anyopaque = null,
    owner_id: BorrowedBytes = .{},
    resolve_participant_fn: ?TransactionRecoveryResolveFn = null,
    owns_recovery_fn: ?TransactionRecoveryOwnsFn = null,
    acknowledge_participant_fn: ?TransactionRecoveryAcknowledgeFn = null,
    cleanup_transaction_fn: ?TransactionRecoveryCleanupFn = null,
};

pub const OpenRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    context: ?*anyopaque = null,
    path: BorrowedBytes = .{},
    table_name: BorrowedBytes = .{},
    group_id: u64 = 0,
    lsm_root_generation: u64 = 0,
    has_identity_namespace: u8 = 0,
    _reserved1: [7]u8 = @splat(0),
    identity_table_id: u64 = 0,
    identity_shard_id: u64 = 0,
    identity_range_id: u64 = 0,
    schema_json: BorrowedBytes = .{},
    indexes_json: BorrowedBytes = .{},
    transaction_recovery: TransactionRecoveryConfig = .{},
};

pub const JsonOperationRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
};

/// Replace the catalog-owned physical schema/index contract on one resident
/// group owner. Both byte slices are borrowed for this synchronous call.
pub const ConfigureRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    schema_json: BorrowedBytes = .{},
    indexes_json: BorrowedBytes = .{},
};

pub const ReconcileState = enum(u32) {
    complete = 0,
    repair_pending = 1,
    busy = 2,
    degraded = 3,
};

/// Advance one bounded schema/index desired-state quantum. The target index is
/// optional; an empty slice reconciles the complete table contract.
pub const ReconcileRequest = extern struct {
    version: u32 = abi_version,
    advance_index_repair: u8 = 0,
    _reserved0: [3]u8 = .{ 0, 0, 0 },
    table_name: BorrowedBytes = .{},
    schema_json: BorrowedBytes = .{},
    indexes_json: BorrowedBytes = .{},
    target_index_name: BorrowedBytes = .{},
};

pub const ReconcileResult = extern struct {
    version: u32 = abi_version,
    state: ReconcileState = .complete,
    indexes_added: u64 = 0,
    indexes_removed: u64 = 0,
    indexes_pending: u64 = 0,
    repair_discovered: u64 = 0,
    repair_attempted: u64 = 0,
    repair_repaired: u64 = 0,
    repair_remaining: u64 = 0,
    repair_terminal: u64 = 0,
    repair_busy: u64 = 0,
    repair_disk_waits: u64 = 0,
    next_retry_at_ms: u64 = 0,
};

pub const TableRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
};

/// Whole-DB maintenance operations. Each invocation crosses the compiled
/// boundary once and performs at most one bounded storage quantum.
pub const MaintenanceAction = enum(u32) {
    inspect = 0,
    inspect_best_effort = 1,
    lsm_step = 2,
    lsm_step_best_effort = 3,
    dense_posting_idle = 4,
};

pub const MaintenanceRequest = extern struct {
    version: u32 = abi_version,
    action: u32 = @intFromEnum(MaintenanceAction.inspect),
    table_name: BorrowedBytes = .{},
};

pub const MaintenanceResult = extern struct {
    version: u32 = abi_version,
    progressed: u8 = 0,
    has_next_wake_delay: u8 = 0,
    _reserved0: [2]u8 = @splat(0),
    dense_steps: u64 = 0,
    maintenance_score: u64 = 0,
    next_wake_delay_ns: u64 = 0,
};

pub const TransactionStatusRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    txn_id: TxnId = .{},
};

pub const TransactionStatusResult = extern struct {
    version: u32 = abi_version,
    status: TxnStatus = .pending,
};

pub const BulkProgressPhase = enum(u32) {
    begin = 0,
    split = 1,
    publish = 2,
    complete = 3,
};

pub const BulkProgress = extern struct {
    phase: BulkProgressPhase,
    _reserved0: u32 = 0,
    publish_window: u64 = 0,
    split_steps: u64 = 0,
    deferred_leaf_splits: u64 = 0,
    elapsed_ns: u64 = 0,
};

pub const BulkProgressFn = *const fn (?*anyopaque, *const BulkProgress) callconv(.c) void;
pub const BulkAdmissionFn = *const fn (?*anyopaque) callconv(.c) Status;

pub const BulkFinishRequest = extern struct {
    version: u32 = abi_version,
    compact: u8 = 1,
    flush: u8 = 0,
    has_max_deferred_l0_runs: u8 = 0,
    has_max_foreground_compaction_input_bytes: u8 = 0,
    has_max_foreground_compaction_ns: u8 = 0,
    has_max_deferred_hbc_leaf_splits_per_publish: u8 = 0,
    has_max_deferred_hbc_leaf_split_members_per_publish: u8 = 0,
    has_bulk_rebuild_hbc_leaf_min_members: u8 = 0,
    _reserved0: [4]u8 = .{ 0, 0, 0, 0 },
    table_name: BorrowedBytes = .{},
    max_deferred_l0_runs: u64 = 0,
    max_foreground_compaction_steps: u64 = 0,
    max_foreground_compaction_input_bytes: u64 = 0,
    max_foreground_compaction_ns: u64 = 0,
    max_deferred_hbc_leaf_splits_per_publish: u64 = 0,
    max_deferred_hbc_leaf_split_members_per_publish: u64 = 0,
    bulk_rebuild_hbc_leaf_min_members: u64 = 0,
    callback_ctx: ?*anyopaque = null,
    progress_fn: ?BulkProgressFn = null,
    admission_fn: ?BulkAdmissionFn = null,
};

pub const SyncLevel = enum(u32) {
    propose = 0,
    write = 1,
    full_text = 2,
    enrichments = 3,
    full_index = 4,
};

pub const SyncRequest = extern struct {
    version: u32 = abi_version,
    sync_level: u32 = @intFromEnum(SyncLevel.write),
    table_name: BorrowedBytes = .{},
};

/// Borrowed logical HA record. The scalar values intentionally mirror the
/// versioned HA envelope without exposing a storage-owned Zig enum or layout
/// across the compiled boundary.
pub const HAReplicationRecordRequest = extern struct {
    version: u32 = abi_version,
    flags: u32 = 0,
    table_name: BorrowedBytes = .{},
    record_kind: u16 = 0,
    payload_codec: u16 = 0,
    _reserved0: u32 = 0,
    cluster_id: u64 = 0,
    shard_id: u64 = 0,
    table_id: u64 = 0,
    timeline_id: u64 = 0,
    epoch: u64 = 0,
    lsn: u64 = 0,
    previous_lsn: u64 = 0,
    commit_timestamp_ns: i64 = 0,
    payload: BorrowedBytes = .{},
};

pub const BackupFormat = enum(u32) {
    native = 0,
    portable = 1,
};

/// Materialize one complete local shard backup through the resident owner.
/// The caller retains reservation, remote transport, and manifest publication;
/// all byte slices are borrowed only for this synchronous operation.
pub const BackupRequest = extern struct {
    version: u32 = abi_version,
    format: u32 = @intFromEnum(BackupFormat.native),
    table_name: BorrowedBytes = .{},
    backup_root: BorrowedBytes = .{},
    backup_id: BorrowedBytes = .{},
};

pub const SnapshotPrepareRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    path: BorrowedBytes = .{},
    table_name: BorrowedBytes = .{},
    group_id: u64 = 0,
    lsm_root_generation: u64 = 0,
    identity_table_id: u64 = 0,
    identity_shard_id: u64 = 0,
    identity_range_id: u64 = 0,
    schema_json: BorrowedBytes = .{},
    indexes_json: BorrowedBytes = .{},
    encoded_snapshot: BorrowedBytes = .{},
};

/// Coarse local backup-restore request. The manifest is a complete JSON value
/// and every byte slice is borrowed for the synchronous prepare/reconcile call.
pub const RestorePrepareRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    path: BorrowedBytes = .{},
    table_name: BorrowedBytes = .{},
    group_id: u64 = 0,
    lsm_root_generation: u64 = 0,
    has_identity_namespace: u8 = 0,
    _reserved1: [7]u8 = @splat(0),
    identity_table_id: u64 = 0,
    identity_shard_id: u64 = 0,
    identity_range_id: u64 = 0,
    backup_root: BorrowedBytes = .{},
    backup_id: BorrowedBytes = .{},
    artifact_backup_id: BorrowedBytes = .{},
    source_identity: BorrowedBytes = .{},
    snapshot_path: BorrowedBytes = .{},
    expected_artifact_size_bytes: u64 = 0,
    expected_artifact_sha256: BorrowedBytes = .{},
    manifest_json: BorrowedBytes = .{},
};

pub const RestorePrepareState = enum(u32) {
    prepared = 0,
    already_imported = 1,
};

pub const RestorePrepareResult = extern struct {
    version: u32 = abi_version,
    state: RestorePrepareState = .prepared,
    snapshot: ?*anyopaque = null,
};

pub const SnapshotPublishResult = extern struct {
    durability_uncertain: u8 = 0,
    _reserved0: [7]u8 = @splat(0),
};

/// JSON operation with synchronous local execution controls. The cancellation
/// pointer refers to the caller's atomic boolean for the duration of the call;
/// it is never retained by the storage owner.
pub const ControlledJsonOperationRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
    execution_deadline_ns: u64 = 0,
    has_execution_deadline: u8 = 0,
    _reserved1: [7]u8 = @splat(0),
    cancellation_flag: ?*const anyopaque = null,
};

pub const ArtifactOperation = enum(u32) {
    corrupt_embedding = 0,
    reprocess_document = 1,
    reprocess_document_range = 2,
    list_repair_issues = 3,
    repair_issues = 4,
    update_child_range_placement = 5,
    apply_child_range_batch = 6,
};

pub const CancellationCheckFn = *const fn (?*anyopaque) callconv(.c) u8;

/// One complete artifact mutation, reprocessing, or repair operation. The JSON
/// payload is the same coarse request shape used by the internal HTTP routes.
/// Cancellation is borrowed for the synchronous call and is never retained.
pub const ArtifactOperationRequest = extern struct {
    version: u32 = abi_version,
    operation: u32 = @intFromEnum(ArtifactOperation.reprocess_document),
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
    cancellation_ctx: ?*anyopaque = null,
    cancellation_fn: ?CancellationCheckFn = null,
    defer_durable_index_repair_execution: u8 = 0,
    _reserved0: [7]u8 = @splat(0),
};

pub extern fn antfly_storage_context_create(
    request: *const ContextRequest,
    out_context: *?*anyopaque,
) callconv(.c) Status;

/// Returns `busy` while any owner still borrows the context. Null destruction
/// is idempotent and succeeds.
pub extern fn antfly_storage_context_destroy(context: ?*anyopaque) callconv(.c) Status;

pub extern fn antfly_storage_context_metrics(
    context: ?*anyopaque,
    out_result: *ContextMetricsResult,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_open(
    request: *const DataApplyOpenRequest,
    out_store: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_close(store: ?*anyopaque) callconv(.c) void;

pub extern fn antfly_data_apply_store_apply_batch(
    store: ?*anyopaque,
    request: *const DataApplyBatchRequest,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_build_snapshot(
    store: ?*anyopaque,
    request: *const DataApplyGroupRequest,
    out_snapshot: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_install_snapshot(
    store: ?*anyopaque,
    request: *const DataApplySnapshotRequest,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_latest(
    store: ?*anyopaque,
    request: *const DataApplyGroupRequest,
    out_result: *DataApplyLatestResult,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_retain_groups(
    store: ?*anyopaque,
    request: *const DataApplyGroupsRequest,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_begin_group_transition(
    store: ?*anyopaque,
    request: *const DataApplyGroupsRequest,
    out_transition: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_commit_group_transition(
    transition: ?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_abort_group_transition(
    transition: ?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_destroy_group_transition(
    transition: ?*anyopaque,
) callconv(.c) void;

pub extern fn antfly_storage_owner_open(
    request: *const OpenRequest,
    out_owner: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_close(owner: ?*anyopaque) callconv(.c) void;

pub extern fn antfly_storage_owner_configure(
    owner: ?*anyopaque,
    request: *const ConfigureRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_reconcile(
    owner: ?*anyopaque,
    request: *const ReconcileRequest,
    out_result: *ReconcileResult,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_bulk_begin(
    owner: ?*anyopaque,
    request: *const TableRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_bulk_finish(
    owner: ?*anyopaque,
    request: *const BulkFinishRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_bulk_abort(
    owner: ?*anyopaque,
    request: *const TableRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_batch_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_replicated_batch_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_transaction_status(
    owner: ?*anyopaque,
    request: *const TransactionStatusRequest,
    out_result: *TransactionStatusResult,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_wait_for_sync(
    owner: ?*anyopaque,
    request: *const SyncRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_apply_ha_replication_record(
    owner: ?*anyopaque,
    request: *const HAReplicationRecordRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_backup_json(
    owner: ?*anyopaque,
    request: *const BackupRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_snapshot_prepare(
    request: *const SnapshotPrepareRequest,
    out_snapshot: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_storage_restore_prepare(
    request: *const RestorePrepareRequest,
    out_result: *RestorePrepareResult,
) callconv(.c) Status;

pub extern fn antfly_storage_restore_reconcile(
    request: *const RestorePrepareRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_restore_repair(
    owner: ?*anyopaque,
    request: *const RestorePrepareRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_snapshot_promote(snapshot: ?*anyopaque) callconv(.c) Status;

pub extern fn antfly_storage_snapshot_publish_prepared(
    snapshot: ?*anyopaque,
    out_result: *SnapshotPublishResult,
) callconv(.c) Status;

pub extern fn antfly_storage_snapshot_commit(snapshot: ?*anyopaque) callconv(.c) Status;

pub extern fn antfly_storage_snapshot_rollback(snapshot: ?*anyopaque) callconv(.c) Status;

pub extern fn antfly_storage_snapshot_destroy(snapshot: ?*anyopaque) callconv(.c) void;

pub extern fn antfly_storage_owner_query_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_lookup_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *VersionedOwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_scan_ndjson(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_preflight_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_text_stats_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_algebraic_partials_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_graph_expand_json(
    owner: ?*anyopaque,
    request: *const ControlledJsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_graph_hydrate_json(
    owner: ?*anyopaque,
    request: *const ControlledJsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_graph_edges_json(
    owner: ?*anyopaque,
    request: *const ControlledJsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_document_artifact_manifest_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_document_artifact_manifests_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_artifact_operation_json(
    owner: ?*anyopaque,
    request: *const ArtifactOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_runtime_status_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_maintenance(
    owner: ?*anyopaque,
    request: *const MaintenanceRequest,
    out_result: *MaintenanceResult,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_buffer_destroy(
    buffer: *OwnedBytes,
) callconv(.c) void;
