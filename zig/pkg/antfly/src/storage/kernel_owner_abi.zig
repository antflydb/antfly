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

const failure_abi = @import("runtime_failure_abi");

pub const abi_version = failure_abi.abi_version;
pub const Status = failure_abi.Status;
pub const FailureBoundary = failure_abi.FailureBoundary;
pub const FailureIdentity = failure_abi.FailureIdentity;
pub const failure_error_name_capacity = failure_abi.failure_error_name_capacity;

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

/// Provider-owned encoded query response plus the exact storage snapshot
/// generation used to produce it. The generation is transport metadata, not
/// part of the public JSON representation.
pub const QueryOwnedResponse = extern struct {
    buffer: OwnedBytes = .{},
    identity_read_generation: u64 = 0,
    has_identity_read_generation: u8 = 0,
    _reserved0: [7]u8 = @splat(0),
};

/// The WAL boundary owns the physical ordered store in the storage archive.
/// Control units retain only this opaque handle and cross once per logical
/// durable log operation; backend transactions and LSM records never cross.
pub const WalOpenRequest = extern struct {
    version: u32 = abi_version,
    commit_backend: u32 = 3, // adaptive
    path: BorrowedBytes = .{},
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: u64 = 64,
    no_sync: u8 = 0,
    read_only: u8 = 0,
    model_commit_backend_completions: u8 = 0,
    _reserved0: [5]u8 = @splat(0),
};

pub const WalOpenResult = extern struct {
    handle: ?*anyopaque = null,
    next_lsn: u64 = 1,
};

pub const WalAppendRequest = extern struct {
    version: u32 = abi_version,
    reposition_if_empty: u8 = 0,
    _reserved0: [3]u8 = @splat(0),
    expected_next_lsn: u64 = 1,
    data: BorrowedBytes = .{},
};

pub const WalAppendResult = extern struct {
    assigned_lsn: u64 = 0,
    next_lsn: u64 = 1,
};

pub const WalPositionRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    lsn: u64 = 0,
};

pub const WalScanRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    from_lsn: u64 = 0,
    max_entries: u64 = 0,
    max_bytes: u64 = 0,
};

pub const WalScanResult = extern struct {
    entries: OwnedBytes = .{},
    next_lsn: u64 = 0,
    done: u8 = 1,
    _reserved0: [7]u8 = @splat(0),
};

pub const WalSyncRequest = extern struct {
    version: u32 = abi_version,
    force: u8 = 0,
    _reserved0: [3]u8 = @splat(0),
};

pub const WalReadResult = extern struct {
    buffer: OwnedBytes = .{},
    found: u8 = 0,
    _reserved0: [7]u8 = @splat(0),
};

/// Exact provider-side WAL counters. Keep this field-for-field compatible
/// with `storage/wal.zig:WalStats`; diagnostics must not become synthetic or
/// lose their meaning merely because the WAL moved behind this ABI.
pub const WalStats = extern struct {
    append_calls: u64 = 0,
    append_batch_calls: u64 = 0,
    logical_entries: u64 = 0,
    physical_commits: u64 = 0,
    grouped_commits: u64 = 0,
    grouped_requests: u64 = 0,
    max_requests_per_commit: u64 = 0,
    max_entries_per_commit: u64 = 0,
    total_wait_ns: u64 = 0,
    total_coalesce_ns: u64 = 0,
    total_txn_open_ns: u64 = 0,
    total_put_ns: u64 = 0,
    total_commit_ns: u64 = 0,
    inner_segment_syncs: u64 = 0,
    inner_index_syncs: u64 = 0,
    post_commit_segment_syncs: u64 = 0,
    post_commit_index_syncs: u64 = 0,
};

pub const TxnId = extern struct {
    bytes: [16]u8 = @splat(0),
};

pub const TxnStatus = enum(u32) {
    pending = 0,
    committed = 1,
    aborted = 2,
};

pub const EnrichmentStreamBeginFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
    BorrowedBytes,
    BorrowedBytes,
) callconv(.c) Status;

/// One coarse media/document extraction call. Unit delivery is batched JSON
/// because extraction metadata is naturally textual and heterogeneous; the
/// callback is never invoked once per store operation or index mutation.
pub const EnrichmentExtractRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    downloaded: BorrowedBytes = .{},
    downloaded_content_type: BorrowedBytes = .{},
    source_url: BorrowedBytes = .{},
    config_json: BorrowedBytes = .{},
    raw_document_json: BorrowedBytes = .{},
    callback_ctx: ?*anyopaque = null,
    on_begin: ?EnrichmentStreamBeginFn = null,
    on_units_json: ?*const fn (?*anyopaque, BorrowedBytes) callconv(.c) Status = null,
    max_decoded_stream_bytes: u64 = 64 * 1024 * 1024,
    max_working_set_bytes: u64 = 96 * 1024 * 1024,
};

pub const EnrichmentRenderPdfRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    pdf_bytes: BorrowedBytes = .{},
    page_number: u64 = 1,
    dpi: u16 = 150,
    _reserved1: u16 = 0,
    max_dimension: u32 = 4096,
    max_pixels: u64 = 40_000_000,
    max_decoded_stream_bytes: u64 = 64 * 1024 * 1024,
    max_working_set_bytes: u64 = 96 * 1024 * 1024,
};

pub const EnrichmentRenderedPdfPage = extern struct {
    requested_dpi: u16 = 0,
    effective_dpi: u16 = 0,
    width: u32 = 0,
    height: u32 = 0,
    _reserved0: u32 = 0,
};

/// Stable stages for failures originating in the enrichment compute unit.
pub const EnrichmentOperation = enum(u32) {
    extract_stream = 1,
    render_pdf_page = 2,
    validate_extract_response = 3,
    validate_render_response = 4,
};

/// Selects the existing query wire dialect. Distributed/local-owner calls use
/// the resolved internal representation; public C API and Lite calls retain
/// public validation and translation inside the query provider.
pub const LocalQueryDialect = enum(u32) {
    internal = 0,
    public = 1,
};

/// Complete operation families owned by the compiled local-query provider. New
/// values are append-only. The storage owner remains responsible for handle
/// lifetime and admission; parsing, physical execution, and response encoding
/// stay together on the provider side.
pub const LocalQueryKind = enum(u32) {
    search = 0,
    graph_expand = 1,
    graph_hydrate = 2,
    graph_edges = 3,
    text_stats = 4,
    algebraic_partials = 5,
    preflight = 6,
};

/// Stable diagnostic identities for the stages of one local-query call. These
/// values are ABI data: append new operations, never renumber existing ones.
/// Control flow continues to use `FailureIdentity.status`; `operation` tells
/// operators which side and stage produced that exact status.
pub const LocalQueryOperation = enum(u32) {
    validate_request = 1,
    parse_internal_request = 2,
    parse_public_request = 3,
    execute_internal_query = 4,
    execute_public_query = 5,
    encode_internal_response = 6,
    encode_public_response = 7,
    validate_provider_response = 8,
    text_stats = 9,
    algebraic_partials = 10,
    parse_graph_expand = 11,
    execute_graph_expand = 12,
    encode_graph_expand = 13,
    parse_graph_hydrate = 14,
    execute_graph_hydrate = 15,
    encode_graph_hydrate = 16,
    parse_graph_edges = 17,
    execute_graph_edges = 18,
    encode_graph_edges = 19,
    parse_aggregation = 20,
    execute_aggregation = 21,
    encode_aggregation = 22,
    preflight = 23,
};

pub const LocalQueryReturnMode = enum(u32) {
    parent = 0,
    chunk = 1,
    parent_with_chunks = 2,
    unit = 3,
    unit_with_chunks = 4,
    /// Return each indexed source member without hierarchy grouping.
    /// Appended to preserve the established numeric ABI.
    member = 5,
};

/// Scalar execution details that are not losslessly represented by the
/// existing public/distributed JSON envelope. They are applied only when
/// `enabled` is set, after parsing and before DB execution.
pub const LocalQueryExecutionOptions = extern struct {
    enabled: u8 = 0,
    include_stored: u8 = 1,
    _reserved0: [2]u8 = @splat(0),
    return_mode: LocalQueryReturnMode = .parent,
    max_chunks_per_parent: u32 = 0,
    dense_k: u32 = 0,
    sparse_k: u32 = 0,
};

/// One complete local query against a DB owned by the storage archive. The DB
/// and cancellation token are opaque borrowed handles valid only for this
/// synchronous call. No posting, candidate, stored document, or index handle
/// crosses the compiled boundary.
pub const LocalQueryRequest = extern struct {
    version: u32 = abi_version,
    dialect: LocalQueryDialect = .internal,
    kind: LocalQueryKind = .search,
    has_execution_deadline: u8 = 0,
    _reserved0: [3]u8 = @splat(0),
    db: ?*anyopaque = null,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
    execution_options: LocalQueryExecutionOptions = .{},
    cancellation_ctx: ?*anyopaque = null,
    cancellation_fn: ?CancellationCheckFn = null,
    execution_deadline_ns: u64 = 0,
};

/// Process-scoped physical-storage owner. The handle owns shared caches and
/// admission state; individual table/group owners borrow it for their full
/// lifetime. A null context in `OpenRequest` retains the standalone ABI path
/// used by focused C API callers and compatibility tests.
pub const ContextStorageKind = enum(u32) {
    directory = 0,
    lite = 1,
};

pub const ContextRequest = extern struct {
    version: u32 = abi_version,
    storage_kind: ContextStorageKind = .directory,
    no_sync: u8 = 0,
    _reserved0: [7]u8 = @splat(0),
    storage_path: BorrowedBytes = .{},
    auth_storage_path: BorrowedBytes = .{},
};

/// One low-volume engine namespace used by control-plane metadata or durable
/// API sessions. User documents never cross this transaction ABI.
pub const SystemStoreOpenRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    namespace: BorrowedBytes = .{},
};

pub const SystemEntryResult = extern struct {
    key: BorrowedBytes = .{},
    value: BorrowedBytes = .{},
    present: u8 = 0,
    _reserved0: [7]u8 = @splat(0),
};

pub const SystemCursorSeek = enum(u32) {
    first = 0,
    last = 1,
    next = 2,
    previous = 3,
    at_or_after = 4,
    at_or_before = 5,
};

pub const LiteAdoptionRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    namespace: BorrowedBytes = .{},
    identity_table_id: u64 = 0,
    identity_shard_id: u64 = 0,
    identity_range_id: u64 = 0,
};

pub const LiteAdoptionProbeResult = extern struct {
    version: u32 = abi_version,
    is_embedded_artifact: u8 = 0,
    embedded_root_has_user_documents: u8 = 0,
    _reserved0: [2]u8 = @splat(0),
};

pub const ContextMaintenanceOperation = enum(u32) {
    check = 0,
    compact = 1,
    vacuum = 2,
};

pub const ContextMaintenanceStatus = extern struct {
    version: u32 = abi_version,
    check: u8 = 0,
    compact: u8 = 0,
    vacuum: u8 = 0,
    online: u8 = 0,
    asynchronous: u8 = 1,
    has_fsync: u8 = 0,
    fsync: u8 = 0,
    _reserved0: u8 = 0,
    engine: BorrowedBytes = .{},
    format: BorrowedBytes = .{},
};

pub const ContextMaintenanceRequest = extern struct {
    version: u32 = abi_version,
    operation: ContextMaintenanceOperation = .check,
    cancel_token: ?*const anyopaque = null,
};

pub const ContextMaintenanceResult = extern struct {
    version: u32 = abi_version,
    has_valid: u8 = 0,
    valid: u8 = 0,
    _reserved0: [2]u8 = @splat(0),
    /// Static machine-readable issue identifier owned by the kernel.
    issue: BorrowedBytes = .{},
    file_size: u64 = 0,
    valid_prefix_size: u64 = 0,
    reclaimable_bytes: u64 = 0,
    before_size: u64 = 0,
    after_size: u64 = 0,
    reclaimed_bytes: u64 = 0,
    live_file_count: u64 = 0,
    live_bytes: u64 = 0,
    present_mask: u16 = 0,
    _reserved1: [6]u8 = @splat(0),
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

/// Process-owned metadata-Raft apply/projection store. Projection requests are
/// complete control-plane reads (one list/get/stats operation), never backend
/// cursor or record-level calls.
pub const MetadataApplyOpenRequest = extern struct {
    version: u32 = abi_version,
    no_sync: u8 = 0,
    read_only: u8 = 0,
    _reserved0: u16 = 0,
    context: ?*anyopaque = null,
    root_dir: BorrowedBytes = .{},
};

pub const MetadataApplyBatchRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
    commit_index: u64 = 0,
    entries: BorrowedBytes = .{},
};

pub const MetadataApplyGroupRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
};

pub const MetadataApplySnapshotRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
    commit_index: u64 = 0,
    snapshot: BorrowedBytes = .{},
};

pub const MetadataApplyPrepareSnapshotRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
    applied_index: u64 = 0,
};

pub const MetadataProjectionKind = enum(u32) {
    latest_batch = 0,
    metadata_incarnation = 1,
    split_transitions = 2,
    placement_intents = 3,
    local_placement_intents = 4,
    nodes = 5,
    stores = 6,
    merge_transitions = 7,
    tables = 8,
    table = 9,
    table_transition_fence = 10,
    schema_progress = 11,
    restore_progress = 12,
    replication_source_statuses = 13,
    replication_source_status = 14,
    extension_packages = 15,
    installed_extensions = 16,
    extension_members = 17,
    extension_dependencies = 18,
    shuffle_join_leases = 19,
    ranges = 20,
    reconcile_lease = 21,
    reallocation_request = 22,
    shuffle_join_lease = 23,
    restore_job_rows = 24,
    restore_job_value = 25,
    maintenance_stats = 26,
    runtime_status_protocol_activation_version = 27,
    placement_version_fences = 28,
    /// Tables and ranges captured from one committed storage snapshot.
    catalog_projection = 29,
};

pub const MetadataProjectionRequest = extern struct {
    version: u32 = abi_version,
    kind: MetadataProjectionKind = .latest_batch,
    group_id: u64 = 0,
    arg0: u64 = 0,
    arg1: u64 = 0,
    key: BorrowedBytes = .{},
};

pub const MetadataProjectionSignalKind = enum(u32) {
    table = 0,
    range = 1,
    store = 2,
    placement_intent = 3,
    reconcile_lease = 4,
    shuffle_join_lease = 5,
    split_transition = 6,
    merge_transition = 7,
    schema_progress = 8,
    restore_progress = 9,
    restore_job = 10,
    replication_source_status = 11,
};

pub const MetadataProjectionSignal = extern struct {
    kind: MetadataProjectionSignalKind = .table,
    _reserved0: u32 = 0,
    metadata_group_id: u64 = 0,
    table_name: BorrowedBytes = .{},
    table_id: u64 = 0,
    group_id: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,
};

pub const MetadataProjectionSignalFn = *const fn (
    context: ?*anyopaque,
    signal: *const MetadataProjectionSignal,
) callconv(.c) void;

pub const MetadataCommittedKeySignalFn = *const fn (
    context: ?*anyopaque,
    metadata_group_id: u64,
    key: BorrowedBytes,
) callconv(.c) void;

pub const MetadataListenerRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    context: ?*anyopaque = null,
    projection_fn: ?MetadataProjectionSignalFn = null,
    committed_key_fn: ?MetadataCommittedKeySignalFn = null,
};

/// One complete local replica-root provisioning round. The JSON envelope owns
/// the hosted group IDs plus projected table/range records; all physical DB,
/// restore, schema, index, enrichment, and resolver work remains in the kernel.
pub const MetadataReplicaRootReconcileRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    context: ?*anyopaque = null,
    replica_root_dir: BorrowedBytes = .{},
    metadata_group_id: u64 = 0,
    request_json: BorrowedBytes = .{},
};

pub const MetadataProvisionSummary = extern struct {
    groups_considered: u64 = 0,
    dbs_opened: u64 = 0,
    indexes_added: u64 = 0,
    indexes_removed: u64 = 0,
    indexes_pending: u64 = 0,
    enrichments_added: u64 = 0,
    enrichments_updated: u64 = 0,
    enrichments_removed: u64 = 0,
    resolvers_added: u64 = 0,
    resolvers_updated: u64 = 0,
    resolvers_removed: u64 = 0,
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

pub const DataApplyPrepareSnapshotRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    group_id: u64 = 0,
    applied_index: u64 = 0,
};

pub const DataApplyPreparedSnapshotResult = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    path: OwnedBytes = .{},
    size: u64 = 0,
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

pub const DataApplyProjectionKind = enum(u32) {
    observe_split_control = 0,
    current_range = 1,
    group_state_page = 2,
    split_deltas_page = 3,
    capture_verified_handoff_metadata = 4,
};

/// One bounded projection read. Fields unused by `kind` must remain zero.
/// Variable-sized results use the versioned binary projection-wire contract.
pub const DataApplyProjectionRequest = extern struct {
    version: u32 = abi_version,
    kind: DataApplyProjectionKind = .observe_split_control,
    group_id: u64 = 0,
    after_sequence: u64 = 0,
    through_sequence: u64 = 0,
    max_entries: u64 = 0,
    max_bytes: u64 = 0,
    expected: DataApplyLatestResult = .{},
    root_incarnation_le: [16]u8 = @splat(0),
    range_start: BorrowedBytes = .{},
    range_end: BorrowedBytes = .{},
    after_key: BorrowedBytes = .{},
};

pub const DataApplyReconcileState = enum(u32) {
    advanced = 0,
    reconciled = 1,
    handoff = 2,
};

/// Reconcile one authoritative resident table owner into its data-Raft
/// projection. Both physical owners stay inside the compiled kernel.
pub const DataApplyReconcileRequest = extern struct {
    version: u32 = abi_version,
    capture_handoff: u8 = 0,
    _reserved0: [3]u8 = @splat(0),
    group_id: u64 = 0,
    max_page_entries: u64 = 0,
    max_page_bytes: u64 = 0,
    expected: DataApplyLatestResult = .{},
};

pub const DataApplyReconcileResult = extern struct {
    version: u32 = abi_version,
    state: DataApplyReconcileState = .advanced,
    handoff_metadata: OwnedBytes = .{},
};

/// One complete local range-transition phase. The distributed runtime owns
/// admission, ordering, and metadata decisions; the compiled storage kernel
/// owns every physical DB and data-Raft projection operation for the phase.
pub const LocalTransitionAction = enum(u32) {
    observe_split = 0,
    prepare_split_source = 1,
    start_split_source = 2,
    bootstrap_split_destination = 3,
    catch_up_split_destination = 4,
    finalize_split_source = 5,
    rollback_split = 6,
    observe_merge = 7,
    accept_merge_receiver = 8,
    catch_up_merge_receiver = 9,
    finalize_merge = 10,
    rollback_merge = 11,
};

pub const LocalTransitionPhase = enum(u32) {
    prepare = 0,
    bootstrap_peer = 1,
    replay_deltas = 2,
    cutover_ready = 3,
    finalized = 4,
    rolling_back = 5,
    rolled_back = 6,
};

pub const LocalTransitionResultKind = enum(u32) {
    none = 0,
    split = 1,
    merge = 2,
};

/// Complete immutable table and identity contract for one synchronous phase.
/// `primary_owner` is the split source or merge donor; `secondary_owner` is
/// the split destination or merge receiver.
pub const LocalTransitionRequest = extern struct {
    version: u32 = abi_version,
    action: LocalTransitionAction = .observe_split,
    transition_id: u64 = 0,
    attempt_epoch: u64 = 0,
    primary_group_id: u64 = 0,
    secondary_group_id: u64 = 0,
    table_id: u64 = 0,
    source_identity_shard_id: u64 = 0,
    source_identity_range_id: u64 = 0,
    target_identity_shard_id: u64 = 0,
    target_identity_range_id: u64 = 0,
    allow_doc_identity_reassignment: u8 = 0,
    has_source_range_end: u8 = 0,
    _reserved0: [6]u8 = @splat(0),
    table_name: BorrowedBytes = .{},
    schema_json: BorrowedBytes = .{},
    indexes_json: BorrowedBytes = .{},
    split_key: BorrowedBytes = .{},
    source_range_end: BorrowedBytes = .{},
};

/// Scalar transition observation. Variable-size state remains kernel-owned;
/// the control plane receives only the facts needed by its state machine.
pub const LocalTransitionResult = extern struct {
    version: u32 = abi_version,
    kind: LocalTransitionResultKind = .none,
    phase: LocalTransitionPhase = .prepare,
    has_source_split_phase: u8 = 0,
    source_split_phase: u8 = 0,
    bootstrapped: u8 = 0,
    replay_required: u8 = 0,
    replay_caught_up: u8 = 0,
    cutover_ready: u8 = 0,
    peer_ready_for_reads: u8 = 0,
    receiver_accepts_donor_range: u8 = 0,
    allow_doc_identity_reassignment: u8 = 0,
    _reserved0: [6]u8 = @splat(0),
    primary_group_id: u64 = 0,
    secondary_group_id: u64 = 0,
    primary_delta_sequence: u64 = 0,
    secondary_delta_sequence: u64 = 0,
    receiver_identity_table_id: u64 = 0,
    receiver_identity_shard_id: u64 = 0,
    receiver_identity_range_id: u64 = 0,
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

/// One candidate entity borrowed for a synchronous resolution callback.
/// The consumer must copy either byte slice before returning if it retains it.
pub const ResolutionCandidateConsumeFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
    BorrowedBytes,
) callconv(.c) Status;
pub const ResolutionCandidateGetFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
    BorrowedBytes,
    ?*anyopaque,
    ResolutionCandidateConsumeFn,
) callconv(.c) Status;
pub const ResolutionCandidateScanPrefixFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
    BorrowedBytes,
    u64,
    ?*anyopaque,
    ResolutionCandidateConsumeFn,
) callconv(.c) Status;
pub const ResolutionCandidateNearestFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
    BorrowedBytes,
    ?[*]const f32,
    u64,
    u64,
    ?*anyopaque,
    ResolutionCandidateConsumeFn,
) callconv(.c) Status;

pub const ResolutionCandidateConfig = extern struct {
    callback_ctx: ?*anyopaque = null,
    get_fn: ?ResolutionCandidateGetFn = null,
    scan_prefix_fn: ?ResolutionCandidateScanPrefixFn = null,
    nearest_fn: ?ResolutionCandidateNearestFn = null,
};

pub const EntityUpsert = extern struct {
    table: BorrowedBytes = .{},
    key: BorrowedBytes = .{},
    doc_json: BorrowedBytes = .{},
};
pub const EntityUpsertFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
    BorrowedBytes,
    BorrowedBytes,
) callconv(.c) Status;
pub const EntityUpsertBatchFn = *const fn (
    ?*anyopaque,
    ?[*]const EntityUpsert,
    u64,
) callconv(.c) Status;

pub const EntitySinkConfig = extern struct {
    callback_ctx: ?*anyopaque = null,
    upsert_fn: ?EntityUpsertFn = null,
    upsert_batch_fn: ?EntityUpsertBatchFn = null,
};

pub const PromotionOwnerFn = *const fn (?*anyopaque, u64) callconv(.c) u8;

/// Distributed resolution/promotion callbacks retained by a live owner. All
/// bytes are borrowed only for a synchronous callback; no allocator crosses
/// the compiled-storage boundary.
pub const RuntimeHooksConfig = extern struct {
    resolution_candidates: ResolutionCandidateConfig = .{},
    entity_sink: EntitySinkConfig = .{},
    promotion_owner_ctx: ?*anyopaque = null,
    promotion_owner_fn: ?PromotionOwnerFn = null,
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
    runtime_hooks: RuntimeHooksConfig = .{},
};

pub const JsonOperationRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
};

/// One committed data-Raft mutation. The log identity is persisted atomically
/// with the document mutation so replay after a crash is idempotent even for
/// non-idempotent transforms and transaction state transitions.
pub const ReplicatedBatchAtRaftEntryRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
    raft_term: u64 = 0,
    raft_index: u64 = 0,
};

/// Complete offline HA-seed operations that must remain beside physical DB
/// restore/validation code. Values are append-only because they are recorded
/// in `FailureIdentity.operation` for cross-unit diagnostics.
pub const HASeedOperation = enum(u32) {
    activate = 100,
    validate_activated_generation = 101,
    prune_activated_generations = 102,
};

/// The request JSON is borrowed for one synchronous coarse operation. Its
/// schema is the corresponding `storage/ha/seed_activation.zig` request type;
/// no allocator, Zig error union, or storage handle crosses the ABI.
pub const HASeedJsonRequest = extern struct {
    version: u32 = abi_version,
    operation: HASeedOperation,
    request_json: BorrowedBytes = .{},
};

pub const HASeedValidationResult = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    checkpoint_lsn: u64 = 0,
};

/// One already-merged hit borrowed for a synchronous coarse aggregation
/// operation. The kernel reads `stored_data` only for the duration of the call.
pub const AggregationHit = extern struct {
    stored_data: BorrowedBytes = .{},
};

/// Complete aggregation fold input. Expected semantic failures are returned
/// as distinct `Status` values; `internal` is reserved for unexpected defects.
pub const AggregationRequest = extern struct {
    version: u32 = abi_version,
    total_hits: u32 = 0,
    aggregations_json: BorrowedBytes = .{},
    context_json: BorrowedBytes = .{},
    hits: ?[*]const AggregationHit = null,
    hit_count: u64 = 0,
};

/// Synchronous coarse dispatch of one generated document-artifact child range.
/// The JSON bytes are borrowed only for the callback and encode the value-only
/// child-range batch contract. Returning an error leaves the provider's durable
/// outbox entry intact so a later batch can retry it.
pub const DocumentChildRangeDispatchFn = *const fn (
    ?*anyopaque,
    u64,
    BorrowedBytes,
) callconv(.c) Status;

/// Invoked synchronously after the provider has durably committed a batch.
/// The borrowed payload is the exact binary derived-change record committed
/// with that batch, or empty when derived replay was intentionally elided.
pub const CommittedBatchEffectsFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
) callconv(.c) Status;

pub const BatchJsonOperationRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
    document_child_range_dispatch_ctx: ?*anyopaque = null,
    document_child_range_dispatch_fn: ?DocumentChildRangeDispatchFn = null,
    committed_batch_effects_ctx: ?*anyopaque = null,
    committed_batch_effects_fn: ?CommittedBatchEffectsFn = null,
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
    expected_native_manifest_size_bytes: u64 = 0,
    expected_native_manifest_sha256: BorrowedBytes = .{},
    manifest_json: BorrowedBytes = .{},
};

/// Complete Raft replica restore bootstrap. Configuration and secret-store
/// handles are borrowed opaque process-local capabilities for this synchronous
/// call; all restore materialization remains inside the compiled storage unit.
pub const RestoreBootstrapRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    replica_root_dir: BorrowedBytes = .{},
    group_id: u64 = 0,
    backup_id: BorrowedBytes = .{},
    artifact_backup_id: BorrowedBytes = .{},
    location: BorrowedBytes = .{},
    snapshot_path: BorrowedBytes = .{},
    connection: BorrowedBytes = .{},
    artifact_size_bytes: u64 = 0,
    artifact_sha256: BorrowedBytes = .{},
    native_manifest_size_bytes: u64 = 0,
    native_manifest_sha256: BorrowedBytes = .{},
    required_capability: BorrowedBytes = .{},
    secret_store: ?*anyopaque = null,
    node_config: ?*const anyopaque = null,
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
/// callback and its opaque context remain borrowed for the duration of the
/// call; neither is retained by the storage owner.
pub const ControlledJsonOperationRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
    execution_deadline_ns: u64 = 0,
    has_execution_deadline: u8 = 0,
    _reserved1: [7]u8 = @splat(0),
    cancellation_ctx: ?*anyopaque = null,
    cancellation_fn: ?CancellationCheckFn = null,
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

/// Attaches the process inference runtime borrowed by storage-owned managed
/// enrichment workers. Attachment is allowed only before table owners open.
pub extern fn antfly_storage_context_attach_inference_provider(
    context: ?*anyopaque,
    inference_handle: ?*anyopaque,
) callconv(.c) Status;

/// Replaces the context-owned remote-content security snapshot before any
/// table owner opens. The payload is a ContentSecurityConfig JSON object.
pub extern fn antfly_storage_context_configure_remote_content_security(
    context: ?*anyopaque,
    security_json: BorrowedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_context_metrics(
    context: ?*anyopaque,
    out_result: *ContextMetricsResult,
) callconv(.c) Status;

/// Retires process-wide decoded storage state after an atomic generation
/// replacement. Existing borrowers remain valid; subsequent owners must load
/// the newly published filesystem generation.
pub extern fn antfly_storage_context_invalidate_caches(context: ?*anyopaque) callconv(.c) Status;

pub extern fn antfly_storage_context_system_store_open(
    context: ?*anyopaque,
    request: *const SystemStoreOpenRequest,
    out_store: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_storage_system_store_close(store: ?*anyopaque) callconv(.c) void;
pub extern fn antfly_storage_system_store_sync(store: ?*anyopaque, force: u8) callconv(.c) Status;
pub extern fn antfly_storage_system_store_begin_read(store: ?*anyopaque, out_txn: *?*anyopaque) callconv(.c) Status;
pub extern fn antfly_storage_system_store_begin_current_scan(store: ?*anyopaque, out_txn: *?*anyopaque) callconv(.c) Status;
pub extern fn antfly_storage_system_store_begin_write(store: ?*anyopaque, out_txn: *?*anyopaque) callconv(.c) Status;
pub extern fn antfly_storage_system_read_get(txn: ?*anyopaque, key: BorrowedBytes, out_value: *BorrowedBytes) callconv(.c) Status;
pub extern fn antfly_storage_system_read_open_cursor(txn: ?*anyopaque, out_cursor: *?*anyopaque) callconv(.c) Status;
pub extern fn antfly_storage_system_read_abort(txn: ?*anyopaque) callconv(.c) void;
pub extern fn antfly_storage_system_current_scan_open_cursor(txn: ?*anyopaque, out_cursor: *?*anyopaque) callconv(.c) Status;
pub extern fn antfly_storage_system_current_scan_abort(txn: ?*anyopaque) callconv(.c) void;
pub extern fn antfly_storage_system_write_get(txn: ?*anyopaque, key: BorrowedBytes, out_value: *BorrowedBytes) callconv(.c) Status;
pub extern fn antfly_storage_system_write_put(txn: ?*anyopaque, key: BorrowedBytes, value: BorrowedBytes) callconv(.c) Status;
pub extern fn antfly_storage_system_write_delete(txn: ?*anyopaque, key: BorrowedBytes) callconv(.c) Status;
pub extern fn antfly_storage_system_write_commit(txn: ?*anyopaque) callconv(.c) Status;
pub extern fn antfly_storage_system_write_abort(txn: ?*anyopaque) callconv(.c) void;
pub extern fn antfly_storage_system_cursor_move(
    cursor: ?*anyopaque,
    operation: SystemCursorSeek,
    key: BorrowedBytes,
    out_entry: *SystemEntryResult,
) callconv(.c) Status;
pub extern fn antfly_storage_system_cursor_close(cursor: ?*anyopaque) callconv(.c) void;

pub extern fn antfly_storage_context_lite_adoption_probe(
    context: ?*anyopaque,
    out_result: *LiteAdoptionProbeResult,
) callconv(.c) Status;
pub extern fn antfly_storage_context_lite_adopt_and_verify(
    context: ?*anyopaque,
    request: *const LiteAdoptionRequest,
) callconv(.c) Status;
pub extern fn antfly_storage_context_lite_mark_standalone(context: ?*anyopaque) callconv(.c) Status;
pub extern fn antfly_storage_context_maintenance_status(
    context: ?*anyopaque,
    out_result: *ContextMaintenanceStatus,
) callconv(.c) Status;
pub extern fn antfly_storage_context_maintenance_run(
    context: ?*anyopaque,
    request: *const ContextMaintenanceRequest,
    out_result: *ContextMaintenanceResult,
) callconv(.c) Status;

pub extern fn antfly_metadata_apply_store_open(
    request: *const MetadataApplyOpenRequest,
    out_store: *?*anyopaque,
) callconv(.c) Status;
pub extern fn antfly_metadata_apply_store_close(store: ?*anyopaque) callconv(.c) void;
pub extern fn antfly_metadata_apply_store_apply_batch(
    store: ?*anyopaque,
    request: *const MetadataApplyBatchRequest,
) callconv(.c) Status;
pub extern fn antfly_metadata_apply_store_build_snapshot(
    store: ?*anyopaque,
    request: *const MetadataApplyGroupRequest,
    out_snapshot: *OwnedBytes,
) callconv(.c) Status;
pub extern fn antfly_metadata_apply_store_install_snapshot(
    store: ?*anyopaque,
    request: *const MetadataApplySnapshotRequest,
) callconv(.c) Status;
pub extern fn antfly_metadata_apply_store_prepare_snapshot(
    store: ?*anyopaque,
    request: *const MetadataApplyPrepareSnapshotRequest,
    out_prepared: *?*anyopaque,
) callconv(.c) Status;
pub extern fn antfly_metadata_apply_prepared_snapshot_materialize(
    prepared: ?*anyopaque,
    out_snapshot: *OwnedBytes,
) callconv(.c) Status;
pub extern fn antfly_metadata_apply_prepared_snapshot_cancel(prepared: ?*anyopaque) callconv(.c) Status;
pub extern fn antfly_metadata_apply_prepared_snapshot_destroy(prepared: ?*anyopaque) callconv(.c) void;
pub extern fn antfly_metadata_apply_store_projection(
    store: ?*anyopaque,
    request: *const MetadataProjectionRequest,
    out_json: *OwnedBytes,
) callconv(.c) Status;
pub extern fn antfly_metadata_apply_store_add_listeners(
    store: ?*anyopaque,
    request: *const MetadataListenerRequest,
) callconv(.c) Status;
pub extern fn antfly_metadata_reconcile_replica_root(
    request: *const MetadataReplicaRootReconcileRequest,
    out_summary: *MetadataProvisionSummary,
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

pub extern fn antfly_data_apply_store_prepare_snapshot(
    store: ?*anyopaque,
    request: *const DataApplyPrepareSnapshotRequest,
    out_prepared: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_data_apply_prepared_snapshot_materialize(
    prepared: ?*anyopaque,
    out_result: *DataApplyPreparedSnapshotResult,
) callconv(.c) Status;

pub extern fn antfly_data_apply_prepared_snapshot_cancel(
    prepared: ?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_data_apply_prepared_snapshot_destroy(
    prepared: ?*anyopaque,
) callconv(.c) void;

pub extern fn antfly_data_apply_store_latest(
    store: ?*anyopaque,
    request: *const DataApplyGroupRequest,
    out_result: *DataApplyLatestResult,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_latest_for_transition(
    store: ?*anyopaque,
    request: *const DataApplyGroupRequest,
    out_result: *DataApplyLatestResult,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_raft_batch_protocol_version(
    store: ?*anyopaque,
    request: *const DataApplyGroupRequest,
    out_version: *u16,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_projection(
    store: ?*anyopaque,
    request: *const DataApplyProjectionRequest,
    out_result: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_data_apply_store_reconcile_owner(
    store: ?*anyopaque,
    owner: ?*anyopaque,
    request: *const DataApplyReconcileRequest,
    out_result: *DataApplyReconcileResult,
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

pub extern fn antfly_storage_owner_local_transition(
    primary_owner: ?*anyopaque,
    secondary_owner: ?*anyopaque,
    apply_store: ?*anyopaque,
    request: *const LocalTransitionRequest,
    out_result: *LocalTransitionResult,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_open(
    request: *const OpenRequest,
    out_owner: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_close(owner: ?*anyopaque) callconv(.c) void;

pub extern fn antfly_storage_ha_seed_activate_json(
    request: *const HASeedJsonRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_ha_seed_validate_json(
    request: *const HASeedJsonRequest,
    out_result: *HASeedValidationResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_ha_seed_prune_json(
    request: *const HASeedJsonRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_configure(
    owner: ?*anyopaque,
    request: *const ConfigureRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_reconcile(
    owner: ?*anyopaque,
    request: *const ReconcileRequest,
    out_result: *ReconcileResult,
) callconv(.c) Status;

/// Checks the resident owner's complete write-admission policy before a
/// distributed leader commits work to Raft. A busy result means the write is
/// currently backpressured and must not be proposed.
pub extern fn antfly_storage_owner_preflight_write_admission(
    owner: ?*anyopaque,
    request: *const TableRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_find_median_key(
    owner: ?*anyopaque,
    request: *const TableRequest,
    out_key: *OwnedBytes,
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
    request: *const BatchJsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_replicated_batch_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_replicated_batch_at_raft_entry_json(
    owner: ?*anyopaque,
    request: *const ReplicatedBatchAtRaftEntryRequest,
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

pub extern fn antfly_storage_restore_apply_bootstrap(
    request: *const RestoreBootstrapRequest,
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
    out_response: *QueryOwnedResponse,
    out_failure: *FailureIdentity,
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
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_text_stats_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_algebraic_partials_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

/// Computes one complete aggregation fold over already-merged query hits.
/// This remains a single coarse call: the descriptor array and document bytes
/// are borrowed, while the comparatively small context and result are JSON.
pub extern fn antfly_storage_aggregate_json(
    request: *const AggregationRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_graph_expand_json(
    owner: ?*anyopaque,
    request: *const ControlledJsonOperationRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_graph_hydrate_json(
    owner: ?*anyopaque,
    request: *const ControlledJsonOperationRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_graph_edges_json(
    owner: ?*anyopaque,
    request: *const ControlledJsonOperationRequest,
    out_response: *OwnedBytes,
    out_failure: *FailureIdentity,
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

/// Returns the owner DB's cached or validated dynamic-field capability sets.
/// The response is provider-owned JSON and must be released with the normal
/// owner response destroy operation.
pub extern fn antfly_storage_owner_observed_dynamic_field_capability_sets_json(
    owner: ?*anyopaque,
    request: *const ControlledJsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_restore_state_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_text_memory_json(
    owner: ?*anyopaque,
    request: *const TableRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_maintenance(
    owner: ?*anyopaque,
    request: *const MaintenanceRequest,
    out_result: *MaintenanceResult,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_open(
    request: *const WalOpenRequest,
    out_result: *WalOpenResult,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_close(wal: ?*anyopaque) callconv(.c) void;

pub extern fn antfly_storage_wal_append(
    wal: ?*anyopaque,
    request: *const WalAppendRequest,
    out_result: *WalAppendResult,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_sync(
    wal: ?*anyopaque,
    request: *const WalSyncRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_truncate_prefix(
    wal: ?*anyopaque,
    request: *const WalPositionRequest,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_truncate_suffix(
    wal: ?*anyopaque,
    request: *const WalPositionRequest,
    out_next_lsn: *u64,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_iterate(
    wal: ?*anyopaque,
    request: *const WalScanRequest,
    out_result: *WalScanResult,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_read(
    wal: ?*anyopaque,
    request: *const WalPositionRequest,
    out_result: *WalReadResult,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_stats_snapshot(
    wal: ?*anyopaque,
    out_stats: *WalStats,
) callconv(.c) Status;

pub extern fn antfly_storage_wal_last_lsn(
    wal: ?*anyopaque,
    out_last_lsn: *u64,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_buffer_destroy(
    buffer: *OwnedBytes,
) callconv(.c) void;

pub extern fn antfly_enrichment_extract_stream(
    request: *const EnrichmentExtractRequest,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_enrichment_render_pdf_page_png(
    request: *const EnrichmentRenderPdfRequest,
    out_png: *OwnedBytes,
    out_page: *EnrichmentRenderedPdfPage,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_enrichment_buffer_destroy(buffer: *OwnedBytes) callconv(.c) void;

pub extern fn antfly_local_query_execute(
    request: *const LocalQueryRequest,
    out_response: *QueryOwnedResponse,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_local_query_buffer_destroy(buffer: *OwnedBytes) callconv(.c) void;
