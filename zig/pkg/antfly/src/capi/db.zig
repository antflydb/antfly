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
const builtin = @import("builtin");
const local_write = antfly.local_write;
const raft_engine = @import("raft_engine");
const storage_root = @import("antfly_storage_root");
const antfly = if (@hasDecl(storage_root, "runtime_impl")) storage_root.runtime_impl else storage_root;
const TestDirectory = antfly.testing.TestDirectory;
const vector_mod = @import("antfly_vector").vector;
const capi = @import("types.zig");
pub const ApiTypes = capi;
const search_wire = @import("search_wire.zig");
const kernel_owner_abi = @import("kernel_owner_abi");
const kernel_error_identity = @import("kernel_error_identity");
const local_query_client = @import("local_query_client");
const capi_build_options = @import("capi_build_options");
const kernel_wal_owner = antfly.kernel_wal_owner;

pub const storageWalOpen = kernel_wal_owner.open;
pub const storageWalClose = kernel_wal_owner.close;
pub const storageWalAppend = kernel_wal_owner.append;
pub const storageWalAppendIdempotent = kernel_wal_owner.appendIdempotent;
pub const storageWalSync = kernel_wal_owner.sync;
pub const storageWalTruncatePrefix = kernel_wal_owner.truncatePrefix;
pub const storageWalTruncateSuffix = kernel_wal_owner.truncateSuffix;
pub const storageWalIterate = kernel_wal_owner.iterate;
pub const storageWalRead = kernel_wal_owner.read;
pub const storageWalStatsSnapshot = kernel_wal_owner.statsSnapshot;
pub const storageWalLastLsn = kernel_wal_owner.lastLsn;

const db_mod = antfly.db;
const backend_types = antfly.storage_backend;
const raft_mod = antfly.raft;
const hbc = antfly.hbc;
const graph_mod = antfly.graph;
const traversal_mod = antfly.traversal;
const paths_mod = antfly.paths;
const graph_query_mod = antfly.graph_query;
const graph_pattern_mod = antfly.graph_pattern;
const ha_seed_activation = antfly.ha_seed_activation;
const transactions_mod = antfly.transactions;
const aggregations_mod = db_mod.aggregations;
const aggregations_contract = aggregations_mod.contract;
const search_agg_mod = antfly.aggregation;
const geo_mod = antfly.geo;
const lite_backend = antfly.lite.backend;
const lite_restore_staging = antfly.lite.restore_staging;
const backup_codec = antfly.backup_codec;
const portable_backup = antfly.portable_backup;
const batch_api = antfly.public_api.batch;
const query_api = antfly.public_api.query;
const tables_api = antfly.public_api.tables;
const table_reads_api = antfly.local_query_contract;
const distributed_graph = antfly.public_api.distributed_graph;
const runtime_status = antfly.public_api.runtime_status;
const shard_state_store = antfly.data_snapshot;
const data_raft_apply = antfly.data_raft_apply;
const metadata_raft_apply = antfly.metadata_raft_apply;
const metadata_table_manager = antfly.metadata_table_manager;
const metadata_table_provisioner = antfly.metadata_table_provisioner;
const data_raft_projection_wire = antfly.data_raft_projection_wire;
const backups_api = antfly.public_api.backups;
const backup_restore = antfly.raft.storage.backup_restore;
const common_config = antfly.common_config;
const common_secrets = antfly.common_secrets;
const scraping = antfly.scraping;
const inference_provider = antfly.inference_provider;
const managed_embedder = antfly.managed_embedder;
const raft_catalog = antfly.raft_catalog;
const indexes_api = antfly.public_api.indexes;
const Allocator = std.mem.Allocator;

/// Version 2 unified the naming (antfly_db_* takes a handle, antfly_* is
/// library-level, antfly_lite_* is the .aflite format) and merged the Lite
/// open options into antfly_open_options.
const abi_version: u32 = 2;
/// ANTFLY_MIN_THREAD_STACK_SIZE in antfly.h.
const capi_min_thread_stack_size = 8 * 1024 * 1024;

const kernel_runtime_services = antfly.kernel_runtime_services;

const StorageOwnerContext = struct {
    allocator_bridge: ?kernel_runtime_services.memory.Allocator = null,
    io_receiver: ?kernel_runtime_services.executor.Receiver = null,
    alloc: Allocator,
    resources: antfly.physical_resources.PhysicalStorageResources,
    backend_runtime: db_mod.background_runtime.BackendRuntimeHandle,
    inference_lifetime: ?inference_provider.EmbeddedInferenceProviderLifetime = null,
    remote_content_security: ?std.json.Parsed(scraping.ContentSecurityConfig) = null,
    remote_content: scraping.RemoteContentConfig = .{},
    secret_store: ?*common_secrets.FileStore = null,
    lite_backend: ?lite_backend.Handle = null,
    auth_backend: ?antfly.lsm_backend.BackendHandle = null,
    auth_users_store: ?antfly.storage_backend_erased.Store = null,
    auth_casbin_store: ?antfly.storage_backend_erased.Store = null,
    mutex: std.atomic.Mutex = .unlocked,
    active_owners: usize = 0,

    fn lock(self: *StorageOwnerContext) void {
        antfly.platform_sync.lockYielding(&self.mutex);
    }

    fn acquire(self: *StorageOwnerContext) void {
        self.lock();
        defer self.mutex.unlock();
        self.active_owners += 1;
    }

    fn release(self: *StorageOwnerContext) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.active_owners > 0);
        self.active_owners -= 1;
    }

    fn antflyProvider(self: *StorageOwnerContext) ?managed_embedder.AntflyProvider {
        const lifetime = if (self.inference_lifetime) |*value| value else return null;
        return inference_provider.inferenceBoundaryProvider(lifetime);
    }

    fn remoteContent(self: *const StorageOwnerContext) ?*const scraping.RemoteContentConfig {
        return if (self.remote_content_security != null) &self.remote_content else null;
    }

    fn deinitIfIdle(self: *StorageOwnerContext) bool {
        self.lock();
        if (self.active_owners != 0) {
            self.mutex.unlock();
            return false;
        }
        self.mutex.unlock();
        if (self.inference_lifetime) |*lifetime| lifetime.quiesce();
        if (self.auth_casbin_store) |*store| store.deinit();
        if (self.auth_users_store) |*store| store.deinit();
        if (self.auth_backend) |*backend| backend.close();
        if (self.lite_backend) |*backend| backend.deinit();
        if (self.remote_content_security) |*parsed| parsed.deinit();
        self.backend_runtime.deinit();
        self.resources.deinit();
        // The standard allocator adapter points into this context. Copy its
        // table before poisoning/freeing the object that contains the adapter.
        const bridge = self.allocator_bridge;
        const alloc = if (bridge) |*value| value.asStd() else self.alloc;
        self.* = undefined;
        alloc.destroy(self);
        return true;
    }
};

const SystemStoreHandle = struct {
    store: *antfly.storage_backend_erased.Store,
    context: *StorageOwnerContext,
};

const SystemReadTxnHandle = struct {
    alloc: Allocator,
    txn: antfly.storage_backend_erased.ReadTxn,
};

const SystemCurrentScanTxnHandle = struct {
    alloc: Allocator,
    txn: antfly.storage_backend_erased.CurrentScanTxn,
};

const SystemWriteTxnHandle = struct {
    alloc: Allocator,
    txn: antfly.storage_backend_erased.WriteTxn,
};

const SystemCursorHandle = struct {
    alloc: Allocator,
    cursor: antfly.storage_backend_erased.Cursor,
};

const DataApplyStoreHandle = struct {
    alloc: Allocator,
    store: data_raft_apply.RaftApplyStore,
    context: ?*StorageOwnerContext,
};

const MetadataApplyStoreHandle = struct {
    alloc: Allocator,
    store: metadata_raft_apply.RaftApplyStore,
    context: ?*StorageOwnerContext,
    listener_bridges: std.ArrayListUnmanaged(*MetadataListenerBridge) = .empty,
    listener_mutex: std.Io.Mutex = .init,
};

const MetadataPreparedSnapshotHandle = struct {
    source: raft_engine.runtime.storage_iface.SnapshotSource,
};

const MetadataListenerBridge = struct {
    registration_id: u64 = 0,
    request: kernel_owner_abi.MetadataListenerRequest,

    const projection_vtable = metadata_raft_apply.ProjectionListener.VTable{
        .on_projection_signal = onProjection,
    };
    const projection_barrier_vtable = metadata_raft_apply.ProjectionListener.VTable{
        .on_projection_signal = onProjection,
        .before_projection_commit = beforeProjectionCommit,
        .after_projection_commit = afterProjectionCommit,
    };
    const committed_key_vtable = metadata_raft_apply.CommittedKeyListener.VTable{
        .matches_key = matchesKey,
        .on_committed_key = onCommittedKey,
    };

    fn projectionKindToAbi(kind: metadata_raft_apply.ProjectionSignalKind) kernel_owner_abi.MetadataProjectionSignalKind {
        return switch (kind) {
            .metadata_incarnation => .metadata_incarnation,
            .table => .table,
            .range => .range,
            .store => .store,
            .placement_intent => .placement_intent,
            .reconcile_lease => .reconcile_lease,
            .shuffle_join_lease => .shuffle_join_lease,
            .split_transition => .split_transition,
            .merge_transition => .merge_transition,
            .schema_progress => .schema_progress,
            .restore_progress => .restore_progress,
            .restore_job => .restore_job,
            .replication_source_status => .replication_source_status,
        };
    }

    fn projectionKindFromAbi(kind: kernel_owner_abi.MetadataProjectionSignalKind) metadata_raft_apply.ProjectionSignalKind {
        return switch (kind) {
            .metadata_incarnation => .metadata_incarnation,
            .table => .table,
            .range => .range,
            .store => .store,
            .placement_intent => .placement_intent,
            .reconcile_lease => .reconcile_lease,
            .shuffle_join_lease => .shuffle_join_lease,
            .split_transition => .split_transition,
            .merge_transition => .merge_transition,
            .schema_progress => .schema_progress,
            .restore_progress => .restore_progress,
            .restore_job => .restore_job,
            .replication_source_status => .replication_source_status,
        };
    }

    fn onProjection(ptr: *anyopaque, signal: metadata_raft_apply.ProjectionSignal) void {
        const self: *MetadataListenerBridge = @ptrCast(@alignCast(ptr));
        const callback = self.request.projection_fn orelse return;
        const value = kernel_owner_abi.MetadataProjectionSignal{
            .kind = projectionKindToAbi(signal.kind),
            .metadata_group_id = signal.metadata_group_id,
            .table_name = .fromSlice(signal.table_name orelse ""),
            .table_id = signal.table_id,
            .group_id = signal.group_id,
            .store_id = signal.store_id,
            .node_id = signal.node_id,
            .store_reports_changed = @intFromBool(signal.store_reports_changed),
            .store_runtime_changed = @intFromBool(signal.store_runtime_changed),
            .has_store_group_ids = @intFromBool(signal.store_group_ids != null),
            .store_group_ids = if (signal.store_group_ids) |ids| ids.ptr else null,
            .store_group_ids_len = if (signal.store_group_ids) |ids| ids.len else 0,
        };
        callback(self.request.context, &value);
    }

    fn beforeProjectionCommit(ptr: *anyopaque) void {
        const self: *MetadataListenerBridge = @ptrCast(@alignCast(ptr));
        if (self.request.before_projection_commit_fn) |callback| callback(self.request.context);
    }

    fn afterProjectionCommit(ptr: *anyopaque) void {
        const self: *MetadataListenerBridge = @ptrCast(@alignCast(ptr));
        if (self.request.after_projection_commit_fn) |callback| callback(self.request.context);
    }

    fn matchesKey(_: *anyopaque, _: metadata_raft_apply.CommittedKeySignal) bool {
        return true;
    }

    fn onCommittedKey(ptr: *anyopaque, signal: metadata_raft_apply.CommittedKeySignal) void {
        const self: *MetadataListenerBridge = @ptrCast(@alignCast(ptr));
        const callback = self.request.committed_key_fn orelse return;
        callback(self.request.context, signal.metadata_group_id, .fromSlice(signal.key));
    }
};

const DataApplyGroupTransitionHandle = struct {
    transition: data_raft_apply.RaftApplyStore.ActiveGroupTransition,
    active: bool = true,
};

const DataApplyPreparedSnapshotHandle = struct {
    prepared: *data_raft_apply.RaftApplyStore.PreparedSnapshot,
    materialized: bool = false,
};

const StorageOwnerTransactionRecovery = struct {
    alloc: Allocator,
    config: kernel_owner_abi.TransactionRecoveryConfig,
    owner_id: []u8,

    fn init(
        alloc: Allocator,
        config: kernel_owner_abi.TransactionRecoveryConfig,
    ) !StorageOwnerTransactionRecovery {
        return .{
            .alloc = alloc,
            .config = config,
            .owner_id = try alloc.dupe(u8, config.owner_id.slice()),
        };
    }

    fn deinit(self: *StorageOwnerTransactionRecovery) void {
        self.alloc.free(self.owner_id);
        self.* = undefined;
    }

    fn callbackStatus(status: kernel_owner_abi.Status) !void {
        return kernel_error_identity.statusToError(status);
    }

    fn resolveParticipant(
        ptr: *anyopaque,
        txn_id: transactions_mod.TxnId,
        participant: []const u8,
        status: transactions_mod.TxnStatus,
        commit_version: u64,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.config.resolve_participant_fn orelse return error.MissingParticipantResolver;
        const abi_txn_id = kernel_owner_abi.TxnId{ .bytes = txn_id };
        try callbackStatus(callback(
            self.config.callback_ctx,
            &abi_txn_id,
            .fromSlice(participant),
            switch (status) {
                .pending => .pending,
                .committed => .committed,
                .aborted => .aborted,
            },
            commit_version,
        ));
    }

    fn ownsRecovery(ptr: *anyopaque, owner_participant: []const u8) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.config.owns_recovery_fn orelse return false;
        return callback(self.config.callback_ctx, .fromSlice(owner_participant)) != 0;
    }

    fn acknowledgeParticipant(
        ptr: *anyopaque,
        txn_id: transactions_mod.TxnId,
        owner_participant: []const u8,
        participant: []const u8,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.config.acknowledge_participant_fn orelse return error.MissingReplicatedRecoveryHooks;
        const abi_txn_id = kernel_owner_abi.TxnId{ .bytes = txn_id };
        try callbackStatus(callback(
            self.config.callback_ctx,
            &abi_txn_id,
            .fromSlice(owner_participant),
            .fromSlice(participant),
        ));
    }

    fn cleanupTransaction(
        ptr: *anyopaque,
        txn_id: transactions_mod.TxnId,
        owner_participant: []const u8,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.config.cleanup_transaction_fn orelse return error.MissingReplicatedRecoveryHooks;
        const abi_txn_id = kernel_owner_abi.TxnId{ .bytes = txn_id };
        try callbackStatus(callback(
            self.config.callback_ctx,
            &abi_txn_id,
            .fromSlice(owner_participant),
            cutoff_timestamp,
            retained_cutoff_timestamp,
        ));
    }

    fn dbConfig(self: *StorageOwnerTransactionRecovery) db_mod.transaction_runtime.Config {
        return .{
            .enabled = true,
            .lease_owned = self.config.lease_owned != 0,
            .owner_id = self.owner_id,
            .interval_ms = self.config.interval_ms,
            .cutoff_ns = self.config.cutoff_ns,
            .resolver_ctx = self,
            .resolve_participant_fn = resolveParticipant,
            .replicated_metadata = self.config.replicated_metadata != 0,
            .owns_recovery_fn = if (self.config.replicated_metadata != 0) ownsRecovery else null,
            .acknowledge_participant_fn = if (self.config.replicated_metadata != 0) acknowledgeParticipant else null,
            .cleanup_transaction_fn = if (self.config.replicated_metadata != 0) cleanupTransaction else null,
        };
    }
};

const StorageOwnerRuntimeHooks = struct {
    fn coordinatedTtlPort(self: *StorageOwnerRuntimeHooks) ?antfly.capi_dependencies.storage_coordinated_ttl.Port {
        if (self.config.coordinated_ttl_enqueue_fn == null) return null;
        return .{ .ptr = self, .expire_fn = enqueueCoordinatedTtl };
    }

    fn enqueueCoordinatedTtl(ptr: *anyopaque, request: antfly.capi_dependencies.storage_coordinated_ttl.Request) !u32 {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.coordinated_ttl_enqueue_fn orelse return error.CoordinatedTtlBackpressure;
        if (request.candidates.len > kernel_owner_abi.coordinated_ttl_page_capacity) return error.CoordinatedTtlBackpressure;
        var candidates: [kernel_owner_abi.coordinated_ttl_page_capacity]kernel_owner_abi.CoordinatedTtlCandidate = undefined;
        for (request.candidates, candidates[0..request.candidates.len]) |source, *dest| {
            dest.* = .{ .key = .fromSlice(source.key), .row_version = source.row_version, .ttl_timestamp_ns = source.ttl_timestamp_ns, .expected_content_digest = source.expected_content_digest };
        }
        const wire = kernel_owner_abi.CoordinatedTtlRequest{
            .table_id = request.table_id,
            .group_id = request.group_id,
            .schema_version = request.schema_version,
            .ttl_duration_ns = request.ttl_duration_ns,
            .ttl_field = .fromSlice(request.ttl_field),
            .observed_at_unix_ns = request.observed_at_unix_ns,
            .grace_period_ns = request.grace_period_ns,
            .candidates = &candidates,
            .candidate_count = @intCast(request.candidates.len),
        };
        if (callback(self.config.coordinated_ttl_ctx, &wire) != 0) return error.CoordinatedTtlBackpressure;
        // Accepted for coordination; no row is reported deleted here.
        return 0;
    }

    fn nativeAuthorityPermitted(ptr: *const anyopaque) bool {
        const self: *const StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.native_authority_fn orelse return false;
        return callback(self.config.native_authority_ctx) != 0;
    }

    fn nativeMigrationPolicy(self: *const StorageOwnerRuntimeHooks) ?db_mod.DenseNativeMigrationPolicySource {
        if (self.config.native_authority_fn == null) return null;
        return .{ .ptr = self, .authority_permitted = nativeAuthorityPermitted };
    }

    config: kernel_owner_abi.RuntimeHooksConfig,
    group_id: u64,

    const CandidateCapture = struct {
        alloc: Allocator,
        value: ?[]u8 = null,

        fn consume(
            ptr: ?*anyopaque,
            _: kernel_owner_abi.BorrowedBytes,
            value: kernel_owner_abi.BorrowedBytes,
        ) callconv(.c) kernel_owner_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
            if (self.value != null) return .invalid_argument;
            self.value = self.alloc.dupe(u8, value.slice()) catch return .out_of_memory;
            return .ok;
        }
    };

    const CandidateConsumerBridge = struct {
        ctx: *anyopaque,
        consume: db_mod.CandidateSource.Consume,

        fn forward(
            ptr: ?*anyopaque,
            entity_key: kernel_owner_abi.BorrowedBytes,
            value: kernel_owner_abi.BorrowedBytes,
        ) callconv(.c) kernel_owner_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
            self.consume(self.ctx, entity_key.slice(), value.slice()) catch |err|
                return kernel_error_identity.statusFromError(err);
            return .ok;
        }
    };

    fn candidateSource(self: *StorageOwnerRuntimeHooks) ?db_mod.CandidateSource {
        if (self.config.resolution_candidates.get_fn == null) return null;
        return .{ .ptr = self, .vtable = &candidate_vtable };
    }

    const candidate_vtable = db_mod.CandidateSource.VTable{
        .get = candidateGet,
        .scan_prefix = candidateScanPrefix,
        .nearest = candidateNearest,
    };

    fn candidateGet(
        ptr: *anyopaque,
        alloc: Allocator,
        table: []const u8,
        key: []const u8,
    ) anyerror!?[]u8 {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.resolution_candidates.get_fn orelse return error.MissingResolutionCandidateSource;
        var capture = CandidateCapture{ .alloc = alloc };
        const status = callback(
            self.config.resolution_candidates.callback_ctx,
            .fromSlice(table),
            .fromSlice(key),
            &capture,
            CandidateCapture.consume,
        );
        if (status == .not_found) return null;
        try kernel_error_identity.statusToError(status);
        return capture.value orelse return error.InvalidArgument;
    }

    fn candidateScanPrefix(
        ptr: *anyopaque,
        _: Allocator,
        table: []const u8,
        prefix: []const u8,
        opts: db_mod.CandidateSource.ScanOptions,
        ctx: *anyopaque,
        consume: db_mod.CandidateSource.Consume,
    ) anyerror!void {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.resolution_candidates.scan_prefix_fn orelse return error.ScanUnsupported;
        var bridge = CandidateConsumerBridge{ .ctx = ctx, .consume = consume };
        try kernel_error_identity.statusToError(callback(
            self.config.resolution_candidates.callback_ctx,
            .fromSlice(table),
            .fromSlice(prefix),
            @intCast(opts.limit),
            &bridge,
            CandidateConsumerBridge.forward,
        ));
    }

    fn candidateNearest(
        ptr: *anyopaque,
        _: Allocator,
        table: []const u8,
        query: db_mod.CandidateSource.NearestQuery,
        ctx: *anyopaque,
        consume: db_mod.CandidateSource.Consume,
    ) anyerror!void {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.resolution_candidates.nearest_fn orelse return error.NearestUnsupported;
        var bridge = CandidateConsumerBridge{ .ctx = ctx, .consume = consume };
        try kernel_error_identity.statusToError(callback(
            self.config.resolution_candidates.callback_ctx,
            .fromSlice(table),
            .fromSlice(query.index_name),
            if (query.embedding.len == 0) null else query.embedding.ptr,
            @intCast(query.embedding.len),
            @intCast(query.k),
            &bridge,
            CandidateConsumerBridge.forward,
        ));
    }

    fn entitySink(self: *StorageOwnerRuntimeHooks) ?db_mod.EntitySink {
        if (self.config.entity_sink.upsert_fn == null) return null;
        return .{ .ptr = self, .vtable = &entity_sink_vtable };
    }

    const entity_sink_vtable = db_mod.EntitySink.VTable{
        .upsert = entityUpsert,
        .upsert_batch = entityUpsertBatch,
    };

    fn entityUpsert(
        ptr: *anyopaque,
        _: Allocator,
        table: []const u8,
        key: []const u8,
        doc_json: []const u8,
    ) anyerror!void {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.entity_sink.upsert_fn orelse return error.MissingEntitySink;
        try kernel_error_identity.statusToError(callback(
            self.config.entity_sink.callback_ctx,
            .fromSlice(table),
            .fromSlice(key),
            .fromSlice(doc_json),
        ));
    }

    fn entityUpsertBatch(
        ptr: *anyopaque,
        alloc: Allocator,
        entries: []const db_mod.EntityUpsert,
    ) anyerror!void {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.entity_sink.upsert_batch_fn orelse {
            for (entries) |entry| try entityUpsert(ptr, alloc, entry.table, entry.key, entry.doc_json);
            return;
        };
        const encoded = try alloc.alloc(kernel_owner_abi.EntityUpsert, entries.len);
        defer alloc.free(encoded);
        for (entries, encoded) |source, *destination| destination.* = .{
            .table = .fromSlice(source.table),
            .key = .fromSlice(source.key),
            .doc_json = .fromSlice(source.doc_json),
        };
        try kernel_error_identity.statusToError(callback(
            self.config.entity_sink.callback_ctx,
            if (encoded.len == 0) null else encoded.ptr,
            @intCast(encoded.len),
        ));
    }

    fn promotionOwner(self: *StorageOwnerRuntimeHooks) ?db_mod.PromotionOwner {
        if (self.config.promotion_owner_fn == null) return null;
        return .{ .ptr = self, .vtable = &promotion_owner_vtable };
    }

    const promotion_owner_vtable = db_mod.PromotionOwner.VTable{ .is_local_owner = isLocalPromotionOwner };

    fn isLocalPromotionOwner(ptr: *anyopaque) bool {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.promotion_owner_fn orelse return true;
        return callback(self.config.promotion_owner_ctx, self.group_id) != 0;
    }
};

test "storage-owner reverse callbacks preserve semantic error identity" {
    try std.testing.expectError(
        error.WouldBlock,
        StorageOwnerTransactionRecovery.callbackStatus(.would_block),
    );
    try std.testing.expectError(
        error.StorageBusy,
        StorageOwnerTransactionRecovery.callbackStatus(.busy),
    );
    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        StorageOwnerTransactionRecovery.callbackStatus(.resource_budget_exceeded),
    );
    try std.testing.expectError(
        error.Canceled,
        StorageOwnerTransactionRecovery.callbackStatus(.canceled),
    );
    try std.testing.expectError(
        error.Cancelled,
        StorageOwnerTransactionRecovery.callbackStatus(.cancelled),
    );
}

fn monotonicNowNs() u64 {
    return antfly.platform_time.monotonicNs();
}

fn tempTestPath(alloc: Allocator, root: []const u8, label: []const u8) ![:0]u8 {
    return try std.fmt.allocPrintSentinel(alloc, "{s}-{s}", .{ root, label }, 0);
}

fn tempTestAflitePath(alloc: Allocator, root: []const u8, label: []const u8) ![:0]u8 {
    const base = try tempTestPath(alloc, root, label);
    defer alloc.free(base);
    const path = try std.fmt.allocPrint(alloc, "{s}.aflite", .{base});
    defer alloc.free(path);
    return try alloc.dupeZ(u8, path);
}

const Handle = struct {
    alloc: std.mem.Allocator,
    db: db_mod.DB,
    open_mode: db_mod.OpenOptions.OpenMode = .writer,
    readable_lease_hook: ?ReadableLeaseHook = null,
    owned_lite_backend: ?lite_backend.Handle = null,
    lite_profile: ?lite_backend.Profile = null,
    lite_inference_status: ?lite_backend.InferenceStatus = null,
    storage_owner_path: ?[]u8 = null,
    storage_owner_managed_config: local_write.OwnerManagedConfig = .{},
    storage_owner_target_observer: kernel_owner_abi.TargetObserver = .{},
    storage_owner_table_name: ?[]u8 = null,
    storage_owner_group_id: u64 = 0,
    storage_owner_root_generation: u64 = 0,
    storage_owner_context: ?*StorageOwnerContext = null,
    storage_owner_transaction_recovery: ?*StorageOwnerTransactionRecovery = null,
    storage_owner_runtime_hooks: ?*StorageOwnerRuntimeHooks = null,
    // Present only for a Lite handle opened with the local-runtime-configured
    // flag on a build that both advertises and actually links the local
    // inference runtime (see capi_build_options.inference_enabled and
    // pkg/antfly/build/runtime.zig's addCapiInferenceVariantUnits). Default
    // libantfly and Lite handles opened without the flag leave these null,
    // which keeps today's behavior unchanged.
    lite_inference_lifetime: ?inference_provider.EmbeddedInferenceProviderLifetime = null,
    lite_inference_io: ?*std.Io.Threaded = null,
    // Mirrors `LiteResolvedOpenOptions.generated_enrichment_replay` from this
    // handle's open call. `refreshLiteManagedEmbeddingRuntime` must not lose
    // this caller intent across its own reconfigure passes -- see its doc
    // comment.
    lite_generated_enrichment_replay: bool = false,
    // Serialized threading mode (see CAPI.md "Thread Safety"): every export
    // enters through `enterHandle`, which takes `api_lock` shared for reads
    // and data writes and exclusively for schema/admin changes. Data writes
    // and maintenance additionally serialize on their own mutexes so they
    // queue instead of failing with ANTFLY_BUSY, while reads keep running
    // against pinned snapshots. Callers hold a HandleRegistry id rather than
    // this pointer, so close can drain and free safely (see HandleRegistry).
    api_lock: std.Io.RwLock = .init,
    write_mutex: std.Io.Mutex = .init,
    maintenance_mutex: std.Io.Mutex = .init,

    fn liteAntflyProvider(self: *Handle) ?managed_embedder.AntflyProvider {
        const lifetime = if (self.lite_inference_lifetime) |*value| value else return null;
        return inference_provider.inferenceBoundaryProvider(lifetime);
    }

    fn prepareSearchRequest(self: *Handle, req: db_mod.types.SearchRequest) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareSearch(hook.group_id, req);
    }

    fn prepareDenseSearchRequest(
        self: *Handle,
        index_name: []const u8,
        vector: []const f32,
        k: u32,
        limit: u32,
        offset: u32,
    ) !void {
        try self.prepareSearchRequest(.{
            .index_name = index_name,
            .query = .{ .dense_knn = .{
                .vector = vector,
                .k = k,
            } },
            .limit = limit,
            .offset = offset,
            .include_stored = false,
        });
    }

    fn prepareLookupRequest(self: *Handle, key: []const u8, opts: db_mod.types.LookupOptions) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareLookup(hook.group_id, key, opts);
    }

    fn prepareScanRequest(
        self: *Handle,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
    ) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareScan(hook.group_id, from_key, to_key, opts);
    }
};

const StorageSnapshot = struct {
    alloc: Allocator,
    preparation: db_mod.generation_lifecycle.PreparationTransition,
    staged: db_mod.generation_lifecycle.StagedGeneration,
    transition: ?db_mod.generation_lifecycle.ExclusiveTransition = null,
    restore_live_path: ?[]u8 = null,
    promoted: bool = false,
    published: bool = false,
    finalized: bool = false,

    fn deinit(self: *StorageSnapshot) void {
        self.staged.deinit();
        if (self.transition) |*transition| transition.deinit();
        self.preparation.deinit();
        if (self.restore_live_path) |path| self.alloc.free(path);
        const alloc = self.alloc;
        self.* = undefined;
        alloc.destroy(self);
    }
};

/// Starts the embedded inference provider owned by a Lite handle. Callers
/// must only invoke this when `capi_build_options.inference_enabled` is true
/// (only true for the isolated `-Dcapi-inference=true` storage_kernel unit
/// and for unit tests): that is the only context where the real inference
/// runtime archive is linked in this binary instead of the
/// capi/link_anchor(_inference).zig trap.
fn startLiteEmbeddedInference(
    handle: *Handle,
    alloc: Allocator,
    path: []const u8,
    budget_options: inference_provider.EmbeddedInferenceNodeOptions,
) !void {
    const io_impl = try alloc.create(std.Io.Threaded);
    errdefer alloc.destroy(io_impl);
    io_impl.* = std.Io.Threaded.init(std.heap.page_allocator, .{});
    errdefer io_impl.deinit();
    const data_dir = std.fs.path.dirname(path) orelse ".";
    const created = try inference_provider.createEmbeddedInferenceNode(data_dir, io_impl.io(), budget_options);
    handle.lite_inference_io = io_impl;
    handle.lite_inference_lifetime = .{ .handle = created.handle, .resource_owner = created.resource_owner };
    // Report the policy the node actually resolved (host-detected by
    // default, or the caller's explicit override) rather than leaving
    // `lite_inference_status` at the pre-open placeholder values.
    if (handle.lite_inference_status) |*status| {
        status.process_memory_limit_bytes = @intCast(created.process_memory_limit_bytes);
        status.process_memory_limit_source = @tagName(created.process_memory_limit_source);
        status.host_budget_mb = created.host_budget_mb;
        status.backend_budget_mb = created.backend_budget_mb;
        status.combined_budget_mb = created.combined_budget_mb;
        status.kv_budget_mb = created.kv_budget_mb;
        status.scratch_budget_mb = created.scratch_budget_mb;
    }
}

fn stopLiteEmbeddedInference(handle: *Handle) void {
    if (handle.lite_inference_lifetime) |*lifetime| {
        lifetime.quiesce();
        inference_provider.destroyEmbeddedInferenceNode(lifetime.handle, lifetime.resource_owner);
        handle.lite_inference_lifetime = null;
    }
    if (handle.lite_inference_io) |io_impl| {
        io_impl.deinit();
        handle.alloc.destroy(io_impl);
        handle.lite_inference_io = null;
    }
}

/// `managed_embedder.zig`'s index scanner only recognizes an entry as a
/// managed embeddings producer when it carries the public
/// `"type":"embeddings"` marker, matching the shape a server table stores
/// (see the `EmbeddingsIndexConfig` OpenAPI schema). Lite's own raw
/// `config_json` for a dense_vector/sparse_vector index uses lower-level,
/// pre-existing field names instead ("dims"/"metric" rather than
/// "dimension"/"distance_metric", and no "type" at all -- confirmed against
/// examples/dogfood's index_config.go), so without this the entry is
/// silently skipped by `parseManagedEmbeddingEntry` rather than erroring.
/// Bridges the gap by injecting the marker (and a "dimension" alias of
/// "dims" so a known vector width skips the capability-discovery probe)
/// without touching any field the caller supplied. Falls back to passing
/// `config_json` through unchanged for any other index kind, or if it fails
/// to parse as a JSON object.
fn liteManagedEmbeddingIndexConfigJson(
    alloc: Allocator,
    kind: db_mod.types.IndexKind,
    config_json: []const u8,
) ![]u8 {
    if (kind != .dense_vector and kind != .sparse_vector) return try alloc.dupe(u8, config_json);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, config_json, .{}) catch
        return try alloc.dupe(u8, config_json);
    if (parsed.value != .object) return try alloc.dupe(u8, config_json);

    if (parsed.value.object.get("type") == null) {
        try parsed.value.object.put(arena, "type", .{ .string = "embeddings" });
    }
    if (kind == .sparse_vector and parsed.value.object.get("sparse") == null) {
        try parsed.value.object.put(arena, "sparse", .{ .bool = true });
    }
    if (parsed.value.object.get("dimension") == null) {
        if (parsed.value.object.get("dims")) |dims_value| {
            try parsed.value.object.put(arena, "dimension", dims_value);
        }
    }
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

/// Server table provisioning accepts the artifact-stream chunk pattern
/// (`go/pkg/docsaf`'s `chunk` enrichment producing a `doc_chunks_v1` artifact,
/// consumed by an embeddings index via `"sources":[{"artifact":...}]`) by
/// nesting an `"enrichments"` array inside whichever index config
/// authoritatively owns each producer, then harvesting every nested
/// declaration across the whole table (`api/indexes.zig`'s
/// `collectArtifactEnrichmentsFromTableIndexesJsonWithOptions`, dependency
/// sorted via `sortArtifactEnrichmentsByDependency`) and registering each one
/// with `db.upsertEnrichment` *before* admitting the physical indexes
/// (`metadata_table_provisioner.reconcileDbIndexesWithOptions` calls
/// `ensureEnrichments` ahead of `ensureIndexes`). A native Lite handle only
/// ever admits one index at a time through `antfly_db_add_index_json`, so
/// there is no single merged table definition to harvest from; this instead
/// harvests the `"enrichments"` nested in *this* index's own raw config
/// before it is translated/admitted, mirroring the same per-index shape
/// docsaf and the dogfood example already send. Two caveats callers must
/// respect that the server's atomic table-create request does not have: (1)
/// a producer index (e.g. the `chunk` enrichment's owning `full_text` index)
/// must be added before any index whose `sources`/`embedding_name` names an
/// artifact that producer's enrichment declares, since enrichment admission
/// validates upstream references immediately; (2) re-adding the same index
/// name replays its enrichment declarations too, which is harmless because
/// `db.upsertEnrichment` is idempotent for an unchanged config. Scoped to the
/// native profile: a hosted Lite handle's owning process reconciles its own
/// enrichments the same way the server does.
fn registerLiteIndexEnrichments(handle: *Handle, config_json: []const u8, rollback: *LiteCatalogRollback) !void {
    var arena_impl = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, config_json, .{}) catch return;
    if (parsed.value != .object or parsed.value.object.get("enrichments") == null) return;

    const alloc = handle.alloc;
    var collected: std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig) = .empty;
    defer {
        for (collected.items) |*cfg| cfg.deinit(alloc);
        collected.deinit(alloc);
    }
    try indexes_api.collectArtifactEnrichmentsFromValueWithOptions(
        alloc,
        parsed.value,
        .{ .antfly_provider = handle.liteAntflyProvider() },
        &collected,
    );
    if (collected.items.len == 0) return;
    indexes_api.sortArtifactEnrichmentsByDependency(collected.items);
    for (collected.items) |cfg| {
        // Record the touch before mutating, so a mid-loop failure still rolls
        // back every enrichment this call may have changed.
        try rollback.willTouchEnrichment(cfg.kind, cfg.name);
        _ = try handle.db.upsertEnrichment(cfg);
    }
}

/// Undo buffer for the catalog mutations a native Lite AddIndex performs
/// before (nested enrichments) and after (nested resolvers) `db.addIndex`.
/// The C ABI admits one index per call with no transaction around the
/// enrichment/resolver catalogs, so a rejected or partially failed AddIndex
/// must restore the pre-call configuration itself: durably upserting an
/// existing enrichment's changed geometry and then failing index admission
/// (for example with IndexAlreadyExists) must not leave the changed
/// enrichment active.
const LiteCatalogRollback = struct {
    const TouchedEnrichment = struct {
        kind: db_mod.types.EnrichmentKind,
        name: []u8,
    };

    handle: *Handle,
    /// Full pre-call enrichment catalog (owned).
    prior: []db_mod.types.EnrichmentConfig,
    /// Full pre-call resolver catalog (owned).
    prior_resolvers: []db_mod.ResolverConfig,
    touched: std.ArrayListUnmanaged(TouchedEnrichment) = .empty,
    touched_resolvers: std.ArrayListUnmanaged([]u8) = .empty,

    fn init(handle: *Handle) !LiteCatalogRollback {
        const prior = try handle.db.listEnrichments(handle.alloc);
        errdefer db_mod.types.freeEnrichmentConfigs(handle.alloc, prior);
        return .{
            .handle = handle,
            .prior = prior,
            .prior_resolvers = try handle.db.listResolvers(handle.alloc),
        };
    }

    fn deinit(self: *LiteCatalogRollback) void {
        const alloc = self.handle.alloc;
        db_mod.types.freeEnrichmentConfigs(alloc, self.prior);
        for (self.prior_resolvers) |*cfg| cfg.deinit(alloc);
        alloc.free(self.prior_resolvers);
        for (self.touched.items) |touch| alloc.free(touch.name);
        self.touched.deinit(alloc);
        for (self.touched_resolvers.items) |name| alloc.free(name);
        self.touched_resolvers.deinit(alloc);
    }

    fn willTouchEnrichment(self: *LiteCatalogRollback, kind: db_mod.types.EnrichmentKind, name: []const u8) !void {
        const alloc = self.handle.alloc;
        for (self.touched.items) |touch| {
            if (touch.kind == kind and std.mem.eql(u8, touch.name, name)) return;
        }
        try self.touched.append(alloc, .{ .kind = kind, .name = try alloc.dupe(u8, name) });
    }

    fn willTouchResolver(self: *LiteCatalogRollback, name: []const u8) !void {
        const alloc = self.handle.alloc;
        for (self.touched_resolvers.items) |touched| {
            if (std.mem.eql(u8, touched, name)) return;
        }
        try self.touched_resolvers.append(alloc, try alloc.dupe(u8, name));
    }

    /// Best-effort restore of every touched enrichment to its pre-call
    /// configuration: re-upsert the prior config, or delete an enrichment
    /// this call introduced. Restore failures are logged, never masked over
    /// the admission error the caller is already returning.
    fn restore(self: *LiteCatalogRollback) void {
        for (self.touched_resolvers.items) |name| {
            const prior = blk: {
                for (self.prior_resolvers) |cfg| {
                    if (std.mem.eql(u8, cfg.name, name)) break :blk cfg;
                }
                break :blk null;
            };
            if (prior) |cfg| {
                _ = self.handle.db.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false }) catch |err| {
                    std.log.warn("lite AddIndex rollback failed to restore resolver {s}: {s}", .{ name, @errorName(err) });
                };
            } else {
                _ = self.handle.db.removeResolverWithoutDrain(name) catch |err| {
                    std.log.warn("lite AddIndex rollback failed to remove resolver {s}: {s}", .{ name, @errorName(err) });
                };
            }
        }
        for (self.touched.items) |touch| {
            const prior = blk: {
                for (self.prior) |cfg| {
                    if (cfg.kind == touch.kind and std.mem.eql(u8, cfg.name, touch.name)) break :blk cfg;
                }
                break :blk null;
            };
            if (prior) |cfg| {
                _ = self.handle.db.upsertEnrichment(cfg) catch |err| {
                    std.log.warn("lite AddIndex rollback failed to restore enrichment {s}: {s}", .{ touch.name, @errorName(err) });
                };
            } else {
                _ = self.handle.db.deleteEnrichment(touch.kind, touch.name) catch |err| {
                    std.log.warn("lite AddIndex rollback failed to remove enrichment {s}: {s}", .{ touch.name, @errorName(err) });
                };
            }
        }
    }
};

/// Register the entity resolvers nested in this index's own raw config, the
/// way `registerLiteIndexEnrichments` harvests nested `"enrichments"`. The
/// server registers resolvers from the whole table's indexes JSON
/// (`metadata_table_provisioner.ensureResolversWithOptions`) after index
/// provisioning; a native Lite handle admits one index at a time, so this
/// harvests the graph config's `"resolvers"` array after the index itself is
/// admitted. Add/update only — a single index's config never proves another
/// index's resolvers are gone, so nothing is removed here. `upsertResolver`
/// is idempotent for an unchanged config; backfill is deferred to the
/// resolver workers (or the next `antfly_db_run_until_idle`).
fn registerLiteIndexResolvers(handle: *Handle, config_json: []const u8, rollback: *LiteCatalogRollback) !void {
    var arena_impl = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, config_json, .{}) catch return;
    if (parsed.value != .object) return;
    const resolvers = parsed.value.object.get("resolvers") orelse return;
    if (resolvers != .array) return;
    for (resolvers.array.items) |item| {
        if (item != .object) continue;
        const cfg = try std.json.parseFromValue(db_mod.ResolverConfig, arena, item, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        // Record the touch before mutating, so a mid-loop failure (an
        // invalid later resolver, a label conflict) still restores every
        // earlier insertion or replacement this call made.
        try rollback.willTouchResolver(cfg.value.name);
        _ = try handle.db.upsertResolverWithResultOptions(cfg.value, .{ .drain_backfill = false });
    }
}

test "capi lite AddIndexJSON registers the server's nested artifact-sourced enrichment shape" {
    // Reproduces the docsaf/dogfood chunk-artifact pattern -- a `chunk`
    // enrichment producing `document_chunks_v1`, then an embeddings index
    // consuming it via `"sources":[{"artifact":"document_chunk_dense_v1"}]`
    // with the producing `embedding` enrichment nested in the index's own
    // config -- driven entirely through `antfly_db_add_index_json`, the way
    // `go/pkg/docsaf/cmd/docsaf/main.go`'s `createHierarchyIndexes` and
    // `antfly.NewArtifactEmbeddingIndexConfig` build it. Before
    // `registerLiteIndexEnrichments` this silently dropped both nested
    // enrichment declarations (they are not valid `db.addIndex` fields), so
    // the physical dense_vector index referenced a `document_chunk_dense_v1`
    // artifact with no enrichment ever registered to produce it.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-nested-enrichments");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    // Producer index: a full_text index over the chunk artifact (docsaf's
    // `document_text`), with the `chunk` enrichment nested in its config.
    const chunk_index_json =
        \\{"name":"document_text_chunks","kind":"full_text","config_json":"{\"chunk_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":64}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = chunk_index_json,
        .len = chunk_index_json.len,
    }));

    var enrichments_after_chunk: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_enrichments_json(handle, &enrichments_after_chunk));
    defer freeRawBuffer(enrichments_after_chunk.ptr, enrichments_after_chunk.len);
    try std.testing.expect(std.mem.indexOf(u8, enrichments_after_chunk.ptr.?[0..enrichments_after_chunk.len], "\"document_chunks_v1\"") != null);

    // Consumer index: the exact two-stage `sources` form docsaf's
    // `NewArtifactEmbeddingIndexConfig` builds, with the `embedding`
    // enrichment nested in this index's own config and its
    // `source_artifact_name` pointing at the chunk artifact above.
    const vector_index_json =
        \\{"name":"document_vectors","kind":"dense_vector","config_json":"{\"type\":\"embeddings\",\"sources\":[{\"artifact\":\"document_chunk_dense_v1\"}],\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"test-embed\",\"api_url\":\"http://127.0.0.1:1\"},\"distance_metric\":\"cosine\",\"enrichments\":[{\"name\":\"document_chunk_dense_v1\",\"kind\":\"embedding\",\"field\":\"text\",\"source_artifact_name\":\"document_chunks_v1\",\"expected_dims\":3}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = vector_index_json,
        .len = vector_index_json.len,
    }));

    var enrichments_after_vectors: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_enrichments_json(handle, &enrichments_after_vectors));
    defer freeRawBuffer(enrichments_after_vectors.ptr, enrichments_after_vectors.len);
    try std.testing.expect(std.mem.indexOf(u8, enrichments_after_vectors.ptr.?[0..enrichments_after_vectors.len], "\"document_chunk_dense_v1\"") != null);

    var indexes: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_indexes_json(handle, &indexes));
    defer freeRawBuffer(indexes.ptr, indexes.len);
    const indexes_json = indexes.ptr.?[0..indexes.len];
    try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"document_vectors\"") != null);
    // The physical config carries the translated artifact source reference
    // (config_json is itself JSON-encoded as a string, so its embedded quotes
    // are backslash-escaped here rather than literal).
    try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\\\"sources\\\":[{\\\"artifact\\\":\\\"document_chunk_dense_v1\\\"") != null);
}

test "capi lite AddIndexJSON restores the enrichment catalog when admission rejects the index" {
    // Reviewer-reported P2: registerLiteIndexEnrichments durably upserted
    // every nested enrichment BEFORE db.addIndex validated the index, so
    // re-adding an existing index with a changed chunk_size updated the
    // active enrichment and then failed with IndexAlreadyExists — the caller
    // saw an error while the configuration had silently changed. AddIndex is
    // now all-or-nothing: the pre-call enrichment catalog is restored on any
    // admission failure.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-addindex-rollback");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const original_index_json =
        \\{"name":"document_text_chunks","kind":"full_text","config_json":"{\"chunk_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":64}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = original_index_json,
        .len = original_index_json.len,
    }));

    // Same index name, changed chunk geometry: admission must reject it AND
    // the active chunk enrichment must keep chunk_size 64.
    const changed_index_json =
        \\{"name":"document_text_chunks","kind":"full_text","config_json":"{\"chunk_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":128}]}"}
    ;
    try std.testing.expect(antfly_db_add_index_json(handle, .{
        .ptr = changed_index_json,
        .len = changed_index_json.len,
    }) != .ok);

    {
        const enrichments = try asHandle(handle).?.db.listEnrichments(alloc);
        defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("document_chunks_v1", enrichments[0].name);
        try std.testing.expectEqual(@as(u32, 64), enrichments[0].chunk_size);
    }
}

test "capi lite AddIndexJSON registers a graph config's nested resolvers" {
    // The server registers entity resolvers from the whole table's indexes
    // JSON (metadata_table_provisioner.ensureResolvers); a native Lite
    // handle admits one index at a time, so registerLiteIndexResolvers
    // harvests the graph config's own "resolvers" array — the shape
    // examples/dogfood's knowledgeGraphIndexJSON declares. Re-adding the
    // same index must be idempotent (upsertResolver observes an unchanged
    // config), matching the enrichment path's contract.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-graph-resolvers");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const graph_index_json =
        \\{"name":"knowledge","kind":"graph","config_json":"{\"source\":{\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\",\"mention_edge_type\":\"mentions\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"source\":{\"type\":\"field\",\"value\":\"body\"},\"content_type\":\"application/json\"},\"resolvers\":[{\"name\":\"entities\",\"table\":\"entities\",\"source_artifact\":\"relations_v1\",\"resolution_artifact\":\"entities_resolution_v1\",\"key_template\":\"{{ lower _entity.label }}/{{ slug _entity.text }}\",\"config_generation\":1}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = graph_index_json,
        .len = graph_index_json.len,
    }));

    {
        const resolvers = try asHandle(handle).?.db.listResolvers(alloc);
        defer {
            for (resolvers) |*cfg| cfg.deinit(alloc);
            alloc.free(resolvers);
        }
        try std.testing.expectEqual(@as(usize, 1), resolvers.len);
        try std.testing.expectEqualStrings("entities", resolvers[0].name);
        try std.testing.expectEqualStrings("relations_v1", resolvers[0].source_artifact);
        try std.testing.expectEqualStrings("entities_resolution_v1", resolvers[0].resolution_artifact);
        try std.testing.expectEqual(@as(u64, 1), resolvers[0].config_generation);
    }

    // Re-adding the identical index (whatever its own admission outcome)
    // must not duplicate or corrupt the resolver catalog: an unchanged
    // config upserts as a no-op.
    _ = antfly_db_add_index_json(handle, .{
        .ptr = graph_index_json,
        .len = graph_index_json.len,
    });
    {
        const resolvers = try asHandle(handle).?.db.listResolvers(alloc);
        defer {
            for (resolvers) |*cfg| cfg.deinit(alloc);
            alloc.free(resolvers);
        }
        try std.testing.expectEqual(@as(usize, 1), resolvers.len);
    }
}

test "capi lite AddIndexJSON surfaces an unresolvable source_artifact_name as invalid_argument, not internal" {
    // Regression test for the ANTFLY_INTERNAL reported against every
    // `sources`-based dense_vector config: the enrichment catalog's own
    // upstream-reference validation (an `embedding` enrichment naming a
    // `source_artifact_name` with no matching `chunk` enrichment) is a
    // caller config mistake, not a server fault, and must map to
    // ANTFLY_INVALID_ARGUMENT (see `capi/types.zig`'s `mapError`).
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-unresolved-artifact-source");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const vector_index_json =
        \\{"name":"document_vectors","kind":"dense_vector","config_json":"{\"type\":\"embeddings\",\"sources\":[{\"artifact\":\"document_chunk_dense_v1\"}],\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"test-embed\",\"api_url\":\"http://127.0.0.1:1\"},\"enrichments\":[{\"name\":\"document_chunk_dense_v1\",\"kind\":\"embedding\",\"field\":\"text\",\"source_artifact_name\":\"missing_chunks_v1\",\"expected_dims\":3}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_add_index_json(handle, .{
        .ptr = vector_index_json,
        .len = vector_index_json.len,
    }));
}

/// Server table provisioning never calls `db.addIndex` with a caller's raw
/// dense/sparse config: `metadata_table_provisioner.extractIndexConfigJsonForKind`
/// first runs it through `managed_embedder.translateEmbeddingsIndexConfigJson`,
/// which is what attaches the internal `"generator"` section that
/// `index_manager`'s `hasGeneratedEnrichmentTargets`/`appendGeneratedEnrichments`
/// need to ever schedule a document's field for embedding. A native Lite
/// handle calling `db.addIndex` directly skipped that step entirely, so even
/// after `refreshLiteManagedEmbeddingRuntime` wires up a working embedder,
/// the physical index never asks it for anything: `parseDenseConfig` only
/// looks at `field`/`dims`/`metric`/`embedding_name`/`external`, and nothing
/// else marks the index as awaiting generated content. Runs the same
/// translation here so what gets stored via `db.addIndex` matches what the
/// server would have stored. Falls back to the untranslated config on any
/// translation error (for example a managed, non-external index with
/// neither `embedder` nor `chunker` configured) so a Lite caller that never
/// relied on this feature keeps today's permissive, pass-through behavior;
/// `refreshLiteManagedEmbeddingRuntime` is likewise a no-op for such an
/// index.
/// True for a plain embedder-only embeddings config -- no `field`/`template`
/// of its own, and none of the other shapes that mean something different
/// (an artifact-backed consumer, an external/caller-supplied index, or one
/// still carrying its own chunker) -- where defaulting `field` to
/// `"embedding"` is unambiguous. Keeps `litePhysicalIndexConfigJson` from
/// defaulting a config whose author meant something other than "index the
/// stored `embedding` field".
fn needsDefaultEmbeddingField(object: std.json.ObjectMap) bool {
    if (object.get("field") != null) return false;
    if (object.get("template") != null) return false;
    if (object.get("sources") != null) return false;
    if (object.get("embedding_name") != null) return false;
    if (object.get("source_artifact_name") != null) return false;
    if (object.get("chunker") != null) return false;
    if (object.get("external")) |external| {
        if (external == .bool and external.bool) return false;
    }
    return true;
}

fn litePhysicalIndexConfigJson(
    alloc: Allocator,
    kind: db_mod.types.IndexKind,
    name: []const u8,
    config_json: []const u8,
    provider: ?managed_embedder.AntflyProvider,
) ![]u8 {
    if (kind != .dense_vector and kind != .sparse_vector) return try alloc.dupe(u8, config_json);
    const bridged = try liteManagedEmbeddingIndexConfigJson(alloc, kind, config_json);
    defer alloc.free(bridged);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, bridged, .{}) catch
        return try alloc.dupe(u8, config_json);
    // The translator requires `field` (or `template`/an artifact source) on
    // a non-external embeddings config and otherwise bails out before ever
    // resolving dimensions -- before this, a caller relying on the same
    // "embedding" default `field` this function's own post-failure fallback
    // below injects would always take that fallback, which has no way to
    // learn the real vector width and stores an index `db.addIndex` then
    // rejects for a missing `dims`. Inject the default proactively so
    // translation actually runs and probes the configured embedder (local or
    // remote) for its output width instead of falling back before trying.
    if (parsed.value == .object and needsDefaultEmbeddingField(parsed.value.object)) {
        parsed.value.object.put(arena, "field", .{ .string = "embedding" }) catch
            return try alloc.dupe(u8, config_json);
    }
    // `provider` must be threaded through here too: an embedder with no
    // `api_url` translates to a durable `"antfly:embedded"` semantic
    // producer identity, and the translator rejects that identity outright
    // when no embedded provider is attached to validate it against.
    return managed_embedder.translateEmbeddingsIndexConfigJsonWithOptions(alloc, name, parsed.value, .{ .antfly_provider = provider }) catch {
        // A translation failure (for example dimension auto-detection
        // requiring a live round trip this call cannot make) must not turn
        // into a hard AddIndex error. Fall back to the bridged config, but
        // `index_manager.parseDenseConfig` hard-requires `field` on every
        // dense/sparse entry -- callers who omit it entirely (relying on
        // translation to supply the artifact-storage default) would
        // otherwise fail `db.addIndex` outright instead of landing in the
        // same "no managed producer configured" no-op state as any other
        // untranslatable config.
        if (parsed.value == .object and parsed.value.object.get("field") == null) {
            parsed.value.object.put(arena, "field", .{ .string = "embedding" }) catch
                return try alloc.dupe(u8, config_json);
            return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
        }
        return try alloc.dupe(u8, config_json);
    };
}

/// Rebuilds the DB's managed embedding/chunking/extraction runtime from the
/// currently declared indexes and enrichments. Provider "antfly" producers
/// with no `api_url` route through the Lite handle's embedded inference
/// provider when one exists (see `startLiteEmbeddedInference`); producers
/// that carry their own `api_url` call that remote Antfly inference service
/// directly and work fine with a null local provider. Scoped to the native
/// profile: hosted Lite handles are reconciled by their owning process and
/// non-Lite (storage-owner) handles are reconciled through
/// `configureStorageKernelOwnerDb` instead. Includes every declared index
/// kind (not just dense/sparse vector) plus standalone enrichments so a
/// graph index's asset-producer extractor and any chunk enrichments are
/// discovered the same way `indexesJsonNeedsAssetProducer` /
/// `indexesJsonHasGeneratedEnrichment` discover them for the server. A
/// database with neither a local provider nor any producer `api_url`
/// resolves an empty producer set, which is a safe no-op: pending work stays
/// visible and neither open nor AddIndex/AddEnrichment errors.
/// Builds the merged `{"<index_name>":<bridged_config_json>, ...}` blob both
/// `refreshLiteManagedEmbeddingRuntime` (write-time enrichment wiring) and
/// `LiteSemanticResolver` (query-time `semantic_search` embedding) feed to
/// `managed_embedder.zig`. Every index kind is included, not just
/// dense_vector/sparse_vector: `indexesJsonNeedsAssetProducer`/
/// `indexesJsonHasGeneratedEnrichment` recursively scan the whole merged
/// object for a nested `"kind":"asset","producer_json":...}` object (see
/// examples/dogfood's knowledgeGraphIndexJSON, which declares the graph
/// index's extractor exactly that way, under an "artifact" key inside the
/// graph index's own config), so a graph/full_text/algebraic index must not
/// be filtered out here even though `managed_embedder.zig`'s embedder
/// scanner only ever recognizes a dense_vector/sparse_vector entry.
/// `liteManagedEmbeddingIndexConfigJson` passes every other kind through
/// unchanged.
///
/// Also appends every *standalone* catalog enrichment from `db.listEnrichments`
/// -- one registered directly through `antfly_db_add_enrichment_json` with no
/// index nesting the same declaration in its own config (see
/// `registerLiteIndexEnrichments`). Without this, a `kind:"asset"` extractor
/// or a `kind:"chunk"` enrichment added standalone is accepted into the
/// catalog (`db.addEnrichment` validates and stores it) but never gets an
/// asset producer or chunk provider wired up: `indexesJsonNeedsAssetProducer`/
/// `indexesJsonHasGeneratedEnrichment` only ever saw the index catalog, so a
/// document's pending generated-enrichment work for that name stays "accepted"
/// forever with nothing servicing it (a stall, surfaced as
/// `error.RunUntilIdleNoProgress` from `antfly_db_run_until_idle`). Each
/// standalone entry is appended under a `"$enrichment:<kind>:<name>"` key --
/// reserved so it cannot collide with a real index name -- as an object shaped
/// `{"kind":<kind>,"producer_json":<...>}` (only when non-empty), which is
/// exactly the shape the two scanners above already recognize wherever it
/// appears in the merged tree. `managed_embedder.zig`'s own scanners
/// (`parseManagedEmbeddingEntry`, `addArtifactBackedManagedEmbeddingEntries`)
/// only ever look at top-level entries carrying `"type":"embeddings"`, and
/// this shape carries no `"type"` or `"enrichments"` key, so it is inert to
/// them and to `LiteSemanticResolver`'s query-time resolution. Caller owns the
/// returned slice.
fn liteMergedIndexesJsonAlloc(handle: *Handle) ![]u8 {
    const alloc = handle.alloc;
    const configs = try handle.db.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, configs);
    const enrichments = try handle.db.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(alloc);
    try buf.append(alloc, '{');
    var wrote_any = false;
    for (configs) |cfg| {
        if (wrote_any) try buf.append(alloc, ',');
        wrote_any = true;
        const escaped_name = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(cfg.name, .{})});
        defer alloc.free(escaped_name);
        try buf.appendSlice(alloc, escaped_name);
        try buf.append(alloc, ':');
        const entry_config_json = try liteManagedEmbeddingIndexConfigJson(alloc, cfg.kind, cfg.config_json);
        defer alloc.free(entry_config_json);
        try buf.appendSlice(alloc, entry_config_json);
    }
    for (enrichments) |cfg| {
        if (wrote_any) try buf.append(alloc, ',');
        wrote_any = true;
        const merged_key = try std.fmt.allocPrint(alloc, "$enrichment:{s}:{s}", .{ @tagName(cfg.kind), cfg.name });
        defer alloc.free(merged_key);
        const escaped_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(merged_key, .{})});
        defer alloc.free(escaped_key);
        try buf.appendSlice(alloc, escaped_key);
        try buf.append(alloc, ':');
        const entry_json = try liteEnrichmentCatalogEntryJsonAlloc(alloc, cfg);
        defer alloc.free(entry_json);
        try buf.appendSlice(alloc, entry_json);
    }
    try buf.append(alloc, '}');
    return try buf.toOwnedSlice(alloc);
}

/// Builds the `{"kind":<kind>,"producer_json":<...>}`-shaped object
/// `liteMergedIndexesJsonAlloc` nests under each standalone catalog
/// enrichment's reserved `"$enrichment:<kind>:<name>"` key. `producer_json`
/// is included only for an `asset` enrichment that carries one (the field
/// `objectIsModelBackedAssetEnrichment` inspects); `kind:"chunk"` needs no
/// further fields since `jsonValueHasGeneratedEnrichment` treats any
/// `"kind":"chunk"` object as a generated-enrichment marker regardless of
/// its other fields. A standalone `embedding` enrichment (always paired with
/// an owning dense/sparse index's own `"type":"embeddings"` config, already
/// merged in above) carries neither marker and is included only for listing
/// symmetry; it is inert to every scanner.
fn liteEnrichmentCatalogEntryJsonAlloc(alloc: Allocator, cfg: db_mod.types.EnrichmentConfig) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(alloc);
    const kind_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(@tagName(cfg.kind), .{})});
    defer alloc.free(kind_json);
    try buf.appendSlice(alloc, "{\"kind\":");
    try buf.appendSlice(alloc, kind_json);
    if (cfg.producer_json.len > 0) {
        const producer_json_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(cfg.producer_json, .{})});
        defer alloc.free(producer_json_json);
        try buf.appendSlice(alloc, ",\"producer_json\":");
        try buf.appendSlice(alloc, producer_json_json);
    }
    try buf.append(alloc, '}');
    return try buf.toOwnedSlice(alloc);
}

test "capi lite merged indexes JSON discovers a standalone asset extractor and chunk enrichment with no owning index" {
    // Regression test for db.zig:1091 (pre-fix): `liteMergedIndexesJsonAlloc`
    // only read `handle.db.listIndexes`, so a `kind:"asset"` extractor or a
    // `kind:"chunk"` enrichment registered directly through
    // `antfly_db_add_enrichment_json` -- with no index nesting the same
    // declaration in its own config (see `registerLiteIndexEnrichments`) --
    // was accepted into the catalog but invisible to
    // `local_write.indexesJsonNeedsAssetProducer`/`indexesJsonHasGeneratedEnrichment`.
    // `refreshLiteManagedEmbeddingRuntime` always resolved an empty producer
    // set for it, so `ManagedDbEnrichmentSet.enabled()` stayed false and the
    // enrichment runtime never serviced that name's pending work at all.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-standalone-enrichment-discovery");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    // Standalone `chunk` enrichment: no index anywhere nests this declaration.
    const chunk_enrichment_json =
        \\{"name":"standalone_chunks_v1","kind":"chunk","field":"body","chunk_size":64,"chunk_overlap":8}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = chunk_enrichment_json,
        .len = chunk_enrichment_json.len,
    }));

    // Standalone `asset` enrichment with a model-backed (non-"copy") extractor
    // producer: also nested nowhere.
    const asset_enrichment_json =
        \\{"name":"standalone_extract_v1","kind":"asset","field":"body","producer_json":"{\"type\":\"extractor\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-extract\"}}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = asset_enrichment_json,
        .len = asset_enrichment_json.len,
    }));

    const owned_handle = asHandle(handle).?;
    const merged_json = try liteMergedIndexesJsonAlloc(owned_handle);
    defer std.heap.c_allocator.free(merged_json);

    try std.testing.expect(std.mem.indexOf(u8, merged_json, "$enrichment:chunk:standalone_chunks_v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, merged_json, "$enrichment:asset:standalone_extract_v1") != null);

    // Before the fix these both returned false: neither scanner ever saw a
    // "kind":"asset"/"chunk" object anywhere in the merged JSON.
    try std.testing.expect(try local_write.indexesJsonHasGeneratedEnrichment(alloc, merged_json));
    try std.testing.expect(try local_write.indexesJsonNeedsAssetProducer(alloc, merged_json));
}

test "capi lite run until idle drains a standalone chunk enrichment with no owning index" {
    // End-to-end reproduction of the same gap: before the fix, a standalone
    // `kind:"chunk"` catalog enrichment left `generated=false` in
    // `local_write.createManagedDbEnrichments`'s scan of the merged JSON, so
    // `ManagedDbEnrichmentSet.enabled()` (dense/sparse/asset_runtime all null,
    // `generated` false) stayed false and `refreshLiteManagedEmbeddingRuntime`
    // never created an enrichment runtime at all. A document's pending chunk
    // work for that name was accepted (the catalog entry validates and
    // stores) but nothing ever serviced it, so `antfly_db_run_until_idle`
    // would return `.stalled` (`error.RunUntilIdleNoProgress`) instead of
    // draining. A fixed-size, non-semantic chunker (`chunk_size`/
    // `chunk_overlap`, no `chunker_json`) needs no embedder or extractor
    // provider at all (`chunker_mod.chunkText` in enrichment_runtime.zig), so
    // this reproduces and proves the fix end to end without any local model.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-standalone-chunk-drain");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const enrichment_json =
        \\{"name":"standalone_chunks_v1","kind":"chunk","field":"body","chunk_size":16,"chunk_overlap":4}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = enrichment_json,
        .len = enrichment_json.len,
    }));

    const batch_json = "{\"inserts\":{\"doc:capi-standalone-chunk\":{\"body\":\"antfly lite chunks this document body text into overlapping windows for later retrieval\"}},\"sync_level\":\"write\"}";
    var batch_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_out));
    defer freeRawBuffer(batch_out.ptr, batch_out.len);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle));

    const owned_handle = asHandle(handle).?;
    const drained = owned_handle.db.pendingWorkStats();
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.error_count);
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.fatal_error_count);
    try std.testing.expect(!drained.enrichment.stalled);
    try std.testing.expectEqual(drained.enrichment.target_sequence, drained.enrichment.applied_sequence);
    try std.testing.expect(drained.enrichment.target_sequence > 0);
}

fn refreshLiteManagedEmbeddingRuntime(handle: *Handle) !void {
    if (handle.lite_profile != .native) return;
    // A read-only/status-only handle has nothing to reconcile toward, and
    // `db.reconfigureEnrichmentRuntime` unconditionally fails with
    // `error.ReadOnly` on one -- before it even looks at whether there is
    // anything to configure. Every open of an already-written native Lite
    // database (query_readonly, status_only) would otherwise fail outright.
    if (!liteOpenModeCanWrite(handle.open_mode)) return;
    const provider = handle.liteAntflyProvider();
    const alloc = handle.alloc;
    const merged_json = try liteMergedIndexesJsonAlloc(handle);
    defer alloc.free(merged_json);

    // Not `local_write.reconfigureManagedDbEnrichmentRuntime` directly: that
    // helper derives `enable_without_producers` purely from
    // `indexesJsonHasGeneratedEnrichment`'s scan of the *index* catalog's own
    // config shape (an inline `"kind":"chunk"`/`"asset"` object, or an
    // `"embeddings"` config). A full-text index that references a chunk
    // enrichment by name -- `{"chunk_name":"..."}`, the shape
    // `antfly_db_add_index_json` stores -- carries no such literal marker, so
    // the scan misses it even though a caller that opened this handle with
    // `generated_enrichment_replay` explicitly asked to resume exactly that
    // pending work. This function runs unconditionally after every open,
    // addIndex, and addEnrichment, so without preserving that intent here it
    // silently tears down and never rebuilds the runtime
    // `replayGeneratedEnrichmentsFromStoredDocs` depends on the very first
    // time this handle reconciles anything.
    var enrichments = try local_write.createManagedDbEnrichments(
        alloc,
        merged_json,
        handle.db.backend_runtime,
        provider,
        null,
        null,
        "",
        null,
        null,
    );
    defer enrichments.deinit(alloc);
    var cfg = enrichments.takeConfig();
    cfg.enable_without_producers = cfg.enable_without_producers or handle.lite_generated_enrichment_replay;
    try handle.db.reconfigureEnrichmentRuntime(cfg);
}

fn closeHandle(handle: *Handle) void {
    const storage_owner_context = handle.storage_owner_context;
    const storage_owner_transaction_recovery = handle.storage_owner_transaction_recovery;
    const storage_owner_runtime_hooks = handle.storage_owner_runtime_hooks;
    if (handle.owned_lite_backend != null and liteOpenModeCanWrite(handle.open_mode)) {
        handle.db.sync(true) catch {};
        handle.db.syncIndexes(true) catch {};
    }
    handle.db.close();
    stopLiteEmbeddedInference(handle);
    if (storage_owner_transaction_recovery) |recovery| {
        recovery.deinit();
        handle.alloc.destroy(recovery);
    }
    if (storage_owner_runtime_hooks) |runtime_hooks| handle.alloc.destroy(runtime_hooks);
    if (handle.owned_lite_backend) |*backend| {
        backend.deinit();
    }
    if (handle.storage_owner_path) |path| handle.alloc.free(path);
    if (handle.storage_owner_table_name) |table_name| handle.alloc.free(table_name);
    handle.alloc.destroy(handle);
    if (storage_owner_context) |context| context.release();
}

fn liteOpenModeCanWrite(open_mode: db_mod.OpenOptions.OpenMode) bool {
    return switch (open_mode) {
        .writer, .writer_no_replay => true,
        else => false,
    };
}

fn currentIdentityReadGenerationForHandle(handle: *Handle, requested: ?u64) !u64 {
    return try handle.db.currentIdentityReadGenerationForRequest(requested);
}

fn stampSearchRequestIdentityGeneration(handle: *Handle, req: *db_mod.types.SearchRequest) !void {
    req.identity_read_generation = try currentIdentityReadGenerationForHandle(handle, req.identity_read_generation);
}

/// Optimistic attempts before `runAtStampedGeneration` blocks writers.
const stamped_generation_optimistic_attempts = 4;

/// Stamps `req` with the current identity generation and runs `query.run`
/// against it. Reads run concurrently with writes, so a batch can commit
/// between the stamp and the executor's re-check, which then rejects the
/// stale stamp with IdentityReadGenerationChanged. When the caller did not pin
/// a generation, restamp and retry, as the server's public query path does.
/// The last attempt holds the handle's write mutex so no write can intervene,
/// which bounds retries under sustained writes. A caller-pinned generation
/// that has gone stale is returned as an error without retrying.
fn runAtStampedGeneration(
    handle: *Handle,
    req: *db_mod.types.SearchRequest,
    query: anytype,
) !@TypeOf(query).Result {
    return runAtStampedGenerationWithOptions(handle, req, query, .{});
}

const StampedGenerationOptions = struct {
    /// Run the readable-lease hook for the stamped request. Paths whose export
    /// already ran a request-specific hook (dense search) turn this off.
    prepare: bool = true,
};

fn runAtStampedGenerationWithOptions(
    handle: *Handle,
    req: *db_mod.types.SearchRequest,
    query: anytype,
    comptime options: StampedGenerationOptions,
) !@TypeOf(query).Result {
    const pinned = req.identity_read_generation;
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        const block_writers = pinned == null and attempt == stamped_generation_optimistic_attempts;
        const last = pinned != null or block_writers;
        // Same order as write exports (api_lock shared, then write_mutex), so
        // this cannot deadlock against them.
        if (block_writers) handle.write_mutex.lockUncancelable(handleLockIo());
        defer if (block_writers) handle.write_mutex.unlock(handleLockIo());
        req.identity_read_generation = pinned;
        try stampSearchRequestIdentityGeneration(handle, req);
        if (options.prepare) try handle.prepareSearchRequest(req.*);
        return query.run(handle, req.*) catch |err| {
            if (err == error.IdentityReadGenerationChanged and !last) continue;
            return err;
        };
    }
}

const LocalSearchQuery = struct {
    const Result = db_mod.types.SearchResult;
    fn run(_: LocalSearchQuery, handle: *Handle, req: db_mod.types.SearchRequest) !Result {
        return executeLocalSearch(handle, req);
    }
};

fn executeLocalSearch(handle: *Handle, req: db_mod.types.SearchRequest) !db_mod.types.SearchResult {
    if (comptime capi_build_options.linked_storage) {
        const request_json = try table_reads_api.encodeStorageKernelQueryRequest(handle.alloc, req);
        defer handle.alloc.free(request_json);
        var failure: kernel_owner_abi.FailureIdentity = .{};
        var cancellation = req.cancellation;
        const response = try local_query_client.executeJsonAlloc(
            handle.alloc,
            @ptrCast(&handle.db),
            "docs",
            request_json,
            .internal,
            antfly.local_query_controls.executionOptions(req),
            req.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
            &failure,
        );
        defer handle.alloc.free(response.json);
        var result = table_reads_api.parseStorageKernelSearchResult(handle.alloc, response.json) catch |err| {
            std.log.err("local query returned an invalid response wire error={s}", .{@errorName(err)});
            return error.InvalidBoundaryQueryResponse;
        };
        result.identity_read_generation = response.identity_read_generation;
        return result;
    }
    return try handle.db.search(handle.alloc, req);
}

fn cancellationTokenRequested(ctx: ?*anyopaque) callconv(.c) u8 {
    const token: *const db_mod.types.CancellationToken = @ptrCast(@alignCast(ctx orelse return 0));
    return @intFromBool(token.isCancelled());
}

const ReadableLeaseHookFn = *const fn (
    ctx: ?*anyopaque,
    group_id: u64,
    request_ctx_ptr: ?[*]const u8,
    request_ctx_len: usize,
) callconv(.c) capi.ErrorCode;

const ReadableLeaseHook = struct {
    group_id: u64,
    callback_ctx: ?*anyopaque,
    callback: ReadableLeaseHookFn,

    fn requester(self: *const ReadableLeaseHook) raft_mod.ReadSafetyBarrier {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .wait_read_safe = waitReadSafe,
            },
        };
    }

    fn featureReads(self: *const ReadableLeaseHook) raft_mod.FeatureReads {
        return raft_mod.FeatureReads.init(self.requester());
    }

    fn waitReadSafe(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *ReadableLeaseHook = @ptrCast(@alignCast(ptr));
        const code = self.callback(
            self.callback_ctx,
            group_id,
            if (request_ctx.len > 0) request_ctx.ptr else null,
            request_ctx.len,
        );
        switch (code) {
            .ok => {},
            .invalid_argument => return error.InvalidArgument,
            .not_found => return error.NotFound,
            .version_conflict => return error.VersionConflict,
            .intent_conflict => return error.IntentConflict,
            .txn_not_found => return error.TxnNotFound,
            .busy => return error.WouldBlock,
            .outcome_unknown => return error.DurabilityOutcomeUnknown,
            .unsupported => return error.UnsupportedOperation,
            .stalled => return error.Stalled,
            .internal => return error.Internal,
        }
    }
};

/// Resolves a caller's handle id without entering it. Exports go through
/// `enterHandle` instead; this is for close and for internal callers (the
/// storage-owner ABI, tests) that do not race close.
fn asHandle(ptr: ?*anyopaque) ?*Handle {
    const id = HandleRegistry.decode(ptr) orelse return null;
    const slot = handle_registry.slotFor(id.index) orelse return null;
    if (slot.state.load(.acquire) >> 1 != id.generation) return null;
    return slot.handle.load(.acquire);
}

/// Handles given to callers are ids naming a registry slot plus a
/// generation, not `*Handle` pointers. Slots live in chunks that are never
/// freed, so any handle value a caller passes, including one for a handle
/// closed on another thread a moment ago, dereferences valid memory:
/// entering a stale or closing generation fails with
/// ANTFLY_INVALID_ARGUMENT instead of touching a freed Handle, closing one
/// is a no-op, and a reused slot never matches an old id.
///
/// Handle values must also be safe for bindings to hold in pointer-typed
/// fields: Go's garbage collector throws on an `unsafe.Pointer` that lands
/// in its heap arenas without naming a live object, and on values below
/// 4096. So on 64-bit POSIX targets each id is encoded as an address inside
/// a PROT_NONE reservation this process owns and never touches: a real,
/// unique address that no allocator can ever return.
const HandleRegistry = struct {
    const chunk_len = 256;
    const index_bits = 20;
    const max_slots = 1 << index_bits;
    const max_chunks = max_slots / chunk_len;
    /// Handle values are 8-byte aligned offsets into the reservation.
    const stride_shift = 3;
    const reserve_address_space = @bitSizeOf(usize) == 64 and switch (builtin.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .ios => true,
        else => false,
    };

    const Slot = struct {
        /// `generation << 1 | closing`. The slot is open for `generation`
        /// exactly when the closing bit is clear.
        state: std.atomic.Value(u64) = .init(0),
        /// Calls that have entered, or are trying to, for any generation.
        active: std.atomic.Value(u32) = .init(0),
        handle: std.atomic.Value(?*Handle) = .init(null),
        next_free: u32 = 0,
    };

    const Id = struct {
        index: u32,
        generation: u64,
    };

    chunks: [max_chunks]std.atomic.Value(?*[chunk_len]Slot) = @splat(.init(null)),
    mutex: std.atomic.Mutex = .unlocked,
    slot_count: u32 = 0,
    free_head: ?u32 = null,
    /// Start of the id reservation (0 until the first registration, or on
    /// targets that use plain integer ids) and the generation width it fits.
    base: std.atomic.Value(usize) = .init(0),
    generation_bits: u6 = 0,

    fn generationMask(self: *const HandleRegistry) u64 {
        return (@as(u64, 1) << self.generation_bits) - 1;
    }

    /// Reserves the id address space on first use. Called with `mutex` held.
    fn ensureIdSpace(self: *HandleRegistry) !void {
        if (self.generation_bits != 0) return;
        if (comptime !reserve_address_space) {
            self.generation_bits = @bitSizeOf(usize) - index_bits - 1;
            return;
        }
        // Prefer 17 generation bits (1 TiB of address space, no memory);
        // step down if the platform limits reservations.
        for ([_]u6{ 17, 13, 9 }) |bits| {
            const len = @as(usize, 1) << (index_bits + bits + stride_shift);
            const region = std.posix.mmap(null, len, .{}, .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .NORESERVE = true }, -1, 0) catch continue;
            self.generation_bits = bits;
            self.base.store(@intFromPtr(region.ptr), .release);
            return;
        }
        return error.OutOfMemory;
    }

    fn encode(self: *const HandleRegistry, id: Id) *anyopaque {
        const offset = (id.generation << index_bits | id.index);
        if (comptime !reserve_address_space) {
            // Plain ids; index + 1 keeps the value non-null.
            return @ptrFromInt(@as(usize, @intCast(offset + 1)));
        }
        return @ptrFromInt(self.base.load(.acquire) + (@as(usize, @intCast(offset)) << stride_shift));
    }

    fn decode(ptr: ?*anyopaque) ?Id {
        const raw = @intFromPtr(ptr orelse return null);
        const offset: u64 = if (comptime !reserve_address_space) blk: {
            break :blk @as(u64, raw) - 1;
        } else blk: {
            const base = handle_registry.base.load(.acquire);
            if (base == 0 or raw < base) return null;
            const delta = raw - base;
            if (delta & ((1 << stride_shift) - 1) != 0) return null;
            const offset = delta >> stride_shift;
            if (offset >> index_bits > handle_registry.generationMask()) return null;
            break :blk offset;
        };
        return .{
            .index = @intCast(offset & (max_slots - 1)),
            .generation = offset >> index_bits,
        };
    }

    fn slotFor(self: *HandleRegistry, index: u32) ?*Slot {
        const chunk_index = index / chunk_len;
        if (chunk_index >= max_chunks) return null;
        const chunk = self.chunks[chunk_index].load(.acquire) orelse return null;
        return &chunk[index % chunk_len];
    }

    /// Publishes `handle` and returns the id callers hold.
    fn register(self: *HandleRegistry, handle: *Handle) !*anyopaque {
        antfly.platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try self.ensureIdSpace();
        const index = if (self.free_head) |free| blk: {
            self.free_head = if (self.slotFor(free).?.next_free == 0) null else self.slotFor(free).?.next_free - 1;
            break :blk free;
        } else blk: {
            const index = self.slot_count;
            const chunk_index = index / chunk_len;
            if (chunk_index >= max_chunks) return error.OutOfMemory;
            if (self.chunks[chunk_index].load(.acquire) == null) {
                const chunk = try std.heap.page_allocator.create([chunk_len]Slot);
                chunk.* = @splat(.{});
                self.chunks[chunk_index].store(chunk, .release);
            }
            self.slot_count += 1;
            break :blk index;
        };
        const slot = self.slotFor(index).?;
        slot.handle.store(handle, .release);
        const generation = slot.state.load(.acquire) >> 1;
        return self.encode(.{ .index = index, .generation = generation });
    }

    /// Claims the slot for close. Returns the handle to free, or null when
    /// the id is stale or another close already claimed it. Waits for every
    /// call that entered (or is backing out) to leave first.
    fn beginClose(self: *HandleRegistry, ptr: ?*anyopaque) ?struct { *Handle, Id } {
        const id = decode(ptr) orelse return null;
        const slot = self.slotFor(id.index) orelse return null;
        if (slot.state.cmpxchgStrong(id.generation << 1, id.generation << 1 | 1, .seq_cst, .seq_cst) != null) return null;
        // A call that counted itself before the closing bit was set may still
        // be queued behind a handle lock, so poll rather than taking the lock:
        // yield first, then back off so a long search does not spin a core.
        var spins: u32 = 0;
        while (slot.active.load(.seq_cst) != 0) : (spins +|= 1) {
            if (spins < 64) {
                std.Thread.yield() catch {};
            } else {
                handleLockIo().sleep(.fromMicroseconds(500), .awake) catch {};
            }
        }
        const handle = slot.handle.swap(null, .acq_rel) orelse return null;
        return .{ handle, id };
    }

    /// Releases a slot claimed by `beginClose` after its handle is freed.
    fn finishClose(self: *HandleRegistry, id: Id) void {
        const slot = self.slotFor(id.index).?;
        antfly.platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (id.generation >= self.generationMask()) {
            // The slot has used every generation an id can encode. Wrapping
            // would let an old id match a future handle, so retire it: the
            // state stays claimed (closing bit set), which no id can enter or
            // close, and the slot never returns to the free list.
            return;
        }
        slot.state.store((id.generation + 1) << 1, .release);
        slot.next_free = if (self.free_head) |free| free + 1 else 0;
        self.free_head = id.index;
    }
};

var handle_registry: HandleRegistry = .{};

/// Registers a newly opened handle, closing it if registration fails. Only for
/// callers that own `handle` outright and have no cleanup of their own left;
/// storageOwnerOpen registers directly so its defers stay the only cleanup.
fn publishHandle(handle: *Handle) !*anyopaque {
    return handle_registry.register(handle) catch |err| {
        closeHandle(handle);
        return err;
    };
}

/// Closes a handle id: rejects new calls, drains entered ones, frees it.
/// Safe for stale ids and concurrent or repeated closes.
fn closeHandleId(ptr: ?*anyopaque) void {
    const handle, const id = handle_registry.beginClose(ptr) orelse return;
    closeHandle(handle);
    handle_registry.finishClose(id);
}

/// Restore destination options for tests that restore into .aflite files.
const lite_restore_options = capi.OpenOptions{ .storage_kind = capi.storage_kind_lite };

/// Tests that build a Handle on the stack register it to get a caller id,
/// then retire the id without freeing the Handle.
fn registerTestHandle(handle: *Handle) !*anyopaque {
    return handle_registry.register(handle);
}

fn unregisterTestHandle(ptr: *anyopaque) void {
    _, const id = handle_registry.beginClose(ptr) orelse return;
    handle_registry.finishClose(id);
}

/// How an export may overlap with other calls on the same handle.
const HandleAccess = enum {
    /// Pure queries. Any number run in parallel with each other and with a
    /// write; the storage layer serves them from pinned snapshots.
    read,
    /// Document writes, transactions, and storage rewrites (compact, vacuum).
    /// One at a time per handle, concurrent with reads.
    write,
    /// Enrichment drains, replay, and snapshot copies. One at a time per
    /// handle, concurrent with reads and writes, mirroring the background
    /// maintenance workers that already run alongside them.
    maintain,
    /// Schema, index, enrichment, range, and restore changes. Waits for
    /// in-flight calls and blocks new ones until done.
    exclusive,
};

/// Held for the duration of one export call; see `enterHandle`.
const HandleGuard = struct {
    handle: *Handle,
    slot: *HandleRegistry.Slot,
    access: HandleAccess,

    fn leave(self: HandleGuard) void {
        const io = handleLockIo();
        switch (self.access) {
            .read => self.handle.api_lock.unlockShared(io),
            .write => {
                self.handle.write_mutex.unlock(io);
                self.handle.api_lock.unlockShared(io);
            },
            .maintain => {
                self.handle.maintenance_mutex.unlock(io);
                self.handle.api_lock.unlockShared(io);
            },
            .exclusive => self.handle.api_lock.unlock(io),
        }
        _ = self.slot.active.fetchSub(1, .release);
    }
};

/// The handle locks are called from arbitrary foreign threads, so they use
/// the process-wide threaded Io, whose waits block the calling OS thread.
fn handleLockIo() std.Io {
    return std.Options.debug_io;
}

/// Entry point for every export that takes a DB handle. Returns null for a
/// null handle or one that is being closed. Must be taken exactly once per
/// export, at entry: the lock is not reentrant, so internal helpers must not
/// call it again.
fn enterHandle(ptr: ?*anyopaque, access: HandleAccess) ?HandleGuard {
    const id = HandleRegistry.decode(ptr) orelse return null;
    const slot = handle_registry.slotFor(id.index) orelse return null;
    // Count the call before checking the slot state so close either sees this
    // call and waits for it, or this call sees closing (or a newer
    // generation) and backs out. Each side stores then loads the other's
    // variable, so both need seq_cst: weaker orderings let both loads miss
    // both stores. The slot itself is never freed, so this is safe even if
    // the handle was closed before we got here.
    _ = slot.active.fetchAdd(1, .seq_cst);
    if (slot.state.load(.seq_cst) != id.generation << 1) {
        _ = slot.active.fetchSub(1, .release);
        return null;
    }
    const handle = slot.handle.load(.acquire) orelse {
        _ = slot.active.fetchSub(1, .release);
        return null;
    };
    const io = handleLockIo();
    switch (access) {
        .read => handle.api_lock.lockSharedUncancelable(io),
        .write => {
            handle.api_lock.lockSharedUncancelable(io);
            handle.write_mutex.lockUncancelable(io);
        },
        .maintain => {
            handle.api_lock.lockSharedUncancelable(io);
            handle.maintenance_mutex.lockUncancelable(io);
        },
        .exclusive => handle.api_lock.lockUncancelable(io),
    }
    return .{ .handle = handle, .slot = slot, .access = access };
}

fn cleanupTestDir(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

fn cleanupTestFile(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteFile(io_impl.io(), path) catch {};
}

fn testPathExists(path: []const u8) bool {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn beginWithIdAndParticipants(
    handle: *Handle,
    txn_id: transactions_mod.TxnId,
    timestamp_ns: u64,
    participants_ptr: ?[*]const capi.Slice,
    participant_count: usize,
) !void {
    const participants = try handle.alloc.alloc([]const u8, participant_count);
    defer handle.alloc.free(participants);
    for (participants, 0..) |*entry, i| {
        entry.* = participants_ptr.?[i].bytes();
    }
    _ = try handle.db.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, participants);
}

fn writeIntentsInternal(
    handle: *Handle,
    txn_id: transactions_mod.TxnId,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
) !void {
    var writes = try handle.alloc.alloc(db_mod.types.TransactionWrite, write_count);
    defer handle.alloc.free(writes);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(handle.alloc);
    var predicates = try handle.alloc.alloc(db_mod.types.TransactionVersionPredicate, predicate_count);
    defer handle.alloc.free(predicates);

    var write_len: usize = 0;
    for (0..write_count) |i| {
        const src = writes_ptr.?[i];
        if (src.is_delete) {
            try deletes.append(handle.alloc, src.key.bytes());
        } else {
            writes[write_len] = .{
                .key = src.key.bytes(),
                .value = src.value.bytes(),
            };
            write_len += 1;
        }
    }
    for (0..predicate_count) |i| {
        predicates[i] = .{
            .key = predicates_ptr.?[i].key.bytes(),
            .expected_version = predicates_ptr.?[i].expected_version,
        };
    }

    try handle.db.writeTransaction(txn_id, .{
        .writes = writes[0..write_len],
        .deletes = deletes.items,
        .predicates = predicates,
    });
}

fn batchInternal(
    handle: *Handle,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
    timestamp_ns: u64,
    sync_level: u8,
) !void {
    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer writes.deinit(handle.alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(handle.alloc);
    var predicates = try handle.alloc.alloc(db_mod.types.TransactionVersionPredicate, predicate_count);
    defer handle.alloc.free(predicates);

    for (0..write_count) |i| {
        const src = writes_ptr.?[i];
        if (src.is_delete) {
            try deletes.append(handle.alloc, src.key.bytes());
        } else {
            try writes.append(handle.alloc, .{
                .key = src.key.bytes(),
                .value = src.value.bytes(),
            });
        }
    }
    for (0..predicate_count) |i| {
        predicates[i] = .{
            .key = predicates_ptr.?[i].key.bytes(),
            .expected_version = predicates_ptr.?[i].expected_version,
        };
    }

    const level: db_mod.types.SyncLevel = switch (sync_level) {
        0 => .write,
        1 => .full_index,
        else => return error.InvalidArgument,
    };

    try handle.db.batch(.{
        .writes = writes.items,
        .deletes = deletes.items,
        .predicates = predicates,
        .timestamp_ns = timestamp_ns,
        .sync_level = level,
    });
}

fn dupBytes(bytes: []const u8) !capi.Buffer {
    if (bytes.len == 0) return .{};
    const out = try std.heap.c_allocator.alloc(u8, bytes.len);
    @memcpy(out, bytes);
    return .{
        .ptr = out.ptr,
        .len = out.len,
    };
}

fn stringifyJson(value: anytype) !capi.Buffer {
    const bytes = try std.fmt.allocPrint(std.heap.c_allocator, "{f}", .{std.json.fmt(value, .{})});
    return .{
        .ptr = bytes.ptr,
        .len = bytes.len,
    };
}

fn dupBase64(alloc: Allocator, bytes: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, bytes);
    return out;
}

fn decodeBase64Alloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, encoded);
    return out;
}

fn parseEnrichmentKind(kind: []const u8) ?db_mod.types.EnrichmentKind {
    if (std.mem.eql(u8, kind, "chunk")) return .chunk;
    if (std.mem.eql(u8, kind, "asset")) return .asset;
    if (std.mem.eql(u8, kind, "embedding")) return .embedding;
    return null;
}

fn graphFreeEdges(alloc: Allocator, edges: []graph_mod.Edge) void {
    // GraphIndex.freeEdges already frees both each edge's owned fields and
    // the slice itself. Freeing `edges` again here double-frees it: harmless
    // for the len==0 case (many allocators no-op an empty-slice free), but a
    // real heap corruption once a query returns actual edges -- see
    // antfly_db_get_edges_json below, the only caller.
    graph_mod.GraphIndex.freeEdges(alloc, edges);
}

fn traversalFreeResults(alloc: Allocator, results: []traversal_mod.TraversalResult) void {
    traversal_mod.freeOwnedResults(alloc, results);
}

const JsonRange = struct {
    start_b64: []u8,
    end_b64: []u8,

    fn init(alloc: Allocator, byte_range: db_mod.types.ByteRange) !JsonRange {
        return .{
            .start_b64 = try dupBase64(alloc, byte_range.start),
            .end_b64 = try dupBase64(alloc, byte_range.end),
        };
    }

    fn deinit(self: *JsonRange, alloc: Allocator) void {
        alloc.free(self.start_b64);
        alloc.free(self.end_b64);
        self.* = undefined;
    }
};

const JsonSplitState = struct {
    phase: u8,
    split_key_b64: []u8,
    new_shard_id: u64,
    started_at: u64,
    original_range_end_b64: []u8,

    fn init(alloc: Allocator, state: db_mod.types.SplitState) !JsonSplitState {
        return .{
            .phase = @intFromEnum(state.phase),
            .split_key_b64 = try dupBase64(alloc, state.split_key),
            .new_shard_id = state.new_shard_id,
            .started_at = state.started_at,
            .original_range_end_b64 = try dupBase64(alloc, state.original_range_end),
        };
    }

    fn deinit(self: *JsonSplitState, alloc: Allocator) void {
        alloc.free(self.split_key_b64);
        alloc.free(self.original_range_end_b64);
        self.* = undefined;
    }
};

const JsonSplitDeltaWrite = struct {
    key_b64: []u8,
    value_b64: []u8,

    fn init(alloc: Allocator, write: db_mod.types.BatchWrite) !JsonSplitDeltaWrite {
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
        };
    }

    fn deinit(self: *JsonSplitDeltaWrite, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        self.* = undefined;
    }
};

const JsonSplitDeltaEntry = struct {
    sequence: u64,
    timestamp: u64,
    writes: []JsonSplitDeltaWrite,
    deletes_b64: [][]u8,

    fn init(alloc: Allocator, entry: db_mod.types.SplitDeltaEntry) !JsonSplitDeltaEntry {
        var writes = try alloc.alloc(JsonSplitDeltaWrite, entry.writes.len);
        errdefer alloc.free(writes);
        var write_count: usize = 0;
        errdefer {
            for (writes[0..write_count]) |*write| write.deinit(alloc);
        }
        for (entry.writes, 0..) |write, i| {
            writes[i] = try JsonSplitDeltaWrite.init(alloc, write);
            write_count += 1;
        }

        var deletes = try alloc.alloc([]u8, entry.deletes.len);
        errdefer alloc.free(deletes);
        var delete_count: usize = 0;
        errdefer {
            for (deletes[0..delete_count]) |item| alloc.free(item);
        }
        for (entry.deletes, 0..) |key, i| {
            deletes[i] = try dupBase64(alloc, key);
            delete_count += 1;
        }

        return .{
            .sequence = entry.sequence,
            .timestamp = entry.timestamp,
            .writes = writes,
            .deletes_b64 = deletes,
        };
    }

    fn deinit(self: *JsonSplitDeltaEntry, alloc: Allocator) void {
        for (self.writes) |*write| write.deinit(alloc);
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes_b64) |item| alloc.free(item);
        if (self.deletes_b64.len > 0) alloc.free(self.deletes_b64);
        self.* = undefined;
    }
};

const JsonIndexConfig = struct {
    name: []const u8,
    kind: []const u8,
    config_json: []const u8,
};

const JsonScanHash = struct {
    id_b64: []u8,
    hash: u64,

    fn init(alloc: Allocator, item: db_mod.types.ScanHash) !JsonScanHash {
        return .{
            .id_b64 = try dupBase64(alloc, item.id),
            .hash = item.hash,
        };
    }

    fn deinit(self: *JsonScanHash, alloc: Allocator) void {
        alloc.free(self.id_b64);
        self.* = undefined;
    }
};

const JsonScanDocument = struct {
    id_b64: []u8,
    json: []const u8,

    fn init(alloc: Allocator, item: db_mod.types.ScanDocument) !JsonScanDocument {
        return .{
            .id_b64 = try dupBase64(alloc, item.id),
            .json = item.json,
        };
    }

    fn deinit(self: *JsonScanDocument, alloc: Allocator) void {
        alloc.free(self.id_b64);
        self.* = undefined;
    }
};

const JsonScanResult = struct {
    hashes: []JsonScanHash,
    documents: []JsonScanDocument,
};

const JsonDBStats = struct {
    doc_count: u64,
    index_count: u32,
    indexes: []JsonDBIndexStats,
    repair_degraded: bool,
    repair_issue_count: u64,
    repair_summary_ready: bool,
    repair_issue_count_estimated: bool,
    enrichment: JsonEnrichmentStats,
    ttl_cleanup: JsonTTLCleanupStats,
    transaction_recovery: JsonTransactionRecoveryStats,
    text_merge: JsonTextMergeStats,
    term_doc_freq_cache_hits: u64,
    term_doc_freq_cache_misses: u64,
};

const JsonDBIndexStats = struct {
    name: []const u8,
    kind: []const u8,
    doc_count: u64,
    term_count: u64,
    edge_count: u64,
    graph_counts_pending: bool,
    node_count: u64,
    repair_degraded: bool,
    repair_issue_count: u64,
    repair_summary_ready: bool,
    repair_issue_count_estimated: bool,
    // Durable per-document generation-outcome coverage (produced / skipped /
    // terminal_failed markers), so an embedded consumer can report honest
    // coverage: documents that failed non-retryably are settled failures,
    // not pending work. Mirrors the server's index-status coverage block.
    coverage_produced_count: u64,
    coverage_skipped_count: u64,
    coverage_terminal_failed_count: u64,
    coverage_summary_ready: bool,
};

const JsonEnrichmentStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
    target_sequence: u64,
    applied_sequence: u64,
    processed_requests: u64,
    error_count: u64,
    retryable_error_count: u64,
    // Durable count of requests parked non-retryably (terminal disposition);
    // per-document terminal state lives in the index coverage counters.
    fatal_error_count: u64,
    retrying: bool,
    worker_failed: bool,
    stalled: bool,
    stall_reason: []const u8,
    skip_by_hash_count: u64,
    skipped_source_count: u64,
    codec_decode_failures: u64,
    dense_artifact_bytes_written: u64,
    sparse_artifact_bytes_written: u64,
    chunk_artifact_bytes_written: u64,
    artifact_bytes_written: u64,
};

const JsonTTLCleanupStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    runs: u64,
    scanned_timestamps: u64,
    deleted_docs: u64,
    last_run_ns: u64,
    error_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
};

const JsonTransactionRecoveryStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
    runs: u64,
    scanned_records: u64,
    auto_aborted: u64,
    resolved_finalized: u64,
    cleaned_records: u64,
    kept_recent_pending: u64,
    deferred_unresolved: u64,
    notification_attempts: u64,
    notification_successes: u64,
    notification_failures: u64,
    last_run_ns: u64,
    error_count: u64,
};

const JsonTextMergeStats = struct {
    enabled: bool,
    active_indexes: u64,
    active_segments: u64,
    max_active_segments_per_index: u64,
    pending_indexes: u64,
    pending_segments: u64,
    pending_bytes: u64,
    in_flight_merges: u64,
    in_flight_segments: u64,
    completed_merges: u64,
    skipped_stale_merges: u64,
    failed_merges: u64,
    quarantined_merges: u64,
    quarantined_segments: u64,
    last_merge_error: db_mod.types.RuntimeErrorName,
    backpressure_events: u64,
    backpressure_ns: u64,
    max_pending_segments: u64,
    max_pending_bytes: u64,
};

const JsonChunkHit = struct {
    id_b64: []u8,
    score: ?f32 = null,
    stored_json: ?[]const u8 = null,
    artifact_ref: ?JsonArtifactRef = null,

    fn init(alloc: Allocator, hit: db_mod.types.ChunkHit) !JsonChunkHit {
        return .{
            .id_b64 = try dupBase64(alloc, hit.id),
            .score = hit.score,
            .stored_json = hit.stored_data,
            .artifact_ref = if (hit.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
        };
    }

    fn deinit(self: *JsonChunkHit, alloc: Allocator) void {
        alloc.free(self.id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

const JsonSearchHit = struct {
    id_b64: []u8,
    score: ?f32 = null,
    stored_json: ?[]const u8 = null,
    artifact_ref: ?JsonArtifactRef = null,
    chunk_hits: []JsonChunkHit = &.{},

    fn init(alloc: Allocator, hit: db_mod.types.SearchHit) !JsonSearchHit {
        var chunk_hits = try alloc.alloc(JsonChunkHit, hit.chunk_hits.len);
        errdefer alloc.free(chunk_hits);
        var count: usize = 0;
        errdefer {
            for (chunk_hits[0..count]) |*item| item.deinit(alloc);
        }
        for (hit.chunk_hits, 0..) |chunk, i| {
            chunk_hits[i] = try JsonChunkHit.init(alloc, chunk);
            count += 1;
        }
        return .{
            .id_b64 = try dupBase64(alloc, hit.id),
            .score = hit.score,
            .stored_json = hit.stored_data,
            .artifact_ref = if (hit.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
            .chunk_hits = chunk_hits,
        };
    }

    fn deinit(self: *JsonSearchHit, alloc: Allocator) void {
        alloc.free(self.id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        for (self.chunk_hits) |*item| item.deinit(alloc);
        if (self.chunk_hits.len > 0) alloc.free(self.chunk_hits);
        self.* = undefined;
    }
};

const JsonSearchResult = struct {
    total_hits: u32,
    identity_read_generation: ?u64 = null,
    hits: []JsonSearchHit,
    graph_results: []JsonGraphSearchResult = &.{},
    aggregations: []JsonSearchAggregationResult = &.{},
};

const JsonAggregateHitsRequest = struct {
    index_name: []const u8 = "",
    hit_ids_b64: []const []const u8 = &.{},
    identity_read_generation: ?u64 = null,
    aggregations: []const JsonSearchAggregationRequest = &.{},
};

const JsonGraphNodeSelectorRequest = struct {
    keys: []const []const u8 = &.{},
    result_ref: []const u8 = "",
    limit: u32 = 0,
};

const JsonGraphQueryRequest = struct {
    name: []const u8,
    type: []const u8,
    index_name: []const u8,
    start_nodes: JsonGraphNodeSelectorRequest,
    target_nodes: ?JsonGraphNodeSelectorRequest = null,
    edge_types: []const []const u8 = &.{},
    direction: []const u8 = "out",
    max_depth: u32 = 3,
    max_results: u32 = 100,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0,
    deduplicate: bool = true,
    include_paths: bool = false,
    weight_mode: []const u8 = "min_hops",
    k: u32 = 1,
};

const JsonNamedGraphInputSetRequest = struct {
    name: []const u8,
    hit_ids_b64: []const []const u8 = &.{},
    total_hits: u32 = 0,
};

const JsonGraphSearchResult = struct {
    name: []u8,
    total_hits: u32,
    identity_read_generation: ?u64 = null,
    nodes: []JsonGraphNode,
    paths: []JsonPath = &.{},
    hits: []JsonSearchHit,

    fn init(alloc: Allocator, result: db_mod.types.GraphSearchResult, identity_read_generation: ?u64) !JsonGraphSearchResult {
        var nodes = try alloc.alloc(JsonGraphNode, result.nodes.len);
        errdefer alloc.free(nodes);
        var node_count: usize = 0;
        errdefer {
            for (nodes[0..node_count]) |*item| item.deinit(alloc);
        }
        for (result.nodes, 0..) |node, i| {
            nodes[i] = try JsonGraphNode.init(alloc, node);
            node_count += 1;
        }

        var paths = try alloc.alloc(JsonPath, result.paths.len);
        errdefer alloc.free(paths);
        var path_count: usize = 0;
        errdefer {
            for (paths[0..path_count]) |*item| item.deinit(alloc);
        }
        for (result.paths, 0..) |path, i| {
            paths[i] = try JsonPath.init(alloc, path);
            path_count += 1;
        }

        var hits = try alloc.alloc(JsonSearchHit, result.hits.len);
        errdefer alloc.free(hits);
        var count: usize = 0;
        errdefer {
            for (hits[0..count]) |*item| item.deinit(alloc);
        }
        for (result.hits, 0..) |hit, i| {
            hits[i] = try JsonSearchHit.init(alloc, hit);
            count += 1;
        }
        return .{
            .name = try alloc.dupe(u8, result.name),
            .total_hits = result.total_hits,
            .identity_read_generation = identity_read_generation,
            .nodes = nodes,
            .paths = paths,
            .hits = hits,
        };
    }

    fn deinit(self: *JsonGraphSearchResult, alloc: Allocator) void {
        alloc.free(self.name);
        for (self.nodes) |*item| item.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.paths) |*item| item.deinit(alloc);
        if (self.paths.len > 0) alloc.free(self.paths);
        for (self.hits) |*item| item.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
};

const JsonSearchAggregationRequest = struct {
    name: []const u8,
    type: []const u8,
    field: []const u8,
    size: i64 = 0,
    interval: f64 = 0,
    calendar_interval: []const u8 = "",
    fixed_interval: []const u8 = "",
    min_doc_count: i64 = 0,
    significance_algorithm: []const u8 = "",
    background_query_type: []const u8 = "",
    background_field: []const u8 = "",
    background_text: []const u8 = "",
    bucket_path: []const u8 = "",
    sort_order: []const u8 = "",
    from: i64 = 0,
    window: i64 = 0,
    gap_policy: []const u8 = "",
    term_prefix: []const u8 = "",
    term_pattern: []const u8 = "",
    ranges: []const JsonNumericRangeRequest = &.{},
    date_ranges: []const JsonDateRangeRequest = &.{},
    distance_ranges: []const JsonDistanceRangeRequest = &.{},
    center_lat: f64 = 0,
    center_lon: f64 = 0,
    distance_unit: []const u8 = "",
    geohash_precision: u8 = 0,
    aggregations: []const JsonSearchAggregationRequest = &.{},
};

const JsonNumericRangeRequest = struct {
    name: []const u8 = "",
    start: ?f64 = null,
    end: ?f64 = null,
};

const JsonDateRangeRequest = struct {
    name: []const u8 = "",
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

const JsonDistanceRangeRequest = struct {
    name: []const u8 = "",
    from: ?f64 = null,
    to: ?f64 = null,
};

const JsonSearchAggregationBucket = struct {
    key_json: []const u8,
    count: i64,
    score: ?f64 = null,
    bg_count: ?i64 = null,
    aggregations: []JsonSearchAggregationResult = &.{},

    fn deinit(self: *JsonSearchAggregationBucket, alloc: Allocator) void {
        alloc.free(self.key_json);
        for (self.aggregations) |*agg| agg.deinit(alloc);
        if (self.aggregations.len > 0) alloc.free(self.aggregations);
        self.* = undefined;
    }
};

const JsonSearchAggregationResult = struct {
    name: []const u8,
    field: []const u8,
    type: []const u8,
    value_json: ?[]const u8 = null,
    metadata_json: ?[]const u8 = null,
    buckets: []JsonSearchAggregationBucket = &.{},

    fn deinit(self: *JsonSearchAggregationResult, alloc: Allocator) void {
        if (self.value_json) |value_json| alloc.free(value_json);
        if (self.metadata_json) |metadata_json| alloc.free(metadata_json);
        for (self.buckets) |*bucket| bucket.deinit(alloc);
        if (self.buckets.len > 0) alloc.free(self.buckets);
        self.* = undefined;
    }
};

fn toAggregationRequest(
    alloc: Allocator,
    requests: []const JsonSearchAggregationRequest,
) ![]aggregations_mod.SearchAggregationRequest {
    const out = try alloc.alloc(aggregations_mod.SearchAggregationRequest, requests.len);
    errdefer alloc.free(out);
    for (requests, 0..) |request, i| {
        const ranges = try alloc.alloc(aggregations_mod.NumericRangeRequest, request.ranges.len);
        errdefer alloc.free(ranges);
        for (request.ranges, 0..) |item, j| {
            ranges[j] = .{ .name = item.name, .start = item.start, .end = item.end };
        }
        const date_ranges = try alloc.alloc(aggregations_mod.DateRangeRequest, request.date_ranges.len);
        errdefer alloc.free(date_ranges);
        for (request.date_ranges, 0..) |item, j| {
            date_ranges[j] = .{ .name = item.name, .start = item.start, .end = item.end };
        }
        const distance_ranges = try alloc.alloc(aggregations_mod.DistanceRangeRequest, request.distance_ranges.len);
        errdefer alloc.free(distance_ranges);
        for (request.distance_ranges, 0..) |item, j| {
            distance_ranges[j] = .{ .name = item.name, .from = item.from, .to = item.to };
        }
        const nested = try toAggregationRequest(alloc, request.aggregations);
        out[i] = .{
            .name = request.name,
            .type = request.type,
            .field = request.field,
            .size = request.size,
            .interval = request.interval,
            .calendar_interval = request.calendar_interval,
            .fixed_interval = request.fixed_interval,
            .min_doc_count = request.min_doc_count,
            .significance_algorithm = request.significance_algorithm,
            .background_query = if (request.background_query_type.len == 0)
                null
            else if (std.mem.eql(u8, request.background_query_type, "match_all"))
                .{ .match_all = {} }
            else if (std.mem.eql(u8, request.background_query_type, "match"))
                .{ .match = .{
                    .field = request.background_field,
                    .text = request.background_text,
                } }
            else if (std.mem.eql(u8, request.background_query_type, "term"))
                .{ .term = .{
                    .field = request.background_field,
                    .term = request.background_text,
                } }
            else
                return error.InvalidArgument,
            .bucket_path = request.bucket_path,
            .sort_order = request.sort_order,
            .from = request.from,
            .window = request.window,
            .gap_policy = request.gap_policy,
            .term_prefix = request.term_prefix,
            .term_pattern = request.term_pattern,
            .ranges = ranges,
            .date_ranges = date_ranges,
            .distance_ranges = distance_ranges,
            .center_lat = request.center_lat,
            .center_lon = request.center_lon,
            .distance_unit = request.distance_unit,
            .geohash_precision = request.geohash_precision,
            .aggregations = nested,
        };
    }
    return out;
}

fn freeAggregationRequests(alloc: Allocator, requests: []const aggregations_mod.SearchAggregationRequest) void {
    for (requests) |request| {
        if (request.ranges.len > 0) alloc.free(request.ranges);
        if (request.date_ranges.len > 0) alloc.free(request.date_ranges);
        if (request.distance_ranges.len > 0) alloc.free(request.distance_ranges);
        freeAggregationRequests(alloc, request.aggregations);
    }
    if (requests.len > 0) alloc.free(requests);
}

fn toJsonAggregationResults(
    alloc: Allocator,
    results: []aggregations_mod.SearchAggregationResult,
) ![]JsonSearchAggregationResult {
    const out = try alloc.alloc(JsonSearchAggregationResult, results.len);
    errdefer alloc.free(out);
    for (results, 0..) |result, i| {
        const buckets = try alloc.alloc(JsonSearchAggregationBucket, result.buckets.len);
        errdefer alloc.free(buckets);
        for (result.buckets, 0..) |bucket, j| {
            buckets[j] = .{
                .key_json = try alloc.dupe(u8, bucket.key_json),
                .count = bucket.count,
                .score = bucket.score,
                .bg_count = bucket.bg_count,
                .aggregations = try toJsonAggregationResults(alloc, bucket.aggregations),
            };
        }
        out[i] = .{
            .name = result.name,
            .field = result.field,
            .type = result.type,
            .value_json = if (result.value_json) |value| try alloc.dupe(u8, value) else null,
            .metadata_json = if (result.metadata_json) |value| try alloc.dupe(u8, value) else null,
            .buckets = buckets,
        };
    }
    return out;
}

const JsonWritePair = struct {
    key_b64: []u8,
    value_b64: []u8,

    fn init(alloc: Allocator, write: db_mod.types.BatchWrite) !JsonWritePair {
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
        };
    }

    fn deinit(self: *JsonWritePair, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        self.* = undefined;
    }
};

fn artifactKindLabel(kind: db_mod.types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk",
        .asset => "asset",
        .embedding => "embedding",
    };
}

const JsonArtifactSourceRef = struct {
    kind: []const u8,
    name: []const u8,
    chunk_id: ?u32 = null,

    fn init(source: db_mod.types.ArtifactSourceRef) JsonArtifactSourceRef {
        return .{
            .kind = artifactKindLabel(source.kind),
            .name = source.name,
            .chunk_id = source.chunk_id,
        };
    }
};

const JsonArtifactRef = struct {
    document_id_b64: []u8,
    name: []const u8,
    kind: []const u8,
    chunk_id: ?u32 = null,
    source: ?JsonArtifactSourceRef = null,

    fn init(alloc: Allocator, artifact_ref: db_mod.types.ArtifactRef) !JsonArtifactRef {
        return .{
            .document_id_b64 = try dupBase64(alloc, artifact_ref.document_id),
            .name = artifact_ref.name,
            .kind = artifactKindLabel(artifact_ref.kind),
            .chunk_id = artifact_ref.chunk_id,
            .source = if (artifact_ref.source) |source| JsonArtifactSourceRef.init(source) else null,
        };
    }

    fn deinit(self: *JsonArtifactRef, alloc: Allocator) void {
        alloc.free(self.document_id_b64);
        self.* = undefined;
    }
};

const JsonArtifactWrite = struct {
    id_b64: []u8,
    value_b64: []u8,
    artifact_ref: JsonArtifactRef,

    fn init(alloc: Allocator, write: db_mod.types.ArtifactWrite) !JsonArtifactWrite {
        return .{
            .id_b64 = try dupBase64(alloc, write.id),
            .value_b64 = try dupBase64(alloc, write.value),
            .artifact_ref = try JsonArtifactRef.init(alloc, write.artifact_ref),
        };
    }

    fn deinit(self: *JsonArtifactWrite, alloc: Allocator) void {
        alloc.free(self.id_b64);
        alloc.free(self.value_b64);
        self.artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

const JsonDenseEnrichmentWrite = struct {
    index_name: []const u8,
    doc_key_b64: []u8,
    artifact_id_b64: ?[]u8 = null,
    artifact_ref: ?JsonArtifactRef = null,
    vector: []const f32,

    fn init(alloc: Allocator, write: db_mod.types.EnrichmentDenseEmbeddingWrite) !JsonDenseEnrichmentWrite {
        return .{
            .index_name = write.index_name,
            .doc_key_b64 = try dupBase64(alloc, write.doc_key),
            .artifact_id_b64 = if (write.artifact_id) |artifact_id| try dupBase64(alloc, artifact_id) else null,
            .artifact_ref = if (write.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
            .vector = write.vector,
        };
    }

    fn deinit(self: *JsonDenseEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.doc_key_b64);
        if (self.artifact_id_b64) |artifact_id_b64| alloc.free(artifact_id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

const JsonSparseEnrichmentWrite = struct {
    index_name: []const u8,
    doc_key_b64: []u8,
    indices: []const u32,
    values: []const f32,

    fn init(alloc: Allocator, write: db_mod.types.EnrichmentSparseEmbeddingWrite) !JsonSparseEnrichmentWrite {
        return .{
            .index_name = write.index_name,
            .doc_key_b64 = try dupBase64(alloc, write.doc_key),
            .indices = write.indices,
            .values = write.values,
        };
    }

    fn deinit(self: *JsonSparseEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.doc_key_b64);
        self.* = undefined;
    }
};

const JsonGraphWrite = struct {
    index_name: []const u8,
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,

    fn init(alloc: Allocator, write: db_mod.types.GraphEdgeWrite) !JsonGraphWrite {
        return .{
            .index_name = write.index_name,
            .source_b64 = try dupBase64(alloc, write.source),
            .target_b64 = try dupBase64(alloc, write.target),
            .edge_type = write.edge_type,
            .weight = write.weight,
            .created_at = write.created_at,
            .updated_at = write.updated_at,
            .metadata_json = write.metadata_json,
        };
    }

    fn deinit(self: *JsonGraphWrite, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        self.* = undefined;
    }
};

const JsonDocumentEnrichmentWrite = struct {
    key_b64: []u8,
    value_b64: []u8,
    target_index_names: [][]const u8,

    fn init(alloc: Allocator, write: db_mod.types.EnrichmentDocumentWrite) !JsonDocumentEnrichmentWrite {
        const target_index_names = try alloc.alloc([]const u8, write.target_index_names.len);
        errdefer alloc.free(target_index_names);
        for (write.target_index_names, 0..) |name, i| target_index_names[i] = name;
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
            .target_index_names = target_index_names,
        };
    }

    fn deinit(self: *JsonDocumentEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        if (self.target_index_names.len > 0) alloc.free(self.target_index_names);
        self.* = undefined;
    }
};

const JsonExtractEnrichmentsResult = struct {
    dense_embeddings: []JsonDenseEnrichmentWrite,
    sparse_embeddings: []JsonSparseEnrichmentWrite,
    graph_writes: []JsonGraphWrite,

    fn deinit(self: *JsonExtractEnrichmentsResult, alloc: Allocator) void {
        for (self.dense_embeddings) |*item| item.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.sparse_embeddings) |*item| item.deinit(alloc);
        if (self.sparse_embeddings.len > 0) alloc.free(self.sparse_embeddings);
        for (self.graph_writes) |*item| item.deinit(alloc);
        if (self.graph_writes.len > 0) alloc.free(self.graph_writes);
        self.* = undefined;
    }
};

const JsonComputeEnrichmentsResult = struct {
    artifact_writes: []JsonArtifactWrite,
    documents: []JsonDocumentEnrichmentWrite,
    dense_embeddings: []JsonDenseEnrichmentWrite,
    failed_keys_b64: [][]u8,

    fn deinit(self: *JsonComputeEnrichmentsResult, alloc: Allocator) void {
        for (self.artifact_writes) |*item| item.deinit(alloc);
        if (self.artifact_writes.len > 0) alloc.free(self.artifact_writes);
        for (self.documents) |*item| item.deinit(alloc);
        if (self.documents.len > 0) alloc.free(self.documents);
        for (self.dense_embeddings) |*item| item.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.failed_keys_b64) |item| alloc.free(item);
        if (self.failed_keys_b64.len > 0) alloc.free(self.failed_keys_b64);
        self.* = undefined;
    }
};

fn buildJsonExtractEnrichmentsResult(
    alloc: Allocator,
    result: db_mod.types.ExtractEnrichmentsResult,
) !JsonExtractEnrichmentsResult {
    var dense_embeddings = try alloc.alloc(JsonDenseEnrichmentWrite, result.dense_embeddings.len);
    var dense_initialized: usize = 0;
    errdefer {
        for (dense_embeddings[0..dense_initialized]) |*item| item.deinit(alloc);
        alloc.free(dense_embeddings);
    }
    for (result.dense_embeddings, 0..) |item, i| {
        dense_embeddings[i] = try JsonDenseEnrichmentWrite.init(alloc, item);
        dense_initialized += 1;
    }

    var sparse_embeddings = try alloc.alloc(JsonSparseEnrichmentWrite, result.sparse_embeddings.len);
    var sparse_initialized: usize = 0;
    errdefer {
        for (sparse_embeddings[0..sparse_initialized]) |*item| item.deinit(alloc);
        alloc.free(sparse_embeddings);
    }
    for (result.sparse_embeddings, 0..) |item, i| {
        sparse_embeddings[i] = try JsonSparseEnrichmentWrite.init(alloc, item);
        sparse_initialized += 1;
    }

    var graph_writes = try alloc.alloc(JsonGraphWrite, result.graph_writes.len);
    var graph_initialized: usize = 0;
    errdefer {
        for (graph_writes[0..graph_initialized]) |*item| item.deinit(alloc);
        alloc.free(graph_writes);
    }
    for (result.graph_writes, 0..) |item, i| {
        graph_writes[i] = try JsonGraphWrite.init(alloc, item);
        graph_initialized += 1;
    }

    return .{
        .dense_embeddings = dense_embeddings,
        .sparse_embeddings = sparse_embeddings,
        .graph_writes = graph_writes,
    };
}

fn buildJsonComputeEnrichmentsResult(
    alloc: Allocator,
    result: db_mod.types.ComputeEnrichmentsResult,
) !JsonComputeEnrichmentsResult {
    var artifact_writes = try alloc.alloc(JsonArtifactWrite, result.artifact_writes.len);
    var artifact_initialized: usize = 0;
    errdefer {
        for (artifact_writes[0..artifact_initialized]) |*item| item.deinit(alloc);
        alloc.free(artifact_writes);
    }
    for (result.artifact_writes, 0..) |item, i| {
        artifact_writes[i] = try JsonArtifactWrite.init(alloc, item);
        artifact_initialized += 1;
    }

    var documents = try alloc.alloc(JsonDocumentEnrichmentWrite, result.documents.len);
    var documents_initialized: usize = 0;
    errdefer {
        for (documents[0..documents_initialized]) |*item| item.deinit(alloc);
        alloc.free(documents);
    }
    for (result.documents, 0..) |item, i| {
        documents[i] = try JsonDocumentEnrichmentWrite.init(alloc, item);
        documents_initialized += 1;
    }

    var dense_embeddings = try alloc.alloc(JsonDenseEnrichmentWrite, result.dense_embeddings.len);
    var dense_initialized: usize = 0;
    errdefer {
        for (dense_embeddings[0..dense_initialized]) |*item| item.deinit(alloc);
        alloc.free(dense_embeddings);
    }
    for (result.dense_embeddings, 0..) |item, i| {
        dense_embeddings[i] = try JsonDenseEnrichmentWrite.init(alloc, item);
        dense_initialized += 1;
    }

    var failed_keys_b64 = try alloc.alloc([]u8, result.failed_keys.len);
    var failed_initialized: usize = 0;
    errdefer {
        for (failed_keys_b64[0..failed_initialized]) |item| alloc.free(item);
        alloc.free(failed_keys_b64);
    }
    for (result.failed_keys, 0..) |item, i| {
        failed_keys_b64[i] = try dupBase64(alloc, item);
        failed_initialized += 1;
    }

    return .{
        .artifact_writes = artifact_writes,
        .documents = documents,
        .dense_embeddings = dense_embeddings,
        .failed_keys_b64 = failed_keys_b64,
    };
}

fn freeOwnedBatchWrites(alloc: Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(writes);
}

fn decodeBatchWritesRequest(alloc: Allocator, request_json: []const u8) ![]db_mod.types.BatchWrite {
    const Request = struct {
        writes: []const struct {
            key_b64: []const u8,
            value_b64: []const u8,
        },
    };

    var parsed = try std.json.parseFromSlice(Request, alloc, request_json, .{});
    defer parsed.deinit();

    const writes = try alloc.alloc(db_mod.types.BatchWrite, parsed.value.writes.len);
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    for (parsed.value.writes, 0..) |write, i| {
        writes[i] = .{
            .key = try decodeBase64Alloc(alloc, write.key_b64),
            .value = try decodeBase64Alloc(alloc, write.value_b64),
        };
        initialized += 1;
    }

    return writes;
}

const JsonEdge = struct {
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,

    fn init(alloc: Allocator, edge: db_mod.types.GraphEdge) !JsonEdge {
        return .{
            .source_b64 = try dupBase64(alloc, edge.source),
            .target_b64 = try dupBase64(alloc, edge.target),
            .edge_type = edge.edge_type,
            .weight = edge.weight,
            .created_at = edge.created_at,
            .updated_at = edge.updated_at,
            .metadata_json = edge.metadata,
        };
    }

    fn deinit(self: *JsonEdge, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        self.* = undefined;
    }
};

const JsonTraversalResult = struct {
    key_b64: []u8,
    depth: u32,
    total_weight: f64,
    path_b64: ?[][]u8 = null,

    fn init(alloc: Allocator, item: db_mod.types.GraphTraversalResult) !JsonTraversalResult {
        var path_b64: ?[][]u8 = null;
        if (item.path) |path| {
            var encoded = try alloc.alloc([]u8, path.len);
            errdefer alloc.free(encoded);
            var count: usize = 0;
            errdefer {
                for (encoded[0..count]) |entry| alloc.free(entry);
            }
            for (path, 0..) |entry, i| {
                encoded[i] = try dupBase64(alloc, entry);
                count += 1;
            }
            path_b64 = encoded;
        }
        return .{
            .key_b64 = try dupBase64(alloc, item.key),
            .depth = item.depth,
            .total_weight = item.total_weight,
            .path_b64 = path_b64,
        };
    }

    fn deinit(self: *JsonTraversalResult, alloc: Allocator) void {
        alloc.free(self.key_b64);
        if (self.path_b64) |items| {
            for (items) |entry| alloc.free(entry);
            alloc.free(items);
        }
        self.* = undefined;
    }
};

const JsonPathEdge = struct {
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,

    fn init(alloc: Allocator, edge: paths_mod.PathEdge) !JsonPathEdge {
        return .{
            .source_b64 = try dupBase64(alloc, edge.source),
            .target_b64 = try dupBase64(alloc, edge.target),
            .edge_type = edge.edge_type,
            .weight = edge.weight,
        };
    }

    fn deinit(self: *JsonPathEdge, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        self.* = undefined;
    }
};

const JsonPath = struct {
    nodes_b64: [][]u8,
    edges: []JsonPathEdge,
    total_weight: f64,
    length: u32,

    fn init(alloc: Allocator, path: db_mod.types.GraphPath) !JsonPath {
        var nodes = try alloc.alloc([]u8, path.nodes.len);
        errdefer alloc.free(nodes);
        var node_count: usize = 0;
        errdefer {
            for (nodes[0..node_count]) |entry| alloc.free(entry);
        }
        for (path.nodes, 0..) |node, i| {
            nodes[i] = try dupBase64(alloc, node);
            node_count += 1;
        }

        var edges = try alloc.alloc(JsonPathEdge, path.edges.len);
        errdefer alloc.free(edges);
        var edge_count: usize = 0;
        errdefer {
            for (edges[0..edge_count]) |*entry| entry.deinit(alloc);
        }
        for (path.edges, 0..) |edge, i| {
            edges[i] = try JsonPathEdge.init(alloc, edge);
            edge_count += 1;
        }

        return .{
            .nodes_b64 = nodes,
            .edges = edges,
            .total_weight = path.total_weight,
            .length = path.length,
        };
    }

    fn deinit(self: *JsonPath, alloc: Allocator) void {
        for (self.nodes_b64) |entry| alloc.free(entry);
        if (self.nodes_b64.len > 0) alloc.free(self.nodes_b64);
        for (self.edges) |*entry| entry.deinit(alloc);
        if (self.edges.len > 0) alloc.free(self.edges);
        self.* = undefined;
    }
};

const JsonPatternBinding = struct {
    alias: []u8,
    key_b64: []u8,
    depth: u32,

    fn init(alloc: Allocator, binding: graph_pattern_mod.PatternBinding) !JsonPatternBinding {
        return .{
            .alias = try alloc.dupe(u8, binding.alias),
            .key_b64 = try dupBase64(alloc, binding.key),
            .depth = binding.depth,
        };
    }

    fn deinit(self: *JsonPatternBinding, alloc: Allocator) void {
        alloc.free(self.alias);
        alloc.free(self.key_b64);
        self.* = undefined;
    }
};

const JsonPatternMatch = struct {
    bindings: []JsonPatternBinding,
    path: []JsonPathEdge,

    fn init(alloc: Allocator, match: graph_pattern_mod.PatternMatch) !JsonPatternMatch {
        var bindings = try alloc.alloc(JsonPatternBinding, match.bindings.len);
        errdefer alloc.free(bindings);
        var binding_count: usize = 0;
        errdefer {
            for (bindings[0..binding_count]) |*binding| binding.deinit(alloc);
        }
        for (match.bindings, 0..) |binding, i| {
            bindings[i] = try JsonPatternBinding.init(alloc, binding);
            binding_count += 1;
        }

        var path = try alloc.alloc(JsonPathEdge, match.path.len);
        errdefer alloc.free(path);
        var path_count: usize = 0;
        errdefer {
            for (path[0..path_count]) |*entry| entry.deinit(alloc);
        }
        for (match.path, 0..) |edge, i| {
            path[i] = try JsonPathEdge.init(alloc, edge);
            path_count += 1;
        }

        return .{
            .bindings = bindings,
            .path = path,
        };
    }

    fn deinit(self: *JsonPatternMatch, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        for (self.path) |*entry| entry.deinit(alloc);
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }
};

const JsonGraphNode = struct {
    key_b64: []u8,
    depth: u32,
    distance: f64,
    path_b64: ?[][]u8 = null,
    path_edges: []JsonPathEdge = &.{},

    fn init(alloc: Allocator, node: graph_query_mod.GraphResultNode) !JsonGraphNode {
        var path_b64: ?[][]u8 = null;
        if (node.path) |path| {
            var encoded = try alloc.alloc([]u8, path.len);
            errdefer alloc.free(encoded);
            var count: usize = 0;
            errdefer {
                for (encoded[0..count]) |entry| alloc.free(entry);
            }
            for (path, 0..) |entry, i| {
                encoded[i] = try dupBase64(alloc, entry);
                count += 1;
            }
            path_b64 = encoded;
        }

        var path_edges = try alloc.alloc(JsonPathEdge, if (node.path_edges) |items| items.len else 0);
        errdefer alloc.free(path_edges);
        var edge_count: usize = 0;
        errdefer {
            for (path_edges[0..edge_count]) |*edge| edge.deinit(alloc);
        }
        if (node.path_edges) |items| {
            for (items, 0..) |edge, i| {
                path_edges[i] = .{
                    .source_b64 = try dupBase64(alloc, edge.source),
                    .target_b64 = try dupBase64(alloc, edge.target),
                    .edge_type = try alloc.dupe(u8, edge.edge_type),
                    .weight = edge.weight,
                };
                edge_count += 1;
            }
        }

        return .{
            .key_b64 = try dupBase64(alloc, node.key),
            .depth = node.depth,
            .distance = node.distance,
            .path_b64 = path_b64,
            .path_edges = path_edges,
        };
    }

    fn deinit(self: *JsonGraphNode, alloc: Allocator) void {
        alloc.free(self.key_b64);
        if (self.path_b64) |items| {
            for (items) |entry| alloc.free(entry);
            alloc.free(items);
        }
        for (self.path_edges) |*edge| edge.deinit(alloc);
        if (self.path_edges.len > 0) alloc.free(self.path_edges);
        self.* = undefined;
    }
};

pub export fn antfly_db_open(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const out = out_handle orelse return .invalid_argument;
    out.* = null;
    const path_slice = cStringSpan(path) orelse return .invalid_argument;
    const handle = openDefaultDirectoryHandle(path_slice) catch |err| return capi.mapError(err);
    out.* = publishHandle(handle) catch |err| return capi.mapError(err);
    return .ok;
}

fn asStorageOwnerContext(ptr: ?*anyopaque) ?*StorageOwnerContext {
    const raw = ptr orelse return null;
    return @ptrCast(@alignCast(raw));
}

pub fn storageOwnerContextCreate(
    request: *const kernel_owner_abi.ContextRequest,
    out_context: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_context.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    out_context.* = createStorageOwnerContext(.{ .context = request.* }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageOwnerContextCreateWithRuntime(
    request: *const kernel_runtime_services.Request,
    out_context: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_context.* = null;
    if (request.version != kernel_runtime_services.abi_version or request._reserved != 0 or request.context.version != kernel_owner_abi.abi_version) return .invalid_abi;
    out_context.* = createStorageOwnerContext(request.*) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

/// Keep fallible construction in an error union: errdefer does not run when
/// an ABI function returns a scalar failure status.
fn createStorageOwnerContext(services: kernel_runtime_services.Request) !*StorageOwnerContext {
    const request = services.context;
    const bridge = if (services.allocator) |value| blk: {
        if (!value.valid()) return error.InvalidArgument;
        break :blk value.*;
    } else null;
    const receiver = if (services.io) |io| try io.receive() else null;
    const bootstrap_alloc = if (bridge) |*value| value.asStd() else std.heap.c_allocator;
    const context = try bootstrap_alloc.create(StorageOwnerContext);
    errdefer bootstrap_alloc.destroy(context);
    context.* = .{
        .allocator_bridge = bridge,
        .io_receiver = receiver,
        .alloc = undefined,
        .resources = undefined,
        .backend_runtime = undefined,
    };
    const alloc = if (context.allocator_bridge) |*value| value.asStd() else std.heap.c_allocator;
    context.alloc = alloc;
    const memory_budget = antfly.memory_budget;
    const memory_limit = std.math.cast(usize, services.memory_limit_bytes) orelse return error.InvalidArgument;
    const budgets = if (services.io != null)
        memory_budget.smartResourceBudgetsResolved(memory_limit, if (memory_limit == 0) .unavailable else .explicit)
    else
        memory_budget.smartResourceBudgets(memory_limit);
    context.resources = try .initWithBudgets(alloc, budgets);
    errdefer context.resources.deinit();
    var runtime_config = db_mod.background_runtime.Config{};
    if (context.io_receiver) |*io| runtime_config = .{
        .backend = .manual,
        .borrowed_io = .{ .general = io.io() },
    };
    context.backend_runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, runtime_config);
    errdefer context.backend_runtime.deinit();
    context.resources.attachResourceManager();
    switch (request.storage_kind) {
        .directory => if (request.storage_path.len != 0) return error.InvalidArgument,
        .lite => {
            const path = request.storage_path.slice();
            if (path.len == 0) return error.InvalidArgument;
            context.lite_backend = try lite_backend.Handle.openOrCreate(alloc, path, .{
                .no_sync = request.no_sync != 0,
                .resource_manager = &context.resources.resource_manager,
                .io = if (context.io_receiver) |*io| io.io() else null,
            });
        },
    }
    errdefer if (context.lite_backend) |*backend| backend.deinit();
    const auth_storage_path = request.auth_storage_path.slice();
    if (auth_storage_path.len != 0) {
        context.auth_backend = try antfly.lsm_backend.BackendHandle.open(alloc, auth_storage_path, .{
            .storage = context.backend_runtime.ptr().storage(),
        });
        errdefer context.auth_backend.?.close();
        context.auth_users_store = try context.auth_backend.?.backend.runtimeStore(alloc, .{ .name = "usermgr_users" });
        errdefer context.auth_users_store.?.deinit();
        context.auth_casbin_store = try context.auth_backend.?.backend.runtimeStore(alloc, .{ .name = "usermgr_casbin" });
    }
    return context;
}

pub fn storageOwnerContextDestroy(context: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const owner_context = asStorageOwnerContext(context) orelse return .ok;
    return if (owner_context.deinitIfIdle()) .ok else .busy;
}

pub fn storageContextAttachInferenceProvider(
    context: ?*anyopaque,
    inference_handle: ?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    const owner_context = asStorageOwnerContext(context) orelse return .invalid_argument;
    const handle = inference_handle orelse return .invalid_argument;
    owner_context.lock();
    defer owner_context.mutex.unlock();
    if (owner_context.active_owners != 0) return .busy;
    if (owner_context.inference_lifetime) |*lifetime| lifetime.quiesce();
    owner_context.inference_lifetime = .{ .handle = handle };
    return .ok;
}

/// The resolver is a borrowed process capability and must outlive all owners.
pub fn storageOwnerContextConfigureSecrets(context: ?*anyopaque, store: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const owner_context = asStorageOwnerContext(context) orelse return .invalid_argument;
    owner_context.lock();
    defer owner_context.mutex.unlock();
    if (owner_context.active_owners != 0) return .busy;
    owner_context.secret_store = if (store) |ptr| @ptrCast(@alignCast(ptr)) else null;
    return .ok;
}

pub fn storageOwnerContextConfigureRemoteContentSecurity(
    context: ?*anyopaque,
    security_json: kernel_owner_abi.BorrowedBytes,
) callconv(.c) kernel_owner_abi.Status {
    const owner_context = asStorageOwnerContext(context) orelse return .invalid_argument;
    const encoded = security_json.slice();
    var parsed: ?std.json.Parsed(scraping.ContentSecurityConfig) = if (encoded.len == 0)
        null
    else
        std.json.parseFromSlice(scraping.ContentSecurityConfig, owner_context.alloc, encoded, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = false,
        }) catch return .invalid_argument;

    owner_context.lock();
    if (owner_context.active_owners != 0) {
        owner_context.mutex.unlock();
        if (parsed) |*value| value.deinit();
        return .busy;
    }
    var previous = owner_context.remote_content_security;
    owner_context.remote_content_security = parsed;
    owner_context.remote_content.security = if (owner_context.remote_content_security) |*value| value.value else null;
    owner_context.mutex.unlock();
    if (previous) |*value| value.deinit();
    return .ok;
}

fn storageOwnerContextCacheKindStats(stats: anytype) kernel_owner_abi.ContextCacheKindStats {
    return .{
        .hits = stats.hits,
        .misses = stats.misses,
        .inserts = stats.inserts,
        .evictions = stats.evictions,
        .invalidations = stats.invalidations,
        .waits = stats.waits,
        .used_bytes = @intCast(stats.used_bytes),
    };
}

pub fn storageOwnerContextMetrics(
    context: ?*anyopaque,
    out_result: *kernel_owner_abi.ContextMetricsResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    const owner_context = asStorageOwnerContext(context) orelse return .invalid_argument;
    const stats = owner_context.resources.lsm_cache.snapshotStats();
    out_result.* = .{
        .lsm_cache_used_bytes = @intCast(stats.used_bytes),
        .lsm_cache_entry_count = @intCast(stats.entry_count),
        .lsm_run_state = storageOwnerContextCacheKindStats(stats.run_state),
        .lsm_run_table_raw = storageOwnerContextCacheKindStats(stats.run_table_raw),
        .lsm_run_table_index = storageOwnerContextCacheKindStats(stats.run_table_index),
        .lsm_run_table_block = storageOwnerContextCacheKindStats(stats.run_table_block),
        .lsm_run_table_physical_block = storageOwnerContextCacheKindStats(stats.run_table_physical_block),
    };
    return .ok;
}

pub fn storageOwnerContextInvalidateCaches(context: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const owner_context = asStorageOwnerContext(context) orelse return .invalid_argument;
    // Generation publication is rare and can replace every inode below one
    // table root. Cache implementations retain active borrowers safely while
    // making all subsequent lookups miss, so invalidating the process-wide
    // context does not disturb owners of unrelated groups.
    owner_context.resources.lsm_cache.invalidatePrefix("");
    owner_context.resources.hbc_cache.clear();
    return .ok;
}

fn asSystemStore(ptr: ?*anyopaque) ?*SystemStoreHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asSystemReadTxn(ptr: ?*anyopaque) ?*SystemReadTxnHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asSystemCurrentScanTxn(ptr: ?*anyopaque) ?*SystemCurrentScanTxnHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asSystemWriteTxn(ptr: ?*anyopaque) ?*SystemWriteTxnHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asSystemCursor(ptr: ?*anyopaque) ?*SystemCursorHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn storageContextSystemStoreOpen(
    context_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.SystemStoreOpenRequest,
    out_store: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_store.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const context = asStorageOwnerContext(context_ptr) orelse return .invalid_argument;
    const namespace = request.namespace.slice();
    if (namespace.len == 0) return .invalid_argument;
    const store = if (std.mem.eql(u8, namespace, "system/auth-users"))
        if (context.auth_users_store) |*value| value else return .invalid_argument
    else if (std.mem.eql(u8, namespace, "system/auth-casbin"))
        if (context.auth_casbin_store) |*value| value else return .invalid_argument
    else blk: {
        const backend = if (context.lite_backend) |*value| value else return .invalid_argument;
        break :blk backend.runtimeStoreForNamespace(namespace) catch |err|
            return storageOwnerStatusFromError(err);
    };
    const handle = context.alloc.create(SystemStoreHandle) catch return .out_of_memory;
    context.acquire();
    handle.* = .{ .store = store, .context = context };
    out_store.* = handle;
    return .ok;
}

pub fn storageSystemStoreClose(store_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asSystemStore(store_ptr) orelse return;
    const context = handle.context;
    context.alloc.destroy(handle);
    context.release();
}

pub fn storageSystemStoreSync(store_ptr: ?*anyopaque, force: u8) callconv(.c) kernel_owner_abi.Status {
    const handle = asSystemStore(store_ptr) orelse return .invalid_argument;
    handle.store.sync(force != 0) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageSystemStoreBeginRead(
    store_ptr: ?*anyopaque,
    out_txn: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_txn.* = null;
    const handle = asSystemStore(store_ptr) orelse return .invalid_argument;
    const txn = handle.store.beginRead() catch |err| return storageOwnerStatusFromError(err);
    const wrapper = handle.context.alloc.create(SystemReadTxnHandle) catch {
        var owned = txn;
        owned.abort();
        return .out_of_memory;
    };
    wrapper.* = .{ .alloc = handle.context.alloc, .txn = txn };
    out_txn.* = wrapper;
    return .ok;
}

pub fn storageSystemStoreBeginCurrentScan(
    store_ptr: ?*anyopaque,
    out_txn: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_txn.* = null;
    const handle = asSystemStore(store_ptr) orelse return .invalid_argument;
    const txn = handle.store.beginCurrentScan() catch |err| return storageOwnerStatusFromError(err);
    const wrapper = handle.context.alloc.create(SystemCurrentScanTxnHandle) catch {
        var owned = txn;
        owned.abort();
        return .out_of_memory;
    };
    wrapper.* = .{ .alloc = handle.context.alloc, .txn = txn };
    out_txn.* = wrapper;
    return .ok;
}

pub fn storageSystemStoreBeginWrite(
    store_ptr: ?*anyopaque,
    out_txn: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_txn.* = null;
    const handle = asSystemStore(store_ptr) orelse return .invalid_argument;
    const txn = handle.store.beginWrite() catch |err| return storageOwnerStatusFromError(err);
    const wrapper = handle.context.alloc.create(SystemWriteTxnHandle) catch {
        var owned = txn;
        owned.abort();
        return .out_of_memory;
    };
    wrapper.* = .{ .alloc = handle.context.alloc, .txn = txn };
    out_txn.* = wrapper;
    return .ok;
}

pub fn storageSystemReadGet(
    txn_ptr: ?*anyopaque,
    key: kernel_owner_abi.BorrowedBytes,
    out_value: *kernel_owner_abi.BorrowedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_value.* = .{};
    const handle = asSystemReadTxn(txn_ptr) orelse return .invalid_argument;
    const value = handle.txn.get(key.slice()) catch |err| return storageOwnerStatusFromError(err);
    out_value.* = .fromSlice(value);
    return .ok;
}

pub fn storageSystemReadOpenCursor(
    txn_ptr: ?*anyopaque,
    out_cursor: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_cursor.* = null;
    const handle = asSystemReadTxn(txn_ptr) orelse return .invalid_argument;
    const cursor = handle.txn.openCursor() catch |err| return storageOwnerStatusFromError(err);
    const wrapper = handle.alloc.create(SystemCursorHandle) catch {
        var owned = cursor;
        owned.close();
        return .out_of_memory;
    };
    wrapper.* = .{ .alloc = handle.alloc, .cursor = cursor };
    out_cursor.* = wrapper;
    return .ok;
}

pub fn storageSystemReadAbort(txn_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asSystemReadTxn(txn_ptr) orelse return;
    const alloc = handle.alloc;
    handle.txn.abort();
    alloc.destroy(handle);
}

pub fn storageSystemCurrentScanOpenCursor(
    txn_ptr: ?*anyopaque,
    out_cursor: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_cursor.* = null;
    const handle = asSystemCurrentScanTxn(txn_ptr) orelse return .invalid_argument;
    const cursor = handle.txn.openCursor() catch |err| return storageOwnerStatusFromError(err);
    const wrapper = handle.txn.allocator.create(SystemCursorHandle) catch {
        var owned = cursor;
        owned.close();
        return .out_of_memory;
    };
    wrapper.* = .{ .alloc = handle.alloc, .cursor = cursor };
    out_cursor.* = wrapper;
    return .ok;
}

pub fn storageSystemCurrentScanAbort(txn_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asSystemCurrentScanTxn(txn_ptr) orelse return;
    const alloc = handle.alloc;
    handle.txn.abort();
    alloc.destroy(handle);
}

pub fn storageSystemWriteGet(
    txn_ptr: ?*anyopaque,
    key: kernel_owner_abi.BorrowedBytes,
    out_value: *kernel_owner_abi.BorrowedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_value.* = .{};
    const handle = asSystemWriteTxn(txn_ptr) orelse return .invalid_argument;
    const value = handle.txn.get(key.slice()) catch |err| return storageOwnerStatusFromError(err);
    out_value.* = .fromSlice(value);
    return .ok;
}

pub fn storageSystemWritePut(
    txn_ptr: ?*anyopaque,
    key: kernel_owner_abi.BorrowedBytes,
    value: kernel_owner_abi.BorrowedBytes,
) callconv(.c) kernel_owner_abi.Status {
    const handle = asSystemWriteTxn(txn_ptr) orelse return .invalid_argument;
    handle.txn.put(key.slice(), value.slice()) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageSystemWriteDelete(
    txn_ptr: ?*anyopaque,
    key: kernel_owner_abi.BorrowedBytes,
) callconv(.c) kernel_owner_abi.Status {
    const handle = asSystemWriteTxn(txn_ptr) orelse return .invalid_argument;
    handle.txn.delete(key.slice()) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageSystemWriteOpenCursor(
    txn_ptr: ?*anyopaque,
    out_cursor: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_cursor.* = null;
    const handle = asSystemWriteTxn(txn_ptr) orelse return .invalid_argument;
    const cursor = handle.txn.openCursor() catch |err| return storageOwnerStatusFromError(err);
    const wrapper = handle.alloc.create(SystemCursorHandle) catch {
        var owned = cursor;
        owned.close();
        return .out_of_memory;
    };
    wrapper.* = .{ .alloc = handle.alloc, .cursor = cursor };
    out_cursor.* = wrapper;
    return .ok;
}

test "capi system write cursor sees pending catalog rows and preserves abort" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("system-write-cursor");
    defer test_tmp.cleanup();
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "catalog");
    defer alloc.free(path);
    var backend = try lite_backend.Handle.create(alloc, path, false);
    defer backend.deinit();
    const store = try backend.runtimeStoreForNamespace("system/metadata");
    {
        var seed = try store.beginWrite();
        errdefer seed.abort();
        try seed.put("catalog:a", "old");
        try seed.commit();
    }
    {
        var txn = SystemWriteTxnHandle{ .alloc = alloc, .txn = try store.beginWrite() };
        defer txn.txn.abort();
        try txn.txn.delete("catalog:a");
        try txn.txn.put("catalog:b", "pending");
        try txn.txn.put("catalog:c", "last");
        var cursor: ?*anyopaque = null;
        try std.testing.expectEqual(kernel_owner_abi.Status.invalid_argument, storageSystemWriteOpenCursor(null, &cursor));
        try std.testing.expect(cursor == null);
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemWriteOpenCursor(&txn, &cursor));
        defer storageSystemCursorClose(cursor);
        var entry: kernel_owner_abi.SystemEntryResult = .{};
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemCursorMove(cursor, .at_or_after, .fromSlice("catalog:"), &entry));
        try std.testing.expectEqual(@as(u8, 1), entry.present);
        try std.testing.expectEqualStrings("catalog:b", entry.key.slice());
        try std.testing.expectEqualStrings("pending", entry.value.slice());
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemCursorMove(cursor, .next, .{}, &entry));
        try std.testing.expectEqualStrings("catalog:c", entry.key.slice());
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemCursorMove(cursor, .previous, .{}, &entry));
        try std.testing.expectEqualStrings("catalog:b", entry.key.slice());
    }
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("old", try read.get("catalog:a"));
    try std.testing.expectError(error.NotFound, read.get("catalog:b"));
}

pub fn storageSystemWriteCommit(txn_ptr: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const handle = asSystemWriteTxn(txn_ptr) orelse return .invalid_argument;
    const alloc = handle.alloc;
    handle.txn.commit() catch |err| return storageOwnerStatusFromError(err);
    alloc.destroy(handle);
    return .ok;
}

pub fn storageSystemWriteAbort(txn_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asSystemWriteTxn(txn_ptr) orelse return;
    const alloc = handle.alloc;
    handle.txn.abort();
    alloc.destroy(handle);
}

pub fn storageSystemCursorMove(
    cursor_ptr: ?*anyopaque,
    operation: kernel_owner_abi.SystemCursorSeek,
    key: kernel_owner_abi.BorrowedBytes,
    out_entry: *kernel_owner_abi.SystemEntryResult,
) callconv(.c) kernel_owner_abi.Status {
    out_entry.* = .{};
    const handle = asSystemCursor(cursor_ptr) orelse return .invalid_argument;
    const entry = switch (operation) {
        .first => handle.cursor.first(),
        .last => handle.cursor.last(),
        .next => handle.cursor.next(),
        .previous => handle.cursor.prev(),
        .at_or_after => handle.cursor.seekAtOrAfter(key.slice()),
        .at_or_before => handle.cursor.seekAtOrBefore(key.slice()),
    } catch |err| return storageOwnerStatusFromError(err);
    if (entry) |row| out_entry.* = .{
        .key = .fromSlice(row.key),
        .value = .fromSlice(row.value),
        .present = 1,
    };
    return .ok;
}

pub fn storageSystemCursorClose(cursor_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asSystemCursor(cursor_ptr) orelse return;
    const alloc = handle.alloc;
    handle.cursor.close();
    alloc.destroy(handle);
}

fn contextLiteBackend(context_ptr: ?*anyopaque) ?*lite_backend.Handle {
    const context = asStorageOwnerContext(context_ptr) orelse return null;
    return if (context.lite_backend) |*backend| backend else null;
}

pub fn storageContextLiteAdoptionProbe(
    context_ptr: ?*anyopaque,
    out_result: *kernel_owner_abi.LiteAdoptionProbeResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    const backend = contextLiteBackend(context_ptr) orelse return .invalid_argument;
    out_result.* = .{
        .is_embedded_artifact = @intFromBool(backend.isEmbeddedArtifact() catch |err|
            return storageOwnerStatusFromError(err)),
        .embedded_root_has_user_documents = @intFromBool(backend.embeddedRootHasUserDocuments() catch |err|
            return storageOwnerStatusFromError(err)),
    };
    return .ok;
}

pub fn storageContextLiteAdoptAndVerify(
    context_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.LiteAdoptionRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const context = asStorageOwnerContext(context_ptr) orelse return .invalid_argument;
    const backend = if (context.lite_backend) |*value| value else return .invalid_argument;
    const namespace = request.namespace.slice();
    if (namespace.len == 0) return .invalid_argument;
    const target_identity = db_mod.DocIdentityNamespace{
        .table_id = request.identity_table_id,
        .shard_id = request.identity_shard_id,
        .range_id = request.identity_range_id,
    };
    if (!target_identity.eql(antfly.lite.connection.embeddedRootIdentity()))
        return .identity_namespace_mismatch;
    backend.adoptEmbeddedRootAsNamespace(namespace) catch |err| return storageOwnerStatusFromError(err);
    var db_opts = db_mod.OpenOptions{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
        .identity_namespace = target_identity,
    };
    backend.configureDbOpenOptionsForNamespace(&db_opts, namespace) catch |err|
        return storageOwnerStatusFromError(err);
    var adopted_db = db_mod.DB.open(context.alloc, namespace, db_opts) catch |err|
        return storageOwnerStatusFromError(err);
    defer adopted_db.close();
    if (!adopted_db.core.identity_namespace.eql(target_identity)) return .identity_namespace_mismatch;
    return .ok;
}

pub fn storageContextLiteMarkStandalone(context_ptr: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const backend = contextLiteBackend(context_ptr) orelse return .invalid_argument;
    backend.markStandaloneArtifact() catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageContextMaintenanceStatus(
    context_ptr: ?*anyopaque,
    out_result: *kernel_owner_abi.ContextMaintenanceStatus,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    const backend = contextLiteBackend(context_ptr) orelse {
        out_result.* = .{ .engine = .fromSlice("local") };
        return .ok;
    };
    const status = backend.maintenanceSource().status();
    out_result.* = .{
        .check = @intFromBool(status.maintenance.check),
        .compact = @intFromBool(status.maintenance.compact),
        .vacuum = @intFromBool(status.maintenance.vacuum),
        .online = @intFromBool(status.maintenance.online),
        .asynchronous = @intFromBool(status.maintenance.asynchronous),
        .has_fsync = @intFromBool(status.fsync != null),
        .fsync = @intFromBool(status.fsync orelse false),
        .engine = .fromSlice(status.engine),
        .format = .fromSlice(status.format orelse ""),
    };
    return .ok;
}

pub fn storageContextMaintenanceRun(
    context_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.ContextMaintenanceRequest,
    out_result: *kernel_owner_abi.ContextMaintenanceResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const backend = contextLiteBackend(context_ptr) orelse return .invalid_argument;
    var local_cancel: antfly.storage_maintenance.CancelToken = .{};
    const cancel: *const antfly.storage_maintenance.CancelToken = if (request.cancel_token) |raw|
        @ptrCast(@alignCast(raw))
    else
        &local_cancel;
    const result = backend.maintenanceSource().run(switch (request.operation) {
        .check => .check,
        .compact => .compact,
        .vacuum => .vacuum,
    }, cancel) catch |err| return storageOwnerStatusFromError(err);
    var present_mask: u16 = 0;
    if (result.file_size != null) present_mask |= 1 << 0;
    if (result.valid_prefix_size != null) present_mask |= 1 << 1;
    if (result.reclaimable_bytes != null) present_mask |= 1 << 2;
    if (result.before_size != null) present_mask |= 1 << 3;
    if (result.after_size != null) present_mask |= 1 << 4;
    if (result.reclaimed_bytes != null) present_mask |= 1 << 5;
    if (result.live_file_count != null) present_mask |= 1 << 6;
    if (result.live_bytes != null) present_mask |= 1 << 7;
    out_result.* = .{
        .has_valid = @intFromBool(result.valid != null),
        .valid = @intFromBool(result.valid orelse false),
        .issue = .fromSlice(result.issue orelse ""),
        .file_size = result.file_size orelse 0,
        .valid_prefix_size = result.valid_prefix_size orelse 0,
        .reclaimable_bytes = result.reclaimable_bytes orelse 0,
        .before_size = result.before_size orelse 0,
        .after_size = result.after_size orelse 0,
        .reclaimed_bytes = result.reclaimed_bytes orelse 0,
        .live_file_count = result.live_file_count orelse 0,
        .live_bytes = result.live_bytes orelse 0,
        .present_mask = present_mask,
    };
    return .ok;
}

fn asDataApplyStore(ptr: ?*anyopaque) ?*DataApplyStoreHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asMetadataApplyStore(ptr: ?*anyopaque) ?*MetadataApplyStoreHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asMetadataPreparedSnapshot(ptr: ?*anyopaque) ?*MetadataPreparedSnapshotHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asDataApplyGroupTransition(ptr: ?*anyopaque) ?*DataApplyGroupTransitionHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn asDataApplyPreparedSnapshot(ptr: ?*anyopaque) ?*DataApplyPreparedSnapshotHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

fn metadataProjectionJson(
    alloc: Allocator,
    out_json: *kernel_owner_abi.OwnedBytes,
    value: anytype,
) kernel_owner_abi.Status {
    const encoded = std.json.Stringify.valueAlloc(alloc, value, .{}) catch |err|
        return storageOwnerStatusFromError(err);
    out_json.* = .{
        .ptr = if (encoded.len == 0) null else encoded.ptr,
        .len = @intCast(encoded.len),
    };
    return .ok;
}

fn metadataProjectionStatusFromError(err: anyerror) kernel_owner_abi.Status {
    if (err == error.InvalidDerivedCatalogIndex)
        return storageOwnerStatusFromError(error.InvalidArgument);
    return storageOwnerStatusFromError(err);
}

pub fn metadataApplyStoreOpen(
    request: *const kernel_owner_abi.MetadataApplyOpenRequest,
    out_store: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_store.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const root_dir = request.root_dir.slice();
    if (root_dir.len == 0) return .invalid_argument;
    const alloc = std.heap.c_allocator;
    const context = asStorageOwnerContext(request.context);
    if (context) |value| value.acquire();
    var context_borrowed = context != null;
    defer if (context_borrowed) context.?.release();
    var store = metadata_raft_apply.RaftApplyStore.init(alloc, .{
        .root_dir = root_dir,
        .no_sync = request.no_sync != 0,
        .read_only = request.read_only != 0,
    }) catch |err| return storageOwnerStatusFromError(err);
    errdefer store.deinit();
    const handle = alloc.create(MetadataApplyStoreHandle) catch return .out_of_memory;
    handle.* = .{ .alloc = alloc, .store = store, .context = context };
    context_borrowed = false;
    out_store.* = handle;
    return .ok;
}

pub fn metadataApplyStoreClose(store_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asMetadataApplyStore(store_ptr) orelse return;
    const alloc = handle.alloc;
    const context = handle.context;
    handle.store.deinit();
    for (handle.listener_bridges.items) |bridge| alloc.destroy(bridge);
    handle.listener_bridges.deinit(alloc);
    handle.* = undefined;
    alloc.destroy(handle);
    if (context) |value| value.release();
}

pub fn metadataApplyStoreApplyBatch(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataApplyBatchRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    handle.store.snapshotBuilder().applyBatch(.{
        .group_id = request.group_id,
        .commit_index = request.commit_index,
        .entries_bytes = request.entries.slice(),
    }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn metadataApplyStoreBuildSnapshot(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataApplyGroupRequest,
    out_snapshot: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_snapshot.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    const snapshot = handle.store.snapshotBuilder().buildSnapshot(handle.alloc, request.group_id) catch |err|
        return storageOwnerStatusFromError(err);
    out_snapshot.* = .{ .ptr = if (snapshot.len == 0) null else snapshot.ptr, .len = @intCast(snapshot.len) };
    return .ok;
}

pub fn metadataApplyStoreInstallSnapshot(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataApplySnapshotRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    const installed = handle.store.snapshotBuilder().installSnapshot(
        handle.alloc,
        request.group_id,
        request.commit_index,
        request.snapshot.slice(),
    ) catch |err| return storageOwnerStatusFromError(err);
    return if (installed) .ok else .internal;
}

pub fn metadataApplyStorePrepareSnapshot(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataApplyPrepareSnapshotRequest,
    out_prepared: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_prepared.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    const source = handle.store.snapshotBuilder().prepareSnapshot(request.group_id, request.applied_index) catch |err|
        return storageOwnerStatusFromError(err);
    const value = source orelse return .ok;
    const prepared = handle.alloc.create(MetadataPreparedSnapshotHandle) catch {
        value.deinit();
        return .out_of_memory;
    };
    prepared.* = .{ .source = value };
    out_prepared.* = prepared;
    return .ok;
}

pub fn metadataApplyPreparedSnapshotMaterialize(
    prepared_ptr: ?*anyopaque,
    out_snapshot: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_snapshot.* = .{};
    const prepared = asMetadataPreparedSnapshot(prepared_ptr) orelse return .invalid_argument;
    const alloc = std.heap.c_allocator;
    var materialized = prepared.source.materialize(alloc) catch |err| return storageOwnerStatusFromError(err);
    switch (materialized) {
        .bytes => |bytes| {
            out_snapshot.* = .{ .ptr = if (bytes.len == 0) null else bytes.ptr, .len = @intCast(bytes.len) };
            materialized = undefined;
        },
        .artifact => |artifact| {
            const bytes = artifact.readAll(alloc) catch |err| {
                materialized.deinit(alloc);
                return storageOwnerStatusFromError(err);
            };
            materialized.deinit(alloc);
            out_snapshot.* = .{ .ptr = if (bytes.len == 0) null else bytes.ptr, .len = @intCast(bytes.len) };
        },
    }
    return .ok;
}

pub fn metadataApplyPreparedSnapshotCancel(prepared_ptr: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const prepared = asMetadataPreparedSnapshot(prepared_ptr) orelse return .invalid_argument;
    prepared.source.cancel();
    return .ok;
}

pub fn metadataApplyPreparedSnapshotDestroy(prepared_ptr: ?*anyopaque) callconv(.c) void {
    const prepared = asMetadataPreparedSnapshot(prepared_ptr) orelse return;
    prepared.source.deinit();
    std.heap.c_allocator.destroy(prepared);
}

pub fn metadataApplyStoreAddListeners(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataListenerRequest,
    out_registration_id: *u64,
) callconv(.c) kernel_owner_abi.Status {
    out_registration_id.* = 0;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.projection_fn == null and request.committed_key_fn == null) return .invalid_argument;
    if (request.has_commit_barrier_kind > 1) return .invalid_argument;
    const has_commit_barrier = request.has_commit_barrier_kind != 0;
    if ((request.before_projection_commit_fn != null) != has_commit_barrier or
        (request.after_projection_commit_fn != null) != has_commit_barrier or
        (has_commit_barrier and request.projection_fn == null)) return .invalid_argument;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    const io = handle.store.io_impl.io();
    handle.listener_mutex.lockUncancelable(io);
    defer handle.listener_mutex.unlock(io);
    handle.listener_bridges.ensureUnusedCapacity(handle.alloc, 1) catch return .out_of_memory;
    const bridge = handle.alloc.create(MetadataListenerBridge) catch return .out_of_memory;
    errdefer handle.alloc.destroy(bridge);
    bridge.* = .{ .request = request.* };
    const projection = metadata_raft_apply.ProjectionListener{
        .ptr = bridge,
        .vtable = if (has_commit_barrier)
            &MetadataListenerBridge.projection_barrier_vtable
        else
            &MetadataListenerBridge.projection_vtable,
        .commit_barrier_kind = if (has_commit_barrier)
            MetadataListenerBridge.projectionKindFromAbi(request.commit_barrier_kind)
        else
            null,
    };
    const committed = metadata_raft_apply.CommittedKeyListener{
        .ptr = bridge,
        .vtable = &MetadataListenerBridge.committed_key_vtable,
    };
    const registration = handle.store.addLifecycleListeners(projection, committed) catch |err|
        return storageOwnerStatusFromError(err);
    bridge.registration_id = registration.id;
    out_registration_id.* = registration.id;
    handle.listener_bridges.appendAssumeCapacity(bridge);
    return .ok;
}

pub fn metadataApplyStoreRemoveListeners(store_ptr: ?*anyopaque, registration_id: u64) callconv(.c) u8 {
    const handle = asMetadataApplyStore(store_ptr) orelse return 0;
    const io = handle.store.io_impl.io();
    handle.listener_mutex.lockUncancelable(io);
    defer handle.listener_mutex.unlock(io);
    for (handle.listener_bridges.items, 0..) |bridge, index| {
        if (bridge.registration_id != registration_id) continue;
        if (!handle.store.removeLifecycleListeners(.{ .id = registration_id })) return 0;
        // Physical detach drains dispatch before either side releases context.
        _ = handle.listener_bridges.orderedRemove(index);
        handle.alloc.destroy(bridge);
        return 1;
    }
    return 0;
}

pub fn metadataApplyStoreBindHA(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataHABindRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    const port: ?antfly.capi_dependencies.storage_metadata_ha_port.Port = if (request.port) |ptr| @as(*const antfly.capi_dependencies.storage_metadata_ha_port.Port, @ptrCast(@alignCast(ptr))).* else null;
    handle.store.bindHAPort(port) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn metadataApplyStoreProjection(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataProjectionRequest,
    out_json: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_json.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    const alloc = handle.alloc;
    return switch (request.kind) {
        .flush_ha_outbox => blk: {
            handle.store.flushHAOutbox() catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .apply_ha_record => blk: {
            if (request.key.len > 2 * 1024 * 1024) break :blk .invalid_argument;
            const record = antfly.capi_dependencies.storage_hot_standby_replication_record.decode(request.key.slice()) catch |err| break :blk storageOwnerStatusFromError(err);
            handle.store.applyHARecord(record) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .export_ha_checkpoint, .import_ha_checkpoint => blk: {
            const path = request.key.slice();
            if (path.len == 0 or path.len > 4096 or std.mem.indexOfScalar(u8, path, 0) != null) break :blk .invalid_argument;
            const io = handle.store.io_impl.io();
            if (request.kind == .export_ha_checkpoint) {
                const value = handle.store.exportHACheckpoint(io, path) catch |err| break :blk storageOwnerStatusFromError(err);
                break :blk metadataProjectionJson(alloc, out_json, value);
            }
            handle.store.importHACheckpoint(io, path, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .migrate_standalone_restore_jobs => blk: {
            if (request.key.len > 64 * 1024 * 1024) break :blk .invalid_argument;
            var rows = std.json.parseFromSlice([]const metadata_raft_apply.RaftApplyStore.RestoreJobRow, alloc, request.key.slice(), .{}) catch |err| break :blk storageOwnerStatusFromError(err);
            defer rows.deinit();
            handle.store.migrateStandaloneRestoreJobs(rows.value) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .backup_cohort => blk: {
            const value = handle.store.getBackupCohort(alloc, request.group_id, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err);
            defer if (value) |bytes| alloc.free(bytes);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .backup_cohort_progress => blk: {
            const value = handle.store.getBackupCohortProgress(alloc, request.group_id, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err);
            defer if (value) |bytes| alloc.free(bytes);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .backup_cohorts => blk: {
            if (request.arg1 > 1) break :blk .invalid_argument;
            const value = handle.store.listBackupCohorts(alloc, request.group_id, if (request.arg1 == 0) null else request.key.slice(), std.math.cast(usize, request.arg0) orelse break :blk .invalid_argument) catch |err| break :blk storageOwnerStatusFromError(err);
            defer antfly.capi_dependencies.storage_docstore.DocStore.freeResults(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .provisioning_catalog => blk: {
            var value = handle.store.captureProvisioningCatalog(alloc, request.group_id) catch |err| break :blk storageOwnerStatusFromError(err);
            defer value.deinit(alloc);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .relational_topology_protocol_activation_version => blk: {
            const value = handle.store.getRelationalTopologyProtocolActivationVersion(request.group_id) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .resolve_table_create_identity => blk: {
            const value = handle.store.resolveTableCreateIdentity(request.group_id, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .standalone_catalog => blk: {
            const value = (if (request.arg0 == 1) handle.store.loadStandaloneCatalogSnapshot(alloc) else handle.store.loadStandaloneCatalog(alloc)) catch |err| break :blk storageOwnerStatusFromError(err);
            defer if (value) |bytes| alloc.free(bytes);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .standalone_revision => blk: {
            const value = handle.store.standaloneRevision() catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .restore_staging_job => blk: {
            if (request.key.len != 16) break :blk .invalid_argument;
            var value = (handle.store.loadRestoreStaging(alloc, request.group_id, request.key.slice()[0..16].*) catch |err| break :blk storageOwnerStatusFromError(err)) orelse break :blk metadataProjectionJson(alloc, out_json, @as(?u8, null));
            defer value.deinit();
            break :blk metadataProjectionJson(alloc, out_json, value.value);
        },
        .restore_staging_owner_job => blk: {
            var value = (handle.store.loadRestoreStagingForOwner(alloc, request.group_id, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err)) orelse break :blk metadataProjectionJson(alloc, out_json, @as(?u8, null));
            defer value.deinit();
            break :blk metadataProjectionJson(alloc, out_json, value.value);
        },
        .restore_staging_authority_allowed => blk: {
            if (request.key.len != 16) break :blk .invalid_argument;
            const owner_group: ?u64 = if (request.arg1 == 0) null else request.arg1;
            const value = handle.store.restoreStagingAuthorityAllowed(alloc, request.group_id, request.key.slice()[0..16].*, request.arg0, owner_group) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .restore_staging_progress, .restore_staging_receipt => blk: {
            if (request.key.len != 16) break :blk .invalid_argument;
            const id = request.key.slice()[0..16].*;
            if (request.kind == .restore_staging_progress) {
                const value = handle.store.loadRestoreStagingProgress(alloc, request.group_id, id) catch |err| break :blk storageOwnerStatusFromError(err);
                break :blk metadataProjectionJson(alloc, out_json, value);
            }
            const state = std.enums.fromInt(antfly.capi_dependencies.metadata_restore_staging.State, request.arg0) orelse break :blk .invalid_argument;
            const value = handle.store.loadRestoreStagingReceipt(alloc, request.group_id, id, state, request.arg1) catch |err| break :blk storageOwnerStatusFromError(err);
            defer if (value) |bytes| alloc.free(bytes);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .standalone_command => blk: {
            const physical = antfly.capi_dependencies.metadata_storage_raft_apply_store;
            if (request.key.len > 32 * 1024 * 1024) break :blk .invalid_argument;
            var command = (physical.decodeTransitionCommand(alloc, request.key.slice()) catch |err| break :blk storageOwnerStatusFromError(err)) orelse break :blk .invalid_argument;
            defer command.deinit(alloc);
            handle.store.applyStandaloneCommand(request.group_id, command) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .replace_standalone_catalog => blk: {
            if (request.key.len > 64 * 1024 * 1024) break :blk .invalid_argument;
            const Input = antfly.capi_dependencies.metadata_storage_raft_apply_contract.StandaloneCatalogUpdate;
            var input = std.json.parseFromSlice(Input, alloc, request.key.slice(), .{}) catch |err| break :blk storageOwnerStatusFromError(err);
            defer input.deinit();
            handle.store.updateStandaloneCatalog(request.group_id, request.arg0, input.value) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .system_catalog => blk: {
            const contract = metadata_raft_apply.apply_contract;
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const input_request = std.json.parseFromSliceLeaky(contract.CatalogProjectionRequest, a, request.key.slice(), .{}) catch |err| break :blk storageOwnerStatusFromError(err);
            const group_id = request.group_id;
            switch (input_request) {
                .read_store => |input| {
                    const value = handle.store.readStore(a, group_id, input.store_id, input.reports) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .read_store_group_facts => |input| {
                    const value = handle.store.readStoreGroupFacts(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .read_store_report_targets => |input| {
                    const value = handle.store.readStoreReportTargetsWithRuntime(a, group_id, .{ .sequence = 1, .report = .{ .store_id = input.store_id }, .base = if (input.full) null else .{ .reporter_incarnation = 0, .sequence = 0, .digest = @splat(0) }, .removed_groups = input.group_ids }, input.include_runtime) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_read => |input| {
                    const value = handle.store.systemCatalogRead(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_export => {
                    const value = handle.store.exportSystemCatalog(a, group_id) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_list_tables => |input| {
                    const value = handle.store.listSystemCatalogTables(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_meta => {
                    const value = handle.store.systemCatalogMeta(a, group_id) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_admission => |input| {
                    const value = handle.store.systemCatalogAdmission(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_prepare => |input| {
                    const value = handle.store.prepareSystemCatalogResult(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_resolve_table => |input| {
                    const value = handle.store.resolveSystemCatalogTable(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_resolve_identity => |input| {
                    const value = handle.store.resolveSystemCatalogIdentity(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_resolve_many => |input| {
                    const value = handle.store.resolveSystemCatalogIdentities(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_write_validation => |name| {
                    const value = handle.store.tableWriteValidation(a, group_id, name) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_write_validation_revision => {
                    const value = handle.store.writeValidationRevision(group_id) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_query_definition => |input| {
                    const value = handle.store.queryTableDefinition(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .topology_activation => {
                    const value = handle.store.topologyActivation(group_id) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .report_baseline_fragment_admission => |input| {
                    const value = handle.store.admitBaselineFragment(group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .report_baseline_progress => |input| {
                    const value = handle.store.reportBaselineProgressForKey(group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .read_control_stores => |groups| {
                    const value = handle.store.readControlStores(a, group_id, groups) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .report_cursor => |input| {
                    const value = handle.store.reportCursor(group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .catalog_snapshot => {
                    var value = handle.store.systemCatalogSnapshot(a, group_id) catch |err| break :blk storageOwnerStatusFromError(err);
                    defer value.deinit();
                    break :blk metadataProjectionJson(alloc, out_json, .{ .meta = value.meta, .value = value.value });
                },
            }
        },
        .latest_checkpoint => blk: {
            const value = handle.store.latestCheckpoint(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .metadata_incarnation => blk: {
            const value = handle.store.getMetadataIncarnation(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .dense_native_storage_protocol_activation_version => blk: {
            const value = handle.store.getDenseNativeStorageProtocolActivationVersion(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .runtime_status_protocol_activation_version => blk: {
            const value = handle.store.getRuntimeStatusProtocolActivationVersion(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .split_transitions => blk: {
            const value = handle.store.listSplitTransitions(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeSplitTransitions(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .placement_intents => blk: {
            const value = handle.store.listPlacementIntents(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freePlacementIntents(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .placement_version_fences => blk: {
            const value = handle.store.listPlacementVersionFences(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer alloc.free(value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .local_placement_intents => blk: {
            const value = handle.store.listLocalPlacementIntents(alloc, request.group_id, request.arg0) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freePlacementIntents(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .nodes => blk: {
            const value = handle.store.listNodes(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeNodes(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .stores => blk: {
            const value = handle.store.listStores(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeStores(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .merge_transitions => blk: {
            const value = handle.store.listMergeTransitions(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeMergeTransitions(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .merge_transition => blk: {
            const value = handle.store.getMergeTransition(alloc, request.group_id, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err);
            defer if (value) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .tables => blk: {
            const value = handle.store.listTables(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeTables(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .catalog_projection => blk: {
            const value = handle.store.captureCatalogProjection(
                alloc,
                request.group_id,
                if (request.arg0 == 0) null else request.arg0,
            ) catch |err| break :blk if (err == error.CatalogRoutingSnapshotTimeout)
                .timeout
            else
                storageOwnerStatusFromError(err);
            defer handle.store.freeTables(alloc, value.tables);
            defer handle.store.freeRanges(alloc, value.ranges);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .catalog_cursor => blk: {
            const value = handle.store.captureCatalogCursor(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .table => blk: {
            const value = handle.store.getTable(alloc, request.group_id, request.arg0) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer if (value) |record| metadata_table_manager.freeTable(alloc, record);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .range => blk: {
            const value = handle.store.getRange(alloc, request.group_id, request.arg0) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer if (value) |record| metadata_table_manager.freeRange(alloc, record);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .table_drop_projection => blk: {
            var value = handle.store.captureTableDropProjection(
                alloc,
                request.group_id,
                request.key.slice(),
            ) catch |err| break :blk metadataProjectionStatusFromError(err);
            defer if (value) |*projection| projection.deinit(alloc);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .table_create_generation => blk: {
            const value = handle.store.captureTableCreateGeneration(
                alloc,
                request.group_id,
                request.arg0,
            ) catch |err| break :blk metadataProjectionStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .table_restore_admission => blk: {
            var parsed = std.json.parseFromSlice(
                metadata_table_manager.TableRecord,
                alloc,
                request.key.slice(),
                .{},
            ) catch |err| break :blk switch (err) {
                error.OutOfMemory => .out_of_memory,
                else => .invalid_argument,
            };
            defer parsed.deinit();
            const value = handle.store.captureTableRestoreAdmission(
                alloc,
                request.group_id,
                parsed.value,
            ) catch |err| break :blk metadataProjectionStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .verify_table_create_projection => blk: {
            const Payload = struct {
                table: metadata_table_manager.TableRecord,
                ranges: []const metadata_table_manager.RangeRecord,
            };
            var parsed = std.json.parseFromSlice(Payload, alloc, request.key.slice(), .{}) catch |err|
                break :blk switch (err) {
                    error.OutOfMemory => .out_of_memory,
                    else => .invalid_argument,
                };
            defer parsed.deinit();
            handle.store.verifyTableCreateProjectionExact(
                alloc,
                request.group_id,
                parsed.value.table,
                parsed.value.ranges,
            ) catch |err| break :blk metadataProjectionStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .table_transition_fence => blk: {
            const value = handle.store.getTableTransitionFence(request.group_id, request.arg0) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .schema_progress => blk: {
            const value = handle.store.listSchemaProgress(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeSchemaProgress(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .restore_progress => blk: {
            const value = handle.store.listRestoreProgress(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeRestoreProgress(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .active_restore_ranges => blk: {
            const value = handle.store.listActiveRestoreRanges(alloc, request.group_id) catch |err|
                break :blk metadataProjectionStatusFromError(err);
            defer handle.store.freeRanges(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .ensure_derived_catalog_indexes => blk: {
            handle.store.ensureDerivedCatalogIndexes(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .rebuild_derived_catalog_indexes => blk: {
            handle.store.rebuildDerivedCatalogIndexes(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .replication_source_statuses => blk: {
            const value = handle.store.listReplicationSourceStatuses(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeReplicationSourceStatuses(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .replication_source_status => blk: {
            const ordinal = std.math.cast(u32, request.arg1) orelse break :blk .invalid_argument;
            const value = handle.store.getReplicationSourceStatus(alloc, request.group_id, request.arg0, ordinal) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer if (value) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .extension_packages => blk: {
            const value = handle.store.listExtensionPackages(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeExtensionPackages(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .installed_extensions => blk: {
            const value = handle.store.listInstalledExtensions(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeInstalledExtensions(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .extension_members => blk: {
            const value = handle.store.listExtensionMembers(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeExtensionMembers(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .extension_dependencies => blk: {
            const value = handle.store.listExtensionDependencies(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeExtensionDependencies(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .extension_lifecycle_delta_applied => blk: {
            var parsed = std.json.parseFromSlice(
                metadata_raft_apply.ExtensionLifecycleDelta,
                alloc,
                request.key.slice(),
                .{},
            ) catch |err| break :blk switch (err) {
                error.OutOfMemory => .out_of_memory,
                else => .invalid_argument,
            };
            defer parsed.deinit();
            const applied = handle.store.extensionLifecycleDeltaApplied(
                alloc,
                request.group_id,
                parsed.value,
            ) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, applied);
        },
        .shuffle_join_leases => blk: {
            const value = handle.store.listShuffleJoinLeases(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeShuffleJoinLeases(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .ranges => blk: {
            const value = handle.store.listRanges(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeRanges(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .reconcile_lease => blk: {
            const value = handle.store.getReconcileLease(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .reallocation_request => blk: {
            const value = handle.store.getReallocationRequest(request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .shuffle_join_lease => blk: {
            const value = handle.store.getShuffleJoinLease(request.group_id, request.arg0) catch |err|
                break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .restore_job_rows => blk: {
            const value = handle.store.listRestoreJobRows(alloc, request.group_id) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer handle.store.freeRestoreJobRows(alloc, value);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .secret_collection => blk: {
            const value = handle.store.getSecretCollection(alloc, request.group_id, request.key.slice()) catch |err|
                break :blk storageOwnerStatusFromError(err);
            // This projection is opaque binary, never a JSON UTF-8 string.
            // Empty means absent; a persisted collection always has a header.
            out_json.* = if (value) |bytes| .{ .ptr = bytes.ptr, .len = @intCast(bytes.len) } else .{};
            break :blk .ok;
        },
        .restore_job_value => blk: {
            const value = handle.store.getRestoreJobValue(alloc, request.group_id, request.key.slice()) catch |err|
                break :blk storageOwnerStatusFromError(err);
            defer if (value) |bytes| alloc.free(bytes);
            break :blk metadataProjectionJson(alloc, out_json, value);
        },
        .maintenance_stats => blk: {
            const stats = handle.store.snapshotMaintenanceStats();
            break :blk metadataProjectionJson(alloc, out_json, .{
                .mutable_bytes = stats.mutable_bytes,
                .immutable_bytes = stats.immutable_bytes,
                .total_run_bytes = stats.total_run_bytes,
                .wal_retained_bytes = stats.wal_retained_bytes,
                .wal_retained_segments = stats.wal_retained_segments,
                .active_readers = stats.active_readers,
                .obsolete_paths = stats.obsolete_paths,
                .obsolete_paths_pinned_by_readers = stats.obsolete_paths_pinned_by_readers,
                .obsolete_paths_pinned_by_versions = stats.obsolete_paths_pinned_by_versions,
                .bulk_ingest_current_scan_clone_active_bytes = stats.bulk_ingest_current_scan_clone_active_bytes,
            });
        },
    };
}

pub fn metadataReconcileReplicaRoot(
    request: *const kernel_owner_abi.MetadataReplicaRootReconcileRequest,
    out_summary: *kernel_owner_abi.MetadataProvisionSummary,
) callconv(.c) kernel_owner_abi.Status {
    out_summary.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const replica_root_dir = request.replica_root_dir.slice();
    if (replica_root_dir.len == 0 or request.request_json.len == 0) return .invalid_argument;
    const context = if (request.context) |_| asStorageOwnerContext(request.context) orelse return .invalid_argument else null;
    if (context) |value| value.acquire();
    defer if (context) |value| value.release();

    const Payload = struct {
        group_ids: []const u64,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
    };
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const payload = std.json.parseFromSliceLeaky(Payload, alloc, request.request_json.slice(), .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch |err| return switch (err) {
        error.OutOfMemory => .out_of_memory,
        else => .invalid_argument,
    };
    const summary = metadata_table_provisioner.reconcileReplicaRoot(
        alloc,
        replica_root_dir,
        request.metadata_group_id,
        payload.group_ids,
        payload.tables,
        payload.ranges,
    ) catch |err| return storageOwnerStatusFromError(err);
    out_summary.* = .{
        .groups_considered = @intCast(summary.groups_considered),
        .dbs_opened = @intCast(summary.dbs_opened),
        .indexes_added = @intCast(summary.indexes_added),
        .indexes_removed = @intCast(summary.indexes_removed),
        .indexes_pending = @intCast(summary.indexes_pending),
        .enrichments_added = @intCast(summary.enrichments_added),
        .enrichments_updated = @intCast(summary.enrichments_updated),
        .enrichments_removed = @intCast(summary.enrichments_removed),
        .resolvers_added = @intCast(summary.resolvers_added),
        .resolvers_updated = @intCast(summary.resolvers_updated),
        .resolvers_removed = @intCast(summary.resolvers_removed),
    };
    return .ok;
}

pub fn dataApplyStoreOpen(
    request: *const kernel_owner_abi.DataApplyOpenRequest,
    out_store: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_store.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.native_source_delegate > 1) return .invalid_argument;
    const root_dir = request.root_dir.slice();
    if (root_dir.len == 0) return .invalid_argument;
    const alloc = std.heap.c_allocator;
    const context = asStorageOwnerContext(request.context);
    if (context) |value| value.acquire();
    var context_borrowed = context != null;
    defer if (context_borrowed) context.?.release();
    var store = data_raft_apply.RaftApplyStore.init(alloc, .{
        .root_dir = root_dir,
        .no_sync = request.no_sync != 0,
        .read_only = request.read_only != 0,
        .native_source_delegate = request.native_source_delegate != 0,
        .resource_manager = if (context) |value| &value.resources.resource_manager else null,
    }) catch |err| return storageOwnerStatusFromError(err);
    errdefer store.deinit();
    const handle = alloc.create(DataApplyStoreHandle) catch return .out_of_memory;
    handle.* = .{
        .alloc = alloc,
        .store = store,
        .context = context,
    };
    context_borrowed = false;
    out_store.* = handle;
    return .ok;
}

pub fn dataApplyStoreClose(store_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asDataApplyStore(store_ptr) orelse return;
    const alloc = handle.alloc;
    const context = handle.context;
    handle.store.deinit();
    handle.* = undefined;
    alloc.destroy(handle);
    if (context) |value| value.release();
}

pub fn dataApplyStoreApplyBatch(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyBatchRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    handle.store.snapshotBuilder().applyBatch(.{
        .group_id = request.group_id,
        .commit_index = request.commit_index,
        .entries_bytes = request.entries.slice(),
    }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn dataApplyStoreBuildSnapshot(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyGroupRequest,
    out_snapshot: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_snapshot.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const snapshot = handle.store.snapshotBuilder().buildSnapshot(handle.alloc, request.group_id) catch |err|
        return storageOwnerStatusFromError(err);
    out_snapshot.* = .{
        .ptr = if (snapshot.len == 0) null else snapshot.ptr,
        .len = @intCast(snapshot.len),
    };
    return .ok;
}

pub fn dataApplyStoreInstallSnapshot(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplySnapshotRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const installed = handle.store.snapshotBuilder().installSnapshot(
        handle.alloc,
        request.group_id,
        request.commit_index,
        request.snapshot.slice(),
    ) catch |err| return storageOwnerStatusFromError(err);
    if (!installed) return .internal;
    return .ok;
}

pub fn dataApplyStorePrepareSnapshot(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyPrepareSnapshotRequest,
    out_prepared: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_prepared.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const prepared = handle.store.prepareSnapshotHandle(request.group_id, request.applied_index) catch |err|
        return storageOwnerStatusFromError(err);
    const value = prepared orelse return .ok;
    const owned = handle.alloc.create(DataApplyPreparedSnapshotHandle) catch {
        value.destroy();
        return .out_of_memory;
    };
    owned.* = .{ .prepared = value };
    out_prepared.* = owned;
    return .ok;
}

pub fn dataApplyPreparedSnapshotMaterialize(
    prepared_ptr: ?*anyopaque,
    out_result: *kernel_owner_abi.DataApplyPreparedSnapshotResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    const handle = asDataApplyPreparedSnapshot(prepared_ptr) orelse return .invalid_argument;
    if (handle.materialized) return .invalid_argument;
    const materialized = handle.prepared.materializeFile(std.heap.c_allocator) catch |err|
        return storageOwnerStatusFromError(err);
    handle.materialized = true;
    out_result.* = .{
        .path = .{
            .ptr = if (materialized.path.len == 0) null else materialized.path.ptr,
            .len = @intCast(materialized.path.len),
        },
        .size = materialized.size,
    };
    return .ok;
}

pub fn dataApplyPreparedSnapshotCancel(prepared_ptr: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const handle = asDataApplyPreparedSnapshot(prepared_ptr) orelse return .invalid_argument;
    handle.prepared.cancel();
    return .ok;
}

pub fn dataApplyPreparedSnapshotDestroy(prepared_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asDataApplyPreparedSnapshot(prepared_ptr) orelse return;
    handle.prepared.destroy();
    std.heap.c_allocator.destroy(handle);
}

pub fn dataApplyStoreLatest(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyGroupRequest,
    out_result: *kernel_owner_abi.DataApplyLatestResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const latest = handle.store.latestBatch(request.group_id) catch |err|
        return storageOwnerStatusFromError(err);
    const value = latest orelse return .ok;
    out_result.* = .{
        .present = 1,
        .commit_index = value.commit_index,
        .entry_count = @intCast(value.entry_count),
        .normal_entry_count = @intCast(value.normal_entry_count),
        .admin_entry_count = @intCast(value.admin_entry_count),
        .last_entry_term = value.last_entry_term,
        .last_entry_index = value.last_entry_index,
    };
    return .ok;
}

pub fn dataApplyStoreLatestForTransition(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyGroupRequest,
    out_result: *kernel_owner_abi.DataApplyLatestResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const latest = handle.store.latestBatchForTransition(request.group_id) catch |err|
        return storageOwnerStatusFromError(err);
    out_result.* = dataApplyLatestResult(latest);
    return .ok;
}

pub fn dataApplyStoreRaftBatchProtocolVersion(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyGroupRequest,
    out_version: *u16,
) callconv(.c) kernel_owner_abi.Status {
    out_version.* = 0;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    out_version.* = handle.store.raftBatchProtocolVersionForRequest(request.group_id) catch |err|
        return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn dataApplyStoreProjection(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyProjectionRequest,
    out_result: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.expected.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const alloc = handle.alloc;
    const encoded = switch (request.kind) {
        .topology_rejection => blk: {
            const value = handle.store.topologyRejection(alloc, request.group_id, request.after_sequence) catch |err|
                return storageOwnerStatusFromError(err);
            break :blk std.json.Stringify.valueAlloc(alloc, value, .{}) catch |err|
                return storageOwnerStatusFromError(err);
        },
        .current_merge_source => blk: {
            const value = handle.store.currentMergeSourceState(alloc, request.group_id) catch |err|
                return storageOwnerStatusFromError(err);
            break :blk std.json.Stringify.valueAlloc(alloc, value, .{}) catch |err|
                return storageOwnerStatusFromError(err);
        },
        .current_merge_receiver => blk: {
            var value = handle.store.currentMergeReceiverState(alloc, request.group_id) catch |err|
                return storageOwnerStatusFromError(err);
            defer if (value) |*state| state.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, value, .{}) catch |err|
                return storageOwnerStatusFromError(err);
        },
        .observe_split_control => blk: {
            var observation = handle.store.observeSplitControl(alloc, request.group_id) catch |err|
                return storageOwnerStatusFromError(err);
            defer observation.deinit(alloc);
            break :blk data_raft_projection_wire.encodeSplitControlAlloc(alloc, observation) catch |err|
                return storageOwnerStatusFromError(err);
        },
        .current_range => blk: {
            const byte_range = handle.store.currentRange(alloc, request.group_id) catch |err|
                return storageOwnerStatusFromError(err);
            defer {
                if (byte_range.start.len > 0) alloc.free(@constCast(byte_range.start));
                if (byte_range.end.len > 0) alloc.free(@constCast(byte_range.end));
            }
            break :blk data_raft_projection_wire.encodeRangeAlloc(alloc, byte_range) catch |err|
                return storageOwnerStatusFromError(err);
        },
        .group_state_page, .group_state_keys_page => blk: {
            const max_entries = std.math.cast(usize, request.max_entries) orelse return .invalid_argument;
            const max_bytes = std.math.cast(usize, request.max_bytes) orelse return .invalid_argument;
            if (max_entries == 0 or max_bytes == 0) return .invalid_argument;
            var page = (if (request.kind == .group_state_keys_page) handle.store.groupStateKeysPageInRange(
                alloc,
                request.group_id,
                .{ .start = request.range_start.slice(), .end = request.range_end.slice() },
                if (request.after_key.len == 0) null else request.after_key.slice(),
                max_entries,
                max_bytes,
            ) else handle.store.groupStatePageInRange(
                alloc,
                request.group_id,
                .{ .start = request.range_start.slice(), .end = request.range_end.slice() },
                if (request.after_key.len == 0) null else request.after_key.slice(),
                max_entries,
                max_bytes,
            )) catch |err| return storageOwnerStatusFromError(err);
            defer page.deinit(alloc);
            break :blk data_raft_projection_wire.encodeGroupStatePageAlloc(alloc, page) catch |err|
                return storageOwnerStatusFromError(err);
        },
        .split_deltas_page => blk: {
            const max_entries = std.math.cast(usize, request.max_entries) orelse return .invalid_argument;
            const max_bytes = std.math.cast(usize, request.max_bytes) orelse return .invalid_argument;
            if (max_entries == 0 or max_bytes == 0) return .invalid_argument;
            const deltas = handle.store.listSplitDeltasPage(
                alloc,
                request.group_id,
                request.after_sequence,
                request.through_sequence,
                max_entries,
                max_bytes,
            ) catch |err| return storageOwnerStatusFromError(err);
            defer antfly.shard.freeDeltas(alloc, deltas);
            break :blk data_raft_projection_wire.encodeSplitDeltasAlloc(alloc, deltas) catch |err|
                return storageOwnerStatusFromError(err);
        },
        .capture_verified_handoff_metadata => blk: {
            const expected = (dataApplyExpectedBatch(request.expected) catch return .invalid_argument) orelse
                return .invalid_argument;
            const root_incarnation = std.mem.readInt(u128, &request.root_incarnation_le, .little);
            const handoff = handle.store.captureVerifiedSplitHandoffMetadataAtRootIncarnation(
                alloc,
                request.group_id,
                expected,
                root_incarnation,
            ) catch |err| return storageOwnerStatusFromError(err);
            const value = handoff orelse return .not_found;
            defer antfly.data_snapshot.freeHandoffMetadata(alloc, value);
            break :blk data_raft_projection_wire.encodeHandoffMetadataAlloc(alloc, value) catch |err|
                return storageOwnerStatusFromError(err);
        },
    };
    out_result.* = .{
        .ptr = if (encoded.len == 0) null else encoded.ptr,
        .len = @intCast(encoded.len),
    };
    return .ok;
}

pub fn dataApplyStoreReconcileOwner(
    store_ptr: ?*anyopaque,
    owner_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyReconcileRequest,
    out_result: *kernel_owner_abi.DataApplyReconcileResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version or
        request.expected.version != kernel_owner_abi.abi_version)
        return .invalid_abi;
    const apply_handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const owner = asHandle(owner_ptr) orelse return .invalid_argument;
    if (owner.storage_owner_group_id != request.group_id) return .invalid_argument;
    const max_entries = std.math.cast(usize, request.max_page_entries) orelse return .invalid_argument;
    const max_bytes = std.math.cast(usize, request.max_page_bytes) orelse return .invalid_argument;
    if (max_entries == 0 or max_bytes == 0) return .invalid_argument;
    if (request.capture_handoff > 1) return .invalid_argument;
    const capture_handoff = request.capture_handoff != 0;
    const expected = dataApplyExpectedBatch(request.expected) catch return .invalid_argument;
    if (capture_handoff and expected == null) return .invalid_argument;
    if (owner.db.hasTopologySensitiveTransactions() catch |err| return storageOwnerStatusFromError(err))
        return .busy;
    const root_incarnation = owner.db.durableRootIncarnation() catch |err|
        return storageOwnerStatusFromError(err);
    const alloc = apply_handle.alloc;

    if (capture_handoff) {
        const handoff = apply_handle.store.captureVerifiedSplitHandoffMetadataAtRootIncarnation(
            alloc,
            request.group_id,
            expected.?,
            root_incarnation,
        ) catch |err| return storageOwnerStatusFromError(err);
        if (handoff) |value| {
            defer antfly.data_snapshot.freeHandoffMetadata(alloc, value);
            const encoded = data_raft_projection_wire.encodeHandoffMetadataAlloc(alloc, value) catch |err|
                return storageOwnerStatusFromError(err);
            out_result.* = .{
                .state = .handoff,
                .handoff_metadata = .{
                    .ptr = if (encoded.len == 0) null else encoded.ptr,
                    .len = @intCast(encoded.len),
                },
            };
            return .ok;
        }
    }

    const active_split = apply_handle.store.currentSplitState(alloc, request.group_id) catch |err|
        return storageOwnerStatusFromError(err);
    defer if (active_split) |state| antfly.data_snapshot.freeSplitState(alloc, state);
    const split_terminal = apply_handle.store.currentSplitTerminal(alloc, request.group_id) catch |err|
        return storageOwnerStatusFromError(err);
    defer if (split_terminal) |terminal| antfly.data_snapshot.freeSplitTerminal(alloc, terminal);
    var projected_range: ?data_raft_apply.AppliedDataRange = null;
    defer if (projected_range) |range| {
        if (range.start.len > 0) alloc.free(@constCast(range.start));
        if (range.end.len > 0) alloc.free(@constCast(range.end));
    };
    const byte_range = if (active_split) |state| blk: {
        const current = apply_handle.store.currentRange(alloc, request.group_id) catch |err|
            return storageOwnerStatusFromError(err);
        projected_range = current;
        break :blk data_raft_apply.AppliedDataRange{
            .start = current.start,
            .end = state.original_range_end,
        };
    } else if (split_terminal != null) blk: {
        // The replicated terminal owns the final range even while the physical
        // document delegate is still applying finalization after restart.
        const current = apply_handle.store.currentRange(alloc, request.group_id) catch |err|
            return storageOwnerStatusFromError(err);
        projected_range = current;
        break :blk current;
    } else owner.db.getRange();

    if (expected) |watermark| {
        const reconciled = apply_handle.store.reconcileGroupSnapshotFromAuthoritativeStoreAtRootIncarnation(
            alloc,
            request.group_id,
            watermark,
            root_incarnation,
            byte_range,
            owner.db.core.store,
            max_entries,
            max_bytes,
        ) catch |err| return storageOwnerStatusFromError(err);
        if (!reconciled) {
            out_result.state = .advanced;
            return .ok;
        }
        if (!capture_handoff) {
            out_result.state = .reconciled;
            return .ok;
        }
        const handoff = apply_handle.store.captureVerifiedSplitHandoffMetadataAtRootIncarnation(
            alloc,
            request.group_id,
            watermark,
            root_incarnation,
        ) catch |err| return storageOwnerStatusFromError(err);
        const value = handoff orelse {
            out_result.state = .advanced;
            return .ok;
        };
        defer antfly.data_snapshot.freeHandoffMetadata(alloc, value);
        const encoded = data_raft_projection_wire.encodeHandoffMetadataAlloc(alloc, value) catch |err|
            return storageOwnerStatusFromError(err);
        out_result.* = .{
            .state = .handoff,
            .handoff_metadata = .{
                .ptr = if (encoded.len == 0) null else encoded.ptr,
                .len = @intCast(encoded.len),
            },
        };
        return .ok;
    }
    const seeded = apply_handle.store.seedGroupSnapshotFromAuthoritativeStoreIfAbsent(
        alloc,
        request.group_id,
        root_incarnation,
        byte_range,
        owner.db.core.store,
        max_entries,
        max_bytes,
    ) catch |err| return storageOwnerStatusFromError(err);
    out_result.state = if (seeded) .reconciled else .advanced;
    return .ok;
}

fn dataApplyLatestResult(latest: ?data_raft_apply.AppliedDataBatch) kernel_owner_abi.DataApplyLatestResult {
    const value = latest orelse return .{};
    return .{
        .present = 1,
        .commit_index = value.commit_index,
        .entry_count = @intCast(value.entry_count),
        .normal_entry_count = @intCast(value.normal_entry_count),
        .admin_entry_count = @intCast(value.admin_entry_count),
        .last_entry_term = value.last_entry_term,
        .last_entry_index = value.last_entry_index,
    };
}

fn dataApplyExpectedBatch(value: kernel_owner_abi.DataApplyLatestResult) !?data_raft_apply.AppliedDataBatch {
    if (value.present == 0) return null;
    if (value.present != 1) return error.InvalidArgument;
    return .{
        .commit_index = value.commit_index,
        .entry_count = std.math.cast(usize, value.entry_count) orelse return error.InvalidArgument,
        .normal_entry_count = std.math.cast(usize, value.normal_entry_count) orelse return error.InvalidArgument,
        .admin_entry_count = std.math.cast(usize, value.admin_entry_count) orelse return error.InvalidArgument,
        .last_entry_term = value.last_entry_term,
        .last_entry_index = value.last_entry_index,
    };
}

pub fn dataApplyStoreRetainGroups(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyGroupsRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const groups = request.slice() orelse return .invalid_argument;
    handle.store.retainActiveGroups(groups) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn dataApplyStoreBeginGroupTransition(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.DataApplyGroupsRequest,
    out_transition: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_transition.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asDataApplyStore(store_ptr) orelse return .invalid_argument;
    const groups = request.slice() orelse return .invalid_argument;
    var transition = handle.store.beginActiveGroupTransition(groups) catch |err|
        return storageOwnerStatusFromError(err);
    errdefer transition.deinit();
    const owned = handle.alloc.create(DataApplyGroupTransitionHandle) catch return .out_of_memory;
    owned.* = .{ .transition = transition };
    out_transition.* = owned;
    return .ok;
}

pub fn dataApplyStoreCommitGroupTransition(
    transition_ptr: ?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    const handle = asDataApplyGroupTransition(transition_ptr) orelse return .invalid_argument;
    if (!handle.active) return .invalid_argument;
    handle.transition.commit();
    handle.active = false;
    return .ok;
}

pub fn dataApplyStoreAbortGroupTransition(
    transition_ptr: ?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    const handle = asDataApplyGroupTransition(transition_ptr) orelse return .invalid_argument;
    if (!handle.active) return .ok;
    handle.transition.abort();
    handle.active = false;
    return .ok;
}

pub fn dataApplyStoreDestroyGroupTransition(transition_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asDataApplyGroupTransition(transition_ptr) orelse return;
    if (handle.active) handle.transition.abort();
    handle.transition.deinit();
    std.heap.c_allocator.destroy(handle);
}

fn releaseBorrowedTransitionOwner(_: *anyopaque) void {}

fn validateLocalTransitionOwner(
    handle: *const Handle,
    group_id: u64,
    table_name: []const u8,
    table_id: u64,
    shard_id: u64,
    range_id: u64,
    allow_same_table_identity: bool,
) !void {
    if (handle.storage_owner_group_id != group_id or
        !std.mem.eql(u8, handle.storage_owner_table_name orelse return error.InvalidArgument, table_name) or
        handle.storage_owner_path == null)
    {
        return error.InvalidArgument;
    }
    const identity = handle.db.core.identity_namespace;
    if (identity.table_id != table_id) return error.DocIdentityNamespaceMismatch;
    if (!allow_same_table_identity and
        (identity.shard_id != shard_id or identity.range_id != range_id))
    {
        return error.DocIdentityNamespaceMismatch;
    }
}

fn localTransitionIdentity(
    request: *const kernel_owner_abi.LocalTransitionRequest,
    target: bool,
) db_mod.DocIdentityNamespace {
    return .{
        .table_id = request.table_id,
        .shard_id = if (target) request.target_identity_shard_id else request.source_identity_shard_id,
        .range_id = if (target) request.target_identity_range_id else request.source_identity_range_id,
    };
}

fn localTransitionSplitResult(status: anytype) kernel_owner_abi.LocalTransitionResult {
    return .{
        .kind = .split,
        .phase = @enumFromInt(@intFromEnum(status.phase)),
        .has_source_split_phase = @intFromBool(status.source_split_phase != null),
        .source_split_phase = if (status.source_split_phase) |phase| @intFromEnum(phase) else 0,
        .bootstrapped = @intFromBool(status.bootstrapped),
        .replay_required = @intFromBool(status.replay_required),
        .replay_caught_up = @intFromBool(status.replay_caught_up),
        .cutover_ready = @intFromBool(status.cutover_ready),
        .peer_ready_for_reads = @intFromBool(status.destination_ready_for_reads),
        .primary_delta_sequence = status.source_delta_sequence,
        .secondary_delta_sequence = status.dest_delta_sequence,
    };
}

fn localTransitionMergeResult(status: anytype) kernel_owner_abi.LocalTransitionResult {
    return .{
        .kind = .merge,
        .phase = @enumFromInt(@intFromEnum(status.phase)),
        .bootstrapped = @intFromBool(status.bootstrapped),
        .replay_required = @intFromBool(status.replay_required),
        .replay_caught_up = @intFromBool(status.replay_caught_up),
        .cutover_ready = @intFromBool(status.cutover_ready),
        .peer_ready_for_reads = @intFromBool(status.receiver_ready_for_reads),
        .receiver_accepts_donor_range = @intFromBool(status.receiver_accepts_donor_range),
        .allow_doc_identity_reassignment = @intFromBool(status.allow_doc_identity_reassignment),
        .primary_group_id = status.donor_group_id,
        .secondary_group_id = status.receiver_group_id,
        .primary_delta_sequence = status.donor_delta_sequence,
        .secondary_delta_sequence = status.receiver_delta_sequence,
        .receiver_identity_table_id = status.receiver_identity_reassignment_namespace_table_id,
        .receiver_identity_shard_id = status.receiver_identity_reassignment_namespace_shard_id,
        .receiver_identity_range_id = status.receiver_identity_reassignment_namespace_range_id,
    };
}

/// Execute one complete local transition phase against two resident opaque DB
/// owners. No database, backend, or per-record representation crosses the ABI.
pub fn storageOwnerLocalTransition(
    primary_owner_ptr: ?*anyopaque,
    secondary_owner_ptr: ?*anyopaque,
    apply_store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.LocalTransitionRequest,
    out_result: *kernel_owner_abi.LocalTransitionResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.transition_id == 0 or request.primary_group_id == 0 or
        request.secondary_group_id == 0 or request.primary_group_id == request.secondary_group_id or
        request.table_id == 0 or request.table_name.slice().len == 0 or
        request.indexes_json.slice().len == 0 or request.allow_doc_identity_reassignment > 1 or
        request.has_source_range_end > 1)
    {
        return .invalid_argument;
    }
    const primary = asHandle(primary_owner_ptr) orelse return .invalid_argument;
    const secondary = asHandle(secondary_owner_ptr) orelse return .invalid_argument;
    const table_name = request.table_name.slice();
    const is_merge = switch (request.action) {
        .observe_merge,
        .accept_merge_receiver,
        .catch_up_merge_receiver,
        .finalize_merge,
        .rollback_merge,
        => true,
        else => false,
    };
    validateLocalTransitionOwner(
        primary,
        request.primary_group_id,
        table_name,
        request.table_id,
        request.source_identity_shard_id,
        request.source_identity_range_id,
        false,
    ) catch |err| return storageOwnerStatusFromError(err);
    validateLocalTransitionOwner(
        secondary,
        request.secondary_group_id,
        table_name,
        request.table_id,
        request.target_identity_shard_id,
        request.target_identity_range_id,
        is_merge and request.allow_doc_identity_reassignment != 0,
    ) catch |err| return storageOwnerStatusFromError(err);
    if ((primary.db.hasTopologySensitiveTransactions() catch |err|
        return storageOwnerStatusFromError(err)) or
        (secondary.db.hasTopologySensitiveTransactions() catch |err|
            return storageOwnerStatusFromError(err)))
    {
        return .busy;
    }
    const primary_path = primary.storage_owner_path.?;
    const secondary_path = secondary.storage_owner_path.?;
    const apply_store = if (apply_store_ptr != null)
        &(asDataApplyStore(apply_store_ptr) orelse return .invalid_argument).store
    else
        null;
    const alloc = std.heap.c_allocator;

    switch (request.action) {
        .observe_split,
        .prepare_split_source,
        .start_split_source,
        .bootstrap_split_destination,
        .catch_up_split_destination,
        .finalize_split_source,
        .rollback_split,
        => {
            if (request.attempt_epoch == 0 or request.allow_doc_identity_reassignment != 0)
                return .invalid_argument;
            var runtime = raft_mod.SplitCoordinatorRuntime.init(alloc, .{
                .transition_id = request.transition_id,
                .attempt_epoch = request.attempt_epoch,
                .source_root_dir = primary_path,
                .dest_root_dir = secondary_path,
                .source_group_id = request.primary_group_id,
                .dest_group_id = request.secondary_group_id,
                .source_store = apply_store,
                .source_lease = .{
                    .db = &primary.db,
                    .ctx = primary,
                    .release_fn = releaseBorrowedTransitionOwner,
                },
                .dest = .{
                    .root_dir = secondary_path,
                    .db = .{ .identity_namespace = localTransitionIdentity(request, true) },
                },
                .dest_lease = .{
                    .db = &secondary.db,
                    .ctx = secondary,
                    .release_fn = releaseBorrowedTransitionOwner,
                },
            }) catch |err| return storageOwnerStatusFromError(err);
            defer runtime.deinit();
            const transition = runtime.runtime();
            switch (request.action) {
                .observe_split => {
                    const status = transition.observeStatus(
                        request.transition_id,
                        request.attempt_epoch,
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                    out_result.* = localTransitionSplitResult(status);
                },
                .prepare_split_source => _ = transition.prepareSource(
                    request.transition_id,
                    request.attempt_epoch,
                    request.primary_group_id,
                    request.secondary_group_id,
                    request.split_key.slice(),
                    if (request.has_source_range_end != 0) request.source_range_end.slice() else null,
                ) catch |err| return storageOwnerStatusFromError(err),
                .start_split_source => _ = transition.startSource(
                    request.transition_id,
                    request.attempt_epoch,
                    request.primary_group_id,
                    request.secondary_group_id,
                ) catch |err| return storageOwnerStatusFromError(err),
                .bootstrap_split_destination => _ = transition.bootstrapDestination(
                    request.transition_id,
                    request.attempt_epoch,
                    request.primary_group_id,
                    request.secondary_group_id,
                ) catch |err| return storageOwnerStatusFromError(err),
                .catch_up_split_destination => _ = transition.catchUpDestination(
                    request.transition_id,
                    request.attempt_epoch,
                    request.primary_group_id,
                    request.secondary_group_id,
                ) catch |err| return storageOwnerStatusFromError(err),
                .finalize_split_source => _ = transition.finalizeSource(
                    request.transition_id,
                    request.attempt_epoch,
                    request.primary_group_id,
                    request.secondary_group_id,
                ) catch |err| return storageOwnerStatusFromError(err),
                .rollback_split => _ = transition.rollbackSource(
                    request.transition_id,
                    request.attempt_epoch,
                    request.primary_group_id,
                    request.secondary_group_id,
                ) catch |err| return storageOwnerStatusFromError(err),
                else => unreachable,
            }
        },
        .observe_merge,
        .accept_merge_receiver,
        .catch_up_merge_receiver,
        .finalize_merge,
        .rollback_merge,
        => {
            var runtime = raft_mod.MergeCoordinatorRuntime.init(alloc, .{
                .donor_root_dir = primary_path,
                .receiver_root_dir = secondary_path,
                .donor_group_id = request.primary_group_id,
                .receiver_group_id = request.secondary_group_id,
                .donor_store = apply_store,
                .donor_lease = .{
                    .db = &primary.db,
                    .ctx = primary,
                    .release_fn = releaseBorrowedTransitionOwner,
                },
                .receiver = .{
                    .root_dir = secondary_path,
                    .db = .{
                        .identity_namespace = localTransitionIdentity(request, true),
                        .prefer_existing_identity_namespace = true,
                    },
                },
                .receiver_lease = .{
                    .db = &secondary.db,
                    .ctx = secondary,
                    .release_fn = releaseBorrowedTransitionOwner,
                },
                .receiver_identity_reassignment_namespace = localTransitionIdentity(request, true),
            }) catch |err| return storageOwnerStatusFromError(err);
            defer runtime.deinit();
            const transition = runtime.runtime();
            switch (request.action) {
                .observe_merge => {
                    const status = transition.observeStatus(
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                    out_result.* = localTransitionMergeResult(status);
                },
                .accept_merge_receiver => {
                    if (request.allow_doc_identity_reassignment != 0) transition.recordDocIdentityReassignment(
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                    transition.acceptReceiver(
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                },
                .catch_up_merge_receiver => {
                    if (request.allow_doc_identity_reassignment != 0) transition.recordDocIdentityReassignment(
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                    _ = transition.catchUpReceiver(
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                },
                .finalize_merge => {
                    if (request.allow_doc_identity_reassignment != 0) transition.recordDocIdentityReassignment(
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                    _ = transition.finalizeMerge(
                        request.primary_group_id,
                        request.secondary_group_id,
                    ) catch |err| return storageOwnerStatusFromError(err);
                },
                .rollback_merge => _ = transition.rollbackMerge(
                    request.primary_group_id,
                    request.secondary_group_id,
                ) catch |err| return storageOwnerStatusFromError(err),
                else => unreachable,
            }
        },
    }
    return .ok;
}

fn storageOwnerTargetAdvanced(
    ptr: *anyopaque,
    table_name: []const u8,
    group_id: u64,
    _: ?*db_mod.DB,
    event: db_mod.QueryVisibilityEvent,
) void {
    if (event.change != .target_advanced) return;
    const handle: *Handle = @ptrCast(@alignCast(ptr));
    const observer = handle.storage_owner_target_observer;
    const notify = observer.notify orelse return;
    const identities_json = if (event.target_scope_known)
        std.json.Stringify.valueAlloc(handle.alloc, event.target_indexes, .{}) catch null
    else
        null;
    defer if (identities_json) |json| handle.alloc.free(json);
    notify(observer.ctx, .fromSlice(table_name), group_id, event.target_sequence orelse 0, @intFromBool(event.target_sequence != null), .fromSlice(identities_json orelse ""));
}

pub fn storageOwnerOpen(
    request: *const kernel_owner_abi.OpenRequest,
    out_owner: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_owner.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const path = request.path.slice();
    const table_name = request.table_name.slice();
    if (path.len == 0 or table_name.len == 0) return .invalid_argument;

    const identity_namespace: ?db_mod.DocIdentityNamespace = if (request.has_identity_namespace != 0)
        .{
            .table_id = request.identity_table_id,
            .shard_id = request.identity_shard_id,
            .range_id = request.identity_range_id,
        }
    else
        null;
    const owner_context = asStorageOwnerContext(request.context);
    const alloc = if (owner_context) |context| context.alloc else std.heap.c_allocator;
    // A metadata restore intent can become visible before Raft bootstrap has
    // imported this replica. Do not create an empty DB and retain its reader
    // lease: that would prevent bootstrap from ever publishing the import.
    // Pin the validated generation until DB.open acquires its own read lease.
    var restore_io_impl: std.Io.Threaded = undefined;
    var owns_restore_io = false;
    defer if (owns_restore_io) restore_io_impl.deinit();
    var restore_lease: ?db_mod.generation_lifecycle.ReadLease = null;
    defer if (restore_lease) |*lease| lease.deinit();
    if (request.restore.required != 0) {
        const io = if (owner_context) |context|
            context.backend_runtime.ptr().filesystemIo() orelse return storageOwnerStatusFromError(error.BackendRuntimeIoUnavailable)
        else io: {
            restore_io_impl = std.Io.Threaded.init(alloc, .{});
            owns_restore_io = true;
            break :io restore_io_impl.io();
        };
        restore_lease = antfly.restore_admission.acquire(alloc, io, path, request.group_id, .{
            .backup_id = request.restore.backup_id.slice(),
            .location = request.restore.location.slice(),
            .snapshot_path = request.restore.snapshot_path.slice(),
            .artifact_sha256 = request.restore.artifact_sha256.slice(),
            .native_manifest_size_bytes = request.restore.native_manifest_size_bytes,
            .native_manifest_sha256 = request.restore.native_manifest_sha256.slice(),
        }) catch |err| return storageOwnerStatusFromError(err);
    }
    if ((request.target_observer.ctx == null) != (request.target_observer.notify == null))
        return .invalid_argument;
    const recovery_config = request.transaction_recovery;
    if (recovery_config.enabled != 0) {
        if (recovery_config.callback_ctx == null or recovery_config.resolve_participant_fn == null)
            return .invalid_argument;
        if (recovery_config.replicated_metadata != 0 and
            (recovery_config.owns_recovery_fn == null or
                recovery_config.acknowledge_participant_fn == null or
                recovery_config.cleanup_transaction_fn == null))
            return .invalid_argument;
    }
    const runtime_hooks_config = request.runtime_hooks;
    if ((runtime_hooks_config.coordinated_ttl_ctx == null) != (runtime_hooks_config.coordinated_ttl_enqueue_fn == null))
        return .invalid_argument;
    const candidate_configured = runtime_hooks_config.resolution_candidates.get_fn != null or
        runtime_hooks_config.resolution_candidates.scan_prefix_fn != null or
        runtime_hooks_config.resolution_candidates.nearest_fn != null;
    if (candidate_configured and
        (runtime_hooks_config.resolution_candidates.callback_ctx == null or
            runtime_hooks_config.resolution_candidates.get_fn == null))
        return .invalid_argument;
    const entity_sink_configured = runtime_hooks_config.entity_sink.upsert_fn != null or
        runtime_hooks_config.entity_sink.upsert_batch_fn != null;
    if (entity_sink_configured and
        (runtime_hooks_config.entity_sink.callback_ctx == null or
            runtime_hooks_config.entity_sink.upsert_fn == null))
        return .invalid_argument;
    if ((runtime_hooks_config.native_authority_ctx == null) != (runtime_hooks_config.native_authority_fn == null))
        return .invalid_argument;
    if ((runtime_hooks_config.promotion_owner_ctx == null) !=
        (runtime_hooks_config.promotion_owner_fn == null))
        return .invalid_argument;
    var success = false;
    var recovery: ?*StorageOwnerTransactionRecovery = null;
    if (recovery_config.enabled != 0) {
        recovery = alloc.create(StorageOwnerTransactionRecovery) catch return .out_of_memory;
        recovery.?.* = StorageOwnerTransactionRecovery.init(alloc, recovery_config) catch {
            alloc.destroy(recovery.?);
            return .out_of_memory;
        };
    }
    defer if (!success) if (recovery) |value| {
        value.deinit();
        alloc.destroy(value);
    };
    var runtime_hooks: ?*StorageOwnerRuntimeHooks = null;
    if (candidate_configured or entity_sink_configured or runtime_hooks_config.promotion_owner_fn != null or runtime_hooks_config.native_authority_fn != null or runtime_hooks_config.coordinated_ttl_enqueue_fn != null) {
        runtime_hooks = alloc.create(StorageOwnerRuntimeHooks) catch return .out_of_memory;
        runtime_hooks.?.* = .{ .config = runtime_hooks_config, .group_id = request.group_id };
    }
    defer if (!success) if (runtime_hooks) |value| alloc.destroy(value);
    if (owner_context) |context| context.acquire();
    var context_borrowed = owner_context != null;
    defer if (context_borrowed) owner_context.?.release();
    const prepared_schema = local_write.prepareOwnerSchemaBeforeIndexLoad(alloc, request.schema_json.slice()) catch |err| return storageOwnerStatusFromError(err);
    defer local_write.freeOwnerSchemaBeforeIndexLoad(alloc, prepared_schema);
    if (request.restore_cancel_recovery > 1 or request.restore_ha_replay > 1 or
        (request.restore_cancel_recovery != 0 and request.restore_ha_replay != 0) or
        ((request.restore_cancel_recovery != 0 or request.restore_ha_replay != 0) and request.restore_bootstrap_json.len == 0) or request.restore_bootstrap_json.len > 16 * 1024 * 1024) return .invalid_argument;
    var restore_bootstrap: ?std.json.Parsed(antfly.capi_dependencies.storage_db_restore_staging_contract.OwnerBootstrap) = null;
    defer if (restore_bootstrap) |*parsed| parsed.deinit();
    if (request.restore_bootstrap_json.len != 0) {
        restore_bootstrap = std.json.parseFromSlice(antfly.capi_dependencies.storage_db_restore_staging_contract.OwnerBootstrap, alloc, request.restore_bootstrap_json.slice(), .{ .ignore_unknown_fields = false }) catch |err| return storageOwnerStatusFromError(err);
        const bootstrap = restore_bootstrap.?.value;
        bootstrap.validate() catch |err| return storageOwnerStatusFromError(err);
        const namespace = identity_namespace orelse return .invalid_argument;
        if (!namespace.eql(bootstrap.scope.target_namespace) or !std.mem.eql(u8, bootstrap.table_name, table_name) or !std.mem.eql(u8, bootstrap.schema_json, request.schema_json.slice()) or !std.mem.eql(u8, bootstrap.indexes_json, request.indexes_json.slice())) return storageOwnerStatusFromError(error.RestoreStagingScopeChanged);
    }
    var open_options = db_mod.OpenOptions{
        .online_source_authority = std.enums.fromInt(antfly.capi_dependencies.storage_source_authority.Kind, request.online_source_authority) orelse return .invalid_argument,
        .table_storage = switch (request.dense_embedding_storage) {
            .persisted => null,
            .primary_lsm => .{ .dense_embeddings = .primary_lsm },
            .vector_store => .{ .dense_embeddings = .vector_store },
            _ => return .invalid_argument,
        },
        .schema_before_index_load = prepared_schema,
        .lsm_cache = if (owner_context) |context| &context.resources.lsm_cache else null,
        .hbc_cache = if (owner_context) |context| &context.resources.hbc_cache else null,
        .lsm_root_generation = request.lsm_root_generation,
        .resource_manager = if (owner_context) |context| &context.resources.resource_manager else null,
        .backend_runtime = if (owner_context) |context| context.backend_runtime.ptr() else null,
        .identity_namespace = identity_namespace,
        .prefer_existing_identity_namespace = identity_namespace != null,
        .transaction_recovery = if (recovery) |value| value.dbConfig() else .{},
        .resolution_candidate_source = if (runtime_hooks) |value| value.candidateSource() else null,
        .entity_sink = if (runtime_hooks) |value| value.entitySink() else null,
        .promotion_owner = if (runtime_hooks) |value| value.promotionOwner() else null,
        // Reconcile the authoritative resolver catalog before autonomous
        // replay can hold its catalog fence or invoke distributed callbacks.
        .start_resolver_workers = false,
        .index_backends = .{ .dense_native_migration_policy_source = if (runtime_hooks) |value| value.nativeMigrationPolicy() else null },
        .secret_store = if (owner_context) |context| context.secret_store else null,
        .remote_content = if (owner_context) |context| context.remoteContent() else null,
        .start_optional_runtimes = restore_bootstrap == null,
        .start_index_workers = restore_bootstrap == null,
    };
    if (request.has_initial_range > 1 or request.initial_range_control.version != kernel_owner_abi.abi_version or request.initial_range_control.has_execution_deadline > 1) return .invalid_argument;
    if (request.has_initial_range != 0) {
        if (identity_namespace == null or request.initial_range_start.len > 1024 * 1024 or request.initial_range_end.len > 1024 * 1024) return .invalid_argument;
        // Private restore bootstrap supplies its own exact durable range;
        // ordinary descriptors only initialize an absent initial owner range.
        if (restore_bootstrap != null) return .invalid_argument;
        open_options.initial_owner_range = .{
            .range = .{ .start = request.initial_range_start.slice(), .end = request.initial_range_end.slice() },
            .namespace = identity_namespace.?,
            .cancellation = ownerQueryCancellation(&request.initial_range_control),
            .deadline_ns = if (request.initial_range_control.has_execution_deadline != 0) request.initial_range_control.execution_deadline_ns else null,
        };
    } else if (request.initial_range_start.len != 0 or request.initial_range_end.len != 0) return .invalid_argument;
    if (owner_context) |context| if (context.lite_backend) |*backend|
        backend.configureDbOpenOptionsForNamespace(&open_options, path) catch |err|
            return storageOwnerStatusFromError(err);
    const owned_path = alloc.dupe(u8, path) catch return .out_of_memory;
    defer if (!success) alloc.free(owned_path);
    const owned_table_name = alloc.dupe(u8, table_name) catch return .out_of_memory;
    defer if (!success) alloc.free(owned_table_name);
    const handle = alloc.create(Handle) catch return .out_of_memory;
    defer if (!success) alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .db = db_mod.DB.open(alloc, path, open_options) catch |err| {
            std.log.err("storage owner open failed table={s} group_id={} err={s}", .{
                table_name, request.group_id, @errorName(err),
            });
            return storageOwnerStatusFromError(err);
        },
        .storage_owner_path = owned_path,
        .storage_owner_table_name = owned_table_name,
        .storage_owner_group_id = request.group_id,
        .storage_owner_root_generation = request.lsm_root_generation,
        .storage_owner_context = owner_context,
        .storage_owner_transaction_recovery = recovery,
        .storage_owner_runtime_hooks = runtime_hooks,
        .storage_owner_target_observer = request.target_observer,
    };
    defer if (!success) handle.db.close();
    if (runtime_hooks) |hooks| handle.db.setCoordinatedTtl(hooks.coordinatedTtlPort(), request.group_id);
    if (request.target_observer.notify != null) handle.db.setQueryVisibilityHook(.{
        .ptr = handle,
        .table_name = owned_table_name,
        .group_id = request.group_id,
        .on_change = storageOwnerTargetAdvanced,
    });
    // Configuration can start DB-owned workers. Publish their pointers only
    // after the DB occupies its final address, and drain them on failure.
    if (restore_bootstrap) |bootstrap| {
        local_write.configureRestoreOwnerDb(alloc, &handle.db, bootstrap.value, request.restore_cancel_recovery != 0, request.restore_ha_replay != 0) catch |err| return storageOwnerStatusFromError(err);
    } else local_write.configureStorageKernelOwnerDb(
        alloc,
        &handle.db,
        table_name,
        request.schema_json.slice(),
        request.indexes_json.slice(),
        if (owner_context) |context| context.backend_runtime.ptr() else null,
        if (owner_context) |context| context.antflyProvider() else null,
        if (owner_context) |context| context.secret_store else null,
        if (owner_context) |context| context.remoteContent() else null,
        &handle.storage_owner_managed_config,
    ) catch |err| return storageOwnerStatusFromError(err);
    // DB.open returns by value. Only now is the compiled owner's DB at its
    // permanent address with configuration installed; use the same startup as
    // resident caches so relational builds, retirement and durable outboxes
    // make progress. Hidden restore owners must remain unpublished/quiescent.
    if (restore_bootstrap == null) {
        handle.db.activateResolverReplayRuntimes() catch |err| return storageOwnerStatusFromError(err);
        handle.db.startResidentBackgroundWorkersIfNeeded();
    }
    // Register as the last fallible step: on failure the defers above close
    // the DB and release the borrowed context exactly once, as for every
    // earlier failure. (publishHandle's close-on-failure would release the
    // context a second time.)
    const owner_id = handle_registry.register(handle) catch |err| return storageOwnerStatusFromError(err);
    success = true;
    out_owner.* = owner_id;
    context_borrowed = false;
    return .ok;
}

pub fn storageOwnerClose(owner: ?*anyopaque) callconv(.c) void {
    antfly_db_close(owner);
}

pub fn storageOwnerConfigure(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ConfigureRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    local_write.configureStorageKernelOwnerDb(
        handle.alloc,
        &handle.db,
        request.table_name.slice(),
        request.schema_json.slice(),
        request.indexes_json.slice(),
        if (handle.storage_owner_context) |context| context.backend_runtime.ptr() else null,
        if (handle.storage_owner_context) |context| context.antflyProvider() else null,
        if (handle.storage_owner_context) |context| context.secret_store else null,
        if (handle.storage_owner_context) |context| context.remoteContent() else null,
        &handle.storage_owner_managed_config,
    ) catch |err| {
        std.log.err("storage owner configure failed table={s} err={s}", .{
            handle.storage_owner_table_name orelse "",
            @errorName(err),
        });
        return storageOwnerStatusFromError(err);
    };
    return .ok;
}

const OwnerRepairControls = struct {
    wire: kernel_owner_abi.RepairControls,
    fn cancelled(ptr: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return if (self.wire.cancelled) |check| check(self.wire.context) != 0 else false;
    }
    fn yieldRequested(ptr: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return if (self.wire.yield_requested) |check| check(self.wire.context) != 0 else false;
    }
    fn activationAllowed(ptr: *anyopaque) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return if (self.wire.activation_allowed) |check| check(self.wire.context) != 0 else true;
    }
    fn options(self: *@This()) db_mod.types.ArtifactRepairRunOptions {
        return .{
            .cancel_check = if (self.wire.cancelled != null) .{ .ptr = self, .is_requested = cancelled } else null,
            .yield_check = if (self.wire.yield_requested != null) .{ .ptr = self, .is_requested = yieldRequested } else null,
            .activation_check = if (self.wire.activation_allowed != null) .{ .ptr = self, .is_current_owner = activationAllowed } else null,
            .owner_epoch = self.wire.owner_epoch,
            .capacity_domain_id = (@as(u128, self.wire.capacity_domain_hi) << 64) | self.wire.capacity_domain_lo,
            .estimated_candidate_bytes = self.wire.estimated_candidate_bytes,
            .max_activation_gap_sequences = self.wire.max_activation_gap_sequences,
            .max_convergence_rounds = self.wire.max_convergence_rounds,
            .max_activation_pause_ms = self.wire.max_activation_pause_ms,
        };
    }
};

pub fn storageOwnerReconcile(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ReconcileRequest,
    out_result: *kernel_owner_abi.ReconcileResult,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    out_result.* = .{};
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    var controls = OwnerRepairControls{ .wire = request.repair_controls };
    const reconciled = if (request.repair_only != 0)
        local_write.repairStorageKernelOwnerDb(handle.alloc, &handle.db, if (request.target_index_name.slice().len == 0) null else request.target_index_name.slice(), request.advance_index_repair != 0, controls.options()) catch |err| return storageOwnerStatusFromError(err)
    else
        local_write.reconcileStorageKernelOwnerDb(
            handle.alloc,
            &handle.db,
            request.table_name.slice(),
            request.schema_json.slice(),
            request.indexes_json.slice(),
            if (request.target_index_name.slice().len == 0) null else request.target_index_name.slice(),
            request.advance_index_repair != 0,
            if (handle.storage_owner_context) |context| context.backend_runtime.ptr() else null,
            if (handle.storage_owner_context) |context| context.antflyProvider() else null,
            &handle.storage_owner_managed_config,
        ) catch |err| return storageOwnerStatusFromError(err);
    out_result.* = .{
        .state = switch (reconciled.state) {
            .complete => .complete,
            .repair_pending => .repair_pending,
            .busy => .busy,
            .degraded => .degraded,
            .restore_repair_pending => .restore_repair_pending,
        },
        .indexes_added = @intCast(reconciled.indexes_added),
        .indexes_removed = @intCast(reconciled.indexes_removed),
        .indexes_pending = @intCast(reconciled.indexes_pending),
        .repair_discovered = @intCast(reconciled.repair_discovered),
        .repair_attempted = @intCast(reconciled.repair_attempted),
        .repair_repaired = @intCast(reconciled.repair_repaired),
        .repair_remaining = @intCast(reconciled.repair_remaining),
        .repair_terminal = @intCast(reconciled.repair_terminal),
        .repair_paused = @intCast(reconciled.repair_paused),
        .repair_busy = @intCast(reconciled.repair_busy),
        .repair_disk_waits = @intCast(reconciled.repair_disk_waits),
        .next_retry_at_ms = reconciled.next_retry_at_ms,
        .restore_repair_attempted = @intCast(reconciled.restore_repair_attempted),
        .restore_repair_progressed = @intCast(reconciled.restore_repair_progressed),
        .restore_repair_pending = @intCast(reconciled.restore_repair_pending),
    };
    return .ok;
}

pub fn storageOwnerPreflightWriteAdmission(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.TableRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    return if (handle.db.denseRepairWriteBackpressured()) .dense_repair_backpressure else .ok;
}

pub fn storageOwnerFindMedianKey(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.TableRequest,
    out_key: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_key.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const key = handle.db.findMedianKey(handle.alloc) catch |err| switch (err) {
        error.NotFound => return .not_found,
        else => return storageOwnerStatusFromError(err),
    };
    out_key.* = .{ .ptr = key.ptr, .len = @intCast(key.len) };
    return .ok;
}

const StorageOwnerBulkCallbacks = struct {
    request: *const kernel_owner_abi.BulkFinishRequest,

    fn progress(ptr: *anyopaque, progress_value: backend_types.BulkIngestFinishOptions.Progress) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.request.progress_fn orelse return;
        const abi_progress = kernel_owner_abi.BulkProgress{
            .phase = switch (progress_value.phase) {
                .begin => .begin,
                .split => .split,
                .publish => .publish,
                .complete => .complete,
            },
            .publish_window = progress_value.publish_window,
            .split_steps = progress_value.split_steps,
            .deferred_leaf_splits = progress_value.deferred_leaf_splits,
            .elapsed_ns = progress_value.elapsed_ns,
        };
        callback(self.request.callback_ctx, &abi_progress);
    }

    fn admission(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.request.admission_fn orelse return;
        return kernel_error_identity.statusToError(callback(self.request.callback_ctx));
    }
};

pub fn storageOwnerBulkBegin(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.TableRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    handle.db.beginBulkIngestSession() catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageOwnerBulkFinish(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.BulkFinishRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const max_deferred_l0_runs = if (request.has_max_deferred_l0_runs != 0)
        std.math.cast(usize, request.max_deferred_l0_runs) orelse return .invalid_argument
    else
        null;
    const max_foreground_compaction_steps = std.math.cast(usize, request.max_foreground_compaction_steps) orelse
        return .invalid_argument;
    const max_deferred_hbc_leaf_splits_per_publish = if (request.has_max_deferred_hbc_leaf_splits_per_publish != 0)
        std.math.cast(usize, request.max_deferred_hbc_leaf_splits_per_publish) orelse return .invalid_argument
    else
        null;
    const max_deferred_hbc_leaf_split_members_per_publish = if (request.has_max_deferred_hbc_leaf_split_members_per_publish != 0)
        std.math.cast(usize, request.max_deferred_hbc_leaf_split_members_per_publish) orelse return .invalid_argument
    else
        null;
    const bulk_rebuild_hbc_leaf_min_members = if (request.has_bulk_rebuild_hbc_leaf_min_members != 0)
        std.math.cast(usize, request.bulk_rebuild_hbc_leaf_min_members) orelse return .invalid_argument
    else
        null;
    var callbacks = StorageOwnerBulkCallbacks{ .request = request };
    handle.db.finishBulkIngestSessionWithOptions(.{
        .compact = request.compact != 0,
        .flush = request.flush != 0,
        .max_deferred_l0_runs = max_deferred_l0_runs,
        .max_foreground_compaction_steps = max_foreground_compaction_steps,
        .max_foreground_compaction_input_bytes = if (request.has_max_foreground_compaction_input_bytes != 0)
            request.max_foreground_compaction_input_bytes
        else
            null,
        .max_foreground_compaction_ns = if (request.has_max_foreground_compaction_ns != 0)
            request.max_foreground_compaction_ns
        else
            null,
        .max_deferred_hbc_leaf_splits_per_publish = max_deferred_hbc_leaf_splits_per_publish,
        .max_deferred_hbc_leaf_split_members_per_publish = max_deferred_hbc_leaf_split_members_per_publish,
        .bulk_rebuild_hbc_leaf_min_members = bulk_rebuild_hbc_leaf_min_members,
        .progress_ctx = if (request.progress_fn != null) &callbacks else null,
        .progress_fn = if (request.progress_fn != null) StorageOwnerBulkCallbacks.progress else null,
        .admission_ctx = if (request.admission_fn != null) &callbacks else null,
        .admission_fn = if (request.admission_fn != null) StorageOwnerBulkCallbacks.admission else null,
    }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageOwnerBulkAbort(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.TableRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    handle.db.abortBulkIngestSession();
    return .ok;
}

fn storageOwnerOperationTableName(
    handle: *const Handle,
    request: *const kernel_owner_abi.JsonOperationRequest,
) ?[]const u8 {
    return storageOwnerTableName(handle, request.table_name);
}

fn storageHASeedFailure(
    err: anyerror,
    operation: kernel_owner_abi.HASeedOperation,
    out_failure: *kernel_owner_abi.FailureIdentity,
) kernel_owner_abi.Status {
    out_failure.* = kernel_error_identity.failureFromError(
        err,
        .storage_owner,
        kernel_owner_abi.abi_version,
        @intFromEnum(operation),
    );
    return out_failure.status;
}

fn validateHASeedRequest(
    request: *const kernel_owner_abi.HASeedJsonRequest,
    expected_operation: kernel_owner_abi.HASeedOperation,
) ![]const u8 {
    if (request.version != kernel_owner_abi.abi_version)
        return error.InvalidAbiVersion;
    if (request.operation != expected_operation)
        return error.InvalidArgument;
    const json = request.request_json.slice();
    if (json.len == 0) return error.InvalidArgument;
    return json;
}

pub fn storageHASeedActivateJson(
    request: *const kernel_owner_abi.HASeedJsonRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    const operation = kernel_owner_abi.HASeedOperation.activate;
    const request_json = validateHASeedRequest(request, operation) catch |err|
        return storageHASeedFailure(err, operation, out_failure);
    const alloc = std.heap.c_allocator;
    var parsed = std.json.parseFromSlice(ha_seed_activation.ActivateRequest, alloc, request_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return storageHASeedFailure(error.InvalidArgument, operation, out_failure);
    defer parsed.deinit();
    var result = ha_seed_activation.activate(alloc, parsed.value) catch |err|
        return storageHASeedFailure(err, operation, out_failure);
    alloc.free(result.generation_path);
    const response = result.active_receipt_json;
    result = undefined;
    out_response.* = .{
        .ptr = if (response.len == 0) null else response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

pub fn storageHASeedValidateJson(
    request: *const kernel_owner_abi.HASeedJsonRequest,
    out_result: *kernel_owner_abi.HASeedValidationResult,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    out_failure.* = .{};
    const operation = kernel_owner_abi.HASeedOperation.validate_activated_generation;
    const request_json = validateHASeedRequest(request, operation) catch |err|
        return storageHASeedFailure(err, operation, out_failure);
    const alloc = std.heap.c_allocator;
    var parsed = std.json.parseFromSlice(ha_seed_activation.StartupExpectation, alloc, request_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return storageHASeedFailure(error.InvalidArgument, operation, out_failure);
    defer parsed.deinit();
    out_result.checkpoint_lsn = ha_seed_activation.validateActivatedGeneration(alloc, parsed.value) catch |err|
        return storageHASeedFailure(err, operation, out_failure);
    return .ok;
}

pub fn storageHASeedPruneJson(
    request: *const kernel_owner_abi.HASeedJsonRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    const operation = kernel_owner_abi.HASeedOperation.prune_activated_generations;
    const request_json = validateHASeedRequest(request, operation) catch |err|
        return storageHASeedFailure(err, operation, out_failure);
    const alloc = std.heap.c_allocator;
    var parsed = std.json.parseFromSlice(ha_seed_activation.ActivatedGenerationGCRequest, alloc, request_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return storageHASeedFailure(error.InvalidArgument, operation, out_failure);
    defer parsed.deinit();
    var result = ha_seed_activation.pruneActivatedGenerations(alloc, parsed.value) catch |err|
        return storageHASeedFailure(err, operation, out_failure);
    const response = result.result_json;
    result = undefined;
    out_response.* = .{
        .ptr = if (response.len == 0) null else response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

test "storage HA seed boundary preserves status and exact failure identity" {
    var response: kernel_owner_abi.OwnedBytes = .{};
    var failure: kernel_owner_abi.FailureIdentity = .{};
    const status = storageHASeedActivateJson(&.{
        .version = 0,
        .operation = .activate,
        .request_json = .fromSlice("{}"),
    }, &response, &failure);
    try std.testing.expectEqual(kernel_owner_abi.Status.invalid_abi, status);
    try std.testing.expectEqual(status, failure.status);
    try std.testing.expectEqual(kernel_owner_abi.FailureBoundary.storage_owner, failure.boundary);
    try std.testing.expectEqual(@intFromEnum(kernel_owner_abi.HASeedOperation.activate), failure.operation);
    try std.testing.expectEqualStrings("InvalidAbiVersion", failure.errorName());
    try kernel_error_identity.validateFailureEnvelope(status, &failure, kernel_owner_abi.abi_version);
    try std.testing.expectEqual(@as(u64, 0), response.len);
}

const StorageOwnerDocumentChildRangeDispatch = struct {
    callback_ctx: ?*anyopaque,
    callback_fn: kernel_owner_abi.DocumentChildRangeDispatchFn,

    fn dispatcher(self: *@This()) db_mod.DocumentArtifactChildRangeDispatcher {
        return .{ .ptr = self, .apply = apply };
    }

    fn apply(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        dispatch: db_mod.DocumentArtifactChildRangeDispatch,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const request_json = try local_write.encodeStorageKernelArtifactChildRangeBatchRequest(
            alloc,
            dispatch.doc_key,
            dispatch.artifact_name,
            dispatch.child_batch,
        );
        defer alloc.free(request_json);
        try storageOwnerCallbackStatusToError(self.callback_fn(
            self.callback_ctx,
            dispatch.owner_group_id,
            .fromSlice(request_json),
        ));
    }
};

const StorageOwnerCommittedBatchEffects = struct {
    callback_ctx: ?*anyopaque,
    callback_fn: kernel_owner_abi.CommittedBatchEffectsFn,

    fn observer(self: *@This()) db_mod.CommittedBatchEffectsObserver {
        return .{ .ptr = self, .apply = apply };
    }

    fn apply(ptr: *anyopaque, replay_payload: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try storageOwnerCallbackStatusToError(self.callback_fn(
            self.callback_ctx,
            .fromSlice(replay_payload),
        ));
    }
};

fn storageOwnerCallbackStatusToError(status: kernel_owner_abi.Status) !void {
    return kernel_error_identity.statusToError(status);
}

fn storageOwnerTableName(handle: *const Handle, table_name: kernel_owner_abi.BorrowedBytes) ?[]const u8 {
    const requested = table_name.slice();
    const owned = handle.storage_owner_table_name orelse return null;
    if (!std.mem.eql(u8, requested, owned)) return null;
    return requested;
}

pub fn storageOwnerBatchJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.BatchJsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    if ((request.document_child_range_dispatch_ctx == null) !=
        (request.document_child_range_dispatch_fn == null)) return .invalid_argument;
    if ((request.committed_batch_effects_ctx == null) !=
        (request.committed_batch_effects_fn == null)) return .invalid_argument;
    var dispatch = if (request.document_child_range_dispatch_fn) |callback_fn|
        StorageOwnerDocumentChildRangeDispatch{
            .callback_ctx = request.document_child_range_dispatch_ctx,
            .callback_fn = callback_fn,
        }
    else
        null;
    var committed_effects = if (request.committed_batch_effects_fn) |callback_fn|
        StorageOwnerCommittedBatchEffects{
            .callback_ctx = request.committed_batch_effects_ctx,
            .callback_fn = callback_fn,
        }
    else
        null;
    var response: capi.Buffer = .{};
    const status = batchStorageKernelJson(handle, .{
        .ptr = request.request_json.ptr,
        .len = @intCast(request.request_json.len),
    }, if (dispatch) |*value| value.dispatcher() else null, if (committed_effects) |*value| value.observer() else null, &response);
    if (status != .ok) return status;
    out_response.* = .{
        .ptr = response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

pub fn storageOwnerReplicatedBatchJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerOperationTableName(handle, request) orelse return .invalid_argument;
    var response: capi.Buffer = .{};
    const status = replicatedBatchStorageKernelJson(handle, .{
        .ptr = request.request_json.ptr,
        .len = @intCast(request.request_json.len),
    }, &response);
    if (status != .ok) return status;
    out_response.* = .{
        .ptr = response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

pub fn storageOwnerReplicatedBatchAtRaftEntryJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ReplicatedBatchAtRaftEntryRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    if (request.raft_term == 0 or request.raft_index == 0) return .invalid_argument;
    var response: capi.Buffer = .{};
    const status = replicatedBatchStorageKernelJsonAtRaftEntry(handle, .{
        .ptr = request.request_json.ptr,
        .len = @intCast(request.request_json.len),
    }, .{
        .term = request.raft_term,
        .index = request.raft_index,
    }, &response);
    if (status != .ok) return status;
    out_response.* = .{
        .ptr = response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

pub fn storageOwnerTransactionStatus(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.TransactionStatusRequest,
    out_result: *kernel_owner_abi.TransactionStatusResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const status = handle.db.getTransactionStatus(request.txn_id.bytes) catch |err|
        return storageOwnerStatusFromError(err);
    out_result.status = switch (status) {
        .pending => .pending,
        .committed => .committed,
        .aborted => .aborted,
    };
    return .ok;
}

pub fn storageOwnerWaitForSync(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.SyncRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const sync_level: db_mod.types.SyncLevel = switch (request.sync_level) {
        @intFromEnum(kernel_owner_abi.SyncLevel.propose) => .propose,
        @intFromEnum(kernel_owner_abi.SyncLevel.write) => .write,
        @intFromEnum(kernel_owner_abi.SyncLevel.full_text) => .full_text,
        @intFromEnum(kernel_owner_abi.SyncLevel.enrichments) => .enrichments,
        @intFromEnum(kernel_owner_abi.SyncLevel.full_index) => .full_index,
        else => return .invalid_argument,
    };
    switch (sync_level) {
        .propose, .write => return .ok,
        .full_text, .enrichments, .full_index => {},
    }
    const Adapter = struct {
        fn cancelled(ptr: *const anyopaque) bool {
            const req: *const kernel_owner_abi.SyncRequest = @ptrCast(@alignCast(ptr));
            const callback = req.cancellation_fn orelse return false;
            return callback(req.cancellation_ctx) != 0;
        }
    };
    handle.db.waitForCurrentSyncLevelWithCancellation(sync_level, .{ .ptr = request, .is_cancelled_fn = Adapter.cancelled }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageOwnerApplyHAReplicationRecord(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.HAReplicationRecordRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    handle.db.applyHAReplicationRecord(.{
        .kind = @enumFromInt(request.record_kind),
        .payload_codec = @enumFromInt(request.payload_codec),
        .flags = request.flags,
        .cluster_id = request.cluster_id,
        .shard_id = request.shard_id,
        .table_id = request.table_id,
        .timeline_id = request.timeline_id,
        .epoch = request.epoch,
        .lsn = request.lsn,
        .previous_lsn = request.previous_lsn,
        .commit_timestamp_ns = request.commit_timestamp_ns,
        .payload = request.payload.slice(),
    }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

var backup_pin_diagnostic_gate: antfly.capi_dependencies.api_bounded_diagnostic_gate.Gate = .{};

pub fn storageOwnerOnlineMergeIoJson(owner: ?*anyopaque, request: *const kernel_owner_abi.ControlledJsonOperationRequest, out: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.request_json.len > antfly.capi_dependencies.storage_db_online_merge_io_contract.max_request_bytes) return .invalid_argument;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const wire = antfly.capi_dependencies.storage_db_online_merge_io_contract;
    var parsed = std.json.parseFromSlice(wire.Request, handle.alloc, request.request_json.slice(), .{}) catch |err| return storageOwnerStatusFromError(err);
    defer parsed.deinit();
    parsed.value.validate() catch |err| return storageOwnerStatusFromError(err);
    if (parsed.value.ownerGroup() != handle.storage_owner_group_id) return .invalid_argument;
    const control: backups_api.BackupOperationControl = .{ .deadline_ns = if (request.has_execution_deadline != 0) request.execution_deadline_ns else std.math.maxInt(u64), .cancellation = ownerQueryCancellation(request) };
    control.ensureActive() catch |err| return storageOwnerStatusFromError(err);
    const response = antfly.capi_dependencies.storage_db_online_merge_io.executeJson(&handle.db, handle.alloc, parsed.value, control.token()) catch |err| return storageOwnerStatusFromError(err);
    if (response.len > wire.max_response_bytes) {
        handle.alloc.free(response);
        return .invalid_argument;
    }
    out.* = .{ .ptr = response.ptr, .len = response.len };
    return .ok;
}

pub fn storageOwnerSourceArtifactJson(owner: ?*anyopaque, request: *const kernel_owner_abi.ControlledJsonOperationRequest, out: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.request_json.len > 2 * 1024 * 1024) return .invalid_argument;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const transfer = antfly.capi_dependencies.storage_db_source_artifact_transfer;
    var parsed = std.json.parseFromSlice(transfer.Request, handle.alloc, request.request_json.slice(), .{}) catch |err| return storageOwnerStatusFromError(err);
    defer parsed.deinit();
    const scope = parsed.value.scope();
    scope.validate() catch |err| return storageOwnerStatusFromError(err);
    if (scope.fence.owner_group_id != handle.storage_owner_group_id) return .invalid_argument;
    const control: backups_api.BackupOperationControl = .{ .deadline_ns = if (request.has_execution_deadline != 0) request.execution_deadline_ns else std.math.maxInt(u64), .cancellation = ownerQueryCancellation(request) };
    control.ensureActive() catch |err| return storageOwnerStatusFromError(err);
    const response = transfer.executeJson(&handle.db, handle.alloc, parsed.value, control.token()) catch |err| return storageOwnerStatusFromError(err);
    out.* = .{ .ptr = response.ptr, .len = response.len };
    return .ok;
}

pub fn storageOwnerSourcePinPublicationJson(owner: ?*anyopaque, request: *const kernel_owner_abi.ControlledJsonOperationRequest, out: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.request_json.len > 4096) return .invalid_argument;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    var parsed = std.json.parseFromSlice(antfly.capi_dependencies.storage_db_online_source_contract.Scope, handle.alloc, request.request_json.slice(), .{}) catch |err| return storageOwnerStatusFromError(err);
    defer parsed.deinit();
    parsed.value.validate() catch |err| return storageOwnerStatusFromError(err);
    if (parsed.value.fence.owner_group_id != handle.storage_owner_group_id) return .invalid_argument;
    const control: backups_api.BackupOperationControl = .{ .deadline_ns = if (request.has_execution_deadline != 0) request.execution_deadline_ns else std.math.maxInt(u64), .cancellation = ownerQueryCancellation(request) };
    control.ensureActive() catch |err| return storageOwnerStatusFromError(err);
    const certificate = handle.db.prepareOnlineSourcePublication(parsed.value, control.token()) catch |err| return storageOwnerStatusFromError(err);
    control.ensureActive() catch |err| return storageOwnerStatusFromError(err);
    const response = std.json.Stringify.valueAlloc(handle.alloc, certificate, .{}) catch |err| return storageOwnerStatusFromError(err);
    out.* = .{ .ptr = response.ptr, .len = response.len };
    return .ok;
}

pub fn storageOwnerBackupPinControlJson(owner: ?*anyopaque, request: *const kernel_owner_abi.ControlledJsonOperationRequest, out: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.request_json.len > 16 * 1024) return .invalid_argument;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const seal = antfly.capi_dependencies.storage_db_native_backup_seal_contract;
    var parsed = std.json.parseFromSlice(seal.Request, handle.alloc, request.request_json.slice(), .{}) catch |err| return storageOwnerStatusFromError(err);
    defer parsed.deinit();
    const control: backups_api.BackupOperationControl = .{ .deadline_ns = if (request.has_execution_deadline != 0) request.execution_deadline_ns else std.math.maxInt(u64), .cancellation = ownerQueryCancellation(request) };
    const response = antfly.capi_dependencies.api_table_writes.executeBackupPinControl(handle.alloc, &handle.db, handle.storage_owner_group_id, parsed.value, control) catch |err| {
        if (backup_pin_diagnostic_gate.admit(@import("antfly_platform").time.monotonicNs()))
            std.log.warn("backup pin failed phase=native_capture action={s} group_id={d} class={s}", .{ @tagName(parsed.value), handle.storage_owner_group_id, @errorName(err) });
        return storageOwnerStatusFromError(err);
    };
    out.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

/// Exact-fence tombstoning remains available after the live table disappeared.
/// This operation never opens a writer cache or recreates a database.
pub fn storageBackupPinReclaimJson(context_ptr: ?*anyopaque, request: *const kernel_owner_abi.BackupPinReclaimRequest, out: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out.* = .{};
    if (request.control.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.control.request_json.len > 16 * 1024 or request.replica_root.len == 0 or request.group_id == 0) return .invalid_argument;
    const context = asStorageOwnerContext(context_ptr) orelse return .invalid_argument;
    const alloc = context.alloc;
    const io = context.backend_runtime.ptr().filesystemIo() orelse return storageOwnerStatusFromError(error.BackendRuntimeIoUnavailable);
    const seal = antfly.capi_dependencies.storage_db_native_backup_seal;
    var parsed = std.json.parseFromSlice(seal.Request, alloc, request.control.request_json.slice(), .{}) catch |err| return storageOwnerStatusFromError(err);
    defer parsed.deinit();
    const fence = switch (parsed.value) {
        .seal => return .invalid_argument,
        .release => |proof| proof.fence,
        .cancel => |proof| proof,
    };
    if (fence.owner_group_id != request.group_id or fence.role != .backup_snapshot) return storageOwnerStatusFromError(error.InvalidBackupFence);
    const control: backups_api.BackupOperationControl = .{ .deadline_ns = if (request.control.has_execution_deadline != 0) request.control.execution_deadline_ns else std.math.maxInt(u64), .cancellation = ownerQueryCancellation(&request.control) };
    control.ensureActive() catch |err| return storageOwnerStatusFromError(err);
    const path = backup_restore.groupDbPathFromReplicaRoot(alloc, request.replica_root.slice(), request.group_id) catch |err| return storageOwnerStatusFromError(err);
    defer alloc.free(path);
    seal.reclaim(alloc, io, path, parsed.value, control.token()) catch |err| return storageOwnerStatusFromError(err);
    const response = alloc.dupe(u8, "{}") catch return .out_of_memory;
    out.* = .{ .ptr = response.ptr, .len = response.len };
    return .ok;
}

pub fn storageOwnerBackupJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.BackupRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const format: kernel_owner_abi.BackupFormat = switch (request.format) {
        @intFromEnum(kernel_owner_abi.BackupFormat.native) => .native,
        @intFromEnum(kernel_owner_abi.BackupFormat.portable) => .portable,
        else => return .invalid_argument,
    };
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const path = handle.storage_owner_path orelse return .invalid_argument;
    if (request.backup_root.slice().len == 0 or request.backup_id.slice().len == 0)
        return .invalid_argument;
    if (request.cohort_json.len > 4096 or request.sealed_handle_json.len > 8192) return .invalid_argument;
    var arena: std.heap.ArenaAllocator = .init(handle.alloc);
    defer arena.deinit();
    const cohort = if (request.cohort_json.len != 0) (std.json.parseFromSlice(antfly.capi_dependencies.storage_db_relational_integrity_topology_contract.Fence, arena.allocator(), request.cohort_json.slice(), .{}) catch |err| return storageOwnerStatusFromError(err)).value else null;
    const sealed = if (request.sealed_handle_json.len != 0) (std.json.parseFromSlice(antfly.capi_dependencies.storage_db_native_backup_seal_contract.Handle, arena.allocator(), request.sealed_handle_json.slice(), .{}) catch |err| return storageOwnerStatusFromError(err)).value else null;
    const RequestCancellation = struct {
        fn canceled(ptr: *const anyopaque) bool {
            const value: *const kernel_owner_abi.BackupRequest = @ptrCast(@alignCast(ptr));
            return if (value.cancellation_fn) |callback| callback(value.cancellation_ctx) != 0 else false;
        }
    };
    const control: backups_api.BackupOperationControl = .{ .deadline_ns = if (request.has_execution_deadline != 0) request.execution_deadline_ns else std.math.maxInt(u64), .cancellation = .{ .ptr = request, .is_cancelled_fn = RequestCancellation.canceled } };
    const shards = local_write.backupStorageKernelOwnerDbWithControl(
        handle.alloc,
        &handle.db,
        path,
        handle.storage_owner_group_id,
        request.backup_root.slice(),
        request.backup_id.slice(),
        switch (format) {
            .native => .native,
            .portable => .portable,
        },
        cohort,
        sealed,
        control,
    ) catch |err| return storageOwnerStatusFromError(err);
    defer local_write.freeStorageKernelBackupShards(handle.alloc, shards);
    const response = std.json.Stringify.valueAlloc(handle.alloc, shards, .{
        .emit_null_optional_fields = false,
    }) catch |err| return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

const RestoreRequestScope = struct {
    fn cancelled(ptr: *const anyopaque) bool {
        const request: *const kernel_owner_abi.RestorePrepareRequest = @ptrCast(@alignCast(ptr));
        const callback = request.cancellation_fn orelse return false;
        return callback(request.cancellation_ctx) != 0;
    }

    alloc: Allocator,
    manifest: std.json.Parsed(backups_api.TableBackupManifest),
    local_location: []u8,

    fn init(alloc: Allocator, request: *const kernel_owner_abi.RestorePrepareRequest) !RestoreRequestScope {
        const path = request.path.slice();
        const table_name = request.table_name.slice();
        const backup_root = request.backup_root.slice();
        if (path.len == 0 or table_name.len == 0 or backup_root.len == 0 or
            request.group_id == 0 or request.backup_id.slice().len == 0 or
            request.artifact_backup_id.slice().len == 0 or
            request.source_identity.slice().len == 0 or
            request.snapshot_path.slice().len == 0 or
            request.manifest_json.slice().len == 0)
        {
            return error.InvalidArgument;
        }
        var manifest = try std.json.parseFromSlice(
            backups_api.TableBackupManifest,
            alloc,
            request.manifest_json.slice(),
            .{ .allocate = .alloc_always },
        );
        errdefer manifest.deinit();
        const local_location = try std.fmt.allocPrint(alloc, "file://{s}", .{backup_root});
        return .{
            .alloc = alloc,
            .manifest = manifest,
            .local_location = local_location,
        };
    }

    fn source(
        self: *const RestoreRequestScope,
        request: *const kernel_owner_abi.RestorePrepareRequest,
    ) backup_restore.RestoreSource {
        return .{
            .cancellation = .{ .ptr = request, .is_cancelled_fn = cancelled },
            .backup_id = request.backup_id.slice(),
            .artifact_backup_id = request.artifact_backup_id.slice(),
            .location = self.local_location,
            .identity_location = request.source_identity.slice(),
            .snapshot_path = request.snapshot_path.slice(),
            .authority = .staged_local,
            .expected_artifact_size_bytes = request.expected_artifact_size_bytes,
            .expected_artifact_sha256 = request.expected_artifact_sha256.slice(),
            .expected_native_manifest_size_bytes = request.expected_native_manifest_size_bytes,
            .expected_native_manifest_sha256 = request.expected_native_manifest_sha256.slice(),
            .manifest = &self.manifest.value,
        };
    }

    fn deinit(self: *RestoreRequestScope) void {
        self.alloc.free(self.local_location);
        self.manifest.deinit();
        self.* = undefined;
    }
};

fn prepareStorageRestore(
    request: *const kernel_owner_abi.RestorePrepareRequest,
) !?*StorageSnapshot {
    const alloc = std.heap.c_allocator;
    var scope = try RestoreRequestScope.init(alloc, request);
    defer scope.deinit();
    const restore_source = scope.source(request);
    const path = request.path.slice();
    const identity_namespace: ?db_mod.DocIdentityNamespace = if (request.has_identity_namespace != 0)
        .{
            .table_id = request.identity_table_id,
            .shard_id = request.identity_shard_id,
            .range_id = request.identity_range_id,
        }
    else
        null;

    var preparation = try db_mod.generation_lifecycle.beginProcessPreparationWithRuntime(path, null);
    var preparation_owned = true;
    errdefer if (preparation_owned) preparation.deinit();
    var staged = (try backup_restore.prepareRestoreSnapshotToPathWithPreparation(
        &preparation,
        alloc,
        path,
        request.group_id,
        restore_source,
        .{
            .expected_table_name = request.table_name.slice(),
            .expected_identity_namespace = identity_namespace,
        },
    )) orelse {
        preparation.deinit();
        preparation_owned = false;
        return null;
    };
    var staged_owned = true;
    errdefer if (staged_owned) staged.deinit();

    var backend_runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{
        .backend = .io_threaded,
    });
    defer backend_runtime.deinit();
    var db = try local_write.openStorageKernelRestoreDb(
        alloc,
        staged.path(),
        scope.manifest.value.indexes_json,
        request.lsm_root_generation,
        backend_runtime.ptr(),
        identity_namespace,
        &staged,
    );
    var db_open = true;
    defer if (db_open) db.close();
    try local_write.repairStorageKernelRestoreDb(
        alloc,
        &db,
        request.group_id,
        scope.manifest.value.schema_json,
        scope.manifest.value.indexes_json,
        restore_source.cancellation,
    );
    db.close();
    db_open = false;

    // The first owner proves the generation it has in memory. Reopen the
    // isolated candidate before sealing so publication is authorized by the
    // files a new process actually observes. A fresh owner can also repair
    // reopen-only dense debt (for example an incomplete HBC publication), but
    // its result must itself survive one more reopen.
    var durability_verified = false;
    for (0..3) |verification_attempt| {
        var verify_db = try local_write.openStorageKernelRestoreDb(
            alloc,
            staged.path(),
            scope.manifest.value.indexes_json,
            request.lsm_root_generation,
            backend_runtime.ptr(),
            identity_namespace,
            &staged,
        );
        defer verify_db.close();
        if (!try verify_db.prepareRestoreDurabilityRetryIfNeeded(alloc)) {
            durability_verified = true;
            break;
        }
        std.log.info("storage-kernel restore durability retry group_id={} attempt={}", .{
            request.group_id,
            verification_attempt + 1,
        });
        try local_write.repairStorageKernelRestoreDb(
            alloc,
            &verify_db,
            request.group_id,
            scope.manifest.value.schema_json,
            scope.manifest.value.indexes_json,
            restore_source.cancellation,
        );
    }
    if (!durability_verified) return error.RestoreDenseArtifactRebuildIncomplete;
    try staged.seal();

    const restore_live_path = try alloc.dupe(u8, path);
    errdefer alloc.free(restore_live_path);
    const snapshot = try alloc.create(StorageSnapshot);
    snapshot.* = .{
        .alloc = alloc,
        .preparation = preparation,
        .staged = staged.takeStagedGeneration(),
        .restore_live_path = restore_live_path,
    };
    preparation_owned = false;
    staged_owned = false;
    return snapshot;
}

pub fn storageRestorePrepare(
    request: *const kernel_owner_abi.RestorePrepareRequest,
    out_result: *kernel_owner_abi.RestorePrepareResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const snapshot = prepareStorageRestore(request) catch |err| {
        std.log.err("storage-kernel restore prepare failed group_id={} class={s}", .{
            request.group_id,
            @errorName(err),
        });
        return storageOwnerStatusFromError(err);
    };
    if (snapshot) |value| {
        out_result.* = .{ .state = .prepared, .snapshot = value };
    } else {
        out_result.* = .{ .state = .already_imported };
    }
    return .ok;
}

pub fn storageRestoreReconcile(
    request: *const kernel_owner_abi.RestorePrepareRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const alloc = std.heap.c_allocator;
    var scope = RestoreRequestScope.init(alloc, request) catch |err| return storageOwnerStatusFromError(err);
    defer scope.deinit();
    var transition = db_mod.generation_lifecycle.beginProcessExclusive(request.path.slice()) catch |err|
        return storageOwnerStatusFromError(err);
    defer transition.deinit();
    backup_restore.reconcileCommittedRestoreWithExclusiveTransition(
        &transition,
        alloc,
        request.path.slice(),
        request.group_id,
        scope.source(request),
    ) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageRestoreApplyBootstrap(
    request: *const kernel_owner_abi.RestoreBootstrapRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.replica_root_dir.slice().len == 0 or request.group_id == 0)
        return .invalid_argument;

    const secret_store: ?*common_secrets.FileStore = if (request.secret_store) |ptr|
        @ptrCast(@alignCast(ptr))
    else
        null;
    const node_config: ?*const common_config.Config = if (request.node_config) |ptr|
        @ptrCast(@alignCast(ptr))
    else
        null;
    const restore = raft_catalog.BackupRestoreBootstrapRecord{
        .backup_id = request.backup_id.slice(),
        .artifact_backup_id = request.artifact_backup_id.slice(),
        .location = request.location.slice(),
        .snapshot_path = request.snapshot_path.slice(),
        .connection = request.connection.slice(),
        .artifact_size_bytes = request.artifact_size_bytes,
        .artifact_sha256 = request.artifact_sha256.slice(),
        .native_manifest_size_bytes = request.native_manifest_size_bytes,
        .native_manifest_sha256 = request.native_manifest_sha256.slice(),
    };
    backup_restore.applyBackupRestoreFromRecordWithOptions(
        std.heap.c_allocator,
        request.replica_root_dir.slice(),
        request.group_id,
        restore,
        .{
            .secret_store = secret_store,
            .node_config = node_config,
            .required_capability = request.required_capability.slice(),
        },
    ) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageOwnerRestoreRepair(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.RestorePrepareRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const path = handle.storage_owner_path orelse return .invalid_argument;
    if (!std.mem.eql(u8, path, request.path.slice()) or
        handle.storage_owner_group_id != request.group_id)
    {
        return .invalid_argument;
    }
    var scope = RestoreRequestScope.init(handle.alloc, request) catch |err| return storageOwnerStatusFromError(err);
    defer scope.deinit();
    backup_restore.validateImportedRestoreIdentity(
        handle.alloc,
        path,
        request.group_id,
        scope.source(request),
    ) catch |err| return storageOwnerStatusFromError(err);
    local_write.repairStorageKernelRestoreDb(
        handle.alloc,
        &handle.db,
        request.group_id,
        "",
        "",
        scope.source(request).cancellation,
    ) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

fn prepareStorageSnapshot(request: *const kernel_owner_abi.SnapshotPrepareRequest) !*StorageSnapshot {
    const alloc = std.heap.c_allocator;
    const path = request.path.slice();
    const table_name = request.table_name.slice();
    if (path.len == 0 or table_name.len == 0 or request.group_id == 0) return error.InvalidArgument;

    const state = try shard_state_store.GroupStateSnapshotStream.init(request.encoded_snapshot.slice());
    try shard_state_store.validateGroupStateSnapshotStream(alloc, request.group_id, state);
    if (state.native_primary) |native| return try prepareNativeStorageSnapshot(request, native);

    var preparation = try db_mod.generation_lifecycle.beginProcessPreparationWithRuntime(path, null);
    var preparation_owned = true;
    errdefer if (preparation_owned) preparation.deinit();
    var staged = try preparation.beginStaging();
    var staged_owned = true;
    errdefer if (staged_owned) staged.deinit();

    var db = try db_mod.DB.open(alloc, staged.path(), .{
        .lsm_root_generation = request.lsm_root_generation,
        .staged_generation = &staged,
        .identity_namespace = .{
            .table_id = request.identity_table_id,
            .shard_id = request.identity_shard_id,
            .range_id = request.identity_range_id,
        },
        .prefer_existing_identity_namespace = true,
    });
    var db_open = true;
    defer if (db_open) db.close();
    try local_write.configureStorageKernelOwnerDb(
        alloc,
        &db,
        table_name,
        request.schema_json.slice(),
        request.indexes_json.slice(),
        null,
        null,
        null,
        null,
        null,
    );

    var parsed_schema: ?tables_api.ParsedTableSchema = null;
    if (request.schema_json.len > 0)
        parsed_schema = try tables_api.parseValidatedTableSchema(alloc, request.schema_json.slice());
    defer if (parsed_schema) |*schema| schema.deinit(alloc);

    const max_chunk_entries = 512;
    const max_chunk_payload_bytes = 4 * 1024 * 1024;
    var writes_buffer: [max_chunk_entries]db_mod.types.BatchWrite = undefined;
    var entries = state.entries();
    var exhausted = false;
    while (!exhausted) {
        var chunk_len: usize = 0;
        var chunk_payload_bytes: usize = 0;
        while (chunk_len < writes_buffer.len) {
            var candidate_entries = entries;
            const entry = (try candidate_entries.next()) orelse {
                exhausted = true;
                break;
            };
            const entry_bytes = std.math.add(usize, entry.key.len, entry.value.len) catch return error.SnapshotTooLarge;
            if (chunk_len > 0 and entry_bytes > max_chunk_payload_bytes -| chunk_payload_bytes) break;
            entries = candidate_entries;
            writes_buffer[chunk_len] = .{ .key = entry.key, .value = entry.value };
            chunk_len += 1;
            chunk_payload_bytes = std.math.add(usize, chunk_payload_bytes, entry_bytes) catch return error.SnapshotTooLarge;
        }
        if (chunk_len == 0) break;
        const writes = writes_buffer[0..chunk_len];
        if (parsed_schema) |schema| try tables_api.validateWritesAgainstTableSchema(alloc, schema, writes);
        try db.appendRaftDocumentSnapshotChunk(&staged, state.byte_range, writes);
    }
    try db.finishRaftDocumentSnapshot(&staged, state.byte_range);
    try db.sync(true);
    try db.syncIndexes(true);
    db.close();
    db_open = false;
    try staged.seal();

    const snapshot = try alloc.create(StorageSnapshot);
    snapshot.* = .{
        .alloc = alloc,
        .preparation = preparation,
        .staged = staged,
    };
    preparation_owned = false;
    staged_owned = false;
    return snapshot;
}

fn prepareNativeStorageSnapshot(request: *const kernel_owner_abi.SnapshotPrepareRequest, native: []const u8) !*StorageSnapshot {
    const alloc = std.heap.c_allocator;
    const snapshot_mod = antfly.capi_dependencies.storage_db_native_raft_snapshot;
    const expected = try snapshot_mod.identity(native);
    if (expected.group_id != request.group_id or request.projection_store == null or request.expected_applied_index == 0 or expected.through_index != request.expected_applied_index) return error.InvalidSnapshot;
    var namespace: [24]u8 = undefined;
    antfly.capi_dependencies.storage_db_doc_identity.encodeNamespace(&namespace, .{
        .table_id = request.identity_table_id,
        .shard_id = request.identity_shard_id,
        .range_id = request.identity_range_id,
    });
    if (!std.mem.eql(u8, &namespace, &expected.namespace)) return error.InvalidSnapshot;
    var preparation = try db_mod.generation_lifecycle.beginProcessPreparationWithRuntime(request.path.slice(), null);
    var preparation_owned = true;
    errdefer if (preparation_owned) preparation.deinit();
    var staged = try preparation.beginStaging();
    var staged_owned = true;
    errdefer if (staged_owned) staged.deinit();
    const io = std.Options.debug_io;
    try snapshot_mod.extract(alloc, io, native, staged.path(), expected, .none);
    {
        var primary = try db_mod.DB.open(alloc, staged.path(), .{ .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
        defer primary.close();
        try primary.verifyNativeRaftSnapshot(expected);
    }
    try db_mod.DB.repairNativeRaftSnapshot(alloc, &staged, expected, request.lsm_root_generation);
    // Read-only verification preserves source intents and retention records.
    {
        var db = try db_mod.DB.open(alloc, staged.path(), .{
            .open_mode = .query_readonly,
            .primary_only_readonly = true,
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        defer db.close();
        try db.verifyNativeRaftSnapshot(expected);
        const raw_state = try shard_state_store.GroupStateSnapshotStream.init(request.encoded_snapshot.slice());
        const native_range = db.getRange();
        if (!std.mem.eql(u8, native_range.start, raw_state.byte_range.start) or !std.mem.eql(u8, native_range.end, raw_state.byte_range.end)) return error.InvalidSnapshot;
        const projection = asDataApplyStore(request.projection_store) orelse return error.InvalidArgument;
        try projection.store.installSnapshotWithNativeSource(alloc, request.group_id, expected.through_index, request.encoded_snapshot.slice(), db.core.store);
    }
    // Both authority and derived readiness have been verified before sealing.
    try staged.seal();
    const result = try alloc.create(StorageSnapshot);
    result.* = .{ .alloc = alloc, .preparation = preparation, .staged = staged };
    preparation_owned = false;
    staged_owned = false;
    return result;
}

const NativeSnapshotCapture = struct {
    capture: antfly.capi_dependencies.storage_db_native_raft_snapshot.Capture,
    lease_ctx: ?*anyopaque = null,
    release_lease: ?*const fn (?*anyopaque) callconv(.c) void = null,
};

pub fn storageOwnerSnapshotCapture(owner: ?*anyopaque, group_id: u64, through_index: u64, out_capture: *?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    out_capture.* = null;
    const handle = asHandle(owner) orelse return .invalid_argument;
    if (handle.storage_owner_group_id != group_id) return .invalid_argument;
    const capture = std.heap.c_allocator.create(NativeSnapshotCapture) catch return .out_of_memory;
    const pinned = handle.db.captureNativeRaftSnapshot(group_id, through_index) catch |err| {
        std.heap.c_allocator.destroy(capture);
        return storageOwnerStatusFromError(err);
    };
    capture.* = .{ .capture = pinned };
    out_capture.* = capture;
    return .ok;
}

pub fn storageSnapshotCaptureDestroy(capture_ptr: ?*anyopaque) callconv(.c) void {
    const capture: *NativeSnapshotCapture = @ptrCast(@alignCast(capture_ptr orelse return));
    capture.capture.deinit();
    if (capture.release_lease) |release| release(capture.lease_ctx);
    std.heap.c_allocator.destroy(capture);
}

pub fn dataApplyPreparedSnapshotAttachNative(prepared_ptr: ?*anyopaque, capture_ptr: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const prepared = asDataApplyPreparedSnapshot(prepared_ptr) orelse return .invalid_argument;
    if (prepared.materialized) return .invalid_argument;
    const capture: *NativeSnapshotCapture = @ptrCast(@alignCast(capture_ptr orelse return .invalid_argument));
    const Adapter = struct {
        fn write(ptr: *anyopaque, writer: *std.Io.Writer, cancelled: *const std.atomic.Value(bool)) anyerror!void {
            const value: *NativeSnapshotCapture = @ptrCast(@alignCast(ptr));
            const Cancel = struct {
                fn check(raw: *const anyopaque) bool {
                    const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
                    return flag.load(.acquire);
                }
            };
            try value.capture.write(writer, .{ .ptr = cancelled, .is_cancelled_fn = Cancel.check });
        }
        fn destroy(ptr: *anyopaque) void {
            storageSnapshotCaptureDestroy(ptr);
        }
    };
    prepared.prepared.attachNative(.{
        .ptr = capture,
        .group_id = capture.capture.identity.group_id,
        .applied_index = capture.capture.identity.through_index,
        .size = capture.capture.encodedSize() catch |err| return storageOwnerStatusFromError(err),
        .write = Adapter.write,
        .deinit = Adapter.destroy,
    }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageSnapshotCaptureBindLease(capture_ptr: ?*anyopaque, ctx: ?*anyopaque, release: ?*const fn (?*anyopaque) callconv(.c) void) callconv(.c) kernel_owner_abi.Status {
    const capture: *NativeSnapshotCapture = @ptrCast(@alignCast(capture_ptr orelse return .invalid_argument));
    if (release == null or capture.release_lease != null) return .invalid_argument;
    capture.lease_ctx = ctx;
    capture.release_lease = release;
    return .ok;
}

pub fn dataApplyPreparedSnapshotRequiresNative(prepared_ptr: ?*anyopaque) callconv(.c) bool {
    const prepared = asDataApplyPreparedSnapshot(prepared_ptr) orelse return false;
    return prepared.prepared.requires_native;
}

pub fn storageSnapshotPrepare(
    request: *const kernel_owner_abi.SnapshotPrepareRequest,
    out_snapshot: *?*anyopaque,
) callconv(.c) kernel_owner_abi.Status {
    out_snapshot.* = null;
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const snapshot = prepareStorageSnapshot(request) catch |err| return storageOwnerStatusFromError(err);
    out_snapshot.* = snapshot;
    return .ok;
}

pub fn storageSnapshotPublishPrepared(
    snapshot_handle: ?*anyopaque,
    out_result: *kernel_owner_abi.SnapshotPublishResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    const snapshot: *StorageSnapshot = @ptrCast(@alignCast(snapshot_handle orelse return .invalid_argument));
    if (!snapshot.promoted or snapshot.published or snapshot.finalized) return .invalid_argument;
    const outcome = snapshot.staged.publishPrepared() catch |err| return storageOwnerStatusFromError(err);
    snapshot.published = true;
    out_result.durability_uncertain = @intFromBool(outcome == .durability_uncertain);
    return .ok;
}

pub fn storageSnapshotPromote(snapshot_handle: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const snapshot: *StorageSnapshot = @ptrCast(@alignCast(snapshot_handle orelse return .invalid_argument));
    if (snapshot.promoted or snapshot.published or snapshot.finalized) return .invalid_argument;
    snapshot.transition = snapshot.preparation.promote() catch |err| return storageOwnerStatusFromError(err);
    snapshot.promoted = true;
    return .ok;
}

pub fn storageSnapshotCommit(snapshot_handle: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const snapshot: *StorageSnapshot = @ptrCast(@alignCast(snapshot_handle orelse return .invalid_argument));
    if (!snapshot.published or snapshot.finalized) return .invalid_argument;
    snapshot.staged.commitPublication() catch |err| return storageOwnerStatusFromError(err);
    if (snapshot.restore_live_path) |path| {
        var io_impl = std.Io.Threaded.init(snapshot.alloc, .{});
        defer io_impl.deinit();
        backup_restore.cleanupSnapshotsForPublishedRestore(snapshot.alloc, snapshot.staged.io orelse io_impl.io(), path);
    }
    snapshot.finalized = true;
    return .ok;
}

pub fn storageSnapshotRollback(snapshot_handle: ?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    const snapshot: *StorageSnapshot = @ptrCast(@alignCast(snapshot_handle orelse return .invalid_argument));
    if (!snapshot.published or snapshot.finalized) return .invalid_argument;
    snapshot.staged.rollbackPublication() catch |err| return storageOwnerStatusFromError(err);
    snapshot.finalized = true;
    return .ok;
}

pub fn storageSnapshotDestroy(snapshot_handle: ?*anyopaque) callconv(.c) void {
    const snapshot: *StorageSnapshot = @ptrCast(@alignCast(snapshot_handle orelse return));
    snapshot.deinit();
}

fn batchStorageKernelJson(
    handle: *Handle,
    request_json: capi.Slice,
    document_child_range_dispatcher: ?db_mod.DocumentArtifactChildRangeDispatcher,
    committed_batch_effects_observer: ?db_mod.CommittedBatchEffectsObserver,
    out_buf: *capi.Buffer,
) kernel_owner_abi.Status {
    // The group-local owner accepts the internal batch dialect so split
    // replication state and the caller's requested sync level survive the
    // compiled boundary. Public CAPI parsing remains intentionally narrower.
    var owned = batch_api.parseInternalBatchRequest(handle.alloc, request_json.bytes()) catch |err|
        return storageOwnerStatusFromError(err);
    defer owned.deinit(handle.alloc);
    if (owned.req.relational_index_maintenance) |command| if (command.owner_group_id != handle.storage_owner_group_id) return storageOwnerStatusFromError(error.PreparedGenerationChanged);

    if (committed_batch_effects_observer) |observer|
        handle.db.batchWithDocumentArtifactChildRangeDispatcherAndCommittedEffectsObserver(
            owned.req,
            document_child_range_dispatcher,
            observer,
        ) catch |err| return storageOwnerStatusFromError(err)
    else if (document_child_range_dispatcher) |dispatcher|
        handle.db.batchWithDocumentArtifactChildRangeDispatcher(owned.req, dispatcher) catch |err|
            return storageOwnerStatusFromError(err)
    else
        handle.db.batch(owned.req) catch |err| return storageOwnerStatusFromError(err);
    const response = batch_api.encodeBatchResponse(std.heap.c_allocator, owned.result()) catch |err|
        return storageOwnerStatusFromError(err);
    out_buf.* = .{
        .ptr = response.ptr,
        .len = response.len,
    };
    return .ok;
}

fn replicatedBatchStorageKernelJson(
    handle: *Handle,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) kernel_owner_abi.Status {
    var owned = batch_api.parseInternalBatchRequest(handle.alloc, request_json.bytes()) catch |err|
        return storageOwnerStatusFromError(err);
    defer owned.deinit(handle.alloc);
    if (owned.req.relational_index_maintenance) |command| if (command.owner_group_id != handle.storage_owner_group_id) return storageOwnerStatusFromError(error.PreparedGenerationChanged);

    local_write.applyStorageKernelReplicatedBatch(
        handle.alloc,
        &handle.db,
        handle.storage_owner_table_name orelse return .invalid_argument,
        handle.storage_owner_group_id,
        owned.req,
    ) catch |err| return storageOwnerStatusFromError(err);
    const response = batch_api.encodeBatchResponse(std.heap.c_allocator, owned.result()) catch |err|
        return storageOwnerStatusFromError(err);
    out_buf.* = .{
        .ptr = response.ptr,
        .len = response.len,
    };
    return .ok;
}

fn replicatedBatchStorageKernelJsonAtRaftEntry(
    handle: *Handle,
    request_json: capi.Slice,
    raft_entry: db_mod.RaftAppliedEntryIdentity,
    out_buf: *capi.Buffer,
) kernel_owner_abi.Status {
    var owned = batch_api.parseInternalBatchRequest(handle.alloc, request_json.bytes()) catch |err|
        return storageOwnerStatusFromError(err);
    defer owned.deinit(handle.alloc);
    if (owned.req.relational_index_maintenance) |command| if (command.owner_group_id != handle.storage_owner_group_id) return storageOwnerStatusFromError(error.PreparedGenerationChanged);

    local_write.applyStorageKernelReplicatedBatchAtRaftEntry(
        handle.alloc,
        &handle.db,
        handle.storage_owner_table_name orelse return .invalid_argument,
        handle.storage_owner_group_id,
        owned.req,
        raft_entry,
    ) catch |err| return storageOwnerStatusFromError(err);
    const response = batch_api.encodeBatchResponse(std.heap.c_allocator, owned.result()) catch |err|
        return storageOwnerStatusFromError(err);
    out_buf.* = .{
        .ptr = response.ptr,
        .len = response.len,
    };
    return .ok;
}

pub fn storageOwnerQueryJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.QueryOperationRequest,
    out_response: *kernel_owner_abi.QueryOwnedResponse,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.control.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const table_name = storageOwnerTableName(handle, request.control.table_name) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const status = searchStorageKernelQueryJson(
        handle,
        table_name,
        request,
        out_response,
        out_failure,
    );
    return status;
}

pub fn storageOwnerLookupJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.VersionedOwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerOperationTableName(handle, request) orelse return .invalid_argument;

    var parsed = std.json.parseFromSlice(
        table_reads_api.StorageKernelLookupWireRequest,
        handle.alloc,
        request.request_json.slice(),
        .{},
    ) catch return .invalid_argument;
    defer parsed.deinit();
    const opts = parsed.value.options();
    handle.prepareLookupRequest(parsed.value.key, opts) catch |err| return storageOwnerStatusFromError(err);
    var result = (handle.db.getDocument(handle.alloc, parsed.value.key, opts) catch |err| return storageOwnerStatusFromError(err)) orelse return .not_found;
    defer result.deinit(handle.alloc);
    const version = if (antfly.local_query_contract.integrityLookupMode(opts)) 0 else result.version orelse (handle.db.getTimestamp(handle.alloc, parsed.value.key) catch |err| return storageOwnerStatusFromError(err));
    const response = dupBytes(result.json) catch return .out_of_memory;
    out_response.* = .{
        .buffer = .{
            .ptr = response.ptr,
            .len = @intCast(response.len),
        },
        .version = version,
        .expected_content_digest = result.expected_content_digest orelse @splat(0),
        .has_expected_content_digest = @intFromBool(result.expected_content_digest != null),
    };
    return .ok;
}

pub fn storageOwnerRelationalReadProvider(
    owner: ?*anyopaque,
    contract: *const antfly.capi_dependencies.runtime_native_abi.TypeContract,
    output: *anyopaque,
) callconv(.c) antfly.capi_dependencies.runtime_error_abi.Status {
    const provider = antfly.capi_dependencies.relational_read_provider;
    const errors = antfly.capi_dependencies.runtime_error_abi;
    if (!contract.matches(.of(provider.Provider))) return errors.statusFromError(error.InvalidArgument);
    const handle = asHandle(owner) orelse return errors.statusFromError(error.InvalidArgument);
    const out: *provider.Provider = @ptrCast(@alignCast(output));
    out.* = .{ .ptr = handle, .vtable = &.{ .open = StorageRelationalRead.open, .try_fence = StorageRelationalRead.tryFence } };
    return .ok;
}

const StorageRelationalRead = struct {
    const View = antfly.capi_dependencies.relational_read_provider.View;
    const Fence = antfly.capi_dependencies.statement_read_fence.Fence;

    const Capture = struct {
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        fence: db_mod.DB.StatementReadFence,
        cancellation: @FieldType(db_mod.types.ScanOptions, "cancellation"),
        deadline_ns: ?u64,

        fn validate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.cancellation) |token| try token.check();
            if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        }
        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fence.release();
            self.alloc.destroy(self);
        }
        fn open(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions) !View {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validate(ptr);
            const view = try StorageRelationalRead.openAny(self.db, alloc, from, to, opts);
            errdefer view.deinit();
            try validate(ptr);
            return view;
        }
    };

    fn tryFence(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, opts: db_mod.types.ScanOptions) !?Fence {
        const handle = asHandle(ptr) orelse return error.InvalidArgument;
        _ = storageOwnerTableName(handle, .fromSlice(table)) orelse return error.InvalidArgument;
        if (opts.cancellation) |token| try token.check();
        var fence = (try handle.db.tryStatementReadFence()) orelse return null;
        errdefer fence.release();
        const capture = try alloc.create(Capture);
        errdefer alloc.destroy(capture);
        capture.* = .{ .alloc = alloc, .db = &handle.db, .fence = fence, .cancellation = opts.cancellation, .deadline_ns = opts.execution_deadline_ns };
        try Capture.validate(capture);
        return .{ .ptr = capture, .vtable = &.{ .validate = Capture.validate, .open = Capture.open, .release = Capture.release } };
    }

    fn open(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions) !View {
        const handle = asHandle(ptr) orelse return error.InvalidArgument;
        _ = storageOwnerTableName(handle, .fromSlice(table)) orelse return error.InvalidArgument;
        try handle.prepareScanRequest(from, to, opts);
        return openAny(&handle.db, alloc, from, to, opts);
    }

    fn openAny(db: *db_mod.DB, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions) !View {
        var schema = db.core.acquireSchemaView();
        defer if (schema) |*epoch| epoch.release();
        if (schema == null or schema.?.storageMode() == .document) {
            const session = try db.openDocumentReadSession(alloc, from, to, opts);
            return .{ .ptr = session, .vtable = &.{ .next = nextDocument, .close = closeDocument, .normalize = normalizeDocument, .range_proofs = documentRangeProofs } };
        }
        const session = try db.openRelationalReadSession(alloc, from, to, opts);
        return .{ .ptr = session, .vtable = &.{ .next = next, .close = close, .normalize = normalize, .range_proofs = rangeProofs } };
    }

    fn documentRangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]antfly.capi_dependencies.storage_range_protection.Proof {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        return session.rangeProofs(alloc);
    }

    fn rangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]antfly.capi_dependencies.storage_range_protection.Proof {
        const session: *db_mod.DB.RelationalReadSession = @ptrCast(@alignCast(ptr));
        return session.rangeProofs(alloc);
    }

    fn nextDocument(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !View.Page {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        return session.next(alloc, limit);
    }

    fn closeDocument(ptr: *anyopaque) void {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        session.deinit();
    }

    fn normalizeDocument(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) ![]db_mod.types.BatchWrite {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        return session.normalizeRows(alloc, writes);
    }

    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !View.Page {
        const session: *db_mod.DB.RelationalReadSession = @ptrCast(@alignCast(ptr));
        var page = try session.nextTypedPage(alloc, null, .{ .rows = limit, .output_bytes = 16 * 1024 * 1024 });
        errdefer page.deinit();
        const owned = page.arena.allocator();
        const rows = try owned.alloc(View.Row, page.rows.len);
        for (page.rows, rows) |row, *out| out.* = .{
            .id = row.key,
            .version = row.version,
            .schema_version = session.reader.active.version(),
            .value = row.typed orelse return error.InvalidResponse,
            .sql_nulls = row.sql_nulls,
            .expected_content_digest = row.expected_content_digest,
        };
        const after = if (page.more) try owned.dupe(u8, session.reader.after.items) else null;
        return .{ .arena = page.arena, .rows = rows, .after = after };
    }

    fn close(ptr: *anyopaque) void {
        const session: *db_mod.DB.RelationalReadSession = @ptrCast(@alignCast(ptr));
        session.deinit();
    }

    fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) ![]db_mod.types.BatchWrite {
        const session: *db_mod.DB.RelationalReadSession = @ptrCast(@alignCast(ptr));
        return session.normalizeRows(alloc, writes);
    }
};

pub fn storageOwnerScanStream(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,
    sink: *const kernel_owner_abi.ScanSink,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    _ = storageOwnerTableName(handle, request.table_name) orelse return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);

    var parsed = std.json.parseFromSlice(
        table_reads_api.StorageKernelScanWireRequest,
        handle.alloc,
        request.request_json.slice(),
        .{ .parse_numbers = false },
    ) catch return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    defer parsed.deinit();
    var opts = parsed.value.options();
    opts.execution_deadline_ns = if (request.has_execution_deadline != 0) request.execution_deadline_ns else null;
    opts.cancellation = ownerQueryCancellation(request);
    if (opts.cancellation.?.isCancelled()) return storageOwnerQueryFailure(error.Canceled, .scan_stream, out_failure);
    if (opts.execution_deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return storageOwnerQueryFailure(error.DeadlineExceeded, .scan_stream, out_failure);
    handle.prepareScanRequest(parsed.value.from_key, parsed.value.to_key, opts) catch |err| return storageOwnerQueryFailure(err, .scan_stream, out_failure);
    if (opts.isRelational()) {
        // Readiness, schema and every typed row must validate before HTTP 200.
        // This is a single bounded owner snapshot, not independently paged reads.
        var result = handle.db.scan(handle.alloc, parsed.value.from_key, parsed.value.to_key, opts) catch |err| return storageOwnerQueryFailure(err, .scan_stream, out_failure);
        defer result.deinit(handle.alloc);
        const ndjson = table_reads_api.encodeStorageKernelScanNdjson(handle.alloc, result, opts.include_documents) catch |err| return storageOwnerQueryFailure(err, .encode_internal_response, out_failure);
        defer handle.alloc.free(ndjson);
        if (opts.cancellation.?.isCancelled()) return storageOwnerQueryFailure(error.Canceled, .scan_stream, out_failure);
        if (opts.execution_deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return storageOwnerQueryFailure(error.DeadlineExceeded, .scan_stream, out_failure);
        if (sink.start(sink.context) == 0 or sink.write(sink.context, .fromSlice(ndjson)) == 0) return storageOwnerQueryFailure(error.Canceled, .scan_stream, out_failure);
        return .ok;
    }
    const Visitor = struct {
        alloc: std.mem.Allocator,
        sink: *const kernel_owner_abi.ScanSink,
        line: std.ArrayListUnmanaged(u8) = .empty,
        fn visit(raw: ?*anyopaque, entry: db_mod.types.ScanVisitEntry) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.line.clearRetainingCapacity();
            if (entry.relational_schema_version) |version| {
                try antfly.local_query_contract.appendRelationalScanLine(self.alloc, &self.line, entry, version);
            } else try antfly.local_query_contract.appendScanLine(self.alloc, &self.line, entry.id, entry.document_json, entry.content_hash);
            if (self.sink.write(self.sink.context, .fromSlice(self.line.items)) == 0) return error.Canceled;
        }
    };
    var visitor = Visitor{ .alloc = handle.alloc, .sink = sink };
    defer visitor.line.deinit(handle.alloc);
    if (sink.start(sink.context) == 0) return storageOwnerQueryFailure(error.Canceled, .scan_stream, out_failure);
    handle.db.scanVisit(handle.alloc, parsed.value.from_key, parsed.value.to_key, opts, .{
        .context = &visitor,
        .visit = Visitor.visit,
    }) catch |err| return storageOwnerQueryFailure(err, .scan_stream, out_failure);
    return .ok;
}

pub fn storageOwnerScanNdjson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    _ = storageOwnerTableName(handle, request.table_name) orelse return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);

    var parsed = std.json.parseFromSlice(
        table_reads_api.StorageKernelScanWireRequest,
        handle.alloc,
        request.request_json.slice(),
        .{ .parse_numbers = false },
    ) catch |err| return storageOwnerQueryFailure(err, .validate_request, out_failure);
    defer parsed.deinit();
    var opts = parsed.value.options();
    opts.execution_deadline_ns = if (request.has_execution_deadline != 0) request.execution_deadline_ns else null;
    opts.cancellation = ownerQueryCancellation(request);
    if (opts.cancellation.?.isCancelled()) return storageOwnerQueryFailure(error.Canceled, .scan_stream, out_failure);
    if (opts.execution_deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return storageOwnerQueryFailure(error.DeadlineExceeded, .scan_stream, out_failure);
    handle.prepareScanRequest(parsed.value.from_key, parsed.value.to_key, opts) catch |err| return storageOwnerQueryFailure(err, .scan_stream, out_failure);
    var result = handle.db.scan(handle.alloc, parsed.value.from_key, parsed.value.to_key, opts) catch |err| return storageOwnerQueryFailure(err, .scan_stream, out_failure);
    defer result.deinit(handle.alloc);
    const ndjson = table_reads_api.encodeStorageKernelScanNdjson(handle.alloc, result, opts.include_documents) catch |err| return storageOwnerQueryFailure(err, .encode_internal_response, out_failure);
    out_response.* = .{
        .ptr = ndjson.ptr,
        .len = @intCast(ndjson.len),
    };
    return .ok;
}

pub fn storageOwnerGraphMetricMaintenanceJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    _ = storageOwnerOperationTableName(handle, request) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const response = local_write.runGraphMetricMaintenanceOrActionJsonAlloc(handle.alloc, &handle.db, request.request_json.slice()) catch |err|
        return storageOwnerQueryFailure(err, .graph_metric_maintenance, out_failure);
    defer handle.alloc.free(response);
    const owned = dupBytes(response) catch return storageOwnerQueryFailure(error.OutOfMemory, .encode_internal_response, out_failure);
    out_response.* = .{ .ptr = owned.ptr, .len = @intCast(owned.len) };
    return .ok;
}

pub fn storageOwnerPreflightJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const table_name = storageOwnerOperationTableName(handle, request) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    return executeStorageOwnerCompiledQueryOperation(
        handle,
        table_name,
        request.request_json.slice(),
        .preflight,
        .preflight,
        out_response,
        out_failure,
    );
}

pub fn storageOwnerTextStatsJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const table_name = storageOwnerOperationTableName(handle, request) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    return executeStorageOwnerCompiledQueryOperation(
        handle,
        table_name,
        request.request_json.slice(),
        .text_stats,
        .text_stats,
        out_response,
        out_failure,
    );
}

pub fn storageOwnerAlgebraicPartialsJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const table_name = storageOwnerOperationTableName(handle, request) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    return executeStorageOwnerCompiledQueryOperation(
        handle,
        table_name,
        request.request_json.slice(),
        .algebraic_partials,
        .algebraic_partials,
        out_response,
        out_failure,
    );
}

fn executeStorageOwnerCompiledQueryOperation(
    handle: *Handle,
    table_name: []const u8,
    request_json: []const u8,
    kind: kernel_owner_abi.LocalQueryKind,
    outer_operation: kernel_owner_abi.LocalQueryOperation,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) kernel_owner_abi.Status {
    const response = local_query_client.executeControlledJsonAlloc(
        std.heap.c_allocator,
        kind,
        @ptrCast(&handle.db),
        table_name,
        request_json,
        null,
        null,
        null,
        out_failure,
    ) catch |err| {
        if (out_failure.status != .ok) return out_failure.status;
        return storageOwnerQueryFailure(err, outer_operation, out_failure);
    };
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

pub fn storageAggregateJson(
    request: *const kernel_owner_abi.AggregationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .parse_aggregation, out_failure);

    var arena_impl = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var wire = std.json.parseFromSliceLeaky(
        aggregations_contract.ComputeContextWire,
        arena,
        request.context_json.slice(),
        .{},
    ) catch |err| return storageOwnerQueryFailure(err, .parse_aggregation, out_failure);

    const requests = query_api.parseAggregationRequestsJson(
        std.heap.c_allocator,
        request.aggregations_json.slice(),
    ) catch |err| return storageOwnerQueryFailure(err, .parse_aggregation, out_failure);
    defer query_api.freeAggregationRequests(std.heap.c_allocator, requests);

    if (request.hit_count > std.math.maxInt(usize))
        return storageOwnerQueryFailure(error.InvalidArgument, .parse_aggregation, out_failure);
    const hit_count: usize = @intCast(request.hit_count);
    const borrowed_hits = if (hit_count == 0)
        @as([]const kernel_owner_abi.AggregationHit, &.{})
    else if (request.hits) |ptr|
        ptr[0..hit_count]
    else
        return storageOwnerQueryFailure(error.InvalidArgument, .parse_aggregation, out_failure);
    const hits = arena.alloc(db_mod.types.SearchHit, hit_count) catch |err|
        return storageOwnerQueryFailure(err, .execute_aggregation, out_failure);
    for (borrowed_hits, 0..) |hit, i| hits[i] = .{
        .id = @constCast(""),
        .stored_data = if (hit.stored_data.len == 0) null else @constCast(hit.stored_data.slice()),
    };
    const result: db_mod.types.SearchResult = .{
        .alloc = arena,
        .hits = hits,
        .total_hits = request.total_hits,
    };
    const aggregation_results = aggregations_mod.computeSearchAggregations(
        std.heap.c_allocator,
        requests,
        result,
        .{
            .text_analysis = if (wire.text_analysis) |*value| value else null,
            .distributed_text_stats = wire.distributed_text_stats,
            .distributed_background_text_stats = wire.distributed_background_text_stats,
        },
    ) catch |err| return storageOwnerQueryFailure(err, .execute_aggregation, out_failure);
    defer aggregations_mod.deinitResults(std.heap.c_allocator, aggregation_results);

    const response = std.json.Stringify.valueAlloc(
        std.heap.c_allocator,
        aggregation_results,
        .{},
    ) catch |err| return storageOwnerQueryFailure(err, .encode_aggregation, out_failure);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

pub fn storageOwnerGraphExpandJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const table_name = storageOwnerTableName(handle, request.table_name) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    return executeStorageOwnerCompiledGraph(
        handle,
        table_name,
        request,
        .graph_expand,
        .encode_graph_expand,
        out_response,
        out_failure,
    );
}

pub fn storageOwnerGraphHydrateJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const table_name = storageOwnerTableName(handle, request.table_name) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    return executeStorageOwnerCompiledGraph(
        handle,
        table_name,
        request,
        .graph_hydrate,
        .encode_graph_hydrate,
        out_response,
        out_failure,
    );
}

pub fn storageOwnerGraphEdgesJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != kernel_owner_abi.abi_version)
        return storageOwnerQueryFailure(error.InvalidAbiVersion, .validate_request, out_failure);
    const handle = asHandle(owner) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    const table_name = storageOwnerTableName(handle, request.table_name) orelse
        return storageOwnerQueryFailure(error.InvalidArgument, .validate_request, out_failure);
    return executeStorageOwnerCompiledGraph(
        handle,
        table_name,
        request,
        .graph_edges,
        .encode_graph_edges,
        out_response,
        out_failure,
    );
}

fn executeStorageOwnerCompiledGraph(
    handle: *Handle,
    table_name: []const u8,
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,
    kind: kernel_owner_abi.LocalQueryKind,
    outer_operation: kernel_owner_abi.LocalQueryOperation,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) kernel_owner_abi.Status {
    const response = local_query_client.executeControlledJsonAlloc(
        std.heap.c_allocator,
        kind,
        @ptrCast(&handle.db),
        table_name,
        request.request_json.slice(),
        if (request.has_execution_deadline != 0) request.execution_deadline_ns else null,
        request.cancellation_ctx,
        request.cancellation_fn,
        out_failure,
    ) catch |err| {
        // A valid nested provider failure retains its local-query origin and
        // operation. Only consumer-side allocation/protocol work receives a
        // new storage-owner identity.
        if (out_failure.status != .ok) return out_failure.status;
        return storageOwnerQueryFailure(err, outer_operation, out_failure);
    };
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

pub fn storageOwnerDocumentArtifactManifestJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerOperationTableName(handle, request) orelse return .invalid_argument;
    var parsed = std.json.parseFromSlice(
        table_reads_api.StorageKernelDocumentArtifactManifestWireRequest,
        handle.alloc,
        request.request_json.slice(),
        .{},
    ) catch return .invalid_argument;
    defer parsed.deinit();
    var manifest = (handle.db.getDocumentArtifactManifest(
        handle.alloc,
        parsed.value.doc_key,
        parsed.value.artifact_name,
    ) catch |err| return storageOwnerStatusFromError(err)) orelse return .not_found;
    defer manifest.deinit(handle.alloc);
    const response = table_reads_api.encodeStorageKernelDocumentArtifactManifestResponse(
        handle.alloc,
        manifest,
    ) catch |err| return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

pub fn storageOwnerDocumentArtifactManifestsJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerOperationTableName(handle, request) orelse return .invalid_argument;
    var parsed = std.json.parseFromSlice(
        table_reads_api.StorageKernelDocumentArtifactManifestsWireRequest,
        handle.alloc,
        request.request_json.slice(),
        .{},
    ) catch return .invalid_argument;
    defer parsed.deinit();
    var manifests = handle.db.listDocumentArtifactManifests(
        handle.alloc,
        parsed.value.doc_key,
    ) catch |err| return storageOwnerStatusFromError(err);
    defer manifests.deinit(handle.alloc);
    const response = table_reads_api.encodeStorageKernelDocumentArtifactManifestsResponse(
        handle.alloc,
        manifests,
    ) catch |err| return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

const StorageOwnerArtifactCancellation = struct {
    request: *const kernel_owner_abi.ArtifactOperationRequest,

    fn requested(ptr: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.request.cancellation_fn orelse return false;
        return callback(self.request.cancellation_ctx) != 0;
    }
};

fn storageOwnerArtifactJsonResponse(
    alloc: std.mem.Allocator,
    out_response: *kernel_owner_abi.OwnedBytes,
    value: anytype,
) kernel_owner_abi.Status {
    const response = std.json.Stringify.valueAlloc(alloc, value, .{
        .emit_null_optional_fields = false,
    }) catch |err| return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

pub fn storageOwnerVectorMigrationJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    var parsed = std.json.parseFromSlice(antfly.vector_migration.Command, handle.alloc, request.request_json.slice(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    // Offline publication owns a separate exclusive root transition; it may
    // never run against a serving compiled owner through this online endpoint.
    if (parsed.value.request.mode != .online) return .invalid_argument;
    const result = handle.db.vectorMigrationCommand(handle.alloc, parsed.value) catch |err| return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = result.ptr, .len = @intCast(result.len) };
    return .ok;
}

pub fn storageOwnerArtifactOperationJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ArtifactOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const operation: kernel_owner_abi.ArtifactOperation = switch (request.operation) {
        0...@intFromEnum(kernel_owner_abi.ArtifactOperation.apply_child_range_batch) => @enumFromInt(request.operation),
        else => return .invalid_argument,
    };
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;

    switch (operation) {
        .corrupt_embedding => {
            var parsed = std.json.parseFromSlice(
                local_write.StorageKernelEmbeddingCorruptionRequest,
                handle.alloc,
                request.request_json.slice(),
                .{},
            ) catch return .invalid_argument;
            defer parsed.deinit();
            const handled = local_write.corruptEmbeddingArtifactInDb(
                handle.alloc,
                &handle.db,
                parsed.value.doc_key,
                parsed.value.index_name,
            ) catch |err| return storageOwnerStatusFromError(err);
            if (!handled) return .not_found;
            return storageOwnerArtifactJsonResponse(handle.alloc, out_response, .{ .handled = true });
        },
        .reprocess_document => {
            var parsed = std.json.parseFromSlice(
                local_write.StorageKernelArtifactDocumentRequest,
                handle.alloc,
                request.request_json.slice(),
                .{},
            ) catch return .invalid_argument;
            defer parsed.deinit();
            const handled = handle.db.reprocessDocumentArtifact(
                handle.alloc,
                parsed.value.doc_key,
                parsed.value.artifact_name,
            ) catch |err| return storageOwnerStatusFromError(err);
            return storageOwnerArtifactJsonResponse(handle.alloc, out_response, .{ .handled = handled });
        },
        .reprocess_document_range => {
            var parsed = std.json.parseFromSlice(
                local_write.StorageKernelArtifactRangeRequest,
                handle.alloc,
                request.request_json.slice(),
                .{},
            ) catch return .invalid_argument;
            defer parsed.deinit();
            var result = handle.db.reprocessDocumentArtifactRange(
                handle.alloc,
                parsed.value.artifact_name,
                parsed.value.request,
            ) catch |err| return storageOwnerStatusFromError(err);
            defer result.deinit(handle.alloc);
            return storageOwnerArtifactJsonResponse(handle.alloc, out_response, result);
        },
        .list_repair_issues => {
            var parsed = std.json.parseFromSlice(
                db_mod.types.ArtifactRepairListRequest,
                handle.alloc,
                request.request_json.slice(),
                .{},
            ) catch return .invalid_argument;
            defer parsed.deinit();
            var result = handle.db.listArtifactRepairIssuesPage(
                handle.alloc,
                parsed.value,
            ) catch |err| return storageOwnerStatusFromError(err);
            defer result.deinit(handle.alloc);
            return storageOwnerArtifactJsonResponse(handle.alloc, out_response, result);
        },
        .repair_issues => {
            var parsed = std.json.parseFromSlice(
                db_mod.types.ArtifactRepairRunRequest,
                handle.alloc,
                request.request_json.slice(),
                .{},
            ) catch return .invalid_argument;
            defer parsed.deinit();
            var cancellation = StorageOwnerArtifactCancellation{ .request = request };
            var result = handle.db.repairArtifactIssuesWithRequestOptions(
                handle.alloc,
                parsed.value,
                .{
                    .cancel_check = if (request.cancellation_fn != null) .{
                        .ptr = &cancellation,
                        .is_requested = StorageOwnerArtifactCancellation.requested,
                    } else null,
                    .defer_durable_index_repair_execution = request.defer_durable_index_repair_execution != 0,
                },
            ) catch |err| return storageOwnerStatusFromError(err);
            defer result.deinit(handle.alloc);
            return storageOwnerArtifactJsonResponse(handle.alloc, out_response, result);
        },
        .update_child_range_placement => {
            var parsed = std.json.parseFromSlice(
                local_write.StorageKernelArtifactPlacementRequest,
                handle.alloc,
                request.request_json.slice(),
                .{},
            ) catch return .invalid_argument;
            defer parsed.deinit();
            const handled = handle.db.updateDocumentArtifactChildRangePlacement(
                handle.alloc,
                parsed.value.doc_key,
                parsed.value.artifact_name,
                parsed.value.update,
            ) catch |err| return storageOwnerStatusFromError(err);
            return storageOwnerArtifactJsonResponse(handle.alloc, out_response, .{ .handled = handled });
        },
        .apply_child_range_batch => {
            var parsed = std.json.parseFromSlice(
                local_write.StorageKernelArtifactChildRangeBatchRequest,
                handle.alloc,
                request.request_json.slice(),
                .{},
            ) catch return .invalid_argument;
            defer parsed.deinit();
            const sequence = handle.db.applyDocumentArtifactChildRangeBatch(parsed.value.batch) catch |err|
                return storageOwnerStatusFromError(err);
            return storageOwnerArtifactJsonResponse(handle.alloc, out_response, .{ .sequence = sequence });
        },
    }
}

pub fn storageOwnerRuntimeStatusJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerOperationTableName(handle, request) orelse return .invalid_argument;

    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = handle.storage_owner_group_id,
        .source_vectors = handle.db.sourceVectorStats() catch |err| return storageOwnerStatusFromError(err),
        .created_at_millis = (handle.db.getGroupCreatedAtMillis(
            handle.alloc,
            handle.storage_owner_group_id,
        ) catch null) orelse 0,
        // Runtime status is a periodic observation, not a foreground
        // consistency barrier. Never retain the shared owner lease while
        // waiting for an apply writer: structural reconciliation may need its
        // exclusive lease to advance the exact work holding that writer.
        .stats = (handle.db.runtimeStatusStatsConsistentIfAvailable(handle.alloc) catch |err|
            return storageOwnerStatusFromError(err)) orelse return .busy,
        .lsm_storage_stats = .{
            .maintenance = handle.db.snapshotLsmMaintenanceStats(),
            .write = handle.db.snapshotLsmWriteStats(),
            .maintenance_score = handle.db.lsmMaintenanceScore(),
            .maintenance_debt_hint = handle.db.lsmMaintenanceDebtHint(),
        },
    };
    defer status.deinit(handle.alloc);
    status.replaceMetadata(.{
        .updated_at_ns = antfly.platform_time.monotonicNs(),
        .source = .live_writer_publish,
        .freshness = .fresh,
        .lsm_root_generation = handle.storage_owner_root_generation,
        .target_observation_revision = runtime_status.observedSourceTargetSequence(status.stats),
        .target_observation_complete = true,
    });
    const response = std.json.Stringify.valueAlloc(handle.alloc, status, .{
        .emit_null_optional_fields = false,
    }) catch |err| return storageOwnerStatusFromError(err);
    out_response.* = .{
        .ptr = response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

test "storage owner runtime status does not wait behind apply writer" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("storage-owner-runtime-status-busy");
    defer test_tmp.cleanup();
    const path = try tempTestPath(alloc, test_tmp.path(), "db");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
        .storage_owner_table_name = @constCast("docs"),
        .storage_owner_group_id = 7,
    };
    defer handle.db.close();
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);

    handle.db.core.lockApplyExclusive();
    defer handle.db.core.unlockApplyExclusive();
    var response: kernel_owner_abi.OwnedBytes = .{};
    try std.testing.expectEqual(
        kernel_owner_abi.Status.busy,
        storageOwnerRuntimeStatusJson(
            handle_id,
            &.{ .table_name = .fromSlice("docs") },
            &response,
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
}

test "storage owner runtime status distinguishes absent and busy source vectors" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("storage-owner-source-status-busy");
    defer test_tmp.cleanup();
    for ([_]bool{ false, true }) |with_source| {
        const path = try tempTestPath(alloc, test_tmp.path(), if (with_source) "with-source" else "without-source");
        defer alloc.free(path);
        defer cleanupTestDir(path);
        var handle = Handle{
            .alloc = alloc,
            .db = try db_mod.DB.open(alloc, path, .{
                .table_storage = .{ .dense_embeddings = if (with_source) .vector_store else .primary_lsm },
                .start_index_workers = false,
                .start_optional_runtimes = false,
                .ttl_cleanup = .{ .enabled = false },
            }),
            .storage_owner_table_name = @constCast("docs"),
            .storage_owner_group_id = 7,
        };
        defer handle.db.close();
        const handle_id = try registerTestHandle(&handle);
        defer unregisterTestHandle(handle_id);
        handle.db.backend_runtime.durable_jobs.drainOwner(handle.db.repair_cleanup_owner_id);
        var response: kernel_owner_abi.OwnedBytes = .{};
        if (handle.db.source_vectors.load(.acquire)) |source| {
            // Holding the mutex on this thread makes both the missing-field
            // bug and any blocking-lock replacement deterministic.
            while (!source.mutex.tryLock()) antfly.platform_time.yieldBriefly();
            defer source.mutex.unlock();
            try std.testing.expectEqual(kernel_owner_abi.Status.busy, storageOwnerRuntimeStatusJson(
                handle_id,
                &.{ .table_name = .fromSlice("docs") },
                &response,
            ));
            try std.testing.expectEqual(@as(u64, 0), response.len);
            try std.testing.expect(response.ptr == null);
        } else try std.testing.expect(!with_source);
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageOwnerRuntimeStatusJson(
            handle_id,
            &.{ .table_name = .fromSlice("docs") },
            &response,
        ));
        defer alloc.free(response.ptr.?[0..@intCast(response.len)]);
        const Status = struct { source_vectors: ?struct { retained_payloads: u64 } = null };
        var parsed = try std.json.parseFromSlice(Status, alloc, response.ptr.?[0..@intCast(response.len)], .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqual(with_source, parsed.value.source_vectors != null);
    }
}

const StorageOwnerObservationCancellation = struct {
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,

    fn requested(ptr: *const anyopaque) bool {
        const self: *const StorageOwnerObservationCancellation = @ptrCast(@alignCast(ptr));
        const callback = self.request.cancellation_fn orelse return false;
        return callback(self.request.cancellation_ctx) != 0;
    }
};

pub fn storageOwnerObservedDynamicFieldCapabilitySetsJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;

    var parsed = std.json.parseFromSlice(
        table_reads_api.StorageKernelDynamicFieldObservationWireRequest,
        handle.alloc,
        request.request_json.slice(),
        .{},
    ) catch return .invalid_argument;
    defer parsed.deinit();
    var cancellation = StorageOwnerObservationCancellation{ .request = request };
    const observation: table_reads_api.DynamicFieldObservationQuery = .{
        .index_name = parsed.value.index_name,
        .fields = parsed.value.fields,
        .coverage_read_mode = parsed.value.coverage_read_mode,
        .execution_deadline_ns = if (request.has_execution_deadline != 0)
            request.execution_deadline_ns
        else
            null,
        .cancellation = if (request.cancellation_fn != null) .{
            .ptr = &cancellation,
            .is_cancelled_fn = StorageOwnerObservationCancellation.requested,
        } else null,
    };
    const sets = handle.db.observedDynamicFieldCapabilitySetsAlloc(handle.alloc, observation) catch |err|
        return storageOwnerStatusFromError(err);
    defer table_reads_api.freeObservedDynamicFieldCapabilitySets(handle.alloc, sets);
    const response = std.json.Stringify.valueAlloc(handle.alloc, sets, .{}) catch |err|
        return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

pub fn storageOwnerRestoreStateJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.JsonOperationRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerOperationTableName(handle, request) orelse return .invalid_argument;
    const path = handle.storage_owner_path orelse return .invalid_argument;
    var state = (db_mod.DB.readRestoreStateForPath(handle.alloc, path) catch |err|
        return storageOwnerStatusFromError(err)) orelse return .not_found;
    defer state.deinit(handle.alloc);
    const wire = antfly.restore_state_contract.State{
        .backup_id = state.backup_id,
        .location = state.location,
        .artifact_sha256 = state.artifact_sha256,
        .native_manifest_size_bytes = state.native_manifest_size_bytes,
        .native_manifest_sha256 = state.native_manifest_sha256,
        .snapshot_path = state.snapshot_path,
        .group_id = state.group_id,
        .phase = state.phase,
        .primary_restored = state.primary_restored,
        .runtime_repair_complete = state.runtime_repair_complete,
        .last_error = state.last_error,
    };
    const response = std.json.Stringify.valueAlloc(handle.alloc, wire, .{}) catch |err|
        return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = response.len };
    return .ok;
}

pub fn storageOwnerTextMemoryJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.TableRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const stats = handle.db.trySnapshotTextMemoryAttributionStats() orelse return .busy;
    const response = std.json.Stringify.valueAlloc(handle.alloc, stats, .{}) catch |err|
        return storageOwnerStatusFromError(err);
    out_response.* = .{
        .ptr = response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

pub fn storageOwnerMaintenance(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.MaintenanceRequest,
    out_result: *kernel_owner_abi.MaintenanceResult,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const action = std.enums.fromInt(kernel_owner_abi.MaintenanceAction, request.action) orelse
        return .invalid_argument;

    switch (action) {
        .inspect, .inspect_best_effort => {},
        .lsm_step => {
            out_result.progressed = @intFromBool(handle.db.runLsmMaintenanceStep() catch |err|
                return storageOwnerStatusFromError(err));
        },
        .lsm_step_best_effort => {
            out_result.progressed = @intFromBool(handle.db.runLsmMaintenanceStepBestEffort() catch |err|
                return storageOwnerStatusFromError(err));
        },
        .dense_posting_idle => {
            if (handle.db.hasActiveDenseBulkWork()) {
                out_result.deferred = 1;
                return .ok;
            }
            const started = antfly.platform_time.monotonicNs();
            var pass: usize = 0;
            out_result.deferred = 1;
            while (pass < 64 and antfly.platform_time.monotonicNs() -| started < 50 * std.time.ns_per_ms) : (pass += 1) {
                const page = handle.db.refreshDensePostingPayloadPageBestEffort() catch |err|
                    return storageOwnerStatusFromError(err);
                out_result.dense_steps += page.repaired;
                out_result.dense_scanned += page.scanned;
                out_result.deferred = @intFromBool(page.pending);
                if (!page.pending or page.scanned == 0 or page.yield_after_page) break;
            }
            out_result.progressed = @intFromBool(out_result.dense_steps != 0 or out_result.dense_scanned != 0);
        },
        .publish_dense_checkpoints => {
            const result = handle.db.publishCompletedDensePostingCheckpoints() catch |err|
                return storageOwnerStatusFromError(err);
            out_result.published = result.published;
            out_result.busy = @intFromBool(result.busy);
            out_result.deferred = @intFromBool(result.deferred);
            out_result.progressed = @intFromBool(result.published != 0);
        },
        .vector_block_idle => {
            if (handle.db.hasActiveDenseBulkWork()) return .ok;
            if (handle.db.finalizeDenseProjectionLifecycleForIdle() catch |err| return storageOwnerStatusFromError(err)) {
                out_result.dense_steps = 1;
            } else {
                const published = handle.db.runVectorBlockMaintenanceForIdle() catch |err| return storageOwnerStatusFromError(err);
                out_result.dense_steps = published;
                if (published != 0 and (handle.db.finalizeDenseProjectionLifecycleForIdle() catch |err| return storageOwnerStatusFromError(err)))
                    out_result.dense_steps += 1;
            }
            out_result.progressed = @intFromBool(out_result.dense_steps != 0);
        },
        .capture_ha_seed_snapshot => {
            const token = request.snapshot_token.slice();
            const destination = request.destination_root.slice();
            if (!antfly.ha_validation.isIdentifier(token) or !std.fs.path.isAbsolute(destination)) return .invalid_argument;
            antfly.ha_seed_snapshot.capture(handle.alloc, &handle.db, handle.db.core.path, token, destination) catch |err| {
                std.log.warn("storage owner HA seed capture failed err={s}", .{@errorName(err)});
                return storageOwnerStatusFromError(err);
            };
        },
        .prepare_ha_seed_snapshot => {
            if (request.deadline_ns == 0) return .invalid_argument;
            handle.db.prepareHASeedSnapshot(request.deadline_ns) catch |err| {
                std.log.warn("storage owner HA seed preparation failed err={s}", .{@errorName(err)});
                return storageOwnerStatusFromError(err);
            };
        },
    }

    out_result.maintenance_score = switch (action) {
        .inspect, .lsm_step, .prepare_ha_seed_snapshot, .capture_ha_seed_snapshot => @max(
            handle.db.lsmMaintenanceScore(),
            handle.db.lsmMaintenanceDebtHint(),
        ),
        .inspect_best_effort, .lsm_step_best_effort, .dense_posting_idle, .publish_dense_checkpoints, .vector_block_idle => handle.db.lsmMaintenanceDebtHint(),
    };
    if (handle.db.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay_ns| {
        out_result.has_next_wake_delay = 1;
        out_result.next_wake_delay_ns = delay_ns;
    }
    return .ok;
}

pub fn storageOwnerBufferDestroy(buffer: *kernel_owner_abi.OwnedBytes) callconv(.c) void {
    freeRawBuffer(buffer.ptr, @intCast(buffer.len));
    buffer.* = .{};
}

fn storageOwnerStatusFromError(err: anyerror) kernel_owner_abi.Status {
    const status = kernel_error_identity.statusFromError(err);
    if (status == .internal) {
        // This status-only boundary cannot carry undeclared error names.
        // Preserve the originating diagnostic before consumers see the
        // intentionally generic StorageKernelFailure control-flow status.
        std.log.warn("storage owner returned undeclared error err={s}", .{@errorName(err)});
    }
    return status;
}

fn openDefaultDirectoryHandle(path: []const u8) !*Handle {
    const alloc = std.heap.c_allocator;
    var db = try db_mod.DB.open(alloc, path, .{});
    errdefer db.close();
    const handle = alloc.create(Handle) catch return error.OutOfMemory;
    errdefer alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .db = db,
    };
    handle.db.startQuarantineRetryWorkerIfNeeded();
    return handle;
}

pub export fn antfly_db_close(handle_ptr: ?*anyopaque) void {
    // Stale ids and concurrent or repeated closes are no-ops.
    closeHandleId(handle_ptr);
}

/// Threading contract of this library, like sqlite3_threadsafe(). Always
/// ANTFLY_THREADING_SERIALIZED: every handle may be used from any thread,
/// concurrently. See zig/CAPI.md "Thread Safety".
pub export fn antfly_threading_mode() u32 {
    return capi.threading_serialized;
}

pub export fn antfly_abi_version() u32 {
    return abi_version;
}

pub export fn antfly_open_options_size() u32 {
    return @intCast(@sizeOf(capi.OpenOptions));
}

pub export fn antfly_error_code_name(code: c_int) [*:0]const u8 {
    return capi.errorCodeName(code);
}

pub export fn antfly_error_code_description(code: c_int) [*:0]const u8 {
    return capi.errorCodeDescription(code);
}

pub export fn antfly_open_options_init(options: ?*capi.OpenOptions) capi.ErrorCode {
    const opts = options orelse return .invalid_argument;
    opts.* = .{};
    return .ok;
}

const open_known_flags = capi.open_flag_no_sync |
    capi.open_flag_ttl_cleanup |
    capi.open_flag_remote_provider_configured |
    capi.open_flag_local_runtime_configured |
    capi.open_flag_generated_enrichment_replay;

const StorageKind = enum {
    directory,
    lite,
};

const LiteResolvedOpenOptions = struct {
    storage_kind: StorageKind = .lite,
    open_mode: db_mod.OpenOptions.OpenMode = .writer,
    profile: lite_backend.Profile = .native,
    map_size: ?usize = null,
    no_sync: bool = false,
    ttl_cleanup: ?db_mod.ttl_runtime.Config = null,
    inference: lite_backend.InferenceOpenOptions = .{},
    generated_enrichment_replay: bool = false,
    busy_timeout_ms: u64 = 0,
};

fn optionFieldType(comptime Options: type, comptime field_name: []const u8) type {
    return @TypeOf(@field(@as(Options, .{}), field_name));
}

fn optionHasField(comptime Options: type, comptime field_name: []const u8) bool {
    inline for (std.meta.fields(Options)) |field| {
        if (std.mem.eql(u8, field.name, field_name)) return true;
    }
    return false;
}

fn optionFieldPresent(comptime Options: type, abi_size: u32, comptime field_name: []const u8) bool {
    const Field = optionFieldType(Options, field_name);
    const offset = @offsetOf(Options, field_name);
    return abi_size >= offset + @sizeOf(Field);
}

fn readOptionField(
    comptime Options: type,
    options: *const Options,
    abi_size: u32,
    comptime field_name: []const u8,
) ?optionFieldType(Options, field_name) {
    const Field = optionFieldType(Options, field_name);
    const offset = @offsetOf(Options, field_name);
    if (abi_size < offset + @sizeOf(Field)) return null;
    const raw: [*]const u8 = @ptrCast(options);
    return std.mem.bytesAsValue(Field, raw[offset..][0..@sizeOf(Field)]).*;
}

fn validateOpenOptionsReserved(comptime Options: type, options: *const Options, abi_size: u32) !void {
    if (comptime optionHasField(Options, "reserved0")) {
        if (optionFieldPresent(Options, abi_size, "reserved0")) {
            if (readOptionField(Options, options, abi_size, "reserved0").? != 0) return error.InvalidArgument;
        }
    }
    if (comptime !optionHasField(Options, "reserved")) {
        return;
    }
    const reserved_offset = @offsetOf(Options, "reserved");
    if (abi_size <= reserved_offset) return;
    const available = @min(@as(usize, abi_size) - reserved_offset, @sizeOf(optionFieldType(Options, "reserved")));
    if (available % @sizeOf(u64) != 0) return error.InvalidArgument;
    const raw: [*]const u8 = @ptrCast(options);
    var offset: usize = reserved_offset;
    var remaining = available;
    while (remaining >= @sizeOf(u64)) : ({
        offset += @sizeOf(u64);
        remaining -= @sizeOf(u64);
    }) {
        const word = std.mem.bytesAsValue(u64, raw[offset..][0..@sizeOf(u64)]).*;
        if (word != 0) return error.InvalidArgument;
    }
}

fn openModeFromU32(value: u32) !db_mod.OpenOptions.OpenMode {
    return switch (value) {
        0 => .writer,
        1 => .query_readonly,
        2 => .status_only,
        else => return error.InvalidArgument,
    };
}

fn profileFromU32(value: u32) !lite_backend.Profile {
    return switch (value) {
        0 => .native,
        1 => .hosted,
        else => return error.InvalidArgument,
    };
}

fn validateResolvedOpenOptions(resolved: LiteResolvedOpenOptions) !void {
    if (resolved.profile == .hosted and resolved.ttl_cleanup != null) {
        return error.InvalidArgument;
    }
    if (resolved.profile == .hosted and resolved.generated_enrichment_replay) {
        return error.InvalidArgument;
    }
}

fn resolveOpenOptions(options_ptr: ?*const capi.OpenOptions) !LiteResolvedOpenOptions {
    const options = options_ptr orelse return .{ .storage_kind = .directory };
    const abi_size = options.abi_size;
    if (abi_size < @offsetOf(capi.OpenOptions, "storage_kind")) return error.InvalidArgument;
    const flags = readOptionField(capi.OpenOptions, options, abi_size, "flags") orelse 0;
    if ((flags & ~open_known_flags) != 0) return error.InvalidArgument;
    try validateOpenOptionsReserved(capi.OpenOptions, options, abi_size);

    const storage_kind: StorageKind = switch (readOptionField(capi.OpenOptions, options, abi_size, "storage_kind") orelse capi.storage_kind_directory) {
        capi.storage_kind_directory => .directory,
        capi.storage_kind_lite => .lite,
        else => return error.InvalidArgument,
    };
    const open_mode = try openModeFromU32(readOptionField(capi.OpenOptions, options, abi_size, "open_mode") orelse capi.open_mode_writer);
    const profile = try profileFromU32(readOptionField(capi.OpenOptions, options, abi_size, "profile") orelse capi.profile_native);
    const map_size = readOptionField(capi.OpenOptions, options, abi_size, "map_size") orelse 0;
    if (map_size > std.math.maxInt(usize)) return error.InvalidArgument;

    var resolved = LiteResolvedOpenOptions{
        .storage_kind = storage_kind,
        .open_mode = open_mode,
        .profile = profile,
        .map_size = if (map_size == 0) null else @as(usize, @intCast(map_size)),
        .no_sync = (flags & capi.open_flag_no_sync) != 0,
        .inference = .{
            .remote_provider_configured = (flags & capi.open_flag_remote_provider_configured) != 0,
            .local_runtime_configured = (flags & capi.open_flag_local_runtime_configured) != 0,
            .host_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_host_budget_mb") orelse 0,
            .backend_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_backend_budget_mb") orelse 0,
            .combined_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_combined_budget_mb") orelse 0,
            .kv_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_kv_budget_mb") orelse 0,
            .scratch_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_scratch_budget_mb") orelse 0,
            .process_memory_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_process_memory_budget_mb") orelse 0,
        },
        .generated_enrichment_replay = (flags & capi.open_flag_generated_enrichment_replay) != 0,
        .busy_timeout_ms = readOptionField(capi.OpenOptions, options, abi_size, "busy_timeout_ms") orelse 0,
    };
    if ((flags & capi.open_flag_ttl_cleanup) != 0) {
        const owner_id = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_owner_id") orelse capi.Slice{};
        if (owner_id.ptr == null and owner_id.len != 0) {
            return error.InvalidArgument;
        }
        var ttl_cfg = db_mod.ttl_runtime.Config{
            .enabled = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_enabled") orelse false,
            .lease_owned = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_lease_owned") orelse false,
        };
        if (owner_id.len != 0) {
            ttl_cfg.owner_id = owner_id.ptr.?[0..owner_id.len];
        }
        const lease_ttl_ms = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_lease_ttl_ms") orelse 0;
        const interval_ms = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_interval_ms") orelse 0;
        const batch_size = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_batch_size") orelse 0;
        const grace_period_ns = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_grace_period_ns") orelse 0;
        if (lease_ttl_ms != 0) ttl_cfg.lease_ttl_ms = lease_ttl_ms;
        if (interval_ms != 0) ttl_cfg.interval_ms = interval_ms;
        if (batch_size != 0) ttl_cfg.batch_size = batch_size;
        if (grace_period_ns != 0) ttl_cfg.grace_period_ns = grace_period_ns;
        resolved.ttl_cleanup = ttl_cfg;
    }
    try validateResolvedOpenOptions(resolved);
    return resolved;
}

fn openLiteHandle(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    const out = out_handle orelse return .invalid_argument;
    out.* = null;
    const handle = openLiteHandleAlloc(path, resolved, create) catch |err| return capi.mapError(err);
    out.* = publishHandle(handle) catch |err| return capi.mapError(err);
    return .ok;
}

fn openLiteHandleAlloc(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
) !*Handle {
    return try openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, resolved, create, null, null);
}

/// Zig embedding seam behind the C ABI. It constructs the same opaque handle
/// and therefore exercises the same exported request/close/callback paths, but
/// lets an in-process host supply deterministic std.Io and runtime ownership.
/// The runtime and I/O interface must outlive the returned handle.
pub const HostLiteOpenOptions = struct {
    create: bool = false,
    read_only: bool = false,
    hosted: bool = true,
    no_sync: bool = false,
};

pub fn openLiteHandleWithRuntime(
    alloc: Allocator,
    path: []const u8,
    io: std.Io,
    backend_runtime: *db_mod.background_runtime.BackendRuntime,
    options: HostLiteOpenOptions,
) !*anyopaque {
    return publishHandle(try openLiteHandleAllocWithRuntime(alloc, path, .{
        .open_mode = if (options.read_only) .query_readonly else .writer,
        .profile = if (options.hosted) .hosted else .native,
        .no_sync = options.no_sync,
    }, options.create, io, backend_runtime));
}

pub fn closeLiteRuntimeHandle(handle_ptr: ?*anyopaque) void {
    closeHandleId(handle_ptr);
}

fn openLiteHandleAllocWithRuntime(
    alloc: Allocator,
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    borrowed_io: ?std.Io,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !*Handle {
    if (create and !liteOpenModeCanWrite(resolved.open_mode)) return error.InvalidArgument;
    var backend = if (create)
        try lite_backend.Handle.createWithOptions(alloc, path, .{
            .exclusive = true,
            .no_sync = resolved.no_sync,
            .io = borrowed_io,
        })
    else
        try lite_backend.Handle.open(alloc, path, .{
            .read_only = resolved.open_mode == .query_readonly or resolved.open_mode == .status_only,
            .no_sync = resolved.no_sync,
            .io = borrowed_io,
        });
    errdefer backend.deinit();

    var opts = db_mod.OpenOptions{
        .open_mode = resolved.open_mode,
        .external_derived_checkpoints = false,
        .backend_runtime = backend_runtime,
    };
    if (resolved.map_size) |map_size| opts.map_size = map_size;
    opts.no_sync = resolved.no_sync;
    if (resolved.ttl_cleanup) |ttl_cleanup| opts.ttl_cleanup = ttl_cleanup;
    if (resolved.generated_enrichment_replay) {
        opts.enrichment = .{ .enable_without_producers = true };
    }
    if (resolved.profile == .hosted) {
        opts.executor = .{ .backend = .manual };
        opts.ttl_cleanup = .{ .enabled = false };
        opts.transaction_recovery = .{ .enabled = false };
        opts.text_merge = .{ .enabled = false };
        opts.sparse_compaction = .{ .enabled = false };
    }
    try backend.configureDbOpenOptions(&opts);

    // One identity policy for every Lite surface (C ABI, embedded package,
    // CLI): pin a new file to the embedded root identity and adopt whatever
    // identity an existing file already carries, so a database created
    // through one surface opens through any other.
    const identity = antfly.lite.connection.identityOpenOptions(create);
    opts.identity_namespace = identity.identity_namespace;
    opts.prefer_existing_identity_namespace = identity.prefer_existing_identity_namespace;
    var db = try db_mod.DB.open(alloc, path, opts);
    errdefer db.close();

    if (create) {
        // Antfly Lite databases provision the same default full-text index
        // the server provisions on every table create, through the routine
        // shared with the CLI and the embedded package.
        try antfly.lite.connection.provisionDefaultFullTextIndex(&db);
    }

    const handle = alloc.create(Handle) catch return error.OutOfMemory;
    errdefer alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .db = db,
        .open_mode = resolved.open_mode,
        .owned_lite_backend = backend,
        .lite_profile = resolved.profile,
        .lite_inference_status = lite_backend.inferenceStatusForProfileWithOptions(resolved.profile, resolved.inference),
        .lite_generated_enrichment_replay = resolved.generated_enrichment_replay,
    };
    // Only the native profile runs background enrichment automatically;
    // only builds that both advertise (lite-local-inference-runtime) and
    // actually link (capi_build_options.inference_enabled) the local
    // inference runtime may construct one. Every other combination -- flag
    // unset, default build, hosted profile -- leaves the handle exactly as
    // it was before this feature existed.
    if (resolved.profile == .native and
        resolved.inference.local_runtime_configured and
        capi_build_options.inference_enabled and
        lite_backend.capabilitiesForProfile(.native).local_inference_runtime)
    {
        // `backend.deinit()`, `db.close()`, and `alloc.destroy(handle)` are
        // already registered as `errdefer`s above (in that unwind order);
        // adding any of that cleanup here too would run it twice on this
        // error path. Only unwind the state this function itself owns.
        try startLiteEmbeddedInference(handle, alloc, path, .{
            .host_budget_mb = resolved.inference.host_budget_mb,
            .backend_budget_mb = resolved.inference.backend_budget_mb,
            .combined_budget_mb = resolved.inference.combined_budget_mb,
            .kv_budget_mb = resolved.inference.kv_budget_mb,
            .scratch_budget_mb = resolved.inference.scratch_budget_mb,
            .process_memory_budget_mb = resolved.inference.process_memory_budget_mb,
        });
    }
    // Restores the managed enrichment runtime for indexes/enrichments that
    // were already declared in a prior session (`create` only ever adds the
    // default full-text index, so this is a no-op there). Must run after
    // `startLiteEmbeddedInference` so an embedded local provider is already
    // attached to the handle when this reads it.
    refreshLiteManagedEmbeddingRuntime(handle) catch |err| {
        stopLiteEmbeddedInference(handle);
        return err;
    };
    handle.db.startQuarantineRetryWorkerIfNeeded();
    return handle;
}

fn dbOpenOptionsFromResolved(resolved: LiteResolvedOpenOptions, lite: bool) db_mod.OpenOptions {
    var opts = db_mod.OpenOptions{
        .open_mode = resolved.open_mode,
        .external_derived_checkpoints = !lite,
    };
    if (resolved.map_size) |map_size| opts.map_size = map_size;
    opts.no_sync = resolved.no_sync;
    if (resolved.ttl_cleanup) |ttl_cleanup| opts.ttl_cleanup = ttl_cleanup;
    if (resolved.generated_enrichment_replay) {
        opts.enrichment = .{ .enable_without_producers = true };
    }
    if (resolved.profile == .hosted) {
        opts.executor = .{ .backend = .manual };
        opts.ttl_cleanup = .{ .enabled = false };
        opts.transaction_recovery = .{ .enabled = false };
        opts.text_merge = .{ .enabled = false };
        opts.sparse_compaction = .{ .enabled = false };
    }
    return opts;
}

fn openDirectoryHandle(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    const out = out_handle orelse return .invalid_argument;
    out.* = null;
    if (create) return .invalid_argument;
    const handle = openDirectoryHandleAlloc(path, resolved) catch |err| return capi.mapError(err);
    out.* = publishHandle(handle) catch |err| return capi.mapError(err);
    return .ok;
}

fn openDirectoryHandleAlloc(path: []const u8, resolved: LiteResolvedOpenOptions) !*Handle {
    const alloc = std.heap.c_allocator;
    var db = try db_mod.DB.open(alloc, path, dbOpenOptionsFromResolved(resolved, false));
    errdefer db.close();
    const handle = alloc.create(Handle) catch return error.OutOfMemory;
    errdefer alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .db = db,
        .open_mode = resolved.open_mode,
    };
    handle.db.startQuarantineRetryWorkerIfNeeded();
    return handle;
}

fn openGenericHandle(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    const first = openGenericHandleOnce(path, resolved, create, out_handle);
    if (first != .busy or resolved.busy_timeout_ms == 0) return first;
    // Another writer holds the writer lock. A failed open releases everything
    // it acquired, so retry the whole open with capped exponential backoff
    // until `busy_timeout_ms` elapses, like sqlite3_busy_timeout.
    const io = handleLockIo();
    const deadline_ns = std.Io.Timestamp.now(io, .awake).toNanoseconds() +
        @as(i96, resolved.busy_timeout_ms) * std.time.ns_per_ms;
    var backoff_ms: i64 = 1;
    while (true) {
        const remaining_ns = deadline_ns - std.Io.Timestamp.now(io, .awake).toNanoseconds();
        if (remaining_ns <= 0) return .busy;
        const sleep_ns = @min(@as(i96, backoff_ms) * std.time.ns_per_ms, remaining_ns);
        io.sleep(.fromNanoseconds(sleep_ns), .awake) catch return .busy;
        const code = openGenericHandleOnce(path, resolved, create, out_handle);
        if (code != .busy) return code;
        backoff_ms = @min(backoff_ms * 2, 50);
    }
}

fn openGenericHandleOnce(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    return switch (resolved.storage_kind) {
        .directory => openDirectoryHandle(path, resolved, create, out_handle),
        .lite => openLiteHandle(path, resolved, create, out_handle),
    };
}

fn cStringSpan(path: ?[*:0]const u8) ?[]const u8 {
    const ptr = path orelse return null;
    return std.mem.span(ptr);
}

pub export fn antfly_db_open_with_options(path: ?[*:0]const u8, options: ?*const capi.OpenOptions, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const out = out_handle orelse return .invalid_argument;
    out.* = null;
    const path_slice = cStringSpan(path) orelse return .invalid_argument;
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);
    return openGenericHandle(path_slice, resolved, false, out);
}

pub export fn antfly_db_create_with_options(path: ?[*:0]const u8, options: ?*const capi.OpenOptions, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const out = out_handle orelse return .invalid_argument;
    out.* = null;
    const path_slice = cStringSpan(path) orelse return .invalid_argument;
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);
    return openGenericHandle(path_slice, resolved, true, out);
}

pub export fn antfly_lite_open(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return .invalid_argument;
    };
    return openLiteHandle(path_slice, .{}, false, out_handle);
}

pub export fn antfly_lite_create(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return .invalid_argument;
    };
    return openLiteHandle(path_slice, .{}, true, out_handle);
}

pub export fn antfly_lite_open_hosted(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return .invalid_argument;
    };
    return openLiteHandle(path_slice, .{ .profile = .hosted }, false, out_handle);
}

pub export fn antfly_lite_create_hosted(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return .invalid_argument;
    };
    return openLiteHandle(path_slice, .{ .profile = .hosted }, true, out_handle);
}

pub export fn antfly_lite_open_readonly(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return .invalid_argument;
    };
    return openLiteHandle(path_slice, .{ .open_mode = .query_readonly }, false, out_handle);
}

pub export fn antfly_lite_open_status_only(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) capi.ErrorCode {
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return .invalid_argument;
    };
    return openLiteHandle(path_slice, .{ .open_mode = .status_only }, false, out_handle);
}

fn resetOutBuffer(out_buf: ?*capi.Buffer) ?*capi.Buffer {
    const out = out_buf orelse return null;
    out.* = .{};
    return out;
}

pub export fn antfly_db_capabilities_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const profile = handle.lite_profile orelse .native;
    const inference = handle.lite_inference_status orelse lite_backend.inferenceStatusForProfile(profile);
    out.* = stringifyJson(lite_backend.capabilitiesForProfileWithInferenceStatus(profile, inference)) catch return .internal;
    return .ok;
}

/// Storage block for a normal Antfly directory handle's status report.
const directory_storage_status: lite_backend.StorageStatus = .{
    .format = "directory",
    .engine = "directory",
    .primary_layout = "directory",
    .replay_layout = "directory",
    .index_layout = "directory",
};

pub export fn antfly_db_status_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const storage: lite_backend.StorageStatus = if (handle.owned_lite_backend) |*backend|
        backend.storageStatus()
    else
        directory_storage_status;

    const stats = handle.db.stats(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeDBStats(handle.alloc, stats);

    const indexes = dbIndexStatsProjectionAlloc(handle.alloc, stats) catch return .internal;
    defer if (indexes.len > 0) handle.alloc.free(indexes);

    const profile = handle.lite_profile orelse .native;
    const inference = handle.lite_inference_status orelse lite_backend.inferenceStatusForProfile(profile);
    const status = lite_backend.Status(JsonDBStats){
        .storage = storage,
        .stats = jsonDBStatsProjection(stats, indexes),
        .pending_work = handle.db.pendingWorkStats(),
        .inference = inference,
        .capabilities = lite_backend.capabilitiesForProfileWithInferenceStatus(profile, inference),
    };

    const bytes = std.fmt.allocPrint(handle.alloc, "{f}", .{std.json.fmt(status, .{})}) catch return .internal;
    out.* = .{ .ptr = bytes.ptr, .len = bytes.len };
    return .ok;
}

pub export fn antfly_db_backup(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .maintain) orelse return .invalid_argument;
    defer guard.leave();
    const out_buf_ptr = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const handle = guard.handle;

    var out = std.ArrayList(u8).empty;
    defer out.deinit(handle.alloc);
    portable_backup.exportPortable(handle.alloc, handle.db.core.store, &out) catch |err| return capi.mapError(err);
    portable_backup.validatePortable(handle.alloc, out.items) catch |err| return capi.mapError(err);
    const bytes = out.toOwnedSlice(handle.alloc) catch return .internal;
    out = .empty;
    out_buf_ptr.* = .{ .ptr = bytes.ptr, .len = bytes.len };
    return .ok;
}

pub export fn antfly_db_import_backup(handle_ptr: ?*anyopaque, backup: capi.Slice) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (backup.len == 0) return .invalid_argument;
    if (backup.ptr == null and backup.len != 0) return .invalid_argument;
    const bytes = backup.bytes();
    if (handle.owned_lite_backend) |*backend| {
        lite_restore_staging.importPortableIntoLiteDb(handle.alloc, &handle.db, backend, bytes) catch |err| return capi.mapError(err);
    } else {
        handle.db.importPortableIntoEmpty(handle.alloc, bytes, handle.db.core.identity_namespace) catch |err| return capi.mapError(err);
    }
    return .ok;
}

const RestoreReport = struct {
    format: []const u8,
    path: []const u8,
};

fn restoreFormatName(kind: StorageKind) []const u8 {
    return switch (kind) {
        .lite => "aflite",
        .directory => "directory",
    };
}

pub export fn antfly_restore_backup_json(
    dest_path: ?[*:0]const u8,
    options: ?*const capi.OpenOptions,
    backup: capi.Slice,
    replace: bool,
    out_buf: ?*capi.Buffer,
) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const path = cStringSpan(dest_path) orelse return .invalid_argument;
    if (backup.len == 0) return .invalid_argument;
    if (backup.ptr == null and backup.len != 0) return .invalid_argument;
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);

    const alloc = std.heap.c_allocator;
    var encoded_report = stringifyJson(RestoreReport{ .format = restoreFormatName(resolved.storage_kind), .path = path }) catch return .internal;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const restored = switch (resolved.storage_kind) {
        .lite => restorePortableBackupToLiteFile(alloc, io_impl.io(), null, path, backup.bytes(), replace, null),
        .directory => restorePortableBackupToDirectory(alloc, io_impl.io(), path, backup.bytes(), replace),
    };
    restored catch |err| {
        antfly_buffer_free(&encoded_report);
        return capi.mapError(err);
    };
    out.* = encoded_report;
    return .ok;
}

pub export fn antfly_restore_backup_file_json(
    dest_path: ?[*:0]const u8,
    options: ?*const capi.OpenOptions,
    backup_path: ?[*:0]const u8,
    replace: bool,
    out_buf: ?*capi.Buffer,
) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const destination = cStringSpan(dest_path) orelse return .invalid_argument;
    const source = cStringSpan(backup_path) orelse return .invalid_argument;
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);

    const alloc = std.heap.c_allocator;
    var encoded_report = stringifyJson(RestoreReport{ .format = restoreFormatName(resolved.storage_kind), .path = destination }) catch return .internal;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const restored = switch (resolved.storage_kind) {
        .lite => restorePortableBackupPathToLiteFile(alloc, io_impl.io(), destination, source, replace),
        .directory => restorePortableBackupPathToDirectory(alloc, io_impl.io(), destination, source, replace),
    };
    restored catch |err| {
        antfly_buffer_free(&encoded_report);
        return capi.mapError(err);
    };
    out.* = encoded_report;
    return .ok;
}

pub export fn antfly_lite_check_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (handle.owned_lite_backend) |*backend| {
        out.* = stringifyJson(backend.check() catch |err| return capi.mapError(err)) catch return .internal;
        return .ok;
    }
    return .invalid_argument;
}

pub export fn antfly_lite_check_file_json(path: ?[*:0]const u8, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const path_slice = cStringSpan(path) orelse return .invalid_argument;
    const alloc = std.heap.c_allocator;
    const report = lite_backend.checkFile(alloc, path_slice) catch |err| return capi.mapError(err);
    out.* = stringifyJson(report) catch return .internal;
    return .ok;
}

pub export fn antfly_lite_copy_stable_snapshot_json(
    handle_ptr: ?*anyopaque,
    dest_path: ?[*:0]const u8,
    replace: bool,
    out_buf: ?*capi.Buffer,
) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .maintain) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (handle.owned_lite_backend) |*backend| {
        const dest = cStringSpan(dest_path) orelse return .invalid_argument;
        out.* = stringifyJson(backend.copyStableSnapshot(dest, replace) catch |err| return capi.mapError(err)) catch return .internal;
        return .ok;
    }
    return .invalid_argument;
}

pub export fn antfly_lite_copy_stable_snapshot_file_json(
    src_path: ?[*:0]const u8,
    dest_path: ?[*:0]const u8,
    replace: bool,
    out_buf: ?*capi.Buffer,
) capi.ErrorCode {
    _ = resetOutBuffer(out_buf) orelse return .invalid_argument;
    var handle_ptr: ?*anyopaque = null;
    const open_status = antfly_lite_open_readonly(src_path, &handle_ptr);
    if (open_status != .ok) return open_status;
    defer antfly_db_close(handle_ptr);
    return antfly_lite_copy_stable_snapshot_json(handle_ptr, dest_path, replace, out_buf);
}

const LiteCompactReport = struct {
    compacted: bool,
    vacuum: lite_backend.VacuumReport,
};

fn prepareLiteCompact(handle: *Handle) !void {
    try handle.db.runUntilIdle();
    try handle.db.forceCompactTextIndexes();
    try handle.db.drainScheduledTextMerges();
    try handle.db.sync(true);
    try handle.db.syncIndexes(true);
}

pub export fn antfly_lite_compact_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (handle.owned_lite_backend) |*backend| {
        prepareLiteCompact(handle) catch |err| return capi.mapError(err);
        const report = LiteCompactReport{
            .compacted = true,
            .vacuum = backend.vacuum() catch |err| return capi.mapError(err),
        };
        out.* = stringifyJson(report) catch return .internal;
        return .ok;
    }
    return .invalid_argument;
}

pub export fn antfly_lite_vacuum_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (handle.owned_lite_backend) |*backend| {
        out.* = stringifyJson(backend.vacuum() catch |err| return capi.mapError(err)) catch return .internal;
        return .ok;
    }
    return .invalid_argument;
}

const LiteReplayGeneratedEnrichmentsReport = struct {
    replayed: usize,
};

pub export fn antfly_db_replay_generated_enrichments_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) capi.ErrorCode {
    const out = resetOutBuffer(out_buf) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .maintain) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const replayed = handle.db.replayGeneratedEnrichmentsFromStoredDocs(handle.alloc) catch |err| return capi.mapError(err);
    out.* = stringifyJson(LiteReplayGeneratedEnrichmentsReport{ .replayed = replayed }) catch return .internal;
    return .ok;
}

pub fn restorePortableBackupToLiteFileWithRuntime(
    alloc: Allocator,
    io: std.Io,
    backend_runtime: *db_mod.background_runtime.BackendRuntime,
    dest_path: []const u8,
    backup: []const u8,
    replace: bool,
    cancel: ?*const antfly.storage_maintenance.CancelToken,
) !void {
    try restorePortableBackupToLiteFile(alloc, io, backend_runtime, dest_path, backup, replace, cancel);
}

fn restorePortableBackupToLiteFile(
    alloc: Allocator,
    io: std.Io,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    dest_path: []const u8,
    backup: []const u8,
    replace: bool,
    cancel: ?*const antfly.storage_maintenance.CancelToken,
) !void {
    if (!lite_backend.isAflitePath(dest_path)) return error.InvalidArgument;
    if (backup.len == 0) return error.InvalidArgument;

    const Populate = struct {
        fn run(context: []const u8, alloc_inner: Allocator, db: *db_mod.DB, _: std.Io) !void {
            try lite_restore_staging.populateUnpublishedLiteDb(alloc_inner, db, context);
        }
    };
    try restorePortableSourceToLiteFile(alloc, io, backend_runtime, dest_path, replace, backup, Populate.run, cancel);
}

/// Restores a portable backup into a directory database at `dest_path`.
///
/// Uses the engine's generation lifecycle, the same publication path as
/// server-side table restores:
/// - The exclusive generation transition fails with BUSY while any database
///   (libantfly handle, server, or CLI, in this process or another) holds a
///   read lease on the destination, and new opens wait or fail until
///   publication finishes.
/// - The database is built and indexed in an unpublished sibling generation,
///   sealed, then published with a durable marker and an atomic exchange (or
///   rename when nothing exists yet), and the parent directory is synced.
/// - A crash at any point leaves `dest_path` holding the complete old or the
///   complete new database; the next open reconciles the marker and
///   reclaims the other generation.
fn restorePortableBackupToDirectory(
    alloc: Allocator,
    io: std.Io,
    dest_path: []const u8,
    backup: []const u8,
    replace: bool,
) !void {
    if (backup.len == 0) return error.InvalidArgument;
    const live_path = std.mem.trimEnd(u8, dest_path, "/");
    if (live_path.len == 0) return error.InvalidArgument;
    var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithIo(live_path, io);
    defer transition.deinit();
    // Reconcile an interrupted earlier publication before deciding whether
    // the destination exists.
    try transition.reconcilePublished();
    if (capiPathExists(io, live_path) and !replace) return error.PathAlreadyExists;

    var staged = try transition.beginStaging();
    defer staged.deinit();
    {
        var db = try db_mod.DB.open(alloc, staged.path(), .{ .staged_generation = &staged });
        defer db.close();
        try db.importPortableIntoUnpublishedEmpty(alloc, backup, db.core.identity_namespace);
        _ = try db.rebuildDenseIndexesForTargetCoverage(alloc);
        _ = try db.rebuildSparseIndexesForTargetCoverage(alloc);
        try db.rebuildGraphIndexesForTargetCoverage(alloc);
        _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        // Drain any derived or replayed generated work before publication,
        // as the Lite restore does, so a read-only reopen sees final results.
        try db.runUntilIdle();
        try db.sync(true);
        try db.syncIndexes(true);
    }
    switch (try staged.publish()) {
        .durable => {},
        .durability_uncertain => {
            std.log.err("directory restore published but crash durability could not be confirmed path={s}", .{live_path});
            return error.DurabilityOutcomeUnknown;
        },
    }
}

fn restorePortableBackupPathToDirectory(
    alloc: Allocator,
    io: std.Io,
    dest_path: []const u8,
    backup_path: []const u8,
    replace: bool,
) !void {
    if (!std.mem.endsWith(u8, backup_path, ".afb")) return error.InvalidArgument;
    const backup = std.Io.Dir.cwd().readFileAlloc(io, backup_path, alloc, .limited(lite_restore_staging.max_afb_file_bytes)) catch |err| switch (err) {
        error.StreamTooLong => return error.InvalidArgument,
        else => return err,
    };
    defer alloc.free(backup);
    try restorePortableBackupToDirectory(alloc, io, dest_path, backup, replace);
}

fn restorePortableBackupPathToLiteFile(
    alloc: Allocator,
    io: std.Io,
    dest_path: []const u8,
    backup_path: []const u8,
    replace: bool,
) !void {
    if (!std.mem.endsWith(u8, backup_path, ".afb")) return error.InvalidArgument;
    var file = if (std.fs.path.isAbsolute(backup_path))
        try std.Io.Dir.openFileAbsolute(io, backup_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, backup_path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size == 0 or stat.size > lite_restore_staging.max_afb_file_bytes) return error.InvalidArgument;

    const Context = struct {
        file: std.Io.File,
        file_size: u64,
    };
    const Populate = struct {
        fn run(context: Context, alloc_inner: Allocator, db: *db_mod.DB, io_inner: std.Io) !void {
            try lite_restore_staging.populateUnpublishedLiteDbFromPortableFile(
                alloc_inner,
                db,
                io_inner,
                context.file,
                context.file_size,
            );
        }
    };
    try restorePortableSourceToLiteFile(
        alloc,
        io,
        null,
        dest_path,
        replace,
        Context{ .file = file, .file_size = stat.size },
        Populate.run,
        null,
    );
}

fn restorePortableSourceToLiteFile(
    alloc: Allocator,
    io: std.Io,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    dest_path: []const u8,
    replace: bool,
    context: anytype,
    comptime populate: anytype,
    cancel: ?*const antfly.storage_maintenance.CancelToken,
) !void {
    if (!lite_backend.isAflitePath(dest_path)) return error.InvalidArgument;
    if (cancel) |token| try token.check();

    const dest_exists = capiPathExists(io, dest_path);
    if (dest_exists and !replace) return error.PathAlreadyExists;

    var dest_lock = try antfly.lite.native.lockWriterPathWithIo(alloc, io, dest_path);
    defer dest_lock.close();

    if (!dest_exists and !replace and capiPathExists(io, dest_path)) return error.PathAlreadyExists;

    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.restore-tmp.aflite", .{dest_path});
    defer alloc.free(tmp_path);
    try capiDeleteFileIfExists(io, tmp_path);
    errdefer capiDeleteFilePath(io, tmp_path) catch {};

    {
        var backend = try lite_backend.Handle.createWithOptions(alloc, tmp_path, .{
            .exclusive = true,
            .io = io,
        });
        defer backend.deinit();

        var opts = db_mod.OpenOptions{
            .open_mode = .writer,
            .external_derived_checkpoints = false,
            .backend_runtime = backend_runtime,
        };
        // A caller-supplied std.Io runtime may be cooperative (VoprIo) rather
        // than backed by std.Io.Threaded. Restore is synchronous, so it must
        // not select the executor variant that requires an owned Threaded
        // implementation merely because the runtime exposes an Io interface.
        if (backend_runtime != null) opts.executor = .{ .backend = .manual };
        try backend.configureDbOpenOptions(&opts);

        var db = try db_mod.DB.open(alloc, tmp_path, opts);
        defer db.close();
        try populate(context, alloc, &db, io);
    }

    if (cancel) |token| try token.check();

    capiRenameFilePath(io, tmp_path, dest_path) catch |err| {
        capiDeleteFilePath(io, tmp_path) catch {};
        return err;
    };
    lite_restore_staging.confirmPublishedFileDurability(io, dest_path) catch |err| {
        std.log.err(
            "Lite restore published but crash durability could not be confirmed path={s} class={s}",
            .{ dest_path, @errorName(err) },
        );
        return error.DurabilityOutcomeUnknown;
    };
}

fn capiPathExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

fn capiDeleteFileIfExists(io: std.Io, path: []const u8) !void {
    capiDeleteFilePath(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

fn capiRenameFilePath(io: std.Io, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(old_path) or std.fs.path.isAbsolute(new_path)) {
        try std.Io.Dir.renameAbsolute(old_path, new_path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
    }
}

fn capiDeleteFilePath(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

pub export fn antfly_db_set_readable_lease_hook(
    handle_ptr: ?*anyopaque,
    group_id: u64,
    callback_ctx: ?*anyopaque,
    callback: ?ReadableLeaseHookFn,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (callback) |hook| {
        handle.readable_lease_hook = .{
            .group_id = group_id,
            .callback_ctx = callback_ctx,
            .callback = hook,
        };
    } else {
        handle.readable_lease_hook = null;
    }
    return .ok;
}

/// Frees bytes this library returned as a pointer/length pair.
fn freeRawBuffer(ptr: ?[*]u8, len: usize) void {
    if (ptr == null or len == 0) return;
    std.heap.c_allocator.free(ptr.?[0..len]);
}

pub export fn antfly_buffer_free(buffer: ?*capi.Buffer) void {
    const out = buffer orelse return;
    freeRawBuffer(out.ptr, out.len);
    out.* = .{};
}

fn wipeBufferBytes(buffer: capi.Buffer) void {
    if (buffer.ptr == null or buffer.len == 0) return;
    std.crypto.secureZero(u8, buffer.ptr.?[0..buffer.len]);
}

pub export fn antfly_buffer_free_zero(buffer: ?*capi.Buffer) void {
    const out = buffer orelse return;
    wipeBufferBytes(out.*);
    antfly_buffer_free(out);
}

test "capi zero buffer helper wipes bytes before free" {
    var bytes = [_]u8{ 0xaa, 0xbb, 0xcc, 0xdd };
    wipeBufferBytes(.{ .ptr = &bytes, .len = bytes.len });
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0 }, &bytes);
    wipeBufferBytes(.{});

    var empty: capi.Buffer = .{};
    antfly_buffer_free_zero(&empty);
    try std.testing.expect(empty.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
}

pub export fn antfly_dense_search_result_free(result: *capi.DenseSearchResult) void {
    if (result.hits_ptr) |hits_ptr| {
        const hits = hits_ptr[0..result.hit_count];
        for (hits) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) {
                std.heap.c_allocator.free(hit.id_ptr.?[0..hit.id_len]);
            }
        }
        std.heap.c_allocator.free(hits);
    }
    result.* = .{};
}

pub export fn antfly_packed_dense_search_result_free(result: *capi.PackedDenseSearchResult) void {
    if (result.hits_ptr) |hits_ptr| {
        const hits = hits_ptr[0..result.hit_count];
        std.heap.c_allocator.free(hits);
    }
    if (result.ids_ptr != null and result.ids_len > 0) {
        std.heap.c_allocator.free(result.ids_ptr.?[0..result.ids_len]);
    }
    result.* = .{};
}

fn packDenseHits(
    total_hits: u32,
    ids: []const []const u8,
    scores: []const f32,
    identity_read_generation: u64,
    out_result: *capi.PackedDenseSearchResult,
) !void {
    std.debug.assert(ids.len == scores.len);

    const alloc = std.heap.c_allocator;
    const hits = try alloc.alloc(capi.PackedDenseSearchHit, ids.len);
    errdefer alloc.free(hits);

    var ids_len: usize = 0;
    for (ids) |id| ids_len += id.len;
    const ids_blob = try alloc.alloc(u8, ids_len);
    errdefer alloc.free(ids_blob);

    var cursor: usize = 0;
    for (ids, scores, 0..) |id, score, i| {
        @memcpy(ids_blob[cursor..][0..id.len], id);
        hits[i] = .{
            .id_offset = cursor,
            .id_len = id.len,
            .score = score,
        };
        cursor += id.len;
    }

    out_result.* = .{
        .hits_ptr = if (hits.len > 0) hits.ptr else null,
        .hit_count = hits.len,
        .total_hits = total_hits,
        .ids_ptr = if (ids_blob.len > 0) ids_blob.ptr else null,
        .ids_len = ids_blob.len,
        .identity_read_generation = identity_read_generation,
    };
}

const DenseOwnedResult = struct {
    alloc: Allocator,
    total_hits: u32,
    ids: [][]const u8,
    scores: []f32,
    identity_read_generation: u64,

    fn deinit(self: *DenseOwnedResult) void {
        for (self.ids) |id| self.alloc.free(id);
        if (self.ids.len > 0) self.alloc.free(self.ids);
        if (self.scores.len > 0) self.alloc.free(self.scores);
        self.* = undefined;
    }
};

const DenseOwnedProfile = struct {
    result: DenseOwnedResult,
    total_ns: u64 = 0,
    index_lookup_ns: u64 = 0,
    search_ns: u64 = 0,
    hits_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,

    fn deinit(self: *DenseOwnedProfile) void {
        self.result.deinit();
        self.* = undefined;
    }

    fn takeResult(self: *DenseOwnedProfile) DenseOwnedResult {
        const result = self.result;
        self.result = .{
            .alloc = result.alloc,
            .total_hits = 0,
            .ids = &.{},
            .scores = &.{},
            .identity_read_generation = result.identity_read_generation,
        };
        return result;
    }
};

const DenseResolvedHit = struct {
    id: []u8,
    score: f32,
};

const DenseResolvedHits = struct {
    alloc: Allocator,
    total_hits: u32,
    hits: []DenseResolvedHit,

    fn deinit(self: *DenseResolvedHits) void {
        for (self.hits) |hit| self.alloc.free(hit.id);
        if (self.hits.len > 0) self.alloc.free(self.hits);
        self.* = undefined;
    }
};

const DenseWireOwnedProfile = struct {
    out: capi.Buffer = .{},
    total_ns: u64 = 0,
    decode_ns: u64 = 0,
    search_ns: u64 = 0,
    resolve_ns: u64 = 0,
    encode_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,
};

fn resolveDenseHitsFromProfiled(
    alloc: Allocator,
    entry: anytype,
    results: *hbc.ProfiledSearchResults,
    limit: u32,
    offset: u32,
) !DenseResolvedHits {
    const raw_hits = results.results.getHits();
    const start: u32 = @min(offset, @as(u32, @intCast(raw_hits.len)));
    const end: u32 = @min(start + limit, @as(u32, @intCast(raw_hits.len)));
    const sliced_hits = raw_hits[@intCast(start)..@intCast(end)];

    const resolved = try alloc.alloc(DenseResolvedHit, sliced_hits.len);
    errdefer alloc.free(resolved);
    var resolved_count: usize = 0;
    errdefer {
        for (resolved[0..resolved_count]) |hit| alloc.free(hit.id);
    }

    for (sliced_hits, 0..) |hit, i| {
        const result_index: usize = @as(usize, @intCast(start)) + i;
        const id = if (results.results.takeMetadata(result_index)) |metadata|
            metadata
        else
            (try entry.index.getMetadata(hit.vector_id)) orelse return error.Internal;
        resolved[i] = .{
            .id = id,
            .score = vector_mod.similarityFromDistance(hit.distance, entry.metric),
        };
        resolved_count += 1;
    }

    return .{
        .alloc = alloc,
        .total_hits = @intCast(raw_hits.len),
        .hits = resolved,
    };
}

fn packResolvedDenseHits(
    resolved: *DenseResolvedHits,
    identity_read_generation: u64,
    out_result: *capi.PackedDenseSearchResult,
) !void {
    const alloc = std.heap.c_allocator;
    const hits = try alloc.alloc(capi.PackedDenseSearchHit, resolved.hits.len);
    errdefer alloc.free(hits);

    var ids_len: usize = 0;
    for (resolved.hits) |hit| ids_len += hit.id.len;
    const ids_blob = try alloc.alloc(u8, ids_len);
    errdefer alloc.free(ids_blob);

    var cursor: usize = 0;
    for (resolved.hits, 0..) |hit, i| {
        @memcpy(ids_blob[cursor..][0..hit.id.len], hit.id);
        hits[i] = .{
            .id_offset = cursor,
            .id_len = hit.id.len,
            .score = hit.score,
        };
        cursor += hit.id.len;
    }

    out_result.* = .{
        .hits_ptr = if (hits.len > 0) hits.ptr else null,
        .hit_count = hits.len,
        .total_hits = resolved.total_hits,
        .ids_ptr = if (ids_blob.len > 0) ids_blob.ptr else null,
        .ids_len = ids_blob.len,
        .identity_read_generation = identity_read_generation,
    };
}

fn encodeResolvedDenseWireResponse(
    resolved: *DenseResolvedHits,
    identity_read_generation: u64,
) !capi.Buffer {
    const header_len: usize = 4 + 2 + 2 + 4 + 4 + 4;
    const hits_len: usize = resolved.hits.len * @sizeOf(search_wire.PackedHit);
    var ids_len: usize = 0;
    for (resolved.hits) |hit| ids_len += hit.id.len;
    const total_len = header_len + hits_len + ids_len + @sizeOf(u64);
    const out = try std.heap.c_allocator.alloc(u8, total_len);
    errdefer std.heap.c_allocator.free(out);

    var cursor: usize = 0;
    std.mem.writeInt(u32, out[cursor..][0..4], search_wire.magic, .little);
    cursor += 4;
    std.mem.writeInt(u16, out[cursor..][0..2], search_wire.version, .little);
    cursor += 2;
    std.mem.writeInt(u16, out[cursor..][0..2], @intFromEnum(search_wire.Op.dense_search), .little);
    cursor += 2;
    std.mem.writeInt(u32, out[cursor..][0..4], resolved.total_hits, .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], @intCast(resolved.hits.len), .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], @intCast(ids_len), .little);
    cursor += 4;

    var id_cursor: u32 = 0;
    for (resolved.hits) |hit| {
        std.mem.writeInt(u32, out[cursor..][0..4], id_cursor, .little);
        cursor += 4;
        std.mem.writeInt(u16, out[cursor..][0..2], @intCast(hit.id.len), .little);
        cursor += 2;
        std.mem.writeInt(u16, out[cursor..][0..2], 0, .little);
        cursor += 2;
        std.mem.writeInt(u32, out[cursor..][0..4], @bitCast(hit.score), .little);
        cursor += 4;
        id_cursor += @intCast(hit.id.len);
    }

    for (resolved.hits) |hit| {
        @memcpy(out[cursor..][0..hit.id.len], hit.id);
        cursor += hit.id.len;
    }
    std.mem.writeInt(u64, out[cursor..][0..8], identity_read_generation, .little);

    return .{ .ptr = out.ptr, .len = out.len };
}

fn searchDensePackedFast(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
    identity_read_generation: u64,
    out_result: *capi.PackedDenseSearchResult,
) !bool {
    if (handle.db.core.schema != null and handle.db.core.schema.?.ttl_duration_ns != 0) return false;
    const entry = handle.db.core.index_manager.denseIndex(index_name) orelse return false;
    if (entry.chunk_name != null) return false;

    var profiled = try entry.index.searchProfiledRequest(.{
        .query = vector,
        .k = k,
    });
    defer profiled.results.deinit();

    var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, limit, offset);
    defer resolved.deinit();
    try packResolvedDenseHits(&resolved, identity_read_generation, out_result);
    return true;
}

fn searchDenseWireFast(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
    identity_read_generation: u64,
) !?capi.Buffer {
    if (handle.db.core.schema != null and handle.db.core.schema.?.ttl_duration_ns != 0) return null;
    const entry = handle.db.core.index_manager.denseIndex(index_name) orelse return null;
    if (entry.chunk_name != null) return null;

    var profiled = try entry.index.searchProfiledRequest(.{
        .query = vector,
        .k = k,
    });
    defer profiled.results.deinit();

    var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, limit, offset);
    defer resolved.deinit();
    return try encodeResolvedDenseWireResponse(&resolved, identity_read_generation);
}

fn searchDenseWireOwnedProfiled(
    handle: *Handle,
    request_buf: []const u8,
) !DenseWireOwnedProfile {
    const total_start = monotonicNowNs();

    const decode_start = monotonicNowNs();
    var req = try search_wire.decodeDenseRequest(handle.alloc, request_buf);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    const decode_end = monotonicNowNs();
    const identity_read_generation = try currentIdentityReadGenerationForHandle(handle, null);

    if (handle.db.core.schema == null or handle.db.core.schema.?.ttl_duration_ns == 0) {
        if (handle.db.core.index_manager.denseIndex(req.index_name)) |entry| {
            if (entry.chunk_name == null) {
                const search_start = monotonicNowNs();
                var profiled = try entry.index.searchProfiledRequest(.{
                    .query = req.vector,
                    .k = req.k,
                });
                defer profiled.results.deinit();
                const search_end = monotonicNowNs();

                const resolve_start = monotonicNowNs();
                var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, req.limit, req.offset);
                defer resolved.deinit();
                const resolve_end = monotonicNowNs();

                const encode_start = monotonicNowNs();
                const out = try encodeResolvedDenseWireResponse(&resolved, identity_read_generation);
                const encode_end = monotonicNowNs();

                return .{
                    .out = out,
                    .total_ns = @intCast(encode_end - total_start),
                    .decode_ns = @intCast(decode_end - decode_start),
                    .search_ns = @intCast(search_end - search_start),
                    .resolve_ns = @intCast(resolve_end - resolve_start),
                    .encode_ns = @intCast(encode_end - encode_start),
                    .fallback_ns = 0,
                    .hbc_total_ns = profiled.profile.total_ns,
                    .hbc_setup_ns = profiled.profile.setup_ns,
                    .hbc_root_load_ns = profiled.profile.root_load_ns,
                    .hbc_node_cache_miss_ns = profiled.profile.node_cache_miss_ns,
                    .hbc_node_cache_misses = profiled.profile.node_cache_misses,
                    .hbc_quantized_cache_miss_ns = profiled.profile.quantized_cache_miss_ns,
                    .hbc_quantized_cache_misses = profiled.profile.quantized_cache_misses,
                    .hbc_child_expand_ns = profiled.profile.child_expand_ns,
                    .hbc_leaf_score_ns = profiled.profile.leaf_score_ns,
                    .hbc_rerank_ns = profiled.profile.rerank_ns,
                    .hbc_rerank_vector_load_ns = profiled.profile.rerank_vector_load_ns,
                    .hbc_rerank_distance_ns = profiled.profile.rerank_distance_ns,
                    .hbc_nodes_visited = profiled.profile.nodes_visited,
                    .hbc_leaves_explored = profiled.profile.leaves_explored,
                    .hbc_reranked_vectors = profiled.profile.reranked_vectors,
                    .hit_count = @intCast(resolved.hits.len),
                    .total_hits = resolved.total_hits,
                    .used_fast_path = true,
                };
            }
        }
    }

    const fallback_start = monotonicNowNs();
    var owned = try searchDenseOwned(handle, req.index_name, req.vector, req.k, req.limit, req.offset);
    defer owned.deinit();
    const fallback_end = monotonicNowNs();

    const encode_start = monotonicNowNs();
    const out = try search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation);
    const encode_end = monotonicNowNs();

    return .{
        .out = out,
        .total_ns = @intCast(encode_end - total_start),
        .decode_ns = @intCast(decode_end - decode_start),
        .search_ns = 0,
        .resolve_ns = 0,
        .encode_ns = @intCast(encode_end - encode_start),
        .fallback_ns = @intCast(fallback_end - fallback_start),
        .hbc_total_ns = 0,
        .hbc_setup_ns = 0,
        .hbc_root_load_ns = 0,
        .hbc_node_cache_miss_ns = 0,
        .hbc_node_cache_misses = 0,
        .hbc_quantized_cache_miss_ns = 0,
        .hbc_quantized_cache_misses = 0,
        .hbc_child_expand_ns = 0,
        .hbc_leaf_score_ns = 0,
        .hbc_rerank_ns = 0,
        .hbc_rerank_vector_load_ns = 0,
        .hbc_rerank_distance_ns = 0,
        .hbc_nodes_visited = 0,
        .hbc_leaves_explored = 0,
        .hbc_reranked_vectors = 0,
        .hit_count = @intCast(owned.ids.len),
        .total_hits = owned.total_hits,
        .used_fast_path = false,
    };
}

fn searchDenseOwned(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    var profiled = try searchDenseOwnedProfiled(handle, index_name, vector, k, limit, offset);
    defer profiled.deinit();
    return profiled.takeResult();
}

fn searchDenseOwnedProfiled(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
) !DenseOwnedProfile {
    if (vector.len == 0) return error.InvalidArgument;
    const identity_read_generation = try currentIdentityReadGenerationForHandle(handle, null);

    const total_start = monotonicNowNs();
    const lookup_start = monotonicNowNs();
    if (handle.db.core.schema == null or handle.db.core.schema.?.ttl_duration_ns == 0) {
        if (handle.db.core.index_manager.denseIndex(index_name)) |entry| {
            const lookup_end = monotonicNowNs();
            if (entry.chunk_name == null) {
                const search_start = monotonicNowNs();
                var profiled = try entry.index.searchProfiledRequest(.{
                    .query = vector,
                    .k = k,
                });
                defer profiled.results.deinit();
                const search_end = monotonicNowNs();

                const raw_hits = profiled.results.getHits();
                const start: u32 = @min(offset, @as(u32, @intCast(raw_hits.len)));
                const end: u32 = @min(start + limit, @as(u32, @intCast(raw_hits.len)));
                const sliced_hits = raw_hits[@intCast(start)..@intCast(end)];

                const hits_start = monotonicNowNs();
                const ids = try handle.alloc.alloc([]const u8, sliced_hits.len);
                errdefer handle.alloc.free(ids);
                var id_count: usize = 0;
                errdefer {
                    for (ids[0..id_count]) |id| handle.alloc.free(id);
                }

                const scores = try handle.alloc.alloc(f32, sliced_hits.len);
                errdefer handle.alloc.free(scores);

                for (sliced_hits, 0..) |hit, i| {
                    const result_index: usize = @as(usize, @intCast(start)) + i;
                    const id = if (profiled.results.takeMetadata(result_index)) |metadata|
                        metadata
                    else
                        (try entry.index.getMetadata(hit.vector_id)) orelse return error.Internal;
                    ids[i] = id;
                    id_count += 1;
                    scores[i] = hit.distance;
                }
                const hits_end = monotonicNowNs();

                return .{
                    .result = .{
                        .alloc = handle.alloc,
                        .total_hits = @intCast(raw_hits.len),
                        .ids = ids,
                        .scores = scores,
                        .identity_read_generation = identity_read_generation,
                    },
                    .total_ns = @intCast(hits_end - total_start),
                    .index_lookup_ns = @intCast(lookup_end - lookup_start),
                    .search_ns = @intCast(search_end - search_start),
                    .hits_ns = @intCast(hits_end - hits_start),
                    .fallback_ns = 0,
                    .hbc_total_ns = profiled.profile.total_ns,
                    .hbc_setup_ns = profiled.profile.setup_ns,
                    .hbc_root_load_ns = profiled.profile.root_load_ns,
                    .hbc_node_cache_miss_ns = profiled.profile.node_cache_miss_ns,
                    .hbc_node_cache_misses = profiled.profile.node_cache_misses,
                    .hbc_quantized_cache_miss_ns = profiled.profile.quantized_cache_miss_ns,
                    .hbc_quantized_cache_misses = profiled.profile.quantized_cache_misses,
                    .hbc_child_expand_ns = profiled.profile.child_expand_ns,
                    .hbc_leaf_score_ns = profiled.profile.leaf_score_ns,
                    .hbc_rerank_ns = profiled.profile.rerank_ns,
                    .hbc_rerank_vector_load_ns = profiled.profile.rerank_vector_load_ns,
                    .hbc_rerank_distance_ns = profiled.profile.rerank_distance_ns,
                    .hbc_nodes_visited = profiled.profile.nodes_visited,
                    .hbc_leaves_explored = profiled.profile.leaves_explored,
                    .hbc_reranked_vectors = profiled.profile.reranked_vectors,
                    .hit_count = @intCast(sliced_hits.len),
                    .total_hits = @intCast(raw_hits.len),
                    .used_fast_path = true,
                };
            }
        }
    }
    const lookup_end = monotonicNowNs();

    var req: db_mod.types.SearchRequest = .{
        .index_name = index_name,
        .query = .{ .dense_knn = .{
            .vector = vector,
            .k = k,
        } },
        .limit = limit,
        .offset = offset,
        .include_stored = false,
    };

    // The export already ran prepareDenseSearchRequest, so skip the generic
    // lease hook; restamp and retry like the other search paths.
    const fallback_start = monotonicNowNs();
    var result = try runAtStampedGenerationWithOptions(handle, &req, LocalSearchQuery{}, .{ .prepare = false });
    defer result.deinit();
    const fallback_end = monotonicNowNs();
    const fallback_generation = req.identity_read_generation.?;

    const ids = try handle.alloc.alloc([]const u8, result.hits.len);
    errdefer handle.alloc.free(ids);
    var id_count: usize = 0;
    errdefer {
        for (ids[0..id_count]) |id| handle.alloc.free(id);
    }

    const scores = try handle.alloc.alloc(f32, result.hits.len);
    errdefer handle.alloc.free(scores);
    for (result.hits, 0..) |hit, i| {
        ids[i] = try handle.alloc.dupe(u8, hit.id);
        id_count += 1;
        scores[i] = hit.score orelse 0;
    }
    const total_end = monotonicNowNs();
    return .{
        .result = .{
            .alloc = handle.alloc,
            .total_hits = result.total_hits,
            .ids = ids,
            .scores = scores,
            .identity_read_generation = fallback_generation,
        },
        .total_ns = @intCast(total_end - total_start),
        .index_lookup_ns = @intCast(lookup_end - lookup_start),
        .search_ns = 0,
        .hits_ns = 0,
        .fallback_ns = @intCast(fallback_end - fallback_start),
        .hbc_total_ns = 0,
        .hbc_setup_ns = 0,
        .hbc_root_load_ns = 0,
        .hbc_node_cache_miss_ns = 0,
        .hbc_node_cache_misses = 0,
        .hbc_quantized_cache_miss_ns = 0,
        .hbc_quantized_cache_misses = 0,
        .hbc_child_expand_ns = 0,
        .hbc_leaf_score_ns = 0,
        .hbc_rerank_ns = 0,
        .hbc_rerank_vector_load_ns = 0,
        .hbc_rerank_distance_ns = 0,
        .hbc_nodes_visited = 0,
        .hbc_leaves_explored = 0,
        .hbc_reranked_vectors = 0,
        .hit_count = @intCast(result.hits.len),
        .total_hits = result.total_hits,
        .used_fast_path = false,
    };
}

fn searchTextMatchOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    text: []const u8,
    analyzer: []const u8,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or text.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .match = .{
            .field = field,
            .text = text,
            .analyzer = if (analyzer.len > 0) analyzer else null,
            .boost = boost,
        },
    }, limit, offset);
}

fn searchTextTermOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    term: []const u8,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or term.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .term = .{
            .field = field,
            .term = term,
            .boost = boost,
        },
    }, limit, offset);
}

fn searchTextMatchPhraseOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    text: []const u8,
    analyzer: []const u8,
    fuzziness: u16,
    auto: bool,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or text.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .match_phrase = .{
            .field = field,
            .text = text,
            .analyzer = if (analyzer.len > 0) analyzer else null,
            .max_edits = @intCast(fuzziness),
            .auto_fuzzy = auto,
            .boost = boost,
        },
    }, limit, offset);
}

fn searchTextOwned(
    handle: *Handle,
    index_name: []const u8,
    query: db_mod.types.Query,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    // An empty index name aliases the default full-text index, matching the
    // server's public-query resolution (see api/tables.zig). A name that
    // does not resolve to an existing index still fails inside
    // executeLocalSearch below.
    const resolved_index_name = if (index_name.len == 0) tables_api.default_full_text_index_name else index_name;

    var req: db_mod.types.SearchRequest = .{
        .index_name = resolved_index_name,
        .query = query,
        .limit = limit,
        .offset = offset,
        .include_stored = false,
    };
    var result = try runAtStampedGeneration(handle, &req, LocalSearchQuery{});
    defer result.deinit();
    const identity_read_generation = req.identity_read_generation.?;

    const ids = try handle.alloc.alloc([]const u8, result.hits.len);
    errdefer handle.alloc.free(ids);
    var id_count: usize = 0;
    errdefer {
        for (ids[0..id_count]) |id| handle.alloc.free(id);
    }

    const scores = try handle.alloc.alloc(f32, result.hits.len);
    errdefer handle.alloc.free(scores);
    for (result.hits, 0..) |hit, i| {
        ids[i] = try handle.alloc.dupe(u8, hit.id);
        id_count += 1;
        scores[i] = hit.score orelse 0;
    }

    return .{
        .alloc = handle.alloc,
        .total_hits = result.total_hits,
        .ids = ids,
        .scores = scores,
        .identity_read_generation = identity_read_generation,
    };
}

pub export fn antfly_scan_hash_result_free(result: *capi.ScanHashResult) void {
    if (result.entries_ptr) |entries_ptr| {
        const entries = entries_ptr[0..result.entry_count];
        for (entries) |entry| {
            if (entry.id_ptr != null and entry.id_len > 0) {
                std.heap.c_allocator.free(entry.id_ptr.?[0..entry.id_len]);
            }
        }
        std.heap.c_allocator.free(entries);
    }
    result.* = .{};
}

pub export fn antfly_db_begin_transaction_with_id(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    timestamp_ns: u64,
    participants_ptr: ?[*]const capi.Slice,
    participant_count: usize,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return .invalid_argument;
    beginWithIdAndParticipants(handle, txn_id.*, timestamp_ns, participants_ptr, participant_count) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_write_transaction(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return .invalid_argument;
    if ((write_count > 0 and writes_ptr == null) or (predicate_count > 0 and predicates_ptr == null)) return .invalid_argument;
    writeIntentsInternal(handle, txn_id.*, writes_ptr, write_count, predicates_ptr, predicate_count) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_batch(
    handle_ptr: ?*anyopaque,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
    timestamp_ns: u64,
    sync_level: u8,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if ((write_count > 0 and writes_ptr == null) or (predicate_count > 0 and predicates_ptr == null)) return .invalid_argument;
    batchInternal(handle, writes_ptr, write_count, predicates_ptr, predicate_count, timestamp_ns, sync_level) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_batch_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    var owned = batch_api.parseBatchRequest(handle.alloc, request_json.bytes()) catch |err| return capi.mapError(err);
    defer owned.deinit(handle.alloc);

    handle.db.batch(owned.req) catch |err| return capi.mapError(err);
    const response = batch_api.encodeBatchResponse(std.heap.c_allocator, owned.result()) catch |err| return capi.mapError(err);
    out_buf.* = .{
        .ptr = response.ptr,
        .len = response.len,
    };
    return .ok;
}

pub export fn antfly_db_resolve_intents(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    status: u8,
    commit_version: u64,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return .invalid_argument;
    const txn_status: transactions_mod.TxnStatus = switch (status) {
        0 => .pending,
        1 => .committed,
        2 => .aborted,
        else => return .invalid_argument,
    };
    handle.db.resolveTransactionIntents(txn_id.*, txn_status, commit_version) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_transaction_status(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    out_status: ?*u8,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const out = out_status orelse return .invalid_argument;
    out.* = 0;
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return .invalid_argument;
    const status = handle.db.getTransactionStatus(txn_id.*) catch |err| return capi.mapError(err);
    out.* = @intFromEnum(status);
    return .ok;
}

pub export fn antfly_db_get_commit_version(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    out_commit_version: ?*u64,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const out = out_commit_version orelse return .invalid_argument;
    out.* = 0;
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return .invalid_argument;
    out.* = handle.db.getCommitVersion(txn_id.*) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_timestamp(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_timestamp: *u64,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    out_timestamp.* = handle.db.getTimestamp(handle.alloc, key.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_lookup_json(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.prepareLookupRequest(key.bytes(), .{}) catch |err| return capi.mapError(err);
    const result = handle.db.getDocument(handle.alloc, key.bytes(), .{}) catch |err| return capi.mapError(err);
    if (result == null) return .not_found;
    out_buf.* = .{
        .ptr = result.?.json.ptr,
        .len = result.?.json.len,
    };
    return .ok;
}

pub export fn antfly_db_get_raw(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const result = handle.db.get(handle.alloc, key.bytes()) catch |err| return capi.mapError(err);
    if (result == null) return .not_found;
    out_buf.* = .{
        .ptr = result.?.ptr,
        .len = result.?.len,
    };
    return .ok;
}

pub export fn antfly_db_lookup_artifact_json(
    handle_ptr: ?*anyopaque,
    artifact_id_b64: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.prepareLookupRequest(artifact_id_b64.bytes(), .{}) catch |err| return capi.mapError(err);
    const artifact_id = decodeBase64Alloc(handle.alloc, artifact_id_b64.bytes()) catch return .invalid_argument;
    defer handle.alloc.free(artifact_id);

    var record = handle.db.getPublicArtifact(handle.alloc, artifact_id) catch |err| return capi.mapError(err);
    if (record == null) return .not_found;
    defer record.?.deinit(handle.alloc);

    var payload = JsonArtifactWrite.init(handle.alloc, record.?) catch return .internal;
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_decode_artifact_id_json(
    artifact_id_b64: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const alloc = std.heap.c_allocator;
    const artifact_id = decodeBase64Alloc(alloc, artifact_id_b64.bytes()) catch return .invalid_argument;
    defer alloc.free(artifact_id);

    var artifact_ref = (db_mod.artifact_ids.decodeArtifactPublicIdAlloc(alloc, artifact_id) catch return .invalid_argument) orelse return .invalid_argument;
    defer artifact_ref.deinit(alloc);

    var payload = JsonArtifactRef.init(alloc, artifact_ref) catch return .internal;
    defer payload.deinit(alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_get_schema_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (handle.db.getSchemaJson(handle.alloc) catch |err| return capi.mapError(err)) |schema_json| {
        out_buf.* = .{ .ptr = schema_json.ptr, .len = schema_json.len };
    } else {
        out_buf.* = dupBytes("null") catch return .internal;
    }
    return .ok;
}

pub export fn antfly_db_set_schema_json(
    handle_ptr: ?*anyopaque,
    schema_json: capi.Slice,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.setSchemaJson(handle.alloc, schema_json.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_run_until_idle(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .maintain) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.runUntilIdle() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_run_until_idle_json(
    handle_ptr: ?*anyopaque,
    out_buf_ptr: ?*capi.Buffer,
) capi.ErrorCode {
    const out_buf = resetOutBuffer(out_buf_ptr) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .maintain) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.runUntilIdle() catch |err| {
        writeRunUntilIdleNoProgressDiagnosticIfAny(&handle.db, out_buf, err);
        return capi.mapError(err);
    };
    out_buf.* = stringifyJson(handle.db.pendingWorkStats()) catch return .internal;
    return .ok;
}

/// On `error.RunUntilIdleNoProgress` (mapped to `capi.ErrorCode.stalled`),
/// best-effort populate `out_buf` with the exact stuck index name and its
/// indexed/expected counters (see `DB.NoProgressDiagnostic`) instead of
/// leaving callers with only the non-descriptive status code. Any other
/// error leaves `out_buf` untouched, matching every other failure path here.
fn writeRunUntilIdleNoProgressDiagnosticIfAny(db: anytype, out_buf: *capi.Buffer, err: anyerror) void {
    if (err != error.RunUntilIdleNoProgress) return;
    const diagnostic = db.lastRunUntilIdleNoProgressDiagnostic() orelse return;
    out_buf.* = stringifyJson(.{
        .index_name = diagnostic.index_name,
        .indexed = diagnostic.indexed,
        .expected = diagnostic.expected,
        .stuck_ms = diagnostic.stuck_ns / std.time.ns_per_ms,
    }) catch return;
}

pub export fn antfly_db_pending_work_stats_json(
    handle_ptr: ?*anyopaque,
    out_buf_ptr: ?*capi.Buffer,
) capi.ErrorCode {
    const out_buf = resetOutBuffer(out_buf_ptr) orelse return .invalid_argument;
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    out_buf.* = stringifyJson(handle.db.pendingWorkStats()) catch return .internal;
    return .ok;
}

fn antflyDbExtractEnrichmentsJson(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const writes = decodeBatchWritesRequest(handle.alloc, request_json.bytes()) catch return .invalid_argument;
    defer freeOwnedBatchWrites(handle.alloc, writes);

    var result = handle.db.extractEnrichments(handle.alloc, writes) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var payload = buildJsonExtractEnrichmentsResult(handle.alloc, result) catch return .internal;
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

fn antflyDbComputeEnrichmentsJson(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const writes = decodeBatchWritesRequest(handle.alloc, request_json.bytes()) catch return .invalid_argument;
    defer freeOwnedBatchWrites(handle.alloc, writes);

    var result = handle.db.computeEnrichments(handle.alloc, writes) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var payload = buildJsonComputeEnrichmentsResult(handle.alloc, result) catch return .internal;
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

comptime {
    @export(&antflyDbExtractEnrichmentsJson, .{
        .name = "antfly_db_extract_enrichments_json",
        .linkage = .strong,
    });
    @export(&antflyDbComputeEnrichmentsJson, .{
        .name = "antfly_db_compute_enrichments_json",
        .linkage = .strong,
    });
}

pub export fn antfly_db_update_range(
    handle_ptr: ?*anyopaque,
    start: capi.Slice,
    end: capi.Slice,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.updateRange(.{
        .start = start.bytes(),
        .end = end.bytes(),
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_range_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    var payload = JsonRange.init(handle.alloc, handle.db.getRange()) catch return .internal;
    defer payload.deinit(handle.alloc);
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_get_split_state_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const state = handle.db.getSplitState(handle.alloc) catch |err| return capi.mapError(err);
    if (state == null) return .not_found;
    var payload = JsonSplitState.init(handle.alloc, state.?) catch return .internal;
    defer payload.deinit(handle.alloc);
    defer db_mod.types.freeSplitState(handle.alloc, state);
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_set_split_state_json(
    handle_ptr: ?*anyopaque,
    state_json: capi.Slice,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const ParsedState = struct {
        phase: u8,
        split_key_b64: []const u8,
        new_shard_id: u64,
        started_at: u64,
        original_range_end_b64: []const u8,
    };
    var parsed = std.json.parseFromSlice(ParsedState, handle.alloc, state_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const split_key = decodeBase64Alloc(handle.alloc, parsed.value.split_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(split_key);
    const original_range_end = decodeBase64Alloc(handle.alloc, parsed.value.original_range_end_b64) catch return .invalid_argument;
    defer handle.alloc.free(original_range_end);
    const phase: db_mod.types.SplitPhase = switch (parsed.value.phase) {
        0 => .none,
        1 => .prepare,
        2 => .splitting,
        3 => .finalizing,
        4 => .rolling_back,
        else => return .invalid_argument,
    };
    handle.db.setSplitState(.{
        .phase = phase,
        .split_key = split_key,
        .new_shard_id = parsed.value.new_shard_id,
        .started_at = parsed.value.started_at,
        .original_range_end = original_range_end,
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_clear_split_state(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.clearSplitState() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_split_delta_seq(
    handle_ptr: ?*anyopaque,
    out_seq: *u64,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    out_seq.* = handle.db.getSplitDeltaSeq();
    return .ok;
}

pub export fn antfly_db_get_split_delta_final_seq(
    handle_ptr: ?*anyopaque,
    out_seq: *u64,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    out_seq.* = handle.db.getSplitDeltaFinalSeq(handle.alloc) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_set_split_delta_final_seq(
    handle_ptr: ?*anyopaque,
    seq: u64,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.setSplitDeltaFinalSeq(seq) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_clear_split_delta_final_seq(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.clearSplitDeltaFinalSeq() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_list_split_delta_entries_after_json(
    handle_ptr: ?*anyopaque,
    after_seq: u64,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const entries = handle.db.listSplitDeltaEntriesAfter(handle.alloc, after_seq) catch |err| return capi.mapError(err);
    defer db_mod.types.freeSplitDeltaEntries(handle.alloc, entries);

    var payload = handle.alloc.alloc(JsonSplitDeltaEntry, entries.len) catch return .internal;
    var payload_count: usize = 0;
    defer {
        for (payload[0..payload_count]) |*entry| entry.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (entries, 0..) |entry, i| {
        payload[i] = JsonSplitDeltaEntry.init(handle.alloc, entry) catch return .internal;
        payload_count += 1;
    }

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_clear_split_delta_entries(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.clearSplitDeltaEntries() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_list_indexes_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const configs = handle.db.listIndexes(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeIndexConfigs(handle.alloc, configs);

    var payload = handle.alloc.alloc(JsonIndexConfig, configs.len) catch return .internal;
    defer handle.alloc.free(payload);
    for (configs, 0..) |cfg, i| {
        payload[i] = .{
            .name = cfg.name,
            .kind = @tagName(cfg.kind),
            .config_json = cfg.config_json,
        };
    }

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_list_enrichments_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const configs = handle.db.listEnrichments(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeEnrichmentConfigs(handle.alloc, configs);
    out_buf.* = stringifyJson(configs) catch return .internal;
    return .ok;
}

pub export fn antfly_db_scan_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        from_key_b64: []const u8 = "",
        to_key_b64: []const u8 = "",
        inclusive_from: bool = false,
        exclusive_to: bool = false,
        include_documents: bool = false,
        limit: u32 = 0,
        fields: []const []const u8 = &.{},
        include_all_fields: bool = true,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .ignore_unknown_fields = true }) catch return .invalid_argument;
    defer parsed.deinit();

    const from_key = decodeBase64Alloc(handle.alloc, parsed.value.from_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(from_key);
    const to_key = decodeBase64Alloc(handle.alloc, parsed.value.to_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(to_key);
    const opts: db_mod.types.ScanOptions = .{
        .inclusive_from = parsed.value.inclusive_from,
        .exclusive_to = parsed.value.exclusive_to,
        .include_documents = parsed.value.include_documents,
        .limit = parsed.value.limit,
        .fields = parsed.value.fields,
        .include_all_fields = parsed.value.include_all_fields,
    };
    handle.prepareScanRequest(from_key, to_key, opts) catch |err| return capi.mapError(err);
    var result = handle.db.scan(handle.alloc, from_key, to_key, opts) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var hashes = handle.alloc.alloc(JsonScanHash, result.hashes.len) catch return .internal;
    var hash_count: usize = 0;
    defer {
        for (hashes[0..hash_count]) |*item| item.deinit(handle.alloc);
        if (hashes.len > 0) handle.alloc.free(hashes);
    }
    for (result.hashes, 0..) |item, i| {
        hashes[i] = JsonScanHash.init(handle.alloc, item) catch return .internal;
        hash_count += 1;
    }

    var documents = handle.alloc.alloc(JsonScanDocument, result.documents.len) catch return .internal;
    var document_count: usize = 0;
    defer {
        for (documents[0..document_count]) |*item| item.deinit(handle.alloc);
        if (documents.len > 0) handle.alloc.free(documents);
    }
    for (result.documents, 0..) |item, i| {
        documents[i] = JsonScanDocument.init(handle.alloc, item) catch return .internal;
        document_count += 1;
    }

    out_buf.* = stringifyJson(JsonScanResult{
        .hashes = hashes,
        .documents = documents,
    }) catch return .internal;
    return .ok;
}

pub export fn antfly_db_scan_hashes(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_result: *capi.ScanHashResult,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        from_key_b64: []const u8 = "",
        to_key_b64: []const u8 = "",
        inclusive_from: bool = false,
        exclusive_to: bool = false,
        limit: u32 = 0,
        fields: []const []const u8 = &.{},
        include_all_fields: bool = true,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .ignore_unknown_fields = true }) catch return .invalid_argument;
    defer parsed.deinit();

    const from_key = decodeBase64Alloc(handle.alloc, parsed.value.from_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(from_key);
    const to_key = decodeBase64Alloc(handle.alloc, parsed.value.to_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(to_key);
    const opts: db_mod.types.ScanOptions = .{
        .inclusive_from = parsed.value.inclusive_from,
        .exclusive_to = parsed.value.exclusive_to,
        .include_documents = false,
        .limit = parsed.value.limit,
        .fields = parsed.value.fields,
        .include_all_fields = parsed.value.include_all_fields,
    };
    handle.prepareScanRequest(from_key, to_key, opts) catch |err| return capi.mapError(err);
    var result = handle.db.scan(handle.alloc, from_key, to_key, opts) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    const entries = std.heap.c_allocator.alloc(capi.ScanHashEntry, result.hashes.len) catch return .internal;
    errdefer std.heap.c_allocator.free(entries);
    for (result.hashes, 0..) |item, i| {
        const id = std.heap.c_allocator.alloc(u8, item.id.len) catch {
            for (entries[0..i]) |entry| {
                if (entry.id_ptr != null and entry.id_len > 0) {
                    std.heap.c_allocator.free(entry.id_ptr.?[0..entry.id_len]);
                }
            }
            return .internal;
        };
        @memcpy(id, item.id);
        entries[i] = .{
            .id_ptr = id.ptr,
            .id_len = id.len,
            .hash = item.hash,
        };
    }

    out_result.* = .{
        .entries_ptr = entries.ptr,
        .entry_count = entries.len,
    };
    return .ok;
}

pub export fn antfly_db_stats_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const bytes = dbStatsJsonAlloc(handle) catch |err| return capi.mapError(err);
    out_buf.* = .{ .ptr = bytes.ptr, .len = bytes.len };
    return .ok;
}

fn dbStatsJsonAlloc(handle: *Handle) ![]u8 {
    const stats = try handle.db.stats(handle.alloc);
    defer db_mod.types.freeDBStats(handle.alloc, stats);

    const indexes = try dbIndexStatsProjectionAlloc(handle.alloc, stats);
    defer if (indexes.len > 0) handle.alloc.free(indexes);

    return try std.fmt.allocPrint(handle.alloc, "{f}", .{std.json.fmt(jsonDBStatsProjection(stats, indexes), .{})});
}

fn dbIndexStatsProjectionAlloc(alloc: Allocator, stats: db_mod.types.DBStats) ![]JsonDBIndexStats {
    var indexes = try alloc.alloc(JsonDBIndexStats, stats.indexes.len);
    for (stats.indexes, 0..) |item, i| {
        indexes[i] = .{
            .name = item.name,
            .kind = @tagName(item.kind),
            .doc_count = item.doc_count,
            .term_count = item.term_count,
            .edge_count = item.edge_count,
            .graph_counts_pending = item.graph_counts_pending,
            .node_count = item.node_count,
            .repair_degraded = item.repair_degraded,
            .repair_issue_count = item.repair_issue_count,
            .repair_summary_ready = item.repair_summary_ready,
            .repair_issue_count_estimated = item.repair_issue_count_estimated,
            .coverage_produced_count = item.coverage_produced_count,
            .coverage_skipped_count = item.coverage_skipped_count,
            .coverage_terminal_failed_count = item.coverage_terminal_failed_count,
            .coverage_summary_ready = item.coverage_summary_ready,
        };
    }
    return indexes;
}

fn jsonDBStatsProjection(stats: db_mod.types.DBStats, indexes: []JsonDBIndexStats) JsonDBStats {
    return JsonDBStats{
        .doc_count = stats.doc_count,
        .index_count = stats.index_count,
        .indexes = indexes,
        .repair_degraded = stats.repair_degraded,
        .repair_issue_count = stats.repair_issue_count,
        .repair_summary_ready = stats.repair_summary_ready,
        .repair_issue_count_estimated = stats.repair_issue_count_estimated,
        .enrichment = .{
            .enabled = stats.enrichment.enabled,
            .lease_owned = stats.enrichment.lease_owned,
            .has_lease = stats.enrichment.has_lease,
            .acquisition_count = stats.enrichment.acquisition_count,
            .lease_acquire_failures = stats.enrichment.lease_acquire_failures,
            .lost_leases = stats.enrichment.lost_leases,
            .last_acquired_ms = stats.enrichment.last_acquired_ms,
            .target_sequence = stats.enrichment.target_sequence,
            .applied_sequence = stats.enrichment.applied_sequence,
            .processed_requests = stats.enrichment.processed_requests,
            .error_count = stats.enrichment.error_count,
            .retryable_error_count = stats.enrichment.retryable_error_count,
            .fatal_error_count = stats.enrichment.fatal_error_count,
            .retrying = stats.enrichment.retrying,
            .worker_failed = stats.enrichment.worker_failed,
            .stalled = stats.enrichment.stalled,
            .stall_reason = stats.enrichment.stall_reason,
            .skip_by_hash_count = stats.enrichment.skip_by_hash_count,
            .skipped_source_count = stats.enrichment.skipped_source_count,
            .codec_decode_failures = stats.enrichment.codec_decode_failures,
            .dense_artifact_bytes_written = stats.enrichment.dense_artifact_bytes_written,
            .sparse_artifact_bytes_written = stats.enrichment.sparse_artifact_bytes_written,
            .chunk_artifact_bytes_written = stats.enrichment.chunk_artifact_bytes_written,
            .artifact_bytes_written = stats.enrichment.artifact_bytes_written,
        },
        .ttl_cleanup = .{
            .enabled = stats.ttl_cleanup.enabled,
            .lease_owned = stats.ttl_cleanup.lease_owned,
            .has_lease = stats.ttl_cleanup.has_lease,
            .acquisition_count = stats.ttl_cleanup.acquisition_count,
            .runs = stats.ttl_cleanup.runs,
            .scanned_timestamps = stats.ttl_cleanup.scanned_timestamps,
            .deleted_docs = stats.ttl_cleanup.deleted_docs,
            .last_run_ns = stats.ttl_cleanup.last_run_ns,
            .error_count = stats.ttl_cleanup.error_count,
            .lease_acquire_failures = stats.ttl_cleanup.lease_acquire_failures,
            .lost_leases = stats.ttl_cleanup.lost_leases,
            .last_acquired_ms = stats.ttl_cleanup.last_acquired_ms,
        },
        .transaction_recovery = .{
            .enabled = stats.transaction_recovery.enabled,
            .lease_owned = stats.transaction_recovery.lease_owned,
            .has_lease = stats.transaction_recovery.has_lease,
            .acquisition_count = stats.transaction_recovery.acquisition_count,
            .lease_acquire_failures = stats.transaction_recovery.lease_acquire_failures,
            .lost_leases = stats.transaction_recovery.lost_leases,
            .last_acquired_ms = stats.transaction_recovery.last_acquired_ms,
            .runs = stats.transaction_recovery.runs,
            .scanned_records = stats.transaction_recovery.scanned_records,
            .auto_aborted = stats.transaction_recovery.auto_aborted,
            .resolved_finalized = stats.transaction_recovery.resolved_finalized,
            .cleaned_records = stats.transaction_recovery.cleaned_records,
            .kept_recent_pending = stats.transaction_recovery.kept_recent_pending,
            .deferred_unresolved = stats.transaction_recovery.deferred_unresolved,
            .notification_attempts = stats.transaction_recovery.notification_attempts,
            .notification_successes = stats.transaction_recovery.notification_successes,
            .notification_failures = stats.transaction_recovery.notification_failures,
            .last_run_ns = stats.transaction_recovery.last_run_ns,
            .error_count = stats.transaction_recovery.error_count,
        },
        .text_merge = .{
            .enabled = stats.text_merge.enabled,
            .active_indexes = stats.text_merge.active_indexes,
            .active_segments = stats.text_merge.active_segments,
            .max_active_segments_per_index = stats.text_merge.max_active_segments_per_index,
            .pending_indexes = stats.text_merge.pending_indexes,
            .pending_segments = stats.text_merge.pending_segments,
            .pending_bytes = stats.text_merge.pending_bytes,
            .in_flight_merges = stats.text_merge.in_flight_merges,
            .in_flight_segments = stats.text_merge.in_flight_segments,
            .completed_merges = stats.text_merge.completed_merges,
            .skipped_stale_merges = stats.text_merge.skipped_stale_merges,
            .failed_merges = stats.text_merge.failed_merges,
            .quarantined_merges = stats.text_merge.quarantined_merges,
            .quarantined_segments = stats.text_merge.quarantined_segments,
            .last_merge_error = stats.text_merge.last_merge_error,
            .backpressure_events = stats.text_merge.backpressure_events,
            .backpressure_ns = stats.text_merge.backpressure_ns,
            .max_pending_segments = stats.text_merge.max_pending_segments,
            .max_pending_bytes = stats.text_merge.max_pending_bytes,
        },
        .term_doc_freq_cache_hits = stats.term_doc_freq_cache_hits,
        .term_doc_freq_cache_misses = stats.term_doc_freq_cache_misses,
    };
}

fn requestLooksLikePublicQueryJson(bytes: []const u8) bool {
    return std.mem.indexOf(u8, bytes, "\"full_text_search\"") != null or
        std.mem.indexOf(u8, bytes, "\"embeddings\"") != null or
        std.mem.indexOf(u8, bytes, "\"graph_queries\"") != null or
        std.mem.indexOf(u8, bytes, "\"merge_config\"") != null or
        std.mem.indexOf(u8, bytes, "\"indexes\"") != null or
        std.mem.indexOf(u8, bytes, "\"query\"") != null;
}

fn searchStorageKernelQueryJson(
    handle: *Handle,
    table_name: []const u8,
    request: *const kernel_owner_abi.QueryOperationRequest,
    out_response: *kernel_owner_abi.QueryOwnedResponse,
    out_failure: *kernel_owner_abi.FailureIdentity,
) kernel_owner_abi.Status {
    out_failure.* = .{};
    const request_json: capi.Slice = .{ .ptr = request.control.request_json.ptr, .len = @intCast(request.control.request_json.len) };
    const controls: db_mod.types.SearchRequest = .{
        .execution_deadline_ns = if (request.control.has_execution_deadline != 0) request.control.execution_deadline_ns else null,
        .cancellation = ownerQueryCancellation(&request.control),
    };
    table_reads_api.checkQueryDeadline(controls) catch |err|
        return storageOwnerQueryFailure(err, .execute_internal_query, out_failure);
    if (comptime capi_build_options.linked_storage) {
        handle.prepareSearchRequest(controls) catch |err|
            return storageOwnerQueryFailure(err, .execute_internal_query, out_failure);
        const response = local_query_client.executeJsonAlloc(
            std.heap.c_allocator,
            @ptrCast(&handle.db),
            table_name,
            request_json.bytes(),
            .internal,
            request.execution_options,
            controls.execution_deadline_ns,
            request.control.cancellation_ctx,
            request.control.cancellation_fn,
            out_failure,
        ) catch |err| {
            // A valid provider failure already carries the exact envelope.
            // Consumer-side allocation/protocol failures originate in this
            // outer operation and receive their own identity.
            if (out_failure.status != .ok) return out_failure.status;
            return storageOwnerQueryFailure(err, .encode_internal_response, out_failure);
        };
        out_response.* = .{
            .buffer = .{ .ptr = response.json.ptr, .len = @intCast(response.json.len) },
            .identity_read_generation = response.identity_read_generation orelse 0,
            .has_identity_read_generation = @intFromBool(response.identity_read_generation != null),
        };
        return .ok;
    }

    // The distributed adapter sends the same resolved/internal query dialect
    // used by remote group routes. It must not be reparsed as a public request:
    // fields such as `_filter_query_json` are deliberately internal.
    var owned = query_api.parseQueryRequest(
        handle.alloc,
        null,
        table_name,
        request_json.bytes(),
    ) catch |err| return storageOwnerQueryFailure(err, .parse_internal_request, out_failure);
    defer owned.deinit(handle.alloc);
    if (controls.execution_deadline_ns) |deadline| {
        owned.req.execution_deadline_ns = if (owned.req.execution_deadline_ns) |parsed| @min(parsed, deadline) else deadline;
    }
    owned.req.cancellation = controls.cancellation;
    antfly.local_query_controls.applyExecutionOptions(&owned.req, request.execution_options);

    stampSearchRequestIdentityGeneration(handle, &owned.req) catch |err|
        return storageOwnerQueryFailure(err, .execute_internal_query, out_failure);
    handle.prepareSearchRequest(owned.req) catch |err|
        return storageOwnerQueryFailure(err, .execute_internal_query, out_failure);

    var result = handle.db.search(handle.alloc, owned.req) catch |err|
        return storageOwnerQueryFailure(err, .execute_internal_query, out_failure);
    defer result.deinit();
    table_reads_api.checkQueryDeadline(owned.req) catch |err|
        return storageOwnerQueryFailure(err, .execute_internal_query, out_failure);

    var response = query_api.encodeQueryResponses(
        handle.alloc,
        table_name,
        owned.req,
        .{},
        result,
    ) catch |err| return storageOwnerQueryFailure(err, .encode_internal_response, out_failure);
    defer response.deinit(handle.alloc);
    table_reads_api.checkQueryDeadline(owned.req) catch |err|
        return storageOwnerQueryFailure(err, .execute_internal_query, out_failure);

    const buffer = dupBytes(response.json) catch |err|
        return storageOwnerQueryFailure(err, .encode_internal_response, out_failure);
    out_response.* = .{
        .buffer = .{ .ptr = buffer.ptr, .len = @intCast(buffer.len) },
        .identity_read_generation = response.identity_read_generation orelse 0,
        .has_identity_read_generation = @intFromBool(response.identity_read_generation != null),
    };
    return .ok;
}

fn ownerQueryCancellation(request: *const kernel_owner_abi.ControlledJsonOperationRequest) db_mod.types.CancellationToken {
    if (request.cancellation_fn == null) return .none;
    return .{ .ptr = request, .is_cancelled_fn = struct {
        fn requested(ptr: *const anyopaque) bool {
            const control: *const kernel_owner_abi.ControlledJsonOperationRequest = @ptrCast(@alignCast(ptr));
            return control.cancellation_fn.?(control.cancellation_ctx) != 0;
        }
    }.requested };
}

fn storageOwnerQueryFailure(
    err: anyerror,
    operation: kernel_owner_abi.LocalQueryOperation,
    out_failure: *kernel_owner_abi.FailureIdentity,
) kernel_owner_abi.Status {
    out_failure.* = kernel_error_identity.failureFromError(
        err,
        .storage_owner,
        kernel_owner_abi.abi_version,
        @intFromEnum(operation),
    );
    return out_failure.status;
}

/// Resolves a public query's `semantic_search` text into a dense query
/// vector for a native Lite handle, the same way the server does for a
/// managed table: embed the text through the index's own configured
/// embedder, using the retrieval-query task/instruction (Qwen3's built-in
/// query instruction, for example) rather than the document-indexing task.
/// Mirrors `http_server.zig`'s `SemanticStatusResolver`, but against Lite's
/// index list instead of a table's admin snapshot, and constructs a
/// throwaway `ManagedEmbedder` per call instead of reusing one cached on the
/// enrichment runtime: Lite queries are not expected at a rate where that
/// matters, and every other Lite embedder call site (`refreshLite...`,
/// `litePhysicalIndexConfigJson`) already does the same thing.
const LiteSemanticResolver = struct {
    handle: *Handle,

    fn resolveDenseQuery(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) anyerror!db_mod.types.DenseKnnQuery {
        _ = table_name;
        if (embedding_template != null) return error.UnsupportedQueryRequest;
        const self: *LiteSemanticResolver = @ptrCast(@alignCast(ptr));
        const handle = self.handle;
        const merged_json = try liteMergedIndexesJsonAlloc(handle);
        defer handle.alloc.free(merged_json);
        var managed = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(
            handle.alloc,
            merged_json,
            .{ .antfly_provider = handle.liteAntflyProvider() },
        );
        defer managed.deinit();
        const vector = try managed.embedQuery(alloc, index_name, semantic_search);
        return .{ .vector = vector, .k = limit };
    }

    fn resolver(self: *LiteSemanticResolver) query_api.SemanticResolver {
        return .{
            .ptr = self,
            .vtable = &.{ .resolve_dense_query = resolveDenseQuery },
        };
    }
};

fn searchPublicQueryJson(
    handle: *Handle,
    table_name: []const u8,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    // `linked_storage` says this compiled library links the full storage
    // internals -- true unconditionally for the default `libantfly` since
    // it is shared with the `antfly` executable -- not that `handle` is a
    // genuine storage-owner/distributed-table handle. A Lite handle
    // (`owned_lite_backend != null`) has no metadata/table catalog for
    // `local_query_client`'s internal semantic-search resolution to look an
    // index's embedder up in, so it always takes the simpler path below,
    // which resolves `semantic_search` itself via `LiteSemanticResolver`.
    if (comptime capi_build_options.linked_storage) {
        if (handle.owned_lite_backend == null) {
            handle.prepareSearchRequest(.{}) catch |err| return capi.mapError(err);
            var failure: kernel_owner_abi.FailureIdentity = .{};
            const response = local_query_client.executeJsonAlloc(
                std.heap.c_allocator,
                @ptrCast(&handle.db),
                table_name,
                request_json.bytes(),
                .public,
                .{},
                null,
                null,
                null,
                &failure,
            ) catch |err| return capi.mapError(err);
            out_buf.* = .{ .ptr = response.json.ptr, .len = response.json.len };
            return .ok;
        }
    }

    var lite_semantic_resolver = LiteSemanticResolver{ .handle = handle };
    const semantic_resolver: ?query_api.SemanticResolver = if (handle.lite_profile == .native)
        lite_semantic_resolver.resolver()
    else
        null;
    var owned = query_api.parsePublicQueryRequest(
        handle.alloc,
        semantic_resolver,
        table_name,
        request_json.bytes(),
    ) catch |err| return capi.mapError(err);
    defer owned.deinit(handle.alloc);

    const DbSearchQuery = struct {
        const Result = db_mod.types.SearchResult;
        fn run(_: @This(), h: *Handle, req: db_mod.types.SearchRequest) !Result {
            return h.db.search(h.alloc, req);
        }
    };
    var result = runAtStampedGeneration(handle, &owned.req, DbSearchQuery{}) catch |err| return capi.mapError(err);
    defer result.deinit();

    var response = query_api.encodeQueryResponses(
        handle.alloc,
        table_name,
        owned.req,
        .{},
        result,
    ) catch |err| return capi.mapError(err);
    defer response.deinit(handle.alloc);

    out_buf.* = dupBytes(response.json) catch return .internal;
    return .ok;
}

/// SQL over one explicitly named embedded table. No metadata catalog or remote
/// coordinator is invented by this single-handle ABI. Output is always freed by
/// antfly_buffer_free, including structured SQL diagnostics on failure.
pub export fn antfly_db_sql_json(handle_ptr: ?*anyopaque, table_name: capi.Slice, request_json: capi.Slice, out_buf: *capi.Buffer) capi.ErrorCode {
    out_buf.* = .{};
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    if (table_name.bytes().len == 0 or table_name.bytes().len > 1024 or request_json.bytes().len > 2 * 1024 * 1024) return .invalid_argument;
    // Managed owners require Raft routing and credentials supplied by API SQL.
    if (handle.storage_owner_context != null or handle.storage_owner_path != null or handle.storage_owner_group_id != 0 or handle.readable_lease_hook != null) return .unsupported;
    executeEmbeddedSql(handle, table_name.bytes(), request_json.bytes(), out_buf) catch |err| {
        const diagnostic = antfly.capi_dependencies.sql_errors.describe(err);
        if (out_buf.ptr == null) out_buf.* = stringifyJson(.{ .@"error" = diagnostic }) catch return .internal;
        if (std.mem.eql(u8, diagnostic.code, "40003")) return .outcome_unknown;
        if (std.mem.eql(u8, diagnostic.code, "0A000")) return .unsupported;
        if (std.mem.eql(u8, diagnostic.code, "40001")) return .version_conflict;
        if (std.mem.eql(u8, diagnostic.code, "XX000") or std.mem.eql(u8, diagnostic.code, "53200")) return .internal;
        return .invalid_argument;
    };
    return .ok;
}

fn executeEmbeddedSql(handle: *Handle, table_name: []const u8, request_json: []const u8, out_buf: *capi.Buffer) !void {
    const sql = @import("sql.zig");
    const Budget = antfly.capi_dependencies.sql_memory_budget;
    var preparation_budget = Budget{ .backing = handle.alloc, .limit = 8 * 1024 * 1024 };
    const temporary = preparation_budget.allocator();
    const Request = struct {
        statement: []const u8,
        parameters: []const std.json.Value = &.{},
        limit: usize = 128,
        session_id: ?[]const u8 = null,
        database: ?[]const u8 = null,
        namespace: ?[]const u8 = null,
    };
    var parsed = std.json.parseFromSlice(Request, temporary, request_json, .{ .allocate = .alloc_always }) catch |err| {
        if (err == error.OutOfMemory and preparation_budget.exhausted) return error.SqlProgramLimitExceeded;
        return error.InvalidSqlParameters;
    };
    defer parsed.deinit();
    if (parsed.value.session_id != null or parsed.value.database != null or parsed.value.namespace != null) return error.UnsupportedSqlExecution;
    var compiled = sql.compiler.compile(temporary, parsed.value.statement, .{}) catch |err| {
        if (err == error.OutOfMemory and preparation_budget.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    defer compiled.deinit();
    // Reserve a bounded C-owned receipt before a write can commit. Neither
    // response encoding nor the final ABI copy may erase a known commit when
    // allocation fails afterwards. JSON trailing whitespace preserves the
    // full allocation length for the caller's buffer-free contract.
    var commit_receipt: ?[]u8 = switch (compiled.statement) {
        .insert, .update, .delete => try std.heap.c_allocator.alloc(u8, 512),
        else => null,
    };
    defer if (commit_receipt) |buffer| std.heap.c_allocator.free(buffer);
    var adapter = sql.Adapter(antfly){ .db = &handle.db, .table_name = table_name, .read_only = handle.open_mode != .writer };
    var result = sql.runtime.execute(handle.alloc, adapter.backend(), &compiled, parsed.value.parameters, .{ .result_rows = parsed.value.limit }) catch |err| {
        if (err == error.SqlMutationOutcomeUnknown) if (adapter.outcome_transaction_id) |txn_id| {
            out_buf.* = embeddedSqlUnknownReceipt(&commit_receipt, txn_id);
        };
        return err;
    };
    defer result.deinit();
    var encoding_budget = Budget{ .backing = handle.alloc, .limit = 16 * 1024 * 1024 };
    const bytes = std.json.Stringify.valueAlloc(encoding_budget.allocator(), result.output, .{}) catch |err| {
        if (result.output.mutation_outcome != null) {
            // Encoding failure after commit must never invite a mutation retry.
            // Preserve the authoritative commit outcome in a bounded envelope.
            out_buf.* = embeddedSqlCommitReceipt(&commit_receipt, result.output);
            return;
        }
        if (err == error.OutOfMemory and encoding_budget.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    defer encoding_budget.allocator().free(bytes);
    out_buf.* = dupBytes(bytes) catch |err| {
        if (result.output.mutation_outcome != null) {
            out_buf.* = embeddedSqlCommitReceipt(&commit_receipt, result.output);
            return;
        }
        return err;
    };
}

fn embeddedSqlUnknownReceipt(reserved: *?[]u8, txn_id: db_mod.types.TxnId) capi.Buffer {
    const buffer = reserved.*.?;
    @memset(buffer, ' ');
    const txn_hex = std.fmt.bytesToHex(txn_id, .lower);
    _ = std.fmt.bufPrint(buffer, "{f}", .{std.json.fmt(.{
        .transaction_id = @as([]const u8, &txn_hex),
        .@"error" = .{ .code = "40003", .message = "The mutation outcome is unknown. Reconcile the native transaction receipt; do not replay the statement.", .retryable = false },
    }, .{})}) catch unreachable;
    reserved.* = null;
    return .{ .ptr = buffer.ptr, .len = buffer.len };
}

fn embeddedSqlCommitReceipt(reserved: *?[]u8, output: antfly.capi_dependencies.sql_runtime.Output) capi.Buffer {
    const buffer = reserved.*.?;
    @memset(buffer, ' ');
    // Only runtime-owned INSERT/UPDATE/DELETE tags and bounded enum/integer
    // fields enter this envelope; 512 bytes exceeds their maximum encoding.
    _ = std.fmt.bufPrint(buffer, "{f}", .{std.json.fmt(.{
        .mutation_outcome = output.mutation_outcome.?,
        .rows_affected = output.rows_affected,
        .command_tag = output.command_tag,
        .@"error" = .{ .code = "53200", .message = "The mutation committed but its full response could not be allocated.", .retryable = false },
    }, .{})}) catch unreachable;
    reserved.* = null;
    return .{ .ptr = buffer.ptr, .len = buffer.len };
}

test "capi SQL reserved receipt preserves every known mutation outcome without allocation" {
    inline for (std.meta.tags(antfly.capi_dependencies.sql_catalog.MutationOutcome)) |outcome| {
        var reserved: ?[]u8 = try std.heap.c_allocator.alloc(u8, 512);
        defer if (reserved) |buffer| std.heap.c_allocator.free(buffer);
        const receipt = embeddedSqlCommitReceipt(&reserved, .{ .command_tag = "DELETE", .rows_affected = std.math.maxInt(u64), .mutation_outcome = outcome });
        defer antfly_db_buffer_free(receipt.ptr, receipt.len);
        try std.testing.expect(reserved == null);
        const decoded = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, receipt.ptr.?[0..receipt.len], .{ .parse_numbers = false });
        defer decoded.deinit();
        try std.testing.expectEqualStrings(@tagName(outcome), decoded.value.object.get("mutation_outcome").?.string);
        try std.testing.expectEqualStrings("18446744073709551615", decoded.value.object.get("rows_affected").?.number_string);
        try std.testing.expect(!decoded.value.object.get("error").?.object.get("retryable").?.bool);
    }
}

test "capi SQL unknown commit receipt retains native transaction identity without allocation" {
    var reserved: ?[]u8 = try std.heap.c_allocator.alloc(u8, 512);
    defer if (reserved) |buffer| std.heap.c_allocator.free(buffer);
    const receipt = embeddedSqlUnknownReceipt(&reserved, @splat(0xab));
    defer antfly_db_buffer_free(receipt.ptr, receipt.len);
    try std.testing.expect(reserved == null);
    const decoded = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, receipt.ptr.?[0..receipt.len], .{});
    defer decoded.deinit();
    try std.testing.expectEqualStrings("abababababababababababababababab", decoded.value.object.get("transaction_id").?.string);
    try std.testing.expectEqualStrings("40003", decoded.value.object.get("error").?.object.get("code").?.string);
    try std.testing.expect(!decoded.value.object.get("error").?.object.get("retryable").?.bool);
}

pub export fn antfly_db_search_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (requestLooksLikePublicQueryJson(request_json.bytes())) {
        return searchPublicQueryJson(handle, "docs", request_json, out_buf);
    }
    const Request = struct {
        mode: []const u8,
        index_name: []const u8 = "",
        text_query_type: []const u8 = "",
        text_query_json: []const u8 = "",
        field: []const u8 = "",
        text: []const u8 = "",
        vector: []const f32 = &.{},
        indices: []const u32 = &.{},
        values: []const f32 = &.{},
        k: u32 = 10,
        return_mode: []const u8 = "parent",
        max_chunks_per_parent: u32 = 0,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = true,
        filter_prefix: []const u8 = "",
        distance_over: ?f32 = null,
        distance_under: ?f32 = null,
        filter_ids: []const u64 = &.{},
        exclude_ids: []const u64 = &.{},
        identity_read_generation: ?u64 = null,
        aggregations: []const JsonSearchAggregationRequest = &.{},
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch |err| {
        std.debug.print("pattern parse error={s}\n", .{@errorName(err)});
        return .invalid_argument;
    };
    defer parsed.deinit();

    var query_arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer query_arena.deinit();
    const query_alloc = query_arena.allocator();

    const return_mode: db_mod.types.ReturnMode = if (std.mem.eql(u8, parsed.value.return_mode, "member"))
        .member
    else if (std.mem.eql(u8, parsed.value.return_mode, "chunk"))
        .chunk
    else if (std.mem.eql(u8, parsed.value.return_mode, "parent_with_chunks"))
        .parent_with_chunks
    else
        .parent;

    var req: db_mod.types.SearchRequest = .{
        .index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .return_mode = return_mode,
        .max_chunks_per_parent = parsed.value.max_chunks_per_parent,
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = parsed.value.include_stored,
        .filter_prefix = parsed.value.filter_prefix,
        .distance_over = parsed.value.distance_over,
        .distance_under = parsed.value.distance_under,
        .filter_ids = parsed.value.filter_ids,
        .exclude_ids = parsed.value.exclude_ids,
        .identity_read_generation = parsed.value.identity_read_generation,
    };

    if (std.mem.eql(u8, parsed.value.mode, "full_text")) {
        if (parsed.value.text_query_json.len > 0) {
            var parsed_query = std.json.parseFromSlice(std.json.Value, query_alloc, parsed.value.text_query_json, .{}) catch return .invalid_argument;
            defer parsed_query.deinit();
            req.full_text = parseTextQueryJson(query_alloc, parsed_query.value) catch return .invalid_argument;
        } else {
            req.full_text = if (std.mem.eql(u8, parsed.value.text_query_type, "term"))
                .{ .term = .{ .field = parsed.value.field, .term = parsed.value.text } }
            else if (std.mem.eql(u8, parsed.value.text_query_type, "match"))
                .{ .match = .{ .field = parsed.value.field, .text = parsed.value.text } }
            else
                .{ .match_all = {} };
        }
    } else if (std.mem.eql(u8, parsed.value.mode, "dense")) {
        req.dense = .{
            .vector = parsed.value.vector,
            .k = parsed.value.k,
        };
    } else if (std.mem.eql(u8, parsed.value.mode, "sparse")) {
        req.sparse = .{
            .indices = parsed.value.indices,
            .values = parsed.value.values,
            .k = parsed.value.k,
        };
    } else {
        return .invalid_argument;
    }

    var result = runAtStampedGeneration(handle, &req, LocalSearchQuery{}) catch |err| return capi.mapError(err);
    defer result.deinit();

    var aggregation_results: []JsonSearchAggregationResult = &.{};
    if (parsed.value.aggregations.len > 0) {
        var agg_source_is_full = result.hits.len == result.total_hits;
        var full_result: ?db_mod.types.SearchResult = null;
        defer {
            if (full_result) |*value| value.deinit();
        }
        if (!agg_source_is_full) {
            if (result.total_hits > aggregations_mod.max_aggregation_source_hits)
                return capi.mapError(error.QueryCandidateBudgetExceeded);
            var agg_req = req;
            agg_req.offset = 0;
            agg_req.limit = if (result.total_hits == 0) 1 else result.total_hits;
            agg_req.include_stored = true;
            full_result = executeLocalSearch(handle, agg_req) catch |err| return capi.mapError(err);
            agg_source_is_full = true;
        }
        const source = if (full_result) |*value| value else &result;
        const requests = toAggregationRequest(handle.alloc, parsed.value.aggregations) catch return .internal;
        defer freeAggregationRequests(handle.alloc, requests);
        const backend_results = aggregations_mod.computeSearchAggregations(handle.alloc, requests, source.*, .{
            .index_manager = handle.db.core.index_manager,
            .full_text_index_name = req.index_name,
            .identity_read_generation = req.identity_read_generation.?,
        }) catch |err| return capi.mapError(err);
        defer aggregations_mod.deinitResults(handle.alloc, backend_results);
        aggregation_results = toJsonAggregationResults(handle.alloc, backend_results) catch return .internal;
    }
    defer {
        for (aggregation_results) |*item| item.deinit(handle.alloc);
        if (aggregation_results.len > 0) handle.alloc.free(aggregation_results);
    }

    var hits = handle.alloc.alloc(JsonSearchHit, result.hits.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (hits[0..count]) |*item| item.deinit(handle.alloc);
        if (hits.len > 0) handle.alloc.free(hits);
    }
    for (result.hits, 0..) |hit, i| {
        hits[i] = JsonSearchHit.init(handle.alloc, hit) catch return .internal;
        count += 1;
    }
    var graph_results = handle.alloc.alloc(JsonGraphSearchResult, result.graph_results.len) catch return .internal;
    var graph_count: usize = 0;
    defer {
        for (graph_results[0..graph_count]) |*item| item.deinit(handle.alloc);
        if (graph_results.len > 0) handle.alloc.free(graph_results);
    }
    for (result.graph_results, 0..) |graph_result, i| {
        graph_results[i] = JsonGraphSearchResult.init(handle.alloc, graph_result, req.identity_read_generation) catch return .internal;
        graph_count += 1;
    }
    out_buf.* = stringifyJson(JsonSearchResult{
        .total_hits = result.total_hits,
        .identity_read_generation = req.identity_read_generation,
        .hits = hits,
        .graph_results = graph_results,
        .aggregations = aggregation_results,
    }) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    vector_ptr: ?[*]const f32,
    vector_len: usize,
    k: u32,
    limit: u32,
    offset: u32,
    out_result: *capi.PackedDenseSearchResult,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (vector_ptr == null or vector_len == 0) return .invalid_argument;
    handle.prepareDenseSearchRequest(index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    const identity_read_generation = currentIdentityReadGenerationForHandle(handle, null) catch |err| return capi.mapError(err);

    const fast = searchDensePackedFast(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset, identity_read_generation, out_result) catch |err| return capi.mapError(err);
    if (fast) return .ok;

    var owned = searchDenseOwned(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    packDenseHits(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation, out_result) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense_profile(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    vector_ptr: ?[*]const f32,
    vector_len: usize,
    k: u32,
    limit: u32,
    offset: u32,
    out_profile: *capi.DenseSearchProfile,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (vector_ptr == null or vector_len == 0) return .invalid_argument;
    handle.prepareDenseSearchRequest(index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);

    var profiled = searchDenseOwnedProfiled(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    defer profiled.deinit();

    out_profile.* = .{
        .total_ns = profiled.total_ns,
        .index_lookup_ns = profiled.index_lookup_ns,
        .search_ns = profiled.search_ns,
        .hits_ns = profiled.hits_ns,
        .fallback_ns = profiled.fallback_ns,
        .hbc_total_ns = profiled.hbc_total_ns,
        .hbc_setup_ns = profiled.hbc_setup_ns,
        .hbc_root_load_ns = profiled.hbc_root_load_ns,
        .hbc_node_cache_miss_ns = profiled.hbc_node_cache_miss_ns,
        .hbc_node_cache_misses = profiled.hbc_node_cache_misses,
        .hbc_quantized_cache_miss_ns = profiled.hbc_quantized_cache_miss_ns,
        .hbc_quantized_cache_misses = profiled.hbc_quantized_cache_misses,
        .hbc_child_expand_ns = profiled.hbc_child_expand_ns,
        .hbc_leaf_score_ns = profiled.hbc_leaf_score_ns,
        .hbc_rerank_ns = profiled.hbc_rerank_ns,
        .hbc_rerank_vector_load_ns = profiled.hbc_rerank_vector_load_ns,
        .hbc_rerank_distance_ns = profiled.hbc_rerank_distance_ns,
        .hbc_nodes_visited = profiled.hbc_nodes_visited,
        .hbc_leaves_explored = profiled.hbc_leaves_explored,
        .hbc_reranked_vectors = profiled.hbc_reranked_vectors,
        .hit_count = profiled.hit_count,
        .total_hits = profiled.total_hits,
        .used_fast_path = profiled.used_fast_path,
    };
    return .ok;
}

pub export fn antfly_db_dense_noop(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    _ = guard.handle;
    return .ok;
}

pub export fn antfly_db_dense_fixed_packed_result(
    handle_ptr: ?*anyopaque,
    out_result: *capi.PackedDenseSearchResult,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;

    const ids = [_][]const u8{ "doc-fixed-1", "doc-fixed-2", "doc-fixed-3" };
    const scores = [_]f32{ 0.125, 0.25, 0.5 };
    packDenseHits(@intCast(ids.len), &ids, &scores, currentIdentityReadGenerationForHandle(handle, null) catch |err| return capi.mapError(err), out_result) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeDenseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    handle.prepareDenseSearchRequest(req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);

    const identity_read_generation = currentIdentityReadGenerationForHandle(handle, null) catch |err| return capi.mapError(err);
    const maybe_fast = searchDenseWireFast(handle, req.index_name, req.vector, req.k, req.limit, req.offset, identity_read_generation) catch |err| return capi.mapError(err);
    if (maybe_fast) |out| {
        out_buf.* = out;
        return .ok;
    }

    var owned = searchDenseOwned(handle, req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense_wire_profile(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
    out_profile: *capi.DenseWireSearchProfile,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeDenseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    handle.prepareDenseSearchRequest(req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);

    const profiled = searchDenseWireOwnedProfiled(handle, request_buf.bytes()) catch |err| return capi.mapError(err);
    out_buf.* = profiled.out;
    out_profile.* = .{
        .total_ns = profiled.total_ns,
        .decode_ns = profiled.decode_ns,
        .search_ns = profiled.search_ns,
        .resolve_ns = profiled.resolve_ns,
        .encode_ns = profiled.encode_ns,
        .fallback_ns = profiled.fallback_ns,
        .hbc_total_ns = profiled.hbc_total_ns,
        .hbc_setup_ns = profiled.hbc_setup_ns,
        .hbc_root_load_ns = profiled.hbc_root_load_ns,
        .hbc_node_cache_miss_ns = profiled.hbc_node_cache_miss_ns,
        .hbc_node_cache_misses = profiled.hbc_node_cache_misses,
        .hbc_quantized_cache_miss_ns = profiled.hbc_quantized_cache_miss_ns,
        .hbc_quantized_cache_misses = profiled.hbc_quantized_cache_misses,
        .hbc_child_expand_ns = profiled.hbc_child_expand_ns,
        .hbc_leaf_score_ns = profiled.hbc_leaf_score_ns,
        .hbc_rerank_ns = profiled.hbc_rerank_ns,
        .hbc_rerank_vector_load_ns = profiled.hbc_rerank_vector_load_ns,
        .hbc_rerank_distance_ns = profiled.hbc_rerank_distance_ns,
        .hbc_nodes_visited = profiled.hbc_nodes_visited,
        .hbc_leaves_explored = profiled.hbc_leaves_explored,
        .hbc_reranked_vectors = profiled.hbc_reranked_vectors,
        .hit_count = profiled.hit_count,
        .total_hits = profiled.total_hits,
        .used_fast_path = profiled.used_fast_path,
    };
    return .ok;
}

pub export fn antfly_db_search_text_match(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    field: capi.Slice,
    text: capi.Slice,
    limit: u32,
    offset: u32,
    out_result: *capi.DenseSearchResult,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    var owned = searchTextMatchOwned(handle, index_name.bytes(), field.bytes(), text.bytes(), "", 1.0, limit, offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    const result_alloc = std.heap.c_allocator;
    const hits = result_alloc.alloc(capi.DenseSearchHit, owned.ids.len) catch return .internal;
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) result_alloc.free(hit.id_ptr.?[0..hit.id_len]);
        }
        result_alloc.free(hits);
    }
    for (owned.ids, owned.scores, 0..) |id, score, i| {
        const duped = result_alloc.dupe(u8, id) catch return .internal;
        hits[i] = .{
            .id_ptr = duped.ptr,
            .id_len = duped.len,
            .score = score,
        };
        initialized += 1;
    }
    out_result.* = .{
        .hits_ptr = hits.ptr,
        .hit_count = hits.len,
        .total_hits = owned.total_hits,
        .identity_read_generation = owned.identity_read_generation,
    };
    return .ok;
}

pub export fn antfly_db_search_text_match_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeTextMatchRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextMatchRequest(handle.alloc, &req);

    var owned = searchTextMatchOwned(handle, req.index_name, req.field, req.text, req.analyzer, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_text_term_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeTextTermRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextTermRequest(handle.alloc, &req);

    var owned = searchTextTermOwned(handle, req.index_name, req.field, req.text, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_text_match_phrase_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeTextMatchPhraseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextMatchPhraseRequest(handle.alloc, &req);

    var owned = searchTextMatchPhraseOwned(handle, req.index_name, req.field, req.text, req.analyzer, req.fuzziness, req.auto, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_hits_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_result: *capi.DenseSearchResult,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        mode: []const u8,
        index_name: []const u8 = "",
        text_query_type: []const u8 = "",
        text_query_json: []const u8 = "",
        field: []const u8 = "",
        text: []const u8 = "",
        vector: []const f32 = &.{},
        indices: []const u32 = &.{},
        values: []const f32 = &.{},
        k: u32 = 10,
        return_mode: []const u8 = "parent",
        max_chunks_per_parent: u32 = 0,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = false,
        identity_read_generation: ?u64 = null,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    if (parsed.value.include_stored) return .invalid_argument;
    if (!std.mem.eql(u8, parsed.value.return_mode, "parent")) return .invalid_argument;

    var query_arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer query_arena.deinit();
    const query_alloc = query_arena.allocator();

    var req: db_mod.types.SearchRequest = .{
        .index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .return_mode = .parent,
        .max_chunks_per_parent = parsed.value.max_chunks_per_parent,
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = false,
        .identity_read_generation = parsed.value.identity_read_generation,
    };

    if (std.mem.eql(u8, parsed.value.mode, "full_text")) {
        if (parsed.value.text_query_json.len > 0) {
            var parsed_query = std.json.parseFromSlice(std.json.Value, query_alloc, parsed.value.text_query_json, .{}) catch return .invalid_argument;
            defer parsed_query.deinit();
            req.full_text = parseTextQueryJson(query_alloc, parsed_query.value) catch return .invalid_argument;
        } else {
            req.full_text = if (std.mem.eql(u8, parsed.value.text_query_type, "term"))
                .{ .term = .{ .field = parsed.value.field, .term = parsed.value.text } }
            else if (std.mem.eql(u8, parsed.value.text_query_type, "match"))
                .{ .match = .{ .field = parsed.value.field, .text = parsed.value.text } }
            else
                .{ .match_all = {} };
        }
    } else if (std.mem.eql(u8, parsed.value.mode, "sparse")) {
        req.sparse = .{
            .indices = parsed.value.indices,
            .values = parsed.value.values,
            .k = parsed.value.k,
        };
    } else {
        return .invalid_argument;
    }

    var result = runAtStampedGeneration(handle, &req, LocalSearchQuery{}) catch |err| return capi.mapError(err);
    defer result.deinit();
    if (result.graph_results.len > 0) return .invalid_argument;

    const result_alloc = std.heap.c_allocator;
    const hits = result_alloc.alloc(capi.DenseSearchHit, result.hits.len) catch return .internal;
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) result_alloc.free(hit.id_ptr.?[0..hit.id_len]);
        }
        result_alloc.free(hits);
    }
    for (result.hits, 0..) |hit, i| {
        if (hit.stored_data != null or hit.chunk_hits.len > 0) return .invalid_argument;
        const id = result_alloc.dupe(u8, hit.id) catch return .internal;
        hits[i] = .{
            .id_ptr = id.ptr,
            .id_len = id.len,
            .score = hit.score orelse 0,
        };
        initialized += 1;
    }
    out_result.* = .{
        .hits_ptr = hits.ptr,
        .hit_count = hits.len,
        .total_hits = result.total_hits,
        .identity_read_generation = req.identity_read_generation.?,
    };
    return .ok;
}

fn parseTextQueryJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror!db_mod.types.TextQuery {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("match_all") != null) {
        return .{ .match_all = {} };
    }
    if (value.object.get("match_none") != null) {
        return .{ .match_none = {} };
    }
    if (value.object.get("phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .terms = try parseStringArrayJson(alloc, phrase.object.get("terms") orelse return error.InvalidArgument),
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("multi_phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .multi_phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .terms = try parseStringMatrixJson(alloc, phrase.object.get("terms") orelse return error.InvalidArgument),
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("term")) |term| {
        if (term != .object) return error.InvalidArgument;
        return .{ .term = .{
            .field = (term.object.get("field") orelse return error.InvalidArgument).string,
            .term = (term.object.get("term") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(term.object),
        } };
    }
    if (value.object.get("match")) |match| {
        if (match != .object) return error.InvalidArgument;
        return .{ .match = .{
            .field = (match.object.get("field") orelse return error.InvalidArgument).string,
            .text = (match.object.get("text") orelse return error.InvalidArgument).string,
            .analyzer = if (match.object.get("analyzer")) |analyzer| switch (analyzer) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .boost = try parseOptionalBoostJson(match.object),
        } };
    }
    if (value.object.get("match_phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .match_phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .text = (phrase.object.get("text") orelse return error.InvalidArgument).string,
            .analyzer = if (phrase.object.get("analyzer")) |analyzer| switch (analyzer) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("fuzzy")) |fuzzy| {
        if (fuzzy != .object) return error.InvalidArgument;
        const edits_value = fuzzy.object.get("max_edits") orelse std.json.Value{ .integer = 1 };
        return .{ .fuzzy = .{
            .field = (fuzzy.object.get("field") orelse return error.InvalidArgument).string,
            .term = (fuzzy.object.get("term") orelse return error.InvalidArgument).string,
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .prefix_len = if (fuzzy.object.get("prefix_length")) |prefix| switch (prefix) {
                .integer => |v| @intCast(v),
                else => return error.InvalidArgument,
            } else 0,
            .auto_fuzzy = if (fuzzy.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(fuzzy.object),
        } };
    }
    if (value.object.get("numeric_range")) |range_query| {
        if (range_query != .object) return error.InvalidArgument;
        return .{ .numeric_range = .{
            .field = (range_query.object.get("field") orelse return error.InvalidArgument).string,
            .min = if (range_query.object.get("min")) |min| switch (min) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max = if (range_query.object.get("max")) |max| switch (max) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_min = if (range_query.object.get("inclusive_min")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_max = if (range_query.object.get("inclusive_max")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(range_query.object),
        } };
    }
    if (value.object.get("date_range")) |range_query| {
        if (range_query != .object) return error.InvalidArgument;
        return .{ .date_range = .{
            .field = (range_query.object.get("field") orelse return error.InvalidArgument).string,
            .start_ns = if (range_query.object.get("start_ns")) |start| switch (start) {
                .integer => |v| @intCast(v),
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .end_ns = if (range_query.object.get("end_ns")) |end| switch (end) {
                .integer => |v| @intCast(v),
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_start = if (range_query.object.get("inclusive_start")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_end = if (range_query.object.get("inclusive_end")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(range_query.object),
        } };
    }
    if (value.object.get("doc_id")) |doc_id| {
        if (doc_id != .object) return error.InvalidArgument;
        return .{ .doc_id = .{
            .ids = try parseStringArrayJson(alloc, doc_id.object.get("ids") orelse return error.InvalidArgument),
            .boost = try parseOptionalBoostJson(doc_id.object),
        } };
    }
    if (value.object.get("bool_field")) |bool_field| {
        if (bool_field != .object) return error.InvalidArgument;
        return .{ .bool_field = .{
            .field = (bool_field.object.get("field") orelse return error.InvalidArgument).string,
            .value = switch (bool_field.object.get("value") orelse return error.InvalidArgument) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(bool_field.object),
        } };
    }
    if (value.object.get("geo_distance")) |geo_distance| {
        if (geo_distance != .object) return error.InvalidArgument;
        return .{ .geo_distance = .{
            .field = (geo_distance.object.get("field") orelse return error.InvalidArgument).string,
            .lon = switch (geo_distance.object.get("lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .lat = switch (geo_distance.object.get("lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .radius_meters = switch (geo_distance.object.get("radius_meters") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(geo_distance.object),
        } };
    }
    if (value.object.get("geo_bbox")) |geo_bbox| {
        if (geo_bbox != .object) return error.InvalidArgument;
        return .{ .geo_bbox = .{
            .field = (geo_bbox.object.get("field") orelse return error.InvalidArgument).string,
            .min_lat = switch (geo_bbox.object.get("min_lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .min_lon = switch (geo_bbox.object.get("min_lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .max_lat = switch (geo_bbox.object.get("max_lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .max_lon = switch (geo_bbox.object.get("max_lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(geo_bbox.object),
        } };
    }
    if (value.object.get("prefix")) |prefix| {
        if (prefix != .object) return error.InvalidArgument;
        return .{ .prefix = .{
            .field = (prefix.object.get("field") orelse return error.InvalidArgument).string,
            .prefix = (prefix.object.get("prefix") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(prefix.object),
        } };
    }
    if (value.object.get("wildcard")) |wildcard| {
        if (wildcard != .object) return error.InvalidArgument;
        return .{ .wildcard = .{
            .field = (wildcard.object.get("field") orelse return error.InvalidArgument).string,
            .pattern = (wildcard.object.get("pattern") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(wildcard.object),
        } };
    }
    if (value.object.get("regexp")) |regexp| {
        if (regexp != .object) return error.InvalidArgument;
        return .{ .regexp = .{
            .field = (regexp.object.get("field") orelse return error.InvalidArgument).string,
            .pattern = (regexp.object.get("pattern") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(regexp.object),
        } };
    }
    if (value.object.get("term_range")) |term_range| {
        if (term_range != .object) return error.InvalidArgument;
        return .{ .term_range = .{
            .field = (term_range.object.get("field") orelse return error.InvalidArgument).string,
            .min = if (term_range.object.get("min")) |min| switch (min) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max = if (term_range.object.get("max")) |max| switch (max) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_min = if (term_range.object.get("inclusive_min")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_max = if (term_range.object.get("inclusive_max")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(term_range.object),
        } };
    }
    if (value.object.get("ip_range")) |ip_range| {
        if (ip_range != .object) return error.InvalidArgument;
        return .{ .ip_range = .{
            .field = (ip_range.object.get("field") orelse return error.InvalidArgument).string,
            .cidr = (ip_range.object.get("cidr") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(ip_range.object),
        } };
    }
    if (value.object.get("geo_shape")) |geo_shape| {
        if (geo_shape != .object) return error.InvalidArgument;
        return .{ .geo_shape = .{
            .field = (geo_shape.object.get("field") orelse return error.InvalidArgument).string,
            .relation = if (geo_shape.object.get("relation")) |relation|
                try parseGeoShapeRelation(relation)
            else
                .intersects,
            .polygons = try parseGeoShapePolygonsJson(alloc, geo_shape),
            .boost = try parseOptionalBoostJson(geo_shape.object),
        } };
    }
    if (value.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;

        var must_list = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
        errdefer must_list.deinit(alloc);
        if (bool_query.object.get("filter")) |filter_value| {
            try appendTextQueryArrayJson(alloc, &must_list, filter_value);
        }
        if (bool_query.object.get("must")) |must_value| {
            try appendTextQueryArrayJson(alloc, &must_list, must_value);
        }
        const must = if (must_list.items.len > 0)
            try must_list.toOwnedSlice(alloc)
        else
            &.{};
        const should = if (bool_query.object.get("should")) |should_value|
            try parseTextQueryArrayJson(alloc, should_value)
        else
            &.{};
        const must_not = if (bool_query.object.get("must_not")) |must_not_value|
            try parseTextQueryArrayJson(alloc, must_not_value)
        else
            &.{};
        const min_should = if (bool_query.object.get("min_should")) |min_should_value|
            try parseMinShouldJson(min_should_value)
        else
            0;

        if (must.len == 0 and should.len == 0 and must_not.len == 0) return error.InvalidArgument;
        return .{ .bool_query = .{
            .must = must,
            .should = should,
            .must_not = must_not,
            .min_should = min_should,
            .boost = try parseOptionalBoostJson(bool_query.object),
        } };
    }
    if (value.object.get("conjuncts")) |conjuncts| {
        return .{ .bool_query = .{ .must = try parseTextQueryArrayJson(alloc, conjuncts) } };
    }
    if (value.object.get("disjuncts")) |disjuncts| {
        const min_should = if (value.object.get("min_should")) |min_should_value|
            try parseMinShouldJson(min_should_value)
        else
            0;
        return .{ .bool_query = .{
            .should = try parseTextQueryArrayJson(alloc, disjuncts),
            .min_should = min_should,
        } };
    }
    return error.InvalidArgument;
}

fn parseMinShouldJson(value: std.json.Value) anyerror!u32 {
    return switch (value) {
        .integer => |v| if (v < 0) error.InvalidArgument else @intCast(v),
        .float => |v| blk: {
            if (v < 0 or @floor(v) != v or v > std.math.maxInt(u32)) return error.InvalidArgument;
            break :blk @intFromFloat(v);
        },
        else => error.InvalidArgument,
    };
}

fn parseOptionalBoostJson(object: std.json.ObjectMap) anyerror!f32 {
    if (object.get("boost")) |value| {
        return switch (value) {
            .float => |v| @floatCast(v),
            .integer => |v| @floatFromInt(v),
            else => return error.InvalidArgument,
        };
    }
    return 1.0;
}

fn parseStringArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var items = try alloc.alloc([]const u8, value.array.items.len);
    errdefer alloc.free(items);
    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidArgument;
        items[i] = item.string;
    }
    return items;
}

fn parseStringMatrixJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var rows = try alloc.alloc([]const []const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| alloc.free(row);
        alloc.free(rows);
    }
    for (value.array.items, 0..) |item, i| {
        rows[i] = try parseStringArrayJson(alloc, item);
        initialized += 1;
    }
    return rows;
}

fn parseGeoPointJson(value: std.json.Value) anyerror!db_mod.types.GeoPoint {
    if (value != .object) return error.InvalidArgument;
    return .{
        .lon = switch (value.object.get("lon") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
        .lat = switch (value.object.get("lat") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
    };
}

fn parseGeoPointArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const db_mod.types.GeoPoint {
    if (value != .array or value.array.items.len < 3) return error.InvalidArgument;
    var points = try alloc.alloc(db_mod.types.GeoPoint, value.array.items.len);
    errdefer alloc.free(points);
    for (value.array.items, 0..) |item, i| {
        points[i] = try parseGeoPointJson(item);
    }
    if (!std.meta.eql(points[0], points[points.len - 1])) {
        var closed = try alloc.alloc(db_mod.types.GeoPoint, points.len + 1);
        @memcpy(closed[0..points.len], points);
        closed[points.len] = points[0];
        alloc.free(points);
        return closed;
    }
    return points;
}

fn parseGeoShapePolygonsJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const db_mod.types.GeoPoint {
    if (value.object.get("polygons")) |polygons_value| {
        if (polygons_value != .array or polygons_value.array.items.len == 0) return error.InvalidArgument;
        var polygons = try alloc.alloc([]const db_mod.types.GeoPoint, polygons_value.array.items.len);
        var initialized: usize = 0;
        errdefer {
            for (polygons[0..initialized]) |polygon| alloc.free(polygon);
            alloc.free(polygons);
        }
        for (polygons_value.array.items, 0..) |item, i| {
            polygons[i] = try parseGeoPointArrayJson(alloc, item);
            initialized += 1;
        }
        return polygons;
    }
    if (value.object.get("polygon")) |polygon_value| {
        var polygons = try alloc.alloc([]const db_mod.types.GeoPoint, 1);
        errdefer alloc.free(polygons);
        polygons[0] = try parseGeoPointArrayJson(alloc, polygon_value);
        return polygons;
    }
    return error.InvalidArgument;
}

fn parseGeoShapeRelation(value: std.json.Value) anyerror!db_mod.types.GeoShapeRelation {
    if (value != .string) return error.InvalidArgument;
    if (std.mem.eql(u8, value.string, "intersects")) return .intersects;
    if (std.mem.eql(u8, value.string, "within")) return .within;
    if (std.mem.eql(u8, value.string, "contains")) return .contains;
    return error.InvalidArgument;
}

fn parseTextQueryArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]db_mod.types.TextQuery {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var clauses = try alloc.alloc(db_mod.types.TextQuery, value.array.items.len);
    for (value.array.items, 0..) |item, i| {
        clauses[i] = try parseTextQueryJson(alloc, item);
    }
    return clauses;
}

fn appendTextQueryArrayJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    value: std.json.Value,
) anyerror!void {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    try out.ensureUnusedCapacity(alloc, value.array.items.len);
    for (value.array.items) |item| {
        out.appendAssumeCapacity(try parseTextQueryJson(alloc, item));
    }
}

test "c api bool text parser treats filter clauses as required" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"bool":{"must":[{"term":{"field":"body","term":"invoice"}}],"filter":[{"term":{"field":"tenant","term":"acme"}}]}}
    , .{});
    const query = try parseTextQueryJson(alloc, parsed.value);

    try std.testing.expect(query == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), query.bool_query.must.len);
    try std.testing.expect(query.bool_query.must[0] == .term);
    try std.testing.expectEqualStrings("tenant", query.bool_query.must[0].term.field);
    try std.testing.expectEqualStrings("acme", query.bool_query.must[0].term.term);
    try std.testing.expect(query.bool_query.must[1] == .term);
    try std.testing.expectEqualStrings("body", query.bool_query.must[1].term.field);
    try std.testing.expectEqualStrings("invoice", query.bool_query.must[1].term.term);
}

pub export fn antfly_db_execute_graph_queries_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        graph_queries: []const JsonGraphQueryRequest,
        named_sets: []const JsonNamedGraphInputSetRequest,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = true,
        identity_read_generation: ?u64 = null,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    if (parsed.value.identity_read_generation == null) {
        for (parsed.value.named_sets) |named_set| {
            if (named_set.hit_ids_b64.len > 0) return .invalid_argument;
        }
    }

    const graph_queries = parseNamedGraphQueries(handle.alloc, parsed.value.graph_queries) catch return .invalid_argument;
    defer freeOwnedNamedGraphQueries(handle.alloc, graph_queries);

    const named_sets = parseNamedGraphInputSets(handle.alloc, parsed.value.named_sets) catch return .invalid_argument;
    defer freeOwnedNamedGraphInputSets(handle.alloc, named_sets);

    var req: db_mod.types.SearchRequest = .{
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = parsed.value.include_stored,
        .graph_queries = graph_queries,
        .identity_read_generation = parsed.value.identity_read_generation,
    };

    const GraphQuery = struct {
        graph_queries: []const db_mod.types.NamedGraphQuery,
        named_sets: []const db_mod.types.NamedGraphInputSet,
        const Result = []db_mod.types.GraphSearchResult;
        fn run(self: @This(), h: *Handle, r: db_mod.types.SearchRequest) !Result {
            return h.db.executeNamedGraphQueries(h.alloc, r, self.graph_queries, self.named_sets);
        }
    };
    const results = runAtStampedGeneration(handle, &req, GraphQuery{
        .graph_queries = graph_queries,
        .named_sets = named_sets,
    }) catch |err| return capi.mapError(err);
    defer {
        for (results) |*result| result.deinit(handle.alloc);
        if (results.len > 0) handle.alloc.free(results);
    }

    var payload = handle.alloc.alloc(JsonGraphSearchResult, results.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |result, i| {
        payload[i] = JsonGraphSearchResult.init(handle.alloc, result, req.identity_read_generation) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_aggregate_hits_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    var parsed = std.json.parseFromSlice(JsonAggregateHitsRequest, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    if (parsed.value.hit_ids_b64.len > 0 and parsed.value.identity_read_generation == null) return .invalid_argument;
    const identity_read_generation = currentIdentityReadGenerationForHandle(handle, parsed.value.identity_read_generation) catch |err| return capi.mapError(err);

    const requests = toAggregationRequest(handle.alloc, parsed.value.aggregations) catch return .internal;
    defer freeAggregationRequests(handle.alloc, requests);

    var hits = handle.alloc.alloc(db_mod.types.SearchHit, parsed.value.hit_ids_b64.len) catch return .internal;
    var hit_count: usize = 0;
    defer {
        for (hits[0..hit_count]) |*hit| hit.deinit(handle.alloc);
        if (hits.len > 0) handle.alloc.free(hits);
    }
    for (parsed.value.hit_ids_b64) |item| {
        const hit_id = decodeBase64Alloc(handle.alloc, item) catch return .invalid_argument;
        errdefer handle.alloc.free(hit_id);
        const stored = handle.db.get(handle.alloc, hit_id) catch |err| {
            handle.alloc.free(hit_id);
            return capi.mapError(err);
        } orelse {
            handle.alloc.free(hit_id);
            continue;
        };
        hits[hit_count] = .{
            .id = hit_id,
            .stored_data = stored,
        };
        hit_count += 1;
    }

    const result = db_mod.types.SearchResult{
        .alloc = handle.alloc,
        .hits = hits[0..hit_count],
        .total_hits = @intCast(hit_count),
    };
    const backend_results = aggregations_mod.computeSearchAggregations(handle.alloc, requests, result, .{
        .index_manager = handle.db.core.index_manager,
        .full_text_index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .identity_read_generation = identity_read_generation,
    }) catch |err| return capi.mapError(err);
    defer aggregations_mod.deinitResults(handle.alloc, backend_results);

    const aggregation_results = toJsonAggregationResults(handle.alloc, backend_results) catch return .internal;
    defer {
        for (aggregation_results) |*item| item.deinit(handle.alloc);
        if (aggregation_results.len > 0) handle.alloc.free(aggregation_results);
    }

    out_buf.* = stringifyJson(aggregation_results) catch return .internal;
    return .ok;
}

fn parseNamedGraphQueries(alloc: Allocator, requests: []const JsonGraphQueryRequest) ![]db_mod.types.NamedGraphQuery {
    var queries = try alloc.alloc(db_mod.types.NamedGraphQuery, requests.len);
    errdefer alloc.free(queries);
    var count: usize = 0;
    errdefer {
        for (queries[0..count]) |*query| deinitOwnedNamedGraphQuery(alloc, query);
    }
    for (requests, 0..) |request, i| {
        queries[i] = .{
            .name = try alloc.dupe(u8, request.name),
            .query = try parseGraphQueryRequestOwned(alloc, request),
        };
        count += 1;
    }
    return queries;
}

fn parseNamedGraphInputSets(alloc: Allocator, requests: []const JsonNamedGraphInputSetRequest) ![]db_mod.types.NamedGraphInputSet {
    var sets = try alloc.alloc(db_mod.types.NamedGraphInputSet, requests.len);
    errdefer alloc.free(sets);
    var count: usize = 0;
    errdefer {
        for (sets[0..count]) |*set| deinitOwnedNamedGraphInputSet(alloc, set);
    }
    for (requests, 0..) |request, i| {
        sets[i] = .{
            .name = try alloc.dupe(u8, request.name),
            .hit_ids = try decodeGraphHitIds(alloc, request.hit_ids_b64),
            .total_hits = request.total_hits,
        };
        count += 1;
    }
    return sets;
}

fn parseGraphQueryRequestOwned(alloc: Allocator, request: JsonGraphQueryRequest) !graph_query_mod.GraphQuery {
    return .{
        .query_type = if (std.mem.eql(u8, request.type, "neighbors"))
            .neighbors
        else if (std.mem.eql(u8, request.type, "traverse"))
            .traverse
        else if (std.mem.eql(u8, request.type, "shortest_path"))
            .shortest_path
        else if (std.mem.eql(u8, request.type, "k_shortest_paths"))
            .k_shortest_paths
        else
            return error.InvalidArgument,
        .index_name = try alloc.dupe(u8, request.index_name),
        .start_nodes = try parseGraphNodeSelectorRequestOwned(alloc, request.start_nodes),
        .target_nodes = if (request.target_nodes) |target_nodes| try parseGraphNodeSelectorRequestOwned(alloc, target_nodes) else null,
        .params = .{
            .edge_types = try cloneGraphEdgeTypes(alloc, request.edge_types),
            .direction = parseGraphDirection(request.direction),
            .max_depth = request.max_depth,
            .max_results = request.max_results,
            .min_weight = legacyGraphWeightBound(request.min_weight),
            .max_weight = legacyGraphWeightBound(request.max_weight),
            .deduplicate = request.deduplicate,
            .include_paths = request.include_paths,
            .weight_mode = parseGraphWeightMode(request.weight_mode),
        },
        .k = request.k,
    };
}

fn parseGraphNodeSelectorRequestOwned(alloc: Allocator, selector: JsonGraphNodeSelectorRequest) !graph_query_mod.NodeSelector {
    if (selector.keys.len > 0) return .{ .keys = try decodeGraphKeys(alloc, selector.keys) };
    if (selector.result_ref.len > 0) {
        return .{ .result_ref = .{
            .ref = try alloc.dupe(u8, selector.result_ref),
            .limit = selector.limit,
        } };
    }
    return error.InvalidArgument;
}

fn decodeGraphKeys(alloc: Allocator, keys: []const []const u8) ![]const []const u8 {
    var owned = try alloc.alloc([]const u8, keys.len);
    errdefer alloc.free(owned);
    var count: usize = 0;
    errdefer {
        for (owned[0..count]) |key| alloc.free(@constCast(key));
    }
    for (keys, 0..) |key, i| {
        owned[i] = try decodeBase64Alloc(alloc, key);
        count += 1;
    }
    return owned;
}

fn cloneGraphEdgeTypes(alloc: Allocator, edge_types: []const []const u8) ![]const []const u8 {
    var owned = try alloc.alloc([]const u8, edge_types.len);
    errdefer alloc.free(owned);
    var count: usize = 0;
    errdefer {
        for (owned[0..count]) |item| alloc.free(@constCast(item));
    }
    for (edge_types, 0..) |edge_type, i| {
        owned[i] = try alloc.dupe(u8, edge_type);
        count += 1;
    }
    return owned;
}

fn decodeGraphHitIds(alloc: Allocator, hit_ids_b64: []const []const u8) ![]const []const u8 {
    var hit_ids = try alloc.alloc([]const u8, hit_ids_b64.len);
    errdefer alloc.free(hit_ids);
    var count: usize = 0;
    errdefer {
        for (hit_ids[0..count]) |hit_id| alloc.free(@constCast(hit_id));
    }
    for (hit_ids_b64, 0..) |item, i| {
        hit_ids[i] = try decodeBase64Alloc(alloc, item);
        count += 1;
    }
    return hit_ids;
}

fn deinitOwnedNodeSelector(alloc: Allocator, selector: *graph_query_mod.NodeSelector) void {
    switch (selector.*) {
        .keys => |keys| {
            for (keys) |key| alloc.free(@constCast(key));
            if (keys.len > 0) alloc.free(keys);
        },
        .identities => |identities| {
            for (identities) |identity| {
                alloc.free(@constCast(identity.key));
                if (identity.table) |table| alloc.free(@constCast(table));
            }
            if (identities.len > 0) alloc.free(identities);
        },
        .result_ref => |result_ref| {
            alloc.free(@constCast(result_ref.ref));
        },
    }
    selector.* = undefined;
}

fn deinitOwnedGraphQuery(alloc: Allocator, query: *graph_query_mod.GraphQuery) void {
    alloc.free(@constCast(query.index_name));
    deinitOwnedNodeSelector(alloc, &query.start_nodes);
    if (query.target_nodes) |*target_nodes| deinitOwnedNodeSelector(alloc, target_nodes);
    for (query.params.edge_types) |edge_type| alloc.free(@constCast(edge_type));
    if (query.params.edge_types.len > 0) alloc.free(query.params.edge_types);
    query.* = undefined;
}

fn deinitOwnedNamedGraphQuery(alloc: Allocator, query: *db_mod.types.NamedGraphQuery) void {
    alloc.free(query.name);
    deinitOwnedGraphQuery(alloc, &query.query);
    query.* = undefined;
}

fn freeOwnedNamedGraphQueries(alloc: Allocator, queries: []db_mod.types.NamedGraphQuery) void {
    for (queries) |*query| deinitOwnedNamedGraphQuery(alloc, query);
    if (queries.len > 0) alloc.free(queries);
}

fn deinitOwnedNamedGraphInputSet(alloc: Allocator, set: *db_mod.types.NamedGraphInputSet) void {
    alloc.free(@constCast(set.name));
    for (set.hit_ids) |hit_id| alloc.free(@constCast(hit_id));
    if (set.hit_ids.len > 0) alloc.free(@constCast(set.hit_ids));
    set.* = undefined;
}

fn freeOwnedNamedGraphInputSets(alloc: Allocator, sets: []db_mod.types.NamedGraphInputSet) void {
    for (sets) |*set| deinitOwnedNamedGraphInputSet(alloc, set);
    if (sets.len > 0) alloc.free(sets);
}

fn parseGraphDirection(direction: []const u8) db_mod.types.GraphEdgeDirection {
    if (std.mem.eql(u8, direction, "in")) return .in;
    if (std.mem.eql(u8, direction, "both")) return .both;
    return .out;
}

fn parseGraphWeightMode(mode: []const u8) db_mod.types.GraphPathWeightMode {
    if (std.mem.eql(u8, mode, "min_weight")) return .min_weight;
    if (std.mem.eql(u8, mode, "max_weight")) return .max_weight;
    return .min_hops;
}

fn legacyGraphWeightBound(value: f64) ?f64 {
    return if (value > 0 and std.math.isFinite(value)) value else null;
}

fn computeSearchAggregations(
    alloc: Allocator,
    requests: []const JsonSearchAggregationRequest,
    result: db_mod.types.SearchResult,
) anyerror![]JsonSearchAggregationResult {
    var out = try alloc.alloc(JsonSearchAggregationResult, requests.len);
    errdefer alloc.free(out);

    for (requests, 0..) |request, i| {
        out[i] = try computeSingleAggregation(alloc, request, result.hits);
    }
    return out;
}

fn computeSingleAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (std.mem.eql(u8, request.type, "count")) {
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try std.fmt.allocPrint(alloc, "{d}", .{hits.len}),
        };
    }
    if (std.mem.eql(u8, request.type, "sum")) return try computeNumericMetricAggregation(alloc, request, hits, .sum);
    if (std.mem.eql(u8, request.type, "min")) return try computeNumericMetricAggregation(alloc, request, hits, .min);
    if (std.mem.eql(u8, request.type, "max")) return try computeNumericMetricAggregation(alloc, request, hits, .max);
    if (std.mem.eql(u8, request.type, "avg")) return try computeNumericMetricAggregation(alloc, request, hits, .avg);
    if (std.mem.eql(u8, request.type, "stats")) return try computeNumericMetricAggregation(alloc, request, hits, .stats);
    if (std.mem.eql(u8, request.type, "cardinality")) return try computeCardinalityAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "terms")) return try computeTermsAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "histogram")) return try computeHistogramAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "date_histogram")) return try computeDateHistogramAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "range")) return try computeRangeAggregation(alloc, request, hits);
    return error.UnsupportedAggregation;
}

const NumericMetricKind = enum { sum, min, max, avg, stats };

fn computeNumericMetricAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
    kind: NumericMetricKind,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var sum: f64 = 0;
    var sum_squares: f64 = 0;
    var count: i64 = 0;
    var min_value: f64 = std.math.inf(f64);
    var max_value: f64 = -std.math.inf(f64);

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        accumulateNumericJsonValue(value, &sum, &sum_squares, &count, &min_value, &max_value);
    }

    const value_json = switch (kind) {
        .sum => try std.fmt.allocPrint(alloc, "{d}", .{sum}),
        .min => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{min_value}),
        .max => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{max_value}),
        .avg => if (count == 0)
            try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0}")
        else
            try std.fmt.allocPrint(alloc, "{{\"count\":{d},\"sum\":{d},\"avg\":{d}}}", .{ count, sum, sum / @as(f64, @floatFromInt(count)) }),
        .stats => blk: {
            if (count == 0) break :blk try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0,\"min\":null,\"max\":null,\"sum_squares\":0,\"variance\":0,\"std_dev\":0}");
            const avg = sum / @as(f64, @floatFromInt(count));
            const variance = (sum_squares / @as(f64, @floatFromInt(count))) - (avg * avg);
            const non_negative_variance = if (variance < 0) 0 else variance;
            break :blk try std.fmt.allocPrint(
                alloc,
                "{{\"count\":{d},\"sum\":{d},\"avg\":{d},\"min\":{d},\"max\":{d},\"sum_squares\":{d},\"variance\":{d},\"std_dev\":{d}}}",
                .{ count, sum, avg, min_value, max_value, sum_squares, non_negative_variance, @sqrt(non_negative_variance) },
            );
        },
    };
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = value_json,
    };
}

fn computeCardinalityAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var seen = std.StringHashMap(void).init(alloc);
    defer {
        var it = seen.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try collectCardinalityValues(alloc, &seen, value);
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{seen.count()}),
    };
}

fn computeTermsAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var counts = std.StringHashMap(i64).init(alloc);
    defer {
        var it = counts.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        counts.deinit();
    }
    var grouped = std.StringHashMap(std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    if (request.term_pattern.len > 0) return error.UnsupportedAggregation;

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try appendTermAggregationValuesZig(alloc, &counts, &grouped, hit, value);
    }

    var entries = std.ArrayList(struct { key: []const u8, count: i64 }).empty;
    defer entries.deinit(alloc);
    var it = counts.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key, request.term_prefix)) continue;
        if (request.min_doc_count > 0 and count < request.min_doc_count) continue;
        try entries.append(alloc, .{ .key = key, .count = count });
    }
    std.mem.sort(@TypeOf(entries.items[0]), entries.items, {}, struct {
        fn lessThan(_: void, lhs: @TypeOf(entries.items[0]), rhs: @TypeOf(entries.items[0])) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < entries.items.len) @intCast(request.size) else entries.items.len;
    var buckets = try alloc.alloc(JsonSearchAggregationBucket, limit);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (entries.items[0..limit], 0..) |entry, idx| {
        const grouped_hits = grouped.get(entry.key).?.items;
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = grouped_hits,
                .total_hits = @intCast(grouped_hits.len),
            });
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{entry.key}),
            .count = entry.count,
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeHistogramAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0 or request.interval <= 0) return error.InvalidAggregation;

    var bucket_counts = std.AutoHashMap(i64, i64).init(alloc);
    defer bucket_counts.deinit();
    var grouped = std.AutoHashMap(i64, std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        if (jsonValueToF64(value)) |numeric| {
            const bucket_index = @as(i64, @intFromFloat(@floor(numeric / request.interval)));
            const entry = try bucket_counts.getOrPut(bucket_index);
            if (entry.found_existing) entry.value_ptr.* += 1 else entry.value_ptr.* = 1;
            const grouped_entry = try grouped.getOrPut(bucket_index);
            if (!grouped_entry.found_existing) grouped_entry.value_ptr.* = .empty;
            try grouped_entry.value_ptr.append(alloc, hit);
        }
    }

    var present_keys = try alloc.alloc(i64, bucket_counts.count());
    defer if (present_keys.len > 0) alloc.free(present_keys);
    var iter = bucket_counts.iterator();
    var present_count: usize = 0;
    while (iter.next()) |entry| {
        if (request.min_doc_count > 0 and entry.value_ptr.* < request.min_doc_count) continue;
        present_keys[present_count] = entry.key_ptr.*;
        present_count += 1;
    }
    std.mem.sort(i64, present_keys[0..present_count], {}, struct {
        fn lessThan(_: void, lhs: i64, rhs: i64) bool {
            return lhs < rhs;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and present_count > 0)
        try fillHistogramBucketKeys(alloc, present_keys[0], present_keys[present_count - 1])
    else
        try alloc.dupe(i64, present_keys[0..present_count]);
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            if (grouped.get(bucket_index)) |list| {
                break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                });
            }
            break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
        };
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = bucket_counts.get(bucket_index) orelse 0,
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeDateHistogramAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const interval = try parseDateInterval(request);
    var agg = search_agg_mod.DateHistogramAgg.init(alloc, interval);
    defer agg.deinit();
    var grouped = std.AutoHashMap(u64, std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (value) |ns| {
            try agg.collect(ns);
            const bucket_key = search_agg_mod.truncateToInterval(ns, interval);
            const entry = try grouped.getOrPut(bucket_key);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(alloc, hit);
        }
    }

    const present_keys = try agg.sortedKeys(alloc);
    defer if (present_keys.len > 0) alloc.free(present_keys);

    var kept: usize = 0;
    for (present_keys) |key| {
        const count = agg.getCount(key);
        if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
        kept += 1;
    }

    const keys = if (request.min_doc_count == 0 and kept > 0)
        try fillDateHistogramBucketKeys(alloc, present_keys[0], present_keys[present_keys.len - 1], interval)
    else blk: {
        var filtered = try alloc.alloc(u64, kept);
        var idx: usize = 0;
        for (present_keys) |key| {
            const count = agg.getCount(key);
            if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
            filtered[idx] = key;
            idx += 1;
        }
        break :blk filtered;
    };
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, key);
        defer alloc.free(formatted);
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            if (grouped.get(key)) |list| {
                break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                });
            }
            break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{formatted}),
            .count = @intCast(agg.getCount(key)),
            .aggregations = nested,
        };
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeRangeAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const has_numeric = request.ranges.len > 0;
    const has_date = request.date_ranges.len > 0;
    const has_distance = request.distance_ranges.len > 0;
    if ((@intFromBool(has_numeric) + @intFromBool(has_date) + @intFromBool(has_distance)) != 1) return error.InvalidAggregation;

    if (has_numeric) {
        var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            defer matched.deinit(alloc);
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractNumericFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |numeric| {
                    if (matchesNumericRangeValue(numeric, range_spec)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }) else try alloc.alloc(JsonSearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    if (has_date) {
        var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.date_ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.date_ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            defer matched.deinit(alloc);
            const start_ns = if (range_spec.start) |start| try parseRfc3339ToNs(start) else null;
            const end_ns = if (range_spec.end) |end| try parseRfc3339ToNs(end) else null;
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |timestamp| {
                    if (matchesDateRangeValue(timestamp, start_ns, end_ns)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }) else try alloc.alloc(JsonSearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    var bands = try alloc.alloc(search_agg_mod.GeoDistanceRange, request.distance_ranges.len);
    defer alloc.free(bands);
    for (request.distance_ranges, 0..) |range_spec, idx| {
        bands[idx] = .{
            .from = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null,
            .to = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null,
        };
    }

    var agg = try search_agg_mod.GeoDistanceAgg.init(alloc, .{
        .lat = request.center_lat,
        .lon = request.center_lon,
    }, bands);
    defer agg.deinit();

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (point) |geo_point| {
            agg.collect(geo_point);
        }
    }

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.distance_ranges.len);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (request.distance_ranges, 0..) |range_spec, idx| {
        var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
        defer matched.deinit(alloc);
        const from_meters = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null;
        const to_meters = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null;
        for (hits) |hit| {
            const stored = hit.stored_data orelse continue;
            const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
            if (point) |geo_point| {
                const dist = geo_mod.haversineDistance(.{ .lat = request.center_lat, .lon = request.center_lon }, geo_point);
                if (matchesGeoDistanceValue(dist, from_meters, to_meters)) try matched.append(alloc, hit);
            }
        }
        const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
            .alloc = alloc,
            .hits = matched.items,
            .total_hits = @intCast(matched.items.len),
        }) else try alloc.alloc(JsonSearchAggregationResult, 0);
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
            .count = @intCast(agg.bands[idx].count),
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn matchesNumericRangeValue(value: f64, range_spec: JsonNumericRangeRequest) bool {
    if (range_spec.start) |start| {
        if (value < start) return false;
    }
    if (range_spec.end) |end| {
        if (value >= end) return false;
    }
    return true;
}

fn matchesDateRangeValue(value: u64, start_ns: ?u64, end_ns: ?u64) bool {
    if (start_ns) |start| {
        if (value < start) return false;
    }
    if (end_ns) |end| {
        if (value >= end) return false;
    }
    return true;
}

fn matchesGeoDistanceValue(value_meters: f64, from_meters: ?f64, to_meters: ?f64) bool {
    if (from_meters) |from| {
        if (value_meters < from) return false;
    }
    if (to_meters) |to| {
        if (value_meters >= to) return false;
    }
    return true;
}

fn accumulateNumericJsonValue(
    value: std.json.Value,
    sum: *f64,
    sum_squares: *f64,
    count: *i64,
    min_value: *f64,
    max_value: *f64,
) void {
    switch (value) {
        .array => |arr| for (arr.items) |item| {
            accumulateNumericJsonValue(item, sum, sum_squares, count, min_value, max_value);
        },
        else => if (jsonValueToF64(value)) |numeric| {
            sum.* += numeric;
            sum_squares.* += numeric * numeric;
            count.* += 1;
            if (numeric < min_value.*) min_value.* = numeric;
            if (numeric > max_value.*) max_value.* = numeric;
        },
    }
}

fn collectCardinalityValues(alloc: Allocator, seen: *std.StringHashMap(void), value: std.json.Value) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try collectCardinalityValues(alloc, seen, item);
        },
        else => {
            const key = try stringifyJsonValueCompact(alloc, value);
            errdefer alloc.free(key);
            const entry = try seen.getOrPut(key);
            if (entry.found_existing) {
                alloc.free(key);
            } else {
                entry.key_ptr.* = key;
                entry.value_ptr.* = {};
            }
        },
    }
}

fn appendTermAggregationValuesZig(
    alloc: Allocator,
    counts: *std.StringHashMap(i64),
    grouped: *std.StringHashMap(std.ArrayListUnmanaged(db_mod.types.SearchHit)),
    hit: db_mod.types.SearchHit,
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try appendTermAggregationValuesZig(alloc, counts, grouped, hit, item);
        },
        else => {
            const key = try jsonValueToTermKey(alloc, value);
            defer alloc.free(key);

            const count_entry = try counts.getOrPut(key);
            if (count_entry.found_existing) {
                count_entry.value_ptr.* += 1;
            } else {
                count_entry.key_ptr.* = try alloc.dupe(u8, key);
                count_entry.value_ptr.* = 1;
            }

            const group_entry = try grouped.getOrPut(count_entry.key_ptr.*);
            if (!group_entry.found_existing) group_entry.value_ptr.* = .empty;
            try group_entry.value_ptr.append(alloc, hit);
        },
    }
}

fn jsonValueToTermKey(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => try alloc.dupe(u8, value.string),
        .bool => if (value.bool) try alloc.dupe(u8, "true") else try alloc.dupe(u8, "false"),
        .integer => try std.fmt.allocPrint(alloc, "{d}", .{value.integer}),
        .float => try std.fmt.allocPrint(alloc, "{d}", .{value.float}),
        .number_string => try alloc.dupe(u8, value.number_string),
        .null => try alloc.dupe(u8, "null"),
        else => try stringifyJsonValueCompact(alloc, value),
    };
}

fn stringifyJsonValueCompact(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn distanceToMeters(value: f64, unit: []const u8) !f64 {
    if (unit.len == 0 or std.mem.eql(u8, unit, "m") or std.mem.eql(u8, unit, "meter") or std.mem.eql(u8, unit, "meters")) {
        return value;
    }
    if (std.mem.eql(u8, unit, "km") or std.mem.eql(u8, unit, "kilometer") or std.mem.eql(u8, unit, "kilometers")) {
        return value * 1000.0;
    }
    if (std.mem.eql(u8, unit, "mi") or std.mem.eql(u8, unit, "mile") or std.mem.eql(u8, unit, "miles")) {
        return value * 1609.344;
    }
    if (std.mem.eql(u8, unit, "ft") or std.mem.eql(u8, unit, "foot") or std.mem.eql(u8, unit, "feet")) {
        return value * 0.3048;
    }
    return error.UnsupportedAggregation;
}

fn extractGeoPointFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?geo_mod.GeoPoint {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .object => |obj| blk: {
            const lat_value = obj.get("lat") orelse break :blk null;
            const lon_value = obj.get("lon") orelse break :blk null;
            const lat = jsonValueToF64(lat_value) orelse break :blk null;
            const lon = jsonValueToF64(lon_value) orelse break :blk null;
            break :blk .{ .lat = lat, .lon = lon };
        },
        else => null,
    };
}

fn jsonValueToF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch null,
        else => null,
    };
}

fn fillHistogramBucketKeys(alloc: Allocator, first_key: i64, last_key: i64) ![]i64 {
    if (last_key < first_key) return &.{};
    const len: usize = @intCast(last_key - first_key + 1);
    const keys = try alloc.alloc(i64, len);
    for (keys, 0..) |*slot, idx| {
        slot.* = first_key + @as(i64, @intCast(idx));
    }
    return keys;
}

fn fillDateHistogramBucketKeys(
    alloc: Allocator,
    first_key: u64,
    last_key: u64,
    interval: search_agg_mod.DateInterval,
) ![]u64 {
    var keys: std.ArrayList(u64) = .empty;
    errdefer keys.deinit(alloc);

    var current = first_key;
    while (current <= last_key) {
        try keys.append(alloc, current);
        const next = try nextDateHistogramBucketKey(current, interval);
        if (next <= current) break;
        current = next;
    }
    return keys.toOwnedSlice(alloc);
}

fn nextDateHistogramBucketKey(current: u64, interval: search_agg_mod.DateInterval) !u64 {
    return switch (interval) {
        .minute => current + 60 * std.time.ns_per_s,
        .hour => current + std.time.ns_per_hour,
        .day => current + std.time.ns_per_day,
        .week => current + 7 * std.time.ns_per_day,
        .month => try addCalendarMonths(current, 1),
        .year => try addCalendarYears(current, 1),
    };
}

fn addCalendarMonths(current: u64, delta_months: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    const month_index = (civil.year * 12 + (civil.month - 1)) + delta_months;
    var year = @divFloor(month_index, 12);
    var month = @mod(month_index, 12) + 1;
    if (month <= 0) {
        month += 12;
        year -= 1;
    }
    return civilDateToBucketNs(year, month, 1);
}

fn addCalendarYears(current: u64, delta_years: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    return civilDateToBucketNs(civil.year + delta_years, 1, 1);
}

fn civilDateToBucketNs(year: i64, month: i64, day: i64) !u64 {
    const days = daysFromCivil(year, month, day);
    if (days < 0) return error.InvalidAggregation;
    return @as(u64, @intCast(days)) * std.time.ns_per_day;
}

fn extractNumericFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?f64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch null,
        else => null,
    };
}

fn extractTimestampFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?u64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .integer => @intCast(value.integer),
        .float => @intFromFloat(value.float),
        .number_string => std.fmt.parseInt(u64, value.number_string, 10) catch null,
        .string => try parseRfc3339ToNs(value.string),
        else => null,
    };
}

fn parseDateInterval(request: JsonSearchAggregationRequest) !search_agg_mod.DateInterval {
    const value = if (request.calendar_interval.len > 0) request.calendar_interval else request.fixed_interval;
    if (std.mem.eql(u8, value, "minute") or std.mem.eql(u8, value, "1m")) return .minute;
    if (std.mem.eql(u8, value, "hour") or std.mem.eql(u8, value, "1h")) return .hour;
    if (std.mem.eql(u8, value, "day") or std.mem.eql(u8, value, "1d")) return .day;
    if (std.mem.eql(u8, value, "week") or std.mem.eql(u8, value, "1w")) return .week;
    if (std.mem.eql(u8, value, "month")) return .month;
    if (std.mem.eql(u8, value, "year")) return .year;
    return error.UnsupportedAggregation;
}

fn parseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn formatRfc3339Bucket(alloc: Allocator, ns: u64) ![]const u8 {
    const total_seconds: u64 = @intCast(@divFloor(ns, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const secs_of_day: u64 = total_seconds % 86_400;
    const civil = civilFromDays(days);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    return try std.fmt.allocPrint(alloc, "{:0>4}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}Z", .{
        @as(u64, @intCast(civil.year)),
        @as(u64, @intCast(civil.month)),
        @as(u64, @intCast(civil.day)),
        hour,
        minute,
        second,
    });
}

fn civilFromDays(days_since_epoch: i64) struct { year: i64, month: i64, day: i64 } {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    var y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    y += if (m <= 2) @as(i64, 1) else @as(i64, 0);
    return .{ .year = y, .month = m, .day = d };
}

fn extractValueAtPath(root: std.json.Value, field_path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, field_path, '.');
    while (parts.next()) |part| {
        switch (current) {
            .object => |obj| {
                current = obj.get(part) orelse return null;
            },
            else => return null,
        }
    }
    return current;
}

pub export fn antfly_db_add_index_json(
    handle_ptr: ?*anyopaque,
    config_json: capi.Slice,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        name: []const u8,
        kind: []const u8,
        config_json: []const u8,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, config_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const kind: db_mod.types.IndexKind = if (std.mem.eql(u8, parsed.value.kind, "full_text"))
        .full_text
    else if (std.mem.eql(u8, parsed.value.kind, "graph"))
        .graph
    else if (std.mem.eql(u8, parsed.value.kind, "dense_vector"))
        .dense_vector
    else if (std.mem.eql(u8, parsed.value.kind, "sparse_vector"))
        .sparse_vector
    else if (std.mem.eql(u8, parsed.value.kind, "algebraic"))
        .algebraic
    else
        return .invalid_argument;
    // A native Lite handle also accepts the server's nested "enrichments"
    // shape on this index's own config (see `registerLiteIndexEnrichments`),
    // registering every declared producer before the index below is admitted
    // so an artifact-sourced `sources`/`embedding_name` reference already
    // resolves. The server's atomic table-create request has no such
    // pre-admission catalog mutation, so this call carries its own undo: a
    // rejected admission (IndexAlreadyExists, invalid config) or a partial
    // enrichment/resolver registration restores the pre-call enrichment
    // catalog instead of leaving durably changed producers behind a caller
    // who was told the AddIndex failed.
    var rollback: ?LiteCatalogRollback = null;
    defer if (rollback) |*undo| undo.deinit();
    if (handle.lite_profile == .native) {
        rollback = LiteCatalogRollback.init(handle) catch |err| return capi.mapError(err);
        registerLiteIndexEnrichments(handle, parsed.value.config_json, &rollback.?) catch |err| {
            rollback.?.restore();
            return capi.mapError(err);
        };
    }
    // Only a native Lite handle calls `db.addIndex` directly with a raw
    // public-shaped dense/sparse config; the server always translates first
    // (see `litePhysicalIndexConfigJson`). Every other handle keeps calling
    // `db.addIndex` with exactly the config it was given, unchanged.
    const stored_config_json = if (handle.lite_profile == .native)
        litePhysicalIndexConfigJson(handle.alloc, kind, parsed.value.name, parsed.value.config_json, handle.liteAntflyProvider()) catch |err| {
            if (rollback) |*undo| undo.restore();
            return capi.mapError(err);
        }
    else
        parsed.value.config_json;
    defer if (handle.lite_profile == .native) handle.alloc.free(@constCast(stored_config_json));
    handle.db.addIndex(.{
        .name = parsed.value.name,
        .kind = kind,
        .config_json = stored_config_json,
    }) catch |err| {
        if (rollback) |*undo| undo.restore();
        return capi.mapError(err);
    };
    // A native handle also registers the entity resolvers a graph config
    // declares inline, after the index (and its source artifact's enrichment)
    // is admitted, the way the server's provisioner runs ensureResolvers
    // after index reconciliation. A resolver registration failure unwinds
    // the just-admitted index and the enrichment catalog so the call is
    // all-or-nothing.
    if (handle.lite_profile == .native and kind == .graph) {
        registerLiteIndexResolvers(handle, parsed.value.config_json, &rollback.?) catch |err| {
            _ = handle.db.deleteIndex(parsed.value.name) catch |delete_err| {
                std.log.warn("lite AddIndex rollback failed to remove index {s}: {s}", .{ parsed.value.name, @errorName(delete_err) });
            };
            if (rollback) |*undo| undo.restore();
            return capi.mapError(err);
        };
    }
    refreshLiteManagedEmbeddingRuntime(handle) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_delete_index(
    handle_ptr: ?*anyopaque,
    name: capi.Slice,
    out_deleted: ?*bool,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const out = out_deleted orelse return .invalid_argument;
    out.* = false;
    const handle = guard.handle;
    out.* = handle.db.deleteIndex(name.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_add_enrichment_json(
    handle_ptr: ?*anyopaque,
    config_json: capi.Slice,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    var parsed = std.json.parseFromSlice(db_mod.types.EnrichmentConfig, handle.alloc, config_json.bytes(), .{
        .ignore_unknown_fields = true,
    }) catch return .invalid_argument;
    defer parsed.deinit();
    handle.db.addEnrichment(parsed.value) catch |err| return capi.mapError(err);
    refreshLiteManagedEmbeddingRuntime(handle) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_delete_enrichment(
    handle_ptr: ?*anyopaque,
    kind_slice: capi.Slice,
    name: capi.Slice,
    out_deleted: ?*bool,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const out = out_deleted orelse return .invalid_argument;
    out.* = false;
    const handle = guard.handle;
    const kind = parseEnrichmentKind(kind_slice.bytes()) orelse return .invalid_argument;
    out.* = handle.db.deleteEnrichment(kind, name.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_edges_json(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    key: capi.Slice,
    edge_type: capi.Slice,
    direction: u8,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const dir: db_mod.types.GraphEdgeDirection = switch (direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const edges = handle.db.getEdges(handle.alloc, index_name.bytes(), key.bytes(), edge_type.bytes(), dir) catch |err| return capi.mapError(err);
    defer graphFreeEdges(handle.alloc, edges);
    var payload = handle.alloc.alloc(JsonEdge, edges.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (edges, 0..) |edge, i| {
        payload[i] = JsonEdge.init(handle.alloc, edge) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

test "capi get edges json does not double free a non-empty edge slice" {
    // Regression test for graphFreeEdges double-freeing the edges slice
    // GraphIndex.freeEdges already frees (antfly_db_get_edges_json's only
    // caller). std.testing.allocator (a GeneralPurposeAllocator) detects a
    // double free immediately, so this test would have failed loudly before
    // the fix -- the bug otherwise only corrupted the libc heap used by
    // production builds, manifesting later as an unrelated SIGABRT with no
    // panic message. An empty-result query (before any edges exist) freed a
    // zero-length slice, which many allocators no-op, so it never caught this.
    // Exercise the edge cleanup helper with the testing allocator as well as
    // the public C ABI, which uses the C allocator for its handle and results.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-get-edges-double-free");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const index_config = "{\"name\":\"gr_edges_v1\",\"kind\":\"graph\",\"config_json\":\"{}\"}";
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle_ptr, .{
        .ptr = index_config.ptr,
        .len = index_config.len,
    }));

    // Before any edges exist, getEdges returns an empty slice: freeing it
    // twice never crashed, which is exactly why this bug went unnoticed.
    var empty_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_edges_json(handle_ptr, .{
        .ptr = "gr_edges_v1",
        .len = "gr_edges_v1".len,
    }, .{ .ptr = "doc:edge-source", .len = "doc:edge-source".len }, .{}, 2, &empty_out));
    freeRawBuffer(empty_out.ptr, empty_out.len);

    const source_doc =
        \\{"title":"source","_edges":{"gr_edges_v1":{"links":[{"target":"doc:edge-target","weight":1.0}]}}}
    ;
    const batch_json = "{\"inserts\":{\"doc:edge-source\":" ++ source_doc ++ ",\"doc:edge-target\":{\"title\":\"target\"}},\"sync_level\":\"write\"}";
    var batch_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle_ptr, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_out));
    defer freeRawBuffer(batch_out.ptr, batch_out.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle_ptr));

    // Now getEdges returns one real edge. Freeing that non-empty slice twice
    // is a real heap corruption that std.testing.allocator catches.
    {
        const edges = try asHandle(handle_ptr).?.db.getEdges(alloc, "gr_edges_v1", "doc:edge-source", "", .both);
        defer graphFreeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }

    // Read the same non-empty result through the public C ABI.
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_edges_json(handle_ptr, .{
        .ptr = "gr_edges_v1",
        .len = "gr_edges_v1".len,
    }, .{ .ptr = "doc:edge-source", .len = "doc:edge-source".len }, .{}, 2, &out));
    defer freeRawBuffer(out.ptr, out.len);
    try std.testing.expect(out.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "edge_type") != null);
}

pub export fn antfly_db_traverse_edges_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        index_name: []const u8,
        start_key_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        direction: u8 = 0,
        max_depth: u32 = 3,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        max_results: u32 = 100,
        deduplicate_nodes: bool = true,
        include_paths: bool = false,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const start_key = decodeBase64Alloc(handle.alloc, parsed.value.start_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(start_key);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const results = handle.db.traverseEdges(handle.alloc, parsed.value.index_name, start_key, .{
        .edge_types = parsed.value.edge_types,
        .direction = direction,
        .max_depth = parsed.value.max_depth,
        .min_weight = legacyGraphWeightBound(parsed.value.min_weight),
        .max_weight = legacyGraphWeightBound(parsed.value.max_weight),
        .max_results = parsed.value.max_results,
        .deduplicate = parsed.value.deduplicate_nodes,
        .include_paths = parsed.value.include_paths,
    }) catch |err| return capi.mapError(err);
    defer traversalFreeResults(handle.alloc, results);
    var payload = handle.alloc.alloc(JsonTraversalResult, results.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |item, i| {
        payload[i] = JsonTraversalResult.init(handle.alloc, item) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_get_neighbors_json(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    key: capi.Slice,
    edge_type: capi.Slice,
    direction: u8,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const dir: db_mod.types.GraphEdgeDirection = switch (direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const results = handle.db.getNeighbors(handle.alloc, index_name.bytes(), key.bytes(), edge_type.bytes(), dir) catch |err| return capi.mapError(err);
    defer traversalFreeResults(handle.alloc, results);
    var payload = handle.alloc.alloc(JsonTraversalResult, results.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |item, i| {
        payload[i] = JsonTraversalResult.init(handle.alloc, item) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_find_shortest_path_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        index_name: []const u8,
        source_b64: []const u8,
        target_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        direction: u8 = 0,
        weight_mode: []const u8 = "min_hops",
        max_depth: u32 = 50,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const source = decodeBase64Alloc(handle.alloc, parsed.value.source_b64) catch return .invalid_argument;
    defer handle.alloc.free(source);
    const target = decodeBase64Alloc(handle.alloc, parsed.value.target_b64) catch return .invalid_argument;
    defer handle.alloc.free(target);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const weight_mode: db_mod.types.GraphPathWeightMode = if (std.mem.eql(u8, parsed.value.weight_mode, "min_weight"))
        .min_weight
    else if (std.mem.eql(u8, parsed.value.weight_mode, "max_weight"))
        .max_weight
    else
        .min_hops;
    const maybe_path = handle.db.findShortestPath(handle.alloc, parsed.value.index_name, source, target, parsed.value.edge_types, direction, weight_mode, parsed.value.max_depth, legacyGraphWeightBound(parsed.value.min_weight), legacyGraphWeightBound(parsed.value.max_weight)) catch |err| return capi.mapError(err);
    if (maybe_path == null) return .not_found;
    var payload = JsonPath.init(handle.alloc, maybe_path.?) catch return .internal;
    defer payload.deinit(handle.alloc);
    defer paths_mod.freePath(handle.alloc, maybe_path.?);
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_find_k_shortest_paths_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        index_name: []const u8,
        source_b64: []const u8,
        target_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        direction: u8 = 0,
        weight_mode: []const u8 = "min_hops",
        max_depth: u32 = 50,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        k: u32 = 1,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const source = decodeBase64Alloc(handle.alloc, parsed.value.source_b64) catch return .invalid_argument;
    defer handle.alloc.free(source);
    const target = decodeBase64Alloc(handle.alloc, parsed.value.target_b64) catch return .invalid_argument;
    defer handle.alloc.free(target);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const weight_mode: db_mod.types.GraphPathWeightMode = if (std.mem.eql(u8, parsed.value.weight_mode, "min_weight"))
        .min_weight
    else if (std.mem.eql(u8, parsed.value.weight_mode, "max_weight"))
        .max_weight
    else
        .min_hops;
    const paths = handle.db.findKShortestPaths(handle.alloc, parsed.value.index_name, source, target, parsed.value.k, parsed.value.edge_types, direction, weight_mode, parsed.value.max_depth, legacyGraphWeightBound(parsed.value.min_weight), legacyGraphWeightBound(parsed.value.max_weight)) catch |err| return capi.mapError(err);
    defer paths_mod.freePaths(handle.alloc, paths);
    var payload = handle.alloc.alloc(JsonPath, paths.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (paths, 0..) |path, i| {
        payload[i] = JsonPath.init(handle.alloc, path) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_match_pattern_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const JsonPatternNodeFilter = struct {
        filter_prefix: []const u8 = "",
        query_json: []const u8 = "",
    };
    const JsonPatternEdgeStep = struct {
        direction: u8 = 0,
        min_hops: u32 = 1,
        max_hops: u32 = 1,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        types: []const []const u8 = &.{},
    };
    const JsonPatternStep = struct {
        alias: []const u8 = "",
        edge: JsonPatternEdgeStep = .{},
        node_filter: JsonPatternNodeFilter = .{},
    };
    const Request = struct {
        index_name: []const u8,
        start_nodes_b64: []const []const u8,
        pattern: []const JsonPatternStep,
        max_results: u32 = 100,
        return_aliases: []const []const u8 = &.{},
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    var start_nodes = handle.alloc.alloc([]const u8, parsed.value.start_nodes_b64.len) catch return .internal;
    defer {
        for (start_nodes) |entry| handle.alloc.free(entry);
        if (start_nodes.len > 0) handle.alloc.free(start_nodes);
    }
    var start_count: usize = 0;
    errdefer {
        for (start_nodes[0..start_count]) |entry| handle.alloc.free(entry);
    }
    for (parsed.value.start_nodes_b64, 0..) |item, i| {
        start_nodes[i] = decodeBase64Alloc(handle.alloc, item) catch return .invalid_argument;
        start_count += 1;
    }

    var pattern = handle.alloc.alloc(graph_pattern_mod.PatternStep, parsed.value.pattern.len) catch return .internal;
    defer handle.alloc.free(pattern);
    for (parsed.value.pattern, 0..) |step, i| {
        const direction: db_mod.types.GraphEdgeDirection = switch (step.edge.direction) {
            0 => .out,
            1 => .in,
            2 => .both,
            else => return .invalid_argument,
        };
        pattern[i] = .{
            .alias = step.alias,
            .edge = .{
                .direction = direction,
                .min_hops = step.edge.min_hops,
                .max_hops = step.edge.max_hops,
                .min_weight = legacyGraphWeightBound(step.edge.min_weight),
                .max_weight = legacyGraphWeightBound(step.edge.max_weight),
                .types = step.edge.types,
            },
            .node_filter = .{
                .filter_prefix = step.node_filter.filter_prefix,
                .filter_query_json = if (step.node_filter.query_json.len == 0) null else step.node_filter.query_json,
            },
        };
    }

    const matches = handle.db.matchPattern(handle.alloc, parsed.value.index_name, start_nodes, pattern, parsed.value.max_results, parsed.value.return_aliases) catch |err| return capi.mapError(err);
    defer graph_pattern_mod.freeMatches(handle.alloc, matches);

    var payload = handle.alloc.alloc(JsonPatternMatch, matches.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (matches, 0..) |match, i| {
        payload[i] = JsonPatternMatch.init(handle.alloc, match) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_create_shadow_index_manager(
    handle_ptr: ?*anyopaque,
    split_key: capi.Slice,
    original_range_end: capi.Slice,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.createShadowIndexManager(split_key.bytes(), original_range_end.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_close_shadow_index_manager(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.closeShadowIndexManager() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_shadow_index_dir(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const dir = handle.db.getShadowIndexDir();
    if (dir.len == 0) return .not_found;
    out_buf.* = dupBytes(dir) catch return .internal;
    return .ok;
}

pub export fn antfly_db_find_median_key(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .read) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    const key = handle.db.findMedianKey(handle.alloc) catch |err| return capi.mapError(err);
    defer handle.alloc.free(key);
    out_buf.* = dupBytes(key) catch return .internal;
    return .ok;
}

pub export fn antfly_db_split(
    handle_ptr: ?*anyopaque,
    curr_start: capi.Slice,
    curr_end: capi.Slice,
    split_key: capi.Slice,
    dest_dir1: capi.Slice,
    dest_dir2: capi.Slice,
    prepare_only: bool,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.split(
        .{
            .start = curr_start.bytes(),
            .end = curr_end.bytes(),
        },
        split_key.bytes(),
        dest_dir1.bytes(),
        dest_dir2.bytes(),
        prepare_only,
    ) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_finalize_split(
    handle_ptr: ?*anyopaque,
    new_start: capi.Slice,
    new_end: capi.Slice,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    handle.db.finalizeSplit(.{
        .start = new_start.bytes(),
        .end = new_end.bytes(),
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_snapshot(
    handle_ptr: ?*anyopaque,
    id: capi.Slice,
    out_size: *u64,
) capi.ErrorCode {
    const guard = enterHandle(handle_ptr, .write) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    out_size.* = handle.db.snapshot(id.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

test "capi transaction lifecycle" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const txn_id: [16]u8 = .{ 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_begin_transaction_with_id(handle_ptr, null, 1_000, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_write_transaction(handle_ptr, null, null, 0, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_resolve_intents(handle_ptr, null, @intFromEnum(transactions_mod.TxnStatus.committed), 2_000));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_transaction_status(handle_ptr, &txn_id, null));
    var reset_status: u8 = 99;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_transaction_status(handle_ptr, null, &reset_status));
    try std.testing.expectEqual(@as(u8, 0), reset_status);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_commit_version(handle_ptr, &txn_id, null));
    var reset_commit_version: u64 = 99;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_commit_version(handle_ptr, null, &reset_commit_version));
    try std.testing.expectEqual(@as(u64, 0), reset_commit_version);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_begin_transaction_with_id(handle_ptr, &txn_id, 1_000, null, 0));

    const writes = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi", .len = "doc:capi".len },
            .value = .{ .ptr = "{\"title\":\"ok\"}", .len = "{\"title\":\"ok\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_write_transaction(handle_ptr, &txn_id, &writes, writes.len, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_resolve_intents(handle_ptr, &txn_id, @intFromEnum(transactions_mod.TxnStatus.committed), 2_000));

    var status: u8 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_transaction_status(handle_ptr, &txn_id, &status));
    try std.testing.expectEqual(@as(u8, @intFromEnum(transactions_mod.TxnStatus.committed)), status);

    var commit_version: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_commit_version(handle_ptr, &txn_id, &commit_version));
    try std.testing.expectEqual(@as(u64, 2_000), commit_version);
}

test "capi SQL document mutations preserve undeclared fields and typed null semantics" {
    var directory = try TestDirectory.init("capi-sql-document");
    defer directory.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, directory.path(), "document");
    defer alloc.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    const schema =
        \\{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"j":{"default":{"annotated":true}}},"additionalProperties":true}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(schema)));
    var inserted: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .fromSlice("{\"inserts\":{\"a\":{\"n\":9007199254740993,\"extra\":true}}}"), &inserted));
    defer antfly_db_buffer_free(inserted.ptr, inserted.len);
    var response: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice("items"), .fromSlice("{\"statement\":\"SELECT n FROM items WHERE _id='a'\"}"), &response));
    defer antfly_db_buffer_free(response.ptr, response.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.ptr.?[0..response.len], .{});
    defer parsed.deinit();
    const rows = parsed.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("9007199254740993", rows[0].array.items[0].string);
    for ([_][]const u8{
        "UPDATE items SET n=n+1, j='null' WHERE _id='a' RETURNING n,j",
        "INSERT INTO items (_id,n,j) VALUES ('b',2,NULL) RETURNING n,j",
        "INSERT INTO items (_id,n) VALUES ('c',2) RETURNING j",
        "UPDATE items SET j=NULL WHERE _id='a' RETURNING j",
    }) |statement| {
        const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
        defer alloc.free(body);
        var updated: capi.Buffer = .{};
        const status = antfly_db_sql_json(handle, .fromSlice("items"), .fromSlice(body), &updated);
        defer antfly_db_buffer_free(updated.ptr, updated.len);
        if (status != .ok) std.debug.print("document mutation error: {s}\n", .{updated.ptr.?[0..updated.len]});
        try std.testing.expectEqual(capi.ErrorCode.ok, status);
    }
    var stored: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle, .fromSlice("a"), &stored));
    defer antfly_db_buffer_free(stored.ptr, stored.len);
    const document = try std.json.parseFromSlice(std.json.Value, alloc, stored.ptr.?[0..stored.len], .{});
    defer document.deinit();
    try std.testing.expectEqual(@as(i64, 9007199254740994), document.value.object.get("n").?.integer);
    try std.testing.expect(document.value.object.get("extra").?.bool);
    try std.testing.expect(!document.value.object.contains("j"));
    // JSON Schema defaults are annotations, not SQL column defaults. Both
    // omitted JSON and explicit SQL NULL stay absent under native semantics.
    for ([_][]const u8{ "b", "c" }) |key| {
        var row: capi.Buffer = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle, .fromSlice(key), &row));
        defer antfly_db_buffer_free(row.ptr, row.len);
        const value = try std.json.parseFromSlice(std.json.Value, alloc, row.ptr.?[0..row.len], .{});
        defer value.deinit();
        try std.testing.expect(!value.value.object.contains("j"));
    }
    var deleted: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice("items"), .fromSlice("{\"statement\":\"DELETE FROM items WHERE _id='a' RETURNING n\"}"), &deleted));
    defer antfly_db_buffer_free(deleted.ptr, deleted.len);
}

test "capi SQL document validation rejects an entire mutation before publication" {
    var directory = try TestDirectory.init("capi-sql-document-validation");
    defer directory.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, directory.path(), "document");
    defer alloc.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(
        \\{"version":1,"enforce_types":true,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","minimum":0}},"required":["n"],"additionalProperties":true}}}}
    )));
    var response: capi.Buffer = .{};
    const status = antfly_db_sql_json(handle, .fromSlice("items"), .fromSlice("{\"statement\":\"INSERT INTO items (_id,n) VALUES ('valid',1),('invalid',-1) RETURNING n\"}"), &response);
    defer antfly_db_buffer_free(response.ptr, response.len);
    try std.testing.expect(status != .ok);
    var count: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice("items"), .fromSlice("{\"statement\":\"SELECT COUNT(*) FROM items\"}"), &count));
    defer antfly_db_buffer_free(count.ptr, count.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, count.ptr.?[0..count.len], .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("0", parsed.value.object.get("rows").?.array.items[0].array.items[0].string);
}

test "capi SQL mutations fence exact primary bytes with unchanged TTL and schema epochs" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "document", "relational" }) |mode| {
        var directory = try TestDirectory.init("capi-sql-digest-fence");
        defer directory.cleanup();
        var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer database.close();
        const schema = try std.fmt.allocPrint(alloc,
            \\{{"version":1,"storage_mode":"{s}","default_type":"row","ttl":{{"duration":"1s","field":"expires"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"n":{{"type":"integer"}},"expires":{{"type":"datetime"}}}},"additionalProperties":false}}}}}}}}
        , .{mode});
        defer alloc.free(schema);
        try database.setSchemaJson(alloc, schema);
        try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1,\"expires\":\"2090-01-01T00:00:00Z\"}" }} });
        var adapter = @import("sql.zig").Adapter(antfly){ .db = &database, .table_name = "items" };
        const backend = adapter.backend();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const table = try backend.vtable.resolve(backend.ptr, arena.allocator(), .{ .table = "items" }, .read_write);
        const cursor = (try backend.vtable.open_scan.?(backend.ptr, alloc, table, .{ .fields = &.{"n"}, .include_primary_digest = true, .primary_key = "a", .limit = 1 })).?;
        defer cursor.close(cursor.ptr);
        const page = try cursor.next(cursor.ptr, alloc, 1);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        const before = page.rows[0];
        try std.testing.expect(before.expected_content_digest != null);
        try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":2,\"expires\":\"2090-01-01T00:00:00Z\"}" }} });
        try std.testing.expectEqual(before.version, try database.getTimestamp(alloc, "a"));
        try std.testing.expectError(error.SqlWriteConflict, backend.vtable.mutate(backend.ptr, arena.allocator(), table, &.{.{ .key = "a", .expected_version = before.version, .expected_content_digest = before.expected_content_digest, .row = null }}));
        try std.testing.expectError(error.PreparedGenerationChanged, database.batch(.{ .schema_version = 2, .deletes = &.{"a"} }));
        const txn = try database.beginTransactionWithId(@splat(97), 1);
        try std.testing.expectError(error.PreparedGenerationChanged, database.writeTransaction(txn, .{ .schema_version = 2, .deletes = &.{"a"} }));
        try database.abortTransaction(txn, 2);
        var retained = (try database.lookup(alloc, "a", .{})).?;
        defer retained.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, retained.json, "\"n\":2") != null);
    }
}

test "capi SQL local integrity coordinator enforces unique arbitration and self FK actions" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-integrity");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 501, .shard_id = 502 } });
    defer database.close();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"parent_fk","child_columns":["parent"],"parent_table":"rows","parent_columns":["id"],"on_delete":"cascade","on_update":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    try database.setSchemaJson(alloc, schema);
    var adapter = @import("sql.zig").Adapter(antfly){ .db = &database, .table_name = "rows" };
    const sql = @import("sql.zig");
    for ([_]struct { statement: []const u8, count: u64 }{
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('p',1,NULL),('c',2,1)", .count = 2 },
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('skipped',1,NULL) ON CONFLICT (id) DO NOTHING RETURNING id", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('skipped',1,NULL),('also_skipped',1,NULL) ON CONFLICT DO NOTHING RETURNING id", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('new',1,NULL) ON CONFLICT (id) DO UPDATE SET id=3 RETURNING id", .count = 1 },
    }) |case| {
        var compiled = try sql.compiler.compile(alloc, case.statement, .{});
        defer compiled.deinit();
        var result = try sql.runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.count, result.output.rows_affected);
    }
    var child = (try database.lookup(alloc, "c", .{})).?;
    defer child.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, child.json, "\"parent\":3") != null);
    var removed = try sql.compiler.compile(alloc, "DELETE FROM rows WHERE _id='p'", .{});
    defer removed.deinit();
    var outcome = try sql.runtime.execute(alloc, adapter.backend(), &removed, &.{}, .{});
    defer outcome.deinit();
    try std.testing.expect(try database.lookup(alloc, "c", .{}) == null);
    try std.testing.expect(try database.lookup(alloc, "p", .{}) == null);
    try std.testing.expect(try database.lookup(alloc, "new", .{}) == null);
    var orphan = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,id,parent) VALUES ('orphan',4,999)", .{});
    defer orphan.deinit();
    try std.testing.expectError(error.ForeignKeyParentMissing, sql.runtime.execute(alloc, adapter.backend(), &orphan, &.{}, .{}));
    try std.testing.expect(try database.lookup(alloc, "orphan", .{}) == null);
    // An absent secondary claim is a commit predicate, not permission to
    // overwrite whichever owner appears after conflict resolution.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const backend = adapter.backend();
    const table = try backend.vtable.resolve(backend.ptr, arena.allocator(), .{ .table = "rows" }, .read_write);
    const row = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"id\":4,\"parent\":null}", .{});
    var mutation: antfly.capi_dependencies.sql_catalog.Mutation = .{ .key = "racer", .row = row, .expected_version = 0 };
    const owners = try backend.vtable.resolve_conflict_owners.?(backend.ptr, arena.allocator(), table, &.{"id"}, &.{}, &.{}, &.{mutation});
    try std.testing.expect(owners[0].key == null);
    mutation.conflict_guard = owners[0].guard;
    var winner = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,id,parent) VALUES ('winner',4,NULL)", .{});
    defer winner.deinit();
    var won = try sql.runtime.execute(alloc, backend, &winner, &.{}, .{});
    defer won.deinit();
    try std.testing.expectError(error.PreparedReadSetChanged, backend.vtable.mutate(backend.ptr, arena.allocator(), table, &.{mutation}));
    try std.testing.expect(try database.lookup(alloc, "racer", .{}) == null);
}

test "capi SQL native expression partial unique claims reject collisions and arbitrate targetless inserts" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-expression-unique");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 601, .shard_id = 602 } });
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"email_key","keys":[{"expression":{"op":"lower_ascii","args":[{"op":"column","column":"email"}]},"result_type":"string"}],"where":[{"column":"active","op":"eq","value":true}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"active":{"type":"boolean"}},"additionalProperties":false}}}}
    );
    var adapter = @import("sql.zig").Adapter(antfly){ .db = &database, .table_name = "rows" };
    const sql = @import("sql.zig");
    for ([_]struct { statement: []const u8, count: u64 }{
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('a','Alice',TRUE)", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('skip','ALICE',TRUE) ON CONFLICT DO NOTHING", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('b','ALICE',FALSE) ON CONFLICT DO NOTHING", .count = 1 },
        .{ .statement = "UPDATE rows SET active=FALSE WHERE _id='a'", .count = 1 },
        .{ .statement = "UPDATE rows SET active=TRUE WHERE _id='b'", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('skip2','alice',TRUE),('c','Carol',TRUE),('skip3','CAROL',TRUE) ON CONFLICT DO NOTHING", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('skip4','ALICE',TRUE) ON CONFLICT (lower(email)) WHERE active=TRUE DO NOTHING", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('replace','ALICE',TRUE) ON CONFLICT ((lower(email))) WHERE active=TRUE DO UPDATE SET email='Bob'", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('d','Alice',TRUE) ON CONFLICT (lower(email)) WHERE active=TRUE DO NOTHING", .count = 1 },
    }) |case| {
        var compiled = try sql.compiler.compile(alloc, case.statement, .{});
        defer compiled.deinit();
        var result = try sql.runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.count, result.output.rows_affected);
    }
    var collision = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,email,active) VALUES ('collision','alice',TRUE)", .{});
    defer collision.deinit();
    try std.testing.expectError(error.UniqueConstraintViolation, sql.runtime.execute(alloc, adapter.backend(), &collision, &.{}, .{}));
    try std.testing.expect(try database.lookup(alloc, "collision", .{}) == null);
    try std.testing.expect(try database.lookup(alloc, "skip3", .{}) == null);
    for ([_][]const u8{
        "INSERT INTO rows (_id,email,active) VALUES ('wrong','ALICE',TRUE) ON CONFLICT (upper(email)) WHERE active=TRUE DO NOTHING",
        "INSERT INTO rows (_id,email,active) VALUES ('wrong','ALICE',TRUE) ON CONFLICT (lower(email)) DO NOTHING",
        "INSERT INTO rows (_id,email,active) VALUES ('wrong','ALICE',TRUE) ON CONFLICT (lower(email)) WHERE active=FALSE DO NOTHING",
    }) |statement| {
        var wrong = try sql.compiler.compile(alloc, statement, .{});
        defer wrong.deinit();
        try std.testing.expectError(error.ConflictArbiterNotFound, sql.runtime.execute(alloc, adapter.backend(), &wrong, &.{}, .{}));
    }
}

test "capi SQL local integrity refuses partial ownership instead of inventing coverage" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-partial-integrity");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 503, .shard_id = 504 } });
    defer database.close();
    try database.updateRange(.{ .start = "a", .end = "z" });
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    var adapter = @import("sql.zig").Adapter(antfly){ .db = &database, .table_name = "rows" };
    const sql = @import("sql.zig");
    var compiled = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,id) VALUES ('k',1)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.UnsupportedSqlExecution, sql.runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{}));
    try std.testing.expect(try database.lookup(alloc, "k", .{}) == null);
}

test "capi SQL RETURNING uses native defaults generated values and versioned preimages" {
    var directory = try TestDirectory.init("capi-sql-returning");
    defer directory.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, directory.path(), "returning");
    defer alloc.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"b","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"a"},{"op":"column","column":"b"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"},"total":{"type":"integer"}},"required":["a","b","total"],"additionalProperties":false}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(schema)));
    for ([_]struct { statement: []const u8, expected: []const u8 }{
        .{ .statement = "INSERT INTO items (_id,a) VALUES ('a',3) RETURNING total", .expected = "5" },
        .{ .statement = "INSERT INTO items (_id,a) VALUES ('a',1) ON CONFLICT (_id) DO UPDATE SET a=excluded.a+items.a RETURNING total", .expected = "6" },
        .{ .statement = "INSERT INTO items (_id,a) VALUES ('a',99) ON CONFLICT (_id) DO NOTHING RETURNING total", .expected = "empty" },
        .{ .statement = "INSERT INTO items (_id,a) VALUES ('a',99) ON CONFLICT DO NOTHING RETURNING total", .expected = "empty" },
        .{ .statement = "UPDATE items SET a=4 WHERE _id='a' RETURNING items.total", .expected = "6" },
        .{ .statement = "DELETE FROM items WHERE _id='a' RETURNING total", .expected = "6" },
    }) |case| {
        const request = try std.json.Stringify.valueAlloc(alloc, .{ .statement = case.statement }, .{});
        defer alloc.free(request);
        var response: capi.Buffer = .{};
        const status = antfly_db_sql_json(handle, .fromSlice("items"), .fromSlice(request), &response);
        defer antfly_db_buffer_free(response.ptr, response.len);
        if (status != .ok) std.debug.print("SQL RETURNING native failure: {s}\n", .{response.ptr.?[0..response.len]});
        try std.testing.expectEqual(capi.ErrorCode.ok, status);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.ptr.?[0..response.len], .{});
        defer parsed.deinit();
        const rows = parsed.value.object.get("rows").?.array.items;
        if (std.mem.eql(u8, case.expected, "empty")) {
            try std.testing.expectEqual(@as(usize, 0), rows.len);
            try std.testing.expectEqual(@as(i64, 0), parsed.value.object.get("rows_affected").?.integer);
            continue;
        }
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqualStrings(case.expected, rows[0].array.items[0].string);
    }
    var generated: capi.Buffer = .{};
    const status = antfly_db_sql_json(handle, .fromSlice("items"), .fromSlice("{\"statement\":\"INSERT INTO items (a) VALUES (3),(3) RETURNING _id,total\"}"), &generated);
    defer antfly_db_buffer_free(generated.ptr, generated.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, status);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, generated.ptr.?[0..generated.len], .{});
    defer parsed.deinit();
    const generated_rows = parsed.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), generated_rows.len);
    const first = generated_rows[0].array.items[0].string;
    const second = generated_rows[1].array.items[0].string;
    try std.testing.expectEqual(@as(usize, 32), first.len);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expectEqualStrings("5", generated_rows[0].array.items[1].string);
}

test "capi SQL uses native typed snapshots and atomic mutations" {
    var test_tmp = try TestDirectory.init("capi-sql");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "sql");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"j":{"type":"json"}},"additionalProperties":false}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle_ptr, .fromSlice(schema_json)));
    const requests = [_][]const u8{
        \\{"statement":"INSERT INTO items (_id,id) VALUES ('a',$1)","parameters":["9007199254740993"]}
        ,
        \\{"statement":"SELECT id FROM items WHERE _id='a'"}
        ,
        \\{"statement":"UPDATE items SET id=id+1 WHERE _id='a'"}
        ,
        \\{"statement":"SELECT id FROM items WHERE _id='a'"}
        ,
    };
    for (requests, 0..) |request, index| {
        var out: capi.Buffer = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice("items"), .fromSlice(request), &out));
        defer antfly_db_buffer_free(out.ptr, out.len);
        if (index == 1) try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "9007199254740993") != null);
        if (index == 3) try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "9007199254740994") != null);
    }
    var joined: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice("items"), .fromSlice("{\"statement\":\"SELECT l.id, r.id FROM items AS l JOIN items AS r ON l.id=r.id\"}"), &joined));
    defer antfly_db_buffer_free(joined.ptr, joined.len);
    const joined_json = try std.json.parseFromSlice(std.json.Value, alloc, joined.ptr.?[0..joined.len], .{});
    defer joined_json.deinit();
    const joined_rows = joined_json.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), joined_rows.len);
    try std.testing.expectEqualStrings("9007199254740994", joined_rows[0].array.items[0].string);
    try std.testing.expectEqualStrings("9007199254740994", joined_rows[0].array.items[1].string);
    var inserted_json_null: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice("items"), .fromSlice("{\"statement\":\"INSERT INTO items (_id,id,j) VALUES ('b',2,CAST('null' AS json))\"}"), &inserted_json_null));
    defer antfly_db_buffer_free(inserted_json_null.ptr, inserted_json_null.len);
    var null_projection: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice("items"), .fromSlice("{\"statement\":\"SELECT j FROM items ORDER BY _id\"}"), &null_projection));
    defer antfly_db_buffer_free(null_projection.ptr, null_projection.len);
    const projected = try std.json.parseFromSlice(std.json.Value, alloc, null_projection.ptr.?[0..null_projection.len], .{});
    defer projected.deinit();
    const projected_rows = projected.value.object.get("rows").?.array.items;
    const projected_nulls = projected.value.object.get("sql_nulls").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), projected_rows.len);
    try std.testing.expect(projected_rows[0].array.items[0] == .null);
    try std.testing.expect(projected_rows[1].array.items[0] == .null);
    try std.testing.expect(projected_nulls[0].array.items[0].bool);
    try std.testing.expect(!projected_nulls[1].array.items[0].bool);
    var rejected: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_sql_json(handle_ptr, .fromSlice("items"), .fromSlice("{\"statement\":\"BEGIN\"}"), &rejected));
    defer antfly_db_buffer_free(rejected.ptr, rejected.len);
    try std.testing.expect(std.mem.indexOf(u8, rejected.ptr.?[0..rejected.len], "0A000") != null);
}

test "capi batch and lookup json" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-batch-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const writes = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi-batch", .len = "doc:capi-batch".len },
            .value = .{ .ptr = "{\"title\":\"ok\"}", .len = "{\"title\":\"ok\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(handle_ptr, &writes, writes.len, null, 0, 1_000, 0));

    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle_ptr, .{
        .ptr = "doc:capi-batch",
        .len = "doc:capi-batch".len,
    }, &out));
    defer freeRawBuffer(out.ptr, out.len);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"title\":\"ok\"") != null);

    const batch_json = "{\"inserts\":{\"doc:capi-batch-json\":{\"title\":\"json path\"}},\"sync_level\":\"write\"}";
    var batch_json_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle_ptr, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_json_out));
    defer freeRawBuffer(batch_json_out.ptr, batch_json_out.len);
    try std.testing.expect(std.mem.indexOf(u8, batch_json_out.ptr.?[0..batch_json_out.len], "\"inserted\":1") != null);

    var json_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle_ptr, .{
        .ptr = "doc:capi-batch-json",
        .len = "doc:capi-batch-json".len,
    }, &json_out));
    defer freeRawBuffer(json_out.ptr, json_out.len);
    try std.testing.expect(std.mem.indexOf(u8, json_out.ptr.?[0..json_out.len], "\"title\":\"json path\"") != null);

    var invalid_json_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch_json(handle_ptr, .{
        .ptr = "{".ptr,
        .len = 1,
    }, &invalid_json_out));
}

test "capi lite opens exports imports checks and vacuums aflite" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const plain_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-plain");
    defer alloc.free(plain_path);
    const invalid_lite_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-invalid");
    defer alloc.free(invalid_lite_path);
    const missing_readonly_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-missing-readonly");
    defer alloc.free(missing_readonly_path);
    const missing_status_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-missing-status");
    defer alloc.free(missing_status_path);
    const short_lite_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-short");
    defer alloc.free(short_lite_path);
    const src_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-src");
    defer alloc.free(src_path);
    const remote_inference_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-remote-inference");
    defer alloc.free(remote_inference_path);
    const local_inference_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-local-inference");
    defer alloc.free(local_inference_path);
    const dst_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-dst");
    defer alloc.free(dst_path);
    const bad_dst_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-bad-dst");
    defer alloc.free(bad_dst_path);
    const schema_dst_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-schema-dst");
    defer alloc.free(schema_dst_path);
    const snapshot_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-snapshot");
    defer alloc.free(snapshot_path);
    const snapshot_file_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-snapshot-file");
    defer alloc.free(snapshot_file_path);
    const pinned_snapshot_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-pinned-snapshot");
    defer alloc.free(pinned_snapshot_path);
    const restore_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore");
    defer alloc.free(restore_path);
    const restore_alias_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-alias");
    defer alloc.free(restore_alias_path);
    const restore_unknown_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-outcome-unknown");
    defer alloc.free(restore_unknown_path);
    const locked_restore_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-locked");
    defer alloc.free(locked_restore_path);
    const restore_malformed_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-malformed");
    defer alloc.free(restore_malformed_path);
    const invalid_snapshot_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-snapshot-invalid");
    defer alloc.free(invalid_snapshot_path);
    const invalid_snapshot_file_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-snapshot-file-invalid");
    defer alloc.free(invalid_snapshot_file_path);
    cleanupTestDir(plain_path);
    cleanupTestFile(invalid_lite_path);
    cleanupTestFile(missing_readonly_path);
    cleanupTestFile(missing_status_path);
    cleanupTestFile(short_lite_path);
    cleanupTestFile(src_path);
    cleanupTestFile(remote_inference_path);
    cleanupTestFile(local_inference_path);
    cleanupTestFile(dst_path);
    cleanupTestFile(bad_dst_path);
    cleanupTestFile(schema_dst_path);
    cleanupTestFile(snapshot_path);
    cleanupTestFile(snapshot_file_path);
    cleanupTestFile(pinned_snapshot_path);
    cleanupTestFile(restore_path);
    cleanupTestFile(restore_alias_path);
    cleanupTestFile(restore_unknown_path);
    cleanupTestFile(locked_restore_path);
    cleanupTestFile(restore_malformed_path);
    cleanupTestFile(invalid_snapshot_path);
    cleanupTestFile(invalid_snapshot_file_path);
    defer cleanupTestDir(plain_path);
    defer cleanupTestFile(invalid_lite_path);
    defer cleanupTestFile(missing_readonly_path);
    defer cleanupTestFile(missing_status_path);
    defer cleanupTestFile(short_lite_path);
    defer cleanupTestFile(src_path);
    defer cleanupTestFile(remote_inference_path);
    defer cleanupTestFile(local_inference_path);
    defer cleanupTestFile(dst_path);
    defer cleanupTestFile(bad_dst_path);
    defer cleanupTestFile(schema_dst_path);
    defer cleanupTestFile(snapshot_path);
    defer cleanupTestFile(snapshot_file_path);
    defer cleanupTestFile(pinned_snapshot_path);
    defer cleanupTestFile(restore_path);
    defer cleanupTestFile(restore_alias_path);
    defer cleanupTestFile(restore_unknown_path);
    defer cleanupTestFile(locked_restore_path);
    defer cleanupTestFile(restore_malformed_path);
    defer cleanupTestFile(invalid_snapshot_path);
    defer cleanupTestFile(invalid_snapshot_file_path);

    try std.testing.expectEqual(@as(u32, 2), antfly_abi_version());
    try std.testing.expectEqualStrings("ANTFLY_OK", std.mem.span(antfly_error_code_name(@intFromEnum(capi.ErrorCode.ok))));
    try std.testing.expectEqualStrings("ANTFLY_INVALID_ARGUMENT", std.mem.span(antfly_error_code_name(@intFromEnum(capi.ErrorCode.invalid_argument))));
    try std.testing.expectEqualStrings("ANTFLY_OUTCOME_UNKNOWN", std.mem.span(antfly_error_code_name(@intFromEnum(capi.ErrorCode.outcome_unknown))));
    try std.testing.expectEqualStrings("ANTFLY_UNSUPPORTED", std.mem.span(antfly_error_code_name(@intFromEnum(capi.ErrorCode.unsupported))));
    try std.testing.expectEqualStrings("ANTFLY_UNKNOWN_ERROR", std.mem.span(antfly_error_code_name(12345)));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(antfly_error_code_description(@intFromEnum(capi.ErrorCode.busy))), "retry") != null);
    try std.testing.expectEqualStrings("unknown Antfly error code", std.mem.span(antfly_error_code_description(12345)));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.FileBusy));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.WriterLocked));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.SourceFileChanged));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.PortableRuntimeActivationPending));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, capi.mapError(error.FileLocksUnsupported));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, capi.mapError(error.GenerationFileLocksUnsupported));
    try std.testing.expectEqual(capi.ErrorCode.not_found, capi.mapError(error.NotFound));
    try std.testing.expectEqual(capi.ErrorCode.txn_not_found, capi.mapError(error.TxnNotFound));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.TruncatedNativeHeader));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.UnsupportedNativeFormatVersion));
    try std.testing.expectEqual(capi.ErrorCode.outcome_unknown, capi.mapError(error.DurabilityOutcomeUnknown));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.InvalidBackupManifest));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.BackupArtifactIntegrityMismatch));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open(src_path, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_create(src_path, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(src_path, null, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_create_with_options(src_path, null, null));
    var null_path_sentinel: u8 = 0;
    var null_path_handle: ?*anyopaque = &null_path_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open(null, &null_path_handle));
    try std.testing.expect(null_path_handle == null);

    var plain_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(plain_path, &plain_handle));
    defer antfly_db_close(plain_handle);
    var scratch: [1]u8 = .{0xaa};
    // Status, capabilities, backup, and drains work for directory storage
    // as well as .aflite files.
    var dir_caps: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(plain_handle, &dir_caps));
    try std.testing.expect(std.mem.indexOf(u8, dir_caps.ptr.?[0..dir_caps.len], "\"threading\":\"serialized\"") != null);
    antfly_buffer_free(&dir_caps);
    var dir_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(plain_handle, &dir_status));
    try std.testing.expect(std.mem.indexOf(u8, dir_status.ptr.?[0..dir_status.len], "\"format\":\"directory\"") != null);
    antfly_buffer_free(&dir_status);
    var dir_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(plain_handle, &dir_backup));
    try std.testing.expect(dir_backup.len > 0);
    antfly_buffer_free(&dir_backup);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(plain_handle));
    var dir_idle: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle_json(plain_handle, &dir_idle));
    antfly_buffer_free(&dir_idle);
    var dir_pending: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_pending_work_stats_json(plain_handle, &dir_pending));
    antfly_buffer_free(&dir_pending);
    // A null output buffer is still rejected.
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(plain_handle, null));
    var invalid_lite_sentinel: u8 = 0;
    var invalid_lite_handle: ?*anyopaque = &invalid_lite_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open(invalid_lite_path, &invalid_lite_handle));
    try std.testing.expect(invalid_lite_handle == null);
    defer antfly_db_close(invalid_lite_handle);

    try std.testing.expect(!testPathExists(src_path));
    var missing_writer_sentinel: u8 = 0;
    var missing_writer_handle: ?*anyopaque = &missing_writer_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.not_found, antfly_lite_open(src_path, &missing_writer_handle));
    try std.testing.expect(missing_writer_handle == null);
    try std.testing.expect(!testPathExists(src_path));
    defer antfly_db_close(missing_writer_handle);

    try std.testing.expect(!testPathExists(missing_readonly_path));
    var missing_readonly_sentinel: u8 = 0;
    var missing_readonly_handle: ?*anyopaque = &missing_readonly_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.not_found, antfly_lite_open_readonly(missing_readonly_path, &missing_readonly_handle));
    try std.testing.expect(missing_readonly_handle == null);
    try std.testing.expect(!testPathExists(missing_readonly_path));
    defer antfly_db_close(missing_readonly_handle);

    try std.testing.expect(!testPathExists(missing_status_path));
    var missing_status_sentinel: u8 = 0;
    var missing_status_handle: ?*anyopaque = &missing_status_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.not_found, antfly_lite_open_status_only(missing_status_path, &missing_status_handle));
    try std.testing.expect(missing_status_handle == null);
    try std.testing.expect(!testPathExists(missing_status_path));
    defer antfly_db_close(missing_status_handle);

    {
        var short_file = try std.Io.Dir.cwd().createFile(std.testing.io, short_lite_path, .{});
        defer short_file.close(std.testing.io);
        try short_file.writePositionalAll(std.testing.io, "short native lite header", 0);
    }
    var short_lite_sentinel: u8 = 0;
    var short_lite_handle: ?*anyopaque = &short_lite_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open_readonly(short_lite_path, &short_lite_handle));
    try std.testing.expect(short_lite_handle == null);
    defer antfly_db_close(short_lite_handle);

    var short_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_file_json(short_lite_path, &short_check));
    defer freeRawBuffer(short_check.ptr, short_check.len);
    const short_check_json = short_check.ptr.?[0..short_check.len];
    try std.testing.expect(std.mem.indexOf(u8, short_check_json, "\"valid\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, short_check_json, "\"issue\":\"truncated_header\"") != null);

    var src_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(src_path, &src_handle));
    defer antfly_db_close(src_handle);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_capabilities_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_backup(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_file_json(src_path, null));
    var null_check_file_path: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_file_json(null, &null_check_file_path));
    try std.testing.expect(null_check_file_path.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_check_file_path.len);
    var invalid_check_file_path: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_file_json(plain_path, &invalid_check_file_path));
    try std.testing.expect(invalid_check_file_path.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), invalid_check_file_path.len);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_json(src_handle, snapshot_path, false, null));
    var null_snapshot_dest: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_json(src_handle, null, false, &null_snapshot_dest));
    try std.testing.expect(null_snapshot_dest.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_snapshot_dest.len);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, snapshot_file_path, false, null));
    var null_snapshot_file_src: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(null, snapshot_file_path, false, &null_snapshot_file_src));
    try std.testing.expect(null_snapshot_file_src.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_snapshot_file_src.len);
    var null_snapshot_file_dest: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, null, false, &null_snapshot_file_dest));
    try std.testing.expect(null_snapshot_file_dest.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_snapshot_file_dest.len);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_compact_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_vacuum_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_run_until_idle_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_replay_generated_enrichments_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_pending_work_stats_json(src_handle, null));

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(src_handle, &status));
    const status_json = status.ptr.?[0..status.len];
    const native_local_runtime_available = lite_backend.capabilitiesForProfile(.native).local_inference_runtime;
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"storage\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"format\":\"aflite\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"engine\":\"native_single_file\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"primary_layout\":\"native_document_pages\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"replay_layout\":\"native_replay_lanes_in_document_catalog\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_layout\":\"native_index_catalog_pages\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_layout\":\"lsm") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_namespace\":\"__antfly_lite\"") != null);
    const expected_format_version = try std.fmt.allocPrint(alloc, "\"format_version\":{d}", .{antfly.lite.native.format_version});
    defer alloc.free(expected_format_version);
    try std.testing.expect(std.mem.indexOf(u8, status_json, expected_format_version) != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"page_size\":4096") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"active_checkpoint\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"stats\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"pending_work\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"inference\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"remote_provider_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"local_runtime_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, if (native_local_runtime_available) "\"local_runtime_available\":true" else "\"local_runtime_available\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"capabilities\":") != null);
    antfly_buffer_free_zero(&status);
    try std.testing.expect(status.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), status.len);

    var remote_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_remote_provider_configured,
    };
    var remote_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(remote_inference_path, &remote_options, &remote_handle));
    defer antfly_db_close(remote_handle);
    var remote_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(remote_handle, &remote_status));
    defer freeRawBuffer(remote_status.ptr, remote_status.len);
    const remote_status_json = remote_status.ptr.?[0..remote_status.len];
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"mode\":\"remote_provider\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"configured\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"remote_provider_configured\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"local_runtime_configured\":false") != null);

    var local_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var local_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(local_inference_path, &local_options, &local_handle));
    defer antfly_db_close(local_handle);
    var local_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(local_handle, &local_status));
    defer freeRawBuffer(local_status.ptr, local_status.len);
    const local_status_json = local_status.ptr.?[0..local_status.len];
    if (native_local_runtime_available) {
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"mode\":\"local_embedded\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"configured\":true") != null);
    } else {
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"mode\":\"caller_supplied_or_disabled\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"configured\":false") != null);
    }
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"remote_provider_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"local_runtime_configured\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, if (native_local_runtime_available) "\"local_runtime_available\":true" else "\"local_runtime_available\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"capabilities\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, if (native_local_runtime_available) "\"inference_mode\":\"local_embedded\"" else "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, if (native_local_runtime_available) "\"local_inference_runtime\":true" else "\"local_inference_runtime\":false") != null);

    var capabilities: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(src_handle, &capabilities));
    defer freeRawBuffer(capabilities.ptr, capabilities.len);
    const capabilities_json = capabilities.ptr.?[0..capabilities.len];
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"hosted_profile\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"manual_maintenance\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"dense_vector_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"sparse_vector_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"no_inference_configured_ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"caller_supplied_artifacts\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, if (native_local_runtime_available) "\"local_inference_runtime\":true" else "\"local_inference_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"raft_replication\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cluster_placement\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cross_node_joins\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_shard_fanout\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"distributed_transaction_coordination\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cluster_heartbeat_status_aggregation\":false") != null);

    var local_capabilities: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(local_handle, &local_capabilities));
    defer freeRawBuffer(local_capabilities.ptr, local_capabilities.len);
    const local_capabilities_json = local_capabilities.ptr.?[0..local_capabilities.len];
    try std.testing.expect(std.mem.indexOf(u8, local_capabilities_json, if (native_local_runtime_available) "\"inference_mode\":\"local_embedded\"" else "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_capabilities_json, if (native_local_runtime_available) "\"available_inference_modes\":[\"caller_supplied_artifacts\",\"remote_provider\",\"local_embedded\",\"disabled_deferred\"]" else "\"available_inference_modes\":[\"caller_supplied_artifacts\",\"remote_provider\",\"disabled_deferred\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_capabilities_json, if (native_local_runtime_available) "\"local_inference_runtime\":true" else "\"local_inference_runtime\":false") != null);

    var pending: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_pending_work_stats_json(src_handle, &pending));
    defer freeRawBuffer(pending.ptr, pending.len);
    const pending_json = pending.ptr.?[0..pending.len];
    try std.testing.expect(std.mem.indexOf(u8, pending_json, "\"has_async_indexes\":") != null);

    var idle: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle_json(src_handle, &idle));
    defer freeRawBuffer(idle.ptr, idle.len);
    const idle_json = idle.ptr.?[0..idle.len];
    try std.testing.expect(std.mem.indexOf(u8, idle_json, "\"derived_target_sequence\":") != null);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(src_handle));

    var replayed: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_replay_generated_enrichments_json(src_handle, &replayed));
    defer freeRawBuffer(replayed.ptr, replayed.len);
    const replayed_json = replayed.ptr.?[0..replayed.len];
    try std.testing.expect(std.mem.indexOf(u8, replayed_json, "\"replayed\":") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_delete_index(src_handle, .{
        .ptr = "missing-index",
        .len = "missing-index".len,
    }, null));
    var missing_index_deleted = true;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_delete_index(src_handle, .{
        .ptr = "missing-index",
        .len = "missing-index".len,
    }, &missing_index_deleted));
    try std.testing.expect(!missing_index_deleted);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(src_handle, .{
        .ptr = schema_json,
        .len = schema_json.len,
    }));

    var loaded_schema: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_schema_json(src_handle, &loaded_schema));
    defer freeRawBuffer(loaded_schema.ptr, loaded_schema.len);
    try std.testing.expectEqualStrings(schema_json, loaded_schema.ptr.?[0..loaded_schema.len]);

    const enrichment_json =
        \\{"name":"body_chunks_v1","kind":"chunk","field":"body","chunk_size":8,"chunk_overlap":2}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(src_handle, .{
        .ptr = enrichment_json,
        .len = enrichment_json.len,
    }));

    const scratch_enrichment_json =
        \\{"name":"scratch_chunks_v1","kind":"chunk","field":"scratch","chunk_size":4,"chunk_overlap":1}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(src_handle, .{
        .ptr = scratch_enrichment_json,
        .len = scratch_enrichment_json.len,
    }));

    var enrichments: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_enrichments_json(src_handle, &enrichments));
    defer freeRawBuffer(enrichments.ptr, enrichments.len);
    try std.testing.expect(std.mem.indexOf(u8, enrichments.ptr.?[0..enrichments.len], "\"body_chunks_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, enrichments.ptr.?[0..enrichments.len], "\"chunk_size\":8") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_delete_enrichment(src_handle, .{
        .ptr = "chunk",
        .len = "chunk".len,
    }, .{
        .ptr = "scratch_chunks_v1",
        .len = "scratch_chunks_v1".len,
    }, null));
    var invalid_enrichment_deleted = true;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_delete_enrichment(src_handle, .{
        .ptr = "unknown",
        .len = "unknown".len,
    }, .{
        .ptr = "scratch_chunks_v1",
        .len = "scratch_chunks_v1".len,
    }, &invalid_enrichment_deleted));
    try std.testing.expect(!invalid_enrichment_deleted);

    var deleted_enrichment = false;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_delete_enrichment(src_handle, .{
        .ptr = "chunk",
        .len = "chunk".len,
    }, .{
        .ptr = "scratch_chunks_v1",
        .len = "scratch_chunks_v1".len,
    }, &deleted_enrichment));
    try std.testing.expect(deleted_enrichment);

    const writes_a = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi-lite", .len = "doc:capi-lite".len },
            .value = .{ .ptr = "{\"title\":\"first\"}", .len = "{\"title\":\"first\"}".len },
            .is_delete = false,
        },
        .{
            .key = .{ .ptr = "doc:gone", .len = "doc:gone".len },
            .value = .{ .ptr = "{\"title\":\"remove\"}", .len = "{\"title\":\"remove\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &writes_a, writes_a.len, null, 0, 1_000, 0));

    const writes_b = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi-lite", .len = "doc:capi-lite".len },
            .value = .{ .ptr = "{\"title\":\"second\"}", .len = "{\"title\":\"second\"}".len },
            .is_delete = false,
        },
        .{
            .key = .{ .ptr = "doc:gone", .len = "doc:gone".len },
            .value = .{},
            .is_delete = true,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &writes_b, writes_b.len, null, 0, 2_000, 0));

    const lite_txn_id: [16]u8 = .{ 0x6c, 0x69, 0x74, 0x65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_begin_transaction_with_id(src_handle, &lite_txn_id, 3_000, null, 0));
    const txn_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-lite-txn", .len = "doc:capi-lite-txn".len },
        .value = .{ .ptr = "{\"title\":\"transactional\"}", .len = "{\"title\":\"transactional\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_write_transaction(src_handle, &lite_txn_id, &txn_writes, txn_writes.len, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_resolve_intents(src_handle, &lite_txn_id, @intFromEnum(transactions_mod.TxnStatus.committed), 4_000));
    var lite_txn_status: u8 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_transaction_status(src_handle, &lite_txn_id, &lite_txn_status));
    try std.testing.expectEqual(@as(u8, @intFromEnum(transactions_mod.TxnStatus.committed)), lite_txn_status);
    var lite_txn_commit_version: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_commit_version(src_handle, &lite_txn_id, &lite_txn_commit_version));
    try std.testing.expectEqual(@as(u64, 4_000), lite_txn_commit_version);
    var lite_txn_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(src_handle, .{
        .ptr = "doc:capi-lite-txn",
        .len = "doc:capi-lite-txn".len,
    }, &lite_txn_lookup));
    defer freeRawBuffer(lite_txn_lookup.ptr, lite_txn_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, lite_txn_lookup.ptr.?[0..lite_txn_lookup.len], "\"transactional\"") != null);

    const pinned_seed_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-pinned", .len = "doc:capi-pinned".len },
        .value = .{ .ptr = "{\"title\":\"pinned-before\"}", .len = "{\"title\":\"pinned-before\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &pinned_seed_writes, pinned_seed_writes.len, null, 0, 4_100, 0));

    var concurrent_readonly_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(src_path, &concurrent_readonly_handle));
    defer antfly_db_close(concurrent_readonly_handle);
    try std.testing.expectEqual(db_mod.OpenOptions.OpenMode.query_readonly, asHandle(concurrent_readonly_handle).?.open_mode);
    var second_writer_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.busy, antfly_lite_open(src_path, &second_writer_handle));
    defer antfly_db_close(second_writer_handle);
    var concurrent_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(concurrent_readonly_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &concurrent_lookup));
    defer freeRawBuffer(concurrent_lookup.ptr, concurrent_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, concurrent_lookup.ptr.?[0..concurrent_lookup.len], "\"second\"") != null);

    const pinned_advance_a = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-pinned", .len = "doc:capi-pinned".len },
        .value = .{ .ptr = "{\"title\":\"pinned-after-a\"}", .len = "{\"title\":\"pinned-after-a\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &pinned_advance_a, pinned_advance_a.len, null, 0, 4_200, 0));
    const pinned_advance_b = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-pinned", .len = "doc:capi-pinned".len },
        .value = .{ .ptr = "{\"title\":\"pinned-after-b\"}", .len = "{\"title\":\"pinned-after-b\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &pinned_advance_b, pinned_advance_b.len, null, 0, 4_300, 0));

    var pinned_snapshot_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_copy_stable_snapshot_json(concurrent_readonly_handle, pinned_snapshot_path, false, &pinned_snapshot_report));
    defer freeRawBuffer(pinned_snapshot_report.ptr, pinned_snapshot_report.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_report.ptr.?[0..pinned_snapshot_report.len], "\"tail_bytes\":") != null);

    var pinned_snapshot_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(pinned_snapshot_path, &pinned_snapshot_handle));
    defer antfly_db_close(pinned_snapshot_handle);
    var pinned_snapshot_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(pinned_snapshot_handle, &pinned_snapshot_check));
    defer freeRawBuffer(pinned_snapshot_check.ptr, pinned_snapshot_check.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_check.ptr.?[0..pinned_snapshot_check.len], "\"valid\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_check.ptr.?[0..pinned_snapshot_check.len], "\"tail_bytes\":0") != null);

    var pinned_snapshot_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(pinned_snapshot_handle, .{
        .ptr = "doc:capi-pinned",
        .len = "doc:capi-pinned".len,
    }, &pinned_snapshot_lookup));
    defer freeRawBuffer(pinned_snapshot_lookup.ptr, pinned_snapshot_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_lookup.ptr.?[0..pinned_snapshot_lookup.len], "\"pinned-before\"") != null);

    var pinned_writer_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(src_handle, .{
        .ptr = "doc:capi-pinned",
        .len = "doc:capi-pinned".len,
    }, &pinned_writer_lookup));
    defer freeRawBuffer(pinned_writer_lookup.ptr, pinned_writer_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_writer_lookup.ptr.?[0..pinned_writer_lookup.len], "\"pinned-after-b\"") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(concurrent_readonly_handle, &writes_a, 1, null, 0, 4_500, 0));

    var concurrent_status_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_status_only(src_path, &concurrent_status_handle));
    defer antfly_db_close(concurrent_status_handle);
    try std.testing.expectEqual(db_mod.OpenOptions.OpenMode.status_only, asHandle(concurrent_status_handle).?.open_mode);
    var concurrent_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_stats_json(concurrent_status_handle, &concurrent_status));
    defer freeRawBuffer(concurrent_status.ptr, concurrent_status.len);
    try std.testing.expect(std.mem.indexOf(u8, concurrent_status.ptr.?[0..concurrent_status.len], "\"doc_count\":") != null);
    // Every fresh Lite database now carries the default full-text index,
    // whose maintenance for the writes above runs in the background. Bring
    // that work to idle before the vacuum and the physical-tail probes that
    // follow: the online vacuum waits its turn for the writer slot, but the
    // junk bytes appended below are only a stable tail if no later
    // checkpoint extends the file past them.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(src_handle));
    var online_vacuum: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_vacuum_json(src_handle, &online_vacuum));
    defer freeRawBuffer(online_vacuum.ptr, online_vacuum.len);
    try std.testing.expect(online_vacuum.len > 0);
    var retired_reader_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(concurrent_readonly_handle, .{
        .ptr = "doc:capi-pinned",
        .len = "doc:capi-pinned".len,
    }, &retired_reader_lookup));
    defer freeRawBuffer(retired_reader_lookup.ptr, retired_reader_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, retired_reader_lookup.ptr.?[0..retired_reader_lookup.len], "\"pinned-before\"") != null);
    antfly_db_close(concurrent_status_handle);
    concurrent_status_handle = null;
    antfly_db_close(concurrent_readonly_handle);
    concurrent_readonly_handle = null;

    var check_before: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(src_handle, &check_before));
    defer freeRawBuffer(check_before.ptr, check_before.len);
    try std.testing.expect(std.mem.indexOf(u8, check_before.ptr.?[0..check_before.len], "\"valid\":true") != null);

    {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        var file = try std.Io.Dir.cwd().openFile(io, src_path, .{ .mode = .read_write });
        defer file.close(io);
        const source_size = (try file.stat(io)).size;
        try file.writePositionalAll(io, "tail", source_size);
    }

    var invalid_snapshot_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_json(src_handle, invalid_snapshot_path, false, &invalid_snapshot_report));
    try std.testing.expect(invalid_snapshot_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), invalid_snapshot_report.len);

    var snapshot_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_copy_stable_snapshot_json(src_handle, snapshot_path, false, &snapshot_report));
    defer freeRawBuffer(snapshot_report.ptr, snapshot_report.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_report.ptr.?[0..snapshot_report.len], "\"tail_bytes\":4") != null);

    var snapshot_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(snapshot_path, &snapshot_handle));
    defer antfly_db_close(snapshot_handle);

    var snapshot_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(snapshot_handle, &snapshot_check));
    defer freeRawBuffer(snapshot_check.ptr, snapshot_check.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_check.ptr.?[0..snapshot_check.len], "\"valid\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_check.ptr.?[0..snapshot_check.len], "\"tail_bytes\":0") != null);

    var snapshot_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(snapshot_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &snapshot_lookup));
    defer freeRawBuffer(snapshot_lookup.ptr, snapshot_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_lookup.ptr.?[0..snapshot_lookup.len], "\"second\"") != null);

    var invalid_snapshot_file_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, invalid_snapshot_file_path, false, &invalid_snapshot_file_report));
    try std.testing.expect(invalid_snapshot_file_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), invalid_snapshot_file_report.len);

    var snapshot_file_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_copy_stable_snapshot_file_json(src_path, snapshot_file_path, false, &snapshot_file_report));
    defer freeRawBuffer(snapshot_file_report.ptr, snapshot_file_report.len);
    if (std.mem.indexOf(u8, snapshot_file_report.ptr.?[0..snapshot_file_report.len], "\"tail_bytes\":4") == null)
        std.debug.print("stable snapshot file report: {s}\n", .{snapshot_file_report.ptr.?[0..snapshot_file_report.len]});
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_report.ptr.?[0..snapshot_file_report.len], "\"tail_bytes\":4") != null);
    var snapshot_file_existing_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, snapshot_file_path, false, &snapshot_file_existing_report));
    try std.testing.expect(snapshot_file_existing_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), snapshot_file_existing_report.len);

    var snapshot_file_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(snapshot_file_path, &snapshot_file_handle));
    defer antfly_db_close(snapshot_file_handle);
    var snapshot_file_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(snapshot_file_handle, &snapshot_file_check));
    defer freeRawBuffer(snapshot_file_check.ptr, snapshot_file_check.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_check.ptr.?[0..snapshot_file_check.len], "\"valid\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_check.ptr.?[0..snapshot_file_check.len], "\"tail_bytes\":0") != null);
    var snapshot_file_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(snapshot_file_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &snapshot_file_lookup));
    defer freeRawBuffer(snapshot_file_lookup.ptr, snapshot_file_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_lookup.ptr.?[0..snapshot_file_lookup.len], "\"second\"") != null);

    var compacted: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_compact_json(src_handle, &compacted));
    defer freeRawBuffer(compacted.ptr, compacted.len);
    try std.testing.expect(std.mem.indexOf(u8, compacted.ptr.?[0..compacted.len], "\"compacted\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, compacted.ptr.?[0..compacted.len], "\"vacuum\":") != null);

    var vacuumed: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_vacuum_json(src_handle, &vacuumed));
    defer freeRawBuffer(vacuumed.ptr, vacuumed.len);
    try std.testing.expect(std.mem.indexOf(u8, vacuumed.ptr.?[0..vacuumed.len], "\"reclaimed_bytes\":") != null);

    var backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src_handle, &backup));
    defer freeRawBuffer(backup.ptr, backup.len);
    try std.testing.expect(backup.len > 0);

    var exported_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src_handle, &exported_backup));
    defer freeRawBuffer(exported_backup.ptr, exported_backup.len);
    try std.testing.expect(exported_backup.len > 0);

    var dst_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(dst_path, &dst_handle));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(dst_handle, .{
        .ptr = null,
        .len = 16,
    }));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(dst_handle, .{
        .ptr = null,
        .len = 16,
    }));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(dst_handle, .{
        .ptr = null,
        .len = 0,
    }));

    var bad_dst_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(bad_dst_path, &bad_dst_handle));
    defer antfly_db_close(bad_dst_handle);

    const target_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-import-target", .len = "doc:capi-import-target".len },
        .value = .{ .ptr = "{\"title\":\"target survives bad capi import\"}", .len = "{\"title\":\"target survives bad capi import\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(bad_dst_handle, &target_writes, target_writes.len, null, 0, 5_000, 0));

    var malformed = std.ArrayList(u8).empty;
    defer malformed.deinit(alloc);
    try backup_codec.writeHeader(&malformed, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = [_]u8{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });
    const malformed_doc_payload = [_]u8{ 1, 0, 0, 0 };
    try backup_codec.writeBlock(&malformed, alloc, .document_batch, &malformed_doc_payload);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(bad_dst_handle, .{
        .ptr = malformed.items.ptr,
        .len = malformed.items.len,
    }));

    var target_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(bad_dst_handle, .{
        .ptr = "doc:capi-import-target",
        .len = "doc:capi-import-target".len,
    }, &target_lookup));
    defer freeRawBuffer(target_lookup.ptr, target_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, target_lookup.ptr.?[0..target_lookup.len], "\"target survives bad capi import\"") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(bad_dst_handle, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }));

    var target_after_valid_rejected: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(bad_dst_handle, .{
        .ptr = "doc:capi-import-target",
        .len = "doc:capi-import-target".len,
    }, &target_after_valid_rejected));
    defer freeRawBuffer(target_after_valid_rejected.ptr, target_after_valid_rejected.len);
    try std.testing.expect(std.mem.indexOf(u8, target_after_valid_rejected.ptr.?[0..target_after_valid_rejected.len], "\"target survives bad capi import\"") != null);

    var schema_dst_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(schema_dst_path, &schema_dst_handle));
    defer antfly_db_close(schema_dst_handle);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(schema_dst_handle, .{
        .ptr = schema_json,
        .len = schema_json.len,
    }));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(schema_dst_handle, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }));
    var schema_after_valid_rejected: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_schema_json(schema_dst_handle, &schema_after_valid_rejected));
    defer freeRawBuffer(schema_after_valid_rejected.ptr, schema_after_valid_rejected.len);
    try std.testing.expectEqualStrings(schema_json, schema_after_valid_rejected.ptr.?[0..schema_after_valid_rejected.len]);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_import_backup(dst_handle, .{
        .ptr = exported_backup.ptr,
        .len = exported_backup.len,
    }));

    var lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(dst_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &lookup));
    defer freeRawBuffer(lookup.ptr, lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, lookup.ptr.?[0..lookup.len], "\"second\"") != null);

    antfly_db_close(dst_handle);
    dst_handle = null;

    var restore_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(restore_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &restore_report));
    defer freeRawBuffer(restore_report.ptr, restore_report.len);
    try std.testing.expect(std.mem.indexOf(u8, restore_report.ptr.?[0..restore_report.len], "\"format\":\"aflite\"") != null);

    lite_restore_staging.failNextPublishedFileDirectorySyncForTest();
    antfly.test_error_logs.expectErrorLogs(1);
    var unknown_report: capi.Buffer = .{ .ptr = @constCast("stale".ptr), .len = "stale".len };
    try std.testing.expectEqual(capi.ErrorCode.outcome_unknown, antfly_restore_backup_json(restore_unknown_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &unknown_report));
    try std.testing.expect(unknown_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), unknown_report.len);

    // Publication already happened, so the destination must be inspectable
    // and a blind retry must be rejected rather than replacing it again.
    var unknown_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(restore_unknown_path, &unknown_handle));
    defer antfly_db_close(unknown_handle);
    var unknown_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(unknown_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &unknown_lookup));
    defer freeRawBuffer(unknown_lookup.ptr, unknown_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, unknown_lookup.ptr.?[0..unknown_lookup.len], "\"second\"") != null);
    var retry_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_restore_backup_json(restore_unknown_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &retry_report));

    var restored_file_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(restore_path, &restored_file_handle));
    defer antfly_db_close(restored_file_handle);
    var restored_file_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(restored_file_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &restored_file_lookup));
    defer freeRawBuffer(restored_file_lookup.ptr, restored_file_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, restored_file_lookup.ptr.?[0..restored_file_lookup.len], "\"second\"") != null);

    var restore_alias_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(restore_alias_path, &lite_restore_options, .{
        .ptr = exported_backup.ptr,
        .len = exported_backup.len,
    }, false, &restore_alias_report));
    defer freeRawBuffer(restore_alias_report.ptr, restore_alias_report.len);
    try std.testing.expect(std.mem.indexOf(u8, restore_alias_report.ptr.?[0..restore_alias_report.len], "\"format\":\"aflite\"") != null);

    var restored_alias_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(restore_alias_path, &restored_alias_handle));
    defer antfly_db_close(restored_alias_handle);
    var restored_alias_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(restored_alias_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &restored_alias_lookup));
    defer freeRawBuffer(restored_alias_lookup.ptr, restored_alias_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, restored_alias_lookup.ptr.?[0..restored_alias_lookup.len], "\"second\"") != null);

    const locked_restore_tmp_path = try std.fmt.allocPrint(alloc, "{s}.restore-tmp.aflite", .{locked_restore_path});
    defer alloc.free(locked_restore_tmp_path);
    {
        var locked_restore = try antfly.lite.native.lockWriterPath(alloc, locked_restore_path);
        defer locked_restore.close();

        var locked_restore_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
        try std.testing.expectEqual(capi.ErrorCode.busy, antfly_restore_backup_json(locked_restore_path, &lite_restore_options, .{
            .ptr = backup.ptr,
            .len = backup.len,
        }, false, &locked_restore_report));
        try std.testing.expect(locked_restore_report.ptr == null);
        try std.testing.expectEqual(@as(usize, 0), locked_restore_report.len);
    }
    {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        try std.testing.expect(!capiPathExists(io_impl.io(), locked_restore_path));
        try std.testing.expect(!capiPathExists(io_impl.io(), locked_restore_tmp_path));
    }

    var restore_existing: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_restore_backup_json(restore_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &restore_existing));
    try std.testing.expect(restore_existing.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), restore_existing.len);

    var malformed_restore_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_restore_backup_json(restore_malformed_path, &lite_restore_options, .{
        .ptr = malformed.items.ptr,
        .len = malformed.items.len,
    }, false, &malformed_restore_report));
    try std.testing.expect(malformed_restore_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), malformed_restore_report.len);
    {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        try std.testing.expect(!capiPathExists(io_impl.io(), restore_malformed_path));
    }

    var readonly_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(dst_path, &readonly_handle));
    defer antfly_db_close(readonly_handle);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(readonly_handle, &writes_a, 1, null, 0, 3_000, 0));
}

test "capi lite exposes hosted and status-only profiles" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-profiles");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var hosted_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create_hosted(path, &hosted_handle));

    var hosted_caps: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(hosted_handle, &hosted_caps));
    defer freeRawBuffer(hosted_caps.ptr, hosted_caps.len);
    const hosted_caps_json = hosted_caps.ptr.?[0..hosted_caps.len];
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"hosted_profile\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"manual_maintenance\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"background_enrichment_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"ttl_cleanup_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"transaction_recovery_runtime\":false") != null);

    // Creating a Lite database already provisions the default full-text
    // index, matching the server's behavior on table create.
    var initial_indexes: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_indexes_json(hosted_handle, &initial_indexes));
    defer freeRawBuffer(initial_indexes.ptr, initial_indexes.len);
    try std.testing.expect(std.mem.indexOf(u8, initial_indexes.ptr.?[0..initial_indexes.len], "\"full_text_index_v0\"") != null);

    const index_json =
        \\{"name":"full_text_index_v0","kind":"full_text","config_json":"{}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.internal, antfly_db_add_index_json(hosted_handle, .{
        .ptr = index_json,
        .len = index_json.len,
    }));

    const writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-lite-profile", .len = "doc:capi-lite-profile".len },
        .value = .{ .ptr = "{\"title\":\"hosted\"}", .len = "{\"title\":\"hosted\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(hosted_handle, &writes, writes.len, null, 0, 1_000, 0));

    var pending_before: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_pending_work_stats_json(hosted_handle, &pending_before));
    defer freeRawBuffer(pending_before.ptr, pending_before.len);
    try std.testing.expect(std.mem.indexOf(u8, pending_before.ptr.?[0..pending_before.len], "\"has_async_indexes\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, pending_before.ptr.?[0..pending_before.len], "\"derived_target_sequence\":") != null);

    var idle_after: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle_json(hosted_handle, &idle_after));
    defer freeRawBuffer(idle_after.ptr, idle_after.len);
    try std.testing.expect(std.mem.indexOf(u8, idle_after.ptr.?[0..idle_after.len], "\"text_merge\"") != null);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(hosted_handle));

    const hosted_query =
        \\{"full_text_search":{"match":{"field":"title","text":"hosted"}},"limit":1}
    ;
    var hosted_search: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(hosted_handle, .{
        .ptr = hosted_query,
        .len = hosted_query.len,
    }, &hosted_search));
    defer freeRawBuffer(hosted_search.ptr, hosted_search.len);
    try std.testing.expect(std.mem.indexOf(u8, hosted_search.ptr.?[0..hosted_search.len], "\"doc:capi-lite-profile\"") != null);

    antfly_db_close(hosted_handle);
    hosted_handle = null;

    var status_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_status_only(path, &status_handle));
    defer antfly_db_close(status_handle);

    var status_caps: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(status_handle, &status_caps));
    defer freeRawBuffer(status_caps.ptr, status_caps.len);
    const status_caps_json = status_caps.ptr.?[0..status_caps.len];
    try std.testing.expect(std.mem.indexOf(u8, status_caps_json, "\"hosted_profile\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_caps_json, "\"manual_maintenance\":false") != null);

    var stats: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_stats_json(status_handle, &stats));
    defer freeRawBuffer(stats.ptr, stats.len);
    try std.testing.expect(std.mem.indexOf(u8, stats.ptr.?[0..stats.len], "\"doc_count\":") != null);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(status_handle, &writes, writes.len, null, 0, 2_000, 0));
}

test "capi directory restore coordinates with open handles and publishes atomically" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const io = handleLockIo();
    const src_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-dir-restore-src");
    defer alloc.free(src_path);
    const dest_path = try tempTestPath(alloc, test_tmp.path(), "capi-dir-restore-dest");
    defer alloc.free(dest_path);
    cleanupTestFile(src_path);
    defer cleanupTestFile(src_path);
    cleanupTestDir(dest_path);
    defer cleanupTestDir(dest_path);

    // Two backups with different contents.
    var src: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(src_path, &src));
    const first_writes = [_]capi.WriteIntent{.{ .key = .{ .ptr = "doc:v1", .len = 6 }, .value = .{ .ptr = "{\"v\":1}", .len = 7 } }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src, &first_writes, 1, null, 0, 1, 0));
    var first_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src, &first_backup));
    defer antfly_buffer_free(&first_backup);
    const second_writes = [_]capi.WriteIntent{.{ .key = .{ .ptr = "doc:v2", .len = 6 }, .value = .{ .ptr = "{\"v\":2}", .len = 7 } }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src, &second_writes, 1, null, 0, 2, 0));
    var second_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src, &second_backup));
    defer antfly_buffer_free(&second_backup);
    antfly_db_close(src);

    const directory = capi.OpenOptions{ .storage_kind = capi.storage_kind_directory };
    var readonly = directory;
    readonly.open_mode = capi.open_mode_readonly;
    const first: capi.Slice = .{ .ptr = first_backup.ptr, .len = first_backup.len };
    const second: capi.Slice = .{ .ptr = second_backup.ptr, .len = second_backup.len };
    var report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(dest_path, &directory, first, false, &report));
    antfly_buffer_free(&report);

    // An open read-only handle blocks replacement, and keeps working.
    var reader: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dest_path, &readonly, &reader));
    try std.testing.expectEqual(capi.ErrorCode.busy, antfly_restore_backup_json(dest_path, &directory, second, true, &report));
    var lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(reader, .{ .ptr = "doc:v1", .len = 6 }, &lookup));
    antfly_buffer_free(&lookup);
    antfly_db_close(reader);

    // A database opened outside libantfly holds the same generation lease.
    {
        var direct = try db_mod.DB.open(alloc, dest_path, .{});
        defer direct.close();
        try std.testing.expectEqual(capi.ErrorCode.busy, antfly_restore_backup_json(dest_path, &directory, second, true, &report));
    }

    // While a restore holds the generation transition, opens are refused.
    {
        var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithIo(dest_path, io);
        defer transition.deinit();
        var blocked: ?*anyopaque = null;
        try std.testing.expectEqual(capi.ErrorCode.busy, antfly_db_open_with_options(dest_path, &directory, &blocked));
        try std.testing.expect(blocked == null);
    }

    // With no handles open, replacement publishes the new database.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(dest_path, &directory, second, true, &report));
    antfly_buffer_free(&report);
    var restored: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dest_path, &readonly, &restored));
    defer antfly_db_close(restored);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(restored, .{ .ptr = "doc:v2", .len = 6 }, &lookup));
    antfly_buffer_free(&lookup);

    // No staging or displaced directory is left beside the destination.
    const parent = std.fs.path.dirname(dest_path).?;
    const prefix = try std.fmt.allocPrint(alloc, "{s}.restore-", .{std.fs.path.basename(dest_path)});
    defer alloc.free(prefix);
    var dir = try std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true });
    defer dir.close(io);
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (std.mem.startsWith(u8, entry.name, prefix)) {
            std.debug.print("unexpected restore sibling left behind: {s}\n", .{entry.name});
            return error.TestUnexpectedResult;
        }
    }
}

test "capi directory restore publishes with derived work drained" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const src_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-dir-restore-drain-src");
    defer alloc.free(src_path);
    const dest_path = try tempTestPath(alloc, test_tmp.path(), "capi-dir-restore-drain-dest");
    defer alloc.free(dest_path);
    cleanupTestFile(src_path);
    defer cleanupTestFile(src_path);
    cleanupTestDir(dest_path);
    defer cleanupTestDir(dest_path);

    var src: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(src_path, &src));
    const writes = [_]capi.WriteIntent{.{ .key = .{ .ptr = "doc:fox", .len = 7 }, .value = .{ .ptr = "{\"body\":\"the quick brown fox\"}", .len = 30 } }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src, &writes, 1, null, 0, 1, 0));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(src));
    var backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src, &backup));
    defer antfly_buffer_free(&backup);
    antfly_db_close(src);

    const directory = capi.OpenOptions{ .storage_kind = capi.storage_kind_directory };
    var report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(dest_path, &directory, .{ .ptr = backup.ptr, .len = backup.len }, false, &report));
    antfly_buffer_free(&report);

    // A read-only handle runs no derived work of its own, so the published
    // directory must already carry complete index results.
    var readonly = directory;
    readonly.open_mode = capi.open_mode_readonly;
    var restored: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dest_path, &readonly, &restored));
    defer antfly_db_close(restored);
    const request = "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"quick fox\"}},\"limit\":5}";
    var result: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(restored, .{ .ptr = request.ptr, .len = request.len }, &result));
    defer antfly_buffer_free(&result);
    const hits = result.ptr.?[0..result.len];
    if (std.mem.indexOf(u8, hits, "doc:fox") == null) {
        std.debug.print("restored directory search missed doc:fox: {s}\n", .{hits});
        return error.TestUnexpectedResult;
    }
}

test "capi handle ids are safe to use after close and across slot reuse" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path_a = try tempTestAflitePath(alloc, test_tmp.path(), "capi-handle-id-a");
    defer alloc.free(path_a);
    const path_b = try tempTestAflitePath(alloc, test_tmp.path(), "capi-handle-id-b");
    defer alloc.free(path_b);
    cleanupTestFile(path_a);
    defer cleanupTestFile(path_a);
    cleanupTestFile(path_b);
    defer cleanupTestFile(path_b);

    var a: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path_a, &a));
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(a, &out));
    antfly_buffer_free(&out);

    antfly_db_close(a);
    // Use after close and repeated close are defined: no access to freed memory.
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(a, &out));
    antfly_db_close(a);

    // The next handle reuses the freed slot under a new generation, so the
    // stale id neither matches it nor closes it.
    var b: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path_b, &b));
    defer antfly_db_close(b);
    try std.testing.expect(a != b);
    try std.testing.expectEqual(HandleRegistry.decode(a).?.index, HandleRegistry.decode(b).?.index);
    if (HandleRegistry.reserve_address_space) {
        // Ids are addresses in the reservation, so bindings can keep them in
        // pointer-typed fields that a garbage collector inspects.
        const base = handle_registry.base.load(.acquire);
        try std.testing.expect(base >= 4096);
        try std.testing.expect(@intFromPtr(b) >= base);
        try std.testing.expectEqual(@as(usize, 0), @intFromPtr(b) % 8);
    }
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(a, &out));
    antfly_db_close(a);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(b, &out));
    antfly_buffer_free(&out);

    // Values that were never issued fail cleanly too.
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(null, &out));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(@ptrFromInt(0x7fff_0000), &out));
}

test "capi handle registry retires a slot instead of wrapping its generation" {
    var first = Handle{ .alloc = std.testing.allocator, .db = undefined };
    var second = Handle{ .alloc = std.testing.allocator, .db = undefined };
    var third = Handle{ .alloc = std.testing.allocator, .db = undefined };

    // Put a slot at the front of the free list, then move it to the last
    // generation an id can encode.
    const first_id = try registerTestHandle(&first);
    const index = HandleRegistry.decode(first_id).?.index;
    unregisterTestHandle(first_id);
    const slot = handle_registry.slotFor(index).?;
    const last_generation = handle_registry.generationMask();
    slot.state.store(last_generation << 1, .release);

    const second_id = try registerTestHandle(&second);
    try std.testing.expectEqual(index, HandleRegistry.decode(second_id).?.index);
    try std.testing.expectEqual(last_generation, HandleRegistry.decode(second_id).?.generation);
    unregisterTestHandle(second_id);

    // Retired: still claimed, unusable by any id, and never handed out again.
    try std.testing.expectEqual(last_generation << 1 | 1, slot.state.load(.acquire));
    try std.testing.expect(asHandle(second_id) == null);
    try std.testing.expect(enterHandle(second_id, .read) == null);
    unregisterTestHandle(second_id);
    const third_id = try registerTestHandle(&third);
    defer unregisterTestHandle(third_id);
    try std.testing.expect(HandleRegistry.decode(third_id).?.index != index);
    const wrapped = handle_registry.encode(.{ .index = index, .generation = 0 });
    try std.testing.expect(enterHandle(wrapped, .read) == null);
}

test "capi concurrent calls and closes on one handle never touch freed memory" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-handle-close-race");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));

    const Worker = struct {
        fn call(id: ?*anyopaque, unexpected: *std.atomic.Value(u32)) void {
            while (true) {
                var out: capi.Buffer = .{};
                switch (antfly_db_status_json(id, &out)) {
                    .ok => antfly_buffer_free(&out),
                    .invalid_argument => return,
                    else => {
                        _ = unexpected.fetchAdd(1, .monotonic);
                        return;
                    },
                }
            }
        }
        fn close(id: ?*anyopaque) void {
            antfly_db_close(id);
        }
    };

    var unexpected = std.atomic.Value(u32).init(0);
    const spawn_config: std.Thread.SpawnConfig = .{ .stack_size = capi_min_thread_stack_size };
    var callers: [6]std.Thread = undefined;
    for (&callers) |*t| t.* = try std.Thread.spawn(spawn_config, Worker.call, .{ handle, &unexpected });
    std.Io.sleep(std.testing.io, .fromMilliseconds(20), .awake) catch {};
    const closer_a = try std.Thread.spawn(spawn_config, Worker.close, .{handle});
    const closer_b = try std.Thread.spawn(spawn_config, Worker.close, .{handle});
    closer_a.join();
    closer_b.join();
    for (callers) |t| t.join();
    try std.testing.expectEqual(@as(u32, 0), unexpected.load(.monotonic));
}

test "capi text and dense searches succeed while writes commit" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-search-during-writes");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);
    const dense_index =
        \\{"name":"dv_v1","kind":"dense_vector","config_json":"{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{ .ptr = dense_index, .len = dense_index.len }));

    const Writer = struct {
        fn run(id: ?*anyopaque, stop: *std.atomic.Value(bool), failures: *std.atomic.Value(u32)) void {
            var ts: u64 = 1;
            var key_buf: [32]u8 = undefined;
            while (!stop.load(.acquire)) : (ts += 1) {
                const key = std.fmt.bufPrint(&key_buf, "doc:{d}", .{ts}) catch unreachable;
                const value =
                    \\{"body":"searchable writes","_embeddings":{"dv_v1":[1.0,0.0]}}
                ;
                const writes = [_]capi.WriteIntent{.{
                    .key = .{ .ptr = key.ptr, .len = key.len },
                    .value = .{ .ptr = value, .len = value.len },
                    .is_delete = false,
                }};
                if (antfly_db_batch(id, &writes, writes.len, null, 0, ts, 0) != .ok) _ = failures.fetchAdd(1, .monotonic);
            }
        }
    };

    var stop = std.atomic.Value(bool).init(false);
    var write_failures = std.atomic.Value(u32).init(0);
    const writer = try std.Thread.spawn(.{ .stack_size = capi_min_thread_stack_size }, Writer.run, .{ handle, &stop, &write_failures });
    defer writer.join();
    defer stop.store(true, .release);

    // Every search stamps an identity generation that a concurrent commit
    // can invalidate; unpinned reads must restamp rather than fail.
    const vector = [_]f32{ 1.0, 0.0 };
    for (0..200) |_| {
        var text_result: capi.DenseSearchResult = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_text_match(
            handle,
            .{},
            .{ .ptr = "body", .len = 4 },
            .{ .ptr = "searchable", .len = 10 },
            5,
            0,
            &text_result,
        ));
        antfly_dense_search_result_free(&text_result);
        var dense_result: capi.PackedDenseSearchResult = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense(
            handle,
            .{ .ptr = "dv_v1", .len = 5 },
            &vector,
            vector.len,
            1,
            1,
            0,
            &dense_result,
        ));
        antfly_packed_dense_search_result_free(&dense_result);
    }
    stop.store(true, .release);
    try std.testing.expectEqual(@as(u32, 0), write_failures.load(.monotonic));
}

test "capi lite open options validate and configure ttl cleanup" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-open-options");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    try std.testing.expectEqual(@as(u32, @intCast(@sizeOf(capi.OpenOptions))), antfly_open_options_size());
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_open_options_init(null));

    var generic_defaults = capi.OpenOptions{
        .abi_size = 0,
        .storage_kind = 99,
        .open_mode = 99,
        .profile = 99,
        .flags = std.math.maxInt(u32),
        .reserved0 = 1,
        .inference_host_budget_mb = 1,
        .busy_timeout_ms = 1,
        .reserved = .{1} ** 8,
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&generic_defaults));
    try std.testing.expectEqual(@as(u32, @sizeOf(capi.OpenOptions)), generic_defaults.abi_size);
    try std.testing.expectEqual(capi.storage_kind_directory, generic_defaults.storage_kind);
    try std.testing.expectEqual(capi.open_mode_writer, generic_defaults.open_mode);
    try std.testing.expectEqual(capi.profile_native, generic_defaults.profile);
    try std.testing.expectEqual(@as(u32, 0), generic_defaults.flags);
    try std.testing.expectEqual(@as(u32, 0), generic_defaults.reserved0);
    try std.testing.expectEqual(@as(u32, 0), generic_defaults.inference_host_budget_mb);
    try std.testing.expectEqual(@as(u64, 0), generic_defaults.busy_timeout_ms);
    for (generic_defaults.reserved) |word| try std.testing.expectEqual(@as(u64, 0), word);

    // Lite opens use the same options with the Lite storage kind.
    var defaults = generic_defaults;
    defaults.storage_kind = capi.storage_kind_lite;

    var default_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &defaults, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &defaults, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    cleanupTestFile(path);

    var prefix_lite_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @offsetOf(capi.OpenOptions, "flags"),
        .open_mode = capi.open_mode_readonly,
        .profile = capi.profile_native,
        .flags = std.math.maxInt(u32),
        .reserved = .{1} ** 8,
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &defaults, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &prefix_lite_options, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    cleanupTestFile(path);

    var generic_lite_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &generic_lite_options, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    generic_lite_options.open_mode = capi.open_mode_readonly;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &generic_lite_options, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    cleanupTestFile(path);

    const dir_path = try tempTestPath(alloc, test_tmp.path(), "capi-generic-directory-open");
    defer alloc.free(dir_path);
    cleanupTestDir(dir_path);
    defer cleanupTestDir(dir_path);
    var directory_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_create_with_options(dir_path, &generic_defaults, &directory_handle));
    try std.testing.expectEqual(@as(?*anyopaque, null), directory_handle);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dir_path, &generic_defaults, &directory_handle));
    const generic_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:generic", .len = "doc:generic".len },
        .value = .{ .ptr = "{\"title\":\"generic\"}", .len = "{\"title\":\"generic\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(directory_handle, &generic_writes, generic_writes.len, null, 0, 1_000, 0));
    antfly_db_close(directory_handle);
    directory_handle = null;
    var generic_readonly = capi.OpenOptions{
        .storage_kind = capi.storage_kind_directory,
        .open_mode = capi.open_mode_readonly,
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dir_path, &generic_readonly, &directory_handle));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(directory_handle, &generic_writes, generic_writes.len, null, 0, 2_000, 0));
    antfly_db_close(directory_handle);
    directory_handle = null;

    var sentinel: u8 = 0;
    var invalid_handle: ?*anyopaque = &sentinel;
    var invalid_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .open_mode = 99,
    };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(path, &invalid_options, &invalid_handle));
    try std.testing.expect(invalid_handle == null);

    var hosted_ttl_handle: ?*anyopaque = &sentinel;
    var hosted_ttl_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .profile = capi.profile_hosted,
        .flags = capi.open_flag_ttl_cleanup,
        .ttl_cleanup_enabled = true,
    };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(path, &hosted_ttl_options, &hosted_ttl_handle));
    try std.testing.expect(hosted_ttl_handle == null);

    var hosted_generated_replay_handle: ?*anyopaque = &sentinel;
    var hosted_generated_replay_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .profile = capi.profile_hosted,
        .flags = capi.open_flag_generated_enrichment_replay,
    };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(path, &hosted_generated_replay_options, &hosted_generated_replay_handle));
    try std.testing.expect(hosted_generated_replay_handle == null);

    const owner_id = "capi-ttl-owner";
    var open_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_no_sync | capi.open_flag_ttl_cleanup,
        .ttl_cleanup_enabled = true,
        .ttl_cleanup_lease_owned = true,
        .ttl_cleanup_batch_size = 8,
        .ttl_cleanup_owner_id = .{ .ptr = owner_id, .len = owner_id.len },
        .ttl_cleanup_lease_ttl_ms = 250,
        .ttl_cleanup_interval_ms = 10,
        .ttl_cleanup_grace_period_ns = 1,
    };

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &open_options, &handle));
    defer antfly_db_close(handle);

    const schema_json =
        \\{"default_type":"doc","ttl_duration_ns":1,"ttl_field":"expires_at","document_schemas":{"doc":{"schema":{"type":"object","properties":{"expires_at":{"type":"datetime"},"title":{"type":"text"}}}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .{
        .ptr = schema_json,
        .len = schema_json.len,
    }));

    const doc_json = "{\"title\":\"gone\",\"expires_at\":1}";
    const writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:expired", .len = "doc:expired".len },
        .value = .{ .ptr = doc_json, .len = doc_json.len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(handle, &writes, writes.len, null, 0, 1, 0));

    var stats: capi.Buffer = .{};
    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        freeRawBuffer(stats.ptr, stats.len);
        stats = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_stats_json(handle, &stats));
        const stats_json = stats.ptr.?[0..stats.len];
        if (std.mem.indexOf(u8, stats_json, "\"enabled\":true") != null and
            std.mem.indexOf(u8, stats_json, "\"lease_owned\":true") != null and
            std.mem.indexOf(u8, stats_json, "\"deleted_docs\":1") != null and
            std.mem.indexOf(u8, stats_json, "\"scanned_timestamps\":1") != null)
        {
            break;
        }
        antfly.platform_clock.Clock.real().sleepMs(10);
    }
    defer freeRawBuffer(stats.ptr, stats.len);
    try std.testing.expect(attempts < 200);
}

test "capi execute graph queries honors identity read generation" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-execute-graph-generation");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const handle = asHandle(handle_ptr).?;
    try handle.db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });
    try handle.db.batch(.{
        .writes = &.{
            .{ .key = "n:a", .value = "{\"title\":\"A\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:b\"}]}}}" },
            .{ .key = "n:b", .value = "{\"title\":\"B\"}" },
        },
        .sync_level = .full_index,
    });

    const missing_generation_request =
        \\{"graph_queries":[{"name":"neighbors","type":"neighbors","index_name":"gr_v1","start_nodes":{"result_ref":"seed"},"edge_types":["links"]}],"named_sets":[{"name":"seed","hit_ids_b64":["bjph"]}],"limit":10}
    ;
    var missing_generation_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_execute_graph_queries_json(
        handle_ptr,
        .{ .ptr = missing_generation_request.ptr, .len = missing_generation_request.len },
        &missing_generation_out,
    ));

    const current_generation = handle.db.core.nextDerivedSequence();
    const request = try std.fmt.allocPrint(alloc,
        \\{{"identity_read_generation":{d},"graph_queries":[{{"name":"neighbors","type":"neighbors","index_name":"gr_v1","start_nodes":{{"result_ref":"seed"}},"edge_types":["links"]}}],"named_sets":[{{"name":"seed","hit_ids_b64":["bjph"]}}],"limit":10}}
    , .{current_generation});
    defer alloc.free(request);
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_execute_graph_queries_json(
        handle_ptr,
        .{ .ptr = request.ptr, .len = request.len },
        &out,
    ));
    defer freeRawBuffer(out.ptr, out.len);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"key_b64\":\"bjpi\"") != null);
    var parsed_out = try std.json.parseFromSlice(std.json.Value, alloc, out.ptr.?[0..out.len], .{});
    defer parsed_out.deinit();
    try std.testing.expectEqual(@as(i64, @intCast(current_generation)), parsed_out.value.array.items[0].object.get("identity_read_generation").?.integer);

    const stale_generation = handle.db.core.nextDerivedSequence() -| 1;
    const stale_request = try std.fmt.allocPrint(alloc,
        \\{{"identity_read_generation":{d},"graph_queries":[{{"name":"neighbors","type":"neighbors","index_name":"gr_v1","start_nodes":{{"result_ref":"seed"}},"edge_types":["links"]}}],"named_sets":[{{"name":"seed","hit_ids_b64":["bjph"]}}],"limit":10}}
    , .{stale_generation});
    defer alloc.free(stale_request);
    var stale_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_execute_graph_queries_json(
        handle_ptr,
        .{ .ptr = stale_request.ptr, .len = stale_request.len },
        &stale_out,
    ));
}

test "capi search rejects stale identity generation before readable lease hook" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-stale-generation-before-lease");
    defer alloc.free(path);

    cleanupTestDir(path);

    const Recorder = struct {
        count: usize = 0,

        fn callback(
            ctx: ?*anyopaque,
            _: u64,
            _: ?[*]const u8,
            _: usize,
        ) callconv(.c) capi.ErrorCode {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.count += 1;
            return .ok;
        }
    };

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try handle.db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
        }},
    });

    var recorder = Recorder{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_readable_lease_hook(
        handle_id,
        42,
        &recorder,
        &Recorder.callback,
    ));

    const stale_generation = handle.db.core.nextDerivedSequence() -| 1;
    const request = try std.fmt.allocPrint(alloc,
        \\{{"mode":"dense","index_name":"dv_v1","vector":[1,0],"k":1,"limit":1,"identity_read_generation":{d}}}
    , .{stale_generation});
    defer alloc.free(request);

    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_search_json(
        handle_id,
        .{ .ptr = request.ptr, .len = request.len },
        &out,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.count);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_readable_lease_hook(
        handle_id,
        0,
        null,
        null,
    ));
}

test "capi search json returns stamped identity generation" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-search-generation-response");
    defer alloc.free(path);

    cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try handle.db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try handle.db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
        }},
    });

    const current_generation = handle.db.core.nextDerivedSequence();
    const search_req =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":1,\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
        handle_id,
        .{ .ptr = search_req.ptr, .len = search_req.len },
        &out,
    ));
    defer freeRawBuffer(out.ptr, out.len);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out.ptr.?[0..out.len], .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, @intCast(current_generation)), parsed.value.object.get("identity_read_generation").?.integer);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "doc_ordinal") == null);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "ordinal") == null);

    var packed_result: capi.PackedDenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &packed_result,
    ));
    defer antfly_packed_dense_search_result_free(&packed_result);
    try std.testing.expectEqual(current_generation, packed_result.identity_read_generation);

    var text_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_text_match(
        handle_id,
        .{ .ptr = "ft_v1".ptr, .len = "ft_v1".len },
        .{ .ptr = "title".ptr, .len = "title".len },
        .{ .ptr = "alpha".ptr, .len = "alpha".len },
        1,
        0,
        &text_result,
    ));
    defer antfly_dense_search_result_free(&text_result);
    try std.testing.expectEqual(current_generation, text_result.identity_read_generation);

    const hits_request =
        "{\"mode\":\"full_text\",\"index_name\":\"ft_v1\",\"text_query_type\":\"match\",\"field\":\"title\",\"text\":\"alpha\",\"limit\":1}";
    var hits_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_hits_json(
        handle_id,
        .{ .ptr = hits_request.ptr, .len = hits_request.len },
        &hits_result,
    ));
    defer antfly_dense_search_result_free(&hits_result);
    try std.testing.expectEqual(current_generation, hits_result.identity_read_generation);
}

test "capi aggregate hits rejects stale identity generation before aggregation materialization" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-aggregate-stale-generation");
    defer alloc.free(path);

    cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\"}",
        }},
    });

    const current_generation = handle.db.core.nextDerivedSequence();
    const stale_generation = current_generation -| 1;
    try std.testing.expect(stale_generation != current_generation);

    const request_template =
        \\{{"identity_read_generation":{d},"hit_ids_b64":["ZG9jOmE="],"aggregations":[{{"name":"bad","type":"terms","field":"title","background_query_type":"bogus"}}]}}
    ;
    const current_request = try std.fmt.allocPrint(alloc, request_template, .{current_generation});
    defer alloc.free(current_request);
    var current_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.internal, antfly_db_aggregate_hits_json(
        handle_id,
        .{ .ptr = current_request.ptr, .len = current_request.len },
        &current_out,
    ));

    const missing_generation_request =
        \\{"hit_ids_b64":["ZG9jOmE="],"aggregations":[{"name":"bad","type":"terms","field":"title","background_query_type":"bogus"}]}
    ;
    var missing_generation_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_aggregate_hits_json(
        handle_id,
        .{ .ptr = missing_generation_request.ptr, .len = missing_generation_request.len },
        &missing_generation_out,
    ));

    const stale_request = try std.fmt.allocPrint(alloc, request_template, .{stale_generation});
    defer alloc.free(stale_request);
    var stale_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_aggregate_hits_json(
        handle_id,
        .{ .ptr = stale_request.ptr, .len = stale_request.len },
        &stale_out,
    ));
}

test "capi request paths trigger readable lease hook" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-readable-lease");
    defer alloc.free(path);

    cleanupTestDir(path);

    const Recorder = struct {
        contexts: [9][32]u8 = [_][32]u8{[_]u8{0} ** 32} ** 9,
        context_lens: [9]usize = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        group_ids: [9]u64 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        count: usize = 0,

        fn callback(
            ctx: ?*anyopaque,
            group_id: u64,
            request_ctx_ptr: ?[*]const u8,
            request_ctx_len: usize,
        ) callconv(.c) capi.ErrorCode {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (self.count >= self.contexts.len or request_ctx_len > self.contexts[self.count].len) return .internal;
            self.group_ids[self.count] = group_id;
            if (request_ctx_ptr != null and request_ctx_len > 0) {
                @memcpy(self.contexts[self.count][0..request_ctx_len], request_ctx_ptr.?[0..request_ctx_len]);
            }
            self.context_lens[self.count] = request_ctx_len;
            self.count += 1;
            return .ok;
        }
    };

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try handle.db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
            },
        },
    });
    const current_generation = handle.db.core.nextDerivedSequence();

    var recorder = Recorder{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_readable_lease_hook(
        handle_id,
        42,
        &recorder,
        &Recorder.callback,
    ));

    var lookup_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(
        handle_id,
        .{ .ptr = "doc:a".ptr, .len = "doc:a".len },
        &lookup_out,
    ));
    freeRawBuffer(lookup_out.ptr, lookup_out.len);

    const scan_req = "{\"from_key_b64\":\"\",\"to_key_b64\":\"\",\"include_documents\":false,\"limit\":10}";
    var scan_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_scan_json(
        handle_id,
        .{ .ptr = scan_req.ptr, .len = scan_req.len },
        &scan_out,
    ));
    freeRawBuffer(scan_out.ptr, scan_out.len);

    const search_req =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":1,\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var search_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
        handle_id,
        .{ .ptr = search_req.ptr, .len = search_req.len },
        &search_out,
    ));
    freeRawBuffer(search_out.ptr, search_out.len);

    var packed_result: capi.PackedDenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &packed_result,
    ));
    antfly_packed_dense_search_result_free(&packed_result);

    var dense_profile: capi.DenseSearchProfile = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_profile(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &dense_profile,
    ));

    const dense_wire_req = [_]u8{
        0x54, 0x46, 0x4E, 0x44,
        0x01, 0x00, 0x01, 0x00,
        0x05, 0x00, 0x02, 0x00,
        0x01, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        'd',  'v',  '_',  'v',
        '1',  0x00, 0x00, 0x80,
        0x3f, 0x00, 0x00, 0x00,
        0x00,
    };
    var dense_wire_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_wire(
        handle_id,
        .{ .ptr = &dense_wire_req, .len = dense_wire_req.len },
        &dense_wire_out,
    ));
    try std.testing.expectEqual(@as(?u64, current_generation), try search_wire.denseResponseIdentityReadGeneration(dense_wire_out.ptr.?[0..dense_wire_out.len]));
    freeRawBuffer(dense_wire_out.ptr, dense_wire_out.len);

    var dense_wire_profile_out: capi.Buffer = .{};
    var dense_wire_profile: capi.DenseWireSearchProfile = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_wire_profile(
        handle_id,
        .{ .ptr = &dense_wire_req, .len = dense_wire_req.len },
        &dense_wire_profile_out,
        &dense_wire_profile,
    ));
    try std.testing.expectEqual(@as(?u64, current_generation), try search_wire.denseResponseIdentityReadGeneration(dense_wire_profile_out.ptr.?[0..dense_wire_profile_out.len]));
    freeRawBuffer(dense_wire_profile_out.ptr, dense_wire_profile_out.len);

    var text_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_text_match(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        .{ .ptr = "title".ptr, .len = "title".len },
        .{ .ptr = "alpha".ptr, .len = "alpha".len },
        1,
        0,
        &text_result,
    ));
    antfly_dense_search_result_free(&text_result);

    const hits_req =
        "{\"mode\":\"full_text\",\"index_name\":\"dv_v1\",\"text_query_type\":\"match\",\"field\":\"title\",\"text\":\"alpha\",\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var hits_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_hits_json(
        handle_id,
        .{ .ptr = hits_req.ptr, .len = hits_req.len },
        &hits_result,
    ));
    antfly_dense_search_result_free(&hits_result);

    try std.testing.expectEqual(@as(usize, 9), recorder.count);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[0]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[1]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[2]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[3]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[4]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[5]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[6]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[7]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[8]);
    try std.testing.expectEqualStrings("enrichment:lookup:read_index", recorder.contexts[0][0..recorder.context_lens[0]]);
    try std.testing.expectEqualStrings("enrichment:scan:read_index", recorder.contexts[1][0..recorder.context_lens[1]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[2][0..recorder.context_lens[2]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[3][0..recorder.context_lens[3]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[4][0..recorder.context_lens[4]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[5][0..recorder.context_lens[5]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[6][0..recorder.context_lens[6]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[7][0..recorder.context_lens[7]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[8][0..recorder.context_lens[8]]);
}

test "capi relational expression errors preserve public status semantics" {
    // Keep the public C error mapping aligned without importing schema source
    // across the standalone C ABI module's directory boundary.
    const expression_errors = antfly.capi_dependencies.relational_expression_errors;
    inline for (@typeInfo(expression_errors.Error).error_set.?) |field| {
        const err = @field(expression_errors.Error, field.name);
        try std.testing.expectEqual(if (expression_errors.isInvalidInput(err)) capi.ErrorCode.invalid_argument else capi.ErrorCode.intent_conflict, capi.mapError(err));
    }
}

test "capi artifact decode and lookup json" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-artifact-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const handle = asHandle(handle_ptr).?;
    var artifact_ref = db_mod.types.ArtifactRef{
        .document_id = try handle.alloc.dupe(u8, "doc:a"),
        .name = try handle.alloc.dupe(u8, "body_chunks_v1"),
        .kind = .chunk,
        .chunk_id = 0,
    };
    defer artifact_ref.deinit(handle.alloc);
    const internal_key = try db_mod.artifact_ids.internalKeyForArtifactRefAlloc(handle.alloc, artifact_ref);
    defer handle.alloc.free(internal_key);
    try handle.db.core.store.put(
        internal_key,
        "{\"body\":\"abcdefgh\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0,\"_artifact_unit_fingerprint\":\"private\"}",
    );

    const artifact_id = try db_mod.artifact_ids.artifactPublicIdAlloc(handle.alloc, artifact_ref);
    defer handle.alloc.free(artifact_id);
    const artifact_id_b64 = try dupBase64(handle.alloc, artifact_id);
    defer handle.alloc.free(artifact_id_b64);

    var decode_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_decode_artifact_id_json(.{
        .ptr = artifact_id_b64.ptr,
        .len = artifact_id_b64.len,
    }, &decode_out));
    defer freeRawBuffer(decode_out.ptr, decode_out.len);
    try std.testing.expect(std.mem.indexOf(u8, decode_out.ptr.?[0..decode_out.len], "\"kind\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, decode_out.ptr.?[0..decode_out.len], "\"name\":\"body_chunks_v1\"") != null);

    var lookup_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_artifact_json(handle_ptr, .{
        .ptr = artifact_id_b64.ptr,
        .len = artifact_id_b64.len,
    }, &lookup_out));
    defer freeRawBuffer(lookup_out.ptr, lookup_out.len);
    var lookup_json = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        lookup_out.ptr.?[0..lookup_out.len],
        .{},
    );
    defer lookup_json.deinit();
    try std.testing.expect(lookup_json.value.object.get("artifact_ref") != null);
    const value_b64 = lookup_json.value.object.get("value_b64") orelse return error.TestUnexpectedResult;
    if (value_b64 != .string) return error.TestUnexpectedResult;
    const public_value = try decodeBase64Alloc(alloc, value_b64.string);
    defer alloc.free(public_value);
    var public_json = try std.json.parseFromSlice(std.json.Value, alloc, public_value, .{});
    defer public_json.deinit();
    if (public_json.value != .object) return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), public_json.value.object.count());
    try std.testing.expectEqualStrings("abcdefgh", public_json.value.object.get("body").?.string);
    try std.testing.expectEqualStrings("body_chunks_v1", public_json.value.object.get("_artifact_name").?.string);
    try std.testing.expectEqual(@as(i64, 0), public_json.value.object.get("_chunk_id").?.integer);
    try std.testing.expect(public_json.value.object.get("_artifact_unit_fingerprint") == null);
}

test "capi dense search profile breakdown" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-dense-profile");
    defer alloc.free(path);

    cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    const writes = try alloc.alloc(db_mod.types.BatchWrite, 2048);
    defer {
        for (writes) |write| alloc.free(write.value);
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        const x: f32 = if (i % 2 == 0) 1 else 0;
        const y: f32 = if (i % 2 == 0) 0 else 1;
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"embedding\":[{d},{d}],\"title\":\"doc-{d}\"}}", .{ x, y, i }),
        };
    }
    defer for (writes) |write| alloc.free(write.key);

    try handle.db.batch(.{ .writes = writes });

    const req: db_mod.types.SearchRequest = .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0 },
            .k = 10,
        } },
        .limit = 10,
        .include_stored = false,
    };

    const dense_entry = handle.db.core.index_manager.denseIndex("dv_v1").?;

    const reps: usize = 20;

    var hbc_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicNowNs();
        var result = try dense_entry.index.search(&.{ 1.0, 0.0 }, 10);
        defer result.deinit();
        hbc_total_ns += monotonicNowNs() - start;
    }

    var db_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicNowNs();
        var result = try handle.db.search(alloc, req);
        defer result.deinit();
        db_total_ns += monotonicNowNs() - start;
    }

    const request_json =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":10,\"limit\":10,\"offset\":0,\"include_stored\":false}";
    var capi_total_ns: u64 = 0;
    for (0..reps) |_| {
        var out: capi.Buffer = .{};
        const start = monotonicNowNs();
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
            handle_id,
            .{ .ptr = request_json.ptr, .len = request_json.len },
            &out,
        ));
        capi_total_ns += monotonicNowNs() - start;
        freeRawBuffer(out.ptr, out.len);
    }

    std.debug.print(
        "dense_profile reps={d} hbc_avg_ns={d} db_avg_ns={d} capi_avg_ns={d}\n",
        .{
            reps,
            @divTrunc(hbc_total_ns, reps),
            @divTrunc(db_total_ns, reps),
            @divTrunc(capi_total_ns, reps),
        },
    );

    var final_result = try handle.db.search(alloc, req);
    defer final_result.deinit();
    try std.testing.expectEqual(@as(u32, 10), final_result.total_hits);
}

// This always runs (on every build, including the default) and only asserts
// on the build-capability signal, never on whether construction succeeded:
// `local_inference_runtime` and `inference_mode: "local_embedded"` must
// report true/present only when the loaded build both advertises and links
// the local inference runtime.
test "capi lite local-runtime-configured flag reports local_embedded only when the build links inference" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-inference-variant-caps");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle, &status));
    defer freeRawBuffer(status.ptr, status.len);
    const status_json = status.ptr.?[0..status.len];

    // capi_build_options.inference_enabled is only true for the isolated
    // `-Dcapi-inference=true` storage_kernel unit and for unit tests
    // themselves; it is never true for the default build.
    if (capi_build_options.inference_enabled and lite_backend.capabilitiesForProfile(.native).local_inference_runtime) {
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"inference_mode\":\"local_embedded\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"local_inference_runtime\":true") != null);
        const owned_handle = asHandle(handle).?;
        try std.testing.expect(owned_handle.lite_inference_lifetime != null);
    } else {
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
        const owned_handle = asHandle(handle).?;
        try std.testing.expect(owned_handle.lite_inference_lifetime == null);
    }
}

// Confirms `EmbeddedInferenceNodeOptions` plumbing end to end: an explicit
// process-memory budget override passed through `antfly_open_options`
// is what the embedded node actually resolves and reports back in
// `antfly_db_status_json`'s "inference" object, rather than the previous
// hardcoded zero-bytes/"automatic" policy that gave every Lite handle no way
// to distinguish "host-detected" from "unset" (see
// `inference_provider.createEmbeddedInferenceNode` and
// `LiteResolvedOpenOptions.inference`). Runs on every build (including the
// default, where the local runtime never actually starts) and only asserts
// the reported budgets on the build-capability signal, matching the sibling
// local-runtime test above.
test "capi lite explicit resource budget overrides are reported in status" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-inference-budget-status");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
        .inference_host_budget_mb = 256,
        .inference_backend_budget_mb = 128,
        .inference_process_memory_budget_mb = 512,
        .inference_combined_budget_mb = 384,
        .inference_kv_budget_mb = 64,
        .inference_scratch_budget_mb = 32,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle, &status));
    defer freeRawBuffer(status.ptr, status.len);
    const status_json = status.ptr.?[0..status.len];

    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"host_budget_mb\":256") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"backend_budget_mb\":128") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_budget_mb\":512") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"combined_budget_mb\":384") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"kv_budget_mb\":64") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"scratch_budget_mb\":32") != null);

    if (capi_build_options.inference_enabled and lite_backend.capabilitiesForProfile(.native).local_inference_runtime) {
        // The node actually started against the explicit override: the
        // resolved envelope is exactly the requested 512 MiB (clamped by
        // `resolveEffectiveDetailed` only when the detected host is
        // smaller, which a 512 MiB request never exceeds on any test
        // runner), and its provenance is "explicit", not "automatic".
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_bytes\":536870912") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_source\":\"explicit\"") != null);
        const owned_handle = asHandle(handle).?;
        try std.testing.expect(owned_handle.lite_inference_lifetime != null);
    } else {
        // No local runtime started, so the resolution never ran; status
        // still echoes the caller's requested override values (asserted
        // above) but the resolved fields stay at their zero-value/automatic
        // defaults.
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_bytes\":0") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_source\":\"automatic\"") != null);
    }
}

// A Lite handle opened with the local-runtime flag but no explicit budget
// overrides must not fall back to the previous zero-bytes/automatic
// generation-budget policy: that policy could not admit even one
// boundary-architecture extraction window (fastino/gliner2.5-base-v1's
// admission estimate exceeds it regardless of request size -- see
// GLINER25.md's "Memory budget" section and this task's
// gliner25-longdoc-handoff.md), so every such call failed with
// error.MemoryBudgetExceeded through the embedded/in-process worker path
// even though `antfly inference run` succeeded (only because an operator
// supplied `--host-budget-mb`/`--backend-budget-mb`/`--combined-budget-mb`/
// `--kv-budget-mb`/`--scratch-budget-mb` by hand). Confirms the embedded
// node instead defaults each lane to
// `inference_provider.default_{host,backend,combined,kv,scratch}_budget_mb`
// (clamped to the host-detected envelope), and logs the resolved values for
// the machine running this test.
test "capi lite defaults embedded generation budgets when no override is given" {
    if (!capi_build_options.inference_enabled) return error.SkipZigTest;
    if (!lite_backend.capabilitiesForProfile(.native).local_inference_runtime) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-inference-budget-defaults");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle, &status));
    defer freeRawBuffer(status.ptr, status.len);
    const status_json = status.ptr.?[0..status.len];
    std.debug.print("capi lite default embedded generation budgets on this machine: {s}\n", .{status_json});

    // None of these lanes may resolve to 0 (the previous automatic/unbounded
    // policy that admitted no boundary-model window). host/backend/scratch
    // default to 16384 MiB, combined to 32768, kv to 4096, each clamped to
    // the host-detected envelope; a real dev/CI machine has well over 4 GiB,
    // so all five stay at their un-clamped defaults on any realistic runner.
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"host_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"backend_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"combined_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"kv_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"scratch_budget_mb\":0") == null);
}

fn liteLocalEmbeddingModelAvailable(alloc: Allocator) bool {
    const home_c = std.c.getenv("HOME") orelse return false;
    const home = std.mem.span(home_c);
    const owner_dir = std.fs.path.join(alloc, &.{ home, ".antfly", "inference", "models", "Qwen" }) catch return false;
    defer alloc.free(owner_dir);
    var dir = std.Io.Dir.cwd().openDir(std.testing.io, owner_dir, .{ .iterate = true }) catch return false;
    defer dir.close(std.testing.io);
    var it = dir.iterateAssumeFirstIteration();
    while (it.next(std.testing.io) catch return false) |entry| {
        if (std.mem.startsWith(u8, entry.name, "Qwen3-Embedding-0.6B-GGUF")) return true;
    }
    return false;
}

// Conditional on the `-Dcapi-inference=true` variant (skips on the default
// build, where capi_build_options.inference_enabled is false) and on the
// small local embedding model being present under
// ~/.antfly/inference/models, so this never requires a network call and
// never fails a machine that has not pulled the model.
test "capi lite drains an antfly embedder with no api_url through the embedded inference provider" {
    if (!capi_build_options.inference_enabled) return error.SkipZigTest;
    if (!lite_backend.capabilitiesForProfile(.native).local_inference_runtime) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    if (!liteLocalEmbeddingModelAvailable(alloc)) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-local-embedding-drain");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const owned_handle = asHandle(handle).?;
    try std.testing.expect(owned_handle.lite_inference_lifetime != null);

    const index_json =
        \\{"name":"body_embedding","kind":"dense_vector","config_json":"{\"type\":\"embeddings\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\"}}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = index_json.ptr,
        .len = index_json.len,
    }));

    const enrichment_json =
        \\{"name":"body_embedder","kind":"embedding","field":"body","vector_space":"body_embedding","producer_json":"{\"type\":\"embedder\",\"config\":{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\"}}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = enrichment_json.ptr,
        .len = enrichment_json.len,
    }));

    const batch_json = "{\"inserts\":{\"doc:capi-local-embedding\":{\"body\":\"antfly lite embeds documents locally\"}},\"sync_level\":\"write\"}";
    var batch_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_out));
    defer freeRawBuffer(batch_out.ptr, batch_out.len);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle));

    const drained = owned_handle.db.pendingWorkStats();
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.error_count);
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.fatal_error_count);
    try std.testing.expect(!drained.enrichment.stalled);
    try std.testing.expectEqual(drained.enrichment.target_sequence, drained.enrichment.applied_sequence);
}

pub fn storageOwnerRestoreControlJson(owner_ptr: ?*anyopaque, request: *const kernel_owner_abi.RestoreOwnerControlRequest, out_result: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.control.version != kernel_owner_abi.abi_version) return .invalid_abi;
    antfly.capi_dependencies.api_restore_owner_contract.validateRequestSize(request.control.request_json.len) catch |err| return storageOwnerStatusFromError(err);
    if (request.source_byte_budget == 0 or request.source_byte_budget > 16 * 1024 * 1024) return .invalid_argument;
    const handle = asHandle(owner_ptr) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.control.table_name) orelse return .invalid_argument;
    const restore = antfly.capi_dependencies.storage_restore_owner;
    var input = std.json.parseFromSlice(restore.Request, handle.alloc, request.control.request_json.slice(), .{ .ignore_unknown_fields = false }) catch |err| return storageOwnerStatusFromError(err);
    defer input.deinit();
    input.value.validate(handle.storage_owner_group_id) catch |err| return storageOwnerStatusFromError(err);
    const runtime = handle.db.backend_runtime;
    const io = runtime.filesystemIo() orelse return storageOwnerStatusFromError(error.BackendRuntimeIoUnavailable);
    const cache_path = std.fmt.allocPrint(handle.alloc, "{s}.restore-source-{s}", .{ handle.db.core.path, std.fmt.bytesToHex(input.value.scope.digest(), .lower) }) catch |err| return storageOwnerStatusFromError(err);
    defer handle.alloc.free(cache_path);
    const Capture = struct {
        alloc: Allocator,
        scope: [32]u8,
        batch_json: ?[]u8 = null,
        fn propose(ptr: *anyopaque, batch: db_mod.types.BatchRequest, context: antfly.capi_dependencies.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            if (self.batch_json != null) return error.InvalidRestoreStagingCommand;
            if (batch.restore_staging_scope) |scope| if (!std.mem.eql(u8, &scope, &self.scope)) return error.RestoreStagingScopeChanged;
            var scoped = batch;
            scoped.restore_staging_scope = self.scope;
            self.batch_json = try antfly.capi_dependencies.api_batch.encodeBatchRequest(self.alloc, scoped);
        }
    };
    var capture: Capture = .{ .alloc = handle.alloc, .scope = input.value.scope.digest() };
    defer if (capture.batch_json) |bytes| handle.alloc.free(bytes);
    const response = restore.executeResident(handle.alloc, &handle.db, .{
        .io = io,
        .runtime = runtime,
        .cache_path = cache_path,
        .source_byte_budget = @intCast(request.source_byte_budget),
        .location_options = .{
            .secret_store = if (request.secret_store) |ptr| @ptrCast(@alignCast(ptr)) else null,
            .node_config = if (request.node_config) |ptr| @ptrCast(@alignCast(ptr)) else null,
            .network_io = runtime.apiIo(),
            .filesystem_io = io,
        },
        .proposer = .{ .ptr = &capture, .propose = Capture.propose },
    }, input.value, .{
        .cancellation = ownerQueryCancellation(&request.control),
        .deadline_ns = if (request.control.has_execution_deadline != 0) request.control.execution_deadline_ns else null,
    }) catch |err| return storageOwnerStatusFromError(err);
    const encoded = std.json.Stringify.valueAlloc(handle.alloc, .{ .response = response, .batch_json = capture.batch_json }, .{}) catch |err| return storageOwnerStatusFromError(err);
    out_result.* = .{ .ptr = encoded.ptr, .len = @intCast(encoded.len) };
    return .ok;
}

pub fn storageOwnerRelationalTransitionRead(owner_ptr: ?*anyopaque, request: *const kernel_owner_abi.RelationalTransitionReadRequest, out_result: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (request.request_json.len > 16 * 1024 * 1024) return .invalid_argument;
    const handle = asHandle(owner_ptr) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const alloc = arena.allocator();
    const command = std.json.parseFromSlice(antfly.capi_dependencies.storage_db_relational_transition_contract.Request, alloc, request.request_json.slice(), .{ .ignore_unknown_fields = false }) catch |err| return storageOwnerStatusFromError(err);
    var output: std.Io.Writer.Allocating = .init(handle.alloc);
    defer output.deinit();
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    switch (command.value) {
        .identity => antfly.capi_dependencies.storage_db_relational_integrity_json.write(handle.db.relationalTopologyIdentity() catch |err| return storageOwnerStatusFromError(err), &stream) catch |err| return storageOwnerStatusFromError(err),
        .status => antfly.capi_dependencies.storage_db_relational_integrity_json.write(handle.db.relationalTopologyStatus() catch |err| return storageOwnerStatusFromError(err), &stream) catch |err| return storageOwnerStatusFromError(err),
        .manifest => |input| antfly.capi_dependencies.storage_db_relational_integrity_json.write(handle.db.relationalHandoffManifest(alloc, input.source, input.destination, input.lower, input.upper, input.primary_sequence) catch |err| return storageOwnerStatusFromError(err), &stream) catch |err| return storageOwnerStatusFromError(err),
        .page => |input| antfly.capi_dependencies.storage_db_relational_integrity_json.write(handle.db.relationalHandoffPage(alloc, input.manifest, input.progress) catch |err| return storageOwnerStatusFromError(err), &stream) catch |err| return storageOwnerStatusFromError(err),
    }
    const encoded = output.toOwnedSlice() catch |err| return storageOwnerStatusFromError(err);
    out_result.* = .{ .ptr = encoded.ptr, .len = @intCast(encoded.len) };
    return .ok;
}

fn hiddenRestoreJson(alloc: std.mem.Allocator, db: *db_mod.DB, request: *const kernel_owner_abi.HiddenRestoreRequest, out_result: *kernel_owner_abi.OwnedBytes) !void {
    if (db.core.identity_namespace.table_id != request.table_id) return error.RestoreStagingScopeChanged;
    if (request.operation == .capture_public_snapshot) return captureOwnerSeedSnapshot(alloc, db, request);
    var bootstrap = (try db.readRestoreStagingBootstrap(alloc)) orelse {
        if (request.operation == .capture_snapshot) return error.RestoreStagingScopeChanged;
        return;
    };
    defer bootstrap.deinit();
    switch (request.operation) {
        .read_bootstrap => {
            const encoded = try std.json.Stringify.valueAlloc(alloc, bootstrap.value, .{});
            out_result.* = .{ .ptr = encoded.ptr, .len = @intCast(encoded.len) };
        },
        .capture_snapshot => {
            if (!std.mem.eql(u8, &request.scope, &bootstrap.value.scope.digest()) or !std.mem.eql(u8, bootstrap.value.table_name, request.table_name.slice())) return error.RestoreStagingScopeChanged;
            var progress = (try db.restoreStagingStatus(alloc)) orelse return error.RestoreStagingScopeChanged;
            defer progress.deinit();
            if (!std.mem.eql(u8, &request.scope, &progress.value.scope.digest())) return error.RestoreStagingScopeChanged;
            try captureOwnerSeedSnapshot(alloc, db, request);
        },
        .capture_public_snapshot => unreachable,
    }
}

fn captureOwnerSeedSnapshot(alloc: std.mem.Allocator, db: *db_mod.DB, request: *const kernel_owner_abi.HiddenRestoreRequest) !void {
    switch (db.primary_backend) {
        .lmdb, .lsm => {},
        .mem, .lsm_memory => return error.HASeedSnapshotUnsupportedBackend,
    }
    const token = request.snapshot_token.slice();
    if (token.len == 0 or std.mem.indexOfAny(u8, token, "/\\") != null or std.mem.eql(u8, token, ".") or std.mem.eql(u8, token, "..")) return error.InvalidSnapshotToken;
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const path = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ db.core.path, token });
    defer alloc.free(path);
    std.Io.Dir.cwd().deleteTree(io, path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io, path) catch {};
    const clock = db.backend_runtime.monotonicClock();
    _ = try db.snapshotHASeed(token, clock.nowRealtimeNs() +| std.time.ns_per_s);
    try backups_api.copyDirectoryRecursive(alloc, path, request.destination_root.slice());
}

pub fn storageOwnerHiddenRestoreJson(owner_ptr: ?*anyopaque, request: *const kernel_owner_abi.HiddenRestoreRequest, out_result: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (asHandle(owner_ptr)) |handle| {
        hiddenRestoreJson(handle.alloc, &handle.db, request, out_result) catch |err| return storageOwnerStatusFromError(err);
        return .ok;
    }
    if (request.operation != .read_bootstrap or request.path.len == 0) return .invalid_argument;
    const context = asStorageOwnerContext(request.context) orelse return .invalid_argument;
    const alloc = context.alloc;
    const runtime = context.backend_runtime.ptr();
    const io = runtime.filesystemIo() orelse return storageOwnerStatusFromError(error.BackendRuntimeIoUnavailable);
    _ = std.Io.Dir.cwd().statFile(io, request.path.slice(), .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return .ok,
        else => return storageOwnerStatusFromError(err),
    };
    var db = db_mod.DB.open(alloc, request.path.slice(), .{ .backend_runtime = runtime, .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false }) catch |err| return storageOwnerStatusFromError(err);
    defer db.close();
    hiddenRestoreJson(alloc, &db, request, out_result) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageOwnerMergeArtifactsPage(owner_ptr: ?*anyopaque, request: *const kernel_owner_abi.MergeArtifactsPageRequest, out_result: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner_ptr) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const rows = handle.db.mergeArtifactsPage(handle.alloc, .{ .start = request.range_start.slice(), .end = request.range_end.slice() }, if (request.after_key.len == 0) null else request.after_key.slice()) catch |err|
        return storageOwnerStatusFromError(err);
    defer {
        for (rows) |row| {
            handle.alloc.free(row.key);
            handle.alloc.free(row.value);
        }
        handle.alloc.free(rows);
    }
    const encoded = data_raft_projection_wire.encodeGroupStatePageAlloc(handle.alloc, .{ .entries = rows, .exhausted = rows.len == 0 }) catch |err|
        return storageOwnerStatusFromError(err);
    out_result.* = .{ .ptr = encoded.ptr, .len = @intCast(encoded.len) };
    return .ok;
}
