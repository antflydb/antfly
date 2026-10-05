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

//! Private server storage-provider operations, separate from public C exports.
const server_group_metadata = @import("../storage/server_group_metadata.zig");
const server_document_child_range = @import("../storage/server_document_child_range.zig");
pub const storage_root = @import("antfly_source_root");
pub const antfly = @import("../capi_root.zig");
const handles = @import("handles.zig");
pub const std = handles.std;
pub const builtin = handles.builtin;
pub const local_write = handles.local_write;
pub const capi = handles.capi;
pub const kernel_owner_abi = handles.kernel_owner_abi;
pub const local_query_client = handles.local_query_client;
pub const capi_build_options = handles.capi_build_options;
pub const db_mod = handles.db_mod;
pub const read_consistency = handles.read_consistency;
pub const transactions_mod = handles.transactions_mod;
pub const aggregations_mod = handles.aggregations_mod;
pub const search_agg_mod = handles.search_agg_mod;
pub const geo_mod = handles.geo_mod;
pub const lite_backend = handles.lite_backend;
pub const batch_api = handles.batch_api;
pub const query_api = handles.query_api;
pub const tables_api = handles.tables_api;
pub const table_reads_api = handles.table_reads_api;
pub const inference_provider = handles.inference_provider;
pub const managed_embedder = handles.managed_embedder;
pub const Allocator = handles.Allocator;
pub const abi_version = handles.abi_version;
pub const Handle = handles.Handle;
pub const stopLiteEmbeddedInference = handles.stopLiteEmbeddedInference;
pub const closeHandle = handles.closeHandle;
pub const liteOpenModeCanWrite = handles.liteOpenModeCanWrite;
pub const currentIdentityReadGenerationForHandle = handles.currentIdentityReadGenerationForHandle;
pub const stampSearchRequestIdentityGeneration = handles.stampSearchRequestIdentityGeneration;
pub const ReadableLeaseHookFn = handles.ReadableLeaseHookFn;
pub const ReadableLeaseHook = handles.ReadableLeaseHook;
pub const asHandle = handles.asHandle;
pub const HandleRegistryOf = handles.HandleRegistryOf;
pub const HandleRegistry = handles.HandleRegistry;
pub const handle_registry = &handles.handle_registry;
pub const closeHandleId = handles.closeHandleId;
pub const handleLockIo = handles.handleLockIo;
pub const dupBytes = handles.dupBytes;
pub const JsonSearchAggregationRequest = handles.JsonSearchAggregationRequest;
pub const JsonNumericRangeRequest = handles.JsonNumericRangeRequest;
pub const JsonDateRangeRequest = handles.JsonDateRangeRequest;
pub const JsonDistanceRangeRequest = handles.JsonDistanceRangeRequest;
pub const JsonSearchAggregationBucket = handles.JsonSearchAggregationBucket;
pub const JsonSearchAggregationResult = handles.JsonSearchAggregationResult;
pub const freeAggregationRequests = handles.freeAggregationRequests;
pub const freeRawBuffer = handles.freeRawBuffer;
pub const computeSearchAggregations = handles.computeSearchAggregations;
pub const computeSingleAggregation = handles.computeSingleAggregation;
pub const NumericMetricKind = handles.NumericMetricKind;
pub const computeNumericMetricAggregation = handles.computeNumericMetricAggregation;
pub const computeCardinalityAggregation = handles.computeCardinalityAggregation;
pub const computeTermsAggregation = handles.computeTermsAggregation;
pub const computeHistogramAggregation = handles.computeHistogramAggregation;
pub const computeDateHistogramAggregation = handles.computeDateHistogramAggregation;
pub const computeRangeAggregation = handles.computeRangeAggregation;
pub const matchesNumericRangeValue = handles.matchesNumericRangeValue;
pub const matchesDateRangeValue = handles.matchesDateRangeValue;
pub const matchesGeoDistanceValue = handles.matchesGeoDistanceValue;
pub const accumulateNumericJsonValue = handles.accumulateNumericJsonValue;
pub const collectCardinalityValues = handles.collectCardinalityValues;
pub const appendTermAggregationValuesZig = handles.appendTermAggregationValuesZig;
pub const jsonValueToTermKey = handles.jsonValueToTermKey;
pub const stringifyJsonValueCompact = handles.stringifyJsonValueCompact;
pub const distanceToMeters = handles.distanceToMeters;
pub const extractGeoPointFieldFromStoredJson = handles.extractGeoPointFieldFromStoredJson;
pub const jsonValueToF64 = handles.jsonValueToF64;
pub const fillHistogramBucketKeys = handles.fillHistogramBucketKeys;
pub const fillDateHistogramBucketKeys = handles.fillDateHistogramBucketKeys;
pub const nextDateHistogramBucketKey = handles.nextDateHistogramBucketKey;
pub const addCalendarMonths = handles.addCalendarMonths;
pub const addCalendarYears = handles.addCalendarYears;
pub const civilDateToBucketNs = handles.civilDateToBucketNs;
pub const extractNumericFieldFromStoredJson = handles.extractNumericFieldFromStoredJson;
pub const extractTimestampFieldFromStoredJson = handles.extractTimestampFieldFromStoredJson;
pub const parseDateInterval = handles.parseDateInterval;
pub const parseRfc3339ToNs = handles.parseRfc3339ToNs;
pub const daysFromCivil = handles.daysFromCivil;
pub const formatRfc3339Bucket = handles.formatRfc3339Bucket;
pub const civilFromDays = handles.civilFromDays;
pub const extractValueAtPath = handles.extractValueAtPath;
pub const replication_ingress = antfly.capi_dependencies.storage_db_replication_ingress;
pub const raft_engine = @import("raft_engine");
pub const kernel_error_identity = @import("kernel_error_identity");
pub const kernel_wal_owner = antfly.kernel_wal_owner;

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

pub const backend_types = antfly.storage_backend;
pub const raft_mod = antfly.raft;
pub const hot_standby_seed_activation = antfly.hot_standby_seed_activation;
pub const aggregations_contract = aggregations_mod.contract;
pub const runtime_status = antfly.public_api.runtime_status;
pub const shard_state_store = antfly.data_snapshot;
pub const data_raft_apply = antfly.data_raft_apply;
pub const metadata_raft_apply = antfly.metadata_raft_apply;
pub const metadata_table_manager = antfly.metadata_table_manager;
pub const metadata_table_provisioner = antfly.metadata_table_provisioner;
pub const data_raft_projection_wire = antfly.data_raft_projection_wire;
pub const backups_api = antfly.public_api.backups;
pub const backup_restore = antfly.raft.storage.backup_restore;
pub const common_config = antfly.common_config;
pub const common_secrets = antfly.common_secrets;
pub const scraping = antfly.scraping;
pub const raft_catalog = antfly.raft_catalog;
pub const kernel_runtime_services = antfly.kernel_runtime_services;

pub const StorageOwnerContext = struct {
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

    pub fn lock(self: *StorageOwnerContext) void {
        antfly.platform_sync.lockYielding(&self.mutex);
    }

    pub fn acquire(self: *StorageOwnerContext) void {
        self.lock();
        defer self.mutex.unlock();
        self.active_owners += 1;
    }

    pub fn release(self: *StorageOwnerContext) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.active_owners > 0);
        self.active_owners -= 1;
    }

    pub fn antflyProvider(self: *StorageOwnerContext) ?managed_embedder.AntflyProvider {
        const lifetime = if (self.inference_lifetime) |*value| value else return null;
        return inference_provider.inferenceBoundaryProvider(lifetime);
    }

    pub fn remoteContent(self: *const StorageOwnerContext) ?*const scraping.RemoteContentConfig {
        return if (self.remote_content_security != null) &self.remote_content else null;
    }

    pub fn deinitIfIdle(self: *StorageOwnerContext) bool {
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

pub const SystemStoreHandle = struct {
    store: *antfly.storage_backend_erased.Store,
    context: *StorageOwnerContext,
    refs: std.atomic.Value(usize) = .init(1),
};

pub const SystemReadTxnHandle = struct {
    alloc: Allocator,
    txn: antfly.storage_backend_erased.ReadTxn,
};

pub const SystemCurrentScanTxnHandle = struct {
    alloc: Allocator,
    txn: antfly.storage_backend_erased.CurrentScanTxn,
};

pub const SystemWriteTxnHandle = struct {
    alloc: Allocator,
    txn: antfly.storage_backend_erased.WriteTxn,
};

pub const SystemCursorHandle = struct {
    alloc: Allocator,
    cursor: antfly.storage_backend_erased.Cursor,
};

pub const DataApplyStoreHandle = struct {
    alloc: Allocator,
    store: data_raft_apply.RaftApplyStore,
    context: ?*StorageOwnerContext,
};

pub const MetadataApplyStoreHandle = struct {
    alloc: Allocator,
    store: metadata_raft_apply.RaftApplyStore,
    context: ?*StorageOwnerContext,
    system_store: ?*SystemStoreHandle = null,
    listener_bridges: std.ArrayListUnmanaged(*MetadataListenerBridge) = .empty,
    listener_mutex: std.Io.Mutex = .init,
};

pub const MetadataPreparedSnapshotHandle = struct {
    source: raft_engine.runtime.storage_iface.SnapshotSource,
};

pub const MetadataListenerBridge = struct {
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

    pub fn projectionKindToAbi(kind: metadata_raft_apply.ProjectionSignalKind) kernel_owner_abi.MetadataProjectionSignalKind {
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

    pub fn projectionKindFromAbi(kind: kernel_owner_abi.MetadataProjectionSignalKind) metadata_raft_apply.ProjectionSignalKind {
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

    pub fn onProjection(ptr: *anyopaque, signal: metadata_raft_apply.ProjectionSignal) void {
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

    pub fn beforeProjectionCommit(ptr: *anyopaque) void {
        const self: *MetadataListenerBridge = @ptrCast(@alignCast(ptr));
        if (self.request.before_projection_commit_fn) |callback| callback(self.request.context);
    }

    pub fn afterProjectionCommit(ptr: *anyopaque) void {
        const self: *MetadataListenerBridge = @ptrCast(@alignCast(ptr));
        if (self.request.after_projection_commit_fn) |callback| callback(self.request.context);
    }

    pub fn matchesKey(_: *anyopaque, _: metadata_raft_apply.CommittedKeySignal) bool {
        return true;
    }

    pub fn onCommittedKey(ptr: *anyopaque, signal: metadata_raft_apply.CommittedKeySignal) void {
        const self: *MetadataListenerBridge = @ptrCast(@alignCast(ptr));
        const callback = self.request.committed_key_fn orelse return;
        callback(self.request.context, signal.metadata_group_id, .fromSlice(signal.key));
    }
};

pub const DataApplyGroupTransitionHandle = struct {
    transition: data_raft_apply.RaftApplyStore.ActiveGroupTransition,
    active: bool = true,
};

pub const DataApplyPreparedSnapshotHandle = struct {
    prepared: *data_raft_apply.RaftApplyStore.PreparedSnapshot,
    materialized: bool = false,
};

pub const StorageOwnerTransactionRecovery = struct {
    alloc: Allocator,
    config: kernel_owner_abi.TransactionRecoveryConfig,
    owner_id: []u8,

    pub fn init(
        alloc: Allocator,
        config: kernel_owner_abi.TransactionRecoveryConfig,
    ) !StorageOwnerTransactionRecovery {
        return .{
            .alloc = alloc,
            .config = config,
            .owner_id = try alloc.dupe(u8, config.owner_id.slice()),
        };
    }

    pub fn deinit(self: *StorageOwnerTransactionRecovery) void {
        self.alloc.free(self.owner_id);
        self.* = undefined;
    }

    pub fn callbackStatus(status: kernel_owner_abi.Status) !void {
        return kernel_error_identity.statusToError(status);
    }

    pub fn resolveParticipant(
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

    pub fn ownsRecovery(ptr: *anyopaque, owner_participant: []const u8) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.config.owns_recovery_fn orelse return false;
        return callback(self.config.callback_ctx, .fromSlice(owner_participant)) != 0;
    }

    pub fn acknowledgeParticipant(
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

    pub fn acknowledgeParticipants(ptr: *anyopaque, txn_id: transactions_mod.TxnId, owner_participant: []const u8, participants: []const []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.config.acknowledge_participants_fn orelse return error.UnsupportedOperation;
        if (participants.len == 0 or participants.len > 64) return error.InvalidParticipant;
        var members: [64]kernel_owner_abi.BorrowedBytes = undefined;
        for (participants, members[0..participants.len]) |participant, *member| member.* = .fromSlice(participant);
        const abi_txn_id = kernel_owner_abi.TxnId{ .bytes = txn_id };
        try callbackStatus(callback(self.config.callback_ctx, &abi_txn_id, .fromSlice(owner_participant), &members, participants.len));
    }

    pub fn cleanupTransaction(
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

    pub fn dbConfig(self: *StorageOwnerTransactionRecovery) db_mod.transaction_runtime.Config {
        return @import("../storage/server_transaction_recovery.zig").configFor(StorageOwnerTransactionRecovery, self, serverConfig);
    }

    fn serverConfig(self: *StorageOwnerTransactionRecovery) @import("../storage/server_transaction_recovery.zig").Config {
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
            .acknowledge_participants_fn = if (self.config.replicated_metadata != 0 and self.config.acknowledge_participants_fn != null) acknowledgeParticipants else null,
            .cleanup_transaction_fn = if (self.config.replicated_metadata != 0) cleanupTransaction else null,
        };
    }
};

pub const StorageOwnerRuntimeHooks = struct {
    pub fn coordinatedTtlPort(self: *StorageOwnerRuntimeHooks) ?antfly.capi_dependencies.storage_coordinated_ttl.Port {
        if (self.config.coordinated_ttl_enqueue_fn == null) return null;
        return .{ .ptr = self, .expire_fn = enqueueCoordinatedTtl };
    }

    pub fn enqueueCoordinatedTtl(ptr: *anyopaque, request: antfly.capi_dependencies.storage_coordinated_ttl.Request) !u32 {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.coordinated_ttl_enqueue_fn orelse return error.CoordinatedTtlBackpressure;
        if (request.candidates.len > kernel_owner_abi.coordinated_ttl_page_capacity) return error.CoordinatedTtlBackpressure;
        var candidates: [kernel_owner_abi.coordinated_ttl_page_capacity]kernel_owner_abi.CoordinatedTtlCandidate = undefined;
        for (request.candidates, candidates[0..request.candidates.len]) |source, *dest| {
            dest.* = .{ .key = .fromSlice(source.key), .row_version = source.row_version, .ttl_timestamp_ns = source.ttl_timestamp_ns, .expected_content_digest = source.expected_content_digest };
        }
        const wire = kernel_owner_abi.CoordinatedTtlRequest{
            .table_id = request.table_id,
            .group_id = self.group_id,
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

    pub fn nativeAuthorityPermitted(ptr: *const anyopaque) bool {
        const self: *const StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.native_authority_fn orelse return false;
        return callback(self.config.native_authority_ctx) != 0;
    }

    pub fn nativeMigrationPolicy(self: *const StorageOwnerRuntimeHooks) ?db_mod.DenseNativeMigrationPolicySource {
        if (self.config.native_authority_fn == null) return null;
        return .{ .ptr = self, .authority_permitted = nativeAuthorityPermitted };
    }

    config: kernel_owner_abi.RuntimeHooksConfig,
    group_id: u64,
    artifact_upload_recovery: @import("../storage/artifact_upload_recovery.zig").Scheduler = .{},

    pub fn artifactPublicationDispatcher(self: *StorageOwnerRuntimeHooks) ?db_mod.ArtifactPublicationDispatcher {
        if (self.config.artifact_publication_enqueue_fn == null) return null;
        return .{ .ptr = self, .enqueue = enqueueArtifactPublication, .upload_recovery = .{ .recover = recoverArtifactUploads, .should_poll = shouldRecoverArtifactUploads } };
    }

    fn shouldRecoverArtifactUploads(ptr: *anyopaque, tick: @import("../storage/db/artifact_publication.zig").UploadRecoveryTick) bool {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        return self.artifact_upload_recovery.shouldPoll(tick);
    }

    fn recoverArtifactUploads(ptr: *anyopaque, invocation: @import("../storage/db/artifact_publication.zig").UploadRecoveryInvocation) !bool {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        return self.artifact_upload_recovery.advance(self.artifactPublicationDispatcher().?, invocation);
    }

    pub fn enqueueArtifactPublication(ptr: *anyopaque, namespace: [24]u8, command: []const u8) !void {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const enqueue = self.config.artifact_publication_enqueue_fn orelse return error.ArtifactCatalogDrift;
        try kernel_error_identity.statusToError(enqueue(self.config.artifact_publication_ctx, self.group_id, &namespace, .fromSlice(command)));
    }

    const CandidateCapture = struct {
        alloc: Allocator,
        value: ?[]u8 = null,

        pub fn consume(
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

        pub fn forward(
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

    pub fn candidateSource(self: *StorageOwnerRuntimeHooks) ?db_mod.CandidateSource {
        if (self.config.resolution_candidates.get_fn == null) return null;
        return .{ .ptr = self, .vtable = &candidate_vtable };
    }

    const candidate_vtable = db_mod.CandidateSource.VTable{
        .get = candidateGet,
        .scan_prefix = candidateScanPrefix,
        .nearest = candidateNearest,
    };

    pub fn candidateGet(
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

    pub fn candidateScanPrefix(
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

    pub fn candidateNearest(
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

    pub fn entitySink(self: *StorageOwnerRuntimeHooks) ?db_mod.EntitySink {
        if (self.config.entity_sink.upsert_fn == null) return null;
        return .{ .ptr = self, .vtable = &entity_sink_vtable };
    }

    const entity_sink_vtable = db_mod.EntitySink.VTable{
        .upsert = entityUpsert,
        .upsert_batch = entityUpsertBatch,
    };

    pub fn entityUpsert(
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

    pub fn entityUpsertBatch(
        ptr: *anyopaque,
        alloc: Allocator,
        entries: []const db_mod.EntityUpsert,
    ) anyerror!void {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.entity_sink.upsert_batch_fn orelse {
            for (entries) |entry| if (entry.storage_table != null or entry.delete) return error.EntityPromotionAtomicCommitUnavailable;
            for (entries) |entry| try entityUpsert(ptr, alloc, entry.table, entry.key, entry.doc_json);
            return;
        };
        const encoded = try alloc.alloc(kernel_owner_abi.EntityUpsert, entries.len);
        defer alloc.free(encoded);
        for (entries, encoded) |source, *destination| destination.* = .{
            .table = .fromSlice(source.table),
            .storage_table = .fromSlice(source.storage_table orelse ""),
            .key = .fromSlice(source.key),
            .doc_json = .fromSlice(source.doc_json),
            .delete = @intFromBool(source.delete),
        };
        try kernel_error_identity.statusToError(callback(
            self.config.entity_sink.callback_ctx,
            if (encoded.len == 0) null else encoded.ptr,
            @intCast(encoded.len),
        ));
    }

    pub fn promotionOwner(self: *StorageOwnerRuntimeHooks) ?db_mod.PromotionOwner {
        if (self.config.promotion_owner_fn == null) return null;
        return .{ .ptr = self, .vtable = &promotion_owner_vtable };
    }

    const promotion_owner_vtable = db_mod.PromotionOwner.VTable{ .is_local_owner = isLocalPromotionOwner };

    pub fn isLocalPromotionOwner(ptr: *anyopaque) bool {
        const self: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(ptr));
        const callback = self.config.promotion_owner_fn orelse return true;
        return callback(self.config.promotion_owner_ctx, self.group_id) != 0;
    }
};

pub const StorageSnapshot = struct {
    alloc: Allocator,
    preparation: db_mod.generation_lifecycle.PreparationTransition,
    staged: db_mod.generation_lifecycle.StagedGeneration,
    transition: ?db_mod.generation_lifecycle.ExclusiveTransition = null,
    restore_live_path: ?[]u8 = null,
    promoted: bool = false,
    published: bool = false,
    finalized: bool = false,

    pub fn deinit(self: *StorageSnapshot) void {
        self.staged.deinit();
        if (self.transition) |*transition| transition.deinit();
        self.preparation.deinit();
        if (self.restore_live_path) |path| self.alloc.free(path);
        const alloc = self.alloc;
        self.* = undefined;
        alloc.destroy(self);
    }
};

pub fn asStorageOwnerContext(ptr: ?*anyopaque) ?*StorageOwnerContext {
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
pub fn createStorageOwnerContext(services: kernel_runtime_services.Request) !*StorageOwnerContext {
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

pub fn storageOwnerContextCacheKindStats(stats: anytype) kernel_owner_abi.ContextCacheKindStats {
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
    const owner_context = asStorageOwnerContext(context) orelse return .invalid_argument;
    // The result has grown across ABI versions. Read only the leading version
    // word, which every revision shares, and reject a caller built against
    // another layout before writing: it may have reserved a smaller struct.
    if (out_result.version != kernel_owner_abi.abi_version) return .invalid_abi;
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
    const resources = owner_context.resources.resource_manager.snapshot();
    out_result.resource_memory = kernel_owner_abi.ContextResourceBudgetStats.fromResourceStats(resources.memory);
    comptime std.debug.assert(@import("../storage/resource_manager.zig").slice_count <= kernel_owner_abi.context_resource_slice_capacity);
    const slice_count = resources.slices.len;
    out_result.resource_slice_count = @intCast(slice_count);
    for (resources.slices[0..slice_count], out_result.resource_slices[0..slice_count]) |slice, *out| {
        out.* = kernel_owner_abi.ContextResourceBudgetStats.fromResourceStats(slice);
    }
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

pub fn asSystemStore(ptr: ?*anyopaque) ?*SystemStoreHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asSystemReadTxn(ptr: ?*anyopaque) ?*SystemReadTxnHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asSystemCurrentScanTxn(ptr: ?*anyopaque) ?*SystemCurrentScanTxnHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asSystemWriteTxn(ptr: ?*anyopaque) ?*SystemWriteTxnHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asSystemCursor(ptr: ?*anyopaque) ?*SystemCursorHandle {
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
    if (handle.refs.fetchSub(1, .acq_rel) != 1) return;
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

pub fn contextLiteBackend(context_ptr: ?*anyopaque) ?*lite_backend.Handle {
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

pub fn asDataApplyStore(ptr: ?*anyopaque) ?*DataApplyStoreHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asMetadataApplyStore(ptr: ?*anyopaque) ?*MetadataApplyStoreHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asMetadataPreparedSnapshot(ptr: ?*anyopaque) ?*MetadataPreparedSnapshotHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asDataApplyGroupTransition(ptr: ?*anyopaque) ?*DataApplyGroupTransitionHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn asDataApplyPreparedSnapshot(ptr: ?*anyopaque) ?*DataApplyPreparedSnapshotHandle {
    return @ptrCast(@alignCast(ptr orelse return null));
}

pub fn metadataProjectionJson(
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

pub fn metadataProjectionStatusFromError(err: anyerror) kernel_owner_abi.Status {
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
        .borrowed_store = if (asSystemStore(request.system_store)) |system| system.store else null,
    }) catch |err| return storageOwnerStatusFromError(err);
    errdefer store.deinit();
    const handle = alloc.create(MetadataApplyStoreHandle) catch return .out_of_memory;
    const system_store = asSystemStore(request.system_store);
    if (system_store) |system| _ = system.refs.fetchAdd(1, .monotonic);
    handle.* = .{ .alloc = alloc, .store = store, .context = context, .system_store = system_store };
    context_borrowed = false;
    out_store.* = handle;
    return .ok;
}

pub fn metadataApplyStoreClose(store_ptr: ?*anyopaque) callconv(.c) void {
    const handle = asMetadataApplyStore(store_ptr) orelse return;
    const alloc = handle.alloc;
    const context = handle.context;
    handle.store.deinit();
    if (handle.system_store) |system| storageSystemStoreClose(system);
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

pub fn metadataApplyStoreBindHotStandby(
    store_ptr: ?*anyopaque,
    request: *const kernel_owner_abi.MetadataHABindRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asMetadataApplyStore(store_ptr) orelse return .invalid_argument;
    const port: ?antfly.capi_dependencies.storage_metadata_hot_standby_port.Port = if (request.port) |ptr| @as(*const antfly.capi_dependencies.storage_metadata_hot_standby_port.Port, @ptrCast(@alignCast(ptr))).* else null;
    handle.store.bindHotStandbyPort(port) catch |err| return storageOwnerStatusFromError(err);
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
        .fk_initial_create_preflight => blk: {
            if (request.key.len == 0 or request.key.len > 2 * 1024 * 1024) break :blk .invalid_argument;
            const result: antfly.capi_dependencies.metadata_storage_raft_apply_contract.InitialFkPreflight = result: {
                handle.store.preflightFkInitialCreateCommand(request.group_id, request.key.slice()) catch |err| break :result switch (err) {
                    error.GenerationPublicationChanged => .generation_changed,
                    error.CatalogAlreadyExists => .catalog_exists,
                    error.TableTransitionActive => .table_transition_active,
                    else => break :blk storageOwnerStatusFromError(err),
                };
                break :result .ready;
            };
            break :blk metadataProjectionJson(alloc, out_json, result);
        },
        .flush_ha_outbox => blk: {
            handle.store.flushHotStandbyOutbox() catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .apply_ha_record => blk: {
            if (request.key.len > 2 * 1024 * 1024) break :blk .invalid_argument;
            const record = antfly.capi_dependencies.storage_hot_standby_replication_record.decode(request.key.slice()) catch |err| break :blk storageOwnerStatusFromError(err);
            handle.store.applyHotStandbyRecord(record) catch |err| break :blk storageOwnerStatusFromError(err);
            break :blk metadataProjectionJson(alloc, out_json, true);
        },
        .export_ha_checkpoint, .import_ha_checkpoint => blk: {
            const path = request.key.slice();
            if (path.len == 0 or path.len > 4096 or std.mem.indexOfScalar(u8, path, 0) != null) break :blk .invalid_argument;
            const io = handle.store.io_impl.io();
            if (request.kind == .export_ha_checkpoint) {
                const value = handle.store.exportHotStandbyCheckpoint(io, path) catch |err| break :blk storageOwnerStatusFromError(err);
                break :blk metadataProjectionJson(alloc, out_json, value);
            }
            handle.store.importHotStandbyCheckpoint(io, path, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err);
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
        .fk_initial_group_reservation => blk: {
            const value = handle.store.initialGroupReservation(request.group_id, request.arg0) catch |err| break :blk storageOwnerStatusFromError(err);
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
                .sql_setting_snapshot => |input| {
                    const value = handle.store.sqlSettingSnapshotJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .sql_policy_snapshot => |input| {
                    const value = handle.store.sqlPolicySnapshotJson(a, group_id, input.table_id, input.principal, input.database, input.roles) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .sql_policy_install_snapshot => |input| {
                    const value = handle.store.sqlPolicyInstallSnapshotJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .sql_policy_publication_status => |input| {
                    const value = handle.store.sqlPolicyPublicationStatusJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .sql_policy_publication_work => |input| {
                    const value = handle.store.sqlPolicyPublicationWorkJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .sql_policy_begin_command => |input| {
                    const value = handle.store.sqlPolicyBeginCommandJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .require_policy_index_mutation_allowed => |input| {
                    handle.store.requirePolicyIndexMutationAllowed(group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, true);
                },
                .require_policy_topology_mutation_allowed => |input| {
                    handle.store.requirePolicyTopologyMutationAllowed(group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, true);
                },
                .fk_generation_publication_status => |input| {
                    const value = handle.store.fkGenerationPublicationStatusJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_generation_publication_work => |input| {
                    const value = handle.store.fkGenerationPublicationWorkJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_generation_publication_decision => |input| {
                    const value = handle.store.fkGenerationPublicationDecisionJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_generation_publication_source_decision => |input| {
                    const value = handle.store.fkGenerationPublicationSourceDecisionJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_initial_create_prepare => |input| {
                    const value = handle.store.fkInitialCreatePrepareJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_initial_child_decision => |input| {
                    const value = handle.store.fkInitialChildDecisionJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_initial_create_status => |input| {
                    const value = handle.store.fkInitialCreateStatusJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_generation_table_locked => |input| {
                    const value = handle.store.fkGenerationTableLockedJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_initial_create_work => |input| {
                    const value = handle.store.fkInitialCreateWorkJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_initial_retirement_page => |input| {
                    const value = handle.store.fkInitialRetirementTicketPageJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .store_root_control => |input| {
                    const value = handle.store.storeRootControlJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
                },
                .fk_initial_parent_decision => |input| {
                    const value = handle.store.fkInitialParentDecisionJson(a, group_id, input) catch |err| break :blk storageOwnerStatusFromError(err);
                    break :blk metadataProjectionJson(alloc, out_json, value);
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
        .merge_membership => blk: {
            const value = handle.store.observeMergeMembership(alloc, request.group_id) catch |err|
                return storageOwnerStatusFromError(err);
            break :blk std.json.Stringify.valueAlloc(alloc, value, .{}) catch |err|
                return storageOwnerStatusFromError(err);
        },
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

pub fn dataApplyLatestResult(latest: ?data_raft_apply.AppliedDataBatch) kernel_owner_abi.DataApplyLatestResult {
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

pub fn dataApplyExpectedBatch(value: kernel_owner_abi.DataApplyLatestResult) !?data_raft_apply.AppliedDataBatch {
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

pub fn releaseBorrowedTransitionOwner(_: *anyopaque) void {}

pub fn validateLocalTransitionOwner(
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

pub fn localTransitionIdentity(
    request: *const kernel_owner_abi.LocalTransitionRequest,
    target: bool,
) db_mod.DocIdentityNamespace {
    return .{
        .table_id = request.table_id,
        .shard_id = if (target) request.target_identity_shard_id else request.source_identity_shard_id,
        .range_id = if (target) request.target_identity_range_id else request.source_identity_range_id,
    };
}

pub fn localTransitionSplitResult(status: anytype) kernel_owner_abi.LocalTransitionResult {
    return .{
        .kind = .split,
        .phase = @fromBackingInt(@backingInt(status.phase)),
        .has_source_split_phase = @intFromBool(status.source_split_phase != null),
        .source_split_phase = if (status.source_split_phase) |phase| @backingInt(phase) else 0,
        .bootstrapped = @intFromBool(status.bootstrapped),
        .replay_required = @intFromBool(status.replay_required),
        .replay_caught_up = @intFromBool(status.replay_caught_up),
        .cutover_ready = @intFromBool(status.cutover_ready),
        .peer_ready_for_reads = @intFromBool(status.destination_ready_for_reads),
        .primary_delta_sequence = status.source_delta_sequence,
        .secondary_delta_sequence = status.dest_delta_sequence,
    };
}

pub fn localTransitionMergeResult(status: anytype) kernel_owner_abi.LocalTransitionResult {
    return .{
        .kind = .merge,
        .phase = @fromBackingInt(@backingInt(status.phase)),
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

pub fn storageOwnerTargetAdvanced(
    ptr: *anyopaque,
    event: db_mod.QueryVisibilityEvent,
) void {
    if (event.change != .target_advanced) return;
    const handle: *Handle = @ptrCast(@alignCast(ptr));
    const table_name = handle.storage_owner_table_name orelse "";
    const group_id = handle.storage_owner_group_id;
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
    if ((runtime_hooks_config.artifact_publication_ctx == null) != (runtime_hooks_config.artifact_publication_enqueue_fn == null)) return .invalid_argument;
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
    if (candidate_configured or entity_sink_configured or runtime_hooks_config.promotion_owner_fn != null or runtime_hooks_config.native_authority_fn != null or runtime_hooks_config.coordinated_ttl_enqueue_fn != null or runtime_hooks_config.artifact_publication_enqueue_fn != null) {
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
        request.historical_raft_apply > 1 or
        (request.restore_cancel_recovery != 0 and request.restore_ha_replay != 0) or
        ((request.restore_cancel_recovery != 0 or request.restore_ha_replay != 0) and request.restore_bootstrap_json.len == 0) or request.restore_bootstrap_json.len > 16 * 1024 * 1024 or
        request.initial_child_bootstrap_json.len > 4096 or
        (request.initial_child_bootstrap_json.len != 0 and (request.restore_bootstrap_json.len != 0 or request.schema_json.len != 0 or request.indexes_json.len != 0))) return .invalid_argument;
    var restore_bootstrap: ?std.json.Parsed(antfly.capi_dependencies.storage_db_restore_staging_contract.OwnerBootstrap) = null;
    defer if (restore_bootstrap) |*parsed| parsed.deinit();
    if (request.restore_bootstrap_json.len != 0) {
        restore_bootstrap = std.json.parseFromSlice(antfly.capi_dependencies.storage_db_restore_staging_contract.OwnerBootstrap, alloc, request.restore_bootstrap_json.slice(), .{ .ignore_unknown_fields = false }) catch |err| return storageOwnerStatusFromError(err);
        const bootstrap = restore_bootstrap.?.value;
        bootstrap.validate() catch |err| return storageOwnerStatusFromError(err);
        const namespace = identity_namespace orelse return .invalid_argument;
        if (!namespace.eql(bootstrap.scope.target_namespace) or !std.mem.eql(u8, bootstrap.table_name, table_name) or !std.mem.eql(u8, bootstrap.schema_json, request.schema_json.slice()) or !std.mem.eql(u8, bootstrap.indexes_json, request.indexes_json.slice())) return storageOwnerStatusFromError(error.RestoreStagingScopeChanged);
    }
    var initial_child_bootstrap: ?std.json.Parsed(antfly.capi_dependencies.storage_db_relational_initial_child_publication.Bootstrap) = null;
    defer if (initial_child_bootstrap) |*parsed| parsed.deinit();
    if (request.initial_child_bootstrap_json.len != 0) {
        initial_child_bootstrap = std.json.parseFromSlice(antfly.capi_dependencies.storage_db_relational_initial_child_publication.Bootstrap, alloc, request.initial_child_bootstrap_json.slice(), .{ .ignore_unknown_fields = false }) catch |err| return storageOwnerStatusFromError(err);
        initial_child_bootstrap.?.value.validate() catch |err| return storageOwnerStatusFromError(err);
        const namespace = identity_namespace orelse return .invalid_argument;
        if (!namespace.eql(initial_child_bootstrap.?.value.namespace)) return storageOwnerStatusFromError(error.InvalidInitialChildPublication);
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
        .reject_stale_schema_before_index_load = request.historical_raft_apply == 0 and restore_bootstrap == null,
        .lsm_cache = if (owner_context) |context| &context.resources.lsm_cache else null,
        .hbc_cache = if (owner_context) |context| &context.resources.hbc_cache else null,
        .lsm_root_generation = request.lsm_root_generation,
        .resource_manager = if (owner_context) |context| &context.resources.resource_manager else null,
        .backend_runtime = if (owner_context) |context| context.backend_runtime.ptr() else null,
        .identity_namespace = identity_namespace,
        .initial_child_bootstrap = if (initial_child_bootstrap) |value| value.value else null,
        .prefer_existing_identity_namespace = identity_namespace != null,
        .transaction_recovery = if (recovery) |value| value.dbConfig() else .{},
        .resolution_candidate_source = if (runtime_hooks) |value| value.candidateSource() else null,
        .artifact_publication_dispatcher = if (runtime_hooks) |value| value.artifactPublicationDispatcher() else null,
        .entity_sink = if (runtime_hooks) |value| value.entitySink() else null,
        .promotion_owner = if (runtime_hooks) |value| value.promotionOwner() else null,
        // Reconcile the authoritative resolver catalog before autonomous
        // replay can hold its catalog fence or invoke distributed callbacks.
        .start_resolver_workers = false,
        .index_backends = .{ .dense_native_migration_policy_source = if (runtime_hooks) |value| value.nativeMigrationPolicy() else null },
        .secret_store = if (owner_context) |context| context.secret_store else null,
        .remote_content = if (owner_context) |context| context.remoteContent() else null,
        .start_optional_runtimes = restore_bootstrap == null and initial_child_bootstrap == null,
        .start_index_workers = restore_bootstrap == null and initial_child_bootstrap == null,
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
    if ((request.row_policy_authority_secret.len != 0 and request.row_policy_authority_secret.ptr == null) or
        (request.row_policy_authority_issuer.len != 0 and request.row_policy_authority_issuer.ptr == null) or
        (request.row_policy_authority_secret.len == 0) != (request.row_policy_authority_issuer.len == 0) or
        request.row_policy_authority_secret.len > 4096 or request.row_policy_authority_issuer.len > 256)
        return .invalid_argument;
    const owned_policy_secret = if (request.row_policy_authority_secret.len != 0)
        alloc.dupe(u8, request.row_policy_authority_secret.slice()) catch return .out_of_memory
    else
        null;
    defer if (!success) if (owned_policy_secret) |secret| {
        @memset(secret, 0);
        alloc.free(secret);
    };
    const owned_policy_issuer = if (request.row_policy_authority_issuer.len != 0)
        alloc.dupe(u8, request.row_policy_authority_issuer.slice()) catch return .out_of_memory
    else
        null;
    defer if (!success) if (owned_policy_issuer) |issuer| alloc.free(issuer);
    const handle = alloc.create(Handle) catch return .out_of_memory;
    defer if (!success) alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .db = db_mod.DB.open(alloc, path, open_options) catch |err| {
            // A descriptor captured before structural reconciliation can outlive
            // the owner that installed a newer durable schema. Reject the stale
            // open without making Raft apply fatal; its next attempt reloads the
            // catalog descriptor. Keep exact restore bootstrap failures strict.
            if (err == error.SchemaVersionRegression and restore_bootstrap == null)
                return storageOwnerStatusFromError(error.StorageBusy);
            std.log.err("storage owner open failed table={s} group_id={} err={s}", .{
                table_name, request.group_id, @errorName(err),
            });
            return storageOwnerStatusFromError(err);
        },
        .storage_owner_path = owned_path,
        .storage_owner_table_name = owned_table_name,
        .row_policy_authority_secret = owned_policy_secret,
        .row_policy_authority_issuer = owned_policy_issuer,
        .storage_owner_group_id = request.group_id,
        .storage_owner_root_generation = request.lsm_root_generation,
        .storage_owner_context = owner_context,
        .storage_owner_transaction_recovery = recovery,
        .storage_owner_runtime_hooks = runtime_hooks,
        .server_cleanup = cleanupServerHandle,
        .server_context_release = releaseServerContext,
        .storage_owner_target_observer = request.target_observer,
    };
    defer if (!success) handle.db.close();
    handle.db.local_execution.row_policy_authority_secret = owned_policy_secret;
    handle.db.local_execution.row_policy_authority_issuer = owned_policy_issuer;
    handle.db.local_execution.row_policy_table_name = owned_table_name;
    if (runtime_hooks) |hooks| handle.db.setCoordinatedTtl(hooks.coordinatedTtlPort());
    if (request.target_observer.notify != null) handle.db.setQueryVisibilityHook(.{
        .ptr = handle,
        .on_change = storageOwnerTargetAdvanced,
    });
    // Configuration can start DB-owned workers. Publish their pointers only
    // after the DB occupies its final address, and drain them on failure.
    if (restore_bootstrap) |bootstrap| {
        local_write.configureRestoreOwnerDb(alloc, &handle.db, bootstrap.value, request.restore_cancel_recovery != 0, request.restore_ha_replay != 0) catch |err| return storageOwnerStatusFromError(err);
    } else {
        const deferred = local_write.configureStorageKernelOwnerDbAtOpen(
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
            request.historical_raft_apply != 0,
        ) catch |err| return storageOwnerStatusFromError(err);
        if (request.owner_catalog_deferred_out) |out| out.* = @intFromBool(deferred);
    }
    // DB.open returns by value. Only now is the compiled owner's DB at its
    // permanent address with configuration installed; use the same startup as
    // resident caches so relational builds, retirement and durable outboxes
    // make progress. Hidden restore owners must remain unpublished/quiescent.
    if (restore_bootstrap == null) {
        handle.db.activateResolverReplayRuntimes() catch |err| {
            return storageOwnerStatusFromError(err);
        };
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
    closeHandleId(owner);
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
        if (asStorageOwnerContext(handle.storage_owner_context)) |context| context.backend_runtime.ptr() else null,
        if (asStorageOwnerContext(handle.storage_owner_context)) |context| context.antflyProvider() else null,
        if (asStorageOwnerContext(handle.storage_owner_context)) |context| context.secret_store else null,
        if (asStorageOwnerContext(handle.storage_owner_context)) |context| context.remoteContent() else null,
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

pub const OwnerRepairControls = struct {
    wire: kernel_owner_abi.RepairControls,
    pub fn cancelled(ptr: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return if (self.wire.cancelled) |check| check(self.wire.context) != 0 else false;
    }
    pub fn yieldRequested(ptr: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return if (self.wire.yield_requested) |check| check(self.wire.context) != 0 else false;
    }
    pub fn activationAllowed(ptr: *anyopaque) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return if (self.wire.activation_allowed) |check| check(self.wire.context) != 0 else true;
    }
    pub fn options(self: *@This()) db_mod.types.ArtifactRepairRunOptions {
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
            if (asStorageOwnerContext(handle.storage_owner_context)) |context| context.backend_runtime.ptr() else null,
            if (asStorageOwnerContext(handle.storage_owner_context)) |context| context.antflyProvider() else null,
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

pub const StorageOwnerBulkCallbacks = struct {
    request: *const kernel_owner_abi.BulkFinishRequest,

    pub fn progress(ptr: *anyopaque, progress_value: backend_types.BulkIngestFinishOptions.Progress) void {
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

    pub fn admission(ptr: *anyopaque) !void {
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

pub fn storageOwnerOperationTableName(
    handle: *const Handle,
    request: *const kernel_owner_abi.JsonOperationRequest,
) ?[]const u8 {
    return storageOwnerTableName(handle, request.table_name);
}

pub fn storageHotStandbySeedFailure(
    err: anyerror,
    operation: kernel_owner_abi.HASeedOperation,
    out_failure: *kernel_owner_abi.FailureIdentity,
) kernel_owner_abi.Status {
    out_failure.* = kernel_error_identity.failureFromError(
        err,
        .storage_owner,
        kernel_owner_abi.abi_version,
        @backingInt(operation),
    );
    return out_failure.status;
}

pub fn validateHotStandbySeedRequest(
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

pub fn storageHotStandbySeedActivateJson(
    request: *const kernel_owner_abi.HASeedJsonRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    const operation = kernel_owner_abi.HASeedOperation.activate;
    const request_json = validateHotStandbySeedRequest(request, operation) catch |err|
        return storageHotStandbySeedFailure(err, operation, out_failure);
    const alloc = std.heap.c_allocator;
    var parsed = std.json.parseFromSlice(hot_standby_seed_activation.ActivateRequest, alloc, request_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return storageHotStandbySeedFailure(error.InvalidArgument, operation, out_failure);
    defer parsed.deinit();
    var result = hot_standby_seed_activation.activate(alloc, parsed.value) catch |err|
        return storageHotStandbySeedFailure(err, operation, out_failure);
    alloc.free(result.generation_path);
    const response = result.active_receipt_json;
    result = undefined;
    out_response.* = .{
        .ptr = if (response.len == 0) null else response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

pub fn storageHotStandbySeedValidateJson(
    request: *const kernel_owner_abi.HASeedJsonRequest,
    out_result: *kernel_owner_abi.HASeedValidationResult,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    out_failure.* = .{};
    const operation = kernel_owner_abi.HASeedOperation.validate_activated_generation;
    const request_json = validateHotStandbySeedRequest(request, operation) catch |err|
        return storageHotStandbySeedFailure(err, operation, out_failure);
    const alloc = std.heap.c_allocator;
    var parsed = std.json.parseFromSlice(hot_standby_seed_activation.StartupExpectation, alloc, request_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return storageHotStandbySeedFailure(error.InvalidArgument, operation, out_failure);
    defer parsed.deinit();
    out_result.checkpoint_lsn = hot_standby_seed_activation.validateActivatedGeneration(alloc, parsed.value) catch |err|
        return storageHotStandbySeedFailure(err, operation, out_failure);
    return .ok;
}

pub fn storageHotStandbySeedPruneJson(
    request: *const kernel_owner_abi.HASeedJsonRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
    out_failure: *kernel_owner_abi.FailureIdentity,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    const operation = kernel_owner_abi.HASeedOperation.prune_activated_generations;
    const request_json = validateHotStandbySeedRequest(request, operation) catch |err|
        return storageHotStandbySeedFailure(err, operation, out_failure);
    const alloc = std.heap.c_allocator;
    var parsed = std.json.parseFromSlice(hot_standby_seed_activation.ActivatedGenerationGCRequest, alloc, request_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return storageHotStandbySeedFailure(error.InvalidArgument, operation, out_failure);
    defer parsed.deinit();
    var result = hot_standby_seed_activation.pruneActivatedGenerations(alloc, parsed.value) catch |err|
        return storageHotStandbySeedFailure(err, operation, out_failure);
    const response = result.result_json;
    result = undefined;
    out_response.* = .{
        .ptr = if (response.len == 0) null else response.ptr,
        .len = @intCast(response.len),
    };
    return .ok;
}

pub const StorageOwnerDocumentChildRangeDispatch = struct {
    callback_ctx: ?*anyopaque,
    callback_fn: kernel_owner_abi.DocumentChildRangeDispatchFn,

    pub fn dispatcher(self: *@This()) db_mod.DocumentArtifactChildRangeDispatcher {
        return .{ .ptr = self, .select_destination = server_document_child_range.selectPersistedDestination, .apply = apply };
    }

    pub fn apply(
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

pub const StorageOwnerCommittedBatchEffects = struct {
    callback_ctx: ?*anyopaque,
    callback_fn: kernel_owner_abi.CommittedBatchEffectsFn,

    pub fn observer(self: *@This()) db_mod.CommittedBatchEffectsObserver {
        return .{ .ptr = self, .apply = apply };
    }

    pub fn apply(ptr: *anyopaque, replay_payload: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try storageOwnerCallbackStatusToError(self.callback_fn(
            self.callback_ctx,
            .fromSlice(replay_payload),
        ));
    }
};

pub fn storageOwnerCallbackStatusToError(status: kernel_owner_abi.Status) !void {
    return kernel_error_identity.statusToError(status);
}

pub fn storageOwnerTableName(handle: *const Handle, table_name: kernel_owner_abi.BorrowedBytes) ?[]const u8 {
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

pub fn storageOwnerNativeFkGenerationControlJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.NativeFkGenerationControlRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    var owned = batch_api.parseInternalBatchRequest(handle.alloc, request.request_json.slice()) catch |err|
        return storageOwnerStatusFromError(err);
    defer owned.deinit(handle.alloc);
    handle.db.batchNativeFkGenerationApply(owned.req) catch |err| return storageOwnerStatusFromError(err);
    const response = batch_api.encodeBatchResponse(std.heap.c_allocator, owned.result()) catch |err|
        return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = response.len };
    return .ok;
}

pub fn storageOwnerNativeInitialChildControlJson(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.NativeInitialChildControlRequest,
    out_response: *kernel_owner_abi.OwnedBytes,
) callconv(.c) kernel_owner_abi.Status {
    out_response.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    if (request.operation_term != 1 or request.operation_index < 1 or request.operation_index > 3) return .invalid_argument;
    var owned = batch_api.parseInternalBatchRequest(handle.alloc, request.request_json.slice()) catch |err|
        return storageOwnerStatusFromError(err);
    defer owned.deinit(handle.alloc);
    handle.db.batchNativeInitialChildApply(owned.req, .{
        .term = request.operation_term,
        .index = request.operation_index,
    }) catch |err| return storageOwnerStatusFromError(err);
    const response = batch_api.encodeBatchResponse(std.heap.c_allocator, owned.result()) catch |err|
        return storageOwnerStatusFromError(err);
    out_response.* = .{ .ptr = response.ptr, .len = response.len };
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
        @backingInt(kernel_owner_abi.SyncLevel.propose) => .propose,
        @backingInt(kernel_owner_abi.SyncLevel.write) => .write,
        @backingInt(kernel_owner_abi.SyncLevel.full_text) => .full_text,
        @backingInt(kernel_owner_abi.SyncLevel.enrichments) => .enrichments,
        @backingInt(kernel_owner_abi.SyncLevel.full_index) => .full_index,
        else => return .invalid_argument,
    };
    switch (sync_level) {
        .propose, .write => return .ok,
        .full_text, .enrichments, .full_index => {},
    }
    const Adapter = struct {
        pub fn cancelled(ptr: *const anyopaque) bool {
            const req: *const kernel_owner_abi.SyncRequest = @ptrCast(@alignCast(ptr));
            const callback = req.cancellation_fn orelse return false;
            return callback(req.cancellation_ctx) != 0;
        }
    };
    handle.db.waitForCurrentSyncLevelWithCancellation(sync_level, .{ .ptr = request, .is_cancelled_fn = Adapter.cancelled }) catch |err| return storageOwnerStatusFromError(err);
    return .ok;
}

pub fn storageOwnerApplyHotStandbyReplicationRecord(
    owner: ?*anyopaque,
    request: *const kernel_owner_abi.HAReplicationRecordRequest,
) callconv(.c) kernel_owner_abi.Status {
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    replication_ingress.applyRecord(&handle.db, .{
        .kind = @fromBackingInt(request.record_kind),
        .payload_codec = @fromBackingInt(request.payload_codec),
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

pub var backup_pin_diagnostic_gate: antfly.capi_dependencies.api_bounded_diagnostic_gate.Gate = .{};

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
    const response = antfly.capi_dependencies.storage_db_backup_pin_control.execute(handle.alloc, &handle.db, handle.storage_owner_group_id, parsed.value, control) catch |err| {
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
        @backingInt(kernel_owner_abi.BackupFormat.native) => .native,
        @backingInt(kernel_owner_abi.BackupFormat.portable) => .portable,
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
        pub fn canceled(ptr: *const anyopaque) bool {
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

pub const RestoreRequestScope = struct {
    pub fn cancelled(ptr: *const anyopaque) bool {
        const request: *const kernel_owner_abi.RestorePrepareRequest = @ptrCast(@alignCast(ptr));
        const callback = request.cancellation_fn orelse return false;
        return callback(request.cancellation_ctx) != 0;
    }

    alloc: Allocator,
    manifest: std.json.Parsed(backups_api.TableBackupManifest),
    local_location: []u8,

    pub fn init(alloc: Allocator, request: *const kernel_owner_abi.RestorePrepareRequest) !RestoreRequestScope {
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

    pub fn source(
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

    pub fn deinit(self: *RestoreRequestScope) void {
        self.alloc.free(self.local_location);
        self.manifest.deinit();
        self.* = undefined;
    }
};

pub fn prepareStorageRestore(
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

pub fn prepareStorageSnapshot(request: *const kernel_owner_abi.SnapshotPrepareRequest) !*StorageSnapshot {
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
        try db.appendStagedSnapshotDocuments(&staged, state.byte_range, writes);
    }
    try db.finishStagedSnapshotRange(&staged, state.byte_range);
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

pub fn prepareNativeStorageSnapshot(request: *const kernel_owner_abi.SnapshotPrepareRequest, native: []const u8) !*StorageSnapshot {
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
        try @import("../storage/server_db_adapter.zig").verifySnapshot(&primary, expected);
    }
    try db_mod.DB.repairVerifiedPrimarySnapshot(alloc, &staged, expected.namespace, if (expected.native_index == 0 and expected.native_term == 0) null else .{ .term = expected.native_term, .index = expected.native_index }, request.lsm_root_generation);
    // Read-only verification preserves source intents and retention records.
    {
        var db = try db_mod.DB.open(alloc, staged.path(), .{
            .open_mode = .query_readonly,
            .primary_only_readonly = true,
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        defer db.close();
        try @import("../storage/server_db_adapter.zig").verifySnapshot(&db, expected);
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

pub const NativeSnapshotCapture = struct {
    capture: antfly.capi_dependencies.storage_db_native_raft_snapshot.Capture,
    lease_ctx: ?*anyopaque = null,
    release_lease: ?*const fn (?*anyopaque) callconv(.c) void = null,
};

pub fn storageOwnerSnapshotCapture(owner: ?*anyopaque, group_id: u64, through_index: u64, out_capture: *?*anyopaque) callconv(.c) kernel_owner_abi.Status {
    out_capture.* = null;
    const handle = asHandle(owner) orelse return .invalid_argument;
    if (handle.storage_owner_group_id != group_id) return .invalid_argument;
    const capture = std.heap.c_allocator.create(NativeSnapshotCapture) catch return .out_of_memory;
    const pinned = @import("../storage/server_db_adapter.zig").captureSnapshot(&handle.db, group_id, through_index) catch |err| {
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
        pub fn write(ptr: *anyopaque, writer: *std.Io.Writer, cancelled: *const std.atomic.Value(bool)) anyerror!void {
            const value: *NativeSnapshotCapture = @ptrCast(@alignCast(ptr));
            const Cancel = struct {
                pub fn check(raw: *const anyopaque) bool {
                    const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
                    return flag.load(.acquire);
                }
            };
            try value.capture.write(writer, .{ .ptr = cancelled, .is_cancelled_fn = Cancel.check });
        }
        pub fn destroy(ptr: *anyopaque) void {
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

pub fn batchStorageKernelJson(
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
    if (owned.req.row_policy_publication != null) return .invalid_argument;
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

pub fn replicatedBatchStorageKernelJson(
    handle: *Handle,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) kernel_owner_abi.Status {
    var owned = batch_api.parseInternalBatchRequest(handle.alloc, request_json.bytes()) catch |err|
        return storageOwnerStatusFromError(err);
    defer owned.deinit(handle.alloc);
    if (owned.req.row_policy_publication != null) return .invalid_argument;
    if (owned.req.relational_index_maintenance) |command| if (command.owner_group_id != handle.storage_owner_group_id) return storageOwnerStatusFromError(error.PreparedGenerationChanged);

    @import("../storage/server_transaction_dispatch.zig").applyStorageKernelReplicatedBatch(
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

pub fn replicatedBatchStorageKernelJsonAtRaftEntry(
    handle: *Handle,
    request_json: capi.Slice,
    raft_entry: db_mod.OrderedApplyReceipt,
    out_buf: *capi.Buffer,
) kernel_owner_abi.Status {
    var owned = batch_api.parseInternalBatchRequest(handle.alloc, request_json.bytes()) catch |err|
        return storageOwnerStatusFromError(err);
    defer owned.deinit(handle.alloc);
    if (owned.req.row_policy_publication) |publication| {
        if (publication.table_id != handle.db.core.identity_namespace.table_id or publication.owner_group_id != handle.storage_owner_group_id) return .invalid_argument;
        const bundle = owned.req.row_policy_install_bundle;
        // Bound the private ABI payload before crossing into the storage owner;
        // the owner independently checks the canonical bundle limit and shape.
        if (bundle.len == 0 or bundle.len > 4 * 1024 * 1024) return .invalid_argument;
        const receipt = handle.db.applyReplicatedRowPolicyPublication(bundle, publication, raft_entry) catch |err|
            return storageOwnerStatusFromError(err);
        var result = owned.result();
        result.row_policy_receipt = receipt;
        const response = batch_api.encodeBatchResponse(std.heap.c_allocator, result) catch |err|
            return storageOwnerStatusFromError(err);
        out_buf.* = .{ .ptr = response.ptr, .len = response.len };
        return .ok;
    }
    if (owned.req.relational_index_maintenance) |command| if (command.owner_group_id != handle.storage_owner_group_id) return storageOwnerStatusFromError(error.PreparedGenerationChanged);

    @import("../storage/server_db_adapter.zig").applyStorageKernelReplicatedBatchAtRaftEntry(
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
    _ = asHandle(owner) orelse return errors.statusFromError(error.InvalidArgument);
    const out: *provider.Provider = @ptrCast(@alignCast(output));
    // Provider callbacks resolve the registry id on every call. Returning the
    // raw Handle here would fail that check and could outlive its generation.
    out.* = .{ .ptr = owner.?, .vtable = &.{ .open = StorageRelationalRead.open, .try_fence = StorageRelationalRead.tryFence } };
    return .ok;
}

pub const StorageRelationalRead = struct {
    const View = antfly.capi_dependencies.relational_read_provider.View;
    const Fence = antfly.capi_dependencies.statement_read_fence.Fence;
    const Snapshot = antfly.capi_dependencies.statement_read_fence.Snapshot;

    const Pinned = struct {
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        read: db_mod.DB.RelationalStatementSnapshot,

        pub fn open(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions) !View {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!opts.include_range_proofs) return error.SqlRangeTrackingRequired;
            const query = opts.relational_query orelse return error.SqlStatementSnapshotRequired;
            if (query.index != null or query.auto_index) return error.SqlStatementSnapshotRequired;
            const session = try self.db.openRelationalReadSessionAtSnapshot(alloc, from, to, opts, &self.read);
            return .{ .ptr = session, .vtable = &.{ .next = StorageRelationalRead.next, .close = StorageRelationalRead.close, .normalize = StorageRelationalRead.normalize, .range_proofs = StorageRelationalRead.rangeProofs } };
        }

        pub fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.read.deinit();
            self.alloc.destroy(self);
        }
    };

    const Capture = struct {
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        fence: db_mod.DB.StatementReadFence,
        cancellation: @FieldType(db_mod.types.ScanOptions, "cancellation"),
        deadline_ns: ?u64,

        pub fn validate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.cancellation) |token| try token.check();
            if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        }
        pub fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fence.release();
            self.alloc.destroy(self);
        }
        pub fn open(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions) !View {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validate(ptr);
            const view = try StorageRelationalRead.openAny(self.db, alloc, from, to, opts);
            errdefer view.deinit();
            try validate(ptr);
            return view;
        }

        pub fn captureSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator) !Snapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validate(ptr);
            const pinned = try alloc.create(Pinned);
            errdefer alloc.destroy(pinned);
            var read = try self.db.captureRelationalStatementSnapshot();
            errdefer read.deinit();
            pinned.* = .{ .alloc = alloc, .db = self.db, .read = read };
            try validate(ptr);
            return .{ .ptr = pinned, .vtable = &.{ .open = Pinned.open, .release = Pinned.release } };
        }
    };

    pub fn tryFence(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, opts: db_mod.types.ScanOptions) !?Fence {
        const handle = asHandle(ptr) orelse return error.InvalidArgument;
        _ = storageOwnerTableName(handle, .fromSlice(table)) orelse return error.InvalidArgument;
        if (opts.cancellation) |token| try token.check();
        var fence = (try handle.db.tryStatementReadFence()) orelse return null;
        errdefer fence.release();
        const capture = try alloc.create(Capture);
        errdefer alloc.destroy(capture);
        capture.* = .{ .alloc = alloc, .db = &handle.db, .fence = fence, .cancellation = opts.cancellation, .deadline_ns = opts.execution_deadline_ns };
        try Capture.validate(capture);
        return .{ .ptr = capture, .vtable = &.{ .validate = Capture.validate, .open = Capture.open, .capture_snapshot = Capture.captureSnapshot, .release = Capture.release } };
    }

    pub fn open(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions) !View {
        const handle = asHandle(ptr) orelse return error.InvalidArgument;
        _ = storageOwnerTableName(handle, .fromSlice(table)) orelse return error.InvalidArgument;
        try handle.prepareScanRequest(from, to, opts);
        return openAny(&handle.db, alloc, from, to, opts);
    }

    pub fn openAny(db: *db_mod.DB, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions) !View {
        var schema = db.core.acquireSchemaView();
        defer if (schema) |*epoch| epoch.release();
        if (schema == null or schema.?.storageMode() == .document) {
            const session = try db.openDocumentReadSession(alloc, from, to, opts);
            return .{ .ptr = session, .vtable = &.{ .next = nextDocument, .close = closeDocument, .normalize = normalizeDocument, .range_proofs = documentRangeProofs } };
        }
        const session = try db.openRelationalReadSession(alloc, from, to, opts);
        return .{ .ptr = session, .vtable = &.{ .next = next, .close = close, .normalize = normalize, .range_proofs = rangeProofs } };
    }

    pub fn documentRangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]antfly.capi_dependencies.storage_range_protection.Proof {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        return session.rangeProofs(alloc);
    }

    pub fn rangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]antfly.capi_dependencies.storage_range_protection.Proof {
        const session: *db_mod.DB.RelationalReadSession = @ptrCast(@alignCast(ptr));
        return session.rangeProofs(alloc);
    }

    pub fn nextDocument(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !View.Page {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        return session.next(alloc, limit);
    }

    pub fn closeDocument(ptr: *anyopaque) void {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        session.deinit();
    }

    pub fn normalizeDocument(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) ![]db_mod.types.BatchWrite {
        const session: *db_mod.DB.DocumentReadSession = @ptrCast(@alignCast(ptr));
        return session.normalizeRows(alloc, writes);
    }

    pub fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !View.Page {
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

    pub fn close(ptr: *anyopaque) void {
        const session: *db_mod.DB.RelationalReadSession = @ptrCast(@alignCast(ptr));
        session.deinit();
    }

    pub fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) ![]db_mod.types.BatchWrite {
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
        pub fn visit(raw: ?*anyopaque, entry: db_mod.types.ScanVisitEntry) anyerror!void {
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

pub fn executeStorageOwnerCompiledQueryOperation(
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

pub fn executeStorageOwnerCompiledGraph(
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

pub const StorageOwnerArtifactCancellation = struct {
    request: *const kernel_owner_abi.ArtifactOperationRequest,

    pub fn requested(ptr: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.request.cancellation_fn orelse return false;
        return callback(self.request.cancellation_ctx) != 0;
    }
};

pub fn storageOwnerArtifactJsonResponse(
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
        0...@backingInt(kernel_owner_abi.ArtifactOperation.apply_child_range_batch) => @fromBackingInt(request.operation),
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
        .created_at_millis = (server_group_metadata.getGroupCreatedAtMillis(
            &handle.db,
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

pub const StorageOwnerObservationCancellation = struct {
    request: *const kernel_owner_abi.ControlledJsonOperationRequest,

    pub fn requested(ptr: *const anyopaque) bool {
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
            if (!antfly.hot_standby_validation.isIdentifier(token) or !std.fs.path.isAbsolute(destination)) return .invalid_argument;
            antfly.hot_standby_seed_snapshot.capture(handle.alloc, &handle.db, handle.db.core.path, token, destination) catch |err| {
                std.log.warn("storage owner HA seed capture failed err={s}", .{@errorName(err)});
                return storageOwnerStatusFromError(err);
            };
        },
        .prepare_ha_seed_snapshot => {
            if (request.deadline_ns == 0) return .invalid_argument;
            handle.db.drainSnapshotMaintenance(request.deadline_ns) catch |err| {
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

pub fn storageOwnerStatusFromError(err: anyerror) kernel_owner_abi.Status {
    const status = kernel_error_identity.statusFromError(err);
    if (status == .internal) {
        // This status-only boundary cannot carry undeclared error names.
        // Preserve the originating diagnostic before consumers see the
        // intentionally generic StorageKernelFailure control-flow status.
        std.log.warn("storage owner returned undeclared error err={s}", .{@errorName(err)});
    }
    return status;
}

pub fn searchStorageKernelQueryJson(
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

pub fn ownerQueryCancellation(request: *const kernel_owner_abi.ControlledJsonOperationRequest) db_mod.types.CancellationToken {
    if (request.cancellation_fn == null) return .none;
    return .{ .ptr = request, .is_cancelled_fn = struct {
        pub fn requested(ptr: *const anyopaque) bool {
            const control: *const kernel_owner_abi.ControlledJsonOperationRequest = @ptrCast(@alignCast(ptr));
            return control.cancellation_fn.?(control.cancellation_ctx) != 0;
        }
    }.requested };
}

pub fn storageOwnerQueryFailure(
    err: anyerror,
    operation: kernel_owner_abi.LocalQueryOperation,
    out_failure: *kernel_owner_abi.FailureIdentity,
) kernel_owner_abi.Status {
    out_failure.* = kernel_error_identity.failureFromError(
        err,
        .storage_owner,
        kernel_owner_abi.abi_version,
        @backingInt(operation),
    );
    return out_failure.status;
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
        plan_id: [16]u8,
        batch_json: ?[]u8 = null,
        pub fn propose(ptr: *anyopaque, batch: db_mod.types.BatchRequest, context: antfly.capi_dependencies.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            if (self.batch_json != null) return error.InvalidRestoreStagingCommand;
            if (batch.restore_staging_scope) |scope| if (!std.mem.eql(u8, &scope, &self.scope)) return error.RestoreStagingScopeChanged;
            var scoped = batch;
            scoped.restore_staging_scope = self.scope;
            scoped.restore_staging_plan_id = self.plan_id;
            self.batch_json = try antfly.capi_dependencies.api_batch.encodeBatchRequest(self.alloc, scoped);
        }
    };
    var capture: Capture = .{ .alloc = handle.alloc, .scope = input.value.scope.digest(), .plan_id = input.value.scope.plan_id };
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

pub fn hiddenRestoreJson(alloc: std.mem.Allocator, db: *db_mod.DB, request: *const kernel_owner_abi.HiddenRestoreRequest, out_result: *kernel_owner_abi.OwnedBytes) !void {
    if (request.operation == .cancel_initial_child_retirement) {
        const Record = @typeInfo(@typeInfo(@TypeOf(db.readInitialChildPublicationRecord())).error_union.payload).optional.child;
        var parsed = try std.json.parseFromSlice(struct { expected: Record, cancel_revision: u64 }, alloc, request.snapshot_token.slice(), .{ .ignore_unknown_fields = false });
        defer parsed.deinit();
        if (parsed.value.expected.namespace.table_id != request.table_id) return error.InitialChildPublicationChanged;
        return db.cancelColdInitialChildForRetirement(parsed.value.expected, parsed.value.cancel_revision);
    }
    if (request.operation == .read_initial_child) {
        const record = (try db.readInitialChildPublicationRecord()) orelse return;
        // A cold hidden owner is opened read-only without a public table
        // descriptor. Its core identity is not the authority for this path;
        // the durable AICH namespace is, and the caller checks the complete
        // bootstrap and terminal receipt before any retirement.
        // Zero is reserved for trusted local retirement classification: it
        // discovers the cold AICH identity before metadata's exact ticket is
        // available. Public/provisioning callers pass a nonzero table ID and
        // retain the scoped comparison.
        if (request.table_id != 0 and record.namespace.table_id != request.table_id) return error.InitialChildPublicationChanged;
        const encoded = try std.json.Stringify.valueAlloc(alloc, record, .{});
        out_result.* = .{ .ptr = encoded.ptr, .len = @intCast(encoded.len) };
        return;
    }
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
        .read_initial_child => unreachable,
        .cancel_initial_child_retirement => unreachable,
    }
}

pub fn captureOwnerSeedSnapshot(alloc: std.mem.Allocator, db: *db_mod.DB, request: *const kernel_owner_abi.HiddenRestoreRequest) !void {
    switch (db.primary_backend) {
        .lsm => {},
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
    _ = try db.snapshotWithMaintenanceDeadline(token, clock.nowRealtimeNs() +| std.time.ns_per_s);
    try backups_api.copyDirectoryRecursive(alloc, path, request.destination_root.slice());
}

pub fn storageOwnerHiddenRestoreJson(owner_ptr: ?*anyopaque, request: *const kernel_owner_abi.HiddenRestoreRequest, out_result: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    if (asHandle(owner_ptr)) |handle| {
        hiddenRestoreJson(handle.alloc, &handle.db, request, out_result) catch |err| return storageOwnerStatusFromError(err);
        return .ok;
    }
    if ((request.operation != .read_bootstrap and request.operation != .read_initial_child and request.operation != .cancel_initial_child_retirement) or request.path.len == 0) return .invalid_argument;
    const context = asStorageOwnerContext(request.context) orelse return .invalid_argument;
    const alloc = context.alloc;
    const runtime = context.backend_runtime.ptr();
    const io = runtime.filesystemIo() orelse return storageOwnerStatusFromError(error.BackendRuntimeIoUnavailable);
    _ = std.Io.Dir.cwd().statFile(io, request.path.slice(), .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return .ok,
        else => return storageOwnerStatusFromError(err),
    };
    var db = db_mod.DB.open(alloc, request.path.slice(), .{ .backend_runtime = runtime, .open_mode = if (request.operation == .cancel_initial_child_retirement) .writer else .query_readonly, .primary_only_readonly = request.operation != .cancel_initial_child_retirement, .start_index_workers = false, .start_optional_runtimes = false }) catch |err| return storageOwnerStatusFromError(err);
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

pub fn storageOwnerMergeCleanupKeysPage(owner_ptr: ?*anyopaque, request: *const kernel_owner_abi.MergeArtifactsPageRequest, out_result: *kernel_owner_abi.OwnedBytes) callconv(.c) kernel_owner_abi.Status {
    out_result.* = .{};
    if (request.version != kernel_owner_abi.abi_version) return .invalid_abi;
    const handle = asHandle(owner_ptr) orelse return .invalid_argument;
    _ = storageOwnerTableName(handle, request.table_name) orelse return .invalid_argument;
    const rows = handle.db.mergeCleanupKeysPage(handle.alloc, .{ .start = request.range_start.slice(), .end = request.range_end.slice() }, if (request.after_key.len == 0) null else request.after_key.slice()) catch |err|
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

pub fn cleanupServerHandle(handle: *Handle) void {
    if (handle.storage_owner_transaction_recovery) |context_ptr| {
        const recovery: *StorageOwnerTransactionRecovery = @ptrCast(@alignCast(context_ptr));
        recovery.deinit();
        handle.alloc.destroy(recovery);
    }
    if (handle.storage_owner_runtime_hooks) |context_ptr| {
        const hooks: *StorageOwnerRuntimeHooks = @ptrCast(@alignCast(context_ptr));
        handle.alloc.destroy(hooks);
    }
}
pub fn releaseServerContext(context_ptr: *anyopaque) void {
    const context: *StorageOwnerContext = @ptrCast(@alignCast(context_ptr));
    context.release();
}
