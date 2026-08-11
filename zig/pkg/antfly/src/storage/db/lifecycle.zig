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
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const apply_rw_lock_mod = @import("apply_rw_lock.zig");
const backfill_state_mod = @import("backfill_state.zig");
const backend_erased_mod = @import("../backend_erased.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const apply_state = @import("derived/apply_state.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const common_secrets = @import("../../common/secrets.zig");
const db_config = @import("config.zig");
const db_core = @import("core.zig");
const db_internal = @import("internal.zig");
const generation_lifecycle = @import("generation_lifecycle.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const derived_async_mod = @import("derived_async.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_types = @import("derived/derived_types.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const enrichment_worker = @import("enrichment/enrichment_worker.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const graph_metric_runtime_mod = @import("maintenance/graph_metric_runtime.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_query_mod = @import("../../graph/query.zig");
const ha_types = @import("ha_types.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const internal_keys = @import("../internal_keys.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const artifact_repair_mod = @import("artifact_repair.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const platform_time = @import("antfly_platform").time;
const promotion_runtime_mod = @import("promotion_runtime.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const resolution_runtime_mod = @import("resolution_runtime.zig");
const resolver_lib = @import("antfly_resolver");
const root_identity = @import("root_identity.zig");
const schema_mod = @import("../schema.zig");
const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("scraping_stub.zig")
else
    @import("antfly_scraping");
const sparse_compaction_runtime_mod = @import("maintenance/sparse_compaction_runtime.zig");
const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const transaction_runtime_mod = @import("maintenance/transaction_runtime.zig");
const transactions_mod = @import("../transactions.zig");
const ttl_runtime_mod = @import("maintenance/ttl_runtime.zig");
const types = @import("types.zig");
const vectorindex_mod = @import("antfly_vectorindex");

const Allocator = std.mem.Allocator;
const ManagedSyncTargets = db_internal.ManagedSyncTargets;

const run_until_idle_max_replay_rounds: usize = 16;
const test_quarantine_publication_fence_entered = &artifact_repair_mod.test_quarantine_publication_fence_entered;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

fn loadDerivedCoverageOutcomeCounterFromStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    generation: u64,
    outcome: []const u8,
) !?u64 {
    const counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, outcome);
    defer alloc.free(counter_key);
    const raw = store.get(alloc, counter_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return try internal_keys.decodeDerivedCoverageOutcomeCount(raw);
}

fn deinitOwnedEnrichmentConfig(alloc: Allocator, cfg: *enrichment_runtime_mod.Config) void {
    if (cfg.dense_embedder) |dense_embedder| {
        dense_embedder.deinit(alloc);
        cfg.dense_embedder = null;
    }
    if (cfg.sparse_embedder) |sparse_embedder| {
        sparse_embedder.deinit(alloc);
        cfg.sparse_embedder = null;
    }
    if (cfg.asset_producer) |producer| {
        producer.deinit(alloc);
        cfg.asset_producer = null;
    }
}

fn loadOrCreateDurableRootIdentity(
    alloc: Allocator,
    backend_runtime: ?*background_runtime_mod.BackendRuntime,
    path: []const u8,
) !root_identity.State {
    if (backend_runtime) |runtime| {
        if (runtime.io()) |io| return try root_identity.loadOrCreate(alloc, io, path);
    }
    if (comptime builtin.os.tag == .freestanding) return error.Unsupported;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try root_identity.loadOrCreate(alloc, io_impl.io(), path);
}

pub const DerivedReplayDebtStatus = struct {
    index_name: []const u8,
    kind: types.IndexKind,
    applied_sequence: u64,
    target_sequence: u64,
    catch_up_required: bool,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(@constCast(self.index_name));
        self.* = undefined;
    }
};

pub const ResolverUpsertOptions = struct {
    /// When false, persist the catalog mutation and mark the resolver backlog
    /// dirty, but leave the actual re-resolution drain to the caller. Managed
    /// table opens use this so DB runtime hooks are installed before
    /// cross-shard candidate blocking or promotion can run.
    drain_backfill: bool = true,
};

fn appendUniqueOwnedName(
    alloc: Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try items.append(alloc, try alloc.dupe(u8, value));
}

pub fn managedIndexReplayHint(kind: types.IndexKind) change_journal_mod.TargetHint {
    return switch (kind) {
        .full_text => .full_text,
        .dense_vector => .dense_vector,
        .sparse_vector => .sparse_vector,
        .graph => .graph,
        .algebraic => .algebraic,
    };
}

pub fn probeDerivedReplayTargetSequence(
    _: anytype,
    alloc: Allocator,
    replay_source: anytype,
    index_ref: index_manager_mod.ManagedIndexRef,
    applied_sequence: u64,
) !u64 {
    return try replay_source.latestMatchingSequence(
        alloc,
        applied_sequence,
        managedIndexReplayHint(index_ref.kind),
    );
}

pub const IndexStatusSnapshot = db_internal.IndexStatusSnapshot;

pub const DocIdentityCoverage = struct {
    scanned_primary_docs: u64 = 0,
    primary_docs_missing_ordinals: u64 = 0,
    primary_docs_missing_identity_state: u64 = 0,
    primary_docs_with_tombstone_ordinals: u64 = 0,
};

pub const OpenOptions = struct {
    pub const PhysicalRootMode = enum {
        filesystem_managed,
        external_backend,
    };

    pub const OpenMode = enum {
        writer,
        writer_no_replay,
        query_readonly,
        status_only,

        fn allowsReplay(self: @This()) bool {
            return self == .writer;
        }

        fn allowsIndexWorkers(self: @This()) bool {
            return self == .writer or self == .writer_no_replay;
        }

        fn allowsOptionalRuntimes(self: @This()) bool {
            return self == .writer or self == .writer_no_replay;
        }
    };

    pub const GraphMetricIdleMaintenanceMode = enum {
        legacy,
        planned,
        auto,
        degree_canary,
    };

    open_mode: OpenOptions.OpenMode = .writer,
    map_size: usize = 256 * 1024 * 1024,
    no_sync: bool = false,
    primary_backend: db_config.PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default },
    primary_runtime_store: ?*backend_erased_mod.Store = null,
    storage: ?lsm_backend_mod.Storage = null,
    lsm_cache: ?*lsm_backend_mod.Cache = null,
    hbc_cache: ?*hbc_mod.Cache = null,
    lsm_root_generation: u64 = 0,
    staged_generation: ?*const generation_lifecycle.StagedGeneration = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    capacity_source: ?types.RepairCapacitySource = null,
    change_journal_backend: ?change_journal_mod.StorageBackend = null,
    change_journal_storage: ?lsm_backend_mod.Storage = null,
    index_backends: db_config.IndexBackendOptions = .{},
    index_base_path: ?[]const u8 = null,
    index_open_parallelism: ?usize = null,
    schema_before_index_load: ?schema_mod.TableSchema = null,
    identity_namespace: ?doc_identity.Namespace = null,
    prefer_existing_identity_namespace: bool = false,
    executor: derived_executor_mod.Config = .{},
    backend_runtime: ?*background_runtime_mod.BackendRuntime = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    start_index_workers: bool = true,
    start_optional_runtimes: bool = true,
    start_optional_runtime_workers: bool = true,
    external_derived_checkpoints: bool = true,
    index_repair_checkpoint_storage: ?lsm_backend_mod.Storage = null,
    physical_root_mode: PhysicalRootMode = .filesystem_managed,
    enrichment: ?enrichment_runtime_mod.Config = null,
    ttl_cleanup: ttl_runtime_mod.Config = .{},
    transaction_recovery: transaction_runtime_mod.Config = .{},
    text_merge: text_merge_runtime_mod.Config = .{},
    sparse_compaction: sparse_compaction_runtime_mod.Config = .{},
    graph_metric_maintenance: graph_metric_runtime_mod.Config = .{},
    graph_metric_idle_maintenance: GraphMetricIdleMaintenanceMode = .auto,
    graph_metric_idle_planned_options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions = .{},
    graph_metric_idle_auto_options: index_manager_mod.IndexManager.GraphMetricPlannedAutoIdleOptions = .{},
    graph_metric_idle_degree_canary_options: index_manager_mod.IndexManager.GraphMetricDegreeCanaryOptions = .{},
    /// Optional cross-shard candidate source for entity resolution blocking,
    /// injected by the serving layer (see `api/distributed_candidate_source.zig`).
    /// Null means local-only blocking against the worker's own store. Must
    /// outlive the DB.
    resolution_candidate_source: ?resolution_runtime_mod.CandidateSource = null,
    /// Optional cross-shard entity sink for the promoter, injected by the serving
    /// layer (see `api/distributed_entity_sink.zig`). Must outlive the DB.
    entity_sink: ?promotion_runtime_mod.EntitySink = null,
    /// Optional ownership guard for promotion. Raft apply-side DBs set this so
    /// only the source shard leader emits entity writes; standalone DBs leave it
    /// null and are treated as local owners.
    promotion_owner: ?promotion_runtime_mod.PromotionOwner = null,
    /// What the promoter does when no sink is currently available. The safe
    /// default holds replay so a later sink injection or routing repair can retry.
    entity_sink_missing_policy: promotion_runtime_mod.MissingSinkPolicy = .wait,
    /// Optional name embedder for resolution: backfills a mention's name
    /// embedding (for cosine/ann blocking) when a resolver declares a
    /// `name_embedding` model and the extraction artifact carries no vector.
    /// Caller-owned; must outlive the DB. Null disables backfill.
    resolution_embedder: ?embedder_mod.DenseEmbedder = null,
    /// Optional mirror for committed derived/change-journal effects into the HA
    /// replication stream. The default policy is async/best-effort; configuring
    /// a non-async sync_policy makes normal DB writes evaluate the HA commit
    /// gate for the appended replication record.
    ha_async_effect_mirror: ?ha_types.AsyncEffectMirror = null,
    /// Optional mirror for committed user batch mutations into the HA
    /// replication stream. This emits versioned `batch_mutation` envelopes for
    /// catch-up/read-replica apply and can be paired with sync_policy for
    /// remote-write/remote-apply gate decisions.
    ha_async_batch_mirror: ?ha_types.AsyncBatchMirror = null,
    /// Optional mirror for committed metadata/catalog changes into the HA
    /// replication stream. The initial metadata mutation payload covers table
    /// schema changes; additional catalog mutation kinds should be nested under
    /// the stable HA `metadata_mutation` envelope.
    ha_async_metadata_mirror: ?ha_types.AsyncMetadataMirror = null,
    /// Optional HA write ownership gate. Client/API writes are allowed only
    /// when this DB is attached to the current HA primary. Standby apply paths
    /// must use replicated-apply entry points that explicitly bypass this
    /// client-write guard. A standby gate also suppresses mutating background
    /// runtimes at open, even if the generic runtime defaults are enabled.
    ha_write_gate: ?ha_types.WriteGate = null,
};

fn groupCreatedAtMetadataKeyAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_created_at:{d}", .{group_id});
}

pub const OpenProfile = struct {
    primary_store_ns: u64 = 0,
    core_resources_ns: u64 = 0,
    init_async_infrastructure_ns: u64 = 0,
    init_optional_runtimes_ns: u64 = 0,
    load_indexes_ns: u64 = 0,
    replay_pending_derived_ns: u64 = 0,
    start_index_workers_ns: u64 = 0,
    start_optional_runtimes_ns: u64 = 0,
    total_ns: u64 = 0,
};

fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return monotonicTimeNs() - start_ns;
}

var open_profile_enabled_cache: std.atomic.Value(u8) = .init(0);

pub fn backgroundRuntimeAllocator(fallback: std.mem.Allocator) std.mem.Allocator {
    if (comptime builtin.os.tag == .freestanding) return fallback;
    if (comptime builtin.link_libc) return std.heap.c_allocator;
    if (comptime builtin.single_threaded) return fallback;
    return std.heap.smp_allocator;
}

pub fn openProfileEnabled() bool {
    const cached = open_profile_enabled_cache.load(.monotonic);
    if (cached == 1 or cached == 2) return cached == 2;
    if (cached == 3) return db_internal.waitForCachedBool(&open_profile_enabled_cache);
    if (open_profile_enabled_cache.cmpxchgStrong(0, 3, .acq_rel, .monotonic) != null) {
        return db_internal.waitForCachedBool(&open_profile_enabled_cache);
    }
    if (comptime builtin.os.tag == .freestanding) {
        open_profile_enabled_cache.store(1, .release);
        return false;
    }
    const raw_z = platform.env.getenv("ANTFLY_DB_OPEN_PROFILE") orelse
        platform.env.getenv("ANTFLY_BENCH_METRICS") orelse {
        open_profile_enabled_cache.store(1, .release);
        return false;
    };
    const enabled = db_internal.envBoolEnabled(raw_z);
    open_profile_enabled_cache.store(if (enabled) 2 else 1, .release);
    return enabled;
}

pub fn logOpenProfile(path: []const u8, open_mode: anytype, start_index_workers: bool, profile: OpenProfile) void {
    std.log.info(
        "db_open_profile path={s} mode={s} start_index_workers={} primary_store_ns={} core_resources_ns={} init_async_infrastructure_ns={} init_optional_runtimes_ns={} load_indexes_ns={} replay_pending_derived_ns={} start_index_workers_ns={} start_optional_runtimes_ns={} total_ns={}",
        .{
            path,
            @tagName(open_mode),
            start_index_workers,
            profile.primary_store_ns,
            profile.core_resources_ns,
            profile.init_async_infrastructure_ns,
            profile.init_optional_runtimes_ns,
            profile.load_indexes_ns,
            profile.replay_pending_derived_ns,
            profile.start_index_workers_ns,
            profile.start_optional_runtimes_ns,
            profile.total_ns,
        },
    );
}

pub const openModeRequiresReadOnlyBackends = db_config.openModeRequiresReadOnlyBackends;
pub const openModeAllowsReplay = db_config.openModeAllowsReplay;
pub const openModeAllowsIndexWorkers = db_config.openModeAllowsIndexWorkers;
pub const openModeAllowsOptionalRuntimes = db_config.openModeAllowsOptionalRuntimes;

pub fn makeLsmOptionsReadOnly(options: *lsm_backend_mod.Options) void {
    options.backend.read_only = true;
    options.backend.create_if_missing = false;
    options.background_executor = null;
}

pub fn applyReadOnlyToPrimaryBackend(primary_backend: *db_config.PrimaryBackend) void {
    switch (primary_backend.*) {
        .lsm => |*lsm_opts| makeLsmOptionsReadOnly(lsm_opts),
        .lsm_memory => |*lsm_opts| makeLsmOptionsReadOnly(lsm_opts),
        .lmdb, .mem => {},
    }
}

pub fn applyReadOnlyToIndexBackends(index_backends: *db_config.IndexBackendOptions) void {
    makeLsmOptionsReadOnly(&index_backends.text_main_lsm_options);
    makeLsmOptionsReadOnly(&index_backends.text_wal_lsm_options);
    makeLsmOptionsReadOnly(&index_backends.dense_lsm_options);
    makeLsmOptionsReadOnly(&index_backends.sparse_lsm_options);
    makeLsmOptionsReadOnly(&index_backends.graph_reverse_lsm_options);
}

pub fn makeLsmBackgroundExecutor(runtime: *background_runtime_mod.BackendRuntime, owner_id: u64) lsm_backend_mod.BackgroundExecutor {
    return lsm_backend_mod.BackgroundExecutor.init(runtime, owner_id);
}

pub fn installLsmReadRuntime(options: *lsm_backend_mod.Options, runtime: *background_runtime_mod.BackendRuntime) void {
    if (options.native_storage_pool == null) options.native_storage_pool = runtime.nativeStoragePool();
    if (options.read_runtime != null) return;
    if (runtime.io()) |io| options.read_runtime = lsm_backend_mod.storage_io.ReadRuntime.init(io);
}

pub fn installIndexLsmReadRuntime(index_backends: anytype, runtime: *background_runtime_mod.BackendRuntime) void {
    installLsmReadRuntime(&index_backends.text_main_lsm_options, runtime);
    installLsmReadRuntime(&index_backends.text_wal_lsm_options, runtime);
    installLsmReadRuntime(&index_backends.dense_lsm_options, runtime);
    installLsmReadRuntime(&index_backends.sparse_lsm_options, runtime);
    installLsmReadRuntime(&index_backends.graph_reverse_lsm_options, runtime);
}

pub const indexStatusKeyAlloc = db_internal.indexStatusKeyAlloc;
pub const encodeIndexStatusSnapshot = db_internal.encodeIndexStatusSnapshot;
pub const decodeIndexStatusSnapshot = db_internal.decodeIndexStatusSnapshot;
pub const collectLiveIndexStatusSnapshot = db_internal.collectLiveIndexStatusSnapshot;
pub const textIndexTermCount = db_internal.textIndexTermCount;
pub const saveIndexStatusSnapshots = db_internal.saveIndexStatusSnapshots;
pub const loadIndexStatusSnapshot = db_internal.loadIndexStatusSnapshot;
pub const applyIndexStatusSnapshot = db_internal.applyIndexStatusSnapshot;

fn projectionCheckpointStatusName(status: apply_state.ProjectionStatus) []const u8 {
    return switch (status) {
        .clean => "clean",
        .rebuilding => "rebuilding",
        .degraded => "degraded",
        .repair_required => "repair_required",
    };
}

fn applyProjectionCheckpointStats(item: *types.DBIndexStats, checkpoint: apply_state.ProjectionCheckpoint, target_sequence: u64) void {
    item.projection_checkpoint_status = projectionCheckpointStatusName(checkpoint.status);
    item.projection_checkpoint_applied_sequence = checkpoint.applied_sequence;
    item.projection_checkpoint_generation = checkpoint.generation;
    item.projection_checkpoint_config_hash = checkpoint.config_hash;
    item.checkpoint_replay_tail_sequence_count = target_sequence -| checkpoint.applied_sequence;
    switch (checkpoint.status) {
        .clean => {},
        .rebuilding => item.backfill_active = true,
        .degraded, .repair_required => item.repair_degraded = true,
    }
}

fn applyDurableIndexRepairStats(
    alloc: Allocator,
    state: ?*const index_repair_state.State,
    repair_state_corrupt: bool,
    item: *types.DBIndexStats,
) !void {
    if (repair_state_corrupt) {
        item.index_repair_trigger = "corrupt_local_repair_state";
        item.index_repair_phase = "terminal";
        item.index_repair_wait_reason = "terminal";
        item.repair_degraded = true;
        return;
    }
    const durable = state orelse return;
    const i = durable.findIndex(item.name) orelse return;
    const intent = durable.entries.items[i].intent;
    item.index_repair_id = intent.repair_id;
    item.index_repair_trigger = @tagName(intent.trigger);
    item.index_repair_phase = @tagName(intent.phase);
    item.index_repair_automation = @tagName(intent.automation);
    item.index_repair_attempts = intent.attempt_count;
    item.index_repair_started_at_ms = intent.started_at_ms;
    item.index_repair_updated_at_ms = intent.updated_at_ms;
    item.index_repair_build_floor_sequence = intent.build_floor_sequence;
    item.index_repair_applied_sequence = intent.candidate_applied_sequence;
    item.index_repair_target_sequence = intent.target_sequence;
    item.index_repair_next_retry_at_ms = intent.next_retry_at_ms;
    item.index_repair_last_error = if (intent.last_error) |value| try alloc.dupe(u8, value) else null;
    item.index_repair_wait_reason = if (intent.automation == .paused)
        "paused"
    else if (intent.phase == .terminal)
        "terminal"
    else if (intent.next_retry_at_ms > db_internal.currentTimeNs() / std.time.ns_per_ms)
        "backoff"
    else if (intent.phase == .waiting_for_convergence)
        "convergence"
    else
        "none";
    switch (intent.trigger) {
        .incomplete_bulk_publish,
        .root_generation_rebuild,
        .projection_generation_invalid,
        .operator_generation_validation,
        => item.repair_degraded = true,
        .artifact_coverage_mismatch, .artifact_counter_missing => {
            item.backfill_active = intent.phase != .terminal;
            item.repair_degraded = true;
        },
        .operator_generation_rebuild => {
            item.backfill_active = intent.phase != .terminal;
            item.repair_degraded = intent.phase == .activating or
                intent.phase == .validating or
                intent.phase == .terminal;
        },
    }
}

fn applyCachedIdentityVisibilitySummary(identity_stats: *doc_identity.Stats, summary: ?doc_identity.VisibilitySummary) void {
    const cached = summary orelse return;
    identity_stats.live_ordinals = cached.live_ordinals;
    identity_stats.tombstone_ordinals = cached.tombstone_ordinals;
    identity_stats.max_created_generation = cached.max_created_generation;
    identity_stats.min_deleted_generation = cached.min_deleted_generation;
    identity_stats.max_deleted_generation = cached.max_deleted_generation;
}

test "db lifecycle operational stats prefer the maintained live identity summary" {
    var stats = doc_identity.Stats{
        .live_ordinals = 0,
        .tombstone_ordinals = 1,
        .max_created_generation = 2,
        .min_deleted_generation = 2,
        .max_deleted_generation = 2,
    };
    applyCachedIdentityVisibilitySummary(&stats, .{
        .live_ordinals = 1,
        .tombstone_ordinals = 0,
        .max_created_generation = 3,
    });
    try std.testing.expectEqual(@as(u64, 1), stats.live_ordinals);
    try std.testing.expectEqual(@as(u64, 0), stats.tombstone_ordinals);
    try std.testing.expectEqual(@as(u64, 3), stats.max_created_generation);
    try std.testing.expectEqual(@as(u64, 0), stats.min_deleted_generation);
    try std.testing.expectEqual(@as(u64, 0), stats.max_deleted_generation);

    const DB = @import("mod.zig").DB;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    var db = try DB.open(std.testing.allocator, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();
    db.identity_visibility_summary_cache = .{
        .live_ordinals = 1,
        .max_created_generation = 3,
    };
    const operational = try db.stats(std.testing.allocator);
    defer types.freeDBStats(std.testing.allocator, operational);
    try std.testing.expectEqual(@as(u64, 1), operational.source_doc_count);
    try std.testing.expectEqual(@as(u64, 1), operational.doc_identity.live_ordinals);
}

fn applyTerminalLoadFailureStatus(item: *types.DBIndexStats) void {
    item.replay_catch_up_required = false;
    item.catch_up_active = false;
    item.backfill_active = false;
    item.repair_degraded = true;
}

test "terminal index load failure clears activity and remains degraded" {
    var item = types.DBIndexStats{
        .name = "dense",
        .kind = .dense_vector,
        .backfill_active = true,
        .catch_up_active = true,
        .replay_catch_up_required = true,
    };
    applyTerminalLoadFailureStatus(&item);
    try std.testing.expect(!item.replay_catch_up_required);
    try std.testing.expect(!item.catch_up_active);
    try std.testing.expect(!item.backfill_active);
    try std.testing.expect(item.repair_degraded);
}

fn markDenseCoverageRegressionIfNeeded(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    item: *types.DBIndexStats,
) !void {
    const status_snapshot = (try loadIndexStatusSnapshot(alloc, store, index_name)) orelse return;
    if (status_snapshot.kind != .dense_vector) return;
    if (status_snapshot.doc_count <= item.doc_count) return;
    item.repair_degraded = true;
    item.repair_issue_count +|= 1;
}

fn finalizeDocIdentityRebuildRequired(identity_stats: *types.DocIdentityStats) void {
    identity_stats.rebuild_required = identity_stats.rebuild_required or
        identity_stats.ordinal_capacity_exhausted or
        identity_stats.primary_docs_missing_ordinals != 0 or
        identity_stats.primary_docs_missing_identity_state != 0 or
        identity_stats.primary_docs_with_tombstone_ordinals != 0;
}

pub fn freeDBIndexStatsItem(alloc: Allocator, item: types.DBIndexStats) void {
    alloc.free(item.name);
    if (item.load_error) |value| alloc.free(value);
    if (item.index_repair_last_error) |value| alloc.free(value);
    if (item.algebraic_last_error_doc_key) |value| alloc.free(value);
    if (item.algebraic_last_error_reason) |value| alloc.free(value);
    if (item.algebraic_capability_fingerprint) |value| alloc.free(value);
    if (item.algebraic_capability_lifecycle_status) |value| alloc.free(value);
    if (item.algebraic_planner_last_decision) |value| alloc.free(value);
    if (item.algebraic_planner_last_fallback_reason) |value| alloc.free(value);
    if (item.algebraic_planner_lifecycle_blocking_reason) |value| alloc.free(value);
    if (item.algebraic_last_observed_query_shape) |value| alloc.free(value);
    if (item.algebraic_last_recommended_materialization) |value| alloc.free(value);
    types.freeGraphMetricStatuses(alloc, @constCast(item.graph_metric_status));
    if (item.algebraic_top_candidate) |candidate| {
        alloc.free(candidate.recommendation);
        alloc.free(candidate.materialization_id);
        alloc.free(candidate.lifecycle);
        alloc.free(candidate.decision);
    }
    if (item.algebraic_active_progress) |progress| {
        alloc.free(progress.recommendation);
        alloc.free(progress.materialization_id);
        alloc.free(progress.lifecycle);
    }
    for (item.algebraic_candidates) |candidate| {
        alloc.free(candidate.recommendation);
        alloc.free(candidate.materialization_id);
        alloc.free(candidate.lifecycle);
        alloc.free(candidate.decision);
    }
    if (item.algebraic_candidates.len > 0) alloc.free(item.algebraic_candidates);
    for (item.algebraic_candidate_decision_history) |entry| {
        alloc.free(entry.recommendation);
        alloc.free(entry.materialization_id);
        alloc.free(entry.lifecycle);
        alloc.free(entry.previous_decision);
        alloc.free(entry.decision);
    }
    if (item.algebraic_candidate_decision_history.len > 0) alloc.free(item.algebraic_candidate_decision_history);
    for (item.algebraic_progress) |progress| {
        alloc.free(progress.recommendation);
        alloc.free(progress.materialization_id);
        alloc.free(progress.lifecycle);
    }
    if (item.algebraic_progress.len > 0) alloc.free(item.algebraic_progress);
}

pub fn Impl(comptime DB: type) type {
    return struct {
        fn resolveRecoveredLocalTransaction(
            ctx: *anyopaque,
            txn_id: transactions_mod.TxnId,
            status: transactions_mod.TxnStatus,
            commit_version: u64,
        ) anyerror!void {
            const db: *DB = @ptrCast(@alignCast(ctx));
            try db.resolveTransactionIntentsWithSyncLevel(txn_id, status, commit_version, .propose);
        }

        const Self = @This();
        const test_quarantine_publication_fence_entered = DB.ArtifactRepairCallbacks.test_quarantine_publication_fence_entered;

        const engine_vtable = db_core.Engine.VTable{
            .batch = engineBatch,
            .lookup = engineLookup,
            .scan = engineScan,
            .search = engineSearch,
            .stats = engineStats,
            .list_indexes = engineListIndexes,
            .list_enrichments = engineListEnrichments,
        };
        const maintenance_driver_vtable = db_core.MaintenanceDriver.VTable{
            .pending_work_stats = maintenanceDriverPendingWorkStats,
            .run_derived_until = maintenanceDriverRunDerivedUntil,
            .run_enrichment_until = maintenanceDriverRunEnrichmentUntil,
            .run_maintenance_until = maintenanceDriverRunMaintenanceUntil,
            .run_until_idle = maintenanceDriverRunUntilIdle,
        };

        pub fn sync(self: *DB, full: bool) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.syncStore(full);
        }

        pub fn syncIndexes(self: *DB, force: bool) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.index_manager.syncAll(force);
        }

        pub fn forceFlushPrimaryStoreForVisibility(self: *DB) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.primary_store_owner.forceFlushLsmMutable();
        }

        pub fn lsmMaintenanceScore(self: *DB) u64 {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return @max(
                self.core.primary_store_owner.lsmMaintenanceScore(),
                self.core.index_manager.lsmMaintenanceScore(),
            );
        }

        pub fn lsmMaintenanceDebtHint(self: *DB) u64 {
            return @max(
                self.core.primary_store_owner.lsmMaintenanceDebtHint(),
                self.core.index_manager.lsmMaintenanceDebtHint(),
            );
        }

        pub fn primaryLsmMaintenanceDebtHint(self: *DB) u64 {
            return self.core.primary_store_owner.lsmMaintenanceDebtHint();
        }

        pub fn nextLsmMaintenanceWakeDelayNsBestEffort(self: *DB) ?u64 {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            var best: ?u64 = null;
            if (self.core.primary_store_owner.nextLsmMaintenanceWakeDelayNsBestEffort()) |candidate| {
                best = candidate;
            }
            if (self.core.index_manager.nextLsmMaintenanceWakeDelayNsBestEffort()) |candidate| {
                best = if (best) |current| @min(current, candidate) else candidate;
            }
            return best;
        }

        pub fn nextPrimaryLsmMaintenanceWakeDelayNsBestEffort(self: *DB) ?u64 {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            return self.core.primary_store_owner.nextLsmMaintenanceWakeDelayNsBestEffort();
        }

        pub fn snapshotAsyncIndexingStats(self: *DB) types.AsyncIndexingStats {
            var async_stats = self.async_context.stats.snapshot();
            async_stats.bulk_coalescing = self.bulk_ingest_coalescer.stats.snapshot();
            async_stats.derived_workers = self.executor.snapshotStats();
            return async_stats;
        }

        pub fn snapshotApplyLockStats(self: *DB) apply_rw_lock_mod.ApplyRwLock.Stats {
            return self.core.apply_mutex.snapshot();
        }

        pub fn startAsyncWorkers(self: *DB) !void {
            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            for (managed_indexes) |index_ref| {
                const applied = try self.core.loadAppliedSequence(self.alloc, index_ref.name);
                try self.executor.addWorker(index_ref.name, index_ref, applied);
            }
        }

        pub fn open(alloc: std.mem.Allocator, path: []const u8, requested_opts: anytype) !DB {
            return blk: {
                var opts = requested_opts;
                errdefer if (opts.enrichment) |*cfg| deinitOwnedEnrichmentConfig(alloc, cfg);
                if (opts.backend_runtime) |runtime| {
                    if (runtime.db_open_configurator) |configurator| {
                        try configurator.configure(path, &opts);
                    }
                }
                var generation_read_lease = if (opts.staged_generation) |staged_generation| staged_blk: {
                    try staged_generation.validatePath(path);
                    break :staged_blk null;
                } else if (opts.physical_root_mode == .external_backend)
                    null
                else
                    try generation_lifecycle.acquirePublishedGenerationReadWithRuntime(alloc, path, opts.backend_runtime);
                errdefer if (generation_read_lease) |*lease| lease.deinit();
                const open_started_ns = monotonicTimeNs();
                const ha_write_gate = if (opts.ha_write_gate) |gate| gate.pinned() else null;
                var profile = OpenProfile{};
                const runtime_alloc = backgroundRuntimeAllocator(alloc);
                var owned_resource_manager: ?*resource_manager_mod.ResourceManager = null;
                const bind_cache_resource_manager = opts.resource_manager != null;
                if (opts.resource_manager == null) {
                    const manager = try alloc.create(resource_manager_mod.ResourceManager);
                    manager.* = resource_manager_mod.ResourceManager.init(.{});
                    owned_resource_manager = manager;
                    opts.resource_manager = manager;
                }
                errdefer if (owned_resource_manager) |manager| {
                    manager.deinit(alloc);
                    alloc.destroy(manager);
                };
                var owned_backend_runtime: ?background_runtime_mod.BackendRuntimeHandle = null;
                errdefer if (owned_backend_runtime) |*handle| handle.deinit();

                const backend_runtime = opts.backend_runtime orelse blk_runtime: {
                    owned_backend_runtime = try background_runtime_mod.BackendRuntimeHandle.init(runtime_alloc, .{
                        .backend = opts.executor.backend,
                    });
                    break :blk_runtime owned_backend_runtime.?.runtime;
                };
                var effective_executor = opts.executor;
                if (backend_runtime.io_impl == null and effective_executor.backend == .io_threaded) {
                    effective_executor.backend = .manual;
                }
                const backend_owner_id = try backend_runtime.allocOwnerId();
                var backend_owner_transferred = false;
                errdefer if (!backend_owner_transferred)
                    backend_runtime.durable_jobs.closeOwner(backend_owner_id);
                const repair_cleanup_owner_id = try backend_runtime.allocOwnerId();
                var repair_cleanup_owner_transferred = false;
                errdefer if (!repair_cleanup_owner_transferred)
                    backend_runtime.durable_jobs.closeOwner(repair_cleanup_owner_id);
                var primary_lsm_background_executor: lsm_backend_mod.BackgroundExecutor = undefined;
                var effective_primary_backend = opts.primary_backend;
                var effective_index_backends = opts.index_backends;
                if (openModeRequiresReadOnlyBackends(opts.open_mode)) {
                    applyReadOnlyToPrimaryBackend(&effective_primary_backend);
                    applyReadOnlyToIndexBackends(&effective_index_backends);
                }
                switch (effective_primary_backend) {
                    .lsm => |*lsm_opts| {
                        lsm_opts.cache = opts.lsm_cache orelse lsm_opts.cache;
                        lsm_opts.root_generation = opts.lsm_root_generation;
                        installLsmReadRuntime(lsm_opts, backend_runtime);
                        primary_lsm_background_executor = makeLsmBackgroundExecutor(backend_runtime, backend_owner_id);
                        lsm_opts.background_executor = &primary_lsm_background_executor;
                    },
                    .lsm_memory => |*lsm_opts| {
                        lsm_opts.cache = opts.lsm_cache orelse lsm_opts.cache;
                        lsm_opts.root_generation = opts.lsm_root_generation;
                        installLsmReadRuntime(lsm_opts, backend_runtime);
                        primary_lsm_background_executor = makeLsmBackgroundExecutor(backend_runtime, backend_owner_id);
                        lsm_opts.background_executor = &primary_lsm_background_executor;
                    },
                    .lmdb, .mem => {},
                }
                installIndexLsmReadRuntime(&effective_index_backends, backend_runtime);
                const relational_index_worker_owner_id = try backend_runtime.allocOwnerId();
                var relational_owner_transferred = false;
                errdefer if (!relational_owner_transferred)
                    backend_runtime.durable_jobs.closeOwner(relational_index_worker_owner_id);

                const core_opts: db_config.CoreOpenOptions = .{
                    .map_size = opts.map_size,
                    .no_sync = opts.no_sync,
                    .read_only = openModeRequiresReadOnlyBackends(opts.open_mode),
                    .primary_backend = effective_primary_backend,
                    .primary_runtime_store = opts.primary_runtime_store,
                    .storage = opts.storage,
                    .lsm_cache = opts.lsm_cache,
                    .hbc_cache = opts.hbc_cache,
                    .lsm_root_generation = opts.lsm_root_generation,
                    .resource_manager = opts.resource_manager,
                    .bind_cache_resource_manager = bind_cache_resource_manager,
                    .index_backends = effective_index_backends,
                };
                const resolved_config = db_config.ResolvedOpenConfig.init(
                    effective_primary_backend,
                    opts.storage,
                    opts.lsm_cache,
                    opts.hbc_cache,
                    opts.lsm_root_generation,
                    opts.resource_manager,
                    bind_cache_resource_manager,
                    effective_index_backends,
                );
                const open_primary_started_ns = monotonicTimeNs();
                const opened_primary = try db_internal.openPrimaryStore(alloc, path, core_opts);
                profile.primary_store_ns = elapsedSince(open_primary_started_ns);

                const core_resources_started_ns = monotonicTimeNs();
                const core = try db_core.openCoreResourcesFromPrimaryStore(
                    alloc,
                    path,
                    opts.index_base_path orelse path,
                    opts.map_size,
                    opts.no_sync,
                    resolved_config.primary_backend_kind,
                    resolved_config.primary_lsm_storage,
                    opts.change_journal_backend,
                    opts.change_journal_storage,
                    resolved_config.index_backends,
                    opened_primary,
                    opts.identity_namespace,
                    false,
                    if (opts.prefer_existing_identity_namespace) .use_existing else .reject,
                    opts.external_derived_checkpoints,
                    opts.index_repair_checkpoint_storage,
                    opts.lsm_root_generation,
                    openModeRequiresReadOnlyBackends(opts.open_mode),
                );
                profile.core_resources_ns = elapsedSince(core_resources_started_ns);

                const async_context = try runtime_alloc.create(db_internal.AsyncContext(DB));
                var async_context_owned = true;
                errdefer if (async_context_owned) runtime_alloc.destroy(async_context);
                const executor = try runtime_alloc.create(derived_executor_mod.Executor);
                var executor_owned = true;
                errdefer if (executor_owned) runtime_alloc.destroy(executor);

                var stored_primary_backend = effective_primary_backend;
                switch (stored_primary_backend) {
                    .lsm => |*lsm_opts| lsm_opts.background_executor = null,
                    .lsm_memory => |*lsm_opts| lsm_opts.background_executor = null,
                    .lmdb, .mem => {},
                }
                const ha_standby_role = ha_types.writeGateIsStandby(ha_write_gate);
                const start_index_workers = openModeAllowsIndexWorkers(opts.open_mode) and opts.start_index_workers and !ha_standby_role;

                var db = DB{
                    .alloc = alloc,
                    .runtime_alloc = runtime_alloc,
                    .generation_read_lease = generation_read_lease,
                    .open_mode = opts.open_mode,
                    .primary_backend = stored_primary_backend,
                    .primary_lsm_storage = resolved_config.primary_lsm_storage,
                    .index_backends = resolved_config.index_backends,
                    .core = db_core.DBCore.fromOpened(alloc, core),
                    .async_context = async_context,
                    .backend_runtime = backend_runtime,
                    .backend_owner_id = backend_owner_id,
                    .relational_index_worker_owner_id = relational_index_worker_owner_id,
                    .repair_cleanup_owner_id = repair_cleanup_owner_id,
                    .algebraic_hll_owner_id = 0,
                    .owned_backend_runtime = owned_backend_runtime,
                    .owned_resource_manager = owned_resource_manager,
                    .capacity_source = opts.capacity_source orelse opts.resource_manager.?.capacitySource(),
                    .executor = executor,
                    .start_index_workers = start_index_workers,
                    .optional_runtime_workers_enabled = false,
                    .graph_metric_idle_maintenance = opts.graph_metric_idle_maintenance,
                    .graph_metric_idle_planned_options = opts.graph_metric_idle_planned_options,
                    .graph_metric_idle_auto_options = opts.graph_metric_idle_auto_options,
                    .graph_metric_idle_degree_canary_options = opts.graph_metric_idle_degree_canary_options,
                    .secret_store = opts.secret_store,
                    .remote_content = opts.remote_content,
                    .enrichment_append_context = null,
                    .enrichment_runtime = null,
                    .resolution_candidate_source = opts.resolution_candidate_source,
                    .resolution_embedder = opts.resolution_embedder,
                    .entity_sink = opts.entity_sink,
                    .promotion_owner = opts.promotion_owner,
                    .entity_sink_missing_policy = opts.entity_sink_missing_policy,
                    .ha_async_effect_mirror = opts.ha_async_effect_mirror,
                    .ha_async_batch_mirror = opts.ha_async_batch_mirror,
                    .ha_async_metadata_mirror = opts.ha_async_metadata_mirror,
                    .ha_write_gate = ha_write_gate,
                    .ttl_cleanup_context = null,
                    .ttl_runtime = null,
                    .transaction_recovery_identity_context = null,
                    .transaction_runtime = null,
                    .text_merge_runtime = null,
                    .sparse_compaction_runtime = null,
                    .graph_metric_runtime = null,
                    .shadow = null,
                };
                backend_owner_transferred = true;
                repair_cleanup_owner_transferred = true;
                relational_owner_transferred = true;
                var executor_ready = false;
                owned_backend_runtime = null;
                owned_resource_manager = null;
                generation_read_lease = null;
                async_context_owned = false;
                executor_owned = false;
                errdefer Self.deinitWrapperState(&db, executor_ready);

                db.core.index_manager.setIo(db.backend_runtime.io());
                db.core.setIndexOpenParallelism(opts.index_open_parallelism);
                const init_async_started_ns = monotonicTimeNs();
                try Self.initAsyncInfrastructure(&db, effective_executor, opts.resource_manager);
                profile.init_async_infrastructure_ns = elapsedSince(init_async_started_ns);
                executor_ready = true;

                if (!openModeRequiresReadOnlyBackends(opts.open_mode) and
                    opts.physical_root_mode == .filesystem_managed)
                {
                    const identity = try loadOrCreateDurableRootIdentity(alloc, db.backend_runtime, path);
                    db.root_incarnation = identity.incarnation;
                }
                if (opts.schema_before_index_load) |table_schema| {
                    try db.core.setSchema(table_schema);
                }

                const optional_runtimes_initialized = openModeAllowsOptionalRuntimes(opts.open_mode) and opts.start_optional_runtimes and !ha_standby_role;
                const optional_runtime_workers_enabled = optional_runtimes_initialized and opts.start_optional_runtime_workers;
                db.optional_runtime_workers_enabled = optional_runtime_workers_enabled;
                if (optional_runtimes_initialized) {
                    const init_optional_started_ns = monotonicTimeNs();
                    try Self.initOptionalRuntimes(&db, &opts);
                    profile.init_optional_runtimes_ns = elapsedSince(init_optional_started_ns);
                } else if (opts.enrichment) |*cfg| {
                    deinitOwnedEnrichmentConfig(alloc, cfg);
                    opts.enrichment = null;
                }

                try Self.attachAlgebraicHllMaintenanceLane(&db);
                try DB.LifecycleCallbacks.load_managed_admission_snapshot_for_open(&db, alloc);
                if (opts.open_mode == .status_only) {
                    try db.core.loadIndexCatalogOnly();
                } else if (opts.open_mode == .query_readonly) {
                    const load_indexes_started_ns = monotonicTimeNs();
                    try db.core.loadIndexesNoBackfill();
                    profile.load_indexes_ns = elapsedSince(load_indexes_started_ns);
                } else {
                    const load_indexes_started_ns = monotonicTimeNs();
                    try db.core.loadIndexes();
                    profile.load_indexes_ns = elapsedSince(load_indexes_started_ns);
                }
                try DB.LifecycleCallbacks.initialize_index_repair_state_for_open(&db, alloc);
                if (opts.open_mode != .status_only) {
                    Self.hydrateAlgebraicObservationStatusBestEffort(&db);
                }
                if (opts.open_mode != .status_only) {
                    try Self.rebaseManagedIndexAppliedSequencesIfNeeded(&db);
                }
                if (!openModeRequiresReadOnlyBackends(opts.open_mode)) {
                    _ = try db.runArtifactRepairMetadataMaintenancePass();
                    try db.persistIndexLoadFailuresFromManager(alloc);
                    if (!ha_standby_role) {
                        _ = try db.discoverRelationalIndexDropJobs("");
                    }
                }
                Self.recordStartupOpenStats(&db, profile);
                if (openModeAllowsReplay(opts.open_mode)) {
                    const replay_started_ns = monotonicTimeNs();
                    DB.LifecycleCallbacks.replay_pending_derived_batches(&db, null, null) catch |err| switch (err) {
                        error.ArtifactRepairRequired => {},
                        else => return err,
                    };
                    profile.replay_pending_derived_ns = elapsedSince(replay_started_ns);
                }
                if (optional_runtime_workers_enabled) {
                    try Self.resumeGeneratedReplayFromJournalIfNeeded(&db);
                }
                if (db.start_index_workers) {
                    const start_workers_started_ns = monotonicTimeNs();
                    try DB.LifecycleCallbacks.start_async_workers(&db);
                    if (openModeAllowsReplay(opts.open_mode)) {
                        Self.resumeAsyncReplayFromJournalIfNeeded(&db);
                    }
                    profile.start_index_workers_ns = elapsedSince(start_workers_started_ns);
                }
                if (optional_runtime_workers_enabled) {
                    const start_optional_started_ns = monotonicTimeNs();
                    try Self.startOptionalRuntimes(&db);
                    profile.start_optional_runtimes_ns = elapsedSince(start_optional_started_ns);
                }
                if (!openModeRequiresReadOnlyBackends(opts.open_mode)) {
                    DB.LifecycleCallbacks.schedule_generated_artifact_cleanup(&db);
                }
                profile.total_ns = monotonicTimeNs() - open_started_ns;
                if (openProfileEnabled()) {
                    logOpenProfile(path, opts.open_mode, db.start_index_workers, profile);
                }
                break :blk db;
            };
        }

        pub fn close(self: *DB) void {
            if (self.closed) return;
            self.closed = true;
            Self.deinitWrapperState(self, true);
        }

        pub fn isClosed(self: *const DB) bool {
            return self.closed;
        }

        pub fn durableRootIncarnation(self: *const DB) !u128 {
            if (self.root_incarnation == 0) {
                return error.DurableRootIncarnationUnavailable;
            }
            return self.root_incarnation;
        }

        pub fn maintenanceDriver(self: *DB) db_core.MaintenanceDriver {
            return .{
                .ptr = self,
                .vtable = &maintenance_driver_vtable,
            };
        }

        pub fn engine(self: *DB) db_core.Engine {
            return .{
                .ptr = self,
                .vtable = &engine_vtable,
            };
        }

        pub fn services(self: *DB) db_core.Services {
            return self.core.services(Self.engine(self), Self.maintenanceDriver(self));
        }

        pub fn runTransactionRecoveryOnce(self: *DB, config: transaction_runtime_mod.Config) !types.TransactionRecoveryStats {
            if (!config.enabled) return .{};
            if (config.replicated_metadata) {
                return try transaction_runtime_mod.recoverOnce(
                    self.alloc,
                    self.core.batchExecutionResources().store,
                    config,
                );
            }
            const resolve_participant = config.resolve_participant_fn orelse return error.MissingParticipantResolver;
            const resolver_ctx = config.resolver_ctx orelse return error.MissingParticipantResolver;
            const now_ns = config.clock.nowRealtimeNs();
            const resolved_finalized = try self.resolveFinalizedTransactionIntentsForRecovery(now_ns);

            var recovery_stats: types.TransactionRecoveryStats = .{
                .enabled = true,
                .lease_owned = config.lease_owned,
                .runs = 1,
                .resolved_finalized = resolved_finalized,
                .last_run_ns = now_ns,
            };

            // Notification may perform network I/O or route back to this DB. Keep
            // it entirely outside the apply lock and acknowledge each successful
            // delivery with a separate short, idempotent locked update.
            const txns = try self.core.listTransactions(self.alloc);
            defer self.alloc.free(txns);
            for (txns) |txn| {
                if (txn.status == .pending) continue;
                const unresolved = self.core.getUnresolvedTransactionParticipants(self.alloc, txn.txn_id) catch |err| switch (err) {
                    transactions_mod.TxnError.TxnNotFound => continue,
                    else => return err,
                };
                defer transactions_mod.freeParticipantList(self.alloc, unresolved);
                for (unresolved) |participant| {
                    recovery_stats.notification_attempts += 1;
                    if (config.local_participant) |local| {
                        if (std.mem.eql(u8, local, participant)) {
                            self.markTransactionParticipantResolved(txn.txn_id, participant) catch |err| switch (err) {
                                transactions_mod.TxnError.TxnNotFound => {},
                                else => return err,
                            };
                            recovery_stats.notification_successes += 1;
                            continue;
                        }
                    }
                    resolve_participant(resolver_ctx, txn.txn_id, participant, txn.status, txn.commit_version) catch {
                        recovery_stats.notification_failures += 1;
                        continue;
                    };
                    self.markTransactionParticipantResolved(txn.txn_id, participant) catch |err| switch (err) {
                        transactions_mod.TxnError.TxnNotFound => {},
                        else => return err,
                    };
                    recovery_stats.notification_successes += 1;
                }
            }

            // Serialize presumed-abort and metadata cleanup with prepare/resolve,
            // but do not hold the lock across participant callbacks above.
            self.core.lockApply();
            const local_stats = self.core.recoverTransactions(now_ns -| config.cutoff_ns, now_ns) catch |err| {
                self.core.unlockApply();
                return err;
            };
            self.core.unlockApply();
            recovery_stats.scanned_records = local_stats.scanned_records;
            recovery_stats.auto_aborted = local_stats.auto_aborted;
            recovery_stats.resolved_finalized += local_stats.resolved_finalized;
            recovery_stats.cleaned_records = local_stats.cleaned_records;
            recovery_stats.kept_recent_pending = local_stats.kept_recent_pending;
            recovery_stats.deferred_unresolved = local_stats.deferred_unresolved;
            return recovery_stats;
        }

        pub fn recordStartupOpenStats(self: *DB, profile: anytype) void {
            self.async_context.stats.startup.configured_indexes.store(self.core.index_manager.count(), .monotonic);
            self.async_context.stats.startup.configured_dense_indexes.store(@intCast(self.core.index_manager.dense_indexes.items.len), .monotonic);
            self.async_context.stats.startup.configured_sparse_indexes.store(@intCast(self.core.index_manager.sparse_indexes.items.len), .monotonic);
            self.async_context.stats.startup.configured_full_text_indexes.store(@intCast(self.core.index_manager.text_indexes.items.len), .monotonic);
            self.async_context.stats.startup.configured_graph_indexes.store(@intCast(self.core.index_manager.graph_indexes.items.len), .monotonic);
            self.async_context.stats.startup.opened_indexes.store(self.core.index_manager.count(), .monotonic);
            self.async_context.stats.startup.db_open_ns.store(profile.total_ns, .monotonic);
            self.async_context.stats.startup.load_indexes_ns.store(profile.load_indexes_ns, .monotonic);

            var lsm_open_stats = lsm_backend_mod.Backend.OpenStats{};
            if (self.core.primary_store_owner.snapshotLsmOpenStats()) |primary_open_stats| {
                lsm_backend_mod.Backend.accumulateOpenStats(&lsm_open_stats, primary_open_stats);
            }
            lsm_backend_mod.Backend.accumulateOpenStats(&lsm_open_stats, self.core.index_manager.snapshotLsmOpenStats());
            self.async_context.stats.startup.lsm_open_stores.store(lsm_open_stats.started, .monotonic);
            self.async_context.stats.startup.lsm_open_completed.store(lsm_open_stats.completed, .monotonic);
            self.async_context.stats.startup.lsm_open_failed.store(lsm_open_stats.failed, .monotonic);
            self.async_context.stats.startup.lsm_open_total_ns.store(lsm_open_stats.total_ns, .monotonic);
            self.async_context.stats.startup.lsm_open_initializing_storage_ns.store(lsm_open_stats.initializing_storage_ns, .monotonic);
            self.async_context.stats.startup.lsm_open_recovered_temp_cleanup_ns.store(lsm_open_stats.cleaning_recovered_run_temps_ns, .monotonic);
            self.async_context.stats.startup.lsm_open_manifest_ns.store(lsm_open_stats.opening_manifest_ns, .monotonic);
            self.async_context.stats.startup.lsm_open_ensuring_dirs_ns.store(lsm_open_stats.ensuring_dirs_ns, .monotonic);
            self.async_context.stats.startup.lsm_open_wal_replay_ns.store(lsm_open_stats.replaying_wal_ns, .monotonic);
            self.async_context.stats.startup.lsm_open_mounting_runs_ns.store(lsm_open_stats.mounting_runs_ns, .monotonic);
            self.async_context.stats.startup.lsm_open_loaded_runs.store(lsm_open_stats.loaded_runs, .monotonic);
            self.async_context.stats.startup.lsm_open_obsolete_paths.store(lsm_open_stats.obsolete_paths, .monotonic);
            self.async_context.stats.startup.lsm_open_mutable_entries_after_replay.store(lsm_open_stats.mutable_entries_after_replay, .monotonic);
            self.async_context.stats.startup.lsm_open_immutable_memtables_after_replay.store(lsm_open_stats.immutable_memtables_after_replay, .monotonic);
            self.async_context.stats.startup.lsm_open_recovered_temp_files_deleted.store(lsm_open_stats.recovered_table_temp_files_deleted, .monotonic);
            self.async_context.stats.startup.lsm_open_recovered_temp_bytes_deleted.store(lsm_open_stats.recovered_table_temp_bytes_deleted, .monotonic);
            self.async_context.stats.startup.wal_replay_records.store(lsm_open_stats.wal_replay_records, .monotonic);
            self.async_context.stats.startup.wal_replay_entries.store(lsm_open_stats.wal_replay_entries, .monotonic);
            self.async_context.stats.startup.wal_replay_bytes.store(lsm_open_stats.wal_replay_bytes, .monotonic);
            self.async_context.stats.startup.wal_replay_ns.store(lsm_open_stats.wal_replay_ns, .monotonic);
            self.async_context.stats.startup.wal_replay_truncated_tail_bytes.store(lsm_open_stats.wal_replay_truncated_tail_bytes, .monotonic);

            const lsm_maintenance_stats = Self.snapshotLsmMaintenanceStatsLocked(self);
            self.async_context.stats.startup.wal_retention_known.store(true, .monotonic);
            self.async_context.stats.startup.wal_retained_segments.store(lsm_maintenance_stats.wal_retained_segments, .monotonic);
            self.async_context.stats.startup.wal_retained_bytes.store(lsm_maintenance_stats.wal_retained_bytes, .monotonic);
            self.async_context.stats.startup.wal_checkpoint_oldest_retained_segment.store(lsm_maintenance_stats.wal_checkpoint_oldest_retained_segment, .monotonic);
            self.async_context.stats.startup.wal_checkpoint_covered_through_segment.store(lsm_maintenance_stats.wal_checkpoint_covered_through_segment, .monotonic);
            self.async_context.stats.startup.wal_checkpoint_current_segment.store(lsm_maintenance_stats.wal_checkpoint_current_segment, .monotonic);
            self.async_context.stats.startup.wal_checkpoint_lag_segments.store(lsm_maintenance_stats.wal_checkpoint_lag_segments, .monotonic);
            self.async_context.stats.startup.wal_replay_retained_segments.store(lsm_maintenance_stats.wal_replay_retained_segments, .monotonic);
            self.async_context.stats.startup.wal_replay_retained_bytes.store(lsm_maintenance_stats.wal_replay_retained_bytes, .monotonic);
            self.async_context.stats.startup.wal_replay_current_segment.store(lsm_maintenance_stats.wal_replay_current_segment, .monotonic);
        }

        pub fn attachAlgebraicHllMaintenanceLane(self: *DB) !void {
            const runtime = self.backend_runtime;
            const owner_id = try runtime.allocOwnerId();
            self.algebraic_hll_owner_id = owner_id;
            self.core.index_manager.attachHllMaintenance(runtime.durable_jobs, owner_id);
        }

        pub fn hydrateAlgebraicObservationStatusBestEffort(self: *DB) void {
            for (self.core.index_manager.algebraic_indexes.items) |*entry| {
                entry.index.loadPersistedObservations(self.core.store) catch {};
            }
        }

        pub fn hydrateAlgebraicObservationStatusForIndexBestEffort(self: *DB, index_name: []const u8) void {
            const entry = self.core.index_manager.algebraicIndex(index_name) orelse return;
            entry.index.loadPersistedObservations(self.core.store) catch {};
        }

        pub fn resumeGeneratedReplayFromJournalIfNeeded(self: *DB) !void {
            const runtime = self.enrichment_runtime orelse return;
            if (!self.core.hasGeneratedEnrichmentTargets()) return;

            const target_sequence = self.core.nextEnrichmentSequence();
            if (target_sequence == 0) return;
            try runtime.resumeFrom(target_sequence, target_sequence);
        }

        pub fn resumeAsyncReplayFromJournalIfNeeded(self: *DB) void {
            const target_sequence = self.core.nextDerivedSequence();
            if (target_sequence == 0) return;
            self.executor.notifySequence(target_sequence);
        }

        pub fn initAsyncInfrastructure(
            self: *DB,
            executor_config: derived_executor_mod.Config,
            resource_manager: ?*resource_manager_mod.ResourceManager,
        ) !void {
            const async_resources = self.core.asyncResources();
            self.async_context.* = .{
                .alloc = self.runtime_alloc,
                .store = async_resources.store,
                .applied_sequence_checkpoint_path = async_resources.applied_sequence_checkpoint_path,
                .index_repair_checkpoint = async_resources.index_repair_checkpoint,
                .index_manager = async_resources.index_manager,
                .apply_mutex = async_resources.apply_mutex,
                .repair_replay_mutex = async_resources.repair_replay_mutex,
                .io = self.backend_runtime.io(),
                .require_graph_resolution_contract = true,
                .query_visibility_hook = null,
                .text_merge_runtime = null,
                .relational_base_rows = self.relationalColumnsForStore() != null,
            };
            self.executor.* = try derived_executor_mod.init(
                self.runtime_alloc,
                executor_config,
                self.core.replaySource(),
                self.async_context,
                DB.LifecycleCallbacks.apply_derived_batch_to_index_async,
                DB.LifecycleCallbacks.persist_applied_sequence_async,
                DB.LifecycleCallbacks.truncate_replay_sequence_async,
                DB.LifecycleCallbacks.begin_derived_catch_up_session_async,
                DB.LifecycleCallbacks.finish_derived_catch_up_session_async,
                DB.LifecycleCallbacks.can_advance_derived_to_target_async,
                DB.LifecycleCallbacks.notify_derived_applied_sequence_advanced,
                resource_manager,
                self.backend_runtime,
            );
        }

        pub fn setQueryVisibilityHook(self: *DB, hook: anytype) void {
            if (hook != null) Self.bindTtlIdentityVisibilityOwner(self);
            var pending_hook: @TypeOf(self.async_context.query_visibility_hook) = null;
            db_internal.lockAtomicWithBackoff(&self.async_context.query_visibility_hook_mutex);
            self.async_context.query_visibility_hook = hook;
            if (hook) |attached| {
                if (self.async_context.index_repair_notification_pending) {
                    self.async_context.index_repair_notification_pending = false;
                    _ = self.async_context.query_visibility_hook_in_flight.fetchAdd(1, .acquire);
                    pending_hook = attached;
                }
            }
            self.async_context.query_visibility_hook_mutex.unlock();
            if (pending_hook) |attached| {
                attached.notify(.index_repair_pending);
                _ = self.async_context.query_visibility_hook_in_flight.fetchSub(1, .release);
            }
            if (hook == null) {
                while (self.async_context.query_visibility_hook_in_flight.load(.acquire) != 0) {
                    db_internal.spinOrYield();
                }
            }
            if (self.enrichment_runtime) |runtime| {
                runtime.setStatusHook(if (hook == null) null else .{
                    .ptr = self.async_context,
                    .on_change = DB.LifecycleCallbacks.notify_async_context_visibility_hook,
                });
            }
        }

        fn bindTtlIdentityVisibilityOwner(self: *DB) void {
            const ttl_ctx = self.ttl_cleanup_context orelse return;
            ttl_ctx.identity_visibility_owner.store(self, .release);

            self.core.lockApply();
            defer self.core.unlockApply();
            if (doc_identity.visibilitySummaryFromStore(self.core.store) catch null) |summary| {
                self.identity_visibility_summary_cache = summary;
                self.clearLiveDocSetCache();
                self.clearNonVisibleDocSetCache();
            }
        }

        pub fn getGroupCreatedAtMillis(self: *DB, alloc: std.mem.Allocator, group_id: u64) !?u64 {
            const key = try groupCreatedAtMetadataKeyAlloc(alloc, group_id);
            defer alloc.free(key);
            const raw = try self.get(alloc, key) orelse return null;
            defer alloc.free(raw);
            return try std.fmt.parseInt(u64, raw, 10);
        }

        pub fn ensureGroupCreatedAtMillis(self: *DB, alloc: std.mem.Allocator, group_id: u64, now_ms: u64) !u64 {
            if (try Self.getGroupCreatedAtMillis(self, alloc, group_id)) |created_at_millis| return created_at_millis;

            const key = try groupCreatedAtMetadataKeyAlloc(alloc, group_id);
            defer alloc.free(key);
            const encoded = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(encoded);

            try self.batch(.{
                .writes = &.{
                    .{
                        .key = key,
                        .value = encoded,
                    },
                },
            });
            return now_ms;
        }

        const DetachedEnrichmentRuntime = struct {
            append_ctx: ?*db_internal.EnrichmentAppendContext(DB) = null,
            runtime: ?*enrichment_runtime_mod.EnrichmentRuntime = null,

            fn deinit(self: *@This(), alloc: Allocator) void {
                if (self.runtime) |runtime| {
                    runtime.deinit();
                    alloc.destroy(runtime);
                    self.runtime = null;
                }
                if (self.append_ctx) |ctx| {
                    alloc.destroy(ctx);
                    self.append_ctx = null;
                }
            }

            fn take(self: *@This()) struct {
                append_ctx: *db_internal.EnrichmentAppendContext(DB),
                runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            } {
                const append_ctx = self.append_ctx.?;
                const runtime = self.runtime.?;
                self.append_ctx = null;
                self.runtime = null;
                return .{
                    .append_ctx = append_ctx,
                    .runtime = runtime,
                };
            }
        };

        fn createDetachedEnrichmentRuntime(self: *DB, enrichment_cfg: enrichment_runtime_mod.Config) !?DetachedEnrichmentRuntime {
            if (enrichment_cfg.dense_embedder == null and enrichment_cfg.sparse_embedder == null and enrichment_cfg.asset_producer == null and !enrichment_cfg.enable_without_producers) return null;
            var runtime_enrichment_cfg = enrichment_cfg;
            runtime_enrichment_cfg.relational_base_rows = self.relationalColumnsForStore() != null;

            const append_ctx = try self.runtime_alloc.create(db_internal.EnrichmentAppendContext(DB));
            errdefer self.runtime_alloc.destroy(append_ctx);
            const resources = self.core.batchExecutionResources();
            append_ctx.* = .{
                .alloc = self.runtime_alloc,
                .store = resources.store,
                .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
                .index_repair_checkpoint = resources.index_repair_checkpoint,
                .shard_manager = resources.shard_manager,
                .index_manager = resources.index_manager,
                .apply_mutex = resources.apply_mutex,
                .repair_replay_mutex = resources.repair_replay_mutex,
                .change_journal = resources.change_journal,
                .replay_source = resources.replay_source,
                .executor = self.executor,
                .async_context = self.async_context,
                .log_mutex = resources.log_mutex,
                .identity_namespace = resources.identity_namespace,
                .ha_async_effect_mirror = self.ha_async_effect_mirror,
                .ha_async_batch_mirror = self.ha_async_batch_mirror,
                .ha_async_metadata_mirror = self.ha_async_metadata_mirror,
                .ha_write_gate = self.ha_write_gate,
                .resolution_runtime = self.resolution_runtime,
                .promotion_runtime = self.promotion_runtime,
            };

            const runtime = try self.runtime_alloc.create(enrichment_runtime_mod.EnrichmentRuntime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try enrichment_runtime_mod.EnrichmentRuntime.init(
                self.runtime_alloc,
                self.core.batchExecutionResources().store,
                self.core.batchExecutionResources().change_journal,
                self.core.replaySource(),
                self.core.batchExecutionResources().index_manager,
                self.core.batchExecutionResources().apply_mutex,
                append_ctx,
                DB.LifecycleCallbacks.append_generated_batch_from_enrichment,
                append_ctx,
                DB.LifecycleCallbacks.record_enrichment_request_failure,
                DB.LifecycleCallbacks.enrichment_request_failure_pending,
                .{
                    .ptr = append_ctx,
                    .lock_fn = DB.LifecycleCallbacks.lock_enrichment_failure_pending_fence,
                    .unlock_fn = DB.LifecycleCallbacks.unlock_enrichment_failure_pending_fence,
                },
                self.executor,
                DB.LifecycleCallbacks.notify_derived_executor_sequence,
                self.backend_runtime,
                runtime_enrichment_cfg,
            );
            errdefer runtime.deinit();
            return .{
                .append_ctx = append_ctx,
                .runtime = runtime,
            };
        }

        pub fn initOptionalEnrichmentRuntime(self: *DB, enrichment_cfg: enrichment_runtime_mod.Config) !void {
            var detached = (try Self.createDetachedEnrichmentRuntime(self, enrichment_cfg)) orelse return;
            const owned = detached.take();
            self.enrichment_runtime = owned.runtime;
            self.enrichment_append_context = owned.append_ctx;
            self.async_context.enrichment_runtime = owned.runtime;
        }

        fn deinitEnrichmentConfig(self: *DB, cfg: *enrichment_runtime_mod.Config) void {
            deinitOwnedEnrichmentConfig(self.runtime_alloc, cfg);
        }

        pub fn reconfigureEnrichmentRuntime(self: *DB, cfg: enrichment_runtime_mod.Config) !void {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;

            var owned_cfg = cfg;
            var cfg_owned = true;
            errdefer if (cfg_owned) Self.deinitEnrichmentConfig(self, &owned_cfg);

            const query_visibility_hook_present = DB.hasQueryVisibilityHook(self.async_context);
            var detached = try Self.createDetachedEnrichmentRuntime(self, owned_cfg);
            cfg_owned = false;
            errdefer if (detached) |*runtime| runtime.deinit(self.runtime_alloc);
            if (detached) |*runtime| {
                if (self.core.hasGeneratedEnrichmentTargets()) {
                    const target_sequence = self.core.nextEnrichmentSequence();
                    if (target_sequence != 0) try runtime.runtime.?.resumeFrom(target_sequence, target_sequence);
                }
                if (query_visibility_hook_present) {
                    runtime.runtime.?.setStatusHook(.{
                        .ptr = self.async_context,
                        .on_change = DB.LifecycleCallbacks.notify_async_context_visibility_hook,
                    });
                }
            }

            const should_start_replacement = detached != null and self.open_mode.allowsOptionalRuntimes();
            const previous_desired = self.async_context.enrichment_desired_running.swap(false, .acq_rel);
            var stopped_existing_runtime = false;
            if (should_start_replacement) {
                db_internal.lockAtomicWithBackoff(&self.async_context.enrichment_lifecycle_mutex);
                if (self.async_context.enrichment_runtime) |runtime| {
                    runtime.stop();
                    stopped_existing_runtime = true;
                }
                self.async_context.enrichment_lifecycle_mutex.unlock();
            }
            errdefer if (stopped_existing_runtime) {
                self.async_context.enrichment_desired_running.store(previous_desired or stopped_existing_runtime, .release);
                DB.LifecycleCallbacks.restart_enrichment_after_structural_mutation(self, "failed enrichment reconfiguration", "") catch |err| {
                    std.log.err("failed to restart previous enrichment runtime after reconfigure failure: {}", .{err});
                };
            };

            if (should_start_replacement) {
                try detached.?.runtime.?.start();
            }

            db_internal.lockAtomicWithBackoff(&self.async_context.enrichment_lifecycle_mutex);
            if (self.enrichment_runtime) |runtime| {
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
                self.enrichment_runtime = null;
                self.async_context.enrichment_runtime = null;
            }
            if (self.enrichment_append_context) |ctx| {
                self.runtime_alloc.destroy(ctx);
                self.enrichment_append_context = null;
            }

            if (detached) |*runtime| {
                const owned = runtime.take();
                self.enrichment_append_context = owned.append_ctx;
                self.enrichment_runtime = owned.runtime;
                self.async_context.enrichment_runtime = owned.runtime;
            }
            self.async_context.enrichment_desired_running.store(should_start_replacement, .release);
            self.async_context.enrichment_lifecycle_mutex.unlock();
            if (!query_visibility_hook_present) Self.setQueryVisibilityHook(self, null);
        }

        pub fn initResolutionRuntime(self: *DB) !void {
            // Always constructed so catalog/status/runUntilIdle APIs can observe
            // and drain replay. The background worker is started lazily only while
            // the resolver catalog is non-empty.
            const append_ctx = try self.runtime_alloc.create(@TypeOf(self.resolution_append_context.?.*));
            errdefer self.runtime_alloc.destroy(append_ctx);
            const resources = self.core.batchExecutionResources();
            append_ctx.* = .{
                .alloc = self.runtime_alloc,
                .store = resources.store,
                .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
                .index_repair_checkpoint = resources.index_repair_checkpoint,
                .shard_manager = resources.shard_manager,
                .index_manager = resources.index_manager,
                .apply_mutex = resources.apply_mutex,
                .repair_replay_mutex = resources.repair_replay_mutex,
                .change_journal = resources.change_journal,
                .replay_source = resources.replay_source,
                .executor = self.executor,
                .async_context = self.async_context,
                .log_mutex = resources.log_mutex,
                .identity_namespace = resources.identity_namespace,
                .ha_async_effect_mirror = self.ha_async_effect_mirror,
                .ha_async_batch_mirror = self.ha_async_batch_mirror,
                .ha_async_metadata_mirror = self.ha_async_metadata_mirror,
                .ha_write_gate = self.ha_write_gate,
            };

            const runtime = try self.runtime_alloc.create(resolution_runtime_mod.ResolutionRuntime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try resolution_runtime_mod.ResolutionRuntime.init(
                self.runtime_alloc,
                self.core.batchExecutionResources().store,
                self.core.replaySource(),
                self.core.batchExecutionResources().index_manager,
                append_ctx,
                DB.LifecycleCallbacks.append_derived_batch_from_enrichment,
                self.backend_runtime,
                // Cross-shard blocking source when the serving layer injected one
                // (via OpenOptions); null means local-only blocking against this
                // worker's own store.
                self.resolution_candidate_source,
                // Optional name embedder for mention-embedding backfill.
                self.resolution_embedder,
            );
            errdefer runtime.deinit();
            append_ctx.resolution_runtime = runtime;
            self.resolution_append_context = append_ctx;
            self.resolution_runtime = runtime;
            self.async_context.resolution_runtime = runtime;
            if (self.enrichment_append_context) |ctx| ctx.resolution_runtime = runtime;
        }

        pub fn initPromotionRuntime(self: *DB) !void {
            // Always constructed (like the resolution runtime) so synchronous
            // drains and status APIs remain available. The background worker is
            // started lazily only while resolver replay can produce promotion work.
            const runtime = try self.runtime_alloc.create(promotion_runtime_mod.PromotionRuntime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try promotion_runtime_mod.PromotionRuntime.init(
                self.runtime_alloc,
                self.core.batchExecutionResources().store,
                self.core.replaySource(),
                self.backend_runtime,
                self.promotion_owner,
                // Cross-shard entity sink when the serving layer injected one (via
                // OpenOptions or setEntitySink); the missing-sink policy decides
                // whether null means wait or explicit no-op.
                self.entity_sink,
                self.entity_sink_missing_policy,
            );
            errdefer runtime.deinit();
            self.promotion_runtime = runtime;
            self.async_context.promotion_runtime = runtime;
            // Patch the resolution stage's append context so journaling a resolution
            // artifact (tagged with the promotion hint) wakes the promoter.
            if (self.resolution_append_context) |ctx| ctx.promotion_runtime = runtime;
        }

        /// Inject (or clear) the cross-shard entity-resolution candidate source
        /// on an already-open DB. The serving layer (managed write cache) calls
        /// this right after a DB is opened, since managed DBs open lazily and
        /// cannot thread the source through `OpenOptions`. No-op when this DB has
        /// no resolution runtime (e.g. read-only / status opens).
        pub fn setResolutionCandidateSource(self: *DB, src: ?resolution_runtime_mod.CandidateSource) void {
            self.resolution_candidate_source = src;
            if (self.resolution_runtime) |runtime| runtime.setCandidateSource(src);
        }

        /// Inject (or clear) the cross-shard entity sink on an already-open DB.
        /// The serving layer (managed write cache) calls this right after a DB is
        /// opened, since managed DBs open lazily and cannot thread the sink
        /// through `OpenOptions`. No-op when this DB has no promotion runtime.
        pub fn setEntitySink(self: *DB, sink: ?promotion_runtime_mod.EntitySink) void {
            self.entity_sink = sink;
            if (self.promotion_runtime) |runtime| runtime.setSink(sink);
        }

        /// Inject (or clear) the source-shard promotion owner on an already-open
        /// DB. Serving-layer managed DBs use this to keep follower raft apply
        /// from turning replay into public entity-table writes.
        pub fn setPromotionOwner(self: *DB, owner: ?promotion_runtime_mod.PromotionOwner) void {
            self.promotion_owner = owner;
            if (self.promotion_runtime) |runtime| runtime.setOwner(owner);
        }

        pub fn initOptionalTtlRuntime(self: *DB, cfg: ttl_runtime_mod.Config) !void {
            const ttl_ctx = try self.runtime_alloc.create(@TypeOf(self.ttl_cleanup_context.?.*));
            errdefer self.runtime_alloc.destroy(ttl_ctx);
            const batch_resources = self.core.batchExecutionResources();
            ttl_ctx.* = .{
                .batch = .{
                    .alloc = self.runtime_alloc,
                    .store = batch_resources.store,
                    .applied_sequence_checkpoint_path = batch_resources.applied_sequence_checkpoint_path,
                    .index_repair_checkpoint = batch_resources.index_repair_checkpoint,
                    .shard_manager = batch_resources.shard_manager,
                    .change_journal = batch_resources.change_journal,
                    .replay_source = batch_resources.replay_source,
                    .index_manager = batch_resources.index_manager,
                    .apply_mutex = batch_resources.apply_mutex,
                    .repair_replay_mutex = batch_resources.repair_replay_mutex,
                    .log_mutex = batch_resources.log_mutex,
                    .identity_namespace = batch_resources.identity_namespace,
                    .artifact_cleanup_maybe = batch_resources.artifact_cleanup_maybe,
                    .executor = self.executor,
                    .io = self.backend_runtime.io(),
                    .enrichment_runtime = self.enrichment_runtime,
                    .async_context = self.async_context,
                    .relational_base_rows = self.relationalColumnsForStore() != null,
                    .resolution_runtime = self.resolution_runtime,
                    .promotion_runtime = self.promotion_runtime,
                    .ha_async_effect_mirror = self.ha_async_effect_mirror,
                    .ha_async_batch_mirror = self.ha_async_batch_mirror,
                    .ha_async_metadata_mirror = self.ha_async_metadata_mirror,
                    .ha_write_gate = self.ha_write_gate,
                },
                .grace_period_ns = cfg.grace_period_ns,
            };
            ttl_ctx.batch.identity_visibility_owner_slot = &ttl_ctx.identity_visibility_owner;
            const runtime = try self.runtime_alloc.create(ttl_runtime_mod.TtlRuntime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try ttl_runtime_mod.TtlRuntime.init(
                self.runtime_alloc,
                self.core.batchExecutionResources().store,
                ttl_ctx,
                DB.LifecycleCallbacks.delete_expired_documents_from_candidates,
                &self.async_context.text_merge_deferred,
                self.backend_runtime,
                cfg,
            );
            errdefer runtime.deinit();
            self.ttl_cleanup_context = ttl_ctx;
            self.ttl_runtime = runtime;
        }

        pub fn initOptionalTransactionRuntime(self: *DB, cfg: transaction_runtime_mod.Config) !void {
            const identity_ctx = try self.runtime_alloc.create(db_core.TransactionRecoveryIdentityContext);
            errdefer self.runtime_alloc.destroy(identity_ctx);
            identity_ctx.* = .{
                .store = self.core.store,
                .identity_namespace = self.core.identity_namespace,
                .alloc = self.runtime_alloc,
                .relational_base_rows = self.relationalColumnsForStore() != null,
                .relational_columns = self.relationalColumnsForStore() orelse &.{},
                .relational_indexes = self.relationalIndexesForStore(),
            };
            var effective_cfg = cfg;
            effective_cfg.resolution_extra_hooks = db_core.transactionRecoveryIdentityHooks(identity_ctx);

            const runtime = try self.runtime_alloc.create(transaction_runtime_mod.Runtime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try transaction_runtime_mod.Runtime.init(
                self.runtime_alloc,
                self.core.batchExecutionResources().store,
                self.backend_runtime,
                effective_cfg,
            );
            errdefer runtime.deinit();
            self.transaction_recovery_identity_context = identity_ctx;
            self.transaction_runtime = runtime;
        }

        pub fn initOptionalTextMergeRuntime(self: *DB, cfg: text_merge_runtime_mod.Config) !void {
            if (!self.start_index_workers) return;
            if (!cfg.enabled) return;
            const resources = self.core.asyncResources();
            const runtime = try self.runtime_alloc.create(text_merge_runtime_mod.TextMergeRuntime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try text_merge_runtime_mod.TextMergeRuntime.init(
                self.runtime_alloc,
                resources.index_manager,
                resources.apply_mutex,
                self.backend_runtime,
                cfg,
            );
            errdefer runtime.deinit();
            self.text_merge_runtime = runtime;
            self.async_context.text_merge_runtime = runtime;
        }

        pub fn initOptionalSparseCompactionRuntime(self: *DB, cfg: sparse_compaction_runtime_mod.Config) !void {
            if (!self.start_index_workers) return;
            if (!cfg.enabled) return;
            const resources = self.core.asyncResources();
            const runtime = try self.runtime_alloc.create(sparse_compaction_runtime_mod.SparseCompactionRuntime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try sparse_compaction_runtime_mod.SparseCompactionRuntime.init(
                self.runtime_alloc,
                resources.index_manager,
                resources.apply_mutex,
                self.backend_runtime,
                cfg,
            );
            errdefer runtime.deinit();
            self.sparse_compaction_runtime = runtime;
            self.async_context.sparse_compaction_runtime = runtime;
        }

        pub fn initOptionalGraphMetricRuntime(self: *DB, cfg: graph_metric_runtime_mod.Config) !void {
            if (!self.start_index_workers) return;
            if (!cfg.enabled) return;
            const resources = self.core.asyncResources();
            const runtime = try self.runtime_alloc.create(graph_metric_runtime_mod.GraphMetricRuntime);
            errdefer self.runtime_alloc.destroy(runtime);
            runtime.* = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                self.runtime_alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                self.backend_runtime,
                cfg,
            );
            errdefer runtime.deinit();
            self.graph_metric_runtime = runtime;
        }

        pub fn initOptionalRuntimes(self: *DB, opts: anytype) !void {
            // Created before enrichment so the enrichment append context can notify
            // it when extraction artifacts land.
            try Self.initResolutionRuntime(self);
            // Created after the resolution runtime so it can patch the resolution
            // append context to wake the promoter when resolution artifacts land.
            try Self.initPromotionRuntime(self);
            if (opts.enrichment) |raw_enrichment_cfg| {
                var enrichment_cfg = raw_enrichment_cfg;
                if (enrichment_cfg.secret_store == null) enrichment_cfg.secret_store = opts.secret_store;
                if (enrichment_cfg.remote_content == null) enrichment_cfg.remote_content = opts.remote_content;
                if (enrichment_cfg.resource_manager == null) enrichment_cfg.resource_manager = opts.resource_manager;
                try Self.initOptionalEnrichmentRuntime(self, enrichment_cfg);
                opts.enrichment = null;
            }
            if (opts.ttl_cleanup.enabled) {
                try Self.initOptionalTtlRuntime(self, opts.ttl_cleanup);
            }
            if (opts.transaction_recovery.enabled) {
                try Self.initOptionalTransactionRuntime(self, opts.transaction_recovery);
            }
            try Self.initOptionalTextMergeRuntime(self, opts.text_merge);
            try Self.initOptionalSparseCompactionRuntime(self, opts.sparse_compaction);
            try Self.initOptionalGraphMetricRuntime(self, opts.graph_metric_maintenance);
        }

        pub fn startOptionalRuntimes(self: *DB) !void {
            try Self.startResolverReplayRuntimesIfConfigured(self);
            if (self.enrichment_runtime) |runtime| {
                try runtime.start();
                self.async_context.enrichment_desired_running.store(true, .release);
            }
            if (self.ttl_runtime) |runtime| try runtime.start();
            if (self.transaction_runtime) |runtime| {
                runtime.config.local_resolution_ctx = self;
                runtime.config.resolve_local_fn = resolveRecoveredLocalTransaction;
                try runtime.start();
            }
            if (self.text_merge_runtime) |runtime| try runtime.start();
            if (self.sparse_compaction_runtime) |runtime| try runtime.start();
            if (self.graph_metric_runtime) |runtime| try runtime.start();
            self.startArtifactRepairMetadataWorkerIfNeeded();
            self.startQuarantineRetryWorkerIfNeeded();
        }

        pub fn ensureTransactionRecoveryRuntime(self: *DB, cfg: transaction_runtime_mod.Config) !void {
            if (!cfg.enabled or self.transaction_runtime != null) return;
            self.core.lockApply();
            defer self.core.unlockApply();
            if (self.transaction_runtime != null) return;
            try Self.initOptionalTransactionRuntime(self, cfg);
            if (self.optional_runtime_workers_enabled) {
                const runtime = self.transaction_runtime.?;
                runtime.config.local_resolution_ctx = self;
                runtime.config.resolve_local_fn = resolveRecoveredLocalTransaction;
                try runtime.start();
            }
        }

        pub fn hasConfiguredResolvers(self: *const DB) bool {
            return self.core.index_manager.resolvers.items.len > 0;
        }

        pub fn resolverReplayNeedsDrain(self: *DB) bool {
            if (Self.hasConfiguredResolvers(self)) return true;
            if (self.resolution_runtime) |runtime| {
                const replay_stats = runtime.stats();
                if (replay_stats.catch_up_required or replay_stats.blocked) return true;
            }
            if (self.promotion_runtime) |runtime| {
                const replay_stats = runtime.stats();
                if (replay_stats.catch_up_required or replay_stats.blocked) return true;
            }
            return false;
        }

        pub fn notifyResolverReplayRuntimes(self: *DB, sequence: u64) void {
            if (!Self.hasConfiguredResolvers(self)) return;
            Self.notifyResolverReplayRuntimesForced(self, sequence);
        }

        pub fn notifyResolverReplayRuntimesForced(self: *DB, sequence: u64) void {
            if (self.resolution_runtime) |runtime| runtime.notifySequence(sequence);
            if (self.promotion_runtime) |runtime| runtime.notifySequence(sequence);
        }

        pub fn startResolverReplayRuntimesIfConfigured(self: *DB) !void {
            if (!Self.hasConfiguredResolvers(self)) return;
            try Self.startResolverReplayRuntimes(self);
        }

        pub fn startResolverReplayRuntimes(self: *DB) !void {
            if (self.resolution_runtime) |runtime| try runtime.start();
            if (self.promotion_runtime) |runtime| try runtime.start();
        }

        pub fn stopResolverReplayRuntimesIfUnconfigured(self: *DB) void {
            if (Self.hasConfiguredResolvers(self)) return;
            if (self.resolution_runtime) |runtime| runtime.stop();
            if (self.promotion_runtime) |runtime| runtime.stop();
        }

        pub fn addResolver(self: *DB, cfg: index_manager_mod.ResolverConfig) !void {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            {
                self.core.lockApply();
                defer self.core.unlockApply();
                try self.core.addResolver(cfg);
            }
            try Self.startResolverReplayRuntimes(self);
            try Self.backfillResolverCorpus(self);
        }

        /// Add or replace a resolver. Inserts and material config changes re-resolve
        /// the existing corpus so the new resolver/scorer behavior applies to
        /// documents already ingested (the extraction artifacts did not change, so
        /// the incremental hint would not fire on its own).
        pub fn upsertResolverWithResultOptions(
            self: *DB,
            cfg: index_manager_mod.ResolverConfig,
            options: ResolverUpsertOptions,
        ) !index_manager_mod.IndexManager.ResolverUpsertResult {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            const upsert_result = blk: {
                self.core.lockApply();
                defer self.core.unlockApply();
                break :blk try self.core.upsertResolver(cfg);
            };
            switch (upsert_result) {
                .inserted, .updated_backfill_required => {
                    try Self.startResolverReplayRuntimes(self);
                    if (options.drain_backfill) {
                        try Self.backfillResolverCorpus(self);
                    } else if (self.resolution_runtime) |runtime| {
                        try runtime.requestReresolveBacklog();
                    }
                },
                .updated_no_backfill => {
                    try Self.startResolverReplayRuntimesIfConfigured(self);
                    if (options.drain_backfill and self.resolution_runtime != null) {
                        const runtime = self.resolution_runtime.?;
                        if (try runtime.hasReresolveBacklog()) {
                            try Self.backfillResolverCorpus(self);
                        }
                    }
                },
            }
            return upsert_result;
        }

        pub fn upsertResolverWithResult(self: *DB, cfg: index_manager_mod.ResolverConfig) !index_manager_mod.IndexManager.ResolverUpsertResult {
            return try Self.upsertResolverWithResultOptions(self, cfg, .{});
        }

        pub fn upsertResolver(self: *DB, cfg: index_manager_mod.ResolverConfig) !void {
            _ = try Self.upsertResolverWithResult(self, cfg);
        }

        pub fn drainResolverBackfill(self: *DB) !void {
            try Self.startResolverReplayRuntimesIfConfigured(self);
            try Self.backfillResolverCorpus(self);
        }

        pub fn listResolvers(self: *DB, alloc: Allocator) ![]index_manager_mod.ResolverConfig {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.core.listResolvers(alloc);
        }

        pub fn removeResolver(self: *DB, name: []const u8) !bool {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            try Self.retireResolverReplayBeforeCatalogRemoval(self);

            const retirement_sequence = blk: {
                self.core.lockApply();
                defer self.core.unlockApply();

                const cfg = (try Self.resolverConfigByNameAlloc(self, name)) orelse return false;
                defer {
                    var owned = cfg;
                    owned.deinit(self.alloc);
                }

                break :blk try Self.retireResolverArtifactsLocked(self, cfg);
            };

            if (retirement_sequence) |sequence| {
                self.executor.notifySequence(sequence);
                Self.notifyResolverReplayRuntimesForced(self, sequence);
                try Self.runUntilIdle(self);
            }

            {
                self.core.lockApply();
                defer self.core.unlockApply();
                if (!try self.core.removeResolver(name)) return false;
            }

            Self.stopResolverReplayRuntimesIfUnconfigured(self);
            return true;
        }

        fn resolverConfigByNameAlloc(self: *DB, name: []const u8) !?index_manager_mod.ResolverConfig {
            const resolvers = try self.core.listResolvers(self.alloc);
            defer {
                for (resolvers) |*cfg| cfg.deinit(self.alloc);
                if (resolvers.len > 0) self.alloc.free(resolvers);
            }
            for (resolvers) |cfg| {
                if (std.mem.eql(u8, cfg.name, name)) {
                    return try index_manager_mod.ResolverConfig.clone(self.alloc, cfg);
                }
            }
            return null;
        }

        fn retireResolverArtifactsLocked(self: *DB, cfg: index_manager_mod.ResolverConfig) !?u64 {
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (deletes.items) |key| self.alloc.free(@constCast(key));
                deletes.deinit(self.alloc);
            }
            var changed = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (changed.items) |key| self.alloc.free(key);
                changed.deinit(self.alloc);
            }

            const marker_prefix = try internal_keys.assetArtifactSourceIndexPrefixAlloc(self.alloc, cfg.source_artifact);
            defer self.alloc.free(marker_prefix);
            const markers = try self.core.store.scanPrefix(self.alloc, marker_prefix);
            defer docstore_mod.DocStore.freeResults(self.alloc, markers);

            for (markers) |marker| {
                const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(self.alloc, marker.value)) orelse continue;
                defer {
                    self.alloc.free(parsed.doc_key);
                    self.alloc.free(parsed.artifact_name);
                }
                if (!std.mem.eql(u8, parsed.artifact_name, cfg.source_artifact)) continue;

                const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(self.alloc, parsed.doc_key, cfg.resolution_artifact);
                var resolution_key_owned = true;
                errdefer if (resolution_key_owned) self.alloc.free(resolution_key);
                if (!db_internal.containsKey(deletes.items, resolution_key)) {
                    try deletes.append(self.alloc, resolution_key);
                    resolution_key_owned = false;
                    try db_internal.appendUniqueOwnedKey(self.alloc, &changed, resolution_key);
                } else {
                    self.alloc.free(resolution_key);
                    resolution_key_owned = false;
                }

                try Self.collectResolverMentionArtifactsForDocLocked(self, cfg, parsed.doc_key, &deletes, &changed);
            }

            if (deletes.items.len == 0 and changed.items.len == 0) return null;

            const sequence = self.core.reserveDerivedAppendSequence();
            const replay_payload = try DB.WritePathCallbacks.encode_change_record_payload(self, .{
                .sequence = sequence,
                .changed_artifact_keys = changed.items,
            }, sequence);
            defer self.alloc.free(replay_payload);

            try self.core.store.putBatchWithReplay(self.backend_runtime.io(), &.{}, deletes.items, .{
                .sequence = sequence,
                .payload = replay_payload,
            });
            DB.WritePathCallbacks.mirror_ha_replay_payload_best_effort(self, replay_payload);
            self.executor.trackBacklogBytes(sequence, @intCast(replay_payload.len)) catch {};
            return sequence;
        }

        fn collectResolverMentionArtifactsForDocLocked(
            self: *DB,
            cfg: index_manager_mod.ResolverConfig,
            doc_key: []const u8,
            deletes: *std.ArrayListUnmanaged([]const u8),
            changed: *std.ArrayListUnmanaged([]u8),
        ) !void {
            for (self.core.graphIndexes()) |graph_entry| {
                const source = graph_entry.artifact_source orelse continue;
                if (source.mention_edge_type.len == 0) continue;
                if (!std.mem.eql(u8, source.artifact_name, cfg.source_artifact)) continue;

                const state_name = try db_internal.mentionGraphStateNameAlloc(self.alloc, cfg.source_artifact, cfg.resolution_artifact);
                defer self.alloc.free(state_name);
                const state_key = try db_internal.graphAssetStateKeyAlloc(self.alloc, doc_key, graph_entry.config.name, state_name);
                var state_key_owned = true;
                errdefer if (state_key_owned) self.alloc.free(state_key);

                if (try db_internal.loadGraphAssetStateKeysAlloc(self.alloc, self.core.store, state_key)) |previous_keys| {
                    defer db_internal.freeOwnedConstKeySlice(self.alloc, previous_keys);
                    for (previous_keys) |previous_key| {
                        if (db_internal.containsKey(deletes.items, previous_key)) continue;
                        const owned_key = try self.alloc.dupe(u8, previous_key);
                        try deletes.append(self.alloc, owned_key);
                        try db_internal.appendUniqueOwnedKey(self.alloc, changed, previous_key);
                    }
                }

                if (!db_internal.containsKey(deletes.items, state_key)) {
                    try deletes.append(self.alloc, state_key);
                    state_key_owned = false;
                } else {
                    self.alloc.free(state_key);
                    state_key_owned = false;
                }
            }
        }

        fn retireResolverReplayBeforeCatalogRemoval(self: *DB) !void {
            try Self.runUntilIdle(self);
            const resolution_stats = Self.resolutionStageStats(self);
            if (resolution_stats.catch_up_required or resolution_stats.blocked) return error.ResolverReplayPending;
            const promotion_stats = Self.promotionStageStats(self);
            if (promotion_stats.catch_up_required or promotion_stats.blocked) return error.ResolverReplayPending;
        }

        /// The review queue: review-band mentions awaiting human curation. Empty
        /// when no resolution runtime is active. Caller owns the result
        /// (`resolution_runtime_mod.freePendingReviews`).
        pub fn listPendingReviews(self: *DB, alloc: Allocator) ![]resolution_runtime_mod.PendingReview {
            const runtime = self.resolution_runtime orelse return try alloc.alloc(resolution_runtime_mod.PendingReview, 0);
            return try runtime.pendingReviews(alloc);
        }

        /// Persist a human curation decision for one review-band mention and enqueue
        /// the document for ordinary replay-driven resolution/promotion.
        pub fn recordReviewDecisionAfterGate(
            self: *DB,
            doc_key: []const u8,
            source_artifact: []const u8,
            resolution_artifact: []const u8,
            local_id: []const u8,
            decision: resolver_lib.Decision,
            table: []const u8,
            key: []const u8,
        ) !u64 {
            try self.executor.failIfUnhealthy();

            self.core.lockApply();
            var apply_mutex_held = true;
            errdefer if (apply_mutex_held) self.core.unlockApply();

            const resolvers = try self.core.listResolvers(self.alloc);
            defer {
                for (resolvers) |*cfg| cfg.deinit(self.alloc);
                if (resolvers.len > 0) self.alloc.free(resolvers);
            }
            for (resolvers) |cfg| {
                if (std.mem.eql(u8, cfg.source_artifact, source_artifact) and
                    std.mem.eql(u8, cfg.resolution_artifact, resolution_artifact))
                {
                    break;
                }
            } else return error.ResolverNotFound;

            const override_key = try resolution_runtime_mod.reviewOverrideArtifactKeyAlloc(self.alloc, doc_key, source_artifact, resolution_artifact);
            defer self.alloc.free(override_key);
            const existing = self.core.store.get(self.alloc, override_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (existing) |raw| self.alloc.free(raw);
            const override_bytes = try resolution_runtime_mod.buildReviewDecisionBytesAlloc(self.alloc, existing, local_id, decision, table, key);
            defer self.alloc.free(override_bytes);

            const source_key = try internal_keys.artifactNamedPrefixAlloc(self.alloc, doc_key, "asset", source_artifact);
            defer self.alloc.free(source_key);
            const sequence = self.core.reserveDerivedAppendSequence();
            const changed_artifact_keys = [_][]const u8{source_key};
            const replay_payload = try DB.WritePathCallbacks.encode_change_record_payload(self, .{
                .sequence = sequence,
                .changed_artifact_keys = changed_artifact_keys[0..],
            }, sequence);
            defer self.alloc.free(replay_payload);

            const writes = [_]docstore_mod.KVPair{.{
                .key = override_key,
                .value = override_bytes,
            }};
            try self.core.store.putBatchWithReplay(self.backend_runtime.io(), writes[0..], &.{}, .{
                .sequence = sequence,
                .payload = replay_payload,
            });
            DB.WritePathCallbacks.mirror_ha_replay_payload_best_effort(self, replay_payload);
            self.executor.trackBacklogBytes(sequence, @intCast(replay_payload.len)) catch {};
            self.core.unlockApply();
            apply_mutex_held = false;

            if (self.executor.hasWorkers()) {
                self.executor.forceSequence(sequence);
            } else {
                self.executor.notifySequence(sequence);
            }
            Self.notifyResolverReplayRuntimes(self, sequence);
            return sequence;
        }

        fn backfillResolverCorpus(self: *DB) !void {
            if (!Self.hasConfiguredResolvers(self)) return;
            if (self.resolution_runtime) |runtime| {
                try runtime.requestReresolveBacklog();
                while (true) {
                    var tick = try runtime.runReresolveBacklogWindow();
                    const queued = tick.queued;
                    const complete = tick.complete;
                    tick.deinit(self.runtime_alloc);
                    // Drive the enqueued extraction artifacts through resolution,
                    // promotion, and graph materialization before queuing more.
                    if (queued > 0) try Self.runUntilIdle(self);
                    if (complete) break;
                }
            }
        }

        pub fn deinitWrapperState(self: *DB, executor_ready: bool) void {
            // Close may flush/coalesce derived watermarks while workers are
            // stopping. That must not call back into the write/status cache after
            // optional runtimes or index state have started tearing down.
            self.stopQuarantineRetryWorker();
            self.stopArtifactRepairMetadataWorker();
            Self.setQueryVisibilityHook(self, null);
            self.async_context.background_closing.store(true, .release);
            self.async_context.enrichment_desired_running.store(false, .release);
            self.backend_runtime.durable_jobs.closeOwner(self.repair_cleanup_owner_id);
            self.backend_runtime.durable_jobs.closeOwner(self.backend_owner_id);
            self.backend_runtime.durable_jobs.closeOwner(self.relational_index_worker_owner_id);
            if (self.algebraic_hll_owner_id != 0) {
                self.backend_runtime.durable_jobs.closeOwner(self.algebraic_hll_owner_id);
            }
            self.clearLiveDocSetCache();
            self.clearNonVisibleDocSetCache();
            DB.LifecycleCallbacks.deinit_bulk_ingest_coalescer(self);
            DB.LifecycleCallbacks.clear_bulk_ingest_identity_all_new_locked(self);
            self.bulk_ingest_seen_doc_keys.deinit(self.alloc);
            self.clearActiveIndexRepairsLocked();
            self.active_index_repairs.deinit(self.alloc);
            self.closeShadowIndexManager() catch {};
            if (self.transaction_runtime) |runtime| {
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            if (self.transaction_recovery_identity_context) |ctx| self.runtime_alloc.destroy(ctx);
            if (self.ttl_runtime) |runtime| {
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            if (self.ttl_cleanup_context) |ctx| self.runtime_alloc.destroy(ctx);
            if (self.enrichment_runtime) |runtime| {
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            if (self.enrichment_append_context) |ctx| self.runtime_alloc.destroy(ctx);
            if (self.resolution_runtime) |runtime| {
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            self.async_context.resolution_runtime = null;
            // After the resolution runtime (its final catch-up may journal
            // resolution artifacts that notify the promoter) but before the
            // resolution append context the promoter is wired into is destroyed.
            if (self.promotion_runtime) |runtime| {
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            self.async_context.promotion_runtime = null;
            if (self.resolution_append_context) |ctx| self.runtime_alloc.destroy(ctx);
            if (executor_ready) self.executor.deinit(self.runtime_alloc);
            self.runtime_alloc.destroy(self.executor);
            if (self.text_merge_runtime) |runtime| {
                self.async_context.text_merge_runtime = null;
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            if (self.sparse_compaction_runtime) |runtime| {
                self.async_context.sparse_compaction_runtime = null;
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            if (self.graph_metric_runtime) |runtime| {
                runtime.deinit();
                self.runtime_alloc.destroy(runtime);
            }
            self.core.deinit();
            if (self.owned_backend_runtime) |*runtime| runtime.deinit();
            self.async_context.deinit(self.runtime_alloc);
            self.runtime_alloc.destroy(self.async_context);
            if (self.owned_resource_manager) |manager| {
                manager.deinit(self.alloc);
                self.alloc.destroy(manager);
            }
            if (self.generation_read_lease) |*lease| lease.deinit();
            self.generation_read_lease = null;
            self.* = undefined;
            self.closed = true;
        }

        pub fn refreshManagedIndexWorkersLocked(self: *DB) !void {
            if (!self.start_index_workers) return;

            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            for (managed_indexes) |index_ref| {
                self.executor.removeWorker(index_ref.name);
            }
            for (managed_indexes) |index_ref| {
                const applied = try self.core.loadAppliedSequence(self.alloc, index_ref.name);
                try self.executor.addWorker(index_ref.name, index_ref, applied);
            }
        }

        pub fn resetManagedIndexAppliedSequences(self: *DB) !void {
            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            for (managed_indexes) |index_ref| {
                try self.core.saveAppliedSequence(index_ref.name, 0);
            }
        }

        pub fn rebaseManagedIndexAppliedSequencesIfNeeded(self: *DB) !void {
            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            var max_applied: u64 = 0;
            for (managed_indexes) |index_ref| {
                const applied = try self.core.loadAppliedSequence(self.alloc, index_ref.name);
                max_applied = @max(max_applied, applied);
            }
            if (max_applied == 0) return;

            // Applied watermarks are independent of the retained replay log.
            // Once replay entries are truncated, a valid applied watermark can
            // be above the current append floor recovered from replay metadata.
            // Preserve the watermark and advance the floor so future replay
            // sequence allocation cannot reuse already-applied sequence numbers.
            try ensureReplayFloor(self.core.store, max_applied + 1);
        }

        pub fn pendingWorkStats(self: *DB) db_core.PendingWorkStats {
            return .{
                .derived_target_sequence = self.core.nextDerivedSequence(),
                .has_async_indexes = self.executor.hasWorkers(),
                .enrichment = Self.enrichmentStatsWithSupervisorState(self, .{}),
                .resolution = Self.resolutionStageStats(self),
                .promotion = Self.promotionStageStats(self),
                .text_merge = if (self.text_merge_runtime) |runtime| runtime.statsAssumeApplyLockHeld() else self.core.index_manager.textMergeStats(),
                .repair_metadata_rebuild_pending = self.artifactRepairMetadataRebuildPending(),
                .graph_metric = self.core.index_manager.graphMetricPlannedWorkStats() catch .{},
            };
        }

        pub fn persistedReplayStageStats(self: *DB, scope_name: []const u8, force_enabled: bool) !types.ReplayStageStats {
            const resources = self.core.batchExecutionResources();
            const applied = try enrichment_state.loadAppliedSequence(self.alloc, resources.store, scope_name);
            const target = @max(applied, self.core.nextDerivedSequence());
            return .{
                .enabled = force_enabled or target > 0 or applied < target,
                .target_sequence = target,
                .applied_sequence = applied,
                .catch_up_required = applied < target,
            };
        }

        pub fn resolutionStageStats(self: *DB) types.ReplayStageStats {
            if (!Self.hasConfiguredResolvers(self)) return .{};
            if (self.resolution_runtime) |runtime| return runtime.stats();
            return Self.persistedReplayStageStats(self, resolution_runtime_mod.scope_name, false) catch .{};
        }

        pub fn promotionStageStats(self: *DB) types.ReplayStageStats {
            if (!Self.hasConfiguredResolvers(self)) return .{};
            if (self.promotion_runtime) |runtime| return runtime.stats();
            return Self.persistedReplayStageStats(self, promotion_runtime_mod.scope_name, false) catch .{};
        }

        fn resolverReplayDiagnosticsLocked(self: *DB, alloc: Allocator) !types.ResolverReplayDiagnostics {
            const catalog = self.core.index_manager.resolvers.items;
            var resolvers = try alloc.alloc(types.ResolverReplayDiagnostic, catalog.len);
            var initialized: usize = 0;
            errdefer {
                for (resolvers[0..initialized]) |resolver| {
                    alloc.free(resolver.name);
                    alloc.free(resolver.table);
                    alloc.free(resolver.source_artifact);
                    alloc.free(resolver.resolution_artifact);
                }
                if (resolvers.len > 0) alloc.free(resolvers);
            }

            for (catalog) |cfg| {
                const name = try alloc.dupe(u8, cfg.name);
                errdefer alloc.free(name);
                const table = try alloc.dupe(u8, cfg.table);
                errdefer alloc.free(table);
                const source_artifact = try alloc.dupe(u8, cfg.source_artifact);
                errdefer alloc.free(source_artifact);
                const resolution_artifact = try alloc.dupe(u8, cfg.resolution_artifact);
                errdefer alloc.free(resolution_artifact);
                resolvers[initialized] = .{
                    .name = name,
                    .table = table,
                    .source_artifact = source_artifact,
                    .resolution_artifact = resolution_artifact,
                };
                initialized += 1;
            }

            return .{
                .resolver_count = catalog.len,
                .resolution_runtime_present = self.resolution_runtime != null,
                .resolution_worker_started = if (self.resolution_runtime) |runtime| runtime.worker_started.load(.acquire) else false,
                .promotion_runtime_present = self.promotion_runtime != null,
                .promotion_worker_started = if (self.promotion_runtime) |runtime| runtime.worker_started.load(.acquire) else false,
                .resolvers = resolvers,
            };
        }

        pub fn persistedEnrichmentStats(self: *DB) !types.EnrichmentStats {
            if (!self.core.hasGeneratedEnrichmentTargets()) return .{};
            const resources = self.core.batchExecutionResources();
            const applied = try enrichment_state.loadAppliedSequence(self.alloc, resources.store, enrichment_runtime_mod.scope_name);
            const status = try enrichment_state.loadRuntimeStatus(self.alloc, resources.store, enrichment_runtime_mod.scope_name);
            const target = @max(applied, status.target_sequence);
            return .{
                .enabled = target > 0 or status.error_count > 0 or status.retrying or status.worker_failed,
                .target_sequence = target,
                .applied_sequence = applied,
                .error_count = status.error_count,
                .retryable_error_count = status.retryable_error_count,
                .fatal_error_count = status.fatal_error_count,
                .consecutive_retry_count = status.consecutive_retry_count,
                .next_retry_at_ms = status.next_retry_at_ms,
                .retrying = status.retrying,
                .worker_failed = status.worker_failed,
                .skipped_source_count = status.skipped_source_count,
            };
        }

        const ReplayDrainOptions = struct {
            truncate_replay: bool = true,
            wait_for_enrichment_retries: bool = false,
        };

        fn runDerivedUntilWithOptions(
            self: *DB,
            sequence: u64,
            options: ReplayDrainOptions,
        ) !void {
            if (sequence == 0) return;
            if (!self.executor.hasWorkers()) {
                try DB.LifecycleCallbacks.replay_pending_derived_batches_with_options(
                    self,
                    null,
                    null,
                    options.truncate_replay,
                );
                return;
            }
            self.executor.notifySequence(sequence);
            try self.executor.waitForAll(sequence);
        }

        pub fn runDerivedUntil(self: *DB, sequence: u64) !void {
            try Self.runDerivedUntilWithOptions(self, sequence, .{});
        }

        pub fn runDerivedUntilTargets(self: *DB, sequence: u64, index_names: []const []const u8) !void {
            if (sequence == 0 or index_names.len == 0) return;
            if (!self.executor.hasWorkers()) return;
            self.executor.notifyIndexes(sequence, index_names);
            try self.executor.waitForIndexes(sequence, index_names);
        }

        pub fn runEnrichmentUntil(self: *DB, sequence: u64) !void {
            if (sequence == 0) return;
            if (self.enrichment_runtime) |runtime| {
                if (!self.optional_runtime_workers_enabled) {
                    try runtime.catchUpUntil(sequence);
                    return;
                }
                runtime.notifySequence(sequence);
                try runtime.waitForApplied(sequence);
            }
        }

        fn runEnrichmentUntilForDrain(self: *DB, sequence: u64, options: ReplayDrainOptions) !void {
            while (true) {
                self.runEnrichmentUntil(sequence) catch |err| switch (err) {
                    error.EnrichmentRetryInProgress => {
                        if (!options.wait_for_enrichment_retries) return err;
                        db_internal.sleepNs(25 * std.time.ns_per_ms);
                        continue;
                    },
                    else => return err,
                };
                return;
            }
        }

        pub fn markPrecomputedEnrichmentAppliedForSync(self: *DB, sync_level: types.SyncLevel, sequence: u64) !void {
            if ((sync_level != .enrichments and sync_level != .full_index) or sequence == 0) return;
            const runtime = self.enrichment_runtime orelse return;
            const runtime_stats = runtime.stats();
            if (runtime_stats.applied_sequence >= sequence -| 1 or
                try Self.noPendingEnrichmentReplayThrough(self, runtime_stats.applied_sequence, sequence))
            {
                try runtime.markAppliedThrough(sequence);
            }
        }

        pub fn noPendingEnrichmentReplayThrough(self: *DB, applied_sequence: u64, sequence: u64) !bool {
            const pending = try enrichment_worker.collectPendingDocumentGroups(self.alloc, self.core.replaySource(), applied_sequence);
            defer enrichment_worker.freePendingDocumentGroups(self.alloc, pending);
            for (pending) |group| {
                if (group.sequence <= sequence) return false;
            }
            return true;
        }

        fn runMaintenanceUntilWithOptions(
            self: *DB,
            sequence: u64,
            sync_targets: ManagedSyncTargets,
            options: ReplayDrainOptions,
        ) !void {
            var stable_target = sequence;
            while (true) {
                try Self.runDerivedUntilWithOptions(
                    self,
                    stable_target,
                    options,
                );
                try Self.runEnrichmentUntilForDrain(self, stable_target, options);

                const next_target = self.core.nextDerivedSequence();
                if (next_target <= stable_target) {
                    _ = try self.runArtifactRepairMetadataMaintenancePass();
                    if (Self.syncTargetsIncludeGraph(self, sync_targets.all_indexes)) _ = try self.runGraphMetricMaintenanceForIdle();
                    return;
                }
                stable_target = next_target;
            }
        }

        pub fn runMaintenanceUntil(self: *DB, sequence: u64, sync_targets: ManagedSyncTargets) !void {
            try Self.runMaintenanceUntilWithOptions(
                self,
                sequence,
                sync_targets,
                .{},
            );
        }

        pub fn runMaintenanceUntilTargets(self: *DB, sequence: u64, index_names: []const []const u8) !void {
            if (index_names.len == 0) return;
            var stable_target = sequence;
            while (true) {
                try Self.runDerivedUntilTargets(self, stable_target, index_names);
                try Self.runEnrichmentUntil(self, stable_target);

                const next_target = self.core.nextDerivedSequence();
                if (next_target <= stable_target) {
                    if (Self.syncTargetsIncludeGraph(self, index_names)) _ = try self.runGraphMetricMaintenanceForIdle();
                    return;
                }
                stable_target = next_target;
            }
        }

        pub fn syncTargetsIncludeGraph(self: *DB, index_names: []const []const u8) bool {
            for (index_names) |index_name| {
                if (self.core.graphIndex(index_name) != null) return true;
            }
            return false;
        }

        pub fn waitForCurrentSyncLevel(self: *DB, sync_level: types.SyncLevel) !void {
            try self.executor.failIfUnhealthy();
            const sequence = self.core.nextDerivedSequence();
            try Self.markPrecomputedEnrichmentAppliedForSync(self, sync_level, sequence);
            var sync_targets = try Self.currentManagedSyncTargets(self, sync_level);
            defer sync_targets.deinit(self.alloc);
            if (self.executor.hasWorkers()) {
                db_internal.notifyExecutorForSyncLevelWithDenseBulkDeferral(self.async_context, self.executor, sync_level, sequence, sync_targets);
            }
            try DB.LifecycleCallbacks.wait_for_sync_level(self, sync_level, sequence, sync_targets);
        }

        pub fn waitForResolvedTransactionSync(self: *DB, sync_level: types.SyncLevel, sequence: u64) !void {
            if (sequence == 0 or sync_level == .propose or sync_level == .write) {
                try self.executor.failIfUnhealthy();
                return;
            }
            try self.executor.failIfUnhealthy();
            try Self.markPrecomputedEnrichmentAppliedForSync(self, sync_level, sequence);
            var sync_targets = try Self.currentManagedSyncTargets(self, sync_level);
            defer sync_targets.deinit(self.alloc);
            if (self.executor.hasWorkers()) {
                db_internal.notifyExecutorForSyncLevelWithDenseBulkDeferral(self.async_context, self.executor, sync_level, sequence, sync_targets);
            }
            if (self.enrichment_runtime) |runtime| runtime.notifySequence(sequence);
            Self.notifyResolverReplayRuntimes(self, sequence);
            try DB.LifecycleCallbacks.wait_for_sync_level(self, sync_level, sequence, sync_targets);
            if (sync_level == .full_index and self.text_merge_runtime == null) {
                try Self.drainScheduledTextMerges(self);
            }
        }

        pub fn currentManagedSyncTargets(self: *DB, sync_level: types.SyncLevel) !ManagedSyncTargets {
            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            var full_text_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (full_text_indexes.items) |name| self.alloc.free(@constCast(name));
                full_text_indexes.deinit(self.alloc);
            }
            var all_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (all_indexes.items) |name| self.alloc.free(@constCast(name));
                all_indexes.deinit(self.alloc);
            }

            for (managed_indexes) |index_ref| {
                switch (sync_level) {
                    .propose, .write => {},
                    .enrichments => {},
                    .full_text => {
                        if (index_ref.kind == .full_text) {
                            try appendUniqueOwnedName(self.alloc, &full_text_indexes, index_ref.name);
                            try appendUniqueOwnedName(self.alloc, &all_indexes, index_ref.name);
                        }
                    },
                    .full_index => {
                        try appendUniqueOwnedName(self.alloc, &all_indexes, index_ref.name);
                        if (index_ref.kind == .full_text) {
                            try appendUniqueOwnedName(self.alloc, &full_text_indexes, index_ref.name);
                        }
                    },
                }
            }

            var targets = ManagedSyncTargets{
                .full_text_indexes = try full_text_indexes.toOwnedSlice(self.alloc),
                .all_indexes = try all_indexes.toOwnedSlice(self.alloc),
            };
            errdefer targets.deinit(self.alloc);
            try derived_async_mod.filterManagedSyncTargetsForRelationalDerivedMaintenance(self.alloc, self.core.schema, &targets);
            return targets;
        }

        pub fn runUntilIdle(self: *DB) !void {
            try Self.drainReplayStagesUntilStable(self);
            _ = try self.evaluateAlgebraicAdaptiveCandidates();
            while (try self.runAlgebraicAdaptiveWork() != 0) {}
            try DB.LifecycleCallbacks.flush_applied_sequences_for_idle(self);
            try Self.drainScheduledTextMerges(self);
            try self.runArtifactRepairMetadataMaintenanceUntilIdle();
            _ = try self.runDensePostingMaintenanceForIdle();
            _ = try self.runGraphMetricMaintenanceForIdle();
            _ = try Self.runLsmMaintenanceUntilIdle(self);
        }

        pub fn compactTextIndexes(self: *DB) !void {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.compactTextIndexes();
        }

        pub fn drainScheduledTextMerges(self: *DB) !void {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.drainScheduledTextMerges();
        }

        pub fn forceCompactTextIndexes(self: *DB) !void {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.forceCompactTextIndexes();
        }

        pub fn bestEffortForceCompactTextIndexes(self: *DB) !void {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.bestEffortForceCompactTextIndexes();
        }

        fn currentMaintenanceTargetSequence(self: *DB) u64 {
            var target_sequence = self.core.nextDerivedSequence();
            if (self.enrichment_runtime) |runtime| {
                target_sequence = @max(target_sequence, runtime.stats().target_sequence);
            }
            return target_sequence;
        }

        pub fn prepareSnapshot(self: *DB) !void {
            try Self.runMaintenanceUntil(self, Self.currentMaintenanceTargetSequence(self), .{});
        }

        fn resolverReplayBlockedAfterRunnableDrain(self: *DB) bool {
            const resolution_stats = Self.resolutionStageStats(self);
            if (resolution_stats.catch_up_required and !resolution_stats.blocked) return false;
            const promotion_stats = Self.promotionStageStats(self);
            return promotion_stats.blocked;
        }

        fn drainReplayStagesUntilStableWithOptions(
            self: *DB,
            options: ReplayDrainOptions,
        ) !void {
            var rounds: usize = 0;
            while (rounds < run_until_idle_max_replay_rounds) : (rounds += 1) {
                const starting_sequence = self.core.nextDerivedSequence();
                const starting_resolution_applied = if (self.resolution_runtime) |runtime| runtime.stats().applied_sequence else 0;
                const starting_promotion_applied = if (self.promotion_runtime) |runtime| runtime.stats().applied_sequence else 0;
                try Self.runMaintenanceUntilWithOptions(
                    self,
                    Self.currentMaintenanceTargetSequence(self),
                    .{},
                    options,
                );
                const drained_sequence = self.core.nextDerivedSequence();

                const drain_resolver_replay = Self.resolverReplayNeedsDrain(self);
                if (drain_resolver_replay) {
                    if (self.resolution_runtime) |runtime| {
                        if (drained_sequence > 0) runtime.notifySequence(drained_sequence - 1);
                        try runtime.catchUp();
                    }

                    if (self.promotion_runtime) |runtime| {
                        const latest = self.core.nextDerivedSequence();
                        if (latest > 0) runtime.notifySequence(latest - 1);
                        try runtime.catchUp();
                    }

                    if (Self.resolverReplayBlockedAfterRunnableDrain(self)) return;

                    try Self.runMaintenanceUntilWithOptions(
                        self,
                        Self.currentMaintenanceTargetSequence(self),
                        .{},
                        options,
                    );
                    if (Self.resolverReplayBlockedAfterRunnableDrain(self)) return;
                }

                const resolution_advanced = if (self.resolution_runtime) |runtime|
                    runtime.stats().applied_sequence > starting_resolution_applied
                else
                    false;
                const promotion_advanced = if (self.promotion_runtime) |runtime|
                    runtime.stats().applied_sequence > starting_promotion_applied
                else
                    false;
                if (self.core.nextDerivedSequence() <= starting_sequence and !resolution_advanced and !promotion_advanced) return;
            }
            return error.RunUntilIdleDidNotConverge;
        }

        fn drainReplayStagesUntilStable(self: *DB) !void {
            try Self.drainReplayStagesUntilStableWithOptions(self, .{ .wait_for_enrichment_retries = true });
        }

        pub fn drainReplayStagesUntilStableWithoutTruncation(self: *DB) !void {
            try Self.drainReplayStagesUntilStableWithOptions(self, .{
                .truncate_replay = false,
                .wait_for_enrichment_retries = true,
            });
        }

        pub fn snapshotForeignKeyStats(self: *DB) types.ForeignKeyStats {
            return self.foreign_key_stats.snapshot();
        }

        pub fn snapshotRelationalIndexRepairStatsBestEffort(self: *DB) types.RelationalIndexRepairStats {
            return self.snapshotRelationalIndexRepairStats() catch .{};
        }

        pub fn snapshotRelationalIndexRepairStatsLockedBestEffort(self: *DB) types.RelationalIndexRepairStats {
            return self.snapshotRelationalIndexRepairStatsAssumeApplyLockHeld() catch .{};
        }

        pub fn reassignIdentityNamespaceForInternalTransition(self: *DB, namespace: doc_identity.Namespace) !void {
            if (self.open_mode == .status_only) return error.UnsupportedOperation;
            self.core.lockApply();
            defer self.core.unlockApply();
            try doc_identity.reassignNamespaceAlloc(self.alloc, self.core.store, namespace);
            self.core.identity_namespace = namespace;
            if (self.transaction_recovery_identity_context) |ctx| {
                ctx.identity_namespace = namespace;
                ctx.relational_base_rows = self.relationalColumnsForStore() != null;
                ctx.relational_columns = self.relationalColumnsForStore() orelse &.{};
                ctx.relational_indexes = self.relationalIndexesForStore();
            }
        }

        pub fn currentIdentityReadGenerationForRequest(self: *DB, requested: ?u64) !u64 {
            const current_generation = try Self.currentIdentityReadGeneration(self);
            if (requested) |generation| {
                if (generation != current_generation) {
                    self.doc_set_planning_stats.recordStaleIdentityGenerationRejection();
                    return error.IdentityReadGenerationChanged;
                }
                return generation;
            }
            return current_generation;
        }

        fn currentIdentityReadGeneration(self: *DB) !u64 {
            var current_generation = self.core.nextDerivedSequence();
            if (self.identity_visibility_summary_cache) |summary| {
                return @max(current_generation, doc_identity.latestGenerationFromSummary(summary));
            }
            if (try doc_identity.latestGenerationFromSummaryFast(self.core.store)) |identity_generation| {
                return @max(current_generation, identity_generation);
            }
            const identity_stats = try doc_identity.fullStatsFromStore(self.core.store);
            current_generation = @max(current_generation, identity_stats.max_created_generation);
            current_generation = @max(current_generation, identity_stats.max_deleted_generation);
            return current_generation;
        }

        pub fn dbDocIdentityStats(raw: doc_identity.Stats, namespace: doc_identity.Namespace) types.DocIdentityStats {
            const max_doc_ordinal = std.math.maxInt(doc_identity.DocOrdinal);
            const remaining = if (raw.next_ordinal >= max_doc_ordinal)
                0
            else
                @as(u64, max_doc_ordinal) - raw.next_ordinal;
            return .{
                .namespace_table_id = namespace.table_id,
                .namespace_shard_id = namespace.shard_id,
                .namespace_range_id = namespace.range_id,
                .next_ordinal = raw.next_ordinal,
                .allocated_ordinals = raw.allocated_ordinals,
                .ordinal_capacity_remaining = remaining,
                .ordinal_capacity_exhausted = raw.next_ordinal >= max_doc_ordinal,
                .rebuild_required = raw.next_ordinal >= max_doc_ordinal,
                .state_rows = raw.state_rows,
                .live_ordinals = raw.live_ordinals,
                .tombstone_ordinals = raw.tombstone_ordinals,
                .visibility_chunk_size = doc_identity.visibility_chunk_size,
                .visibility_chunks = raw.visibility_chunks,
                .visibility_deleted_ordinals = raw.visibility_deleted_ordinals,
                .visibility_mask_bytes = raw.visibility_mask_bytes,
                .visibility_repair_count = raw.visibility_repair_count,
                .min_created_generation = raw.min_created_generation,
                .max_created_generation = raw.max_created_generation,
                .min_deleted_generation = raw.min_deleted_generation,
                .max_deleted_generation = raw.max_deleted_generation,
                .complete = raw.complete,
            };
        }

        pub fn diagnosticDocIdentityStats(self: *DB, byte_range: types.ByteRange) !types.DocIdentityStats {
            var identity_stats = Self.dbDocIdentityStats(try doc_identity.fullStatsFromStore(self.core.store), self.core.identity_namespace);
            const coverage = try Self.scanPrimaryDocIdentityCoverage(self, byte_range);
            identity_stats.scanned_primary_docs = coverage.scanned_primary_docs;
            identity_stats.primary_docs_missing_ordinals = coverage.primary_docs_missing_ordinals;
            identity_stats.primary_docs_missing_identity_state = coverage.primary_docs_missing_identity_state;
            identity_stats.primary_docs_with_tombstone_ordinals = coverage.primary_docs_with_tombstone_ordinals;
            finalizeDocIdentityRebuildRequired(&identity_stats);
            return identity_stats;
        }

        pub fn snapshotDocSetPlanningStats(self: *DB) types.DocSetPlanningStats {
            return self.doc_set_planning_stats.snapshot();
        }

        pub fn snapshotVisibilityStats(self: *DB) types.VisibilityStats {
            return self.visibility_runtime_stats.snapshot(self.nonvisible_doc_set_cache_entries.load(.monotonic));
        }

        pub fn scanPrimaryDocCount(self: *DB, byte_range: types.ByteRange) !u64 {
            const lower = try self.core.documentRangeLowerAlloc(byte_range.start);
            defer self.core.alloc.free(lower);
            const upper = if (byte_range.end.len > 0) try self.core.documentRangeUpperAlloc(byte_range.end) else null;
            defer if (upper) |buf| self.core.alloc.free(buf);

            var doc_count: u64 = 0;
            const CountState = struct {
                relational_base_rows: bool,
                doc_count: *u64,

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    _ = value;
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    if (db_internal.isBaseDocumentStoreKeyForMode(state.relational_base_rows, key)) state.doc_count.* += 1;
                    return .@"continue";
                }
            };

            var state = CountState{
                .relational_base_rows = self.relationalColumnsForStore() != null,
                .doc_count = &doc_count,
            };
            try self.core.store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &state, CountState.scanEntry);
            return doc_count;
        }

        pub fn primaryDocCount(self: *DB) !u64 {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try Self.scanPrimaryDocCount(self, self.core.byteRange());
        }

        pub fn initializeDerivedCoverageIdentity(cfg: types.IndexConfig, item: *types.DBIndexStats) void {
            if (cfg.kind != .dense_vector and cfg.kind != .sparse_vector) return;
            item.coverage_generation = internal_keys.derivedCoverageGenerationForConfig(cfg.coverage_generation, cfg.config_json);
            if (cfg.coverage_config_fingerprint) |fingerprint| {
                item.coverage_config_hash = fingerprint;
                item.coverage_identity_ready = true;
            } else {
                // Catalog admission should make this state unreachable. Keep status
                // available and expose the missing identity as degraded coverage.
                item.coverage_summary_ready = false;
                item.repair_degraded = true;
            }
        }

        fn hydrateDerivedCoverageIdentities(self: *DB, alloc: Allocator, items: []types.DBIndexStats) !void {
            var needs_hydration = false;
            for (items) |item| {
                if ((item.kind == .dense_vector or item.kind == .sparse_vector) and !item.coverage_identity_ready) {
                    needs_hydration = true;
                    break;
                }
            }
            if (!needs_hydration) return;

            var identities = try self.core.index_manager.coverageIdentityMapAlloc(alloc);
            defer identities.deinit(alloc);
            for (items) |*item| {
                if (item.kind != .dense_vector and item.kind != .sparse_vector) continue;
                const identity = identities.get(item.name) orelse {
                    markMissingDerivedCoverageIdentity(item);
                    continue;
                };
                item.coverage_generation = identity.generation;
                if (identity.config_fingerprint) |fingerprint| {
                    item.coverage_config_hash = fingerprint;
                    item.coverage_identity_ready = true;
                } else {
                    markMissingDerivedCoverageIdentity(item);
                }
            }
        }

        fn hydrateDerivedCoverageIdentitiesBestEffort(self: *DB, alloc: Allocator, items: []types.DBIndexStats) void {
            self.hydrateDerivedCoverageIdentities(alloc, items) catch markMissingDerivedCoverageIdentities(items);
        }

        fn markMissingDerivedCoverageIdentities(items: []types.DBIndexStats) void {
            for (items) |*item| {
                if (item.kind == .dense_vector or item.kind == .sparse_vector) markMissingDerivedCoverageIdentity(item);
            }
        }

        fn markMissingDerivedCoverageIdentity(item: *types.DBIndexStats) void {
            item.coverage_identity_ready = false;
            item.coverage_summary_ready = false;
            item.repair_degraded = true;
        }

        fn populateDerivedCoverageCounts(self: *DB, index_name: []const u8, generation: u64, config_hash: u64, item: *types.DBIndexStats) !void {
            item.coverage_config_hash = config_hash;
            const produced = try loadDerivedCoverageOutcomeCounterFromStore(self.core.alloc, self.core.store, index_name, generation, "produced");
            const skipped = try loadDerivedCoverageOutcomeCounterFromStore(self.core.alloc, self.core.store, index_name, generation, "skipped");
            const terminal_failed = try loadDerivedCoverageOutcomeCounterFromStore(self.core.alloc, self.core.store, index_name, generation, "terminal_failed");

            const present_count: u2 = @as(u2, @intFromBool(produced != null)) +
                @as(u2, @intFromBool(skipped != null)) +
                @as(u2, @intFromBool(terminal_failed != null));
            // Counter creation and marker mutation share one atomic batch. No
            // counters means a new generation; a partial tuple means corruption or
            // an interrupted legacy write and must not be reported as complete.
            item.coverage_summary_ready = present_count == 0 or present_count == 3;
            item.coverage_produced_count = produced orelse 0;
            item.coverage_skipped_count = skipped orelse 0;
            item.coverage_terminal_failed_count = terminal_failed orelse 0;
            if (!item.coverage_summary_ready) item.repair_degraded = true;
        }

        fn populateConfiguredDerivedCoverageCounts(self: *DB, index_name: []const u8, item: *types.DBIndexStats) !void {
            if (!item.coverage_identity_ready) {
                item.coverage_summary_ready = false;
                item.repair_degraded = true;
                return;
            }
            try Self.populateDerivedCoverageCounts(self, index_name, item.coverage_generation, item.coverage_config_hash, item);
        }

        fn populateConfiguredDerivedCoverageCountsBestEffort(self: *DB, index_name: []const u8, item: *types.DBIndexStats) void {
            Self.populateConfiguredDerivedCoverageCounts(self, index_name, item) catch {
                item.coverage_summary_ready = false;
                item.repair_degraded = true;
            };
        }

        pub fn scanPrimaryDocIdentityCoverage(self: *DB, byte_range: types.ByteRange) !DocIdentityCoverage {
            const lower = try self.core.documentRangeLowerAlloc(byte_range.start);
            defer self.core.alloc.free(lower);
            const upper = if (byte_range.end.len > 0) try self.core.documentRangeUpperAlloc(byte_range.end) else null;
            defer if (upper) |buf| self.core.alloc.free(buf);

            var doc_ids = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (doc_ids.items) |doc_id| self.core.alloc.free(doc_id);
                doc_ids.deinit(self.core.alloc);
            }

            var coverage = DocIdentityCoverage{};
            const ScanState = struct {
                alloc: std.mem.Allocator,
                relational_base_rows: bool,
                doc_ids: *std.ArrayListUnmanaged([]u8),
                coverage: *DocIdentityCoverage,

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    _ = value;
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    if (!db_internal.isBaseDocumentStoreKeyForMode(state.relational_base_rows, key)) return .@"continue";
                    const raw_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(state.alloc, key)) orelse return .@"continue";
                    errdefer state.alloc.free(raw_key);
                    try state.doc_ids.append(state.alloc, raw_key);
                    state.coverage.scanned_primary_docs += 1;
                    return .@"continue";
                }
            };

            var scan_state = ScanState{
                .alloc = self.core.alloc,
                .relational_base_rows = self.relationalColumnsForStore() != null,
                .doc_ids = &doc_ids,
                .coverage = &coverage,
            };
            try self.core.store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &scan_state, ScanState.scanEntry);

            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            for (doc_ids.items) |doc_id| {
                const ordinal = (try doc_identity.lookupOrdinalTxn(self.core.alloc, &txn, doc_id)) orelse {
                    coverage.primary_docs_missing_ordinals += 1;
                    continue;
                };
                const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse {
                    coverage.primary_docs_missing_identity_state += 1;
                    continue;
                };
                if (!state.isLive()) coverage.primary_docs_with_tombstone_ordinals += 1;
            }
            return coverage;
        }

        pub fn overlayRuntimeStatusBestEffort(self: *DB, stats_alloc: std.mem.Allocator, runtime_stats: *types.DBStats) void {
            Self.overlayRuntimeStatusRuntimeOnly(self, runtime_stats);
            Self.overlayRuntimeStatusIndexesAssumeApplyLockHeld(self, stats_alloc, runtime_stats);
        }

        pub fn overlayRuntimeStatusConsistent(self: *DB, stats_alloc: std.mem.Allocator, runtime_stats: *types.DBStats) !void {
            Self.overlayRuntimeStatusRuntimeOnly(self, runtime_stats);
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            try Self.overlayRuntimeStatusIndexesLocked(self, stats_alloc, runtime_stats);
        }

        pub fn stats(self: *DB, alloc: Allocator) !types.DBStats {
            if (self.open_mode == .status_only) {
                return try Self.statusOnlyStats(self, alloc);
            }

            // Operational stats must stay bounded and avoid maintenance/scan work.
            // See STATUS.md for the status-plane contract.
            if (!self.core.tryLockApplyShared()) {
                return .{
                    .async_indexing = Self.snapshotAsyncIndexingStats(self),
                    .doc_set_planning = Self.snapshotDocSetPlanningStats(self),
                    .visibility = Self.snapshotVisibilityStats(self),
                    .foreign_keys = Self.snapshotForeignKeyStats(self),
                    .relational_index_repair = .{},
                    .enrichment = Self.enrichmentStatsWithSupervisorState(self, .{}),
                    .resolution = Self.resolutionStageStats(self),
                    .promotion = Self.promotionStageStats(self),
                    .ttl_cleanup = if (self.ttl_runtime) |runtime| runtime.stats() else .{},
                    .transaction_recovery = if (self.transaction_runtime) |runtime| runtime.stats() else .{},
                    .graph_metric_runtime = Self.graphMetricRuntimeStats(self),
                };
            }
            defer self.core.unlockApplyShared();

            return try Self.statsLocked(self, alloc);
        }

        pub fn runtimeStatusStatsConsistent(self: *DB, alloc: Allocator) !types.DBStats {
            if (self.open_mode == .status_only) {
                return try Self.statusOnlyStats(self, alloc);
            }

            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            return try Self.statsLocked(self, alloc);
        }

        pub fn tryRuntimeStatusStatsConsistent(self: *DB, alloc: Allocator) !?types.DBStats {
            if (self.open_mode == .status_only) {
                return try Self.statusOnlyStats(self, alloc);
            }

            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();

            return try Self.statsLocked(self, alloc);
        }

        pub fn populateAlgebraicIndexStats(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            item: *types.DBIndexStats,
            include_adaptive_scans: bool,
        ) !void {
            const entry = self.core.index_manager.algebraicIndex(index_name) orelse return;
            const status_value = entry.index.status();
            const cfg = entry.index.config();
            item.algebraic_parse_error_count = status_value.parse_error_count;
            item.algebraic_schema_version = cfg.schema_version;
            if (cfg.capability_fingerprint.len > 0) {
                item.algebraic_capability_fingerprint = try alloc.dupe(u8, cfg.capability_fingerprint);
            }
            if (cfg.capability_lifecycle_status.len > 0) {
                item.algebraic_capability_lifecycle_status = try alloc.dupe(u8, cfg.capability_lifecycle_status);
            }
            item.algebraic_capability_change_added_fields = cfg.capability_change_added_fields;
            item.algebraic_capability_change_removed_fields = cfg.capability_change_removed_fields;
            item.algebraic_capability_change_changed_type_fields = cfg.capability_change_changed_type_fields;
            item.algebraic_skipped_dynamic_fields = cfg.skipped_dynamic_fields;
            item.algebraic_skipped_complex_fields = cfg.skipped_complex_fields;
            item.algebraic_skipped_unbounded_fields = cfg.skipped_unbounded_fields;
            item.algebraic_minmax_cache_hits = status_value.minmax_cache_hits;
            item.algebraic_minmax_cache_misses = status_value.minmax_cache_misses;
            item.algebraic_minmax_support_scans = status_value.minmax_support_scans;
            item.algebraic_planner_selected = status_value.planner_algebraic_selected;
            item.algebraic_planner_fallback_count = status_value.planner_fallback_count;
            if (status_value.planner_last_decision) |value| {
                item.algebraic_planner_last_decision = try alloc.dupe(u8, value);
            }
            if (status_value.planner_last_fallback_reason) |value| {
                item.algebraic_planner_last_fallback_reason = try alloc.dupe(u8, value);
            }
            item.algebraic_planner_last_estimated_scan_rows = if (status_value.planner_last_estimated_scan_rows) |value| @intCast(value) else null;
            item.algebraic_planner_last_estimated_result_buckets = if (status_value.planner_last_estimated_result_buckets) |value| @intCast(value) else null;
            item.algebraic_planner_lifecycle_ready = status_value.planner_lifecycle_ready;
            if (status_value.planner_lifecycle_blocking_reason) |value| {
                item.algebraic_planner_lifecycle_blocking_reason = try alloc.dupe(u8, value);
            }
            item.algebraic_dictionary_registry_claimed_count = status_value.dictionary_registry_claimed_count;
            item.algebraic_dictionary_registry_already_owned_count = status_value.dictionary_registry_already_owned_count;
            item.algebraic_dictionary_registry_owned_by_other_count = status_value.dictionary_registry_owned_by_other_count;
            item.algebraic_dictionary_registry_ready_hit_count = status_value.dictionary_registry_ready_hit_count;
            item.algebraic_dictionary_registry_ready_miss_count = status_value.dictionary_registry_ready_miss_count;
            item.algebraic_distributed_partial_validation_proven_count = status_value.distributed_partial_validation_proven_count;
            item.algebraic_distributed_partial_validation_rejected_count = status_value.distributed_partial_validation_rejected_count;
            item.algebraic_distributed_partial_rows_exported_count = status_value.distributed_partial_rows_exported_count;
            item.algebraic_vector_filter_attempt_count = status_value.vector_filter_attempt_count;
            item.algebraic_vector_filter_resolved_count = status_value.vector_filter_resolved_count;
            item.algebraic_vector_filter_unsupported_count = status_value.vector_filter_unsupported_count;
            item.algebraic_vector_filter_fail_closed_count = status_value.vector_filter_fail_closed_count;
            item.algebraic_vector_filter_include_doc_id_count = status_value.vector_filter_include_doc_id_count;
            item.algebraic_vector_filter_exclude_doc_id_count = status_value.vector_filter_exclude_doc_id_count;
            item.algebraic_observed_query_shape_count = status_value.observed_query_shape_count;
            item.algebraic_recommendation_count = status_value.recommendation_count;
            if (include_adaptive_scans) {
                const adaptive_candidates = try entry.index.scanPersistedAdaptiveCandidates(self.core.store);
                defer {
                    for (adaptive_candidates) |*candidate| candidate.deinit(entry.index.alloc);
                    if (adaptive_candidates.len > 0) entry.index.alloc.free(adaptive_candidates);
                }
                item.algebraic_adaptive_candidate_count = @intCast(adaptive_candidates.len);
                var top_candidate_index: ?usize = null;
                for (adaptive_candidates) |candidate| switch (candidate.lifecycle) {
                    .backfilling => item.algebraic_adaptive_backfilling_count += 1,
                    .ready => item.algebraic_adaptive_ready_count += 1,
                    .stale => item.algebraic_adaptive_stale_count += 1,
                    .dematerialize_recommended => item.algebraic_adaptive_dematerialize_recommended_count += 1,
                    else => {},
                };
                for (adaptive_candidates, 0..) |candidate, i| {
                    const selected_index = top_candidate_index orelse {
                        top_candidate_index = i;
                        continue;
                    };
                    const selected = adaptive_candidates[selected_index];
                    if (candidate.score > selected.score or
                        (candidate.score == selected.score and candidate.observation_count > selected.observation_count))
                    {
                        top_candidate_index = i;
                    }
                }
                if (adaptive_candidates.len > 0) {
                    const status_candidates = try alloc.alloc(types.AlgebraicCandidateStatus, adaptive_candidates.len);
                    var initialized_candidates: usize = 0;
                    errdefer {
                        for (status_candidates[0..initialized_candidates]) |candidate| {
                            alloc.free(candidate.recommendation);
                            alloc.free(candidate.materialization_id);
                            alloc.free(candidate.lifecycle);
                            alloc.free(candidate.decision);
                        }
                        alloc.free(status_candidates);
                    }
                    for (adaptive_candidates, 0..) |candidate, i| {
                        status_candidates[i] = try cloneAlgebraicCandidateStatusAlloc(
                            alloc,
                            candidate.recommendation,
                            candidate.materialization_id,
                            @tagName(candidate.lifecycle),
                            candidate.decision,
                            candidate.observation_count,
                            candidate.estimated_scan_rows_saved,
                            candidate.estimated_write_cost,
                            candidate.estimated_tensor_rows,
                            candidate.estimated_storage_bytes,
                            candidate.estimated_write_amplification,
                            candidate.score,
                            candidate.idle_miss_count,
                            candidate.generation,
                        );
                        initialized_candidates += 1;
                    }
                    item.algebraic_candidates = status_candidates;
                }
                if (top_candidate_index) |i| {
                    const candidate = adaptive_candidates[i];
                    item.algebraic_top_candidate = try cloneAlgebraicCandidateStatusAlloc(
                        alloc,
                        candidate.recommendation,
                        candidate.materialization_id,
                        @tagName(candidate.lifecycle),
                        candidate.decision,
                        candidate.observation_count,
                        candidate.estimated_scan_rows_saved,
                        candidate.estimated_write_cost,
                        candidate.estimated_tensor_rows,
                        candidate.estimated_storage_bytes,
                        candidate.estimated_write_amplification,
                        candidate.score,
                        candidate.idle_miss_count,
                        candidate.generation,
                    );
                }
                const adaptive_decisions = try entry.index.scanPersistedAdaptiveDecisions(self.core.store, 16);
                defer {
                    for (adaptive_decisions) |*decision| decision.deinit(entry.index.alloc);
                    if (adaptive_decisions.len > 0) entry.index.alloc.free(adaptive_decisions);
                }
                item.algebraic_adaptive_decision_history_count = @intCast(adaptive_decisions.len);
                for (adaptive_decisions) |decision| {
                    if (!std.mem.eql(u8, decision.previous_decision, decision.decision) or decision.score_delta != 0) {
                        item.algebraic_adaptive_policy_drift_count += 1;
                    }
                }
                if (adaptive_decisions.len > 0) {
                    const status_decisions = try alloc.alloc(types.AlgebraicCandidateDecisionStatus, adaptive_decisions.len);
                    var initialized_decisions: usize = 0;
                    errdefer {
                        for (status_decisions[0..initialized_decisions]) |decision| {
                            alloc.free(decision.recommendation);
                            alloc.free(decision.materialization_id);
                            alloc.free(decision.lifecycle);
                            alloc.free(decision.previous_decision);
                            alloc.free(decision.decision);
                        }
                        alloc.free(status_decisions);
                    }
                    for (adaptive_decisions, 0..) |decision, i| {
                        status_decisions[i] = try cloneAlgebraicCandidateDecisionStatusAlloc(
                            alloc,
                            decision.recommendation,
                            decision.materialization_id,
                            @tagName(decision.lifecycle),
                            decision.previous_decision,
                            decision.decision,
                            decision.observation_count,
                            decision.estimated_scan_rows_saved,
                            decision.estimated_write_cost,
                            decision.score,
                            decision.score_delta,
                            decision.idle_miss_count,
                            decision.generation,
                        );
                        initialized_decisions += 1;
                    }
                    item.algebraic_candidate_decision_history = status_decisions;
                }
                const adaptive_progress = try entry.index.scanPersistedAdaptiveProgress(self.core.store);
                defer {
                    for (adaptive_progress) |*progress| progress.deinit(entry.index.alloc);
                    if (adaptive_progress.len > 0) entry.index.alloc.free(adaptive_progress);
                }
                item.algebraic_adaptive_progress_count = @intCast(adaptive_progress.len);
                var active_progress_index: ?usize = null;
                if (adaptive_candidates.len == 0) {
                    for (adaptive_progress) |progress| switch (progress.lifecycle) {
                        .backfilling => item.algebraic_adaptive_backfilling_count += 1,
                        .ready => item.algebraic_adaptive_ready_count += 1,
                        .stale => item.algebraic_adaptive_stale_count += 1,
                        .dematerialize_recommended => item.algebraic_adaptive_dematerialize_recommended_count += 1,
                        else => {},
                    };
                }
                for (adaptive_progress, 0..) |progress, i| {
                    const selected_index = active_progress_index orelse {
                        active_progress_index = i;
                        continue;
                    };
                    const selected = adaptive_progress[selected_index];
                    if ((progress.lifecycle == .backfilling and selected.lifecycle != .backfilling) or
                        (progress.lifecycle == selected.lifecycle and progress.target_sequence > selected.target_sequence) or
                        (progress.lifecycle == selected.lifecycle and progress.target_sequence == selected.target_sequence and progress.rows_processed > selected.rows_processed))
                    {
                        active_progress_index = i;
                    }
                }
                if (adaptive_progress.len > 0) {
                    const status_progress = try alloc.alloc(types.AlgebraicProgressStatus, adaptive_progress.len);
                    var initialized_progress: usize = 0;
                    errdefer {
                        for (status_progress[0..initialized_progress]) |progress| {
                            alloc.free(progress.recommendation);
                            alloc.free(progress.materialization_id);
                            alloc.free(progress.lifecycle);
                        }
                        alloc.free(status_progress);
                    }
                    for (adaptive_progress, 0..) |progress, i| {
                        status_progress[i] = try cloneAlgebraicProgressStatusAlloc(
                            alloc,
                            progress.recommendation,
                            progress.materialization_id,
                            @tagName(progress.lifecycle),
                            progress.target_sequence,
                            progress.applied_sequence,
                            progress.rows_processed,
                            progress.target_rows,
                        );
                        initialized_progress += 1;
                    }
                    item.algebraic_progress = status_progress;
                }
                if (active_progress_index) |i| {
                    const progress = adaptive_progress[i];
                    item.algebraic_active_progress = try cloneAlgebraicProgressStatusAlloc(
                        alloc,
                        progress.recommendation,
                        progress.materialization_id,
                        @tagName(progress.lifecycle),
                        progress.target_sequence,
                        progress.applied_sequence,
                        progress.rows_processed,
                        progress.target_rows,
                    );
                }
            }
            if (status_value.last_error_doc_key) |value| {
                item.algebraic_last_error_doc_key = try alloc.dupe(u8, value);
            }
            errdefer if (item.algebraic_last_error_doc_key) |value| {
                alloc.free(value);
                item.algebraic_last_error_doc_key = null;
            };
            if (status_value.last_error_reason) |value| {
                item.algebraic_last_error_reason = try alloc.dupe(u8, value);
            }
            errdefer if (item.algebraic_last_error_reason) |value| {
                alloc.free(value);
                item.algebraic_last_error_reason = null;
            };
            if (status_value.last_observed_query_shape) |value| {
                item.algebraic_last_observed_query_shape = try alloc.dupe(u8, value);
            }
            errdefer if (item.algebraic_last_observed_query_shape) |value| {
                alloc.free(value);
                item.algebraic_last_observed_query_shape = null;
            };
            if (status_value.last_recommended_materialization) |value| {
                item.algebraic_last_recommended_materialization = try alloc.dupe(u8, value);
            }
        }

        fn applyRelationalGenerationDiagnosticsForIndexStats(self: *DB, item: *types.DBIndexStats) void {
            const active_schema = self.core.schema orelse return;
            if (active_schema.storage_mode != .relational) return;
            for (active_schema.relational_indexes) |index| {
                if (!std.mem.eql(u8, index.name, item.name)) continue;
                if (!relationalIndexAccessMethodMatchesStatsKind(index.access_method, item.kind)) return;
                item.relational_generation_present = true;
                item.relational_generation = index.generation;
                if (schema_mod.relationalIndexGenerationRecordValid(index)) {
                    const record = index.generation_record.?;
                    item.relational_generation_record_valid = true;
                    item.relational_generation = record.generation;
                    item.relational_generation_lifecycle = record.lifecycle;
                    item.relational_generation_lag = record.lag;
                    item.relational_generation_ready_watermark = record.ready_watermark;
                    item.relational_generation_catch_up_required = record.lifecycle != .ready or record.lag != 0;
                } else {
                    item.relational_generation_record_valid = false;
                    item.relational_generation_lifecycle = .rebuild_required;
                    item.relational_generation_lag = 0;
                    item.relational_generation_ready_watermark = 0;
                    item.relational_generation_catch_up_required = true;
                }
                return;
            }
        }

        fn relationalIndexAccessMethodMatchesStatsKind(
            access_method: schema_mod.RelationalIndexAccessMethod,
            kind: types.IndexKind,
        ) bool {
            return switch (access_method) {
                .text_search => kind == .full_text,
                .algebraic_filter => kind == .algebraic,
                .scalar_column, .ordered_tuple => false,
            };
        }

        fn loadIndexRepairStateForStats(self: *const DB, alloc: Allocator) !?index_repair_state.State {
            return self.loadIndexRepairState(alloc) catch |err| switch (err) {
                error.FileNotFound, error.DurableIndexRepairStateUnavailable, error.InvalidIndexRepairState => null,
                else => return err,
            };
        }

        pub fn saveAllLiveIndexStatusSnapshots(self: *DB, alloc: Allocator) !void {
            const configs = try self.core.listIndexes(alloc);
            defer types.freeIndexConfigs(alloc, configs);
            if (configs.len == 0) return;

            var updates = std.ArrayListUnmanaged(apply_state.AppliedSequenceUpdate).empty;
            defer updates.deinit(alloc);
            for (configs) |cfg| {
                try updates.append(alloc, .{
                    .index_name = cfg.name,
                    .sequence = try self.core.loadAppliedSequence(alloc, cfg.name),
                });
            }
            try saveIndexStatusSnapshots(alloc, self.core.store, self.core.index_manager, updates.items);
        }

        pub fn statsLocked(self: *DB, alloc: Allocator) !types.DBStats {
            const configs = try self.core.listIndexes(alloc);
            defer types.freeIndexConfigs(alloc, configs);
            var durable_index_repairs = try Self.loadIndexRepairStateForStats(self, alloc);
            defer if (durable_index_repairs) |*state| state.deinit(alloc);
            const async_indexing = Self.snapshotAsyncIndexingStats(self);
            var raw_identity_stats = try doc_identity.fastStatsFromStore(self.core.store);
            applyCachedIdentityVisibilitySummary(&raw_identity_stats, self.identity_visibility_summary_cache);
            const identity_stats = Self.dbDocIdentityStats(raw_identity_stats, self.core.identity_namespace);
            var primary_doc_count_fallback: ?u64 = null;
            const repair_summary = try self.artifactRepairSummaryRootSnapshot(alloc);
            const repair_issue_count = repair_summary.count;
            var repair_index_fallback = DB.ArtifactRepairIndexFallbackCounts{ .alloc = alloc };
            defer repair_index_fallback.deinit();

            var index_stats = try alloc.alloc(types.DBIndexStats, configs.len);
            var index_count: usize = 0;
            var repair_degraded = !repair_summary.ready or repair_issue_count != 0;
            errdefer {
                for (index_stats[0..index_count]) |item| freeDBIndexStatsItem(alloc, item);
                alloc.free(index_stats);
            }

            var visible_doc_count: u64 = 0;
            var term_doc_freq_cache_hits: u64 = 0;
            var term_doc_freq_cache_misses: u64 = 0;
            for (configs) |cfg| {
                const projection_checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
                const applied_sequence = try Self.managedIndexAppliedSequence(self, alloc, cfg.name);
                const target_sequence = try Self.projectionStatsTargetSequence(self, alloc, cfg, applied_sequence);
                var item = types.DBIndexStats{
                    .name = try alloc.dupe(u8, cfg.name),
                    .kind = cfg.kind,
                    .replay_applied_sequence = applied_sequence,
                    .replay_target_sequence = target_sequence,
                    .replay_catch_up_required = applied_sequence < target_sequence,
                    .catch_up_active = false,
                    .catch_up_applied_sequence = applied_sequence,
                    .catch_up_target_sequence = target_sequence,
                };
                errdefer freeDBIndexStatsItem(alloc, item);
                Self.initializeDerivedCoverageIdentity(cfg, &item);
                applyProjectionCheckpointStats(&item, projection_checkpoint, target_sequence);
                try applyDurableIndexRepairStats(
                    alloc,
                    if (durable_index_repairs) |*state| state else null,
                    self.async_context.index_repair_state_corrupt.load(.acquire),
                    &item,
                );
                Self.applyRelationalGenerationDiagnosticsForIndexStats(self, &item);
                if (target_sequence > 0) {
                    item.backfill_progress = @min(
                        1.0,
                        @as(f64, @floatFromInt(applied_sequence)) / @as(f64, @floatFromInt(target_sequence)),
                    );
                    item.backfill_active = item.backfill_active or applied_sequence < target_sequence;
                }
                if (self.core.index_manager.loadFailure(cfg.name)) |load_error| {
                    item.load_error = try alloc.dupe(u8, load_error);
                    item.repair_degraded = true;
                    item.backfill_active = false;
                    item.catch_up_active = false;
                } else if (try self.loadPersistedIndexLoadFailure(alloc, cfg.name)) |load_error| {
                    item.load_error = load_error;
                    item.repair_degraded = true;
                    item.backfill_active = false;
                    item.catch_up_active = false;
                }
                const index_repair_summary = try self.artifactRepairSummaryIndexSnapshotForStats(alloc, cfg.name, repair_summary.ready, &repair_index_fallback);
                item.repair_issue_count = index_repair_summary.count;
                item.repair_summary_ready = index_repair_summary.ready;
                item.repair_issue_count_estimated = !index_repair_summary.ready;
                item.repair_scan_issue_count = index_repair_summary.repair_scan_count;
                item.repair_degraded = item.repair_degraded or !index_repair_summary.ready or item.repair_issue_count != 0;

                switch (cfg.kind) {
                    .full_text => {
                        if (self.core.textIndex(cfg.name)) |entry| {
                            const text_snapshot = entry.acquireSnapshot();
                            defer text_snapshot.release();
                            item.doc_count = text_snapshot.liveDocCount();
                            item.term_count = textIndexTermCount(entry);
                            visible_doc_count = @max(visible_doc_count, item.doc_count);
                            term_doc_freq_cache_hits += text_snapshot.term_doc_freq_cache_hits;
                            term_doc_freq_cache_misses += text_snapshot.term_doc_freq_cache_misses;
                        }
                        item.text_merge = self.core.index_manager.textMergeStatsSnapshotForIndex(cfg.name);
                    },
                    .dense_vector => {
                        if (self.core.denseIndex(cfg.name)) |entry| {
                            const hbc_stats = entry.index.stats();
                            item.doc_count = hbc_stats.active_count;
                            item.node_count = hbc_stats.node_count;
                            item.root_node = hbc_stats.root_node;
                            item.hbc_cache = dbHbcCacheStats(entry.index.hbcCacheStats());
                            visible_doc_count = @max(visible_doc_count, item.doc_count);
                            try markDenseCoverageRegressionIfNeeded(alloc, self.core.store, cfg.name, &item);
                        }
                        if (async_indexing.dense_catch_up.active) {
                            item.catch_up_active = true;
                            item.backfill_active = true;
                        }
                        item.catch_up_phase = async_indexing.dense_catch_up.phase;
                    },
                    .sparse_vector => {
                        if (self.core.sparseIndex(cfg.name)) |entry| {
                            const sparse_snapshot = entry.index.stats();
                            const sparse_doc_cap = if (identity_stats.live_ordinals > 0)
                                identity_stats.live_ordinals
                            else fallback: {
                                if (primary_doc_count_fallback == null) {
                                    primary_doc_count_fallback = Self.scanPrimaryDocCount(self, self.core.byteRange()) catch 0;
                                }
                                break :fallback if (primary_doc_count_fallback.? > 0)
                                    primary_doc_count_fallback.?
                                else
                                    visible_doc_count;
                            };
                            item.doc_count = if (entry.chunk_name == null and sparse_doc_cap > 0)
                                @min(sparse_snapshot.doc_count, sparse_doc_cap)
                            else
                                sparse_snapshot.doc_count;
                            item.term_count = sparse_snapshot.term_count;
                            visible_doc_count = @max(visible_doc_count, item.doc_count);
                        }
                    },
                    .graph => {
                        if (self.core.graphIndex(cfg.name)) |entry| {
                            graph_stats: {
                                const graph_snapshot = entry.index.stats(alloc) catch break :graph_stats;
                                item.edge_count = graph_snapshot.edge_count;
                                item.node_count = graph_snapshot.node_count;
                                item.doc_count = graph_snapshot.node_count;
                                visible_doc_count = @max(visible_doc_count, item.doc_count);
                            }
                            applyGraphAlgebraicRuntimeStats(&item, &entry.index);
                            try populateGraphMetricStatusStats(alloc, &item, &entry.index);
                        }
                    },
                    .algebraic => try DB.LifecycleCallbacks.populate_algebraic_index_stats(self, alloc, cfg.name, &item, false),
                }
                if (cfg.kind == .dense_vector or cfg.kind == .sparse_vector) {
                    try Self.populateConfiguredDerivedCoverageCounts(self, cfg.name, &item);
                }
                if (item.load_error != null) applyTerminalLoadFailureStatus(&item);
                if (item.repair_degraded) repair_degraded = true;
                index_stats[index_count] = item;
                index_count += 1;
            }

            return .{
                .storage_change_token = self.storageChangeTokenLocked(),
                .source_doc_count = identity_stats.live_ordinals,
                .doc_count = visible_doc_count,
                .index_count = @intCast(self.core.indexCount()),
                .indexes = index_stats[0..index_count],
                .repair_degraded = repair_degraded,
                .repair_issue_count = repair_issue_count,
                .repair_summary_ready = repair_summary.ready,
                .repair_issue_count_estimated = !repair_summary.ready,
                .doc_identity = identity_stats,
                .doc_set_planning = Self.snapshotDocSetPlanningStats(self),
                .visibility = Self.snapshotVisibilityStats(self),
                .foreign_keys = Self.snapshotForeignKeyStats(self),
                .relational_index_repair = Self.snapshotRelationalIndexRepairStatsLockedBestEffort(self),
                .enrichment = Self.enrichmentStatsWithSupervisorState(self, .{}),
                .resolution = Self.resolutionStageStats(self),
                .promotion = Self.promotionStageStats(self),
                .resolver_replay = try Self.resolverReplayDiagnosticsLocked(self, alloc),
                .ttl_cleanup = if (self.ttl_runtime) |runtime| runtime.stats() else .{},
                .transaction_recovery = if (self.transaction_runtime) |runtime| runtime.stats() else .{},
                .text_merge = if (self.text_merge_runtime) |runtime| runtime.statsAssumeApplyLockHeld() else self.core.index_manager.textMergeStatsSnapshot(),
                .graph_metric_runtime = Self.graphMetricRuntimeStats(self),
                .term_doc_freq_cache_hits = term_doc_freq_cache_hits,
                .term_doc_freq_cache_misses = term_doc_freq_cache_misses,
                .async_indexing = async_indexing,
            };
        }

        pub fn diagnosticStats(self: *DB, alloc: Allocator) !types.DBStats {
            if (self.open_mode == .status_only) {
                return try Self.statusOnlyStats(self, alloc);
            }

            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            const byte_range = self.core.byteRange();
            const configs = try self.core.listIndexes(alloc);
            defer types.freeIndexConfigs(alloc, configs);
            var durable_index_repairs = try Self.loadIndexRepairStateForStats(self, alloc);
            defer if (durable_index_repairs) |*state| state.deinit(alloc);
            const replay_debt = try Self.listDerivedReplayDebtAssumeApplyLockHeld(self, alloc);
            defer {
                for (replay_debt) |*status| status.deinit(alloc);
                alloc.free(replay_debt);
            }
            var indexed_doc_count: ?u64 = null;
            for (configs) |cfg| {
                if (cfg.kind != .full_text) continue;
                if (self.core.textIndex(cfg.name)) |entry| {
                    indexed_doc_count = @max(indexed_doc_count orelse 0, entry.snapshot().liveDocCount());
                }
            }
            const visible_doc_count = indexed_doc_count orelse try Self.scanPrimaryDocCount(self, byte_range);
            const async_indexing = self.async_context.stats.snapshot();
            const identity_stats = try Self.diagnosticDocIdentityStats(self, byte_range);
            const repair_summary = try self.artifactRepairSummaryRootSnapshot(alloc);
            const repair_issue_count = repair_summary.count;
            var repair_index_fallback = DB.ArtifactRepairIndexFallbackCounts{ .alloc = alloc };
            defer repair_index_fallback.deinit();

            var index_stats = try alloc.alloc(types.DBIndexStats, configs.len);
            var index_count: usize = 0;
            var repair_degraded = !repair_summary.ready or repair_issue_count != 0;
            errdefer {
                for (index_stats[0..index_count]) |item| freeDBIndexStatsItem(alloc, item);
                alloc.free(index_stats);
            }

            for (configs) |cfg| {
                var item = types.DBIndexStats{
                    .name = try alloc.dupe(u8, cfg.name),
                    .kind = cfg.kind,
                };
                errdefer freeDBIndexStatsItem(alloc, item);
                Self.initializeDerivedCoverageIdentity(cfg, &item);
                Self.applyRelationalGenerationDiagnosticsForIndexStats(self, &item);
                if (self.core.index_manager.loadFailure(cfg.name)) |load_error| {
                    item.load_error = try alloc.dupe(u8, load_error);
                    item.repair_degraded = true;
                    item.backfill_active = false;
                    item.catch_up_active = false;
                } else if (try self.loadPersistedIndexLoadFailure(alloc, cfg.name)) |load_error| {
                    item.load_error = load_error;
                    item.repair_degraded = true;
                    item.backfill_active = false;
                    item.catch_up_active = false;
                }
                const index_repair_summary = try self.artifactRepairSummaryIndexSnapshotForStats(alloc, cfg.name, repair_summary.ready, &repair_index_fallback);
                item.repair_issue_count = index_repair_summary.count;
                item.repair_summary_ready = index_repair_summary.ready;
                item.repair_issue_count_estimated = !index_repair_summary.ready;
                item.repair_scan_issue_count = index_repair_summary.repair_scan_count;
                item.repair_degraded = item.repair_degraded or !index_repair_summary.ready or item.repair_issue_count != 0;
                for (replay_debt) |status| {
                    if (!std.mem.eql(u8, status.index_name, cfg.name)) continue;
                    item.replay_applied_sequence = status.applied_sequence;
                    item.replay_target_sequence = status.target_sequence;
                    item.replay_catch_up_required = status.catch_up_required;
                    item.catch_up_applied_sequence = status.applied_sequence;
                    item.catch_up_target_sequence = status.target_sequence;
                    item.catch_up_active = false;
                    break;
                }
                applyProjectionCheckpointStats(&item, try self.core.loadProjectionCheckpoint(alloc, cfg.name), item.replay_target_sequence);
                try applyDurableIndexRepairStats(
                    alloc,
                    if (durable_index_repairs) |*state| state else null,
                    self.async_context.index_repair_state_corrupt.load(.acquire),
                    &item,
                );
                if (item.load_error != null) applyTerminalLoadFailureStatus(&item);
                switch (cfg.kind) {
                    .full_text => {
                        if (self.core.textIndex(cfg.name)) |entry| {
                            item.doc_count = entry.snapshot().liveDocCount();
                            indexed_doc_count = @max(indexed_doc_count orelse 0, item.doc_count);
                        }
                        item.text_merge = self.core.index_manager.textMergeStatsForIndex(cfg.name);
                        if (self.core.textIndexEntry(cfg.name)) |entry| {
                            const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
                            if (try rebuild_state.estimateProgress(byte_range.start, byte_range.end, alloc)) |progress| {
                                item.backfill_active = true;
                                item.backfill_progress = progress;
                            }
                        }
                    },
                    .dense_vector => {
                        if (self.core.denseIndex(cfg.name)) |entry| {
                            const hbc_stats = entry.index.stats();
                            item.doc_count = hbc_stats.active_count;
                            item.node_count = hbc_stats.node_count;
                            item.root_node = hbc_stats.root_node;
                            item.hbc_cache = dbHbcCacheStats(entry.index.hbcCacheStats());
                            item.hbc_posting = dbHbcPostingStats(try entry.index.postingBacklogStats(), entry.index.getWriteProfile());
                            try markDenseCoverageRegressionIfNeeded(alloc, self.core.store, cfg.name, &item);
                            if (async_indexing.dense_catch_up.active) {
                                item.catch_up_active = true;
                                item.backfill_active = true;
                            }
                            const rebuild_root_path = try DB.LifecycleCallbacks.dense_index_rebuild_state_path_alloc(self, alloc, entry.config.name);
                            defer alloc.free(rebuild_root_path);
                            const rebuild_state = self.core.index_manager.rebuildState(
                                .dense_vector,
                                rebuild_root_path,
                                entry.config,
                            );
                            if (item.doc_count < visible_doc_count) {
                                if (try rebuild_state.estimateProgress(byte_range.start, byte_range.end, alloc)) |progress| {
                                    item.backfill_active = true;
                                    item.backfill_progress = progress;
                                }
                            }
                        }
                        if (!item.backfill_active and item.replay_target_sequence > 0 and item.doc_count < visible_doc_count) {
                            item.backfill_progress = @min(
                                1.0,
                                @as(f64, @floatFromInt(item.replay_applied_sequence)) / @as(f64, @floatFromInt(item.replay_target_sequence)),
                            );
                            item.backfill_active = item.replay_applied_sequence < item.replay_target_sequence;
                        }
                    },
                    .sparse_vector => {
                        if (self.core.sparseIndex(cfg.name)) |entry| {
                            const sparse_stats = entry.index.stats();
                            const raw_identity_stats = doc_identity.fastStatsFromStore(self.core.store) catch null;
                            const sparse_doc_cap = if (identity_stats.live_ordinals > 0)
                                identity_stats.live_ordinals
                            else if (raw_identity_stats) |raw|
                                raw.live_ordinals
                            else
                                visible_doc_count;
                            item.doc_count = if (entry.chunk_name == null and sparse_doc_cap > 0)
                                @min(sparse_stats.doc_count, sparse_doc_cap)
                            else
                                sparse_stats.doc_count;
                            item.term_count = sparse_stats.term_count;
                            const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
                            if (try rebuild_state.estimateProgress(byte_range.start, byte_range.end, alloc)) |progress| {
                                item.backfill_active = true;
                                item.backfill_progress = progress;
                            }
                        }
                        if (!item.backfill_active and item.replay_target_sequence > 0) {
                            item.backfill_progress = @min(
                                1.0,
                                @as(f64, @floatFromInt(item.replay_applied_sequence)) / @as(f64, @floatFromInt(item.replay_target_sequence)),
                            );
                            item.backfill_active = item.replay_applied_sequence < item.replay_target_sequence;
                        }
                    },
                    .graph => {
                        if (self.core.graphIndex(cfg.name)) |entry| {
                            const graph_stats = try entry.index.stats(alloc);
                            item.edge_count = graph_stats.edge_count;
                            item.node_count = graph_stats.node_count;
                            item.doc_count = graph_stats.node_count;
                            applyGraphAlgebraicRuntimeStats(&item, &entry.index);
                            try populateGraphMetricStatusStats(alloc, &item, &entry.index);
                            const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
                            if (try rebuild_state.estimateProgress(byte_range.start, byte_range.end, alloc)) |progress| {
                                item.backfill_active = true;
                                item.backfill_progress = progress;
                            }
                        }
                    },
                    .algebraic => try DB.LifecycleCallbacks.populate_algebraic_index_stats(self, alloc, cfg.name, &item, true),
                }
                if (cfg.kind == .dense_vector or cfg.kind == .sparse_vector) {
                    try Self.populateConfiguredDerivedCoverageCounts(self, cfg.name, &item);
                }
                if (item.load_error != null) applyTerminalLoadFailureStatus(&item);
                if (item.repair_degraded) repair_degraded = true;
                index_stats[index_count] = item;
                index_count += 1;
            }

            // This is a v1 query-visible count, not a canonical primary-store
            // cardinality. Do not add a second public stats count backed by a
            // docstore scan for managed-index coverage; the long-term replacement
            // is a durable table cardinality counter updated on writes/deletes.
            // The full-text index count is the current efficient proxy when
            // present; tables without full-text still fall back to the docstore.
            return .{
                .source_doc_count = identity_stats.live_ordinals,
                .doc_count = visible_doc_count,
                .index_count = @intCast(self.core.indexCount()),
                .indexes = index_stats[0..index_count],
                .repair_degraded = repair_degraded,
                .repair_issue_count = repair_issue_count,
                .repair_summary_ready = repair_summary.ready,
                .repair_issue_count_estimated = !repair_summary.ready,
                .doc_identity = identity_stats,
                .doc_set_planning = Self.snapshotDocSetPlanningStats(self),
                .visibility = Self.snapshotVisibilityStats(self),
                .foreign_keys = Self.snapshotForeignKeyStats(self),
                .relational_index_repair = Self.snapshotRelationalIndexRepairStatsLockedBestEffort(self),
                .enrichment = Self.enrichmentStatsWithSupervisorState(self, .{}),
                .resolution = Self.resolutionStageStats(self),
                .promotion = Self.promotionStageStats(self),
                .resolver_replay = try Self.resolverReplayDiagnosticsLocked(self, alloc),
                .ttl_cleanup = if (self.ttl_runtime) |runtime| runtime.stats() else .{},
                .transaction_recovery = if (self.transaction_runtime) |runtime| runtime.stats() else .{},
                .text_merge = if (self.text_merge_runtime) |runtime| runtime.statsAssumeApplyLockHeld() else self.core.index_manager.textMergeStats(),
                .graph_metric_runtime = Self.graphMetricRuntimeStats(self),
                .term_doc_freq_cache_hits = blk: {
                    var total: u64 = 0;
                    for (configs) |cfg| {
                        if (cfg.kind != .full_text) continue;
                        if (self.core.textIndex(cfg.name)) |entry| {
                            const text_snapshot = entry.acquireSnapshot();
                            defer text_snapshot.release();
                            total += text_snapshot.term_doc_freq_cache_hits;
                        }
                    }
                    break :blk total;
                },
                .term_doc_freq_cache_misses = blk: {
                    var total: u64 = 0;
                    for (configs) |cfg| {
                        if (cfg.kind != .full_text) continue;
                        if (self.core.textIndex(cfg.name)) |entry| {
                            const text_snapshot = entry.acquireSnapshot();
                            defer text_snapshot.release();
                            total += text_snapshot.term_doc_freq_cache_misses;
                        }
                    }
                    break :blk total;
                },
                .async_indexing = async_indexing,
            };
        }

        fn applyStatusOnlyRebuildStateStats(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            item: *types.DBIndexStats,
        ) !void {
            if (cfg.kind == .algebraic) return;
            const rebuild_root_path = try DB.LifecycleCallbacks.dense_index_rebuild_state_path_alloc(self, alloc, cfg.name);
            defer alloc.free(rebuild_root_path);
            const rebuild_state = self.core.index_manager.rebuildState(cfg.kind, rebuild_root_path, cfg);
            var loaded = try rebuild_state.loadWithIo(
                alloc,
                self.core.index_manager.checkpointIo(),
            );
            defer loaded.deinit(alloc);
            const key = switch (loaded) {
                .absent => return,
                .valid => |resume_key| resume_key,
                .legacy => {
                    item.backfill_active = true;
                    item.backfill_progress = 0;
                    return;
                },
                .corrupt => {
                    item.load_error = try alloc.dupe(u8, @errorName(error.InvalidRebuildState));
                    item.repair_degraded = true;
                    item.backfill_active = false;
                    item.backfill_progress = 0;
                    return;
                },
            };
            const byte_range = self.core.byteRange();
            item.backfill_active = true;
            item.backfill_progress = backfill_state_mod.estimateProgressForKey(
                byte_range.start,
                byte_range.end,
                key,
            );
        }

        pub fn statusOnlyStats(self: *DB, alloc: Allocator) !types.DBStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            const configs = try self.core.listIndexes(alloc);
            defer types.freeIndexConfigs(alloc, configs);
            var durable_index_repairs = try Self.loadIndexRepairStateForStats(self, alloc);
            defer if (durable_index_repairs) |*state| state.deinit(alloc);
            var visible_doc_count: u64 = 0;
            const identity_stats = Self.dbDocIdentityStats(try doc_identity.fastStatsFromStore(self.core.store), self.core.identity_namespace);
            const repair_summary = try self.artifactRepairSummaryRootSnapshot(alloc);
            const repair_issue_count = repair_summary.count;
            var repair_index_fallback = DB.ArtifactRepairIndexFallbackCounts{ .alloc = alloc };
            defer repair_index_fallback.deinit();

            var index_stats = try alloc.alloc(types.DBIndexStats, configs.len);
            var index_count: usize = 0;
            var repair_degraded = !repair_summary.ready or repair_issue_count != 0;
            errdefer {
                for (index_stats[0..index_count]) |item| freeDBIndexStatsItem(alloc, item);
                alloc.free(index_stats);
            }

            for (configs) |cfg| {
                const projection_checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
                const applied_sequence = try self.core.loadAppliedSequence(alloc, cfg.name);
                const target_sequence = try Self.projectionStatsTargetSequence(self, alloc, cfg, applied_sequence);
                var item = types.DBIndexStats{
                    .name = try alloc.dupe(u8, cfg.name),
                    .kind = cfg.kind,
                    .replay_applied_sequence = applied_sequence,
                    .replay_target_sequence = target_sequence,
                    .replay_catch_up_required = applied_sequence < target_sequence,
                    .catch_up_active = false,
                    .catch_up_applied_sequence = applied_sequence,
                    .catch_up_target_sequence = target_sequence,
                };
                errdefer freeDBIndexStatsItem(alloc, item);
                Self.initializeDerivedCoverageIdentity(cfg, &item);
                try Self.applyStatusOnlyRebuildStateStats(self, alloc, cfg, &item);
                applyProjectionCheckpointStats(&item, projection_checkpoint, target_sequence);
                try applyDurableIndexRepairStats(
                    alloc,
                    if (durable_index_repairs) |*state| state else null,
                    self.async_context.index_repair_state_corrupt.load(.acquire),
                    &item,
                );
                Self.applyRelationalGenerationDiagnosticsForIndexStats(self, &item);
                if (target_sequence > 0) {
                    item.backfill_progress = @min(
                        1.0,
                        @as(f64, @floatFromInt(applied_sequence)) / @as(f64, @floatFromInt(target_sequence)),
                    );
                    item.backfill_active = item.backfill_active or applied_sequence < target_sequence;
                }
                if (self.core.index_manager.loadFailure(cfg.name)) |load_error| {
                    if (item.load_error) |previous| alloc.free(previous);
                    item.load_error = try alloc.dupe(u8, load_error);
                    applyTerminalLoadFailureStatus(&item);
                } else if (try self.loadPersistedIndexLoadFailure(alloc, cfg.name)) |load_error| {
                    if (item.load_error) |previous| alloc.free(previous);
                    item.load_error = load_error;
                    applyTerminalLoadFailureStatus(&item);
                }
                const index_repair_summary = try self.artifactRepairSummaryIndexSnapshotForStats(alloc, cfg.name, repair_summary.ready, &repair_index_fallback);
                item.repair_issue_count = index_repair_summary.count;
                item.repair_summary_ready = index_repair_summary.ready;
                item.repair_issue_count_estimated = !index_repair_summary.ready;
                item.repair_scan_issue_count = index_repair_summary.repair_scan_count;
                item.repair_degraded = item.repair_degraded or !index_repair_summary.ready or item.repair_issue_count != 0;
                if (try loadIndexStatusSnapshot(alloc, self.core.store, cfg.name)) |status_snapshot| {
                    applyIndexStatusSnapshot(&item, status_snapshot);
                    visible_doc_count = @max(visible_doc_count, item.doc_count);
                }
                if (cfg.kind == .full_text) {
                    item.text_merge = self.core.index_manager.textMergeStatsSnapshotForIndex(cfg.name);
                } else if (cfg.kind == .graph) {
                    if (self.core.graphIndex(cfg.name)) |entry| {
                        try populateGraphMetricStatusStats(alloc, &item, &entry.index);
                    }
                }
                if (cfg.kind == .dense_vector or cfg.kind == .sparse_vector) {
                    Self.populateConfiguredDerivedCoverageCountsBestEffort(self, cfg.name, &item);
                }
                if (item.load_error != null) applyTerminalLoadFailureStatus(&item);
                if (item.repair_degraded) repair_degraded = true;
                index_stats[index_count] = item;
                index_count += 1;
            }

            return .{
                .source_doc_count = identity_stats.live_ordinals,
                .doc_count = visible_doc_count,
                .index_count = @intCast(self.core.indexCount()),
                .indexes = index_stats[0..index_count],
                .repair_degraded = repair_degraded,
                .repair_issue_count = repair_issue_count,
                .repair_summary_ready = repair_summary.ready,
                .repair_issue_count_estimated = !repair_summary.ready,
                .doc_identity = identity_stats,
                .doc_set_planning = Self.snapshotDocSetPlanningStats(self),
                .visibility = Self.snapshotVisibilityStats(self),
                .foreign_keys = Self.snapshotForeignKeyStats(self),
                .relational_index_repair = Self.snapshotRelationalIndexRepairStatsLockedBestEffort(self),
                .resolution = Self.resolutionStageStats(self),
                .promotion = Self.promotionStageStats(self),
                .resolver_replay = try Self.resolverReplayDiagnosticsLocked(self, alloc),
                .graph_metric_runtime = Self.graphMetricRuntimeStats(self),
                .async_indexing = self.async_context.stats.snapshot(),
            };
        }

        pub fn managedIndexAppliedSequence(self: *DB, alloc: Allocator, index_name: []const u8) !u64 {
            var applied_sequence = try self.core.loadAppliedSequence(alloc, index_name);
            if (self.executor.appliedSequence(index_name)) |live_applied| {
                applied_sequence = @max(applied_sequence, live_applied);
            }
            return applied_sequence;
        }

        fn probeDerivedReplayTargetSequence(
            self: *DB,
            alloc: Allocator,
            index_ref: index_manager_mod.ManagedIndexRef,
            applied_sequence: u64,
        ) !u64 {
            return try self.core.replaySource().latestMatchingSequence(
                alloc,
                applied_sequence,
                managedIndexReplayHint(index_ref.kind),
            );
        }

        pub fn managedIndexReplayTargetSequence(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            kind: types.IndexKind,
            applied_sequence: u64,
        ) !u64 {
            return try Self.probeDerivedReplayTargetSequence(
                self,
                alloc,
                .{
                    .name = index_name,
                    .kind = kind,
                },
                applied_sequence,
            );
        }

        pub fn projectionStatsTargetSequence(self: *DB, alloc: Allocator, cfg: types.IndexConfig, applied_sequence: u64) !u64 {
            return try Self.managedIndexReplayTargetSequence(
                self,
                alloc,
                cfg.name,
                cfg.kind,
                applied_sequence,
            );
        }

        pub fn listDerivedReplayDebt(self: *DB, alloc: Allocator) ![]DerivedReplayDebtStatus {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            return try Self.listDerivedReplayDebtAssumeApplyLockHeld(self, alloc);
        }

        fn listDerivedReplayDebtAssumeApplyLockHeld(self: *DB, alloc: Allocator) ![]DerivedReplayDebtStatus {
            const managed_indexes = try self.core.managedIndexes(alloc);
            var transferred_names: usize = 0;
            errdefer {
                for (managed_indexes[transferred_names..]) |index_ref| alloc.free(@constCast(index_ref.name));
                alloc.free(managed_indexes);
            }

            const out = try alloc.alloc(DerivedReplayDebtStatus, managed_indexes.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |*status| status.deinit(alloc);
                alloc.free(out);
            }

            for (managed_indexes) |index_ref| {
                const applied_sequence = try Self.managedIndexAppliedSequence(self, alloc, index_ref.name);
                const target_sequence = try Self.probeDerivedReplayTargetSequence(
                    self,
                    alloc,
                    index_ref,
                    applied_sequence,
                );
                out[initialized] = .{
                    .index_name = index_ref.name,
                    .kind = index_ref.kind,
                    .applied_sequence = applied_sequence,
                    .target_sequence = target_sequence,
                    .catch_up_required = applied_sequence < target_sequence,
                };
                initialized += 1;
                transferred_names += 1;
            }
            alloc.free(managed_indexes);
            return out;
        }

        pub fn graphMetricRuntimeStats(self: *DB) types.GraphMetricRuntimeStats {
            const runtime = self.graph_metric_runtime orelse return .{};
            const runtime_snapshot = runtime.stats();
            const total = runtime_snapshot.total_result;
            const last = runtime_snapshot.last_result;
            return .{
                .enabled = runtime_snapshot.enabled,
                .role = Self.graphMetricRuntimeRoleToStats(runtime_snapshot.role),
                .runtime_id_hash = runtime_snapshot.runtime_id_hash,
                .owner_id_hash = runtime_snapshot.owner_id_hash,
                .lease_key_hash = runtime_snapshot.lease_key_hash,
                .worker_id_hash = runtime_snapshot.worker_id_hash,
                .worker_count = @intCast(runtime_snapshot.worker_count),
                .lease_owned = runtime_snapshot.lease_owned,
                .has_lease = runtime_snapshot.has_lease,
                .acquisition_count = runtime_snapshot.acquisition_count,
                .takeover_count = runtime_snapshot.takeover_count,
                .lease_acquire_failures = runtime_snapshot.lease_acquire_failures,
                .lost_leases = runtime_snapshot.lost_leases,
                .last_acquired_ms = runtime_snapshot.last_acquired_ms,
                .started = runtime_snapshot.started,
                .shutdown = runtime_snapshot.shutdown,
                .notified = runtime_snapshot.notified,
                .ticks_started = runtime_snapshot.ticks_started,
                .ticks_completed = runtime_snapshot.ticks_completed,
                .durable_progress_ticks = runtime_snapshot.durable_progress_ticks,
                .idle_ticks = runtime_snapshot.idle_ticks,
                .error_ticks = runtime_snapshot.error_ticks,
                .last_error_name = runtime_snapshot.last_error_name,
                .total_metrics_scanned = @intCast(total.metrics_scanned),
                .total_active_builds = @intCast(total.active_builds),
                .total_builds_started = @intCast(total.builds_started),
                .total_worker_steps = @intCast(total.worker_steps),
                .total_coordinator_steps = @intCast(total.coordinator_steps),
                .total_pages_claimed = @intCast(total.pages_claimed),
                .total_pages_completed = @intCast(total.pages_completed),
                .total_phases_advanced = @intCast(total.phases_advanced),
                .total_published = @intCast(total.published),
                .total_failed_builds = @intCast(total.failed_builds),
                .last_metrics_scanned = @intCast(last.metrics_scanned),
                .last_active_builds = @intCast(last.active_builds),
                .last_builds_started = @intCast(last.builds_started),
                .last_worker_steps = @intCast(last.worker_steps),
                .last_coordinator_steps = @intCast(last.coordinator_steps),
                .last_pages_claimed = @intCast(last.pages_claimed),
                .last_pages_completed = @intCast(last.pages_completed),
                .last_phases_advanced = @intCast(last.phases_advanced),
                .last_published = @intCast(last.published),
                .last_failed_builds = @intCast(last.failed_builds),
                .last_budget_exhausted = last.budget_exhausted,
            };
        }

        fn graphMetricRuntimeRoleToStats(role: graph_metric_runtime_mod.Role) types.GraphMetricRuntimeRole {
            return switch (role) {
                .combined => .combined,
                .coordinator => .coordinator,
                .worker => .worker,
                .worker_pool => .worker_pool,
            };
        }

        pub fn enrichmentStatsWithSupervisorState(self: *DB, fallback: types.EnrichmentStats) types.EnrichmentStats {
            // Status is called while the DB apply lock is held. Structural
            // mutations hold lifecycle while stopping and joining the enrichment
            // worker, and that worker can be waiting for apply. Never close the
            // lifecycle -> worker join -> apply -> lifecycle cycle here.
            const lifecycle_locked = self.async_context.enrichment_lifecycle_mutex.tryLock();
            defer if (lifecycle_locked) self.async_context.enrichment_lifecycle_mutex.unlock();
            var enrichment_status = if (lifecycle_locked)
                if (self.async_context.enrichment_runtime) |runtime|
                    runtime.stats()
                else
                    Self.persistedEnrichmentStats(self) catch fallback
            else
                fallback;
            const desired = self.async_context.enrichment_desired_running.load(.acquire);
            const started = if (lifecycle_locked)
                if (self.async_context.enrichment_runtime) |runtime| runtime.isStarted() else false
            else
                false;
            if (desired and !started) {
                // While lifecycle owns the runtime pointer, expose the persisted
                // snapshot as a supervised transition rather than a fatal worker.
                const supervised = !lifecycle_locked or self.async_context.enrichment_restart_state.load(.acquire) != 0;
                enrichment_status.retrying = supervised;
                enrichment_status.worker_failed = !supervised;
                enrichment_status.projection_checkpoint_status = if (supervised) "retrying" else "failed";
            }
            return enrichment_status;
        }

        fn overlayRuntimeStatusRuntimeOnly(self: *DB, runtime_stats: *types.DBStats) void {
            runtime_stats.async_indexing = Self.snapshotAsyncIndexingStats(self);
            runtime_stats.doc_set_planning = Self.snapshotDocSetPlanningStats(self);
            runtime_stats.visibility = Self.snapshotVisibilityStats(self);
            runtime_stats.foreign_keys = Self.snapshotForeignKeyStats(self);
            runtime_stats.relational_index_repair = Self.snapshotRelationalIndexRepairStatsBestEffort(self);
            runtime_stats.enrichment = Self.enrichmentStatsWithSupervisorState(self, runtime_stats.enrichment);
            runtime_stats.resolution = Self.resolutionStageStats(self);
            runtime_stats.promotion = Self.promotionStageStats(self);
            runtime_stats.ttl_cleanup = if (self.ttl_runtime) |runtime| runtime.stats() else runtime_stats.ttl_cleanup;
            runtime_stats.transaction_recovery = if (self.transaction_runtime) |runtime| runtime.stats() else runtime_stats.transaction_recovery;
            runtime_stats.graph_metric_runtime = Self.graphMetricRuntimeStats(self);

            // Runtime-only diagnostics may run under apply. Skip enrichment
            // detail while its lifecycle owner is transitioning the runtime.
            if (self.async_context.enrichment_lifecycle_mutex.tryLock()) {
                defer self.async_context.enrichment_lifecycle_mutex.unlock();
                if (self.async_context.enrichment_runtime) |runtime| {
                    for (runtime_stats.indexes) |*item| {
                        item.enrichment_failed = runtime.indexHasIsolatedFailure(item.name);
                    }
                }
            }

            for (runtime_stats.indexes) |*item| {
                const dense_catch_up = item.kind == .dense_vector and runtime_stats.async_indexing.dense_catch_up.active;
                if (!dense_catch_up) if (self.executor.appliedSequence(item.name)) |live_applied| {
                    item.replay_applied_sequence = @max(item.replay_applied_sequence, live_applied);
                    item.replay_target_sequence = @max(item.replay_target_sequence, live_applied);
                };
                item.catch_up_active = dense_catch_up;
                if (item.kind == .dense_vector) {
                    const progress = runtime_stats.async_indexing.dense_catch_up;
                    if (dense_catch_up) {
                        if (progress.current_sequence != 0) {
                            item.replay_applied_sequence = if (item.replay_applied_sequence == 0)
                                progress.current_sequence
                            else
                                @min(item.replay_applied_sequence, progress.current_sequence);
                            item.catch_up_applied_sequence = progress.current_sequence;
                        } else {
                            item.catch_up_applied_sequence = item.replay_applied_sequence;
                        }
                        item.catch_up_target_sequence = @max(item.replay_target_sequence, progress.current_target_sequence);
                        item.replay_target_sequence = item.catch_up_target_sequence;
                        item.replay_catch_up_required = true;
                        item.backfill_active = true;
                    } else {
                        item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
                        item.catch_up_applied_sequence = item.replay_applied_sequence;
                        item.catch_up_target_sequence = item.replay_target_sequence;
                    }
                    item.catch_up_phase = progress.phase;
                } else {
                    item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
                    item.catch_up_applied_sequence = item.replay_applied_sequence;
                    item.catch_up_target_sequence = item.replay_target_sequence;
                    item.catch_up_phase = .idle;
                }
                if (item.catch_up_active or item.replay_catch_up_required) {
                    item.backfill_active = true;
                    if (item.replay_target_sequence > 0) {
                        item.backfill_progress = @min(
                            1.0,
                            @as(f64, @floatFromInt(item.replay_applied_sequence)) /
                                @as(f64, @floatFromInt(item.replay_target_sequence)),
                        );
                    }
                } else {
                    item.backfill_active = durableGenerationBuildActive(item);
                    if (item.replay_target_sequence > 0) item.backfill_progress = 1.0;
                }
                normalizeReplayStatusFromDurableCheckpoint(item);
                item.checkpoint_replay_tail_sequence_count = item.replay_target_sequence -| item.projection_checkpoint_applied_sequence;
                Self.applyRelationalGenerationDiagnosticsForIndexStats(self, item);
                if (item.load_error != null) applyTerminalLoadFailureStatus(item);
            }
        }

        pub fn normalizeReplayStatusFromDurableCheckpoint(item: *types.DBIndexStats) void {
            const durable_applied = item.projection_checkpoint_applied_sequence;
            if (durable_applied <= item.replay_applied_sequence) return;

            // Projection checkpoints are persisted only after the index publication
            // boundary. A worker snapshot can briefly lag that durable write while
            // it updates its in-memory sequence, but status must not expose the
            // transient ordering as durable replay debt.
            item.replay_applied_sequence = durable_applied;
            item.replay_target_sequence = @max(item.replay_target_sequence, durable_applied);
            item.catch_up_applied_sequence = @max(item.catch_up_applied_sequence, durable_applied);
            item.catch_up_target_sequence = @max(item.catch_up_target_sequence, item.replay_target_sequence);
            item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
            if (!item.catch_up_active and !item.replay_catch_up_required) {
                item.backfill_active = durableGenerationBuildActive(item);
                if (item.replay_target_sequence > 0) item.backfill_progress = 1.0;
            }
        }

        fn durableGenerationBuildActive(item: *const types.DBIndexStats) bool {
            const generation_build = std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.operator_generation_rebuild)) or
                std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.operator_generation_validation)) or
                std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.artifact_coverage_mismatch)) or
                std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.artifact_counter_missing)) or
                std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.projection_generation_invalid));
            return generation_build and
                !std.mem.eql(u8, item.index_repair_phase, @tagName(index_repair_state.Phase.terminal));
        }

        fn overlayRuntimeStatusIndexesAssumeApplyLockHeld(self: *DB, stats_alloc: std.mem.Allocator, runtime_stats: *types.DBStats) void {
            if (!self.core.tryLockApplyShared()) return;
            defer self.core.unlockApplyShared();
            Self.overlayRuntimeStatusIndexesLocked(self, stats_alloc, runtime_stats) catch {
                markMissingDerivedCoverageIdentities(runtime_stats.indexes);
                return;
            };
        }

        fn overlayRuntimeStatusIndexesLocked(self: *DB, stats_alloc: std.mem.Allocator, runtime_stats: *types.DBStats) !void {
            // Coverage outcomes and identity totals form one apply-fenced status
            // invariant; refresh both before overlaying live index counters.
            var identity_stats = try doc_identity.fastStatsFromStore(self.core.store);
            applyCachedIdentityVisibilitySummary(&identity_stats, self.identity_visibility_summary_cache);
            runtime_stats.source_doc_count = identity_stats.live_ordinals;
            runtime_stats.doc_identity = Self.dbDocIdentityStats(identity_stats, self.core.identity_namespace);
            try Self.hydrateDerivedCoverageIdentities(self, stats_alloc, runtime_stats.indexes);
            var visible_doc_count = runtime_stats.doc_count;
            for (runtime_stats.indexes) |*item| {
                switch (item.kind) {
                    .full_text => {
                        if (self.core.textIndex(item.name)) |entry| {
                            const text_snapshot = entry.acquireSnapshot();
                            defer text_snapshot.release();
                            item.doc_count = text_snapshot.liveDocCount();
                            visible_doc_count = @max(visible_doc_count, item.doc_count);
                        }
                        item.text_merge = self.core.index_manager.textMergeStatsSnapshotForIndex(item.name);
                    },
                    .dense_vector => {
                        if (self.core.denseIndex(item.name)) |entry| {
                            const hbc_stats = entry.index.stats();
                            item.doc_count = hbc_stats.active_count;
                            item.node_count = hbc_stats.node_count;
                            item.root_node = hbc_stats.root_node;
                            item.hbc_cache = dbHbcCacheStats(entry.index.hbcCacheStats());
                        }
                        try Self.populateConfiguredDerivedCoverageCounts(self, item.name, item);
                        visible_doc_count = @max(visible_doc_count, item.doc_count);
                    },
                    .sparse_vector => {
                        if (self.core.sparseIndex(item.name)) |entry| {
                            const sparse_stats = entry.index.stats();
                            item.doc_count = if (entry.chunk_name == null and runtime_stats.doc_count > 0)
                                @min(sparse_stats.doc_count, runtime_stats.doc_count)
                            else
                                sparse_stats.doc_count;
                            item.term_count = sparse_stats.term_count;
                        }
                        try Self.populateConfiguredDerivedCoverageCounts(self, item.name, item);
                        visible_doc_count = @max(visible_doc_count, item.doc_count);
                    },
                    .graph => {
                        if (self.core.graphIndex(item.name)) |entry| {
                            if (entry.index.stats(self.alloc)) |graph_stats| {
                                item.edge_count = graph_stats.edge_count;
                                item.node_count = graph_stats.node_count;
                                item.doc_count = graph_stats.node_count;
                            } else |_| {}
                            applyGraphAlgebraicRuntimeStats(item, &entry.index);
                        }
                        visible_doc_count = @max(visible_doc_count, item.doc_count);
                    },
                    .algebraic => {
                        if (self.core.index_manager.algebraicIndex(item.name)) |entry| {
                            const status_value = entry.index.status();
                            item.algebraic_parse_error_count = status_value.parse_error_count;
                            item.algebraic_minmax_cache_hits = status_value.minmax_cache_hits;
                            item.algebraic_minmax_cache_misses = status_value.minmax_cache_misses;
                            item.algebraic_minmax_support_scans = status_value.minmax_support_scans;
                            item.algebraic_planner_selected = status_value.planner_algebraic_selected;
                            item.algebraic_planner_fallback_count = status_value.planner_fallback_count;
                            replaceOptionalOwnedStringBestEffort(
                                stats_alloc,
                                &item.algebraic_planner_last_decision,
                                status_value.planner_last_decision,
                            );
                            replaceOptionalOwnedStringBestEffort(
                                stats_alloc,
                                &item.algebraic_planner_last_fallback_reason,
                                status_value.planner_last_fallback_reason,
                            );
                            item.algebraic_planner_last_estimated_scan_rows = if (status_value.planner_last_estimated_scan_rows) |value| @intCast(value) else null;
                            item.algebraic_planner_last_estimated_result_buckets = if (status_value.planner_last_estimated_result_buckets) |value| @intCast(value) else null;
                            item.algebraic_planner_lifecycle_ready = status_value.planner_lifecycle_ready;
                            replaceOptionalOwnedStringBestEffort(
                                stats_alloc,
                                &item.algebraic_planner_lifecycle_blocking_reason,
                                status_value.planner_lifecycle_blocking_reason,
                            );
                            item.algebraic_dictionary_registry_claimed_count = status_value.dictionary_registry_claimed_count;
                            item.algebraic_dictionary_registry_already_owned_count = status_value.dictionary_registry_already_owned_count;
                            item.algebraic_dictionary_registry_owned_by_other_count = status_value.dictionary_registry_owned_by_other_count;
                            item.algebraic_dictionary_registry_ready_hit_count = status_value.dictionary_registry_ready_hit_count;
                            item.algebraic_dictionary_registry_ready_miss_count = status_value.dictionary_registry_ready_miss_count;
                            item.algebraic_distributed_partial_validation_proven_count = status_value.distributed_partial_validation_proven_count;
                            item.algebraic_distributed_partial_validation_rejected_count = status_value.distributed_partial_validation_rejected_count;
                            item.algebraic_distributed_partial_rows_exported_count = status_value.distributed_partial_rows_exported_count;
                            item.algebraic_vector_filter_attempt_count = status_value.vector_filter_attempt_count;
                            item.algebraic_vector_filter_resolved_count = status_value.vector_filter_resolved_count;
                            item.algebraic_vector_filter_unsupported_count = status_value.vector_filter_unsupported_count;
                            item.algebraic_vector_filter_fail_closed_count = status_value.vector_filter_fail_closed_count;
                            item.algebraic_vector_filter_include_doc_id_count = status_value.vector_filter_include_doc_id_count;
                            item.algebraic_vector_filter_exclude_doc_id_count = status_value.vector_filter_exclude_doc_id_count;
                            item.algebraic_observed_query_shape_count = status_value.observed_query_shape_count;
                            item.algebraic_recommendation_count = status_value.recommendation_count;
                        }
                    },
                }
            }
            runtime_stats.doc_count = visible_doc_count;
            runtime_stats.text_merge = if (self.text_merge_runtime) |runtime| runtime.statsAssumeApplyLockHeld() else self.core.index_manager.textMergeStatsSnapshot();
        }

        pub fn snapshotLsmMaintenanceStats(self: *DB) lsm_backend_mod.Backend.MaintenanceStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return Self.snapshotLsmMaintenanceStatsLocked(self);
        }

        pub fn trySnapshotLsmMaintenanceStats(self: *DB) ?lsm_backend_mod.Backend.MaintenanceStats {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            return Self.snapshotLsmMaintenanceStatsLocked(self);
        }

        pub fn snapshotLsmMaintenanceStatsLocked(self: *DB) lsm_backend_mod.Backend.MaintenanceStats {
            var maintenance_stats = lsm_backend_mod.Backend.MaintenanceStats{};
            if (self.core.primary_store_owner.snapshotLsmMaintenanceStats()) |primary_stats| {
                lsm_backend_mod.Backend.accumulateMaintenanceStats(&maintenance_stats, primary_stats);
            }
            lsm_backend_mod.Backend.accumulateMaintenanceStats(&maintenance_stats, self.core.index_manager.snapshotLsmMaintenanceStats());
            return maintenance_stats;
        }

        pub fn snapshotLsmWriteStats(self: *DB) lsm_backend_mod.Backend.WriteStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return Self.snapshotLsmWriteStatsLocked(self);
        }

        pub fn trySnapshotLsmWriteStats(self: *DB) ?lsm_backend_mod.Backend.WriteStats {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            return Self.snapshotLsmWriteStatsLocked(self);
        }

        pub fn snapshotLsmWriteStatsLocked(self: *DB) lsm_backend_mod.Backend.WriteStats {
            var write_stats = lsm_backend_mod.Backend.WriteStats{};
            if (self.core.primary_store_owner.snapshotLsmWriteStats()) |primary_stats| {
                lsm_backend_mod.Backend.accumulateWriteStats(&write_stats, primary_stats);
            }
            lsm_backend_mod.Backend.accumulateWriteStats(&write_stats, self.core.index_manager.snapshotLsmWriteStats());
            return write_stats;
        }

        pub fn storageChangeTokenLocked(self: *DB) u64 {
            const write_stats = Self.snapshotLsmWriteStatsLocked(self);
            var hasher = std.hash.Wyhash.init(0x6c736d5f73746f72);
            const wal_growth_bucket = write_stats.wal_append_bytes / (1024 * 1024);
            hasher.update(std.mem.asBytes(&wal_growth_bucket));
            hasher.update(std.mem.asBytes(&write_stats.wal_resets));
            hasher.update(std.mem.asBytes(&write_stats.table_file_bytes));
            hasher.update(std.mem.asBytes(&write_stats.manifest_writes));
            hasher.update(std.mem.asBytes(&write_stats.manifest_bytes));
            return hasher.final();
        }

        pub fn snapshotTextMemoryAttributionStats(self: *DB) index_manager_mod.TextMemoryAttributionStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return self.core.index_manager.snapshotTextMemoryAttribution();
        }

        pub fn trySnapshotTextMemoryAttributionStats(self: *DB) ?index_manager_mod.TextMemoryAttributionStats {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            return self.core.index_manager.snapshotTextMemoryAttribution();
        }

        pub fn observedDynamicFieldCapabilitiesAlloc(
            self: *DB,
            alloc: Allocator,
            index_name: ?[]const u8,
        ) ![]schema_mod.FieldCapability {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.core.index_manager.observedDynamicFieldCapabilitiesAlloc(alloc, index_name);
        }

        pub fn tryObservedDynamicFieldCapabilitiesAlloc(
            self: *DB,
            alloc: Allocator,
            index_name: ?[]const u8,
        ) ?[]schema_mod.FieldCapability {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            return self.core.index_manager.observedDynamicFieldCapabilitiesAlloc(alloc, index_name) catch null;
        }

        pub fn observedDynamicFieldCapabilitySetsAlloc(
            self: *DB,
            alloc: Allocator,
        ) ![]index_manager_mod.IndexManager.ObservedDynamicFieldCapabilitySet {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.core.index_manager.observedDynamicFieldCapabilitySetsAlloc(alloc);
        }

        pub fn tryObservedDynamicFieldCapabilitySetsAlloc(
            self: *DB,
            alloc: Allocator,
        ) ?[]index_manager_mod.IndexManager.ObservedDynamicFieldCapabilitySet {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            return self.core.index_manager.observedDynamicFieldCapabilitySetsAlloc(alloc) catch null;
        }

        pub fn snapshotTextMergeStats(self: *DB) types.TextMergeStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return self.core.index_manager.textMergeStatsSnapshot();
        }

        pub fn trySnapshotTextMergeStats(self: *DB) ?types.TextMergeStats {
            if (!self.core.tryLockApplyShared()) return null;
            defer self.core.unlockApplyShared();
            return self.core.index_manager.textMergeStatsSnapshot();
        }

        pub fn snapshotPrimaryLsmWriteStatsForTest(self: *DB) ?lsm_backend_mod.Backend.WriteStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return self.core.primary_store_owner.snapshotLsmWriteStats();
        }

        pub fn snapshotLsmNativeStorageStats(self: *DB) lsm_backend_mod.NativeStorageStats {
            return self.backend_runtime.snapshotNativeStorageStats();
        }

        pub fn trySnapshotLsmNativeStorageStats(self: *DB) ?lsm_backend_mod.NativeStorageStats {
            return self.backend_runtime.snapshotNativeStorageStats();
        }

        pub fn runLsmMaintenanceStep(self: *DB) !bool {
            self.core.lockApply();
            defer self.core.unlockApply();

            const primary_score = self.core.primary_store_owner.lsmMaintenanceScore();
            const index_score = self.core.index_manager.lsmMaintenanceScore();
            if (primary_score == 0 and index_score == 0) {
                self.core.primary_store_owner.refreshLsmMaintenanceDebtHint();
                self.core.index_manager.refreshLsmMaintenanceDebtHint();
                return false;
            }
            if (primary_score >= index_score) {
                return try self.core.primary_store_owner.runLsmMaintenanceStep();
            }
            return try self.core.index_manager.runLsmMaintenanceStep();
        }

        pub fn runPrimaryLsmMaintenanceStep(self: *DB) !bool {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.primary_store_owner.runLsmMaintenanceStep();
        }

        pub fn runPrimaryLsmMaintenanceStepBestEffort(self: *DB) !bool {
            if (!self.core.tryLockApplyExclusive()) return false;
            defer self.core.unlockApply();

            if (try self.core.primary_store_owner.runDueLsmObsoleteReclaim()) return true;
            const primary_score = self.core.primary_store_owner.lsmMaintenanceDebtHint();
            const primary_reclaim_due = if (self.core.primary_store_owner.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay_ns| delay_ns == 0 else false;
            if (primary_score == 0 and !primary_reclaim_due) return false;
            if (try self.core.primary_store_owner.runLsmMaintenanceStepBestEffort()) return true;
            self.core.primary_store_owner.refreshLsmMaintenanceDebtHint();
            return false;
        }

        pub fn runLsmMaintenanceStepBestEffort(self: *DB) !bool {
            if (!self.core.tryLockApplyExclusive()) return false;
            defer self.core.unlockApply();

            if (self.core.index_manager.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay_ns| {
                if (delay_ns == 0 and try self.core.index_manager.runLsmMaintenanceStepBestEffort()) return true;
            }
            if (self.core.primary_store_owner.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay_ns| {
                if (delay_ns == 0 and try self.core.primary_store_owner.runLsmMaintenanceStepBestEffort()) return true;
            }

            const primary_score = self.core.primary_store_owner.lsmMaintenanceDebtHint();
            const index_score = self.core.index_manager.lsmMaintenanceDebtHint();
            if (index_score > primary_score) {
                if (try self.core.index_manager.runLsmMaintenanceStepBestEffort()) return true;
            }
            if (primary_score > 0) {
                if (try self.core.primary_store_owner.runLsmMaintenanceStepBestEffort()) return true;
            }
            if (index_score > 0) {
                if (try self.core.index_manager.runLsmMaintenanceStepBestEffort()) return true;
            }
            if (primary_score > 0 or index_score > 0) {
                self.core.primary_store_owner.refreshLsmMaintenanceDebtHint();
                self.core.index_manager.refreshLsmMaintenanceDebtHint();
            }
            return false;
        }

        pub fn runLsmMaintenanceUntilIdle(self: *DB) !usize {
            var steps: usize = 0;
            while (try Self.runLsmMaintenanceStep(self)) {
                steps += 1;
            }
            return steps;
        }

        pub fn retryQuarantinedIndexLoads(self: *DB, force: bool) !index_manager_mod.IndexManager.QuarantineRetryResult {
            return try DB.ArtifactRepairCallbacks.retry_quarantined_index_loads(self, force);
        }

        const quarantine_retry_poll_ns: u64 = 10 * std.time.ns_per_s;
        const quarantine_retry_sleep_slice_ns: u64 = 25 * std.time.ns_per_ms;

        pub fn startQuarantineRetryWorkerIfNeeded(self: *DB) void {
            if (comptime builtin.single_threaded or builtin.os.tag == .freestanding) return;
            // Tests drive retries deterministically via retryQuarantinedIndexLoads.
            if (comptime builtin.is_test) return;
            if (self.open_mode != .writer) return;
            if (!self.core.index_manager.hasLoadFailures()) return;
            if (self.quarantine_retry_future != null) return;
            const io_impl = self.backend_runtime.io_impl orelse return;
            self.quarantine_retry_stop.store(false, .release);
            self.quarantine_retry_future = io_impl.io().concurrent(quarantineRetryWorkerMain, .{self}) catch |err| {
                std.log.warn("quarantine retry worker spawn failed: {}", .{err});
                return;
            };
        }

        pub fn stopQuarantineRetryWorker(self: *DB) void {
            self.quarantine_retry_stop.store(true, .release);
            if (self.quarantine_retry_future) |*future| {
                if (self.backend_runtime.io_impl) |io_impl| {
                    _ = future.await(io_impl.io());
                }
                self.quarantine_retry_future = null;
            }
        }

        fn sleepQuarantineRetryWorker(self: *DB) bool {
            var slept: u64 = 0;
            while (slept < quarantine_retry_poll_ns) : (slept += quarantine_retry_sleep_slice_ns) {
                if (self.quarantine_retry_stop.load(.acquire)) return false;
                db_internal.sleepNs(quarantine_retry_sleep_slice_ns);
            }
            return !self.quarantine_retry_stop.load(.acquire);
        }

        fn quarantineRetryWorkerMain(self: *DB) void {
            while (true) {
                if (!sleepQuarantineRetryWorker(self)) return;
                if (self.quarantine_retry_stop.load(.acquire)) return;
                const result = self.retryQuarantinedIndexLoads(false) catch |err| {
                    std.log.warn("quarantine retry pass failed: {}", .{err});
                    continue;
                };
                if (result.remaining == 0) return;
            }
        }

        pub fn runDueLsmObsoleteReclaimUntilIdle(self: *DB, max_steps: usize) !usize {
            var steps: usize = 0;
            while (steps < max_steps) : (steps += 1) {
                var progressed = false;
                if (try self.core.primary_store_owner.runDueLsmObsoleteReclaim()) progressed = true;
                if (try self.core.index_manager.runLsmObsoleteReclaimDue()) progressed = true;

                if (!progressed) {
                    const wake_due = if (Self.nextLsmMaintenanceWakeDelayNsBestEffort(self)) |delay_ns| delay_ns == 0 else false;
                    if (!wake_due) break;
                    if (!try Self.runLsmMaintenanceStep(self)) break;
                }
            }
            return steps;
        }

        fn engineBatch(ptr: *anyopaque, req: types.BatchRequest) !void {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.batch(req);
        }

        fn engineLookup(ptr: *anyopaque, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.lookup(alloc, key, opts);
        }

        fn engineScan(ptr: *anyopaque, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !types.ScanResult {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.scan(alloc, from_key, to_key, opts);
        }

        fn engineSearch(ptr: *anyopaque, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.search(alloc, req);
        }

        fn engineStats(ptr: *anyopaque, alloc: Allocator) !types.DBStats {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.stats(alloc);
        }

        fn engineListIndexes(ptr: *anyopaque, alloc: Allocator) ![]types.IndexConfig {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.listIndexes(alloc);
        }

        fn engineListEnrichments(ptr: *anyopaque, alloc: Allocator) ![]types.EnrichmentConfig {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.listEnrichments(alloc);
        }

        fn maintenanceDriverPendingWorkStats(ptr: *anyopaque) db_core.PendingWorkStats {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return self.pendingWorkStats();
        }

        fn maintenanceDriverRunDerivedUntil(ptr: *anyopaque, sequence: u64) !void {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.runDerivedUntil(sequence);
        }

        fn maintenanceDriverRunEnrichmentUntil(ptr: *anyopaque, sequence: u64) !void {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.runEnrichmentUntil(sequence);
        }

        fn maintenanceDriverRunMaintenanceUntil(ptr: *anyopaque, sequence: u64) !void {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.runMaintenanceUntil(sequence, .{});
        }

        fn maintenanceDriverRunUntilIdle(ptr: *anyopaque) !void {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try self.runUntilIdle();
        }
    };
}

fn ensureReplayFloor(store: anytype, next_sequence: u64) !void {
    try store.ensureReplayNextSequenceAtLeast(next_sequence);
}

fn dbHbcCacheKindStats(cache_stats: anytype) types.HbcCacheKindStats {
    return .{
        .used_bytes = cache_stats.used_bytes,
        .peak_bytes = cache_stats.peak_bytes,
        .insertions = cache_stats.insertions,
        .admission_skips = cache_stats.admission_skips,
        .evictions = cache_stats.evictions,
    };
}

fn dbHbcCacheStats(cache_stats: anytype) types.HbcCacheStats {
    return .{
        .total_bytes = cache_stats.total_bytes,
        .accounted_bytes = cache_stats.accounted_bytes,
        .node = dbHbcCacheKindStats(cache_stats.node),
        .quantized = dbHbcCacheKindStats(cache_stats.quantized),
        .vector = dbHbcCacheKindStats(cache_stats.vector),
        .metadata = dbHbcCacheKindStats(cache_stats.metadata),
    };
}

fn dbHbcPostingStats(backlog: hbc_mod.PostingBacklogStats, profile: hbc_mod.WriteProfile) types.HbcPostingStats {
    return .{
        .scanned_nodes = backlog.scanned_nodes,
        .scanned_postings = backlog.scanned_postings,
        .dirty_postings = backlog.dirty_postings,
        .centroid_dirty_postings = backlog.centroid_dirty_postings,
        .payload_dirty_postings = backlog.payload_dirty_postings,
        .max_centroid_version_lag = backlog.max_centroid_version_lag,
        .max_payload_version_lag = backlog.max_payload_version_lag,
        .max_mutation_version = backlog.max_mutation_version,
        .skipped_missing = backlog.skipped_missing,
        .maintenance_scanned_nodes = profile.posting_maintenance_scanned_nodes,
        .maintenance_scanned_postings = profile.posting_maintenance_scanned_postings,
        .maintenance_dirty_postings = profile.posting_maintenance_dirty_postings,
        .maintenance_repaired_postings = profile.posting_maintenance_repaired_postings,
        .maintenance_centroid_refreshed = profile.posting_maintenance_centroid_refreshed,
        .maintenance_payload_refreshed = profile.posting_maintenance_payload_refreshed,
        .maintenance_ancestor_refresh_roots = profile.posting_maintenance_ancestor_refresh_roots,
        .maintenance_split_postings = profile.posting_maintenance_split_postings,
        .maintenance_merged_postings = profile.posting_maintenance_merged_postings,
        .maintenance_boundary_reassigned_vectors = profile.posting_maintenance_boundary_reassigned_vectors,
        .lazy_centroid_deferrals = profile.posting_lazy_centroid_deferrals,
        .lazy_payload_deferrals = profile.posting_lazy_payload_deferrals,
        .lazy_ancestor_deferrals = profile.posting_lazy_ancestor_deferrals,
    };
}

pub fn cloneAlgebraicCandidateStatusAlloc(
    alloc: Allocator,
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    decision: []const u8,
    observation_count: u64,
    estimated_scan_rows_saved: u64,
    estimated_write_cost: u64,
    estimated_tensor_rows: u64,
    estimated_storage_bytes: u64,
    estimated_write_amplification: u64,
    score: i128,
    idle_miss_count: u64,
    generation: u64,
) !types.AlgebraicCandidateStatus {
    const owned_recommendation = try alloc.dupe(u8, recommendation);
    errdefer alloc.free(owned_recommendation);
    const owned_materialization_id = try alloc.dupe(u8, materialization_id);
    errdefer alloc.free(owned_materialization_id);
    const owned_lifecycle = try alloc.dupe(u8, lifecycle);
    errdefer alloc.free(owned_lifecycle);
    const owned_decision = try alloc.dupe(u8, decision);
    errdefer alloc.free(owned_decision);
    return .{
        .recommendation = owned_recommendation,
        .materialization_id = owned_materialization_id,
        .lifecycle = owned_lifecycle,
        .decision = owned_decision,
        .observation_count = observation_count,
        .estimated_scan_rows_saved = estimated_scan_rows_saved,
        .estimated_write_cost = estimated_write_cost,
        .estimated_tensor_rows = estimated_tensor_rows,
        .estimated_storage_bytes = estimated_storage_bytes,
        .estimated_write_amplification = estimated_write_amplification,
        .score = score,
        .idle_miss_count = idle_miss_count,
        .generation = generation,
    };
}

pub fn cloneAlgebraicCandidateDecisionStatusAlloc(
    alloc: Allocator,
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    previous_decision: []const u8,
    decision: []const u8,
    observation_count: u64,
    estimated_scan_rows_saved: u64,
    estimated_write_cost: u64,
    score: i128,
    score_delta: i128,
    idle_miss_count: u64,
    generation: u64,
) !types.AlgebraicCandidateDecisionStatus {
    const owned_recommendation = try alloc.dupe(u8, recommendation);
    errdefer alloc.free(owned_recommendation);
    const owned_materialization_id = try alloc.dupe(u8, materialization_id);
    errdefer alloc.free(owned_materialization_id);
    const owned_lifecycle = try alloc.dupe(u8, lifecycle);
    errdefer alloc.free(owned_lifecycle);
    const owned_previous_decision = try alloc.dupe(u8, previous_decision);
    errdefer alloc.free(owned_previous_decision);
    const owned_decision = try alloc.dupe(u8, decision);
    errdefer alloc.free(owned_decision);
    return .{
        .recommendation = owned_recommendation,
        .materialization_id = owned_materialization_id,
        .lifecycle = owned_lifecycle,
        .previous_decision = owned_previous_decision,
        .decision = owned_decision,
        .observation_count = observation_count,
        .estimated_scan_rows_saved = estimated_scan_rows_saved,
        .estimated_write_cost = estimated_write_cost,
        .score = score,
        .score_delta = score_delta,
        .idle_miss_count = idle_miss_count,
        .generation = generation,
    };
}

pub fn cloneAlgebraicProgressStatusAlloc(
    alloc: Allocator,
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    target_sequence: u64,
    applied_sequence: u64,
    rows_processed: u64,
    target_rows: u64,
) !types.AlgebraicProgressStatus {
    const owned_recommendation = try alloc.dupe(u8, recommendation);
    errdefer alloc.free(owned_recommendation);
    const owned_materialization_id = try alloc.dupe(u8, materialization_id);
    errdefer alloc.free(owned_materialization_id);
    const owned_lifecycle = try alloc.dupe(u8, lifecycle);
    errdefer alloc.free(owned_lifecycle);
    return .{
        .recommendation = owned_recommendation,
        .materialization_id = owned_materialization_id,
        .lifecycle = owned_lifecycle,
        .target_sequence = target_sequence,
        .applied_sequence = applied_sequence,
        .rows_processed = rows_processed,
        .target_rows = target_rows,
    };
}

pub fn cloneGraphMetricBuildPageStatusesFromGraph(
    alloc: Allocator,
    source: []const graph_mod.GraphIndex.GraphMetricBuildPageStatus,
) ![]types.GraphMetricBuildPageStatus {
    if (source.len == 0) return @constCast((&[_]types.GraphMetricBuildPageStatus{})[0..]);
    const out = try alloc.alloc(types.GraphMetricBuildPageStatus, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*page| page.deinit(alloc);
        alloc.free(out);
    }
    for (source, 0..) |page, i| {
        const worker_id = if (page.worker_id.len > 0) try alloc.dupe(u8, page.worker_id) else "";
        var worker_id_moved = false;
        errdefer if (!worker_id_moved and worker_id.len > 0) alloc.free(worker_id);
        const cursor = if (page.cursor.len > 0) try alloc.dupe(u8, page.cursor) else "";
        var cursor_moved = false;
        errdefer if (!cursor_moved and cursor.len > 0) alloc.free(cursor);
        const last_error = if (page.last_error.len > 0) try alloc.dupe(u8, page.last_error) else "";
        var last_error_moved = false;
        errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
        out[i] = .{
            .phase = page.phase,
            .iteration = page.iteration,
            .page_id = page.page_id,
            .state = page.state,
            .range_kind = page.range_kind,
            .worker_id = worker_id,
            .lease_expires_at_ms = page.lease_expires_at_ms,
            .attempt = page.attempt,
            .cursor = cursor,
            .completed_units = page.completed_units,
            .total_units = page.total_units,
            .last_error = last_error,
        };
        worker_id_moved = true;
        cursor_moved = true;
        last_error_moved = true;
        initialized += 1;
    }
    return out;
}

fn applyGraphAlgebraicRuntimeStats(item: *types.DBIndexStats, graph_index: *const graph_mod.GraphIndex) void {
    const algebraic_graph = graph_index.algebraicTraversalRuntimeStats();
    item.algebraic_graph_traversal_attempt_count = algebraic_graph.attempt_count;
    item.algebraic_graph_traversal_proven_count = algebraic_graph.proven_count;
    item.algebraic_graph_traversal_rejected_count = algebraic_graph.rejected_count;
    item.algebraic_graph_traversal_fallback_count = algebraic_graph.fallback_count;
    item.algebraic_graph_traversal_result_node_count = algebraic_graph.result_node_count;
}

fn populateGraphMetricStatusStats(alloc: Allocator, item: *types.DBIndexStats, graph_index: *graph_mod.GraphIndex) !void {
    if (graph_index.metric_configs.len == 0) return;
    const statuses = try alloc.alloc(types.GraphMetricStatus, graph_index.metric_configs.len);
    var initialized: usize = 0;
    errdefer {
        for (statuses[0..initialized]) |*status| status.deinit(alloc);
        alloc.free(statuses);
    }
    for (graph_index.metric_configs, 0..) |cfg, i| {
        var status = try graph_index.graphMetricStatus(cfg.name);
        defer status.deinit(graph_index.alloc);
        const name = try alloc.dupe(u8, status.name);
        var name_moved = false;
        errdefer if (!name_moved) alloc.free(name);
        var edge_filter = try status.edge_filter.cloneAlloc(alloc);
        var edge_filter_moved = false;
        errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
        const recent_events = if (status.recent_events.len > 0)
            try alloc.dupe(graph_mod.GraphIndex.GraphMetricEvent, status.recent_events)
        else
            @constCast((&[_]graph_mod.GraphIndex.GraphMetricEvent{})[0..]);
        var recent_events_moved = false;
        errdefer if (!recent_events_moved and recent_events.len > 0) alloc.free(recent_events);
        const last_error = if (status.last_error.len > 0) try alloc.dupe(u8, status.last_error) else "";
        var last_error_moved = false;
        errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
        const build_worker_id = if (status.build_worker_id.len > 0) try alloc.dupe(u8, status.build_worker_id) else "";
        var build_worker_id_moved = false;
        errdefer if (!build_worker_id_moved and build_worker_id.len > 0) alloc.free(build_worker_id);
        const build_cursor = if (status.build_cursor.len > 0) try alloc.dupe(u8, status.build_cursor) else "";
        var build_cursor_moved = false;
        errdefer if (!build_cursor_moved and build_cursor.len > 0) alloc.free(build_cursor);
        const build_pages = try cloneGraphMetricBuildPageStatusesFromGraph(alloc, status.build_pages);
        var build_pages_moved = false;
        errdefer if (!build_pages_moved) {
            for (build_pages) |*page| page.deinit(alloc);
            if (build_pages.len > 0) alloc.free(build_pages);
        };
        statuses[i] = .{
            .name = name,
            .state = status.state,
            .phase = status.phase,
            .edge_filter = edge_filter,
            .metadata_version = status.metadata_version,
            .maintenance_paused = status.maintenance_paused,
            .build_queued = status.build_queued,
            .published_generation = status.published_generation,
            .edge_generation = status.edge_generation,
            .target_edge_generation = status.target_edge_generation,
            .queued_generation = status.queued_generation,
            .building_generation = status.building_generation,
            .build_job_id = status.build_job_id,
            .build_started_at_ms = status.build_started_at_ms,
            .build_iteration = status.build_iteration,
            .build_lease_expires_at_ms = status.build_lease_expires_at_ms,
            .build_worker_id = build_worker_id,
            .build_cursor = build_cursor,
            .build_completed_units = status.build_completed_units,
            .build_total_units = status.build_total_units,
            .build_pages = build_pages,
            .build_pages_truncated = status.build_pages_truncated,
            .retry_count = status.retry_count,
            .last_error = last_error,
            .progress = status.progress,
            .converged = status.converged,
            .iterations_completed = status.iterations_completed,
            .delta = status.delta,
            .computed_at_ms = status.computed_at_ms,
            .last_event = status.last_event,
            .recent_events = recent_events,
        };
        name_moved = true;
        edge_filter_moved = true;
        recent_events_moved = true;
        last_error_moved = true;
        build_worker_id_moved = true;
        build_cursor_moved = true;
        build_pages_moved = true;
        initialized += 1;
    }
    item.graph_metric_status = statuses;
}

fn replaceOptionalOwnedStringBestEffort(alloc: std.mem.Allocator, slot: *?[]const u8, value: ?[]const u8) void {
    if (value) |raw| {
        const owned = alloc.dupe(u8, raw) catch return;
        if (slot.*) |previous| alloc.free(previous);
        slot.* = owned;
    } else {
        if (slot.*) |previous| alloc.free(previous);
        slot.* = null;
    }
}

fn writeRawProjectionCheckpointSidecarForTest(path: []const u8, raw: []const u8) !void {
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = path,
        .data = raw,
    });
}

test "db lifecycle open borrows shared backend runtime" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var first_path_buf: [256]u8 = undefined;
    const first_path = TestHelpers.tempPath(&first_path_buf);
    defer TestHelpers.cleanupTempDir(first_path);

    var second_path_buf: [256]u8 = undefined;
    const second_path = TestHelpers.tempPath(&second_path_buf);
    defer TestHelpers.cleanupTempDir(second_path);

    var first = try DB.open(alloc, std.mem.span(first_path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
    });
    defer first.close();

    var second = try DB.open(alloc, std.mem.span(second_path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
    });
    defer second.close();

    try std.testing.expectEqual(runtime.ptr(), first.backend_runtime);
    try std.testing.expectEqual(runtime.ptr(), second.backend_runtime);
    try std.testing.expect(first.owned_backend_runtime == null);
    try std.testing.expect(second.owned_backend_runtime == null);
    try std.testing.expect(first.backend_owner_id != 0);
    try std.testing.expect(second.backend_owner_id != 0);
    try std.testing.expect(first.backend_owner_id != second.backend_owner_id);
    // Each open allocates four owner ids from the shared runtime: backend,
    // repair cleanup, relational index workers, and algebraic HLL maintenance.
    // The runtime also reserves a shared retired-generation cleanup owner.
    try std.testing.expectEqual(
        second.algebraic_hll_owner_id + 1,
        try runtime.ptr().allocOwnerId(),
    );
}

test "db close retires runtime owners for memory primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .mem = .{} },
    });
    const backend_owner_id = db.backend_owner_id;
    const repair_cleanup_owner_id = db.repair_cleanup_owner_id;
    const relational_index_worker_owner_id = db.relational_index_worker_owner_id;
    const algebraic_hll_owner_id = db.algebraic_hll_owner_id;
    db.close();

    const Fns = struct {
        fn run(_: *anyopaque) !void {}
        fn deinit(_: *anyopaque) void {}
    };
    var ctx: u8 = 0;
    try std.testing.expectError(error.BackgroundOwnerClosed, runtime.ptr().durable_jobs.submit(.{
        .owner_id = backend_owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
    try std.testing.expectError(error.BackgroundOwnerClosed, runtime.ptr().durable_jobs.submit(.{
        .owner_id = repair_cleanup_owner_id,
        .class = .cleanup,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
    try std.testing.expectError(error.BackgroundOwnerClosed, runtime.ptr().durable_jobs.submit(.{
        .owner_id = relational_index_worker_owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
    try std.testing.expectError(error.BackgroundOwnerClosed, runtime.ptr().durable_jobs.submit(.{
        .owner_id = algebraic_hll_owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
}

test "db inherits the resource manager capacity source" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const CapacityContext = struct {
        fn observe(_: *anyopaque) anyerror!resource_manager_mod.CapacityObservation {
            return .{ .available_bytes = 1234, .capacity_bytes = 5678 };
        }
    };
    var source_context: u8 = 0;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    try manager.installCapacitySource(.{
        .ptr = &source_context,
        .domain_id = 91,
        .observe = CapacityContext.observe,
    });

    var db = try DB.open(alloc, std.mem.span(path), .{ .resource_manager = &manager });
    defer db.close();
    const source = db.capacity_source orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u128, 91), source.domain_id);
    const observed = try source.current();
    try std.testing.expectEqual(@as(?u64, 1234), observed.available_bytes);
    try std.testing.expectEqual(@as(?u64, 5678), observed.capacity_bytes);
}

test "db lifecycle standalone resource manager governs dense cache and storage" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const resources = db.owned_resource_manager orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(resources, db.core.index_manager.resource_manager.?);
    try std.testing.expectEqual(resources, db.core.index_manager.dense_lsm_options.resource_manager.?);

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":8,\"external\":true}",
    });
    const dense = db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(resources, dense.index.resource_manager.?);
}

test "db lifecycle fallback resource manager does not bind caller owned lsm cache" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var cache = lsm_backend_mod.Cache.init(alloc, lsm_backend_mod.DefaultCacheSizeBytes);
    defer cache.deinit();
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_cache = &cache,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try std.testing.expect(db.owned_resource_manager != null);
        try std.testing.expect(cache.resource_manager == null);
    }
    try std.testing.expect(cache.resource_manager == null);
}

test "db repair capacity treats unsupported observation as accounting only" {
    const DB = @import("mod.zig").DB;
    const Unsupported = struct {
        fn observe(_: *anyopaque) anyerror!resource_manager_mod.CapacityObservation {
            return error.UnsupportedPlatform;
        }
    };
    var context: u8 = 0;
    const observed = try DB.ArtifactRepairCallbacks.repair_capacity_observation(.{
        .capacity_source = .{
            .ptr = &context,
            .domain_id = 17,
            .observe = Unsupported.observe,
        },
    });
    try std.testing.expectEqual(@as(?u64, null), observed.available_bytes);
    try std.testing.expectEqual(@as(?u64, null), observed.capacity_bytes);
}

test "db lifecycle open downgrades borrowed manual backend runtime to manual executor" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
    });
    defer db.close();

    try std.testing.expectEqual(runtime.ptr(), db.backend_runtime);
    try std.testing.expect(db.owned_backend_runtime == null);
}

test "db lifecycle open wires algebraic HLL maintenance lane and adaptively backfills sketches" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    // A manual backend runtime drives durable jobs inline (synchronously on
    // submit), so a lane-scheduled HLL backfill completes within the call that
    // promotes the sketch — the path a read-mostly cardinality workload takes.
    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const cfg =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "docs",
        \\  "group_fields": [{"name":"product","path":"product","type":"string"}],
        \\  "materializations": [],
        \\  "adaptive": {"observe": true, "lazy_materialization": true, "min_observations": 2}
        \\}
    ;

    var adaptive_name_buf: [64]u8 = undefined;
    var adaptive_name_len: usize = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .backend_runtime = runtime.ptr(),
        });
        defer db.close();

        try db.addIndex(.{ .name = "alg", .kind = .algebraic, .config_json = cfg });
        try db.batch(.{
            .writes = &.{
                .{ .key = "d1", .value = "{\"product\":\"pen\"}" },
                .{ .key = "d2", .value = "{\"product\":\"book\"}" },
                .{ .key = "d3", .value = "{\"product\":\"pen\"}" },
                .{ .key = "d4", .value = "{\"product\":\"notebook\"}" },
            },
            .sync_level = .full_index,
        });

        const entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        const index = &entry.index;

        // DB.open must thread the durable-jobs lane into the algebraic index so
        // background HLL maintenance has somewhere to run on a live server.
        try std.testing.expect(index.hll_maintenance_lane != null);

        const adaptive_name = try index.hllAdaptiveNameAlloc(null, "product");
        defer alloc.free(adaptive_name);
        @memcpy(adaptive_name_buf[0..adaptive_name.len], adaptive_name);
        adaptive_name_len = adaptive_name.len;

        // Reads only record observations — they never promote — so no sketch and
        // no approximate total exist after two recurring cardinality queries.
        index.observeCardinalityForAdaptive(db.core.store, null, "product");
        index.observeCardinalityForAdaptive(db.core.store, null, "product");
        try std.testing.expect(!index.hllRegistryContains(adaptive_name));
        try std.testing.expect((try index.approxCardinalityTotalForFieldAlloc(db.core.store, "product", &.{}, null)) == null);

        // The leader-gated adaptive maintenance pass promotes the over-threshold
        // observation and backfills it, so the next read resolves an approximate
        // distinct-product count (4 docs, 3 distinct products).
        _ = try db.evaluateAlgebraicAdaptiveCandidates();
        try std.testing.expect(index.hllRegistryContains(adaptive_name));
        const estimate = (try index.approxCardinalityTotalForFieldAlloc(db.core.store, "product", &.{}, null)) orelse
            return error.TestUnexpectedResult;
        try std.testing.expect(@max(estimate, 3) - @min(estimate, 3) <= 1);
    }

    // The promotion is durable: a freshly reopened DB reloads the adaptive marker
    // through the open-site wiring (attachHllMaintenance + loadAdaptiveHllCardinalities)
    // and keeps serving the approximate count without re-observing.
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .backend_runtime = runtime.ptr(),
        });
        defer db.close();

        const entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        const index = &entry.index;
        const adaptive_name = adaptive_name_buf[0..adaptive_name_len];
        try std.testing.expect(index.hllRegistryContains(adaptive_name));
        const reopened = (try index.approxCardinalityTotalForFieldAlloc(db.core.store, "product", &.{}, null)) orelse
            return error.TestUnexpectedResult;
        try std.testing.expect(@max(reopened, 3) - @min(reopened, 3) <= 1);
    }
}

test "db lifecycle open text merge enabled requires backend runtime io" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    try std.testing.expectError(error.MissingBackendRuntimeIo, DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .text_merge = .{ .enabled = true },
    }));
}

test "db lifecycle open enrichment enabled requires backend runtime io" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    try std.testing.expectError(error.MissingBackendRuntimeIo, DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    }));
}

test "db enrichment reconfigure preserves active runtime when replacement cannot initialize" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();
    const original_backend_runtime = db.backend_runtime;
    defer db.backend_runtime = original_backend_runtime;

    const original_runtime = db.enrichment_runtime orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(original_runtime, db.async_context.enrichment_runtime.?);
    try std.testing.expect(db.async_context.enrichment_desired_running.load(.acquire));
    var manual_runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer manual_runtime.deinit();

    db.backend_runtime = manual_runtime.ptr();
    var replacement_embedder = embedder_mod.DeterministicDenseEmbedder{};
    try std.testing.expectError(error.MissingBackendRuntimeIo, db.reconfigureEnrichmentRuntime(.{
        .dense_embedder = replacement_embedder.interface(),
    }));
    try std.testing.expectEqual(original_runtime, db.enrichment_runtime.?);
    try std.testing.expectEqual(original_runtime, db.async_context.enrichment_runtime.?);
    try std.testing.expect(db.async_context.enrichment_desired_running.load(.acquire));
}

test "db lifecycle open ttl cleanup enabled requires backend runtime io" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    try std.testing.expectError(error.MissingBackendRuntimeIo, DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .ttl_cleanup = .{ .enabled = true },
    }));
}

test "db lifecycle open transaction recovery enabled requires backend runtime io" {
    const DB = @import("mod.zig").DB;
    const TestTransactionRecoveryResolver = TestHelpers.TestTransactionRecoveryResolver;
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var resolver_ctx: u8 = 0;
    try std.testing.expectError(error.MissingBackendRuntimeIo, DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .transaction_recovery = .{
            .enabled = true,
            .resolver_ctx = &resolver_ctx,
            .resolve_participant_fn = TestTransactionRecoveryResolver.resolve,
        },
    }));
}

test "db lifecycle open status_only reads index catalog without loading index state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[0.1,0.2,0.3]}" },
            },
            .sync_level = .write,
        });
        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });
        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3}",
        });
    }

    {
        var status_db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .transaction_recovery = .{ .enabled = false },
            .text_merge = .{ .enabled = false },
        });
        defer status_db.close();

        try std.testing.expect(status_db.core.textIndex("ft_v1") == null);
        try std.testing.expect(status_db.core.denseIndex("dv_v1") == null);

        const indexes = try status_db.listIndexes(alloc);
        defer types.freeIndexConfigs(alloc, indexes);
        try std.testing.expectEqual(@as(usize, 2), indexes.len);
        var full_text_generation: u64 = 0;
        for (indexes) |config| {
            if (std.mem.eql(u8, config.name, "ft_v1")) {
                full_text_generation = config.coverage_generation;
                break;
            }
        }
        try std.testing.expect(full_text_generation != 0);

        const stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_count);
        try std.testing.expectEqual(@as(u32, 2), stats.index_count);
        try std.testing.expectEqual(@as(usize, 2), stats.indexes.len);
        var saw_dense = false;
        for (stats.indexes) |item| {
            if (!std.mem.eql(u8, item.name, "dv_v1")) continue;
            saw_dense = true;
            try std.testing.expectEqual(@as(u64, 1), item.doc_count);
            try std.testing.expect(item.node_count > 0);
            try std.testing.expect(item.root_node > 0);
        }
        try std.testing.expect(saw_dense);

        const rebuild_root = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1", .{std.mem.span(path)});
        defer alloc.free(rebuild_root);
        const stale_generation = if (full_text_generation == 1) 2 else full_text_generation - 1;
        const stale_state = backfill_state_mod.RebuildState.initOwned(rebuild_root, null, stale_generation);
        defer stale_state.clear() catch {};
        try stale_state.update("doc:stale");

        const stale_stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, stale_stats);
        const stale_ft = blk: {
            for (stale_stats.indexes) |item| {
                if (std.mem.eql(u8, item.name, "ft_v1")) break :blk item;
            }
            return error.TestUnexpectedResult;
        };
        try std.testing.expect(!stale_ft.backfill_active);

        const current_state = backfill_state_mod.RebuildState.initOwned(rebuild_root, null, full_text_generation);
        defer current_state.clear() catch {};
        try current_state.update("doc:a");
        const rebuilding_stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, rebuilding_stats);
        const rebuilding_ft = blk: {
            for (rebuilding_stats.indexes) |item| {
                if (std.mem.eql(u8, item.name, "ft_v1")) break :blk item;
            }
            return error.TestUnexpectedResult;
        };
        try std.testing.expect(rebuilding_ft.backfill_active);

        try current_state.clear();
        const current_state_path = try current_state.pathAlloc(alloc);
        defer alloc.free(current_state_path);
        const legacy_state = backfill_state_mod.RebuildState.init(rebuild_root);
        const legacy_state_path = try legacy_state.pathAlloc(alloc);
        defer alloc.free(legacy_state_path);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{
            .sub_path = legacy_state_path,
            .data = "doc:legacy",
        });

        const legacy_stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, legacy_stats);
        const legacy_ft = blk: {
            for (legacy_stats.indexes) |item| {
                if (std.mem.eql(u8, item.name, "ft_v1")) break :blk item;
            }
            return error.TestUnexpectedResult;
        };
        try std.testing.expect(legacy_ft.backfill_active);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, current_state_path, .{}));
        const preserved_legacy = try std.Io.Dir.cwd().readFileAlloc(
            std.testing.io,
            legacy_state_path,
            alloc,
            .limited(1024),
        );
        defer alloc.free(preserved_legacy);
        try std.testing.expectEqualStrings("doc:legacy", preserved_legacy);
    }
}

test "db lifecycle stats marks stale dense status coverage regression degraded" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[0.1,0.2,0.3]}" },
        },
        .sync_level = .full_index,
    });

    const status_key = try indexStatusKeyAlloc(alloc, "dv_v1");
    defer alloc.free(status_key);
    var stale_status: [64]u8 = undefined;
    encodeIndexStatusSnapshot(.{
        .kind = .dense_vector,
        .doc_count = 2,
        .node_count = 2,
        .root_node = 1,
    }, &stale_status);
    try db.core.store.put(status_key, &stale_status);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.repair_degraded);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqualStrings("dv_v1", stats.indexes[0].name);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].doc_count);
    try std.testing.expect(stats.indexes[0].repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].repair_issue_count);
}

test "db lifecycle open query_readonly opens empty declared graph index" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json = "{}",
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{ .open_mode = .query_readonly });
    defer reopened.close();

    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "relations_graph",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"mentions"}, .direction = .out },
    };
    var result = try reopened.search(alloc, .{ .graph_queries = &.{.{ .name = "mentions", .query = query }} });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 0), result.graph_results[0].total_hits);
    try std.testing.expectEqual(@as(usize, 0), result.graph_results[0].nodes.len);
}

test "db lifecycle open query_readonly skips pending derived replay on reopen" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });

        var dense_embeddings = try alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.reserveNextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, appended_sequence, encoded);
    }

    {
        var reopened_without_replay = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened_without_replay.close();

        const skipped_applied = try reopened_without_replay.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expectEqual(@as(u64, 0), skipped_applied);
        try std.testing.expect(!reopened_without_replay.pendingWorkStats().has_async_indexes);

        const replay_debt = try reopened_without_replay.listDerivedReplayDebt(alloc);
        defer {
            for (replay_debt) |*status| status.deinit(alloc);
            alloc.free(replay_debt);
        }
        try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
        try std.testing.expectEqualStrings("dv_v1", replay_debt[0].index_name);
        try std.testing.expectEqual(types.IndexKind.dense_vector, replay_debt[0].kind);
        try std.testing.expectEqual(@as(u64, 0), replay_debt[0].applied_sequence);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
        try std.testing.expect(replay_debt[0].catch_up_required);

        const skipped_stats = try reopened_without_replay.stats(alloc);
        defer types.freeDBStats(alloc, skipped_stats);
        try std.testing.expectEqual(@as(usize, 1), skipped_stats.indexes.len);
        try std.testing.expectEqualStrings("dv_v1", skipped_stats.indexes[0].name);
        try std.testing.expectEqual(@as(u64, 0), skipped_stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, skipped_stats.indexes[0].replay_target_sequence);
        try std.testing.expect(skipped_stats.indexes[0].replay_catch_up_required);

        var skipped_result = try reopened_without_replay.search(alloc, .{
            .index_name = "dv_v1",
            .dense = .{
                .vector = &[_]f32{ 1, 0 },
                .k = 1,
            },
            .limit = 1,
        });
        defer skipped_result.deinit();
        try std.testing.expectEqual(@as(u32, 0), skipped_result.total_hits);
    }

    try std.testing.expectEqual(@as(u64, 1), appended_sequence);

    {
        var reopened_with_replay = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened_with_replay.close();

        const replay_debt = try reopened_with_replay.listDerivedReplayDebt(alloc);
        defer {
            for (replay_debt) |*status| status.deinit(alloc);
            alloc.free(replay_debt);
        }
        try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].applied_sequence);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
        try std.testing.expect(!replay_debt[0].catch_up_required);

        const replayed_stats = try reopened_with_replay.stats(alloc);
        defer types.freeDBStats(alloc, replayed_stats);
        try std.testing.expectEqual(@as(usize, 1), replayed_stats.indexes.len);
        try std.testing.expectEqual(appended_sequence, replayed_stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, replayed_stats.indexes[0].replay_target_sequence);
        try std.testing.expect(!replayed_stats.indexes[0].replay_catch_up_required);
    }
}

test "db lifecycle diagnostics load relational generation records for derived access methods" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"body":{"type":"text"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"body_text_idx","owner_kind":"relational_column","owner_name":"body","access_method":"text_search","method_config":{"type":"full_text","field":"body","analyzer":"standard"},"columns":["body"],"lifecycle":"catching_up","generation":7,"schema_fingerprint":"secondary-index-v1:body_text","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"catching_up","lag":19,"ready_watermark":101}},{"name":"schema_alg_idx","owner_kind":"table","owner_name":"__antfly_table__","access_method":"algebraic_filter","method_config":{"type":"algebraic","derive_from_schema":true},"lifecycle":"building","generation":9,"schema_fingerprint":"secondary-index-v1:schema_alg","generation_record":{"generation":9,"owner_ranges":[],"lifecycle":"building","lag":23,"ready_watermark":144}}]}
    ;
    const algebraic_config =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "docs",
        \\  "group_fields": [{"name":"status","path":"status","type":"string"}],
        \\  "materializations": []
        \\}
    ;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.addIndex(.{ .name = "body_text_idx", .kind = .full_text, .config_json = "{}" });
    try db.addIndex(.{ .name = "schema_alg_idx", .kind = .algebraic, .config_json = algebraic_config });

    const Expect = struct {
        name: []const u8,
        kind: types.IndexKind,
        lifecycle: schema_mod.RelationalIndexLifecycle,
        generation: u64,
        lag: u64,
        ready_watermark: u64,

        fn assertInStats(self: @This(), stats: types.DBStats) !void {
            for (stats.indexes) |item| {
                if (!std.mem.eql(u8, item.name, self.name)) continue;
                try std.testing.expectEqual(self.kind, item.kind);
                try std.testing.expect(item.relational_generation_present);
                try std.testing.expect(item.relational_generation_record_valid);
                try std.testing.expectEqual(self.generation, item.relational_generation);
                try std.testing.expectEqual(self.lifecycle, item.relational_generation_lifecycle);
                try std.testing.expectEqual(self.lag, item.relational_generation_lag);
                try std.testing.expectEqual(self.ready_watermark, item.relational_generation_ready_watermark);
                try std.testing.expect(item.relational_generation_catch_up_required);
                return;
            }
            return error.TestUnexpectedResult;
        }
    };
    const expected = [_]Expect{
        .{
            .name = "body_text_idx",
            .kind = .full_text,
            .lifecycle = .catching_up,
            .generation = 7,
            .lag = 19,
            .ready_watermark = 101,
        },
        .{
            .name = "schema_alg_idx",
            .kind = .algebraic,
            .lifecycle = .building,
            .generation = 9,
            .lag = 23,
            .ready_watermark = 144,
        },
    };

    const live_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, live_stats);
    const diagnostic_stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, diagnostic_stats);

    for (expected) |case| {
        try case.assertInStats(live_stats);
        try case.assertInStats(diagnostic_stats);
    }
}

test "db lifecycle open query_readonly lsm primary opens physical backend read-only" {
    const db_mod = @import("mod.zig");
    const DB = db_mod.DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try db.sync(true);
    }

    var readonly = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .open_mode = .query_readonly,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer readonly.close();

    switch (readonly.core.primary_store_owner) {
        .lsm => |owner| {
            try std.testing.expect(owner.handle.backend.options.backend.read_only);
            try std.testing.expect(!owner.handle.backend.options.backend.create_if_missing);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectError(error.ReadOnly, readonly.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        .sync_level = .write,
    }));
    const NoopDocumentArtifactChildRangeDispatcher = struct {
        fn apply(_: *anyopaque, _: Allocator, _: @import("mod.zig").DocumentArtifactChildRangeDispatch) anyerror!void {}
    };
    var noop_dispatcher_state: u8 = 0;
    const noop_dispatcher = @import("mod.zig").DocumentArtifactChildRangeDispatcher{
        .ptr = &noop_dispatcher_state,
        .apply = NoopDocumentArtifactChildRangeDispatcher.apply,
    };
    var blocked_profile = @import("mod.zig").BatchProfile{};
    try std.testing.expectError(error.ReadOnly, readonly.batchProfiled(.{
        .writes = &.{.{ .key = "doc:profiled", .value = "{\"title\":\"beta\"}" }},
        .sync_level = .write,
    }, &blocked_profile));
    try std.testing.expectError(error.ReadOnly, readonly.batchWithDocumentArtifactChildRangeDispatcher(.{
        .writes = &.{.{ .key = "doc:dispatch", .value = "{\"title\":\"beta\"}" }},
        .sync_level = .write,
    }, noop_dispatcher));
    try std.testing.expectError(error.ReadOnly, readonly.batchWithoutRangeValidation(.{
        .writes = &.{.{ .key = "doc:norange", .value = "{\"title\":\"beta\"}" }},
        .sync_level = .write,
    }));
    try std.testing.expectError(error.ReadOnly, readonly.applyDocumentArtifactChildRangeBatch(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.beginBulkIngestSession());
    try std.testing.expectError(error.ReadOnly, readonly.finishBulkIngestSessionWithOptions(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.beginDenseAutoBulkIngestSession());
    try std.testing.expectError(error.ReadOnly, readonly.beginPrimaryStoreAutoBulkIngestSession());
    try std.testing.expectError(error.ReadOnly, readonly.finishPrimaryStoreAutoBulkIngestSessionWithOptions(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.finishDenseAutoBulkIngestSessionWithOptionsAndNotifyExecutor(.{}, false));
    try std.testing.expectError(error.ReadOnly, readonly.finishDenseAutoBulkIngestSessionWithOptions(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.rollDenseAutoBulkIngestSessionWithOptions(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.rollPrimaryStoreAutoBulkIngestSessionWithOptions(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.drainDocumentArtifactChildRangeOutbox(noop_dispatcher, 1));
    try std.testing.expectError(error.ReadOnly, readonly.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "asset", @as(types.DocumentArtifactChildRangePlacementUpdate, undefined)));
    try std.testing.expectError(error.ReadOnly, readonly.reprocessDocumentArtifact(alloc, "doc:a", "asset"));
    try std.testing.expectError(error.ReadOnly, readonly.reprocessDocumentArtifactRange(alloc, "asset", @as(types.DocumentArtifactTableReprocessRequest, undefined)));
    try std.testing.expectError(error.ReadOnly, readonly.updateRange(.{ .start = "doc:a", .end = "doc:z" }));
    try std.testing.expectError(error.ReadOnly, readonly.setSplitState(null));
    try std.testing.expectError(error.ReadOnly, readonly.clearSplitState());
    try std.testing.expectError(error.ReadOnly, readonly.setSplitDeltaFinalSeq(1));
    try std.testing.expectError(error.ReadOnly, readonly.clearSplitDeltaFinalSeq());
    try std.testing.expectError(error.ReadOnly, readonly.clearSplitDeltaEntries());
    try std.testing.expectError(error.ReadOnly, readonly.createShadowIndexManager("doc:m", "doc:z"));
    try std.testing.expectError(error.ReadOnly, readonly.closeShadowIndexManager());
    try std.testing.expectError(
        error.ReadOnly,
        readonly.split(.{ .start = "doc:a", .end = "doc:z" }, "doc:m", "dest1", "dest2", true),
    );
    try std.testing.expectError(error.ReadOnly, readonly.finalizeSplit(.{ .start = "doc:a", .end = "doc:m" }));
    try std.testing.expectError(error.ReadOnly, readonly.snapshot("blocked"));
    try std.testing.expectError(error.ReadOnly, readonly.sync(false));
    try std.testing.expectError(error.ReadOnly, readonly.syncIndexes(false));
    try std.testing.expectError(error.ReadOnly, readonly.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.repairRestoreRuntimeStateIfNeeded(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricMaintenanceForIdle());
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedMaintenanceForIdle(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricServiceMaintenanceJsonAlloc(alloc, "{}"));
    try std.testing.expectError(error.ReadOnly, readonly.refreshGraphMetric(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.ReadOnly, readonly.rebuildGraphMetric(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.ReadOnly, readonly.deleteGraphMetricMaterialization(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.ReadOnly, readonly.pauseGraphMetricMaintenance(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.ReadOnly, readonly.resumeGraphMetricMaintenance(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.ReadOnly, readonly.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "manual_degree", 1));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedWorkerPageStep("graph_idx", "manual_degree", "worker-a"));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedWorkerPageStepAt("graph_idx", "manual_degree", "worker-a", 1));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedCoordinatorStep("graph_idx", "manual_degree"));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedCoordinatorStepAt("graph_idx", "manual_degree", 1));
    try std.testing.expectError(error.ReadOnly, readonly.failGraphMetricPlannedBuild(alloc, "graph_idx", "manual_degree", error.TestExpectedError));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedDrain(alloc, "graph_idx", "manual_degree", 1, .{ .worker_ids = &.{"worker-a"} }));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedCoordinatorSweep(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.runGraphMetricPlannedWorkerSweep(.{ .worker_id = "worker-a" }));
    try std.testing.expectError(error.ReadOnly, readonly.runLsmMaintenanceStep());
    try std.testing.expectError(error.ReadOnly, readonly.retryQuarantinedIndexLoads(true));
    try std.testing.expectError(error.ReadOnly, readonly.runUntilIdle());
    try std.testing.expectError(error.ReadOnly, readonly.evaluateAlgebraicAdaptiveCandidates());
    try std.testing.expectError(error.ReadOnly, readonly.setSchema(.{ .version = 99 }));
    try std.testing.expectError(error.ReadOnly, readonly.applyTableSchemaJson(alloc, "{\"version\":99}", .{}));
    try std.testing.expectError(error.ReadOnly, readonly.setSchemaJson(alloc, "{\"version\":99}"));
    try std.testing.expectError(error.ReadOnly, readonly.reloadAlgebraicSchemaConfigs("{\"version\":99}"));
    try std.testing.expectError(error.ReadOnly, readonly.schemaRuntimeStageAlgebraicSchemaConfigsPending("{\"version\":99}"));
    const metadata_table_manager = @import("../../metadata/table_manager.zig");
    try std.testing.expectError(error.ReadOnly, readonly.applyLiteSqlTableRecord(alloc, .{
        .table_id = 99,
        .name = "blocked",
        .schema_json = "{\"version\":99}",
    }));
    try std.testing.expectError(error.ReadOnly, readonly.executeClaimedSchemaRewriteJob(alloc, @as(metadata_table_manager.SchemaRewriteJobRecord, undefined)));
    try std.testing.expectError(error.ReadOnly, readonly.addIndex(@as(types.IndexConfig, undefined)));
    try std.testing.expectError(error.ReadOnly, readonly.addEnrichment(@as(types.EnrichmentConfig, undefined)));
    try std.testing.expectError(error.ReadOnly, readonly.upsertEnrichment(@as(types.EnrichmentConfig, undefined)));
    try std.testing.expectError(error.ReadOnly, readonly.drainResolverBackfill());
    try std.testing.expectError(error.ReadOnly, readonly.compactTextIndexes());
    try std.testing.expectError(error.ReadOnly, readonly.drainScheduledTextMerges());
    try std.testing.expectError(error.ReadOnly, readonly.forceCompactTextIndexes());
    try std.testing.expectError(error.ReadOnly, readonly.bestEffortForceCompactTextIndexes());
    try std.testing.expectError(error.ReadOnly, readonly.deleteIndex("missing"));
    try std.testing.expectError(error.ReadOnly, readonly.deleteEnrichment(.asset, "missing"));
    try std.testing.expectError(error.ReadOnly, readonly.removeResolver("missing"));
    try std.testing.expectError(error.ReadOnly, readonly.rewriteEntityEdges(alloc, "missing", "a", "b"));
    try std.testing.expectError(
        error.ReadOnly,
        readonly.rebuildRelationalSecondaryIndexInRange("missing", 0, "doc:a", "doc:z"),
    );
    try std.testing.expectError(error.ReadOnly, readonly.repairRelationalColumnBackedIndexesInRange("doc:a", "doc:z"));
    try std.testing.expectError(error.ReadOnly, readonly.repairForeignKeyRefsInRange("doc:a", "doc:z"));
    try std.testing.expectError(error.ReadOnly, readonly.repairForeignKeyRefsInRangeForConstraint("fk_parent", "doc:a", "doc:z"));
    try std.testing.expectError(error.ReadOnly, readonly.repairUniqueConstraintRowsInRange("doc:a", "doc:z"));
    try std.testing.expectError(error.ReadOnly, readonly.repairForeignKeyRefOwnerForParent("fk_parent", "parent", "p1"));
    try std.testing.expectError(error.ReadOnly, readonly.repairForeignKeyRefOwnerRange("fk_parent", "parent", "p1", "p9"));
    try std.testing.expectError(error.ReadOnly, readonly.claimForeignKeyIntegrityWorkUnit("claim-a", "worker-a", 1, "scan", "repair", null, "doc:a", "doc:z", 1000));
    try std.testing.expectError(error.ReadOnly, readonly.claimForeignKeyIntegrityWorkUnitAt("claim-at", "worker-a", 1, "scan", "repair", null, "doc:a", "doc:z", 1000, 1));
    try std.testing.expectError(error.ReadOnly, readonly.upsertForeignKeyIntegrityJobRecord("job-a", "child", "repair", "worker-a", null, "doc:a", "doc:z", 1000, 10, "running"));
    try std.testing.expectError(error.ReadOnly, readonly.upsertForeignKeyIntegrityJobRecordAt("job-at", "child", "repair", "worker-a", null, "doc:a", "doc:z", 1000, 10, "running", 1));
    try std.testing.expectError(error.ReadOnly, readonly.upsertRelationalIndexRepairJobRecord("repair-a", "default", "public", "docs", "worker-a", "doc:a", "doc:z", 1000, 10, "running"));
    try std.testing.expectError(error.ReadOnly, readonly.upsertRelationalIndexRepairJobRecordAt("repair-at", "default", "public", "docs", "worker-a", "doc:a", "doc:z", 1000, 10, "running", 1));
    try std.testing.expectError(error.ReadOnly, readonly.recordRelationalIndexRepairJobPass("repair-a", "running", false, 1, 1, 0, "doc:m", .{}, null));
    try std.testing.expectError(error.ReadOnly, readonly.recordRelationalIndexRepairJobPassAt("repair-at", "complete", true, 1, 1, 0, "", .{}, null, 1));
    try std.testing.expectError(error.ReadOnly, readonly.upsertRelationalIndexRepairJobTargetAt("repair-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", "", 1000, 10, "running", 1));
    try std.testing.expectError(error.ReadOnly, readonly.recordRelationalIndexRepairJobTargetPassAt("repair-target", "complete", true, "", 1, 1, 0, 1, null, false, 1));
    try std.testing.expectError(error.ReadOnly, readonly.runRelationalIndexRepairJobPageAt("repair-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", 1000, 10, 1));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleRelationalIndexRepairJobPageAt("repair-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", 1000, 10, 1));
    try std.testing.expectError(error.ReadOnly, readonly.upsertRelationalIndexDropJobRecordAt("drop-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", "", 1000, 10, "running", 1));
    try std.testing.expectError(error.ReadOnly, readonly.recordRelationalIndexDropJobPassAt("drop-target", "worker-a", 1, "complete", true, "", 1, 1, 0, 1, null, false, 1));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleRelationalIndexDropJob("drop-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", 1000, 10));
    try std.testing.expectError(error.ReadOnly, readonly.discoverRelationalIndexDropJobs("docs"));
    try std.testing.expectError(error.ReadOnly, readonly.completeForeignKeyIntegrityJobRecord("job-a", "complete", true, .{}));
    try std.testing.expectError(error.ReadOnly, readonly.completeForeignKeyIntegrityJobRecordWithDiagnostics("job-a", "complete", true, .{}, "[]", 0, false));
    try std.testing.expectError(error.ReadOnly, readonly.completeForeignKeyIntegrityJobRecordAt("job-at", "complete", true, .{}, 1));
    try std.testing.expectError(error.ReadOnly, readonly.completeForeignKeyIntegrityJobRecordWithDiagnosticsAt("job-at", "complete", true, .{}, "[]", 0, false, 1));
    try std.testing.expectError(error.ReadOnly, readonly.updateForeignKeyIntegrityJobDiagnostics("job-a", "[]", 0, false));
    try std.testing.expectError(error.ReadOnly, readonly.updateForeignKeyIntegrityJobDiagnosticsWithReport("job-a", .{}, "[]", 0, false));
    try std.testing.expectError(error.ReadOnly, readonly.updateForeignKeyIntegrityJobDiagnosticsAt("job-at", "[]", 0, false, 1));
    try std.testing.expectError(error.ReadOnly, readonly.updateForeignKeyIntegrityJobDiagnosticsWithReportAt("job-at", .{}, "[]", 0, false, 1));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleForeignKeyActionJob("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleForeignKeyActionJobWithUpdatedParentKey("action-b", "cascade", "worker-a", "fk_parent", "parent", "p1", "p2", 16));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleForeignKeyActionJobAt("action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.ReadOnly, readonly.requeueForeignKeyActionJob("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.ReadOnly, readonly.requeueForeignKeyActionJobWithUpdatedParentKey("action-b", "cascade", "worker-a", "fk_parent", "parent", "p1", "p2", 16));
    try std.testing.expectError(error.ReadOnly, readonly.requeueForeignKeyActionJobAt("action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleForeignKeyActionSchedule("schedule-a", "action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleForeignKeyActionScheduleWithUpdatedParentKey("schedule-b", "action-b", "cascade", "worker-a", "fk_parent", "parent", "p1", "p2", 16));
    try std.testing.expectError(error.ReadOnly, readonly.scheduleForeignKeyActionScheduleAt("schedule-c", "action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.ReadOnly, readonly.requeueForeignKeyActionSchedule("schedule-a", "action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.ReadOnly, readonly.requeueForeignKeyActionScheduleAt("schedule-c", "action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.ReadOnly, readonly.markForeignKeyActionScheduleSeeded("schedule-a", 1));
    try std.testing.expectError(error.ReadOnly, readonly.markForeignKeyActionScheduleSeededAt("schedule-a", 1, 1));
    try std.testing.expectError(error.ReadOnly, readonly.claimAndRunForeignKeyActionJobPage("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000));
    try std.testing.expectError(error.ReadOnly, readonly.claimAndRunForeignKeyActionJobPageAt("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000, 1));
    try std.testing.expectError(error.ReadOnly, readonly.claimForeignKeyActionJobPage("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000));
    try std.testing.expectError(error.ReadOnly, readonly.claimForeignKeyActionJobPageAt("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000, 1));
    try std.testing.expectError(error.ReadOnly, readonly.finishClaimedForeignKeyActionJobPage(@as(DB.ForeignKeyActionJobRecord, undefined), 0, false, null, null, null));
    try std.testing.expectError(error.ReadOnly, readonly.finishClaimedForeignKeyActionJobPageAt(@as(DB.ForeignKeyActionJobRecord, undefined), 0, false, null, null, null, 1));
    try std.testing.expectError(error.ReadOnly, readonly.claimAndRunForeignKeyIntegrityWorkUnit("claim-a", "worker-a", 1, "scan", .repair, null, "doc:a", "doc:z", 1000));
    try std.testing.expectError(error.ReadOnly, readonly.claimAndRunForeignKeyIntegrityWorkUnitAt("claim-at", "worker-a", 1, "scan", .repair, null, "doc:a", "doc:z", 1000, 1));
    try std.testing.expectError(error.ReadOnly, readonly.catchUpPendingDerivedReplay());
    const NoopReplayProgress = struct {
        fn hook(_: *anyopaque, _: []const u8, _: db_mod.ReplayProgress) anyerror!void {}
    };
    try std.testing.expectError(error.ReadOnly, readonly.catchUpPendingDerivedReplayWithProgress(&noop_dispatcher_state, NoopReplayProgress.hook));
    try std.testing.expectError(error.ReadOnly, readonly.derivedAsyncAppendDerivedBatchRecord(.{}));
    try std.testing.expectError(error.ReadOnly, readonly.rebuildDenseIndexesForTargetCoverage(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.rebuildSparseIndexesForTargetCoverage(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.rebuildGraphIndexesForTargetCoverage(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.runDensePostingMaintenanceForIdle());
    try std.testing.expectError(error.ReadOnly, readonly.runDensePostingMaintenanceForIdleBestEffort());
    try std.testing.expectError(error.ReadOnly, readonly.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, null, null));
    try std.testing.expectError(error.ReadOnly, readonly.derivedAsyncRebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(alloc, null, null, null, null, null, null, null, 16, 16));
    try std.testing.expectError(error.ReadOnly, readonly.replayGeneratedEnrichmentsFromStoredDocs(alloc));
    try std.testing.expectError(error.ReadOnly, readonly.ensureGroupCreatedAtMillis(alloc, 42, 1234));
    try std.testing.expectError(error.ReadOnly, readonly.runMaintenanceUntilTargets(1, &.{"idx"}));
    try std.testing.expectError(error.ReadOnly, readonly.mutateRelationalRowsFromSource(alloc, .{}, .{ .kind = .update }));
    try std.testing.expectError(error.ReadOnly, readonly.stagePlannedRelationalRowsMutationSourceAlloc(alloc, .{}, .{ .kind = .update }, 0, &.{}));
    try std.testing.expectError(error.ReadOnly, readonly.mutateRelationalRowsJoinedSourceAlloc(alloc, .{}, .{ .kind = .update }));
    try std.testing.expectError(error.ReadOnly, readonly.stagePlannedRelationalRowsJoinedMutationSourceAlloc(alloc, .{}, .{ .kind = .update }, 0, &.{}));
    try std.testing.expectError(error.ReadOnly, readonly.stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(alloc, .{}, .{}, .{ .kind = .update }, 0, &.{}));
    const blocked_txn: transactions_mod.TxnId = .{ 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43 };
    try std.testing.expectError(error.ReadOnly, readonly.beginTransaction(1));
    try std.testing.expectError(error.ReadOnly, readonly.beginTransactionWithId(blocked_txn, 1));
    try std.testing.expectError(error.ReadOnly, readonly.beginTransactionWithParticipants(1, &.{"remote"}));
    try std.testing.expectError(error.ReadOnly, readonly.beginTransactionWithIdAndParticipants(blocked_txn, 1, &.{"remote"}));
    try std.testing.expectError(error.ReadOnly, readonly.writeIntents(blocked_txn, &.{}, &.{}));
    try std.testing.expectError(error.ReadOnly, readonly.writeTransaction(blocked_txn, .{}));
    try std.testing.expectError(error.ReadOnly, readonly.claimRowsForTransaction(blocked_txn, &.{"row:a"}, .{}));
    try std.testing.expectError(error.ReadOnly, readonly.commitTransaction(blocked_txn, 2));
    try std.testing.expectError(error.ReadOnly, readonly.resolveTransactionIntents(blocked_txn, .committed, 3));
    try std.testing.expectError(error.ReadOnly, readonly.abortTransaction(blocked_txn, 4));
    try std.testing.expectError(error.ReadOnly, readonly.markTransactionParticipantResolved(blocked_txn, "remote"));
    try std.testing.expectError(error.ReadOnly, readonly.recoverTransactions(3, 4));
    try std.testing.expectError(
        error.ReadOnly,
        readonly.reassignIdentityNamespaceForInternalTransition(.{ .table_id = 301, .shard_id = 1, .range_id = 1 }),
    );
}

test "db read-only open modes can share lsm root with live writer" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var writer = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer writer.close();

    try writer.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });
    try writer.sync(true);

    {
        var readonly = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .open_mode = .query_readonly,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer readonly.close();

        var result = (try readonly.lookup(alloc, "doc:a", .{})) orelse return error.TestUnexpectedResult;
        defer result.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
    }

    {
        var status = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .open_mode = .status_only,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer status.close();
    }
}

test "db read-only open modes reject catalog mutations before side effects" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var writer = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer writer.close();

        try writer.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try writer.sync(true);
    }

    for ([_]OpenOptions.OpenMode{ .query_readonly, .status_only }) |mode| {
        var readonly = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .open_mode = mode,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer readonly.close();

        try std.testing.expectError(error.ReadOnly, readonly.deleteIndex("missing_idx"));
        try std.testing.expectError(error.ReadOnly, readonly.addEnrichment(.{
            .name = "readonly_asset_v1",
            .kind = .asset,
            .template = "{{url}}",
            .content_type = "application/json",
            .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
        }));
        try std.testing.expectError(error.ReadOnly, readonly.upsertEnrichment(.{
            .name = "readonly_asset_v1",
            .kind = .asset,
            .template = "{{url}}",
            .content_type = "application/json",
            .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
        }));
        try std.testing.expectError(error.ReadOnly, readonly.deleteEnrichment(.asset, "readonly_asset_v1"));
        try std.testing.expectError(error.ReadOnly, readonly.addResolver(.{
            .name = "readonly_resolver_v1",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ lower _entity.label }}",
            .config_generation = 1,
        }));
        try std.testing.expectError(error.ReadOnly, readonly.upsertResolver(.{
            .name = "readonly_resolver_v1",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ lower _entity.label }}",
            .config_generation = 1,
        }));
        try std.testing.expectError(error.ReadOnly, readonly.removeResolver("readonly_resolver_v1"));
        try std.testing.expectError(error.ReadOnly, readonly.compactTextIndexes());
        try std.testing.expectError(error.ReadOnly, readonly.drainScheduledTextMerges());
        try std.testing.expectError(error.ReadOnly, readonly.forceCompactTextIndexes());
        try std.testing.expectError(error.ReadOnly, readonly.bestEffortForceCompactTextIndexes());
    }
}

test "db lifecycle open query_readonly lmdb primary does not create missing database" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var readonly = DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .lmdb,
        .open_mode = .query_readonly,
        .ttl_cleanup = .{ .enabled = false },
    }) catch |err| switch (err) {
        error.UnsupportedPlatform => return,
        error.FileNotFound, error.NotFound, error.LmdbUnexpected => return,
        else => return err,
    };
    readonly.close();
    return error.ExpectedReadonlyLmdbMissingOpenFailure;
}

test "db lifecycle open query_readonly lmdb primary rejects writes after readonly open" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .lmdb,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        }) catch |err| switch (err) {
            error.UnsupportedPlatform => return,
            else => return err,
        };
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
    }

    var readonly = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .lmdb,
        .open_mode = .query_readonly,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer readonly.close();

    var result = (try readonly.lookup(alloc, "doc:a", .{})) orelse return error.MissingReadonlyLmdbDocument;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
    try std.testing.expectError(error.ReadOnly, readonly.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        .sync_level = .write,
    }));
}

test "db lifecycle open quarantines dense index with unsupported artifact version" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const dense_cfg: types.IndexConfig = .{
        .name = "dv_quarantine",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_quarantine\"}}",
    };

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const open_options: OpenOptions = .{
        .enrichment = .{
            .owner_id = "quarantine-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), open_options);
        defer db.close();

        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
        try db.addIndex(dense_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        // Persist an HBC metadata record with a version this build does not
        // support, simulating an artifact written by an incompatible build.
        const entry = db.core.denseIndex("dv_quarantine") orelse return error.TestUnexpectedResult;
        var bad_meta = entry.index.metadata;
        bad_meta.version = 99;
        var meta_buf: [vectorindex_mod.hbc.IndexMetadata.encoded_size]u8 = undefined;
        const encoded = bad_meta.encode(&meta_buf);
        var txn = try entry.index.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(.meta, vectorindex_mod.hbc.meta_key, encoded);
        try txn.commit();
    }

    var db = try DB.open(alloc, std.mem.span(path), open_options);
    defer db.close();

    // The broken dense index is quarantined: absent from the runtime, its
    // load error recorded, and the rest of the table fully usable.
    try std.testing.expect(db.core.denseIndex("dv_quarantine") == null);
    const recorded = db.core.index_manager.loadFailure("dv_quarantine") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("UnsupportedVersion", recorded);

    var ft_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer ft_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), ft_result.total_hits);

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        var found = false;
        for (stats.indexes) |item| {
            if (!std.mem.eql(u8, item.name, "dv_quarantine")) continue;
            found = true;
            const load_error = item.load_error orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings("UnsupportedVersion", load_error);
            try std.testing.expect(!item.replay_catch_up_required);
            try std.testing.expect(!item.backfill_active);
            try std.testing.expect(!item.catch_up_active);
        }
        try std.testing.expect(found);
    }

    // Queries that explicitly reference the quarantined index get a distinct
    // error instead of IndexNotFound or a stall.
    const query_vec = [_]f32{ 0.1, 0.2, 0.3 };
    try std.testing.expectError(error.IndexUnavailable, db.search(alloc, .{
        .index_name = "dv_quarantine",
        .dense = .{
            .vector = &query_vec,
            .k = 1,
        },
        .limit = 1,
    }));

    // Drop + recreate recovers and clears the recorded failure.
    try std.testing.expect(try db.deleteIndex("dv_quarantine"));
    try std.testing.expect(db.core.index_manager.loadFailure("dv_quarantine") == null);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);
    try db.addIndex(dense_cfg);
    try db.runUntilIdle();
    try std.testing.expect(db.core.denseIndex("dv_quarantine") != null);

    const recovered_vec = try deterministic.interface().embedDense(alloc, "dv_quarantine", "alpha concept overview", 3);
    defer alloc.free(recovered_vec);
    var recovered = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_quarantine",
        .dense = .{
            .vector = recovered_vec,
            .k = 1,
        },
        .limit = 1,
    }, 1);
    defer recovered.deinit();
    try std.testing.expect(recovered.total_hits >= 1);
}

test "db lifecycle quarantine drops dense index after persisted index directory corruption" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const corruptNonEmptyFilesUnderDir = TestHelpers.corruptNonEmptyFilesUnderDir;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const path_slice = std.mem.span(path);

    const dense_cfg: types.IndexConfig = .{
        .name = "dv_corrupt",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_corrupt\"}}",
    };

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const open_options: OpenOptions = .{
        .enrichment = .{
            .owner_id = "corrupt-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    {
        var db = try DB.open(alloc, path_slice, open_options);
        defer db.close();

        try db.addIndex(dense_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try std.testing.expect(db.core.denseIndex("dv_corrupt") != null);
    }

    const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dv_corrupt", .{path_slice});
    defer alloc.free(index_path);
    try std.testing.expect((try corruptNonEmptyFilesUnderDir(alloc, index_path)) > 0);

    var db = try DB.open(alloc, path_slice, open_options);
    defer db.close();

    try std.testing.expect(db.core.denseIndex("dv_corrupt") == null);
    _ = db.core.index_manager.loadFailure("dv_corrupt") orelse return error.TestUnexpectedResult;

    try std.testing.expect(try db.deleteIndex("dv_corrupt"));
    try std.testing.expect(db.core.index_manager.loadFailure("dv_corrupt") == null);
    try std.testing.expect(db.core.index_manager.get("dv_corrupt") == null);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().statFile(io_impl.io(), index_path, .{}));
}

test "db lifecycle quarantine self-heals via retryQuarantinedIndexLoads" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }
        for (0..300) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"alpha\",\"n\":{d}}}", .{i}),
            });
        }
        try db.batch(.{ .writes = writes.items });
        try db.core.index_manager.addAllNoBackfill(db.core.store, &.{.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        }});
    }

    index_manager_mod.test_abort_text_backfill_after_batches = 1;
    defer index_manager_mod.test_abort_text_backfill_after_batches = null;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") != null);
    try std.testing.expect(db.core.textIndexEntry("ft_v1") == null);

    // Writes keep flowing while the index is quarantined; the heal's resumed
    // backfill must pick them up. They also guarantee the next backfill
    // attempt flushes at least once, so the still-set abort knob fires.
    {
        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }
        for (300..400) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"alpha\",\"n\":{d}}}", .{i}),
            });
        }
        try db.batch(.{ .writes = writes.items });
    }

    // While the cause persists, a forced retry fails and applies backoff.
    {
        const result = try db.retryQuarantinedIndexLoads(true);
        try std.testing.expectEqual(@as(usize, 0), result.recovered);
        try std.testing.expectEqual(@as(usize, 1), result.remaining);
    }
    // Cause fixed — but the failed attempt set a backoff deadline, so a
    // non-forced retry is skipped without re-attempting the open (it would
    // recover if it attempted).
    index_manager_mod.test_abort_text_backfill_after_batches = null;
    {
        const result = try db.retryQuarantinedIndexLoads(false);
        try std.testing.expectEqual(@as(usize, 0), result.recovered);
        try std.testing.expectEqual(@as(usize, 1), result.remaining);
    }

    // Forced retry recovers the index in-process — no reopen, no
    // drop+recreate — resuming the persisted rebuild state. Hold a simulated
    // query lease across the final publication and prove recovery waits for
    // it rather than reallocating the inline catalog beneath a reader.
    const RetryState = struct {
        db: *DB,
        result: ?index_manager_mod.IndexManager.QuarantineRetryResult = null,
        err: ?anyerror = null,
        completed: std.atomic.Value(bool) = .init(false),

        fn run(state: *@This()) void {
            state.result = state.db.retryQuarantinedIndexLoads(true) catch |err| {
                state.err = err;
                state.completed.store(true, .release);
                return;
            };
            state.completed.store(true, .release);
        }
    };
    test_quarantine_publication_fence_entered.store(false, .release);
    defer test_quarantine_publication_fence_entered.store(false, .release);
    db.core.lockApplyShared();
    var apply_shared_held = true;
    var retry_state = RetryState{ .db = &db };
    var retry_thread = try std.Thread.spawn(.{}, RetryState.run, .{&retry_state});
    var retry_thread_joined = false;
    defer {
        if (apply_shared_held) db.core.unlockApplyShared();
        if (!retry_thread_joined) retry_thread.join();
    }

    const publication_deadline = monotonicTimeNs() +| 30 * std.time.ns_per_s;
    while (!test_quarantine_publication_fence_entered.load(.acquire) and monotonicTimeNs() < publication_deadline) {
        db_internal.sleepNs(100 * std.time.ns_per_us);
    }
    const publication_fence_entered = test_quarantine_publication_fence_entered.load(.acquire);
    const publication_waited_for_reader = publication_fence_entered and !retry_state.completed.load(.acquire);
    db.core.unlockApplyShared();
    apply_shared_held = false;
    retry_thread.join();
    retry_thread_joined = true;

    try std.testing.expect(publication_fence_entered);
    try std.testing.expect(publication_waited_for_reader);
    if (retry_state.err) |err| return err;
    const result = retry_state.result orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), result.recovered);
    try std.testing.expectEqual(@as(usize, 0), result.remaining);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
    try std.testing.expect(db.core.textIndexEntry("ft_v1") != null);

    try db.runUntilIdle();
    var search_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match_all = {} },
        .limit = 500,
    });
    defer search_result.deinit();
    try std.testing.expectEqual(@as(u32, 400), search_result.total_hits);
}

test "db lifecycle open read-only propagates transient index load errors instead of quarantining" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }

    index_manager_mod.test_inject_index_open_error = error.FileNotFound;
    defer index_manager_mod.test_inject_index_open_error = null;

    // A read-only (replica) open must NOT quarantine a transient read race
    // (e.g. the writer reclaiming obsolete LSM runs mid-open): the error
    // propagates so the query layer reopens against a fresh manifest and
    // retries. A quarantined replica would instead serve IndexUnavailable
    // until the read cache next invalidates it — the quarantine retry
    // worker only runs on the writer.
    try std.testing.expectError(error.FileNotFound, DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
    }));

    // The writer open quarantines the same error: the writer cannot race
    // its own (not yet started) reclaim, so a missing artifact there is
    // persistent damage — and the in-process retry recovers it once the
    // cause clears.
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    const recorded = db.core.index_manager.loadFailure("ft_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("FileNotFound", recorded);

    index_manager_mod.test_inject_index_open_error = null;
    const result = try db.retryQuarantinedIndexLoads(true);
    try std.testing.expectEqual(@as(usize, 1), result.recovered);
    try std.testing.expectEqual(@as(usize, 0), result.remaining);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
}

test "db lifecycle open writer_no_replay defers pending derived replay until runUntilIdle" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });

        var dense_embeddings = try alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.reserveNextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, appended_sequence, encoded);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    {
        const replay_debt = try reopened.listDerivedReplayDebt(alloc);
        defer {
            for (replay_debt) |*status| status.deinit(alloc);
            alloc.free(replay_debt);
        }
        try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
        try std.testing.expectEqual(@as(u64, 0), replay_debt[0].applied_sequence);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
        try std.testing.expect(replay_debt[0].catch_up_required);
    }

    try reopened.catchUpPendingDerivedReplay();
    try reopened.runUntilIdle();

    {
        const replay_debt = try reopened.listDerivedReplayDebt(alloc);
        defer {
            for (replay_debt) |*status| status.deinit(alloc);
            alloc.free(replay_debt);
        }
        try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].applied_sequence);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
        try std.testing.expect(!replay_debt[0].catch_up_required);
    }

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db managed projection checkpoints persist status and config identity" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const configs = [_]types.IndexConfig{
        .{ .name = "ft_v1", .kind = .full_text, .config_json = "{\"field\":\"title\"}" },
        .{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}" },
        .{ .name = "sp_v1", .kind = .sparse_vector, .config_json = "{\"field\":\"sparse\"}" },
        .{ .name = "gr_v1", .kind = .graph, .config_json = "{}" },
        .{
            .name = "alg_v1",
            .kind = .algebraic,
            .config_json =
            \\{
            \\  "version": 1,
            \\  "table": "docs",
            \\  "group_fields": [{"name":"category","path":"category","type":"string"}],
            \\  "measure_fields": [{"name":"score","path":"score","type":"number"}],
            \\  "materializations": [{"name":"count_by_category","op":"count","group_by":["category"]}]
            \\}
            ,
        },
    };

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    for (configs) |cfg| try db.addIndex(cfg);

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\",\"category\":\"news\",\"score\":3,\"embedding\":[1,0],\"sparse\":{\"indices\":[1,3],\"values\":[0.5,0.75]}}",
            },
        },
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
        },
        .sync_level = .full_index,
    });

    for (configs) |cfg| {
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
        const stored_cfg = db.core.index_manager.get(cfg.name) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
        try std.testing.expectEqual(try db.core.loadAppliedSequence(alloc, cfg.name), checkpoint.applied_sequence);
        try std.testing.expect(checkpoint.applied_sequence > 0);
        try std.testing.expectEqual(types.indexConfigHash(stored_cfg.*), checkpoint.config_hash);
    }
}

test "db stats report projection checkpoint replay tail per index hint" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const dense_target_sequence: u64 = 7;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
        try db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2}" });

        try db.core.store.ensureReplayNextSequenceAtLeast(dense_target_sequence + 1);
        var latest_raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &latest_raw, dense_target_sequence, .little);
        const latest_key = internal_keys.replayLatestSequenceKey(@intCast(@intFromEnum(change_journal_mod.TargetHint.dense_vector)));
        var batch = try db.core.store.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(latest_key[0..], latest_raw[0..]);
        try batch.commit();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 2), stats.indexes.len);
        var full_text_seen = false;
        var dense_seen = false;
        for (stats.indexes) |index| {
            if (std.mem.eql(u8, index.name, "ft_v1")) {
                full_text_seen = true;
                try std.testing.expectEqual(@as(u64, 0), index.replay_target_sequence);
                try std.testing.expectEqual(@as(u64, 0), index.checkpoint_replay_tail_sequence_count);
                try std.testing.expect(!index.replay_catch_up_required);
            } else if (std.mem.eql(u8, index.name, "dv_v1")) {
                dense_seen = true;
                try std.testing.expectEqual(dense_target_sequence, index.replay_target_sequence);
                try std.testing.expectEqual(dense_target_sequence, index.checkpoint_replay_tail_sequence_count);
                try std.testing.expect(index.replay_catch_up_required);
            }
        }
        try std.testing.expect(full_text_seen);
        try std.testing.expect(dense_seen);
    }

    var status_db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .status_only,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer status_db.close();

    const status_stats = try status_db.stats(alloc);
    defer types.freeDBStats(alloc, status_stats);
    try std.testing.expectEqual(@as(usize, 2), status_stats.indexes.len);
    var status_full_text_seen = false;
    var status_dense_seen = false;
    for (status_stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "ft_v1")) {
            status_full_text_seen = true;
            try std.testing.expectEqual(@as(u64, 0), index.replay_target_sequence);
            try std.testing.expectEqual(@as(u64, 0), index.checkpoint_replay_tail_sequence_count);
            try std.testing.expect(!index.replay_catch_up_required);
        } else if (std.mem.eql(u8, index.name, "dv_v1")) {
            status_dense_seen = true;
            try std.testing.expectEqual(dense_target_sequence, index.replay_target_sequence);
            try std.testing.expectEqual(dense_target_sequence, index.checkpoint_replay_tail_sequence_count);
            try std.testing.expect(index.replay_catch_up_required);
        }
    }
    try std.testing.expect(status_full_text_seen);
    try std.testing.expect(status_dense_seen);
}

test "db dense projection checkpoint prefers hbc metadata over corrupt sidecar" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    var target_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const stored_dense_cfg = &(db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult).config;
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            },
            .sync_level = .full_index,
        });

        target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 11,
            .config_hash = types.indexConfigHash(stored_dense_cfg.*),
        });

        const checkpoint_path = db.core.applied_sequence_checkpoint_path orelse return error.TestUnexpectedResult;
        try writeRawProjectionCheckpointSidecarForTest(checkpoint_path, "not-a-derived-apply-checkpoint");
        try std.testing.expectError(
            error.InvalidDerivedApplyState,
            apply_state.loadProjectionCheckpointWithSidecar(alloc, db.core.index_manager.checkpointIo(), db.core.store, checkpoint_path, "dense_idx"),
        );
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 11), checkpoint.generation);
    const reopened_stored_dense_cfg = &(reopened.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult).config;
    try std.testing.expectEqual(types.indexConfigHash(reopened_stored_dense_cfg.*), checkpoint.config_hash);
    try std.testing.expectEqual(target_sequence, checkpoint.applied_sequence);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqual(checkpoint.applied_sequence, stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expectEqual(@as(u64, 11), stats.indexes[0].projection_checkpoint_generation);
}

test "db corrupt projection sidecar degrades non-dense checkpoint and quarantines writes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const text_cfg: types.IndexConfig = .{ .name = "ft_idx", .kind = .full_text, .config_json = "{}" };
    var text_config_hash: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(text_cfg);
        const stored_text_cfg = db.core.index_manager.get("ft_idx") orelse return error.TestUnexpectedResult;
        text_config_hash = types.indexConfigHash(stored_text_cfg.*);
        try db.core.saveProjectionCheckpoint("ft_idx", .{
            .applied_sequence = 9,
            .status = .clean,
            .generation = 4,
            .config_hash = text_config_hash,
        });

        const checkpoint_path = db.core.applied_sequence_checkpoint_path orelse return error.TestUnexpectedResult;
        try writeRawProjectionCheckpointSidecarForTest(checkpoint_path, "not-a-derived-apply-checkpoint");
    }

    {
        var status_only = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer status_only.close();

        try std.testing.expectEqual(@as(u64, 0), try status_only.core.loadAppliedSequence(alloc, "ft_idx"));
        const degraded_checkpoint = try status_only.core.loadProjectionCheckpoint(alloc, "ft_idx");
        try std.testing.expectEqual(apply_state.ProjectionStatus.repair_required, degraded_checkpoint.status);
        try std.testing.expectEqual(@as(u64, 0), degraded_checkpoint.applied_sequence);
        try std.testing.expectEqual(text_config_hash, degraded_checkpoint.config_hash);

        const degraded_stats = try status_only.stats(alloc);
        defer types.freeDBStats(alloc, degraded_stats);
        try std.testing.expectEqual(@as(usize, 1), degraded_stats.indexes.len);
        try std.testing.expectEqualStrings("repair_required", degraded_stats.indexes[0].projection_checkpoint_status);
        try std.testing.expect(degraded_stats.indexes[0].repair_degraded);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expectEqual(@as(u64, 0), try reopened.core.loadAppliedSequence(alloc, "ft_idx"));
    const degraded_by_open_checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "ft_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.repair_required, degraded_by_open_checkpoint.status);
    try std.testing.expectEqual(@as(u64, 0), degraded_by_open_checkpoint.applied_sequence);
    try std.testing.expectEqual(text_config_hash, degraded_by_open_checkpoint.config_hash);
    try std.testing.expectEqualStrings(
        "InvalidDerivedApplyState",
        reopened.core.index_manager.loadFailure("ft_idx") orelse return error.TestUnexpectedResult,
    );

    try std.testing.expectError(error.IndexNotFound, reopened.core.saveAppliedSequence("ft_idx", 7));
    const still_degraded_checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "ft_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.repair_required, still_degraded_checkpoint.status);
    try std.testing.expectEqual(@as(u64, 0), still_degraded_checkpoint.applied_sequence);
    try std.testing.expectEqual(text_config_hash, still_degraded_checkpoint.config_hash);
}

test "db lifecycle open dense replay progress target matches replay debt target" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });

        var dense_embeddings = try alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.reserveNextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, appended_sequence, encoded);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const replay_debt = try reopened.listDerivedReplayDebt(alloc);
    defer {
        for (replay_debt) |*status| status.deinit(alloc);
        alloc.free(replay_debt);
    }
    try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
    try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);

    const Capture = struct {
        seen: usize = 0,
        last: db_internal.ReplayProgress = .{},

        fn run(ptr: *anyopaque, _: []const u8, progress: db_internal.ReplayProgress) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            self.last = progress;
        }
    };

    var capture = Capture{};
    try reopened.catchUpPendingDerivedReplayWithProgress(&capture, Capture.run);
    try reopened.runUntilIdle();

    try std.testing.expect(capture.seen > 0);
    try std.testing.expectEqual(replay_debt[0].target_sequence, capture.last.target_sequence);
    try std.testing.expectEqual(replay_debt[0].target_sequence, capture.last.sequence);
}

test "db lifecycle open writer_no_replay starts workers without resuming pending derived replay" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });

        var dense_embeddings = try alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.reserveNextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, appended_sequence, encoded);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = true,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    platform.time.sleepMs(50);

    {
        const replay_debt = try reopened.listDerivedReplayDebt(alloc);
        defer {
            for (replay_debt) |*status| status.deinit(alloc);
            alloc.free(replay_debt);
        }
        try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
        try std.testing.expectEqual(@as(u64, 0), replay_debt[0].applied_sequence);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
        try std.testing.expect(replay_debt[0].catch_up_required);
    }

    try reopened.runDerivedUntil(appended_sequence);

    {
        const replay_debt = try reopened.listDerivedReplayDebt(alloc);
        defer {
            for (replay_debt) |*status| status.deinit(alloc);
            alloc.free(replay_debt);
        }
        try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].applied_sequence);
        try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
        try std.testing.expect(!replay_debt[0].catch_up_required);
    }
}

test "db derived target advance does not skip unseen matching replay records" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation","mention_edge_type":"mentions"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });
    try db.addIndex(.{
        .name = "plain_relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });
    try db.addResolver(.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const graph_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_artifact_keys = &.{resolution_key},
        .target_hints = &.{.graph},
    });
    defer alloc.free(graph_payload);
    try db.core.store.appendReplayOpaque(alloc, 1, graph_payload);

    try std.testing.expect(!try DB.derivedAsyncCanAdvanceDerivedToTargetAsync(
        db.async_context,
        .{ .name = "relations_graph", .kind = .graph },
        0,
        1,
    ));
    try std.testing.expect(try DB.derivedAsyncCanAdvanceDerivedToTargetAsync(
        db.async_context,
        .{ .name = "plain_relations_graph", .kind = .graph },
        0,
        1,
    ));
    try std.testing.expect(try DB.derivedAsyncCanAdvanceDerivedToTargetAsync(
        db.async_context,
        .{ .name = "ft_v1", .kind = .full_text },
        0,
        1,
    ));
}

test "db lifecycle open default primary backend survives reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
            },
        });
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        const raw = try reopened.get(alloc, "doc:a");
        defer if (raw) |value| alloc.free(value);
        try std.testing.expect(raw != null);
        try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"alpha\"") != null);

        const stats = try reopened.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 2), stats.doc_count);
    }
}

test "db lifecycle lsm maintenance reclaims due index obsolete paths before primary compaction" {
    const DB = @import("mod.zig").DB;
    const expectObsoletePathsReclaimable = TestHelpers.expectObsoletePathsReclaimable;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        // This test verifies the explicit primary-only maintenance entry
        // point; keep detached LSM maintenance from consuming the queued
        // primary obsolete path before the assertion.
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .lsm = .{
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
            .l0_soft_limit_runs = 100,
            .obsolete_retention_ns = 0,
        } },
        .index_backends = .{
            .dense_lsm_options = .{
                .flush_threshold = 1,
                .defer_flush_on_commit = true,
                .l0_soft_limit_runs = 100,
                .obsolete_retention_ns = 0,
            },
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    var key_buf: [16]u8 = undefined;
    for (0..4) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
        switch (db.core.primary_store_owner) {
            .lsm => |*owner| try owner.handle.backend.sync(true),
            else => return error.SkipZigTest,
        }
    }

    switch (db.core.primary_store_owner) {
        .lsm => |*owner| try owner.handle.backend.flushBufferedWritesWithOptions(.{ .compact = false, .flush = true }),
        else => return error.SkipZigTest,
    }

    switch (db.core.primary_store_owner) {
        .lsm => |*owner| owner.handle.backend.options.l0_soft_limit_runs = 1,
        else => return error.SkipZigTest,
    }

    const dense_entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.TestUnexpectedResult;
    const dense_backend = switch (dense_entry.index.env_owner) {
        .lsm => |*handle| handle.backend,
        else => return error.SkipZigTest,
    };
    const dense_root = dense_backend.root_dir orelse return error.TestUnexpectedResult;
    const obsolete_path = try lsm_backend_mod.repository.runPath(alloc, dense_root, 999_999);
    defer alloc.free(obsolete_path);

    try lsm_backend_mod.repository.writeFileAbsoluteWithStorage(dense_backend.storage.?, obsolete_path, "obsolete");
    {
        const locked = lsm_backend_mod.runtime.lockBackend(lsm_backend_mod.Backend, dense_backend);
        defer lsm_backend_mod.runtime.unlockBackend(lsm_backend_mod.Backend, dense_backend, locked);
        try dense_backend.queueObsoleteFilePath(try alloc.dupe(u8, obsolete_path));
        dense_backend.manifest_dirty = false;
    }

    try expectObsoletePathsReclaimable(dense_backend, 1);
    try std.testing.expect(try db.runLsmMaintenanceStepBestEffort());
    try std.testing.expectEqual(@as(u64, 0), dense_backend.snapshotMaintenanceStats().obsolete_paths);
    try std.testing.expectError(error.FileNotFound, dense_backend.storage.?.readFileAlloc(alloc, obsolete_path, 1024));
}

test "db lifecycle primary lsm maintenance step does not reclaim index obsolete paths" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .primary_backend = .{ .lsm = .{
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
            .l0_soft_limit_runs = 100,
            .obsolete_retention_ns = 0,
        } },
        .index_backends = .{
            .dense_lsm_options = .{
                .flush_threshold = 1,
                .defer_flush_on_commit = true,
                .l0_soft_limit_runs = 100,
                .obsolete_retention_ns = 0,
            },
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    const primary_backend = switch (db.core.primary_store_owner) {
        .lsm => |*owner| owner.handle.backend,
        else => return error.SkipZigTest,
    };
    const primary_root = primary_backend.root_dir orelse return error.TestUnexpectedResult;
    const primary_obsolete_path = try lsm_backend_mod.repository.runPath(alloc, primary_root, 888_888);
    defer alloc.free(primary_obsolete_path);

    const dense_entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.TestUnexpectedResult;
    const dense_backend = switch (dense_entry.index.env_owner) {
        .lsm => |*handle| handle.backend,
        else => return error.SkipZigTest,
    };
    const dense_root = dense_backend.root_dir orelse return error.TestUnexpectedResult;
    const dense_obsolete_path = try lsm_backend_mod.repository.runPath(alloc, dense_root, 999_999);
    defer alloc.free(dense_obsolete_path);

    try lsm_backend_mod.repository.writeFileAbsoluteWithStorage(primary_backend.storage.?, primary_obsolete_path, "primary obsolete");
    {
        const locked = lsm_backend_mod.runtime.lockBackend(lsm_backend_mod.Backend, primary_backend);
        defer lsm_backend_mod.runtime.unlockBackend(lsm_backend_mod.Backend, primary_backend, locked);
        try primary_backend.queueObsoleteFilePath(try alloc.dupe(u8, primary_obsolete_path));
        primary_backend.manifest_dirty = false;
    }

    try lsm_backend_mod.repository.writeFileAbsoluteWithStorage(dense_backend.storage.?, dense_obsolete_path, "dense obsolete");
    {
        const locked = lsm_backend_mod.runtime.lockBackend(lsm_backend_mod.Backend, dense_backend);
        defer lsm_backend_mod.runtime.unlockBackend(lsm_backend_mod.Backend, dense_backend, locked);
        try dense_backend.queueObsoleteFilePath(try alloc.dupe(u8, dense_obsolete_path));
        dense_backend.manifest_dirty = false;
    }

    _ = try db.runPrimaryLsmMaintenanceStep();

    try std.testing.expectEqual(@as(u64, 0), primary_backend.snapshotMaintenanceStats().obsolete_paths);
    try std.testing.expectError(error.FileNotFound, primary_backend.storage.?.readFileAlloc(alloc, primary_obsolete_path, 1024));
    try std.testing.expectEqual(@as(u64, 1), dense_backend.snapshotMaintenanceStats().obsolete_paths);
    const dense_bytes = try dense_backend.storage.?.readFileAlloc(alloc, dense_obsolete_path, 1024);
    alloc.free(dense_bytes);
}

test "dense target advance is not blocked by local catch-up session" {
    const DB = @import("mod.zig").DB;
    const AsyncContext = db_internal.AsyncContext(DB);
    const beginDenseCatchUpSessionTracked = db_internal.beginDenseCatchUpSessionTracked;
    const finishDenseCatchUpSessionTracked = db_internal.finishDenseCatchUpSessionTracked;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });

    const resources = db.core.asyncResources();
    var ctx = AsyncContext{
        .alloc = alloc,
        .store = resources.store,
        .index_manager = resources.index_manager,
        .apply_mutex = resources.apply_mutex,
    };
    defer ctx.deinit(alloc);

    try beginDenseCatchUpSessionTracked(&ctx, "semantic_idx");
    defer finishDenseCatchUpSessionTracked(&ctx, "semantic_idx");

    const can_advance = try DB.derivedAsyncCanAdvanceDerivedToTargetAsync(&ctx, .{
        .name = "semantic_idx",
        .kind = .dense_vector,
    }, 1, 2);
    try std.testing.expect(can_advance);
}

test "dense target advance is blocked while external bulk session is active" {
    const DB = @import("mod.zig").DB;
    const AsyncContext = db_internal.AsyncContext(DB);
    const beginExternalDenseBulkSessionTracked = db_internal.beginExternalDenseBulkSessionTracked;
    const finishExternalDenseBulkSessionTracked = db_internal.finishExternalDenseBulkSessionTracked;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });

    const resources = db.core.asyncResources();
    var ctx = AsyncContext{
        .alloc = alloc,
        .store = resources.store,
        .index_manager = resources.index_manager,
        .apply_mutex = resources.apply_mutex,
    };
    defer ctx.deinit(alloc);

    try beginExternalDenseBulkSessionTracked(&ctx);
    defer finishExternalDenseBulkSessionTracked(&ctx);

    const can_advance = try DB.derivedAsyncCanAdvanceDerivedToTargetAsync(&ctx, .{
        .name = "semantic_idx",
        .kind = .dense_vector,
    }, 1, 2);
    try std.testing.expect(!can_advance);
}

test "db catch-up defers artifact dense target advance without durable counter" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const target_sequence: u64 = 7;
    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    const counter_key = try DB.DerivedAsyncCallbacks.dense_artifact_target_counter_key_alloc(alloc, "dv_v1");
    defer alloc.free(counter_key);
    try db.core.store.putBatch(&.{}, &.{counter_key});

    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(stored_key);
    try db.core.store.putBatch(&.{
        .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
    }, &.{});

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });

    try db.core.store.ensureReplayNextSequenceAtLeast(target_sequence + 1);
    var latest_raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &latest_raw, target_sequence, .little);
    const latest_key = internal_keys.replayLatestSequenceKey(@intCast(@intFromEnum(change_journal_mod.TargetHint.dense_vector)));
    var batch = try db.core.store.beginWriteBatch();
    errdefer batch.abort();
    try batch.put(latest_key[0..], latest_raw[0..]);
    try batch.commit();

    try std.testing.expectEqual(@as(?u64, null), try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dv_v1"));

    try db.catchUpPendingDerivedReplay();

    const after = try db.listDerivedReplayDebt(alloc);
    defer {
        for (after) |*status| status.deinit(alloc);
        alloc.free(after);
    }
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqual(@as(u64, 0), after[0].applied_sequence);
    try std.testing.expectEqual(target_sequence, after[0].target_sequence);
    try std.testing.expect(after[0].catch_up_required);
}

test "db catch-up advances vacuous derived replay target gap" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const target_sequence: u64 = 7;
    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2}",
    });

    try db.core.store.ensureReplayNextSequenceAtLeast(target_sequence + 1);
    var latest_raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &latest_raw, target_sequence, .little);
    const latest_key = internal_keys.replayLatestSequenceKey(@intCast(@intFromEnum(change_journal_mod.TargetHint.dense_vector)));
    var batch = try db.core.store.beginWriteBatch();
    errdefer batch.abort();
    try batch.put(latest_key[0..], latest_raw[0..]);
    try batch.commit();

    const before = try db.listDerivedReplayDebt(alloc);
    defer {
        for (before) |*status| status.deinit(alloc);
        alloc.free(before);
    }
    try std.testing.expectEqual(@as(usize, 1), before.len);
    try std.testing.expect(before[0].catch_up_required);

    try db.catchUpPendingDerivedReplay();

    const after = try db.listDerivedReplayDebt(alloc);
    defer {
        for (after) |*status| status.deinit(alloc);
        alloc.free(after);
    }
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqual(target_sequence, after[0].applied_sequence);
    try std.testing.expectEqual(target_sequence, after[0].target_sequence);
    try std.testing.expect(!after[0].catch_up_required);
}

test "db catch-up rebuilds dense coverage before vacuous target advance" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const target_sequence: u64 = 7;
    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });

    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(stored_key);
    try db.core.store.putBatch(&.{
        .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
    }, &.{});

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 0 });
    defer alloc.free(payload);
    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    try writes.append(alloc, .{ .key = artifact_key, .value = payload });
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }
    try DB.appendDenseArtifactCounterMutations(
        alloc,
        db.core.store,
        db.core.index_manager,
        &writes,
        &.{},
        &owned_keys,
        &owned_values,
    );
    try db.core.store.putBatch(writes.items, &.{});
    try db.core.putArtifactPresenceMarker();

    const source_only_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = target_sequence,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    defer alloc.free(source_only_payload);
    try db.core.store.appendReplayOpaque(alloc, target_sequence, source_only_payload);

    {
        const before = try db.listDerivedReplayDebt(alloc);
        defer {
            for (before) |*status| status.deinit(alloc);
            alloc.free(before);
        }
        try std.testing.expectEqual(@as(usize, 1), before.len);
        try std.testing.expectEqual(@as(u64, 0), before[0].applied_sequence);
        try std.testing.expectEqual(target_sequence, before[0].target_sequence);
        try std.testing.expect(before[0].catch_up_required);
    }

    try db.catchUpPendingDerivedReplay();

    {
        const after = try db.listDerivedReplayDebt(alloc);
        defer {
            for (after) |*status| status.deinit(alloc);
            alloc.free(after);
        }
        try std.testing.expectEqual(@as(usize, 1), after.len);
        try std.testing.expectEqual(target_sequence, after[0].applied_sequence);
        try std.testing.expectEqual(target_sequence, after[0].target_sequence);
        try std.testing.expect(!after[0].catch_up_required);
    }

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db lifecycle persists configured doc identity namespace for batch writes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 11, .range_id = 13 };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = namespace,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            },
        });

        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(namespace, "doc:a"), state.canonical_doc_id);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(namespace.table_id, stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(namespace.shard_id, stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(namespace.range_id, stats.doc_identity.namespace_range_id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
        });
        defer reopened.close();
        try std.testing.expect(reopened.core.identity_namespace.eql(namespace));
    }

    try std.testing.expectError(error.IdentityNamespaceMismatch, DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 14 },
    }));
}

test "db lifecycle preferred identity namespace seeds new stores but preserves existing namespace" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var managed_path_buf: [256]u8 = undefined;
    const managed_path = TestHelpers.tempPath(&managed_path_buf);
    defer TestHelpers.cleanupTempDir(managed_path);

    const managed_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 7001 };
    {
        var db = try DB.open(alloc, std.mem.span(managed_path), .{
            .start_index_workers = false,
            .identity_namespace = managed_namespace,
            .prefer_existing_identity_namespace = true,
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        try std.testing.expect(db.core.identity_namespace.eql(managed_namespace));
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(managed_path), .{
            .start_index_workers = false,
            .identity_namespace = .{ .table_id = 7, .shard_id = 7002, .range_id = 7002 },
            .prefer_existing_identity_namespace = true,
        });
        defer reopened.close();
        try std.testing.expect(reopened.core.identity_namespace.eql(managed_namespace));
    }

    var legacy_path_buf: [256]u8 = undefined;
    const legacy_path = TestHelpers.tempPath(&legacy_path_buf);
    defer TestHelpers.cleanupTempDir(legacy_path);

    {
        var legacy = try DB.open(alloc, std.mem.span(legacy_path), .{
            .start_index_workers = false,
        });
        defer legacy.close();
        try legacy.batch(.{
            .writes = &.{.{ .key = "doc:legacy", .value = "{\"name\":\"legacy\"}" }},
        });
    }

    {
        var opened = try DB.open(alloc, std.mem.span(legacy_path), .{
            .start_index_workers = false,
            .identity_namespace = managed_namespace,
            .prefer_existing_identity_namespace = true,
        });
        defer opened.close();
        try std.testing.expect(opened.core.identity_namespace.eql(doc_identity.default_namespace));
    }
}

test "db lifecycle can reassign identity namespace for rebuild" {
    const DB = @import("mod.zig").DB;
    const doc_set = @import("doc_set.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 8, .shard_id = 801, .range_id = 8001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 8, .shard_id = 802, .range_id = 8002 };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = old_namespace,
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(old_namespace, "doc:a"), state.canonical_doc_id);
    }

    try db.reassignIdentityNamespaceForInternalTransition(new_namespace);
    try std.testing.expect(db.core.identity_namespace.eql(new_namespace));

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:a"), state.canonical_doc_id);
        try std.testing.expectEqual(@as(u32, 1), ordinal);
        try std.testing.expectEqual(@as(u64, 1), state.created_generation);
    }

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(new_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(new_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(new_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expect(!stats.doc_identity.rebuild_required);

    const generation = try db.currentIdentityReadGenerationForRequest(null);
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, db.search(alloc, .{
        .limit = 10,
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = old_namespace,
            .identity_read_generation = generation,
        },
    }));

    var ok = try db.search(alloc, .{
        .limit = 10,
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = new_namespace,
            .identity_read_generation = generation,
        },
    });
    defer ok.deinit();
    try std.testing.expectEqual(@as(u32, 1), ok.total_hits);
}

test "db lifecycle strict namespace reopen recovers after identity reassignment repair" {
    const DB = @import("mod.zig").DB;
    const doc_set = @import("doc_set.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 38, .shard_id = 3801, .range_id = 38001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 38, .shard_id = 3802, .range_id = 38002 };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
    }

    try std.testing.expectError(error.IdentityNamespaceMismatch, DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = new_namespace,
    }));

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer db.close();
        try db.reassignIdentityNamespaceForInternalTransition(new_namespace);
    }

    {
        var repaired = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = new_namespace,
        });
        defer repaired.close();

        var filtered = try repaired.search(alloc, .{
            .query = .{ .match_all = {} },
            .filter_doc_ids = &.{"doc:a"},
            .filter_doc_ids_positive = true,
            .limit = 10,
        });
        defer filtered.deinit();
        try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
        try std.testing.expectEqualStrings("doc:a", filtered.hits[0].id);
        try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 1), filtered.hits[0].doc_ordinal);

        var txn = try repaired.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:a"), state.canonical_doc_id);
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, null), try doc_identity.lookupCanonicalOrdinalTxn(&txn, doc_identity.canonicalDocIdForNamespace(old_namespace, "doc:a")));
    }
}

test "db lifecycle identity namespace reassignment refreshes transaction recovery hook context" {
    const DB = @import("mod.zig").DB;
    const TxnResolverRecorder = TestHelpers.TxnResolverRecorder;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 28, .shard_id = 2801, .range_id = 28001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 28, .shard_id = 2802, .range_id = 28002 };
    var recorder = TxnResolverRecorder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = old_namespace,
        .transaction_recovery = .{
            .enabled = true,
            .interval_ms = 60_000,
            .resolver_ctx = &recorder,
            .resolve_participant_fn = TxnResolverRecorder.resolve,
        },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });

    const identity_ctx = db.transaction_recovery_identity_context orelse return error.TestExpectedEqual;
    try std.testing.expect(identity_ctx.identity_namespace.eql(old_namespace));
    try std.testing.expect(db.transaction_runtime.?.config.resolution_extra_hooks.build != null);
    try std.testing.expectEqual(
        @intFromPtr(identity_ctx),
        @intFromPtr(db.transaction_runtime.?.config.resolution_extra_hooks.ctx.?),
    );

    try db.reassignIdentityNamespaceForInternalTransition(new_namespace);
    try std.testing.expect(db.core.identity_namespace.eql(new_namespace));
    try std.testing.expect(identity_ctx.identity_namespace.eql(new_namespace));

    var txn = try db.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
    const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:a"), state.canonical_doc_id);
}

test "db lifecycle setSchema refreshes transaction recovery relational mode context" {
    const DB = @import("mod.zig").DB;
    const TxnResolverRecorder = TestHelpers.TxnResolverRecorder;
    const schema_api_mod = @import("../../schema/mod.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var recorder = TxnResolverRecorder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .transaction_recovery = .{
            .enabled = true,
            .interval_ms = 60_000,
            .resolver_ctx = &recorder,
            .resolve_participant_fn = TxnResolverRecorder.resolve,
        },
    });
    defer db.close();

    const identity_ctx = db.transaction_recovery_identity_context orelse return error.TestExpectedEqual;
    try std.testing.expect(!identity_ctx.relational_base_rows);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try std.testing.expect(identity_ctx.relational_base_rows);
}

test "db lifecycle identity namespace reassignment is unavailable on status-only handles" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 18, .shard_id = 1801, .range_id = 18001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 18, .shard_id = 1802, .range_id = 18002 };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
    }

    {
        var status_db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer status_db.close();
        try std.testing.expectError(error.ReadOnly, status_db.reassignIdentityNamespaceForInternalTransition(new_namespace));
        const stats = try status_db.runtimeStatusStatsConsistent(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(old_namespace.table_id, stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(old_namespace.shard_id, stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(old_namespace.range_id, stats.doc_identity.namespace_range_id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer reopened.close();
        try std.testing.expect(reopened.core.identity_namespace.eql(old_namespace));
    }
}

test "db lifecycle doc identity stats expose coverage and tombstones" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
        },
    });

    var resolved_existing = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:a"});
    defer resolved_existing.deinit(alloc);
    var resolved_missing = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:missing"});
    defer resolved_missing.deinit(alloc);

    const fast = try db.stats(alloc);
    defer types.freeDBStats(alloc, fast);
    try std.testing.expectEqual(@as(u32, 3), fast.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 2), fast.doc_identity.allocated_ordinals);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(doc_identity.DocOrdinal) - 3), fast.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(!fast.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(!fast.doc_identity.rebuild_required);
    try std.testing.expect(!fast.doc_identity.complete);
    try std.testing.expectEqual(@as(u64, 2), fast.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.ordinal_list_docs);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.doc_key_list_count);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.doc_key_list_docs);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.missing_ordinal_coverage_count);

    try db.batch(.{
        .deletes = &.{"doc:b"},
    });

    const diagnostic = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, diagnostic);
    try std.testing.expect(diagnostic.doc_identity.complete);
    try std.testing.expectEqual(@as(u32, 3), diagnostic.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 2), diagnostic.doc_identity.allocated_ordinals);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(doc_identity.DocOrdinal) - 3), diagnostic.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(!diagnostic.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(!diagnostic.doc_identity.rebuild_required);
    try std.testing.expectEqual(@as(u64, 2), diagnostic.doc_identity.state_rows);
    try std.testing.expectEqual(@as(u64, 1), diagnostic.doc_identity.live_ordinals);
    try std.testing.expectEqual(@as(u64, 1), diagnostic.doc_identity.tombstone_ordinals);
    try std.testing.expect(diagnostic.doc_identity.min_created_generation > 0);
    try std.testing.expectEqual(diagnostic.doc_identity.min_created_generation, diagnostic.doc_identity.max_created_generation);
    try std.testing.expect(diagnostic.doc_identity.min_deleted_generation > diagnostic.doc_identity.min_created_generation);
    try std.testing.expectEqual(diagnostic.doc_identity.min_deleted_generation, diagnostic.doc_identity.max_deleted_generation);
    try std.testing.expectEqual(@as(u64, 1), diagnostic.doc_identity.scanned_primary_docs);
    try std.testing.expectEqual(@as(u64, 0), diagnostic.doc_identity.primary_docs_missing_ordinals);
    try std.testing.expectEqual(@as(u64, 0), diagnostic.doc_identity.primary_docs_missing_identity_state);
    try std.testing.expectEqual(@as(u64, 0), diagnostic.doc_identity.primary_docs_with_tombstone_ordinals);

    const legacy_key = try internal_keys.documentKeyAlloc(alloc, "doc:legacy");
    defer alloc.free(legacy_key);
    try db.core.store.put(legacy_key, "{\"name\":\"legacy\"}");

    const resurrected_key = try internal_keys.documentKeyAlloc(alloc, "doc:b");
    defer alloc.free(resurrected_key);
    try db.core.store.put(resurrected_key, "{\"name\":\"resurrected\"}");

    const repaired_diagnostic = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, repaired_diagnostic);
    try std.testing.expectEqual(@as(u64, 3), repaired_diagnostic.doc_identity.scanned_primary_docs);
    try std.testing.expectEqual(@as(u64, 1), repaired_diagnostic.doc_identity.primary_docs_missing_ordinals);
    try std.testing.expectEqual(@as(u64, 0), repaired_diagnostic.doc_identity.primary_docs_missing_identity_state);
    try std.testing.expectEqual(@as(u64, 1), repaired_diagnostic.doc_identity.primary_docs_with_tombstone_ordinals);
    try std.testing.expect(repaired_diagnostic.doc_identity.rebuild_required);
}

test "db lifecycle doc identity stats flag ordinal capacity exhaustion" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, std.math.maxInt(doc_identity.DocOrdinal), .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(std.math.maxInt(doc_identity.DocOrdinal), stats.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(stats.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(stats.doc_identity.rebuild_required);

    try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
        .writes = &.{.{ .key = "doc:overflow", .value = "{\"name\":\"overflow\"}" }},
    }));
}

test "db lifecycle pending work stats track replay stream sequence" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expectEqual(@as(u64, 0), replay_stream_mod.lastSequence(db.core.store, 0));
    try std.testing.expectEqual(@as(u64, 0), db.pendingWorkStats().derived_target_sequence);

    const record = change_journal_mod.Record{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    };
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);
    try replay_stream_mod.appendOpaque(alloc, db.core.store, 1, payload);

    try std.testing.expectEqual(@as(u64, 1), replay_stream_mod.lastSequence(db.core.store, 0));
    try std.testing.expectEqual(@as(u64, 1), db.pendingWorkStats().derived_target_sequence);
}

test "db lifecycle open preserves existing change journal records" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const payload = try change_journal_mod.encodeRecord(alloc, .{
            .sequence = 1,
            .changed_doc_keys = &.{"doc:legacy"},
            .target_hints = &.{.full_text},
        });
        defer alloc.free(payload);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, 1, payload);
        try std.testing.expectEqual(@as(u64, 1), replay_stream_mod.lastSequence(db.core.store, 0));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const entries = try replay_stream_mod.iterateFrom(alloc, reopened.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 1), reopened.pendingWorkStats().derived_target_sequence);
}

test "db lifecycle lsm primary reopens explicit dense replay stream state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":8,\"metric\":\"l2_squared\"}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:1", .value = "{\"embedding\":[1,0,0,0,0,0,0,0]}" },
                .{ .key = "doc:2", .value = "{\"embedding\":[0,1,0,0,0,0,0,0]}" },
            },
            .sync_level = .write,
        });
        try db.sync(true);

        try std.testing.expectEqual(@as(u64, 1), replay_stream_mod.lastSequence(db.core.store, 0));
        const live_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
        defer {
            for (live_entries) |*entry| entry.deinit(alloc);
            alloc.free(live_entries);
        }
        try std.testing.expectEqual(@as(usize, 1), live_entries.len);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = db_config.primary_lsm_options_default },
        .open_mode = .query_readonly,
    });
    defer reopened.close();

    const entries = try replay_stream_mod.iterateFrom(alloc, reopened.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].sequence);
}

test "db lifecycle lsm generated chunked enrichment publishes replay stream state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = db_config.primary_lsm_options_default },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"dense_idx\"}}",
    });

    var doc_idx: usize = 0;
    while (doc_idx < 40) : (doc_idx += 4) {
        var writes: [4]types.BatchWrite = undefined;
        var owned_keys: [4][]u8 = undefined;
        var owned_values: [4][]u8 = undefined;
        defer {
            for (owned_keys, owned_values) |key, value| {
                alloc.free(key);
                alloc.free(value);
            }
        }
        for (0..4) |offset| {
            owned_keys[offset] = try std.fmt.allocPrint(alloc, "doc:{d}", .{doc_idx + offset});
            owned_values[offset] = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"doc-{d}\",\"body\":\"abcdefghijklmno {d}\"}}",
                .{ doc_idx + offset, doc_idx + offset },
            );
            writes[offset] = .{
                .key = owned_keys[offset],
                .value = owned_values[offset],
            };
        }

        try db.batch(.{
            .writes = writes[0..],
            .sync_level = .write,
        });
    }

    try db.runUntilIdle();
    try db.sync(true);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 0), stats.enrichment.error_count);
    try std.testing.expect(stats.enrichment.applied_sequence > 0);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:0", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expect(artifacts.len > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "dense_idx", "abcdefgh", 3);
    defer alloc.free(query_vec);
    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .dense = .{
            .vector = query_vec,
            .k = 5,
        },
        .limit = 1,
        .include_stored = true,
    });
    defer result.deinit();
    try std.testing.expect(result.total_hits > 0);
}

test "db lifecycle basic batch/get works with in-memory lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm_memory = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
        },
    });

    const raw = try db.get(alloc, "doc:b");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"beta\"") != null);
}

test "db enrichment restart supervisor recovers transient start failures" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();
    const runtime = db.enrichment_runtime orelse return error.TestUnexpectedResult;
    try std.testing.expect(runtime.isStarted());
    try std.testing.expect(DB.SchemaRuntimeCallbacks.quiesce_enrichment_for_structural_mutation(&db));
    try std.testing.expect(!runtime.isStarted());

    DB.ArtifactRepairCallbacks.set_test_enrichment_restart_failures_remaining(std.math.maxInt(u32));
    defer DB.ArtifactRepairCallbacks.set_test_enrichment_restart_failures_remaining(0);
    try std.testing.expectError(
        error.TestTransientEnrichmentRestart,
        DB.SchemaRuntimeCallbacks.restart_enrichment_after_structural_mutation(&db, "test", "semantic_idx"),
    );
    const degraded = DB.LifecycleCallbacks.enrichment_stats_with_supervisor_state(&db, .{});
    try std.testing.expect(degraded.retrying);
    try std.testing.expect(!degraded.worker_failed);
    const pending = db.pendingWorkStats();
    try std.testing.expect(pending.enrichment.retrying);
    try std.testing.expect(!pending.enrichment.worker_failed);
    const operational = try db.stats(alloc);
    defer types.freeDBStats(alloc, operational);
    try std.testing.expect(operational.enrichment.retrying);
    try std.testing.expect(!operational.enrichment.worker_failed);
    const diagnostic = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, diagnostic);
    try std.testing.expect(diagnostic.enrichment.retrying);
    try std.testing.expect(!diagnostic.enrichment.worker_failed);
    DB.ArtifactRepairCallbacks.set_test_enrichment_restart_failures_remaining(12);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    try std.testing.expectEqual(@as(u32, 0), DB.ArtifactRepairCallbacks.test_enrichment_restart_failures_remaining());
    try std.testing.expect(db.async_context.enrichment_desired_running.load(.acquire));
    try std.testing.expect(runtime.isStarted());
}

test "db enrichment status does not wait for lifecycle mutation" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();

    db_internal.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
    const fallback = types.EnrichmentStats{
        .enabled = true,
        .target_sequence = 41,
        .applied_sequence = 37,
    };
    const status = DB.LifecycleCallbacks.enrichment_stats_with_supervisor_state(&db, fallback);
    db.async_context.enrichment_lifecycle_mutex.unlock();

    try std.testing.expect(status.enabled);
    try std.testing.expectEqual(@as(u64, 41), status.target_sequence);
    try std.testing.expectEqual(@as(u64, 37), status.applied_sequence);
    try std.testing.expect(status.retrying);
    try std.testing.expect(!status.worker_failed);
}

test "db lifecycle basic batch/get works with memory primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
        },
    });

    const raw = try db.get(alloc, "doc:a");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"alpha\"") != null);
}

test "db lifecycle in-memory primary backends keep derived log off disk" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    inline for ([_]db_config.PrimaryBackend{
        .{ .mem = .{} },
        .{ .lsm_memory = .{} },
    }) |primary_backend| {
        var path_buf: [256]u8 = undefined;
        const path = TestHelpers.tempPath(&path_buf);
        defer TestHelpers.cleanupTempDir(path);

        {
            var db = try DB.open(alloc, std.mem.span(path), .{
                .primary_backend = primary_backend,
            });
            defer db.close();

            try db.batch(.{
                .writes = &.{
                    .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
                },
            });
        }

        const derived_log_path = try std.fmt.allocPrint(alloc, "{s}/derived_log", .{std.mem.span(path)});
        defer alloc.free(derived_log_path);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openDir(std.testing.io, derived_log_path, .{}));
    }
}

test "db lifecycle can override change journal backend to lmdb" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm_memory = .{} },
            .change_journal_backend = .lmdb,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            },
        });
    }

    const change_journal_path = try std.fmt.allocPrint(alloc, "{s}/change_journal", .{std.mem.span(path)});
    defer alloc.free(change_journal_path);
    var dir = try std.Io.Dir.cwd().openDir(std.testing.io, change_journal_path, .{});
    dir.close(std.testing.io);
}

test "db lifecycle basic batch/get survives reopen with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = db_config.primary_lsm_options_default },
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
            },
        });
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
        defer reopened.close();

        const raw = try reopened.get(alloc, "doc:a");
        defer if (raw) |value| alloc.free(value);
        try std.testing.expect(raw != null);
        try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"alpha\"") != null);
    }
}

test "db lifecycle doc identity lsm primary compaction preserves ordinals" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const primary_backend: db_config.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"body\":\"beta\"}" },
            },
            .sync_level = .write,
        });
        try db.core.store.flushBufferedWritesWithOptions(.{ .compact = true });

        {
            var txn = try db.core.store.beginProbeTxn();
            defer txn.abort();
            try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 1), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a"));
            try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b"));
            const doc_b = (try doc_identity.lookupDocIdTxn(alloc, &txn, 2)) orelse return error.TestUnexpectedResult;
            defer alloc.free(doc_b);
            try std.testing.expectEqualStrings("doc:b", doc_b);
        }

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:c", .value = "{\"body\":\"gamma\"}" },
                .{ .key = "doc:a", .value = "{\"body\":\"alpha updated\"}" },
            },
            .deletes = &.{"doc:b"},
            .sync_level = .write,
        });
        try db.core.store.flushBufferedWritesWithOptions(.{ .compact = true });

        var compacted_txn = try db.core.store.beginProbeTxn();
        defer compacted_txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 1), try doc_identity.lookupOrdinalTxn(alloc, &compacted_txn, "doc:a"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupOrdinalTxn(alloc, &compacted_txn, "doc:b"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 3), try doc_identity.lookupOrdinalTxn(alloc, &compacted_txn, "doc:c"));
        const doc_c = (try doc_identity.lookupDocIdTxn(alloc, &compacted_txn, 3)) orelse return error.TestUnexpectedResult;
        defer alloc.free(doc_c);
        try std.testing.expectEqualStrings("doc:c", doc_c);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer reopened.close();

        var txn = try reopened.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 1), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 3), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:c"));
        const doc_a = (try doc_identity.lookupDocIdTxn(alloc, &txn, 1)) orelse return error.TestUnexpectedResult;
        defer alloc.free(doc_a);
        try std.testing.expectEqualStrings("doc:a", doc_a);
    }
}

test "db lifecycle group created-at metadata is written once and readable" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expect((try db.getGroupCreatedAtMillis(alloc, 42)) == null);
    try std.testing.expectEqual(@as(u64, 1234), try db.ensureGroupCreatedAtMillis(alloc, 42, 1234));
    try std.testing.expectEqual(@as(u64, 1234), (try db.getGroupCreatedAtMillis(alloc, 42)).?);
    try std.testing.expectEqual(@as(u64, 1234), try db.ensureGroupCreatedAtMillis(alloc, 42, 5678));
}

test "db lifecycle rw lock allows search and scan while shared read lock is held" {
    const DB = @import("mod.zig").DB;
    const SharedReadLockHold = TestHelpers.SharedReadLockHold(DB);
    const ConcurrentReadProbe = TestHelpers.ConcurrentReadProbe(DB);
    const alloc = std.heap.c_allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
        .sync_level = .full_text,
    });

    var held = SharedReadLockHold{ .db = &db };
    const held_thread = try std.Thread.spawn(.{}, SharedReadLockHold.run, .{&held});
    try std.testing.expect(db_internal.waitForAtomicU8(&held.acquired, 1, 10_000));

    var search_probe = ConcurrentReadProbe{ .db = &db };
    const search_thread = try std.Thread.spawn(.{}, ConcurrentReadProbe.runSearch, .{&search_probe});

    try std.testing.expect(db_internal.waitForAtomicU8(&search_probe.started, 1, 10_000));
    try std.testing.expect(db_internal.waitForAtomicU8(&search_probe.done, 1, 10_000));
    try std.testing.expectEqual(@as(u8, 0), search_probe.failed.load(.monotonic));
    search_thread.join();

    var scan_probe = ConcurrentReadProbe{ .db = &db };
    const scan_thread = try std.Thread.spawn(.{}, ConcurrentReadProbe.runScan, .{&scan_probe});
    try std.testing.expect(db_internal.waitForAtomicU8(&scan_probe.started, 1, 10_000));
    try std.testing.expect(db_internal.waitForAtomicU8(&scan_probe.done, 1, 10_000));
    try std.testing.expectEqual(@as(u8, 0), scan_probe.failed.load(.monotonic));
    scan_thread.join();

    held.release.store(1, .monotonic);
    held_thread.join();
}

test "db lifecycle rw lock keeps batch writes blocked behind shared read lock" {
    const DB = @import("mod.zig").DB;
    const SharedReadLockHold = TestHelpers.SharedReadLockHold(DB);
    const ConcurrentWriteProbe = TestHelpers.ConcurrentWriteProbe(DB);
    const alloc = std.heap.c_allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
    });

    var held = SharedReadLockHold{ .db = &db };
    const held_thread = try std.Thread.spawn(.{}, SharedReadLockHold.run, .{&held});
    try std.testing.expect(db_internal.waitForAtomicU8(&held.acquired, 1, 10_000));

    var write_probe = ConcurrentWriteProbe{ .db = &db };
    const write_thread = try std.Thread.spawn(.{}, ConcurrentWriteProbe.runBatch, .{&write_probe});

    try std.testing.expect(db_internal.waitForAtomicU8(&write_probe.started, 1, 10_000));

    var still_blocked = true;
    var attempts: usize = 0;
    while (attempts < 10_000) : (attempts += 1) {
        if (write_probe.done.load(.monotonic) != 0 or write_probe.failed.load(.monotonic) != 0) {
            still_blocked = false;
            break;
        }
        db_internal.spinOrYield();
    }
    try std.testing.expect(still_blocked);

    held.release.store(1, .monotonic);
    held_thread.join();
    write_thread.join();
    try std.testing.expectEqual(@as(u8, 0), write_probe.failed.load(.monotonic));
    try std.testing.expectEqual(@as(u8, 1), write_probe.done.load(.monotonic));

    const doc = (try db.get(alloc, "doc:b")) orelse return error.TestExpectedEqual;
    defer alloc.free(doc);
    try std.testing.expectEqualStrings("{\"title\":\"bravo\"}", doc);
}
