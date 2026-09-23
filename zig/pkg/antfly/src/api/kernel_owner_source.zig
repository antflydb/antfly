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

//! Provisioned group-local read/write adapters for the compiled storage owner.
//! Distributed sources retain routing, admission, consistency, aggregation,
//! and lifecycle; this source owns only coarse physical operations.

const std = @import("std");
const request_operation = @import("operation.zig");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const abi = @import("kernel_owner_abi");
const kernel_error_identity = @import("kernel_error_identity");
const client = @import("../storage/kernel_owner_client.zig");
const data_apply_client = @import("../storage/data_raft_apply_client.zig");
const descriptor_contract = @import("../storage/kernel_owner_descriptor.zig");
const backend_types = @import("../storage/backend_types.zig");
const db_types = @import("../storage/db/types.zig");
const runtime_callbacks = @import("../storage/db/runtime_callbacks.zig");
const ha_contract = @import("../storage/db/ha_contract.zig");
const document_artifact_child_range = @import("../storage/db/document_artifact_child_range.zig");
const text_memory = @import("../storage/db/text_memory_stats.zig");
const ha_commit_gate = @import("../storage/hot_standby/commit_gate.zig");
const ha_effects = @import("../storage/hot_standby/effects.zig");
const ha_replication_record = @import("../storage/hot_standby/replication_record.zig");
const runtime_preflight = @import("../storage/db/runtime_preflight.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_domain = @import("../metadata/domain.zig");
const backup_contract = @import("backup_contract.zig");
const distributed_graph = @import("distributed_graph.zig");
const query_response = @import("query_response.zig");
const runtime_status = @import("runtime_status.zig");
const restore_state_contract = @import("../storage/restore_state_contract.zig");
const read_gate = @import("../raft/read_gate.zig");
const feature_reads = @import("../raft/feature_reads.zig");
const table_catalog = @import("table_catalog.zig");
const table_read_source = @import("table_read_source.zig");
const table_reads = @import("local_query_contract.zig");
const storage_snapshot_source = @import("storage_snapshot_source.zig");
const storage_maintenance_source = @import("storage_maintenance_source.zig");
const table_write_source = @import("table_write_source.zig");
const table_writes = @import("antfly_source_root").antfly_sources.table_writes;
const transaction_recovery_source = @import("transaction_recovery_source.zig");
const common_config = @import("../common/config.zig");
const scraping = @import("antfly_scraping");
const completion_capsule = @import("../common/completion_installation_capsule.zig");

/// Native owner controls use the platform monotonic clock. In particular on
/// Darwin that clock is not std.Io's awake clock. Translate the remaining
/// budget once, before both cold owner acquisition and the compiled boundary.
fn platformDeadlineContext(context: request_operation.RequestContext) !request_operation.RequestContext {
    return context.platformDeadline();
}

test "distributed txn native lookup read-index rejects leader loss before storage execution" {
    const Barrier = struct {
        calls: usize = 0,
        fn wait(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.NotLeader;
        }
    };
    var barrier: Barrier = .{};
    // The gate must fail before touching any owner/catalog/storage fields.
    var source: ProvisionedKernelOwnerSource = undefined;
    source.read_safety_barrier = .{ .ptr = &barrier, .vtable = &.{ .wait_read_safe = Barrier.wait } };
    const reader = source.readSource();
    try std.testing.expect(reader.strict_read_index_absence);
    try std.testing.expectError(error.NotLeader, reader.lookupGroupLocal(std.testing.allocator, 7, "rows", "missing", .{}, .read_index));
    try std.testing.expectEqual(@as(usize, 1), barrier.calls);
    try source.prepareLookupRead(7, "missing", .{}, .stale);
    try std.testing.expectEqual(@as(usize, 1), barrier.calls);
}

test "source owner deadlines normalize executor clock epochs without extending budgets" {
    const FakeClock = struct {
        fn now(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const value: *const u64 = @ptrCast(@alignCast(raw.?));
            return .{ .nanoseconds = value.* };
        }
    };
    var clock_now: u64 = 10;
    var vtable = std.testing.io.vtable.*;
    vtable.now = FakeClock.now;
    const io: std.Io = .{ .userdata = &clock_now, .vtable = &vtable };
    const context: request_operation.RequestContext = .{ .deadline_ns = 10 + std.time.ns_per_s, .deadline_io = @import("../runtime_io_abi.zig").Borrow.init(&io) };
    const before = platform_time.monotonicNs();
    const normalized = try platformDeadlineContext(context);
    const after = platform_time.monotonicNs();
    try std.testing.expect(normalized.deadline_io == null);
    try std.testing.expect(normalized.deadline_ns.? >= before + std.time.ns_per_s);
    try std.testing.expect(normalized.deadline_ns.? <= after + std.time.ns_per_s);
    try normalized.ensureActive();
    clock_now = context.deadline_ns.?;
    try std.testing.expectError(error.DeadlineExceeded, platformDeadlineContext(context));
    try std.testing.expect((try platformDeadlineContext(.{})).deadline_ns == null);
    const native_context: request_operation.RequestContext = .{ .deadline_ns = after + std.time.ns_per_s };
    try std.testing.expectEqual(native_context.deadline_ns, (try platformDeadlineContext(native_context)).deadline_ns);
}

test "source owner routed admission preserves the fence clock" {
    const Fixture = struct {
        now: u64,
        io: std.Io = undefined,
        calls: usize = 0,
        fn clock(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return .{ .nanoseconds = self.now };
        }
        fn resolve(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: table_catalog.RouteQuery, deadline: ?u64) !table_catalog.RouteResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try table_catalog.RoutingBudget.initIo(deadline, self.io).checkpoint();
            try std.testing.expectEqual(self.now + std.time.ns_per_s, deadline.?);
            self.calls += 1;
            return .not_found;
        }
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.TestUnexpectedResult;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var vtable = std.testing.io.vtable.*;
    vtable.now = Fixture.clock;
    var request: Fixture = .{ .now = 10 };
    request.io = .{ .userdata = &request, .vtable = &vtable };
    var catalog: Fixture = .{ .now = 1000 * std.time.ns_per_s };
    catalog.io = .{ .userdata = &catalog, .vtable = &vtable };
    var source: ProvisionedKernelOwnerSource = undefined;
    source.catalog = .{ .ptr = &catalog, .io = @import("../runtime_io_abi.zig").Borrow.init(&catalog.io), .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free, .validate_route = Fixture.resolve } };
    const fence: metadata_api.CatalogRouteFence = .{
        .metadata_group_id = 1,
        .catalog_revision = 1,
        .table_id = 1,
        .topology_epoch = 1,
        .route = .{ .group_id = 2, .range_id = 2, .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 } },
        .admission_deadline_ns = request.now + std.time.ns_per_s,
        .admission_deadline_io = @import("../runtime_io_abi.zig").Borrow.init(&request.io),
    };
    try std.testing.expectError(error.TopologyChanged, source.validateRoutedRead(std.testing.allocator, fence, 2, "rows"));
    try std.testing.expectEqual(@as(usize, 1), catalog.calls);
    request.now = fence.admission_deadline_ns.?;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, source.validateRoutedRead(std.testing.allocator, fence, 2, "rows"));
    try std.testing.expectEqual(@as(usize, 1), catalog.calls);
}

pub const ProvisionedKernelOwnerSource = struct {
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    read_safety_barrier: read_gate.ReadSafetyBarrier,
    /// Selected once by the hosting runtime before any owner is opened.
    online_source_authority: @import("../storage/db/online_source_contract.zig").Authority = .raft,
    group_visible_root_generation: ?table_reads.GroupVisibleRootGenerationSource = null,
    transaction_recovery_source: ?transaction_recovery_source.Source = null,
    restore_descriptor_recovery: ?RestoreDescriptorRecovery = null,
    document_child_range_dispatch_source: ?table_write_source.TableWriteSource = null,
    resolution_candidate_source: ?runtime_callbacks.CandidateSource = null,
    coordinated_ttl: ?@import("../storage/coordinated_ttl.zig").Port = null,
    deferred_runtime_hooks: bool = false,
    runtime_hooks_ready: std.atomic.Value(bool) = .init(false),
    entity_sink: ?runtime_callbacks.EntitySink = null,
    runtime_status_cache: ?*runtime_status.TableRuntimeSnapshotCache = null,
    native_migration_policy: ?runtime_callbacks.DenseNativeMigrationPolicySource = null,
    promotion_leadership_source: ?table_writes.PromotionLeadershipSource = null,
    ha_write_gate: ?ha_contract.WriteGate = null,
    ha_async_mirror: ?ha_contract.AsyncEffectMirror = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    remote_content_configured: bool = false,
    secret_store: ?*anyopaque = null,
    context: client.Context = .{},
    owns_context: bool = true,
    mutex: std.atomic.Mutex = .unlocked,
    quiescing: bool = false,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    completion_installations: std.AutoHashMapUnmanaged(u64, *CompletionInstallation) = .empty,
    completion_installation_preparations: usize = 0,
    completion_filesystem_io: ?std.Io = null,

    publications: std.ArrayListUnmanaged(*PendingPublication) = .empty,
    owner_cache_hits: std.atomic.Value(u64) = .init(0),
    owner_cache_misses: std.atomic.Value(u64) = .init(0),

    // Requests only publish a bounded hint; native open/close belongs to the
    // independently owned DATA lifecycle worker. No caller memory escapes.
    recovery_owner_mutex: std.atomic.Mutex = .unlocked,
    recovery_owner_requests: [256]RecoveryOwnerRequest = @splat(.{}),
    recovery_owner_pending: std.atomic.Value(usize) = .init(0),
    recovery_owner_closing: std.atomic.Value(bool) = .init(false),
    recovery_owner_draining: std.atomic.Value(bool) = .init(false),
    recovery_owner_cursor: usize = 0,

    const RecoveryOwnerRequest = struct {
        group_id: u64 = 0,
        // Covers public names and qualified internal restore identities.
        table_name: [1024]u8 = undefined,
        table_name_len: usize = 0,
        revision: u64 = 0,

        fn name(self: *const @This()) []const u8 {
            return self.table_name[0..self.table_name_len];
        }
    };

    const CompletionInstallation = struct {
        binding: abi.completion_pool.InstallBinding,
        read_schema_json: []u8,
        settings_json: []u8,
        settings: @import("../common/table_storage.zig").Settings,
        active: bool = false,
        metadata_authorized: bool = false,
        identity_mismatch: bool = false,
        state: enum { installing, failed, backed } = .installing,
    };

    const PendingPublication = struct {
        group_id: u64,
        table_name: []u8,
    };

    const Identity = descriptor_contract.Identity;

    pub const CacheStats = struct {
        hit_count: u64 = 0,
        miss_count: u64 = 0,
    };

    pub const LoadedDescriptor = struct {
        path: []u8,
        schema_json: []u8,
        indexes_json: []u8,
        table_storage: ?@import("../common/table_storage.zig").Settings = null,
        generation: u64,
        initial_range: ?db_types.ByteRange = null,
        identity: descriptor_contract.Identity,
        restore: ?@import("../storage/restore_identity.zig").Identity = null,

        pub fn view(self: *const LoadedDescriptor) descriptor_contract.Descriptor {
            return .{
                .lsm_root_generation = self.generation,
                .identity = self.identity,
                .schema_json = self.schema_json,
                .indexes_json = self.indexes_json,
                .table_storage = self.table_storage,
                .initial_range = self.initial_range,
                .restore = self.restore,
            };
        }

        pub fn deinit(self: *LoadedDescriptor, alloc: std.mem.Allocator) void {
            alloc.free(self.path);
            alloc.free(self.schema_json);
            alloc.free(self.indexes_json);
            descriptor_contract.freeInitialRange(alloc, self.initial_range);
            if (self.restore) |*identity| identity.deinit(alloc);
            self.* = undefined;
        }
    };

    const LeaseAdmission = enum { shared, exclusive, exclusive_if_idle, completion_install };

    const Entry = struct {
        group_id: u64,
        table_name: []u8,
        generation: u64,
        identity: Identity,
        schema_json: []u8,
        indexes_json: []u8,
        restore_bootstrap_json: []u8,
        initial_range: ?db_types.ByteRange = null,
        restore_cancel_recovery: bool = false,
        restore_ha_replay: bool = false,
        table_storage: ?@import("../common/table_storage.zig").Settings = null,
        restore: ?@import("../storage/restore_identity.zig").Identity = null,
        owner: client.Owner,
        // Exact descriptor/target proof, owned by this physical generation.
        // Shared repair steps may reuse it until a structural follow-up is due.
        repair_target: ?[]u8 = null,
        repair_configuration: ?abi.ReconcileResult = null,
        active_users: usize = 0,
        /// Foreground admission or durable background debt owns residency.
        /// Status and maintenance leases only borrow it until their release.
        resident: bool = false,
        transient_retirement_pending: bool = false,
        /// Writer preference for structural reconciliation. Once an exclusive
        /// caller observes live readers, new observational/foreground readers
        /// must stop entering so the existing leases can drain.
        exclusive_pending: bool = false,
        exclusive_active: bool = false,
        retired: bool = false,
        closing: bool = false,
        bulk_ingest_active: std.atomic.Value(bool) = .init(false),
    };

    const Lease = struct {
        source: *ProvisionedKernelOwnerSource,
        entry: *Entry,
        exclusive: bool = false,
        active: bool = true,

        fn owner(self: *Lease) *client.Owner {
            return &self.entry.owner;
        }

        fn downgrade(self: *Lease) void {
            lock(&self.source.mutex);
            defer self.source.mutex.unlock();
            std.debug.assert(self.active and self.exclusive and self.entry.active_users == 1);
            self.entry.exclusive_active = false;
            self.exclusive = false;
        }

        fn retireAfterConfigurationFailure(self: *Lease) void {
            lock(&self.source.mutex);
            self.entry.retired = true;
            self.source.mutex.unlock();
        }

        fn requestTransientRetirement(self: *Lease) void {
            lock(&self.source.mutex);
            defer self.source.mutex.unlock();
            if (!self.entry.resident) self.entry.transient_retirement_pending = true;
        }

        fn retain(self: *Lease) void {
            lock(&self.source.mutex);
            defer self.source.mutex.unlock();
            self.entry.resident = true;
            self.entry.transient_retirement_pending = false;
        }

        fn deinit(self: *Lease) void {
            if (!self.active) return;
            self.source.release(self.entry, self.exclusive);
            self.active = false;
        }
    };

    pub fn init(
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        read_safety_barrier: read_gate.ReadSafetyBarrier,
    ) ProvisionedKernelOwnerSource {
        return .{
            .alloc = alloc,
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .read_safety_barrier = read_safety_barrier,
        };
    }

    pub fn withGroupVisibleRootGeneration(
        self: *ProvisionedKernelOwnerSource,
        source: ?table_reads.GroupVisibleRootGenerationSource,
    ) *ProvisionedKernelOwnerSource {
        self.group_visible_root_generation = source;
        return self;
    }

    pub fn withReadSafetyBarrier(
        self: *ProvisionedKernelOwnerSource,
        read_safety_barrier: read_gate.ReadSafetyBarrier,
    ) *ProvisionedKernelOwnerSource {
        self.read_safety_barrier = read_safety_barrier;
        return self;
    }

    pub fn withTransactionRecoverySource(
        self: *ProvisionedKernelOwnerSource,
        source: ?transaction_recovery_source.Source,
    ) *ProvisionedKernelOwnerSource {
        self.transaction_recovery_source = source;
        return self;
    }

    /// Generated child-range artifacts are routed by the distributed table
    /// source while the physical owner retains the durable outbox. The source
    /// is borrowed for synchronous batch calls and is never retained by the
    /// compiled provider.
    pub fn withDocumentChildRangeDispatchSource(
        self: *ProvisionedKernelOwnerSource,
        source: table_write_source.TableWriteSource,
    ) *ProvisionedKernelOwnerSource {
        self.document_child_range_dispatch_source = source;
        return self;
    }

    /// Install stable callback trampolines before restoring any owner. They
    /// refuse work until startup publishes the actual DATA callback targets.
    pub fn withDeferredRuntimeHooks(self: *ProvisionedKernelOwnerSource) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        std.debug.assert(!self.runtime_hooks_ready.load(.acquire));
        self.deferred_runtime_hooks = true;
        return self;
    }

    /// Publication is one-time: existing workers acquire the latch before
    /// reading these fields, and a retry may only supply identical targets.
    pub fn withRuntimeHooks(
        self: *ProvisionedKernelOwnerSource,
        candidate_source: ?runtime_callbacks.CandidateSource,
        entity_sink: ?runtime_callbacks.EntitySink,
        leadership_source: ?table_writes.PromotionLeadershipSource,
    ) !*ProvisionedKernelOwnerSource {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        if (self.runtime_hooks_ready.load(.acquire)) {
            if (!std.meta.eql(self.resolution_candidate_source, candidate_source) or
                !std.meta.eql(self.entity_sink, entity_sink) or
                !std.meta.eql(self.promotion_leadership_source, leadership_source))
                return error.PreparedCompletionActive;
            return self;
        }
        if (!self.deferred_runtime_hooks and self.entries.items.len != 0)
            return error.PreparedCompletionActive;
        self.resolution_candidate_source = candidate_source;
        self.entity_sink = entity_sink;
        self.promotion_leadership_source = leadership_source;
        self.runtime_hooks_ready.store(true, .release);
        return self;
    }

    fn runtimeHooksAvailable(self: *const ProvisionedKernelOwnerSource) bool {
        return !self.deferred_runtime_hooks or self.runtime_hooks_ready.load(.acquire);
    }

    /// HA policy stays in distributed control. The compiled owner performs the
    /// physical commit; this adapter fences before it and appends the exact
    /// coarse batch plus its provider-produced derived effect only after that
    /// commit succeeds.
    pub fn withHAControls(
        self: *ProvisionedKernelOwnerSource,
        gate: ?ha_contract.WriteGate,
        mirror: ?ha_contract.AsyncEffectMirror,
    ) *ProvisionedKernelOwnerSource {
        self.ha_write_gate = gate;
        self.ha_async_mirror = mirror;
        return self;
    }

    pub fn withSecretStore(self: *ProvisionedKernelOwnerSource, store: ?*anyopaque) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.secret_store = store;
        self.remote_content_configured = false;
        return self;
    }

    pub fn withRemoteContent(
        self: *ProvisionedKernelOwnerSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.remote_content = remote_content;
        self.remote_content_configured = false;
        return self;
    }

    fn ensureContextConfigured(self: *ProvisionedKernelOwnerSource) !void {
        try self.context.ensure();
        if (self.remote_content_configured) return;
        const security_json = try common_config.remoteContentSecurityJsonAlloc(self.alloc, self.remote_content);
        defer self.alloc.free(security_json);
        try self.context.configureRemoteContentSecurity(security_json);
        try self.context.configureSecrets(self.secret_store);
        self.remote_content_configured = true;
    }

    pub fn withStorageContextHandle(
        self: *ProvisionedKernelOwnerSource,
        handle: ?*anyopaque,
    ) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        std.debug.assert(self.context.handle == null);
        self.context.handle = handle;
        self.owns_context = false;
        // A borrowed process context must be fully configured before any
        // system store or table owner acquires it. Reconfiguring it lazily
        // here would race those existing owners and correctly return busy.
        self.remote_content_configured = true;
        return self;
    }

    /// Call only after every attached read/write source and lifecycle job has drained. Owner
    /// closure is deliberately centralized here so one live DB serves both
    /// operation families for its full group lifecycle.
    pub fn deinit(self: *ProvisionedKernelOwnerSource) void {
        self.recovery_owner_closing.store(true, .release);
        std.debug.assert(!self.recovery_owner_draining.load(.acquire));
        lock(&self.mutex);
        self.quiescing = true;
        for (self.entries.items) |entry| {
            std.debug.assert(entry.active_users == 0 and !entry.closing);
            entry.retired = true;
        }
        self.drainRetiredLocked(null, null);
        std.debug.assert(self.publications.items.len == 0);
        self.publications.deinit(self.alloc);
        self.entries.deinit(self.alloc);
        var installations = self.completion_installations.valueIterator();
        while (installations.next()) |record| {
            self.alloc.free(record.*.read_schema_json);
            self.alloc.free(record.*.settings_json);
            self.alloc.destroy(record.*);
        }
        self.completion_installations.deinit(self.alloc);
        self.entries = .empty;
        self.mutex.unlock();
        if (self.owns_context) self.context.deinit();
    }

    /// Close admission and join every DB-owned worker while its Raft,
    /// candidate, sink, and provider callback contexts are still alive.
    /// Attached request/apply sources and lifecycle jobs must already be stopped. Keep the
    /// registry and context valid until their ordinary final deinit.
    pub fn quiesce(self: *ProvisionedKernelOwnerSource, io: std.Io) !void {
        self.recovery_owner_closing.store(true, .release);
        while (true) {
            const drained = blk: {
                lock(&self.mutex);
                defer self.mutex.unlock();
                self.quiescing = true;
                for (self.entries.items) |entry| entry.retired = true;
                self.drainRetiredLocked(null, null);
                break :blk self.entries.items.len == 0;
            };
            if (drained) return;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    pub fn readSource(self: *ProvisionedKernelOwnerSource) table_read_source.TableReadSource {
        return .{
            .ptr = self,
            .strict_read_index_absence = true,
            .vtable = &.{
                .lookup = unsupportedTopLevelLookup,
                .scan = unsupportedTopLevelScan,
                .query = unsupportedTopLevelQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .preflight_query_group_local_routed = preflightQueryGroupLocalRouted,
                .lookup_group_local = lookupGroupLocal,
                .lookup_group_local_routed = lookupGroupLocalRouted,
                .scan_group_local_stream = scanGroupLocalStream,
                .scan_group_local = scanGroupLocal,
                .scan_group_local_routed_stream = scanGroupLocalRoutedStream,
                .scan_group_local_routed = scanGroupLocalRouted,
                .query_group_local = queryGroupLocal,
                .query_group_local_routed = queryGroupLocalRouted,
                .search_result_group_local = searchResultGroupLocal,
                .search_result_group_local_routed = searchResultGroupLocalRouted,
                .text_stats_group_local = textStatsGroupLocal,
                .text_stats_group_local_routed = textStatsGroupLocalRouted,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .algebraic_partials_group_local_routed = algebraicPartialsGroupLocalRouted,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_expand_group_local_routed = graphExpandGroupLocalRouted,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_hydrate_group_local_routed = graphHydrateGroupLocalRouted,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .graph_edges_group_local_routed = graphEdgesGroupLocalRouted,
                .observed_dynamic_field_capability_sets = observedDynamicFieldCapabilitySets,
                .document_artifact_manifest_group_local = documentArtifactManifestGroupLocal,
                .document_artifact_manifest_group_local_routed = documentArtifactManifestGroupLocalRouted,
                .document_artifact_manifests_group_local = documentArtifactManifestsGroupLocal,
                .document_artifact_manifests_group_local_routed = documentArtifactManifestsGroupLocalRouted,
            },
        };
    }

    pub fn writeSource(self: *ProvisionedKernelOwnerSource) table_write_source.TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch = unsupportedTopLevelBatch,
                .batch_group_local = batchGroupLocal,
                .replicated_batch_group_local = replicatedBatchGroupLocal,
                .backup_table_group_local = backupTableGroupLocal,
                .backup_pin_control = backupPinControl,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_begin_group_local_with_pre_decision_context = txnBeginGroupLocalWithPreDecisionContext,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_resolve_group_local_with_cancellation = txnResolveGroupLocalWithCancellation,
                .txn_resolve_group_local_until = txnResolveGroupLocalUntil,
                .txn_decide_group_local_with_pre_decision_context = txnDecideGroupLocalWithPreDecisionContext,
                .txn_status_group_local = txnStatusGroupLocal,
                .txn_status_group_local_until = txnStatusGroupLocalUntil,
                .txn_acknowledge_group_local = txnAcknowledgeGroupLocal,
                .txn_acknowledge_group_local_until = txnAcknowledgeGroupLocalUntil,
                .begin_bulk_ingest_group_local = beginBulkIngestGroupLocal,
                .finish_bulk_ingest_group_local = finishBulkIngestGroupLocal,
                .abort_bulk_ingest_group_local = abortBulkIngestGroupLocal,
                .corrupt_embedding_artifact_group_local = corruptEmbeddingArtifactGroupLocal,
                .reprocess_document_artifact_group_local = reprocessDocumentArtifactGroupLocal,
                .reprocess_document_artifact_range_group_local = reprocessDocumentArtifactRangeGroupLocal,
                .list_artifact_repair_issues_group_local = listArtifactRepairIssuesGroupLocal,
                .vector_migration_group_local = vectorMigrationGroupLocal,
                .graph_metric_maintenance_group_local = graphMetricMaintenanceGroupLocal,
                .repair_artifact_issues_group_local = repairArtifactIssuesGroupLocal,
                .repair_artifact_issues_group_local_controlled = repairArtifactIssuesGroupLocalControlled,
                .update_document_artifact_child_range_placement_group_local = updateDocumentArtifactChildRangePlacementGroupLocal,
                .apply_document_artifact_child_range_batch_group_local = applyDocumentArtifactChildRangeBatchGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
                .text_memory_attribution_stats_best_effort = textMemoryAttributionStatsBestEffort,
                .preflight_write_admission_group_local = preflightWriteAdmissionGroupLocal,
                .prepare_ha_seed_snapshot_group_local = prepareHASeedSnapshotGroupLocal,
                .capture_ha_seed_snapshot_group_local = captureHASeedSnapshotGroupLocal,
                .find_median_key_group_local = findMedianKeyGroupLocal,
                .reconcile_table_group_local = reconcileTableGroupLocal,
                .reconcile_table_group_local_transient = reconcileTableGroupLocalTransient,
                .retire_table_group_local = retireTableGroupLocal,
                .reconcile_table_group_local_observed = reconcileTableGroupLocalObserved,
                .local_runtime_status_group_local = localRuntimeStatusGroupLocal,
            },
        };
    }

    pub fn captureNativeRaftSnapshot(self: *ProvisionedKernelOwnerSource, group_id: u64, applied_index: u64) !*anyopaque {
        // Keep the owner and its runtime alive across deferred materialization.
        // The compiled capture destroys its pin before releasing this lease.
        var catalog = try self.catalog.adminSnapshot();
        const table_name = blk: {
            defer self.catalog.freeAdminSnapshot(&catalog);
            const range = metadata_domain.findAdminRange(&catalog, group_id) orelse return error.UnknownGroup;
            const table = metadata_domain.findAdminTable(&catalog, range.table_id) orelse return error.TableNotFound;
            break :blk try self.alloc.dupe(u8, table.name);
        };
        defer self.alloc.free(table_name);
        const lease = try self.alloc.create(Lease);
        errdefer self.alloc.destroy(lease);
        lease.* = try self.acquire(group_id, table_name);
        errdefer lease.deinit();
        var capture = try lease.owner().captureNativeRaftSnapshot(group_id, applied_index);
        errdefer capture.deinit();
        try capture.bindLease(lease, struct {
            fn release(ptr: ?*anyopaque) callconv(.c) void {
                const held: *Lease = @ptrCast(@alignCast(ptr.?));
                const alloc = held.source.alloc;
                held.deinit();
                alloc.destroy(held);
            }
        }.release);
        return capture.handle orelse unreachable;
    }

    pub fn snapshotSource(self: *ProvisionedKernelOwnerSource) storage_snapshot_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_publication = beginPublication,
                .end_publication = endPublication,
                .prepare = prepareSnapshot,
                .prepare_restore = prepareRestore,
                .reconcile_restore = reconcileRestore,
                .repair_published_restore = repairPublishedRestore,
                .promote = promoteSnapshot,
                .publish_prepared = publishPreparedSnapshot,
                .commit = commitSnapshot,
                .rollback = rollbackSnapshot,
                .destroy = destroySnapshot,
            },
        };
    }

    pub fn maintenanceSource(self: *ProvisionedKernelOwnerSource) storage_maintenance_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .run_lsm_round = runLsmMaintenanceRound,
                .run_dense_posting_round = runDensePostingMaintenanceRound,
                .publish_dense_checkpoints = publishDenseCheckpoints,
                .run_vector_block_round = runVectorBlockRound,
                .snapshot = maintenanceSnapshot,
                .publish_runtime_statuses = publishRuntimeStatuses,
            },
        };
    }

    /// Process-owner reuse replaces the legacy read/write cache split. Report
    /// one shared acquisition counter to both compatibility metric names until
    /// those public metrics are renamed around the owner model.
    pub fn cacheStats(self: *const ProvisionedKernelOwnerSource) CacheStats {
        return .{
            .hit_count = self.owner_cache_hits.load(.monotonic),
            .miss_count = self.owner_cache_misses.load(.monotonic),
        };
    }

    pub fn contextMetrics(self: *ProvisionedKernelOwnerSource) !abi.ContextMetricsResult {
        return try self.context.metrics();
    }

    pub fn storageContextHandle(self: *ProvisionedKernelOwnerSource) !?*anyopaque {
        try self.ensureContextConfigured();
        return self.context.handle;
    }

    /// Read one durable restore marker through the compiled storage owner.
    /// The returned wire value is fully owned by `alloc` and contains no DB
    /// implementation types.
    pub fn restoreState(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?restore_state_contract.State {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, descriptor.path, descriptor.view(), false, .transient, .{});
        defer lease.deinit();
        defer lease.requestTransientRetirement();
        var response = (try lease.owner().restoreStateJson(table_name)) orelse return null;
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(
            restore_state_contract.State,
            alloc,
            response.bytes(),
            .{},
        );
        defer parsed.deinit();
        return try parsed.value.cloneAlloc(alloc);
    }

    /// Run one bounded projection reconciliation while borrowing the same
    /// resident physical owner used by table reads and writes.
    /// Holds a generation for an admitted transition without exposing its DB.
    pub const TransitionLease = struct {
        lease: Lease,

        pub fn deinit(self: *TransitionLease) void {
            self.lease.deinit();
        }

        pub fn reconcile(self: *TransitionLease, apply_store: *data_apply_client.RaftApplyStore, alloc: std.mem.Allocator, expected: ?data_apply_client.AppliedDataBatch) !data_apply_client.RaftApplyStore.ReconcileResult {
            return try apply_store.reconcileAuthoritativeOwner(alloc, self.lease.owner().handle, self.lease.entry.group_id, expected, false, 256, 2 * 1024 * 1024);
        }

        pub fn mergeArtifactsPage(self: *TransitionLease, alloc: std.mem.Allocator, range: db_types.ByteRange, after_key: ?[]const u8) ![]db_types.BatchWrite {
            var response: abi.OwnedBytes = .{};
            try @import("kernel_error_identity").statusToError(abi.antfly_storage_owner_merge_artifacts_page(self.lease.owner().handle, &.{
                .table_name = .fromSlice(self.lease.entry.table_name),
                .range_start = .fromSlice(range.start),
                .range_end = .fromSlice(range.end),
                .after_key = .fromSlice(after_key orelse ""),
            }, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            var page = try @import("../storage/data_raft_projection_wire.zig").decodeGroupStatePageAlloc(alloc, response.slice());
            errdefer page.deinit(alloc);
            const rows = try alloc.alloc(db_types.BatchWrite, page.entries.len);
            for (page.entries, 0..) |entry, i| rows[i] = .{ .key = entry.key, .value = entry.value };
            alloc.free(page.entries);
            return rows;
        }

        fn relationalRead(self: *TransitionLease, comptime T: type, alloc: std.mem.Allocator, request: @import("../storage/db/relational_transition_contract.zig").Request) !T {
            var encoded: std.Io.Writer.Allocating = .init(alloc);
            defer encoded.deinit();
            var json: std.json.Stringify = .{ .writer = &encoded.writer };
            try @import("../storage/db/relational_integrity_json.zig").write(request, &json);
            var response: abi.OwnedBytes = .{};
            try kernel_error_identity.statusToError(abi.antfly_storage_owner_relational_transition_read(self.lease.owner().handle, &.{
                .table_name = .fromSlice(self.lease.entry.table_name),
                .request_json = .fromSlice(encoded.written()),
            }, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            // Handoff pages/manifests borrow the caller's bounded request
            // arena, exactly like their native counterparts. Never retain ABI
            // response bytes after the owner releases its output buffer.
            return std.json.parseFromSliceLeaky(T, alloc, response.slice(), .{ .allocate = .alloc_always });
        }

        /// Read bounded lifecycle metadata from this exact transition owner.
        /// The caller establishes its Raft read barrier before acquiring the
        /// lease. In particular, an unpublished split destination must not be
        /// re-resolved through the public table catalog here.
        pub fn readRelationalTopologyJson(self: *TransitionLease, alloc: std.mem.Allocator, mode: []const u8) ![]u8 {
            const Mode = @FieldType(@import("../raft/shard_ops.zig").TopologyReadRequest, "mode");
            _ = std.meta.stringToEnum(Mode, mode) orelse return error.InvalidArgument;
            const control = try std.json.Stringify.valueAlloc(alloc, .{ .mode = mode }, .{});
            defer alloc.free(control);
            const request = try table_reads.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_topology_json = control });
            defer alloc.free(request);
            var response = try self.lease.owner().lookupJson(self.lease.entry.table_name, request);
            defer response.deinit();
            return alloc.dupe(u8, response.bytes());
        }

        pub fn relationalTopologyStatus(self: *TransitionLease) !@import("../storage/db/relational_integrity_topology_contract.zig").Status {
            var arena = std.heap.ArenaAllocator.init(self.lease.source.alloc);
            defer arena.deinit();
            return self.relationalRead(@import("../storage/db/relational_integrity_topology_contract.zig").Status, arena.allocator(), .{ .status = {} });
        }

        pub fn relationalHandoffManifest(self: *TransitionLease, alloc: std.mem.Allocator, source: @import("../storage/db/relational_integrity_topology_contract.zig").Fence, destination: @import("../storage/db/relational_integrity_topology_contract.zig").Fence, lower: []const u8, upper: []const u8, primary_sequence: u64) !@import("../storage/db/relational_integrity_handoff_contract.zig").Manifest {
            return self.relationalRead(@import("../storage/db/relational_integrity_handoff_contract.zig").Manifest, alloc, .{ .manifest = .{ .source = source, .destination = destination, .lower = lower, .upper = upper, .primary_sequence = primary_sequence } });
        }

        pub fn relationalHandoffPage(self: *TransitionLease, alloc: std.mem.Allocator, manifest: @import("../storage/db/relational_integrity_handoff_contract.zig").Manifest, progress: @import("../storage/db/relational_integrity_handoff_contract.zig").Progress) !@import("../storage/db/relational_integrity_handoff_contract.zig").Page {
            return self.relationalRead(@import("../storage/db/relational_integrity_handoff_contract.zig").Page, alloc, .{ .page = .{ .manifest = manifest, .progress = progress } });
        }
    };

    pub fn leaseTransitionOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor) !TransitionLease {
        const path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer self.alloc.free(path);
        return .{ .lease = try self.acquireDescriptor(group_id, table_name, path, descriptor) };
    }

    pub fn reconcileDataRaftProjection(
        self: *ProvisionedKernelOwnerSource,
        apply_store: *data_apply_client.RaftApplyStore,
        work_alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        expected: ?data_apply_client.AppliedDataBatch,
        capture_handoff: bool,
        max_page_entries: usize,
        max_page_bytes: usize,
    ) !data_apply_client.RaftApplyStore.ReconcileResult {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try apply_store.reconcileAuthoritativeOwner(
            work_alloc,
            lease.owner().handle,
            group_id,
            expected,
            capture_handoff,
            max_page_entries,
            max_page_bytes,
        );
    }

    /// Borrow both resident group owners for one complete local split/merge
    /// phase. Acquisition is globally ordered so inverse group pairs cannot
    /// deadlock, while argument order remains source/destination or
    /// donor/receiver at the compiled ABI.
    pub fn runLocalTransition(
        self: *ProvisionedKernelOwnerSource,
        apply_store: ?*data_apply_client.RaftApplyStore,
        primary_group_id: u64,
        secondary_group_id: u64,
        table_name: []const u8,
        request: client.LocalTransitionRequest,
    ) !client.LocalTransitionResult {
        if (primary_group_id == secondary_group_id or
            request.primary_group_id != primary_group_id or
            request.secondary_group_id != secondary_group_id or
            !std.mem.eql(u8, request.table_name.slice(), table_name))
        {
            return error.InvalidTransitionRequest;
        }

        var primary_lease: ?Lease = null;
        defer if (primary_lease) |*lease| lease.deinit();
        var secondary_lease: ?Lease = null;
        defer if (secondary_lease) |*lease| lease.deinit();
        const primary_path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            primary_group_id,
        });
        defer self.alloc.free(primary_path);
        const secondary_path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            secondary_group_id,
        });
        defer self.alloc.free(secondary_path);
        const primary_descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = self.visibleRootGeneration(primary_group_id),
            .identity = .{
                .table_id = request.table_id,
                .shard_id = request.source_identity_shard_id,
                .range_id = request.source_identity_range_id,
            },
            .schema_json = request.schema_json.slice(),
            .indexes_json = request.indexes_json.slice(),
        };
        const secondary_descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = self.visibleRootGeneration(secondary_group_id),
            .identity = .{
                .table_id = request.table_id,
                .shard_id = request.target_identity_shard_id,
                .range_id = request.target_identity_range_id,
            },
            .schema_json = request.schema_json.slice(),
            .indexes_json = request.indexes_json.slice(),
        };
        if (primary_group_id < secondary_group_id) {
            primary_lease = try self.acquireDescriptor(
                primary_group_id,
                table_name,
                primary_path,
                primary_descriptor,
            );
            secondary_lease = try self.acquireDescriptor(
                secondary_group_id,
                table_name,
                secondary_path,
                secondary_descriptor,
            );
        } else {
            secondary_lease = try self.acquireDescriptor(
                secondary_group_id,
                table_name,
                secondary_path,
                secondary_descriptor,
            );
            primary_lease = try self.acquireDescriptor(
                primary_group_id,
                table_name,
                primary_path,
                primary_descriptor,
            );
        }
        return try primary_lease.?.owner().localTransition(
            secondary_lease.?.owner(),
            if (apply_store) |store| store.handle else null,
            request,
        );
    }

    pub fn retireAll(self: *ProvisionedKernelOwnerSource) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (self.installationPinsOwnerLocked(entry.group_id)) continue;
            entry.retired = true;
            count += 1;
        }
        self.drainRetiredLocked(null, null);
        return count;
    }

    /// Existing leases keep their owner alive; retirement prevents admission
    /// while close drains storage workers outside the registry mutex.
    pub fn retireTable(self: *ProvisionedKernelOwnerSource, table_name: []const u8) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (self.installationPinsOwnerLocked(entry.group_id)) continue;
            count += 1;
            entry.retired = true;
        }
        self.drainRetiredLocked(null, table_name);
        return count;
    }

    fn publicationPendingLocked(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) bool {
        for (self.publications.items) |publication| {
            if (publication.group_id == group_id and std.mem.eql(u8, publication.table_name, table_name)) return true;
        }
        return false;
    }

    fn registerPublication(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !*PendingPublication {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.installationPinsOwnerLocked(group_id)) return error.PreparedCompletionActive;
        if (self.publicationPendingLocked(group_id, table_name)) return error.StorageBusy;
        const publication = try self.alloc.create(PendingPublication);
        errdefer self.alloc.destroy(publication);
        publication.* = .{ .group_id = group_id, .table_name = try self.alloc.dupe(u8, table_name) };
        errdefer self.alloc.free(publication.table_name);
        try self.publications.append(self.alloc, publication);
        // Close admission before observing users or dropping the registry lock.
        // The gate outlives the last Entry, including an initially cold group.
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and std.mem.eql(u8, entry.table_name, table_name)) entry.retired = true;
        }
        return publication;
    }

    fn publicationDrained(self: *ProvisionedKernelOwnerSource, publication: *PendingPublication) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        self.drainRetiredLocked(publication.group_id, publication.table_name);
        for (self.entries.items) |entry| {
            // Closing entries remain registered while owner workers drain.
            if (entry.group_id == publication.group_id and std.mem.eql(u8, entry.table_name, publication.table_name)) return false;
        }
        return true;
    }

    fn beginPublication(ptr: *anyopaque, request: storage_snapshot_source.PublicationRequest) !*anyopaque {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try request.cancellation.check();
        const publication = try self.registerPublication(request.group_id, request.table_name);
        errdefer endPublication(ptr, publication);
        const deadline = std.Io.Clock.awake.now(request.io).nanoseconds + request.drain_timeout_ns;
        while (true) {
            try request.cancellation.check();
            if (self.publicationDrained(publication)) {
                try request.cancellation.check();
                return publication;
            }
            if (std.Io.Clock.awake.now(request.io).nanoseconds >= deadline) return error.StorageBusy;
            // Borrow the operation's I/O: cancellation and simulated time must
            // remain on the same runtime as the work whose leases are draining.
            try request.io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    fn endPublication(ptr: *anyopaque, handle: *anyopaque) void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const publication: *PendingPublication = @ptrCast(@alignCast(handle));
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.publications.items, 0..) |candidate, index| {
            if (candidate != publication) continue;
            _ = self.publications.orderedRemove(index);
            self.alloc.free(publication.table_name);
            self.alloc.destroy(publication);
            return;
        }
        unreachable;
    }

    fn installationPinsOwnerLocked(self: *const ProvisionedKernelOwnerSource, group_id: u64) bool {
        return !self.quiescing and self.completion_installations.contains(group_id);
    }

    fn drainRetiredLocked(self: *ProvisionedKernelOwnerSource, group_id: ?u64, table_name: ?[]const u8) void {
        while (true) {
            const index = for (self.entries.items, 0..) |entry, i| {
                if (!entry.retired or entry.closing or entry.active_users != 0) continue;
                if (self.installationPinsOwnerLocked(entry.group_id)) continue;
                if (group_id) |id| if (entry.group_id != id) continue;
                if (table_name) |name| if (!std.mem.eql(u8, entry.table_name, name)) continue;
                break i;
            } else return;
            self.destroyEntryAtIndexLocked(index);
        }
    }

    fn retireTableGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.retireGroupAndWait(group_id, table_name);
        return {};
    }

    fn captureHASeedSnapshotGroupLocal(ptr: *anyopaque, group_id: u64, table_name: []const u8, token: []const u8, destination: []const u8) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        // Preparation opened the owner before the exclusive HA freeze. Never
        // resolve catalog metadata or open a competing owner inside that freeze.
        var lease = (try self.acquireIfPresent(group_id, table_name)) orelse return error.StorageKernelOwnerUnavailable;
        defer lease.deinit();
        try lease.owner().captureHASeedSnapshot(table_name, token, destination);
        return {};
    }

    fn prepareHASeedSnapshotGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: u64,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = self.acquire(group_id, table_name) catch |err| {
            std.log.warn("HA seed owner acquisition failed group_id={d} err={s}", .{ group_id, @errorName(err) });
            return err;
        };
        defer lease.deinit();
        try lease.owner().prepareHASeedSnapshot(table_name, deadline_ns);
        return {};
    }

    /// Prevent new admissions to every resident generation for a dropped
    /// group, then wait for already-admitted work to release its leases before
    /// the caller moves or deletes the physical root.
    fn retireGroupAndWait(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        if (group_id == 0) return error.InvalidArgument;
        var wait_io_impl = std.Io.Threaded.init(self.alloc, .{});
        defer wait_io_impl.deinit();
        const wait_io = wait_io_impl.io();
        const deadline_ns = platform_time.monotonicNs() +| 5 * std.time.ns_per_s;
        const name_filter: ?[]const u8 = if (table_name.len == 0) null else table_name;
        while (true) {
            var active = false;
            {
                lock(&self.mutex);
                defer self.mutex.unlock();
                if (self.installationPinsOwnerLocked(group_id)) return error.PreparedCompletionActive;
                for (self.entries.items) |entry| {
                    if (entry.group_id == group_id and (name_filter == null or std.mem.eql(u8, entry.table_name, name_filter.?))) entry.retired = true;
                }
                self.drainRetiredLocked(group_id, name_filter);
                for (self.entries.items) |entry| {
                    if (entry.group_id == group_id and (name_filter == null or std.mem.eql(u8, entry.table_name, name_filter.?))) active = true;
                }
            }
            if (!active) return;
            if (platform_time.monotonicNs() >= deadline_ns) return error.StorageBusy;
            try wait_io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
        }
    }

    fn prepareSnapshot(
        ptr: *anyopaque,
        request: storage_snapshot_source.PrepareRequest,
    ) !*anyopaque {
        _ = ptr;
        const snapshot = try client.Snapshot.prepare(.{
            .path = .fromSlice(request.path),
            .table_name = .fromSlice(request.table_name),
            .group_id = request.group_id,
            .lsm_root_generation = request.lsm_root_generation,
            .identity_table_id = request.identity.table_id,
            .identity_shard_id = request.identity.shard_id,
            .identity_range_id = request.identity.range_id,
            .schema_json = .fromSlice(request.schema_json),
            .indexes_json = .fromSlice(request.indexes_json),
            .encoded_snapshot = .fromSlice(request.encoded_snapshot),
            .projection_store = request.projection_store,
            .expected_applied_index = request.expected_applied_index,
        });
        return snapshot.handle orelse error.StorageKernelFailure;
    }

    const EncodedRestoreRequest = struct {
        alloc: std.mem.Allocator,
        manifest_json: []u8,
        request: abi.RestorePrepareRequest,
        cancellation: db_types.CancellationToken,

        fn cancelled(ptr: ?*anyopaque) callconv(.c) u8 {
            const token: *const db_types.CancellationToken = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(token.isCancelled());
        }

        fn bindCancellation(self: *EncodedRestoreRequest) void {
            self.request.cancellation_ctx = &self.cancellation;
            self.request.cancellation_fn = cancelled;
        }

        fn deinit(self: *EncodedRestoreRequest) void {
            self.alloc.free(self.manifest_json);
            self.* = undefined;
        }
    };

    fn encodeRestoreRequest(
        self: *ProvisionedKernelOwnerSource,
        request: storage_snapshot_source.RestoreRequest,
    ) !EncodedRestoreRequest {
        const manifest_json = try std.json.Stringify.valueAlloc(self.alloc, request.manifest.*, .{
            .emit_null_optional_fields = false,
        });
        return .{
            .alloc = self.alloc,
            .manifest_json = manifest_json,
            .cancellation = request.cancellation,
            .request = .{
                .path = .fromSlice(request.path),
                .table_name = .fromSlice(request.table_name),
                .group_id = request.group_id,
                .lsm_root_generation = request.lsm_root_generation,
                .has_identity_namespace = @intFromBool(request.identity != null),
                .identity_table_id = if (request.identity) |identity| identity.table_id else 0,
                .identity_shard_id = if (request.identity) |identity| identity.shard_id else 0,
                .identity_range_id = if (request.identity) |identity| identity.range_id else 0,
                .backup_root = .fromSlice(request.backup_root),
                .backup_id = .fromSlice(request.manifest.backup_id),
                .artifact_backup_id = .fromSlice(request.artifact_backup_id),
                .source_identity = .fromSlice(request.source_identity),
                .snapshot_path = .fromSlice(request.shard.snapshot_path),
                .expected_artifact_size_bytes = request.shard.artifact_size_bytes,
                .expected_artifact_sha256 = .fromSlice(request.shard.artifact_sha256),
                .expected_native_manifest_size_bytes = request.shard.native_manifest_size_bytes,
                .expected_native_manifest_sha256 = .fromSlice(request.shard.native_manifest_sha256),
                .manifest_json = .fromSlice(manifest_json),
            },
        };
    }

    fn prepareRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !storage_snapshot_source.RestorePreparation {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        encoded.bindCancellation();
        return switch (try client.Snapshot.prepareRestore(encoded.request)) {
            .prepared => |snapshot| .{ .prepared = .{
                .source = self.snapshotSource(),
                .handle = snapshot.handle orelse return error.StorageKernelFailure,
            } },
            .already_imported => .already_imported,
        };
    }

    fn reconcileRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        encoded.bindCancellation();
        try client.Snapshot.reconcileRestore(encoded.request);
    }

    fn repairPublishedRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        encoded.bindCancellation();
        var lease = try self.acquire(request.group_id, request.table_name);
        defer lease.deinit();
        try lease.owner().repairRestore(&encoded.request);
    }

    fn promoteSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.promote();
    }

    fn publishPreparedSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        const durability_uncertain = try snapshot.publishPrepared();
        // The compiled storage context owns the caches used by every resident
        // table owner. The control-only caller cannot invalidate them through
        // its legacy DB-cache path, so make the physical publication boundary
        // explicit before a new owner can open the replacement generation.
        try self.context.invalidateCaches();
        return durability_uncertain;
    }

    fn commitSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.commit();
    }

    fn rollbackSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.rollback();
    }

    fn destroySnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        snapshot.deinit();
    }

    /// Apply one already-committed local Raft batch without consulting the
    /// catalog from the apply thread. The descriptor is part of the replicated
    /// envelope, so every replica opens the same generation and identity.
    pub fn applyPreparedReplicatedBatchGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        req: db_types.BatchRequest,
    ) !void {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            group_id,
        });
        defer alloc.free(path);
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor);
        defer lease.deinit();
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
    }

    /// Apply one exact committed Raft entry. The provider persists the log
    /// identity in the same physical batch as the mutation, making retries
    /// after an apply-watermark crash safe across the compiled boundary.
    pub fn applyPreparedReplicatedBatchGroupLocalAtRaftEntry(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        req: db_types.BatchRequest,
        raft_term: u64,
        raft_index: u64,
    ) !void {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            group_id,
        });
        defer alloc.free(path);
        // The Raft progress driver owns the committed entry and its retry
        // checkpoint. Yield admission conflicts to it immediately: waiting for
        // another owner lease here stalls unrelated groups and can deadlock a
        // maintenance callback waiting for this same progress driver.
        var lease = self.acquireDescriptorOnce(group_id, table_name, path, descriptor, .shared, .resident, .{}) catch |err| switch (err) {
            error.StorageKernelOwnerTransitionRequired => return error.StorageBusy,
            else => return err,
        };
        defer lease.deinit();
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var response = try lease.owner().replicatedBatchAtRaftEntryJson(
            table_name,
            request_json,
            raft_term,
            raft_index,
        );
        defer response.deinit();
    }

    pub fn waitForCurrentSyncGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        sync_level: db_types.SyncLevel,
    ) !void {
        return try self.waitForCurrentSyncGroupLocalWithCancellation(group_id, table_name, sync_level, .none);
    }

    pub fn waitForCurrentSyncGroupLocalWithCancellation(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        sync_level: db_types.SyncLevel,
        cancellation: db_types.CancellationToken,
    ) !void {
        switch (sync_level) {
            .propose, .write => return,
            .full_text, .enrichments, .full_index => {},
        }
        const owner_sync_level: abi.SyncLevel = switch (sync_level) {
            .propose => .propose,
            .write => .write,
            .full_text => .full_text,
            .enrichments => .enrichments,
            .full_index => .full_index,
        };
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().waitForSyncWithCancellation(table_name, owner_sync_level, cancellation);
    }

    pub fn applyHAReplicationRecordGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        record: ha_replication_record.RecordView,
    ) !void {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().applyHAReplicationRecord(table_name, .{
            .record_kind = @intFromEnum(record.kind),
            .payload_codec = @intFromEnum(record.payload_codec),
            .flags = record.flags,
            .cluster_id = record.cluster_id,
            .shard_id = record.shard_id,
            .table_id = record.table_id,
            .timeline_id = record.timeline_id,
            .epoch = record.epoch,
            .lsn = record.lsn,
            .previous_lsn = record.previous_lsn,
            .commit_timestamp_ns = record.commit_timestamp_ns,
            .payload = record.payload,
        });
    }

    /// Read the persisted private bootstrap without asking the public catalog
    /// to invent a route for an unpublished owner. Warm reads pin the exact
    /// current generation; cold reads stay inside the compiled storage owner.
    pub fn readHAHiddenOwnerBootstrap(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_id: u64) !?std.json.Parsed(@import("../storage/db/restore_staging_contract.zig").OwnerBootstrap) {
        const generation = self.visibleRootGeneration(group_id);
        var resident: ?Lease = blk: {
            lock(&self.mutex);
            defer self.mutex.unlock();
            for (self.entries.items) |entry| {
                if (entry.group_id != group_id or entry.generation != generation or entry.retired or entry.closing) continue;
                if (!tryReserveEntryLeaseLocked(entry, .shared)) return error.StorageReadTemporarilyUnavailable;
                break :blk .{ .source = self, .entry = entry };
            }
            break :blk null;
        };
        defer if (resident) |*lease| lease.deinit();
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(if (resident) |*lease| lease.owner().handle else null, &.{ .operation = .read_bootstrap, .context = self.context.handle, .path = .fromSlice(path), .table_id = table_id }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
        if (output.len == 0) return null;
        if (generation != self.visibleRootGeneration(group_id)) return error.RestoreStagingScopeChanged;
        return try std.json.parseFromSlice(@import("../storage/db/restore_staging_contract.zig").OwnerBootstrap, alloc, output.slice(), .{ .allocate = .alloc_always });
    }

    pub fn captureHASeedHiddenReplicaSnapshot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, scope: [32]u8, snapshot_token: []const u8, destination_root: []const u8) !void {
        var descriptor = (try self.cachedRestoreDescriptor(alloc, group_id, table_name, scope)) orelse return error.RestoreStagingScopeChanged;
        defer descriptor.deinit(alloc);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor.view());
        defer lease.deinit();
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(lease.owner().handle, &.{ .operation = .capture_snapshot, .table_name = .fromSlice(table_name), .table_id = descriptor.descriptor.identity.table_id, .scope = scope, .snapshot_token = .fromSlice(snapshot_token), .destination_root = .fromSlice(destination_root) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
    }

    pub fn captureHASeedReplicaSnapshot(self: *ProvisionedKernelOwnerSource, table_name: []const u8, group_id: u64, snapshot_token: []const u8, destination_root: []const u8) !void {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(lease.owner().handle, &.{ .operation = .capture_public_snapshot, .table_name = .fromSlice(table_name), .table_id = lease.entry.identity.table_id, .snapshot_token = .fromSlice(snapshot_token), .destination_root = .fromSlice(destination_root) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
    }

    pub fn applyHAHiddenOwnerRecord(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8, record: ha_replication_record.RecordView) !void {
        var descriptor = (try self.cachedRestoreDescriptor(alloc, group_id, table_name, scope)) orelse return error.RestoreStagingScopeChanged;
        defer descriptor.deinit(alloc);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor.view());
        defer lease.deinit();
        try lease.owner().applyHAReplicationRecord(table_name, .{
            .record_kind = @intFromEnum(record.kind),
            .payload_codec = @intFromEnum(record.payload_codec),
            .flags = record.flags,
            .cluster_id = record.cluster_id,
            .shard_id = record.shard_id,
            .table_id = record.table_id,
            .timeline_id = record.timeline_id,
            .epoch = record.epoch,
            .lsn = record.lsn,
            .previous_lsn = record.previous_lsn,
            .commit_timestamp_ns = record.commit_timestamp_ns,
            .payload = record.payload,
        });
    }

    const BackupShardWire = struct {
        group_id: u64,
        range_id: u64 = 0,
        doc_identity_shard_id: u64 = 0,
        doc_identity_range_id: u64 = 0,
        split_attempt_epoch: u64 = 0,
        start_key: []const u8,
        end_key: ?[]const u8 = null,
        snapshot_path: []const u8,
        artifact_size_bytes: u64 = 0,
        artifact_sha256: []const u8 = "",
        native_manifest_size_bytes: u64 = 0,
        native_manifest_sha256: []const u8 = "",
    };

    fn backupTableGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        plan: backup_contract.TableBackupPlan,
    ) !?[]backup_contract.ShardSnapshot {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try plan.ensureActive();
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        const cohort_json = if (plan.relational_cohort_fence) |fence| try std.json.Stringify.valueAlloc(alloc, fence, .{}) else null;
        defer if (cohort_json) |json| alloc.free(json);
        const handle = try backup_contract.sealedHandleForGroup(plan.sealed_handles, group_id);
        const handle_json = if (handle) |sealed| try std.json.Stringify.valueAlloc(alloc, sealed.handle, .{}) else null;
        defer if (handle_json) |json| alloc.free(json);
        var cancellation = plan.cancellation;
        var response = try lease.owner().backupWithControl(.{
            .table_name = .fromSlice(table_name),
            .backup_root = .fromSlice(plan.backup_root),
            .backup_id = .fromSlice(plan.backup_id),
            .format = @intFromEnum(switch (plan.format) {
                .native => abi.BackupFormat.native,
                .portable => abi.BackupFormat.portable,
            }),
            .cohort_json = .fromSlice(cohort_json orelse ""),
            .sealed_handle_json = .fromSlice(handle_json orelse ""),
            .execution_deadline_ns = plan.deadline_ns orelse 0,
            .has_execution_deadline = @intFromBool(plan.deadline_ns != null),
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        });
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(
            []BackupShardWire,
            alloc,
            response.bytes(),
            .{},
        );
        defer parsed.deinit();
        if (parsed.value.len != 1 or parsed.value[0].group_id != group_id)
            return error.StorageKernelFailure;
        const shards = try alloc.alloc(backup_contract.ShardSnapshot, parsed.value.len);
        var initialized: usize = 0;
        errdefer {
            for (shards[0..initialized]) |shard| shard.deinit(alloc);
            alloc.free(shards);
        }
        for (parsed.value, 0..) |shard, i| {
            const start_key = try alloc.dupe(u8, shard.start_key);
            errdefer alloc.free(start_key);
            const end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null;
            errdefer if (end_key) |value| alloc.free(value);
            const snapshot_path = try alloc.dupe(u8, shard.snapshot_path);
            errdefer alloc.free(snapshot_path);
            const artifact_sha256 = if (shard.artifact_sha256.len > 0)
                try alloc.dupe(u8, shard.artifact_sha256)
            else
                "";
            errdefer if (artifact_sha256.len > 0) alloc.free(@constCast(artifact_sha256));
            const native_manifest_sha256 = if (shard.native_manifest_sha256.len > 0)
                try alloc.dupe(u8, shard.native_manifest_sha256)
            else
                "";
            errdefer if (native_manifest_sha256.len > 0) alloc.free(@constCast(native_manifest_sha256));
            shards[i] = .{
                .group_id = shard.group_id,
                .range_id = shard.range_id,
                .doc_identity_shard_id = shard.doc_identity_shard_id,
                .doc_identity_range_id = shard.doc_identity_range_id,
                .split_attempt_epoch = shard.split_attempt_epoch,
                .start_key = start_key,
                .end_key = end_key,
                .snapshot_path = snapshot_path,
                .artifact_size_bytes = shard.artifact_size_bytes,
                .artifact_sha256 = artifact_sha256,
                .native_manifest_size_bytes = shard.native_manifest_size_bytes,
                .native_manifest_sha256 = native_manifest_sha256,
            };
            initialized += 1;
        }
        return shards;
    }

    fn backupPinControl(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, input: @import("../storage/db/native_backup_seal_contract.zig").Request, control: backup_contract.BackupOperationControl) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try control.ensureActive();
        const json = try std.json.Stringify.valueAlloc(alloc, input, .{});
        defer alloc.free(json);
        var cancellation = control.cancellation;
        const native_control: abi.ControlledJsonOperationRequest = .{
            .table_name = .fromSlice(table_name),
            .request_json = .fromSlice(json),
            .execution_deadline_ns = control.deadline_ns,
            .has_execution_deadline = 1,
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        };
        if (input != .seal) {
            try self.ensureContextConfigured();
            var response = try self.context.reclaimBackupPinJson(.{ .control = native_control, .replica_root = .fromSlice(self.replica_root_dir), .group_id = group_id });
            defer response.deinit();
            return try alloc.dupe(u8, response.bytes());
        }
        var lease = try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = control.deadline_ns, .cancellation = control.cancellation });
        defer lease.deinit();
        var response = try lease.owner().backupPinControlJson(native_control);
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    /// Private lifecycle bridge: materialize from the admitted durable pin,
    /// then return its certificate for a separate replicated publication CAS.
    pub fn prepareOnlineSourcePublication(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: @import("../storage/db/online_source_contract.zig").Scope, context: @import("operation.zig").RequestContext) !@import("../storage/source_snapshot.zig").Certificate {
        try context.ensureActive();
        try scope.validate();
        if (scope.fence.owner_group_id != group_id) return error.OnlineSourceScopeChanged;
        const json = try std.json.Stringify.valueAlloc(alloc, scope, .{});
        defer alloc.free(json);
        const native_context = try platformDeadlineContext(context);
        var cancellation = context.cancellation;
        var lease = try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation = cancellation });
        defer lease.deinit();
        try context.ensureActive();
        var response = try lease.owner().prepareSourcePinPublicationJson(.{ .table_name = .fromSlice(table_name), .request_json = .fromSlice(json), .execution_deadline_ns = native_context.deadline_ns orelse 0, .has_execution_deadline = @intFromBool(native_context.deadline_ns != null), .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(@import("../storage/source_snapshot.zig").Certificate, alloc, response.bytes(), .{});
        defer parsed.deinit();
        _ = try parsed.value.encode();
        if (!parsed.value.cut.namespace.eql(scope.fence.namespace)) return error.SourceSnapshotCutMismatch;
        try context.ensureActive();
        return parsed.value;
    }

    pub fn onlineSourceArtifact(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request: @import("../storage/db/source_artifact_transfer.zig").Request, context: @import("operation.zig").RequestContext) ![]u8 {
        try context.ensureActive();
        try request.scope().validate();
        if (request.scope().fence.owner_group_id != group_id) return error.OnlineSourceScopeChanged;
        const json = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(json);
        if (json.len > 2 * 1024 * 1024) return error.InvalidSourceSnapshot;
        const native_context = try platformDeadlineContext(context);
        var cancellation = context.cancellation;
        var lease = try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation = cancellation });
        defer lease.deinit();
        try context.ensureActive();
        var response = try lease.owner().sourceArtifactJson(.{ .table_name = .fromSlice(table_name), .request_json = .fromSlice(json), .execution_deadline_ns = native_context.deadline_ns orelse 0, .has_execution_deadline = @intFromBool(native_context.deadline_ns != null), .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        try context.ensureActive();
        return alloc.dupe(u8, response.bytes());
    }

    pub fn onlineMergeIo(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request: @import("../storage/db/online_merge_io_contract.zig").Request, context: @import("operation.zig").RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate();
        if (request.ownerGroup() != group_id) return error.OnlineSourceScopeChanged;
        // An immutable artifact may be served by another donor replica. Its
        // inner published-ledger scope/certificate is the authority, not a
        // new current read cut. All row/status operations require read-index.
        if (request.operation != .artifact) try feature_reads.FeatureReads.init(self.read_safety_barrier).prepareLookupWithConsistency(group_id, "", .{
            .execution_deadline_ns = context.deadline_ns,
            .execution_io = context.deadline_io,
            .cancellation = context.cancellation,
        }, .read_index);
        const json = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(json);
        if (json.len > @import("online_merge_io.zig").contract.max_request_bytes) return error.InvalidMergePage;
        const native_context = try platformDeadlineContext(context);
        var cancellation = context.cancellation;
        var lease = try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation = cancellation });
        defer lease.deinit();
        try context.ensureActive();
        var response = try lease.owner().onlineMergeIoJson(.{ .table_name = .fromSlice(table_name), .request_json = .fromSlice(json), .execution_deadline_ns = native_context.deadline_ns orelse 0, .has_execution_deadline = @intFromBool(native_context.deadline_ns != null), .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        try context.ensureActive();
        return alloc.dupe(u8, response.bytes());
    }

    pub fn ownerCountForTest(self: *ProvisionedKernelOwnerSource) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.entries.items.len;
    }

    /// Pre-open the same resident owner used by reads and writes. Warmup must
    /// not create a second status-only DB in the distributed compilation unit;
    /// acquiring and releasing the owner performs descriptor validation
    /// without opening a second DB in distributed code.
    pub fn warmTableGroup(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, descriptor.path, descriptor.view(), false, .transient, .{});
        defer lease.deinit();
        // Warmup can overlap startup catch-up or foreground admission. Retire
        // only a transient owner, while the lease still pins it. Borrowing
        // observers drain before close; foreground adoption retains it.
        // Never retire by group after
        // releasing the lease: that can close another operation's owner.
        lease.requestTransientRetirement();
    }

    /// Apply the latest catalog schema/index contract to the already-resident
    /// physical group owner, or open that owner with the contract when absent.
    /// Routing and catalog selection remain in distributed control.
    pub fn reconcileTableGroup(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !abi.ReconcileResult {
        return try self.reconcileTableGroupStep(group_id, table_name, null, false);
    }

    /// Advance at most one durable index-repair intent in addition to the
    /// desired-state pass. Node scheduling decides when to request that work.
    pub fn reconcileTableGroupStep(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !abi.ReconcileResult {
        return try self.reconcileTableGroupStepWithRetention(
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
            true,
        );
    }

    fn reconcileTableGroupStepWithRetention(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
        retain_cold_owner: bool,
    ) !abi.ReconcileResult {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        var lease = (try self.acquireDescriptorForReconcile(
            group_id,
            table_name,
            descriptor.path,
            descriptor.view(),
            retain_cold_owner or advance_index_repair,
            if (retain_cold_owner) .resident else .transient,
        )) orelse return .{ .state = .busy };
        defer lease.deinit();
        errdefer lease.requestTransientRetirement();
        const result = lease.owner().reconcile(
            table_name,
            descriptor.schema_json,
            descriptor.indexes_json,
            target_index_name,
            advance_index_repair,
        ) catch |err| {
            lease.retireAfterConfigurationFailure();
            return err;
        };
        if (!retain_cold_owner) lease.requestTransientRetirement();
        return result;
    }

    fn reconcileTableGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?table_write_source.LocalStructuralReconcileResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const result = try self.reconcileTableGroupStep(
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
        );
        return localStructuralReconcileResult(result);
    }

    fn localStructuralReconcileResult(
        result: abi.ReconcileResult,
    ) table_write_source.LocalStructuralReconcileResult {
        return .{
            .state = switch (result.state) {
                .complete => .complete,
                .repair_pending => .repair_pending,
                .busy => .busy,
                .degraded => .degraded,
                .restore_repair_pending => .restore_repair_pending,
            },
            .indexes_added = result.indexes_added,
            .indexes_removed = result.indexes_removed,
            .indexes_pending = result.indexes_pending,
            .repair_discovered = result.repair_discovered,
            .repair_attempted = result.repair_attempted,
            .repair_repaired = result.repair_repaired,
            .repair_remaining = result.repair_remaining,
            .repair_terminal = result.repair_terminal,
            .repair_paused = result.repair_paused,
            .repair_busy = result.repair_busy,
            .repair_disk_waits = result.repair_disk_waits,
            .next_retry_at_ms = result.next_retry_at_ms,
            .restore_repair_attempted = result.restore_repair_attempted,
            .restore_repair_progressed = result.restore_repair_progressed,
            .restore_repair_pending = result.restore_repair_pending,
        };
    }

    fn reconcileTableGroupLocalTransient(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?table_write_source.LocalStructuralReconcileResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const result = try self.reconcileTableGroupStepWithRetention(
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
            false,
        );
        return localStructuralReconcileResult(result);
    }

    const RepairControlsBridge = struct {
        options: db_types.ArtifactRepairRunOptions,
        deadline_ns: u64 = 0,

        fn cancelled(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(self.options.cancelled());
        }
        fn yieldRequested(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(if (self.options.yield_check) |check| check.requested() else platform_time.monotonicNs() >= self.deadline_ns);
        }
        fn activationAllowed(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(if (self.options.activation_check) |check| check.current() catch false else true);
        }
        fn wire(self: *@This()) abi.RepairControls {
            self.deadline_ns = platform_time.monotonicNs() +| 50 * std.time.ns_per_ms;
            return .{
                .context = self,
                .cancelled = cancelled,
                .yield_requested = yieldRequested,
                .activation_allowed = activationAllowed,
                .owner_epoch = self.options.owner_epoch,
                .capacity_domain_lo = @truncate(self.options.capacity_domain_id),
                .capacity_domain_hi = @truncate(self.options.capacity_domain_id >> 64),
                .estimated_candidate_bytes = self.options.estimated_candidate_bytes,
                .max_activation_gap_sequences = self.options.max_activation_gap_sequences,
                .max_convergence_rounds = self.options.max_convergence_rounds,
                .max_activation_pause_ms = self.options.max_activation_pause_ms,
            };
        }
    };

    fn reconcileTableGroupLocalObserved(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
        repair_options: db_types.ArtifactRepairRunOptions,
        retain_cold_owner: bool,
    ) !?table_write_source.LocalStructuralReconcileObservation {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (repair_options.cancelled()) return error.Canceled;
        if (repair_options.yield_check) |check| if (check.requested()) return .{ .result = .{ .state = .busy } };
        var descriptor = try self.loadDescriptorWithDeadline(self.alloc, group_id, table_name, repair_options.admission_deadline_ns);
        defer descriptor.deinit(self.alloc);
        if (repair_options.cancelled()) return error.Canceled;
        if (repair_options.yield_check) |check| if (check.requested()) return .{ .result = .{ .state = .busy } };
        const configured = if (advance_index_repair)
            self.tryAcquireConfiguredRepair(group_id, table_name, descriptor.view(), target_index_name)
        else
            null;
        var lease = if (configured) |ready| ready.lease else (try self.acquireDescriptorForReconcile(
            group_id,
            table_name,
            descriptor.path,
            descriptor.view(),
            // Only an explicit structural caller may install writer preference.
            // A scheduled repair returns busy immediately behind live leases.
            retain_cold_owner,
            if (retain_cold_owner) .resident else .transient,
        )) orelse return .{ .result = .{ .state = .busy } };
        defer lease.deinit();
        errdefer lease.requestTransientRetirement();

        var result = if (configured) |ready| ready.result else lease.owner().reconcile(
            table_name,
            descriptor.schema_json,
            descriptor.indexes_json,
            target_index_name,
            false,
        ) catch |err| {
            lease.retireAfterConfigurationFailure();
            return err;
        };
        if (advance_index_repair and result.restore_repair_pending == 0) {
            if (configured == null) {
                const owned_target = if (target_index_name) |target| try self.alloc.dupe(u8, target) else null;
                lock(&self.mutex);
                if (lease.entry.repair_target) |old| self.alloc.free(old);
                lease.entry.repair_target = owned_target;
                lease.entry.repair_configuration = result;
                self.mutex.unlock();
            }
            if (lease.exclusive) lease.downgrade();
            var controls = RepairControlsBridge{ .options = repair_options };
            const repair = try lease.owner().repairIndex(table_name, target_index_name, controls.wire());
            const added = result.indexes_added;
            const removed = result.indexes_removed;
            const pending = result.indexes_pending;
            result = repair;
            result.indexes_added = added;
            result.indexes_removed = removed;
            result.indexes_pending = pending;
            if (repair.state == .complete and pending != 0) {
                // Admission may have left cleanup/activation debt. Verify it
                // under a new nonblocking structural lease on the next pass.
                lock(&self.mutex);
                lease.entry.repair_configuration = null;
                self.mutex.unlock();
                result.state = .busy;
            }
        }
        var response = lease.owner().runtimeStatusJson(table_name) catch |err| switch (err) {
            // Runtime status is deliberately best effort and returns busy
            // rather than waiting behind a concurrent Raft apply writer. The
            // structural reconcile above is already authoritative, so retain
            // its result and let the periodic status refresher observe the
            // owner after apply releases its writer guard.
            error.StorageBusy => null,
            else => return err,
        };
        defer if (response) |*value| value.deinit();
        var observed: ?runtime_status.LocalTableRuntimeStatus = if (response) |*value| observed: {
            var parsed = try std.json.parseFromSlice(
                runtime_status.LocalTableRuntimeStatus,
                alloc,
                value.bytes(),
                .{},
            );
            defer parsed.deinit();
            var status = try parsed.value.clone(alloc);
            status.group_id = group_id;
            // Retain the provider's source-target proof; replacing metadata
            // with defaults would erase the sequence sampled with these stats.
            status.metadata.updated_at_ns = platform_time.monotonicNs();
            status.metadata.source = .live_writer_publish;
            status.metadata.freshness = .fresh;
            status.metadata.lsm_root_generation = lease.entry.generation;
            break :observed status;
        } else null;
        errdefer if (observed) |*status| status.deinit(alloc);
        const retain_for_background_work = if (observed) |status|
            runtimeStatusNeedsResidentOwner(status)
        else
            // The status probe lost a best-effort race with Raft apply. Keep
            // this otherwise-cold owner resident so the periodic refresher can
            // publish the exact generation proof once the writer guard drains.
            true;

        // A transient startup inspection normally gives the cold owner back
        // immediately. Managed enrichment and index catch-up are different:
        // their retry scheduler lives inside that owner, so retiring it here
        // strands durable work until an unrelated foreground request happens
        // to reopen the group. Keep only owners with observed background debt;
        // idle groups preserve the bounded transient-open contract.
        if (retain_for_background_work) lease.retain() else lease.requestTransientRetirement();
        return .{
            .result = localStructuralReconcileResult(result),
            .runtime_status = observed,
        };
    }

    const ConfiguredRepair = struct { lease: Lease, result: abi.ReconcileResult };

    fn tryAcquireConfiguredRepair(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor, target: ?[]const u8) ?ConfiguredRepair {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.closing or entry.retired or entry.generation != descriptor.lsm_root_generation or
                !entry.identity.eql(descriptor.identity) or !std.mem.eql(u8, entry.schema_json, descriptor.schema_json) or
                !std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) or !std.meta.eql(entry.table_storage, descriptor.table_storage)) return null;
            const result = entry.repair_configuration orelse return null;
            if ((target == null) != (entry.repair_target == null)) return null;
            if (target) |name| if (!std.mem.eql(u8, entry.repair_target.?, name)) return null;
            if (!tryReserveEntryLeaseLocked(entry, .shared)) return null;
            return .{ .lease = .{ .source = self, .entry = entry }, .result = result };
        }
        return null;
    }

    fn runtimeStatusNeedsResidentOwner(status: runtime_status.LocalTableRuntimeStatus) bool {
        const enrichment = status.stats.enrichment;
        if (enrichment.retrying or
            enrichment.target_sequence > enrichment.applied_sequence or
            enrichment.active_embed_batch_items != 0)
        {
            return true;
        }
        if (status.stats.async_indexing.startup.active or
            status.stats.async_indexing.dense_catch_up.active or
            status.stats.async_indexing.bulk_coalescing.active_session)
        {
            return true;
        }
        for (status.stats.indexes) |index| {
            if (index.backfill_active or
                index.catch_up_active or
                index.replay_catch_up_required or
                index.replay_target_sequence > index.replay_applied_sequence)
            {
                return true;
            }
        }
        return false;
    }

    fn preflightWriteAdmissionGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().preflightWriteAdmission(table_name);
        return {};
    }

    fn findMedianKeyGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = (try lease.owner().findMedianKey(table_name)) orelse return null;
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn visibleRootGeneration(self: *const ProvisionedKernelOwnerSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |source|
            source.visibleRootGenerationForGroup(group_id)
        else
            table_reads.backend_current_root_generation;
    }

    fn lock(mutex: *std.atomic.Mutex) void {
        platform_sync.lockYielding(mutex);
    }

    fn destroyEntryAtIndexLocked(self: *ProvisionedKernelOwnerSource, index: usize) void {
        const entry = self.entries.items[index];
        std.debug.assert(entry.active_users == 0 and !entry.exclusive_active and !entry.closing);
        entry.retired = true;
        entry.closing = true;
        // Keep the closing owner registered until all storage work has drained.
        // A concurrent open or cleanup must not mistake a removed pointer for
        // permission to reopen, move, or delete the same physical root.
        const quiescing = self.quiescing;
        self.mutex.unlock();
        if (quiescing) entry.owner.quiesce() catch @panic("storage owner quiesce boundary failed");
        entry.owner.deinit();
        lock(&self.mutex);
        for (self.entries.items, 0..) |candidate, current_index| {
            if (candidate == entry) {
                _ = self.entries.orderedRemove(current_index);
                break;
            }
        } else unreachable;
        self.alloc.free(entry.table_name);
        self.alloc.free(entry.schema_json);
        self.alloc.free(entry.indexes_json);
        self.alloc.free(entry.restore_bootstrap_json);
        descriptor_contract.freeInitialRange(self.alloc, entry.initial_range);
        if (entry.restore) |*identity| identity.deinit(self.alloc);
        if (entry.repair_target) |target| self.alloc.free(target);
        self.alloc.destroy(entry);
    }

    fn release(self: *ProvisionedKernelOwnerSource, entry: *Entry, exclusive: bool) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(entry.active_users > 0);
        if (exclusive) {
            std.debug.assert(entry.exclusive_active);
            std.debug.assert(entry.active_users == 1);
            entry.exclusive_active = false;
        }
        entry.active_users -= 1;
        if (entry.active_users == 0 and entry.transient_retirement_pending and !entry.resident)
            entry.retired = true;
        if (!entry.retired or entry.active_users != 0 or self.installationPinsOwnerLocked(entry.group_id)) return;
        for (self.entries.items, 0..) |candidate, index| {
            if (candidate != entry) continue;
            self.destroyEntryAtIndexLocked(index);
            return;
        }
        unreachable;
    }

    fn snapshotOwnerLeases(
        self: *ProvisionedKernelOwnerSource,
        best_effort: bool,
        skip_bulk_ingest: bool,
    ) !?[]Lease {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();

        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.retired or entry.transient_retirement_pending or entry.exclusive_pending or entry.exclusive_active or (skip_bulk_ingest and entry.bulk_ingest_active.load(.acquire))) continue;
            count += 1;
        }
        const leases = try self.alloc.alloc(Lease, count);
        var initialized: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.retired or entry.transient_retirement_pending or entry.exclusive_pending or entry.exclusive_active or (skip_bulk_ingest and entry.bulk_ingest_active.load(.acquire))) continue;
            entry.active_users += 1;
            leases[initialized] = .{ .source = self, .entry = entry };
            initialized += 1;
        }
        std.debug.assert(initialized == count);
        return leases;
    }

    fn releaseMaintenanceLeases(self: *ProvisionedKernelOwnerSource, leases: []Lease) void {
        for (leases) |*lease| lease.deinit();
        self.alloc.free(leases);
    }

    fn maintenanceEntryLimit(self: *ProvisionedKernelOwnerSource, best_effort: bool) ?usize {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();
        return self.entries.items.len;
    }

    /// Maintenance must never pin unrelated owners across a slow storage step.
    /// The cursor may skip an entry removed during the round; the next round
    /// will see it if it is still resident. Newly appended entries wait too.
    fn nextMaintenanceLease(
        self: *ProvisionedKernelOwnerSource,
        cursor: *usize,
        limit: usize,
        best_effort: bool,
        skip_bulk_ingest: bool,
    ) ?Lease {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();
        while (cursor.* < limit and cursor.* < self.entries.items.len) {
            const entry = self.entries.items[cursor.*];
            cursor.* += 1;
            if (entry.retired or entry.transient_retirement_pending or entry.exclusive_pending or
                entry.exclusive_active or (skip_bulk_ingest and entry.bulk_ingest_active.load(.acquire))) continue;
            entry.active_users += 1;
            return .{ .source = self, .entry = entry };
        }
        return null;
    }

    fn selectedMaintenanceLease(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        generation: u64,
        best_effort: bool,
    ) ?Lease {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or entry.generation != generation or
                !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.retired or entry.transient_retirement_pending or entry.exclusive_pending or
                entry.exclusive_active or entry.bulk_ingest_active.load(.acquire)) return null;
            entry.active_users += 1;
            return .{ .source = self, .entry = entry };
        }
        return null;
    }

    fn runLsmMaintenanceRound(
        ptr: *anyopaque,
        best_effort: bool,
    ) !storage_maintenance_source.RoundResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(best_effort) orelse return .{};
        var cursor: usize = 0;
        var selected_name: ?[]u8 = null;
        defer if (selected_name) |name| self.alloc.free(name);
        var selected_group_id: u64 = 0;
        var selected_generation: u64 = 0;
        var selected_score: u64 = 0;
        var selected_due = false;
        while (self.nextMaintenanceLease(&cursor, limit, best_effort, true)) |borrowed| {
            var lease = borrowed;
            const status = lease.owner().maintenance(
                lease.entry.table_name,
                if (best_effort) .inspect_best_effort else .inspect,
            ) catch |err| {
                lease.deinit();
                if (best_effort) continue;
                return err;
            };
            const due = status.has_next_wake_delay != 0 and status.next_wake_delay_ns == 0;
            if (due or status.maintenance_score != 0) {
                if (selected_name == null or
                    (due and !selected_due) or
                    (due == selected_due and status.maintenance_score > selected_score))
                {
                    const name = self.alloc.dupe(u8, lease.entry.table_name) catch |err| {
                        lease.deinit();
                        return err;
                    };
                    if (selected_name) |previous| self.alloc.free(previous);
                    selected_name = name;
                    selected_group_id = lease.entry.group_id;
                    selected_generation = lease.entry.generation;
                    selected_score = status.maintenance_score;
                    selected_due = due;
                }
            }
            lease.deinit();
        }
        const name = selected_name orelse return .{};
        var selected = self.selectedMaintenanceLease(selected_group_id, name, selected_generation, best_effort) orelse return .{};
        defer selected.deinit();
        const lease = &selected;
        const result = try lease.owner().maintenance(
            lease.entry.table_name,
            if (best_effort) .lsm_step_best_effort else .lsm_step,
        );
        return .{
            .progressed = result.progressed != 0,
            .group_id = lease.entry.group_id,
        };
    }

    fn runDensePostingMaintenanceRound(ptr: *anyopaque) !@import("storage_maintenance_source.zig").PostingRefreshProgress {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return .{ .pending = true };
        var cursor: usize = 0;

        var total: @import("storage_maintenance_source.zig").PostingRefreshProgress = .{};
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            const result = lease.owner().maintenance(
                lease.entry.table_name,
                .dense_posting_idle,
            ) catch |err| {
                std.log.warn("storage owner dense posting maintenance failed table={s} group_id={d} err={s}", .{
                    lease.entry.table_name,
                    lease.entry.group_id,
                    @errorName(err),
                });
                total.pending = true;
                continue;
            };
            total.repaired +|= @intCast(result.dense_steps);
            total.scanned +|= @intCast(result.dense_scanned);
            total.pending = total.pending or result.deferred != 0 or result.busy != 0;
        }
        if (cursor < limit) total.pending = true;
        return total;
    }

    fn targetAdvanced(
        ptr: ?*anyopaque,
        table_name: abi.BorrowedBytes,
        group_id: u64,
        sequence: u64,
        has_sequence: u8,
        identities_json: abi.BorrowedBytes,
    ) callconv(.c) void {
        const cache: *runtime_status.TableRuntimeSnapshotCache = @ptrCast(@alignCast(ptr orelse return));
        const target_sequence: ?u64 = if (has_sequence != 0) sequence else null;
        if (identities_json.len == 0) {
            cache.markGroupTargetObservationPending(table_name.slice(), group_id, target_sequence);
            return;
        }
        var identities = std.json.parseFromSlice(
            []db_types.IndexTargetVisibility,
            cache.alloc,
            identities_json.slice(),
            .{ .ignore_unknown_fields = true },
        ) catch {
            cache.markGroupTargetObservationPending(table_name.slice(), group_id, target_sequence);
            return;
        };
        defer identities.deinit();
        cache.markIndexTargetsObservationPending(table_name.slice(), group_id, identities.value, sequence);
    }

    pub fn withRuntimeStatusCache(self: *ProvisionedKernelOwnerSource, cache: *runtime_status.TableRuntimeSnapshotCache) *ProvisionedKernelOwnerSource {
        self.runtime_status_cache = cache;
        return self;
    }

    fn publishRuntimeStatuses(ptr: *anyopaque) void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (self.runtime_status_cache == null) return;
        const limit = self.maintenanceEntryLimit(true) orelse return;
        var cursor: usize = 0;
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            self.refreshMaintenanceStatus(&lease);
            lease.deinit();
        }
    }

    fn refreshMaintenanceStatus(self: *ProvisionedKernelOwnerSource, lease: *Lease) void {
        const cache = self.runtime_status_cache orelse return;
        // Capture the table fence before observing the pinned generation.
        const token = cache.capturePublicationToken(lease.entry.table_name) catch return;
        var response = lease.owner().runtimeStatusJson(lease.entry.table_name) catch return;
        defer response.deinit();
        var parsed = std.json.parseFromSlice(runtime_status.LocalTableRuntimeStatus, self.alloc, response.bytes(), .{}) catch return;
        defer parsed.deinit();
        _ = cache.publishGroups(token, lease.entry.table_name, &.{parsed.value}) catch return;
    }

    fn publishDenseCheckpoints(ptr: *anyopaque) !db_types.NativePublicationResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return .{ .busy = true };
        var cursor: usize = 0;
        var combined: db_types.NativePublicationResult = .{};
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            const result = try lease.owner().maintenance(lease.entry.table_name, .publish_dense_checkpoints);
            if (result.published != 0) self.refreshMaintenanceStatus(&lease);
            combined.published += @intCast(result.published);
            combined.busy = combined.busy or result.busy != 0;
            combined.deferred = combined.deferred or result.deferred != 0;
        }
        if (cursor < limit) combined.busy = true;
        return combined;
    }

    fn runVectorBlockRound(ptr: *anyopaque) !usize {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return 0;
        var cursor: usize = 0;
        var steps: usize = 0;
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            self.refreshMaintenanceStatus(&lease);
            defer self.refreshMaintenanceStatus(&lease);
            const result = try lease.owner().maintenance(lease.entry.table_name, .vector_block_idle);
            steps += @intCast(result.dense_steps);
        }
        return steps;
    }

    fn maintenanceSnapshot(
        ptr: *anyopaque,
        best_effort: bool,
    ) !storage_maintenance_source.Snapshot {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(best_effort) orelse return .{};
        var cursor: usize = 0;
        var result = storage_maintenance_source.Snapshot{};
        while (self.nextMaintenanceLease(&cursor, limit, best_effort, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            result.owner_count += 1;
            const status = lease.owner().maintenance(
                lease.entry.table_name,
                if (best_effort) .inspect_best_effort else .inspect,
            ) catch continue;
            result.maintenance_score = @max(result.maintenance_score, status.maintenance_score);
            if (status.has_next_wake_delay != 0) {
                result.next_wake_delay_ns = if (result.next_wake_delay_ns) |current|
                    @min(current, status.next_wake_delay_ns)
                else
                    status.next_wake_delay_ns;
            }
        }
        return result;
    }

    pub fn loadDescriptor(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !LoadedDescriptor {
        return self.loadDescriptorWithDeadline(alloc, group_id, table_name, null);
    }

    fn loadDescriptorWithDeadline(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: ?u64,
    ) !LoadedDescriptor {
        var projection = (try table_catalog.tableGroupDescriptorProjection(
            alloc,
            self.catalog,
            table_name,
            group_id,
            deadline_ns,
        )) orelse return error.TableNotFound;
        errdefer projection.deinit(alloc);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        return .{
            .path = path,
            .schema_json = projection.schema_json,
            .indexes_json = projection.indexes_json,
            .table_storage = projection.table_storage,
            .initial_range = projection.initial_range,
            .restore = projection.restore,
            .generation = self.visibleRootGeneration(group_id),
            .identity = .{
                .table_id = projection.table_id,
                .shard_id = projection.doc_identity_shard_id,
                .range_id = projection.doc_identity_range_id,
            },
        };
    }

    const ReadControls = struct {
        execution_deadline_ns: ?u64 = null,
        execution_io: ?@import("../runtime_io_abi.zig").Borrow = null,
        cancellation: ?db_types.CancellationToken = null,

        fn from(req: anytype) ReadControls {
            return .{ .execution_deadline_ns = req.execution_deadline_ns, .execution_io = if (@hasField(@TypeOf(req), "execution_io")) req.execution_io else null, .cancellation = req.cancellation };
        }

        fn check(self: ReadControls) !void {
            const context: request_operation.RequestContext = .{
                .deadline_ns = self.execution_deadline_ns,
                .deadline_io = self.execution_io,
                .cancellation = self.cancellation orelse .none,
            };
            context.ensureActive() catch |err| return switch (err) {
                error.Canceled => error.Cancelled,
                error.DeadlineExceeded => error.Timeout,
                else => err,
            };
        }
    };

    fn acquire(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !Lease {
        return self.acquireWithControls(group_id, table_name, .{});
    }

    fn acquireWithControls(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        controls: ReadControls,
    ) !Lease {
        try controls.check();
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        return self.acquireDescriptorWithMode(group_id, table_name, descriptor.path, descriptor.view(), false, .resident, controls);
    }

    fn acquireTransactionOwner(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        context: request_operation.RequestContext,
    ) !Lease {
        const controls: ReadControls = .{ .execution_deadline_ns = context.deadline_ns, .execution_io = context.deadline_io, .cancellation = context.cancellation };
        if (req.restore_staging_scope) |scope| {
            var descriptor = try self.resolveRestoreDescriptor(alloc, group_id, table_name, scope, req.restore_staging_plan_id, restoreDescriptorUseForBatch(req), context);
            defer descriptor.deinit(alloc);
            const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
            defer alloc.free(path);
            return self.acquireDescriptorWithMode(group_id, table_name, path, descriptor.view(), false, .resident, controls);
        }
        if (req.restore_staging_plan_id != null) return error.RestoreStagingScopeChanged;
        return self.acquireWithControls(group_id, table_name, controls);
    }

    fn acquireHiddenTransactionOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !?Lease {
        const generation = self.visibleRootGeneration(group_id);
        lock(&self.mutex);
        defer self.mutex.unlock();
        var found: ?*Entry = null;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or
                entry.retired or entry.closing or entry.generation != generation or entry.restore_bootstrap_json.len == 0) continue;
            if (found != null) return error.CommitDecisionUnknown;
            found = entry;
        }
        return if (found) |entry| try self.borrowEntryLocked(entry) else null;
    }

    fn transactionRecoveryStatus(err: anyerror) abi.Status {
        return kernel_error_identity.statusFromError(err);
    }

    fn checkRecoveryBudget(deadline_ns: u64, cancellation: db_types.CancellationToken) !void {
        try cancellation.check();
        if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
    }

    fn acquireForRecovery(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: u64,
        cancellation: db_types.CancellationToken,
    ) !Lease {
        try checkRecoveryBudget(deadline_ns, cancellation);
        const catalog_deadline = self.catalog.budget(null).deadlineFrom(table_catalog.RoutingBudget.init(deadline_ns));
        var descriptor = self.loadDescriptorWithDeadline(self.alloc, group_id, table_name, catalog_deadline) catch |err| {
            try checkRecoveryBudget(deadline_ns, cancellation);
            return if (err == error.CatalogRoutingUnavailable) error.CommitPropagationIncomplete else err;
        };
        defer descriptor.deinit(self.alloc);
        try checkRecoveryBudget(deadline_ns, cancellation);
        // Opening, closing, or reconfiguring a native owner cannot be made
        // interruptible by checking a clock afterwards. Hand that work to the
        // lifecycle lane; this request only borrows an exact resident match.
        return self.borrowRecoveryOwner(group_id, table_name, descriptor.view()) catch |err| {
            try checkRecoveryBudget(deadline_ns, cancellation);
            if (err == error.CommitPropagationIncomplete) try self.requestRecoveryOwner(group_id, table_name);
            return err;
        };
    }

    fn borrowRecoveryOwner(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
    ) !Lease {
        if (!self.mutex.tryLock()) return error.CommitPropagationIncomplete;
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        if (self.completion_installations.get(group_id)) |record| {
            if (record.state != .backed or !record.active) return error.CompletionAdmissionUnavailable;
        }
        if (self.publicationPendingLocked(group_id, table_name)) return error.CommitPropagationIncomplete;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.resident or entry.retired or entry.closing or
                entry.generation != descriptor.lsm_root_generation or
                !entry.identity.eql(descriptor.identity) or
                !std.mem.eql(u8, entry.schema_json, descriptor.schema_json) or
                !std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) or
                !std.meta.eql(entry.table_storage, descriptor.table_storage))
                return error.CommitPropagationIncomplete;
            return self.borrowEntryLocked(entry) catch return error.CommitPropagationIncomplete;
        }
        return error.CommitPropagationIncomplete;
    }

    fn requestRecoveryOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !void {
        if (self.recovery_owner_closing.load(.acquire)) return error.Canceled;
        if (table_name.len == 0 or table_name.len > 1024) return error.CommitPropagationIncomplete;
        if (!self.recovery_owner_mutex.tryLock()) return error.CommitPropagationIncomplete;
        defer self.recovery_owner_mutex.unlock();
        if (self.recovery_owner_closing.load(.acquire)) return error.Canceled;
        var free_slot: ?*RecoveryOwnerRequest = null;
        for (&self.recovery_owner_requests) |*request| {
            if (request.table_name_len == 0) {
                if (free_slot == null) free_slot = request;
            } else if (request.group_id == group_id and std.mem.eql(u8, request.name(), table_name)) {
                // A request arriving during open must survive that attempt's
                // completion; its descriptor may name a newer generation.
                request.revision = std.math.add(u64, request.revision, 1) catch return error.CommitPropagationIncomplete;
                return;
            }
        }
        const slot = free_slot orelse return error.CommitPropagationIncomplete;
        slot.group_id = group_id;
        slot.table_name_len = table_name.len;
        @memcpy(slot.table_name[0..table_name.len], table_name);
        slot.revision = 1;
        _ = self.recovery_owner_pending.fetchAdd(1, .release);
    }

    pub fn hasPendingRecoveryOwners(self: *const ProvisionedKernelOwnerSource) bool {
        return !self.recovery_owner_closing.load(.acquire) and self.recovery_owner_pending.load(.acquire) != 0;
    }

    /// Called by DATA's existing shutdown-owned lifecycle job, never by the
    /// deadline-bound request. Each pass attempts one slot and rotates before
    /// doing I/O, so an unavailable catalog/owner cannot starve other groups.
    pub fn drainRecoveryOwnerRequest(self: *ProvisionedKernelOwnerSource) !void {
        if (!self.hasPendingRecoveryOwners()) return;
        if (self.recovery_owner_draining.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
        defer self.recovery_owner_draining.store(false, .release);
        const selected = blk: {
            if (!self.recovery_owner_mutex.tryLock()) return;
            defer self.recovery_owner_mutex.unlock();
            if (self.recovery_owner_closing.load(.acquire)) return;
            for (0..self.recovery_owner_requests.len) |offset| {
                const index = (self.recovery_owner_cursor + offset) % self.recovery_owner_requests.len;
                const request = self.recovery_owner_requests[index];
                if (request.table_name_len == 0) continue;
                self.recovery_owner_cursor = (index + 1) % self.recovery_owner_requests.len;
                break :blk .{ .index = index, .request = request };
            }
            return;
        };
        // Fresh metadata is required after every failure or topology change.
        // Even the lifecycle lane uses the bounded point projection; only the
        // native open itself is outside the caller's recovery budget.
        const catalog_deadline = self.catalog.budget(null).deadlineFrom(table_catalog.RoutingBudget.init(platform_time.monotonicNs() +| 5 * std.time.ns_per_s));
        var descriptor = try self.loadDescriptorWithDeadline(self.alloc, selected.request.group_id, selected.request.name(), catalog_deadline);
        defer descriptor.deinit(self.alloc);
        var lease = try self.acquireDescriptor(selected.request.group_id, selected.request.name(), descriptor.path, descriptor.view());
        defer lease.deinit();
        lock(&self.recovery_owner_mutex);
        defer self.recovery_owner_mutex.unlock();
        const current = &self.recovery_owner_requests[selected.index];
        if (current.revision == selected.request.revision) {
            current.table_name_len = 0;
            _ = self.recovery_owner_pending.fetchSub(1, .release);
        }
    }

    const CandidateConsumerBridge = struct {
        ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,

        fn consumeCandidate(
            ptr: *anyopaque,
            entity_key: []const u8,
            value: []const u8,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try kernel_error_identity.statusToError(self.consume(
                self.ctx,
                .fromSlice(entity_key),
                .fromSlice(value),
            ));
        }
    };

    fn resolutionCandidateGet(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        key: abi.BorrowedBytes,
        consume_ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        if (!self.runtimeHooksAvailable()) return .completion_admission_unavailable;
        const source = self.resolution_candidate_source orelse return .invalid_argument;
        const value = source.get(self.alloc, table.slice(), key.slice()) catch |err|
            return kernel_error_identity.statusFromError(err);
        const bytes = value orelse return .not_found;
        defer self.alloc.free(bytes);
        return consume(consume_ctx, key, .fromSlice(bytes));
    }

    fn resolutionCandidateScanPrefix(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        prefix: abi.BorrowedBytes,
        limit: u64,
        consume_ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        if (!self.runtimeHooksAvailable()) return .completion_admission_unavailable;
        const source = self.resolution_candidate_source orelse return .invalid_argument;
        var bridge = CandidateConsumerBridge{ .ctx = consume_ctx, .consume = consume };
        source.scanPrefix(
            self.alloc,
            table.slice(),
            prefix.slice(),
            .{ .limit = @intCast(@min(limit, std.math.maxInt(usize))) },
            &bridge,
            CandidateConsumerBridge.consumeCandidate,
        ) catch |err| return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn resolutionCandidateNearest(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        index_name: abi.BorrowedBytes,
        embedding_ptr: ?[*]const f32,
        embedding_len: u64,
        k: u64,
        consume_ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        if (!self.runtimeHooksAvailable()) return .completion_admission_unavailable;
        const source = self.resolution_candidate_source orelse return .invalid_argument;
        if (embedding_len > 0 and embedding_ptr == null) return .invalid_argument;
        const embedding = if (embedding_len == 0)
            &.{}
        else
            embedding_ptr.?[0..@intCast(embedding_len)];
        var bridge = CandidateConsumerBridge{ .ctx = consume_ctx, .consume = consume };
        source.nearest(
            self.alloc,
            table.slice(),
            .{
                .index_name = index_name.slice(),
                .embedding = embedding,
                .k = @intCast(@min(k, std.math.maxInt(usize))),
            },
            &bridge,
            CandidateConsumerBridge.consumeCandidate,
        ) catch |err| return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn entityUpsert(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        key: abi.BorrowedBytes,
        doc_json: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        if (!self.runtimeHooksAvailable()) return .completion_admission_unavailable;
        const sink = self.entity_sink orelse return .invalid_argument;
        sink.upsert(self.alloc, table.slice(), key.slice(), doc_json.slice()) catch |err|
            return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn entityUpsertBatch(
        ptr: ?*anyopaque,
        entries_ptr: ?[*]const abi.EntityUpsert,
        entry_count: u64,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        if (!self.runtimeHooksAvailable()) return .completion_admission_unavailable;
        const sink = self.entity_sink orelse return .invalid_argument;
        if (entry_count > 0 and entries_ptr == null) return .invalid_argument;
        const encoded = if (entry_count == 0) &.{} else entries_ptr.?[0..@intCast(entry_count)];
        const entries = self.alloc.alloc(runtime_callbacks.EntityUpsert, encoded.len) catch return .out_of_memory;
        defer self.alloc.free(entries);
        for (encoded, entries) |source, *destination| destination.* = .{
            .table = source.table.slice(),
            .key = source.key.slice(),
            .doc_json = source.doc_json.slice(),
        };
        sink.upsertBatch(self.alloc, entries) catch |err|
            return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn promotionOwner(
        ptr: ?*anyopaque,
        group_id: u64,
    ) callconv(.c) u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return 0));
        if (!self.runtimeHooksAvailable()) return 0;
        const source = self.promotion_leadership_source orelse return 1;
        return @intFromBool(source.isLocalLeader(group_id));
    }

    pub fn withNativeMigrationPolicy(self: *ProvisionedKernelOwnerSource, policy: runtime_callbacks.DenseNativeMigrationPolicySource) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.native_migration_policy = policy;
        return self;
    }

    fn nativeAuthorityPermitted(ptr: ?*const anyopaque) callconv(.c) u8 {
        const self: *const ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr.?));
        return @intFromBool(self.native_migration_policy.?.authorityPermitted());
    }

    fn runtimeHooksConfig(self: *ProvisionedKernelOwnerSource) abi.RuntimeHooksConfig {
        return .{
            .coordinated_ttl_ctx = if (self.coordinated_ttl != null) self else null,
            .coordinated_ttl_enqueue_fn = if (self.coordinated_ttl != null) enqueueCoordinatedTtl else null,
            .native_authority_ctx = if (self.native_migration_policy != null) self else null,
            .native_authority_fn = if (self.native_migration_policy != null) nativeAuthorityPermitted else null,
            .resolution_candidates = if (self.deferred_runtime_hooks or self.resolution_candidate_source != null) .{
                .callback_ctx = self,
                .get_fn = resolutionCandidateGet,
                .scan_prefix_fn = resolutionCandidateScanPrefix,
                .nearest_fn = resolutionCandidateNearest,
            } else .{},
            .entity_sink = if (self.deferred_runtime_hooks or self.entity_sink != null) .{
                .callback_ctx = self,
                .upsert_fn = entityUpsert,
                .upsert_batch_fn = entityUpsertBatch,
            } else .{},
            .promotion_owner_ctx = if (self.deferred_runtime_hooks or self.promotion_leadership_source != null) self else null,
            .promotion_owner_fn = if (self.deferred_runtime_hooks or self.promotion_leadership_source != null) promotionOwner else null,
        };
    }

    pub fn withCoordinatedTtl(self: *ProvisionedKernelOwnerSource, port: @import("../storage/coordinated_ttl.zig").Port) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.coordinated_ttl = port;
        return self;
    }

    fn enqueueCoordinatedTtl(ptr: ?*anyopaque, request: *const abi.CoordinatedTtlRequest) callconv(.c) u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return 1));
        const port = self.coordinated_ttl orelse return 1;
        if (request.candidate_count > abi.coordinated_ttl_page_capacity or
            (request.candidate_count != 0 and request.candidates == null)) return 1;
        var candidates: [abi.coordinated_ttl_page_capacity]@import("../storage/coordinated_ttl.zig").Candidate = undefined;
        for (candidates[0..request.candidate_count], 0..) |*dest, index| {
            const source = request.candidates.?[index];
            if (source.key.len != 0 and source.key.ptr == null) return 1;
            dest.* = .{ .key = source.key.slice(), .row_version = source.row_version, .ttl_timestamp_ns = source.ttl_timestamp_ns, .expected_content_digest = source.expected_content_digest };
        }
        if (request.ttl_field.len != 0 and request.ttl_field.ptr == null) return 1;
        _ = port.expire(.{ .table_id = request.table_id, .group_id = request.group_id, .schema_version = request.schema_version, .ttl_duration_ns = request.ttl_duration_ns, .ttl_field = request.ttl_field.slice(), .observed_at_unix_ns = request.observed_at_unix_ns, .grace_period_ns = request.grace_period_ns, .candidates = candidates[0..request.candidate_count] }) catch return 1;
        return 0;
    }

    fn transactionRecoveryResolve(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        participant: abi.BorrowedBytes,
        status: abi.TxnStatus,
        commit_version: u64,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        source.resolve(
            txn_id.bytes,
            participant.slice(),
            switch (status) {
                .pending => return .invalid_argument,
                .committed => .committed,
                .aborted => .aborted,
            },
            commit_version,
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryOwns(
        ptr: ?*anyopaque,
        owner_participant: abi.BorrowedBytes,
    ) callconv(.c) u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return 0));
        const source = self.transaction_recovery_source orelse return 0;
        return @intFromBool(source.owns(owner_participant.slice()));
    }

    fn transactionRecoveryAcknowledge(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        owner_participant: abi.BorrowedBytes,
        participant: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        source.acknowledge(
            txn_id.bytes,
            owner_participant.slice(),
            participant.slice(),
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryCleanup(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        owner_participant: abi.BorrowedBytes,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        source.cleanup(
            txn_id.bytes,
            owner_participant.slice(),
            cutoff_timestamp,
            retained_cutoff_timestamp,
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryConfig(self: *ProvisionedKernelOwnerSource) abi.TransactionRecoveryConfig {
        const source = self.transaction_recovery_source orelse return .{};
        const options = source.options();
        if (!options.enabled) return .{};
        return .{
            .enabled = 1,
            .lease_owned = @intFromBool(options.lease_owned),
            .replicated_metadata = @intFromBool(options.replicated_metadata),
            .interval_ms = options.interval_ms,
            .cutoff_ns = options.cutoff_ns,
            .callback_ctx = self,
            .owner_id = .fromSlice(options.owner_id),
            .resolve_participant_fn = transactionRecoveryResolve,
            .owns_recovery_fn = if (options.replicated_metadata) transactionRecoveryOwns else null,
            .acknowledge_participant_fn = if (options.replicated_metadata) transactionRecoveryAcknowledge else null,
            .cleanup_transaction_fn = if (options.replicated_metadata) transactionRecoveryCleanup else null,
        };
    }

    fn acquireDescriptor(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
    ) !Lease {
        return try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor, false, .resident, .{});
    }

    pub const RestoreOwnerOptions = struct {
        secret_store: ?*anyopaque = null,
        node_config: ?*const anyopaque = null,
        source_byte_budget: u64 = 1024 * 1024,
    };

    pub const OwnedRestoreDescriptor = struct {
        descriptor: descriptor_contract.Descriptor,

        pub fn view(self: *const OwnedRestoreDescriptor) descriptor_contract.Descriptor {
            return self.descriptor;
        }

        pub fn deinit(self: *OwnedRestoreDescriptor, alloc: std.mem.Allocator) void {
            alloc.free(self.descriptor.schema_json);
            alloc.free(self.descriptor.indexes_json);
            alloc.free(self.descriptor.restore_bootstrap_json);
            descriptor_contract.freeInitialRange(alloc, self.descriptor.initial_range);
            self.* = undefined;
        }
    };

    pub const RestoreDescriptorUse = enum { read, mutate, resolve };
    pub const RestoreDescriptorRecovery = struct {
        const VTable = struct { recover: @FieldType(RestoreDescriptorRecovery, "recover_fn") };
        const Boundary = @import("../runtime_callback_abi.zig").Boundary(VTable);
        ptr: *anyopaque,
        recover_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, [32]u8, [16]u8, RestoreDescriptorUse, @import("operation.zig").RequestContext) anyerror!OwnedRestoreDescriptor,
        boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

        fn recover(self: @This(), alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8, plan_id: [16]u8, use: RestoreDescriptorUse, context: @import("operation.zig").RequestContext) !OwnedRestoreDescriptor {
            return Boundary.call("recover", self.boundary_dispatch, self.recover_fn, .{ self.ptr, alloc, group_id, table_name, scope, plan_id, use, context });
        }
    };

    pub fn withRestoreDescriptorRecovery(self: *ProvisionedKernelOwnerSource, recovery: RestoreDescriptorRecovery) *ProvisionedKernelOwnerSource {
        self.restore_descriptor_recovery = recovery;
        return self;
    }

    /// Cold hidden-owner recovery is bounded by its explicit plan identity.
    /// Only the host's authoritative restore-plan callback can supply a missing
    /// descriptor; ordinary named-table discovery is never an alternative.
    pub fn resolveRestoreDescriptor(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8, plan_id: ?[16]u8, use: RestoreDescriptorUse, context: @import("operation.zig").RequestContext) !OwnedRestoreDescriptor {
        try context.ensureActive();
        var descriptor = if (try self.cachedRestoreDescriptor(alloc, group_id, table_name, scope)) |cached| cached else blk: {
            const plan = plan_id orelse return error.RestoreStagingScopeChanged;
            if (std.mem.allEqual(u8, &plan, 0)) return error.RestoreStagingScopeChanged;
            const recovery = self.restore_descriptor_recovery orelse return error.RestoreStagingScopeChanged;
            break :blk try recovery.recover(alloc, group_id, table_name, scope, plan, use, context);
        };
        errdefer descriptor.deinit(alloc);
        var parsed = try std.json.parseFromSlice(@import("../storage/db/restore_staging_contract.zig").OwnerBootstrap, alloc, descriptor.descriptor.restore_bootstrap_json, .{});
        defer parsed.deinit();
        try parsed.value.validate();
        const namespace = parsed.value.scope.target_namespace;
        if (!std.mem.eql(u8, &scope, &parsed.value.scope.digest()) or
            !std.mem.eql(u8, table_name, parsed.value.table_name) or namespace.shard_id != group_id or
            namespace.table_id != descriptor.descriptor.identity.table_id or namespace.range_id != descriptor.descriptor.identity.range_id or
            descriptor.descriptor.identity.shard_id != group_id or descriptor.descriptor.lsm_root_generation != self.visibleRootGeneration(group_id)) return error.RestoreStagingScopeChanged;
        if (plan_id) |plan| if (!std.mem.eql(u8, &plan, &parsed.value.scope.plan_id)) return error.RestoreStagingScopeChanged;
        try context.ensureActive();
        return descriptor;
    }

    pub fn cachedRestoreDescriptor(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8) !?OwnedRestoreDescriptor {
        const generation = self.visibleRootGeneration(group_id);
        var pinned: Lease = blk: {
            lock(&self.mutex);
            defer self.mutex.unlock();
            for (self.entries.items) |entry| {
                if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or entry.retired or entry.closing or entry.generation != generation or entry.restore_bootstrap_json.len == 0) continue;
                if (!tryReserveEntryLeaseLocked(entry, .shared)) return error.StorageReadTemporarilyUnavailable;
                break :blk .{ .source = self, .entry = entry };
            }
            return null;
        };
        defer pinned.deinit();
        const entry = pinned.entry;
        {
            var bootstrap = try std.json.parseFromSlice(@import("../storage/db/restore_staging_contract.zig").OwnerBootstrap, alloc, entry.restore_bootstrap_json, .{});
            defer bootstrap.deinit();
            if (!std.mem.eql(u8, &scope, &bootstrap.value.scope.digest())) return error.RestoreStagingScopeChanged;
            const schema_json = try alloc.dupe(u8, entry.schema_json);
            errdefer alloc.free(schema_json);
            const indexes_json = try alloc.dupe(u8, entry.indexes_json);
            errdefer alloc.free(indexes_json);
            const restore_bootstrap_json = try alloc.dupe(u8, entry.restore_bootstrap_json);
            errdefer alloc.free(restore_bootstrap_json);
            const initial_range = try descriptor_contract.cloneInitialRange(alloc, entry.initial_range);
            return .{ .descriptor = .{
                .lsm_root_generation = entry.generation,
                .identity = entry.identity,
                .schema_json = schema_json,
                .indexes_json = indexes_json,
                .restore_bootstrap_json = restore_bootstrap_json,
                .restore_cancel_recovery = entry.restore_cancel_recovery,
                .restore_ha_replay = entry.restore_ha_replay,
                .table_storage = entry.table_storage,
                .initial_range = initial_range,
            } };
        }
    }

    /// The caller supplies an authoritative private descriptor, never a public
    /// name lookup. Opening and all physical work remain in the compiled owner.
    pub fn primeRestoreOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor) !void {
        if (descriptor.restore_bootstrap_json.len == 0) return error.RestoreStagingScopeChanged;
        if (descriptor.lsm_root_generation != self.visibleRootGeneration(group_id)) return error.RestoreStagingScopeChanged;
        const path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer self.alloc.free(path);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor);
        defer lease.deinit();
    }

    pub fn restoreOwnerControl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        input: @import("restore_owner.zig").Request,
        proposer: ?@import("restore_owner.zig").Proposer,
        options: RestoreOwnerOptions,
        request: @import("operation.zig").RequestContext,
    ) !@import("restore_owner.zig").Response {
        try request.ensureActive();
        try input.validate(group_id);
        if (descriptor.restore_bootstrap_json.len == 0) return error.RestoreStagingScopeChanged;
        if (descriptor.lsm_root_generation != self.visibleRootGeneration(group_id)) return error.RestoreStagingScopeChanged;
        var bootstrap = try std.json.parseFromSlice(@import("../storage/db/restore_staging_contract.zig").OwnerBootstrap, alloc, descriptor.restore_bootstrap_json, .{});
        defer bootstrap.deinit();
        if (!std.mem.eql(u8, &bootstrap.value.scope.digest(), &input.scope.digest())) return error.RestoreStagingScopeChanged;
        var prepared = try self.prepareRestoreOwnerControl(alloc, group_id, table_name, descriptor, input, options, request);
        defer prepared.deinit();
        const encoded = prepared.value.batch_json orelse return prepared.value.response;
        var batch = try @import("batch.zig").parseInternalBatchRequest(alloc, encoded);
        defer batch.deinit(alloc);
        const scope = batch.req.restore_staging_scope orelse return error.RestoreStagingScopeChanged;
        if (!std.mem.eql(u8, &scope, &input.scope.digest()) or batch.req.restore_staging == null) return error.RestoreStagingScopeChanged;
        // No owner lease crosses the proposal callback: Raft apply must acquire
        // this same owner, and the returned receipt must reflect that commit.
        if (proposer) |replicated| {
            try replicated.submit(batch.req, request);
        } else {
            _ = try self.batchGroupLocalWithDescriptor(alloc, group_id, table_name, batch.req, descriptor);
        }
        const followup = if (input.action == .publish or input.action == .cancel) input else input.statusRead();
        var committed = try self.prepareRestoreOwnerControl(alloc, group_id, table_name, descriptor, followup, options, request);
        defer committed.deinit();
        if (committed.value.batch_json != null) return error.RestoreStagingScopeChanged;
        committed.value.response.source_next_offset = prepared.value.response.source_next_offset;
        return committed.value.response;
    }

    fn prepareRestoreOwnerControl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        input: @import("restore_owner.zig").Request,
        options: RestoreOwnerOptions,
        request: @import("operation.zig").RequestContext,
    ) !std.json.Parsed(@import("restore_owner.zig").Prepared) {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        const json = try std.json.Stringify.valueAlloc(alloc, input, .{});
        defer alloc.free(json);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor, false, .resident, .{ .execution_deadline_ns = request.deadline_ns, .execution_io = request.deadline_io, .cancellation = request.cancellation });
        defer lease.deinit();
        var cancellation = request.cancellation;
        const native_context = try platformDeadlineContext(request);
        var result = try lease.owner().restoreControlJson(.{
            .control = .{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(json),
                .execution_deadline_ns = native_context.deadline_ns orelse 0,
                .has_execution_deadline = @intFromBool(native_context.deadline_ns != null),
                .cancellation_ctx = &cancellation,
                .cancellation_fn = cancellationTokenRequested,
            },
            .source_byte_budget = options.source_byte_budget,
            .secret_store = options.secret_store,
            .node_config = options.node_config,
        });
        defer result.deinit();
        return std.json.parseFromSlice(@import("restore_owner.zig").Prepared, alloc, result.bytes(), .{ .allocate = .alloc_always });
    }

    /// Lease only an already-resident owner whose complete catalog descriptor
    /// still matches. Observability uses this path so a cold status read never
    /// opens storage or discards query-warmed physical coverage.
    fn acquireIfPresent(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !?Lease {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.retired or
                entry.generation != descriptor.generation or
                !entry.identity.eql(descriptor.identity) or
                !std.mem.eql(u8, entry.schema_json, descriptor.schema_json) or
                !std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) or
                !descriptor_contract.initialRangesEqual(entry.initial_range, descriptor.initial_range) or
                !std.meta.eql(entry.table_storage, descriptor.table_storage))
            {
                return null;
            }
            return try self.borrowEntryLocked(entry);
        }
        return null;
    }

    /// The registry mutex and a validated descriptor pin this entry. Borrowing
    /// must neither adopt residency nor admit new work after transient cleanup.
    fn borrowEntryLocked(self: *ProvisionedKernelOwnerSource, entry: *Entry) !Lease {
        if (entry.retired or entry.closing or entry.transient_retirement_pending or !tryReserveEntryLeaseLocked(entry, .shared))
            return error.StorageReadTemporarilyUnavailable;
        _ = self.owner_cache_hits.fetchAdd(1, .monotonic);
        return .{ .source = self, .entry = entry };
    }

    const Residency = enum { transient, resident };

    fn acquireDescriptorExclusive(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        residency: Residency,
    ) !Lease {
        return try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor, true, residency, .{});
    }

    fn acquireDescriptorForReconcile(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        wait_for_readers: bool,
        residency: Residency,
    ) !?Lease {
        if (wait_for_readers) return try self.acquireDescriptorExclusive(group_id, table_name, path, descriptor, residency);
        // Periodic inspection must yield to admitted foreground/maintenance
        // leases. Queueing a writer here closes foreground admission while an
        // existing lease may itself be waiting for a long derived-index apply.
        // Return busy without installing that gate; the startup scheduler
        // retains the inspection debt and retries. Explicit structural changes
        // and admitted repair work keep their writer-preference contract.
        return self.acquireDescriptorOnce(group_id, table_name, path, descriptor, .exclusive_if_idle, residency, .{}) catch |err| switch (err) {
            error.StorageKernelOwnerTransitionRequired => null,
            else => return err,
        };
    }

    fn acquireDescriptorWithMode(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        exclusive: bool,
        residency: Residency,
        controls: ReadControls,
    ) !Lease {
        try controls.check();
        var lease = self.acquireDescriptorOnce(group_id, table_name, path, descriptor, if (exclusive) .exclusive else .shared, residency, controls) catch |err| switch (err) {
            error.StorageKernelOwnerTransitionRequired => try self.acquireDescriptorAfterTransition(
                group_id,
                table_name,
                path,
                descriptor,
                exclusive,
                residency,
                controls,
            ),
            else => return err,
        };
        errdefer lease.deinit();
        try controls.check();
        return lease;
    }

    fn acquireDescriptorAfterTransition(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        exclusive: bool,
        residency: Residency,
        controls: ReadControls,
    ) !Lease {
        errdefer if (exclusive) self.clearExclusivePending(group_id, table_name);
        var wait_io_impl = std.Io.Threaded.init(self.alloc, .{});
        defer wait_io_impl.deinit();
        const wait_io = wait_io_impl.io();
        const deadline_ns = platform_time.monotonicNs() +| 5 * std.time.ns_per_s;
        while (true) {
            try controls.check();
            try wait_io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
            try controls.check();
            return self.acquireDescriptorOnce(group_id, table_name, path, descriptor, if (exclusive) .exclusive else .shared, residency, controls) catch |err| switch (err) {
                error.StorageKernelOwnerTransitionRequired => {
                    try controls.check();
                    if (platform_time.monotonicNs() >= deadline_ns) return error.StorageBusy;
                    continue;
                },
                else => return err,
            };
        }
    }

    fn clearExclusivePending(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.exclusive_active) entry.exclusive_pending = false;
        }
    }

    fn tryReserveEntryLeaseLocked(entry: *Entry, admission: LeaseAdmission) bool {
        const exclusive = admission != .shared;
        if (entry.exclusive_active or (admission != .exclusive and entry.exclusive_pending)) return false;
        if (exclusive and entry.active_users != 0) {
            if (admission == .exclusive) entry.exclusive_pending = true;
            return false;
        }
        entry.active_users += 1;
        if (exclusive) {
            entry.exclusive_pending = false;
            entry.exclusive_active = true;
        }
        return true;
    }

    fn acquireDescriptorOnce(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        admission: LeaseAdmission,
        residency: Residency,
        controls: ReadControls,
    ) !Lease {
        const exclusive = admission != .shared;
        if (!self.mutex.tryLock()) return error.StorageKernelOwnerTransitionRequired;
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        const installation = self.completion_installations.get(group_id);
        if (installation) |record| {
            if (admission != .completion_install and (record.state != .backed or !record.active)) return error.CompletionAdmissionUnavailable;
        }
        // Return to the caller rather than waiting with a descriptor captured
        // before publication; a retry must acquire the new catalog descriptor.
        if (self.publicationPendingLocked(group_id, table_name)) return error.StorageReadTemporarilyUnavailable;
        var stale_index: ?usize = null;
        for (self.entries.items, 0..) |entry, index| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.closing) return error.StorageKernelOwnerTransitionRequired;
            if (entry.retired or entry.generation != descriptor.lsm_root_generation or !entry.identity.eql(descriptor.identity)) {
                if (self.installationPinsOwnerLocked(group_id)) return error.PreparedCompletionActive;
                entry.retired = true;
                if (entry.active_users == 0) {
                    stale_index = index;
                    break;
                }
                return error.StorageKernelOwnerTransitionRequired;
            }
            if (installation) |record| {
                if (record.state == .backed and record.active and
                    std.mem.eql(u8, entry.schema_json, descriptor.schema_json) and
                    std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) and
                    std.meta.eql(@as(?@import("../common/table_storage.zig").Settings, record.settings), descriptor.table_storage))
                    entry.table_storage = descriptor.table_storage;
            }
            if (!std.mem.eql(u8, entry.schema_json, descriptor.schema_json) or
                !std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) or
                !std.mem.eql(u8, entry.restore_bootstrap_json, descriptor.restore_bootstrap_json) or
                !descriptor_contract.initialRangesEqual(entry.initial_range, descriptor.initial_range) or
                entry.restore_cancel_recovery != descriptor.restore_cancel_recovery or
                entry.restore_ha_replay != descriptor.restore_ha_replay or
                !std.meta.eql(entry.table_storage, descriptor.table_storage))
            {
                if (self.installationPinsOwnerLocked(group_id)) return error.PreparedCompletionActive;
                // Catalog definition changes do not necessarily publish a new
                // physical root generation. An API lease is not the only DB
                // activity: index reconciliation may have handed durable work
                // to the owner's background runtime before releasing its
                // lease. Retire the idle owner so close drains that work, then
                // reopen with the new exact descriptor. Live configure here
                // would race the old descriptor's DB-owned maintenance.
                if (entry.active_users != 0) return error.StorageKernelOwnerTransitionRequired;
                entry.retired = true;
                stale_index = index;
                break;
            }
            if (!tryReserveEntryLeaseLocked(entry, admission)) return error.StorageKernelOwnerTransitionRequired;
            if (residency == .resident) {
                entry.resident = true;
                entry.transient_retirement_pending = false;
            }
            _ = self.owner_cache_hits.fetchAdd(1, .monotonic);
            return .{ .source = self, .entry = entry, .exclusive = exclusive };
        }
        if (stale_index) |index| {
            self.destroyEntryAtIndexLocked(index);
            // Closing releases the registry mutex. Recheck the descriptor on
            // retry in case another caller installed its replacement.
            return error.StorageKernelOwnerTransitionRequired;
        }

        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const owned_schema_json = try self.alloc.dupe(u8, descriptor.schema_json);
        errdefer self.alloc.free(owned_schema_json);
        const owned_indexes_json = try self.alloc.dupe(u8, descriptor.indexes_json);
        errdefer self.alloc.free(owned_indexes_json);
        const owned_restore_bootstrap_json = try self.alloc.dupe(u8, descriptor.restore_bootstrap_json);
        errdefer self.alloc.free(owned_restore_bootstrap_json);
        const owned_initial_range = try descriptor_contract.cloneInitialRange(self.alloc, descriptor.initial_range);
        errdefer descriptor_contract.freeInitialRange(self.alloc, owned_initial_range);
        var owned_restore = if (descriptor.restore) |identity| try identity.clone(self.alloc) else null;
        errdefer if (owned_restore) |*identity| identity.deinit(self.alloc);
        const entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(entry);
        try self.ensureContextConfigured();
        // Native completion scopes retain absolute file names. Use the same
        // borrowed filesystem as installation, including when DATA was started
        // with a relative spelling of an existing replica root.
        const canonical_path = if (self.completion_filesystem_io) |io| blk: {
            try @import("../common/fs_paths.zig").createDirPathPortable(io, path);
            break :blk if (std.fs.path.isAbsolute(path))
                try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, self.alloc)
            else
                try std.Io.Dir.cwd().realPathFileAlloc(io, path, self.alloc);
        } else null;
        defer if (canonical_path) |owned| self.alloc.free(owned);
        var cancellation = controls.cancellation orelse db_types.CancellationToken.none;
        const native_context = try platformDeadlineContext(.{
            .deadline_ns = controls.execution_deadline_ns,
            .deadline_io = controls.execution_io,
            .cancellation = cancellation,
        });
        var owner = try client.Owner.open(.{
            .context = self.context.handle,
            .completion_installation = if (installation) |record| &record.binding else null,
            .completion_read_schema_json = if (installation) |record| .fromSlice(record.read_schema_json) else .{},
            .completion_settings_json = if (installation) |record| .fromSlice(record.settings_json) else .{},
            .path = abi.BorrowedBytes.fromSlice(canonical_path orelse path),
            .table_name = abi.BorrowedBytes.fromSlice(table_name),
            .group_id = group_id,
            .lsm_root_generation = descriptor.lsm_root_generation,
            .has_identity_namespace = 1,
            .identity_table_id = descriptor.identity.table_id,
            .identity_shard_id = descriptor.identity.shard_id,
            .identity_range_id = descriptor.identity.range_id,
            .schema_json = .fromSlice(descriptor.schema_json),
            .indexes_json = .fromSlice(descriptor.indexes_json),
            .restore_bootstrap_json = .fromSlice(descriptor.restore_bootstrap_json),
            .restore_cancel_recovery = @intFromBool(descriptor.restore_cancel_recovery),
            .restore_ha_replay = @intFromBool(descriptor.restore_ha_replay),
            .online_source_authority = @intFromEnum(self.online_source_authority),
            .dense_embedding_storage = if (descriptor.table_storage) |settings| switch (settings.dense_embeddings) {
                .primary_lsm => .primary_lsm,
                .vector_store => .vector_store,
            } else .persisted,
            .target_observer = if (self.runtime_status_cache) |cache| .{
                .ctx = cache,
                .notify = targetAdvanced,
            } else .{},
            .transaction_recovery = self.transactionRecoveryConfig(),
            .runtime_hooks = self.runtimeHooksConfig(),
            .has_initial_range = @intFromBool(descriptor.initial_range != null),
            .initial_range_start = .fromSlice(if (descriptor.initial_range) |range| range.start else ""),
            .initial_range_end = .fromSlice(if (descriptor.initial_range) |range| range.end else ""),
            .initial_range_control = .{
                .execution_deadline_ns = native_context.deadline_ns orelse 0,
                .has_execution_deadline = @intFromBool(native_context.deadline_ns != null),
                .cancellation_ctx = &cancellation,
                .cancellation_fn = cancellationTokenRequested,
            },
            .restore = if (descriptor.restore) |identity| .{
                .required = 1,
                .backup_id = .fromSlice(identity.backup_id),
                .location = .fromSlice(identity.location),
                .snapshot_path = .fromSlice(identity.snapshot_path),
                .artifact_sha256 = .fromSlice(identity.artifact_sha256),
                .native_manifest_size_bytes = identity.native_manifest_size_bytes,
                .native_manifest_sha256 = .fromSlice(identity.native_manifest_sha256),
            } else .{},
        });
        errdefer owner.deinit();
        entry.* = .{
            .group_id = group_id,
            .table_name = owned_table_name,
            .generation = descriptor.lsm_root_generation,
            .identity = descriptor.identity,
            .schema_json = owned_schema_json,
            .indexes_json = owned_indexes_json,
            .restore_bootstrap_json = owned_restore_bootstrap_json,
            .initial_range = owned_initial_range,
            .restore_cancel_recovery = descriptor.restore_cancel_recovery,
            .restore_ha_replay = descriptor.restore_ha_replay,
            .table_storage = descriptor.table_storage,
            .restore = owned_restore,
            .owner = owner,
            .active_users = 1,
            .resident = residency == .resident,
            .exclusive_pending = false,
            .exclusive_active = exclusive,
        };
        self.entries.appendAssumeCapacity(entry);
        _ = self.owner_cache_misses.fetchAdd(1, .monotonic);
        return .{ .source = self, .entry = entry, .exclusive = exclusive };
    }

    fn prepareQueryRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.read_safety_barrier);
        reads.prepareSearchWithConsistency(group_id, req, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareSearchWithConsistency(group_id, req, .stale),
            else => return err,
        };
    }

    fn prepareLookupRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.read_safety_barrier);
        reads.prepareLookupWithConsistency(group_id, key, opts, consistency) catch |err| switch (err) {
            // Read-index null is an authoritative absence proof. Never
            // manufacture one by downgrading a failed leader read to stale.
            error.NotLeader => if (consistency != .leader_lease)
                return err
            else
                return try reads.prepareLookupWithConsistency(group_id, key, opts, .stale),
            else => return err,
        };
    }

    fn prepareScanRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.read_safety_barrier);
        reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, .stale),
            else => return err,
        };
    }

    fn prepareGraphExpandRead(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        for (req.frontier) |item| {
            const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
            defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
            try self.prepareQueryRead(group_id, search_req, consistency);
        }
    }

    fn executeQuery(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        raw_search_result: bool,
    ) !client.QueryResponse {
        try table_reads.checkQueryDeadline(req);
        try self.prepareQueryRead(group_id, req, consistency);
        const request_json = try table_reads.encodeStorageKernelQueryRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, .from(req));
        defer lease.deinit();
        try table_reads.checkQueryDeadline(req);
        var cancellation = req.cancellation;
        var execution = @import("../storage/local_query_controls.zig").executionOptions(req);
        execution.raw_search_result = @intFromBool(raw_search_result);
        var response = try lease.owner().queryJsonWithOptions(table_name, request_json, .{
            .execution_deadline_ns = req.execution_deadline_ns,
            .cancellation_ctx = if (cancellation != null) @ptrCast(&cancellation.?) else null,
            .cancellation_fn = if (cancellation != null) cancellationTokenRequested else null,
            .execution = execution,
        });
        errdefer response.deinit();
        try table_reads.checkQueryDeadline(req);
        return response;
    }

    fn unsupportedTopLevelLookup(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: db_types.LookupOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelScan(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: []const u8,
        _: db_types.ScanOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelQuery(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.SearchRequest,
        _: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelBatch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.BatchRequest,
    ) !?void {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn validateRoutedRead(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        try fence.validate();
        if (fence.route.group_id != group_id) return error.TopologyChanged;
        try fence.admission_cancellation.check();
        try table_catalog.validateCatalogRouteFenceUntil(
            alloc,
            self.catalog,
            table_name,
            fence,
            self.catalog.routeFenceDeadline(fence),
        );
        try fence.admission_cancellation.check();
    }

    fn lookupGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (opts.restore_staging_scope != null) return self.lookupRestoreStaging(alloc, group_id, table_name, key, opts, fence);
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try lookupGroupLocal(ptr, alloc, group_id, table_name, key, opts, consistency);
    }

    fn scanGroupLocalRoutedStream(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
        sink: table_read_source.ScanStreamSink,
    ) !bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try scanGroupLocalStream(ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency, sink);
    }

    fn scanGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try scanGroupLocal(ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency);
    }

    fn documentArtifactManifestGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifest {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try documentArtifactManifestGroupLocal(ptr, alloc, group_id, table_name, doc_key, artifact_name, consistency);
    }

    fn documentArtifactManifestsGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifestList {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try documentArtifactManifestsGroupLocal(ptr, alloc, group_id, table_name, doc_key, consistency);
    }

    fn preflightQueryGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try preflightQueryGroupLocal(ptr, alloc, group_id, table_name, req, consistency, max_work);
    }

    fn queryGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try queryGroupLocal(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn searchResultGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.SearchResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try searchResultGroupLocal(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn textStatsGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try textStatsGroupLocal(ptr, alloc, group_id, table_name, body);
    }

    fn algebraicPartialsGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try algebraicPartialsGroupLocal(ptr, alloc, group_id, table_name, body);
    }

    fn graphExpandGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try graphExpandGroupLocal(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn graphHydrateGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try graphHydrateGroupLocal(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn graphEdgesGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try graphEdgesGroupLocal(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (opts.restore_staging_scope != null) return self.lookupRestoreStaging(alloc, group_id, table_name, key, opts, null);
        try self.prepareLookupRead(group_id, key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = lease.owner().lookupJson(table_name, request_json) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        if (opts.include_primary_digest and !table_reads.integrityLookupMode(opts) and response.expectedContentDigest() == null) return error.InvalidResponse;
        return .{
            .json = try alloc.dupe(u8, response.bytes()),
            .version = response.version(),
            .expected_content_digest = response.expectedContentDigest(),
        };
    }

    fn lookupRestoreStaging(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8, opts: db_types.LookupOptions, fence: ?metadata_api.CatalogRouteFence) !?table_read_source.LookupResponse {
        const scope = opts.restore_staging_scope orelse return error.RestoreStagingScopeChanged;
        var descriptor = try self.resolveRestoreDescriptor(alloc, group_id, table_name, scope, opts.restore_staging_plan_id, .read, .{ .deadline_ns = opts.execution_deadline_ns, .deadline_io = opts.execution_io, .cancellation = opts.cancellation orelse .none });
        defer descriptor.deinit(alloc);
        const identity = descriptor.view().identity;
        if (identity.shard_id != group_id) return error.RestoreStagingScopeChanged;
        if (fence) |expected| {
            try expected.validate();
            try expected.admission_cancellation.check();
            if (expected.table_id != identity.table_id or expected.route.group_id != group_id or expected.route.range_id != identity.range_id or
                expected.route.identity_namespace.table_id != identity.table_id or expected.route.identity_namespace.shard_id != identity.shard_id or expected.route.identity_namespace.range_id != identity.range_id) return error.RestoreStagingScopeChanged;
        }
        // Hidden reads certify UNIQUE/FK activation. Never use the ordinary
        // observational NotLeader -> stale fallback for those proofs.
        try feature_reads.FeatureReads.init(self.read_safety_barrier).prepareLookupWithConsistency(group_id, key, opts, .read_index);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor.view(), false, .resident, ReadControls.from(opts));
        defer lease.deinit();
        const encoded = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
        defer alloc.free(encoded);
        var response = lease.owner().lookupJson(table_name, encoded) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        if (opts.include_primary_digest and !table_reads.integrityLookupMode(opts) and response.expectedContentDigest() == null) return error.InvalidResponse;
        return .{ .json = try alloc.dupe(u8, response.bytes()), .version = response.version(), .expected_content_digest = response.expectedContentDigest() };
    }

    fn scanGroupLocalStream(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
        sink: table_read_source.ScanStreamSink,
    ) !bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareScanRead(group_id, from_key, to_key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelScanRequest(alloc, from_key, to_key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, .from(opts));
        defer lease.deinit();
        var cancellation = opts.cancellation;
        try lease.owner().scanStreamWithOptions(table_name, request_json, sink, .{
            .execution_deadline_ns = opts.execution_deadline_ns,
            .cancellation_ctx = if (cancellation != null) @ptrCast(&cancellation.?) else null,
            .cancellation_fn = if (cancellation != null) cancellationTokenRequested else null,
        });
        return true;
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareScanRead(group_id, from_key, to_key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelScanRequest(alloc, from_key, to_key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, .from(opts));
        defer lease.deinit();
        var cancellation = opts.cancellation;
        var response = try lease.owner().scanNdjsonWithOptions(table_name, request_json, .{
            .execution_deadline_ns = opts.execution_deadline_ns,
            .cancellation_ctx = if (cancellation != null) @ptrCast(&cancellation.?) else null,
            .cancellation_fn = if (cancellation != null) cancellationTokenRequested else null,
        });
        defer response.deinit();
        return .{ .ndjson = try alloc.dupe(u8, response.bytes()) };
    }

    fn documentArtifactManifestGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifest {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareLookupRead(group_id, doc_key, .{}, consistency);
        const request_json = try table_reads.encodeStorageKernelDocumentArtifactManifestRequest(
            alloc,
            doc_key,
            artifact_name,
        );
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = lease.owner().documentArtifactManifestJson(
            table_name,
            request_json,
        ) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return try table_reads.parseStorageKernelDocumentArtifactManifestResponse(
            alloc,
            response.bytes(),
        );
    }

    fn documentArtifactManifestsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifestList {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareLookupRead(group_id, doc_key, .{}, consistency);
        const request_json = try table_reads.encodeStorageKernelDocumentArtifactManifestsRequest(
            alloc,
            doc_key,
        );
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().documentArtifactManifestsJson(
            table_name,
            request_json,
        );
        defer response.deinit();
        return try table_reads.parseStorageKernelDocumentArtifactManifestsResponse(
            alloc,
            response.bytes(),
        );
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareQueryRead(group_id, req, consistency);
        const request_json = try table_reads.encodeStorageKernelPreflightRequest(alloc, req, max_work);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, .from(req));
        defer lease.deinit();
        var response = try lease.owner().preflightJson(table_name, request_json);
        defer response.deinit();
        var summary = try table_reads.parseStorageKernelPreflightSummary(alloc, response.bytes());
        table_reads.annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    fn restoreDescriptorUseForBatch(req: db_types.BatchRequest) RestoreDescriptorUse {
        if (req.transaction) |txn| switch (txn) {
            .resolve, .acknowledge, .cleanup => return .resolve,
            else => {},
        };
        return .mutate;
    }

    fn acquirePreparedOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !Lease {
        const generation = self.visibleRootGeneration(group_id);
        if (!self.mutex.tryLock()) return error.RaftApplyWriterUnavailable;
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.retired or entry.closing or entry.generation != generation or
                !tryReserveEntryLeaseLocked(entry, .shared)) return error.RaftApplyWriterUnavailable;
            entry.resident = true;
            entry.transient_retirement_pending = false;
            return .{ .source = self, .entry = entry, .exclusive = false };
        }
        return error.RaftApplyWriterUnavailable;
    }

    fn replicatedBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        metadata_prepared: bool,
        entry: ?db_types.RaftAppliedEntryIdentity,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = if (metadata_prepared)
            try self.acquirePreparedOwner(group_id, table_name)
        else
            try self.acquire(group_id, table_name);
        defer lease.deinit();
        const encoded = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(encoded);
        var response = if (entry) |identity|
            try lease.owner().replicatedBatchAtRaftEntryJson(table_name, encoded, identity.term, identity.index)
        else
            try lease.owner().replicatedBatchJson(table_name, encoded);
        defer response.deinit();
        return {};
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (req.restore_staging_scope) |scope| {
            var descriptor = try self.resolveRestoreDescriptor(alloc, group_id, table_name, scope, req.restore_staging_plan_id, restoreDescriptorUseForBatch(req), .{});
            defer descriptor.deinit(alloc);
            return self.batchGroupLocalWithDescriptor(alloc, group_id, table_name, req, descriptor.view());
        }
        return self.batchGroupLocalWithDescriptor(alloc, group_id, table_name, req, null);
    }

    fn batchGroupLocalWithDescriptor(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        descriptor: ?descriptor_contract.Descriptor,
    ) !?void {
        if (self.ha_write_gate) |gate| try gate.check();
        var ha_mutation = if (self.ha_async_mirror) |mirror|
            if (mirror.mutation_barrier) |barrier| barrier.acquireShared() else null
        else
            null;
        defer if (ha_mutation) |*lease| lease.release();
        try self.preflightHAMirrorSyncCommit();
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        const private_path = if (descriptor != null) try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id }) else null;
        defer if (private_path) |path| alloc.free(path);
        var lease = if (descriptor) |owned| try self.acquireDescriptor(group_id, table_name, private_path.?, owned) else try self.acquire(group_id, table_name);
        defer lease.deinit();
        var callback_error_relay: kernel_error_identity.CallbackErrorRelay = .{};
        var committed_effects_context = CommittedBatchEffectsContext{
            .source = self,
            .request = req,
            .identity = lease.entry.identity,
            .error_relay = &callback_error_relay,
        };
        var dispatch_context = if (self.document_child_range_dispatch_source) |source|
            DocumentChildRangeDispatchContext{
                .alloc = alloc,
                .source = source,
                .table_name = table_name,
            }
        else
            null;
        var response: client.Response = .{};
        const callback_status = lease.owner().batchJsonWithCallbacksStatus(
            table_name,
            request_json,
            if (dispatch_context) |*context| context else null,
            if (dispatch_context != null) dispatchDocumentChildRange else null,
            &committed_effects_context,
            committedBatchEffects,
            &response,
        );
        defer response.deinit();
        try callback_error_relay.finish(callback_status);
        return {};
    }

    fn preflightHAMirrorSyncCommit(self: *ProvisionedKernelOwnerSource) !void {
        const mirror = self.ha_async_mirror orelse return;
        if (mirror.sync_policy.mode == .async or mirror.sync_policy.failure_policy != .fail_closed) return;
        const target_lsn = mirror.primary.nextLsn();
        const decision = try mirror.primary.evaluateAppendDurability(target_lsn, mirror.sync_policy);
        const gate = ha_commit_gate.GateResult{
            .target_lsn = target_lsn,
            .action = switch (decision.status) {
                .satisfied => .acknowledge,
                .would_block => .wait_for_standby,
                .fail_closed => .reject,
                .degraded_to_async => .acknowledge_degraded,
            },
            .decision = decision,
        };
        recordHAMirrorGate(mirror, gate);
        if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
    }

    const CommittedBatchEffectsContext = struct {
        source: *ProvisionedKernelOwnerSource,
        request: db_types.BatchRequest,
        identity: Identity,
        error_relay: *kernel_error_identity.CallbackErrorRelay,
    };

    fn committedBatchEffects(
        ptr: ?*anyopaque,
        replay_payload: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const context: *CommittedBatchEffectsContext = @ptrCast(@alignCast(ptr orelse
            return .invalid_argument));
        context.source.mirrorHABatchMutationCommit(context.request, context.identity) catch |err|
            return context.error_relay.capture(err);
        if (replay_payload.len != 0) {
            context.source.mirrorHAReplayPayloadCommit(replay_payload.slice(), context.identity) catch |err|
                return context.error_relay.capture(err);
        }
        return .ok;
    }

    fn mirrorHABatchMutationCommit(
        self: *ProvisionedKernelOwnerSource,
        req: db_types.BatchRequest,
        identity: Identity,
    ) !void {
        const mirror = self.ha_async_mirror orelse return;
        const transition_mutex = mirror.transition_mutex;
        if (transition_mutex) |mutex| lock(mutex);
        var transition_locked = transition_mutex != null;
        defer if (transition_locked) transition_mutex.?.unlock();

        const lsn = ha_effects.appendBatchMutationRequest(self.alloc, mirror.primary, req, .{
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
        }) catch |err| {
            noteHAMirrorFailure(mirror, err);
            if (mirror.sync_policy.mode != .async) return err;
            return;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);

        if (transition_mutex) |mutex| {
            mutex.unlock();
            transition_locked = false;
        }
        try evaluateHAMirrorCommitGate(mirror, lsn);
        if (transition_mutex) |mutex| {
            lock(mutex);
            transition_locked = true;
        }
        if (self.ha_write_gate) |gate| try gate.check();
    }

    fn mirrorHAReplayPayloadCommit(
        self: *ProvisionedKernelOwnerSource,
        replay_payload: []const u8,
        identity: Identity,
    ) !void {
        const mirror = self.ha_async_mirror orelse return;
        const transition_mutex = mirror.transition_mutex;
        if (transition_mutex) |mutex| lock(mutex);
        var transition_locked = transition_mutex != null;
        defer if (transition_locked) transition_mutex.?.unlock();

        const lsn = ha_effects.appendEncodedDerivedChangeRecord(mirror.primary, replay_payload, .{
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
        }) catch |err| {
            noteHAMirrorFailure(mirror, err);
            if (mirror.sync_policy.mode != .async) return err;
            return;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);

        if (transition_mutex) |mutex| {
            mutex.unlock();
            transition_locked = false;
        }
        try evaluateHAMirrorCommitGate(mirror, lsn);
        if (transition_mutex) |mutex| {
            lock(mutex);
            transition_locked = true;
        }
        if (self.ha_write_gate) |gate| try gate.check();
    }

    fn evaluateHAMirrorCommitGate(mirror: ha_contract.AsyncEffectMirror, lsn: u64) !void {
        if (mirror.sync_policy.mode == .async) return;
        var gate = try ha_commit_gate.evaluate(mirror.primary, lsn, mirror.sync_policy);
        recordHAMirrorGate(mirror, gate);
        switch (gate.action) {
            .acknowledge, .acknowledge_degraded => return,
            .reject => return error.SyncPolicyUnsatisfied,
            .wait_for_standby => {
                const wait_fn = mirror.sync_wait_fn orelse return error.HASyncCommitWouldBlock;
                const wait_ctx = mirror.sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext;
                try wait_fn(wait_ctx, mirror.primary, lsn, mirror.sync_policy);
                gate = try ha_commit_gate.evaluate(mirror.primary, lsn, mirror.sync_policy);
                recordHAMirrorGate(mirror, gate);
                switch (gate.action) {
                    .acknowledge, .acknowledge_degraded => return,
                    .reject => return error.SyncPolicyUnsatisfied,
                    .wait_for_standby => return error.HASyncCommitWouldBlock,
                }
            },
        }
    }

    fn recordHAMirrorGate(mirror: ha_contract.AsyncEffectMirror, gate: ha_commit_gate.GateResult) void {
        if (mirror.last_gate_lsn) |last_lsn| last_lsn.store(gate.target_lsn, .release);
        if (mirror.last_gate_action) |last_action| last_action.store(@intFromEnum(gate.action), .release);
        switch (gate.action) {
            .acknowledge => {},
            .acknowledge_degraded => {
                if (mirror.sync_degraded_count) |counter| _ = counter.fetchAdd(1, .monotonic);
            },
            .reject => {
                if (mirror.sync_reject_count) |counter| _ = counter.fetchAdd(1, .monotonic);
            },
            .wait_for_standby => {
                if (mirror.sync_wait_count) |counter| _ = counter.fetchAdd(1, .monotonic);
            },
        }
    }

    fn noteHAMirrorFailure(mirror: ha_contract.AsyncEffectMirror, err: anyerror) void {
        if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror compiled-owner commit into HA stream: {s}", .{@errorName(err)});
    }

    const DocumentChildRangeDispatchContext = struct {
        alloc: std.mem.Allocator,
        source: table_write_source.TableWriteSource,
        table_name: []const u8,
    };

    fn dispatchDocumentChildRange(
        ptr: ?*anyopaque,
        owner_group_id: u64,
        request_json: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const context: *DocumentChildRangeDispatchContext = @ptrCast(@alignCast(ptr orelse
            return .invalid_argument));
        var parsed = std.json.parseFromSlice(
            table_writes.StorageKernelArtifactChildRangeBatchRequest,
            context.alloc,
            request_json.slice(),
            .{ .allocate = .alloc_always },
        ) catch |err| return documentChildRangeDispatchStatusFromError(err);
        defer parsed.deinit();
        const sequence = context.source.applyDocumentArtifactChildRangeBatch(
            context.alloc,
            owner_group_id,
            context.table_name,
            parsed.value.doc_key,
            parsed.value.artifact_name,
            parsed.value.batch,
        ) catch |err| return documentChildRangeDispatchStatusFromError(err);
        if (sequence == null) return .not_found;
        return .ok;
    }

    fn documentChildRangeDispatchStatusFromError(err: anyerror) abi.Status {
        return kernel_error_identity.statusFromError(err);
    }

    fn vectorMigrationGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request_json: []const u8) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = lease.owner().vectorMigrationJson(table_name, request_json) catch |err| {
            if (err == error.VectorMigrationRecoveryRequired or err == error.VectorPayloadStorePoisoned)
                lease.retireAfterConfigurationFailure();
            return err;
        };
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn executeArtifactOperation(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        operation: abi.ArtifactOperation,
        request_json: []const u8,
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
        defer_durable_index_repair_execution: bool,
    ) !client.Response {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try lease.owner().artifactOperationJson(
            table_name,
            operation,
            request_json,
            cancellation_ctx,
            cancellation_fn,
            defer_durable_index_repair_execution,
        );
    }

    fn corruptEmbeddingArtifactGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelEmbeddingCorruptionRequest(
            alloc,
            doc_key,
            index_name,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .corrupt_embedding,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        if (!try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes())) return error.NotFound;
        return {};
    }

    fn reprocessDocumentArtifactGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactDocumentRequest(
            alloc,
            doc_key,
            artifact_name,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .reprocess_document,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes());
    }

    fn reprocessDocumentArtifactRangeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
        request: db_types.DocumentArtifactTableReprocessRequest,
    ) !?db_types.DocumentArtifactTableReprocessResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactRangeRequest(
            alloc,
            artifact_name,
            request,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .reprocess_document_range,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelDocumentArtifactTableReprocessResult(
            alloc,
            response.bytes(),
        );
    }

    fn listArtifactRepairIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairListRequest,
    ) !?db_types.ArtifactRepairListResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactRepairListRequest(alloc, request);
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .list_repair_issues,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelArtifactRepairListResult(alloc, response.bytes());
    }

    const ArtifactRepairCancellation = struct {
        check: db_types.RepairCancelCheck,

        fn requested(ctx: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return 0));
            return @intFromBool(self.check.requested());
        }
    };

    fn validateArtifactRepairControls(options: db_types.ArtifactRepairRunOptions) !void {
        const defaults = db_types.ArtifactRepairRunOptions{};
        if (options.yield_check != null or
            options.activation_check != null or
            options.capacity_source != null or
            options.capacity_check != null or
            options.owner_epoch != defaults.owner_epoch or
            options.max_activation_gap_sequences != defaults.max_activation_gap_sequences or
            options.max_convergence_rounds != defaults.max_convergence_rounds or
            options.max_activation_pause_ms != defaults.max_activation_pause_ms or
            options.estimated_candidate_bytes != defaults.estimated_candidate_bytes or
            options.planned_disk_bytes != defaults.planned_disk_bytes or
            options.capacity_domain_id != defaults.capacity_domain_id or
            !std.meta.eql(options.capacity_observation, defaults.capacity_observation))
        {
            return error.UnsupportedStorageKernelRepairControls;
        }
    }

    fn repairArtifactIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairRunRequest,
    ) !?db_types.ArtifactRepairResult {
        return try repairArtifactIssuesGroupLocalControlled(
            ptr,
            alloc,
            group_id,
            table_name,
            request,
            .{},
        );
    }

    fn repairArtifactIssuesGroupLocalControlled(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairRunRequest,
        options: db_types.ArtifactRepairRunOptions,
    ) !?db_types.ArtifactRepairResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try validateArtifactRepairControls(options);
        if (options.cancelled()) return error.Canceled;
        const request_json = try table_writes.encodeStorageKernelArtifactRepairRequest(alloc, request);
        defer alloc.free(request_json);
        var cancellation: ?ArtifactRepairCancellation = if (options.cancel_check) |check|
            .{ .check = check }
        else
            null;
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .repair_issues,
            request_json,
            if (cancellation) |*value| value else null,
            if (cancellation != null) ArtifactRepairCancellation.requested else null,
            options.defer_durable_index_repair_execution,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelArtifactRepairResult(alloc, response.bytes());
    }

    fn updateDocumentArtifactChildRangePlacementGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactPlacementRequest(
            alloc,
            doc_key,
            artifact_name,
            update,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .update_child_range_placement,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes());
    }

    fn applyDocumentArtifactChildRangeBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        batch: document_artifact_child_range.ApplyBatch,
    ) !?u64 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactChildRangeBatchRequest(
            alloc,
            doc_key,
            artifact_name,
            batch,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .apply_child_range_batch,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelSequenceResponse(alloc, response.bytes());
    }

    fn applyTransactionGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !void {
        return self.applyTransactionGroupLocalWithContext(alloc, group_id, table_name, req, .{});
    }

    fn applyTransactionGroupLocalWithContext(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        context: request_operation.RequestContext,
    ) !void {
        try context.ensureActive();
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquireTransactionOwner(alloc, group_id, table_name, req, context);
        defer lease.deinit();
        try context.ensureActive();
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
    }

    /// Provider for existing native owners. Installation and catalog discovery
    /// happen outside Raft callbacks, before these references can be acquired.
    /// Called only with authenticated metadata output, outside DATA Raft
    /// serialization. Failure remains represented in the registry so guard
    /// acquisition cannot downgrade this group to legacy operation.
    pub const PreparedCompletionInstallation = struct {
        source: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        descriptor: LoadedDescriptor,
        record: *CompletionInstallation,
        table_name: []const u8,
        schema_json: []const u8,
        indexes_json: []const u8,
        registered: bool = false,
        capacity_reserved: bool = true,
        already_backed: bool = false,
        filesystem_io: ?std.Io = null,

        pub fn deinit(self: *@This()) void {
            lock(&self.source.mutex);
            if (self.capacity_reserved) self.source.completion_installation_preparations -= 1;
            if (self.registered and self.record.state == .installing) self.record.state = .failed;
            self.source.mutex.unlock();
            self.descriptor.deinit(self.alloc);
            if (!self.registered) {
                self.source.alloc.free(self.record.read_schema_json);
                self.source.alloc.free(self.record.settings_json);
                self.source.alloc.destroy(self.record);
            }
            self.* = undefined;
        }

        /// Allocation-free publication under DATA Raft serialization. The
        /// caller first proves its accepted frontier fully applied, then this
        /// barrier prevents a concurrent legacy proposal during physical setup.
        pub fn begin(self: *@This()) !void {
            if (!self.source.mutex.tryLock()) return error.CompletionAdmissionUnavailable;
            defer self.source.mutex.unlock();
            if (self.source.quiescing) return error.CompletionAdmissionUnavailable;
            const group_id = self.record.binding.identity.group_id;
            if (self.source.completion_installations.get(group_id)) |existing| {
                if (existing.state != .backed or existing.identity_mismatch or !std.meta.eql(existing.binding, self.record.binding) or
                    !std.meta.eql(existing.settings, self.record.settings)) return error.CompletionAdmissionUnavailable;
                if (self.record.metadata_authorized) {
                    if (existing.active and !self.record.active) {
                        existing.identity_mismatch = true;
                        existing.metadata_authorized = false;
                        return error.CompletionProfileChanged;
                    }
                    existing.metadata_authorized = true;
                    if (self.record.active) existing.active = true;
                }
                self.already_backed = true;
                self.source.completion_installation_preparations -= 1;
                self.capacity_reserved = false;
                return;
            }
            for (self.source.entries.items) |entry| {
                if (entry.group_id == group_id and (entry.active_users != 0 or entry.closing or entry.exclusive_pending))
                    return error.CompletionAdmissionUnavailable;
            }
            // Capacity was reserved while preparing, before DATA took its lock.
            self.source.completion_installations.putAssumeCapacity(group_id, self.record);
            self.source.completion_installation_preparations -= 1;
            self.capacity_reserved = false;
            self.registered = true;
        }

        pub fn finish(self: *@This()) !void {
            if (self.already_backed) return;
            if (!self.registered) return error.CompletionAdmissionUnavailable;
            return self.source.finishCompletionInstallation(self.filesystem_io orelse return error.CompletionAdmissionUnavailable, self.record.binding.identity.group_id, self.table_name, self.descriptor, self.record, self.schema_json, self.indexes_json) catch |err| {
                lock(&self.source.mutex);
                self.record.state = .failed;
                self.source.mutex.unlock();
                return err;
            };
        }
    };

    pub fn prepareCompletionInstallation(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, binding: abi.completion_pool.InstallBinding, schema_json: []const u8, read_schema_json: []const u8, indexes_json: []const u8, settings: @import("../common/table_storage.zig").Settings, active: bool, restore_existing: bool) !PreparedCompletionInstallation {
        // Fresh production installation stays closed until ordinary begin,
        // decision, and acknowledgement entries have protected admission too.
        // Do not publish a barrier that could strand legacy transaction debt.
        if (!@import("../common/durable_completion_policy.zig").replicated_activation_supported and !restore_existing)
            return error.CompletionAdmissionUnavailable;
        if (binding.identity.group_id != group_id) return error.CompletionAdmissionUnavailable;
        var descriptor = try self.loadDescriptor(alloc, group_id, table_name);
        errdefer descriptor.deinit(alloc);
        if (descriptor.identity.table_id != binding.table_id or descriptor.identity.range_id != binding.range_id)
            return error.CompletionProfileChanged;
        if (!std.mem.eql(u8, &try @import("../common/completion_catalog_digest.zig").digest(alloc, descriptor.schema_json, read_schema_json, descriptor.indexes_json), &binding.schema_catalog_digest))
            return error.CompletionProfileChanged;
        return try self.prepareCompletionDescriptor(alloc, descriptor, table_name, binding, schema_json, read_schema_json, indexes_json, settings, active, true);
    }

    /// The caller obtained this value only from a validated local capsule. No
    /// catalog projection or metadata access is performed on the restart path.
    pub fn prepareCompletionRestoration(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, io: std.Io, value: completion_capsule.Value) !PreparedCompletionInstallation {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, value.binding.identity.group_id });
        errdefer alloc.free(path);
        try completion_capsule.validateRoot(alloc, io, path, value);
        const schema = try alloc.dupe(u8, value.schema_json);
        errdefer alloc.free(schema);
        const indexes = try alloc.dupe(u8, value.indexes_json);
        errdefer alloc.free(indexes);
        const descriptor: LoadedDescriptor = .{
            .path = path,
            .schema_json = schema,
            .indexes_json = indexes,
            .table_storage = value.settings,
            .generation = value.root_generation,
            .identity = .{ .table_id = value.binding.table_id, .range_id = value.binding.range_id, .shard_id = value.shard_id },
        };
        var prepared = try self.prepareCompletionDescriptor(alloc, descriptor, value.table_name, value.binding, value.schema_json, value.read_schema_json, value.indexes_json, value.settings, false, false);
        prepared.filesystem_io = io;
        return prepared;
    }

    fn prepareCompletionDescriptor(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, descriptor: LoadedDescriptor, table_name: []const u8, binding: abi.completion_pool.InstallBinding, schema_json: []const u8, read_schema_json: []const u8, indexes_json: []const u8, settings: @import("../common/table_storage.zig").Settings, active: bool, authorized: bool) !PreparedCompletionInstallation {
        const record = try self.alloc.create(CompletionInstallation);
        errdefer self.alloc.destroy(record);
        const read_copy = try self.alloc.dupe(u8, read_schema_json);
        errdefer self.alloc.free(read_copy);
        const settings_json = try std.json.Stringify.valueAlloc(self.alloc, settings, .{});
        errdefer self.alloc.free(settings_json);
        record.* = .{ .binding = binding, .read_schema_json = read_copy, .settings_json = settings_json, .settings = settings, .active = active, .metadata_authorized = authorized };
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.completion_installations.count() + self.completion_installation_preparations >= 1024)
            return error.CompletionAdmissionUnavailable;
        try self.completion_installations.ensureUnusedCapacity(self.alloc, @intCast(self.completion_installation_preparations + 1));
        self.completion_installation_preparations += 1;
        return .{ .source = self, .alloc = alloc, .descriptor = descriptor, .record = record, .table_name = table_name, .schema_json = schema_json, .indexes_json = indexes_json };
    }

    pub fn completionInstallationPresent(self: *ProvisionedKernelOwnerSource, group_id: u64) bool {
        // Contention must not manufacture a legacy absence.
        if (!self.mutex.tryLock()) return true;
        defer self.mutex.unlock();
        return self.completion_installations.contains(group_id);
    }

    pub fn completionBackingIdentityMatches(self: *ProvisionedKernelOwnerSource, identity: abi.completion_pool.Identity) bool {
        if (!self.mutex.tryLock()) return false;
        defer self.mutex.unlock();
        const record = self.completion_installations.get(identity.group_id) orelse return false;
        return record.state == .backed and std.meta.eql(record.binding.identity, identity);
    }

    pub fn completionAdmissionAuthorized(self: *ProvisionedKernelOwnerSource, group_id: u64) bool {
        return self.completionAuthorization(group_id, true);
    }

    pub fn completionInstallationAuthorized(self: *ProvisionedKernelOwnerSource, group_id: u64) bool {
        return self.completionAuthorization(group_id, false);
    }

    fn completionAuthorization(self: *ProvisionedKernelOwnerSource, group_id: u64, require_active: bool) bool {
        if (!self.mutex.tryLock()) return false;
        defer self.mutex.unlock();
        const record = self.completion_installations.get(group_id) orelse return false;
        return !self.quiescing and record.state == .backed and (!require_active or record.active) and record.metadata_authorized and !record.identity_mismatch;
    }

    /// A verified authority mismatch fences new work without releasing backing
    /// or disturbing accepted/prepared recovery. A later cache cannot undo it.
    pub fn fenceCompletionAuthorization(self: *ProvisionedKernelOwnerSource, group_id: u64) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.completion_installations.get(group_id)) |record| {
            record.metadata_authorized = false;
            record.identity_mismatch = true;
        }
    }

    fn finishCompletionInstallation(self: *ProvisionedKernelOwnerSource, io: std.Io, group_id: u64, table_name: []const u8, descriptor: LoadedDescriptor, record: *CompletionInstallation, schema_json: []const u8, indexes_json: []const u8) !void {
        lock(&self.mutex);
        self.completion_filesystem_io = io;
        self.mutex.unlock();
        var lease = try self.acquireDescriptorOnce(group_id, table_name, descriptor.path, descriptor.view(), .completion_install, .resident, .{});
        defer lease.deinit();
        try lease.owner().installCompletion(.{ .binding = record.binding, .schema_json = .fromSlice(schema_json), .read_schema_json = .fromSlice(record.read_schema_json), .indexes_json = .fromSlice(indexes_json), .settings_json = .fromSlice(record.settings_json) });
        // Native install is still hidden behind state.installing. Persist the
        // complete restart configuration before any provider can accept work.
        var capsule: completion_capsule.Value = .{
            .binding = record.binding,
            .table_name = table_name,
            .shard_id = descriptor.identity.shard_id,
            .root_generation = descriptor.generation,
            .settings = record.settings,
            .schema_json = schema_json,
            .read_schema_json = record.read_schema_json,
            .indexes_json = indexes_json,
            .canonical_root_digest = undefined,
            .root_identity_digest = undefined,
            .receipt_digest = undefined,
        };
        try completion_capsule.bindRoot(self.alloc, io, descriptor.path, &capsule);
        try completion_capsule.publish(self.alloc, io, descriptor.path, capsule);
        lock(&self.mutex);
        record.state = .backed;
        self.mutex.unlock();
    }

    pub fn completionProvider(self: *ProvisionedKernelOwnerSource) abi.completion_pool.Provider {
        return .{ .context = self, .acquire = acquireCompletionBacking, .attest = attestCompletionBacking };
    }

    /// Borrow an already installed owner only. This path runs under DATA Raft
    /// serialization and must not perform catalog refresh, root open, or install.
    fn installedCompletionOwner(self: *ProvisionedKernelOwnerSource, group_id: u64) !Lease {
        if (!self.mutex.tryLock()) return error.CompletionAdmissionUnavailable;
        defer self.mutex.unlock();
        if (self.quiescing) return error.CompletionAdmissionUnavailable;
        const installation = self.completion_installations.get(group_id) orelse return error.NotFound;
        if (installation.state != .backed) return error.CompletionAdmissionUnavailable;
        var found: ?*Entry = null;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or entry.retired or entry.closing) continue;
            if (entry.exclusive_active or entry.exclusive_pending or found != null) return error.CompletionAdmissionUnavailable;
            found = entry;
        }
        const entry = found orelse return error.CompletionAdmissionUnavailable;
        entry.active_users += 1;
        return .{ .source = self, .entry = entry };
    }

    fn acquireCompletionBacking(raw: ?*anyopaque, group_id: u64, node_id: u64, output: *abi.completion_pool.Lease) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(raw orelse return .invalid_argument));
        var lease = self.installedCompletionOwner(group_id) catch |err| return kernel_error_identity.statusFromError(err);
        defer lease.deinit();
        output.* = lease.owner().acquireCompletionLease(group_id, node_id) catch |err| return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn attestCompletionBacking(raw: ?*anyopaque, group_id: u64, node_id: u64, output: *abi.completion_pool.NativeAttestation) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(raw orelse return .invalid_argument));
        if (!self.completionInstallationAuthorized(group_id)) return .completion_admission_unavailable;
        var lease = self.installedCompletionOwner(group_id) catch |err| return kernel_error_identity.statusFromError(err);
        defer lease.deinit();
        output.* = lease.owner().attestCompletionBacking(group_id, node_id) catch |err| return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    /// Pins owner configuration and input outside DATA Raft's lock. Only the
    /// synchronous storage-kernel compiler runs under proposal ordering.
    pub const CompletionCompiler = struct {
        lease: Lease,
        alloc: std.mem.Allocator,
        request_json: []u8,
        table_name: []const u8,

        pub fn compile(self: *CompletionCompiler, previous: db_types.RaftAppliedEntryIdentity) !client.Response {
            return self.lease.owner().compileReplicatedCompletion(self.table_name, self.request_json, previous.term, previous.index);
        }
        pub fn deinit(self: *CompletionCompiler) void {
            self.lease.deinit();
            self.alloc.free(self.request_json);
            self.* = undefined;
        }
    };

    pub fn prepareCompletionCompiler(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_types.BatchRequest) !CompletionCompiler {
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        errdefer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        errdefer lease.deinit();
        // A rejected provisional proposal leaves a spent native generation.
        // Revisit the trusted installation outside DATA proposal ordering so
        // its idle checkpoint/rearm can finish before the next capture. The
        // retained guard itself must never run streaming maintenance.
        const installation = blk: {
            if (!self.mutex.tryLock()) return error.CompletionAdmissionUnavailable;
            defer self.mutex.unlock();
            const record = self.completion_installations.get(group_id) orelse return error.CompletionAdmissionUnavailable;
            if (self.quiescing or record.state != .backed or !record.active or
                !record.metadata_authorized or record.identity_mismatch) return error.CompletionAdmissionUnavailable;
            // Identity and these owned slices are immutable after publication;
            // the owner lease pins SourceOwner teardown across the C call.
            break :blk abi.InstallCompletionRequest{
                .binding = record.binding,
                .schema_json = .fromSlice(lease.entry.schema_json),
                .read_schema_json = .fromSlice(record.read_schema_json),
                .indexes_json = .fromSlice(lease.entry.indexes_json),
                .settings_json = .fromSlice(record.settings_json),
            };
        };
        try lease.owner().installCompletion(installation);
        return .{ .lease = lease, .alloc = alloc, .request_json = request_json, .table_name = table_name };
    }

    pub fn compileReplicatedCompletionGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        previous: db_types.RaftAppliedEntryIdentity,
    ) !client.Response {
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return lease.owner().compileReplicatedCompletion(table_name, request_json, previous.term, previous.index);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        retain_terminal: bool,
        participants: []const []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .transaction = .{ .begin = .{
                .txn_id = txn_id,
                .begin_timestamp = begin_timestamp,
                .created_at_ns = platform_time.realtimeNs(),
                .topology_epoch = topology_epoch,
                .retain_terminal = retain_terminal,
                .participants = participants,
            } },
        });
        return {};
    }

    fn txnBeginGroupLocalWithPreDecisionContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        retain_terminal: bool,
        participants: []const []const u8,
        context: @import("distributed_txn_contract.zig").PreDecisionContext,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const active: request_operation.RequestContext = .{ .deadline_ns = context.deadline_ns, .deadline_io = context.deadline_io, .cancellation = context.cancellation };
        try active.ensureActive();
        if (context.restore_staging_scope == null and context.restore_staging_plan_id != null) return error.RestoreStagingScopeChanged;
        try self.applyTransactionGroupLocalWithContext(alloc, group_id, table_name, .{
            .restore_staging_scope = context.restore_staging_scope,
            .restore_staging_plan_id = context.restore_staging_plan_id,
            .transaction = .{ .begin = .{
                .txn_id = txn_id,
                .begin_timestamp = begin_timestamp,
                .created_at_ns = platform_time.realtimeNs(),
                .topology_epoch = topology_epoch,
                .retain_terminal = retain_terminal,
                .participants = participants,
            } },
        }, active);
        return {};
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        topology_epoch: u64,
        req: db_types.TransactionIntentRequest,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        comptime std.debug.assert(@sizeOf(db_types.TransactionWrite) == @sizeOf(db_types.BatchWrite));
        if (req.relational_index_maintenance) |command| if (command.owner_group_id != group_id) return error.PreparedGenerationChanged;
        comptime std.debug.assert(@alignOf(db_types.TransactionWrite) == @alignOf(db_types.BatchWrite));
        const writes: []const db_types.BatchWrite = @ptrCast(req.writes);
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .writes = writes,
            .deletes = req.deletes,
            .transforms = req.transforms,
            .predicates = req.predicates,
            .integrity = req.integrity,
            .integrity_commands = req.integrity_commands,
            .relational_schema_version = req.relational_schema_version,
            .relational_integrity_generation_set = req.relational_integrity_generation_set,
            .relational_repair = req.relational_repair,
            .relational_activation = req.relational_activation,
            .relational_retirement = req.relational_retirement,
            .relational_index_maintenance = req.relational_index_maintenance,
            .restore_staging_scope = req.restore_staging_scope,
            .restore_staging_plan_id = req.restore_staging_plan_id,
            .transaction = .{ .prepare = .{
                .txn_id = txn_id,
                .topology_epoch = topology_epoch,
            } },
        });
        return {};
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        status: db_types.TxnStatus,
        commit_version: u64,
        _: u64,
        sync_level: db_types.SyncLevel,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .sync_level = sync_level,
            .transaction = .{ .resolve = .{
                .txn_id = txn_id,
                .status = status,
                .commit_version = commit_version,
            } },
        });
        return {};
    }

    fn txnResolveGroupLocalWithCancellation(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        status: db_types.TxnStatus,
        commit_version: u64,
        _: u64,
        sync_level: db_types.SyncLevel,
        cancellation: db_types.CancellationToken,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try cancellation.check();
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, .{
            .sync_level = sync_level,
            .transaction = .{ .resolve = .{ .txn_id = txn_id, .status = status, .commit_version = commit_version } },
        });
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try cancellation.check();
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
        return {};
    }

    fn txnResolveGroupLocalUntil(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        status: db_types.TxnStatus,
        commit_version: u64,
        _: u64,
        sync_level: db_types.SyncLevel,
        deadline_ns: u64,
        cancellation: db_types.CancellationToken,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try checkRecoveryBudget(deadline_ns, cancellation);
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, .{
            .sync_level = sync_level,
            .transaction = .{ .resolve = .{ .txn_id = txn_id, .status = status, .commit_version = commit_version } },
        });
        defer alloc.free(request_json);
        var lease = try self.acquireForRecovery(group_id, table_name, deadline_ns, cancellation);
        defer lease.deinit();
        try checkRecoveryBudget(deadline_ns, cancellation);
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
        return {};
    }

    fn txnDecideGroupLocalWithPreDecisionContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        status: db_types.TxnStatus,
        commit_version: u64,
        _: u64,
        sync_level: db_types.SyncLevel,
        context: @import("distributed_txn_contract.zig").PreDecisionContext,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const contract = @import("distributed_txn_contract.zig");
        if (status != .committed) return error.PreDecisionNotProposed;
        contract.ensurePreDecisionContextActive(context) catch return error.PreDecisionNotProposed;
        const request_json = table_writes.encodeStorageKernelBatchRequest(alloc, .{
            .sync_level = sync_level,
            .transaction = .{ .resolve = .{ .txn_id = txn_id, .status = status, .commit_version = commit_version } },
        }) catch return error.PreDecisionNotProposed;
        defer alloc.free(request_json);
        var lease = self.acquire(group_id, table_name) catch return error.PreDecisionNotProposed;
        defer lease.deinit();
        // Owner acquisition may have opened a DB or waited for admission.
        // Only this final, pre-invocation check proves no decision was sent.
        contract.ensurePreDecisionContextActive(context) catch return error.PreDecisionNotProposed;
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
        return {};
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
    ) !?db_types.TxnStatus {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = (try self.acquireHiddenTransactionOwner(group_id, table_name)) orelse try self.acquire(group_id, table_name);
        defer lease.deinit();
        return switch (try lease.owner().transactionStatus(table_name, txn_id)) {
            .pending => .pending,
            .committed => .committed,
            .aborted => .aborted,
        };
    }

    fn txnStatusGroupLocalUntil(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        deadline_ns: u64,
    ) !?db_types.TxnStatus {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquireForRecovery(group_id, table_name, deadline_ns, .none);
        defer lease.deinit();
        try checkRecoveryBudget(deadline_ns, .none);
        const status = try lease.owner().transactionStatus(table_name, txn_id);
        // This is a read, not a new decision: an observation finishing after
        // its budget cannot substitute for a timely authoritative response.
        try checkRecoveryBudget(deadline_ns, .none);
        return switch (status) {
            .pending => .pending,
            .committed => .committed,
            .aborted => .aborted,
        };
    }

    fn txnAcknowledgeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        participant: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .transaction = .{ .acknowledge = .{
                .txn_id = txn_id,
                .participant = participant,
            } },
        });
        return {};
    }

    fn txnAcknowledgeGroupLocalUntil(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        participant: []const u8,
        deadline_ns: u64,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, .{
            .transaction = .{ .acknowledge = .{ .txn_id = txn_id, .participant = participant } },
        });
        defer alloc.free(request_json);
        var lease = try self.acquireForRecovery(group_id, table_name, deadline_ns, .none);
        defer lease.deinit();
        // Recovery owns this absolute deadline independently of the original
        // caller. Acquisition cannot silently rebase its remaining budget.
        if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
        return {};
    }

    fn beginBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().beginBulkIngest(table_name);
        lease.entry.bulk_ingest_active.store(true, .release);
        return {};
    }

    const BulkFinishCallbacks = struct {
        options: backend_types.BulkIngestFinishOptions,
        error_relay: kernel_error_identity.CallbackErrorRelay = .{},

        fn progress(
            ctx: ?*anyopaque,
            value: *const abi.BulkProgress,
        ) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            const callback = self.options.progress_fn orelse return;
            callback(self.options.progress_ctx.?, .{
                .phase = switch (value.phase) {
                    .begin => .begin,
                    .split => .split,
                    .publish => .publish,
                    .complete => .complete,
                },
                .publish_window = value.publish_window,
                .split_steps = value.split_steps,
                .deferred_leaf_splits = value.deferred_leaf_splits,
                .elapsed_ns = value.elapsed_ns,
            });
        }

        fn admission(ctx: ?*anyopaque) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.options.checkAdmission() catch |err| return self.error_relay.capture(err);
            return .ok;
        }
    };

    pub fn validateBulkCallbackIdentityForTest() !void {
        const Admission = struct {
            fn fail(_: *anyopaque) !void {
                return error.TestBulkAdmissionIdentity;
            }
        };
        var admission_context: u8 = 0;
        var callbacks = BulkFinishCallbacks{ .options = .{
            .admission_ctx = &admission_context,
            .admission_fn = Admission.fail,
        } };
        const callback_status = BulkFinishCallbacks.admission(&callbacks);

        // Model the real provider adapter: callback status becomes a Zig
        // error inside storage, then becomes a status again at the exported
        // operation boundary. The consumer relay must still win.
        const provider_status = blk: {
            kernel_error_identity.statusToError(callback_status) catch |err| {
                break :blk kernel_error_identity.statusFromError(err);
            };
            break :blk abi.Status.ok;
        };
        try std.testing.expectEqual(abi.Status.storage_kernel_callback_failed, provider_status);
        try std.testing.expectError(
            error.TestBulkAdmissionIdentity,
            callbacks.error_relay.finish(provider_status),
        );
    }

    fn finishBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var callbacks = BulkFinishCallbacks{ .options = options };
        const request = abi.BulkFinishRequest{
            .compact = @intFromBool(options.compact),
            .flush = @intFromBool(options.flush),
            .has_max_deferred_l0_runs = @intFromBool(options.max_deferred_l0_runs != null),
            .has_max_foreground_compaction_input_bytes = @intFromBool(options.max_foreground_compaction_input_bytes != null),
            .has_max_foreground_compaction_ns = @intFromBool(options.max_foreground_compaction_ns != null),
            .has_max_deferred_hbc_leaf_splits_per_publish = @intFromBool(options.max_deferred_hbc_leaf_splits_per_publish != null),
            .has_max_deferred_hbc_leaf_split_members_per_publish = @intFromBool(options.max_deferred_hbc_leaf_split_members_per_publish != null),
            .has_bulk_rebuild_hbc_leaf_min_members = @intFromBool(options.bulk_rebuild_hbc_leaf_min_members != null),
            .table_name = .fromSlice(table_name),
            .max_deferred_l0_runs = @intCast(options.max_deferred_l0_runs orelse 0),
            .max_foreground_compaction_steps = @intCast(options.max_foreground_compaction_steps),
            .max_foreground_compaction_input_bytes = options.max_foreground_compaction_input_bytes orelse 0,
            .max_foreground_compaction_ns = options.max_foreground_compaction_ns orelse 0,
            .max_deferred_hbc_leaf_splits_per_publish = @intCast(options.max_deferred_hbc_leaf_splits_per_publish orelse 0),
            .max_deferred_hbc_leaf_split_members_per_publish = @intCast(options.max_deferred_hbc_leaf_split_members_per_publish orelse 0),
            .bulk_rebuild_hbc_leaf_min_members = @intCast(options.bulk_rebuild_hbc_leaf_min_members orelse 0),
            .callback_ctx = &callbacks,
            .progress_fn = if (options.progress_fn != null and options.progress_ctx != null)
                BulkFinishCallbacks.progress
            else
                null,
            .admission_fn = if (options.admission_fn != null and options.admission_ctx != null)
                BulkFinishCallbacks.admission
            else
                null,
        };
        const status = lease.owner().finishBulkIngestStatus(&request);
        try callbacks.error_relay.finish(status);
        lease.entry.bulk_ingest_active.store(false, .release);
        return {};
    }

    fn abortBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = self.acquire(group_id, table_name) catch |err| {
            std.log.warn("storage owner bulk abort acquire failed table={s} group_id={d} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return;
        };
        defer lease.deinit();
        lease.owner().abortBulkIngest(table_name) catch |err| {
            std.log.warn("storage owner bulk abort failed table={s} group_id={d} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return;
        };
        lease.entry.bulk_ingest_active.store(false, .release);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        var items = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (items.items) |*item| item.deinit(alloc);
            items.deinit(alloc);
        }
        for (group_ids) |group_id| {
            // Each group is an independent best-effort observation. One cold
            // or busy owner must not discard facts sampled from its siblings.
            var status = (localRuntimeStatusGroupLocal(self, alloc, group_id, table_name) catch |err| switch (err) {
                error.StorageBusy, error.StorageReadTemporarilyUnavailable => continue,
                else => return err,
            }) orelse continue;
            errdefer status.deinit(alloc);
            try items.append(alloc, status);
        }
        if (items.items.len == 0) {
            items.deinit(alloc);
            return null;
        }
        return .{ .items = try items.toOwnedSlice(alloc) };
    }

    fn localRuntimeStatusGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatus {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = (try self.acquireIfPresent(group_id, table_name)) orelse return null;
        defer lease.deinit();
        var response = try lease.owner().runtimeStatusJson(table_name);
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(
            runtime_status.LocalTableRuntimeStatus,
            alloc,
            response.bytes(),
            .{},
        );
        defer parsed.deinit();
        var observed = try parsed.value.clone(alloc);
        observed.group_id = group_id;
        observed.metadata.lsm_root_generation = lease.entry.generation;
        return observed;
    }

    const ObservationCancellationDispatch = struct {
        token: db_types.CancellationToken,

        fn requested(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *const ObservationCancellationDispatch = @ptrCast(@alignCast(ptr orelse return 0));
            return @intFromBool(self.token.isCancelled());
        }
    };

    fn observedDynamicFieldCapabilitySets(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        observation: table_reads.DynamicFieldObservationQuery,
    ) !?[]table_reads.ObservedDynamicFieldCapabilitySet {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        const request_json = try table_reads.encodeStorageKernelDynamicFieldObservationRequest(alloc, observation);
        defer alloc.free(request_json);
        var cancellation = ObservationCancellationDispatch{
            .token = observation.cancellation orelse .none,
        };
        var merged = std.ArrayListUnmanaged(table_reads.ObservedDynamicFieldCapabilitySet).empty;
        errdefer {
            for (merged.items) |*set| set.deinit(alloc);
            merged.deinit(alloc);
        }

        for (group_ids) |group_id| {
            // Observation is best effort and must not map a cold text index.
            // Query admission owns the warm-and-retry protocol and installs a
            // resident owner whose validated coverage remains visible to the
            // subsequent status read.
            var lease = (try self.acquireIfPresent(group_id, table_name)) orelse
                return error.StorageReadTemporarilyUnavailable;
            defer lease.deinit();
            var response = try lease.owner().observedDynamicFieldCapabilitySetsJson(
                table_name,
                request_json,
                observation.execution_deadline_ns,
                if (observation.cancellation != null) @ptrCast(&cancellation) else null,
                if (observation.cancellation != null) ObservationCancellationDispatch.requested else null,
            );
            defer response.deinit();
            var parsed = try std.json.parseFromSlice(
                []table_reads.ObservedDynamicFieldCapabilitySet,
                alloc,
                response.bytes(),
                .{},
            );
            defer parsed.deinit();
            for (parsed.value) |set| try table_reads.mergeObservedDynamicFieldCapabilitySet(alloc, &merged, set);
        }
        return try merged.toOwnedSlice(alloc);
    }

    fn textMemoryAttributionStatsBestEffort(
        ptr: *anyopaque,
    ) text_memory.TextMemoryAttributionStats {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return .{};
        var cursor: usize = 0;
        var result: text_memory.TextMemoryAttributionStats = .{};
        while (self.nextMaintenanceLease(&cursor, limit, true, false)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            var response = lease.owner().textMemoryJson(lease.entry.table_name) catch continue;
            defer response.deinit();
            var parsed = std.json.parseFromSlice(
                text_memory.TextMemoryAttributionStats,
                self.alloc,
                response.bytes(),
                .{},
            ) catch continue;
            defer parsed.deinit();
            result.accumulate(parsed.value);
        }
        return result;
    }

    fn graphMetricMaintenanceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().graphMetricMaintenanceJson(table_name, body);
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().textStatsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().algebraicPartialsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        try self.prepareGraphExpandRead(alloc, group_id, controlled, consistency);
        const request_json = try distributed_graph.encodeGraphExpandRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, .from(controlled));
        defer lease.deinit();
        var cancellation = req.cancellation;
        var response = try lease.owner().graphExpandJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphExpandResponse(alloc, response.bytes());
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        try self.prepareQueryRead(group_id, table_reads.graphHydrateSearchRequest(controlled), consistency);
        const request_json = try distributed_graph.encodeGraphHydrateRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, .from(controlled));
        defer lease.deinit();
        var cancellation = req.cancellation;
        var response = try lease.owner().graphHydrateJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphHydrateResponse(alloc, response.bytes());
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        try self.prepareLookupRead(group_id, req.key, .{}, consistency);
        const request_json = try distributed_graph.encodeGraphEdgesRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, .from(controlled));
        defer lease.deinit();
        var cancellation = req.cancellation;
        var response = try lease.owner().graphEdgesJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphEdgesResponse(alloc, response.bytes());
    }

    fn cancellationTokenRequested(ctx: ?*anyopaque) callconv(.c) u8 {
        const token: *const db_types.CancellationToken = @ptrCast(@alignCast(ctx orelse return 0));
        return @intFromBool(token.isCancelled());
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency, false);
        defer response.deinit();
        return .{
            .json = try alloc.dupe(u8, response.bytes()),
            .identity_read_generation = response.identityReadGeneration(),
        };
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.SearchResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        // This is a raw shard phase. The coordinator owns its aggregation;
        // queryGroupLocal instead requests a complete local response.
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency, true);
        defer response.deinit();
        var result = try table_reads.parseStorageKernelSearchResult(alloc, response.bytes());
        errdefer result.deinit();
        result.identity_read_generation = response.identityReadGeneration();
        if (req.identity_read_generation) |expected| {
            if (result.identity_read_generation != expected)
                return error.IdentityReadGenerationChanged;
        }
        return result;
    }
};

test "compiled owner coordinated ttl admission preserves exact observations and pressure" {
    const ttl = @import("../storage/coordinated_ttl.zig");
    const Fake = struct {
        calls: usize = 0,
        pressure: bool = false,
        fn enqueue(ptr: *anyopaque, request: ttl.Request) !u32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.pressure) return error.CoordinatedTtlBackpressure;
            try std.testing.expectEqual(@as(u64, 17), request.table_id);
            try std.testing.expectEqual(@as(u64, 23), request.group_id);
            try std.testing.expectEqual(@as(u32, 3), request.schema_version);
            try std.testing.expectEqualStrings("expires", request.ttl_field);
            try std.testing.expectEqual(@as(usize, 1), request.candidates.len);
            try std.testing.expectEqualStrings("\x00\xffkey", request.candidates[0].key);
            try std.testing.expectEqual(@as(u64, 99), request.candidates[0].row_version);
            try std.testing.expectEqual(@as(u64, 44), request.candidates[0].ttl_timestamp_ns);
            try std.testing.expectEqual([_]u8{0xa7} ** 32, request.candidates[0].expected_content_digest);
            self.calls += 1;
            return 0;
        }
    };
    var fake = Fake{};
    var source: ProvisionedKernelOwnerSource = undefined;
    source.coordinated_ttl = .{ .ptr = &fake, .expire_fn = Fake.enqueue };
    const candidates = [_]abi.CoordinatedTtlCandidate{.{ .key = .fromSlice("\x00\xffkey"), .row_version = 99, .ttl_timestamp_ns = 44, .expected_content_digest = @splat(0xa7) }};
    var request = abi.CoordinatedTtlRequest{ .table_id = 17, .group_id = 23, .schema_version = 3, .ttl_duration_ns = 100, .ttl_field = .fromSlice("expires"), .observed_at_unix_ns = 200, .grace_period_ns = 1, .candidates = &candidates, .candidate_count = 1 };
    try std.testing.expectEqual(@as(u8, 0), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    fake.pressure = true;
    try std.testing.expectEqual(@as(u8, 1), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    fake.pressure = false;
    request.candidate_count = abi.coordinated_ttl_page_capacity + 1;
    try std.testing.expectEqual(@as(u8, 1), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    request.candidate_count = 1;
    request.candidates = null;
    try std.testing.expectEqual(@as(u8, 1), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "storage owner quiesce drains leases and promotion callbacks before context destruction" {
    const alloc = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("owner-quiesce");
    defer directory.cleanup();
    const path = std.mem.span(directory.path().ptr);
    var source = ProvisionedKernelOwnerSource.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.unavailableReadSafetyBarrier());
    defer source.deinit();
    const Callback = struct {
        entered: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(bool) = .init(false),
        fn isLeader(ptr: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.released.load(.acquire)) std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
            return false;
        }
    };
    var callback = Callback{};
    // Release on any failed assertion before source.deinit joins the worker.
    defer callback.released.store(true, .release);
    _ = try source.withRuntimeHooks(null, null, .{ .ptr = &callback, .vtable = &.{ .is_local_leader = Callback.isLeader } });
    const descriptor = descriptor_contract.Descriptor{
        .lsm_root_generation = 0,
        .identity = .{ .table_id = 7, .shard_id = 7001, .range_id = 7001 },
        .indexes_json =
        \\{"relations_graph":{"type":"graph","source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"},"resolvers":[{"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1","key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","config_generation":1,"_antfly_destination_authorization_v1":{"principal":"service:auth-disabled","signature":"auth-disabled","destinations":["entities"]}}]}}
        ,
    };
    var lease = try source.acquireDescriptor(7001, "docs", path, descriptor);
    var lease_active = true;
    defer if (lease_active) lease.deinit();
    errdefer callback.released.store(true, .release);
    var response = try lease.owner().batchJson("docs",
        \\{"inserts":{"a":{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada"}]}}},"sync_level":"write"}
    );
    response.deinit();
    const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!callback.entered.load(.acquire)) {
        if (platform_time.monotonicNs() >= deadline) return error.PromotionCallbackDidNotStart;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const Shutdown = struct {
        source: *ProvisionedKernelOwnerSource,
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.source.quiesce(std.testing.io) catch |err| {
                self.err = err;
            };
            self.done.store(true, .release);
        }
    };
    var shutdown = Shutdown{ .source = &source };
    var shutdown_task = try std.testing.io.concurrent(Shutdown.run, .{&shutdown});
    defer {
        callback.released.store(true, .release);
        if (lease_active) {
            lease.deinit();
            lease_active = false;
        }
        shutdown_task.await(std.testing.io);
    }
    while (true) {
        ProvisionedKernelOwnerSource.lock(&source.mutex);
        const quiescing = source.quiescing;
        source.mutex.unlock();
        if (quiescing) break;
        if (platform_time.monotonicNs() >= deadline) return error.QuiesceDidNotStart;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!shutdown.done.load(.acquire));
    try std.testing.expectError(error.Canceled, source.acquireDescriptor(7001, "docs", path, descriptor));
    // Closing the last lease must wait for the autonomous callback as well.
    var release_task = try std.testing.io.concurrent(ProvisionedKernelOwnerSource.Lease.deinit, .{&lease});
    lease_active = false;
    defer {
        callback.released.store(true, .release);
        release_task.await(std.testing.io);
    }
    while (true) {
        ProvisionedKernelOwnerSource.lock(&source.mutex);
        const closing = source.entries.items.len == 1 and source.entries.items[0].closing;
        source.mutex.unlock();
        if (closing) break;
        if (platform_time.monotonicNs() >= deadline) return error.OwnerCloseDidNotStart;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!shutdown.done.load(.acquire));
    callback.released.store(true, .release);
    while (!shutdown.done.load(.acquire)) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    if (shutdown.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
    try source.quiesce(std.testing.io);
    try std.testing.expectError(error.Canceled, source.acquireDescriptor(7001, "docs", path, descriptor));
}

test "committed owner apply yields admission conflicts and retries the exact entry once" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]enum { registry, exclusive, publication }{ .registry, .exclusive, .publication }) |history| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer alloc.free(root);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-1/table-db", .{root});
        defer alloc.free(path);
        var source = Source.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        try source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", descriptor, .{
            .writes = &.{.{ .key = "doc:counter", .value = "{\"count\":0}" }},
        }, 1, 1);
        const increment: db_types.BatchRequest = .{ .transforms = &.{.{
            .key = "doc:counter",
            .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
        }} };
        {
            var exclusive: ?Source.Lease = null;
            var publication: ?*Source.PendingPublication = null;
            switch (history) {
                .registry => Source.lock(&source.mutex),
                .exclusive => exclusive = try source.acquireDescriptorExclusive(1, "docs", path, descriptor, .resident),
                .publication => publication = try source.registerPublication(1, "docs"),
            }
            defer switch (history) {
                .registry => source.mutex.unlock(),
                .exclusive => exclusive.?.deinit(),
                .publication => Source.endPublication(&source, publication.?),
            };
            const started = platform_time.monotonicNs();
            try std.testing.expectError(
                if (history == .publication) error.StorageReadTemporarilyUnavailable else error.StorageBusy,
                source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", descriptor, increment, 1, 2),
            );
            // The gate stays held by this test. Waiting for the five-second
            // foreground admission timeout would strand the Raft progress lane.
            try std.testing.expect(platform_time.monotonicNs() - started < std.time.ns_per_s);
        }
        // Publication may first retire the old owner. Bounded progress retries
        // reopen it without abandoning the original term/index identity.
        for (0..4) |_| {
            source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", descriptor, increment, 1, 2) catch |err| switch (err) {
                error.StorageBusy => continue,
                else => return err,
            };
            break;
        } else return error.TestOwnerAdmissionDidNotRecover;
        try source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", descriptor, increment, 1, 2);
        var reader = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer reader.deinit();
        var value = try reader.owner().lookupJson("docs", "{\"key\":\"doc:counter\",\"include_all_fields\":true}");
        defer value.deinit();
        try std.testing.expect(std.mem.indexOf(u8, value.bytes(), "\"count\":1") != null);
    }
}

test "pending exclusive storage owner lease blocks new readers until drain" {
    var entry: ProvisionedKernelOwnerSource.Entry = undefined;
    entry.active_users = 1;
    entry.exclusive_pending = false;
    entry.exclusive_active = false;

    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive));
    try std.testing.expect(entry.exclusive_pending);
    try std.testing.expectEqual(@as(usize, 1), entry.active_users);

    // Observational status reads arriving after the writer must not starve it.
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
    try std.testing.expectEqual(@as(usize, 1), entry.active_users);

    // Once the original reader drains, the waiting exclusive lease wins and
    // clears the pending gate while its active gate remains authoritative.
    entry.active_users = 0;
    try std.testing.expect(ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive));
    try std.testing.expect(!entry.exclusive_pending);
    try std.testing.expect(entry.exclusive_active);
    try std.testing.expectEqual(@as(usize, 1), entry.active_users);
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
}

test "background storage owner lease inspection yields without gating readers" {
    var entry: ProvisionedKernelOwnerSource.Entry = undefined;
    entry.active_users = 1;
    entry.exclusive_pending = false;
    entry.exclusive_active = false;

    // A long-lived query or maintenance lease must not turn periodic
    // inspection into a barrier for later foreground requests.
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive_if_idle));
    try std.testing.expect(!entry.exclusive_pending and !entry.exclusive_active);
    try std.testing.expect(ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
    try std.testing.expectEqual(@as(usize, 2), entry.active_users);

    // Inspection can run once admitted users drain, with the same exclusion
    // while actually reconciling. It cannot jump an explicit structural waiter.
    entry.active_users = 0;
    entry.exclusive_pending = true;
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive_if_idle));
    try std.testing.expect(entry.exclusive_pending);
    entry.exclusive_pending = false;
    try std.testing.expect(ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive_if_idle));
    try std.testing.expect(entry.exclusive_active and !entry.exclusive_pending);
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
}

test "transient storage owner retirement drains borrowers and permits foreground adoption" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]enum { observation_finished, observation_held, maintenance_held, foreground_adoption, prepared_adoption }{ .observation_finished, .observation_held, .maintenance_held, .foreground_adoption, .prepared_adoption }) |history| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
        defer alloc.free(path);
        var source = Source.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        var transient = try source.acquireDescriptorWithMode(1, "docs", path, descriptor, false, .transient, .{});
        defer transient.deinit();
        const original = transient.entry;
        var observation: ?Source.Lease = null;
        defer if (observation) |*lease| lease.deinit();
        var maintenance: ?[]Source.Lease = null;
        defer if (maintenance) |leases| source.releaseMaintenanceLeases(leases);
        if (history == .maintenance_held) {
            maintenance = (try source.snapshotOwnerLeases(false, false)).?;
            try std.testing.expectEqual(@as(usize, 1), maintenance.?.len);
        } else {
            Source.lock(&source.mutex);
            observation = source.borrowEntryLocked(original) catch |err| {
                source.mutex.unlock();
                return err;
            };
            source.mutex.unlock();
            if (history == .observation_finished) observation.?.deinit();
        }
        try std.testing.expect(!original.resident);
        transient.requestTransientRetirement();
        transient.deinit();
        if (history == .observation_finished) {
            try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
        } else {
            try std.testing.expectEqual(@as(usize, 1), source.ownerCountForTest());
            try std.testing.expect(original.transient_retirement_pending);
            // Once cleanup begins, new observational work cannot starve drain.
            Source.lock(&source.mutex);
            const refused = source.borrowEntryLocked(original);
            source.mutex.unlock();
            try std.testing.expectError(error.StorageReadTemporarilyUnavailable, refused);
            const excluded = (try source.snapshotOwnerLeases(false, false)).?;
            defer source.releaseMaintenanceLeases(excluded);
            try std.testing.expectEqual(@as(usize, 0), excluded.len);
            if (history == .foreground_adoption or history == .prepared_adoption) {
                var foreground = if (history == .prepared_adoption) try source.acquirePreparedOwner(1, "docs") else try source.acquireDescriptor(1, "docs", path, descriptor);
                defer foreground.deinit();
                try std.testing.expectEqual(original, foreground.entry);
                try std.testing.expect(original.resident);
                try std.testing.expect(!original.transient_retirement_pending);
                foreground.deinit();
            }
            if (observation) |*lease| lease.deinit();
            if (maintenance) |leases| {
                source.releaseMaintenanceLeases(leases);
                maintenance = null;
            }
            try std.testing.expectEqual(@as(usize, if (history == .foreground_adoption or history == .prepared_adoption) 1 else 0), source.ownerCountForTest());
        }
        // A finished transient lease cannot retire a later replacement owner.
        var resident = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer resident.deinit();
        const misses = source.cacheStats().miss_count;
        transient.deinit();
        try std.testing.expectEqual(@as(usize, 1), source.ownerCountForTest());
        try std.testing.expectEqual(@as(u64, if (history == .foreground_adoption or history == .prepared_adoption) 1 else 2), misses);
    }
}

test "storage repair lease downgrade admits readers while fencing configuration" {
    const Source = ProvisionedKernelOwnerSource;
    var source: Source = undefined;
    source.mutex = .unlocked;
    var entry: Source.Entry = undefined;
    entry.active_users = 1;
    entry.exclusive_active = true;
    entry.exclusive_pending = false;
    var lease = Source.Lease{ .source = &source, .entry = &entry, .exclusive = true };
    try std.testing.expect(!Source.tryReserveEntryLeaseLocked(&entry, .shared));
    lease.downgrade();
    try std.testing.expect(!lease.exclusive);
    try std.testing.expect(Source.tryReserveEntryLeaseLocked(&entry, .shared));
    try std.testing.expectEqual(@as(usize, 2), entry.active_users);
    try std.testing.expect(!Source.tryReserveEntryLeaseLocked(&entry, .exclusive));
}

test "scheduled repair admission yields to readers and reuses exact configured generation" {
    const Source = ProvisionedKernelOwnerSource;
    var source = Source.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.entries.deinit(std.testing.allocator);
    var entry: Source.Entry = .{
        .group_id = 1,
        .table_name = @constCast("docs"),
        .generation = 7,
        .identity = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        .schema_json = @constCast("schema"),
        .indexes_json = @constCast("indexes"),
        .restore_bootstrap_json = @constCast(""),
        .owner = undefined,
        .active_users = 1,
        .resident = true,
    };
    try source.entries.append(std.testing.allocator, &entry);
    const descriptor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = 7,
        .identity = entry.identity,
        .schema_json = entry.schema_json,
        .indexes_json = entry.indexes_json,
    };
    try std.testing.expect((try source.acquireDescriptorForReconcile(1, "docs", "/unused", descriptor, false, .transient)) == null);
    try std.testing.expect(!entry.exclusive_pending);
    try std.testing.expect(Source.tryReserveEntryLeaseLocked(&entry, .shared));
    entry.active_users -= 1;
    entry.repair_target = @constCast("text");
    entry.repair_configuration = .{ .state = .busy, .repair_remaining = 1 };
    var repair = source.tryAcquireConfiguredRepair(1, "docs", descriptor, "text").?;
    try std.testing.expect(!repair.lease.exclusive);
    try std.testing.expectEqual(@as(usize, 2), entry.active_users);
    repair.lease.deinit();
    try std.testing.expect(source.tryAcquireConfiguredRepair(1, "docs", descriptor, "other") == null);
    var changed = descriptor;
    changed.schema_json = "new schema";
    try std.testing.expect(source.tryAcquireConfiguredRepair(1, "docs", changed, "text") == null);
    entry.repair_target = null;
    var ordinary = source.tryAcquireConfiguredRepair(1, "docs", descriptor, null).?;
    try std.testing.expect(!ordinary.lease.exclusive);
    ordinary.lease.deinit();
    changed = descriptor;
    changed.lsm_root_generation += 1;
    try std.testing.expect(source.tryAcquireConfiguredRepair(1, "docs", changed, null) == null);
}

// The fake clock controls only the publication wait. Owners below use the real
// compiled storage kernel, including worker shutdown and registry removal.
const PublicationWaitTest = struct {
    now_ns: i96 = 0,
    sleeps: usize = 0,
    lease: ?*ProvisionedKernelOwnerSource.Lease = null,
    cancel: ?*std.atomic.Value(bool) = null,

    fn now(ptr: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
        const self: *@This() = @ptrCast(@alignCast(ptr.?));
        return .{ .nanoseconds = self.now_ns };
    }

    fn sleep(ptr: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
        const self: *@This() = @ptrCast(@alignCast(ptr.?));
        self.sleeps += 1;
        self.now_ns += std.time.ns_per_ms;
        if (self.lease) |lease| {
            lease.deinit();
            self.lease = null;
        }
        if (self.cancel) |signal| signal.store(true, .release);
    }

    fn io(self: *@This(), vtable: *std.Io.VTable) std.Io {
        vtable.* = std.testing.io.vtable.*;
        vtable.now = now;
        vtable.sleep = sleep;
        return .{ .userdata = self, .vtable = vtable };
    }
};

test "publication drains existing readers status and maintenance before reopening admission" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]enum { reader, status, maintenance }{ .reader, .status, .maintenance }) |history| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
        defer alloc.free(path);
        var source = Source.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        var original = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer original.deinit();
        var held: Source.Lease = undefined;
        var maintenance: ?[]Source.Lease = null;
        defer if (maintenance) |leases| source.releaseMaintenanceLeases(leases);
        switch (history) {
            .reader => held = try source.acquireDescriptor(1, "docs", path, descriptor),
            .status => {
                Source.lock(&source.mutex);
                held = source.borrowEntryLocked(original.entry) catch |err| {
                    source.mutex.unlock();
                    return err;
                };
                source.mutex.unlock();
            },
            .maintenance => {
                maintenance = (try source.snapshotOwnerLeases(false, false)).?;
                try std.testing.expectEqual(@as(usize, 1), maintenance.?.len);
            },
        }
        const borrower = if (maintenance) |leases| &leases[0] else &held;
        defer borrower.deinit();
        original.deinit();
        // Force publication to see a live user, then release it at the first
        // cooperative wait. The old implementation returned StorageBusy here.
        var wait = PublicationWaitTest{ .lease = borrower };
        var vtable: std.Io.VTable = undefined;
        var publication = try source.snapshotSource().beginPublication(.{
            .io = wait.io(&vtable),
            .group_id = 1,
            .table_name = "docs",
        });
        defer publication.deinit();
        try std.testing.expectEqual(@as(usize, 1), wait.sleeps);
        try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
        // With no Entry remaining, the independent gate still excludes open
        // and prepared apply until the publisher completes commit/rollback.
        try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptor(1, "docs", path, descriptor));
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.acquirePreparedOwner(1, "docs"));
        try std.testing.expectError(error.StorageBusy, source.snapshotSource().beginPublication(.{
            .io = wait.io(&vtable),
            .group_id = 1,
            .table_name = "docs",
        }));
        const excluded = (try source.snapshotOwnerLeases(false, false)).?;
        defer source.releaseMaintenanceLeases(excluded);
        try std.testing.expectEqual(@as(usize, 0), excluded.len);
        publication.deinit();
        var replacement = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer replacement.deinit();
        try std.testing.expectEqual(@as(u64, 2), source.cacheStats().miss_count);
    }
}

test "maintenance on one owner does not pin another owner against publication" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    var source = Source.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();

    for ([_]struct { group_id: u64, name: []const u8 }{
        .{ .group_id = 1, .name = "busy" },
        .{ .group_id = 2, .name = "restoring" },
    }) |owner| {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ root, owner.group_id });
        defer alloc.free(path);
        var lease = try source.acquireDescriptor(owner.group_id, owner.name, path, .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = owner.group_id, .shard_id = owner.group_id, .range_id = owner.group_id },
        });
        lease.deinit();
    }

    const limit = source.maintenanceEntryLimit(false).?;
    var cursor: usize = 0;
    var slow_maintenance = source.nextMaintenanceLease(&cursor, limit, false, true).?;
    defer slow_maintenance.deinit();
    try std.testing.expectEqual(@as(u64, 1), slow_maintenance.entry.group_id);

    var wait = PublicationWaitTest{};
    var vtable: std.Io.VTable = undefined;
    var publication = try source.snapshotSource().beginPublication(.{
        .io = wait.io(&vtable),
        .group_id = 2,
        .table_name = "restoring",
        .drain_timeout_ns = std.time.ns_per_ms,
    });
    defer publication.deinit();
    try std.testing.expectEqual(@as(usize, 0), wait.sleeps);
    try std.testing.expectEqual(@as(u64, 1), slow_maintenance.entry.group_id);
}

test "publication cancellation and timeout release admission without invalidating borrowers" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]bool{ false, true }) |cancel| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
        defer alloc.free(path);
        var source = Source.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        var borrower = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer borrower.deinit();
        var signal = std.atomic.Value(bool).init(false);
        var wait = PublicationWaitTest{ .cancel = if (cancel) &signal else null };
        var vtable: std.Io.VTable = undefined;
        try std.testing.expectError(if (cancel) error.Canceled else error.StorageBusy, source.snapshotSource().beginPublication(.{
            .io = wait.io(&vtable),
            .group_id = 1,
            .table_name = "docs",
            .cancellation = .fromAtomic(&signal),
            .drain_timeout_ns = std.time.ns_per_ms,
        }));
        try std.testing.expectEqual(@as(usize, 1), wait.sleeps);
        try std.testing.expectEqual(@as(usize, 0), source.publications.items.len);
        try std.testing.expectEqual(@as(usize, 1), source.ownerCountForTest());
        try std.testing.expectEqual(@as(usize, 1), borrower.entry.active_users);
        borrower.deinit();
        var replacement = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer replacement.deinit();
    }
}

test "workload admission recovery ACK deadline and abort token survive owner acquisition" {
    const alloc = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("owner-ack-deadline");
    defer directory.cleanup();
    const Catalog = struct {
        wait_until_ns: ?u64 = null,
        late_acquisitions: usize = 0,
        cancel_on_catalog: ?*std.atomic.Value(bool) = null,
        fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.cancel_on_catalog) |signal| signal.store(true, .release);
            if (self.wait_until_ns) |deadline| {
                self.late_acquisitions += 1;
                while (platform_time.monotonicNs() < deadline) platform_time.sleepNs(std.time.ns_per_ms);
            }
            const metadata = @import("../metadata/table_manager.zig");
            return .{
                .status = .{ .metadata_group_id = 9, .metadata_epoch = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata.TableRecord{.{ .table_id = 1, .name = "docs", .indexes_json = "{}" }})[0..]),
                .ranges = @constCast((&[_]metadata.RangeRecord{.{ .table_id = 1, .group_id = 2, .range_id = 3, .start_key = "", .doc_identity_shard_id = 2, .doc_identity_range_id = 3 }})[0..]),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn point(ptr: *anyopaque, _: []const u8, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const value = try snapshot(ptr);
            return .{ .metadata_group_id = value.status.metadata_group_id, .tables = value.tables, .ranges = value.ranges };
        }
        fn freePoint(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    };
    var catalog: Catalog = .{};
    var owner_source = ProvisionedKernelOwnerSource.init(alloc, std.mem.span(directory.path().ptr), .{
        .ptr = &catalog,
        .vtable = &.{ .admin_snapshot = Catalog.snapshot, .free_admin_snapshot = Catalog.free, .table_routing_snapshot = Catalog.point, .free_routing_snapshot = Catalog.freePoint },
    }, read_gate.alreadyReadSafeBarrier());
    defer owner_source.deinit();
    const writes = owner_source.writeSource();
    const txn_id: db_types.TxnId = @splat(0x72);
    const participant = "table2:00000004:docs:2";
    _ = try writes.txnBeginGroupLocal(alloc, 2, "docs", txn_id, 100, 1, true, &.{participant});
    _ = try writes.txnResolveGroupLocal(alloc, 2, "docs", txn_id, .committed, 200, 1, .propose);
    const deadline = platform_time.monotonicNs() + std.time.ns_per_s;
    catalog.wait_until_ns = deadline;
    // The public boundary starts before expiry, then real catalog acquisition
    // consumes the remaining budget. Without the final pre-C check this would
    // return InvalidParticipant, proving an ACK was invoked after its deadline.
    try std.testing.expectError(error.Timeout, writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, "not-enlisted", deadline));
    try std.testing.expectEqual(@as(usize, 1), catalog.late_acquisitions);
    catalog.wait_until_ns = null;
    const recovery_deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    try std.testing.expectError(error.InvalidParticipant, writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, "not-enlisted", recovery_deadline));
    // A new recovery window is independent from the expired window and the
    // original request. Accepted C errors/results pass through unchanged.
    _ = try writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, participant, recovery_deadline);
    _ = try writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, participant, recovery_deadline);

    const abort_id: db_types.TxnId = @splat(0x73);
    _ = try writes.txnBeginGroupLocal(alloc, 2, "docs", abort_id, 300, 1, true, &.{participant});
    var recovery_expired = std.atomic.Value(bool).init(false);
    catalog.cancel_on_catalog = &recovery_expired;
    try std.testing.expectError(error.Canceled, writes.txnResolveGroupLocalUntil(alloc, 2, "docs", abort_id, .aborted, 301, 1, .propose, platform_time.monotonicNs() + std.time.ns_per_s, .fromAtomic(&recovery_expired)));
    catalog.cancel_on_catalog = null;
    try std.testing.expect(recovery_expired.load(.acquire));
    try std.testing.expectEqual(db_types.TxnStatus.pending, (try writes.txnStatusGroupLocal(alloc, 2, "docs", abort_id)).?);
    _ = try writes.txnResolveGroupLocalUntil(alloc, 2, "docs", abort_id, .aborted, 301, 1, .propose, platform_time.monotonicNs() + std.time.ns_per_s, .none);
    try std.testing.expectEqual(db_types.TxnStatus.aborted, (try writes.txnStatusGroupLocal(alloc, 2, "docs", abort_id)).?);
}

test "workload admission recovery descriptor acquisition translates clocks and never falls back" {
    const alloc = std.testing.allocator;
    const ns = std.time.ns_per_s;
    var clock = PublicationWaitTest{ .now_ns = @intCast(platform_time.monotonicNs() + 1000 * ns) };
    var clock_vtable: std.Io.VTable = undefined;
    const io = clock.io(&clock_vtable);
    const Fixture = struct {
        last_deadline: ?u64 = null,
        point_calls: usize = 0,
        admin_calls: usize = 0,
        cancel_on_point: ?*std.atomic.Value(bool) = null,
        fn admin(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.admin_calls += 1;
            return error.UnexpectedAdminSnapshot;
        }
        fn freeAdmin(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn point(ptr: *anyopaque, _: []const u8, deadline: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.point_calls += 1;
            self.last_deadline = deadline;
            if (self.cancel_on_point) |signal| signal.store(true, .release);
            return .{ .tables = &.{}, .ranges = &.{} };
        }
        fn freePoint(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    };
    var fixture: Fixture = .{};
    var source = ProvisionedKernelOwnerSource.init(alloc, "unused", .{
        .ptr = &fixture,
        .io = @import("../runtime_io_abi.zig").Borrow.init(&io),
        .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.freeAdmin, .table_routing_snapshot = Fixture.point, .free_routing_snapshot = Fixture.freePoint },
    }, read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    const writes = source.writeSource();
    const txn_id: db_types.TxnId = @splat(0x79);
    const native_deadline = platform_time.monotonicNs() + 5 * ns;
    const catalog_now = source.catalog.budget(null).nowNs();
    try std.testing.expectError(error.CommitPropagationIncomplete, writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, "table2:00000004:docs:2", native_deadline));
    try std.testing.expect(fixture.last_deadline.? > catalog_now);
    try std.testing.expect(fixture.last_deadline.? <= catalog_now + 5 * ns);
    try std.testing.expectEqual(@as(usize, 0), fixture.admin_calls);
    try std.testing.expect(!source.hasPendingRecoveryOwners());
    try std.testing.expectError(error.Timeout, writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, "table2:00000004:docs:2", 0));
    try std.testing.expectEqual(@as(usize, 1), fixture.point_calls);
    var signal = std.atomic.Value(bool).init(false);
    fixture.cancel_on_point = &signal;
    try std.testing.expectError(error.Canceled, writes.txnResolveGroupLocalUntil(alloc, 2, "docs", txn_id, .aborted, 10, 1, .propose, native_deadline, .fromAtomic(&signal)));
    try std.testing.expectEqual(@as(usize, 2), fixture.point_calls);
    try std.testing.expect(!source.hasPendingRecoveryOwners());
    // A rolling peer without point projection cannot silently select the
    // unbounded administrative projection, even when it can serve one.
    source.catalog.vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.freeAdmin };
    try std.testing.expectError(error.CommitPropagationIncomplete, writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, "table2:00000004:docs:2", native_deadline));
    try std.testing.expectEqual(@as(usize, 0), fixture.admin_calls);
    try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
}

test "workload admission recovery owner lifecycle opens cold debt independently and retries fairly" {
    const alloc = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("recovery-owner-lifecycle");
    defer directory.cleanup();
    const Catalog = struct {
        point_calls: usize = 0,
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedAdminSnapshot;
        }
        fn freeAdmin(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn point(ptr: *anyopaque, name: []const u8, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.point_calls += 1;
            if (!std.mem.eql(u8, name, "docs")) return error.CatalogRoutingUnavailable;
            const metadata = @import("../metadata/table_manager.zig");
            return .{
                .metadata_group_id = 9,
                .tables = @constCast((&[_]metadata.TableRecord{.{ .table_id = 1, .name = "docs", .indexes_json = "{}" }})[0..]),
                .ranges = @constCast((&[_]metadata.RangeRecord{.{ .table_id = 1, .group_id = 2, .range_id = 3, .start_key = "", .doc_identity_shard_id = 2, .doc_identity_range_id = 3 }})[0..]),
            };
        }
        fn freePoint(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
        fn source(self: *@This()) table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{ .admin_snapshot = admin, .free_admin_snapshot = freeAdmin, .table_routing_snapshot = point, .free_routing_snapshot = freePoint } };
        }
    };
    var catalog: Catalog = .{};
    const root = std.mem.span(directory.path().ptr);
    const txn_id: db_types.TxnId = @splat(0x7a);
    const participant = "table2:00000004:docs:2";
    {
        var original = ProvisionedKernelOwnerSource.init(alloc, root, catalog.source(), read_gate.alreadyReadSafeBarrier());
        defer original.deinit();
        const writes = original.writeSource();
        _ = try writes.txnBeginGroupLocal(alloc, 2, "docs", txn_id, 100, 1, true, &.{participant});
        _ = try writes.txnResolveGroupLocal(alloc, 2, "docs", txn_id, .committed, 200, 1, .propose);
    }
    var restored = ProvisionedKernelOwnerSource.init(alloc, root, catalog.source(), read_gate.alreadyReadSafeBarrier());
    defer restored.deinit();
    const writes = restored.writeSource();
    // One unavailable group precedes the real debt, exercising cursor fairness.
    try restored.requestRecoveryOwner(9, "unavailable");
    var request_name = [_]u8{ 'd', 'o', 'c', 's' };
    try std.testing.expectError(error.CommitPropagationIncomplete, writes.txnStatusGroupLocalUntil(alloc, 2, &request_name, txn_id, platform_time.monotonicNs() + std.time.ns_per_s));
    for (0..2) |_| {
        try std.testing.expectError(error.CommitPropagationIncomplete, writes.txnAcknowledgeGroupLocalUntil(alloc, 2, &request_name, txn_id, participant, platform_time.monotonicNs() + std.time.ns_per_s));
    }
    request_name[0] = 'X';
    try std.testing.expectEqual(@as(usize, 2), restored.recovery_owner_pending.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), restored.ownerCountForTest());
    try std.testing.expectError(error.CatalogRoutingUnavailable, restored.drainRecoveryOwnerRequest());
    try std.testing.expectEqual(@as(usize, 0), restored.ownerCountForTest());
    // DATA's lifecycle lane invokes this method after the request has returned.
    try restored.drainRecoveryOwnerRequest();
    try std.testing.expectEqual(@as(usize, 1), restored.ownerCountForTest());
    try std.testing.expectEqual(@as(usize, 1), restored.recovery_owner_pending.load(.acquire));
    try std.testing.expectEqual(db_types.TxnStatus.committed, (try writes.txnStatusGroupLocalUntil(alloc, 2, "docs", txn_id, platform_time.monotonicNs() + std.time.ns_per_s)).?);
    _ = try writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, participant, platform_time.monotonicNs() + std.time.ns_per_s);
    _ = try writes.txnAcknowledgeGroupLocalUntil(alloc, 2, "docs", txn_id, participant, platform_time.monotonicNs() + std.time.ns_per_s);
}

test "workload admission recovery owner handoff bounds admission and closes with lifecycle" {
    var source = ProvisionedKernelOwnerSource.init(std.testing.failing_allocator, "unused", undefined, undefined);
    defer source.deinit();
    // Queueing owns its strings without consulting the normal allocator.
    var name = [_]u8{ 'd', 'o', 'c', 's' };
    for (0..source.recovery_owner_requests.len) |index| try source.requestRecoveryOwner(index + 1, &name);
    name[0] = 'X';
    try std.testing.expectEqualStrings("docs", source.recovery_owner_requests[0].name());
    try source.requestRecoveryOwner(1, "docs");
    try std.testing.expectEqual(source.recovery_owner_requests.len, source.recovery_owner_pending.load(.acquire));
    try std.testing.expectError(error.CommitPropagationIncomplete, source.requestRecoveryOwner(999, "docs"));
    try std.testing.expect(source.recovery_owner_mutex.tryLock());
    try std.testing.expectError(error.CommitPropagationIncomplete, source.requestRecoveryOwner(1, "docs"));
    source.recovery_owner_mutex.unlock();
    try source.quiesce(std.testing.io);
    try std.testing.expect(!source.hasPendingRecoveryOwners());
    try std.testing.expectError(error.Canceled, source.requestRecoveryOwner(1, "docs"));
    // No catalog or native owner call occurs once shutdown closes admission.
    try source.drainRecoveryOwnerRequest();
}

test "workload admission provisioned routed reads translate fence clock domains" {
    const ns = std.time.ns_per_s;
    var clock = PublicationWaitTest{ .now_ns = @intCast(@import("antfly_platform").time.monotonicNs() + 1000 * ns) };
    var clock_vtable: std.Io.VTable = undefined;
    const io = clock.io(&clock_vtable);
    const Fixture = struct {
        io: std.Io,
        last_deadline: ?u64 = null,
        fence: metadata_api.CatalogRouteFence = .{
            .metadata_group_id = 1,
            .catalog_revision = 1,
            .table_id = 1,
            .topology_epoch = 1,
            .route = .{ .group_id = 2, .range_id = 2, .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 } },
        },
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedAdminSnapshot;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: table_catalog.RouteQuery, deadline: ?u64) !table_catalog.RouteResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_deadline = deadline;
            try table_catalog.RoutingBudget.initIo(deadline, self.io).checkpoint();
            const groups = try alloc.dupe(table_catalog.CatalogGroupRoute, &.{self.fence.route});
            return .{ .found = .{
                .metadata_group_id = self.fence.metadata_group_id,
                .metadata_incarnation = self.fence.metadata_incarnation,
                .catalog_revision = self.fence.catalog_revision,
                .table_id = self.fence.table_id,
                .topology_epoch = self.fence.topology_epoch,
                .groups = groups,
            } };
        }
    };
    var fixture = Fixture{ .io = io };
    const catalog = table_catalog.CatalogSource{
        .ptr = &fixture,
        .io = @import("../runtime_io_abi.zig").Borrow.init(&io),
        .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free, .validate_route = Fixture.resolve },
    };
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "unused", catalog, read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    var fence = fixture.fence;
    const routing_now = catalog.budget(null).nowNs();
    fence.admission_deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * ns;
    // The actual worker path receives a native deadline from authenticated
    // remaining time, but its RemoteMetadataSource owns an Io routing clock.
    try source.validateRoutedRead(std.testing.allocator, fence, 2, "docs");
    try std.testing.expect(fixture.last_deadline.? > routing_now);
    try std.testing.expect(fixture.last_deadline.? <= routing_now + 5 * ns);
    fence.admission_deadline_ns = 0;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, source.validateRoutedRead(std.testing.allocator, fence, 2, "docs"));
    try std.testing.expectEqual(routing_now, fixture.last_deadline.?);
    fence.admission_deadline_io = catalog.io;
    fence.admission_deadline_ns = routing_now + ns;
    try source.validateRoutedRead(std.testing.allocator, fence, 2, "docs");
    try std.testing.expectEqual(fence.admission_deadline_ns, fixture.last_deadline);
    fence.admission_deadline_ns = null;
    try source.validateRoutedRead(std.testing.allocator, fence, 2, "docs");
    try std.testing.expect(fixture.last_deadline == null);
}

test "workload admission completion installation registry never downgrades failed or missing backing" {
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "/unused", undefined, undefined);
    defer source.completion_installations.deinit(std.testing.allocator);
    try std.testing.expectError(error.NotFound, source.installedCompletionOwner(7));
    var record: ProvisionedKernelOwnerSource.CompletionInstallation = .{
        .binding = .{},
        .read_schema_json = &.{},
        .settings_json = &.{},
        .settings = .{},
    };
    try source.completion_installations.put(std.testing.allocator, 7, &record);
    try std.testing.expectError(error.CompletionAdmissionUnavailable, source.installedCompletionOwner(7));
    record.state = .failed;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, source.installedCompletionOwner(7));
    record.state = .backed;
    record.active = true;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, source.installedCompletionOwner(7));
    source.quiescing = true;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, source.installedCompletionOwner(8));
}

test "workload admission completion installation barrier consumes reserved capacity without allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var source = ProvisionedKernelOwnerSource.init(failing.allocator(), "/unused", undefined, undefined);
    defer source.completion_installations.deinit(failing.allocator());
    try source.completion_installations.ensureUnusedCapacity(failing.allocator(), 2);
    source.completion_installation_preparations = 2;
    var first: ProvisionedKernelOwnerSource.CompletionInstallation = .{
        .binding = .{ .identity = .{ .group_id = 7 } },
        .read_schema_json = &.{},
        .settings_json = &.{},
        .settings = .{},
    };
    var second = first;
    second.binding.identity.group_id = 8;
    var first_prepared: ProvisionedKernelOwnerSource.PreparedCompletionInstallation = .{
        .source = &source,
        .alloc = failing.allocator(),
        .descriptor = undefined,
        .record = &first,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "",
    };
    var second_prepared = first_prepared;
    second_prepared.record = &second;
    failing.fail_index = failing.alloc_index;
    try first_prepared.begin();
    try second_prepared.begin();
    try std.testing.expectEqual(@as(usize, 0), source.completion_installation_preparations);
    try std.testing.expectEqual(@as(u32, 2), source.completion_installations.count());
    try std.testing.expectError(error.CompletionAdmissionUnavailable, source.installedCompletionOwner(7));
    first.state = .failed;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, source.installedCompletionOwner(7));
    // A second preparation cannot clear the failed barrier or consume its own
    // outstanding reservation until the caller explicitly cancels it.
    var duplicate = first;
    duplicate.state = .installing;
    var duplicate_prepared = first_prepared;
    duplicate_prepared.record = &duplicate;
    duplicate_prepared.registered = false;
    duplicate_prepared.capacity_reserved = true;
    source.completion_installation_preparations = 1;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, duplicate_prepared.begin());
    try std.testing.expectEqual(@as(usize, 1), source.completion_installation_preparations);
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index - failing.fail_index);
}

test "workload admission fresh completion installation rejection preserves legacy admission" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var source = ProvisionedKernelOwnerSource.init(failing.allocator(), "/unused", undefined, undefined);
    try std.testing.expectError(error.CompletionAdmissionUnavailable, source.prepareCompletionInstallation(failing.allocator(), 7, "docs", .{ .identity = .{ .group_id = 7 } }, "", "", "{}", .{}, false, false));
    try std.testing.expectEqual(@as(u32, 0), source.completion_installations.count());
    try std.testing.expectEqual(@as(usize, 0), source.completion_installation_preparations);
    try std.testing.expectError(error.NotFound, source.installedCompletionOwner(7));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}

test "workload admission completion installation pins owner through ordinary retirement" {
    const alloc = std.testing.allocator;
    var source = ProvisionedKernelOwnerSource.init(alloc, "/unused", undefined, undefined);
    defer source.entries.deinit(alloc);
    defer source.completion_installations.deinit(alloc);
    var record: ProvisionedKernelOwnerSource.CompletionInstallation = .{ .binding = .{}, .read_schema_json = &.{}, .settings_json = &.{}, .settings = .{}, .state = .backed, .active = true };
    try source.completion_installations.put(alloc, 7, &record);
    var entry: ProvisionedKernelOwnerSource.Entry = .{ .group_id = 7, .table_name = @constCast("docs"), .generation = 1, .identity = .{ .table_id = 1, .range_id = 2, .shard_id = 7 }, .schema_json = &.{}, .indexes_json = &.{}, .owner = undefined };
    try source.entries.append(alloc, &entry);
    try std.testing.expectEqual(@as(usize, 0), source.retireAll());
    try std.testing.expectEqual(@as(usize, 0), source.retireTable("docs"));
    try std.testing.expect(!entry.retired);
    try std.testing.expectError(error.PreparedCompletionActive, source.registerPublication(7, "docs"));
    try std.testing.expectError(error.PreparedCompletionActive, source.acquireDescriptorOnce(7, "docs", "/unused", .{ .lsm_root_generation = 2, .identity = entry.identity }, .shared, .resident, .{}));
    try std.testing.expectEqual(@as(usize, 1), source.entries.items.len);
    source.quiescing = true;
    try std.testing.expect(!source.installationPinsOwnerLocked(7));
}

test "workload admission restored owner callback snapshot waits for one-time runtime publication" {
    const Fixture = struct {
        calls: usize = 0,
        fn get(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return try alloc.dupe(u8, "entity");
        }
        fn scan(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: runtime_callbacks.CandidateSource.ScanOptions, ctx: *anyopaque, consume_fn: runtime_callbacks.CandidateSource.Consume) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try consume_fn(ctx, "key", "entity");
        }
        fn nearest(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, _: runtime_callbacks.CandidateSource.NearestQuery, ctx: *anyopaque, consume_fn: runtime_callbacks.CandidateSource.Consume) !void {
            try scan(ptr, alloc, table, "", .{}, ctx, consume_fn);
        }
        fn upsert(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
        fn leader(ptr: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return true;
        }
        fn consume(_: ?*anyopaque, _: abi.BorrowedBytes, value: abi.BorrowedBytes) callconv(.c) abi.Status {
            return if (std.mem.eql(u8, value.slice(), "entity")) .ok else .internal;
        }
    };
    var fixture = Fixture{};
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.unavailableReadSafetyBarrier());
    defer source.deinit();
    _ = source.withDeferredRuntimeHooks();
    // A compiled owner retains this value before DATA/API callback targets exist.
    const retained = source.runtimeHooksConfig();
    const candidates = retained.resolution_candidates;
    const sink = retained.entity_sink;
    const entries = [_]abi.EntityUpsert{.{ .table = .fromSlice("t"), .key = .fromSlice("k"), .doc_json = .fromSlice("{}") }};
    try std.testing.expectEqual(abi.Status.completion_admission_unavailable, candidates.get_fn.?(candidates.callback_ctx, .{}, .{}, null, Fixture.consume));
    try std.testing.expectEqual(abi.Status.completion_admission_unavailable, candidates.scan_prefix_fn.?(candidates.callback_ctx, .{}, .{}, 1, null, Fixture.consume));
    try std.testing.expectEqual(abi.Status.completion_admission_unavailable, candidates.nearest_fn.?(candidates.callback_ctx, .{}, .{}, null, 0, 1, null, Fixture.consume));
    try std.testing.expectEqual(abi.Status.completion_admission_unavailable, sink.upsert_fn.?(sink.callback_ctx, .{}, .{}, .{}));
    try std.testing.expectEqual(abi.Status.completion_admission_unavailable, sink.upsert_batch_fn.?(sink.callback_ctx, &entries, entries.len));
    try std.testing.expectEqual(@as(u8, 0), retained.promotion_owner_fn.?(retained.promotion_owner_ctx, 7));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    const candidate_source: runtime_callbacks.CandidateSource = .{ .ptr = &fixture, .vtable = &.{ .get = Fixture.get, .scan_prefix = Fixture.scan, .nearest = Fixture.nearest } };
    const entity_sink: runtime_callbacks.EntitySink = .{ .ptr = &fixture, .vtable = &.{ .upsert = Fixture.upsert } };
    const leadership: table_writes.PromotionLeadershipSource = .{ .ptr = &fixture, .vtable = &.{ .is_local_leader = Fixture.leader } };
    _ = try source.withRuntimeHooks(candidate_source, entity_sink, leadership);
    _ = try source.withRuntimeHooks(candidate_source, entity_sink, leadership);
    try std.testing.expectError(error.PreparedCompletionActive, source.withRuntimeHooks(null, null, null));
    try std.testing.expectEqual(abi.Status.ok, candidates.get_fn.?(candidates.callback_ctx, .{}, .{}, null, Fixture.consume));
    try std.testing.expectEqual(abi.Status.ok, candidates.scan_prefix_fn.?(candidates.callback_ctx, .{}, .{}, 1, null, Fixture.consume));
    try std.testing.expectEqual(abi.Status.ok, candidates.nearest_fn.?(candidates.callback_ctx, .{}, .{}, null, 0, 1, null, Fixture.consume));
    try std.testing.expectEqual(abi.Status.ok, sink.upsert_fn.?(sink.callback_ctx, .{}, .{}, .{}));
    try std.testing.expectEqual(abi.Status.ok, sink.upsert_batch_fn.?(sink.callback_ctx, &entries, entries.len));
    try std.testing.expectEqual(@as(u8, 1), retained.promotion_owner_fn.?(retained.promotion_owner_ctx, 7));
    try std.testing.expectEqual(@as(usize, 6), fixture.calls);
}

test "workload admission completion capsule restores a real compiled owner without catalog or new admission" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const relative_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(relative_root);
    const root = try std.Io.Dir.cwd().realPathFileAlloc(io, relative_root, alloc);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-2/table-db", .{root});
    defer alloc.free(path);
    const settings: @import("../common/table_storage.zig").Settings = .{ .transaction_recovery = .{
        .protocol_version = 1,
        .max_count = 4,
        .max_bytes = 1024 * 1024,
        .max_transaction_bytes = 64 * 1024,
        .completion_protocol_version = 1,
        .profile_version = 1,
    } };
    var binding: abi.completion_pool.InstallBinding = .{
        .identity = .{ .group_id = 2, .node_id = 7, .capacity = 4, .generation = 1, .incarnation = @splat(15), .policy_digest = @import("../metadata/completion_activation.zig").policyDigest(settings.transaction_recovery.?) },
        .table_id = 1,
        .range_id = 3,
    };
    binding.schema_catalog_digest = try @import("../common/completion_catalog_digest.zig").digest(alloc, "", "", "{}");
    var canonical: ?[]u8 = null;
    defer if (canonical) |bytes| alloc.free(bytes);
    var accepted_identity: abi.completion_pool.Progress = undefined;
    {
        var context: client.Context = .{};
        defer context.deinit();
        try context.ensureWith(.{ .transaction_completion_bytes = 1024 * 1024, .durable_completion_enabled = 1 });
        var source = Source.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.unavailableReadSafetyBarrier());
        defer source.deinit();
        _ = source.withStorageContextHandle(context.handle);
        // Model the authenticated installer's already validated descriptor.
        // The production helper obtains this from the coherent signed catalog.
        const descriptor: Source.LoadedDescriptor = .{
            .path = try alloc.dupe(u8, path),
            .schema_json = try alloc.dupe(u8, ""),
            .indexes_json = try alloc.dupe(u8, "{}"),
            .generation = table_reads.backend_current_root_generation,
            .table_storage = settings,
            .identity = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        };
        var prepared = try source.prepareCompletionDescriptor(alloc, descriptor, "docs", binding, "", "", "{}", settings, true, true);
        defer prepared.deinit();
        prepared.filesystem_io = io;
        try prepared.begin();
        var hidden: abi.completion_pool.Lease = undefined;
        const provider = source.completionProvider();
        try std.testing.expectEqual(abi.Status.completion_admission_unavailable, provider.acquire(provider.context, 2, 7, &hidden));
        try prepared.finish();
        var capsule = (try completion_capsule.load(alloc, io, path)).?;
        defer capsule.deinit();
        try std.testing.expectEqualDeep(binding, capsule.value.binding);
        try std.testing.expect(source.completionAdmissionAuthorized(2));
        try std.testing.expectEqual(abi.Status.ok, provider.acquire(provider.context, 2, 7, &hidden));
        defer hidden.vtable.release(hidden.context);
        var owner_lease = try source.installedCompletionOwner(2);
        defer owner_lease.deinit();
        var compiled = try owner_lease.owner().compileReplicatedCompletion("docs",
            \\{"inserts":{"doc":{"value":2}},"_timestamp_ns":"100","sync_level":"write"}
        , 0, 0);
        defer compiled.deinit();
        canonical = try alloc.dupe(u8, compiled.bytes());
        const payloads = [_]abi.completion_pool.Bytes{.{ .ptr = canonical.?.ptr, .len = canonical.?.len }};
        const proposal: abi.completion_pool.Check = .{ .kind = .proposal, .new_work_allowed = 1, .state = .{ .term = 1, .applied_term_known = 1 }, .proposals = .{ .ptr = &payloads, .len = 1 } };
        var checked: abi.completion_pool.CheckResult = undefined;
        try std.testing.expectEqual(abi.Status.ok, hidden.vtable.check(hidden.context, &proposal, &checked));
        hidden.vtable.proposal_result(hidden.context, &.{ .state = proposal.state, .first_index = 1, .last_index = 1, .payloads = proposal.proposals });
        var pending: abi.completion_pool.DurableCells = .{};
        try std.testing.expectEqual(abi.Status.ok, hidden.vtable.durable_cells.?(hidden.context, &pending));
        try std.testing.expectEqual(@as(u32, 1), pending.count);
        try std.testing.expectEqual(@as(u8, 0), pending.cells[0].prepared);
        accepted_identity = pending.cells[0].identity;
        try std.testing.expectError(error.NotFound, owner_lease.owner().lookupJson("docs", "{\"key\":\"doc\",\"include_all_fields\":true}"));
    }
    // Close and recreate both the registry and compiled context. No service
    // keys, cached catalog, HTTP executor or external metadata source exists.
    var context: client.Context = .{};
    defer context.deinit();
    try context.ensureWith(.{ .transaction_completion_bytes = 1024 * 1024, .durable_completion_enabled = 0 });
    var source = Source.init(alloc, relative_root, table_catalog.emptyCatalogSource(), read_gate.unavailableReadSafetyBarrier());
    defer source.deinit();
    _ = source.withStorageContextHandle(context.handle);
    var capsule = (try completion_capsule.load(alloc, io, path)).?;
    defer capsule.deinit();
    var prepared = try source.prepareCompletionRestoration(alloc, io, capsule.value);
    defer prepared.deinit();
    try prepared.begin();
    try prepared.finish();
    try std.testing.expect(!source.completionAdmissionAuthorized(2));
    try std.testing.expect(!source.completionInstallationAuthorized(2));
    const provider = source.completionProvider();
    var lease: abi.completion_pool.Lease = undefined;
    try std.testing.expectEqual(abi.Status.ok, provider.acquire(provider.context, 2, 7, &lease));
    defer lease.vtable.release(lease.context);
    var cells: abi.completion_pool.DurableCells = .{};
    try std.testing.expectEqual(abi.Status.ok, lease.vtable.durable_cells.?(lease.context, &cells));
    try std.testing.expectEqual(@as(u32, 1), cells.count);
    try std.testing.expectEqualDeep(accepted_identity, cells.cells[0].identity);
    try std.testing.expectEqual(@as(u8, 0), cells.cells[0].prepared);
    var durable_log: abi.completion_pool.DurableLog = .{ .mode = .startup_complete, .last_index = 1, .commit_index = 1, .count = 1 };
    durable_log.observations[0] = .{ .expected = accepted_identity, .observed_term = 1, .observed_digest = accepted_identity.payload_digest, .present = 1 };
    try std.testing.expectEqual(abi.Status.ok, lease.vtable.reconcile_durable.?(lease.context, &durable_log));
    // Replay uses the restored ownership with new admission disabled. The
    // authoritative log observation above certifies the exact retained entry.
    try std.testing.expectEqual(abi.Status.ok, lease.vtable.apply_accepted.?(lease.context, 1, 1, .{ .ptr = canonical.?.ptr, .len = canonical.?.len }));
    try std.testing.expectEqual(abi.Status.ok, lease.vtable.apply_accepted.?(lease.context, 1, 1, .{ .ptr = canonical.?.ptr, .len = canonical.?.len }));
    var owner_lease = try source.installedCompletionOwner(2);
    defer owner_lease.deinit();
    var document = try owner_lease.owner().lookupJson("docs", "{\"key\":\"doc\",\"include_all_fields\":true}");
    defer document.deinit();
    try std.testing.expect(std.mem.indexOf(u8, document.bytes(), "\"value\":2") != null);
    var progress: abi.completion_pool.Progress = undefined;
    try std.testing.expectEqual(abi.Status.ok, lease.vtable.progress.?(lease.context, &progress));
    try std.testing.expectEqualDeep(accepted_identity, progress);
    var evidence: abi.completion_pool.NativeAttestation = .{};
    try std.testing.expectEqual(abi.Status.completion_admission_unavailable, provider.attest.?(provider.context, 2, 7, &evidence));
    try std.testing.expectError(error.TableNotFound, source.loadDescriptor(alloc, 2, "docs"));
    // A copied capsule cannot authorize a different root or native identity.
    var changed = capsule.value;
    changed.root_identity_digest[0] ^= 1;
    try std.testing.expectError(error.CompletionProfileChanged, source.prepareCompletionRestoration(alloc, io, changed));
}

test "workload admission completion capsule reconciliation cannot erase an identity fence" {
    const alloc = std.testing.allocator;
    var source = ProvisionedKernelOwnerSource.init(alloc, "/unused", undefined, undefined);
    defer source.completion_installations.deinit(alloc);
    var installed: ProvisionedKernelOwnerSource.CompletionInstallation = .{
        .binding = .{ .identity = .{ .group_id = 7 } },
        .read_schema_json = &.{},
        .settings_json = &.{},
        .settings = .{},
        .state = .backed,
    };
    try source.completion_installations.put(alloc, 7, &installed);
    try std.testing.expect(source.completionInstallationPresent(7));
    try std.testing.expect(!source.completionInstallationPresent(8));
    try std.testing.expect(source.completionBackingIdentityMatches(installed.binding.identity));
    var wrong_identity = installed.binding.identity;
    wrong_identity.generation += 1;
    try std.testing.expect(!source.completionBackingIdentityMatches(wrong_identity));
    ProvisionedKernelOwnerSource.lock(&source.mutex);
    try std.testing.expect(source.completionInstallationPresent(8));
    source.mutex.unlock();
    try std.testing.expect(!source.completionAdmissionAuthorized(7));
    var authenticated = installed;
    authenticated.metadata_authorized = true;
    var prepared: ProvisionedKernelOwnerSource.PreparedCompletionInstallation = .{
        .source = &source,
        .alloc = alloc,
        .descriptor = undefined,
        .record = &authenticated,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
    };
    source.completion_installation_preparations = 1;
    try prepared.begin();
    try std.testing.expect(source.completionInstallationAuthorized(7));
    try std.testing.expect(!source.completionAdmissionAuthorized(7)); // pending may attest but not admit
    authenticated.active = true;
    prepared.capacity_reserved = true;
    source.completion_installation_preparations = 1;
    try prepared.begin();
    try std.testing.expect(source.completionAdmissionAuthorized(7));
    source.fenceCompletionAuthorization(7);
    try std.testing.expect(!source.completionAdmissionAuthorized(7));
    try std.testing.expect(!source.completionInstallationAuthorized(7));
    prepared.capacity_reserved = true;
    source.completion_installation_preparations = 1;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, prepared.begin());
    try std.testing.expectEqual(@as(usize, 1), source.completion_installation_preparations);
    try std.testing.expect(installed.state == .backed);
}
