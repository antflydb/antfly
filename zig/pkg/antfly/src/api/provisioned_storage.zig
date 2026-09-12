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
const platform_sync = @import("antfly_platform").sync;
const memory_budget = @import("../storage/memory_budget.zig");
const hbc_mod = @import("../storage/hbc_adapter.zig");
const runtime_callbacks = @import("../storage/db/runtime_callbacks.zig");
const background_runtime_mod = @import("../storage/background_runtime.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const filesystem_capacity = @import("../storage/filesystem_capacity.zig");
const runtime_status = @import("runtime_status.zig");
const scraping = @import("antfly_scraping");
const table_catalog = @import("table_catalog.zig");
const table_reads = @import("antfly_source_root").antfly_sources.table_reads;
const table_writes = @import("antfly_source_root").antfly_sources.table_writes;
const storage_source_options = @import("storage_source_options");

const linked_storage = storage_source_options.control_only;

const ProvisionedLsmCache = if (linked_storage) void else lsm_backend.Cache;
const ProvisionedHbcCache = if (linked_storage) void else hbc_mod.Cache;
const ProvisionedReadCache = if (linked_storage) void else table_reads.ProvisionedTableReadCache;
const ProvisionedWriteCache = if (linked_storage) void else table_writes.ProvisionedTableWriteCache;

const MiB = memory_budget.MiB;
const GiB = memory_budget.GiB;
const MinSmartLsmCacheBytes = memory_budget.MinSmartLsmCacheBytes;
// Primary values and decoded run indexes are the authoritative miss path for
// HBC. Give their reclaimable cache an elastic ceiling instead of imposing a
// fixed corpus-size cliff; the aggregate ResourceManager ledger and synchronous
// reclaim still preserve room for foreground work and the process reserve.
const MaxSmartLsmCacheBytes = memory_budget.MaxSmartLsmCacheBytes;
const MinSmartLsmCompactionBytes = memory_budget.MinSmartLsmCompactionBytes;
const MaxSmartLsmCompactionBytes = memory_budget.MaxSmartLsmCompactionBytes;
const MinSmartLsmTableBuilderBytes = memory_budget.MinSmartLsmTableBuilderBytes;
const MaxSmartLsmTableBuilderBytes = memory_budget.MaxSmartLsmTableBuilderBytes;
const MinSmartLsmInMemoryStateBytes = memory_budget.MinSmartLsmInMemoryStateBytes;
// Allocator-backed LSM state has substantial process-footprint amplification
// while immutable tables are being encoded and published. Do not scale this
// slice to multi-gigabyte queues on large hosts; local backend limits provide
// fairness, while this remains the aggregate process admission ceiling.
const MaxSmartLsmInMemoryStateBytes = memory_budget.MaxSmartLsmInMemoryStateBytes;
const MinSmartHbcCacheBytes = memory_budget.MinSmartHbcCacheBytes;
// HBC exact vectors are derivative, reclaimable copies of LSM-owned values.
// A fixed 2 GiB ceiling produces a deterministic corpus-size cliff for 768-D
// vectors, so this cache is allowed to consume an elastic share of the node
// envelope while the aggregate ResourceManager budget remains authoritative.
const MaxSmartHbcCacheBytes = memory_budget.MaxSmartHbcCacheBytes;
const MinSmartDenseApplyBytes = memory_budget.MinSmartDenseApplyBytes;
const MaxSmartDenseApplyBytes = memory_budget.MaxSmartDenseApplyBytes;
const MinSmartReplayWindowBytes = memory_budget.MinSmartReplayWindowBytes;
const MaxSmartReplayWindowBytes = memory_budget.MaxSmartReplayWindowBytes;
const MinSmartFullTextPendingBytes = memory_budget.MinSmartFullTextPendingBytes;
const MaxSmartFullTextPendingBytes = memory_budget.MaxSmartFullTextPendingBytes;
const MinSmartFullTextBuildBytes = memory_budget.MinSmartFullTextBuildBytes;
const MaxSmartFullTextBuildBytes = memory_budget.MaxSmartFullTextBuildBytes;
const MinSmartFullTextResidencyBytes = memory_budget.MinSmartFullTextResidencyBytes;
const MaxSmartFullTextResidencyBytes = memory_budget.MaxSmartFullTextResidencyBytes;
const MinSmartDerivedBacklogBytes = memory_budget.MinSmartDerivedBacklogBytes;
const MaxSmartDerivedBacklogBytes = memory_budget.MaxSmartDerivedBacklogBytes;
const MinSmartTextMergeBytes = memory_budget.MinSmartTextMergeBytes;
const MaxSmartTextMergeBytes = memory_budget.MaxSmartTextMergeBytes;
const MinSmartAlgebraicTensorBytes = memory_budget.MinSmartAlgebraicTensorBytes;
const MaxSmartAlgebraicTensorBytes = memory_budget.MaxSmartAlgebraicTensorBytes;
const MinSmartDenseRepairBytes = memory_budget.MinSmartDenseRepairBytes;
const MaxSmartDenseRepairBytes = memory_budget.MaxSmartDenseRepairBytes;
const MinSmartShardTransitionBytes = memory_budget.MinSmartShardTransitionBytes;
const MaxSmartShardTransitionBytes = memory_budget.MaxSmartShardTransitionBytes;
const MinSmartVectorBlockBuildBytes = memory_budget.MinSmartVectorBlockBuildBytes;
const MaxSmartVectorBlockBuildBytes = memory_budget.MaxSmartVectorBlockBuildBytes;

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

pub const MemoryLimitSource = memory_budget.MemoryLimitSource;

const DetectedMemoryLimit = memory_budget.DetectedMemoryLimit;

const resolveEffectiveMemoryLimit = memory_budget.resolveEffectiveMemoryLimit;

const detectedMemoryLimit = memory_budget.detectedMemoryLimit;

const adaptiveSliceHardLimit = memory_budget.adaptiveSliceHardLimit;

const clampU64ToUsize = memory_budget.clampU64ToUsize;

const resourceBudget = memory_budget.resourceBudget;

const elasticCacheBudget = memory_budget.elasticCacheBudget;

const SmartResourceBudgets = memory_budget.SmartResourceBudgets;

const smartResourceBudgets = memory_budget.smartResourceBudgets;

const smartResourceBudgetsResolved = memory_budget.smartResourceBudgetsResolved;

const safeManagedHostMemory = memory_budget.safeManagedHostMemory;

const smartResourceBudgetsForTotal = memory_budget.smartResourceBudgetsForTotal;

pub const ProvisionedGroupStorage = struct {
    const VisibleRootGeneration = struct {
        generation: u64 = table_reads.backend_current_root_generation,
        reservations: usize = 0,
    };

    alloc: std.mem.Allocator,
    group_visible_root_generation_mutex: std.atomic.Mutex = .unlocked,
    group_visible_root_generations: std.AutoHashMapUnmanaged(u64, VisibleRootGeneration) = .empty,
    resource_manager: resource_manager_mod.ResourceManager,
    filesystem_capacity_probe: ?filesystem_capacity.Probe = null,
    lsm_cache: ProvisionedLsmCache,
    hbc_cache: ProvisionedHbcCache,
    runtime_status_cache: runtime_status.TableRuntimeSnapshotCache,
    read_cache: ProvisionedReadCache,
    write_cache_state_mutex: std.atomic.Mutex = .unlocked,
    write_cache: ProvisionedWriteCache,
    startup_write_cache: ProvisionedWriteCache,
    backend_runtime: ?*background_runtime_mod.BackendRuntime = null,
    effective_memory_limit_bytes: u64 = 0,
    memory_limit_source: MemoryLimitSource = .unavailable,
    /// Closed by default for provisioned databases. Metadata opens this only
    /// after the complete table-serving store set advertises the native HBC
    /// protocol, so a rolling old binary can never be handed native authority.
    dense_native_authority_permitted: std.atomic.Value(bool) = .init(false),

    pub fn init(alloc: std.mem.Allocator) ProvisionedGroupStorage {
        return initWithProcessMemoryLimit(alloc, 0);
    }

    pub fn initWithProcessMemoryLimit(
        alloc: std.mem.Allocator,
        process_memory_limit_bytes: usize,
    ) ProvisionedGroupStorage {
        return initWithProcessMemoryPolicy(alloc, process_memory_limit_bytes, null);
    }

    pub fn initWithProcessMemoryPolicy(
        alloc: std.mem.Allocator,
        process_memory_limit_bytes: usize,
        resolved_source: ?MemoryLimitSource,
    ) ProvisionedGroupStorage {
        const budgets = if (resolved_source) |source|
            smartResourceBudgetsResolved(process_memory_limit_bytes, source)
        else
            smartResourceBudgets(process_memory_limit_bytes);
        var manager_options = budgets.options;
        manager_options.identity_allocator = alloc;
        return .{
            .alloc = alloc,
            .resource_manager = resource_manager_mod.ResourceManager.init(manager_options),
            .effective_memory_limit_bytes = budgets.effective_memory_limit_bytes,
            .memory_limit_source = budgets.memory_limit_source,
            .lsm_cache = if (linked_storage) {} else lsm_backend.Cache.init(alloc, budgets.lsm_cache_budget_bytes),
            .hbc_cache = if (linked_storage) {} else hbc_mod.Cache.init(alloc),
            .runtime_status_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc),
            .read_cache = if (linked_storage) {} else table_reads.ProvisionedTableReadCache.init(alloc),
            .write_cache = if (linked_storage) {} else table_writes.ProvisionedTableWriteCache.init(alloc),
            .startup_write_cache = if (linked_storage) {} else table_writes.ProvisionedTableWriteCache.init(alloc),
        };
    }

    pub fn deinit(self: *ProvisionedGroupStorage) void {
        self.group_visible_root_generations.deinit(self.alloc);
        if (comptime !linked_storage) {
            self.startup_write_cache.deinit();
            self.write_cache.deinit();
            self.read_cache.deinit();
            self.hbc_cache.deinit();
            self.lsm_cache.deinit();
        }
        self.runtime_status_cache.deinit();
        self.resource_manager.deinit(self.alloc);
        self.* = undefined;
    }

    /// Join every cached writer DB before an externally owned provider is
    /// destroyed. Sources and request runtimes must already be quiescent, so
    /// no new cache lease can appear while this barrier holds the shared state
    /// mutex. The cache containers remain valid for the ordinary final deinit.
    pub fn quiesceExternalProviderUsers(self: *ProvisionedGroupStorage) !void {
        if (comptime linked_storage) return;
        lockAtomic(&self.write_cache_state_mutex);
        defer self.write_cache_state_mutex.unlock();
        try self.startup_write_cache.closeAllDbsLocked();
        try self.write_cache.closeAllDbsLocked();
    }

    /// Break every cache-to-source callback edge while both owners are still
    /// alive. Call this after attached write sources are quiescent and before
    /// either the sources or this storage are destroyed.
    pub fn detachWriteSourceRuntimeHooks(self: *ProvisionedGroupStorage) void {
        if (comptime linked_storage) return;
        self.startup_write_cache.detachRuntimeHooks();
        self.write_cache.detachRuntimeHooks();
        self.startup_write_cache.table_eviction_hook = null;
        self.write_cache.table_eviction_hook = null;
    }

    /// Install an operator/runtime-owned capacity domain before sources are
    /// attached. Deterministic runtimes use this to keep admission and status
    /// reporting on modeled storage instead of probing the host filesystem.
    pub fn installCapacitySource(
        self: *ProvisionedGroupStorage,
        source: resource_manager_mod.CapacitySource,
    ) !void {
        if (self.filesystem_capacity_probe != null) return error.CapacitySourceAlreadyInstalled;
        try self.resource_manager.installCapacitySource(source);
    }

    pub fn attachSources(
        self: *ProvisionedGroupStorage,
        read_source: *table_reads.ProvisionedTableReadSource,
        write_source: *table_writes.ProvisionedTableWriteSource,
    ) !void {
        // All provisioned DBs under one replica root share the ResourceManager
        // and therefore one physical capacity domain. BackendRuntime remains
        // the execution abstraction; filesystem policy and accounting stay in
        // the ResourceManager.
        if (filesystem_capacity.supported and self.resource_manager.capacitySource() == null) {
            if (self.filesystem_capacity_probe) |probe| {
                if (!std.mem.eql(u8, probe.path, write_source.replica_root_dir)) {
                    return error.CapacitySourceAlreadyInstalled;
                }
            } else {
                self.filesystem_capacity_probe = filesystem_capacity.Probe.init(write_source.replica_root_dir, 1);
            }
            try self.resource_manager.installCapacitySource(self.filesystem_capacity_probe.?.source());
        }
        if (self.backend_runtime) |runtime| {
            read_source.backend_runtime = runtime;
            write_source.backend_runtime = runtime;
        }
        if (comptime linked_storage) {
            read_source.runtime_status_cache = &self.runtime_status_cache;
            _ = read_source.withGroupVisibleRootGeneration(self.groupVisibleRootGenerationSource());
            write_source.runtime_status_cache = &self.runtime_status_cache;
            _ = write_source.withGroupVisibleRootGeneration(self.groupVisibleRootGenerationSource());
            return;
        }
        self.lsm_cache.attachResourceManager(&self.resource_manager);
        self.hbc_cache.attachResourceManager(&self.resource_manager);
        self.read_cache.lsm_cache = &self.lsm_cache;
        self.read_cache.hbc_cache = &self.hbc_cache;
        self.read_cache.resource_manager = &self.resource_manager;
        self.read_cache.backend_runtime = self.backend_runtime;
        self.read_cache.antfly_provider = read_source.antfly_provider;
        self.read_cache.secret_store = read_source.secret_store;
        // Capability discovery is a storage-runtime service, not a query-only
        // concern. Bind the same cache into writer enrichment so semantic
        // chunking and every other remote family reuse planner/executor leases
        // across documents.
        _ = write_source.withRemoteCapabilityCache(&self.read_cache.remote_capability_cache);
        read_source.reranker_runtime = try self.read_cache.ensureRerankerRuntime();
        // Resident writer DBs also serve freshness-sensitive reads. Leaving
        // their cache unset makes the LSM backend retain a private decoded
        // index for every run, bypassing both the shared cache bound and the
        // ResourceManager. The shared cache already evicts block/index data by
        // budget, so use it for every provisioned DB owner.
        self.write_cache.lsm_cache = &self.lsm_cache;
        self.write_cache.hbc_cache = &self.hbc_cache;
        self.write_cache.resource_manager = &self.resource_manager;
        self.write_cache.backend_runtime = self.backend_runtime;
        self.write_cache.antfly_provider = write_source.antfly_provider;
        self.write_cache.secret_store = write_source.secret_store;
        self.write_cache.remote_content = write_source.remote_content;
        self.write_cache.dense_native_migration_policy_source = self.denseNativeMigrationPolicySource();
        self.startup_write_cache.lsm_cache = &self.lsm_cache;
        self.startup_write_cache.hbc_cache = &self.hbc_cache;
        self.startup_write_cache.resource_manager = &self.resource_manager;
        self.startup_write_cache.backend_runtime = self.backend_runtime;
        self.startup_write_cache.antfly_provider = write_source.antfly_provider;
        self.startup_write_cache.secret_store = write_source.secret_store;
        self.startup_write_cache.remote_content = write_source.remote_content;
        self.startup_write_cache.dense_native_migration_policy_source = self.denseNativeMigrationPolicySource();
        read_source.cache = &self.read_cache;
        read_source.runtime_status_cache = &self.runtime_status_cache;
        read_source.prepare_for_read = write_source.readPreparation();
        _ = read_source.withGroupVisibleRootGeneration(self.groupVisibleRootGenerationSource());
        read_source.resident_db = write_source.residentDbSource();
        write_source.read_cache = &self.read_cache;
        write_source.bindWriteCachesWithStateMutex(
            &self.write_cache,
            &self.startup_write_cache,
            &self.write_cache_state_mutex,
        );
        write_source.runtime_status_cache = &self.runtime_status_cache;
        write_source.dense_native_migration_policy_source = self.denseNativeMigrationPolicySource();
        _ = write_source.withGroupVisibleRootGeneration(self.groupVisibleRootGenerationSource());
    }

    pub fn setDenseNativeAuthorityPermitted(self: *ProvisionedGroupStorage, permitted: bool) void {
        // Monotonic within a process. The durable per-index AUTHORITY marker is
        // the crash-sticky decision; a transient or older catalog snapshot may
        // never revoke it or make a later callback close the gate again.
        if (permitted) self.dense_native_authority_permitted.store(true, .release);
    }

    fn denseNativeAuthorityPermitted(ptr: *const anyopaque) bool {
        const self: *const ProvisionedGroupStorage = @ptrCast(@alignCast(ptr));
        return self.dense_native_authority_permitted.load(.acquire);
    }

    pub fn denseNativeMigrationPolicySource(self: *const ProvisionedGroupStorage) runtime_callbacks.DenseNativeMigrationPolicySource {
        return .{
            .ptr = self,
            .authority_permitted = denseNativeAuthorityPermitted,
        };
    }

    pub fn attachBackendRuntime(
        self: *ProvisionedGroupStorage,
        runtime: *background_runtime_mod.BackendRuntime,
        read_source: *table_reads.ProvisionedTableReadSource,
        write_source: *table_writes.ProvisionedTableWriteSource,
    ) void {
        self.backend_runtime = runtime;
        if (comptime linked_storage) {
            read_source.backend_runtime = runtime;
            write_source.backend_runtime = runtime;
            return;
        }
        self.read_cache.backend_runtime = runtime;
        self.write_cache.backend_runtime = runtime;
        self.startup_write_cache.backend_runtime = runtime;
        self.runtime_status_cache.setModeledRuntimeTelemetry(runtime.usesBorrowedIo());
        read_source.backend_runtime = runtime;
        write_source.backend_runtime = runtime;
    }

    pub fn visibleRootGenerationForGroup(self: *ProvisionedGroupStorage, group_id: u64) u64 {
        lockAtomic(&self.group_visible_root_generation_mutex);
        defer self.group_visible_root_generation_mutex.unlock();
        return if (self.group_visible_root_generations.get(group_id)) |entry| entry.generation else table_reads.backend_current_root_generation;
    }

    /// Publish schema/index metadata reconciled into the currently visible
    /// physical roots. Query owners and artifact handles must reopen, but the
    /// root generation must remain stable: advancing it would fence the one
    /// live writer even though no root replacement occurred.
    pub fn invalidateInPlaceMetadataReconcileCaches(self: *ProvisionedGroupStorage) void {
        if (comptime linked_storage) return;
        self.read_cache.clear();
        self.hbc_cache.clear();
    }

    pub fn bumpGroupVisibleRootGenerations(self: *ProvisionedGroupStorage, group_ids: []const u64) !void {
        lockAtomic(&self.group_visible_root_generation_mutex);
        defer self.group_visible_root_generation_mutex.unlock();
        for (group_ids) |group_id| {
            const entry = try self.group_visible_root_generations.getOrPut(self.alloc, group_id);
            if (entry.found_existing) {
                entry.value_ptr.generation +%= 1;
            } else {
                entry.value_ptr.* = .{ .generation = 1 };
            }
        }
    }

    fn reserveGroupVisibleRootGeneration(self: *ProvisionedGroupStorage, group_id: u64) !void {
        lockAtomic(&self.group_visible_root_generation_mutex);
        defer self.group_visible_root_generation_mutex.unlock();
        const entry = try self.group_visible_root_generations.getOrPut(self.alloc, group_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        entry.value_ptr.reservations = std.math.add(usize, entry.value_ptr.reservations, 1) catch return error.TooManyGenerationReservations;
    }

    fn finishGroupVisibleRootGenerationReservation(self: *ProvisionedGroupStorage, group_id: u64, advance: bool) void {
        lockAtomic(&self.group_visible_root_generation_mutex);
        defer self.group_visible_root_generation_mutex.unlock();
        const entry = self.group_visible_root_generations.getPtr(group_id) orelse unreachable;
        std.debug.assert(entry.reservations > 0);
        if (advance) entry.generation +%= 1;
        entry.reservations -= 1;
        if (!advance and entry.reservations == 0 and entry.generation == table_reads.backend_current_root_generation) {
            _ = self.group_visible_root_generations.remove(group_id);
        }
    }

    pub fn pruneGroupVisibleRootGenerations(self: *ProvisionedGroupStorage, retain_group_ids: []const u64) void {
        lockAtomic(&self.group_visible_root_generation_mutex);
        defer self.group_visible_root_generation_mutex.unlock();

        var stale = std.ArrayListUnmanaged(u64).empty;
        defer stale.deinit(self.alloc);
        var i = self.group_visible_root_generations.iterator();
        while (i.next()) |entry| {
            for (retain_group_ids) |group_id| {
                if (entry.key_ptr.* == group_id) break;
            } else {
                if (entry.value_ptr.reservations != 0) continue;
                stale.append(self.alloc, entry.key_ptr.*) catch return;
            }
        }
        for (stale.items) |group_id| _ = self.group_visible_root_generations.remove(group_id);
    }

    pub fn groupVisibleRootGenerationSource(self: *ProvisionedGroupStorage) table_reads.GroupVisibleRootGenerationSource {
        return .{
            .ptr = self,
            .visible_root_generation_for_group = groupVisibleRootGenerationForGroup,
            .reserve_root_generation_for_group = reserveGroupVisibleRootGenerationForGroup,
            .finish_root_generation_reservation = finishGroupVisibleRootGenerationReservationForGroup,
        };
    }

    fn groupVisibleRootGenerationForGroup(ptr: *anyopaque, group_id: u64) u64 {
        const self: *ProvisionedGroupStorage = @ptrCast(@alignCast(ptr));
        return self.visibleRootGenerationForGroup(group_id);
    }

    fn reserveGroupVisibleRootGenerationForGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *ProvisionedGroupStorage = @ptrCast(@alignCast(ptr));
        try self.reserveGroupVisibleRootGeneration(group_id);
    }

    fn finishGroupVisibleRootGenerationReservationForGroup(ptr: *anyopaque, group_id: u64, advance: bool) void {
        const self: *ProvisionedGroupStorage = @ptrCast(@alignCast(ptr));
        self.finishGroupVisibleRootGenerationReservation(group_id, advance);
    }
};

pub const implementation_tests = implementationTests();
fn implementationTests() type {
    if (!@import("builtin").is_test or @import("storage_source_options").control_only) return struct {};
    const Suite = struct {
        test "provisioned dense native authority gate is fail-closed and monotonic" {
            var storage = ProvisionedGroupStorage.init(std.testing.allocator);
            defer storage.deinit();
            const source = storage.denseNativeMigrationPolicySource();
            try std.testing.expect(!source.authorityPermitted());
            storage.setDenseNativeAuthorityPermitted(true);
            try std.testing.expect(source.authorityPermitted());
            storage.setDenseNativeAuthorityPermitted(false);
            try std.testing.expect(source.authorityPermitted());
        }

        test "provisioned group storage prunes stale visible root generations" {
            var storage = ProvisionedGroupStorage.init(std.testing.allocator);
            defer storage.deinit();

            const generation_source = storage.groupVisibleRootGenerationSource();
            var reservation = (try generation_source.reserveRootGenerationForGroup(44)).?;
            defer reservation.deinit();
            try std.testing.expectEqual(@as(u64, table_reads.backend_current_root_generation), storage.visibleRootGenerationForGroup(44));
            storage.pruneGroupVisibleRootGenerations(&.{});
            try std.testing.expectEqual(@as(u64, table_reads.backend_current_root_generation), storage.visibleRootGenerationForGroup(44));
            reservation.advance();
            try std.testing.expectEqual(@as(u64, 1), storage.visibleRootGenerationForGroup(44));

            var cancelled = (try generation_source.reserveRootGenerationForGroup(55)).?;
            cancelled.deinit();
            try std.testing.expect(!storage.group_visible_root_generations.contains(55));

            try storage.bumpGroupVisibleRootGenerations(&.{ 11, 22, 33 });
            try std.testing.expectEqual(@as(u64, 1), storage.visibleRootGenerationForGroup(11));
            try std.testing.expectEqual(@as(u64, 1), storage.visibleRootGenerationForGroup(22));
            try std.testing.expectEqual(@as(u64, 1), storage.visibleRootGenerationForGroup(33));

            storage.invalidateInPlaceMetadataReconcileCaches();
            try std.testing.expectEqual(@as(u64, 1), storage.visibleRootGenerationForGroup(11));

            storage.pruneGroupVisibleRootGenerations(&.{ 11, 33 });
            try std.testing.expectEqual(@as(u64, 1), storage.visibleRootGenerationForGroup(11));
            try std.testing.expectEqual(@as(u64, table_reads.backend_current_root_generation), storage.visibleRootGenerationForGroup(22));
            try std.testing.expectEqual(@as(u64, 1), storage.visibleRootGenerationForGroup(33));
        }

        test "provisioned group storage aligns lsm cache with resource budget" {
            var storage = ProvisionedGroupStorage.init(std.testing.allocator);
            defer storage.deinit();

            const stats = storage.resource_manager.sliceStats(.lsm_block_table_cache);
            try std.testing.expect(stats.hard_limit_bytes > 0);
            try std.testing.expectEqual(stats.hard_limit_bytes, @as(u64, @intCast(storage.lsm_cache.max_bytes)));
        }

        test "provisioned lsm cache is an elastic share of the node envelope" {
            const small = smartResourceBudgetsForTotal(2 * 1024 * MiB);
            const medium = smartResourceBudgetsForTotal(8 * 1024 * MiB);
            const large = smartResourceBudgetsForTotal(64 * 1024 * MiB);

            try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), small.lsm_cache_budget_bytes);
            try std.testing.expectEqual(@as(usize, 2 * GiB), medium.lsm_cache_budget_bytes);
            try std.testing.expectEqual(@as(usize, MaxSmartLsmCacheBytes), large.lsm_cache_budget_bytes);
            try std.testing.expect(small.lsm_cache_budget_bytes < medium.lsm_cache_budget_bytes);
            try std.testing.expect(medium.lsm_cache_budget_bytes < large.lsm_cache_budget_bytes);

            inline for (.{
                .{ .budgets = small, .total = 2 * 1024 * MiB },
                .{ .budgets = medium, .total = 8 * 1024 * MiB },
                .{ .budgets = large, .total = 64 * 1024 * MiB },
            }) |fixture| {
                const budgets = fixture.budgets;
                const configured = budgets.options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)];
                try std.testing.expectEqual(@as(u64, @intCast(budgets.lsm_cache_budget_bytes)), configured.hard_limit_bytes);
                try std.testing.expectEqual(configured.hard_limit_bytes * 7 / 8, configured.soft_limit_bytes);
                try std.testing.expectEqual(
                    safeManagedHostMemory(fixture.total),
                    budgets.options.memory_budget.hard_limit_bytes,
                );
            }
        }

        test "provisioned HBC cache is an elastic share of the node envelope" {
            const small = smartResourceBudgetsForTotal(2 * GiB);
            const medium = smartResourceBudgetsForTotal(12 * GiB);
            const large = smartResourceBudgetsForTotal(64 * GiB);

            const small_hbc = small.options.budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)];
            const medium_hbc = medium.options.budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)];
            const large_hbc = large.options.budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)];

            try std.testing.expectEqual(@as(u64, 2 * GiB / 3), small_hbc.hard_limit_bytes);
            try std.testing.expectEqual(@as(u64, 4 * GiB), medium_hbc.hard_limit_bytes);
            try std.testing.expectEqual(@as(u64, MaxSmartHbcCacheBytes), large_hbc.hard_limit_bytes);
            try std.testing.expect(small_hbc.hard_limit_bytes < medium_hbc.hard_limit_bytes);
            try std.testing.expect(medium_hbc.hard_limit_bytes < large_hbc.hard_limit_bytes);
            inline for (.{ small_hbc, medium_hbc, large_hbc }) |budget| {
                try std.testing.expectEqual(budget.hard_limit_bytes * 7 / 8, budget.soft_limit_bytes);
            }
        }

        test "effective process memory limit preserves source and clamps explicit requests" {
            const detected = DetectedMemoryLimit{ .bytes = 8 * GiB, .source = .cgroup_v2 };

            const automatic = resolveEffectiveMemoryLimit(null, detected).?;
            try std.testing.expectEqual(@as(u64, 8 * GiB), automatic.bytes);
            try std.testing.expectEqual(MemoryLimitSource.cgroup_v2, automatic.source);

            const explicit = resolveEffectiveMemoryLimit(4 * GiB, detected).?;
            try std.testing.expectEqual(@as(u64, 4 * GiB), explicit.bytes);
            try std.testing.expectEqual(MemoryLimitSource.explicit, explicit.source);

            const clamped = resolveEffectiveMemoryLimit(16 * GiB, detected).?;
            try std.testing.expectEqual(@as(u64, 8 * GiB), clamped.bytes);
            try std.testing.expectEqual(MemoryLimitSource.cgroup_v2, clamped.source);

            const explicit_without_detection = resolveEffectiveMemoryLimit(2 * GiB, null).?;
            try std.testing.expectEqual(@as(u64, 2 * GiB), explicit_without_detection.bytes);
            try std.testing.expectEqual(MemoryLimitSource.explicit, explicit_without_detection.source);
            try std.testing.expectEqual(@as(?DetectedMemoryLimit, null), resolveEffectiveMemoryLimit(null, null));
        }

        test "provisioned group storage wires remote content to writer caches" {
            var storage = ProvisionedGroupStorage.init(std.testing.allocator);
            defer storage.deinit();

            var read_source = table_reads.ProvisionedTableReadSource.init("/tmp/unused-antfly-read", table_catalog.CatalogSource{
                .ptr = undefined,
                .vtable = undefined,
            }, raft_mod.read_gate.alreadyReadSafeBarrier());
            var write_source = table_writes.ProvisionedTableWriteSource.init(".", table_catalog.CatalogSource{
                .ptr = undefined,
                .vtable = undefined,
            });
            const remote_content = scraping.RemoteContentConfig{};
            _ = write_source.withRemoteContent(&remote_content);

            try storage.attachSources(&read_source, &write_source);

            try std.testing.expectEqual(&remote_content, storage.write_cache.remote_content.?);
            try std.testing.expectEqual(&remote_content, storage.startup_write_cache.remote_content.?);
            try std.testing.expectEqual(&storage.lsm_cache, storage.read_cache.lsm_cache.?);
            try std.testing.expectEqual(&storage.lsm_cache, storage.write_cache.lsm_cache.?);
            try std.testing.expectEqual(&storage.lsm_cache, storage.startup_write_cache.lsm_cache.?);
            try std.testing.expectEqual(&storage.read_cache.remote_capability_cache, write_source.remote_capability_cache.?);

            // Keep the production aggregate LSM admission policy covered by the API
            // module's permanent root-test filter as well as the exhaustive budget
            // fixture below.
            const lsm_state = storage.resource_manager.sliceStats(.lsm_in_memory_state);
            try std.testing.expect(lsm_state.hard_limit_bytes <= MaxSmartLsmInMemoryStateBytes);
            try std.testing.expectEqual(resource_manager_mod.PressureAction.throttle_writes, lsm_state.soft_action);
            try std.testing.expectEqual(resource_manager_mod.PressureAction.throttle_writes, lsm_state.hard_action);

            if (filesystem_capacity.supported) {
                const capacity = try storage.resource_manager.capacitySource().?.current();
                try std.testing.expect(capacity.capacity_bytes.? > 0);
                try std.testing.expect(capacity.available_bytes.? <= capacity.capacity_bytes.?);
            }
        }

        test "provisioned group storage derives all resource budgets" {
            var storage = ProvisionedGroupStorage.init(std.testing.allocator);
            defer storage.deinit();

            inline for (.{
                resource_manager_mod.Slice.lsm_block_table_cache,
                resource_manager_mod.Slice.lsm_compaction_work,
                resource_manager_mod.Slice.lsm_table_builder_working_set,
                resource_manager_mod.Slice.lsm_in_memory_state,
                resource_manager_mod.Slice.lsm_wal_write_working_set,
                resource_manager_mod.Slice.hbc_node_metadata_cache,
                resource_manager_mod.Slice.dense_search_working_set,
                resource_manager_mod.Slice.dense_apply_working_set,
                resource_manager_mod.Slice.dense_routing_working_set,
                resource_manager_mod.Slice.full_text_pending_segments,
                resource_manager_mod.Slice.full_text_segment_residency,
                resource_manager_mod.Slice.derived_backlog,
                resource_manager_mod.Slice.text_merge_buffers,
                resource_manager_mod.Slice.algebraic_tensor_accumulators,
                resource_manager_mod.Slice.lite_native_page_cache,
                resource_manager_mod.Slice.lite_native_link_cache,
                resource_manager_mod.Slice.dense_repair_working_set,
                resource_manager_mod.Slice.shard_transition_working_set,
                resource_manager_mod.Slice.dense_vector_block_build_working_set,
            }) |slice| {
                const stats = storage.resource_manager.sliceStats(slice);
                try std.testing.expect(stats.hard_limit_bytes > 0);
                try std.testing.expect(stats.soft_limit_bytes > 0);
                try std.testing.expect(stats.soft_limit_bytes <= stats.hard_limit_bytes);
            }

            inline for (.{
                resource_manager_mod.Slice.inference_model_residency,
                resource_manager_mod.Slice.inference_kv_working_set,
                resource_manager_mod.Slice.inference_scratch_working_set,
            }) |slice| {
                const stats = storage.resource_manager.sliceStats(slice);
                try std.testing.expectEqual(@as(u64, 0), stats.hard_limit_bytes);
            }
            try std.testing.expect(storage.resource_manager.snapshot().memory.hard_limit_bytes > 0);

            const lsm_state = storage.resource_manager.sliceStats(.lsm_in_memory_state);
            try std.testing.expect(lsm_state.hard_limit_bytes <= MaxSmartLsmInMemoryStateBytes);
            try std.testing.expectEqual(resource_manager_mod.PressureAction.throttle_writes, lsm_state.soft_action);
            try std.testing.expectEqual(resource_manager_mod.PressureAction.throttle_writes, lsm_state.hard_action);
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = implementation_tests;
}
