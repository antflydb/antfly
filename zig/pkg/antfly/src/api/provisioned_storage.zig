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
const builtin = @import("builtin");
const hbc_mod = @import("../storage/hbc_adapter.zig");
const background_runtime_mod = @import("../storage/background_runtime.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const filesystem_capacity = @import("../storage/filesystem_capacity.zig");
const runtime_status = @import("runtime_status.zig");
const scraping = @import("antfly_scraping");
const table_catalog = @import("table_catalog.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_writes.zig");

const MiB: u64 = 1024 * 1024;
const MinSmartLsmCacheBytes: u64 = 64 * 1024 * 1024;
const MaxSmartLsmCacheBytes: u64 = 1024 * 1024 * 1024;
const MinSmartLsmCompactionBytes: u64 = 128 * 1024 * 1024;
const MaxSmartLsmCompactionBytes: u64 = 1024 * 1024 * 1024;
const MinSmartLsmTableBuilderBytes: u64 = 64 * 1024 * 1024;
const MaxSmartLsmTableBuilderBytes: u64 = 512 * 1024 * 1024;
const MinSmartLsmInMemoryStateBytes: u64 = 256 * 1024 * 1024;
const MaxSmartLsmInMemoryStateBytes: u64 = 2 * 1024 * 1024 * 1024;
const MinSmartHbcCacheBytes: u64 = 128 * 1024 * 1024;
const MaxSmartHbcCacheBytes: u64 = 2 * 1024 * 1024 * 1024;
const MinSmartDenseApplyBytes: u64 = 64 * 1024 * 1024;
const MaxSmartDenseApplyBytes: u64 = 512 * 1024 * 1024;
const MinSmartReplayWindowBytes: u64 = 64 * 1024 * 1024;
const MaxSmartReplayWindowBytes: u64 = 256 * 1024 * 1024;
const MinSmartFullTextPendingBytes: u64 = 64 * 1024 * 1024;
const MaxSmartFullTextPendingBytes: u64 = 512 * 1024 * 1024;
const MinSmartFullTextBuildBytes: u64 = 128 * 1024 * 1024;
const MaxSmartFullTextBuildBytes: u64 = 1024 * 1024 * 1024;
const MinSmartDerivedBacklogBytes: u64 = 64 * 1024 * 1024;
const MaxSmartDerivedBacklogBytes: u64 = 512 * 1024 * 1024;
const MinSmartTextMergeBytes: u64 = 32 * 1024 * 1024;
const MaxSmartTextMergeBytes: u64 = 256 * 1024 * 1024;
const MinSmartAlgebraicTensorBytes: u64 = 32 * 1024 * 1024;
const MaxSmartAlgebraicTensorBytes: u64 = 256 * 1024 * 1024;
const MinSmartDenseRepairBytes: u64 = 64 * 1024 * 1024;
const MaxSmartDenseRepairBytes: u64 = 512 * 1024 * 1024;

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn readMemoryLimitFile(path: []const u8) ?u64 {
    if (builtin.os.tag == .freestanding) return null;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();

    var file = std.Io.Dir.openFileAbsolute(io_impl.io(), path, .{}) catch return null;
    defer file.close(io_impl.io());

    var reader_buf: [128]u8 = undefined;
    var reader = file.reader(io_impl.io(), &reader_buf);
    var value_buf: [128]u8 = undefined;
    const n = reader.interface.readSliceShort(&value_buf) catch return null;
    const raw = std.mem.trim(u8, value_buf[0..n], " \t\r\n");
    if (raw.len == 0 or std.mem.eql(u8, raw, "max")) return null;
    const value = std.fmt.parseUnsigned(u64, raw, 10) catch return null;
    if (value == 0 or value > (1 << 60)) return null;
    return value;
}

fn detectedMemoryLimitBytes() ?u64 {
    if (builtin.os.tag == .linux) {
        if (readMemoryLimitFile("/sys/fs/cgroup/memory.max")) |limit| return limit;
        if (readMemoryLimitFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")) |limit| return limit;
    }
    return std.process.totalSystemMemory() catch null;
}

fn adaptiveSliceHardLimit(total: u64, divisor: u64, min_bytes: u64, max_bytes: u64) u64 {
    const target = if (divisor == 0) total else total / divisor;
    if (total < min_bytes * 4) {
        return @min(@max(8 * MiB, target), max_bytes);
    }
    return std.math.clamp(target, min_bytes, max_bytes);
}

fn clampU64ToUsize(value: u64) usize {
    const usize_max: u64 = std.math.maxInt(usize);
    return @intCast(@min(value, usize_max));
}

fn resourceBudget(soft_numerator: u64, hard_limit_bytes: u64) resource_manager_mod.Budget {
    return .{
        .soft_limit_bytes = hard_limit_bytes * soft_numerator / 4,
        .hard_limit_bytes = hard_limit_bytes,
    };
}

const SmartResourceBudgets = struct {
    options: resource_manager_mod.Options,
    lsm_cache_budget_bytes: usize,
};

fn smartResourceBudgets() SmartResourceBudgets {
    var options = resource_manager_mod.Options{};
    const total = detectedMemoryLimitBytes() orelse {
        const lsm_cache_budget = lsm_backend.DefaultCacheSizeBytes;
        options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)] = resourceBudget(3, @intCast(lsm_cache_budget));
        return .{
            .options = options,
            .lsm_cache_budget_bytes = lsm_cache_budget,
        };
    };

    const lsm_hard = adaptiveSliceHardLimit(total, 16, MinSmartLsmCacheBytes, MaxSmartLsmCacheBytes);
    const lsm_compaction_hard = adaptiveSliceHardLimit(total, 16, MinSmartLsmCompactionBytes, MaxSmartLsmCompactionBytes);
    const lsm_table_builder_hard = adaptiveSliceHardLimit(total, 32, MinSmartLsmTableBuilderBytes, MaxSmartLsmTableBuilderBytes);
    const lsm_in_memory_state_hard = adaptiveSliceHardLimit(total, 8, MinSmartLsmInMemoryStateBytes, MaxSmartLsmInMemoryStateBytes);
    const lsm_wal_write_hard = adaptiveSliceHardLimit(total, 16, MinSmartLsmInMemoryStateBytes, MaxSmartLsmInMemoryStateBytes);
    const hbc_hard = adaptiveSliceHardLimit(total, 12, MinSmartHbcCacheBytes, MaxSmartHbcCacheBytes);
    const dense_search_hard = adaptiveSliceHardLimit(total, 24, MinSmartDenseApplyBytes, MaxSmartDenseApplyBytes);
    const dense_apply_hard = adaptiveSliceHardLimit(total, 24, MinSmartDenseApplyBytes, MaxSmartDenseApplyBytes);
    const replay_hard = adaptiveSliceHardLimit(total, 32, MinSmartReplayWindowBytes, MaxSmartReplayWindowBytes);
    const full_text_hard = adaptiveSliceHardLimit(total, 32, MinSmartFullTextPendingBytes, MaxSmartFullTextPendingBytes);
    const full_text_build_hard = adaptiveSliceHardLimit(total, 24, MinSmartFullTextBuildBytes, MaxSmartFullTextBuildBytes);
    const derived_hard = adaptiveSliceHardLimit(total, 32, MinSmartDerivedBacklogBytes, MaxSmartDerivedBacklogBytes);
    const text_merge_hard = adaptiveSliceHardLimit(total, 64, MinSmartTextMergeBytes, MaxSmartTextMergeBytes);
    const algebraic_tensor_hard = adaptiveSliceHardLimit(total, 64, MinSmartAlgebraicTensorBytes, MaxSmartAlgebraicTensorBytes);
    const dense_repair_hard = adaptiveSliceHardLimit(total, 24, MinSmartDenseRepairBytes, MaxSmartDenseRepairBytes);

    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)] = resourceBudget(3, lsm_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_compaction_work)] = resourceBudget(3, lsm_compaction_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_table_builder_working_set)] = resourceBudget(3, lsm_table_builder_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)] = resourceBudget(3, lsm_in_memory_state_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_wal_write_working_set)] = resourceBudget(3, lsm_wal_write_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)] = resourceBudget(3, hbc_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_search_working_set)] = resourceBudget(3, dense_search_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_apply_working_set)] = resourceBudget(3, dense_apply_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_routing_working_set)] = resourceBudget(3, dense_apply_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.derived_replay_window)] = resourceBudget(3, replay_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)] = resourceBudget(3, full_text_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.full_text_build_working_set)] = resourceBudget(2, full_text_build_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = resourceBudget(3, derived_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = resourceBudget(3, text_merge_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.algebraic_tensor_accumulators)] = resourceBudget(3, algebraic_tensor_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_repair_working_set)] = resourceBudget(3, dense_repair_hard);

    return .{
        .options = options,
        .lsm_cache_budget_bytes = clampU64ToUsize(lsm_hard),
    };
}

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
    lsm_cache: lsm_backend.Cache,
    hbc_cache: hbc_mod.Cache,
    runtime_status_cache: runtime_status.TableRuntimeSnapshotCache,
    read_cache: table_reads.ProvisionedTableReadCache,
    write_cache: table_writes.ProvisionedTableWriteCache,
    startup_write_cache: table_writes.ProvisionedTableWriteCache,
    backend_runtime: ?*background_runtime_mod.BackendRuntime = null,

    pub fn init(alloc: std.mem.Allocator) ProvisionedGroupStorage {
        const budgets = smartResourceBudgets();
        return .{
            .alloc = alloc,
            .resource_manager = resource_manager_mod.ResourceManager.init(budgets.options),
            .lsm_cache = lsm_backend.Cache.init(alloc, budgets.lsm_cache_budget_bytes),
            .hbc_cache = hbc_mod.Cache.init(alloc),
            .runtime_status_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc),
            .read_cache = table_reads.ProvisionedTableReadCache.init(alloc),
            .write_cache = table_writes.ProvisionedTableWriteCache.init(alloc),
            .startup_write_cache = table_writes.ProvisionedTableWriteCache.init(alloc),
        };
    }

    pub fn deinit(self: *ProvisionedGroupStorage) void {
        self.group_visible_root_generations.deinit(self.alloc);
        self.startup_write_cache.deinit();
        self.write_cache.deinit();
        self.read_cache.deinit();
        self.runtime_status_cache.deinit();
        self.hbc_cache.deinit();
        self.lsm_cache.deinit();
        self.resource_manager.deinit(self.alloc);
        self.* = undefined;
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
        if (filesystem_capacity.supported) {
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
        self.lsm_cache.attachResourceManager(&self.resource_manager);
        self.hbc_cache.attachResourceManager(&self.resource_manager);
        self.read_cache.lsm_cache = &self.lsm_cache;
        self.read_cache.hbc_cache = &self.hbc_cache;
        self.read_cache.resource_manager = &self.resource_manager;
        self.read_cache.backend_runtime = self.backend_runtime;
        self.read_cache.antfly_provider = read_source.antfly_provider;
        self.read_cache.secret_store = read_source.secret_store;
        // Keep the shared LSM block cache query-side. Writer DBs should use
        // transient read buffers during ingest/build work rather than warming
        // the read cache with blocks that may never be queried.
        self.write_cache.lsm_cache = null;
        self.write_cache.hbc_cache = &self.hbc_cache;
        self.write_cache.resource_manager = &self.resource_manager;
        self.write_cache.backend_runtime = self.backend_runtime;
        self.write_cache.antfly_provider = write_source.antfly_provider;
        self.write_cache.secret_store = write_source.secret_store;
        self.write_cache.remote_content = write_source.remote_content;
        self.startup_write_cache.lsm_cache = null;
        self.startup_write_cache.hbc_cache = &self.hbc_cache;
        self.startup_write_cache.resource_manager = &self.resource_manager;
        self.startup_write_cache.backend_runtime = self.backend_runtime;
        self.startup_write_cache.antfly_provider = write_source.antfly_provider;
        self.startup_write_cache.secret_store = write_source.secret_store;
        self.startup_write_cache.remote_content = write_source.remote_content;
        read_source.cache = &self.read_cache;
        read_source.runtime_status_cache = &self.runtime_status_cache;
        read_source.prepare_for_read = write_source.readPreparation();
        _ = read_source.withGroupVisibleRootGeneration(self.groupVisibleRootGenerationSource());
        read_source.primary_lookup_db = write_source.primaryLookupDbSource();
        write_source.read_cache = &self.read_cache;
        write_source.bindWriteCaches(&self.write_cache, &self.startup_write_cache);
        write_source.runtime_status_cache = &self.runtime_status_cache;
        _ = write_source.withGroupVisibleRootGeneration(self.groupVisibleRootGenerationSource());
    }

    pub fn attachBackendRuntime(
        self: *ProvisionedGroupStorage,
        runtime: *background_runtime_mod.BackendRuntime,
        read_source: *table_reads.ProvisionedTableReadSource,
        write_source: *table_writes.ProvisionedTableWriteSource,
    ) void {
        self.backend_runtime = runtime;
        self.read_cache.backend_runtime = runtime;
        self.write_cache.backend_runtime = runtime;
        self.startup_write_cache.backend_runtime = runtime;
        read_source.backend_runtime = runtime;
        write_source.backend_runtime = runtime;
    }

    pub fn visibleRootGenerationForGroup(self: *ProvisionedGroupStorage, group_id: u64) u64 {
        lockAtomic(&self.group_visible_root_generation_mutex);
        defer self.group_visible_root_generation_mutex.unlock();
        return if (self.group_visible_root_generations.get(group_id)) |entry| entry.generation else table_reads.backend_current_root_generation;
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

test "provisioned group storage wires remote content to writer caches" {
    var storage = ProvisionedGroupStorage.init(std.testing.allocator);
    defer storage.deinit();

    var read_source = table_reads.ProvisionedTableReadSource.init("/tmp/unused-antfly-read", table_catalog.CatalogSource{
        .ptr = undefined,
        .vtable = undefined,
    }, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.ProvisionedTableWriteSource.init(".", table_catalog.CatalogSource{
        .ptr = undefined,
        .vtable = undefined,
    });
    const remote_content = scraping.RemoteContentConfig{};
    _ = write_source.withRemoteContent(&remote_content);

    try storage.attachSources(&read_source, &write_source);

    try std.testing.expectEqual(&remote_content, storage.write_cache.remote_content.?);
    try std.testing.expectEqual(&remote_content, storage.startup_write_cache.remote_content.?);
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
        resource_manager_mod.Slice.derived_backlog,
        resource_manager_mod.Slice.text_merge_buffers,
        resource_manager_mod.Slice.algebraic_tensor_accumulators,
        resource_manager_mod.Slice.lite_native_page_cache,
        resource_manager_mod.Slice.lite_native_link_cache,
        resource_manager_mod.Slice.dense_repair_working_set,
    }) |slice| {
        const stats = storage.resource_manager.sliceStats(slice);
        try std.testing.expect(stats.hard_limit_bytes > 0);
        try std.testing.expect(stats.soft_limit_bytes > 0);
        try std.testing.expect(stats.soft_limit_bytes <= stats.hard_limit_bytes);
    }
}
