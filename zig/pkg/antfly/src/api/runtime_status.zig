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
const platform_time = @import("antfly_platform").time;
const db_mod = struct {
    pub const types = @import("../storage/db/types.zig");
};
const lsm_backend = @import("../storage/lsm_backend/mod.zig");

pub const RuntimeStatusSource = enum {
    unknown,
    synthetic_config,
    cached_snapshot,
    live_writer_publish,
    background_refresh,
    startup_catch_up,
    remote_store,
    rebuild_state_quarantine,
};

pub const RuntimeStatusFreshness = enum {
    unknown,
    fresh,
    stale,
    missing,
    remote_unknown,
    opening,
    catching_up,
    failed,
};

pub const RuntimeStatusMetadata = struct {
    updated_at_ns: u64 = 0,
    source: RuntimeStatusSource = .unknown,
    freshness: RuntimeStatusFreshness = .unknown,
    topology_generation: u64 = 0,
    lsm_root_generation: u64 = 0,
    status_generation: u64 = 0,
    // Highest durable source target sampled under the same DB lock as this
    // observation. It is diagnostic across reporters; the accompanying
    // completeness bit is the portable authority decision.
    target_observation_revision: u64 = 0,
    // Independent convergence authority. A committed table mutation clears
    // this bit in the immutable status cache; only a subsequent runtime-owner
    // publication may assert that its replay target and coverage describe the
    // latest accepted table target. Serving authority remains index-scoped in
    // DBIndexStats and is intentionally unaffected by this bit.
    target_observation_complete: bool = true,
    store_id: u64 = 0,
    node_id: u64 = 0,

    pub fn withDefaults(self: @This(), source: RuntimeStatusSource, now_ns: u64) @This() {
        var out = self;
        if (out.updated_at_ns == 0) out.updated_at_ns = now_ns;
        if (out.source == .unknown) out.source = source;
        if (out.freshness == .unknown) out.freshness = .fresh;
        return out;
    }
};

pub const LocalTableRuntimeStatus = struct {
    group_id: u64 = 0,
    // Internal ordering for concurrent observations of one live DB. This is
    // deliberately separate from metadata.status_generation, which identifies
    // externally published store snapshots.
    cache_observation_generation: u64 = 0,
    // Identifies the filesystem observation that produced disk_bytes. Disk
    // usage has a separate causal lifetime from DB/runtime facts: a cached or
    // startup status can still carry a freshly scanned, authoritative size.
    disk_observation_generation: u64 = 0,
    metadata: RuntimeStatusMetadata = .{},
    disk_bytes: u64 = 0,
    disk_bytes_known: bool = false,
    created_at_millis: u64 = 0,
    stats: db_mod.types.DBStats,
    lsm_storage_stats: ?LsmStorageStats = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        db_mod.types.freeDBStats(alloc, self.stats);
        self.* = undefined;
    }

    pub fn clone(self: *const @This(), alloc: std.mem.Allocator) !@This() {
        return .{
            .group_id = self.group_id,
            .cache_observation_generation = self.cache_observation_generation,
            .disk_observation_generation = self.disk_observation_generation,
            .metadata = self.metadata,
            .disk_bytes = self.disk_bytes,
            .disk_bytes_known = self.disk_bytes_known,
            .created_at_millis = self.created_at_millis,
            .stats = try cloneDBStats(alloc, self.stats),
            .lsm_storage_stats = self.lsm_storage_stats,
        };
    }

    pub fn withMetadataDefaults(self: *@This(), source: RuntimeStatusSource, now_ns: u64) void {
        self.replaceMetadata(self.metadata.withDefaults(source, now_ns));
    }

    // Metadata transitions never establish index serviceability. Replacing or
    // relabeling an observation therefore clears every cache-local proof; only
    // the exact cache merge may mint one after validating its full identity.
    pub fn replaceMetadata(self: *@This(), metadata: RuntimeStatusMetadata) void {
        self.metadata = metadata;
        for (self.stats.indexes) |*item| {
            item.runtime_observation_serviceable = false;
            item.runtime_observation_targeted_sibling = false;
        }
    }

    pub fn relabel(
        self: *@This(),
        source: RuntimeStatusSource,
        freshness: RuntimeStatusFreshness,
        updated_at_ns: u64,
    ) void {
        var metadata = self.metadata;
        metadata.source = source;
        metadata.freshness = freshness;
        metadata.updated_at_ns = updated_at_ns;
        self.replaceMetadata(metadata);
    }

    pub fn replaceFreshness(self: *@This(), freshness: RuntimeStatusFreshness) void {
        var metadata = self.metadata;
        metadata.freshness = freshness;
        self.replaceMetadata(metadata);
    }
};

pub const LsmStorageStats = struct {
    maintenance: lsm_backend.Backend.MaintenanceStats = .{},
    write: lsm_backend.Backend.WriteStats = .{},
    maintenance_score: u64 = 0,
    maintenance_debt_hint: u64 = 0,
};

pub fn statusHasRuntimeFacts(status: LocalTableRuntimeStatus) bool {
    return switch (status.metadata.source) {
        .live_writer_publish, .background_refresh, .startup_catch_up, .remote_store, .rebuild_state_quarantine => true,
        .cached_snapshot, .unknown, .synthetic_config => statusStatsHaveRuntimeFacts(status.stats),
    };
}

pub fn statusRuntimeFresh(status: LocalTableRuntimeStatus) bool {
    return statusHasRuntimeFacts(status) and status.metadata.freshness == .fresh;
}

pub const LocalTableRuntimeStatuses = struct {
    items: []LocalTableRuntimeStatus = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }

    pub fn clone(self: *const @This(), alloc: std.mem.Allocator) !@This() {
        const items = try alloc.alloc(LocalTableRuntimeStatus, self.items.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(items);
        }

        for (self.items, 0..) |item, i| {
            items[i] = try item.clone(alloc);
            initialized += 1;
        }
        return .{ .items = items };
    }
};

pub const TableRuntimeSnapshot = struct {
    table_name: []u8,
    statuses: LocalTableRuntimeStatuses,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.statuses.deinit(alloc);
        self.* = undefined;
    }
};

pub const TableRuntimeSummary = struct {
    table_count: usize = 0,
    group_count: usize = 0,
    index_count: usize = 0,
    tables_with_replay_debt: usize = 0,
    groups_with_replay_debt: usize = 0,
    indexes_with_replay_debt: usize = 0,
    outstanding_replay_sequences: u64 = 0,
    max_index_replay_backlog: u64 = 0,
    text_merge: db_mod.types.TextMergeStats = .{},
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
};

/// Stable status-plane failure classes. This deliberately does not import
/// metadata's RPC progress type: storage/runtime status is below that layer and
/// callers translate their transport-specific enum at the boundary.
pub const IndexActivationFailureCode = enum(u8) {
    invalid_target = 1,
    conflicting_target = 2,
    unsupported = 3,
    publication_failed = 4,
    internal = 5,

    fn stableName(self: @This()) []const u8 {
        return switch (self) {
            .invalid_target => "InvalidIndexActivationTarget",
            .conflicting_target => "IndexActivationTargetConflict",
            .unsupported => "UnsupportedOperation",
            .publication_failed => "IndexActivationPublicationFailed",
            .internal => "IndexActivationInternalFailure",
        };
    }
};

const TargetedIndexAuthority = struct {
    const GroupAcknowledgement = struct {
        // Serving authority is independent of exact-identity acceptance. Keep
        // the proof supplied by the resident owner until the all-group handoff
        // completes, even if a cache-only refresh replaces the visible row.
        serviceable: bool = false,
    };

    const Identity = struct {
        kind: db_mod.types.IndexKind,
        incarnation: u64,
        config_hash: u64,
    };

    const Expectation = union(enum) {
        // A catalog mutation has crossed its visibility boundary, but the
        // structural owner has not yet acknowledged the desired target. No
        // same-name runtime observation may acquire authority in this state.
        unknown,
        // The authoritative structural observation proved that the target is
        // no longer part of the desired index set.
        absent,
        // Only this durable catalog incarnation may acquire target authority.
        exact: Identity,
    };

    const ConvergenceRequirement = struct {
        // Cache-local ordering fence: a status capture begun before the
        // commit event cannot acknowledge it even if it publishes later.
        event_revision: u64,
        // Durable per-index target ordering. The runtime row must prove that
        // its replay target includes this source commit.
        source_target_sequence: u64,
        // Serving and convergence are independent authorities. Additive work
        // may make completion stale, but it cannot authorize a lower serving
        // cardinality for the same incarnation. Reduction authority is kept
        // until an owner observation covers the coalesced requirement.
        serving_set_may_reduce: bool,
    };

    const TerminalFailure = struct {
        expectation: Expectation,
        code: IndexActivationFailureCode,
    };

    // Globally unique operation revision. Every control-plane mutation gets a
    // new revision, so a delayed arm/acknowledgement/release from an older
    // owner cannot modify the authority selected by a newer mutation.
    transition_revision: u64,
    // Exact records survive transition settlement so a late same-name
    // publisher cannot replace a live incarnation. Settled absence records are
    // reclaimed: the deleted name is not addressable through the catalog, and
    // a future same-name create installs a new revision before it is visible.
    transition_active: bool = true,
    // Coalesced work for one revision shares one owner. A newer revision
    // supersedes the old owner rather than joining its lifetime.
    owner_active: bool = true,
    // Only observations captured after the mutation boundary may hand target
    // authority back. This excludes the resident sibling snapshot deliberately
    // published between fencing and applying the target.
    accept_target_after_observation_generation: u64,
    expectation: Expectation = .unknown,
    // The observation generation which bound `expectation`. A delayed
    // targeted publication may never roll desired authority backwards.
    expectation_observation_generation: u64 = 0,
    // Once the exact target publishes queryable state (or an exact failure),
    // it no longer remains stale. The active transition still waits for a
    // fresh post-release table observation; the accepted identity then
    // remains as a persistent watermark.
    target_authority_handed_off: bool = false,
    // Structural ownership may finish before the target's async generation
    // catch-up. Retain the fence until a later fresh table publication
    // performs the actual authority handoff.
    release_after_observation_generation: ?u64 = null,
    // Groups which have published the exact structural expectation after the
    // transition boundary. This turns handoff into an incremental reduction:
    // each group publication is inspected once instead of rescanning every
    // index in every group for every active transition.
    handoff_groups: std.AutoHashMapUnmanaged(u64, GroupAcknowledgement) = .empty,
    // Terminal owner outcomes are durable authority for this exact desired
    // identity, not transient handoff bookkeeping. Settlement may discard
    // acknowledgements after every group has crossed the release boundary,
    // but the public action-required result must survive until a new catalog
    // transition (or an explicit replan to a different identity) replaces it.
    terminal_failures: std.AutoHashMapUnmanaged(u64, TerminalFailure) = .empty,
    // Immutable catalog group set for this transition. Handoff is measured
    // against desired topology, never against whichever groups happen to have
    // a cached runtime observation at the time of publication.
    expected_handoff_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,
    handoff_topology_bound: bool = false,
    // Independent convergence watermarks by shard/group. These are attached
    // to the exact expectation above, so a delayed event for a retired
    // incarnation cannot fence its same-name replacement.
    convergence_requirements: std.AutoHashMapUnmanaged(u64, ConvergenceRequirement) = .empty,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.handoff_groups.deinit(alloc);
        self.terminal_failures.deinit(alloc);
        self.expected_handoff_groups.deinit(alloc);
        self.convergence_requirements.deinit(alloc);
        self.* = undefined;
    }
};

/// Copy-on-write acknowledgement capacity for one retained table lane.
/// Authority readers synchronize only through the global cache mutex, so a
/// live hash map must never reallocate while that mutex is available. Build
/// replacements under exact table ownership, swap them after epoch
/// revalidation, and retire the displaced maps after the commit unlocks.
const TargetAcknowledgementCapacityPreparation = struct {
    const Item = struct {
        authority: *TargetedIndexAuthority,
        replacement: std.AutoHashMapUnmanaged(u64, TargetedIndexAuthority.GroupAcknowledgement),
    };

    items: []Item = &.{},
    initialized: usize = 0,
    installed: bool = false,

    fn init(
        alloc: std.mem.Allocator,
        authorities: *std.StringHashMapUnmanaged(TargetedIndexAuthority),
        statuses: []const LocalTableRuntimeStatus,
    ) !@This() {
        if (statuses.len == 0) return .{};
        var growth_count: usize = 0;
        var count_it = authorities.valueIterator();
        while (count_it.next()) |authority| {
            if (!authority.transition_active or authority.target_authority_handed_off) continue;
            const required = try requiredCapacity(authority.*, statuses);
            if (maximumCount(authority.handoff_groups.capacity()) < required)
                growth_count += 1;
        }
        if (growth_count == 0) return .{};

        const items = try alloc.alloc(Item, growth_count);
        var out = @This(){ .items = items };
        errdefer out.deinit(alloc);
        var authority_it = authorities.valueIterator();
        while (authority_it.next()) |authority| {
            if (!authority.transition_active or authority.target_authority_handed_off) continue;
            const required = try requiredCapacity(authority.*, statuses);
            if (maximumCount(authority.handoff_groups.capacity()) >= required) continue;
            var replacement = std.AutoHashMapUnmanaged(
                u64,
                TargetedIndexAuthority.GroupAcknowledgement,
            ).empty;
            errdefer replacement.deinit(alloc);
            try replacement.ensureTotalCapacity(
                alloc,
                std.math.cast(u32, required) orelse return error.OutOfMemory,
            );
            var acknowledgement_it = authority.handoff_groups.iterator();
            while (acknowledgement_it.next()) |entry|
                replacement.putAssumeCapacityNoClobber(entry.key_ptr.*, entry.value_ptr.*);
            out.items[out.initialized] = .{
                .authority = authority,
                .replacement = replacement,
            };
            out.initialized += 1;
        }
        return out;
    }

    fn requiredCapacity(
        authority: TargetedIndexAuthority,
        statuses: []const LocalTableRuntimeStatus,
    ) !usize {
        var required: usize = authority.handoff_groups.count();
        for (statuses) |status| {
            if (authority.handoff_groups.contains(status.group_id) or
                (authority.handoff_topology_bound and
                    !authority.expected_handoff_groups.contains(status.group_id))) continue;
            required = std.math.add(usize, required, 1) catch return error.OutOfMemory;
        }
        return required;
    }

    fn maximumCount(capacity: usize) usize {
        return (capacity * std.hash_map.default_max_load_percentage) / 100;
    }

    fn install(self: *@This()) void {
        std.debug.assert(!self.installed);
        for (self.items[0..self.initialized]) |*item|
            std.mem.swap(
                std.AutoHashMapUnmanaged(u64, TargetedIndexAuthority.GroupAcknowledgement),
                &item.authority.handoff_groups,
                &item.replacement,
            );
        self.installed = true;
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.items[0..self.initialized]) |*item| item.replacement.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

const IndexObservationLookup = struct {
    by_name: std.StringHashMapUnmanaged(usize) = .empty,

    fn initCapacity(alloc: std.mem.Allocator, count: usize) !@This() {
        var out: @This() = .{};
        errdefer out.deinit(alloc);
        try out.by_name.ensureTotalCapacity(alloc, @intCast(count));
        return out;
    }

    fn init(
        alloc: std.mem.Allocator,
        indexes: []const db_mod.types.DBIndexStats,
    ) !@This() {
        var out = try initCapacity(alloc, indexes.len);
        errdefer out.deinit(alloc);
        try out.populate(indexes);
        return out;
    }

    fn populate(self: *@This(), indexes: []const db_mod.types.DBIndexStats) !void {
        std.debug.assert(self.by_name.count() == 0);
        for (indexes, 0..) |item, index| {
            const result = self.by_name.getOrPutAssumeCapacity(item.name);
            if (result.found_existing) return error.DuplicateRuntimeStatusIndex;
            result.value_ptr.* = index;
        }
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.by_name.deinit(alloc);
        self.* = undefined;
    }
};

/// Allocation-owned scratch for one optimistic multi-group publication.
/// Cached and incoming index rows are moved—not cloned—into `merged_indexes`
/// only after every group has been validated under the cache mutex. Retired
/// deep state is destroyed after unlocking, keeping the shared critical path
/// allocation-free and linear in the two index sets.
const IndexDeltaMergeWorkspace = struct {
    expected_previous_generation: u64,
    expected_previous_index_count: usize,
    merged_indexes: []db_mod.types.DBIndexStats = &.{},
    retired_indexes: []db_mod.types.DBIndexStats = &.{},
    retired_count: usize = 0,
    cached_selected: []bool = &.{},
    incoming_selected: []bool = &.{},
    previous_lookup: IndexObservationLookup = .{},
    retired_status: ?LocalTableRuntimeStatus = null,
    old_cached_backing: []db_mod.types.DBIndexStats = &.{},
    old_incoming_backing: []db_mod.types.DBIndexStats = &.{},
    installed: bool = false,

    fn init(
        alloc: std.mem.Allocator,
        previous_generation: u64,
        previous_index_count: usize,
        incoming_index_count: usize,
        merged_index_count: usize,
    ) !@This() {
        var out: @This() = .{
            .expected_previous_generation = previous_generation,
            .expected_previous_index_count = previous_index_count,
        };
        errdefer out.deinit(alloc);
        if (merged_index_count > 0) out.merged_indexes = try alloc.alloc(db_mod.types.DBIndexStats, merged_index_count);
        if (previous_index_count + incoming_index_count > 0)
            out.retired_indexes = try alloc.alloc(db_mod.types.DBIndexStats, previous_index_count + incoming_index_count);
        if (previous_index_count > 0) {
            out.cached_selected = try alloc.alloc(bool, previous_index_count);
            @memset(out.cached_selected, false);
        }
        if (incoming_index_count > 0) {
            out.incoming_selected = try alloc.alloc(bool, incoming_index_count);
            @memset(out.incoming_selected, false);
        }
        out.previous_lookup = try IndexObservationLookup.initCapacity(alloc, previous_index_count);
        return out;
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.previous_lookup.deinit(alloc);
        if (self.installed) {
            for (self.retired_indexes[0..self.retired_count]) |item|
                db_mod.types.freeDBIndexStatsItem(alloc, item);
            if (self.old_cached_backing.len > 0) alloc.free(self.old_cached_backing);
            if (self.old_incoming_backing.len > 0) alloc.free(self.old_incoming_backing);
            if (self.retired_status) |*status| status.deinit(alloc);
        } else if (self.merged_indexes.len > 0) {
            alloc.free(self.merged_indexes);
        }
        if (self.retired_indexes.len > 0) alloc.free(self.retired_indexes);
        if (self.cached_selected.len > 0) alloc.free(self.cached_selected);
        if (self.incoming_selected.len > 0) alloc.free(self.incoming_selected);
        self.* = undefined;
    }
};

const IndexDeltaMergeSpec = struct {
    previous_generation: u64,
    previous_index_count: usize,
    merged_index_count: usize,
};

const TargetObservationUpdate = enum {
    group_applied,
    index_applied,
    no_change,
    state_invalidated,
};

const TargetAuthorityAcknowledgementKey = struct {
    index_name: []const u8,
    group_id: u64,
};

const TargetAuthorityAcknowledgementKeyContext = struct {
    pub fn hash(_: @This(), key: TargetAuthorityAcknowledgementKey) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&key.group_id));
        hasher.update(key.index_name);
        return hasher.final();
    }

    pub fn eql(_: @This(), lhs: TargetAuthorityAcknowledgementKey, rhs: TargetAuthorityAcknowledgementKey) bool {
        return lhs.group_id == rhs.group_id and std.mem.eql(u8, lhs.index_name, rhs.index_name);
    }
};

const TargetAuthorityAcknowledgementCandidates = std.HashMapUnmanaged(
    TargetAuthorityAcknowledgementKey,
    struct {
        transition_revision: u64,
        serviceable: bool,
    },
    TargetAuthorityAcknowledgementKeyContext,
    std.hash_map.default_max_load_percentage,
);

const TargetAuthorityAcknowledgementPresence = std.HashMapUnmanaged(
    TargetAuthorityAcknowledgementKey,
    void,
    TargetAuthorityAcknowledgementKeyContext,
    std.hash_map.default_max_load_percentage,
);

const TestStructuralPublishPreparationHook = struct {
    ptr: *anyopaque,
    run: *const fn (*anyopaque) void,
};

var test_after_structural_publish_preparation_hook: ?TestStructuralPublishPreparationHook = null;

const TestReadGroupPreparationHook = struct {
    ptr: *anyopaque,
    run: *const fn (*anyopaque) void,
};

var test_read_group_preparation_hook: ?TestReadGroupPreparationHook = null;
var test_read_group_retirement_hook: ?TestReadGroupPreparationHook = null;
var test_authority_sync_index_visits: std.atomic.Value(usize) = .init(0);

fn runTestAfterStructuralPublishPreparationHook() void {
    if (comptime builtin.is_test) {
        if (test_after_structural_publish_preparation_hook) |hook| hook.run(hook.ptr);
    }
}

fn runTestReadGroupPreparationHook() void {
    if (comptime builtin.is_test) {
        if (test_read_group_preparation_hook) |hook| hook.run(hook.ptr);
    }
}

fn runTestReadGroupRetirementHook() void {
    if (comptime builtin.is_test) {
        if (test_read_group_retirement_hook) |hook| hook.run(hook.ptr);
    }
}

pub const TableRuntimeSnapshotCache = struct {
    const ReadIndexAuthority = struct {
        kind: db_mod.types.IndexKind,
        coverage_generation: u64,
        coverage_config_hash: u64,
        coverage_identity_ready: bool,
        target_observation_complete: std.atomic.Value(bool),
        observation_stale: std.atomic.Value(bool),
        observation_serviceable: std.atomic.Value(bool),
        targeted_sibling: std.atomic.Value(bool),
        terminal_failure_code: std.atomic.Value(u8),

        fn init(status: db_mod.types.DBIndexStats) @This() {
            return .{
                .kind = status.kind,
                .coverage_generation = status.coverage_generation,
                .coverage_config_hash = status.coverage_config_hash,
                .coverage_identity_ready = status.coverage_identity_ready,
                .target_observation_complete = .init(status.runtime_target_observation_complete),
                .observation_stale = .init(status.runtime_observation_stale),
                .observation_serviceable = .init(status.runtime_observation_serviceable),
                .targeted_sibling = .init(status.runtime_observation_targeted_sibling),
                .terminal_failure_code = .init(0),
            };
        }

        fn identityMatches(self: *const @This(), status: db_mod.types.DBIndexStats) bool {
            return self.kind == status.kind and
                self.coverage_generation == status.coverage_generation and
                self.coverage_config_hash == status.coverage_config_hash and
                self.coverage_identity_ready == status.coverage_identity_ready;
        }

        fn fenceIdentityMismatch(self: *@This()) void {
            // The immutable payload still belongs to another incarnation. It
            // may remain readable as diagnostic history, but authority from a
            // replacement must never be attached to it under memory pressure.
            self.target_observation_complete.store(false, .release);
            self.observation_stale.store(true, .release);
            self.observation_serviceable.store(false, .release);
            self.targeted_sibling.store(false, .release);
            self.storeTerminalFailure(null);
        }

        fn store(self: *@This(), status: db_mod.types.DBIndexStats) void {
            // Withdraw optimistic facts first and publish them last. Readers
            // may observe either side of a concurrent transition, but never
            // a new unfenced/serviceable claim without its preceding proof.
            if (!status.runtime_target_observation_complete)
                self.target_observation_complete.store(false, .release);
            if (status.runtime_observation_stale)
                self.observation_stale.store(true, .release);
            self.observation_serviceable.store(status.runtime_observation_serviceable, .release);
            self.targeted_sibling.store(status.runtime_observation_targeted_sibling, .release);
            if (!status.runtime_observation_stale)
                self.observation_stale.store(false, .release);
            if (status.runtime_target_observation_complete)
                self.target_observation_complete.store(true, .release);
        }

        fn storeTerminalFailure(self: *@This(), failure: ?IndexActivationFailureCode) void {
            self.terminal_failure_code.store(if (failure) |code| @intFromEnum(code) else 0, .release);
        }

        fn terminalFailure(self: *const @This()) ?IndexActivationFailureCode {
            const raw = self.terminal_failure_code.load(.acquire);
            return if (raw == 0) null else @enumFromInt(raw);
        }
    };

    const ReadGroupAuthority = struct {
        target_observation_complete: std.atomic.Value(bool),
        indexes: []ReadIndexAuthority,
        index_by_name: std.StringHashMapUnmanaged(usize) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.index_by_name.deinit(alloc);
            alloc.free(self.indexes);
            self.* = undefined;
        }
    };

    const ReadGroupSnapshot = struct {
        ref_count: std.atomic.Value(usize) = .init(1),
        status: LocalTableRuntimeStatus,
        authority: ReadGroupAuthority,

        fn init(
            alloc: std.mem.Allocator,
            status: LocalTableRuntimeStatus,
        ) !*@This() {
            runTestReadGroupPreparationHook();
            const group_snapshot = try alloc.create(@This());
            errdefer alloc.destroy(group_snapshot);
            var owned_status = try status.clone(alloc);
            errdefer owned_status.deinit(alloc);
            const indexes = try alloc.alloc(ReadIndexAuthority, owned_status.stats.indexes.len);
            var authority = ReadGroupAuthority{
                .target_observation_complete = .init(owned_status.metadata.target_observation_complete),
                .indexes = indexes,
            };
            errdefer alloc.free(indexes);
            try authority.index_by_name.ensureTotalCapacity(alloc, @intCast(indexes.len));
            for (owned_status.stats.indexes, 0..) |index_status, index| {
                indexes[index] = ReadIndexAuthority.init(index_status);
                authority.index_by_name.putAssumeCapacityNoClobber(index_status.name, index);
            }
            group_snapshot.* = .{
                .status = owned_status,
                .authority = authority,
            };
            return group_snapshot;
        }

        fn retain(self: *@This()) void {
            _ = self.ref_count.fetchAdd(1, .acq_rel);
        }

        fn release(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
            runTestReadGroupRetirementHook();
            self.authority.deinit(alloc);
            self.status.deinit(alloc);
            alloc.destroy(self);
        }

        fn applyAuthority(
            self: *const @This(),
            alloc: std.mem.Allocator,
            status: *LocalTableRuntimeStatus,
        ) !void {
            status.metadata.target_observation_complete = self.authority.target_observation_complete.load(.acquire);
            for (status.stats.indexes, self.authority.indexes) |*index_status, *index_authority| {
                index_status.runtime_target_observation_complete = index_authority.target_observation_complete.load(.acquire);
                index_status.runtime_observation_stale = index_authority.observation_stale.load(.acquire);
                index_status.runtime_observation_serviceable = index_authority.observation_serviceable.load(.acquire);
                index_status.runtime_observation_targeted_sibling = index_authority.targeted_sibling.load(.acquire);
                if (index_authority.terminalFailure()) |failure|
                    try projectIndexActivationFailure(alloc, index_status, failure);
            }
        }
    };

    /// Immutable topology whose group payloads are independent ref-counted COW
    /// snapshots. Ordinary group publication swaps one slot; only a genuine
    /// topology change constructs and publishes a replacement view.
    const ReadView = struct {
        ref_count: std.atomic.Value(usize) = .init(1),
        groups: []*ReadGroupSnapshot,
        group_by_id: std.AutoHashMapUnmanaged(u64, usize) = .empty,

        fn init(alloc: std.mem.Allocator, statuses: LocalTableRuntimeStatuses) !*@This() {
            const view = try alloc.create(@This());
            errdefer alloc.destroy(view);
            const groups = try alloc.alloc(*ReadGroupSnapshot, statuses.items.len);
            var initialized: usize = 0;
            errdefer {
                for (groups[0..initialized]) |group| group.release(alloc);
                alloc.free(groups);
            }
            var group_by_id = std.AutoHashMapUnmanaged(u64, usize).empty;
            errdefer group_by_id.deinit(alloc);
            try group_by_id.ensureTotalCapacity(alloc, @intCast(statuses.items.len));
            for (statuses.items, 0..) |status, group_index| {
                groups[group_index] = try ReadGroupSnapshot.init(alloc, status);
                group_by_id.putAssumeCapacityNoClobber(status.group_id, group_index);
                initialized += 1;
            }
            view.* = .{
                .groups = groups,
                .group_by_id = group_by_id,
            };
            return view;
        }

        fn retain(self: *@This()) void {
            _ = self.ref_count.fetchAdd(1, .acq_rel);
        }

        fn release(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
            for (self.groups) |group| group.release(alloc);
            alloc.free(self.groups);
            self.group_by_id.deinit(alloc);
            alloc.destroy(self);
        }

        fn groupPosition(self: *const @This(), group_id: u64) ?usize {
            return self.group_by_id.get(group_id);
        }
    };

    const RetiredReadView = struct {
        name: []const u8,
        view: *ReadView,

        fn deinit(self: @This(), alloc: std.mem.Allocator) void {
            alloc.free(@constCast(self.name));
            self.view.release(alloc);
        }
    };

    pub const IndexIdentity = struct {
        index_name: []const u8,
        kind: db_mod.types.IndexKind,
        incarnation: u64,
        config_hash: u64,
    };

    /// Desired catalog authority for one targeted structural transition.
    /// Runtime observations validate this value; they never define it.
    pub const TargetedIndexExpectation = union(enum) {
        absent,
        exact: IndexIdentity,
    };

    pub const TableEpoch = struct {
        invalidation_epoch: u64,
        root_generation: u64,
    };

    pub const PublicationToken = struct {
        table_epoch: TableEpoch,
        observation_generation: u64,
        target_observation_revision: u64,
    };

    pub const TargetedIndexTransitionToken = struct {
        // Root replacement invalidates every target-local authority token.
        root_generation: u64,
        // Globally unique, so name reuse and overlapping DDL cannot alias.
        revision: u64,
    };

    pub const PublishResult = enum {
        published,
        stale_table,
        stale_observation,
    };

    pub const RecordTerminalFailureResult = enum {
        recorded,
        superseded,
        storage_failure,
    };

    pub const CatalogToken = struct {
        alloc: std.mem.Allocator,
        topology_revision: u64,
        complete_catalog: bool,
        observation_generation: u64,
        target_observation_revision: u64,
        table_epochs: std.StringHashMapUnmanaged(TableEpoch) = .empty,

        pub fn deinit(self: *@This()) void {
            var it = self.table_epochs.keyIterator();
            while (it.next()) |name| self.alloc.free(@constCast(name.*));
            self.table_epochs.deinit(self.alloc);
            self.* = undefined;
        }
    };

    pub const RefreshResult = struct {
        alloc: std.mem.Allocator,
        published_tables: usize = 0,
        removed_tables: usize = 0,
        removals_deferred: bool = false,
        rejected_tables: std.ArrayListUnmanaged([]u8) = .empty,

        pub fn deinit(self: *@This()) void {
            for (self.rejected_tables.items) |name| self.alloc.free(name);
            self.rejected_tables.deinit(self.alloc);
            self.* = undefined;
        }

        pub fn hasRejectedTables(self: *const @This()) bool {
            return self.rejected_tables.items.len != 0;
        }
    };

    const TableState = struct {
        const TargetObservationRequirement = struct {
            // Cache-local event ordering prevents an observation which began
            // before a commit notification from clearing its fence.
            event_revision: u64,
            // Durable DB replay ordering deduplicates repeated notifications
            // and proves that the sampled source target includes the commit.
            source_target_sequence: u64,
        };

        ref_count: std.atomic.Value(usize) = .init(1),
        mutation_mutex: std.Io.Mutex = .init,
        epoch: TableEpoch,
        groups: std.AutoHashMapUnmanaged(u64, LocalTableRuntimeStatus) = .empty,
        // Latest commit watermark that each group must have observed before
        // its coverage may be treated as current.
        required_target_observation_revisions: std.AutoHashMapUnmanaged(u64, TargetObservationRequirement) = .empty,
        // Current catalog authority by index name. Settled live identities
        // remain as watermarks; settled deletion entries are reclaimed. The
        // map is therefore bounded by live indexes plus active transitions,
        // rather than by historical DDL names.
        index_authorities: std.StringHashMapUnmanaged(TargetedIndexAuthority) = .empty,
        // Keep the normal publication path O(indexes in the observation), not
        // O(historical index names), when no structural transition is active.
        active_index_transition_count: usize = 0,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            var it = self.groups.valueIterator();
            while (it.next()) |status| status.deinit(alloc);
            self.groups.deinit(alloc);
            self.required_target_observation_revisions.deinit(alloc);
            var fence_it = self.index_authorities.iterator();
            while (fence_it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                entry.value_ptr.deinit(alloc);
            }
            self.index_authorities.deinit(alloc);
            self.* = undefined;
        }

        fn retain(self: *@This()) void {
            _ = self.ref_count.fetchAdd(1, .acq_rel);
        }

        fn release(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
            self.deinit(alloc);
            alloc.destroy(self);
        }
    };

    alloc: std.mem.Allocator,
    read_view_alloc: std.mem.Allocator,
    // Ordinary writers serialize only with mutations for the same table while
    // retaining a shared lifecycle lease. Catalog-wide replacement/clear uses
    // the exclusive side. This keeps stable heap-owned table state alive while
    // group payloads are prepared off the main cache lock without convoying
    // unrelated tables behind inference-sized clone/allocation work.
    mutation_barrier: std.Io.RwLock = .init,
    mutex: std.atomic.Mutex = .unlocked,
    read_view_mutex: std.atomic.Mutex = .unlocked,
    topology_revision: u64 = 1,
    next_invalidation_epoch: u64 = 1,
    next_observation_generation: u64 = 1,
    next_targeted_index_transition_revision: u64 = 1,
    target_observation_revision: u64 = 1,
    tables: std.StringHashMapUnmanaged(*TableState) = .empty,
    read_views: std.StringHashMapUnmanaged(*ReadView) = .empty,

    pub fn init(alloc: std.mem.Allocator) @This() {
        return .{ .alloc = alloc, .read_view_alloc = alloc };
    }

    fn lockExistingTableMutation(self: *@This(), table_name: []const u8) ?*TableState {
        self.mutation_barrier.lockSharedUncancelable(std.Options.debug_io);
        lockAtomic(&self.mutex);
        const state = self.tables.get(table_name);
        if (state) |value| value.retain();
        self.mutex.unlock();
        self.mutation_barrier.unlockShared(std.Options.debug_io);
        if (state == null) {
            return null;
        }
        state.?.mutation_mutex.lockUncancelable(std.Options.debug_io);
        self.mutation_barrier.lockSharedUncancelable(std.Options.debug_io);
        lockAtomic(&self.mutex);
        const still_current = self.tables.get(table_name) == state.?;
        self.mutex.unlock();
        if (!still_current) {
            self.mutation_barrier.unlockShared(std.Options.debug_io);
            state.?.mutation_mutex.unlock(std.Options.debug_io);
            state.?.release(self.alloc);
            return null;
        }
        return state.?;
    }

    fn lockEnsuredTableMutation(self: *@This(), table_name: []const u8) !*TableState {
        while (true) {
            self.mutation_barrier.lockSharedUncancelable(std.Options.debug_io);
            lockAtomic(&self.mutex);
            const state = self.ensureTableLocked(table_name) catch |err| {
                self.mutex.unlock();
                self.mutation_barrier.unlockShared(std.Options.debug_io);
                return err;
            };
            state.retain();
            self.mutex.unlock();
            self.mutation_barrier.unlockShared(std.Options.debug_io);
            state.mutation_mutex.lockUncancelable(std.Options.debug_io);
            self.mutation_barrier.lockSharedUncancelable(std.Options.debug_io);
            lockAtomic(&self.mutex);
            const still_current = self.tables.get(table_name) == state;
            self.mutex.unlock();
            if (still_current) return state;
            self.mutation_barrier.unlockShared(std.Options.debug_io);
            state.mutation_mutex.unlock(std.Options.debug_io);
            state.release(self.alloc);
        }
    }

    fn unlockTableMutation(self: *@This(), state: *TableState) void {
        state.mutation_mutex.unlock(std.Options.debug_io);
        self.mutation_barrier.unlockShared(std.Options.debug_io);
        state.release(self.alloc);
    }

    pub fn deinit(self: *@This()) void {
        self.mutation_barrier.lockUncancelable(std.Options.debug_io);
        lockAtomic(&self.mutex);
        self.clearTablesLocked();
        self.tables.deinit(self.alloc);
        self.mutex.unlock();
        self.clearReadViews();
        lockAtomic(&self.read_view_mutex);
        self.read_views.deinit(self.read_view_alloc);
        self.read_view_mutex.unlock();
        self.mutation_barrier.unlock(std.Options.debug_io);
        self.* = undefined;
    }

    pub fn clear(self: *@This()) void {
        self.mutation_barrier.lockUncancelable(std.Options.debug_io);
        defer self.mutation_barrier.unlock(std.Options.debug_io);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();
        self.clearTablesLocked();
    }

    pub fn invalidateTable(self: *@This(), table_name: []const u8) void {
        const mutation_state = self.lockEnsuredTableMutation(table_name) catch {
            self.clear();
            return;
        };
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();

        const state = mutation_state;
        self.clearGroupsLocked(state);
        self.clearIndexAuthoritiesLocked(state);
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
        state.epoch.root_generation +%= 1;
        if (state.epoch.root_generation == 0) state.epoch.root_generation = 1;
        self.removeReadView(table_name);
    }

    /// Fence observations captured before an in-place, index-targeted catalog
    /// mutation without discarding the last published status for unaffected
    /// sibling indexes. The storage root did not change, so retaining those
    /// immutable observations is safe; subsequent publication still uses the
    /// new epoch and rejects every pre-mutation token.
    pub fn fenceTablePublications(self: *@This(), table_name: []const u8) void {
        const mutation_state = self.lockEnsuredTableMutation(table_name) catch {
            self.clear();
            return;
        };
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();

        const state = mutation_state;
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
    }

    /// Applies a durable repair visibility edge without consulting external
    /// activity state. If the exact index is already protected by a targeted
    /// mutation fence, preserve sibling authority and stale only that target.
    /// Unknown or unrelated repair scope invalidates the table conservatively.
    /// The match and epoch transition occur under one cache lock, so mutation
    /// lease release cannot race between classification and publication.
    pub fn fenceIndexRepairPublications(
        self: *@This(),
        table_name: []const u8,
        index_name: ?[]const u8,
    ) bool {
        const mutation_state = self.lockEnsuredTableMutation(table_name) catch {
            self.clear();
            return false;
        };
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();

        const state = mutation_state;
        const target = index_name orelse {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        const fence = state.index_authorities.getPtr(target) orelse {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        if (!fence.owner_active) {
            // A repair discovered after structural ownership ended is its own
            // authority transition. Retiring DDL tokens must not be able to
            // arm, acknowledge, or release this repair boundary.
            fence.transition_revision = self.takeTargetedIndexTransitionRevisionLocked();
        }
        // A repair edge is a new authority boundary even when the structural
        // owner has already handed off an earlier observation. Re-arm the
        // exact target under the same cache lock used to classify its scope;
        // otherwise an edge racing reservation release can leave the target's
        // formerly-authoritative snapshot visible until an unrelated refresh.
        fence.accept_target_after_observation_generation = self.next_observation_generation;
        if (!fence.transition_active) state.active_index_transition_count += 1;
        fence.transition_active = true;
        fence.target_authority_handed_off = false;
        fence.handoff_groups.clearRetainingCapacity();
        fence.terminal_failures.clearRetainingCapacity();
        fence.handoff_groups.ensureTotalCapacity(self.alloc, @intCast(state.groups.count())) catch {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        resetExpectedHandoffGroupsFromSnapshotLocked(fence, state, self.alloc) catch {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        if (!fence.owner_active) {
            fence.release_after_observation_generation = self.next_observation_generation;
        }
        var status_it = state.groups.valueIterator();
        while (status_it.next()) |status| {
            for (status.stats.indexes) |*item| {
                if (std.mem.eql(u8, item.name, target)) item.runtime_observation_stale = true;
            }
        }
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
        self.syncReadViewAuthorityLocked(table_name, state);
        return true;
    }

    /// Starts an index-local publication fence for an in-place catalog
    /// mutation. The target's cached observation is persistently stale from
    /// this point forward; untouched sibling observations may remain
    /// authoritative while a current-token writer reports table-level
    /// opening/catch-up metadata.
    pub fn fenceTargetedIndexPublications(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
    ) ?TargetedIndexTransitionToken {
        const mutation_state = self.lockEnsuredTableMutation(table_name) catch {
            self.clear();
            return null;
        };
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();

        const state = mutation_state;
        const transition_revision = self.takeTargetedIndexTransitionRevisionLocked();
        if (state.index_authorities.getPtr(index_name)) |fence| {
            if (!fence.transition_active) state.active_index_transition_count += 1;
            fence.transition_revision = transition_revision;
            fence.owner_active = true;
            fence.transition_active = true;
            fence.accept_target_after_observation_generation = self.next_observation_generation;
            fence.expectation = .unknown;
            fence.expectation_observation_generation = self.next_observation_generation;
            fence.target_authority_handed_off = false;
            fence.handoff_groups.clearRetainingCapacity();
            fence.terminal_failures.clearRetainingCapacity();
            fence.release_after_observation_generation = null;
            fence.convergence_requirements.clearRetainingCapacity();
        } else {
            const owned_name = self.alloc.dupe(u8, index_name) catch {
                self.clearGroupsLocked(state);
                self.clearIndexAuthoritiesLocked(state);
                state.epoch.invalidation_epoch = self.next_invalidation_epoch;
                return null;
            };
            state.index_authorities.put(self.alloc, owned_name, .{
                .transition_revision = transition_revision,
                .accept_target_after_observation_generation = self.next_observation_generation,
                .expectation_observation_generation = self.next_observation_generation,
            }) catch {
                self.alloc.free(owned_name);
                self.clearGroupsLocked(state);
                self.clearIndexAuthoritiesLocked(state);
                state.epoch.invalidation_epoch = self.next_invalidation_epoch;
                return null;
            };
            state.active_index_transition_count += 1;
        }
        const authority = state.index_authorities.getPtr(index_name).?;
        authority.handoff_groups.ensureTotalCapacity(self.alloc, @intCast(state.groups.count())) catch {
            self.invalidateTableStateLocked(table_name, state);
            return null;
        };
        resetExpectedHandoffGroupsFromSnapshotLocked(authority, state, self.alloc) catch {
            self.invalidateTableStateLocked(table_name, state);
            return null;
        };
        var status_it = state.groups.valueIterator();
        while (status_it.next()) |status| {
            for (status.stats.indexes) |*item| {
                if (std.mem.eql(u8, item.name, index_name)) item.runtime_observation_stale = true;
            }
        }
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
        self.syncReadViewAuthorityLocked(table_name, state);
        return .{
            .root_generation = state.epoch.root_generation,
            .revision = transition_revision,
        };
    }

    /// Advances the target's observation boundary after the resident writer
    /// snapshot has been captured and immediately before mutation begins.
    pub fn armTargetedIndexPublications(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
    ) bool {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return false;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return false;
        const fence = currentTargetedIndexAuthority(state, index_name, token) orelse return false;
        if (!fence.owner_active) return false;
        fence.accept_target_after_observation_generation = self.next_observation_generation;
        fence.expectation = .unknown;
        fence.expectation_observation_generation = self.next_observation_generation;
        fence.target_authority_handed_off = false;
        fence.handoff_groups.clearRetainingCapacity();
        fence.handoff_groups.ensureTotalCapacity(self.alloc, @intCast(state.groups.count())) catch {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        resetExpectedHandoffGroupsFromSnapshotLocked(fence, state, self.alloc) catch {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        fence.convergence_requirements.clearRetainingCapacity();
        var status_it = state.groups.valueIterator();
        while (status_it.next()) |status| {
            for (status.stats.indexes) |*item| {
                if (std.mem.eql(u8, item.name, index_name)) item.runtime_observation_stale = true;
            }
        }
        self.syncReadViewAuthorityLocked(table_name, state);
        return true;
    }

    /// Binds a targeted transition to the identity accepted by the durable
    /// catalog. This is deliberately separate from runtime publication: the
    /// first shard observation may be stale, incomplete, or the wrong kind and
    /// therefore cannot be allowed to choose serving authority.
    pub fn bindTargetedIndexExpectation(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
        expected: TargetedIndexExpectation,
    ) bool {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return false;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return false;
        const authority = currentTargetedIndexAuthority(state, index_name, token) orelse return false;
        if (!authority.owner_active) return false;
        const normalized: TargetedIndexAuthority.Expectation = switch (expected) {
            .absent => .absent,
            .exact => |identity| blk: {
                if (!std.mem.eql(u8, identity.index_name, index_name) or
                    identity.incarnation == 0 or identity.config_hash == 0) return false;
                break :blk .{ .exact = .{
                    .kind = identity.kind,
                    .incarnation = identity.incarnation,
                    .config_hash = identity.config_hash,
                } };
            },
        };
        if (targetExpectationsEqual(authority.expectation, normalized)) return true;
        // One coalesced activation owner may replan after the catalog changes
        // while it is queued. The latest successfully fenced catalog snapshot
        // supersedes the earlier expectation under the same operation token;
        // observations still cannot perform this transition themselves.
        authority.expectation = normalized;
        authority.expectation_observation_generation = self.next_observation_generation;
        authority.target_authority_handed_off = false;
        authority.handoff_groups.clearRetainingCapacity();
        authority.convergence_requirements.clearRetainingCapacity();
        markTargetObservationStaleLocked(state, index_name);
        self.syncReadViewAuthorityLocked(table_name, state);
        return true;
    }

    /// Binds the transition to the catalog topology captured by its owner.
    /// The set replaces the snapshot-derived fallback installed by `fence`;
    /// later cache population cannot shrink or expand this authority boundary.
    pub fn bindTargetedIndexExpectedGroups(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
        group_ids: []const u64,
    ) bool {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return false;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return false;
        const authority = currentTargetedIndexAuthority(state, index_name, token) orelse return false;
        if (!authority.owner_active) return false;
        authority.handoff_groups.ensureUnusedCapacity(self.alloc, @intCast(group_ids.len)) catch {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        authority.terminal_failures.ensureUnusedCapacity(self.alloc, @intCast(group_ids.len)) catch {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        authority.expected_handoff_groups.ensureTotalCapacity(self.alloc, @intCast(group_ids.len)) catch {
            self.invalidateTableStateLocked(table_name, state);
            return false;
        };
        authority.expected_handoff_groups.clearRetainingCapacity();
        for (group_ids) |group_id| {
            if (authority.expected_handoff_groups.contains(group_id)) continue;
            authority.expected_handoff_groups.putAssumeCapacity(group_id, {});
        }
        authority.handoff_topology_bound = true;
        var acknowledged = authority.handoff_groups.iterator();
        while (acknowledged.next()) |entry| {
            if (!authority.expected_handoff_groups.contains(entry.key_ptr.*))
                authority.handoff_groups.removeByPtr(entry.key_ptr);
        }
        var failed = authority.terminal_failures.iterator();
        while (failed.next()) |entry| {
            if (!authority.expected_handoff_groups.contains(entry.key_ptr.*))
                authority.terminal_failures.removeByPtr(entry.key_ptr);
        }
        _ = self.advanceTargetedIndexAuthorityLocked(state);
        self.syncReadViewAuthorityLocked(table_name, state);
        return true;
    }

    /// Reports completion only for the currently bound catalog identity and
    /// exact group. Control-plane dispatchers use this as their acknowledgement
    /// boundary: queue admission alone is not proof that the resident owner
    /// survived a leadership transfer and published the target.
    pub fn targetedIndexGroupAcknowledged(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        expected: TargetedIndexExpectation,
        group_id: u64,
    ) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return false;
        const authority = state.index_authorities.getPtr(index_name) orelse return false;
        const matches = switch (expected) {
            .absent => authority.expectation == .absent,
            .exact => |identity| switch (authority.expectation) {
                .exact => |actual| std.mem.eql(u8, identity.index_name, index_name) and
                    identity.kind == actual.kind and
                    identity.incarnation == actual.incarnation and
                    identity.config_hash == actual.config_hash,
                .unknown, .absent => false,
            },
        };
        return matches and authority.handoff_groups.contains(group_id);
    }

    /// Returns the serving proof attached to an exact resident-owner
    /// acknowledgement. Null means that identity/group is not acknowledged;
    /// false means it is acknowledged but has no queryable generation yet.
    pub fn targetedIndexGroupServiceability(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        expected: TargetedIndexExpectation,
        group_id: u64,
    ) ?bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return null;
        const authority = state.index_authorities.getPtr(index_name) orelse return null;
        const matches = switch (expected) {
            .absent => authority.expectation == .absent,
            .exact => |identity| switch (authority.expectation) {
                .exact => |actual| std.mem.eql(u8, identity.index_name, index_name) and
                    identity.kind == actual.kind and
                    identity.incarnation == actual.incarnation and
                    identity.config_hash == actual.config_hash,
                .unknown, .absent => false,
            },
        };
        if (!matches) return null;
        return (authority.handoff_groups.get(group_id) orelse return null).serviceable;
    }

    /// Records an exact acknowledgement returned by the resident group owner.
    /// This is the cross-process handoff channel used by a coordinator whose
    /// private cache does not receive the resident's local publications.
    pub fn acknowledgeTargetedIndexGroup(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
        expected: TargetedIndexExpectation,
        group_id: u64,
        serviceable: bool,
    ) bool {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return false;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return false;
        const authority = currentTargetedIndexAuthority(state, index_name, token) orelse return false;
        if (!authority.owner_active) return false;
        const matches = switch (expected) {
            .absent => authority.expectation == .absent,
            .exact => |identity| switch (authority.expectation) {
                .exact => |actual| std.mem.eql(u8, identity.index_name, index_name) and
                    identity.kind == actual.kind and
                    identity.incarnation == actual.incarnation and
                    identity.config_hash == actual.config_hash,
                .unknown, .absent => false,
            },
        };
        const group_in_scope = if (authority.handoff_topology_bound)
            authority.expected_handoff_groups.contains(group_id)
        else
            state.groups.contains(group_id);
        if (!matches or !group_in_scope or
            (targetExpectationIsAbsent(authority.expectation) and
                authority.terminal_failures.contains(group_id))) return false;
        authority.handoff_groups.ensureUnusedCapacity(self.alloc, 1) catch return false;
        recordTargetGroupAcknowledgement(authority, group_id, serviceable);
        if (self.advanceTargetedIndexAuthorityLocked(state))
            self.syncReadViewAuthorityLocked(table_name, state)
        else
            self.syncReadGroupAuthorityLocked(table_name, state, group_id);
        return true;
    }

    /// Publishes an operator-actionable result for one exact activation owner.
    /// The error is authority, not activity: it survives heartbeat loss and
    /// cache-only refreshes, but it is scoped to the transition revision,
    /// desired incarnation, and resident group which reported it. Starting a
    /// newer targeted transition clears the acknowledgement and its error.
    pub fn recordTargetedIndexTerminalFailure(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
        expected: TargetedIndexExpectation,
        group_id: u64,
        code: IndexActivationFailureCode,
    ) RecordTerminalFailureResult {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return .superseded;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return .superseded;
        const authority = currentTargetedIndexAuthority(state, index_name, token) orelse return .superseded;
        if (!authority.owner_active) return .superseded;
        const matches = switch (expected) {
            .absent => authority.expectation == .absent,
            .exact => |exact_identity| switch (authority.expectation) {
                .exact => |actual| std.mem.eql(u8, exact_identity.index_name, index_name) and
                    exact_identity.kind == actual.kind and
                    exact_identity.incarnation == actual.incarnation and
                    exact_identity.config_hash == actual.config_hash,
                .unknown, .absent => false,
            },
        };
        const group_in_scope = if (authority.handoff_topology_bound)
            authority.expected_handoff_groups.contains(group_id)
        else
            state.groups.contains(group_id);
        if (!matches or !group_in_scope) return .superseded;
        if (!authority.handoff_topology_bound) {
            authority.handoff_groups.ensureUnusedCapacity(self.alloc, 1) catch return .storage_failure;
            authority.terminal_failures.ensureUnusedCapacity(self.alloc, 1) catch return .storage_failure;
        }
        const terminal_absence = targetExpectationIsAbsent(authority.expectation);
        const revoked_handoff = terminal_absence and authority.target_authority_handed_off;
        if (terminal_absence) {
            // A terminal drop result is not evidence that the desired absence
            // exists. Revoke any earlier same-transition absence reduction and
            // prevent later refreshes from silently converting this terminal
            // owner result into success. Only a new transition may supersede
            // it.
            authority.target_authority_handed_off = false;
            _ = authority.handoff_groups.remove(group_id);
        } else {
            recordTargetGroupAcknowledgement(authority, group_id, false);
        }
        authority.terminal_failures.putAssumeCapacity(group_id, .{
            .expectation = authority.expectation,
            .code = code,
        });
        if (revoked_handoff) markTargetObservationStaleLocked(state, index_name);
        if (revoked_handoff or self.advanceTargetedIndexAuthorityLocked(state))
            self.syncReadViewAuthorityLocked(table_name, state)
        else
            self.syncReadGroupAuthorityLocked(table_name, state, group_id);
        return .recorded;
    }

    /// Requests release after the named mutation's last synchronous/queued
    /// owner finishes. The fence remains active until a subsequent fresh
    /// table publication performs the authority handoff; structural work can
    /// finish before the target's asynchronous generation catch-up.
    pub fn releaseTargetedIndexPublications(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
    ) void {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return;
        const fence = currentTargetedIndexAuthority(state, index_name, token) orelse return;
        if (!fence.owner_active) return;
        fence.owner_active = false;
        fence.release_after_observation_generation = self.next_observation_generation;
        _ = self.advanceTargetedIndexAuthorityLocked(state);
        self.settleReleasedIndexAuthoritiesLocked(state);
        self.syncReadViewAuthorityLocked(table_name, state);
    }

    /// Returns true only when the exact transition has already installed its
    /// catalog identity across every observed group. Callers may treat a stale
    /// publication as benign only after this proof; a merely newer generic
    /// observation is not an activation acknowledgement.
    pub fn targetedIndexAuthorityHandedOff(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
    ) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return false;
        const authority = currentTargetedIndexAuthority(state, index_name, token) orelse return false;
        return authority.target_authority_handed_off;
    }

    /// Records an authoritative structural acknowledgement that the named
    /// target is absent from the desired catalog. This does not manufacture a
    /// fresh observation: existing same-name runtime rows remain fenced until
    /// a subsequent fresh publication also proves their absence.
    pub fn acknowledgeTargetedIndexAbsence(
        self: *@This(),
        table_name: []const u8,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
    ) void {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return;
        const fence = currentTargetedIndexAuthority(state, index_name, token) orelse return;
        if (!fence.owner_active) return;
        // Desired identity is write-once for a transition. A successful create
        // acknowledgement cannot subsequently be converted into absence by a
        // delayed drop callback carrying the same operation token.
        if (fence.expectation != .unknown and fence.expectation != .absent) return;
        fence.expectation = .absent;
        fence.expectation_observation_generation = self.next_observation_generation;
        fence.target_authority_handed_off = false;
        fence.handoff_groups.clearRetainingCapacity();
        _ = self.advanceTargetedIndexAuthorityLocked(state);
        self.settleReleasedIndexAuthoritiesLocked(state);
        self.syncReadViewAuthorityLocked(table_name, state);
    }

    /// Captures the table lifecycle before a DB is opened or inspected.
    pub fn capturePublicationToken(self: *@This(), table_name: []const u8) !PublicationToken {
        const mutation_state = try self.lockEnsuredTableMutation(table_name);
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = mutation_state;
        return .{
            .table_epoch = state.epoch,
            .observation_generation = self.takeObservationGenerationLocked(),
            .target_observation_revision = self.target_observation_revision,
        };
    }

    /// Captures all catalog tables in one lock acquisition before refresh DB
    /// inspection begins. `table_names` need only live for this call.
    pub fn captureCatalogToken(
        self: *@This(),
        alloc: std.mem.Allocator,
        table_names: []const []const u8,
        complete_catalog: bool,
    ) !CatalogToken {
        var token: CatalogToken = .{
            .alloc = alloc,
            .topology_revision = 0,
            .complete_catalog = complete_catalog,
            .observation_generation = 0,
            .target_observation_revision = 0,
        };
        errdefer token.deinit();

        self.mutation_barrier.lockSharedUncancelable(std.Options.debug_io);
        defer self.mutation_barrier.unlockShared(std.Options.debug_io);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        token.topology_revision = self.topology_revision;
        token.observation_generation = self.takeObservationGenerationLocked();
        token.target_observation_revision = self.target_observation_revision;
        // A complete refresh is also authoritative for tables absent from the
        // catalog. Capture cached epochs as removal candidates so publication
        // can prove that an unseen table was not invalidated or recreated
        // while the catalog snapshot was being inspected.
        const cached_table_count = if (complete_catalog) self.tables.count() else 0;
        try token.table_epochs.ensureTotalCapacity(alloc, @intCast(cached_table_count + table_names.len));
        if (complete_catalog) {
            var cached_it = self.tables.iterator();
            while (cached_it.next()) |entry| {
                const owned_name = try alloc.dupe(u8, entry.key_ptr.*);
                token.table_epochs.putAssumeCapacityNoClobber(owned_name, entry.value_ptr.*.epoch);
            }
        }
        for (table_names) |table_name| {
            const state = try self.ensureTableLocked(table_name);
            if (token.table_epochs.contains(table_name)) continue;
            const owned_name = try alloc.dupe(u8, table_name);
            token.table_epochs.putAssumeCapacityNoClobber(owned_name, state.epoch);
        }
        return token;
    }

    /// Publishes one owned observation in O(indexes in that group), independent
    /// of table group count and historical mutation count. The status is cloned
    /// before locking so DBStats ownership never crosses the caller/cache boundary.
    pub fn publishGroup(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        status: LocalTableRuntimeStatus,
    ) !PublishResult {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return .stale_table;
        defer self.unlockTableMutation(mutation_state);
        var owned = try status.clone(self.alloc);
        var owned_transferred = false;
        defer if (!owned_transferred) owned.deinit(self.alloc);
        var incoming_lookup = try IndexObservationLookup.init(self.alloc, status.stats.indexes);
        defer incoming_lookup.deinit(self.alloc);
        owned.cache_observation_generation = token.observation_generation;
        const now_ns = platform_time.monotonicNs();
        owned.withMetadataDefaults(.live_writer_publish, now_ns);

        // Pin the exact previous payload with mutation ownership, then perform
        // all merge cloning/allocation while the global cache mutex is free.
        lockAtomic(&self.mutex);
        const initial_state = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return .stale_table;
        };
        if (!std.meta.eql(initial_state.epoch, token.table_epoch)) {
            self.mutex.unlock();
            return .stale_table;
        }
        initial_state.groups.ensureUnusedCapacity(self.alloc, 1) catch |err| {
            self.mutex.unlock();
            return err;
        };
        reserveTargetAcknowledgementCapacityLocked(initial_state, 1, self.alloc) catch |err| {
            self.mutex.unlock();
            return err;
        };
        const previous = initial_state.groups.getPtr(status.group_id);
        const previous_generation = if (previous) |value| value.cache_observation_generation else null;
        if (previous_generation) |generation| {
            if (generation > token.observation_generation) {
                self.mutex.unlock();
                return .stale_observation;
            }
        }
        self.mutex.unlock();

        try self.mergeRefreshStatusLocked(
            previous,
            &owned,
            now_ns,
            &initial_state.index_authorities,
            initial_state.active_index_transition_count != 0,
            &incoming_lookup,
        );
        const prepared = ReadGroupSnapshot.init(self.read_view_alloc, owned) catch null;
        var prepared_owned = prepared != null;
        defer if (prepared_owned) prepared.?.release(self.read_view_alloc);
        var retired_status: ?LocalTableRuntimeStatus = null;
        defer if (retired_status) |*retired| retired.deinit(self.alloc);
        var retired_read_group: ?*ReadGroupSnapshot = null;
        defer if (retired_read_group) |retired| retired.release(self.read_view_alloc);
        var needs_topology_refresh = false;

        lockAtomic(&self.mutex);
        const state = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return .stale_table;
        };
        if (!std.meta.eql(state.epoch, token.table_epoch)) {
            self.mutex.unlock();
            return .stale_table;
        }
        const current = state.groups.getPtr(status.group_id);
        if (previous_generation) |expected_generation| {
            const value = current orelse {
                self.mutex.unlock();
                return error.RuntimeStatusPublicationContended;
            };
            if (value.cache_observation_generation != expected_generation) {
                self.mutex.unlock();
                return error.RuntimeStatusPublicationContended;
            }
        } else if (current != null) {
            self.mutex.unlock();
            return error.RuntimeStatusPublicationContended;
        }
        self.applyTargetObservationAuthorityLocked(
            state,
            status.group_id,
            &owned,
            token.target_observation_revision,
        );
        if (current) |value| {
            retired_status = value.*;
            value.* = owned;
        } else {
            state.groups.putAssumeCapacityNoClobber(status.group_id, owned);
        }
        owned_transferred = true;
        const published_status = state.groups.getPtr(status.group_id).?;
        self.enforceIndexAuthoritiesInStatusLocked(state, published_status);
        recordGroupTargetAuthorityAcknowledgementsLocked(
            state,
            published_status,
            status.stats.indexes,
            &incoming_lookup,
        );
        const completed_global_handoff = self.advanceTargetedIndexAuthorityLocked(state);
        self.settleReleasedIndexAuthoritiesLocked(state);

        if (completed_global_handoff) self.syncReadViewAuthorityLocked(table_name, state);
        if (prepared) |group| {
            group.status.cache_observation_generation = published_status.cache_observation_generation;
            group.status.metadata = published_status.metadata;
            syncReadGroupAuthorityFromStateLocked(state, group);
            lockAtomic(&self.read_view_mutex);
            if (self.read_views.get(table_name)) |view| {
                if (view.groupPosition(status.group_id)) |position| {
                    retired_read_group = view.groups[position];
                    view.groups[position] = group;
                    prepared_owned = false;
                } else {
                    needs_topology_refresh = true;
                }
            } else {
                needs_topology_refresh = true;
            }
            self.read_view_mutex.unlock();
        }
        self.mutex.unlock();

        if (needs_topology_refresh or prepared == null)
            self.refreshReadViewPrepared(table_name, token.table_epoch);
        return .published;
    }

    /// Publishes a bounded set of owned observations under one table-epoch
    /// decision. Status payloads are cloned outside the cache mutex; a short
    /// optimistic preflight reserves any missing group slots, and the final
    /// all-group commit performs no allocation or deep destruction. A newer
    /// observation for one group is preserved without rejecting valid
    /// observations for the other groups.
    pub fn publishGroups(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        statuses: []const LocalTableRuntimeStatus,
    ) !PublishResult {
        return self.publishGroupsForStructuralTarget(token, table_name, null, null, statuses);
    }

    /// Publish a targeted structural observation without allowing its bounded
    /// DB snapshot to replace stronger serving facts owned by untouched index
    /// siblings. The target name is cache-local scope, not wire metadata.
    // Internal convenience for cache-focused tests which predate operation
    // handles. Production call sites use publishTargetedGroupsForTransition.
    fn publishTargetedGroups(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        target_index_name: []const u8,
        statuses: []const LocalTableRuntimeStatus,
    ) !PublishResult {
        return self.publishGroupsForStructuralTarget(token, table_name, target_index_name, null, statuses);
    }

    /// Production structural publishers must present the mutation handle that
    /// authorized their DB work. This closes the gap where an old owner could
    /// sample after a newer fence and accidentally borrow its table epoch.
    pub fn publishTargetedGroupsForTransition(
        self: *@This(),
        token: PublicationToken,
        transition_token: TargetedIndexTransitionToken,
        table_name: []const u8,
        target_index_name: []const u8,
        statuses: []const LocalTableRuntimeStatus,
    ) !PublishResult {
        return self.publishGroupsForStructuralTarget(
            token,
            table_name,
            target_index_name,
            transition_token,
            statuses,
        );
    }

    fn validateUniqueRuntimeStatusGroups(
        self: *@This(),
        statuses: []const LocalTableRuntimeStatus,
    ) !void {
        // Group batches are supplied by topology reconciliation and are not
        // subject to a small cardinality bound. Validate them once before
        // acquiring the per-table mutation lane so a large table cannot turn
        // publication admission into quadratic serialized work.
        var group_ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer group_ids.deinit(self.alloc);
        try group_ids.ensureTotalCapacity(
            self.alloc,
            std.math.cast(u32, statuses.len) orelse return error.OutOfMemory,
        );
        for (statuses) |status| {
            const entry = group_ids.getOrPutAssumeCapacity(status.group_id);
            if (entry.found_existing) return error.DuplicateRuntimeStatusGroup;
        }
    }

    fn publishGroupsForStructuralTarget(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        transition_token: ?TargetedIndexTransitionToken,
        statuses: []const LocalTableRuntimeStatus,
    ) !PublishResult {
        try self.validateUniqueRuntimeStatusGroups(statuses);
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return .stale_table;
        defer self.unlockTableMutation(mutation_state);

        const transferred = try self.alloc.alloc(bool, statuses.len);
        defer if (transferred.len > 0) self.alloc.free(transferred);
        @memset(transferred, false);
        const publishable = try self.alloc.alloc(bool, statuses.len);
        defer if (publishable.len > 0) self.alloc.free(publishable);
        @memset(publishable, false);
        const owned = try self.alloc.alloc(LocalTableRuntimeStatus, statuses.len);
        var initialized: usize = 0;
        defer {
            for (owned[0..initialized], transferred[0..initialized]) |*status, was_transferred|
                if (!was_transferred) status.deinit(self.alloc);
            self.alloc.free(owned);
        }
        for (statuses, 0..) |status, index| {
            owned[index] = try status.clone(self.alloc);
            initialized += 1;
        }
        const incoming_lookups = try self.alloc.alloc(IndexObservationLookup, statuses.len);
        var initialized_lookups: usize = 0;
        defer {
            for (incoming_lookups[0..initialized_lookups]) |*lookup| lookup.deinit(self.alloc);
            if (incoming_lookups.len > 0) self.alloc.free(incoming_lookups);
        }
        for (statuses, 0..) |status, index| {
            incoming_lookups[index] = try IndexObservationLookup.init(self.alloc, status.stats.indexes);
            initialized_lookups += 1;
        }

        // Size the ownership-transfer merge under a brief read of the exact
        // cached generations, then allocate every workspace after unlocking.
        // The final publication lock validates these generation/count pairs
        // before it mutates any group.
        const merge_specs = try self.alloc.alloc(?IndexDeltaMergeSpec, statuses.len);
        defer if (merge_specs.len > 0) self.alloc.free(merge_specs);
        @memset(merge_specs, null);
        lockAtomic(&self.mutex);
        {
            defer self.mutex.unlock();
            const state = self.tables.get(table_name) orelse return .stale_table;
            if (!std.meta.eql(state.epoch, token.table_epoch)) return .stale_table;
            var missing_group_count: usize = 0;
            for (statuses) |status| {
                if (!state.groups.contains(status.group_id)) missing_group_count += 1;
            }
            // Reserve only the currently missing group slots before any index
            // payload is moved. A concurrent publisher may consume this spare
            // capacity after we unlock; the final preflight below detects that
            // case and asks the durable owner to retry instead of allocating
            // or rehashing in the commit section.
            try state.groups.ensureUnusedCapacity(self.alloc, @intCast(missing_group_count));
            try reserveTargetAcknowledgementCapacityLocked(state, statuses.len, self.alloc);
            for (statuses, incoming_lookups, 0..) |status, incoming_lookup, index| {
                const previous = state.groups.getPtr(status.group_id) orelse continue;
                if (previous.cache_observation_generation > token.observation_generation) continue;
                merge_specs[index] = .{
                    .previous_generation = previous.cache_observation_generation,
                    .previous_index_count = previous.stats.indexes.len,
                    .merged_index_count = mergedIndexObservationCount(
                        previous.*,
                        incoming_lookup,
                        target_index_name,
                    ),
                };
            }
        }
        const merge_workspaces = try self.alloc.alloc(?IndexDeltaMergeWorkspace, statuses.len);
        var initialized_workspaces: usize = 0;
        defer {
            for (merge_workspaces[0..initialized_workspaces]) |*workspace| {
                if (workspace.*) |*value| value.deinit(self.alloc);
            }
            if (merge_workspaces.len > 0) self.alloc.free(merge_workspaces);
        }
        for (merge_specs, statuses, 0..) |spec, status, index| {
            merge_workspaces[index] = if (spec) |value| try IndexDeltaMergeWorkspace.init(
                self.alloc,
                value.previous_generation,
                value.previous_index_count,
                status.stats.indexes.len,
                value.merged_index_count,
            ) else null;
            initialized_workspaces += 1;
        }

        runTestAfterStructuralPublishPreparationHook();

        var refresh_read_groups = false;
        defer if (refresh_read_groups) {
            for (statuses, publishable) |status, should_publish| {
                if (!should_publish) continue;
                self.refreshReadGroupPrepared(
                    table_name,
                    token.table_epoch,
                    status.group_id,
                    token.observation_generation,
                );
            }
        };
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return .stale_table;
        if (!std.meta.eql(state.epoch, token.table_epoch)) return .stale_table;
        if (transition_token) |transition| {
            const name = target_index_name orelse return .stale_observation;
            _ = currentTargetedIndexAuthority(state, name, transition) orelse return .stale_observation;
        }

        // Optimistic preflight is valid only for the exact cached generation.
        // A newer observation makes this token stale; any other replacement is
        // transient contention and is retried by the durable publisher rather
        // than merging against an allocation plan for a different snapshot.
        for (statuses, merge_specs) |status, spec| {
            const previous = state.groups.getPtr(status.group_id);
            if (spec) |expected| {
                const current = previous orelse return error.RuntimeStatusPublicationContended;
                if (current.cache_observation_generation > token.observation_generation) continue;
                if (current.cache_observation_generation != expected.previous_generation or
                    current.stats.indexes.len != expected.previous_index_count)
                    return error.RuntimeStatusPublicationContended;
            } else if (previous) |current| {
                if (current.cache_observation_generation <= token.observation_generation)
                    return error.RuntimeStatusPublicationContended;
            }
        }

        var new_groups: usize = 0;
        for (statuses) |status| {
            if (!state.groups.contains(status.group_id)) new_groups += 1;
        }
        const maximum_group_count = (state.groups.capacity() * std.hash_map.default_max_load_percentage) / 100;
        if (maximum_group_count - state.groups.count() < new_groups)
            return error.RuntimeStatusPublicationContended;
        // A publisher may have consumed acknowledgement-map spare capacity
        // while payload preparation ran without the cache mutex. Re-reserve
        // at the final preflight, before any ownership transfer, so the commit
        // phase remains allocation-free and `putAssumeCapacity` is sound.
        try reserveTargetAcknowledgementCapacityLocked(state, statuses.len, self.alloc);

        const now_ns = platform_time.monotonicNs();
        if (target_index_name) |name| {
            // A whole-table invalidation clears both cached groups and their
            // target fences. The current-epoch structural observation may
            // still repopulate that empty table; there is no retained target
            // authority to bind in that case.
            if (state.index_authorities.getPtr(name)) |authority| {
                if (authority.transition_active and
                    token.observation_generation >= authority.accept_target_after_observation_generation and
                    token.observation_generation >= authority.expectation_observation_generation)
                {
                    // Production publishers carry an operation token and must
                    // have bound the durable catalog identity before sampling
                    // runtime state. Only the token-free test convenience path
                    // may infer an expectation from its synthetic observation.
                    if (transition_token != null and authority.expectation == .unknown)
                        return error.MissingTargetedIndexExpectation;
                    // Decide publication eligibility before mutating the
                    // authority registry. A delayed structural observation
                    // whose every group has already been superseded cannot
                    // become the first writer of desired catalog identity.
                    const observed_expectation = (try targetedIndexExpectationForPublishableGroups(
                        state,
                        statuses,
                        name,
                        token.observation_generation,
                    )) orelse return .stale_observation;
                    switch (authority.expectation) {
                        .unknown => {
                            authority.expectation = observed_expectation;
                            authority.expectation_observation_generation = token.observation_generation;
                            authority.target_authority_handed_off = false;
                            authority.convergence_requirements.clearRetainingCapacity();
                        },
                        .absent, .exact => {
                            // Desired authority is write-once within one
                            // transition. A different later observation is
                            // evidence from a superseded owner, not a new
                            // catalog intent.
                            if (!targetExpectationsEqual(authority.expectation, observed_expectation)) {
                                return .stale_observation;
                            }
                        },
                    }
                }
            }
        }
        // Every allocation and generation validation is complete. Build each
        // per-index delta by transferring ownership into its preallocated
        // workspace; the cache remains unchanged until all groups are ready.
        for (owned, statuses, incoming_lookups, merge_workspaces, 0..) |*next, status, incoming_lookup, *workspace, owned_index| {
            next.cache_observation_generation = token.observation_generation;
            next.withMetadataDefaults(.live_writer_publish, now_ns);
            self.applyTargetObservationAuthorityLocked(
                state,
                status.group_id,
                next,
                token.target_observation_revision,
            );
            if (state.groups.getPtr(status.group_id)) |previous| {
                if (previous.cache_observation_generation > token.observation_generation) {
                    continue;
                }
                const merge_workspace = &(workspace.* orelse unreachable);
                moveIndexObservationDeltaLocked(
                    previous,
                    next,
                    incoming_lookup,
                    &state.index_authorities,
                    target_index_name,
                    merge_workspace,
                );
                try preserveArtifactVisibilityUsingLookup(
                    self.alloc,
                    previous,
                    next,
                    &state.index_authorities,
                    state.active_index_transition_count != 0,
                    target_index_name,
                    merge_workspace.previous_lookup,
                    true,
                );
            }
            publishable[owned_index] = true;
        }

        var published = false;
        for (owned, statuses, publishable, merge_workspaces, 0..) |*next, status, should_publish, *workspace, owned_index| {
            if (!should_publish) continue;
            if (state.groups.getPtr(status.group_id)) |previous| {
                finishMovedIndexObservationDeltaLocked(previous, next, &(workspace.* orelse unreachable));
            } else {
                state.groups.putAssumeCapacity(status.group_id, next.*);
                next.* = undefined;
            }
            self.enforceIndexAuthoritiesInStatusLocked(state, state.groups.getPtr(status.group_id).?);
            recordGroupTargetAuthorityAcknowledgementsLocked(
                state,
                state.groups.getPtr(status.group_id).?,
                status.stats.indexes,
                &incoming_lookups[owned_index],
            );
            transferred[owned_index] = true;
            published = true;
        }
        const completed_global_handoff = self.advanceTargetedIndexAuthorityLocked(state);
        if (completed_global_handoff) self.syncReadViewAuthorityLocked(table_name, state);
        self.settleReleasedIndexAuthoritiesLocked(state);
        refresh_read_groups = true;
        return if (published or statuses.len == 0) .published else .stale_observation;
    }

    /// Atomically replaces a table's visible observations while advancing its
    /// lifecycle epoch. Use this when a durable structural transition makes
    /// every observation from the preceding epoch unsafe to republish.
    pub fn publishLifecycleTransition(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        statuses: []const LocalTableRuntimeStatus,
    ) !PublishResult {
        try self.validateUniqueRuntimeStatusGroups(statuses);
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return .stale_table;
        defer self.unlockTableMutation(mutation_state);

        var replacement = std.AutoHashMapUnmanaged(u64, LocalTableRuntimeStatus).empty;
        var replacement_owned = true;
        defer if (replacement_owned) {
            var it = replacement.valueIterator();
            while (it.next()) |status| status.deinit(self.alloc);
            replacement.deinit(self.alloc);
        };
        try replacement.ensureTotalCapacity(self.alloc, @intCast(statuses.len));
        for (statuses) |status| {
            const owned = try status.clone(self.alloc);
            replacement.putAssumeCapacityNoClobber(status.group_id, owned);
        }

        var prepared_read_view: ?*ReadView = if (statuses.len == 0)
            null
        else
            ReadView.init(self.read_view_alloc, .{ .items = @constCast(statuses) }) catch null;
        var prepared_read_owned = prepared_read_view != null;
        defer if (prepared_read_owned) prepared_read_view.?.release(self.read_view_alloc);
        var prepared_read_name = if (prepared_read_view != null)
            self.read_view_alloc.dupe(u8, table_name) catch null
        else
            null;
        if (prepared_read_view != null and prepared_read_name == null) {
            prepared_read_view.?.release(self.read_view_alloc);
            prepared_read_view = null;
            prepared_read_owned = false;
        }
        var prepared_read_name_owned = prepared_read_name != null;
        defer if (prepared_read_name_owned) self.read_view_alloc.free(prepared_read_name.?);
        if (prepared_read_view != null) {
            lockAtomic(&self.read_view_mutex);
            self.read_views.ensureUnusedCapacity(self.read_view_alloc, 1) catch {
                self.read_view_mutex.unlock();
                prepared_read_view.?.release(self.read_view_alloc);
                prepared_read_view = null;
                prepared_read_owned = false;
                self.read_view_alloc.free(prepared_read_name.?);
                prepared_read_name = null;
                prepared_read_name_owned = false;
            };
            if (prepared_read_view != null) self.read_view_mutex.unlock();
        }

        var retired_read_view: ?*ReadView = null;
        var retired_read_name: ?[]const u8 = null;
        defer {
            if (retired_read_name) |name| self.read_view_alloc.free(@constCast(name));
            if (retired_read_view) |view| view.release(self.read_view_alloc);
        }

        lockAtomic(&self.mutex);
        const state = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return .stale_table;
        };
        if (!std.meta.eql(state.epoch, token.table_epoch)) {
            self.mutex.unlock();
            return .stale_table;
        }

        const observation_generation = self.takeObservationGenerationLocked();
        const now_ns = platform_time.monotonicNs();
        var replacement_it = replacement.iterator();
        while (replacement_it.next()) |entry| {
            const status = entry.value_ptr;
            status.cache_observation_generation = observation_generation;
            status.withMetadataDefaults(.live_writer_publish, now_ns);
            self.applyTargetObservationAuthorityLocked(
                state,
                entry.key_ptr.*,
                status,
                token.target_observation_revision,
            );
        }

        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
        var retired = state.groups;
        state.groups = replacement;
        replacement = .empty;
        replacement_owned = false;
        self.clearIndexAuthoritiesLocked(state);

        if (prepared_read_view) |view| {
            for (view.groups) |group| {
                const current = state.groups.get(group.status.group_id).?;
                group.status.cache_observation_generation = current.cache_observation_generation;
                group.status.metadata = current.metadata;
                syncReadGroupAuthorityFromStateLocked(state, group);
            }
            lockAtomic(&self.read_view_mutex);
            if (self.read_views.getPtr(table_name)) |current| {
                retired_read_view = current.*;
                current.* = view;
            } else {
                self.read_views.putAssumeCapacity(prepared_read_name.?, view);
                prepared_read_name_owned = false;
            }
            prepared_read_owned = false;
            self.read_view_mutex.unlock();
        } else {
            lockAtomic(&self.read_view_mutex);
            if (self.read_views.fetchRemove(table_name)) |removed| {
                retired_read_name = removed.key;
                retired_read_view = removed.value;
            }
            self.read_view_mutex.unlock();
        }
        self.mutex.unlock();

        var retired_it = retired.valueIterator();
        while (retired_it.next()) |status| status.deinit(self.alloc);
        retired.deinit(self.alloc);
        return .published;
    }

    /// Consumes every snapshot. Epoch-valid tables publish independently;
    /// catalog-wide absence removals occur only when topology stayed stable.
    pub fn publishRefresh(
        self: *@This(),
        catalog_token: *const CatalogToken,
        snapshots: []TableRuntimeSnapshot,
    ) !RefreshResult {
        var result: RefreshResult = .{ .alloc = self.alloc };
        errdefer result.deinit();
        var next_unconsumed: usize = 0;
        defer {
            for (snapshots[next_unconsumed..]) |*snapshot_entry| snapshot_entry.deinit(self.alloc);
        }

        var seen_tables = std.StringHashMapUnmanaged(void).empty;
        defer {
            var seen_it = seen_tables.keyIterator();
            while (seen_it.next()) |name| self.alloc.free(@constCast(name.*));
            seen_tables.deinit(self.alloc);
        }
        try seen_tables.ensureTotalCapacity(self.alloc, @intCast(snapshots.len));
        const retired_read_views = try self.alloc.alloc(RetiredReadView, catalog_token.table_epochs.count());
        var retired_read_view_count: usize = 0;
        defer {
            for (retired_read_views[0..retired_read_view_count]) |retired| retired.deinit(self.read_view_alloc);
            if (retired_read_views.len > 0) self.alloc.free(retired_read_views);
        }
        for (snapshots) |snapshot_entry| {
            if (seen_tables.contains(snapshot_entry.table_name)) return error.DuplicateRuntimeStatusTable;
            const owned_name = try self.alloc.dupe(u8, snapshot_entry.table_name);
            seen_tables.putAssumeCapacityNoClobber(owned_name, {});
        }

        const now_ns = platform_time.monotonicNs();
        for (snapshots) |*snapshot_entry| {
            const expected_epoch = catalog_token.table_epochs.get(snapshot_entry.table_name);
            const state = self.lockExistingTableMutation(snapshot_entry.table_name);
            if (expected_epoch == null or state == null or !std.meta.eql(expected_epoch.?, state.?.epoch)) {
                if (state) |value| self.unlockTableMutation(value);
                const rejected_name = try self.alloc.dupe(u8, snapshot_entry.table_name);
                errdefer self.alloc.free(rejected_name);
                try result.rejected_tables.append(self.alloc, rejected_name);
                snapshot_entry.deinit(self.alloc);
                next_unconsumed += 1;
                continue;
            }

            self.publishTableRefreshLocked(
                state.?,
                snapshot_entry.table_name,
                expected_epoch.?,
                &snapshot_entry.statuses,
                catalog_token.observation_generation,
                catalog_token.target_observation_revision,
                now_ns,
            ) catch |err| {
                self.unlockTableMutation(state.?);
                return err;
            };
            self.refreshReadViewPrepared(snapshot_entry.table_name, expected_epoch.?);
            self.unlockTableMutation(state.?);
            snapshot_entry.deinit(self.alloc);
            next_unconsumed += 1;
            result.published_tables += 1;
        }

        if (!catalog_token.complete_catalog) return result;
        const RetiredTableState = struct {
            name: []const u8,
            state: *TableState,
        };
        const retired_tables = try self.alloc.alloc(RetiredTableState, catalog_token.table_epochs.count());
        var retired_table_count: usize = 0;
        defer {
            for (retired_tables[0..retired_table_count]) |retired| {
                self.alloc.free(@constCast(retired.name));
                retired.state.release(self.alloc);
            }
            if (retired_tables.len > 0) self.alloc.free(retired_tables);
        }

        // Only catalog absence/topology commitment is global. All expensive
        // per-table merge, clone, and read-view construction above ran through
        // exact table ownership while unrelated publishers remained runnable.
        self.mutation_barrier.lockUncancelable(std.Options.debug_io);
        lockAtomic(&self.mutex);
        if (catalog_token.topology_revision != self.topology_revision) {
            self.mutex.unlock();
            self.mutation_barrier.unlock(std.Options.debug_io);
            result.removals_deferred = true;
            return result;
        }

        var advanced_invalidation_epoch = false;
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            if (seen_tables.contains(entry.key_ptr.*)) continue;
            const expected_epoch = catalog_token.table_epochs.get(entry.key_ptr.*) orelse continue;
            const retired_state = entry.value_ptr.*;
            if (!std.meta.eql(expected_epoch, retired_state.epoch)) continue;
            if (!advanced_invalidation_epoch) {
                self.advanceInvalidationEpochLocked();
                advanced_invalidation_epoch = true;
            }
            lockAtomic(&self.read_view_mutex);
            if (self.read_views.fetchRemove(entry.key_ptr.*)) |removed| {
                retired_read_views[retired_read_view_count] = .{
                    .name = removed.key,
                    .view = removed.value,
                };
                retired_read_view_count += 1;
            }
            self.read_view_mutex.unlock();
            retired_tables[retired_table_count] = .{
                .name = entry.key_ptr.*,
                .state = retired_state,
            };
            retired_table_count += 1;
            self.tables.removeByPtr(entry.key_ptr);
            result.removed_tables += 1;
        }
        self.mutex.unlock();
        self.mutation_barrier.unlock(std.Options.debug_io);
        return result;
    }

    fn cloneStableTableStatuses(
        state: *const TableState,
        alloc: std.mem.Allocator,
    ) !?LocalTableRuntimeStatuses {
        if (state.groups.count() == 0) return null;
        const ordered = try alloc.alloc(*const LocalTableRuntimeStatus, state.groups.count());
        defer alloc.free(ordered);
        var ordered_len: usize = 0;
        var it = state.groups.valueIterator();
        while (it.next()) |status| : (ordered_len += 1) ordered[ordered_len] = status;
        std.mem.sort(*const LocalTableRuntimeStatus, ordered, {}, lessThanGroupIdPtr);

        const items = try alloc.alloc(LocalTableRuntimeStatus, state.groups.count());
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*status| status.deinit(alloc);
            alloc.free(items);
        }
        for (ordered) |status| {
            items[initialized] = try status.clone(alloc);
            initialized += 1;
        }
        return .{ .items = items };
    }

    fn removeReadView(self: *@This(), table_name: []const u8) void {
        var retired: ?*ReadView = null;
        lockAtomic(&self.read_view_mutex);
        if (self.read_views.fetchRemove(table_name)) |entry| {
            self.read_view_alloc.free(@constCast(entry.key));
            retired = entry.value;
        }
        self.read_view_mutex.unlock();
        if (retired) |view| view.release(self.read_view_alloc);
    }

    /// Prepare a complete topology replacement without the mutable-cache lock.
    /// The caller holds the exact table mutex and a shared lifecycle lease,
    /// so the captured table/status backing
    /// remains stable; the final lock still revalidates exact epoch and every
    /// group generation before publishing the immutable view.
    fn refreshReadViewPrepared(
        self: *@This(),
        table_name: []const u8,
        expected_epoch: TableEpoch,
    ) void {
        lockAtomic(&self.mutex);
        const initial_state = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return;
        };
        if (!std.meta.eql(initial_state.epoch, expected_epoch) or initial_state.groups.count() == 0) {
            self.mutex.unlock();
            return;
        }
        self.mutex.unlock();

        var statuses = (cloneStableTableStatuses(initial_state, self.read_view_alloc) catch return) orelse return;
        defer statuses.deinit(self.read_view_alloc);
        const prepared = ReadView.init(self.read_view_alloc, statuses) catch return;
        var prepared_owned = true;
        defer if (prepared_owned) prepared.release(self.read_view_alloc);
        const owned_name = self.read_view_alloc.dupe(u8, table_name) catch return;
        var name_owned = true;
        defer if (name_owned) self.read_view_alloc.free(owned_name);

        // Any missing table slot can now be inserted without allocating in the
        // final main-mutex section. The table lane excludes another topology
        // writer from consuming this reservation before the swap.
        lockAtomic(&self.read_view_mutex);
        self.read_views.ensureUnusedCapacity(self.read_view_alloc, 1) catch {
            self.read_view_mutex.unlock();
            return;
        };
        self.read_view_mutex.unlock();

        var retired: ?*ReadView = null;
        defer if (retired) |old| old.release(self.read_view_alloc);
        lockAtomic(&self.mutex);
        const state = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return;
        };
        if (!std.meta.eql(state.epoch, expected_epoch) or state.groups.count() != prepared.groups.len) {
            self.mutex.unlock();
            return;
        }
        for (prepared.groups) |group| {
            const current = state.groups.get(group.status.group_id) orelse {
                self.mutex.unlock();
                return;
            };
            if (current.cache_observation_generation != group.status.cache_observation_generation) {
                self.mutex.unlock();
                return;
            }
            syncReadGroupAuthorityFromStateLocked(state, group);
        }

        lockAtomic(&self.read_view_mutex);
        if (self.read_views.getPtr(table_name)) |current| {
            retired = current.*;
            current.* = prepared;
        } else {
            self.read_views.putAssumeCapacity(owned_name, prepared);
            name_owned = false;
        }
        prepared_owned = false;
        self.read_view_mutex.unlock();
        self.mutex.unlock();
    }

    /// Clone a committed group while mutable writers are serialized but the
    /// global cache mutex is available to readers/control-plane lookups. The
    /// final lock validates the exact epoch and observation generation before
    /// swapping one COW slot; all allocation and retirement happen off-lock.
    fn refreshReadGroupPrepared(
        self: *@This(),
        table_name: []const u8,
        expected_epoch: TableEpoch,
        group_id: u64,
        expected_generation: u64,
    ) void {
        lockAtomic(&self.mutex);
        const initial_state = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return;
        };
        if (!std.meta.eql(initial_state.epoch, expected_epoch)) {
            self.mutex.unlock();
            return;
        }
        const initial_status = initial_state.groups.getPtr(group_id) orelse {
            self.mutex.unlock();
            return;
        };
        if (initial_status.cache_observation_generation != expected_generation) {
            self.mutex.unlock();
            return;
        }
        // The table lane and stable heap-owned table state pin the status
        // payload while the main mutex is released for cloning.
        self.mutex.unlock();

        const prepared = ReadGroupSnapshot.init(self.read_view_alloc, initial_status.*) catch return;
        var prepared_owned = true;
        defer if (prepared_owned) prepared.release(self.read_view_alloc);
        var retired: ?*ReadGroupSnapshot = null;
        defer if (retired) |old| old.release(self.read_view_alloc);

        lockAtomic(&self.mutex);
        const state = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return;
        };
        if (!std.meta.eql(state.epoch, expected_epoch)) {
            self.mutex.unlock();
            return;
        }
        const current_status = state.groups.getPtr(group_id) orelse {
            self.mutex.unlock();
            return;
        };
        if (current_status.cache_observation_generation != expected_generation) {
            self.mutex.unlock();
            return;
        }
        syncReadGroupAuthorityFromStateLocked(state, prepared);

        lockAtomic(&self.read_view_mutex);
        if (self.read_views.get(table_name)) |view| {
            if (view.groupPosition(group_id)) |position| {
                const current = view.groups[position];
                if (current.status.cache_observation_generation <= expected_generation) {
                    view.groups[position] = prepared;
                    prepared_owned = false;
                    retired = current;
                }
                self.read_view_mutex.unlock();
                self.mutex.unlock();
                return;
            }
        }
        self.read_view_mutex.unlock();
        self.mutex.unlock();

        // No read topology exists yet (or this group is new). Build that rare
        // lifecycle replacement off-lock and revalidate every group at swap.
        self.refreshReadViewPrepared(table_name, expected_epoch);
    }

    fn retainReadView(self: *@This(), table_name: []const u8) ?*ReadView {
        lockAtomic(&self.read_view_mutex);
        defer self.read_view_mutex.unlock();
        const view = self.read_views.get(table_name) orelse return null;
        view.retain();
        return view;
    }

    fn syncReadViewAuthorityLocked(self: *@This(), table_name: []const u8, state: *const TableState) void {
        lockAtomic(&self.read_view_mutex);
        defer self.read_view_mutex.unlock();
        const view = self.read_views.get(table_name) orelse return;
        for (view.groups) |group| syncReadGroupAuthorityFromStateLocked(state, group);
    }

    fn syncReadGroupAuthorityLocked(
        self: *@This(),
        table_name: []const u8,
        state: *const TableState,
        group_id: u64,
    ) void {
        lockAtomic(&self.read_view_mutex);
        defer self.read_view_mutex.unlock();
        const view = self.read_views.get(table_name) orelse return;
        const position = view.groupPosition(group_id) orelse return;
        syncReadGroupAuthorityFromStateLocked(state, view.groups[position]);
    }

    fn syncReadGroupAuthorityFromStateLocked(
        state: *const TableState,
        group: *ReadGroupSnapshot,
    ) void {
        const status = state.groups.get(group.status.group_id) orelse return;
        const view_authority = &group.authority;
        if (!status.metadata.target_observation_complete)
            view_authority.target_observation_complete.store(false, .release);
        for (status.stats.indexes) |index_status| {
            if (comptime builtin.is_test) _ = test_authority_sync_index_visits.fetchAdd(1, .monotonic);
            const index = view_authority.index_by_name.get(index_status.name) orelse continue;
            const index_authority = &view_authority.indexes[index];
            if (!index_authority.identityMatches(index_status)) {
                index_authority.fenceIdentityMismatch();
                continue;
            }
            index_authority.store(index_status);
            const terminal_failure = if (state.index_authorities.get(index_status.name)) |authority|
                if (authority.terminal_failures.get(group.status.group_id)) |failure|
                    if (targetExpectationsEqual(authority.expectation, failure.expectation) and
                        targetExpectationAcceptsIdentity(failure.expectation, index_status))
                        failure.code
                    else
                        null
                else
                    null
            else
                null;
            index_authority.storeTerminalFailure(terminal_failure);
        }
        if (status.metadata.target_observation_complete)
            view_authority.target_observation_complete.store(true, .release);
    }

    fn markReadViewGroupTargetPendingLocked(self: *@This(), table_name: []const u8, group_id: u64) void {
        lockAtomic(&self.read_view_mutex);
        defer self.read_view_mutex.unlock();
        const view = self.read_views.get(table_name) orelse return;
        const group_index = view.groupPosition(group_id) orelse return;
        view.groups[group_index].authority.target_observation_complete.store(false, .release);
    }

    fn markReadViewIndexTargetsPendingLocked(
        self: *@This(),
        table_name: []const u8,
        group_id: u64,
        identities: anytype,
        applied_positions: []const ?usize,
    ) void {
        lockAtomic(&self.read_view_mutex);
        defer self.read_view_mutex.unlock();
        const view = self.read_views.get(table_name) orelse return;
        const group_index = view.groupPosition(group_id) orelse return;
        const group = view.groups[group_index];
        for (identities, applied_positions) |identity, applied_position| {
            if (applied_position == null) continue;
            const index = group.authority.index_by_name.get(identity.index_name) orelse continue;
            if (!indexMatchesTargetIdentity(group.status.stats.indexes[index], identity)) continue;
            group.authority.indexes[index].target_observation_complete.store(false, .release);
        }
    }

    pub fn snapshot(self: *@This(), alloc: std.mem.Allocator, table_name: []const u8) !?LocalTableRuntimeStatuses {
        if (self.retainReadView(table_name)) |view| {
            defer view.release(self.read_view_alloc);
            const retained = try alloc.alloc(*ReadGroupSnapshot, view.groups.len);
            defer alloc.free(retained);
            lockAtomic(&self.read_view_mutex);
            for (view.groups, 0..) |group, index| {
                group.retain();
                retained[index] = group;
            }
            self.read_view_mutex.unlock();
            defer for (retained) |group| group.release(self.read_view_alloc);

            const items = try alloc.alloc(LocalTableRuntimeStatus, retained.len);
            var initialized: usize = 0;
            errdefer {
                for (items[0..initialized]) |*status| status.deinit(alloc);
                alloc.free(items);
            }
            for (retained, 0..) |group, index| {
                items[index] = try group.status.clone(alloc);
                errdefer items[index].deinit(alloc);
                try group.applyAuthority(alloc, &items[index]);
                initialized += 1;
            }
            return .{ .items = items };
        }
        return null;
    }

    /// Records one group's committed target advance without changing any
    /// published serving fact. This is an O(1) commit-path watermark update
    /// under the small status-cache mutex; HTTP readers only clone the result
    /// and never consult the writer cache or DB. Exact durable sequences make
    /// duplicate/out-of-order commit delivery an idempotent no-op. A null
    /// sequence is reserved for structural invalidations with no DB owner and
    /// therefore always advances the causal event fence.
    pub fn markGroupTargetObservationPending(
        self: *@This(),
        table_name: []const u8,
        group_id: u64,
        source_target_sequence: ?u64,
    ) void {
        const mutation_state = self.lockEnsuredTableMutation(table_name) catch {
            self.clear();
            return;
        };
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = mutation_state;
        switch (self.markGroupTargetObservationPendingLocked(table_name, state, group_id, source_target_sequence)) {
            .group_applied => self.markReadViewGroupTargetPendingLocked(table_name, group_id),
            .index_applied => unreachable,
            .no_change, .state_invalidated => {},
        }
    }

    fn markGroupTargetObservationPendingLocked(
        self: *@This(),
        table_name: []const u8,
        state: *TableState,
        group_id: u64,
        source_target_sequence: ?u64,
    ) TargetObservationUpdate {
        if (source_target_sequence) |sequence| {
            if (state.required_target_observation_revisions.get(group_id)) |required| {
                if (sequence <= required.source_target_sequence) return .no_change;
            }
        }
        self.advanceTargetObservationRevisionLocked();
        const requirement = TableState.TargetObservationRequirement{
            .event_revision = self.target_observation_revision,
            .source_target_sequence = source_target_sequence orelse 0,
        };
        state.required_target_observation_revisions.put(self.alloc, group_id, requirement) catch {
            // Failure to record the convergence fence cannot leave an older
            // completion proof visible. Retire the table observation instead.
            self.invalidateTableStateLocked(table_name, state);
            return .state_invalidated;
        };
        if (state.groups.getPtr(group_id)) |status|
            status.metadata.target_observation_complete = false;
        return .group_applied;
    }

    /// Records a committed target advance for one exact index incarnation.
    /// A mismatched same-name event is stale by construction and is ignored;
    /// it must not revoke convergence for a replacement incarnation.
    pub fn markIndexTargetObservationPending(
        self: *@This(),
        table_name: []const u8,
        group_id: u64,
        identity: IndexIdentity,
        source_target_sequence: u64,
    ) void {
        const identities = [_]IndexIdentity{identity};
        self.markIndexTargetsObservationPending(
            table_name,
            group_id,
            identities[0..],
            source_target_sequence,
        );
    }

    /// Batched exact-target variant used by commit callbacks. One cache lock
    /// and one current-index lookup keep the commit notification O(indexes in
    /// the event), even when a batch affects every configured index.
    pub fn markIndexTargetsObservationPending(
        self: *@This(),
        table_name: []const u8,
        group_id: u64,
        identities: anytype,
        source_target_sequence: u64,
    ) void {
        if (identities.len == 0) return;

        // Build the event-side lookup before taking the cache mutex. Commit
        // callbacks may affect every configured index; allocating and hashing
        // that set while holding the process-wide status lock would make HTTP
        // snapshot latency depend on write fan-out.
        var event_lookup = std.StringHashMapUnmanaged(usize).empty;
        defer event_lookup.deinit(self.alloc);
        event_lookup.ensureTotalCapacity(self.alloc, @intCast(identities.len)) catch {
            self.markGroupTargetObservationPending(table_name, group_id, source_target_sequence);
            return;
        };
        const current_positions = self.alloc.alloc(?usize, identities.len) catch {
            self.markGroupTargetObservationPending(table_name, group_id, source_target_sequence);
            return;
        };
        defer self.alloc.free(current_positions);
        @memset(current_positions, null);
        for (identities, 0..) |identity, identity_index| {
            const result = event_lookup.getOrPutAssumeCapacity(identity.index_name);
            if (result.found_existing) {
                self.markGroupTargetObservationPending(table_name, group_id, source_target_sequence);
                return;
            }
            result.value_ptr.* = identity_index;
        }

        const mutation_state = self.lockEnsuredTableMutation(table_name) catch {
            self.clear();
            return;
        };
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = mutation_state;
        const current_status = state.groups.getPtr(group_id);
        if (current_status) |status| {
            for (status.stats.indexes, 0..) |item, item_index| {
                const identity_index = event_lookup.get(item.name) orelse continue;
                if (indexMatchesTargetIdentity(item, identities[identity_index]))
                    current_positions[identity_index] = item_index;
            }
        }

        var group_fallback_applied = false;
        for (identities, current_positions, 0..) |identity, current_position, identity_index| {
            switch (self.markOneIndexTargetObservationPendingLocked(
                table_name,
                state,
                current_status,
                current_position,
                group_id,
                identity,
                source_target_sequence,
            )) {
                .index_applied => {},
                .group_applied => {
                    group_fallback_applied = true;
                    current_positions[identity_index] = null;
                },
                .no_change => current_positions[identity_index] = null,
                .state_invalidated => return,
            }
        }
        if (group_fallback_applied)
            self.markReadViewGroupTargetPendingLocked(table_name, group_id)
        else
            self.markReadViewIndexTargetsPendingLocked(table_name, group_id, identities, current_positions);
    }

    fn markOneIndexTargetObservationPendingLocked(
        self: *@This(),
        table_name: []const u8,
        state: *TableState,
        current_status: ?*LocalTableRuntimeStatus,
        current_position: ?usize,
        group_id: u64,
        identity: anytype,
        source_target_sequence: u64,
    ) TargetObservationUpdate {
        const index_name = identity.index_name;

        var authority = state.index_authorities.getPtr(index_name);
        if (authority) |existing| {
            const expected = switch (existing.expectation) {
                .exact => |value| value,
                // An active structural transition owns the identity decision;
                // a data-plane callback cannot fill or overturn it.
                .unknown, .absent => return .no_change,
            };
            if (!targetIdentitiesEqual(expected, identity)) return .no_change;
        } else {
            // If status already knows this name, require the event to match
            // that exact row before creating persistent authority. This makes
            // delayed callbacks for retired incarnations harmless. Before the
            // first runtime snapshot exists, the commit event's durable exact
            // identity is itself the authority; do not widen known scope and
            // unnecessarily fence sibling convergence.
            if (current_status != null and current_position == null) return .no_change;
            const owned_name = self.alloc.dupe(u8, index_name) catch {
                return self.markGroupTargetObservationPendingLocked(table_name, state, group_id, source_target_sequence);
            };
            state.index_authorities.put(self.alloc, owned_name, .{
                .transition_revision = 0,
                .transition_active = false,
                .owner_active = false,
                .accept_target_after_observation_generation = 0,
                .expectation = .{ .exact = .{
                    .kind = identity.kind,
                    .incarnation = identity.incarnation,
                    .config_hash = identity.config_hash,
                } },
                .expectation_observation_generation = 0,
                .target_authority_handed_off = true,
            }) catch {
                self.alloc.free(owned_name);
                return self.markGroupTargetObservationPendingLocked(table_name, state, group_id, source_target_sequence);
            };
            authority = state.index_authorities.getPtr(owned_name).?;
        }

        var serving_set_may_reduce =
            if (@hasField(@TypeOf(identity), "serving_set_effect"))
                switch (identity.serving_set_effect) {
                    .additive_only => false,
                    .may_reduce => true,
                }
            else
                true;
        if (authority.?.convergence_requirements.get(group_id)) |required| {
            if (source_target_sequence < required.source_target_sequence) return .no_change;
            const previous_still_pending = if (current_status) |status|
                if (current_position) |position|
                    !status.stats.indexes[position].runtime_target_observation_complete
                else
                    true
            else
                true;
            if (previous_still_pending and required.serving_set_may_reduce)
                serving_set_may_reduce = true;
            if (source_target_sequence == required.source_target_sequence and
                serving_set_may_reduce == required.serving_set_may_reduce)
                return .no_change;
        }
        self.advanceTargetObservationRevisionLocked();
        authority.?.convergence_requirements.put(self.alloc, group_id, .{
            .event_revision = self.target_observation_revision,
            .source_target_sequence = source_target_sequence,
            .serving_set_may_reduce = serving_set_may_reduce,
        }) catch {
            // Exact-scope bookkeeping failed, so conservatively widen this
            // one event to the established group-wide convergence fence.
            // Serving authority remains independent and is not revoked.
            return self.markGroupTargetObservationPendingLocked(
                table_name,
                state,
                group_id,
                source_target_sequence,
            );
        };
        if (current_status) |status| {
            const item = &status.stats.indexes[current_position orelse return .index_applied];
            if (indexMatchesTargetIdentity(item.*, identity))
                item.runtime_target_observation_complete = false;
        }
        return .index_applied;
    }

    /// A runtime owner disappearing makes current-target convergence unknown,
    /// but it does not revoke the last immutable serving observation. Fence
    /// every group against an already-in-flight publication while preserving
    /// incarnation-scoped counts and serviceability for status readers.
    pub fn markTableTargetObservationPending(
        self: *@This(),
        table_name: []const u8,
    ) void {
        const mutation_state = self.lockExistingTableMutation(table_name) orelse return;
        defer self.unlockTableMutation(mutation_state);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.get(table_name) orelse return;
        if (state.groups.count() == 0) return;
        state.required_target_observation_revisions.ensureTotalCapacity(
            self.alloc,
            state.groups.count(),
        ) catch {
            // Allocation pressure cannot erase serving authority. Make the
            // currently visible completion projection conservative; a later
            // authoritative owner publication can recover it.
            var statuses = state.groups.valueIterator();
            while (statuses.next()) |status| status.metadata.target_observation_complete = false;
            self.syncReadViewAuthorityLocked(table_name, state);
            return;
        };
        self.advanceTargetObservationRevisionLocked();
        const requirement = TableState.TargetObservationRequirement{
            .event_revision = self.target_observation_revision,
            .source_target_sequence = 0,
        };
        var groups = state.groups.iterator();
        while (groups.next()) |entry| {
            state.required_target_observation_revisions.putAssumeCapacity(entry.key_ptr.*, requirement);
            entry.value_ptr.metadata.target_observation_complete = false;
        }
        self.syncReadViewAuthorityLocked(table_name, state);
    }

    pub fn snapshotGroupStatus(
        self: *@This(),
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
    ) !?LocalTableRuntimeStatus {
        if (self.retainReadView(table_name)) |view| {
            defer view.release(self.read_view_alloc);
            const group_index = view.groupPosition(group_id) orelse return null;
            lockAtomic(&self.read_view_mutex);
            const group = view.groups[group_index];
            group.retain();
            self.read_view_mutex.unlock();
            defer group.release(self.read_view_alloc);
            var cloned = try group.status.clone(alloc);
            errdefer cloned.deinit(alloc);
            try group.applyAuthority(alloc, &cloned);
            return cloned;
        }
        return null;
    }

    fn mergeRefreshStatusLocked(
        self: *@This(),
        previous: ?*LocalTableRuntimeStatus,
        status: *LocalTableRuntimeStatus,
        now_ns: u64,
        index_authorities: *const std.StringHashMapUnmanaged(TargetedIndexAuthority),
        has_active_index_transition: bool,
        incoming_lookup: ?*const IndexObservationLookup,
    ) !void {
        const cached = previous orelse return;
        if (cached.cache_observation_generation > status.cache_observation_generation) {
            const cloned = try cached.clone(self.alloc);
            status.deinit(self.alloc);
            status.* = cloned;
            return;
        }
        if (status.metadata.source == .synthetic_config and runtimeStatusWorthPreserving(cached.*)) {
            const merged = try mergeCachedStatusWithSyntheticPlaceholder(
                self.alloc,
                cached.*,
                status.*,
                now_ns,
                index_authorities,
                has_active_index_transition,
            );
            status.deinit(self.alloc);
            status.* = merged;
            return;
        }
        if (incoming_lookup) |lookup| try mergeIndexObservationDelta(
            self.alloc,
            cached.*,
            status,
            lookup.*,
            index_authorities,
            null,
        );
        try preserveArtifactVisibilityOnReplayRegression(
            self.alloc,
            cached.*,
            status,
            index_authorities,
            has_active_index_transition,
            null,
        );
    }

    pub fn summary(self: *@This()) TableRuntimeSummary {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var result: TableRuntimeSummary = .{};
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            if (entry.groups.count() == 0) continue;
            result.table_count += 1;
            var table_has_replay_debt = false;
            var group_it = entry.groups.valueIterator();
            while (group_it.next()) |status| {
                result.group_count += 1;
                db_mod.types.accumulateTextMergeStats(&result.text_merge, status.stats.text_merge);
                db_mod.types.accumulateAsyncIndexingStats(&result.async_indexing, status.stats.async_indexing);
                var group_has_replay_debt = false;
                result.index_count += status.stats.indexes.len;
                for (status.stats.indexes) |index| {
                    const backlog = if (index.replay_target_sequence > index.replay_applied_sequence)
                        index.replay_target_sequence - index.replay_applied_sequence
                    else
                        0;
                    const has_replay_debt = index.replay_catch_up_required or backlog > 0;
                    if (!has_replay_debt) continue;
                    group_has_replay_debt = true;
                    table_has_replay_debt = true;
                    result.indexes_with_replay_debt += 1;
                    result.outstanding_replay_sequences += backlog;
                    result.max_index_replay_backlog = @max(result.max_index_replay_backlog, backlog);
                }
                if (group_has_replay_debt) result.groups_with_replay_debt += 1;
            }
            if (table_has_replay_debt) result.tables_with_replay_debt += 1;
        }
        return result;
    }

    fn publishTableRefreshLocked(
        self: *@This(),
        state: *TableState,
        table_name: []const u8,
        expected_epoch: TableEpoch,
        statuses: *LocalTableRuntimeStatuses,
        observation_generation: u64,
        target_observation_revision: u64,
        now_ns: u64,
    ) !void {
        // Validate the retained table/epoch in a short global-cache section.
        // Exact table ownership pins its authority maps during the
        // allocation-heavy preparation below; unrelated tables remain free.
        lockAtomic(&self.mutex);
        if (self.tables.get(table_name) != state or !std.meta.eql(state.epoch, expected_epoch)) {
            self.mutex.unlock();
            return error.RuntimeStatusPublicationContended;
        }
        self.mutex.unlock();

        var acknowledgement_capacity = try TargetAcknowledgementCapacityPreparation.init(
            self.alloc,
            &state.index_authorities,
            statuses.items,
        );
        defer acknowledgement_capacity.deinit(self.alloc);

        var replacement = std.AutoHashMapUnmanaged(u64, LocalTableRuntimeStatus).empty;
        errdefer {
            var it = replacement.valueIterator();
            while (it.next()) |status| status.deinit(self.alloc);
            replacement.deinit(self.alloc);
        }
        try replacement.ensureTotalCapacity(self.alloc, @intCast(statuses.items.len));

        const source_items = statuses.items;
        var last_source_by_group = std.AutoHashMapUnmanaged(u64, usize).empty;
        defer last_source_by_group.deinit(self.alloc);
        try last_source_by_group.ensureTotalCapacity(self.alloc, @intCast(source_items.len));
        for (source_items, 0..) |source_status, source_index|
            last_source_by_group.putAssumeCapacity(source_status.group_id, source_index);

        var acknowledgement_candidates = TargetAuthorityAcknowledgementCandidates.empty;
        defer acknowledgement_candidates.deinit(self.alloc);
        var maximum_acknowledgement_candidates: usize = 0;
        for (source_items) |source_status| {
            maximum_acknowledgement_candidates = std.math.add(
                usize,
                maximum_acknowledgement_candidates,
                source_status.stats.indexes.len,
            ) catch return error.OutOfMemory;
        }
        var active_absence_authorities: usize = 0;
        var absence_count_it = state.index_authorities.valueIterator();
        while (absence_count_it.next()) |authority| {
            if (authority.transition_active and !authority.target_authority_handed_off and
                targetExpectationIsAbsent(authority.expectation))
                active_absence_authorities += 1;
        }
        maximum_acknowledgement_candidates = std.math.add(
            usize,
            maximum_acknowledgement_candidates,
            std.math.mul(usize, active_absence_authorities, source_items.len) catch return error.OutOfMemory,
        ) catch return error.OutOfMemory;
        try acknowledgement_candidates.ensureTotalCapacity(
            self.alloc,
            @intCast(maximum_acknowledgement_candidates),
        );
        var moved: usize = 0;
        defer {
            for (source_items[moved..]) |*status| status.deinit(self.alloc);
            if (source_items.len > 0) self.alloc.free(source_items);
            statuses.items = &.{};
        }
        for (source_items, 0..) |*source_status, source_index| {
            var owned = source_status.*;
            source_status.* = undefined;
            moved += 1;
            var owned_needs_deinit = true;
            defer if (owned_needs_deinit) owned.deinit(self.alloc);
            owned.cache_observation_generation = observation_generation;
            owned.withMetadataDefaults(.background_refresh, now_ns);
            self.applyTargetObservationAuthorityLocked(
                state,
                owned.group_id,
                &owned,
                target_observation_revision,
            );
            if (state.active_index_transition_count != 0 and
                last_source_by_group.get(owned.group_id).? == source_index and
                owned.metadata.source != .synthetic_config and
                owned.metadata.source != .cached_snapshot)
            {
                var raw_lookup = try IndexObservationLookup.init(self.alloc, owned.stats.indexes);
                defer raw_lookup.deinit(self.alloc);
                for (owned.stats.indexes) |item| {
                    const authority = state.index_authorities.getPtr(item.name) orelse continue;
                    if (!authority.transition_active or authority.target_authority_handed_off or
                        (authority.handoff_topology_bound and
                            !authority.expected_handoff_groups.contains(owned.group_id)) or
                        observation_generation < authority.accept_target_after_observation_generation or
                        observation_generation < authority.expectation_observation_generation or
                        !targetAuthorityAcceptsIdentity(authority.*, item)) continue;
                    acknowledgement_candidates.putAssumeCapacity(.{
                        .index_name = state.index_authorities.getKey(item.name).?,
                        .group_id = owned.group_id,
                    }, .{
                        .transition_revision = authority.transition_revision,
                        .serviceable = targetObservationProvesServiceability(
                            item,
                            owned,
                            authority.expectation,
                        ),
                    });
                }
                var absence_it = state.index_authorities.iterator();
                while (absence_it.next()) |entry| {
                    const authority = entry.value_ptr;
                    if (!authority.transition_active or authority.target_authority_handed_off or
                        (authority.handoff_topology_bound and
                            !authority.expected_handoff_groups.contains(owned.group_id)) or
                        !targetExpectationIsAbsent(authority.expectation) or
                        authority.terminal_failures.contains(owned.group_id) or
                        observation_generation < authority.accept_target_after_observation_generation or
                        observation_generation < authority.expectation_observation_generation or
                        raw_lookup.by_name.contains(entry.key_ptr.*)) continue;
                    acknowledgement_candidates.putAssumeCapacity(.{
                        .index_name = entry.key_ptr.*,
                        .group_id = owned.group_id,
                    }, .{
                        .transition_revision = authority.transition_revision,
                        .serviceable = false,
                    });
                }
            }
            // prepareRefreshStatusLocked consumes `owned` on both success and
            // error. Disarm the local guard only at that ownership boundary;
            // errors above it (notably raw lookup allocation) must still free
            // the status already removed from `source_items`.
            owned_needs_deinit = false;
            owned = try self.prepareRefreshStatusLocked(
                state.groups.getPtr(owned.group_id),
                owned,
                now_ns,
                &state.index_authorities,
                state.active_index_transition_count != 0,
            );
            self.enforceIndexAuthoritiesInStatusLocked(state, &owned);
            if (replacement.getPtr(owned.group_id)) |duplicate| {
                duplicate.deinit(self.alloc);
                duplicate.* = owned;
            } else {
                replacement.putAssumeCapacity(owned.group_id, owned);
            }
        }

        // Materialize the effective identity set before transferring the
        // replacement into the cache. Allocation failure must leave the
        // previous snapshot intact; mutating `state.groups` first would both
        // publish a partial refresh and leak the displaced map on OOM.
        var effective_presence = TargetAuthorityAcknowledgementPresence.empty;
        defer effective_presence.deinit(self.alloc);
        var effective_index_count: usize = 0;
        var replacement_status_it = replacement.valueIterator();
        while (replacement_status_it.next()) |status| {
            effective_index_count = std.math.add(
                usize,
                effective_index_count,
                status.stats.indexes.len,
            ) catch return error.OutOfMemory;
        }
        try effective_presence.ensureTotalCapacity(self.alloc, @intCast(effective_index_count));
        replacement_status_it = replacement.valueIterator();
        while (replacement_status_it.next()) |status| {
            for (status.stats.indexes) |effective| {
                effective_presence.putAssumeCapacity(.{
                    .index_name = effective.name,
                    .group_id = status.group_id,
                }, {});
            }
        }

        lockAtomic(&self.mutex);
        if (self.tables.get(table_name) != state or !std.meta.eql(state.epoch, expected_epoch)) {
            self.mutex.unlock();
            return error.RuntimeStatusPublicationContended;
        }
        acknowledgement_capacity.install();
        var old_groups = state.groups;
        state.groups = replacement;
        var authority_it = state.index_authorities.valueIterator();
        while (authority_it.next()) |authority| {
            if (!authority.transition_active or authority.target_authority_handed_off) continue;
            // Exact acknowledgements are monotonic for groups which remain in
            // the topology. Retain them across a background refresh, pruning
            // only groups no longer present; clearing every acknowledgement
            // could strand a multi-group handoff after its owner had already
            // published one shard exactly once.
            if (authority.handoff_topology_bound) continue;
            var ack_it = authority.handoff_groups.iterator();
            while (ack_it.next()) |ack| {
                if (!state.groups.contains(ack.key_ptr.*))
                    authority.handoff_groups.removeByPtr(ack.key_ptr);
            }
        }
        var acknowledged_status_it = state.groups.valueIterator();
        while (acknowledged_status_it.next()) |status| {
            for (status.stats.indexes) |effective| {
                const candidate_key = TargetAuthorityAcknowledgementKey{
                    .index_name = effective.name,
                    .group_id = status.group_id,
                };
                const candidate = acknowledgement_candidates.get(candidate_key) orelse continue;
                const authority = state.index_authorities.getPtr(effective.name) orelse continue;
                if (!authority.transition_active or authority.target_authority_handed_off or
                    authority.transition_revision != candidate.transition_revision or
                    !targetObservationHandsOffAuthority(effective, status.*, authority.expectation)) continue;
                recordTargetGroupAcknowledgement(
                    authority,
                    status.group_id,
                    candidate.serviceable,
                );
            }
        }
        var absence_candidate_it = acknowledgement_candidates.iterator();
        while (absence_candidate_it.next()) |candidate| {
            const authority = state.index_authorities.getPtr(candidate.key_ptr.index_name) orelse continue;
            if (!authority.transition_active or authority.target_authority_handed_off or
                authority.transition_revision != candidate.value_ptr.transition_revision or
                !targetExpectationIsAbsent(authority.expectation) or
                authority.terminal_failures.contains(candidate.key_ptr.group_id) or
                effective_presence.contains(candidate.key_ptr.*)) continue;
            const status = state.groups.get(candidate.key_ptr.group_id) orelse continue;
            if (status.metadata.freshness != .fresh) continue;
            recordTargetGroupAcknowledgement(authority, candidate.key_ptr.group_id, false);
        }
        _ = self.advanceTargetedIndexAuthorityLocked(state);
        self.settleReleasedIndexAuthoritiesLocked(state);
        self.mutex.unlock();
        var old_it = old_groups.valueIterator();
        while (old_it.next()) |status| status.deinit(self.alloc);
        old_groups.deinit(self.alloc);
    }

    fn prepareRefreshStatusLocked(
        self: *@This(),
        previous: ?*LocalTableRuntimeStatus,
        incoming: LocalTableRuntimeStatus,
        now_ns: u64,
        index_authorities: *const std.StringHashMapUnmanaged(TargetedIndexAuthority),
        has_active_index_transition: bool,
    ) !LocalTableRuntimeStatus {
        var owned = incoming;
        errdefer owned.deinit(self.alloc);
        try self.mergeRefreshStatusLocked(
            previous,
            &owned,
            now_ns,
            index_authorities,
            has_active_index_transition,
            null,
        );
        return owned;
    }

    fn ensureTableLocked(self: *@This(), table_name: []const u8) !*TableState {
        if (self.tables.get(table_name)) |state| return state;
        const owned_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_name);
        const state = try self.alloc.create(TableState);
        errdefer self.alloc.destroy(state);
        state.* = .{
            .epoch = .{
                .invalidation_epoch = self.next_invalidation_epoch,
                .root_generation = 0,
            },
        };
        try self.tables.put(self.alloc, owned_name, state);
        return state;
    }

    fn clearGroupsLocked(self: *@This(), state: *TableState) void {
        var it = state.groups.valueIterator();
        while (it.next()) |status| status.deinit(self.alloc);
        state.groups.clearRetainingCapacity();
        state.required_target_observation_revisions.clearRetainingCapacity();
    }

    fn invalidateTableStateLocked(self: *@This(), table_name: []const u8, state: *TableState) void {
        self.clearGroupsLocked(state);
        self.clearIndexAuthoritiesLocked(state);
        self.removeReadView(table_name);
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
        state.epoch.root_generation +%= 1;
        if (state.epoch.root_generation == 0) state.epoch.root_generation = 1;
    }

    fn clearIndexAuthoritiesLocked(self: *@This(), state: *TableState) void {
        var it = state.index_authorities.iterator();
        while (it.next()) |entry| {
            self.alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(self.alloc);
        }
        state.index_authorities.clearRetainingCapacity();
        state.active_index_transition_count = 0;
    }

    fn settleReleasedIndexAuthoritiesLocked(self: *@This(), state: *TableState) void {
        if (state.groups.count() == 0 or state.active_index_transition_count == 0) return;
        // Release is independent of target identity: every group must have
        // produced a fresh observation after the owner released its lease.
        // Reduce that table-wide fact once, then settle every eligible
        // transition in one authority pass.
        var minimum_fresh_generation: u64 = std.math.maxInt(u64);
        var group_it = state.groups.valueIterator();
        while (group_it.next()) |status| {
            if (status.metadata.freshness != .fresh) return;
            minimum_fresh_generation = @min(
                minimum_fresh_generation,
                status.cache_observation_generation,
            );
        }

        var fence_it = state.index_authorities.iterator();
        while (fence_it.next()) |entry| {
            const authority = entry.value_ptr;
            if (!authority.transition_active or authority.owner_active or
                !authority.target_authority_handed_off) continue;
            const release_generation = authority.release_after_observation_generation orelse continue;
            if (minimum_fresh_generation < release_generation) continue;

            std.debug.assert(state.active_index_transition_count > 0);
            state.active_index_transition_count -= 1;
            if (targetExpectationIsAbsent(authority.expectation)) {
                const owned_name = entry.key_ptr.*;
                authority.deinit(self.alloc);
                state.index_authorities.removeByPtr(entry.key_ptr);
                self.alloc.free(@constCast(owned_name));
            } else {
                authority.transition_active = false;
                authority.release_after_observation_generation = null;
                authority.handoff_groups.clearRetainingCapacity();
            }
        }
    }

    fn reserveTargetAcknowledgementCapacityLocked(
        state: *TableState,
        maximum_new_groups: usize,
        alloc: std.mem.Allocator,
    ) !void {
        if (maximum_new_groups == 0 or state.active_index_transition_count == 0) return;
        var authority_it = state.index_authorities.valueIterator();
        while (authority_it.next()) |authority| {
            if (!authority.transition_active or authority.target_authority_handed_off) continue;
            // Reserve before any status ownership is transferred. Each input
            // group can contribute at most one acknowledgement to an exact
            // transition, so the commit path can remain allocation-free and
            // cannot silently lose its only handoff edge under memory pressure.
            try authority.handoff_groups.ensureUnusedCapacity(alloc, @intCast(maximum_new_groups));
        }
    }

    fn resetExpectedHandoffGroupsFromSnapshotLocked(
        authority: *TargetedIndexAuthority,
        state: *const TableState,
        alloc: std.mem.Allocator,
    ) !void {
        authority.handoff_topology_bound = false;
        authority.expected_handoff_groups.clearRetainingCapacity();
        try authority.expected_handoff_groups.ensureTotalCapacity(alloc, @intCast(state.groups.count()));
        var groups = state.groups.keyIterator();
        while (groups.next()) |group_id|
            authority.expected_handoff_groups.putAssumeCapacity(group_id.*, {});
    }

    fn recordTargetGroupAcknowledgement(
        authority: *TargetedIndexAuthority,
        group_id: u64,
        serviceable: bool,
    ) void {
        const entry = authority.handoff_groups.getOrPutAssumeCapacity(group_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        // A later catch-up/cache observation cannot revoke a serving proof
        // already supplied by this exact incarnation's resident owner.
        entry.value_ptr.serviceable = entry.value_ptr.serviceable or serviceable;
    }

    fn recordGroupTargetAuthorityAcknowledgementsLocked(
        state: *TableState,
        status: *LocalTableRuntimeStatus,
        observed_indexes: []const db_mod.types.DBIndexStats,
        observed_lookup: *const IndexObservationLookup,
    ) void {
        if (state.active_index_transition_count == 0 or
            status.metadata.source == .synthetic_config or
            status.metadata.source == .cached_snapshot) return;

        // Walk the effective rows once and resolve their raw owner rows by
        // name. This preserves retained same-incarnation serving facts without
        // turning bulk activation into one linear effective-row search per
        // observed index.
        for (status.stats.indexes) |*effective| {
            const observed_position = observed_lookup.by_name.get(effective.name) orelse continue;
            const item = observed_indexes[observed_position];
            const authority = state.index_authorities.getPtr(effective.name) orelse continue;
            if (!authority.transition_active or authority.target_authority_handed_off or
                (authority.handoff_topology_bound and
                    !authority.expected_handoff_groups.contains(status.group_id)) or
                status.cache_observation_generation < authority.accept_target_after_observation_generation or
                status.cache_observation_generation < authority.expectation_observation_generation or
                !targetAuthorityAcceptsIdentity(authority.*, item)) continue;
            // Presence and identity must come from this observation; serving
            // visibility may come from the same-incarnation snapshot retained
            // by the merge policy while its owner reports catch-up.
            if (!targetObservationHandsOffAuthority(effective.*, status.*, authority.expectation)) continue;
            // Record each group's serving proof when that owner publishes it,
            // independently of the later all-groups identity handoff. The row
            // remains stale for aggregate admission until every group acks,
            // but a cached refresh can safely retain this exact proof.
            const serviceable = targetObservationProvesServiceability(
                effective.*,
                status.*,
                authority.expectation,
            );
            effective.runtime_observation_serviceable = serviceable;
            recordTargetGroupAcknowledgement(authority, status.group_id, serviceable);
        }

        // Absence has no index row to drive the name lookup. Active absence
        // transitions are bounded by the configured catalog, and each is
        // checked once against this already-materialized group observation.
        var authority_it = state.index_authorities.iterator();
        while (authority_it.next()) |entry| {
            const authority = entry.value_ptr;
            if (!authority.transition_active or authority.target_authority_handed_off or
                (authority.handoff_topology_bound and
                    !authority.expected_handoff_groups.contains(status.group_id)) or
                !targetExpectationIsAbsent(authority.expectation) or
                authority.terminal_failures.contains(status.group_id) or
                status.cache_observation_generation < authority.accept_target_after_observation_generation or
                status.cache_observation_generation < authority.expectation_observation_generation or
                status.metadata.freshness != .fresh or
                observed_lookup.by_name.contains(entry.key_ptr.*)) continue;
            recordTargetGroupAcknowledgement(authority, status.group_id, false);
        }
    }

    fn advanceTargetedIndexAuthorityLocked(self: *@This(), state: *TableState) bool {
        _ = self;
        if (state.groups.count() == 0 or state.active_index_transition_count == 0) return false;
        var handed_off_any = false;
        var fence_it = state.index_authorities.iterator();
        while (fence_it.next()) |entry| {
            const fence = entry.value_ptr;
            if (!fence.transition_active) continue;
            if (fence.target_authority_handed_off) continue;
            const required_group_count = if (fence.handoff_topology_bound)
                fence.expected_handoff_groups.count()
            else
                state.groups.count();
            if (required_group_count == 0 or fence.handoff_groups.count() != required_group_count) continue;
            fence.target_authority_handed_off = true;
            handed_off_any = true;
        }
        if (!handed_off_any) return false;

        // Publish serviceability for every transition completed by this
        // observation in one table pass. Bulk activation therefore remains
        // O(active transitions + published indexes), not O(indexes squared).
        var status_it = state.groups.valueIterator();
        while (status_it.next()) |status| {
            for (status.stats.indexes) |*item| {
                const authority = state.index_authorities.get(item.name) orelse continue;
                if (!authority.target_authority_handed_off or
                    !targetAuthorityAcceptsIdentity(authority, item.*)) continue;
                item.runtime_observation_stale = false;
                // Exact identity acceptance and query admission are separate
                // authorities. A failed owner row may complete the handoff so
                // its incarnation-scoped diagnostics become visible, but it
                // must not manufacture a serving proof for an unpublished or
                // blocked generation.
                const acknowledged_serviceable = if (authority.handoff_groups.get(status.group_id)) |ack|
                    ack.serviceable
                else
                    false;
                item.runtime_observation_serviceable = item.runtime_observation_serviceable or
                    acknowledged_serviceable or
                    targetObservationProvesServiceability(item.*, status.*, authority.expectation);
                item.runtime_observation_targeted_sibling = true;
            }
        }
        return true;
    }

    /// Applies persistent live-index authority independently of transition
    /// ownership. Settling a fence must not make a superseded incarnation
    /// eligible for a later generic status publication.
    fn enforceIndexAuthoritiesInStatusLocked(
        self: *@This(),
        state: *const TableState,
        status: *LocalTableRuntimeStatus,
    ) void {
        _ = self;
        if (state.index_authorities.count() == 0) return;
        for (status.stats.indexes) |*item| {
            const authority = state.index_authorities.get(item.name) orelse continue;
            const accepted = switch (authority.expectation) {
                .unknown, .absent => false,
                .exact => authority.target_authority_handed_off and
                    targetAuthorityAcceptsIdentity(authority, item.*),
            };
            if (accepted) continue;
            item.runtime_observation_stale = true;
            item.runtime_observation_serviceable = false;
            item.runtime_observation_targeted_sibling = false;
        }
    }

    fn currentTargetedIndexAuthority(
        state: *TableState,
        index_name: []const u8,
        token: TargetedIndexTransitionToken,
    ) ?*TargetedIndexAuthority {
        if (state.epoch.root_generation != token.root_generation) return null;
        const authority = state.index_authorities.getPtr(index_name) orelse return null;
        if (!authority.transition_active or authority.transition_revision != token.revision) return null;
        return authority;
    }

    fn clearTablesLocked(self: *@This()) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.*.release(self.alloc);
        }
        self.tables.clearRetainingCapacity();
        self.clearReadViews();
    }

    fn clearReadViews(self: *@This()) void {
        while (true) {
            var retired_name: ?[]const u8 = null;
            var retired_view: ?*ReadView = null;
            lockAtomic(&self.read_view_mutex);
            var it = self.read_views.iterator();
            if (it.next()) |entry| {
                const removed = self.read_views.fetchRemove(entry.key_ptr.*).?;
                retired_name = removed.key;
                retired_view = removed.value;
            }
            self.read_view_mutex.unlock();
            if (retired_view) |view| {
                self.read_view_alloc.free(@constCast(retired_name.?));
                view.release(self.read_view_alloc);
                continue;
            }
            return;
        }
    }

    fn advanceInvalidationEpochLocked(self: *@This()) void {
        self.next_invalidation_epoch +%= 1;
        if (self.next_invalidation_epoch == 0) self.next_invalidation_epoch = 1;
    }

    fn advanceTopologyRevisionLocked(self: *@This()) void {
        self.topology_revision +%= 1;
        if (self.topology_revision == 0) self.topology_revision = 1;
    }

    fn takeObservationGenerationLocked(self: *@This()) u64 {
        const generation = self.next_observation_generation;
        self.next_observation_generation +%= 1;
        if (self.next_observation_generation == 0) self.next_observation_generation = 1;
        return generation;
    }

    fn takeTargetedIndexTransitionRevisionLocked(self: *@This()) u64 {
        const revision = self.next_targeted_index_transition_revision;
        self.next_targeted_index_transition_revision +%= 1;
        if (self.next_targeted_index_transition_revision == 0) self.next_targeted_index_transition_revision = 1;
        return revision;
    }

    fn advanceTargetObservationRevisionLocked(self: *@This()) void {
        self.target_observation_revision +%= 1;
        if (self.target_observation_revision == 0) self.target_observation_revision = 1;
    }

    fn applyTargetObservationAuthorityLocked(
        self: *@This(),
        state: *const TableState,
        group_id: u64,
        status: *LocalTableRuntimeStatus,
        observed_revision: u64,
    ) void {
        _ = self;
        const required = state.required_target_observation_revisions.get(group_id) orelse TableState.TargetObservationRequirement{
            .event_revision = 0,
            .source_target_sequence = 0,
        };
        status.metadata.target_observation_complete = status.metadata.target_observation_complete and
            observed_revision >= required.event_revision and
            status.metadata.target_observation_revision >= required.source_target_sequence;
        for (status.stats.indexes) |*item| {
            item.runtime_target_observation_complete = true;
            const authority = state.index_authorities.get(item.name) orelse continue;
            if (!targetAuthorityAcceptsIdentity(authority, item.*)) continue;
            const index_required = authority.convergence_requirements.get(group_id) orelse continue;
            item.runtime_target_observation_complete = observed_revision >= index_required.event_revision and
                item.replay_target_sequence >= index_required.source_target_sequence;
        }
    }
};

fn lessThanGroupIdPtr(_: void, lhs: *const LocalTableRuntimeStatus, rhs: *const LocalTableRuntimeStatus) bool {
    return lhs.group_id < rhs.group_id;
}

fn projectIndexActivationFailure(
    alloc: std.mem.Allocator,
    item: *db_mod.types.DBIndexStats,
    failure: IndexActivationFailureCode,
) !void {
    const error_name = failure.stableName();
    const load_error = try alloc.dupe(u8, error_name);
    errdefer alloc.free(load_error);
    const repair_error = try alloc.dupe(u8, error_name);

    if (item.load_error) |value| alloc.free(value);
    if (item.index_repair_last_error) |value| alloc.free(value);
    item.load_error = load_error;
    item.repair_degraded = true;
    item.repair_issue_count = @max(item.repair_issue_count, 1);
    item.repair_summary_ready = true;
    item.index_lifecycle_work_class = .repair;
    item.index_repair_trigger = "index_activation";
    item.index_repair_phase = "terminal";
    item.index_repair_automation = "paused";
    item.index_repair_last_error = repair_error;
    item.index_repair_wait_reason = "action_required";
    item.index_repair_status = .failed;
    item.index_repair_action_required = true;
    item.index_repair_active_generation_serviceable = item.runtime_observation_serviceable;
}

test "activation failure projection preserves every stable failure code" {
    const cases = [_]struct {
        code: IndexActivationFailureCode,
        name: []const u8,
    }{
        .{ .code = .invalid_target, .name = "InvalidIndexActivationTarget" },
        .{ .code = .conflicting_target, .name = "IndexActivationTargetConflict" },
        .{ .code = .unsupported, .name = "UnsupportedOperation" },
        .{ .code = .publication_failed, .name = "IndexActivationPublicationFailed" },
        .{ .code = .internal, .name = "IndexActivationInternalFailure" },
    };
    for (cases) |case| {
        var item = db_mod.types.DBIndexStats{
            .name = "thumbnail",
            .kind = .dense_vector,
            .runtime_observation_serviceable = true,
        };
        try projectIndexActivationFailure(std.testing.allocator, &item, case.code);
        try std.testing.expectEqualStrings(case.name, item.load_error.?);
        try std.testing.expectEqualStrings(case.name, item.index_repair_last_error.?);
        try std.testing.expectEqual(db_mod.types.IndexRepairStatus.failed, item.index_repair_status.?);
        try std.testing.expect(item.index_repair_action_required);
        try std.testing.expect(item.index_repair_active_generation_serviceable);
        std.testing.allocator.free(item.load_error.?);
        std.testing.allocator.free(item.index_repair_last_error.?);
    }
}

test "terminal activation failure precedes and binds its exact catalog row" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    const expected = TableRuntimeSnapshotCache.TargetedIndexExpectation{ .exact = .{
        .index_name = "thumbnail",
        .kind = .dense_vector,
        .incarnation = 42,
        .config_hash = 77,
    } };
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, expected));
    try std.testing.expect(cache.bindTargetedIndexExpectedGroups("docs", "thumbnail", transition, &.{7}));
    try std.testing.expectEqual(TableRuntimeSnapshotCache.RecordTerminalFailureResult.recorded, cache.recordTargetedIndexTerminalFailure(
        "docs",
        "thumbnail",
        transition,
        expected,
        7,
        .publication_failed,
    ));

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "thumbnail",
        .kind = .dense_vector,
        .coverage_generation = 42,
        .coverage_config_hash = 77,
        .coverage_identity_ready = true,
    }};
    const publication = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(publication, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .synthetic_config, .freshness = .opening },
            .stats = .{ .index_count = 1, .indexes = &indexes },
        }),
    );

    var failed = (try cache.snapshot(alloc, "docs")).?;
    defer failed.deinit(alloc);
    const item = failed.items[0].stats.indexes[0];
    try std.testing.expectEqualStrings("IndexActivationPublicationFailed", item.load_error.?);
    try std.testing.expectEqual(db_mod.types.IndexRepairStatus.failed, item.index_repair_status.?);
    try std.testing.expect(item.index_repair_action_required);
    try std.testing.expect(!item.runtime_observation_serviceable);

    cache.releaseTargetedIndexPublications("docs", "thumbnail", transition);
    const settled_publication = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(settled_publication, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = &indexes },
        }),
    );
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.transition_active);
    var settled = (try cache.snapshot(alloc, "docs")).?;
    defer settled.deinit(alloc);
    try std.testing.expectEqualStrings(
        "IndexActivationPublicationFailed",
        settled.items[0].stats.indexes[0].load_error.?,
    );
    try std.testing.expect(settled.items[0].stats.indexes[0].index_repair_action_required);

    _ = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    var superseded = (try cache.snapshot(alloc, "docs")).?;
    defer superseded.deinit(alloc);
    try std.testing.expect(superseded.items[0].stats.indexes[0].load_error == null);
}

test "terminal drop failure never acknowledges absence and remains actionable" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "thumbnail",
        .kind = .dense_vector,
        .coverage_generation = 41,
        .coverage_config_hash = 76,
        .coverage_identity_ready = true,
        .serving_snapshot_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = &indexes },
        }),
    );

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    const absent = TableRuntimeSnapshotCache.TargetedIndexExpectation.absent;
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, absent));
    try std.testing.expect(cache.bindTargetedIndexExpectedGroups("docs", "thumbnail", transition, &.{7}));
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.RecordTerminalFailureResult.recorded,
        cache.recordTargetedIndexTerminalFailure(
            "docs",
            "thumbnail",
            transition,
            absent,
            7,
            .publication_failed,
        ),
    );
    try std.testing.expect(!cache.targetedIndexGroupAcknowledged("docs", "thumbnail", absent, 7));
    try std.testing.expect(!cache.targetedIndexAuthorityHandedOff("docs", "thumbnail", transition));
    try std.testing.expect(!cache.acknowledgeTargetedIndexGroup(
        "docs",
        "thumbnail",
        transition,
        absent,
        7,
        false,
    ));

    cache.releaseTargetedIndexPublications("docs", "thumbnail", transition);
    const later = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(later, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            // Even a later raw observation of absence cannot reinterpret the
            // terminal owner result as a successful drop. The fenced
            // predecessor remains the public action-required row.
            .stats = .{},
        }),
    );
    const authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(authority.transition_active);
    try std.testing.expect(!authority.target_authority_handed_off);
    try std.testing.expect(!cache.targetedIndexGroupAcknowledged("docs", "thumbnail", absent, 7));
    var failed = (try cache.snapshot(alloc, "docs")).?;
    defer failed.deinit(alloc);
    try std.testing.expectEqualStrings(
        "IndexActivationPublicationFailed",
        failed.items[0].stats.indexes[0].load_error.?,
    );
    try std.testing.expect(failed.items[0].stats.indexes[0].index_repair_action_required);

    _ = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    var superseded = (try cache.snapshot(alloc, "docs")).?;
    defer superseded.deinit(alloc);
    try std.testing.expect(superseded.items[0].stats.indexes[0].load_error == null);
}

test "terminal failure result distinguishes supersession and reserved storage" {
    {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var cache = TableRuntimeSnapshotCache.init(failing.allocator());
        defer cache.deinit();
        const initial = try cache.capturePublicationToken("docs");
        try std.testing.expectEqual(
            TableRuntimeSnapshotCache.PublishResult.published,
            try cache.publishGroup(initial, "docs", .{ .group_id = 7, .stats = .{} }),
        );
        const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
        const absent = TableRuntimeSnapshotCache.TargetedIndexExpectation.absent;
        try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, absent));
        failing.fail_index = failing.alloc_index;
        try std.testing.expectEqual(
            TableRuntimeSnapshotCache.RecordTerminalFailureResult.storage_failure,
            cache.recordTargetedIndexTerminalFailure(
                "docs",
                "thumbnail",
                transition,
                absent,
                7,
                .publication_failed,
            ),
        );
        try std.testing.expect(failing.has_induced_failure);
    }
    {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var cache = TableRuntimeSnapshotCache.init(failing.allocator());
        defer cache.deinit();
        const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
        const absent = TableRuntimeSnapshotCache.TargetedIndexExpectation.absent;
        try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, absent));
        try std.testing.expect(cache.bindTargetedIndexExpectedGroups("docs", "thumbnail", transition, &.{7}));
        failing.fail_index = failing.alloc_index;
        try std.testing.expectEqual(
            TableRuntimeSnapshotCache.RecordTerminalFailureResult.recorded,
            cache.recordTargetedIndexTerminalFailure(
                "docs",
                "thumbnail",
                transition,
                absent,
                7,
                .publication_failed,
            ),
        );
        try std.testing.expect(!failing.has_induced_failure);
    }
    {
        var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
        defer cache.deinit();
        const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
        const absent = TableRuntimeSnapshotCache.TargetedIndexExpectation.absent;
        try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, absent));
        _ = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
        try std.testing.expectEqual(
            TableRuntimeSnapshotCache.RecordTerminalFailureResult.superseded,
            cache.recordTargetedIndexTerminalFailure(
                "docs",
                "thumbnail",
                transition,
                absent,
                7,
                .publication_failed,
            ),
        );
    }
}

fn preserveArtifactVisibilityOnReplayRegression(
    alloc: std.mem.Allocator,
    previous: LocalTableRuntimeStatus,
    incoming: *LocalTableRuntimeStatus,
    index_authorities: ?*const std.StringHashMapUnmanaged(TargetedIndexAuthority),
    has_active_index_transition: bool,
    structural_target_index_name: ?[]const u8,
) !void {
    var previous_lookup = try IndexObservationLookup.init(alloc, previous.stats.indexes);
    defer previous_lookup.deinit(alloc);
    var mutable_previous = previous;
    try preserveArtifactVisibilityUsingLookup(
        alloc,
        &mutable_previous,
        incoming,
        index_authorities,
        has_active_index_transition,
        structural_target_index_name,
        previous_lookup,
        false,
    );
}

fn preserveArtifactVisibilityUsingLookup(
    alloc: std.mem.Allocator,
    previous: *LocalTableRuntimeStatus,
    incoming: *LocalTableRuntimeStatus,
    index_authorities: ?*const std.StringHashMapUnmanaged(TargetedIndexAuthority),
    has_active_index_transition: bool,
    structural_target_index_name: ?[]const u8,
    previous_lookup: IndexObservationLookup,
    transfer_retained_state: bool,
) !void {
    var preserved_visibility = false;
    for (incoming.stats.indexes) |*dst| {
        // Serviceability is a cache-local continuity proof. Re-derive it for
        // every publication instead of trusting a copied incoming snapshot.
        dst.runtime_observation_serviceable = false;
        dst.runtime_observation_targeted_sibling = false;
        const cached_index = previous_lookup.by_name.get(dst.name) orelse continue;
        const cached = &previous.stats.indexes[cached_index];
        if (cached.kind != dst.kind) continue;
        const accepted_authority_continuity = if (index_authorities) |authorities|
            if (authorities.get(dst.name)) |authority|
                (!authority.transition_active or authority.target_authority_handed_off) and
                    targetAuthorityAcceptsIdentity(authority, cached.*) and
                    cached.runtime_observation_serviceable
            else
                false
        else
            false;
        const pending_cached_target_continuity = if (index_authorities) |authorities|
            if (authorities.get(dst.name)) |authority|
                authority.transition_active and
                    !authority.target_authority_handed_off and
                    incoming.metadata.source == .cached_snapshot and
                    targetAuthorityAcceptsIdentity(authority, cached.*) and
                    targetAuthorityAcceptsIdentity(authority, dst.*) and
                    cached.runtime_observation_serviceable
            else
                false
        else
            false;
        const derived_index = dst.kind == .dense_vector or dst.kind == .sparse_vector;
        const same_runtime_root = incoming.metadata.lsm_root_generation == previous.metadata.lsm_root_generation;
        const same_derived_incarnation = derived_index and
            dst.coverage_identity_ready and
            cached.coverage_identity_ready and
            dst.coverage_generation != 0 and
            dst.coverage_generation == cached.coverage_generation and
            dst.coverage_config_hash != 0 and
            dst.coverage_config_hash == cached.coverage_config_hash;
        const target_not_older = dst.replay_target_sequence >= cached.replay_target_sequence;
        const same_projection_config = if (dst.projection_checkpoint_config_hash != 0 and
            cached.projection_checkpoint_config_hash != 0)
            dst.projection_checkpoint_config_hash == cached.projection_checkpoint_config_hash
        else
            dst.coverage_config_hash != 0 and
                dst.coverage_config_hash == cached.coverage_config_hash;
        const same_projection_identity = same_runtime_root and
            (if (derived_index) same_derived_incarnation else same_projection_config);
        const cached_serving_owner = if (cached.serving_snapshot_owner_id != 0)
            cached.serving_snapshot_owner_id
        else
            previous.stats.runtime_owner_id;
        const incoming_serving_owner = if (dst.serving_snapshot_owner_id != 0)
            dst.serving_snapshot_owner_id
        else
            incoming.stats.runtime_owner_id;
        const same_serving_owner = cached_serving_owner != 0 and
            cached_serving_owner == incoming_serving_owner;
        const serving_revision_not_newer = same_serving_owner and
            same_projection_identity and
            dst.serving_snapshot_revision <= cached.serving_snapshot_revision;
        // Serving revisions are process-local, so they cannot order snapshots
        // across owner replacement. The durable replay target supplies the
        // cross-owner ordering boundary: for one root, incarnation, and
        // accepted source target, publication and coverage are monotonic.
        // A source update/delete advances that target before a lower
        // cardinality can become authoritative.
        // The storage replay cursor is deliberately conservative and can
        // advance across a journal record which does not affect this index.
        // Exact commit-time target authority is the semantic boundary for an
        // index projection: the cached row is marked incomplete synchronously
        // whenever a relevant source target advances. If it is still
        // complete, a larger broad replay cursor cannot authorize a serving
        // cardinality regression for this incarnation.
        const exact_source_target_cannot_reduce_cached = if (index_authorities) |authorities|
            if (authorities.get(dst.name)) |authority|
                targetAuthorityAcceptsIdentity(authority, cached.*) and
                    targetAuthorityAcceptsIdentity(authority, dst.*) and
                    (if (authority.convergence_requirements.get(incoming.group_id)) |required|
                        cached.runtime_target_observation_complete or
                            !required.serving_set_may_reduce
                    else
                        false)
            else
                false
        else
            false;
        const same_accepted_source_target = same_projection_identity and
            (dst.replay_target_sequence == cached.replay_target_sequence or
                exact_source_target_cannot_reduce_cached);
        const serving_cardinality_regressed =
            (dst.doc_count < cached.doc_count or
                dst.node_count < cached.node_count or
                (cached.publication_target_ready and
                    (!dst.publication_target_ready or
                        dst.publication_target_count < cached.publication_target_count)) or
                (!dst.serving_snapshot_ready and cached.serving_snapshot_ready));
        const serving_visibility_regressed = serving_cardinality_regressed and
            (serving_revision_not_newer or
                (same_accepted_source_target and
                    dst.load_error == null and
                    incoming.metadata.freshness != .failed));
        const cached_coverage_settled = cached.coverage_produced_count +|
            cached.coverage_skipped_count +|
            cached.coverage_terminal_failed_count;
        const incoming_coverage_settled = dst.coverage_produced_count +|
            dst.coverage_skipped_count +|
            dst.coverage_terminal_failed_count;
        // Coverage settlement is a separate convergence authority from HBC
        // serving cardinality. A late replay/status snapshot can retain the
        // published vectors while forgetting already classified skipped or
        // failed sources. Under the same exact accepted target, that cannot be
        // a legitimate regression. A real source mutation first clears the
        // exact target observation and therefore still admits deletes and
        // reclassification work.
        const coverage_settlement_regressed = same_accepted_source_target and
            cached.coverage_summary_ready and
            (!dst.coverage_summary_ready or
                incoming_coverage_settled < cached_coverage_settled or
                dst.coverage_produced_count < cached.coverage_produced_count or
                dst.coverage_skipped_count < cached.coverage_skipped_count or
                dst.coverage_terminal_failed_count < cached.coverage_terminal_failed_count);
        const applied_regressed = same_projection_identity and
            dst.replay_applied_sequence < cached.replay_applied_sequence;
        const same_projection = same_projection_identity and
            dst.projection_checkpoint_generation <= cached.projection_checkpoint_generation;
        const projection_regressed = same_projection and
            dst.projection_checkpoint_applied_sequence < cached.projection_checkpoint_applied_sequence;
        const previous_observation_serviceable = previous.metadata.freshness == .fresh or
            cached.runtime_observation_serviceable;
        const untouched_targeted_sibling = if (index_authorities) |authorities|
            has_active_index_transition and
                (if (authorities.get(dst.name)) |authority|
                    !authority.transition_active or authority.target_authority_handed_off
                else
                    true)
        else
            false;
        // A named in-place mutation cannot change a sibling incarnation or
        // the table root. Preserve the last authoritative sibling observation
        // when a current-token publication carries only table-level startup
        // state. Genuine fresh/failed observations still replace the cache.
        const exact_structural_sibling = if (structural_target_index_name) |target_name|
            !std.mem.eql(u8, dst.name, target_name) and same_projection_identity
        else
            false;
        const targeted_sibling_continuity = untouched_targeted_sibling and
            (exact_structural_sibling or
                incoming.metadata.freshness == .opening or
                incoming.metadata.freshness == .catching_up) and
            previous_observation_serviceable and
            !cached.runtime_observation_stale and
            dst.load_error == null;
        const target_fence_accepts_same_incarnation = if (index_authorities) |authorities|
            if (authorities.get(dst.name)) |authority|
                authority.transition_active and
                    !authority.target_authority_handed_off and
                    incoming.cache_observation_generation >= authority.accept_target_after_observation_generation and
                    targetAuthorityAcceptsIdentity(authority, dst.*) and
                    same_derived_incarnation and
                    same_runtime_root
            else
                false
        else
            false;
        const serviceable_catch_up_continuity = incoming.metadata.freshness == .catching_up and
            same_derived_incarnation and
            same_runtime_root and
            previous_observation_serviceable and
            (!cached.runtime_observation_stale or target_fence_accepts_same_incarnation) and
            cached.coverage_summary_ready and
            indexHasPublishedGenerationVisibility(cached.*);
        const serviceable_continuity = serviceable_catch_up_continuity or
            targeted_sibling_continuity or
            pending_cached_target_continuity or
            accepted_authority_continuity;
        dst.runtime_observation_serviceable = serviceable_continuity;
        dst.runtime_observation_targeted_sibling = targeted_sibling_continuity;
        // Exact incarnation authority admits this observation; it does not
        // make the cached artifact state newer than legitimate progress from
        // that same incarnation.
        if (accepted_authority_continuity) dst.runtime_observation_stale = false;
        const visibility_regressed_without_newer_replay = serviceable_continuity and
            target_not_older and
            !indexHasPublishedGenerationVisibility(dst.*) and
            dst.replay_applied_sequence <= cached.replay_applied_sequence;
        const artifact_visibility_needs_preservation = targeted_sibling_continuity or
            serving_visibility_regressed or
            applied_regressed or
            projection_regressed or
            visibility_regressed_without_newer_replay;
        if (!artifact_visibility_needs_preservation and !coverage_settlement_regressed) continue;

        if (artifact_visibility_needs_preservation) {
            preserveIndexArtifactVisibility(dst, cached.*);
            if (targeted_sibling_continuity or
                projection_regressed or
                visibility_regressed_without_newer_replay or
                (same_accepted_source_target and serving_cardinality_regressed))
                try preserveIndexProjectionLifecycle(alloc, dst, cached, transfer_retained_state);
            dst.replay_applied_sequence = @max(dst.replay_applied_sequence, cached.replay_applied_sequence);
            dst.replay_target_sequence = @max(dst.replay_target_sequence, cached.replay_target_sequence);
            dst.catch_up_applied_sequence = @max(dst.catch_up_applied_sequence, cached.catch_up_applied_sequence);
            dst.catch_up_target_sequence = @max(dst.catch_up_target_sequence, cached.catch_up_target_sequence);
            dst.replay_catch_up_required = dst.replay_applied_sequence < dst.replay_target_sequence;
            dst.backfill_active = dst.backfill_active or dst.replay_catch_up_required;
            if (dst.replay_target_sequence > 0 and dst.replay_applied_sequence < dst.replay_target_sequence) {
                dst.backfill_progress = @min(
                    1.0,
                    @as(f64, @floatFromInt(dst.replay_applied_sequence)) /
                        @as(f64, @floatFromInt(dst.replay_target_sequence)),
                );
            }
            preserved_visibility = true;
        }
        if (coverage_settlement_regressed) preserveIndexCoverageSettlement(dst, cached.*);
    }
    if (preserved_visibility and incoming.stats.doc_count < previous.stats.doc_count) {
        incoming.stats.doc_count = previous.stats.doc_count;
    }
    // A live writer's source cardinality is authoritative and can legitimately
    // fall after deletes (including TTL cleanup). Only background projections
    // need the anti-regression guard for source visibility.
    if (preserved_visibility and
        incoming.metadata.source != .live_writer_publish and
        incoming.stats.source_doc_count < previous.stats.source_doc_count)
    {
        incoming.stats.source_doc_count = previous.stats.source_doc_count;
    }
}

fn preserveIndexCoverageSettlement(
    dst: *db_mod.types.DBIndexStats,
    cached: db_mod.types.DBIndexStats,
) void {
    dst.coverage_produced_count = cached.coverage_produced_count;
    dst.coverage_skipped_count = cached.coverage_skipped_count;
    dst.coverage_terminal_failed_count = cached.coverage_terminal_failed_count;
    dst.coverage_config_hash = cached.coverage_config_hash;
    dst.coverage_summary_ready = cached.coverage_summary_ready;
    dst.coverage_generation = cached.coverage_generation;
    dst.coverage_identity_ready = cached.coverage_identity_ready;
}

fn preserveIndexProjectionLifecycle(
    alloc: std.mem.Allocator,
    dst: *db_mod.types.DBIndexStats,
    cached: *db_mod.types.DBIndexStats,
    transfer_retained_state: bool,
) !void {
    preserveIndexCoverageSettlement(dst, cached.*);
    // Activity is intentionally not preserved across a stale projection
    // handoff. Motion must come from the current runtime owner, never from the
    // retained serving snapshot.
    dst.backfill_active = cached.backfill_active;
    dst.backfill_progress = cached.backfill_progress;
    dst.enrichment_failed = cached.enrichment_failed;
    // Projection visibility and its lifecycle classification form one
    // incarnation-scoped observation. Mixing a cached work class with an
    // incoming repair state (or vice versa) can turn normal backfill into a
    // synthetic repair, or hide genuine recovery. Preserve the whole durable
    // lifecycle bundle whenever the corresponding projection is retained.
    const retained_last_error = if (transfer_retained_state) blk: {
        const value = cached.index_repair_last_error;
        cached.index_repair_last_error = dst.index_repair_last_error;
        break :blk value;
    } else if (cached.index_repair_last_error) |value|
        try alloc.dupe(u8, value)
    else
        null;
    if (!transfer_retained_state) {
        if (dst.index_repair_last_error) |value| alloc.free(value);
    }
    dst.index_repair_id = cached.index_repair_id;
    dst.index_lifecycle_work_class = cached.index_lifecycle_work_class;
    dst.index_repair_trigger = cached.index_repair_trigger;
    dst.index_repair_phase = cached.index_repair_phase;
    dst.index_repair_automation = cached.index_repair_automation;
    dst.index_repair_attempts = cached.index_repair_attempts;
    dst.index_repair_started_at_ms = cached.index_repair_started_at_ms;
    dst.index_repair_updated_at_ms = cached.index_repair_updated_at_ms;
    dst.index_repair_build_floor_sequence = cached.index_repair_build_floor_sequence;
    dst.index_repair_applied_sequence = cached.index_repair_applied_sequence;
    dst.index_repair_target_sequence = cached.index_repair_target_sequence;
    dst.index_repair_next_retry_at_ms = cached.index_repair_next_retry_at_ms;
    dst.index_repair_last_error = retained_last_error;
    dst.index_repair_wait_reason = cached.index_repair_wait_reason;
    dst.index_repair_status = cached.index_repair_status;
    dst.index_repair_action_required = cached.index_repair_action_required;
    dst.index_repair_active_generation_serviceable = cached.index_repair_active_generation_serviceable;
    dst.projection_checkpoint_status = cached.projection_checkpoint_status;
    dst.projection_checkpoint_applied_sequence = cached.projection_checkpoint_applied_sequence;
    dst.projection_checkpoint_generation = cached.projection_checkpoint_generation;
    dst.projection_checkpoint_config_hash = cached.projection_checkpoint_config_hash;
    dst.checkpoint_replay_tail_sequence_count = cached.checkpoint_replay_tail_sequence_count;
}

fn runtimeStatusWorthPreserving(status: LocalTableRuntimeStatus) bool {
    if (statusHasRuntimeFacts(status)) return true;
    return false;
}

fn statusStatsHaveRuntimeFacts(stats: db_mod.types.DBStats) bool {
    if (stats.doc_count > 0) return true;
    if (stats.repair_degraded or stats.repair_issue_count != 0) return true;
    if (docIdentityStatsHaveRuntimeFacts(stats.doc_identity)) return true;
    if (docSetPlanningStatsHaveRuntimeFacts(stats.doc_set_planning)) return true;
    if (stats.async_indexing.startup.active or stats.async_indexing.dense_catch_up.active) return true;
    if (stats.enrichment.enabled and (stats.enrichment.processed_requests > 0 or stats.enrichment.applied_sequence > 0 or stats.enrichment.target_sequence > 0 or stats.enrichment.retrying or stats.enrichment.worker_failed)) return true;
    if (stats.text_merge.pending_segments > 0 or stats.text_merge.in_flight_merges > 0 or stats.text_merge.completed_merges > 0 or stats.text_merge.failed_merges > 0) return true;
    for (stats.indexes) |index| {
        if (indexHasArtifactVisibilityFacts(index)) return true;
        if (index.repair_degraded or index.repair_issue_count != 0) return true;
        if (index.backfill_active or index.catch_up_active or index.replay_catch_up_required) return true;
        // A target-only replay/catch-up marker can be synthesized from topology
        // and accepted sequence. It is not enough to prove that a live runtime
        // has ever published concrete index state.
    }
    return false;
}

fn docIdentityStatsHaveRuntimeFacts(stats: db_mod.types.DocIdentityStats) bool {
    return stats.namespace_table_id != 0 or
        stats.namespace_shard_id != 0 or
        stats.namespace_range_id != 0 or
        stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.ordinal_capacity_remaining != 0 or
        stats.ordinal_capacity_exhausted or
        stats.rebuild_required or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0 or
        stats.min_created_generation != 0 or
        stats.max_created_generation != 0 or
        stats.min_deleted_generation != 0 or
        stats.max_deleted_generation != 0 or
        stats.scanned_primary_docs != 0 or
        stats.primary_docs_missing_ordinals != 0 or
        stats.primary_docs_missing_identity_state != 0 or
        stats.primary_docs_with_tombstone_ordinals != 0;
}

fn docSetPlanningStatsHaveRuntimeFacts(stats: db_mod.types.DocSetPlanningStats) bool {
    return stats.resolved_set_count != 0 or
        stats.all_set_count != 0 or
        stats.none_set_count != 0 or
        stats.doc_key_list_count != 0 or
        stats.ordinal_list_count != 0 or
        stats.ordinal_bitmap_count != 0 or
        stats.doc_key_list_docs != 0 or
        stats.ordinal_list_docs != 0 or
        stats.ordinal_bitmap_docs != 0 or
        stats.missing_ordinal_coverage_count != 0 or
        stats.bitmap_promotion_count != 0 or
        stats.unsupported_filter_shape_count != 0 or
        stats.stale_identity_generation_rejection_count != 0;
}

fn indexHasArtifactVisibilityFacts(index: db_mod.types.DBIndexStats) bool {
    return indexHasPublishedArtifactVisibility(index) or
        index.coverage_config_hash != 0;
}

fn indexHasPublishedArtifactVisibility(index: db_mod.types.DBIndexStats) bool {
    return index.doc_count > 0 or
        index.term_count > 0 or
        index.edge_count > 0 or
        index.node_count > 0 or
        index.root_node > 0;
}

fn indexHasPublishedGenerationVisibility(index: db_mod.types.DBIndexStats) bool {
    if (index.kind == .dense_vector or index.kind == .sparse_vector)
        return index.serving_snapshot_ready;
    return indexHasPublishedArtifactVisibility(index);
}

fn findIndexStatusByName(
    indexes: []const db_mod.types.DBIndexStats,
    name: []const u8,
) ?db_mod.types.DBIndexStats {
    for (indexes) |item| {
        if (std.mem.eql(u8, item.name, name)) return item;
    }
    return null;
}

fn targetObservationHandsOffAuthority(
    item: db_mod.types.DBIndexStats,
    status: LocalTableRuntimeStatus,
    expectation: TargetedIndexAuthority.Expectation,
) bool {
    const expected = switch (expectation) {
        .exact => |identity| identity,
        .unknown, .absent => return false,
    };
    if (!indexMatchesAuthorityIdentity(item, expected)) return false;
    if (item.load_error != null or status.metadata.freshness == .failed) return true;
    if (status.metadata.freshness == .fresh) return true;
    if (status.metadata.freshness != .opening and status.metadata.freshness != .catching_up) return false;
    if (item.kind == .dense_vector or item.kind == .sparse_vector) {
        return item.coverage_identity_ready and
            item.coverage_summary_ready and
            indexHasPublishedGenerationVisibility(item);
    }
    return indexHasPublishedArtifactVisibility(item);
}

fn targetObservationProvesServiceability(
    item: db_mod.types.DBIndexStats,
    status: LocalTableRuntimeStatus,
    expectation: TargetedIndexAuthority.Expectation,
) bool {
    const expected = switch (expectation) {
        .exact => |identity| identity,
        .unknown, .absent => return false,
    };
    if (!indexMatchesAuthorityIdentity(item, expected)) return false;

    const derived = item.kind == .dense_vector or item.kind == .sparse_vector;
    if (item.load_error != null or status.metadata.freshness == .failed) {
        if (!item.index_repair_active_generation_serviceable) return false;
        return if (derived) item.serving_snapshot_ready else true;
    }
    if (status.metadata.freshness == .fresh)
        return if (derived) item.serving_snapshot_ready else true;
    if (status.metadata.freshness != .opening and status.metadata.freshness != .catching_up)
        return false;
    if (derived) {
        return item.coverage_identity_ready and
            item.coverage_summary_ready and
            item.serving_snapshot_ready;
    }
    return indexHasPublishedArtifactVisibility(item);
}

fn targetExpectationIsAbsent(expectation: TargetedIndexAuthority.Expectation) bool {
    return switch (expectation) {
        .absent => true,
        .unknown, .exact => false,
    };
}

fn targetExpectationsEqual(
    lhs: TargetedIndexAuthority.Expectation,
    rhs: TargetedIndexAuthority.Expectation,
) bool {
    return switch (lhs) {
        .unknown => rhs == .unknown,
        .absent => rhs == .absent,
        .exact => |identity| switch (rhs) {
            .exact => |other| std.meta.eql(identity, other),
            .unknown, .absent => false,
        },
    };
}

fn targetAuthorityAcceptsIdentity(
    authority: TargetedIndexAuthority,
    item: db_mod.types.DBIndexStats,
) bool {
    const expected = switch (authority.expectation) {
        .exact => |identity| identity,
        .unknown, .absent => return false,
    };
    return indexMatchesAuthorityIdentity(item, expected);
}

fn targetExpectationAcceptsIdentity(
    expectation: TargetedIndexAuthority.Expectation,
    item: db_mod.types.DBIndexStats,
) bool {
    return switch (expectation) {
        .exact => |identity| indexMatchesAuthorityIdentity(item, identity),
        // An absent expectation belongs to the authority map entry selected
        // by this row's name. Project its failed drop onto the retained
        // predecessor without pretending that absence was acknowledged.
        .absent => true,
        .unknown => false,
    };
}

fn indexMatchesAuthorityIdentity(
    item: db_mod.types.DBIndexStats,
    expected: TargetedIndexAuthority.Identity,
) bool {
    return item.coverage_identity_ready and
        item.kind == expected.kind and
        item.coverage_generation == expected.incarnation and
        item.coverage_config_hash == expected.config_hash;
}

fn indexMatchesTargetIdentity(
    item: db_mod.types.DBIndexStats,
    expected: anytype,
) bool {
    return std.mem.eql(u8, item.name, expected.index_name) and
        item.coverage_identity_ready and
        item.kind == expected.kind and
        item.coverage_generation == expected.incarnation and
        item.coverage_config_hash == expected.config_hash;
}

fn targetIdentitiesEqual(
    expected: TargetedIndexAuthority.Identity,
    incoming: anytype,
) bool {
    return expected.kind == incoming.kind and
        expected.incarnation == incoming.incarnation and
        expected.config_hash == incoming.config_hash;
}

/// Merge an ordinary owner publication as a per-index delta. Omission means
/// "no new observation" and retains the cached row; it never means deletion.
/// A targeted structural publication may remove only its named target, while
/// a complete catalog refresh uses a separate replacement path. Exact accepted
/// identities are likewise retained independently, so a stale target row does
/// not suppress fresh sibling telemetry. The prebuilt lookup keeps this pass
/// O(cached indexes + incoming indexes) while the cache mutex is held.
fn mergeIndexObservationDelta(
    alloc: std.mem.Allocator,
    previous: LocalTableRuntimeStatus,
    status: *LocalTableRuntimeStatus,
    incoming: IndexObservationLookup,
    index_authorities: *const std.StringHashMapUnmanaged(TargetedIndexAuthority),
    removable_structural_target: ?[]const u8,
) !void {
    var retained_count: usize = 0;
    for (previous.stats.indexes) |cached| {
        if (incoming.by_name.get(cached.name)) |candidate_index| {
            const authority = index_authorities.get(cached.name) orelse continue;
            if (!targetAuthorityAcceptsIdentity(authority, cached)) continue;
            const candidate = status.stats.indexes[candidate_index];
            if (targetAuthorityAcceptsIdentity(authority, candidate)) continue;
            const retained = try cloneDBIndexStats(alloc, cached);
            db_mod.types.freeDBIndexStatsItem(alloc, candidate);
            status.stats.indexes[candidate_index] = retained;
            continue;
        }
        if (removable_structural_target) |target| {
            if (std.mem.eql(u8, cached.name, target)) continue;
        }
        retained_count += 1;
    }
    if (retained_count == 0) return;

    const original = status.stats.indexes;
    const merged = try alloc.alloc(db_mod.types.DBIndexStats, original.len + retained_count);
    var initialized = original.len;
    errdefer {
        for (merged[original.len..initialized]) |item|
            db_mod.types.freeDBIndexStatsItem(alloc, item);
        alloc.free(merged);
    }
    @memcpy(merged[0..original.len], original);
    for (previous.stats.indexes) |cached| {
        if (incoming.by_name.contains(cached.name)) continue;
        if (removable_structural_target) |target| {
            if (std.mem.eql(u8, cached.name, target)) continue;
        }
        merged[initialized] = try cloneDBIndexStats(alloc, cached);
        initialized += 1;
    }
    if (original.len > 0) alloc.free(original);
    status.stats.indexes = merged;
    status.stats.index_count = @intCast(merged.len);
}

fn mergedIndexObservationCount(
    previous: LocalTableRuntimeStatus,
    incoming: IndexObservationLookup,
    removable_structural_target: ?[]const u8,
) usize {
    var count = incoming.by_name.count();
    for (previous.stats.indexes) |cached| {
        if (incoming.by_name.contains(cached.name)) continue;
        if (removable_structural_target) |target| {
            if (std.mem.eql(u8, cached.name, target)) continue;
        }
        count += 1;
    }
    return count;
}

/// Allocation-free ownership merge used by atomic multi-group publication.
/// The caller must have sized the workspace from the same cached generation
/// and must retire both detached backing arrays after releasing the mutex.
fn moveIndexObservationDeltaLocked(
    previous: *LocalTableRuntimeStatus,
    status: *LocalTableRuntimeStatus,
    incoming: IndexObservationLookup,
    index_authorities: *const std.StringHashMapUnmanaged(TargetedIndexAuthority),
    removable_structural_target: ?[]const u8,
    workspace: *IndexDeltaMergeWorkspace,
) void {
    std.debug.assert(previous.cache_observation_generation == workspace.expected_previous_generation);
    std.debug.assert(previous.stats.indexes.len == workspace.expected_previous_index_count);
    std.debug.assert(workspace.merged_indexes.len == mergedIndexObservationCount(previous.*, incoming, removable_structural_target));
    workspace.previous_lookup.populate(previous.stats.indexes) catch unreachable;

    var merged_count: usize = 0;
    for (status.stats.indexes, 0..) |candidate, candidate_index| {
        if (workspace.previous_lookup.by_name.get(candidate.name)) |cached_index| {
            const cached = previous.stats.indexes[cached_index];
            if (index_authorities.get(cached.name)) |authority| {
                if (targetAuthorityAcceptsIdentity(authority, cached) and
                    !targetAuthorityAcceptsIdentity(authority, candidate))
                {
                    workspace.merged_indexes[merged_count] = cached;
                    workspace.cached_selected[cached_index] = true;
                    merged_count += 1;
                    continue;
                }
            }
        }
        workspace.merged_indexes[merged_count] = candidate;
        workspace.incoming_selected[candidate_index] = true;
        merged_count += 1;
    }
    for (previous.stats.indexes, 0..) |cached, cached_index| {
        if (incoming.by_name.contains(cached.name)) continue;
        if (removable_structural_target) |target| {
            if (std.mem.eql(u8, cached.name, target)) continue;
        }
        workspace.merged_indexes[merged_count] = cached;
        workspace.cached_selected[cached_index] = true;
        merged_count += 1;
    }
    std.debug.assert(merged_count == workspace.merged_indexes.len);

    workspace.old_incoming_backing = status.stats.indexes;
    status.stats.indexes = workspace.merged_indexes;
    status.stats.index_count = @intCast(merged_count);
}

fn finishMovedIndexObservationDeltaLocked(
    previous: *LocalTableRuntimeStatus,
    status: *LocalTableRuntimeStatus,
    workspace: *IndexDeltaMergeWorkspace,
) void {
    for (workspace.old_incoming_backing, workspace.incoming_selected) |item, selected| {
        if (selected) continue;
        workspace.retired_indexes[workspace.retired_count] = item;
        workspace.retired_count += 1;
    }
    workspace.old_cached_backing = previous.stats.indexes;
    for (workspace.old_cached_backing, workspace.cached_selected) |item, selected| {
        if (selected) continue;
        workspace.retired_indexes[workspace.retired_count] = item;
        workspace.retired_count += 1;
    }

    workspace.retired_status = previous.*;
    workspace.retired_status.?.stats.indexes = &.{};
    workspace.retired_status.?.stats.index_count = 0;
    previous.* = status.*;
    status.* = undefined;
    workspace.installed = true;
}

fn cloneDBIndexStats(
    alloc: std.mem.Allocator,
    item: db_mod.types.DBIndexStats,
) !db_mod.types.DBIndexStats {
    var source = [_]db_mod.types.DBIndexStats{item};
    var cloned = try cloneDBStats(alloc, .{ .index_count = 1, .indexes = source[0..] });
    const result = cloned.indexes[0];
    alloc.free(cloned.indexes);
    cloned.indexes = &.{};
    cloned.index_count = 0;
    db_mod.types.freeDBStats(alloc, cloned);
    return result;
}

fn targetedIndexExpectationForPublishableGroups(
    state: *const TableRuntimeSnapshotCache.TableState,
    statuses: []const LocalTableRuntimeStatus,
    index_name: []const u8,
    observation_generation: u64,
) !?TargetedIndexAuthority.Expectation {
    if (statuses.len == 0) return error.EmptyTargetedIndexObservation;
    var expected: ?TargetedIndexAuthority.Identity = null;
    var observed_absent = false;
    var has_publishable_group = false;
    for (statuses) |status| {
        if (state.groups.get(status.group_id)) |cached| {
            if (cached.cache_observation_generation > observation_generation) continue;
        }
        has_publishable_group = true;
        const item = findIndexStatusByName(status.stats.indexes, index_name) orelse {
            observed_absent = true;
            if (expected != null) return error.InconsistentTargetedIndexObservation;
            continue;
        };
        if (observed_absent) return error.InconsistentTargetedIndexObservation;
        if (!item.coverage_identity_ready) return error.MissingTargetedIndexIdentity;
        const identity: TargetedIndexAuthority.Identity = .{
            .kind = item.kind,
            .incarnation = item.coverage_generation,
            .config_hash = item.coverage_config_hash,
        };
        if (expected) |current| {
            if (!std.meta.eql(current, identity)) return error.InconsistentTargetedIndexObservation;
        } else {
            expected = identity;
        }
    }
    if (!has_publishable_group) return null;
    return if (expected) |identity| .{ .exact = identity } else .absent;
}

fn markTargetObservationStaleLocked(
    state: *TableRuntimeSnapshotCache.TableState,
    index_name: []const u8,
) void {
    var status_it = state.groups.valueIterator();
    while (status_it.next()) |status| {
        for (status.stats.indexes) |*item| {
            if (!std.mem.eql(u8, item.name, index_name)) continue;
            item.runtime_observation_stale = true;
            item.runtime_observation_serviceable = false;
            item.runtime_observation_targeted_sibling = false;
        }
    }
}

fn preserveIndexArtifactVisibility(dst: *db_mod.types.DBIndexStats, cached: db_mod.types.DBIndexStats) void {
    dst.doc_count = cached.doc_count;
    dst.term_count = cached.term_count;
    dst.edge_count = cached.edge_count;
    dst.node_count = cached.node_count;
    dst.root_node = cached.root_node;
    dst.publication_target_count = cached.publication_target_count;
    dst.publication_target_ready = cached.publication_target_ready;
    dst.serving_snapshot_ready = cached.serving_snapshot_ready;
    dst.serving_snapshot_revision = cached.serving_snapshot_revision;
    dst.serving_snapshot_owner_id = cached.serving_snapshot_owner_id;
    dst.text_merge = cached.text_merge;
    dst.hbc_cache = cached.hbc_cache;
    dst.hbc_posting = cached.hbc_posting;
}

fn mergeCachedStatusWithSyntheticPlaceholder(
    alloc: std.mem.Allocator,
    previous: LocalTableRuntimeStatus,
    placeholder: LocalTableRuntimeStatus,
    now_ns: u64,
    index_authorities: ?*const std.StringHashMapUnmanaged(TargetedIndexAuthority),
    has_active_index_transition: bool,
) !LocalTableRuntimeStatus {
    if (placeholder.stats.indexes.len == 0) {
        var cloned = try previous.clone(alloc);
        cloned.replaceMetadata(cachedSnapshotMetadata(previous.metadata, placeholder.metadata, now_ns));
        if (index_authorities) |authorities| {
            for (cloned.stats.indexes) |*item| {
                const targeted = if (authorities.get(item.name)) |authority|
                    authority.transition_active and !authority.target_authority_handed_off
                else
                    false;
                if (targeted) {
                    item.runtime_observation_stale = true;
                } else if (has_active_index_transition) {
                    item.runtime_observation_serviceable = true;
                    item.runtime_observation_targeted_sibling = true;
                }
            }
        }
        return cloned;
    }

    var merged = try placeholder.clone(alloc);
    errdefer merged.deinit(alloc);

    // Synthetic catalog rows carry no runtime-owner ordering authority. Keep
    // the generation of the exact snapshot they extend so a delayed owner or
    // structural publication captured before this refresh can still publish
    // its incarnation-scoped facts. Table epochs independently reject facts
    // captured before an actual catalog mutation.
    merged.cache_observation_generation = previous.cache_observation_generation;

    merged.stats.storage_change_token = previous.stats.storage_change_token;
    merged.stats.source_doc_count = previous.stats.source_doc_count;
    merged.stats.doc_count = previous.stats.doc_count;
    merged.stats.enrichment = previous.stats.enrichment;
    merged.stats.ttl_cleanup = previous.stats.ttl_cleanup;
    merged.stats.transaction_recovery = previous.stats.transaction_recovery;
    merged.stats.text_merge = previous.stats.text_merge;
    merged.stats.term_doc_freq_cache_hits = previous.stats.term_doc_freq_cache_hits;
    merged.stats.term_doc_freq_cache_misses = previous.stats.term_doc_freq_cache_misses;
    merged.stats.async_indexing = previous.stats.async_indexing;
    merged.stats.index_count = @intCast(merged.stats.indexes.len);

    // Relabel before copying exact cached index snapshots. replaceMetadata
    // intentionally clears cache-local authority bits; copying first would
    // immediately erase the owner acknowledgement we are trying to retain.
    merged.replaceMetadata(cachedSnapshotMetadata(previous.metadata, placeholder.metadata, now_ns));

    var previous_lookup = try IndexObservationLookup.init(alloc, previous.stats.indexes);
    defer previous_lookup.deinit(alloc);
    for (merged.stats.indexes) |*dst| {
        const target_fence = if (index_authorities) |authorities| authorities.get(dst.name) else null;
        const targeted = if (target_fence) |fence| !fence.target_authority_handed_off else false;
        // A post-fence owner observation already belongs to the new target
        // incarnation even when it has not published a serviceable artifact.
        // Retain those immutable facts across a synthetic refresh while the
        // fence continues to withhold serving authority. Pre-fence target
        // snapshots remain unusable and are intentionally omitted.
        const cached_index = previous_lookup.by_name.get(dst.name) orelse continue;
        const cached = previous.stats.indexes[cached_index];
        if (cached.kind != dst.kind) continue;
        const target_facts_current = if (target_fence) |fence|
            previous.cache_observation_generation >= fence.accept_target_after_observation_generation and
                targetAuthorityAcceptsIdentity(fence, cached)
        else
            false;
        if (targeted and !target_facts_current) continue;
        const owned_name = dst.name;
        dst.* = cached;
        dst.name = owned_name;
        if (targeted) {
            dst.runtime_observation_stale = false;
            dst.runtime_observation_serviceable = false;
            dst.runtime_observation_targeted_sibling = false;
            continue;
        }
        if (previous.metadata.freshness == .fresh) {
            // A current, exact runtime observation is sufficient to preserve
            // owner/runtime presence across a missed refresh. This does not
            // manufacture queryability: derived serving still requires the
            // separately persisted serving_snapshot_ready/artifact facts.
            dst.runtime_observation_serviceable = true;
        } else if (previous.metadata.source != .cached_snapshot) {
            // A transition-only observation cannot bootstrap durable owner
            // authority merely by being cached. Only a previously fresh owner
            // observation (or its already-validated cached successor) may do so.
            dst.runtime_observation_serviceable = false;
            dst.runtime_observation_targeted_sibling = false;
        }
    }
    if (index_authorities) |authorities| {
        for (merged.stats.indexes) |*dst| {
            const target_fence = authorities.get(dst.name);
            const targeted = if (target_fence) |fence|
                fence.transition_active and !fence.target_authority_handed_off and
                    previous.cache_observation_generation < fence.accept_target_after_observation_generation
            else
                false;
            if (targeted) {
                dst.runtime_observation_stale = true;
            } else if (target_fence == null and has_active_index_transition) {
                dst.runtime_observation_serviceable = true;
                dst.runtime_observation_targeted_sibling = true;
            }
        }
    }
    return merged;
}

fn cachedSnapshotMetadata(
    previous: RuntimeStatusMetadata,
    placeholder: RuntimeStatusMetadata,
    now_ns: u64,
) RuntimeStatusMetadata {
    var metadata = previous;
    metadata.source = .cached_snapshot;
    metadata.freshness = switch (placeholder.freshness) {
        .unknown, .missing => .stale,
        else => placeholder.freshness,
    };
    metadata.updated_at_ns = now_ns;
    // A synthetic refresh means the observer could not sample the resident
    // owner. Keep the last incarnation-scoped serving proof in the index
    // snapshot, but never claim that it covers the latest accepted source
    // target. Serving and convergence authority are deliberately independent.
    if (placeholder.source == .synthetic_config and
        (placeholder.freshness == .missing or
            placeholder.freshness == .unknown or
            placeholder.freshness == .stale))
    {
        metadata.target_observation_complete = false;
    }
    return metadata;
}

fn findMatchingIndexStatus(
    indexes: []const db_mod.types.DBIndexStats,
    name: []const u8,
    kind: db_mod.types.IndexKind,
) ?db_mod.types.DBIndexStats {
    for (indexes) |index| {
        if (index.kind != kind) continue;
        if (std.mem.eql(u8, index.name, name)) return index;
    }
    return null;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        std.Thread.yield() catch {};
    }
}

fn freeAlgebraicCandidateStatuses(alloc: std.mem.Allocator, candidates: []const db_mod.types.AlgebraicCandidateStatus) void {
    for (candidates) |candidate| {
        alloc.free(candidate.recommendation);
        alloc.free(candidate.materialization_id);
        alloc.free(candidate.lifecycle);
        alloc.free(candidate.decision);
    }
    if (candidates.len > 0) alloc.free(candidates);
}

fn cloneAlgebraicCandidateStatuses(
    alloc: std.mem.Allocator,
    candidates: []const db_mod.types.AlgebraicCandidateStatus,
) ![]const db_mod.types.AlgebraicCandidateStatus {
    if (candidates.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.AlgebraicCandidateStatus, candidates.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |candidate| {
            alloc.free(candidate.recommendation);
            alloc.free(candidate.materialization_id);
            alloc.free(candidate.lifecycle);
            alloc.free(candidate.decision);
        }
        alloc.free(out);
    }
    for (candidates, 0..) |candidate, i| {
        const recommendation = try alloc.dupe(u8, candidate.recommendation);
        errdefer alloc.free(recommendation);
        const materialization_id = try alloc.dupe(u8, candidate.materialization_id);
        errdefer alloc.free(materialization_id);
        const lifecycle = try alloc.dupe(u8, candidate.lifecycle);
        errdefer alloc.free(lifecycle);
        const decision = try alloc.dupe(u8, candidate.decision);
        errdefer alloc.free(decision);
        out[i] = .{
            .recommendation = recommendation,
            .materialization_id = materialization_id,
            .lifecycle = lifecycle,
            .decision = decision,
            .observation_count = candidate.observation_count,
            .estimated_scan_rows_saved = candidate.estimated_scan_rows_saved,
            .estimated_write_cost = candidate.estimated_write_cost,
            .estimated_tensor_rows = candidate.estimated_tensor_rows,
            .estimated_storage_bytes = candidate.estimated_storage_bytes,
            .estimated_write_amplification = candidate.estimated_write_amplification,
            .score = candidate.score,
            .idle_miss_count = candidate.idle_miss_count,
            .generation = candidate.generation,
        };
        initialized += 1;
    }
    return out;
}

fn freeAlgebraicCandidateDecisionStatuses(alloc: std.mem.Allocator, decisions: []const db_mod.types.AlgebraicCandidateDecisionStatus) void {
    for (decisions) |decision| {
        alloc.free(decision.recommendation);
        alloc.free(decision.materialization_id);
        alloc.free(decision.lifecycle);
        alloc.free(decision.previous_decision);
        alloc.free(decision.decision);
    }
    if (decisions.len > 0) alloc.free(decisions);
}

fn cloneAlgebraicCandidateDecisionStatuses(
    alloc: std.mem.Allocator,
    decisions: []const db_mod.types.AlgebraicCandidateDecisionStatus,
) ![]const db_mod.types.AlgebraicCandidateDecisionStatus {
    if (decisions.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.AlgebraicCandidateDecisionStatus, decisions.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |decision| {
            alloc.free(decision.recommendation);
            alloc.free(decision.materialization_id);
            alloc.free(decision.lifecycle);
            alloc.free(decision.previous_decision);
            alloc.free(decision.decision);
        }
        alloc.free(out);
    }
    for (decisions, 0..) |decision, i| {
        const recommendation = try alloc.dupe(u8, decision.recommendation);
        errdefer alloc.free(recommendation);
        const materialization_id = try alloc.dupe(u8, decision.materialization_id);
        errdefer alloc.free(materialization_id);
        const lifecycle = try alloc.dupe(u8, decision.lifecycle);
        errdefer alloc.free(lifecycle);
        const previous_decision = try alloc.dupe(u8, decision.previous_decision);
        errdefer alloc.free(previous_decision);
        const decision_text = try alloc.dupe(u8, decision.decision);
        errdefer alloc.free(decision_text);
        out[i] = .{
            .recommendation = recommendation,
            .materialization_id = materialization_id,
            .lifecycle = lifecycle,
            .previous_decision = previous_decision,
            .decision = decision_text,
            .observation_count = decision.observation_count,
            .estimated_scan_rows_saved = decision.estimated_scan_rows_saved,
            .estimated_write_cost = decision.estimated_write_cost,
            .score = decision.score,
            .score_delta = decision.score_delta,
            .idle_miss_count = decision.idle_miss_count,
            .generation = decision.generation,
        };
        initialized += 1;
    }
    return out;
}

fn freeAlgebraicProgressStatuses(alloc: std.mem.Allocator, progress_items: []const db_mod.types.AlgebraicProgressStatus) void {
    for (progress_items) |progress| {
        alloc.free(progress.recommendation);
        alloc.free(progress.materialization_id);
        alloc.free(progress.lifecycle);
    }
    if (progress_items.len > 0) alloc.free(progress_items);
}

fn cloneAlgebraicProgressStatuses(
    alloc: std.mem.Allocator,
    progress_items: []const db_mod.types.AlgebraicProgressStatus,
) ![]const db_mod.types.AlgebraicProgressStatus {
    if (progress_items.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.AlgebraicProgressStatus, progress_items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |progress| {
            alloc.free(progress.recommendation);
            alloc.free(progress.materialization_id);
            alloc.free(progress.lifecycle);
        }
        alloc.free(out);
    }
    for (progress_items, 0..) |progress, i| {
        const recommendation = try alloc.dupe(u8, progress.recommendation);
        errdefer alloc.free(recommendation);
        const materialization_id = try alloc.dupe(u8, progress.materialization_id);
        errdefer alloc.free(materialization_id);
        const lifecycle = try alloc.dupe(u8, progress.lifecycle);
        errdefer alloc.free(lifecycle);
        out[i] = .{
            .recommendation = recommendation,
            .materialization_id = materialization_id,
            .lifecycle = lifecycle,
            .target_sequence = progress.target_sequence,
            .applied_sequence = progress.applied_sequence,
            .rows_processed = progress.rows_processed,
            .target_rows = progress.target_rows,
        };
        initialized += 1;
    }
    return out;
}

fn cloneResolverReplayDiagnostics(alloc: std.mem.Allocator, stats: db_mod.types.ResolverReplayDiagnostics) !db_mod.types.ResolverReplayDiagnostics {
    var resolvers = try alloc.alloc(db_mod.types.ResolverReplayDiagnostic, stats.resolvers.len);
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

    for (stats.resolvers, 0..) |resolver, i| {
        const name = try alloc.dupe(u8, resolver.name);
        errdefer alloc.free(name);
        const table = try alloc.dupe(u8, resolver.table);
        errdefer alloc.free(table);
        const source_artifact = try alloc.dupe(u8, resolver.source_artifact);
        errdefer alloc.free(source_artifact);
        const resolution_artifact = try alloc.dupe(u8, resolver.resolution_artifact);
        errdefer alloc.free(resolution_artifact);
        resolvers[i] = .{
            .name = name,
            .table = table,
            .source_artifact = source_artifact,
            .resolution_artifact = resolution_artifact,
        };
        initialized += 1;
    }

    return .{
        .resolver_count = stats.resolver_count,
        .resolution_runtime_present = stats.resolution_runtime_present,
        .resolution_worker_started = stats.resolution_worker_started,
        .promotion_runtime_present = stats.promotion_runtime_present,
        .promotion_worker_started = stats.promotion_worker_started,
        .resolvers = resolvers,
    };
}

pub fn cloneDBStats(alloc: std.mem.Allocator, stats: db_mod.types.DBStats) !db_mod.types.DBStats {
    const resolver_replay = try cloneResolverReplayDiagnostics(alloc, stats.resolver_replay);
    errdefer db_mod.types.freeResolverReplayDiagnostics(alloc, resolver_replay);
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, stats.indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |item| {
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
        alloc.free(indexes);
    }

    for (stats.indexes, 0..) |item, i| {
        const load_error = if (item.load_error) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (load_error) |value| alloc.free(value);
        const index_repair_last_error = if (item.index_repair_last_error) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (index_repair_last_error) |value| alloc.free(value);
        const algebraic_last_error_doc_key = if (item.algebraic_last_error_doc_key) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_error_doc_key) |value| alloc.free(value);
        const algebraic_last_error_reason = if (item.algebraic_last_error_reason) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_error_reason) |value| alloc.free(value);
        const algebraic_capability_fingerprint = if (item.algebraic_capability_fingerprint) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_capability_fingerprint) |value| alloc.free(value);
        const algebraic_capability_lifecycle_status = if (item.algebraic_capability_lifecycle_status) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_capability_lifecycle_status) |value| alloc.free(value);
        const algebraic_planner_last_decision = if (item.algebraic_planner_last_decision) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_planner_last_decision) |value| alloc.free(value);
        const algebraic_planner_last_fallback_reason = if (item.algebraic_planner_last_fallback_reason) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_planner_last_fallback_reason) |value| alloc.free(value);
        const algebraic_planner_lifecycle_blocking_reason = if (item.algebraic_planner_lifecycle_blocking_reason) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_planner_lifecycle_blocking_reason) |value| alloc.free(value);
        const algebraic_last_observed_query_shape = if (item.algebraic_last_observed_query_shape) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_observed_query_shape) |value| alloc.free(value);
        const algebraic_last_recommended_materialization = if (item.algebraic_last_recommended_materialization) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_recommended_materialization) |value| alloc.free(value);
        const algebraic_top_candidate: ?db_mod.types.AlgebraicCandidateStatus = if (item.algebraic_top_candidate) |candidate| .{
            .recommendation = try alloc.dupe(u8, candidate.recommendation),
            .materialization_id = try alloc.dupe(u8, candidate.materialization_id),
            .lifecycle = try alloc.dupe(u8, candidate.lifecycle),
            .decision = try alloc.dupe(u8, candidate.decision),
            .observation_count = candidate.observation_count,
            .estimated_scan_rows_saved = candidate.estimated_scan_rows_saved,
            .estimated_write_cost = candidate.estimated_write_cost,
            .estimated_tensor_rows = candidate.estimated_tensor_rows,
            .estimated_storage_bytes = candidate.estimated_storage_bytes,
            .estimated_write_amplification = candidate.estimated_write_amplification,
            .score = candidate.score,
            .idle_miss_count = candidate.idle_miss_count,
            .generation = candidate.generation,
        } else null;
        errdefer if (algebraic_top_candidate) |candidate| {
            alloc.free(candidate.recommendation);
            alloc.free(candidate.materialization_id);
            alloc.free(candidate.lifecycle);
            alloc.free(candidate.decision);
        };
        const algebraic_active_progress: ?db_mod.types.AlgebraicProgressStatus = if (item.algebraic_active_progress) |progress| .{
            .recommendation = try alloc.dupe(u8, progress.recommendation),
            .materialization_id = try alloc.dupe(u8, progress.materialization_id),
            .lifecycle = try alloc.dupe(u8, progress.lifecycle),
            .target_sequence = progress.target_sequence,
            .applied_sequence = progress.applied_sequence,
            .rows_processed = progress.rows_processed,
            .target_rows = progress.target_rows,
        } else null;
        errdefer if (algebraic_active_progress) |progress| {
            alloc.free(progress.recommendation);
            alloc.free(progress.materialization_id);
            alloc.free(progress.lifecycle);
        };
        const algebraic_candidates = try cloneAlgebraicCandidateStatuses(alloc, item.algebraic_candidates);
        errdefer freeAlgebraicCandidateStatuses(alloc, algebraic_candidates);
        const algebraic_candidate_decision_history = try cloneAlgebraicCandidateDecisionStatuses(alloc, item.algebraic_candidate_decision_history);
        errdefer freeAlgebraicCandidateDecisionStatuses(alloc, algebraic_candidate_decision_history);
        const algebraic_progress = try cloneAlgebraicProgressStatuses(alloc, item.algebraic_progress);
        errdefer freeAlgebraicProgressStatuses(alloc, algebraic_progress);
        indexes[i] = .{
            .name = try alloc.dupe(u8, item.name),
            .kind = item.kind,
            .runtime_observation_stale = item.runtime_observation_stale,
            .runtime_observation_serviceable = item.runtime_observation_serviceable,
            .runtime_observation_targeted_sibling = item.runtime_observation_targeted_sibling,
            .runtime_target_observation_complete = item.runtime_target_observation_complete,
            .load_error = load_error,
            .doc_count = item.doc_count,
            .term_count = item.term_count,
            .edge_count = item.edge_count,
            .node_count = item.node_count,
            .root_node = item.root_node,
            .publication_target_count = item.publication_target_count,
            .publication_target_ready = item.publication_target_ready,
            .serving_snapshot_ready = item.serving_snapshot_ready,
            .serving_snapshot_revision = item.serving_snapshot_revision,
            .serving_snapshot_owner_id = item.serving_snapshot_owner_id,
            .coverage_produced_count = item.coverage_produced_count,
            .coverage_skipped_count = item.coverage_skipped_count,
            .coverage_terminal_failed_count = item.coverage_terminal_failed_count,
            .coverage_config_hash = item.coverage_config_hash,
            .coverage_summary_ready = item.coverage_summary_ready,
            .coverage_generation = item.coverage_generation,
            .coverage_identity_ready = item.coverage_identity_ready,
            .embedding_activity_observed = item.embedding_activity_observed,
            .embedding_activity_sample_fresh = item.embedding_activity_sample_fresh,
            .embedding_activity = item.embedding_activity,
            .backfill_active = item.backfill_active,
            .backfill_progress = item.backfill_progress,
            .enrichment_failed = item.enrichment_failed,
            .repair_degraded = item.repair_degraded,
            .repair_issue_count = item.repair_issue_count,
            .repair_summary_ready = item.repair_summary_ready,
            .repair_issue_count_estimated = item.repair_issue_count_estimated,
            .repair_scan_issue_count = item.repair_scan_issue_count,
            .index_repair_id = item.index_repair_id,
            .index_lifecycle_work_class = item.index_lifecycle_work_class,
            .index_repair_trigger = item.index_repair_trigger,
            .index_repair_phase = item.index_repair_phase,
            .index_repair_automation = item.index_repair_automation,
            .index_repair_attempts = item.index_repair_attempts,
            .index_repair_started_at_ms = item.index_repair_started_at_ms,
            .index_repair_updated_at_ms = item.index_repair_updated_at_ms,
            .index_repair_build_floor_sequence = item.index_repair_build_floor_sequence,
            .index_repair_applied_sequence = item.index_repair_applied_sequence,
            .index_repair_target_sequence = item.index_repair_target_sequence,
            .index_repair_next_retry_at_ms = item.index_repair_next_retry_at_ms,
            .index_repair_last_error = index_repair_last_error,
            .index_repair_wait_reason = item.index_repair_wait_reason,
            .index_repair_status = item.index_repair_status,
            .index_repair_action_required = item.index_repair_action_required,
            .index_repair_active_generation_serviceable = item.index_repair_active_generation_serviceable,
            .projection_checkpoint_status = item.projection_checkpoint_status,
            .projection_checkpoint_applied_sequence = item.projection_checkpoint_applied_sequence,
            .projection_checkpoint_generation = item.projection_checkpoint_generation,
            .projection_checkpoint_config_hash = item.projection_checkpoint_config_hash,
            .replay_applied_sequence = item.replay_applied_sequence,
            .replay_target_sequence = item.replay_target_sequence,
            .checkpoint_replay_tail_sequence_count = item.checkpoint_replay_tail_sequence_count,
            .replay_catch_up_required = item.replay_catch_up_required,
            .catch_up_active = item.catch_up_active,
            .catch_up_phase = item.catch_up_phase,
            .catch_up_applied_sequence = item.catch_up_applied_sequence,
            .catch_up_target_sequence = item.catch_up_target_sequence,
            .text_merge = item.text_merge,
            .hbc_cache = item.hbc_cache,
            .hbc_posting = item.hbc_posting,
            .algebraic_parse_error_count = item.algebraic_parse_error_count,
            .algebraic_last_error_doc_key = algebraic_last_error_doc_key,
            .algebraic_last_error_reason = algebraic_last_error_reason,
            .algebraic_schema_version = item.algebraic_schema_version,
            .algebraic_capability_fingerprint = algebraic_capability_fingerprint,
            .algebraic_capability_lifecycle_status = algebraic_capability_lifecycle_status,
            .algebraic_capability_change_added_fields = item.algebraic_capability_change_added_fields,
            .algebraic_capability_change_removed_fields = item.algebraic_capability_change_removed_fields,
            .algebraic_capability_change_changed_type_fields = item.algebraic_capability_change_changed_type_fields,
            .algebraic_skipped_dynamic_fields = item.algebraic_skipped_dynamic_fields,
            .algebraic_skipped_complex_fields = item.algebraic_skipped_complex_fields,
            .algebraic_skipped_unbounded_fields = item.algebraic_skipped_unbounded_fields,
            .algebraic_minmax_cache_hits = item.algebraic_minmax_cache_hits,
            .algebraic_minmax_cache_misses = item.algebraic_minmax_cache_misses,
            .algebraic_minmax_support_scans = item.algebraic_minmax_support_scans,
            .algebraic_planner_selected = item.algebraic_planner_selected,
            .algebraic_planner_fallback_count = item.algebraic_planner_fallback_count,
            .algebraic_planner_last_decision = algebraic_planner_last_decision,
            .algebraic_planner_last_fallback_reason = algebraic_planner_last_fallback_reason,
            .algebraic_planner_last_estimated_scan_rows = item.algebraic_planner_last_estimated_scan_rows,
            .algebraic_planner_last_estimated_result_buckets = item.algebraic_planner_last_estimated_result_buckets,
            .algebraic_planner_lifecycle_ready = item.algebraic_planner_lifecycle_ready,
            .algebraic_planner_lifecycle_blocking_reason = algebraic_planner_lifecycle_blocking_reason,
            .algebraic_dictionary_registry_claimed_count = item.algebraic_dictionary_registry_claimed_count,
            .algebraic_dictionary_registry_already_owned_count = item.algebraic_dictionary_registry_already_owned_count,
            .algebraic_dictionary_registry_owned_by_other_count = item.algebraic_dictionary_registry_owned_by_other_count,
            .algebraic_dictionary_registry_ready_hit_count = item.algebraic_dictionary_registry_ready_hit_count,
            .algebraic_dictionary_registry_ready_miss_count = item.algebraic_dictionary_registry_ready_miss_count,
            .algebraic_distributed_partial_validation_proven_count = item.algebraic_distributed_partial_validation_proven_count,
            .algebraic_distributed_partial_validation_rejected_count = item.algebraic_distributed_partial_validation_rejected_count,
            .algebraic_distributed_partial_rows_exported_count = item.algebraic_distributed_partial_rows_exported_count,
            .algebraic_vector_filter_attempt_count = item.algebraic_vector_filter_attempt_count,
            .algebraic_vector_filter_resolved_count = item.algebraic_vector_filter_resolved_count,
            .algebraic_vector_filter_unsupported_count = item.algebraic_vector_filter_unsupported_count,
            .algebraic_vector_filter_fail_closed_count = item.algebraic_vector_filter_fail_closed_count,
            .algebraic_vector_filter_include_doc_id_count = item.algebraic_vector_filter_include_doc_id_count,
            .algebraic_vector_filter_exclude_doc_id_count = item.algebraic_vector_filter_exclude_doc_id_count,
            .algebraic_graph_traversal_attempt_count = item.algebraic_graph_traversal_attempt_count,
            .algebraic_graph_traversal_proven_count = item.algebraic_graph_traversal_proven_count,
            .algebraic_graph_traversal_rejected_count = item.algebraic_graph_traversal_rejected_count,
            .algebraic_graph_traversal_fallback_count = item.algebraic_graph_traversal_fallback_count,
            .algebraic_graph_traversal_result_node_count = item.algebraic_graph_traversal_result_node_count,
            .algebraic_observed_query_shape_count = item.algebraic_observed_query_shape_count,
            .algebraic_recommendation_count = item.algebraic_recommendation_count,
            .algebraic_adaptive_candidate_count = item.algebraic_adaptive_candidate_count,
            .algebraic_adaptive_progress_count = item.algebraic_adaptive_progress_count,
            .algebraic_adaptive_backfilling_count = item.algebraic_adaptive_backfilling_count,
            .algebraic_adaptive_ready_count = item.algebraic_adaptive_ready_count,
            .algebraic_adaptive_stale_count = item.algebraic_adaptive_stale_count,
            .algebraic_adaptive_dematerialize_recommended_count = item.algebraic_adaptive_dematerialize_recommended_count,
            .algebraic_adaptive_decision_history_count = item.algebraic_adaptive_decision_history_count,
            .algebraic_adaptive_policy_drift_count = item.algebraic_adaptive_policy_drift_count,
            .algebraic_last_observed_query_shape = algebraic_last_observed_query_shape,
            .algebraic_last_recommended_materialization = algebraic_last_recommended_materialization,
            .algebraic_top_candidate = algebraic_top_candidate,
            .algebraic_active_progress = algebraic_active_progress,
            .algebraic_candidates = algebraic_candidates,
            .algebraic_candidate_decision_history = algebraic_candidate_decision_history,
            .algebraic_progress = algebraic_progress,
        };
        initialized += 1;
    }

    return .{
        .runtime_owner_id = stats.runtime_owner_id,
        .storage_change_token = stats.storage_change_token,
        .source_doc_count = stats.source_doc_count,
        .doc_count = stats.doc_count,
        .index_count = stats.index_count,
        .indexes = indexes,
        .repair_degraded = stats.repair_degraded,
        .repair_issue_count = stats.repair_issue_count,
        .repair_summary_ready = stats.repair_summary_ready,
        .repair_issue_count_estimated = stats.repair_issue_count_estimated,
        .doc_identity = stats.doc_identity,
        .doc_set_planning = stats.doc_set_planning,
        .enrichment = stats.enrichment,
        .resolution = stats.resolution,
        .promotion = stats.promotion,
        .resolver_replay = resolver_replay,
        .ttl_cleanup = stats.ttl_cleanup,
        .transaction_recovery = stats.transaction_recovery,
        .text_merge = stats.text_merge,
        .term_doc_freq_cache_hits = stats.term_doc_freq_cache_hits,
        .term_doc_freq_cache_misses = stats.term_doc_freq_cache_misses,
        .async_indexing = stats.async_indexing,
    };
}

fn publishGroupForTest(
    cache: *TableRuntimeSnapshotCache,
    table_name: []const u8,
    status: LocalTableRuntimeStatus,
) !TableRuntimeSnapshotCache.PublishResult {
    const token = try cache.capturePublicationToken(table_name);
    return try cache.publishGroup(token, table_name, status);
}

fn publishRefreshForTest(
    cache: *TableRuntimeSnapshotCache,
    snapshots: []TableRuntimeSnapshot,
) !void {
    var ownership_transferred = false;
    errdefer if (!ownership_transferred) {
        for (snapshots) |*snapshot_entry| snapshot_entry.deinit(cache.alloc);
    };
    const names = try cache.alloc.alloc([]const u8, snapshots.len);
    defer cache.alloc.free(names);
    for (snapshots, 0..) |snapshot_entry, i| names[i] = snapshot_entry.table_name;
    var token = try cache.captureCatalogToken(cache.alloc, names, true);
    defer token.deinit();
    ownership_transferred = true;
    var result = try cache.publishRefresh(&token, snapshots);
    defer result.deinit();
    try std.testing.expect(!result.hasRejectedTables());
}

test "runtime status cache rejects refresh captured before invalidation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_names = [_][]const u8{"docs"};
    var stale_token = try cache.captureCatalogToken(alloc, &stale_names, true);
    defer stale_token.deinit();
    cache.invalidateTable("docs");

    const stale_statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    stale_statuses[0] = .{
        .group_id = 7,
        .stats = .{ .repair_degraded = true },
    };
    const stale_snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(stale_snapshots);
    stale_snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = stale_statuses },
    };
    var stale_result = try cache.publishRefresh(&stale_token, stale_snapshots);
    defer stale_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), stale_result.rejected_tables.items.len);
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);

    const clean_statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    clean_statuses[0] = .{ .group_id = 7, .stats = .{} };
    const clean_snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(clean_snapshots);
    clean_snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = clean_statuses },
    };
    try publishRefreshForTest(&cache, clean_snapshots);

    var published = (try cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expect(!published.items[0].stats.repair_degraded);
}

test "runtime status cache publishes unaffected tables and retries only invalidated tables" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const table_names = [_][]const u8{ "docs", "logs" };
    var token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer token.deinit();
    cache.invalidateTable("docs");

    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 2);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = try alloc.dupe(LocalTableRuntimeStatus, &.{.{ .group_id = 7, .stats = .{ .doc_count = 7 } }}) },
    };
    snapshots[1] = .{
        .table_name = try alloc.dupe(u8, "logs"),
        .statuses = .{ .items = try alloc.dupe(LocalTableRuntimeStatus, &.{.{ .group_id = 9, .stats = .{ .doc_count = 9 } }}) },
    };

    var result = try cache.publishRefresh(&token, snapshots);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.published_tables);
    try std.testing.expectEqual(@as(usize, 1), result.rejected_tables.items.len);
    try std.testing.expectEqualStrings("docs", result.rejected_tables.items[0]);
    try std.testing.expect(result.removals_deferred);
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);
    var logs = (try cache.snapshot(alloc, "logs")).?;
    defer logs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), logs.items[0].stats.doc_count);
}

test "runtime status cache stable absence removal retires the old table epoch" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_logs_token = try cache.capturePublicationToken("logs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        stale_logs_token,
        "logs",
        .{ .group_id = 9, .stats = .{ .doc_count = 9 } },
    ));

    // Production passes only tables present in the current catalog. A complete
    // token must still capture the cached epoch for the now-absent table.
    const table_names = [_][]const u8{"docs"};
    var token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer token.deinit();
    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = try alloc.dupe(LocalTableRuntimeStatus, &.{.{ .group_id = 7, .stats = .{ .doc_count = 7 } }}) },
    };

    var result = try cache.publishRefresh(&token, snapshots);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.removed_tables);
    try std.testing.expect((try cache.snapshot(alloc, "logs")) == null);
    try std.testing.expect((try cache.snapshotGroupStatus(alloc, "logs", 9)) == null);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.stale_table, try cache.publishGroup(
        stale_logs_token,
        "logs",
        .{ .group_id = 9, .stats = .{ .doc_count = 10 } },
    ));
    const recreated = try cache.capturePublicationToken("logs");
    try std.testing.expect(!std.meta.eql(stale_logs_token.table_epoch, recreated.table_epoch));
}

test "runtime status snapshots never wait for mutable cache ownership" {
    const alloc = std.heap.page_allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();
    const token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        token,
        "docs",
        .{ .group_id = 7, .stats = .{ .doc_count = 9 } },
    ));

    const Snapshot = struct {
        cache: *TableRuntimeSnapshotCache,
        done: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var snapshot = self.cache.snapshot(std.heap.page_allocator, "docs") catch {
                self.failed.store(true, .release);
                self.done.store(true, .release);
                return;
            } orelse {
                self.failed.store(true, .release);
                self.done.store(true, .release);
                return;
            };
            snapshot.deinit(std.heap.page_allocator);
            self.done.store(true, .release);
        }
    };
    var snapshot = Snapshot{ .cache = &cache };
    lockAtomic(&cache.mutex);
    const thread = try std.Thread.spawn(.{}, Snapshot.run, .{&snapshot});
    var completed_while_locked = false;
    for (0..10_000) |_| {
        if (snapshot.done.load(.acquire)) {
            completed_while_locked = true;
            break;
        }
        std.Thread.yield() catch {};
    }
    cache.mutex.unlock();
    thread.join();
    try std.testing.expect(completed_while_locked);
    try std.testing.expect(!snapshot.failed.load(.acquire));
}

test "group read payload preparation and retirement stay off the global cache mutex" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const group_count = 256;
    const statuses = try alloc.alloc(LocalTableRuntimeStatus, group_count);
    defer alloc.free(statuses);
    for (statuses, 0..) |*status, index| status.* = .{
        .group_id = @intCast(index + 1),
        .stats = .{ .doc_count = 1 },
    };
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishLifecycleTransition(initial, "docs", statuses),
    );
    const view = cache.read_views.get("docs").?;
    const untouched_position = view.groupPosition(2).?;
    const untouched_group = view.groups[untouched_position];

    const Probe = struct {
        cache: *TableRuntimeSnapshotCache,
        calls: usize = 0,
        mutex_available: bool = true,

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.cache.mutex.tryLock()) {
                self.cache.mutex.unlock();
            } else {
                self.mutex_available = false;
            }
        }
    };
    var preparation = Probe{ .cache = &cache };
    var retirement = Probe{ .cache = &cache };
    test_read_group_preparation_hook = .{ .ptr = &preparation, .run = Probe.run };
    defer test_read_group_preparation_hook = null;
    test_read_group_retirement_hook = .{ .ptr = &retirement, .run = Probe.run };
    defer test_read_group_retirement_hook = null;

    const update = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(update, "docs", .{ .group_id = 1, .stats = .{ .doc_count = 2 } }),
    );
    try std.testing.expectEqual(@as(usize, 1), preparation.calls);
    try std.testing.expect(preparation.mutex_available);
    try std.testing.expectEqual(@as(usize, 1), retirement.calls);
    try std.testing.expect(retirement.mutex_available);
    try std.testing.expect(cache.read_views.get("docs").?.groups[untouched_position] == untouched_group);
}

test "blocked read payload preparation does not convoy an unrelated table" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const initial_a = try cache.capturePublicationToken("table-a");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishLifecycleTransition(initial_a, "table-a", &.{.{
            .group_id = 1,
            .stats = .{ .doc_count = 1 },
        }}),
    );
    const initial_b = try cache.capturePublicationToken("table-b");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishLifecycleTransition(initial_b, "table-b", &.{.{
            .group_id = 2,
            .stats = .{ .doc_count = 1 },
        }}),
    );
    const token_a = try cache.capturePublicationToken("table-a");
    const token_b = try cache.capturePublicationToken("table-b");

    const BlockFirstPreparation = struct {
        calls: std.atomic.Value(usize) = .init(0),
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.calls.fetchAdd(1, .acq_rel) != 0) return;
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
        }
    };
    const Publish = struct {
        cache: *TableRuntimeSnapshotCache,
        token: TableRuntimeSnapshotCache.PublicationToken,
        table_name: []const u8,
        group_id: u64,
        done: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            const result = self.cache.publishGroup(self.token, self.table_name, .{
                .group_id = self.group_id,
                .stats = .{ .doc_count = 2 },
            }) catch {
                self.failed.store(true, .release);
                self.done.store(true, .release);
                return;
            };
            if (result != .published) self.failed.store(true, .release);
            self.done.store(true, .release);
        }
    };

    var blocker = BlockFirstPreparation{};
    test_read_group_preparation_hook = .{ .ptr = &blocker, .run = BlockFirstPreparation.run };
    defer test_read_group_preparation_hook = null;
    var publish_a = Publish{ .cache = &cache, .token = token_a, .table_name = "table-a", .group_id = 1 };
    var publish_b = Publish{ .cache = &cache, .token = token_b, .table_name = "table-b", .group_id = 2 };
    const thread_a = try std.Thread.spawn(.{}, Publish.run, .{&publish_a});
    while (!blocker.entered.load(.acquire)) std.Thread.yield() catch {};
    const thread_b = try std.Thread.spawn(.{}, Publish.run, .{&publish_b});
    var unrelated_completed = false;
    for (0..100_000) |_| {
        if (publish_b.done.load(.acquire)) {
            unrelated_completed = true;
            break;
        }
        std.Thread.yield() catch {};
    }
    blocker.release.store(true, .release);
    thread_a.join();
    thread_b.join();
    try std.testing.expect(unrelated_completed);
    try std.testing.expect(!publish_a.failed.load(.acquire));
    try std.testing.expect(!publish_b.failed.load(.acquire));
}

test "blocked table refresh preparation does not convoy unrelated publication" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();
    for ([_][]const u8{ "table-a", "table-b" }, [_]u64{ 1, 2 }) |table_name, group_id| {
        const token = try cache.capturePublicationToken(table_name);
        try std.testing.expectEqual(
            TableRuntimeSnapshotCache.PublishResult.published,
            try cache.publishLifecycleTransition(token, table_name, &.{.{
                .group_id = group_id,
                .stats = .{ .doc_count = 1 },
            }}),
        );
    }
    const names = [_][]const u8{"table-a"};
    var refresh_token = try cache.captureCatalogToken(alloc, &names, false);
    defer refresh_token.deinit();
    const refresh_statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    refresh_statuses[0] = .{ .group_id = 1, .stats = .{ .doc_count = 2 } };
    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "table-a"),
        .statuses = .{ .items = refresh_statuses },
    };
    const token_b = try cache.capturePublicationToken("table-b");

    const BlockFirstPreparation = struct {
        calls: std.atomic.Value(usize) = .init(0),
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.calls.fetchAdd(1, .acq_rel) != 0) return;
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
        }
    };
    const Refresh = struct {
        cache: *TableRuntimeSnapshotCache,
        token: *const TableRuntimeSnapshotCache.CatalogToken,
        snapshots: []TableRuntimeSnapshot,
        failed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var result = self.cache.publishRefresh(self.token, self.snapshots) catch {
                self.failed.store(true, .release);
                return;
            };
            result.deinit();
        }
    };
    const Publish = struct {
        cache: *TableRuntimeSnapshotCache,
        token: TableRuntimeSnapshotCache.PublicationToken,
        done: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            const result = self.cache.publishGroup(self.token, "table-b", .{
                .group_id = 2,
                .stats = .{ .doc_count = 2 },
            }) catch {
                self.failed.store(true, .release);
                self.done.store(true, .release);
                return;
            };
            if (result != .published) self.failed.store(true, .release);
            self.done.store(true, .release);
        }
    };

    var blocker = BlockFirstPreparation{};
    test_read_group_preparation_hook = .{ .ptr = &blocker, .run = BlockFirstPreparation.run };
    defer test_read_group_preparation_hook = null;
    var refresh = Refresh{ .cache = &cache, .token = &refresh_token, .snapshots = snapshots };
    var publish = Publish{ .cache = &cache, .token = token_b };
    const refresh_thread = try std.Thread.spawn(.{}, Refresh.run, .{&refresh});
    var preparation_entered = false;
    for (0..100_000) |_| {
        if (blocker.entered.load(.acquire)) {
            preparation_entered = true;
            break;
        }
        std.Thread.yield() catch {};
    }
    if (!preparation_entered) {
        blocker.release.store(true, .release);
        refresh_thread.join();
        try std.testing.expect(preparation_entered);
        return;
    }
    const publish_thread = try std.Thread.spawn(.{}, Publish.run, .{&publish});
    var unrelated_completed = false;
    for (0..100_000) |_| {
        if (publish.done.load(.acquire)) {
            unrelated_completed = true;
            break;
        }
        std.Thread.yield() catch {};
    }
    blocker.release.store(true, .release);
    refresh_thread.join();
    publish_thread.join();
    try std.testing.expect(unrelated_completed);
    try std.testing.expect(!refresh.failed.load(.acquire));
    try std.testing.expect(!publish.failed.load(.acquire));
}

test "target invalidation retires only its table read view" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = TableRuntimeSnapshotCache.init(failing.allocator());
    cache.read_view_alloc = std.testing.allocator;
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .coverage_generation = 5,
        .coverage_config_hash = 15,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const docs = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        docs,
        "docs",
        .{ .group_id = 7, .stats = .{ .index_count = 1, .indexes = indexes[0..] } },
    ));
    const logs = try cache.capturePublicationToken("logs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        logs,
        "logs",
        .{ .group_id = 9, .stats = .{ .doc_count = 9 } },
    ));

    failing.fail_index = failing.alloc_index;
    cache.markIndexTargetObservationPending("docs", 7, .{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 5,
        .config_hash = 15,
    }, 1);
    try std.testing.expect((try cache.snapshot(std.testing.allocator, "docs")) == null);
    var unaffected = (try cache.snapshot(std.testing.allocator, "logs")).?;
    defer unaffected.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 9), unaffected.items[0].stats.doc_count);
}

test "lifecycle mirror failure cannot expose the retired generation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    cache.read_view_alloc = failing.allocator();
    defer cache.deinit();

    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        initial,
        "docs",
        .{ .group_id = 7, .stats = .{ .doc_count = 7 } },
    ));
    failing.fail_index = failing.alloc_index;
    const replacement = [_]LocalTableRuntimeStatus{.{ .group_id = 8, .stats = .{ .doc_count = 8 } }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishLifecycleTransition(
            try cache.capturePublicationToken("docs"),
            "docs",
            &replacement,
        ),
    );
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expect((try cache.snapshot(std.testing.allocator, "docs")) == null);
    try std.testing.expectEqual(@as(u64, 8), cache.tables.get("docs").?.groups.get(8).?.stats.doc_count);
}

test "table runtime snapshot cache clones stored status" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 11,
            .index_count = 2,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 2),
            .doc_identity = .{
                .namespace_table_id = 101,
                .namespace_shard_id = 202,
                .namespace_range_id = 303,
                .next_ordinal = 44,
                .allocated_ordinals = 43,
                .rebuild_required = true,
                .state_rows = 41,
                .live_ordinals = 40,
                .min_created_generation = 12,
                .max_created_generation = 18,
                .min_deleted_generation = 15,
                .max_deleted_generation = 19,
            },
            .doc_set_planning = .{
                .resolved_set_count = 9,
                .ordinal_list_count = 8,
                .ordinal_list_docs = 7,
                .missing_ordinal_coverage_count = 6,
                .stale_identity_generation_rejection_count = 5,
            },
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 11,
        .node_count = 5,
        .coverage_produced_count = 5,
        .coverage_skipped_count = 6,
        .coverage_terminal_failed_count = 7,
        .coverage_config_hash = 0x1234,
        .coverage_summary_ready = false,
        .coverage_generation = 0x5678,
        .coverage_identity_ready = true,
        .embedding_activity_observed = true,
        .embedding_activity_sample_fresh = true,
        .embedding_activity = .{ .epoch = 13, .embeddings_computed = 17, .active_batch_size = 3 },
        .backfill_active = true,
        .backfill_progress = 0.5,
        .enrichment_failed = true,
        .repair_scan_issue_count = 8,
        .projection_checkpoint_status = "rebuilding",
        .projection_checkpoint_applied_sequence = 9,
        .projection_checkpoint_generation = 10,
        .projection_checkpoint_config_hash = 11,
        .checkpoint_replay_tail_sequence_count = 12,
    };
    items[0].stats.indexes[1] = .{
        .name = try std.testing.allocator.dupe(u8, "alg"),
        .kind = .algebraic,
        .doc_count = 11,
        .algebraic_parse_error_count = 1,
        .algebraic_schema_version = 42,
        .algebraic_capability_fingerprint = try std.testing.allocator.dupe(u8, "cap:v1"),
        .algebraic_capability_lifecycle_status = try std.testing.allocator.dupe(u8, "stale"),
        .algebraic_capability_change_added_fields = 15,
        .algebraic_capability_change_removed_fields = 16,
        .algebraic_capability_change_changed_type_fields = 17,
        .algebraic_skipped_dynamic_fields = 18,
        .algebraic_skipped_complex_fields = 19,
        .algebraic_skipped_unbounded_fields = 20,
        .algebraic_minmax_cache_hits = 2,
        .algebraic_minmax_cache_misses = 3,
        .algebraic_minmax_support_scans = 4,
        .algebraic_planner_selected = 5,
        .algebraic_planner_fallback_count = 6,
        .algebraic_planner_last_decision = try std.testing.allocator.dupe(u8, "fallback"),
        .algebraic_planner_last_fallback_reason = try std.testing.allocator.dupe(u8, "no_materialization"),
        .algebraic_planner_last_estimated_scan_rows = 61,
        .algebraic_planner_last_estimated_result_buckets = 62,
        .algebraic_planner_lifecycle_ready = false,
        .algebraic_planner_lifecycle_blocking_reason = try std.testing.allocator.dupe(u8, "capability_lifecycle_not_ready"),
        .algebraic_dictionary_registry_claimed_count = 63,
        .algebraic_dictionary_registry_already_owned_count = 64,
        .algebraic_dictionary_registry_owned_by_other_count = 65,
        .algebraic_dictionary_registry_ready_hit_count = 66,
        .algebraic_dictionary_registry_ready_miss_count = 67,
        .algebraic_distributed_partial_validation_proven_count = 68,
        .algebraic_distributed_partial_validation_rejected_count = 69,
        .algebraic_distributed_partial_rows_exported_count = 70,
        .algebraic_vector_filter_attempt_count = 71,
        .algebraic_vector_filter_resolved_count = 72,
        .algebraic_vector_filter_unsupported_count = 73,
        .algebraic_vector_filter_fail_closed_count = 74,
        .algebraic_vector_filter_include_doc_id_count = 75,
        .algebraic_vector_filter_exclude_doc_id_count = 76,
        .algebraic_graph_traversal_attempt_count = 77,
        .algebraic_graph_traversal_proven_count = 78,
        .algebraic_graph_traversal_rejected_count = 79,
        .algebraic_graph_traversal_fallback_count = 80,
        .algebraic_graph_traversal_result_node_count = 81,
        .algebraic_observed_query_shape_count = 7,
        .algebraic_recommendation_count = 8,
        .algebraic_adaptive_candidate_count = 9,
        .algebraic_adaptive_progress_count = 10,
        .algebraic_adaptive_backfilling_count = 11,
        .algebraic_adaptive_ready_count = 12,
        .algebraic_adaptive_stale_count = 13,
        .algebraic_adaptive_dematerialize_recommended_count = 14,
        .algebraic_adaptive_decision_history_count = 15,
        .algebraic_adaptive_policy_drift_count = 16,
        .algebraic_last_error_doc_key = try std.testing.allocator.dupe(u8, "bad-doc"),
        .algebraic_last_error_reason = try std.testing.allocator.dupe(u8, "invalid_json"),
        .algebraic_last_observed_query_shape = try std.testing.allocator.dupe(u8, "shape:v1"),
        .algebraic_last_recommended_materialization = try std.testing.allocator.dupe(u8, "recommendation:v1"),
        .algebraic_top_candidate = .{
            .recommendation = try std.testing.allocator.dupe(u8, "recommendation:v2"),
            .materialization_id = try std.testing.allocator.dupe(u8, "adaptive:v2"),
            .lifecycle = try std.testing.allocator.dupe(u8, "recommended"),
            .decision = try std.testing.allocator.dupe(u8, "materialize"),
            .observation_count = 15,
            .estimated_scan_rows_saved = 16,
            .estimated_write_cost = 17,
            .estimated_tensor_rows = 18,
            .estimated_storage_bytes = 19,
            .estimated_write_amplification = 20,
            .score = 21,
            .idle_miss_count = 22,
            .generation = 23,
        },
        .algebraic_active_progress = .{
            .recommendation = try std.testing.allocator.dupe(u8, "recommendation:v2"),
            .materialization_id = try std.testing.allocator.dupe(u8, "adaptive:v2"),
            .lifecycle = try std.testing.allocator.dupe(u8, "backfilling"),
            .target_sequence = 23,
            .applied_sequence = 24,
            .rows_processed = 25,
            .target_rows = 26,
        },
    };

    const snapshots = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(snapshots);
    snapshots[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    try publishRefreshForTest(&cache, snapshots);

    var cloned = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer cloned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), cloned.items.len);
    try std.testing.expectEqual(@as(u64, 7), cloned.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 11), cloned.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 101), cloned.items[0].stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(@as(u64, 202), cloned.items[0].stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(@as(u64, 303), cloned.items[0].stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u32, 44), cloned.items[0].stats.doc_identity.next_ordinal);
    try std.testing.expect(cloned.items[0].stats.doc_identity.rebuild_required);
    try std.testing.expectEqual(@as(u64, 12), cloned.items[0].stats.doc_identity.min_created_generation);
    try std.testing.expectEqual(@as(u64, 18), cloned.items[0].stats.doc_identity.max_created_generation);
    try std.testing.expectEqual(@as(u64, 15), cloned.items[0].stats.doc_identity.min_deleted_generation);
    try std.testing.expectEqual(@as(u64, 19), cloned.items[0].stats.doc_identity.max_deleted_generation);
    try std.testing.expectEqual(@as(u64, 9), cloned.items[0].stats.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 8), cloned.items[0].stats.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 5), cloned.items[0].stats.doc_set_planning.stale_identity_generation_rejection_count);
    try std.testing.expectEqualStrings("vec", cloned.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(@as(u64, 5), cloned.items[0].stats.indexes[0].coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 6), cloned.items[0].stats.indexes[0].coverage_skipped_count);
    try std.testing.expectEqual(@as(u64, 7), cloned.items[0].stats.indexes[0].coverage_terminal_failed_count);
    try std.testing.expectEqual(@as(u64, 0x1234), cloned.items[0].stats.indexes[0].coverage_config_hash);
    try std.testing.expect(!cloned.items[0].stats.indexes[0].coverage_summary_ready);
    try std.testing.expectEqual(@as(u64, 0x5678), cloned.items[0].stats.indexes[0].coverage_generation);
    try std.testing.expect(cloned.items[0].stats.indexes[0].coverage_identity_ready);
    try std.testing.expect(cloned.items[0].stats.indexes[0].embedding_activity_observed);
    try std.testing.expect(cloned.items[0].stats.indexes[0].embedding_activity_sample_fresh);
    try std.testing.expectEqual(@as(u64, 13), cloned.items[0].stats.indexes[0].embedding_activity.epoch);
    try std.testing.expectEqual(@as(u64, 17), cloned.items[0].stats.indexes[0].embedding_activity.embeddings_computed);
    try std.testing.expectEqual(@as(u64, 3), cloned.items[0].stats.indexes[0].embedding_activity.active_batch_size);
    try std.testing.expect(cloned.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(f64, 0.5), cloned.items[0].stats.indexes[0].backfill_progress);
    try std.testing.expect(cloned.items[0].stats.indexes[0].enrichment_failed);
    try std.testing.expectEqual(@as(u64, 8), cloned.items[0].stats.indexes[0].repair_scan_issue_count);
    try std.testing.expectEqualStrings("rebuilding", cloned.items[0].stats.indexes[0].projection_checkpoint_status);
    try std.testing.expectEqual(@as(u64, 9), cloned.items[0].stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expectEqual(@as(u64, 10), cloned.items[0].stats.indexes[0].projection_checkpoint_generation);
    try std.testing.expectEqual(@as(u64, 11), cloned.items[0].stats.indexes[0].projection_checkpoint_config_hash);
    try std.testing.expectEqual(@as(u64, 12), cloned.items[0].stats.indexes[0].checkpoint_replay_tail_sequence_count);
    try std.testing.expectEqualStrings("alg", cloned.items[0].stats.indexes[1].name);
    try std.testing.expectEqual(@as(u64, 1), cloned.items[0].stats.indexes[1].algebraic_parse_error_count);
    try std.testing.expectEqual(@as(u32, 42), cloned.items[0].stats.indexes[1].algebraic_schema_version);
    try std.testing.expectEqualStrings("cap:v1", cloned.items[0].stats.indexes[1].algebraic_capability_fingerprint.?);
    try std.testing.expectEqualStrings("stale", cloned.items[0].stats.indexes[1].algebraic_capability_lifecycle_status.?);
    try std.testing.expectEqual(@as(u32, 15), cloned.items[0].stats.indexes[1].algebraic_capability_change_added_fields);
    try std.testing.expectEqual(@as(u32, 16), cloned.items[0].stats.indexes[1].algebraic_capability_change_removed_fields);
    try std.testing.expectEqual(@as(u32, 17), cloned.items[0].stats.indexes[1].algebraic_capability_change_changed_type_fields);
    try std.testing.expectEqual(@as(u32, 18), cloned.items[0].stats.indexes[1].algebraic_skipped_dynamic_fields);
    try std.testing.expectEqual(@as(u32, 19), cloned.items[0].stats.indexes[1].algebraic_skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 20), cloned.items[0].stats.indexes[1].algebraic_skipped_unbounded_fields);
    try std.testing.expectEqual(@as(u64, 2), cloned.items[0].stats.indexes[1].algebraic_minmax_cache_hits);
    try std.testing.expectEqual(@as(u64, 3), cloned.items[0].stats.indexes[1].algebraic_minmax_cache_misses);
    try std.testing.expectEqual(@as(u64, 4), cloned.items[0].stats.indexes[1].algebraic_minmax_support_scans);
    try std.testing.expectEqual(@as(u64, 5), cloned.items[0].stats.indexes[1].algebraic_planner_selected);
    try std.testing.expectEqual(@as(u64, 6), cloned.items[0].stats.indexes[1].algebraic_planner_fallback_count);
    try std.testing.expectEqualStrings("fallback", cloned.items[0].stats.indexes[1].algebraic_planner_last_decision.?);
    try std.testing.expectEqualStrings("no_materialization", cloned.items[0].stats.indexes[1].algebraic_planner_last_fallback_reason.?);
    try std.testing.expectEqual(@as(u64, 61), cloned.items[0].stats.indexes[1].algebraic_planner_last_estimated_scan_rows.?);
    try std.testing.expectEqual(@as(u64, 62), cloned.items[0].stats.indexes[1].algebraic_planner_last_estimated_result_buckets.?);
    try std.testing.expect(!cloned.items[0].stats.indexes[1].algebraic_planner_lifecycle_ready);
    try std.testing.expectEqualStrings("capability_lifecycle_not_ready", cloned.items[0].stats.indexes[1].algebraic_planner_lifecycle_blocking_reason.?);
    try std.testing.expectEqual(@as(u64, 63), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_claimed_count);
    try std.testing.expectEqual(@as(u64, 64), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_already_owned_count);
    try std.testing.expectEqual(@as(u64, 65), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_owned_by_other_count);
    try std.testing.expectEqual(@as(u64, 66), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_ready_hit_count);
    try std.testing.expectEqual(@as(u64, 67), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_ready_miss_count);
    try std.testing.expectEqual(@as(u64, 68), cloned.items[0].stats.indexes[1].algebraic_distributed_partial_validation_proven_count);
    try std.testing.expectEqual(@as(u64, 69), cloned.items[0].stats.indexes[1].algebraic_distributed_partial_validation_rejected_count);
    try std.testing.expectEqual(@as(u64, 70), cloned.items[0].stats.indexes[1].algebraic_distributed_partial_rows_exported_count);
    try std.testing.expectEqual(@as(u64, 71), cloned.items[0].stats.indexes[1].algebraic_vector_filter_attempt_count);
    try std.testing.expectEqual(@as(u64, 72), cloned.items[0].stats.indexes[1].algebraic_vector_filter_resolved_count);
    try std.testing.expectEqual(@as(u64, 73), cloned.items[0].stats.indexes[1].algebraic_vector_filter_unsupported_count);
    try std.testing.expectEqual(@as(u64, 74), cloned.items[0].stats.indexes[1].algebraic_vector_filter_fail_closed_count);
    try std.testing.expectEqual(@as(u64, 75), cloned.items[0].stats.indexes[1].algebraic_vector_filter_include_doc_id_count);
    try std.testing.expectEqual(@as(u64, 76), cloned.items[0].stats.indexes[1].algebraic_vector_filter_exclude_doc_id_count);
    try std.testing.expectEqual(@as(u64, 77), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_attempt_count);
    try std.testing.expectEqual(@as(u64, 78), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_proven_count);
    try std.testing.expectEqual(@as(u64, 79), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_rejected_count);
    try std.testing.expectEqual(@as(u64, 80), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_fallback_count);
    try std.testing.expectEqual(@as(u64, 81), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_result_node_count);
    try std.testing.expectEqual(@as(u64, 7), cloned.items[0].stats.indexes[1].algebraic_observed_query_shape_count);
    try std.testing.expectEqual(@as(u64, 8), cloned.items[0].stats.indexes[1].algebraic_recommendation_count);
    try std.testing.expectEqual(@as(u64, 9), cloned.items[0].stats.indexes[1].algebraic_adaptive_candidate_count);
    try std.testing.expectEqual(@as(u64, 10), cloned.items[0].stats.indexes[1].algebraic_adaptive_progress_count);
    try std.testing.expectEqual(@as(u64, 11), cloned.items[0].stats.indexes[1].algebraic_adaptive_backfilling_count);
    try std.testing.expectEqual(@as(u64, 12), cloned.items[0].stats.indexes[1].algebraic_adaptive_ready_count);
    try std.testing.expectEqual(@as(u64, 13), cloned.items[0].stats.indexes[1].algebraic_adaptive_stale_count);
    try std.testing.expectEqual(@as(u64, 14), cloned.items[0].stats.indexes[1].algebraic_adaptive_dematerialize_recommended_count);
    try std.testing.expectEqual(@as(u64, 15), cloned.items[0].stats.indexes[1].algebraic_adaptive_decision_history_count);
    try std.testing.expectEqual(@as(u64, 16), cloned.items[0].stats.indexes[1].algebraic_adaptive_policy_drift_count);
    try std.testing.expectEqualStrings("bad-doc", cloned.items[0].stats.indexes[1].algebraic_last_error_doc_key.?);
    try std.testing.expectEqualStrings("invalid_json", cloned.items[0].stats.indexes[1].algebraic_last_error_reason.?);
    try std.testing.expectEqualStrings("shape:v1", cloned.items[0].stats.indexes[1].algebraic_last_observed_query_shape.?);
    try std.testing.expectEqualStrings("recommendation:v1", cloned.items[0].stats.indexes[1].algebraic_last_recommended_materialization.?);
    const top_candidate = cloned.items[0].stats.indexes[1].algebraic_top_candidate.?;
    try std.testing.expectEqualStrings("recommendation:v2", top_candidate.recommendation);
    try std.testing.expectEqualStrings("adaptive:v2", top_candidate.materialization_id);
    try std.testing.expectEqualStrings("recommended", top_candidate.lifecycle);
    try std.testing.expectEqualStrings("materialize", top_candidate.decision);
    try std.testing.expectEqual(@as(u64, 15), top_candidate.observation_count);
    try std.testing.expectEqual(@as(i128, 21), top_candidate.score);
    const active_progress = cloned.items[0].stats.indexes[1].algebraic_active_progress.?;
    try std.testing.expectEqualStrings("recommendation:v2", active_progress.recommendation);
    try std.testing.expectEqualStrings("adaptive:v2", active_progress.materialization_id);
    try std.testing.expectEqualStrings("backfilling", active_progress.lifecycle);
    try std.testing.expectEqual(@as(u64, 23), active_progress.target_sequence);
    try std.testing.expectEqual(@as(u64, 25), active_progress.rows_processed);
}

test "table runtime snapshot cache replaces snapshots while preserving one group status" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const docs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    docs_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 11,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    docs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 11,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 10,
        .replay_catch_up_required = true,
    };
    const initial = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(initial);
    initial[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = docs_items },
    };
    try publishRefreshForTest(&cache, initial);

    const refresh_docs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    refresh_docs_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 99,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    refresh_docs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 99,
    };
    const refresh_logs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    refresh_logs_items[0] = .{
        .group_id = 8,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    refresh_logs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "kw"),
        .kind = .full_text,
        .doc_count = 3,
    };
    const refresh = try std.testing.allocator.alloc(TableRuntimeSnapshot, 2);
    refresh[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = refresh_docs_items },
    };
    refresh[1] = .{
        .table_name = try std.testing.allocator.dupe(u8, "logs"),
        .statuses = .{ .items = refresh_logs_items },
    };

    try publishRefreshForTest(&cache, refresh);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 11), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 5), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 10), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(docs.items[0].stats.indexes[0].replay_catch_up_required);

    var logs = (try cache.snapshot(std.testing.allocator, "logs")).?;
    defer logs.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 3), logs.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("kw", logs.items[0].stats.indexes[0].name);
}

test "table runtime snapshot cache does not replace published live status with synthetic zero" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const live_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    live_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .live_writer_publish,
            .freshness = .fresh,
            .status_generation = 12,
        },
        .stats = .{
            .doc_count = 1_000_000,
            .index_count = 2,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 2),
        },
    };
    live_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .runtime_observation_serviceable = true,
        .doc_count = 1_000_000,
        .node_count = 44_321,
        .root_node = 3,
        .replay_applied_sequence = 4000,
        .replay_target_sequence = 4000,
        .catch_up_applied_sequence = 4000,
        .catch_up_target_sequence = 4000,
    };
    live_items[0].stats.indexes[1] = .{
        .name = try std.testing.allocator.dupe(u8, "text_idx"),
        .kind = .full_text,
        .doc_count = 1_000_000,
        .term_count = 83,
    };
    const initial = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(initial);
    initial[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = live_items },
    };
    try publishRefreshForTest(&cache, initial);

    const synthetic_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    synthetic_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    synthetic_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
    };
    const refresh = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(refresh);
    refresh[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = synthetic_items },
    };
    try publishRefreshForTest(&cache, refresh);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), docs.items.len);
    try std.testing.expectEqual(RuntimeStatusSource.cached_snapshot, docs.items[0].metadata.source);
    try std.testing.expectEqual(RuntimeStatusFreshness.stale, docs.items[0].metadata.freshness);
    try std.testing.expect(!docs.items[0].metadata.target_observation_complete);
    try std.testing.expectEqual(@as(u64, 1_000_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u32, 1), docs.items[0].stats.index_count);
    try std.testing.expectEqual(@as(usize, 1), docs.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("dense_idx", docs.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(@as(u64, 1_000_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 44_321), docs.items[0].stats.indexes[0].node_count);
    try std.testing.expectEqual(@as(u64, 3), docs.items[0].stats.indexes[0].root_node);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].catch_up_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].catch_up_target_sequence);
    try std.testing.expect(docs.items[0].stats.indexes[0].runtime_observation_serviceable);
}

test "single group synthetic publication preserves owner runtime authority" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    var live_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 4,
        .replay_target_sequence = 4,
    }};
    const live_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(live_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = live_indexes[0..] },
        }),
    );

    var placeholder_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
    }};
    const placeholder_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(placeholder_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .synthetic_config, .freshness = .stale, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = placeholder_indexes[0..] },
        }),
    );

    var observed = (try cache.snapshotGroupStatus(std.testing.allocator, "docs", 7)).?;
    defer observed.deinit(std.testing.allocator);
    try std.testing.expectEqual(RuntimeStatusSource.cached_snapshot, observed.metadata.source);
    try std.testing.expectEqual(RuntimeStatusFreshness.stale, observed.metadata.freshness);
    try std.testing.expect(!observed.metadata.target_observation_complete);
    try std.testing.expectEqual(@as(u64, 100), observed.stats.source_doc_count);
    try std.testing.expectEqual(@as(u64, 42), observed.stats.indexes[0].coverage_generation);
    try std.testing.expectEqual(@as(u64, 4), observed.stats.indexes[0].replay_applied_sequence);
    try std.testing.expect(observed.stats.indexes[0].runtime_observation_serviceable);
}

test "table runtime snapshot cache preserving replacement does not replace live status with synthetic zero" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const live_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    live_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .live_writer_publish,
            .freshness = .fresh,
        },
        .stats = .{
            .doc_count = 250_000,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    live_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 250_000,
        .node_count = 2048,
        .root_node = 1,
        .replay_applied_sequence = 1000,
        .replay_target_sequence = 4000,
        .replay_catch_up_required = true,
        .catch_up_active = true,
        .catch_up_applied_sequence = 1000,
        .catch_up_target_sequence = 4000,
    };
    const initial = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(initial);
    initial[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = live_items },
    };
    try publishRefreshForTest(&cache, initial);

    const synthetic_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    synthetic_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    synthetic_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
    };
    const refresh = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(refresh);
    refresh[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = synthetic_items },
    };

    try publishRefreshForTest(&cache, refresh);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), docs.items.len);
    try std.testing.expectEqual(RuntimeStatusSource.cached_snapshot, docs.items[0].metadata.source);
    try std.testing.expectEqual(RuntimeStatusFreshness.stale, docs.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(u64, 250_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 250_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 2048), docs.items[0].stats.indexes[0].node_count);
    try std.testing.expectEqual(@as(u64, 1000), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(docs.items[0].stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(docs.items[0].stats.indexes[0].catch_up_active);
}

test "table runtime snapshot cache can clone a single group status" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const statuses = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 2);
    defer std.testing.allocator.free(statuses);
    statuses[0] = .{
        .group_id = 7,
        .stats = .{ .doc_count = 1, .indexes = &.{} },
    };
    statuses[1] = .{
        .group_id = 9,
        .stats = .{ .doc_count = 2, .indexes = &.{} },
    };
    const snapshots = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(snapshots);
    snapshots[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = statuses },
    };
    try publishRefreshForTest(&cache, snapshots);

    var status = (try cache.snapshotGroupStatus(std.testing.allocator, "docs", 9)).?;
    defer status.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 9), status.group_id);
    try std.testing.expectEqual(@as(u64, 2), status.stats.doc_count);
    try std.testing.expect((try cache.snapshotGroupStatus(std.testing.allocator, "docs", 8)) == null);
}

test "table runtime snapshot cache annotates publisher metadata defaults" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 1, .indexes = &.{} },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", status));

    var cloned = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer cloned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), cloned.items.len);
    try std.testing.expectEqual(RuntimeStatusSource.live_writer_publish, cloned.items[0].metadata.source);
    try std.testing.expectEqual(RuntimeStatusFreshness.fresh, cloned.items[0].metadata.freshness);
    try std.testing.expect(cloned.items[0].metadata.updated_at_ns > 0);
}

test "table runtime snapshot cache preserves dense visibility when live publish status regresses replay" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const cached_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    cached_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .node_count = 469,
        .root_node = 1,
        .coverage_generation = 42,
        .coverage_config_hash = 77,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 200,
        .replay_catch_up_required = true,
        .catch_up_applied_sequence = 100,
        .catch_up_target_sequence = 200,
        .hbc_cache = .{ .total_bytes = 1234, .accounted_bytes = 1234 },
    };
    var cached_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = cached_indexes,
        },
    };
    defer cached_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", cached_status));

    const regressed_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    regressed_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 1,
        .root_node = 1,
        .coverage_generation = 42,
        .coverage_config_hash = 77,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 0,
        .replay_target_sequence = 200,
        .replay_catch_up_required = true,
        .catch_up_phase = .bulk_finish,
        .catch_up_applied_sequence = 0,
        .catch_up_target_sequence = 200,
    };
    var regressed_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = regressed_indexes,
            .async_indexing = .{
                .dense_catch_up = .{
                    .begin_calls = 2,
                    .finish_calls = 1,
                    .active = true,
                    .phase = .bulk_finish,
                },
            },
        },
    };
    defer regressed_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", regressed_status));

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 25_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 25_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 469), docs.items[0].stats.indexes[0].node_count);
    try std.testing.expectEqual(@as(u64, 100), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 200), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(docs.items[0].stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(docs.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(db_mod.types.DenseCatchUpStats.Phase.bulk_finish, docs.items[0].stats.indexes[0].catch_up_phase);
    try std.testing.expectEqual(db_mod.types.DenseCatchUpStats.Phase.bulk_finish, docs.items[0].stats.async_indexing.dense_catch_up.phase);
}

test "table runtime snapshot cache allows dense visibility decrease with newer applied replay" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const cached_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    cached_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 100,
        .catch_up_applied_sequence = 100,
        .catch_up_target_sequence = 100,
    };
    var cached_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = cached_indexes,
        },
    };
    defer cached_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", cached_status));

    const newer_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    newer_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 24_999,
        .replay_applied_sequence = 101,
        .replay_target_sequence = 101,
        .catch_up_applied_sequence = 101,
        .catch_up_target_sequence = 101,
    };
    var newer_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 24_999,
            .index_count = 1,
            .indexes = newer_indexes,
        },
    };
    defer newer_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", newer_status));

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 24_999), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 24_999), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 101), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 101), docs.items[0].stats.indexes[0].replay_target_sequence);
}

test "table runtime snapshot cache rejects a late stale live observation" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    const current_token = try cache.capturePublicationToken("docs");

    const current = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current_token, "docs", current));

    const stale = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.stale_observation, try cache.publishGroup(stale_token, "docs", stale));

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(current_token.observation_generation, docs.items[0].cache_observation_generation);
}

test "table runtime snapshot cache preserves active managed admission proof" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "thumbnail",
        .kind = .dense_vector,
        .index_repair_id = 1,
        .index_repair_trigger = "projection_generation_invalid",
        .index_repair_phase = "detected",
        .index_repair_active_generation_serviceable = true,
    }};
    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .index_count = 1,
            .indexes = &indexes,
        },
    };
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try publishGroupForTest(&cache, "docs", status),
    );

    var snapshot = (try cache.snapshot(alloc, "docs")).?;
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot.items.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.items[0].stats.indexes.len);
    try std.testing.expect(snapshot.items[0].stats.indexes[0].index_repair_active_generation_serviceable);
}

test "table runtime snapshot cache invalidation fences a stale observed publisher" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    cache.invalidateTable("docs");

    const stale = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.stale_table, try cache.publishGroup(stale_token, "docs", stale));
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);

    const current_token = try cache.capturePublicationToken("docs");
    const current = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current_token, "docs", current));

    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(current_token.observation_generation, docs.items[0].cache_observation_generation);
}

test "table runtime snapshot cache publication fence preserves the last snapshot" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(stale_token, "docs", .{
            .group_id = 7,
            .stats = .{ .doc_count = 10 },
        }),
    );

    cache.fenceTablePublications("docs");
    var preserved = (try cache.snapshot(alloc, "docs")).?;
    defer preserved.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 10), preserved.items[0].stats.doc_count);

    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_table,
        try cache.publishGroup(stale_token, "docs", .{
            .group_id = 7,
            .stats = .{ .doc_count = 11 },
        }),
    );
}

test "targeted publication fence preserves only untouched siblings during catch up" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var published_indexes = [_]db_mod.types.DBIndexStats{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .doc_count = 2,
            .node_count = 1,
            .coverage_produced_count = 2,
            .coverage_config_hash = 99,
            .coverage_summary_ready = true,
            .coverage_generation = 42,
            .coverage_identity_ready = true,
        },
        .{ .name = "search_idx", .kind = .full_text, .doc_count = 2, .term_count = 4 },
        .{
            .name = "thumbnail",
            .kind = .dense_vector,
            .doc_count = 1,
            .node_count = 1,
            .coverage_produced_count = 1,
            .coverage_config_hash = 77,
            .coverage_summary_ready = true,
            .coverage_generation = 7,
            .coverage_identity_ready = true,
        },
    };
    const initial_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 2, .doc_count = 2, .index_count = published_indexes.len, .indexes = &published_indexes },
        }),
    );

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    var fenced = (try cache.snapshot(alloc, "docs")).?;
    defer fenced.deinit(alloc);
    try std.testing.expect(findMatchingIndexStatus(fenced.items[0].stats.indexes, "thumbnail", .dense_vector).?.runtime_observation_stale);
    try std.testing.expect(!findMatchingIndexStatus(fenced.items[0].stats.indexes, "semantic_idx", .dense_vector).?.runtime_observation_stale);

    var opening_indexes = [_]db_mod.types.DBIndexStats{
        .{ .name = "semantic_idx", .kind = .dense_vector },
        .{ .name = "search_idx", .kind = .full_text },
        .{ .name = "thumbnail", .kind = .dense_vector, .coverage_config_hash = 88, .coverage_generation = 8, .backfill_active = true },
    };
    const current_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(current_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 2, .index_count = opening_indexes.len, .indexes = &opening_indexes },
        }),
    );

    var merged = (try cache.snapshot(alloc, "docs")).?;
    defer merged.deinit(alloc);
    const semantic = findMatchingIndexStatus(merged.items[0].stats.indexes, "semantic_idx", .dense_vector).?;
    try std.testing.expectEqual(@as(u64, 2), semantic.doc_count);
    try std.testing.expectEqual(@as(u64, 42), semantic.coverage_generation);
    try std.testing.expect(semantic.runtime_observation_serviceable);
    try std.testing.expect(semantic.runtime_observation_targeted_sibling);
    const search = findMatchingIndexStatus(merged.items[0].stats.indexes, "search_idx", .full_text).?;
    try std.testing.expectEqual(@as(u64, 2), search.doc_count);
    try std.testing.expect(search.runtime_observation_targeted_sibling);
    const thumbnail = findMatchingIndexStatus(merged.items[0].stats.indexes, "thumbnail", .dense_vector).?;
    try std.testing.expectEqual(@as(u64, 0), thumbnail.doc_count);
    try std.testing.expect(!thumbnail.runtime_observation_serviceable);
    try std.testing.expect(!thumbnail.runtime_observation_targeted_sibling);

    cache.releaseTargetedIndexPublications("docs", "thumbnail", transition);
    try std.testing.expect(cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.transition_active);
    var fresh_indexes = [_]db_mod.types.DBIndexStats{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .doc_count = 2,
            .node_count = 1,
            .coverage_produced_count = 2,
            .coverage_config_hash = 99,
            .coverage_summary_ready = true,
            .coverage_generation = 42,
            .coverage_identity_ready = true,
        },
        .{ .name = "search_idx", .kind = .full_text, .doc_count = 2, .term_count = 4 },
        .{
            .name = "thumbnail",
            .kind = .dense_vector,
            .doc_count = 1,
            .node_count = 1,
            .coverage_produced_count = 1,
            .coverage_config_hash = 88,
            .coverage_summary_ready = true,
            .coverage_generation = 8,
            .coverage_identity_ready = true,
        },
    };
    const fresh_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(fresh_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 2, .doc_count = 2, .index_count = fresh_indexes.len, .indexes = &fresh_indexes },
        }),
    );
    // Freshness alone cannot retire an unknown desired target. Only the
    // structural publication may bind the incarnation and complete handoff.
    try std.testing.expect(cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.transition_active);
    const structural_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(structural_token, "docs", "thumbnail", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 2, .doc_count = 2, .index_count = fresh_indexes.len, .indexes = &fresh_indexes },
        }}),
    );
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.transition_active);
}

test "new targeted transition supersedes delayed controls from an older owner" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const older = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    const newer = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    var fence = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(older.revision != newer.revision);
    try std.testing.expect(fence.owner_active);
    try std.testing.expectEqual(@as(?u64, null), fence.release_after_observation_generation);

    // The old owner's completion cannot release or otherwise alter the newer
    // mutation's authority.
    try std.testing.expect(!cache.armTargetedIndexPublications("docs", "thumbnail", older));
    cache.acknowledgeTargetedIndexAbsence("docs", "thumbnail", older);
    fence = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(fence.expectation == .unknown);
    var current_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
    }};
    const publication = try cache.capturePublicationToken("docs");
    const current_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = 1, .indexes = current_indexes[0..] },
    };
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", newer, .{ .exact = .{
        .index_name = "thumbnail",
        .kind = .dense_vector,
        .incarnation = 13,
        .config_hash = 44,
    } }));
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_observation,
        try cache.publishTargetedGroupsForTransition(publication, older, "docs", "thumbnail", &.{current_status}),
    );
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroupsForTransition(publication, newer, "docs", "thumbnail", &.{current_status}),
    );
    fence = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(fence.expectation == .exact);
    cache.releaseTargetedIndexPublications("docs", "thumbnail", older);
    fence = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(fence.owner_active);
    try std.testing.expectEqual(@as(?u64, null), fence.release_after_observation_generation);

    cache.releaseTargetedIndexPublications("docs", "thumbnail", newer);
    fence = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(!fence.owner_active);
    try std.testing.expect(fence.release_after_observation_generation != null);

    // A token captured after owner release but before the repair callback is
    // not allowed to settle the newly observed durable repair edge.
    const racing_token = try cache.capturePublicationToken("docs");
    try std.testing.expect(cache.fenceIndexRepairPublications("docs", "thumbnail"));
    fence = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(fence.transition_revision != newer.revision);
    try std.testing.expect(!fence.target_authority_handed_off);
    try std.testing.expect(fence.accept_target_after_observation_generation > racing_token.observation_generation);
    cache.releaseTargetedIndexPublications("docs", "thumbnail", newer);
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.owner_active);
}

test "targeted catch up hands off same incarnation serving authority" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var published_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .doc_count = 3,
        .node_count = 1,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 3,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = published_indexes[0..] },
        }),
    );

    _ = cache.fenceTargetedIndexPublications("docs", "thumbnail");
    const catch_up_token = try cache.capturePublicationToken("docs");
    var catching_up_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .backfill_active = true,
    }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(catch_up_token, "docs", "thumbnail", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = catching_up_indexes[0..] },
        }}),
    );

    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    const target = findIndexStatusByName(observed.stats.indexes, "thumbnail").?;
    try std.testing.expect(!target.runtime_observation_stale);
    try std.testing.expect(target.runtime_observation_serviceable);
    try std.testing.expectEqual(@as(u64, 3), target.doc_count);
    try std.testing.expectEqual(@as(u64, 3), target.coverage_produced_count);
    try std.testing.expect(cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.target_authority_handed_off);
}

test "failed exact handoff accepts identity without fabricating serving authority" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "thumbnail", transition));
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, .{ .exact = .{
        .index_name = "thumbnail",
        .kind = .dense_vector,
        .incarnation = 12,
        .config_hash = 44,
    } }));
    var failed_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .load_error = @constCast("InvalidIndexConfig"),
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const publication = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishTargetedGroupsForTransition(
        publication,
        transition,
        "docs",
        "thumbnail",
        &.{
            .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .failed }, .stats = .{ .index_count = 1, .indexes = failed_indexes[0..] } },
            .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .failed }, .stats = .{ .index_count = 1, .indexes = failed_indexes[0..] } },
        },
    ));

    const authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(authority.target_authority_handed_off);
    try std.testing.expectEqual(@as(usize, 2), authority.handoff_groups.count());
    // Persistent identity authority may retain a serving proof, but it cannot
    // create one. Repeating the same failed incarnation after handoff must
    // remain non-serviceable.
    const repeated = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(
        repeated,
        "docs",
        &.{
            .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .failed }, .stats = .{ .index_count = 1, .indexes = failed_indexes[0..] } },
            .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .failed }, .stats = .{ .index_count = 1, .indexes = failed_indexes[0..] } },
        },
    ));
    var observed = (try cache.snapshot(alloc, "docs")).?;
    defer observed.deinit(alloc);
    for (observed.items) |status| {
        const target = findIndexStatusByName(status.stats.indexes, "thumbnail").?;
        try std.testing.expect(!target.runtime_observation_stale);
        try std.testing.expect(!target.runtime_observation_serviceable);
        try std.testing.expectEqualStrings("InvalidIndexConfig", target.load_error.?);
    }
}

test "target authority settles only after every group acknowledges the exact incarnation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var old_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .serving_snapshot_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroups(initial, "docs", &.{
            .{
                .group_id = 7,
                .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
                .stats = .{ .index_count = 1, .indexes = old_indexes[0..] },
            },
            .{
                .group_id = 8,
                .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
                .stats = .{ .index_count = 1, .indexes = old_indexes[0..] },
            },
        }),
    );

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "thumbnail", transition));
    var desired_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .serving_snapshot_ready = true,
    }};
    const first_group = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(first_group, "docs", "thumbnail", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = desired_indexes[0..] },
        }}),
    );
    cache.releaseTargetedIndexPublications("docs", "thumbnail", transition);
    var authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(authority.transition_active);
    try std.testing.expect(!authority.target_authority_handed_off);

    const final_group = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(final_group, "docs", .{
            .group_id = 8,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = desired_indexes[0..] },
        }),
    );
    authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(authority.target_authority_handed_off);
    try std.testing.expect(authority.transition_active);

    // Exact identity handoff and transition settlement are separate gates:
    // every group must also publish once after the last mutation owner exits.
    const post_release_first_group = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(post_release_first_group, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = desired_indexes[0..] },
        }),
    );
    authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expect(!authority.transition_active);
    try std.testing.expectEqual(@as(usize, 0), cache.tables.get("docs").?.active_index_transition_count);
}

test "target authority uses bound catalog groups instead of cached group count" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var predecessor = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 11,
        .coverage_config_hash = 33,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(initial, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = 1, .indexes = predecessor[0..] },
    }));

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "thumbnail", transition));
    const expectation: TableRuntimeSnapshotCache.TargetedIndexExpectation = .{ .exact = .{
        .index_name = "thumbnail",
        .kind = .dense_vector,
        .incarnation = 12,
        .config_hash = 44,
    } };
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, expectation));
    try std.testing.expect(cache.bindTargetedIndexExpectedGroups("docs", "thumbnail", transition, &.{ 7, 8 }));

    var replacement = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .doc_count = 1,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 1,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const group_a = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(group_a, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = 1, .indexes = replacement[0..] },
    }));
    try std.testing.expect(cache.targetedIndexGroupAcknowledged("docs", "thumbnail", expectation, 7));
    try std.testing.expect(!cache.targetedIndexAuthorityHandedOff("docs", "thumbnail", transition));

    const group_b = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(group_b, "docs", .{
        .group_id = 8,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = 1, .indexes = replacement[0..] },
    }));
    try std.testing.expect(cache.targetedIndexGroupAcknowledged("docs", "thumbnail", expectation, 8));
    try std.testing.expect(cache.targetedIndexAuthorityHandedOff("docs", "thumbnail", transition));
}

test "resident serving acknowledgement hands off a separate coordinator cache" {
    const alloc = std.testing.allocator;
    var coordinator = TableRuntimeSnapshotCache.init(alloc);
    defer coordinator.deinit();
    var resident = TableRuntimeSnapshotCache.init(alloc);
    defer resident.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .coverage_generation = 42,
        .coverage_config_hash = 84,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .serving_snapshot_ready = true,
        .runtime_observation_serviceable = true,
    }};
    for ([_]*TableRuntimeSnapshotCache{ &coordinator, &resident }) |cache| {
        const initial = try cache.capturePublicationToken("docs");
        try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
            initial,
            "docs",
            .{
                .group_id = 7001,
                .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
                .stats = .{ .index_count = 1, .indexes = indexes[0..] },
            },
        ));
    }
    const expected = TableRuntimeSnapshotCache.TargetedIndexExpectation{ .exact = .{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 42,
        .config_hash = 84,
    } };
    const expected_groups = [_]u64{7001};

    const coordinator_transition = coordinator.fenceTargetedIndexPublications("docs", "semantic").?;
    try std.testing.expect(coordinator.bindTargetedIndexExpectation("docs", "semantic", coordinator_transition, expected));
    try std.testing.expect(coordinator.bindTargetedIndexExpectedGroups("docs", "semantic", coordinator_transition, &expected_groups));

    const resident_transition = resident.fenceTargetedIndexPublications("docs", "semantic").?;
    try std.testing.expect(resident.bindTargetedIndexExpectation("docs", "semantic", resident_transition, expected));
    try std.testing.expect(resident.bindTargetedIndexExpectedGroups("docs", "semantic", resident_transition, &expected_groups));
    const resident_observation = try resident.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try resident.publishTargetedGroupsForTransition(
        resident_observation,
        resident_transition,
        "docs",
        "semantic",
        &.{.{
            .group_id = 7001,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = indexes[0..] },
        }},
    ));
    const serviceable = resident.targetedIndexGroupServiceability(
        "docs",
        "semantic",
        expected,
        7001,
    ).?;
    try std.testing.expect(serviceable);
    try std.testing.expect(coordinator.acknowledgeTargetedIndexGroup(
        "docs",
        "semantic",
        coordinator_transition,
        expected,
        7001,
        serviceable,
    ));
    try std.testing.expect(coordinator.targetedIndexAuthorityHandedOff(
        "docs",
        "semantic",
        coordinator_transition,
    ));
    var observed = (try coordinator.snapshotGroupStatus(alloc, "docs", 7001)).?;
    defer observed.deinit(alloc);
    const semantic = findIndexStatusByName(observed.stats.indexes, "semantic").?;
    try std.testing.expect(!semantic.runtime_observation_stale);
    try std.testing.expect(semantic.runtime_observation_serviceable);
}

test "targeted publication rejects a completed stale incarnation until structural acknowledgement" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var old_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 3,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 3, .doc_count = 3, .index_count = 1, .indexes = old_indexes[0..] },
        }),
    );

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic_idx").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic_idx", transition));

    // A retiring worker can complete after the catalog mutation boundary. Its
    // post-fence timestamp does not make the deleted incarnation current.
    const retiring_owner = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(retiring_owner, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 3, .doc_count = 3, .index_count = 1, .indexes = old_indexes[0..] },
        }),
    );
    var fenced = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer fenced.deinit(alloc);
    const stale = findIndexStatusByName(fenced.stats.indexes, "semantic_idx").?;
    try std.testing.expect(stale.runtime_observation_stale);
    try std.testing.expect(!stale.runtime_observation_serviceable);
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx").?.target_authority_handed_off);

    var new_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const structural_owner = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(structural_owner, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 3, .index_count = 1, .indexes = new_indexes[0..] },
        }}),
    );
    var acknowledged = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer acknowledged.deinit(alloc);
    const current = findIndexStatusByName(acknowledged.stats.indexes, "semantic_idx").?;
    try std.testing.expectEqual(@as(u64, 13), current.coverage_generation);
    try std.testing.expect(!current.runtime_observation_stale);
    try std.testing.expect(cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx").?.target_authority_handed_off);
}

test "settled target authority preserves the accepted incarnation against late publishers" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var old_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 3,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 3, .doc_count = 3, .index_count = 1, .indexes = old_indexes[0..] },
        }),
    );

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic_idx").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic_idx", transition));
    var new_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 1,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const structural = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(structural, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 3, .doc_count = 1, .index_count = 1, .indexes = new_indexes[0..] },
        }}),
    );
    cache.releaseTargetedIndexPublications("docs", "semantic_idx", transition);
    const settle = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(settle, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 3, .doc_count = 1, .index_count = 1, .indexes = new_indexes[0..] },
        }),
    );
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx").?.transition_active);

    // Persistent identity authority rejects superseded incarnations, but it
    // must not freeze ordinary publication progress from the accepted one.
    var progressed_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 4,
        .serving_snapshot_ready = true,
        .serving_snapshot_revision = 2,
        .coverage_produced_count = 4,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 4,
        .replay_target_sequence = 4,
    }};
    const progress = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(progress, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 4, .doc_count = 4, .index_count = 1, .indexes = progressed_indexes[0..] },
        }),
    );

    // Stale target data is ignored as an index-local delta; unrelated table
    // and sibling facts remain eligible to advance.
    const late = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(late, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 3, .doc_count = 3, .index_count = 1, .indexes = old_indexes[0..] },
        }),
    );
    const omitted = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(omitted, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 999 },
        }),
    );
    var wrong_kind_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .full_text,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
    }};
    const wrong_kind = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(wrong_kind, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 999, .index_count = 1, .indexes = wrong_kind_indexes[0..] },
        }),
    );
    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    const current = findIndexStatusByName(observed.stats.indexes, "semantic_idx").?;
    try std.testing.expectEqual(@as(u64, 13), current.coverage_generation);
    try std.testing.expectEqual(@as(u64, 4), current.doc_count);
    try std.testing.expectEqual(@as(u64, 4), current.coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 4), current.replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 999), observed.stats.source_doc_count);
    try std.testing.expect(!current.runtime_observation_stale);
    try std.testing.expect(current.runtime_observation_serviceable);
}

test "accepted authority requires identity containment not equal cardinality" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var cached_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("accepted_a"),
        .kind = .dense_vector,
        .doc_count = 7,
        .serving_snapshot_ready = true,
        .coverage_generation = 11,
        .coverage_config_hash = 101,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .doc_count = 7, .index_count = 1, .indexes = cached_indexes[0..] },
        }),
    );

    // Model two persistent catalog authorities on a newly repopulated group:
    // the cached owner observed only A and a partial incoming owner observes
    // only B. Cache teardown owns and frees these authority names.
    const state = cache.tables.get("docs").?;
    {
        const owned_name = try alloc.dupe(u8, "accepted_a");
        errdefer alloc.free(owned_name);
        try state.index_authorities.put(alloc, owned_name, .{
            .transition_revision = 1,
            .transition_active = false,
            .accept_target_after_observation_generation = 1,
            .expectation = .{ .exact = .{
                .kind = .dense_vector,
                .incarnation = 11,
                .config_hash = 101,
            } },
        });
    }
    {
        const owned_name = try alloc.dupe(u8, "accepted_b");
        errdefer alloc.free(owned_name);
        try state.index_authorities.put(alloc, owned_name, .{
            .transition_revision = 2,
            .transition_active = false,
            .accept_target_after_observation_generation = 1,
            .expectation = .{ .exact = .{
                .kind = .sparse_vector,
                .incarnation = 22,
                .config_hash = 202,
            } },
        });
    }

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("accepted_b"),
        .kind = .sparse_vector,
        .coverage_generation = 22,
        .coverage_config_hash = 202,
        .coverage_identity_ready = true,
    }};
    const disjoint = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(disjoint, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .doc_count = 22, .index_count = 1, .indexes = incoming_indexes[0..] },
        }),
    );

    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 22), observed.stats.doc_count);
    try std.testing.expect(findIndexStatusByName(observed.stats.indexes, "accepted_b") != null);
    try std.testing.expectEqual(@as(usize, 2), observed.stats.indexes.len);
    const retained = findIndexStatusByName(observed.stats.indexes, "accepted_a").?;
    try std.testing.expectEqual(@as(u64, 11), retained.coverage_generation);
    try std.testing.expectEqual(@as(u64, 7), retained.doc_count);
}

test "accepted group identity survives late predecessor before global handoff" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var old_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .coverage_generation = 1,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(initial, "docs", &.{
        .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = 1, .indexes = old_indexes[0..] } },
        .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = 1, .indexes = old_indexes[0..] } },
    }));

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic", transition));
    var replacement = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_generation = 2,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const structural = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishTargetedGroups(
        structural,
        "docs",
        "semantic",
        &.{.{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = 1, .indexes = replacement[0..] } }},
    ));
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("semantic").?.target_authority_handed_off);

    // A late generic owner reports the predecessor for group A. The group
    // publication is accepted as a delta, but the exact replacement row is
    // retained even though group B has not acknowledged it yet.
    const late = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(late, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = 1, .indexes = old_indexes[0..] },
    }));
    var group_a = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer group_a.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), findIndexStatusByName(group_a.stats.indexes, "semantic").?.coverage_generation);

    // Matching numeric fields with the wrong kind do not complete handoff.
    var wrong_kind = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .sparse_vector,
        .coverage_generation = 2,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const wrong = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(wrong, "docs", .{
        .group_id = 8,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = 1, .indexes = wrong_kind[0..] },
    }));
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("semantic").?.target_authority_handed_off);
}

test "catalog identity rejects a wrong first structural observation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic", transition));
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "semantic", transition, .{ .exact = .{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 22,
        .config_hash = 91,
    } }));

    var wrong_kind = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .sparse_vector,
        .coverage_generation = 22,
        .coverage_config_hash = 91,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const wrong = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_observation,
        try cache.publishTargetedGroupsForTransition(wrong, transition, "docs", "semantic", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = wrong_kind[0..] },
        }}),
    );
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("semantic").?.target_authority_handed_off);

    const absent = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_observation,
        try cache.publishTargetedGroupsForTransition(absent, transition, "docs", "semantic", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{},
        }}),
    );
    try std.testing.expect(cache.tables.get("docs").?.index_authorities.getPtr("semantic").?.expectation == .exact);
}

test "exact target advances fence only their index incarnation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{
        .{
            .name = @constCast("title_body"),
            .kind = .dense_vector,
            .coverage_generation = 10,
            .coverage_config_hash = 100,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .replay_target_sequence = 7,
        },
        .{
            .name = @constCast("thumbnail"),
            .kind = .dense_vector,
            .coverage_generation = 20,
            .coverage_config_hash = 200,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .replay_target_sequence = 7,
        },
    };
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(initial, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .target_observation_revision = 7 },
        .stats = .{ .index_count = 2, .indexes = indexes[0..] },
    }));

    const title_identity = TableRuntimeSnapshotCache.IndexIdentity{
        .index_name = "title_body",
        .kind = .dense_vector,
        .incarnation = 10,
        .config_hash = 100,
    };
    cache.markIndexTargetObservationPending("docs", 7, title_identity, 8);
    var pending = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    try std.testing.expect(pending.metadata.target_observation_complete);
    try std.testing.expect(!findIndexStatusByName(pending.stats.indexes, "title_body").?.runtime_target_observation_complete);
    try std.testing.expect(findIndexStatusByName(pending.stats.indexes, "thumbnail").?.runtime_target_observation_complete);
    pending.deinit(alloc);

    indexes[0].replay_target_sequence = 8;
    const current = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .target_observation_revision = 8 },
        .stats = .{ .index_count = 2, .indexes = indexes[0..] },
    }));

    // A delayed event for the retired incarnation is ignored.
    cache.markIndexTargetObservationPending("docs", 7, .{
        .index_name = "title_body",
        .kind = .dense_vector,
        .incarnation = 9,
        .config_hash = 100,
    }, 9);
    var complete = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    try std.testing.expect(findIndexStatusByName(complete.stats.indexes, "title_body").?.runtime_target_observation_complete);
    try std.testing.expect(findIndexStatusByName(complete.stats.indexes, "thumbnail").?.runtime_target_observation_complete);
    complete.deinit(alloc);

    // Unknown scope remains conservative and fences the whole group.
    cache.markGroupTargetObservationPending("docs", 7, 9);
    var unknown = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer unknown.deinit(alloc);
    try std.testing.expect(!unknown.metadata.target_observation_complete);
}

test "exact target advance before first status remains index scoped" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    // Capture before the commit callback to model a status read already in
    // flight while the table has not published its first runtime group.
    const stale_capture = try cache.capturePublicationToken("docs");
    cache.markIndexTargetObservationPending("docs", 7, .{
        .index_name = "title_body",
        .kind = .dense_vector,
        .incarnation = 10,
        .config_hash = 100,
    }, 8);

    var indexes = [_]db_mod.types.DBIndexStats{
        .{
            .name = @constCast("title_body"),
            .kind = .dense_vector,
            .coverage_generation = 10,
            .coverage_config_hash = 100,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .replay_target_sequence = 8,
        },
        .{
            .name = @constCast("thumbnail"),
            .kind = .dense_vector,
            .coverage_generation = 20,
            .coverage_config_hash = 200,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .replay_target_sequence = 8,
        },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(stale_capture, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .target_observation_revision = 8 },
        .stats = .{ .index_count = 2, .indexes = indexes[0..] },
    }));
    var pending = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    try std.testing.expect(!findIndexStatusByName(pending.stats.indexes, "title_body").?.runtime_target_observation_complete);
    try std.testing.expect(findIndexStatusByName(pending.stats.indexes, "thumbnail").?.runtime_target_observation_complete);
    pending.deinit(alloc);

    const current_capture = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current_capture, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .target_observation_revision = 8 },
        .stats = .{ .index_count = 2, .indexes = indexes[0..] },
    }));
    var complete = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer complete.deinit(alloc);
    try std.testing.expect(findIndexStatusByName(complete.stats.indexes, "title_body").?.runtime_target_observation_complete);
    try std.testing.expect(findIndexStatusByName(complete.stats.indexes, "thumbnail").?.runtime_target_observation_complete);
}

test "exact target advance allocation failure remains fail closed" {
    for (0..2) |successful_target_allocations| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        const alloc = failing.allocator();
        var cache = TableRuntimeSnapshotCache.init(alloc);
        defer cache.deinit();

        var indexes = [_]db_mod.types.DBIndexStats{.{
            .name = @constCast("semantic"),
            .kind = .dense_vector,
            .coverage_generation = 7,
            .coverage_config_hash = 17,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .runtime_target_observation_complete = true,
        }};
        const initial = try cache.capturePublicationToken("docs");
        try std.testing.expectEqual(
            TableRuntimeSnapshotCache.PublishResult.published,
            try cache.publishGroup(initial, "docs", .{
                .group_id = 3,
                .metadata = .{
                    .source = .live_writer_publish,
                    .freshness = .fresh,
                    .target_observation_complete = true,
                },
                .stats = .{ .index_count = 1, .indexes = indexes[0..] },
            }),
        );

        // Fail either the owned authority name or its map insertion. The
        // conservative group fallback may encounter the same exhausted
        // allocator and retire the snapshot; both outcomes are fail closed.
        failing.fail_index = failing.alloc_index + successful_target_allocations;
        cache.markIndexTargetObservationPending("docs", 3, .{
            .index_name = "semantic",
            .kind = .dense_vector,
            .incarnation = 7,
            .config_hash = 17,
        }, 9);
        try std.testing.expect(failing.has_induced_failure);

        const state = cache.tables.get("docs") orelse continue;
        const observed = state.groups.getPtr(3) orelse continue;
        const item = findIndexStatusByName(observed.stats.indexes, "semantic").?;
        try std.testing.expect(!observed.metadata.target_observation_complete or
            !item.runtime_target_observation_complete);
    }
}

test "batched target advance stops after fallback invalidates cached state" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{
        .{
            .name = @constCast("semantic"),
            .kind = .dense_vector,
            .coverage_generation = 7,
            .coverage_config_hash = 17,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
        },
        .{
            .name = @constCast("thumbnail"),
            .kind = .dense_vector,
            .coverage_generation = 8,
            .coverage_config_hash = 18,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
        },
    };
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(initial, "docs", .{
        .group_id = 3,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = indexes.len, .indexes = indexes[0..] },
    }));

    // Let the event lookup allocate, then exhaust the allocator while the
    // first identity attempts to establish exact authority. Its group-wide
    // fallback also fails and retires the cached observations. Processing the
    // second identity must stop instead of dereferencing those retired rows.
    failing.fail_index = failing.alloc_index + 1;
    const target_batch = [_]TableRuntimeSnapshotCache.IndexIdentity{
        TableRuntimeSnapshotCache.IndexIdentity{
            .index_name = "semantic",
            .kind = .dense_vector,
            .incarnation = 7,
            .config_hash = 17,
        },
        TableRuntimeSnapshotCache.IndexIdentity{
            .index_name = "thumbnail",
            .kind = .dense_vector,
            .incarnation = 8,
            .config_hash = 18,
        },
    };
    cache.markIndexTargetsObservationPending("docs", 3, target_batch[0..], 9);
    try std.testing.expect(failing.has_induced_failure);
    const state = cache.tables.get("docs").?;
    try std.testing.expectEqual(@as(usize, 0), state.groups.count());
    try std.testing.expectEqual(@as(usize, 0), state.index_authorities.count());
}

test "per-index delta merge stays bounded across many accepted authorities" {
    const alloc = std.testing.allocator;
    const index_count = 512;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, index_count);
    defer alloc.free(indexes);
    defer for (indexes) |item| alloc.free(@constCast(item.name));
    for (indexes, 0..) |*item, i| {
        const name = try std.fmt.allocPrint(alloc, "index-{d}", .{i});
        item.* = .{
            .name = name,
            .kind = .dense_vector,
            .coverage_generation = @intCast(i + 1),
            .coverage_config_hash = @intCast(i + 1000),
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
        };
    }
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(initial, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .index_count = index_count, .indexes = indexes },
    }));
    const state = cache.tables.get("docs").?;
    try state.index_authorities.ensureTotalCapacity(alloc, index_count);
    for (indexes) |item| {
        const owned_name = try alloc.dupe(u8, item.name);
        state.index_authorities.putAssumeCapacityNoClobber(owned_name, .{
            .transition_revision = 0,
            .transition_active = false,
            .owner_active = false,
            .accept_target_after_observation_generation = 0,
            .expectation = .{ .exact = .{
                .kind = item.kind,
                .incarnation = item.coverage_generation,
                .config_hash = item.coverage_config_hash,
            } },
            .target_authority_handed_off = true,
        });
    }

    // Empty ordinary delta retains all independently keyed observations. A
    // quadratic accepted-set containment pass would make this test scale with
    // the square of the configured index count.
    const delta = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(delta, "docs", .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{},
    }));
    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    try std.testing.expectEqual(index_count, observed.stats.indexes.len);
}

test "bulk target handoff reduces per-group acknowledgements linearly" {
    const alloc = std.testing.allocator;
    const index_count = 256;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, index_count);
    defer alloc.free(indexes);
    defer for (indexes) |item| alloc.free(@constCast(item.name));
    for (indexes, 0..) |*item, i| {
        item.* = .{
            .name = try std.fmt.allocPrint(alloc, "index-{d}", .{i}),
            .kind = .dense_vector,
            .coverage_generation = @intCast(i + 1),
            .coverage_config_hash = @intCast(i + 1000),
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .serving_snapshot_ready = true,
        };
    }
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(initial, "docs", &.{
        .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = index_count, .indexes = indexes } },
        .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = index_count, .indexes = indexes } },
    }));

    // Establish many simultaneous exact transitions directly so setup cost
    // does not dominate the publication contract under test.
    const state = cache.tables.get("docs").?;
    try state.index_authorities.ensureTotalCapacity(alloc, index_count);
    const boundary = cache.next_observation_generation;
    for (indexes, 0..) |item, i| {
        const owned_name = try alloc.dupe(u8, item.name);
        var authority = TargetedIndexAuthority{
            .transition_revision = @intCast(i + 1),
            .accept_target_after_observation_generation = boundary,
            .expectation = .{ .exact = .{
                .kind = item.kind,
                .incarnation = item.coverage_generation,
                .config_hash = item.coverage_config_hash,
            } },
            .expectation_observation_generation = boundary,
        };
        try authority.handoff_groups.ensureTotalCapacity(alloc, 2);
        state.index_authorities.putAssumeCapacityNoClobber(owned_name, authority);
    }
    state.active_index_transition_count = index_count;

    const handoff = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(handoff, "docs", &.{
        .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = index_count, .indexes = indexes } },
        .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = index_count, .indexes = indexes } },
    }));

    var authority_it = state.index_authorities.valueIterator();
    while (authority_it.next()) |authority| {
        try std.testing.expect(authority.target_authority_handed_off);
        try std.testing.expectEqual(@as(usize, 2), authority.handoff_groups.count());
    }
    var observed = (try cache.snapshot(alloc, "docs")).?;
    defer observed.deinit(alloc);
    for (observed.items) |status| for (status.stats.indexes) |item| {
        try std.testing.expect(!item.runtime_observation_stale);
        try std.testing.expect(item.runtime_observation_serviceable);
    };
}

test "incremental group acknowledgement sync is linear with one global handoff" {
    const alloc = std.testing.allocator;
    const group_count = 64;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();
    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "semantic",
        .kind = .dense_vector,
        .coverage_generation = 42,
        .coverage_config_hash = 77,
        .coverage_identity_ready = true,
        .serving_snapshot_ready = true,
    }};
    const statuses = try alloc.alloc(LocalTableRuntimeStatus, group_count);
    defer alloc.free(statuses);
    const group_ids = try alloc.alloc(u64, group_count);
    defer alloc.free(group_ids);
    for (statuses, group_ids, 0..) |*status, *group_id, index| {
        group_id.* = @intCast(index + 1);
        status.* = .{
            .group_id = group_id.*,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = &indexes },
        };
    }
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishLifecycleTransition(initial, "docs", statuses),
    );
    const transition = cache.fenceTargetedIndexPublications("docs", "semantic").?;
    const expected = TableRuntimeSnapshotCache.TargetedIndexExpectation{ .exact = .{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 42,
        .config_hash = 77,
    } };
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "semantic", transition, expected));
    try std.testing.expect(cache.bindTargetedIndexExpectedGroups("docs", "semantic", transition, group_ids));

    test_authority_sync_index_visits.store(0, .release);
    for (group_ids) |group_id|
        try std.testing.expect(cache.acknowledgeTargetedIndexGroup(
            "docs",
            "semantic",
            transition,
            expected,
            group_id,
            true,
        ));
    const visits = test_authority_sync_index_visits.load(.acquire);
    try std.testing.expect(visits <= (2 * group_count));
    try std.testing.expect(cache.targetedIndexAuthorityHandedOff("docs", "semantic", transition));
}

test "multi-group delta allocation failure is atomic and leak free" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var cache = TableRuntimeSnapshotCache.init(alloc);
            // This test injects failures into the atomic status-state commit;
            // the independently best-effort read mirror has its own leak-
            // checked allocator so an expected mirror refresh cannot swallow
            // the injection intended for the commit path.
            cache.read_view_alloc = std.testing.allocator;
            defer cache.deinit();

            var accepted = [_]db_mod.types.DBIndexStats{.{
                .name = @constCast("semantic"),
                .kind = .dense_vector,
                .coverage_generation = 5,
                .coverage_config_hash = 15,
                .coverage_identity_ready = true,
                .coverage_summary_ready = true,
                .doc_count = 3,
            }};
            const initial = try cache.capturePublicationToken("docs");
            try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(initial, "docs", &.{
                .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .doc_count = 10, .index_count = 1, .indexes = accepted[0..] } },
                .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .doc_count = 10, .index_count = 1, .indexes = accepted[0..] } },
            }));
            const update = try cache.capturePublicationToken("docs");
            _ = cache.publishGroups(update, "docs", &.{
                .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .doc_count = 20 } },
                .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .doc_count = 20 } },
            }) catch |err| switch (err) {
                error.OutOfMemory => {},
                else => return err,
            };

            var observed = (try cache.snapshot(alloc, "docs")) orelse {
                // A mirror allocation may be the injected failure. The
                // authoritative commit must still be all-old or all-new;
                // the next successful publication recreates the read view.
                const state = cache.tables.get("docs").?;
                const first = state.groups.get(7).?.stats.doc_count;
                const second = state.groups.get(8).?.stats.doc_count;
                try std.testing.expectEqual(first, second);
                try std.testing.expect(first == 10 or first == 20);
                return;
            };
            defer observed.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), observed.items.len);
            try std.testing.expect(observed.items[0].stats.doc_count == observed.items[1].stats.doc_count);
            try std.testing.expect(observed.items[0].stats.doc_count == 10 or observed.items[0].stats.doc_count == 20);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "multi-group commit uses preflight group capacity without allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();
    var cache = TableRuntimeSnapshotCache.init(alloc);
    cache.read_view_alloc = std.testing.allocator;
    defer cache.deinit();

    _ = try cache.capturePublicationToken("docs");
    const state = cache.tables.get("docs").?;
    try state.groups.ensureTotalCapacity(alloc, 512);
    const initial_capacity = state.groups.capacity();
    const initial_maximum_count = (initial_capacity * std.hash_map.default_max_load_percentage) / 100;
    for (0..initial_maximum_count) |index| {
        const group_id: u64 = @intCast(index + 1);
        state.groups.putAssumeCapacityNoClobber(group_id, .{
            .group_id = group_id,
            .cache_observation_generation = 1,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{},
        });
    }
    try std.testing.expectEqual(initial_maximum_count, state.groups.count());

    const owned_target_name = try alloc.dupe(u8, "retired-index");
    try state.index_authorities.put(alloc, owned_target_name, .{
        .transition_revision = 1,
        .accept_target_after_observation_generation = cache.next_observation_generation,
        .expectation = .absent,
        .expectation_observation_generation = cache.next_observation_generation,
    });
    state.active_index_transition_count = 1;

    const FailAfterPreparation = struct {
        allocator: *std.testing.FailingAllocator,

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.allocator.fail_index = self.allocator.alloc_index;
        }
    };
    var hook = FailAfterPreparation{ .allocator = &failing };
    test_after_structural_publish_preparation_hook = .{ .ptr = &hook, .run = FailAfterPreparation.run };
    defer test_after_structural_publish_preparation_hook = null;

    const new_group_id: u64 = @intCast(initial_maximum_count + 1);
    const update = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(update, "docs", &.{.{
        .group_id = new_group_id,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{},
    }}));
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expect(state.groups.contains(new_group_id));
    try std.testing.expectEqual(
        @as(usize, 1),
        state.index_authorities.getPtr("retired-index").?.handoff_groups.count(),
    );
}

test "prepared index delta commit performs no allocation" {
    const alloc = std.testing.allocator;
    var cached_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .coverage_generation = 5,
        .coverage_config_hash = 15,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .serving_snapshot_ready = true,
        .doc_count = 3,
        .index_repair_last_error = "cached failure",
    }};
    var previous = try (LocalTableRuntimeStatus{
        .group_id = 7,
        .cache_observation_generation = 11,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{ .runtime_owner_id = 1, .index_count = 1, .indexes = cached_indexes[0..] },
    }).clone(alloc);
    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .coverage_generation = 5,
        .coverage_config_hash = 15,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .serving_snapshot_ready = false,
        .index_repair_last_error = "incoming failure",
    }};
    var incoming = try (LocalTableRuntimeStatus{
        .group_id = 7,
        .cache_observation_generation = 12,
        .metadata = .{ .source = .live_writer_publish, .freshness = .catching_up },
        .stats = .{ .runtime_owner_id = 1, .index_count = 1, .indexes = incoming_indexes[0..] },
    }).clone(alloc);
    var incoming_lookup = try IndexObservationLookup.init(alloc, incoming.stats.indexes);
    defer incoming_lookup.deinit(alloc);
    var authorities = std.StringHashMapUnmanaged(TargetedIndexAuthority).empty;
    defer authorities.deinit(alloc);
    var workspace = try IndexDeltaMergeWorkspace.init(alloc, 11, 1, 1, 1);

    moveIndexObservationDeltaLocked(
        &previous,
        &incoming,
        incoming_lookup,
        &authorities,
        null,
        &workspace,
    );
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try preserveArtifactVisibilityUsingLookup(
        failing.allocator(),
        &previous,
        &incoming,
        &authorities,
        false,
        null,
        workspace.previous_lookup,
        true,
    );
    finishMovedIndexObservationDeltaLocked(&previous, &incoming, &workspace);
    workspace.deinit(alloc);
    defer previous.deinit(alloc);

    try std.testing.expect(previous.stats.indexes[0].serving_snapshot_ready);
    try std.testing.expectEqual(@as(u64, 3), previous.stats.indexes[0].doc_count);
    try std.testing.expectEqualStrings("cached failure", previous.stats.indexes[0].index_repair_last_error.?);
}

test "targeted authority binding is monotonic under reversed publication order" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic_idx").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic_idx", transition));
    const older_token = try cache.capturePublicationToken("docs");
    const newer_token = try cache.capturePublicationToken("docs");
    var older_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
    }};
    var newer_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
    }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(newer_token, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = newer_indexes[0..] },
        }}),
    );
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_observation,
        try cache.publishTargetedGroups(older_token, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = older_indexes[0..] },
        }}),
    );
    const late_old_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_observation,
        try cache.publishTargetedGroups(late_old_token, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = older_indexes[0..] },
        }}),
    );
    const authority = cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx").?;
    switch (authority.expectation) {
        .exact => |identity| try std.testing.expectEqual(@as(u64, 13), identity.incarnation),
        .unknown, .absent => return error.TestUnexpectedResult,
    }
}

test "wholly stale targeted publication cannot bind unknown authority" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var old_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = old_indexes[0..] },
        }),
    );

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic_idx").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic_idx", transition));
    const delayed_structural = try cache.capturePublicationToken("docs");
    const newer_owner = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(newer_owner, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = old_indexes[0..] },
        }),
    );

    var desired_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_generation = 13,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
    }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_observation,
        try cache.publishTargetedGroups(delayed_structural, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = desired_indexes[0..] },
        }}),
    );
    var authority = cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx").?;
    try std.testing.expect(authority.expectation == .unknown);

    const current_structural = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(current_structural, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = desired_indexes[0..] },
        }}),
    );
    authority = cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx").?;
    switch (authority.expectation) {
        .exact => |identity| try std.testing.expectEqual(@as(u64, 13), identity.incarnation),
        .unknown, .absent => return error.TestUnexpectedResult,
    }
}

test "targeted deletion hands off only after authoritative absence" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = indexes[0..] },
        }),
    );

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic_idx").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic_idx", transition));
    cache.acknowledgeTargetedIndexAbsence("docs", "semantic_idx", transition);
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx").?.target_authority_handed_off);
    cache.releaseTargetedIndexPublications("docs", "semantic_idx", transition);

    const absent = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroupsForTransition(absent, transition, "docs", "semantic_idx", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{},
        }}),
    );
    // Settled deletion state is represented by the authoritative catalog and
    // group absence; it does not leave an unbounded per-name tombstone behind.
    try std.testing.expect(cache.tables.get("docs").?.index_authorities.getPtr("semantic_idx") == null);
    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    try std.testing.expect(findIndexStatusByName(observed.stats.indexes, "semantic_idx") == null);

    // A retiring writer may still be observed internally, but the deleted name
    // is not catalog-addressable and cannot become serviceable by itself.
    const late = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(late, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = indexes[0..] },
        }),
    );
    var resurrected = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer resurrected.deinit(alloc);
    const retired = findIndexStatusByName(resurrected.stats.indexes, "semantic_idx").?;
    try std.testing.expect(!retired.runtime_observation_serviceable);

    // Reusing the name establishes a new revision before the create becomes
    // visible, immediately fencing the retired row without relying on history.
    _ = cache.fenceTargetedIndexPublications("docs", "semantic_idx").?;
    var recreated = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer recreated.deinit(alloc);
    try std.testing.expect(findIndexStatusByName(recreated.stats.indexes, "semantic_idx").?.runtime_observation_stale);
}

test "table runtime snapshot cache batch publication is table epoch atomic" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    cache.invalidateTable("docs");
    const statuses = [_]LocalTableRuntimeStatus{
        .{ .group_id = 7, .stats = .{ .doc_count = 10 } },
        .{ .group_id = 8, .stats = .{ .doc_count = 20 } },
    };
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_table,
        try cache.publishGroups(stale_token, "docs", &statuses),
    );
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);
}

test "targeted structural publication cannot regress an untouched sibling generation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var published_indexes = [_]db_mod.types.DBIndexStats{
        .{
            .name = @constCast("semantic"),
            .kind = .dense_vector,
            .doc_count = 20,
            .serving_snapshot_ready = true,
            .serving_snapshot_revision = 4,
            .serving_snapshot_owner_id = 77,
            .coverage_produced_count = 20,
            .coverage_generation = 41,
            .coverage_config_hash = 91,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .projection_checkpoint_generation = 7,
            .projection_checkpoint_applied_sequence = 20,
            .projection_checkpoint_config_hash = 91,
        },
        .{
            .name = @constCast("thumbnail"),
            .kind = .dense_vector,
            .coverage_generation = 11,
            .coverage_config_hash = 33,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
        },
    };
    const initial_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 77, .source_doc_count = 100, .doc_count = 20, .index_count = 2, .indexes = published_indexes[0..] },
        }),
    );

    _ = cache.fenceTargetedIndexPublications("docs", "thumbnail");
    const structural_token = try cache.capturePublicationToken("docs");
    var structural_indexes = [_]db_mod.types.DBIndexStats{
        .{
            .name = @constCast("semantic"),
            .kind = .dense_vector,
            // The structural DB snapshot can lag the resident producer. It
            // observes the same sibling incarnation but only an older
            // progressive publication.
            .doc_count = 8,
            .serving_snapshot_ready = true,
            .serving_snapshot_revision = 2,
            .serving_snapshot_owner_id = 88,
            .coverage_produced_count = 8,
            .coverage_generation = 41,
            .coverage_config_hash = 91,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
            .projection_checkpoint_generation = 7,
            .projection_checkpoint_applied_sequence = 8,
            .projection_checkpoint_config_hash = 91,
        },
        .{
            .name = @constCast("thumbnail"),
            .kind = .dense_vector,
            .doc_count = 1,
            .serving_snapshot_ready = true,
            .coverage_produced_count = 1,
            .coverage_generation = 12,
            .coverage_config_hash = 44,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
        },
    };
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(structural_token, "docs", "thumbnail", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 88, .source_doc_count = 100, .doc_count = 8, .index_count = 2, .indexes = structural_indexes[0..] },
        }}),
    );

    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    const semantic = findIndexStatusByName(observed.stats.indexes, "semantic").?;
    const thumbnail = findIndexStatusByName(observed.stats.indexes, "thumbnail").?;
    try std.testing.expectEqual(@as(u64, 20), semantic.doc_count);
    try std.testing.expectEqual(@as(u64, 20), semantic.coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 77), semantic.serving_snapshot_owner_id);
    try std.testing.expect(semantic.runtime_observation_targeted_sibling);
    try std.testing.expectEqual(@as(u64, 12), thumbnail.coverage_generation);
    try std.testing.expectEqual(@as(u64, 1), thumbnail.doc_count);

    // Retaining the sibling payload must retain its serving owner too. A
    // delayed observation from that owner remains ordered even though the
    // structural sample changed the enclosing group owner and source target.
    // Without index-scoped ownership this publication could regress 20
    // searchable artifacts to 12.
    var delayed_sibling = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 12,
        .serving_snapshot_ready = true,
        .serving_snapshot_revision = 2,
        .serving_snapshot_owner_id = 77,
        .coverage_produced_count = 12,
        .coverage_generation = 41,
        .coverage_config_hash = 91,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .projection_checkpoint_generation = 8,
        .projection_checkpoint_applied_sequence = 12,
        .projection_checkpoint_config_hash = 91,
        .replay_applied_sequence = 4,
        .replay_target_sequence = 4,
    }};
    const delayed_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(delayed_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 77, .source_doc_count = 100, .index_count = 1, .indexes = delayed_sibling[0..] },
        }),
    );
    var after_delayed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer after_delayed.deinit(alloc);
    const retained = findIndexStatusByName(after_delayed.stats.indexes, "semantic").?;
    try std.testing.expectEqual(@as(u64, 20), retained.doc_count);
    try std.testing.expectEqual(@as(u64, 4), retained.serving_snapshot_revision);
    try std.testing.expectEqual(@as(u64, 77), retained.serving_snapshot_owner_id);
}

test "synthetic refresh cannot outrank targeted structural owner observation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var initial_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 11,
        .coverage_config_hash = 33,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = initial_indexes[0..] },
        }),
    );

    _ = cache.fenceTargetedIndexPublications("docs", "thumbnail");
    const structural_token = try cache.capturePublicationToken("docs");
    const synthetic_token = try cache.capturePublicationToken("docs");
    var synthetic_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_config_hash = 44,
    }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(synthetic_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .synthetic_config, .freshness = .stale, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = synthetic_indexes[0..] },
        }),
    );
    try std.testing.expectEqual(
        initial_token.observation_generation,
        cache.tables.get("docs").?.groups.getPtr(7).?.cache_observation_generation,
    );

    var structural_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .doc_count = 1,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 1,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(structural_token, "docs", "thumbnail", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = structural_indexes[0..] },
        }}),
    );

    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    const target = findIndexStatusByName(observed.stats.indexes, "thumbnail").?;
    try std.testing.expect(!target.runtime_observation_stale);
    try std.testing.expect(target.runtime_observation_serviceable);
    try std.testing.expectEqual(@as(u64, 1), target.doc_count);
    try std.testing.expect(cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.target_authority_handed_off);
}

test "background refresh completes exact multi-group target handoff incrementally" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var predecessor = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 11,
        .coverage_config_hash = 33,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(initial, "docs", &.{
        .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = 1, .indexes = predecessor[0..] } },
        .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = 1, .indexes = predecessor[0..] } },
    }));

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "thumbnail", transition));
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, .{ .exact = .{
        .index_name = "thumbnail",
        .kind = .dense_vector,
        .incarnation = 12,
        .config_hash = 44,
    } }));

    var replacement = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .doc_count = 1,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 1,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const group_a = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishTargetedGroupsForTransition(
        group_a,
        transition,
        "docs",
        "thumbnail",
        &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .source_doc_count = 1, .index_count = 1, .indexes = replacement[0..] },
        }},
    ));
    var authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expectEqual(@as(usize, 1), authority.handoff_groups.count());
    try std.testing.expect(!authority.target_authority_handed_off);

    // A catalog refresh may combine a retained/cache-only row for a group
    // which already acknowledged with a fresh raw owner row for the remaining
    // group. Only the raw row may add an acknowledgement; the retained first
    // acknowledgement must nevertheless survive the table replacement.
    const refresh_statuses = try alloc.alloc(LocalTableRuntimeStatus, 2);
    refresh_statuses[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .cached_snapshot, .freshness = .stale },
        .stats = try cloneDBStats(alloc, .{ .source_doc_count = 1, .index_count = 1, .indexes = replacement[0..] }),
    };
    refresh_statuses[1] = .{
        .group_id = 8,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = try cloneDBStats(alloc, .{ .source_doc_count = 1, .index_count = 1, .indexes = replacement[0..] }),
    };
    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = refresh_statuses },
    };
    try publishRefreshForTest(&cache, snapshots);

    authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expectEqual(@as(usize, 2), authority.handoff_groups.count());
    try std.testing.expect(authority.target_authority_handed_off);
    var observed = (try cache.snapshot(alloc, "docs")).?;
    defer observed.deinit(alloc);
    for (observed.items) |status| {
        const target = findIndexStatusByName(status.stats.indexes, "thumbnail").?;
        try std.testing.expect(target.runtime_observation_serviceable);
        try std.testing.expect(!target.runtime_observation_stale);
        try std.testing.expectEqual(@as(u64, 12), target.coverage_generation);
    }
}

test "background refresh exact target allocation failure is atomic and leak free" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var cache = TableRuntimeSnapshotCache.init(alloc);
            defer cache.deinit();

            var predecessor = [_]db_mod.types.DBIndexStats{.{
                .name = @constCast("thumbnail"),
                .kind = .dense_vector,
                .coverage_generation = 11,
                .coverage_config_hash = 33,
                .coverage_identity_ready = true,
                .coverage_summary_ready = true,
            }};
            const initial = try cache.capturePublicationToken("docs");
            try std.testing.expectEqual(
                TableRuntimeSnapshotCache.PublishResult.published,
                try cache.publishGroups(initial, "docs", &.{.{
                    .group_id = 7,
                    .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
                    .stats = .{ .index_count = 1, .indexes = predecessor[0..] },
                }}),
            );

            const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail") orelse
                return error.OutOfMemory;
            try std.testing.expect(cache.armTargetedIndexPublications("docs", "thumbnail", transition));
            try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, .{ .exact = .{
                .index_name = "thumbnail",
                .kind = .dense_vector,
                .incarnation = 12,
                .config_hash = 44,
            } }));

            const refresh_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            var refresh_items_owned = true;
            errdefer if (refresh_items_owned) {
                refresh_items[0].deinit(alloc);
                alloc.free(refresh_items);
            };
            refresh_items[0] = .{
                .group_id = 7,
                .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
                .stats = .{
                    .source_doc_count = 1,
                    .index_count = 1,
                },
            };
            const refresh_name = try alloc.dupe(u8, "thumbnail");
            var refresh_name_owned = true;
            errdefer if (refresh_name_owned) alloc.free(refresh_name);
            const refresh_indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
            refresh_indexes[0] = .{
                .name = refresh_name,
                .kind = .dense_vector,
                .doc_count = 1,
                .serving_snapshot_ready = true,
                .coverage_produced_count = 1,
                .coverage_generation = 12,
                .coverage_config_hash = 44,
                .coverage_identity_ready = true,
                .coverage_summary_ready = true,
            };
            refresh_items[0].stats.indexes = refresh_indexes;
            refresh_name_owned = false;
            const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
            defer alloc.free(snapshots);
            snapshots[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = refresh_items },
            };
            refresh_items_owned = false;
            publishRefreshForTest(&cache, snapshots) catch |err| switch (err) {
                error.OutOfMemory => {},
                else => return err,
            };

            var observed = (try cache.snapshot(alloc, "docs")).?;
            defer observed.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), observed.items.len);
            const target = findIndexStatusByName(observed.items[0].stats.indexes, "thumbnail").?;
            try std.testing.expect(target.coverage_generation == 11 or target.coverage_generation == 12);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "background refresh completes absent multi-group target handoff incrementally" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var predecessor = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 11,
        .coverage_config_hash = 33,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroups(initial, "docs", &.{
        .{ .group_id = 7, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = 1, .indexes = predecessor[0..] } },
        .{ .group_id = 8, .metadata = .{ .source = .live_writer_publish, .freshness = .fresh }, .stats = .{ .index_count = 1, .indexes = predecessor[0..] } },
    }));

    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "thumbnail", transition));
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "thumbnail", transition, .absent));

    const group_a = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishTargetedGroupsForTransition(
        group_a,
        transition,
        "docs",
        "thumbnail",
        &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{},
        }},
    ));
    var authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expectEqual(@as(usize, 1), authority.handoff_groups.count());
    try std.testing.expect(!authority.target_authority_handed_off);

    // The first group's exact absence acknowledgement survives a cache-only
    // refresh while the second group's raw owner absence completes handoff.
    const refresh_statuses = try alloc.alloc(LocalTableRuntimeStatus, 2);
    refresh_statuses[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .cached_snapshot, .freshness = .stale },
        .stats = .{},
    };
    refresh_statuses[1] = .{
        .group_id = 8,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{},
    };
    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = refresh_statuses },
    };
    try publishRefreshForTest(&cache, snapshots);

    authority = cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?;
    try std.testing.expectEqual(@as(usize, 2), authority.handoff_groups.count());
    try std.testing.expect(authority.target_authority_handed_off);
    var observed = (try cache.snapshot(alloc, "docs")).?;
    defer observed.deinit(alloc);
    for (observed.items) |status|
        try std.testing.expect(findIndexStatusByName(status.stats.indexes, "thumbnail") == null);
}

test "synthetic refresh preserves post-fence target facts before serving handoff" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    _ = try cache.capturePublicationToken("docs");
    const transition = cache.fenceTargetedIndexPublications("docs", "thumbnail").?;
    const owner_token = try cache.capturePublicationToken("docs");
    var owner_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_generation = 12,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 100,
        .backfill_active = true,
    }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishTargetedGroups(owner_token, "docs", "thumbnail", &.{.{
            .group_id = 7,
            .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = owner_indexes[0..] },
        }}),
    );
    try std.testing.expect(!cache.tables.get("docs").?.index_authorities.getPtr("thumbnail").?.target_authority_handed_off);
    cache.releaseTargetedIndexPublications("docs", "thumbnail", transition);

    const synthetic_token = try cache.capturePublicationToken("docs");
    var synthetic_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_config_hash = 44,
    }};
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(synthetic_token, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .synthetic_config, .freshness = .stale, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = synthetic_indexes[0..] },
        }),
    );

    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    const target = findIndexStatusByName(observed.stats.indexes, "thumbnail").?;
    try std.testing.expectEqual(@as(u64, 12), target.coverage_generation);
    try std.testing.expectEqual(@as(u64, 3), target.replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 100), target.replay_target_sequence);
    try std.testing.expect(target.runtime_observation_stale);
    try std.testing.expect(!target.runtime_observation_serviceable);
    try std.testing.expectEqual(RuntimeStatusFreshness.stale, observed.metadata.freshness);
}

test "table runtime snapshot cache lifecycle transition replaces and fences observations" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const initial_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroups(initial_token, "docs", &.{
            .{ .group_id = 7, .stats = .{ .doc_count = 10 } },
            .{ .group_id = 8, .stats = .{ .doc_count = 20 } },
        }),
    );

    const in_flight_token = try cache.capturePublicationToken("docs");
    const transition_token = try cache.capturePublicationToken("docs");
    // A data commit accepted after the structural owner captured its snapshot
    // must remain a convergence fence across the lifecycle replacement.
    cache.markGroupTargetObservationPending("docs", 7, null);
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishLifecycleTransition(transition_token, "docs", &.{
            .{
                .group_id = 7,
                .metadata = .{
                    .source = .startup_catch_up,
                    .freshness = .catching_up,
                },
                .stats = .{ .doc_count = 10 },
            },
        }),
    );

    const current_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        transition_token.table_epoch.root_generation,
        current_token.table_epoch.root_generation,
    );
    try std.testing.expect(
        transition_token.table_epoch.invalidation_epoch != current_token.table_epoch.invalidation_epoch,
    );
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_table,
        try cache.publishGroup(in_flight_token, "docs", .{
            .group_id = 8,
            .stats = .{ .doc_count = 21 },
        }),
    );

    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs.items.len);
    try std.testing.expectEqual(@as(u64, 7), docs.items[0].group_id);
    try std.testing.expectEqual(RuntimeStatusFreshness.catching_up, docs.items[0].metadata.freshness);
    try std.testing.expect(!docs.items[0].metadata.target_observation_complete);

    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(current_token, "docs", .{
            .group_id = 7,
            .stats = .{ .doc_count = 11 },
        }),
    );
    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    try std.testing.expect(observed.metadata.target_observation_complete);
}

test "runtime owner retirement preserves serving snapshot and fences convergence" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 9,
        .serving_snapshot_ready = true,
        .coverage_produced_count = 9,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .target_observation_complete = true,
                .target_observation_revision = 12,
            },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = indexes[0..] },
        }),
    );

    const in_flight = try cache.capturePublicationToken("docs");
    cache.markTableTargetObservationPending("docs");
    var ownerless = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    try std.testing.expect(!ownerless.metadata.target_observation_complete);
    try std.testing.expectEqual(@as(u64, 9), ownerless.stats.indexes[0].doc_count);
    try std.testing.expect(ownerless.stats.indexes[0].serving_snapshot_ready);
    ownerless.deinit(alloc);

    // A publisher that started before retirement may refresh counters, but it
    // cannot restore completion authority across the owner-loss fence.
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(in_flight, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .target_observation_complete = true,
                .target_observation_revision = 12,
            },
            .stats = .{ .source_doc_count = 100, .index_count = 1, .indexes = indexes[0..] },
        }),
    );
    var fenced = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer fenced.deinit(alloc);
    try std.testing.expect(!fenced.metadata.target_observation_complete);
    try std.testing.expectEqual(@as(u64, 9), fenced.stats.indexes[0].doc_count);
    try std.testing.expect(fenced.stats.indexes[0].serving_snapshot_ready);
}

test "same owner stale serving revision cannot regress an exact incarnation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var current_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 20,
        .node_count = 3,
        .serving_snapshot_ready = true,
        .serving_snapshot_revision = 4,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 10,
        .replay_target_sequence = 100,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = current_indexes[0..] },
        }),
    );

    // Publication completion order is not serving-state order. A delayed
    // sample from the same owner may carry newer convergence facts while its
    // immutable HBC snapshot predates the cached serving revision.
    var stale_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 18,
        .node_count = 2,
        .serving_snapshot_ready = true,
        .serving_snapshot_revision = 2,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 12,
        .replay_target_sequence = 100,
    }};
    const delayed = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(delayed, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = stale_indexes[0..] },
        }),
    );
    {
        var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
        defer observed.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 20), observed.stats.indexes[0].doc_count);
        try std.testing.expectEqual(@as(u64, 3), observed.stats.indexes[0].node_count);
        try std.testing.expectEqual(@as(u64, 4), observed.stats.indexes[0].serving_snapshot_revision);
        try std.testing.expectEqual(@as(u64, 12), observed.stats.indexes[0].replay_applied_sequence);
    }

    // A genuinely newer source target may reduce cardinality after an update
    // or delete; the cache must not turn a gauge into a high-water mark.
    stale_indexes[0].serving_snapshot_revision = 6;
    stale_indexes[0].replay_target_sequence = 101;
    const newer = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(newer, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = stale_indexes[0..] },
        }),
    );
    {
        var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
        defer observed.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 18), observed.stats.indexes[0].doc_count);
        try std.testing.expectEqual(@as(u64, 6), observed.stats.indexes[0].serving_snapshot_revision);
    }
}

test "owner replacement cannot regress serving facts at the same accepted source target" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var current_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 20,
        .node_count = 3,
        .serving_snapshot_ready = true,
        .serving_snapshot_revision = 8,
        .coverage_produced_count = 10,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 4,
        .replay_target_sequence = 4,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = current_indexes[0..] },
        }),
    );

    var replacement_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 14,
        .node_count = 2,
        .serving_snapshot_ready = true,
        // This revision belongs to a different process-local owner and is not
        // comparable with the prior value.
        .serving_snapshot_revision = 12,
        .coverage_produced_count = 7,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 4,
        .replay_target_sequence = 4,
    }};
    const replacement = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(replacement, "docs", .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
            .stats = .{ .runtime_owner_id = 88, .index_count = 1, .indexes = replacement_indexes[0..] },
        }),
    );

    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 20), observed.stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 3), observed.stats.indexes[0].node_count);
    try std.testing.expectEqual(@as(u64, 10), observed.stats.indexes[0].coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 4), observed.stats.indexes[0].replay_target_sequence);
}

test "irrelevant broad replay cursor cannot regress exact index serving or coverage facts" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var current_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 20,
        .node_count = 3,
        .serving_snapshot_ready = true,
        .serving_snapshot_revision = 6,
        .coverage_produced_count = 10,
        .coverage_skipped_count = 2,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 6,
        .replay_target_sequence = 6,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 6,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = current_indexes[0..] },
        }),
    );

    const identity = TableRuntimeSnapshotCache.IndexIdentity{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 42,
        .config_hash = 99,
    };
    cache.markIndexTargetObservationPending("docs", 7, identity, 6);
    const acknowledged = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(acknowledged, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 6,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = current_indexes[0..] },
        }),
    );

    // Coverage settlement has independent authority from serving cardinality.
    // A stale observer may retain the same HBC member count while forgetting
    // already classified source outcomes.
    var coverage_only_regression = current_indexes;
    coverage_only_regression[0].doc_count = 25;
    coverage_only_regression[0].node_count = 4;
    coverage_only_regression[0].serving_snapshot_revision = 7;
    coverage_only_regression[0].coverage_skipped_count = 0;
    coverage_only_regression[0].replay_applied_sequence = 7;
    coverage_only_regression[0].replay_target_sequence = 8;
    const coverage_only = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(coverage_only, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 8,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = coverage_only_regression[0..] },
        }),
    );
    {
        var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
        defer observed.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 25), observed.stats.indexes[0].doc_count);
        try std.testing.expectEqual(@as(u64, 4), observed.stats.indexes[0].node_count);
        try std.testing.expectEqual(@as(u64, 7), observed.stats.indexes[0].serving_snapshot_revision);
        try std.testing.expectEqual(@as(u64, 2), observed.stats.indexes[0].coverage_skipped_count);
        try std.testing.expectEqual(@as(u64, 8), observed.stats.indexes[0].replay_target_sequence);
    }

    // A record for a sibling index can advance the broad enrichment replay
    // cursor. It did not emit an exact target event for this incarnation, so
    // a cache-only zero must not replace its immutable serving projection.
    var irrelevant_indexes = current_indexes;
    irrelevant_indexes[0].doc_count = 0;
    irrelevant_indexes[0].node_count = 0;
    irrelevant_indexes[0].serving_snapshot_revision = 7;
    irrelevant_indexes[0].replay_applied_sequence = 7;
    irrelevant_indexes[0].replay_target_sequence = 8;
    const irrelevant = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(irrelevant, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 8,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = irrelevant_indexes[0..] },
        }),
    );
    {
        var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
        defer observed.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 25), observed.stats.indexes[0].doc_count);
        try std.testing.expectEqual(@as(u64, 4), observed.stats.indexes[0].node_count);
        try std.testing.expectEqual(@as(u64, 10), observed.stats.indexes[0].coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 8), observed.stats.indexes[0].replay_target_sequence);
    }

    // A real exact target edge clears the cached convergence proof before the
    // owner publishes its replacement, so a legitimate cardinality decrease
    // remains observable rather than becoming a permanent high-water mark.
    cache.markIndexTargetObservationPending("docs", 7, identity, 9);
    irrelevant_indexes[0].coverage_skipped_count = 0;
    irrelevant_indexes[0].serving_snapshot_revision = 8;
    irrelevant_indexes[0].replay_applied_sequence = 9;
    irrelevant_indexes[0].replay_target_sequence = 9;
    const relevant = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(relevant, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 9,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = irrelevant_indexes[0..] },
        }),
    );
    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), observed.stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 0), observed.stats.indexes[0].coverage_skipped_count);
    try std.testing.expectEqual(@as(u64, 8), observed.stats.indexes[0].serving_snapshot_revision);
}

test "exact additive target advance preserves serving facts while delete authority permits reduction" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .doc_count = 20,
        .node_count = 3,
        .publication_target_count = 20,
        .publication_target_ready = true,
        .serving_snapshot_ready = true,
        .serving_snapshot_revision = 4,
        .coverage_produced_count = 10,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .replay_applied_sequence = 4,
        .replay_target_sequence = 4,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(initial, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 4,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = indexes[0..] },
        }),
    );

    const Effect = enum { additive_only, may_reduce };
    const Target = struct {
        index_name: []const u8,
        kind: db_mod.types.IndexKind,
        incarnation: u64,
        config_hash: u64,
        serving_set_effect: Effect,
    };
    const additive = [_]Target{.{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 42,
        .config_hash = 99,
        .serving_set_effect = .additive_only,
    }};
    cache.markIndexTargetsObservationPending("docs", 7, additive[0..], 11);

    indexes[0].doc_count = 8;
    indexes[0].node_count = 2;
    indexes[0].publication_target_count = 6;
    indexes[0].serving_snapshot_revision = 7;
    indexes[0].coverage_produced_count = 6;
    indexes[0].replay_applied_sequence = 7;
    indexes[0].replay_target_sequence = 11;
    const additive_observation = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(additive_observation, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 11,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = indexes[0..] },
        }),
    );
    {
        var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
        defer observed.deinit(alloc);
        try std.testing.expect(observed.stats.indexes[0].runtime_target_observation_complete);
        try std.testing.expectEqual(@as(u64, 20), observed.stats.indexes[0].doc_count);
        try std.testing.expectEqual(@as(u64, 20), observed.stats.indexes[0].publication_target_count);
        try std.testing.expectEqual(@as(u64, 10), observed.stats.indexes[0].coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 11), observed.stats.indexes[0].replay_target_sequence);
    }

    const reducing = [_]Target{.{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 42,
        .config_hash = 99,
        .serving_set_effect = .may_reduce,
    }};
    cache.markIndexTargetsObservationPending("docs", 7, reducing[0..], 12);
    // A later additive commit can coalesce before the owner publishes. It
    // must not erase the still-pending permission established by the delete.
    cache.markIndexTargetsObservationPending("docs", 7, additive[0..], 13);
    indexes[0].doc_count = 6;
    indexes[0].node_count = 1;
    indexes[0].publication_target_count = 6;
    indexes[0].serving_snapshot_revision = 8;
    indexes[0].coverage_produced_count = 6;
    indexes[0].replay_applied_sequence = 13;
    indexes[0].replay_target_sequence = 13;
    const reducing_observation = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(reducing_observation, "docs", .{
            .group_id = 7,
            .metadata = .{
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = 9,
                .target_observation_revision = 13,
            },
            .stats = .{ .runtime_owner_id = 77, .index_count = 1, .indexes = indexes[0..] },
        }),
    );
    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    try std.testing.expect(observed.stats.indexes[0].runtime_target_observation_complete);
    try std.testing.expectEqual(@as(u64, 6), observed.stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 6), observed.stats.indexes[0].publication_target_count);
    try std.testing.expectEqual(@as(u64, 6), observed.stats.indexes[0].coverage_produced_count);
}

test "table runtime snapshot cache batch preserves newer group observations" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const batch_token = try cache.capturePublicationToken("docs");
    const newer_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(newer_token, "docs", .{ .group_id = 7, .stats = .{ .doc_count = 12 } }),
    );
    const statuses = [_]LocalTableRuntimeStatus{
        .{ .group_id = 7, .stats = .{ .doc_count = 10 } },
        .{ .group_id = 8, .stats = .{ .doc_count = 20 } },
    };
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroups(batch_token, "docs", &statuses),
    );

    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), docs.items.len);
    for (docs.items) |status| switch (status.group_id) {
        7 => {
            try std.testing.expectEqual(@as(u64, 12), status.stats.doc_count);
            try std.testing.expectEqual(newer_token.observation_generation, status.cache_observation_generation);
        },
        8 => {
            try std.testing.expectEqual(@as(u64, 20), status.stats.doc_count);
            try std.testing.expectEqual(batch_token.observation_generation, status.cache_observation_generation);
        },
        else => return error.UnexpectedRuntimeStatusGroup,
    };
}

test "runtime status group batches reject duplicate group ids before publication" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const duplicate = [_]LocalTableRuntimeStatus{
        .{ .group_id = 7, .stats = .{ .doc_count = 10 } },
        .{ .group_id = 8, .stats = .{ .doc_count = 20 } },
        .{ .group_id = 7, .stats = .{ .doc_count = 30 } },
    };
    const ordinary_token = try cache.capturePublicationToken("docs");
    try std.testing.expectError(
        error.DuplicateRuntimeStatusGroup,
        cache.publishGroups(ordinary_token, "docs", &duplicate),
    );
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);

    const lifecycle_token = try cache.capturePublicationToken("docs");
    try std.testing.expectError(
        error.DuplicateRuntimeStatusGroup,
        cache.publishLifecycleTransition(lifecycle_token, "docs", &duplicate),
    );
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);
}

test "table runtime snapshot cache live publication does not starve structural refresh" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const table_names = [_][]const u8{"docs"};
    var refresh_token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer refresh_token.deinit();
    const live_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(live_token, "docs", .{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    }));

    const statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    statuses[0] = .{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = statuses },
    };

    var refresh_result = try cache.publishRefresh(&refresh_token, snapshots);
    defer refresh_result.deinit();
    try std.testing.expect(!refresh_result.hasRejectedTables());
    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(live_token.observation_generation, docs.items[0].cache_observation_generation);
}

test "live writer artifact regression keeps authoritative source deletions" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .projection_checkpoint_applied_sequence = 2,
        .projection_checkpoint_config_hash = 77,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{
            .source_doc_count = 1,
            .doc_count = 1,
            .index_count = 1,
            .indexes = previous_indexes[0..],
        },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .projection_checkpoint_applied_sequence = 0,
        .projection_checkpoint_config_hash = 77,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{
            .source_doc_count = 0,
            .doc_count = 0,
            .index_count = 1,
            .indexes = incoming_indexes[0..],
        },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.source_doc_count);
    try std.testing.expectEqual(@as(u64, 1), incoming.stats.doc_count);
}

test "catching up observation preserves same-incarnation published visibility" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .doc_count = 2,
        .node_count = 1,
        .coverage_produced_count = 2,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{
            .source_doc_count = 2,
            .doc_count = 2,
            .index_count = 1,
            .indexes = previous_indexes[0..],
        },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{
            .source_doc_count = 2,
            .index_count = 1,
            .indexes = incoming_indexes[0..],
        },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    try std.testing.expectEqual(@as(u64, 2), incoming.stats.doc_count);
    try std.testing.expectEqual(@as(u64, 2), incoming.stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 1), incoming.stats.indexes[0].node_count);
    try std.testing.expect(incoming.stats.indexes[0].coverage_identity_ready);
    try std.testing.expect(incoming.stats.indexes[0].runtime_observation_serviceable);
    try std.testing.expect(incoming.stats.indexes[0].serving_snapshot_ready);
    try std.testing.expect(incoming.stats.indexes[0].coverage_summary_ready);
    try std.testing.expectEqual(@as(u64, 2), incoming.stats.indexes[0].coverage_produced_count);
}

test "projection continuity preserves lifecycle classification as one bundle" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .projection_checkpoint_applied_sequence = 10,
        .projection_checkpoint_generation = 4,
        .projection_checkpoint_config_hash = 77,
        .index_repair_id = 91,
        .index_lifecycle_work_class = .initial_build,
        .index_repair_trigger = "catalog_admission",
        .index_repair_phase = "building",
        .index_repair_status = .rebuilding,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{ .index_count = 1, .indexes = previous_indexes[0..] },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("thumbnail"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .projection_checkpoint_applied_sequence = 0,
        .projection_checkpoint_generation = 4,
        .projection_checkpoint_config_hash = 77,
        // A separately sampled lifecycle must not be combined with the
        // retained projection generation.
        .index_repair_id = 92,
        .index_lifecycle_work_class = .repair,
        .index_repair_trigger = "artifact_coverage_mismatch",
        .index_repair_phase = "preflight",
        .index_repair_status = .waiting,
        .index_repair_action_required = true,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{ .index_count = 1, .indexes = incoming_indexes[0..] },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    const retained = incoming.stats.indexes[0];
    try std.testing.expectEqual(@as(?u128, 91), retained.index_repair_id);
    try std.testing.expectEqual(db_mod.types.IndexLifecycleWorkClass.initial_build, retained.index_lifecycle_work_class);
    try std.testing.expectEqualStrings("catalog_admission", retained.index_repair_trigger);
    try std.testing.expectEqualStrings("building", retained.index_repair_phase);
    try std.testing.expectEqual(db_mod.types.IndexRepairStatus.rebuilding, retained.index_repair_status.?);
    try std.testing.expect(!retained.index_repair_action_required);
}

test "catching up observation cannot preserve a same-config replacement incarnation" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 2,
        .node_count = 1,
        .coverage_produced_count = 2,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{
            .source_doc_count = 2,
            .doc_count = 2,
            .index_count = 1,
            .indexes = previous_indexes[0..],
        },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 43,
        .coverage_identity_ready = true,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{
            .source_doc_count = 2,
            .index_count = 1,
            .indexes = incoming_indexes[0..],
        },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.doc_count);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 43), incoming.stats.indexes[0].coverage_generation);
    try std.testing.expect(!incoming.stats.indexes[0].runtime_observation_serviceable);
}

test "catching up observation cannot preserve across an lsm root change" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 2,
        .node_count = 1,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{ .doc_count = 2, .index_count = 1, .indexes = previous_indexes[0..] },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 10 },
        .stats = .{ .index_count = 1, .indexes = incoming_indexes[0..] },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.doc_count);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.indexes[0].doc_count);
    try std.testing.expect(!incoming.stats.indexes[0].runtime_observation_serviceable);
}

test "unpublished embeddings incarnation cannot mint catch up serviceability" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{ .source_doc_count = 2, .doc_count = 2, .index_count = 1, .indexes = previous_indexes[0..] },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{ .index_count = 1, .indexes = incoming_indexes[0..] },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    try std.testing.expect(!incoming.stats.indexes[0].runtime_observation_serviceable);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.source_doc_count);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.indexes[0].coverage_produced_count);
}

test "empty embeddings incarnation preserves serviceability during catch up" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{ .index_count = 1, .indexes = previous_indexes[0..] },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{ .index_count = 1, .indexes = incoming_indexes[0..] },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    try std.testing.expect(incoming.stats.indexes[0].runtime_observation_serviceable);
    try std.testing.expect(incoming.stats.indexes[0].serving_snapshot_ready);
    try std.testing.expect(incoming.stats.indexes[0].coverage_identity_ready);
    try std.testing.expect(incoming.stats.indexes[0].coverage_summary_ready);
}

test "synthetic relabel cannot reuse cached catch up serviceability" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .runtime_observation_serviceable = true,
        .doc_count = 2,
        .node_count = 1,
        .coverage_produced_count = 2,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{ .source_doc_count = 2, .doc_count = 2, .index_count = 1, .indexes = previous_indexes[0..] },
    };

    var placeholder_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
    }};
    for ([_]RuntimeStatusFreshness{ .stale, .catching_up }) |freshness| {
        const placeholder = LocalTableRuntimeStatus{
            .group_id = 7,
            .metadata = .{ .source = .synthetic_config, .freshness = freshness, .lsm_root_generation = 9 },
            .stats = .{ .index_count = 1, .indexes = placeholder_indexes[0..] },
        };

        var merged = try mergeCachedStatusWithSyntheticPlaceholder(std.testing.allocator, previous, placeholder, 100, null, false);
        defer merged.deinit(std.testing.allocator);
        try std.testing.expectEqual(freshness, merged.metadata.freshness);
        try std.testing.expect(!merged.stats.indexes[0].runtime_observation_serviceable);
    }
}

test "all-skipped embeddings incarnation preserves logical publication during catch up" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_skipped_count = 2,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh, .lsm_root_generation = 9 },
        .stats = .{ .source_doc_count = 2, .doc_count = 2, .index_count = 1, .indexes = previous_indexes[0..] },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .coverage_generation = 42,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .startup_catch_up, .freshness = .catching_up, .lsm_root_generation = 9 },
        .stats = .{ .source_doc_count = 2, .index_count = 1, .indexes = incoming_indexes[0..] },
    };

    try preserveArtifactVisibilityOnReplayRegression(std.testing.allocator, previous, &incoming, null, false, null);
    try std.testing.expect(incoming.stats.indexes[0].runtime_observation_serviceable);
    try std.testing.expect(incoming.stats.indexes[0].serving_snapshot_ready);
    try std.testing.expectEqual(@as(u64, 2), incoming.stats.indexes[0].coverage_skipped_count);
    try std.testing.expect(incoming.stats.indexes[0].coverage_summary_ready);
}

test "table runtime snapshot cache preserves live completion over regressing persisted projection" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var live_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .coverage_produced_count = 1,
        .coverage_config_hash = 77,
        .coverage_generation = 1,
        .coverage_identity_ready = true,
        .backfill_progress = 1.0,
        .projection_checkpoint_status = "clean",
        .projection_checkpoint_applied_sequence = 2,
        .projection_checkpoint_generation = 0,
        .projection_checkpoint_config_hash = 0,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    }};
    const live_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(live_token, "docs", .{
        .group_id = 7001,
        .stats = .{
            .source_doc_count = 1,
            .doc_count = 1,
            .index_count = 1,
            .indexes = live_indexes[0..],
        },
    }));

    const table_names = [_][]const u8{"docs"};
    var refresh_token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer refresh_token.deinit();
    const refresh_indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    refresh_indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .coverage_produced_count = 1,
        .coverage_config_hash = 77,
        .coverage_generation = 1,
        .coverage_identity_ready = true,
        .backfill_active = true,
        .backfill_progress = 0.5,
        .projection_checkpoint_status = "rebuilding",
        .projection_checkpoint_applied_sequence = 0,
        .projection_checkpoint_generation = 0,
        .projection_checkpoint_config_hash = 0,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    };
    const refresh_statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    refresh_statuses[0] = .{
        .group_id = 7001,
        .stats = .{
            .source_doc_count = 0,
            .doc_count = 1,
            .index_count = 1,
            .indexes = refresh_indexes,
        },
    };
    const refresh = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(refresh);
    refresh[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = refresh_statuses },
    };
    var refresh_result = try cache.publishRefresh(&refresh_token, refresh);
    defer refresh_result.deinit();

    var published = (try cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.source_doc_count);
    try std.testing.expectEqualStrings("clean", published.items[0].stats.indexes[0].projection_checkpoint_status);
    try std.testing.expectEqual(@as(u64, 2), published.items[0].stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expect(!published.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(f64, 1.0), published.items[0].stats.indexes[0].backfill_progress);
}

test "table runtime snapshot cache table fences isolate unrelated invalidations" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const docs_token = try cache.capturePublicationToken("docs");
    cache.invalidateTable("other");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        docs_token,
        "docs",
        .{ .group_id = 7, .stats = .{ .doc_count = 1 } },
    ));
}

test "table runtime snapshot cache replacement preserves a newer live observation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    const stale = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(stale_token, "docs", stale));

    const table_names = [_][]const u8{"docs"};
    var refresh_token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer refresh_token.deinit();

    // Model a refresh that cloned generation 1, then released the cache lock.
    const replacement = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(replacement);
    replacement[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = try alloc.alloc(LocalTableRuntimeStatus, 1) },
    };
    replacement[0].statuses.items[0] = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;

    const current_token = try cache.capturePublicationToken("docs");
    const current = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current_token, "docs", current));

    var refresh_result = try cache.publishRefresh(&refresh_token, replacement);
    defer refresh_result.deinit();
    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(current_token.observation_generation, docs.items[0].cache_observation_generation);
}

test "cached replay sequence alone is not a runtime fact" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .replay_applied_sequence = 4000,
        .replay_target_sequence = 4000,
        .catch_up_applied_sequence = 4000,
        .catch_up_target_sequence = 4000,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .cached_snapshot,
            .freshness = .fresh,
        },
        .stats = .{
            .index_count = 1,
            .indexes = indexes,
        },
    };

    try std.testing.expect(!statusHasRuntimeFacts(status));
}

test "synthetic status with preserved visibility counters is a runtime fact" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 1_000_000,
        .node_count = 8_837,
        .replay_applied_sequence = 10_002,
        .replay_target_sequence = 10_002,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 1_000_000,
            .index_count = 1,
            .indexes = indexes,
        },
    };

    try std.testing.expect(statusHasRuntimeFacts(status));
}

test "cached all-skipped coverage observation is a runtime fact" {
    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "visual",
        .kind = .dense_vector,
        .coverage_skipped_count = 2,
        .coverage_config_hash = 0x1234,
        .coverage_summary_ready = true,
    }};
    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .cached_snapshot, .freshness = .fresh },
        .stats = .{ .source_doc_count = 2, .index_count = 1, .indexes = indexes[0..] },
    };
    try std.testing.expect(statusHasRuntimeFacts(status));
}

test "cached identity and doc set telemetry are runtime facts" {
    const identity_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .cached_snapshot,
            .freshness = .stale,
        },
        .stats = .{
            .doc_identity = .{
                .rebuild_required = true,
            },
        },
    };

    const planning_status = LocalTableRuntimeStatus{
        .group_id = 8,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_set_planning = .{
                .stale_identity_generation_rejection_count = 1,
            },
        },
    };

    try std.testing.expect(statusHasRuntimeFacts(identity_status));
    try std.testing.expect(statusHasRuntimeFacts(planning_status));
}

test "cached repair telemetry is runtime facts" {
    const status = LocalTableRuntimeStatus{
        .group_id = 9,
        .metadata = .{
            .source = .cached_snapshot,
            .freshness = .stale,
        },
        .stats = .{
            .repair_degraded = true,
            .repair_issue_count = 1,
        },
    };

    try std.testing.expect(statusHasRuntimeFacts(status));
}

test "table runtime snapshot cache preserves generic artifact visibility on sequence-only refresh" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const cached_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    cached_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "text_idx"),
        .kind = .full_text,
        .doc_count = 10_000,
        .term_count = 321,
        .replay_applied_sequence = 400,
        .replay_target_sequence = 400,
    };
    var cached_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 10_000,
            .index_count = 1,
            .indexes = cached_indexes,
        },
    };
    defer cached_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", cached_status));

    const incoming_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    incoming_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "text_idx"),
        .kind = .full_text,
        .replay_applied_sequence = 400,
        .replay_target_sequence = 400,
    };
    var incoming_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .background_refresh,
            .freshness = .fresh,
        },
        .stats = .{
            .index_count = 1,
            .indexes = incoming_indexes,
        },
    };
    defer incoming_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", incoming_status));

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 10_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 10_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 321), docs.items[0].stats.indexes[0].term_count);
    try std.testing.expectEqual(@as(u64, 400), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 400), docs.items[0].stats.indexes[0].replay_target_sequence);
}

test "table runtime snapshot cache preserves existing status on replacement allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var cache = TableRuntimeSnapshotCache.init(alloc);
            defer cache.deinit();

            const initial_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            initial_items[0] = .{
                .group_id = 7,
                .stats = .{
                    .doc_count = 11,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            initial_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec"),
                .kind = .dense_vector,
                .doc_count = 11,
                .replay_applied_sequence = 5,
                .replay_target_sequence = 10,
                .replay_catch_up_required = true,
            };
            const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
            defer alloc.free(snapshots);
            snapshots[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = initial_items },
            };
            try publishRefreshForTest(&cache, snapshots);

            var replacement = LocalTableRuntimeStatus{
                .group_id = 7,
                .stats = .{
                    .doc_count = 99,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            defer replacement.deinit(alloc);
            replacement.stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec-replacement"),
                .kind = .dense_vector,
                .doc_count = 99,
            };

            _ = publishGroupForTest(&cache, "docs", replacement) catch |err| switch (err) {
                error.OutOfMemory => {},
            };

            var docs = (try cache.snapshot(alloc, "docs")).?;
            defer docs.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), docs.items.len);
            try std.testing.expectEqual(@as(u64, 11), docs.items[0].stats.doc_count);
            try std.testing.expectEqualStrings("vec", docs.items[0].stats.indexes[0].name);
            try std.testing.expectEqual(@as(u64, 5), docs.items[0].stats.indexes[0].replay_applied_sequence);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "read mirror allocation failure cannot attach replacement authority to predecessor identity" {
    const alloc = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    var cache = TableRuntimeSnapshotCache.init(alloc);
    cache.read_view_alloc = failing.allocator();
    defer cache.deinit();

    var predecessor = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_generation = 1,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .runtime_observation_serviceable = true,
    }};
    const initial = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        initial,
        "docs",
        .{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = predecessor[0..] },
        },
    ));

    const transition = cache.fenceTargetedIndexPublications("docs", "semantic").?;
    try std.testing.expect(cache.armTargetedIndexPublications("docs", "semantic", transition));
    try std.testing.expect(cache.bindTargetedIndexExpectation("docs", "semantic", transition, .{ .exact = .{
        .index_name = "semantic",
        .kind = .dense_vector,
        .incarnation = 2,
        .config_hash = 44,
    } }));

    var replacement = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic"),
        .kind = .dense_vector,
        .serving_snapshot_ready = true,
        .coverage_generation = 2,
        .coverage_config_hash = 44,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .runtime_observation_serviceable = true,
    }};
    failing.fail_index = failing.alloc_index;
    const publish = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishTargetedGroupsForTransition(
        publish,
        transition,
        "docs",
        "semantic",
        &.{.{
            .group_id = 7,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = replacement[0..] },
        }},
    ));
    try std.testing.expect(failing.has_induced_failure);

    // The authoritative mutable cache advanced. The best-effort read mirror
    // still contains generation 1, so it must remain explicitly fenced until
    // a later refresh can atomically install generation 2's payload.
    try std.testing.expectEqual(
        @as(u64, 2),
        cache.tables.get("docs").?.groups.get(7).?.stats.indexes[0].coverage_generation,
    );
    var observed = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;
    defer observed.deinit(alloc);
    const stale = findIndexStatusByName(observed.stats.indexes, "semantic").?;
    try std.testing.expectEqual(@as(u64, 1), stale.coverage_generation);
    try std.testing.expect(stale.runtime_observation_stale);
    try std.testing.expect(!stale.runtime_observation_serviceable);
    try std.testing.expect(!stale.runtime_target_observation_complete);
}

test "table runtime snapshot cache preserves previous snapshots when replace preserve install fails" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var cache = TableRuntimeSnapshotCache.init(alloc);
            defer cache.deinit();

            const initial_docs_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            initial_docs_items[0] = .{
                .group_id = 7,
                .stats = .{
                    .doc_count = 11,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            initial_docs_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec"),
                .kind = .dense_vector,
                .doc_count = 11,
                .replay_applied_sequence = 5,
                .replay_target_sequence = 10,
                .replay_catch_up_required = true,
            };
            const initial_logs_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            initial_logs_items[0] = .{
                .group_id = 8,
                .stats = .{
                    .doc_count = 2,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            initial_logs_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "kw"),
                .kind = .full_text,
                .doc_count = 2,
            };
            const initial = try alloc.alloc(TableRuntimeSnapshot, 2);
            defer alloc.free(initial);
            initial[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = initial_docs_items },
            };
            initial[1] = .{
                .table_name = try alloc.dupe(u8, "logs"),
                .statuses = .{ .items = initial_logs_items },
            };
            try publishRefreshForTest(&cache, initial);

            const refresh_docs_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            refresh_docs_items[0] = .{
                .group_id = 7,
                .stats = .{
                    .doc_count = 99,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            refresh_docs_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec-new"),
                .kind = .dense_vector,
                .doc_count = 99,
            };
            const refresh = try alloc.alloc(TableRuntimeSnapshot, 1);
            defer alloc.free(refresh);
            refresh[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = refresh_docs_items },
            };

            publishRefreshForTest(&cache, refresh) catch |err| switch (err) {
                error.OutOfMemory => {},
                else => return err,
            };

            var docs = (try cache.snapshot(alloc, "docs")).?;
            defer docs.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 11), docs.items[0].stats.doc_count);
            try std.testing.expectEqualStrings("vec", docs.items[0].stats.indexes[0].name);
            try std.testing.expectEqual(@as(u64, 5), docs.items[0].stats.indexes[0].replay_applied_sequence);

            var logs = (try cache.snapshot(alloc, "logs")).?;
            defer logs.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 2), logs.items[0].stats.doc_count);
            try std.testing.expectEqualStrings("kw", logs.items[0].stats.indexes[0].name);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "table runtime snapshot cache summarizes replay debt" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const docs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 2);
    docs_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 11,
            .index_count = 2,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 2),
        },
    };
    docs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 8,
        .replay_catch_up_required = true,
    };
    docs_items[0].stats.indexes[1] = .{
        .name = try std.testing.allocator.dupe(u8, "text"),
        .kind = .full_text,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 3,
    };
    docs_items[1] = .{
        .group_id = 8,
        .stats = .{
            .doc_count = 6,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    docs_items[1].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "graph"),
        .kind = .graph,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 4,
    };

    const logs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    logs_items[0] = .{
        .group_id = 9,
        .stats = .{
            .doc_count = 2,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    logs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search"),
        .kind = .full_text,
        .replay_applied_sequence = 9,
        .replay_target_sequence = 9,
    };

    const snapshots = try std.testing.allocator.alloc(TableRuntimeSnapshot, 2);
    snapshots[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = docs_items },
    };
    snapshots[1] = .{
        .table_name = try std.testing.allocator.dupe(u8, "logs"),
        .statuses = .{ .items = logs_items },
    };
    try publishRefreshForTest(&cache, snapshots);

    const summary = cache.summary();
    try std.testing.expectEqual(@as(usize, 2), summary.table_count);
    try std.testing.expectEqual(@as(usize, 3), summary.group_count);
    try std.testing.expectEqual(@as(usize, 4), summary.index_count);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_with_replay_debt);
    try std.testing.expectEqual(@as(usize, 2), summary.groups_with_replay_debt);
    try std.testing.expectEqual(@as(usize, 2), summary.indexes_with_replay_debt);
    try std.testing.expectEqual(@as(u64, 6), summary.outstanding_replay_sequences);
    try std.testing.expectEqual(@as(u64, 3), summary.max_index_replay_backlog);
}
