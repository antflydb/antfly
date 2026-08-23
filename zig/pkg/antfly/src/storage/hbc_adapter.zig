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

//! Storage-backed shell for the HBC (Hierarchical Balanced Clustering) vector
//! index engine in `lib/vectorindex`.
//!
//! The HBC engine itself now lives in `antfly_vectorindex`; this module owns:
//!   - backend opening/ownership
//!   - backend-neutral runtime transaction/store wiring
//!   - persistence layout for HBC namespaces
//!   - a thin storage facade over the library-owned engine
//!
//! HBC namespaces:
//!   "hbc_nodes"  - packed tree nodes
//!   "hbc_meta"   - index metadata
//!   "hbc_quant"  - node search payloads: raw leaf/root vector sets and RaBitQ internal-node sets
//!   "hbc_vecs"   - vector metadata and legacy/index-local raw vectors keyed by vector ID

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const hbc_backend = @import("hbc_backend.zig");
const posting_segment_store_mod = @import("posting_segment_store.zig");
const resource_manager_mod = @import("resource_manager.zig");
const apply_rw_lock_mod = @import("db/apply_rw_lock.zig");
const supports_lmdb = builtin.os.tag != .freestanding and build_options.lmdb_enabled;
const lmdb = if (supports_lmdb) @import("lmdb.zig") else struct {
    pub const Error = error{NotFound};
};
const lsm_backend = @import("lsm_backend/mod.zig");
const platform_time = @import("antfly_platform").time;
const vec = @import("antfly_vector").vector;
const proto = @import("antfly_vector").proto;
const quantizer_mod = @import("antfly_vector").quantizer;
const rabitq = @import("antfly_vector").rabitq;
const go_rand = @import("antfly_vector").go_rand;
const vectorindex_types = @import("antfly_vectorindex").types;
const vectorindex_bulk_build = @import("antfly_vectorindex").bulk_build;
const vectorindex_search_results = @import("antfly_vectorindex").search_results;
const vectorindex_search_types = @import("antfly_vectorindex").search_types;
const vectorindex_search = @import("antfly_vectorindex").search;
const vectorindex_search_runtime = @import("antfly_vectorindex").search_runtime;
const vectorindex_store = @import("antfly_vectorindex").store;
const vectorindex_hbc_runtime = @import("antfly_vectorindex").hbc_runtime;
const vectorindex_hbc = @import("antfly_vectorindex").hbc;
const vectorindex_hbc_index = @import("antfly_vectorindex").hbc_index;
const vectorindex_posting = @import("antfly_vectorindex").posting;
const vectorindex_posting_segment = @import("antfly_vectorindex").posting_segment;
const vectorindex_posting_wal = @import("antfly_vectorindex").posting_wal;
const vectorindex_hbc_vector_directory = @import("antfly_vectorindex").hbc_vector_directory;
const vectorindex_spfresh_index = @import("antfly_vectorindex").spfresh_index;
const vectorindex_hbc_transfer = @import("antfly_vectorindex").hbc_transfer;
const vectorindex_hbc_debug = @import("antfly_vectorindex").hbc_debug;

var temp_path_nonce: u64 = 0;
const default_deferred_hbc_leaf_splits_per_publish: usize = 256;
const default_bulk_split_vector_workspace_budget_bytes: u64 = 256 * 1024 * 1024;

pub const TestGetVectorViewOrScratchHook = *const fn (?*anyopaque, *HBCIndex, u64) void;
var test_get_vector_view_or_scratch_ctx: ?*anyopaque = null;
var test_get_vector_view_or_scratch_hook: ?TestGetVectorViewOrScratchHook = null;

pub fn setTestGetVectorViewOrScratchHook(ctx: ?*anyopaque, hook: ?TestGetVectorViewOrScratchHook) void {
    if (!builtin.is_test) return;
    test_get_vector_view_or_scratch_ctx = ctx;
    test_get_vector_view_or_scratch_hook = hook;
}

// ============================================================================
// Configuration
// ============================================================================

pub const HBCConfig = vectorindex_types.HBCConfig;
pub const StorageBackend = vectorindex_types.StorageBackend;
pub const BulkBuildAlgo = vectorindex_types.BulkBuildAlgo;
pub const LsmWriteStats = lsm_backend.Backend.WriteStats;
pub const LsmMaintenanceStats = lsm_backend.Backend.MaintenanceStats;
pub const LsmOpenStats = lsm_backend.Backend.OpenStats;

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.os.tag == .freestanding) {
            std.atomic.spinLoopHint();
        } else {
            std.Thread.yield() catch {};
        }
    }
}

fn hbcRuntimeBatchMode(in_bulk_session: bool, lsm_direct_bulk_ingest_enabled: ?bool) vectorindex_store.BatchMode {
    if (!in_bulk_session) return .default;
    const direct_bulk_ingest_enabled = lsm_direct_bulk_ingest_enabled orelse return .default;
    return if (direct_bulk_ingest_enabled) .default else .bulk_ingest;
}

// ============================================================================
// Index metadata (serialized to LMDB)
// ============================================================================

const meta_key = vectorindex_hbc.meta_key;
const posting_wal_authoritative_key = "hbc:posting-wal-authoritative-v1";
const posting_wal_authoritative_value = "1";
const bulk_publish_state_key = "__bulk_publish_state";
const bulk_publish_state_value = "incomplete";
pub const WriteSessionKind = enum {
    streaming_replay,
    bulk_publication,
};
const hbc_index_version = vectorindex_hbc.hbc_index_version;
const IndexMetadata = vectorindex_hbc.IndexMetadata;
pub const ProjectionCheckpointMetadata = vectorindex_hbc.ProjectionCheckpointMetadata;

// ============================================================================
// Node representation
// ============================================================================

pub const Node = vectorindex_types.Node;

// ============================================================================
// Priority queue item for search
// ============================================================================

pub const PriorityItem = vectorindex_types.PriorityItem;

const candidateLessThan = vectorindex_search_types.candidateLessThan;

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    const end_ns = nowNs();
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

fn elapsedSinceNs(start_ns: i128) u64 {
    const end_ns = nowNsI128();
    return @intCast(@max(end_ns - start_ns, 0));
}

fn nowNsI128() i128 {
    return @intCast(platform_time.monotonicNs());
}

fn isNotFound(err: anyerror) bool {
    return err == error.NotFound or (supports_lmdb and err == lmdb.Error.NotFound);
}

// ============================================================================
// Node key encoding
// ============================================================================

const Suffix = vectorindex_hbc.Suffix;
const encodeNodeKey = vectorindex_hbc.encodeNodeKey;
const encodeVecKey = vectorindex_hbc.encodeVecKey;
const encodeVecLeafKey = vectorindex_hbc.encodeVecLeafKey;
const encodeVecMetaKey = vectorindex_hbc.encodeVecMetaKey;
const encodeQuantKey = vectorindex_hbc.encodeQuantKey;

// ============================================================================
// Node header
// ============================================================================

const NodeHeader = vectorindex_hbc.NodeHeader;

pub const NodeSplitClass = vectorindex_types.NodeSplitClass;
pub const NodeSplitRange = vectorindex_types.NodeSplitRange;
pub const SplitPlanningStats = vectorindex_types.SplitPlanningStats;
pub const SplitReusePlan = vectorindex_types.SplitReusePlan;
pub const SplitRebuildWork = vectorindex_types.SplitRebuildWork;

const DeferredNodeValue = struct {
    value: ?[]u8 = null,

    fn deinit(self: *DeferredNodeValue, alloc: Allocator) void {
        if (self.value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const SplitMemberPlan = struct {
    right_only_members: []u64,
    mixed_right_members: []u64,

    pub fn deinit(self: *SplitMemberPlan, alloc: Allocator) void {
        alloc.free(self.right_only_members);
        alloc.free(self.mixed_right_members);
        self.* = undefined;
    }
};

const LeafKeyEntry = vectorindex_hbc_index.LeafKeyEntry;

const initNodeSplitRangeFromInput = vectorindex_bulk_build.initNodeSplitRangeFromInput;
const extendNodeSplitRangeFromInput = vectorindex_bulk_build.extendNodeSplitRangeFromInput;
const mergeNodeSplitRanges = vectorindex_bulk_build.mergeNodeSplitRanges;
const planBalancedGroupSizes = vectorindex_bulk_build.planBalancedGroupSizes;
const encodeNodeRange = vectorindex_bulk_build.encodeNodeRange;
const decodeNodeRange = vectorindex_bulk_build.decodeNodeRange;

const QuantizedSet = vectorindex_hbc_runtime.QuantizedSet;

const HbcCacheKind = enum {
    node,
    quantized,
    vector,
    metadata,
};

const hbc_cache_kind_count: usize = 4;

pub const HbcCacheKindStats = struct {
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    insertions: u64 = 0,
    admission_skips: u64 = 0,
    evictions: u64 = 0,
};

pub const HbcCacheStats = struct {
    total_bytes: u64 = 0,
    accounted_bytes: u64 = 0,
    node: HbcCacheKindStats = .{},
    quantized: HbcCacheKindStats = .{},
    vector: HbcCacheKindStats = .{},
    metadata: HbcCacheKindStats = .{},
};

const NodeCacheEntry = struct {
    refs: std.atomic.Value(u32) = .init(1),
    node: Node,
};

const QuantizedCacheEntry = struct {
    refs: std.atomic.Value(u32) = .init(1),
    quantized: QuantizedSet,
};

const VectorCacheEntry = struct {
    refs: std.atomic.Value(u32) = .init(1),
    vector: []f32,
};

const MetadataCacheEntry = struct {
    refs: std.atomic.Value(u32) = .init(1),
    metadata: []u8,
};

fn retainNodeCacheEntry(entry: *NodeCacheEntry) void {
    _ = entry.refs.fetchAdd(1, .acq_rel);
}

fn releaseNodeCacheEntry(alloc: Allocator, entry: *NodeCacheEntry) void {
    if (entry.refs.fetchSub(1, .acq_rel) == 1) {
        var node = entry.node;
        node.deinit(alloc);
        alloc.destroy(entry);
    }
}

fn retainQuantizedCacheEntry(entry: *QuantizedCacheEntry) void {
    _ = entry.refs.fetchAdd(1, .acq_rel);
}

fn releaseQuantizedCacheEntry(alloc: Allocator, entry: *QuantizedCacheEntry) void {
    if (entry.refs.fetchSub(1, .acq_rel) == 1) {
        var quantized = entry.quantized;
        quantized.deinit(alloc);
        alloc.destroy(entry);
    }
}

fn retainVectorCacheEntry(entry: *VectorCacheEntry) void {
    _ = entry.refs.fetchAdd(1, .acq_rel);
}

fn releaseVectorCacheEntry(alloc: Allocator, entry: *VectorCacheEntry) void {
    if (entry.refs.fetchSub(1, .acq_rel) == 1) {
        alloc.free(entry.vector);
        alloc.destroy(entry);
    }
}

fn retainMetadataCacheEntry(entry: *MetadataCacheEntry) void {
    _ = entry.refs.fetchAdd(1, .acq_rel);
}

fn releaseMetadataCacheEntry(alloc: Allocator, entry: *MetadataCacheEntry) void {
    if (entry.refs.fetchSub(1, .acq_rel) == 1) {
        alloc.free(entry.metadata);
        alloc.destroy(entry);
    }
}

const BorrowedNodeLease = union(enum) {
    locked: struct {
        lock: *apply_rw_lock_mod.ApplyRwLock,
        node: *const Node,
    },
    retained: struct {
        alloc: Allocator,
        entry: *NodeCacheEntry,
    },

    pub fn ptr(self: *const BorrowedNodeLease) *const Node {
        return switch (self.*) {
            .locked => |lease| lease.node,
            .retained => |lease| &lease.entry.node,
        };
    }

    pub fn deinit(self: *BorrowedNodeLease) void {
        switch (self.*) {
            .locked => |lease| lease.lock.unlockShared(),
            .retained => |lease| releaseNodeCacheEntry(lease.alloc, lease.entry),
        }
        self.* = undefined;
    }
};

const BorrowedQuantizedLease = union(enum) {
    locked: struct {
        lock: *apply_rw_lock_mod.ApplyRwLock,
        quantized: *const QuantizedSet,
    },
    retained: struct {
        alloc: Allocator,
        entry: *QuantizedCacheEntry,
    },

    pub fn ptr(self: *const BorrowedQuantizedLease) *const QuantizedSet {
        return switch (self.*) {
            .locked => |lease| lease.quantized,
            .retained => |lease| &lease.entry.quantized,
        };
    }

    pub fn deinit(self: *BorrowedQuantizedLease) void {
        switch (self.*) {
            .locked => |lease| lease.lock.unlockShared(),
            .retained => |lease| releaseQuantizedCacheEntry(lease.alloc, lease.entry),
        }
        self.* = undefined;
    }
};

const BorrowedVectorLease = union(enum) {
    locked: struct {
        lock: *apply_rw_lock_mod.ApplyRwLock,
        vector: []const f32,
    },
    retained: struct {
        alloc: Allocator,
        entry: *VectorCacheEntry,
    },

    pub fn view(self: *const BorrowedVectorLease) []const f32 {
        return switch (self.*) {
            .locked => |lease| lease.vector,
            .retained => |lease| lease.entry.vector,
        };
    }

    pub fn deinit(self: *BorrowedVectorLease) void {
        switch (self.*) {
            .locked => |lease| lease.lock.unlockShared(),
            .retained => |lease| releaseVectorCacheEntry(lease.alloc, lease.entry),
        }
        self.* = undefined;
    }
};

const BorrowedMetadataLease = union(enum) {
    locked: struct {
        lock: *apply_rw_lock_mod.ApplyRwLock,
        metadata: []const u8,
    },
    retained: struct {
        alloc: Allocator,
        entry: *MetadataCacheEntry,
    },

    pub fn view(self: *const BorrowedMetadataLease) []const u8 {
        return switch (self.*) {
            .locked => |lease| lease.metadata,
            .retained => |lease| lease.entry.metadata,
        };
    }

    pub fn deinit(self: *BorrowedMetadataLease) void {
        switch (self.*) {
            .locked => |lease| lease.lock.unlockShared(),
            .retained => |lease| releaseMetadataCacheEntry(lease.alloc, lease.entry),
        }
        self.* = undefined;
    }
};

const HbcSharedCacheKey = struct {
    namespace: u64,
    id: u64,
};

const HbcSharedClockEntry = struct {
    key: HbcSharedCacheKey = .{ .namespace = 0, .id = 0 },
    referenced: bool = false,
};

const HbcSharedAdmission = struct {
    bytes: u64 = 0,
    reserved: bool = false,
    overcommitted: bool = false,
};

fn hbcCacheNamespace(path: []const u8) u64 {
    const hash = std.hash.Wyhash.hash(0xa6f9_19e5_cace_f00d, path);
    return if (hash == 0) 1 else hash;
}

fn hbcCacheStablePathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    if (comptime builtin.os.tag == .freestanding) {
        return try alloc.dupe(u8, path);
    } else {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();

        const absolute_path = if (std.fs.path.isAbsolute(path))
            try alloc.dupe(u8, path)
        else blk: {
            const cwd = try std.process.currentPathAlloc(io_impl.io(), alloc);
            defer alloc.free(cwd);
            break :blk try std.fs.path.resolve(alloc, &.{ cwd, path });
        };
        errdefer alloc.free(absolute_path);
        if (!std.fs.path.isAbsolute(absolute_path)) return absolute_path;

        const canonical_z = std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), absolute_path, alloc) catch return absolute_path;
        defer alloc.free(canonical_z);
        const canonical = try alloc.dupe(u8, canonical_z);
        alloc.free(absolute_path);
        return canonical;
    }
}

fn hbcCacheNamespaceStable(alloc: Allocator, path: []const u8) u64 {
    const stable_path = hbcCacheStablePathAlloc(alloc, path) catch return hbcCacheNamespace(path);
    defer alloc.free(stable_path);
    return hbcCacheNamespace(stable_path);
}

const HbcCacheIdentity = struct {
    namespace: u64,
    stable_path: []u8,
};

const HbcNamespacePathRegistration = struct {
    path: []u8,
    active_owners: usize,
};

fn hbcCacheIdentityAlloc(alloc: Allocator, path: []const u8) !HbcCacheIdentity {
    const stable_path = hbcCacheStablePathAlloc(alloc, path) catch try alloc.dupe(u8, path);
    return .{
        .namespace = hbcCacheNamespace(stable_path),
        .stable_path = stable_path,
    };
}

fn hbcPathHasPrefixBoundary(path: []const u8, prefix: []const u8) bool {
    if (std.mem.eql(u8, path, prefix)) return true;
    if (!std.mem.startsWith(u8, path, prefix)) return false;
    if (path.len <= prefix.len) return false;
    return path[prefix.len] == std.fs.path.sep;
}

fn hbcKindStats(stats: *HbcCacheStats, kind: HbcCacheKind) *HbcCacheKindStats {
    return switch (kind) {
        .node => &stats.node,
        .quantized => &stats.quantized,
        .vector => &stats.vector,
        .metadata => &stats.metadata,
    };
}

fn addHbcKindBytes(stats: *HbcCacheStats, kind: HbcCacheKind, bytes: u64) void {
    if (bytes == 0) return;
    stats.total_bytes +|= bytes;
    const kind_stats = hbcKindStats(stats, kind);
    kind_stats.used_bytes +|= bytes;
    kind_stats.peak_bytes = @max(kind_stats.peak_bytes, kind_stats.used_bytes);
}

fn removeHbcKindBytes(stats: *HbcCacheStats, kind: HbcCacheKind, bytes: u64) void {
    if (bytes == 0) return;
    stats.total_bytes -|= bytes;
    hbcKindStats(stats, kind).used_bytes -|= bytes;
}

fn noteHbcKindInsertion(stats: *HbcCacheStats, kind: HbcCacheKind) void {
    hbcKindStats(stats, kind).insertions += 1;
}

fn noteHbcKindAdmissionSkip(stats: *HbcCacheStats, kind: HbcCacheKind) void {
    hbcKindStats(stats, kind).admission_skips += 1;
}

fn noteHbcKindEviction(stats: *HbcCacheStats, kind: HbcCacheKind) void {
    hbcKindStats(stats, kind).evictions += 1;
}

pub const Cache = struct {
    alloc: Allocator,
    mutex: apply_rw_lock_mod.ApplyRwLock = .{},
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    accounted_bytes: u64 = 0,
    global_stats: HbcCacheStats = .{},
    namespace_stats: std.AutoHashMapUnmanaged(u64, HbcCacheStats) = .empty,
    node_cache: std.AutoHashMapUnmanaged(HbcSharedCacheKey, *NodeCacheEntry) = .empty,
    node_slots: std.AutoHashMapUnmanaged(HbcSharedCacheKey, usize) = .empty,
    node_clock: std.ArrayListUnmanaged(HbcSharedClockEntry) = .empty,
    node_hand: usize = 0,
    quantized_cache: std.AutoHashMapUnmanaged(HbcSharedCacheKey, *QuantizedCacheEntry) = .empty,
    quantized_slots: std.AutoHashMapUnmanaged(HbcSharedCacheKey, usize) = .empty,
    quantized_clock: std.ArrayListUnmanaged(HbcSharedClockEntry) = .empty,
    quantized_hand: usize = 0,
    vector_cache: std.AutoHashMapUnmanaged(HbcSharedCacheKey, *VectorCacheEntry) = .empty,
    vector_slots: std.AutoHashMapUnmanaged(HbcSharedCacheKey, usize) = .empty,
    vector_clock: std.ArrayListUnmanaged(HbcSharedClockEntry) = .empty,
    vector_hand: usize = 0,
    metadata_cache: std.AutoHashMapUnmanaged(HbcSharedCacheKey, *MetadataCacheEntry) = .empty,
    metadata_slots: std.AutoHashMapUnmanaged(HbcSharedCacheKey, usize) = .empty,
    metadata_clock: std.ArrayListUnmanaged(HbcSharedClockEntry) = .empty,
    metadata_hand: usize = 0,
    namespace_paths: std.AutoHashMapUnmanaged(u64, HbcNamespacePathRegistration) = .empty,

    pub fn init(alloc: Allocator) Cache {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Cache) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();

        self.clearAllLocked();
        self.namespace_stats.deinit(self.alloc);
        self.node_cache.deinit(self.alloc);
        self.node_slots.deinit(self.alloc);
        self.node_clock.deinit(self.alloc);
        self.quantized_cache.deinit(self.alloc);
        self.quantized_slots.deinit(self.alloc);
        self.quantized_clock.deinit(self.alloc);
        self.vector_cache.deinit(self.alloc);
        self.vector_slots.deinit(self.alloc);
        self.vector_clock.deinit(self.alloc);
        self.metadata_cache.deinit(self.alloc);
        self.metadata_slots.deinit(self.alloc);
        self.metadata_clock.deinit(self.alloc);
        var path_it = self.namespace_paths.valueIterator();
        while (path_it.next()) |registration| self.alloc.free(registration.path);
        self.namespace_paths.deinit(self.alloc);
    }

    pub fn attachResourceManager(self: *Cache, resource_manager: *resource_manager_mod.ResourceManager) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        self.resource_manager = resource_manager;
        resource_manager.observeUsage(.hbc_node_metadata_cache, &self.accounted_bytes, self.global_stats.total_bytes);
    }

    pub fn namespaceStats(self: *Cache, namespace: u64) HbcCacheStats {
        self.mutex.lockShared();
        defer self.mutex.unlockShared();
        var stats = self.namespace_stats.get(namespace) orelse HbcCacheStats{};
        stats.accounted_bytes = stats.total_bytes;
        return stats;
    }

    pub fn invalidateNamespace(self: *Cache, namespace: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        while (self.evictNamespaceEntryLocked(namespace, .vector)) {}
        while (self.evictNamespaceEntryLocked(namespace, .metadata)) {}
        while (self.evictNamespaceEntryLocked(namespace, .quantized)) {}
        while (self.evictNamespaceEntryLocked(namespace, .node)) {}
    }

    pub fn registerNamespacePath(self: *Cache, namespace: u64, path: []const u8) bool {
        const stable_path = hbcCacheStablePathAlloc(self.alloc, path) catch return false;
        errdefer self.alloc.free(stable_path);

        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        self.pruneUnusedNamespacePathsLocked();

        // Registered namespaces always have an accounting slot. This makes
        // retained-entry detection O(1) without adding cache hot-path state.
        const stats_entry = self.namespace_stats.getOrPut(self.alloc, namespace) catch {
            self.alloc.free(stable_path);
            return false;
        };
        if (!stats_entry.found_existing) stats_entry.value_ptr.* = .{};

        const entry = self.namespace_paths.getOrPut(self.alloc, namespace) catch {
            if (!stats_entry.found_existing) _ = self.namespace_stats.remove(namespace);
            self.alloc.free(stable_path);
            return false;
        };
        if (!entry.found_existing) {
            entry.value_ptr.* = .{ .path = stable_path, .active_owners = 1 };
            return true;
        }
        if (std.mem.eql(u8, entry.value_ptr.path, stable_path)) {
            entry.value_ptr.active_owners += 1;
            self.alloc.free(stable_path);
            return true;
        }

        self.alloc.free(stable_path);
        return false;
    }

    pub fn unregisterNamespacePath(self: *Cache, namespace: u64, stable_path: []const u8) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();

        const registration = self.namespace_paths.getPtr(namespace) orelse return;
        if (!std.mem.eql(u8, registration.path, stable_path) or registration.active_owners == 0) return;
        registration.active_owners -= 1;
        self.removeNamespaceStateIfUnusedLocked(namespace);
    }

    pub fn invalidatePath(self: *Cache, path: []const u8) void {
        const stable_prefix = hbcCacheStablePathAlloc(self.alloc, path) catch {
            self.clear();
            return;
        };
        defer self.alloc.free(stable_prefix);

        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();

        while (self.evictPathPrefixEntryLocked(stable_prefix, .vector)) {}
        while (self.evictPathPrefixEntryLocked(stable_prefix, .metadata)) {}
        while (self.evictPathPrefixEntryLocked(stable_prefix, .quantized)) {}
        while (self.evictPathPrefixEntryLocked(stable_prefix, .node)) {}
        self.pruneUnusedNamespacePathsLocked();
    }

    pub fn clear(self: *Cache) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        self.clearAllLocked();
        self.pruneUnusedNamespacePathsLocked();
    }

    pub fn clearNodeNamespace(self: *Cache, namespace: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        while (self.evictNamespaceEntryLocked(namespace, .node)) {}
    }

    pub fn clearQuantizedNamespace(self: *Cache, namespace: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        while (self.evictNamespaceEntryLocked(namespace, .quantized)) {}
    }

    pub fn clearVectorNamespace(self: *Cache, namespace: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        while (self.evictNamespaceEntryLocked(namespace, .vector)) {}
    }

    pub fn clearMetadataNamespace(self: *Cache, namespace: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        while (self.evictNamespaceEntryLocked(namespace, .metadata)) {}
    }

    pub fn invalidateNode(self: *Cache, namespace: u64, node_id: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        _ = self.removeNodeLocked(.{ .namespace = namespace, .id = node_id }, false);
    }

    pub fn invalidateQuantized(self: *Cache, namespace: u64, node_id: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        _ = self.removeQuantizedLocked(.{ .namespace = namespace, .id = node_id }, false);
    }

    pub fn invalidateVector(self: *Cache, namespace: u64, vector_id: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        _ = self.removeVectorLocked(.{ .namespace = namespace, .id = vector_id }, false);
    }

    pub fn invalidateMetadata(self: *Cache, namespace: u64, vector_id: u64) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        _ = self.removeMetadataLocked(.{ .namespace = namespace, .id = vector_id }, false);
    }

    pub fn borrowNode(self: *Cache, namespace: u64, node_id: u64) ?BorrowedNodeLease {
        self.mutex.lockShared();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        if (self.node_cache.get(key)) |entry| {
            retainNodeCacheEntry(entry);
            self.mutex.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.mutex.unlockShared();
        return null;
    }

    pub fn getNodePtr(self: *Cache, namespace: u64, node_id: u64) ?*const Node {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        if (self.node_cache.get(key)) |entry| {
            self.touchSlot(&self.node_clock, self.node_slots.get(key));
            return &entry.node;
        }
        return null;
    }

    pub fn cloneNode(self: *Cache, namespace: u64, node_id: u64) !?Node {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        if (self.node_cache.get(key)) |entry| {
            self.touchSlot(&self.node_clock, self.node_slots.get(key));
            return try entry.node.clone(self.alloc);
        }
        return null;
    }

    pub fn borrowQuantized(self: *Cache, namespace: u64, node_id: u64) ?BorrowedQuantizedLease {
        self.mutex.lockShared();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        if (self.quantized_cache.get(key)) |entry| {
            retainQuantizedCacheEntry(entry);
            self.mutex.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.mutex.unlockShared();
        return null;
    }

    pub fn getQuantizedPtr(self: *Cache, namespace: u64, node_id: u64) ?*QuantizedSet {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        if (self.quantized_cache.get(key)) |entry| {
            self.touchSlot(&self.quantized_clock, self.quantized_slots.get(key));
            return &entry.quantized;
        }
        return null;
    }

    pub fn cloneQuantized(self: *Cache, namespace: u64, node_id: u64) !?QuantizedSet {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        if (self.quantized_cache.get(key)) |entry| {
            self.touchSlot(&self.quantized_clock, self.quantized_slots.get(key));
            return try entry.quantized.clone(self.alloc);
        }
        return null;
    }

    pub fn getVector(self: *Cache, namespace: u64, vector_id: u64) ?[]const f32 {
        self.mutex.lockShared();
        defer self.mutex.unlockShared();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = vector_id };
        if (self.vector_cache.get(key)) |entry| return entry.vector;
        return null;
    }

    pub fn borrowVector(self: *Cache, namespace: u64, vector_id: u64) ?BorrowedVectorLease {
        self.mutex.lockShared();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = vector_id };
        if (self.vector_cache.get(key)) |entry| {
            retainVectorCacheEntry(entry);
            self.mutex.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.mutex.unlockShared();
        return null;
    }

    pub fn getMetadata(self: *Cache, namespace: u64, vector_id: u64) ?[]const u8 {
        self.mutex.lockShared();
        defer self.mutex.unlockShared();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = vector_id };
        if (self.metadata_cache.get(key)) |entry| return entry.metadata;
        return null;
    }

    pub fn borrowMetadata(self: *Cache, namespace: u64, vector_id: u64) ?BorrowedMetadataLease {
        self.mutex.lockShared();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = vector_id };
        if (self.metadata_cache.get(key)) |entry| {
            retainMetadataCacheEntry(entry);
            self.mutex.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.mutex.unlockShared();
        return null;
    }

    pub fn cacheNode(self: *Cache, namespace: u64, node: *const Node) !bool {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node.id };
        _ = self.removeNodeLocked(key, false);
        const bytes = estimateNodeCacheBytes(node);
        const admission = self.admitLocked(.node, namespace, key, bytes, false) orelse {
            self.noteAdmissionSkipLocked(.node, namespace);
            return false;
        };
        errdefer self.rollbackAdmissionLocked(admission);
        const cloned = try node.clone(self.alloc);
        const entry = try self.alloc.create(NodeCacheEntry);
        entry.* = .{ .node = cloned };
        errdefer releaseNodeCacheEntry(self.alloc, entry);
        try self.recordClockSlot(&self.node_clock, &self.node_slots, key);
        errdefer removeSlot(&self.node_clock, &self.node_slots, key);
        try self.node_cache.put(self.alloc, key, entry);
        self.finishInsertLocked(.node, key, bytes, admission);
        return true;
    }

    pub fn cacheNodeOwned(self: *Cache, namespace: u64, node: Node) !*const Node {
        var owned = node;
        var owned_active = true;
        errdefer if (owned_active) owned.deinit(self.alloc);
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = owned.id };
        _ = self.removeNodeLocked(key, false);
        const bytes = estimateNodeCacheBytes(&owned);
        const admission = self.admitLocked(.node, namespace, key, bytes, true) orelse unreachable;
        errdefer self.rollbackAdmissionLocked(admission);
        const entry = try self.alloc.create(NodeCacheEntry);
        entry.* = .{ .node = owned };
        owned_active = false;
        errdefer releaseNodeCacheEntry(self.alloc, entry);
        try self.recordClockSlot(&self.node_clock, &self.node_slots, key);
        errdefer removeSlot(&self.node_clock, &self.node_slots, key);
        try self.node_cache.put(self.alloc, key, entry);
        self.finishInsertLocked(.node, key, bytes, admission);
        return &self.node_cache.get(key).?.node;
    }

    pub fn cacheQuantized(self: *Cache, namespace: u64, node_id: u64, qs: *const QuantizedSet) !bool {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        _ = self.removeQuantizedLocked(key, false);
        const bytes = estimateQuantizedCacheBytes(qs);
        const admission = self.admitLocked(.quantized, namespace, key, bytes, false) orelse {
            self.noteAdmissionSkipLocked(.quantized, namespace);
            return false;
        };
        errdefer self.rollbackAdmissionLocked(admission);
        const cloned = try qs.clone(self.alloc);
        const entry = try self.alloc.create(QuantizedCacheEntry);
        entry.* = .{ .quantized = cloned };
        errdefer releaseQuantizedCacheEntry(self.alloc, entry);
        try self.recordClockSlot(&self.quantized_clock, &self.quantized_slots, key);
        errdefer removeSlot(&self.quantized_clock, &self.quantized_slots, key);
        try self.quantized_cache.put(self.alloc, key, entry);
        self.finishInsertLocked(.quantized, key, bytes, admission);
        return true;
    }

    pub fn cacheQuantizedOwned(self: *Cache, namespace: u64, node_id: u64, qs: QuantizedSet) !*const QuantizedSet {
        var owned = qs;
        var owned_active = true;
        errdefer if (owned_active) owned.deinit(self.alloc);
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = node_id };
        _ = self.removeQuantizedLocked(key, false);
        const bytes = estimateQuantizedCacheBytes(&owned);
        const admission = self.admitLocked(.quantized, namespace, key, bytes, true) orelse unreachable;
        errdefer self.rollbackAdmissionLocked(admission);
        const entry = try self.alloc.create(QuantizedCacheEntry);
        entry.* = .{ .quantized = owned };
        owned_active = false;
        errdefer releaseQuantizedCacheEntry(self.alloc, entry);
        try self.recordClockSlot(&self.quantized_clock, &self.quantized_slots, key);
        errdefer removeSlot(&self.quantized_clock, &self.quantized_slots, key);
        try self.quantized_cache.put(self.alloc, key, entry);
        self.finishInsertLocked(.quantized, key, bytes, admission);
        return &self.quantized_cache.get(key).?.quantized;
    }

    pub fn cacheVector(self: *Cache, namespace: u64, vector_id: u64, vector_data: []const f32) ![]const f32 {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = vector_id };
        if (self.vector_cache.get(key)) |existing| {
            if (existing.vector.ptr == vector_data.ptr and existing.vector.len == vector_data.len) return existing.vector;
        }
        _ = self.removeVectorLocked(key, false);
        const bytes = estimateVectorCacheBytes(vector_data);
        const admission = self.admitLocked(.vector, namespace, key, bytes, false) orelse {
            self.noteAdmissionSkipLocked(.vector, namespace);
            return vector_data;
        };
        errdefer self.rollbackAdmissionLocked(admission);
        const copied = try self.alloc.dupe(f32, vector_data);
        const entry = try self.alloc.create(VectorCacheEntry);
        entry.* = .{ .vector = copied };
        errdefer releaseVectorCacheEntry(self.alloc, entry);
        try self.recordClockSlot(&self.vector_clock, &self.vector_slots, key);
        errdefer removeSlot(&self.vector_clock, &self.vector_slots, key);
        try self.vector_cache.put(self.alloc, key, entry);
        self.finishInsertLocked(.vector, key, bytes, admission);
        return self.vector_cache.get(key).?.vector;
    }

    pub fn cacheMetadata(self: *Cache, namespace: u64, vector_id: u64, metadata: []const u8) ![]const u8 {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        const key: HbcSharedCacheKey = .{ .namespace = namespace, .id = vector_id };
        _ = self.removeMetadataLocked(key, false);
        const bytes = estimateMetadataCacheBytes(metadata);
        const admission = self.admitLocked(.metadata, namespace, key, bytes, false) orelse {
            self.noteAdmissionSkipLocked(.metadata, namespace);
            return metadata;
        };
        errdefer self.rollbackAdmissionLocked(admission);
        const copied = try self.alloc.dupe(u8, metadata);
        const entry = try self.alloc.create(MetadataCacheEntry);
        entry.* = .{ .metadata = copied };
        errdefer releaseMetadataCacheEntry(self.alloc, entry);
        try self.recordClockSlot(&self.metadata_clock, &self.metadata_slots, key);
        errdefer removeSlot(&self.metadata_clock, &self.metadata_slots, key);
        try self.metadata_cache.put(self.alloc, key, entry);
        self.finishInsertLocked(.metadata, key, bytes, admission);
        return self.metadata_cache.get(key).?.metadata;
    }

    fn namespaceStatsPtrLocked(self: *Cache, namespace: u64) !*HbcCacheStats {
        const entry = try self.namespace_stats.getOrPut(self.alloc, namespace);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        return entry.value_ptr;
    }

    fn addStatsLocked(self: *Cache, kind: HbcCacheKind, namespace: u64, bytes: u64) !void {
        const stats = try self.namespaceStatsPtrLocked(namespace);
        addHbcKindBytes(&self.global_stats, kind, bytes);
        addHbcKindBytes(stats, kind, bytes);
    }

    fn removeStatsLocked(self: *Cache, kind: HbcCacheKind, namespace: u64, bytes: u64) void {
        removeHbcKindBytes(&self.global_stats, kind, bytes);
        if (self.namespace_stats.getPtr(namespace)) |stats| {
            removeHbcKindBytes(stats, kind, bytes);
            if (stats.total_bytes == 0) self.removeNamespaceStateIfUnusedLocked(namespace);
        }
    }

    fn noteInsertionLocked(self: *Cache, kind: HbcCacheKind, namespace: u64) void {
        noteHbcKindInsertion(&self.global_stats, kind);
        if (self.namespace_stats.getPtr(namespace)) |stats| noteHbcKindInsertion(stats, kind);
    }

    fn noteAdmissionSkipLocked(self: *Cache, kind: HbcCacheKind, namespace: u64) void {
        noteHbcKindAdmissionSkip(&self.global_stats, kind);
        if (self.namespace_stats.getPtr(namespace)) |stats| noteHbcKindAdmissionSkip(stats, kind);
    }

    fn noteEvictionLocked(self: *Cache, kind: HbcCacheKind, namespace: u64) void {
        noteHbcKindEviction(&self.global_stats, kind);
        if (self.namespace_stats.getPtr(namespace)) |stats| noteHbcKindEviction(stats, kind);
    }

    fn reserveLocked(self: *Cache, bytes: u64) bool {
        if (bytes == 0) return true;
        const manager = self.resource_manager orelse return true;
        var reservation = manager.reserve(.hbc_node_metadata_cache, bytes) catch return false;
        reservation.released = true;
        self.accounted_bytes +|= bytes;
        return true;
    }

    fn releaseLocked(self: *Cache, bytes: u64) void {
        if (bytes == 0) return;
        if (self.resource_manager) |manager| manager.releaseBytes(.hbc_node_metadata_cache, bytes);
        self.accounted_bytes -|= bytes;
    }

    fn observeLocked(self: *Cache) void {
        if (self.resource_manager) |manager| manager.observeUsage(.hbc_node_metadata_cache, &self.accounted_bytes, self.global_stats.total_bytes);
    }

    fn admitLocked(self: *Cache, kind: HbcCacheKind, namespace: u64, protected: HbcSharedCacheKey, bytes: u64, must_cache: bool) ?HbcSharedAdmission {
        if (bytes == 0) return .{};
        if (self.reserveLocked(bytes)) return .{ .bytes = bytes, .reserved = true };
        _ = kind;
        _ = namespace;
        while (self.evictOneLocked(protected)) {
            if (self.reserveLocked(bytes)) return .{ .bytes = bytes, .reserved = true };
        }
        if (must_cache) return .{ .bytes = bytes, .overcommitted = true };
        return null;
    }

    fn rollbackAdmissionLocked(self: *Cache, admission: HbcSharedAdmission) void {
        if (admission.reserved) self.releaseLocked(admission.bytes);
    }

    fn finishInsertLocked(self: *Cache, kind: HbcCacheKind, key: HbcSharedCacheKey, bytes: u64, admission: HbcSharedAdmission) void {
        self.addStatsLocked(kind, key.namespace, bytes) catch {};
        self.noteInsertionLocked(kind, key.namespace);
        if (admission.overcommitted) self.observeLocked();
        self.enforceBudgetLocked(key);
    }

    fn enforceBudgetLocked(self: *Cache, protected: HbcSharedCacheKey) void {
        const manager = self.resource_manager orelse return;
        const stats = manager.sliceStats(.hbc_node_metadata_cache);
        const action = switch (stats.pressure) {
            .normal => return,
            .soft => stats.soft_action,
            .hard => stats.hard_action,
        };
        if (action != .shrink_cache) return;
        const target_bytes = if (stats.soft_limit_bytes > 0) stats.soft_limit_bytes else stats.hard_limit_bytes;
        if (target_bytes == 0) return;
        while (self.accounted_bytes > target_bytes) {
            if (!self.evictOneLocked(protected)) break;
        }
    }

    fn touchSlot(_: *Cache, clock: *std.ArrayListUnmanaged(HbcSharedClockEntry), maybe_slot: ?usize) void {
        const slot = maybe_slot orelse return;
        if (slot < clock.items.len) clock.items[slot].referenced = true;
    }

    fn recordClockSlot(
        self: *Cache,
        clock: *std.ArrayListUnmanaged(HbcSharedClockEntry),
        slots: *std.AutoHashMapUnmanaged(HbcSharedCacheKey, usize),
        key: HbcSharedCacheKey,
    ) !void {
        for (clock.items, 0..) |entry, i| {
            if (entry.key.namespace == 0) {
                try slots.put(self.alloc, key, i);
                clock.items[i] = .{ .key = key, .referenced = true };
                return;
            }
        }
        const slot = clock.items.len;
        try slots.put(self.alloc, key, slot);
        errdefer _ = slots.remove(key);
        try clock.append(self.alloc, .{ .key = key, .referenced = true });
    }

    fn nextVictim(clock: *std.ArrayListUnmanaged(HbcSharedClockEntry), hand: *usize, protected: HbcSharedCacheKey) ?HbcSharedCacheKey {
        if (clock.items.len == 0) return null;
        var scanned: usize = 0;
        const limit = clock.items.len * 2;
        while (scanned < limit) : (scanned += 1) {
            const slot = hand.* % clock.items.len;
            const entry = &clock.items[slot];
            if (entry.key.namespace != 0 and !(entry.key.namespace == protected.namespace and entry.key.id == protected.id)) {
                if (entry.referenced) {
                    entry.referenced = false;
                } else {
                    hand.* = (slot + 1) % clock.items.len;
                    return entry.key;
                }
            }
            hand.* = (slot + 1) % clock.items.len;
        }
        return null;
    }

    fn evictOneLocked(self: *Cache, protected: HbcSharedCacheKey) bool {
        if (nextVictim(&self.vector_clock, &self.vector_hand, protected)) |key| return self.removeVectorLocked(key, true);
        if (nextVictim(&self.metadata_clock, &self.metadata_hand, protected)) |key| return self.removeMetadataLocked(key, true);
        if (nextVictim(&self.quantized_clock, &self.quantized_hand, protected)) |key| return self.removeQuantizedLocked(key, true);
        if (nextVictim(&self.node_clock, &self.node_hand, protected)) |key| return self.removeNodeLocked(key, true);
        return false;
    }

    fn evictNamespaceEntryLocked(self: *Cache, namespace: u64, kind: HbcCacheKind) bool {
        switch (kind) {
            .vector => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.vector_cache.keyIterator();
                    while (it.next()) |key| if (key.namespace == namespace) {
                        victim = key.*;
                        break;
                    };
                }
                if (victim) |key| return self.removeVectorLocked(key, false);
            },
            .metadata => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.metadata_cache.keyIterator();
                    while (it.next()) |key| if (key.namespace == namespace) {
                        victim = key.*;
                        break;
                    };
                }
                if (victim) |key| return self.removeMetadataLocked(key, false);
            },
            .quantized => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.quantized_cache.keyIterator();
                    while (it.next()) |key| if (key.namespace == namespace) {
                        victim = key.*;
                        break;
                    };
                }
                if (victim) |key| return self.removeQuantizedLocked(key, false);
            },
            .node => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.node_cache.keyIterator();
                    while (it.next()) |key| if (key.namespace == namespace) {
                        victim = key.*;
                        break;
                    };
                }
                if (victim) |key| return self.removeNodeLocked(key, false);
            },
        }
        return false;
    }

    fn evictPathPrefixEntryLocked(self: *Cache, stable_prefix: []const u8, kind: HbcCacheKind) bool {
        switch (kind) {
            .vector => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.vector_cache.keyIterator();
                    while (it.next()) |key| {
                        const registration = self.namespace_paths.get(key.namespace) orelse continue;
                        if (hbcPathHasPrefixBoundary(registration.path, stable_prefix)) {
                            victim = key.*;
                            break;
                        }
                    }
                }
                if (victim) |key| return self.removeVectorLocked(key, false);
            },
            .metadata => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.metadata_cache.keyIterator();
                    while (it.next()) |key| {
                        const registration = self.namespace_paths.get(key.namespace) orelse continue;
                        if (hbcPathHasPrefixBoundary(registration.path, stable_prefix)) {
                            victim = key.*;
                            break;
                        }
                    }
                }
                if (victim) |key| return self.removeMetadataLocked(key, false);
            },
            .quantized => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.quantized_cache.keyIterator();
                    while (it.next()) |key| {
                        const registration = self.namespace_paths.get(key.namespace) orelse continue;
                        if (hbcPathHasPrefixBoundary(registration.path, stable_prefix)) {
                            victim = key.*;
                            break;
                        }
                    }
                }
                if (victim) |key| return self.removeQuantizedLocked(key, false);
            },
            .node => {
                var victim: ?HbcSharedCacheKey = null;
                {
                    var it = self.node_cache.keyIterator();
                    while (it.next()) |key| {
                        const registration = self.namespace_paths.get(key.namespace) orelse continue;
                        if (hbcPathHasPrefixBoundary(registration.path, stable_prefix)) {
                            victim = key.*;
                            break;
                        }
                    }
                }
                if (victim) |key| return self.removeNodeLocked(key, false);
            },
        }
        return false;
    }

    fn removeSlot(clock: *std.ArrayListUnmanaged(HbcSharedClockEntry), slots: *std.AutoHashMapUnmanaged(HbcSharedCacheKey, usize), key: HbcSharedCacheKey) void {
        if (slots.fetchRemove(key)) |removed| {
            if (removed.value < clock.items.len) clock.items[removed.value] = .{};
        }
    }

    fn removeNodeLocked(self: *Cache, key: HbcSharedCacheKey, evicted: bool) bool {
        removeSlot(&self.node_clock, &self.node_slots, key);
        if (self.node_cache.fetchRemove(key)) |removed| {
            const bytes = estimateNodeCacheBytes(&removed.value.node);
            releaseNodeCacheEntry(self.alloc, removed.value);
            self.removeStatsLocked(.node, key.namespace, bytes);
            self.releaseLocked(bytes);
            if (evicted) self.noteEvictionLocked(.node, key.namespace);
            return true;
        }
        return false;
    }

    fn removeQuantizedLocked(self: *Cache, key: HbcSharedCacheKey, evicted: bool) bool {
        removeSlot(&self.quantized_clock, &self.quantized_slots, key);
        if (self.quantized_cache.fetchRemove(key)) |removed| {
            const bytes = estimateQuantizedCacheBytes(&removed.value.quantized);
            releaseQuantizedCacheEntry(self.alloc, removed.value);
            self.removeStatsLocked(.quantized, key.namespace, bytes);
            self.releaseLocked(bytes);
            if (evicted) self.noteEvictionLocked(.quantized, key.namespace);
            return true;
        }
        return false;
    }

    fn removeVectorLocked(self: *Cache, key: HbcSharedCacheKey, evicted: bool) bool {
        removeSlot(&self.vector_clock, &self.vector_slots, key);
        if (self.vector_cache.fetchRemove(key)) |removed| {
            const bytes = estimateVectorCacheBytes(removed.value.vector);
            releaseVectorCacheEntry(self.alloc, removed.value);
            self.removeStatsLocked(.vector, key.namespace, bytes);
            self.releaseLocked(bytes);
            if (evicted) self.noteEvictionLocked(.vector, key.namespace);
            return true;
        }
        return false;
    }

    fn removeMetadataLocked(self: *Cache, key: HbcSharedCacheKey, evicted: bool) bool {
        removeSlot(&self.metadata_clock, &self.metadata_slots, key);
        if (self.metadata_cache.fetchRemove(key)) |removed| {
            const bytes = estimateMetadataCacheBytes(removed.value.metadata);
            releaseMetadataCacheEntry(self.alloc, removed.value);
            self.removeStatsLocked(.metadata, key.namespace, bytes);
            self.releaseLocked(bytes);
            if (evicted) self.noteEvictionLocked(.metadata, key.namespace);
            return true;
        }
        return false;
    }

    fn clearAllLocked(self: *Cache) void {
        while (self.evictOneLocked(.{ .namespace = 0, .id = 0 })) {}
        self.observeLocked();
    }

    fn namespaceHasEntriesLocked(self: *Cache, namespace: u64) bool {
        const stats = self.namespace_stats.get(namespace) orelse return false;
        return stats.total_bytes != 0;
    }

    fn removeNamespaceStateIfUnusedLocked(self: *Cache, namespace: u64) void {
        if (self.namespaceHasEntriesLocked(namespace)) return;
        if (self.namespace_paths.get(namespace)) |registration| {
            if (registration.active_owners != 0) return;
            const removed = self.namespace_paths.fetchRemove(namespace) orelse return;
            self.alloc.free(removed.value.path);
        }
        _ = self.namespace_stats.remove(namespace);
    }

    fn pruneUnusedNamespacePathsLocked(self: *Cache) void {
        while (true) {
            var victim: ?u64 = null;
            var it = self.namespace_paths.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.active_owners == 0 and !self.namespaceHasEntriesLocked(entry.key_ptr.*)) {
                    victim = entry.key_ptr.*;
                    break;
                }
            }
            const namespace = victim orelse return;
            self.removeNamespaceStateIfUnusedLocked(namespace);
        }
    }
};

const HbcCacheProtection = struct {
    kind: ?HbcCacheKind = null,
    key: u64 = 0,

    fn none() HbcCacheProtection {
        return .{};
    }

    fn one(kind: HbcCacheKind, key: u64) HbcCacheProtection {
        return .{ .kind = kind, .key = key };
    }

    fn protects(self: HbcCacheProtection, kind: HbcCacheKind, key: u64) bool {
        return self.kind == kind and self.key == key;
    }
};

fn estimateNodeCacheBytes(node: *const Node) u64 {
    var total: u64 = @sizeOf(Node);
    if (node.backing.len > 0) {
        total +|= @intCast(node.backing.len);
    } else {
        total +|= @intCast(node.centroid.len * @sizeOf(f32));
        total +|= @intCast(node.children.len * @sizeOf(u64));
        total +|= @intCast(node.members.len * @sizeOf(u64));
    }
    return total;
}

fn estimateQuantizedCacheBytes(qs: *const QuantizedSet) u64 {
    return @sizeOf(QuantizedSet) + switch (qs.*) {
        .rabit => |*set| @as(u64, @intCast(set.centroid.len * @sizeOf(f32))) +|
            @as(u64, @intCast(set.codes.data.len * @sizeOf(u64))) +|
            @as(u64, @intCast(set.code_counts.len * @sizeOf(u32))) +|
            @as(u64, @intCast(set.centroid_distances.len * @sizeOf(f32))) +|
            @as(u64, @intCast(set.quantized_dot_products.len * @sizeOf(f32))) +|
            @as(u64, @intCast(set.centroid_dot_products.len * @sizeOf(f32))),
        .nonquant => |*set| @as(u64, @intCast(set.vectors.data.len * @sizeOf(f32))),
    };
}

fn estimateVectorCacheBytes(vector: []const f32) u64 {
    return @sizeOf(VectorCacheEntry) +
        @as(u64, @intCast(vector.len * @sizeOf(f32))) +
        @sizeOf(HbcSharedClockEntry) +
        @sizeOf(HbcSharedCacheKey) +
        @sizeOf(usize);
}

fn estimateMetadataCacheBytes(metadata: []const u8) u64 {
    return @sizeOf(MetadataCacheEntry) +
        @as(u64, @intCast(metadata.len)) +
        @sizeOf(HbcSharedClockEntry) +
        @sizeOf(HbcSharedCacheKey) +
        @sizeOf(usize);
}

fn nextHbcClockVictim(
    clock_keys: []u64,
    clock_refs: []bool,
    hand: *usize,
    protection: HbcCacheProtection,
    kind: HbcCacheKind,
) ?u64 {
    if (clock_keys.len == 0) return null;
    var scanned: usize = 0;
    const limit = clock_keys.len * 2;
    while (scanned < limit) : (scanned += 1) {
        const slot = hand.*;
        const key = clock_keys[slot];
        if (key != 0 and !protection.protects(kind, key)) {
            if (clock_refs[slot]) {
                clock_refs[slot] = false;
            } else {
                hand.* = (slot + 1) % clock_keys.len;
                return key;
            }
        }
        hand.* = (slot + 1) % clock_keys.len;
    }
    return null;
}

fn claimLocalClockSlot(clock_keys: []u64, start_slot: usize, key: u64) ?usize {
    if (clock_keys.len == 0) return null;
    for (0..clock_keys.len) |offset| {
        const slot = (start_slot + offset) % clock_keys.len;
        if (clock_keys[slot] == 0) {
            clock_keys[slot] = key;
            return slot;
        }
    }
    return null;
}

// ============================================================================
// HBC Index
// ============================================================================

const ExperimentalPostingMaterializedValue = struct {
    bytes: ?[]const u8,
    owned: bool,
};

const ExperimentalPostingReadState = struct {
    alloc: Allocator,
    covered_source_sequence: u64,
    retained_segment: posting_segment_store_mod.RetainedSegment,
    segment: vectorindex_posting_segment.VerifiedReader,
    vector_directory: ?vectorindex_hbc_vector_directory.Reader = null,
    // Full WAL records borrow directly from this one retained buffer. Only
    // decoded replacement patches allocate individual owned values, avoiding
    // a second full copy of the bounded WAL tail after restart.
    wal_bytes: ?[]u8 = null,
    wal_committed_bytes: u64,
    materialized: std.AutoHashMapUnmanaged(u128, ExperimentalPostingMaterializedValue) = .empty,

    fn deinit(self: *ExperimentalPostingReadState) void {
        var values = self.materialized.valueIterator();
        while (values.next()) |slot| if (slot.owned) if (slot.bytes) |owned| self.alloc.free(owned);
        self.materialized.deinit(self.alloc);
        if (self.wal_bytes) |bytes| self.alloc.free(bytes);
        self.segment.deinit();
        self.retained_segment.deinit(self.alloc);
        self.* = undefined;
    }

    fn value(self: *ExperimentalPostingReadState, posting_id: u64, kind: vectorindex_posting_wal.RecordKind) !?[]const u8 {
        if (self.materialized.get(experimentalPostingValueKey(posting_id, kind))) |materialized| {
            return materialized.bytes orelse error.NotFound;
        }
        if (self.vector_directory) |directory| switch (kind) {
            .vector_leaf => return try directory.get(.leaf, posting_id),
            .vector_metadata => return try directory.get(.metadata, posting_id),
            else => {},
        };
        const entry_kind = experimentalPostingSegmentKind(kind) orelse return null;
        return try self.segment.getValue(posting_id, entry_kind);
    }

    fn replaceMaterialized(
        self: *ExperimentalPostingReadState,
        posting_id: u64,
        kind: vectorindex_posting_wal.RecordKind,
        materialized_value: ExperimentalPostingMaterializedValue,
    ) !void {
        const key = experimentalPostingValueKey(posting_id, kind);
        const result = try self.materialized.getOrPut(self.alloc, key);
        if (result.found_existing and result.value_ptr.owned) if (result.value_ptr.bytes) |old| self.alloc.free(old);
        result.value_ptr.* = materialized_value;
    }

    fn set(self: *ExperimentalPostingReadState, posting_id: u64, kind: vectorindex_posting_wal.RecordKind, value_bytes: ?[]const u8) !void {
        const owned = if (value_bytes) |bytes| try self.alloc.dupe(u8, bytes) else null;
        errdefer if (owned) |bytes| self.alloc.free(bytes);
        try self.replaceMaterialized(posting_id, kind, .{ .bytes = owned, .owned = owned != null });
    }

    fn setBorrowed(self: *ExperimentalPostingReadState, posting_id: u64, kind: vectorindex_posting_wal.RecordKind, value_bytes: ?[]const u8) !void {
        try self.replaceMaterialized(posting_id, kind, .{ .bytes = value_bytes, .owned = false });
    }

    fn setOwned(self: *ExperimentalPostingReadState, posting_id: u64, kind: vectorindex_posting_wal.RecordKind, value_bytes: []u8) !void {
        errdefer self.alloc.free(value_bytes);
        try self.replaceMaterialized(posting_id, kind, .{ .bytes = value_bytes, .owned = true });
    }

    fn materializeWal(self: *ExperimentalPostingReadState, wal: *const posting_segment_store_mod.RecoveredWal) !void {
        for (wal.replay.records.items) |record| switch (record.kind) {
            .base, .quantized_checkpoint, .posting_state, .node_range, .vector_leaf, .vector_metadata, .index_metadata => try self.setBorrowed(record.posting_id, record.kind, record.payload),
            .mutation => {
                if (record.payload.len < 6) return error.CorruptedPostingPatch;
                const target: vectorindex_posting_wal.RecordKind = switch (record.payload[5]) {
                    @intFromEnum(vectorindex_posting_wal.RecordKind.base) => .base,
                    @intFromEnum(vectorindex_posting_wal.RecordKind.quantized_checkpoint) => .quantized_checkpoint,
                    @intFromEnum(vectorindex_posting_wal.RecordKind.posting_state) => .posting_state,
                    @intFromEnum(vectorindex_posting_wal.RecordKind.node_range) => .node_range,
                    @intFromEnum(vectorindex_posting_wal.RecordKind.vector_leaf) => .vector_leaf,
                    @intFromEnum(vectorindex_posting_wal.RecordKind.vector_metadata) => .vector_metadata,
                    @intFromEnum(vectorindex_posting_wal.RecordKind.index_metadata) => .index_metadata,
                    else => return error.InvalidPostingPatchTarget,
                };
                const base = self.value(record.posting_id, target) catch |err| switch (err) {
                    error.NotFound => return error.PostingPatchBaseMismatch,
                    else => return err,
                } orelse return error.PostingPatchBaseMismatch;
                const decoded = try vectorindex_posting_wal.applyReplacementPatchAlloc(self.alloc, record.payload, base);
                if (decoded.target != target) {
                    self.alloc.free(decoded.replacement);
                    return error.InvalidPostingPatchTarget;
                }
                try self.setOwned(record.posting_id, target, decoded.replacement);
            },
            .tombstone => inline for (.{
                vectorindex_posting_wal.RecordKind.base,
                vectorindex_posting_wal.RecordKind.quantized_checkpoint,
                vectorindex_posting_wal.RecordKind.posting_state,
                vectorindex_posting_wal.RecordKind.node_range,
                vectorindex_posting_wal.RecordKind.vector_leaf,
                vectorindex_posting_wal.RecordKind.vector_metadata,
                vectorindex_posting_wal.RecordKind.index_metadata,
            }) |kind| try self.setBorrowed(record.posting_id, kind, null),
            .base_tombstone => try self.setBorrowed(record.posting_id, .base, null),
            .quantized_checkpoint_tombstone => try self.setBorrowed(record.posting_id, .quantized_checkpoint, null),
            .posting_state_tombstone => try self.setBorrowed(record.posting_id, .posting_state, null),
            .node_range_tombstone => try self.setBorrowed(record.posting_id, .node_range, null),
            .vector_leaf_tombstone => try self.setBorrowed(record.posting_id, .vector_leaf, null),
            .vector_metadata_tombstone => try self.setBorrowed(record.posting_id, .vector_metadata, null),
            .index_metadata_tombstone => try self.setBorrowed(record.posting_id, .index_metadata, null),
            .coverage => {},
            .centroid_directory, .commit => return error.UnsupportedExperimentalPostingWalRecord,
        };
    }
};

fn experimentalPostingSegmentKind(kind: vectorindex_posting_wal.RecordKind) ?vectorindex_posting_segment.EntryKind {
    return switch (kind) {
        .base => .base,
        .quantized_checkpoint => .quantized_checkpoint,
        .posting_state => .posting_state,
        .node_range => .node_range,
        .vector_leaf => .vector_leaf,
        .vector_metadata => .vector_metadata,
        .index_metadata => .index_metadata,
        else => null,
    };
}

/// Immutable query generation. A root owns a decoded segment/WAL snapshot;
/// later generations own only the values changed by one committed derived
/// batch and retain their parent. Read transactions pin one generation, so a
/// query sees either the complete old or complete new posting state.
const ExperimentalPostingReadGeneration = struct {
    alloc: Allocator,
    refs: std.atomic.Value(u32) = .init(1),
    /// Highest source-journal sequence durably represented by this immutable
    /// generation. This comes from the posting checkpoint/WAL commit boundary,
    /// not from the projection metadata value stored inside the generation.
    covered_source_sequence: std.atomic.Value(u64),
    root: ?*ExperimentalPostingReadState = null,
    parent: ?*ExperimentalPostingReadGeneration = null,
    values: std.AutoHashMapUnmanaged(u128, ?[]u8) = .empty,

    fn createRoot(alloc: Allocator, state: *ExperimentalPostingReadState) !*ExperimentalPostingReadGeneration {
        const generation = try alloc.create(ExperimentalPostingReadGeneration);
        generation.* = .{
            .alloc = alloc,
            .covered_source_sequence = .init(state.covered_source_sequence),
            .root = state,
        };
        return generation;
    }

    fn createOverlay(
        alloc: Allocator,
        parent: *ExperimentalPostingReadGeneration,
        covered_source_sequence: u64,
    ) !*ExperimentalPostingReadGeneration {
        if (covered_source_sequence < parent.covered_source_sequence.load(.acquire)) return error.PostingWalSequenceRegression;
        const generation = try alloc.create(ExperimentalPostingReadGeneration);
        generation.* = .{
            .alloc = alloc,
            .covered_source_sequence = .init(covered_source_sequence),
            .parent = parent,
        };
        parent.retain();
        return generation;
    }

    fn retain(self: *ExperimentalPostingReadGeneration) void {
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0 and previous < std.math.maxInt(u32));
    }

    fn releaseOpaque(ptr: *anyopaque) void {
        const self: *ExperimentalPostingReadGeneration = @ptrCast(@alignCast(ptr));
        self.release();
    }

    fn release(self: *ExperimentalPostingReadGeneration) void {
        var current: ?*ExperimentalPostingReadGeneration = self;
        while (current) |generation| {
            const previous = generation.refs.fetchSub(1, .acq_rel);
            std.debug.assert(previous > 0);
            if (previous != 1) return;
            const parent = generation.parent;
            var values = generation.values.valueIterator();
            while (values.next()) |slot| if (slot.*) |owned| generation.alloc.free(owned);
            generation.values.deinit(generation.alloc);
            if (generation.root) |state| {
                state.deinit();
                generation.alloc.destroy(state);
            }
            const alloc = generation.alloc;
            alloc.destroy(generation);
            current = parent;
        }
    }

    fn advanceCoveredSourceSequence(self: *ExperimentalPostingReadGeneration, sequence: u64) void {
        var current = self.covered_source_sequence.load(.acquire);
        while (sequence > current) {
            current = self.covered_source_sequence.cmpxchgWeak(
                current,
                sequence,
                .release,
                .acquire,
            ) orelse return;
        }
    }

    fn set(self: *ExperimentalPostingReadGeneration, posting_id: u64, kind: vectorindex_posting_wal.RecordKind, value_bytes: ?[]const u8) !void {
        const owned = if (value_bytes) |bytes| try self.alloc.dupe(u8, bytes) else null;
        errdefer if (owned) |bytes| self.alloc.free(bytes);
        const result = try self.values.getOrPut(self.alloc, experimentalPostingValueKey(posting_id, kind));
        std.debug.assert(!result.found_existing);
        result.value_ptr.* = owned;
    }

    fn value(self: *ExperimentalPostingReadGeneration, posting_id: u64, kind: vectorindex_posting_wal.RecordKind) !?[]const u8 {
        var generation: ?*ExperimentalPostingReadGeneration = self;
        const key = experimentalPostingValueKey(posting_id, kind);
        while (generation) |current| : (generation = current.parent) {
            if (current.values.get(key)) |maybe_value| return maybe_value orelse error.NotFound;
            if (current.root) |state| return try state.value(posting_id, kind);
        }
        return error.Corrupted;
    }
};

fn experimentalPostingValueKey(posting_id: u64, kind: vectorindex_posting_wal.RecordKind) u128 {
    return (@as(u128, posting_id) << 8) | @intFromEnum(kind);
}

fn experimentalPostingValueKeyId(key: u128) u64 {
    return @intCast(key >> 8);
}

const ExperimentalPostingLatestValues = std.AutoHashMapUnmanaged(u128, ?[]const u8);
const ExperimentalPostingCapturedValues = std.AutoHashMapUnmanaged(u128, ?[]u8);

fn experimentalPostingRootState(generation: *ExperimentalPostingReadGeneration) ?*ExperimentalPostingReadState {
    var current: ?*ExperimentalPostingReadGeneration = generation;
    while (current) |item| : (current = item.parent) {
        if (item.root) |state| return state;
    }
    return null;
}

/// Collects only values changed since the immutable root, newest first. The
/// slices remain borrowed from `generation`, which the caller must retain.
fn collectExperimentalPostingLatestValues(
    alloc: Allocator,
    generation: *ExperimentalPostingReadGeneration,
    latest: *ExperimentalPostingLatestValues,
) !void {
    var current: ?*ExperimentalPostingReadGeneration = generation;
    while (current) |item| : (current = item.parent) {
        var values = item.values.iterator();
        while (values.next()) |entry| {
            const result = try latest.getOrPut(alloc, entry.key_ptr.*);
            if (!result.found_existing) result.value_ptr.* = entry.value_ptr.*;
        }
        if (item.root) |state| {
            var materialized = state.materialized.iterator();
            while (materialized.next()) |entry| {
                const result = try latest.getOrPut(alloc, entry.key_ptr.*);
                if (!result.found_existing) result.value_ptr.* = entry.value_ptr.bytes;
            }
            break;
        }
    }
}

/// Collects only live overlays above the immutable root. This is used to
/// collapse many per-batch generations without duplicating values already
/// materialized by the root segment/WAL snapshot.
fn collectExperimentalPostingOverlayValues(
    alloc: Allocator,
    generation: *ExperimentalPostingReadGeneration,
    latest: *ExperimentalPostingLatestValues,
) !*ExperimentalPostingReadGeneration {
    var current: ?*ExperimentalPostingReadGeneration = generation;
    while (current) |item| : (current = item.parent) {
        if (item.root != null) return item;
        var values = item.values.iterator();
        while (values.next()) |entry| {
            const result = try latest.getOrPut(alloc, entry.key_ptr.*);
            if (!result.found_existing) result.value_ptr.* = entry.value_ptr.*;
        }
    }
    return error.Corrupted;
}

fn experimentalPostingCheckpointValue(
    latest: *const ExperimentalPostingLatestValues,
    root: *ExperimentalPostingReadState,
    id: u64,
    kind: vectorindex_posting_wal.RecordKind,
) !?[]const u8 {
    if (latest.get(experimentalPostingValueKey(id, kind))) |value| return value;
    return root.value(id, kind) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

const ExperimentalVectorOverride = struct {
    kind: vectorindex_hbc_vector_directory.Kind,
    id: u64,
    value: ?[]const u8,

    fn lessThan(_: void, lhs: ExperimentalVectorOverride, rhs: ExperimentalVectorOverride) bool {
        return compare(lhs, rhs) == .lt;
    }

    fn compare(lhs: ExperimentalVectorOverride, rhs: ExperimentalVectorOverride) std.math.Order {
        const lhs_kind = @intFromEnum(lhs.kind);
        const rhs_kind = @intFromEnum(rhs.kind);
        if (lhs_kind < rhs_kind) return .lt;
        if (lhs_kind > rhs_kind) return .gt;
        return std.math.order(lhs.id, rhs.id);
    }

    fn compareItem(lhs: ExperimentalVectorOverride, rhs: vectorindex_hbc_vector_directory.Reader.Item) std.math.Order {
        return compare(lhs, .{ .kind = rhs.kind, .id = rhs.id, .value = rhs.value });
    }
};

fn experimentalPostingValueKeyKind(key: u128) !vectorindex_posting_wal.RecordKind {
    return switch (@as(u8, @truncate(key))) {
        @intFromEnum(vectorindex_posting_wal.RecordKind.base) => .base,
        @intFromEnum(vectorindex_posting_wal.RecordKind.quantized_checkpoint) => .quantized_checkpoint,
        @intFromEnum(vectorindex_posting_wal.RecordKind.posting_state) => .posting_state,
        @intFromEnum(vectorindex_posting_wal.RecordKind.node_range) => .node_range,
        @intFromEnum(vectorindex_posting_wal.RecordKind.vector_leaf) => .vector_leaf,
        @intFromEnum(vectorindex_posting_wal.RecordKind.vector_metadata) => .vector_metadata,
        @intFromEnum(vectorindex_posting_wal.RecordKind.index_metadata) => .index_metadata,
        else => error.InvalidPostingPatchTarget,
    };
}

pub const PostingCheckpointStats = struct {
    generation: u64,
    covered_source_sequence: u64,
    posting_count: u64,
    segment_bytes: u64,
    flattened_generation: bool,
};

const ExperimentalPostingCheckpointBuildResult = struct {
    segment_bytes: []u8,
    posting_count: u64,
};

/// A retained immutable generation can be rewritten independently of source
/// replay. Publication later carries forward the exact durable WAL suffix
/// appended after `flattened_wal_bytes`.
const ExperimentalPostingCheckpointBuild = struct {
    owner_alloc: Allocator,
    source_generation: *ExperimentalPostingReadGeneration,
    metadata: IndexMetadata,
    segment_generation: u64,
    covered_source_sequence: u64,
    flattened_wal_bytes: u64,
    wal_generation: u64,
    staging_store: posting_segment_store_mod.Store,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    /// Hard recovery-debt enforcement and graceful close can promote an
    /// opportunistic build to mandatory progress so foreground queries cannot
    /// indefinitely hold durability/shutdown behind the soft-priority gate.
    force_progress: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,
    completed: std.atomic.Value(bool) = .init(false),
    build_error: ?anyerror = null,
    result: ?ExperimentalPostingCheckpointBuildResult = null,

    fn allocator() Allocator {
        return platform.allocator.processAllocator(std.heap.smp_allocator);
    }

    fn run(self: *ExperimentalPostingCheckpointBuild) void {
        yieldExperimentalCheckpointForForeground(
            self.resource_manager,
            &self.force_progress,
        );
        const result = HBCIndex.buildExperimentalPostingCheckpointFromGeneration(
            allocator(),
            self.source_generation,
            self.metadata,
            self.covered_source_sequence,
            self.resource_manager,
            &self.force_progress,
        ) catch |err| {
            self.build_error = err;
            self.completed.store(true, .release);
            return;
        };
        self.staging_store.stageCheckpointSegment(
            self.segment_generation,
            result.segment_bytes,
        ) catch |err| {
            allocator().free(result.segment_bytes);
            self.build_error = err;
            self.completed.store(true, .release);
            return;
        };
        self.result = result;
        self.completed.store(true, .release);
    }

    fn deinit(self: *ExperimentalPostingCheckpointBuild) void {
        if (self.thread) |thread| thread.join();
        if (self.result) |result| allocator().free(result.segment_bytes);
        self.staging_store.deinit();
        self.source_generation.release();
        const owner_alloc = self.owner_alloc;
        owner_alloc.destroy(self);
    }
};

fn yieldExperimentalCheckpointForForeground(
    resource_manager: ?*resource_manager_mod.ResourceManager,
    force_progress: ?*const std.atomic.Value(bool),
) void {
    const manager = resource_manager orelse return;
    while (manager.shouldDeferOptionalMaintenanceForForegroundQuery()) {
        if (force_progress) |force| if (force.load(.acquire)) return;
        platform.time.yieldBriefly();
    }
}

pub const PostingWalAppendStats = struct {
    batch_id: u64,
    covered_source_sequence: u64,
    record_count: u64,
    wal_committed_bytes: u64,
};

pub const ExperimentalPostingWalAppendOptions = struct {
    sync: bool = true,
};

/// Cheap in-memory overlay collapse controls resident version debt separately
/// from durable full-segment compaction. A larger durable tail substantially
/// reduces whole-index rewrites while keeping restart work explicitly bounded.
const managedPostingOverlayCollapseBytes: u64 = 32 * 1024 * 1024;
const managedPostingIdleCheckpointWalBytes: u64 = 64 * 1024 * 1024;
const managedPostingCheckpointHardWalBytes: u64 = 256 * 1024 * 1024;
const managedPostingCheckpointPolicy: posting_segment_store_mod.CheckpointPolicy = .{
    .min_wal_bytes = 128 * 1024 * 1024,
    .max_wal_bytes = 128 * 1024 * 1024,
    .wal_to_segment_percent = 100,
};

const ExperimentalPostingMaintenanceCapture = struct {
    covered_source_sequence: u64,
};

pub const HBCIndex = struct {
    alloc: Allocator,
    env_owner: EnvOwner,
    store: vectorindex_store.NamespaceStore,
    config: HBCConfig,
    metadata: IndexMetadata,
    published_root_node: AtomicU64,
    published_active_count: AtomicU64,
    published_node_count: AtomicU64,
    published_generation: AtomicU64,
    experimental_posting_reads_enabled: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    experimental_posting_sidecar_managed: bool = false,
    experimental_posting_read_generation: ?*ExperimentalPostingReadGeneration = null,
    experimental_posting_read_generation_mu: std.atomic.Mutex = .unlocked,
    experimental_posting_delta_base_state: ?*ExperimentalPostingReadState = null,
    experimental_posting_write_store: ?posting_segment_store_mod.Store = null,
    experimental_posting_checkpoint_build: ?*ExperimentalPostingCheckpointBuild = null,
    experimental_posting_overlay_collapsed_wal_bytes: u64 = 0,
    experimental_posting_wal_mutations_enabled: bool = false,
    experimental_posting_wal_authoritative: std.atomic.Value(bool) = .init(false),
    experimental_posting_wal_authoritative_persisted: bool = false,
    experimental_posting_mutation_base_generation: ?*ExperimentalPostingReadGeneration = null,
    experimental_posting_capture_enabled: bool = false,
    experimental_posting_captured_values: ExperimentalPostingCapturedValues = .empty,
    rng: go_rand.GoPcg,
    // Set when a write path observes a tree-link inconsistency (stale parent
    // pointer, dangling node reference); background maintenance runs a
    // bounded repairTreeLinks sweep and clears it on completion.
    link_repair_pending: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    quantizer: quantizer_mod.RaBitQuantizer,
    rot: vec.RandomOrthogonalTransformer,
    node_cache: std.AutoHashMapUnmanaged(u64, *NodeCacheEntry),
    node_cache_slots: std.AutoHashMapUnmanaged(u64, usize),
    node_clock_keys: []u64,
    node_clock_refs: []bool,
    node_clock_hand: usize,
    pinned_node_cache: std.AutoHashMapUnmanaged(u64, *NodeCacheEntry),
    quantized_cache: std.AutoHashMapUnmanaged(u64, *QuantizedCacheEntry),
    quantized_cache_slots: std.AutoHashMapUnmanaged(u64, usize),
    quantized_clock_keys: []u64,
    quantized_clock_refs: []bool,
    quantized_clock_hand: usize,
    pinned_quantized_cache: std.AutoHashMapUnmanaged(u64, *QuantizedCacheEntry),
    vector_cache: std.AutoHashMapUnmanaged(u64, *VectorCacheEntry),
    vector_cache_slots: std.AutoHashMapUnmanaged(u64, usize),
    vector_clock_keys: []u64,
    vector_clock_refs: []bool,
    vector_clock_hand: usize,
    metadata_cache: std.AutoHashMapUnmanaged(u64, *MetadataCacheEntry),
    metadata_cache_slots: std.AutoHashMapUnmanaged(u64, usize),
    metadata_clock_keys: []u64,
    metadata_clock_refs: []bool,
    metadata_clock_hand: usize,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    bind_shared_cache_resource_manager: bool = true,
    shared_cache: ?*Cache = null,
    shared_cache_registered: bool = false,
    cache_namespace: u64 = 0,
    cache_path: []u8 = &.{},
    cache_enabled: bool = true,
    retained_vector_cache_enabled: bool = true,
    bypass_external_vector_cache: bool = false,
    cache_mu: apply_rw_lock_mod.ApplyRwLock,
    active_searches: std.atomic.Value(u32),
    hbc_cache_bytes_accounted: u64 = 0,
    search_workspace_bytes_accounted: u64 = 0,
    routing_scratch_bytes_accounted: u64 = 0,
    apply_workspace_bytes_accounted: u64 = 0,
    apply_workspace_split_bytes: u64 = 0,
    deferred_node_key_value_bytes: u64 = 0,
    deferred_oversized_leaves_peak: u64 = 0,
    bulk_split_vector_workspace: SplitVectorWorkspace = .{},
    hbc_cache_kind_stats: [hbc_cache_kind_count]HbcCacheKindStats = .{HbcCacheKindStats{}} ** hbc_cache_kind_count,
    deferred_quantized_nodes: std.AutoHashMapUnmanaged(u64, void),
    deferred_node_keys: std.AutoHashMapUnmanaged(u128, DeferredNodeValue),
    deferred_oversized_leaves: std.AutoHashMapUnmanaged(u64, void),
    write_session_depth: usize = 0,
    write_session_kind: ?WriteSessionKind = null,
    bulk_publication_may_have_mutated: bool = false,
    hilbert: ?vec.Hilbert,
    scratch_mu: std.atomic.Mutex,
    cached_scratch: ?SearchScratch,
    routing_scratch_mu: std.atomic.Mutex,
    cached_routing_scratch: ?RoutingScratch,
    flat_centroid_mu: std.atomic.Mutex,
    flat_centroid_directory: ?*vectorindex_spfresh_index.FlatCentroidDirectory,
    write_profile: WriteProfile = .{},
    external_vector_ctx: ?*anyopaque = null,
    external_vector_loader: ?ExternalVectorLoader = null,
    external_vector_scratch_loader: ?ExternalVectorScratchLoader = null,
    external_vector_batch_scratch_loader: ?ExternalVectorBatchScratchLoader = null,
    external_vector_batch_transformed_matrix_loader: ?ExternalVectorBatchTransformedMatrixLoader = null,
    external_vector_batch_distance_loader: ?ExternalVectorBatchDistanceLoader = null,

    const EnvOwner = hbc_backend.OpenedBackend;
    pub const ExternalVectorLoader = *const fn (ctx: *anyopaque, alloc: Allocator, vector_id: u64, metadata: []const u8) anyerror![]f32;
    pub const ExternalVectorScratchLoader = *const fn (ctx: *anyopaque, vector_id: u64, metadata: []const u8, scratch: []f32) anyerror![]const f32;
    pub const ExternalVectorBatchScratchLoader = *const fn (ctx: *anyopaque, vector_ids: []const u64, metadata: []const ?[]const u8, vector_views: [][]const f32, batch_scratch: []f32, dims: usize) anyerror!void;
    pub const ExternalVectorTransformFn = *const fn (index: *HBCIndex, original: []const f32, transformed: []f32) []const f32;
    pub const ExternalVectorBatchTransformedMatrixLoader = *const fn (ctx: *anyopaque, vector_ids: []const u64, metadata: []const ?[]const u8, matrix_positions: []const usize, matrix: []f32, scratch: []f32, dims: usize, index: *HBCIndex, transform: ExternalVectorTransformFn) anyerror!void;
    pub const ExternalVectorBatchDistanceScratch = struct {
        artifact_keys: [][]const u8,
        raw_values: []?[]const u8,
    };
    pub const ExternalVectorBatchDistanceLoader = *const fn (
        ctx: *anyopaque,
        vector_ids: []const u64,
        metadata: []const ?[]const u8,
        query: []const f32,
        query_measure: f32,
        metric: vec.DistanceMetric,
        distances: []f32,
        batch_scratch: []f32,
        dims: usize,
        scratch: ExternalVectorBatchDistanceScratch,
        profile: ?*vectorindex_search_types.SearchProfile,
    ) anyerror!void;
    pub const BorrowedNode = BorrowedNodeLease;
    pub const BorrowedQuantized = BorrowedQuantizedLease;
    pub const BorrowedVector = BorrowedVectorLease;
    pub const BorrowedMetadata = BorrowedMetadataLease;

    pub const Namespace = vectorindex_store.Namespace;

    const SplitVectorWorkspace = struct {
        active: bool = false,
        map: std.AutoHashMapUnmanaged(u64, usize) = .empty,
        vectors: std.ArrayListUnmanaged(f32) = .empty,
        ids: std.ArrayListUnmanaged(u64) = .empty,
        next_victim: usize = 0,
        replacements_since_rebuild: usize = 0,
        accounted_bytes: u64 = 0,

        fn bytes(self: *const SplitVectorWorkspace) u64 {
            return @as(u64, @intCast(self.vectors.capacity)) * @sizeOf(f32) +
                @as(u64, @intCast(self.ids.capacity)) * @sizeOf(u64) +
                @as(u64, @intCast(self.map.capacity())) * (@sizeOf(u64) + @sizeOf(usize));
        }

        fn clearRetainingCapacity(self: *SplitVectorWorkspace) void {
            self.map.clearRetainingCapacity();
            self.vectors.clearRetainingCapacity();
            self.ids.clearRetainingCapacity();
            self.next_victim = 0;
            self.replacements_since_rebuild = 0;
        }

        fn deinit(self: *SplitVectorWorkspace, alloc: Allocator) void {
            self.map.deinit(alloc);
            self.vectors.deinit(alloc);
            self.ids.deinit(alloc);
            self.* = .{};
        }
    };

    const HbcCacheAdmission = struct {
        index: *HBCIndex,
        reserved_bytes: u64 = 0,
        active: bool = false,

        fn none(index: *HBCIndex) HbcCacheAdmission {
            return .{ .index = index };
        }

        fn commit(self: *HbcCacheAdmission) void {
            self.active = false;
        }

        fn rollback(self: *HbcCacheAdmission) void {
            if (!self.active or self.reserved_bytes == 0) return;
            if (self.index.resource_manager) |manager| {
                manager.releaseBytes(.hbc_node_metadata_cache, self.reserved_bytes);
            }
            self.index.hbc_cache_bytes_accounted -|= self.reserved_bytes;
            self.active = false;
        }
    };

    const RoutingScratch = struct {
        estimate: quantizer_mod.RaBitQuantizer.EstimateScratch,
        child_ids: []u64,
        distances: []f32,
        error_bounds: []f32,
        competitive: []vectorindex_types.PriorityItem,

        fn init(alloc: Allocator, dims: usize, initial_capacity: usize) !@This() {
            const capacity = @max(initial_capacity, 1);
            const estimate = try quantizer_mod.RaBitQuantizer.EstimateScratch.init(alloc, dims);
            errdefer {
                var tmp = estimate;
                tmp.deinit(alloc);
            }
            const child_ids = try alloc.alloc(u64, capacity);
            errdefer alloc.free(child_ids);
            const distances = try alloc.alloc(f32, capacity);
            errdefer alloc.free(distances);
            const error_bounds = try alloc.alloc(f32, capacity);
            errdefer alloc.free(error_bounds);
            const competitive = try alloc.alloc(vectorindex_types.PriorityItem, capacity);
            return .{
                .estimate = estimate,
                .child_ids = child_ids,
                .distances = distances,
                .error_bounds = error_bounds,
                .competitive = competitive,
            };
        }

        pub fn ensureCapacity(self: *@This(), alloc: Allocator, needed: usize) !void {
            const capacity = @max(needed, 1);
            if (self.child_ids.len < capacity) self.child_ids = try alloc.realloc(self.child_ids, capacity);
            if (self.distances.len < capacity) self.distances = try alloc.realloc(self.distances, capacity);
            if (self.error_bounds.len < capacity) self.error_bounds = try alloc.realloc(self.error_bounds, capacity);
            if (self.competitive.len < capacity) self.competitive = try alloc.realloc(self.competitive, capacity);
        }

        fn deinit(self: *@This(), alloc: Allocator) void {
            self.estimate.deinit(alloc);
            alloc.free(self.child_ids);
            alloc.free(self.distances);
            alloc.free(self.error_bounds);
            alloc.free(self.competitive);
            self.* = undefined;
        }

        fn bytes(self: *const @This()) u64 {
            return @as(u64, @intCast(self.estimate.query_diff.len * @sizeOf(f32))) +
                @as(u64, @intCast(self.estimate.q1.len * @sizeOf(u64))) +
                @as(u64, @intCast(self.estimate.q2.len * @sizeOf(u64))) +
                @as(u64, @intCast(self.estimate.q3.len * @sizeOf(u64))) +
                @as(u64, @intCast(self.estimate.q4.len * @sizeOf(u64))) +
                @as(u64, @intCast(self.child_ids.len * @sizeOf(u64))) +
                @as(u64, @intCast(self.distances.len * @sizeOf(f32))) +
                @as(u64, @intCast(self.error_bounds.len * @sizeOf(f32))) +
                @as(u64, @intCast(self.competitive.len * @sizeOf(vectorindex_types.PriorityItem)));
        }
    };

    const RoutingScratchHandle = struct {
        scratch: RoutingScratch,
        from_cache: bool = false,
    };

    fn runtimeNamespace(namespace: Namespace) backend_types.Namespace {
        return switch (namespace) {
            .nodes => .{ .name = "hbc_nodes" },
            .meta => .{ .name = "hbc_meta" },
            .quant => .{ .name = "hbc_quant" },
            .vecs => .{ .name = "hbc_vecs" },
        };
    }

    fn mapBackendNamespace(namespace: vectorindex_store.Namespace) !backend_types.Namespace {
        return runtimeNamespace(namespace);
    }

    const NativeStoreShell = struct {
        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn beginRead(_: Allocator, _: *anyopaque) anyerror!vectorindex_store.NamespaceReadTxn {
            return error.HbcNativeGenerationRequired;
        }
        fn beginWrite(_: Allocator, _: *anyopaque) anyerror!vectorindex_store.NamespaceWriteTxn {
            return error.HbcNativeGenerationRequired;
        }
        fn beginBatch(_: Allocator, _: *anyopaque) anyerror!vectorindex_store.NamespaceBatch {
            return error.HbcNativeGenerationRequired;
        }
        const vtable: vectorindex_store.NamespaceStore.VTable = .{
            .deinit = deinit,
            .begin_read = beginRead,
            .begin_write = beginWrite,
            .begin_batch = beginBatch,
        };
    };

    fn nativeNamespaceStore(allocator: Allocator, backend: *hbc_backend.NativeBackend) vectorindex_store.NamespaceStore {
        return .{
            .allocator = allocator,
            .ptr = backend,
            .vtable = &NativeStoreShell.vtable,
        };
    }

    fn openVectorIndexStore(allocator: Allocator, opened: hbc_backend.OpenedBackend) !vectorindex_store.NamespaceStore {
        if (opened == .native) return nativeNamespaceStore(allocator, opened.native);
        var backend_store: ?backend_erased.NamespaceStore = try opened.runtimeNamespaceStore(allocator);
        errdefer if (backend_store) |*owned| owned.deinit();

        const store = try vectorindex_store.namespaceStoreFrom(
            allocator,
            backend_store.?,
            backend_types.Namespace,
            mapBackendNamespace,
        );
        backend_store = null;
        return store;
    }

    pub fn runtimeNamespaceStore(self: *HBCIndex, allocator: Allocator) !vectorindex_store.NamespaceStore {
        return try openVectorIndexStore(allocator, self.env_owner);
    }

    pub fn snapshotLsmWriteStats(self: *const HBCIndex) ?LsmWriteStats {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.snapshotWriteStats(),
            .lmdb, .native => null,
        };
    }

    pub fn snapshotLsmMaintenanceStats(self: *const HBCIndex) ?LsmMaintenanceStats {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
            .lmdb, .native => null,
        };
    }

    pub fn snapshotLsmOpenStats(self: *const HBCIndex) ?LsmOpenStats {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.snapshotOpenStats(),
            .lmdb, .native => null,
        };
    }

    pub fn checkpointLsmWalAfterDurableBoundary(self: *HBCIndex) !void {
        switch (self.env_owner) {
            .lsm => |handle| try handle.backend.checkpointWalAfterDurableBoundary(),
            .lmdb, .native => {},
        }
    }

    pub fn snapshotLsmNativeStorageStats(self: *const HBCIndex) ?lsm_backend.NativeStorageStats {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.snapshotNativeStorageStats(),
            .lmdb, .native => null,
        };
    }

    pub fn lsmMaintenanceScore(self: *const HBCIndex) u64 {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.maintenanceScore(),
            .lmdb, .native => 0,
        };
    }

    pub fn lsmMaintenanceDebtHint(self: *const HBCIndex) u64 {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.maintenanceDebtHint(),
            .lmdb, .native => 0,
        };
    }

    pub fn nextLsmMaintenanceWakeDelayNsBestEffort(self: *const HBCIndex) ?u64 {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.nextMaintenanceWakeDelayNsBestEffort(),
            .lmdb, .native => null,
        };
    }

    pub fn refreshLsmMaintenanceDebtHint(self: *HBCIndex) void {
        switch (self.env_owner) {
            .lsm => |handle| handle.backend.refreshMaintenanceDebtHint(),
            .lmdb, .native => {},
        }
    }

    pub fn runLsmMaintenanceStep(self: *HBCIndex) !bool {
        return switch (self.env_owner) {
            .lsm => |handle| try handle.backend.runMaintenanceStep(),
            .lmdb, .native => false,
        };
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *HBCIndex) !bool {
        return switch (self.env_owner) {
            .lsm => |handle| blk: {
                // An open HBC publication session suppresses compaction and
                // ordinary flush work, but never a due WAL resource/durability
                // checkpoint. Backend bulk-mode maintenance is pressure-only.
                if (self.shouldSuppressRoutineMaintenance()) {
                    const due = if (handle.backend.nextMaintenanceWakeDelayNsBestEffort()) |delay_ns|
                        delay_ns == 0
                    else
                        false;
                    if (!due) break :blk false;
                }
                break :blk try handle.backend.runMaintenanceStepBestEffort();
            },
            .lmdb, .native => false,
        };
    }

    pub fn lsmSessionBatchingActive(self: *const HBCIndex) bool {
        return self.write_session_depth != 0;
    }

    pub fn crossBatchPublicationActive(self: *const HBCIndex) bool {
        return self.write_session_depth != 0 and self.write_session_kind == .bulk_publication;
    }

    fn nativeHbcAuthoritative(self: *const HBCIndex) bool {
        // Raw vector values are intentionally not duplicated into the HBC
        // generation. Native-only operation is therefore complete only when
        // exact vectors come from the primary document/embedding store.
        return self.experimental_posting_wal_authoritative.load(.acquire) and self.hasExternalVectorLoader();
    }

    pub fn mustPublishMetadataPerBatch(self: *const HBCIndex) bool {
        return !self.crossBatchPublicationActive();
    }

    pub fn shouldPublishSearchStatePerBatch(self: *const HBCIndex) bool {
        return !self.crossBatchPublicationActive();
    }

    pub fn shouldSuppressRoutineMaintenance(self: *const HBCIndex) bool {
        return self.lsmSessionBatchingActive();
    }

    fn writeSessionFinishNeedsExplicitDurableSync(self: *const HBCIndex) bool {
        if (self.nativeHbcAuthoritative()) return false;
        return switch (self.env_owner) {
            .lmdb => self.config.no_sync or self.config.no_meta_sync,
            .lsm => |handle| handle.backend.options.backend.durability != .full,
            .native => false,
        };
    }

    fn beginWriteSession(self: *HBCIndex, kind: WriteSessionKind) !void {
        if (self.write_session_depth != 0 and self.write_session_kind != kind) {
            return error.MixedWriteSessionKinds;
        }
        if (!self.nativeHbcAuthoritative()) switch (self.env_owner) {
            .lsm => |handle| handle.backend.beginBulkIngestSession() catch |err| return err,
            .lmdb, .native => {},
        };
        const opening_outermost = self.write_session_depth == 0;
        if (opening_outermost) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
            self.clearDeferredNodeKeys();
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = self.bulk_split_vector_workspace.accounted_bytes;
            self.deferred_node_key_value_bytes = 0;
            self.observeApplyWorkspaceBytes();
            self.write_session_kind = kind;
            self.bulk_publication_may_have_mutated = false;
            // Streaming replay normalizes oversized leaves inside its source
            // window. Retain only vectors actually needed by a fallback split
            // so recursive/update-heavy mutations do not reread them from the
            // primary LSM. The workspace is resource-accounted, bounded, and
            // discarded at the transaction-session boundary.
            if (kind == .streaming_replay) self.beginStreamingSplitVectorWorkspace();
        }
        self.write_session_depth += 1;
        if (opening_outermost and kind == .bulk_publication and !self.nativeHbcAuthoritative()) {
            self.persistBulkPublishState() catch |err| {
                self.write_session_depth -= 1;
                self.write_session_kind = null;
                switch (self.env_owner) {
                    .lsm => |handle| handle.backend.abortBulkIngestSession(),
                    .lmdb, .native => {},
                }
                return err;
            };
        }
    }

    pub fn beginStreamingReplaySession(self: *HBCIndex) !void {
        try self.beginWriteSession(.streaming_replay);
    }

    pub fn beginBulkIngestSession(self: *HBCIndex) !void {
        try self.beginWriteSession(.bulk_publication);
    }

    fn finishWriteSessionWithOptions(
        self: *HBCIndex,
        expected_kind: WriteSessionKind,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        if (self.write_session_depth == 0) return error.NoActiveWriteSession;
        if (self.write_session_kind != expected_kind) return error.WriteSessionKindMismatch;
        const finishing_outermost = self.write_session_depth == 1;
        if (finishing_outermost and expected_kind == .bulk_publication) {
            if (options.progress_fn) |progress| if (options.progress_ctx) |progress_ctx| {
                progress(progress_ctx, .{
                    .phase = .begin,
                    .deferred_leaf_splits = @intCast(self.deferred_oversized_leaves.count()),
                });
            };
            self.beginBulkSplitVectorWorkspace();
            errdefer self.endBulkSplitVectorWorkspace();
            var publish_window: u64 = 0;
            while (true) {
                try options.checkAdmission();
                publish_window += 1;
                const window_start_ns = nowNs();
                var batch = try self.beginRuntimeBatchTxn();
                errdefer batch.abort();
                const split_calls_before = self.write_profile.split_leaf_calls;
                const has_more_deferred_splits = try self.normalizeDeferredOversizedLeavesForBulkFinishTxn(&batch, options);
                const split_steps = self.write_profile.split_leaf_calls - split_calls_before;
                const split_elapsed_ns = elapsedSince(window_start_ns);
                if (options.progress_fn) |progress| if (options.progress_ctx) |progress_ctx| {
                    progress(progress_ctx, .{
                        .phase = .split,
                        .publish_window = publish_window,
                        .split_steps = @intCast(split_steps),
                        .deferred_leaf_splits = @intCast(self.deferred_oversized_leaves.count()),
                        .elapsed_ns = split_elapsed_ns,
                    });
                };
                if (split_steps > 0) {
                    self.write_profile.deferred_leaf_split_publish_windows += 1;
                    self.write_profile.deferred_leaf_split_steps += split_steps;
                    self.write_profile.deferred_leaf_split_window_max_steps = @max(
                        self.write_profile.deferred_leaf_split_window_max_steps,
                        split_steps,
                    );
                }
                try self.publishDeferredNodeKeysForBulkFinishTxn(&batch);
                try self.publishDeferredQuantizedNodesForBulkFinishTxn(&batch);
                try self.flushMetadataNow(&batch);
                if (!has_more_deferred_splits and !self.nativeHbcAuthoritative()) {
                    try self.clearBulkPublishStateTxn(&batch);
                }
                const commit_start = nowNs();
                self.beginPublishedSearchStateRefresh();
                errdefer self.abortPublishedSearchStateRefresh();
                try batch.commit();
                self.write_profile.insert_commit_ns += elapsedSince(commit_start);
                self.finishPublishedSearchStateRefresh();
                if (options.progress_fn) |progress| if (options.progress_ctx) |progress_ctx| {
                    progress(progress_ctx, .{
                        .phase = .publish,
                        .publish_window = publish_window,
                        .split_steps = @intCast(split_steps),
                        .deferred_leaf_splits = @intCast(self.deferred_oversized_leaves.count()),
                        .elapsed_ns = elapsedSince(window_start_ns),
                    });
                };
                if (!has_more_deferred_splits) break;
            }
            self.endBulkSplitVectorWorkspace();
            if (options.progress_fn) |progress| if (options.progress_ctx) |progress_ctx| {
                progress(progress_ctx, .{
                    .phase = .complete,
                    .publish_window = publish_window,
                    .deferred_leaf_splits = @intCast(self.deferred_oversized_leaves.count()),
                });
            };
        }
        var finish_options = options;
        if (finishing_outermost) {
            // Both session kinds require a durable LSM boundary before their
            // caller may publish an applied sequence. Bulk publication also
            // publishes deferred HBC state above before reaching this point.
            finish_options.flush = true;
        }
        if (!self.nativeHbcAuthoritative()) switch (self.env_owner) {
            .lsm => |handle| handle.backend.finishBulkIngestSessionWithOptions(finish_options) catch |err| {
                if (finishing_outermost and expected_kind == .bulk_publication) {
                    self.persistBulkPublishState() catch {};
                }
                return err;
            },
            .lmdb, .native => {},
        };
        if (finishing_outermost and expected_kind == .streaming_replay) self.endStreamingSplitVectorWorkspace();
        self.write_session_depth -= 1;
        if (finishing_outermost) self.refreshPublishedSearchState();
        if (self.write_session_depth == 0) {
            self.write_session_kind = null;
            self.bulk_publication_may_have_mutated = false;
            self.releaseDeferredBulkWorkspaceCapacity();
        }
    }

    pub fn finishStreamingReplaySessionWithOptions(
        self: *HBCIndex,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        const finishing_outermost = self.write_session_depth == 1 and self.write_session_kind == .streaming_replay;
        const needs_explicit_sync = finishing_outermost and self.writeSessionFinishNeedsExplicitDurableSync();
        try self.finishWriteSessionWithOptions(.streaming_replay, options);
        if (needs_explicit_sync) try self.sync(true);
    }

    pub fn finishBulkIngestSessionWithOptions(self: *HBCIndex, options: backend_types.BulkIngestFinishOptions) !void {
        const finishing_outermost = self.write_session_depth == 1 and self.write_session_kind == .bulk_publication;
        const needs_explicit_sync = finishing_outermost and self.writeSessionFinishNeedsExplicitDurableSync();
        try self.finishWriteSessionWithOptions(.bulk_publication, options);
        if (needs_explicit_sync) try self.sync(true);
    }

    fn abortWriteSession(self: *HBCIndex, expected_kind: WriteSessionKind) void {
        if (self.write_session_depth == 0 or self.write_session_kind != expected_kind) return;
        if (!self.nativeHbcAuthoritative()) switch (self.env_owner) {
            .lsm => |handle| handle.backend.abortBulkIngestSession(),
            .lmdb, .native => {},
        };
        self.write_session_depth -= 1;
        if (self.write_session_depth == 0) {
            if (expected_kind == .streaming_replay) self.endStreamingSplitVectorWorkspace();
            if (expected_kind == .bulk_publication) self.endBulkSplitVectorWorkspace();
            if (expected_kind == .bulk_publication and
                !self.bulk_publication_may_have_mutated and
                !self.nativeHbcAuthoritative())
            {
                self.clearBulkPublishStateBestEffort();
            }
            self.write_session_kind = null;
            self.bulk_publication_may_have_mutated = false;
            self.refreshPublishedSearchState();
            self.releaseDeferredBulkWorkspaceCapacity();
        }
    }

    pub fn abortStreamingReplaySession(self: *HBCIndex) void {
        self.abortWriteSession(.streaming_replay);
    }

    pub fn abortBulkIngestSession(self: *HBCIndex) void {
        self.abortWriteSession(.bulk_publication);
    }

    const SplitResult = vectorindex_hbc_index.SplitResult;

    /// Open or create an HBC index at the given path.
    pub fn open(alloc: Allocator, path: [*:0]const u8, config: HBCConfig) !HBCIndex {
        return try openWithLsmStorage(alloc, path, config, null);
    }

    pub fn openWithLsmStorage(alloc: Allocator, path: [*:0]const u8, config: HBCConfig, lsm_storage: ?lsm_backend.Storage) !HBCIndex {
        return try openWithLsmOptions(alloc, path, config, .{ .storage = lsm_storage });
    }

    fn loadNativeIndexMetadata(alloc: Allocator, backend: *hbc_backend.NativeBackend) !IndexMetadata {
        const posting_root = try std.fs.path.join(alloc, &.{ backend.root_dir, "posting-segments" });
        defer alloc.free(posting_root);
        var opened = try posting_segment_store_mod.Store.openWithSegmentAlloc(alloc, backend.storage, posting_root);
        defer opened.deinit();
        var segment = try vectorindex_posting_segment.VerifiedReader.init(alloc, opened.segment.bytes());
        defer segment.deinit();
        var current = (try segment.getValue(0, .index_metadata)) orelse return error.MissingHbcNativeMetadata;
        var owned: ?[]u8 = null;
        defer if (owned) |bytes| alloc.free(bytes);
        var wal = try opened.store.recoverWal();
        defer wal.deinit();
        for (wal.replay.records.items) |record| {
            if (record.posting_id != 0) continue;
            switch (record.kind) {
                .index_metadata => {
                    if (owned) |bytes| alloc.free(bytes);
                    owned = null;
                    current = record.payload;
                },
                .mutation => {
                    if (record.payload.len < 6 or
                        record.payload[5] != @intFromEnum(vectorindex_posting_wal.RecordKind.index_metadata)) continue;
                    const decoded = try vectorindex_posting_wal.applyReplacementPatchAlloc(alloc, record.payload, current);
                    if (owned) |bytes| alloc.free(bytes);
                    owned = decoded.replacement;
                    current = decoded.replacement;
                },
                .index_metadata_tombstone, .tombstone => return error.MissingHbcNativeMetadata,
                else => {},
            }
        }
        return IndexMetadata.decode(current);
    }

    pub fn openWithLsmOptions(alloc: Allocator, path: [*:0]const u8, config: HBCConfig, lsm_options: hbc_backend.LsmOptions) !HBCIndex {
        var opened = try hbc_backend.openBackendWithLsmOptions(alloc, path, config, lsm_options);
        errdefer opened.close(alloc);

        var store = try openVectorIndexStore(alloc, opened);
        errdefer store.deinit();

        const metadata = if (opened == .native)
            try loadNativeIndexMetadata(alloc, opened.native)
        else if (lsm_options.backend_options.backend.read_only) blk: {
            var txn = try store.beginRead();
            defer txn.abort();
            const existing = txn.get(.meta, meta_key) catch |err| switch (err) {
                error.NotFound => return error.NotFound,
                else => return err,
            };
            break :blk IndexMetadata.decode(existing);
        } else blk: {
            var txn = try store.beginWrite();
            var txn_active = true;
            errdefer if (txn_active) txn.abort();

            const loaded = meta_blk: {
                const existing = txn.get(.meta, meta_key) catch |err| switch (err) {
                    error.NotFound => {
                        const meta = IndexMetadata{
                            .dims = config.dims,
                            .branching_factor = config.branching_factor,
                            .leaf_size = config.leaf_size,
                            .use_quantization = config.use_quantization,
                            .quantizer_seed = config.quantizer_seed,
                            .metric = @as(u8, @intCast(@intFromEnum(config.metric))),
                        };

                        var meta_buf: [IndexMetadata.encoded_size]u8 = undefined;
                        try txn.put(.meta, meta_key, meta.encode(&meta_buf));

                        var key_buf: [12]u8 = undefined;
                        var packed_buf: [vectorindex_hbc.packed_node_header_size]u8 = undefined;
                        const header = NodeHeader{ .is_leaf = true, .level = 0, .parent = 0 };
                        const packed_node = try vectorindex_hbc.encodePackedNodeValue(&packed_buf, header, &.{}, &.{});
                        try txn.put(.nodes, encodeNodeKey(&key_buf, 1, .packed_node), packed_node);

                        break :meta_blk meta;
                    },
                    else => return err,
                };
                break :meta_blk IndexMetadata.decode(existing);
            };
            if (txn.get(.meta, bulk_publish_state_key)) |_| {
                return error.IncompleteBulkPublish;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            try txn.commit();
            txn_active = false;
            break :blk loaded;
        };

        var effective_config = config;
        if (metadata.version != hbc_index_version) return error.UnsupportedVersion;
        if (metadata.dims != config.dims) return error.DimensionMismatch;

        const stored_metric: vec.DistanceMetric = switch (metadata.metric) {
            @intCast(@intFromEnum(vec.DistanceMetric.l2_squared)) => .l2_squared,
            @intCast(@intFromEnum(vec.DistanceMetric.inner_product)) => .inner_product,
            @intCast(@intFromEnum(vec.DistanceMetric.cosine)) => .cosine,
            else => return error.Corrupted,
        };
        if (stored_metric != config.metric) return error.DistanceMetricMismatch;

        effective_config.metric = stored_metric;
        effective_config.branching_factor = metadata.branching_factor;
        effective_config.leaf_size = metadata.leaf_size;
        effective_config.use_quantization = metadata.use_quantization;
        effective_config.quantizer_seed = metadata.quantizer_seed;

        const env_owner: EnvOwner = opened;

        var quantizer = try quantizer_mod.RaBitQuantizer.init(
            alloc,
            effective_config.dims,
            effective_config.quantizer_seed,
            effective_config.metric,
        );
        errdefer quantizer.deinit();

        var rot = try vec.RandomOrthogonalTransformer.init(
            alloc,
            if (effective_config.use_random_ortho_trans) .givens else .none,
            effective_config.dims,
            effective_config.quantizer_seed,
        );
        errdefer rot.deinit();

        const node_clock_keys = try alloc.alloc(u64, effective_config.max_cached_nodes);
        errdefer alloc.free(node_clock_keys);
        const node_clock_refs = try alloc.alloc(bool, effective_config.max_cached_nodes);
        errdefer alloc.free(node_clock_refs);
        const quantized_clock_keys = try alloc.alloc(u64, effective_config.max_cached_nodes);
        errdefer alloc.free(quantized_clock_keys);
        const quantized_clock_refs = try alloc.alloc(bool, effective_config.max_cached_nodes);
        errdefer alloc.free(quantized_clock_refs);
        const vector_clock_keys = try alloc.alloc(u64, effective_config.max_cached_vectors);
        errdefer alloc.free(vector_clock_keys);
        const vector_clock_refs = try alloc.alloc(bool, effective_config.max_cached_vectors);
        errdefer alloc.free(vector_clock_refs);
        const metadata_clock_keys = try alloc.alloc(u64, effective_config.max_cached_metadata);
        errdefer alloc.free(metadata_clock_keys);
        const metadata_clock_refs = try alloc.alloc(bool, effective_config.max_cached_metadata);
        errdefer alloc.free(metadata_clock_refs);
        @memset(node_clock_keys, 0);
        @memset(node_clock_refs, false);
        @memset(quantized_clock_keys, 0);
        @memset(quantized_clock_refs, false);
        @memset(vector_clock_keys, 0);
        @memset(vector_clock_refs, false);
        @memset(metadata_clock_keys, 0);
        @memset(metadata_clock_refs, false);

        const cache_identity = try hbcCacheIdentityAlloc(alloc, std.mem.span(path));
        errdefer alloc.free(cache_identity.stable_path);

        const posting_wal_authoritative = if (opened == .native)
            true
        else blk: {
            var marker_txn = try store.beginRead();
            defer marker_txn.abort();
            break :blk if (marker_txn.get(.meta, posting_wal_authoritative_key)) |value|
                std.mem.eql(u8, value, posting_wal_authoritative_value)
            else |err| switch (err) {
                error.NotFound => false,
                else => return err,
            };
        };

        const idx = HBCIndex{
            .alloc = alloc,
            .env_owner = env_owner,
            .store = store,
            .config = effective_config,
            .metadata = metadata,
            .published_root_node = .init(metadata.root_node),
            .published_active_count = .init(metadata.active_count),
            .published_node_count = .init(metadata.node_count),
            .published_generation = .init(0),
            .experimental_posting_wal_authoritative = .init(posting_wal_authoritative),
            .experimental_posting_wal_authoritative_persisted = posting_wal_authoritative,
            .rng = go_rand.GoPcg.init(effective_config.quantizer_seed, 1024),
            .quantizer = quantizer,
            .rot = rot,
            .node_cache = .empty,
            .node_cache_slots = .empty,
            .node_clock_keys = node_clock_keys,
            .node_clock_refs = node_clock_refs,
            .node_clock_hand = 0,
            .pinned_node_cache = .empty,
            .quantized_cache = .empty,
            .quantized_cache_slots = .empty,
            .quantized_clock_keys = quantized_clock_keys,
            .quantized_clock_refs = quantized_clock_refs,
            .quantized_clock_hand = 0,
            .pinned_quantized_cache = .empty,
            .vector_cache = .empty,
            .vector_cache_slots = .empty,
            .vector_clock_keys = vector_clock_keys,
            .vector_clock_refs = vector_clock_refs,
            .vector_clock_hand = 0,
            .metadata_cache = .empty,
            .metadata_cache_slots = .empty,
            .metadata_clock_keys = metadata_clock_keys,
            .metadata_clock_refs = metadata_clock_refs,
            .metadata_clock_hand = 0,
            .resource_manager = null,
            .bind_shared_cache_resource_manager = true,
            .shared_cache = null,
            .shared_cache_registered = false,
            .cache_namespace = cache_identity.namespace,
            .cache_path = cache_identity.stable_path,
            .cache_enabled = true,
            // Cache policy is governed by ResourceManager byte admission and
            // pressure actions. There is deliberately no environment or
            // per-index switch for retained-vector caching.
            .retained_vector_cache_enabled = true,
            .cache_mu = .{},
            .active_searches = .init(0),
            .hbc_cache_bytes_accounted = 0,
            .search_workspace_bytes_accounted = 0,
            .routing_scratch_bytes_accounted = 0,
            .apply_workspace_bytes_accounted = 0,
            .apply_workspace_split_bytes = 0,
            .deferred_node_key_value_bytes = 0,
            .deferred_oversized_leaves_peak = 0,
            .bulk_split_vector_workspace = .{},
            .hbc_cache_kind_stats = .{HbcCacheKindStats{}} ** hbc_cache_kind_count,
            .deferred_quantized_nodes = .empty,
            .deferred_node_keys = .empty,
            .deferred_oversized_leaves = .empty,
            .write_session_depth = 0,
            .write_session_kind = null,
            .bulk_publication_may_have_mutated = false,
            .hilbert = null,
            .scratch_mu = .unlocked,
            .cached_scratch = null,
            .routing_scratch_mu = .unlocked,
            .cached_routing_scratch = null,
            .flat_centroid_mu = .unlocked,
            .flat_centroid_directory = null,
        };
        return idx;
    }

    pub fn beginPublishedSearchStateRefresh(self: *HBCIndex) void {
        _ = self.published_generation.fetchAdd(1, .acq_rel);
        vectorindex_spfresh_index.clearFlatCentroidDirectory(self);
    }

    pub fn finishPublishedSearchStateRefresh(self: *HBCIndex) void {
        self.published_root_node.store(self.metadata.root_node, .release);
        self.published_active_count.store(self.metadata.active_count, .release);
        self.published_node_count.store(self.metadata.node_count, .release);
        _ = self.published_generation.fetchAdd(1, .acq_rel);
    }

    pub fn abortPublishedSearchStateRefresh(self: *HBCIndex) void {
        _ = self.published_generation.fetchAdd(1, .acq_rel);
    }

    pub fn refreshPublishedSearchState(self: *HBCIndex) void {
        self.beginPublishedSearchStateRefresh();
        self.finishPublishedSearchStateRefresh();
    }

    pub fn shouldPublishSearchStateAfterWrite(self: *const HBCIndex) bool {
        return self.shouldPublishSearchStatePerBatch();
    }

    pub fn publishedRootNode(self: *const HBCIndex) u64 {
        return self.published_root_node.load(.acquire);
    }

    pub fn publishedActiveCount(self: *const HBCIndex) u64 {
        return self.published_active_count.load(.acquire);
    }

    pub fn publishedNodeCount(self: *const HBCIndex) u64 {
        return self.published_node_count.load(.acquire);
    }

    pub fn publishedGeneration(self: *const HBCIndex) u64 {
        return self.published_generation.load(.acquire);
    }

    pub fn attachResourceManager(self: *HBCIndex, resource_manager: *resource_manager_mod.ResourceManager) void {
        self.attachResourceManagerWithSharedCacheBinding(resource_manager, true);
    }

    pub fn attachResourceManagerWithSharedCacheBinding(
        self: *HBCIndex,
        resource_manager: *resource_manager_mod.ResourceManager,
        bind_shared_cache_resource_manager: bool,
    ) void {
        self.resource_manager = resource_manager;
        self.bind_shared_cache_resource_manager = bind_shared_cache_resource_manager;
        const current_search_bytes = self.search_workspace_bytes_accounted;
        self.search_workspace_bytes_accounted = 0;
        resource_manager.observeUsage(.dense_search_working_set, &self.search_workspace_bytes_accounted, current_search_bytes);
        const current_routing_bytes = self.routing_scratch_bytes_accounted;
        self.routing_scratch_bytes_accounted = 0;
        resource_manager.observeUsage(.dense_routing_working_set, &self.routing_scratch_bytes_accounted, current_routing_bytes);
        const current_apply_bytes = self.currentApplyWorkspaceBytes();
        self.apply_workspace_bytes_accounted = 0;
        resource_manager.observeUsage(.dense_apply_working_set, &self.apply_workspace_bytes_accounted, current_apply_bytes);
        if (self.shared_cache) |cache| {
            if (bind_shared_cache_resource_manager) cache.attachResourceManager(resource_manager);
            return;
        }
        self.refreshAndEnforceHbcCacheUsage(.none());
    }

    pub fn attachSharedCache(self: *HBCIndex, cache: *Cache) void {
        if (self.shared_cache) |current| {
            if (current == cache and self.shared_cache_registered) return;
            if (self.shared_cache_registered) current.unregisterNamespacePath(self.cache_namespace, self.cache_path);
            self.shared_cache = null;
            self.shared_cache_registered = false;
        }
        self.clearNodeCache();
        self.clearQuantizedCache();
        self.clearVectorCache();
        self.clearMetadataCache();
        if (!cache.registerNamespacePath(self.cache_namespace, self.cache_path)) return;
        self.shared_cache = cache;
        self.shared_cache_registered = true;
        if (self.bind_shared_cache_resource_manager) {
            if (self.resource_manager) |manager| cache.attachResourceManager(manager);
        }
    }

    /// Test-only fault/cache-coherency control. Production cache policy is
    /// owned by ResourceManager.
    fn setCacheEnabled(self: *HBCIndex, enabled: bool) void {
        if (self.cache_enabled == enabled) return;
        self.cache_enabled = enabled;
        if (!enabled) {
            self.clearNodeCache();
            self.clearQuantizedCache();
            self.clearVectorCache();
            self.clearMetadataCache();
        }
    }

    /// Test-only cache-path control. Production cache admission and eviction
    /// are owned by ResourceManager.
    fn setRetainedVectorCacheEnabled(self: *HBCIndex, enabled: bool) void {
        if (self.retained_vector_cache_enabled == enabled) return;
        self.retained_vector_cache_enabled = enabled;
        if (!enabled) self.clearVectorCache();
    }

    pub fn setBypassExternalVectorCache(self: *HBCIndex, enabled: bool) void {
        self.bypass_external_vector_cache = enabled;
    }

    pub fn acquireRoutingScratch(self: *HBCIndex) !RoutingScratchHandle {
        lockAtomic(&self.routing_scratch_mu);
        defer self.routing_scratch_mu.unlock();
        if (self.cached_routing_scratch) |scratch| {
            self.cached_routing_scratch = null;
            return .{ .scratch = scratch, .from_cache = true };
        }
        const scratch = try RoutingScratch.init(self.alloc, self.config.dims, self.config.branching_factor);
        self.observeRoutingScratchBytes(self.routing_scratch_bytes_accounted + scratch.bytes());
        return .{
            .scratch = scratch,
            .from_cache = false,
        };
    }

    pub fn releaseRoutingScratch(self: *HBCIndex, handle: *RoutingScratchHandle) void {
        lockAtomic(&self.routing_scratch_mu);
        defer self.routing_scratch_mu.unlock();
        if (self.cached_routing_scratch == null) {
            self.cached_routing_scratch = handle.scratch;
        } else {
            var scratch = handle.scratch;
            self.observeRoutingScratchBytes(self.routing_scratch_bytes_accounted -| scratch.bytes());
            scratch.deinit(self.alloc);
        }
    }

    fn observeRoutingScratchBytes(self: *HBCIndex, next: u64) void {
        if (self.resource_manager) |manager| {
            manager.observeUsage(.dense_routing_working_set, &self.routing_scratch_bytes_accounted, next);
        } else {
            self.routing_scratch_bytes_accounted = next;
        }
    }

    pub fn observeSearchWorkspaceBytes(self: *HBCIndex, next: u64) void {
        if (self.resource_manager) |manager| {
            manager.observeUsage(.dense_search_working_set, &self.search_workspace_bytes_accounted, next);
        } else {
            self.search_workspace_bytes_accounted = next;
        }
    }

    fn currentApplyWorkspaceBytes(self: *const HBCIndex) u64 {
        const staged_node_key_bytes = @as(u64, @intCast(self.deferred_node_keys.count())) *
            @as(u64, @intCast(@sizeOf(u128) + @sizeOf(DeferredNodeValue)));
        return self.apply_workspace_split_bytes +
            @as(u64, @intCast(self.deferred_oversized_leaves.count() * @sizeOf(u64))) +
            self.deferred_node_key_value_bytes +
            staged_node_key_bytes;
    }

    fn observeApplyWorkspaceBytes(self: *HBCIndex) void {
        const next = self.currentApplyWorkspaceBytes();
        if (self.resource_manager) |manager| {
            manager.observeUsage(.dense_apply_working_set, &self.apply_workspace_bytes_accounted, next);
        } else {
            self.apply_workspace_bytes_accounted = next;
        }
    }

    fn maybeObserveApplyWorkspaceBytes(self: *HBCIndex) void {
        const next = self.currentApplyWorkspaceBytes();
        const current = self.apply_workspace_bytes_accounted;
        const delta = if (next >= current) next - current else current - next;
        if (delta >= 1024 * 1024 or (self.deferred_node_keys.count() & 1023) == 0) {
            self.observeApplyWorkspaceBytes();
        }
    }

    pub fn addApplyWorkspaceBytes(self: *HBCIndex, bytes: u64) void {
        if (bytes == 0) return;
        self.apply_workspace_split_bytes +|= bytes;
        self.observeApplyWorkspaceBytes();
    }

    pub fn releaseApplyWorkspaceBytes(self: *HBCIndex, bytes: u64) void {
        if (bytes == 0) return;
        self.apply_workspace_split_bytes -|= bytes;
        self.observeApplyWorkspaceBytes();
    }

    fn observeBulkSplitVectorWorkspaceBytes(self: *HBCIndex) void {
        const next = self.bulk_split_vector_workspace.bytes();
        const current = self.bulk_split_vector_workspace.accounted_bytes;
        if (next > current) {
            self.apply_workspace_split_bytes +|= next - current;
        } else if (current > next) {
            self.apply_workspace_split_bytes -|= current - next;
        }
        self.bulk_split_vector_workspace.accounted_bytes = next;
        self.observeApplyWorkspaceBytes();
    }

    fn beginBulkSplitVectorWorkspace(self: *HBCIndex) void {
        self.deinitBulkSplitVectorWorkspace();
        self.bulk_split_vector_workspace.active = true;
    }

    fn beginStreamingSplitVectorWorkspace(self: *HBCIndex) void {
        // Keep the bounded delta cache across replay transactions. The durable
        // leaf payload is the immutable base; recent transformed inserts are
        // its in-memory delta until a split or payload refresh absorbs them.
        self.bulk_split_vector_workspace.active = true;
    }

    fn endStreamingSplitVectorWorkspace(self: *HBCIndex) void {
        self.bulk_split_vector_workspace.active = false;
    }

    fn endBulkSplitVectorWorkspace(self: *HBCIndex) void {
        self.bulk_split_vector_workspace.active = false;
        self.deinitBulkSplitVectorWorkspace();
    }

    fn deinitBulkSplitVectorWorkspace(self: *HBCIndex) void {
        if (self.bulk_split_vector_workspace.accounted_bytes != 0) {
            self.apply_workspace_split_bytes -|= self.bulk_split_vector_workspace.accounted_bytes;
            self.bulk_split_vector_workspace.accounted_bytes = 0;
            self.observeApplyWorkspaceBytes();
        }
        self.bulk_split_vector_workspace.deinit(self.alloc);
    }

    fn bulkSplitVectorWorkspaceBudgetBytes(self: *const HBCIndex) u64 {
        _ = self;
        return default_bulk_split_vector_workspace_budget_bytes;
    }

    fn bulkSplitVectorWorkspaceMaxEntries(self: *const HBCIndex) usize {
        const bytes_per_entry = @as(u64, @intCast(self.config.dims)) * @sizeOf(f32) +
            @sizeOf(u64) + @sizeOf(usize) * 2;
        // Leave headroom for hash-table and ArrayList capacity growth so the
        // resource-accounted allocation remains below the advertised bound.
        const bounded: usize = @intCast(@max(1, (self.bulkSplitVectorWorkspaceBudgetBytes() / 2) / bytes_per_entry));
        return if (builtin.is_test) @min(bounded, 8) else bounded;
    }

    fn bulkSplitVectorWorkspaceRebuildInterval(workspace: *const SplitVectorWorkspace) usize {
        return @max(@as(usize, 1), @min(@as(usize, 4096), workspace.ids.items.len / 4));
    }

    fn rebuildBulkSplitVectorWorkspaceMap(self: *HBCIndex) void {
        var workspace = &self.bulk_split_vector_workspace;
        workspace.map.clearRetainingCapacity();
        for (workspace.ids.items, 0..) |vector_id, slot| {
            workspace.map.putAssumeCapacity(vector_id, slot * self.config.dims);
        }
        workspace.replacements_since_rebuild = 0;
    }

    fn prepareBulkSplitVectorWorkspaceReplacement(self: *HBCIndex) bool {
        var workspace = &self.bulk_split_vector_workspace;
        if (workspace.replacements_since_rebuild != 0) return true;
        const rebuild_interval = bulkSplitVectorWorkspaceRebuildInterval(workspace);
        // HashMap removals leave tombstones and do not restore its insertion
        // allowance. Reserve one bounded cleanup window up front, then rebuild
        // in place before that allowance can be exhausted. Constant-cardinality
        // streaming ingest therefore retains constant hash-table capacity.
        workspace.map.ensureUnusedCapacity(self.alloc, @intCast(rebuild_interval)) catch return false;
        self.observeBulkSplitVectorWorkspaceBytes();
        return true;
    }

    fn installBulkSplitVectorWorkspaceSlot(
        self: *HBCIndex,
        slot: usize,
        vector_id: u64,
        transformed: []const f32,
    ) void {
        var workspace = &self.bulk_split_vector_workspace;
        const offset = slot * self.config.dims;
        std.debug.assert(offset + self.config.dims <= workspace.vectors.items.len);
        @memcpy(workspace.vectors.items[offset .. offset + self.config.dims], transformed);
        workspace.ids.items[slot] = vector_id;
        workspace.replacements_since_rebuild += 1;
        if (workspace.replacements_since_rebuild >= bulkSplitVectorWorkspaceRebuildInterval(workspace)) {
            self.rebuildBulkSplitVectorWorkspaceMap();
        } else {
            workspace.map.putAssumeCapacity(vector_id, offset);
        }
    }

    pub fn bulkSplitVectorWorkspaceContains(self: *const HBCIndex, vector_id: u64) bool {
        return self.bulk_split_vector_workspace.active and
            self.bulk_split_vector_workspace.map.contains(vector_id);
    }

    pub fn bulkSplitVectorWorkspaceLookup(self: *HBCIndex, vector_id: u64, out: []f32) bool {
        const workspace = &self.bulk_split_vector_workspace;
        if (!workspace.active) return false;
        if (out.len != self.config.dims) return false;
        const offset = workspace.map.get(vector_id) orelse return false;
        if (offset + self.config.dims > workspace.vectors.items.len) return false;
        @memcpy(out, workspace.vectors.items[offset .. offset + self.config.dims]);
        return true;
    }

    fn bulkSplitVectorWorkspaceAdmit(self: *HBCIndex, vector_id: u64, transformed: []const f32) void {
        var workspace = &self.bulk_split_vector_workspace;
        if (!workspace.active) return;
        if (transformed.len != self.config.dims) return;
        if (workspace.map.get(vector_id)) |offset| {
            if (offset + self.config.dims <= workspace.vectors.items.len) {
                @memcpy(workspace.vectors.items[offset .. offset + self.config.dims], transformed);
            }
            return;
        }

        if (workspace.ids.items.len >= self.bulkSplitVectorWorkspaceMaxEntries()) {
            const slot = workspace.next_victim % workspace.ids.items.len;
            const old_id = workspace.ids.items[slot];
            const offset = slot * self.config.dims;
            if (!self.prepareBulkSplitVectorWorkspaceReplacement()) return;
            _ = workspace.map.remove(old_id);
            if (offset + self.config.dims > workspace.vectors.items.len) return;
            self.installBulkSplitVectorWorkspaceSlot(slot, vector_id, transformed);
            workspace.next_victim = (slot + 1) % workspace.ids.items.len;
            return;
        }

        const offset = workspace.vectors.items.len;
        workspace.vectors.appendSlice(self.alloc, transformed) catch return;
        workspace.ids.append(self.alloc, vector_id) catch {
            workspace.vectors.items.len = offset;
            return;
        };
        workspace.map.put(self.alloc, vector_id, offset) catch {
            workspace.vectors.items.len = offset;
            workspace.ids.items.len -= 1;
            return;
        };
        self.observeBulkSplitVectorWorkspaceBytes();
    }

    pub fn noteBulkSplitLeafPayloadCoverage(self: *HBCIndex, member_ids: []const u64) void {
        // A later structural mutation can consume exact vectors across the
        // just-written leaf boundary, so payload publication is not yet a safe
        // retirement boundary. Keep the bounded rolling entries until normal
        // ring eviction; eager removal both lost that proof and accumulated
        // hash-table tombstones under sustained ingest.
        _ = self;
        _ = member_ids;
    }

    fn admitBatchVectorsToBulkSplitWorkspace(
        self: *HBCIndex,
        items: []const BatchInsertItem,
        assume_absent_ids: bool,
    ) void {
        if (!self.bulk_split_vector_workspace.active or items.len == 0) return;
        if (!assume_absent_ids) {
            self.bulk_split_vector_workspace.clearRetainingCapacity();
            self.observeBulkSplitVectorWorkspaceBytes();
            return;
        }
        const transformed = self.alloc.alloc(f32, self.config.dims) catch return;
        defer self.alloc.free(transformed);
        for (items) |item| {
            if (item.vector.len != self.config.dims) continue;
            _ = self.transformVector(item.vector, transformed);
            self.bulkSplitVectorWorkspaceAdmit(item.vector_id, transformed);
        }
    }

    pub fn setExternalVectorLoader(self: *HBCIndex, ctx: *anyopaque, loader: ExternalVectorLoader) void {
        self.external_vector_ctx = ctx;
        self.external_vector_loader = loader;
    }

    pub fn setExternalVectorScratchLoader(self: *HBCIndex, ctx: *anyopaque, loader: ExternalVectorScratchLoader) void {
        self.external_vector_ctx = ctx;
        self.external_vector_scratch_loader = loader;
    }

    pub fn setExternalVectorBatchScratchLoader(self: *HBCIndex, ctx: *anyopaque, loader: ExternalVectorBatchScratchLoader) void {
        self.external_vector_ctx = ctx;
        self.external_vector_batch_scratch_loader = loader;
    }

    pub fn setExternalVectorBatchTransformedMatrixLoader(self: *HBCIndex, ctx: *anyopaque, loader: ExternalVectorBatchTransformedMatrixLoader) void {
        self.external_vector_ctx = ctx;
        self.external_vector_batch_transformed_matrix_loader = loader;
    }

    pub fn setExternalVectorBatchDistanceLoader(self: *HBCIndex, ctx: *anyopaque, loader: ExternalVectorBatchDistanceLoader) void {
        self.external_vector_ctx = ctx;
        self.external_vector_batch_distance_loader = loader;
    }

    pub fn hasExternalVectorLoader(self: *const HBCIndex) bool {
        return self.external_vector_ctx != null and
            (self.external_vector_loader != null or self.external_vector_scratch_loader != null or self.external_vector_batch_scratch_loader != null or self.external_vector_batch_transformed_matrix_loader != null or self.external_vector_batch_distance_loader != null);
    }

    pub fn refreshHbcCacheUsage(self: *HBCIndex) void {
        if (self.shared_cache != null) return;
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.hbc_node_metadata_cache, &self.hbc_cache_bytes_accounted, self.refreshHbcCacheKindBytes());
    }

    fn hbcCacheBytes(self: *const HBCIndex) u64 {
        var total: u64 = 0;
        var node_it = self.node_cache.iterator();
        while (node_it.next()) |entry| total +|= estimateNodeCacheBytes(&entry.value_ptr.*.node);
        var quantized_it = self.quantized_cache.iterator();
        while (quantized_it.next()) |entry| total +|= estimateQuantizedCacheBytes(&entry.value_ptr.*.quantized);
        var vector_it = self.vector_cache.iterator();
        while (vector_it.next()) |entry| total +|= estimateVectorCacheBytes(entry.value_ptr.*.vector);
        var metadata_it = self.metadata_cache.iterator();
        while (metadata_it.next()) |entry| total +|= estimateMetadataCacheBytes(entry.value_ptr.*.metadata);
        return total;
    }

    fn refreshHbcCacheKindBytes(self: *HBCIndex) u64 {
        var bytes: [hbc_cache_kind_count]u64 = .{0} ** hbc_cache_kind_count;
        var node_it = self.node_cache.iterator();
        while (node_it.next()) |entry| bytes[@intFromEnum(HbcCacheKind.node)] +|= estimateNodeCacheBytes(&entry.value_ptr.*.node);
        var quantized_it = self.quantized_cache.iterator();
        while (quantized_it.next()) |entry| bytes[@intFromEnum(HbcCacheKind.quantized)] +|= estimateQuantizedCacheBytes(&entry.value_ptr.*.quantized);
        var vector_it = self.vector_cache.iterator();
        while (vector_it.next()) |entry| bytes[@intFromEnum(HbcCacheKind.vector)] +|= estimateVectorCacheBytes(entry.value_ptr.*.vector);
        var metadata_it = self.metadata_cache.iterator();
        while (metadata_it.next()) |entry| bytes[@intFromEnum(HbcCacheKind.metadata)] +|= estimateMetadataCacheBytes(entry.value_ptr.*.metadata);

        var total: u64 = 0;
        for (bytes, 0..) |used_bytes, i| {
            total +|= used_bytes;
            self.hbc_cache_kind_stats[i].used_bytes = used_bytes;
            self.hbc_cache_kind_stats[i].peak_bytes = @max(self.hbc_cache_kind_stats[i].peak_bytes, used_bytes);
        }
        return total;
    }

    fn hbcCacheEntryBytes(self: *const HBCIndex, kind: HbcCacheKind, key: u64) u64 {
        return switch (kind) {
            .node => if (self.node_cache.get(key)) |entry| estimateNodeCacheBytes(&entry.node) else 0,
            .quantized => if (self.quantized_cache.get(key)) |entry| estimateQuantizedCacheBytes(&entry.quantized) else 0,
            .vector => if (self.vector_cache.get(key)) |entry| estimateVectorCacheBytes(entry.vector) else 0,
            .metadata => if (self.metadata_cache.get(key)) |entry| estimateMetadataCacheBytes(entry.metadata) else 0,
        };
    }

    fn noteHbcCacheInsertion(self: *HBCIndex, kind: HbcCacheKind) void {
        self.hbc_cache_kind_stats[@intFromEnum(kind)].insertions += 1;
    }

    fn noteHbcCacheAdmissionSkip(self: *HBCIndex, kind: HbcCacheKind) void {
        self.hbc_cache_kind_stats[@intFromEnum(kind)].admission_skips += 1;
    }

    fn noteHbcCacheEviction(self: *HBCIndex, kind: HbcCacheKind) void {
        self.hbc_cache_kind_stats[@intFromEnum(kind)].evictions += 1;
    }

    pub fn hbcCacheStats(self: *HBCIndex) HbcCacheStats {
        if (self.shared_cache) |cache| return cache.namespaceStats(self.cache_namespace);
        const total_bytes = self.refreshHbcCacheKindBytes();
        return .{
            .total_bytes = total_bytes,
            .accounted_bytes = self.hbc_cache_bytes_accounted,
            .node = self.hbc_cache_kind_stats[@intFromEnum(HbcCacheKind.node)],
            .quantized = self.hbc_cache_kind_stats[@intFromEnum(HbcCacheKind.quantized)],
            .vector = self.hbc_cache_kind_stats[@intFromEnum(HbcCacheKind.vector)],
            .metadata = self.hbc_cache_kind_stats[@intFromEnum(HbcCacheKind.metadata)],
        };
    }

    pub fn clearAllCaches(self: *HBCIndex) void {
        self.clearNodeCache();
        self.clearQuantizedCache();
        self.clearVectorCache();
        self.clearMetadataCache();
    }

    fn reserveHbcCacheDelta(self: *HBCIndex, admission: *HbcCacheAdmission, delta_bytes: u64) bool {
        if (delta_bytes == 0) return true;
        const manager = self.resource_manager orelse return true;
        var reservation = manager.reserve(.hbc_node_metadata_cache, delta_bytes) catch return false;
        reservation.released = true;
        self.hbc_cache_bytes_accounted +|= delta_bytes;
        admission.* = .{
            .index = self,
            .reserved_bytes = delta_bytes,
            .active = true,
        };
        return true;
    }

    fn prepareHbcCacheAdmission(self: *HBCIndex, kind: HbcCacheKind, key: u64, next_entry_bytes: u64) ?HbcCacheAdmission {
        var admission = HbcCacheAdmission.none(self);
        const manager = self.resource_manager orelse return admission;

        self.refreshHbcCacheUsage();
        const existing_bytes = self.hbcCacheEntryBytes(kind, key);
        if (next_entry_bytes <= existing_bytes) return admission;

        const delta_bytes = next_entry_bytes - existing_bytes;
        if (self.reserveHbcCacheDelta(&admission, delta_bytes)) return admission;

        const protection = HbcCacheProtection.one(kind, key);
        while (self.evictOneHbcCacheEntry(protection)) {
            manager.observeUsage(.hbc_node_metadata_cache, &self.hbc_cache_bytes_accounted, self.hbcCacheBytes());
            if (self.reserveHbcCacheDelta(&admission, delta_bytes)) return admission;
        }

        return null;
    }

    fn refreshAndEnforceHbcCacheUsage(self: *HBCIndex, protection: HbcCacheProtection) void {
        if (self.shared_cache != null) return;
        self.refreshHbcCacheUsage();
        self.enforceHbcCacheBudget(protection);
    }

    fn enforceHbcCacheBudget(self: *HBCIndex, protection: HbcCacheProtection) void {
        const manager = self.resource_manager orelse return;
        const cache_stats = manager.sliceStats(.hbc_node_metadata_cache);
        const action = switch (cache_stats.pressure) {
            .normal => return,
            .soft => cache_stats.soft_action,
            .hard => cache_stats.hard_action,
        };
        if (action != .shrink_cache) return;

        const target_bytes = if (cache_stats.soft_limit_bytes > 0) cache_stats.soft_limit_bytes else cache_stats.hard_limit_bytes;
        if (target_bytes == 0) return;

        var current_bytes = self.hbc_cache_bytes_accounted;
        var evicted_any = false;
        while (current_bytes > target_bytes) {
            if (!self.evictOneHbcCacheEntry(protection)) break;
            evicted_any = true;
            current_bytes = self.hbcCacheBytes();
        }
        if (evicted_any or current_bytes != self.hbc_cache_bytes_accounted) {
            manager.observeUsage(.hbc_node_metadata_cache, &self.hbc_cache_bytes_accounted, current_bytes);
        }
    }

    fn evictOneHbcCacheEntry(self: *HBCIndex, protection: HbcCacheProtection) bool {
        if (self.evictOneVectorCacheEntry(protection)) return true;
        if (self.evictOneMetadataCacheEntry(protection)) return true;
        if (self.evictOneQuantizedCacheEntry(protection)) return true;
        return self.evictOneNodeCacheEntry(protection);
    }

    fn evictOneVectorCacheEntry(self: *HBCIndex, protection: HbcCacheProtection) bool {
        const victim = nextHbcClockVictim(self.vector_clock_keys, self.vector_clock_refs, &self.vector_clock_hand, protection, .vector) orelse return false;
        if (self.vector_cache_slots.fetchRemove(victim)) |removed_slot| {
            self.vector_clock_keys[removed_slot.value] = 0;
            self.vector_clock_refs[removed_slot.value] = false;
        }
        if (self.vector_cache.fetchRemove(victim)) |removed| {
            releaseVectorCacheEntry(self.alloc, removed.value);
            self.noteHbcCacheEviction(.vector);
            return true;
        }
        return false;
    }

    fn evictOneMetadataCacheEntry(self: *HBCIndex, protection: HbcCacheProtection) bool {
        const victim = nextHbcClockVictim(self.metadata_clock_keys, self.metadata_clock_refs, &self.metadata_clock_hand, protection, .metadata) orelse return false;
        if (self.metadata_cache_slots.fetchRemove(victim)) |removed_slot| {
            self.metadata_clock_keys[removed_slot.value] = 0;
            self.metadata_clock_refs[removed_slot.value] = false;
        }
        if (self.metadata_cache.fetchRemove(victim)) |removed| {
            releaseMetadataCacheEntry(self.alloc, removed.value);
            self.noteHbcCacheEviction(.metadata);
            return true;
        }
        return false;
    }

    fn evictOneQuantizedCacheEntry(self: *HBCIndex, protection: HbcCacheProtection) bool {
        const victim = nextHbcClockVictim(self.quantized_clock_keys, self.quantized_clock_refs, &self.quantized_clock_hand, protection, .quantized) orelse return false;
        if (self.quantized_cache_slots.fetchRemove(victim)) |removed_slot| {
            self.quantized_clock_keys[removed_slot.value] = 0;
            self.quantized_clock_refs[removed_slot.value] = false;
        }
        if (self.quantized_cache.fetchRemove(victim)) |removed| {
            releaseQuantizedCacheEntry(self.alloc, removed.value);
            self.noteHbcCacheEviction(.quantized);
            return true;
        }
        return false;
    }

    fn evictOneNodeCacheEntry(self: *HBCIndex, protection: HbcCacheProtection) bool {
        const victim = nextHbcClockVictim(self.node_clock_keys, self.node_clock_refs, &self.node_clock_hand, protection, .node) orelse return false;
        if (self.node_cache_slots.fetchRemove(victim)) |removed_slot| {
            self.node_clock_keys[removed_slot.value] = 0;
            self.node_clock_refs[removed_slot.value] = false;
        }
        if (self.node_cache.fetchRemove(victim)) |removed| {
            releaseNodeCacheEntry(self.alloc, removed.value);
            self.noteHbcCacheEviction(.node);
            return true;
        }
        return false;
    }

    pub fn close(self: *HBCIndex) void {
        self.deinitWithBackendDisposition(false);
    }

    /// Test and fault-injection teardown that discards unsynced backend state
    /// instead of performing a graceful close. Production recovery code does
    /// not call this; it exists to exercise the exact interrupted-session
    /// boundary with modeled and WAL-backed storage.
    pub fn abandonAfterCrash(self: *HBCIndex) void {
        self.deinitWithBackendDisposition(true);
    }

    fn deinitWithBackendDisposition(self: *HBCIndex, abandon_after_crash: bool) void {
        if (!abandon_after_crash and self.experimental_posting_checkpoint_build != null) {
            self.experimental_posting_checkpoint_build.?.force_progress.store(true, .release);
            if (self.experimental_posting_checkpoint_build.?.thread) |thread| {
                thread.join();
                self.experimental_posting_checkpoint_build.?.thread = null;
            }
            if (self.experimental_posting_write_store) |*posting_store| {
                _ = self.publishCompletedExperimentalPostingCheckpointBuild(posting_store) catch |err| {
                    // The committed WAL remains authoritative. Graceful close
                    // may leave compaction debt, but never converts optional
                    // cleanup failure into loss of an acknowledged mutation.
                    std.log.warn("dense posting idle checkpoint publication deferred during close err={s}", .{@errorName(err)});
                };
            }
        }
        self.discardExperimentalPostingCheckpointBuild();
        self.clearExperimentalPostingMutationBase();
        self.experimental_posting_reads_enabled.store(false, .release);
        self.clearExperimentalPostingReadGeneration();
        if (self.experimental_posting_delta_base_state) |state| {
            state.deinit();
            self.alloc.destroy(state);
            self.experimental_posting_delta_base_state = null;
        }
        if (self.experimental_posting_write_store) |*store| {
            store.deinit();
            self.experimental_posting_write_store = null;
        }
        self.clearExperimentalPostingCapturedValues();
        self.experimental_posting_captured_values.deinit(self.alloc);
        if (self.shared_cache == null) {
            self.clearNodeCache();
            self.clearQuantizedCache();
            self.clearVectorCache();
            self.clearMetadataCache();
        } else {
            self.cache_mu.lockExclusive();
            self.clearLocalNodeCacheLocked();
            self.clearLocalQuantizedCacheLocked();
            self.cache_mu.unlockExclusive();
        }
        if (self.shared_cache) |cache| {
            if (self.shared_cache_registered) cache.unregisterNamespacePath(self.cache_namespace, self.cache_path);
            self.shared_cache = null;
            self.shared_cache_registered = false;
        }
        self.alloc.free(self.node_clock_keys);
        self.alloc.free(self.node_clock_refs);
        self.alloc.free(self.quantized_clock_keys);
        self.alloc.free(self.quantized_clock_refs);
        self.alloc.free(self.vector_clock_keys);
        self.alloc.free(self.vector_clock_refs);
        self.alloc.free(self.metadata_clock_keys);
        self.alloc.free(self.metadata_clock_refs);
        if (self.hilbert) |*hilbert| hilbert.deinit();
        if (self.cached_scratch) |*scratch| {
            self.observeSearchWorkspaceBytes(self.search_workspace_bytes_accounted -| scratch.bytes());
            scratch.deinit(self.alloc);
        }
        if (self.cached_routing_scratch) |*scratch| {
            self.observeRoutingScratchBytes(self.routing_scratch_bytes_accounted -| scratch.bytes());
            scratch.deinit(self.alloc);
        }
        vectorindex_spfresh_index.clearFlatCentroidDirectory(self);
        self.deinitBulkSplitVectorWorkspace();
        self.deferred_oversized_leaves.clearRetainingCapacity();
        self.apply_workspace_split_bytes = self.bulk_split_vector_workspace.accounted_bytes;
        self.deferred_node_key_value_bytes = 0;
        self.observeApplyWorkspaceBytes();
        self.deferred_quantized_nodes.deinit(self.alloc);
        self.clearDeferredNodeKeys();
        self.deferred_node_keys.deinit(self.alloc);
        self.deferred_oversized_leaves.deinit(self.alloc);
        self.rot.deinit();
        self.quantizer.deinit();
        self.alloc.free(self.cache_path);
        self.store.deinit();
        if (abandon_after_crash) {
            self.env_owner.abandonAfterCrash(self.alloc);
        } else {
            self.env_owner.close(self.alloc);
        }
        self.* = undefined;
    }

    pub fn sync(self: *HBCIndex, force: bool) !void {
        try self.env_owner.sync(force);
    }

    pub fn syncReplayState(self: *HBCIndex) !void {
        try self.env_owner.syncReplayState();
    }

    fn disableExperimentalPostingReads(self: *HBCIndex) void {
        self.experimental_posting_reads_enabled.store(false, .release);
    }

    fn clearExperimentalPostingReadGeneration(self: *HBCIndex) void {
        lockAtomic(&self.experimental_posting_read_generation_mu);
        const old = self.experimental_posting_read_generation;
        self.experimental_posting_read_generation = null;
        self.experimental_posting_read_generation_mu.unlock();
        if (old) |generation| generation.release();
    }

    fn installExperimentalPostingReadGeneration(
        self: *HBCIndex,
        generation: *ExperimentalPostingReadGeneration,
    ) void {
        lockAtomic(&self.experimental_posting_read_generation_mu);
        const old = self.experimental_posting_read_generation;
        self.experimental_posting_read_generation = generation;
        self.experimental_posting_reads_enabled.store(true, .release);
        self.experimental_posting_read_generation_mu.unlock();
        if (old) |previous| previous.release();
    }

    fn acquireExperimentalPostingReadGeneration(self: *HBCIndex) ?*ExperimentalPostingReadGeneration {
        if (!self.experimental_posting_reads_enabled.load(.acquire)) return null;
        lockAtomic(&self.experimental_posting_read_generation_mu);
        defer self.experimental_posting_read_generation_mu.unlock();
        if (!self.experimental_posting_reads_enabled.load(.acquire)) return null;
        const generation = self.experimental_posting_read_generation orelse return null;
        generation.retain();
        return generation;
    }

    fn experimentalPostingReadGenerationIsCurrent(
        self: *HBCIndex,
        generation: *ExperimentalPostingReadGeneration,
    ) bool {
        lockAtomic(&self.experimental_posting_read_generation_mu);
        defer self.experimental_posting_read_generation_mu.unlock();
        return self.experimental_posting_reads_enabled.load(.acquire) and
            self.experimental_posting_read_generation == generation;
    }

    fn experimentalPostingReadGenerationLoaded(self: *HBCIndex) bool {
        lockAtomic(&self.experimental_posting_read_generation_mu);
        defer self.experimental_posting_read_generation_mu.unlock();
        return self.experimental_posting_read_generation != null;
    }

    fn retainCurrentExperimentalPostingReadGeneration(self: *HBCIndex) ?*ExperimentalPostingReadGeneration {
        lockAtomic(&self.experimental_posting_read_generation_mu);
        defer self.experimental_posting_read_generation_mu.unlock();
        const generation = self.experimental_posting_read_generation orelse return null;
        generation.retain();
        return generation;
    }

    /// Returns the source boundary represented by the currently published,
    /// durably committed posting generation. In WAL-authoritative mode this is
    /// the dense index's applied sequence; projection metadata retains only
    /// lifecycle identity/status and may intentionally lag between lifecycle
    /// transitions.
    pub fn experimentalPostingDurableAppliedSequence(self: *const HBCIndex) ?u64 {
        const mutable: *HBCIndex = @constCast(self);
        const generation = mutable.retainCurrentExperimentalPostingReadGeneration() orelse return null;
        defer generation.release();
        return generation.covered_source_sequence.load(.acquire);
    }

    fn collapseExperimentalPostingReadOverlays(self: *HBCIndex) !u64 {
        if (try self.collapseExperimentalPostingReadOverlaysExclusive()) |value_count| return value_count;

        const source = self.retainCurrentExperimentalPostingReadGeneration() orelse return 0;
        defer source.release();
        var latest: ExperimentalPostingLatestValues = .empty;
        defer latest.deinit(self.alloc);
        const root = try collectExperimentalPostingOverlayValues(self.alloc, source, &latest);
        if (latest.count() == 0) return 0;

        const collapsed = try ExperimentalPostingReadGeneration.createOverlay(
            self.alloc,
            root,
            source.covered_source_sequence.load(.acquire),
        );
        errdefer collapsed.release();
        try collapsed.values.ensureTotalCapacity(self.alloc, latest.count());
        var it = latest.iterator();
        while (it.next()) |entry| {
            try collapsed.set(
                experimentalPostingValueKeyId(entry.key_ptr.*),
                try experimentalPostingValueKeyKind(entry.key_ptr.*),
                entry.value_ptr.*,
            );
        }
        const value_count: u64 = @intCast(latest.count());
        self.installExperimentalPostingReadGeneration(collapsed);
        return value_count;
    }

    /// When no query or checkpoint builder leases any generation, merge maps
    /// by transferring their owned values into the newest overlay. The read
    /// generation mutex prevents a new lease during the transfer. Concurrent
    /// readers take the copy-on-collapse path above instead.
    fn collapseExperimentalPostingReadOverlaysExclusive(self: *HBCIndex) !?u64 {
        lockAtomic(&self.experimental_posting_read_generation_mu);
        var locked = true;
        defer if (locked) self.experimental_posting_read_generation_mu.unlock();
        const newest = self.experimental_posting_read_generation orelse return 0;
        if (!self.experimental_posting_reads_enabled.load(.acquire) or newest.root != null) return 0;

        var total_values: usize = 0;
        var root: ?*ExperimentalPostingReadGeneration = null;
        var current: ?*ExperimentalPostingReadGeneration = newest;
        while (current) |generation| : (current = generation.parent) {
            if (generation.refs.load(.acquire) != 1) return null;
            total_values = std.math.add(usize, total_values, generation.values.count()) catch return error.OutOfMemory;
            if (generation.root != null) {
                root = generation;
                break;
            }
        }
        const immutable_root = root orelse return error.Corrupted;
        if (newest.parent == immutable_root) return @intCast(newest.values.count());

        var merged: std.AutoHashMapUnmanaged(u128, ?[]u8) = .empty;
        errdefer merged.deinit(self.alloc);
        try merged.ensureTotalCapacity(self.alloc, @intCast(total_values));
        current = newest;
        while (current) |generation| : (current = generation.parent) {
            if (generation == immutable_root) break;
            var values = generation.values.iterator();
            while (values.next()) |entry| {
                const result = merged.getOrPutAssumeCapacity(entry.key_ptr.*);
                if (!result.found_existing) {
                    result.value_ptr.* = entry.value_ptr.*;
                    // Ownership moved to `merged`; null prevents the old
                    // generation destructor from releasing the same slice.
                    if (entry.value_ptr.* != null) entry.value_ptr.* = null;
                }
            }
        }

        const old_parent = newest.parent.?;
        immutable_root.retain();
        newest.values.deinit(self.alloc);
        newest.values = merged;
        newest.parent = immutable_root;
        const value_count: u64 = @intCast(newest.values.count());
        self.experimental_posting_read_generation_mu.unlock();
        locked = false;
        old_parent.release();
        return value_count;
    }

    fn publishExperimentalPostingReadOverlay(
        self: *HBCIndex,
        touched_keys: []const u128,
        covered_source_sequence: u64,
    ) !void {
        const parent = self.retainCurrentExperimentalPostingReadGeneration() orelse return;
        defer parent.release();
        if (covered_source_sequence < parent.covered_source_sequence.load(.acquire)) {
            return error.PostingWalSequenceRegression;
        }
        if (touched_keys.len == 0) {
            // Coverage does not change query contents, so advance it in place
            // instead of growing an empty generation chain for source windows
            // that do not touch this dense index.
            parent.advanceCoveredSourceSequence(covered_source_sequence);
            return;
        }
        for (touched_keys) |key| {
            if (!self.experimental_posting_captured_values.contains(key)) {
                return error.MissingExperimentalPostingCaptureValue;
            }
        }

        // The installed delta generation is mutable only while it has no
        // reader or checkpoint-builder leases. Serialize acquisition with
        // publication, then update that one map in place. This is the common
        // ingest path and keeps lookups at delta + mmap base instead of
        // growing one hash-map probe per committed source batch. If a reader
        // has pinned the generation, fall through to copy-on-write below so
        // that query retains its exact pre-commit snapshot.
        lockAtomic(&self.experimental_posting_read_generation_mu);
        var generation_locked = true;
        defer if (generation_locked) self.experimental_posting_read_generation_mu.unlock();
        if (self.experimental_posting_reads_enabled.load(.acquire)) {
            if (self.experimental_posting_read_generation) |current| {
                // Two references are expected here: the installed-generation
                // owner and this function's local `parent` lease.
                if (current == parent and current.root == null and current.refs.load(.acquire) == 2) {
                    try current.values.ensureUnusedCapacity(self.alloc, @intCast(touched_keys.len));
                    for (touched_keys) |key| {
                        const captured = self.experimental_posting_captured_values.getPtr(key).?;
                        const result = current.values.getOrPutAssumeCapacity(key);
                        if (result.found_existing) if (result.value_ptr.*) |old| self.alloc.free(old);
                        result.value_ptr.* = captured.*;
                        captured.* = null;
                    }
                    current.advanceCoveredSourceSequence(covered_source_sequence);
                    return;
                }
            }
        }
        self.experimental_posting_read_generation_mu.unlock();
        generation_locked = false;

        const generation = try ExperimentalPostingReadGeneration.createOverlay(
            self.alloc,
            parent,
            covered_source_sequence,
        );
        errdefer generation.release();
        try generation.values.ensureTotalCapacity(self.alloc, @intCast(touched_keys.len));
        for (touched_keys) |key| {
            const posting_id = experimentalPostingValueKeyId(key);
            const kind = try experimentalPostingValueKeyKind(key);
            const captured = self.experimental_posting_captured_values.getPtr(key) orelse
                return error.MissingExperimentalPostingCaptureValue;
            const result = generation.values.getOrPutAssumeCapacity(experimentalPostingValueKey(posting_id, kind));
            std.debug.assert(!result.found_existing);
            // The durable append no longer borrows these bytes. Transfer the
            // transaction-local allocation into the immutable overlay instead
            // of briefly holding a second full posting copy at publication.
            result.value_ptr.* = captured.*;
            captured.* = null;
        }
        self.installExperimentalPostingReadGeneration(generation);
    }

    fn decodeExperimentalPostingMetadata(self: *const HBCIndex, encoded: []const u8) !IndexMetadata {
        if (encoded.len != IndexMetadata.encoded_size) return error.CorruptedPostingMetadata;
        const metadata = IndexMetadata.decode(encoded);
        if (metadata.version != self.metadata.version or
            metadata.dims != self.metadata.dims or
            metadata.branching_factor != self.metadata.branching_factor or
            metadata.leaf_size != self.metadata.leaf_size or
            metadata.use_quantization != self.metadata.use_quantization or
            metadata.quantizer_seed != self.metadata.quantizer_seed or
            metadata.metric != self.metadata.metric)
        {
            return error.PostingCheckpointMetadataMismatch;
        }
        return metadata;
    }

    fn metadataFromExperimentalPostingGeneration(
        self: *const HBCIndex,
        generation: *ExperimentalPostingReadGeneration,
    ) !IndexMetadata {
        const encoded = try generation.value(0, .index_metadata) orelse
            return error.MissingPostingMetadata;
        return try self.decodeExperimentalPostingMetadata(encoded);
    }

    fn installExperimentalPostingRootGeneration(
        self: *HBCIndex,
        generation: *ExperimentalPostingReadGeneration,
    ) !void {
        const metadata = try self.metadataFromExperimentalPostingGeneration(generation);
        self.beginPublishedSearchStateRefresh();
        self.metadata = metadata;
        self.installExperimentalPostingReadGeneration(generation);
        self.finishPublishedSearchStateRefresh();
    }

    fn refreshExperimentalPostingReadGeneration(
        self: *HBCIndex,
        covered_source_sequence: u64,
    ) !void {
        const state = try self.loadExperimentalPostingState(covered_source_sequence);
        var state_owned = true;
        errdefer if (state_owned) {
            state.deinit();
            self.alloc.destroy(state);
        };
        const generation = try ExperimentalPostingReadGeneration.createRoot(self.alloc, state);
        state_owned = false;
        var generation_owned = true;
        errdefer if (generation_owned) generation.release();
        try self.installExperimentalPostingRootGeneration(generation);
        generation_owned = false;
        self.experimental_posting_sidecar_managed = true;
    }

    fn refreshExperimentalPostingReadGenerationBestEffort(
        self: *HBCIndex,
        covered_source_sequence: u64,
    ) void {
        self.refreshExperimentalPostingReadGeneration(covered_source_sequence) catch |err| {
            std.log.warn("dense posting sidecar live generation refresh failed sequence={} err={s}", .{
                covered_source_sequence,
                @errorName(err),
            });
            return;
        };
    }

    fn discardExperimentalPostingDeltaBaseState(self: *HBCIndex) void {
        if (self.experimental_posting_delta_base_state) |state| {
            state.deinit();
            self.alloc.destroy(state);
            self.experimental_posting_delta_base_state = null;
        }
    }

    fn clearExperimentalPostingCapturedValues(self: *HBCIndex) void {
        var values = self.experimental_posting_captured_values.valueIterator();
        while (values.next()) |value| if (value.*) |owned| self.alloc.free(owned);
        self.experimental_posting_captured_values.clearRetainingCapacity();
    }

    fn clearExperimentalPostingMutationBase(self: *HBCIndex) void {
        if (self.experimental_posting_mutation_base_generation) |generation| {
            self.experimental_posting_mutation_base_generation = null;
            generation.release();
        }
    }

    fn restoreExperimentalPostingMutationBase(self: *HBCIndex) !void {
        const generation = self.experimental_posting_mutation_base_generation orelse return;
        self.metadata = try self.metadataFromExperimentalPostingGeneration(generation);
        self.refreshPublishedSearchState();
        self.clearAllCaches();
    }

    fn preserveExperimentalPostingWalAfterCaptureFailure(self: *HBCIndex) void {
        self.closeExperimentalPostingWalWriter();
        self.restoreExperimentalPostingMutationBase() catch |err| {
            std.log.err("dense posting WAL could not restore the last committed mutation base err={s}", .{@errorName(err)});
            self.disableExperimentalPostingReads();
        };
    }

    fn openExperimentalPostingStore(self: *HBCIndex) !posting_segment_store_mod.Store {
        const location: struct { storage: lsm_backend.Storage, root_dir: []const u8 } = switch (self.env_owner) {
            .lsm => |handle| .{
                .storage = handle.backend.storage orelse return error.MissingStorageBackend,
                .root_dir = handle.backend.root_dir orelse return error.MissingStorageRoot,
            },
            .native => |backend| .{ .storage = backend.storage, .root_dir = backend.root_dir },
            .lmdb => return error.UnsupportedStorageBackend,
        };
        const posting_root = try std.fs.path.join(self.alloc, &.{ location.root_dir, "posting-segments" });
        defer self.alloc.free(posting_root);
        return try posting_segment_store_mod.Store.open(self.alloc, location.storage, posting_root);
    }

    fn openExperimentalPostingStoreWithSegment(self: *HBCIndex) !posting_segment_store_mod.OpenedWithSegment {
        const location: struct { storage: lsm_backend.Storage, root_dir: []const u8 } = switch (self.env_owner) {
            .lsm => |handle| .{
                .storage = handle.backend.storage orelse return error.MissingStorageBackend,
                .root_dir = handle.backend.root_dir orelse return error.MissingStorageRoot,
            },
            .native => |backend| .{ .storage = backend.storage, .root_dir = backend.root_dir },
            .lmdb => return error.UnsupportedStorageBackend,
        };
        const posting_root = try std.fs.path.join(self.alloc, &.{ location.root_dir, "posting-segments" });
        defer self.alloc.free(posting_root);
        return try posting_segment_store_mod.Store.openWithSegmentAlloc(self.alloc, location.storage, posting_root);
    }

    fn discardExperimentalPostingCheckpointBuild(self: *HBCIndex) void {
        const build = self.experimental_posting_checkpoint_build orelse return;
        self.experimental_posting_checkpoint_build = null;
        build.deinit();
    }

    fn startExperimentalPostingCheckpointBuild(
        self: *HBCIndex,
        posting_store: *const posting_segment_store_mod.Store,
    ) !bool {
        if (self.experimental_posting_checkpoint_build != null) return false;
        const checkpoint = posting_store.checkpoint orelse return false;
        const source_generation = self.retainCurrentExperimentalPostingReadGeneration() orelse return false;
        const build_alloc = ExperimentalPostingCheckpointBuild.allocator();
        const owned_root = build_alloc.dupe(u8, posting_store.root_dir) catch |err| {
            source_generation.release();
            return err;
        };
        var staging_store = posting_store.*;
        staging_store.alloc = build_alloc;
        staging_store.root_dir = owned_root;
        const build = self.alloc.create(ExperimentalPostingCheckpointBuild) catch |err| {
            staging_store.deinit();
            source_generation.release();
            return err;
        };
        const segment_generation = std.math.add(u64, checkpoint.segment_generation, 1) catch {
            self.alloc.destroy(build);
            staging_store.deinit();
            source_generation.release();
            return error.PostingSegmentGenerationOverflow;
        };
        build.* = .{
            .owner_alloc = self.alloc,
            .source_generation = source_generation,
            .metadata = self.metadata,
            .segment_generation = segment_generation,
            .covered_source_sequence = posting_store.covered_source_sequence,
            .flattened_wal_bytes = posting_store.wal_committed_bytes,
            .wal_generation = posting_store.wal_generation,
            .staging_store = staging_store,
            .resource_manager = self.resource_manager,
        };
        build.thread = std.Thread.spawn(.{}, ExperimentalPostingCheckpointBuild.run, .{build}) catch |err| {
            build.deinit();
            return err;
        };
        self.experimental_posting_checkpoint_build = build;
        std.log.info("dense posting checkpoint build started generation={} sequence={} wal_prefix_bytes={}", .{
            build.segment_generation,
            build.covered_source_sequence,
            build.flattened_wal_bytes,
        });
        return true;
    }

    fn publishCompletedExperimentalPostingCheckpointBuild(
        self: *HBCIndex,
        posting_store: *posting_segment_store_mod.Store,
    ) !bool {
        const build = self.experimental_posting_checkpoint_build orelse return false;
        if (!build.completed.load(.acquire)) return false;
        if (build.thread) |thread| {
            thread.join();
            build.thread = null;
        }
        self.experimental_posting_checkpoint_build = null;
        defer build.deinit();
        if (build.build_error) |err| {
            std.log.warn("dense posting checkpoint background build failed generation={} sequence={} err={s}", .{
                build.segment_generation,
                build.covered_source_sequence,
                @errorName(err),
            });
            // WAL-authoritative callers retain the committed tail and defer
            // compaction; optional sidecars may invalidate and fall back to
            // the LSM. In neither mode may a failed build publish partial
            // checkpoint state.
            return err;
        }
        if (posting_store.wal_generation != build.wal_generation) {
            std.log.warn("dense posting checkpoint background build discarded after WAL generation changed expected={} actual={}", .{
                build.wal_generation,
                posting_store.wal_generation,
            });
            return false;
        }
        const result = build.result orelse return error.MissingPostingCheckpoint;
        try posting_store.publishStagedCheckpointPreservingWalTail(
            build.segment_generation,
            build.covered_source_sequence,
            result.segment_bytes,
            build.flattened_wal_bytes,
        );
        std.log.info("dense posting checkpoint published generation={} sequence={} bytes={} source=immutable_generation_background wal_tail_bytes={}", .{
            build.segment_generation,
            build.covered_source_sequence,
            result.segment_bytes.len,
            posting_store.wal_committed_bytes,
        });
        self.discardExperimentalPostingDeltaBaseState();
        self.refreshExperimentalPostingReadGenerationBestEffort(posting_store.covered_source_sequence);
        self.experimental_posting_overlay_collapsed_wal_bytes = posting_store.wal_committed_bytes;
        return true;
    }

    fn maintainExperimentalPostingCheckpoint(
        self: *HBCIndex,
        posting_store: *posting_segment_store_mod.Store,
    ) !void {
        // The builder normally finishes well before the durable tail reaches
        // the hard policy bound. If it does not, wait only at that bound so
        // recovery debt cannot grow without limit.
        if (self.experimental_posting_checkpoint_build) |build| {
            if (!build.completed.load(.acquire) and
                posting_store.wal_committed_bytes >= managedPostingCheckpointHardWalBytes)
            {
                build.force_progress.store(true, .release);
                if (build.thread) |thread| {
                    thread.join();
                    build.thread = null;
                }
                std.debug.assert(build.completed.load(.acquire));
            }
        }
        _ = try self.publishCompletedExperimentalPostingCheckpointBuild(posting_store);
        const next_overlay_collapse = std.math.add(
            u64,
            self.experimental_posting_overlay_collapsed_wal_bytes,
            managedPostingOverlayCollapseBytes,
        ) catch std.math.maxInt(u64);
        if (posting_store.wal_committed_bytes >= next_overlay_collapse) {
            const value_count = try self.collapseExperimentalPostingReadOverlays();
            self.experimental_posting_overlay_collapsed_wal_bytes = posting_store.wal_committed_bytes;
            std.log.info("dense posting live overlays collapsed sequence={} wal_bytes={} values={}", .{
                posting_store.covered_source_sequence,
                posting_store.wal_committed_bytes,
                value_count,
            });
        }
        if (self.experimental_posting_checkpoint_build == null and
            posting_store.shouldCheckpoint(managedPostingCheckpointPolicy))
        {
            if (!try self.startExperimentalPostingCheckpointBuild(posting_store)) {
                // A live immutable generation is normally available. Retain
                // the synchronous LSM fallback for fail-closed recovery paths.
                _ = try self.publishExperimentalPostingCheckpoint(posting_store.covered_source_sequence);
            }
        }
    }

    /// Starts or publishes opportunistic posting compaction at an explicit
    /// database-idle boundary. Building remains off the caller: readiness is
    /// bounded by the durable WAL commit, while later maintenance/write/close
    /// paths can publish the completed staged segment. The hard WAL ceiling in
    /// maintainExperimentalPostingCheckpoint remains the only join point.
    pub fn requestExperimentalPostingCheckpointForIdle(self: *HBCIndex) !bool {
        if (!self.experimental_posting_sidecar_managed or
            !self.experimental_posting_wal_authoritative.load(.acquire) or
            self.write_session_depth != 0 or
            self.experimental_posting_capture_enabled)
        {
            return false;
        }
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = try self.openExperimentalPostingStore();
        }
        const posting_store = &self.experimental_posting_write_store.?;
        const generation_before = if (posting_store.checkpoint) |checkpoint| checkpoint.segment_generation else 0;
        const build_before = self.experimental_posting_checkpoint_build != null;
        try self.maintainExperimentalPostingCheckpoint(posting_store);
        if (self.experimental_posting_checkpoint_build == null and
            posting_store.wal_committed_bytes >= managedPostingIdleCheckpointWalBytes)
        {
            _ = try self.startExperimentalPostingCheckpointBuild(posting_store);
        }
        const generation_after = if (posting_store.checkpoint) |checkpoint| checkpoint.segment_generation else 0;
        return generation_after != generation_before or
            (!build_before and self.experimental_posting_checkpoint_build != null);
    }

    pub fn closeExperimentalPostingWalWriter(self: *HBCIndex) void {
        if (self.experimental_posting_write_store) |*store| {
            store.deinit();
            self.experimental_posting_write_store = null;
        }
    }

    /// Resolve an ambiguous append by reopening and parsing the durable WAL
    /// prefix. If the intended batch committed, acknowledge it exactly once;
    /// if it did not, retry it once on the repaired/truncated WAL. A different
    /// next batch id means another writer violated the single-writer contract.
    fn appendExperimentalPostingBatchDurably(
        self: *HBCIndex,
        batch_id: u64,
        records: []const posting_segment_store_mod.BatchRecord,
        covered_source_sequence: u64,
        options: ExperimentalPostingWalAppendOptions,
    ) !void {
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = try self.openExperimentalPostingStore();
        }
        self.experimental_posting_write_store.?.appendBatchWithOptions(
            batch_id,
            records,
            covered_source_sequence,
            .{ .sync = options.sync },
        ) catch |first_err| {
            self.closeExperimentalPostingWalWriter();
            self.experimental_posting_write_store = self.openExperimentalPostingStore() catch |reopen_err| {
                std.log.err("dense posting WAL append recovery failed append_err={s} reopen_err={s}", .{
                    @errorName(first_err),
                    @errorName(reopen_err),
                });
                return reopen_err;
            };
            const recovered = &self.experimental_posting_write_store.?;
            if (recovered.last_committed_batch == batch_id) {
                if (recovered.covered_source_sequence != covered_source_sequence) {
                    return error.PostingWalAmbiguousCommitMismatch;
                }
                return;
            }
            if (try recovered.nextBatchId() != batch_id) return error.PostingWalConcurrentWriter;
            recovered.appendBatchWithOptions(
                batch_id,
                records,
                covered_source_sequence,
                .{ .sync = options.sync },
            ) catch |retry_err| {
                self.closeExperimentalPostingWalWriter();
                return retry_err;
            };
        };
    }

    pub fn invalidateExperimentalPostingSidecar(self: *HBCIndex) !void {
        self.discardExperimentalPostingCheckpointBuild();
        self.experimental_posting_overlay_collapsed_wal_bytes = 0;
        self.experimental_posting_capture_enabled = false;
        self.clearExperimentalPostingMutationBase();
        self.clearExperimentalPostingCapturedValues();
        self.disableExperimentalPostingReads();
        self.clearExperimentalPostingReadGeneration();
        self.discardExperimentalPostingDeltaBaseState();
        self.closeExperimentalPostingWalWriter();
        var posting_store = try self.openExperimentalPostingStore();
        defer posting_store.deinit();
        try posting_store.invalidate();
        self.experimental_posting_sidecar_managed = false;
    }

    pub fn persistExperimentalPostingSidecarAtAppliedSequence(
        self: *HBCIndex,
        applied_sequence: u64,
        options: ExperimentalPostingWalAppendOptions,
    ) !void {
        if (!self.experimental_posting_sidecar_managed) return;
        if (self.write_session_depth != 0) return error.ActiveWriteSession;
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = try self.openExperimentalPostingStore();
        }
        var posting_store = &self.experimental_posting_write_store.?;
        if (posting_store.checkpoint == null) {
            self.experimental_posting_capture_enabled = false;
            self.clearExperimentalPostingMutationBase();
            self.clearExperimentalPostingCapturedValues();
            _ = try self.publishExperimentalPostingCheckpoint(applied_sequence);
            return;
        }
        // Applied-sequence persistence is coalesced independently from dense
        // replay. A delayed callback may therefore carry a watermark below a
        // sidecar generation already published by a later callback. Never
        // regress source coverage or discard captured posting changes: record
        // those changes as another derived batch at the current source epoch.
        const publication_sequence = @max(posting_store.covered_source_sequence, applied_sequence);
        if (self.experimental_posting_capture_enabled) {
            _ = try self.finishExperimentalPostingMutationCapture(
                try posting_store.nextBatchId(),
                publication_sequence,
                options,
            );
        } else if (posting_store.covered_source_sequence < applied_sequence) {
            const batch_id = try posting_store.nextBatchId();
            const coverage = [_]posting_segment_store_mod.BatchRecord{.{
                .kind = .coverage,
                .posting_id = 0,
                .source_sequence = applied_sequence,
                .payload = &.{},
            }};
            try self.appendExperimentalPostingBatchDurably(batch_id, &coverage, applied_sequence, options);
            try self.publishExperimentalPostingGenerationAfterDurableCommit(&.{}, applied_sequence);
        }

        // finishExperimentalPostingMutationCapture may have lazily replaced
        // the writer handle. Native activation also deliberately closes that
        // LSM-borrowing handle while detaching the legacy backend, so reopen
        // it from the retained native storage lease before applying the tail
        // policy.
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = try self.openExperimentalPostingStore();
        }
        posting_store = &self.experimental_posting_write_store.?;
        self.maintainExperimentalPostingCheckpoint(posting_store) catch |err| {
            // The committed WAL batch above is already sufficient for exact
            // recovery. Compaction is bounded maintenance debt, not part of
            // the source transaction's durability decision.
            if (!self.experimental_posting_wal_authoritative.load(.acquire)) return err;
            std.log.warn("dense posting WAL checkpoint maintenance deferred sequence={} err={s}", .{
                posting_store.covered_source_sequence,
                @errorName(err),
            });
        };
    }

    pub fn beginExperimentalPostingMutationCapture(self: *HBCIndex) !void {
        if (self.experimental_posting_capture_enabled) return error.ExperimentalPostingCaptureAlreadyActive;
        // A posting capture is the derived-state transaction boundary, not an
        // LSM snapshot. It is valid (and required) inside a streaming replay
        // or bulk publication session: those sessions only batch backend
        // durability and may outlive several independently committed source
        // windows. The captured values are owned copies and publication still
        // waits until the outer write session is durably finished.
        self.clearExperimentalPostingCapturedValues();
        self.clearExperimentalPostingMutationBase();
        const wal_authoritative = self.experimental_posting_wal_authoritative.load(.acquire);
        if (self.experimental_posting_wal_mutations_enabled or wal_authoritative) {
            self.experimental_posting_mutation_base_generation = self.acquireExperimentalPostingReadGeneration();
            // A persisted authority marker is irreversible without an
            // explicit rebuild. Never route a later mutation into the stale
            // LSM merely because the live immutable generation became
            // unavailable after startup or an allocation failure.
            if (wal_authoritative and
                self.experimental_posting_mutation_base_generation == null)
            {
                return error.PostingWalMutationStoreUnavailable;
            }
        }
        self.experimental_posting_sidecar_managed = true;
        self.experimental_posting_capture_enabled = true;
    }

    pub fn experimentalPostingMutationCaptureActive(self: *const HBCIndex) bool {
        return self.experimental_posting_capture_enabled;
    }

    /// Routes query-facing HBC mutations through the committed posting WAL
    /// once an immutable base generation is available. Bootstrap and rebuild
    /// still use the general store until the first complete checkpoint exists.
    pub fn enableExperimentalPostingWalMutations(self: *HBCIndex) void {
        self.experimental_posting_wal_mutations_enabled = true;
    }

    pub fn experimentalPostingWalAuthoritative(self: *const HBCIndex) bool {
        return self.experimental_posting_wal_authoritative.load(.acquire);
    }

    pub fn cancelExperimentalPostingMutationCapture(self: *HBCIndex) void {
        self.experimental_posting_capture_enabled = false;
        var mutation_base_restored = true;
        if (self.experimental_posting_wal_authoritative.load(.acquire) and
            self.experimental_posting_mutation_base_generation != null)
        {
            // The source window may fail after one or more local HBC
            // transactions committed but before their posting-WAL batch was
            // published. Those transactions were WAL-only for derived state;
            // restore mutable topology and discard caches populated from the
            // abandoned overlay before admitting another query or replay.
            self.restoreExperimentalPostingMutationBase() catch |err| {
                mutation_base_restored = false;
                self.disableExperimentalPostingReads();
                std.log.err("dense posting WAL capture rollback could not restore committed base err={s}", .{@errorName(err)});
            };
        }
        self.clearExperimentalPostingMutationBase();
        self.clearExperimentalPostingCapturedValues();
        if (mutation_base_restored and
            self.experimental_posting_sidecar_managed and
            self.experimentalPostingReadGenerationLoaded())
        {
            self.experimental_posting_reads_enabled.store(true, .release);
        }
    }

    fn beginExperimentalPostingMaintenanceCapture(self: *HBCIndex) !?ExperimentalPostingMaintenanceCapture {
        // A source-replay capture already owns the ordering boundary. Any
        // maintenance it invokes belongs to that source batch and must not be
        // committed independently.
        if (!self.experimental_posting_sidecar_managed or self.experimental_posting_capture_enabled) return null;
        if (self.write_session_depth != 0) return null;
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = try self.openExperimentalPostingStore();
        }
        const posting_store = &self.experimental_posting_write_store.?;
        if (posting_store.checkpoint == null) return null;
        const capture: ExperimentalPostingMaintenanceCapture = .{
            .covered_source_sequence = posting_store.covered_source_sequence,
        };
        try self.beginExperimentalPostingMutationCapture();
        return capture;
    }

    fn cancelExperimentalPostingMaintenanceCapture(self: *HBCIndex) void {
        self.cancelExperimentalPostingMutationCapture();
    }

    /// Completes an independently committed maintenance transaction without
    /// advancing authoritative source coverage. In WAL-authoritative mode the
    /// caller observes publication failure and can retry the idempotent repair;
    /// optional sidecar mode invalidates itself and continues from the LSM.
    fn finishExperimentalPostingMaintenanceCapture(
        self: *HBCIndex,
        capture: ExperimentalPostingMaintenanceCapture,
    ) !void {
        if (self.experimental_posting_captured_values.count() == 0) {
            self.cancelExperimentalPostingMaintenanceCapture();
            return;
        }
        if (self.experimental_posting_write_store == null) return error.MissingPostingCheckpoint;
        var posting_store = &self.experimental_posting_write_store.?;
        if (posting_store.covered_source_sequence != capture.covered_source_sequence) {
            return error.PostingCheckpointSequenceMismatch;
        }
        _ = try self.finishExperimentalPostingMutationCapture(
            try posting_store.nextBatchId(),
            capture.covered_source_sequence,
            .{},
        );
        posting_store = &self.experimental_posting_write_store.?;
        self.maintainExperimentalPostingCheckpoint(posting_store) catch |err| {
            if (!self.experimental_posting_wal_authoritative.load(.acquire)) return err;
            std.log.warn("dense posting WAL maintenance checkpoint deferred sequence={} err={s}", .{
                capture.covered_source_sequence,
                @errorName(err),
            });
        };
    }

    fn experimentalPostingKindBenefitsFromPatch(kind: vectorindex_posting_wal.RecordKind) bool {
        return switch (kind) {
            // Packed nodes and quantized payloads contain long stable runs.
            // The remaining values are small fixed-width or short metadata;
            // walking an immutable generation chain and building an AFPD patch
            // costs more than writing their complete value.
            .base, .quantized_checkpoint, .posting_state => true,
            .node_range, .vector_leaf, .vector_metadata, .index_metadata => false,
            else => false,
        };
    }

    /// Must be called only after the authoritative HBC/source transaction has
    /// committed. It snapshots exactly the postings touched while capture was
    /// active into one committed derived-WAL batch.
    pub fn finishExperimentalPostingMutationCapture(
        self: *HBCIndex,
        batch_id: u64,
        covered_source_sequence: u64,
        options: ExperimentalPostingWalAppendOptions,
    ) !PostingWalAppendStats {
        if (!self.experimental_posting_capture_enabled) return error.ExperimentalPostingCaptureNotActive;
        self.experimental_posting_capture_enabled = false;
        defer self.clearExperimentalPostingMutationBase();
        defer self.clearExperimentalPostingCapturedValues();
        // The source/HBC transaction has already committed locally, but its
        // applied watermark has not. Optional dual-write mode may invalidate
        // and serve the complete LSM state. WAL-authoritative mode has no such
        // fallback: retain and restore the last committed generation so replay
        // can retry without ever exposing a partial mutation.
        errdefer if (self.experimental_posting_wal_authoritative.load(.acquire))
            self.preserveExperimentalPostingWalAfterCaptureFailure()
        else
            self.invalidateExperimentalPostingSidecar() catch |invalidate_err| {
                std.log.warn("dense posting sidecar capture invalidation failed err={s}", .{@errorName(invalidate_err)});
            };
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = try self.openExperimentalPostingStore();
        }
        var delta_base_generation = self.retainCurrentExperimentalPostingReadGeneration();
        defer if (delta_base_generation) |generation| generation.release();
        const delta_base_state = if (delta_base_generation == null)
            try self.ensureExperimentalPostingDeltaBaseState()
        else
            null;

        const touched_keys = try self.alloc.alloc(u128, self.experimental_posting_captured_values.count());
        defer self.alloc.free(touched_keys);
        var touched_index: usize = 0;
        var touched_it = self.experimental_posting_captured_values.keyIterator();
        while (touched_it.next()) |key| : (touched_index += 1) touched_keys[touched_index] = key.*;
        std.mem.sort(u128, touched_keys, {}, std.sort.asc(u128));

        var records = std.ArrayListUnmanaged(posting_segment_store_mod.BatchRecord).empty;
        defer records.deinit(self.alloc);
        var owned_patches = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_patches.items) |payload| self.alloc.free(payload);
            owned_patches.deinit(self.alloc);
        }
        try records.ensureTotalCapacity(self.alloc, @max(touched_keys.len, 1));
        try owned_patches.ensureTotalCapacity(self.alloc, touched_keys.len);

        for (touched_keys) |key| {
            const posting_id = experimentalPostingValueKeyId(key);
            const kind = try experimentalPostingValueKeyKind(key);
            const current = self.experimental_posting_captured_values.get(key) orelse
                return error.MissingExperimentalPostingCaptureValue;
            if (current) |value| {
                var emitted_patch = false;
                if (experimentalPostingKindBenefitsFromPatch(kind)) {
                    const previous = (if (delta_base_generation) |generation|
                        generation.value(posting_id, kind)
                    else
                        delta_base_state.?.value(posting_id, kind)) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (previous) |base| {
                        const patch = try vectorindex_posting_wal.encodeReplacementPatchAlloc(self.alloc, kind, base, value);
                        if (patch.len < value.len) {
                            owned_patches.appendAssumeCapacity(patch);
                            records.appendAssumeCapacity(.{
                                .kind = .mutation,
                                .posting_id = posting_id,
                                .source_sequence = covered_source_sequence,
                                .payload = patch,
                            });
                            emitted_patch = true;
                        } else {
                            self.alloc.free(patch);
                        }
                    }
                }
                if (!emitted_patch) {
                    records.appendAssumeCapacity(.{
                        .kind = kind,
                        .posting_id = posting_id,
                        .source_sequence = covered_source_sequence,
                        .payload = value,
                    });
                }
            } else {
                records.appendAssumeCapacity(.{
                    .kind = switch (kind) {
                        .base => .base_tombstone,
                        .quantized_checkpoint => .quantized_checkpoint_tombstone,
                        .posting_state => .posting_state_tombstone,
                        .node_range => .node_range_tombstone,
                        .vector_leaf => .vector_leaf_tombstone,
                        .vector_metadata => .vector_metadata_tombstone,
                        .index_metadata => .index_metadata_tombstone,
                        else => unreachable,
                    },
                    .posting_id = posting_id,
                    .source_sequence = covered_source_sequence,
                    .payload = &.{},
                });
            }
        }
        if (records.items.len == 0) {
            records.appendAssumeCapacity(.{
                .kind = .coverage,
                .posting_id = 0,
                .source_sequence = covered_source_sequence,
                .payload = &.{},
            });
        }
        try self.appendExperimentalPostingBatchDurably(batch_id, records.items, covered_source_sequence, options);

        // Patch construction and rollback no longer need their old-generation
        // leases after the WAL commit. Release them before publication so the
        // common no-reader path can safely extend the current aggregate delta
        // map in place. A publication allocation failure reloads the already
        // durable WAL generation below.
        if (delta_base_generation) |generation| {
            generation.release();
            delta_base_generation = null;
        }
        self.clearExperimentalPostingMutationBase();

        // The live immutable generation is itself the next delta encoder base.
        // Only the restart/fallback path maintains a separate materialized
        // state when no live generation was available.
        if (delta_base_state) |state| {
            for (touched_keys) |key| {
                const posting_id = experimentalPostingValueKeyId(key);
                const kind = try experimentalPostingValueKeyKind(key);
                const current = self.experimental_posting_captured_values.get(key) orelse
                    return error.MissingExperimentalPostingCaptureValue;
                try state.set(posting_id, kind, current);
            }
        }
        try self.publishExperimentalPostingGenerationAfterDurableCommit(
            touched_keys,
            covered_source_sequence,
        );
        const posting_store = &self.experimental_posting_write_store.?;
        const append_stats: PostingWalAppendStats = .{
            .batch_id = batch_id,
            .covered_source_sequence = covered_source_sequence,
            .record_count = @intCast(records.items.len),
            .wal_committed_bytes = posting_store.wal_committed_bytes,
        };
        self.detachLegacyLsmAfterNativeActivationBestEffort();
        return append_stats;
    }

    /// Installs the exact durable posting boundary for new queries. A
    /// coverage-only commit still needs a zero-value overlay so the in-process
    /// durableAppliedSequence agrees with crash recovery and journal
    /// truncation. Allocation failure on an authoritative index reloads the
    /// committed state; it may never fall back to stale LSM postings.
    fn publishExperimentalPostingGenerationAfterDurableCommit(
        self: *HBCIndex,
        touched_keys: []const u128,
        covered_source_sequence: u64,
    ) !void {
        self.publishExperimentalPostingReadOverlay(touched_keys, covered_source_sequence) catch |overlay_err| {
            if (self.experimental_posting_wal_authoritative.load(.acquire)) {
                self.refreshExperimentalPostingReadGeneration(covered_source_sequence) catch |refresh_err| {
                    self.disableExperimentalPostingReads();
                    std.log.err("dense posting WAL durable generation reload failed sequence={} overlay_err={s} reload_err={s}", .{
                        covered_source_sequence,
                        @errorName(overlay_err),
                        @errorName(refresh_err),
                    });
                    return refresh_err;
                };
            } else {
                std.log.warn("dense posting sidecar live overlay publication failed sequence={} err={s}", .{
                    covered_source_sequence,
                    @errorName(overlay_err),
                });
                self.disableExperimentalPostingReads();
                self.clearExperimentalPostingReadGeneration();
            }
        };
    }

    fn readCommittedExperimentalPostingValue(
        self: *HBCIndex,
        txn: *vectorindex_store.NamespaceReadTxn,
        posting_id: u64,
        kind: vectorindex_posting_wal.RecordKind,
    ) ![]const u8 {
        _ = self;
        return switch (kind) {
            .base => blk: {
                var key_buf: [12]u8 = undefined;
                break :blk try txn.get(.nodes, encodeNodeKey(&key_buf, posting_id, .packed_node));
            },
            .posting_state => blk: {
                var key_buf: [12]u8 = undefined;
                break :blk try txn.get(.nodes, encodeNodeKey(&key_buf, posting_id, .posting));
            },
            .quantized_checkpoint => blk: {
                var key_buf: [10]u8 = undefined;
                break :blk try txn.get(.quant, encodeQuantKey(&key_buf, posting_id));
            },
            .node_range => blk: {
                var key_buf: [12]u8 = undefined;
                break :blk try txn.get(.nodes, encodeNodeKey(&key_buf, posting_id, .range));
            },
            .vector_leaf => blk: {
                var key_buf: [10]u8 = undefined;
                break :blk try txn.get(.vecs, encodeVecLeafKey(&key_buf, posting_id));
            },
            .vector_metadata => blk: {
                var key_buf: [10]u8 = undefined;
                break :blk try txn.get(.vecs, encodeVecMetaKey(&key_buf, posting_id));
            },
            .index_metadata => try txn.get(.meta, meta_key),
            else => error.InvalidPostingPatchTarget,
        };
    }

    /// Rewrites a checkpoint from the already complete immutable query
    /// generation. This avoids opening a new LSM snapshot and scanning all
    /// derived namespaces every time the sidecar WAL is flattened.
    fn appendExperimentalPostingCheckpointFromGeneration(
        alloc: Allocator,
        writer: *vectorindex_posting_segment.Writer,
        generation: *ExperimentalPostingReadGeneration,
        metadata: IndexMetadata,
        covered_source_sequence: u64,
        foreground_manager: ?*resource_manager_mod.ResourceManager,
        force_progress: ?*const std.atomic.Value(bool),
    ) !?u64 {
        const root = experimentalPostingRootState(generation) orelse return null;
        const base_directory = root.vector_directory orelse return null;

        var latest: ExperimentalPostingLatestValues = .empty;
        defer latest.deinit(alloc);
        try collectExperimentalPostingLatestValues(alloc, generation, &latest);

        var metadata_buf: [IndexMetadata.encoded_size]u8 = undefined;
        try writer.appendValueAt(0, .index_metadata, covered_source_sequence, metadata.encode(&metadata_buf));

        var posting_count: u64 = 0;
        var node_id: u64 = 1;
        while (node_id <= metadata.node_count) : (node_id = std.math.add(u64, node_id, 1) catch break) {
            if (node_id % 256 == 0) {
                yieldExperimentalCheckpointForForeground(foreground_manager, force_progress);
            }
            const packed_node = (try experimentalPostingCheckpointValue(&latest, root, node_id, .base)) orelse continue;
            try writer.appendBaseAt(node_id, covered_source_sequence, packed_node);
            posting_count += 1;
            if (try experimentalPostingCheckpointValue(&latest, root, node_id, .node_range)) |range| {
                try writer.appendValueAt(node_id, .node_range, covered_source_sequence, range);
            }
            if (try experimentalPostingCheckpointValue(&latest, root, node_id, .posting_state)) |posting_state| {
                try writer.appendPostingStateAt(node_id, covered_source_sequence, posting_state);
            }
            if (try experimentalPostingCheckpointValue(&latest, root, node_id, .quantized_checkpoint)) |quantized| {
                try writer.appendQuantizedCheckpointAt(node_id, covered_source_sequence, quantized);
            }
        }

        var vector_overrides = std.ArrayListUnmanaged(ExperimentalVectorOverride).empty;
        defer vector_overrides.deinit(alloc);
        try vector_overrides.ensureTotalCapacity(alloc, latest.count());
        var latest_it = latest.iterator();
        while (latest_it.next()) |entry| {
            const record_kind = experimentalPostingValueKeyKind(entry.key_ptr.*) catch continue;
            const directory_kind: vectorindex_hbc_vector_directory.Kind = switch (record_kind) {
                .vector_leaf => .leaf,
                .vector_metadata => .metadata,
                else => continue,
            };
            vector_overrides.appendAssumeCapacity(.{
                .kind = directory_kind,
                .id = experimentalPostingValueKeyId(entry.key_ptr.*),
                .value = entry.value_ptr.*,
            });
        }
        std.mem.sort(ExperimentalVectorOverride, vector_overrides.items, {}, ExperimentalVectorOverride.lessThan);

        var directory_writer = try vectorindex_hbc_vector_directory.Writer.init(alloc);
        defer directory_writer.deinit();
        var base_it = base_directory.iterator();
        var override_index: usize = 0;
        var directory_items: usize = 0;
        while (try base_it.next()) |base| {
            directory_items += 1;
            if (directory_items % 256 == 0) {
                yieldExperimentalCheckpointForForeground(foreground_manager, force_progress);
            }
            while (override_index < vector_overrides.items.len and
                ExperimentalVectorOverride.compareItem(vector_overrides.items[override_index], base) == .lt)
            {
                const replacement = vector_overrides.items[override_index];
                if (replacement.value) |value| try directory_writer.append(replacement.kind, replacement.id, value);
                override_index += 1;
            }
            if (override_index < vector_overrides.items.len and
                ExperimentalVectorOverride.compareItem(vector_overrides.items[override_index], base) == .eq)
            {
                const replacement = vector_overrides.items[override_index];
                if (replacement.value) |value| try directory_writer.append(replacement.kind, replacement.id, value);
                override_index += 1;
            } else {
                try directory_writer.append(base.kind, base.id, base.value);
            }
        }
        while (override_index < vector_overrides.items.len) : (override_index += 1) {
            const replacement = vector_overrides.items[override_index];
            if (replacement.value) |value| try directory_writer.append(replacement.kind, replacement.id, value);
        }
        const vector_directory = try directory_writer.build();
        try writer.appendValueOwnedAt(0, .vector_directory, covered_source_sequence, vector_directory);
        return posting_count;
    }

    fn buildExperimentalPostingCheckpointFromGeneration(
        alloc: Allocator,
        generation: *ExperimentalPostingReadGeneration,
        metadata: IndexMetadata,
        covered_source_sequence: u64,
        foreground_manager: ?*resource_manager_mod.ResourceManager,
        force_progress: ?*const std.atomic.Value(bool),
    ) !ExperimentalPostingCheckpointBuildResult {
        var writer = vectorindex_posting_segment.Writer.init(alloc);
        defer writer.deinit();
        const posting_count = (try appendExperimentalPostingCheckpointFromGeneration(
            alloc,
            &writer,
            generation,
            metadata,
            covered_source_sequence,
            foreground_manager,
            force_progress,
        )) orelse return error.MissingPostingCheckpoint;
        return .{
            .segment_bytes = try writer.build(),
            .posting_count = posting_count,
        };
    }

    /// Materializes the currently committed packed HBC node and RaBitQ values
    /// into a durable immutable generation. The caller supplies the upstream
    /// source sequence whose complete effects are represented by this state.
    /// Once CURRENT is durable, installs the same immutable generation for new
    /// in-process readers. Publication therefore cannot expose a checkpoint
    /// before its segment and empty next-generation WAL are recoverable.
    pub fn publishExperimentalPostingCheckpoint(
        self: *HBCIndex,
        covered_source_sequence: u64,
    ) !PostingCheckpointStats {
        if (self.write_session_depth != 0) return error.ActiveWriteSession;
        self.discardExperimentalPostingCheckpointBuild();
        const source_generation = self.retainCurrentExperimentalPostingReadGeneration();
        defer if (source_generation) |generation| generation.release();
        self.discardExperimentalPostingDeltaBaseState();
        self.closeExperimentalPostingWalWriter();
        var posting_store = try self.openExperimentalPostingStore();
        defer posting_store.deinit();

        const generation = if (posting_store.checkpoint) |checkpoint|
            std.math.add(u64, checkpoint.segment_generation, 1) catch return error.PostingSegmentGenerationOverflow
        else
            1;
        var writer = vectorindex_posting_segment.Writer.init(self.alloc);
        defer writer.deinit();
        var posting_count: u64 = 0;

        const flattened_generation = if (source_generation) |read_generation|
            if (try appendExperimentalPostingCheckpointFromGeneration(
                self.alloc,
                &writer,
                read_generation,
                self.metadata,
                covered_source_sequence,
                null,
                null,
            )) |count| blk: {
                posting_count = count;
                break :blk true;
            } else false
        else
            false;

        if (!flattened_generation) {
            var txn = try self.store.beginRead();
            defer txn.abort();

            if (txn.get(.meta, meta_key)) |metadata| {
                try writer.appendValueAt(0, .index_metadata, covered_source_sequence, metadata);
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }
            var node_id: u64 = 1;
            while (node_id <= self.metadata.node_count) : (node_id = std.math.add(u64, node_id, 1) catch break) {
                var node_key_buf: [12]u8 = undefined;
                const packed_node = txn.get(.nodes, encodeNodeKey(&node_key_buf, node_id, .packed_node)) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                try writer.appendBaseAt(node_id, covered_source_sequence, packed_node);
                posting_count += 1;

                var range_key_buf: [12]u8 = undefined;
                if (txn.get(.nodes, encodeNodeKey(&range_key_buf, node_id, .range))) |range| {
                    try writer.appendValueAt(node_id, .node_range, covered_source_sequence, range);
                } else |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                }

                var posting_key_buf: [12]u8 = undefined;
                if (txn.get(.nodes, encodeNodeKey(&posting_key_buf, node_id, .posting))) |posting_state| {
                    try writer.appendPostingStateAt(node_id, covered_source_sequence, posting_state);
                } else |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                }

                var quant_key_buf: [10]u8 = undefined;
                if (txn.get(.quant, encodeQuantKey(&quant_key_buf, node_id))) |quantized| {
                    try writer.appendQuantizedCheckpointAt(node_id, covered_source_sequence, quantized);
                } else |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                }
            }

            // Vector-to-leaf assignments and external-vector metadata share
            // one compact ordered block. The first checkpoint (or a legacy
            // root without that block) must source it from the LSM; subsequent
            // checkpoints merge the immutable directory with live overlays.
            var vector_directory_writer = try vectorindex_hbc_vector_directory.Writer.init(self.alloc);
            defer vector_directory_writer.deinit();
            var vec_cursor = try txn.openCursor(.vecs);
            defer vec_cursor.close();
            var vec_entry = try vec_cursor.first();
            while (vec_entry) |entry| : (vec_entry = try vec_cursor.next()) {
                const decoded = decodeVectorDerivedKey(entry.key) orelse continue;
                try vector_directory_writer.append(switch (decoded.kind) {
                    .vector_leaf => .leaf,
                    .vector_metadata => .metadata,
                    else => unreachable,
                }, decoded.vector_id, entry.value);
            }
            const vector_directory = try vector_directory_writer.build();
            try writer.appendValueOwnedAt(0, .vector_directory, covered_source_sequence, vector_directory);
        }
        const segment_bytes = try writer.build();
        defer self.alloc.free(segment_bytes);
        try posting_store.publishCheckpoint(generation, covered_source_sequence, segment_bytes);
        std.log.info("dense posting checkpoint published generation={} sequence={} bytes={} source={s}", .{
            generation,
            covered_source_sequence,
            segment_bytes.len,
            if (flattened_generation) "immutable_generation" else "authoritative_lsm",
        });
        // Collapse the live overlay chain at the same point the durable WAL is
        // checkpointed. Failure leaves the equivalent old generation active.
        self.refreshExperimentalPostingReadGenerationBestEffort(covered_source_sequence);
        self.experimental_posting_overlay_collapsed_wal_bytes = posting_store.wal_committed_bytes;
        return .{
            .generation = generation,
            .covered_source_sequence = covered_source_sequence,
            .posting_count = posting_count,
            .segment_bytes = @intCast(segment_bytes.len),
            .flattened_generation = flattened_generation,
        };
    }

    /// Appends full committed snapshots for selected postings to the sidecar
    /// WAL. This is an experimental measurement hook: production mutation
    /// wiring must derive posting ids and source sequence from the transaction
    /// that has already committed to the authoritative source journal.
    pub fn appendExperimentalPostingWalSnapshotBatch(
        self: *HBCIndex,
        batch_id: u64,
        covered_source_sequence: u64,
        posting_ids: []const u64,
    ) !PostingWalAppendStats {
        return try self.appendExperimentalPostingWalSnapshotBatchWithOptions(
            batch_id,
            covered_source_sequence,
            posting_ids,
            .{},
        );
    }

    pub fn appendExperimentalPostingWalSnapshotBatchWithOptions(
        self: *HBCIndex,
        batch_id: u64,
        covered_source_sequence: u64,
        posting_ids: []const u64,
        options: ExperimentalPostingWalAppendOptions,
    ) !PostingWalAppendStats {
        if (self.write_session_depth != 0) return error.ActiveWriteSession;
        if (posting_ids.len == 0) return error.EmptyPostingWalBatch;
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = try self.openExperimentalPostingStore();
        }
        const posting_store = &self.experimental_posting_write_store.?;

        var records = std.ArrayListUnmanaged(posting_segment_store_mod.BatchRecord).empty;
        defer records.deinit(self.alloc);
        var owned_payloads = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_payloads.items) |payload| self.alloc.free(payload);
            owned_payloads.deinit(self.alloc);
        }
        const max_records = std.math.mul(usize, posting_ids.len, 3) catch return error.PostingWalTooLarge;
        try records.ensureTotalCapacity(self.alloc, max_records);
        try owned_payloads.ensureTotalCapacity(self.alloc, max_records);

        var txn = try self.store.beginRead();
        defer txn.abort();
        for (posting_ids) |posting_id| {
            if (posting_id == 0) return error.InvalidPostingId;
            var node_key_buf: [12]u8 = undefined;
            const packed_node = txn.get(.nodes, encodeNodeKey(&node_key_buf, posting_id, .packed_node)) catch |err| switch (err) {
                error.NotFound => {
                    records.appendAssumeCapacity(.{
                        .kind = .tombstone,
                        .posting_id = posting_id,
                        .source_sequence = covered_source_sequence,
                        .payload = &.{},
                    });
                    continue;
                },
                else => return err,
            };
            const owned_packed = try self.alloc.dupe(u8, packed_node);
            owned_payloads.appendAssumeCapacity(owned_packed);
            records.appendAssumeCapacity(.{
                .kind = .base,
                .posting_id = posting_id,
                .source_sequence = covered_source_sequence,
                .payload = owned_packed,
            });

            var posting_key_buf: [12]u8 = undefined;
            if (txn.get(.nodes, encodeNodeKey(&posting_key_buf, posting_id, .posting))) |posting_state| {
                const owned_state = try self.alloc.dupe(u8, posting_state);
                owned_payloads.appendAssumeCapacity(owned_state);
                records.appendAssumeCapacity(.{
                    .kind = .posting_state,
                    .posting_id = posting_id,
                    .source_sequence = covered_source_sequence,
                    .payload = owned_state,
                });
            } else |err| switch (err) {
                error.NotFound => records.appendAssumeCapacity(.{
                    .kind = .posting_state_tombstone,
                    .posting_id = posting_id,
                    .source_sequence = covered_source_sequence,
                    .payload = &.{},
                }),
                else => return err,
            }

            var quant_key_buf: [10]u8 = undefined;
            if (txn.get(.quant, encodeQuantKey(&quant_key_buf, posting_id))) |quantized| {
                const owned_quantized = try self.alloc.dupe(u8, quantized);
                owned_payloads.appendAssumeCapacity(owned_quantized);
                records.appendAssumeCapacity(.{
                    .kind = .quantized_checkpoint,
                    .posting_id = posting_id,
                    .source_sequence = covered_source_sequence,
                    .payload = owned_quantized,
                });
            } else |err| switch (err) {
                error.NotFound => records.appendAssumeCapacity(.{
                    .kind = .quantized_checkpoint_tombstone,
                    .posting_id = posting_id,
                    .source_sequence = covered_source_sequence,
                    .payload = &.{},
                }),
                else => return err,
            }
        }
        try posting_store.appendBatchWithOptions(batch_id, records.items, covered_source_sequence, .{ .sync = options.sync });
        return .{
            .batch_id = batch_id,
            .covered_source_sequence = covered_source_sequence,
            .record_count = @intCast(records.items.len),
            .wal_committed_bytes = posting_store.wal_committed_bytes,
        };
    }

    fn loadExperimentalPostingState(self: *HBCIndex, expected_source_sequence: ?u64) !*ExperimentalPostingReadState {
        var opened = try self.openExperimentalPostingStoreWithSegment();
        defer opened.store.deinit();
        var segment_bytes_owned = true;
        errdefer if (segment_bytes_owned) opened.segment.deinit(self.alloc);
        if (opened.store.checkpoint == null) return error.MissingPostingCheckpoint;
        if (expected_source_sequence) |expected| if (opened.store.covered_source_sequence != expected) {
            return error.PostingCheckpointSequenceMismatch;
        };

        const segment_bytes = opened.segment.bytes();
        var segment = try vectorindex_posting_segment.VerifiedReader.init(self.alloc, segment_bytes);
        var segment_owned = true;
        errdefer if (segment_owned) segment.deinit();
        if (try segment.getValue(0, .index_metadata)) |encoded_metadata| {
            _ = try self.decodeExperimentalPostingMetadata(encoded_metadata);
        }
        var wal = try opened.store.recoverWal();
        var wal_owned = true;
        errdefer if (wal_owned) wal.deinit();
        for (wal.replay.records.items) |record| switch (record.kind) {
            .base,
            .quantized_checkpoint,
            .posting_state,
            .node_range,
            .vector_leaf,
            .vector_metadata,
            .index_metadata,
            .tombstone,
            .base_tombstone,
            .quantized_checkpoint_tombstone,
            .posting_state_tombstone,
            .node_range_tombstone,
            .vector_leaf_tombstone,
            .vector_metadata_tombstone,
            .index_metadata_tombstone,
            .mutation,
            .coverage,
            => {},
            .centroid_directory, .commit => return error.UnsupportedExperimentalPostingWalRecord,
        };

        // The directory validates its compact index eagerly and values lazily.
        // Avoid the generic segment payload CRC here: hashing the entire nested
        // container would fault every metadata page on an otherwise cold mmap.
        const vector_directory = if (try segment.getNestedContainer(0, .vector_directory)) |bytes|
            try vectorindex_hbc_vector_directory.Reader.init(bytes)
        else
            null;
        const state = try self.alloc.create(ExperimentalPostingReadState);
        errdefer self.alloc.destroy(state);
        state.* = .{
            .alloc = self.alloc,
            .covered_source_sequence = opened.store.covered_source_sequence,
            .retained_segment = opened.segment,
            .segment = segment,
            .vector_directory = vector_directory,
            .wal_committed_bytes = @intCast(wal.replay.committed_bytes),
        };
        segment_bytes_owned = false;
        segment_owned = false;
        var state_initialized = true;
        errdefer if (state_initialized) state.deinit();
        try state.materializeWal(&wal);
        // Materialized full values borrow from the recovered byte buffer; the
        // replay frame index itself is no longer needed after activation.
        state.wal_bytes = wal.bytes;
        wal.replay.deinit();
        wal_owned = false;
        state_initialized = false;
        return state;
    }

    fn ensureExperimentalPostingDeltaBaseState(self: *HBCIndex) !*ExperimentalPostingReadState {
        if (self.experimental_posting_delta_base_state) |state| return state;
        const state = try self.loadExperimentalPostingState(null);
        self.experimental_posting_delta_base_state = state;
        return state;
    }

    /// Activates a previously published segment plus its committed WAL tail
    /// only when it exactly covers the caller's authoritative source sequence.
    /// Captured posting mutations leave the last committed immutable generation
    /// readable, then atomically publish a delta overlay after commit. Leased
    /// old generations remain valid for transactions already in flight.
    pub fn activateExperimentalPostingReads(self: *HBCIndex, expected_source_sequence: u64) !void {
        if (self.experimentalPostingReadGenerationLoaded()) return error.ExperimentalPostingReadsAlreadyLoaded;
        const state = try self.loadExperimentalPostingState(expected_source_sequence);
        try self.activateExperimentalPostingReadState(state);
    }

    /// A WAL-authoritative generation may be one committed source operation
    /// ahead when a process crashes after posting-WAL fsync but before the
    /// catalog watermark write. Existing source replay is idempotent, so this
    /// is valid prepared state. Falling behind the catalog is never valid.
    pub fn activateExperimentalPostingReadsAtOrAfter(self: *HBCIndex, minimum_source_sequence: u64) !u64 {
        if (self.experimentalPostingReadGenerationLoaded()) return error.ExperimentalPostingReadsAlreadyLoaded;
        const state = try self.loadExperimentalPostingState(null);
        const covered_source_sequence = state.covered_source_sequence;
        if (covered_source_sequence < minimum_source_sequence) {
            state.deinit();
            self.alloc.destroy(state);
            return error.PostingCheckpointSequenceMismatch;
        }
        try self.activateExperimentalPostingReadState(state);
        return covered_source_sequence;
    }

    fn activateExperimentalPostingReadState(self: *HBCIndex, state: *ExperimentalPostingReadState) !void {
        var state_owned = true;
        errdefer if (state_owned) {
            state.deinit();
            self.alloc.destroy(state);
        };
        const generation = try ExperimentalPostingReadGeneration.createRoot(self.alloc, state);
        state_owned = false;
        var generation_owned = true;
        errdefer if (generation_owned) generation.release();
        try self.installExperimentalPostingRootGeneration(generation);
        generation_owned = false;
        self.experimental_posting_sidecar_managed = true;
        self.experimental_posting_overlay_collapsed_wal_bytes = state.wal_committed_bytes;
        self.detachLegacyLsmAfterNativeActivationBestEffort();
    }

    fn detachLegacyLsmAfterNativeActivationBestEffort(self: *HBCIndex) void {
        if (!self.nativeHbcAuthoritative() or
            !self.experimentalPostingReadGenerationLoaded() or
            self.write_session_depth != 0 or
            self.experimental_posting_capture_enabled or
            self.experimental_posting_checkpoint_build != null)
        {
            return;
        }
        if (self.env_owner != .lsm) return;
        if (self.experimental_posting_write_store == null) {
            self.experimental_posting_write_store = self.openExperimentalPostingStore() catch |err| {
                std.log.warn("HBC native authority marker could not open mutation store err={s}", .{@errorName(err)});
                return;
            };
        }
        self.experimental_posting_write_store.?.markAuthoritative() catch |err| {
            std.log.warn("HBC native authority marker publication failed err={s}", .{@errorName(err)});
            return;
        };
        const native = hbc_backend.NativeBackend.detachFromLsm(self.alloc, &self.env_owner.lsm) catch |err| {
            std.log.warn("HBC native generation could not release legacy LSM backend err={s}", .{@errorName(err)});
            return;
        };

        // Store handles and a retained posting Store can borrow the LSM's
        // internally-owned NativeStorage object. Drop them before closing the
        // LSM, then recreate the posting handle from the retained storage
        // lease held by NativeBackend.
        self.closeExperimentalPostingWalWriter();
        if (self.experimental_posting_write_store) |*store| store.deinit();
        self.experimental_posting_write_store = null;
        self.store.deinit();
        self.env_owner.lsm.close();
        self.env_owner = .{ .native = native };
        self.store = nativeNamespaceStore(self.alloc, native);
        std.log.info("HBC native generation released legacy LSM backend", .{});
    }

    pub fn experimentalPostingReadsEnabled(self: *const HBCIndex) bool {
        return self.experimental_posting_reads_enabled.load(.acquire);
    }

    fn txnLikeChild(comptime T: type) type {
        return switch (@typeInfo(T)) {
            .pointer => |ptr| ptr.child,
            else => @compileError("expected pointer to transaction-like type"),
        };
    }

    pub fn txnUsesImmutableGeneration(self: *HBCIndex, txn: anytype) bool {
        _ = self;
        const Child = comptime txnLikeChild(@TypeOf(txn));
        return if (comptime Child == vectorindex_store.NamespaceReadTxn)
            txn.read_lease != null
        else
            false;
    }

    pub fn bindTxnLike(self: *HBCIndex, txn: anytype) !void {
        _ = self;
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceReadTxn,
            vectorindex_store.NamespaceWriteTxn,
            vectorindex_store.NamespaceBatch,
            => {},
            else => @compileError("expected vectorindex namespace transaction"),
        }
    }

    fn decodeNodeKey(key: []const u8) ?struct { id: u64, suffix: Suffix } {
        if (key.len != 12) return null;
        if (key[0] != 'n' or key[1] != ':' or key[10] != ':') return null;
        const suffix: Suffix = switch (key[11]) {
            @intFromEnum(Suffix.header) => .header,
            @intFromEnum(Suffix.centroid) => .centroid,
            @intFromEnum(Suffix.children) => .children,
            @intFromEnum(Suffix.members) => .members,
            @intFromEnum(Suffix.packed_node) => .packed_node,
            @intFromEnum(Suffix.range) => .range,
            @intFromEnum(Suffix.posting) => .posting,
            else => return null,
        };
        return .{
            .id = std.mem.readInt(u64, key[2..10], .big),
            .suffix = suffix,
        };
    }

    fn stagedNodeKeyId(key: []const u8) ?u128 {
        const decoded = decodeNodeKey(key) orelse return null;
        return (@as(u128, decoded.id) << 8) | @as(u128, @intFromEnum(decoded.suffix));
    }

    fn stagedNodeKeyParts(staged_key: u128) struct { id: u64, suffix: Suffix } {
        const suffix_byte: u8 = @intCast(staged_key & 0xff);
        const suffix: Suffix = switch (suffix_byte) {
            @intFromEnum(Suffix.header) => .header,
            @intFromEnum(Suffix.centroid) => .centroid,
            @intFromEnum(Suffix.children) => .children,
            @intFromEnum(Suffix.members) => .members,
            @intFromEnum(Suffix.packed_node) => .packed_node,
            @intFromEnum(Suffix.range) => .range,
            @intFromEnum(Suffix.posting) => .posting,
            else => unreachable,
        };
        return .{
            .id = @intCast(staged_key >> 8),
            .suffix = suffix,
        };
    }

    fn clearDeferredNodeKeys(self: *HBCIndex) void {
        {
            var it = self.deferred_node_keys.valueIterator();
            while (it.next()) |entry| entry.deinit(self.alloc);
        }
        self.deferred_node_keys.clearRetainingCapacity();
        self.deferred_node_key_value_bytes = 0;
        self.observeApplyWorkspaceBytes();
    }

    fn releaseDeferredBulkWorkspaceCapacity(self: *HBCIndex) void {
        self.deferred_quantized_nodes.deinit(self.alloc);
        self.deferred_quantized_nodes = .empty;
        self.clearDeferredNodeKeys();
        self.deferred_node_keys.deinit(self.alloc);
        self.deferred_node_keys = .empty;
        self.deferred_oversized_leaves.deinit(self.alloc);
        self.deferred_oversized_leaves = .empty;
        self.apply_workspace_split_bytes = self.bulk_split_vector_workspace.accounted_bytes;
        self.deferred_node_key_value_bytes = 0;
        self.observeApplyWorkspaceBytes();
    }

    fn stagedNodeKey(self: *HBCIndex, key: []const u8) ?*DeferredNodeValue {
        const staged_key = stagedNodeKeyId(key) orelse return null;
        return self.deferred_node_keys.getPtr(staged_key);
    }

    fn stageNodeKeyPut(self: *HBCIndex, key: []const u8, value: []const u8) !bool {
        if (!self.crossBatchPublicationActive()) return false;
        const staged_key = stagedNodeKeyId(key) orelse return false;
        const owned = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned);
        const result = try self.deferred_node_keys.getOrPut(self.alloc, staged_key);
        if (result.found_existing) {
            if (result.value_ptr.value) |old| self.deferred_node_key_value_bytes -|= old.len;
            result.value_ptr.deinit(self.alloc);
        }
        result.value_ptr.* = .{ .value = owned };
        self.deferred_node_key_value_bytes +|= owned.len;
        self.maybeObserveApplyWorkspaceBytes();
        return true;
    }

    fn stageNodeKeyDelete(self: *HBCIndex, key: []const u8) !bool {
        if (!self.crossBatchPublicationActive()) return false;
        const staged_key = stagedNodeKeyId(key) orelse return false;
        const result = try self.deferred_node_keys.getOrPut(self.alloc, staged_key);
        if (result.found_existing) {
            if (result.value_ptr.value) |old| self.deferred_node_key_value_bytes -|= old.len;
            result.value_ptr.deinit(self.alloc);
        }
        result.value_ptr.* = .{ .value = null };
        self.maybeObserveApplyWorkspaceBytes();
        return true;
    }

    fn decodeQuantizedNodeId(key: []const u8) ?u64 {
        if (key.len != 10 or key[0] != 'q' or key[1] != ':') return null;
        return std.mem.readInt(u64, key[2..10], .big);
    }

    fn decodeVectorDerivedKey(key: []const u8) ?struct {
        vector_id: u64,
        kind: vectorindex_posting_wal.RecordKind,
    } {
        if (key.len != 10 or key[1] != ':') return null;
        const kind: vectorindex_posting_wal.RecordKind = switch (key[0]) {
            'l' => .vector_leaf,
            'm' => .vector_metadata,
            // Raw vectors are source-owned when an external loader is
            // configured. Index-local vectors need a separately sharded block
            // container rather than inflating the compact posting segment.
            else => return null,
        };
        return .{
            .vector_id = std.mem.readInt(u64, key[2..10], .big),
            .kind = kind,
        };
    }

    fn isRawVectorKey(key: []const u8) bool {
        return key.len == 10 and key[0] == 'v' and key[1] == ':';
    }

    fn experimentalPostingMutationIdentity(namespace: Namespace, key: []const u8) ?struct {
        posting_id: u64,
        kind: vectorindex_posting_wal.RecordKind,
    } {
        return switch (namespace) {
            .nodes => blk: {
                const decoded = decodeNodeKey(key) orelse return null;
                break :blk switch (decoded.suffix) {
                    .packed_node => .{ .posting_id = decoded.id, .kind = .base },
                    .posting => .{ .posting_id = decoded.id, .kind = .posting_state },
                    .range => .{ .posting_id = decoded.id, .kind = .node_range },
                    else => return null,
                };
            },
            .quant => .{
                .posting_id = decodeQuantizedNodeId(key) orelse return null,
                .kind = .quantized_checkpoint,
            },
            // Mutable topology and counts must recover from the same epoch as
            // posting values. Projection progress shares this compact row, so
            // independent metadata-only updates are published as maintenance
            // batches at the current source sequence.
            .meta => if (std.mem.eql(u8, key, meta_key))
                .{ .posting_id = 0, .kind = .index_metadata }
            else
                null,
            .vecs => if (decodeVectorDerivedKey(key)) |decoded|
                .{ .posting_id = decoded.vector_id, .kind = decoded.kind }
            else
                null,
        };
    }

    /// Called before the authoritative mutation is staged. Non-posting keys
    /// cannot stale the sidecar. A covered posting mutation is captured;
    /// otherwise CURRENT is removed before that mutation can commit.
    fn noteExperimentalPostingMutation(
        self: *HBCIndex,
        namespace: Namespace,
        key: []const u8,
        value: ?[]const u8,
    ) !bool {
        const posting_value = experimentalPostingMutationIdentity(namespace, key) orelse return false;
        if (!self.experimental_posting_capture_enabled) {
            if (self.experimental_posting_wal_authoritative.load(.acquire)) {
                // A query-facing derived mutation outside a publication
                // boundary would otherwise exist in neither the committed WAL
                // nor a complete LSM fallback. Refuse it before staging.
                return error.PostingWalMutationOutsideCapture;
            }
            if (self.experimental_posting_sidecar_managed) try self.invalidateExperimentalPostingSidecar();
            return false;
        }
        const owned = if (value) |bytes| try self.alloc.dupe(u8, bytes) else null;
        errdefer if (owned) |bytes| self.alloc.free(bytes);
        const result = try self.experimental_posting_captured_values.getOrPut(
            self.alloc,
            experimentalPostingValueKey(posting_value.posting_id, posting_value.kind),
        );
        if (result.found_existing) if (result.value_ptr.*) |old| self.alloc.free(old);
        result.value_ptr.* = owned;
        return self.experimental_posting_mutation_base_generation != null;
    }

    fn experimentalPostingValue(self: *HBCIndex, txn: anytype, namespace: Namespace, key: []const u8) !?[]const u8 {
        _ = self;
        const Child = comptime txnLikeChild(@TypeOf(txn));
        if (comptime Child != vectorindex_store.NamespaceReadTxn) return null;
        const lease = txn.read_lease orelse return null;
        const generation: *ExperimentalPostingReadGeneration = @ptrCast(@alignCast(lease.ptr));
        return switch (namespace) {
            .nodes => blk: {
                const decoded = decodeNodeKey(key) orelse break :blk null;
                break :blk switch (decoded.suffix) {
                    .packed_node => (try generation.value(decoded.id, .base)) orelse return error.NotFound,
                    .posting => (try generation.value(decoded.id, .posting_state)) orelse return error.NotFound,
                    .range => (try generation.value(decoded.id, .node_range)) orelse return error.NotFound,
                    else => null,
                };
            },
            .quant => blk: {
                const node_id = decodeQuantizedNodeId(key) orelse break :blk null;
                break :blk (try generation.value(node_id, .quantized_checkpoint)) orelse return error.NotFound;
            },
            .meta => if (std.mem.eql(u8, key, meta_key))
                (try generation.value(0, .index_metadata)) orelse return error.NotFound
            else
                null,
            .vecs => blk: {
                const decoded = decodeVectorDerivedKey(key) orelse break :blk null;
                break :blk (try generation.value(decoded.vector_id, decoded.kind)) orelse return error.NotFound;
            },
        };
    }

    /// Resolves mutation reads against the transaction-window overlay and its
    /// pinned immutable base. Read transactions deliberately do not use this
    /// path: they keep serving the last fully committed generation until WAL
    /// commit and overlay publication complete together.
    fn experimentalPostingMutationValue(self: *HBCIndex, txn: anytype, namespace: Namespace, key: []const u8) !?[]const u8 {
        const Child = comptime txnLikeChild(@TypeOf(txn));
        if (comptime Child == vectorindex_store.NamespaceReadTxn) return null;
        const base = self.experimental_posting_mutation_base_generation orelse return null;
        const identity = experimentalPostingMutationIdentity(namespace, key) orelse return null;
        const logical_key = experimentalPostingValueKey(identity.posting_id, identity.kind);
        if (self.experimental_posting_captured_values.get(logical_key)) |captured| {
            return captured orelse error.NotFound;
        }
        return (try base.value(identity.posting_id, identity.kind)) orelse return error.NotFound;
    }

    /// Batch lookups must honor the same immutable transaction lease as point
    /// reads; otherwise rerank metadata and split planning silently fall back
    /// to a newer LSM snapshot than the posting generation.
    pub fn getNamespacedManySorted(
        self: *HBCIndex,
        txn: anytype,
        comptime namespace: Namespace,
        keys: []const []const u8,
        values: []?[]const u8,
    ) !void {
        if (keys.len != values.len) return error.InvalidArgument;
        const Child = comptime txnLikeChild(@TypeOf(txn));
        if (comptime Child == vectorindex_store.NamespaceReadTxn) {
            if (txn.read_lease != null) {
                for (keys, values) |key, *value| {
                    value.* = self.getNamespaced(txn, namespace, key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                }
                return;
            }
        } else if (self.experimental_posting_mutation_base_generation != null) {
            for (keys, values) |key, *value| {
                value.* = self.getNamespaced(txn, namespace, key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
            }
            return;
        }
        try txn.getManySorted(namespace, keys, values);
    }

    pub fn getNamespaced(self: *HBCIndex, txn: anytype, comptime namespace: Namespace, key: []const u8) ![]const u8 {
        if (namespace == .vecs and isRawVectorKey(key) and self.hasExternalVectorLoader()) {
            // Exact vectors are source-owned. Report a private-store miss so
            // the HBC vector loader takes its normal primary-store fallback.
            return error.NotFound;
        }
        if (namespace == .nodes) {
            if (self.stagedNodeKey(key)) |staged| {
                return staged.value orelse error.NotFound;
            }
        }
        if (try self.experimentalPostingMutationValue(txn, namespace, key)) |value| return value;
        if (try self.experimentalPostingValue(txn, namespace, key)) |value| return value;
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceReadTxn,
            vectorindex_store.NamespaceWriteTxn,
            vectorindex_store.NamespaceBatch,
            => return try txn.get(namespace, key),
            else => @compileError("expected vectorindex namespace transaction"),
        }
    }

    fn getNamespacedCommitted(self: *HBCIndex, txn: anytype, comptime namespace: Namespace, key: []const u8) ![]const u8 {
        if (namespace == .vecs and isRawVectorKey(key) and self.hasExternalVectorLoader()) return error.NotFound;
        if (try self.experimentalPostingMutationValue(txn, namespace, key)) |value| return value;
        if (try self.experimentalPostingValue(txn, namespace, key)) |value| return value;
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceReadTxn,
            vectorindex_store.NamespaceWriteTxn,
            vectorindex_store.NamespaceBatch,
            => return try txn.get(namespace, key),
            else => @compileError("expected vectorindex namespace transaction"),
        }
    }

    pub fn putNamespaced(self: *HBCIndex, txn: anytype, comptime namespace: Namespace, key: []const u8, value: []const u8) !void {
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceWriteTxn,
            vectorindex_store.NamespaceBatch,
            => {
                if (namespace == .vecs and isRawVectorKey(key) and self.hasExternalVectorLoader()) return;
                const wal_owned = try self.noteExperimentalPostingMutation(namespace, key, value);
                if (namespace == .nodes and try self.stageNodeKeyPut(key, value)) return;
                if (wal_owned) return;
                try txn.put(namespace, key, value);
                self.noteNamespacePut(namespace, key.len, value.len, false);
            },
            vectorindex_store.NamespaceReadTxn => return error.ReadOnly,
            else => @compileError("expected vectorindex namespace transaction"),
        }
    }

    pub fn appendNamespaced(self: *HBCIndex, txn: anytype, comptime namespace: Namespace, key: []const u8, value: []const u8) !void {
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceWriteTxn => {
                if (namespace == .vecs and isRawVectorKey(key) and self.hasExternalVectorLoader()) return;
                if (try self.noteExperimentalPostingMutation(namespace, key, value)) return;
                txn.appendPut(namespace, key, value) catch |err| switch (err) {
                    error.Unsupported => {
                        try txn.put(namespace, key, value);
                        self.noteNamespacePut(namespace, key.len, value.len, false);
                        return;
                    },
                    else => return err,
                };
                self.noteNamespacePut(namespace, key.len, value.len, true);
            },
            vectorindex_store.NamespaceBatch => {
                if (namespace == .vecs and isRawVectorKey(key) and self.hasExternalVectorLoader()) return;
                if (try self.noteExperimentalPostingMutation(namespace, key, value)) return;
                txn.appendPut(namespace, key, value) catch |err| switch (err) {
                    error.Unsupported => {
                        try txn.put(namespace, key, value);
                        self.noteNamespacePut(namespace, key.len, value.len, false);
                        return;
                    },
                    else => return err,
                };
                self.noteNamespacePut(namespace, key.len, value.len, true);
            },
            vectorindex_store.NamespaceReadTxn => return error.ReadOnly,
            else => @compileError("expected vectorindex namespace transaction"),
        }
    }

    pub fn deleteNamespaced(self: *HBCIndex, txn: anytype, comptime namespace: Namespace, key: []const u8) !void {
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceWriteTxn,
            vectorindex_store.NamespaceBatch,
            => {
                if (namespace == .vecs and isRawVectorKey(key) and self.hasExternalVectorLoader()) return;
                const wal_owned = try self.noteExperimentalPostingMutation(namespace, key, null);
                if (namespace == .nodes and try self.stageNodeKeyDelete(key)) return;
                if (wal_owned) return;
                try txn.delete(namespace, key);
                self.noteNamespaceDelete(namespace, key.len);
            },
            vectorindex_store.NamespaceReadTxn => return error.ReadOnly,
            else => @compileError("expected vectorindex namespace transaction"),
        }
    }

    fn noteNamespacePut(self: *HBCIndex, namespace: Namespace, key_len: usize, value_len: usize, append: bool) void {
        const key_bytes: u64 = @intCast(key_len);
        const value_bytes: u64 = @intCast(value_len);
        switch (namespace) {
            .nodes => {
                if (append) {
                    self.write_profile.ns_nodes_append_calls += 1;
                } else {
                    self.write_profile.ns_nodes_put_calls += 1;
                }
                self.write_profile.ns_nodes_key_bytes += key_bytes;
                self.write_profile.ns_nodes_value_bytes += value_bytes;
            },
            .meta => {
                if (append) {
                    self.write_profile.ns_meta_append_calls += 1;
                } else {
                    self.write_profile.ns_meta_put_calls += 1;
                }
                self.write_profile.ns_meta_key_bytes += key_bytes;
                self.write_profile.ns_meta_value_bytes += value_bytes;
            },
            .quant => {
                if (append) {
                    self.write_profile.ns_quant_append_calls += 1;
                } else {
                    self.write_profile.ns_quant_put_calls += 1;
                }
                self.write_profile.ns_quant_key_bytes += key_bytes;
                self.write_profile.ns_quant_value_bytes += value_bytes;
            },
            .vecs => {
                if (append) {
                    self.write_profile.ns_vecs_append_calls += 1;
                } else {
                    self.write_profile.ns_vecs_put_calls += 1;
                }
                self.write_profile.ns_vecs_key_bytes += key_bytes;
                self.write_profile.ns_vecs_value_bytes += value_bytes;
            },
        }
    }

    fn noteNamespaceDelete(self: *HBCIndex, namespace: Namespace, key_len: usize) void {
        const key_bytes: u64 = @intCast(key_len);
        switch (namespace) {
            .nodes => {
                self.write_profile.ns_nodes_delete_calls += 1;
                self.write_profile.ns_nodes_key_bytes += key_bytes;
            },
            .meta => {
                self.write_profile.ns_meta_delete_calls += 1;
                self.write_profile.ns_meta_key_bytes += key_bytes;
            },
            .quant => {
                self.write_profile.ns_quant_delete_calls += 1;
                self.write_profile.ns_quant_key_bytes += key_bytes;
            },
            .vecs => {
                self.write_profile.ns_vecs_delete_calls += 1;
                self.write_profile.ns_vecs_key_bytes += key_bytes;
            },
        }
    }

    pub fn openNamespacedCursor(self: *HBCIndex, allocator: Allocator, txn: anytype, comptime namespace: Namespace) !vectorindex_store.Cursor {
        _ = self;
        _ = allocator;
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceReadTxn,
            vectorindex_store.NamespaceWriteTxn,
            => return try txn.openCursor(namespace),
            else => @compileError("expected vectorindex namespace transaction"),
        }
    }

    /// Authoritative native HBC transactions are deliberately only lifetime
    /// shells. Reads resolve through the immutable generation lease and
    /// complete mutations through the HBC WAL overlay. Reaching this vtable
    /// means a key escaped the native ownership map, so fail closed.
    const NativeTxnShell = struct {
        fn report(ptr: *anyopaque, operation: []const u8, namespace: Namespace, key: []const u8) void {
            const index: *HBCIndex = @ptrCast(@alignCast(ptr));
            _ = index;
            std.log.err("HBC native transaction encountered unowned key operation={s} namespace={s} key_len={} key_prefix=0x{x}", .{
                operation,
                @tagName(namespace),
                key.len,
                if (key.len > 0) key[0] else 0,
            });
        }
        fn abort(_: Allocator, _: *anyopaque) void {}
        fn commit(_: Allocator, _: *anyopaque) anyerror!void {}
        fn get(ptr: *anyopaque, namespace: Namespace, key: []const u8) anyerror![]const u8 {
            report(ptr, "get", namespace, key);
            return error.HbcNativeUnownedKey;
        }
        fn getManySorted(ptr: *anyopaque, namespace: Namespace, keys: []const []const u8, _: []?[]const u8) anyerror!void {
            report(ptr, "get_many_sorted", namespace, if (keys.len > 0) keys[0] else &.{});
            return error.HbcNativeUnownedKey;
        }
        fn put(ptr: *anyopaque, namespace: Namespace, key: []const u8, _: []const u8) anyerror!void {
            report(ptr, "put", namespace, key);
            return error.HbcNativeUnownedKey;
        }
        fn delete(ptr: *anyopaque, namespace: Namespace, key: []const u8) anyerror!void {
            report(ptr, "delete", namespace, key);
            return error.HbcNativeUnownedKey;
        }

        const read_vtable: vectorindex_store.NamespaceReadTxn.VTable = .{
            .abort = abort,
            .get = get,
            .get_many_sorted = getManySorted,
        };
        const write_vtable: vectorindex_store.NamespaceWriteTxn.VTable = .{
            .abort = abort,
            .commit = commit,
            .get = get,
            .get_many_sorted = getManySorted,
            .put = put,
            .delete = NativeTxnShell.delete,
        };
        const batch_vtable: vectorindex_store.NamespaceBatch.VTable = .{
            .abort = abort,
            .commit = commit,
            .get = get,
            .get_many_sorted = getManySorted,
            .put = put,
            .delete = NativeTxnShell.delete,
        };
    };

    fn nativeReadTxn(self: *HBCIndex) vectorindex_store.NamespaceReadTxn {
        return .{ .allocator = self.alloc, .ptr = self, .vtable = &NativeTxnShell.read_vtable };
    }

    fn nativeWriteTxn(self: *HBCIndex) vectorindex_store.NamespaceWriteTxn {
        return .{ .allocator = self.alloc, .ptr = self, .vtable = &NativeTxnShell.write_vtable };
    }

    fn nativeBatchTxn(self: *HBCIndex) vectorindex_store.NamespaceBatch {
        return .{ .allocator = self.alloc, .ptr = self, .vtable = &NativeTxnShell.batch_vtable };
    }

    pub fn beginRuntimeReadTxn(self: *HBCIndex) !vectorindex_store.NamespaceReadTxn {
        while (true) {
            const generation = self.acquireExperimentalPostingReadGeneration();
            if (generation == null and self.experimental_posting_wal_authoritative.load(.acquire)) {
                return error.PostingWalMutationStoreUnavailable;
            }
            if (generation) |acquired| if (self.nativeHbcAuthoritative()) {
                var txn = self.nativeReadTxn();
                txn.read_lease = .{
                    .ptr = acquired,
                    .release = ExperimentalPostingReadGeneration.releaseOpaque,
                };
                return txn;
            };
            var txn = self.store.beginRead() catch |err| {
                if (generation) |acquired| acquired.release();
                return err;
            };
            if (generation) |acquired| {
                if (!self.experimentalPostingReadGenerationIsCurrent(acquired)) {
                    acquired.release();
                    txn.abort();
                    continue;
                }
                txn.read_lease = .{
                    .ptr = acquired,
                    .release = ExperimentalPostingReadGeneration.releaseOpaque,
                };
            }
            return txn;
        }
    }

    pub fn beginRuntimeSearchTxn(self: *HBCIndex) !vectorindex_store.NamespaceReadTxn {
        while (true) {
            const generation = self.acquireExperimentalPostingReadGeneration();
            if (generation == null and self.experimental_posting_wal_authoritative.load(.acquire)) {
                return error.PostingWalMutationStoreUnavailable;
            }
            if (generation) |acquired| if (self.nativeHbcAuthoritative()) {
                var txn = self.nativeReadTxn();
                txn.read_lease = .{
                    .ptr = acquired,
                    .release = ExperimentalPostingReadGeneration.releaseOpaque,
                };
                return txn;
            };
            var txn = self.store.beginProbeOrRead() catch |err| {
                if (generation) |acquired| acquired.release();
                return err;
            };
            if (generation) |acquired| {
                if (!self.experimentalPostingReadGenerationIsCurrent(acquired)) {
                    acquired.release();
                    txn.abort();
                    continue;
                }
                txn.read_lease = .{
                    .ptr = acquired,
                    .release = ExperimentalPostingReadGeneration.releaseOpaque,
                };
            }
            return txn;
        }
    }

    pub fn beginRuntimeWriteTxn(self: *HBCIndex) !vectorindex_store.NamespaceWriteTxn {
        if (self.crossBatchPublicationActive()) self.bulk_publication_may_have_mutated = true;
        if (!self.crossBatchPublicationActive()) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = self.bulk_split_vector_workspace.accounted_bytes;
            self.observeApplyWorkspaceBytes();
        }
        var txn = if (self.nativeHbcAuthoritative() and
            self.experimental_posting_mutation_base_generation != null)
            self.nativeWriteTxn()
        else
            try self.store.beginWrite();
        errdefer txn.abort();
        try self.markExperimentalPostingWalAuthoritative(&txn);
        return txn;
    }

    pub fn beginRuntimeBatchTxn(self: *HBCIndex) !vectorindex_store.NamespaceBatch {
        if (self.crossBatchPublicationActive()) self.bulk_publication_may_have_mutated = true;
        if (!self.crossBatchPublicationActive()) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = self.bulk_split_vector_workspace.accounted_bytes;
            self.observeApplyWorkspaceBytes();
        }
        var txn = if (self.nativeHbcAuthoritative() and
            self.experimental_posting_mutation_base_generation != null)
            self.nativeBatchTxn()
        else
            try self.store.beginBatch();
        errdefer txn.abort();
        try self.markExperimentalPostingWalAuthoritative(&txn);
        return txn;
    }

    pub fn beginRuntimeBatchTxnOptions(self: *HBCIndex, options: BatchInsertOptions) !vectorindex_store.NamespaceBatch {
        if (self.crossBatchPublicationActive()) self.bulk_publication_may_have_mutated = true;
        if (!self.shouldDeferQuantizedRebuildToBulkFinish(options)) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
        }
        if (!self.crossBatchPublicationActive()) {
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = self.bulk_split_vector_workspace.accounted_bytes;
            self.observeApplyWorkspaceBytes();
        }
        const in_bulk_session = self.lsmSessionBatchingActive();
        // HBC mutation batches rewrite nodes, ranges, and quantized payloads
        // heavily. Keep mutable-state coalescing, but preserve bulk transaction
        // mode when the LSM profile disables direct bulk ingest so LSM defers
        // batch-exit maintenance until the session boundary. Direct-bulk LSM
        // profiles and non-LSM backends stay in default mode; otherwise stale
        // internal rewrites can become durable table bytes during large loads.
        var txn = if (self.nativeHbcAuthoritative() and
            self.experimental_posting_mutation_base_generation != null)
            self.nativeBatchTxn()
        else
            try self.store.beginBatchWithOptions(.{
                .mode = self.runtimeBatchMode(in_bulk_session),
                .defer_commit_flush = in_bulk_session,
            });
        errdefer txn.abort();
        try self.markExperimentalPostingWalAuthoritative(&txn);
        return txn;
    }

    fn markExperimentalPostingWalAuthoritative(self: *HBCIndex, txn: anytype) !void {
        if (self.experimental_posting_mutation_base_generation == null or
            self.experimental_posting_wal_authoritative_persisted) return;
        if (self.experimental_posting_wal_authoritative.load(.acquire)) {
            self.experimental_posting_wal_authoritative_persisted = true;
            return;
        }
        try txn.put(.meta, posting_wal_authoritative_key, posting_wal_authoritative_value);
        // This is conservatively sticky in memory even if the surrounding
        // transaction later aborts. The next writable transaction stages the
        // same idempotent marker again, while startup trusts only committed
        // storage.
        self.experimental_posting_wal_authoritative.store(true, .release);
    }

    fn noteExperimentalPostingWalAuthorityCommit(self: *HBCIndex) void {
        if (self.experimental_posting_mutation_base_generation != null and
            self.experimental_posting_wal_authoritative.load(.acquire))
        {
            self.experimental_posting_wal_authoritative_persisted = true;
        }
    }

    fn runtimeBatchMode(self: *const HBCIndex, in_bulk_session: bool) vectorindex_store.BatchMode {
        return switch (self.env_owner) {
            .lsm => |handle| hbcRuntimeBatchMode(in_bulk_session, handle.backend.options.direct_bulk_ingest),
            .lmdb, .native => hbcRuntimeBatchMode(in_bulk_session, null),
        };
    }

    fn commitTxn(txn: anytype) !void {
        const Child = comptime txnLikeChild(@TypeOf(txn));
        switch (Child) {
            vectorindex_store.NamespaceWriteTxn, vectorindex_store.NamespaceBatch => try txn.commit(),
            else => @compileError("expected writable transaction with commit()"),
        }
    }

    fn clearLocalNodeCacheLocked(self: *HBCIndex) void {
        var pinned_it = self.pinned_node_cache.iterator();
        while (pinned_it.next()) |entry| releaseNodeCacheEntry(self.alloc, entry.value_ptr.*);
        self.pinned_node_cache.deinit(self.alloc);
        self.pinned_node_cache = .empty;

        var it = self.node_cache.iterator();
        while (it.next()) |entry| releaseNodeCacheEntry(self.alloc, entry.value_ptr.*);
        self.node_cache.deinit(self.alloc);
        self.node_cache = .empty;
        self.node_cache_slots.deinit(self.alloc);
        self.node_cache_slots = .empty;
        @memset(self.node_clock_keys, 0);
        @memset(self.node_clock_refs, false);
        self.node_clock_hand = 0;
    }

    fn clearLocalQuantizedCacheLocked(self: *HBCIndex) void {
        var pinned_it = self.pinned_quantized_cache.iterator();
        while (pinned_it.next()) |entry| releaseQuantizedCacheEntry(self.alloc, entry.value_ptr.*);
        self.pinned_quantized_cache.deinit(self.alloc);
        self.pinned_quantized_cache = .empty;

        var it = self.quantized_cache.iterator();
        while (it.next()) |entry| releaseQuantizedCacheEntry(self.alloc, entry.value_ptr.*);
        self.quantized_cache.deinit(self.alloc);
        self.quantized_cache = .empty;
        self.quantized_cache_slots.deinit(self.alloc);
        self.quantized_cache_slots = .empty;
        @memset(self.quantized_clock_keys, 0);
        @memset(self.quantized_clock_refs, false);
        self.quantized_clock_hand = 0;
    }

    fn clearLocalVectorCacheLocked(self: *HBCIndex) void {
        var it = self.vector_cache.iterator();
        while (it.next()) |entry| releaseVectorCacheEntry(self.alloc, entry.value_ptr.*);
        self.vector_cache.deinit(self.alloc);
        self.vector_cache = .empty;
        self.vector_cache_slots.deinit(self.alloc);
        self.vector_cache_slots = .empty;
        @memset(self.vector_clock_keys, 0);
        @memset(self.vector_clock_refs, false);
        self.vector_clock_hand = 0;
    }

    fn clearLocalMetadataCacheLocked(self: *HBCIndex) void {
        var it = self.metadata_cache.iterator();
        while (it.next()) |entry| releaseMetadataCacheEntry(self.alloc, entry.value_ptr.*);
        self.metadata_cache.deinit(self.alloc);
        self.metadata_cache = .empty;
        self.metadata_cache_slots.deinit(self.alloc);
        self.metadata_cache_slots = .empty;
        @memset(self.metadata_clock_keys, 0);
        @memset(self.metadata_clock_refs, false);
        self.metadata_clock_hand = 0;
    }

    fn invalidateLocalNodeCacheLocked(self: *HBCIndex, node_id: u64) void {
        if (self.pinned_node_cache.fetchRemove(node_id)) |removed| releaseNodeCacheEntry(self.alloc, removed.value);
        if (self.node_cache_slots.fetchRemove(node_id)) |removed_slot| {
            self.node_clock_keys[removed_slot.value] = 0;
            self.node_clock_refs[removed_slot.value] = false;
        }
        if (self.node_cache.fetchRemove(node_id)) |removed| releaseNodeCacheEntry(self.alloc, removed.value);
    }

    fn invalidateLocalQuantizedCacheLocked(self: *HBCIndex, node_id: u64) void {
        if (self.pinned_quantized_cache.fetchRemove(node_id)) |removed| releaseQuantizedCacheEntry(self.alloc, removed.value);
        if (self.quantized_cache_slots.fetchRemove(node_id)) |removed_slot| {
            self.quantized_clock_keys[removed_slot.value] = 0;
            self.quantized_clock_refs[removed_slot.value] = false;
        }
        if (self.quantized_cache.fetchRemove(node_id)) |removed| releaseQuantizedCacheEntry(self.alloc, removed.value);
    }

    fn invalidateLocalVectorCacheLocked(self: *HBCIndex, vector_id: u64) void {
        if (self.vector_cache_slots.fetchRemove(vector_id)) |removed_slot| {
            self.vector_clock_keys[removed_slot.value] = 0;
            self.vector_clock_refs[removed_slot.value] = false;
        }
        if (self.vector_cache.fetchRemove(vector_id)) |removed| releaseVectorCacheEntry(self.alloc, removed.value);
    }

    fn invalidateLocalMetadataCacheLocked(self: *HBCIndex, vector_id: u64) void {
        if (self.metadata_cache_slots.fetchRemove(vector_id)) |removed_slot| {
            self.metadata_clock_keys[removed_slot.value] = 0;
            self.metadata_clock_refs[removed_slot.value] = false;
        }
        if (self.metadata_cache.fetchRemove(vector_id)) |removed| releaseMetadataCacheEntry(self.alloc, removed.value);
    }

    fn ensureLocalNodeCacheCapacityLocked(self: *HBCIndex, key: u64) ?usize {
        if (self.config.max_cached_nodes == 0) return null;
        if (self.node_cache.contains(key)) return null;
        while (self.node_cache.count() >= self.config.max_cached_nodes) {
            const victim = nextHbcClockVictim(self.node_clock_keys, self.node_clock_refs, &self.node_clock_hand, .none(), .node) orelse break;
            const slot = self.node_cache_slots.get(victim).?;
            if (self.node_cache_slots.fetchRemove(victim)) |removed_slot| {
                self.node_clock_keys[removed_slot.value] = 0;
                self.node_clock_refs[removed_slot.value] = false;
            }
            if (self.node_cache.fetchRemove(victim)) |removed| releaseNodeCacheEntry(self.alloc, removed.value);
            return slot;
        }
        return null;
    }

    fn ensureLocalQuantizedCacheCapacityLocked(self: *HBCIndex, key: u64) ?usize {
        if (self.config.max_cached_nodes == 0) return null;
        if (self.quantized_cache.contains(key)) return null;
        while (self.quantized_cache.count() >= self.config.max_cached_nodes) {
            const victim = nextHbcClockVictim(self.quantized_clock_keys, self.quantized_clock_refs, &self.quantized_clock_hand, .none(), .quantized) orelse break;
            const slot = self.quantized_cache_slots.get(victim).?;
            if (self.quantized_cache_slots.fetchRemove(victim)) |removed_slot| {
                self.quantized_clock_keys[removed_slot.value] = 0;
                self.quantized_clock_refs[removed_slot.value] = false;
            }
            if (self.quantized_cache.fetchRemove(victim)) |removed| releaseQuantizedCacheEntry(self.alloc, removed.value);
            return slot;
        }
        return null;
    }

    fn ensureLocalVectorCacheCapacityLocked(self: *HBCIndex, key: u64) ?usize {
        if (self.config.max_cached_vectors == 0) return null;
        if (self.vector_cache.contains(key)) return null;
        while (self.vector_cache.count() >= self.config.max_cached_vectors) {
            const victim = nextHbcClockVictim(self.vector_clock_keys, self.vector_clock_refs, &self.vector_clock_hand, .none(), .vector) orelse break;
            const slot = self.vector_cache_slots.get(victim).?;
            self.invalidateLocalVectorCacheLocked(victim);
            return slot;
        }
        return null;
    }

    fn ensureLocalMetadataCacheCapacityLocked(self: *HBCIndex, key: u64) ?usize {
        if (self.config.max_cached_metadata == 0) return null;
        if (self.metadata_cache.contains(key)) return null;
        while (self.metadata_cache.count() >= self.config.max_cached_metadata) {
            const victim = nextHbcClockVictim(self.metadata_clock_keys, self.metadata_clock_refs, &self.metadata_clock_hand, .none(), .metadata) orelse break;
            const slot = self.metadata_cache_slots.get(victim).?;
            self.invalidateLocalMetadataCacheLocked(victim);
            return slot;
        }
        return null;
    }

    fn cacheNodeLocalLocked(self: *HBCIndex, node: Node) !*const Node {
        var owned = node;
        errdefer owned.deinit(self.alloc);
        const reserved_slot = self.ensureLocalNodeCacheCapacityLocked(owned.id);
        if (self.node_cache_slots.fetchRemove(owned.id)) |removed_slot| {
            self.node_clock_keys[removed_slot.value] = 0;
            self.node_clock_refs[removed_slot.value] = false;
        }
        if (self.node_cache.fetchRemove(owned.id)) |removed| releaseNodeCacheEntry(self.alloc, removed.value);
        const entry = try self.alloc.create(NodeCacheEntry);
        errdefer self.alloc.destroy(entry);
        entry.* = .{ .node = owned };
        errdefer releaseNodeCacheEntry(self.alloc, entry);
        try self.node_cache.put(self.alloc, owned.id, entry);
        const slot = reserved_slot orelse claimLocalClockSlot(self.node_clock_keys, self.node_clock_hand, owned.id) orelse return error.CacheDisabled;
        self.node_clock_refs[slot] = true;
        try self.node_cache_slots.put(self.alloc, owned.id, slot);
        return &entry.node;
    }

    fn cacheQuantizedLocalLocked(self: *HBCIndex, node_id: u64, qs: QuantizedSet) !*const QuantizedSet {
        var owned = qs;
        errdefer owned.deinit(self.alloc);
        const reserved_slot = self.ensureLocalQuantizedCacheCapacityLocked(node_id);
        if (self.quantized_cache_slots.fetchRemove(node_id)) |removed_slot| {
            self.quantized_clock_keys[removed_slot.value] = 0;
            self.quantized_clock_refs[removed_slot.value] = false;
        }
        if (self.quantized_cache.fetchRemove(node_id)) |removed| releaseQuantizedCacheEntry(self.alloc, removed.value);
        const entry = try self.alloc.create(QuantizedCacheEntry);
        errdefer self.alloc.destroy(entry);
        entry.* = .{ .quantized = owned };
        errdefer releaseQuantizedCacheEntry(self.alloc, entry);
        try self.quantized_cache.put(self.alloc, node_id, entry);
        const slot = reserved_slot orelse claimLocalClockSlot(self.quantized_clock_keys, self.quantized_clock_hand, node_id) orelse return error.CacheDisabled;
        self.quantized_clock_refs[slot] = true;
        try self.quantized_cache_slots.put(self.alloc, node_id, slot);
        return &entry.quantized;
    }

    fn cachePinnedNodeLocked(self: *HBCIndex, node: *const Node, replace_existing: bool) !void {
        if (self.config.max_pinned_tree_nodes == 0) return;
        if (self.pinned_node_cache.get(node.id)) |_| {
            if (!replace_existing) return;
            if (self.pinned_node_cache.fetchRemove(node.id)) |removed| releaseNodeCacheEntry(self.alloc, removed.value);
        } else if (self.pinned_node_cache.count() >= self.config.max_pinned_tree_nodes) {
            return;
        }

        var cloned = try node.clone(self.alloc);
        var cloned_active = true;
        errdefer if (cloned_active) cloned.deinit(self.alloc);
        const entry = try self.alloc.create(NodeCacheEntry);
        entry.* = .{ .node = cloned };
        cloned_active = false;
        errdefer releaseNodeCacheEntry(self.alloc, entry);
        try self.pinned_node_cache.put(self.alloc, node.id, entry);
    }

    fn cachePinnedQuantizedOwnedLocked(self: *HBCIndex, node_id: u64, qs: QuantizedSet, replace_existing: bool) !void {
        if (self.config.max_pinned_tree_nodes == 0) {
            var owned = qs;
            owned.deinit(self.alloc);
            return;
        }
        var owned = qs;
        var owned_active = true;
        errdefer if (owned_active) owned.deinit(self.alloc);
        if (self.pinned_quantized_cache.get(node_id)) |_| {
            if (!replace_existing) {
                owned.deinit(self.alloc);
                owned_active = false;
                return;
            }
            if (self.pinned_quantized_cache.fetchRemove(node_id)) |removed| releaseQuantizedCacheEntry(self.alloc, removed.value);
        } else if (self.pinned_quantized_cache.count() >= self.config.max_pinned_tree_nodes) {
            owned.deinit(self.alloc);
            owned_active = false;
            return;
        }

        const entry = try self.alloc.create(QuantizedCacheEntry);
        entry.* = .{ .quantized = owned };
        owned_active = false;
        errdefer releaseQuantizedCacheEntry(self.alloc, entry);
        try self.pinned_quantized_cache.put(self.alloc, node_id, entry);
    }

    fn cacheVectorLocalLocked(self: *HBCIndex, vector_id: u64, vector_data: []const f32) ![]const f32 {
        if (self.vector_cache.get(vector_id)) |existing| {
            if (existing.vector.ptr == vector_data.ptr and existing.vector.len == vector_data.len) return existing.vector;
        }
        const reserved_slot = self.ensureLocalVectorCacheCapacityLocked(vector_id);
        self.invalidateLocalVectorCacheLocked(vector_id);
        const copied = try self.alloc.dupe(f32, vector_data);
        errdefer self.alloc.free(copied);
        const entry = try self.alloc.create(VectorCacheEntry);
        errdefer self.alloc.destroy(entry);
        entry.* = .{ .vector = copied };
        errdefer releaseVectorCacheEntry(self.alloc, entry);
        try self.vector_cache.put(self.alloc, vector_id, entry);
        const slot = reserved_slot orelse claimLocalClockSlot(self.vector_clock_keys, self.vector_clock_hand, vector_id) orelse return error.CacheDisabled;
        self.vector_clock_refs[slot] = true;
        try self.vector_cache_slots.put(self.alloc, vector_id, slot);
        return entry.vector;
    }

    fn cacheMetadataLocalLocked(self: *HBCIndex, vector_id: u64, metadata: []const u8) ![]const u8 {
        const reserved_slot = self.ensureLocalMetadataCacheCapacityLocked(vector_id);
        self.invalidateLocalMetadataCacheLocked(vector_id);
        const copied = try self.alloc.dupe(u8, metadata);
        errdefer self.alloc.free(copied);
        const entry = try self.alloc.create(MetadataCacheEntry);
        errdefer self.alloc.destroy(entry);
        entry.* = .{ .metadata = copied };
        errdefer releaseMetadataCacheEntry(self.alloc, entry);
        try self.metadata_cache.put(self.alloc, vector_id, entry);
        const slot = reserved_slot orelse claimLocalClockSlot(self.metadata_clock_keys, self.metadata_clock_hand, vector_id) orelse return error.CacheDisabled;
        self.metadata_clock_refs[slot] = true;
        try self.metadata_cache_slots.put(self.alloc, vector_id, slot);
        return entry.metadata;
    }

    fn clearNodeCache(self: *HBCIndex) void {
        if (self.shared_cache) |cache| {
            cache.clearNodeNamespace(self.cache_namespace);
            self.cache_mu.lockExclusive();
            defer self.cache_mu.unlockExclusive();
            self.clearLocalNodeCacheLocked();
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.clearLocalNodeCacheLocked();
        self.refreshHbcCacheUsage();
    }

    fn clearQuantizedCache(self: *HBCIndex) void {
        if (self.shared_cache) |cache| {
            cache.clearQuantizedNamespace(self.cache_namespace);
            self.cache_mu.lockExclusive();
            defer self.cache_mu.unlockExclusive();
            self.clearLocalQuantizedCacheLocked();
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.clearLocalQuantizedCacheLocked();
        self.refreshHbcCacheUsage();
    }

    fn clearVectorCache(self: *HBCIndex) void {
        if (self.shared_cache) |cache| {
            cache.clearVectorNamespace(self.cache_namespace);
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.clearLocalVectorCacheLocked();
        self.refreshHbcCacheUsage();
    }

    pub fn clearMetadataCache(self: *HBCIndex) void {
        if (self.shared_cache) |cache| {
            cache.clearMetadataNamespace(self.cache_namespace);
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.clearLocalMetadataCacheLocked();
        self.refreshHbcCacheUsage();
    }

    pub fn invalidateNodeCache(self: *HBCIndex, node_id: u64) void {
        if (self.shared_cache) |cache| {
            cache.invalidateNode(self.cache_namespace, node_id);
            self.cache_mu.lockExclusive();
            defer self.cache_mu.unlockExclusive();
            self.invalidateLocalNodeCacheLocked(node_id);
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.invalidateLocalNodeCacheLocked(node_id);
        self.refreshHbcCacheUsage();
    }

    pub fn invalidateQuantizedCache(self: *HBCIndex, node_id: u64) void {
        if (self.shared_cache) |cache| {
            cache.invalidateQuantized(self.cache_namespace, node_id);
            self.cache_mu.lockExclusive();
            defer self.cache_mu.unlockExclusive();
            self.invalidateLocalQuantizedCacheLocked(node_id);
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.invalidateLocalQuantizedCacheLocked(node_id);
        self.refreshHbcCacheUsage();
    }

    pub fn invalidateVectorCache(self: *HBCIndex, vector_id: u64) void {
        if (self.shared_cache) |cache| {
            cache.invalidateVector(self.cache_namespace, vector_id);
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.invalidateLocalVectorCacheLocked(vector_id);
        self.refreshHbcCacheUsage();
    }

    pub fn invalidateMetadataCache(self: *HBCIndex, vector_id: u64) void {
        if (self.shared_cache) |cache| {
            cache.invalidateMetadata(self.cache_namespace, vector_id);
            return;
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        self.invalidateLocalMetadataCacheLocked(vector_id);
        self.refreshHbcCacheUsage();
    }

    pub fn cacheNode(self: *HBCIndex, node: *const Node) !void {
        if (!self.cache_enabled) return;
        {
            self.cache_mu.lockExclusive();
            defer self.cache_mu.unlockExclusive();
            if (self.pinned_node_cache.contains(node.id)) {
                try self.cachePinnedNodeLocked(node, true);
            }
        }
        if (self.active_searches.load(.acquire) > 1) return;
        if (self.shared_cache) |cache| {
            if (self.config.max_cached_nodes == 0) return;
            _ = try cache.cacheNode(self.cache_namespace, node);
            return;
        }
        const cloned = try node.clone(self.alloc);
        errdefer {
            var owned = cloned;
            owned.deinit(self.alloc);
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        var admission = self.prepareHbcCacheAdmission(.node, node.id, estimateNodeCacheBytes(node)) orelse {
            self.noteHbcCacheAdmissionSkip(.node);
            var owned = cloned;
            owned.deinit(self.alloc);
            return;
        };
        errdefer {
            admission.rollback();
            self.refreshHbcCacheUsage();
        }
        _ = try self.cacheNodeLocalLocked(cloned);
        self.noteHbcCacheInsertion(.node);
        admission.commit();
        self.refreshAndEnforceHbcCacheUsage(.one(.node, node.id));
    }

    pub fn cacheSearchNode(self: *HBCIndex, node: *const Node) !void {
        if (self.lsmSessionBatchingActive()) return;
        try self.cacheNode(node);
    }

    fn cacheNodeOwned(self: *HBCIndex, node: Node) !*const Node {
        if (!self.cache_enabled) {
            var owned = node;
            owned.deinit(self.alloc);
            return error.CacheDisabled;
        }
        if (self.active_searches.load(.acquire) > 1) {
            var owned = node;
            owned.deinit(self.alloc);
            return error.CacheDisabled;
        }
        if (self.shared_cache) |cache| {
            if (self.config.max_cached_nodes == 0) {
                var owned = node;
                owned.deinit(self.alloc);
                return error.CacheDisabled;
            }
            return try cache.cacheNodeOwned(self.cache_namespace, node);
        }
        const node_id = node.id;
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        var admission = self.prepareHbcCacheAdmission(.node, node_id, estimateNodeCacheBytes(&node)) orelse HbcCacheAdmission.none(self);
        errdefer {
            admission.rollback();
            self.refreshHbcCacheUsage();
        }
        const cached = try self.cacheNodeLocalLocked(node);
        self.noteHbcCacheInsertion(.node);
        admission.commit();
        self.refreshAndEnforceHbcCacheUsage(.one(.node, node_id));
        return cached;
    }

    pub fn cacheQuantized(self: *HBCIndex, node_id: u64, qs: *const QuantizedSet) !void {
        if (!self.cache_enabled) return;
        {
            self.cache_mu.lockExclusive();
            defer self.cache_mu.unlockExclusive();
            if (self.pinned_quantized_cache.contains(node_id)) {
                try self.cachePinnedQuantizedOwnedLocked(node_id, try qs.clone(self.alloc), true);
            }
        }
        if (self.active_searches.load(.acquire) > 1) return;
        if (self.shared_cache) |cache| {
            if (self.config.max_cached_nodes == 0) return;
            _ = try cache.cacheQuantized(self.cache_namespace, node_id, qs);
            return;
        }
        var cloned = try qs.clone(self.alloc);
        errdefer cloned.deinit(self.alloc);
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        var admission = self.prepareHbcCacheAdmission(.quantized, node_id, estimateQuantizedCacheBytes(qs)) orelse {
            self.noteHbcCacheAdmissionSkip(.quantized);
            cloned.deinit(self.alloc);
            return;
        };
        errdefer {
            admission.rollback();
            self.refreshHbcCacheUsage();
        }
        _ = try self.cacheQuantizedLocalLocked(node_id, cloned);
        self.noteHbcCacheInsertion(.quantized);
        admission.commit();
        self.refreshAndEnforceHbcCacheUsage(.one(.quantized, node_id));
    }

    pub fn cacheQuantizedOwned(self: *HBCIndex, node_id: u64, qs: QuantizedSet) !*const QuantizedSet {
        if (!self.cache_enabled) {
            var owned = qs;
            owned.deinit(self.alloc);
            return error.CacheDisabled;
        }
        if (self.active_searches.load(.acquire) > 1) {
            var owned = qs;
            owned.deinit(self.alloc);
            return error.CacheDisabled;
        }
        if (self.shared_cache) |cache| {
            if (self.config.max_cached_nodes == 0) {
                var owned = qs;
                owned.deinit(self.alloc);
                return error.CacheDisabled;
            }
            return try cache.cacheQuantizedOwned(self.cache_namespace, node_id, qs);
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        var admission = self.prepareHbcCacheAdmission(.quantized, node_id, estimateQuantizedCacheBytes(&qs)) orelse HbcCacheAdmission.none(self);
        errdefer {
            admission.rollback();
            self.refreshHbcCacheUsage();
        }
        const cached = try self.cacheQuantizedLocalLocked(node_id, qs);
        self.noteHbcCacheInsertion(.quantized);
        admission.commit();
        self.refreshAndEnforceHbcCacheUsage(.one(.quantized, node_id));
        return cached;
    }

    pub fn cacheVector(self: *HBCIndex, vector_id: u64, vector_data: []const f32) ![]const f32 {
        if (!self.cache_enabled) return vector_data;
        if (!self.retained_vector_cache_enabled) return vector_data;
        if (self.bypass_external_vector_cache) return vector_data;
        if (self.lsmSessionBatchingActive()) return vector_data;
        if (self.active_searches.load(.acquire) > 1) return vector_data;
        return try self.cacheVectorRetained(vector_id, vector_data);
    }

    pub fn cacheVectorForWarmup(self: *HBCIndex, vector_id: u64, vector_data: []const f32) ![]const f32 {
        if (!self.cache_enabled) return vector_data;
        if (!self.retained_vector_cache_enabled) return vector_data;
        if (self.bypass_external_vector_cache) return vector_data;
        if (self.active_searches.load(.acquire) > 1) return vector_data;
        return try self.cacheVectorRetained(vector_id, vector_data);
    }

    fn cacheVectorRetained(self: *HBCIndex, vector_id: u64, vector_data: []const f32) ![]const f32 {
        if (self.shared_cache) |cache| {
            if (self.config.max_cached_vectors == 0) return vector_data;
            return try cache.cacheVector(self.cache_namespace, vector_id, vector_data);
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        var admission = self.prepareHbcCacheAdmission(.vector, vector_id, estimateVectorCacheBytes(vector_data)) orelse {
            self.noteHbcCacheAdmissionSkip(.vector);
            return vector_data;
        };
        errdefer {
            admission.rollback();
            self.refreshHbcCacheUsage();
        }
        const cached = try self.cacheVectorLocalLocked(vector_id, vector_data);
        self.noteHbcCacheInsertion(.vector);
        admission.commit();
        self.refreshAndEnforceHbcCacheUsage(.one(.vector, vector_id));
        return cached;
    }

    pub fn cacheMetadata(self: *HBCIndex, vector_id: u64, metadata: []const u8) ![]const u8 {
        if (!self.cache_enabled) return metadata;
        if (self.lsmSessionBatchingActive()) return metadata;
        if (self.config.max_cached_metadata == 0) return metadata;
        if (self.shared_cache) |cache| {
            return try cache.cacheMetadata(self.cache_namespace, vector_id, metadata);
        }
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        var admission = self.prepareHbcCacheAdmission(.metadata, vector_id, estimateMetadataCacheBytes(metadata)) orelse {
            self.noteHbcCacheAdmissionSkip(.metadata);
            return metadata;
        };
        errdefer {
            admission.rollback();
            self.refreshHbcCacheUsage();
        }
        const cached = try self.cacheMetadataLocalLocked(vector_id, metadata);
        self.noteHbcCacheInsertion(.metadata);
        admission.commit();
        self.refreshAndEnforceHbcCacheUsage(.one(.metadata, vector_id));
        return cached;
    }

    pub fn getCachedNodePtr(self: *HBCIndex, node_id: u64) ?*const Node {
        if (!self.cache_enabled) return null;
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        if (self.pinned_node_cache.get(node_id)) |entry| return &entry.node;
        if (self.shared_cache) |cache| return cache.getNodePtr(self.cache_namespace, node_id);
        if (self.node_cache.get(node_id)) |entry| return &entry.node;
        return null;
    }

    pub fn getCachedNodeClone(self: *HBCIndex, node_id: u64) !?Node {
        if (!self.cache_enabled) return null;
        self.cache_mu.lockShared();
        if (self.pinned_node_cache.get(node_id)) |entry| {
            const cloned = entry.node.clone(self.alloc) catch |err| {
                self.cache_mu.unlockShared();
                return err;
            };
            self.cache_mu.unlockShared();
            return cloned;
        }
        if (self.shared_cache == null) {
            if (self.node_cache.get(node_id)) |entry| {
                const cloned = entry.node.clone(self.alloc) catch |err| {
                    self.cache_mu.unlockShared();
                    return err;
                };
                self.cache_mu.unlockShared();
                return cloned;
            }
        }
        self.cache_mu.unlockShared();
        if (self.shared_cache) |cache| return try cache.cloneNode(self.cache_namespace, node_id);
        return null;
    }

    pub fn borrowCachedNode(self: *HBCIndex, node_id: u64) ?BorrowedNode {
        if (!self.cache_enabled) return null;
        self.cache_mu.lockShared();
        if (self.pinned_node_cache.get(node_id)) |entry| {
            retainNodeCacheEntry(entry);
            self.cache_mu.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        if (self.shared_cache != null) {
            self.cache_mu.unlockShared();
            if (self.shared_cache) |cache| return cache.borrowNode(self.cache_namespace, node_id);
            return null;
        }
        if (self.node_cache.get(node_id)) |entry| {
            retainNodeCacheEntry(entry);
            self.cache_mu.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.cache_mu.unlockShared();
        return null;
    }

    pub fn borrowCachedNodeForSearch(self: *HBCIndex, node_id: u64) ?BorrowedNode {
        if (self.lsmSessionBatchingActive()) return null;
        return self.borrowCachedNode(node_id);
    }

    pub fn getCachedQuantizedPtr(self: *HBCIndex, node_id: u64) ?*QuantizedSet {
        if (!self.cache_enabled) return null;
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        if (self.pinned_quantized_cache.get(node_id)) |entry| return &entry.quantized;
        if (self.shared_cache) |cache| return cache.getQuantizedPtr(self.cache_namespace, node_id);
        if (self.quantized_cache.get(node_id)) |entry| return &entry.quantized;
        return null;
    }

    pub fn getCachedQuantizedClone(self: *HBCIndex, node_id: u64) !?QuantizedSet {
        if (!self.cache_enabled) return null;
        self.cache_mu.lockShared();
        if (self.pinned_quantized_cache.get(node_id)) |entry| {
            const cloned = entry.quantized.clone(self.alloc) catch |err| {
                self.cache_mu.unlockShared();
                return err;
            };
            self.cache_mu.unlockShared();
            return cloned;
        }
        if (self.shared_cache == null) {
            if (self.quantized_cache.get(node_id)) |entry| {
                const cloned = entry.quantized.clone(self.alloc) catch |err| {
                    self.cache_mu.unlockShared();
                    return err;
                };
                self.cache_mu.unlockShared();
                return cloned;
            }
        }
        self.cache_mu.unlockShared();
        if (self.shared_cache) |cache| return try cache.cloneQuantized(self.cache_namespace, node_id);
        return null;
    }

    pub fn borrowCachedQuantized(self: *HBCIndex, node_id: u64) ?BorrowedQuantized {
        if (!self.cache_enabled) return null;
        self.cache_mu.lockShared();
        if (self.pinned_quantized_cache.get(node_id)) |entry| {
            retainQuantizedCacheEntry(entry);
            self.cache_mu.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        if (self.shared_cache != null) {
            self.cache_mu.unlockShared();
            if (self.shared_cache) |cache| return cache.borrowQuantized(self.cache_namespace, node_id);
            return null;
        }
        if (self.quantized_cache.get(node_id)) |entry| {
            retainQuantizedCacheEntry(entry);
            self.cache_mu.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.cache_mu.unlockShared();
        return null;
    }

    pub fn noteMutatedCachedQuantized(self: *HBCIndex, node_id: u64) void {
        if (!self.cache_enabled) return;
        self.refreshAndEnforceHbcCacheUsage(.one(.quantized, node_id));
    }

    pub fn getCachedVector(self: *HBCIndex, vector_id: u64) ?[]const f32 {
        if (!self.cache_enabled) return null;
        if (!self.retained_vector_cache_enabled) return null;
        if (self.shared_cache) |cache| return cache.getVector(self.cache_namespace, vector_id);
        self.cache_mu.lockShared();
        defer self.cache_mu.unlockShared();
        if (self.vector_cache.get(vector_id)) |entry| return entry.vector;
        return null;
    }

    pub fn getCachedMetadata(self: *HBCIndex, vector_id: u64) ?[]const u8 {
        if (!self.cache_enabled) return null;
        if (self.shared_cache) |cache| return cache.getMetadata(self.cache_namespace, vector_id);
        self.cache_mu.lockShared();
        defer self.cache_mu.unlockShared();
        if (self.metadata_cache.get(vector_id)) |entry| return entry.metadata;
        return null;
    }

    pub fn borrowCachedVector(self: *HBCIndex, vector_id: u64) ?BorrowedVector {
        if (!self.cache_enabled) return null;
        if (!self.retained_vector_cache_enabled) return null;
        if (self.shared_cache) |cache| return cache.borrowVector(self.cache_namespace, vector_id);
        self.cache_mu.lockShared();
        if (self.vector_cache.get(vector_id)) |entry| {
            retainVectorCacheEntry(entry);
            self.cache_mu.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.cache_mu.unlockShared();
        return null;
    }

    pub fn borrowCachedMetadata(self: *HBCIndex, vector_id: u64) ?BorrowedMetadata {
        if (!self.cache_enabled) return null;
        if (self.shared_cache) |cache| return cache.borrowMetadata(self.cache_namespace, vector_id);
        self.cache_mu.lockShared();
        if (self.metadata_cache.get(vector_id)) |entry| {
            retainMetadataCacheEntry(entry);
            self.cache_mu.unlockShared();
            return .{ .retained = .{ .alloc = self.alloc, .entry = entry } };
        }
        self.cache_mu.unlockShared();
        return null;
    }

    pub fn acquireSearchScratch(self: *HBCIndex) !ScratchHandle {
        return try vectorindex_hbc_runtime.acquireSearchScratch(self);
    }

    pub fn releaseSearchScratch(self: *HBCIndex, handle: *ScratchHandle) void {
        vectorindex_hbc_runtime.releaseSearchScratch(self, handle);
    }

    pub fn refreshSearchScratchAccounting(self: *HBCIndex, handle: *ScratchHandle) void {
        vectorindex_hbc_runtime.refreshSearchScratchAccounting(self, handle);
    }

    pub fn transformVector(self: *HBCIndex, original: []const f32, transformed: []f32) []const f32 {
        return vectorindex_hbc_runtime.transformVector(self, original, transformed);
    }

    pub fn nextNodeId(self: *HBCIndex) u64 {
        return vectorindex_hbc_runtime.nextNodeId(self);
    }

    fn flushMetadataNow(self: *HBCIndex, txn: anytype) !void {
        var buf: [IndexMetadata.encoded_size]u8 = undefined;
        try self.putNamespaced(txn, .meta, meta_key, self.metadata.encode(&buf));
    }

    pub fn projectionCheckpointMetadata(self: *const HBCIndex) ProjectionCheckpointMetadata {
        return self.metadata.projectionCheckpoint();
    }

    pub fn saveProjectionCheckpointMetadata(self: *HBCIndex, checkpoint: ProjectionCheckpointMetadata) !void {
        const posting_capture = try self.beginExperimentalPostingMaintenanceCapture();
        errdefer if (posting_capture != null) self.cancelExperimentalPostingMaintenanceCapture();
        var txn = try self.beginRuntimeBatchTxnOptions(.{});
        var active = true;
        errdefer if (active) txn.abort();
        self.metadata.setProjectionCheckpoint(checkpoint);
        try self.flushMetadataNow(&txn);
        try txn.commit();
        self.noteExperimentalPostingWalAuthorityCommit();
        active = false;
        if (posting_capture) |capture| try self.finishExperimentalPostingMaintenanceCapture(capture);
    }

    fn persistBulkPublishState(self: *HBCIndex) !void {
        // Publication intent must not itself make an otherwise empty session
        // look mutated, and it must be committed outside deferred session batch
        // mode before any cross-batch HBC mutation begins.
        var txn = try self.store.beginBatch();
        var active = true;
        errdefer if (active) txn.abort();
        try self.putNamespaced(&txn, .meta, bulk_publish_state_key, bulk_publish_state_value);
        try txn.commit();
        active = false;
        if (self.writeSessionFinishNeedsExplicitDurableSync()) try self.sync(true);
    }

    fn clearBulkPublishStateTxn(self: *HBCIndex, txn: anytype) !void {
        self.deleteNamespaced(txn, .meta, bulk_publish_state_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn clearBulkPublishStateBestEffort(self: *HBCIndex) void {
        var txn = self.store.beginWrite() catch return;
        var active = true;
        defer if (active) txn.abort();
        self.clearBulkPublishStateTxn(&txn) catch return;
        txn.commit() catch return;
        active = false;
        if (self.writeSessionFinishNeedsExplicitDurableSync()) self.sync(true) catch return;
    }

    pub fn flushMetadata(self: *HBCIndex, txn: anytype) !void {
        if (!self.mustPublishMetadataPerBatch()) return;
        try self.flushMetadataNow(txn);
    }

    pub fn beginReadTxn(self: *HBCIndex) !vectorindex_store.NamespaceReadTxn {
        return try self.beginRuntimeReadTxn();
    }

    pub fn beginWriteTxn(self: *HBCIndex) !vectorindex_store.NamespaceWriteTxn {
        return try self.beginRuntimeWriteTxn();
    }

    pub fn beginBatchTxn(self: *HBCIndex) !vectorindex_store.NamespaceBatch {
        return try self.beginRuntimeBatchTxn();
    }

    pub fn finishWriteTxn(self: *HBCIndex, txn: anytype) !void {
        try self.finishWriteTxnOptions(txn, .{});
    }

    pub fn finishWriteTxnOptions(self: *HBCIndex, txn: anytype, options: BatchInsertOptions) !void {
        try self.finalizeWriteTxnOptions(txn, options);
        const commit_start = nowNs();
        self.beginPublishedSearchStateRefresh();
        errdefer self.abortPublishedSearchStateRefresh();
        try commitTxn(txn);
        self.noteExperimentalPostingWalAuthorityCommit();
        self.write_profile.insert_commit_ns += elapsedSince(commit_start);
        self.finishPublishedSearchStateRefresh();
    }

    fn finalizeWriteTxnOptions(self: *HBCIndex, txn: anytype, options: BatchInsertOptions) !void {
        try vectorindex_hbc_index.finalizeWriteTxnOptions(self, txn, options, nowNs, elapsedSince);
        if (options.bulk_ingest and
            !self.shouldDeferLeafSplitToBulkFinish(options) and
            !self.shouldDeferQuantizedRebuildToBulkFinish(options))
        {
            try self.publishDeferredNodeKeysForBulkFinishTxn(txn);
        }
    }

    fn rebuildAllQuantized(self: *HBCIndex, txn: anytype) !void {
        try vectorindex_hbc_index.rebuildAllQuantized(self, txn);
    }

    pub fn rebuildAllQuantizedForExperiment(self: *HBCIndex) !void {
        var txn = try self.beginRuntimeWriteTxn();
        errdefer txn.abort();
        try self.rebuildAllQuantized(&txn);
        try self.finishWriteTxn(&txn);
    }

    fn rebuildQuantizedSubtree(self: *HBCIndex, txn: anytype, node_id: u64) !void {
        var node = try self.loadNode(txn, node_id);
        defer node.deinit(self.alloc);
        if (!node.is_leaf) {
            for (node.children) |child_id| {
                try self.rebuildQuantizedSubtree(txn, child_id);
            }
        }
        try self.refreshQuantized(txn, &node);
    }

    pub fn recordDeferredQuantizedNode(self: *HBCIndex, node_id: u64) !void {
        if (!self.config.use_quantization or node_id == 0) return;
        try self.deferred_quantized_nodes.put(self.alloc, node_id, {});
    }

    pub fn clearDeferredQuantizedNode(self: *HBCIndex, node_id: u64) void {
        _ = self.deferred_quantized_nodes.remove(node_id);
    }

    pub fn shouldDeferQuantizedRebuildToBulkFinish(self: *const HBCIndex, options: BatchInsertOptions) bool {
        return self.crossBatchPublicationActive() and options.bulk_ingest and options.defer_quantized_rebuild_to_bulk_finish;
    }

    pub fn shouldDeferLeafSplitToBulkFinish(self: *const HBCIndex, options: BatchInsertOptions) bool {
        return self.crossBatchPublicationActive() and options.bulk_ingest and options.defer_leaf_splits_to_bulk_finish;
    }

    pub fn recordDeferredOversizedLeaf(self: *HBCIndex, leaf_id: u64) !void {
        const gop = try self.deferred_oversized_leaves.getOrPut(self.alloc, leaf_id);
        if (!gop.found_existing) {
            self.deferred_oversized_leaves_peak = @max(self.deferred_oversized_leaves_peak, @as(u64, @intCast(self.deferred_oversized_leaves.count())));
            self.observeApplyWorkspaceBytes();
        }
    }

    fn publishDeferredQuantizedNodesForBulkFinishTxn(self: *HBCIndex, txn: anytype) !void {
        if (self.deferred_quantized_nodes.count() == 0) return;
        const rebuild_start = nowNs();
        try self.rebuildDeferredQuantizedNodes(txn);
        self.write_profile.refresh_quantized_ns += elapsedSince(rebuild_start);
    }

    fn publishDeferredNodeKeysForBulkFinishTxn(self: *HBCIndex, txn: anytype) !void {
        if (self.deferred_node_keys.count() == 0) return;

        var key_buf: [12]u8 = undefined;
        var it = self.deferred_node_keys.iterator();
        while (it.next()) |entry| {
            const parts = stagedNodeKeyParts(entry.key_ptr.*);
            const key = encodeNodeKey(&key_buf, parts.id, parts.suffix);
            if (entry.value_ptr.value) |value| {
                // This is the publication boundary for values deliberately
                // staged by putNamespaced. Calling putNamespaced again while
                // the outer bulk session is active would merely re-stage the
                // value and clear it below. Route it exactly once to the
                // authoritative native WAL or the legacy backing transaction.
                if (!try self.noteExperimentalPostingMutation(.nodes, key, value)) {
                    try txn.put(.nodes, key, value);
                    self.noteNamespacePut(.nodes, key.len, value.len, false);
                }
            } else {
                if (!try self.noteExperimentalPostingMutation(.nodes, key, null)) {
                    try txn.delete(.nodes, key);
                    self.noteNamespaceDelete(.nodes, key.len);
                }
            }
        }

        self.clearDeferredNodeKeys();
    }

    pub fn publishDeferredNodeKeysForBatchFinishTxn(self: *HBCIndex, txn: anytype, options: BatchInsertOptions) !void {
        if (!options.bulk_ingest) return;
        if (self.crossBatchPublicationActive() and self.config.centroid_directory_mode == .flat_rabitq) return;
        if (self.shouldDeferLeafSplitToBulkFinish(options)) return;
        if (self.shouldDeferQuantizedRebuildToBulkFinish(options)) return;
        try self.publishDeferredNodeKeysForBulkFinishTxn(txn);
    }

    pub fn normalizeDeferredOversizedLeavesForBatchFinishTxn(self: *HBCIndex, txn: anytype, options: BatchInsertOptions) !void {
        var split_options = options;
        split_options.defer_leaf_splits_to_batch_finish = false;
        split_options.defer_leaf_splits_to_bulk_finish = false;
        split_options.defer_quantized_rebuild = true;
        split_options.coalesce_leaf_writes = true;
        split_options.skip_vector_store = true;
        split_options.bulk_ingest = true;
        while (try self.normalizeDeferredOversizedLeavesTxn(txn, null, null, split_options, false)) {}
    }

    fn normalizeDeferredOversizedLeavesForBulkFinishTxn(self: *HBCIndex, txn: anytype, options: backend_types.BulkIngestFinishOptions) !bool {
        const split_options: BatchInsertOptions = .{
            .defer_quantized_rebuild = true,
            .coalesce_leaf_writes = true,
            .skip_vector_store = true,
            .bulk_ingest = true,
            .bulk_rebuild_leaf_min_members = options.bulk_rebuild_hbc_leaf_min_members orelse @max(
                @as(usize, @intCast(self.config.leaf_size)) * 2,
                @as(usize, @intCast(self.config.leaf_size)) + 1,
            ),
        };
        return try self.normalizeDeferredOversizedLeavesTxn(
            txn,
            options.max_deferred_hbc_leaf_splits_per_publish,
            options.max_deferred_hbc_leaf_split_members_per_publish,
            split_options,
            true,
        );
    }

    fn normalizeDeferredOversizedLeavesTxn(
        self: *HBCIndex,
        txn: anytype,
        max_splits_per_publish: ?usize,
        max_split_members_per_publish: ?usize,
        split_options: BatchInsertOptions,
        allow_kway: bool,
    ) !bool {
        if (self.deferred_oversized_leaves.count() == 0) return false;

        var steps: usize = 0;
        var split_members: usize = 0;
        const split_limit = @max(max_splits_per_publish orelse default_deferred_hbc_leaf_splits_per_publish, 1);
        const member_limit = max_split_members_per_publish orelse std.math.maxInt(usize);
        const max_steps: usize = @max(
            @as(usize, @intCast(self.metadata.active_count)) * 8,
            self.deferred_oversized_leaves.count() * 4,
        ) + 64;

        while (self.deferred_oversized_leaves.count() > 0) {
            if (steps > max_steps) return error.HBCBatchSplitLimitExceeded;
            if (steps >= split_limit) return true;

            const maybe_leaf_id: ?u64 = blk: {
                var it = self.deferred_oversized_leaves.keyIterator();
                break :blk if (it.next()) |leaf_id| leaf_id.* else null;
            };
            const leaf_id = maybe_leaf_id orelse break;
            _ = self.deferred_oversized_leaves.remove(leaf_id);
            self.observeApplyWorkspaceBytes();

            var leaf = self.loadNode(txn, leaf_id) catch |err| {
                if (isNotFound(err)) continue;
                return err;
            };
            defer leaf.deinit(self.alloc);
            if (!leaf.is_leaf or leaf.members.len <= self.config.leaf_size) continue;
            if (!(try self.deferredLeafIsStillAttached(txn, &leaf))) continue;

            if (steps > 0 and split_members + leaf.members.len > member_limit) {
                try self.deferred_oversized_leaves.put(self.alloc, leaf_id, {});
                self.observeApplyWorkspaceBytes();
                return true;
            }

            steps += 1;
            split_members += leaf.members.len;

            const bulk_rebuild_min_members = if (split_options.bulk_rebuild_leaf_min_members != 0)
                split_options.bulk_rebuild_leaf_min_members
            else if (split_options.bulk_ingest)
                @max(@as(usize, @intCast(self.config.leaf_size)) * 4, @as(usize, @intCast(self.config.leaf_size)) + 1)
            else
                0;
            const should_bulk_rebuild = bulk_rebuild_min_members != 0 and leaf.members.len >= bulk_rebuild_min_members;
            const kway_within_member_budget = max_split_members_per_publish == null or leaf.members.len <= member_limit;
            if (allow_kway and kway_within_member_budget and !should_bulk_rebuild and try self.rebuildOversizedLeafKmeansWithOptions(txn, &leaf, split_options)) {
                continue;
            }

            const right_leaf_id = self.metadata.node_count + 1;
            try self.splitLeafWithOptions(txn, &leaf, split_options);
            {
                var left = self.loadNode(txn, leaf.id) catch |err| {
                    if (isNotFound(err)) continue;
                    return err;
                };
                defer left.deinit(self.alloc);
                if (left.is_leaf and left.members.len > self.config.leaf_size) {
                    try self.deferred_oversized_leaves.put(self.alloc, left.id, {});
                }
            }
            {
                var right = self.loadNode(txn, right_leaf_id) catch |err| {
                    if (isNotFound(err)) continue;
                    return err;
                };
                defer right.deinit(self.alloc);
                if (right.is_leaf and right.members.len > self.config.leaf_size) {
                    try self.deferred_oversized_leaves.put(self.alloc, right.id, {});
                }
            }
        }
        return false;
    }

    fn deferredLeafIsStillAttached(self: *HBCIndex, txn: anytype, leaf: *const Node) !bool {
        if (!leaf.is_leaf) return false;
        if (leaf.parent == 0) return leaf.id == self.metadata.root_node;

        var parent = self.loadNode(txn, leaf.parent) catch |err| {
            if (isNotFound(err)) return false;
            return err;
        };
        defer parent.deinit(self.alloc);
        for (parent.children) |child_id| {
            if (child_id == leaf.id) return true;
        }
        return false;
    }

    pub fn rebuildDeferredQuantizedNodes(self: *HBCIndex, txn: anytype) !void {
        try self.rebuildDeferredQuantizedNodesWithOptions(txn, .{});
    }

    pub fn rebuildDeferredQuantizedNodesWithOptions(self: *HBCIndex, txn: anytype, options: BatchInsertOptions) !void {
        if (!self.config.use_quantization) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
            return;
        }

        var retained = std.ArrayListUnmanaged(u64).empty;
        defer retained.deinit(self.alloc);

        {
            var it = self.deferred_quantized_nodes.keyIterator();
            while (it.next()) |node_id| {
                var node = self.loadNode(txn, node_id.*) catch |err| {
                    if (isNotFound(err)) continue;
                    return err;
                };
                defer node.deinit(self.alloc);
                if (self.deferred_oversized_leaves.contains(node_id.*) and node.is_leaf and node.members.len > self.config.leaf_size) {
                    try retained.append(self.alloc, node_id.*);
                    continue;
                }
                self.refreshQuantizedWithOptions(txn, &node, options) catch |err| {
                    if (isNotFound(err)) {
                        var key_buf: [10]u8 = undefined;
                        self.deleteNamespaced(txn, .quant, encodeQuantKey(&key_buf, node.id)) catch {};
                        self.invalidateQuantizedCache(node.id);
                        continue;
                    }
                    return err;
                };
            }
        }
        self.deferred_quantized_nodes.clearRetainingCapacity();
        for (retained.items) |node_id| {
            try self.deferred_quantized_nodes.put(self.alloc, node_id, {});
        }
    }

    // ========================================================================
    // Node I/O
    // ========================================================================

    pub fn loadNodeFromStorage(self: *HBCIndex, txn: anytype, node_id: u64) !Node {
        var key_buf: [12]u8 = undefined;

        const packed_data = try self.getNamespaced(txn, .nodes, encodeNodeKey(&key_buf, node_id, .packed_node));
        const packed_value = try vectorindex_hbc.decodePackedNodeValue(packed_data);
        if (packed_value.centroid_bytes.len % @sizeOf(f32) != 0) return error.Corrupted;
        if (packed_value.ids_bytes.len % @sizeOf(u64) != 0) return error.Corrupted;
        const centroid_len = packed_value.centroid_bytes.len;
        const ids_len = packed_value.ids_bytes.len;
        const ids_offset = std.mem.alignForward(usize, centroid_len, @alignOf(u64));
        const total_len = ids_offset + ids_len;

        var backing: []align(@alignOf(u64)) u8 = if (total_len > 0)
            try self.alloc.alignedAlloc(u8, std.mem.Alignment.of(u64), total_len)
        else
            &.{};
        errdefer if (backing.len > 0) self.alloc.free(backing);

        const centroid: []f32 = if (centroid_len > 0) blk: {
            const dst: []align(@alignOf(f32)) u8 = @alignCast(backing[0..centroid_len]);
            @memcpy(dst, packed_value.centroid_bytes);
            break :blk @as([*]f32, @ptrCast(dst.ptr))[0 .. centroid_len / @sizeOf(f32)];
        } else blk: {
            break :blk &.{};
        };

        var children: []u64 = &.{};
        var members: []u64 = &.{};
        if (ids_len > 0) {
            const dst: []u8 = backing[ids_offset .. ids_offset + ids_len];
            @memcpy(dst, packed_value.ids_bytes);
            const aligned_dst: []align(@alignOf(u64)) u8 = @alignCast(dst);
            if (packed_value.header.is_leaf) {
                members = std.mem.bytesAsSlice(u64, aligned_dst);
            } else {
                children = std.mem.bytesAsSlice(u64, aligned_dst);
            }
        }

        const node = Node{
            .id = node_id,
            .is_leaf = packed_value.header.is_leaf,
            .level = packed_value.header.level,
            .parent = packed_value.header.parent,
            .centroid = centroid,
            .children = children,
            .members = members,
            .posting_state = if (packed_value.header.is_leaf) try vectorindex_posting.PostingStore.loadState(self, txn, node_id, isNotFound) else .{},
            .backing = backing,
        };
        return node;
    }

    fn loadCommittedPostingState(self: *HBCIndex, txn: anytype, posting_id: u64) !vectorindex_posting.PostingState {
        var key_buf: [12]u8 = undefined;
        const data = self.getNamespacedCommitted(txn, .nodes, encodeNodeKey(&key_buf, posting_id, .posting)) catch |err| {
            if (isNotFound(err)) return .{};
            return err;
        };
        return try vectorindex_posting.decodeState(data);
    }

    pub fn loadSearchNodeFromStorage(self: *HBCIndex, txn: anytype, node_id: u64) !Node {
        var key_buf: [12]u8 = undefined;

        const packed_data = try self.getNamespacedCommitted(txn, .nodes, encodeNodeKey(&key_buf, node_id, .packed_node));
        const packed_value = try vectorindex_hbc.decodePackedNodeValue(packed_data);
        if (packed_value.centroid_bytes.len % @sizeOf(f32) != 0) return error.Corrupted;
        if (packed_value.ids_bytes.len % @sizeOf(u64) != 0) return error.Corrupted;
        const centroid_len = packed_value.centroid_bytes.len;
        const ids_len = packed_value.ids_bytes.len;
        const ids_offset = std.mem.alignForward(usize, centroid_len, @alignOf(u64));
        const total_len = ids_offset + ids_len;

        var backing: []align(@alignOf(u64)) u8 = if (total_len > 0)
            try self.alloc.alignedAlloc(u8, std.mem.Alignment.of(u64), total_len)
        else
            &.{};
        errdefer if (backing.len > 0) self.alloc.free(backing);

        const centroid: []f32 = if (centroid_len > 0) blk: {
            const dst: []align(@alignOf(f32)) u8 = @alignCast(backing[0..centroid_len]);
            @memcpy(dst, packed_value.centroid_bytes);
            break :blk @as([*]f32, @ptrCast(dst.ptr))[0 .. centroid_len / @sizeOf(f32)];
        } else blk: {
            break :blk &.{};
        };

        var children: []u64 = &.{};
        var members: []u64 = &.{};
        if (ids_len > 0) {
            const dst: []u8 = backing[ids_offset .. ids_offset + ids_len];
            @memcpy(dst, packed_value.ids_bytes);
            const aligned_dst: []align(@alignOf(u64)) u8 = @alignCast(dst);
            if (packed_value.header.is_leaf) {
                members = std.mem.bytesAsSlice(u64, aligned_dst);
            } else {
                children = std.mem.bytesAsSlice(u64, aligned_dst);
            }
        }

        return .{
            .id = node_id,
            .is_leaf = packed_value.header.is_leaf,
            .level = packed_value.header.level,
            .parent = packed_value.header.parent,
            .centroid = centroid,
            .children = children,
            .members = members,
            .posting_state = if (packed_value.header.is_leaf) try self.loadCommittedPostingState(txn, node_id) else .{},
            .backing = backing,
        };
    }

    fn pinnedNodeCached(self: *HBCIndex, node_id: u64) bool {
        self.cache_mu.lockShared();
        defer self.cache_mu.unlockShared();
        return self.pinned_node_cache.contains(node_id);
    }

    fn pinnedQuantizedCached(self: *HBCIndex, node_id: u64) bool {
        self.cache_mu.lockShared();
        defer self.cache_mu.unlockShared();
        return self.pinned_quantized_cache.contains(node_id);
    }

    fn ensurePinnedNode(self: *HBCIndex, node: *const Node) !void {
        if (self.config.max_pinned_tree_nodes == 0) return;
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        try self.cachePinnedNodeLocked(node, false);
    }

    fn ensurePinnedQuantized(self: *HBCIndex, txn: anytype, node: *const Node) !void {
        if (!self.config.use_quantization) return;
        if (self.config.max_pinned_tree_nodes == 0) return;
        const expected_count = if (node.is_leaf) node.members.len else node.children.len;
        if (expected_count == 0) return;
        if (self.pinnedQuantizedCached(node.id)) return;

        if (self.borrowCachedQuantized(node.id)) |borrowed| {
            var handle = borrowed;
            defer handle.deinit();
            const cloned = try handle.ptr().clone(self.alloc);
            self.cache_mu.lockExclusive();
            defer self.cache_mu.unlockExclusive();
            try self.cachePinnedQuantizedOwnedLocked(node.id, cloned, false);
            return;
        }

        const loaded = self.loadQuantized(txn, node.id, node.parent == 0, expected_count) catch |err| {
            if (isNotFound(err) or err == error.Corrupted) return;
            return err;
        };
        self.cache_mu.lockExclusive();
        defer self.cache_mu.unlockExclusive();
        try self.cachePinnedQuantizedOwnedLocked(node.id, loaded, false);
    }

    pub fn pinUpperTreeCache(self: *HBCIndex, txn: anytype) !void {
        if (!self.cache_enabled) return;
        if (self.config.max_pinned_tree_nodes == 0) return;
        if (self.metadata.root_node == 0) return;

        const PinQueueEntry = struct {
            node_id: u64,
            depth: u8,
        };

        var pending = std.ArrayListUnmanaged(PinQueueEntry).empty;
        defer pending.deinit(self.alloc);
        try pending.append(self.alloc, .{ .node_id = self.publishedRootNode(), .depth = 0 });

        var index: usize = 0;
        var visited: usize = 0;
        while (index < pending.items.len and visited < self.config.max_pinned_tree_nodes) : (index += 1) {
            const item = pending.items[index];
            visited += 1;

            if (self.borrowCachedNode(item.node_id)) |borrowed| {
                var handle = borrowed;
                defer handle.deinit();
                const node = handle.ptr();
                if (!self.pinnedNodeCached(item.node_id)) try self.ensurePinnedNode(node);
                try self.ensurePinnedQuantized(txn, node);
                if (!node.is_leaf and item.depth < self.config.pinned_tree_depth) {
                    for (node.children) |child_id| {
                        if (pending.items.len >= self.config.max_pinned_tree_nodes) break;
                        try pending.append(self.alloc, .{ .node_id = child_id, .depth = item.depth + 1 });
                    }
                }
                continue;
            }

            var node = self.loadNodeFromStorage(txn, item.node_id) catch |err| {
                if (isNotFound(err)) continue;
                return err;
            };
            defer node.deinit(self.alloc);
            try self.ensurePinnedNode(&node);
            try self.ensurePinnedQuantized(txn, &node);
            if (!node.is_leaf and item.depth < self.config.pinned_tree_depth) {
                for (node.children) |child_id| {
                    if (pending.items.len >= self.config.max_pinned_tree_nodes) break;
                    try pending.append(self.alloc, .{ .node_id = child_id, .depth = item.depth + 1 });
                }
            }
        }
    }

    pub fn loadNode(self: *HBCIndex, txn: anytype, node_id: u64) !Node {
        return try vectorindex_hbc_index.loadNode(self, txn, node_id);
    }

    pub fn validateStoredStructure(self: *HBCIndex, alloc: Allocator) !void {
        return try self.validateStoredStructureWithCancellation(alloc, null, null);
    }

    /// Validates the published tree through one stable runtime read snapshot.
    /// Background repair preflight supplies a cooperative cancellation hook so
    /// owner shutdown does not have to wait for a large tree walk to finish.
    pub fn validateStoredStructureWithCancellation(
        self: *HBCIndex,
        alloc: Allocator,
        cancel_ctx: ?*anyopaque,
        cancel_fn: ?*const fn (*anyopaque) bool,
    ) !void {
        if (self.metadata.active_count == 0) return;
        if (self.metadata.root_node == 0 or self.metadata.node_count == 0) return error.Corrupted;

        var txn = try self.beginRuntimeReadTxn();
        defer txn.abort();

        var pending = std.ArrayListUnmanaged(u64).empty;
        defer pending.deinit(alloc);
        try pending.append(alloc, self.metadata.root_node);

        var seen = std.AutoHashMapUnmanaged(u64, void).empty;
        defer seen.deinit(alloc);

        var visited_since_cancel_check: usize = 0;
        while (pending.pop()) |node_id| {
            visited_since_cancel_check += 1;
            if (visited_since_cancel_check == 256) {
                visited_since_cancel_check = 0;
                if (cancel_ctx != null and cancel_fn != null and cancel_fn.?(cancel_ctx.?)) return error.Canceled;
            }
            if (node_id == 0) return error.Corrupted;
            const gop = try seen.getOrPut(alloc, node_id);
            if (gop.found_existing) continue;

            var node = try self.loadNodeFromStorage(&txn, node_id);
            defer node.deinit(self.alloc);

            if (node.is_leaf) {
                if (node.members.len == 0) return error.Corrupted;
                continue;
            }
            if (node.children.len == 0) return error.Corrupted;
            try pending.appendSlice(alloc, node.children);
        }
    }

    fn collectNamespaceKeys(
        self: *HBCIndex,
        alloc: Allocator,
        txn: anytype,
        comptime namespace: Namespace,
    ) !std.ArrayListUnmanaged([]u8) {
        var keys = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (keys.items) |key| alloc.free(key);
            keys.deinit(alloc);
        }

        var cursor = try self.openNamespacedCursor(alloc, txn, namespace);
        defer cursor.close();

        var entry = try cursor.first();
        while (entry) |kv| : (entry = try cursor.next()) {
            try keys.append(alloc, try alloc.dupe(u8, kv.key));
        }
        return keys;
    }

    fn clearNamespace(self: *HBCIndex, txn: anytype, comptime namespace: Namespace) !void {
        var keys = try self.collectNamespaceKeys(self.alloc, txn, namespace);
        defer {
            for (keys.items) |key| self.alloc.free(key);
            keys.deinit(self.alloc);
        }

        for (keys.items) |key| {
            try self.deleteNamespaced(txn, namespace, key);
        }
    }

    fn putEmptyRootNodeTxn(self: *HBCIndex, txn: anytype) !void {
        var key_buf: [12]u8 = undefined;
        var packed_buf: [vectorindex_hbc.packed_node_header_size]u8 = undefined;
        const header = NodeHeader{ .is_leaf = true, .level = 0, .parent = 0 };
        const packed_node = try vectorindex_hbc.encodePackedNodeValue(&packed_buf, header, &.{}, &.{});
        try self.putNamespaced(txn, .nodes, encodeNodeKey(&key_buf, 1, .packed_node), packed_node);
    }

    fn resetStoredStructureTxn(self: *HBCIndex, txn: anytype) !void {
        try self.clearNamespace(txn, .nodes);
        try self.clearNamespace(txn, .vecs);
        try self.clearNamespace(txn, .quant);
        try self.clearNamespace(txn, .meta);

        self.clearNodeCache();
        self.clearQuantizedCache();
        self.clearVectorCache();
        self.clearMetadataCache();
        self.deferred_quantized_nodes.clearRetainingCapacity();
        self.clearDeferredNodeKeys();
        self.deferred_oversized_leaves.clearRetainingCapacity();
        self.observeApplyWorkspaceBytes();

        self.metadata.root_node = 1;
        self.metadata.node_count = 1;
        self.metadata.active_count = 0;
        try self.putEmptyRootNodeTxn(txn);
    }

    pub fn resetStoredStructure(self: *HBCIndex) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        try self.resetStoredStructureTxn(&txn);
        try self.finishWriteTxn(&txn);
    }

    pub fn deleteNodeHeaderForTest(self: *HBCIndex, node_id: u64) !void {
        if (!builtin.is_test) return error.Unsupported;

        var txn = try self.beginWriteTxn();
        errdefer txn.abort();

        var key_buf: [12]u8 = undefined;
        try self.deleteNamespaced(&txn, .nodes, encodeNodeKey(&key_buf, node_id, .packed_node));
        try self.finishWriteTxn(&txn);
    }

    pub fn getNodePtr(self: *HBCIndex, txn: anytype, node_id: u64) !*const Node {
        if (self.getCachedNodePtr(node_id)) |cached| return cached;

        const loaded = try self.loadNodeFromStorage(txn, node_id);
        return try self.cacheNodeOwned(loaded);
    }

    pub fn getNodePtrProfiled(self: *HBCIndex, txn: anytype, node_id: u64, profile: *SearchProfile) !*const Node {
        if (self.getCachedNodePtr(node_id)) |cached| {
            return cached;
        }

        const start = nowNs();
        const loaded = try self.loadNodeFromStorage(txn, node_id);
        const cached = try self.cacheNodeOwned(loaded);
        profile.node_cache_miss_ns += elapsedSince(start);
        profile.node_cache_misses += 1;
        return cached;
    }

    pub fn saveNode(self: *HBCIndex, txn: anytype, node: *const Node) !void {
        try vectorindex_hbc_index.saveNode(self, txn, node, nowNsI128, elapsedSinceNs);
    }

    pub fn saveNodeWithOptions(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        options: BatchInsertOptions,
    ) !void {
        try vectorindex_hbc_index.saveNodeWithOptions(self, txn, node, options, nowNsI128, elapsedSinceNs);
    }

    pub fn saveNodeWithOptionsMode(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        options: BatchInsertOptions,
        write_header: bool,
    ) !void {
        try vectorindex_hbc_index.saveNodeWithOptionsMode(self, txn, node, options, write_header, nowNsI128, elapsedSinceNs);
    }

    pub fn saveNodeBody(self: *HBCIndex, txn: anytype, node: *const Node) !void {
        try vectorindex_hbc_index.saveNodeBody(self, txn, node, nowNsI128, elapsedSinceNs);
    }

    fn saveNodeBodyWithAddedVector(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        transformed_vector: []const f32,
    ) !void {
        try vectorindex_hbc_index.saveNodeBodyWithAddedVector(
            self,
            txn,
            node,
            transformed_vector,
            nowNsI128,
            elapsedSinceNs,
        );
    }

    fn saveNodeBodyWithAddedVectorOptions(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        transformed_vector: []const f32,
        options: BatchInsertOptions,
    ) !void {
        try vectorindex_hbc_index.saveNodeBodyWithAddedVectorOptions(
            self,
            txn,
            node,
            transformed_vector,
            options,
            nowNsI128,
            elapsedSinceNs,
        );
    }

    fn saveExistingNodeBodyWithAddedVectorOptions(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        transformed_vector: []const f32,
        options: BatchInsertOptions,
    ) !void {
        try vectorindex_hbc_index.saveExistingNodeBodyWithAddedVectorOptions(
            self,
            txn,
            node,
            transformed_vector,
            options,
            nowNsI128,
            elapsedSinceNs,
        );
    }

    fn saveNodeBodyInternal(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        added_vector: ?[]const f32,
        defer_quantized_rebuild: bool,
        write_header: bool,
    ) !void {
        try vectorindex_hbc_index.saveNodeBodyInternal(
            self,
            txn,
            node,
            added_vector,
            defer_quantized_rebuild,
            write_header,
            std.time.nanoTimestamp,
            elapsedSinceNs,
        );
    }

    fn updateQuantizedWithAddedVector(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        transformed_vector: []const f32,
    ) !bool {
        return try vectorindex_hbc_index.updateQuantizedWithAddedVector(self, txn, node, transformed_vector, nowNsI128, elapsedSinceNs, nowNsI128());
    }

    pub fn deleteNode(self: *HBCIndex, txn: anytype, node_id: u64) !void {
        try vectorindex_hbc_index.deleteNode(self, txn, node_id);
    }

    pub fn updateParent(self: *HBCIndex, txn: anytype, node_id: u64, new_parent: u64) !void {
        try vectorindex_hbc_index.updateParent(self, txn, node_id, new_parent, nowNs, elapsedSince);
    }

    fn loadNodeParent(self: *HBCIndex, txn: anytype, node_id: u64) !u64 {
        return try vectorindex_hbc_index.loadNodeParent(self, txn, node_id);
    }

    // ========================================================================
    // Vector storage
    // ========================================================================

    /// Store a raw vector by ID.
    pub fn putVector(self: *HBCIndex, txn: anytype, vector_id: u64, vector_data: []const f32) !void {
        try vectorindex_hbc_index.putVector(self, txn, vector_id, vector_data);
    }

    /// Load a raw vector by ID. Caller must free the returned slice.
    pub fn getVector(self: *HBCIndex, txn: anytype, vector_id: u64) ![]f32 {
        return vectorindex_hbc_index.getVector(self, txn, vector_id) catch |err| {
            if (!isNotFound(err)) return err;
            return try self.loadExternalVector(txn, vector_id);
        };
    }

    /// Load a raw vector into caller-provided scratch storage and return the populated view.
    pub fn getVectorInto(self: *HBCIndex, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
        if (builtin.is_test) {
            if (test_get_vector_view_or_scratch_hook) |hook| hook(test_get_vector_view_or_scratch_ctx, self, vector_id);
        }
        return vectorindex_hbc_index.getVectorInto(self, txn, vector_id, scratch) catch |err| {
            if (!isNotFound(err)) return err;
            if (self.bypass_external_vector_cache) {
                return try self.loadExternalVectorIntoScratch(txn, vector_id, scratch);
            }
            return try self.loadExternalVectorCachedIntoScratch(txn, vector_id, scratch);
        };
    }

    pub fn getVectorViewOrScratch(self: *HBCIndex, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
        return try self.getVectorInto(txn, vector_id, scratch);
    }

    pub fn notifyVectorViewLoadForTest(self: *HBCIndex, vector_id: u64) void {
        if (builtin.is_test) {
            if (test_get_vector_view_or_scratch_hook) |hook| hook(test_get_vector_view_or_scratch_ctx, self, vector_id);
        }
    }

    pub fn getVectorViewOrScratchWithCursor(self: *HBCIndex, cursor: *vectorindex_store.Cursor, vector_id: u64, scratch: []f32) ![]const f32 {
        if (builtin.is_test) {
            if (test_get_vector_view_or_scratch_hook) |hook| hook(test_get_vector_view_or_scratch_ctx, self, vector_id);
        }
        return try vectorindex_hbc_index.getVectorViewOrScratchWithCursor(self, cursor, vector_id, scratch);
    }

    pub fn getExternalVectorViewsSortedWithScratch(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        vector_views: [][]const f32,
        lookup_storage: []FixedKeyLookup,
        key_views_storage: [][]const u8,
        values_storage: []?[]const u8,
        scratch: []f32,
        batch_scratch: []f32,
    ) !bool {
        const loader = self.external_vector_batch_scratch_loader orelse return false;
        const ctx = self.external_vector_ctx orelse return false;
        if (vector_views.len < vector_ids.len) return error.InvalidArgument;
        if (vector_ids.len == 0) return true;

        const metadata = try self.alloc.alloc(?[]const u8, vector_ids.len);
        defer self.alloc.free(metadata);
        try self.getMetadataManySortedInTxnWithScratch(
            txn,
            vector_ids,
            metadata,
            lookup_storage,
            key_views_storage,
            values_storage,
        );
        loader(ctx, vector_ids, metadata, vector_views[0..vector_ids.len], batch_scratch, scratch.len) catch |err| switch (err) {
            error.Unsupported => return false,
            else => return err,
        };
        return true;
    }

    fn transformExternalVectorForMatrix(index: *HBCIndex, original: []const f32, transformed: []f32) []const f32 {
        return index.transformVector(original, transformed);
    }

    pub fn loadExternalVectorsTransformedIntoMatrix(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        matrix_positions: []const usize,
        matrix: []f32,
        lookup_storage: []FixedKeyLookup,
        key_views_storage: [][]const u8,
        values_storage: []?[]const u8,
        scratch: []f32,
    ) !bool {
        if (self.bulk_split_vector_workspace.active) {
            return try self.loadExternalVectorsTransformedIntoMatrixWithBulkSplitWorkspace(
                txn,
                vector_ids,
                matrix_positions,
                matrix,
                lookup_storage,
                key_views_storage,
                values_storage,
                scratch,
            );
        }
        return try self.loadExternalVectorsTransformedIntoMatrixUncached(
            txn,
            vector_ids,
            matrix_positions,
            matrix,
            lookup_storage,
            key_views_storage,
            values_storage,
            scratch,
        );
    }

    pub fn loadPostingVectorsTransformed(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        matrix: []f32,
    ) !void {
        try vectorindex_hbc_index.loadPostingVectorsTransformed(self, txn, vector_ids, matrix);
    }

    pub fn loadPostingVectorsTransformedWithOptions(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        matrix: []f32,
        options: anytype,
    ) !void {
        try vectorindex_hbc_index.loadPostingVectorsTransformedWithOptions(
            self,
            txn,
            vector_ids,
            matrix,
            options,
        );
    }

    fn loadExternalVectorsTransformedIntoMatrixUncached(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        matrix_positions: []const usize,
        matrix: []f32,
        lookup_storage: []FixedKeyLookup,
        key_views_storage: [][]const u8,
        values_storage: []?[]const u8,
        scratch: []f32,
    ) !bool {
        const loader = self.external_vector_batch_transformed_matrix_loader orelse return false;
        const ctx = self.external_vector_ctx orelse return false;
        if (vector_ids.len != matrix_positions.len) return error.InvalidArgument;
        if (vector_ids.len == 0) return true;

        const metadata = try self.alloc.alloc(?[]const u8, vector_ids.len);
        defer self.alloc.free(metadata);
        try self.getMetadataManySortedInTxnWithScratch(
            txn,
            vector_ids,
            metadata,
            lookup_storage,
            key_views_storage,
            values_storage,
        );
        loader(
            ctx,
            vector_ids,
            metadata,
            matrix_positions,
            matrix,
            scratch,
            self.config.dims,
            self,
            transformExternalVectorForMatrix,
        ) catch |err| switch (err) {
            error.Unsupported => return false,
            else => return err,
        };
        return true;
    }

    fn loadExternalVectorsTransformedIntoMatrixWithBulkSplitWorkspace(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        matrix_positions: []const usize,
        matrix: []f32,
        lookup_storage: []FixedKeyLookup,
        key_views_storage: [][]const u8,
        values_storage: []?[]const u8,
        scratch: []f32,
    ) !bool {
        if (vector_ids.len != matrix_positions.len) return error.InvalidArgument;
        if (vector_ids.len == 0) return true;

        var missing_ids = try self.alloc.alloc(u64, vector_ids.len);
        defer self.alloc.free(missing_ids);
        var missing_positions = try self.alloc.alloc(usize, vector_ids.len);
        defer self.alloc.free(missing_positions);

        var missing_count: usize = 0;
        for (vector_ids, matrix_positions) |vector_id, matrix_position| {
            const offset = std.math.mul(usize, matrix_position, self.config.dims) catch return error.BufferTooSmall;
            if (offset + self.config.dims > matrix.len) return error.BufferTooSmall;
            if (self.bulkSplitVectorWorkspaceLookup(vector_id, matrix[offset .. offset + self.config.dims])) {
                continue;
            }
            missing_ids[missing_count] = vector_id;
            missing_positions[missing_count] = matrix_position;
            missing_count += 1;
        }

        if (missing_count == 0) return true;
        const loaded = try self.loadExternalVectorsTransformedIntoMatrixUncached(
            txn,
            missing_ids[0..missing_count],
            missing_positions[0..missing_count],
            matrix,
            lookup_storage[0..missing_count],
            key_views_storage[0..missing_count],
            values_storage[0..missing_count],
            scratch,
        );
        if (!loaded) return false;

        for (missing_ids[0..missing_count], missing_positions[0..missing_count]) |vector_id, matrix_position| {
            const offset = std.math.mul(usize, matrix_position, self.config.dims) catch return error.BufferTooSmall;
            if (offset + self.config.dims > matrix.len) return error.BufferTooSmall;
            self.bulkSplitVectorWorkspaceAdmit(vector_id, matrix[offset .. offset + self.config.dims]);
        }
        return true;
    }

    pub fn scoreExternalRerankVectorsSortedWithScratch(
        self: *HBCIndex,
        txn: anytype,
        ranked_items: []const ApproxSearchResult,
        rerank_positions: []const usize,
        query: []const f32,
        query_measure: f32,
        distances: []f32,
        vector_id_storage: []u64,
        metadata_storage: []?[]const u8,
        lookup_storage: []FixedKeyLookup,
        key_views_storage: [][]const u8,
        values_storage: []?[]const u8,
        batch_scratch: []f32,
        profile: ?*SearchProfile,
    ) !bool {
        const loader = self.external_vector_batch_distance_loader orelse return false;
        const ctx = self.external_vector_ctx orelse return false;
        if (distances.len < rerank_positions.len) return error.InvalidArgument;
        if (vector_id_storage.len < rerank_positions.len) return error.InvalidArgument;
        if (metadata_storage.len < rerank_positions.len) return error.InvalidArgument;
        if (rerank_positions.len == 0) return true;

        const vector_ids = vector_id_storage[0..rerank_positions.len];
        for (rerank_positions, 0..) |index, slot| {
            vector_ids[slot] = ranked_items[index].vector_id;
            distances[slot] = std.math.inf(f32);
        }

        const metadata = metadata_storage[0..rerank_positions.len];
        const metadata_start = platform_time.monotonicNs();
        try self.getMetadataManySortedInTxnWithScratch(
            txn,
            vector_ids,
            metadata,
            lookup_storage,
            key_views_storage,
            values_storage,
        );
        if (profile) |p| p.rerank_metadata_lookup_ns += platform_time.monotonicNs() - metadata_start;
        loader(
            ctx,
            vector_ids,
            metadata,
            query,
            query_measure,
            self.config.metric,
            distances[0..rerank_positions.len],
            batch_scratch,
            @intCast(self.config.dims),
            .{
                .artifact_keys = key_views_storage,
                .raw_values = values_storage,
            },
            profile,
        ) catch |err| switch (err) {
            error.Unsupported => return false,
            else => return err,
        };
        return true;
    }

    pub fn scoreExternalVectorsSortedWithScratch(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        query: []const f32,
        query_measure: f32,
        distances: []f32,
        metadata_storage: []?[]const u8,
        lookup_storage: []FixedKeyLookup,
        key_views_storage: [][]const u8,
        values_storage: []?[]const u8,
        batch_scratch: []f32,
    ) !bool {
        const loader = self.external_vector_batch_distance_loader orelse return false;
        const ctx = self.external_vector_ctx orelse return false;
        if (distances.len < vector_ids.len) return error.InvalidArgument;
        if (metadata_storage.len < vector_ids.len) return error.InvalidArgument;
        if (vector_ids.len == 0) return true;

        for (distances[0..vector_ids.len]) |*distance| distance.* = std.math.inf(f32);
        const metadata = metadata_storage[0..vector_ids.len];
        try self.getMetadataManySortedInTxnWithScratch(
            txn,
            vector_ids,
            metadata,
            lookup_storage,
            key_views_storage,
            values_storage,
        );
        loader(
            ctx,
            vector_ids,
            metadata,
            query,
            query_measure,
            self.config.metric,
            distances[0..vector_ids.len],
            batch_scratch,
            @intCast(self.config.dims),
            .{
                .artifact_keys = key_views_storage,
                .raw_values = values_storage,
            },
            null,
        ) catch |err| switch (err) {
            error.Unsupported => return false,
            else => return err,
        };
        return true;
    }

    pub fn getVectorScratch(self: *HBCIndex, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
        return vectorindex_hbc_index.getVectorScratch(self, txn, vector_id, scratch) catch |err| {
            if (!isNotFound(err)) return err;
            if (self.bypass_external_vector_cache and self.hasExternalVectorLoader()) {
                return try self.loadExternalVectorIntoScratch(txn, vector_id, scratch);
            }
            return try self.loadExternalVectorCachedIntoScratch(txn, vector_id, scratch);
        };
    }

    fn vectorViewFromRaw(data: []const u8, scratch: []f32) ![]const f32 {
        return try vectorindex_hbc_index.vectorViewFromRaw(data, scratch);
    }

    fn loadExternalVector(self: *HBCIndex, txn: anytype, vector_id: u64) ![]f32 {
        const loader = self.external_vector_loader orelse return error.NotFound;
        const ctx = self.external_vector_ctx orelse return error.NotFound;
        const metadata = (try self.loadMetadataRaw(txn, vector_id)) orelse return error.NotFound;
        return try loader(ctx, self.alloc, vector_id, metadata);
    }

    fn loadExternalVectorIntoScratch(self: *HBCIndex, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
        const metadata = (try self.loadMetadataRaw(txn, vector_id)) orelse return error.NotFound;
        if (self.external_vector_scratch_loader) |loader| {
            const ctx = self.external_vector_ctx orelse return error.NotFound;
            return try loader(ctx, vector_id, metadata, scratch);
        }
        const vector = try self.loadExternalVector(txn, vector_id);
        defer self.alloc.free(vector);
        if (vector.len > scratch.len) return error.BufferTooSmall;
        @memcpy(scratch[0..vector.len], vector);
        return scratch[0..vector.len];
    }

    fn loadExternalVectorCachedIntoScratch(self: *HBCIndex, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
        if (self.borrowCachedVector(vector_id)) |cached_handle| {
            var handle = cached_handle;
            defer handle.deinit();
            const cached = handle.view();
            self.write_profile.external_vector_cache_hits += 1;
            if (cached.len > scratch.len) return error.BufferTooSmall;
            @memcpy(scratch[0..cached.len], cached);
            return scratch[0..cached.len];
        }
        self.write_profile.external_vector_cache_misses += 1;
        if (self.external_vector_scratch_loader != null) {
            return try self.loadExternalVectorIntoScratch(txn, vector_id, scratch);
        }
        const vector = try self.loadExternalVector(txn, vector_id);
        defer self.alloc.free(vector);
        if (vector.len > scratch.len) return error.BufferTooSmall;
        @memcpy(scratch[0..vector.len], vector);
        _ = try self.cacheVector(vector_id, vector);
        return scratch[0..vector.len];
    }

    /// Store vector-to-leaf mapping.
    pub fn putVecLeaf(self: *HBCIndex, txn: anytype, vector_id: u64, leaf_id: u64) !void {
        try vectorindex_hbc_index.putVecLeaf(self, txn, vector_id, leaf_id);
    }

    /// Get which leaf a vector belongs to.
    pub fn getVecLeaf(self: *HBCIndex, txn: anytype, vector_id: u64) !u64 {
        return try vectorindex_hbc_index.getVecLeaf(self, txn, vector_id);
    }

    pub fn loadMetadataRaw(self: *HBCIndex, txn: anytype, vector_id: u64) !?[]const u8 {
        return try vectorindex_hbc_index.loadMetadataRaw(self, txn, vector_id, isNotFound);
    }

    fn putMetadata(self: *HBCIndex, txn: anytype, vector_id: u64, metadata: []const u8) !void {
        try vectorindex_hbc_index.putMetadata(self, txn, vector_id, metadata);
    }

    pub fn getMetadata(self: *HBCIndex, vector_id: u64) !?[]u8 {
        return try vectorindex_hbc_index.getMetadata(self, vector_id);
    }

    pub fn getMetadataInTxn(self: *HBCIndex, txn: anytype, vector_id: u64) !?[]const u8 {
        return try vectorindex_hbc_index.getMetadataInTxn(self, txn, vector_id, isNotFound);
    }

    pub fn getMetadataManySortedInTxn(self: *HBCIndex, txn: anytype, vector_ids: []const u64, out_metadata: []?[]const u8) !void {
        return try vectorindex_hbc_index.getMetadataManySortedInTxn(self, txn, vector_ids, out_metadata);
    }

    pub fn getMetadataManySortedInTxnWithScratch(
        self: *HBCIndex,
        txn: anytype,
        vector_ids: []const u64,
        out_metadata: []?[]const u8,
        lookup_storage: []FixedKeyLookup,
        key_views_storage: [][]const u8,
        values_storage: []?[]const u8,
    ) !void {
        return try vectorindex_hbc_index.getMetadataManySortedInTxnWithScratch(
            self,
            txn,
            vector_ids,
            out_metadata,
            lookup_storage,
            key_views_storage,
            values_storage,
        );
    }

    fn loadNodeSplitRange(self: *HBCIndex, txn: anytype, node_id: u64) !?NodeSplitRange {
        return try vectorindex_hbc_index.loadNodeSplitRange(self, txn, node_id, isNotFound);
    }

    fn computeNodeSplitRange(self: *HBCIndex, txn: anytype, node: *const Node) !?NodeSplitRange {
        return try vectorindex_hbc_index.computeNodeSplitRange(self, txn, node, isNotFound);
    }

    fn saveNodeSplitRange(self: *HBCIndex, txn: anytype, node: *const Node) !void {
        try vectorindex_hbc_index.saveNodeSplitRange(self, txn, node, isNotFound);
    }

    pub fn putNodeSplitRange(self: *HBCIndex, txn: anytype, node_id: u64, range: ?*const NodeSplitRange) !void {
        try vectorindex_hbc_index.putNodeSplitRange(self, txn, node_id, range, isNotFound);
    }

    pub fn getNodeSplitRange(self: *HBCIndex, node_id: u64) !?NodeSplitRange {
        return try vectorindex_hbc_index.getNodeSplitRange(self, node_id, isNotFound);
    }

    fn classifyNodeForSplitInTxn(self: *HBCIndex, txn: anytype, node_id: u64, split_key: []const u8) !NodeSplitClass {
        return try vectorindex_hbc_index.classifyNodeForSplitInTxn(self, txn, node_id, split_key, isNotFound);
    }

    pub fn classifyNodeForSplit(self: *HBCIndex, node_id: u64, split_key: []const u8) !NodeSplitClass {
        return try vectorindex_hbc_index.classifyNodeForSplit(self, node_id, split_key, isNotFound);
    }

    pub fn splitPlanningStats(self: *HBCIndex, split_key: []const u8) !SplitPlanningStats {
        var txn = try self.beginRuntimeReadTxn();
        defer txn.abort();

        var pending = std.ArrayListUnmanaged(u64).empty;
        defer pending.deinit(self.alloc);
        try pending.append(self.alloc, self.metadata.root_node);

        var planning = SplitPlanningStats{};
        while (pending.pop()) |node_id| {
            var node = try self.loadNode(&txn, node_id);
            defer node.deinit(self.alloc);

            if (node.is_leaf) {
                planning.leaves += 1;
            } else {
                planning.internal += 1;
                try pending.appendSlice(self.alloc, node.children);
            }

            switch (try self.classifyNodeForSplitInTxn(&txn, node_id, split_key)) {
                .left_only => planning.left_only += 1,
                .right_only => planning.right_only += 1,
                .mixed => planning.mixed += 1,
                .unknown => planning.unknown += 1,
            }
        }
        return planning;
    }

    pub fn buildSplitReusePlan(self: *HBCIndex, split_key: []const u8) !SplitReusePlan {
        var txn = try self.beginRuntimeReadTxn();
        defer txn.abort();

        var pending = std.ArrayListUnmanaged(u64).empty;
        defer pending.deinit(self.alloc);
        try pending.append(self.alloc, self.metadata.root_node);

        var right_only_roots = std.ArrayListUnmanaged(u64).empty;
        errdefer right_only_roots.deinit(self.alloc);
        var mixed_leaves = std.ArrayListUnmanaged(u64).empty;
        errdefer mixed_leaves.deinit(self.alloc);

        while (pending.pop()) |node_id| {
            const class = try self.classifyNodeForSplitInTxn(&txn, node_id, split_key);
            switch (class) {
                .left_only => {},
                .right_only => try right_only_roots.append(self.alloc, node_id),
                .mixed => {
                    var node = try self.loadNode(&txn, node_id);
                    defer node.deinit(self.alloc);
                    if (node.is_leaf) {
                        try mixed_leaves.append(self.alloc, node_id);
                    } else {
                        try pending.appendSlice(self.alloc, node.children);
                    }
                },
                .unknown => {
                    var node = try self.loadNode(&txn, node_id);
                    defer node.deinit(self.alloc);
                    if (node.is_leaf) {
                        try mixed_leaves.append(self.alloc, node_id);
                    } else {
                        try pending.appendSlice(self.alloc, node.children);
                    }
                },
            }
        }

        return .{
            .right_only_roots = try right_only_roots.toOwnedSlice(self.alloc),
            .mixed_leaves = try mixed_leaves.toOwnedSlice(self.alloc),
        };
    }

    pub fn estimateSplitRebuildWork(self: *HBCIndex, split_key: []const u8) !SplitRebuildWork {
        var txn = try self.beginRuntimeReadTxn();
        defer txn.abort();

        var plan = try self.buildSplitReusePlan(split_key);
        defer plan.deinit(self.alloc);

        var work = SplitRebuildWork{
            .right_only_roots = plan.right_only_roots.len,
            .mixed_leaves = plan.mixed_leaves.len,
        };

        for (plan.right_only_roots) |node_id| {
            work.right_only_members += try self.subtreeMemberCount(&txn, node_id);
        }
        for (plan.mixed_leaves) |node_id| {
            work.mixed_right_members += try self.mixedLeafRightMemberCount(&txn, node_id, split_key);
        }
        return work;
    }

    pub fn collectSplitMembers(self: *HBCIndex, split_key: []const u8) !SplitMemberPlan {
        var txn = try self.beginRuntimeReadTxn();
        defer txn.abort();

        var plan = try self.buildSplitReusePlan(split_key);
        defer plan.deinit(self.alloc);

        var right_only_members = std.ArrayListUnmanaged(u64).empty;
        errdefer right_only_members.deinit(self.alloc);
        var mixed_right_members = std.ArrayListUnmanaged(u64).empty;
        errdefer mixed_right_members.deinit(self.alloc);

        for (plan.right_only_roots) |node_id| {
            try self.appendSubtreeMembers(&txn, node_id, &right_only_members);
        }
        for (plan.mixed_leaves) |node_id| {
            try self.appendMixedLeafRightMembers(&txn, node_id, split_key, &mixed_right_members);
        }

        return .{
            .right_only_members = try right_only_members.toOwnedSlice(self.alloc),
            .mixed_right_members = try mixed_right_members.toOwnedSlice(self.alloc),
        };
    }

    fn subtreeMemberCount(self: *HBCIndex, txn: anytype, node_id: u64) !usize {
        var node = try self.loadNode(txn, node_id);
        defer node.deinit(self.alloc);
        if (node.is_leaf) return node.members.len;

        var total: usize = 0;
        for (node.children) |child_id| {
            total += try self.subtreeMemberCount(txn, child_id);
        }
        return total;
    }

    fn mixedLeafRightMemberCount(self: *HBCIndex, txn: anytype, node_id: u64, split_key: []const u8) !usize {
        var node = try self.loadNode(txn, node_id);
        defer node.deinit(self.alloc);
        if (!node.is_leaf) return error.ExpectedLeaf;

        var count: usize = 0;
        for (node.members) |member_id| {
            const metadata = (try self.loadMetadataRaw(txn, member_id)) orelse continue;
            if (std.mem.order(u8, metadata, split_key) != .lt) count += 1;
        }
        return count;
    }

    fn appendSubtreeMembers(
        self: *HBCIndex,
        txn: anytype,
        node_id: u64,
        out: *std.ArrayListUnmanaged(u64),
    ) !void {
        var node = try self.loadNode(txn, node_id);
        defer node.deinit(self.alloc);
        if (node.is_leaf) {
            try out.appendSlice(self.alloc, node.members);
            return;
        }
        for (node.children) |child_id| {
            try self.appendSubtreeMembers(txn, child_id, out);
        }
    }

    fn appendMixedLeafRightMembers(
        self: *HBCIndex,
        txn: anytype,
        node_id: u64,
        split_key: []const u8,
        out: *std.ArrayListUnmanaged(u64),
    ) !void {
        var node = try self.loadNode(txn, node_id);
        defer node.deinit(self.alloc);
        if (!node.is_leaf) return error.ExpectedLeaf;

        for (node.members) |member_id| {
            const metadata = (try self.loadMetadataRaw(txn, member_id)) orelse continue;
            if (std.mem.order(u8, metadata, split_key) != .lt) {
                try out.append(self.alloc, member_id);
            }
        }
    }

    // ========================================================================
    // Quantized vector set I/O
    // ========================================================================

    pub fn saveQuantized(self: *HBCIndex, txn: anytype, node_id: u64, qs: *const QuantizedSet) !void {
        try vectorindex_hbc_index.saveQuantized(self, txn, node_id, qs, nowNs, elapsedSince);
    }

    pub fn putQuantizedCached(self: *HBCIndex, txn: anytype, node_id: u64, qs: *const QuantizedSet) !void {
        try vectorindex_hbc_index.putQuantizedCached(self, txn, node_id, qs, nowNs, elapsedSince);
    }

    pub fn loadQuantized(self: *HBCIndex, txn: anytype, node_id: u64, is_root: bool, expected_count: usize) !QuantizedSet {
        return try vectorindex_hbc_index.loadQuantized(self, txn, node_id, is_root, expected_count, isNotFound);
    }

    pub fn getQuantized(self: *HBCIndex, txn: anytype, node_id: u64, is_root: bool, expected_count: usize) !?*const QuantizedSet {
        return try vectorindex_hbc_index.getQuantized(self, txn, node_id, is_root, expected_count, isNotFound);
    }

    pub fn getQuantizedProfiled(self: *HBCIndex, txn: anytype, node_id: u64, is_root: bool, expected_count: usize, profile: *SearchProfile) !?*const QuantizedSet {
        return try vectorindex_hbc_index.getQuantizedProfiled(self, txn, node_id, is_root, expected_count, profile, isNotFound, nowNs, elapsedSince);
    }

    pub fn estimateQuantizedDistances(
        self: *HBCIndex,
        qs: *const QuantizedSet,
        query: []const f32,
        query_measure: f32,
        distances: []f32,
        error_bounds: []f32,
        scratch: *quantizer_mod.RaBitQuantizer.EstimateScratch,
    ) !void {
        try vectorindex_hbc_index.estimateQuantizedDistances(self, qs, query, query_measure, distances, error_bounds, scratch);
    }

    fn refreshAncestorSplitRanges(self: *HBCIndex, txn: anytype, parent_id: u64) !void {
        try vectorindex_hbc_index.refreshAncestorSplitRanges(self, txn, parent_id);
    }

    fn extendAncestorSplitRanges(
        self: *HBCIndex,
        txn: anytype,
        parent_id: u64,
        child_range: *const NodeSplitRange,
    ) !void {
        try vectorindex_hbc_index.extendAncestorSplitRanges(self, txn, parent_id, child_range);
    }

    pub fn refreshQuantized(self: *HBCIndex, txn: anytype, node: *const Node) !void {
        try vectorindex_hbc_index.refreshQuantized(self, txn, node, nowNs, elapsedSince);
    }

    pub fn refreshQuantizedWithOptions(self: *HBCIndex, txn: anytype, node: *const Node, options: BatchInsertOptions) !void {
        try vectorindex_hbc_index.refreshQuantizedWithOptions(self, txn, node, options, nowNs, elapsedSince);
    }

    // ========================================================================
    // Balanced binary k-means++ splitting
    // ========================================================================

    /// Split a set of vectors into two groups using balanced binary k-means++.
    /// Returns (left_centroid, left_ids, right_centroid, right_ids).
    pub fn splitVectorSet(
        self: *HBCIndex,
        vectors: *const vec.Set,
        ids: []const u64,
    ) !SplitResult {
        return try vectorindex_hbc_index.splitVectorSet(self, vectors, ids);
    }

    pub fn getHilbert(self: *HBCIndex) !*vec.Hilbert {
        if (self.hilbert == null) {
            self.hilbert = try vec.Hilbert.init(self.alloc, self.config.dims);
        }
        return &self.hilbert.?;
    }

    pub fn minLeafOccupancy(self: *const HBCIndex) usize {
        return vectorindex_hbc_index.minLeafOccupancy(self);
    }

    const BatchVectorContext = struct {
        items: []const BatchInsertItem,
        map: std.AutoHashMapUnmanaged(u64, usize) = .empty,
        accounted_bytes: u64 = 0,

        fn init(index: *HBCIndex, items: []const BatchInsertItem) !@This() {
            var self: @This() = .{ .items = items };
            errdefer self.deinit(index);
            try self.map.ensureTotalCapacity(index.alloc, @intCast(items.len));
            for (items, 0..) |item, item_index| {
                try self.map.put(index.alloc, item.vector_id, item_index);
            }
            self.accounted_bytes = @as(u64, @intCast(self.map.capacity())) * (@sizeOf(u64) + @sizeOf(usize));
            index.addApplyWorkspaceBytes(self.accounted_bytes);
            return self;
        }

        fn deinit(self: *@This(), index: *HBCIndex) void {
            index.releaseApplyWorkspaceBytes(self.accounted_bytes);
            self.map.deinit(index.alloc);
            self.* = undefined;
        }

        fn lookup(ptr: *const anyopaque, vector_id: u64) ?[]const f32 {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const item_index = self.map.get(vector_id) orelse return null;
            return self.items[item_index].vector;
        }

        fn erased(self: *const @This()) vectorindex_hbc_runtime.BatchVectorLookup {
            return .{
                .ptr = self,
                .getFn = lookup,
            };
        }
    };

    fn optionsWithBatchVectors(
        options: BatchInsertOptions,
        batch_vectors: vectorindex_hbc_runtime.BatchVectorLookup,
    ) BatchInsertOptions {
        var next = options;
        if (next.batch_vectors == null) next.batch_vectors = batch_vectors;
        return next;
    }

    fn recomputeLeafCentroid(self: *HBCIndex, txn: anytype, leaf: *Node) !void {
        try vectorindex_hbc_index.recomputeLeafCentroid(self, txn, leaf);
    }

    pub fn recomputeInternalCentroid(self: *HBCIndex, txn: anytype, node: *Node) !void {
        try vectorindex_hbc_index.recomputeInternalCentroid(self, txn, node);
    }

    pub fn collapseSingleChildParents(self: *HBCIndex, txn: anytype, start_node_id: u64) !void {
        try vectorindex_hbc_index.collapseSingleChildParents(self, txn, start_node_id);
    }

    // ========================================================================
    // Insert
    // ========================================================================

    /// Insert a vector into the index.
    pub fn insert(self: *HBCIndex, vector_id: u64, vector_data: []const f32) !void {
        try vectorindex_hbc_index.insert(self, vector_id, vector_data, nowNs, elapsedSince);
    }

    pub fn insertWithMetadata(self: *HBCIndex, vector_id: u64, vector_data: []const f32, metadata_value: []const u8) !void {
        try vectorindex_hbc_index.insertWithMetadata(self, vector_id, vector_data, metadata_value, nowNs, elapsedSince);
    }

    pub fn batchInsertWithMetadata(self: *HBCIndex, items: []const BatchInsertItem) !void {
        try vectorindex_hbc_index.batchInsertWithMetadata(self, items, nowNs, elapsedSince);
    }

    pub fn batchApply(self: *HBCIndex, writes: []const BatchInsertItem, deletes: []const u64) !void {
        try vectorindex_hbc_index.batchApply(self, writes, deletes, nowNs, elapsedSince);
    }

    pub fn batchApplyOptions(
        self: *HBCIndex,
        writes: []const BatchInsertItem,
        deletes: []const u64,
        options: BatchInsertOptions,
    ) !void {
        self.admitBatchVectorsToBulkSplitWorkspace(writes, options.assume_absent_ids);
        if (writes.len == 0 or options.batch_vectors != null) {
            try vectorindex_hbc_index.batchApplyOptions(self, writes, deletes, options, nowNs, elapsedSince);
            return;
        }
        var batch_vectors = try BatchVectorContext.init(self, writes);
        defer batch_vectors.deinit(self);
        try vectorindex_hbc_index.batchApplyOptions(self, writes, deletes, optionsWithBatchVectors(options, batch_vectors.erased()), nowNs, elapsedSince);
    }

    pub fn batchInsertWithMetadataOptions(self: *HBCIndex, items: []const BatchInsertItem, options: BatchInsertOptions) !void {
        self.admitBatchVectorsToBulkSplitWorkspace(items, options.assume_absent_ids);
        if (items.len == 0 or options.batch_vectors != null) {
            try vectorindex_hbc_index.batchInsertWithMetadataOptions(self, items, options, nowNs, elapsedSince);
            return;
        }
        var batch_vectors = try BatchVectorContext.init(self, items);
        defer batch_vectors.deinit(self);
        try vectorindex_hbc_index.batchInsertWithMetadataOptions(self, items, optionsWithBatchVectors(options, batch_vectors.erased()), nowNs, elapsedSince);
    }

    pub fn batchInsertWithMetadataTxn(self: *HBCIndex, txn: anytype, items: []const BatchInsertItem) !void {
        try vectorindex_hbc_index.batchInsertWithMetadataTxn(self, txn, items);
    }

    pub fn bulkBuildWithMetadata(self: *HBCIndex, items: []const BatchInsertItem) !void {
        try vectorindex_hbc_index.bulkBuildWithMetadata(self, items, nowNs, elapsedSince);
    }

    pub fn bulkBuildWithMetadataOptions(self: *HBCIndex, items: []const BatchInsertItem, options: BulkBuildOptions) !void {
        try vectorindex_hbc_index.bulkBuildWithMetadataOptions(self, items, options, nowNs, elapsedSince);
    }

    pub fn bulkBuildWithMetadataTxn(self: *HBCIndex, txn: anytype, items: []const BatchInsertItem) !void {
        try vectorindex_hbc_index.bulkBuildWithMetadataTxn(self, txn, items);
    }

    pub fn bulkBuildPreparedInputsTxn(self: *HBCIndex, txn: anytype, inputs: []const PreparedBulkBuildInput) !void {
        try vectorindex_hbc_index.bulkBuildPreparedInputsTxn(self, txn, inputs);
    }

    pub fn bulkBuildPreparedInputsTxnOptions(
        self: *HBCIndex,
        txn: anytype,
        inputs: []const PreparedBulkBuildInput,
        options: BulkBuildOptions,
    ) !void {
        try vectorindex_hbc_index.bulkBuildPreparedInputsTxnOptions(self, txn, inputs, options, nowNs, elapsedSince);
    }

    pub fn bulkBuildWithMetadataTxnOptions(
        self: *HBCIndex,
        txn: anytype,
        items: []const BatchInsertItem,
        options: BulkBuildOptions,
    ) !void {
        try vectorindex_hbc_index.bulkBuildWithMetadataTxnOptions(self, txn, items, options, nowNs, elapsedSince);
    }

    pub fn buildBulkRecursiveFromInputs(
        self: *HBCIndex,
        txn: anytype,
        inputs: []const PreparedBulkBuildInput,
    ) !BuiltBulkNode {
        return try vectorindex_hbc_index.buildBulkRecursiveFromInputs(self, txn, inputs);
    }

    pub fn batchInsertWithMetadataTxnOptions(
        self: *HBCIndex,
        txn: anytype,
        items: []const BatchInsertItem,
        options: BatchInsertOptions,
    ) !void {
        try vectorindex_hbc_index.batchInsertWithMetadataTxnOptions(self, txn, items, options);
    }

    pub fn prepareEmptyBulkBuild(self: *HBCIndex, txn: anytype, items: []const BatchInsertItem) !void {
        if (self.metadata.active_count != 0) return error.IndexNotEmpty;

        var seen = std.AutoHashMapUnmanaged(u64, void).empty;
        defer seen.deinit(self.alloc);
        try seen.ensureTotalCapacity(self.alloc, @intCast(items.len));
        for (items) |item| {
            if (seen.contains(item.vector_id)) return error.DuplicateVectorId;
            seen.putAssumeCapacity(item.vector_id, {});
        }

        if (self.metadata.root_node != 0) {
            self.deleteNode(txn, self.metadata.root_node) catch |err| {
                if (isNotFound(err)) {} else return err;
            };
        }
        self.clearNodeCache();
        self.clearQuantizedCache();
        self.clearVectorCache();
        self.clearMetadataCache();
        self.metadata.root_node = 0;
        self.metadata.node_count = 0;
        self.metadata.active_count = 0;
    }

    pub fn prepareEmptyPreparedBulkBuild(self: *HBCIndex, txn: anytype, inputs: []const PreparedBulkBuildInput) !void {
        if (self.metadata.active_count != 0) return error.IndexNotEmpty;

        var seen = std.AutoHashMapUnmanaged(u64, void).empty;
        defer seen.deinit(self.alloc);
        try seen.ensureTotalCapacity(self.alloc, @intCast(inputs.len));
        for (inputs) |input| {
            if (seen.contains(input.vector_id)) return error.DuplicateVectorId;
            seen.putAssumeCapacity(input.vector_id, {});
        }

        if (self.metadata.root_node != 0) {
            self.deleteNode(txn, self.metadata.root_node) catch |err| {
                if (isNotFound(err)) {} else return err;
            };
        }
        self.clearNodeCache();
        self.clearQuantizedCache();
        self.clearVectorCache();
        self.clearMetadataCache();
        self.metadata.root_node = 0;
        self.metadata.node_count = 0;
        self.metadata.active_count = 0;
    }

    pub fn buildBulkHilbertSeeded(
        self: *HBCIndex,
        txn: anytype,
        inputs: []const PreparedBulkBuildInput,
    ) !BuiltBulkNode {
        return try vectorindex_hbc_index.buildBulkHilbertSeeded(self, txn, inputs);
    }

    pub fn buildBulkDocKeySeeded(
        self: *HBCIndex,
        txn: anytype,
        inputs: []const PreparedBulkBuildInput,
    ) !BuiltBulkNode {
        return try vectorindex_hbc_index.buildBulkDocKeySeeded(self, txn, inputs);
    }

    pub fn buildBulkKmeansFromInputs(
        self: *HBCIndex,
        txn: anytype,
        inputs: []const PreparedBulkBuildInput,
    ) !BuiltBulkNode {
        return try vectorindex_hbc_index.buildBulkKmeansFromInputs(self, txn, inputs);
    }

    pub fn ingestMembersFrom(self: *HBCIndex, src: *HBCIndex, member_ids: []const u64, batch_size: usize) !void {
        try vectorindex_hbc_transfer.ingestMembersFrom(self, src, member_ids, batch_size);
    }

    pub fn bulkBuildMembersFrom(self: *HBCIndex, src: *HBCIndex, member_ids: []const u64) !void {
        try vectorindex_hbc_transfer.bulkBuildMembersFrom(self, src, member_ids);
    }

    pub fn streamSplitMembers(
        self: *HBCIndex,
        split_key: []const u8,
        batch_size: usize,
        ctx: anytype,
        comptime consume: fn (@TypeOf(ctx), []const BatchInsertItem) anyerror!void,
    ) !usize {
        return try vectorindex_hbc_transfer.streamSplitMembers(self, split_key, batch_size, ctx, consume);
    }

    fn insertWithMetadataTxn(
        self: *HBCIndex,
        txn: anytype,
        vector_id: u64,
        vector_data: []const f32,
        metadata_value: []const u8,
        transformed_vector: []f32,
    ) !void {
        try vectorindex_hbc_index.insertWithMetadataTxn(
            self,
            txn,
            vector_id,
            vector_data,
            metadata_value,
            transformed_vector,
            nowNs,
            elapsedSince,
        );
    }

    pub fn insertWithMetadataTxnOptions(
        self: *HBCIndex,
        txn: anytype,
        vector_id: u64,
        vector_data: []const f32,
        pretransformed_vector: ?[]const f32,
        metadata_value: []const u8,
        transformed_vector: []f32,
        options: BatchInsertOptions,
    ) !void {
        try vectorindex_hbc_index.insertWithMetadataTxnOptions(
            self,
            txn,
            vector_id,
            vector_data,
            pretransformed_vector,
            metadata_value,
            transformed_vector,
            options,
            nowNs,
            elapsedSince,
        );
    }

    fn removeFromLeaf(self: *HBCIndex, txn: anytype, leaf_id: u64, vector_id: u64) !void {
        try vectorindex_hbc_index.removeFromLeaf(self, txn, leaf_id, vector_id);
    }

    /// Find the best leaf for a vector by traversing from root.
    fn findLeaf(self: *HBCIndex, txn: anytype, node_id: u64, query: []const f32) !u64 {
        return self.findLeafWithOptions(txn, node_id, query, true);
    }

    pub fn findLeafWithOptions(
        self: *HBCIndex,
        txn: anytype,
        node_id: u64,
        query: []const f32,
        allow_quantized: bool,
    ) !u64 {
        var handle = try self.acquireRoutingScratch();
        defer self.releaseRoutingScratch(&handle);
        return try self.findLeafWithOptionsScratch(txn, node_id, query, allow_quantized, &handle.scratch);
    }

    pub fn findLeafWithOptionsScratch(
        self: *HBCIndex,
        txn: anytype,
        node_id: u64,
        query: []const f32,
        allow_quantized: bool,
        scratch: *RoutingScratch,
    ) !u64 {
        try self.bindTxnLike(txn);
        var node = try self.loadNode(txn, node_id);
        defer node.deinit(self.alloc);
        if (node.is_leaf) return node_id;

        const n_children = node.children.len;
        try scratch.ensureCapacity(self.alloc, n_children);
        @memcpy(scratch.child_ids[0..n_children], node.children);
        const child_ids = scratch.child_ids[0..n_children];

        const query_measure: f32 = switch (self.config.metric) {
            .l2_squared => vec.dot(query, query),
            .cosine => vec.norm(query),
            .inner_product => 0,
        };
        var best_child: u64 = 0;
        var best_dist: f32 = std.math.inf(f32);

        if (allow_quantized and self.config.use_quantization) quantized_route: {
            var borrowed_quantized: ?BorrowedQuantized = self.borrowCachedQuantized(node_id);
            defer if (borrowed_quantized) |*borrowed| borrowed.deinit();

            var owned_quantized: ?QuantizedSet = null;
            defer if (owned_quantized) |*owned| owned.deinit(self.alloc);

            const quantized: *const QuantizedSet = if (borrowed_quantized) |*borrowed|
                borrowed.ptr()
            else blk: {
                owned_quantized = self.loadQuantized(txn, node_id, node.parent == 0, n_children) catch |err| {
                    if (isNotFound(err) or err == error.Corrupted) break :quantized_route;
                    return err;
                };
                break :blk &owned_quantized.?;
            };

            self.estimateQuantizedDistances(
                quantized,
                query,
                query_measure,
                scratch.distances[0..n_children],
                scratch.error_bounds[0..n_children],
                &scratch.estimate,
            ) catch {
                self.invalidateQuantizedCache(node_id);
                break :quantized_route;
            };
            for (child_ids, 0..) |child_id, i| {
                const dist = scratch.distances[i];
                if (dist < best_dist) {
                    best_dist = dist;
                    best_child = child_id;
                }
            }
            if (best_child != 0) {
                return self.findLeafWithOptionsScratch(txn, best_child, query, allow_quantized, scratch);
            }
        }

        for (child_ids) |child_id| {
            var child = self.loadNode(txn, child_id) catch continue;
            defer child.deinit(self.alloc);
            const dist = vec.distanceToQuery(query, query_measure, child.centroid, self.config.metric);
            if (dist < best_dist) {
                best_dist = dist;
                best_child = child_id;
            }
        }

        if (best_child == 0) {
            self.invalidateNodeCache(node_id);
            var fresh_node = self.loadNodeFromStorage(txn, node_id) catch |err| {
                if (isNotFound(err)) return error.Corrupted;
                return err;
            };
            defer fresh_node.deinit(self.alloc);
            if (fresh_node.is_leaf) return node_id;
            if (fresh_node.children.len == 0) return error.Corrupted;

            try scratch.ensureCapacity(self.alloc, fresh_node.children.len);
            @memcpy(scratch.child_ids[0..fresh_node.children.len], fresh_node.children);
            const fresh_child_ids = scratch.child_ids[0..fresh_node.children.len];

            for (fresh_child_ids) |child_id| {
                var child = self.loadNodeFromStorage(txn, child_id) catch |err| {
                    if (isNotFound(err)) continue;
                    return err;
                };
                defer child.deinit(self.alloc);
                const dist = vec.distanceToQuery(query, query_measure, child.centroid, self.config.metric);
                if (dist < best_dist) {
                    best_dist = dist;
                    best_child = child_id;
                }
            }
            if (best_child == 0) return error.Corrupted;
        }
        return self.findLeafWithOptionsScratch(txn, best_child, query, false, scratch);
    }

    pub fn collectCompetitiveInsertCandidatesScratch(
        _: *HBCIndex,
        child_ids: []const u64,
        distances: []const f32,
        error_bounds: []const f32,
        scratch: []vectorindex_types.PriorityItem,
    ) ![]vectorindex_types.PriorityItem {
        var competitive_len: usize = 0;

        outer: for (child_ids, 0..) |child_id, i| {
            const candidate: vectorindex_types.PriorityItem = .{
                .id = child_id,
                .distance = distances[i],
                .error_bound = error_bounds[i],
            };
            if (competitive_len == 0) {
                scratch[0] = candidate;
                competitive_len = 1;
                continue;
            }

            while (true) {
                const worst_idx = worstCompetitiveIndex(scratch[0..competitive_len]);
                const worst = scratch[worst_idx];
                if (!candidate.definitelyCloser(worst)) break;
                competitive_len -= 1;
                if (worst_idx != competitive_len) scratch[worst_idx] = scratch[competitive_len];
                if (competitive_len == 0) {
                    scratch[0] = candidate;
                    competitive_len = 1;
                    continue :outer;
                }
            }

            const worst_idx = worstCompetitiveIndex(scratch[0..competitive_len]);
            if (candidate.maybeCloser(scratch[worst_idx])) {
                scratch[competitive_len] = candidate;
                competitive_len += 1;
            }
        }

        return scratch[0..competitive_len];
    }

    pub fn collectCompetitiveInsertCandidates(
        self: *HBCIndex,
        child_ids: []const u64,
        distances: []const f32,
        error_bounds: []const f32,
    ) ![]vectorindex_types.PriorityItem {
        const scratch = try self.alloc.alloc(vectorindex_types.PriorityItem, child_ids.len);
        errdefer self.alloc.free(scratch);
        const competitive = try self.collectCompetitiveInsertCandidatesScratch(child_ids, distances, error_bounds, scratch);
        return self.alloc.realloc(scratch, competitive.len);
    }

    fn worstCompetitiveIndex(items: []const vectorindex_types.PriorityItem) usize {
        var worst_idx: usize = 0;
        for (items[1..], 1..) |item, idx| {
            if (item.distance > items[worst_idx].distance) worst_idx = idx;
        }
        return worst_idx;
    }

    // ========================================================================
    // Split operations
    // ========================================================================

    /// Split a leaf using balanced binary k-means++.
    pub fn splitLeaf(self: *HBCIndex, txn: anytype, leaf: *const Node) !void {
        try vectorindex_hbc_index.splitLeaf(self, txn, leaf);
    }

    pub fn splitLeafWithOptions(
        self: *HBCIndex,
        txn: anytype,
        leaf: *const Node,
        options: BatchInsertOptions,
    ) !void {
        try vectorindex_hbc_index.splitLeafWithOptions(self, txn, leaf, options, nowNsI128, elapsedSinceNs);
    }

    pub fn rebuildOversizedLeafKmeansWithOptions(
        self: *HBCIndex,
        txn: anytype,
        leaf: *const Node,
        options: BatchInsertOptions,
    ) !bool {
        return try vectorindex_hbc_index.rebuildOversizedLeafKmeansWithOptions(self, txn, leaf, options, nowNsI128, elapsedSinceNs);
    }

    pub fn maybeBuildKeyLocalLeafSplit(
        self: *HBCIndex,
        txn: anytype,
        member_ids: []const u64,
        vectors: *const vec.Set,
        current: *const SplitResult,
    ) !?SplitResult {
        return try vectorindex_hbc_index.maybeBuildKeyLocalLeafSplit(self, txn, member_ids, vectors, current);
    }

    /// Split an internal node using balanced binary k-means++ on child centroids.
    fn splitInternal(self: *HBCIndex, txn: anytype, node: *const Node) !void {
        try vectorindex_hbc_index.splitInternal(self, txn, node);
    }

    pub fn splitInternalWithOptions(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        options: BatchInsertOptions,
    ) !void {
        try vectorindex_hbc_index.splitInternalWithOptions(self, txn, node, options, nowNsI128, elapsedSinceNs);
    }

    // ========================================================================
    // Search
    // ========================================================================

    /// Search for the k nearest vectors. Returns results sorted by distance.
    pub fn search(self: *HBCIndex, query: []const f32, k: usize) !SearchResults {
        return try vectorindex_hbc_index.search(self, query, k, nowNs, elapsedSince);
    }

    pub fn searchWithRequest(self: *HBCIndex, req: SearchRequest) !SearchResults {
        return try vectorindex_hbc_index.searchWithRequest(self, req, nowNs, elapsedSince);
    }

    pub fn searchProfiled(self: *HBCIndex, query: []const f32, k: usize) !ProfiledSearchResults {
        return try vectorindex_hbc_index.searchProfiled(self, query, k, nowNs, elapsedSince);
    }

    pub fn searchProfiledRequest(self: *HBCIndex, req: SearchRequest) !ProfiledSearchResults {
        return try vectorindex_hbc_index.searchProfiledRequest(self, req, nowNs, elapsedSince);
    }

    /// Add children of a node to the candidate queue.
    fn addChildCandidates(
        self: *HBCIndex,
        txn: anytype,
        node: *const Node,
        query: []const f32,
        query_measure: f32,
        candidates: *std.PriorityQueue(PriorityItem, void, candidateLessThan),
        scratch: *SearchScratch,
        profile: *SearchProfile,
    ) !void {
        try vectorindex_hbc_index.addChildCandidates(self, txn, node, query, query_measure, candidates, scratch, profile, nowNs, elapsedSince);
    }

    /// Score all members of a leaf against the query using exact distances.
    fn scoreLeafMembers(
        self: *HBCIndex,
        txn: anytype,
        leaf: *const Node,
        approx_query: []const f32,
        approx_query_measure: f32,
        exact_query: []const f32,
        exact_query_measure: f32,
        req: SearchRequest,
        filter_state: *const RequestFilterState,
        results: *ApproxSearchResults,
        scratch: *SearchScratch,
        profile: *SearchProfile,
    ) !void {
        try vectorindex_hbc_index.scoreLeafMembers(
            self,
            txn,
            leaf,
            approx_query,
            approx_query_measure,
            exact_query,
            exact_query_measure,
            req,
            filter_state,
            results,
            scratch,
            profile,
            nowNs,
            elapsedSince,
        );
    }

    fn rerankResults(
        self: *HBCIndex,
        txn: anytype,
        approx_results: *const ApproxSearchResults,
        query: []const f32,
        query_measure: f32,
        req: SearchRequest,
        filter_state: *const RequestFilterState,
        scratch: *SearchScratch,
        profile: *SearchProfile,
    ) !SearchResults {
        return try vectorindex_hbc_index.rerankResults(self, txn, approx_results, query, query_measure, req, filter_state, scratch, profile, nowNs, elapsedSince);
    }

    fn populateMetadata(self: *HBCIndex, txn: anytype, results: *SearchResults) !void {
        try vectorindex_hbc_index.populateMetadata(self, txn, results);
    }

    fn memberMatchesRequest(
        self: *HBCIndex,
        txn: anytype,
        vector_id: u64,
        distance: f32,
        error_bound: f32,
        req: SearchRequest,
        filter_state: *const RequestFilterState,
        approximate: bool,
    ) !bool {
        return try vectorindex_hbc_index.memberMatchesRequest(self, txn, vector_id, distance, error_bound, req, filter_state, approximate);
    }

    // ========================================================================
    // Delete
    // ========================================================================

    /// Delete a vector from the index by ID.
    pub fn delete(self: *HBCIndex, vector_id: u64) !void {
        try vectorindex_hbc_index.delete(self, vector_id);
    }

    pub fn batchDelete(self: *HBCIndex, vector_ids: []const u64) !void {
        try vectorindex_hbc_index.batchDelete(self, vector_ids);
    }

    fn deleteTxn(self: *HBCIndex, txn: anytype, vector_id: u64) !void {
        try vectorindex_hbc_index.deleteTxn(self, txn, vector_id);
    }

    // ========================================================================
    // Tree link consistency
    // ========================================================================

    pub fn noteTreeLinkInconsistency(self: *HBCIndex) void {
        self.link_repair_pending.store(true, .release);
    }

    pub fn treeLinkRepairPending(self: *const HBCIndex) bool {
        return self.link_repair_pending.load(.acquire);
    }

    /// Read-only structural invariant check (see hbc_index.verifyTreeLinks).
    pub fn verifyTreeLinks(self: *HBCIndex) !vectorindex_hbc_index.TreeLinkReport {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        return try vectorindex_hbc_index.verifyTreeLinks(self, &txn);
    }

    /// Bounded repair sweep (see hbc_index.repairTreeLinks). Clears the
    /// pending flag once a sweep completes within budget.
    pub fn repairTreeLinks(self: *HBCIndex, max_nodes: usize) !vectorindex_hbc_index.TreeLinkRepairReport {
        const posting_capture = try self.beginExperimentalPostingMaintenanceCapture();
        errdefer if (posting_capture != null) self.cancelExperimentalPostingMaintenanceCapture();
        const report = try vectorindex_hbc_index.repairLinks(self, max_nodes);
        if (posting_capture) |capture| try self.finishExperimentalPostingMaintenanceCapture(capture);
        if (report.completed) self.link_repair_pending.store(false, .release);
        return report;
    }

    /// Runs a repair sweep only when a write path flagged an inconsistency.
    pub fn maybeRepairTreeLinks(self: *HBCIndex, max_nodes: usize) !?vectorindex_hbc_index.TreeLinkRepairReport {
        if (!self.treeLinkRepairPending()) return null;
        return try self.repairTreeLinks(max_nodes);
    }

    // ========================================================================
    // Stats
    // ========================================================================

    pub fn stats(self: *const HBCIndex) IndexStats {
        return .{
            .dims = self.metadata.dims,
            .active_count = self.publishedActiveCount(),
            .node_count = self.publishedNodeCount(),
            .root_node = self.publishedRootNode(),
            .branching_factor = self.metadata.branching_factor,
            .leaf_size = self.metadata.leaf_size,
        };
    }

    pub fn debugLeafForVector(self: *HBCIndex, vector_id: u64) !?u64 {
        return try vectorindex_hbc_debug.debugLeafForVector(self, vector_id, isNotFound);
    }

    pub fn debugLeafMembers(self: *HBCIndex, alloc: Allocator, leaf_id: u64) ![]u64 {
        return try vectorindex_hbc_debug.debugLeafMembers(self, alloc, leaf_id);
    }

    pub fn debugScanLeafForVector(self: *HBCIndex, vector_id: u64) !?u64 {
        return try vectorindex_hbc_debug.debugScanLeafForVector(self, vector_id);
    }

    pub fn debugDumpNodes(self: *HBCIndex, alloc: Allocator) ![]HBCDebugNode {
        return try vectorindex_hbc_debug.debugDumpNodes(self, alloc);
    }

    pub fn debugScoreLeaf(self: *HBCIndex, alloc: Allocator, leaf_id: u64, query: []const f32) ![]DebugLeafScore {
        return try vectorindex_hbc_debug.debugScoreLeaf(self, alloc, leaf_id, query);
    }

    pub fn debugScoreLeafFreshQuantized(self: *HBCIndex, alloc: Allocator, leaf_id: u64, query: []const f32) ![]DebugLeafScore {
        return try vectorindex_hbc_debug.debugScoreLeafFreshQuantized(self, alloc, leaf_id, query);
    }

    pub fn debugLeafCentroidL2Error(self: *HBCIndex, alloc: Allocator, leaf_id: u64) !f32 {
        return try vectorindex_hbc_debug.debugLeafCentroidL2Error(self, alloc, leaf_id);
    }

    pub fn debugLeafCentroid(self: *HBCIndex, alloc: Allocator, leaf_id: u64) ![]f32 {
        return try vectorindex_hbc_debug.debugLeafCentroid(self, alloc, leaf_id);
    }

    pub fn debugRootChildDistances(self: *HBCIndex, alloc: Allocator, query: []const f32) ![]DebugNodeDistance {
        return try vectorindex_hbc_debug.debugRootChildDistances(self, alloc, query);
    }

    pub fn debugFindLeafForQuery(self: *HBCIndex, query: []const f32, allow_quantized: bool) !u64 {
        return try vectorindex_hbc_debug.debugFindLeafForQuery(self, query, allow_quantized);
    }

    pub fn debugChildDistances(self: *HBCIndex, alloc: Allocator, node_id: u64, query: []const f32) ![]DebugNodeDistance {
        return try vectorindex_hbc_debug.debugChildDistances(self, alloc, node_id, query);
    }

    pub fn resetWriteProfile(self: *HBCIndex) void {
        self.write_profile = .{};
    }

    pub fn getWriteProfile(self: *const HBCIndex) WriteProfile {
        return self.write_profile;
    }

    pub fn repairDirtyPostings(self: *HBCIndex) !PostingMaintenanceResult {
        return try self.repairDirtyPostingsWithOptions(.{});
    }

    pub fn repairDirtyPostingsWithOptions(self: *HBCIndex, options: PostingMaintenanceOptions) !PostingMaintenanceResult {
        const posting_capture = try self.beginExperimentalPostingMaintenanceCapture();
        errdefer if (posting_capture != null) self.cancelExperimentalPostingMaintenanceCapture();
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        const result = try vectorindex_hbc_index.repairDirtyPostingsTxnWithOptions(self, &txn, options);
        const commit_start = nowNs();
        self.beginPublishedSearchStateRefresh();
        errdefer self.abortPublishedSearchStateRefresh();
        try commitTxn(&txn);
        self.noteExperimentalPostingWalAuthorityCommit();
        self.write_profile.insert_commit_ns += elapsedSince(commit_start);
        self.finishPublishedSearchStateRefresh();
        if (posting_capture) |capture| try self.finishExperimentalPostingMaintenanceCapture(capture);
        return result;
    }

    pub fn postingBacklogStats(self: *HBCIndex) !PostingBacklogStats {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        return try vectorindex_hbc_index.postingBacklogStatsTxn(self, &txn);
    }

    pub fn writePostingBacklogStats(self: *HBCIndex, writer: *std.Io.Writer) !void {
        const backlog = try self.postingBacklogStats();
        try backlog.write(writer);
    }
};

// ============================================================================
// Search results (bounded max-heap)
// ============================================================================

pub const SearchResult = vectorindex_search_results.SearchResult;
const ApproxSearchResult = vectorindex_search_results.ApproxSearchResult;
pub const SearchRequest = vectorindex_search_types.SearchRequest;
pub const CancellationToken = vectorindex_search_types.CancellationToken;
pub const SearchProfile = vectorindex_search_types.SearchProfile;

pub const WriteProfile = vectorindex_hbc_runtime.WriteProfile;
pub const BatchInsertItem = vectorindex_hbc_runtime.BatchInsertItem;
pub const FixedKeyLookup = vectorindex_search_runtime.RerankLookup;
pub const BatchInsertOptions = vectorindex_hbc_runtime.BatchInsertOptions;
pub const VectorId = vectorindex_posting.VectorId;
pub const PostingId = vectorindex_posting.PostingId;
pub const PostingView = vectorindex_posting.PostingView;
pub const PostingState = vectorindex_posting.PostingState;
pub const PostingMaintenanceOptions = vectorindex_posting.PostingMaintenanceOptions;
pub const PostingMaintenanceResult = vectorindex_posting.PostingMaintenanceResult;
pub const PostingBacklogStats = vectorindex_posting.PostingBacklogStats;
pub const PostingStore = vectorindex_posting.PostingStore;
pub const AssignmentMap = vectorindex_posting.AssignmentMap;
pub const CentroidDirectory = vectorindex_posting.CentroidDirectory;

pub const BulkBuildOptions = vectorindex_bulk_build.BulkBuildOptions;
pub const PreparedBulkBuildInput = vectorindex_bulk_build.PreparedBulkBuildInput;

const BuiltBulkNode = vectorindex_hbc_index.BuiltBulkNode;

pub const ProfiledSearchResults = vectorindex_search_types.ProfiledSearchResults;
const RequestFilterState = vectorindex_search_types.RequestFilterState;

const SearchScratch = vectorindex_search_runtime.SearchScratch;

const ScratchHandle = vectorindex_hbc_runtime.ScratchHandle;

pub const SearchResults = vectorindex_search_results.SearchResults;
const ApproxSearchResults = vectorindex_search_results.ApproxSearchResults;

pub const DebugLeafScore = vectorindex_search_types.DebugLeafScore;
pub const DebugNodeDistance = vectorindex_search_types.DebugNodeDistance;
pub const IndexStats = vectorindex_search_types.IndexStats;
pub const HBCDebugNode = vectorindex_search_types.HBCDebugNode;

// ============================================================================
// Tests
// ============================================================================

test "create and open index" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{ .dims = 4 });
        defer idx.close();

        const s = idx.stats();
        try std.testing.expectEqual(@as(u32, 4), s.dims);
        try std.testing.expectEqual(@as(u64, 0), s.active_count);
        try std.testing.expectEqual(@as(u64, 1), s.node_count);
    }

    // Reopen and verify persistence
    {
        var idx = try HBCIndex.open(alloc, path, .{ .dims = 4 });
        defer idx.close();
        try std.testing.expectEqual(@as(u32, 4), idx.stats().dims);
    }
}

test "hbc randomized insert delete churn preserves tree link invariants" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    // Small fanout forces frequent splits, sibling merges, and single-child
    // collapses — the maintenance operations that rewrite parent/child links.
    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .leaf_size = 4,
        .branching_factor = 4,
    });
    defer idx.close();

    var prng = std.Random.DefaultPrng.init(0x5eed_11ab);
    const random = prng.random();

    var live = std.ArrayListUnmanaged(u64).empty;
    defer live.deinit(alloc);
    var next_id: u64 = 1;

    var op: usize = 0;
    while (op < 1200) : (op += 1) {
        const do_insert = live.items.len < 8 or random.intRangeLessThan(u8, 0, 100) < 60;
        if (do_insert) {
            var v: [4]f32 = undefined;
            for (&v) |*x| x.* = random.float(f32) * 2.0 - 1.0;
            try idx.insert(next_id, &v);
            try live.append(alloc, next_id);
            next_id += 1;
        } else if (random.boolean() or live.items.len < 4) {
            const pick = random.intRangeLessThan(usize, 0, live.items.len);
            const vid = live.swapRemove(pick);
            try idx.delete(vid);
        } else {
            var batch: [6]u64 = undefined;
            const want = @min(live.items.len, random.intRangeLessThan(usize, 2, 7));
            var i: usize = 0;
            while (i < want) : (i += 1) {
                const pick = random.intRangeLessThan(usize, 0, live.items.len);
                batch[i] = live.swapRemove(pick);
            }
            try idx.batchDelete(batch[0..want]);
        }

        if (op % 50 == 49) {
            const report = try idx.verifyTreeLinks();
            if (!report.consistent()) {
                std.debug.print("tree links inconsistent after op {d}: {any}\n", .{ op, report });
                return error.TestUnexpectedResult;
            }
            try std.testing.expect(!idx.treeLinkRepairPending());
        }
    }

    const final_report = try idx.verifyTreeLinks();
    if (!final_report.consistent()) {
        std.debug.print("tree links inconsistent at end: {any}\n", .{final_report});
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(u64, @intCast(live.items.len)), idx.stats().active_count);
}

test "hbc repairTreeLinks clears dangling references and restores consistency" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .leaf_size = 4,
        .branching_factor = 4,
    });
    defer idx.close();

    var prng = std.Random.DefaultPrng.init(0xfee1_600d);
    const random = prng.random();
    var id: u64 = 1;
    while (id <= 60) : (id += 1) {
        var v: [4]f32 = undefined;
        for (&v) |*x| x.* = random.float(f32) * 2.0 - 1.0;
        try idx.insert(id, &v);
    }

    // Corrupt the tree the way the field incident did: a leaf node vanishes
    // while its parent still lists it and vec→leaf entries still point at it.
    const victim_leaf = (try idx.debugLeafForVector(7)) orelse return error.TestUnexpectedResult;
    const victim_members = try idx.debugLeafMembers(alloc, victim_leaf);
    defer alloc.free(victim_members);
    try std.testing.expect(victim_members.len > 0);
    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        try idx.deleteNode(&txn, victim_leaf);
        try txn.commit();
    }

    const broken = try idx.verifyTreeLinks();
    try std.testing.expect(!broken.consistent());
    try std.testing.expect(broken.dangling_children >= 1);

    // Search still serves while the tree is inconsistent (tolerant descent).
    {
        const query = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
        var results = try idx.search(&query, 5);
        defer results.deinit();
        try std.testing.expect(results.getHits().len > 0);
    }

    // Deleting a vector whose leaf is gone cleans up instead of erroring,
    // and flags the index for repair.
    try idx.delete(victim_members[0]);
    try std.testing.expect(idx.treeLinkRepairPending());

    const repair = try idx.repairTreeLinks(10_000);
    try std.testing.expect(repair.completed);
    try std.testing.expect(repair.dangling_children_removed >= 1);
    try std.testing.expect(!idx.treeLinkRepairPending());

    const healed = try idx.verifyTreeLinks();
    if (!healed.consistent()) {
        std.debug.print("tree links inconsistent after repair: {any}\n", .{healed});
        return error.TestUnexpectedResult;
    }

    // The index stays fully usable after repair.
    const probe = [_]f32{ 0.5, -0.25, 0.75, -0.5 };
    try idx.insert(1000, &probe);
    {
        var results = try idx.search(&probe, 3);
        defer results.deinit();
        const hits = results.getHits();
        try std.testing.expect(hits.len > 0);
        try std.testing.expectEqual(@as(u64, 1000), hits[0].vector_id);
    }
}

test "hbc duplicate child links are dropped by unlink and repair" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .leaf_size = 4,
        .branching_factor = 4,
    });
    defer idx.close();

    var prng = std.Random.DefaultPrng.init(0xd00b_1e5);
    const random = prng.random();
    var id: u64 = 1;
    while (id <= 40) : (id += 1) {
        var v: [4]f32 = undefined;
        for (&v) |*x| x.* = random.float(f32) * 2.0 - 1.0;
        try idx.insert(id, &v);
    }

    const victim_leaf = (try idx.debugLeafForVector(3)) orelse return error.TestUnexpectedResult;
    const victim_members = try idx.debugLeafMembers(alloc, victim_leaf);
    defer alloc.free(victim_members);
    try std.testing.expect(victim_members.len > 0);

    const corrupt = struct {
        fn duplicateChildLink(index: *HBCIndex, leaf_id: u64) !void {
            var txn = try index.beginWriteTxn();
            errdefer txn.abort();
            var leaf = try index.loadNode(&txn, leaf_id);
            defer leaf.deinit(index.alloc);
            const parent_id = leaf.parent;
            try std.testing.expect(parent_id != 0);
            var parent = try index.loadNode(&txn, parent_id);
            defer parent.deinit(index.alloc);
            try parent.ensureUnbacked(index.alloc);
            const dup = try index.alloc.alloc(u64, parent.children.len + 1);
            @memcpy(dup[0..parent.children.len], parent.children);
            dup[parent.children.len] = leaf_id;
            index.alloc.free(parent.children);
            parent.children = dup;
            try index.saveNode(&txn, &parent);
            try txn.commit();
        }
    }.duplicateChildLink;

    // Repair path: the sweep must drop the duplicate occurrence.
    try corrupt(&idx, victim_leaf);
    {
        const broken = try idx.verifyTreeLinks();
        try std.testing.expect(!broken.consistent());
    }
    const repair = try idx.repairTreeLinks(10_000);
    try std.testing.expect(repair.completed);
    try std.testing.expect(repair.duplicate_children_removed >= 1);
    {
        const healed = try idx.verifyTreeLinks();
        try std.testing.expect(healed.consistent());
    }

    // Unlink path: emptying the leaf drives removeChildLink against the
    // duplicated reference, which must drop BOTH occurrences — an
    // underfilled rebuild here used to persist an uninitialized child id.
    try corrupt(&idx, victim_leaf);
    try idx.batchDelete(victim_members);
    {
        const after_delete = try idx.verifyTreeLinks();
        if (!after_delete.consistent()) {
            std.debug.print("tree links inconsistent after duplicate unlink: {any}\n", .{after_delete});
            return error.TestUnexpectedResult;
        }
    }
}

test "default random ortho transform matches go hbc" {
    const alloc = std.testing.allocator;
    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };

    {
        var tp: TestPath = .{};
        const path = tp.init();
        defer tp.cleanup();

        var idx = try HBCIndex.open(alloc, path, .{ .dims = 4 });
        defer idx.close();

        var transformed: [4]f32 = undefined;
        _ = idx.transformVector(&input, &transformed);
        try std.testing.expectEqual(vec.RotAlgorithm.none, idx.rot.algo);
        try std.testing.expectEqualSlices(f32, &input, &transformed);
    }

    {
        var tp: TestPath = .{};
        const path = tp.init();
        defer tp.cleanup();

        var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .use_random_ortho_trans = true });
        defer idx.close();

        var transformed: [4]f32 = undefined;
        _ = idx.transformVector(&input, &transformed);
        try std.testing.expectEqual(vec.RotAlgorithm.givens, idx.rot.algo);
        try std.testing.expect(!std.mem.eql(f32, &input, &transformed));
    }
}

test "hbc shared cache namespaces entries" {
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();

    const ns_a = hbcCacheNamespace("/tmp/hbc-a");
    const ns_b = hbcCacheNamespace("/tmp/hbc-b");
    const vec_a = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const vec_b = [_]f32{ 5.0, 6.0, 7.0, 8.0 };

    _ = try cache.cacheVector(ns_a, 7, &vec_a);
    _ = try cache.cacheVector(ns_b, 7, &vec_b);

    try std.testing.expectEqualSlices(f32, &vec_a, cache.getVector(ns_a, 7).?);
    try std.testing.expectEqualSlices(f32, &vec_b, cache.getVector(ns_b, 7).?);

    cache.invalidateNamespace(ns_a);
    try std.testing.expectEqual(@as(?[]const f32, null), cache.getVector(ns_a, 7));
    try std.testing.expectEqualSlices(f32, &vec_b, cache.getVector(ns_b, 7).?);
}

test "hbc shared cache evicts across namespaces under one resource budget" {
    const vector_bytes = estimateVectorCacheBytes(&.{ 1.0, 2.0, 3.0, 4.0 });
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)] = .{
        .soft_limit_bytes = vector_bytes,
        .hard_limit_bytes = vector_bytes,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();
    cache.attachResourceManager(&resource_manager);

    const ns_a = hbcCacheNamespace("/tmp/hbc-a");
    const ns_b = hbcCacheNamespace("/tmp/hbc-b");
    _ = try cache.cacheVector(ns_a, 1, &.{ 1.0, 2.0, 3.0, 4.0 });
    _ = try cache.cacheVector(ns_b, 1, &.{ 5.0, 6.0, 7.0, 8.0 });

    try std.testing.expectEqual(@as(?[]const f32, null), cache.getVector(ns_a, 1));
    try std.testing.expect(cache.getVector(ns_b, 1) != null);
    try std.testing.expectEqual(@as(u64, 0), cache.namespaceStats(ns_a).vector.used_bytes);
    try std.testing.expectEqual(vector_bytes, cache.namespaceStats(ns_b).vector.used_bytes);
    try std.testing.expectEqual(vector_bytes, resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes);
}

test "hbc stable cache namespace canonicalizes equivalent path spellings" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_rel = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hbc-cache-namespace", .{tmp.sub_path});
    defer alloc.free(root_rel);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const cwd = try std.process.currentPathAlloc(io_impl.io(), alloc);
    defer alloc.free(cwd);
    const root = try std.fs.path.resolve(alloc, &.{ cwd, root_rel });
    defer alloc.free(root);
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), root);

    const absolute = try std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), root, alloc);
    defer alloc.free(absolute);
    const alt = try std.fmt.allocPrint(alloc, "{s}/../{s}", .{ absolute, "hbc-cache-namespace" });
    defer alloc.free(alt);

    try std.testing.expectEqual(hbcCacheNamespaceStable(alloc, absolute), hbcCacheNamespaceStable(alloc, alt));
}

test "hbc index cache disable clears shared namespace and stops accounting growth" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = Cache.init(alloc);
    defer cache.deinit();
    cache.attachResourceManager(&resource_manager);

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.attachSharedCache(&cache);
    idx.setRetainedVectorCacheEnabled(true);

    _ = try idx.cacheVector(1, &.{ 1.0, 2.0, 3.0, 4.0 });
    try std.testing.expect(cache.namespaceStats(idx.cache_namespace).total_bytes > 0);
    try std.testing.expect(resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes > 0);

    idx.setCacheEnabled(false);
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(1));
    try std.testing.expectEqual(@as(u64, 0), cache.namespaceStats(idx.cache_namespace).total_bytes);
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes);

    const bypass = try idx.cacheVector(2, &.{ 5.0, 6.0, 7.0, 8.0 });
    try std.testing.expectEqualSlices(f32, &.{ 5.0, 6.0, 7.0, 8.0 }, bypass);
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(2));
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes);
}

test "hbc retained vector cache is bypassed during external vector replay" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.setRetainedVectorCacheEnabled(true);

    idx.setBypassExternalVectorCache(true);
    defer idx.setBypassExternalVectorCache(false);

    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const returned = try idx.cacheVector(1, &input);
    try std.testing.expectEqual(@intFromPtr(input[0..].ptr), @intFromPtr(returned.ptr));
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(1));
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes);
}

test "hbc retained vector cache can be disabled independently of metadata cache" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = Cache.init(alloc);
    defer cache.deinit();
    cache.attachResourceManager(&resource_manager);

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.attachSharedCache(&cache);

    idx.setRetainedVectorCacheEnabled(false);

    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const returned = try idx.cacheVector(1, &input);
    try std.testing.expectEqual(@intFromPtr(input[0..].ptr), @intFromPtr(returned.ptr));
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(1));
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);

    _ = try idx.cacheMetadata(1, "doc:1");
    try std.testing.expectEqualStrings("doc:1", idx.getCachedMetadata(1).?);
    try std.testing.expect(idx.hbcCacheStats().metadata.used_bytes > 0);
    try std.testing.expect(resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes > 0);
}

test "hbc metadata cache remains active when vector cache capacity is zero" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = Cache.init(alloc);
    defer cache.deinit();
    cache.attachResourceManager(&resource_manager);

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .max_cached_vectors = 0,
        .max_cached_metadata = 8,
    });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.attachSharedCache(&cache);
    idx.setRetainedVectorCacheEnabled(false);

    const vector = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const returned_vector = try idx.cacheVector(1, &vector);
    try std.testing.expectEqual(@intFromPtr(vector[0..].ptr), @intFromPtr(returned_vector.ptr));
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(1));
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);

    _ = try idx.cacheMetadata(1, "doc:1");
    try std.testing.expectEqualStrings("doc:1", idx.getCachedMetadata(1).?);
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);
    try std.testing.expect(idx.hbcCacheStats().metadata.used_bytes > 0);
    try std.testing.expect(resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes > 0);
}

test "hbc metadata cache is retained and resource managed during concurrent search" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = Cache.init(alloc);
    defer cache.deinit();
    cache.attachResourceManager(&resource_manager);

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.attachSharedCache(&cache);
    idx.setRetainedVectorCacheEnabled(false);

    idx.active_searches.store(2, .release);
    defer idx.active_searches.store(0, .release);

    const input = "doc:concurrent";
    const returned = try idx.cacheMetadata(11, input);
    try std.testing.expect(@intFromPtr(input.ptr) != @intFromPtr(returned.ptr));
    try std.testing.expectEqualStrings(input, idx.getCachedMetadata(11).?);
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);
    try std.testing.expect(idx.hbcCacheStats().metadata.used_bytes > 0);
    try std.testing.expect(resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes > 0);
}

test "hbc retained vector cache defaults on for search performance" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    defer idx.close();

    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const returned = try idx.cacheVector(1, &input);
    try std.testing.expect(@intFromPtr(input[0..].ptr) != @intFromPtr(returned.ptr));
    try std.testing.expectEqualSlices(f32, &input, idx.getCachedVector(1).?);
    try std.testing.expect(idx.hbcCacheStats().vector.used_bytes > 0);

    _ = try idx.cacheMetadata(1, "doc:1");
    try std.testing.expectEqualStrings("doc:1", idx.getCachedMetadata(1).?);
    try std.testing.expect(idx.hbcCacheStats().metadata.used_bytes > 0);
}

test "hbc index close does not clear shared namespace bytes" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var cache = Cache.init(alloc);
    defer cache.deinit();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    defer idx.close();
    idx.attachSharedCache(&cache);
    idx.setRetainedVectorCacheEnabled(true);

    _ = try idx.cacheVector(1, &.{ 1.0, 2.0, 3.0, 4.0 });
    const namespace = idx.cache_namespace;
    try std.testing.expect(cache.namespaceStats(namespace).total_bytes > 0);

    var second = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    second.attachSharedCache(&cache);
    second.close();

    try std.testing.expect(cache.namespaceStats(namespace).total_bytes > 0);
    try std.testing.expect(cache.namespaceStats(namespace).vector.used_bytes > 0);
}

test "hbc shared cache releases unused namespace path registrations" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var cache = Cache.init(alloc);
    defer cache.deinit();

    var first = try HBCIndex.open(alloc, path, .{ .dims = 4 });
    var first_open = true;
    defer if (first_open) first.close();
    first.attachSharedCache(&cache);
    const namespace = first.cache_namespace;
    try std.testing.expectEqual(@as(usize, 1), cache.namespace_paths.get(namespace).?.active_owners);

    var second = try HBCIndex.open(alloc, path, .{ .dims = 4 });
    var second_open = true;
    defer if (second_open) second.close();
    second.attachSharedCache(&cache);
    try std.testing.expectEqual(@as(usize, 2), cache.namespace_paths.get(namespace).?.active_owners);

    first.close();
    first_open = false;
    try std.testing.expectEqual(@as(usize, 1), cache.namespace_paths.get(namespace).?.active_owners);

    second.close();
    second_open = false;
    try std.testing.expect(!cache.namespace_paths.contains(namespace));
    try std.testing.expect(!cache.namespace_stats.contains(namespace));
}

test "hbc shared cache bounds namespace state across path churn" {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc);
    defer cache.deinit();

    var path_buf: [128]u8 = undefined;
    for (0..512) |i| {
        const path = try std.fmt.bufPrint(&path_buf, "/tmp/antfly-hbc-namespace-churn/{d}", .{i});
        const namespace = hbcCacheNamespace(path);
        try std.testing.expect(cache.registerNamespacePath(namespace, path));
        cache.unregisterNamespacePath(namespace, path);
    }

    try std.testing.expectEqual(@as(usize, 0), cache.namespace_paths.count());
    try std.testing.expectEqual(@as(usize, 0), cache.namespace_stats.count());
}

test "hbc cache reports byte usage to resource manager" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 256,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.setRetainedVectorCacheEnabled(true);

    _ = try idx.cacheVector(1, &.{ 1.0, 2.0, 3.0, 4.0 });
    _ = try idx.cacheMetadata(1, "doc:1");
    var stats = resource_manager.snapshot();
    try std.testing.expect(stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)].used_bytes > 0);
    try std.testing.expect(stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)].soft_limit_events > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)].hard_limit_rejections);

    idx.invalidateVectorCache(1);
    idx.invalidateMetadataCache(1);
    stats = resource_manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)].used_bytes);
}

test "hbc opportunistic vector cache skips instead of overcommitting resource budget" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 2,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.setRetainedVectorCacheEnabled(true);

    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const returned = try idx.cacheVector(1, &input);
    try std.testing.expectEqual(@intFromPtr(input[0..].ptr), @intFromPtr(returned.ptr));

    const stats = resource_manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)].used_bytes);
    try std.testing.expect(stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)].hard_limit_rejections > 0);
    try std.testing.expectEqual(@as(u64, 1), idx.hbcCacheStats().vector.admission_skips);
}

test "hbc routing scratch reports bytes to resource manager" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    idx.attachResourceManager(&resource_manager);

    try idx.batchInsertWithMetadata(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 1.0, 1.0 }, .metadata = "doc:2" },
    });

    var txn = try idx.beginReadTxn();
    _ = try idx.findLeafWithOptions(&txn, idx.metadata.root_node, &.{ 0.5, 0.5 }, true);
    txn.abort();

    try std.testing.expect(resource_manager.sliceStats(.dense_routing_working_set).used_bytes > 0);

    idx.close();
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.dense_routing_working_set).used_bytes);
}

test "hbc search scratch reports bytes to resource manager" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    idx.attachResourceManager(&resource_manager);

    try idx.batchInsertWithMetadata(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 1.0, 1.0 }, .metadata = "doc:2" },
    });

    var results = try idx.searchWithRequest(.{
        .query = &[_]f32{ 0.1, 0.1 },
        .k = 1,
    });
    defer results.deinit();

    try std.testing.expect(idx.search_workspace_bytes_accounted > 0);
    try std.testing.expect(resource_manager.sliceStats(.dense_search_working_set).used_bytes > 0);

    idx.close();
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.dense_search_working_set).used_bytes);
}

test "hbc leaf split matrix reports dense apply workspace bytes" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);

    try idx.batchInsertWithMetadata(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 1.0, 1.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 2.0, 2.0 }, .metadata = "doc:3" },
    });

    const stats = resource_manager.sliceStats(.dense_apply_working_set);
    try std.testing.expect(stats.peak_bytes >= 3 * 2 * @sizeOf(f32));
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
}

test "hbc cache shrinks to resource budget under pressure" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const vector_bytes = estimateVectorCacheBytes(&.{ 1.0, 2.0, 3.0, 4.0 });
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)] = .{
        .soft_limit_bytes = vector_bytes,
        .hard_limit_bytes = vector_bytes,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)] = .{
        .soft_action = .shrink_cache,
        .hard_action = .shrink_cache,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets, .policies = policies });

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .max_cached_vectors = 8 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.setRetainedVectorCacheEnabled(true);

    _ = try idx.cacheVector(1, &.{ 1.0, 2.0, 3.0, 4.0 });
    _ = try idx.cacheVector(2, &.{ 5.0, 6.0, 7.0, 8.0 });
    _ = try idx.cacheVector(3, &.{ 9.0, 10.0, 11.0, 12.0 });

    const stats = resource_manager.sliceStats(.hbc_node_metadata_cache);
    try std.testing.expect(stats.used_bytes <= stats.soft_limit_bytes);
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(1));
    try std.testing.expect(idx.getCachedVector(3) != null);

    const cache_stats = idx.hbcCacheStats();
    try std.testing.expectEqual(stats.used_bytes, cache_stats.total_bytes);
    try std.testing.expectEqual(@as(u64, 3), cache_stats.vector.insertions);
    try std.testing.expect(cache_stats.vector.evictions > 0);
    try std.testing.expect(cache_stats.vector.used_bytes <= stats.soft_limit_bytes);
}

test "reopen rejects dimension mismatch" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{ .dims = 4 });
        defer idx.close();
    }

    try std.testing.expectError(error.DimensionMismatch, HBCIndex.open(alloc, path, .{ .dims = 3 }));
}

test "reopen rejects metric mismatch" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{ .dims = 3, .metric = .l2_squared });
        defer idx.close();
    }

    try std.testing.expectError(error.DistanceMetricMismatch, HBCIndex.open(alloc, path, .{ .dims = 3, .metric = .cosine }));
}

test "insert and search" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 4, .leaf_size = 10 });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
    try idx.insert(2, &[_]f32{ 0.0, 1.0, 0.0, 0.0 });
    try idx.insert(3, &[_]f32{ 0.0, 0.0, 1.0, 0.0 });
    try idx.insert(4, &[_]f32{ 1.0, 1.0, 0.0, 0.0 });
    try idx.insert(5, &[_]f32{ 0.0, 0.0, 0.0, 1.0 });

    try std.testing.expectEqual(@as(u64, 5), idx.stats().active_count);

    // Search near [1, 0, 0, 0] — should return vector 1 as closest
    var results = try idx.search(&[_]f32{ 1.0, 0.1, 0.0, 0.0 }, 3);
    defer results.deinit();

    const hits = results.getHits();
    try std.testing.expect(hits.len > 0);
    // First result should be vector 1 (distance ~0.01)
    try std.testing.expectEqual(@as(u64, 1), hits[0].vector_id);
}

test "experimental posting checkpoint reopens safely and publishes immutable generations" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer idx.close();
        try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
        try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");
        const checkpoint = try idx.publishExperimentalPostingCheckpoint(2);
        try std.testing.expectEqual(@as(u64, 2), checkpoint.covered_source_sequence);
        try std.testing.expect(checkpoint.posting_count > 0);
        try std.testing.expect(!checkpoint.flattened_generation);
        const wal = try idx.appendExperimentalPostingWalSnapshotBatch(1, 3, &.{1});
        try std.testing.expect(wal.record_count > 0);
        try std.testing.expectEqual(@as(u64, 3), wal.covered_source_sequence);
    }

    {
        var reopened = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer reopened.close();
        try std.testing.expectError(error.PostingCheckpointSequenceMismatch, reopened.activateExperimentalPostingReads(4));
        try reopened.activateExperimentalPostingReads(3);
        try std.testing.expect(reopened.experimentalPostingReadsEnabled());
        try std.testing.expect(reopened.experimental_posting_read_generation.?.root.?.retained_segment.isMapped());

        var results = try reopened.search(&[_]f32{ 1.0, 0.0 }, 1);
        defer results.deinit();
        try std.testing.expectEqual(@as(u64, 1), results.items.items[0].vector_id);

        var old_txn = try reopened.beginReadTxn();
        defer old_txn.abort();
        try std.testing.expect(old_txn.read_lease != null);
        try std.testing.expectEqualStrings("doc:1", (try reopened.getMetadataInTxn(&old_txn, 1)).?);
        _ = try reopened.getVecLeaf(&old_txn, 1);
        var root_key_buf: [12]u8 = undefined;
        const root_key = encodeNodeKey(&root_key_buf, reopened.metadata.root_node, .packed_node);
        const old_root = try alloc.dupe(u8, try reopened.getNamespaced(&old_txn, .nodes, root_key));
        defer alloc.free(old_root);

        try reopened.beginExperimentalPostingMutationCapture();
        errdefer reopened.cancelExperimentalPostingMutationCapture();
        try reopened.insertWithMetadata(3, &[_]f32{ 0.5, 0.5 }, "doc:3");
        // Until commit, newly opened reads keep using the previous complete
        // segment generation rather than falling through to the LSM.
        try std.testing.expect(reopened.experimentalPostingReadsEnabled());
        var during_txn = try reopened.beginReadTxn();
        defer during_txn.abort();
        try std.testing.expect(during_txn.read_lease != null);
        try std.testing.expectEqualSlices(u8, old_root, try reopened.getNamespaced(&during_txn, .nodes, root_key));
        const captured = try reopened.finishExperimentalPostingMutationCapture(2, 4, .{});
        try std.testing.expect(captured.record_count > 0);
        try std.testing.expect(reopened.experimentalPostingReadsEnabled());

        // A transaction opened before publication remains pinned to the old
        // immutable generation; a new transaction sees the complete overlay.
        try std.testing.expectEqualSlices(u8, old_root, try reopened.getNamespaced(&old_txn, .nodes, root_key));
        try std.testing.expectEqual(@as(?[]const u8, null), try reopened.getMetadataInTxn(&old_txn, 3));
        var new_txn = try reopened.beginReadTxn();
        defer new_txn.abort();
        try std.testing.expect(new_txn.read_lease != null);
        try std.testing.expect(!std.mem.eql(u8, old_root, try reopened.getNamespaced(&new_txn, .nodes, root_key)));
        try std.testing.expectEqualStrings("doc:3", (try reopened.getMetadataInTxn(&new_txn, 3)).?);
        _ = try reopened.getVecLeaf(&new_txn, 3);

        // A subsequent checkpoint flattens the complete leased generation,
        // including a vector id that was absent from the root directory.
        const flattened = try reopened.publishExperimentalPostingCheckpoint(4);
        try std.testing.expectEqual(@as(u64, 2), flattened.generation);
        try std.testing.expect(flattened.flattened_generation);
        var flattened_txn = try reopened.beginReadTxn();
        defer flattened_txn.abort();
        try std.testing.expectEqualStrings("doc:3", (try reopened.getMetadataInTxn(&flattened_txn, 3)).?);
        _ = try reopened.getVecLeaf(&flattened_txn, 3);

        var after_write = try reopened.search(&[_]f32{ 0.5, 0.5 }, 3);
        defer after_write.deinit();
        try std.testing.expectEqual(@as(usize, 3), after_write.items.items.len);
    }

    var final = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer final.close();
    try final.activateExperimentalPostingReads(4);
    var final_results = try final.search(&[_]f32{ 0.5, 0.5 }, 3);
    defer final_results.deinit();
    try std.testing.expectEqual(@as(usize, 3), final_results.items.items.len);
}

test "background posting checkpoint preserves concurrent same-sequence WAL tail" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer idx.close();

        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(1, .{});
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{});
        const aggregate_delta = idx.experimental_posting_read_generation.?;
        try std.testing.expect(aggregate_delta.root == null);
        try std.testing.expect(aggregate_delta.parent.?.root != null);
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(3, &[_]f32{ 0.8, 0.2 }, "doc:3");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(3, .{});

        // With no leases, publication extends the same aggregate delta map;
        // it does not add one immutable-map probe per committed source batch.
        try std.testing.expectEqual(aggregate_delta, idx.experimental_posting_read_generation.?);
        try std.testing.expectEqualStrings("doc:3", (try aggregate_delta.value(3, .vector_metadata)).?);
        try std.testing.expect((try idx.collapseExperimentalPostingReadOverlays()) > 0);
        var collapsed_generation = idx.experimental_posting_read_generation.?;
        try std.testing.expect(collapsed_generation.root == null);
        try std.testing.expect(collapsed_generation.parent.?.root != null);

        var pinned_before_collapse = try idx.beginReadTxn();
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(4, &[_]f32{ 0.2, 0.8 }, "doc:4");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(4, .{});
        // The pinned transaction forces copy-on-collapse and retains its old
        // complete view while new readers use the collapsed generation.
        try std.testing.expect((try idx.collapseExperimentalPostingReadOverlays()) > 0);
        try std.testing.expectEqual(@as(?[]const u8, null), try idx.getMetadataInTxn(&pinned_before_collapse, 4));
        pinned_before_collapse.abort();
        collapsed_generation = idx.experimental_posting_read_generation.?;
        try std.testing.expect(collapsed_generation.root == null);
        try std.testing.expect(collapsed_generation.parent.?.root != null);

        var posting_store = &idx.experimental_posting_write_store.?;
        try std.testing.expect(try idx.startExperimentalPostingCheckpointBuild(posting_store));
        const flattened_wal_bytes = posting_store.wal_committed_bytes;

        // A delayed callback may commit another ordered batch at sequence 4
        // while the segment for the earlier sequence-4 prefix is building.
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(5, &[_]f32{ 0.5, 0.5 }, "doc:5");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(4, .{});
        posting_store = &idx.experimental_posting_write_store.?;

        while (idx.experimental_posting_checkpoint_build) |build| {
            if (build.completed.load(.acquire)) {
                _ = try idx.publishCompletedExperimentalPostingCheckpointBuild(posting_store);
                break;
            }
            std.Thread.yield() catch {};
        }
        try std.testing.expectEqual(@as(u64, 2), posting_store.checkpoint.?.segment_generation);
        try std.testing.expectEqual(@as(u64, 4), posting_store.checkpoint.?.covered_source_sequence);
        try std.testing.expect(posting_store.wal_committed_bytes > 0);
        try std.testing.expect(flattened_wal_bytes > 0);

        var results = try idx.search(&[_]f32{ 0.5, 0.5 }, 5);
        defer results.deinit();
        try std.testing.expectEqual(@as(usize, 5), results.items.items.len);
    }

    var reopened = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer reopened.close();
    try reopened.activateExperimentalPostingReads(4);
    var results = try reopened.search(&[_]f32{ 0.5, 0.5 }, 5);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 5), results.items.items.len);
    try std.testing.expectEqualStrings("doc:5", results.items.items[0].metadata.?);
}

test "posting WAL mutations provide read your writes without derived LSM persistence" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer idx.close();

        // Bootstrap remains authoritative in the LSM until one complete
        // immutable generation exists.
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(1, .{});
        idx.enableExperimentalPostingWalMutations();

        var pinned_before_mutation = try idx.beginReadTxn();
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");

        // Queries remain on the committed generation until the derived batch
        // commits. The transaction window itself has already routed and read
        // through its pinned base plus captured writes.
        try std.testing.expectEqual(@as(?[]const u8, null), try idx.getMetadataInTxn(&pinned_before_mutation, 2));

        var leaf_key_buf: [10]u8 = undefined;
        var lsm_read = try idx.store.beginRead();
        defer lsm_read.abort();
        try std.testing.expectError(error.NotFound, lsm_read.get(.vecs, encodeVecLeafKey(&leaf_key_buf, 2)));

        try idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{});
        try std.testing.expectEqual(@as(?[]const u8, null), try idx.getMetadataInTxn(&pinned_before_mutation, 2));
        pinned_before_mutation.abort();
        var after_publish = try idx.search(&[_]f32{ 0.0, 1.0 }, 2);
        defer after_publish.deinit();
        try std.testing.expectEqual(@as(usize, 2), after_publish.items.items.len);
        try std.testing.expectEqualStrings("doc:2", after_publish.items.items[0].metadata.?);
    }

    {
        var reopened = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer reopened.close();
        try std.testing.expect(reopened.experimentalPostingWalAuthoritative());
        try reopened.activateExperimentalPostingReads(2);

        // Durable authority itself makes WAL routing sticky; a post-restart
        // mutation must not depend on the original rollout flag or another
        // enable call. The corresponding derived LSM rows do not exist.
        try reopened.beginExperimentalPostingMutationCapture();
        try reopened.insertWithMetadata(3, &[_]f32{ 0.5, 0.5 }, "doc:3");
        try reopened.persistExperimentalPostingSidecarAtAppliedSequence(3, .{});
        var results = try reopened.search(&[_]f32{ 0.5, 0.5 }, 3);
        defer results.deinit();
        try std.testing.expectEqual(@as(usize, 3), results.items.items.len);
        try std.testing.expectEqualStrings("doc:3", results.items.items[0].metadata.?);

        // Model the stale mutable metadata that remains in the LSM after
        // query-facing state moves to the posting store. Restart must source
        // mutable topology/count metadata from the same WAL generation as the
        // postings, not combine two different recovery epochs.
        var stale_metadata = reopened.metadata;
        stale_metadata.root_node = 1;
        stale_metadata.active_count = 0;
        stale_metadata.node_count = 1;
        var metadata_buf: [IndexMetadata.encoded_size]u8 = undefined;
        var stale_txn = try reopened.store.beginWrite();
        errdefer stale_txn.abort();
        try stale_txn.put(.meta, meta_key, stale_metadata.encode(&metadata_buf));
        try stale_txn.commit();
    }

    var final = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer final.close();
    try std.testing.expectEqual(@as(u64, 0), final.stats().active_count);
    try std.testing.expectError(error.PostingCheckpointSequenceMismatch, final.activateExperimentalPostingReads(2));
    try std.testing.expectEqual(@as(u64, 3), try final.activateExperimentalPostingReadsAtOrAfter(2));
    try std.testing.expectEqual(@as(u64, 3), final.stats().active_count);
    var results = try final.search(&[_]f32{ 0.5, 0.5 }, 3);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 3), results.items.items.len);
    try std.testing.expectEqualStrings("doc:3", results.items.items[0].metadata.?);
}

test "posting WAL mutation capture aborts without publishing partial state" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer idx.close();

    try idx.beginExperimentalPostingMutationCapture();
    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
    try idx.persistExperimentalPostingSidecarAtAppliedSequence(1, .{});
    idx.enableExperimentalPostingWalMutations();

    var store_before = try idx.openExperimentalPostingStore();
    const committed_bytes_before = store_before.wal_committed_bytes;
    const last_batch_before = store_before.last_committed_batch;
    store_before.deinit();

    try idx.beginExperimentalPostingMutationCapture();
    var txn = try idx.beginWriteTxn();
    var txn_active = true;
    defer if (txn_active) txn.abort();
    var leaf_key_buf: [10]u8 = undefined;
    const leaf_key = encodeVecLeafKey(&leaf_key_buf, 99);
    try idx.putNamespaced(&txn, .vecs, leaf_key, &.{ 1, 2, 3, 4 });
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, try idx.getNamespaced(&txn, .vecs, leaf_key));
    txn.abort();
    txn_active = false;
    idx.cancelExperimentalPostingMutationCapture();

    var committed_read = try idx.beginReadTxn();
    defer committed_read.abort();
    try std.testing.expectError(error.NotFound, idx.getNamespaced(&committed_read, .vecs, leaf_key));
    var store_after = try idx.openExperimentalPostingStore();
    defer store_after.deinit();
    try std.testing.expectEqual(committed_bytes_before, store_after.wal_committed_bytes);
    try std.testing.expectEqual(last_batch_before, store_after.last_committed_batch);

    // A source window can also fail after its local HBC transaction commits
    // but before posting-WAL publication. WAL-only state and mutable metadata
    // must roll back to the pinned immutable base, not linger in caches.
    try idx.beginExperimentalPostingMutationCapture();
    try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");
    try std.testing.expectEqual(@as(u64, 2), idx.stats().active_count);
    idx.cancelExperimentalPostingMutationCapture();
    try std.testing.expectEqual(@as(u64, 1), idx.stats().active_count);
    var after_cancel = try idx.search(&[_]f32{ 0.0, 1.0 }, 2);
    defer after_cancel.deinit();
    try std.testing.expectEqual(@as(usize, 1), after_cancel.items.items.len);
    try std.testing.expectEqualStrings("doc:1", after_cancel.items.items[0].metadata.?);

    // Once the marker is authoritative, query-facing mutations outside a
    // capture boundary are rejected before either store is changed.
    idx.experimental_posting_wal_authoritative.store(true, .release);
    var outside_txn = try idx.beginWriteTxn();
    defer outside_txn.abort();
    try std.testing.expectError(
        error.PostingWalMutationOutsideCapture,
        idx.putNamespaced(&outside_txn, .vecs, leaf_key, &.{9}),
    );

    // If an authoritative live generation becomes unavailable, neither
    // queries nor the next mutation may silently consult stale LSM rows.
    idx.disableExperimentalPostingReads();
    try std.testing.expectError(error.PostingWalMutationStoreUnavailable, idx.beginRuntimeReadTxn());
    try std.testing.expectError(error.PostingWalMutationStoreUnavailable, idx.beginRuntimeSearchTxn());
    try std.testing.expectError(error.PostingWalMutationStoreUnavailable, idx.beginExperimentalPostingMutationCapture());
}

test "authoritative external-vector HBC detaches legacy LSM and reopens native first" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const Loader = struct {
        fn load(_: *anyopaque, loader_alloc: Allocator, vector_id: u64, _: []const u8) ![]f32 {
            const vector: []const f32 = switch (vector_id) {
                1 => &.{ 1.0, 0.0 },
                2 => &.{ 0.0, 1.0 },
                3 => &.{ 0.5, 0.5 },
                else => return error.NotFound,
            };
            return try loader_alloc.dupe(f32, vector);
        }
    };
    var loader_context: u8 = 0;

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer idx.close();
        idx.setExternalVectorLoader(&loader_context, Loader.load);

        try idx.beginExperimentalPostingMutationCapture();
        try idx.batchInsertWithMetadataOptions(&.{.{
            .vector_id = 1,
            .vector = &.{ 1.0, 0.0 },
            .metadata = "doc:1",
        }}, .{ .skip_vector_store = true });
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(1, .{});
        idx.enableExperimentalPostingWalMutations();

        try idx.beginExperimentalPostingMutationCapture();
        try idx.batchInsertWithMetadataOptions(&.{.{
            .vector_id = 2,
            .vector = &.{ 0.0, 1.0 },
            .metadata = "doc:2",
        }}, .{ .skip_vector_store = true });
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{});

        try std.testing.expect(idx.env_owner == .native);
        try std.testing.expect(idx.snapshotLsmWriteStats() == null);
        {
            var read = try idx.beginRuntimeReadTxn();
            defer read.abort();
            try std.testing.expectError(error.HbcNativeUnownedKey, read.get(.meta, "not-an-hbc-key"));
            try std.testing.expectEqualStrings("doc:2", (try idx.getMetadataInTxn(&read, 2)).?);
            try std.testing.expect((try idx.getMetadataInTxn(&read, 99)) == null);
            var vector_scratch: [2]f32 = undefined;
            try std.testing.expectEqualSlices(f32, &.{ 1.0, 0.0 }, try idx.getVectorScratch(&read, 1, &vector_scratch));
        }

        try idx.beginExperimentalPostingMutationCapture();
        try idx.batchInsertWithMetadataOptions(&.{.{
            .vector_id = 3,
            .vector = &.{ 0.5, 0.5 },
            .metadata = "doc:3",
        }}, .{ .skip_vector_store = true });
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(3, .{});
    }

    var reopened = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer reopened.close();
    try std.testing.expect(reopened.env_owner == .native);
    reopened.setExternalVectorLoader(&loader_context, Loader.load);
    try std.testing.expectEqual(@as(u64, 3), try reopened.activateExperimentalPostingReadsAtOrAfter(3));
    var results = try reopened.search(&.{ 0.0, 1.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 2), results.items.items.len);
    try std.testing.expectEqualStrings("doc:2", results.items.items[0].metadata.?);
}

test "posting WAL capture can begin inside a streaming replay session" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer idx.close();

        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(1, .{});
        idx.enableExperimentalPostingWalMutations();

        // A long-lived replay session can already be open when the next
        // independently durable source window begins. This ordering used to
        // silently skip capture and later fail with
        // PostingWalMutationOutsideCapture.
        try idx.beginStreamingReplaySession();
        var session_open = true;
        defer if (session_open) idx.abortStreamingReplaySession();
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");

        // WAL publication remains fenced until the backend session itself has
        // established its durability boundary.
        try std.testing.expectError(
            error.ActiveWriteSession,
            idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{}),
        );
        try idx.finishStreamingReplaySessionWithOptions(.{ .compact = false });
        session_open = false;
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{});

        var leaf_key_buf: [10]u8 = undefined;
        var lsm_read = try idx.store.beginRead();
        defer lsm_read.abort();
        try std.testing.expectError(error.NotFound, lsm_read.get(.vecs, encodeVecLeafKey(&leaf_key_buf, 2)));
    }

    var reopened = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer reopened.close();
    try reopened.activateExperimentalPostingReads(2);
    var results = try reopened.search(&[_]f32{ 0.0, 1.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 2), results.items.items.len);
    try std.testing.expectEqualStrings("doc:2", results.items.items[0].metadata.?);
}

test "managed posting sidecar follows applied sequence and uncovered writes invalidate it" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer idx.close();

        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(1, .{});

        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{});
        // A source-only journal advance must still make the exact startup
        // sequence fence usable without fabricating a posting mutation.
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(3, .{});
        try std.testing.expectEqual(
            @as(?u64, 3),
            idx.experimentalPostingDurableAppliedSequence(),
        );
        // A delayed persistence callback must not regress source coverage or
        // invalidate a newer sidecar. Its captured changes become a derived
        // batch at the already-published source epoch.
        try idx.beginExperimentalPostingMutationCapture();
        try idx.insertWithMetadata(3, &[_]f32{ 0.5, 0.5 }, "doc:3");
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{});
        try std.testing.expectEqual(
            @as(?u64, 3),
            idx.experimentalPostingDurableAppliedSequence(),
        );

        var posting_store = try idx.openExperimentalPostingStore();
        defer posting_store.deinit();
        try std.testing.expectEqual(@as(u64, 3), posting_store.covered_source_sequence);
        try std.testing.expectEqual(@as(?u64, 3), posting_store.last_committed_batch);
        var recovered = try posting_store.recoverWal();
        defer recovered.deinit();
        var derived_at_current_epoch: usize = 0;
        var coverage_count: usize = 0;
        for (recovered.replay.records.items) |record| switch (record.kind) {
            .base,
            .quantized_checkpoint,
            .posting_state,
            .node_range,
            .vector_leaf,
            .vector_metadata,
            .index_metadata,
            .base_tombstone,
            .quantized_checkpoint_tombstone,
            .posting_state_tombstone,
            .node_range_tombstone,
            .vector_leaf_tombstone,
            .vector_metadata_tombstone,
            .index_metadata_tombstone,
            .tombstone,
            .mutation,
            => if (record.source_sequence == 3) {
                derived_at_current_epoch += 1;
            },
            .coverage => coverage_count += 1,
            .centroid_directory, .commit => {},
        };
        try std.testing.expect(derived_at_current_epoch > 0);
        try std.testing.expectEqual(@as(usize, 1), coverage_count);
    }

    {
        var reopened = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer reopened.close();
        try reopened.activateExperimentalPostingReads(3);
        var results = try reopened.search(&[_]f32{ 0.0, 1.0 }, 3);
        defer results.deinit();
        try std.testing.expectEqual(@as(usize, 3), results.items.items.len);

        // Projection progress is metadata, not a posting mutation. It must
        // neither disable reads nor remove CURRENT.
        var projection = reopened.projectionCheckpointMetadata();
        projection.applied_sequence = 3;
        projection.generation += 1;
        try reopened.saveProjectionCheckpointMetadata(projection);
        try std.testing.expect(reopened.experimentalPostingReadsEnabled());
        var still_published = try reopened.openExperimentalPostingStore();
        defer still_published.deinit();
        try std.testing.expect(still_published.checkpoint != null);
        try std.testing.expectEqual(@as(u64, 3), still_published.covered_source_sequence);

        // This mutation deliberately has no source-sequence capture. CURRENT
        // is removed before its first posting write can be staged, so a crash
        // or restart cannot accidentally serve the old sequence-3 sidecar.
        try reopened.insertWithMetadata(4, &[_]f32{ 0.25, 0.75 }, "doc:4");
    }

    var final = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer final.close();
    try std.testing.expectError(error.MissingPostingCheckpoint, final.activateExperimentalPostingReads(3));
}

test "managed posting sidecar records maintenance without advancing source coverage" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = false,
            .lazy_posting_maintenance = true,
            .storage_backend = .lsm,
        });
        defer idx.close();

        try idx.beginExperimentalPostingMutationCapture();
        try idx.insert(1, &[_]f32{ 1.0, 0.0 });
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(1, .{});
        try std.testing.expect(idx.experimentalPostingReadsEnabled());

        try idx.beginExperimentalPostingMutationCapture();
        try idx.insert(2, &[_]f32{ 3.0, 0.0 });
        try idx.persistExperimentalPostingSidecarAtAppliedSequence(2, .{});
        try std.testing.expect(idx.experimentalPostingReadsEnabled());

        const before = try idx.postingBacklogStats();
        try std.testing.expect(before.needsRepair());
    }

    {
        var reopened = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .leaf_size = 8,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = false,
            .lazy_posting_maintenance = true,
            .storage_backend = .lsm,
        });
        defer reopened.close();
        try reopened.activateExperimentalPostingReads(2);

        const repaired = try reopened.repairDirtyPostings();
        try std.testing.expectEqual(@as(u64, 1), repaired.centroid_refreshed);
        try std.testing.expect(reopened.experimentalPostingReadsEnabled());

        var posting_store = try reopened.openExperimentalPostingStore();
        defer posting_store.deinit();
        try std.testing.expectEqual(@as(u64, 2), posting_store.covered_source_sequence);
        try std.testing.expectEqual(@as(?u64, 2), posting_store.last_committed_batch);
        var recovered = try posting_store.recoverWal();
        defer recovered.deinit();
        try std.testing.expectEqual(@as(u64, 2), recovered.replay.covered_source_sequence);
    }

    var final = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = false,
        .lazy_posting_maintenance = true,
        .storage_backend = .lsm,
    });
    defer final.close();
    try final.activateExperimentalPostingReads(2);
    const backlog = try final.postingBacklogStats();
    try std.testing.expect(!backlog.needsRepair());
    var results = try final.search(&[_]f32{ 3.0, 0.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 2), results.items.items.len);
}

test "flat rabitq centroid directory searches leaf postings" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 4,
        .use_quantization = true,
        .centroid_directory_mode = .flat_rabitq,
        .flat_centroid_block_size = 2,
        .flat_centroid_probe_count = 2,
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:a" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.2, 0.0 }, .metadata = "doc:b" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:y" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.2, 10.0 }, .metadata = "doc:z" },
    };
    try idx.bulkBuildWithMetadata(&items);

    var profiled = try idx.searchProfiledRequest(.{
        .query = &[_]f32{ 10.0, 10.0 },
        .k = 2,
        .search_width = 4,
        .load_metadata = false,
    });
    defer profiled.results.deinit();

    const hits = profiled.results.getHits();
    try std.testing.expect(hits.len > 0);
    try std.testing.expectEqual(@as(u64, 3), hits[0].vector_id);
    try std.testing.expect(profiled.profile.approx_nodes_expanded > 0);
    try std.testing.expect(profiled.profile.leaves_explored > 0);
}

test "searchProfiled records phase timings and counters" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .leaf_size = 3,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    for (0..16) |i| {
        const base = @as(f32, @floatFromInt(i));
        try idx.insert(@intCast(i + 1), &[_]f32{ base, base + 0.1, base + 0.2, base + 0.3 });
    }

    var profiled = try idx.searchProfiled(&[_]f32{ 0.0, 0.1, 0.2, 0.3 }, 5);
    defer profiled.results.deinit();

    try std.testing.expect(profiled.profile.total_ns > 0);
    try std.testing.expect(profiled.profile.root_load_ns <= profiled.profile.total_ns);
    try std.testing.expect(profiled.profile.nodes_visited > 0);
    try std.testing.expect(profiled.profile.leaf_score_ns > 0);
}

test "reopened lsm hbc loads quantized payloads on cold cache miss" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 4,
            .leaf_size = 3,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer idx.close();

        for (0..24) |i| {
            const group: f32 = @floatFromInt(i / 6);
            const offset: f32 = @floatFromInt(i % 6);
            const vector = [_]f32{ group, offset * 0.1, group + 0.25, offset * 0.2 };
            try idx.insertWithMetadata(@intCast(i + 1), &vector, "doc");
        }
    }

    {
        var reopened = try HBCIndex.open(alloc, path, .{
            .dims = 4,
            .leaf_size = 3,
            .branching_factor = 2,
            .search_width = 8,
            .use_quantization = true,
            .storage_backend = .lsm,
        });
        defer reopened.close();

        var profiled = try reopened.searchProfiled(&[_]f32{ 1.0, 0.2, 1.25, 0.4 }, 5);
        defer profiled.results.deinit();

        try std.testing.expect(profiled.results.getHits().len > 0);
        try std.testing.expect(profiled.profile.quantized_cache_misses > 0);
        try std.testing.expect(profiled.profile.approx_vectors_scored > 0);
    }
}

test "searchWithRequest returns empty when published root is missing" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");

    idx.published_root_node.store(std.math.maxInt(u64), .release);
    idx.published_active_count.store(2, .release);

    var results = try idx.searchWithRequest(.{
        .query = &[_]f32{ 1.0, 0.0 },
        .k = 2,
    });
    defer results.deinit();

    try std.testing.expectEqual(@as(usize, 0), results.getHits().len);
}

test "searchWithRequest tolerates concurrent readers with runtime caches enabled" {
    const alloc = std.heap.c_allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(alloc, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .max_cached_nodes = 2,
        .max_cached_vectors = 8,
    });
    defer idx.close();

    for (0..256) |i| {
        const x = @as(f32, @floatFromInt(i % 16));
        const y = @as(f32, @floatFromInt(i / 16));
        var metadata_buf: [32]u8 = undefined;
        const metadata = try std.fmt.bufPrint(&metadata_buf, "doc:{d}", .{i});
        try idx.insertWithMetadata(@intCast(i + 1), &[_]f32{ x, y }, metadata);
    }

    const Worker = struct {
        idx: *HBCIndex,
        failed: *std.atomic.Value(u8),

        fn run(self: *@This(), worker_index: usize) void {
            var iter: usize = 0;
            while (iter < 1000 and self.failed.load(.monotonic) == 0) : (iter += 1) {
                const query_id = (iter + worker_index * 29) % 256;
                const x = @as(f32, @floatFromInt(query_id % 16));
                const y = @as(f32, @floatFromInt(query_id / 16));
                var results = self.idx.searchWithRequest(.{
                    .query = &[_]f32{ x, y },
                    .k = 4,
                }) catch {
                    self.failed.store(1, .monotonic);
                    return;
                };
                defer results.deinit();
                if (results.items.items.len == 0) {
                    self.failed.store(1, .monotonic);
                    return;
                }
                if (results.items.items[0].metadata == null) {
                    self.failed.store(1, .monotonic);
                    return;
                }
            }
        }
    };

    var failed = std.atomic.Value(u8).init(0);
    var workers = [_]Worker{.{ .idx = &idx, .failed = &failed }} ** 8;
    var threads: [workers.len]std.Thread = undefined;
    for (&threads, &workers, 0..) |*thread, *worker, worker_index| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{ worker, worker_index });
    }
    for (threads) |thread| thread.join();
    try std.testing.expectEqual(@as(u8, 0), failed.load(.monotonic));
}

test "searchWithRequest applies filter prefix and distance bounds" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();
    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .epsilon = 2,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "keep:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.9, 0.1 }, "drop:2");
    try idx.insertWithMetadata(3, &[_]f32{ 0.8, 0.2 }, "keep:3");

    var results = try idx.searchWithRequest(.{
        .query = &[_]f32{ 1.0, 0.0 },
        .k = 10,
        .filter_prefix = "keep:",
        .distance_under = 0.1,
    });
    defer results.deinit();

    try std.testing.expectEqual(@as(usize, 2), results.items.items.len);
    try std.testing.expect(results.items.items[0].distance < 0.1);
    try std.testing.expect(results.items.items[1].distance < 0.1);
    try std.testing.expect(results.items.items[0].metadata != null);
    try std.testing.expect(results.items.items[1].metadata != null);
    try std.testing.expect(std.mem.startsWith(u8, results.items.items[0].metadata.?, "keep:"));
    try std.testing.expect(std.mem.startsWith(u8, results.items.items[1].metadata.?, "keep:"));

    var over_results = try idx.searchWithRequest(.{
        .query = &[_]f32{ 1.0, 0.0 },
        .k = 10,
        .distance_over = 0.01,
    });
    defer over_results.deinit();
    for (over_results.items.items) |item| {
        try std.testing.expect(item.distance > 0.01);
    }
}

test "searchWithRequest can skip metadata population" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();
    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .epsilon = 2,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.9, 0.1 }, "doc:2");

    var results = try idx.searchWithRequest(.{
        .query = &[_]f32{ 1.0, 0.0 },
        .k = 2,
        .load_metadata = false,
    });
    defer results.deinit();

    try std.testing.expectEqual(@as(usize, 2), results.items.items.len);
    for (results.items.items) |item| {
        try std.testing.expect(item.metadata == null);
    }
}

test "searchWithRequest applies filter and exclude ids" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();
    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .epsilon = 2,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.01, 0.0 }, "doc:2");
    try idx.insertWithMetadata(3, &[_]f32{ 0.02, 0.0 }, "doc:3");

    var included = try idx.searchWithRequest(.{
        .query = &[_]f32{ 0.0, 0.0 },
        .k = 3,
        .filter_ids = &[_]u64{ 2, 3 },
    });
    defer included.deinit();
    try std.testing.expectEqual(@as(usize, 2), included.items.items.len);
    try std.testing.expectEqual(@as(u64, 2), included.items.items[0].vector_id);
    try std.testing.expectEqual(@as(u64, 3), included.items.items[1].vector_id);

    var excluded = try idx.searchWithRequest(.{
        .query = &[_]f32{ 0.0, 0.0 },
        .k = 3,
        .exclude_ids = &[_]u64{1},
    });
    defer excluded.deinit();
    try std.testing.expectEqual(@as(usize, 2), excluded.items.items.len);
    try std.testing.expectEqual(@as(u64, 2), excluded.items.items[0].vector_id);
    try std.testing.expectEqual(@as(u64, 3), excluded.items.items[1].vector_id);
}

test "searchProfiled respects rerank_policy never" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 4,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .rerank_policy = .never,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0, 0.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0, 0.0, 0.0 }, "doc:2");
    try idx.insertWithMetadata(3, &[_]f32{ 0.2, 0.0, 0.0, 0.0 }, "doc:3");

    var profiled = try idx.searchProfiledRequest(.{
        .query = &[_]f32{ 0.0, 0.0, 0.0, 0.0 },
        .k = 2,
    });
    defer profiled.results.deinit();

    try std.testing.expectEqual(@as(u64, 0), profiled.profile.reranked_vectors);
    try std.testing.expectEqual(@as(usize, 2), profiled.results.getHits().len);
    for (profiled.results.items.items) |item| {
        try std.testing.expect(item.metadata != null);
    }
}

test "root quantized set is persisted as nonquantized" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");

    var txn = try idx.beginReadTxn();
    defer txn.abort();

    const quantized = (try idx.getQuantized(&txn, idx.metadata.root_node, true, 2)) orelse return error.TestUnexpectedResult;
    switch (quantized.*) {
        .nonquant => |set| {
            try std.testing.expectEqual(@as(usize, 2), set.getCount());
            try std.testing.expectEqual(@as(i64, 2), set.vectors.dims);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "loadQuantized rejects malformed non-root quantized count" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(alloc, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:2");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:3");
    try idx.insertWithMetadata(4, &[_]f32{ 10.1, 10.0 }, "doc:4");

    var leaf_id: u64 = 0;
    var expected_count: usize = 0;
    var bad_quantized: QuantizedSet = undefined;
    {
        var read_txn = try idx.beginReadTxn();
        defer read_txn.abort();
        var root = try idx.loadNode(&read_txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(!root.is_leaf);

        leaf_id = root.children[0];
        var leaf = try idx.loadNode(&read_txn, leaf_id);
        defer leaf.deinit(alloc);
        expected_count = leaf.members.len;

        bad_quantized = try idx.loadQuantized(&read_txn, leaf_id, false, expected_count);
    }
    defer bad_quantized.deinit(alloc);

    switch (bad_quantized) {
        .rabit => |*set| {
            try std.testing.expect(set.code_counts.len > 0);
            const shorter = try alloc.dupe(u32, set.code_counts[0 .. set.code_counts.len - 1]);
            alloc.free(set.code_counts);
            set.code_counts = shorter;
        },
        else => return error.TestUnexpectedResult,
    }

    var write_txn = try idx.beginWriteTxn();
    errdefer write_txn.abort();

    const encoded = switch (bad_quantized) {
        .rabit => |*set| try set.encode(alloc),
        .nonquant => |*set| try set.encode(alloc),
    };
    defer alloc.free(encoded);

    var key_buf: [10]u8 = undefined;
    try idx.putNamespaced(&write_txn, .quant, encodeQuantKey(&key_buf, leaf_id), encoded);
    try write_txn.commit();

    var validate_txn = try idx.beginReadTxn();
    defer validate_txn.abort();
    try std.testing.expectError(error.Corrupted, idx.loadQuantized(&validate_txn, leaf_id, false, expected_count));
}

test "batch insert options can defer quantized rebuild until finish" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.1, 10.0 }, .metadata = "doc:4" },
    };

    var txn = try idx.beginWriteTxn();
    var txn_active = true;
    errdefer if (txn_active) txn.abort();
    try idx.batchInsertWithMetadataTxnOptions(&txn, &items, .{
        .defer_quantized_rebuild = true,
        .centroid_only_routing = true,
    });
    try idx.finishWriteTxnOptions(&txn, .{
        .defer_quantized_rebuild = true,
        .centroid_only_routing = true,
    });
    txn_active = false;

    var results = try idx.search(&[_]f32{ 10.0, 10.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 2), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 3), results.getHits()[0].vector_id);

    var read_txn = try idx.beginReadTxn();
    defer read_txn.abort();
    var root = try idx.loadNode(&read_txn, idx.metadata.root_node);
    defer root.deinit(std.testing.allocator);
    const quantized = (try idx.getQuantized(&read_txn, idx.metadata.root_node, root.parent == 0, root.children.len)) orelse return error.TestUnexpectedResult;
    switch (quantized.*) {
        .nonquant, .rabit => {},
    }
}

test "deferred quantized rebuild refreshes touched nodes only" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(alloc, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    for (0..24) |i| {
        const vector_id: u64 = @intCast(i + 1);
        const x: f32 = @floatFromInt(i % 6);
        const y: f32 = @floatFromInt(i / 6);
        const vector = [_]f32{ x, y };
        var metadata_buf: [32]u8 = undefined;
        const metadata = try std.fmt.bufPrint(&metadata_buf, "doc:{d}", .{vector_id});
        try idx.insertWithMetadata(vector_id, &vector, metadata);
    }

    const node_count = idx.stats().node_count;
    try std.testing.expect(node_count > 6);

    idx.resetWriteProfile();
    const update = [_]BatchInsertItem{
        .{ .vector_id = 5, .vector = &[_]f32{ 0.25, 0.75 }, .metadata = "doc:5-updated" },
    };
    try idx.batchInsertWithMetadataOptions(&update, .{
        .defer_quantized_rebuild = true,
        .centroid_only_routing = true,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expect(profile.ns_quant_put_calls > 0);
    try std.testing.expect(profile.ns_quant_put_calls < node_count);
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_quantized_nodes.count());

    var results = try idx.search(&[_]f32{ 0.25, 0.75 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 5), results.getHits()[0].vector_id);
}

test "deferred quantized append updates leaf without queued full rebuild" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    const base_vectors = [_][2]f32{
        .{ 0.0, 0.0 },
        .{ 0.1, 0.0 },
        .{ 0.2, 0.0 },
        .{ 0.3, 0.0 },
        .{ 8.0, 8.0 },
        .{ 8.1, 8.0 },
        .{ 8.2, 8.0 },
        .{ 8.3, 8.0 },
        .{ 16.0, 16.0 },
    };
    var base_items: [base_vectors.len]BatchInsertItem = undefined;
    for (&base_items, 0..) |*item, i| {
        item.* = .{
            .vector_id = @intCast(i + 1),
            .vector = &base_vectors[i],
            .metadata = "doc",
        };
    }

    var idx = try HBCIndex.open(alloc, tmp_path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 4,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    try idx.bulkBuildWithMetadata(&base_items);
    try std.testing.expect(idx.stats().node_count > 1);

    const append_vector = [_]f32{ 0.05, 0.0 };
    {
        var read_txn = try idx.beginReadTxn();
        defer read_txn.abort();
        const leaf_id = try idx.findLeafWithOptions(&read_txn, idx.metadata.root_node, &append_vector, true);
        var leaf = try idx.loadNode(&read_txn, leaf_id);
        defer leaf.deinit(alloc);
        try std.testing.expect(leaf.parent != 0);
        try std.testing.expect(leaf.members.len < idx.config.leaf_size);
    }

    const append_items = [_]BatchInsertItem{
        .{ .vector_id = 10, .vector = &append_vector, .metadata = "doc:10" },
    };
    idx.resetWriteProfile();
    var txn = try idx.beginWriteTxn();
    var txn_active = true;
    errdefer if (txn_active) txn.abort();
    try idx.batchInsertWithMetadataTxnOptions(&txn, &append_items, .{
        .assume_absent_ids = true,
        .defer_quantized_rebuild = true,
        .allow_quantized_routing = true,
    });

    try std.testing.expectEqual(@as(u32, 0), idx.deferred_quantized_nodes.count());
    const profile = idx.getWriteProfile();
    try std.testing.expect(profile.ns_quant_put_calls > 0);
    try txn.commit();
    txn_active = false;

    var results = try idx.search(&append_vector, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 10), results.getHits()[0].vector_id);
}

test "bulk ingest session publishes deferred quantized nodes once at finish" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, tmp_path, .{
        .dims = 2,
        .leaf_size = 3,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 8,
        },
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &[_]f32{ 10.1, 10.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &[_]f32{ 10.2, 10.0 }, .metadata = "doc:6" },
    };
    const options: BatchInsertOptions = .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .defer_quantized_rebuild_to_bulk_finish = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();
    try idx.batchInsertWithMetadataOptions(items[0..3], options);
    try std.testing.expect(idx.deferred_quantized_nodes.count() > 0);
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().ns_quant_put_calls);

    try idx.batchInsertWithMetadataOptions(items[3..], options);
    try std.testing.expect(idx.deferred_quantized_nodes.count() > 0);
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().ns_quant_put_calls);

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_quantized_nodes.count());
    try std.testing.expect(idx.getWriteProfile().ns_quant_put_calls > 0);

    var results = try idx.search(&[_]f32{ 10.1, 10.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 2), results.getHits().len);
}

test "streaming replay session publishes reopenable batches without incomplete marker" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    const config: HBCConfig = .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    };
    var idx = try HBCIndex.openWithLsmOptions(alloc, tmp_path, config, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 8,
        },
    });

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.1, 10.0 }, .metadata = "doc:4" },
    };

    try idx.beginStreamingReplaySession();
    try std.testing.expectEqual(WriteSessionKind.streaming_replay, idx.write_session_kind.?);
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .defer_quantized_rebuild_to_bulk_finish = true,
        .defer_leaf_splits_to_bulk_finish = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
    });

    try std.testing.expectEqual(@as(u32, 0), idx.deferred_quantized_nodes.count());
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_node_keys.count());
    try std.testing.expectEqual(@as(u64, items.len), idx.publishedActiveCount());

    // A long-lived streaming scope may contain several independently durable
    // mutation batches. Exercise overwrite, delete, and insert before the
    // process interruption; replaying any suffix remains idempotent.
    try idx.batchDelete(&.{2});
    try idx.batchInsertWithMetadataOptions(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 20.0, 20.0 }, .metadata = "doc:1-updated" },
        .{ .vector_id = 5, .vector = &[_]f32{ 20.1, 20.0 }, .metadata = "doc:5" },
    }, .{
        .assume_absent_ids = false,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .defer_quantized_rebuild_to_bulk_finish = true,
        .defer_leaf_splits_to_bulk_finish = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
    });
    try std.testing.expectEqual(@as(u64, 4), idx.publishedActiveCount());

    var read = try idx.beginReadTxn();
    try std.testing.expectError(error.NotFound, read.get(.meta, bulk_publish_state_key));
    read.abort();

    // Do not run either session finish or abort. The crash teardown discards
    // unsynced process state, so reopen proves that the committed batch and its
    // metadata are independently recoverable and carry no publication marker.
    idx.abandonAfterCrash();

    var reopened = try HBCIndex.openWithLsmOptions(alloc, tmp_path, config, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 8,
        },
    });
    try std.testing.expectEqual(@as(u64, 4), reopened.stats().active_count);
    var results = try reopened.search(&[_]f32{ 20.0, 20.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 2), results.getHits().len);

    // Replay the committed suffix: identical insert, cross-leaf overwrite,
    // absent delete, and delete-then-insert. A second interruption must still
    // reopen with exact cardinality and final values.
    try reopened.beginStreamingReplaySession();
    try reopened.batchInsertWithMetadataOptions(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 20.0, 20.0 }, .metadata = "doc:1-updated" },
        .{ .vector_id = 3, .vector = &[_]f32{ -20.0, -20.0 }, .metadata = "doc:3-moved" },
    }, .{
        .assume_absent_ids = false,
        .coalesce_leaf_writes = true,
        .bulk_ingest = true,
    });
    try reopened.batchDelete(&.{ 99, 5 });
    try reopened.batchInsertWithMetadataOptions(&.{
        .{ .vector_id = 5, .vector = &[_]f32{ 20.1, 20.0 }, .metadata = "doc:5" },
    }, .{
        .assume_absent_ids = false,
        .coalesce_leaf_writes = true,
        .bulk_ingest = true,
    });
    try std.testing.expectEqual(@as(u64, 4), reopened.publishedActiveCount());
    reopened.abandonAfterCrash();

    var final = try HBCIndex.openWithLsmOptions(alloc, tmp_path, config, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 8,
        },
    });
    defer final.close();
    try std.testing.expectEqual(@as(u64, 4), final.stats().active_count);
    const moved_metadata = (try final.getMetadata(3)) orelse return error.TestUnexpectedResult;
    defer alloc.free(moved_metadata);
    try std.testing.expectEqualStrings("doc:3-moved", moved_metadata);
    var moved_results = try final.search(&[_]f32{ -20.0, -20.0 }, 1);
    defer moved_results.deinit();
    try std.testing.expectEqual(@as(u64, 3), moved_results.getHits()[0].vector_id);
}

test "streaming replay finish establishes durability for relaxed backend" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();
    const config: HBCConfig = .{
        .dims = 2,
        .storage_backend = .lsm,
    };
    const options: hbc_backend.LsmOptions = .{
        .backend_options = .{ .backend = .{ .durability = .none } },
    };
    var idx = try HBCIndex.openWithLsmOptions(alloc, tmp_path, config, options);
    try idx.beginStreamingReplaySession();
    try idx.batchInsertWithMetadataOptions(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 1, 0 }, .metadata = "doc:1" },
    }, .{
        .assume_absent_ids = true,
        .bulk_ingest = true,
    });
    // The finish performs an explicit forced sync before a higher layer may
    // advance its durable applied-sequence watermark.
    try idx.finishStreamingReplaySessionWithOptions(.{});
    idx.abandonAfterCrash();

    var reopened = try HBCIndex.openWithLsmOptions(alloc, tmp_path, config, options);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.stats().active_count);
    var read = try reopened.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(.meta, bulk_publish_state_key));
}

test "streaming replay finish establishes explicit durability for lmdb no_sync" {
    if (!supports_lmdb) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();
    const config: HBCConfig = .{
        .dims = 2,
        .storage_backend = .lmdb,
        .no_sync = true,
        .no_meta_sync = true,
    };
    var idx = try HBCIndex.open(alloc, tmp_path, config);
    try idx.beginStreamingReplaySession();
    try idx.batchInsertWithMetadataOptions(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 1, 0 }, .metadata = "doc:1" },
    }, .{
        .assume_absent_ids = true,
        .bulk_ingest = true,
    });
    // The logical LMDB commit runs with no_sync, so the streaming finish must
    // perform the explicit forced sync before a caller can publish its applied
    // sequence.
    try idx.finishStreamingReplaySessionWithOptions(.{});
    idx.close();

    var reopened = try HBCIndex.open(alloc, tmp_path, config);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.stats().active_count);
    const metadata = (try reopened.getMetadata(1)) orelse return error.TestUnexpectedResult;
    defer alloc.free(metadata);
    try std.testing.expectEqualStrings("doc:1", metadata);
}

test "interrupted bulk publication remains quarantined" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    const config: HBCConfig = .{
        .dims = 2,
        .storage_backend = .lsm,
    };
    var idx = try HBCIndex.open(alloc, tmp_path, config);
    try idx.beginBulkIngestSession();
    try idx.batchInsertWithMetadataOptions(&[_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc:1" },
    }, .{
        .assume_absent_ids = true,
        .bulk_ingest = true,
    });
    idx.abandonAfterCrash();

    try std.testing.expectError(
        error.IncompleteBulkPublish,
        HBCIndex.open(alloc, tmp_path, config),
    );
}

test "bulk publication revalidates admission before every publish window" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    const config: HBCConfig = .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
        .prefer_key_local_leaf_splits = true,
    };
    var idx = try HBCIndex.open(alloc, tmp_path, config);
    var abandoned = false;
    defer if (!abandoned) idx.close();
    try idx.batchInsertWithMetadata(&.{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 100.0, 0.0 }, .metadata = "doc:3" },
    });
    try idx.beginBulkIngestSession();
    try idx.batchInsertWithMetadataOptions(&.{
        .{ .vector_id = 4, .vector = &.{ 100.1, 0.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 100.2, 0.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 100.3, 0.0 }, .metadata = "doc:6" },
        .{ .vector_id = 7, .vector = &.{ 100.4, 0.0 }, .metadata = "doc:7" },
        .{ .vector_id = 8, .vector = &.{ 100.5, 0.0 }, .metadata = "doc:8" },
        .{ .vector_id = 9, .vector = &.{ 100.6, 0.0 }, .metadata = "doc:9" },
        .{ .vector_id = 10, .vector = &.{ 100.7, 0.0 }, .metadata = "doc:10" },
    }, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .defer_leaf_splits_to_bulk_finish = true,
        .bulk_ingest = true,
    });

    const Admission = struct {
        calls: usize = 0,

        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 2) return error.TestCapacityUnavailable;
        }
    };
    var admission = Admission{};
    try std.testing.expectError(error.TestCapacityUnavailable, idx.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .max_deferred_hbc_leaf_splits_per_publish = 1,
        .bulk_rebuild_hbc_leaf_min_members = 999,
        .admission_ctx = &admission,
        .admission_fn = Admission.check,
    }));
    try std.testing.expectEqual(@as(usize, 2), admission.calls);

    // The first bounded window was durable. A denied second window therefore
    // leaves the publication marker intact and the partial generation fenced.
    idx.abandonAfterCrash();
    abandoned = true;
    try std.testing.expectError(error.IncompleteBulkPublish, HBCIndex.open(alloc, tmp_path, config));
}

test "bulk publication abort clears only an unmodified session" {
    const alloc = std.testing.allocator;
    var empty_path: TestPath = .{};
    const empty_tmp_path = empty_path.init();
    defer empty_path.cleanup();

    const config: HBCConfig = .{
        .dims = 2,
        .storage_backend = .lsm,
    };
    var empty = try HBCIndex.open(alloc, empty_tmp_path, config);
    try empty.beginBulkIngestSession();
    empty.abortBulkIngestSession();
    empty.close();
    var empty_reopened = try HBCIndex.open(alloc, empty_tmp_path, config);
    empty_reopened.close();

    var dirty_path: TestPath = .{};
    const dirty_tmp_path = dirty_path.init();
    defer dirty_path.cleanup();

    var dirty = try HBCIndex.open(alloc, dirty_tmp_path, config);
    try dirty.beginBulkIngestSession();
    try dirty.batchInsertWithMetadataOptions(&[_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc:1" },
    }, .{
        .assume_absent_ids = true,
        .bulk_ingest = true,
    });
    dirty.abortBulkIngestSession();
    dirty.close();
    try std.testing.expectError(
        error.IncompleteBulkPublish,
        HBCIndex.open(alloc, dirty_tmp_path, config),
    );
}

test "write sessions reject mixed nested publication semantics" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(alloc, tmp_path, .{ .dims = 2 });
    defer idx.close();

    try idx.beginStreamingReplaySession();
    defer idx.abortStreamingReplaySession();
    try std.testing.expectError(error.MixedWriteSessionKinds, idx.beginBulkIngestSession());
    try std.testing.expectEqual(@as(usize, 1), idx.write_session_depth);
    try std.testing.expectEqual(WriteSessionKind.streaming_replay, idx.write_session_kind.?);
}

test "bulk ingest keeps published search state stale until finish" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .max_cached_vectors = 16,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.0, 1.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 0.9, 0.1 }, .metadata = "doc:3" },
    };
    const options: BatchInsertOptions = .{
        .assume_absent_ids = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    defer if (session_open) idx.abortBulkIngestSession();

    try idx.batchInsertWithMetadataOptions(&items, options);
    try std.testing.expectEqual(@as(u64, 0), idx.publishedActiveCount());
    try std.testing.expectEqual(@as(u64, 1), idx.publishedNodeCount());

    var pending = try idx.search(&[_]f32{ 1.0, 0.0 }, 2);
    defer pending.deinit();
    try std.testing.expectEqual(@as(usize, 0), pending.getHits().len);

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;

    try std.testing.expectEqual(@as(u64, 3), idx.publishedActiveCount());
    try std.testing.expect(idx.publishedNodeCount() >= 1);

    var published = try idx.search(&[_]f32{ 1.0, 0.0 }, 2);
    defer published.deinit();
    try std.testing.expect(published.getHits().len > 0);
}

test "flat centroid search ignores staged bulk ingest nodes until publish" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .search_width = 4,
        .use_quantization = true,
        .centroid_directory_mode = .flat_rabitq,
        .flat_centroid_block_size = 4,
        .flat_centroid_probe_count = 4,
        .max_cached_nodes = 16,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    const initial = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc:2" },
    };
    try idx.bulkBuildWithMetadata(&initial);

    var warm = try idx.search(&[_]f32{ 0.0, 0.0 }, 1);
    defer warm.deinit();
    try std.testing.expectEqual(@as(u64, 1), warm.getHits()[0].vector_id);

    const staged = [_]BatchInsertItem{
        .{ .vector_id = 99, .vector = &[_]f32{ 100.0, 100.0 }, .metadata = "doc:99" },
    };
    const options: BatchInsertOptions = .{
        .assume_absent_ids = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    defer if (session_open) idx.abortBulkIngestSession();

    try idx.batchInsertWithMetadataOptions(&staged, options);
    try std.testing.expectEqual(@as(u64, 2), idx.publishedActiveCount());

    var pending = try idx.search(&[_]f32{ 100.0, 100.0 }, 2);
    defer pending.deinit();
    for (pending.getHits()) |hit| {
        try std.testing.expect(hit.vector_id != 99);
    }

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;

    var published = try idx.search(&[_]f32{ 100.0, 100.0 }, 1);
    defer published.deinit();
    try std.testing.expectEqual(@as(usize, 1), published.getHits().len);
    try std.testing.expectEqual(@as(u64, 99), published.getHits()[0].vector_id);
}

test "bulk build creates searchable index and persists metadata" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:a" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.1, 0.0 }, .metadata = "doc:b" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:y" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.1, 10.0 }, .metadata = "doc:z" },
    };
    try idx.bulkBuildWithMetadata(&items);

    const stats = idx.stats();
    try std.testing.expectEqual(@as(u64, 4), stats.active_count);
    try std.testing.expect(stats.node_count > 0);

    var results = try idx.search(&[_]f32{ 10.0, 10.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(u64, 3), results.getHits()[0].vector_id);
    try std.testing.expect(results.getHits()[0].metadata != null);
    try std.testing.expectEqualStrings("doc:y", results.getHits()[0].metadata.?);

    const reopened = try idx.getMetadata(4);
    defer if (reopened) |owned| std.testing.allocator.free(owned);
    try std.testing.expect(reopened != null);
    try std.testing.expectEqualStrings("doc:z", reopened.?);
}

test "bulk build refreshes quantized payload after internal reparenting" {
    const alloc = std.testing.allocator;
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    const dims = 384;
    const count = 32;
    var vectors = try alloc.alloc(f32, count * dims);
    defer alloc.free(vectors);
    var items = try alloc.alloc(BatchInsertItem, count);
    defer alloc.free(items);

    for (0..count) |i| {
        const vector = vectors[i * dims ..][0..dims];
        for (vector, 0..) |*value, d| {
            value.* = @as(f32, @floatFromInt(((i + 1) * 17 + (d + 3) * 11) % 97)) / 97.0;
        }
        items[i] = .{
            .vector_id = @intCast(i + 1),
            .vector = vector,
            .metadata = "doc",
        };
    }

    var idx = try HBCIndex.open(alloc, tmp_path, .{
        .dims = dims,
        .leaf_size = 4,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    try idx.bulkBuildWithMetadata(items);

    const query = vectors[13 * dims ..][0..dims];
    var results = try idx.search(query, 5);
    defer results.deinit();
    try std.testing.expect(results.getHits().len > 0);
}

test "hilbert-seeded bulk build creates searchable index" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 4,
        .search_width = 8,
        .use_quantization = true,
        .bulk_build_algo = .hilbert_seeded,
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:a" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.1, 0.0 }, .metadata = "doc:b" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:y" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.1, 10.0 }, .metadata = "doc:z" },
    };
    try idx.bulkBuildWithMetadata(&items);

    var results = try idx.search(&[_]f32{ 10.0, 10.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(u64, 3), results.getHits()[0].vector_id);
    try std.testing.expect(results.getHits()[0].metadata != null);
    try std.testing.expectEqualStrings("doc:y", results.getHits()[0].metadata.?);
}

test "doc-key-seeded bulk build creates searchable index" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 4,
        .search_width = 8,
        .use_quantization = true,
        .bulk_build_algo = .doc_key_seeded,
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:a" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.1, 0.0 }, .metadata = "doc:b" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:y" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.1, 10.0 }, .metadata = "doc:z" },
    };
    try idx.bulkBuildWithMetadata(&items);

    var results = try idx.search(&[_]f32{ 10.0, 10.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(u64, 3), results.getHits()[0].vector_id);
    try std.testing.expect(results.getHits()[0].metadata != null);
    try std.testing.expectEqualStrings("doc:y", results.getHits()[0].metadata.?);
}

test "kmeans bulk build creates searchable index" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 4,
        .search_width = 8,
        .use_quantization = true,
        .bulk_build_algo = .kmeans,
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:a" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.1, 0.0 }, .metadata = "doc:b" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.0, 10.0 }, .metadata = "doc:y" },
        .{ .vector_id = 4, .vector = &[_]f32{ 10.1, 10.0 }, .metadata = "doc:z" },
        .{ .vector_id = 5, .vector = &[_]f32{ 20.0, 0.0 }, .metadata = "doc:m" },
        .{ .vector_id = 6, .vector = &[_]f32{ 20.1, 0.0 }, .metadata = "doc:n" },
    };
    try idx.bulkBuildWithMetadata(&items);

    const stats = idx.stats();
    try std.testing.expectEqual(@as(u64, items.len), stats.active_count);
    try std.testing.expect(stats.node_count >= 4);

    var results = try idx.search(&[_]f32{ 10.0, 10.0 }, 2);
    defer results.deinit();
    try std.testing.expectEqual(@as(u64, 3), results.getHits()[0].vector_id);
    try std.testing.expect(results.getHits()[0].metadata != null);
    try std.testing.expectEqualStrings("doc:y", results.getHits()[0].metadata.?);
}

test "reinsert existing vector id does not grow active count and updates search" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .epsilon = 2,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:2");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:3");
    try std.testing.expectEqual(@as(u64, 3), idx.stats().active_count);

    try idx.insertWithMetadata(1, &[_]f32{ 10.1, 10.0 }, "doc:1-updated");
    try std.testing.expectEqual(@as(u64, 3), idx.stats().active_count);

    var near_old = try idx.searchWithRequest(.{
        .query = &[_]f32{ 0.0, 0.0 },
        .k = 3,
    });
    defer near_old.deinit();
    try std.testing.expectEqual(@as(u64, 2), near_old.items.items[0].vector_id);

    var near_new = try idx.searchWithRequest(.{
        .query = &[_]f32{ 10.0, 10.0 },
        .k = 3,
    });
    defer near_new.deinit();
    try std.testing.expectEqual(@as(u64, 3), near_new.items.items[0].vector_id);
    try std.testing.expectEqual(@as(u64, 1), near_new.items.items[1].vector_id);
    try std.testing.expect(near_new.items.items[1].metadata != null);
    try std.testing.expectEqualStrings("doc:1-updated", near_new.items.items[1].metadata.?);
}

test "reinsert existing vector id after reopen on lsm backend updates search" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    {
        var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
            .dims = 2,
            .leaf_size = 2,
            .branching_factor = 2,
            .search_width = 8,
            .epsilon = 2,
            .storage_backend = .lsm,
        });
        defer idx.close();

        try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:1");
        try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:2");
        try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:3");
        try std.testing.expectEqual(@as(u64, 3), idx.stats().active_count);
    }

    {
        var reopened = try HBCIndex.open(std.testing.allocator, tmp_path, .{
            .dims = 2,
            .leaf_size = 2,
            .branching_factor = 2,
            .search_width = 8,
            .epsilon = 2,
            .storage_backend = .lsm,
        });
        defer reopened.close();

        try reopened.insertWithMetadata(1, &[_]f32{ 10.1, 10.0 }, "doc:1-updated");
        try std.testing.expectEqual(@as(u64, 3), reopened.stats().active_count);

        var near_old = try reopened.searchWithRequest(.{
            .query = &[_]f32{ 0.0, 0.0 },
            .k = 3,
        });
        defer near_old.deinit();
        try std.testing.expectEqual(@as(u64, 2), near_old.items.items[0].vector_id);

        var near_new = try reopened.searchWithRequest(.{
            .query = &[_]f32{ 10.0, 10.0 },
            .k = 3,
        });
        defer near_new.deinit();
        try std.testing.expectEqual(@as(u64, 3), near_new.items.items[0].vector_id);
        try std.testing.expectEqual(@as(u64, 1), near_new.items.items[1].vector_id);
        try std.testing.expect(near_new.items.items[1].metadata != null);
        try std.testing.expectEqualStrings("doc:1-updated", near_new.items.items[1].metadata.?);
    }
}

test "kmeans split produces balanced clusters" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2, .leaf_size = 5 });
    defer idx.close();

    // Insert two distinct clusters
    // Cluster A near (0, 0)
    try idx.insert(1, &[_]f32{ 0.0, 0.0 });
    try idx.insert(2, &[_]f32{ 0.1, 0.1 });
    try idx.insert(3, &[_]f32{ 0.2, 0.0 });
    // Cluster B near (10, 10)
    try idx.insert(4, &[_]f32{ 10.0, 10.0 });
    try idx.insert(5, &[_]f32{ 10.1, 10.1 });
    try idx.insert(6, &[_]f32{ 10.2, 10.0 });

    // Should have split — root is internal node now
    const s = idx.stats();
    try std.testing.expectEqual(@as(u64, 6), s.active_count);
    try std.testing.expect(s.node_count > 1);

    // Search in cluster A should find cluster A vectors first
    var results = try idx.search(&[_]f32{ 0.0, 0.0 }, 3);
    defer results.deinit();
    const hits = results.getHits();
    try std.testing.expectEqual(@as(usize, 3), hits.len);
    // All three closest should be from cluster A (IDs 1-3)
    for (hits) |hit| {
        try std.testing.expect(hit.vector_id <= 3);
    }
}

test "inner product split keeps mean centroids" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .metric = .inner_product,
        .leaf_size = 8,
    });
    defer idx.close();

    const ids = [_]u64{ 1, 2, 3, 4 };
    const raw = [_]f32{
        2.0,  0.0,
        4.0,  0.0,
        -2.0, 0.0,
        -4.0, 0.0,
    };
    const vector_set = vec.Set{
        .dims = 2,
        .count = ids.len,
        .data = @constCast(raw[0..]),
    };

    const split = try idx.splitVectorSet(&vector_set, &ids);
    defer alloc.free(split.c1);
    defer alloc.free(split.g1);
    defer alloc.free(split.c2);
    defer alloc.free(split.g2);

    try std.testing.expectApproxEqAbs(@as(f32, 3.0), vec.norm(split.c1), 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), vec.norm(split.c2), 0.001);
}

test "cosine split rejects non unit vectors" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .metric = .cosine,
        .leaf_size = 8,
    });
    defer idx.close();

    const ids = [_]u64{ 1, 2 };
    const raw = [_]f32{
        2.0, 0.0,
        0.0, 1.0,
    };
    const vector_set = vec.Set{
        .dims = 2,
        .count = ids.len,
        .data = @constCast(raw[0..]),
    };

    try std.testing.expectError(error.NonUnitVector, idx.splitVectorSet(&vector_set, &ids));
}

test "cosine leaf centroid stays unit through insert update delete" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .metric = .cosine,
        .leaf_size = 8,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc:2");

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(root.is_leaf);
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), vec.norm(root.centroid), 0.001);
    }

    try idx.insertWithMetadata(1, &[_]f32{ 0.70710677, 0.70710677 }, "doc:1-updated");
    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(root.is_leaf);
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), vec.norm(root.centroid), 0.001);
    }

    try idx.delete(2);
    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(root.is_leaf);
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), vec.norm(root.centroid), 0.001);
    }
}

test "cosine split root centroid stays unit" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .metric = .cosine,
        .leaf_size = 2,
        .branching_factor = 2,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc:1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.9805807, 0.19611613 }, "doc:2");
    try idx.insertWithMetadata(3, &[_]f32{ -1.0, 0.0 }, "doc:3");
    try idx.insertWithMetadata(4, &[_]f32{ -0.9805807, 0.19611613 }, "doc:4");

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    var root = try idx.loadNode(&txn, idx.metadata.root_node);
    defer root.deinit(alloc);
    try std.testing.expect(!root.is_leaf);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), vec.norm(root.centroid), 0.001);

    for (root.children) |child_id| {
        var child = try idx.loadNode(&txn, child_id);
        defer child.deinit(alloc);
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), vec.norm(child.centroid), 0.001);
    }
}

test "collectCompetitiveInsertCandidates retains maybe closer children" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
    });
    defer idx.close();

    const child_ids = [_]u64{ 11, 22, 33 };
    const distances = [_]f32{ 1.00, 1.06, 1.40 };
    const error_bounds = [_]f32{ 0.02, 0.08, 0.01 };

    const competitive = try idx.collectCompetitiveInsertCandidates(&child_ids, &distances, &error_bounds);
    defer std.testing.allocator.free(competitive);

    try std.testing.expectEqual(@as(usize, 2), competitive.len);
    try std.testing.expectEqual(@as(u64, 11), competitive[0].id);
    try std.testing.expectEqual(@as(u64, 22), competitive[1].id);
}

test "hilbert split produces balanced clusters" {
    var path: TestPath = .{};
    const tmp_path = path.init();
    defer path.cleanup();

    var idx = try HBCIndex.open(std.testing.allocator, tmp_path, .{
        .dims = 2,
        .leaf_size = 5,
        .split_algo = .hilbert,
    });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 0.0, 0.0 });
    try idx.insert(2, &[_]f32{ 0.1, 0.1 });
    try idx.insert(3, &[_]f32{ 0.2, 0.0 });
    try idx.insert(4, &[_]f32{ 10.0, 10.0 });
    try idx.insert(5, &[_]f32{ 10.1, 10.1 });
    try idx.insert(6, &[_]f32{ 10.2, 10.0 });

    const s = idx.stats();
    try std.testing.expectEqual(@as(u64, 6), s.active_count);
    try std.testing.expect(s.node_count > 1);

    var results = try idx.search(&[_]f32{ 0.0, 0.0 }, 3);
    defer results.deinit();
    const hits = results.getHits();
    try std.testing.expectEqual(@as(usize, 3), hits.len);
    for (hits) |hit| {
        try std.testing.expect(hit.vector_id <= 3);
    }
}

test "delete removes vector" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2, .leaf_size = 10 });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 1.0, 0.0 });
    try idx.insert(2, &[_]f32{ 0.0, 1.0 });
    try idx.insert(3, &[_]f32{ 1.0, 1.0 });

    try std.testing.expectEqual(@as(u64, 3), idx.stats().active_count);

    try idx.delete(2);
    try std.testing.expectEqual(@as(u64, 2), idx.stats().active_count);

    // Search should not return deleted vector
    var results = try idx.search(&[_]f32{ 0.0, 1.0 }, 3);
    defer results.deinit();
    for (results.getHits()) |hit| {
        try std.testing.expect(hit.vector_id != 2);
    }
}

test "batchApply supports mixed writes and deletes" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2, .leaf_size = 10 });
    defer idx.close();

    try idx.batchInsertWithMetadata(&.{
        .{ .vector_id = 1, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc-1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.0, 1.0 }, .metadata = "doc-2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 1.0, 1.0 }, .metadata = "doc-3" },
    });

    try idx.batchApply(&.{
        .{ .vector_id = 4, .vector = &[_]f32{ 0.2, 0.9 }, .metadata = "doc-4" },
        .{ .vector_id = 3, .vector = &[_]f32{ 0.9, 0.9 }, .metadata = "doc-3b" },
    }, &.{2});

    try std.testing.expectEqual(@as(u64, 3), idx.stats().active_count);

    var results = try idx.search(&[_]f32{ 0.0, 1.0 }, 4);
    defer results.deinit();
    var saw_two = false;
    var saw_three = false;
    var saw_four = false;
    for (results.getHits()) |hit| {
        if (hit.vector_id == 2) saw_two = true;
        if (hit.vector_id == 3) saw_three = true;
        if (hit.vector_id == 4) saw_four = true;
    }
    try std.testing.expect(!saw_two);
    try std.testing.expect(saw_three);
    try std.testing.expect(saw_four);
}

test "search returns metadata for hits" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2, .leaf_size = 4, .use_quantization = true });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 1.0, 0.0 }, "doc-1");
    try idx.insertWithMetadata(2, &[_]f32{ 0.0, 1.0 }, "doc-2");

    var results = try idx.search(&[_]f32{ 1.0, 0.0 }, 2);
    defer results.deinit();

    const hits = results.getHits();
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expect(hits[0].metadata != null);
    try std.testing.expectEqualStrings("doc-1", hits[0].metadata.?);
}

test "delete repairs underfull leaf" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2, .leaf_size = 5, .branching_factor = 2, .search_width = 8 });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 0.0, 0.0 });
    try idx.insert(2, &[_]f32{ 0.1, 0.0 });
    try idx.insert(3, &[_]f32{ 0.2, 0.0 });
    try idx.insert(4, &[_]f32{ 10.0, 10.0 });
    try idx.insert(5, &[_]f32{ 10.1, 10.0 });
    try idx.insert(6, &[_]f32{ 10.2, 10.0 });

    try idx.delete(1);
    try idx.delete(2);

    var results = try idx.search(&[_]f32{ 0.2, 0.0 }, 4);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 4), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 3), results.getHits()[0].vector_id);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    var root = try idx.loadNode(&txn, idx.metadata.root_node);
    defer root.deinit(alloc);
    if (root.is_leaf) {
        try std.testing.expect(root.members.len > 0);
    } else {
        try std.testing.expect(root.children.len > 0);
    }
}

test "node roundtrip" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 3 });
    defer idx.close();

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();

        const centroid = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
        defer alloc.free(centroid);
        const members = try alloc.dupe(u64, &[_]u64{ 10, 20, 30 });
        defer alloc.free(members);

        const node = Node{
            .id = 42,
            .is_leaf = true,
            .level = 2,
            .parent = 7,
            .centroid = centroid,
            .children = &.{},
            .members = members,
        };
        try idx.saveNode(&txn, &node);
        try txn.commit();
    }

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();

        var loaded = try idx.loadNode(&txn, 42);
        defer loaded.deinit(alloc);

        try std.testing.expectEqual(@as(u64, 42), loaded.id);
        try std.testing.expect(loaded.is_leaf);
        try std.testing.expectEqual(@as(u16, 2), loaded.level);
        try std.testing.expectEqual(@as(u64, 7), loaded.parent);
        try std.testing.expectEqualSlices(f32, &[_]f32{ 1.0, 2.0, 3.0 }, loaded.centroid);
        try std.testing.expectEqualSlices(u64, &[_]u64{ 10, 20, 30 }, loaded.members);
    }
}

test "node split ranges classify left right and mixed subtrees" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:a");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:b");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:y");
    try idx.insertWithMetadata(4, &[_]f32{ 10.1, 10.0 }, "doc:z");

    const stats = idx.stats();
    try std.testing.expect(stats.node_count > 1);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    var root = try idx.loadNode(&txn, idx.metadata.root_node);
    defer root.deinit(alloc);

    const root_range = (try idx.getNodeSplitRange(idx.metadata.root_node)) orelse return error.TestUnexpectedResult;
    defer {
        var owned = root_range;
        owned.deinit(alloc);
    }
    try std.testing.expectEqualStrings("doc:a", root_range.min_key);
    try std.testing.expectEqualStrings("doc:z", root_range.max_key);
    try std.testing.expectEqual(NodeSplitClass.mixed, try idx.classifyNodeForSplit(idx.metadata.root_node, "doc:m"));

    var saw_left = false;
    var saw_right = false;
    for (root.children) |child_id| {
        const class = try idx.classifyNodeForSplit(child_id, "doc:m");
        switch (class) {
            .left_only => saw_left = true,
            .right_only => saw_right = true,
            else => {},
        }
    }
    try std.testing.expect(saw_left);
    try std.testing.expect(saw_right);
}

test "coalesced batch insert extends ancestor split ranges for small leaf groups" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = false,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:m");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:n");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:o");
    try idx.insertWithMetadata(4, &[_]f32{ 10.1, 10.0 }, "doc:p");
    try idx.insertWithMetadata(5, &[_]f32{ 10.2, 10.0 }, "doc:q");
    try std.testing.expect(idx.stats().node_count > 1);

    idx.resetWriteProfile();
    const items = [_]BatchInsertItem{
        .{ .vector_id = 6, .vector = &[_]f32{ 0.05, 0.0 }, .metadata = "doc:a" },
        .{ .vector_id = 7, .vector = &[_]f32{ 0.06, 0.0 }, .metadata = "doc:b" },
        .{ .vector_id = 8, .vector = &[_]f32{ 10.15, 10.0 }, .metadata = "doc:z" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
    });

    try std.testing.expectEqual(@as(u64, 8), idx.stats().active_count);
    const root_range = (try idx.getNodeSplitRange(idx.metadata.root_node)) orelse return error.TestUnexpectedResult;
    defer {
        var owned = root_range;
        owned.deinit(alloc);
    }
    try std.testing.expectEqualStrings("doc:a", root_range.min_key);
    try std.testing.expectEqualStrings("doc:z", root_range.max_key);

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_calls);
    try std.testing.expect(profile.batch_route_internal_nodes > 0);
    try std.testing.expect(profile.batch_route_leaf_groups >= 2);
    try std.testing.expectEqual(@as(u64, items.len), profile.batch_route_items);
}

test "coalesced batch insert keeps routed child nodes stable across cache eviction" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const dims = 4;
    const base_count = 96;
    const insert_count = 16;
    var vectors = try alloc.alloc(f32, (base_count + insert_count) * dims);
    defer alloc.free(vectors);
    var base_items = try alloc.alloc(BatchInsertItem, base_count);
    defer alloc.free(base_items);
    var insert_items = try alloc.alloc(BatchInsertItem, insert_count);
    defer alloc.free(insert_items);

    for (0..base_count + insert_count) |i| {
        const vector = vectors[i * dims ..][0..dims];
        for (vector, 0..) |*value, d| {
            value.* = @as(f32, @floatFromInt(((i + 7) * 31 + (d + 11) * 13) % 257)) / 257.0;
        }
        if (i < base_count) {
            base_items[i] = .{
                .vector_id = @intCast(i + 1),
                .vector = vector,
                .metadata = "doc",
            };
        } else {
            insert_items[i - base_count] = .{
                .vector_id = @intCast(i + 1),
                .vector = vector,
                .metadata = "doc",
            };
        }
    }

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = dims,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 16,
        .use_quantization = false,
        .max_cached_nodes = 2,
        .max_cached_vectors = 2,
    });
    defer idx.close();

    try idx.bulkBuildWithMetadata(base_items);
    try std.testing.expect(idx.stats().node_count > 8);

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(insert_items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .allow_quantized_routing = false,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, base_count + insert_count), idx.stats().active_count);
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_calls);
    try std.testing.expect(profile.batch_route_internal_nodes > 0);
    try std.testing.expectEqual(@as(u64, insert_items.len), profile.batch_route_items);

    var results = try idx.search(insert_items[0].vector, 1);
    defer results.deinit();
    try std.testing.expect(results.getHits().len > 0);
}

test "mixed delete write batch keeps covered replacements on sequential routing" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = false,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:a");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:b");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:c");
    try idx.insertWithMetadata(4, &[_]f32{ 10.1, 10.0 }, "doc:d");
    try idx.insertWithMetadata(5, &[_]f32{ 20.0, 20.0 }, "doc:e");
    try std.testing.expect(idx.stats().node_count > 1);

    idx.resetWriteProfile();
    const writes = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.2, 0.0 }, .metadata = "doc:a2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 10.2, 10.0 }, .metadata = "doc:c2" },
        .{ .vector_id = 5, .vector = &[_]f32{ 20.2, 20.0 }, .metadata = "doc:e2" },
    };
    const deletes = [_]u64{ 1, 3, 5 };
    try idx.batchApplyOptions(&writes, &deletes, .{
        .assume_absent_ids = false,
        .coalesce_leaf_writes = true,
        .allow_quantized_routing = false,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 5), idx.stats().active_count);
    // Transaction-local deletes allow the existence lookup to be skipped, but
    // are not proof that grouped routing against one pre-insert topology is
    // quality-safe. This pins the replacement invariant documented above.
    try std.testing.expectEqual(@as(u64, 0), profile.batch_route_calls);
    try std.testing.expectEqual(@as(u64, 0), profile.batch_route_items);

    var results = try idx.search(&[_]f32{ 20.2, 20.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 5), results.getHits()[0].vector_id);
}

test "coalesced batch insert routes writes with quantized child scores" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const dims = 16;
    const base_count = 32;
    const insert_count = 8;
    var vectors = try alloc.alloc(f32, (base_count + insert_count) * dims);
    defer alloc.free(vectors);
    var base_items = try alloc.alloc(BatchInsertItem, base_count);
    defer alloc.free(base_items);
    var insert_items = try alloc.alloc(BatchInsertItem, insert_count);
    defer alloc.free(insert_items);

    for (0..base_count + insert_count) |i| {
        const vector = vectors[i * dims ..][0..dims];
        for (vector, 0..) |*value, d| {
            value.* = @as(f32, @floatFromInt(((i + 1) * 17 + (d + 3) * 11) % 97)) / 97.0;
        }
        if (i < base_count) {
            base_items[i] = .{
                .vector_id = @intCast(i + 1),
                .vector = vector,
                .metadata = "doc",
            };
        } else {
            insert_items[i - base_count] = .{
                .vector_id = @intCast(i + 1),
                .vector = vector,
                .metadata = "doc",
            };
        }
    }

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = dims,
        .leaf_size = 4,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = true,
    });
    defer idx.close();

    try idx.bulkBuildWithMetadata(base_items);
    try std.testing.expect(idx.stats().node_count > 1);

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(insert_items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .allow_quantized_routing = true,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_calls);
    try std.testing.expect(profile.batch_route_internal_nodes > 0);
    try std.testing.expect(profile.batch_route_quantized_nodes > 0);
    try std.testing.expectEqual(@as(u64, insert_items.len), profile.batch_route_items);
}

test "coalesced batch insert splits one-overflow leaf group once" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = false,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:a");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:b");
    try idx.insertWithMetadata(3, &[_]f32{ 0.2, 0.0 }, "doc:c");

    idx.resetWriteProfile();
    const items = [_]BatchInsertItem{
        .{ .vector_id = 4, .vector = &[_]f32{ 0.3, 0.0 }, .metadata = "doc:d" },
        .{ .vector_id = 5, .vector = &[_]f32{ 0.4, 0.0 }, .metadata = "doc:e" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 5), idx.stats().active_count);
    try std.testing.expect(idx.stats().node_count > 1);
    try std.testing.expectEqual(@as(u64, 1), profile.split_leaf_calls);
    try std.testing.expectEqual(@as(u64, 1), profile.grouped_leaf_groups);
    try std.testing.expectEqual(@as(u64, 2), profile.grouped_items);
    try std.testing.expectEqual(@as(u64, 0), profile.grouped_fallback_items);
    try std.testing.expectEqual(@as(u64, 1), profile.grouped_split_candidates);
    try std.testing.expectEqual(@as(u64, 1), profile.grouped_recursive_splits);
    try std.testing.expectEqual(@as(u64, 2), profile.grouped_split_scan_iterations);
    try std.testing.expectEqual(@as(u64, 2), profile.grouped_split_queue_peak_total);
    try std.testing.expectEqual(@as(u64, 5), profile.split_leaf_input_members_total);
    try std.testing.expectEqual(@as(u64, 1), profile.split_leaf_input_overflow_members_total);
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_calls);
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_leaf_groups);
    try std.testing.expectEqual(@as(u64, items.len), profile.batch_route_items);

    var results = try idx.search(&[_]f32{ 0.4, 0.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 5), results.getHits()[0].vector_id);
}

test "coalesced batch insert recursively splits bounded overflow leaf group" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:a");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:b");
    try idx.insertWithMetadata(3, &[_]f32{ 0.2, 0.0 }, "doc:c");

    idx.resetWriteProfile();
    const items = [_]BatchInsertItem{
        .{ .vector_id = 4, .vector = &[_]f32{ 0.3, 0.0 }, .metadata = "doc:d" },
        .{ .vector_id = 5, .vector = &[_]f32{ 0.4, 0.0 }, .metadata = "doc:e" },
        .{ .vector_id = 6, .vector = &[_]f32{ 0.5, 0.0 }, .metadata = "doc:f" },
        .{ .vector_id = 7, .vector = &[_]f32{ 0.6, 0.0 }, .metadata = "doc:g" },
        .{ .vector_id = 8, .vector = &[_]f32{ 0.7, 0.0 }, .metadata = "doc:h" },
        .{ .vector_id = 9, .vector = &[_]f32{ 0.8, 0.0 }, .metadata = "doc:i" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 9), idx.stats().active_count);
    try std.testing.expect(profile.split_leaf_calls >= 2);
    try std.testing.expectEqual(@as(u64, 1), profile.grouped_leaf_groups);
    try std.testing.expectEqual(@as(u64, 6), profile.grouped_items);
    try std.testing.expectEqual(@as(u64, 0), profile.grouped_fallback_items);
    try std.testing.expectEqual(@as(u64, 1), profile.grouped_split_candidates);
    try std.testing.expect(profile.grouped_recursive_splits >= 2);
    try std.testing.expect(profile.grouped_split_scan_iterations >= profile.grouped_recursive_splits);
    try std.testing.expect(profile.grouped_split_queue_peak_total >= 2);
    try std.testing.expect(profile.split_leaf_input_members_total > 0);
    try std.testing.expect(profile.split_leaf_input_overflow_members_total > 0);
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_calls);
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_leaf_groups);
    try std.testing.expectEqual(@as(u64, items.len), profile.batch_route_items);

    var results = try idx.search(&[_]f32{ 0.8, 0.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 9), results.getHits()[0].vector_id);
}

test "split planning stats count reusable dense subtrees" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:a");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:b");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:y");
    try idx.insertWithMetadata(4, &[_]f32{ 10.1, 10.0 }, "doc:z");

    const stats = try idx.splitPlanningStats("doc:m");
    try std.testing.expect(stats.leaves > 0);
    try std.testing.expect(stats.internal > 0);
    try std.testing.expect(stats.left_only > 0);
    try std.testing.expect(stats.right_only > 0);
    try std.testing.expect(stats.mixed > 0);
    try std.testing.expectEqual(@as(usize, 0), stats.unknown);
}

test "split reuse plan finds right-only subtree roots" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:a");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:b");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:y");
    try idx.insertWithMetadata(4, &[_]f32{ 10.1, 10.0 }, "doc:z");

    var plan = try idx.buildSplitReusePlan("doc:m");
    defer plan.deinit(alloc);

    try std.testing.expect(plan.right_only_roots.len > 0);
    for (plan.right_only_roots) |node_id| {
        try std.testing.expectEqual(NodeSplitClass.right_only, try idx.classifyNodeForSplit(node_id, "doc:m"));
    }
    for (plan.mixed_leaves) |node_id| {
        try std.testing.expectEqual(NodeSplitClass.mixed, try idx.classifyNodeForSplit(node_id, "doc:m"));
    }
}

test "split rebuild work counts reusable and mixed-right members" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
    });
    defer idx.close();

    try idx.insertWithMetadata(1, &[_]f32{ 0.0, 0.0 }, "doc:a");
    try idx.insertWithMetadata(2, &[_]f32{ 0.1, 0.0 }, "doc:b");
    try idx.insertWithMetadata(3, &[_]f32{ 10.0, 10.0 }, "doc:y");
    try idx.insertWithMetadata(4, &[_]f32{ 10.1, 10.0 }, "doc:z");

    const work = try idx.estimateSplitRebuildWork("doc:m");
    try std.testing.expect(work.mixed_leaves > 0 or work.right_only_roots > 0);
    try std.testing.expect(work.totalRightMembers() > 0);
}

test "search returns results from both halves after split" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 3,
        .search_width = 32,
    });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 0.0, 0.0 });
    try idx.insert(2, &[_]f32{ 0.1, 0.1 });
    try idx.insert(3, &[_]f32{ 0.2, 0.2 });
    try idx.insert(4, &[_]f32{ 10.0, 10.0 });
    try idx.insert(5, &[_]f32{ 10.1, 10.1 });

    var results = try idx.search(&[_]f32{ 0.0, 0.0 }, 5);
    defer results.deinit();

    try std.testing.expectEqual(@as(usize, 5), results.getHits().len);
}

test "vector to leaf mapping stays in sync across repeated splits" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 3,
        .branching_factor = 2,
        .search_width = 32,
    });
    defer idx.close();

    for (0..18) |i| {
        const x: f32 = @floatFromInt(i / 3);
        const y: f32 = @floatFromInt(i % 3);
        try idx.insert(@intCast(i + 1), &[_]f32{ x, y });
    }

    try std.testing.expect(idx.stats().node_count > 1);

    for (0..18) |i| {
        const vector_id: u64 = @intCast(i + 1);
        const mapped_leaf = try idx.debugLeafForVector(vector_id);
        const scanned_leaf = try idx.debugScanLeafForVector(vector_id);
        try std.testing.expect(mapped_leaf != null);
        try std.testing.expectEqual(scanned_leaf, mapped_leaf);

        const members = try idx.debugLeafMembers(alloc, mapped_leaf.?);
        defer alloc.free(members);

        var found = false;
        for (members) |member_id| {
            if (member_id == vector_id) {
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }
}

test "insert tolerates loaded leaf with empty stored centroid" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2 });
    defer idx.close();

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();

        const members = try alloc.dupe(u64, &[_]u64{1});
        defer alloc.free(members);

        const root = Node{
            .id = idx.metadata.root_node,
            .is_leaf = true,
            .level = 0,
            .parent = 0,
            .centroid = &.{},
            .children = &.{},
            .members = members,
        };
        try idx.saveNode(&txn, &root);
        try idx.putVector(&txn, 1, &[_]f32{ 1.0, 0.0 });
        try idx.putVecLeaf(&txn, 1, idx.metadata.root_node);
        idx.metadata.active_count = 1;
        try txn.commit();
    }

    try idx.insert(2, &[_]f32{ 0.0, 1.0 });

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    try std.testing.expectEqual(@as(u64, 2), idx.stats().active_count);
    const leaf1 = (try idx.debugLeafForVector(1)) orelse return error.TestUnexpectedResult;
    const leaf2 = (try idx.debugLeafForVector(2)) orelse return error.TestUnexpectedResult;
    const members1 = try idx.debugLeafMembers(alloc, leaf1);
    defer alloc.free(members1);
    const members2 = if (leaf2 == leaf1) members1 else try idx.debugLeafMembers(alloc, leaf2);
    defer if (leaf2 != leaf1) alloc.free(members2);

    var found1 = false;
    for (members1) |member_id| {
        if (member_id == 1) {
            found1 = true;
            break;
        }
    }
    try std.testing.expect(found1);

    var found2 = false;
    for (members2) |member_id| {
        if (member_id == 2) {
            found2 = true;
            break;
        }
    }
    try std.testing.expect(found2);
}

test "flat centroid search falls back when published leaves have no centroids" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = true,
        .centroid_directory_mode = .flat_rabitq,
        .flat_centroid_block_size = 4,
        .flat_centroid_probe_count = 4,
    });
    defer idx.close();

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();

        const members = try alloc.dupe(u64, &[_]u64{1});
        defer alloc.free(members);

        const root = Node{
            .id = idx.metadata.root_node,
            .is_leaf = true,
            .level = 0,
            .parent = 0,
            .centroid = &.{},
            .children = &.{},
            .members = members,
        };
        try idx.saveNode(&txn, &root);
        try idx.putVector(&txn, 1, &[_]f32{ 1.0, 0.0 });
        try idx.putVecLeaf(&txn, 1, idx.metadata.root_node);
        idx.metadata.active_count = 1;
        try txn.commit();
    }
    idx.refreshPublishedSearchState();

    var results = try idx.search(&[_]f32{ 1.0, 0.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 1), results.getHits()[0].vector_id);
}

test "insert tolerates stale vec_leaf mapping without dropping the member" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
    });
    defer idx.close();

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();

        const members = try alloc.dupe(u64, &[_]u64{41});
        defer alloc.free(members);

        const root = Node{
            .id = idx.metadata.root_node,
            .is_leaf = true,
            .level = 0,
            .parent = 0,
            .centroid = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0 }),
            .children = &.{},
            .members = members,
        };
        defer alloc.free(root.centroid);
        try idx.saveNode(&txn, &root);
        try idx.putVector(&txn, 41, &[_]f32{ 1.0, 2.0 });
        try idx.putMetadata(&txn, 41, "doc:41");
        try idx.putVecLeaf(&txn, 41, idx.metadata.root_node);
        try idx.putVecLeaf(&txn, 42, idx.metadata.root_node);
        idx.metadata.active_count = 1;
        try txn.commit();
    }

    try idx.insertWithMetadata(42, &[_]f32{ 2.0, 3.0 }, "doc:42");

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    try std.testing.expectEqual(@as(u64, idx.metadata.root_node), try idx.getVecLeaf(&txn, 42));
    const members = try idx.debugLeafMembers(alloc, idx.metadata.root_node);
    defer alloc.free(members);
    try std.testing.expectEqual(@as(usize, 2), members.len);
    try std.testing.expect(members[0] == 41 or members[1] == 41);
    try std.testing.expect(members[0] == 42 or members[1] == 42);
}

test "posting maintenance repairs dirty leaf centroid and state" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
    });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 1.0, 0.0 });
    try idx.insert(2, &[_]f32{ 3.0, 0.0 });

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try root.ensureUnbacked(alloc);
        @memset(root.centroid, 0);
        root.posting_state.noteMembersChanged(root.members.len);

        var key_buf: [12]u8 = undefined;
        try idx.putNamespaced(&txn, .nodes, encodeNodeKey(&key_buf, root.id, .centroid), std.mem.sliceAsBytes(root.centroid));
        try PostingStore.saveState(&idx, &txn, root.id, root.posting_state);
        try idx.finishWriteTxn(&txn);
        idx.invalidateNodeCache(root.id);
    }

    idx.resetWriteProfile();
    const result = try idx.repairDirtyPostings();
    try std.testing.expectEqual(@as(u64, 1), result.dirty_postings);
    try std.testing.expectEqual(@as(u64, 1), result.repaired_postings);
    try std.testing.expectEqual(@as(u64, 1), result.centroid_refreshed);
    try std.testing.expectEqual(@as(u64, 1), result.payload_refreshed);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    var root = try idx.loadNode(&txn, idx.metadata.root_node);
    defer root.deinit(alloc);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), root.centroid[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), root.centroid[1], 0.0001);
    try std.testing.expect(!root.posting_state.dirty);
    try std.testing.expect(!root.posting_state.centroid_dirty);
    try std.testing.expect(!root.posting_state.payload_dirty);

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 1), profile.posting_maintenance_repaired_postings);
    try std.testing.expectEqual(@as(u64, 1), profile.posting_maintenance_centroid_refreshed);
}

test "lazy posting maintenance defers foreground centroid refresh" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
        .lazy_posting_maintenance = true,
    });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 1.0, 0.0 });
    idx.resetWriteProfile();
    try idx.insert(2, &[_]f32{ 3.0, 0.0 });

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), root.centroid[0], 0.0001);
        try std.testing.expect(root.posting_state.dirty);
        try std.testing.expect(root.posting_state.centroid_dirty);
        try std.testing.expect(!root.posting_state.payload_dirty);
    }

    const profile = idx.getWriteProfile();
    try std.testing.expect(profile.posting_lazy_centroid_deferrals > 0);

    const result = try idx.repairDirtyPostings();
    try std.testing.expectEqual(@as(u64, 1), result.centroid_refreshed);

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expectApproxEqAbs(@as(f32, 2.0), root.centroid[0], 0.0001);
        try std.testing.expect(!root.posting_state.dirty);
        try std.testing.expect(!root.posting_state.centroid_dirty);
    }
}

test "posting backlog stats report lazy dirty leaves with std Io writer" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
        .lazy_posting_maintenance = true,
    });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 1.0, 0.0 });
    try idx.insert(2, &[_]f32{ 3.0, 0.0 });

    const stats = try idx.postingBacklogStats();
    try std.testing.expect(stats.needsRepair());
    try std.testing.expectEqual(@as(u64, 1), stats.dirty_postings);
    try std.testing.expectEqual(@as(u64, 1), stats.centroid_dirty_postings);
    try std.testing.expectEqual(@as(u64, 0), stats.payload_dirty_postings);
    try std.testing.expectEqual(@as(u64, 1), stats.max_centroid_version_lag);

    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try idx.writePostingBacklogStats(&out.writer);
    const rendered = out.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rendered, "dirty_postings=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "centroid_dirty_postings=1") != null);
}

test "dirty quantized posting payloads are scored exactly" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 10,
        .use_quantization = true,
        .rerank_policy = .never,
    });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 1.0, 0.0 });
    try idx.insert(2, &[_]f32{ 3.0, 0.0 });

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        root.posting_state.noteMembersChanged(root.members.len);
        root.posting_state.centroid_dirty = false;
        root.posting_state.centroid_version = root.posting_state.mutation_version;
        root.posting_state.dirty = root.posting_state.payload_dirty;
        try PostingStore.saveState(&idx, &txn, root.id, root.posting_state);
        try idx.finishWriteTxn(&txn);
        idx.invalidateNodeCache(root.id);
    }

    var profiled = try idx.searchProfiled(&[_]f32{ 1.0, 0.0 }, 2);
    defer profiled.results.deinit();

    try std.testing.expectEqual(@as(usize, 2), profiled.results.getHits().len);
    try std.testing.expectEqual(@as(u64, 0), profiled.profile.approx_vectors_scored);
    try std.testing.expectEqual(@as(u64, 2), profiled.profile.exact_vectors_scored);
}

test "auto posting maintenance repairs bounded lazy backlog before commit" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
        .lazy_posting_maintenance = true,
        .auto_posting_maintenance_max_postings = 1,
    });
    defer idx.close();

    try idx.insert(1, &[_]f32{ 1.0, 0.0 });
    idx.resetWriteProfile();
    try idx.insert(2, &[_]f32{ 3.0, 0.0 });

    const stats = try idx.postingBacklogStats();
    try std.testing.expect(!stats.needsRepair());
    try std.testing.expectEqual(@as(u64, 0), stats.dirty_postings);

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expectApproxEqAbs(@as(f32, 2.0), root.centroid[0], 0.0001);
        try std.testing.expect(!root.posting_state.dirty);
        try std.testing.expect(!root.posting_state.centroid_dirty);
    }

    const profile = idx.getWriteProfile();
    try std.testing.expect(profile.posting_lazy_centroid_deferrals > 0);
    try std.testing.expect(profile.posting_maintenance_repaired_postings > 0);
}

test "manual posting repair honors explicit bound when auto repair is configured" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = false,
    });
    defer idx.close();

    for (0..8) |i| {
        const vector_id: u64 = @intCast(i + 1);
        const x: f32 = if (i < 4) @floatFromInt(i) else @floatFromInt(i + 20);
        try idx.insert(vector_id, &[_]f32{ x, 0.0 });
    }

    var dirtied: u64 = 0;
    var dirtied_leaf_ids: [2]u64 = .{ 0, 0 };
    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        var node_id: u64 = 1;
        while (node_id <= idx.metadata.node_count and dirtied < 2) : (node_id += 1) {
            var node = idx.loadNode(&txn, node_id) catch continue;
            defer node.deinit(alloc);
            if (!node.is_leaf or node.members.len == 0) continue;
            node.posting_state.noteMembersChanged(node.members.len);
            try PostingStore.saveState(&idx, &txn, node.id, node.posting_state);
            dirtied_leaf_ids[@intCast(dirtied)] = node.id;
            dirtied += 1;
        }
        try idx.finishWriteTxn(&txn);
    }
    try std.testing.expectEqual(@as(u64, 2), dirtied);
    for (dirtied_leaf_ids) |leaf_id| idx.invalidateNodeCache(leaf_id);

    const before = try idx.postingBacklogStats();
    try std.testing.expectEqual(@as(u64, 2), before.dirty_postings);

    idx.config.auto_posting_maintenance_max_postings = 100;
    idx.resetWriteProfile();
    const repaired = try idx.repairDirtyPostingsWithOptions(.{ .max_postings = 1 });
    try std.testing.expectEqual(@as(u64, 1), repaired.repaired_postings);

    const after = try idx.postingBacklogStats();
    try std.testing.expectEqual(before.dirty_postings - 1, after.dirty_postings);
    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 1), profile.posting_maintenance_repaired_postings);
}

test "posting dirty state survives reopen and bounded repair makes progress" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .use_quantization = false,
            .lazy_posting_maintenance = true,
        });
        defer idx.close();

        try idx.insert(1, &[_]f32{ 1.0, 0.0 });
        try idx.insert(2, &[_]f32{ 3.0, 0.0 });
        const stats = try idx.postingBacklogStats();
        try std.testing.expectEqual(@as(u64, 1), stats.dirty_postings);
    }

    {
        var idx = try HBCIndex.open(alloc, path, .{
            .dims = 2,
            .use_quantization = false,
            .lazy_posting_maintenance = true,
        });
        defer idx.close();

        const reopened_stats = try idx.postingBacklogStats();
        try std.testing.expectEqual(@as(u64, 1), reopened_stats.dirty_postings);

        const repaired = try idx.repairDirtyPostingsWithOptions(.{ .max_postings = 1 });
        try std.testing.expectEqual(@as(u64, 1), repaired.repaired_postings);
        const clean_stats = try idx.postingBacklogStats();
        try std.testing.expectEqual(@as(u64, 0), clean_stats.dirty_postings);
    }
}

test "lazy posting maintenance keeps assignment map and members consistent through dynamic writes" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 3,
        .branching_factor = 2,
        .search_width = 16,
        .use_quantization = true,
        .lazy_posting_maintenance = true,
        .auto_posting_maintenance_max_postings = 2,
    });
    defer idx.close();

    var present = std.AutoHashMap(u64, void).init(alloc);
    defer present.deinit();

    for (0..24) |i| {
        const vector_id: u64 = @intCast(i + 1);
        const cluster: f32 = if (i % 2 == 0) 0.0 else 100.0;
        const x: f32 = cluster + @as(f32, @floatFromInt(i / 2));
        const y: f32 = @floatFromInt(i % 3);
        try idx.insert(vector_id, &[_]f32{ x, y });
        try present.put(vector_id, {});
    }

    for (&[_]u64{ 2, 5, 9, 12, 17 }) |vector_id| {
        try idx.delete(vector_id);
        _ = present.remove(vector_id);
    }

    for (&[_]u64{ 3, 7, 21 }) |vector_id| {
        try idx.insert(vector_id, &[_]f32{ @as(f32, @floatFromInt(vector_id)) + 0.5, 7.0 });
        try present.put(vector_id, {});
    }

    var it = present.keyIterator();
    while (it.next()) |vector_id_ptr| {
        const vector_id = vector_id_ptr.*;
        const mapped_leaf = try idx.debugLeafForVector(vector_id);
        const scanned_leaf = try idx.debugScanLeafForVector(vector_id);
        try std.testing.expect(mapped_leaf != null);
        try std.testing.expectEqual(scanned_leaf, mapped_leaf);

        const members = try idx.debugLeafMembers(alloc, mapped_leaf.?);
        defer alloc.free(members);
        var found = false;
        for (members) |member_id| {
            if (member_id == vector_id) {
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }

    var results = try idx.search(&[_]f32{ 1.0, 0.0 }, 5);
    defer results.deinit();
    try std.testing.expect(results.getHits().len > 0);
}

test "posting maintenance can split and merge postings as bounded layout work" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 100,
        .branching_factor = 4,
        .search_width = 8,
        .use_quantization = false,
        .lazy_posting_maintenance = true,
    });
    defer idx.close();

    for (0..8) |i| {
        const vector_id: u64 = @intCast(i + 1);
        const x: f32 = if (i < 4) @floatFromInt(i) else 100.0 + @as(f32, @floatFromInt(i));
        try idx.insert(vector_id, &[_]f32{ x, 0.0 });
    }

    idx.config.leaf_size = 4;
    const split_result = try idx.repairDirtyPostingsWithOptions(.{
        .max_postings = 8,
        .rebalance_layout = true,
        .max_layout_changes = 1,
        .max_boundary_reassignments = 8,
    });
    try std.testing.expectEqual(@as(u64, 1), split_result.split_postings);
    try std.testing.expect(idx.stats().node_count >= 3);

    for (1..9) |vector_id_usize| {
        const vector_id: u64 = @intCast(vector_id_usize);
        try std.testing.expectEqual(try idx.debugScanLeafForVector(vector_id), try idx.debugLeafForVector(vector_id));
    }

    idx.config.leaf_size = 100;
    const merge_result = try idx.repairDirtyPostingsWithOptions(.{
        .max_postings = 8,
        .rebalance_layout = true,
        .max_layout_changes = 1,
    });
    try std.testing.expectEqual(@as(u64, 1), merge_result.merged_postings);

    for (1..9) |vector_id_usize| {
        const vector_id: u64 = @intCast(vector_id_usize);
        try std.testing.expectEqual(try idx.debugScanLeafForVector(vector_id), try idx.debugLeafForVector(vector_id));
    }

    var results = try idx.search(&[_]f32{ 102.0, 0.0 }, 4);
    defer results.deinit();
    try std.testing.expect(results.getHits().len > 0);
}

test "bulk replay recomputes same-leaf existing members once per leaf" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const LoaderCtx = struct {
        load_calls: usize = 0,

        fn load(ctx_ptr: *anyopaque, loader_alloc: Allocator, vector_id: u64, _: []const u8) ![]f32 {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            ctx.load_calls += 1;
            const vector: []const f32 = switch (vector_id) {
                1 => &[_]f32{ 0.0, 0.0 },
                2 => &[_]f32{ 1.0, 0.0 },
                else => return error.NotFound,
            };
            return try loader_alloc.dupe(f32, vector);
        }
    };

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .use_quantization = false,
    });
    defer idx.close();

    var loader_ctx = LoaderCtx{};
    idx.setExternalVectorLoader(&loader_ctx, LoaderCtx.load);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc:2" },
    };
    const options: BatchInsertOptions = .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
    };

    try idx.beginBulkIngestSession();
    var initial_session_open = true;
    errdefer if (initial_session_open) idx.abortBulkIngestSession();
    try idx.batchInsertWithMetadataOptions(&items, options);
    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    initial_session_open = false;

    loader_ctx.load_calls = 0;
    idx.resetWriteProfile();

    try idx.beginBulkIngestSession();
    var replay_session_open = true;
    errdefer if (replay_session_open) idx.abortBulkIngestSession();
    try idx.batchApplyOptions(&items, &.{}, .{
        .assume_absent_ids = false,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = true,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
    });
    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    replay_session_open = false;

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 2), profile.insert_calls);
    try std.testing.expectEqual(@as(u64, 1), profile.save_node_calls);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    const members = try idx.debugLeafMembers(alloc, idx.metadata.root_node);
    defer alloc.free(members);
    try std.testing.expectEqual(@as(usize, 2), members.len);
}

test "atomic replacements do not take the grouped new-id routing path" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .use_quantization = false,
    });
    defer idx.close();

    const initial = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.0, 1.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &[_]f32{ -1.0, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &[_]f32{ 0.0, -1.0 }, .metadata = "doc:4" },
    };
    try idx.batchInsertWithMetadataOptions(&initial, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
    });

    idx.resetWriteProfile();
    const replacements = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.8, 0.2 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 0.2, 0.8 }, .metadata = "doc:2" },
    };
    try idx.batchApplyOptions(&replacements, &.{ 1, 2 }, .{
        .assume_absent_ids = false,
        .coalesce_leaf_writes = true,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 0), profile.grouped_items);
    try std.testing.expectEqual(@as(u64, 4), idx.stats().active_count);
}

test "deferred quantized rebuild uses current batch vectors before external loader" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const LoaderCtx = struct {
        load_calls: usize = 0,

        fn load(ctx_ptr: *anyopaque, loader_alloc: Allocator, vector_id: u64, _: []const u8) ![]f32 {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            ctx.load_calls += 1;
            const vector: []const f32 = switch (vector_id) {
                1 => &[_]f32{ 0.0, 0.0 },
                2 => &[_]f32{ 1.0, 0.0 },
                3 => &[_]f32{ 0.0, 1.0 },
                else => return error.NotFound,
            };
            return try loader_alloc.dupe(f32, vector);
        }
    };

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .branching_factor = 2,
        .use_quantization = true,
    });
    defer idx.close();

    var loader_ctx = LoaderCtx{};
    idx.setExternalVectorLoader(&loader_ctx, LoaderCtx.load);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &[_]f32{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &[_]f32{ 1.0, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &[_]f32{ 0.0, 1.0 }, .metadata = "doc:3" },
    };

    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = true,
        .bulk_ingest = true,
    });

    try std.testing.expectEqual(@as(usize, 0), loader_ctx.load_calls);
    const profile = idx.getWriteProfile();
    try std.testing.expect(profile.refresh_quantized_ns > 0);
    try std.testing.expect(profile.quantized_vector_load_ns > 0);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    _ = try idx.getQuantized(&txn, idx.metadata.root_node, true, items.len);
}

test "vector storage roundtrip" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 3 });
    defer idx.close();

    // Insert stores vector
    try idx.insert(42, &[_]f32{ 1.0, 2.0, 3.0 });

    // Read it back
    var txn = try idx.beginReadTxn();
    defer txn.abort();
    const v = try idx.getVector(&txn, 42);
    defer alloc.free(v);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1.0, 2.0, 3.0 }, v);
}

test "getVectorScratch caches external vector loads" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const LoaderCtx = struct {
        load_calls: usize = 0,

        fn load(ctx_ptr: *anyopaque, loader_alloc: Allocator, vector_id: u64, _: []const u8) ![]f32 {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            ctx.load_calls += 1;
            const vector: []const f32 = switch (vector_id) {
                7 => &[_]f32{ 7.0, 8.0, 9.0 },
                else => return error.NotFound,
            };
            return try loader_alloc.dupe(f32, vector);
        }
    };

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 3,
        .max_cached_vectors = 8,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    var loader_ctx = LoaderCtx{};
    idx.setExternalVectorLoader(&loader_ctx, LoaderCtx.load);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 7, .vector = &[_]f32{ 7.0, 8.0, 9.0 }, .metadata = "doc:7" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .skip_vector_store = true,
    });
    idx.setCacheEnabled(false);
    idx.setCacheEnabled(true);

    var txn = try idx.beginReadTxn();
    defer txn.abort();

    var scratch_a: [3]f32 = undefined;
    var scratch_b: [3]f32 = undefined;

    const first = try idx.getVectorScratch(&txn, 7, &scratch_a);
    const second = try idx.getVectorScratch(&txn, 7, &scratch_b);

    try std.testing.expectEqual(@as(usize, 1), loader_ctx.load_calls);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, first);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, second);
}

test "getVectorScratch can bypass external vector cache during replay sessions" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const LoaderCtx = struct {
        load_calls: usize = 0,

        fn load(ctx_ptr: *anyopaque, loader_alloc: Allocator, vector_id: u64, _: []const u8) ![]f32 {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            ctx.load_calls += 1;
            const vector: []const f32 = switch (vector_id) {
                7 => &[_]f32{ 7.0, 8.0, 9.0 },
                else => return error.NotFound,
            };
            return try loader_alloc.dupe(f32, vector);
        }
    };

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 3,
        .max_cached_vectors = 8,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    var loader_ctx = LoaderCtx{};
    idx.setExternalVectorLoader(&loader_ctx, LoaderCtx.load);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 7, .vector = &[_]f32{ 7.0, 8.0, 9.0 }, .metadata = "doc:7" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .skip_vector_store = true,
    });
    idx.setCacheEnabled(false);
    idx.setCacheEnabled(true);
    idx.setBypassExternalVectorCache(true);
    defer idx.setBypassExternalVectorCache(false);

    var txn = try idx.beginReadTxn();
    defer txn.abort();

    var scratch_a: [3]f32 = undefined;
    var scratch_b: [3]f32 = undefined;

    const first = try idx.getVectorScratch(&txn, 7, &scratch_a);
    const second = try idx.getVectorScratch(&txn, 7, &scratch_b);

    try std.testing.expectEqual(@as(usize, 2), loader_ctx.load_calls);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, first);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, second);
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);
}

test "skip vector store writes do not seed retained vector cache when bypassed" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 3,
        .max_cached_vectors = 8,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    idx.setBypassExternalVectorCache(true);
    defer idx.setBypassExternalVectorCache(false);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 7, .vector = &[_]f32{ 7.0, 8.0, 9.0 }, .metadata = "doc:7" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .skip_vector_store = true,
    });

    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(7));
}

test "getVectorInto skips external vector cache population during concurrent search" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const LoaderCtx = struct {
        load_calls: usize = 0,

        fn load(ctx_ptr: *anyopaque, loader_alloc: Allocator, vector_id: u64, _: []const u8) ![]f32 {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            ctx.load_calls += 1;
            const vector: []const f32 = switch (vector_id) {
                7 => &[_]f32{ 7.0, 8.0, 9.0 },
                else => return error.NotFound,
            };
            return try loader_alloc.dupe(f32, vector);
        }
    };

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 3,
        .max_cached_vectors = 8,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    var loader_ctx = LoaderCtx{};
    idx.setExternalVectorLoader(&loader_ctx, LoaderCtx.load);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 7, .vector = &[_]f32{ 7.0, 8.0, 9.0 }, .metadata = "doc:7" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .skip_vector_store = true,
    });
    idx.setCacheEnabled(false);
    idx.setCacheEnabled(true);
    idx.active_searches.store(2, .release);
    defer idx.active_searches.store(0, .release);

    var txn = try idx.beginReadTxn();
    defer txn.abort();

    var scratch_a: [3]f32 = undefined;
    var scratch_b: [3]f32 = undefined;

    const first = try idx.getVectorInto(&txn, 7, &scratch_a);
    const second = try idx.getVectorInto(&txn, 7, &scratch_b);

    try std.testing.expectEqual(@as(usize, 2), loader_ctx.load_calls);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, first);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, second);
    try std.testing.expect(first.ptr == scratch_a[0..].ptr);
    try std.testing.expect(second.ptr == scratch_b[0..].ptr);
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);
}

test "updating an existing vector in the same leaf uses centroid delta instead of full recompute" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 3 });
    defer idx.close();

    try idx.insertWithMetadata(7, &[_]f32{ 1.0, 2.0, 3.0 }, "doc:7");
    const before = idx.getWriteProfile();

    try idx.insertWithMetadata(7, &[_]f32{ 3.0, 2.0, 1.0 }, "doc:7");
    const after = idx.getWriteProfile();

    try std.testing.expectEqual(before.centroid_recompute_calls, after.centroid_recompute_calls);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    const stored = try idx.getVector(&txn, 7);
    defer alloc.free(stored);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 3.0, 2.0, 1.0 }, stored);
}

test "reinserting the same vector without metadata is a no-op" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 3 });
    defer idx.close();

    try idx.insert(7, &[_]f32{ 1.0, 2.0, 3.0 });
    idx.resetWriteProfile();

    try idx.insert(7, &[_]f32{ 1.0, 2.0, 3.0 });
    const after = idx.getWriteProfile();

    try std.testing.expectEqual(@as(u64, 1), after.noop_existing_skips);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    const stored = try idx.getVector(&txn, 7);
    defer alloc.free(stored);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1.0, 2.0, 3.0 }, stored);
}

test "bulk ingest existing vector update can stay on existing leaf without reroute" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 3 });
    defer idx.close();

    try idx.insertWithMetadata(7, &[_]f32{ 1.0, 2.0, 3.0 }, "doc:7");
    idx.resetWriteProfile();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 7, .vector = &[_]f32{ 3.0, 2.0, 1.0 }, .metadata = "doc:7" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .bulk_ingest = true,
        .centroid_only_routing = true,
        .skip_vector_store = false,
    });

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u64, 0), profile.insert_find_leaf_ns);
    try std.testing.expectEqual(@as(u64, 1), idx.stats().active_count);

    var txn = try idx.beginReadTxn();
    defer txn.abort();
    const stored = try idx.getVector(&txn, 7);
    defer alloc.free(stored);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 3.0, 2.0, 1.0 }, stored);
    try std.testing.expectEqual(@as(u64, idx.metadata.root_node), try idx.getVecLeaf(&txn, 7));
}

test "hbc namespace adapters expose multi-partition txn operations" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 3 });
    defer idx.close();

    const probe_meta_key = "stats:probe";

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        var key_buf: [10]u8 = undefined;
        try txn.put(.meta, probe_meta_key, "ok");
        try txn.put(.vecs, encodeVecMetaKey(&key_buf, 42), "meta42");
        try txn.commit();
    }

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqualStrings("ok", try txn.get(.meta, probe_meta_key));

        var key_buf: [10]u8 = undefined;
        try std.testing.expectEqualStrings("meta42", try txn.get(.vecs, encodeVecMetaKey(&key_buf, 42)));
    }

    {
        var batch = try idx.beginBatchTxn();
        errdefer batch.abort();
        var key_buf: [10]u8 = undefined;
        try batch.put(.vecs, encodeVecMetaKey(&key_buf, 77), "meta77");
        try std.testing.expectEqualStrings("meta77", try batch.get(.vecs, encodeVecMetaKey(&key_buf, 77)));
        try batch.commit();
    }
}

test "hbc backend runtime erases namespace store handles" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 3 });
    defer idx.close();

    var runtime = try idx.runtimeNamespaceStore(std.testing.allocator);
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put(.meta, "stats:runtime", "ok");
        try txn.put(.vecs, "custom:vec", "v");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("ok", try txn.get(.meta, "stats:runtime"));
        try std.testing.expectEqualStrings("v", try txn.get(.vecs, "custom:vec"));
    }
}

test "hbc core persistence helpers work through erased namespace txns" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
    });
    defer idx.close();

    var runtime = try idx.runtimeNamespaceStore(std.testing.allocator);
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        errdefer txn.abort();

        var loaded_root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer loaded_root.deinit(alloc);

        var root = try loaded_root.clone(alloc);
        defer root.deinit(alloc);

        if (root.centroid.len > 0) alloc.free(root.centroid);
        if (root.children.len > 0) alloc.free(root.children);
        if (root.members.len > 0) alloc.free(root.members);
        root.centroid = try alloc.dupe(f32, &.{ 1.0, 2.0 });
        root.children = &.{};
        root.members = try alloc.dupe(u64, &.{42});

        try idx.putVector(&txn, 42, &.{ 1.0, 2.0 });
        try idx.putMetadata(&txn, 42, "doc:1");
        try idx.putVecLeaf(&txn, 42, root.id);
        try idx.saveNode(&txn, &root);
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();

        const metadata = (try idx.getMetadataInTxn(&txn, 42)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("doc:1", metadata);
        try std.testing.expectEqual(@as(u64, 1), try idx.getVecLeaf(&txn, 42));

        const vector = try idx.getVector(&txn, 42);
        defer alloc.free(vector);
        try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, vector);

        var root = try idx.loadNode(&txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(root.is_leaf);
        try std.testing.expectEqual(@as(usize, 1), root.members.len);
        try std.testing.expectEqual(@as(u64, 42), root.members[0]);

        const maybe_range = try idx.loadNodeSplitRange(&txn, idx.metadata.root_node);
        try std.testing.expect(maybe_range != null);
        var range = maybe_range.?;
        defer range.deinit(alloc);
        try std.testing.expectEqualStrings("doc:1", range.min_key);
        try std.testing.expectEqualStrings("doc:1", range.max_key);
    }
}

test "hbc runtime namespace store works for lsm backend" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
        .storage_backend = .lsm,
    });
    defer idx.close();

    var runtime = try idx.runtimeNamespaceStore(std.testing.allocator);
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        errdefer txn.abort();

        try idx.putVector(&txn, 7, &.{ 1.0, 3.0 });
        try idx.putMetadata(&txn, 7, "doc:lsm");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();

        const metadata = (try idx.getMetadataInTxn(&txn, 7)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("doc:lsm", metadata);

        const vector = try idx.getVector(&txn, 7);
        defer alloc.free(vector);
        try std.testing.expectEqualSlices(f32, &.{ 1.0, 3.0 }, vector);
    }
}

test "hbc getMetadataManySortedInTxn batches ordered metadata lookups and caches results" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .use_quantization = false,
        .storage_backend = .lsm,
    });
    defer idx.close();

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        try idx.putMetadata(&txn, 2, "doc:2");
        try idx.putMetadata(&txn, 7, "doc:7");
        try txn.commit();
    }

    var read_txn = try idx.beginRuntimeReadTxn();
    defer read_txn.abort();

    const ids = [_]u64{ 7, 5, 2 };
    var out: [ids.len]?[]const u8 = undefined;
    try idx.getMetadataManySortedInTxn(&read_txn, &ids, &out);

    try std.testing.expectEqualStrings("doc:7", out[0].?);
    try std.testing.expectEqual(@as(?[]const u8, null), out[1]);
    try std.testing.expectEqualStrings("doc:2", out[2].?);
    try std.testing.expectEqualStrings("doc:7", idx.getCachedMetadata(7).?);
    try std.testing.expectEqualStrings("doc:2", idx.getCachedMetadata(2).?);
}

test "hbc bulk ingest skips retained metadata cache population" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2 });
    defer idx.close();

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    const input = "doc:1";
    const returned = try idx.cacheMetadata(1, input);
    try std.testing.expectEqual(@intFromPtr(input.ptr), @intFromPtr(returned.ptr));
    try std.testing.expectEqual(@as(?[]const u8, null), idx.getCachedMetadata(1));
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().metadata.used_bytes);

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;
}

test "hbc bulk ingest skips retained vector cache population" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var idx = try HBCIndex.open(alloc, path, .{ .dims = 2 });
    defer idx.close();
    idx.attachResourceManager(&resource_manager);
    idx.setRetainedVectorCacheEnabled(true);

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    const input = [_]f32{ 1.0, 2.0 };
    const returned = try idx.cacheVector(1, &input);
    try std.testing.expectEqual(@intFromPtr(input[0..].ptr), @intFromPtr(returned.ptr));
    try std.testing.expectEqual(@as(?[]const f32, null), idx.getCachedVector(1));
    try std.testing.expectEqual(@as(u64, 0), idx.hbcCacheStats().vector.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.hbc_node_metadata_cache).used_bytes);

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;
}

test "hbc bulk ingest mutation batches defer manifest without direct sorted ingest" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    const before = idx.snapshotLsmWriteStats() orelse return error.TestUnexpectedResult;
    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 1.0, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.0, 1.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 1.0, 1.0 }, .metadata = "doc:4" },
    };
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .bulk_ingest = true,
        .skip_vector_store = true,
    });

    const during = idx.snapshotLsmWriteStats() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(before.manifest_writes, during.manifest_writes);
    try std.testing.expectEqual(before.sorted_ingest_runs, during.sorted_ingest_runs);
    {
        var read_txn = try idx.beginReadTxn();
        defer read_txn.abort();
        idx.clearNodeCache();
        var staged_root = try idx.loadNode(&read_txn, idx.metadata.root_node);
        defer staged_root.deinit(alloc);
        try std.testing.expectEqual(@as(usize, items.len), staged_root.members.len);
    }

    const staged_range = (try idx.getNodeSplitRange(idx.metadata.root_node)) orelse return error.TestUnexpectedResult;
    var staged_range_owned = staged_range;
    defer staged_range_owned.deinit(alloc);
    try std.testing.expectEqualStrings("doc:1", staged_range_owned.min_key);
    try std.testing.expectEqualStrings("doc:4", staged_range_owned.max_key);

    {
        var cold = try HBCIndex.openWithLsmOptions(alloc, path, .{
            .dims = 2,
            .leaf_size = 4,
            .branching_factor = 8,
            .search_width = 8,
            .use_quantization = false,
            .storage_backend = .lsm,
        }, .{
            .backend_options = .{
                .backend = .{
                    .read_only = true,
                    .create_if_missing = false,
                },
                .flush_threshold = 1,
                .bulk_ingest_flush_threshold_multiplier = 4,
            },
        });
        defer cold.close();

        try std.testing.expectEqual(@as(u64, 0), cold.stats().active_count);
        var cold_results = try cold.search(&[_]f32{ 0.5, 0.0 }, 1);
        defer cold_results.deinit();
        try std.testing.expectEqual(@as(usize, 0), cold_results.getHits().len);
    }

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;

    const after = idx.snapshotLsmWriteStats() orelse return error.TestUnexpectedResult;
    try std.testing.expect(after.manifest_writes > before.manifest_writes);
    try std.testing.expectEqual(before.sorted_ingest_runs, after.sorted_ingest_runs);
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_node_keys.count());

    {
        var cold = try HBCIndex.openWithLsmOptions(alloc, path, .{
            .dims = 2,
            .leaf_size = 4,
            .branching_factor = 8,
            .search_width = 8,
            .use_quantization = false,
            .storage_backend = .lsm,
        }, .{
            .backend_options = .{
                .backend = .{
                    .read_only = true,
                    .create_if_missing = false,
                },
                .flush_threshold = 1,
                .bulk_ingest_flush_threshold_multiplier = 4,
            },
        });
        defer cold.close();

        try std.testing.expectEqual(@as(u64, items.len), cold.stats().active_count);
        var cold_txn = try cold.beginReadTxn();
        defer cold_txn.abort();
        var cold_root = try cold.loadNode(&cold_txn, cold.stats().root_node);
        defer cold_root.deinit(alloc);
        try std.testing.expectEqual(@as(usize, items.len), cold_root.members.len);
    }
}

test "hbc reset stored structure preserves an empty query root" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{});
    var idx_open = true;
    defer if (idx_open) idx.close();

    try idx.resetStoredStructure();
    try std.testing.expectEqual(@as(u64, 1), idx.stats().root_node);
    try std.testing.expectEqual(@as(u64, 1), idx.stats().node_count);
    try std.testing.expectEqual(@as(u64, 0), idx.stats().active_count);

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        var root = try idx.loadNode(&txn, idx.stats().root_node);
        defer root.deinit(alloc);
        try std.testing.expect(root.is_leaf);
        try std.testing.expectEqual(@as(usize, 0), root.members.len);
    }

    idx.close();
    idx_open = false;

    var reopened = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{});
    defer reopened.close();

    try std.testing.expectEqual(@as(u64, 1), reopened.stats().root_node);
    var txn = try reopened.beginReadTxn();
    defer txn.abort();
    var root = try reopened.loadNode(&txn, reopened.stats().root_node);
    defer root.deinit(alloc);
    try std.testing.expect(root.is_leaf);
    try std.testing.expectEqual(@as(usize, 0), root.members.len);
}

test "bulk ingest defers oversized root leaf until finish" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 0.3, 0.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 0.4, 0.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 0.5, 0.0 }, .metadata = "doc:6" },
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
    });

    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().split_leaf_calls);
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());

    {
        var read_txn = try idx.beginReadTxn();
        defer read_txn.abort();
        idx.clearNodeCache();
        var staged_root = try idx.loadNode(&read_txn, idx.metadata.root_node);
        defer staged_root.deinit(alloc);
        try std.testing.expect(staged_root.is_leaf);
        try std.testing.expect(staged_root.members.len > idx.config.leaf_size);
    }

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;

    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u64, 1), idx.getWriteProfile().split_leaf_calls);
    try std.testing.expect(idx.stats().node_count > 1);

    var results = try idx.search(&[_]f32{ 0.5, 0.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 6), results.getHits()[0].vector_id);
}

test "bulk ingest uses kway kmeans for large deferred oversized leaf" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 16,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
        .kmeans_backend = .cpu,
        .kmeans_update_strategy = .segmented,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    var seed_items: [5]BatchInsertItem = undefined;
    var seed_vectors: [5][2]f32 = undefined;
    var seed_metadata: [5][8]u8 = undefined;
    for (&seed_items, 0..) |*item, i| {
        seed_vectors[i] = if (i < 4)
            .{ @as(f32, @floatFromInt(i)) * 0.01, 0.0 }
        else
            .{ 100.0, 0.0 };
        const key = try std.fmt.bufPrint(&seed_metadata[i], "doc:{d:0>4}", .{i});
        item.* = .{
            .vector_id = @intCast(i + 1),
            .vector = &seed_vectors[i],
            .metadata = key,
        };
    }
    try idx.batchInsertWithMetadata(&seed_items);
    {
        var read_txn = try idx.beginReadTxn();
        defer read_txn.abort();
        var root = try idx.loadNode(&read_txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(!root.is_leaf);
    }

    var items: [9]BatchInsertItem = undefined;
    var vectors: [9][2]f32 = undefined;
    var metadata: [9][8]u8 = undefined;
    for (&items, 0..) |*item, i| {
        vectors[i] = .{ 100.0 + @as(f32, @floatFromInt(i + 1)) * 0.01, 0.0 };
        const key = try std.fmt.bufPrint(&metadata[i], "doc:{d:0>4}", .{i + seed_items.len});
        item.* = .{
            .vector_id = @intCast(i + seed_items.len + 1),
            .vector = &vectors[i],
            .metadata = key,
        };
    }

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
    });
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().split_leaf_calls);
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());

    try idx.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .bulk_rebuild_hbc_leaf_min_members = 999,
    });
    session_open = false;

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u64, 1), profile.split_leaf_calls);
    try std.testing.expect(profile.kmeans_assignment_calls > 0);
    try std.testing.expect(idx.stats().node_count > 2);

    var results = try idx.search(&[_]f32{ 100.09, 0.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
}

test "bulk ingest does not persist deferred oversized leaf quantized payloads" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    const seed = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 0.3, 0.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 100.0, 0.0 }, .metadata = "doc:5" },
    };
    try idx.batchInsertWithMetadata(&seed);
    {
        var read_txn = try idx.beginReadTxn();
        defer read_txn.abort();
        var root = try idx.loadNode(&read_txn, idx.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(!root.is_leaf);
    }

    const first = [_]BatchInsertItem{
        .{ .vector_id = 6, .vector = &.{ 100.1, 0.0 }, .metadata = "doc:6" },
        .{ .vector_id = 7, .vector = &.{ 100.2, 0.0 }, .metadata = "doc:7" },
        .{ .vector_id = 8, .vector = &.{ 100.3, 0.0 }, .metadata = "doc:8" },
        .{ .vector_id = 9, .vector = &.{ 100.4, 0.0 }, .metadata = "doc:9" },
    };
    const second = [_]BatchInsertItem{
        .{ .vector_id = 10, .vector = &.{ 100.5, 0.0 }, .metadata = "doc:10" },
    };

    const options: BatchInsertOptions = .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&first, options);
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_quantized_nodes.count());
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().ns_quant_put_calls);
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().ns_quant_value_bytes);

    try idx.batchInsertWithMetadataOptions(&second, options);
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_quantized_nodes.count());
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().ns_quant_put_calls);
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().ns_quant_value_bytes);

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;

    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_quantized_nodes.count());
    try std.testing.expect(idx.getWriteProfile().ns_quant_put_calls > 0);

    var results = try idx.search(&[_]f32{ 100.5, 0.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 10), results.getHits()[0].vector_id);
}

test "bulk split workspace reuses transformed external vectors and reports apply memory" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    const LoaderCtx = struct {
        calls: usize = 0,
        loaded: usize = 0,

        fn load(
            ctx_ptr: *anyopaque,
            vector_ids: []const u64,
            metadata: []const ?[]const u8,
            matrix_positions: []const usize,
            matrix: []f32,
            scratch: []f32,
            dims: usize,
            index: *HBCIndex,
            transform: HBCIndex.ExternalVectorTransformFn,
        ) !void {
            _ = metadata;
            _ = scratch;
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            ctx.calls += 1;
            ctx.loaded += vector_ids.len;
            for (vector_ids, matrix_positions) |vector_id, matrix_position| {
                const offset = matrix_position * dims;
                var original = [_]f32{
                    @as(f32, @floatFromInt(vector_id)),
                    @as(f32, @floatFromInt(vector_id + 100)),
                };
                _ = transform(index, original[0..], matrix[offset .. offset + dims]);
            }
        }
    };

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{});
    defer idx.close();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    idx.attachResourceManager(&resource_manager);

    var loader_ctx = LoaderCtx{};
    idx.setExternalVectorBatchTransformedMatrixLoader(&loader_ctx, LoaderCtx.load);

    var txn = try idx.beginWriteTxn();
    defer txn.abort();
    for ([_]u64{ 1, 2, 3 }) |vector_id| {
        var metadata_buf: [16]u8 = undefined;
        const metadata = try std.fmt.bufPrint(&metadata_buf, "doc:{d}", .{vector_id});
        try idx.putMetadata(&txn, vector_id, metadata);
    }

    const ids = [_]u64{ 1, 2, 3 };
    const positions = [_]usize{ 0, 1, 2 };
    var matrix: [6]f32 = .{0} ** 6;
    var matrix_again: [6]f32 = .{0} ** 6;
    var lookups: [ids.len]FixedKeyLookup = undefined;
    var key_views: [ids.len][]const u8 = undefined;
    var values: [ids.len]?[]const u8 = undefined;
    var scratch: [2]f32 = undefined;

    const baseline_apply_bytes = resource_manager.sliceStats(.dense_apply_working_set).used_bytes;
    idx.beginBulkSplitVectorWorkspace();
    var workspace_active = true;
    defer if (workspace_active) idx.endBulkSplitVectorWorkspace();

    try std.testing.expect(try idx.loadExternalVectorsTransformedIntoMatrix(
        &txn,
        &ids,
        &positions,
        &matrix,
        &lookups,
        &key_views,
        &values,
        &scratch,
    ));
    try std.testing.expectEqual(@as(usize, 1), loader_ctx.calls);
    try std.testing.expectEqual(@as(usize, ids.len), loader_ctx.loaded);
    try std.testing.expect(resource_manager.sliceStats(.dense_apply_working_set).used_bytes > baseline_apply_bytes);

    try std.testing.expect(try idx.loadExternalVectorsTransformedIntoMatrix(
        &txn,
        &ids,
        &positions,
        &matrix_again,
        &lookups,
        &key_views,
        &values,
        &scratch,
    ));
    try std.testing.expectEqual(@as(usize, 1), loader_ctx.calls);
    try std.testing.expectEqualSlices(f32, &matrix, &matrix_again);

    idx.endBulkSplitVectorWorkspace();
    workspace_active = false;
    try std.testing.expectEqual(baseline_apply_bytes, resource_manager.sliceStats(.dense_apply_working_set).used_bytes);
}

test "streaming split vector workspace survives retirement and ring churn" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    }, .{});
    defer idx.close();

    try idx.beginStreamingReplaySession();
    errdefer idx.abortStreamingReplaySession();
    for (1..65) |raw_id| {
        const vector_id: u64 = @intCast(raw_id);
        const transformed = [_]f32{ @floatFromInt(raw_id), @floatFromInt(raw_id + 1) };
        idx.bulkSplitVectorWorkspaceAdmit(vector_id, &transformed);
        if (raw_id % 3 == 0) idx.noteBulkSplitLeafPayloadCoverage(&.{vector_id});
        try std.testing.expect(idx.bulk_split_vector_workspace.map.count() <= idx.bulk_split_vector_workspace.ids.items.len);
        try std.testing.expect(idx.bulk_split_vector_workspace.ids.items.len <= 8);
        try std.testing.expect(idx.bulk_split_vector_workspace.map.capacity() <= 32);
        try std.testing.expectEqual(
            idx.bulk_split_vector_workspace.map.count(),
            idx.bulk_split_vector_workspace.ids.items.len,
        );
        try std.testing.expectEqual(
            idx.bulk_split_vector_workspace.bytes(),
            idx.bulk_split_vector_workspace.accounted_bytes,
        );
    }
    try idx.finishStreamingReplaySessionWithOptions(.{});

    // Retained lookup entries must still point at the ring slot that owns the
    // same id; tombstone churn must never manufacture aliases or underflow the
    // hash table's capacity accounting.
    var entries = idx.bulk_split_vector_workspace.map.iterator();
    while (entries.next()) |entry| {
        const slot = entry.value_ptr.* / idx.config.dims;
        try std.testing.expect(slot < idx.bulk_split_vector_workspace.ids.items.len);
        try std.testing.expectEqual(entry.key_ptr.*, idx.bulk_split_vector_workspace.ids.items[slot]);
    }
}

test "bulk ingest deferred leaf splits publish in bounded windows" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
        .prefer_key_local_leaf_splits = true,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    const seed_items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 100.0, 0.0 }, .metadata = "doc:3" },
    };
    try idx.batchInsertWithMetadata(&seed_items);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 4, .vector = &.{ 100.1, 0.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 100.2, 0.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 100.3, 0.0 }, .metadata = "doc:6" },
        .{ .vector_id = 7, .vector = &.{ 100.4, 0.0 }, .metadata = "doc:7" },
        .{ .vector_id = 8, .vector = &.{ 100.5, 0.0 }, .metadata = "doc:8" },
        .{ .vector_id = 9, .vector = &.{ 100.6, 0.0 }, .metadata = "doc:9" },
        .{ .vector_id = 10, .vector = &.{ 100.7, 0.0 }, .metadata = "doc:10" },
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
    });
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());

    try idx.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .max_deferred_hbc_leaf_splits_per_publish = 1,
        .bulk_rebuild_hbc_leaf_min_members = 999,
    });
    session_open = false;

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expect(profile.deferred_leaf_split_publish_windows > 1);
    try std.testing.expectEqual(@as(u64, 1), profile.deferred_leaf_split_window_max_steps);
    try std.testing.expectEqual(profile.split_leaf_calls, profile.deferred_leaf_split_steps);
}

test "bulk ingest deferred leaf split member budget bounds publish windows" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 0.3, 0.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 0.4, 0.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 0.5, 0.0 }, .metadata = "doc:6" },
        .{ .vector_id = 7, .vector = &.{ 0.6, 0.0 }, .metadata = "doc:7" },
        .{ .vector_id = 8, .vector = &.{ 0.7, 0.0 }, .metadata = "doc:8" },
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
    });
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());

    try idx.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .max_deferred_hbc_leaf_splits_per_publish = 999,
        .max_deferred_hbc_leaf_split_members_per_publish = 3,
        .bulk_rebuild_hbc_leaf_min_members = 999,
    });
    session_open = false;

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expect(profile.deferred_leaf_split_publish_windows > 1);
    try std.testing.expectEqual(@as(u64, 1), profile.deferred_leaf_split_window_max_steps);
    try std.testing.expectEqual(profile.split_leaf_calls, profile.deferred_leaf_split_steps);
}

test "bulk ingest deferred leaf split quantized publish reuses split vectors" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 10.0, 10.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 10.1, 10.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 10.2, 10.0 }, .metadata = "doc:6" },
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .defer_quantized_rebuild_to_bulk_finish = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
    });
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u64, 0), idx.getWriteProfile().ns_quant_put_calls);

    try idx.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .bulk_rebuild_hbc_leaf_min_members = 999,
    });
    session_open = false;

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_quantized_nodes.count());
    try std.testing.expect(profile.split_leaf_vector_load_ns > 0);
    try std.testing.expectEqual(@as(u64, 0), profile.quantized_leaf_vector_load_ns);
    try std.testing.expect(profile.ns_quant_put_calls > 0);

    var results = try idx.search(&[_]f32{ 10.1, 10.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
}

test "append-only deferred leaf split bounds source fallback with native delta" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    }, .{});
    defer idx.close();

    const seed = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 10.0, 10.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 10.1, 10.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 10.2, 10.0 }, .metadata = "doc:6" },
    };
    try idx.batchInsertWithMetadata(&seed);

    const HookCtx = struct {
        loads: usize = 0,

        fn onVectorLoad(ctx_ptr: ?*anyopaque, _: *HBCIndex, _: u64) void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr.?));
            ctx.loads += 1;
        }
    };
    var hook_ctx: HookCtx = .{};
    test_get_vector_view_or_scratch_ctx = &hook_ctx;
    test_get_vector_view_or_scratch_hook = HookCtx.onVectorLoad;
    defer {
        test_get_vector_view_or_scratch_ctx = null;
        test_get_vector_view_or_scratch_hook = null;
    }

    try idx.beginStreamingReplaySession();
    errdefer idx.abortStreamingReplaySession();
    try idx.batchInsertWithMetadataOptions(&.{.{
        .vector_id = 7,
        .vector = &.{ 10.3, 10.0 },
        .metadata = "doc:7",
    }}, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .bulk_ingest = true,
        .defer_leaf_splits_to_batch_finish = true,
    });
    try idx.finishStreamingReplaySessionWithOptions(.{});
    // The seed batch may leave an unrelated deferred split; measure the
    // cross-session append path independently of that setup cleanup.
    hook_ctx.loads = 0;
    try idx.beginStreamingReplaySession();

    const appended = [_]BatchInsertItem{
        .{ .vector_id = 8, .vector = &.{ 10.4, 10.0 }, .metadata = "doc:8" },
        .{ .vector_id = 9, .vector = &.{ 10.5, 10.0 }, .metadata = "doc:9" },
        .{ .vector_id = 10, .vector = &.{ 10.6, 10.0 }, .metadata = "doc:10" },
    };
    try idx.batchInsertWithMetadataOptions(&appended, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .bulk_ingest = true,
        .defer_leaf_splits_to_batch_finish = true,
    });
    try idx.finishStreamingReplaySessionWithOptions(.{});

    // A RaBit base cannot reconstruct exact structural vectors; those prefix
    // members still use the source loader. The rolling native delta must keep
    // the fallback smaller than loading the complete seven-member leaf.
    try std.testing.expect(hook_ctx.loads > 0);
    try std.testing.expect(hook_ctx.loads < 7);
    var results = try idx.search(&[_]f32{ 10.5, 10.0 }, 3);
    defer results.deinit();
    try std.testing.expect(results.getHits().len > 0);
}

test "bulk ingest oversized leaf finish rebuilds local subtree instead of repeated binary splits" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    var seed_items: [5]BatchInsertItem = undefined;
    var seed_vectors: [5][2]f32 = undefined;
    var seed_metadata: [5][8]u8 = undefined;
    for (&seed_items, 0..) |*item, i| {
        seed_vectors[i] = if (i < 4)
            .{ @as(f32, @floatFromInt(i)) * 0.01, 0.0 }
        else
            .{ 100.0, 0.0 };
        const key = try std.fmt.bufPrint(&seed_metadata[i], "doc:{d:0>4}", .{i});
        item.* = .{
            .vector_id = @intCast(i + 1),
            .vector = &seed_vectors[i],
            .metadata = key,
        };
    }
    try idx.batchInsertWithMetadata(&seed_items);

    const item_count = 15;
    var vectors = try alloc.alloc([2]f32, item_count);
    defer alloc.free(vectors);
    const items = try alloc.alloc(BatchInsertItem, item_count);
    defer {
        for (items) |item| alloc.free(@constCast(item.metadata));
        alloc.free(items);
    }
    for (items, 0..) |*item, i| {
        vectors[i] = .{ 100.0 + @as(f32, @floatFromInt(i + 1)) * 0.01, 0.0 };
        item.* = .{
            .vector_id = @intCast(i + seed_items.len + 1),
            .vector = vectors[i][0..],
            .metadata = try std.fmt.allocPrint(alloc, "doc:{d}", .{i + seed_items.len + 1}),
        };
    }

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_bulk_finish = true,
        .bulk_rebuild_leaf_min_members = 8,
    });
    try std.testing.expectEqual(@as(u32, 1), idx.deferred_oversized_leaves.count());

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;

    const profile = idx.getWriteProfile();
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expectEqual(@as(u64, 1), profile.bulk_leaf_rebuild_calls);
    try std.testing.expect(profile.bulk_leaf_rebuild_members_max > item_count);
    try std.testing.expect(idx.stats().node_count > 1);

    var results = try idx.search(vectors[item_count - 1][0..], 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, item_count + seed_items.len), results.getHits()[0].vector_id);
}

test "bulk ingest batch-finish leaf splits normalize before commit" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 0.3, 0.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 0.4, 0.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 0.5, 0.0 }, .metadata = "doc:6" },
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
        .defer_leaf_splits_to_batch_finish = true,
    });

    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expect(idx.getWriteProfile().split_leaf_calls > 0);
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_node_keys.count());

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());

    var results = try idx.search(&[_]f32{ 0.5, 0.0 }, 1);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 1), results.getHits().len);
    try std.testing.expectEqual(@as(u64, 6), results.getHits()[0].vector_id);
}

test "bulk ingest splits oversized leaves incrementally by default" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(alloc, path, .{
        .dims = 2,
        .leaf_size = 4,
        .branching_factor = 8,
        .search_width = 8,
        .use_quantization = false,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        },
    });
    defer idx.close();

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 0.1, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.2, 0.0 }, .metadata = "doc:3" },
        .{ .vector_id = 4, .vector = &.{ 0.3, 0.0 }, .metadata = "doc:4" },
        .{ .vector_id = 5, .vector = &.{ 0.4, 0.0 }, .metadata = "doc:5" },
        .{ .vector_id = 6, .vector = &.{ 0.5, 0.0 }, .metadata = "doc:6" },
    };

    try idx.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) idx.abortBulkIngestSession();

    idx.resetWriteProfile();
    try idx.batchInsertWithMetadataOptions(&items, .{
        .assume_absent_ids = true,
        .coalesce_leaf_writes = true,
        .defer_quantized_rebuild = true,
        .skip_vector_store = false,
        .bulk_ingest = true,
    });

    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
    try std.testing.expect(idx.getWriteProfile().split_leaf_calls > 0);

    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    session_open = false;
    try std.testing.expectEqual(@as(u32, 0), idx.deferred_oversized_leaves.count());
}

test "hbc bulk build skip vector store can delete using live vector cache" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 8,
        .use_quantization = true,
        .storage_backend = .lsm,
    });
    defer idx.close();
    idx.setRetainedVectorCacheEnabled(true);

    const items = [_]BatchInsertItem{
        .{ .vector_id = 1, .vector = &.{ 0.0, 0.0 }, .metadata = "doc:1" },
        .{ .vector_id = 2, .vector = &.{ 1.0, 0.0 }, .metadata = "doc:2" },
        .{ .vector_id = 3, .vector = &.{ 0.0, 1.0 }, .metadata = "doc:3" },
    };
    try idx.bulkBuildWithMetadataOptions(&items, .{ .skip_vector_store = true });
    try std.testing.expectEqual(@as(u64, 3), idx.stats().active_count);

    {
        var txn = try idx.beginRuntimeReadTxn();
        defer txn.abort();
        var key_buf: [10]u8 = undefined;
        const maybe_raw: ?[]const u8 = idx.getNamespaced(&txn, .vecs, encodeVecKey(&key_buf, 2)) catch |err| blk: {
            if (isNotFound(err)) break :blk null;
            return err;
        };
        // The raw vector is intentionally not persisted in HBC's private vector
        // namespace; live mutations use the cache.
        try std.testing.expect(maybe_raw == null);
    }

    try idx.batchApply(&.{}, &.{2});
    try std.testing.expectEqual(@as(u64, 2), idx.stats().active_count);
    try std.testing.expect((try idx.getMetadata(2)) == null);
}

test "hbc routes dense lsm profile options" {
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.openWithLsmOptions(std.testing.allocator, path, .{
        .dims = 3,
        .storage_backend = .lsm,
    }, .{
        .backend_options = .{ .flush_threshold = 123 },
    });
    defer idx.close();

    switch (idx.env_owner) {
        .lsm => |handle| try std.testing.expectEqual(@as(usize, 123), handle.backend.options.flush_threshold),
        else => return error.TestUnexpectedResult,
    }
}

test "large insert and search" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 8,
        .leaf_size = 10,
        .branching_factor = 4,
        .search_width = 16,
    });
    defer idx.close();

    // Insert 100 vectors
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();
    for (0..100) |i| {
        var v: [8]f32 = undefined;
        for (&v) |*x| x.* = random.float(f32) * 10.0;
        try idx.insert(@intCast(i + 1), &v);
    }

    try std.testing.expectEqual(@as(u64, 100), idx.stats().active_count);
    try std.testing.expect(idx.stats().node_count > 1);

    // Search
    var results = try idx.search(&[_]f32{ 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0 }, 10);
    defer results.deinit();
    try std.testing.expectEqual(@as(usize, 10), results.getHits().len);

    // Results should be sorted by distance
    const hits = results.getHits();
    for (1..hits.len) |j| {
        try std.testing.expect(hits[j].distance >= hits[j - 1].distance);
    }
}

test "findLeafWithOptions does not use-after-free when cache evicts mid-traversal" {
    // Regression test for a use-after-free in findLeafWithOptions.
    //
    // findLeafWithOptions calls getNodePtr to get a pointer into the node
    // cache, then iterates node.children (which points into the cached node's
    // backing buffer).  For each child it calls getNodePtr again, which may
    // trigger clearNodeCache when the cache is full — freeing ALL backing
    // buffers and leaving the children slice dangling.
    //
    // By setting max_cached_nodes very small we guarantee clearNodeCache fires
    // during traversal once the tree has more than a couple of internal nodes.
    // Without the fix (copying children to local storage), this test segfaults
    // or triggers Zig's debug allocator use-after-free detection (0xaa poison).
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .max_cached_nodes = 2,
        .max_cached_vectors = 2,
    });
    defer idx.close();

    // Phase 1: build a multi-level tree.  With leaf_size=2 and
    // branching_factor=2 the tree will start splitting after a handful of
    // inserts, creating internal nodes with children slices.
    var prng = std.Random.DefaultPrng.init(0xdeadbeef);
    const random = prng.random();
    for (0..30) |i| {
        var v: [4]f32 = undefined;
        for (&v) |*x| x.* = random.float(f32) * 10.0;
        try idx.insert(@intCast(i + 1), &v);
    }

    // Sanity: we must have multiple tree nodes for the bug to manifest.
    try std.testing.expect(idx.stats().node_count > 3);

    // Phase 2: insert more vectors.  Each insert calls findLeafWithOptions
    // which traverses the tree.  With max_cached_nodes=2, nearly every
    // getNodePtr call triggers a full cache clear — exactly the scenario
    // that caused the original crash.
    for (30..60) |i| {
        var v: [4]f32 = undefined;
        for (&v) |*x| x.* = random.float(f32) * 10.0;
        try idx.insert(@intCast(i + 1), &v);
    }

    try std.testing.expectEqual(@as(u64, 60), idx.stats().active_count);

    // Verify the index is still functional by searching.
    var results = try idx.search(&[_]f32{ 5.0, 5.0, 5.0, 5.0 }, 5);
    defer results.deinit();
    try std.testing.expect(results.getHits().len > 0);
}

test "findLeafWithOptions fails closed when cached internal children are stale" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 2,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .max_cached_nodes = 2,
    });
    defer idx.close();

    var centroid = [_]f32{ 0.0, 0.0 };
    var children = [_]u64{999};
    const root = Node{
        .id = 1,
        .is_leaf = false,
        .level = 1,
        .parent = 0,
        .centroid = centroid[0..],
        .children = children[0..],
        .members = &.{},
    };

    var write_txn = try idx.beginWriteTxn();
    errdefer write_txn.abort();
    const centroid_bytes = std.mem.sliceAsBytes(root.centroid);
    const ids_bytes = std.mem.sliceAsBytes(root.children);
    const packed_len = vectorindex_hbc.packedNodeValueSize(centroid_bytes.len, ids_bytes.len);
    const packed_buf = try alloc.alloc(u8, packed_len);
    defer alloc.free(packed_buf);
    const header = NodeHeader{ .is_leaf = false, .level = 1, .parent = 0 };
    const encoded = try vectorindex_hbc.encodePackedNodeValue(packed_buf, header, centroid_bytes, ids_bytes);
    var key_buf: [12]u8 = undefined;
    try idx.putNamespaced(&write_txn, .nodes, encodeNodeKey(&key_buf, root.id, .packed_node), encoded);
    try idx.cacheNode(&root);
    try idx.finishWriteTxn(&write_txn);

    var read_txn = try idx.beginRuntimeReadTxn();
    defer read_txn.abort();
    try std.testing.expectError(
        error.Corrupted,
        idx.findLeafWithOptions(&read_txn, idx.metadata.root_node, &.{ 1.0, 0.0 }, true),
    );
}

test "insert routes while search cache admission is disabled" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .leaf_size = 2,
        .branching_factor = 2,
        .search_width = 8,
        .max_cached_nodes = 2,
        .max_cached_vectors = 2,
    });
    defer idx.close();

    var prng = std.Random.DefaultPrng.init(0xbadc0de);
    const random = prng.random();
    for (0..30) |i| {
        var v: [4]f32 = undefined;
        for (&v) |*x| x.* = random.float(f32) * 10.0;
        try idx.insert(@intCast(i + 1), &v);
    }

    idx.clearNodeCache();
    idx.active_searches.store(2, .release);
    defer idx.active_searches.store(0, .release);

    var v: [4]f32 = undefined;
    for (&v) |*x| x.* = random.float(f32) * 10.0;
    try idx.insert(31, &v);

    try std.testing.expectEqual(@as(u64, 31), idx.stats().active_count);
}

test "scoreLeafMembers does not use-after-free when cache evicts during member scoring" {
    const alloc = std.testing.allocator;
    var tp: TestPath = .{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try HBCIndex.open(alloc, path, .{
        .dims = 4,
        .leaf_size = 64,
        .branching_factor = 2,
        .search_width = 8,
        .use_quantization = false,
        .max_cached_nodes = 2,
        .max_cached_vectors = 8,
    });
    defer idx.close();

    var prng = std.Random.DefaultPrng.init(0xfacefeed);
    const random = prng.random();
    for (0..24) |i| {
        var v: [4]f32 = undefined;
        for (&v) |*x| x.* = random.float(f32) * 10.0;
        try idx.insert(@intCast(i + 1), &v);
    }

    const HookCtx = struct {
        idx: *HBCIndex,
        fired: bool = false,

        fn onVectorLoad(ctx_ptr: ?*anyopaque, hook_idx: *HBCIndex, _: u64) void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr.?));
            if (ctx.fired) return;
            ctx.fired = true;
            std.debug.assert(ctx.idx == hook_idx);
            hook_idx.clearNodeCache();
        }
    };

    var ctx = HookCtx{ .idx = &idx };
    test_get_vector_view_or_scratch_ctx = &ctx;
    test_get_vector_view_or_scratch_hook = HookCtx.onVectorLoad;
    defer {
        test_get_vector_view_or_scratch_ctx = null;
        test_get_vector_view_or_scratch_hook = null;
    }

    var results = try idx.search(&[_]f32{ 5.0, 5.0, 5.0, 5.0 }, 5);
    defer results.deinit();
    try std.testing.expect(ctx.fired);
    try std.testing.expect(results.getHits().len > 0);
}

test "HBC runtime batch mode preserves bulk only for non-direct LSM bulk sessions" {
    try std.testing.expectEqual(vectorindex_store.BatchMode.default, hbcRuntimeBatchMode(false, false));
    try std.testing.expectEqual(vectorindex_store.BatchMode.default, hbcRuntimeBatchMode(false, true));
    try std.testing.expectEqual(vectorindex_store.BatchMode.default, hbcRuntimeBatchMode(false, null));

    try std.testing.expectEqual(vectorindex_store.BatchMode.bulk_ingest, hbcRuntimeBatchMode(true, false));
    try std.testing.expectEqual(vectorindex_store.BatchMode.default, hbcRuntimeBatchMode(true, true));
    try std.testing.expectEqual(vectorindex_store.BatchMode.default, hbcRuntimeBatchMode(true, null));
}

// ============================================================================
// Test helpers
// ============================================================================

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath) [*:0]const u8 {
        const ts = platform_time.monotonicNs();
        const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-hbc-test-{d}-{d}\x00", .{ ts, nonce }) catch unreachable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
        return @ptrCast(slice.ptr);
    }

    fn cleanup(self: *TestPath) void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(&self.buf)))) catch {};
    }
};
