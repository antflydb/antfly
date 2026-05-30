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
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const hbc_backend = @import("hbc_backend.zig");
const resource_manager_mod = @import("resource_manager.zig");
const apply_rw_lock_mod = @import("db/apply_rw_lock.zig");
const supports_lmdb = builtin.os.tag != .freestanding;
const lmdb = if (supports_lmdb) @import("lmdb.zig") else struct {
    pub const Error = error{NotFound};
};
const lsm_backend = @import("lsm_backend/mod.zig");
const platform_time = @import("../platform/time.zig");
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
const vectorindex_spfresh_index = @import("antfly_vectorindex").spfresh_index;
const vectorindex_hbc_transfer = @import("antfly_vectorindex").hbc_transfer;
const vectorindex_hbc_debug = @import("antfly_vectorindex").hbc_debug;

fn getenv(name: [*:0]const u8) ?[]const u8 {
    return platform.env.getenv(name);
}

var temp_path_nonce: u64 = 0;
const default_deferred_hbc_leaf_splits_per_publish: usize = 256;
const default_bulk_split_vector_workspace_budget_bytes: u64 = 256 * 1024 * 1024;

const TestGetVectorViewOrScratchHook = *const fn (?*anyopaque, *HBCIndex, u64) void;
var test_get_vector_view_or_scratch_ctx: ?*anyopaque = null;
var test_get_vector_view_or_scratch_hook: ?TestGetVectorViewOrScratchHook = null;

// ============================================================================
// Configuration
// ============================================================================

pub const HBCConfig = vectorindex_types.HBCConfig;
pub const StorageBackend = vectorindex_types.StorageBackend;
pub const BulkBuildAlgo = vectorindex_types.BulkBuildAlgo;
pub const LsmWriteStats = lsm_backend.Backend.WriteStats;
pub const LsmMaintenanceStats = lsm_backend.Backend.MaintenanceStats;

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.os.tag == .freestanding) {
            std.atomic.spinLoopHint();
        } else {
            std.Thread.yield() catch {};
        }
    }
}

fn envBoolDisabled(raw: []const u8) bool {
    return std.mem.eql(u8, raw, "0") or
        std.ascii.eqlIgnoreCase(raw, "false") or
        std.ascii.eqlIgnoreCase(raw, "no") or
        std.ascii.eqlIgnoreCase(raw, "off");
}

fn envBoolEnabled(raw: []const u8) bool {
    return std.mem.eql(u8, raw, "1") or
        std.ascii.eqlIgnoreCase(raw, "true") or
        std.ascii.eqlIgnoreCase(raw, "yes") or
        std.ascii.eqlIgnoreCase(raw, "on");
}

fn defaultRetainedVectorCacheEnabled() bool {
    if (comptime builtin.os.tag == .freestanding) return true;
    if (getenv("ANTFLY_HBC_VECTOR_CACHE")) |raw| {
        return !envBoolDisabled(raw);
    }
    if (getenv("ANTFLY_HBC_DISABLE_VECTOR_CACHE")) |raw| {
        return !envBoolEnabled(raw);
    }
    return true;
}

// ============================================================================
// Index metadata (serialized to LMDB)
// ============================================================================

const meta_key = vectorindex_hbc.meta_key;
const hbc_index_version = vectorindex_hbc.hbc_index_version;
const IndexMetadata = vectorindex_hbc.IndexMetadata;

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

fn hbcCacheNamespaceStable(alloc: Allocator, path: []const u8) u64 {
    if (comptime builtin.os.tag == .freestanding) {
        return hbcCacheNamespace(path);
    } else {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();

        const absolute_path = if (std.fs.path.isAbsolute(path))
            alloc.dupe(u8, path) catch return hbcCacheNamespace(path)
        else blk: {
            const cwd = std.process.currentPathAlloc(io_impl.io(), alloc) catch return hbcCacheNamespace(path);
            defer alloc.free(cwd);
            break :blk std.fs.path.resolve(alloc, &.{ cwd, path }) catch return hbcCacheNamespace(path);
        };
        defer alloc.free(absolute_path);
        if (!std.fs.path.isAbsolute(absolute_path)) return hbcCacheNamespace(absolute_path);

        const canonical = std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), absolute_path, alloc) catch return hbcCacheNamespace(absolute_path);
        defer alloc.free(canonical);
        return hbcCacheNamespace(canonical);
    }
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

    pub fn clear(self: *Cache) void {
        self.mutex.lockExclusive();
        defer self.mutex.unlockExclusive();
        self.clearAllLocked();
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
        if (self.namespace_stats.getPtr(namespace)) |stats| removeHbcKindBytes(stats, kind, bytes);
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
    rng: go_rand.GoPcg,
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
    shared_cache: ?*Cache = null,
    cache_namespace: u64 = 0,
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
    hbc_cache_kind_stats: [hbc_cache_kind_count]HbcCacheKindStats = @splat(HbcCacheKindStats{}),
    deferred_quantized_nodes: std.AutoHashMapUnmanaged(u64, void),
    deferred_node_keys: std.AutoHashMapUnmanaged(u128, DeferredNodeValue),
    deferred_oversized_leaves: std.AutoHashMapUnmanaged(u64, void),
    bulk_ingest_session_depth: usize = 0,
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
        accounted_bytes: u64 = 0,

        fn bytes(self: *const SplitVectorWorkspace) u64 {
            return @as(u64, @intCast(self.vectors.capacity)) * @sizeOf(f32) +
                @as(u64, @intCast(self.map.capacity())) * (@sizeOf(u64) + @sizeOf(usize));
        }

        fn clearRetainingCapacity(self: *SplitVectorWorkspace) void {
            self.map.clearRetainingCapacity();
            self.vectors.clearRetainingCapacity();
        }

        fn deinit(self: *SplitVectorWorkspace, alloc: Allocator) void {
            self.map.deinit(alloc);
            self.vectors.deinit(alloc);
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

    fn openVectorIndexStore(allocator: Allocator, opened: hbc_backend.OpenedBackend) !vectorindex_store.NamespaceStore {
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
            .lmdb => null,
        };
    }

    pub fn snapshotLsmMaintenanceStats(self: *const HBCIndex) ?LsmMaintenanceStats {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
            .lmdb => null,
        };
    }

    pub fn snapshotLsmNativeStorageStats(self: *const HBCIndex) ?lsm_backend.NativeStorageStats {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.snapshotNativeStorageStats(),
            .lmdb => null,
        };
    }

    pub fn lsmMaintenanceScore(self: *const HBCIndex) u64 {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.maintenanceScore(),
            .lmdb => 0,
        };
    }

    pub fn lsmMaintenanceDebtHint(self: *const HBCIndex) u64 {
        return switch (self.env_owner) {
            .lsm => |handle| handle.backend.maintenanceDebtHint(),
            .lmdb => 0,
        };
    }

    pub fn refreshLsmMaintenanceDebtHint(self: *HBCIndex) void {
        switch (self.env_owner) {
            .lsm => |handle| handle.backend.refreshMaintenanceDebtHint(),
            .lmdb => {},
        }
    }

    pub fn runLsmMaintenanceStep(self: *HBCIndex) !bool {
        return switch (self.env_owner) {
            .lsm => |handle| try handle.backend.runMaintenanceStep(),
            .lmdb => false,
        };
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *HBCIndex) !bool {
        if (self.bulk_ingest_session_depth > 0) return false;
        return switch (self.env_owner) {
            .lsm => |handle| try handle.backend.runMaintenanceStepBestEffort(),
            .lmdb => false,
        };
    }

    pub fn beginBulkIngestSession(self: *HBCIndex) !void {
        switch (self.env_owner) {
            .lsm => |handle| try handle.backend.beginBulkIngestSession(),
            .lmdb => {},
        }
        if (self.bulk_ingest_session_depth == 0) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
            self.clearDeferredNodeKeys();
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = 0;
            self.deferred_node_key_value_bytes = 0;
            self.observeApplyWorkspaceBytes();
        }
        self.bulk_ingest_session_depth += 1;
    }

    pub fn finishBulkIngestSessionWithOptions(self: *HBCIndex, options: backend_types.BulkIngestFinishOptions) !void {
        const finishing_outermost = self.bulk_ingest_session_depth > 0 and self.bulk_ingest_session_depth == 1;
        if (finishing_outermost) {
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
                publish_window += 1;
                const window_start_ns = nowNs();
                var batch = try self.store.beginBatch();
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
        switch (self.env_owner) {
            .lsm => |handle| try handle.backend.finishBulkIngestSessionWithOptions(options),
            .lmdb => {},
        }
        if (self.bulk_ingest_session_depth > 0) self.bulk_ingest_session_depth -= 1;
        if (finishing_outermost) self.refreshPublishedSearchState();
        if (self.bulk_ingest_session_depth == 0) {
            self.releaseDeferredBulkWorkspaceCapacity();
        }
    }

    pub fn abortBulkIngestSession(self: *HBCIndex) void {
        if (self.bulk_ingest_session_depth == 0) return;
        switch (self.env_owner) {
            .lsm => |handle| handle.backend.abortBulkIngestSession(),
            .lmdb => {},
        }
        self.bulk_ingest_session_depth -= 1;
        if (self.bulk_ingest_session_depth == 0) {
            self.releaseDeferredBulkWorkspaceCapacity();
        }
    }

    const SplitResult = vectorindex_hbc_index.SplitResult;

    /// Open or create an HBC index at the given path.
    pub fn open(alloc: Allocator, path: [*:0]const u8, config: HBCConfig) !HBCIndex {
        return try openWithLsmStorage(alloc, path, config, null);
    }

    pub fn openWithLsmStorage(alloc: Allocator, path: [*:0]const u8, config: HBCConfig, lsm_storage: ?lsm_backend.Storage) !HBCIndex {
        return try openWithLsmOptions(alloc, path, config, .{ .storage = lsm_storage });
    }

    pub fn openWithLsmOptions(alloc: Allocator, path: [*:0]const u8, config: HBCConfig, lsm_options: hbc_backend.LsmOptions) !HBCIndex {
        var opened = try hbc_backend.openBackendWithLsmOptions(alloc, path, config, lsm_options);
        errdefer opened.close(alloc);

        var store = try openVectorIndexStore(alloc, opened);
        errdefer store.deinit();

        const metadata = if (lsm_options.backend_options.backend.read_only) blk: {
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
            .shared_cache = null,
            .cache_namespace = hbcCacheNamespaceStable(alloc, std.mem.span(path)),
            .cache_enabled = true,
            .retained_vector_cache_enabled = defaultRetainedVectorCacheEnabled(),
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
            .hbc_cache_kind_stats = @splat(HbcCacheKindStats{}),
            .deferred_quantized_nodes = .empty,
            .deferred_node_keys = .empty,
            .deferred_oversized_leaves = .empty,
            .bulk_ingest_session_depth = 0,
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
        return self.bulk_ingest_session_depth == 0;
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
        self.resource_manager = resource_manager;
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
            cache.attachResourceManager(resource_manager);
            return;
        }
        self.refreshAndEnforceHbcCacheUsage(.none());
    }

    pub fn attachSharedCache(self: *HBCIndex, cache: *Cache) void {
        self.clearNodeCache();
        self.clearQuantizedCache();
        self.clearVectorCache();
        self.clearMetadataCache();
        self.shared_cache = cache;
        if (self.resource_manager) |manager| cache.attachResourceManager(manager);
    }

    pub fn setCacheEnabled(self: *HBCIndex, enabled: bool) void {
        if (self.cache_enabled == enabled) return;
        self.cache_enabled = enabled;
        if (!enabled) {
            self.clearNodeCache();
            self.clearQuantizedCache();
            self.clearVectorCache();
            self.clearMetadataCache();
        }
    }

    pub fn setRetainedVectorCacheEnabled(self: *HBCIndex, enabled: bool) void {
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

    fn bulkSplitVectorWorkspaceLookup(self: *HBCIndex, vector_id: u64, out: []f32) bool {
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
        if (workspace.map.contains(vector_id)) return;

        const vector_bytes = @as(u64, @intCast(self.config.dims)) * @sizeOf(f32);
        if (workspace.accounted_bytes + vector_bytes > self.bulkSplitVectorWorkspaceBudgetBytes()) return;

        const offset = workspace.vectors.items.len;
        workspace.vectors.appendSlice(self.alloc, transformed) catch return;
        workspace.map.put(self.alloc, vector_id, offset) catch {
            workspace.vectors.items.len = offset;
            return;
        };
        self.observeBulkSplitVectorWorkspaceBytes();
    }

    pub fn setCacheCaps(self: *HBCIndex, max_cached_nodes: usize, max_cached_vectors: usize) void {
        const changed = self.config.max_cached_nodes != max_cached_nodes or self.config.max_cached_vectors != max_cached_vectors;
        self.config.max_cached_nodes = max_cached_nodes;
        self.config.max_cached_vectors = max_cached_vectors;
        if (!changed) return;
        self.clearNodeCache();
        self.clearQuantizedCache();
        self.clearVectorCache();
        self.clearMetadataCache();
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
        var bytes: [hbc_cache_kind_count]u64 = @splat(0);
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
        self.apply_workspace_split_bytes = 0;
        self.deferred_node_key_value_bytes = 0;
        self.observeApplyWorkspaceBytes();
        self.deferred_quantized_nodes.deinit(self.alloc);
        self.clearDeferredNodeKeys();
        self.deferred_node_keys.deinit(self.alloc);
        self.deferred_oversized_leaves.deinit(self.alloc);
        self.rot.deinit();
        self.quantizer.deinit();
        self.store.deinit();
        self.env_owner.close(self.alloc);
        self.* = undefined;
    }

    pub fn sync(self: *HBCIndex, force: bool) !void {
        try self.env_owner.sync(force);
    }

    pub fn syncReplayState(self: *HBCIndex) !void {
        try self.env_owner.syncReplayState();
    }

    fn txnLikeChild(comptime T: type) type {
        return switch (@typeInfo(T)) {
            .pointer => |ptr| ptr.child,
            else => @compileError("expected pointer to transaction-like type"),
        };
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
        self.endBulkSplitVectorWorkspace();
        self.deferred_quantized_nodes.deinit(self.alloc);
        self.deferred_quantized_nodes = .empty;
        self.clearDeferredNodeKeys();
        self.deferred_node_keys.deinit(self.alloc);
        self.deferred_node_keys = .empty;
        self.deferred_oversized_leaves.deinit(self.alloc);
        self.deferred_oversized_leaves = .empty;
        self.apply_workspace_split_bytes = 0;
        self.deferred_node_key_value_bytes = 0;
        self.observeApplyWorkspaceBytes();
    }

    fn stagedNodeKey(self: *HBCIndex, key: []const u8) ?*DeferredNodeValue {
        const staged_key = stagedNodeKeyId(key) orelse return null;
        return self.deferred_node_keys.getPtr(staged_key);
    }

    fn stageNodeKeyPut(self: *HBCIndex, key: []const u8, value: []const u8) !bool {
        if (self.bulk_ingest_session_depth == 0) return false;
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
        if (self.bulk_ingest_session_depth == 0) return false;
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

    pub fn getNamespaced(self: *HBCIndex, txn: anytype, comptime namespace: Namespace, key: []const u8) ![]const u8 {
        if (namespace == .nodes) {
            if (self.stagedNodeKey(key)) |staged| {
                return staged.value orelse error.NotFound;
            }
        }
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
        _ = self;
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
                if (namespace == .nodes and try self.stageNodeKeyPut(key, value)) return;
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
                if (namespace == .nodes and try self.stageNodeKeyDelete(key)) return;
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

    pub fn beginRuntimeReadTxn(self: *HBCIndex) !vectorindex_store.NamespaceReadTxn {
        return try self.store.beginRead();
    }

    pub fn beginRuntimeSearchTxn(self: *HBCIndex) !vectorindex_store.NamespaceReadTxn {
        return try self.store.beginProbeOrRead();
    }

    pub fn beginRuntimeWriteTxn(self: *HBCIndex) !vectorindex_store.NamespaceWriteTxn {
        if (self.bulk_ingest_session_depth == 0) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = 0;
            self.observeApplyWorkspaceBytes();
        }
        return try self.store.beginWrite();
    }

    pub fn beginRuntimeBatchTxn(self: *HBCIndex) !vectorindex_store.NamespaceBatch {
        if (self.bulk_ingest_session_depth == 0) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = 0;
            self.observeApplyWorkspaceBytes();
        }
        return try self.store.beginBatch();
    }

    pub fn beginRuntimeBatchTxnOptions(self: *HBCIndex, options: BatchInsertOptions) !vectorindex_store.NamespaceBatch {
        if (!self.shouldDeferQuantizedRebuildToBulkFinish(options)) {
            self.deferred_quantized_nodes.clearRetainingCapacity();
        }
        if (self.bulk_ingest_session_depth == 0) {
            self.deferred_oversized_leaves.clearRetainingCapacity();
            self.apply_workspace_split_bytes = 0;
            self.observeApplyWorkspaceBytes();
        }
        // HBC mutation batches rewrite nodes, ranges, and quantized payloads
        // heavily. The outer bulk-ingest session still defers manifests and
        // compaction, but each mutation batch must use normal mutable-state
        // coalescing instead of direct sorted-run ingestion. Otherwise every
        // stale internal rewrite becomes durable table bytes during large loads.
        return try self.store.beginBatchWithOptions(.{
            .mode = .default,
        });
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
        if (self.bulk_ingest_session_depth > 0) return;
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
        if (self.bulk_ingest_session_depth > 0) return vector_data;
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
        if (self.bulk_ingest_session_depth > 0) return metadata;
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
        if (self.bulk_ingest_session_depth > 0) return null;
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

    pub fn flushMetadata(self: *HBCIndex, txn: anytype) !void {
        if (self.bulk_ingest_session_depth > 0) return;
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
        return self.bulk_ingest_session_depth > 0 and options.bulk_ingest and options.defer_quantized_rebuild_to_bulk_finish;
    }

    pub fn shouldDeferLeafSplitToBulkFinish(self: *const HBCIndex, options: BatchInsertOptions) bool {
        return self.bulk_ingest_session_depth > 0 and options.bulk_ingest and options.defer_leaf_splits_to_bulk_finish;
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
                try txn.put(.nodes, key, value);
                self.noteNamespacePut(.nodes, key.len, value.len, false);
            } else {
                try txn.delete(.nodes, key);
                self.noteNamespaceDelete(.nodes, key.len);
            }
        }

        self.clearDeferredNodeKeys();
    }

    pub fn publishDeferredNodeKeysForBatchFinishTxn(self: *HBCIndex, txn: anytype, options: BatchInsertOptions) !void {
        if (!options.bulk_ingest) return;
        if (self.bulk_ingest_session_depth > 0 and self.config.centroid_directory_mode == .flat_rabitq) return;
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
        if (self.metadata.active_count == 0) return;
        if (self.metadata.root_node == 0 or self.metadata.node_count == 0) return error.Corrupted;

        var txn = try self.beginRuntimeReadTxn();
        defer txn.abort();

        var pending = std.ArrayListUnmanaged(u64).empty;
        defer pending.deinit(alloc);
        try pending.append(alloc, self.metadata.root_node);

        var seen = std.AutoHashMapUnmanaged(u64, void).empty;
        defer seen.deinit(alloc);

        while (pending.pop()) |node_id| {
            if (node_id == 0) return error.Corrupted;
            const gop = try seen.getOrPut(alloc, node_id);
            if (gop.found_existing) continue;

            var node = try self.loadNodeFromStorage(&txn, node_id);
            defer node.deinit(alloc);

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
        if (writes.len == 0 or options.batch_vectors != null) {
            try vectorindex_hbc_index.batchApplyOptions(self, writes, deletes, options, nowNs, elapsedSince);
            return;
        }
        var batch_vectors = try BatchVectorContext.init(self, writes);
        defer batch_vectors.deinit(self);
        try vectorindex_hbc_index.batchApplyOptions(self, writes, deletes, optionsWithBatchVectors(options, batch_vectors.erased()), nowNs, elapsedSince);
    }

    pub fn batchInsertWithMetadataOptions(self: *HBCIndex, items: []const BatchInsertItem, options: BatchInsertOptions) !void {
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

        if (best_child == 0) return node_id;
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
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        const result = try vectorindex_hbc_index.repairDirtyPostingsTxnWithOptions(self, &txn, options);
        const commit_start = nowNs();
        self.beginPublishedSearchStateRefresh();
        errdefer self.abortPublishedSearchStateRefresh();
        try commitTxn(&txn);
        self.write_profile.insert_commit_ns += elapsedSince(commit_start);
        self.finishPublishedSearchStateRefresh();
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
    var workers = @as([8]Worker, @splat(.{ .idx = &idx, .failed = &failed }));
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

test "mixed delete write batch routes covered replacements as absent" {
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
    try std.testing.expectEqual(@as(u64, 1), profile.batch_route_calls);
    try std.testing.expect(profile.batch_route_internal_nodes > 0);
    try std.testing.expectEqual(@as(u64, writes.len), profile.batch_route_items);

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
    defer idx.close();

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
    var matrix: [6]f32 = @splat(0);
    var matrix_again: [6]f32 = @splat(0);
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
