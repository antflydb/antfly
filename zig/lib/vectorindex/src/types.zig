// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const Allocator = std.mem.Allocator;
const vec = @import("antfly_vector").vector;

pub const HBCConfig = struct {
    pub const RerankPolicy = enum {
        always,
        boundary,
        never,
    };

    pub const KmeansUpdateStrategy = enum {
        auto,
        scatter,
        segmented,
        metal,
    };

    pub const KmeansBackend = enum {
        auto,
        cpu,
        metal,
    };

    pub const CentroidDirectoryMode = enum {
        hbc,
        flat_rabitq,
    };

    storage_backend: StorageBackend = .lmdb,
    dims: u32,
    metric: vec.DistanceMetric = .l2_squared,
    split_algo: vec.ClustAlgorithm = .kmeans,
    branching_factor: u32 = 16,
    leaf_size: u32 = 100,
    search_width: u32 = 32,
    epsilon: f32 = 0.1,
    kmeans_max_iter: u32 = 16,
    kmeans_min_balance_pct: u32 = 33,
    kmeans_backend: KmeansBackend = .auto,
    kmeans_update_strategy: KmeansUpdateStrategy = .auto,
    use_quantization: bool = true,
    rerank_policy: RerankPolicy = .always,
    quantizer_seed: u64 = 42,
    use_random_ortho_trans: bool = false,
    bulk_build_algo: BulkBuildAlgo = .hilbert_seeded,
    prefer_key_local_leaf_splits: bool = false,
    key_local_leaf_split_penalty: f32 = 1.10,
    max_cached_nodes: usize = 100_000,
    max_cached_vectors: usize = 100_000,
    max_cached_metadata: usize = 100_000,
    pinned_tree_depth: u8 = 3,
    max_pinned_tree_nodes: usize = 4096,
    map_size: usize = 256 * 1024 * 1024,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    defer_page_mutation: bool = false,
    lazy_posting_maintenance: bool = false,
    auto_posting_maintenance_max_postings: usize = 0,
    centroid_directory_mode: CentroidDirectoryMode = .hbc,
    flat_centroid_block_size: usize = 8192,
    flat_centroid_probe_count: usize = 0,
};

pub const StorageBackend = enum {
    lmdb,
    lsm,
};

pub const BulkBuildAlgo = enum {
    recursive,
    hilbert_seeded,
    doc_key_seeded,
    kmeans,
};

pub const Node = struct {
    id: u64,
    is_leaf: bool,
    level: u16,
    parent: u64,
    centroid: []f32,
    children: []u64,
    members: []u64,
    posting_state: PostingState = .{},
    backing: []align(@alignOf(u64)) u8 = &.{},

    pub fn deinit(self: *Node, alloc: Allocator) void {
        if (self.backing.len > 0) {
            alloc.free(self.backing);
        } else {
            if (self.centroid.len > 0) alloc.free(self.centroid);
            if (self.children.len > 0) alloc.free(self.children);
            if (self.members.len > 0) alloc.free(self.members);
        }
        self.* = undefined;
    }

    /// Convert from backed mode (slices are sub-views of a single backing
    /// buffer) to individually-owned mode so fields can be independently
    /// freed or replaced.  No-op if already unbacked.
    pub fn ensureUnbacked(self: *Node, alloc: Allocator) !void {
        if (self.backing.len == 0) return;
        const centroid = try alloc.dupe(f32, self.centroid);
        errdefer alloc.free(centroid);
        const children = try alloc.dupe(u64, self.children);
        errdefer alloc.free(children);
        const members = try alloc.dupe(u64, self.members);
        alloc.free(self.backing);
        self.backing = &.{};
        self.centroid = centroid;
        self.children = children;
        self.members = members;
    }

    pub fn clone(self: Node, alloc: Allocator) !Node {
        return .{
            .id = self.id,
            .is_leaf = self.is_leaf,
            .level = self.level,
            .parent = self.parent,
            .centroid = try alloc.dupe(f32, self.centroid),
            .children = try alloc.dupe(u64, self.children),
            .members = try alloc.dupe(u64, self.members),
            .posting_state = self.posting_state,
            .backing = &.{},
        };
    }
};

pub const PostingState = struct {
    mutation_version: u64 = 0,
    centroid_version: u64 = 0,
    payload_version: u64 = 0,
    dirty: bool = false,
    centroid_dirty: bool = false,
    payload_dirty: bool = false,

    pub fn noteMembersChanged(self: *PostingState, member_count: usize) void {
        _ = member_count;
        self.mutation_version +|= 1;
        self.dirty = true;
        self.centroid_dirty = true;
        self.payload_dirty = true;
    }

    pub fn noteCentroidRefreshed(self: *PostingState) void {
        self.centroid_version = self.mutation_version;
        self.centroid_dirty = false;
        self.refreshDirtyFlag();
    }

    pub fn notePayloadRefreshed(self: *PostingState) void {
        self.payload_version = self.mutation_version;
        self.payload_dirty = false;
        self.refreshDirtyFlag();
    }

    fn refreshDirtyFlag(self: *PostingState) void {
        self.dirty = self.centroid_dirty or self.payload_dirty;
    }
};

pub const PriorityItem = struct {
    id: u64,
    distance: f32,
    error_bound: f32 = 0,
    is_leaf: bool = false,

    pub fn definitelyCloser(self: PriorityItem, other: PriorityItem) bool {
        return self.distance + self.error_bound < other.distance - other.error_bound;
    }

    pub fn maybeCloser(self: PriorityItem, other: PriorityItem) bool {
        return self.distance - self.error_bound <= other.distance + other.error_bound;
    }
};

pub const NodeSplitClass = enum {
    left_only,
    right_only,
    mixed,
    unknown,
};

pub const NodeSplitRange = struct {
    min_key: []u8,
    max_key: []u8,

    pub fn deinit(self: *NodeSplitRange, alloc: Allocator) void {
        alloc.free(self.min_key);
        alloc.free(self.max_key);
        self.* = undefined;
    }

    pub fn clone(self: *const NodeSplitRange, alloc: Allocator) !NodeSplitRange {
        return .{
            .min_key = try alloc.dupe(u8, self.min_key),
            .max_key = try alloc.dupe(u8, self.max_key),
        };
    }

    pub fn classify(self: *const NodeSplitRange, split_key: []const u8) NodeSplitClass {
        if (std.mem.order(u8, self.max_key, split_key) == .lt) return .left_only;
        if (std.mem.order(u8, self.min_key, split_key) != .lt) return .right_only;
        return .mixed;
    }
};

pub const SplitPlanningStats = struct {
    left_only: usize = 0,
    right_only: usize = 0,
    mixed: usize = 0,
    unknown: usize = 0,
    leaves: usize = 0,
    internal: usize = 0,
};

pub const SplitReusePlan = struct {
    right_only_roots: []u64,
    mixed_leaves: []u64,

    pub fn deinit(self: *SplitReusePlan, alloc: Allocator) void {
        alloc.free(self.right_only_roots);
        alloc.free(self.mixed_leaves);
        self.* = undefined;
    }
};

pub const SplitRebuildWork = struct {
    right_only_roots: usize = 0,
    mixed_leaves: usize = 0,
    right_only_members: usize = 0,
    mixed_right_members: usize = 0,

    pub fn totalRightMembers(self: SplitRebuildWork) usize {
        return self.right_only_members + self.mixed_right_members;
    }
};
