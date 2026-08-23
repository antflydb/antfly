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
const Allocator = std.mem.Allocator;
const artifacts_mod = @import("../artifacts/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const graph_segment_mod = @import("../graph_segment/mod.zig");
const cache_mod = @import("cache.zig");
const bounded_decode = @import("../bounded_decode.zig");
const graph_reader = @import("graph_reader.zig");
const request_mod = @import("request.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const QueryExecutionMetrics = struct {
    total_queries: u64 = 0,
    vector_queries: u64 = 0,
    hybrid_queries: u64 = 0,
    sparse_queries: u64 = 0,
    total_actual_probes: u64 = 0,
    total_shortlist_candidates: u64 = 0,
    total_quantized_candidates: u64 = 0,
    total_exact_reranks: u64 = 0,
    total_cluster_prunes: u64 = 0,
};

pub const NamespaceQueryExecutionMetrics = struct {
    namespace: []u8,
    metrics: QueryExecutionMetrics,

    pub fn deinit(self: *NamespaceQueryExecutionMetrics, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const QueryRuntime = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,
    cache: ?*cache_mod.QueryCache = null,
    metrics_mu: std.atomic.Mutex = .unlocked,
    metrics: QueryExecutionMetrics = .{},
    namespace_metrics: std.StringHashMapUnmanaged(QueryExecutionMetrics) = .empty,

    pub fn init(
        alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        manifests: *manifest_mod.ManifestStore,
        progress: *catalog_mod.ProgressStore,
    ) QueryRuntime {
        return .{
            .alloc = alloc,
            .artifacts = artifacts,
            .manifests = manifests,
            .progress = progress,
        };
    }

    pub fn deinit(self: *QueryRuntime) void {
        lockAtomic(&self.metrics_mu);
        var it = self.namespace_metrics.iterator();
        while (it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.namespace_metrics.deinit(self.alloc);
        self.metrics_mu.unlock();
        self.* = undefined;
    }

    pub fn initWithCache(
        alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        manifests: *manifest_mod.ManifestStore,
        progress: *catalog_mod.ProgressStore,
        cache: *cache_mod.QueryCache,
    ) QueryRuntime {
        var runtime = init(alloc, artifacts, manifests, progress);
        runtime.cache = cache;
        return runtime;
    }

    pub fn openVersionSession(self: *QueryRuntime, namespace: []const u8, version: u64) !QuerySession {
        return .{
            .alloc = self.alloc,
            .artifacts = self.artifacts,
            .cache = self.cache,
            .manifest = try self.manifests.getAlloc(namespace, version),
        };
    }

    pub fn openHeadSession(self: *QueryRuntime, namespace: []const u8) !QuerySession {
        const version = try self.progress.getHead(namespace);
        return try self.openVersionSession(namespace, version);
    }

    pub fn recordSearchStats(self: *QueryRuntime, namespace: []const u8, mode: request_mod.QueryMode, stats: anytype) !void {
        lockAtomic(&self.metrics_mu);
        defer self.metrics_mu.unlock();

        var namespace_metrics = self.namespace_metrics.getPtr(namespace);
        if (namespace_metrics == null) {
            try self.namespace_metrics.ensureUnusedCapacity(self.alloc, 1);
            const owned_namespace = try self.alloc.dupe(u8, namespace);
            self.namespace_metrics.putAssumeCapacityNoClobber(owned_namespace, .{});
            namespace_metrics = self.namespace_metrics.getPtr(owned_namespace).?;
        }

        self.metrics.total_queries += 1;
        applyModeCount(&self.metrics, mode);
        self.metrics.total_actual_probes += stats.actual_probe_count;
        self.metrics.total_shortlist_candidates += stats.actual_shortlist_count;
        self.metrics.total_quantized_candidates += stats.quantized_candidate_count;
        self.metrics.total_exact_reranks += stats.exact_rerank_count;
        self.metrics.total_cluster_prunes += stats.cluster_prune_count;

        namespace_metrics.?.total_queries += 1;
        applyModeCount(namespace_metrics.?, mode);
        namespace_metrics.?.total_actual_probes += stats.actual_probe_count;
        namespace_metrics.?.total_shortlist_candidates += stats.actual_shortlist_count;
        namespace_metrics.?.total_quantized_candidates += stats.quantized_candidate_count;
        namespace_metrics.?.total_exact_reranks += stats.exact_rerank_count;
        namespace_metrics.?.total_cluster_prunes += stats.cluster_prune_count;
    }

    pub fn metricsSnapshot(self: *QueryRuntime) QueryExecutionMetrics {
        lockAtomic(&self.metrics_mu);
        defer self.metrics_mu.unlock();
        return self.metrics;
    }

    pub fn namespaceMetricsAlloc(self: *QueryRuntime, alloc: Allocator) ![]NamespaceQueryExecutionMetrics {
        lockAtomic(&self.metrics_mu);
        defer self.metrics_mu.unlock();
        const out = try alloc.alloc(NamespaceQueryExecutionMetrics, self.namespace_metrics.count());
        errdefer alloc.free(out);
        var idx: usize = 0;
        errdefer for (out[0..idx]) |*entry| entry.deinit(alloc);
        var it = self.namespace_metrics.iterator();
        while (it.next()) |entry| : (idx += 1) {
            out[idx] = .{
                .namespace = try alloc.dupe(u8, entry.key_ptr.*),
                .metrics = entry.value_ptr.*,
            };
        }
        return out;
    }
};

pub const QuerySession = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    cache: ?*cache_mod.QueryCache = null,
    manifest: manifest_mod.Manifest,
    cancellation: CancellationToken = .none,

    pub fn deinit(self: *QuerySession) void {
        self.manifest.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn namespace(self: *const QuerySession) []const u8 {
        return self.manifest.namespace;
    }

    pub fn version(self: *const QuerySession) u64 {
        return self.manifest.version;
    }

    pub fn artifactCount(self: *const QuerySession) usize {
        return self.manifest.artifacts.len;
    }

    pub fn artifactRef(self: *const QuerySession, index: usize) ?manifest_mod.ArtifactRef {
        if (index >= self.manifest.artifacts.len) return null;
        return self.manifest.artifacts[index];
    }

    pub fn setCancellation(self: *QuerySession, cancellation: CancellationToken) void {
        self.cancellation = cancellation;
    }

    pub fn checkCancellation(self: *const QuerySession) !void {
        return self.cancellation.check();
    }

    pub fn findArtifactIndex(self: *const QuerySession, kind: manifest_mod.ArtifactKind) ?usize {
        for (self.manifest.artifacts, 0..) |artifact, idx| {
            if (artifact.kind == kind) return idx;
        }
        return null;
    }

    pub fn findNamedArtifactIndex(self: *const QuerySession, kind: manifest_mod.ArtifactKind, name: []const u8) ?usize {
        for (self.manifest.artifacts, 0..) |artifact, idx| {
            if (artifact.kind != kind) continue;
            if (std.mem.eql(u8, artifact.name, name)) return idx;
        }
        return null;
    }

    pub fn fetchArtifactAlloc(self: *QuerySession, index: usize) ![]u8 {
        try self.checkCancellation();
        const artifact = self.artifactRef(index) orelse return error.ArtifactNotFound;
        try validateArtifactForQuery(artifact);
        const result = if (self.cache) |cache|
            try cache.getOrFetchVerifiedAllocWithCancellationUsingAllocator(
                self.alloc,
                self.artifacts,
                artifact.artifact_id,
                artifact.byte_len,
                artifact.checksum,
                self.cancellation,
            )
        else
            try self.artifacts.getVerifiedAllocWithCancellationUsingAllocator(
                self.alloc,
                artifact.artifact_id,
                artifact.byte_len,
                artifact.checksum,
                self.cancellation,
            );
        errdefer self.alloc.free(result);
        try self.checkCancellation();
        return result;
    }

    pub fn fetchArtifactRangeAlloc(self: *QuerySession, index: usize, offset: u64, len: usize) ![]u8 {
        try self.checkCancellation();
        const artifact = self.artifactRef(index) orelse return error.ArtifactNotFound;
        try validateArtifactRange(artifact, offset, len);
        const result = if (self.cache) |cache|
            try cache.getRangeOrFetchAllocWithCancellationUsingAllocator(self.alloc, self.artifacts, artifact.artifact_id, offset, len, self.cancellation)
        else
            try self.artifacts.getRangeAllocWithCancellationUsingAllocator(self.alloc, artifact.artifact_id, offset, len, self.cancellation);
        errdefer self.alloc.free(result);
        try self.checkCancellation();
        return result;
    }

    pub fn fetchArtifactBlockRangeAlloc(self: *QuerySession, index: usize, block_id: []const u8, offset: u64, len: usize) ![]u8 {
        try self.checkCancellation();
        const artifact = self.artifactRef(index) orelse return error.ArtifactNotFound;
        try validateArtifactRange(artifact, offset, len);
        const result = if (self.cache) |cache|
            try cache.getBlockOrFetchRangeAllocWithCancellationUsingAllocator(self.alloc, self.artifacts, artifact.artifact_id, block_id, offset, len, self.cancellation)
        else
            try self.artifacts.getRangeAllocWithCancellationUsingAllocator(self.alloc, artifact.artifact_id, offset, len, self.cancellation);
        errdefer self.alloc.free(result);
        try self.checkCancellation();
        return result;
    }

    pub fn warmArtifact(self: *QuerySession, index: usize) !void {
        if (self.cache == null) return;
        const contents = try self.fetchArtifactAlloc(index);
        self.alloc.free(contents);
    }

    pub fn warmArtifactKind(self: *QuerySession, kind: manifest_mod.ArtifactKind) !void {
        const index = self.findArtifactIndex(kind) orelse return;
        try self.warmArtifact(index);
    }
};

fn validateArtifactRange(artifact: manifest_mod.ArtifactRef, offset: u64, len: usize) !void {
    try validateArtifactIdentity(artifact);
    if (len > (bounded_decode.Limits{}).max_artifact_bytes) return error.QueryArtifactBudgetExceeded;
    const len_u64 = std.math.cast(u64, len) orelse return error.ArtifactRangeTooLarge;
    const end = std.math.add(u64, offset, len_u64) catch return error.InvalidArtifactRange;
    if (end > artifact.byte_len) return error.InvalidArtifactRange;
}

fn validateArtifactForQuery(artifact: manifest_mod.ArtifactRef) !void {
    try validateArtifactIdentity(artifact);
    if (artifact.byte_len > (bounded_decode.Limits{}).max_artifact_bytes) {
        return error.QueryArtifactBudgetExceeded;
    }
}

fn validateArtifactIdentity(artifact: manifest_mod.ArtifactRef) !void {
    artifacts_mod.validateSha256ArtifactIdentity(artifact.artifact_id, artifact.checksum) catch
        return error.ArtifactIntegrityMismatch;
}

test "query runtime pins manifest version while head advances" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var artifact_v1 = try artifact_store.put("version-one");
    defer artifact_v1.deinit(alloc);
    var manifest_v1 = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 10,
        .wal_end_lsn = 11,
        .stats = .{ .document_count = 1, .text_segment_count = 1, .vector_segment_count = 0 },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1),
    };
    defer manifest_v1.deinit(alloc);
    manifest_v1.artifacts[0] = .{
        .kind = .text_segment,
        .artifact_id = try alloc.dupe(u8, artifact_v1.artifact_id),
        .byte_len = artifact_v1.byte_len,
        .checksum = try alloc.dupe(u8, artifact_v1.checksum),
    };
    try manifest_store.put(manifest_v1);
    try manifest_store.setHead("docs", 1);

    var runtime = QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer runtime.deinit();
    var session_v1 = try runtime.openHeadSession("docs");
    defer session_v1.deinit();

    var artifact_v2 = try artifact_store.put("version-two");
    defer artifact_v2.deinit(alloc);
    var manifest_v2 = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 2,
        .built_at_ns = 2,
        .wal_start_lsn = 12,
        .wal_end_lsn = 13,
        .stats = .{ .document_count = 1, .text_segment_count = 1, .vector_segment_count = 0 },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1),
    };
    defer manifest_v2.deinit(alloc);
    manifest_v2.artifacts[0] = .{
        .kind = .text_segment,
        .artifact_id = try alloc.dupe(u8, artifact_v2.artifact_id),
        .byte_len = artifact_v2.byte_len,
        .checksum = try alloc.dupe(u8, artifact_v2.checksum),
    };
    try manifest_store.put(manifest_v2);
    try manifest_store.setHead("docs", 2);

    const pinned = try session_v1.fetchArtifactAlloc(0);
    defer alloc.free(pinned);
    try std.testing.expectEqual(@as(u64, 1), session_v1.version());
    try std.testing.expectEqualStrings("version-one", pinned);

    var session_v2 = try runtime.openHeadSession("docs");
    defer session_v2.deinit();
    const latest = try session_v2.fetchArtifactAlloc(0);
    defer alloc.free(latest);
    try std.testing.expectEqual(@as(u64, 2), session_v2.version());
    try std.testing.expectEqualStrings("version-two", latest);
}

test "query session named artifact lookup does not fall back to unnamed artifact" {
    const alloc = std.testing.allocator;

    var session = QuerySession{
        .alloc = alloc,
        .artifacts = undefined,
        .cache = null,
        .manifest = .{
            .namespace = try alloc.dupe(u8, "docs"),
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 0,
            .wal_end_lsn = 0,
            .stats = .{},
            .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 2),
        },
    };
    defer session.deinit();

    session.manifest.artifacts[0] = .{
        .kind = .sparse_segment,
        .name = &.{},
        .artifact_id = try alloc.dupe(u8, "artifact-default"),
        .byte_len = 0,
        .checksum = try alloc.dupe(u8, "checksum-default"),
    };
    session.manifest.artifacts[1] = .{
        .kind = .sparse_segment,
        .name = try alloc.dupe(u8, "sparse_b"),
        .artifact_id = try alloc.dupe(u8, "artifact-b"),
        .byte_len = 0,
        .checksum = try alloc.dupe(u8, "checksum-b"),
    };

    try std.testing.expectEqual(@as(?usize, 1), session.findNamedArtifactIndex(.sparse_segment, "sparse_b"));
    try std.testing.expectEqual(@as(?usize, null), session.findNamedArtifactIndex(.sparse_segment, "missing"));
}

test "graph reader routes named graph artifacts by index name" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph-named-routing");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph-named-routing");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var graph_a = graph_segment_mod.Segment{
        .adjacencies = try alloc.alloc(graph_segment_mod.Adjacency, 1),
    };
    defer graph_segment_mod.freeSegment(alloc, &graph_a);
    graph_a.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_segment_mod.Edge, 1),
        .in_edges = try alloc.alloc(graph_segment_mod.Edge, 0),
    };
    graph_a.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };

    var graph_b = graph_segment_mod.Segment{
        .adjacencies = try alloc.alloc(graph_segment_mod.Adjacency, 1),
    };
    defer graph_segment_mod.freeSegment(alloc, &graph_b);
    graph_b.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_segment_mod.Edge, 1),
        .in_edges = try alloc.alloc(graph_segment_mod.Edge, 0),
    };
    graph_b.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-z"),
        .edge_type = try alloc.dupe(u8, "rel"),
        .weight = 2.0,
    };

    const payload_a = try graph_segment_mod.encodeAlloc(alloc, graph_a);
    defer alloc.free(payload_a);
    const payload_b = try graph_segment_mod.encodeAlloc(alloc, graph_b);
    defer alloc.free(payload_b);

    var artifact_a = try artifact_store.put(payload_a);
    defer artifact_a.deinit(alloc);
    var artifact_b = try artifact_store.put(payload_b);
    defer artifact_b.deinit(alloc);

    var manifest = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{ .graph_segment_count = 2 },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 2),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .graph_segment,
        .name = try alloc.dupe(u8, "graph_a"),
        .artifact_id = try alloc.dupe(u8, artifact_a.artifact_id),
        .byte_len = artifact_a.byte_len,
        .checksum = try alloc.dupe(u8, artifact_a.checksum),
    };
    manifest.artifacts[1] = .{
        .kind = .graph_segment,
        .name = try alloc.dupe(u8, "graph_b"),
        .artifact_id = try alloc.dupe(u8, artifact_b.artifact_id),
        .byte_len = artifact_b.byte_len,
        .checksum = try alloc.dupe(u8, artifact_b.checksum),
    };
    try manifest_store.put(manifest);
    try std.testing.expect(try progress_store.compareAndSwapHead("docs", null, 1));

    var runtime = QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var session = try runtime.openHeadSession("docs");
    defer session.deinit();

    var req = request_mod.GraphNeighborsRequest{
        .index_name = try alloc.dupe(u8, "graph_b"),
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .direction = .out,
        .limit = 10,
    };
    defer req.deinit(alloc);

    const neighbors = try graph_reader.neighborsAlloc(alloc, &session, req);
    defer graph_reader.freeNeighbors(alloc, neighbors);

    try std.testing.expectEqual(@as(usize, 1), neighbors.len);
    try std.testing.expectEqualStrings("doc-z", neighbors[0].doc_id);
    try std.testing.expectEqualStrings("rel", neighbors[0].edge_type);
}

test "query runtime warming keeps artifact available through cache" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-cache");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-cache");
    const cache_root = tmpPath(&cache_root_buf, "cache");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var cache = try cache_mod.QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    var artifact = try artifact_store.put("warm-me");
    defer artifact.deinit(alloc);

    var manifest = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{ .document_count = 1, .text_segment_count = 1, .vector_segment_count = 0 },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .text_segment,
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
    try manifest_store.put(manifest);
    try manifest_store.setHead("docs", 1);

    var runtime = QueryRuntime.initWithCache(alloc, &artifact_store, &manifest_store, &progress_store, &cache);
    defer runtime.deinit();
    var session = try runtime.openHeadSession("docs");
    defer session.deinit();

    try session.warmArtifactKind(.text_segment);
    try artifact_store.delete(artifact.artifact_id);

    const cached = try session.fetchArtifactAlloc(0);
    defer alloc.free(cached);
    try std.testing.expectEqualStrings("warm-me", cached);
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn applyModeCount(metrics: *QueryExecutionMetrics, mode: request_mod.QueryMode) void {
    switch (mode) {
        .vector => metrics.vector_queries += 1,
        .hybrid => metrics.hybrid_queries += 1,
        .sparse => metrics.sparse_queries += 1,
        .text => {},
    }
}

test "query runtime block range fetch uses cache after artifact deletion" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-block-cache");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-block-cache");
    const cache_root = tmpPath(&cache_root_buf, "cache-block");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var cache = try cache_mod.QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    var artifact = try artifact_store.put("abcdefgh");
    defer artifact.deinit(alloc);

    var manifest = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{ .document_count = 1, .text_segment_count = 0, .vector_segment_count = 1 },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .vector_segment,
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
    try manifest_store.put(manifest);
    try manifest_store.setHead("docs", 1);

    var runtime = QueryRuntime.initWithCache(alloc, &artifact_store, &manifest_store, &progress_store, &cache);
    defer runtime.deinit();
    var session = try runtime.openHeadSession("docs");
    defer session.deinit();

    const first = try session.fetchArtifactBlockRangeAlloc(0, "vector-header", 2, 3);
    defer alloc.free(first);
    try std.testing.expectEqualStrings("cde", first);

    try artifact_store.delete(artifact.artifact_id);

    const second = try session.fetchArtifactBlockRangeAlloc(0, "vector-header", 2, 3);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("cde", second);
}

test "serverless query session propagates cancellation through full and cached range transports" {
    const alloc = std.testing.allocator;
    const State = struct {
        cancelled: *std.atomic.Value(bool),
        full_calls: usize = 0,
        range_calls: usize = 0,

        fn deinit(_: Allocator, _: *anyopaque) void {}

        fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedPut;
        }

        fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return error.NonCancellableFullReadUsed;
        }

        fn getAllocWithCancellation(ptr: *anyopaque, _: Allocator, _: []const u8, cancellation: CancellationToken) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.full_calls += 1;
            self.cancelled.store(true, .release);
            try cancellation.check();
            return error.ExpectedCancellation;
        }

        fn getRangeAlloc(_: *anyopaque, _: Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            return error.NonCancellableRangeReadUsed;
        }

        fn getRangeAllocWithCancellation(ptr: *anyopaque, _: Allocator, _: []const u8, _: u64, _: usize, cancellation: CancellationToken) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.range_calls += 1;
            self.cancelled.store(true, .release);
            try cancellation.check();
            return error.ExpectedCancellation;
        }

        fn stat(_: *anyopaque, result_alloc: Allocator, artifact_id: []const u8) !artifacts_mod.ArtifactMetadata {
            const owned_id = try result_alloc.dupe(u8, artifact_id);
            errdefer result_alloc.free(owned_id);
            const checksum = try result_alloc.dupe(u8, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
            errdefer result_alloc.free(checksum);
            return .{
                .artifact_id = owned_id,
                .byte_len = 7,
                .checksum = checksum,
            };
        }

        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnexpectedDelete;
        }

        const vtable = artifacts_mod.ArtifactStore.VTable{
            .deinit = deinit,
            .put = put,
            .get_alloc = getAlloc,
            .get_alloc_with_cancellation = getAllocWithCancellation,
            .get_range_alloc = getRangeAlloc,
            .get_range_alloc_with_cancellation = getRangeAllocWithCancellation,
            .stat = stat,
            .delete = delete,
        };
    };

    var cancelled = std.atomic.Value(bool).init(false);
    var state = State{ .cancelled = &cancelled };
    var artifact_store = artifacts_mod.ArtifactStore{
        .allocator = alloc,
        .ptr = &state,
        .vtable = &State.vtable,
    };
    defer artifact_store.deinit();
    var refs = [_]manifest_mod.ArtifactRef{.{
        .kind = .vector_segment,
        .artifact_id = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        .byte_len = 7,
        .checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    }};
    var session = QuerySession{
        .alloc = alloc,
        .artifacts = &artifact_store,
        .manifest = .{
            .namespace = "docs",
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 1,
            .wal_end_lsn = 1,
            .stats = .{},
            .artifacts = &refs,
        },
        .cancellation = CancellationToken.fromAtomic(&cancelled),
    };

    try std.testing.expectError(error.Canceled, session.fetchArtifactAlloc(0));
    try std.testing.expectEqual(@as(usize, 0), state.full_calls);
    try std.testing.expectEqual(@as(usize, 1), state.range_calls);

    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "transport-cancellation-cache");
    defer cleanupTmp(cache_root);
    var cache = try cache_mod.QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();
    session.cache = &cache;
    cancelled.store(false, .release);
    try std.testing.expectError(error.Canceled, session.fetchArtifactBlockRangeAlloc(0, "vector-header", 0, 7));
    try std.testing.expectEqual(@as(usize, 2), state.range_calls);
}

test "query runtime tracks namespace-scoped search metrics" {
    const alloc = std.testing.allocator;

    var runtime = QueryRuntime.init(alloc, undefined, undefined, undefined);
    defer runtime.deinit();

    try runtime.recordSearchStats("docs-a", .vector, .{
        .actual_probe_count = 3,
        .actual_shortlist_count = 8,
        .quantized_candidate_count = 12,
        .exact_rerank_count = 4,
        .cluster_prune_count = 2,
    });
    try runtime.recordSearchStats("docs-a", .hybrid, .{
        .actual_probe_count = 2,
        .actual_shortlist_count = 6,
        .quantized_candidate_count = 9,
        .exact_rerank_count = 3,
        .cluster_prune_count = 1,
    });
    try runtime.recordSearchStats("docs-b", .sparse, .{
        .actual_probe_count = 0,
        .actual_shortlist_count = 0,
        .quantized_candidate_count = 0,
        .exact_rerank_count = 0,
        .cluster_prune_count = 0,
    });

    const global = runtime.metricsSnapshot();
    try std.testing.expectEqual(@as(u64, 3), global.total_queries);
    try std.testing.expectEqual(@as(u64, 1), global.vector_queries);
    try std.testing.expectEqual(@as(u64, 1), global.hybrid_queries);
    try std.testing.expectEqual(@as(u64, 1), global.sparse_queries);

    const namespace_metrics = try runtime.namespaceMetricsAlloc(alloc);
    defer {
        for (namespace_metrics) |*metric| metric.deinit(alloc);
        alloc.free(namespace_metrics);
    }
    try std.testing.expectEqual(@as(usize, 2), namespace_metrics.len);

    var docs_a: ?NamespaceQueryExecutionMetrics = null;
    var docs_b: ?NamespaceQueryExecutionMetrics = null;
    for (namespace_metrics) |metric| {
        if (std.mem.eql(u8, metric.namespace, "docs-a")) docs_a = metric;
        if (std.mem.eql(u8, metric.namespace, "docs-b")) docs_b = metric;
    }
    try std.testing.expect(docs_a != null);
    try std.testing.expect(docs_b != null);
    try std.testing.expectEqual(@as(u64, 2), docs_a.?.metrics.total_queries);
    try std.testing.expectEqual(@as(u64, 1), docs_a.?.metrics.vector_queries);
    try std.testing.expectEqual(@as(u64, 1), docs_a.?.metrics.hybrid_queries);
    try std.testing.expectEqual(@as(u64, 5), docs_a.?.metrics.total_actual_probes);
    try std.testing.expectEqual(@as(u64, 1), docs_b.?.metrics.total_queries);
    try std.testing.expectEqual(@as(u64, 1), docs_b.?.metrics.sparse_queries);
}

test "serverless query runtime metrics remain valid across every allocation failure" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var runtime = QueryRuntime.init(alloc, undefined, undefined, undefined);
            defer runtime.deinit();
            const stats = .{
                .actual_probe_count = @as(u32, 1),
                .actual_shortlist_count = @as(u32, 2),
                .quantized_candidate_count = @as(u32, 3),
                .exact_rerank_count = @as(u32, 4),
                .cluster_prune_count = @as(u32, 5),
            };
            try runtime.recordSearchStats("docs-a", .vector, stats);
            try runtime.recordSearchStats("docs-b", .hybrid, stats);
            const snapshot = try runtime.namespaceMetricsAlloc(alloc);
            defer {
                for (snapshot) |*entry| entry.deinit(alloc);
                alloc.free(snapshot);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "serverless query session validates content addresses and declared range bounds" {
    const alloc = std.testing.allocator;
    var refs = [_]manifest_mod.ArtifactRef{.{
        .kind = .vector_segment,
        .artifact_id = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        .byte_len = 8,
        .checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    }};
    var session = QuerySession{
        .alloc = alloc,
        .artifacts = undefined,
        .manifest = .{
            .namespace = "docs",
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 1,
            .wal_end_lsn = 1,
            .stats = .{},
            .artifacts = &refs,
        },
    };
    try std.testing.expectError(error.InvalidArtifactRange, session.fetchArtifactRangeAlloc(0, 7, 2));
    try std.testing.expectError(error.InvalidArtifactRange, session.fetchArtifactBlockRangeAlloc(0, "vector-header", std.math.maxInt(u64), 2));
    refs[0].artifact_id = "../../outside-cache";
    try std.testing.expectError(error.ArtifactIntegrityMismatch, session.fetchArtifactRangeAlloc(0, 0, 1));
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-query-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
