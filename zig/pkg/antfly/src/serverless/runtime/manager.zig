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
const Allocator = std.mem.Allocator;
const api_mod = @import("../api/mod.zig");
const build_mod = @import("../build/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const enrichment_mod = @import("../enrichment/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");

pub const RuntimeConfig = struct {
    tick_interval_ms: u64 = 25,
    role: api_mod.RuntimeRole = .combined,
    publish_enabled: bool = true,
    compaction_enabled: bool = true,
    prune_enabled: bool = true,
    enrichment_enabled: bool = true,
};

pub const RuntimeRunStats = struct {
    published_namespaces: usize = 0,
    publish_head_conflicts: usize = 0,
    compacted_namespaces: usize = 0,
    compact_head_conflicts: usize = 0,
    pruned_namespaces: usize = 0,
    prune_gc_conflicts: usize = 0,
    deleted_versions: usize = 0,
    deleted_artifacts: usize = 0,
    wal_records_removed: u64 = 0,
    enriched_namespaces: usize = 0,
    enriched_documents: usize = 0,
    enrichment_wal_appends: usize = 0,
    enrichment_model_documents: usize = 0,
    enrichment_fallback_documents: usize = 0,
    enrichment_failed_documents: usize = 0,
    enrichment_stage_failures: usize = 0,
};

pub const ManagedRuntime = struct {
    alloc: Allocator,
    cfg: RuntimeConfig,
    catalog: *catalog_mod.CatalogService,
    publisher: build_mod.BackgroundPublisher,
    compactor: ?build_mod.Compactor = null,
    enricher: ?enrichment_mod.SparseEnricher = null,
    pruner: build_mod.Pruner,
    stats_mu: std.atomic.Mutex = .unlocked,
    run_mu: std.atomic.Mutex = .unlocked,
    cumulative_stats: RuntimeRunStats = .{},
    thread: ?std.Thread = null,
    stop_requested: std.atomic.Value(bool) = .init(false),

    pub fn init(
        alloc: Allocator,
        cfg: RuntimeConfig,
        catalog: *catalog_mod.CatalogService,
        pruner: build_mod.Pruner,
    ) ManagedRuntime {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .catalog = catalog,
            .publisher = build_mod.BackgroundPublisher.init(alloc, catalog, cfg.tick_interval_ms),
            .pruner = pruner,
        };
    }

    pub fn deinit(self: *ManagedRuntime) void {
        self.stop();
        if (self.enricher) |*enricher| enricher.deinit();
        self.publisher.deinit();
        self.* = undefined;
    }

    pub fn start(self: *ManagedRuntime) !void {
        if (self.thread != null) return error.AlreadyStarted;
        if (self.cfg.role == .query_only or self.cfg.role == .api_only) return;
        self.stop_requested.store(false, .monotonic);
        self.thread = try std.Thread.spawn(.{}, runLoop, .{self});
    }

    pub fn stop(self: *ManagedRuntime) void {
        self.stop_requested.store(true, .monotonic);
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    pub fn runOnce(self: *ManagedRuntime) !RuntimeRunStats {
        if (self.cfg.role == .query_only or self.cfg.role == .api_only) return RuntimeRunStats{};
        lockAtomic(&self.run_mu);
        defer self.run_mu.unlock();

        var stats = RuntimeRunStats{};
        if (self.cfg.publish_enabled) {
            const publish_stats = try self.publisher.runOnce();
            stats.published_namespaces = publish_stats.published_namespaces;
            stats.publish_head_conflicts = publish_stats.head_conflicts;
        }

        const namespaces = try self.catalog.listNamespacesAlloc(self.alloc);
        defer self.catalog.freeNamespaces(self.alloc, namespaces);

        for (namespaces) |namespace| {
            const policy = self.catalog.getPolicy(namespace.name) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            var status = self.catalog.buildStatus(namespace.name) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            defer status.deinit(self.alloc);
            var effective_policy = policy;
            effective_policy.enrichment_enabled = status.enrichment_enabled;
            effective_policy.chunk_preview_enabled = status.chunk_preview_enabled;
            effective_policy.chunk_embeddings_enabled = status.chunk_embeddings_enabled;
            effective_policy.rerank_terms_enabled = status.rerank_terms_enabled;

            if (self.enricher) |*enricher| {
                const maybe_table_record = self.catalog.getTableForNamespaceAlloc(self.alloc, namespace.name) catch |err| switch (err) {
                    error.FileNotFound => null,
                    else => return err,
                };
                if (maybe_table_record) |table_record| {
                    var table = table_record;
                    defer table.deinit(self.alloc);

                    if (try managed_embedder.ManagedEmbedder.createSparseEmbedder(self.alloc, table.indexes_json)) |sparse_embedder| {
                        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, table.indexes_json, .{});
                        defer parsed.deinit();
                        const sparse_name = firstSparseIndexNameFromIndexesJson(parsed.value) orelse "serverless_sparse";
                        try enricher.setSparseEmbedder(sparse_embedder, sparse_name);
                    } else {
                        enricher.clearSparseEmbedder();
                    }

                    if (try managed_embedder.ManagedEmbedder.createDenseEmbedder(self.alloc, table.indexes_json)) |dense_embedder| {
                        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, table.indexes_json, .{});
                        defer parsed.deinit();
                        const dims = denseDimsFromIndexesJson(parsed.value) orelse 8;
                        const dense_name = firstDenseIndexNameFromIndexesJson(parsed.value) orelse "serverless_chunk";
                        try enricher.setChunkEmbedder(dense_embedder, dense_name, dims);
                    } else {
                        enricher.clearChunkEmbedder();
                    }
                }

                if (self.cfg.enrichment_enabled and status.enrichment_active_stage != null) {
                    const stage_spec = enrichment_mod.builtinPipelineForPolicy(effective_policy).stageSpec(status.enrichment_active_stage.?) orelse continue;
                    const enrichment = enricher.runNamespaceWithConfig(namespace.name, .{
                        .batch_size = policy.enrichment_batch_size,
                        .pipeline_version = stage_spec.pipeline_version,
                        .stage = status.enrichment_active_stage.?,
                        .model_preference = stage_spec.model_preference,
                        .failure_policy = policy.enrichment_failure_policy,
                    }) catch |err| switch (err) {
                        error.FileNotFound => continue,
                        else => return err,
                    };
                    stats.enriched_namespaces += enrichment.enriched_namespaces;
                    stats.enriched_documents += enrichment.enriched_documents;
                    stats.enrichment_wal_appends += enrichment.wal_appends;
                    stats.enrichment_model_documents += enrichment.model_documents;
                    stats.enrichment_fallback_documents += enrichment.fallback_documents;
                    stats.enrichment_failed_documents += enrichment.failed_documents;
                    stats.enrichment_stage_failures += enrichment.stage_failures;
                }
            }

            if (self.compactor) |*compactor| {
                if (self.cfg.compaction_enabled) {
                    if (status.compaction_recommended) {
                        var compacted = compactor.compactHead(namespace.name) catch |err| switch (err) {
                            error.HeadChanged => {
                                stats.compact_head_conflicts += 1;
                                continue;
                            },
                            error.FileNotFound => continue,
                            else => return err,
                        };
                        defer compacted.deinit(self.alloc);
                        if (compacted.published) stats.compacted_namespaces += 1;
                    }
                }
            }

            if (self.cfg.prune_enabled) {
                var result = self.pruner.pruneNamespace(namespace.name, policy.keep_latest_versions) catch |err| switch (err) {
                    error.FileNotFound => continue,
                    else => return err,
                };
                defer result.deinit(self.alloc);
                if (result.gc_watermark_conflict) stats.prune_gc_conflicts += 1;
                if (result.deleted_versions == 0 and result.wal_records_removed == 0) continue;
                stats.pruned_namespaces += 1;
                stats.deleted_versions += result.deleted_versions;
                stats.deleted_artifacts += result.deleted_artifacts;
                stats.wal_records_removed += result.wal_records_removed;
            }
        }
        self.recordStats(stats);
        return stats;
    }

    pub fn metricsSnapshot(self: *ManagedRuntime) RuntimeRunStats {
        lockAtomic(&self.stats_mu);
        defer self.stats_mu.unlock();
        return self.cumulative_stats;
    }

    pub fn setCompactor(self: *ManagedRuntime, compactor: build_mod.Compactor) void {
        self.compactor = compactor;
    }

    pub fn setEnricher(self: *ManagedRuntime, enricher: enrichment_mod.SparseEnricher) void {
        self.enricher = enricher;
    }

    fn recordStats(self: *ManagedRuntime, stats: RuntimeRunStats) void {
        lockAtomic(&self.stats_mu);
        defer self.stats_mu.unlock();
        self.cumulative_stats.published_namespaces += stats.published_namespaces;
        self.cumulative_stats.publish_head_conflicts += stats.publish_head_conflicts;
        self.cumulative_stats.compacted_namespaces += stats.compacted_namespaces;
        self.cumulative_stats.compact_head_conflicts += stats.compact_head_conflicts;
        self.cumulative_stats.pruned_namespaces += stats.pruned_namespaces;
        self.cumulative_stats.prune_gc_conflicts += stats.prune_gc_conflicts;
        self.cumulative_stats.deleted_versions += stats.deleted_versions;
        self.cumulative_stats.deleted_artifacts += stats.deleted_artifacts;
        self.cumulative_stats.wal_records_removed += stats.wal_records_removed;
        self.cumulative_stats.enriched_namespaces += stats.enriched_namespaces;
        self.cumulative_stats.enriched_documents += stats.enriched_documents;
        self.cumulative_stats.enrichment_wal_appends += stats.enrichment_wal_appends;
        self.cumulative_stats.enrichment_model_documents += stats.enrichment_model_documents;
        self.cumulative_stats.enrichment_fallback_documents += stats.enrichment_fallback_documents;
        self.cumulative_stats.enrichment_failed_documents += stats.enrichment_failed_documents;
        self.cumulative_stats.enrichment_stage_failures += stats.enrichment_stage_failures;
    }

    fn runLoop(self: *ManagedRuntime) void {
        while (!self.stop_requested.load(.monotonic)) {
            _ = self.runOnce() catch RuntimeRunStats{};
            sleepMs(@max(self.cfg.tick_interval_ms, 1));
        }
    }
};

fn denseDimsFromIndexesJson(root: std.json.Value) ?u32 {
    if (root != .object) return null;
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const config = entry.value_ptr.object;
        const type_value = config.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;
        if (config.get("sparse")) |sparse_value| {
            if (sparse_value == .bool and sparse_value.bool) continue;
        }
        const dims_value = config.get("dimension") orelse config.get("dims") orelse continue;
        return switch (dims_value) {
            .integer => std.math.cast(u32, dims_value.integer),
            .float => std.math.lossyCast(u32, dims_value.float),
            else => null,
        };
    }
    return null;
}

fn firstDenseIndexNameFromIndexesJson(root: std.json.Value) ?[]const u8 {
    if (root != .object) return null;
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const config = entry.value_ptr.object;
        const type_value = config.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;
        if (config.get("sparse")) |sparse_value| {
            if (sparse_value == .bool and sparse_value.bool) continue;
        }
        return entry.key_ptr.*;
    }
    return null;
}

fn firstSparseIndexNameFromIndexesJson(root: std.json.Value) ?[]const u8 {
    if (root != .object) return null;
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const config = entry.value_ptr.object;
        const type_value = config.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;
        if (config.get("sparse")) |sparse_value| {
            if (sparse_value == .bool and sparse_value.bool) return entry.key_ptr.*;
        }
    }
    return null;
}

test "managed runtime publishes and prunes based on namespace policy" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .default_query_view = .latest,
        .keep_latest_versions = 1,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch_a = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest_a = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch_a });
    defer ingest_a.deinit(alloc);
    var build_a = try builder.publishNamespace("docs");
    defer build_a.deinit(alloc);

    const batch_b = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var ingest_b = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &batch_b });
    defer ingest_b.deinit(alloc);
    var build_b = try builder.publishNamespace("docs");
    defer build_b.deinit(alloc);

    const batch_c = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };
    var ingest_c = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 300, .mutations = &batch_c });
    defer ingest_c.deinit(alloc);

    var runtime = ManagedRuntime.init(alloc, .{ .tick_interval_ms = 1 }, &catalog, build_mod.Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store));
    defer runtime.deinit();

    const stats = try runtime.runOnce();
    try std.testing.expectEqual(@as(usize, 1), stats.published_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.pruned_namespaces);
    try std.testing.expectEqual(@as(usize, 2), stats.deleted_versions);
    try std.testing.expectEqual(@as(usize, 6), stats.deleted_artifacts);
    try std.testing.expectEqual(@as(u64, 3), try progress_store.getHead("docs"));

    const versions = try manifest_store.listVersionsAlloc("docs");
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u64, &.{3}, versions);

    const cumulative = runtime.metricsSnapshot();
    try std.testing.expectEqual(stats.published_namespaces, cumulative.published_namespaces);
    try std.testing.expectEqual(stats.pruned_namespaces, cumulative.pruned_namespaces);
}

test "managed runtime query-only role skips maintenance work" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-query-only");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-query-only");
    const wal_root = tmpPath(&wal_root_buf, "wal-query-only");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-query-only");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespace("docs", 100));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &batch });
    defer ingest.deinit(alloc);

    var runtime = ManagedRuntime.init(alloc, .{
        .tick_interval_ms = 1,
        .role = .query_only,
    }, &catalog, build_mod.Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store));
    defer runtime.deinit();

    const stats = try runtime.runOnce();
    try std.testing.expectEqual(@as(usize, 0), stats.published_namespaces);
    try std.testing.expectError(error.FileNotFound, progress_store.getHead("docs"));
}

test "managed runtime api-only role skips maintenance work" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-api-only");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-api-only");
    const wal_root = tmpPath(&wal_root_buf, "wal-api-only");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-api-only");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespace("docs", 100));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &batch });
    defer ingest.deinit(alloc);

    var runtime = ManagedRuntime.init(alloc, .{
        .tick_interval_ms = 1,
        .role = .api_only,
    }, &catalog, build_mod.Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store));
    defer runtime.deinit();

    try runtime.start();
    defer runtime.stop();
    try std.testing.expect(runtime.thread == null);

    const stats = try runtime.runOnce();
    try std.testing.expectEqual(@as(usize, 0), stats.published_namespaces);
    try std.testing.expectError(error.FileNotFound, progress_store.getHead("docs"));
}

test "managed runtime honors maintenance feature flags" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-feature-flags");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-feature-flags");
    const wal_root = tmpPath(&wal_root_buf, "wal-feature-flags");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-feature-flags");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespace("docs", 100));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &batch });
    defer ingest.deinit(alloc);

    var runtime = ManagedRuntime.init(alloc, .{
        .tick_interval_ms = 1,
        .publish_enabled = false,
        .compaction_enabled = false,
        .prune_enabled = false,
        .enrichment_enabled = false,
    }, &catalog, build_mod.Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store));
    defer runtime.deinit();

    const stats = try runtime.runOnce();
    try std.testing.expectEqual(@as(usize, 0), stats.published_namespaces);
    try std.testing.expectEqual(@as(usize, 0), stats.compacted_namespaces);
    try std.testing.expectEqual(@as(usize, 0), stats.pruned_namespaces);
    try std.testing.expectEqual(@as(usize, 0), stats.enriched_namespaces);
    try std.testing.expectError(error.FileNotFound, progress_store.getHead("docs"));
}

test "managed runtime compacts head when namespace exceeds compaction threshold" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compact");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compact");
    const wal_root = tmpPath(&wal_root_buf, "wal-compact");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-compact");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .keep_latest_versions = 4,
        .compaction_trigger_version_count = 2,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha beta" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try builder.publishNamespace("docs");
    defer build_first.deinit(alloc);

    const second = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta gamma" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var build_second = try builder.publishNamespace("docs");
    defer build_second.deinit(alloc);

    var runtime = ManagedRuntime.init(alloc, .{ .tick_interval_ms = 1 }, &catalog, build_mod.Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store));
    runtime.setCompactor(build_mod.Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store));
    defer runtime.deinit();

    const stats = try runtime.runOnce();
    try std.testing.expectEqual(@as(usize, 1), stats.compacted_namespaces);
    try std.testing.expectEqual(@as(u64, 3), try progress_store.getHead("docs"));

    var compacted = try manifest_store.getAlloc("docs", 3);
    defer compacted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), compacted.artifacts.len);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.document_segment, compacted.artifacts[0].kind);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.text_segment, compacted.artifacts[1].kind);
}

test "managed runtime runs sparse enrichment for opted-in namespaces" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-enrich");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-enrich");
    const wal_root = tmpPath(&wal_root_buf, "wal-enrich");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-enrich");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .enrichment_enabled = true,
        .keep_latest_versions = 2,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo alpha\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var runtime = ManagedRuntime.init(alloc, .{ .tick_interval_ms = 1 }, &catalog, build_mod.Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store));
    runtime.setEnricher(enrichment_mod.SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store));
    defer runtime.deinit();

    const stats = try runtime.runOnce();
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), stats.enrichment_wal_appends);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer @import("../wal/mod.zig").freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-runtime-{s}-{d}-{d}\x00", .{
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

fn sleepMs(ms: u64) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromMilliseconds(@intCast(if (ms == 0) @as(u64, 1) else ms)),
    }, io_impl.io()) catch {};
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}
