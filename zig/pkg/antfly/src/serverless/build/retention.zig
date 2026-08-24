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
const artifacts_mod = @import("../artifacts/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const maintenance_cancellation = @import("../maintenance_cancellation.zig");
const objectstore = @import("objectstore");
const artifacts_object_store = @import("../artifacts/object_store.zig");
const manifest_object_store = @import("../manifest/object_store.zig");
const progress_object_store = @import("../catalog/object_progress_store.zig");
const wal_object_store = @import("../wal/object_store.zig");

pub const PruneResult = struct {
    namespace: []u8,
    kept_versions: usize,
    deleted_versions: usize,
    deleted_artifacts: usize,
    wal_keep_from_lsn: u64,
    wal_records_removed: u64,
    gc_watermark_conflict: bool = false,

    pub fn deinit(self: *PruneResult, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const Pruner = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,
    wal: *wal_mod.WalStore,

    pub fn init(alloc: Allocator, artifacts: *artifacts_mod.ArtifactStore, manifests: *manifest_mod.ManifestStore, progress: *catalog_mod.ProgressStore, wal: *wal_mod.WalStore) Pruner {
        return .{
            .alloc = alloc,
            .artifacts = artifacts,
            .manifests = manifests,
            .progress = progress,
            .wal = wal,
        };
    }

    pub fn pruneNamespace(self: *Pruner, namespace: []const u8, keep_latest_versions: usize) !PruneResult {
        return try self.pruneNamespaceUntil(namespace, keep_latest_versions, null);
    }

    pub fn pruneNamespaceUntil(
        self: *Pruner,
        namespace: []const u8,
        keep_latest_versions: usize,
        cancellation: ?maintenance_cancellation.Token,
    ) !PruneResult {
        try maintenance_cancellation.check(cancellation);
        const versions = try self.manifests.listVersionsAlloc(namespace);
        defer self.alloc.free(versions);

        if (versions.len == 0) {
            return .{
                .namespace = try self.alloc.dupe(u8, namespace),
                .kept_versions = 0,
                .deleted_versions = 0,
                .deleted_artifacts = 0,
                .wal_keep_from_lsn = 0,
                .wal_records_removed = 0,
            };
        }

        // Object stores can contain manifests whose publisher never completed
        // the HEAD CAS. Retention is defined relative to the published head,
        // never the numerically greatest stored version.
        const published_head = try self.progress.getHead(namespace);
        if (!containsVersion(versions, published_head)) {
            return error.PublishedHeadManifestMissing;
        }
        const keep_count = @max(keep_latest_versions, 1);
        var kept_versions = std.AutoHashMapUnmanaged(u64, void).empty;
        defer kept_versions.deinit(self.alloc);
        var lineage_version: ?u64 = published_head;
        var wal_keep_from_lsn: u64 = 0;
        while (lineage_version != null and kept_versions.count() < keep_count) {
            try maintenance_cancellation.check(cancellation);
            const version = lineage_version.?;
            if (!containsVersion(versions, version)) return error.PublishedLineageManifestMissing;
            if (kept_versions.contains(version)) return error.PublishedLineageCycle;

            var manifest = try self.manifests.getAlloc(namespace, version);
            defer manifest.deinit(self.alloc);
            try kept_versions.put(self.alloc, version, {});
            wal_keep_from_lsn = manifest.wal_start_lsn;
            lineage_version = if (manifest.publication_lineage_tracked)
                manifest.publication_parent_version
            else
                previousStoredVersion(versions, version);
        }

        const current_gc = try self.progress.getGcWatermark(namespace);
        const effective_keep_from = if (current_gc) |value| @max(value, wal_keep_from_lsn) else wal_keep_from_lsn;
        if (current_gc == null) {
            const advanced = try self.progress.compareAndSwapGcWatermark(namespace, null, effective_keep_from);
            if (!advanced) return try self.noopResult(namespace, kept_versions.count(), true);
        } else if (current_gc.? < effective_keep_from) {
            const advanced = try self.progress.compareAndSwapGcWatermark(namespace, current_gc, effective_keep_from);
            if (!advanced) return try self.noopResult(namespace, kept_versions.count(), true);
        }

        var retained_artifacts = std.StringHashMapUnmanaged(void).empty;
        defer freeOwnedKeys(self.alloc, &retained_artifacts);
        for (versions) |version| {
            // Preserve artifacts reachable from the visible publication
            // lineage and from every above-HEAD candidate. The latter may be
            // an in-flight publisher or a crashed orphan and retention must
            // not break its content-addressed references while leaving its
            // immutable manifest behind.
            if (version <= published_head and !kept_versions.contains(version)) continue;
            try maintenance_cancellation.check(cancellation);
            var manifest = try self.manifests.getAlloc(namespace, version);
            defer manifest.deinit(self.alloc);
            try collectArtifactIds(self.alloc, &retained_artifacts, manifest.artifacts);
        }

        var pruned_artifacts = std.StringHashMapUnmanaged(void).empty;
        defer freeOwnedKeys(self.alloc, &pruned_artifacts);
        for (versions) |version| {
            if (version > published_head or kept_versions.contains(version)) continue;
            try maintenance_cancellation.check(cancellation);
            var manifest = try self.manifests.getAlloc(namespace, version);
            defer manifest.deinit(self.alloc);
            try collectUnretainedArtifactIds(self.alloc, &pruned_artifacts, retained_artifacts, manifest.artifacts);
        }

        // Delete content before its obsolete manifests. If cancellation lands
        // mid-sweep, the manifests let the next pass rediscover every remaining
        // artifact instead of leaking unreachable objects forever.
        var deleted_artifacts: usize = 0;
        var artifact_it = pruned_artifacts.iterator();
        while (artifact_it.next()) |entry| {
            try maintenance_cancellation.check(cancellation);
            self.artifacts.delete(entry.key_ptr.*) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            deleted_artifacts += 1;
        }

        var deleted_versions: usize = 0;
        for (versions) |version| {
            if (version > published_head or kept_versions.contains(version)) continue;
            try maintenance_cancellation.check(cancellation);
            // Retire per-head enrichment progress before its manifest. The
            // progress stores leave a same-key tombstone so a worker that
            // loaded this manifest before retention cannot recreate progress.
            inline for (std.meta.tags(catalog_mod.EnrichmentStage)) |stage| {
                try self.progress.deleteEnrichmentStageHeadDocOffset(namespace, stage, version);
            }
            try self.manifests.deleteVersion(namespace, version);
            deleted_versions += 1;
        }

        try maintenance_cancellation.check(cancellation);
        const wal_records_removed = try self.wal.truncatePrefix(namespace, effective_keep_from);
        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .kept_versions = kept_versions.count(),
            .deleted_versions = deleted_versions,
            .deleted_artifacts = deleted_artifacts,
            .wal_keep_from_lsn = effective_keep_from,
            .wal_records_removed = wal_records_removed,
        };
    }

    fn noopResult(self: *Pruner, namespace: []const u8, kept_versions: usize, gc_watermark_conflict: bool) !PruneResult {
        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .kept_versions = kept_versions,
            .deleted_versions = 0,
            .deleted_artifacts = 0,
            .wal_keep_from_lsn = 0,
            .wal_records_removed = 0,
            .gc_watermark_conflict = gc_watermark_conflict,
        };
    }
};

fn containsVersion(versions: []const u64, target: u64) bool {
    for (versions) |version| {
        if (version == target) return true;
        if (version > target) return false;
    }
    return false;
}

fn previousStoredVersion(versions: []const u64, target: u64) ?u64 {
    var previous: ?u64 = null;
    for (versions) |version| {
        if (version >= target) return previous;
        previous = version;
    }
    return previous;
}

fn collectArtifactIds(
    alloc: Allocator,
    dst: *std.StringHashMapUnmanaged(void),
    artifacts: []const manifest_mod.ArtifactRef,
) !void {
    for (artifacts) |artifact| {
        if (dst.contains(artifact.artifact_id)) continue;
        const owned_id = try alloc.dupe(u8, artifact.artifact_id);
        errdefer alloc.free(owned_id);
        try dst.put(alloc, owned_id, {});
    }
}

fn collectUnretainedArtifactIds(
    alloc: Allocator,
    dst: *std.StringHashMapUnmanaged(void),
    retained: std.StringHashMapUnmanaged(void),
    artifacts: []const manifest_mod.ArtifactRef,
) !void {
    for (artifacts) |artifact| {
        if (retained.contains(artifact.artifact_id) or dst.contains(artifact.artifact_id)) continue;
        const owned_id = try alloc.dupe(u8, artifact.artifact_id);
        errdefer alloc.free(owned_id);
        try dst.put(alloc, owned_id, {});
    }
}

fn freeOwnedKeys(alloc: Allocator, map: *std.StringHashMapUnmanaged(void)) void {
    var it = map.iterator();
    while (it.next()) |entry| alloc.free(entry.key_ptr.*);
    map.deinit(alloc);
}

fn putTestManifest(
    manifests: *manifest_mod.ManifestStore,
    version: u64,
    wal_start_lsn: u64,
    artifact: artifacts_mod.ArtifactMetadata,
) !void {
    return try putTestManifestWithLineage(manifests, version, wal_start_lsn, artifact, false, null);
}

fn putTestManifestWithLineage(
    manifests: *manifest_mod.ManifestStore,
    version: u64,
    wal_start_lsn: u64,
    artifact: artifacts_mod.ArtifactMetadata,
    lineage_tracked: bool,
    parent_version: ?u64,
) !void {
    var refs = [_]manifest_mod.ArtifactRef{.{
        .kind = .document_segment,
        .artifact_id = artifact.artifact_id,
        .byte_len = artifact.byte_len,
        .checksum = artifact.checksum,
    }};
    try manifests.put(.{
        .namespace = "docs",
        .version = version,
        .built_at_ns = version,
        .wal_start_lsn = wal_start_lsn,
        .wal_end_lsn = wal_start_lsn,
        .publication_lineage_tracked = lineage_tracked,
        .publication_parent_version = parent_version,
        .stats = .{ .document_count = 1, .document_base_version = version },
        .artifacts = &refs,
    });
}

test "serverless retention preserves shared artifacts referenced above head" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();

    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(alloc, memory.client(), "artifacts-shared-orphan", "tenant");
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(alloc, memory.client(), "manifests-shared-orphan", "tenant");
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(alloc, memory.client(), "progress-shared-orphan", "tenant");
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var wal_impl = try wal_object_store.ObjectStore.initWithClient(alloc, memory.client(), "wal-shared-orphan", "tenant");
    var wal = wal_impl.walStore();
    defer wal.deinit();

    var shared_artifact = try artifacts.put("shared");
    defer shared_artifact.deinit(alloc);
    var head_artifact = try artifacts.put("head");
    defer head_artifact.deinit(alloc);
    try putTestManifest(&manifests, 1, 1, shared_artifact);
    try putTestManifest(&manifests, 2, 2, head_artifact);
    try putTestManifest(&manifests, 3, 3, shared_artifact);
    try std.testing.expect(try progress.compareAndSwapHead("docs", null, 2));

    var pruner = Pruner.init(alloc, &artifacts, &manifests, &progress, &wal);
    var result = try pruner.pruneNamespace("docs", 1);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.deleted_versions);
    try std.testing.expectEqual(@as(usize, 0), result.deleted_artifacts);
    const shared = try artifacts.getAlloc(shared_artifact.artifact_id);
    defer alloc.free(shared);
    try std.testing.expectEqualStrings("shared", shared);
}

test "serverless retention follows publication lineage around lower orphan" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();

    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(alloc, memory.client(), "artifacts-lineage", "tenant");
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(alloc, memory.client(), "manifests-lineage", "tenant");
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(alloc, memory.client(), "progress-lineage", "tenant");
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var wal_impl = try wal_object_store.ObjectStore.initWithClient(alloc, memory.client(), "wal-lineage", "tenant");
    var wal = wal_impl.walStore();
    defer wal.deinit();

    var first_artifact = try artifacts.put("first-visible");
    defer first_artifact.deinit(alloc);
    var orphan_artifact = try artifacts.put("lower-orphan");
    defer orphan_artifact.deinit(alloc);
    var head_artifact = try artifacts.put("new-head");
    defer head_artifact.deinit(alloc);
    try putTestManifestWithLineage(&manifests, 1, 1, first_artifact, true, null);
    try putTestManifestWithLineage(&manifests, 2, 2, orphan_artifact, true, 1);
    try putTestManifestWithLineage(&manifests, 3, 3, head_artifact, true, 1);
    try std.testing.expect(try progress.compareAndSwapHead("docs", null, 3));
    try std.testing.expect(try progress.compareAndSwapEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 1, null, 1));
    try std.testing.expect(try progress.compareAndSwapEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 2, null, 1));

    var pruner = Pruner.init(alloc, &artifacts, &manifests, &progress, &wal);
    var result = try pruner.pruneNamespace("docs", 2);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.kept_versions);
    try std.testing.expectEqual(@as(usize, 1), result.deleted_versions);
    const versions = try manifests.listVersionsAlloc("docs");
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u64, &.{ 1, 3 }, versions);
    try std.testing.expectEqual(@as(?u64, 1), try progress.getEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 1));
    try std.testing.expectEqual(@as(?u64, null), try progress.getEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 2));
    try std.testing.expect(!(try progress.compareAndSwapEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 2, null, 0)));
    try std.testing.expectError(error.FileNotFound, artifacts.getAlloc(orphan_artifact.artifact_id));
    const first = try artifacts.getAlloc(first_artifact.artifact_id);
    defer alloc.free(first);
    try std.testing.expectEqualStrings("first-visible", first);
}

test "serverless retention ignores unpublished manifests above the visible head" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();

    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(
        alloc,
        memory.client(),
        "artifacts",
        "tenant",
    );
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(
        alloc,
        memory.client(),
        "manifests",
        "tenant",
    );
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(
        alloc,
        memory.client(),
        "progress",
        "tenant",
    );
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var wal_impl = try wal_object_store.ObjectStore.initWithClient(
        alloc,
        memory.client(),
        "wal",
        "tenant",
    );
    var wal = wal_impl.walStore();
    defer wal.deinit();

    var old_artifact = try artifacts.put("old");
    defer old_artifact.deinit(alloc);
    var active_artifact = try artifacts.put("active");
    defer active_artifact.deinit(alloc);
    var orphan_artifact = try artifacts.put("orphan");
    defer orphan_artifact.deinit(alloc);
    try putTestManifest(&manifests, 1, 1, old_artifact);
    try putTestManifest(&manifests, 2, 2, active_artifact);
    try putTestManifest(&manifests, 3, 3, orphan_artifact);
    try std.testing.expect(try progress.compareAndSwapHead("docs", null, 2));

    var pruner = Pruner.init(alloc, &artifacts, &manifests, &progress, &wal);
    var result = try pruner.pruneNamespace("docs", 1);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.kept_versions);
    try std.testing.expectEqual(@as(usize, 1), result.deleted_versions);

    const active = try artifacts.getAlloc(active_artifact.artifact_id);
    defer alloc.free(active);
    try std.testing.expectEqualStrings("active", active);
    const orphan = try artifacts.getAlloc(orphan_artifact.artifact_id);
    defer alloc.free(orphan);
    try std.testing.expectEqualStrings("orphan", orphan);
    try std.testing.expectError(error.FileNotFound, artifacts.getAlloc(old_artifact.artifact_id));

    const versions = try manifests.listVersionsAlloc("docs");
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u64, &.{ 2, 3 }, versions);
}

test "serverless retention resumes artifact cleanup after cancellation" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();

    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(alloc, memory.client(), "artifacts-resume", "tenant");
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(alloc, memory.client(), "manifests-resume", "tenant");
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(alloc, memory.client(), "progress-resume", "tenant");
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var wal_impl = try wal_object_store.ObjectStore.initWithClient(alloc, memory.client(), "wal-resume", "tenant");
    var wal = wal_impl.walStore();
    defer wal.deinit();

    var first_artifact = try artifacts.put("first");
    defer first_artifact.deinit(alloc);
    var second_artifact = try artifacts.put("second");
    defer second_artifact.deinit(alloc);
    var head_artifact = try artifacts.put("head");
    defer head_artifact.deinit(alloc);
    try putTestManifest(&manifests, 1, 1, first_artifact);
    try putTestManifest(&manifests, 2, 2, second_artifact);
    try putTestManifest(&manifests, 3, 3, head_artifact);
    try std.testing.expect(try progress.compareAndSwapHead("docs", null, 3));

    var requested = std.atomic.Value(bool).init(false);
    const CancelAfterFirstDelete = struct {
        requested: *std.atomic.Value(bool),
        checkpoints: usize = 0,

        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.checkpoints += 1;
            if (self.checkpoints == 5) self.requested.store(true, .release);
        }
    };
    var cancel_state = CancelAfterFirstDelete{ .requested = &requested };
    const token = (maintenance_cancellation.Token{
        .io = std.testing.io,
        .requested = &requested,
    }).withCheckpoint(&cancel_state, CancelAfterFirstDelete.checkpoint);

    var pruner = Pruner.init(alloc, &artifacts, &manifests, &progress, &wal);
    try std.testing.expectError(error.Canceled, pruner.pruneNamespaceUntil("docs", 1, token));
    const interrupted_versions = try manifests.listVersionsAlloc("docs");
    defer alloc.free(interrupted_versions);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, interrupted_versions);

    var resumed = try pruner.pruneNamespace("docs", 1);
    defer resumed.deinit(alloc);
    const final_versions = try manifests.listVersionsAlloc("docs");
    defer alloc.free(final_versions);
    try std.testing.expectEqualSlices(u64, &.{3}, final_versions);
    try std.testing.expectError(error.FileNotFound, artifacts.getAlloc(first_artifact.artifact_id));
    try std.testing.expectError(error.FileNotFound, artifacts.getAlloc(second_artifact.artifact_id));
    const head_payload = try artifacts.getAlloc(head_artifact.artifact_id);
    defer alloc.free(head_payload);
    try std.testing.expectEqualStrings("head", head_payload);
}

test "pruner retains recent manifests and truncates WAL history" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const first = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try builder.publishNamespace("docs");
    defer build_first.deinit(alloc);
    var manifest_first = try manifest_store.getAlloc("docs", 1);
    defer manifest_first.deinit(alloc);
    const first_artifact_a = try alloc.dupe(u8, manifest_first.artifacts[0].artifact_id);
    defer alloc.free(first_artifact_a);
    const first_artifact_b = try alloc.dupe(u8, manifest_first.artifacts[1].artifact_id);
    defer alloc.free(first_artifact_b);

    const second = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .delete, .doc_id = "doc-a", .body = null },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var build_second = try builder.publishNamespace("docs");
    defer build_second.deinit(alloc);

    const third = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };
    var ingest_third = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 300, .mutations = &third });
    defer ingest_third.deinit(alloc);
    var build_third = try builder.publishNamespace("docs");
    defer build_third.deinit(alloc);

    var pruner = Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try pruner.pruneNamespace("docs", 2);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.kept_versions);
    try std.testing.expectEqual(@as(usize, 1), result.deleted_versions);
    try std.testing.expectEqual(@as(usize, 3), result.deleted_artifacts);
    try std.testing.expectEqual(@as(u64, 3), result.wal_keep_from_lsn);
    try std.testing.expectEqual(@as(u64, 2), result.wal_records_removed);

    const versions = try manifest_store.listVersionsAlloc("docs");
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u64, &.{ 2, 3 }, versions);

    const wal_records = try wal_store.readFromAlloc("docs", 1);
    defer wal_mod.freeRecords(alloc, wal_records);
    try std.testing.expectEqual(@as(usize, 2), wal_records.len);
    try std.testing.expectEqual(@as(u64, 3), wal_records[0].lsn);

    try std.testing.expectError(error.FileNotFound, artifact_store.getAlloc(first_artifact_a));
    try std.testing.expectError(error.FileNotFound, artifact_store.getAlloc(first_artifact_b));
}

test "concurrent pruners observe gc watermark conflict" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-prune-race");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-prune-race");
    const wal_root = tmpPath(&wal_root_buf, "wal-prune-race");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const first = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try builder.publishNamespace("docs");
    defer build_first.deinit(alloc);

    const second = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var build_second = try builder.publishNamespace("docs");
    defer build_second.deinit(alloc);

    const third = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };
    var ingest_third = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 300, .mutations = &third });
    defer ingest_third.deinit(alloc);
    var build_third = try builder.publishNamespace("docs");
    defer build_third.deinit(alloc);

    const RaceState = struct {
        pruner_a: Pruner,
        pruner_b: Pruner,
        result_a: ?PruneResult = null,
        result_b: ?PruneResult = null,

        fn runA(self: *@This()) void {
            self.result_a = self.pruner_a.pruneNamespace("docs", 3) catch null;
        }

        fn runB(self: *@This()) void {
            self.result_b = self.pruner_b.pruneNamespace("docs", 3) catch null;
        }
    };

    var state = RaceState{
        .pruner_a = Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store),
        .pruner_b = Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store),
    };
    const thread_a = try std.Thread.spawn(.{}, RaceState.runA, .{&state});
    const thread_b = try std.Thread.spawn(.{}, RaceState.runB, .{&state});
    thread_a.join();
    thread_b.join();

    defer if (state.result_a) |*result| result.deinit(alloc);
    defer if (state.result_b) |*result| result.deinit(alloc);
    try std.testing.expect(state.result_a != null);
    try std.testing.expect(state.result_b != null);
    try std.testing.expectEqual(@as(?u64, 1), try progress_store.getGcWatermark("docs"));
    const versions = try manifest_store.listVersionsAlloc("docs");
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, versions);
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-retention-{s}-{d}-{d}\x00", .{
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
