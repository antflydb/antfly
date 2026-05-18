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

        const keep_count = @max(keep_latest_versions, 1);
        const keep_start = versions.len -| @min(versions.len, keep_count);
        const kept_versions = versions[keep_start..];

        var keep_manifest = try self.manifests.getAlloc(namespace, kept_versions[0]);
        defer keep_manifest.deinit(self.alloc);

        const current_gc = try self.progress.getGcWatermark(namespace);
        const effective_keep_from = if (current_gc) |value| @max(value, keep_manifest.wal_start_lsn) else keep_manifest.wal_start_lsn;
        if (current_gc == null) {
            const advanced = try self.progress.compareAndSwapGcWatermark(namespace, null, effective_keep_from);
            if (!advanced) return try self.noopResult(namespace, kept_versions.len, true);
        } else if (current_gc.? < effective_keep_from) {
            const advanced = try self.progress.compareAndSwapGcWatermark(namespace, current_gc, effective_keep_from);
            if (!advanced) return try self.noopResult(namespace, kept_versions.len, true);
        }

        var retained_artifacts = std.StringHashMapUnmanaged(void).empty;
        defer freeOwnedKeys(self.alloc, &retained_artifacts);
        for (kept_versions) |version| {
            var manifest = try self.manifests.getAlloc(namespace, version);
            defer manifest.deinit(self.alloc);
            try collectArtifactIds(self.alloc, &retained_artifacts, manifest.artifacts);
        }

        var pruned_artifacts = std.StringHashMapUnmanaged(void).empty;
        defer freeOwnedKeys(self.alloc, &pruned_artifacts);
        var deleted_versions: usize = 0;
        for (versions[0..keep_start]) |version| {
            var manifest = try self.manifests.getAlloc(namespace, version);
            defer manifest.deinit(self.alloc);
            try collectUnretainedArtifactIds(self.alloc, &pruned_artifacts, retained_artifacts, manifest.artifacts);
            self.manifests.deleteVersion(namespace, version) catch |err| switch (err) {
                error.CannotDeleteHead => continue,
                else => return err,
            };
            deleted_versions += 1;
        }

        var deleted_artifacts: usize = 0;
        var artifact_it = pruned_artifacts.iterator();
        while (artifact_it.next()) |entry| {
            self.artifacts.delete(entry.key_ptr.*) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            deleted_artifacts += 1;
        }

        const wal_records_removed = try self.wal.truncatePrefix(namespace, effective_keep_from);
        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .kept_versions = kept_versions.len,
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
