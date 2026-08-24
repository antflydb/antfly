// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production serverless object-store protocols over the reusable scripted
//! objectstore fault client. Fault selection belongs to VOPR; artifacts,
//! manifests, WAL, and progress stores remain their production implementations.

const std = @import("std");
const objectstore = @import("objectstore");
const artifacts_object_store = @import("../serverless/artifacts/object_store.zig");
const manifest_object_store = @import("../serverless/manifest/object_store.zig");
const manifest_types = @import("../serverless/manifest/types.zig");
const wal_object_store = @import("../serverless/wal/object_store.zig");
const wal_types = @import("../serverless/wal/types.zig");
const progress_object_store = @import("../serverless/catalog/object_progress_store.zig");
const progress_store = @import("../serverless/catalog/progress_store.zig");
const artifacts_mod = @import("../serverless/artifacts/mod.zig");
const manifest_mod = @import("../serverless/manifest/mod.zig");
const wal_mod = @import("../serverless/wal/mod.zig");
const catalog_mod = @import("../serverless/catalog/mod.zig");
const builder_mod = @import("../serverless/build/mod.zig");
const api_mod = @import("../serverless/api/mod.zig");
const api_codec = @import("../serverless/api/codec.zig");
const document_segment = @import("../serverless/document_segment/mod.zig");
const runtime_manager = @import("../serverless/runtime/manager.zig");
const backup_manifest = @import("../storage/ha/backup_manifest.zig");
const seed_artifact = @import("../storage/ha/seed_artifact.zig");

const RejectConditionalAppendWal = struct {
    inner: *wal_mod.WalStore,

    fn walStore(self: *@This()) wal_mod.WalStore {
        return .{ .allocator = self.inner.allocator, .ptr = self, .vtable = &vtable };
    }

    const vtable: wal_mod.WalStore.VTable = .{
        .deinit = deinit,
        .append = append,
        .append_idempotent_if_latest = appendIdempotentIfLatest,
        .read_from_alloc = readFromAlloc,
        .latest_lsn = latestLsn,
        .truncate_prefix = truncatePrefix,
    };

    fn deinit(_: std.mem.Allocator, _: *anyopaque) void {}

    fn append(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.append(namespace, timestamp_ns, payload);
    }

    fn appendIdempotentIfLatest(
        _: *anyopaque,
        _: []const u8,
        _: u64,
        _: []const u8,
        _: []const u8,
        _: u64,
    ) !?u64 {
        return null;
    }

    fn readFromAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, namespace: []const u8, start_lsn: u64) ![]wal_mod.Record {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.vtable.read_from_alloc(self.inner.ptr, alloc, namespace, start_lsn);
    }

    fn latestLsn(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.latestLsn(namespace);
    }

    fn truncatePrefix(ptr: *anyopaque, namespace: []const u8, keep_from_lsn: u64) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.inner.truncatePrefix(namespace, keep_from_lsn);
    }
};

test "serverless object store VOPR composes real artifact manifest WAL and progress protocols" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();

    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(
        alloc,
        faults.client(),
        "artifacts",
        "tenant",
    );
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();

    // A short provider write followed by timeout must not be mistaken for a
    // published content-addressed artifact. Retrying replaces it with the full
    // immutable body, so checksum-derived identity and bytes agree.
    faults.partiallyCommitNextPut(3, error.Timeout);
    try std.testing.expectError(error.Timeout, artifacts.put("complete-artifact"));
    var artifact = try artifacts.put("complete-artifact");
    defer artifact.deinit(alloc);
    faults.failNextGet(error.Canceled);
    try std.testing.expectError(error.Canceled, artifacts.getAlloc(artifact.artifact_id));
    const artifact_bytes = try artifacts.getAlloc(artifact.artifact_id);
    defer alloc.free(artifact_bytes);
    try std.testing.expectEqualStrings("complete-artifact", artifact_bytes);

    // Duplicate provider completion is harmless for an unconditional
    // content-addressed artifact write.
    faults.duplicateNextPut();
    var duplicate = try artifacts.put("complete-artifact");
    defer duplicate.deinit(alloc);
    try std.testing.expectEqualStrings(artifact.artifact_id, duplicate.artifact_id);

    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(
        alloc,
        faults.client(),
        "manifests",
        "tenant",
    );
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var artifact_refs = [_]manifest_types.ArtifactRef{.{
        .kind = .document_segment,
        .artifact_id = artifact.artifact_id,
        .byte_len = artifact.byte_len,
        .checksum = artifact.checksum,
    }};
    const manifest = manifest_types.Manifest{
        .namespace = "docs",
        .version = 1,
        .built_at_ns = 10,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{ .document_count = 1 },
        .artifacts = &artifact_refs,
    };

    faults.hideNextSuccessfulPut(2);
    try manifests.put(manifest);
    try std.testing.expectError(error.FileNotFound, manifests.getAlloc("docs", 1));
    try std.testing.expectError(error.FileNotFound, manifests.getAlloc("docs", 1));
    var visible_manifest = try manifests.getAlloc("docs", 1);
    visible_manifest.deinit(alloc);

    // CAS publication may commit even when the response is lost. A retry does
    // not claim it won, while a read reconciles the committed head.
    faults.commitNextPutThenFail(error.Timeout);
    try std.testing.expectError(error.Timeout, manifests.compareAndSwapHead("docs", null, 1));
    try std.testing.expectEqual(@as(u64, 1), try manifests.getHead("docs"));
    try std.testing.expect(!(try manifests.compareAndSwapHead("docs", null, 1)));

    var wal_impl = try wal_object_store.ObjectStore.initWithClient(
        alloc,
        faults.client(),
        "wal",
        "tenant",
    );
    var wal = wal_impl.walStore();
    defer wal.deinit();
    faults.commitNextPutThenFail(error.Timeout);
    try std.testing.expectError(error.Timeout, wal.appendIdempotent("docs", 20, "mutation", "request-1"));
    try std.testing.expectEqual(@as(u64, 1), try wal.appendIdempotent("docs", 20, "mutation", "request-1"));
    try std.testing.expectError(
        error.WalIdempotencyConflict,
        wal.appendIdempotent("docs", 20, "different", "request-1"),
    );
    const records = try wal.readFromAlloc("docs", 1);
    defer wal_types.freeRecords(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 1), records[0].lsn);
    try std.testing.expectEqualStrings("mutation", records[0].payload);
    try std.testing.expectEqualStrings("request-1", records[0].operation_id.?);

    // A provider that omits an ETag must not turn the conditional append into
    // an unconditional whole-log replacement.
    faults.omitEtagFromNextGet();
    try std.testing.expectError(
        error.MissingObjectEtag,
        wal.appendIdempotentIfLatest("docs", 21, "derived", "enrich-v1/1/1/0/1", 1),
    );
    try std.testing.expectEqual(@as(u64, 1), try wal.latestLsn("docs"));

    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(
        alloc,
        faults.client(),
        "progress",
        "tenant",
    );
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    faults.commitNextPutThenFail(error.Timeout);
    try std.testing.expectError(error.Timeout, progress.compareAndSwapHead("docs", null, 1));
    try std.testing.expectEqual(@as(u64, 1), try progress.getHead("docs"));
    try std.testing.expect(!(try progress.compareAndSwapHead("docs", null, 1)));
    const first_progress = progress_store.EnrichmentStageProgress{
        .head_version = 1,
        .doc_offset = 1,
    };
    try std.testing.expect(try progress.compareAndSwapEnrichmentStageProgress(
        "docs",
        .lexical_sparse,
        null,
        first_progress,
    ));
    faults.omitEtagFromNextGet();
    try std.testing.expectError(
        error.MissingObjectEtag,
        progress.compareAndSwapEnrichmentStageProgress(
            "docs",
            .lexical_sparse,
            first_progress,
            .{ .head_version = 1, .doc_offset = 2 },
        ),
    );
    try std.testing.expectEqual(
        @as(?progress_store.EnrichmentStageProgress, first_progress),
        try progress.getEnrichmentStageProgress("docs", .lexical_sparse),
    );

    faults.resetClientAfterCrash();
    try std.testing.expectEqual(@as(u64, 1), try manifests.getHead("docs"));
    try std.testing.expectEqual(@as(u64, 1), try wal.latestLsn("docs"));
    try std.testing.expectEqual(@as(u64, 1), try progress.getHead("docs"));
}

test "HA seed backup restore VOPR retries ambiguous publication and canceled download" {
    const alloc = std.testing.allocator;
    const io = std.testing.io; // vopr-audit: allow(host_filesystem) seed artifact materialization is the retained native differential boundary
    var tmp = std.testing.tmpDir(.{}); // vopr-audit: allow(host_filesystem) seed artifact materialization is the retained native differential boundary
    defer tmp.cleanup();
    try tmp.dir.makePath(io, "source");
    try tmp.dir.writeFile(io, .{ .sub_path = "source/table.sst", .data = "durable-table-state" });

    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const source_root = try std.fs.path.join(alloc, &.{ root, "source" });
    defer alloc.free(source_root);
    const staging_root = try std.fs.path.join(alloc, &.{ root, "staging" });
    defer alloc.free(staging_root);

    const identity = backup_manifest.Identity{
        .cluster_id = 1,
        .shard_id = 2,
        .table_id = 3,
        .timeline_id = 4,
        .epoch = 5,
    };
    const contents = "durable-table-state";
    const files = [_]backup_manifest.FileEntry{.{
        .path = "table.sst",
        .kind = .sstable,
        .size_bytes = contents.len,
        .crc32 = backup_manifest.crc32(contents),
    }};
    const manifest_bytes = try backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = "vopr-backup",
        .backup_lsn = 8,
        .checkpoint_lsn = 11,
        .files = &files,
    });
    defer alloc.free(manifest_bytes);

    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var client = faults.client();
    try client.makeBucket("ha-seeds");
    const store = seed_artifact.Store{
        .client = &client,
        .bucket = "ha-seeds",
        .prefix = "cluster-a",
    };
    const publish_request = seed_artifact.PublishRequest{
        .generation = "generation-1",
        .slot_name = "standby-a",
        .manifest_bytes = manifest_bytes,
        .content_root = source_root,
        .limits = .{ .max_chunk_bytes = 5 },
    };

    // The first chunk is committed by the provider but its response is lost.
    // A restarted publisher reconciles the immutable bytes and completes the
    // same generation instead of creating a second backup.
    faults.commitNextPutThenFail(error.Timeout);
    try std.testing.expectError(error.Timeout, seed_artifact.publish(alloc, store, publish_request));
    faults.resetClientAfterCrash();
    var published = try seed_artifact.publish(alloc, store, publish_request);
    defer published.deinit(alloc);
    try std.testing.expect(!published.already_available);
    var repeated = try seed_artifact.publish(alloc, store, publish_request);
    defer repeated.deinit(alloc);
    try std.testing.expect(repeated.already_available);
    try std.testing.expectEqualStrings(published.receipt_json, repeated.receipt_json);

    const expected = seed_artifact.ExpectedArtifact{
        .generation = "generation-1",
        .slot_name = "standby-a",
        .identity = identity,
        .minimum_checkpoint_lsn = 11,
    };
    var remote = try seed_artifact.verifyRemote(alloc, store, expected, publish_request.limits);
    defer remote.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), remote.file_count);
    try std.testing.expectEqual(@as(u64, contents.len), remote.total_bytes);

    // Cancellation before the completion receipt is read cannot publish a
    // local staging receipt. Retrying downloads and verifies every chunk.
    faults.failNextGet(error.Canceled);
    try std.testing.expectError(error.Canceled, seed_artifact.restoreToStaging(alloc, store, .{
        .expected = expected,
        .staging_root = staging_root,
        .limits = publish_request.limits,
    }));
    faults.resetClientAfterCrash();
    var restored = try seed_artifact.restoreToStaging(alloc, store, .{
        .expected = expected,
        .staging_root = staging_root,
        .limits = publish_request.limits,
    });
    defer restored.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), restored.file_count);
    try std.testing.expectEqual(@as(u64, contents.len), restored.total_bytes);
    try seed_artifact.verifyStaged(alloc, staging_root, expected, publish_request.limits);

    const restored_bytes = try tmp.dir.readFileAlloc(io, "staging/table.sst", alloc, .limited(1024));
    defer alloc.free(restored_bytes);
    try std.testing.expectEqualStrings(contents, restored_bytes);
}

test "serverless object store VOPR consumes stale enrichment generation without overwriting documents" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();

    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(alloc, memory.client(), "generation-artifacts", "tenant");
    var artifacts: artifacts_mod.ArtifactStore = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(alloc, memory.client(), "generation-manifests", "tenant");
    var manifests: manifest_mod.ManifestStore = manifest_impl.manifestStore();
    defer manifests.deinit();
    var wal_impl = try wal_object_store.ObjectStore.initWithClient(alloc, memory.client(), "generation-wal", "tenant");
    var wal: wal_mod.WalStore = wal_impl.walStore();
    defer wal.deinit();
    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(alloc, memory.client(), "generation-progress", "tenant");
    var progress: catalog_mod.ProgressStore = progress_impl.progressStore();
    defer progress.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifacts, &manifests, &progress, &wal);
    var api = api_mod.Service.init(alloc, &wal, &builder);
    const initial = [_]api_mod.DocumentMutation{.{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"authoritative\"}",
    }};
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &initial });
    defer ingest.deinit(alloc);
    var first = try builder.publishNamespace("docs");
    defer first.deinit(alloc);

    // Model a WAL-free metadata/external cutover after the enricher captured
    // head 1 but before it appended its full-body derived mutation.
    var generation_two = try manifests.getAlloc("docs", 1);
    defer generation_two.deinit(alloc);
    generation_two.version = 2;
    try manifests.put(generation_two);
    try std.testing.expect(try progress.compareAndSwapHead("docs", 1, 2));

    const stale_mutation = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"stale-derived-overwrite\"}",
    });
    defer alloc.free(stale_mutation);
    try std.testing.expectEqual(
        @as(?u64, 2),
        try wal.appendIdempotentIfLatest(
            "docs",
            101,
            stale_mutation,
            "enrich-v1/1/1/0/1",
            1,
        ),
    );

    var consumed = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{
            .published_search_sources = generation_two.stats.published_search_sources,
            .include_graph = true,
        },
        .policy = .{ .keep_latest_versions = 9 },
        .metadata_republish = .{ .artifact_families_changed = true },
        .artifact_actions = .{
            .document_segment = .reuse,
            .full_text = .reuse,
            .dense_vector = .reuse,
            .sparse_vector = .reuse,
            .graph = .reuse,
        },
    });
    defer consumed.deinit(alloc);
    try std.testing.expect(consumed.published);
    try std.testing.expectEqual(@as(u64, 3), consumed.version);
    try std.testing.expectEqual(@as(u64, 2), consumed.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 2), consumed.wal_end_lsn);

    var visible = try manifests.getAlloc("docs", 3);
    defer visible.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 9), visible.stats.policy.keep_latest_versions);
    try std.testing.expectEqual(@as(u64, 2), visible.wal_start_lsn);
    const document_ref = for (visible.artifacts) |artifact| {
        if (artifact.kind == .document_segment) break artifact;
    } else return error.DocumentSegmentNotFound;
    const payload = try artifacts.getAlloc(document_ref.artifact_id);
    defer alloc.free(payload);
    const entries = try document_segment.decodeAlloc(alloc, payload);
    defer document_segment.freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("{\"text\":\"authoritative\"}", entries[0].body);

    // Exercise the cheap unchanged-plan path as well. The stale source head 2
    // record is consumed by a clone of head 3, and that clone must carry the
    // tighter retention boundary without losing the plan applied above.
    try std.testing.expectEqual(
        @as(?u64, 3),
        try wal.appendIdempotentIfLatest(
            "docs",
            102,
            stale_mutation,
            "enrich-v1/2/1/0/1",
            2,
        ),
    );
    var cloned = try builder.publishNamespace("docs");
    defer cloned.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4), cloned.version);
    try std.testing.expectEqual(@as(u64, 3), cloned.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 3), cloned.wal_end_lsn);
    var cloned_manifest = try manifests.getAlloc("docs", 4);
    defer cloned_manifest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 9), cloned_manifest.stats.policy.keep_latest_versions);
    const cloned_document_ref = for (cloned_manifest.artifacts) |artifact| {
        if (artifact.kind == .document_segment) break artifact;
    } else return error.DocumentSegmentNotFound;
    try std.testing.expectEqualStrings(
        document_ref.artifact_id,
        cloned_document_ref.artifact_id,
    );

    var pruner = builder_mod.Pruner.init(alloc, &artifacts, &manifests, &progress, &wal);
    var pruning = try pruner.pruneNamespace("docs", 1);
    defer pruning.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), pruning.wal_keep_from_lsn);
    try std.testing.expectEqual(@as(u64, 2), pruning.wal_records_removed);
    const retained_wal = try wal.readFromAlloc("docs", 1);
    defer wal_mod.freeRecords(alloc, retained_wal);
    try std.testing.expectEqual(@as(usize, 1), retained_wal.len);
    try std.testing.expectEqual(@as(u64, 3), retained_wal[0].lsn);
}

test "serverless object store VOPR enrichment conflict preserves pruning progress" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();

    var artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(alloc, memory.client(), "maintenance-artifacts", "tenant");
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(alloc, memory.client(), "maintenance-manifests", "tenant");
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();
    var wal_impl = try wal_object_store.ObjectStore.initWithClient(alloc, memory.client(), "maintenance-wal", "tenant");
    var wal = wal_impl.walStore();
    defer wal.deinit();
    var progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(alloc, memory.client(), "maintenance-progress", "tenant");
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var catalog_impl = try @import("../serverless/catalog/object_store.zig").ObjectStore.initWithClient(
        alloc,
        memory.client(),
        "maintenance-catalog",
        "tenant",
    );
    var catalog_store = catalog_impl.catalogStore();
    defer catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifacts, &manifests, &progress, &wal);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifacts, &manifests, &progress, &wal, &builder, &catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .enrichment_enabled = true,
        .keep_latest_versions = 1,
    }));
    var api = api_mod.Service.init(alloc, &wal, &builder);
    const first_batch = [_]api_mod.DocumentMutation{.{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\"}" }};
    var first_ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first_batch });
    defer first_ingest.deinit(alloc);
    var first_build = try builder.publishNamespace("docs");
    defer first_build.deinit(alloc);
    const second_batch = [_]api_mod.DocumentMutation{.{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"text\":\"beta\"}" }};
    var second_ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second_batch });
    defer second_ingest.deinit(alloc);
    var second_build = try builder.publishNamespace("docs");
    defer second_build.deinit(alloc);

    var rejecting_impl = RejectConditionalAppendWal{ .inner = &wal };
    var rejecting_wal = rejecting_impl.walStore();
    defer rejecting_wal.deinit();
    var runtime = runtime_manager.ManagedRuntime.init(alloc, .{
        .publish_enabled = false,
        .compaction_enabled = false,
        .prune_enabled = true,
        .enrichment_enabled = true,
    }, &catalog, builder_mod.Pruner.init(alloc, &artifacts, &manifests, &progress, &wal));
    runtime.setEnricher(@import("../serverless/enrichment/worker.zig").SparseEnricher.init(
        alloc,
        &artifacts,
        &manifests,
        &progress,
        &rejecting_wal,
    ));
    defer runtime.deinit();

    const stats = try runtime.runOnce();
    try std.testing.expectEqual(@as(usize, 1), stats.enrichment_conflicts);
    try std.testing.expectEqual(@as(usize, 1), stats.pruned_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.deleted_versions);
    const versions = try manifests.listVersionsAlloc("docs");
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u64, &.{2}, versions);
}
