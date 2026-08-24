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
const backup_manifest = @import("../storage/ha/backup_manifest.zig");
const seed_artifact = @import("../storage/ha/seed_artifact.zig");

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
