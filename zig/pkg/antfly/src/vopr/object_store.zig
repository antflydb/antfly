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
    try std.testing.expectError(error.Timeout, wal.append("docs", 20, "mutation"));
    const records = try wal.readFromAlloc("docs", 1);
    defer wal_types.freeRecords(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 1), records[0].lsn);
    try std.testing.expectEqualStrings("mutation", records[0].payload);

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
