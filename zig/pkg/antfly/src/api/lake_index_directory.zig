// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Bounded metadata references an authenticated, immutable artifact directory.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Document = struct {
    format: []const u8 = "native-lake-index-directory-v1",
    declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact,
    file_contributions: []const catalog.FileContribution = &.{},
};
pub fn publish(a: A, store: *stores.ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, cancellation: @import("antfly_cancellation").CancellationToken) !catalog.DirectoryRef {
    return publishWithContributions(a, store, declarations, &.{}, cancellation);
}
pub fn publishWithContributions(a: A, store: *stores.ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, contributions: []const catalog.FileContribution, cancellation: @import("antfly_cancellation").CancellationToken) !catalog.DirectoryRef {
    if (contributions.len > 16384) return error.InvalidLakeIndexCatalog;
    if (declarations.len > catalog.max_directory_artifacts) return error.InvalidLakeIndexCatalog;
    try (local.serverless_segment_sidecar_manifest.Manifest{ .artifacts = declarations }).validate();
    const bytes = try std.json.Stringify.valueAlloc(a, Document{ .declarations = declarations, .file_contributions = contributions }, .{});
    defer a.free(bytes);
    if (bytes.len > catalog.max_directory_bytes) return error.InvalidLakeIndexCatalog;
    const artifact = try store.putWithCancellation(bytes, cancellation);
    return .{ .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len, .count = @intCast(declarations.len) };
}
/// The publication must already have fresh source/store/authorization proof.
/// Hydrated declarations borrow a; durable serialization retains only the ref.
pub fn hydrate(a: A, store: stores.ArtifactStore, publication: *catalog.Publication, cancellation: @import("antfly_cancellation").CancellationToken, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead) !void {
    const directory = publication.directory orelse return;
    if (publication.declarations.len != 0) return;
    const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store, .{ .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, cancellation, cached);
    defer a.free(bytes);
    const document = try std.json.parseFromSliceLeaky(Document, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, document.format, "native-lake-index-directory-v1") or document.declarations.len != directory.count) return error.InvalidLakeIndexCatalog;
    publication.declarations = document.declarations;
    publication.file_contributions = document.file_contributions;
    try publication.validate();
}
