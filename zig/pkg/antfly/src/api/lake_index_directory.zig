// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Bounded metadata references an authenticated, immutable artifact directory.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
pub const Document = struct {
    format: []const u8 = "native-lake-index-directory-v1",
    declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact,
    file_contributions: []const catalog.FileContribution = &.{},
    contribution_pages: []const local.serverless_manifest_artifact_ref.ArtifactRef = &.{},
};
pub fn publish(a: A, store: *stores.ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, cancellation: @import("antfly_cancellation").CancellationToken) !catalog.DirectoryRef {
    return publishWithContributions(a, store, declarations, &.{}, cancellation);
}
pub fn publishWithContributions(a: A, store: *stores.ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, contributions: []const catalog.FileContribution, cancellation: @import("antfly_cancellation").CancellationToken) !catalog.DirectoryRef {
    if (contributions.len > catalog.max_contributions) return error.InvalidLakeIndexCatalog;
    if (declarations.len > catalog.max_directory_artifacts) return error.InvalidLakeIndexCatalog;
    try (local.serverless_segment_sidecar_manifest.Manifest{ .artifacts = declarations }).validate();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const ca = scratch.allocator();
    var pages: std.ArrayList(local.serverless_manifest_artifact_ref.ArtifactRef) = .empty;
    var offset: usize = 0;
    while (offset < contributions.len) {
        const end = @min(contributions.len, offset + 256);
        const encoded = try std.json.Stringify.valueAlloc(a, contributions[offset..end], .{});
        defer a.free(encoded);
        if (encoded.len > max_contribution_page_bytes) return error.InvalidLakeIndexCatalog;
        var page_store = store.*;
        page_store.allocator = ca;
        const uploaded = try page_store.putWithCancellation(encoded, cancellation);
        try pages.append(ca, .{ .kind = .external_base_source, .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len });
        offset = end;
    }
    const bytes = try std.json.Stringify.valueAlloc(a, Document{ .format = "native-lake-index-directory-v2", .declarations = declarations, .contribution_pages = pages.items }, .{});
    defer a.free(bytes);
    if (bytes.len > catalog.max_directory_bytes) return error.InvalidLakeIndexCatalog;
    var upload = store.*;
    upload.allocator = a;
    const artifact = try upload.putWithCancellation(bytes, cancellation);
    return .{ .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len, .count = @intCast(declarations.len) };
}
/// The publication must already have fresh source/store/authorization proof.
/// Hydrated declarations borrow a; durable serialization retains only the ref.
pub fn hydrate(a: A, store: stores.ArtifactStore, publication: *catalog.Publication, cancellation: @import("antfly_cancellation").CancellationToken, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead) !void {
    const directory = publication.directory orelse return;
    if (publication.declarations.len != 0) return;
    const document = try loadDocument(a, store, .{ .kind = .external_base_source, .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, cancellation, cached);
    if (document.declarations.len != directory.count) return error.InvalidLakeIndexCatalog;
    publication.declarations = document.declarations;
    var contributions: std.ArrayList(catalog.FileContribution) = .empty;
    try contributions.appendSlice(a, document.file_contributions);
    for (document.contribution_pages) |page| {
        try contributions.appendSlice(a, try loadContributionPage(a, store, page, cancellation, cached));
        if (contributions.items.len > catalog.max_contributions) return error.InvalidLakeIndexCatalog;
    }
    publication.file_contributions = try contributions.toOwnedSlice(a);
    try publication.validate();
}

pub fn loadDocument(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: @import("antfly_cancellation").CancellationToken, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead) !Document {
    if (ref.byte_len > catalog.max_directory_bytes) return error.InvalidLakeIndexCatalog;
    const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cached);
    defer a.free(bytes);
    const document = try std.json.parseFromSliceLeaky(Document, a, bytes, .{ .allocate = .alloc_always });
    if ((!std.mem.eql(u8, document.format, "native-lake-index-directory-v1") and !std.mem.eql(u8, document.format, "native-lake-index-directory-v2")) or document.contribution_pages.len > (catalog.max_contributions + 255) / 256) return error.InvalidLakeIndexCatalog;
    for (document.contribution_pages) |page| {
        if (page.kind != .external_base_source or page.byte_len == 0 or page.byte_len > max_contribution_page_bytes) return error.InvalidLakeIndexCatalog;
        try stores.validateSha256ArtifactIdentity(page.artifact_id, page.checksum);
    }
    return document;
}

pub const max_contribution_page_bytes = 1024 * 1024;
pub fn loadContributionPage(a: A, store: stores.ArtifactStore, page: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: @import("antfly_cancellation").CancellationToken, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead) ![]const catalog.FileContribution {
    if (page.byte_len == 0 or page.byte_len > max_contribution_page_bytes) return error.InvalidLakeIndexCatalog;
    const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store, .{ .artifact_id = page.artifact_id, .checksum = page.checksum, .byte_len = page.byte_len }, cancellation, cached);
    defer a.free(bytes);
    const contributions = try std.json.parseFromSliceLeaky([]const catalog.FileContribution, a, bytes, .{ .allocate = .alloc_always });
    if (contributions.len == 0 or contributions.len > 256) return error.InvalidLakeIndexCatalog;
    for (contributions) |contribution| {
        if (contribution.name.len == 0 or contribution.name.len > 128 or std.mem.allEqual(u8, &contribution.file, 0) or std.mem.allEqual(u8, &contribution.recipe, 0) or contribution.artifact.kind != .algebraic_segment) return error.InvalidLakeIndexCatalog;
        try stores.validateSha256ArtifactIdentity(contribution.artifact.artifact_id, contribution.artifact.checksum);
    }
    return contributions;
}
