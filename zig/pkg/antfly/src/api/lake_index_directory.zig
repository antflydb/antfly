// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
    contribution_index: ?@import("../serverless/graph_segment/page_tree.zig").Ref = null,
    contribution_roots: []const catalog.Digest = &.{},
    contribution_ownership_version: u16 = 0,
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
    try hydrateLazy(a, store, publication, cancellation, cached);
    if (publication.contribution_index) |bytes| {
        var read_store = store;
        const directory = publication.directory.?;
        read_store.upload_scope = (try stores.uploadScopeFromArtifactId(directory.artifact_id)) orelse return error.InvalidLakeIndexCatalog;
        const root = try std.json.parseFromSliceLeaky(@import("../serverless/graph_segment/page_tree.zig").Ref, a, bytes, .{});
        var index: @import("lake_index_contributions.zig").Index = undefined;
        try index.init(a, read_store, root, cancellation);
        defer index.deinit();
        var cursor = try @import("../serverless/graph_segment/page_tree.zig").Cursor.init(std.heap.page_allocator, index.cache.store(), root, "", null);
        defer cursor.deinit();
        var values: std.ArrayList(catalog.FileContribution) = .empty;
        while (try cursor.next()) |entry| {
            const value = try std.json.parseFromSliceLeaky(catalog.FileContribution, a, entry.value, .{ .allocate = .alloc_always });
            try @import("lake_index_contributions.zig").validate(value);
            if (!std.mem.eql(u8, entry.key, &@import("lake_index_contributions.zig").identity(value))) return error.InvalidLakeIndexCatalog;
            try values.append(a, value);
            if (values.items.len > catalog.max_contributions) return error.InvalidLakeIndexCatalog;
        }
        publication.file_contributions = try values.toOwnedSlice(a);
    }
}
pub fn hydrateLazy(a: A, store: stores.ArtifactStore, publication: *catalog.Publication, cancellation: @import("antfly_cancellation").CancellationToken, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead) !void {
    const directory = publication.directory orelse return;
    if (publication.declarations.len != 0) return;
    const document = try loadDocument(a, store, .{ .kind = .external_base_source, .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, cancellation, cached);
    if (document.declarations.len != directory.count) return error.InvalidLakeIndexCatalog;
    publication.declarations = document.declarations;
    publication.contribution_roots = document.contribution_roots;
    publication.contribution_ownership_version = document.contribution_ownership_version;
    if (document.contribution_index) |root| publication.contribution_index = try std.json.Stringify.valueAlloc(a, root, .{});
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
    if ((!std.mem.eql(u8, document.format, "native-lake-index-directory-v1") and !std.mem.eql(u8, document.format, "native-lake-index-directory-v2") and !std.mem.eql(u8, document.format, "native-lake-index-directory-v3")) or document.contribution_pages.len > (catalog.max_contributions + 255) / 256) return error.InvalidLakeIndexCatalog;
    if (document.contribution_index) |root| {
        try root.validate();
        if (!std.mem.eql(u8, document.format, "native-lake-index-directory-v3") or root.records > catalog.max_contributions or document.file_contributions.len != 0 or document.contribution_pages.len != 0) return error.InvalidLakeIndexCatalog;
    }
    if (document.contribution_ownership_version > 1 or document.contribution_roots.len > catalog.max_directory_artifacts or
        (document.contribution_ownership_version == 0 and document.contribution_roots.len != 0) or
        (document.contribution_ownership_version == 1 and (document.contribution_index != null) != (document.contribution_roots.len != 0)) or
        (document.contribution_index == null and document.contribution_roots.len != 0)) return error.InvalidLakeIndexCatalog;
    for (document.contribution_roots, 0..) |key, i| {
        if (std.mem.allEqual(u8, &key, 0) or (i != 0 and std.mem.order(u8, &document.contribution_roots[i - 1], &key) != .lt)) return error.InvalidLakeIndexCatalog;
    }
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

pub fn publishIndexed(a: A, store: *stores.ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, contributions: []const catalog.FileContribution, index: *@import("lake_index_contributions.zig").Index, cancellation: @import("antfly_cancellation").CancellationToken) !catalog.DirectoryRef {
    if (declarations.len > catalog.max_directory_artifacts) return error.InvalidLakeIndexCatalog;
    try (local.serverless_segment_sidecar_manifest.Manifest{ .artifacts = declarations }).validate();
    const root = try index.update(contributions);
    const roots = try index.rootKeys(a);
    defer a.free(roots);
    const bytes = try std.json.Stringify.valueAlloc(a, Document{ .format = "native-lake-index-directory-v3", .declarations = declarations, .contribution_index = root, .contribution_roots = roots, .contribution_ownership_version = if (index.counted) 1 else 0 }, .{});
    defer a.free(bytes);
    if (bytes.len > catalog.max_directory_bytes) return error.InvalidLakeIndexCatalog;
    var upload = store.*;
    upload.allocator = a;
    const artifact = try upload.putWithCancellation(bytes, cancellation);
    return .{ .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len, .count = @intCast(declarations.len) };
}
