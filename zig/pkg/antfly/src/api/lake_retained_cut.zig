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

//! Immutable, object-store backed search cuts. A content-addressed token is an
//! opaque capability to server-written data, not a client-supplied snapshot.
//! Current table permissions, schema/recipe and incarnation are rechecked on
//! every use. Index reader leases and lake snapshot pins protect the archive;
//! copying only the bounded WAL suffix makes WAL retirement independent of it.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const ingestion = @import("../serverless/lake_ingestion.zig");
const catalog = local.metadata_lake_index_catalog;
const A = std.mem.Allocator;
pub const ttl_ms: u64 = 60_000;
pub const max_bytes: usize = 40 * 1024 * 1024;
pub const prefix = "lake2:";
pub const Descriptor = struct {
    version: u16 = 1,
    expires_ms: u64,
    table_id: u64,
    object_generation: u64,
    desired: catalog.Digest,
    publication: catalog.Publication,
    reader_token: catalog.Token,
    pending: ingestion.Pending,
    catalog_metadata: ?local.serverless_external_source_mod.lake_catalog.types.Table = null,
    published_only: bool,
    recent: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = &.{},
};
fn domain(table: u64, store: catalog.Digest) catalog.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-retained-search-cuts-v1");
    hash.update(&store);
    var id: [8]u8 = undefined;
    std.mem.writeInt(u64, &id, table, .little);
    hash.update(&id);
    var digest: catalog.Digest = undefined;
    hash.final(&digest);
    return digest;
}
pub fn save(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, io: std.Io, descriptor: Descriptor, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(bytes);
    if (bytes.len > max_bytes) return error.QueryCandidateBudgetExceeded;
    const scope = try stores.UploadScope.forPublication(domain(descriptor.table_id, store_identity), descriptor.expires_ms, io);
    var metadata = try store.putScoped(scope, bytes, cancellation);
    defer metadata.deinit(store.allocator);
    return std.fmt.allocPrint(a, "{s}{s}:{d}", .{ prefix, metadata.artifact_id, metadata.byte_len });
}
pub fn load(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, token: []const u8, table: local.common_topology_records.TableRecord, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !Descriptor {
    if (!std.mem.startsWith(u8, token, prefix)) return error.InvalidQueryRequest;
    const split = std.mem.lastIndexOfScalar(u8, token, ':') orelse return error.InvalidQueryRequest;
    const artifact = token[prefix.len..split];
    const scope = (try stores.uploadScopeFromArtifactId(artifact)) orelse return error.InvalidQueryRequest;
    if (!std.mem.eql(u8, &scope.domain, &domain(table.table_id, store_identity))) return error.CatalogGenerationChanged;
    if (scope.fencingToken() <= now) return error.CatalogGenerationChanged;
    const length = std.fmt.parseInt(usize, token[split + 1 ..], 10) catch return error.InvalidQueryRequest;
    if (length > max_bytes or length == 0) return error.InvalidQueryRequest;
    const checksum = try stores.sha256ChecksumFromArtifactId(artifact);
    const bytes = store.getVerifiedAllocWithCancellation(artifact, length, checksum, cancellation) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return error.CatalogGenerationChanged,
        else => return err,
    };
    defer store.allocator.free(bytes);
    const result = try std.json.parseFromSliceLeaky(Descriptor, a, bytes, .{ .allocate = .alloc_always });
    if (result.version != 1 or result.expires_ms != scope.fencingToken() or result.expires_ms <= now or result.expires_ms > now +| ttl_ms or result.table_id != table.table_id or result.object_generation != table.object_storage_generation or !std.mem.eql(u8, &result.desired, &catalog.desiredFingerprint(table))) return error.CatalogGenerationChanged;
    try result.publication.validate();
    return result;
}
/// Bounded discovery over a dedicated domain; archive generations use a
/// different domain and cannot be collected by this path.
pub fn collect(store: *stores.ArtifactStore, table: u64, store_identity: catalog.Digest, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !void {
    const Visitor = struct {
        store: *stores.ArtifactStore,
        now: u64,
        deleted: usize = 0,
        fn visit(raw: *anyopaque, scope: stores.UploadScope, artifact_id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (scope.fencingToken() +| 30_000 >= self.now) return;
            if (self.deleted == 128) return error.RetainedCutCollectionBound;
            try self.store.delete(artifact_id);
            self.deleted += 1;
        }
    };
    var visitor: Visitor = .{ .store = store, .now = now };
    store.visitScopedUploads(domain(table, store_identity), .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = now -| 30_000, .max_entries = 128 }, cancellation) catch |err| switch (err) {
        error.RetainedCutCollectionBound => {},
        else => return err,
    };
}

const CursorEnvelope = struct { version: u16 = 1, expires_ms: u64, payload: []const u8 };
pub fn saveCursor(a: A, store: *stores.ArtifactStore, identity: catalog.Digest, io: std.Io, payload: []const u8, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    if (payload.len > 1024 * 1024) return error.QueryCandidateBudgetExceeded;
    var expires = now +| ttl_ms;
    const state = try std.json.parseFromSliceLeaky(std.json.Value, a, payload, .{});
    if (state != .object) return error.InvalidQueryRequest;
    const sources = state.object.get("sources") orelse return error.InvalidQueryRequest;
    if (sources != .array) return error.InvalidQueryRequest;
    for (sources.array.items) |source| {
        if (source != .object) return error.InvalidQueryRequest;
        const remote = source.object.get("remote_snapshot") orelse return error.InvalidQueryRequest;
        if (remote != .string or !std.mem.startsWith(u8, remote.string, prefix)) return error.UnsupportedQueryRequest;
        const split = std.mem.lastIndexOfScalar(u8, remote.string, ':') orelse return error.InvalidQueryRequest;
        const leaf_scope = (try stores.uploadScopeFromArtifactId(remote.string[prefix.len..split])) orelse return error.InvalidQueryRequest;
        expires = @min(expires, leaf_scope.fencingToken());
    }
    if (expires <= now) return error.CatalogGenerationChanged;
    const encoded = try std.json.Stringify.valueAlloc(a, CursorEnvelope{ .expires_ms = expires, .payload = payload }, .{});
    defer a.free(encoded);
    const scope = try stores.UploadScope.forPublication(domain(0, identity), expires, io);
    var metadata = try store.putScoped(scope, encoded, cancellation);
    defer metadata.deinit(store.allocator);
    return std.fmt.allocPrint(a, "source2:{s}:{d}", .{ metadata.artifact_id, metadata.byte_len });
}
pub fn loadCursor(a: A, store: *stores.ArtifactStore, identity: catalog.Digest, token: []const u8, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    if (!std.mem.startsWith(u8, token, "source2:")) return error.InvalidQueryRequest;
    const split = std.mem.lastIndexOfScalar(u8, token, ':') orelse return error.InvalidQueryRequest;
    const artifact = token[8..split];
    const scope = (try stores.uploadScopeFromArtifactId(artifact)) orelse return error.InvalidQueryRequest;
    if (scope.fencingToken() <= now or !std.mem.eql(u8, &scope.domain, &domain(0, identity))) return error.CatalogGenerationChanged;
    const length = std.fmt.parseInt(usize, token[split + 1 ..], 10) catch return error.InvalidQueryRequest;
    if (length == 0 or length > 2 * 1024 * 1024) return error.InvalidQueryRequest;
    const bytes = try store.getVerifiedAllocWithCancellation(artifact, length, try stores.sha256ChecksumFromArtifactId(artifact), cancellation);
    defer store.allocator.free(bytes);
    const envelope = try std.json.parseFromSliceLeaky(CursorEnvelope, a, bytes, .{ .allocate = .alloc_always });
    if (envelope.version != 1 or envelope.expires_ms != scope.fencingToken() or envelope.expires_ms <= now or envelope.expires_ms > now +| ttl_ms or envelope.payload.len > 1024 * 1024) return error.CatalogGenerationChanged;
    return envelope.payload;
}

test "external lake retained cuts survive reopening and fence expiration incarnation and recipe" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-retained-cuts");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var store = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    const identity = store.identity;
    var artifacts = store.artifactStore();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 4, .name = "history", .schema_json = "{}", .indexes_json = "{}" };
    const publication: catalog.Publication = .{ .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) }, .published_at_ms = 1, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } }, .inventory = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } };
    const now: u64 = 1000;
    const token = try save(a, &artifacts, identity, std.testing.io, .{ .expires_ms = now + ttl_ms, .table_id = 4, .object_generation = 0, .desired = catalog.desiredFingerprint(table), .publication = publication, .reader_token = @splat(5), .pending = .{ .lsn = 8, .key_fields = &.{"id"}, .changes = &.{} }, .published_only = false }, .none);
    defer a.free(token);
    store.deinit();
    var reopened = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    defer reopened.deinit();
    var reads = reopened.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const cursor_payload = try std.json.Stringify.valueAlloc(scratch, .{ .sources = .{.{ .remote_snapshot = token }} }, .{});
    const cursor_token = try saveCursor(scratch, &reads, identity, std.testing.io, cursor_payload, now + 20, .none);
    const cursor_split = std.mem.lastIndexOfScalar(u8, cursor_token, ':').?;
    const cursor_scope = (try stores.uploadScopeFromArtifactId(cursor_token[8..cursor_split])).?;
    try std.testing.expectEqual(now + ttl_ms, cursor_scope.fencingToken());
    try std.testing.expectEqualStrings(cursor_payload, try loadCursor(scratch, &reads, identity, cursor_token, now + 21, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, loadCursor(scratch, &reads, identity, cursor_token, now + ttl_ms, .none));
    const loaded = try load(scratch, &reads, identity, token, table, now + 1, .none);
    try std.testing.expectEqual(@as(u64, 8), loaded.pending.lsn);
    try std.testing.expectEqual(publication.generation, loaded.publication.generation);
    try std.testing.expectError(error.CatalogGenerationChanged, load(scratch, &reads, identity, token, table, now + ttl_ms, .none));
    var changed = table;
    changed.object_storage_generation = 2;
    try std.testing.expectError(error.CatalogGenerationChanged, load(scratch, &reads, identity, token, changed, now + 1, .none));
    changed = table;
    changed.indexes_json = "{\"text\":{\"type\":\"full_text\"}}";
    try std.testing.expectError(error.CatalogGenerationChanged, load(scratch, &reads, identity, token, changed, now + 1, .none));
}
