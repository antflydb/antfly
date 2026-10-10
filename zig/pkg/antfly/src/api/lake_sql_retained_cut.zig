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

//! Session-owned accepted cuts: immutable metadata plus a copied WAL suffix.
//! The native session stores only the authenticated artifact capability.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("../serverless/artifacts/store.zig");
const catalog = local.sql_catalog;
const A = std.mem.Allocator;
const prefix = "sql-lake1:";
pub const ttl_ms: u64 = std.time.ms_per_hour;
pub const max_bytes = 40 * 1024 * 1024;
pub const Descriptor = struct {
    version: u16 = 1,
    name: ?local.sql_ast.Name = null,
    table_id: u64,
    schema_version: u32,
    object_generation: u64,
    expires_ms: u64,
    metadata_location: []const u8,
    metadata_json: []const u8,
    table_uuid: []const u8,
    snapshot_id: []const u8,
    pending: @import("../serverless/lake_ingestion.zig").Pending,
};
fn domain(table: u64, identity: [32]u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("accepted-sql-transaction-cut-v1");
    hash.update(&identity);
    var id: [8]u8 = undefined;
    std.mem.writeInt(u64, &id, table, .big);
    hash.update(&id);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
pub fn save(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, io: std.Io, descriptor: Descriptor, cancellation: @import("antfly_cancellation").CancellationToken) ![]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(bytes);
    if (bytes.len > max_bytes) return error.QueryCandidateBudgetExceeded;
    const scope = try artifacts.UploadScope.forPublication(domain(descriptor.table_id, identity), descriptor.expires_ms, io);
    var object = try store.putScoped(scope, bytes, cancellation);
    defer object.deinit(store.allocator);
    return std.fmt.allocPrint(a, "{s}{s}:{d}", .{ prefix, object.artifact_id, object.byte_len });
}
pub fn load(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, token: []const u8, table: catalog.Table, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !Descriptor {
    const value = try loadDescriptor(a, store, identity, token, table.id, now, cancellation);
    if (value.schema_version != table.schema_version or value.object_generation != (if (table.external_indexes) |indexes| indexes.object_generation else 0)) return error.CatalogGenerationChanged;
    return value;
}
pub fn loadDescriptor(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, token: []const u8, table_id: u64, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !Descriptor {
    if (!std.mem.startsWith(u8, token, prefix)) return error.CatalogGenerationChanged;
    const at = std.mem.lastIndexOfScalar(u8, token, ':') orelse return error.CatalogGenerationChanged;
    const id = token[prefix.len..at];
    const scope = (try artifacts.uploadScopeFromArtifactId(id)) orelse return error.CatalogGenerationChanged;
    if (!std.mem.eql(u8, &scope.domain, &domain(table_id, identity))) return error.CatalogGenerationChanged;
    const size = std.fmt.parseInt(u64, token[at + 1 ..], 10) catch return error.CatalogGenerationChanged;
    if (size > max_bytes or size == 0) return error.QueryCandidateBudgetExceeded;
    const bytes = try store.getVerifiedAllocWithCancellation(id, size, try artifacts.sha256ChecksumFromArtifactId(id), cancellation);
    defer store.allocator.free(bytes);
    const value = try std.json.parseFromSliceLeaky(Descriptor, a, bytes, .{ .allocate = .alloc_always });
    if (value.version != 1 or value.table_id != table_id or value.expires_ms <= now or value.expires_ms != scope.fencingToken()) return error.CatalogGenerationChanged;
    return value;
}

pub fn collectBounded(store: *artifacts.ArtifactStore, table: u64, store_identity: [32]u8, now: u64, limit: usize, cancellation: @import("antfly_cancellation").CancellationToken) !usize {
    if (limit == 0) return 0;
    const Visitor = struct {
        store: *artifacts.ArtifactStore,
        now: u64,
        limit: usize,
        deleted: usize = 0,
        fn visit(raw: *anyopaque, scope: artifacts.UploadScope, artifact_id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (scope.fencingToken() +| 30_000 >= self.now) return;
            if (self.deleted == self.limit) return error.RetainedCutCollectionBound;
            try self.store.delete(artifact_id);
            self.deleted += 1;
        }
    };
    var visitor: Visitor = .{ .store = store, .now = now, .limit = limit };
    store.visitScopedUploads(domain(table, store_identity), .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = now -| 30_000, .max_entries = limit }, cancellation) catch |err| switch (err) {
        error.RetainedCutCollectionBound, error.ArtifactEnumerationPaused => {},
        else => return err,
    };
    return visitor.deleted;
}

test "lake SQL accepted capability survives storage reopen and fences schema incarnation expiry and collection" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("sql-accepted-retained-cut");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var store = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    const identity = try @import("native_retained_cut.zig").storeIdentity(a, store.locator);
    var writer = store.artifactStore();
    const table: catalog.Table = .{ .id = 7, .physical_name = "hn", .schema_version = 1, .columns = &.{} };
    const now: u64 = 1000;
    const token = try save(a, &writer, identity, std.testing.io, .{ .table_id = 7, .schema_version = 1, .object_generation = 0, .expires_ms = now + ttl_ms, .metadata_location = "s3://warehouse/hn/metadata/original.json", .metadata_json = "{}", .table_uuid = "original", .snapshot_id = "42", .pending = .{ .lsn = 8, .key_fields = &.{"id"}, .changes = &.{} } }, .none);
    defer a.free(token);
    store.deinit();
    var reopened = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    defer reopened.deinit();
    var reader = reopened.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const loaded = try load(arena.allocator(), &reader, identity, token, table, now + 1, .none);
    try std.testing.expectEqual(@as(u64, 8), loaded.pending.lsn);
    try std.testing.expectEqualStrings("42", loaded.snapshot_id);
    var changed = table;
    changed.id = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, load(arena.allocator(), &reader, identity, token, changed, now + 1, .none));
    changed = table;
    changed.schema_version += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, load(arena.allocator(), &reader, identity, token, changed, now + 1, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, load(arena.allocator(), &reader, identity, token, table, now + ttl_ms, .none));
    try std.testing.expectEqual(@as(usize, 0), try collectBounded(&reader, 7, identity, now + 1, 1, .none));
    try std.testing.expectEqual(@as(usize, 1), try collectBounded(&reader, 7, identity, now + ttl_ms + 30_001, 1, .none));
}
