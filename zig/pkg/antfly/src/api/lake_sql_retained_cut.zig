// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
/// Pointer-free read authority. Allocation owners and runtime caches never
/// enter a retained capability; source/schema fields are immutable views.
pub const ReadTable = struct {
    id: u64,
    physical_name: []const u8,
    schema_version: u32,
    columns: []const catalog.Column,
    scope: ?catalog.Table.Scope,
    external_indexes: ?catalog.Table.ExternalIndexes,
    binding: local.serverless_external_source_catalog_binding.Binding,
    external_table_id: []const u8,
    source_uri: []const u8,
    credential_ref_id: ?[]const u8,
    credential_scope: ?[]const u8,
    snapshot_value: ?[]const u8,
    schema_fingerprint: []const u8,
    pub fn from(table: catalog.Table) ReadTable {
        const source = table.external_base_source.?;
        return .{ .id = table.id, .physical_name = table.physical_name, .schema_version = table.schema_version, .columns = table.columns, .scope = table.scope, .external_indexes = table.external_indexes, .binding = source.binding, .external_table_id = source.table_id, .source_uri = source.source_uri, .credential_ref_id = source.credential_ref_id, .credential_scope = source.credential_scope, .snapshot_value = source.snapshot_value, .schema_fingerprint = source.schema_fingerprint };
    }
    pub fn view(self: ReadTable) catalog.Table {
        return .{ .id = self.id, .physical_name = self.physical_name, .schema_version = self.schema_version, .columns = self.columns, .scope = self.scope, .external_indexes = self.external_indexes, .external_base_source = .{ .binding = self.binding, .table_id = @constCast(self.external_table_id), .source_uri = @constCast(self.source_uri), .credential_ref_id = if (self.credential_ref_id) |v| @constCast(v) else null, .credential_scope = if (self.credential_scope) |v| @constCast(v) else null, .snapshot_value = if (self.snapshot_value) |v| @constCast(v) else null, .schema_fingerprint = @constCast(self.schema_fingerprint) } };
    }
};
pub const Descriptor = struct {
    version: u16 = 1,
    /// Issued only by an authenticated SQL admission owner. The immutable
    /// artifact's domain, digest, expiry and opaque scope bind this read grant;
    /// workers cannot replace its schema or source binding through a ticket.
    authorization: ?struct { scope: [64]u8, table: ReadTable } = null,
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
    paged: ?Paged = null,
    pub fn jsonStringify(self: Descriptor, jw: anytype) !void {
        try jw.beginObject();
        inline for (@typeInfo(Descriptor).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "authorization")) {
                if (self.authorization) |grant| {
                    try jw.objectField(field);
                    try jw.write(grant);
                }
            } else {
                try jw.objectField(field);
                try jw.write(@field(self, field));
            }
        }
        try jw.endObject();
    }
};
pub const ChangePage = struct { begin: u32, count: u32, token: []const u8 };
pub const KeyPage = struct { first: [64]u8, last: [64]u8, token: []const u8 };
pub const Paged = struct { count: u32, bytes: u64, changes: []const ChangePage, keys: []const KeyPage, bloom: []const u8, key_bytes: u8 = 64 };
pub const distributed_rows = 128;
pub const Saved = struct {
    token: []const u8,
    rows: u64 = 0,
    bytes: u64 = 0,
    paged: bool = false,
    pub fn minimumSplits(self: Saved) u32 {
        return if (self.paged) @intCast(@max(1, (self.rows + distributed_rows - 1) / distributed_rows)) else 1;
    }
};
fn savePage(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, io: std.Io, descriptor: Descriptor, bytes: []const u8, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    const scope = try artifacts.UploadScope.forPublication(domain(descriptor.table_id, identity), descriptor.expires_ms, io);
    var object = try store.putScoped(scope, bytes, cancellation);
    defer object.deinit(store.allocator);
    return @import("lake_generation_envelope.zig").encode(a, "sql-page1:", object);
}
pub fn readPage(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, descriptor: Descriptor, token: []const u8, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    const envelope = try @import("lake_generation_envelope.zig").decode(a, "sql-page1:", token, max_bytes);
    defer if (envelope.generation) |generation| a.free(generation);
    const scope = (try artifacts.uploadScopeFromArtifactId(envelope.id)) orelse return error.CatalogGenerationChanged;
    if (!std.mem.eql(u8, &scope.domain, &domain(descriptor.table_id, identity)) or scope.fencingToken() != descriptor.expires_ms) return error.CatalogGenerationChanged;
    var scoped = store.*;
    scoped.allocator = a;
    return @import("lake_generation_envelope.zig").read(&scoped, envelope, cancellation);
}
pub fn bloomBit(key: [64]u8, bits: usize, seed: u64) usize {
    return @intCast(std.hash.Wyhash.hash(seed, &key) % bits);
}
pub fn saveDistributed(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, io: std.Io, descriptor: Descriptor, cancellation: @import("antfly_cancellation").CancellationToken) !Saved {
    // Reclaim publication scratch even when the result allocator is a query
    // arena. Keep accounting through the statement memory budget.
    var arena = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
    defer arena.deinit();
    var publisher = store.*;
    publisher.allocator = local.sql_memory_budget.reclaimingAllocator(a);
    const saved = try publishDistributed(arena.allocator(), &publisher, identity, io, descriptor, cancellation);
    return .{ .token = try a.dupe(u8, saved.token), .rows = saved.rows, .bytes = saved.bytes, .paged = saved.paged };
}
fn encodedSize(value: anytype) !usize {
    var buffer: [4096]u8 = undefined;
    var count = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(value, .{}, &count.writer);
    return @intCast(count.fullCount());
}
fn publishDistributed(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, io: std.Io, descriptor: Descriptor, cancellation: @import("antfly_cancellation").CancellationToken) !Saved {
    const pending = try descriptor.pending.materialized(a);
    const encoded_len = try encodedSize(pending.changes);
    if (encoded_len <= 64 * 1024) return .{ .token = try save(a, store, identity, io, descriptor, cancellation), .rows = pending.changes.len, .bytes = encoded_len };
    if (encoded_len > max_bytes) return error.QueryCandidateBudgetExceeded;
    const table = (descriptor.authorization orelse return error.SqlExecutionUnauthorized).table.view();
    const normalized = try @import("lake_sql_overlay.zig").Cut.normalizeForTable(a, pending, table);
    const digests = try a.alloc([64]u8, normalized.pending.changes.len);
    var key_arena = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
    defer key_arena.deinit();
    for (normalized.pending.changes, digests) |change, *key| {
        _ = key_arena.reset(.retain_capacity);
        key.* = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.digestHex(try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.row_commit.stable_key.identity(key_arena.allocator(), pending.key_fields, change.row, normalized.kinds));
    }
    std.mem.sort([64]u8, digests, {}, struct {
        fn less(_: void, l: [64]u8, r: [64]u8) bool {
            return std.mem.order(u8, &l, &r) == .lt;
        }
    }.less);
    const bloom = try a.alloc(u8, @min(@as(usize, 256 * 1024), @max(@as(usize, 64), digests.len * 2)));
    @memset(bloom, 0);
    for (digests) |key| for (0..4) |seed| {
        const bit = bloomBit(key, bloom.len * 8, seed);
        bloom[bit / 8] |= @as(u8, 1) << @as(u3, @intCast(bit % 8));
    };
    var keys: std.ArrayList(KeyPage) = .empty;
    var begin: usize = 0;
    while (begin < digests.len) {
        const end = @min(digests.len, begin + 4096);
        const page_allocator = local.sql_memory_budget.reclaimingAllocator(a);
        const bytes = try page_allocator.alloc(u8, (end - begin) * 32);
        defer page_allocator.free(bytes);
        for (digests[begin..end], 0..) |key, index| _ = try std.fmt.hexToBytes(bytes[index * 32 ..][0..32], &key);
        try keys.append(a, .{ .first = digests[begin], .last = digests[end - 1], .token = try savePage(a, store, identity, io, descriptor, bytes, cancellation) });
        begin = end;
    }
    var changes: std.ArrayList(ChangePage) = .empty;
    begin = 0;
    while (begin < normalized.pending.changes.len) {
        var end = begin;
        var bytes: usize = 2;
        while (end < normalized.pending.changes.len and end - begin < 4096) {
            const row_len = try encodedSize(normalized.pending.changes[end]);
            if (row_len > max_bytes) return error.QueryCandidateBudgetExceeded;
            if (end != begin and bytes + row_len + 1 > 32 * 1024) break;
            bytes += row_len + 1;
            end += 1;
        }
        const page_allocator = local.sql_memory_budget.reclaimingAllocator(a);
        const body = try std.json.Stringify.valueAlloc(page_allocator, normalized.pending.changes[begin..end], .{});
        defer page_allocator.free(body);
        try changes.append(a, .{ .begin = @intCast(begin), .count = @intCast(end - begin), .token = try savePage(a, store, identity, io, descriptor, body, cancellation) });
        begin = end;
    }
    var root = descriptor;
    root.pending = normalized.pending;
    root.pending.changes = &.{};
    root.paged = .{ .count = @intCast(normalized.pending.changes.len), .bytes = encoded_len, .changes = changes.items, .keys = keys.items, .key_bytes = 32, .bloom = try savePage(a, store, identity, io, descriptor, bloom, cancellation) };
    return .{ .token = try save(a, store, identity, io, root, cancellation), .rows = normalized.pending.changes.len, .bytes = encoded_len, .paged = true };
}
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
    return @import("lake_generation_envelope.zig").encode(a, prefix, object);
}
pub fn load(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, token: []const u8, table: catalog.Table, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !Descriptor {
    var value = try loadDescriptor(a, store, identity, token, table.id, now, cancellation);
    if (value.schema_version != table.schema_version or value.object_generation != (if (table.external_indexes) |indexes| indexes.object_generation else 0)) return error.CatalogGenerationChanged;
    if (value.paged) |index| {
        const changes = try a.alloc(@import("../serverless/lake_ingestion.zig").Batch.Change, index.count);
        var cursor: usize = 0;
        for (index.changes) |page| {
            if (page.begin != cursor) return error.CatalogGenerationChanged;
            const bytes = try readPage(a, store, identity, value, page.token, cancellation);
            const rows = try std.json.parseFromSliceLeaky([]const @import("../serverless/lake_ingestion.zig").Batch.Change, a, bytes, .{ .allocate = .alloc_always });
            if (rows.len != page.count or page.begin > changes.len or rows.len > changes.len - page.begin) return error.CatalogGenerationChanged;
            @memcpy(changes[page.begin..][0..rows.len], rows);
            cursor += rows.len;
        }
        if (cursor != changes.len) return error.CatalogGenerationChanged;
        value.pending.changes = changes;
        value.paged = null;
    }
    return value;
}
pub fn loadDescriptor(a: A, store: *artifacts.ArtifactStore, identity: [32]u8, token: []const u8, table_id: u64, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !Descriptor {
    if (!std.mem.startsWith(u8, token, prefix)) return error.CatalogGenerationChanged;
    const envelope = try @import("lake_generation_envelope.zig").decode(a, prefix, token, max_bytes);
    defer if (envelope.generation) |generation| a.free(generation);
    const id = envelope.id;
    const scope = (try artifacts.uploadScopeFromArtifactId(id)) orelse return error.CatalogGenerationChanged;
    if (!std.mem.eql(u8, &scope.domain, &domain(table_id, identity))) return error.CatalogGenerationChanged;
    const bytes = try @import("lake_generation_envelope.zig").read(store, envelope, cancellation);
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
