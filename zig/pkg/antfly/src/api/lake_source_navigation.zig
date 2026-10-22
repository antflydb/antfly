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

//! Authenticated, paged file identities and delete-aware publication deltas.
//! Query navigation resolves only reached files; legacy inventories remain valid.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const native = @import("lake_index_native_state.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const Inventory = local.serverless_external_source_types.Inventory;
pub const DeleteProof = struct { recipe: [32]u8, digest: [32]u8, source: [32]u8, data: [32]u8, count: u32, versions: tree.Ref };
const DeleteVersion = struct { recipe: [32]u8, etag: []const u8, version: []const u8 };
fn deleteRecipe(value: anytype) [32]u8 {
    var buffer: [4096]u8 = undefined;
    var writer: std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256) = .init(&buffer);
    std.json.Stringify.value(value, .{}, &writer.writer) catch unreachable;
    writer.writer.flush() catch unreachable;
    var digest: [32]u8 = undefined;
    writer.hasher.final(&digest);
    return digest;
}
fn exactVersion(version: []const u8) bool {
    return version.len != 0 and !std.mem.eql(u8, version, "null") and !std.mem.startsWith(u8, version, "object-stat:") and !std.mem.startsWith(u8, version, "iceberg:");
}
pub const Root = struct {
    version: u8 = 2,
    public_identity_version: u8 = 1,
    coverage: ?struct { version: u8 = 1, plan: ?[32]u8, data: [32]u8 } = null,
    domain: [32]u8,
    source: []const u8,
    schema: []const u8,
    snapshot: []const u8,
    count: u32,
    rows: ?u64 = null,
    bytes: ?u64 = null,
    files: ?tree.Ref = null,
    file_summary: ?Summary = null,
    public: ?tree.Ref = null,
    private: ?tree.Ref = null,
    delete_proof: ?DeleteProof = null,
    costing: []const Costing = &.{},
};
pub const Costing = struct { name: []const u8, files: ?tree.Ref, statistics: local.sql_catalog.ColumnStatistics, complete: bool = true };
pub const max_costing_columns = 64;
/// Every GC mode traverses the same authenticated navigation edges.
pub fn dependencies(root: Root) [4 + max_costing_columns]?tree.Ref {
    var refs: [4 + max_costing_columns]?tree.Ref = @splat(null);
    refs[0..4].* = .{ root.files, root.public, root.private, if (root.delete_proof) |proof| proof.versions else null };
    for (root.costing, 0..) |column, i| refs[i + 4] = column.files;
    return refs;
}
// Protocol 53 publishes an inventory header plus the authenticated file tree.
// Its pages are also reached through source_navigation by every GC/retention
// walker. Full inventory decoding is a compatibility fallback, not publication.
const inventory_magic = "ANLIP001";
pub fn inventoryVersion(bytes: []const u8) u16 {
    return if (std.mem.startsWith(u8, bytes, inventory_magic)) 1 else 0;
}
const InventoryDescriptor = struct {
    header: Inventory,
    domain: [32]u8,
    files: ?tree.Ref,
    count: u32,
};
pub fn encodeInventory(a: A, provider: *Provider, root: Root) ![]u8 {
    var header = provider.source.inventory;
    if (header.deleted_row_groups.len != 0) {
        const files = try a.alloc(local.serverless_external_source_types.FileEntry, header.files.len);
        defer a.free(files);
        for (header.files, files) |file, *out| {
            out.* = qualified(provider, file);
            out.row_groups = file.row_groups;
        }
        header.files = files;
        return local.serverless_external_source_mod.encodeInventoryAlloc(a, header);
    }
    if (root.count != header.files.len or !std.mem.eql(u8, root.source, header.source_id) or !std.mem.eql(u8, root.snapshot, header.snapshot_id) or !std.mem.eql(u8, root.schema, header.schema_fingerprint) or root.public_identity_version != header.row_identity_version) return error.InvalidNativeLakeFileState;
    header.files = &.{};
    const bytes = try std.json.Stringify.valueAlloc(a, InventoryDescriptor{ .header = header, .domain = root.domain, .files = root.files, .count = root.count }, .{});
    defer a.free(bytes);
    return std.mem.concat(a, u8, &.{ inventory_magic, bytes });
}
pub fn decodeInventory(a: A, store: stores.ArtifactStore, bytes: []const u8, token: Token) !Inventory {
    const codec = local.serverless_external_source_mod;
    if (!std.mem.startsWith(u8, bytes, inventory_magic)) return codec.decodeInventoryAlloc(a, bytes) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidNativeLakeFileState,
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const descriptor = std.json.parseFromSliceLeaky(InventoryDescriptor, work, bytes[inventory_magic.len..], .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidNativeLakeFileState,
    };
    if (descriptor.header.files.len != 0 or descriptor.header.deleted_row_groups.len != 0 or descriptor.count > 1_000_000 or ((descriptor.files == null) != (descriptor.count == 0))) return error.InvalidNativeLakeFileState;
    if (descriptor.files) |ref| {
        try ref.validate();
        if (ref.records != descriptor.count) return error.InvalidNativeLakeFileState;
    }
    const encoded_header = codec.encodeInventoryAlloc(work, descriptor.header) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidNativeLakeFileState,
    };
    var inventory = codec.decodeInventoryAlloc(a, encoded_header) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidNativeLakeFileState,
    };
    errdefer inventory.deinit(a);
    const files = try a.alloc(local.serverless_external_source_types.FileEntry, descriptor.count);
    var count: usize = 0;
    errdefer {
        for (files[0..count]) |*file| file.deinit(a);
        a.free(files);
    }
    var reads: u64 = (local.serverless_external_source_mod.codec.DecodeLimits{}).max_artifact_bytes;
    var writes: u64 = 0;
    var artifacts_ = store;
    var pages: pages_mod.PageStore = .{ .domain = descriptor.domain, .artifacts = &artifacts_, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var cursor = try tree.Cursor.init(a, pages.store(), descriptor.files, "", null);
    defer cursor.deinit();
    var row_arena = std.heap.ArenaAllocator.init(a);
    defer row_arena.deinit();
    while (try cursor.next()) |entry| {
        try token.check();
        if (count == files.len) return error.InvalidNativeLakeFileState;
        _ = row_arena.reset(.retain_capacity);
        const ra = row_arena.allocator();
        const record = std.json.parseFromSliceLeaky(Record, ra, entry.value, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidNativeLakeFileState,
        };
        if (!std.mem.eql(u8, entry.key, &key(record.file.file_id))) return error.InvalidNativeLakeFileState;
        var one = descriptor.header;
        one.files = @constCast(&[_]local.serverless_external_source_types.FileEntry{record.file});
        const encoded = codec.encodeInventoryAlloc(ra, one) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidNativeLakeFileState,
        };
        var owned = codec.decodeInventoryAlloc(a, encoded) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidNativeLakeFileState,
        };
        defer owned.deinit(a);
        files[count] = owned.files[0];
        a.free(owned.files);
        owned.files = &.{};
        count += 1;
    }
    if (count != files.len) return error.InvalidNativeLakeFileState;
    std.mem.sort(local.serverless_external_source_types.FileEntry, files, {}, struct {
        fn less(_: void, l: local.serverless_external_source_types.FileEntry, r: local.serverless_external_source_types.FileEntry) bool {
            return std.mem.lessThan(u8, l.file_id, r.file_id);
        }
    }.less);
    var view = inventory;
    view.files = files;
    view.validateAlloc(a) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidNativeLakeFileState,
    };
    a.free(inventory.files);
    inventory.files = files;
    count = 0;
    return inventory;
}
const Proof = struct { id: [32]u8, digest: [32]u8, index: usize };
pub const Summary = struct {
    first: [32]u8,
    last: [32]u8,
    digest: [32]u8,
    count: u64,
    fn encode(self: @This(), a: A) ![]u8 {
        const bytes = try a.alloc(u8, 104);
        bytes[0..32].* = self.first;
        bytes[32..64].* = self.last;
        bytes[64..96].* = self.digest;
        std.mem.writeInt(u64, bytes[96..104], self.count, .big);
        return bytes;
    }
    fn decode(bytes: []const u8, count: u64) !@This() {
        if (bytes.len != 104 or std.mem.readInt(u64, bytes[96..104], .big) != count or count == 0 or std.mem.order(u8, bytes[0..32], bytes[32..64]) == .gt) return error.InvalidNativeLakeFileState;
        return .{ .first = bytes[0..32].*, .last = bytes[32..64].*, .digest = bytes[64..96].*, .count = count };
    }
};
fn proofHash(id: [32]u8, digest: [32]u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-file-set-proof-v1");
    hash.update(&id);
    hash.update(&digest);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn reduceFiles(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    var result: ?Summary = null;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (entries) |entry| {
        _ = scratch.reset(.retain_capacity);
        const summary = if (height == 0) leaf: {
            const record = try std.json.parseFromSliceLeaky(Record, scratch.allocator(), entry.value, .{});
            const id = key(record.file.file_id);
            if (!std.mem.eql(u8, entry.key, &id)) return error.InvalidNativeLakeFileState;
            break :leaf Summary{ .first = id, .last = id, .count = 1, .digest = proofHash(id, record.digest) };
        } else branch: {
            if (entry.aggregate.len == 0) return try a.alloc(u8, 0);
            break :branch try Summary.decode(entry.aggregate, entry.child.?.records);
        };
        if (result) |*value| {
            if (std.mem.order(u8, &value.last, &summary.first) != .lt) return error.InvalidNativeLakeFileState;
            value.last = summary.last;
            value.count = try std.math.add(u64, value.count, summary.count);
            for (&value.digest, summary.digest) |*byte, other| byte.* ^= other;
        } else result = summary;
    }
    return if (result) |value| value.encode(a) else try a.alloc(u8, 0);
}
fn proofLower(proofs: []const Proof, id: [32]u8, inclusive: bool) usize {
    var lo: usize = 0;
    var hi = proofs.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const order = std.mem.order(u8, &proofs[mid].id, &id);
        if (order == .lt or (inclusive and order == .eq)) lo = mid + 1 else hi = mid;
    }
    return lo;
}
pub const Record = struct { file: local.serverless_external_source_types.FileEntry, digest: [32]u8, deletes: [32]u8 };
/// Logical census commitment stays stable across attempt-scoped page rewrites.
/// Include the predecessor separately when authorizing a partial checkpoint.
pub fn cutIdentity(a: A, root: Root) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u8, 2), .identity = root.public_identity_version, .domain = root.domain, .source = root.source, .schema = root.schema, .snapshot = root.snapshot, .count = root.count, .summary = root.file_summary, .legacy_files = if (root.file_summary == null and root.count != 0) root.files else null }, .{});
    defer a.free(bytes);
    var hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &hash, .{});
    return hash;
}
pub const Delta = struct {
    source_cut: ?[32]u8 = null,
    prior_cut: ?[32]u8 = null,
    selection: ?native.Selection = null,
    public_navigation: ?@import("lake_index_ordered_files.zig").Public = null,
    source: []const u8,
    schema: []const u8,
    prior_snapshot: ?[]const u8,
    prior_count: u32,
    files: []native.File,
    changed: []bool,
    prior_changed: []const native.File,
    by_id: local.serverless_query_lake_inventory_catalog.Positions,
    pub fn count(self: Delta) usize {
        return if (self.selection) |selection| selection.files.len else self.files.len;
    }
    pub fn file(self: Delta, index: usize) !native.File {
        return if (self.selection) |selection| selection.file(index) else self.files[index];
    }
    pub fn isChanged(self: Delta, index: usize) bool {
        return if (self.selection) |selection| selection.changed(index) else self.changed[index];
    }
};
fn qualified(provider: *Provider, file: local.serverless_external_source_types.FileEntry) local.serverless_external_source_types.FileEntry {
    var result = file;
    if (provider.source.verified_files) |files| if (files.get(file.file_id)) |verified| {
        result.etag = verified.etag;
        result.version_id = verified.version_id;
    };
    result.row_groups = &.{};
    result.lower_bounds = &.{};
    result.upper_bounds = &.{};
    return result;
}
fn key(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
pub fn load(a: A, store: stores.ArtifactStore, ref: Ref, token: Token, cached: ?artifacts.CachedRead) !Root {
    if (ref.kind != .doc_values or (ref.metadata_version != 1 and ref.metadata_version != 2 and ref.metadata_version != 3 and ref.metadata_version != 4) or ref.byte_len > 64 * 1024) return error.InvalidNativeLakeFileState;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeFileState;
    if (root.version != ref.metadata_version or !std.mem.eql(u8, &scope.domain, &root.domain) or root.source.len == 0 or root.schema.len == 0 or root.snapshot.len == 0) return error.InvalidNativeLakeFileState;
    if ((root.public_identity_version != 1 and root.public_identity_version != 2) or (root.public_identity_version == 2 and root.version < 4)) return error.InvalidNativeLakeFileState;
    if (root.delete_proof) |proof| {
        try proof.versions.validate();
        if (proof.count == 0 or proof.versions.records != proof.count or root.coverage == null or !std.mem.eql(u8, &proof.data, &root.coverage.?.data)) return error.InvalidNativeLakeFileState;
    }
    if (root.coverage) |proof| if (root.version < 2 or proof.version != 1) return error.InvalidNativeLakeFileState;
    for ([_]?tree.Ref{ root.files, root.public, root.private }) |r| {
        if ((r == null) != (root.count == 0)) return error.InvalidNativeLakeFileState;
        if (r) |value| {
            try value.validate();
            if (value.records != root.count) return error.InvalidNativeLakeFileState;
        }
    }
    if (root.costing.len > max_costing_columns or (root.costing.len != 0 and root.version < 3)) return error.InvalidNativeLakeFileState;
    for (root.costing, 0..) |column, i| {
        if (column.name.len == 0 or column.name.len > 512 or !std.mem.eql(u8, column.name, column.statistics.name) or column.statistics.frequencies.len > 4) return error.InvalidNativeLakeFileState;
        if (column.files) |ref_| {
            try ref_.validate();
            if (ref_.records > root.count or (column.complete and ref_.records != root.count)) return error.InvalidNativeLakeFileState;
        }
        for (root.costing[0..i]) |prior| if (std.mem.eql(u8, prior.name, column.name)) return error.InvalidNativeLakeFileState;
        if ((column.statistics.non_null_rows orelse 0) > (column.statistics.population_rows orelse 0)) return error.InvalidNativeLakeFileState;
    }
    if (root.file_summary) |summary| if (summary.count != root.count or std.mem.order(u8, &summary.first, &summary.last) == .gt) return error.InvalidNativeLakeFileState;
    return root;
}
fn publishDeleteProof(a: A, storage_: tree.Store, provider: *Provider, data: [32]u8) !?DeleteProof {
    const source = provider.source;
    // A retained catalog snapshot plus exact immutable provider versions is
    // the proof contract. Unversioned stores keep fresh HEAD qualification.
    const plan = source.scanner.iceberg_delete_plan orelse return null;
    if (!source.immutable_objects or source.snapshot_pin == null) return null;
    if (plan.files.len == 0 or plan.files.len > std.math.maxInt(u32)) return null;
    try provider.context.ensureActive();
    const location = try local.serverless_query_lake_range_io.objectLocationForUri(plan.files[0].file_path);
    const client = if (source.context_store) |context_store| context_store.base else source.scanner.object_reader.client;
    if (!try client.supportsObjectVersions(location.bucket, provider.context.cancellation)) return null;
    const qualification = try local.serverless_query_lake_serving.DeleteQualification.create(a);
    defer qualification.destroy();
    const evidence_set = (try qualification.acquire(source.scanner.object_reader.client, plan, provider.context)) orelse return null;
    for (evidence_set.versions) |version| if (!exactVersion(version.version)) return null;
    const coverage = try @import("lake_index_coverage.zig").finishDigest(source, provider.context, data);
    if (!std.mem.eql(u8, &coverage.delete_objects, &evidence_set.digest)) return error.ExternalLakeIndexSourceChanged;
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = storage_ };
    defer builder.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (plan.files, evidence_set.versions, 0..) |file, version, index| {
        try provider.context.ensureActive();
        _ = scratch.reset(.retain_capacity);
        var key_: [32]u8 = @splat(0);
        std.mem.writeInt(u32, key_[28..32], @intCast(index), .big);
        const bytes = try std.json.Stringify.valueAlloc(scratch.allocator(), DeleteVersion{ .recipe = deleteRecipe(file), .etag = version.etag, .version = version.version }, .{});
        try builder.add(&key_, bytes);
    }
    return .{ .recipe = evidence_set.recipe, .digest = evidence_set.digest, .source = coverage.source, .data = data, .count = @intCast(plan.files.len), .versions = (try builder.finish()).? };
}
/// Allocates only one reached delete selector; no archive-wide version map.
pub fn deleteVersion(a: A, store: stores.ArtifactStore, root: Root, index: usize, file: local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile, token: Token, context: local.serverless_query_lake_read_context.Context) !local.serverless_query_lake_serving.DeleteQualification.Version {
    try context.ensureActive();
    try token.check();
    const proof = root.delete_proof orelse return error.InvalidNativeLakeFileState;
    if (index >= proof.count) return error.InvalidNativeLakeFileState;
    var reads: u64 = 8 * 1024 * 1024;
    var writes: u64 = 0;
    var backend = store;
    var pages: pages_mod.PageStore = .{ .domain = root.domain, .artifacts = &backend, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var key_: [32]u8 = @splat(0);
    std.mem.writeInt(u32, key_[28..32], @intCast(index), .big);
    const bytes = (try lookup(sa, pages.store(), proof.versions, &key_)) orelse return error.InvalidNativeLakeFileState;
    return ownDeleteVersion(a, bytes, file);
}
fn ownDeleteVersion(a: A, bytes: []const u8, file: local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile) !local.serverless_query_lake_serving.DeleteQualification.Version {
    const parsed = try std.json.parseFromSlice(DeleteVersion, a, bytes, .{});
    defer parsed.deinit();
    const record = parsed.value;
    if (!std.mem.eql(u8, &record.recipe, &deleteRecipe(file)) or !exactVersion(record.version)) return error.InvalidNativeLakeFileState;
    const etag = try a.dupe(u8, record.etag);
    errdefer a.free(etag);
    return .{ .etag = etag, .version = try a.dupe(u8, record.version), .sequence = file.data_sequence_number, .position = file.content == .position_deletes };
}
fn lookup(a: A, storage: tree.Store, root: ?tree.Ref, id: []const u8) !?[]u8 {
    var cursor = try tree.Cursor.init(a, storage, root, id, null);
    defer cursor.deinit();
    const found = (try cursor.next()) orelse return null;
    if (!std.mem.eql(u8, found.key, id)) return null;
    return try a.dupe(u8, found.value);
}
pub const Batch = struct {
    arena: std.heap.ArenaAllocator,
    changes: std.ArrayList(tree.Mutation) = .empty,
    pub fn add(self: *@This(), id: []const u8, value: ?[]const u8) !void {
        const a = self.arena.allocator();
        try self.changes.append(a, .{ .key = try a.dupe(u8, id), .value = if (value) |v| try a.dupe(u8, v) else null });
    }
    pub fn flush(self: *@This(), storage: tree.Store, root: *?tree.Ref) !void {
        if (self.changes.items.len == 0) return;
        std.mem.sort(tree.Mutation, self.changes.items, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.order(u8, l.key, r.key) == .lt;
            }
        }.less);
        root.* = try tree.apply(self.arena.allocator(), storage, root.*, self.changes.items);
        _ = self.arena.reset(.retain_capacity);
        self.changes = .empty;
    }
};
pub const BuildStats = struct { prior_pages: usize = 0, prior_records: usize = 0, skipped_subtrees: usize = 0, rehashed_files: usize = 0, cached_census: bool = false };
pub const Built = struct {
    ref: Ref,
    root: Root,
    delta: Delta,
    stats: BuildStats,
    pub fn deinit(self: Built) void {
        if (self.delta.selection) |selection| selection.census.release();
    }
};
pub fn build(a: A, out: A, store: *stores.ArtifactStore, provider: *Provider, prior: ?Root, token: Token) !Built {
    return buildProven(a, out, store, provider, prior, token, null);
}
pub fn buildProven(a: A, out: A, store: *stores.ArtifactStore, provider: *Provider, prior: ?Root, token: Token, data: ?[32]u8) !Built {
    const inv = provider.source.inventory;
    const compatible = if (prior) |old| std.mem.eql(u8, old.source, inv.source_id) and std.mem.eql(u8, old.schema, inv.schema_fingerprint) and std.mem.eql(u8, &old.domain, &store.upload_scope.?.domain) else false;
    var root: Root = .{ .version = if (inv.row_identity_version == 2) 4 else 2, .public_identity_version = inv.row_identity_version, .domain = store.upload_scope.?.domain, .source = inv.source_id, .schema = inv.schema_fingerprint, .snapshot = inv.snapshot_id, .count = @intCast(inv.files.len) };
    root.rows = 0;
    root.bytes = 0;
    for (inv.files) |file| {
        root.rows.? +|= file.row_count;
        root.bytes.? +|= file.byte_len;
    }
    if (data) |digest| root.coverage = .{ .plan = if (provider.source.immutable_objects) provider.source.plan_identity else null, .data = digest };
    if (compatible) {
        root.files = prior.?.files;
        root.private = prior.?.private;
    }
    var reads: u64 = tree.max_page_bytes;
    var writes: u64 = tree.max_page_bytes;
    var pages: pages_mod.PageStore = .{ .domain = root.domain, .attempt = store.upload_scope.?.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    var file_storage = pages.store();
    file_storage.reduce = reduceFiles;
    if (data) |digest| root.delete_proof = try publishDeleteProof(a, pages.store(), provider, digest);
    var files: Batch = .{ .arena = .init(a) };
    defer files.arena.deinit();
    var private: Batch = .{ .arena = .init(a) };
    defer private.arena.deinit();
    const PublicEntry = struct { digest: [32]u8, file: []const u8 };
    var native_by_id: local.serverless_query_lake_inventory_catalog.Positions = .{ .catalog = provider.source.catalog() };
    try native_by_id.ensureTotalCapacity(out, @intCast(inv.files.len));
    for (inv.files, 0..) |file, i| native_by_id.putAssumeCapacity(file.file_id, i);
    var stats: BuildStats = .{};
    const Census = local.serverless_query_lake_file_census;
    var census: ?*Census.Census = null;
    defer if (census) |value| value.release();
    if (provider.source.immutable_objects) if (provider.source.catalog()) |catalog| {
        var domain_hash = std.crypto.hash.Blake3.init(.{});
        domain_hash.update("native-census-domain-v1");
        domain_hash.update(&root.domain);
        for ([_][]const u8{ inv.source_id, inv.schema_fingerprint }) |part| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, part.len, .little);
            domain_hash.update(&length);
            domain_hash.update(part);
        }
        const deletes = try std.json.Stringify.valueAlloc(a, .{ .groups = inv.deleted_row_groups, .plan = provider.source.scanner.iceberg_delete_plan, .versions = if (provider.source.prepared_deletes) |prepared| prepared.object_versions else null }, .{});
        defer a.free(deletes);
        domain_hash.update(deletes);
        const Fingerprints = struct {
            a: A,
            provider: *Provider,
            positions: *local.serverless_query_lake_inventory_catalog.Positions,
            calls: usize = 0,
            fn get(raw: *anyopaque, id: []const u8) ![32]u8 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                const position = self.positions.get(id) orelse return error.InvalidNativeLakeFileState;
                var arena = std.heap.ArenaAllocator.init(self.a);
                defer arena.deinit();
                return native.identity(arena.allocator(), self.provider, self.provider.source.inventory.files[position]);
            }
        };
        var fingerprints: Fingerprints = .{ .a = a, .provider = provider, .positions = &native_by_id };
        var census_domain: [32]u8 = undefined;
        domain_hash.final(&census_domain);
        census = catalog.prepareCensus(census_domain, inv.files, .{ .ptr = &fingerprints, .get = Fingerprints.get }, provider.context) catch |err| switch (err) {
            error.OutOfMemory, error.SqlMemoryLimitExceeded => null,
            else => return err,
        };
        if (census != null) {
            stats.cached_census = true;
            stats.rehashed_files = fingerprints.calls;
        }
    };
    const current = try out.alloc(native.File, if (census == null) inv.files.len else 0);
    const changed = try out.alloc(bool, current.len);
    @memset(changed, true);
    var selection: ?native.Selection = if (census) |value| .{ .files = inv.files, .census = value, .positions = native_by_id } else null;
    for (current, inv.files[0..current.len]) |*state, file| state.* = .{ .id = file.file_id, .digest = undefined };
    const proofs = try a.alloc(Proof, if (census == null) inv.files.len else 0);
    defer a.free(proofs);
    const prefix = try a.alloc([32]u8, if (census == null) inv.files.len + 1 else 1);
    defer a.free(prefix);
    prefix[0] = @splat(0);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    if (census == null or !compatible) for (inv.files, 0..) |file, i| {
        try provider.context.ensureActive();
        try token.check();
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const digest = if (census) |value| (value.find(key(file.file_id)) orelse return error.InvalidNativeLakeFileState).digest else try native.identity(sa, provider, file);
        if (census == null) current[i].digest = digest;
        if (census == null) {
            stats.rehashed_files += 1;
            proofs[i] = .{ .id = key(file.file_id), .digest = digest, .index = i };
        }
        // No descriptor serialization or delete fingerprinting is owed for an
        // unchanged compatible subtree. Only mutations need full records.
        if (!compatible) {
            const deletes = if (provider.source.prepared_deletes) |prepared| try prepared.fileFingerprint(file.file_id) else local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint();
            const bytes = try std.json.Stringify.valueAlloc(sa, Record{ .file = qualified(provider, file), .digest = digest, .deletes = deletes }, .{});
            try files.add(&key(file.file_id), bytes);
            try private.add(&digest, file.file_id);
            if (files.changes.items.len >= 64) {
                try files.flush(file_storage, &root.files);
                try private.flush(pages.store(), &root.private);
            }
        }
    };
    var prior_changed: std.ArrayList(native.File) = .empty;
    std.mem.sort(Proof, proofs, {}, struct {
        fn less(_: void, l: Proof, r: Proof) bool {
            return std.mem.order(u8, &l.id, &r.id) == .lt;
        }
    }.less);
    for (proofs, 0..) |proof, i| {
        prefix[i + 1] = prefix[i];
        const digest = proofHash(proof.id, proof.digest);
        for (&prefix[i + 1], digest) |*byte, other| byte.* ^= other;
    }
    if (census) |value| {
        root.file_summary = if (value.summary()) |summary| .{ .first = summary.first, .last = summary.last, .count = summary.count, .digest = summary.digest } else null;
    } else root.file_summary = if (proofs.len != 0) .{ .first = proofs[0].id, .last = proofs[proofs.len - 1].id, .count = proofs.len, .digest = prefix[proofs.len] } else null;
    if (compatible) {
        const Diff = struct {
            a: A,
            out: A,
            storage: tree.Store,
            file_storage: tree.Store,
            provider: *Provider,
            census: ?*Census.Census,
            by_id: *const local.serverless_query_lake_inventory_catalog.Positions,
            proofs: []const Proof,
            prefix: []const [32]u8,
            current: []const native.File,
            changed: []bool,
            selection: ?*native.Selection,
            prior_changed: *std.ArrayList(native.File),
            files: *Batch,
            private: *Batch,
            root: *Root,
            token: Token,
            stats: *BuildStats,
            fn unchanged(self: *@This(), summary: Summary) !bool {
                if (self.census) |value| {
                    const found = value.range(summary.first, summary.last);
                    if (found.count != summary.count or !std.mem.eql(u8, &found.digest, &summary.digest)) return false;
                    try self.selection.?.unchanged.append(self.out, .{ .first = summary.first, .last = summary.last });
                    self.stats.skipped_subtrees += 1;
                    return true;
                }
                const lower = proofLower(self.proofs, summary.first, false);
                const upper = proofLower(self.proofs, summary.last, true);
                if (upper - lower != summary.count) return false;
                var digest = self.prefix[upper];
                for (&digest, self.prefix[lower]) |*byte, other| byte.* ^= other;
                if (!std.mem.eql(u8, &digest, &summary.digest)) return false;
                for (self.proofs[lower..upper]) |proof| self.changed[proof.index] = false;
                self.stats.skipped_subtrees += 1;
                return true;
            }
            pub fn childSummary(self: *@This(), ref: tree.Ref, bytes: []const u8) anyerror!void {
                try self.provider.context.ensureActive();
                try self.token.check();
                if (bytes.len != 0 and try self.unchanged(try Summary.decode(bytes, ref.records))) return;
                self.stats.prior_pages += 1;
                try tree.walkPage(self.a, self.storage, ref, self);
            }
            pub fn record(self: *@This(), id: []const u8, bytes: []const u8) anyerror!void {
                self.stats.prior_records += 1;
                var arena_ = std.heap.ArenaAllocator.init(self.a);
                defer arena_.deinit();
                const sa = arena_.allocator();
                const old = try std.json.parseFromSliceLeaky(Record, sa, bytes, .{});
                const old_id = key(old.file.file_id);
                if (!std.mem.eql(u8, id, &old_id)) return error.InvalidNativeLakeFileState;
                if (self.census) |value| {
                    if (value.find(old_id)) |entry| {
                        if (!std.mem.eql(u8, old.file.file_id, entry.id)) return error.InvalidNativeLakeFileState;
                        if (std.mem.eql(u8, &old.digest, &entry.digest)) {
                            try self.selection.?.unchanged.append(self.out, .{ .first = old_id, .last = old_id });
                            return;
                        }
                    }
                }
                const index = proofLower(self.proofs, old_id, false);
                if (index < self.proofs.len and std.mem.eql(u8, &self.proofs[index].id, &old_id)) {
                    const i = self.proofs[index].index;
                    if (!std.mem.eql(u8, old.file.file_id, self.current[i].id)) return error.InvalidNativeLakeFileState;
                    self.changed[i] = !std.mem.eql(u8, &old.digest, &self.current[i].digest);
                    if (!self.changed[i]) return;
                }
                try self.prior_changed.append(self.out, .{ .id = try self.out.dupe(u8, old.file.file_id), .digest = old.digest, .aggregate_digest = @import("lake_index_native_aggregates.zig").inventoryFileIdentity(self.provider.source.inventory, old.file) });
                try self.files.add(id, null);
                try self.private.add(&old.digest, null);
                if (self.files.changes.items.len >= 64) {
                    try self.files.flush(self.file_storage, &self.root.files);
                    try self.private.flush(self.storage, &self.root.private);
                }
            }
        };
        var diff: Diff = .{ .a = a, .out = out, .storage = pages.store(), .file_storage = file_storage, .provider = provider, .census = census, .by_id = &native_by_id, .proofs = proofs, .prefix = prefix, .current = current, .changed = changed, .selection = if (selection) |*value| value else null, .prior_changed = &prior_changed, .files = &files, .private = &private, .root = &root, .token = token, .stats = &stats };
        if (prior.?.files) |ref| {
            if (prior.?.file_summary == null or !try diff.unchanged(prior.?.file_summary.?)) {
                stats.prior_pages += 1;
                try tree.walkPage(a, pages.store(), ref, &diff);
            }
        }
        try files.flush(file_storage, &root.files);
        try private.flush(pages.store(), &root.private);
        if (selection) |*value| value.finish();
        var planned = native.Plan{ .a = a, .files = current, .changed = changed, .selection = selection, .by_id = native_by_id };
        var changes = planned.iterator(true);
        while (try changes.next()) |item| {
            const file = inv.files[item.ordinal];
            const state = item.file;
            _ = scratch.reset(.retain_capacity);
            const sa = scratch.allocator();
            const deletes = if (provider.source.prepared_deletes) |prepared| try prepared.fileFingerprint(file.file_id) else local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint();
            const bytes = try std.json.Stringify.valueAlloc(sa, Record{ .file = qualified(provider, file), .digest = state.digest, .deletes = deletes }, .{});
            try files.add(&key(file.file_id), bytes);
            try private.add(&state.digest, file.file_id);
            if (files.changes.items.len >= 64) {
                try files.flush(file_storage, &root.files);
                try private.flush(pages.store(), &root.private);
            }
        }
    }
    try files.flush(file_storage, &root.files);
    try private.flush(pages.store(), &root.private);
    if (compatible and inv.row_identity_version == 2 and prior.?.public_identity_version == 2) {
        // Public keys name immutable files independently of the snapshot. Only
        // true membership changes touch this tree; replacements retain a key.
        root.public = prior.?.public;
        var mutations: Batch = .{ .arena = .init(a) };
        defer mutations.arena.deinit();
        var replaced: std.StringHashMapUnmanaged(void) = .empty;
        defer replaced.deinit(a);
        for (prior_changed.items) |old| {
            if (native_by_id.contains(old.id)) {
                try replaced.put(a, old.id, {});
                continue;
            }
            try mutations.add(&local.storage_rowsource_identity.navigationDigest(2, inv.source_id, inv.snapshot_id, old.id), null);
            if (mutations.changes.items.len >= 64) try mutations.flush(pages.store(), &root.public);
        }
        var planned = native.Plan{ .a = a, .files = current, .changed = changed, .selection = selection, .by_id = native_by_id };
        var changes = planned.iterator(true);
        while (try changes.next()) |item| {
            const file = item.file;
            if (replaced.contains(file.id)) continue;
            try mutations.add(&local.storage_rowsource_identity.navigationDigest(2, inv.source_id, inv.snapshot_id, file.id), file.id);
            if (mutations.changes.items.len >= 64) try mutations.flush(pages.store(), &root.public);
        }
        try mutations.flush(pages.store(), &root.public);
    } else {
        const same_membership = membership: {
            if (!compatible or prior.?.public_identity_version != inv.row_identity_version or prior.?.count != inv.files.len or !std.mem.eql(u8, prior.?.snapshot, inv.snapshot_id)) break :membership false;
            for (prior_changed.items) |old| if (!native_by_id.contains(old.id)) break :membership false;
            break :membership true;
        };
        if (same_membership) root.public = prior.?.public else {
            const public_entries = try a.alloc(PublicEntry, inv.files.len);
            defer a.free(public_entries);
            for (inv.files, public_entries) |file, *entry| entry.* = .{ .digest = local.storage_rowsource_identity.navigationDigest(inv.row_identity_version, inv.source_id, inv.snapshot_id, file.file_id), .file = file.file_id };
            std.mem.sort(PublicEntry, public_entries, {}, struct {
                fn less(_: void, l: PublicEntry, r: PublicEntry) bool {
                    return std.mem.order(u8, &l.digest, &r.digest) == .lt;
                }
            }.less);
            const PublicSource = struct {
                entries: []const PublicEntry,
                position: usize = 0,
                pub fn next(self: *@This()) !?tree.Cursor.Record {
                    if (self.position == self.entries.len) return null;
                    const entry = &self.entries[self.position];
                    self.position += 1;
                    return .{ .key = &entry.digest, .value = entry.file };
                }
            };
            var public_source: PublicSource = .{ .entries = public_entries };
            root.public = try tree.buildSorted(a, pages.store(), &public_source);
        }
    }
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(bytes);
    var uploaded = try store.putWithCancellation(bytes, token);
    defer uploaded.deinit(store.allocator);
    const prior_snapshot = if (compatible) try out.dupe(u8, prior.?.snapshot) else null;
    const ref: Ref = .{ .kind = .doc_values, .metadata_version = root.version, .artifact_id = try out.dupe(u8, uploaded.artifact_id), .checksum = try out.dupe(u8, uploaded.checksum), .byte_len = uploaded.byte_len };
    const source_cut = try cutIdentity(a, root);
    const prior_cut = if (compatible) try cutIdentity(a, prior.?) else null;
    // Complete every fallible operation before transferring the census lease.
    if (selection) |value| _ = value.census.retain();
    return .{ .root = root, .stats = stats, .ref = ref, .delta = .{ .source_cut = source_cut, .prior_cut = prior_cut, .public_navigation = .{ .identity_version = root.public_identity_version, .root = root.public, .count = root.count, .source = inv.source_id, .snapshot = inv.snapshot_id }, .source = inv.source_id, .schema = inv.schema_fingerprint, .prior_count = if (compatible) prior.?.count else 0, .prior_snapshot = prior_snapshot, .selection = selection, .files = current, .changed = changed, .prior_changed = prior_changed.items, .by_id = native_by_id } };
}
/// Incremental per-column, per-file sketches piggyback on native replay.
/// Each changed leaf replaces its previous sketch; HLLs are never subtracted.
pub const CostingBuilder = struct {
    const Sketch = local.sql_catalog.StatisticsSketch;
    a: A,
    out: A,
    arena: std.heap.ArenaAllocator,
    store: *stores.ArtifactStore,
    provider: *Provider,
    root: Root,
    token: Token,
    current: Ref,
    columns: []const []const u8,
    costing: []Costing,
    batches: []Batch,
    seen: []bool,
    reads: u64 = std.math.maxInt(u64),
    writes: u64 = std.math.maxInt(u64),
    mutex: std.Io.Mutex = .init,
    same_files: bool = false,
    checkpoint: ?struct { ptr: *anyopaque, save: *const fn (*anyopaque, Ref) anyerror!void } = null,
    fn storage(self: *@This(), pages: *pages_mod.PageStore) tree.Store {
        pages.* = .{ .domain = self.root.domain, .attempt = self.store.upload_scope.?.attempt, .artifacts = self.store, .cancellation = self.token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .streaming = true };
        var result = pages.store();
        result.reduce = reduce;
        return result;
    }
    fn reduce(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
        var result: Sketch = .{};
        for (entries) |entry| result.merge(try Sketch.decode(if (height == 0) entry.value else entry.aggregate));
        return a.dupe(u8, &result.encode());
    }
    pub fn init(a: A, out: A, store: *stores.ArtifactStore, provider: *Provider, current: Ref, previous: ?Root, columns: []const []const u8, token: Token) !@This() {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const ca = arena.allocator();
        const root = try load(ca, store.*, current, token, null);
        var selected: std.ArrayList([]const u8) = .empty;
        var planned_bytes = (try std.json.Stringify.valueAlloc(ca, root, .{})).len;
        for (columns) |name| {
            if (selected.items.len == max_costing_columns) break;
            if (name.len == 0 or name.len > 512) continue;
            const summary_bytes = 1024 + name.len * 2;
            if (summary_bytes > 64 * 1024 -| planned_bytes) continue;
            const schema = if (provider.source.iceberg_schema) |schema_| schema_.columns else provider.schema_contract;
            const kind = for (schema) |column| {
                if (std.mem.eql(u8, name, column.name)) break column.kind;
            } else continue;
            if (!std.mem.eql(u8, kind, "integer") and !std.mem.eql(u8, kind, "number") and !std.mem.eql(u8, kind, "numeric") and !std.mem.eql(u8, kind, "string") and !std.mem.eql(u8, kind, "boolean") and !std.mem.eql(u8, kind, "datetime")) continue;
            try selected.append(ca, name);
            planned_bytes += summary_bytes;
        }
        const costing = try ca.alloc(Costing, selected.items.len);
        const batches = try ca.alloc(Batch, selected.items.len);
        const seen = try ca.alloc(bool, provider.source.inventory.files.len);
        @memset(seen, false);
        var initialized: usize = 0;
        errdefer for (batches[0..initialized]) |*batch| batch.arena.deinit();
        const reuse = if (previous) |prior| std.mem.eql(u8, prior.source, root.source) and std.mem.eql(u8, prior.schema, root.schema) and std.mem.eql(u8, &prior.domain, &root.domain) else false;
        for (selected.items, costing, batches) |name, *column, *batch| {
            column.* = .{ .name = name, .files = null, .statistics = .{ .name = name } };
            batch.* = .{ .arena = .init(a) };
            initialized += 1;
            if (reuse) for (previous.?.costing) |prior| if (std.mem.eql(u8, name, prior.name)) {
                column.files = prior.files;
                break;
            };
        }
        return .{ .a = a, .out = out, .arena = arena, .store = store, .provider = provider, .root = root, .token = token, .current = current, .columns = selected.items, .costing = costing, .batches = batches, .seen = seen, .same_files = reuse and previous.?.file_summary != null and std.meta.eql(previous.?.file_summary, root.file_summary) };
    }
    /// Called after moving this owner to its stable address.
    pub fn start(self: *@This()) !void {
        var pages: pages_mod.PageStore = undefined;
        const store = self.storage(&pages);
        const delta = self.provider.native_delta.?;
        for (self.costing, self.batches) |*column, *batch| if (column.files != null and !self.same_files) {
            for (delta.prior_changed) |old| {
                try batch.add(&key(old.id), null);
                if (batch.changes.items.len >= 64) try batch.flush(store, &column.files);
            }
            try batch.flush(store, &column.files);
        };
        self.provider.statistics = .{ .ptr = self, .columns = self.columns, .files = &self.provider.native_delta.?.by_id, .needed = needed, .save = save };
    }
    pub fn deinit(self: *@This()) void {
        self.provider.statistics = null;
        for (self.batches) |*batch| batch.arena.deinit();
        self.arena.deinit();
    }
    fn needed(raw: *anyopaque, file: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.provider.context.io) |io| self.mutex.lockUncancelable(io);
        defer if (self.provider.context.io) |io| self.mutex.unlock(io);
        return self.columns.len != 0 and file < self.seen.len and !self.seen[file];
    }
    fn save(raw: *anyopaque, file: usize, sketches: []const Sketch) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.provider.context.io) |io| try self.mutex.lock(io);
        defer if (self.provider.context.io) |io| self.mutex.unlock(io);
        if (file >= self.seen.len or sketches.len != self.costing.len) return error.InvalidNativeLakeFileState;
        if (self.seen[file]) return;
        try self.provider.context.ensureActive();
        try self.token.check();
        var pages: pages_mod.PageStore = undefined;
        const store = self.storage(&pages);
        const id = key(self.provider.source.inventory.files[file].file_id);
        for (sketches, self.costing, self.batches) |sketch, *column, *batch| {
            try batch.add(&id, &sketch.encode());
            if (batch.changes.items.len >= 64) try batch.flush(store, &column.files);
        }
        self.seen[file] = true;
    }
    fn saveCheckpoint(self: *@This()) !Ref {
        var pages: pages_mod.PageStore = undefined;
        const store = self.storage(&pages);
        for (self.costing, self.batches) |*column, *batch| try batch.flush(store, &column.files);
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        const a = scratch.allocator();
        const columns = try a.alloc(Costing, self.costing.len);
        for (self.costing, columns) |column, *saved| saved.* = .{ .name = column.name, .files = column.files, .statistics = .{ .name = column.name }, .complete = false };
        var root = self.root;
        root.version = @max(root.version, 3);
        root.costing = columns;
        const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
        if (bytes.len > 64 * 1024) return self.current;
        var uploaded = try self.store.putWithCancellation(bytes, self.token);
        defer uploaded.deinit(self.store.allocator);
        return .{ .kind = .doc_values, .metadata_version = root.version, .artifact_id = try self.out.dupe(u8, uploaded.artifact_id), .checksum = try self.out.dupe(u8, uploaded.checksum), .byte_len = uploaded.byte_len };
    }
    pub fn finish(self: *@This()) !Ref {
        if (self.columns.len == 0) return self.current;
        var pages: pages_mod.PageStore = undefined;
        const store = self.storage(&pages);
        for (self.costing, self.batches) |*column, *batch| try batch.flush(store, &column.files);
        // A legacy publication has no sketches. Backfill missing leaves once;
        // ordinary refreshes reuse complete unchanged trees without source I/O.
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        const needs_backfill = for (self.costing) |column| {
            if ((if (column.files) |ref| ref.records else 0) != self.root.count) break true;
        } else false;
        var backfilled: usize = 0;
        if (needs_backfill) for (self.provider.source.inventory.files, 0..) |file, index| {
            try self.provider.context.ensureActive();
            try self.token.check();
            if (self.seen[index]) continue;
            var missing = false;
            for (self.costing) |column| {
                _ = scratch.reset(.free_all);
                const id = key(file.file_id);
                var cursor = try tree.Cursor.init(scratch.allocator(), store, column.files, &id, null);
                defer cursor.deinit();
                const found = try cursor.next();
                if (found == null or !std.mem.eql(u8, found.?.key, &id)) {
                    missing = true;
                    break;
                }
            }
            if (!missing) continue;
            _ = scratch.reset(.free_all);
            var input = self.provider.*;
            input.only_files = null;
            input.only_file = index;
            input.progress = null;
            const binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .text, .source_kind = if (self.provider.source.inventory.format == .iceberg) .external_iceberg else .external_parquet, .row_ref_kind = .external, .source_id = self.root.source, .snapshot_id = self.root.snapshot, .schema_fingerprint = self.root.schema, .column_bindings = self.columns, .index_config_hash = "native-planner-statistics-v1" };
            const provider = input.provider();
            const source = try provider.openWithCancellation(scratch.allocator(), binding, self.token);
            defer source.deinit(scratch.allocator());
            while (try source.next(scratch.allocator())) |_| {}
            backfilled += 1;
            if (self.checkpoint) |checkpoint| if (backfilled % 64 == 0) try checkpoint.save(checkpoint.ptr, try self.saveCheckpoint());
        };
        var complete: std.ArrayList(Costing) = .empty;
        const ca = self.arena.allocator();
        for (self.costing, self.batches) |*column, *batch| {
            try batch.flush(store, &column.files);
            if ((if (column.files) |ref| ref.records else 0) != self.root.count) continue;
            var sketch: Sketch = .{};
            if (column.files) |ref| {
                var page = try tree.readPage(ca, store, ref);
                defer page.deinit(ca);
                sketch = try Sketch.decode(page.aggregate);
            }
            column.statistics = try sketch.estimate(ca, column.name);
            try complete.append(ca, column.*);
        }
        if (complete.items.len == 0) return self.current;
        self.root.version = @max(self.root.version, 3);
        self.root.costing = complete.items;
        const bytes = try std.json.Stringify.valueAlloc(ca, self.root, .{});
        if (bytes.len > 64 * 1024) return self.current;
        var uploaded = try self.store.putWithCancellation(bytes, self.token);
        defer uploaded.deinit(self.store.allocator);
        return .{ .kind = .doc_values, .metadata_version = self.root.version, .artifact_id = try self.out.dupe(u8, uploaded.artifact_id), .checksum = try self.out.dupe(u8, uploaded.checksum), .byte_len = uploaded.byte_len };
    }
};
/// The resolver borrows fresh publication authority. Only immutable root/page
/// payloads enter shared caches; request contexts never escape this owner.
pub const Resolver = struct {
    root: Root,
    store: stores.ArtifactStore,
    cached: artifacts.CachedRead,
    token: Token,
    reads: u64 = 64 * 1024 * 1024,
    writes: u64 = 0,
    pages: pages_mod.PageStore = undefined,
    page_cache: ?tree.Cache = null,
    page_cache_limit: usize = 1024 * 1024,
    memo_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 },
    memo_arena: ?std.heap.ArenaAllocator = null,
    public_memo: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_memo: std.StringHashMapUnmanaged([]const u8) = .empty,
    digest_memo: std.StringHashMapUnmanaged([]const u8) = .empty,
    memo_disabled: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *@This()) !void {
        while (!self.mutex.tryLock()) {
            try self.token.check();
            try self.cached.context.ensureActive();
            if (self.cached.context.io) |io| try io.sleep(std.Io.Duration.zero, .awake) else @import("antfly_platform").time.yieldBriefly();
        }
    }
    pub fn deinit(self: *@This()) void {
        if (self.page_cache) |*cache| cache.deinit();
        if (self.memo_arena) |*arena| arena.deinit();
    }
    fn remember(self: *@This(), map: *std.StringHashMapUnmanaged([]const u8), id: []const u8, value: []const u8) ?[]const u8 {
        if (self.memo_disabled or self.public_memo.count() + self.private_memo.count() + self.digest_memo.count() >= 512) return null;
        if (self.memo_arena == null) self.memo_arena = .init(self.memo_budget.allocator());
        const a = self.memo_arena.?.allocator();
        const owned_id = a.dupe(u8, id) catch {
            self.memo_disabled = true;
            return null;
        };
        const owned = a.dupe(u8, value) catch {
            self.memo_disabled = true;
            return null;
        };
        map.put(a, owned_id, owned) catch {
            self.memo_disabled = true;
            return null;
        };
        return owned;
    }
    fn storage(self: *@This()) tree.Store {
        self.pages = .{ .domain = self.root.domain, .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .read_cache = .{ .ptr = self, .read = readPage } };
        if (self.page_cache == null) self.page_cache = .{ .alloc = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .underlying = self.pages.store(), .max_bytes = self.page_cache_limit };
        return self.page_cache.?.store();
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: Ref, offset: u64, length: usize, _: [32]u8, token: Token, budget: *u64) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.cached.context.ensureActive();
        try stores.chargeReadBudget(budget, length);
        if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeFileState;
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
    fn point(self: *@This(), a: A, root: ?tree.Ref, id: []const u8) !?[]u8 {
        var ref = root orelse return null;
        var lower: ?[32]u8 = null;
        var upper: ?[32]u8 = null;
        const Loader = struct {
            owner: *Resolver,
            ref: tree.Ref,
            fn decode(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                const owned = item.arena.allocator();
                const page = try owned.create(tree.Page);
                page.* = try tree.readPage(owned, self_.owner.storage(), self_.ref);
                for (page.entries) |entry| if (entry.key.len != 32) return error.InvalidNativeLakeFileState;
                item.payload = .{ .extension = page };
            }
        };
        const Check = struct {
            owner: *Resolver,
            fn check(raw: *anyopaque) !void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                try self_.owner.token.check();
                try self_.owner.cached.context.ensureActive();
            }
        };
        var check: Check = .{ .owner = self };
        var context = self.cached.context;
        context.checkpoint = .{ .ptr = &check, .check = Check.check };
        while (true) {
            try context.ensureActive();
            const cache_key = @import("lake_wal_image.zig").digest(.{ .format = "decoded-file-navigation-page-v1", .scope = self.cached.scope, .domain = self.root.domain, .ref = ref });
            var loader: Loader = .{ .owner = self, .ref = ref };
            const lease = self.cached.cache.decoded.acquire(cache_key, @min(64 * 1024 * 1024, @as(usize, ref.bytes) * 16 + 65536), context, .{ .ptr = &loader, .load = Loader.decode }) catch |err| switch (err) {
                // Caching is optional under memory pressure. The ordinary
                // authenticated cursor retains the same complete point lookup.
                error.OutOfMemory, error.SqlMemoryLimitExceeded => return lookup(a, self.storage(), root, id),
                else => return err,
            };
            defer lease.release();
            try context.ensureActive();
            const page: *const tree.Page = @ptrCast(@alignCast(lease.item.payload.extension));
            if (lower) |first| if (!std.mem.eql(u8, page.entries[0].key, &first)) return error.InvalidNativeLakeFileState;
            if (upper) |last| if (std.mem.order(u8, page.entries[page.entries.len - 1].key, &last) != .lt) return error.InvalidNativeLakeFileState;
            var lo: usize = 0;
            var hi = page.entries.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (std.mem.order(u8, page.entries[mid].key, id) != .gt) lo = mid + 1 else hi = mid;
            }
            if (lo == 0) return null;
            const entry = page.entries[lo - 1];
            if (ref.height == 0) return if (std.mem.eql(u8, entry.key, id)) try a.dupe(u8, entry.value) else null;
            ref = entry.child orelse return error.InvalidNativeLakeFileState;
            lower = entry.key[0..32].*;
            if (lo < page.entries.len) upper = page.entries[lo].key[0..32].*;
        }
    }
    pub fn file(self: *@This(), a: A, hex: []const u8, public: bool) !?[]const u8 {
        try self.lock();
        defer self.mutex.unlock();
        try self.cached.context.ensureActive();
        try self.token.check();
        const map = if (public) &self.public_memo else &self.private_memo;
        if (map.get(hex)) |found| return found;
        var digest_hex = hex;
        if (public and self.root.public_identity_version == 2) {
            if (hex.len != 129 or hex[64] != ':' or !std.mem.eql(u8, hex[0..64], &std.fmt.bytesToHex(local.storage_rowsource_identity.fileDigest(self.root.source, self.root.snapshot, ""), .lower))) return error.ExternalLakeSnapshotMismatch;
            digest_hex = hex[65..];
        }
        if (digest_hex.len != 64) return error.ExternalLakeSnapshotMismatch;
        var id: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, digest_hex) catch return error.ExternalLakeSnapshotMismatch;
        var scratch_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(scratch_budget.allocator());
        defer scratch.deinit();
        const found = (try self.point(scratch.allocator(), if (public) self.root.public else self.root.private, &id)) orelse return null;
        return self.remember(map, hex, found) orelse try a.dupe(u8, found);
    }
    /// A selected file entry owns every field independently of decoded-page
    /// residency. This permits sparse source opening after cold process restart.
    pub fn inventoryFile(self: *@This(), a: A, id: []const u8) !local.serverless_external_source_types.FileEntry {
        return self.inventoryFileBounded(a, id, false);
    }
    pub fn inventoryFileForScan(self: *@This(), a: A, id: []const u8) !local.serverless_external_source_types.FileEntry {
        return self.inventoryFileBounded(a, id, true);
    }
    fn inventoryFileBounded(self: *@This(), a: A, id: []const u8, scan: bool) !local.serverless_external_source_types.FileEntry {
        try self.lock();
        defer self.mutex.unlock();
        const previous_reads = self.reads;
        defer if (scan) {
            self.reads = previous_reads;
        };
        if (scan) self.reads = 8 * 1024 * 1024;
        try self.cached.context.ensureActive();
        try self.token.check();
        var memory: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(memory.allocator());
        defer scratch.deinit();
        const bytes = (try self.point(scratch.allocator(), self.root.files, &key(id))) orelse return error.ExternalLakeSnapshotMismatch;
        const record = try std.json.parseFromSliceLeaky(Record, scratch.allocator(), bytes, .{});
        if (!std.mem.eql(u8, record.file.file_id, id)) return error.ExternalLakeSnapshotMismatch;
        try record.file.validate();
        // The binary codec transfers ordinary FileEntry ownership and cleans up
        // partially decoded fields on allocation failure. Leaky JSON decoding
        // into the request allocator would strand allocations under pressure.
        const codec = local.serverless_external_source_codec;
        const encoded = try codec.encodeAlloc(scratch.allocator(), .{
            .format = .iceberg,
            .source_id = @constCast(self.root.source),
            .source_uri = @constCast("published-navigation"),
            .snapshot_id = @constCast(self.root.snapshot),
            .schema_fingerprint = @constCast(self.root.schema),
            .files = @constCast(&[_]local.serverless_external_source_types.FileEntry{record.file}),
        });
        var inventory = try codec.decodeAlloc(a, encoded);
        defer inventory.deinit(a);
        const file_ = inventory.files[0];
        a.free(inventory.files);
        inventory.files = &.{};
        return file_;
    }
    /// Return only the exact selectors needed by a reached Parquet file.
    /// The caller owns the strings; all other source metadata remains in its
    /// fresh immutable manifest plan.
    pub fn deleteSelector(self: *@This(), a: A, index: usize, file_: local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile, context: local.serverless_query_lake_read_context.Context) !local.serverless_query_lake_serving.DeleteQualification.Version {
        try context.ensureActive();
        try self.lock();
        defer self.mutex.unlock();
        const proof = self.root.delete_proof orelse return error.InvalidNativeLakeFileState;
        if (index >= proof.count) return error.InvalidNativeLakeFileState;
        const previous_reads = self.reads;
        defer self.reads = previous_reads;
        self.reads = 8 * 1024 * 1024;
        var memory: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(memory.allocator());
        defer scratch.deinit();
        var key_: [32]u8 = @splat(0);
        std.mem.writeInt(u32, key_[28..32], @intCast(index), .big);
        const bytes = (try self.point(scratch.allocator(), proof.versions, &key_)) orelse return error.InvalidNativeLakeFileState;
        try context.ensureActive();
        return ownDeleteVersion(a, bytes, file_);
    }
    pub fn selectors(self: *@This(), a: A, file_: local.serverless_external_source_types.FileEntry) !local.serverless_query_lake_serving.ServingSource.FileVersions {
        try self.lock();
        defer self.mutex.unlock();
        try self.cached.context.ensureActive();
        try self.token.check();
        var memory: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(memory.allocator());
        defer scratch.deinit();
        const sa = scratch.allocator();
        const bytes = (try self.point(sa, self.root.files, &key(file_.file_id))) orelse return error.ExternalLakeSnapshotMismatch;
        const record = try std.json.parseFromSliceLeaky(Record, sa, bytes, .{});
        if (!std.mem.eql(u8, record.file.file_id, file_.file_id) or !std.mem.eql(u8, record.file.object_uri, file_.object_uri) or record.file.byte_len != file_.byte_len) return error.ExternalLakeSnapshotMismatch;
        const coverage = @import("lake_index_coverage.zig");
        if (!coverage.versionIsStrong(record.file.etag, record.file.version_id)) return error.InvalidExternalLakeIndexCoverage;
        if (coverage.versionIsStrong(file_.etag, file_.version_id) and (!std.mem.eql(u8, file_.etag, record.file.etag) or !std.mem.eql(u8, file_.version_id, record.file.version_id))) return error.ExternalLakeIndexSourceChanged;
        const etag = try a.dupe(u8, record.file.etag);
        errdefer a.free(etag);
        return .{ .etag = etag, .version_id = try a.dupe(u8, record.file.version_id) };
    }
    pub fn digest(self: *@This(), a: A, file_id: []const u8) ![]const u8 {
        try self.lock();
        defer self.mutex.unlock();
        try self.cached.context.ensureActive();
        try self.token.check();
        if (self.digest_memo.get(file_id)) |found| return found;
        var scratch_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(scratch_budget.allocator());
        defer scratch.deinit();
        const sa = scratch.allocator();
        const bytes = (try self.point(sa, self.root.files, &key(file_id))) orelse return error.ExternalLakeSnapshotMismatch;
        const record = try std.json.parseFromSliceLeaky(Record, sa, bytes, .{});
        if (!std.mem.eql(u8, record.file.file_id, file_id)) return error.ExternalLakeSnapshotMismatch;
        const hex = std.fmt.bytesToHex(record.digest, .lower);
        return self.remember(&self.digest_memo, file_id, &hex) orelse try a.dupe(u8, &hex);
    }
};
pub fn walk(a: A, store: *stores.ArtifactStore, ref: Ref, token: Token, visitor: anytype) !void {
    const root = try load(a, store.*, ref, token, null);
    var reads: u64 = tree.max_page_bytes;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = root.domain, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    for ([_]?tree.Ref{ root.files, root.public, root.private }) |r| if (r) |value| try tree.walkPostOrder(a, pages.store(), value, visitor, false);
}

test "external lake bounded WAL paged source navigation resolves only reached identities and reconciles per-file deletes" {
    try navigationRefreshScenario(1);
    try navigationRefreshScenario(2);
}
fn navigationRefreshScenario(identity_version: u8) !void {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("paged-source-navigation");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const entries = try ca.alloc(local.serverless_external_source_types.FileEntry, 256);
    for (entries, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .object_uri = try std.fmt.allocPrint(ca, "file:///source-{d}.parquet", .{i}), .etag = @constCast("exact"), .byte_len = 128, .row_count = 0, .row_groups = &.{} };
    var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .inventory = undefined, .scanner = undefined };
    source.inventory = .{ .format = .iceberg, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("before"), .row_identity_version = identity_version, .schema_fingerprint = @constCast("schema"), .files = entries };
    source.verified_files = null;
    source.prepared_deletes = null;
    source.scanner.iceberg_delete_plan = null;
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    source.immutable_objects = true;
    source.plan_identity = @splat(8);
    const first = try buildProven(a, ca, &store, &provider, null, .none, @splat(9));
    defer first.deinit();
    const old = try load(ca, store, first.ref, .none, null);
    const descriptor = try encodeInventory(a, &provider, old);
    defer a.free(descriptor);
    try std.testing.expect(descriptor.len < 8192);
    var decoded_inventory = try decodeInventory(a, store, descriptor, .none);
    defer decoded_inventory.deinit(a);
    try std.testing.expectEqual(entries.len, decoded_inventory.files.len);
    try std.testing.expectEqualStrings("source", decoded_inventory.source_id);
    for (decoded_inventory.files) |file| {
        const original_file = source.inventory.fileById(file.file_id) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(original_file.object_uri, file.object_uri);
        try std.testing.expectEqualStrings(original_file.etag, file.etag);
    }
    try std.testing.expectEqualDeep(@as([32]u8, @splat(8)), old.coverage.?.plan.?);
    try std.testing.expectEqualDeep(@as([32]u8, @splat(9)), old.coverage.?.data);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(3), .context = .{} };
    var resolver: Resolver = .{ .root = old, .store = store, .cached = cached, .token = .none };
    defer resolver.deinit();
    {
        var cold = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cold.deinit();
        var walking: Resolver = .{ .root = old, .store = store, .cached = .{ .cache = &cold, .scope = @splat(5), .context = .{} }, .token = .none, .reads = 0 };
        defer walking.deinit();
        var entry = try walking.inventoryFileForScan(a, "file-255");
        defer entry.deinit(a);
        try std.testing.expectEqualStrings("file-255", entry.file_id);
        try std.testing.expectEqual(@as(u64, 0), walking.reads);
    }
    const Ownership = struct {
        fn check(alloc: A, reader: *Resolver) !void {
            var file_ = try reader.inventoryFile(alloc, "file-255");
            defer file_.deinit(alloc);
            try std.testing.expectEqualStrings("file-255", file_.file_id);
            // Every field is owned independently of the immutable decoded page.
            file_.file_id[0] = 'X';
            try std.testing.expectEqualStrings("exact", file_.etag);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Ownership.check, .{&resolver});
    var owned_file = try resolver.inventoryFile(a, "file-255");
    defer owned_file.deinit(a);
    try std.testing.expectEqualStrings("file-255", owned_file.file_id);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, resolver.inventoryFile(a, "absent"));
    const versions = try resolver.selectors(a, entries[255]);
    defer a.free(versions.etag);
    defer a.free(versions.version_id);
    try std.testing.expectEqualStrings("exact", versions.etag);
    var wrong_file = entries[255];
    wrong_file.byte_len += 1;
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, resolver.selectors(a, wrong_file));
    wrong_file = entries[255];
    wrong_file.etag = @constCast("changed");
    try std.testing.expectError(error.ExternalLakeIndexSourceChanged, resolver.selectors(a, wrong_file));
    const public_hex = try local.storage_rowsource_identity.publicFileAlloc(ca, identity_version, "source", "before", "file-255");
    try std.testing.expectEqualStrings("file-255", (try resolver.file(ca, public_hex, true)).?);
    const private_hex = try resolver.digest(ca, "file-255");
    try std.testing.expectEqualStrings("file-255", (try resolver.file(ca, private_hex, false)).?);
    // One query resolves one file, not the inventory. Repeated hits perform no
    // more metadata I/O; cache ownership stays independent of result arenas.
    const read_bytes = resolver.reads;
    try std.testing.expectEqualStrings("file-255", (try resolver.file(ca, public_hex, true)).?);
    try std.testing.expectEqual(read_bytes, resolver.reads);
    try std.testing.expectEqual(@as(u32, 1), resolver.public_memo.count());
    var peer: Resolver = .{ .root = old, .store = store, .cached = cached, .token = .none };
    defer peer.deinit();
    const allowance = peer.reads;
    try std.testing.expectEqualStrings("file-255", (try peer.file(ca, public_hex, true)).?);
    try std.testing.expectEqual(allowance, peer.reads);
    try std.testing.expect(resolver.memo_budget.peak < 64 * 1024);
    var prepared: local.serverless_query_lake_prepared_deletes.Prepared = .{ .arena = .init(a), .delete_files = &.{}, .equality = &.{}, .columns = &.{} };
    defer prepared.arena.deinit();
    for (entries, 0..) |file, i| try prepared.files.put(prepared.arena.allocator(), file.file_id, .{ .index = i, .fingerprint = if (i == 0) @splat(9) else local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint() });
    source.prepared_deletes = &prepared;
    source.inventory.snapshot_id = @constCast("after");
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 2, std.testing.io);
    const next = try build(a, ca, &store, &provider, old, .none);
    defer next.deinit();
    try std.testing.expectEqual(@as(usize, 1), next.delta.prior_changed.len);
    try std.testing.expect(next.stats.prior_records < entries.len / 2);
    try std.testing.expect(next.stats.skipped_subtrees > 0);
    for (0..next.delta.count()) |i| try std.testing.expectEqual(i == 0, next.delta.isChanged(i));
    const root = try load(ca, store, next.ref, .none, null);
    var newer: Resolver = .{ .root = root, .store = store, .cached = cached, .token = .none };
    defer newer.deinit();
    const old_digest = try resolver.digest(ca, "file-0");
    const new_digest = try newer.digest(ca, "file-0");
    try std.testing.expect(!std.mem.eql(u8, old_digest, new_digest));
    try std.testing.expect(try newer.file(ca, old_digest, false) == null);
    try std.testing.expectEqualStrings("file-0", (try resolver.file(ca, old_digest, false)).?);
    if (identity_version == 2) {
        try std.testing.expectEqual(@as(u16, 4), next.ref.metadata_version);
        try std.testing.expectEqualDeep(old.public, root.public);
        try std.testing.expectError(error.ExternalLakeSnapshotMismatch, newer.file(ca, public_hex, true));
        const fresh_hex = try local.storage_rowsource_identity.publicFileAlloc(ca, 2, "source", "after", "file-255");
        try std.testing.expectEqualStrings("file-255", (try newer.file(ca, fresh_hex, true)).?);
    } else try std.testing.expect(try newer.file(ca, public_hex, true) == null);
    provider.native_delta = next.delta;
    const exact = try native.identity(ca, &provider, entries[0]);
    try std.testing.expectEqualStrings(new_digest, &std.fmt.bytesToHex(exact, .lower));
    // Unchanged delete-bearing refreshes read no prior file records/pages.
    provider.native_delta = null;
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 3, std.testing.io);
    const unchanged = try build(a, ca, &store, &provider, root, .none);
    defer unchanged.deinit();
    try std.testing.expectEqual(@as(usize, 0), unchanged.stats.prior_pages);
    try std.testing.expectEqual(@as(usize, 0), unchanged.stats.prior_records);
    try std.testing.expectEqual(@as(usize, 0), unchanged.delta.prior_changed.len);
    if (identity_version == 2) {
        // A membership delta edits only the affected public path; retained roots
        // and changed-file replacements keep their original authority.
        const same = try load(ca, store, unchanged.ref, .none, null);
        source.inventory.files = entries[0 .. entries.len - 1];
        const removal = try build(a, ca, &store, &provider, same, .none);
        defer removal.deinit();
        const removed = try load(ca, store, removal.ref, .none, null);
        try std.testing.expectEqual(@as(u64, 255), removed.public.?.records);
        var removal_reader: Resolver = .{ .root = removed, .store = store, .cached = cached, .token = .none };
        defer removal_reader.deinit();
        const removed_hex = try local.storage_rowsource_identity.publicFileAlloc(ca, 2, "source", "after", "file-255");
        try std.testing.expect(try removal_reader.file(ca, removed_hex, true) == null);
        source.inventory.files = entries;
        const addition = try build(a, ca, &store, &provider, removed, .none);
        defer addition.deinit();
        const added = try load(ca, store, addition.ref, .none, null);
        try std.testing.expectEqual(@as(u64, 256), added.public.?.records);
        var addition_reader: Resolver = .{ .root = added, .store = store, .cached = cached, .token = .none };
        defer addition_reader.deinit();
        try std.testing.expectEqualStrings("file-255", (try addition_reader.file(ca, removed_hex, true)).?);
    }
    // Wrong authority cannot turn a cached page into a capability.
    newer.cached.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, newer.file(ca, new_digest, false));
    try std.testing.expectError(error.DeadlineExceeded, newer.inventoryFile(a, "file-0"));
}

test "external lake metadata navigation durable delete selectors validate only reached versions and remain GC roots" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var provider_storage = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider.init(a);
    defer provider_storage.deinit();
    var client = provider_storage.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "delete", "bytes", .{});
    put.deinit(a);
    const iceberg = local.serverless_query_lake_iceberg_snapshot;
    var deletes = [_]iceberg.IcebergDeleteFile{.{ .content = .position_deletes, .file_path = @constCast("s3://bucket/delete"), .file_format = @constCast("PARQUET"), .snapshot_id = 1, .data_sequence_number = 2, .file_sequence_number = 3, .record_count = 1, .file_size_in_bytes = 5 }};
    const inventory: Inventory = .{ .format = .iceberg, .source_id = @constCast("source"), .source_uri = @constCast("s3://bucket/table"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = &.{} };
    const Pin = struct {
        fn check(_: *anyopaque) !void {}
        fn close(_: *anyopaque) void {}
    };
    var marker: u8 = 0;
    var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .inventory = inventory, .scanner = local.serverless_query_lake_serving.PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client), .immutable_objects = true, .snapshot_pin = .{ .ptr = &marker, .check = Pin.check, .deinit = Pin.close } };
    source.scanner.iceberg_delete_plan = .{ .files = &deletes };
    var provider: Provider = .{ .source = &source, .context = .{} };
    var directory = try local.common_test_directory.TestDirectory.init("delete-version-proof");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@import("lake_index_publication.zig").uploadDomainWithNamespace(1, @splat(7), null), 1, std.testing.io);
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: pages_mod.PageStore = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const proof = (try publishDeleteProof(a, pages.store(), &provider, @splat(9))).?;
    const root: Root = .{ .domain = store.upload_scope.?.domain, .source = "source", .schema = "schema", .snapshot = "snapshot", .count = 0, .coverage = .{ .plan = @splat(8), .data = @splat(9) }, .delete_proof = proof };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    var resolver: Resolver = .{ .root = root, .store = store, .cached = .{ .cache = &cache, .scope = @splat(3), .context = .{} }, .token = .none };
    defer resolver.deinit();
    for (0..2) |_| {
        const selector = try resolver.deleteSelector(a, 0, deletes[0], .{});
        a.free(selector.etag);
        a.free(selector.version);
    }
    try std.testing.expect(cache.decoded.hits > 0);
    const before = provider_storage.stats;
    const version = try deleteVersion(a, store, root, 0, deletes[0], .none, .{});
    defer {
        a.free(version.etag);
        a.free(version.version);
    }
    try std.testing.expect(exactVersion(version.version));
    try std.testing.expectEqual(before, provider_storage.stats);
    var changed = deletes[0];
    changed.file_size_in_bytes += 1;
    try std.testing.expectError(error.InvalidNativeLakeFileState, deleteVersion(a, store, root, 0, changed, .none, .{}));
    try std.testing.expectError(error.DeadlineExceeded, deleteVersion(a, store, root, 0, deletes[0], .none, .{ .deadline_ns = 0 }));
    const root_bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
    var root_ref = try store.putWithCancellation(root_bytes, .none);
    defer root_ref.deinit(a);
    var inventory_ref = try store.putWithCancellation("inventory", .none);
    defer inventory_ref.deinit(a);
    var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 1, .authority = undefined, .store = store, .identity = @splat(7), .context = .{}, .token = @splat(1), .audit_only = true, .remaining_reads = 16 * 1024 * 1024 };
    defer {
        var keys = collector.marked.keyIterator();
        while (keys.next()) |key_| a.free(key_.*);
        collector.marked.deinit(a);
    }
    const publication: local.metadata_lake_index_catalog.Publication = .{ .reader_protocol = local.metadata_lake_index_catalog.native_reader_protocol, .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = proof.source, .credentials = @splat(2), .store = @splat(7) }, .published_at_ms = 1, .base_source = .{ .external_iceberg = .{ .format = .iceberg, .source_uri = "s3://bucket/metadata.json", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory_ref.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = inventory_ref.artifact_id, .checksum = inventory_ref.checksum, .byte_len = inventory_ref.byte_len }, .source_navigation = .{ .kind = .doc_values, .metadata_version = 2, .artifact_id = root_ref.artifact_id, .checksum = root_ref.checksum, .byte_len = root_ref.byte_len } };
    try collector.markPublication(publication);
    try std.testing.expect(collector.marked.count() >= 3);
    source.immutable_objects = false;
    try std.testing.expect((try publishDeleteProof(a, pages.store(), &provider, @splat(9))) == null);
}

test "external lake durable costing reuses unchanged leaves and removes replaced file sketches without source reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-costing");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const files = try ca.alloc(local.serverless_external_source_types.FileEntry, 3);
    for (files, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .object_uri = try std.fmt.allocPrint(ca, "file:///source-{d}.parquet", .{i}), .etag = @constCast("exact"), .byte_len = 128, .row_count = 100, .row_groups = &.{} };
    var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .inventory = undefined, .scanner = undefined };
    source.inventory = .{ .format = .iceberg, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("before"), .schema_fingerprint = @constCast("schema"), .files = files };
    source.verified_files = null;
    source.prepared_deletes = null;
    source.scanner.iceberg_delete_plan = null;
    source.iceberg_schema = null;
    source.immutable_objects = true;
    source.plan_identity = @splat(8);
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .schema_contract = &.{.{ .name = "key", .kind = "integer", .required = true }} };
    const initial = try buildProven(a, ca, &store, &provider, null, .none, @splat(9));
    defer initial.deinit();
    provider.native_delta = initial.delta;
    var builder = try CostingBuilder.init(a, ca, &store, &provider, initial.ref, null, &.{"key"}, .none);
    defer builder.deinit();
    try builder.start();
    var initial_sketch: local.sql_catalog.StatisticsSketch = .{};
    for (0..100) |_| try initial_sketch.add(local.sql_scalar.Datum.fromJson(.{ .integer = 0 }), 8);
    try CostingBuilder.save(&builder, 0, &.{initial_sketch});
    const partial = try builder.saveCheckpoint();
    const checkpoint = try load(ca, store, partial, .none, null);
    try std.testing.expect(!checkpoint.costing[0].complete);
    try std.testing.expectEqual(@as(u64, 1), checkpoint.costing[0].files.?.records);
    var resumed = try CostingBuilder.init(a, ca, &store, &provider, initial.ref, checkpoint, &.{"key"}, .none);
    defer resumed.deinit();
    try resumed.start();
    for (1..3) |file| {
        var sketch: local.sql_catalog.StatisticsSketch = .{};
        for (0..100) |_| try sketch.add(local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(file) }), 8);
        try CostingBuilder.save(&resumed, file, &.{sketch});
    }
    const first = try resumed.finish();
    const before = try load(ca, store, first, .none, null);
    try std.testing.expectEqual(@as(u16, 3), first.metadata_version);
    try std.testing.expectEqual(@as(usize, 1), before.costing.len);
    try std.testing.expectEqual(@as(?u64, 300), before.costing[0].statistics.population_rows);
    try std.testing.expectEqual(@as(u64, 3), before.costing[0].files.?.records);
    source.inventory.files = files[0..2];
    source.inventory.snapshot_id = @constCast("after");
    files[0].etag = @constCast("replacement");
    provider.native_delta = null;
    const refreshed = try buildProven(a, ca, &store, &provider, before, .none, @splat(10));
    defer refreshed.deinit();
    provider.native_delta = refreshed.delta;
    try std.testing.expect(refreshed.delta.isChanged(0));
    try std.testing.expect(!refreshed.delta.isChanged(1));
    var next = try CostingBuilder.init(a, ca, &store, &provider, refreshed.ref, before, &.{"key"}, .none);
    defer next.deinit();
    try next.start();
    var replacement: local.sql_catalog.StatisticsSketch = .{};
    try replacement.add(local.sql_scalar.Datum.fromJson(.{ .integer = 99 }), 8);
    try CostingBuilder.save(&next, 0, &.{replacement});
    const second = try next.finish();
    const after = try load(ca, store, second, .none, null);
    try std.testing.expectEqual(@as(?u64, 101), after.costing[0].statistics.population_rows);
    try std.testing.expectEqual(@as(?i64, 1), after.costing[0].statistics.min_integer);
    try std.testing.expectEqual(@as(?i64, 99), after.costing[0].statistics.max_integer);
    try std.testing.expectEqual(@as(u64, 2), after.costing[0].files.?.records);
    const retained = for (dependencies(after)) |ref| {
        if (ref) |value| if (std.meta.eql(value, after.costing[0].files.?)) break true;
    } else false;
    try std.testing.expect(retained);
}

test "external lake bounded WAL sparse native file directories retain delta state and GC document edges" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-paged-file-state");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(1, @splat(7), null);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    const entries = try ca.alloc(local.serverless_external_source_types.FileEntry, 256);
    var positions: local.serverless_query_lake_inventory_catalog.Positions = .empty;
    defer positions.deinit(a);
    for (entries, 0..) |*entry, i| {
        entry.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .object_uri = @constCast("file:///source.parquet"), .byte_len = 128, .row_count = 1, .row_groups = &.{} };
        try positions.put(a, entry.file_id, i);
    }
    const Hasher = struct {
        fn get(_: *anyopaque, id: []const u8) ![32]u8 {
            return native.Selection.idKey(id);
        }
    };
    var dummy: u8 = 0;
    const census = try local.serverless_query_lake_file_census.Census.build(a, domain, null, &.{}, &.{}, entries, .{ .ptr = &dummy, .get = Hasher.get }, .{});
    defer census.release();
    const selection: native.Selection = .{ .files = entries, .census = census, .positions = positions };
    var initial: native.Plan = .{ .a = a, .selection = selection, .by_id = positions, .borrowed_positions = true, .force_all = true };
    defer initial.deinit();
    const old_digest = (try initial.stateAt(0)).digest;
    const document = try std.fmt.allocPrint(ca, "lake2:{s}:00000000:0000000000000000", .{std.fmt.bytesToHex(&old_digest, .lower)});
    var uploaded = try store.putWithCancellation(document, .none);
    defer uploaded.deinit(a);
    const docs = try ca.dupe(artifacts.ChunkRef, &.{.{ .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len }});
    const first = try initial.mutable(0);
    first.docs = docs;
    first.count = 1;
    const root = (try initial.publishDirectory(a, &store, domain, .none)).?;
    try std.testing.expectEqual(@as(u64, entries.len), root.records);
    try std.testing.expectEqual(@as(usize, 0), initial.files.len);
    try std.testing.expectEqual(@as(usize, 1), initial.updates.count());
    var delta_selection = selection;
    var ordered = census.iterator();
    var range: ?native.Selection.Range = null;
    while (try ordered.next()) |entry| {
        if (std.mem.eql(u8, entry.id, entries[0].file_id)) {
            if (range) |value| try delta_selection.unchanged.append(ca, value);
            range = null;
        } else if (range) |*value| value.last = entry.key else range = .{ .first = entry.key, .last = entry.key };
    }
    if (range) |value| try delta_selection.unchanged.append(ca, value);
    delta_selection.finish();
    var delta: native.Plan = .{ .a = a, .selection = delta_selection, .by_id = positions, .borrowed_positions = true, .prior_directory = root };
    defer delta.deinit();
    const changed = try delta.mutable(0);
    changed.digest[0] ^= 1;
    var updates = delta.iterator(true);
    try std.testing.expectEqual(@as(usize, 0), ((try updates.next()).?).ordinal);
    try std.testing.expect((try updates.next()) == null);
    try std.testing.expect(delta_selection.unchanged.items.len <= 2);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 2, std.testing.io);
    const next = (try delta.publishDirectory(a, &store, domain, .none)).?;
    const decoded = try native.directoryFiles(ca, store, next, domain, .none);
    try std.testing.expectEqual(entries.len, decoded.len);
    for (decoded) |entry| {
        const ordinal = positions.get(entry.id).?;
        try std.testing.expectEqual((try delta.stateAt(ordinal)).digest, entry.digest);
        if (ordinal == 0) try std.testing.expectEqual(@as(u64, 0), entry.count);
    }
    var deletes: native.Deletes = .{ .files = &.{.{ .id = entries[0].file_id, .digest = old_digest }}, .current = &delta, .directory = root, .domain = domain };
    defer deletes.deinit();
    const deleted = (try deletes.next(a, store, .none)).?;
    defer a.free(deleted);
    try std.testing.expectEqualStrings(document, deleted);
    try std.testing.expect((try deletes.next(a, store, .none)) == null);
    // An earlier generic mark must not suppress native file-state expansion.
    var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 1, .authority = undefined, .store = store, .identity = @splat(7), .context = .{}, .token = @splat(1), .audit_only = true, .remaining_reads = 16 * 1024 * 1024 };
    defer {
        var keys = collector.marked.keyIterator();
        while (keys.next()) |id| a.free(id.*);
        collector.marked.deinit(a);
    }
    _ = try collector.mark(.{ .artifact_id = &try pages_mod.PageStore.identity(domain, root), .checksum = &std.fmt.bytesToHex(&root.digest, .lower), .byte_len = root.bytes });
    try collector.markNativeFiles(a, domain, root);
    try std.testing.expect(collector.marked.contains(uploaded.artifact_id));
}

test "external lake bounded WAL census delta borrows unchanged source state without flat native arrays" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-borrowed-census");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    var memory: local.serverless_query_lake_decoded_cache.Cache = .{ .a = a };
    defer memory.deinit();
    const chunk = try memory.create(1024 * 1024);
    defer chunk.release();
    const ma = chunk.item.budget.allocator();
    const inventory: Inventory = .{ .format = .iceberg, .source_id = try ma.dupe(u8, "source"), .source_uri = try ma.dupe(u8, "file:///source"), .snapshot_id = try ma.dupe(u8, "before"), .row_identity_version = 2, .schema_fingerprint = try ma.dupe(u8, "schema"), .files = try ma.alloc(local.serverless_external_source_types.FileEntry, 256) };
    chunk.item.payload = .{ .iceberg_chunk = inventory };
    for (inventory.files, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ma, "file-{d:0>4}", .{i}), .object_uri = try ma.dupe(u8, "file:///data.parquet"), .etag = try ma.dupe(u8, "immutable"), .byte_len = 128, .row_count = 1, .row_groups = &.{} };
    const index = try local.serverless_query_lake_inventory_catalog.Index.build(a, null, &.{.{ .key = @splat(1), .lease = chunk }}, .{});
    var snapshots: local.serverless_query_lake_decoded_cache.Cache = .{ .a = a };
    defer snapshots.deinit();
    const snapshot = try snapshots.create(1024 * 1024);
    defer snapshot.release();
    // The snapshot owns the catalog; its descriptor is borrowed from the chunk
    // only for this test, and cleared before the snapshot's ordinary teardown.
    snapshot.item.payload = .{ .snapshot = .{ .inventory = inventory, .catalog = index } };
    defer snapshot.item.payload.snapshot.inventory = .{ .format = .iceberg, .source_id = &.{}, .source_uri = &.{}, .snapshot_id = &.{}, .schema_fingerprint = &.{}, .files = &.{} };
    var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .scanner = undefined, .inventory = inventory, .plan_lease = snapshot, .immutable_objects = true };
    source.scanner.iceberg_delete_plan = null;
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    const first = try build(a, ca, &store, &provider, null, .none);
    defer first.deinit();
    try std.testing.expect(first.stats.cached_census);
    try std.testing.expectEqual(@as(usize, 256), first.stats.rehashed_files);
    source.inventory.snapshot_id = @constCast("after");
    const next = try build(a, ca, &store, &provider, first.root, .none);
    defer next.deinit();
    try std.testing.expect(next.stats.cached_census);
    try std.testing.expectEqual(@as(usize, 0), next.stats.rehashed_files);
    try std.testing.expectEqual(@as(usize, 0), next.stats.prior_records);
    try std.testing.expectEqual(@as(usize, 0), next.delta.files.len);
    try std.testing.expectEqual(@as(usize, 0), next.delta.changed.len);
    try std.testing.expectEqual(@as(usize, 1), next.delta.selection.?.unchanged.items.len);
    var plan = try native.Plan.fromDelta(a, ca, next.delta);
    defer plan.deinit();
    var changed = plan.iterator(true);
    try std.testing.expect((try changed.next()) == null);
    try std.testing.expectEqual(@as(usize, 0), plan.updates.count());
    provider.native_delta = next.delta;
    var fresh = try native.Plan.initPaged(a, ca, &provider, &.{}, null, false, false);
    defer fresh.deinit();
    try std.testing.expectEqual(@as(usize, 256), fresh.count());
    provider.native_delta.?.source = "another-source";
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, native.Plan.initPaged(a, ca, &provider, &.{}, null, false, false));
}
