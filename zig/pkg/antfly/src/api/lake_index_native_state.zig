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

//! Snapshot-independent private row identities and authenticated per-file build
//! state. Public row IDs remain fenced to the current serving snapshot.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
pub const File = struct {
    id: []const u8,
    digest: [32]u8,
    docs: []const artifacts.ChunkRef = &.{},
    count: u64 = 0,
    key_width: u16 = 96,
};
fn part(hash: *std.crypto.hash.Blake3, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}
pub fn recipe(table: local.common_topology_records.TableRecord, config: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    part(&hash, "native-lake-producer-schema-v3-signed-datetime");
    part(&hash, table.schema_json);
    part(&hash, config);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn identity(a: A, provider: *Provider, input_file: local.serverless_external_source_types.FileEntry) ![32]u8 {
    if (provider.native_delta) |delta| {
        if (!std.mem.eql(u8, delta.source, provider.source.inventory.source_id) or !std.mem.eql(u8, delta.schema, provider.source.inventory.schema_fingerprint)) return error.ExternalLakeSnapshotMismatch;
        const index = delta.by_id.get(input_file.file_id) orelse return error.ExternalLakeSnapshotMismatch;
        return delta.files[index].digest;
    }
    // Warm immutable plans retain an authenticated publication instead of
    // materializing every provider version into the fresh manifest inventory.
    // Private row keys must use those same versions as the index builder.
    var file = input_file;
    if (provider.source.verified_files) |files| {
        const verified = files.get(input_file.file_id) orelse return error.ExternalLakeSnapshotMismatch;
        if (!std.mem.eql(u8, verified.object_uri, input_file.object_uri) or verified.byte_len != input_file.byte_len) return error.ExternalLakeSnapshotMismatch;
        // Selectors come from coverage evidence; sequence/partition metadata
        // belongs to this immutable source snapshot, not a prior inventory.
        file.etag = verified.etag;
        file.version_id = verified.version_id;
    }
    if (provider.source.scanner.iceberg_delete_plan != null and provider.source.prepared_deletes == null) {
        var stream = try local.serverless_query_lake_stream.Stream.init(a, provider.source, &.{}, &.{}, provider.context, provider.limits);
        defer stream.deinit();
        try stream.prepareDeletes();
    }
    const deletes: [32]u8 = fingerprint: {
        if (provider.source.prepared_deletes) |prepared| {
            if (provider.expected_delete_objects) |expected| if (!std.mem.eql(u8, &expected, &prepared.object_versions)) return error.ExternalLakeIndexSourceChanged;
            break :fingerprint try prepared.fileFingerprint(file.file_id);
        } else {
            break :fingerprint local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint();
        }
    };
    return inventoryIdentity(a, provider.source.inventory, file, deletes);
}
/// The authenticated inventory and per-file delete proof fully determine the
/// native identity. Inventory delta planning uses the same function as scans.
pub fn inventoryIdentity(a: A, inventory: local.serverless_external_source_types.Inventory, file: local.serverless_external_source_types.FileEntry, deletes: [32]u8) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    part(&hash, "native-lake-private-file-v3");
    for ([_][]const u8{ inventory.source_id, inventory.schema_fingerprint, file.file_id, file.object_uri, file.etag, file.version_id }) |bytes| part(&hash, bytes);
    // Inventory deletes are sorted by file ID. Hash only this file's slice;
    // hashing the complete delete inventory per file would be quadratic.
    const groups = inventory.deleted_row_groups;
    var lower: usize = 0;
    var upper = groups.len;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        if (std.mem.order(u8, groups[middle].file_id, file.file_id) == .lt) lower = middle + 1 else upper = middle;
    }
    var end = lower;
    while (end < groups.len and std.mem.eql(u8, groups[end].file_id, file.file_id)) end += 1;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .bytes = file.byte_len, .data_sequence = file.data_sequence_number, .partition = file.partition_spec_id, .values = file.partition_values, .deletes = groups[lower..end] }, .{});
    defer a.free(encoded);
    part(&hash, encoded);
    hash.update(&deletes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn validate(files: []const File, domain: [32]u8) !void {
    if (files.len > 16384) return error.InvalidNativeLakeFileState;
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(std.heap.page_allocator);
    for (files) |file| {
        if (file.key_width != 96 and file.key_width != 108) return error.InvalidNativeLakeFileState;
        if (file.id.len == 0 or std.mem.allEqual(u8, &file.digest, 0) or (try names.getOrPut(std.heap.page_allocator, file.id)).found_existing) return error.InvalidNativeLakeFileState;
        var count: u64 = 0;
        for (file.docs) |ref| {
            if (ref.byte_len == 0 or ref.byte_len % file.key_width != 0 or ref.byte_len > 256 * 1024) return error.InvalidNativeLakeFileState;
            try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
            const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeFileState;
            if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeFileState;
            count = std.math.add(u64, count, ref.byte_len / file.key_width) catch return error.InvalidNativeLakeFileState;
        }
        if (count != file.count) return error.InvalidNativeLakeFileState;
    }
}
pub fn coordinates(input_key: []const u8) !struct { group: u32, row: u64 } {
    const key = try @import("lake_enrichment_units.zig").parent(input_key);
    const parts = try local.storage_rowsource_identity.parse(key);
    return .{ .group = parts.group, .row = parts.row };
}
pub const Plan = struct {
    a: A,
    files: []File,
    changed: []bool,
    by_id: std.StringHashMapUnmanaged(usize) = .empty,
    pub fn init(a: A, out: A, provider: *Provider, previous: []const File) !Plan {
        var self: Plan = .{ .a = a, .files = try out.alloc(File, provider.source.inventory.files.len), .changed = try a.alloc(bool, provider.source.inventory.files.len) };
        errdefer self.deinit();
        var old: std.StringHashMapUnmanaged(*const File) = .empty;
        defer old.deinit(a);
        for (previous) |*file| try old.put(a, file.id, file);
        for (provider.source.inventory.files, self.files, self.changed, 0..) |file, *state, *changed, index| {
            state.* = .{ .id = file.file_id, .digest = try identity(a, provider, file) };
            changed.* = true;
            if (old.get(file.file_id)) |prior| if (std.mem.eql(u8, &prior.digest, &state.digest)) {
                state.docs = prior.docs;
                state.count = prior.count;
                state.key_width = prior.key_width;
                changed.* = false;
            };
            try self.by_id.put(a, file.file_id, index);
        }
        return self;
    }
    pub fn fromDelta(a: A, out: A, delta: @import("lake_source_navigation.zig").Delta) !Plan {
        var self: Plan = .{ .a = a, .files = try out.dupe(File, delta.files), .changed = try a.dupe(bool, delta.changed) };
        errdefer self.deinit();
        for (self.files, 0..) |file, i| try self.by_id.put(a, file.id, i);
        return self;
    }
    pub fn fromInventory(a: A, out: A, provider: *Provider, prior: local.serverless_external_source_types.Inventory) !Plan {
        const previous = try a.alloc(File, prior.files.len);
        defer a.free(previous);
        for (prior.files, previous) |file, *owned| {
            try provider.context.ensureActive();
            owned.* = .{ .id = file.file_id, .digest = try inventoryIdentity(a, prior, file, local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint()) };
        }
        return init(a, out, provider, previous);
    }
    pub fn deinit(self: *Plan) void {
        self.a.free(self.changed);
        self.by_id.deinit(self.a);
    }
    pub fn privateKey(self: *const Plan, a: A, ref: local.storage_rowsource_types.RowRef) ![]u8 {
        const row = switch (ref) {
            .external => |row| row,
            else => return error.SidecarSourceBindingMismatch,
        };
        const file = self.by_id.get(row.file_id) orelse return error.SidecarSourceBindingMismatch;
        return std.fmt.allocPrint(a, "lake2:{s}:{x:0>8}:{x:0>16}", .{ std.fmt.bytesToHex(&self.files[file].digest, .lower), row.row_group_ordinal, row.row_ordinal });
    }
    pub fn unchanged(self: *const Plan, file: File) bool {
        const ordinal = self.by_id.get(file.id) orelse return false;
        return !self.changed[ordinal] and std.mem.eql(u8, &file.digest, &self.files[ordinal].digest);
    }
};
pub const Tracker = struct {
    plan: *Plan,
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    cancellation: Cancellation,
    refs: []std.ArrayList(artifacts.ChunkRef),
    pending: std.ArrayList(u8) = .empty,
    active: ?usize = null,
    output_bytes: u64 = 256 * 1024 * 1024,
    pub fn init(a: A, out: A, plan: *Plan, store: *stores.ArtifactStore, cancellation: Cancellation) !Tracker {
        const refs = try a.alloc(std.ArrayList(artifacts.ChunkRef), plan.files.len);
        @memset(refs, .empty);
        return .{ .plan = plan, .a = a, .out = out, .store = store, .cancellation = cancellation, .refs = refs };
    }
    pub fn deinit(self: *Tracker) void {
        for (self.refs) |*refs| refs.deinit(self.a);
        self.a.free(self.refs);
        self.pending.deinit(self.a);
    }
    pub fn append(self: *Tracker, ref: local.storage_rowsource_types.RowRef, key: []const u8) !void {
        if ((key.len != 96 and key.len != 108) or !std.mem.startsWith(u8, key, "lake2:")) return error.InvalidNativeLakeFileState;
        const file = self.plan.by_id.get(ref.external.file_id) orelse return error.SidecarSourceBindingMismatch;
        if (self.plan.files[file].count == 0) self.plan.files[file].key_width = @intCast(key.len);
        if (self.plan.files[file].key_width != key.len) return error.InvalidNativeLakeFileState;
        if (self.active != null and self.active.? != file) try self.flush();
        self.active = file;
        try self.pending.appendSlice(self.a, key);
        self.plan.files[file].count += 1;
        if (self.pending.items.len >= 96 * 2048) try self.flush();
    }
    fn flush(self: *Tracker) !void {
        if (self.pending.items.len == 0) return;
        try stores.chargeReadBudget(&self.output_bytes, self.pending.items.len);
        var store = self.store.*;
        store.allocator = self.out;
        const ref = try store.putWithCancellation(self.pending.items, self.cancellation);
        defer if (ref.provider_version) |version| store.allocator.free(version);
        try self.refs[self.active.?].append(self.a, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len });
        self.pending.clearRetainingCapacity();
    }
    pub fn finish(self: *Tracker) !void {
        try self.flush();
        for (self.plan.files, self.plan.changed, self.refs) |*file, changed, refs| if (changed) {
            file.docs = try self.out.dupe(artifacts.ChunkRef, refs.items);
        };
    }
};
/// Stream IDs for removed/replaced files; callers apply bounded native deletes.
pub const Deletes = struct {
    files: []const File,
    current: *const Plan,
    file: usize = 0,
    chunk: usize = 0,
    budget: u64 = 256 * 1024 * 1024,
    key_width: usize = 96,
    pub fn next(self: *Deletes, a: A, store: stores.ArtifactStore, cancellation: Cancellation) !?[]const u8 {
        while (self.file < self.files.len) {
            const file = self.files[self.file];
            if (self.current.unchanged(file) or self.chunk == file.docs.len) {
                self.file += 1;
                self.chunk = 0;
                continue;
            }
            const ref = file.docs[self.chunk];
            self.chunk += 1;
            try stores.chargeReadBudget(&self.budget, ref.byte_len);
            const bytes = try artifacts.readArtifact(a, store, ref, cancellation, null);
            errdefer a.free(bytes);
            self.key_width = file.key_width;
            if (bytes.len % self.key_width != 0) return error.InvalidNativeLakeFileState;
            for (0..bytes.len / self.key_width) |row| {
                const key = bytes[row * self.key_width ..][0..self.key_width];
                const expected = std.fmt.bytesToHex(&file.digest, .lower);
                if (!std.mem.startsWith(u8, key, "lake2:") or !std.mem.eql(u8, key[6..70], &expected)) return error.InvalidNativeLakeFileState;
                _ = try coordinates(key);
            }
            return bytes;
        }
        return null;
    }
};

test "external lake incremental native file identity ignores snapshot labels and binds deletion evidence" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-file-identity");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "n", .values = &.{1} }});
    defer a.free(data);
    var uploaded = try client.putObject("antfly", "part.parquet", data, .{});
    uploaded.deinit(a);
    const schema_json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .expected_delete_objects = @splat(1) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var first = try Plan.init(a, ca, &provider, &.{});
    defer first.deinit();
    try std.testing.expect(first.changed[0]);
    const authenticated_file = source.inventory.files[0];
    var unresolved_file = authenticated_file;
    unresolved_file.etag = "";
    unresolved_file.version_id = @constCast("iceberg:v1:unresolved");
    try std.testing.expect(!std.mem.eql(u8, &first.files[0].digest, &try identity(a, &provider, unresolved_file)));
    var verified_files: std.StringHashMapUnmanaged(*const local.serverless_external_source_types.FileEntry) = .empty;
    defer verified_files.deinit(a);
    try verified_files.put(a, authenticated_file.file_id, &authenticated_file);
    source.verified_files = &verified_files;
    try std.testing.expectEqual(first.files[0].digest, try identity(a, &provider, unresolved_file));
    unresolved_file.object_uri = @constCast("object://antfly/conflicting.parquet");
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, identity(a, &provider, unresolved_file));
    source.verified_files = null;
    const snapshot = source.inventory.snapshot_id;
    source.inventory.snapshot_id = try a.dupe(u8, "next-snapshot");
    defer {
        a.free(source.inventory.snapshot_id);
        source.inventory.snapshot_id = snapshot;
    }
    var retained = try Plan.init(a, ca, &provider, first.files);
    defer retained.deinit();
    try std.testing.expect(!retained.changed[0]);
    var metadata_cache: @import("lake_index_search_metadata.zig").Cache = .{ .max_entries = 1 };
    defer metadata_cache.deinit();
    const metadata = try metadata_cache.acquire(@splat(1), &provider);
    defer metadata.release();
    const reused_metadata = try metadata_cache.acquire(@splat(1), &provider);
    defer reused_metadata.release();
    try std.testing.expect(metadata == reused_metadata);
    const next_metadata = try metadata_cache.acquire(@splat(2), &provider);
    defer next_metadata.release();
    try std.testing.expect(next_metadata != metadata);
    try std.testing.expectEqual(@as(usize, 1), metadata_cache.entries.count());
    provider.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, metadata_cache.acquire(@splat(1), &provider));
    provider.context.deadline_ns = null;
    provider.expected_delete_objects = @splat(2);
    var deleted = try Plan.init(a, ca, &provider, first.files);
    defer deleted.deinit();
    // A global delete-object version alone must not invalidate unrelated files.
    try std.testing.expect(!deleted.changed[0]);
    const previous_groups = source.inventory.deleted_row_groups;
    var deleted_rows = [_]u64{0};
    var deleted_groups = [_]local.serverless_external_source_types.DeletedRowGroup{.{ .file_id = source.inventory.files[0].file_id, .row_group_ordinal = 0, .row_ordinals = &deleted_rows }};
    source.inventory.deleted_row_groups = &deleted_groups;
    defer source.inventory.deleted_row_groups = previous_groups;
    var localized = try Plan.init(a, ca, &provider, first.files);
    defer localized.deinit();
    try std.testing.expect(localized.changed[0]);
    const private = try retained.privateKey(ca, .{ .external = .{ .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .file_id = source.inventory.files[0].file_id, .row_group_ordinal = 3, .row_ordinal = 9007199254740993 } });
    const position = try coordinates(private);
    try std.testing.expectEqual(@as(u32, 3), position.group);
    try std.testing.expectEqual(@as(u64, 9007199254740993), position.row);
}
