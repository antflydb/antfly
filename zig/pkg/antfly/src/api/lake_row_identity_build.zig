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

//! Native typed stable-key verification over delete-aware external batches.
//! Discovery reads source values; verification never assumes the declared key.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const rows = local.storage_rowsource_types;
const tuples = local.storage_db_relational_index_keys;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;

fn bytes(hash: *std.crypto.hash.Blake3, value: []const u8) void {
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, value.len, .big);
    hash.update(&size);
    hash.update(value);
}
/// Includes validity, column domain and exact input representation. In
/// particular SQL NULL and JSON null, integers and floats remain distinct.
/// Encoding differences may conservatively rebuild; they never authorize reuse.
fn values(a: A, recipe: identity.logical.Digest, batch: rows.ColumnBatch, row: usize, names: []const []const u8) !identity.logical.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.physical-values.v1");
    hash.update(&recipe);
    for (names) |name| {
        bytes(&hash, name);
        const column = batch.findColumn(name) orelse return error.InvalidLogicalRowIdentity;
        hash.update(&.{@intFromBool(column.nulls.isNull(row))});
        if (column.nulls.isNull(row)) continue;
        switch (column.values) {
            .vector_f32 => |vectors| {
                bytes(&hash, "vector_f32");
                var count: [8]u8 = undefined;
                std.mem.writeInt(u64, &count, vectors[row].len, .big);
                hash.update(&count);
                for (vectors[row]) |value| {
                    var word: [4]u8 = undefined;
                    std.mem.writeInt(u32, &word, @bitCast(value), .big);
                    hash.update(&word);
                }
            },
            else => {
                // jsonAt decodes dictionaries, retains exact decimal text and
                // parses JSON numbers without an f64 round trip.
                const value = try column.values.jsonAt(a, row);
                bytes(&hash, @tagName(value));
                const encoded = try std.json.Stringify.valueAlloc(a, value, .{});
                defer a.free(encoded);
                bytes(&hash, encoded);
            },
        }
    }
    var digest: identity.logical.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, store: *@import("../serverless/artifacts/store.zig").ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cut: identity.logical.Digest, navigation: local.serverless_manifest_artifact_ref.ArtifactRef, previous: ?identity.Root, token: Token) !?identity.Root {
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer parsed.deinit(a);
    const binding = (parsed.external_base_source orelse return error.InvalidExternalTableBinding).binding;
    const adapter = binding.row_identity orelse return null;
    const runtime = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, runtime);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, runtime);
    defer layout.deinit();
    const definitions = try a.alloc(local.storage_relational_index.RelationalIndexKey, adapter.key_columns.len);
    defer a.free(definitions);
    for (adapter.key_columns, definitions) |column, *definition| definition.* = .{ .column = column };
    var tuple = try tuples.TuplePlan.init(a, runtime, &layout, definitions);
    defer tuple.deinit();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.table-incarnation.v1");
    var table_id: [16]u8 = undefined;
    std.mem.writeInt(u64, table_id[0..8], table.table_id, .big);
    std.mem.writeInt(u64, table_id[8..16], table.object_storage_generation, .big);
    hash.update(&table_id);
    bytes(&hash, binding.table_id);
    var incarnation: identity.logical.Digest = undefined;
    hash.final(&incarnation);
    const namespace: identity.logical.Namespace = .{ .incarnation = incarnation, .adapter = .stable_key, .recipe = tuple.fingerprint };
    // Whole-cut reuse initially hashes every declared source input. This is
    // conservative for changes to fields that no current index consumes.
    const recipe = local.metadata_lake_index_catalog.desiredFingerprint(table);
    const columns = try a.alloc([]const u8, runtime.relational_columns.len);
    defer a.free(columns);
    const contract = try a.alloc(local.serverless_query_lake_schema.Column, runtime.relational_columns.len);
    defer a.free(contract);
    for (runtime.relational_columns, columns, contract) |column, *name, *item| {
        name.* = column.path;
        item.* = .{ .name = column.path, .kind = @tagName(column.column_type), .required = column.required and !column.allows_null };
    }
    const inventory = provider.source.inventory;
    const files = try out.alloc([]const u8, inventory.files.len);
    const slots = try a.alloc(u32, inventory.files.len);
    defer a.free(slots);
    var by_file: std.StringHashMapUnmanaged(u32) = .empty;
    defer by_file.deinit(a);
    for (inventory.files, files, slots, 0..) |file, *name, *slot, index| {
        name.* = try out.dupe(u8, file.file_id);
        slot.* = std.math.cast(u32, index) orelse return error.InvalidLogicalRowIdentity;
        try by_file.put(a, file.file_id, slot.*);
    }
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = provider.context.io orelse return error.InvalidLogicalRowIdentity, .context = provider, .checkpoint = checkpoint };
    defer manager.deinit();
    var builder = try identity.Builder.init(a, &manager, files, 4 * 1024 * 1024, .{ .source = inventory.source_id, .snapshot = inventory.snapshot_id, .version = inventory.row_identity_version });
    defer builder.deinit();
    var read_bytes: u64 = 512 * 1024 * 1024;
    var write_bytes: u64 = 1024 * 1024 * 1024;
    const scope = store.upload_scope orelse return error.InvalidLogicalRowIdentity;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes, .cancellation = token, .streaming = true };
    pages.admitPublication();
    // Reuse verified logical state for unchanged physical files. The source
    // delta includes object versions and delete visibility; snapshot labels or
    // file names alone cannot bypass decoding changed files.
    var scan_files: ?[]bool = null;
    defer if (scan_files) |mask| a.free(mask);
    if (previous) |prior| if (provider.native_delta) |delta| {
        if (delta.prior_snapshot != null and delta.count() == inventory.files.len and delta.prior_count == prior.current.file_count and
            std.mem.eql(u8, &prior.domain, &scope.domain) and std.mem.eql(u8, &prior.namespace, &namespace.digest()) and
            std.mem.eql(u8, &prior.values_recipe, &recipe) and std.mem.eql(u8, prior.current.source, inventory.source_id) and
            std.mem.eql(u8, delta.prior_snapshot.?, prior.current.snapshot))
        {
            const remap = try a.alloc(u32, prior.current.file_count);
            defer a.free(remap);
            @memset(remap, std.math.maxInt(u32));
            var files_cursor = @import("../serverless/graph_segment/page_tree.zig").Cursor.init(a, pages.store(), prior.current.files, "", null) catch |err| return identity.derivedReadFailure(err);
            defer files_cursor.deinit();
            var reused: usize = 0;
            var seen_files: usize = 0;
            while (files_cursor.next() catch |err| return identity.derivedReadFailure(err)) |record| {
                if (record.key.len != 4 or std.mem.readInt(u32, record.key[0..4], .big) != seen_files or seen_files >= remap.len) return error.InvalidLogicalRowIdentity;
                if (by_file.get(record.value)) |slot| if (!delta.isChanged(slot)) {
                    remap[seen_files] = slot;
                    reused += 1;
                };
                seen_files += 1;
            }
            if (seen_files != remap.len) return error.InvalidLogicalRowIdentity;
            if (reused != 0) {
                scan_files = try a.alloc(bool, inventory.files.len);
                @memset(scan_files.?, true);
                for (remap) |slot| if (slot != std.math.maxInt(u32)) {
                    scan_files.?[slot] = false;
                };
                var cursor = @import("../serverless/graph_segment/page_tree.zig").Cursor.init(a, pages.store(), prior.current.by_id, "", null) catch |err| return identity.derivedReadFailure(err);
                defer cursor.deinit();
                var seen: u64 = 0;
                while (cursor.next() catch |err| return identity.derivedReadFailure(err)) |record| {
                    try token.check();
                    try provider.context.ensureActive();
                    const state = try identity.decodeState(prior.current, record.key, record.value);
                    seen += 1;
                    const slot = remap[state.coordinate.file];
                    if (slot != std.math.maxInt(u32)) try builder.add(state.state, .{ .file = slot, .group = state.coordinate.group, .row = state.coordinate.row });
                }
                if (seen != prior.current.rows) return error.InvalidLogicalRowIdentity;
            }
        }
    };
    var input = provider.*;
    input.only_file = null;
    input.only_files = scan_files;
    input.only_plan = null;
    input.replay = null;
    input.replay_consumer = null;
    input.enrichment_columns = false;
    input.schema_contract = contract;
    input.statistics = null;
    var source = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, .{ .sidecar_kind = .text, .source_kind = if (inventory.format == .iceberg) .external_iceberg else .external_parquet, .row_ref_kind = .external, .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .schema_fingerprint = inventory.schema_fingerprint, .index_config_hash = "native-logical-row-state-v1", .column_bindings = columns }, token);
    defer source.deinit(a);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(a);
    while (try source.next(a)) |batch| {
        var bound = try tuple.bindExternalBatch(a, batch);
        defer bound.deinit();
        for (0..batch.rowCount()) |row| {
            try token.check();
            try provider.context.ensureActive();
            _ = scratch.reset(.retain_capacity);
            key.clearRetainingCapacity();
            if (try bound.append(a, &key, row)) return error.NullLogicalRowKey;
            const ref = batch.rowRef(row);
            if (ref != .external) return error.InvalidLogicalRowIdentity;
            const slot = by_file.get(ref.external.file_id) orelse return error.InvalidLogicalRowIdentity;
            try builder.add(.{ .id = try identity.logical.keyId(namespace, key.items), .values = try values(scratch.allocator(), recipe, batch, row, columns) }, .{ .file = slot, .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal });
        }
    }
    var root = try builder.finish(&pages, try out.dupe(u8, inventory.source_id), try out.dupe(u8, inventory.snapshot_id), namespace.digest(), recipe, cut);
    root.current.navigation = navigation;
    return root;
}
fn checkpoint(raw: *anyopaque) !void {
    const provider: *@import("lake_index_row_source.zig").Provider = @ptrCast(@alignCast(raw));
    try provider.context.ensureActive();
}
