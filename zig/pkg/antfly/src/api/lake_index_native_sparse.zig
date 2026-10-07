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

//! Remote sparse generations use the native sparse index and its retained LSM
//! checkpoint. No second posting codec or ranking implementation is involved.
const std = @import("std");
const local = @import("antfly_local_sources");
const state = @import("lake_index_native_state.zig");
const files = @import("lake_index_native_files.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const rebuild = @import("../serverless/build/lake_rebuild.zig");
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
pub const metadata_version: u16 = 2;
pub const Root = struct {
    version: u16 = metadata_version,
    binding: local.serverless_segment_source_binding.Binding,
    config_json: []const u8,
    generation: files.Root,
    recipe: [32]u8 = @splat(0),
    file_states: []const state.File = &.{},
};
pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .sparse_segment or ref.metadata_version != metadata_version or ref.byte_len > files.max_root_bytes) return error.InvalidNativeLakeSparseRoot;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    if (root.version != metadata_version or root.binding.sidecar_kind != .sparse or root.binding.column_bindings.len != 1 or root.config_json.len > 256 * 1024) return error.InvalidNativeLakeSparseRoot;
    try root.binding.validate();
    try root.generation.validate();
    try state.validate(root.file_states, root.generation.domain);
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeSparseRoot;
    if (!std.mem.eql(u8, &scope.domain, &root.generation.domain)) return error.InvalidNativeLakeSparseRoot;
    return root;
}
pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, base: local.serverless_manifest_base_source.BaseSourceDescriptor, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared) ![]const Declared {
    return buildIncremental(a, out, table, source, base, store, provider, cancellation, reusable, &.{});
}
pub fn buildIncremental(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, base: local.serverless_manifest_base_source.BaseSourceDescriptor, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared, candidates: []const Declared) ![]const Declared {
    var desired = try rebuild.desiredArtifactsFromResolvedExternalSourceAlloc(a, base, source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .indexes_json = table.indexes_json });
    defer desired.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const configs = try std.json.parseFromSliceLeaky(std.json.Value, ca, table.indexes_json, .{});
    var result: std.ArrayList(Declared) = .empty;
    errdefer result.deinit(out);
    for (desired.artifacts) |wanted| {
        if (wanted.kind != .sparse_segment) continue;
        try provider.context.ensureActive();
        var binding = wanted.binding;
        binding.index_config_hash = try std.fmt.allocPrint(ca, "native-sparse-checkpoint-v2:{s}", .{binding.index_config_hash});
        const public_config = try std.json.Stringify.valueAlloc(ca, configs.object.get(wanted.name) orelse return error.InvalidTableIndexMetadata, .{});
        const recipe = state.recipe(table, public_config);
        const prior = for (reusable) |declaration| {
            if (declaration.artifact.kind == .sparse_segment and declaration.artifact.metadata_version == metadata_version and std.mem.eql(u8, declaration.name, wanted.name) and rebuild.bindingsEqual(binding, declaration.binding)) break declaration;
        } else null;
        if (prior) |declaration| {
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (std.mem.eql(u8, &root.recipe, &recipe)) {
                try result.append(out, declaration);
                continue;
            }
        }
        const seed: ?Root = for (candidates) |declaration| {
            if (declaration.artifact.kind != .sparse_segment or declaration.artifact.metadata_version != metadata_version or !std.mem.eql(u8, declaration.name, wanted.name)) continue;
            var previous_binding = declaration.binding;
            previous_binding.snapshot_id = binding.snapshot_id;
            if (!rebuild.bindingsEqual(binding, previous_binding)) continue;
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (!std.mem.eql(u8, &root.recipe, &recipe) or !std.mem.eql(u8, &root.generation.domain, &store.upload_scope.?.domain)) continue;
            break root;
        } else null;
        var plan = try state.Plan.init(a, ca, provider, if (seed) |root| root.file_states else &.{});
        defer plan.deinit();
        var tracker = try state.Tracker.init(a, ca, &plan, store, cancellation);
        defer tracker.deinit();
        const io = provider.context.io orelse return error.UnsupportedSqlExecution;
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const path = try std.fmt.allocPrintSentinel(a, "{s}/antfly-lake-sparse-{s}", .{ @import("antfly_platform").filesystem.temporaryDirectory(), std.fmt.bytesToHex(random, .lower) }, 0);
        defer a.free(path);
        try std.Io.Dir.cwd().createDir(io, path, if (@hasDecl(std.Io.File.Permissions, "fromMode")) .fromMode(0o700) else .default_dir);
        defer std.Io.Dir.cwd().deleteTree(io, path) catch {};
        const overlay = if (seed) |root| try files.Overlay.create(a, root.generation, store.*, path, provider.context, cancellation) else null;
        defer if (overlay) |candidate| candidate.release();
        const candidate_storage = if (overlay) |candidate| candidate.storage() else null;
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = 256 * 1024 * 1024 };
        var index = try local.sparse_sparse.SparseIndex.open(budget.allocator(), path, .{ .lsm_storage = candidate_storage, .lsm_options = .{ .flush_threshold = 8 * 1024 * 1024 } });
        defer index.close();
        if (seed == null) try index.beginBulkIngestSession();
        var finished = false;
        defer if (!finished and seed == null) index.abortBulkIngestSession();
        if (seed) |root| {
            var deletes: state.Deletes = .{ .files = root.file_states, .current = &plan };
            while (try deletes.next(a, store.*, cancellation)) |keys| {
                defer a.free(keys);
                const ids = try a.alloc([]const u8, keys.len / 96);
                defer a.free(ids);
                for (ids, 0..) |*id, row| id.* = keys[row * 96 ..][0..96];
                try index.batchWithOptions(&.{}, ids, .{});
            }
        }
        var input_provider = provider.*;
        input_provider.only_files = plan.changed;
        const rows = try input_provider.provider().open_with_cancellation_fn.?(input_provider.provider().ptr, a, binding, cancellation);
        defer rows.deinit(a);
        var input: u64 = 512 * 1024 * 1024;
        while (try rows.next(a)) |batch| {
            var page_arena = std.heap.ArenaAllocator.init(a);
            defer page_arena.deinit();
            const pa = page_arena.allocator();
            var writes: std.ArrayList(local.sparse_sparse.SparseWrite) = .empty;
            for (batch.row_refs, 0..) |ref, ordinal| {
                try cancellation.check();
                try provider.context.ensureActive();
                const page: local.sql_catalog.ColumnPage = .{ .batch = batch, .selection = &.{ordinal} };
                var value = (try page.cell(pa, 0, wanted.build_spec.?.sparse.sparse_column)).value;
                if (value == .null) continue;
                if (value == .string) value = try std.json.parseFromSliceLeaky(std.json.Value, pa, value.string, .{});
                const vector = try local.storage_db_document_mapper.parseSparseValue(pa, value);
                try stores.chargeReadBudget(&input, @as(u64, @intCast(vector.indices.len)) * 8 + 128);
                const key = try plan.privateKey(pa, ref);
                try tracker.append(ref, key);
                try writes.append(pa, .{ .doc_id = key, .vec = .{ .indices = vector.indices, .values = vector.values } });
            }
            try index.batchWithOptions(writes.items, &.{}, .{ .prefer_bulk_build = seed == null, .assume_new_doc_ids = true, .backend_batch_options = .{ .mode = if (seed == null) .bulk_ingest else .default } });
        }
        if (seed == null) try index.finishBulkIngestSessionWithOptions(.{});
        try tracker.finish();
        finished = true;
        var checkpoint = try index.pinNativeCheckpoint();
        defer checkpoint.deinit();
        var output_bytes: u64 = 1024 * 1024 * 1024;
        const generation = try files.publishCheckpoint(a, ca, &checkpoint, store, cancellation, &output_bytes, if (seed) |root| .{ .root = root.generation } else null);
        const root: Root = .{ .binding = binding, .config_json = try std.json.Stringify.valueAlloc(ca, configs.object.get(wanted.name) orelse return error.InvalidTableIndexMetadata, .{}), .generation = generation, .recipe = recipe, .file_states = plan.files };
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        if (bytes.len > files.max_root_bytes) return error.NativeLakeFilesTooLarge;
        var uploaded = try store.putWithCancellation(bytes, cancellation);
        defer uploaded.deinit(store.allocator);
        const declaration: Declared = .{ .name = wanted.name, .binding = binding, .artifact = .{ .kind = .sparse_segment, .name = wanted.name, .metadata_version = metadata_version, .artifact_id = uploaded.artifact_id, .byte_len = uploaded.byte_len, .checksum = uploaded.checksum } };
        const encoded = try std.json.Stringify.valueAlloc(ca, declaration, .{});
        try result.append(out, try std.json.parseFromSliceLeaky(Declared, out, encoded, .{ .allocate = .alloc_always }));
    }
    return result.toOwnedSlice(out);
}
