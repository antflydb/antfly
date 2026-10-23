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
pub const metadata_version: u16 = 4;
pub fn supportsMetadata(version: u16) bool {
    return version == 2 or version == 3 or version == metadata_version;
}
pub const Root = struct {
    version: u16 = metadata_version,
    binding: local.serverless_segment_source_binding.Binding,
    config_json: []const u8,
    generation: files.Root,
    recipe: [32]u8 = @splat(0),
    file_states: []const state.File = &.{},
    file_directory: ?@import("../serverless/graph_segment/page_tree.zig").Ref = null,
};
pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .sparse_segment or !supportsMetadata(ref.metadata_version) or ref.byte_len > files.max_root_bytes) return error.InvalidNativeLakeSparseRoot;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    if (root.version != ref.metadata_version or root.binding.sidecar_kind != .sparse or root.binding.column_bindings.len != 1 or root.config_json.len > 256 * 1024) return error.InvalidNativeLakeSparseRoot;
    try root.binding.validate();
    try root.generation.validate();
    try state.validate(root.file_states, root.generation.domain);
    if (root.file_directory) |directory| {
        try directory.validate();
        if (root.version < 3 or root.file_states.len != 0) return error.InvalidNativeLakeFileState;
    }
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeSparseRoot;
    if (!std.mem.eql(u8, &scope.domain, &root.generation.domain)) return error.InvalidNativeLakeSparseRoot;
    return root;
}
/// Changed immutable file/fragment digests produce fresh private keys. Keep
/// fresh writes in native immutable posting segments: scalar insertion repeatedly
/// dequantizes/re-encodes retained chunks and accumulates score error.
fn appendFresh(index: *local.sparse_sparse.SparseIndex, writes: []const local.sparse_sparse.SparseWrite, initial: bool) !void {
    try index.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true, .backend_batch_options = .{ .mode = if (initial) .bulk_ingest else .default } });
}
fn compactNative(index: *local.sparse_sparse.SparseIndex, a: A, budget: *local.sql_memory_budget, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, path: []const u8, maximum: usize) !void {
    for (0..maximum) |_| {
        try cancellation.check();
        try provider.context.ensureActive();
        const compacted = index.compactSegmentsWithOptions(a, .{ .min_segments = 32, .max_segments = 32, .size_tiered = true, .exact_forward_weights = true, .scratch = .{ .io = provider.context.io.?, .directory = path, .cancellation = cancellation, .max_input_bytes = @intCast(@min(provider.identity_spill_bytes, std.math.maxInt(usize))) } }) catch |err| {
            if (err == error.ResourceBudgetExceeded or (err == error.OutOfMemory and budget.isLocallyExhausted())) return;
            return err;
        };
        if (!compacted) return;
    }
}
pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, base: local.serverless_manifest_base_source.BaseSourceDescriptor, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared) ![]const Declared {
    return buildIncremental(a, out, table, source, base, store, provider, cancellation, reusable, &.{});
}
pub fn buildIncremental(a: A, out: A, table: local.common_topology_records.TableRecord, parent_source: *local.serverless_query_lake_serving.ServingSource, base: local.serverless_manifest_base_source.BaseSourceDescriptor, store: *stores.ArtifactStore, parent_provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared, candidates: []const Declared) ![]const Declared {
    var desired = try rebuild.desiredArtifactsFromResolvedExternalSourceAlloc(a, base, parent_source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .indexes_json = table.indexes_json });
    defer desired.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const configs = try std.json.parseFromSliceLeaky(std.json.Value, ca, table.indexes_json, .{});
    var result: std.ArrayList(Declared) = .empty;
    errdefer result.deinit(out);
    for (desired.artifacts) |wanted| {
        var source_view = parent_source.*;
        var selected_provider = parent_provider.forIndex(&source_view, wanted.name);
        const provider = &selected_provider;
        if (!provider.wantsIndex(wanted.name)) continue;
        if (wanted.kind != .sparse_segment) continue;
        try provider.context.ensureActive();
        var binding = wanted.binding;
        binding.index_config_hash = try std.fmt.allocPrint(ca, "native-sparse-checkpoint-v9:{s}", .{binding.index_config_hash});
        const public_config = try std.json.Stringify.valueAlloc(ca, configs.object.get(wanted.name) orelse return error.InvalidTableIndexMetadata, .{});
        const configuration = configs.object.get(wanted.name).?;
        const whole_row = @import("lake_enrichment_units.zig").configured(configuration, "template") != null or !@import("lake_row_input_recipes.zig").configurationReusable(configuration);
        const recipe = try state.recipe(ca, table, public_config, if (whole_row) null else wanted.binding.column_bindings);
        var producer = try @import("lake_vector_enrichment.zig").Producer.init(a, wanted.name, wanted.build_spec.?.sparse.sparse_column, configs.object.get(wanted.name).?, provider.embedding_options);
        defer producer.deinit();
        producer.memo = provider.vector_memo;
        const prior = for (reusable) |declaration| {
            if (declaration.artifact.kind == .sparse_segment and supportsMetadata(declaration.artifact.metadata_version) and std.mem.eql(u8, declaration.name, wanted.name) and rebuild.bindingsEqual(binding, declaration.binding)) break declaration;
        } else null;
        if (prior) |declaration| {
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (std.mem.eql(u8, &root.recipe, &recipe)) {
                try result.append(out, declaration);
                continue;
            }
        }
        const seed: ?Root = for (candidates) |declaration| {
            if (declaration.artifact.kind != .sparse_segment or !supportsMetadata(declaration.artifact.metadata_version) or !std.mem.eql(u8, declaration.name, wanted.name)) continue;
            var previous_binding = declaration.binding;
            previous_binding.snapshot_id = binding.snapshot_id;
            if (!rebuild.bindingsEqual(binding, previous_binding)) continue;
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (!std.mem.eql(u8, &root.recipe, &recipe) or !std.mem.eql(u8, &root.generation.domain, &store.upload_scope.?.domain)) continue;
            break root;
        } else null;
        if (provider.resume_work) |work| if (work.file != null and seed != null) {
            const accepted = for (candidates) |declaration| {
                if (work.trusts(store.*, declaration.artifact) and std.mem.eql(u8, declaration.name, wanted.name)) break true;
            } else false;
            if (!accepted) return error.InvalidNativeLakeFileState;
        };
        const appending = if (provider.resume_work) |work| work.file != null else false;
        const previous_states = if (appending) &.{} else if (seed) |root| if (root.file_directory != null and !state.canUseDelta(provider, root.binding.snapshot_id)) try state.directoryFiles(ca, store.*, root.file_directory.?, root.generation.domain, cancellation) else root.file_states else &.{};
        var plan = try state.Plan.initPaged(a, ca, provider, previous_states, if (seed) |root| root.file_directory else null, seed != null, if (seed) |root| state.canUseDelta(provider, root.binding.snapshot_id) else false);
        defer plan.deinit();
        var tracker = try state.Tracker.init(a, ca, &plan, store, cancellation);
        defer tracker.deinit();
        tracker.cumulative_bytes = if (provider.resume_work) |work| &work.native_keys_left else null;
        const io = provider.context.io orelse return error.UnsupportedSqlExecution;
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const path = try std.fmt.allocPrintSentinel(a, "/tmp/antfly-lake-sparse-{s}", .{std.fmt.bytesToHex(random, .lower)}, 0);
        defer a.free(path);
        try std.Io.Dir.cwd().createDir(io, path, .fromMode(0o700));
        defer std.Io.Dir.cwd().deleteTree(io, path) catch {};
        const overlay = if (seed) |root| try files.Overlay.create(a, root.generation, store.*, path, provider.context, cancellation) else null;
        defer if (overlay) |candidate| candidate.release();
        const candidate_storage = if (overlay) |candidate| candidate.storage() else null;
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = 256 * 1024 * 1024 };
        var index = try local.sparse_sparse.SparseIndex.open(budget.allocator(), path, .{ .lsm_storage = candidate_storage, .lsm_options = .{ .flush_threshold = 8 * 1024 * 1024 } });
        defer index.close();
        var baseline = if (seed != null) try index.pinNativeCheckpoint() else null;
        defer if (baseline) |*cut| cut.deinit();
        if (seed == null) try index.beginBulkIngestSession();
        var finished = false;
        defer if (!finished and seed == null) index.abortBulkIngestSession();
        if (seed) |root| {
            var deletes: state.Deletes = .{ .files = if (appending) &.{} else if (root.file_directory != null and provider.native_delta != null and state.canUseDelta(provider, root.binding.snapshot_id)) provider.native_delta.?.prior_changed else previous_states, .directory = root.file_directory, .domain = root.generation.domain, .current = &plan };
            defer deletes.deinit();
            while (try deletes.next(a, store.*, cancellation)) |keys| {
                defer a.free(keys);
                const ids = try a.alloc([]const u8, keys.len / deletes.key_width);
                defer a.free(ids);
                for (ids, 0..) |*id, row| id.* = keys[row * deletes.key_width ..][0..deletes.key_width];
                try index.batchWithOptions(&.{}, ids, .{});
                if (deletes.key_width == 108) {
                    var source_keys: std.ArrayList([]u8) = .empty;
                    defer {
                        for (source_keys.items) |key| a.free(key);
                        source_keys.deinit(a);
                    }
                    {
                        var read = try index.beginReadTxn();
                        defer read.abort();
                        for (ids) |key| {
                            const record_key = try std.fmt.allocPrint(a, "lake-unit:{s}", .{key});
                            defer a.free(record_key);
                            const source_key = try @import("lake_enrichment_units.zig").sourceKeyFromRecord(a, key, try read.get(record_key));
                            defer a.free(source_key);
                            try source_keys.append(a, try std.fmt.allocPrint(a, "lake-unit:{s}", .{source_key}));
                        }
                    }
                    var transaction = try index.backendStore().beginBatch();
                    errdefer transaction.abort();
                    for (source_keys.items) |key| try transaction.delete(key);
                    for (ids) |key| {
                        const record_key = try std.fmt.allocPrint(a, "lake-unit:{s}", .{key});
                        defer a.free(record_key);
                        try transaction.delete(record_key);
                    }
                    try transaction.commit();
                }
            }
        }
        var input_provider = provider.*;
        input_provider.only_plan = &plan;
        input_provider.enrichment_columns = @import("lake_enrichment_units.zig").configured(producer.config, "template") != null;
        const rows = try input_provider.provider().open_with_cancellation_fn.?(input_provider.provider().ptr, a, binding, cancellation);
        defer rows.deinit(a);
        var input_remaining: u64 = 512 * 1024 * 1024;
        const input = if (provider.resume_work) |work| &work.native_input_left else &input_remaining;
        var batches: usize = 0;
        while (try rows.next(a)) |batch| {
            var page_arena = std.heap.ArenaAllocator.init(a);
            defer page_arena.deinit();
            const pa = page_arena.allocator();
            var writes: std.ArrayList(local.sparse_sparse.SparseWrite) = .empty;
            var unit_records: std.StringHashMapUnmanaged([]const u8) = .empty;
            for (0..batch.rowCount()) |ordinal| {
                const ref = batch.rowRef(ordinal);
                try cancellation.check();
                try provider.context.ensureActive();
                const page: local.sql_catalog.ColumnPage = .{ .batch = batch, .selection = &.{ordinal} };
                const row = try @import("lake_enrichment_units.zig").rowValue(pa, page);
                const parent_key = try plan.privateKey(pa, ref);
                for (try producer.units(pa, row)) |unit| {
                    const vector = (try producer.sparseUnit(pa, unit)) orelse continue;
                    try stores.chargeReadBudget(input, @as(u64, @intCast(vector.indices.len)) * 8 + 128);
                    const key = try @import("lake_enrichment_units.zig").identity(pa, parent_key, unit);
                    try tracker.append(ref, key);
                    if (unit.chunked) {
                        try unit_records.put(pa, key, try @import("lake_enrichment_units.zig").recordJson(pa, unit));
                        try unit_records.put(pa, try @import("lake_enrichment_units.zig").sourceKey(pa, parent_key, unit.source_ordinal), try @import("lake_enrichment_units.zig").sourceJson(pa, unit));
                    }
                    try writes.append(pa, .{ .doc_id = key, .vec = .{ .indices = vector.indices, .values = vector.values } });
                }
            }
            if (unit_records.count() != 0) {
                var transaction = try index.backendStore().beginBatch();
                errdefer transaction.abort();
                var records = unit_records.iterator();
                while (records.next()) |record| try transaction.put(try std.fmt.allocPrint(pa, "lake-unit:{s}", .{record.key_ptr.*}), record.value_ptr.*);
                try transaction.commit();
            }
            try appendFresh(&index, writes.items, seed == null);
            batches += 1;
            if (batches % 32 == 0) try compactNative(&index, budget.allocator(), &budget, provider, cancellation, path, 4);
        }
        if (seed == null) try index.finishBulkIngestSessionWithOptions(.{});
        try tracker.finish();
        try compactNative(&index, budget.allocator(), &budget, provider, cancellation, path, 16);
        finished = true;
        var checkpoint = try index.pinNativeCheckpoint();
        defer checkpoint.deinit();
        var output_remaining: u64 = 1024 * 1024 * 1024;
        const output_bytes: files.OutputBudget = .{ .generation = &output_remaining, .work = if (provider.resume_work) |work| &work.native_output_left else null };
        var navigation: @import("lake_index_native_directory.zig").Navigation = .{ .allocator = a };
        defer navigation.deinit();
        const staging_generation = if (seed != null and seed.?.generation.version == 2) null else try files.publishCheckpoint(a, ca, &checkpoint, store, cancellation, output_bytes, if (seed) |root| .{ .root = root.generation, .navigation = &navigation } else null);
        const generation = if (staging_generation == null)
            try files.publishExtentDelta(a, ca, checkpoint.storage, try files.checkpointExtents(ca, &baseline.?, ""), try files.checkpointExtents(ca, &checkpoint, ""), seed.?.generation, store, cancellation, output_bytes)
        else
            try @import("lake_index_native_directory.zig").persist(a, ca, store, staging_generation.?, if (seed) |root| root.generation else null, cancellation, if (provider.resume_work) |work| &work.native_output_left else null);
        const root: Root = .{ .binding = binding, .config_json = try std.json.Stringify.valueAlloc(ca, configs.object.get(wanted.name) orelse return error.InvalidTableIndexMetadata, .{}), .generation = generation, .recipe = recipe, .file_states = if (!plan.isPaged()) plan.files else &.{}, .file_directory = if (plan.isPaged()) try plan.publishDirectory(a, store, generation.domain, cancellation) else null };
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        if (bytes.len > files.max_root_bytes) return error.NativeLakeFilesTooLarge;
        if (provider.resume_work) |work| try stores.chargeReadBudget(&work.native_output_left, bytes.len);
        var uploaded = try store.putWithCancellation(bytes, cancellation);
        defer uploaded.deinit(store.allocator);
        const declaration: Declared = .{ .name = wanted.name, .binding = binding, .artifact = .{ .kind = .sparse_segment, .name = wanted.name, .metadata_version = metadata_version, .artifact_id = uploaded.artifact_id, .byte_len = uploaded.byte_len, .checksum = uploaded.checksum } };
        const encoded = try std.json.Stringify.valueAlloc(ca, declaration, .{});
        try result.append(out, try std.json.parseFromSliceLeaky(Declared, out, encoded, .{ .allocate = .alloc_always }));
    }
    return result.toOwnedSlice(out);
}

test "external lake fresh sparse fragment mutations keep quantization error bounded across updates" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-sparse-fresh-mutations");
    defer directory.cleanup();
    const path = try a.dupeSentinel(u8, directory.path(), 0);
    defer a.free(path);
    var index = try local.sparse_sparse.SparseIndex.open(a, path, .{ .backend = .lsm });
    defer index.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var ids: [257][]const u8 = undefined;
    var weights: [257][2]f32 = undefined;
    var initial: [257]local.sparse_sparse.SparseWrite = undefined;
    for (&ids, &weights, &initial, 0..) |*id, *weight, *write, row| {
        id.* = try std.fmt.allocPrint(scratch, "row-{d:0>4}-0", .{row});
        weight[0] = @floatFromInt(row + 1);
        weight[1] = @as(f32, @floatFromInt(258 - row)) * 0.5;
        write.* = .{ .doc_id = id.*, .vec = .{ .indices = &.{ 1, 7 }, .values = weight } };
    }
    try appendFresh(&index, &initial, true);
    var compactions: usize = 0;
    for (0..40) |cycle| {
        var removed: [16][]const u8 = undefined;
        var next: [16]local.sparse_sparse.SparseWrite = undefined;
        var values: [16][2]f32 = undefined;
        for (&removed, &next, &values, 0..) |*old, *write, *weight, lane| {
            const row = (cycle * 17 + lane * 13) % ids.len;
            old.* = ids[row];
            ids[row] = try std.fmt.allocPrint(scratch, "row-{d:0>4}-{d}", .{ row, cycle + 1 });
            weight[0] = @floatFromInt(row + 1);
            weight[1] = @as(f32, @floatFromInt(258 - row)) * 0.5;
            write.* = .{ .doc_id = ids[row], .vec = .{ .indices = &.{ 1, 7 }, .values = weight } };
        }
        try index.batchWithOptions(&.{}, &removed, .{});
        try appendFresh(&index, &next, false);
        if (try index.compactSegmentsWithOptions(a, .{ .min_segments = 4, .max_segments = 4, .size_tiered = true, .exact_forward_weights = true, .scratch = .{ .io = std.testing.io, .directory = directory.path() } })) compactions += 1;
        const hits = try index.search(a, &.{ .indices = &.{ 1, 7 }, .values = &.{ 2, 0.25 } }, 257);
        defer {
            for (hits) |hit| a.free(hit.doc_id);
            a.free(hits);
        }
        try std.testing.expectEqual(ids.len, hits.len);
        var seen: [257]bool = @splat(false);
        for (hits) |hit| {
            const row = try std.fmt.parseInt(usize, hit.doc_id[4..8], 10);
            try std.testing.expect(!seen[row]);
            seen[row] = true;
            try std.testing.expectEqualStrings(ids[row], hit.doc_id);
            const expected: f32 = @as(f32, @floatFromInt(2 * (row + 1))) + 0.125 * @as(f32, @floatFromInt(258 - row));
            try std.testing.expectApproxEqAbs(expected, hit.score, (2 * 258.0 + 0.25 * 129.0) / 255.0 + 0.001);
        }
    }
    try std.testing.expect(compactions >= 10);
}
