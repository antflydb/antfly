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

//! Remote dense generations use the native HBC posting authority and its
//! retained immutable checkpoint. No second posting codec or ranking implementation is involved.
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
pub const metadata_version: u16 = 5;
pub fn supportsMetadata(version: u16) bool {
    return version == 4 or version == metadata_version;
}
/// Reconstruct codec options on every build and reopen; rotation is required by
/// multi-bit quantization and must agree with the persisted projection.
pub fn applyQuantization(config: *local.storage_hbc_adapter.HBCConfig, value: std.json.Value) !void {
    if (value != .object) return error.InvalidIndexConfig;
    if (value.object.get("quantization_bits")) |bits| {
        if (bits != .integer or bits.integer < 1 or bits.integer > 8) return error.InvalidIndexConfig;
        config.quantization_bits = @intCast(bits.integer);
    }
    if (value.object.get("quantization_refine")) |refine| {
        if (refine != .string) return error.InvalidIndexConfig;
        config.quantization_refine = std.meta.stringToEnum(@TypeOf(config.quantization_refine), refine.string) orelse return error.InvalidIndexConfig;
    }
    if (value.object.get("use_random_ortho_trans")) |rotation| {
        if (rotation != .bool) return error.InvalidIndexConfig;
        config.use_random_ortho_trans = rotation.bool;
    }
    config.use_random_ortho_trans = config.use_random_ortho_trans or config.quantization_bits > 1;
}

pub fn applyQuantizationJson(a: A, config: *local.storage_hbc_adapter.HBCConfig, json: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    try applyQuantization(config, parsed.value);
}

pub const Root = struct {
    version: u16 = metadata_version,
    binding: local.serverless_segment_source_binding.Binding,
    config_json: []const u8,
    dims: u32,
    metric: []const u8,
    generation: files.Root,
    recipe: [32]u8 = @splat(0),
    file_states: []const state.File = &.{},
    file_directory: ?@import("../serverless/graph_segment/page_tree.zig").Ref = null,
};
pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .vector_segment or !supportsMetadata(ref.metadata_version) or ref.byte_len > files.max_root_bytes) return error.InvalidNativeLakeDenseRoot;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    if (root.version != ref.metadata_version or root.binding.sidecar_kind != .vector or root.binding.column_bindings.len != 1 or root.config_json.len > 256 * 1024 or root.dims == 0 or root.dims > 65536) return error.InvalidNativeLakeDenseRoot;
    try root.binding.validate();
    try root.generation.validate();
    try state.validate(root.file_states, root.generation.domain);
    if (root.file_directory) |directory| {
        try directory.validate();
        if (root.file_states.len != 0) return error.InvalidNativeLakeFileState;
    }
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeDenseRoot;
    if (!std.mem.eql(u8, &scope.domain, &root.generation.domain)) return error.InvalidNativeLakeDenseRoot;
    return root;
}
const BuildCancellation = struct {
    token: Cancellation,
    provider: *@import("lake_index_row_source.zig").Provider,

    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.token.check();
        try self.provider.context.ensureActive();
    }
};

fn vectorStoreOptions(storage: ?local.storage_lsm_backend.Storage) local.storage_lsm_backend.Options {
    return .{
        .storage = storage,
        .flush_threshold = 64 * 1024,
        .flush_threshold_bytes = 8 * 1024 * 1024,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .bulk_ingest_flush_threshold_bytes_multiplier = 1,
    };
}

/// Fresh snapshots publish sorted immutable source runs directly. The run
/// codec, checkpoints, point readers and later mutation overlay stay shared.
const FreshVectors = struct {
    arena: std.heap.ArenaAllocator,
    entries: std.ArrayList(local.storage_lsm_backend.TableEntry) = .empty,
    bytes: usize = 0,
    fn init(a: A) FreshVectors {
        return .{ .arena = .init(a) };
    }
    fn deinit(self: *FreshVectors) void {
        self.arena.deinit();
    }
    fn append(self: *FreshVectors, namespace: []const u8, key: []const u8, value: []const u8) !void {
        const a = self.arena.allocator();
        try self.entries.append(a, .{ .namespace_name = namespace, .key = try a.dupe(u8, key), .value = try a.dupe(u8, value) });
        self.bytes += key.len + value.len + @sizeOf(local.storage_lsm_backend.TableEntry);
    }
    fn less(_: void, lhs: local.storage_lsm_backend.TableEntry, rhs: local.storage_lsm_backend.TableEntry) bool {
        const order = std.mem.order(u8, lhs.namespace_name.?, rhs.namespace_name.?);
        return if (order == .eq) std.mem.order(u8, lhs.key, rhs.key) == .lt else order == .lt;
    }
    fn flush(self: *FreshVectors, backend: *local.storage_lsm_backend.Backend) !void {
        if (self.entries.items.len == 0) return;
        std.mem.sort(local.storage_lsm_backend.TableEntry, self.entries.items, {}, less);
        for (self.entries.items[1..], self.entries.items[0 .. self.entries.items.len - 1]) |entry, previous| {
            if (std.mem.eql(u8, entry.namespace_name.?, previous.namespace_name.?) and std.mem.eql(u8, entry.key, previous.key)) return error.NativeLakeVectorIdentityCollision;
        }
        try backend.ingestSortedTableEntries(self.entries.items);
        _ = self.arena.reset(.free_all);
        self.entries = .empty;
        self.bytes = 0;
    }
};

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
        selected_provider.paged_key_state = true;
        const provider = &selected_provider;
        if (!provider.wantsIndex(wanted.name)) continue;
        if (wanted.kind != .vector_segment) continue;
        try provider.context.ensureActive();
        var binding = wanted.binding;
        binding.index_config_hash = try std.fmt.allocPrint(ca, "native-dense-checkpoint-v2:{s}", .{binding.index_config_hash});
        const public_config = try std.json.Stringify.valueAlloc(ca, configs.object.get(wanted.name) orelse return error.InvalidTableIndexMetadata, .{});
        const configuration = configs.object.get(wanted.name).?;
        const whole_row = @import("lake_enrichment_units.zig").configured(configuration, "template") != null or !@import("lake_row_input_recipes.zig").configurationReusable(configuration);
        const recipe = try state.recipe(ca, table, public_config, if (whole_row) null else wanted.binding.column_bindings);
        var producer = try @import("lake_vector_enrichment.zig").Producer.init(a, wanted.name, wanted.build_spec.?.vector.vector_column, configs.object.get(wanted.name).?, provider.embedding_options);
        defer producer.deinit();
        producer.memo = provider.vector_memo;
        const prior = for (reusable) |declaration| {
            if (declaration.artifact.kind == .vector_segment and supportsMetadata(declaration.artifact.metadata_version) and std.mem.eql(u8, declaration.name, wanted.name) and rebuild.bindingsEqual(binding, declaration.binding)) break declaration;
        } else null;
        if (prior) |declaration| {
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (std.mem.eql(u8, &root.recipe, &recipe)) {
                try result.append(out, declaration);
                continue;
            }
        }
        const seed: ?Root = for (candidates) |declaration| {
            if (declaration.artifact.kind != .vector_segment or !supportsMetadata(declaration.artifact.metadata_version) or !std.mem.eql(u8, declaration.name, wanted.name)) continue;
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
        if (seed) |root| plan.enableRowDeltas(provider, root.binding.snapshot_id);
        var tracker = try state.Tracker.init(a, ca, &plan, store, cancellation);
        defer tracker.deinit();
        tracker.cumulative_bytes = if (provider.resume_work) |work| &work.native_keys_left else null;
        const io = provider.context.io orelse return error.UnsupportedSqlExecution;
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const path = try std.fmt.allocPrintSentinel(a, "/tmp/antfly-lake-dense-{s}", .{std.fmt.bytesToHex(random, .lower)}, 0);
        defer a.free(path);
        try std.Io.Dir.cwd().createDir(io, path, .fromMode(0o700));
        defer std.Io.Dir.cwd().deleteTree(io, path) catch {};
        const overlay = if (seed) |root| try files.Overlay.create(a, root.generation, store.*, path, provider.context, cancellation) else null;
        defer if (overlay) |candidate| candidate.release();
        const candidate_storage = if (overlay) |candidate| candidate.storage() else null;
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = 256 * 1024 * 1024 };
        const config = try local.api_table_index_config.parseIndexConfig(ca, wanted.name, public_config);
        const normalized = try std.json.parseFromSliceLeaky(std.json.Value, ca, config.config_json, .{});
        const dims = std.math.cast(u32, normalized.object.get("dims").?.integer) orelse return error.InvalidVectorDimensions;
        const metric = normalized.object.get("metric").?.string;
        const hbc = local.storage_hbc_adapter;
        var options: hbc.HBCConfig = .{ .dims = dims };
        options.metric = std.meta.stringToEnum(@TypeOf(options.metric), metric) orelse return error.InvalidIndexConfig;
        try applyQuantization(&options, normalized);
        var build_cancellation: BuildCancellation = .{ .token = cancellation, .provider = provider };
        var build_options: ?hbc.HBCIndex.SpoolBuildOptions = null;
        if (@import("lake_enrichment_units.zig").configured(configuration, "lake_bulk_build")) |algorithm| {
            if (algorithm != .string) return error.InvalidIndexConfig;
            const selected = std.meta.stringToEnum(@TypeOf((hbc.HBCIndex.SpoolBuildOptions{}).algorithm), algorithm.string) orelse return error.InvalidIndexConfig;
            if (selected == .emeans and options.metric != .l2_squared) return error.EmeansRequiresL2;
            if (seed == null) build_options = .{ .algorithm = selected, .check = BuildCancellation.check, .context = &build_cancellation };
        }
        const configured_bulk = @import("lake_enrichment_units.zig").configured(configuration, "lake_bulk_build") != null;
        var bulk_input_limit: u64 = 512 * 1024 * 1024;
        if (@import("lake_enrichment_units.zig").configured(configuration, "lake_bulk_build_max_input_bytes")) |limit| {
            if (limit != .integer or limit.integer <= 0) return error.InvalidIndexConfig;
            bulk_input_limit = @intCast(limit.integer);
        }
        if (configured_bulk) {
            tracker.output_bytes = bulk_input_limit;
            tracker.directory_bytes = bulk_input_limit;
        }
        if (build_options != null) {
            // Construction consumes each source vector once per partition.
            // Keep retained HBC caches within the shared builder allocator.
            const per_leaf = @as(usize, dims) * (@as(usize, options.leaf_size) + 8);
            options.max_cached_nodes = std.math.clamp((16 * 1024 * 1024) / per_leaf, 1, 256);
            options.max_cached_vectors = 0;
            options.max_cached_metadata = 0;
            options.pinned_tree_depth = 0;
        }
        const vector_path = try std.fmt.allocPrint(ca, "{s}/vectors", .{path});
        var vectors = try local.storage_lsm_backend.Backend.open(budget.allocator(), vector_path, vectorStoreOptions(candidate_storage));
        defer vectors.close();
        var fresh_vectors = FreshVectors.init(budget.allocator());
        defer fresh_vectors.deinit();
        var source_session = build_options != null;
        if (source_session) try vectors.beginBulkIngestSession();
        defer if (source_session) vectors.abortBulkIngestSession();
        var index = try hbc.HBCIndex.openWithLsmOptions(budget.allocator(), path, options, .{ .storage = candidate_storage });
        defer index.close();
        index.setIo(io);
        var loader: VectorLoader = .{ .backend = &vectors, .dims = dims };
        index.setExternalVectorLoader(&loader, VectorLoader.load);
        index.setExternalVectorScratchLoader(&loader, VectorLoader.loadInto);
        index.setExternalVectorBatchScratchLoader(&loader, VectorLoader.loadMany);

        // This is a private fresh candidate, never a live v1 authority migration.
        index.setExperimentalPostingAuthorityTransitionPermitted(true);
        if (seed != null) {
            try index.activateExperimentalPostingReads(0);
            try index.enableNativePostingMutationStore();
        }
        var baseline_vectors = if (seed != null) try vectors.pinNativeCheckpoint() else null;
        defer if (baseline_vectors) |*cut| cut.deinit();
        var baseline_postings = if (seed != null) (try index.nativeBackupGeneration(a, 0)) orelse return error.InvalidNativeLakeDenseRoot else null;
        defer if (baseline_postings) |*cut| cut.deinit();
        const spool_path = try std.fmt.allocPrint(ca, "{s}/bulk-spool", .{path});
        const spool_dir = if (build_options != null) dir: {
            try std.Io.Dir.cwd().createDir(io, spool_path, .fromMode(0o700));
            break :dir try std.Io.Dir.cwd().openDir(io, spool_path, .{});
        } else null;
        defer if (spool_dir) |dir| dir.close(io);
        var spool: hbc.HBCIndex.BulkSpool = .{ .dir = spool_dir orelse undefined, .io = io, .dims = dims };
        var spool_buffer: [64 * 1024]u8 = undefined;
        var spool_output = if (build_options != null) try spool.create(&spool_buffer) else null;
        defer if (spool_output) |*output| output.abort(spool);
        if (spool_output) |*output| try output.enableSample(budget.allocator(), 16 * 32, build_options.?.sample_bytes, index.rng.intN(std.math.maxInt(usize)));
        const transformed: []f32 = if (build_options != null) try budget.allocator().alloc(f32, dims) else &.{};
        defer if (build_options != null) budget.allocator().free(transformed);
        const incremental_session = seed == null and build_options == null;
        if (incremental_session) try index.beginBulkIngestSession();
        var finished = false;
        defer if (!finished and incremental_session) index.abortBulkIngestSession();
        var identities: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer identities.deinit(budget.allocator());
        if (seed) |root| {
            var deletes: state.Deletes = .{ .files = if (appending) &.{} else if (root.file_directory != null and provider.native_delta != null and state.canUseDelta(provider, root.binding.snapshot_id)) provider.native_delta.?.prior_changed else previous_states, .directory = root.file_directory, .domain = root.generation.domain, .current = &plan, .tracker = &tracker, .budget = if (configured_bulk) bulk_input_limit else 256 * 1024 * 1024 };
            defer deletes.deinit();
            while (try deletes.next(a, store.*, cancellation)) |keys| {
                defer a.free(keys);
                var ids: std.ArrayList(u64) = .empty;
                defer ids.deinit(a);
                for (0..keys.len / deletes.key_width) |row| try ids.append(a, vectorId(keys[row * deletes.key_width ..][0..deletes.key_width]));
                // Exact vectors remain available to native HBC deletion until its batch commits.
                try index.beginExperimentalPostingMutationCapture();
                errdefer index.cancelExperimentalPostingMutationCapture();
                try index.batchDelete(ids.items);
                try index.persistExperimentalPostingSidecarAtAppliedSequence(0, .{});
                var source_keys: std.ArrayList([]u8) = .empty;
                defer {
                    for (source_keys.items) |key| a.free(key);
                    source_keys.deinit(a);
                }
                if (deletes.key_width == 108) {
                    var read = try vectors.beginRead();
                    defer read.abort();
                    for (0..keys.len / deletes.key_width) |row| {
                        const key = keys[row * deletes.key_width ..][0..deletes.key_width];
                        try source_keys.append(a, try @import("lake_enrichment_units.zig").sourceKeyFromRecord(a, key, try read.get(.{ .name = "lake_units" }, key)));
                    }
                }
                var transaction = try vectors.beginBatch();
                errdefer transaction.abort();
                for (source_keys.items) |key| try transaction.delete(.{ .name = "lake_units" }, key);
                for (0..keys.len / deletes.key_width) |row| {
                    const key = keys[row * deletes.key_width ..][0..deletes.key_width];
                    try transaction.delete(.{ .name = "exact_vectors" }, key);
                    if (deletes.key_width == 108) try transaction.delete(.{ .name = "lake_units" }, key);
                }
                try transaction.commit();
            }
        }
        var input_provider = provider.*;
        input_provider.only_plan = &plan;
        input_provider.enrichment_columns = @import("lake_enrichment_units.zig").configured(producer.config, "template") != null;
        const rows = try input_provider.provider().open_with_cancellation_fn.?(input_provider.provider().ptr, a, binding, cancellation);
        defer rows.deinit(a);
        var input_remaining: u64 = bulk_input_limit;
        const input = if (provider.resume_work) |work| &work.native_input_left else &input_remaining;
        while (try rows.next(a)) |batch| {
            var page_arena = std.heap.ArenaAllocator.init(a);
            defer page_arena.deinit();
            const pa = page_arena.allocator();
            var writes: std.ArrayList(hbc.BatchInsertItem) = .empty;
            var unit_records: std.StringHashMapUnmanaged([]const u8) = .empty;
            const vector_input = producer.bind(batch);
            for (0..batch.rowCount()) |ordinal| {
                const ref = batch.rowRef(ordinal);
                try cancellation.check();
                try provider.context.ensureActive();
                const parent_key = try plan.privateKey(pa, ref);
                const prepared_input = try vector_input.prepare(pa, ordinal);
                for (prepared_input.units()) |unit| {
                    const vector = (try producer.denseUnit(pa, unit, dims)) orelse continue;
                    const key = try @import("lake_enrichment_units.zig").identity(pa, parent_key, unit);
                    try tracker.append(ref, key);
                    if (unit.chunked) {
                        try unit_records.put(pa, key, try @import("lake_enrichment_units.zig").recordJson(pa, unit));
                        try unit_records.put(pa, try @import("lake_enrichment_units.zig").sourceKey(pa, parent_key, unit.source_ordinal), try @import("lake_enrichment_units.zig").sourceJson(pa, unit));
                    }
                    const id = vectorId(key);
                    if (build_options == null and (try identities.getOrPut(budget.allocator(), id)).found_existing) return error.NativeLakeVectorIdentityCollision;
                    if (seed != null) if (try index.getMetadata(id)) |existing| {
                        defer index.alloc.free(existing);
                        return error.NativeLakeVectorIdentityCollision;
                    };
                    try stores.chargeReadBudget(input, @as(u64, dims) * 4 + 128);
                    try writes.append(pa, .{ .vector_id = id, .vector = vector, .metadata = key });
                }
            }
            if (build_options != null) {
                for (writes.items) |write| {
                    const encoded = try @import("antfly_vector").codec.encodePackedF32BytesAlloc(pa, write.vector);
                    try fresh_vectors.append("exact_vectors", write.metadata, encoded);
                    if (fresh_vectors.bytes >= 16 * 1024 * 1024) try fresh_vectors.flush(&vectors);
                }
                var records = unit_records.iterator();
                while (records.next()) |record| {
                    try fresh_vectors.append("lake_units", record.key_ptr.*, record.value_ptr.*);
                    if (fresh_vectors.bytes >= 16 * 1024 * 1024) try fresh_vectors.flush(&vectors);
                }
            } else {
                var transaction = try vectors.beginBatchWithOptions(.{ .mode = if (seed == null) .bulk_ingest else .default });
                errdefer transaction.abort();
                for (writes.items) |write| {
                    const encoded_vector = try @import("antfly_vector").codec.encodePackedF32BytesAlloc(pa, write.vector);
                    try transaction.put(.{ .name = "exact_vectors" }, write.metadata, encoded_vector);
                }
                var records = unit_records.iterator();
                while (records.next()) |record| try transaction.put(.{ .name = "lake_units" }, record.key_ptr.*, record.value_ptr.*);
                try transaction.commit();
            }
            if (spool_output) |*output| {
                for (writes.items) |write| {
                    const vector = index.transformVector(write.vector, transformed);
                    try output.append(write.vector_id, write.metadata, vector);
                }
            } else {
                if (seed != null) try index.beginExperimentalPostingMutationCapture();
                errdefer if (seed != null) index.cancelExperimentalPostingMutationCapture();
                try index.batchInsertWithMetadataOptions(writes.items, .{ .assume_absent_ids = true, .bulk_ingest = seed == null, .skip_vector_store = true });
                if (seed != null) try index.persistExperimentalPostingSidecarAtAppliedSequence(0, .{});
            }
        }
        if (spool_output) |*output| {
            const root = try output.finish(io);
            spool_output = null;
            try fresh_vectors.flush(&vectors);
            try vectors.finishBulkIngestSessionWithOptions(.{ .compact = false });
            source_session = false;
            index.bulkBuildSpool(&spool, root, build_options.?) catch |err| switch (err) {
                error.DuplicateVectorId => return error.NativeLakeVectorIdentityCollision,
                else => return err,
            };
        }
        if (incremental_session) try index.finishBulkIngestSessionWithOptions(.{});
        try tracker.finish();
        finished = true;
        // Keep small committed deltas; compact at a bounded WAL/depth boundary.
        var flatten = seed == null;
        if (seed != null) if (try index.nativeBackupGeneration(a, 0)) |captured| {
            var tip = captured;
            defer tip.deinit();
            var wal_bytes = tip.wal_committed_bytes;
            for (tip.sealed_wals[0..tip.sealed_wal_count]) |wal| wal_bytes +|= wal.committed_bytes;
            flatten = wal_bytes >= 32 * 1024 * 1024 or tip.segment_generations.len >= 8 or tip.sealed_wal_count >= 4;
        };
        try index.finalizeExperimentalPostingGenerationAtAppliedSequence(0, .{ .flatten = flatten, .make_authoritative = true, .check = BuildCancellation.check, .context = &build_cancellation });
        var checkpoint = (try index.nativeBackupGeneration(a, 0)) orelse return error.InvalidNativeLakeDenseRoot;
        defer checkpoint.deinit();
        var output_remaining: u64 = if (configured_bulk) try std.math.mul(u64, bulk_input_limit, 2) else 1024 * 1024 * 1024;
        const output_bytes: files.OutputBudget = .{ .generation = &output_remaining, .work = if (provider.resume_work) |work| &work.native_output_left else null };
        var navigation: @import("lake_index_native_directory.zig").Navigation = .{ .allocator = a };
        defer navigation.deinit();
        var vector_checkpoint = try vectors.pinNativeCheckpoint();
        defer vector_checkpoint.deinit();
        const generation = if (seed != null and seed.?.generation.version == 2) delta: {
            const before = try combineExtents(ca, try generationExtents(ca, &baseline_postings.?), try files.checkpointExtents(ca, &baseline_vectors.?, "vectors/"));
            const after = try combineExtents(ca, try generationExtents(ca, &checkpoint), try files.checkpointExtents(ca, &vector_checkpoint, "vectors/"));
            break :delta try files.publishExtentDelta(a, ca, checkpoint.storage, before, after, seed.?.generation, store, cancellation, output_bytes);
        } else full: {
            const posting_generation = try publishGeneration(a, ca, &checkpoint, store, cancellation, output_bytes, if (seed) |root| .{ .root = root.generation, .navigation = &navigation } else null);
            const vector_generation = try files.publishCheckpoint(a, ca, &vector_checkpoint, store, cancellation, output_bytes, if (seed) |root| .{ .root = root.generation, .prefix = "vectors/", .navigation = &navigation } else null);
            const staging_generation = try combineVectors(ca, posting_generation, vector_generation);
            break :full try @import("lake_index_native_directory.zig").persist(a, ca, store, staging_generation, if (seed) |root| root.generation else null, cancellation, if (provider.resume_work) |work| &work.native_output_left else null);
        };
        const root: Root = .{ .binding = binding, .config_json = config.config_json, .dims = dims, .metric = metric, .generation = generation, .recipe = recipe, .file_states = if (!plan.isPaged()) plan.files else &.{}, .file_directory = if (plan.isPaged()) try plan.publishDirectory(a, store, generation.domain, cancellation) else null };
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        if (bytes.len > files.max_root_bytes) return error.NativeLakeFilesTooLarge;
        if (provider.resume_work) |work| try stores.chargeReadBudget(&work.native_output_left, bytes.len);
        var uploaded = try store.putWithCancellation(bytes, cancellation);
        defer uploaded.deinit(store.allocator);
        const declaration: Declared = .{ .name = wanted.name, .binding = binding, .artifact = .{ .kind = .vector_segment, .name = wanted.name, .metadata_version = metadata_version, .artifact_id = uploaded.artifact_id, .byte_len = uploaded.byte_len, .checksum = uploaded.checksum } };
        const encoded = try std.json.Stringify.valueAlloc(ca, declaration, .{});
        try result.append(out, try std.json.parseFromSliceLeaky(Declared, out, encoded, .{ .allocate = .alloc_always }));
    }
    return result.toOwnedSlice(out);
}

/// Collision detection at build and reverse metadata verification at lookup
/// make compact native IDs exact while keeping predicate mapping constant time.
pub fn vectorId(key: []const u8) u64 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-vector-id-v1");
    hash.update(key);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return std.mem.readInt(u64, digest[0..8], .little);
}
fn combineExtents(out: A, postings: []const files.Extent, vectors: []const files.Extent) ![]files.Extent {
    const result = try out.alloc(files.Extent, postings.len + vectors.len);
    @memcpy(result[0..postings.len], postings);
    @memcpy(result[postings.len..], vectors);
    files.sortExtents(result);
    return result;
}
fn generationExtents(out: A, checkpoint: *const local.storage_hbc_adapter.HBCIndex.NativeBackupGeneration) ![]files.Extent {
    const posting = local.storage_posting_segment_store;
    const result = try out.alloc(files.Extent, 3 + checkpoint.segment_generations.len + checkpoint.sealed_wal_count);
    result[0] = .{ .path = "posting-segments/CURRENT", .bytes = checkpoint.current_bytes.len, .data = checkpoint.current_bytes };
    result[1] = .{ .path = "posting-segments/" ++ posting.authority_name, .bytes = posting.authority_value.len, .data = posting.authority_value };
    var n: usize = 2;
    for (checkpoint.segment_generations) |generation| {
        const physical = try posting.checkpointSegmentPathAlloc(out, checkpoint.root_dir, generation);
        result[n] = .{ .path = try std.fmt.allocPrint(out, "posting-segments/segment-{d}.afps", .{generation}), .physical = physical, .bytes = try checkpoint.storage.fileSize(physical) };
        n += 1;
    }
    for (checkpoint.sealed_wals[0..checkpoint.sealed_wal_count]) |extent| {
        result[n] = .{ .path = try std.fmt.allocPrint(out, "posting-segments/wal-{d}.afpw", .{extent.generation}), .physical = try posting.checkpointWalPathAlloc(out, checkpoint.root_dir, extent.generation), .bytes = extent.committed_bytes, .append_only = true };
        n += 1;
    }
    result[n] = .{ .path = try std.fmt.allocPrint(out, "posting-segments/wal-{d}.afpw", .{checkpoint.wal_generation}), .physical = try posting.checkpointWalPathAlloc(out, checkpoint.root_dir, checkpoint.wal_generation), .bytes = checkpoint.wal_committed_bytes, .append_only = true };
    files.sortExtents(result);
    return result;
}

pub fn publishGeneration(a: A, out: A, checkpoint: *const local.storage_hbc_adapter.HBCIndex.NativeBackupGeneration, store: *stores.ArtifactStore, cancellation: Cancellation, budget: anytype, reuse: ?files.Reuse) !files.Root {
    var result: std.ArrayList(files.File) = .empty;
    const posting = local.storage_posting_segment_store;
    try result.append(out, try files.publishBytes(out, "posting-segments/CURRENT", checkpoint.current_bytes, store, cancellation, budget, reuse));
    try result.append(out, try files.publishBytes(out, "posting-segments/" ++ posting.authority_name, posting.authority_value, store, cancellation, budget, reuse));
    for (checkpoint.segment_generations) |generation| {
        const physical = try posting.checkpointSegmentPathAlloc(a, checkpoint.root_dir, generation);
        defer a.free(physical);
        const relative = try std.fmt.allocPrint(a, "posting-segments/segment-{d}.afps", .{generation});
        defer a.free(relative);
        try result.append(out, try files.publishPrefix(a, out, checkpoint.storage, physical, relative, try checkpoint.storage.fileSize(physical), store, cancellation, budget, reuse));
    }
    for (checkpoint.sealed_wals[0..checkpoint.sealed_wal_count]) |extent| {
        const physical = try posting.checkpointWalPathAlloc(a, checkpoint.root_dir, extent.generation);
        defer a.free(physical);
        const relative = try std.fmt.allocPrint(a, "posting-segments/wal-{d}.afpw", .{extent.generation});
        defer a.free(relative);
        try result.append(out, try files.publishPrefix(a, out, checkpoint.storage, physical, relative, extent.committed_bytes, store, cancellation, budget, reuse));
    }
    const physical = try posting.checkpointWalPathAlloc(a, checkpoint.root_dir, checkpoint.wal_generation);
    defer a.free(physical);
    const relative = try std.fmt.allocPrint(a, "posting-segments/wal-{d}.afpw", .{checkpoint.wal_generation});
    defer a.free(relative);
    try result.append(out, try files.publishPrefix(a, out, checkpoint.storage, physical, relative, checkpoint.wal_committed_bytes, store, cancellation, budget, reuse));
    std.mem.sort(files.File, result.items, {}, struct {
        fn less(_: void, left: files.File, right: files.File) bool {
            return std.mem.order(u8, left.path, right.path) == .lt;
        }
    }.less);
    const root: files.Root = .{ .domain = store.upload_scope.?.domain, .files = try result.toOwnedSlice(out) };
    try root.validate();
    return root;
}

pub fn combineVectors(out: A, postings: files.Root, vectors: files.Root) !files.Root {
    if (!std.mem.eql(u8, &postings.domain, &vectors.domain)) return error.InvalidNativeLakeDenseRoot;
    const entries = try out.alloc(files.File, postings.files.len + vectors.files.len);
    @memcpy(entries[0..postings.files.len], postings.files);
    for (vectors.files, entries[postings.files.len..]) |source, *target| {
        target.* = source;
        target.path = try std.fmt.allocPrint(out, "vectors/{s}", .{source.path});
    }
    const root: files.Root = .{ .domain = postings.domain, .files = entries };
    try root.validate();
    return root;
}
/// Exact values are a separately pinned native LSM source plane. HBC owns
/// candidate search; canonical packed float32 values retain reranking precision.
pub const VectorLoader = struct {
    backend: *local.storage_lsm_backend.Backend,
    dims: u32,
    context: local.serverless_query_lake_read_context.Context = .{},
    pub fn load(raw: *anyopaque, a: A, _: u64, metadata: []const u8) ![]f32 {
        const self: *VectorLoader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        var transaction = try self.backend.beginRead();
        defer transaction.abort();
        const bytes = try transaction.get(.{ .name = "exact_vectors" }, metadata);
        if (bytes.len != @as(usize, self.dims) * 4) return error.InvalidNativeLakeDenseRoot;
        return @import("antfly_vector").codec.decodePackedF32BytesAlloc(a, bytes);
    }
    pub fn loadInto(raw: *anyopaque, _: u64, metadata: []const u8, scratch: []f32) ![]const f32 {
        const self: *VectorLoader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        if (scratch.len < self.dims) return error.InvalidVectorDimensions;
        var transaction = try self.backend.beginRead();
        defer transaction.abort();
        const bytes = try transaction.get(.{ .name = "exact_vectors" }, metadata);
        try @import("antfly_vector").codec.decodePackedF32BytesInto(scratch[0..self.dims], bytes);
        try self.context.ensureActive();
        return scratch[0..self.dims];
    }
    pub fn loadMany(raw: *anyopaque, ids: []const u64, metadata: []const ?[]const u8, views: [][]const f32, scratch: []f32, dims: usize) !void {
        const self: *VectorLoader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        if (dims != self.dims or ids.len != metadata.len or views.len != ids.len or ids.len > scratch.len / dims) return error.InvalidVectorDimensions;
        const Slot = struct { key: []const u8, position: usize };
        const a = self.backend.allocator;
        const slots = try a.alloc(Slot, ids.len);
        defer a.free(slots);
        for (slots, metadata, 0..) |*slot, key, position| slot.* = .{ .key = key orelse return error.MissingCommittedVectorPayload, .position = position };
        std.mem.sort(Slot, slots, {}, struct {
            fn less(_: void, left: Slot, right: Slot) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        }.less);
        const keys = try a.alloc([]const u8, ids.len);
        defer a.free(keys);
        const values = try a.alloc(?[]const u8, ids.len);
        defer a.free(values);
        for (slots, keys) |slot, *key| key.* = slot.key;
        var transaction = try self.backend.beginRead();
        defer transaction.abort();
        try transaction.getManySorted(.{ .name = "exact_vectors" }, keys, values);
        for (slots, values) |slot, value| {
            try self.context.ensureActive();
            const vector = scratch[slot.position * dims ..][0..dims];
            try @import("antfly_vector").codec.decodePackedF32BytesInto(vector, value orelse return error.MissingCommittedVectorPayload);
            views[slot.position] = vector;
        }
    }
};

test "external lake dense roots reject obsolete metadata versions" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("obsolete-lake_index_native_dense");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    // Missing objects must not be read for unsupported root formats.
    for ([_]u16{ 2, 3 }) |version| try std.testing.expectError(error.InvalidNativeLakeDenseRoot, loadRoot(a, fs.artifactStore(), .{ .kind = .vector_segment, .metadata_version = @intCast(version), .artifact_id = "missing", .byte_len = 1, .checksum = "missing" }, .none, null));
}

test "external lake execution offline dense source flushes by bytes within its allocation quota" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("offline-dense-source");
    defer directory.cleanup();
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
    const path = try std.fmt.allocPrint(a, "{s}/vectors", .{directory.path()});
    defer a.free(path);
    const dims = 768;
    const count = 32768;
    const vector: [dims]f32 = @splat(1.25);
    const encoded = try @import("antfly_vector").codec.encodePackedF32BytesAlloc(a, &vector);
    defer a.free(encoded);
    {
        var backend = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, vectorStoreOptions(null));
        defer backend.close();
        var offset: usize = 0;
        while (offset < count) : (offset += 128) {
            var batch = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
            errdefer batch.abort();
            for (offset..@min(offset + 128, count)) |row| {
                var key: [32]u8 = undefined;
                try batch.put(.{ .name = "exact_vectors" }, try std.fmt.bufPrint(&key, "row-{d:0>8}", .{row}), encoded);
            }
            try batch.commit();
        }
        try backend.flushBufferedWritesWithOptions(.{ .compact = false });
        var checkpoint = try backend.pinNativeCheckpoint();
        defer checkpoint.deinit();
        try std.testing.expect(@as(u64, count) * dims * 4 > budget.limit);
        try std.testing.expect(budget.peak <= budget.limit);
    }
    var reopened = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, vectorStoreOptions(null));
    defer reopened.close();
    var read = try reopened.beginRead();
    defer read.abort();
    for ([_]usize{ 0, count / 2, count - 1 }) |row| {
        var key: [32]u8 = undefined;
        try std.testing.expectEqualSlices(u8, encoded, try read.get(.{ .name = "exact_vectors" }, try std.fmt.bufPrint(&key, "row-{d:0>8}", .{row})));
    }
}

test "external lake execution immutable exact vectors bypass WAL within source quota" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("offline-immutable-vectors");
    defer directory.cleanup();
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
    const path = try std.fmt.allocPrint(a, "{s}/vectors", .{directory.path()});
    defer a.free(path);
    const dims = 768;
    const count = 32768;
    const vector: [dims]f32 = @splat(1.25);
    const encoded = try @import("antfly_vector").codec.encodePackedF32BytesAlloc(a, &vector);
    defer a.free(encoded);
    {
        var backend = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, vectorStoreOptions(null));
        defer backend.close();
        try backend.beginBulkIngestSession();
        var fresh = FreshVectors.init(budget.allocator());
        defer fresh.deinit();
        for (0..count) |row| {
            var key: [32]u8 = undefined;
            try fresh.append("exact_vectors", try std.fmt.bufPrint(&key, "row-{d:0>8}", .{row}), encoded);
            if (fresh.bytes >= 16 * 1024 * 1024) try fresh.flush(&backend);
        }
        try fresh.flush(&backend);
        try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
        const stats = backend.snapshotWriteStats();
        try std.testing.expect(stats.sorted_ingest_runs > 0);
        try std.testing.expectEqual(@as(u64, 0), stats.wal_append_bytes);
        try std.testing.expectEqual(@as(u64, 0), stats.flush_input_entries);
        try std.testing.expect(@as(u64, count) * dims * 4 > budget.limit);
        try std.testing.expect(budget.peak <= budget.limit);
    }
    var reopened = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, vectorStoreOptions(null));
    defer reopened.close();
    var read = try reopened.beginRead();
    defer read.abort();
    for ([_]usize{ 0, count / 2, count - 1 }) |row| {
        var key: [32]u8 = undefined;
        try std.testing.expectEqualSlices(u8, encoded, try read.get(.{ .name = "exact_vectors" }, try std.fmt.bufPrint(&key, "row-{d:0>8}", .{row})));
    }
}
