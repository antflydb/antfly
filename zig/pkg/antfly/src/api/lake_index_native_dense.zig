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
pub const metadata_version: u16 = 2;
pub const Root = struct {
    version: u16 = metadata_version,
    binding: local.serverless_segment_source_binding.Binding,
    config_json: []const u8,
    dims: u32,
    metric: []const u8,
    generation: files.Root,
    recipe: [32]u8 = @splat(0),
    file_states: []const state.File = &.{},
};
pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .vector_segment or ref.metadata_version != metadata_version or ref.byte_len > files.max_root_bytes) return error.InvalidNativeLakeDenseRoot;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    if (root.version != metadata_version or root.binding.sidecar_kind != .vector or root.binding.column_bindings.len != 1 or root.config_json.len > 256 * 1024 or root.dims == 0 or root.dims > 65536) return error.InvalidNativeLakeDenseRoot;
    try root.binding.validate();
    try root.generation.validate();
    try state.validate(root.file_states, root.generation.domain);
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeDenseRoot;
    if (!std.mem.eql(u8, &scope.domain, &root.generation.domain)) return error.InvalidNativeLakeDenseRoot;
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
        if (wanted.kind != .vector_segment) continue;
        try provider.context.ensureActive();
        var binding = wanted.binding;
        binding.index_config_hash = try std.fmt.allocPrint(ca, "native-dense-checkpoint-v2:{s}", .{binding.index_config_hash});
        const public_config = try std.json.Stringify.valueAlloc(ca, configs.object.get(wanted.name) orelse return error.InvalidTableIndexMetadata, .{});
        const recipe = state.recipe(table, public_config);
        var producer = try @import("lake_vector_enrichment.zig").Producer.init(a, wanted.name, wanted.build_spec.?.vector.vector_column, configs.object.get(wanted.name).?, provider.embedding_options);
        defer producer.deinit();
        producer.memo = provider.vector_memo;
        const prior = for (reusable) |declaration| {
            if (declaration.artifact.kind == .vector_segment and declaration.artifact.metadata_version == metadata_version and std.mem.eql(u8, declaration.name, wanted.name) and rebuild.bindingsEqual(binding, declaration.binding)) break declaration;
        } else null;
        if (prior) |declaration| {
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (std.mem.eql(u8, &root.recipe, &recipe)) {
                try result.append(out, declaration);
                continue;
            }
        }
        const seed: ?Root = for (candidates) |declaration| {
            if (declaration.artifact.kind != .vector_segment or declaration.artifact.metadata_version != metadata_version or !std.mem.eql(u8, declaration.name, wanted.name)) continue;
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
        const vector_path = try std.fmt.allocPrint(ca, "{s}/vectors", .{path});
        var vectors = try local.storage_lsm_backend.Backend.open(budget.allocator(), vector_path, .{ .storage = candidate_storage, .flush_threshold = 8 * 1024 * 1024 });
        defer vectors.close();
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
        if (seed == null) try index.beginBulkIngestSession();
        var finished = false;
        defer if (!finished and seed == null) index.abortBulkIngestSession();
        var identities: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer identities.deinit(budget.allocator());
        if (seed) |root| {
            var deletes: state.Deletes = .{ .files = root.file_states, .current = &plan };
            while (try deletes.next(a, store.*, cancellation)) |keys| {
                defer a.free(keys);
                var ids: std.ArrayList(u64) = .empty;
                defer ids.deinit(a);
                for (0..keys.len / 96) |row| try ids.append(a, vectorId(keys[row * 96 ..][0..96]));
                // Exact vectors remain available to native HBC deletion until its batch commits.
                try index.beginExperimentalPostingMutationCapture();
                errdefer index.cancelExperimentalPostingMutationCapture();
                try index.batchDelete(ids.items);
                try index.persistExperimentalPostingSidecarAtAppliedSequence(0, .{});
                var transaction = try vectors.beginBatch();
                errdefer transaction.abort();
                for (0..keys.len / 96) |row| try transaction.delete(.{ .name = "exact_vectors" }, keys[row * 96 ..][0..96]);
                try transaction.commit();
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
            var writes: std.ArrayList(hbc.BatchInsertItem) = .empty;
            for (batch.row_refs, 0..) |ref, ordinal| {
                try cancellation.check();
                try provider.context.ensureActive();
                const page: local.sql_catalog.ColumnPage = .{ .batch = batch, .selection = &.{ordinal} };
                const value = (try page.cell(pa, 0, wanted.build_spec.?.vector.vector_column)).value;
                const vector = (try producer.dense(pa, value, dims)) orelse continue;
                const key = try plan.privateKey(pa, ref);
                try tracker.append(ref, key);
                const id = vectorId(key);
                if ((try identities.getOrPut(budget.allocator(), id)).found_existing) return error.NativeLakeVectorIdentityCollision;
                if (seed != null) if (try index.getMetadata(id)) |existing| {
                    defer index.alloc.free(existing);
                    return error.NativeLakeVectorIdentityCollision;
                };
                try stores.chargeReadBudget(&input, @as(u64, dims) * 4 + 128);
                try writes.append(pa, .{ .vector_id = id, .vector = vector, .metadata = key });
            }
            var transaction = try vectors.beginBatchWithOptions(.{ .mode = if (seed == null) .bulk_ingest else .default });
            errdefer transaction.abort();
            for (writes.items) |write| {
                const encoded_vector = try @import("antfly_vector").codec.encodePackedF32BytesAlloc(pa, write.vector);
                try transaction.put(.{ .name = "exact_vectors" }, write.metadata, encoded_vector);
            }
            try transaction.commit();
            if (seed != null) try index.beginExperimentalPostingMutationCapture();
            errdefer if (seed != null) index.cancelExperimentalPostingMutationCapture();
            try index.batchInsertWithMetadataOptions(writes.items, .{ .assume_absent_ids = true, .bulk_ingest = seed == null, .skip_vector_store = true });
            if (seed != null) try index.persistExperimentalPostingSidecarAtAppliedSequence(0, .{});
        }
        if (seed == null) try index.finishBulkIngestSessionWithOptions(.{});
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
        try index.finalizeExperimentalPostingGenerationAtAppliedSequence(0, .{ .flatten = flatten, .make_authoritative = true });
        var checkpoint = (try index.nativeBackupGeneration(a, 0)) orelse return error.InvalidNativeLakeDenseRoot;
        defer checkpoint.deinit();
        var output_bytes: u64 = 1024 * 1024 * 1024;
        const posting_generation = try publishGeneration(a, ca, &checkpoint, store, cancellation, &output_bytes, if (seed) |root| .{ .root = root.generation } else null);
        var vector_checkpoint = try vectors.pinNativeCheckpoint();
        defer vector_checkpoint.deinit();
        const vector_generation = try files.publishCheckpoint(a, ca, &vector_checkpoint, store, cancellation, &output_bytes, if (seed) |root| .{ .root = root.generation, .prefix = "vectors/" } else null);
        const generation = try combineVectors(ca, posting_generation, vector_generation);
        const root: Root = .{ .binding = binding, .config_json = config.config_json, .dims = dims, .metric = metric, .generation = generation, .recipe = recipe, .file_states = plan.files };
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        if (bytes.len > files.max_root_bytes) return error.NativeLakeFilesTooLarge;
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
pub fn publishGeneration(a: A, out: A, checkpoint: *const local.storage_hbc_adapter.HBCIndex.NativeBackupGeneration, store: *stores.ArtifactStore, cancellation: Cancellation, budget: *u64, reuse: ?files.Reuse) !files.Root {
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
