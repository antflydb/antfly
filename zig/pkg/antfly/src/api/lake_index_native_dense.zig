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
const probe = @import("lake_execution_probe_hooks.zig");
const local = @import("antfly_local_sources");
const state = @import("lake_index_native_state.zig");
const files = @import("lake_index_native_files.zig");
const bulk_work = @import("lake_bulk_work.zig");
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

// v4 lake writers ignored quantization recipe fields and emitted the default
// one-bit, unrotated codec. Preserve that actual representation on reopen.
pub fn applyStoredQuantization(a: A, config: *local.storage_hbc_adapter.HBCConfig, root: Root) !void {
    if (root.version >= 5) return applyQuantizationJson(a, config, root.config_json);
    config.quantization_bits = 1;
    config.quantization_refine = .near_cutoff;
    config.use_random_ortho_trans = false;
}
pub fn canReuseQuantization(version: u16, desired: local.storage_hbc_adapter.HBCConfig) bool {
    return version >= 5 or (desired.quantization_bits == 1 and !desired.use_random_ortho_trans and desired.quantization_refine == .near_cutoff);
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
    fn checkStorage(raw: *const anyopaque) !void {
        const self: *const BuildCancellation = @ptrCast(@alignCast(raw));
        try self.token.check();
        try self.provider.context.ensureActive();
    }
};

pub fn vectorStoreOptions(storage: ?local.storage_lsm_backend.Storage) local.storage_lsm_backend.Options {
    return .{
        .storage = storage,
        .flush_threshold = 64 * 1024,
        .flush_threshold_bytes = 8 * 1024 * 1024,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .bulk_ingest_flush_threshold_bytes_multiplier = 1,
    };
}

/// Private fresh source runs are immutable after their receipt is pinned.
/// Ordinary close-time L0 pressure must not rewrite those authenticated files.
/// Incremental/serving backends retain normal maintenance policy on reopen.
pub const bulk_run_entry_limit: usize = 65536;

pub fn bulkVectorStoreOptions(storage: ?local.storage_lsm_backend.Storage) local.storage_lsm_backend.Options {
    var options = vectorStoreOptions(storage);
    options.retain_bloom_filters = false;
    // This unpublished source has no serving grace period. Version pins remain
    // authoritative; reclaim superseded ingest files after the durable seal.
    options.obsolete_retention_ns = 0;
    // Bound each decoded input index as well as the merge's stream count.
    // The byte limit still splits unusually wide rows earlier.
    options.max_run_file_entries = bulk_run_entry_limit;
    options.compact_threshold_runs = std.math.maxInt(usize);
    options.l0_soft_limit_runs = std.math.maxInt(usize);
    options.l0_hard_limit_runs = std.math.maxInt(usize);
    // Delay size-tier merging during ingest, then seal the complete window
    // once before its receipt. This yields bounded-size, disjoint sorted runs
    // rather than retaining one overlapping run for every 16 MiB input batch.
    options.bulk_ingest_tiered_l0_fan_in = std.math.maxInt(usize);
    return options;
}

/// Fresh snapshots publish sorted immutable source runs directly. The run
/// codec, checkpoints, point readers and later mutation overlay stay shared.
pub const FreshVectors = struct {
    arena: std.heap.ArenaAllocator,
    entries: std.ArrayList(local.storage_lsm_backend.TableEntry) = .empty,
    bytes: usize = 0,
    pub fn init(a: A) FreshVectors {
        return .{ .arena = .init(a) };
    }
    pub fn deinit(self: *FreshVectors) void {
        self.arena.deinit();
    }
    pub fn append(self: *FreshVectors, namespace: []const u8, key: []const u8, value: []const u8) !void {
        const a = self.arena.allocator();
        try self.entries.append(a, .{ .namespace_name = namespace, .key = try a.dupe(u8, key), .value = try a.dupe(u8, value) });
        self.bytes += key.len + value.len + @sizeOf(local.storage_lsm_backend.TableEntry);
    }
    fn less(_: void, lhs: local.storage_lsm_backend.TableEntry, rhs: local.storage_lsm_backend.TableEntry) bool {
        const order = std.mem.order(u8, lhs.namespace_name.?, rhs.namespace_name.?);
        return if (order == .eq) std.mem.order(u8, lhs.key, rhs.key) == .lt else order == .lt;
    }
    pub fn flush(self: *FreshVectors, backend: *local.storage_lsm_backend.Backend) !void {
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
        const config = try local.api_table_index_config.parseIndexConfig(ca, wanted.name, public_config);
        const normalized = try std.json.parseFromSliceLeaky(std.json.Value, ca, config.config_json, .{});
        const dims = std.math.cast(u32, normalized.object.get("dims").?.integer) orelse return error.InvalidVectorDimensions;
        const metric = normalized.object.get("metric").?.string;
        const hbc = local.storage_hbc_adapter;
        var options: hbc.HBCConfig = .{ .dims = dims };
        options.metric = std.meta.stringToEnum(@TypeOf(options.metric), metric) orelse return error.InvalidIndexConfig;
        try applyQuantization(&options, normalized);
        const recipe = try state.recipe(ca, table, public_config, if (whole_row) null else wanted.binding.column_bindings);
        var producer = try @import("lake_vector_enrichment.zig").Producer.init(a, wanted.name, wanted.build_spec.?.vector.vector_column, configs.object.get(wanted.name).?, provider.embedding_options);
        defer producer.deinit();
        producer.memo = provider.vector_memo;
        const prior = for (reusable) |declaration| {
            if (declaration.artifact.kind == .vector_segment and supportsMetadata(declaration.artifact.metadata_version) and std.mem.eql(u8, declaration.name, wanted.name) and rebuild.bindingsEqual(binding, declaration.binding)) break declaration;
        } else null;
        if (prior) |declaration| {
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (canReuseQuantization(root.version, options) and std.mem.eql(u8, &root.recipe, &recipe)) {
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
            if (!canReuseQuantization(root.version, options)) continue;
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
        const local_stage = if (provider.bulk_stage) |stage| if (stage.finished != null and std.mem.eql(u8, stage.name, wanted.name)) stage else null else null;
        const path = if (local_stage) |stage| try a.dupeSentinel(u8, stage.path, 0) else try std.fmt.allocPrintSentinel(a, "/tmp/antfly-lake-dense-{s}", .{std.fmt.bytesToHex(random, .lower)}, 0);
        defer a.free(path);
        if (local_stage == null) try std.Io.Dir.cwd().createDir(io, path, .fromMode(0o700));
        defer std.Io.Dir.cwd().deleteTree(io, path) catch {};
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = 256 * 1024 * 1024 };
        var build_cancellation: BuildCancellation = .{ .token = cancellation, .provider = provider };
        var build_options: ?hbc.HBCIndex.SpoolBuildOptions = null;
        if (@import("lake_enrichment_units.zig").configured(configuration, "lake_bulk_build")) |algorithm| {
            if (algorithm != .string) return error.InvalidIndexConfig;
            const selected = std.meta.stringToEnum(@TypeOf((hbc.HBCIndex.SpoolBuildOptions{}).algorithm), algorithm.string) orelse return error.InvalidIndexConfig;
            if (selected == .emeans and options.metric != .l2_squared) return error.EmeansRequiresL2;
            if (seed == null) build_options = .{ .algorithm = selected, .prebind_parents = provider.bulk_prebind_parents, .compact_partition_bytes = if (provider.bulk_compact_partitions) 32 * 1024 * 1024 else 0, .check = BuildCancellation.check, .context = &build_cancellation };
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
        var source_work: ?bulk_work.Root = if (local_stage) |stage| stage.finished else null;
        if (build_options != null) for (provider.construction) |work| {
            if (!std.mem.eql(u8, work.name, wanted.name)) continue;
            const root = bulk_work.load(ca, store.*, work.manifest, cancellation) catch |err| switch (err) {
                error.FileNotFound, error.NotFound, error.ObjectNotFound, error.LakeIndexArtifactUnavailable => continue,
                else => return err,
            };
            if (root.dims == dims and root.input_allowance == bulk_input_limit and std.mem.eql(u8, root.algorithm, @tagName(build_options.?.algorithm)) and
                std.mem.eql(u8, &root.recipe, &recipe) and rebuild.bindingsEqual(root.binding, binding) and
                std.mem.eql(u8, &root.generation.domain, &store.upload_scope.?.domain)) source_work = root;
        };
        const resuming_source = source_work != null;
        const overlay = if (local_stage != null) null else if (seed) |root| try files.Overlay.create(a, root.generation, store.*, path, provider.context, cancellation) else if (source_work) |root| try files.Overlay.create(a, root.generation, store.*, path, provider.context, cancellation) else null;
        defer if (overlay) |candidate| candidate.release();
        const candidate_storage = if (overlay) |candidate| candidate.storage() else null;
        if (build_options != null) {
            // Construction consumes each source vector once per partition.
            // Keep retained HBC caches within the shared builder allocator.
            const per_leaf = @as(usize, dims) * (@as(usize, options.leaf_size) + 8) * @sizeOf(f32);
            options.max_cached_nodes = std.math.clamp((16 * 1024 * 1024) / per_leaf, 1, 256);
            options.max_cached_vectors = 0;
            options.max_cached_metadata = 0;
            options.pinned_tree_depth = 0;
        }
        var projection_cache = if (build_options != null) try local.storage_lsm_backend.Cache.initFallible(budget.allocator(), 16 * 1024 * 1024) else null;
        defer if (projection_cache) |*cache| cache.deinit();
        const cache = if (projection_cache) |*value| value else null;
        const vector_path = try std.fmt.allocPrint(ca, "{s}/vectors", .{path});
        var vector_options = if (build_options != null) bulkVectorStoreOptions(candidate_storage) else vectorStoreOptions(candidate_storage);
        if (build_options != null) vector_options.bulk_cancellation = .{ .ptr = &build_cancellation, .check_fn = BuildCancellation.checkStorage };
        vector_options.cache = cache;
        var vectors = try local.storage_lsm_backend.Backend.open(budget.allocator(), vector_path, vector_options);
        defer vectors.close();
        var fresh_vectors = FreshVectors.init(budget.allocator());
        defer fresh_vectors.deinit();
        var source_session = build_options != null and !resuming_source;
        if (source_session) try vectors.beginBulkIngestSession();
        defer if (source_session) vectors.abortBulkIngestSession();
        // This projection is an unpublished candidate, consumed by the native
        // generation writer. Preserve its immutable batches without a final
        // bulk-window merge or close-time pressure compaction.
        var projection_options: local.storage_lsm_backend.Options = .{ .storage = candidate_storage, .cache = cache };
        if (build_options != null) {
            projection_options.bulk_cancellation = .{ .ptr = &build_cancellation, .check_fn = BuildCancellation.checkStorage };
            projection_options.retain_bloom_filters = false;
            projection_options.max_run_file_entries = bulk_run_entry_limit;
            projection_options.bulk_ingest_tiered_l0_fan_in = 0;
            projection_options.compact_threshold_runs = std.math.maxInt(usize);
            projection_options.l0_soft_limit_runs = std.math.maxInt(usize);
            projection_options.l0_hard_limit_runs = std.math.maxInt(usize);
        }
        var index = try hbc.HBCIndex.openWithLsmOptions(budget.allocator(), path, options, .{ .backend_options = projection_options, .storage = candidate_storage, .cache = cache });
        defer index.close();
        errdefer if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) {
            const owned_indexes = switch (index.env_owner) {
                .lsm => |handle| handle.backend.run_index_cache.items.len,
                .native => 0,
            };
            std.debug.print("PARQUET_BULK_FAILURE allocator_live={} allocator_peak={} owned_indexes={} commits={} cached_nodes={} node_limit={} pinned_nodes={} cached_quantized={}\n", .{ budget.live, budget.peak, owned_indexes, index.bulk_commit_rotation.commits, index.node_cache.count(), index.config.max_cached_nodes, index.pinned_node_cache.count(), index.quantized_cache.count() });
        };
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
            if (local_stage == null) try std.Io.Dir.cwd().createDir(io, spool_path, .fromMode(0o700));
            break :dir try std.Io.Dir.cwd().openDir(io, spool_path, .{});
        } else null;
        defer if (spool_dir) |dir| dir.close(io);
        var spool: hbc.HBCIndex.BulkSpool = .{ .dir = spool_dir orelse undefined, .io = io, .dims = dims };
        var spool_buffer: [64 * 1024]u8 = undefined;
        var spool_output = if (build_options != null and !resuming_source) try spool.create(&spool_buffer) else null;
        defer if (spool_output) |*output| output.abort(spool);
        if (spool_output) |*output| try output.enableIdentityCensus(budget.allocator(), .{ .check = BuildCancellation.check, .context = &build_cancellation });
        if (source_work) |root| {
            if (local_stage == null and !root.index_complete) {
                if (root.spool_from_exact) try bulk_work.rebuildPartition(budget.allocator(), store.*, root, &spool, provider, &index, &vectors, cancellation);
                try bulk_work.restorePartition(budget.allocator(), store.*, root, &spool, provider.context, cancellation);
            } else spool.next_file = 1;
            index.rng.hi = root.rng_hi;
            index.rng.lo = root.rng_lo;
        }
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
        const rows = if (!resuming_source) try input_provider.provider().open_with_cancellation_fn.?(input_provider.provider().ptr, a, binding, cancellation) else null;
        defer if (rows) |source_rows| source_rows.deinit(a);
        var input_remaining: u64 = bulk_input_limit;
        const input = if (provider.resume_work) |work| &work.native_input_left else &input_remaining;
        const ingest_started = probe.start();
        while (if (rows) |source_rows| try source_rows.next(a) else null) |batch| {
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
        probe.finish("dense_ingest", ingest_started);
        if (build_options != null and !(source_work != null and source_work.?.index_complete)) {
            const root = if (spool_output) |*output| output.finish(io) catch |err| return if (err == error.DuplicateVectorId) error.NativeLakeVectorIdentityCollision else err else source_work.?.partition;
            spool_output = null;
            if (!resuming_source) {
                try fresh_vectors.flush(&vectors);
                try vectors.finishBulkIngestSessionWithOptions(.{ .compact = false });
                source_session = false;
                try vectors.checkpointRecoveryManifest();
                try tracker.finish();
                if (provider.bulk_checkpoints and provider.progress != null and provider.progress.?.save_work != null) {
                    var cut = try vectors.pinNativeCheckpoint();
                    defer cut.deinit();
                    var phase_bytes: u64 = bulk_input_limit *| 3;
                    const phase_budget: files.OutputBudget = .{ .generation = &phase_bytes };
                    var generation = try files.publishCheckpointPaged(a, &cut, .{ .version = 2, .domain = store.upload_scope.?.domain }, "vectors/", store, cancellation, phase_budget, null);
                    generation = try bulk_work.publishPartition(a, store, cut.storage, generation, spool_path, root, cancellation, phase_budget, true);
                    source_work = .{ .spool_from_exact = true, .census_order = plan.selection != null, .binding = binding, .recipe = recipe, .dims = dims, .algorithm = @tagName(build_options.?.algorithm), .input_allowance = bulk_input_limit, .generation = generation, .partition = root, .file_directory = try plan.publishDirectory(a, store, generation.domain, cancellation), .rng_hi = index.rng.hi, .rng_lo = index.rng.lo };
                    try bulk_work.save(a, provider, store, wanted.name, source_work.?, cancellation);
                }
            }
            const cluster_started = probe.start();
            defer probe.finish("dense_cluster", cluster_started);
            index.bulkBuildSpool(&spool, root, build_options.?) catch |err| switch (err) {
                error.DuplicateVectorId => return error.NativeLakeVectorIdentityCollision,
                else => return err,
            };
            if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) {
                const profile = index.getWriteProfile();
                const report = try std.json.Stringify.valueAlloc(a, .{ .identity_census_verified = root.identities_verified, .identity_read_bytes = root.identity_read_bytes, .identity_written_bytes = root.identity_written_bytes, .identity_merge_inputs = root.identity_merge_inputs, .projection_cache_peak_bytes = if (projection_cache) |*value| value.snapshotStats().peak_used_bytes else 0, .owned_index_count = switch (index.env_owner) {
                    .lsm => |handle| handle.backend.run_index_cache.items.len,
                    .native => 0,
                }, .partition_bytes = profile.bulk_spool_partition_bytes, .partition_read_bytes = profile.bulk_spool_partition_read_bytes, .compact_rows = profile.bulk_spool_compact_rows, .projection_commit_ns = profile.bulk_projection_commit_ns, .save_node_ns = profile.save_node_ns, .update_parent_ns = profile.update_parent_ns, .update_parent_calls = profile.update_parent_calls, .training_ns = profile.bulk_build_leaf_training_ns, .routing_ns = profile.bulk_build_leaf_route_ns, .allocator_peak_bytes = budget.peak }, .{});
                defer a.free(report);
                std.debug.print("PARQUET_BULK_WORK {s}\n", .{report});
            }
        } else if (!resuming_source) {
            try tracker.finish();
        }
        if (build_options != null and provider.bulk_checkpoints and provider.progress != null and source_work != null and !source_work.?.index_complete) {
            var cut = try index.pinNativeCheckpoint();
            defer cut.deinit();
            var phase_bytes: u64 = bulk_input_limit *| 3;
            const phase_budget: files.OutputBudget = .{ .generation = &phase_bytes };
            if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) {
                var projection_bytes: u64 = cut.manifest_bytes.len;
                for (cut.run_sizes) |size| projection_bytes += size;
                const observation = try std.json.Stringify.valueAlloc(a, .{ .runs = cut.run_ids.len, .bytes = projection_bytes }, .{});
                defer a.free(observation);
                std.debug.print("PARQUET_BULK_PROJECTION {s}\n", .{observation});
            }
            var receipt = source_work.?;
            receipt.generation = try files.publishCheckpointPaged(a, &cut, receipt.generation, "", store, cancellation, phase_budget, null);
            const directory = @import("lake_index_native_directory.zig");
            // The completed tree replaces the routing frontier; keep only its
            // exact source and native projection in the new recovery closure.
            var changes = [_]directory.FileChange{
                .{ .path = try std.fmt.allocPrint(ca, "bulk-spool/{s}", .{receipt.partition.name}), .file = null },
                .{ .path = try std.fmt.allocPrint(ca, "bulk-spool/{s}.sample", .{receipt.partition.name}), .file = null },
            };
            receipt.generation = try directory.applyChanges(a, store, receipt.generation, if (receipt.partition.sample_count != 0) &changes else changes[0..1], cancellation, null);
            receipt.index_complete = true;
            receipt.rng_hi = index.rng.hi;
            receipt.rng_lo = index.rng.lo;
            try bulk_work.save(a, provider, store, wanted.name, receipt, cancellation);
            source_work = receipt;
        }
        if (resuming_source and source_work.?.index_complete and index.stats().active_count != source_work.?.partition.count) return error.InvalidNativeLakeFiles;
        if (incremental_session) try index.finishBulkIngestSessionWithOptions(.{});
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
        const checkpoint_started = probe.start();
        try index.finalizeExperimentalPostingGenerationAtAppliedSequence(0, .{ .flatten = flatten, .make_authoritative = true, .check = BuildCancellation.check, .context = &build_cancellation });
        probe.finish("dense_checkpoint", checkpoint_started);
        var checkpoint = (try index.nativeBackupGeneration(a, 0)) orelse return error.InvalidNativeLakeDenseRoot;
        defer checkpoint.deinit();
        var output_remaining: u64 = if (configured_bulk) try std.math.mul(u64, bulk_input_limit, 2) else 1024 * 1024 * 1024;
        const output_bytes: files.OutputBudget = .{ .generation = &output_remaining, .work = if (provider.resume_work) |work| &work.native_output_left else null };
        var navigation: @import("lake_index_native_directory.zig").Navigation = .{ .allocator = a };
        defer navigation.deinit();
        var vector_checkpoint = try vectors.pinNativeCheckpoint();
        defer vector_checkpoint.deinit();
        const upload_started = probe.start();
        if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) if (source_work) |source_phase| {
            const retained: files.Reuse = .{ .root = source_phase.generation, .navigation = &navigation };
            var reused_bytes: u64 = 0;
            var new_bytes: u64 = 0;
            var reused_runs: usize = 0;
            for (vector_checkpoint.run_paths, vector_checkpoint.run_ids) |physical, id| {
                const name = try std.fmt.allocPrint(a, "vectors/runs/{d}.tbl", .{id});
                defer a.free(name);
                const size = try vector_checkpoint.storage.fileSize(physical);
                const retained_file = try retained.lookup(store.*, name, cancellation);
                if (retained_file != null and retained_file.?.bytes == size) {
                    reused_runs += 1;
                    reused_bytes += size;
                } else new_bytes += size;
            }
            const observation = try std.json.Stringify.valueAlloc(a, .{ .runs = vector_checkpoint.run_ids.len, .reused_runs = reused_runs, .reused_bytes = reused_bytes, .new_bytes = new_bytes }, .{});
            defer a.free(observation);
            std.debug.print("PARQUET_BULK_SOURCE {s}\n", .{observation});
        };
        const generation = if (seed != null and seed.?.generation.version == 2) delta: {
            const before = try combineExtents(ca, try generationExtents(ca, &baseline_postings.?), try files.checkpointExtents(ca, &baseline_vectors.?, "vectors/"));
            const after = try combineExtents(ca, try generationExtents(ca, &checkpoint), try files.checkpointExtents(ca, &vector_checkpoint, "vectors/"));
            break :delta try files.publishExtentDelta(a, ca, checkpoint.storage, before, after, seed.?.generation, store, cancellation, output_bytes);
        } else full: {
            const posting_generation = try publishGeneration(a, ca, &checkpoint, store, cancellation, output_bytes, if (seed) |root| .{ .root = root.generation, .navigation = &navigation } else null);
            const directory = @import("lake_index_native_directory.zig");
            const posting_root = try directory.persist(a, ca, store, posting_generation, null, cancellation, if (provider.resume_work) |work| &work.native_output_left else null);
            break :full try files.publishCheckpointPaged(a, &vector_checkpoint, posting_root, "vectors/", store, cancellation, output_bytes, if (seed) |root| .{ .root = root.generation, .navigation = &navigation } else if (source_work) |root| .{ .root = root.generation, .navigation = &navigation } else null);
        };
        probe.finish("dense_upload", upload_started);
        const root: Root = .{ .binding = binding, .config_json = config.config_json, .dims = dims, .metric = metric, .generation = generation, .recipe = recipe, .file_states = if (source_work == null and !plan.isPaged()) plan.files else &.{}, .file_directory = if (source_work) |root| root.file_directory else if (plan.isPaged()) try plan.publishDirectory(a, store, generation.domain, cancellation) else null };
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        if (bytes.len > files.max_root_bytes) return error.NativeLakeFilesTooLarge;
        if (provider.resume_work) |work| try stores.chargeReadBudget(&work.native_output_left, bytes.len);
        var uploaded = try store.putWithCancellation(bytes, cancellation);
        defer uploaded.deinit(store.allocator);
        if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) {
            const source_peak = if (local_stage) |stage| stage.budget.peak else 0;
            const observation = try std.json.Stringify.valueAlloc(a, .{ .dense_allocator_peak_bytes = budget.peak, .source_allocator_peak_bytes = source_peak, .max_vector_allocator_peak_bytes = @max(budget.peak, source_peak) }, .{});
            defer a.free(observation);
            std.debug.print("PARQUET_BULK_MEMORY {s}\n", .{observation});
        }
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
        var backend = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, bulkVectorStoreOptions(null));
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
        try backend.checkpointRecoveryManifest();
        var sealed = try backend.pinNativeCheckpoint();
        defer sealed.deinit();
        try std.testing.expectEqual(@as(usize, 1), sealed.run_ids.len);
        const stats = backend.snapshotWriteStats();
        try std.testing.expect(stats.sorted_ingest_runs > 0);
        try std.testing.expectEqual(@as(u64, 0), stats.wal_append_bytes);
        try std.testing.expectEqual(@as(u64, 0), stats.flush_input_entries);
        try std.testing.expect(@as(u64, count) * dims * 4 > budget.limit);
        try std.testing.expect(budget.peak <= budget.limit);
    }
    var reopened = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, bulkVectorStoreOptions(null));
    defer reopened.close();
    var read = try reopened.beginRead();
    defer read.abort();
    for ([_]usize{ 0, count / 2, count - 1 }) |row| {
        var key: [32]u8 = undefined;
        try std.testing.expectEqualSlices(u8, encoded, try read.get(.{ .name = "exact_vectors" }, try std.fmt.bufPrint(&key, "row-{d:0>8}", .{row})));
    }
}

test "external lake execution private source caps narrow SST entry counts across seal and reopen" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("offline-source-entry-cap");
    defer directory.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}/vectors", .{directory.path()});
    defer a.free(path);
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 16 * 1024 * 1024 };
    const count = bulk_run_entry_limit + 1;
    const encoded = try @import("antfly_vector").codec.encodePackedF32BytesAlloc(a, &.{ 1, 2 });
    defer a.free(encoded);
    {
        var backend = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, bulkVectorStoreOptions(null));
        defer backend.close();
        try backend.beginBulkIngestSession();
        var fresh = FreshVectors.init(budget.allocator());
        defer fresh.deinit();
        for (0..count) |row| {
            var key: [32]u8 = undefined;
            try fresh.append("exact_vectors", try std.fmt.bufPrint(&key, "row-{d:0>8}", .{row}), encoded);
            // Match the thousands of ingest runs in a corpus-scale source.
            if (row % 32 == 31) try fresh.flush(&backend);
        }
        try fresh.flush(&backend);
        try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
        try backend.checkpointRecoveryManifest();
        try std.testing.expect(backend.runs.count() >= 2);
        var rows: usize = 0;
        for (0..backend.runs.count()) |i| {
            const run = backend.runs.at(i);
            try std.testing.expect(run.entry_count <= bulk_run_entry_limit);
            try std.testing.expect(run.bloom_filter == null);
            rows += run.entry_count;
        }
        try std.testing.expectEqual(count, rows);
        try std.testing.expect(budget.footprint() <= budget.limit);
    }
    var reopened = try local.storage_lsm_backend.Backend.open(budget.allocator(), path, bulkVectorStoreOptions(null));
    defer reopened.close();
    const runs_path = try std.fs.path.join(a, &.{ path, "runs" });
    defer a.free(runs_path);
    const physical_runs = try reopened.storage.?.listFileNamesAlloc(a, runs_path);
    defer local.storage_lsm_backend.Storage.freeFileNames(a, physical_runs);
    // Recovery must not retain the old pre-merge corpus through its manifest.
    try std.testing.expectEqual(reopened.runs.count(), physical_runs.len);
    var read = try reopened.beginRead();
    defer read.abort();
    var key: [32]u8 = undefined;
    try std.testing.expectEqualSlices(u8, encoded, try read.get(.{ .name = "exact_vectors" }, try std.fmt.bufPrint(&key, "row-{d:0>8}", .{count - 1})));
}

test "external lake execution immutable source receipt survives close and reopen without run rewrites" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("offline-source-receipt");
    defer directory.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}/vectors", .{directory.path()});
    defer a.free(path);
    var ids: std.ArrayList(u64) = .empty;
    defer ids.deinit(a);
    {
        var backend = try local.storage_lsm_backend.Backend.open(a, path, bulkVectorStoreOptions(null));
        defer backend.close();
        // Exceed the ordinary close-time L0 hard limit without a large fixture.
        var fresh = FreshVectors.init(a);
        defer fresh.deinit();
        for (0..24) |row| {
            var key: [32]u8 = undefined;
            try fresh.append("exact_vectors", try std.fmt.bufPrint(&key, "row-{d:0>8}", .{row}), "immutable");
            try fresh.flush(&backend);
        }
        var cut = try backend.pinNativeCheckpoint();
        defer cut.deinit();
        try std.testing.expect(cut.run_ids.len > 8);
        try ids.appendSlice(a, cut.run_ids);
    }
    var reopened = try local.storage_lsm_backend.Backend.open(a, path, bulkVectorStoreOptions(null));
    defer reopened.close();
    var cut = try reopened.pinNativeCheckpoint();
    defer cut.deinit();
    try std.testing.expectEqualSlices(u64, ids.items, cut.run_ids);
    var read = try reopened.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("immutable", try read.get(.{ .name = "exact_vectors" }, "row-00000023"));
}
