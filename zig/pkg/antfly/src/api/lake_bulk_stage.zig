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

//! Tee a fresh authored dense corpus into its final exact-vector source and
//! transformed root partition while the identity verifier owns each page.
//! These private outputs gain recovery authority only after full verification.
const std = @import("std");
const probe = @import("lake_execution_probe_hooks.zig");
const local = @import("antfly_local_sources");
const dense = @import("lake_index_native_dense.zig");
const state = @import("lake_index_native_state.zig");
const work = @import("lake_bulk_work.zig");
const files = @import("lake_index_native_files.zig");
const stores = @import("../serverless/artifacts/store.zig");
const rebuild = @import("../serverless/build/lake_rebuild.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const A = std.mem.Allocator;
const Hbc = local.storage_hbc_adapter;
const Token = @import("antfly_cancellation").CancellationToken;
pub const Stage = struct {
    a: A,
    arena: *std.heap.ArenaAllocator,
    budget: local.sql_memory_budget,
    path: [:0]u8,
    spool_path: []const u8,
    io: std.Io,
    store: *stores.ArtifactStore,
    context: local.serverless_query_lake_read_context.Context,
    token: Token,
    name: []const u8,
    column: []const u8,
    config: std.json.Value,
    binding: local.serverless_segment_source_binding.Binding,
    recipe: [32]u8,
    algorithm: []const u8,
    dims: u32,
    allowance: u64,
    left: u64,
    plan: state.Plan,
    tracker: state.Tracker,
    vectors: local.storage_lsm_backend.Backend,
    fresh: dense.FreshVectors,
    index: Hbc.HBCIndex,
    dir: std.Io.Dir,
    spool: Hbc.HBCIndex.BulkSpool,
    output: ?Hbc.HBCIndex.BulkSpoolOutput = null,
    buffer: [64 * 1024]u8 = undefined,
    source_session: bool = false,
    finished: ?work.Root = null,
    closed: bool = false,

    pub fn start(a: A, table: local.common_topology_records.TableRecord, base: local.serverless_manifest_base_source.BaseSourceDescriptor, provider: *Provider, store: *stores.ArtifactStore, token: Token) !?*Stage {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const ra = region.allocator();
        const configs = try std.json.parseFromSliceLeaky(std.json.Value, ra, table.indexes_json, .{});
        if (configs != .object or configs.object.count() != 1) return null;
        const config = configs.object.values()[0];
        const configured = @import("lake_enrichment_units.zig").configured;
        if (configured(config, "lake_bulk_build") == null or !@import("lake_row_input_recipes.zig").configurationReusable(config)) return null;
        if (configured(config, "template") != null) return null;
        const external = configured(config, "external") orelse return null;
        if (external != .bool or !external.bool) return null;
        var schema = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
        defer schema.deinit(a);
        if (schema.external_base_source == null or schema.external_base_source.?.binding.row_identity == null) return null;
        var desired = try rebuild.desiredArtifactsFromResolvedExternalSourceAlloc(a, base, provider.source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .indexes_json = table.indexes_json });
        defer desired.deinit(a);
        if (desired.artifacts.len != 1 or desired.artifacts[0].kind != .vector_segment) return null;
        const wanted = desired.artifacts[0];
        const self = try a.create(Stage);
        errdefer a.destroy(self);
        const arena = try a.create(std.heap.ArenaAllocator);
        errdefer a.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const ca = arena.allocator();
        const public = try std.json.Stringify.valueAlloc(ca, config, .{});
        const normalized_config = try local.api_table_index_config.parseIndexConfig(ca, wanted.name, public);
        const normalized = try std.json.parseFromSliceLeaky(std.json.Value, ca, normalized_config.config_json, .{});
        const dims = std.math.cast(u32, normalized.object.get("dims").?.integer) orelse return error.InvalidVectorDimensions;
        var options: Hbc.HBCConfig = .{ .dims = dims, .max_cached_nodes = 1, .max_cached_vectors = 0, .max_cached_metadata = 0, .pinned_tree_depth = 0 };
        options.metric = std.meta.stringToEnum(@TypeOf(options.metric), normalized.object.get("metric").?.string) orelse return error.InvalidIndexConfig;
        try dense.applyQuantization(&options, normalized);
        const algorithm = configured(config, "lake_bulk_build").?;
        if (algorithm != .string or (!std.mem.eql(u8, algorithm.string, "lloyd") and !std.mem.eql(u8, algorithm.string, "emeans"))) return error.InvalidIndexConfig;
        if (std.mem.eql(u8, algorithm.string, "emeans") and options.metric != .l2_squared) return error.EmeansRequiresL2;
        const allowance: u64 = if (configured(config, "lake_bulk_build_max_input_bytes")) |limit| if (limit == .integer and limit.integer > 0) @intCast(limit.integer) else return error.InvalidIndexConfig else 512 * 1024 * 1024;
        const io = provider.context.io orelse return null;
        var nonce: [16]u8 = undefined;
        try io.randomSecure(&nonce);
        const path = try std.fmt.allocPrintSentinel(a, "/tmp/antfly-lake-bulk-stage-{s}", .{std.fmt.bytesToHex(nonce, .lower)}, 0);
        errdefer a.free(path);
        try std.Io.Dir.cwd().createDir(io, path, .fromMode(0o700));
        errdefer std.Io.Dir.cwd().deleteTree(io, path) catch {};
        const spool_path = try std.fmt.allocPrint(ca, "{s}/bulk-spool", .{path});
        try std.Io.Dir.cwd().createDir(io, spool_path, .fromMode(0o700));
        const dir = try std.Io.Dir.cwd().openDir(io, spool_path, .{});
        errdefer dir.close(io);
        var plan = try state.Plan.initPaged(a, ca, provider, &.{}, null, false, false);
        errdefer plan.deinit();
        self.* = .{ .a = a, .arena = arena, .budget = .{ .backing = a, .limit = 256 * 1024 * 1024 }, .path = path, .spool_path = spool_path, .io = io, .store = store, .context = provider.context, .token = token, .name = try ca.dupe(u8, wanted.name), .column = try ca.dupe(u8, wanted.build_spec.?.vector.vector_column), .config = try std.json.parseFromSliceLeaky(std.json.Value, ca, public, .{ .allocate = .alloc_always }), .binding = try std.json.parseFromSliceLeaky(@TypeOf(wanted.binding), ca, try std.json.Stringify.valueAlloc(ca, wanted.binding, .{}), .{ .allocate = .alloc_always }), .recipe = try state.recipe(ca, table, public, wanted.binding.column_bindings), .algorithm = try ca.dupe(u8, algorithm.string), .dims = dims, .allowance = allowance, .left = allowance, .plan = plan, .tracker = undefined, .vectors = undefined, .fresh = undefined, .index = undefined, .dir = dir, .spool = .{ .dir = dir, .io = io, .dims = dims } };
        // The source phase uses exactly the serving binding, including its native
        // implementation epoch. No generated vector or expression bypasses it.
        self.binding.index_config_hash = try std.fmt.allocPrint(ca, "native-dense-checkpoint-v2:{s}", .{self.binding.index_config_hash});
        self.tracker = try state.Tracker.init(a, ca, &self.plan, store, token);
        errdefer self.tracker.deinit();
        self.tracker.output_bytes = allowance;
        self.tracker.directory_bytes = allowance;
        var vector_options = dense.bulkVectorStoreOptions(null);
        vector_options.bulk_cancellation = .{ .ptr = self, .check_fn = checkStorage };
        self.vectors = try local.storage_lsm_backend.Backend.open(self.budget.allocator(), try std.fmt.allocPrint(ca, "{s}/vectors", .{path}), vector_options);
        errdefer self.vectors.close();
        self.fresh = dense.FreshVectors.init(self.budget.allocator());
        errdefer self.fresh.deinit();
        self.index = try Hbc.HBCIndex.open(self.budget.allocator(), path, options);
        errdefer self.index.close();
        self.index.setIo(io);
        try self.vectors.beginBulkIngestSession();
        self.source_session = true;
        errdefer self.vectors.abortBulkIngestSession();
        self.output = try self.spool.create(&self.buffer);
        errdefer self.output.?.abort(self.spool);
        try self.output.?.enableIdentityCensus(self.budget.allocator(), .{ .check = checkCensus, .context = self });
        try self.output.?.enableSample(self.budget.allocator(), 512, 8 * 1024 * 1024, self.index.rng.intN(std.math.maxInt(usize)));
        return self;
    }
    fn checkCensus(raw: *anyopaque) !void {
        const self: *Stage = @ptrCast(@alignCast(raw));
        try self.token.check();
        try self.context.ensureActive();
    }
    fn checkStorage(raw: *const anyopaque) !void {
        const self: *const Stage = @ptrCast(@alignCast(raw));
        try self.token.check();
        try self.context.ensureActive();
    }
    pub fn deinit(self: *Stage) void {
        if (self.output) |*output| output.abort(self.spool);
        if (self.source_session) self.vectors.abortBulkIngestSession();
        if (!self.closed) {
            self.index.close();
            self.fresh.deinit();
            self.vectors.close();
        }
        self.tracker.deinit();
        self.plan.deinit();
        self.dir.close(self.io);
        std.Io.Dir.cwd().deleteTree(self.io, self.path) catch {};
        self.a.free(self.path);
        self.arena.deinit();
        self.a.destroy(self.arena);
        self.a.destroy(self);
    }
    pub fn add(self: *Stage, batch: local.storage_rowsource_types.ColumnBatch) !void {
        const started = probe.start();
        defer probe.finish("bulk_tee", started);
        var region = std.heap.ArenaAllocator.init(self.budget.allocator());
        defer region.deinit();
        const a = region.allocator();
        var producer = try @import("lake_vector_enrichment.zig").Producer.init(a, self.name, self.column, self.config, null);
        defer producer.deinit();
        const bound = producer.bind(batch);
        const transformed = try a.alloc(f32, self.dims);
        for (0..batch.rowCount()) |row| {
            try self.token.check();
            try self.context.ensureActive();
            const prepared = try bound.prepare(a, row);
            for (prepared.units()) |unit| {
                const vector = (try producer.denseUnit(a, unit, self.dims)) orelse continue;
                try stores.chargeReadBudget(&self.left, @as(u64, self.dims) * 4 + 128);
                const ref = batch.rowRef(row);
                const key = try self.plan.privateKey(a, ref);
                self.tracker.append(ref, key) catch |err| {
                    probe.failure("identity_chunk_upload", err, null);
                    return err;
                };
                try self.fresh.append("exact_vectors", key, try @import("antfly_vector").codec.encodePackedF32BytesAlloc(a, vector));
                if (self.fresh.bytes >= 16 * 1024 * 1024) self.fresh.flush(&self.vectors) catch |err| {
                    probe.failure("exact_vector_flush", err, null);
                    return err;
                };
                self.output.?.append(dense.vectorId(key), key, self.index.transformVector(vector, transformed)) catch |err| {
                    probe.failure("spool_append", err, self.output.?.writer.err);
                    return err;
                };
            }
        }
    }
    pub fn finish(self: *Stage, provider: *Provider) !void {
        const started = probe.start();
        defer probe.finish("bulk_receipt", started);
        const partition = self.output.?.finish(self.io) catch |err| {
            probe.failure("spool_finish", err, self.output.?.writer.err);
            return if (err == error.DuplicateVectorId) error.NativeLakeVectorIdentityCollision else err;
        };
        self.output = null;
        self.fresh.flush(&self.vectors) catch |err| {
            probe.failure("exact_vector_flush", err, null);
            return err;
        };
        try self.vectors.finishBulkIngestSessionWithOptions(.{ .compact = false });
        self.source_session = false;
        try self.vectors.checkpointRecoveryManifest();
        self.tracker.finish() catch |err| {
            probe.failure("identity_chunk_finish", err, null);
            return err;
        };
        var checkpoint = try self.vectors.pinNativeCheckpoint();
        var checkpoint_open = true;
        defer if (checkpoint_open) checkpoint.deinit();
        var bytes: u64 = self.allowance *| 3;
        const budget: files.OutputBudget = .{ .generation = &bytes };
        var generation: files.Root = .{ .version = 2, .domain = self.store.upload_scope.?.domain };
        // Identity reduction has joined its workers before this quiescent
        // handoff. Unlike an index job, the fused stage otherwise has no I/O
        // capability, forcing every source chunk through required-inline PUTs.
        // Reuse the ordinary bounded publication scheduler and preserve any
        // caller-owned capability. All upload children drain synchronously.
        var publication_io: stores.PublicationIo = .{};
        publication_io.install(&self.store.io_phase, self.io, if (provider.source.scanner.shared_reader) |reader| reader.scheduler else local.sql_parallel_scheduler.global(), .{ .ptr = self, .check = checkCensus });
        defer publication_io.deinit();
        if (provider.bulk_checkpoints) {
            generation = try files.publishCheckpointPaged(self.a, &checkpoint, generation, "vectors/", self.store, self.token, budget, null);
            generation = try work.publishPartition(self.a, self.store, checkpoint.storage, generation, self.spool_path, partition, self.token, budget, true);
        }
        const root: work.Root = .{ .spool_from_exact = true, .binding = self.binding, .recipe = self.recipe, .dims = self.dims, .algorithm = self.algorithm, .input_allowance = self.allowance, .generation = generation, .partition = partition, .file_directory = try self.plan.publishDirectory(self.a, self.store, generation.domain, self.token), .rng_hi = self.index.rng.hi, .rng_lo = self.index.rng.lo };
        if (provider.bulk_checkpoints) try work.save(self.a, provider, self.store, self.name, root, self.token);
        self.finished = root;
        checkpoint.deinit();
        checkpoint_open = false;
        self.index.close();
        self.fresh.deinit();
        self.vectors.close();
        self.closed = true;
    }
};
