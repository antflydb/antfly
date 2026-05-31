// Copyright 2026 Antfly, Inc.
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

//! Entity-resolution stage core (see zig/RESOLUTION.md).
//!
//! This is the transform the managed resolution worker runs per changed
//! extraction artifact: given the shard's configured resolvers and the bytes of
//! a changed extraction artifact, find the resolver that consumes it, build its
//! engine from the durable catalog config, resolve the mentions, and produce
//! the resolution artifact bytes. The worker that wraps this owns the store I/O
//! (reading the extraction artifact, persisting the resolution artifact through
//! a DerivedBatch) and candidate blocking; keeping the transform separate makes
//! it pure and unit-testable.

const std = @import("std");
const builtin = @import("builtin");
const resolver_lib = @import("antfly_resolver");
const matcher = @import("antfly_matcher");
const resolver_catalog = @import("catalog/resolver_catalog.zig");
const internal_keys = @import("../internal_keys.zig");
const derived_types = @import("derived/derived_types.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const replay_source_mod = @import("derived/replay_source.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const backend_erased = @import("../backend_erased.zig");
const background_runtime_mod = @import("../background_runtime.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const ResolverConfig = resolver_catalog.ResolverConfig;

/// applied-sequence checkpoint scope; also used by the replay prune watermark
/// so resolution records survive until the worker consumes them.
pub const scope_name = "resolution";

/// Appends a derived batch to the replay log, returning its sequence. Matches
/// the enrichment runtime's writer so the db wires the same callback.
pub const DerivedRecordWriter = *const fn (ptr: *anyopaque, batch: derived_types.DerivedBatch) anyerror!u64;

/// Source of candidate entities for blocking, abstracted over locality. The
/// storage worker calls this; a local source reads the worker's own store
/// (`localCandidateSource`), while the api layer implements a cross-shard source
/// over the cluster transport (topology lookup + group routing +
/// fetchGroupLookup / vector-worker). Unlike the in-store seam this works at the
/// logical entity-key level (the cross-shard impl decodes keys on each shard).
pub const CandidateSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Consume = *const fn (ctx: *anyopaque, entity_key: []const u8, value: []const u8) anyerror!void;

    pub const VTable = struct {
        /// Fetch the entity doc for `key` in `table` (owned bytes or null).
        get: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8,
        /// Scan `table` for entities whose key starts with `prefix`.
        scan_prefix: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, ctx: *anyopaque, consume: Consume) anyerror!void = null,
        /// The `k` nearest entities in `table` to `embedding`.
        nearest: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, embedding: []const f32, k: usize, ctx: *anyopaque, consume: Consume) anyerror!void = null,
    };

    pub fn get(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8 {
        return self.vtable.get(self.ptr, allocator, table, key);
    }
    pub fn scanPrefix(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, ctx: *anyopaque, consume: Consume) anyerror!void {
        const f = self.vtable.scan_prefix orelse return error.ScanUnsupported;
        return f(self.ptr, allocator, table, prefix, ctx, consume);
    }
    pub fn nearest(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, embedding: []const f32, k: usize, ctx: *anyopaque, consume: Consume) anyerror!void {
        const f = self.vtable.nearest orelse return error.NearestUnsupported;
        return f(self.ptr, allocator, table, embedding, k, ctx, consume);
    }
};

pub const ResolutionOutput = struct {
    /// Name of the resolution artifact to write (borrows the matched config).
    resolution_artifact: []const u8,
    /// Serialized resolution artifact; owned by the caller.
    bytes: []u8,
};

/// Returns the resolver whose `source_artifact` matches `artifact_name`, or
/// null. The first match wins; V1 expects one resolver per source artifact.
pub fn resolverForArtifact(
    resolvers: []const ResolverConfig,
    artifact_name: []const u8,
) ?*const ResolverConfig {
    for (resolvers) |*cfg| {
        if (std.mem.eql(u8, cfg.source_artifact, artifact_name)) return cfg;
    }
    return null;
}

/// Resolve a changed extraction artifact into resolution artifact bytes.
/// Returns null when no configured resolver consumes `artifact_name`.
/// `candidates` supplies blocking candidates per entity (empty = deterministic
/// minting only); the worker fills this from the entity table.
pub fn resolveExtraction(
    gpa: std.mem.Allocator,
    resolvers: []const ResolverConfig,
    artifact_name: []const u8,
    extraction_bytes: []const u8,
    candidates: []const []const resolver_lib.Candidate,
) !?ResolutionOutput {
    const cfg = resolverForArtifact(resolvers, artifact_name) orelse return null;

    var resolver = try resolver_lib.Resolver.initFromParts(
        gpa,
        cfg.table,
        cfg.key_template,
        cfg.type_must_match,
        cfg.scorer_json,
    );
    defer resolver.deinit();

    var parsed = try resolver_lib.parseExtractionEntities(gpa, extraction_bytes);
    defer parsed.deinit();

    var resolution = try resolver.resolve(gpa, cfg.config_generation, parsed.entities, candidates);
    defer resolution.deinit();

    const bytes = try resolution.toJson(gpa);
    return .{ .resolution_artifact = cfg.resolution_artifact, .bytes = bytes };
}

pub const ProcessOutcome = struct {
    result: resolver_lib.RunResult,
    /// The resolution artifact key the stage acted on; owned by the caller.
    resolution_key: []u8,
};

/// Process a changed extraction (asset) artifact key: look up the resolver that
/// consumes it and run the resolution stage to idempotently (re)persist the
/// resolution artifact through `store`. Returns null when `changed_key` is not
/// an asset artifact or no configured resolver consumes it. `provider` supplies
/// blocking candidates (null = deterministic minting only). The returned
/// `resolution_key` is owned by the caller, which journals it (on written /
/// cleared) via a `DerivedBatch` so downstream stages (graph materializer) wake.
pub fn processChangedExtraction(
    gpa: std.mem.Allocator,
    resolvers: []const ResolverConfig,
    store: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    changed_key: []const u8,
    candidate_source: ?CandidateSource,
    embedder: ?embedder_mod.DenseEmbedder,
) !?ProcessOutcome {
    const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(gpa, changed_key)) orelse return null;
    defer gpa.free(parsed.doc_key);
    defer gpa.free(parsed.artifact_name);

    const cfg = resolverForArtifact(resolvers, parsed.artifact_name) orelse return null;

    var resolver = try resolver_lib.Resolver.initFromParts(
        gpa,
        cfg.table,
        cfg.key_template,
        cfg.type_must_match,
        cfg.scorer_json,
    );
    defer resolver.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(gpa, parsed.doc_key, cfg.resolution_artifact);
    errdefer gpa.free(resolution_key);

    // Candidate blocking: if configured and the caller didn't supply a provider,
    // look up the rendered canonical key as a candidate so the scorer can link
    // to an existing entity instead of always minting.
    var exact_provider = ExactKeyCandidateProvider{ .store = store, .resolver = &resolver, .table = cfg.table };
    var prefix_provider = PrefixCandidateProvider{ .store = store, .resolver = &resolver, .table = cfg.table };
    var source_provider: SourceCandidateProvider = undefined;
    const effective_provider = provider orelse blk: {
        // A cross-shard source (injected by the api layer) wins over the local
        // in-store providers when the resolver declares a candidate search mode.
        if (candidate_source) |src| {
            if (candidateModeFromConfig(cfg.candidate_search)) |mode| {
                source_provider = .{ .source = src, .resolver = &resolver, .table = cfg.table, .mode = mode };
                break :blk source_provider.provider();
            }
            break :blk null;
        }
        if (std.mem.eql(u8, cfg.candidate_search, "exact_key")) break :blk exact_provider.provider();
        if (std.mem.eql(u8, cfg.candidate_search, "prefix")) break :blk prefix_provider.provider();
        break :blk null;
    };

    // Name-embedding backfill: when the resolver declares a `name_embedding`
    // model and the runtime has an embedder, mentions without a vector get one
    // from their text so cosine/ann blocking has a query vector.
    var mention_embedder = DenseMentionEmbedder{
        .embedder = undefined,
        .embedding_name = cfg.name_embedding,
        .dims = cfg.name_embedding_dims,
    };
    const stage_embedder: ?resolver_lib.MentionEmbedder = if (cfg.name_embedding.len > 0) blk: {
        const e = embedder orelse break :blk null;
        mention_embedder.embedder = e;
        break :blk mention_embedder.mentionEmbedder();
    } else null;

    const stage = resolver_lib.ResolutionStage{
        .resolver = &resolver,
        .config_generation = cfg.config_generation,
        .embedder = stage_embedder,
    };
    const result = try stage.run(gpa, store, effective_provider, changed_key, resolution_key);
    return .{ .result = result, .resolution_key = resolution_key };
}

/// Adapts the storage `DenseEmbedder` to the resolver's `MentionEmbedder` seam,
/// binding the resolver's embedding model name and dimensionality.
const DenseMentionEmbedder = struct {
    embedder: embedder_mod.DenseEmbedder,
    embedding_name: []const u8,
    dims: u32,

    fn mentionEmbedder(self: *DenseMentionEmbedder) resolver_lib.MentionEmbedder {
        return .{ .ptr = self, .embed_fn = embed };
    }
    fn embed(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8) anyerror!?[]const f32 {
        const self: *DenseMentionEmbedder = @ptrCast(@alignCast(ptr));
        return try self.embedder.embedDense(alloc, self.embedding_name, text, self.dims);
    }
};

/// Build a candidate from an entity document's JSON (its string fields become
/// the matcher record; "label"/"entity_type" set the candidate label) and
/// append it to `out`. `entity_key` + `table` form the candidate DocRef.
fn appendEntityCandidate(
    allocator: std.mem.Allocator,
    table: []const u8,
    entity_key: []const u8,
    value: []const u8,
    out: *std.ArrayListUnmanaged(resolver_lib.Candidate),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, value, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;

    var fields = std.ArrayListUnmanaged(matcher.Field).empty;
    var label: []const u8 = "";
    var it = parsed.value.object.iterator();
    while (it.next()) |e| {
        switch (e.value_ptr.*) {
            .string => |s| {
                const fname = try allocator.dupe(u8, e.key_ptr.*);
                const fval = try allocator.dupe(u8, s);
                try fields.append(allocator, .{ .name = fname, .value = .{ .text = fval } });
                if (std.mem.eql(u8, e.key_ptr.*, "label") or std.mem.eql(u8, e.key_ptr.*, "entity_type")) label = fval;
            },
            .array => |arr| {
                // A numeric array becomes a vector field (e.g. name_embedding).
                if (arr.items.len == 0) continue;
                const vec = try allocator.alloc(f32, arr.items.len);
                var ok = true;
                for (arr.items, 0..) |item, i| {
                    vec[i] = switch (item) {
                        .float => |f| @floatCast(f),
                        .integer => |n| @floatFromInt(n),
                        .number_string => |ns| std.fmt.parseFloat(f32, ns) catch {
                            ok = false;
                            break;
                        },
                        else => {
                            ok = false;
                            break;
                        },
                    };
                }
                if (ok) {
                    try fields.append(allocator, .{ .name = try allocator.dupe(u8, e.key_ptr.*), .value = .{ .vector = vec } });
                } else allocator.free(vec);
            },
            else => {},
        }
    }

    try out.append(allocator, .{
        .doc_ref = .{ .table = try allocator.dupe(u8, table), .key = try allocator.dupe(u8, entity_key) },
        .label = label,
        .record = .{ .fields = try fields.toOwnedSlice(allocator) },
    });
}

pub const CandidateMode = enum { exact_key, prefix, ann };

fn candidateModeFromConfig(s: []const u8) ?CandidateMode {
    if (std.mem.eql(u8, s, "exact_key")) return .exact_key;
    if (std.mem.eql(u8, s, "prefix")) return .prefix;
    if (std.mem.eql(u8, s, "ann")) return .ann;
    return null;
}

/// Candidate provider over a (possibly cross-shard) `CandidateSource`: renders
/// the mention's canonical key, then queries the source by exact key, label
/// prefix, or vector nearest-neighbour, building candidates for the scorer.
const SourceCandidateProvider = struct {
    source: CandidateSource,
    resolver: *const resolver_lib.Resolver,
    table: []const u8,
    mode: CandidateMode,

    fn provider(self: *SourceCandidateProvider) resolver_lib.CandidateProvider {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = resolver_lib.CandidateProvider.VTable{ .candidates_for = candidatesFor };

    const ScanCtx = struct {
        allocator: std.mem.Allocator,
        table: []const u8,
        out: *std.ArrayListUnmanaged(resolver_lib.Candidate),
        fn consume(ptr: *anyopaque, entity_key: []const u8, value: []const u8) anyerror!void {
            const self: *ScanCtx = @ptrCast(@alignCast(ptr));
            try appendEntityCandidate(self.allocator, self.table, entity_key, value, self.out);
        }
    };

    fn candidatesFor(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        entity: resolver_lib.ExtractedEntity,
        out: *std.ArrayListUnmanaged(resolver_lib.Candidate),
    ) anyerror!void {
        const self: *SourceCandidateProvider = @ptrCast(@alignCast(ptr));
        switch (self.mode) {
            .exact_key => {
                const key = try self.resolver.renderKeyAlloc(allocator, entity);
                defer allocator.free(@constCast(key));
                const raw = (try self.source.get(allocator, self.table, key)) orelse return;
                defer allocator.free(raw);
                try appendEntityCandidate(allocator, self.table, key, raw, out);
            },
            .prefix => {
                const key = try self.resolver.renderKeyAlloc(allocator, entity);
                defer allocator.free(@constCast(key));
                const slash = std.mem.indexOfScalar(u8, key, '/');
                const prefix = if (slash) |i| key[0 .. i + 1] else key;
                var ctx = ScanCtx{ .allocator = allocator, .table = self.table, .out = out };
                self.source.scanPrefix(allocator, self.table, prefix, &ctx, ScanCtx.consume) catch |e| switch (e) {
                    error.ScanUnsupported => return,
                    else => return e,
                };
            },
            .ann => {
                const emb = entity.embedding orelse return;
                var ctx = ScanCtx{ .allocator = allocator, .table = self.table, .out = out };
                self.source.nearest(allocator, self.table, emb, 25, &ctx, ScanCtx.consume) catch |e| switch (e) {
                    error.NearestUnsupported => return,
                    else => return e,
                };
            },
        }
    }
};

/// "exact_key" blocking: look up the rendered canonical key as an existing
/// entity (by document key through the store seam). Reads whatever the worker's
/// store can see; cross-shard entity tables need distributed reads (phase 2).
const ExactKeyCandidateProvider = struct {
    store: resolver_lib.ArtifactStore,
    resolver: *const resolver_lib.Resolver,
    table: []const u8,

    fn provider(self: *ExactKeyCandidateProvider) resolver_lib.CandidateProvider {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = resolver_lib.CandidateProvider.VTable{ .candidates_for = candidatesFor };
    fn candidatesFor(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        entity: resolver_lib.ExtractedEntity,
        out: *std.ArrayListUnmanaged(resolver_lib.Candidate),
    ) anyerror!void {
        const self: *ExactKeyCandidateProvider = @ptrCast(@alignCast(ptr));
        const key = try self.resolver.renderKeyAlloc(allocator, entity);
        defer allocator.free(@constCast(key));
        const doc_store_key = try internal_keys.documentKeyAlloc(allocator, key);
        defer allocator.free(doc_store_key);
        const raw = (try self.store.get(allocator, doc_store_key)) orelse return;
        defer allocator.free(raw);
        try appendEntityCandidate(allocator, self.table, key, raw, out);
    }
};

/// "prefix" blocking: scan the entity key range under the rendered key's label
/// namespace and offer every entity as a candidate for the scorer to rank, so a
/// typo'd mention can link to an existing entity with a different key. Suited to
/// modest entity tables; ANN blocking is the large-table successor.
const PrefixCandidateProvider = struct {
    store: resolver_lib.ArtifactStore,
    resolver: *const resolver_lib.Resolver,
    table: []const u8,

    fn provider(self: *PrefixCandidateProvider) resolver_lib.CandidateProvider {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = resolver_lib.CandidateProvider.VTable{ .candidates_for = candidatesFor };

    const ScanCtx = struct {
        allocator: std.mem.Allocator,
        table: []const u8,
        out: *std.ArrayListUnmanaged(resolver_lib.Candidate),

        fn consume(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            const self: *ScanCtx = @ptrCast(@alignCast(ptr));
            const entity_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(self.allocator, key)) orelse return;
            defer self.allocator.free(entity_key);
            try appendEntityCandidate(self.allocator, self.table, entity_key, value, self.out);
        }
    };

    fn candidatesFor(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        entity: resolver_lib.ExtractedEntity,
        out: *std.ArrayListUnmanaged(resolver_lib.Candidate),
    ) anyerror!void {
        const self: *PrefixCandidateProvider = @ptrCast(@alignCast(ptr));
        const key = try self.resolver.renderKeyAlloc(allocator, entity);
        defer allocator.free(@constCast(key));
        const slash = std.mem.indexOfScalar(u8, key, '/');
        const prefix = if (slash) |i| key[0 .. i + 1] else key;

        const lower = try internal_keys.documentRangeLowerAlloc(allocator, prefix);
        defer allocator.free(lower);
        const upper = (try internal_keys.documentRangeUpperAlloc(allocator, prefix)) orelse return;
        defer allocator.free(upper);

        var ctx = ScanCtx{ .allocator = allocator, .table = self.table, .out = out };
        self.store.scanPrefix(lower, upper, &ctx, ScanCtx.consume) catch |e| switch (e) {
            error.ScanUnsupported => return,
            else => return e,
        };
    }
};

/// Process every changed artifact key in a replay record: resolve each
/// extraction artifact and journal the resolution keys that actually changed
/// (written/cleared) in a single `DerivedBatch`, so downstream stages wake.
pub fn processRecordKeys(
    gpa: std.mem.Allocator,
    resolvers: []const ResolverConfig,
    store: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    changed_artifact_keys: []const []const u8,
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
    candidate_source: ?CandidateSource,
    embedder: ?embedder_mod.DenseEmbedder,
) !void {
    var journal_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (journal_keys.items) |k| gpa.free(@constCast(k));
        journal_keys.deinit(gpa);
    }

    for (changed_artifact_keys) |key| {
        const outcome = (try processChangedExtraction(gpa, resolvers, store, provider, key, candidate_source, embedder)) orelse continue;
        switch (outcome.result) {
            .written, .cleared => try journal_keys.append(gpa, outcome.resolution_key),
            else => gpa.free(outcome.resolution_key),
        }
    }

    if (journal_keys.items.len > 0) {
        _ = try write_fn(write_ctx, .{ .changed_artifact_keys = journal_keys.items });
    }
}

/// Re-resolve every stored extraction artifact a configured resolver consumes,
/// re-journaling the resolution keys that change. Used for config-generation
/// re-resolution: when a resolver's scorer / key_template / `config_generation`
/// changes, the incremental hint won't fire (the extraction artifacts didn't
/// change), so this backfill re-runs resolution over the existing corpus. The
/// stage is idempotent, so artifacts whose bytes are unchanged are skipped.
/// Scans the user-key namespace once and processes matches after the scan so no
/// write happens under an open read cursor.
pub fn reresolveAll(
    gpa: std.mem.Allocator,
    store: resolver_lib.ArtifactStore,
    resolvers: []const ResolverConfig,
    candidate_source: ?CandidateSource,
    embedder: ?embedder_mod.DenseEmbedder,
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
) !usize {
    var asset_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (asset_keys.items) |k| gpa.free(@constCast(k));
        asset_keys.deinit(gpa);
    }

    const Collector = struct {
        gpa: std.mem.Allocator,
        resolvers: []const ResolverConfig,
        out: *std.ArrayListUnmanaged([]const u8),

        fn consume(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            _ = value;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!internal_keys.isAssetArtifactKey(key)) return;
            const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(self.gpa, key)) orelse return;
            defer self.gpa.free(parsed.doc_key);
            defer self.gpa.free(parsed.artifact_name);
            if (resolverForArtifact(self.resolvers, parsed.artifact_name) == null) return;
            try self.out.append(self.gpa, try self.gpa.dupe(u8, key));
        }
    };
    var collector = Collector{ .gpa = gpa, .resolvers = resolvers, .out = &asset_keys };
    // All user keys live in [user_namespace, user_namespace+1); asset artifacts
    // are a subset filtered by the collector.
    const lower = [_]u8{internal_keys.user_namespace};
    const upper = [_]u8{internal_keys.user_namespace + 1};
    store.scanPrefix(lower[0..], upper[0..], &collector, Collector.consume) catch |err| switch (err) {
        error.ScanUnsupported => return 0,
        else => return err,
    };

    var journal_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (journal_keys.items) |k| gpa.free(@constCast(k));
        journal_keys.deinit(gpa);
    }
    var reresolved: usize = 0;
    for (asset_keys.items) |key| {
        const outcome = (try processChangedExtraction(gpa, resolvers, store, null, key, candidate_source, embedder)) orelse continue;
        reresolved += 1;
        switch (outcome.result) {
            .written, .cleared => try journal_keys.append(gpa, outcome.resolution_key),
            else => gpa.free(outcome.resolution_key),
        }
    }
    if (journal_keys.items.len > 0) {
        _ = try write_fn(write_ctx, .{ .changed_artifact_keys = journal_keys.items });
    }
    return reresolved;
}

pub const default_max_records_per_window: usize = 1024;

/// Iterate replay records matching the resolution hint from `from_sequence`,
/// resolve each record's changed extraction artifacts, and journal the
/// resolution writes via `write_fn`. Returns the highest sequence processed (or
/// `from_sequence -| 1` if none), which the caller persists as applied. Pure of
/// the runtime's threading/state so it is unit-testable.
pub fn catchUpWindow(
    gpa: Allocator,
    replay_source: replay_source_mod.Source,
    resolvers: []const ResolverConfig,
    store: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    from_sequence: u64,
    max_records: usize,
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
    candidate_source: ?CandidateSource,
    embedder: ?embedder_mod.DenseEmbedder,
) !u64 {
    const Ctx = struct {
        gpa: Allocator,
        resolvers: []const ResolverConfig,
        store: resolver_lib.ArtifactStore,
        provider: ?resolver_lib.CandidateProvider,
        write_ctx: *anyopaque,
        write_fn: DerivedRecordWriter,
        candidate_source: ?CandidateSource,
        embedder: ?embedder_mod.DenseEmbedder,
        max_seen: u64,

        fn consume(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var decoded = try change_journal_mod.decodeRecord(self.gpa, payload);
            defer decoded.deinit();
            try processRecordKeys(
                self.gpa,
                self.resolvers,
                self.store,
                self.provider,
                decoded.record.changed_artifact_keys,
                self.write_ctx,
                self.write_fn,
                self.candidate_source,
                self.embedder,
            );
            if (sequence > self.max_seen) self.max_seen = sequence;
        }
    };

    var ctx = Ctx{
        .gpa = gpa,
        .resolvers = resolvers,
        .store = store,
        .provider = provider,
        .write_ctx = write_ctx,
        .write_fn = write_fn,
        .candidate_source = candidate_source,
        .embedder = embedder,
        // from_sequence is exclusive; with no records, return it unchanged.
        .max_seen = from_sequence,
    };
    _ = try replay_source.forEachMatchingRecord(gpa, from_sequence, .resolution, max_records, &ctx, Ctx.consume);
    return ctx.max_seen;
}

/// Erased shard store + ownership, shared with the promotion runtime so both
/// stages adapt a concrete shard store to the erased store the same way.
pub const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    pub fn deinit(self: *RuntimeStoreHandle) void {
        if (self.owned) self.store.deinit();
    }
};

pub fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };
    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{ .store = try backend_erased.storeFrom(alloc, store.backendStore()), .owned = true };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{ .store = try backend_erased.storeFrom(alloc, store.backendStore()), .owned = true };
            }
        },
    }
    return .{ .store = try backend_erased.storeFrom(alloc, store), .owned = true };
}

/// Managed worker that catches the resolution stage up on changed extraction
/// artifacts. Mirrors `EnrichmentRuntime`'s lifecycle: it wraps the shard store
/// into the erased store and runs a background loop on the `backend_runtime` io
/// that drains applied -> target. Crash recovery is ordinary replay --
/// `applied_sequence` is persisted only after a window's resolution writes are
/// durable, and the stage is idempotent.
pub const ResolutionRuntime = struct {
    alloc: Allocator,
    store_handle: RuntimeStoreHandle,
    replay_source: replay_source_mod.Source,
    index_manager: *index_manager_mod.IndexManager,
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
    io_impl: ?*background_runtime_mod.IoImpl,
    /// Optional cross-shard candidate source injected by the api/serving layer;
    /// null means local-only blocking (the worker's own store). Must outlive the
    /// runtime.
    candidate_source: ?CandidateSource,
    /// Optional name embedder, injected by the db from the enrichment config;
    /// used to backfill mention name embeddings for cosine/ann blocking.
    embedder: ?embedder_mod.DenseEmbedder,
    applied_sequence: std.atomic.Value(u64),
    target_sequence: std.atomic.Value(u64),
    shutdown_flag: std.atomic.Value(bool),
    catch_up_mutex: std.atomic.Mutex = .unlocked,
    future: ?Io.Future(void),

    pub fn init(
        alloc: Allocator,
        store: anytype,
        replay_source: replay_source_mod.Source,
        index_manager: *index_manager_mod.IndexManager,
        write_ctx: *anyopaque,
        write_fn: DerivedRecordWriter,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        candidate_source: ?CandidateSource,
        embedder: ?embedder_mod.DenseEmbedder,
    ) !ResolutionRuntime {
        var store_handle = try initRuntimeStore(alloc, store);
        errdefer store_handle.deinit();
        const applied = try enrichment_state.loadAppliedSequence(alloc, store_handle.store, scope_name);
        return .{
            .alloc = alloc,
            .store_handle = store_handle,
            .replay_source = replay_source,
            .index_manager = index_manager,
            .write_ctx = write_ctx,
            .write_fn = write_fn,
            .io_impl = backend_runtime.io_impl,
            .candidate_source = candidate_source,
            .embedder = embedder,
            .applied_sequence = .init(applied),
            .target_sequence = .init(applied),
            .shutdown_flag = .init(false),
            .future = null,
        };
    }

    pub fn deinit(self: *ResolutionRuntime) void {
        self.stop();
        self.store_handle.deinit();
        self.* = undefined;
    }

    /// Raise the catch-up target; the worker loop drains toward it.
    pub fn notifySequence(self: *ResolutionRuntime, sequence: u64) void {
        var cur = self.target_sequence.load(.monotonic);
        while (sequence > cur) {
            cur = self.target_sequence.cmpxchgWeak(cur, sequence, .monotonic, .monotonic) orelse break;
        }
    }

    /// Inject (or clear) the cross-shard candidate source after construction.
    /// The serving layer uses this when it cannot pass the source through
    /// `OpenOptions` (managed DBs open lazily through the write cache). Taken
    /// under `catch_up_mutex` so it cannot tear against an in-flight catch-up;
    /// the next catch-up window then blocks against the new source.
    pub fn setCandidateSource(self: *ResolutionRuntime, src: ?CandidateSource) void {
        lockMutex(&self.catch_up_mutex);
        defer self.catch_up_mutex.unlock();
        self.candidate_source = src;
    }

    pub fn start(self: *ResolutionRuntime) !void {
        // Without io there is no background thread; the stage is then driven
        // synchronously via catchUp (e.g. from runUntilIdle).
        const io_impl = self.io_impl orelse return;
        self.future = try io_impl.io().concurrent(workerMain, .{self});
    }

    pub fn stop(self: *ResolutionRuntime) void {
        self.shutdown_flag.store(true, .release);
        if (self.future) |*future| {
            if (self.io_impl) |io_impl| {
                _ = future.await(io_impl.io());
            }
            self.future = null;
        }
    }

    /// Drain applied -> target. Serialized so the background worker and a
    /// synchronous driver (runUntilIdle) cannot process the same records at
    /// once; idempotent and safe to retry (the stage skips unchanged
    /// resolutions). `applied_sequence` is persisted only after durable writes.
    pub fn catchUp(self: *ResolutionRuntime) !void {
        lockMutex(&self.catch_up_mutex);
        defer self.catch_up_mutex.unlock();

        while (true) {
            const target = self.target_sequence.load(.acquire);
            const applied = self.applied_sequence.load(.acquire);
            if (applied >= target) return;

            const resolvers = try self.index_manager.listResolvers(self.alloc);
            defer {
                for (resolvers) |*cfg| cfg.deinit(self.alloc);
                self.alloc.free(resolvers);
            }
            if (resolvers.len == 0) {
                // No resolver configured; advance so the hint does not rescan.
                try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, target);
                self.applied_sequence.store(target, .release);
                return;
            }

            var das = DbArtifactStore(backend_erased.Store){ .store = &self.store_handle.store };
            const max_seen = try catchUpWindow(
                self.alloc,
                self.replay_source,
                resolvers,
                das.artifactStore(),
                null,
                // from_sequence is exclusive (records with seq > from), matching
                // the derived workers, which pass their applied_sequence.
                applied,
                default_max_records_per_window,
                self.write_ctx,
                self.write_fn,
                self.candidate_source,
                self.embedder,
            );
            if (max_seen <= applied) {
                // No matching records in (applied, target]; advance to target.
                try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, target);
                self.applied_sequence.store(target, .release);
                return;
            }
            try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, max_seen);
            self.applied_sequence.store(max_seen, .release);
            // Loop to process the next window if max_seen is still below target.
        }
    }

    /// Re-resolve the whole stored corpus for the configured resolvers (used
    /// when a resolver's config generation bumps). Serialized with catch-up so
    /// the worker and this pass cannot write the same resolution at once.
    /// Returns the number of extraction artifacts re-processed.
    pub fn reresolveBacklog(self: *ResolutionRuntime) !usize {
        lockMutex(&self.catch_up_mutex);
        defer self.catch_up_mutex.unlock();

        const resolvers = try self.index_manager.listResolvers(self.alloc);
        defer {
            for (resolvers) |*cfg| cfg.deinit(self.alloc);
            self.alloc.free(resolvers);
        }
        if (resolvers.len == 0) return 0;

        var das = DbArtifactStore(backend_erased.Store){ .store = &self.store_handle.store };
        return reresolveAll(
            self.alloc,
            das.artifactStore(),
            resolvers,
            self.candidate_source,
            self.embedder,
            self.write_ctx,
            self.write_fn,
        );
    }

    fn workerMain(self: *ResolutionRuntime) void {
        const io = (self.io_impl orelse return).io();
        while (!self.shutdown_flag.load(.acquire)) {
            if (self.applied_sequence.load(.acquire) < self.target_sequence.load(.acquire)) {
                self.catchUp() catch |err| {
                    std.log.warn("resolution catch-up failed: {s}", .{@errorName(err)});
                    io.sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
                };
            } else {
                io.sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
            }
        }
        self.catchUp() catch {};
    }
};

fn lockMutex(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.single_threaded) {
            std.atomic.spinLoopHint();
            continue;
        }
        std.Thread.yield() catch {};
    }
}

const testing = std.testing;

/// Adapts any shard store (the erased backend store: `beginRead/get`,
/// `beginWrite/put/delete/commit/abort`) to the resolver's `ArtifactStore`
/// seam. Generic over the store type so it works with the production erased
/// store and is unit-testable with a fake. The worker holds one of these and
/// passes `artifactStore()` to `processChangedExtraction`.
pub fn DbArtifactStore(comptime Store: type) type {
    return struct {
        store: *Store,

        const Self = @This();

        pub fn artifactStore(self: *Self) resolver_lib.ArtifactStore {
            return .{ .ptr = self, .vtable = &vtable };
        }

        // Range scan is only wired for the production erased store; fake stores
        // used to compile-check the runtime do not provide a cursor.
        const vtable = resolver_lib.ArtifactStore.VTable{
            .get = getFn,
            .put = putFn,
            .delete = deleteFn,
            .scan_prefix = if (Store == backend_erased.Store) scanPrefixFn else null,
        };

        fn scanPrefixFn(
            ptr: *anyopaque,
            lower: []const u8,
            upper: []const u8,
            ctx: *anyopaque,
            consume: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!void,
        ) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var txn = try self.store.beginRead();
            defer txn.abort();
            var cursor = try txn.openCursor();
            defer cursor.close();
            cursor.setUpperBound(upper);
            var entry = try cursor.seekAtOrAfter(lower);
            while (entry) |e| {
                try consume(ctx, e.key, e.value);
                entry = try cursor.next();
            }
        }

        fn getFn(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var txn = try self.store.beginRead();
            defer txn.abort();
            const raw = txn.get(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
            return try allocator.dupe(u8, raw);
        }

        fn putFn(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var txn = try self.store.beginWrite();
            errdefer txn.abort();
            try txn.put(key, value);
            try txn.commit();
        }

        fn deleteFn(ptr: *anyopaque, key: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var txn = try self.store.beginWrite();
            errdefer txn.abort();
            txn.delete(key) catch |err| {
                if (err != error.NotFound) return err;
            };
            try txn.commit();
        }
    };
}

const test_extraction =
    \\{ "entities": [
    \\    { "id": "e0", "label": "Person", "text": "Ada Lovelace" },
    \\    { "id": "e1", "label": "Org", "text": "Antfly" }
    \\  ] }
;

test "resolveExtraction produces a resolution artifact for the matching resolver" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "knowledge_graph",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 2,
    }};

    const out = (try resolveExtraction(alloc, &resolvers, "relations_v1", test_extraction, &.{})).?;
    defer alloc.free(out.bytes);
    try testing.expectEqualStrings("resolution_v1", out.resolution_artifact);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out.bytes, .{});
    defer parsed.deinit();
    try testing.expectEqual(@as(i64, 2), parsed.value.object.get("config_generation").?.integer);
    const entities = parsed.value.object.get("entities").?.array.items;
    try testing.expectEqual(@as(usize, 2), entities.len);
    try testing.expectEqualStrings(
        "person/ada_lovelace",
        entities[0].object.get("doc_ref").?.object.get("key").?.string,
    );
    try testing.expectEqualStrings(
        "org/antfly",
        entities[1].object.get("doc_ref").?.object.get("key").?.string,
    );
}

/// In-memory ArtifactStore for tests.
const MapStore = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *MapStore) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn store(self: *MapStore) resolver_lib.ArtifactStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = resolver_lib.ArtifactStore.VTable{ .get = get, .put = put, .delete = delete, .scan_prefix = scanPrefix };

    fn get(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        const v = self.map.get(key) orelse return null;
        return try allocator.dupe(u8, v);
    }
    fn scanPrefix(
        ptr: *anyopaque,
        lower: []const u8,
        upper: []const u8,
        ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!void,
    ) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        var it = self.map.iterator();
        while (it.next()) |e| {
            const k = e.key_ptr.*;
            if (std.mem.order(u8, k, lower) == .lt) continue;
            if (std.mem.order(u8, k, upper) != .lt) continue;
            try consume(ctx, k, e.value_ptr.*);
        }
    }
    fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        const owned_value = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned_value);
        const gop = try self.map.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            self.alloc.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
        }
        gop.value_ptr.* = owned_value;
    }
    fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        if (self.map.fetchRemove(key)) |kv| {
            self.alloc.free(kv.key);
            self.alloc.free(kv.value);
        }
    }
};

test "processChangedExtraction resolves and persists the resolution artifact" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "knowledge_graph",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 4,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a1", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try map.store().put(extraction_key, test_extraction);

    {
        const outcome = (try processChangedExtraction(alloc, &resolvers, map.store(), null, extraction_key, null, null)).?;
        defer alloc.free(outcome.resolution_key);
        try testing.expectEqual(resolver_lib.RunResult.written, outcome.result);
    }
    // Replay is idempotent.
    {
        const outcome = (try processChangedExtraction(alloc, &resolvers, map.store(), null, extraction_key, null, null)).?;
        defer alloc.free(outcome.resolution_key);
        try testing.expectEqual(resolver_lib.RunResult.unchanged, outcome.result);
    }

    // The resolution artifact landed at the expected key with the resolved entities.
    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a1", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try map.store().get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    try testing.expectEqual(@as(i64, 4), parsed.value.object.get("config_generation").?.integer);
    const entities = parsed.value.object.get("entities").?.array.items;
    try testing.expectEqualStrings("person/ada_lovelace", entities[0].object.get("doc_ref").?.object.get("key").?.string);

    // A non-asset key is ignored.
    try testing.expect((try processChangedExtraction(alloc, &resolvers, map.store(), null, "not-an-artifact-key", null, null)) == null);
}

test "resolveExtraction returns null when no resolver consumes the artifact" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "knowledge_graph",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
    }};
    try testing.expect((try resolveExtraction(alloc, &resolvers, "other_artifact", test_extraction, &.{})) == null);
    try testing.expect(resolverForArtifact(&resolvers, "relations_v1") != null);
    try testing.expect(resolverForArtifact(&resolvers, "nope") == null);
}

/// In-memory store with the erased-store txn shape, for testing DbArtifactStore.
const FakeStore = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *FakeStore) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn beginRead(self: *FakeStore) !ReadTxn {
        return .{ .s = self };
    }
    fn beginWrite(self: *FakeStore) !WriteTxn {
        return .{ .s = self };
    }

    const ReadTxn = struct {
        s: *FakeStore,
        fn abort(self: *ReadTxn) void {
            _ = self;
        }
        fn get(self: *ReadTxn, key: []const u8) ![]const u8 {
            return self.s.map.get(key) orelse error.NotFound;
        }
    };

    const WriteTxn = struct {
        s: *FakeStore,
        fn abort(self: *WriteTxn) void {
            _ = self;
        }
        fn commit(self: *WriteTxn) !void {
            _ = self;
        }
        fn put(self: *WriteTxn, key: []const u8, value: []const u8) !void {
            const owned_value = try self.s.alloc.dupe(u8, value);
            errdefer self.s.alloc.free(owned_value);
            const gop = try self.s.map.getOrPut(self.s.alloc, key);
            if (gop.found_existing) {
                self.s.alloc.free(gop.value_ptr.*);
            } else {
                gop.key_ptr.* = try self.s.alloc.dupe(u8, key);
            }
            gop.value_ptr.* = owned_value;
        }
        fn delete(self: *WriteTxn, key: []const u8) !void {
            if (self.s.map.fetchRemove(key)) |kv| {
                self.s.alloc.free(kv.key);
                self.s.alloc.free(kv.value);
            } else return error.NotFound;
        }
    };
};

test "DbArtifactStore adapts a shard store to the ArtifactStore seam" {
    const alloc = testing.allocator;
    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();

    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    try testing.expect((try store.get(alloc, "k")) == null);
    try store.put("k", "v1");
    {
        const got = (try store.get(alloc, "k")).?;
        defer alloc.free(got);
        try testing.expectEqualStrings("v1", got);
    }
    try store.put("k", "v2");
    {
        const got = (try store.get(alloc, "k")).?;
        defer alloc.free(got);
        try testing.expectEqualStrings("v2", got);
    }
    try store.delete("k");
    try testing.expect((try store.get(alloc, "k")) == null);
    try store.delete("k"); // delete of a missing key is a no-op
}

test "processChangedExtraction runs over a DbArtifactStore" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
        .config_generation = 1,
    }};

    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:z", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key, test_extraction);

    {
        const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, null, null)).?;
        defer alloc.free(outcome.resolution_key);
        try testing.expectEqual(resolver_lib.RunResult.written, outcome.result);
    }

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:z", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    try testing.expect(std.mem.indexOf(u8, stored, "ada_lovelace") != null);
}

/// Capturing DerivedRecordWriter for tests.
const CaptureWriter = struct {
    alloc: std.mem.Allocator,
    keys: std.ArrayListUnmanaged([]u8) = .empty,
    calls: u64 = 0,

    fn deinit(self: *CaptureWriter) void {
        for (self.keys.items) |k| self.alloc.free(k);
        self.keys.deinit(self.alloc);
    }

    fn writeFn(ptr: *anyopaque, batch: derived_types.DerivedBatch) anyerror!u64 {
        const self: *CaptureWriter = @ptrCast(@alignCast(ptr));
        for (batch.changed_artifact_keys) |k| {
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, k));
        }
        self.calls += 1;
        return self.calls;
    }
};

test "processRecordKeys resolves changed asset keys and journals resolution keys" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
        .config_generation = 1,
    }};

    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:r", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key, test_extraction);

    var writer = CaptureWriter{ .alloc = alloc };
    defer writer.deinit();

    // A record carrying the extraction key plus a non-asset key (ignored).
    const changed = [_][]const u8{ extraction_key, "not-an-artifact" };
    try processRecordKeys(alloc, &resolvers, store, null, &changed, &writer, CaptureWriter.writeFn, null, null);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:r", "resolution_v1");
    defer alloc.free(resolution_key);
    try testing.expectEqual(@as(usize, 1), writer.keys.items.len);
    try testing.expectEqualSlices(u8, resolution_key, writer.keys.items[0]);
    try testing.expectEqual(@as(u64, 1), writer.calls);

    // Idempotent replay: recomputed bytes match, so nothing is journaled.
    writer.calls = 0;
    try processRecordKeys(alloc, &resolvers, store, null, &changed, &writer, CaptureWriter.writeFn, null, null);
    try testing.expectEqual(@as(usize, 1), writer.keys.items.len);
    try testing.expectEqual(@as(u64, 0), writer.calls);
}

/// Minimal replay Source for tests: replays a fixed list of encoded records.
const FakeSource = struct {
    const Rec = struct { sequence: u64, payload: []const u8 };
    records: []const Rec,

    fn source(self: *FakeSource) replay_source_mod.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = replay_source_mod.Source.VTable{
        .open_matching_cursor = openCursor,
        .for_each_matching_record = forEach,
        .latest_matching_sequence = latest,
        .collect_enrichment_document_groups = collectGroups,
        .is_sequence_visible = isVisible,
    };

    fn forEach(
        ptr: *anyopaque,
        alloc: Allocator,
        from_sequence: u64,
        hint: replay_source_mod.TargetHint,
        max_matched_entries: usize,
        ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
    ) anyerror!replay_source_mod.MatchingRecordStats {
        _ = alloc;
        _ = hint;
        const self: *FakeSource = @ptrCast(@alignCast(ptr));
        var matched: usize = 0;
        var last: u64 = 0;
        for (self.records) |rec| {
            if (rec.sequence <= from_sequence) continue; // exclusive, matching the real source
            if (max_matched_entries != 0 and matched >= max_matched_entries) break;
            try consume(ctx, rec.sequence, rec.payload);
            matched += 1;
            last = rec.sequence;
        }
        return .{ .matched_entries = matched, .last_sequence = last };
    }

    fn openCursor(_: *anyopaque, _: Allocator, _: u64, _: replay_source_mod.TargetHint) anyerror!replay_source_mod.MatchingCursor {
        return error.Unsupported;
    }
    fn latest(_: *anyopaque, _: Allocator, _: u64, _: replay_source_mod.TargetHint) anyerror!u64 {
        return error.Unsupported;
    }
    fn collectGroups(_: *anyopaque, _: Allocator, _: u64) anyerror![]replay_source_mod.PendingDocumentGroup {
        return error.Unsupported;
    }
    fn isVisible(_: *anyopaque, _: u64) anyerror!bool {
        return error.Unsupported;
    }
};

test "catchUpWindow resolves matching records and journals resolution writes" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
        .config_generation = 1,
    }};

    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:w", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key, test_extraction);

    const payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 7,
        .changed_artifact_keys = &.{extraction_key},
        .target_hints = &.{.resolution},
    });
    defer alloc.free(payload);

    var fake_source = FakeSource{ .records = &.{.{ .sequence = 7, .payload = payload }} };
    var writer = CaptureWriter{ .alloc = alloc };
    defer writer.deinit();

    const max_seen = try catchUpWindow(
        alloc,
        fake_source.source(),
        &resolvers,
        store,
        null,
        1,
        0,
        &writer,
        CaptureWriter.writeFn,
        null,
        null,
    );
    try testing.expectEqual(@as(u64, 7), max_seen);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:w", "resolution_v1");
    defer alloc.free(resolution_key);
    try testing.expect(fake.map.contains(resolution_key));
    try testing.expectEqual(@as(usize, 1), writer.keys.items.len);
    try testing.expectEqualSlices(u8, resolution_key, writer.keys.items[0]);
}

test "catchUpWindow with no matching records returns from-1" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{};
    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    var fake_source = FakeSource{ .records = &.{} };
    var writer = CaptureWriter{ .alloc = alloc };
    defer writer.deinit();

    const max_seen = try catchUpWindow(alloc, fake_source.source(), &resolvers, das.artifactStore(), null, 5, 0, &writer, CaptureWriter.writeFn, null, null);
    try testing.expectEqual(@as(u64, 5), max_seen);
    try testing.expectEqual(@as(u64, 0), writer.calls);
}

test "ResolutionRuntime compiles end-to-end" {
    testing.refAllDecls(ResolutionRuntime);
}

test "processChangedExtraction with exact_key blocking links to an existing entity" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "exact_key",
        .scorer_json =
        \\{ "comparisons": [ { "name": "n", "left": "canonical_text", "right": "canonical_name",
        \\  "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );
    // An existing canonical entity at the key the resolver would mint.
    const entity_doc_key = try internal_keys.documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(entity_doc_key);
    try store.put(entity_doc_key,
        \\{ "canonical_name": "Ada Lovelace", "label": "person" }
    );

    const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, null, null)).?;
    defer alloc.free(outcome.resolution_key);
    try testing.expectEqual(resolver_lib.RunResult.written, outcome.result);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    // Linked to the existing entity (decision=match), not minted as new.
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}

test "prefix blocking links a typo'd mention to an existing entity with a different key" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "prefix",
        .scorer_json =
        \\{ "comparisons": [ { "name": "n", "left": "canonical_text", "right": "canonical_name",
        \\  "levels": [ { "when": "jaro_winkler > 0.9", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();

    // The mention has a typo and would mint "person/ada_lovlace".
    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovlace" } ] }
    );
    // An existing entity under the correct (different) key.
    const entity_doc_key = try internal_keys.documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(entity_doc_key);
    try store.put(entity_doc_key,
        \\{ "canonical_name": "Ada Lovelace", "label": "person" }
    );

    const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, null, null)).?;
    defer alloc.free(outcome.resolution_key);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    // Linked to the existing entity (prefix scan + scorer), not the typo'd mint.
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}

test "reresolveAll re-resolves the corpus when a resolver config generation bumps" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);

    // Resolve at config_generation 1.
    {
        const resolvers = [_]ResolverConfig{.{
            .name = "kg",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ slug _entity.text }}",
            .config_generation = 1,
        }};
        const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, null, null)).?;
        alloc.free(outcome.resolution_key);
    }
    {
        const stored = (try store.get(alloc, resolution_key)).?;
        defer alloc.free(stored);
        try testing.expect(std.mem.indexOf(u8, stored, "\"config_generation\":1") != null);
    }

    // Bump to config_generation 2: the extraction artifact is unchanged, so the
    // incremental hint never fires; the backfill re-resolves the corpus.
    const bumped = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
        .config_generation = 2,
    }};
    var writer = CaptureWriter{ .alloc = alloc };
    defer writer.deinit();
    const reresolved = try reresolveAll(alloc, store, &bumped, null, null, &writer, CaptureWriter.writeFn);
    try testing.expectEqual(@as(usize, 1), reresolved);
    // The re-resolution changed the artifact (new generation) and was journaled.
    try testing.expectEqual(@as(usize, 1), writer.keys.items.len);
    try testing.expectEqualSlices(u8, resolution_key, writer.keys.items[0]);

    const restored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(restored);
    try testing.expect(std.mem.indexOf(u8, restored, "\"config_generation\":2") != null);

    // Idempotent: a second backfill at the same generation journals nothing.
    writer.calls = 0;
    for (writer.keys.items) |k| alloc.free(k);
    writer.keys.clearRetainingCapacity();
    _ = try reresolveAll(alloc, store, &bumped, null, null, &writer, CaptureWriter.writeFn);
    try testing.expectEqual(@as(u64, 0), writer.calls);
}

test "prefix blocking follows a merged_into redirect to the survivor entity" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "prefix",
        .scorer_json =
        \\{ "comparisons": [ { "name": "n", "left": "canonical_text", "right": "canonical_name",
        \\  "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );
    // The entity blocked by prefix is a merged-away duplicate pointing at the
    // surviving canonical entity via merged_into.
    const dup_key = try internal_keys.documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(dup_key);
    try store.put(dup_key,
        \\{ "canonical_name": "Ada Lovelace", "label": "person", "merged_into": "person/ada_canonical" }
    );

    const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, null, null)).?;
    defer alloc.free(outcome.resolution_key);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    // Resolved to the survivor named by merged_into, not the matched duplicate.
    try testing.expectEqualStrings("person/ada_canonical", ent.get("doc_ref").?.object.get("key").?.string);
}

test "embedding (cosine) scoring links via prefix blocking" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "prefix",
        .scorer_json =
        \\{ "comparisons": [ { "name": "emb", "left": "name_embedding", "right": "name_embedding",
        \\  "levels": [ { "when": "cosine > 0.9", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();

    // Mention carries a query embedding; its text differs from the entity's.
    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "A Lovelace", "embedding": [0.1, 0.2, 0.3, 0.4] } ] }
    );
    const entity_doc_key = try internal_keys.documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(entity_doc_key);
    try store.put(entity_doc_key,
        \\{ "canonical_name": "Ada Lovelace", "label": "person", "name_embedding": [0.1, 0.2, 0.3, 0.4] }
    );

    const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, null, null)).?;
    defer alloc.free(outcome.resolution_key);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    // Linked by embedding similarity despite differing text.
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}

/// In-memory CandidateSource for cross-shard blocking tests: maps an entity key
/// to its stored JSON doc. Serves all three blocking modes (exact key, label
/// prefix, vector nearest) so the worker's `SourceCandidateProvider` dispatch
/// can be exercised without a live cluster transport.
const FakeCandidateSource = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    /// Records the (table) each mode was queried with, for assertions.
    last_table: []const u8 = "",

    fn deinit(self: *FakeCandidateSource) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn put(self: *FakeCandidateSource, key: []const u8, value: []const u8) !void {
        const owned_value = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned_value);
        const gop = try self.map.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            self.alloc.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
        }
        gop.value_ptr.* = owned_value;
    }

    fn candidateSource(self: *FakeCandidateSource) CandidateSource {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = CandidateSource.VTable{
        .get = getFn,
        .scan_prefix = scanPrefixFn,
        .nearest = nearestFn,
    };

    fn getFn(ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8 {
        const self: *FakeCandidateSource = @ptrCast(@alignCast(ptr));
        self.last_table = table;
        const v = self.map.get(key) orelse return null;
        return try allocator.dupe(u8, v);
    }

    fn scanPrefixFn(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table: []const u8,
        prefix: []const u8,
        ctx: *anyopaque,
        consume: CandidateSource.Consume,
    ) anyerror!void {
        _ = allocator;
        const self: *FakeCandidateSource = @ptrCast(@alignCast(ptr));
        self.last_table = table;
        var it = self.map.iterator();
        while (it.next()) |e| {
            if (!std.mem.startsWith(u8, e.key_ptr.*, prefix)) continue;
            try consume(ctx, e.key_ptr.*, e.value_ptr.*);
        }
    }

    fn nearestFn(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table: []const u8,
        embedding: []const f32,
        k: usize,
        ctx: *anyopaque,
        consume: CandidateSource.Consume,
    ) anyerror!void {
        _ = allocator;
        _ = embedding;
        const self: *FakeCandidateSource = @ptrCast(@alignCast(ptr));
        self.last_table = table;
        var emitted: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |e| {
            if (emitted >= k) break;
            try consume(ctx, e.key_ptr.*, e.value_ptr.*);
            emitted += 1;
        }
    }
};

test "SourceCandidateProvider links via an injected cross-shard exact_key source" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "exact_key",
        .scorer_json =
        \\{ "comparisons": [ { "name": "n", "left": "canonical_text", "right": "canonical_name",
        \\  "levels": [ { "when": "exact", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    }};

    // The worker's own store holds only the extraction; the entity lives on
    // another shard, served by the injected CandidateSource.
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();
    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace" } ] }
    );

    var remote = FakeCandidateSource{ .alloc = alloc };
    defer remote.deinit();
    try remote.put("person/ada_lovelace",
        \\{ "canonical_name": "Ada Lovelace", "label": "person" }
    );

    const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, remote.candidateSource(), null)).?;
    defer alloc.free(outcome.resolution_key);
    try testing.expectEqual(resolver_lib.RunResult.written, outcome.result);
    try testing.expectEqualStrings("entities", remote.last_table);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    // Linked to the cross-shard entity, not minted locally.
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}

test "SourceCandidateProvider prefix blocking scans a cross-shard label namespace" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "prefix",
        .scorer_json =
        \\{ "comparisons": [ { "name": "n", "left": "canonical_text", "right": "canonical_name",
        \\  "levels": [ { "when": "jaro_winkler > 0.9", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();
    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    // Typo'd mention would mint "person/ada_lovlace".
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovlace" } ] }
    );

    var remote = FakeCandidateSource{ .alloc = alloc };
    defer remote.deinit();
    try remote.put("person/ada_lovelace",
        \\{ "canonical_name": "Ada Lovelace", "label": "person" }
    );
    // A different-label entity that must be excluded by the prefix scan.
    try remote.put("org/antfly",
        \\{ "canonical_name": "Antfly", "label": "org" }
    );

    const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, remote.candidateSource(), null)).?;
    defer alloc.free(outcome.resolution_key);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}

test "SourceCandidateProvider ann blocking links via cross-shard nearest neighbours" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "ann",
        .scorer_json =
        \\{ "comparisons": [ { "name": "emb", "left": "name_embedding", "right": "name_embedding",
        \\  "levels": [ { "when": "cosine > 0.9", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    const store = map.store();
    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    // Mention carries a query embedding; its text differs from the entity's.
    try store.put(extraction_key,
        \\{ "entities": [ { "id": "e0", "label": "person", "text": "A Lovelace", "embedding": [0.1, 0.2, 0.3, 0.4] } ] }
    );

    var remote = FakeCandidateSource{ .alloc = alloc };
    defer remote.deinit();
    try remote.put("person/ada_lovelace",
        \\{ "canonical_name": "Ada Lovelace", "label": "person", "name_embedding": [0.1, 0.2, 0.3, 0.4] }
    );

    const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key, remote.candidateSource(), null)).?;
    defer alloc.free(outcome.resolution_key);
    try testing.expectEqualStrings("entities", remote.last_table);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    // Linked by vector similarity across the shard boundary despite differing text.
    try testing.expectEqualStrings("match", ent.get("decision").?.string);
    try testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);
}
